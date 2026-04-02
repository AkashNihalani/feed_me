from __future__ import annotations

import re
import json
import time
import mimetypes
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone, timedelta, date
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

import psycopg
import requests
from dateutil import parser as date_parser
from psycopg.rows import dict_row

from .scraper import run_actor_handle, run_actor_post_urls
from .instagram import canonical_post_url, shortcode_from_media_id, shortcode_from_url
from .config import (
    POSTGRES_DSN,
    RETRY_BACKOFF_MINUTES,
    APP_TIMEZONE,
    RUN_JOB_CONCURRENCY,
    CHECKPOINT_SCRAPE_CHUNK_SIZE,
    CHECKPOINT_JOB_CLAIM_LIMIT,
    STALE_JOB_MINUTES,
    CHECKPOINT_BATCH_HOUR_24,
    CHECKPOINT_BATCH_MINUTE,
    CHECKPOINT_BUCKET_MINUTES,
    SUPABASE_URL,
    SUPABASE_SERVICE_ROLE_KEY,
    SUPABASE_MEDIA_BUCKET,
    FIRE_MEDIA_RETENTION_ENABLED,
)
from .web_push import is_enabled as web_push_enabled, send as send_web_push
from .post_intelligence import (
    current_model_version as pi_model_version,
    extract_tags as pi_extract_tags,
    is_enabled as pi_enabled,
)


def _next_retry_time(attempt: int) -> datetime:
    idx = min(attempt - 1, len(RETRY_BACKOFF_MINUTES) - 1)
    return datetime.now(timezone.utc) + timedelta(minutes=RETRY_BACKOFF_MINUTES[idx])


_HARD_SKIP_PREFIX = 'hard-skip:'
_TRANSIENT_FAILURE_TOKENS = (
    'timeout',
    'timed out',
    'too many requests',
    '429',
    'rate limit',
    'connection',
    'network',
    'temporarily unavailable',
    'service unavailable',
    'bad gateway',
    'gateway timeout',
    'internal server error',
    'connection reset',
    'bright data json parse error',
    'json decode',
    'extra data',
    'expecting value',
)
_HARD_FAILURE_TOKENS = (
    'not found',
    'private',
    'deleted',
    'post missing',
    'missing in checkpoint batch',
    'missing post',
)
_PUSH_TRANSIENT_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
_MEDIA_CAPTURE_TIMEOUT_SECONDS = 60
_MEDIA_UPLOAD_TIMEOUT_SECONDS = 120
_MEDIA_ALLOWED_FETCH_PROTOCOLS = ("http://", "https://")
_MEDIA_FETCH_HEADERS = {
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
    "accept": "*/*",
    "referer": "https://www.instagram.com/",
}
_IMAGE_ASSET_RETENTION_DAYS = 30
_HEAVY_ASSET_RETENTION_DAYS = 8
_DB_CONNECTION_ERROR_TOKENS = (
    "connection is closed",
    "server closed the connection unexpectedly",
    "consuming input failed",
    "ssl error",
    "unexpected eof",
    "broken pipe",
    "connection reset",
    "connection refused",
    "terminating connection",
    "could not receive data from server",
)


def _normalize_error(err: str | None) -> str:
    return (err or '').strip().lower()


def _post_media_rollover_deadline(posted_at: datetime | None, asset_role: str | None) -> datetime:
    role = (asset_role or "").strip().lower()
    base = posted_at or datetime.now(timezone.utc)
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    days = _HEAVY_ASSET_RETENTION_DAYS
    if role in ("thumbnail", "display") or role.startswith("carousel_"):
        days = _IMAGE_ASSET_RETENTION_DAYS
    return base + timedelta(days=days)


def _is_transient_failure(err: str | None) -> bool:
    e = _normalize_error(err)
    return any(tok in e for tok in _TRANSIENT_FAILURE_TOKENS)


def _is_hard_failure(err: str | None) -> bool:
    e = _normalize_error(err)
    if any(tok in e for tok in _HARD_FAILURE_TOKENS):
        return True
    # Policy: only transient failures are retried. Everything else is terminal skip.
    return not _is_transient_failure(e)


def _hard_skip_error(err: str | None, fallback: str) -> str:
    msg = (err or '').strip() or fallback
    return f"{_HARD_SKIP_PREFIX}{msg[:900]}"


def _should_retry_web_push(status_code: int | None, err: str | None) -> bool:
    if status_code in _PUSH_TRANSIENT_STATUS_CODES:
        return True
    return _is_transient_failure(err)


def _is_connection_error(exc: Exception | None) -> bool:
    if exc is None:
        return False
    msg = _normalize_error(str(exc))
    if any(tok in msg for tok in _DB_CONNECTION_ERROR_TOKENS):
        return True
    return isinstance(exc, psycopg.OperationalError)


def _to_dt(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1_000_000_000_000:
            ts = ts / 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    try:
        dt = date_parser.parse(str(value))
        if dt.tzinfo is None:
            try:
                source_tz = ZoneInfo(APP_TIMEZONE or "Asia/Kolkata")
            except Exception:
                source_tz = timezone.utc
            dt = dt.replace(tzinfo=source_tz)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _post_key_from_url(post_url: str) -> str:
    u = (post_url or "").strip().lower()
    u = re.sub(r"^https?://(www\.)?instagram\.com/", "", u)
    u = u.split("?", 1)[0].split("#", 1)[0].strip("/")
    return u


def _media_type(item: dict) -> str:
    m = (item.get("type") or item.get("mediaType") or "").lower()
    if "reel" in m or "video" in m:
        return "reel"
    if "sidecar" in m or "carousel" in m:
        return "sidecar"
    if "image" in m:
        return "image"
    return m or "unknown"


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def _clean_profile_pic_url(url: Any) -> str | None:
    if url is None:
        return None
    u = str(url).strip()
    if not u:
        return None
    lu = u.lower()
    if "static.cdninstagram.com/rsrc.php" in lu or "/rsrc.php/" in lu:
        return None
    return u

def _extract_metrics(item: dict) -> tuple[int | None, int | None, int | None]:
    views = _to_int(
        item.get("videoViewCount")
        or item.get("videoPlayCount")
        or item.get("views")
        or item.get("viewCount")
        or item.get("plays")
    )
    likes = _to_int(item.get("likesCount") or item.get("likes") or item.get("likeCount"))
    comments = _to_int(
        item.get("commentsCount")
        or item.get("comments")
        or item.get("commentCount")
        or item.get("comments_count")
        or item.get("num_comments")
    )
    return views, likes, comments


def _is_reel_media_type(media_type: Any) -> bool:
    return str(media_type or "").strip().lower() == "reel"


def _checkpoint_scrape_url(job: dict) -> str:
    post_url = str(job.get("post_url") or "").strip()
    provider_post_id = str(job.get("provider_post_id") or "").strip()
    shortcode = shortcode_from_media_id(provider_post_id) or shortcode_from_url(post_url)
    if not shortcode:
        return canonical_post_url(shortcode, post_url)
    if _is_reel_media_type(job.get("media_type")):
        return f"https://www.instagram.com/reel/{shortcode}/"
    return canonical_post_url(shortcode, post_url)


def _checkpoint_item_error(item: dict) -> str | None:
    code = str(item.get("errorCode") or item.get("error_code") or "").strip()
    message = str(item.get("error") or "").strip()
    if code or message:
        detail = f"{code}: {message}".strip(": ").strip()
        return detail or "checkpoint scrape returned provider error"
    return None

def _extract_owner_profile(item: dict) -> tuple[str | None, int | None]:
    owner = item.get("owner") if isinstance(item.get("owner"), dict) else {}

    profile_pic = (
        item.get("ownerProfilePicUrl")
        or item.get("ownerProfilePicURL")
        or item.get("ownerProfilePicUrlHD")
        or owner.get("profilePicUrl")
        or owner.get("profile_pic_url")
    )
    profile_pic = _clean_profile_pic_url(profile_pic)

    followers = _to_int(
        item.get("ownerFollowersCount")
        or item.get("followersCount")
        or owner.get("followersCount")
        or (owner.get("edge_followed_by") or {}).get("count")
    )

    return profile_pic, followers


def _extract_media_refs(item: dict) -> tuple[str | None, str | None, list[str]]:
    """Extract reusable media references for UI thumbnails and downstream vector jobs."""
    display_url = (
        item.get("displayUrl")
        or item.get("display_url")
        or item.get("imageUrl")
        or item.get("thumbnailUrl")
        or item.get("thumbnailSrc")
    )
    video_url = item.get("videoUrl") or item.get("video_url")
    thumbnail_url = item.get("thumbnailUrl") or item.get("thumbnailSrc") or display_url
    if thumbnail_url and video_url and str(thumbnail_url).strip() == str(video_url).strip():
        thumbnail_url = display_url if display_url and str(display_url).strip() != str(video_url).strip() else None

    carousel_urls: list[str] = []
    children = item.get("childPosts") or item.get("sidecarImages") or item.get("carouselMedia") or []
    if isinstance(children, list):
        for c in children:
            if not isinstance(c, dict):
                continue
            u = c.get("displayUrl") or c.get("imageUrl") or c.get("videoUrl")
            if u and str(u) not in carousel_urls:
                carousel_urls.append(str(u))

    def _clean(u: Any) -> str | None:
        if u is None:
            return None
        t = str(u).strip()
        return t if t else None

    return _clean(thumbnail_url), _clean(video_url), carousel_urls


def _chunk_list(items: list[Any], size: int) -> list[list[Any]]:
    chunk_size = max(1, size)
    return [items[idx: idx + chunk_size] for idx in range(0, len(items), chunk_size)]


def _fire_media_object_key(post_key: str, asset_role: str, source_url: str | None, content_type: str | None = None) -> str:
    safe_post_key = re.sub(r"[^a-z0-9._-]+", "_", (post_key or "").strip().lower()) or "post"
    safe_role = re.sub(r"[^a-z0-9._-]+", "_", (asset_role or "").strip().lower()) or "asset"
    ext = ""
    guessed = mimetypes.guess_extension((content_type or "").split(";", 1)[0].strip()) if content_type else None
    if guessed:
        ext = guessed
    elif source_url:
        src = str(source_url).split("?", 1)[0].split("#", 1)[0]
        m = re.search(r"(\.[A-Za-z0-9]{2,6})$", src)
        if m:
            ext = m.group(1).lower()
    return f"posts/{safe_post_key}/{safe_role}{ext}"


def _storage_object_url(bucket: str, path: str) -> str:
    return f"{SUPABASE_URL.rstrip('/')}/storage/v1/object/{bucket}/{quote(path, safe='/')}"


def _storage_authenticated_object_url(bucket: str, path: str) -> str:
    return f"{SUPABASE_URL.rstrip('/')}/storage/v1/object/authenticated/{bucket}/{quote(path, safe='/')}"


def _daily_checkpoint_for_post(posted_at: datetime | None, business_date_ist: date | None) -> str:
    if posted_at is None:
        return ""
    if business_date_ist is None:
        return ""
    try:
        tz = ZoneInfo(APP_TIMEZONE or "Asia/Kolkata")
    except Exception:
        tz = timezone.utc
    post_date = posted_at.astimezone(tz).date()
    if post_date == business_date_ist:
        return "d1"
    if post_date == (business_date_ist - timedelta(days=1)):
        return "d2"
    return ""


def _checkpoint_business_day(next_run_at: Any) -> date:
    dt = _to_dt(next_run_at)
    try:
        tz = ZoneInfo(APP_TIMEZONE or "Asia/Kolkata")
    except Exception:
        tz = timezone.utc
    if dt is None:
        return datetime.now(tz).date()
    return dt.astimezone(tz).date()


def _checkpoint_days_after(checkpoint: str) -> int | None:
    cp = (checkpoint or "").strip().lower()
    mapping = {
        "d1": 1,
        "d3": 3,
        "d7": 7,
        "d21": 21,
    }
    return mapping.get(cp)


def _checkpoint_due_at_for_post(posted_at: datetime | None, checkpoint: str) -> datetime | None:
    if posted_at is None:
        return None
    days_after = _checkpoint_days_after(checkpoint)
    if days_after is None:
        return None
    return posted_at + timedelta(days=days_after)


def _checkpoint_business_day_for_post(posted_at: datetime | None, checkpoint: str) -> date | None:
    due_at = _checkpoint_due_at_for_post(posted_at, checkpoint)
    if due_at is None:
        return None
    try:
        tz = ZoneInfo(APP_TIMEZONE or "Asia/Kolkata")
    except Exception:
        tz = timezone.utc
    return due_at.astimezone(tz).date()


def _business_date_from_job(job: dict) -> date | None:
    raw = job.get("business_date_ist")
    if raw:
        try:
            return date_parser.parse(str(raw)).date()
        except Exception:
            pass
    try:
        tz = ZoneInfo(APP_TIMEZONE or "Asia/Kolkata")
    except Exception:
        tz = timezone.utc
    return datetime.now(tz).date()


def _push_payload_url(day_key: str | None, threshold: int) -> str:
    url = f"/fire?threshold={threshold}"
    if day_key:
        return f"{url}&day={day_key}"
    return url


def _build_fire_push_payload(rows: list[dict[str, Any]], threshold: int) -> dict[str, Any]:
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            _to_dt(row.get("fire_created_at") or row.get("created_at")) or datetime.fromtimestamp(0, tz=timezone.utc)
        ),
        reverse=True,
    )

    grouped: dict[str, dict[str, Any]] = {}
    for row in sorted_rows:
        feed_id = str(row.get("feed_id") or "unknown")
        created_at = _to_dt(row.get("fire_created_at") or row.get("created_at")) or datetime.fromtimestamp(0, tz=timezone.utc)
        title = str(row.get("feed_name") or "Tracked feed").strip() or "Tracked feed"
        current = grouped.get(feed_id)
        if current is None:
            grouped[feed_id] = {
                "count": 1,
                "latest": row,
                "latest_ts": created_at,
                "title": title,
            }
            continue
        current["count"] += 1
        if created_at >= current["latest_ts"]:
            current["latest"] = row
            current["latest_ts"] = created_at

    feed_groups = sorted(
        grouped.values(),
        key=lambda item: (-int(item["count"]), item["latest_ts"]),
        reverse=False,
    )
    feed_groups.sort(key=lambda item: (-int(item["count"]), -item["latest_ts"].timestamp()))

    lead = feed_groups[0]
    lead_title = str(lead["title"] or "Tracked feed")
    lead_latest = lead["latest"] or {}
    checkpoint = str(lead_latest.get("checkpoint") or "").upper() or "the latest checkpoint"
    lead_day = str(lead_latest.get("business_date_ist") or "").strip() or None

    if len(feed_groups) <= 1:
        if len(rows) == 1:
            title = f"{lead_title} triggered"
            body = f"{lead_title} crossed your {threshold}% fire line after {checkpoint}."
        else:
            title = f"{lead_title} heating up"
            body = (
                f"{lead['count']} fresh fire alerts in {lead_title} crossed your "
                f"{threshold}% line after the latest checkpoint sweep."
            )
    else:
        title = f"{len(feed_groups)} feeds triggered"
        body = (
            f"{len(feed_groups)} feeds crossed your {threshold}% line. "
            f"{lead_title} led with {lead['count']} fresh alerts."
        )

    return {
        "title": title,
        "body": body,
        "url": _push_payload_url(lead_day, threshold),
        "tag": f"fire-threshold-{threshold}",
    }


class PureEngine:
    def __init__(self):
        self.conn = self._connect()
        self._feeder_tracking_started_cache: dict[int, datetime | None] = {}

    def _connect(self):
        return psycopg.connect(
            POSTGRES_DSN,
            row_factory=dict_row,
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=3,
        )

    def _reconnect(self, reason: str | None = None):
        try:
            self.conn.close()
        except Exception:
            pass
        self.conn = self._connect()
        if reason:
            print(f"[db] reconnected: {reason}")

    def ensure_connection(self, reason: str | None = None, verify: bool = False):
        conn = getattr(self, "conn", None)
        if conn is None or getattr(conn, "closed", False):
            self._reconnect(reason or "connection was closed")
            return
        if verify:
            try:
                with conn.cursor() as cur:
                    cur.execute("select 1")
                    cur.fetchone()
            except Exception as exc:
                if _is_connection_error(exc):
                    self._reconnect(reason or str(exc))
                    return
                raise

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def _storage_headers(self, content_type: str | None = None) -> dict[str, str]:
        headers = {
            "apikey": SUPABASE_SERVICE_ROLE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
            "x-upsert": "true",
        }
        if content_type:
            headers["Content-Type"] = content_type
        return headers

    def _storage_read_headers(self) -> dict[str, str]:
        return {
            "apikey": SUPABASE_SERVICE_ROLE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        }

    def _stage_post_media_assets(
        self,
        post_key: str,
        posted_at: datetime | None,
        thumbnail_url: str | None,
        video_url: str | None,
        carousel_urls: list[str] | None,
    ):
        if not FIRE_MEDIA_RETENTION_ENABLED:
            return

        payloads: list[tuple[str, str]] = []
        if thumbnail_url and thumbnail_url != video_url:
            payloads.append(("thumbnail", thumbnail_url))
        if video_url:
            payloads.append(("video", video_url))
        for idx, url in enumerate(carousel_urls or []):
            if url:
                payloads.append((f"carousel_{idx}", url))

        for asset_role, source_url in payloads:
            purge_after = _post_media_rollover_deadline(posted_at, asset_role)
            if purge_after <= datetime.now(timezone.utc):
                continue
            self.conn.execute(
                """
                insert into public.post_media_assets (
                  post_key, asset_role, source_url, storage_bucket, status, attempt,
                  next_run_at, purge_after, last_error, updated_at
                )
                values (
                  %s,
                  %s,
                  %s,
                  %s,
                  'pending_capture',
                  0,
                  now(),
                  %s,
                  null,
                  now()
                )
                on conflict (post_key, asset_role) do update
                set source_url = excluded.source_url,
                    storage_bucket = excluded.storage_bucket,
                    purge_after = excluded.purge_after,
                    deleted_at = null,
                    updated_at = now(),
                    last_error = null,
                    status = case
                      when public.post_media_assets.status = 'active'
                           and coalesce(public.post_media_assets.storage_path, '') <> ''
                           and coalesce(public.post_media_assets.source_url, '') = excluded.source_url
                        then 'active'
                      when public.post_media_assets.status = 'purge_pending'
                           and coalesce(public.post_media_assets.storage_path, '') <> ''
                           and coalesce(public.post_media_assets.source_url, '') = excluded.source_url
                        then 'active'
                      else 'pending_capture'
                    end,
                    attempt = case
                      when public.post_media_assets.status = 'active'
                           and coalesce(public.post_media_assets.storage_path, '') <> ''
                           and coalesce(public.post_media_assets.source_url, '') = excluded.source_url
                        then public.post_media_assets.attempt
                      else 0
                    end,
                    next_run_at = case
                      when public.post_media_assets.status = 'active'
                           and coalesce(public.post_media_assets.storage_path, '') <> ''
                           and coalesce(public.post_media_assets.source_url, '') = excluded.source_url
                        then public.post_media_assets.next_run_at
                      else now()
                    end
                """,
                (post_key, asset_role, source_url, SUPABASE_MEDIA_BUCKET, purge_after),
            )

    def _claim_post_media_assets_for_capture(self, limit: int) -> list[dict]:
        if not FIRE_MEDIA_RETENTION_ENABLED:
            return []
        rows = self.conn.execute(
            "select * from public.claim_post_media_assets_for_capture(%s)",
            (max(1, limit),),
        ).fetchall()
        self.conn.commit()
        return rows

    def _claim_post_media_assets_for_purge(self, limit: int) -> list[dict]:
        if not FIRE_MEDIA_RETENTION_ENABLED:
            return []
        rows = self.conn.execute(
            "select * from public.claim_post_media_assets_for_purge(%s)",
            (max(1, limit),),
        ).fetchall()
        self.conn.commit()
        return rows

    def _set_post_media_asset_result(
        self,
        asset_id: int,
        status: str,
        *,
        attempt: int | None = None,
        next_run_at: datetime | None = None,
        storage_path: str | None = None,
        mime_type: str | None = None,
        byte_size: int | None = None,
        captured_at: datetime | None = None,
        deleted_at: datetime | None = None,
        error: str | None = None,
    ):
        self.conn.execute(
            """
            update public.post_media_assets
            set status = %s,
                attempt = coalesce(%s, attempt),
                next_run_at = coalesce(%s::timestamptz, next_run_at),
                storage_path = coalesce(%s::text, storage_path),
                mime_type = coalesce(%s::text, mime_type),
                byte_size = coalesce(%s, byte_size),
                captured_at = coalesce(%s::timestamptz, captured_at),
                deleted_at = coalesce(%s::timestamptz, deleted_at),
                last_error = case when %s::text is null then null else left(%s::text, 1000) end,
                updated_at = now()
            where id = %s
            """,
            (
                status,
                attempt,
                next_run_at,
                storage_path,
                mime_type,
                byte_size,
                captured_at,
                deleted_at,
                error,
                error,
                asset_id,
            ),
        )
        self.conn.commit()

    def _delete_post_media_asset_row(self, asset_id: int):
        self.conn.execute(
            """
            delete from public.post_media_assets
            where id = %s
            """,
            (asset_id,),
        )
        self.conn.commit()

    def _capture_post_media_asset(self, asset: dict):
        source_url = str(asset.get("source_url") or "").strip()
        if not source_url.startswith(_MEDIA_ALLOWED_FETCH_PROTOCOLS):
            raise RuntimeError("invalid media source url")

        upstream = requests.get(
            source_url,
            headers=_MEDIA_FETCH_HEADERS,
            timeout=_MEDIA_CAPTURE_TIMEOUT_SECONDS,
        )
        upstream.raise_for_status()
        content_type = (upstream.headers.get("content-type") or "application/octet-stream").split(";", 1)[0].strip()
        body = upstream.content
        if not body:
            raise RuntimeError("empty media payload")

        storage_path = _fire_media_object_key(
            str(asset.get("post_key") or ""),
            str(asset.get("asset_role") or ""),
            source_url,
            content_type,
        )
        upload = requests.post(
            _storage_object_url(str(asset.get("storage_bucket") or SUPABASE_MEDIA_BUCKET), storage_path),
            headers=self._storage_headers(content_type),
            data=body,
            timeout=_MEDIA_UPLOAD_TIMEOUT_SECONDS,
        )
        upload.raise_for_status()
        self._set_post_media_asset_result(
            int(asset["id"]),
            "active",
            attempt=0,
            next_run_at=None,
            storage_path=storage_path,
            mime_type=content_type,
            byte_size=len(body),
            captured_at=datetime.now(timezone.utc),
            deleted_at=None,
            error=None,
        )

    def _delete_post_media_asset(self, asset: dict):
        storage_path = str(asset.get("storage_path") or "").strip()
        if not storage_path:
            self._delete_post_media_asset_row(int(asset["id"]))
            return

        resp = requests.delete(
            _storage_object_url(str(asset.get("storage_bucket") or SUPABASE_MEDIA_BUCKET), storage_path),
            headers=self._storage_headers(),
            timeout=_MEDIA_CAPTURE_TIMEOUT_SECONDS,
        )
        if resp.status_code not in (200, 204, 404):
            resp.raise_for_status()
        self._delete_post_media_asset_row(int(asset["id"]))

    def enqueue_daily(self) -> int:
        r = self.conn.execute("select public.enqueue_daily_jobs(now()) as c").fetchone()
        self.conn.commit()
        return int((r or {}).get("c") or 0)

    def enqueue_poll(self) -> int:
        r = self.conn.execute("select public.enqueue_poll_jobs(now()) as c").fetchone()
        self.conn.commit()
        return int((r or {}).get("c") or 0)

    def enqueue_daily_followers(self) -> int:
        try:
            r = self.conn.execute("select public.enqueue_daily_follower_jobs(now()) as c").fetchone()
        except Exception:
            self.conn.rollback()
            r = self.conn.execute("select public.enqueue_weekly_follower_jobs(now()) as c").fetchone()
        self.conn.commit()
        return int((r or {}).get("c") or 0)

    def enqueue_weekly_followers(self) -> int:
        return self.enqueue_daily_followers()

    def requeue_stale(self, minutes: int = 30):
        self.conn.execute("select * from public.requeue_stale_jobs(%s)", (max(1, minutes),)).fetchone()
        self.conn.commit()

    def prune_expired_fire_state(self):
        row = self.conn.execute("select public.prune_expired_fire_state() as payload").fetchone()
        self.conn.commit()
        return (row or {}).get("payload")



    def _refresh_feeder_profile(self, feeder_id: int, profile_pic_url: str | None, follower_count: int | None):
        self.conn.execute(
            """
            update public.feeders
            set profile_pic_url = coalesce(%s, profile_pic_url),
                follower_count = coalesce(%s, follower_count),
                updated_at = now()
            where id = %s
            """,
            (profile_pic_url, follower_count, feeder_id),
        )

    def _recent_provider_post_ids(self, feeder_id: int, limit: int = 200) -> list[str]:
        rows = self.conn.execute(
            """
            select provider_post_id
            from public.posts
            where feeder_id = %s
              and provider_post_id is not null
            order by posted_at desc nulls last, updated_at desc, created_at desc
            limit %s
            """,
            (feeder_id, max(1, limit)),
        ).fetchall()
        return [str(row.get("provider_post_id") or "").strip() for row in rows if str(row.get("provider_post_id") or "").strip()]

    def _feeder_tracking_started_at(self, feeder_id: int) -> datetime | None:
        if feeder_id in self._feeder_tracking_started_cache:
            return self._feeder_tracking_started_cache[feeder_id]

        row = self.conn.execute(
            """
            select created_at
            from public.feeders
            where id = %s
            """,
            (feeder_id,),
        ).fetchone()
        created_at = _to_dt((row or {}).get("created_at"))
        self._feeder_tracking_started_cache[feeder_id] = created_at
        return created_at

    def _upsert_post(
        self,
        feeder_id: int,
        post_url: str,
        media_type: str,
        posted_at: datetime | None,
        caption: str | None,
        provider_post_id: str | None = None,
        thumbnail_url: str | None = None,
        video_url: str | None = None,
        carousel_urls: list[str] | None = None,
    ) -> tuple[str | None, bool]:
        tracking_started_at = self._feeder_tracking_started_at(feeder_id)
        if posted_at is not None and tracking_started_at is not None and posted_at < tracking_started_at:
            return None, False

        post_key = _post_key_from_url(post_url)
        row = self.conn.execute(
            """
            insert into public.posts (
              post_key, feeder_id, post_url, media_type, posted_at, caption,
              provider_post_id, thumbnail_url, video_url, carousel_urls,
              created_at, updated_at
            )
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,now(),now())
            on conflict (post_key)
            do update set
              feeder_id=excluded.feeder_id,
              post_url=excluded.post_url,
              media_type=coalesce(excluded.media_type, public.posts.media_type),
              posted_at=coalesce(excluded.posted_at, public.posts.posted_at),
              caption=coalesce(excluded.caption, public.posts.caption),
              provider_post_id=coalesce(excluded.provider_post_id, public.posts.provider_post_id),
              thumbnail_url=coalesce(excluded.thumbnail_url, public.posts.thumbnail_url),
              video_url=coalesce(excluded.video_url, public.posts.video_url),
              carousel_urls=case
                when excluded.carousel_urls is null then public.posts.carousel_urls
                else excluded.carousel_urls
              end,
              updated_at=now()
            returning post_key, (xmax = 0) as inserted
            """,
            (
                post_key,
                feeder_id,
                post_url,
                media_type,
                posted_at,
                caption,
                provider_post_id,
                thumbnail_url,
                video_url,
                json.dumps(carousel_urls or []),
            ),
        ).fetchone()
        if not row:
            return None, False
        return str((row or {}).get("post_key") or post_key), bool((row or {}).get("inserted"))

    def _refresh_post_media(
        self,
        post_key: str,
        thumbnail_url: str | None,
        video_url: str | None,
        carousel_urls: list[str] | None,
    ):
        self.conn.execute(
            """
            update public.posts
            set thumbnail_url = coalesce(%s, thumbnail_url),
                video_url = coalesce(%s, video_url),
                carousel_urls = case when %s::jsonb = '[]'::jsonb then carousel_urls else %s::jsonb end,
                updated_at = now()
            where post_key = %s
            """,
            (
                thumbnail_url,
                video_url,
                json.dumps(carousel_urls or []),
                json.dumps(carousel_urls or []),
                post_key,
            ),
        )

    def _upsert_metric(
        self,
        post_key: str,
        checkpoint: str,
        views: int | None,
        likes: int | None,
        comments: int | None,
        business_date_ist: date | None = None,
    ):
        """Write raw metrics only — Supabase trigger computes derived fields."""
        self.conn.execute(
            """
            insert into public.post_metrics
              (post_key, checkpoint, views, likes, comments, computed_at, business_date_ist)
            values (%s,%s,%s,%s,%s,now(),%s)
            on conflict (post_key, checkpoint)
            do update set
              views=excluded.views,
              likes=excluded.likes,
              comments=excluded.comments,
              computed_at=now(),
              business_date_ist=coalesce(excluded.business_date_ist, public.post_metrics.business_date_ist)
            """,
            (post_key, checkpoint, views, likes, comments, business_date_ist),
        )

    def _insert_metric_if_missing(
        self,
        post_key: str,
        checkpoint: str,
        views: int | None,
        likes: int | None,
        comments: int | None,
        business_date_ist: date | None = None,
    ):
        """Insert-only metric writer for fallback stamps (never overwrite existing checkpoint rows)."""
        self.conn.execute(
            """
            insert into public.post_metrics
              (post_key, checkpoint, views, likes, comments, computed_at, business_date_ist)
            values (%s,%s,%s,%s,%s,now(),%s)
            on conflict (post_key, checkpoint) do nothing
            """,
            (post_key, checkpoint, views, likes, comments, business_date_ist),
        )

    def _claim_run_jobs(self, limit: int) -> list[dict]:
        rows = self.conn.execute(
            """
            select rj.*, fd.handle
            from public.claim_run_jobs(%s) rj
            join public.feeders fd on fd.id = rj.feeder_id
            """,
            (max(1, limit),),
        ).fetchall()
        self.conn.commit()
        return rows

    def _claim_checkpoint_jobs(self, limit: int) -> list[dict]:
        self.conn.execute("select public.skip_unqualified_d21_jobs()")
        rows = self.conn.execute(
            """
            select cj.*, p.post_url, p.media_type, p.feeder_id, p.posted_at, fd.handle
                 , p.provider_post_id
            from public.claim_checkpoint_jobs(%s) cj
            join public.posts p on p.post_key = cj.post_key
            join public.feeders fd on fd.id = p.feeder_id
            """,
            (max(1, limit),),
        ).fetchall()
        self.conn.commit()
        return rows

    def _claim_web_push_jobs(self, limit: int) -> list[dict]:
        rows = self.conn.execute(
            """
            select
              jobs.*,
              alerts.created_at as fire_created_at,
              alerts.checkpoint,
              alerts.business_date_ist,
              alerts.surface_percentile,
              feeds.name as feed_name,
              users.fire_alert_threshold,
              users.pwa_push_enabled
            from public.claim_web_push_jobs(%s) jobs
            left join public.fire_alerts alerts on alerts.id = jobs.fire_alert_id
            left join public.feeds feeds on feeds.id = coalesce(jobs.feed_id, alerts.feed_id)
            left join public.users users on users.id = jobs.user_id
            """,
            (max(1, limit),),
        ).fetchall()
        self.conn.commit()
        return rows

    def _set_run_result(self, job_id: int, status: str, attempt: int | None = None, next_run_at: datetime | None = None, error: str | None = None):
        self.conn.execute(
            "select public.set_run_job_result(%s,%s,%s,%s,%s)",
            (job_id, status, attempt, next_run_at, (error or "")[:1000] if error else None),
        )
        self.conn.commit()

    def _set_checkpoint_result(self, job_id: int, status: str, attempt: int | None = None, next_run_at: datetime | None = None, error: str | None = None):
        self.conn.execute(
            "select public.set_checkpoint_job_result(%s,%s,%s,%s,%s)",
            (job_id, status, attempt, next_run_at, (error or "")[:1000] if error else None),
        )
        self.conn.commit()

    def _set_web_push_result(
        self,
        job_id: int,
        status: str,
        attempt: int | None = None,
        next_run_at: datetime | None = None,
        error: str | None = None,
    ):
        self.conn.execute(
            """
            update public.web_push_jobs
            set status = %s,
                attempt = coalesce(%s, attempt),
                next_run_at = coalesce(%s, next_run_at),
                last_error = case when %s is null then null else left(%s, 1000) end,
                sent_at = case when %s = 'sent' then now() else sent_at end,
                updated_at = now()
            where id = %s
            """,
            (status, attempt, next_run_at, error, error, status, job_id),
        )
        self.conn.commit()

    def _list_active_web_push_subscriptions(self, user_id: str) -> list[dict]:
        rows = self.conn.execute(
            """
            select id, endpoint, p256dh_key, auth_key
            from public.web_push_subscriptions
            where user_id = %s
              and enabled = true
            order by updated_at desc, id desc
            """,
            (user_id,),
        ).fetchall()
        self.conn.commit()
        return rows

    def _mark_web_push_subscription_success(self, subscription_id: int):
        self.conn.execute(
            """
            update public.web_push_subscriptions
            set last_error = null,
                failed_at = null,
                last_seen_at = now(),
                updated_at = now()
            where id = %s
            """,
            (subscription_id,),
        )
        self.conn.commit()

    def _mark_web_push_subscription_failure(self, subscription_id: int, error: str, disable: bool = False):
        self.conn.execute(
            """
            update public.web_push_subscriptions
            set enabled = case when %s then false else enabled end,
                last_error = left(%s, 1000),
                failed_at = case when %s then now() else failed_at end,
                updated_at = now()
            where id = %s
            """,
            (disable, error or "web push failed", disable, subscription_id),
        )
        self.conn.commit()

    def _resolve_for_feeder(self, feeder_id: int, checkpoint: str, business_date_ist: date | None = None):
        cp = (checkpoint or '').lower()
        if cp not in ('d1', 'd3', 'd7', 'd21'):
            return
        # Canonical resolver path: one function owns post+feed alert resolution.
        self.conn.execute("select public.fn_process_checkpoint(%s, %s, %s)", (feeder_id, cp, business_date_ist))
        self.conn.commit()

        # Pattern enrichment: attach semantic signals to freshly created/updated fire alerts.
        if pi_enabled():
            try:
                with self.conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        """SELECT id FROM public.fire_alerts
                           WHERE feeder_id = %s AND checkpoint = %s
                             AND business_date_ist = %s
                             AND pattern_signal IS NULL
                             AND signal_code = 'slot_v3'
                             AND context = 'own'
                           LIMIT 50""",
                        (feeder_id, cp, business_date_ist),
                    )
                    alert_ids = [row["id"] for row in cur.fetchall()]
                for aid in alert_ids:
                    self.conn.execute("SELECT public.fn_enrich_pattern_signal(%s)", (aid,))
                if alert_ids:
                    self.conn.commit()
            except Exception as exc:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                print(f"[pattern-enrich] failed for feeder {feeder_id}: {exc}")

    def _try_resolve_feed(self, feeder_id: int, checkpoint: str, business_date_ist: date | None = None):
        # Feed-level logic is integrated inside fn_process_checkpoint.
        return

    def _supabase_media_url(self, post_key: str, asset_role: str) -> str | None:
        """Get an authenticated URL for a cached media asset in Supabase Storage."""
        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """SELECT storage_bucket, storage_path
                   FROM public.post_media_assets
                   WHERE post_key = %s AND asset_role = %s
                     AND status = 'active'
                     AND storage_path IS NOT NULL
                   LIMIT 1""",
                (post_key, asset_role),
            )
            row = cur.fetchone()
        if not row or not row.get("storage_path"):
            return None
        bucket = row.get("storage_bucket") or SUPABASE_MEDIA_BUCKET
        path = row["storage_path"]
        return _storage_authenticated_object_url(str(bucket), str(path))

    def _get_cached_carousel_urls(self, post_key: str) -> list[str]:
        """Get authenticated URLs for cached carousel slides in Supabase Storage."""
        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """SELECT storage_bucket, storage_path, asset_role
                   FROM public.post_media_assets
                   WHERE post_key = %s
                     AND asset_role LIKE 'carousel_%%'
                     AND status = 'active'
                     AND storage_path IS NOT NULL
                   ORDER BY asset_role""",
                (post_key,),
            )
            rows = cur.fetchall()
        urls = []
        for row in rows:
            bucket = row.get("storage_bucket") or SUPABASE_MEDIA_BUCKET
            path = row["storage_path"]
            urls.append(_storage_authenticated_object_url(str(bucket), str(path)))
        return urls

    def _extract_post_intelligence_for_checkpoint(self, feeder_id: int, checkpoint: str, business_date: date | None):
        """
        Extract semantic tags for the current hot D7 batch using Supabase-cached media.
        Only processes posts from this feeder/business day that qualified as hot
        and do not already have intelligence tags.
        """
        if not pi_enabled():
            return

        # Pattern intelligence is produced as soon as a post qualifies hot at D7.
        if (checkpoint or "").lower() != "d7":
            return

        if business_date is None:
            try:
                tz = ZoneInfo(APP_TIMEZONE or "Asia/Kolkata")
            except Exception:
                tz = timezone.utc
            business_date = datetime.now(tz).date()

        # Find hot D7 posts for this feeder+business day that don't have intelligence yet.
        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """SELECT p.post_key,
                          p.caption,
                          lower(coalesce(p.media_type, 'image')) as media_type,
                          pm.percentile_performance,
                          p.posted_at
                   FROM public.posts p
                   JOIN public.post_metrics pm ON pm.post_key = p.post_key
                   WHERE p.feeder_id = %s
                     AND lower(pm.checkpoint) = 'd7'
                     AND pm.business_date_ist = %s
                     AND public.fn_is_hot_percentile(pm.percentile_performance)
                     AND NOT EXISTS (
                       SELECT 1 FROM public.post_intelligence pi
                       WHERE pi.post_key = p.post_key
                     )
                   ORDER BY pm.percentile_performance ASC, p.posted_at DESC NULLS LAST
                   LIMIT 50""",
                (feeder_id, business_date),
            )
            posts = cur.fetchall()

        for post in posts:
            post_key = str(post["post_key"])
            caption = post.get("caption") or ""
            media_type = str(post.get("media_type") or "image")

            try:
                # Resolve media URLs from Supabase cache (not Instagram CDN)
                video_url = self._supabase_media_url(post_key, "video") if media_type == "reel" else None
                carousel_urls = self._get_cached_carousel_urls(post_key) if media_type in ("sidecar", "carousel") else []
                thumbnail_url = self._supabase_media_url(post_key, "thumbnail")

                tags = pi_extract_tags(
                    caption,
                    media_type,
                    thumbnail_url=thumbnail_url,
                    video_url=video_url,
                    carousel_urls=carousel_urls if carousel_urls else None,
                    media_fetch_headers=self._storage_read_headers(),
                )
                if tags:
                    model = pi_model_version(skipped=bool(tags.get("_skipped")))
                    self.conn.execute(
                        """INSERT INTO public.post_intelligence (post_key, tags, model_version, extracted_at)
                           VALUES (%s, %s::jsonb, %s, now())
                           ON CONFLICT (post_key) DO NOTHING""",
                        (post_key, json.dumps(tags), model),
                    )
                    self.conn.commit()
                    print(f"[post-intelligence] D7 extracted tags for {post_key} ({tags.get('_visual_source', 'unknown')})")
            except Exception as exc:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                print(f"[post-intelligence] D7 extraction failed for {post_key}: {exc}")

    def process_run_jobs(self, limit: int = 120):
        if limit <= 0:
            return

        # Keep DB queue state aligned with real work: only claim jobs when we
        # have an execution slot ready for them instead of marking a large tail
        # of jobs as running while they are still waiting in the local executor.
        remaining = int(limit)
        max_workers = max(1, min(RUN_JOB_CONCURRENCY, remaining))
        futures_by_id: dict[int, Any] = {}
        future_to_id: dict[Any, int] = {}
        jobs_by_id: dict[int, dict] = {}

        def submit_job(pool: ThreadPoolExecutor, job: dict):
            handle = (job.get("handle") or "").lstrip("@")
            job_type = str(job.get("job_type") or "daily").strip().lower()
            days_window = 2 if job_type in ("daily", "repair", "poll") else 1
            recent_provider_post_ids = None if job_type == "followers" else self._recent_provider_post_ids(int(job["feeder_id"]))
            jid = int(job["id"])
            jobs_by_id[jid] = job
            future = pool.submit(run_actor_handle, handle, days_window, recent_provider_post_ids)
            futures_by_id[jid] = future
            future_to_id[future] = jid

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            while remaining > 0 and len(futures_by_id) < max_workers:
                claim_limit = min(max_workers - len(futures_by_id), remaining)
                jobs = self._claim_run_jobs(claim_limit)
                if not jobs:
                    break
                for job in jobs:
                    submit_job(pool, job)
                    remaining -= 1

            if not futures_by_id:
                return

            pending_job_ids = set(futures_by_id.keys())
            last_checkpoint_yield = time.time()
            while pending_job_ids:
                pending_futures = [futures_by_id[jid] for jid in pending_job_ids]
                done, _ = wait(pending_futures, timeout=1.0, return_when=FIRST_COMPLETED)

                if not done:
                    if time.time() - last_checkpoint_yield >= 5:
                        try:
                            self.process_checkpoint_jobs(max(1, min(CHECKPOINT_SCRAPE_CHUNK_SIZE, CHECKPOINT_JOB_CLAIM_LIMIT)))
                        except Exception:
                            try:
                                self.conn.rollback()
                            except Exception:
                                pass
                        last_checkpoint_yield = time.time()
                    continue

                for future in done:
                    jid = future_to_id.get(future)
                    if jid is None:
                        continue
                    pending_job_ids.discard(jid)
                    future_to_id.pop(future, None)
                    futures_by_id.pop(jid, None)
                    job = jobs_by_id.pop(jid)
                    att = int(job.get("attempt") or 0)
                    feeder_id = int(job["feeder_id"])
                    job_type = str(job.get("job_type") or "daily").strip().lower()

                    try:
                        items = future.result()

                        profile_pic_url = None
                        profile_followers = None
                        for probe_item in items:
                            pic, fcount = _extract_owner_profile(probe_item)
                            if not profile_pic_url and pic:
                                profile_pic_url = pic
                            if profile_followers is None and fcount is not None:
                                profile_followers = fcount
                            if profile_pic_url and profile_followers is not None:
                                break
                        self._refresh_feeder_profile(feeder_id, profile_pic_url, profile_followers)

                        if job_type == "followers":
                            if profile_pic_url is None and profile_followers is None:
                                raise RuntimeError("follower scrape returned no profile data")
                            self.conn.commit()
                            self._set_run_result(jid, "done", att, None, None)
                            continue

                        for item in items:
                            source_url = item.get("url") or ""
                            provider_post_id = str(item.get("providerPostId") or item.get("postId") or "").strip() or None
                            shortcode = (
                                str(item.get("shortCode") or item.get("shortcode") or "").strip()
                                or shortcode_from_media_id(provider_post_id)
                                or shortcode_from_url(source_url)
                            )
                            post_url = canonical_post_url(shortcode, source_url)
                            if not post_url:
                                continue

                            posted_at = _to_dt(item.get("timestamp") or item.get("takenAtTimestamp") or item.get("takenAt") or item.get("createdAt"))
                            media_type = _media_type(item)
                            caption = item.get("caption") or item.get("text") or item.get("description") or ""

                            thumbnail_url, video_url, carousel_urls = _extract_media_refs(item)
                            post_key, is_new_post = self._upsert_post(
                                feeder_id,
                                post_url,
                                media_type,
                                posted_at,
                                caption,
                                provider_post_id=provider_post_id,
                                thumbnail_url=thumbnail_url,
                                video_url=video_url,
                                carousel_urls=carousel_urls,
                            )
                            if not post_key:
                                continue
                            self._stage_post_media_assets(
                                post_key,
                                posted_at,
                                thumbnail_url,
                                video_url,
                                carousel_urls,
                            )
                            if is_new_post:
                                self.conn.execute(
                                    "select public.enqueue_checkpoint_jobs(%s,%s,%s,%s,%s,%s)",
                                    (
                                        post_key,
                                        posted_at,
                                        APP_TIMEZONE,
                                        CHECKPOINT_BATCH_HOUR_24,
                                        CHECKPOINT_BATCH_MINUTE,
                                        CHECKPOINT_BUCKET_MINUTES,
                                    ),
                                )

                        self.conn.commit()
                        self._set_run_result(jid, "done", att, None, None)
                    except Exception as exc:
                        try:
                            self.conn.rollback()
                        except Exception:
                            pass
                        err = str(exc)[:1000] or "run job failed"
                        if _is_hard_failure(err):
                            self._set_run_result(jid, "skipped", att, None, _hard_skip_error(err, "daily hard failure"))
                        else:
                            na = att + 1
                            if na <= len(RETRY_BACKOFF_MINUTES):
                                self._set_run_result(jid, "retry", na, _next_retry_time(na), err)
                            else:
                                self._set_run_result(jid, "failed", na, None, err)

                while remaining > 0 and len(pending_job_ids) < max_workers:
                    claim_limit = min(max_workers - len(pending_job_ids), remaining)
                    jobs = self._claim_run_jobs(claim_limit)
                    if not jobs:
                        break
                    for job in jobs:
                        submit_job(pool, job)
                        jid = int(job["id"])
                        pending_job_ids.add(jid)
                        remaining -= 1

                if time.time() - last_checkpoint_yield >= 5:
                    try:
                        self.process_checkpoint_jobs(max(1, min(CHECKPOINT_SCRAPE_CHUNK_SIZE, CHECKPOINT_JOB_CLAIM_LIMIT)))
                    except Exception:
                        try:
                            self.conn.rollback()
                        except Exception:
                            pass
                    last_checkpoint_yield = time.time()

    def process_checkpoint_jobs(self, limit: int = 5000):
        jobs = self._claim_checkpoint_jobs(limit)
        if not jobs:
            return

        jobs_by_post_key: dict[str, list[dict]] = defaultdict(list)
        urls_by_post_key: dict[str, str] = {}
        mode_by_post_key: dict[str, str] = {}
        for job in jobs:
            post_key = str(job.get("post_key") or "").strip().lower()
            resolved_post_url = _checkpoint_scrape_url(job)
            if not post_key or not resolved_post_url:
                continue
            jobs_by_post_key[post_key].append(job)
            urls_by_post_key.setdefault(post_key, resolved_post_url)
            mode_by_post_key.setdefault(post_key, "reel" if _is_reel_media_type(job.get("media_type")) else "post")

        ordered_post_keys = [post_key for post_key in jobs_by_post_key.keys() if urls_by_post_key.get(post_key)]
        if not ordered_post_keys:
            return

        try:
            touched: set[tuple[int, str, date]] = set()
            ordered_modes = ["post", "reel"]
            for mode in ordered_modes:
                scoped_post_keys = [post_key for post_key in ordered_post_keys if mode_by_post_key.get(post_key) == mode]
                if not scoped_post_keys:
                    continue
                for post_key_chunk in _chunk_list(scoped_post_keys, CHECKPOINT_SCRAPE_CHUNK_SIZE):
                    chunk_urls = [urls_by_post_key[post_key] for post_key in post_key_chunk]
                    items = run_actor_post_urls("", chunk_urls, mode=mode)

                    # Match by shortcode/post_key so /p vs /reel URL variants do not trigger false retries.
                    by_short: dict[str, dict] = {}
                    by_post_key: dict[str, dict] = {}
                    for item in items:
                        source_url = str(item.get("url") or "")
                        provider_post_id = str(item.get("providerPostId") or item.get("postId") or "").strip()
                        shortcode = (
                            str(item.get("shortCode") or item.get("shortcode") or "").strip()
                            or shortcode_from_media_id(provider_post_id)
                            or shortcode_from_url(source_url)
                        )
                        if shortcode:
                            by_short[shortcode.lower()] = item
                        k = _post_key_from_url(canonical_post_url(shortcode, source_url) or source_url)
                        if k:
                            by_post_key[k] = item

                    for chunk_post_key in post_key_chunk:
                        for j in jobs_by_post_key.get(chunk_post_key, []):
                            jid = int(j["id"])
                            att = int(j.get("attempt") or 0)
                            checkpoint = str(j.get("checkpoint") or "")
                            cp = checkpoint.lower()
                            job_posted_at = _to_dt(j.get("posted_at"))
                            job_post_key = str(j.get("post_key") or "").strip().lower()
                            job_post_url = str(j.get("post_url") or "")
                            job_provider_post_id = str(j.get("provider_post_id") or "").strip()
                            job_short = (shortcode_from_media_id(job_provider_post_id) or shortcode_from_url(job_post_url)).lower()

                            item = by_post_key.get(job_post_key) or (by_short.get(job_short) if job_short else None)
                            if not item:
                                try:
                                    self.conn.rollback()
                                except Exception:
                                    pass
                                self._set_checkpoint_result(
                                    jid,
                                    "skipped",
                                    att,
                                    None,
                                    _hard_skip_error("Post missing in checkpoint batch", "checkpoint hard failure"),
                                )
                                continue

                            provider_error = _checkpoint_item_error(item)
                            if provider_error:
                                try:
                                    self.conn.rollback()
                                except Exception:
                                    pass
                                self._set_checkpoint_result(
                                    jid,
                                    "skipped",
                                    att,
                                    None,
                                    _hard_skip_error(provider_error, "checkpoint hard failure"),
                                )
                                continue

                            feeder_id = int(j.get("feeder_id") or 0)
                            if cp in ("d1", "d3", "d7", "d21"):
                                business_day = _checkpoint_business_day_for_post(job_posted_at, cp)
                                if business_day is None:
                                    business_day = _checkpoint_business_day(j.get("next_run_at"))
                            else:
                                business_day = _business_date_from_job(j)
                                if business_day is None:
                                    business_day = _checkpoint_business_day(j.get("next_run_at"))

                            views, likes, comments = _extract_metrics(item)
                            if views is None and likes is None and comments is None:
                                try:
                                    self.conn.rollback()
                                except Exception:
                                    pass
                                na = att + 1
                                err = "Checkpoint scrape returned no usable metrics"
                                if na <= len(RETRY_BACKOFF_MINUTES):
                                    self._set_checkpoint_result(jid, "retry", na, _next_retry_time(na), err)
                                else:
                                    self._set_checkpoint_result(jid, "failed", na, None, err)
                                continue
                            thumbnail_url, video_url, carousel_urls = _extract_media_refs(item)
                            self._refresh_post_media(
                                str(j["post_key"]),
                                thumbnail_url,
                                video_url,
                                carousel_urls,
                            )
                            # Canonical dedupe for metrics is (post_key, checkpoint). Checkpoint re-runs
                            # update the same row while preserving the checkpoint business day stamp.
                            self._upsert_metric(str(j["post_key"]), checkpoint, views, likes, comments, business_day)
                            self.conn.commit()
                            self._set_checkpoint_result(jid, "done", att, None, None)

                            if feeder_id and cp in ("d1", "d3", "d7", "d21"):
                                touched.add((feeder_id, cp, business_day))

            # Resolver chain for checkpoint jobs once batch writes are done
            for feeder_id, cp, business_day in sorted(touched, key=lambda item: (item[2], item[0], item[1])):
                # At D7: extract AI tags from Supabase-cached media before resolving
                # (tags feed into pattern enrichment in _resolve_for_feeder)
                try:
                    if cp == "d7":
                        self._extract_post_intelligence_for_checkpoint(feeder_id, cp, business_day)
                    self._resolve_for_feeder(feeder_id, cp, business_day)
                    self._try_resolve_feed(feeder_id, cp, business_day)
                except Exception as resolve_exc:
                    try:
                        self.conn.rollback()
                    except Exception:
                        pass
                    print(
                        f"[checkpoint-resolve] failed for feeder={feeder_id} checkpoint={cp} business_day={business_day}: {resolve_exc}"
                    )

        except Exception as exc:
            try:
                self.conn.rollback()
            except Exception:
                pass
            err = str(exc)[:1000] or "checkpoint batch failed"
            is_hard = _is_hard_failure(err)
            for j in jobs:
                jid = int(j["id"])
                att = int(j.get("attempt") or 0)
                if is_hard:
                    self._set_checkpoint_result(jid, "skipped", att, None, _hard_skip_error(err, "checkpoint hard failure"))
                else:
                    na = att + 1
                    if na <= len(RETRY_BACKOFF_MINUTES):
                        self._set_checkpoint_result(jid, "retry", na, _next_retry_time(na), err)
                    else:
                        self._set_checkpoint_result(jid, "failed", na, None, err)

    def process_post_media_assets(self, capture_limit: int = 40, purge_limit: int = 80):
        if not FIRE_MEDIA_RETENTION_ENABLED:
            return

        capture_jobs = self._claim_post_media_assets_for_capture(capture_limit) if capture_limit > 0 else []
        for asset in capture_jobs:
            attempt = int(asset.get("attempt") or 0) + 1
            try:
                self._capture_post_media_asset(asset)
            except Exception as exc:
                err = str(exc)[:1000] or "post media capture failed"
                if attempt <= len(RETRY_BACKOFF_MINUTES):
                    self._set_post_media_asset_result(
                        int(asset["id"]),
                        "capture_failed",
                        attempt=attempt,
                        next_run_at=_next_retry_time(attempt),
                        error=err,
                    )
                else:
                    purge_after = _to_dt(asset.get("purge_after"))
                    final_status = "deleted" if purge_after is not None and purge_after <= datetime.now(timezone.utc) else "capture_failed"
                    if final_status == "deleted":
                        self._delete_post_media_asset_row(int(asset["id"]))
                    else:
                        self._set_post_media_asset_result(
                            int(asset["id"]),
                            final_status,
                            attempt=attempt,
                            next_run_at=None,
                            deleted_at=None,
                            error=err,
                        )

        purge_jobs = self._claim_post_media_assets_for_purge(purge_limit) if purge_limit > 0 else []
        for asset in purge_jobs:
            attempt = int(asset.get("attempt") or 0) + 1
            try:
                self._delete_post_media_asset(asset)
            except Exception as exc:
                err = str(exc)[:1000] or "post media purge failed"
                if attempt <= len(RETRY_BACKOFF_MINUTES):
                    self._set_post_media_asset_result(
                        int(asset["id"]),
                        "purge_failed",
                        attempt=attempt,
                        next_run_at=_next_retry_time(attempt),
                        error=err,
                    )
                else:
                    self._set_post_media_asset_result(
                        int(asset["id"]),
                        "purge_failed",
                        attempt=attempt,
                        next_run_at=None,
                        error=err,
                    )

    def process_web_push_jobs(self, limit: int = 200):
        if not web_push_enabled():
            return

        jobs = self._claim_web_push_jobs(limit)
        if not jobs:
            return

        batches: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for job in jobs:
            if str(job.get("kind") or "fire") == "fire":
                threshold = int(job.get("fire_alert_threshold") or 25)
                percentile = job.get("surface_percentile")
                enabled = bool(job.get("pwa_push_enabled"))
                if not enabled:
                    self._set_web_push_result(int(job["id"]), "skipped", int(job.get("attempt") or 0), None, "PWA push disabled")
                    continue
                if percentile is None or int(percentile) > threshold:
                    self._set_web_push_result(
                        int(job["id"]),
                        "skipped",
                        int(job.get("attempt") or 0),
                        None,
                        "Alert no longer meets current fire threshold",
                    )
                    continue
            batches[(str(job.get("user_id") or ""), str(job.get("kind") or "fire"))].append(job)

        for (user_id, kind), batch in batches.items():
            subscriptions = self._list_active_web_push_subscriptions(user_id)
            if not subscriptions:
                for job in batch:
                    self._set_web_push_result(int(job["id"]), "skipped", int(job.get("attempt") or 0), None, "No active web push subscriptions")
                continue

            if kind == "test":
                for job in batch:
                    payload = job.get("payload") if isinstance(job.get("payload"), dict) else {}
                    if not payload:
                        self._set_web_push_result(int(job["id"]), "skipped", int(job.get("attempt") or 0), None, "Missing test push payload")
                        continue
                    ok = False
                    last_error = ""
                    last_status: int | None = None
                    for subscription in subscriptions:
                        success, error, status_code = send_web_push(
                            {
                                "endpoint": subscription["endpoint"],
                                "keys": {
                                    "p256dh": subscription["p256dh_key"],
                                    "auth": subscription["auth_key"],
                                },
                            },
                            payload,
                        )
                        if success:
                            ok = True
                            self._mark_web_push_subscription_success(int(subscription["id"]))
                        else:
                            last_error = error or "web push failed"
                            last_status = status_code
                            self._mark_web_push_subscription_failure(
                                int(subscription["id"]),
                                last_error,
                                disable=status_code in (404, 410),
                            )
                    if ok:
                        self._set_web_push_result(int(job["id"]), "sent", int(job.get("attempt") or 0), None, None)
                    else:
                        attempt = int(job.get("attempt") or 0) + 1
                        if _should_retry_web_push(last_status, last_error) and attempt <= len(RETRY_BACKOFF_MINUTES):
                            self._set_web_push_result(int(job["id"]), "retry", attempt, _next_retry_time(attempt), last_error)
                        else:
                            self._set_web_push_result(int(job["id"]), "failed", attempt, None, last_error or "web push failed")
                continue

            threshold = int(batch[0].get("fire_alert_threshold") or 25)
            payload = _build_fire_push_payload(batch, threshold)
            sent = False
            last_error = ""
            last_status: int | None = None

            for subscription in subscriptions:
                success, error, status_code = send_web_push(
                    {
                        "endpoint": subscription["endpoint"],
                        "keys": {
                            "p256dh": subscription["p256dh_key"],
                            "auth": subscription["auth_key"],
                        },
                    },
                    payload,
                )
                if success:
                    sent = True
                    self._mark_web_push_subscription_success(int(subscription["id"]))
                else:
                    last_error = error or "web push failed"
                    last_status = status_code
                    self._mark_web_push_subscription_failure(
                        int(subscription["id"]),
                        last_error,
                        disable=status_code in (404, 410),
                    )

            if sent:
                for job in batch:
                    self._set_web_push_result(int(job["id"]), "sent", int(job.get("attempt") or 0), None, None)
                continue

            for job in batch:
                attempt = int(job.get("attempt") or 0) + 1
                if _should_retry_web_push(last_status, last_error) and attempt <= len(RETRY_BACKOFF_MINUTES):
                    self._set_web_push_result(int(job["id"]), "retry", attempt, _next_retry_time(attempt), last_error)
                else:
                    self._set_web_push_result(int(job["id"]), "failed", attempt, None, last_error or "web push failed")


    def backfill_d1_media(self, limit: int = 300, days: int = 14, batch_size: int = 50) -> dict[str, int]:
        """Backfill missing media refs and cached asset rows for existing D1 posts."""
        rows = self.conn.execute(
            """
            select p.post_key, p.post_url, p.posted_at
            from public.posts p
            join public.post_metrics pm on pm.post_key = p.post_key and pm.checkpoint = 'd1'
            where p.post_url is not null
              and p.created_at >= now() - (%s::text || ' days')::interval
              and (
                p.thumbnail_url is null
                or not exists (
                  select 1
                  from public.post_media_assets assets
                  where assets.post_key = p.post_key
                )
              )
            order by p.created_at desc
            limit %s
            """,
            (max(1, days), max(1, limit)),
        ).fetchall()
        self.conn.commit()

        if not rows:
            return {"selected": 0, "updated": 0, "missing": 0}

        by_url: dict[str, dict] = {}
        urls = [str(r.get("post_url") or "").strip() for r in rows if str(r.get("post_url") or "").strip()]

        for i in range(0, len(urls), max(1, batch_size)):
            chunk = urls[i : i + max(1, batch_size)]
            if not chunk:
                continue
            items = run_actor_post_urls("", chunk)
            for item in items:
                source_url = item.get("url") or ""
                provider_post_id = str(item.get("providerPostId") or item.get("postId") or "").strip()
                shortcode = (
                    str(item.get("shortCode") or item.get("shortcode") or "").strip()
                    or shortcode_from_media_id(provider_post_id)
                    or shortcode_from_url(source_url)
                )
                canonical = canonical_post_url(shortcode, source_url)
                if canonical:
                    by_url[canonical] = item

        updated = 0
        missing = 0
        for r in rows:
            post_key = str(r.get("post_key") or "")
            post_url = str(r.get("post_url") or "")
            posted_at = _to_dt(r.get("posted_at"))
            item = by_url.get(post_url)
            if not item:
                missing += 1
                continue
            thumbnail_url, video_url, carousel_urls = _extract_media_refs(item)
            self._refresh_post_media(post_key, thumbnail_url, video_url, carousel_urls)
            self._stage_post_media_assets(
                post_key,
                posted_at,
                thumbnail_url,
                video_url,
                carousel_urls,
            )
            updated += 1

        self.conn.commit()
        return {"selected": len(rows), "updated": updated, "missing": missing}

    def restore_missing_d7_fire_thumbnails(self, limit: int = 250, days: int = 21, batch_size: int = 25) -> dict[str, int]:
        """Repair D7 Fire posts missing a thumbnail ref or an active cached thumbnail asset."""
        rows = self.conn.execute(
            """
            select distinct on (p.post_key)
              p.post_key,
              p.post_url,
              p.posted_at,
              lower(coalesce(p.media_type, 'image')) as media_type
            from public.posts p
            join public.fire_alerts fa
              on fa.post_key = p.post_key
             and lower(fa.checkpoint) = 'd7'
             and fa.status not in ('dropped', 'error', 'archived')
            where coalesce(p.post_url, '') <> ''
              and coalesce(p.posted_at, p.created_at, now()) >= now() - (%s::text || ' days')::interval
              and (
                coalesce(p.thumbnail_url, '') = ''
                or not exists (
                  select 1
                  from public.post_media_assets assets
                  where assets.post_key = p.post_key
                    and assets.asset_role = 'thumbnail'
                    and assets.status = 'active'
                    and coalesce(assets.storage_path, '') <> ''
                )
              )
            order by p.post_key, fa.created_at desc
            limit %s
            """,
            (max(1, days), max(1, limit)),
        ).fetchall()
        self.conn.commit()

        if not rows:
            return {"selected": 0, "updated": 0, "missing": 0}

        rows_by_post_key: dict[str, dict] = {}
        urls_by_post_key: dict[str, str] = {}
        mode_by_post_key: dict[str, str] = {}
        for row in rows:
            post_key = str(row.get("post_key") or "").strip().lower()
            post_url = str(row.get("post_url") or "").strip()
            if not post_key or not post_url:
                continue
            rows_by_post_key[post_key] = row
            urls_by_post_key[post_key] = post_url
            mode_by_post_key[post_key] = "reel" if _is_reel_media_type(row.get("media_type")) else "post"

        if not rows_by_post_key:
            return {"selected": len(rows), "updated": 0, "missing": len(rows)}

        updated = 0
        missing = 0
        ordered_post_keys = list(rows_by_post_key.keys())

        for mode in ("post", "reel"):
            scoped_post_keys = [post_key for post_key in ordered_post_keys if mode_by_post_key.get(post_key) == mode]
            if not scoped_post_keys:
                continue

            for post_key_chunk in _chunk_list(scoped_post_keys, max(1, batch_size)):
                chunk_urls = [urls_by_post_key[post_key] for post_key in post_key_chunk]
                items = run_actor_post_urls("", chunk_urls, mode=mode)

                by_short: dict[str, dict] = {}
                by_post_key: dict[str, dict] = {}
                for item in items:
                    source_url = str(item.get("url") or "").strip()
                    provider_post_id = str(item.get("providerPostId") or item.get("postId") or "").strip()
                    shortcode = (
                        str(item.get("shortCode") or item.get("shortcode") or "").strip()
                        or shortcode_from_media_id(provider_post_id)
                        or shortcode_from_url(source_url)
                    )
                    if shortcode:
                        by_short[shortcode.lower()] = item
                    canonical = canonical_post_url(shortcode, source_url) or source_url
                    item_post_key = _post_key_from_url(canonical)
                    if item_post_key:
                        by_post_key[item_post_key] = item

                for chunk_post_key in post_key_chunk:
                    row = rows_by_post_key[chunk_post_key]
                    post_url = str(row.get("post_url") or "").strip()
                    short = shortcode_from_url(post_url).lower()
                    item = by_post_key.get(chunk_post_key) or (by_short.get(short) if short else None)
                    if not item:
                        missing += 1
                        continue

                    posted_at = _to_dt(row.get("posted_at"))
                    thumbnail_url, video_url, carousel_urls = _extract_media_refs(item)
                    self._refresh_post_media(chunk_post_key, thumbnail_url, video_url, carousel_urls)
                    self._stage_post_media_assets(
                        chunk_post_key,
                        posted_at,
                        thumbnail_url,
                        video_url,
                        carousel_urls,
                    )
                    updated += 1

        self.conn.commit()
        if updated > 0:
            self.process_post_media_assets(capture_limit=max(40, updated * 3), purge_limit=0)

        return {"selected": len(rows), "updated": updated, "missing": missing}



def run_once(run_limit: int = 120, checkpoint_limit: int | None = None):
    eng = PureEngine()
    try:
        effective_checkpoint_limit = max(1, int(checkpoint_limit or CHECKPOINT_JOB_CLAIM_LIMIT))
        eng.ensure_connection("run_once start", verify=True)
        eng.requeue_stale(STALE_JOB_MINUTES)
        eng.ensure_connection("before run jobs", verify=True)
        eng.process_run_jobs(run_limit)
        eng.ensure_connection("before checkpoint jobs", verify=True)
        eng.process_checkpoint_jobs(effective_checkpoint_limit)
        eng.ensure_connection("before media assets", verify=True)
        eng.process_post_media_assets()
        eng.ensure_connection("before web push jobs", verify=True)
        eng.process_web_push_jobs()
        eng.ensure_connection("before retention cleanup", verify=True)
        eng.prune_expired_fire_state()
    finally:
        eng.close()


def run_worker(loop_sleep_seconds: int = 2, run_limit: int = 120, checkpoint_limit: int | None = None):
    from .telegram import (
        alert_worker_started, alert_worker_error, alert_permanently_failed,
        alert_job_failed, alert_job_skipped, alert_daily_summary, is_enabled as tg_enabled,
    )

    eng = PureEngine()
    last_watchdog = 0.0
    last_dead_check = 0.0
    last_retention_cleanup = 0.0
    last_summary_date = ""
    effective_checkpoint_limit = max(1, int(checkpoint_limit or CHECKPOINT_JOB_CLAIM_LIMIT))

    alert_worker_started()
    if tg_enabled():
        print("[worker] Telegram alerting enabled")
    print(
        f"[worker] checkpoint_claim_limit={effective_checkpoint_limit} "
        f"checkpoint_chunk_size={CHECKPOINT_SCRAPE_CHUNK_SIZE}"
    )

    try:
        while True:
            now_ts = time.time()

            try:
                eng.ensure_connection("worker loop heartbeat", verify=True)
            except Exception as e:
                print(f"[db] reconnect failed: {e}")
                alert_worker_error(e)
                time.sleep(10)
                continue

            # Ensure we never carry an aborted transaction into the next loop.
            try:
                eng.conn.rollback()
            except Exception:
                pass

            # Watchdog: requeue stale jobs every 60s
            if now_ts - last_watchdog >= 60:
                try:
                    eng.ensure_connection("watchdog", verify=True)
                    eng.requeue_stale(STALE_JOB_MINUTES)
                except Exception as e:
                    try:
                        eng.conn.rollback()
                    except Exception:
                        pass
                    if _is_connection_error(e):
                        try:
                            eng._reconnect(f"watchdog: {e}")
                        except Exception as reconnect_exc:
                            print(f"[db] watchdog reconnect failed: {reconnect_exc}")
                    print(f"[watchdog] {e}")
                    alert_worker_error(e)
                last_watchdog = now_ts

            # Failed job alerts: check every 60s for freshly failed jobs
            if now_ts - last_dead_check >= 60:
                try:
                    eng.ensure_connection("dead-check", verify=True)
                    with eng.conn.cursor(row_factory=dict_row) as cur:
                        # Instant alert: any run_job that just failed
                        cur.execute("""
                            select rj.id, rj.status, rj.attempt, rj.last_error, f.handle
                            from public.run_jobs rj
                            join public.feeders f on f.id = rj.feeder_id
                            where rj.updated_at > now() - interval '2 minutes'
                              and (
                                rj.status = 'failed'
                                or (
                                  rj.status = 'retry'
                                  and coalesce(rj.last_error, '') <> ''
                                  and coalesce(rj.last_error, '') not ilike 'Watchdog:%'
                                  and coalesce(rj.last_error, '') not ilike 'Recovered stale%'
                                )
                                or (rj.status = 'skipped' and coalesce(rj.last_error, '') like 'hard-skip:%')
                              )
                        """)
                        for row in cur.fetchall():
                            if row.get("status") == 'skipped':
                                reason = str(row.get("last_error", "")).replace('hard-skip:', '', 1).strip()
                                alert_job_skipped("run", row["id"], row["handle"], reason)
                            elif row.get("status") == 'failed' and int(row.get("attempt") or 0) >= len(RETRY_BACKOFF_MINUTES):
                                alert_permanently_failed("run", row["id"], row["handle"], row.get("last_error", ""))
                            else:
                                alert_job_failed("run", row["id"], row["handle"], row.get("attempt", 0), row.get("last_error", ""))

                        # Instant alert: any checkpoint_job that just failed
                        cur.execute("""
                            select cj.id, cj.status, cj.attempt, cj.last_error, cj.checkpoint, f.handle
                            from public.checkpoint_jobs cj
                            join public.posts p on p.post_key = cj.post_key
                            join public.feeders f on f.id = p.feeder_id
                            where cj.updated_at > now() - interval '2 minutes'
                              and (
                                cj.status = 'failed'
                                or (
                                  cj.status = 'retry'
                                  and coalesce(cj.last_error, '') <> ''
                                  and coalesce(cj.last_error, '') not ilike 'Watchdog:%'
                                  and coalesce(cj.last_error, '') not ilike 'Recovered stale%'
                                )
                                or (cj.status = 'skipped' and coalesce(cj.last_error, '') like 'hard-skip:%')
                              )
                        """)
                        for row in cur.fetchall():
                            if row.get("status") == 'skipped':
                                reason = str(row.get("last_error", "")).replace('hard-skip:', '', 1).strip()
                                alert_job_skipped(
                                    f"checkpoint_{row['checkpoint']}", row["id"], row["handle"], reason
                                )
                            elif row.get("status") == 'failed' and int(row.get("attempt") or 0) >= len(RETRY_BACKOFF_MINUTES):
                                alert_permanently_failed(
                                    f"checkpoint_{row['checkpoint']}", row["id"],
                                    row["handle"], row.get("last_error", ""),
                                )
                            else:
                                alert_job_failed(
                                    f"checkpoint_{row['checkpoint']}", row["id"],
                                    row["handle"], row.get("attempt", 0), row.get("last_error", ""),
                                )
                except Exception as e:
                    if _is_connection_error(e):
                        try:
                            eng._reconnect(f"dead-check: {e}")
                        except Exception as reconnect_exc:
                            print(f"[db] dead-check reconnect failed: {reconnect_exc}")
                    print(f"[dead-check] {e}")
                last_dead_check = now_ts

            # Daily summary at 8:00 AM IST
            today = datetime.now(ZoneInfo(APP_TIMEZONE)).strftime("%Y-%m-%d")
            now_ist = datetime.now(ZoneInfo(APP_TIMEZONE))
            if today != last_summary_date and now_ist.hour == 8 and now_ist.minute >= 0:
                try:
                    eng.ensure_connection("daily-summary", verify=True)
                    with eng.conn.cursor(row_factory=dict_row) as cur:
                        cur.execute("""
                            select
                              count(*) filter (where status='done') as ok,
                              count(*) filter (where status='failed') as fail,
                              count(*) filter (where status in ('pending','retry')) as pending
                            from public.run_jobs
                            where updated_at > now() - interval '24 hours'
                        """)
                        r = cur.fetchone() or {}
                        cur.execute("""
                            select
                              count(*) filter (where status='done') as ok,
                              count(*) filter (where status='failed') as fail,
                              count(*) filter (where status in ('pending','retry')) as pending
                            from public.checkpoint_jobs
                            where updated_at > now() - interval '24 hours'
                        """)
                        c = cur.fetchone() or {}
                        alert_daily_summary(
                            r.get("ok", 0), r.get("fail", 0),
                            c.get("ok", 0), c.get("fail", 0),
                            r.get("pending", 0), c.get("pending", 0),
                        )
                except Exception as e:
                    if _is_connection_error(e):
                        try:
                            eng._reconnect(f"daily-summary: {e}")
                        except Exception as reconnect_exc:
                            print(f"[db] daily-summary reconnect failed: {reconnect_exc}")
                    print(f"[daily-summary] {e}")
                last_summary_date = today

            if now_ts - last_retention_cleanup >= 3600:
                try:
                    eng.ensure_connection("retention-cleanup", verify=True)
                    eng.prune_expired_fire_state()
                except Exception as e:
                    try:
                        eng.conn.rollback()
                    except Exception:
                        pass
                    if _is_connection_error(e):
                        try:
                            eng._reconnect(f"retention-cleanup: {e}")
                        except Exception as reconnect_exc:
                            print(f"[db] retention-cleanup reconnect failed: {reconnect_exc}")
                    print(f"[retention-cleanup] {e}")
                last_retention_cleanup = now_ts

            # Process jobs
            try:
                eng.ensure_connection("before job processing", verify=True)
                eng.ensure_connection("before checkpoint processing", verify=True)
                eng.process_checkpoint_jobs(effective_checkpoint_limit)
                eng.ensure_connection("before run processing", verify=True)
                eng.process_run_jobs(run_limit)
                eng.ensure_connection("after run checkpoint processing", verify=True)
                eng.process_checkpoint_jobs(effective_checkpoint_limit)
                eng.ensure_connection("before media processing", verify=True)
                eng.process_post_media_assets()
                eng.ensure_connection("before web push processing", verify=True)
                eng.process_web_push_jobs()
            except Exception as e:
                try:
                    eng.conn.rollback()
                except Exception:
                    pass
                if _is_connection_error(e):
                    try:
                        eng._reconnect(f"worker-loop: {e}")
                    except Exception as reconnect_exc:
                        print(f"[db] worker-loop reconnect failed: {reconnect_exc}")
                print(f"[worker-loop] error: {e}")
                alert_worker_error(e)
                time.sleep(10)  # back off on error

            time.sleep(max(1, int(loop_sleep_seconds)))
    finally:
        eng.close()
