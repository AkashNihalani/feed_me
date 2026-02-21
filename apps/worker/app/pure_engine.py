from __future__ import annotations

import re
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta, date
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
from dateutil import parser as date_parser
from psycopg.rows import dict_row

from .apify import run_actor_handle, run_actor_post_urls
from .config import POSTGRES_DSN, RETRY_BACKOFF_MINUTES, APP_TIMEZONE, RUN_JOB_CONCURRENCY


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
)
_HARD_FAILURE_TOKENS = (
    'not found',
    'private',
    'deleted',
    'post missing',
    'missing in checkpoint batch',
    'missing post',
)


def _normalize_error(err: str | None) -> str:
    return (err or '').strip().lower()


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
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _shortcode_from_url(url: str) -> str:
    m = re.search(r"/(?:p|reel|tv)/([A-Za-z0-9_-]+)", (url or "").strip(), flags=re.IGNORECASE)
    return m.group(1).lower() if m else ""


def _canonical_post_url(shortcode: str, fallback_url: str = "") -> str:
    if shortcode:
        return f"https://www.instagram.com/p/{shortcode.strip()}/"
    u = (fallback_url or "").strip()
    return u.split("?", 1)[0].split("#", 1)[0] if u else ""


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
    views = _to_int(item.get("videoViewCount") or item.get("videoPlayCount") or item.get("views") or item.get("viewCount"))
    likes = _to_int(item.get("likesCount") or item.get("likes") or item.get("likeCount"))
    comments = _to_int(item.get("commentsCount") or item.get("comments") or item.get("commentCount"))
    return views, likes, comments

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


def _extract_media_refs(item: dict) -> tuple[str | None, str | None, str | None, list[str], str | None]:
    """Extract reusable media references for UI thumbnails and downstream vector jobs."""
    display_url = (
        item.get("displayUrl")
        or item.get("display_url")
        or item.get("imageUrl")
        or item.get("thumbnailUrl")
        or item.get("thumbnailSrc")
    )
    thumbnail_url = item.get("thumbnailUrl") or item.get("thumbnailSrc") or display_url
    video_url = item.get("videoUrl") or item.get("video_url")
    audio_url = item.get("audioUrl") or item.get("audio_url")

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

    return _clean(thumbnail_url), _clean(display_url), _clean(video_url), carousel_urls, _clean(audio_url)


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


class PureEngine:
    def __init__(self):
        self.conn = psycopg.connect(POSTGRES_DSN, row_factory=dict_row)

    def close(self):
        self.conn.close()

    def enqueue_daily(self) -> int:
        r = self.conn.execute("select public.enqueue_daily_jobs(now()) as c").fetchone()
        self.conn.commit()
        return int((r or {}).get("c") or 0)

    def enqueue_weekly(self) -> int:
        r = self.conn.execute("select public.enqueue_weekly_jobs(now()) as c").fetchone()
        self.conn.commit()
        return int((r or {}).get("c") or 0)

    def requeue_stale(self, minutes: int = 30):
        self.conn.execute("select * from public.requeue_stale_jobs(%s)", (max(1, minutes),)).fetchone()
        self.conn.commit()



    def _refresh_feeder_profile(self, feeder_id: int, profile_pic_url: str | None, follower_count: int | None):
        self.conn.execute(
            """
            update public.feeders
            set profile_pic_url = coalesce(%s, profile_pic_url),
                follower_count = coalesce(%s, follower_count),
                profile_pic_fetched_at = now(),
                updated_at = now()
            where id = %s
            """,
            (profile_pic_url, follower_count, feeder_id),
        )

    def _upsert_post(
        self,
        feeder_id: int,
        post_url: str,
        media_type: str,
        posted_at: datetime | None,
        caption: str | None,
        thumbnail_url: str | None = None,
        display_url: str | None = None,
        video_url: str | None = None,
        carousel_urls: list[str] | None = None,
        audio_url: str | None = None,
    ) -> str:
        post_key = _post_key_from_url(post_url)
        self.conn.execute(
            """
            insert into public.posts (
              post_key, feeder_id, post_url, media_type, posted_at, caption,
              thumbnail_url, display_url, video_url, carousel_urls, audio_url, media_fetched_at,
              created_at, updated_at
            )
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,now(),now(),now())
            on conflict (post_key)
            do update set
              feeder_id=excluded.feeder_id,
              post_url=excluded.post_url,
              media_type=coalesce(excluded.media_type, public.posts.media_type),
              posted_at=coalesce(excluded.posted_at, public.posts.posted_at),
              caption=coalesce(excluded.caption, public.posts.caption),
              thumbnail_url=coalesce(excluded.thumbnail_url, public.posts.thumbnail_url),
              display_url=coalesce(excluded.display_url, public.posts.display_url),
              video_url=coalesce(excluded.video_url, public.posts.video_url),
              carousel_urls=case
                when excluded.carousel_urls is null then public.posts.carousel_urls
                else excluded.carousel_urls
              end,
              audio_url=coalesce(excluded.audio_url, public.posts.audio_url),
              media_fetched_at=coalesce(excluded.media_fetched_at, public.posts.media_fetched_at),
              updated_at=now()
            """,
            (
                post_key,
                feeder_id,
                post_url,
                media_type,
                posted_at,
                caption,
                thumbnail_url,
                display_url,
                video_url,
                json.dumps(carousel_urls or []),
                audio_url,
            ),
        )
        return post_key

    def _refresh_post_media(
        self,
        post_key: str,
        thumbnail_url: str | None,
        display_url: str | None,
        video_url: str | None,
        carousel_urls: list[str] | None,
        audio_url: str | None,
    ):
        self.conn.execute(
            """
            update public.posts
            set thumbnail_url = coalesce(%s, thumbnail_url),
                display_url = coalesce(%s, display_url),
                video_url = coalesce(%s, video_url),
                carousel_urls = case when %s::jsonb = '[]'::jsonb then carousel_urls else %s::jsonb end,
                audio_url = coalesce(%s, audio_url),
                media_fetched_at = now(),
                updated_at = now()
            where post_key = %s
            """,
            (
                thumbnail_url,
                display_url,
                video_url,
                json.dumps(carousel_urls or []),
                json.dumps(carousel_urls or []),
                audio_url,
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
        captured_business_date_ist: date | None = None,
        d1_source: str | None = None,
    ):
        """Write raw metrics only — Supabase trigger computes derived fields."""
        self.conn.execute(
            """
            insert into public.post_metrics
              (post_key, checkpoint, views, likes, comments, computed_at, captured_business_date_ist, d1_source)
            values (%s,%s,%s,%s,%s,now(),%s,%s)
            on conflict (post_key, checkpoint)
            do update set
              views=excluded.views,
              likes=excluded.likes,
              comments=excluded.comments,
              computed_at=now(),
              captured_business_date_ist=coalesce(excluded.captured_business_date_ist, public.post_metrics.captured_business_date_ist),
              d1_source=coalesce(excluded.d1_source, public.post_metrics.d1_source)
            """,
            (post_key, checkpoint, views, likes, comments, captured_business_date_ist, d1_source),
        )

    def _insert_metric_if_missing(
        self,
        post_key: str,
        checkpoint: str,
        views: int | None,
        likes: int | None,
        comments: int | None,
        captured_business_date_ist: date | None = None,
        d1_source: str | None = None,
    ):
        """Insert-only metric writer for fallback stamps (never overwrite existing checkpoint rows)."""
        self.conn.execute(
            """
            insert into public.post_metrics
              (post_key, checkpoint, views, likes, comments, computed_at, captured_business_date_ist, d1_source)
            values (%s,%s,%s,%s,%s,now(),%s,%s)
            on conflict (post_key, checkpoint) do nothing
            """,
            (post_key, checkpoint, views, likes, comments, captured_business_date_ist, d1_source),
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
            select cj.*, p.post_url, p.media_type, p.feeder_id, fd.handle
            from public.claim_checkpoint_jobs(%s) cj
            join public.posts p on p.post_key = cj.post_key
            join public.feeders fd on fd.id = p.feeder_id
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

    def _resolve_for_feeder(self, feeder_id: int, checkpoint: str, business_date_ist: date | None = None):
        cp = (checkpoint or '').lower()
        if cp not in ('d1', 'd3', 'd7', 'd21'):
            return
        self.conn.execute("select public.fn_refresh_feeder_baselines(%s, %s)", (feeder_id, cp))
        self.conn.execute("select public.fn_resolve_post_signals(%s, %s)", (feeder_id, cp))
        self.conn.execute("select public.enqueue_slot_state_alerts(%s, %s, %s)", (feeder_id, cp, business_date_ist))
        self.conn.commit()

    def _try_resolve_feed(self, feeder_id: int, checkpoint: str, business_date_ist: date | None = None):
        cp = (checkpoint or '').lower()
        if cp not in ('d1', 'd3', 'd7', 'd21'):
            return
        row = self.conn.execute("select feed_id from public.feeders where id = %s", (feeder_id,)).fetchone()
        feed_id = int((row or {}).get('feed_id') or 0)
        if not feed_id:
            self.conn.commit()
            return
        self.conn.execute("select public.fn_try_resolve_feed_signals(%s, %s, %s)", (feed_id, cp, business_date_ist))
        self.conn.commit()

    def process_run_jobs(self, limit: int = 120):
        jobs = self._claim_run_jobs(limit)
        if not jobs:
            return

        # Fan out all feeder scrapes at once so nightly burst starts simultaneously.
        futures = {}
        max_workers = max(1, RUN_JOB_CONCURRENCY)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for job in jobs:
                handle = (job.get("handle") or "").lstrip("@")
                job_type = str(job.get("job_type") or "daily")
                days_window = 3 if job_type == "repair" else 2
                futures[int(job["id"])] = pool.submit(run_actor_handle, handle, days_window)

            for job in jobs:
                jid = int(job["id"])
                att = int(job.get("attempt") or 0)
                feeder_id = int(job["feeder_id"])
                business_date_ist = _business_date_from_job(job)

                try:
                    items = futures[jid].result()

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

                    for item in items:
                        source_url = item.get("url") or ""
                        shortcode = item.get("shortCode") or item.get("shortcode") or _shortcode_from_url(source_url)
                        post_url = _canonical_post_url(shortcode, source_url)
                        if not post_url:
                            continue

                        posted_at = _to_dt(item.get("timestamp") or item.get("takenAtTimestamp") or item.get("takenAt") or item.get("createdAt"))
                        daily_checkpoint = _daily_checkpoint_for_post(posted_at, business_date_ist)
                        if not daily_checkpoint:
                            continue
                        media_type = _media_type(item)
                        caption = item.get("caption") or item.get("text") or item.get("description") or ""
                        views, likes, comments = _extract_metrics(item)

                        thumbnail_url, display_url, video_url, carousel_urls, audio_url = _extract_media_refs(item)
                        post_key = self._upsert_post(
                            feeder_id,
                            post_url,
                            media_type,
                            posted_at,
                            caption,
                            thumbnail_url=thumbnail_url,
                            display_url=display_url,
                            video_url=video_url,
                            carousel_urls=carousel_urls,
                            audio_url=audio_url,
                        )
                        self._upsert_metric(post_key, daily_checkpoint, views, likes, comments, business_date_ist, 'on_time' if daily_checkpoint == 'd1' else None)

                        # Buffer capture fallback: if this post was first seen in D2, stamp D1 once
                        # (user-facing timeline remains D1/D3/D7/D21 while backend keeps D2 for audit).
                        if daily_checkpoint == "d2":
                            self._insert_metric_if_missing(post_key, "d1", views, likes, comments, business_date_ist, 'from_d2b')

                        self.conn.execute("select public.enqueue_checkpoint_jobs(%s,%s)", (post_key, posted_at))

                    self.conn.commit()
                    self._set_run_result(jid, "done", att, None, None)

                    # Post-ingest resolver chain (baseline -> post signals -> feed signals)
                    self._resolve_for_feeder(feeder_id, 'd1', business_date_ist)
                    self._try_resolve_feed(feeder_id, 'd1', business_date_ist)
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

    def process_checkpoint_jobs(self, limit: int = 5000):
        jobs = self._claim_checkpoint_jobs(limit)
        if not jobs:
            return

        # Single batch call for all due jobs in this claim cycle.
        all_urls = list({str(j.get("post_url") or "").strip() for j in jobs if j.get("post_url")})
        if not all_urls:
            return

        try:
            items = run_actor_post_urls("", all_urls)

            # Match by shortcode/post_key so /p vs /reel URL variants do not trigger false retries.
            by_short: dict[str, dict] = {}
            by_post_key: dict[str, dict] = {}
            for item in items:
                source_url = str(item.get("url") or "")
                shortcode = (
                    str(item.get("shortCode") or item.get("shortcode") or "").strip().lower()
                    or _shortcode_from_url(source_url)
                )
                if shortcode:
                    by_short[shortcode] = item
                k = _post_key_from_url(_canonical_post_url(shortcode, source_url) or source_url)
                if k:
                    by_post_key[k] = item

            touched: set[tuple[int, str]] = set()
            for j in jobs:
                jid = int(j["id"])
                att = int(j.get("attempt") or 0)
                checkpoint = str(j.get("checkpoint") or "")

                job_post_key = str(j.get("post_key") or "").strip().lower()
                job_post_url = str(j.get("post_url") or "")
                job_short = _shortcode_from_url(job_post_url)

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

                views, likes, comments = _extract_metrics(item)
                thumbnail_url, display_url, video_url, carousel_urls, audio_url = _extract_media_refs(item)
                self._refresh_post_media(
                    str(j["post_key"]),
                    thumbnail_url,
                    display_url,
                    video_url,
                    carousel_urls,
                    audio_url,
                )
                self._upsert_metric(str(j["post_key"]), checkpoint, views, likes, comments, None, None)
                self.conn.commit()
                self._set_checkpoint_result(jid, "done", att, None, None)

                feeder_id = int(j.get("feeder_id") or 0)
                cp = checkpoint.lower()
                if feeder_id and cp in ("d3", "d7", "d21"):
                    touched.add((feeder_id, cp))

            # Resolver chain for checkpoint jobs once batch writes are done
            for feeder_id, cp in touched:
                self._resolve_for_feeder(feeder_id, cp)
                try:
                    tz = ZoneInfo(APP_TIMEZONE or "Asia/Kolkata")
                except Exception:
                    tz = timezone.utc
                self._try_resolve_feed(feeder_id, cp, datetime.now(tz).date())

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


    def backfill_d1_media(self, limit: int = 300, days: int = 14, batch_size: int = 50) -> dict[str, int]:
        """Backfill missing media refs for existing D1 posts without creating new metrics/checkpoints."""
        rows = self.conn.execute(
            """
            select p.post_key, p.post_url
            from public.posts p
            join public.post_metrics pm on pm.post_key = p.post_key and pm.checkpoint = 'd1'
            where p.post_url is not null
              and p.created_at >= now() - (%s::text || ' days')::interval
              and (
                p.media_fetched_at is null
                or (p.thumbnail_url is null and p.display_url is null)
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
                shortcode = item.get("shortCode") or item.get("shortcode") or _shortcode_from_url(source_url)
                canonical = _canonical_post_url(shortcode, source_url)
                if canonical:
                    by_url[canonical] = item

        updated = 0
        missing = 0
        for r in rows:
            post_key = str(r.get("post_key") or "")
            post_url = str(r.get("post_url") or "")
            item = by_url.get(post_url)
            if not item:
                missing += 1
                continue
            thumbnail_url, display_url, video_url, carousel_urls, audio_url = _extract_media_refs(item)
            self._refresh_post_media(post_key, thumbnail_url, display_url, video_url, carousel_urls, audio_url)
            updated += 1

        self.conn.commit()
        return {"selected": len(rows), "updated": updated, "missing": missing}



def run_once(run_limit: int = 120, checkpoint_limit: int = 5000):
    eng = PureEngine()
    try:
        eng.requeue_stale(30)
        eng.process_run_jobs(run_limit)
        eng.process_checkpoint_jobs(checkpoint_limit)
    finally:
        eng.close()


def run_worker(loop_sleep_seconds: int = 2, run_limit: int = 120, checkpoint_limit: int = 5000):
    from .telegram import (
        alert_worker_started, alert_worker_error, alert_permanently_failed,
        alert_job_failed, alert_job_skipped, alert_daily_summary, is_enabled as tg_enabled,
    )

    eng = PureEngine()
    last_watchdog = 0.0
    last_dead_check = 0.0
    last_summary_date = ""

    alert_worker_started()
    if tg_enabled():
        print("[worker] Telegram alerting enabled")

    try:
        while True:
            now_ts = time.time()

            # Ensure we never carry an aborted transaction into the next loop.
            try:
                eng.conn.rollback()
            except Exception:
                pass

            # Watchdog: requeue stale jobs every 60s
            if now_ts - last_watchdog >= 60:
                try:
                    eng.requeue_stale(30)
                except Exception as e:
                    try:
                        eng.conn.rollback()
                    except Exception:
                        pass
                    print(f"[watchdog] {e}")
                    alert_worker_error(e)
                last_watchdog = now_ts

            # Failed job alerts: check every 60s for freshly failed jobs
            if now_ts - last_dead_check >= 60:
                try:
                    with eng.conn.cursor(row_factory=dict_row) as cur:
                        # Instant alert: any run_job that just failed
                        cur.execute("""
                            select rj.id, rj.status, rj.attempt, rj.last_error, rj.resurrection_count, f.handle
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
                            elif row.get("resurrection_count", 0) >= 3:
                                alert_permanently_failed("run", row["id"], row["handle"], row.get("last_error", ""))
                            else:
                                alert_job_failed("run", row["id"], row["handle"], row.get("attempt", 0), row.get("last_error", ""))

                        # Instant alert: any checkpoint_job that just failed
                        cur.execute("""
                            select cj.id, cj.status, cj.attempt, cj.last_error, cj.checkpoint, cj.resurrection_count, f.handle
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
                            elif row.get("resurrection_count", 0) >= 3:
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
                    print(f"[dead-check] {e}")
                last_dead_check = now_ts

            # Daily summary at 8:00 AM IST
            today = datetime.now(ZoneInfo(APP_TIMEZONE)).strftime("%Y-%m-%d")
            now_ist = datetime.now(ZoneInfo(APP_TIMEZONE))
            if today != last_summary_date and now_ist.hour == 8 and now_ist.minute >= 0:
                try:
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
                    print(f"[daily-summary] {e}")
                last_summary_date = today

            # Process jobs
            try:
                eng.process_run_jobs(run_limit)
                eng.process_checkpoint_jobs(checkpoint_limit)
            except Exception as e:
                try:
                    eng.conn.rollback()
                except Exception:
                    pass
                print(f"[worker-loop] error: {e}")
                alert_worker_error(e)
                time.sleep(10)  # back off on error

            time.sleep(max(1, int(loop_sleep_seconds)))
    finally:
        eng.close()
