from __future__ import annotations

import os
import re
import json
import time
import tempfile
import mimetypes
import subprocess
import statistics
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone, timedelta, date
from html import unescape
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
    FEEDER_FILE_MEMORY_DAYS,
    FEEDER_FILE_MEMORY_LIMIT,
    FEEDER_INTELLIGENCE_AUTO_INTERVAL_SECONDS,
    FEEDER_INTELLIGENCE_AUTO_LIMIT,
    FEEDER_INTELLIGENCE_ENABLED,
    SUPABASE_URL,
    SUPABASE_SERVICE_ROLE_KEY,
    SUPABASE_MEDIA_BUCKET,
    MEDIA_STORAGE_PROVIDER,
    R2_ENDPOINT_URL,
    R2_ACCESS_KEY_ID,
    R2_SECRET_ACCESS_KEY,
    R2_BUCKET,
    R2_REGION,
    MEDIA_PUBLIC_BASE_URL,
    R2_MEDIA_ENABLED,
    FIRE_MEDIA_RETENTION_ENABLED,
)
from .web_push import is_enabled as web_push_enabled, send as send_web_push
from .signal_detection import (
    resolve_audience_signals_for_feed as run_audience_signals_for_feed,
    resolve_signals_for_feed as run_signals_for_feed,
)
from .fingerprint_intelligence import fingerprint_reels as run_fingerprint_reels
from .feeder_file_pipeline import (
    package_feeder_file_once as package_feeder_file_pipeline_once,
    repair_feeder_file_compile_once as repair_feeder_file_compile_pipeline_once,
    run_feeder_file_from_recent_fingerprints_once as run_feeder_file_recent_fingerprints_pipeline_once,
    run_feeder_file_once as run_feeder_file_pipeline_once,
)
from .retry_policy import (
    hard_skip_error as _hard_skip_error,
    is_connection_error as _is_connection_error,
    is_hard_failure as _is_hard_failure,
    is_transient_failure as _is_transient_failure,
    next_retry_time as _next_retry_time,
    should_retry_web_push as _should_retry_web_push,
)

_MEDIA_CAPTURE_TIMEOUT_SECONDS = 60
_MEDIA_UPLOAD_TIMEOUT_SECONDS = 120
_MEDIA_ALLOWED_FETCH_PROTOCOLS = ("http://", "https://")
_MEDIA_FETCH_HEADERS = {
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
    "accept": "*/*",
    "referer": "https://www.instagram.com/",
}
_IMAGE_ASSET_RETENTION_DAYS = 90
_PREVIEW_ASSET_RETENTION_DAYS = 8
_HOT_VISUAL_ASSET_RETENTION_DAYS = 30
_PREVIEW_CLIP_SECONDS = 5
_PREVIEW_MAX_DURATION_SECONDS = 5.0
_PREVIEW_DURATION_TOLERANCE_SECONDS = 0.08
_HEAVY_ASSET_RETENTION_DAYS = 1
_HOT_PERCENTILE_MAX = 35
_PREVIEW_CAPTURE_START_DAY = os.getenv("FIRE_PREVIEW_START_DAY", "2026-04-14").strip()
_FIRE_RANKING_WINDOW_DAYS = 90
_R2_CLIENT: Any | None = None


def _post_media_rollover_deadline(
    posted_at: datetime | None,
    asset_role: str | None,
    retention_days_override: int | None = None,
) -> datetime:
    role = (asset_role or "").strip().lower()
    base = posted_at or datetime.now(timezone.utc)
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    if role == "preview_5s":
        base = _preview_retention_anchor(base)
    if retention_days_override is not None:
        days = max(1, int(retention_days_override))
    elif role in ("thumbnail", "display"):
        days = _IMAGE_ASSET_RETENTION_DAYS
    elif role == "preview_5s":
        days = _PREVIEW_ASSET_RETENTION_DAYS
    else:
        days = _HEAVY_ASSET_RETENTION_DAYS
    return base + timedelta(days=days)


def _preview_extension_deadline(posted_at: datetime | None) -> datetime:
    return _post_media_rollover_deadline(posted_at, "preview_5s", _HOT_VISUAL_ASSET_RETENTION_DAYS)


def _preview_retention_anchor(posted_at: datetime | None) -> datetime:
    base = posted_at or datetime.now(timezone.utc)
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)

    cutoff_raw = (_PREVIEW_CAPTURE_START_DAY or "").strip()
    if not cutoff_raw:
        return base

    try:
        cutoff_date = date.fromisoformat(cutoff_raw)
        cutoff_tz = ZoneInfo(APP_TIMEZONE or "Asia/Kolkata")
        cutoff_dt = datetime(
            cutoff_date.year,
            cutoff_date.month,
            cutoff_date.day,
            0,
            0,
            0,
            tzinfo=cutoff_tz,
        ).astimezone(timezone.utc)
        return cutoff_dt if base < cutoff_dt else base
    except Exception:
        return base


def _preview_enabled_for_source(video_url: str | None) -> bool:
    return bool(str(video_url or "").strip())


def _preview_capture_allowed_for_business_day(business_day: str | None) -> bool:
    day = str(business_day or "").strip()
    cutoff = _PREVIEW_CAPTURE_START_DAY
    if not day or not cutoff:
        return True
    return day >= cutoff


def _render_preview_clip(video_bytes: bytes) -> tuple[bytes, str]:
    tmp_in = None
    tmp_out = None
    try:
        tmp_in = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
        tmp_in.write(video_bytes)
        tmp_in.close()
        tmp_out = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
        tmp_out.close()

        proc = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-threads",
                "1",
                "-i",
                tmp_in.name,
                "-t",
                str(_PREVIEW_CLIP_SECONDS),
                "-an",
                "-vf",
                "scale=w=1280:h=720:force_original_aspect_ratio=decrease:force_divisible_by=2",
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "24",
                "-pix_fmt",
                "yuv420p",
                "-movflags",
                "+faststart",
                tmp_out.name,
            ],
            capture_output=True,
            timeout=90,
        )
        if proc.returncode != 0:
            detail = proc.stderr.decode("utf-8", errors="replace").strip()[:500]
            raise RuntimeError(f"ffmpeg preview render failed: {detail or proc.returncode}")

        with open(tmp_out.name, "rb") as handle:
            rendered = handle.read()
        if not rendered:
            raise RuntimeError("ffmpeg preview render produced empty output")
        duration_seconds = _probe_media_duration_seconds(rendered, suffix=".mp4")
        if duration_seconds is None:
            raise RuntimeError("ffprobe preview validation failed")
        max_allowed_duration = _PREVIEW_MAX_DURATION_SECONDS + _PREVIEW_DURATION_TOLERANCE_SECONDS
        if duration_seconds > max_allowed_duration:
            raise RuntimeError(
                f"preview clip exceeded max duration: {duration_seconds:.3f}s > {max_allowed_duration:.3f}s"
            )
        return rendered, "video/mp4"
    except FileNotFoundError as exc:
        raise RuntimeError("ffmpeg is required for preview generation in the worker image") from exc
    finally:
        for tmp in (tmp_in, tmp_out):
            if tmp and getattr(tmp, "name", None):
                try:
                    os.unlink(tmp.name)
                except Exception:
                    pass


def _looks_like_video_source(source_url: str | None, content_type: str | None) -> bool:
    normalized_type = (content_type or "").strip().lower()
    if normalized_type.startswith("video/"):
        return True
    guessed_type, _ = mimetypes.guess_type(str(source_url or "").split("?", 1)[0])
    return bool(guessed_type and guessed_type.lower().startswith("video/"))


def _probe_media_duration_seconds(media_bytes: bytes, suffix: str = ".mp4") -> float | None:
    tmp = None
    try:
        tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
        tmp.write(media_bytes)
        tmp.close()
        proc = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                tmp.name,
            ],
            capture_output=True,
            timeout=30,
        )
        if proc.returncode != 0:
            return None
        output = proc.stdout.decode("utf-8", errors="replace").strip()
        if not output:
            return None
        duration = float(output)
        return duration if duration >= 0 else None
    except (FileNotFoundError, ValueError, subprocess.TimeoutExpired):
        return None
    finally:
        if tmp and getattr(tmp, "name", None):
            try:
                os.unlink(tmp.name)
            except Exception:
                pass


def _render_video_thumbnail(video_bytes: bytes) -> tuple[bytes, str]:
    tmp_in = None
    tmp_out = None
    try:
        tmp_in = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
        tmp_in.write(video_bytes)
        tmp_in.close()
        tmp_out = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
        tmp_out.close()

        proc = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                tmp_in.name,
                "-ss",
                "0.4",
                "-frames:v",
                "1",
                "-vf",
                "scale=w=1080:h=1920:force_original_aspect_ratio=decrease:force_divisible_by=2",
                "-q:v",
                "3",
                tmp_out.name,
            ],
            capture_output=True,
            timeout=60,
        )
        if proc.returncode != 0:
            detail = proc.stderr.decode("utf-8", errors="replace").strip()[:500]
            raise RuntimeError(f"ffmpeg thumbnail render failed: {detail or proc.returncode}")

        with open(tmp_out.name, "rb") as handle:
            rendered = handle.read()
        if not rendered:
            raise RuntimeError("ffmpeg thumbnail render produced empty output")
        return rendered, "image/jpeg"
    except FileNotFoundError as exc:
        raise RuntimeError("ffmpeg is required for thumbnail generation in the worker image") from exc
    finally:
        for tmp in (tmp_in, tmp_out):
            if tmp and getattr(tmp, "name", None):
                try:
                    os.unlink(tmp.name)
                except Exception:
                    pass


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


def _normalize_handle(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    raw = raw.replace("@", "").strip().strip("/")
    if not raw:
        return None
    lowered = raw.lower()
    if not re.fullmatch(r"[a-z0-9._]{1,30}", lowered):
        return None
    return lowered


def _scoped_post_key(feeder_id: int, post_url: str) -> str:
    base_key = _post_key_from_url(post_url)
    return f"{base_key}#f{feeder_id}" if base_key else f"post#f{feeder_id}"


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
    media_type = _media_type(item)
    views = None
    if _is_reel_media_type(media_type):
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


def _canonical_fire_metric_order(media_type: Any) -> tuple[str, ...]:
    normalized = str(media_type or "").strip().lower()
    if normalized in {"reel", "video"}:
        return ("views", "likes", "comments")
    return ("likes", "comments", "views")


def _round_half_up(value: float | None) -> int | None:
    if value is None:
        return None
    return int(value + 0.5)


def _median_bigint(values: list[int | float | None]) -> int | None:
    cleaned = sorted(float(value) for value in values if value is not None)
    if not cleaned:
        return None
    return _round_half_up(float(statistics.median(cleaned)))


def _metric_multiple(value: int | None, baseline: int | None) -> float | None:
    if value is None or baseline is None or baseline <= 0:
        return None
    return round(float(value) / float(baseline), 4)


def _competition_rank_maps(values: list[int | None]) -> tuple[dict[int, int], int]:
    counts: dict[int, int] = defaultdict(int)
    total = 0
    for value in values:
        if value is None:
            continue
        normalized = int(value)
        counts[normalized] += 1
        total += 1
    if total <= 0:
        return {}, 0

    rank_map: dict[int, int] = {}
    rank = 1
    for value in sorted(counts.keys(), reverse=True):
        rank_map[value] = rank
        rank += counts[value]
    return rank_map, total


def _percentile_from_rank(rank: int | None, pool_size: int) -> tuple[int | None, float | None]:
    if rank is None or pool_size <= 0:
        return None, None
    exact = max(1.0, min(100.0, (float(rank) / float(pool_size)) * 100.0))
    rounded = _round_half_up(exact)
    if rounded is None:
        return None, None
    return max(1, min(100, rounded)), exact


def _reference_ts_for_metric_row(posted_at: Any, business_day: Any) -> datetime | None:
    dt = _to_dt(posted_at)
    if dt is not None:
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)

    if business_day is None:
        return None

    day_value = business_day
    if isinstance(day_value, str):
        try:
            day_value = date.fromisoformat(day_value.strip())
        except Exception:
            return None

    if isinstance(day_value, date):
        try:
            local_zone = ZoneInfo(APP_TIMEZONE or "Asia/Kolkata")
        except Exception:
            local_zone = timezone.utc
        return datetime(
            day_value.year,
            day_value.month,
            day_value.day,
            12,
            0,
            0,
            tzinfo=local_zone,
        ).astimezone(timezone.utc)

    return None


def _metric_value_for_name(record: dict[str, Any], metric: str) -> int | None:
    normalized = str(metric or "").strip().lower()
    value = record.get(normalized)
    return _to_int(value)


def _baseline_value_for_name(record: dict[str, Any], metric: str) -> int | None:
    normalized = str(metric or "").strip().lower()
    value = record.get(f"{normalized}_baseline")
    return _to_int(value)


def _multiple_value_for_name(record: dict[str, Any], metric: str) -> float | None:
    normalized = str(metric or "").strip().lower()
    value = record.get(f"{normalized}_multiple")
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except Exception:
        return None


def _choose_canonical_fire_metric(record: dict[str, Any], media_type: Any) -> str | None:
    ordered = _canonical_fire_metric_order(media_type)
    best_metric: str | None = None
    best_multiple: float | None = None

    for metric in ordered:
        multiple = _multiple_value_for_name(record, metric)
        if multiple is None:
            continue
        if best_multiple is None or multiple > best_multiple:
            best_metric = metric
            best_multiple = multiple

    if best_metric:
        return best_metric

    for metric in ordered:
        if _metric_value_for_name(record, metric) is not None:
            return metric

    return ordered[0] if ordered else None


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


def _is_deleted_post_provider_error(error: str | None) -> bool:
    normalized = str(error or "").strip().lower()
    if not normalized:
        return False
    return (
        "dead_page" in normalized
        or "post isn't available" in normalized
        or "link may be broken" in normalized
        or "profile may have been removed" in normalized
        or "removed" in normalized
    )


def _extract_related_handles(item: dict, fallback_handle: str | None = None) -> list[str]:
    handles: list[str] = []
    seen: set[str] = set()

    def add(value: Any):
        if isinstance(value, list):
            for entry in value:
                add(entry)
            return
        if value is None:
            return
        handle = _normalize_handle(value)
        if handle and handle not in seen:
            seen.add(handle)
            handles.append(handle)

    add(item.get("relatedHandles"))
    add(item.get("ownerHandle"))

    owner = item.get("owner") if isinstance(item.get("owner"), dict) else {}
    for key in ("username", "user_name", "handle"):
        add(owner.get(key))

    add(item.get("username"))
    add(item.get("handle"))

    if fallback_handle:
        add(fallback_handle)

    return handles

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

    edge_followed_by = owner.get("edge_followed_by") if isinstance(owner.get("edge_followed_by"), dict) else {}
    follower_raw = None
    for candidate in (
        item.get("ownerFollowersCount"),
        item.get("followersCount"),
        owner.get("followersCount"),
        edge_followed_by.get("count"),
    ):
        if candidate is not None and candidate != "":
            follower_raw = candidate
            break
    followers = _to_int(follower_raw)

    return profile_pic, followers


def _extract_media_refs(item: dict) -> tuple[str | None, str | None, list[str]]:
    """Extract reusable media references for UI thumbnails and downstream vector jobs."""
    media_kind = _media_type(item)
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
    if media_kind == "sidecar":
        children = item.get("childPosts") or item.get("sidecarImages") or item.get("carouselMedia") or []
        if isinstance(children, list):
            for c in children:
                if not isinstance(c, dict):
                    continue
                u = c.get("displayUrl") or c.get("imageUrl") or c.get("videoUrl")
                if u and str(u) not in carousel_urls:
                    carousel_urls.append(str(u))
        if not carousel_urls and display_url:
            carousel_urls.append(str(display_url))
    elif media_kind != "reel":
        video_url = None

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


def _active_media_storage_provider() -> str:
    return "r2" if MEDIA_STORAGE_PROVIDER == "r2" and R2_MEDIA_ENABLED else "supabase"


def _active_media_bucket() -> str:
    return R2_BUCKET if _active_media_storage_provider() == "r2" else SUPABASE_MEDIA_BUCKET


def _public_media_url(path: str) -> str | None:
    base = (MEDIA_PUBLIC_BASE_URL or "").strip().rstrip("/")
    clean_path = (path or "").strip().lstrip("/")
    if not base or not clean_path:
        return None
    return f"{base}/{quote(clean_path, safe='/')}"


def _r2_client():
    global _R2_CLIENT
    if _R2_CLIENT is not None:
        return _R2_CLIENT
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise RuntimeError("MEDIA_STORAGE_PROVIDER=r2 requires boto3 in the worker environment") from exc

    if not (R2_ENDPOINT_URL and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY):
        raise RuntimeError("Missing R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, or R2_SECRET_ACCESS_KEY")

    _R2_CLIENT = boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name=R2_REGION or "auto",
        config=Config(signature_version="s3v4"),
    )
    return _R2_CLIENT


def _r2_signed_object_url(bucket: str, path: str, expires_seconds: int = 900) -> str | None:
    clean_bucket = (bucket or "").strip()
    clean_path = (path or "").strip().lstrip("/")
    if not clean_bucket or not clean_path:
        return None
    return _r2_client().generate_presigned_url(
        "get_object",
        Params={"Bucket": clean_bucket, "Key": clean_path},
        ExpiresIn=max(60, min(3600, int(expires_seconds))),
    )


def _extract_instagram_meta_content(html: str, key: str) -> str | None:
    if not html or not key:
        return None
    patterns = [
        rf'<meta[^>]+property=["\']{re.escape(key)}["\'][^>]+content=["\']([^"\']+)["\']',
        rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']{re.escape(key)}["\']',
        rf'<meta[^>]+name=["\']{re.escape(key)}["\'][^>]+content=["\']([^"\']+)["\']',
        rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']{re.escape(key)}["\']',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            value = unescape((match.group(1) or "").strip())
            if value:
                return value
    return None


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
        *,
        thumbnail_retention_days: int | None = None,
        preview_retention_days: int | None = None,
        full_video_retention_days: int | None = None,
        carousel_retention_days: int | None = None,
        stage_preview: bool = True,
    ):
        if not FIRE_MEDIA_RETENTION_ENABLED:
            return

        payloads: list[tuple[str, str, int | None]] = []
        if thumbnail_url:
            payloads.append(("thumbnail", thumbnail_url, thumbnail_retention_days))
        if stage_preview and _preview_enabled_for_source(video_url):
            payloads.append(("preview_5s", str(video_url).strip(), preview_retention_days))
        if full_video_retention_days is not None and _preview_enabled_for_source(video_url):
            payloads.append(("video_full", str(video_url).strip(), full_video_retention_days))
        if carousel_retention_days is not None and carousel_urls:
            for idx, source_url in enumerate(carousel_urls, start=1):
                normalized_source_url = str(source_url or "").strip()
                if not normalized_source_url:
                    continue
                payloads.append((f"carousel_{idx:02d}", normalized_source_url, carousel_retention_days))

        storage_provider = _active_media_storage_provider()
        storage_bucket = _active_media_bucket()

        for asset_role, source_url, retention_days in payloads:
            purge_after = _post_media_rollover_deadline(posted_at, asset_role, retention_days)
            if purge_after <= datetime.now(timezone.utc):
                continue
            self.conn.execute(
                """
                insert into public.post_media_assets (
                  post_key, asset_role, source_url, storage_provider, storage_bucket, status, attempt,
                  next_run_at, purge_after, last_error, updated_at
                )
                values (
                  %s,
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
                    storage_provider = excluded.storage_provider,
                    storage_bucket = excluded.storage_bucket,
                    purge_after = excluded.purge_after,
                    deleted_at = null,
                    updated_at = now(),
                    last_error = null,
                    status = case
                      when public.post_media_assets.status = 'active'
                           and coalesce(public.post_media_assets.storage_path, '') <> ''
                           and coalesce(public.post_media_assets.source_url, '') = excluded.source_url
                           and coalesce(public.post_media_assets.storage_provider, 'supabase') = excluded.storage_provider
                           and coalesce(public.post_media_assets.storage_bucket, '') = coalesce(excluded.storage_bucket, '')
                        then 'active'
                      when public.post_media_assets.status = 'purge_pending'
                           and coalesce(public.post_media_assets.storage_path, '') <> ''
                           and coalesce(public.post_media_assets.source_url, '') = excluded.source_url
                           and coalesce(public.post_media_assets.storage_provider, 'supabase') = excluded.storage_provider
                           and coalesce(public.post_media_assets.storage_bucket, '') = coalesce(excluded.storage_bucket, '')
                        then 'active'
                      else 'pending_capture'
                    end,
                    attempt = case
                      when public.post_media_assets.status = 'active'
                           and coalesce(public.post_media_assets.storage_path, '') <> ''
                           and coalesce(public.post_media_assets.source_url, '') = excluded.source_url
                           and coalesce(public.post_media_assets.storage_provider, 'supabase') = excluded.storage_provider
                           and coalesce(public.post_media_assets.storage_bucket, '') = coalesce(excluded.storage_bucket, '')
                        then public.post_media_assets.attempt
                      else 0
                    end,
                    next_run_at = case
                      when public.post_media_assets.status = 'active'
                           and coalesce(public.post_media_assets.storage_path, '') <> ''
                           and coalesce(public.post_media_assets.source_url, '') = excluded.source_url
                           and coalesce(public.post_media_assets.storage_provider, 'supabase') = excluded.storage_provider
                           and coalesce(public.post_media_assets.storage_bucket, '') = coalesce(excluded.storage_bucket, '')
                        then public.post_media_assets.next_run_at
                      else now()
                    end
                """,
                (post_key, asset_role, source_url, storage_provider, storage_bucket, purge_after),
            )

    def _best_effort_refresh_post_media(
        self,
        post_key: str,
        thumbnail_url: str | None,
        video_url: str | None,
        carousel_urls: list[str] | None,
        *,
        context: str,
    ):
        try:
            with self.conn.transaction():
                self._refresh_post_media(post_key, thumbnail_url, video_url, carousel_urls)
        except Exception as exc:
            print(f"[media-refresh] skipped post_key={post_key} context={context}: {exc}")

    def _best_effort_stage_post_media_assets(
        self,
        post_key: str,
        posted_at: datetime | None,
        thumbnail_url: str | None,
        video_url: str | None,
        carousel_urls: list[str] | None,
        *,
        thumbnail_retention_days: int | None = None,
        preview_retention_days: int | None = None,
        full_video_retention_days: int | None = None,
        carousel_retention_days: int | None = None,
        context: str,
        stage_preview: bool = True,
    ):
        try:
            with self.conn.transaction():
                self._stage_post_media_assets(
                    post_key,
                    posted_at,
                    thumbnail_url,
                    video_url,
                    carousel_urls,
                    thumbnail_retention_days=thumbnail_retention_days,
                    preview_retention_days=preview_retention_days,
                    full_video_retention_days=full_video_retention_days,
                    carousel_retention_days=carousel_retention_days,
                    stage_preview=stage_preview,
                )
        except Exception as exc:
            print(f"[media-stage] skipped post_key={post_key} context={context}: {exc}")

    def _capture_post_media_assets_for_post_keys(
        self,
        post_keys: list[str],
        *,
        asset_roles: tuple[str, ...] = ("thumbnail", "preview_5s"),
        include_all_roles: bool = False,
        stale_minutes: int = 5,
    ) -> dict[str, int]:
        normalized_post_keys = [str(value).strip().lower() for value in post_keys if str(value).strip()]
        normalized_roles = [str(value).strip().lower() for value in asset_roles if str(value).strip()]
        if not normalized_post_keys or (not normalized_roles and not include_all_roles):
            return {"selected": 0, "captured": 0, "failed": 0}

        rows = self.conn.execute(
            """
            select *
            from public.post_media_assets
            where post_key = any(%s)
              and (
                %s
                or asset_role = any(%s)
              )
              and (
                status in ('pending_capture', 'capture_failed')
                or (
                  status = 'capturing'
                  and coalesce(updated_at, now() - interval '365 days') <= now() - (%s::text || ' minutes')::interval
                )
              )
              and coalesce(source_url, '') <> ''
            order by
              case
                when lower(coalesce(asset_role, '')) = 'thumbnail' then 0
                when lower(coalesce(asset_role, '')) = 'video_full' then 1
                when lower(coalesce(asset_role, '')) like 'carousel_%%' then 2
                when lower(coalesce(asset_role, '')) = 'preview_5s' then 3
                else 9
              end,
              updated_at asc,
              id asc
            """,
            (normalized_post_keys, include_all_roles, normalized_roles, max(1, stale_minutes)),
        ).fetchall()
        self.conn.commit()

        captured = 0
        failed = 0
        for asset in rows:
            attempt = int(asset.get("attempt") or 0) + 1
            try:
                self._capture_post_media_asset(asset)
                captured += 1
            except Exception as exc:
                err = str(exc)[:1000] or "post media capture failed"
                failed += 1
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

        return {"selected": len(rows), "captured": captured, "failed": failed}

    def _retire_legacy_post_media_rows(
        self,
        post_keys: list[str] | None = None,
        *,
        limit: int = 1000,
    ) -> dict[str, int]:
        normalized_post_keys = [str(value).strip().lower() for value in (post_keys or []) if str(value).strip()]
        scoped_keys = normalized_post_keys or None
        rows = self.conn.execute(
            """
            with candidates as (
              select assets.id
              from public.post_media_assets assets
              where assets.status <> 'deleted'
                and (%s::text[] is null or assets.post_key = any(%s))
                and (
                  lower(coalesce(assets.asset_role, '')) = 'video'
                  or lower(coalesce(assets.asset_role, '')) like 'carousel_%%'
                  or (
                    coalesce(assets.storage_provider, 'supabase') = 'supabase'
                    and lower(coalesce(assets.asset_role, '')) in ('thumbnail', 'display', 'preview_5s')
                    and exists (
                      select 1
                      from public.post_media_assets r2
                      where r2.post_key = assets.post_key
                        and r2.asset_role = assets.asset_role
                        and r2.status in ('active', 'purge_pending')
                        and coalesce(r2.storage_provider, 'supabase') = 'r2'
                        and coalesce(r2.storage_path, '') <> ''
                    )
                  )
                )
              order by assets.updated_at desc nulls last, assets.id desc
              limit %s
            )
            update public.post_media_assets assets
            set purge_after = now() - interval '1 minute',
                status = case
                  when coalesce(assets.storage_path, '') <> '' then 'purge_pending'
                  else 'deleted'
                end,
                attempt = 0,
                next_run_at = now(),
                deleted_at = case
                  when coalesce(assets.storage_path, '') <> '' then null
                  else coalesce(assets.deleted_at, now())
                end,
                last_error = 'cleanup: retired legacy or superseded media asset',
                updated_at = now()
            from candidates
            where assets.id = candidates.id
            returning assets.id, assets.status
            """,
            (scoped_keys, scoped_keys, max(1, limit)),
        ).fetchall()
        self.conn.commit()

        marked = len(rows)
        purged = 0
        if marked > 0:
            self.process_post_media_assets(capture_limit=0, purge_limit=max(80, marked))
            purged = marked
        return {"marked": marked, "purged": purged}

    def _metric_percentile(self, post_key: str, checkpoint: str) -> float | None:
        row = self.conn.execute(
            """
            select percentile_performance
            from public.post_metrics
            where post_key = %s
              and lower(checkpoint) = lower(%s)
            """,
            (post_key, checkpoint),
        ).fetchone()
        value = (row or {}).get("percentile_performance")
        if isinstance(value, (int, float)):
            return float(value)
        return None

    def _recompute_feeder_checkpoint_rankings(
        self,
        feeder_id: int,
        checkpoint: str,
        *,
        window_days: int = _FIRE_RANKING_WINDOW_DAYS,
    ) -> dict[str, int]:
        cp = str(checkpoint or "").strip().lower()
        if feeder_id <= 0 or cp not in {"d1", "d3", "d7", "d21"}:
            return {"rows": 0, "lanes": 0}

        rows = self.conn.execute(
            """
            select
              pm.post_key,
              lower(pm.checkpoint) as checkpoint,
              pm.business_date_ist,
              pm.computed_at,
              pm.views,
              pm.likes,
              pm.comments,
              lower(coalesce(p.media_type, 'unknown')) as media_type,
              p.posted_at,
              extract(hour from (p.posted_at at time zone %s))::smallint as hour_ist
            from public.post_metrics pm
            join public.posts p on p.post_key = pm.post_key
            where p.feeder_id = %s
              and lower(pm.checkpoint) = %s
            order by
              coalesce(p.posted_at, (pm.business_date_ist::text || ' 12:00:00+00')::timestamptz, pm.computed_at) asc,
              pm.computed_at asc,
              pm.post_key asc
            """,
            (APP_TIMEZONE, feeder_id, cp),
        ).fetchall()

        if not rows:
            return {"rows": 0, "lanes": 0}

        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            normalized = dict(row)
            normalized["post_key"] = str(row.get("post_key") or "").strip().lower()
            normalized["checkpoint"] = cp
            normalized["media_type"] = str(row.get("media_type") or "unknown").strip().lower() or "unknown"
            normalized["views"] = _to_int(row.get("views"))
            normalized["likes"] = _to_int(row.get("likes"))
            normalized["comments"] = _to_int(row.get("comments"))
            normalized["hour_ist"] = _to_int(row.get("hour_ist"))
            normalized["reference_ts"] = (
                _reference_ts_for_metric_row(row.get("posted_at"), row.get("business_date_ist"))
                or _to_dt(row.get("computed_at"))
                or datetime.now(timezone.utc)
            )
            normalized_rows.append(normalized)

        rows_by_lane: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in normalized_rows:
            rows_by_lane[row["media_type"]].append(row)

        updates_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        ranking_cutoff = timedelta(days=max(1, int(window_days)))
        lane_count = 0

        for media_type, lane_rows in rows_by_lane.items():
            lane_rows.sort(
                key=lambda item: (
                    item.get("reference_ts") or datetime.now(timezone.utc),
                    _to_dt(item.get("computed_at")) or datetime.now(timezone.utc),
                    item.get("post_key") or "",
                )
            )
            lane_count += 1
            start = 0
            for index, current in enumerate(lane_rows):
                current_ts = current.get("reference_ts") or datetime.now(timezone.utc)
                while start < index:
                    compare_ts = lane_rows[start].get("reference_ts") or current_ts
                    if compare_ts >= current_ts - ranking_cutoff:
                        break
                    start += 1
                window = lane_rows[start:index + 1]

                views_values = [_metric_value_for_name(candidate, "views") for candidate in window]
                likes_values = [_metric_value_for_name(candidate, "likes") for candidate in window]
                comments_values = [_metric_value_for_name(candidate, "comments") for candidate in window]

                current["views_baseline"] = _median_bigint(views_values)
                current["likes_baseline"] = _median_bigint(likes_values)
                current["comments_baseline"] = _median_bigint(comments_values)
                current["views_multiple"] = _metric_multiple(current.get("views"), current.get("views_baseline"))
                current["likes_multiple"] = _metric_multiple(current.get("likes"), current.get("likes_baseline"))
                current["comments_multiple"] = _metric_multiple(current.get("comments"), current.get("comments_baseline"))

                views_rank_map, views_pool_size = _competition_rank_maps(views_values)
                likes_rank_map, likes_pool_size = _competition_rank_maps(likes_values)
                comments_rank_map, comments_pool_size = _competition_rank_maps(comments_values)

                current_views = current.get("views")
                current_likes = current.get("likes")
                current_comments = current.get("comments")

                current["views_percentile"], _ = _percentile_from_rank(
                    views_rank_map.get(int(current_views)) if current_views is not None else None,
                    views_pool_size,
                )
                current["likes_percentile"], _ = _percentile_from_rank(
                    likes_rank_map.get(int(current_likes)) if current_likes is not None else None,
                    likes_pool_size,
                )
                current["comments_percentile"], _ = _percentile_from_rank(
                    comments_rank_map.get(int(current_comments)) if current_comments is not None else None,
                    comments_pool_size,
                )

                current["ranking_metric"] = _choose_canonical_fire_metric(current, media_type)
                ranking_metric = str(current.get("ranking_metric") or "").strip().lower()
                ranking_value = _metric_value_for_name(current, ranking_metric)
                current["metric_value"] = ranking_value
                current["ranking_multiple"] = _multiple_value_for_name(current, ranking_metric)

                if ranking_metric == "views":
                    ranking_rank = views_rank_map.get(int(ranking_value)) if ranking_value is not None else None
                    ranking_pool_size = views_pool_size
                elif ranking_metric == "likes":
                    ranking_rank = likes_rank_map.get(int(ranking_value)) if ranking_value is not None else None
                    ranking_pool_size = likes_pool_size
                else:
                    ranking_rank = comments_rank_map.get(int(ranking_value)) if ranking_value is not None else None
                    ranking_pool_size = comments_pool_size

                percentile_rounded, percentile_exact = _percentile_from_rank(ranking_rank, ranking_pool_size)
                current["percentile_performance"] = percentile_rounded
                current["percentile_performance_exact"] = percentile_exact

                hour_multiple = None
                hour_ist = current.get("hour_ist")
                if hour_ist is not None and ranking_metric in {"views", "likes", "comments"}:
                    hour_values = [
                        _metric_value_for_name(candidate, ranking_metric)
                        for candidate in window
                        if candidate.get("hour_ist") == hour_ist
                    ]
                    hour_baseline = _median_bigint(hour_values)
                    hour_multiple = _metric_multiple(ranking_value, hour_baseline)
                current["hour_multiple"] = hour_multiple
                current["feed_percentile"] = None

                updates_by_key[(current["post_key"], cp)] = current

        if not updates_by_key:
            return {"rows": 0, "lanes": lane_count}

        d1_percentile_by_post: dict[str, int | None] = {}
        if cp != "d1":
            d1_rows = self.conn.execute(
                """
                select post_key, percentile_performance
                from public.post_metrics
                where post_key = any(%s)
                  and lower(checkpoint) = 'd1'
                """,
                ([key[0] for key in updates_by_key.keys()],),
            ).fetchall()
            for row in d1_rows:
                post_key = str(row.get("post_key") or "").strip().lower()
                d1_percentile_by_post[post_key] = _to_int(row.get("percentile_performance"))

        update_params: list[tuple[Any, ...]] = []
        for (post_key, row_checkpoint), values in updates_by_key.items():
            current_percentile = _to_int(values.get("percentile_performance"))
            d1_percentile = d1_percentile_by_post.get(post_key)
            delta_from_d1 = None
            if row_checkpoint != "d1" and d1_percentile is not None and current_percentile is not None:
                delta_from_d1 = d1_percentile - current_percentile

            update_params.append(
                (
                    values.get("metric_value"),
                    current_percentile,
                    values.get("percentile_performance_exact"),
                    values.get("views_percentile"),
                    values.get("likes_percentile"),
                    values.get("comments_percentile"),
                    delta_from_d1,
                    values.get("ranking_metric"),
                    values.get("ranking_multiple"),
                    values.get("views_baseline"),
                    values.get("likes_baseline"),
                    values.get("comments_baseline"),
                    values.get("views_multiple"),
                    values.get("likes_multiple"),
                    values.get("comments_multiple"),
                    values.get("hour_multiple"),
                    post_key,
                    row_checkpoint,
                )
            )

        with self.conn.cursor() as cur:
            cur.executemany(
                """
                update public.post_metrics
                set metric_value = %s,
                    percentile_performance = %s,
                    percentile_performance_exact = %s,
                    views_percentile = %s,
                    likes_percentile = %s,
                    comments_percentile = %s,
                    feed_percentile = null,
                    delta_from_d1 = %s,
                    ranking_metric = %s,
                    ranking_multiple = %s,
                    views_baseline = %s,
                    likes_baseline = %s,
                    comments_baseline = %s,
                    views_multiple = %s,
                    likes_multiple = %s,
                    comments_multiple = %s,
                    hour_multiple = %s
                where post_key = %s
                  and lower(checkpoint) = %s
                """,
                update_params,
            )
        self.conn.commit()
        return {"rows": len(update_params), "lanes": lane_count}

    def recompute_fire_rankings(
        self,
        *,
        limit: int = 300,
        days: int | None = None,
    ) -> dict[str, int]:
        scoped_days = max(1, int(days)) if days is not None and int(days) > 0 else None
        rows = self.conn.execute(
            """
            with scope as (
              select
                p.feeder_id,
                lower(pm.checkpoint) as checkpoint,
                max(coalesce(p.posted_at, (pm.business_date_ist::text || ' 12:00:00+00')::timestamptz, pm.computed_at)) as latest_ref_ts
              from public.post_metrics pm
              join public.posts p on p.post_key = pm.post_key
              where lower(pm.checkpoint) in ('d1', 'd3', 'd7', 'd21')
                and (
                  %s::int is null
                  or coalesce(p.posted_at, (pm.business_date_ist::text || ' 12:00:00+00')::timestamptz, pm.computed_at)
                    >= now() - (%s::text || ' days')::interval
                )
              group by p.feeder_id, lower(pm.checkpoint)
            )
            select feeder_id, checkpoint
            from scope
            order by latest_ref_ts desc nulls last, feeder_id asc, checkpoint asc
            limit %s
            """,
            (scoped_days, scoped_days, max(1, int(limit))),
        ).fetchall()

        processed = 0
        updated_rows = 0
        lane_count = 0
        for row in rows:
            feeder_id = int(row.get("feeder_id") or 0)
            checkpoint = str(row.get("checkpoint") or "").strip().lower()
            if feeder_id <= 0 or not checkpoint:
                continue
            result = self._recompute_feeder_checkpoint_rankings(feeder_id, checkpoint)
            processed += 1
            updated_rows += int(result.get("rows", 0))
            lane_count += int(result.get("lanes", 0))

        return {
            "selected": len(rows),
            "processed": processed,
            "updated_rows": updated_rows,
            "lanes": lane_count,
        }

    def _extend_hot_visual_media_for_day(self, feeder_id: int, checkpoint: str, business_day: date | None):
        cp = str(checkpoint or "").strip().lower()
        if feeder_id <= 0 or business_day is None or cp not in {"d7", "d21"}:
            return

        rows = self.conn.execute(
            """
            select
              p.post_key,
              p.posted_at,
              lower(coalesce(p.media_type, 'image')) as media_type,
              p.thumbnail_url,
              p.video_url,
              p.carousel_urls,
              pm.business_date_ist
            from public.post_metrics pm
            join public.posts p on p.post_key = pm.post_key
            where p.feeder_id = %s
              and lower(pm.checkpoint) = %s
              and pm.business_date_ist = %s
              and pm.percentile_performance is not null
              and pm.percentile_performance <= %s
            """,
            (feeder_id, cp, business_day, _HOT_PERCENTILE_MAX),
        ).fetchall()

        staged_post_keys: list[str] = []
        for row in rows:
            post_key = str(row.get("post_key") or "").strip().lower()
            if not post_key:
                continue
            posted_at = _to_dt(row.get("posted_at"))
            media_type = str(row.get("media_type") or "image").strip().lower()
            thumbnail_url = str(row.get("thumbnail_url") or "").strip() or None
            preview_allowed = _preview_capture_allowed_for_business_day(str(row.get("business_date_ist") or "").strip())
            video_url = str(row.get("video_url") or "").strip() or None
            raw_carousel_urls = row.get("carousel_urls")
            carousel_urls = (
                [str(value).strip() for value in raw_carousel_urls if str(value).strip()]
                if isinstance(raw_carousel_urls, list)
                else None
            )
            self._stage_post_media_assets(
                post_key,
                posted_at,
                thumbnail_url,
                video_url,
                carousel_urls,
                preview_retention_days=_HOT_VISUAL_ASSET_RETENTION_DAYS,
                full_video_retention_days=_HOT_VISUAL_ASSET_RETENTION_DAYS if media_type == "reel" else None,
                carousel_retention_days=_HOT_VISUAL_ASSET_RETENTION_DAYS if media_type in {"sidecar", "carousel"} else None,
                stage_preview=preview_allowed,
            )
            staged_post_keys.append(post_key)
        self.conn.commit()
        if staged_post_keys:
            self._capture_post_media_assets_for_post_keys(
                staged_post_keys,
                include_all_roles=True,
            )

    def _stage_d7_feeder_file_media_for_feeder(
        self,
        feeder_id: int,
        *,
        limit: int | None = None,
        days: int = _FIRE_RANKING_WINDOW_DAYS,
    ) -> dict[str, int]:
        if not FIRE_MEDIA_RETENTION_ENABLED or feeder_id <= 0:
            return {"selected": 0, "staged": 0, "captured": 0, "failed": 0}

        rows = self.conn.execute(
            """
            with reel_d7 as (
              select
                p.post_key,
                p.feeder_id,
                p.posted_at,
                lower(coalesce(p.media_type, '')) as media_type,
                p.video_url,
                least(
                  coalesce(pm.percentile_performance_exact, 101),
                  coalesce(pm.percentile_performance, 101)
                ) as d7_percentile,
                row_number() over (
                  partition by p.feeder_id
                  order by p.posted_at desc nulls last
                ) as recent_rank,
                pf_high.post_key as high_fingerprint_post_key
              from public.posts p
              join public.post_metrics pm
                on pm.post_key = p.post_key
               and lower(pm.checkpoint) = 'd7'
              left join public.post_fingerprints pf_high
                on pf_high.post_key = p.post_key
               and pf_high.media_confidence = 'high'
              where p.feeder_id = %s
                and lower(coalesce(p.media_type, '')) in ('reel', 'video')
                and p.posted_at >= now() - (%s::int * interval '1 day')
            ),
            scored as (
              select
                *,
                row_number() over (
                  partition by feeder_id
                  order by
                    case when recent_rank <= 10 then d7_percentile else null end asc nulls last,
                    posted_at desc nulls last
                ) as recent_performance_rank
              from reel_d7
            )
            select post_key, posted_at, media_type, video_url
            from scored
            where (
                d7_percentile <= 25
                or (recent_rank <= 10 and recent_performance_rank <= 2)
              )
              and high_fingerprint_post_key is null
              and coalesce(video_url, '') <> ''
              and not exists (
                select 1
                from public.post_media_assets pma
                where pma.post_key = scored.post_key
                  and pma.asset_role = 'video_full'
                  and pma.status in ('active', 'purge_pending')
                  and coalesce(pma.storage_path, pma.public_url, '') <> ''
              )
            order by d7_percentile asc nulls last, posted_at desc nulls last
            limit %s
            """,
            (feeder_id, max(1, int(days)), max(1, int(limit or FEEDER_INTELLIGENCE_AUTO_LIMIT))),
        ).fetchall()

        staged_post_keys: list[str] = []
        for row in rows:
            post_key = str(row.get("post_key") or "").strip().lower()
            video_url = str(row.get("video_url") or "").strip() or None
            if not post_key or not video_url:
                continue
            self._stage_post_media_assets(
                post_key,
                _to_dt(row.get("posted_at")),
                None,
                video_url,
                None,
                full_video_retention_days=_HOT_VISUAL_ASSET_RETENTION_DAYS,
                stage_preview=False,
            )
            staged_post_keys.append(post_key)
        self.conn.commit()

        capture_result = {"captured": 0, "failed": 0}
        if staged_post_keys:
            capture_result = self._capture_post_media_assets_for_post_keys(
                staged_post_keys,
                asset_roles=("video_full",),
            )
        return {
            "selected": len(rows),
            "staged": len(staged_post_keys),
            "captured": int(capture_result.get("captured", 0)),
            "failed": int(capture_result.get("failed", 0)),
        }

    def _run_d7_feeder_file_metric_trigger(self, feeder_id: int) -> dict[str, Any]:
        if not FEEDER_INTELLIGENCE_ENABLED or feeder_id <= 0:
            return {"enabled": False}
        media_result = self._stage_d7_feeder_file_media_for_feeder(
            feeder_id,
            limit=max(1, int(FEEDER_INTELLIGENCE_AUTO_LIMIT)),
        )
        intelligence_result = self.fingerprint_reels(
            feeder_id=feeder_id,
            limit=max(1, int(FEEDER_INTELLIGENCE_AUTO_LIMIT)),
            days=_FIRE_RANKING_WINDOW_DAYS,
        )
        return {
            "enabled": True,
            "media": media_result,
            "intelligence": intelligence_result,
        }

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
        storage_provider: str | None = None,
        storage_bucket: str | None = None,
        storage_path: str | None = None,
        public_url: str | None = None,
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
                storage_provider = coalesce(%s::text, storage_provider),
                storage_bucket = coalesce(%s::text, storage_bucket),
                storage_path = coalesce(%s::text, storage_path),
                public_url = %s::text,
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
                storage_provider,
                storage_bucket,
                storage_path,
                public_url,
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

    def _delete_post_media_object(self, asset: dict):
        storage_path = str(asset.get("storage_path") or "").strip()
        if not storage_path:
            return

        storage_provider = str(asset.get("storage_provider") or "supabase").strip().lower()
        storage_bucket = str(asset.get("storage_bucket") or (_active_media_bucket() if storage_provider == "r2" else SUPABASE_MEDIA_BUCKET))
        if storage_provider == "r2":
            _r2_client().delete_object(Bucket=storage_bucket, Key=storage_path)
        else:
            resp = requests.delete(
                _storage_object_url(storage_bucket, storage_path),
                headers=self._storage_headers(),
                timeout=_MEDIA_CAPTURE_TIMEOUT_SECONDS,
            )
            if resp.status_code not in (200, 204, 404):
                resp.raise_for_status()

    def _reset_post_media_asset_for_recapture(self, asset_id: int, error: str | None = None):
        self.conn.execute(
            """
            update public.post_media_assets
            set status = 'pending_capture',
                attempt = 0,
                next_run_at = now(),
                storage_path = null,
                public_url = null,
                mime_type = null,
                byte_size = null,
                captured_at = null,
                deleted_at = null,
                last_error = case when %s::text is null then null else left(%s::text, 1000) end,
                updated_at = now()
            where id = %s
            """,
            (error, error, asset_id),
        )
        self.conn.commit()

    def _fetch_stored_post_media_asset_bytes(self, asset: dict) -> bytes | None:
        storage_path = str(asset.get("storage_path") or "").strip()
        if not storage_path:
            return None

        storage_provider = str(asset.get("storage_provider") or "supabase").strip().lower()
        storage_bucket = str(asset.get("storage_bucket") or (_active_media_bucket() if storage_provider == "r2" else SUPABASE_MEDIA_BUCKET))
        if storage_provider == "r2":
            obj = _r2_client().get_object(Bucket=storage_bucket, Key=storage_path)
            body = obj.get("Body")
            if body is None:
                return None
            data = body.read()
            return data if data else None

        resp = requests.get(
            _storage_authenticated_object_url(storage_bucket, storage_path),
            headers=self._storage_read_headers(),
            timeout=_MEDIA_CAPTURE_TIMEOUT_SECONDS,
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.content or None

    def _store_post_media_asset_body(
        self,
        asset: dict,
        body: bytes,
        content_type: str,
    ) -> tuple[str, str, str | None]:
        if not body:
            raise RuntimeError("empty media payload")

        storage_path = _fire_media_object_key(
            str(asset.get("post_key") or ""),
            str(asset.get("asset_role") or ""),
            str(asset.get("source_url") or asset.get("public_url") or asset.get("storage_path") or "").strip() or None,
            content_type,
        )
        storage_provider = str(asset.get("storage_provider") or _active_media_storage_provider()).strip().lower()
        if storage_provider not in ("supabase", "r2"):
            storage_provider = _active_media_storage_provider()
        storage_bucket = str(asset.get("storage_bucket") or _active_media_bucket())
        public_url = None

        if storage_provider == "r2":
            _r2_client().put_object(
                Bucket=storage_bucket,
                Key=storage_path,
                Body=body,
                ContentType=content_type,
                CacheControl="public, max-age=31536000, immutable",
            )
            public_url = _public_media_url(storage_path)
        else:
            upload = requests.post(
                _storage_object_url(storage_bucket, storage_path),
                headers=self._storage_headers(content_type),
                data=body,
                timeout=_MEDIA_UPLOAD_TIMEOUT_SECONDS,
            )
            upload.raise_for_status()

        return storage_provider, storage_path, public_url

    def _capture_post_media_asset(self, asset: dict):
        source_url = str(asset.get("source_url") or "").strip()
        if not source_url.startswith(_MEDIA_ALLOWED_FETCH_PROTOCOLS):
            raise RuntimeError("invalid media source url")

        asset_role = str(asset.get("asset_role") or "").strip().lower()
        upstream = requests.get(
            source_url,
            headers=_MEDIA_FETCH_HEADERS,
            timeout=_MEDIA_CAPTURE_TIMEOUT_SECONDS,
        )
        upstream.raise_for_status()
        upstream_content_type = (upstream.headers.get("content-type") or "application/octet-stream").split(";", 1)[0].strip()
        body = upstream.content
        if not body:
            raise RuntimeError("empty media payload")

        if asset_role == "preview_5s":
            body, content_type = _render_preview_clip(body)
        elif asset_role in ("thumbnail", "display") and _looks_like_video_source(source_url, upstream_content_type):
            body, content_type = _render_video_thumbnail(body)
        else:
            content_type = upstream_content_type

        storage_provider, storage_path, public_url = self._store_post_media_asset_body(asset, body, content_type)

        self._set_post_media_asset_result(
            int(asset["id"]),
            "active",
            attempt=0,
            next_run_at=None,
            storage_provider=storage_provider,
            storage_bucket=str(asset.get("storage_bucket") or _active_media_bucket()),
            storage_path=storage_path,
            public_url=public_url,
            mime_type=content_type,
            byte_size=len(body),
            captured_at=datetime.now(timezone.utc),
            deleted_at=None,
            error=None,
        )

    def _fetch_instagram_post_page_thumbnail_url(self, post_url: str) -> str | None:
        url = str(post_url or "").strip()
        if not url.startswith(_MEDIA_ALLOWED_FETCH_PROTOCOLS):
            return None

        response = requests.get(
            url,
            headers={"user-agent": _MEDIA_FETCH_HEADERS["user-agent"]},
            timeout=_MEDIA_CAPTURE_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        html = response.text or ""
        if not html:
            return None

        for key in ("og:image", "twitter:image"):
            value = _extract_instagram_meta_content(html, key)
            if value and value.startswith(_MEDIA_ALLOWED_FETCH_PROTOCOLS):
                return value
        return None

    def _delete_post_media_asset(self, asset: dict):
        storage_path = str(asset.get("storage_path") or "").strip()
        if not storage_path:
            self._delete_post_media_asset_row(int(asset["id"]))
            return

        self._delete_post_media_object(asset)
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

    def _active_feeder_ids_by_handle(self, handles: set[str]) -> dict[str, list[int]]:
        normalized_handles = sorted({handle for handle in (_normalize_handle(value) for value in handles) if handle})
        if not normalized_handles:
            return {}

        rows = self.conn.execute(
            """
            select id, lower(handle) as handle
            from public.feeders
            where status = 'active'
              and lower(handle) = any(%s)
            """,
            (normalized_handles,),
        ).fetchall()

        mapping: dict[str, list[int]] = defaultdict(list)
        for row in rows:
            handle = str(row.get("handle") or "").strip().lower()
            feeder_id = int(row.get("id") or 0)
            if handle and feeder_id > 0:
                mapping[handle].append(feeder_id)
        return dict(mapping)

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

        post_key = _scoped_post_key(feeder_id, post_url)
        normalized_media = _media_type({"type": media_type})
        carousel_count = len(carousel_urls or []) if normalized_media in {"sidecar", "carousel"} else None
        depth_bucket = None
        if carousel_count is not None:
            if 2 <= carousel_count <= 3:
                depth_bucket = "DEPTH_MINI"
            elif 4 <= carousel_count <= 7:
                depth_bucket = "DEPTH_STANDARD"
            elif carousel_count >= 8:
                depth_bucket = "DEPTH_DEEP"
            else:
                depth_bucket = "DEPTH_UNKNOWN"
        duration_bucket = "DUR_UNKNOWN" if normalized_media == "reel" else None
        row = self.conn.execute(
            """
            insert into public.posts (
              post_key, feeder_id, post_url, media_type, posted_at, caption,
              provider_post_id, thumbnail_url, video_url, carousel_urls,
              carousel_slide_count, depth_bucket, duration_bucket,
              created_at, updated_at
            )
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,now(),now())
            on conflict (feeder_id, post_url)
            do update set
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
              carousel_slide_count=coalesce(excluded.carousel_slide_count, public.posts.carousel_slide_count),
              depth_bucket=coalesce(excluded.depth_bucket, public.posts.depth_bucket),
              duration_bucket=coalesce(excluded.duration_bucket, public.posts.duration_bucket),
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
                carousel_count,
                depth_bucket,
                duration_bucket,
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
    ) -> bool:
        """Write raw metrics only — Supabase trigger computes derived fields."""
        row = self.conn.execute(
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
            returning post_key
            """,
            (post_key, checkpoint, views, likes, comments, business_date_ist),
        ).fetchone()
        return bool(row and row.get("post_key"))

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

    def _mark_post_availability(self, post_key: str, status: str, reason: str | None = None):
        normalized_status = str(status or "active").strip().lower() or "active"
        if normalized_status not in {"active", "deleted", "unavailable"}:
            normalized_status = "unavailable"
        self.conn.execute(
            """
            update public.posts
            set availability_status = %s,
                availability_error = case when %s::text is null then null else left(%s::text, 1000) end,
                availability_checked_at = now(),
                updated_at = now()
            where post_key = %s
            """,
            (normalized_status, reason, reason, post_key),
        )

    def _latest_prior_metric_snapshot(self, post_key: str, checkpoint: str) -> dict[str, Any] | None:
        cp = str(checkpoint or "").strip().lower()
        if cp == "d21":
            candidates = ("d7", "d3", "d1")
        elif cp == "d7":
            candidates = ("d3", "d1")
        elif cp == "d3":
            candidates = ("d1",)
        else:
            candidates = ()
        if not candidates:
            return None

        row = self.conn.execute(
            """
            select checkpoint, views, likes, comments, business_date_ist
            from public.post_metrics
            where post_key = %s
              and lower(checkpoint) = any(%s)
            order by
              case lower(checkpoint)
                when 'd7' then 3
                when 'd3' then 2
                when 'd1' then 1
                else 0
              end desc,
              computed_at desc nulls last
            limit 1
            """,
            (post_key, list(candidates)),
        ).fetchone()
        return row or None

    def _has_cached_thumbnail_asset(self, post_key: str) -> bool:
        row = self.conn.execute(
            """
            select 1
            from public.post_media_assets
            where post_key = %s
              and asset_role = 'thumbnail'
              and status in ('active', 'purge_pending')
              and coalesce(storage_provider, 'supabase') = 'r2'
              and coalesce(storage_path, '') <> ''
            limit 1
            """,
            (post_key,),
        ).fetchone()
        return bool(row)

    def _checkpoint_business_day_for_job(
        self,
        posted_at: datetime | None,
        checkpoint: str,
        next_run_at: Any,
    ) -> date | None:
        cp = str(checkpoint or "").strip().lower()
        if cp in ("d1", "d3", "d7", "d21"):
            business_day = _checkpoint_business_day_for_post(posted_at, cp)
            if business_day is None:
                business_day = _checkpoint_business_day(next_run_at)
            return business_day
        business_day = _business_date_from_job({"next_run_at": next_run_at})
        if business_day is None:
            business_day = _checkpoint_business_day(next_run_at)
        return business_day

    def _freeze_checkpoint_from_previous_metrics(
        self,
        job: dict,
        business_day: date | None,
        reason: str,
    ) -> str | None:
        post_key = str(job.get("post_key") or "").strip().lower()
        checkpoint = str(job.get("checkpoint") or "").strip().lower()
        if not post_key or not checkpoint or business_day is None:
            return None
        if not self._has_cached_thumbnail_asset(post_key):
            return None

        snapshot = self._latest_prior_metric_snapshot(post_key, checkpoint)
        if not snapshot:
            return None

        self._insert_metric_if_missing(
            post_key,
            checkpoint,
            snapshot.get("views"),
            snapshot.get("likes"),
            snapshot.get("comments"),
            business_day,
        )
        prior_checkpoint = str(snapshot.get("checkpoint") or "").strip().upper() or "PREVIOUS"
        return f"checkpoint frozen: post unavailable; reused {prior_checkpoint} metrics ({reason[:240]})"

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
                 , p.availability_status
                 , p.availability_error
                 , p.availability_checked_at
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
        # Checkpoint cards now come from post_metrics directly in the Fire API.
        return

    def _try_resolve_feed(self, feeder_id: int, checkpoint: str, business_date_ist: date | None = None):
        cp = (checkpoint or '').lower()
        if cp not in ('d3', 'd7', 'd21') or business_date_ist is None:
            return
        self._resolve_signals_for_feed(feeder_id, cp, business_date_ist)

    def _resolve_signals_for_feed(self, feeder_id: int, checkpoint: str, business_date_ist: date):
        return run_signals_for_feed(
            self.conn,
            feeder_id,
            checkpoint,
            business_date_ist,
            app_timezone=APP_TIMEZONE,
        )

    def _resolve_audience_signals_for_feed(self, feeder_id: int, business_date_ist: date):
        return run_audience_signals_for_feed(
            self.conn,
            feeder_id,
            business_date_ist,
            app_timezone=APP_TIMEZONE,
        )

    def fingerprint_reels(
        self,
        feeder_id: int | None = None,
        limit: int = 10,
        days: int = 90,
    ):
        return run_fingerprint_reels(
            self.conn,
            feeder_id=feeder_id,
            limit=limit,
            days=days,
        )

    def run_feeder_file_once(
        self,
        feeder_id: int | None = None,
        handle: str | None = None,
        limit: int = FEEDER_FILE_MEMORY_LIMIT,
        days: int = FEEDER_FILE_MEMORY_DAYS,
        pattern_limit: int = 3,
    ):
        return run_feeder_file_pipeline_once(
            self.conn,
            feeder_id=feeder_id,
            handle=handle,
            limit=limit,
            days=days,
            pattern_limit=pattern_limit,
        )

    def run_feeder_file_from_recent_fingerprints_once(
        self,
        feeder_id: int | None = None,
        handle: str | None = None,
        limit: int = 10,
        days: int = FEEDER_FILE_MEMORY_DAYS,
        pattern_limit: int = 3,
    ):
        return run_feeder_file_recent_fingerprints_pipeline_once(
            self.conn,
            feeder_id=feeder_id,
            handle=handle,
            limit=limit,
            days=days,
            pattern_limit=pattern_limit,
        )

    def package_feeder_file_once(
        self,
        feeder_id: int | None = None,
        handle: str | None = None,
        compile_version: str | None = None,
        pattern_id: str | None = None,
        pattern_limit: int = 3,
    ):
        return package_feeder_file_pipeline_once(
            self.conn,
            feeder_id=feeder_id,
            handle=handle,
            compile_version=compile_version,
            pattern_id=pattern_id,
            pattern_limit=pattern_limit,
        )

    def repair_feeder_file_compile_once(
        self,
        feeder_id: int | None = None,
        handle: str | None = None,
        compile_version: str | None = None,
        pattern_id: str | None = None,
        pattern_limit: int = 3,
    ):
        return repair_feeder_file_compile_pipeline_once(
            self.conn,
            feeder_id=feeder_id,
            handle=handle,
            compile_version=compile_version,
            pattern_id=pattern_id,
            pattern_limit=pattern_limit,
        )

    def _supabase_media_url(self, post_key: str, asset_role: str) -> str | None:
        """Get a URL for a cached media asset."""
        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """SELECT storage_provider, storage_bucket, storage_path, public_url
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
        if row.get("storage_provider") == "r2":
            bucket = row.get("storage_bucket") or R2_BUCKET
            return _r2_signed_object_url(str(bucket), str(row["storage_path"]))
        if row.get("public_url"):
            return str(row["public_url"])
        bucket = row.get("storage_bucket") or SUPABASE_MEDIA_BUCKET
        path = row["storage_path"]
        return _storage_authenticated_object_url(str(bucket), str(path))

    def _get_cached_carousel_urls(self, post_key: str) -> list[str]:
        """Get URLs for cached carousel slides."""
        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """SELECT storage_provider, storage_bucket, storage_path, public_url, asset_role
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
            if row.get("storage_provider") == "r2":
                bucket = row.get("storage_bucket") or R2_BUCKET
                url = _r2_signed_object_url(str(bucket), str(row["storage_path"]))
                if url:
                    urls.append(url)
                continue
            if row.get("public_url"):
                urls.append(str(row["public_url"]))
                continue
            bucket = row.get("storage_bucket") or SUPABASE_MEDIA_BUCKET
            path = row["storage_path"]
            urls.append(_storage_authenticated_object_url(str(bucket), str(path)))
        return urls

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
            days_window = 2 if job_type in ("repair", "poll") else 1
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
                        feeder_handle = _normalize_handle(job.get("handle"))
                        related_handles_by_item = [
                            _extract_related_handles(item, feeder_handle)
                            for item in items
                        ]
                        active_feeder_ids_by_handle = self._active_feeder_ids_by_handle({
                            handle
                            for handles in related_handles_by_item
                            for handle in handles
                        })

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
                            business_day = job.get("business_date_ist")
                            if not isinstance(business_day, date):
                                try:
                                    business_day = date.fromisoformat(str(business_day))
                                except Exception:
                                    business_day = datetime.now(ZoneInfo(APP_TIMEZONE or "Asia/Kolkata")).date()
                            self._resolve_audience_signals_for_feed(feeder_id, business_day)
                            self._set_run_result(jid, "done", att, None, None)
                            continue

                        for item, related_handles in zip(items, related_handles_by_item):
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
                            candidate_feeder_ids = {feeder_id}
                            for related_handle in related_handles:
                                candidate_feeder_ids.update(active_feeder_ids_by_handle.get(related_handle, []))

                            for target_feeder_id in sorted(candidate_feeder_ids):
                                post_key, is_new_post = self._upsert_post(
                                    target_feeder_id,
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
                                self._best_effort_stage_post_media_assets(
                                    post_key,
                                    posted_at,
                                    thumbnail_url,
                                    None,
                                    None,
                                    context="run_job_ingest",
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
        touched: set[tuple[int, str, date]] = set()
        for job in jobs:
            post_key = str(job.get("post_key") or "").strip().lower()
            resolved_post_url = _checkpoint_scrape_url(job)
            checkpoint = str(job.get("checkpoint") or "").strip().lower()
            feeder_id = int(job.get("feeder_id") or 0)
            business_day = self._checkpoint_business_day_for_job(
                _to_dt(job.get("posted_at")),
                checkpoint,
                job.get("next_run_at"),
            )
            availability_status = str(job.get("availability_status") or "active").strip().lower() or "active"

            if availability_status in {"deleted", "unavailable"}:
                jid = int(job["id"])
                att = int(job.get("attempt") or 0)
                reason = str(job.get("availability_error") or "post unavailable").strip() or "post unavailable"
                frozen_message = self._freeze_checkpoint_from_previous_metrics(job, business_day, reason)
                try:
                    self.conn.commit()
                except Exception:
                    try:
                        self.conn.rollback()
                    except Exception:
                        pass
                if frozen_message:
                    self._set_checkpoint_result(jid, "done", att, None, frozen_message)
                    if feeder_id and checkpoint in ("d1", "d3", "d7", "d21") and business_day is not None:
                        touched.add((feeder_id, checkpoint, business_day))
                else:
                    self._set_checkpoint_result(
                        jid,
                        "skipped",
                        att,
                        None,
                        _hard_skip_error(reason, "checkpoint hard failure"),
                    )
                continue

            if not post_key or not resolved_post_url:
                continue
            jobs_by_post_key[post_key].append(job)
            urls_by_post_key.setdefault(post_key, resolved_post_url)
            mode_by_post_key.setdefault(post_key, "reel" if _is_reel_media_type(job.get("media_type")) else "post")

        ordered_post_keys = [post_key for post_key in jobs_by_post_key.keys() if urls_by_post_key.get(post_key)]
        if not ordered_post_keys:
            return

        try:
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
                            feeder_id = int(j.get("feeder_id") or 0)
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
                                na = att + 1
                                err = "Post missing in checkpoint batch"
                                if na <= len(RETRY_BACKOFF_MINUTES):
                                    self._set_checkpoint_result(jid, "retry", na, _next_retry_time(na), err)
                                else:
                                    self._set_checkpoint_result(
                                        jid,
                                        "skipped",
                                        na,
                                        None,
                                        _hard_skip_error(err, "checkpoint hard failure"),
                                    )
                                continue

                            provider_error = _checkpoint_item_error(item)
                            if provider_error:
                                try:
                                    self.conn.rollback()
                                except Exception:
                                    pass
                                if _is_deleted_post_provider_error(provider_error):
                                    business_day = self._checkpoint_business_day_for_job(
                                        job_posted_at,
                                        cp,
                                        j.get("next_run_at"),
                                    )
                                    self._mark_post_availability(job_post_key, "deleted", provider_error)
                                    frozen_message = self._freeze_checkpoint_from_previous_metrics(
                                        j,
                                        business_day,
                                        provider_error,
                                    )
                                    self.conn.commit()
                                    if frozen_message:
                                        self._set_checkpoint_result(jid, "done", att, None, frozen_message)
                                        if feeder_id and cp in ("d1", "d3", "d7", "d21") and business_day is not None:
                                            touched.add((feeder_id, cp, business_day))
                                    else:
                                        self._set_checkpoint_result(
                                            jid,
                                            "skipped",
                                            att,
                                            None,
                                            _hard_skip_error(provider_error, "checkpoint hard failure"),
                                        )
                                else:
                                    self._set_checkpoint_result(
                                        jid,
                                        "skipped",
                                        att,
                                        None,
                                        _hard_skip_error(provider_error, "checkpoint hard failure"),
                                    )
                                continue

                            business_day = self._checkpoint_business_day_for_job(
                                job_posted_at,
                                cp,
                                j.get("next_run_at"),
                            )

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
                            self._mark_post_availability(job_post_key, "active", None)
                            thumbnail_url, video_url, carousel_urls = _extract_media_refs(item)
                            resolved_media_type = _media_type(item) or str(j.get("media_type") or "").strip().lower()
                            self._best_effort_refresh_post_media(
                                str(j["post_key"]),
                                thumbnail_url,
                                video_url,
                                carousel_urls,
                                context=f"checkpoint_{cp}_refresh",
                            )
                            preview_video_url = video_url if _preview_capture_allowed_for_business_day(business_day) else None
                            self._best_effort_stage_post_media_assets(
                                str(j["post_key"]),
                                job_posted_at,
                                thumbnail_url,
                                preview_video_url,
                                None,
                                context=f"checkpoint_{cp}_base",
                            )
                            # Canonical dedupe for metrics is (post_key, checkpoint). Checkpoint re-runs
                            # update the same row while preserving the checkpoint business day stamp.
                            metric_written = self._upsert_metric(str(j["post_key"]), checkpoint, views, likes, comments, business_day)
                            if not metric_written:
                                try:
                                    self.conn.rollback()
                                except Exception:
                                    pass
                                na = att + 1
                                due_at = _to_dt(j.get("next_run_at"))
                                now_utc = datetime.now(timezone.utc)
                                err = "Checkpoint metric insert was rejected before a row was written"
                                if due_at is not None and due_at > now_utc:
                                    self._set_checkpoint_result(jid, "retry", na, due_at, err)
                                elif na <= len(RETRY_BACKOFF_MINUTES):
                                    self._set_checkpoint_result(jid, "retry", na, _next_retry_time(na), err)
                                else:
                                    self._set_checkpoint_result(jid, "failed", na, None, err)
                                continue

                            percentile = self._metric_percentile(str(j["post_key"]), checkpoint)
                            should_extend_visual_media = (
                                cp == "d21"
                                or (cp == "d7" and percentile is not None and percentile <= _HOT_PERCENTILE_MAX)
                            )
                            if (
                                should_extend_visual_media
                                and (
                                    thumbnail_url
                                    or (
                                        _preview_enabled_for_source(video_url)
                                        and _preview_capture_allowed_for_business_day(business_day)
                                    )
                                )
                            ):
                                self._best_effort_stage_post_media_assets(
                                    str(j["post_key"]),
                                    job_posted_at,
                                    thumbnail_url,
                                    video_url,
                                    carousel_urls if resolved_media_type in {"sidecar", "carousel"} else None,
                                    preview_retention_days=_HOT_VISUAL_ASSET_RETENTION_DAYS,
                                    full_video_retention_days=(
                                        _HOT_VISUAL_ASSET_RETENTION_DAYS
                                        if resolved_media_type == "reel"
                                        and _preview_enabled_for_source(video_url)
                                        else None
                                    ),
                                    carousel_retention_days=(
                                        _HOT_VISUAL_ASSET_RETENTION_DAYS
                                        if resolved_media_type in {"sidecar", "carousel"}
                                        else None
                                    ),
                                    stage_preview=_preview_capture_allowed_for_business_day(business_day),
                                    context=f"checkpoint_{cp}_hot_extension",
                                )
                            self.conn.commit()

                            if cp == "d7" and percentile is not None and percentile <= _HOT_PERCENTILE_MAX:
                                self._capture_post_media_assets_for_post_keys(
                                    [str(j["post_key"])],
                                    include_all_roles=True,
                                )

                            self._set_checkpoint_result(jid, "done", att, None, None)

                            if feeder_id and cp in ("d1", "d3", "d7", "d21"):
                                touched.add((feeder_id, cp, business_day))

            recomputed_pairs: set[tuple[int, str]] = set()
            intelligence_triggered_feeders: set[int] = set()

            # Resolver chain for checkpoint jobs once batch writes are done
            for feeder_id, cp, business_day in sorted(touched, key=lambda item: (item[2], item[0], item[1])):
                try:
                    recompute_key = (feeder_id, cp)
                    if recompute_key not in recomputed_pairs:
                        self._recompute_feeder_checkpoint_rankings(feeder_id, cp)
                        recomputed_pairs.add(recompute_key)
                    self._extend_hot_visual_media_for_day(feeder_id, cp, business_day)
                    if cp == "d7" and feeder_id not in intelligence_triggered_feeders:
                        intelligence_triggered_feeders.add(feeder_id)
                        try:
                            intelligence_result = self._run_d7_feeder_file_metric_trigger(feeder_id)
                            stats = intelligence_result.get("intelligence") if isinstance(intelligence_result, dict) else None
                            media_stats = intelligence_result.get("media") if isinstance(intelligence_result, dict) else None
                            if (
                                isinstance(stats, dict)
                                and (stats.get("selected") or stats.get("failed"))
                            ) or (
                                isinstance(media_stats, dict)
                                and (media_stats.get("staged") or media_stats.get("failed"))
                            ):
                                print(
                                    "[feeder-file-trigger] "
                                    f"feeder_id={feeder_id} media={media_stats} intelligence={stats}"
                                )
                        except Exception as intelligence_exc:
                            try:
                                self.conn.rollback()
                            except Exception:
                                pass
                            print(f"[feeder-file-trigger] failed for feeder={feeder_id}: {intelligence_exc}")
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
        """Backfill recent Fire media when thumbnail or preview assets are missing or still on the old provider."""
        active_provider = _active_media_storage_provider()
        rows = self.conn.execute(
            """
            with latest_metric as (
              select
                pm.post_key,
                max(pm.business_date_ist) as business_day,
                bool_or(
                  lower(coalesce(pm.checkpoint, '')) = 'd7'
                  and pm.percentile_performance is not null
                  and pm.percentile_performance <= %s
                ) as hot_d7
              from public.post_metrics pm
              where pm.checkpoint in ('d1', 'd3', 'd7', 'd21')
              group by pm.post_key
            )
            select distinct
              p.post_key,
              p.post_url,
              p.posted_at,
              coalesce(p.posted_at, p.created_at) as sort_posted_at,
              latest_metric.business_day,
              latest_metric.hot_d7
            from public.posts p
            join latest_metric
              on latest_metric.post_key = p.post_key
            where p.post_url is not null
              and coalesce(p.posted_at, p.created_at, now()) >= now() - (%s::text || ' days')::interval
              and (
                coalesce(p.thumbnail_url, '') = ''
                or not exists (
                  select 1
                  from public.post_media_assets assets
                  where assets.post_key = p.post_key
                    and assets.asset_role = 'thumbnail'
                    and assets.status = 'active'
                    and coalesce(assets.storage_provider, 'supabase') = %s
                    and coalesce(assets.storage_path, '') <> ''
                )
                or (
                  lower(coalesce(p.media_type, '')) = 'reel'
                  and coalesce(p.video_url, '') <> ''
                  and not exists (
                    select 1
                    from public.post_media_assets assets
                    where assets.post_key = p.post_key
                      and assets.asset_role = 'preview_5s'
                      and assets.status = 'active'
                      and coalesce(assets.storage_path, '') <> ''
                  )
                )
              )
            order by sort_posted_at desc
            limit %s
            """,
            (_HOT_PERCENTILE_MAX, max(1, days), active_provider, max(1, limit)),
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
            allow_preview_capture = _preview_capture_allowed_for_business_day(r.get("business_day"))
            hot_retention_days = _HOT_VISUAL_ASSET_RETENTION_DAYS if bool(r.get("hot_d7")) else None
            self._refresh_post_media(post_key, thumbnail_url, video_url, carousel_urls)
            self._stage_post_media_assets(
                post_key,
                posted_at,
                thumbnail_url,
                video_url if allow_preview_capture else None,
                carousel_urls,
                preview_retention_days=hot_retention_days,
            )
            updated += 1

        self.conn.commit()
        if updated > 0:
            self.process_post_media_assets(capture_limit=max(40, updated * 3), purge_limit=0)
        return {"selected": len(rows), "updated": updated, "missing": missing}

    def backfill_fire_day_media(self, day: str | None = None, limit: int = 400, batch_size: int = 50) -> dict[str, int]:
        """Backfill thumbnail and 5s preview assets for Fire cards on a specific business day."""
        try:
            business_day = (day or "").strip() or datetime.now(ZoneInfo(APP_TIMEZONE or "Asia/Kolkata")).date().isoformat()
        except Exception:
            business_day = (day or "").strip() or datetime.now(timezone.utc).date().isoformat()
        allow_preview_capture = _preview_capture_allowed_for_business_day(business_day)
        active_provider = _active_media_storage_provider()
        rows = self.conn.execute(
            """
            with day_metric as (
              select
                pm.post_key,
                max(coalesce(pm.computed_at, now())) as sort_ts,
                bool_or(
                  lower(coalesce(pm.checkpoint, '')) = 'd7'
                  and pm.percentile_performance is not null
                  and pm.percentile_performance <= %s
                ) as hot_d7
              from public.post_metrics pm
              where pm.business_date_ist = %s
                and pm.checkpoint in ('d1', 'd3', 'd7', 'd21')
              group by pm.post_key
            )
            select distinct
              p.post_key,
              p.post_url,
              p.posted_at,
              p.thumbnail_url,
              p.video_url,
              p.carousel_urls,
              lower(coalesce(p.media_type, 'image')) as media_type,
              coalesce(day_metric.sort_ts, p.posted_at, p.created_at) as sort_ts,
              coalesce(day_metric.hot_d7, false) as hot_d7
            from public.posts p
            join day_metric
              on day_metric.post_key = p.post_key
            where coalesce(p.post_url, '') <> ''
              and (
                not exists (
                  select 1
                  from public.post_media_assets assets
                  where assets.post_key = p.post_key
                    and assets.asset_role = 'thumbnail'
                    and assets.status = 'active'
                    and coalesce(assets.storage_provider, 'supabase') = %s
                    and coalesce(assets.storage_path, '') <> ''
                )
                or (
                  lower(coalesce(p.media_type, '')) = 'reel'
                  and not exists (
                    select 1
                    from public.post_media_assets assets
                    where assets.post_key = p.post_key
                      and assets.asset_role = 'preview_5s'
                      and assets.status = 'active'
                      and coalesce(assets.storage_path, '') <> ''
                  )
                )
              )
            order by sort_ts desc
            limit %s
            """,
            (_HOT_PERCENTILE_MAX, business_day, active_provider, max(1, limit)),
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
                    fallback_thumbnail_url = str(row.get("thumbnail_url") or "").strip() or None
                    fallback_video_url = str(row.get("video_url") or "").strip() or None
                    raw_carousel_urls = row.get("carousel_urls")
                    if isinstance(raw_carousel_urls, list):
                        fallback_carousel_urls = [str(value).strip() for value in raw_carousel_urls if str(value).strip()]
                    else:
                        fallback_carousel_urls = []
                    short = shortcode_from_url(post_url).lower()
                    item = by_post_key.get(chunk_post_key) or (by_short.get(short) if short else None)
                    posted_at = _to_dt(row.get("posted_at"))
                    if item:
                        thumbnail_url, video_url, carousel_urls = _extract_media_refs(item)
                        thumbnail_url = thumbnail_url or fallback_thumbnail_url
                        video_url = video_url or fallback_video_url
                        carousel_urls = carousel_urls or fallback_carousel_urls
                    else:
                        thumbnail_url = fallback_thumbnail_url
                        video_url = fallback_video_url
                        carousel_urls = fallback_carousel_urls
                        if not thumbnail_url and not video_url and not carousel_urls:
                            missing += 1
                            continue

                    self._refresh_post_media(chunk_post_key, thumbnail_url, video_url, carousel_urls)
                    hot_retention_days = _HOT_VISUAL_ASSET_RETENTION_DAYS if bool(row.get("hot_d7")) else None
                    self._stage_post_media_assets(
                        chunk_post_key,
                        posted_at,
                        thumbnail_url,
                        video_url if allow_preview_capture else None,
                        carousel_urls,
                        preview_retention_days=hot_retention_days,
                    )
                    updated += 1

        self.conn.commit()
        if updated > 0:
            self.process_post_media_assets(capture_limit=max(60, updated * 3), purge_limit=0)
        return {"selected": len(rows), "updated": updated, "missing": missing}

    def repair_post_visual_media(self, post_key: str) -> dict[str, Any]:
        """Refresh, re-stage, and capture visual assets for a single post."""
        normalized_post_key = str(post_key or "").strip().lower()
        if not normalized_post_key:
            return {"found": False, "staged": 0, "captured": 0, "failed": 0, "retired": 0, "assets": []}

        row = self.conn.execute(
            """
            with latest_metric as (
              select
                pm.post_key,
                max(pm.business_date_ist) as business_day,
                bool_or(
                  lower(coalesce(pm.checkpoint, '')) = 'd7'
                  and pm.percentile_performance is not null
                  and pm.percentile_performance <= %s
                ) as hot_d7
              from public.post_metrics pm
              where pm.post_key = %s
                and pm.checkpoint in ('d1', 'd3', 'd7', 'd21')
              group by pm.post_key
            )
            select
              p.post_key,
              p.posted_at,
              lower(coalesce(p.media_type, 'image')) as media_type,
              p.post_url,
              p.thumbnail_url,
              p.video_url,
              p.carousel_urls,
              latest_metric.business_day,
              coalesce(latest_metric.hot_d7, false) as hot_d7,
              thumbnail_asset.source_url as staged_thumbnail_source,
              preview_asset.source_url as staged_preview_source
            from public.posts p
            left join latest_metric
              on latest_metric.post_key = p.post_key
            left join lateral (
              select assets.source_url
              from public.post_media_assets assets
              where assets.post_key = p.post_key
                and assets.asset_role = 'thumbnail'
                and coalesce(assets.source_url, '') <> ''
              order by assets.updated_at desc nulls last, assets.id desc
              limit 1
            ) thumbnail_asset on true
            left join lateral (
              select assets.source_url
              from public.post_media_assets assets
              where assets.post_key = p.post_key
                and assets.asset_role = 'preview_5s'
                and coalesce(assets.source_url, '') <> ''
              order by assets.updated_at desc nulls last, assets.id desc
              limit 1
            ) preview_asset on true
            where p.post_key = %s
            limit 1
            """,
            (_HOT_PERCENTILE_MAX, normalized_post_key, normalized_post_key),
        ).fetchone()
        self.conn.commit()

        if not row:
            return {"found": False, "staged": 0, "captured": 0, "failed": 0, "retired": 0, "assets": []}

        posted_at = _to_dt(row.get("posted_at"))
        business_day = row.get("business_day")
        media_type = str(row.get("media_type") or "").strip().lower()
        thumbnail_url = str(row.get("thumbnail_url") or row.get("staged_thumbnail_source") or "").strip() or None
        video_url = str(row.get("video_url") or row.get("staged_preview_source") or "").strip() or None
        raw_carousel_urls = row.get("carousel_urls")
        carousel_urls = [str(value).strip() for value in raw_carousel_urls if str(value).strip()] if isinstance(raw_carousel_urls, list) else []

        post_url = str(row.get("post_url") or "").strip()
        if post_url:
            try:
                mode = "reel" if _is_reel_media_type(media_type) else "post"
                items = run_actor_post_urls("", [post_url], mode=mode)
                item = items[0] if items else None
                if item:
                    fresh_thumbnail_url, fresh_video_url, fresh_carousel_urls = _extract_media_refs(item)
                    thumbnail_url = fresh_thumbnail_url or thumbnail_url
                    video_url = fresh_video_url or video_url
                    carousel_urls = fresh_carousel_urls or carousel_urls
            except Exception as exc:
                print(f"[media-repair] source refresh failed post_key={normalized_post_key}: {exc}")

        if not thumbnail_url and video_url:
            thumbnail_url = video_url

        allow_preview_capture = _preview_capture_allowed_for_business_day(business_day)
        hot_retention_days = _HOT_VISUAL_ASSET_RETENTION_DAYS if bool(row.get("hot_d7")) else None
        carousel_retention_days = _IMAGE_ASSET_RETENTION_DAYS if media_type in {"sidecar", "carousel"} and carousel_urls else None
        full_video_retention_days = _HEAVY_ASSET_RETENTION_DAYS if _is_reel_media_type(media_type) and video_url else None

        if thumbnail_url or (allow_preview_capture and video_url) or carousel_urls:
            self._refresh_post_media(normalized_post_key, thumbnail_url, video_url, carousel_urls)
            self._stage_post_media_assets(
                normalized_post_key,
                posted_at,
                thumbnail_url,
                video_url if allow_preview_capture else None,
                carousel_urls,
                preview_retention_days=hot_retention_days,
                full_video_retention_days=full_video_retention_days,
                carousel_retention_days=carousel_retention_days,
            )
            self.conn.commit()

        capture_result = self._capture_post_media_assets_for_post_keys([normalized_post_key], include_all_roles=True)
        retire_result = self._retire_legacy_post_media_rows([normalized_post_key], limit=25)
        assets = self.conn.execute(
            """
            select asset_role, status, storage_provider, public_url, storage_path, purge_after, updated_at
            from public.post_media_assets
            where post_key = %s
              and (
                asset_role in ('thumbnail', 'preview_5s', 'video_full')
                or asset_role like 'carousel_%%'
              )
            order by asset_role
            """,
            (normalized_post_key,),
        ).fetchall()
        self.conn.commit()
        return {
            "found": True,
            "staged": 1,
            "captured": capture_result.get("captured", 0),
            "failed": capture_result.get("failed", 0),
            "retired": retire_result.get("marked", 0),
            "assets": assets,
        }

    def migrate_stored_supabase_visual_media_to_r2(self, limit: int = 500) -> dict[str, int]:
        """Copy recoverable Supabase-stored thumbnails/previews into R2 before any source-url fallback work."""
        active_provider = _active_media_storage_provider()
        if active_provider != "r2":
            raise RuntimeError("migrate_stored_supabase_visual_media_to_r2 requires MEDIA_STORAGE_PROVIDER=r2")

        rows = self.conn.execute(
            """
            select
              id,
              post_key,
              asset_role,
              source_url,
              storage_provider,
              storage_bucket,
              storage_path,
              public_url,
              mime_type,
              status,
              updated_at
            from public.post_media_assets
            where lower(coalesce(storage_provider, 'supabase')) = 'supabase'
              and lower(coalesce(asset_role, '')) in ('thumbnail', 'preview_5s', 'display')
              and status in ('active', 'purge_pending')
              and coalesce(storage_path, '') <> ''
            order by updated_at desc nulls last, id desc
            limit %s
            """,
            (max(1, limit),),
        ).fetchall()
        self.conn.commit()

        if not rows:
            return {"selected": 0, "migrated": 0, "missing": 0, "failed": 0}

        migrated = 0
        missing = 0
        failed = 0

        for row in rows:
            old_asset = dict(row)
            try:
                body = self._fetch_stored_post_media_asset_bytes(old_asset)
                if not body:
                    missing += 1
                    continue

                content_type = str(row.get("mime_type") or "").strip().lower()
                if not content_type:
                    guessed_type, _ = mimetypes.guess_type(str(row.get("storage_path") or row.get("source_url") or ""))
                    if guessed_type:
                        content_type = guessed_type
                    elif str(row.get("asset_role") or "").strip().lower() == "preview_5s":
                        content_type = "video/mp4"
                    else:
                        content_type = "image/jpeg"

                target_asset = dict(old_asset)
                target_asset["storage_provider"] = "r2"
                target_asset["storage_bucket"] = _active_media_bucket()
                storage_provider, storage_path, public_url = self._store_post_media_asset_body(target_asset, body, content_type)
                self._set_post_media_asset_result(
                    int(row["id"]),
                    str(row.get("status") or "active"),
                    attempt=0,
                    next_run_at=None,
                    storage_provider=storage_provider,
                    storage_bucket=target_asset["storage_bucket"],
                    storage_path=storage_path,
                    public_url=public_url,
                    mime_type=content_type,
                    byte_size=len(body),
                    captured_at=datetime.now(timezone.utc),
                    deleted_at=None,
                    error=None,
                )
                try:
                    self._delete_post_media_object(old_asset)
                except Exception:
                    self.conn.rollback()
                    self._set_post_media_asset_result(
                        int(row["id"]),
                        str(row.get("status") or "active"),
                        error="migrated to r2 but failed to delete legacy supabase object",
                    )
                migrated += 1
            except Exception as exc:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                failed += 1
                self._set_post_media_asset_result(
                    int(row["id"]),
                    str(row.get("status") or "capture_failed"),
                    error=f"supabase-to-r2 copy failed: {str(exc)[:300]}",
                )

        return {"selected": len(rows), "migrated": migrated, "missing": missing, "failed": failed}

    def restore_recent_thumbnails_from_post_pages(self, limit: int = 500, days: int = 90) -> dict[str, int]:
        """Recover missing R2 thumbnails from Instagram page metadata without scraping media datasets."""
        active_provider = _active_media_storage_provider()
        if active_provider != "r2":
            raise RuntimeError("restore_recent_thumbnails_from_post_pages requires MEDIA_STORAGE_PROVIDER=r2")

        rows = self.conn.execute(
            """
            select
              p.post_key,
              p.post_url,
              p.posted_at
            from public.posts p
            where coalesce(p.post_url, '') <> ''
              and coalesce(p.posted_at, p.created_at, now()) >= now() - (%s::text || ' days')::interval
              and not exists (
                select 1
                from public.post_media_assets assets
                where assets.post_key = p.post_key
                  and assets.asset_role = 'thumbnail'
                  and assets.status in ('active', 'purge_pending')
                  and coalesce(assets.storage_provider, 'supabase') = 'r2'
                  and coalesce(assets.storage_path, '') <> ''
              )
            order by coalesce(p.posted_at, p.created_at) desc, p.post_key desc
            limit %s
            """,
            (max(1, days), max(1, limit)),
        ).fetchall()
        self.conn.commit()

        if not rows:
            return {"selected": 0, "staged": 0, "captured": 0, "failed": 0, "missing": 0}

        staged_post_keys: list[str] = []
        missing = 0
        for row in rows:
            post_key = str(row.get("post_key") or "").strip().lower()
            post_url = str(row.get("post_url") or "").strip()
            if not post_key or not post_url:
                missing += 1
                continue
            try:
                thumbnail_url = self._fetch_instagram_post_page_thumbnail_url(post_url)
            except Exception:
                thumbnail_url = None
            if not thumbnail_url:
                missing += 1
                continue

            posted_at = _to_dt(row.get("posted_at"))
            self._refresh_post_media(post_key, thumbnail_url, None, None)
            self._stage_post_media_assets(post_key, posted_at, thumbnail_url, None, None)
            staged_post_keys.append(post_key)

        self.conn.commit()
        capture_result = self._capture_post_media_assets_for_post_keys(staged_post_keys, asset_roles=("thumbnail",))
        return {
            "selected": len(rows),
            "staged": len(staged_post_keys),
            "captured": capture_result.get("captured", 0),
            "failed": capture_result.get("failed", 0),
            "missing": missing,
        }

    def refresh_recent_visual_media_sources(
        self,
        limit: int = 500,
        days: int = 90,
        batch_size: int = 25,
    ) -> dict[str, int]:
        """Refresh recent posts missing R2 thumbnails or eligible previews using the current worker pipeline."""
        active_provider = _active_media_storage_provider()
        if active_provider != "r2":
            raise RuntimeError("refresh_recent_visual_media_sources requires MEDIA_STORAGE_PROVIDER=r2")

        rows = self.conn.execute(
            """
            with latest_metric as (
              select
                pm.post_key,
                max(pm.business_date_ist)::text as business_day,
                bool_or(
                  lower(coalesce(pm.checkpoint, '')) = 'd7'
                  and pm.percentile_performance is not null
                  and pm.percentile_performance <= %s
                ) as hot_d7
              from public.post_metrics pm
              where pm.checkpoint in ('d1', 'd3', 'd7', 'd21')
              group by pm.post_key
            )
            select
              p.post_key,
              p.post_url,
              p.posted_at,
              lower(coalesce(p.media_type, 'image')) as media_type,
              p.thumbnail_url,
              p.video_url,
              p.carousel_urls,
              p.carousel_slide_count,
              coalesce(
                latest_metric.business_day,
                ((coalesce(p.posted_at, p.created_at, now()) at time zone 'Asia/Kolkata')::date)::text
              ) as business_day,
              coalesce(latest_metric.hot_d7, false) as hot_d7,
              (
                select count(*)
                from public.post_media_assets assets
                where assets.post_key = p.post_key
                  and assets.asset_role like 'carousel_%%'
                  and assets.status in ('active', 'purge_pending')
                  and coalesce(assets.storage_provider, 'supabase') = 'r2'
                  and coalesce(assets.storage_path, '') <> ''
              ) as active_carousel_count,
              exists (
                select 1
                from public.post_media_assets assets
                where assets.post_key = p.post_key
                  and assets.asset_role = 'thumbnail'
                  and assets.status in ('active', 'purge_pending')
                  and coalesce(assets.storage_provider, 'supabase') = 'r2'
                  and coalesce(assets.storage_path, '') <> ''
              ) as has_r2_thumbnail,
              exists (
                select 1
                from public.post_media_assets assets
                where assets.post_key = p.post_key
                  and assets.asset_role = 'preview_5s'
                  and assets.status in ('active', 'purge_pending')
                  and coalesce(assets.storage_provider, 'supabase') = 'r2'
                  and coalesce(assets.storage_path, '') <> ''
              ) as has_r2_preview
            from public.posts p
            left join latest_metric
              on latest_metric.post_key = p.post_key
            where coalesce(p.post_url, '') <> ''
              and coalesce(p.posted_at, p.created_at, now()) >= now() - (%s::text || ' days')::interval
              and (
                not exists (
                  select 1
                  from public.post_media_assets assets
                  where assets.post_key = p.post_key
                    and assets.asset_role = 'thumbnail'
                    and assets.status in ('active', 'purge_pending')
                    and coalesce(assets.storage_provider, 'supabase') = 'r2'
                    and coalesce(assets.storage_path, '') <> ''
                )
                or (
                  lower(coalesce(p.media_type, '')) in ('sidecar', 'carousel', 'album')
                  and (
                    select count(*)
                    from public.post_media_assets assets
                    where assets.post_key = p.post_key
                      and assets.asset_role like 'carousel_%%'
                      and assets.status in ('active', 'purge_pending')
                      and coalesce(assets.storage_provider, 'supabase') = 'r2'
                      and coalesce(assets.storage_path, '') <> ''
                  ) < greatest(
                    coalesce(p.carousel_slide_count, 0),
                    case when jsonb_typeof(p.carousel_urls) = 'array' then jsonb_array_length(p.carousel_urls) else 0 end
                  )
                )
                or (
                  lower(coalesce(p.media_type, '')) = 'reel'
                  and coalesce(
                    latest_metric.business_day,
                    ((coalesce(p.posted_at, p.created_at, now()) at time zone 'Asia/Kolkata')::date)::text
                  ) >= %s
                  and not exists (
                    select 1
                    from public.post_media_assets assets
                    where assets.post_key = p.post_key
                      and assets.asset_role = 'preview_5s'
                      and assets.status in ('active', 'purge_pending')
                      and coalesce(assets.storage_provider, 'supabase') = 'r2'
                      and coalesce(assets.storage_path, '') <> ''
                  )
                )
              )
            order by coalesce(p.posted_at, p.created_at) desc, p.post_key desc
            limit %s
            """,
            (_HOT_PERCENTILE_MAX, max(1, days), _PREVIEW_CAPTURE_START_DAY, max(1, limit)),
        ).fetchall()
        self.conn.commit()

        if not rows:
            retire_result = self._retire_legacy_post_media_rows(None, limit=max(500, limit * 4))
            return {
                "selected": 0,
                "staged": 0,
                "captured": 0,
                "failed": 0,
                "missing": 0,
                "retired": retire_result.get("marked", 0),
            }

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
            return {"selected": len(rows), "staged": 0, "captured": 0, "failed": 0, "missing": len(rows), "retired": 0}

        staged_post_keys: list[str] = []
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
                    fallback_thumbnail_url = str(row.get("thumbnail_url") or "").strip() or None
                    fallback_video_url = str(row.get("video_url") or "").strip() or None
                    raw_carousel_urls = row.get("carousel_urls")
                    if isinstance(raw_carousel_urls, list):
                        fallback_carousel_urls = [str(value).strip() for value in raw_carousel_urls if str(value).strip()]
                    else:
                        fallback_carousel_urls = []

                    short = shortcode_from_url(post_url).lower()
                    item = by_post_key.get(chunk_post_key) or (by_short.get(short) if short else None)
                    if item:
                        thumbnail_url, video_url, carousel_urls = _extract_media_refs(item)
                        thumbnail_url = thumbnail_url or fallback_thumbnail_url
                        video_url = video_url or fallback_video_url
                        carousel_urls = carousel_urls or fallback_carousel_urls
                    else:
                        thumbnail_url = fallback_thumbnail_url
                        video_url = fallback_video_url
                        carousel_urls = fallback_carousel_urls

                    if not thumbnail_url and video_url:
                        thumbnail_url = video_url

                    allow_preview_capture = _preview_capture_allowed_for_business_day(row.get("business_day"))
                    needs_thumbnail = not bool(row.get("has_r2_thumbnail"))
                    needs_preview = (
                        _is_reel_media_type(row.get("media_type"))
                        and allow_preview_capture
                        and not bool(row.get("has_r2_preview"))
                    )
                    expected_carousel_count = max(
                        int(row.get("carousel_slide_count") or 0),
                        len(carousel_urls or fallback_carousel_urls),
                    )
                    needs_carousel = (
                        str(row.get("media_type") or "").strip().lower() in {"sidecar", "carousel", "album"}
                        and expected_carousel_count > 0
                        and int(row.get("active_carousel_count") or 0) < expected_carousel_count
                    )

                    if needs_thumbnail and not thumbnail_url:
                        missing += 1
                    if needs_preview and not video_url:
                        missing += 1
                    if needs_carousel and not carousel_urls:
                        missing += 1

                    staged_thumbnail_url = thumbnail_url if needs_thumbnail else None
                    staged_preview_url = video_url if needs_preview else None
                    staged_carousel_urls = carousel_urls if needs_carousel else []
                    if not staged_thumbnail_url and not staged_preview_url and not staged_carousel_urls:
                        continue

                    posted_at = _to_dt(row.get("posted_at"))
                    hot_retention_days = _HOT_VISUAL_ASSET_RETENTION_DAYS if bool(row.get("hot_d7")) else None
                    self._refresh_post_media(chunk_post_key, thumbnail_url, video_url, carousel_urls)
                    self._stage_post_media_assets(
                        chunk_post_key,
                        posted_at,
                        staged_thumbnail_url,
                        staged_preview_url,
                        staged_carousel_urls,
                        preview_retention_days=hot_retention_days,
                        carousel_retention_days=_IMAGE_ASSET_RETENTION_DAYS if staged_carousel_urls else None,
                    )
                    staged_post_keys.append(chunk_post_key)

        self.conn.commit()
        capture_result = self._capture_post_media_assets_for_post_keys(staged_post_keys, include_all_roles=True)
        retire_result = (
            self._retire_legacy_post_media_rows(staged_post_keys, limit=max(250, len(staged_post_keys) * 4))
            if staged_post_keys
            else {"marked": 0}
        )
        return {
            "selected": len(rows),
            "staged": len(staged_post_keys),
            "captured": capture_result.get("captured", 0),
            "failed": capture_result.get("failed", 0),
            "missing": missing,
            "retired": retire_result.get("marked", 0),
        }

    def migrate_visual_media_to_r2(self, limit: int = 500, days: int = 30) -> dict[str, int]:
        """Migrate recent visual media to R2 using existing post/source URLs, then retire legacy Supabase assets."""
        active_provider = _active_media_storage_provider()
        if active_provider != "r2":
            raise RuntimeError("migrate_visual_media_to_r2 requires MEDIA_STORAGE_PROVIDER=r2")

        copied_result = self.migrate_stored_supabase_visual_media_to_r2(limit=max(100, limit))

        rows = self.conn.execute(
            """
            with latest_metric as (
              select
                pm.post_key,
                max(pm.business_date_ist) as business_day,
                bool_or(
                  lower(coalesce(pm.checkpoint, '')) = 'd7'
                  and pm.percentile_performance is not null
                  and pm.percentile_performance <= %s
                ) as hot_d7
              from public.post_metrics pm
              where pm.checkpoint in ('d1', 'd3', 'd7', 'd21')
              group by pm.post_key
            )
            select
              p.post_key,
              p.posted_at,
              p.post_url,
              lower(coalesce(p.media_type, 'image')) as media_type,
              p.thumbnail_url,
              p.video_url,
              p.carousel_urls,
              p.carousel_slide_count,
              latest_metric.business_day,
              coalesce(latest_metric.hot_d7, false) as hot_d7,
              (
                select count(*)
                from public.post_media_assets assets
                where assets.post_key = p.post_key
                  and assets.asset_role like 'carousel_%%'
                  and assets.status in ('active', 'purge_pending')
                  and coalesce(assets.storage_provider, 'supabase') = 'r2'
                  and coalesce(assets.storage_path, '') <> ''
              ) as active_carousel_count,
              thumbnail_asset.source_url as staged_thumbnail_source,
              preview_asset.source_url as staged_preview_source,
              exists (
                select 1
                from public.post_media_assets assets
                where assets.post_key = p.post_key
                  and assets.asset_role = 'thumbnail'
                  and assets.status in ('active', 'purge_pending')
                  and coalesce(assets.storage_provider, 'supabase') = 'r2'
                  and coalesce(assets.storage_path, '') <> ''
              ) as has_r2_thumbnail,
              exists (
                select 1
                from public.post_media_assets assets
                where assets.post_key = p.post_key
                  and assets.asset_role = 'preview_5s'
                  and assets.status in ('active', 'purge_pending')
                  and coalesce(assets.storage_provider, 'supabase') = 'r2'
                  and coalesce(assets.storage_path, '') <> ''
              ) as has_r2_preview,
              exists (
                select 1
                from public.post_media_assets assets
                where assets.post_key = p.post_key
                  and assets.status <> 'deleted'
                  and (
                    coalesce(assets.storage_provider, 'supabase') = 'supabase'
                    or lower(coalesce(assets.asset_role, '')) = 'video'
                    or lower(coalesce(assets.asset_role, '')) like 'carousel_%%'
                  )
              ) as has_legacy_media
            from public.posts p
            join latest_metric
              on latest_metric.post_key = p.post_key
            left join lateral (
              select assets.source_url
              from public.post_media_assets assets
              where assets.post_key = p.post_key
                and assets.asset_role = 'thumbnail'
                and coalesce(assets.source_url, '') <> ''
              order by assets.updated_at desc nulls last, assets.id desc
              limit 1
            ) thumbnail_asset on true
            left join lateral (
              select assets.source_url
              from public.post_media_assets assets
              where assets.post_key = p.post_key
                and assets.asset_role = 'preview_5s'
                and coalesce(assets.source_url, '') <> ''
              order by assets.updated_at desc nulls last, assets.id desc
              limit 1
            ) preview_asset on true
            where coalesce(p.post_url, '') <> ''
              and coalesce(p.posted_at, p.created_at, now()) >= now() - (%s::text || ' days')::interval
              and (
                coalesce(p.thumbnail_url, '') <> ''
                or coalesce(thumbnail_asset.source_url, '') <> ''
                or coalesce(p.video_url, '') <> ''
                or coalesce(preview_asset.source_url, '') <> ''
              )
              and (
                not exists (
                  select 1
                  from public.post_media_assets assets
                  where assets.post_key = p.post_key
                    and assets.asset_role = 'thumbnail'
                    and assets.status in ('active', 'purge_pending')
                    and coalesce(assets.storage_provider, 'supabase') = 'r2'
                    and coalesce(assets.storage_path, '') <> ''
                )
                or (
                  lower(coalesce(p.media_type, '')) in ('sidecar', 'carousel', 'album')
                  and (
                    select count(*)
                    from public.post_media_assets assets
                    where assets.post_key = p.post_key
                      and assets.asset_role like 'carousel_%%'
                      and assets.status in ('active', 'purge_pending')
                      and coalesce(assets.storage_provider, 'supabase') = 'r2'
                      and coalesce(assets.storage_path, '') <> ''
                  ) < greatest(
                    coalesce(p.carousel_slide_count, 0),
                    case when jsonb_typeof(p.carousel_urls) = 'array' then jsonb_array_length(p.carousel_urls) else 0 end
                  )
                )
                or (
                  lower(coalesce(p.media_type, '')) = 'reel'
                  and latest_metric.business_day >= %s
                  and not exists (
                    select 1
                    from public.post_media_assets assets
                    where assets.post_key = p.post_key
                      and assets.asset_role = 'preview_5s'
                      and assets.status in ('active', 'purge_pending')
                      and coalesce(assets.storage_provider, 'supabase') = 'r2'
                      and coalesce(assets.storage_path, '') <> ''
                  )
                )
                or exists (
                  select 1
                  from public.post_media_assets assets
                  where assets.post_key = p.post_key
                    and assets.status <> 'deleted'
                    and (
                      coalesce(assets.storage_provider, 'supabase') = 'supabase'
                      or lower(coalesce(assets.asset_role, '')) = 'video'
                      or lower(coalesce(assets.asset_role, '')) like 'carousel_%%'
                    )
                )
              )
            order by coalesce(latest_metric.business_day, (coalesce(p.posted_at, p.created_at) at time zone 'Asia/Kolkata')::date) desc,
                     coalesce(p.posted_at, p.created_at) desc,
                     p.post_key desc
            limit %s
            """,
            (_HOT_PERCENTILE_MAX, max(1, days), _PREVIEW_CAPTURE_START_DAY, max(1, limit)),
        ).fetchall()
        self.conn.commit()

        if not rows:
            cleanup = self._retire_legacy_post_media_rows(None, limit=max(500, limit * 4))
            return {
                "selected": 0,
                "copied": copied_result.get("migrated", 0),
                "staged": 0,
                "captured": 0,
                "failed": 0,
                "missing": copied_result.get("missing", 0),
                "retired": cleanup.get("marked", 0),
            }

        staged_post_keys: list[str] = []
        missing = 0
        for row in rows:
            post_key = str(row.get("post_key") or "").strip().lower()
            posted_at = _to_dt(row.get("posted_at"))
            business_day = row.get("business_day")
            thumbnail_url = str(row.get("thumbnail_url") or row.get("staged_thumbnail_source") or "").strip() or None
            video_url = str(row.get("video_url") or row.get("staged_preview_source") or "").strip() or None
            raw_carousel_urls = row.get("carousel_urls")
            carousel_urls = [str(value).strip() for value in raw_carousel_urls if str(value).strip()] if isinstance(raw_carousel_urls, list) else []
            if not thumbnail_url and video_url:
                thumbnail_url = video_url

            allow_preview_capture = _preview_capture_allowed_for_business_day(business_day)
            hot_retention_days = _HOT_VISUAL_ASSET_RETENTION_DAYS if bool(row.get("hot_d7")) else None
            expected_carousel_count = max(int(row.get("carousel_slide_count") or 0), len(carousel_urls))
            needs_carousel = (
                str(row.get("media_type") or "").strip().lower() in {"sidecar", "carousel", "album"}
                and expected_carousel_count > 0
                and int(row.get("active_carousel_count") or 0) < expected_carousel_count
            )

            if not thumbnail_url and not (allow_preview_capture and video_url) and not (needs_carousel and carousel_urls):
                missing += 1
                continue

            self._refresh_post_media(post_key, thumbnail_url, video_url, carousel_urls)
            self._stage_post_media_assets(
                post_key,
                posted_at,
                thumbnail_url,
                video_url if allow_preview_capture else None,
                carousel_urls,
                preview_retention_days=hot_retention_days,
                carousel_retention_days=_IMAGE_ASSET_RETENTION_DAYS if needs_carousel else None,
            )
            staged_post_keys.append(post_key)

        self.conn.commit()
        capture_result = self._capture_post_media_assets_for_post_keys(staged_post_keys, include_all_roles=True)
        retire_result = self._retire_legacy_post_media_rows(None, limit=max(500, limit * 4))
        return {
            "selected": len(rows),
            "copied": copied_result.get("migrated", 0),
            "staged": len(staged_post_keys),
            "captured": capture_result.get("captured", 0),
            "failed": capture_result.get("failed", 0) + copied_result.get("failed", 0),
            "missing": missing + copied_result.get("missing", 0),
            "retired": retire_result.get("marked", 0),
        }

    def purge_preview_assets_before_day(self, day: str | None = None, limit: int = 500) -> dict[str, int]:
        """Delete preview clips for posts whose latest Fire business day is before the given day."""
        try:
            cutoff_day = (day or "").strip() or datetime.now(ZoneInfo(APP_TIMEZONE or "Asia/Kolkata")).date().isoformat()
        except Exception:
            cutoff_day = (day or "").strip() or datetime.now(timezone.utc).date().isoformat()
        rows = self.conn.execute(
            """
            with candidates as (
              select
                assets.id
              from public.post_media_assets assets
              left join lateral (
                select max(pm.business_date_ist) as latest_business_day
                from public.post_metrics pm
                where pm.post_key = assets.post_key
                  and pm.checkpoint in ('d1', 'd3', 'd7', 'd21')
              ) latest on true
              where assets.asset_role = 'preview_5s'
                and assets.status = 'active'
                and coalesce(latest.latest_business_day, '0001-01-01') < %s
              order by assets.updated_at desc
              limit %s
            )
            update public.post_media_assets assets
            set status = 'purge_pending',
                attempt = 0,
                next_run_at = now(),
                last_error = null,
                updated_at = now()
            from candidates
            where assets.id = candidates.id
            returning assets.id
            """,
            (cutoff_day, max(1, limit)),
        ).fetchall()
        self.conn.commit()

        marked = len(rows)
        if marked > 0:
            self.process_post_media_assets(capture_limit=0, purge_limit=max(80, marked))
        return {"marked": marked, "purged": marked}

    def repair_overlong_preview_assets(self, limit: int = 250) -> dict[str, int]:
        """Requeue active preview clips whose stored duration exceeds the 5s product cap."""
        rows = self.conn.execute(
            """
            select
              id,
              post_key,
              asset_role,
              source_url,
              storage_provider,
              storage_bucket,
              storage_path,
              public_url,
              mime_type,
              status,
              updated_at
            from public.post_media_assets
            where asset_role = 'preview_5s'
              and status = 'active'
              and coalesce(storage_path, '') <> ''
            order by updated_at desc, id desc
            limit %s
            """,
            (max(1, limit),),
        ).fetchall()
        self.conn.commit()

        if not rows:
            return {"scanned": 0, "requeued": 0, "missing": 0, "valid": 0}

        scanned = 0
        requeued = 0
        missing = 0
        valid = 0
        max_allowed_duration = _PREVIEW_MAX_DURATION_SECONDS + _PREVIEW_DURATION_TOLERANCE_SECONDS

        for row in rows:
            scanned += 1
            try:
                body = self._fetch_stored_post_media_asset_bytes(row)
                if not body:
                    missing += 1
                    self._reset_post_media_asset_for_recapture(
                        int(row["id"]),
                        error="stored preview asset missing; requeued for recapture",
                    )
                    continue

                duration_seconds = _probe_media_duration_seconds(body, suffix=".mp4")
                if duration_seconds is None:
                    self._delete_post_media_object(row)
                    self._reset_post_media_asset_for_recapture(
                        int(row["id"]),
                        error="preview duration probe failed; requeued for recapture",
                    )
                    requeued += 1
                    continue

                if duration_seconds > max_allowed_duration:
                    self._delete_post_media_object(row)
                    self._reset_post_media_asset_for_recapture(
                        int(row["id"]),
                        error=(
                            f"preview exceeded {max_allowed_duration:.2f}s cap "
                            f"({duration_seconds:.3f}s); requeued for recapture"
                        ),
                    )
                    requeued += 1
                    continue

                valid += 1
            except Exception as exc:
                self.conn.rollback()
                self._reset_post_media_asset_for_recapture(
                    int(row["id"]),
                    error=f"preview repair reset after error: {str(exc)[:300]}",
                )
                requeued += 1

        if requeued > 0 or missing > 0:
            self.process_post_media_assets(capture_limit=max(40, requeued + missing), purge_limit=0)

        return {
            "scanned": scanned,
            "requeued": requeued,
            "missing": missing,
            "valid": valid,
        }

    def refresh_fire_preview_sources_from_day(
        self,
        day: str | None = None,
        limit: int = 250,
        batch_size: int = 25,
        stale_minutes: int = 30,
    ) -> dict[str, int]:
        """Refresh April-14+ reel preview source URLs and requeue failed/stale rows for the live worker."""
        try:
            business_day = (day or "").strip() or datetime.now(ZoneInfo(APP_TIMEZONE or "Asia/Kolkata")).date().isoformat()
        except Exception:
            business_day = (day or "").strip() or datetime.now(timezone.utc).date().isoformat()

        target_row = self.conn.execute(
            """
            select storage_provider, storage_bucket
            from public.post_media_assets
            where asset_role = 'preview_5s'
              and status = 'active'
              and coalesce(storage_path, '') <> ''
            order by updated_at desc, id desc
            limit 1
            """
        ).fetchone() or {}
        inferred_provider = str(target_row.get("storage_provider") or _active_media_storage_provider() or "r2").strip().lower() or "r2"
        inferred_bucket = str(target_row.get("storage_bucket") or _active_media_bucket() or R2_BUCKET or SUPABASE_MEDIA_BUCKET).strip()

        rows = self.conn.execute(
            """
            select distinct on (p.post_key)
              p.post_key,
              p.post_url,
              p.posted_at,
              p.thumbnail_url,
              p.video_url,
              p.carousel_urls,
              exists (
                select 1
                from public.post_metrics hot_pm
                where hot_pm.post_key = p.post_key
                  and lower(coalesce(hot_pm.checkpoint, '')) = 'd7'
                  and hot_pm.percentile_performance is not null
                  and hot_pm.percentile_performance <= %s
              ) as hot_d7,
              assets.id as asset_id,
              assets.status as asset_status,
              assets.source_url as asset_source_url,
              assets.storage_provider,
              assets.storage_bucket,
              assets.updated_at as asset_updated_at
            from public.posts p
            join public.post_metrics pm
              on pm.post_key = p.post_key
             and pm.business_date_ist >= %s
             and pm.checkpoint in ('d1', 'd3', 'd7', 'd21')
            left join public.post_media_assets assets
              on assets.post_key = p.post_key
             and assets.asset_role = 'preview_5s'
            where lower(coalesce(p.media_type, '')) = 'reel'
              and coalesce(p.post_url, '') <> ''
              and (
                assets.id is null
                or assets.status = 'capture_failed'
                or (
                  assets.status = 'capturing'
                  and coalesce(assets.updated_at, now() - interval '365 days') <= now() - (%s::text || ' minutes')::interval
                )
                or (
                  assets.status <> 'active'
                  and coalesce(assets.storage_path, '') = ''
                )
              )
            order by p.post_key, pm.business_date_ist desc, pm.computed_at desc nulls last
            limit %s
            """,
            (_HOT_PERCENTILE_MAX, business_day, max(1, stale_minutes), max(1, limit)),
        ).fetchall()
        self.conn.commit()

        if not rows:
            return {"selected": 0, "updated": 0, "missing": 0}

        rows_by_post_key: dict[str, dict] = {}
        urls_by_post_key: dict[str, str] = {}
        for row in rows:
            post_key = str(row.get("post_key") or "").strip().lower()
            post_url = str(row.get("post_url") or "").strip()
            if not post_key or not post_url:
                continue
            rows_by_post_key[post_key] = row
            urls_by_post_key[post_key] = post_url

        if not rows_by_post_key:
            return {"selected": len(rows), "updated": 0, "missing": len(rows)}

        updated = 0
        missing = 0
        ordered_post_keys = list(rows_by_post_key.keys())

        for post_key_chunk in _chunk_list(ordered_post_keys, max(1, batch_size)):
            chunk_urls = [urls_by_post_key[post_key] for post_key in post_key_chunk]
            items = run_actor_post_urls("", chunk_urls, mode="reel")

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

                fallback_thumbnail_url = str(row.get("thumbnail_url") or "").strip() or None
                fallback_video_url = str(row.get("video_url") or "").strip() or None
                raw_carousel_urls = row.get("carousel_urls")
                if isinstance(raw_carousel_urls, list):
                    fallback_carousel_urls = [str(value).strip() for value in raw_carousel_urls if str(value).strip()]
                else:
                    fallback_carousel_urls = []

                if item:
                    thumbnail_url, video_url, carousel_urls = _extract_media_refs(item)
                    thumbnail_url = thumbnail_url or fallback_thumbnail_url
                    video_url = video_url or fallback_video_url
                    carousel_urls = carousel_urls or fallback_carousel_urls
                else:
                    thumbnail_url = fallback_thumbnail_url
                    video_url = fallback_video_url
                    carousel_urls = fallback_carousel_urls

                if not video_url:
                    missing += 1
                    continue

                posted_at = _to_dt(row.get("posted_at"))
                storage_provider = str(row.get("storage_provider") or inferred_provider).strip().lower() or inferred_provider
                storage_bucket = str(row.get("storage_bucket") or inferred_bucket).strip() or inferred_bucket
                purge_after = _post_media_rollover_deadline(
                    posted_at,
                    "preview_5s",
                    _HOT_VISUAL_ASSET_RETENTION_DAYS if bool(row.get("hot_d7")) else None,
                )

                self._refresh_post_media(chunk_post_key, thumbnail_url, video_url, carousel_urls)
                self.conn.execute(
                    """
                    insert into public.post_media_assets (
                      post_key, asset_role, source_url, storage_provider, storage_bucket, status, attempt,
                      next_run_at, purge_after, storage_path, public_url, mime_type, byte_size,
                      captured_at, deleted_at, last_error, updated_at
                    )
                    values (
                      %s,
                      'preview_5s',
                      %s,
                      %s,
                      %s,
                      'pending_capture',
                      0,
                      now(),
                      %s,
                      null,
                      null,
                      null,
                      null,
                      null,
                      null,
                      null,
                      now()
                    )
                    on conflict (post_key, asset_role) do update
                    set source_url = excluded.source_url,
                        storage_provider = coalesce(public.post_media_assets.storage_provider, excluded.storage_provider),
                        storage_bucket = coalesce(public.post_media_assets.storage_bucket, excluded.storage_bucket),
                        status = 'pending_capture',
                        attempt = 0,
                        next_run_at = now(),
                        purge_after = excluded.purge_after,
                        storage_path = null,
                        public_url = null,
                        mime_type = null,
                        byte_size = null,
                        captured_at = null,
                        deleted_at = null,
                        last_error = null,
                        updated_at = now()
                    """,
                    (chunk_post_key, video_url, storage_provider, storage_bucket, purge_after),
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
                        None,
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


def run_fingerprint_once(limit: int | None = None, feeder_id: int | None = None, days: int = 90) -> dict[str, dict]:
    eng = PureEngine()
    try:
        eng.ensure_connection("fingerprint_once start", verify=True)
        result: dict[str, dict] = {}
        if FEEDER_INTELLIGENCE_ENABLED:
            result["fingerprints"] = eng.fingerprint_reels(
                feeder_id=feeder_id,
                limit=max(1, int(limit or FEEDER_INTELLIGENCE_AUTO_LIMIT)),
                days=days,
            )
        return result
    finally:
        eng.close()


def run_fingerprint_worker(loop_sleep_seconds: int = 30):
    eng = PureEngine()
    print(
        "[fingerprint-worker] "
        f"enabled={FEEDER_INTELLIGENCE_ENABLED} "
        f"interval={FEEDER_INTELLIGENCE_AUTO_INTERVAL_SECONDS}s "
        f"limit={FEEDER_INTELLIGENCE_AUTO_LIMIT}"
    )
    try:
        last_run = 0.0
        while True:
            now_ts = time.time()
            try:
                eng.ensure_connection("fingerprint worker heartbeat", verify=True)
            except Exception as e:
                print(f"[fingerprint-worker] db reconnect failed: {e}")
                time.sleep(10)
                continue

            try:
                eng.conn.rollback()
            except Exception:
                pass

            if FEEDER_INTELLIGENCE_ENABLED and now_ts - last_run >= max(30, int(FEEDER_INTELLIGENCE_AUTO_INTERVAL_SECONDS)):
                try:
                    result = eng.fingerprint_reels(limit=max(1, int(FEEDER_INTELLIGENCE_AUTO_LIMIT)))
                    if result.get("selected") or result.get("failed"):
                        print(f"[fingerprint-worker] fingerprints={result}")
                except Exception as e:
                    try:
                        eng.conn.rollback()
                    except Exception:
                        pass
                    if _is_connection_error(e):
                        try:
                            eng._reconnect(f"fingerprint: {e}")
                        except Exception as reconnect_exc:
                            print(f"[fingerprint-worker] reconnect failed: {reconnect_exc}")
                    print(f"[fingerprint-worker] error: {e}")
                last_run = now_ts

            time.sleep(max(1, int(loop_sleep_seconds)))
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
