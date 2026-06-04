from __future__ import annotations

import re
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import requests
from dateutil import parser as date_parser

from .config import (
    APP_TIMEZONE,
    BRIGHTDATA_API_BASE_URL,
    BRIGHTDATA_API_KEY,
    BRIGHTDATA_DISCOVERY_MAX_POSTS,
    BRIGHTDATA_DISCOVERY_OVERLAP_DAYS,
    BRIGHTDATA_POSTED_AT_FALLBACK_HOUR_24,
    BRIGHTDATA_PROFILES_DATASET_ID,
    BRIGHTDATA_POSTS_DATASET_ID,
    BRIGHTDATA_REELS_DATASET_ID,
    BRIGHTDATA_POLL_INTERVAL_SECONDS,
    BRIGHTDATA_SNAPSHOT_TIMEOUT_SECONDS,
)
from .instagram import canonical_post_url, shortcode_from_media_id, shortcode_from_url


def _app_tz() -> ZoneInfo:
    try:
        return ZoneInfo(APP_TIMEZONE or "Asia/Kolkata")
    except Exception:
        return ZoneInfo("UTC")


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {BRIGHTDATA_API_KEY}",
        "Content-Type": "application/json",
    }


def _api_url(path: str) -> str:
    return f"{BRIGHTDATA_API_BASE_URL.rstrip('/')}{path}"


def _date_only_value(raw: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw.strip()))


def _to_utc_string(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None

    try:
        dt = date_parser.parse(raw)
    except Exception:
        return None

    # Bright Data may return a date-only string for some post records; use
    # local noon so checkpoint timing error is bounded within the discovery
    # window instead of skewing toward midnight.
    if _date_only_value(raw):
        dt = dt.replace(hour=BRIGHTDATA_POSTED_AT_FALLBACK_HOUR_24, minute=0, second=0, microsecond=0)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_app_tz())

    return dt.astimezone(timezone.utc).isoformat()


def _as_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if value is None:
        return []
    text = str(value).strip()
    return [text] if text else []


def _extract_media_candidates(item: dict[str, Any], *keys: str) -> list[str]:
    values: list[str] = []
    for key in keys:
        raw = item.get(key)
        if isinstance(raw, list):
            for v in raw:
                s = str(v).strip()
                if s:
                    values.append(s)
        else:
            s = str(raw or "").strip()
            if s:
                values.append(s)
    return values


def _normalize_handle(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None

    profile_match = re.search(r"(?:https?://)?(?:www\.)?instagram\.com/([A-Za-z0-9._]+)/?", raw, re.IGNORECASE)
    if profile_match:
        raw = profile_match.group(1)

    raw = raw.replace("@", "").strip().strip("/")
    if not raw:
        return None

    lowered = raw.lower()
    if lowered in {"p", "reel", "reels", "tv", "stories", "explore", "accounts", "api"}:
        return None
    if not re.fullmatch(r"[A-Za-z0-9._]{1,30}", raw):
        return None
    return lowered


def _collect_handle_candidate(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        handles: list[str] = []
        for item in value:
            handles.extend(_collect_handle_candidate(item))
        return handles
    if isinstance(value, dict):
        handles: list[str] = []
        for key in ("username", "user_name", "handle", "owner_username", "ownerUsername"):
            handle = _normalize_handle(value.get(key))
            if handle:
                handles.append(handle)
        return handles

    handle = _normalize_handle(value)
    return [handle] if handle else []


def _extract_related_handles(item: dict[str, Any]) -> list[str]:
    handles: list[str] = []
    seen: set[str] = set()

    def add(values: list[str]):
        for handle in values:
            if handle and handle not in seen:
                seen.add(handle)
                handles.append(handle)

    direct_keys = (
        "username",
        "user_name",
        "handle",
        "owner_username",
        "ownerUsername",
        "author_username",
        "authorUsername",
    )
    for key in direct_keys:
        add(_collect_handle_candidate(item.get(key)))

    owner_keys = ("owner", "user", "author", "creator")
    for key in owner_keys:
        raw = item.get(key)
        if isinstance(raw, (dict, list)):
            add(_collect_handle_candidate(raw))

    collaborator_keys = (
        "coauthor_producers",
        "coauthors",
        "collaborators",
        "collab_accounts",
        "invited_coauthor_producers",
        "invited_coauthors",
        "shared_with_users",
    )
    for key in collaborator_keys:
        raw = item.get(key)
        if isinstance(raw, (dict, list)):
            add(_collect_handle_candidate(raw))

    return handles


def _normalize_media_type(content_type: str, photo_count: int, video_count: int) -> str:
    m = (content_type or "").strip().lower()
    if m in ("reel", "video"):
        return "reel"
    if photo_count + video_count > 1:
        return "sidecar"
    if m in ("carousel_album", "carousel", "sidecar"):
        return "sidecar"
    if video_count > 0 and photo_count == 0:
        return "reel"
    return "image"


def _build_children(photo_urls: list[str], video_urls: list[str]) -> list[dict[str, str]]:
    children: list[dict[str, str]] = []
    for url in photo_urls:
        children.append({"displayUrl": url})
    for url in video_urls:
        children.append({"videoUrl": url})
    return children


def _first_present(item: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in item and item.get(key) is not None and item.get(key) != "":
            return item.get(key)
    return None


def _profile_probe_from_items(items: list[dict[str, Any]]) -> dict[str, Any] | None:
    profile_keys = (
        "followers",
        "followers_count",
        "profile_image_link",
        "profile_pic_url",
        "profile_picture",
    )
    for item in items:
        if not isinstance(item, dict):
            continue
        if any(key in item and item.get(key) is not None and item.get(key) != "" for key in profile_keys):
            return {key: item.get(key) for key in profile_keys if key in item}
    return None


def _normalized_has_profile(item: dict[str, Any]) -> bool:
    owner = item.get("owner") if isinstance(item.get("owner"), dict) else {}
    return (
        bool(item.get("ownerProfilePicUrl") or owner.get("profilePicUrl"))
        or item.get("ownerFollowersCount") is not None
        or owner.get("followersCount") is not None
    )


def _normalize_item(item: dict[str, Any]) -> dict[str, Any]:
    photo_urls = _extract_media_candidates(
        item,
        "photos",
        "image_urls",
        "image_url",
        "display_url",
        "thumbnail",
        "thumbnail_url",
        "thumbnailUrl",
    )
    video_urls = _extract_media_candidates(item, "videos", "video_urls", "video_url", "videoUrl")
    post_url = str(item.get("url") or item.get("post_url") or "").strip()
    provider_post_id = str(item.get("post_id") or item.get("id") or "").strip() or None
    shortcode = shortcode_from_media_id(provider_post_id) or shortcode_from_url(post_url)
    media_type = _normalize_media_type(str(item.get("content_type") or ""), len(photo_urls), len(video_urls))
    canonical_url = canonical_post_url(shortcode, post_url)
    owner_profile_pic = str(
        item.get("profile_image_link")
        or item.get("profile_pic_url")
        or item.get("profile_picture")
        or ""
    ).strip() or None
    follower_count = _first_present(item, "followers", "followers_count")
    related_handles = _extract_related_handles(item)
    media_display_url = photo_urls[0] if photo_urls else ""
    media_thumbnail_url = photo_urls[0] if photo_urls else ""
    carousel_media: list[dict[str, str]] = []
    media_video_url = ""

    if media_type == "sidecar":
        carousel_media = _build_children(photo_urls, video_urls)
        media_video_url = ""
    elif media_type == "reel":
        carousel_media = []
        media_video_url = video_urls[0] if video_urls else ""
    else:
        carousel_media = []
        media_video_url = ""

    def _first_metric(*values: Any) -> Any:
        for value in values:
            if value is not None and value != "":
                return value
        return None

    normalized: dict[str, Any] = {
        "url": canonical_url,
        "shortCode": shortcode,
        "timestamp": _to_utc_string(item.get("datetime") or item.get("date_posted")),
        "type": media_type,
        "caption": item.get("caption") or item.get("description") or "",
        "likesCount": _first_metric(item.get("likes"), item.get("likes_count"), item.get("likesCount")),
        "commentsCount": _first_metric(
            item.get("comments"),
            item.get("num_comments"),
            item.get("comments_count"),
            item.get("comment_count"),
            item.get("commentsCount"),
        ),
        "videoViewCount": _first_metric(item.get("video_view_count"), item.get("videoViewCount")) if media_type == "reel" else None,
        "videoPlayCount": _first_metric(item.get("video_play_count"), item.get("videoPlayCount"), item.get("plays")) if media_type == "reel" else None,
        "displayUrl": media_display_url,
        "thumbnailUrl": media_thumbnail_url,
        "videoUrl": media_video_url,
        "carouselMedia": carousel_media,
        "ownerProfilePicUrl": owner_profile_pic,
        "ownerFollowersCount": follower_count,
        "owner": {
            "profilePicUrl": owner_profile_pic,
            "followersCount": follower_count,
        },
        "relatedHandles": related_handles,
        "providerPostId": provider_post_id,
        "error": item.get("error"),
        "errorCode": item.get("error_code") or item.get("errorCode"),
    }
    return normalized


def _to_dt(value: Any) -> datetime | None:
    raw = _to_utc_string(value)
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _trim_recent_posts(
    items: list[dict[str, Any]],
    *,
    recent_post_ids: list[str] | None = None,
    days_window: int | None = None,
    max_posts: int | None = None,
) -> list[dict[str, Any]]:
    recent = {str(v).strip() for v in (recent_post_ids or []) if str(v).strip()}
    if recent:
        items = [item for item in items if str(item.get("providerPostId") or "").strip() not in recent]

    effective_days = max(1, days_window or BRIGHTDATA_DISCOVERY_OVERLAP_DAYS)
    cutoff = datetime.now(timezone.utc) - timedelta(days=effective_days)
    trimmed: list[dict[str, Any]] = []
    for item in items:
        posted_at = _to_dt(item.get("timestamp") or item.get("date_posted") or item.get("datetime"))
        if posted_at is None or posted_at >= cutoff:
            trimmed.append(item)

    effective_max = max(1, max_posts or BRIGHTDATA_DISCOVERY_MAX_POSTS)
    return trimmed[:effective_max]


def _normalize_reel_item(item: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_item(item)
    normalized["type"] = "reel"
    video_urls = _extract_media_candidates(item, "videos", "video_urls", "video_url", "videoUrl")
    if video_urls and not normalized.get("videoUrl"):
        normalized["videoUrl"] = video_urls[0]
    if video_urls and not normalized.get("thumbnailUrl"):
        normalized["thumbnailUrl"] = str(item.get("thumbnail") or item.get("thumbnail_url") or "").strip() or None
    video_play_count = (
        item.get("video_play_count")
        or item.get("videoPlayCount")
        or item.get("plays")
    )
    if video_play_count is not None:
        # Product contract: for reels we treat play count as the canonical
        # "views" metric so the stored value matches the visible Instagram reel counter.
        normalized["videoViewCount"] = video_play_count
        normalized["videoPlayCount"] = video_play_count
    elif item.get("views") is not None:
        normalized["videoViewCount"] = item.get("views")
    return normalized


def _flatten_result(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        rows: list[dict[str, Any]] = []
        for item in payload:
            rows.extend(_flatten_result(item))
        return rows

    if not isinstance(payload, dict):
        return []

    posts = payload.get("posts")
    if isinstance(posts, list):
        parent = {
            "followers": payload.get("followers"),
            "followers_count": payload.get("followers_count"),
            "profile_image_link": payload.get("profile_image_link"),
            "profile_pic_url": payload.get("profile_pic_url"),
            "profile_picture": payload.get("profile_picture"),
        }
        rows = []
        for post in posts:
            if not isinstance(post, dict):
                continue
            merged = dict(parent)
            merged.update(post)
            rows.append(merged)
        if rows:
            return rows
        if any(value is not None and value != "" for value in parent.values()):
            return [parent]
        return rows

    nested = payload.get("data") or payload.get("items") or payload.get("records")
    if nested is not None:
        return _flatten_result(nested)

    return [payload]


def _parse_json_payload(raw: str) -> Any:
    text = (raw or "").strip()
    if not text:
        raise json.JSONDecodeError("Expecting value", raw or "", 0)

    decoder = json.JSONDecoder()
    values: list[Any] = []
    index = 0
    length = len(text)

    while index < length:
        while index < length and text[index].isspace():
            index += 1
        if index >= length:
            break
        value, index = decoder.raw_decode(text, index)
        values.append(value)

    if not values:
        raise json.JSONDecodeError("Expecting value", text, 0)
    if len(values) == 1:
        return values[0]
    return values


def _load_json_payload(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except ValueError as exc:
        try:
            return _parse_json_payload(resp.text or "")
        except json.JSONDecodeError as parse_exc:
            message = f"Bright Data JSON parse error: {parse_exc}"
            raise RuntimeError(message) from exc


def _download_snapshot(snapshot_id: str) -> list[dict[str, Any]]:
    resp = requests.get(
        _api_url(f"/datasets/v3/snapshot/{snapshot_id}"),
        headers=_headers(),
        params={"format": "json"},
        timeout=120,
    )
    if resp.status_code == 202:
        return []
    resp.raise_for_status()
    return _flatten_result(_load_json_payload(resp))


def _snapshot_ready(snapshot_id: str) -> bool:
    resp = requests.get(
        _api_url(f"/datasets/v3/progress/{snapshot_id}"),
        headers=_headers(),
        timeout=30,
    )
    resp.raise_for_status()
    payload = _load_json_payload(resp)
    if isinstance(payload, list):
        payload = next((item for item in payload if isinstance(item, dict) and item.get("status")), {}) or {}
    status = str(payload.get("status") or "").strip().lower()
    if status in {"failed", "error", "aborted", "canceled"}:
        raise RuntimeError(f"Bright Data snapshot failed: {status}")
    return status == "ready"


def _scrape_dataset(dataset_id: str, inputs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    resp = requests.post(
        _api_url("/datasets/v3/scrape"),
        headers=_headers(),
        params={
            "dataset_id": dataset_id,
            "notify": "false",
            "include_errors": "true",
        },
        data=json.dumps({"input": inputs}),
        timeout=120,
    )
    if resp.status_code >= 400:
        body = (resp.text or "").strip()
        detail = body[:500] if body else f"HTTP {resp.status_code}"
        raise RuntimeError(f"Bright Data scrape error {resp.status_code}: {detail}")
    result = _load_json_payload(resp)
    if resp.status_code == 200:
        return _flatten_result(result)

    snapshot_id = str(result.get("snapshot_id") or "").strip()
    if not snapshot_id:
        return _flatten_result(result)

    started = time.time()
    while True:
        if time.time() - started > BRIGHTDATA_SNAPSHOT_TIMEOUT_SECONDS:
            raise TimeoutError("Bright Data snapshot timeout")
        if _snapshot_ready(snapshot_id):
            rows = _download_snapshot(snapshot_id)
            if rows:
                return rows
        time.sleep(max(5, BRIGHTDATA_POLL_INTERVAL_SECONDS))


def run_handle(
    handle: str,
    *,
    recent_post_ids: list[str] | None = None,
    days_window: int | None = None,
    max_posts: int | None = None,
) -> list[dict[str, Any]]:
    clean = (handle or "").lstrip("@").strip()
    if not clean:
        return []

    items = _scrape_dataset(BRIGHTDATA_PROFILES_DATASET_ID, [{"url": f"https://www.instagram.com/{clean}/"}])
    profile_probe = _profile_probe_from_items(items)
    normalized = [_normalize_item(item) for item in items]
    trimmed = _trim_recent_posts(
        normalized,
        recent_post_ids=recent_post_ids,
        days_window=days_window,
        max_posts=max_posts,
    )
    if profile_probe and not any(_normalized_has_profile(item) for item in trimmed):
        trimmed.insert(0, _normalize_item(profile_probe))
    return trimmed


def run_post_urls(post_urls: list[str]) -> list[dict[str, Any]]:
    urls = [u.strip() for u in (post_urls or []) if (u or "").strip()]
    if not urls:
        return []

    items = _scrape_dataset(BRIGHTDATA_POSTS_DATASET_ID, [{"url": url} for url in urls])
    return [_normalize_item(item) for item in items]


def run_reel_post_urls(post_urls: list[str]) -> list[dict[str, Any]]:
    urls = [u.strip() for u in (post_urls or []) if (u or "").strip()]
    if not urls:
        return []

    items = _scrape_dataset(BRIGHTDATA_REELS_DATASET_ID, [{"url": url} for url in urls])
    return [_normalize_reel_item(item) for item in items]
