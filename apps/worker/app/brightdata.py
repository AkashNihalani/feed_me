from __future__ import annotations

import re
import json
import time
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import requests
from dateutil import parser as date_parser

from .config import (
    APP_TIMEZONE,
    BRIGHTDATA_API_BASE_URL,
    BRIGHTDATA_API_KEY,
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


def _normalize_media_type(content_type: str, photo_count: int, video_count: int) -> str:
    m = (content_type or "").strip().lower()
    if m in ("reel", "video"):
        return "reel"
    if photo_count + video_count > 1:
        return "sidecar"
    if m in ("carousel_album", "carousel", "sidecar"):
        return "sidecar"
    return "image"


def _build_children(photo_urls: list[str], video_urls: list[str]) -> list[dict[str, str]]:
    children: list[dict[str, str]] = []
    for url in photo_urls:
        children.append({"displayUrl": url})
    for url in video_urls:
        children.append({"videoUrl": url})
    return children


def _normalize_item(item: dict[str, Any]) -> dict[str, Any]:
    photo_urls = _extract_media_candidates(item, "photos", "image_urls", "image_url", "display_url")
    video_urls = _extract_media_candidates(item, "videos", "video_urls", "video_url")
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
    follower_count = item.get("followers") or item.get("followers_count")
    media_display_url = photo_urls[0] if photo_urls else ""
    media_thumbnail_url = photo_urls[0] if photo_urls else ""

    normalized: dict[str, Any] = {
        "url": canonical_url,
        "shortCode": shortcode,
        "timestamp": _to_utc_string(item.get("datetime") or item.get("date_posted")),
        "type": media_type,
        "caption": item.get("caption") or item.get("description") or "",
        "likesCount": item.get("likes") or item.get("likes_count") or item.get("likesCount"),
        "commentsCount": (
            item.get("comments")
            or item.get("num_comments")
            or item.get("comments_count")
            or item.get("comment_count")
            or item.get("commentsCount")
        ),
        "videoViewCount": item.get("video_view_count") or item.get("videoViewCount"),
        "videoPlayCount": item.get("video_play_count") or item.get("videoPlayCount") or item.get("plays"),
        "displayUrl": media_display_url,
        "thumbnailUrl": media_thumbnail_url,
        "videoUrl": video_urls[0] if video_urls else "",
        "carouselMedia": _build_children(photo_urls, video_urls),
        "ownerProfilePicUrl": owner_profile_pic,
        "ownerFollowersCount": follower_count,
        "owner": {
            "profilePicUrl": owner_profile_pic,
            "followersCount": follower_count,
        },
        "providerPostId": provider_post_id,
        "error": item.get("error"),
        "errorCode": item.get("error_code") or item.get("errorCode"),
    }
    return normalized


def _normalize_reel_item(item: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_item(item)
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
    resp.raise_for_status()
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


def run_handle(handle: str, recent_post_ids: list[str] | None = None) -> list[dict[str, Any]]:
    clean = (handle or "").lstrip("@").strip()
    if not clean:
        return []

    items = _scrape_dataset(BRIGHTDATA_PROFILES_DATASET_ID, [{"url": f"https://www.instagram.com/{clean}/"}])
    normalized = [_normalize_item(item) for item in items]
    if not recent_post_ids:
        return normalized
    recent = {str(v).strip() for v in recent_post_ids if str(v).strip()}
    return [item for item in normalized if str(item.get("providerPostId") or "").strip() not in recent]


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
