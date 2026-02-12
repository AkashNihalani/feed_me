from __future__ import annotations
import os
import statistics
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from dateutil import parser as date_parser

from .apify import run_actor
from .sheets import ensure_header, get_values, batch_update, append_values, sort_by_posted_at
from .db import upsert_snapshot, get_snapshots
from .config import SHEET_HEADER_LIST


def _to_iso(value) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
        return _format_dt(dt)
    try:
        dt = date_parser.parse(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return _format_dt(dt.astimezone(timezone.utc))
    except Exception:
        return str(value)


def _format_dt(dt: datetime) -> str:
    tz_name = os.getenv("TZ", "UTC")
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc
    local_dt = dt.astimezone(tz)
    date_str = local_dt.strftime("%Y-%m-%d")
    time_str = local_dt.strftime("%H:%M:%S")
    return f'=DATEVALUE("{date_str}")+TIMEVALUE("{time_str}")'


def _to_dt(value):
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    try:
        dt = date_parser.parse(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _extract_hashtags(text: str) -> str:
    if not text:
        return ""
    tags = []
    seen = set()
    for w in text.split():
        if w.startswith("#") and len(w) > 1:
            t = w[1:]
            if t not in seen:
                seen.add(t)
                tags.append(t)
    return ",".join(tags)


def _extract_mentions(text: str) -> str:
    if not text:
        return ""
    tags = []
    seen = set()
    for w in text.split():
        if w.startswith("@") and len(w) > 1:
            t = w[1:]
            if t not in seen:
                seen.add(t)
                tags.append(t)
    return ",".join(tags)


def _list_to_csv(value) -> str:
    if not value:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return ",".join([str(v) for v in value if v is not None])
    return str(value)


def _list_to_tagged_users(value) -> str:
    if not value:
        return ""
    users = []
    if isinstance(value, list):
        for v in value:
            if isinstance(v, dict):
                username = v.get("username") or v.get("user", {}).get("username")
                if not username:
                    username = v.get("full_name") or v.get("fullName")
                if username:
                    users.append(f"@{username}")
            elif isinstance(v, str):
                users.append(v if v.startswith("@") else f"@{v}")
    elif isinstance(value, dict):
        username = value.get("username") or value.get("user", {}).get("username")
        if not username:
            username = value.get("full_name") or value.get("fullName")
        if username:
            users.append(f"@{username}")
    elif isinstance(value, str):
        users.append(value if value.startswith("@") else f"@{value}")
    return "\n".join(users)


def _hook_line(caption: str) -> str:
    if not caption:
        return ""
    first = caption.strip().splitlines()[0].strip()
    words = [w for w in first.split() if w]
    return " ".join(words[:12])


def _hook_type(caption: str) -> str:
    if not caption:
        return ""
    first = caption.strip().splitlines()[0].strip()
    lower = first.lower()
    if lower.startswith("pov"):
        return "POV"
    if "?" in first:
        return "Question"
    if lower.startswith(("stop ", "don't ", "dont ", "never ")):
        return "Command"
    if lower.startswith(("how to", "tutorial", "tips", "guide")):
        return "Educational"
    if lower.startswith(("when ", "me when", "you when")):
        return "Relatable"
    if any(ch.isdigit() for ch in first[:16]):
        return "Stat"
    return "Other"


def _get_media_category(media_type: str) -> str:
    """Categorize media type for velocity comparison buckets."""
    m = (media_type or "").lower()
    if "video" in m or "reel" in m:
        return "video"
    elif "sidecar" in m or "carousel" in m or "album" in m:
        return "sidecar"
    else:
        return "image"


def _is_video(media_type: str) -> bool:
    return _get_media_category(media_type) == "video"


def _normalize_item(item: dict) -> dict:
    timestamp = (
        item.get("timestamp")
        or item.get("takenAtTimestamp")
        or item.get("takenAt")
        or item.get("createdAt")
    )
    caption = item.get("caption") or item.get("text") or item.get("description") or ""

    handle = (
        item.get("ownerUsername")
        or item.get("username")
        or (item.get("owner") or {}).get("username")
        or ""
    )
    display_name = (
        item.get("ownerFullName")
        or item.get("fullName")
        or (item.get("owner") or {}).get("fullName")
        or ""
    )

    likes = item.get("likesCount") or item.get("likes") or item.get("likeCount") or ""
    comments = item.get("commentsCount") or item.get("comments") or item.get("commentCount") or ""
    views = item.get("videoViewCount") or item.get("videoPlayCount") or item.get("views") or item.get("viewCount") or ""
    media_type = item.get("type") or item.get("mediaType") or ""

    url = item.get("url")
    if not url:
        shortcode = item.get("shortCode") or item.get("shortcode") or item.get("code")
        if shortcode:
            url = f"https://www.instagram.com/p/{shortcode}/"

    display_url = item.get("displayUrl") or item.get("thumbnailUrl") or ""
    video_url = item.get("videoUrl") or item.get("videoUrlHd") or ""

    is_pinned = item.get("isPinned") or item.get("pinned") or False
    paid = item.get("isPaidPartnership") or item.get("isPaid") or item.get("isCommercial") or False
    sponsors = item.get("sponsors") or item.get("brands") or ""
    tagged_users = item.get("taggedUsers") or item.get("userTags") or item.get("tagged") or ""
    music_info = item.get("musicInfo") or item.get("music") or ""

    scanned = _format_dt(datetime.now(timezone.utc))

    return {
        "post_url": url or "",
        "posted_at": _to_iso(timestamp),
        "handle": handle,
        "display_name": display_name,
        "media_type": media_type,
        "is_pinned": str(bool(is_pinned)),
        "views": str(views) if views is not None else "",
        "likes": str(likes) if likes is not None else "",
        "comments": str(comments) if comments is not None else "",
        "perf_score": "",
        "velocity_score": "",  # Kept for column compatibility, will be empty
        "velocity_tag": "",
        "velocity_trend": "",
        "hook_type": _hook_type(caption),
        "hook_line": _hook_line(caption),
        "caption": caption,
        "hashtags": _extract_hashtags(caption),
        "caption_mentions": _extract_mentions(caption),
        "display_url": display_url,
        "video_url": video_url,
        "tagged_users": _list_to_tagged_users(tagged_users),
        "music_info": _list_to_csv(music_info),
        "paid_partnership": str(bool(paid)),
        "sponsors": _list_to_csv(sponsors),
        "scanned_at": scanned,
        "last_updated_at": scanned,
    }


def sync_handle(subscriber_id: int, spreadsheet_id: str, handle: str, sheet_name: str, run_type: str) -> tuple[str | None, int, int, int]:
    scrape_handle = handle.strip()
    if scrape_handle.startswith("@"):
        scrape_handle = scrape_handle[1:]
    items = run_actor(scrape_handle, run_type)
    header = ensure_header(sheet_name, spreadsheet_id)

    values = get_values(f"{sheet_name}!A3:AZ10000", spreadsheet_id)
    post_id_idx = header.index("post_url") if "post_url" in header else 0

    existing = {}
    existing_rows = []
    for i, row in enumerate(values, start=3):
        if len(row) > post_id_idx and row[post_id_idx]:
            existing[row[post_id_idx]] = i
        existing_rows.append(row)

    med_video, med_nonvideo = _perf_medians_from_rows(header, existing_rows)

    # Collect all posts for velocity comparison
    all_post_metrics = []
    normalized_items = []
    
    for item in items:
        norm = _normalize_item(item)
        if not norm["post_url"]:
            continue
        if not norm.get("handle"):
            norm["handle"] = handle.lstrip("@")
        
        raw = _metric_value(
            item,
            _safe_int(norm.get("views")),
            _safe_int(norm.get("likes")),
            _safe_int(norm.get("comments")),
        )
        denom = med_video if _is_video(norm.get("media_type")) else med_nonvideo
        norm["perf_score"] = "" if denom <= 0 else f"{(raw / denom):.2f}"
        
        posted_at_dt = _to_dt(item.get("timestamp") or item.get("takenAtTimestamp") or item.get("takenAt") or item.get("createdAt"))
        if posted_at_dt:
            age_hours = (datetime.now(timezone.utc) - posted_at_dt).total_seconds() / 3600
            all_post_metrics.append({
                "norm": norm,
                "item": item,
                "raw": raw,
                "age_hours": age_hours,
                "posted_at_dt": posted_at_dt,
                "media_category": _get_media_category(norm.get("media_type")),
            })
        normalized_items.append((norm, item))
    
    # Apply MAD-based velocity with media-type separation
    _apply_velocity_batch(subscriber_id, handle, all_post_metrics)

    updates = []
    appends = []
    updated_count = 0
    inserted_count = 0

    for norm, item in normalized_items:
        row = [norm.get(col, "") for col in header]
        if norm["post_url"] in existing:
            row_num = existing[norm["post_url"]]
            end_col = _col_letter(len(header))
            updates.append({
                "range": f"{sheet_name}!A{row_num}:{end_col}{row_num}",
                "values": [row],
            })
            updated_count += 1
        else:
            appends.append(row)
            inserted_count += 1

    batch_update(updates, spreadsheet_id)
    append_values(f"{sheet_name}!A3", appends, spreadsheet_id)
    sort_by_posted_at(sheet_name, spreadsheet_id)

    if items:
        latest = _normalize_item(items[0]).get("post_url")
        return latest or None, len(items), inserted_count, updated_count
    return None, len(items), inserted_count, updated_count


def _perf_medians_from_rows(header: list[str], rows: list[list[str]]) -> tuple[float, float]:
    def idx(name: str) -> int:
        try:
            return header.index(name)
        except ValueError:
            return -1

    i_media = idx("media_type")
    i_views = idx("views")
    i_likes = idx("likes")
    i_comments = idx("comments")
    if i_media < 0 or i_views < 0 or i_likes < 0 or i_comments < 0:
        return 1.0, 1.0

    video_vals = []
    nonvideo_vals = []
    for row in rows:
        media = row[i_media] if i_media < len(row) else ""
        v = _safe_int(row[i_views] if i_views < len(row) else "")
        l = _safe_int(row[i_likes] if i_likes < len(row) else "")
        c = _safe_int(row[i_comments] if i_comments < len(row) else "")
        raw = _metric_value({"type": media}, v, l, c)
        if raw <= 0:
            continue
        (video_vals if _is_video(media) else nonvideo_vals).append(raw)

    def med(vals):
        if not vals:
            return 1.0
        try:
            return float(statistics.median(vals))
        except Exception:
            return 1.0

    return med(video_vals), med(nonvideo_vals)


def _col_letter(n: int) -> str:
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _apply_velocity_batch(subscriber_id: int, handle: str, all_post_metrics: list):
    """
    Apply velocity tags using MAD (Median Absolute Deviation).
    
    Compares posts within same age bucket AND same media type:
    - video vs video
    - image vs image  
    - sidecar vs sidecar
    
    Emoji mapping:
    🔥 On fire     = Top 5% (MAD score >= 1.5)
    🚀 Taking off  = Top 15% (MAD score >= 0.67)
    ✅ Steady      = Above median (MAD score >= 0)
    😴 Sleeping    = Below median (MAD score < 0)
    👀 Too new     = Post < 24 hours old
    """
    if not all_post_metrics:
        return
    
    # Age buckets (hours)
    AGE_BUCKETS = [
        (0, 24, "too_new"),
        (24, 72, "early"),
        (72, 168, "mid"),
        (168, 504, "mature"),
        (504, float('inf'), "old"),
    ]
    
    def get_age_bucket(age_hours):
        for min_h, max_h, name in AGE_BUCKETS:
            if min_h <= age_hours < max_h:
                return name
        return "old"
    
    # Group posts by (age_bucket, media_category)
    buckets = {}
    for pm in all_post_metrics:
        age_bucket = get_age_bucket(pm["age_hours"])
        media_cat = pm["media_category"]
        key = (age_bucket, media_cat)
        if key not in buckets:
            buckets[key] = []
        buckets[key].append(pm)
    
    # Calculate velocity for each bucket
    for (age_bucket, media_cat), posts in buckets.items():
        if age_bucket == "too_new":
            for pm in posts:
                norm = pm["norm"]
                norm["velocity_score"] = ""
                norm["velocity_tag"] = "👀"
                norm["velocity_trend"] = "Too new"
                _save_snapshots(subscriber_id, handle, pm)
            continue
        
        raw_values = [pm["raw"] for pm in posts if pm["raw"] > 0]
        
        if len(raw_values) < 3:
            for pm in posts:
                norm = pm["norm"]
                norm["velocity_score"] = ""
                norm["velocity_tag"] = "👀"
                norm["velocity_trend"] = "Too new"
                _save_snapshots(subscriber_id, handle, pm)
            continue
        
        # Calculate MAD
        median_val = statistics.median(raw_values)
        deviations = [abs(x - median_val) for x in raw_values]
        mad = statistics.median(deviations) if deviations else 1.0
        MAD_CONSTANT = 1.4826
        
        for pm in posts:
            norm = pm["norm"]
            raw = pm["raw"]
            
            if mad > 0:
                mad_score = (raw - median_val) / (mad * MAD_CONSTANT)
            else:
                mad_score = 0 if raw == median_val else (1 if raw > median_val else -1)
            
            # Map to emoji
            if mad_score >= 1.5:
                tag, trend = "🔥", "On fire"
            elif mad_score >= 0.67:
                tag, trend = "🚀", "Taking off"
            elif mad_score >= 0:
                tag, trend = "✅", "Steady"
            else:
                tag, trend = "😴", "Sleeping"
            
            norm["velocity_score"] = ""  # Empty - not exposed
            norm["velocity_tag"] = tag
            norm["velocity_trend"] = trend
            
            _save_snapshots(subscriber_id, handle, pm)


def _save_snapshots(subscriber_id: int, handle: str, pm: dict):
    """Save snapshots to DB for historical tracking."""
    norm = pm["norm"]
    posted_at_dt = pm["posted_at_dt"]
    age_days = pm["age_hours"] / 24
    
    views = _safe_int(norm.get("views"))
    likes = _safe_int(norm.get("likes"))
    comments = _safe_int(norm.get("comments"))
    
    upsert_snapshot(subscriber_id, handle, norm["post_url"], posted_at_dt, "d0", views, likes, comments)
    if age_days >= 3:
        upsert_snapshot(subscriber_id, handle, norm["post_url"], posted_at_dt, "d3", views, likes, comments)
    if age_days >= 7:
        upsert_snapshot(subscriber_id, handle, norm["post_url"], posted_at_dt, "d7", views, likes, comments)
    if age_days >= 21:
        upsert_snapshot(subscriber_id, handle, norm["post_url"], posted_at_dt, "d21", views, likes, comments)


def _safe_int(value):
    try:
        return int(value)
    except Exception:
        return None


def _metric_value(item: dict, views: int | None, likes: int | None, comments: int | None):
    media_type = (item.get("type") or item.get("mediaType") or "").lower()
    if "video" in media_type or "reel" in media_type:
        v = (views or 0) * 0.5 + (likes or 0) * 0.3 + (comments or 0) * 0.2
    else:
        v = (likes or 0) * 0.6 + (comments or 0) * 0.4
    return v
