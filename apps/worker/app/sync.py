from __future__ import annotations
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from dateutil import parser as date_parser

from .apify import run_actor, run_actor_post_url
from .sheets import ensure_header, get_values, batch_update, append_values, sort_by_posted_at
from .db import (
    upsert_snapshot,
    get_snapshots,
    get_conn,
    upsert_post_signal,
    upsert_checkpoint_metric,
    upsert_post_core,
    ensure_post_checkpoint_jobs,
    get_latest_followers,
)
from .config import SHEET_HEADER_LIST


def _to_iso(value) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, (int, float)):
        ts = float(value)
        # Some APIs return milliseconds; normalize to seconds.
        if ts > 1_000_000_000_000:
            ts = ts / 1000.0
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
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
    # Locale-independent Sheets datetime formula.
    return f"=DATE({local_dt.year},{local_dt.month},{local_dt.day})+TIME({local_dt.hour},{local_dt.minute},{local_dt.second})"


def _to_dt(value):
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

def _is_video(media_type: str) -> bool:
    m = (media_type or "").lower()
    return ("video" in m) or ("reel" in m)

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
    followers = (
        item.get("ownerFollowersCount")
        or item.get("followersCount")
        or (item.get("owner") or {}).get("followersCount")
        or ((item.get("owner") or {}).get("edge_followed_by") or {}).get("count")
        or ""
    )

    likes = item.get("likesCount") or item.get("likes") or item.get("likeCount") or ""
    comments = item.get("commentsCount") or item.get("comments") or item.get("commentCount") or ""
    views = item.get("videoViewCount") or item.get("videoPlayCount") or item.get("views") or item.get("viewCount") or ""
    media_type = item.get("type") or item.get("mediaType") or ""
    duration = item.get("videoDuration") or item.get("duration") or item.get("videoDurationSeconds") or ""

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
        "followers_at_scan": str(followers) if followers not in (None, "") else "",
        "media_type": media_type,
        "is_pinned": str(bool(is_pinned)),
        "views": str(views) if views is not None else "",
        "likes": str(likes) if likes is not None else "",
        "comments": str(comments) if comments is not None else "",
        "perf_score": "",
        "velocity": "",
        "velocity_percentile": "",
        "velocity_stage": "",
        "caption": caption,
        "hashtags": _extract_hashtags(caption),
        "caption_mentions": _extract_mentions(caption),
        "display_url": display_url,
        "video_url": video_url,
        "tagged_users": _list_to_tagged_users(tagged_users),
        "music_info": _list_to_csv(music_info),
        "duration_seconds": str(duration) if duration not in (None, "") else "",
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

    updates = []
    appends = []
    updated_count = 0
    inserted_count = 0
    follower_baseline = get_latest_followers(subscriber_id, handle) or get_latest_followers(subscriber_id, f"@{handle.lstrip('@')}")

    for item in items:
        norm = _normalize_item(item)
        if not norm["post_url"]:
            continue
        # If Apify didn't return an owner username, fall back to the sheet name/handle we were asked to sync.
        if not norm.get("handle"):
            norm["handle"] = handle.lstrip("@")
        upsert_post_core(
            subscriber_id=subscriber_id,
            handle=handle,
            post_url=norm.get("post_url") or "",
            media_type=norm.get("media_type"),
            posted_at=_to_dt(
                item.get("timestamp")
                or item.get("takenAtTimestamp")
                or item.get("takenAt")
                or item.get("createdAt")
            ),
            caption=norm.get("caption"),
            hashtags=norm.get("hashtags"),
            caption_mentions=norm.get("caption_mentions"),
            tagged_users=norm.get("tagged_users"),
            music_info=norm.get("music_info"),
            is_pinned=str(norm.get("is_pinned")).upper() == "TRUE",
            paid_partnership=str(norm.get("paid_partnership")).upper() == "TRUE",
            sponsors=norm.get("sponsors"),
            display_url=norm.get("display_url"),
            video_url=norm.get("video_url"),
            duration_seconds=float(norm["duration_seconds"]) if norm.get("duration_seconds") not in ("", None) else None,
        )
        norm["perf_score"] = _compute_perf_score(norm, follower_baseline)
        _apply_velocity(subscriber_id, handle, item, norm, forced_checkpoint="d1")
        posted_at_dt = _to_dt(
            item.get("timestamp")
            or item.get("takenAtTimestamp")
            or item.get("takenAt")
            or item.get("createdAt")
        )
        ensure_post_checkpoint_jobs(
            subscriber_id=subscriber_id,
            spreadsheet_id=spreadsheet_id,
            handle=handle,
            post_url=norm.get("post_url") or "",
            posted_at=posted_at_dt,
        )
        if norm["post_url"] in existing:
            row_num = existing[norm["post_url"]]
            row = [norm.get(col, "") for col in header]
            end_col = _col_letter(len(header))
            updates.append(
                {
                    "range": f"{sheet_name}!A{row_num}:{end_col}{row_num}",
                    "values": [row],
                }
            )
            updated_count += 1
        else:
            row = [norm.get(col, "") for col in header]
            appends.append(row)
            inserted_count += 1

    batch_update(updates, spreadsheet_id)
    append_values(f"{sheet_name}!A3", appends, spreadsheet_id)

    sort_by_posted_at(sheet_name, spreadsheet_id)

    if items:
        latest = _normalize_item(items[0]).get("post_url")
        return latest or None, len(items), inserted_count, updated_count
    return None, len(items), inserted_count, updated_count


def sync_post_checkpoint(
    subscriber_id: int,
    spreadsheet_id: str,
    handle: str,
    sheet_name: str,
    post_url: str,
    checkpoint: str,
) -> tuple[int, int]:
    scrape_handle = handle.strip()
    if scrape_handle.startswith("@"):
        scrape_handle = scrape_handle[1:]

    item = run_actor_post_url(scrape_handle, post_url)
    if not item:
        return 0, 0

    header = ensure_header(sheet_name, spreadsheet_id)
    values = get_values(f"{sheet_name}!A3:AZ10000", spreadsheet_id)
    post_id_idx = header.index("post_url") if "post_url" in header else 0

    existing = {}
    for i, row in enumerate(values, start=3):
        if len(row) > post_id_idx and row[post_id_idx]:
            existing[row[post_id_idx]] = i

    norm = _normalize_item(item)
    if not norm.get("post_url"):
        norm["post_url"] = post_url
    if not norm.get("handle"):
        norm["handle"] = handle.lstrip("@")

    upsert_post_core(
        subscriber_id=subscriber_id,
        handle=handle,
        post_url=norm.get("post_url") or "",
        media_type=norm.get("media_type"),
        posted_at=_to_dt(
            item.get("timestamp")
            or item.get("takenAtTimestamp")
            or item.get("takenAt")
            or item.get("createdAt")
        ),
        caption=norm.get("caption"),
        hashtags=norm.get("hashtags"),
        caption_mentions=norm.get("caption_mentions"),
        tagged_users=norm.get("tagged_users"),
        music_info=norm.get("music_info"),
        is_pinned=str(norm.get("is_pinned")).upper() == "TRUE",
        paid_partnership=str(norm.get("paid_partnership")).upper() == "TRUE",
        sponsors=norm.get("sponsors"),
        display_url=norm.get("display_url"),
        video_url=norm.get("video_url"),
        duration_seconds=float(norm["duration_seconds"]) if norm.get("duration_seconds") not in ("", None) else None,
    )

    follower_baseline = get_latest_followers(subscriber_id, handle) or get_latest_followers(subscriber_id, f"@{handle.lstrip('@')}")
    norm["perf_score"] = _compute_perf_score(norm, follower_baseline)
    _apply_velocity(subscriber_id, handle, item, norm, forced_checkpoint=checkpoint)

    if norm["post_url"] in existing:
        row_num = existing[norm["post_url"]]
        row = [norm.get(col, "") for col in header]
        end_col = _col_letter(len(header))
        batch_update(
            [
                {
                    "range": f"{sheet_name}!A{row_num}:{end_col}{row_num}",
                    "values": [row],
                }
            ],
            spreadsheet_id,
        )
        sort_by_posted_at(sheet_name, spreadsheet_id)
        return 0, 1

    row = [norm.get(col, "") for col in header]
    append_values(f"{sheet_name}!A3", [row], spreadsheet_id)
    sort_by_posted_at(sheet_name, spreadsheet_id)
    return 1, 0


def _col_letter(n: int) -> str:
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _apply_velocity(
    subscriber_id: int,
    handle: str,
    item: dict,
    norm: dict,
    forced_checkpoint: str | None = None,
):
    posted_at_dt = _to_dt(
        item.get("timestamp")
        or item.get("takenAtTimestamp")
        or item.get("takenAt")
        or item.get("createdAt")
    )
    if not posted_at_dt:
        return

    now = datetime.now(timezone.utc)
    age_hours = (now - posted_at_dt).total_seconds() / 3600.0

    checkpoint, _ = _checkpoint_from_age(age_hours)
    if forced_checkpoint:
        checkpoint = forced_checkpoint
    norm["velocity_stage"] = _stage_label(checkpoint, age_hours)

    views = _safe_int(norm.get("views"))
    likes = _safe_int(norm.get("likes"))
    comments = _safe_int(norm.get("comments"))
    metric_now = _metric_value(item, views, likes, comments)
    days_now = _checkpoint_days(checkpoint)
    velocity_now = (metric_now / days_now) if (metric_now is not None and days_now > 0) else None

    # Day-21 only if Day-7 passed (🔥 or 🚀). If not, keep D7 signal.
    if checkpoint == "d21":
        snap = get_snapshots(subscriber_id, handle, norm["post_url"])
        if snap:
            media_type = (snap.get("media_type") or item.get("type") or item.get("mediaType") or "").lower()
            d7_tag = _velocity_tag_for_checkpoint(subscriber_id, handle, media_type, snap, item, "d7")
            if not _is_high_tag(d7_tag):
                _, d7_percentile, d7_tag_full = _velocity_from_snapshots(
                    subscriber_id, handle, snap, item, "d7"
                )
                signal_tag = d7_tag_full or d7_tag or "✅"
                signal_percentile = d7_percentile or ""
                if signal_tag == "insufficient_data":
                    norm["velocity"] = ""
                    norm["velocity_percentile"] = ""
                else:
                    norm["velocity"] = signal_tag
                    norm["velocity_percentile"] = signal_percentile
                norm["velocity_stage"] = _stage_label("d7", age_hours)
                upsert_post_signal(
                    subscriber_id=subscriber_id,
                    handle=handle,
                    post_url=norm.get("post_url") or "",
                    media_type=norm.get("media_type"),
                    posted_at=posted_at_dt,
                    caption=norm.get("caption"),
                    velocity_tag=signal_tag,
                    velocity_stage=norm.get("velocity_stage"),
                    velocity_percentile=signal_percentile,
                )
                upsert_checkpoint_metric(
                    subscriber_id=subscriber_id,
                    handle=handle,
                    post_url=norm.get("post_url") or "",
                    checkpoint="d7",
                    stage_label=_stage_label("d7", age_hours),
                    views=_safe_int(snap.get("d7_views")),
                    likes=_safe_int(snap.get("d7_likes")),
                    comments=_safe_int(snap.get("d7_comments")),
                    metric_value=_metric_for_checkpoint(snap, item, "d7"),
                    velocity_value=(
                        (_metric_for_checkpoint(snap, item, "d7") or 0) / 7
                        if _metric_for_checkpoint(snap, item, "d7") is not None
                        else None
                    ),
                    velocity_tag=signal_tag,
                    velocity_percentile=signal_percentile,
                    perf_score=None,
                )
                return

    # Save / update snapshot with current metrics (always-overwrite).
    upsert_snapshot(
        subscriber_id,
        handle,
        norm["post_url"],
        norm.get("media_type") or "",
        posted_at_dt,
        checkpoint,
        views,
        likes,
        comments,
    )

    # Calculate percentile-based velocity from latest snapshot.
    snap = get_snapshots(subscriber_id, handle, norm["post_url"])
    if not snap:
        return

    _, percentile, tag = _velocity_from_snapshots(subscriber_id, handle, snap, item, checkpoint)
    signal_tag = tag or "✅"
    signal_percentile = percentile or ""
    if signal_tag == "insufficient_data":
        norm["velocity"] = ""
        norm["velocity_percentile"] = ""
    else:
        norm["velocity"] = signal_tag
        norm["velocity_percentile"] = signal_percentile
    norm["velocity_stage"] = _stage_label(checkpoint, age_hours)

    upsert_post_signal(
        subscriber_id=subscriber_id,
        handle=handle,
        post_url=norm.get("post_url") or "",
        media_type=norm.get("media_type"),
        posted_at=posted_at_dt,
        caption=norm.get("caption"),
        velocity_tag=signal_tag,
        velocity_stage=norm.get("velocity_stage"),
        velocity_percentile=signal_percentile,
    )
    upsert_checkpoint_metric(
        subscriber_id=subscriber_id,
        handle=handle,
        post_url=norm.get("post_url") or "",
        checkpoint=checkpoint,
        stage_label=norm.get("velocity_stage") or _stage_label(checkpoint, age_hours),
        views=views,
        likes=likes,
        comments=comments,
        metric_value=metric_now,
        velocity_value=velocity_now,
        velocity_tag=signal_tag,
        velocity_percentile=signal_percentile,
        perf_score=None,
    )


def _safe_int(value):
    try:
        return int(value)
    except Exception:
        return None


def _metric_value(item: dict, views: int | None, likes: int | None, comments: int | None):
    media_type = (item.get("type") or item.get("mediaType") or "").lower()
    if "video" in media_type or "reel" in media_type:
        return float(views or 0)
    if "sidecar" in media_type or "carousel" in media_type:
        return float(likes or 0) + 2.0 * float(comments or 0)
    return float(likes or 0)


def _compute_perf_score(norm: dict, followers_baseline: int | None) -> str:
    media_type = (norm.get("media_type") or "").lower()
    views = _safe_int(norm.get("views")) or 0
    likes = _safe_int(norm.get("likes")) or 0
    comments = _safe_int(norm.get("comments")) or 0

    if "video" in media_type or "reel" in media_type:
        if views <= 0:
            return ""
        score = ((likes + comments) / views) * 100.0
        return f"{score:.2f}%"

    # Image + Sidecar use weekly followers baseline.
    if followers_baseline and followers_baseline > 0:
        score = ((likes + comments) / followers_baseline) * 100.0
        return f"{score:.2f}%"
    return ""


def _checkpoint_from_age(age_hours: float) -> tuple[str, str]:
    if age_hours < 48:
        return "d1", "D1"
    if age_hours < 168:
        return "d3", "D3"
    if age_hours < 504:
        return "d7", "D7"
    return "d21", "D21"


def _stage_label(checkpoint: str, age_hours: float = 0.0) -> str:
    if checkpoint == "d1":
        return "D1" if age_hours < 24 else "D2"
    return {
        "d3": "D3",
        "d7": "D7",
        "d21": "D21",
    }.get(checkpoint, "D1")


def _metric_for_checkpoint(snap: dict, item: dict, checkpoint: str) -> float | None:
    mapping = {
        "d1": ("d1_views", "d1_likes", "d1_comments"),
        "d3": ("d3_views", "d3_likes", "d3_comments"),
        "d7": ("d7_views", "d7_likes", "d7_comments"),
        "d21": ("d21_views", "d21_likes", "d21_comments"),
    }
    if checkpoint not in mapping:
        return None
    v_col, l_col, c_col = mapping[checkpoint]
    v = snap.get(v_col)
    l = snap.get(l_col)
    c = snap.get(c_col)
    if v is None and l is None and c is None:
        return None
    return _metric_value(item, v, l, c)


def _checkpoint_days(checkpoint: str) -> int:
    return {"d1": 1, "d3": 3, "d7": 7, "d21": 21}.get(checkpoint, 0)


def _min_cohort_size(checkpoint: str) -> int:
    # D1 and D2 both come from d1 checkpoint data.
    return 12 if checkpoint == "d1" else 20


def _velocity_from_snapshots(subscriber_id: int, handle: str, snap: dict, item: dict, checkpoint: str):
    days = _checkpoint_days(checkpoint)
    if days <= 0:
        return None, None, ""

    current_metric = _metric_for_checkpoint(snap, item, checkpoint)
    if current_metric is None:
        return None, None, ""

    metric_per_day = current_metric / days
    media_type = (snap.get("media_type") or item.get("type") or item.get("mediaType") or "").lower()

    pool = _velocity_pool(subscriber_id, handle, media_type, checkpoint)
    if not pool:
        return None, None, ""
    if len(pool) < _min_cohort_size(checkpoint):
        return None, None, "insufficient_data"

    percentile = _percentile(pool, metric_per_day)
    tag = _velocity_tag(percentile)

    # Late bloomer: D1 was low, D7 is high
    if checkpoint == "d7":
        prev = _velocity_tag_for_checkpoint(subscriber_id, handle, media_type, snap, item, "d1")
        if (not _is_high_tag(prev)) and _is_high_tag(tag):
            tag = f"☘️{tag}"

    return None, percentile, tag


def _velocity_pool(subscriber_id: int, handle: str, media_type: str, checkpoint: str) -> list[float]:
    days = _checkpoint_days(checkpoint)
    if days <= 0:
        return []
    field_map = {
        "d1": ("d1_views", "d1_likes", "d1_comments"),
        "d3": ("d3_views", "d3_likes", "d3_comments"),
        "d7": ("d7_views", "d7_likes", "d7_comments"),
        "d21": ("d21_views", "d21_likes", "d21_comments"),
    }
    if checkpoint not in field_map:
        return []
    v_col, l_col, c_col = field_map[checkpoint]
    values: list[float] = []
    with get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT {v_col} as v, {l_col} as l, {c_col} as c, media_type
            FROM post_snapshots
            WHERE subscriber_id=%s
              AND handle=%s
              AND ({v_col} IS NOT NULL OR {l_col} IS NOT NULL OR {c_col} IS NOT NULL)
            """,
            (subscriber_id, handle),
        ).fetchall()
    for row in rows:
        mtype = (row.get("media_type") or "").lower()
        if media_type and mtype and media_type not in mtype and mtype not in media_type:
            continue
        metric = _metric_value({"type": mtype}, row.get("v"), row.get("l"), row.get("c"))
        if metric is None:
            continue
        values.append(metric / days)
    return values


def _percentile(values: list[float], value: float) -> str | None:
    if not values:
        return None
    # Dense-rank percentile: ties share rank, 1% = top-performing.
    uniq_desc = sorted(set(values), reverse=True)
    if not uniq_desc:
        return None
    if len(uniq_desc) == 1:
        return "50%"
    rank = len(uniq_desc)
    for i, v in enumerate(uniq_desc, start=1):
        if value >= v:
            rank = i
            break
    p = int(round(1 + ((rank - 1) * 99 / (len(uniq_desc) - 1))))
    p = max(1, min(100, p))
    return f"{p}%"


def _velocity_tag(percentile: str | None) -> str:
    p = None
    if percentile:
        try:
            p = int(str(percentile).replace("%", ""))
        except Exception:
            p = None
    if p is None:
        return "✅"
    if p <= 5:
        return "🚀"
    if p <= 15:
        return "🔥"
    if p <= 35:
        return "✅"
    if p > 35:
        return "😴"
    return "✅"


def _is_high_tag(tag: str | None) -> bool:
    if not tag:
        return False
    return ("🔥" in tag) or ("🚀" in tag)


def _velocity_tag_for_checkpoint(
    subscriber_id: int, handle: str, media_type: str, snap: dict, item: dict, checkpoint: str
) -> str | None:
    days = _checkpoint_days(checkpoint)
    if days <= 0:
        return None
    metric = _metric_for_checkpoint(snap, item, checkpoint)
    if metric is None:
        return None
    metric_per_day = metric / days
    pool = _velocity_pool(subscriber_id, handle, media_type, checkpoint)
    if not pool:
        return None
    if len(pool) < _min_cohort_size(checkpoint):
        return "insufficient_data"
    percentile = _percentile(pool, metric_per_day)
    return _velocity_tag(percentile)
