from __future__ import annotations

import statistics
from datetime import datetime, timezone
from typing import Any

from psycopg.rows import dict_row

_WINDOW_DAYS = 90
_HARD_CAP = 100
_WARM_START_CAP = 15
_MAX_TOP_BOTTOM_PER_FORMAT = 30
_MAX_FEED_TOP_BOTTOM_PER_FEEDER_FORMAT = 10
_ANOMALY_SLICE_CAP = 5
_SIGNAL_RESIDUAL_CAP = 10

_FORMAT_ORDER = ("reels", "carousels", "statics")
_CHECKPOINTS = ("d3", "d7", "d21")

_SIGNAL_TYPE_MAP: dict[str, list[str]] = {
    "OWN_BREAKOUT_EARLY": ["L_BREAKOUT", "T_EARLY"],
    "OWN_BREAKOUT": ["L_BREAKOUT"],
    "OWN_SUSTAIN": ["L_SUSTAIN"],
    "OWN_SUSTAIN_LONG": ["L_EVERGREEN"],
    "OWN_FADE": ["L_FADE"],
    "OWN_COMMENT_SPIKE": ["S_COMMENT_SPIKE"],
    "OWN_LIKE_HEAVY": ["S_LIKE_HEAVY"],
    "OWN_VIRAL_PASSIVE": ["S_VIRAL_PASSIVE"],
    "OWN_LATE_JUMP": ["T_LATE_JUMP"],
    "OWN_FOLLOWER_SPIKE": ["W_SPIKE"],
    "OWN_FOLLOWER_DROP": ["W_DROP"],
    "CROSS_MOMENTUM": ["C_MOMENTUM"],
    "CROSS_FORMAT_SHIFT": ["C_FORMAT_SHIFT"],
    "CROSS_MICRO_BREAKOUT": ["C_CONVERGENCE", "L_BREAKOUT"],
    "CROSS_MICRO_COMMENT_SPIKE": ["C_CONVERGENCE", "S_COMMENT_SPIKE"],
    "CROSS_MICRO_LIKE_HEAVY": ["C_CONVERGENCE", "S_LIKE_HEAVY"],
    "CROSS_MICRO_VIRAL_PASSIVE": ["C_CONVERGENCE", "S_VIRAL_PASSIVE"],
    "CROSS_MICRO_FADE": ["C_CONVERGENCE", "L_FADE"],
    "CROSS_FOLLOWER_WAVE": ["C_FOLLOWER_WAVE"],
    "ANCHOR_GAP_WIDENING": ["A_GAP_WIDENING"],
    "ANCHOR_GAP_CLOSING": ["A_GAP_CLOSING"],
    "ANCHOR_CHALLENGER_SURGE": ["A_CHALLENGER_SURGE"],
    "ANCHOR_FOLLOWER_GAP": ["A_FOLLOWER_GAP"],
}


def _num(value: Any) -> float | None:
    try:
        parsed = float(value)
        return parsed if parsed == parsed and parsed not in {float("inf"), float("-inf")} else None
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    parsed = _num(value)
    return int(round(parsed)) if parsed is not None else None


def _media_format(media_type: Any) -> str:
    value = str(media_type or "").strip().lower()
    if value in {"reel", "video"}:
        return "reels"
    if value in {"sidecar", "carousel", "album"}:
        return "carousels"
    return "statics"


def _checkpoint_metrics(row: dict[str, Any], checkpoint: str) -> dict[str, Any]:
    metrics = row.get("checkpoint_metrics")
    if not isinstance(metrics, dict):
        return {}
    value = metrics.get(checkpoint)
    return value if isinstance(value, dict) else {}


def _metric(row: dict[str, Any], key: str, checkpoint: str = "d7") -> float | None:
    value = _checkpoint_metrics(row, checkpoint).get(key)
    if value is None and checkpoint != "d7":
        value = _checkpoint_metrics(row, "d7").get(key)
    return _num(value)


def _best_top_metric(row: dict[str, Any]) -> tuple[float, str] | None:
    candidates: list[tuple[float, str]] = []
    for checkpoint in _CHECKPOINTS:
        percentile = _metric(row, "percentile_performance_exact", checkpoint)
        if percentile is None:
            percentile = _metric(row, "percentile_performance", checkpoint)
        if percentile is not None:
            candidates.append((percentile, checkpoint))
    if not candidates:
        return None
    return min(candidates, key=lambda item: item[0])


def _bottom_metric(row: dict[str, Any]) -> tuple[float, str] | None:
    percentile = _metric(row, "percentile_performance_exact", "d7")
    if percentile is None:
        percentile = _metric(row, "percentile_performance", "d7")
    if percentile is not None:
        return percentile, "d7"
    candidates: list[tuple[float, str]] = []
    for checkpoint in _CHECKPOINTS:
        value = _metric(row, "percentile_performance_exact", checkpoint)
        if value is None:
            value = _metric(row, "percentile_performance", checkpoint)
        if value is not None:
            candidates.append((value, checkpoint))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])


def _caption_excerpt(value: Any, limit: int = 220) -> str:
    text = " ".join(str(value or "").split())
    return text[:limit]


def _ordered_unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        key = str(value or "").strip()
        if not key or key in seen:
            continue
        out.append(key)
        seen.add(key)
    return out


def _source_signal_types(row: dict[str, Any]) -> list[str]:
    values = row.get("source_signal_types")
    if isinstance(values, list):
        return _ordered_unique([str(item or "").upper() for item in values])
    if isinstance(values, tuple):
        return _ordered_unique([str(item or "").upper() for item in values])
    return []


def _recent_signal_types(row: dict[str, Any]) -> list[str]:
    values = row.get("recent_signal_types")
    if isinstance(values, list):
        recent = _ordered_unique([str(item or "").upper() for item in values])
        if recent:
            return recent
    if isinstance(values, tuple):
        recent = _ordered_unique([str(item or "").upper() for item in values])
        if recent:
            return recent
    return _source_signal_types(row)


def annotate_signal_types(row: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    for signal_type in _source_signal_types(row):
        tags.extend(_SIGNAL_TYPE_MAP.get(signal_type, [signal_type]))

    best = _best_top_metric(row)
    if best and best[0] <= 5:
        tags.append("L_BREAKOUT")
    if _metric(row, "percentile_performance", "d3") is not None and (_metric(row, "percentile_performance", "d3") or 101) <= 5:
        tags.append("T_EARLY")
    if _metric(row, "percentile_performance", "d21") is not None and (_metric(row, "percentile_performance", "d21") or 101) <= 25:
        tags.append("L_EVERGREEN")

    comments_x = _metric(row, "comments_multiple", "d7") or 0
    likes_x = _metric(row, "likes_multiple", "d7") or 0
    views_x = _metric(row, "views_multiple", "d7") or 0
    if comments_x >= 2.0 and comments_x >= max(likes_x, views_x) * 1.2:
        tags.append("S_COMMENT_SPIKE")
    if likes_x >= 2.0 and likes_x >= max(comments_x, views_x) * 1.1:
        tags.append("S_LIKE_HEAVY")
    if _media_format(row.get("media_type")) == "reels" and views_x >= 2.5 and views_x >= max(comments_x, likes_x) * 1.4:
        tags.append("S_VIRAL_PASSIVE")
    if comments_x >= 1.5 and likes_x >= 1.5 and views_x >= 1.5:
        tags.append("S_BROAD")

    d3 = _metric(row, "percentile_performance", "d3")
    d7 = _metric(row, "percentile_performance", "d7")
    d21 = _metric(row, "percentile_performance", "d21")
    if d3 is not None and d7 is not None and d3 - d7 >= 25:
        tags.append("T_LATE_JUMP")
    if d7 is not None and d21 is not None and d21 - d7 >= 25:
        tags.append("T_FALSE_DAWN")
    return _ordered_unique(tags)


def _outlier_score(row: dict[str, Any], axis: str) -> float | None:
    comments_x = _metric(row, "comments_multiple", "d7") or 0
    likes_x = _metric(row, "likes_multiple", "d7") or 0
    views_x = _metric(row, "views_multiple", "d7") or 0
    if axis == "comments":
        return comments_x - max(likes_x, views_x)
    if axis == "likes":
        return likes_x - max(comments_x, views_x)
    if axis == "views":
        return views_x - max(comments_x, likes_x)
    return None


def _follower_signal(row: dict[str, Any]) -> bool:
    signal_types = set(_source_signal_types(row))
    return bool(signal_types & {"OWN_FOLLOWER_SPIKE", "OWN_FOLLOWER_DROP", "CROSS_FOLLOWER_WAVE", "ANCHOR_FOLLOWER_GAP"})


def _entry(row: dict[str, Any], group: str, rank: int | None) -> dict[str, Any]:
    best = _best_top_metric(row)
    bottom = _bottom_metric(row)
    checkpoint = best[1] if best else bottom[1] if bottom else "d7"
    metrics = dict(_checkpoint_metrics(row, checkpoint))
    metrics["ranking_checkpoint"] = checkpoint
    metrics["canonical_percentile"] = best[0] if best else bottom[0] if bottom else None
    return {
        "post_id": row.get("post_key"),
        "post_key": row.get("post_key"),
        "group": group,
        "groups": [group],
        "rank": rank,
        "media_type": row.get("media_type"),
        "format": _media_format(row.get("media_type")),
        "duration_seconds": _num(row.get("duration_seconds")),
        "duration_bucket": row.get("duration_bucket"),
        "carousel_slide_count": _int_or_none(row.get("carousel_slide_count")),
        "carousel_depth_bucket": row.get("depth_bucket"),
        "posted_at": row.get("posted_at"),
        "feeder_id": row.get("feeder_id"),
        "feed_id": row.get("feed_id"),
        "feeder_handle": row.get("handle"),
        "feeder_role": row.get("feeder_role"),
        "post_url": row.get("post_url"),
        "metrics": metrics,
        "checkpoint_metrics": row.get("checkpoint_metrics") if isinstance(row.get("checkpoint_metrics"), dict) else {},
        "signal_types": annotate_signal_types(row),
        "source_signal_types": _source_signal_types(row),
        "recent_signal_types": _recent_signal_types(row),
        "fingerprint": row.get("fingerprint") if isinstance(row.get("fingerprint"), dict) else {},
        "fingerprint_model_version": row.get("fingerprint_model_version"),
        "caption_excerpt": _caption_excerpt(row.get("caption")),
    }


def _add_entry(
    selected: dict[str, dict[str, Any]],
    row: dict[str, Any],
    *,
    group: str,
    rank: int | None = None,
) -> None:
    post_key = str(row.get("post_key") or "")
    if not post_key:
        return
    existing = selected.get(post_key)
    if existing:
        if group not in existing["groups"]:
            existing["groups"].append(group)
        return
    selected[post_key] = _entry(row, group, rank)


def _sort_for_cap(entry: dict[str, Any]) -> tuple[int, float, str]:
    group_priority = {
        "top": 0,
        "signal_residual": 1,
        "bottom": 2,
        "comment_outlier": 3,
        "like_outlier": 4,
        "view_outlier": 5,
        "follower_correlated": 6,
    }.get(str(entry.get("group") or ""), 9)
    percentile = _num((entry.get("metrics") or {}).get("canonical_percentile"))
    if percentile is None:
        percentile = 999
    return group_priority, percentile, str(entry.get("post_key") or "")


def select_packet_posts(
    rows: list[dict[str, Any]],
    *,
    hard_cap: int = _HARD_CAP,
    max_top_bottom_per_format: int = _MAX_TOP_BOTTOM_PER_FORMAT,
) -> list[dict[str, Any]]:
    present_formats = [fmt for fmt in _FORMAT_ORDER if any(_media_format(row.get("media_type")) == fmt for row in rows)]
    if not present_formats:
        return []
    top_bottom_budget = max(20, int(hard_cap) - (3 * _ANOMALY_SLICE_CAP) - _SIGNAL_RESIDUAL_CAP)
    per_side = min(max_top_bottom_per_format, max(4, top_bottom_budget // max(1, len(present_formats) * 2)))
    selected: dict[str, dict[str, Any]] = {}

    for fmt in present_formats:
        format_rows = [row for row in rows if _media_format(row.get("media_type")) == fmt]
        top_rows = [row for row in format_rows if _best_top_metric(row)]
        top_rows.sort(key=lambda row: (_best_top_metric(row) or (999, "z"))[0])
        for idx, row in enumerate(top_rows[:per_side], start=1):
            _add_entry(selected, row, group="top", rank=idx)

        bottom_rows = [row for row in format_rows if _bottom_metric(row)]
        bottom_rows.sort(key=lambda row: (_bottom_metric(row) or (-1, "z"))[0], reverse=True)
        for idx, row in enumerate(bottom_rows[:per_side], start=1):
            _add_entry(selected, row, group="bottom", rank=idx)

        for axis, group in (("comments", "comment_outlier"), ("likes", "like_outlier"), ("views", "view_outlier")):
            if axis == "views" and fmt != "reels":
                continue
            scored = [(score, row) for row in format_rows if (score := _outlier_score(row, axis)) is not None and score > 0]
            scored.sort(key=lambda item: item[0], reverse=True)
            for idx, (_, row) in enumerate(scored[:_ANOMALY_SLICE_CAP], start=1):
                _add_entry(selected, row, group=group, rank=idx)

        follower_rows = [row for row in format_rows if _follower_signal(row)]
        for idx, row in enumerate(follower_rows[:_ANOMALY_SLICE_CAP], start=1):
            _add_entry(selected, row, group="follower_correlated", rank=idx)

    residual = [
        row for row in rows
        if str(row.get("post_key") or "") not in selected and _recent_signal_types(row)
    ]
    residual.sort(key=lambda row: str(row.get("posted_at") or ""), reverse=True)
    for row in residual[:_SIGNAL_RESIDUAL_CAP]:
        _add_entry(selected, row, group="signal_residual", rank=None)

    entries = list(selected.values())
    if len(entries) > hard_cap:
        entries = sorted(entries, key=_sort_for_cap)[:hard_cap]
    return sorted(
        entries,
        key=lambda entry: (
            _FORMAT_ORDER.index(str(entry.get("format") or "statics")) if str(entry.get("format") or "") in _FORMAT_ORDER else 9,
            _sort_for_cap(entry),
        ),
    )


def _d7_percentile(row: dict[str, Any]) -> float | None:
    value = _metric(row, "percentile_performance_exact", "d7")
    if value is None:
        value = _metric(row, "percentile_performance", "d7")
    return value


def select_warm_start_posts(rows: list[dict[str, Any]], *, hard_cap: int = _WARM_START_CAP) -> list[dict[str, Any]]:
    """Small pilot packet: recent signal, top, bottom, typical references.

    This is intentionally account-level, not per-format. It keeps pilot LLM
    cost bounded while still giving Pro contrast instead of only winners.
    """
    hard_cap = max(1, int(hard_cap))
    signal_quota = min(5, max(1, round(hard_cap * 5 / _WARM_START_CAP)))
    top_quota = min(4, max(1, round(hard_cap * 4 / _WARM_START_CAP)))
    bottom_quota = min(4, max(1, round(hard_cap * 4 / _WARM_START_CAP)))
    typical_quota = min(2, max(1, hard_cap - signal_quota - top_quota - bottom_quota))
    selected: dict[str, dict[str, Any]] = {}

    def room() -> bool:
        return len(selected) < hard_cap

    signal_rows = [row for row in rows if _recent_signal_types(row)]
    signal_rows.sort(key=lambda row: str(row.get("posted_at") or ""), reverse=True)
    for row in signal_rows[: min(signal_quota, hard_cap)]:
        if not room():
            break
        _add_entry(selected, row, group="signal_residual", rank=None)

    top_rows = [row for row in rows if _best_top_metric(row)]
    top_rows.sort(key=lambda row: (_best_top_metric(row) or (999, "z"))[0])
    top_rank = 0
    for row in top_rows:
        if not room() or top_rank >= top_quota:
            break
        key = str(row.get("post_key") or "")
        if key in selected:
            continue
        top_rank += 1
        _add_entry(selected, row, group="top", rank=top_rank)

    bottom_rows = [row for row in rows if _bottom_metric(row)]
    bottom_rows.sort(key=lambda row: (_bottom_metric(row) or (-1, "z"))[0], reverse=True)
    bottom_rank = 0
    for row in bottom_rows:
        if not room() or bottom_rank >= bottom_quota:
            break
        key = str(row.get("post_key") or "")
        if key in selected:
            continue
        bottom_rank += 1
        _add_entry(selected, row, group="bottom", rank=bottom_rank)

    percentiles = [value for row in rows if (value := _d7_percentile(row)) is not None]
    median_percentile = _median(percentiles)
    if median_percentile is not None:
        typical_rows = [row for row in rows if _d7_percentile(row) is not None]
        typical_rows.sort(key=lambda row: abs((_d7_percentile(row) or median_percentile) - median_percentile))
        typical_rank = 0
        for row in typical_rows:
            if not room() or typical_rank >= typical_quota:
                break
            key = str(row.get("post_key") or "")
            if key in selected:
                continue
            typical_rank += 1
            _add_entry(selected, row, group="typical", rank=typical_rank)

    if room():
        for entry in select_packet_posts(rows, hard_cap=hard_cap):
            if not room():
                break
            key = str(entry.get("post_key") or "")
            if key and key not in selected:
                selected[key] = entry

    return sorted(list(selected.values()), key=lambda entry: (str(entry.get("posted_at") or ""), str(entry.get("post_key") or "")), reverse=True)


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    metrics = normalized.get("checkpoint_metrics")
    if not isinstance(metrics, dict):
        normalized["checkpoint_metrics"] = {}
    source_signal_types = normalized.get("source_signal_types")
    if source_signal_types is None:
        normalized["source_signal_types"] = []
    if normalized.get("recent_signal_types") is None:
        normalized["recent_signal_types"] = []
    return normalized


def _fetch_rows(conn: Any, *, feeder_id: int | None = None, feed_id: int | None = None, window_days: int = _WINDOW_DAYS) -> list[dict[str, Any]]:
    where = ["p.posted_at >= now() - (%s::text || ' days')::interval"]
    params: list[Any] = [max(1, int(window_days))]
    if feeder_id is not None:
        where.append("fd.id = %s")
        params.append(feeder_id)
    if feed_id is not None:
        where.append("fd.feed_id = %s")
        params.append(feed_id)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            select
              p.post_key, p.post_url, p.caption, lower(coalesce(p.media_type, 'image')) as media_type,
              p.posted_at, p.duration_seconds, p.duration_bucket, p.carousel_slide_count, p.depth_bucket,
              fd.id as feeder_id, fd.feed_id, fd.handle, coalesce(fd.role, 'standard') as feeder_role,
              fd.context_role, fd.context_note, fd.bio,
              pf.fingerprint,
              pf.model_version as fingerprint_model_version,
              coalesce(
                jsonb_object_agg(
                  pm.checkpoint,
                  jsonb_build_object(
                    'checkpoint', pm.checkpoint,
                    'business_date_ist', pm.business_date_ist,
                    'computed_at', pm.computed_at,
                    'views', pm.views,
                    'likes', pm.likes,
                    'comments', pm.comments,
                    'metric_value', pm.metric_value,
                    'percentile_performance', pm.percentile_performance,
                    'percentile_performance_exact', pm.percentile_performance_exact,
                    'views_percentile', pm.views_percentile,
                    'likes_percentile', pm.likes_percentile,
                    'comments_percentile', pm.comments_percentile,
                    'feed_percentile', pm.feed_percentile,
                    'ranking_metric', pm.ranking_metric,
                    'ranking_multiple', pm.ranking_multiple,
                    'views_multiple', pm.views_multiple,
                    'likes_multiple', pm.likes_multiple,
                    'comments_multiple', pm.comments_multiple,
                    'hour_multiple', pm.hour_multiple
                  )
                ) filter (where pm.checkpoint is not null),
                '{{}}'::jsonb
              ) as checkpoint_metrics,
              coalesce(
                array_remove(array_agg(distinct s.signal_type) filter (where s.signal_type is not null), null),
                '{{}}'::text[]
              ) as source_signal_types,
              coalesce(
                array_remove(array_agg(distinct s.signal_type) filter (
                  where s.signal_type is not null and s.created_at >= now() - interval '14 days'
                ), null),
                '{{}}'::text[]
              ) as recent_signal_types
            from public.posts p
            join public.feeders fd on fd.id = p.feeder_id
            left join public.post_fingerprints pf on pf.post_key = p.post_key
            left join public.post_metrics pm on pm.post_key = p.post_key and pm.checkpoint in ('d3', 'd7', 'd21')
            left join public.signal_posts sp on sp.post_key = p.post_key
            left join public.signals s on s.id = sp.signal_id and s.created_at >= now() - interval '90 days'
            where {' and '.join(where)}
            group by
              p.post_key, p.post_url, p.caption, p.media_type, p.posted_at,
              p.duration_seconds, p.duration_bucket, p.carousel_slide_count, p.depth_bucket,
              fd.id, fd.feed_id, fd.handle, fd.role, fd.context_role, fd.context_note, fd.bio,
              pf.fingerprint, pf.model_version
            order by p.posted_at desc nulls last, p.post_key
            """,
            tuple(params),
        )
        rows = cur.fetchall()
    return [_normalize_row(dict(row)) for row in rows]


def _fetch_feeder(conn: Any, feeder_id: int) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select fd.id, fd.feed_id, fd.handle, coalesce(fd.role, 'standard') as role,
                   fd.context_role, fd.context_note, fd.bio, fd.follower_count
            from public.feeders fd
            where fd.id = %s
            limit 1
            """,
            (feeder_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else {}


def _fetch_feed(conn: Any, feed_id: int) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select f.id, f.name, f.context_bible,
                   (
                     select fd.id
                     from public.feeders fd
                     where fd.feed_id = f.id and fd.role = 'anchor' and fd.status = 'active'
                     order by fd.id
                     limit 1
                   ) as anchor_feeder_id
            from public.feeds f
            where f.id = %s
            limit 1
            """,
            (feed_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else {}


def _median(values: list[float]) -> float | None:
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return float(statistics.median(clean))


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def metric_windows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    out: dict[str, Any] = {}
    for fmt in _FORMAT_ORDER:
        format_rows = [row for row in rows if _media_format(row.get("media_type")) == fmt]
        out[fmt] = {}
        for days in (7, 30, 90):
            recent = []
            for row in format_rows:
                posted_at = _parse_dt(row.get("posted_at"))
                if posted_at and (now - posted_at).days <= days:
                    recent.append(row)
            d7_percentiles = [
                value for row in recent
                if (value := _metric(row, "percentile_performance", "d7")) is not None
            ]
            out[fmt][f"{days}d"] = {
                "post_count": len(recent),
                "d7_median_percentile": _median(d7_percentiles),
                "d7_top15_rate": (
                    round(sum(1 for value in d7_percentiles if value <= 15) / len(d7_percentiles), 4)
                    if d7_percentiles else None
                ),
                "views_multiple_median": _median([
                    value for row in recent if (value := _metric(row, "views_multiple", "d7")) is not None
                ]),
                "likes_multiple_median": _median([
                    value for row in recent if (value := _metric(row, "likes_multiple", "d7")) is not None
                ]),
                "comments_multiple_median": _median([
                    value for row in recent if (value := _metric(row, "comments_multiple", "d7")) is not None
                ]),
            }
    return out


def format_mix(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    counts = {fmt: sum(1 for row in rows if _media_format(row.get("media_type")) == fmt) for fmt in _FORMAT_ORDER}
    return {
        fmt: {
            "count": count,
            "share": round(count / total, 4) if total else 0,
        }
        for fmt, count in counts.items()
    }


def cadence_profile(rows: list[dict[str, Any]]) -> dict[str, Any]:
    posted = [_parse_dt(row.get("posted_at")) for row in rows]
    posted = [value for value in posted if value]
    if not posted:
        return {"posts_per_week": None, "post_count": 0, "hour_distribution": {}}
    earliest = min(posted)
    latest = max(posted)
    days = max(1, (latest - earliest).days + 1)
    hours: dict[str, int] = {}
    for value in posted:
        bucket = f"{value.hour:02d}:00"
        hours[bucket] = hours.get(bucket, 0) + 1
    return {
        "posts_per_week": round(len(posted) / days * 7, 2),
        "post_count": len(posted),
        "hour_distribution": dict(sorted(hours.items())),
    }


def follower_trends(conn: Any, *, feeder_id: int | None = None, feed_id: int | None = None) -> dict[str, Any]:
    where: list[str] = []
    params: list[Any] = []
    if feeder_id is not None:
        where.append("fd.id = %s")
        params.append(feeder_id)
    if feed_id is not None:
        where.append("fd.feed_id = %s")
        params.append(feed_id)
    clause = f"where {' and '.join(where)}" if where else ""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            select fd.id as feeder_id, fd.handle, s.snapshot_date_ist, s.follower_count
            from public.feeders fd
            join public.feeder_follower_snapshots s on s.feeder_id = fd.id
            {clause}
            order by fd.id, s.snapshot_date_ist desc
            """,
            tuple(params),
        )
        rows = [dict(row) for row in cur.fetchall()]
    by_feeder: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        fid = int(row.get("feeder_id") or 0)
        by_feeder.setdefault(fid, []).append(row)
    feeders: list[dict[str, Any]] = []
    for fid, items in by_feeder.items():
        latest = items[0] if items else {}
        prior_7 = items[6] if len(items) > 6 else items[-1] if len(items) > 1 else None
        prior_30 = items[29] if len(items) > 29 else items[-1] if len(items) > 1 else None
        latest_count = _num(latest.get("follower_count"))
        delta_7 = latest_count - (_num(prior_7.get("follower_count")) or latest_count) if latest_count is not None and prior_7 else None
        delta_30 = latest_count - (_num(prior_30.get("follower_count")) or latest_count) if latest_count is not None and prior_30 else None
        feeders.append({
            "feeder_id": fid,
            "handle": latest.get("handle"),
            "latest_count": latest_count,
            "delta_7d": delta_7,
            "delta_30d": delta_30,
        })
    if feeder_id is not None:
        return feeders[0] if feeders else {}
    deltas = [item["delta_30d"] for item in feeders if item.get("delta_30d") is not None]
    return {
        "feeders": feeders,
        "feed_median_delta_30d": _median(deltas),
    }


def _packet_summary(rows: list[dict[str, Any]], posts: list[dict[str, Any]], hard_cap: int) -> dict[str, Any]:
    missing = [post.get("post_key") for post in posts if not post.get("fingerprint")]
    return {
        "window_days": _WINDOW_DAYS,
        "population_posts": len(rows),
        "selected_posts": len(posts),
        "hard_cap": hard_cap,
        "missing_fingerprint_count": len(missing),
        "missing_fingerprint_post_keys": missing[:25],
        "format_mix": format_mix(rows),
    }


def build_feeder_evidence_packet(
    conn: Any,
    feeder_id: int,
    *,
    hard_cap: int = _HARD_CAP,
    window_days: int = _WINDOW_DAYS,
) -> dict[str, Any]:
    feeder = _fetch_feeder(conn, feeder_id)
    rows = _fetch_rows(conn, feeder_id=feeder_id, window_days=window_days)
    posts = select_packet_posts(rows, hard_cap=hard_cap)
    return {
        "packet_version": "evidence_packet_v4",
        "scope": "feeder",
        "feeder": feeder,
        "posts": posts,
        "metric_windows": metric_windows(rows),
        "follower_trends": follower_trends(conn, feeder_id=feeder_id),
        "cadence": cadence_profile(rows),
        "format_mix": format_mix(rows),
        "summary": _packet_summary(rows, posts, hard_cap),
    }


def build_feeder_warm_start_packet(
    conn: Any,
    feeder_id: int,
    *,
    hard_cap: int = _WARM_START_CAP,
    window_days: int = _WINDOW_DAYS,
) -> dict[str, Any]:
    feeder = _fetch_feeder(conn, feeder_id)
    rows = _fetch_rows(conn, feeder_id=feeder_id, window_days=window_days)
    posts = select_warm_start_posts(rows, hard_cap=hard_cap)
    return {
        "packet_version": "evidence_packet_v4_warm_start",
        "scope": "feeder",
        "compile_kind": "warm_start",
        "feeder": feeder,
        "posts": posts,
        "metric_windows": metric_windows(rows),
        "follower_trends": follower_trends(conn, feeder_id=feeder_id),
        "cadence": cadence_profile(rows),
        "format_mix": format_mix(rows),
        "summary": _packet_summary(rows, posts, hard_cap),
    }


def build_feed_evidence_packet(
    conn: Any,
    feed_id: int,
    *,
    hard_cap: int = _HARD_CAP,
    window_days: int = _WINDOW_DAYS,
) -> dict[str, Any]:
    feed = _fetch_feed(conn, feed_id)
    rows = _fetch_rows(conn, feed_id=feed_id, window_days=window_days)
    by_feeder: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        by_feeder.setdefault(int(row.get("feeder_id") or 0), []).append(row)

    selected: dict[str, dict[str, Any]] = {}
    feeder_count = max(1, len(by_feeder))
    per_feeder_cap = max(6, hard_cap // feeder_count)
    for feeder_rows in by_feeder.values():
        for entry in select_packet_posts(
            feeder_rows,
            hard_cap=per_feeder_cap,
            max_top_bottom_per_format=_MAX_FEED_TOP_BOTTOM_PER_FEEDER_FORMAT,
        ):
            selected[str(entry.get("post_key") or "")] = entry

    # Add feed-level residuals after per-feeder balancing so cohort alerts do
    # not disappear just because an account has many ordinary top/bottom posts.
    for row in rows:
        key = str(row.get("post_key") or "")
        if key and key not in selected and _recent_signal_types(row) and len(selected) < hard_cap + _SIGNAL_RESIDUAL_CAP:
            selected[key] = _entry(row, "signal_residual", None)

    posts = list(selected.values())
    if len(posts) > hard_cap:
        posts = sorted(posts, key=_sort_for_cap)[:hard_cap]
    posts = sorted(posts, key=lambda entry: (str(entry.get("feeder_handle") or ""), _sort_for_cap(entry)))
    return {
        "packet_version": "evidence_packet_v4",
        "scope": "feed",
        "feed": feed,
        "active_feeder_count": len(by_feeder),
        "posts": posts,
        "metric_windows": metric_windows(rows),
        "follower_trends": follower_trends(conn, feed_id=feed_id),
        "cadence": cadence_profile(rows),
        "format_mix": format_mix(rows),
        "summary": _packet_summary(rows, posts, hard_cap),
    }


def packet_post_keys_needing_fingerprints(packet: dict[str, Any], *, required_model_version: str | None = None) -> list[str]:
    keys: list[str] = []
    for post in packet.get("posts") or []:
        if not isinstance(post, dict):
            continue
        if (
            isinstance(post.get("fingerprint"), dict)
            and post.get("fingerprint")
            and (not required_model_version or post.get("fingerprint_model_version") == required_model_version)
        ):
            continue
        key = str(post.get("post_key") or "").strip()
        if key:
            keys.append(key)
    return _ordered_unique(keys)
