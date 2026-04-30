from __future__ import annotations

import json
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from psycopg.rows import dict_row

from .config import APP_TIMEZONE

_LOOKBACK_DAYS = 120
_OWN_WEEKLY_CAP = 2
_CROSS_WEEKLY_CAP = 5
_ANCHOR_WEEKLY_CAP = 3
_FAMILY_WEEKLY_CAP = 2
_WEEKLY_CAP_DAYS = 7
_AUDIENCE_EVENT_SIGNAL_TYPES = {
    "OWN_FOLLOWER_SPIKE",
    "OWN_FOLLOWER_DROP",
    "CROSS_FOLLOWER_WAVE",
    "ANCHOR_FOLLOWER_GAP",
}
_ANCHOR_CONTENT_SIGNAL_TYPES = {
    "ANCHOR_GAP_WIDENING",
    "ANCHOR_GAP_CLOSING",
    "ANCHOR_CHALLENGER_SURGE",
}
_ANCHOR_MIN_MEDIA_ANCHOR_POSTS = 5
_ANCHOR_MIN_MEDIA_FEED_POSTS = 10
_CONTENT_OVERLAP_RATIO = 0.67

_SIGNAL_PRIORITY = {
    "ANCHOR_CHALLENGER_SURGE": 100,
    "ANCHOR_GAP_WIDENING": 90,
    "ANCHOR_GAP_CLOSING": 90,
    "CROSS_FORMAT_SHIFT": 85,
    "CROSS_MOMENTUM": 80,
    "CROSS_MICRO_BREAKOUT": 76,
    "CROSS_MICRO_COMMENT_SPIKE": 74,
    "CROSS_MICRO_LIKE_HEAVY": 72,
    "CROSS_MICRO_VIRAL_PASSIVE": 70,
    "CROSS_MICRO_FADE": 68,
    "OWN_LATE_JUMP": 66,
    "OWN_BREAKOUT": 64,
    "OWN_BREAKOUT_EARLY": 62,
    "OWN_COMMENT_SPIKE": 60,
    "OWN_VIRAL_PASSIVE": 58,
    "OWN_LIKE_HEAVY": 56,
    "OWN_SUSTAIN": 54,
    "OWN_SUSTAIN_LONG": 52,
    "OWN_FADE": 50,
}


@dataclass
class Metric:
    checkpoint: str
    business_date: date | None
    percentile: float | None
    views: float | None
    likes: float | None
    comments: float | None
    views_baseline: float | None
    likes_baseline: float | None
    comments_baseline: float | None
    views_x: float | None
    likes_x: float | None
    comments_x: float | None


@dataclass
class Post:
    post_key: str
    feed_id: int
    feeder_id: int
    handle: str
    role: str
    posted_at: datetime | None
    media_type: str
    sub_bucket: str | None
    metrics: dict[str, Metric] = field(default_factory=dict)


@dataclass
class SignalCandidate:
    scope: str
    signal_type: str
    feed_id: int
    feeder_id: int | None
    checkpoint: str
    business_date: date
    media_type: str | None
    sub_bucket: str | None
    window_start: date | None
    window_end: date | None
    metric_snapshot: dict[str, Any]
    cohort_a: list[Post]
    cohort_b: list[Post] = field(default_factory=list)
    body: str = ""
    forced_status: str | None = None

    @property
    def signal_family(self) -> str:
        return self.signal_type


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        value = float(value)
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    try:
        parsed = float(str(value))
        return parsed if math.isfinite(parsed) else None
    except Exception:
        return None


def _first_float(*values: Any) -> float | None:
    for value in values:
        parsed = _to_float(value)
        if parsed is not None:
            return parsed
    return None


def _to_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _normalize_media_type(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"reel", "video"}:
        return "reel"
    if normalized in {"sidecar", "carousel"}:
        return "sidecar"
    if normalized in {"image", "photo"}:
        return "image"
    return normalized or "unknown"


def _raw_metric(post: Post, checkpoint: str) -> Metric | None:
    return post.metrics.get(checkpoint.lower())


def _metric_required_channels(post: Post) -> tuple[str, ...]:
    if post.media_type == "reel":
        return ("views", "likes", "comments")
    return ("likes", "comments")


def _metric_complete(post: Post, checkpoint: str) -> bool:
    metric = _raw_metric(post, checkpoint)
    if metric is None or metric.percentile is None:
        return False
    for channel in _metric_required_channels(post):
        if getattr(metric, channel) is None:
            return False
        if getattr(metric, f"{channel}_baseline") is None:
            return False
    return True


def _metric(post: Post, checkpoint: str) -> Metric | None:
    return _raw_metric(post, checkpoint) if _metric_complete(post, checkpoint) else None


def _pct(post: Post, checkpoint: str) -> float | None:
    metric = _metric(post, checkpoint)
    return metric.percentile if metric else None


def _has_pct(post: Post, checkpoint: str) -> bool:
    return _pct(post, checkpoint) is not None


def _top(post: Post, checkpoint: str, threshold: float) -> bool:
    pct = _pct(post, checkpoint)
    return pct is not None and pct <= threshold


def _bottom(post: Post, checkpoint: str, threshold: float) -> bool:
    pct = _pct(post, checkpoint)
    return pct is not None and pct >= threshold


def _dominant_multiple(lead: float | None, others: list[float | None], *, minimum: float = 2.0, gap: float = 2.0) -> bool:
    if lead is None or lead < minimum:
        return False
    comparable = [value for value in others if value is not None]
    if not comparable:
        return False
    return all(lead >= value * gap for value in comparable)


def _sorted_recent(posts: list[Post]) -> list[Post]:
    return sorted(posts, key=lambda post: (post.posted_at or datetime.min.replace(tzinfo=timezone.utc)), reverse=True)


def _median(values: list[float]) -> float | None:
    clean = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    if not clean:
        return None
    return float(statistics.median(clean))


def _avg(values: list[float]) -> float | None:
    clean = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    if not clean:
        return None
    return round(sum(clean) / len(clean), 4)


def _business_day(dt: datetime | None, timezone_name: str) -> date | None:
    if dt is None:
        return None
    try:
        tz = ZoneInfo(timezone_name)
    except Exception:
        tz = timezone.utc
    return dt.astimezone(tz).date()


def _latest_by_feeder(posts: list[Post], limit: int = 5) -> list[Post]:
    seen: set[int] = set()
    selected: list[Post] = []
    for post in _sorted_recent(posts):
        if post.feeder_id in seen:
            continue
        selected.append(post)
        seen.add(post.feeder_id)
        if len(selected) >= limit:
            break
    return selected


def _dominant_media_posts(
    posts: list[Post],
    checkpoint: str,
    *,
    limit: int = 5,
    unique_feeders: bool = False,
) -> tuple[str | None, list[Post]]:
    buckets: dict[str, list[Post]] = defaultdict(list)
    for post in posts:
        if _has_pct(post, checkpoint):
            buckets[post.media_type].append(post)
    if not buckets:
        return None, []

    def bucket_rank(item: tuple[str, list[Post]]) -> tuple[int, int, float, float]:
        _media_type, bucket = item
        coverage = len({post.feeder_id for post in bucket}) if unique_feeders else len(bucket)
        avg_pct = _avg([_pct(post, checkpoint) for post in bucket if _pct(post, checkpoint) is not None])
        latest_ts = max(
            [(post.posted_at or datetime.min.replace(tzinfo=timezone.utc)).timestamp() for post in bucket],
            default=0.0,
        )
        return coverage, len(bucket), -(avg_pct if avg_pct is not None else 999.0), latest_ts

    media_type, bucket = max(buckets.items(), key=bucket_rank)
    selected = _latest_by_feeder(bucket, limit) if unique_feeders else _sorted_recent(bucket)[:limit]
    return media_type, selected


def _sample_top(posts: list[Post], checkpoint: str, limit: int = 5) -> list[Post]:
    ranked = sorted(
        [post for post in posts if _has_pct(post, checkpoint)],
        key=lambda post: (_pct(post, checkpoint) or 999.0, -(post.posted_at or datetime.min.replace(tzinfo=timezone.utc)).timestamp()),
    )
    return ranked[:limit]


def _sample_worst(posts: list[Post], checkpoint: str, limit: int = 5) -> list[Post]:
    ranked = sorted(
        [post for post in posts if _has_pct(post, checkpoint)],
        key=lambda post: (_pct(post, checkpoint) or -1.0, (post.posted_at or datetime.min.replace(tzinfo=timezone.utc)).timestamp()),
        reverse=True,
    )
    return ranked[:limit]


def _sample_typical(posts: list[Post], checkpoint: str, target_percentile: float | None = None, limit: int = 5) -> list[Post]:
    candidates = [post for post in posts if _has_pct(post, checkpoint)]
    if not candidates:
        return []
    target = target_percentile
    if target is None:
        target = _median([_pct(post, checkpoint) for post in candidates if _pct(post, checkpoint) is not None])
    if target is None:
        return _sorted_recent(candidates)[:limit]
    ranked = sorted(
        candidates,
        key=lambda post: (
            abs((_pct(post, checkpoint) or target) - target),
            -(post.posted_at or datetime.min.replace(tzinfo=timezone.utc)).timestamp(),
        ),
    )
    return ranked[:limit]


def _jump_delta(post: Post, checkpoint: str, previous_checkpoint: str) -> float | None:
    previous = _pct(post, previous_checkpoint)
    current = _pct(post, checkpoint)
    if previous is None or current is None:
        return None
    return previous - current


def _sample_no_jump_reference(posts: list[Post], checkpoint: str, previous_checkpoint: str, limit: int = 2) -> list[Post]:
    ranked = sorted(
        [
            post for post in posts
            if _jump_delta(post, checkpoint, previous_checkpoint) is not None
            and (_jump_delta(post, checkpoint, previous_checkpoint) or 0.0) < 10.0
        ],
        key=lambda post: (
            abs(_jump_delta(post, checkpoint, previous_checkpoint) or 0.0),
            -(post.posted_at or datetime.min.replace(tzinfo=timezone.utc)).timestamp(),
        ),
    )
    selected = ranked[:limit]
    selected_keys = {post.post_key for post in selected}
    if len(selected) < limit:
        for post in _sample_typical(posts, checkpoint, None, limit):
            if post.post_key in selected_keys:
                continue
            selected.append(post)
            selected_keys.add(post.post_key)
            if len(selected) >= limit:
                break
    return selected


def _recent_metric_window(posts: list[Post], checkpoint: str, size: int) -> list[Post]:
    return [post for post in _sorted_recent(posts) if _has_pct(post, checkpoint)][:size]


def _prior_metric_window(posts: list[Post], checkpoint: str, recent_posts: list[Post], size: int) -> list[Post]:
    recent_keys = {post.post_key for post in recent_posts}
    return [post for post in _sorted_recent(posts) if post.post_key not in recent_keys and _has_pct(post, checkpoint)][:size]


def _stratum_key(post: Post, history_counts: dict[tuple[str, str | None], int], min_history: int) -> tuple[str, str | None]:
    if post.sub_bucket and history_counts.get((post.media_type, post.sub_bucket), 0) >= min_history:
        return post.media_type, post.sub_bucket
    return post.media_type, None


def _build_posts(rows: list[dict[str, Any]]) -> list[Post]:
    posts_by_key: dict[str, Post] = {}
    for row in rows:
        post_key = str(row.get("post_key") or "").strip()
        if not post_key:
            continue
        media_type = _normalize_media_type(row.get("media_type"))
        sub_bucket = None
        if media_type == "reel":
            sub_bucket = str(row.get("duration_bucket") or "DUR_UNKNOWN").strip() or "DUR_UNKNOWN"
        elif media_type == "sidecar":
            sub_bucket = str(row.get("depth_bucket") or "DEPTH_UNKNOWN").strip() or "DEPTH_UNKNOWN"

        post = posts_by_key.get(post_key)
        if post is None:
            post = Post(
                post_key=post_key,
                feed_id=int(row.get("feed_id") or 0),
                feeder_id=int(row.get("feeder_id") or 0),
                handle=str(row.get("handle") or ""),
                role=str(row.get("role") or "standard"),
                posted_at=_to_dt(row.get("posted_at")),
                media_type=media_type,
                sub_bucket=sub_bucket,
            )
            posts_by_key[post_key] = post

        checkpoint = str(row.get("checkpoint") or "").strip().lower()
        if checkpoint:
            post.metrics[checkpoint] = Metric(
                checkpoint=checkpoint,
                business_date=row.get("business_date_ist"),
                percentile=_first_float(row.get("percentile_performance_exact"), row.get("percentile_performance")),
                views=_to_float(row.get("views")),
                likes=_to_float(row.get("likes")),
                comments=_to_float(row.get("comments")),
                views_baseline=_to_float(row.get("views_baseline")),
                likes_baseline=_to_float(row.get("likes_baseline")),
                comments_baseline=_to_float(row.get("comments_baseline")),
                views_x=_to_float(row.get("views_multiple")),
                likes_x=_to_float(row.get("likes_multiple")),
                comments_x=_to_float(row.get("comments_multiple")),
            )
    return list(posts_by_key.values())


def _load_feed_posts(conn: Any, feed_id: int, business_date_ist: date, *, app_timezone: str) -> list[Post]:
    tz = ZoneInfo(app_timezone or APP_TIMEZONE or "Asia/Kolkata")
    window_end = datetime.combine(business_date_ist + timedelta(days=1), datetime.min.time(), tzinfo=tz).astimezone(timezone.utc)
    window_start = window_end - timedelta(days=_LOOKBACK_DAYS)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select
              fd.feed_id,
              fd.id as feeder_id,
              fd.handle,
              coalesce(fd.role, 'standard') as role,
              p.post_key,
              p.posted_at,
              lower(coalesce(p.media_type, 'unknown')) as media_type,
              p.duration_bucket,
              p.depth_bucket,
              pm.checkpoint,
              pm.business_date_ist,
              pm.percentile_performance,
              pm.percentile_performance_exact,
              pm.views,
              pm.likes,
              pm.comments,
              pm.views_baseline,
              pm.likes_baseline,
              pm.comments_baseline,
              pm.views_multiple,
              pm.likes_multiple,
              pm.comments_multiple
            from public.posts p
            join public.feeders fd on fd.id = p.feeder_id
            left join public.post_metrics pm on pm.post_key = p.post_key
            where fd.feed_id = %s
              and fd.status = 'active'
              and p.posted_at >= %s
              and p.posted_at < %s
            order by p.posted_at desc nulls last, p.post_key desc
            """,
            (feed_id, window_start, window_end),
        )
        return _build_posts(cur.fetchall())


def _window_dates(posts: list[Post], timezone_name: str) -> tuple[date | None, date | None]:
    days = [_business_day(post.posted_at, timezone_name) for post in posts if post.posted_at]
    clean = [day for day in days if day is not None]
    if not clean:
        return None, None
    return min(clean), max(clean)


def _snapshot(posts: list[Post], checkpoint: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    pct_values = [_pct(post, checkpoint) for post in posts]
    payload: dict[str, Any] = {
        "post_count": len(posts),
        "avg_percentile": _avg([value for value in pct_values if value is not None]),
        "best_percentile": min([value for value in pct_values if value is not None], default=None),
    }
    if extra:
        payload.update(extra)
    return payload


def _body(signal_type: str, count: int, checkpoint: str, metric: str | None = None) -> str:
    label = {
        "CROSS_FORMAT_SHIFT": "Format Productivity",
    }.get(signal_type, signal_type.replace("_", " ").title())
    bits = [label, f"{count} posts"]
    if checkpoint != "daily":
        bits.append(checkpoint.upper())
    if metric:
        bits.append(metric)
    return " · ".join(bits)


def _own_candidates(feed_id: int, feeder_posts: list[Post], business_date_ist: date, timezone_name: str) -> list[SignalCandidate]:
    candidates: list[SignalCandidate] = []
    posts_by_media: dict[str, list[Post]] = defaultdict(list)
    for post in feeder_posts:
        posts_by_media[post.media_type].append(post)

    for media_posts in posts_by_media.values():
        history_counts: dict[tuple[str, str | None], int] = defaultdict(int)
        for post in media_posts:
            history_counts[(post.media_type, post.sub_bucket)] += 1

        grouped: dict[tuple[str, str | None], list[Post]] = defaultdict(list)
        for post in media_posts:
            grouped[_stratum_key(post, history_counts, 7)].append(post)

        for (media_type, sub_bucket), posts in grouped.items():
            feeder_id = posts[0].feeder_id
            recent7_d3 = _recent_metric_window(posts, "d3", 7)
            prior30_d7_for_d3 = _prior_metric_window(posts, "d7", recent7_d3, 30)
            d3_breakouts = [post for post in recent7_d3 if _top(post, "d3", 10)]
            prior_median = _median([_pct(post, "d7") for post in prior30_d7_for_d3 if _pct(post, "d7") is not None])
            if len(recent7_d3) >= 7 and len(d3_breakouts) >= 2 and prior_median is not None and prior_median >= 35:
                ws, we = _window_dates(recent7_d3, timezone_name)
                candidates.append(SignalCandidate(
                    "own", "OWN_BREAKOUT_EARLY", feed_id, feeder_id, "d3", business_date_ist,
                    media_type, sub_bucket, ws, we,
                    _snapshot(d3_breakouts, "d3", {"prior_30_d7_median": prior_median, "trigger": "2_of_last_7_top_10_d3"}),
                    _sample_top(d3_breakouts, "d3", 3), _sample_typical(prior30_d7_for_d3, "d7", prior_median, 2),
                    _body("OWN_BREAKOUT_EARLY", len(d3_breakouts), "d3"),
                ))

            recent10_d7 = _recent_metric_window(posts, "d7", 10)
            prior30_d7 = _prior_metric_window(posts, "d7", recent10_d7, 30)
            prior_median_d7 = _median([_pct(post, "d7") for post in prior30_d7 if _pct(post, "d7") is not None])
            d7_breakouts = [post for post in recent10_d7 if _top(post, "d7", 10)]
            if len(recent10_d7) >= 10 and len(d7_breakouts) >= 3 and prior_median_d7 is not None and prior_median_d7 >= 35:
                ws, we = _window_dates(recent10_d7, timezone_name)
                candidates.append(SignalCandidate(
                    "own", "OWN_BREAKOUT", feed_id, feeder_id, "d7", business_date_ist,
                    media_type, sub_bucket, ws, we,
                    _snapshot(d7_breakouts, "d7", {"prior_30_d7_median": prior_median_d7, "trigger": "3_of_last_10_top_10_d7"}),
                    _sample_top(d7_breakouts, "d7", 3), _sample_typical(prior30_d7, "d7", prior_median_d7, 2),
                    _body("OWN_BREAKOUT", len(d7_breakouts), "d7"),
                ))

            recent15_d7 = _recent_metric_window(posts, "d7", 15)
            sustained = [post for post in recent15_d7 if _top(post, "d7", 15)]
            prior30_for_sustain = _prior_metric_window(posts, "d7", recent15_d7, 30)
            prior_sustain_median = _median([_pct(post, "d7") for post in prior30_for_sustain if _pct(post, "d7") is not None])
            if len(recent15_d7) >= 15 and len(sustained) >= 5 and prior_sustain_median is not None and prior_sustain_median > 40:
                ws, we = _window_dates(recent15_d7, timezone_name)
                candidates.append(SignalCandidate(
                    "own", "OWN_SUSTAIN", feed_id, feeder_id, "d7", business_date_ist,
                    media_type, sub_bucket, ws, we,
                    _snapshot(sustained, "d7", {"prior_30_d7_median": prior_sustain_median, "trigger": "5_of_last_15_top_15_d7"}),
                    _sample_top(sustained, "d7", 5), [],
                    _body("OWN_SUSTAIN", len(sustained), "d7"),
                ))

            recent10_d21 = _recent_metric_window(posts, "d21", 10)
            evergreen = [post for post in recent10_d21 if _top(post, "d21", 25)]
            if len(recent10_d21) >= 10 and len(evergreen) >= 3:
                ws, we = _window_dates(recent10_d21, timezone_name)
                candidates.append(SignalCandidate(
                    "own", "OWN_SUSTAIN_LONG", feed_id, feeder_id, "d21", business_date_ist,
                    media_type, sub_bucket, ws, we,
                    _snapshot(evergreen, "d21", {"trigger": "3_of_last_10_top_25_d21"}),
                    _sample_top(evergreen, "d21", 5), [],
                    _body("OWN_SUSTAIN_LONG", len(evergreen), "d21"),
                ))

            top25_recent = [post for post in recent10_d7 if _top(post, "d7", 25)]
            if len(recent10_d7) >= 10 and len(top25_recent) <= 1 and prior_median_d7 is not None and prior_median_d7 <= 30:
                ws, we = _window_dates(recent10_d7, timezone_name)
                failing = [post for post in recent10_d7 if not _top(post, "d7", 25)]
                candidates.append(SignalCandidate(
                    "own", "OWN_FADE", feed_id, feeder_id, "d7", business_date_ist,
                    media_type, sub_bucket, ws, we,
                    _snapshot(failing, "d7", {"prior_30_d7_median": prior_median_d7, "trigger": "0_1_of_last_10_top_25_d7"}),
                    _sample_worst(failing, "d7", 3), _sample_top(prior30_d7, "d7", 2),
                    _body("OWN_FADE", len(failing), "d7"),
                ))

            prior_comment_values: list[float] = []
            for post in prior30_d7:
                metric = _metric(post, "d7")
                if metric and metric.comments is not None:
                    prior_comment_values.append(metric.comments)
            prior_comment_median = _median(prior_comment_values)
            comment_floor = max(10.0, (prior_comment_median or 0.0) * 1.5)
            comment_spikes: list[Post] = []
            like_heavy: list[Post] = []
            viral_passive: list[Post] = []
            for post in recent10_d7:
                metric = _metric(post, "d7")
                if not metric:
                    continue
                if _dominant_multiple(metric.comments_x, [metric.likes_x, metric.views_x]) and (metric.comments or 0.0) >= comment_floor:
                    comment_spikes.append(post)
                if _dominant_multiple(metric.likes_x, [metric.comments_x, metric.views_x]):
                    like_heavy.append(post)
                if media_type == "reel" and _dominant_multiple(metric.views_x, [metric.likes_x, metric.comments_x]):
                    viral_passive.append(post)

            if len(recent10_d7) >= 10 and len(comment_spikes) >= 4:
                ws, we = _window_dates(recent10_d7, timezone_name)
                candidates.append(SignalCandidate(
                    "own", "OWN_COMMENT_SPIKE", feed_id, feeder_id, "d7", business_date_ist,
                    media_type, sub_bucket, ws, we,
                    _snapshot(comment_spikes, "d7", {"comment_floor": comment_floor, "trigger": "4_of_last_10_comments_x_2_gap_2x"}),
                    _sample_top(comment_spikes, "d7", 4), [],
                    _body("OWN_COMMENT_SPIKE", len(comment_spikes), "d7", "comments"),
                ))

            if len(recent10_d7) >= 10 and len(like_heavy) >= 4:
                ws, we = _window_dates(recent10_d7, timezone_name)
                candidates.append(SignalCandidate(
                    "own", "OWN_LIKE_HEAVY", feed_id, feeder_id, "d7", business_date_ist,
                    media_type, sub_bucket, ws, we,
                    _snapshot(like_heavy, "d7", {"trigger": "4_of_last_10_likes_x_2_gap_2x"}),
                    _sample_top(like_heavy, "d7", 4), [],
                    _body("OWN_LIKE_HEAVY", len(like_heavy), "d7", "likes"),
                ))

            if len(recent10_d7) >= 10 and len(viral_passive) >= 3:
                ws, we = _window_dates(recent10_d7, timezone_name)
                candidates.append(SignalCandidate(
                    "own", "OWN_VIRAL_PASSIVE", feed_id, feeder_id, "d7", business_date_ist,
                    media_type, sub_bucket, ws, we,
                    _snapshot(viral_passive, "d7", {"trigger": "3_of_last_10_views_x_2_gap_2x"}),
                    _sample_top(viral_passive, "d7", 3), [],
                    _body("OWN_VIRAL_PASSIVE", len(viral_passive), "d7", "views"),
                ))

            for checkpoint, previous_checkpoint in (("d7", "d3"), ("d21", "d7")):
                recent = _recent_metric_window(posts, checkpoint, 15)
                jumpers = [
                    post for post in recent
                    if _pct(post, previous_checkpoint) is not None
                    and _pct(post, checkpoint) is not None
                    and ((_pct(post, previous_checkpoint) or 0) - (_pct(post, checkpoint) or 0)) >= 25
                ]
                if len(jumpers) >= 3:
                    ws, we = _window_dates(recent, timezone_name)
                    candidates.append(SignalCandidate(
                        "own", "OWN_LATE_JUMP", feed_id, feeder_id, checkpoint, business_date_ist,
                        media_type, sub_bucket, ws, we,
                        _snapshot(jumpers, checkpoint, {"previous_checkpoint": previous_checkpoint, "trigger": "3_posts_plus_25_percentile_points"}),
                        _sample_top(jumpers, checkpoint, 3), _sample_no_jump_reference(recent, checkpoint, previous_checkpoint, 2),
                        _body("OWN_LATE_JUMP", len(jumpers), checkpoint),
                    ))

    return candidates


def _fetch_recent_signals(conn: Any, feed_id: int, since_days: int = 14) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select s.*, array_agg(sp.post_key order by sp.rank, sp.post_key) filter (where sp.cohort = 'a') as cohort_a_post_keys
            from public.signals s
            left join public.signal_posts sp on sp.signal_id = s.id
            where s.feed_id = %s
              and s.created_at >= now() - (%s::text || ' days')::interval
              and s.status in ('pending', 'fresh', 'stale')
            group by s.id
            """,
            (feed_id, max(1, since_days)),
        )
        return cur.fetchall()


def _cross_candidates(feed_id: int, posts: list[Post], business_date_ist: date, timezone_name: str, conn: Any) -> list[SignalCandidate]:
    candidates: list[SignalCandidate] = []
    active_feeders = {post.feeder_id for post in posts}
    if len(active_feeders) < 5:
        return candidates

    d7_posts = [post for post in posts if _has_pct(post, "d7")]
    cutoff_14 = datetime.combine(business_date_ist - timedelta(days=13), datetime.min.time(), tzinfo=ZoneInfo(timezone_name)).astimezone(timezone.utc)
    cutoff_74 = datetime.combine(business_date_ist - timedelta(days=73), datetime.min.time(), tzinfo=ZoneInfo(timezone_name)).astimezone(timezone.utc)
    recent_14 = [post for post in d7_posts if post.posted_at and post.posted_at >= cutoff_14]
    prior_60 = [post for post in d7_posts if post.posted_at and cutoff_74 <= post.posted_at < cutoff_14]

    for media_type in sorted({post.media_type for post in d7_posts}):
        recent_media = [post for post in recent_14 if post.media_type == media_type]
        prior_media = [post for post in prior_60 if post.media_type == media_type]
        active_media_feeders = {post.feeder_id for post in d7_posts if post.media_type == media_type}
        if len(active_media_feeders) >= 5:
            recent_hot_by_feeder: dict[int, list[Post]] = defaultdict(list)
            for post in recent_media:
                if _top(post, "d7", 15):
                    recent_hot_by_feeder[post.feeder_id].append(post)
            prior_hot_feeders = {post.feeder_id for post in prior_media if _top(post, "d7", 15)}
            recent_rate = len(recent_hot_by_feeder) / max(len(active_media_feeders), 1)
            prior_rate = len(prior_hot_feeders) / max(len(active_media_feeders), 1)
            if len(recent_hot_by_feeder) >= 4 and recent_rate >= 0.40 and (recent_rate - prior_rate) >= 0.15:
                selected = _latest_by_feeder([post for posts_for_feeder in recent_hot_by_feeder.values() for post in posts_for_feeder], 5)
                candidates.append(SignalCandidate(
                    "cross", "CROSS_MOMENTUM", feed_id, None, "d7", business_date_ist,
                    media_type, None, business_date_ist - timedelta(days=13), business_date_ist,
                    _snapshot(selected, "d7", {"recent_feeder_rate": round(recent_rate, 4), "prior_feeder_rate": round(prior_rate, 4), "trigger": "40pct_feeders_top15_same_media"}),
                    selected, [],
                    _body("CROSS_MOMENTUM", len(selected), "d7", media_type),
                ))

        if len(recent_media) < 5 or len(prior_media) < 10:
            continue
        recent_hot_rate = sum(1 for post in recent_media if _top(post, "d7", 15)) / len(recent_media)
        prior_hot_rate = sum(1 for post in prior_media if _top(post, "d7", 15)) / len(prior_media)
        if recent_hot_rate >= 0.30 and (recent_hot_rate - prior_hot_rate) >= 0.20:
            rising = _sample_top([post for post in recent_media if _top(post, "d7", 15)], "d7", 3)
            comparison = _sample_typical([post for post in recent_14 if post.media_type != media_type and _has_pct(post, "d7")], "d7", None, 2)
            candidates.append(SignalCandidate(
                "cross", "CROSS_FORMAT_SHIFT", feed_id, None, "d7", business_date_ist,
                media_type, None, business_date_ist - timedelta(days=13), business_date_ist,
                _snapshot(rising, "d7", {"recent_hot_rate": round(recent_hot_rate, 4), "prior_hot_rate": round(prior_hot_rate, 4), "trigger": "format_hot_rate_plus_20pp"}),
                rising, comparison,
                _body("CROSS_FORMAT_SHIFT", len(rising), "d7", media_type),
            ))

    recent_signals = _fetch_recent_signals(conn, feed_id, 14)
    micro_map = {
        "CROSS_MICRO_BREAKOUT": {"OWN_BREAKOUT", "OWN_BREAKOUT_EARLY"},
        "CROSS_MICRO_COMMENT_SPIKE": {"OWN_COMMENT_SPIKE"},
        "CROSS_MICRO_LIKE_HEAVY": {"OWN_LIKE_HEAVY"},
        "CROSS_MICRO_VIRAL_PASSIVE": {"OWN_VIRAL_PASSIVE"},
        "CROSS_MICRO_FADE": {"OWN_FADE"},
    }
    post_by_key = {post.post_key: post for post in posts}
    for cross_type, own_types in micro_map.items():
        contributing_by_media: dict[str, dict[int, str]] = defaultdict(dict)
        for row in recent_signals:
            scope = str(row.get("scope") or "")
            signal_type = str(row.get("signal_type") or "")
            feeder_id = int(row.get("feeder_id") or 0)
            if scope != "own" or signal_type not in own_types or not feeder_id:
                continue
            for key in [str(value) for value in (row.get("cohort_a_post_keys") or []) if value]:
                post = post_by_key.get(key)
                if not post or not _has_pct(post, "d7"):
                    continue
                contributing_by_media[post.media_type].setdefault(feeder_id, key)
                break
        for media_type, contributing in contributing_by_media.items():
            if len(contributing) < 3:
                continue
            selected = [post_by_key[key] for _feeder_id, key in sorted(contributing.items())][:5]
            candidates.append(SignalCandidate(
                "cross", cross_type, feed_id, None, "d7", business_date_ist,
                media_type, None, business_date_ist - timedelta(days=13), business_date_ist,
                _snapshot(selected, "d7", {"contributing_feeders": len(contributing), "trigger": "3_feeders_with_own_signal_same_media"}),
                selected, [],
                _body(cross_type, len(selected), "d7", media_type),
            ))

    follower_signal_feeders = {
        int(row.get("feeder_id") or 0)
        for row in recent_signals
        if str(row.get("scope") or "") == "own"
        and str(row.get("signal_type") or "") in {"OWN_FOLLOWER_SPIKE", "OWN_FOLLOWER_DROP"}
        and int(row.get("feeder_id") or 0)
    }
    follower_rate = len(follower_signal_feeders) / max(len(active_feeders), 1)
    if len(follower_signal_feeders) >= 2 and follower_rate >= 0.30:
        media_type, selected = _dominant_media_posts(
            [post for post in posts if post.feeder_id in follower_signal_feeders],
            "d7",
            limit=5,
            unique_feeders=True,
        )
        if selected:
            candidates.append(SignalCandidate(
                "cross", "CROSS_FOLLOWER_WAVE", feed_id, None, "daily", business_date_ist,
                media_type, None, business_date_ist - timedelta(days=13), business_date_ist,
                _snapshot(selected, "d7", {"affected_feeders": len(follower_signal_feeders), "active_feeders": len(active_feeders), "affected_rate": round(follower_rate, 4), "trigger": "30pct_feeders_follower_signal"}),
                selected, [],
                _body("CROSS_FOLLOWER_WAVE", len(selected), "daily"),
            ))

    return candidates


def _anchor_candidates(feed_id: int, posts: list[Post], business_date_ist: date, timezone_name: str) -> list[SignalCandidate]:
    anchor_posts = [post for post in posts if post.role == "anchor"]
    non_anchor_posts = [post for post in posts if post.role != "anchor"]
    if len(anchor_posts) < 10 or len(non_anchor_posts) < 20:
        return []

    cutoff_30 = datetime.combine(business_date_ist - timedelta(days=29), datetime.min.time(), tzinfo=ZoneInfo(timezone_name)).astimezone(timezone.utc)
    anchor_d7 = [post for post in anchor_posts if post.posted_at and post.posted_at >= cutoff_30 and _has_pct(post, "d7")]
    feed_d7 = [post for post in non_anchor_posts if post.posted_at and post.posted_at >= cutoff_30 and _has_pct(post, "d7")]
    anchor_median = _median([_pct(post, "d7") for post in anchor_d7 if _pct(post, "d7") is not None])
    feed_median = _median([_pct(post, "d7") for post in feed_d7 if _pct(post, "d7") is not None])
    if anchor_median is None or feed_median is None:
        return []

    candidates: list[SignalCandidate] = []
    anchor_id = anchor_posts[0].feeder_id
    for media_type in sorted({post.media_type for post in anchor_d7 + feed_d7}):
        anchor_media_d7 = [post for post in anchor_d7 if post.media_type == media_type]
        feed_media_d7 = [post for post in feed_d7 if post.media_type == media_type]
        if len(anchor_media_d7) < _ANCHOR_MIN_MEDIA_ANCHOR_POSTS or len(feed_media_d7) < _ANCHOR_MIN_MEDIA_FEED_POSTS:
            continue
        media_anchor_median = _median([_pct(post, "d7") for post in anchor_media_d7 if _pct(post, "d7") is not None])
        media_feed_median = _median([_pct(post, "d7") for post in feed_media_d7 if _pct(post, "d7") is not None])
        if media_anchor_median is None or media_feed_median is None:
            continue
        gap = media_feed_median - media_anchor_median
        if gap >= 8:
            candidates.append(SignalCandidate(
                "anchor", "ANCHOR_GAP_WIDENING", feed_id, anchor_id, "d7", business_date_ist,
                media_type, None, business_date_ist - timedelta(days=29), business_date_ist,
                {
                    "anchor_median": media_anchor_median,
                    "feed_median": media_feed_median,
                    "all_media_anchor_median": anchor_median,
                    "all_media_feed_median": feed_median,
                    "gap": round(gap, 4),
                    "trigger": "anchor_beats_feed_by_8pp_same_media",
                },
                _sample_top(anchor_media_d7, "d7", 3), _sample_typical(feed_media_d7, "d7", media_feed_median, 2),
                _body("ANCHOR_GAP_WIDENING", 3, "d7", media_type),
            ))
        if gap <= -8:
            candidates.append(SignalCandidate(
                "anchor", "ANCHOR_GAP_CLOSING", feed_id, anchor_id, "d7", business_date_ist,
                media_type, None, business_date_ist - timedelta(days=29), business_date_ist,
                {
                    "anchor_median": media_anchor_median,
                    "feed_median": media_feed_median,
                    "all_media_anchor_median": anchor_median,
                    "all_media_feed_median": feed_median,
                    "gap": round(gap, 4),
                    "trigger": "feed_beats_anchor_by_8pp_same_media",
                },
                _sample_top(feed_media_d7, "d7", 3), _sample_typical(anchor_media_d7, "d7", media_anchor_median, 2),
                _body("ANCHOR_GAP_CLOSING", 3, "d7", media_type),
            ))

    by_competitor: dict[int, list[Post]] = defaultdict(list)
    for post in non_anchor_posts:
        by_competitor[post.feeder_id].append(post)
    challenger_options: list[tuple[float, float, int, int, str, list[Post], list[Post], float]] = []
    for competitor_id, competitor_posts in by_competitor.items():
        for media_type in sorted({post.media_type for post in competitor_posts}):
            anchor_media_d7 = [post for post in anchor_d7 if post.media_type == media_type]
            if len(anchor_media_d7) < _ANCHOR_MIN_MEDIA_ANCHOR_POSTS:
                continue
            media_anchor_median = _median([_pct(post, "d7") for post in anchor_media_d7 if _pct(post, "d7") is not None])
            if media_anchor_median is None:
                continue
            competitor_media_posts = [post for post in competitor_posts if post.media_type == media_type]
            recent10 = _recent_metric_window(competitor_media_posts, "d7", 10)
            challengers = [
                post for post in recent10
                if (_pct(post, "d7") is not None and (_pct(post, "d7") or 999) <= media_anchor_median - 20)
            ]
            if len(recent10) >= 10 and len(challengers) >= 3:
                sampled = _sample_top(challengers, "d7", 3)
                sampled_pcts = [_pct(post, "d7") for post in sampled if _pct(post, "d7") is not None]
                avg_pct = _avg([value for value in sampled_pcts if value is not None]) or 999.0
                best_pct = min([value for value in sampled_pcts if value is not None], default=999.0)
                challenger_options.append((
                    avg_pct,
                    best_pct,
                    -len(challengers),
                    competitor_id,
                    media_type,
                    challengers,
                    anchor_media_d7,
                    media_anchor_median,
                ))
    if challenger_options:
        avg_pct, best_pct, negative_count, competitor_id, media_type, challengers, anchor_media_d7, media_anchor_median = sorted(challenger_options)[0]
        candidates.append(SignalCandidate(
            "anchor", "ANCHOR_CHALLENGER_SURGE", feed_id, competitor_id, "d7", business_date_ist,
            media_type, None, business_date_ist - timedelta(days=29), business_date_ist,
            _snapshot(challengers, "d7", {
                "anchor_median": media_anchor_median,
                "all_media_anchor_median": anchor_median,
                "challenger_avg_percentile": avg_pct,
                "challenger_best_percentile": best_pct,
                "challenger_count": abs(negative_count),
                "trigger": "3_of_last_10_beat_anchor_by_20pp_same_media",
            }),
            _sample_top(challengers, "d7", 3), _sample_typical(anchor_media_d7, "d7", media_anchor_median, 2),
            _body("ANCHOR_CHALLENGER_SURGE", len(challengers), "d7", media_type),
        ))

    return candidates


def _weekly_cap_status(conn: Any, candidate: SignalCandidate) -> str:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select id, status
            from public.signals
            where scope = %s
              and feed_id = %s
              and feeder_id is not distinct from %s
              and signal_type = %s
              and media_type is not distinct from %s
              and sub_bucket is not distinct from %s
              and checkpoint = %s
              and business_date_ist = %s
            limit 1
            """,
            (
                candidate.scope,
                candidate.feed_id,
                candidate.feeder_id,
                candidate.signal_type,
                candidate.media_type,
                candidate.sub_bucket,
                candidate.checkpoint,
                candidate.business_date,
            ),
        )
        existing = cur.fetchone()
        existing_id = int(existing.get("id") or 0) if existing else 0
        existing_status = str(existing.get("status") or "pending") if existing else ""
        if existing_status in {"dismissed", "fresh"}:
            return existing_status

        subject_filter = "and feeder_id = %s" if candidate.scope == "own" and candidate.feeder_id else ""
        subject_params: tuple[Any, ...] = (candidate.feeder_id,) if subject_filter else ()
        existing_filter = "and id <> %s" if existing_id else ""
        existing_params: tuple[Any, ...] = (existing_id,) if existing_id else ()
        scope_cap = _OWN_WEEKLY_CAP if candidate.scope == "own" else _CROSS_WEEKLY_CAP if candidate.scope == "cross" else _ANCHOR_WEEKLY_CAP

        if candidate.signal_type in _AUDIENCE_EVENT_SIGNAL_TYPES:
            cur.execute(
                f"""
                select count(*) as c
                from public.signals
                where feed_id = %s
                  and scope = %s
                  {subject_filter}
                  and signal_type = %s
                  {existing_filter}
                  and status in ('pending', 'fresh', 'stale')
                  and business_date_ist >= %s
                  and business_date_ist < %s
                """,
                (
                    candidate.feed_id,
                    candidate.scope,
                    *subject_params,
                    candidate.signal_type,
                    *existing_params,
                    candidate.business_date - timedelta(days=_WEEKLY_CAP_DAYS - 1),
                    candidate.business_date,
                ),
            )
            audience_event_count = int((cur.fetchone() or {}).get("c") or 0)
            if audience_event_count >= 1:
                return "suppressed_cap"

        cur.execute(
            f"""
            select count(*) as c
            from public.signals
            where feed_id = %s
              and scope = %s
              {subject_filter}
              {existing_filter}
              and status in ('pending', 'fresh', 'stale')
              and last_fired_at >= now() - (%s::text || ' days')::interval
            """,
            (candidate.feed_id, candidate.scope, *subject_params, *existing_params, _WEEKLY_CAP_DAYS),
        )
        scope_count = int((cur.fetchone() or {}).get("c") or 0)
        if scope_count >= scope_cap:
            return "suppressed_cap"

        cur.execute(
            f"""
            select count(*) as c
            from public.signals
            where feed_id = %s
              and scope = %s
              {subject_filter}
              and signal_family = %s
              {existing_filter}
              and status in ('pending', 'fresh', 'stale')
              and last_fired_at >= now() - (%s::text || ' days')::interval
            """,
            (candidate.feed_id, candidate.scope, *subject_params, candidate.signal_family, *existing_params, _WEEKLY_CAP_DAYS),
        )
        family_count = int((cur.fetchone() or {}).get("c") or 0)
        if family_count >= _FAMILY_WEEKLY_CAP:
            return "suppressed_cap"

    return "pending"


def _signal_post_role(candidate: SignalCandidate, cohort: str, rank: int, post: Post) -> str:
    if cohort == "a":
        return "trigger_core" if rank <= 2 else "trigger_support"

    signal_type = candidate.signal_type
    if signal_type in {"OWN_BREAKOUT", "OWN_BREAKOUT_EARLY"}:
        return "reference_typical"
    if signal_type == "OWN_FADE":
        return "reference_strong"
    if signal_type == "OWN_LATE_JUMP":
        return "reference_no_jump"
    if signal_type == "CROSS_FORMAT_SHIFT":
        return "reference_other_format"
    if signal_type in {"ANCHOR_GAP_WIDENING", "ANCHOR_GAP_CLOSING", "ANCHOR_CHALLENGER_SURGE", "ANCHOR_FOLLOWER_GAP"}:
        return "reference_anchor" if post.role == "anchor" else "reference_feed"
    return "reference_context"


def _candidate_signal_post_signature(candidate: SignalCandidate) -> list[tuple[str, str, int, str]]:
    rows: list[tuple[str, str, int, str]] = []
    has_comparison = len(candidate.cohort_b) > 0
    remaining_posts = 5
    cohort_limits = (("a", 3 if has_comparison else 5), ("b", 2))
    for cohort, limit in cohort_limits:
        cohort_posts = candidate.cohort_a if cohort == "a" else candidate.cohort_b
        if cohort == "a" and candidate.media_type:
            cohort_posts = [post for post in cohort_posts if post.media_type == candidate.media_type]
        if remaining_posts <= 0 or limit <= 0:
            break
        selected_posts = cohort_posts[:min(limit, remaining_posts)]
        for idx, post in enumerate(selected_posts, start=1):
            role = _signal_post_role(candidate, cohort, idx, post)
            rows.append((post.post_key, cohort, idx, role))
        remaining_posts -= len(selected_posts)
    return rows


def _json_signature(value: Any) -> str:
    return json.dumps(value or {}, sort_keys=True, default=str)


def _candidate_priority(candidate: SignalCandidate) -> int:
    return _SIGNAL_PRIORITY.get(candidate.signal_type, 0)


def _post_keys(posts: list[Post]) -> set[str]:
    return {post.post_key for post in posts if post.post_key}


def _overlap_ratio(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / max(min(len(left), len(right)), 1)


def _same_content_lane(left: SignalCandidate, right: SignalCandidate) -> bool:
    if left.signal_type in _AUDIENCE_EVENT_SIGNAL_TYPES or right.signal_type in _AUDIENCE_EVENT_SIGNAL_TYPES:
        return False
    if left.feed_id != right.feed_id:
        return False
    if left.business_date != right.business_date:
        return False
    if left.checkpoint != right.checkpoint:
        return False
    if left.media_type != right.media_type or left.sub_bucket != right.sub_bucket:
        return False
    if left.scope != right.scope:
        return False
    if left.scope == "own" and left.feeder_id != right.feeder_id:
        return False
    return True


def _candidates_overlap(left: SignalCandidate, right: SignalCandidate) -> bool:
    if not _same_content_lane(left, right):
        return False

    left_trigger = _post_keys(left.cohort_a)
    right_trigger = _post_keys(right.cohort_a)
    if len(left_trigger & right_trigger) >= 2 and _overlap_ratio(left_trigger, right_trigger) >= _CONTENT_OVERLAP_RATIO:
        return True

    left_all = left_trigger | _post_keys(left.cohort_b)
    right_all = right_trigger | _post_keys(right.cohort_b)
    if len(left_all & right_all) >= 3 and _overlap_ratio(left_all, right_all) >= _CONTENT_OVERLAP_RATIO:
        return True

    anchor_pair = {left.signal_type, right.signal_type}
    if anchor_pair == {"ANCHOR_GAP_CLOSING", "ANCHOR_CHALLENGER_SURGE"}:
        shared_refs = _post_keys(left.cohort_b) & _post_keys(right.cohort_b)
        return bool(shared_refs or (left_trigger & right_trigger))

    return False


def _suppress_overlapping_candidates(candidates: list[SignalCandidate]) -> list[SignalCandidate]:
    ordered = sorted(
        candidates,
        key=lambda candidate: (
            -_candidate_priority(candidate),
            candidate.scope,
            candidate.signal_type,
            candidate.feeder_id or 0,
            candidate.media_type or "",
            candidate.sub_bucket or "",
        ),
    )
    kept: list[SignalCandidate] = []
    suppressed: list[SignalCandidate] = []
    for candidate in ordered:
        if any(_candidates_overlap(candidate, visible) for visible in kept):
            candidate.forced_status = "suppressed_cap"
            suppressed.append(candidate)
        else:
            kept.append(candidate)
    return kept + suppressed


def _upsert_signal(conn: Any, candidate: SignalCandidate) -> int:
    status = candidate.forced_status or _weekly_cap_status(conn, candidate)
    new_post_signature = _candidate_signal_post_signature(candidate)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select id, status, metric_snapshot
            from public.signals
            where scope = %s
              and feed_id = %s
              and feeder_id is not distinct from %s
              and signal_type = %s
              and media_type is not distinct from %s
              and sub_bucket is not distinct from %s
              and checkpoint = %s
              and business_date_ist = %s
            limit 1
            """,
            (
                candidate.scope,
                candidate.feed_id,
                candidate.feeder_id,
                candidate.signal_type,
                candidate.media_type,
                candidate.sub_bucket,
                candidate.checkpoint,
                candidate.business_date,
            ),
        )
        existing_signal = cur.fetchone()
        existing_status = str((existing_signal or {}).get("status") or "")
        existing_metric_snapshot = (existing_signal or {}).get("metric_snapshot") or {}
        existing_post_signature: list[tuple[str, str, int, str]] = []
        existing_id = int((existing_signal or {}).get("id") or 0)
        if existing_id:
            cur.execute(
                """
                select post_key, cohort, rank, role
                from public.signal_posts
                where signal_id = %s
                order by cohort, rank, post_key
                """,
                (existing_id,),
            )
            existing_post_signature = [
                (
                    str(row.get("post_key") or ""),
                    str(row.get("cohort") or ""),
                    int(row.get("rank") or 0),
                    str(row.get("role") or ""),
                )
                for row in cur.fetchall()
            ]

        cur.execute(
            """
            insert into public.signals (
              feed_id, feeder_id, scope, signal_type, signal_family,
              media_type, sub_bucket, checkpoint, business_date_ist,
              trigger_window_start, trigger_window_end,
              metric_snapshot, status, body, last_fired_at, updated_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, now(), now())
            on conflict (
              scope,
              feed_id,
              feeder_id,
              signal_type,
              media_type,
              sub_bucket,
              checkpoint,
              business_date_ist
            )
            do update set
              metric_snapshot = excluded.metric_snapshot,
              status = case
                when public.signals.status in ('dismissed', 'fresh') then public.signals.status
                else excluded.status
              end,
              body = excluded.body,
              trigger_window_start = excluded.trigger_window_start,
              trigger_window_end = excluded.trigger_window_end,
              updated_at = now()
            returning id
            """,
            (
                candidate.feed_id,
                candidate.feeder_id,
                candidate.scope,
                candidate.signal_type,
                candidate.signal_family,
                candidate.media_type,
                candidate.sub_bucket,
                candidate.checkpoint,
                candidate.business_date,
                candidate.window_start,
                candidate.window_end,
                json.dumps(candidate.metric_snapshot, default=str),
                status,
                candidate.body[:1000],
            ),
        )
        signal_id = int((cur.fetchone() or {}).get("id") or 0)
        cur.execute("delete from public.signal_posts where signal_id = %s", (signal_id,))
        post_rows: list[tuple[int, str, str, int, str]] = [
            (signal_id, post_key, cohort, rank, role)
            for post_key, cohort, rank, role in new_post_signature
        ]
        if post_rows:
            cur.executemany(
                """
                insert into public.signal_posts (signal_id, post_key, cohort, rank, role)
                values (%s, %s, %s, %s, %s)
                on conflict (signal_id, post_key, cohort) do update
                set rank = excluded.rank,
                    role = excluded.role
                """,
                post_rows,
            )
        stale_existing_card = (
            existing_status == "fresh"
            and (
                _json_signature(existing_metric_snapshot) != _json_signature(candidate.metric_snapshot)
                or existing_post_signature != new_post_signature
            )
        )
        if stale_existing_card:
            cur.execute("delete from public.signal_intelligence where signal_id = %s", (signal_id,))
            cur.execute("update public.signals set status = 'stale', updated_at = now() where id = %s", (signal_id,))
        return signal_id


def _suppress_legacy_mixed_anchor_content_signals(
    conn: Any,
    feed_id: int,
    checkpoint: str,
    business_date_ist: date,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.signals
            set status = 'suppressed_cap',
                updated_at = now()
            where feed_id = %s
              and scope = 'anchor'
              and signal_type = any(%s::text[])
              and media_type is null
              and sub_bucket is null
              and checkpoint = %s
              and business_date_ist = %s
              and status in ('pending', 'stale')
            """,
            (feed_id, list(_ANCHOR_CONTENT_SIGNAL_TYPES), checkpoint, business_date_ist),
        )


def resolve_signals_for_feed(
    conn: Any,
    feeder_id: int,
    checkpoint: str,
    business_date_ist: date,
    *,
    app_timezone: str | None = None,
) -> dict[str, int]:
    cp = (checkpoint or "").strip().lower()
    if cp not in {"d3", "d7", "d21", "daily"}:
        return {"candidates": 0, "written": 0}

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select feed_id
            from public.feeders
            where id = %s and status = 'active'
            limit 1
            """,
            (feeder_id,),
        )
        row = cur.fetchone()
    feed_id = int((row or {}).get("feed_id") or 0)
    if not feed_id:
        return {"candidates": 0, "written": 0}

    timezone_name = app_timezone or APP_TIMEZONE or "Asia/Kolkata"
    posts = _load_feed_posts(conn, feed_id, business_date_ist, app_timezone=timezone_name)
    if not posts:
        return {"candidates": 0, "written": 0}

    own_candidates: list[SignalCandidate] = []
    feeder_groups: dict[int, list[Post]] = defaultdict(list)
    for post in posts:
        feeder_groups[post.feeder_id].append(post)
    for group_posts in feeder_groups.values():
        own_candidates.extend(_own_candidates(feed_id, group_posts, business_date_ist, timezone_name))
    own_candidates = _suppress_overlapping_candidates(own_candidates)
    written = 0
    for candidate in own_candidates:
        if candidate.checkpoint != cp and not (cp == "daily" and candidate.checkpoint == "daily"):
            continue
        _upsert_signal(conn, candidate)
        written += 1
    conn.commit()

    feed_candidates = _cross_candidates(feed_id, posts, business_date_ist, timezone_name, conn)
    feed_candidates.extend(_anchor_candidates(feed_id, posts, business_date_ist, timezone_name))
    feed_candidates = _suppress_overlapping_candidates(feed_candidates)
    if cp == "d7":
        _suppress_legacy_mixed_anchor_content_signals(conn, feed_id, cp, business_date_ist)
    for candidate in feed_candidates:
        if candidate.checkpoint != cp and not (cp == "daily" and candidate.checkpoint == "daily"):
            continue
        _upsert_signal(conn, candidate)
        written += 1
    conn.commit()
    return {"candidates": len(own_candidates) + len(feed_candidates), "written": written}


def resolve_audience_signals_for_feed(
    conn: Any,
    feeder_id: int,
    business_date_ist: date,
    *,
    app_timezone: str | None = None,
) -> dict[str, int]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select fd.feed_id
            from public.feeders fd
            where fd.id = %s and fd.status = 'active'
            limit 1
            """,
            (feeder_id,),
        )
        row = cur.fetchone()
    feed_id = int((row or {}).get("feed_id") or 0)
    if not feed_id:
        return {"candidates": 0, "written": 0}

    timezone_name = app_timezone or APP_TIMEZONE or "Asia/Kolkata"
    posts = _load_feed_posts(conn, feed_id, business_date_ist, app_timezone=timezone_name)
    by_feeder: dict[int, list[Post]] = defaultdict(list)
    for post in posts:
        by_feeder[post.feeder_id].append(post)

    candidates: list[SignalCandidate] = []
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select fd.id as feeder_id, fd.follower_count, s.snapshot_date_ist, s.follower_count as snapshot_count
            from public.feeders fd
            left join public.feeder_follower_snapshots s on s.feeder_id = fd.id
            where fd.feed_id = %s
              and fd.status = 'active'
              and s.snapshot_date_ist >= %s
              and s.snapshot_date_ist <= %s
            order by fd.id, s.snapshot_date_ist
            """,
            (feed_id, business_date_ist - timedelta(days=75), business_date_ist),
        )
        rows = cur.fetchall()

    snapshots: dict[int, list[tuple[date, float]]] = defaultdict(list)
    for row in rows:
        fid = int(row.get("feeder_id") or 0)
        snap_date = row.get("snapshot_date_ist")
        count = _to_float(row.get("snapshot_count"))
        if fid and isinstance(snap_date, date) and count is not None:
            snapshots[fid].append((snap_date, count))

    for fid, series in snapshots.items():
        if len(series) < 14:
            continue
        by_day = {day: count for day, count in series}
        latest_day = max(by_day)
        latest_count = by_day[latest_day]
        seven_day_prior = max([day for day in by_day if day <= latest_day - timedelta(days=7)], default=None)
        sixty_day_prior = max([day for day in by_day if day <= latest_day - timedelta(days=60)], default=None)
        if seven_day_prior is None:
            continue
        net_7d = latest_count - by_day[seven_day_prior]
        if sixty_day_prior is not None:
            weekly_rate = (latest_count - by_day[sixty_day_prior]) / max(((latest_day - sixty_day_prior).days / 7), 1)
        else:
            weekly_rate = 0.0
        deltas = [
            count - by_day.get(day - timedelta(days=7), count)
            for day, count in series
            if day - timedelta(days=7) in by_day
        ]
        volatility = statistics.pstdev(deltas) if len(deltas) >= 2 else max(abs(weekly_rate), 1.0)
        media_type, recent_posts = _dominant_media_posts(by_feeder.get(fid, []), "d7", limit=5)
        if not recent_posts:
            continue
        if net_7d >= max(50.0, latest_count * 0.01) and net_7d >= max(weekly_rate * 5.0, 50.0):
            candidates.append(SignalCandidate(
                "own", "OWN_FOLLOWER_SPIKE", feed_id, fid, "daily", business_date_ist,
                media_type, None, latest_day - timedelta(days=7), latest_day,
                _snapshot(recent_posts, "d7", {"net_7d": round(net_7d, 2), "weekly_rate": round(weekly_rate, 2), "latest_count": latest_count, "trigger": "7d_gain_5x_trailing_rate"}),
                recent_posts, [],
                _body("OWN_FOLLOWER_SPIKE", len(recent_posts), "daily", "followers"),
            ))
        if abs(net_7d) >= max(25.0, latest_count * 0.005) and net_7d <= -(max(volatility * 3.0, 25.0)):
            candidates.append(SignalCandidate(
                "own", "OWN_FOLLOWER_DROP", feed_id, fid, "daily", business_date_ist,
                media_type, None, latest_day - timedelta(days=7), latest_day,
                _snapshot(recent_posts, "d7", {"net_7d": round(net_7d, 2), "volatility": round(volatility, 2), "latest_count": latest_count, "trigger": "7d_loss_3x_volatility"}),
                recent_posts, [],
                _body("OWN_FOLLOWER_DROP", len(recent_posts), "daily", "followers"),
            ))

    # Anchor follower gap is a feed-level audience signal.
    anchor_fids = {post.feeder_id for post in posts if post.role == "anchor"}
    if anchor_fids:
        gains: dict[int, float] = {}
        for fid, series in snapshots.items():
            by_day = {day: count for day, count in series}
            latest_day = max(by_day) if by_day else None
            prior_day = max([day for day in by_day if latest_day and day <= latest_day - timedelta(days=30)], default=None)
            if latest_day and prior_day:
                gains[fid] = by_day[latest_day] - by_day[prior_day]
        if gains:
            anchor_gain = max([gain for fid, gain in gains.items() if fid in anchor_fids], default=None)
            non_anchor_gains = [gain for fid, gain in gains.items() if fid not in anchor_fids]
            feed_median_gain = _median(non_anchor_gains)
            if anchor_gain is not None and feed_median_gain is not None:
                winner_posts: list[Post] = []
                comparison_posts: list[Post] = []
                media_type: str | None = None
                if anchor_gain >= max(feed_median_gain * 2, 50):
                    media_type, winner_posts = _dominant_media_posts(
                        [post for post in posts if post.feeder_id in anchor_fids],
                        "d7",
                        limit=3,
                        unique_feeders=True,
                    )
                    comparison_posts = _latest_by_feeder(
                        [post for post in posts if post.feeder_id not in anchor_fids and post.media_type == media_type and _has_pct(post, "d7")],
                        2,
                    )
                elif feed_median_gain >= max(anchor_gain * 2, 50):
                    media_type, winner_posts = _dominant_media_posts(
                        [post for post in posts if post.feeder_id not in anchor_fids],
                        "d7",
                        limit=3,
                        unique_feeders=True,
                    )
                    comparison_posts = _latest_by_feeder(
                        [post for post in posts if post.feeder_id in anchor_fids and post.media_type == media_type and _has_pct(post, "d7")],
                        2,
                    )
                if winner_posts:
                    candidates.append(SignalCandidate(
                        "anchor", "ANCHOR_FOLLOWER_GAP", feed_id, winner_posts[0].feeder_id, "daily", business_date_ist,
                        media_type, None, business_date_ist - timedelta(days=30), business_date_ist,
                        _snapshot(winner_posts, "d7", {"anchor_gain": anchor_gain, "feed_median_gain": feed_median_gain, "trigger": "30d_follower_gap_2x"}),
                        winner_posts, comparison_posts,
                        _body("ANCHOR_FOLLOWER_GAP", len(winner_posts), "daily", "followers"),
                    ))

    written = 0
    for candidate in candidates:
        _upsert_signal(conn, candidate)
        written += 1
    conn.commit()

    active_feeders = {post.feeder_id for post in posts}
    recent_signals = _fetch_recent_signals(conn, feed_id, 14)
    follower_signal_feeders = {
        int(row.get("feeder_id") or 0)
        for row in recent_signals
        if str(row.get("scope") or "") == "own"
        and str(row.get("signal_type") or "") in {"OWN_FOLLOWER_SPIKE", "OWN_FOLLOWER_DROP"}
        and int(row.get("feeder_id") or 0)
    }
    follower_rate = len(follower_signal_feeders) / max(len(active_feeders), 1)
    if len(follower_signal_feeders) >= 2 and follower_rate >= 0.30:
        media_type, selected = _dominant_media_posts(
            [post for post in posts if post.feeder_id in follower_signal_feeders],
            "d7",
            limit=5,
            unique_feeders=True,
        )
        if selected:
            _upsert_signal(conn, SignalCandidate(
                "cross", "CROSS_FOLLOWER_WAVE", feed_id, None, "daily", business_date_ist,
                media_type, None, business_date_ist - timedelta(days=13), business_date_ist,
                _snapshot(selected, "d7", {"affected_feeders": len(follower_signal_feeders), "active_feeders": len(active_feeders), "affected_rate": round(follower_rate, 4), "trigger": "30pct_feeders_follower_signal"}),
                selected, [],
                _body("CROSS_FOLLOWER_WAVE", len(selected), "daily"),
            ))
            written += 1
            conn.commit()
    return {"candidates": len(candidates), "written": written}
