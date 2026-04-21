from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from dateutil import parser as date_parser
from psycopg.rows import dict_row

from .config import APP_TIMEZONE

_PATTERN_LOOKBACK_DAYS = 90
_PATTERN_RECENT_WINDOW_DAYS = 14
_PATTERN_HOT_PERCENTILE_MAX = 35
_PATTERN_CUE_KEYS = (
    "opening_move",
    "proof_mode",
    "pacing",
    "audio_mode",
    "style",
    "text_overlay",
    "density",
    "face",
    "duration_bucket",
    "depth",
)


def _to_dt(value: Any, *, app_timezone: str | None = None) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            try:
                source_tz = ZoneInfo(app_timezone or APP_TIMEZONE or "Asia/Kolkata")
            except Exception:
                source_tz = timezone.utc
            value = value.replace(tzinfo=source_tz)
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1_000_000_000_000:
            ts = ts / 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    try:
        dt = date_parser.parse(str(value))
        if dt.tzinfo is None:
            try:
                source_tz = ZoneInfo(app_timezone or APP_TIMEZONE or "Asia/Kolkata")
            except Exception:
                source_tz = timezone.utc
            dt = dt.replace(tzinfo=source_tz)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def resolve_pattern_alerts_for_feed(
    conn: Any,
    feeder_id: int,
    business_date_ist: date,
    *,
    app_timezone: str | None = None,
) -> None:
    def _tags(value: Any) -> dict[str, Any]:
        return value if isinstance(value, dict) else {}

    def _tag_text(value: Any) -> str | None:
        if isinstance(value, str) and value.strip():
            return value.strip().upper()
        return None

    def _normalize_media_type(value: Any) -> str:
        normalized = str(value or '').strip().lower()
        if normalized in {'reel', 'video'}:
            return 'reel'
        if normalized in {'sidecar', 'carousel'}:
            return 'carousel'
        if normalized in {'image', 'photo'}:
            return 'image'
        return normalized or 'unknown'

    def _is_hot(value: Any) -> bool:
        if isinstance(value, (int, float)):
            return float(value) <= _PATTERN_HOT_PERCENTILE_MAX
        return False

    def _metric_key(media_type: str, views: Any, likes: Any, comments: Any) -> str:
        if media_type == 'reel' and views is not None:
            return 'views'
        if likes is not None:
            return 'likes'
        if comments is not None:
            return 'comments'
        return 'views'

    def _metric_value(metric: str, views: Any, likes: Any, comments: Any) -> Any:
        if metric == 'views':
            return views
        if metric == 'comments':
            return comments
        return likes

    def _build_signatures(tags: dict[str, Any]) -> list[dict[str, Any]]:
        mechanic = _tag_text(tags.get('mechanic'))
        if not mechanic:
            return []
        return [{
            'mechanic': mechanic,
            'modifier_key': None,
            'modifier_value': None,
            'signature_key': mechanic,
        }]

    def _matches_signature(row: dict[str, Any], signature: dict[str, Any]) -> bool:
        tags = _tags(row.get('tags'))
        if _tag_text(tags.get('mechanic')) != signature['mechanic']:
            return False
        modifier_key = signature.get('modifier_key')
        modifier_value = signature.get('modifier_value')
        if not modifier_key:
            return True
        return _tag_text(tags.get(str(modifier_key))) == modifier_value

    def _avg_percentile(rows: list[dict[str, Any]]) -> float | None:
        values = [float(row['percentile']) for row in rows if isinstance(row.get('percentile'), (int, float))]
        if not values:
            return None
        return round(sum(values) / len(values), 2)

    def _baseline_share(rows: list[dict[str, Any]], signature: dict[str, Any]) -> float | None:
        tagged = [row for row in rows if _tag_text(_tags(row.get('tags')).get('mechanic'))]
        if not tagged:
            return None
        matched = [row for row in tagged if _matches_signature(row, signature)]
        return round(len(matched) / max(len(tagged), 1), 4)

    def _recent_lift(rows: list[dict[str, Any]]) -> float | None:
        if not rows:
            return None
        recent_hot = sum(1 for row in rows if row.get('business_day') and row['business_day'] >= recent_window_start)
        prior_hot = len(rows) - recent_hot
        if recent_hot <= 0:
            return None
        if prior_hot <= 0:
            return float(recent_hot)
        recent_rate = recent_hot / max(_PATTERN_RECENT_WINDOW_DAYS, 1)
        prior_rate = prior_hot / max(_PATTERN_LOOKBACK_DAYS - _PATTERN_RECENT_WINDOW_DAYS, 1)
        if prior_rate <= 0:
            return float(recent_hot)
        return round(recent_rate / prior_rate, 2)

    def _contrast_gap(rows: list[dict[str, Any]], signature: dict[str, Any]) -> float | None:
        matched = [row for row in rows if _matches_signature(row, signature) and isinstance(row.get('percentile'), (int, float))]
        other = [row for row in rows if not _matches_signature(row, signature) and isinstance(row.get('percentile'), (int, float))]
        if len(matched) == 0 or len(other) < 2:
            return None
        matched_avg = _avg_percentile(matched)
        other_avg = _avg_percentile(other)
        if matched_avg is None or other_avg is None:
            return None
        return round(other_avg - matched_avg, 2)

    def _common_cues(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        threshold = max(2, int(len(rows) * 0.6 + 0.999))
        counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for row in rows:
            tags = _tags(row.get('tags'))
            for key in _PATTERN_CUE_KEYS:
                value = _tag_text(tags.get(key))
                if value:
                    counts[key][value] += 1

        cues: list[dict[str, Any]] = []
        for key in _PATTERN_CUE_KEYS:
            ranked = sorted(counts[key].items(), key=lambda item: (-item[1], item[0]))
            if not ranked:
                continue
            value, count = ranked[0]
            if count < threshold:
                continue
            cues.append({
                'key': key,
                'value': value,
                'count': count,
            })
        return sorted(cues, key=lambda item: (-int(item['count']), str(item['key']), str(item['value'])))[:4]

    def _required_cues(cues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return cues[:2]

    def _pattern_key(signature: dict[str, Any], media_type: str, cues: list[dict[str, Any]]) -> str:
        cue_bits = [f"{cue['key']}:{cue['value']}" for cue in _required_cues(cues)]
        cue_suffix = '|'.join(cue_bits) if cue_bits else 'no-cues'
        return f"{signature['mechanic']}|media:{media_type}|{cue_suffix}"

    def _support_post_keys(rows: list[dict[str, Any]], current_post_key: str) -> list[str]:
        sorted_rows = sorted(
            rows,
            key=lambda row: (
                float(row['percentile']) if isinstance(row.get('percentile'), (int, float)) else 999.0,
                row.get('posted_at') or datetime.min.replace(tzinfo=timezone.utc),
            ),
        )
        support = [str(row['post_key']) for row in sorted_rows if str(row.get('post_key') or '') != current_post_key]
        return support[:4]

    def _candidate_score(
        family: str,
        signature: dict[str, Any],
        match_count: int,
        avg_percentile: float,
        baseline_share: float | None,
        *,
        feeders_count: int | None = None,
        anchor_gap: float | None = None,
        recent_lift: float | None = None,
    ) -> int:
        score = 36
        if family == 'cross':
            score += 10
        elif family == 'anchor':
            score += 16
        if signature.get('modifier_key'):
            score += 10
        score += max(0, min(20, int(round(35 - avg_percentile))))
        score += min(12, max(0, match_count - 1) * 3)
        if baseline_share is not None:
            score += min(12, max(0, int(round((1 - min(max(baseline_share, 0.0), 1.0)) * 12))))
        if feeders_count:
            score += min(10, max(0, feeders_count - 1) * 3)
        if anchor_gap:
            score += min(12, max(0, int(round(anchor_gap))))
        if recent_lift and recent_lift > 1:
            score += min(10, max(1, int(round(recent_lift * 2))))
        return score

    def _urgency(percentile: float, score: int) -> str:
        if score >= 85 or percentile <= 10:
            return 'now'
        if score >= 65 or percentile <= 20:
            return 'today'
        return 'watch'

    def _summary_body(candidate: dict[str, Any]) -> str:
        if candidate['family'] == 'anchor':
            return (
                f"Gap +{int(round(candidate['anchor_gap']))} pts vs anchor"
                f" · own top {int(round(candidate['avg_percentile']))}%"
                f" · anchor top {int(round(candidate['anchor_avg_percentile']))}%"
            )
        if candidate['family'] == 'cross':
            extra = ""
            if candidate.get('recent_lift'):
                extra = f" · 14d lift {candidate['recent_lift']:.1f}x"
            return (
                f"{candidate['match_count']} hot posts"
                f" · {candidate['feeders_count']} feeders"
                f" · avg top {int(round(candidate['avg_percentile']))}%"
                f"{extra}"
            )
        return (
            f"{candidate['match_count']} hot posts"
            f" · avg top {int(round(candidate['avg_percentile']))}%"
            f" · baseline {int(round(candidate['baseline_share'] * 100))}%"
        )

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select fd.feed_id
            from public.feeders fd
            where fd.id = %s
              and fd.status = 'active'
            limit 1
            """,
            (feeder_id,),
        )
        feed_row = cur.fetchone()

    feed_id = int((feed_row or {}).get('feed_id') or 0)
    if not feed_id:
        return

    timezone_name = app_timezone or APP_TIMEZONE or 'Asia/Kolkata'
    window_end = datetime.combine(
        business_date_ist + timedelta(days=1),
        datetime.min.time(),
        tzinfo=ZoneInfo(timezone_name),
    )
    window_start = window_end - timedelta(days=_PATTERN_LOOKBACK_DAYS)
    recent_window_start = business_date_ist - timedelta(days=_PATTERN_RECENT_WINDOW_DAYS - 1)

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
              pm.business_date_ist,
              pm.percentile_performance,
              pm.views,
              pm.likes,
              pm.comments,
              pi.tags
            from public.posts p
            join public.feeders fd
              on fd.id = p.feeder_id
            left join public.post_metrics pm
              on pm.post_key = p.post_key
             and lower(pm.checkpoint) = 'd7'
            left join public.post_intelligence pi
              on pi.post_key = p.post_key
            where fd.feed_id = %s
              and fd.status = 'active'
              and p.posted_at >= %s
              and p.posted_at < %s
            order by p.posted_at desc nulls last, p.post_key desc
            """,
            (feed_id, window_start, window_end),
        )
        raw_rows = cur.fetchall()

    conn.execute(
        """
        delete from public.fire_alerts
        where feed_id = %s
          and lower(checkpoint) = 'd7'
          and business_date_ist = %s
          and context in ('own', 'cross', 'anchor')
        """,
        (feed_id, business_date_ist),
    )

    if not raw_rows:
        conn.commit()
        return

    feed_rows: list[dict[str, Any]] = []
    anchor_feeder_id: int | None = None
    for row in raw_rows:
        tags = _tags(row.get('tags'))
        normalized = {
            'feed_id': int(row['feed_id']),
            'feeder_id': int(row['feeder_id']),
            'handle': str(row.get('handle') or ''),
            'role': str(row.get('role') or 'standard'),
            'post_key': str(row.get('post_key') or ''),
            'posted_at': _to_dt(row.get('posted_at'), app_timezone=timezone_name),
            'media_type': _normalize_media_type(row.get('media_type')),
            'business_day': row.get('business_date_ist'),
            'percentile': float(row['percentile_performance']) if isinstance(row.get('percentile_performance'), (int, float)) else None,
            'views': row.get('views'),
            'likes': row.get('likes'),
            'comments': row.get('comments'),
            'tags': tags,
        }
        if normalized['role'] == 'anchor':
            anchor_feeder_id = normalized['feeder_id']
        feed_rows.append(normalized)

    current_hot_rows = [
        row for row in feed_rows
        if row.get('business_day') == business_date_ist
        and _is_hot(row.get('percentile'))
        and _tag_text(_tags(row.get('tags')).get('mechanic'))
    ]
    if not current_hot_rows:
        conn.commit()
        return

    feed_hot_rows = [row for row in feed_rows if _is_hot(row.get('percentile'))]

    for current_row in current_hot_rows:
        signatures = _build_signatures(_tags(current_row.get('tags')))
        if not signatures:
            continue

        current_media_type = current_row['media_type']
        feeder_rows = [
            row for row in feed_rows
            if row['feeder_id'] == current_row['feeder_id'] and row['media_type'] == current_media_type
        ]
        feeder_hot_rows = [
            row for row in feed_hot_rows
            if row['feeder_id'] == current_row['feeder_id'] and row['media_type'] == current_media_type
        ]
        feed_media_rows = [row for row in feed_rows if row['media_type'] == current_media_type]
        feed_media_hot_rows = [row for row in feed_hot_rows if row['media_type'] == current_media_type]
        anchor_rows = [
            row for row in feed_rows
            if anchor_feeder_id is not None and row['feeder_id'] == anchor_feeder_id and row['media_type'] == current_media_type
        ]
        family_best: dict[str, dict[str, Any]] = {}

        for signature in signatures:
            min_match_count = 3
            avg_limit = _PATTERN_HOT_PERCENTILE_MAX
            baseline_limit = 0.80

            own_hot_matches = [row for row in feeder_hot_rows if _matches_signature(row, signature)]
            own_avg = _avg_percentile(own_hot_matches)
            own_baseline = _baseline_share(feeder_rows, signature)
            own_lift = _recent_lift(own_hot_matches)
            own_contrast = _contrast_gap([row for row in feeder_rows if row.get('percentile') is not None], signature)
            own_cues = _common_cues(own_hot_matches)

            if (
                len(own_hot_matches) >= min_match_count
                and own_avg is not None
                and own_avg <= avg_limit
                and own_baseline is not None
                and own_baseline <= baseline_limit
                and len(_required_cues(own_cues)) >= 2
            ):
                score = _candidate_score('own', signature, len(own_hot_matches), own_avg, own_baseline, recent_lift=own_lift)
                candidate = {
                    'family': 'own',
                    'signal_code': 'OWN_PATTERN',
                    'context': 'own',
                    'signature': signature,
                    'match_count': len(own_hot_matches),
                    'feeders_count': 1,
                    'avg_percentile': own_avg,
                    'baseline_share': own_baseline,
                    'recent_lift': own_lift,
                    'contrast_gap': own_contrast,
                    'support_post_keys': _support_post_keys(own_hot_matches, current_row['post_key']),
                    'cues': own_cues,
                    'required_cues': _required_cues(own_cues),
                    'media_type': current_media_type,
                    'pattern_key': _pattern_key(signature, current_media_type, own_cues),
                    'score': score,
                }
                family_best['own'] = max(
                    family_best.get('own', candidate),
                    candidate,
                    key=lambda item: (
                        1 if item['signature'].get('modifier_key') else 0,
                        item['score'],
                        -item['avg_percentile'],
                        item['match_count'],
                    ),
                )

            cross_hot_matches = [row for row in feed_media_hot_rows if _matches_signature(row, signature)]
            cross_avg = _avg_percentile(cross_hot_matches)
            cross_baseline = _baseline_share(feed_media_rows, signature)
            cross_lift = _recent_lift(cross_hot_matches)
            cross_feeder_counts: dict[int, int] = defaultdict(int)
            for row in cross_hot_matches:
                cross_feeder_counts[row['feeder_id']] += 1
            cross_feeders = len(cross_feeder_counts)
            dominant_share = (max(cross_feeder_counts.values()) / len(cross_hot_matches)) if cross_hot_matches else 1.0
            cross_cues = _common_cues(cross_hot_matches)

            if (
                len(cross_hot_matches) >= 2
                and cross_feeders >= 2
                and cross_avg is not None
                and cross_avg <= avg_limit
                and cross_baseline is not None
                and cross_baseline <= baseline_limit
                and dominant_share <= 0.60
                and len(_required_cues(cross_cues)) >= 2
            ):
                score = _candidate_score(
                    'cross',
                    signature,
                    len(cross_hot_matches),
                    cross_avg,
                    cross_baseline,
                    feeders_count=cross_feeders,
                    recent_lift=cross_lift,
                )
                candidate = {
                    'family': 'cross',
                    'signal_code': 'CROSS_PATTERN',
                    'context': 'cross',
                    'signature': signature,
                    'match_count': len(cross_hot_matches),
                    'feeders_count': cross_feeders,
                    'avg_percentile': cross_avg,
                    'baseline_share': cross_baseline,
                    'recent_lift': cross_lift,
                    'contrast_gap': own_contrast,
                    'support_post_keys': _support_post_keys(cross_hot_matches, current_row['post_key']),
                    'cues': cross_cues,
                    'required_cues': _required_cues(cross_cues),
                    'media_type': current_media_type,
                    'pattern_key': _pattern_key(signature, current_media_type, cross_cues),
                    'score': score,
                }
                family_best['cross'] = max(
                    family_best.get('cross', candidate),
                    candidate,
                    key=lambda item: (
                        1 if item['signature'].get('modifier_key') else 0,
                        item['score'],
                        -item['avg_percentile'],
                        item['match_count'],
                    ),
                )

            if anchor_feeder_id is None or current_row['feeder_id'] == anchor_feeder_id:
                continue

            anchor_signature_rows = [
                row for row in anchor_rows
                if _matches_signature(row, signature) and isinstance(row.get('percentile'), (int, float))
            ]
            anchor_avg = _avg_percentile(anchor_signature_rows)
            anchor_gap = round(anchor_avg - own_avg, 2) if anchor_avg is not None and own_avg is not None else None
            anchor_pattern_rows = own_hot_matches + anchor_signature_rows
            anchor_cues = _common_cues(anchor_pattern_rows)

            if (
                len(own_hot_matches) >= 2
                and len(anchor_signature_rows) >= 2
                and own_avg is not None
                and own_avg <= avg_limit
                and own_baseline is not None
                and own_baseline <= baseline_limit
                and anchor_gap is not None
                and anchor_gap >= 6
                and len(_required_cues(anchor_cues)) >= 2
            ):
                score = _candidate_score(
                    'anchor',
                    signature,
                    len(own_hot_matches),
                    own_avg,
                    own_baseline,
                    anchor_gap=anchor_gap,
                )
                candidate = {
                    'family': 'anchor',
                    'signal_code': 'ANCHOR_PATTERN',
                    'context': 'anchor',
                    'signature': signature,
                    'match_count': len(own_hot_matches),
                    'feeders_count': 1,
                    'avg_percentile': own_avg,
                    'baseline_share': own_baseline,
                    'recent_lift': own_lift,
                    'contrast_gap': own_contrast,
                    'anchor_avg_percentile': anchor_avg,
                    'anchor_gap': anchor_gap,
                    'support_post_keys': _support_post_keys(own_hot_matches, current_row['post_key']),
                    'anchor_support_post_keys': _support_post_keys(anchor_signature_rows, ''),
                    'cues': anchor_cues,
                    'required_cues': _required_cues(anchor_cues),
                    'media_type': current_media_type,
                    'pattern_key': _pattern_key(signature, current_media_type, anchor_cues),
                    'score': score,
                }
                family_best['anchor'] = max(
                    family_best.get('anchor', candidate),
                    candidate,
                    key=lambda item: (
                        1 if item['signature'].get('modifier_key') else 0,
                        item['score'],
                        -item['avg_percentile'],
                        item['match_count'],
                    ),
                )

        ranked_candidates = sorted(
            family_best.values(),
            key=lambda item: (
                1 if item['signature'].get('modifier_key') else 0,
                {'anchor': 3, 'cross': 2, 'own': 1}.get(item['family'], 0),
                item['score'],
                -item['avg_percentile'],
                item['match_count'],
            ),
            reverse=True,
        )[:2]

        if not ranked_candidates:
            continue

        metric_key = _metric_key(current_row['media_type'], current_row['views'], current_row['likes'], current_row['comments'])
        metric_value = _metric_value(metric_key, current_row['views'], current_row['likes'], current_row['comments'])

        for candidate in ranked_candidates:
            payload = {
                'version': 2,
                'pattern_name': candidate['signature']['mechanic'],
                'mechanic': candidate['signature']['mechanic'],
                'modifier_key': candidate['signature'].get('modifier_key'),
                'modifier_value': candidate['signature'].get('modifier_value'),
                'media_type': candidate.get('media_type'),
                'pattern_key': candidate.get('pattern_key'),
                'match_count': candidate['match_count'],
                'feeders_count': candidate['feeders_count'],
                'avg_hot_percentile': candidate['avg_percentile'],
                'baseline_share': candidate['baseline_share'],
                'recent_lift': candidate.get('recent_lift'),
                'contrast_gap': candidate.get('contrast_gap'),
                'anchor_avg_percentile': candidate.get('anchor_avg_percentile'),
                'anchor_gap': candidate.get('anchor_gap'),
                'cues': candidate.get('cues') or [],
                'required_cues': candidate.get('required_cues') or [],
                'support_post_keys': candidate.get('support_post_keys') or [],
                'anchor_support_post_keys': candidate.get('anchor_support_post_keys') or [],
            }
            dedupe_key = ':'.join([
                'pattern-v2',
                candidate['family'],
                candidate['signature']['signature_key'],
                current_row['post_key'],
                'd7',
                business_date_ist.isoformat(),
            ])
            conn.execute(
                """
                insert into public.fire_alerts (
                  dedupe_key,
                  feed_id,
                  feeder_id,
                  post_key,
                  checkpoint,
                  business_date_ist,
                  signal_code,
                  context,
                  alert_type,
                  status,
                  metric_key,
                  metric_value,
                  surface_percentile,
                  surface_delta,
                  body,
                  signal_payload,
                  created_at,
                  updated_at
                )
                values (
                  %s, %s, %s, %s, 'd7', %s, %s, %s, %s, 'new', %s, %s, %s, null, %s, %s::jsonb, now(), now()
                )
                on conflict (dedupe_key) do update
                set signal_code = excluded.signal_code,
                    context = excluded.context,
                    alert_type = excluded.alert_type,
                    metric_key = excluded.metric_key,
                    metric_value = excluded.metric_value,
                    surface_percentile = excluded.surface_percentile,
                    surface_delta = excluded.surface_delta,
                    body = excluded.body,
                    signal_payload = excluded.signal_payload,
                    status = 'new',
                    updated_at = now()
                """,
                (
                    dedupe_key,
                    current_row['feed_id'],
                    current_row['feeder_id'],
                    current_row['post_key'],
                    business_date_ist,
                    candidate['signal_code'],
                    candidate['context'],
                    _urgency(float(current_row['percentile']), int(candidate['score'])),
                    metric_key,
                    metric_value,
                    int(round(float(current_row['percentile']))),
                    _summary_body(candidate),
                    json.dumps(payload),
                ),
            )

    conn.commit()
