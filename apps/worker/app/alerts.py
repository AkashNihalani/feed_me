from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable
import math
from datetime import datetime, timedelta, timezone

from .db import (
    get_conn,
    list_feeds,
    list_recent_alert_events,
    get_or_init_alert_engine_state,
    mark_alert_engine_scan,
    upsert_alert_candidate,
)


@dataclass
class Candidate:
    feed_id: int
    feeder_id: int | None
    ui_tab: str
    alert_category: str
    alert_color: str
    alert_urgency: str
    alert_family: str
    alert_type: str
    impact: float
    confidence: float
    freshness: float
    novelty: float
    actionability: float
    title: str
    body: str
    payload: dict

    @property
    def priority(self) -> float:
        return (
            (self.impact * 0.35)
            + (self.confidence * 0.25)
            + (self.freshness * 0.20)
            + (self.novelty * 0.10)
            + (self.actionability * 0.10)
        )


ALERT_UI = {
    "velocity": {"color": "#CCFF00"},
    "competitive": {"color": "#FF2D8A"},
    "intelligence": {"color": "#39A8FF"},
}


def _cosine(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = 0.0
    norm_a = 0.0
    norm_b = 0.0
    for x, y in zip(a, b):
        dot += x * y
        norm_a += x * x
        norm_b += y * y
    denom = math.sqrt(norm_a) * math.sqrt(norm_b)
    if denom <= 0:
        return 0.0
    return dot / denom


def _recent_types(feed_id: int, hours: int = 24) -> set[str]:
    events = list_recent_alert_events(feed_id, hours)
    return {row["alert_type"] for row in events}


def _velocity_candidates(feed_id: int, recent: set[str], hot_since) -> Iterable[Candidate]:
    with get_conn() as conn:
        hot = conn.execute(
            """
            WITH thresholds AS (
              SELECT
                COALESCE(pcm.media_type, core.media_type, 'Unknown') AS media_type,
                pcm.checkpoint,
                percentile_cont(0.80) WITHIN GROUP (ORDER BY pcm.velocity_value) AS p80
              FROM post_checkpoint_metrics pcm
              LEFT JOIN posts_core core
                ON core.subscriber_id = pcm.subscriber_id
               AND core.handle = pcm.handle
               AND core.post_url = pcm.post_url
              WHERE pcm.feed_id=%s
                AND pcm.checkpoint_at >= NOW() - INTERVAL '30 days'
                AND pcm.velocity_value IS NOT NULL
              GROUP BY COALESCE(pcm.media_type, core.media_type, 'Unknown'), pcm.checkpoint
            ),
            latest AS (
              SELECT DISTINCT ON (pcm.feeder_id, pcm.post_url)
                pcm.feeder_id,
                pcm.handle,
                pcm.post_url,
                pcm.checkpoint,
                pcm.checkpoint_at,
                COALESCE(pcm.media_type, core.media_type, 'Unknown') AS media_type,
                pcm.velocity_value,
                COALESCE(ps.velocity_tag, pcm.velocity_tag) AS velocity_tag,
                COALESCE(ps.velocity_stage, UPPER(pcm.checkpoint)) AS velocity_stage,
                COALESCE(ps.velocity_percentile, pcm.velocity_percentile) AS velocity_percentile
              FROM post_checkpoint_metrics pcm
              LEFT JOIN posts_core core
                ON core.subscriber_id = pcm.subscriber_id
               AND core.handle = pcm.handle
               AND core.post_url = pcm.post_url
              LEFT JOIN post_signals ps
                ON ps.subscriber_id = pcm.subscriber_id
               AND ps.handle = pcm.handle
               AND ps.post_url = pcm.post_url
              WHERE pcm.feed_id=%s
                AND pcm.checkpoint_at > %s
                AND pcm.velocity_value IS NOT NULL
              ORDER BY pcm.feeder_id, pcm.post_url, pcm.checkpoint_at DESC
            )
            SELECT
              l.feeder_id,
              l.handle,
              l.post_url,
              l.velocity_tag,
              l.velocity_stage,
              l.velocity_percentile,
              l.velocity_value,
              l.checkpoint_at
            FROM latest l
            LEFT JOIN thresholds t
              ON t.media_type = l.media_type
             AND t.checkpoint = l.checkpoint
            WHERE
              (
                t.p80 IS NOT NULL
                AND l.velocity_value >= t.p80
              )
              OR (
                l.velocity_percentile ~ '^[0-9]{1,3}%$'
                AND regexp_replace(l.velocity_percentile, '[^0-9]', '', 'g')::INT <= 20
              )
            ORDER BY l.checkpoint_at DESC, l.velocity_value DESC
            LIMIT 10
            """,
            (feed_id, feed_id, hot_since),
        ).fetchall()

        for row in hot:
            if "velocity_spike" in recent:
                break
            tag = row["velocity_tag"] or "🔥"
            yield Candidate(
                feed_id=feed_id,
                feeder_id=row["feeder_id"],
                ui_tab="flags",
                alert_category="velocity",
                alert_color=ALERT_UI["velocity"]["color"],
                alert_urgency="now",
                alert_family="velocity",
                alert_type="velocity_spike",
                impact=0.9,
                confidence=0.8,
                freshness=0.95,
                novelty=0.75,
                actionability=0.9,
                title=f"Velocity spike on {row['handle']}",
                body=f"{tag} at {row['velocity_stage'] or 'latest'} ({row['velocity_percentile'] or 'n/a'}). Act in next 12h.",
                payload={"post_url": row["post_url"], "handle": row["handle"]},
            )

        decay = conn.execute(
            """
            WITH d1 AS (
              SELECT feeder_id, handle, post_url, velocity_value AS v1
              FROM post_checkpoint_metrics
              WHERE feed_id=%s AND checkpoint='d1'
            ),
            d2 AS (
              SELECT feeder_id, post_url, velocity_value AS v2
              FROM post_checkpoint_metrics
              WHERE feed_id=%s AND checkpoint='d2'
            )
            SELECT d1.feeder_id, d1.handle, d1.post_url, d1.v1, d2.v2
            FROM d1 JOIN d2
            ON d1.feeder_id=d2.feeder_id AND d1.post_url=d2.post_url
            WHERE d1.v1 > 0 AND d2.v2 > 0 AND d2.v2 <= d1.v1 * 0.6
            ORDER BY (d1.v1 - d2.v2) DESC
            LIMIT 3
            """,
            (feed_id, feed_id),
        ).fetchall()

        for row in decay:
            if "momentum_drop" in recent:
                break
            drop_pct = int(round(((row["v1"] - row["v2"]) / row["v1"]) * 100))
            yield Candidate(
                feed_id=feed_id,
                feeder_id=row["feeder_id"],
                ui_tab="flags",
                alert_category="velocity",
                alert_color=ALERT_UI["velocity"]["color"],
                alert_urgency="today",
                alert_family="velocity",
                alert_type="momentum_drop",
                impact=0.78,
                confidence=0.85,
                freshness=0.82,
                novelty=0.7,
                actionability=0.7,
                title=f"Momentum drop on {row['handle']}",
                body=f"Velocity fell {drop_pct}% from D1 to D2. Rework format before boosting.",
                payload={"post_url": row["post_url"], "handle": row["handle"], "drop_pct": drop_pct},
            )

        personal_record = conn.execute(
            """
            WITH recent_window AS (
              SELECT feeder_id, handle, post_url, metric_value, checkpoint_at,
                     ROW_NUMBER() OVER (PARTITION BY feeder_id ORDER BY metric_value DESC) AS rk
              FROM post_checkpoint_metrics
              WHERE feed_id=%s
                AND checkpoint='d0'
                AND checkpoint_at >= NOW() - INTERVAL '30 days'
                AND metric_value IS NOT NULL
            )
            SELECT feeder_id, handle, post_url, metric_value
            FROM recent_window
            WHERE rk=1
            ORDER BY metric_value DESC
            LIMIT 1
            """,
            (feed_id,),
        ).fetchone()
        if personal_record and "personal_record" not in recent:
            yield Candidate(
                feed_id=feed_id,
                feeder_id=personal_record["feeder_id"],
                ui_tab="flags",
                alert_category="velocity",
                alert_color=ALERT_UI["velocity"]["color"],
                alert_urgency="today",
                alert_family="velocity",
                alert_type="personal_record",
                impact=0.86,
                confidence=0.8,
                freshness=0.75,
                novelty=0.8,
                actionability=0.65,
                title=f"Personal record on {personal_record['handle']}",
                body=f"Highest D0 metric in 30 days. Replicate this format in next 48h.",
                payload={
                    "post_url": personal_record["post_url"],
                    "handle": personal_record["handle"],
                    "metric_value": float(personal_record["metric_value"] or 0),
                },
            )

        format_win = conn.execute(
            """
            SELECT pcm.feeder_id, pcm.handle, COALESCE(core.media_type, 'Unknown') AS media_type,
                   AVG(pcm.velocity_value) AS avg_velocity,
                   COUNT(*) AS n
            FROM post_checkpoint_metrics pcm
            LEFT JOIN posts_core core
              ON core.subscriber_id = pcm.subscriber_id
             AND core.handle = pcm.handle
             AND core.post_url = pcm.post_url
            WHERE pcm.feed_id=%s
              AND pcm.checkpoint IN ('d1','d2','d3')
              AND pcm.checkpoint_at >= NOW() - INTERVAL '14 days'
              AND pcm.velocity_value IS NOT NULL
            GROUP BY pcm.feeder_id, pcm.handle, COALESCE(core.media_type, 'Unknown')
            HAVING COUNT(*) >= 3
            ORDER BY avg_velocity DESC
            LIMIT 1
            """,
            (feed_id,),
        ).fetchone()
        if format_win and "format_win" not in recent:
            yield Candidate(
                feed_id=feed_id,
                feeder_id=format_win["feeder_id"],
                ui_tab="flags",
                alert_category="velocity",
                alert_color=ALERT_UI["velocity"]["color"],
                alert_urgency="today",
                alert_family="velocity",
                alert_type="format_win",
                impact=0.72,
                confidence=0.7,
                freshness=0.68,
                novelty=0.7,
                actionability=0.8,
                title=f"Format win on {format_win['handle']}",
                body=f"{format_win['media_type'] or 'mixed'} is leading on recent velocity.",
                payload={
                    "handle": format_win["handle"],
                    "media_type": format_win["media_type"],
                    "avg_velocity": float(format_win["avg_velocity"] or 0),
                },
            )


def _competitive_candidates(feed_id: int, recent: set[str], pattern_since) -> Iterable[Candidate]:
    with get_conn() as conn:
        pairs = conn.execute(
            """
            SELECT m.feeder_id, f.handle, m.velocity_delta, m.perf_delta, m.sample_size
            FROM feeder_pair_metrics m
            JOIN feeders f ON f.id = m.feeder_id
            WHERE m.feed_id=%s
              AND m.window_days=30
              AND m.computed_at > %s
            ORDER BY m.relation_score DESC
            LIMIT 5
            """,
            (feed_id, pattern_since),
        ).fetchall()
        for row in pairs:
            if "circle_leader" in recent:
                break
            if (row["sample_size"] or 0) < 4:
                continue
            yield Candidate(
                feed_id=feed_id,
                feeder_id=row["feeder_id"],
                ui_tab="flags",
                alert_category="competitive",
                alert_color=ALERT_UI["competitive"]["color"],
                alert_urgency="today",
                alert_family="competitive",
                alert_type="circle_leader",
                impact=0.82,
                confidence=0.72,
                freshness=0.65,
                novelty=0.7,
                actionability=0.75,
                title=f"{row['handle']} is leading your circle",
                body=f"7-day velocity delta vs anchor: {round(float(row['velocity_delta'] or 0), 2)}.",
                payload={
                    "handle": row["handle"],
                    "velocity_delta": float(row["velocity_delta"] or 0),
                    "perf_delta": float(row["perf_delta"] or 0),
                },
            )

        timing_gap = conn.execute(
            """
            SELECT EXTRACT(DOW FROM posted_at) AS dow, COUNT(*) AS n
            FROM posts_core
            WHERE subscriber_id = (SELECT subscriber_id FROM feeds WHERE id=%s)
              AND posted_at >= NOW() - INTERVAL '28 days'
            GROUP BY EXTRACT(DOW FROM posted_at)
            ORDER BY n ASC
            LIMIT 1
            """,
            (feed_id,),
        ).fetchone()
        if timing_gap and "timing_gap" not in recent:
            day_map = {
                0: "Sunday",
                1: "Monday",
                2: "Tuesday",
                3: "Wednesday",
                4: "Thursday",
                5: "Friday",
                6: "Saturday",
            }
            dow = int(timing_gap["dow"])
            yield Candidate(
                feed_id=feed_id,
                feeder_id=None,
                ui_tab="flags",
                alert_category="competitive",
                alert_color=ALERT_UI["competitive"]["color"],
                alert_urgency="today",
                alert_family="competitive",
                alert_type="timing_gap",
                impact=0.68,
                confidence=0.72,
                freshness=0.6,
                novelty=0.75,
                actionability=0.8,
                title="Posting lane is open",
                body=f"{day_map.get(dow, 'Unknown day')} has the lowest activity in your feed. Test a post there.",
                payload={"day_of_week": dow},
            )


def _intelligence_candidates(feed_id: int, recent: set[str], pattern_since) -> Iterable[Candidate]:
    with get_conn() as conn:
        sat = conn.execute(
            """
            SELECT signal_key, adoption_rate, velocity_delta, saturation_score, confidence
            FROM signal_aggregates
            WHERE feed_id=%s
              AND signal_type='media_type'
              AND window_key='d3'
              AND saturation_score >= 0.5
              AND confidence >= 0.5
              AND updated_at > %s
            ORDER BY saturation_score DESC, adoption_rate DESC
            LIMIT 1
            """,
            (feed_id, pattern_since),
        ).fetchone()
        if sat and "sector_fatigue" not in recent:
            yield Candidate(
                feed_id=feed_id,
                feeder_id=None,
                ui_tab="flags",
                alert_category="intelligence",
                alert_color=ALERT_UI["intelligence"]["color"],
                alert_urgency="today",
                alert_family="intelligence",
                alert_type="sector_fatigue",
                impact=0.8,
                confidence=float(sat["confidence"] or 0.6),
                freshness=0.68,
                novelty=0.78,
                actionability=0.82,
                title=f"Format fatigue in {sat['signal_key']}",
                body=f"Adoption is high but return is flattening. Rotate to a fresher format now.",
                payload={
                    "signal_key": sat["signal_key"],
                    "adoption_rate": float(sat["adoption_rate"] or 0),
                    "velocity_delta": float(sat["velocity_delta"] or 0),
                    "saturation_score": float(sat["saturation_score"] or 0),
                },
            )

        wave = conn.execute(
            """
            WITH thresholds AS (
              SELECT
                COALESCE(pcm.media_type, core.media_type, 'Unknown') AS media_type,
                pcm.checkpoint,
                percentile_cont(0.80) WITHIN GROUP (ORDER BY pcm.velocity_value) AS p80
              FROM post_checkpoint_metrics pcm
              LEFT JOIN posts_core core
                ON core.subscriber_id = pcm.subscriber_id
               AND core.handle = pcm.handle
               AND core.post_url = pcm.post_url
              WHERE pcm.feed_id=%s
                AND pcm.checkpoint_at >= NOW() - INTERVAL '30 days'
                AND pcm.velocity_value IS NOT NULL
              GROUP BY COALESCE(pcm.media_type, core.media_type, 'Unknown'), pcm.checkpoint
            ),
            recent AS (
              SELECT DISTINCT ON (pcm.feeder_id, pcm.post_url)
                COALESCE(pcm.media_type, core.media_type, 'Unknown') AS media_type,
                pcm.checkpoint,
                pcm.velocity_value
              FROM post_checkpoint_metrics pcm
              LEFT JOIN posts_core core
                ON core.subscriber_id = pcm.subscriber_id
               AND core.handle = pcm.handle
               AND core.post_url = pcm.post_url
              WHERE pcm.feed_id=%s
                AND pcm.checkpoint_at >= NOW() - INTERVAL '7 days'
                AND pcm.checkpoint_at > %s
                AND pcm.velocity_value IS NOT NULL
              ORDER BY pcm.feeder_id, pcm.post_url, pcm.checkpoint_at DESC
            )
            SELECT
              r.media_type,
              COUNT(*) AS n,
              AVG(
                CASE
                  WHEN t.p80 IS NOT NULL AND r.velocity_value >= t.p80 THEN 1
                  ELSE 0
                END
              ) AS hot_rate
            FROM recent r
            LEFT JOIN thresholds t
              ON t.media_type = r.media_type
             AND t.checkpoint = r.checkpoint
            GROUP BY r.media_type
            HAVING COUNT(*) >= 5
            ORDER BY hot_rate DESC, n DESC
            LIMIT 1
            """,
            (feed_id, feed_id, pattern_since),
        ).fetchone()
        if wave and "sector_wave" not in recent:
            hot_rate = int(round(float(wave["hot_rate"] or 0) * 100))
            yield Candidate(
                feed_id=feed_id,
                feeder_id=None,
                ui_tab="flags",
                alert_category="intelligence",
                alert_color=ALERT_UI["intelligence"]["color"],
                alert_urgency="today",
                alert_family="intelligence",
                alert_type="sector_wave",
                impact=0.84,
                confidence=0.7,
                freshness=0.7,
                novelty=0.8,
                actionability=0.8,
                title=f"Sector wave in {wave['media_type'] or 'mixed format'}",
                body=f"{hot_rate}% of recent posts are high-velocity in this format. Prioritize this next.",
                payload={"media_type": wave["media_type"], "hot_rate": hot_rate},
            )

        breakout = conn.execute(
            """
            SELECT
              pcm.feeder_id,
              pcm.handle,
              pcm.post_url,
              COALESCE(ps.velocity_percentile, pcm.velocity_percentile) AS velocity_percentile,
              pcm.velocity_value
            FROM post_checkpoint_metrics pcm
            LEFT JOIN post_signals ps
              ON ps.subscriber_id = pcm.subscriber_id
             AND ps.handle = pcm.handle
             AND ps.post_url = pcm.post_url
            WHERE pcm.feed_id=%s
              AND pcm.checkpoint_at > %s
              AND pcm.velocity_value IS NOT NULL
            ORDER BY pcm.velocity_value DESC, pcm.checkpoint_at DESC
            LIMIT 1
            """,
            (feed_id, pattern_since),
        ).fetchone()
        if breakout and "breakout_post" not in recent:
            yield Candidate(
                feed_id=feed_id,
                feeder_id=breakout["feeder_id"],
                ui_tab="flags",
                alert_category="intelligence",
                alert_color=ALERT_UI["intelligence"]["color"],
                alert_urgency="now",
                alert_family="intelligence",
                alert_type="breakout_post",
                impact=0.88,
                confidence=0.75,
                freshness=0.92,
                novelty=0.78,
                actionability=0.86,
                title=f"Breakout post on {breakout['handle']}",
                body=f"Rocket signal at {breakout['velocity_percentile'] or 'n/a'}. Reverse engineer and test quickly.",
                payload={
                    "handle": breakout["handle"],
                    "post_url": breakout["post_url"],
                },
            )

        if "visual_mimicry" not in recent:
            embeddings = conn.execute(
                """
                SELECT feeder_id, handle, post_url, embedding_json
                FROM post_embeddings
                WHERE feed_id=%s
                  AND signal_type='performance_semantic'
                  AND updated_at >= NOW() - INTERVAL '7 days'
                ORDER BY updated_at DESC
                LIMIT 60
                """,
                (feed_id,),
            ).fetchall()
            best = None
            vectors = []
            for row in embeddings:
                emb = row.get("embedding_json")
                if isinstance(emb, list) and emb:
                    vectors.append((row, emb))
            for idx in range(len(vectors)):
                row_a, emb_a = vectors[idx]
                for jdx in range(idx + 1, len(vectors)):
                    row_b, emb_b = vectors[jdx]
                    if row_a["feeder_id"] == row_b["feeder_id"]:
                        continue
                    sim = _cosine(emb_a, emb_b)
                    if sim >= 0.93 and (best is None or sim > best[0]):
                        best = (sim, row_a, row_b)
            if best:
                sim, row_a, row_b = best
                yield Candidate(
                    feed_id=feed_id,
                    feeder_id=row_b["feeder_id"],
                    ui_tab="flags",
                    alert_category="competitive",
                    alert_color=ALERT_UI["competitive"]["color"],
                    alert_urgency="today",
                    alert_family="competitive",
                    alert_type="visual_mimicry",
                    impact=0.77,
                    confidence=0.7,
                    freshness=0.72,
                    novelty=0.8,
                    actionability=0.82,
                    title=f"Possible mimicry: {row_b['handle']}",
                    body=f"Pattern similarity with {row_a['handle']} is high ({round(sim, 3)}). Differentiate your next creative.",
                    payload={
                        "source_handle": row_a["handle"],
                        "mimic_handle": row_b["handle"],
                        "source_post": row_a["post_url"],
                        "mimic_post": row_b["post_url"],
                        "similarity": round(sim, 4),
                    },
                )


def generate_alert_candidates(subscriber_id: int | None = None, max_per_feed: int = 3) -> dict[int, int]:
    feeds = [f for f in list_feeds() if subscriber_id is None or f["subscriber_id"] == subscriber_id]
    created: dict[int, int] = {}
    for feed in feeds:
        feed_id = feed["id"]
        scan_started_at = datetime.now(timezone.utc)
        state = get_or_init_alert_engine_state(feed_id)
        hot_since = state["last_hot_scan_at"] or (scan_started_at - timedelta(hours=24))
        pattern_since = state["last_pattern_scan_at"] or (scan_started_at - timedelta(hours=24))

        recent = _recent_types(feed_id, 24)
        candidates: list[Candidate] = list(_velocity_candidates(feed_id, recent, hot_since))
        candidates.extend(list(_intelligence_candidates(feed_id, recent, pattern_since)))
        if feed["mode"] == "anchor":
            candidates.extend(list(_competitive_candidates(feed_id, recent, pattern_since)))

        if not candidates:
            created[feed_id] = 0
            mark_alert_engine_scan(feed_id, hot_scan_at=scan_started_at, pattern_scan_at=scan_started_at)
            continue

        top = sorted(candidates, key=lambda candidate: candidate.priority, reverse=True)[:max_per_feed]
        for candidate in top:
            upsert_alert_candidate(
                feed_id=candidate.feed_id,
                feeder_id=candidate.feeder_id,
                ui_tab=candidate.ui_tab,
                alert_category=candidate.alert_category,
                alert_color=candidate.alert_color,
                alert_urgency=candidate.alert_urgency,
                alert_family=candidate.alert_family,
                alert_type=candidate.alert_type,
                priority_score=candidate.priority,
                impact_score=candidate.impact,
                confidence_score=candidate.confidence,
                freshness_score=candidate.freshness,
                novelty_score=candidate.novelty,
                actionability_score=candidate.actionability,
                title=candidate.title,
                body=candidate.body,
                payload=candidate.payload,
            )
        created[feed_id] = len(top)
        mark_alert_engine_scan(feed_id, hot_scan_at=scan_started_at, pattern_scan_at=scan_started_at)
    return created
