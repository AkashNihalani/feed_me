from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ENV_PATH = ROOT / "apps" / "worker" / ".env"
HANDLE = "lakmeindia"
DEFAULT_TRIGGER = "p/dxq_tfdcndg#f35"
DEFAULT_NON_COLLAB = "p/dyhntvpsnxc#f35"
BASE_WINDOW = 30


def _load_env() -> None:
    if not ENV_PATH.exists():
        return
    for raw_line in ENV_PATH.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

    dsn = os.environ.get("POSTGRES_DSN", "")
    if "db.worqtdkvicuhmdgoncru.supabase.co" in dsn:
        dsn = dsn.replace(
            "db.worqtdkvicuhmdgoncru.supabase.co",
            "aws-1-ap-south-1.pooler.supabase.com",
        )
        dsn = dsn.replace("postgres:", "postgres.worqtdkvicuhmdgoncru:", 1)
        os.environ["POSTGRES_DSN"] = dsn


def _fetch_row(conn: Any, post_key: str) -> dict[str, Any]:
    row = conn.execute(
        """
        select distinct on (p.post_key)
          p.post_key,
          p.feeder_id,
          f.handle,
          p.post_url,
          p.caption,
          lower(coalesce(p.media_type, 'reel')) as media_type,
          p.posted_at,
          p.related_handles,
          p.collab_post,
          pm.views,
          pm.likes,
          pm.comments,
          pm.views_multiple,
          pm.likes_multiple,
          pm.comments_multiple,
          pm.percentile_performance,
          pm.percentile_performance_exact,
          pm.business_date_ist,
          pc.condensation
        from public.posts p
        join public.feeders f on f.id = p.feeder_id
        join public.post_metrics pm
          on pm.post_key = p.post_key
         and lower(pm.checkpoint) = 'd7'
        join public.post_condensations pc
          on pc.post_key = p.post_key
         and pc.condensation_version = 'post_condensation_v5_character_transfer'
        where p.post_key = %s
        order by p.post_key, pm.computed_at desc nulls last
        """,
        (post_key,),
    ).fetchone()
    if not row:
        raise RuntimeError(f"post is missing d7 metrics or v5 condensation: {post_key}")
    return dict(row)


def _fetch_base(conn: Any, *, feeder_id: int, trigger_post_key: str, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        select distinct on (p.post_key)
          p.post_key,
          p.feeder_id,
          f.handle,
          p.post_url,
          p.caption,
          lower(coalesce(p.media_type, 'reel')) as media_type,
          p.posted_at,
          p.related_handles,
          p.collab_post,
          pm.views,
          pm.likes,
          pm.comments,
          pm.views_multiple,
          pm.likes_multiple,
          pm.comments_multiple,
          pm.percentile_performance,
          pm.percentile_performance_exact,
          pm.business_date_ist,
          pc.condensation
        from public.posts p
        join public.feeders f on f.id = p.feeder_id
        join public.post_metrics pm
          on pm.post_key = p.post_key
         and lower(pm.checkpoint) = 'd7'
        join public.post_condensations pc
          on pc.post_key = p.post_key
         and pc.condensation_version = 'post_condensation_v5_character_transfer'
        where p.feeder_id = %s
          and p.post_key <> %s
          and lower(coalesce(p.media_type, '')) in ('reel', 'video')
          and p.posted_at >= now() - interval '90 days'
        order by p.post_key, pm.computed_at desc nulls last
        """,
        (feeder_id, trigger_post_key),
    ).fetchall()
    deduped = [dict(row) for row in rows]
    deduped.sort(key=lambda row: row.get("posted_at") or "", reverse=True)
    if len(deduped) < limit:
        raise RuntimeError(f"only {len(deduped)} condensed base posts available; need {limit}")
    return deduped[:limit]


def _magnitude(row: dict[str, Any]) -> float:
    from apps.worker.app.fingerprint_intelligence import _d7_mult

    values = [_d7_mult(row, axis) for axis in ("views", "likes", "comments")]
    numeric = [float(value) for value in values if value is not None]
    return max(numeric) if numeric else 0.0


def _build_payload(trigger: dict[str, Any], base: list[dict[str, Any]]) -> dict[str, Any]:
    from apps.worker.app.fingerprint_intelligence import (
        _d7_collab_info,
        _d7_group_vs_usual,
        _d7_metric,
        _d7_metric_anomaly,
        _d7_mult,
        _d7_posted_on,
        _d7_recent_beats,
        _d7_recent_form,
        _d7_scene_body,
        _d7_standing,
        _d7_vs_usual,
    )

    trigger_scene = _d7_scene_body(trigger.get("condensation"))
    if not trigger_scene:
        raise RuntimeError("trigger condensation could not be converted into a D7 scene")

    last_10_rows = base[:10]
    record_rows = sorted(base[10:], key=_magnitude, reverse=True)[:20]

    last_10: list[dict[str, Any]] = []
    for idx, row in enumerate(last_10_rows, start=1):
        scene = _d7_scene_body(row.get("condensation"))
        if not scene:
            raise RuntimeError(f"base row missing scene: {row.get('post_key')}")
        last_10.append({
            "post_key": row.get("post_key"),
            "source_set": "last_10",
            "posts_ago": idx,
            "posted_on": _d7_posted_on(row),
            "posted_on_source": "posts.posted_at",
            "views": _d7_metric(row, "views"),
            "likes": _d7_metric(row, "likes"),
            "comments": _d7_metric(row, "comments"),
            **_d7_collab_info(row),
            "vs_usual": _d7_vs_usual(row),
            "scene": scene,
        })

    record: list[dict[str, Any]] = []
    for row in record_rows:
        scene = _d7_scene_body(row.get("condensation"))
        if not scene:
            continue
        record.append({
            "post_key": row.get("post_key"),
            "source_set": "record",
            "posted_on": _d7_posted_on(row),
            "posted_on_source": "posts.posted_at",
            "views": _d7_metric(row, "views"),
            "likes": _d7_metric(row, "likes"),
            "comments": _d7_metric(row, "comments"),
            **_d7_collab_info(row),
            "vs_usual": _d7_vs_usual(row),
            "scene": scene,
        })

    trigger_multiples = {axis: _d7_mult(trigger, axis) for axis in ("views", "likes", "comments")}
    recent_form = _d7_recent_form(last_10_rows)
    recent_form["vs_usual"] = _d7_group_vs_usual(last_10_rows)

    return {
        "account": {"handle": str(trigger.get("handle") or "").strip()},
        "this_post": {
            "post_key": trigger["post_key"],
            "posted_on": _d7_posted_on(trigger),
            "posted_on_source": "posts.posted_at",
            "caption": trigger.get("caption"),
            **_d7_collab_info(trigger),
            "views": _d7_metric(trigger, "views"),
            "likes": _d7_metric(trigger, "likes"),
            "comments": _d7_metric(trigger, "comments"),
            "vs_usual": _d7_vs_usual(trigger),
            "scene": trigger_scene,
        },
        "metric_anomaly": _d7_metric_anomaly(trigger_multiples, len(base) + 1),
        "standing": _d7_standing(trigger),
        "recent_form": recent_form,
        "recent_beats": _d7_recent_beats(trigger, last_10_rows),
        "feeder_file": {
            "last_10": last_10,
            "record": record,
        },
        "feeder_file_source": {
            "mode": "controlled_trigger_against_latest_30_condensed_posts",
            "base_post_count": len(base),
            "note": "Trigger is intentionally placed against the existing Lakme feeder file, not chronological prior posts.",
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trigger-post-key", default=DEFAULT_TRIGGER)
    parser.add_argument("--base-window", type=int, default=BASE_WINDOW)
    parser.add_argument("--model")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--store", action="store_true", help="store the controlled read in feeder_file_model_calls")
    args = parser.parse_args()

    _load_env()

    from apps.worker.app import config
    import psycopg
    from psycopg.rows import dict_row

    from apps.worker.app import fingerprint_intelligence as fi
    from apps.worker.app.fingerprint_intelligence import (
        _call_d7_read_model,
        _normalize_d7_read_mapping,
        _record_d7_read_model_call,
    )

    if args.model:
        config.D7_READ_MODEL = args.model
        fi.D7_READ_MODEL = args.model

    with psycopg.connect(os.environ["POSTGRES_DSN"], row_factory=dict_row) as conn:
        trigger = _fetch_row(conn, args.trigger_post_key)
        base = _fetch_base(conn, feeder_id=int(trigger["feeder_id"]), trigger_post_key=trigger["post_key"], limit=args.base_window)
        payload = _build_payload(trigger, base)
        preflight = {
            "handle": HANDLE,
            "trigger": {
                "post_key": trigger["post_key"],
                "url": trigger.get("post_url"),
                "posted_at": trigger.get("posted_at"),
                "views": trigger.get("views"),
                "likes": trigger.get("likes"),
                "comments": trigger.get("comments"),
                "collab_post": payload["this_post"].get("collab_post"),
                "related_handles": payload["this_post"].get("related_handles"),
            },
            "model": args.model or config.D7_READ_MODEL,
            "base_post_count": len(base),
            "last_10_count": len(payload["feeder_file"]["last_10"]),
            "record_count": len(payload["feeder_file"]["record"]),
            "recent_beats": payload.get("recent_beats"),
            "standing": payload.get("standing"),
        }
        print(json.dumps({"status": "preflight", **preflight}, default=str), flush=True)
        if args.dry_run:
            return

        result = _call_d7_read_model(payload, post_key=trigger["post_key"])
        if not result:
            raise RuntimeError("D7 model did not return a result")
        raw_output, parsed, call_error = result
        normalized = _normalize_d7_read_mapping(parsed, post_key=trigger["post_key"]) if parsed else None

        if args.store:
            _record_d7_read_model_call(
                conn,
                post_key=trigger["post_key"],
                user_payload=payload,
                raw_output=raw_output,
                parsed_output=parsed,
                status="complete" if parsed else "failed",
                error=call_error,
            )
            conn.commit()

        print(json.dumps({
            "status": "complete" if normalized else "failed",
            **preflight,
            "stored": bool(args.store),
            "d7_read": normalized,
            "error": call_error,
            "raw": None if normalized else raw_output,
        }, ensure_ascii=False, default=str, indent=2), flush=True)


if __name__ == "__main__":
    main()
