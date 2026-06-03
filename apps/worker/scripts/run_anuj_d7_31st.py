from __future__ import annotations

import json
import os
import sys
import argparse
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ENV_PATH = ROOT / "apps" / "worker" / ".env"
HANDLE = "anuj.mp4"
TARGET_ORDER = 31
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


def _select_trigger_and_base(
    conn,
    *,
    handle: str,
    target_order: int,
    base_window: int,
    trigger_post_key: str | None = None,
    pool_size: int | None = None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              p.post_key,
              p.feeder_id,
              f.handle,
              p.caption,
              lower(coalesce(p.media_type, 'reel')) as media_type,
              p.posted_at,
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
            where f.handle = %s
              and lower(coalesce(p.media_type, '')) in ('reel', 'video')
              and p.posted_at >= now() - interval '90 days'
            order by p.posted_at asc nulls last
            """,
            (handle,),
        )
        condensed_rows = [dict(row) for row in cur.fetchall()]

    if trigger_post_key:
        pool_count = pool_size or (base_window + 1)
        if len(condensed_rows) < pool_count:
            raise RuntimeError(f"only {len(condensed_rows)} condensed rows available; need {pool_count}")
        pool = condensed_rows[:pool_count]
        trigger = next((row for row in pool if str(row.get("post_key") or "") == trigger_post_key), None)
        if trigger is None:
            raise RuntimeError(f"trigger post is not in the first {pool_count} condensed rows: {trigger_post_key}")
        base = [row for row in pool if str(row.get("post_key") or "") != trigger_post_key]
        if len(base) != base_window:
            raise RuntimeError(f"expected {base_window} base rows after trigger swap, got {len(base)}")
        return trigger, base, condensed_rows

    if len(condensed_rows) < target_order:
        raise RuntimeError(f"only {len(condensed_rows)} condensed rows available")

    trigger_index = target_order - 1
    base_start = trigger_index - base_window
    if base_start < 0:
        raise RuntimeError(f"target order {target_order} has only {trigger_index} prior rows; need {base_window}")
    trigger = condensed_rows[trigger_index]
    base = condensed_rows[base_start:trigger_index]
    if len(base) != base_window:
        raise RuntimeError(f"expected {base_window} base rows, got {len(base)}")
    return trigger, base, condensed_rows


def _scene_preview(condensation: object, *, limit: int = 180) -> str | None:
    from apps.worker.app.fingerprint_intelligence import _d7_scene_body

    scene = _d7_scene_body(condensation)
    if not scene:
        return None
    if isinstance(scene, dict):
        for key in ("reel", "scene", "summary", "what_happens"):
            value = scene.get(key)
            if isinstance(value, str) and value.strip():
                scene = value
                break
        else:
            scene = json.dumps(scene, ensure_ascii=False, default=str)
    compact = " ".join(scene.split())
    return compact[:limit]


def _swap_base_post(
    *,
    trigger: dict,
    base: list[dict],
    condensed_rows: list[dict],
    swap_out_post_key: str | None,
    swap_in_post_key: str | None,
) -> tuple[list[dict], dict[str, object]]:
    if not swap_out_post_key and not swap_in_post_key:
        return base, {"enabled": False}

    base_keys = {str(row.get("post_key") or "") for row in base}
    trigger_key = str(trigger.get("post_key") or "")

    if not swap_out_post_key:
        raise RuntimeError("--swap-out-post-key is required when swapping context")
    swap_out_post_key = swap_out_post_key.strip()
    out_index = next((idx for idx, row in enumerate(base) if str(row.get("post_key") or "") == swap_out_post_key), None)
    if out_index is None:
        raise RuntimeError(f"swap-out post is not in the base context: {swap_out_post_key}")

    replacement: dict | None = None
    if swap_in_post_key:
        swap_in_post_key = swap_in_post_key.strip()
        replacement = next((row for row in condensed_rows if str(row.get("post_key") or "") == swap_in_post_key), None)
        if replacement is None:
            raise RuntimeError(f"swap-in post is not an available condensed Anuj row: {swap_in_post_key}")
        if swap_in_post_key == trigger_key:
            raise RuntimeError("swap-in post cannot be the trigger post")
        if swap_in_post_key in base_keys and swap_in_post_key != swap_out_post_key:
            raise RuntimeError("swap-in post is already in the base context")
    else:
        replacement = next(
            (
                row
                for row in condensed_rows
                if str(row.get("post_key") or "") not in base_keys
                and str(row.get("post_key") or "") != trigger_key
            ),
            None,
        )
        if replacement is None:
            raise RuntimeError("no condensed Anuj row is available outside the base context and trigger")

    swapped = list(base)
    outgoing = swapped[out_index]
    swapped[out_index] = replacement
    return swapped, {
        "enabled": True,
        "out": {
            "post_key": outgoing.get("post_key"),
            "posted_at": outgoing.get("posted_at"),
            "views": outgoing.get("views"),
        },
        "in": {
            "post_key": replacement.get("post_key"),
            "posted_at": replacement.get("posted_at"),
            "views": replacement.get("views"),
        },
    }


def _existing_d7(conn, post_key: str, prompt_version: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            select parsed_output
            from public.feeder_file_model_calls
            where call_type = 'd7_read'
              and post_key = %s
              and prompt_version = %s
              and status = 'complete'
              and parsed_output is not null
            order by updated_at desc nulls last, id desc
            limit 1
            """,
            (post_key, prompt_version),
        )
        row = cur.fetchone()
    parsed = row.get("parsed_output") if row else None
    return parsed if isinstance(parsed, dict) else None


def _build_payload(trigger: dict, base: list[dict]) -> dict:
    from apps.worker.app.fingerprint_intelligence import (
        _D7_RECORD_BAND_CAP,
        _D7_RECORD_MAX,
        _D7_RECORD_SPIKE_BAR,
        _d7_group_vs_usual,
        _d7_metric_anomaly,
        _d7_mult,
        _d7_posted_on,
        _d7_posted_ts,
        _d7_recent_beats,
        _d7_recent_form,
        _d7_scene_body,
        _d7_standing,
        _d7_vs_usual,
    )

    trigger_scene = _d7_scene_body(trigger.get("condensation"))
    if not trigger_scene:
        raise RuntimeError("trigger condensation could not be converted into a D7 scene")

    previous_rows = sorted(base, key=_d7_posted_ts, reverse=True)
    last_10_rows = previous_rows[:10]
    if len(last_10_rows) != 10:
        raise RuntimeError("missing last_10 rows")

    last_10 = []
    for idx, row in enumerate(last_10_rows):
        scene = _d7_scene_body(row.get("condensation"))
        if not scene:
            raise RuntimeError(f"base row missing scene: {row.get('post_key')}")
        last_10.append({
            "post_key": row.get("post_key"),
            "source_set": "last_10",
            "posts_ago": idx + 1,
            "posted_on": _d7_posted_on(row),
            "posted_on_source": "posts.posted_at",
            "views": row.get("views"),
            "likes": row.get("likes"),
            "comments": row.get("comments"),
            "vs_usual": _d7_vs_usual(row),
            "scene": scene,
        })

    trigger_ts = _d7_posted_ts(trigger)
    candidates = []
    for row in previous_rows[10:]:
        mags = [m for m in (_d7_mult(row, axis) for axis in ("views", "likes", "comments")) if m is not None]
        if not mags:
            continue
        magnitude = max(mags)
        if magnitude < _D7_RECORD_SPIKE_BAR:
            continue
        candidates.append((magnitude, row))
    candidates.sort(key=lambda item: item[0], reverse=True)

    band_counts = {0: 0, 1: 0, 2: 0}
    record = []
    for _magnitude, row in candidates:
        if len(record) >= _D7_RECORD_MAX:
            break
        days_ago = int(max(0.0, (trigger_ts - _d7_posted_ts(row)) / 86400.0))
        band = min(2, days_ago // 30)
        if band_counts[band] >= _D7_RECORD_BAND_CAP:
            continue
        scene = _d7_scene_body(row.get("condensation"))
        if not scene:
            continue
        band_counts[band] += 1
        record.append({
            "post_key": row.get("post_key"),
            "source_set": "record",
            "posted_on": _d7_posted_on(row),
            "posted_on_source": "posts.posted_at",
            "views": row.get("views"),
            "likes": row.get("likes"),
            "comments": row.get("comments"),
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
            "views": trigger.get("views"),
            "likes": trigger.get("likes"),
            "comments": trigger.get("comments"),
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
            "mode": "previous_30_condensed_posts",
            "base_post_count": len(base),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handle", default=HANDLE, help="feeder handle to run")
    parser.add_argument("--force", action="store_true", help="rerun D7 even if a complete read already exists")
    parser.add_argument("--target-order", type=int, default=TARGET_ORDER, help="1-based condensed post order to use as trigger")
    parser.add_argument("--base-window", type=int, default=BASE_WINDOW, help="number of prior condensed posts to use as feeder context")
    parser.add_argument("--trigger-post-key", help="use this post_key as trigger inside the first --pool-size condensed rows; base becomes the other rows")
    parser.add_argument("--pool-size", type=int, help="number of earliest condensed rows to use for --trigger-post-key mode")
    parser.add_argument("--model", help="override D7_READ_MODEL for this run")
    parser.add_argument("--swap-out-post-key", help="post_key to remove from the base context before running D7")
    parser.add_argument("--swap-in-post-key", help="post_key to insert into the base context; defaults to first condensed row outside base/trigger")
    parser.add_argument("--list-pool", action="store_true", help="print condensed rows eligible for trigger swaps, without calling D7")
    parser.add_argument("--dry-run", action="store_true", help="print trigger/base/swap details without calling D7")
    parser.add_argument("--no-store", action="store_true", help="run D7 and print the result without writing feeder_file_model_calls")
    args = parser.parse_args()

    _load_env()

    from apps.worker.app import config

    config.POSTGRES_DSN = os.environ["POSTGRES_DSN"]

    import psycopg
    from psycopg.rows import dict_row

    from apps.worker.app import fingerprint_intelligence as fi
    from apps.worker.app.fingerprint_intelligence import (
        D7_READ_PROMPT_VERSION,
        _call_d7_read_model,
        _normalize_d7_read_mapping,
        _record_d7_read_model_call,
    )
    if args.model:
        config.D7_READ_MODEL = args.model
        fi.D7_READ_MODEL = args.model

    conn = psycopg.connect(os.environ["POSTGRES_DSN"], row_factory=dict_row)
    try:
        trigger, base, condensed_rows = _select_trigger_and_base(
            conn,
            handle=args.handle,
            target_order=args.target_order,
            base_window=args.base_window,
            trigger_post_key=args.trigger_post_key,
            pool_size=args.pool_size,
        )
        if args.list_pool:
            pool_count = args.pool_size or args.target_order
            pool = condensed_rows[:pool_count]
            print(json.dumps({
                "status": "pool",
                "handle": args.handle,
                "pool_count": len(pool),
                "rows": [
                    {
                        "order": idx + 1,
                        "post_key": row.get("post_key"),
                        "posted_at": row.get("posted_at"),
                        "views": row.get("views"),
                        "likes": row.get("likes"),
                        "comments": row.get("comments"),
                        "caption": row.get("caption"),
                        "scene_preview": _scene_preview(row.get("condensation")),
                    }
                    for idx, row in enumerate(pool)
                ],
            }, default=str, indent=2), flush=True)
            return
        base, swap_info = _swap_base_post(
            trigger=trigger,
            base=base,
            condensed_rows=condensed_rows,
            swap_out_post_key=args.swap_out_post_key,
            swap_in_post_key=args.swap_in_post_key,
        )
        if args.dry_run:
            print(json.dumps({
                "status": "dry_run",
                "handle": args.handle,
                "trigger_post_key": trigger["post_key"],
                "trigger_posted_at": trigger["posted_at"],
                "target_order": args.target_order,
                "trigger_mode": "post_key_pool_swap" if args.trigger_post_key else "target_order",
                "model": args.model or config.D7_READ_MODEL,
                "base_post_count": len(base),
                "base_first_post_key": base[0]["post_key"] if base else None,
                "base_last_post_key": base[-1]["post_key"] if base else None,
                "swap": swap_info,
            }, default=str, indent=2), flush=True)
            return

        existing = _existing_d7(conn, trigger["post_key"], D7_READ_PROMPT_VERSION)
        if existing and not args.force and not args.no_store:
            normalized = _normalize_d7_read_mapping(existing, post_key=trigger["post_key"])
            print(json.dumps({
                "status": "existing",
                "trigger_post_key": trigger["post_key"],
                "target_order": args.target_order,
                "base_post_count": len(base),
                "d7_read": normalized,
            }, default=str, indent=2), flush=True)
            return

        payload = _build_payload(trigger, base)
        print(json.dumps({
            "status": "calling_d7",
            "handle": args.handle,
            "trigger_post_key": trigger["post_key"],
            "trigger_posted_at": trigger["posted_at"],
            "target_order": args.target_order,
            "trigger_mode": "post_key_pool_swap" if args.trigger_post_key else "target_order",
            "model": args.model or config.D7_READ_MODEL,
            "base_post_count": len(base),
            "base_first_post_key": base[0]["post_key"] if base else None,
            "base_last_post_key": base[-1]["post_key"] if base else None,
            "swap": swap_info,
            "last_10_count": len(payload["feeder_file"]["last_10"]),
            "record_count": len(payload["feeder_file"]["record"]),
        }, default=str), flush=True)

        result = _call_d7_read_model(payload, post_key=trigger["post_key"])
        if not result:
            raise RuntimeError("D7 model did not return a result")
        raw_output, parsed, call_error = result
        status = "complete" if parsed else "failed"
        if not args.no_store:
            _record_d7_read_model_call(
                conn,
                post_key=trigger["post_key"],
                user_payload=payload,
                raw_output=raw_output,
                parsed_output=parsed,
                status=status,
                error=call_error,
            )
            conn.commit()

        print(json.dumps({
            "status": f"{status}_not_stored" if args.no_store else status,
            "handle": args.handle,
            "trigger_post_key": trigger["post_key"],
            "target_order": args.target_order,
            "trigger_mode": "post_key_pool_swap" if args.trigger_post_key else "target_order",
            "model": args.model or config.D7_READ_MODEL,
            "base_post_count": len(base),
            "base_first_post_key": base[0]["post_key"] if base else None,
            "base_last_post_key": base[-1]["post_key"] if base else None,
            "swap": swap_info,
            "last_10_count": len(payload["feeder_file"]["last_10"]),
            "record_count": len(payload["feeder_file"]["record"]),
            "d7_read": parsed,
            "error": call_error,
        }, default=str, indent=2), flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
