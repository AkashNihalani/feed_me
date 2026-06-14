from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ENV_PATHS = (
    ROOT / "apps" / "worker" / ".env",
    ROOT / "apps" / "worker" / ".env.vps-production",
)
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
DEFAULT_HANDLES = ("anuj.mp4", "lakmeindia")


def _load_env() -> None:
    for env_path in ENV_PATHS:
        if not env_path.exists():
            continue
        for raw_line in env_path.read_text().splitlines():
            line = raw_line.strip()
            if line.startswith("export "):
                line = line[len("export "):]
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

    dsn = os.environ.get("POSTGRES_DSN", "")
    if "db.worqtdkvicuhmdgoncru.supabase.co" in dsn:
        dsn = dsn.replace("db.worqtdkvicuhmdgoncru.supabase.co", POOLER_HOST)
        dsn = dsn.replace("postgres:", "postgres.worqtdkvicuhmdgoncru:", 1)
        os.environ["POSTGRES_DSN"] = dsn


def _target_rows(conn: Any, *, handle: str, target: int, model_version: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            with latest_d7 as (
              select distinct on (post_key)
                post_key,
                views,
                likes,
                comments,
                percentile_performance,
                percentile_performance_exact,
                computed_at
              from public.post_metrics
              where lower(checkpoint) = 'd7'
              order by post_key, computed_at desc nulls last
            ),
            media as (
              select
                post_key,
                bool_or(
                  asset_role = 'video_full'
                  and status in ('active', 'purge_pending')
                  and coalesce(storage_path, public_url, '') <> ''
                ) as has_video_full,
                string_agg(distinct coalesce(status, ''), ', ' order by coalesce(status, '')) filter (where asset_role = 'video_full') as video_statuses
              from public.post_media_assets
              group by post_key
            )
            select
              p.post_key,
              p.post_url,
              p.posted_at,
              p.duration_seconds,
              lower(coalesce(p.media_type, '')) as media_type,
              left(coalesce(p.caption, ''), 160) as caption_preview,
              d7.views,
              d7.likes,
              d7.comments,
              coalesce(d7.percentile_performance_exact, d7.percentile_performance) as d7_percentile,
              coalesce(media.has_video_full, false) as has_video_full,
              coalesce(media.video_statuses, '') as video_statuses,
              pf.model_version as fingerprint_model_version,
              pf.media_confidence as fingerprint_media_confidence,
              pf.updated_at as fingerprint_updated_at,
              (
                pf.post_key is not null
                and pf.model_version = %s
                and pf.media_confidence = 'high'
              ) as has_latest_high_fingerprint
            from public.posts p
            join public.feeders f on f.id = p.feeder_id
            join latest_d7 d7 on d7.post_key = p.post_key
            left join media on media.post_key = p.post_key
            left join public.post_fingerprints pf on pf.post_key = p.post_key
            where lower(f.handle) = lower(%s)
              and lower(coalesce(p.media_type, '')) in ('reel', 'video')
            order by p.posted_at desc nulls last
            limit %s
            """,
            (model_version, handle, target),
        )
        return [dict(row) for row in cur.fetchall()]


def _summarize(rows_by_handle: dict[str, list[dict[str, Any]]], *, model_version: str) -> dict[str, Any]:
    handles = {}
    total_rows = 0
    total_latest = 0
    for handle, rows in rows_by_handle.items():
        latest = [row for row in rows if row.get("has_latest_high_fingerprint")]
        stale = [row for row in rows if row.get("fingerprint_model_version") and not row.get("has_latest_high_fingerprint")]
        missing = [row for row in rows if not row.get("fingerprint_model_version")]
        no_video = [row for row in rows if not row.get("has_video_full")]
        handles[handle] = {
            "target_rows": len(rows),
            "latest_high_fingerprints": len(latest),
            "stale_or_low_confidence": len(stale),
            "missing_fingerprint": len(missing),
            "without_video_full": len(no_video),
            "not_ready": [
                {
                    "post_key": row.get("post_key"),
                    "posted_at": row.get("posted_at"),
                    "has_video_full": row.get("has_video_full"),
                    "video_statuses": row.get("video_statuses"),
                    "fingerprint_model_version": row.get("fingerprint_model_version"),
                    "fingerprint_media_confidence": row.get("fingerprint_media_confidence"),
                }
                for row in rows
                if not row.get("has_latest_high_fingerprint")
            ],
        }
        total_rows += len(rows)
        total_latest += len(latest)
    return {
        "expected_model_version": model_version,
        "total_target_rows": total_rows,
        "total_latest_high_fingerprints": total_latest,
        "complete": total_rows == total_latest,
        "handles": handles,
    }


def _fill_missing(
    engine: Any,
    rows_by_handle: dict[str, list[dict[str, Any]]],
    *,
    prepare_media: bool,
    include_failed: bool,
    allow_private_refresh: bool,
    target_per_handle: int,
) -> list[dict[str, Any]]:
    from apps.worker.app.fingerprint_intelligence import ensure_post_fingerprint

    results: list[dict[str, Any]] = []
    conn = engine.conn
    for handle, rows in rows_by_handle.items():
        if prepare_media:
            try:
                media_result = engine.prepare_feeder_intelligence_media(
                    handle=handle,
                    limit=max(target_per_handle + 12, len(rows) + 12),
                    days=180,
                    batch_size=8,
                    include_failed=include_failed,
                    allow_private_refresh=allow_private_refresh,
                    dry_run=False,
                )
                print(json.dumps({"handle": handle, "media_prepare": media_result}, default=str), flush=True)
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                print(json.dumps({"handle": handle, "media_prepare_error": f"{type(exc).__name__}: {str(exc)[:300]}"}), flush=True)
        for row in rows:
            if row.get("has_latest_high_fingerprint"):
                continue
            post_key = str(row.get("post_key") or "")
            result = {
                "handle": handle,
                "post_key": post_key,
                "pre_model_version": row.get("fingerprint_model_version"),
                "pre_confidence": row.get("fingerprint_media_confidence"),
                "pre_has_video_full": row.get("has_video_full"),
            }
            try:
                if not row.get("has_video_full"):
                    repair = engine.repair_post_visual_media(post_key)
                    result["repair"] = {
                        key: repair.get(key)
                        for key in ("found", "staged", "captured", "failed", "retired")
                    }
                fingerprint = ensure_post_fingerprint(conn, post_key)
                result["fingerprint_ok"] = bool(fingerprint)
                if isinstance(fingerprint, dict):
                    status = fingerprint.get("fingerprint_status") if isinstance(fingerprint.get("fingerprint_status"), dict) else {}
                    result["post_model_version"] = status.get("model_version")
                    result["post_confidence"] = status.get("media_confidence") or fingerprint.get("media_confidence")
                try:
                    conn.commit()
                except Exception:
                    pass
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                result["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
            results.append(result)
            print(json.dumps(result, default=str), flush=True)
            time.sleep(0.5)
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-per-handle", type=int, default=10)
    parser.add_argument("--handles", nargs="+", default=list(DEFAULT_HANDLES))
    parser.add_argument("--fill", action="store_true", help="generate missing/stale latest-prompt fingerprints")
    parser.add_argument("--prepare-media", action="store_true", help="stage/capture video_full before fingerprinting")
    parser.add_argument("--include-failed", action="store_true", help="retry stale capture_failed media rows")
    parser.add_argument("--allow-private-refresh", action="store_true", help="refresh missing video URLs from BrightData post pages")
    args = parser.parse_args()

    _load_env()

    from apps.worker.app import config

    config.POSTGRES_DSN = os.environ["POSTGRES_DSN"]

    from apps.worker.app.fingerprint_intelligence import current_model_version
    from apps.worker.app.pure_engine import PureEngine

    model_version = current_model_version()
    engine = PureEngine()
    conn = engine.conn

    handles = tuple(str(handle).strip().lower() for handle in args.handles if str(handle).strip())

    rows_by_handle = {
        handle: _target_rows(conn, handle=handle, target=args.target_per_handle, model_version=model_version)
        for handle in handles
    }
    before = _summarize(rows_by_handle, model_version=model_version)
    print("before", json.dumps(before, default=str, indent=2), flush=True)

    fill_results: list[dict[str, Any]] = []
    if args.fill and not before["complete"]:
        fill_results = _fill_missing(
            engine,
            rows_by_handle,
            prepare_media=args.prepare_media,
            include_failed=args.include_failed,
            allow_private_refresh=args.allow_private_refresh,
            target_per_handle=args.target_per_handle,
        )
        rows_by_handle = {
            handle: _target_rows(conn, handle=handle, target=args.target_per_handle, model_version=model_version)
            for handle in handles
        }

    after = _summarize(rows_by_handle, model_version=model_version)
    print("after", json.dumps(after, default=str, indent=2), flush=True)
    out = {
        "before": before,
        "fill_results": fill_results,
        "after": after,
    }
    label = "_".join(handle.replace(".", "_") for handle in handles) or "cohort"
    out_path = ROOT / "apps" / "worker" / "scripts" / "out" / f"d7_latest_fingerprint_coverage_{label}.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, default=str, indent=2))
    print(json.dumps({"summary_path": str(out_path)}, default=str), flush=True)

    try:
        conn.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
