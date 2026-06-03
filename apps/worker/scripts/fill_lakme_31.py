from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ENV_PATH = ROOT / "apps" / "worker" / ".env"
HANDLE = "lakmeindia"
TARGET_V5_TOTAL = 31
BACKFILL_VIDEO_RETENTION_DAYS = 120


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


def _counts(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              count(*) filter (where pc.post_key is not null) as v5_condensations,
              count(*) filter (where pf.post_key is not null) as v10_fingerprints,
              count(*) filter (where vf.has_stored_video_full) as stored_video_full
            from public.posts p
            join public.feeders f on f.id = p.feeder_id
            join public.post_metrics pm
              on pm.post_key = p.post_key
             and lower(pm.checkpoint) = 'd7'
            left join public.post_condensations pc
              on pc.post_key = p.post_key
             and pc.condensation_version = 'post_condensation_v5_character_transfer'
            left join public.post_fingerprints pf
              on pf.post_key = p.post_key
             and pf.model_version like 'openrouter:google/gemini-3.5-flash:fingerprint_v10_duration_context%%'
             and pf.media_confidence = 'high'
            left join lateral (
              select true as has_stored_video_full
              from public.post_media_assets a
              where a.post_key = p.post_key
                and a.asset_role = 'video_full'
                and a.status in ('active', 'purge_pending')
                and coalesce(a.storage_path, a.public_url, '') <> ''
              limit 1
            ) vf on true
            where lower(f.handle) = lower(%s)
              and lower(coalesce(p.media_type, '')) in ('reel', 'video')
              and p.posted_at >= now() - interval '90 days'
            """,
            (HANDLE,),
        )
        row = cur.fetchone() or {}
    return dict(row)


def _candidate_rows(conn, *, include_failed: bool, limit: int) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            with latest_d7 as (
              select distinct on (post_key)
                post_key,
                views,
                computed_at
              from public.post_metrics
              where lower(checkpoint) = 'd7'
              order by post_key, computed_at desc nulls last
            ),
            base as (
              select
                p.post_key,
                p.post_url,
                p.posted_at,
                p.video_url,
                left(coalesce(p.caption, ''), 180) as caption,
                d7.views
              from public.posts p
              join public.feeders f on f.id = p.feeder_id
              join latest_d7 d7 on d7.post_key = p.post_key
              left join public.post_condensations pc
                on pc.post_key = p.post_key
               and pc.condensation_version = 'post_condensation_v5_character_transfer'
              where lower(f.handle) = lower(%s)
                and lower(coalesce(p.media_type, '')) in ('reel', 'video')
                and p.posted_at >= now() - interval '90 days'
                and pc.post_key is null
            ),
            vf as (
              select
                a.post_key,
                bool_or(a.status in ('active', 'purge_pending') and coalesce(a.storage_path, a.public_url, '') <> '') as has_stored_video_full,
                bool_or(a.status in ('active', 'purge_pending') and coalesce(a.storage_path, a.public_url, '') <> '' and lower(coalesce(a.storage_provider, 'supabase')) = 'supabase') as has_readable_stored_video_full,
                bool_or(coalesce(a.source_url, '') <> '') as has_source_url,
                bool_or(a.status = 'pending_capture' and coalesce(a.source_url, '') <> '') as has_pending_capture,
                bool_or(a.status = 'capture_failed' and coalesce(a.source_url, '') <> '') as has_capture_failed,
                string_agg(distinct coalesce(a.status, ''), ', ' order by coalesce(a.status, '')) as statuses
              from public.post_media_assets a
              join base b on b.post_key = a.post_key
              where a.asset_role = 'video_full'
              group by a.post_key
            ),
            fp as (
              select post_key, true as has_v10_fp
              from public.post_fingerprints
              where model_version like 'openrouter:google/gemini-3.5-flash:fingerprint_v10_duration_context%%'
                and media_confidence = 'high'
              group by post_key
            )
            select
              b.post_key,
              b.post_url,
              b.posted_at,
              b.video_url,
              b.views,
              b.caption,
              coalesce(vf.has_stored_video_full, false) as has_stored_video_full,
              coalesce(vf.has_readable_stored_video_full, false) as has_readable_stored_video_full,
              coalesce(vf.has_source_url, false) as has_source_url,
              coalesce(vf.has_pending_capture, false) as has_pending_capture,
              coalesce(vf.has_capture_failed, false) as has_capture_failed,
              coalesce(vf.statuses, 'none') as statuses,
              coalesce(fp.has_v10_fp, false) as has_v10_fp
            from base b
            left join vf on vf.post_key = b.post_key
            left join fp on fp.post_key = b.post_key
            where (
                coalesce(vf.has_stored_video_full, false)
                or coalesce(vf.has_pending_capture, false)
                or coalesce(b.video_url, '') <> ''
                or coalesce(vf.has_source_url, false)
              )
              and (
                %s
                or not coalesce(vf.has_capture_failed, false)
                or coalesce(vf.has_pending_capture, false)
                or coalesce(vf.has_stored_video_full, false)
              )
            order by
              case
                when coalesce(vf.has_stored_video_full, false) then 0
                when coalesce(vf.has_pending_capture, false) then 1
                when coalesce(vf.statuses, '') = '' or coalesce(vf.statuses, 'none') = 'none' then 2
                when coalesce(vf.has_capture_failed, false) then 3
                else 4
              end,
              b.posted_at desc nulls last
            limit %s
            """,
            (HANDLE, include_failed, max(1, limit)),
        )
        return [dict(row) for row in cur.fetchall()]


def _has_v5(conn, post_key: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            select exists (
              select 1
              from public.post_condensations
              where post_key = %s
                and condensation_version = 'post_condensation_v5_character_transfer'
            ) as ok
            """,
            (post_key,),
        )
        row = cur.fetchone() or {}
    return bool(row.get("ok"))


def _has_readable_video_full(conn, post_key: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            select exists (
              select 1
              from public.post_media_assets
              where post_key = %s
                and asset_role = 'video_full'
                and status in ('active', 'purge_pending')
                and coalesce(storage_path, public_url, '') <> ''
                and lower(coalesce(storage_provider, 'supabase')) = 'supabase'
            ) as ok
            """,
            (post_key,),
        )
        row = cur.fetchone() or {}
    return bool(row.get("ok"))


def _stage_video_full(engine, row: dict) -> bool:
    if row.get("has_readable_stored_video_full"):
        return False
    post_key = str(row.get("post_key") or "").strip().lower()
    video_url = str(row.get("video_url") or "").strip()
    if not post_key or not video_url:
        return False
    engine._stage_post_media_assets(
        post_key,
        row.get("posted_at"),
        None,
        video_url,
        None,
        full_video_retention_days=BACKFILL_VIDEO_RETENTION_DAYS,
        stage_preview=False,
    )
    return True


def _refresh_private_video_sources(engine, rows: list[dict], *, batch_size: int = 8) -> dict[str, int]:
    from apps.worker.app.instagram import canonical_post_url, shortcode_from_media_id, shortcode_from_url
    from apps.worker.app.pure_engine import _extract_media_refs, _post_key_from_url, run_actor_post_urls

    targets = [
        row
        for row in rows
        if str(row.get("post_key") or "").strip()
        and str(row.get("post_url") or "").strip()
        and not row.get("has_readable_stored_video_full")
    ]
    if not targets:
        return {"selected": 0, "updated": 0, "missing": 0}

    rows_by_post_key = {str(row.get("post_key") or "").strip().lower(): row for row in targets}
    urls_by_post_key = {
        str(row.get("post_key") or "").strip().lower(): str(row.get("post_url") or "").strip()
        for row in targets
    }
    updated = 0
    missing = 0
    ordered_keys = list(rows_by_post_key)
    for start in range(0, len(ordered_keys), max(1, batch_size)):
        chunk_keys = ordered_keys[start : start + max(1, batch_size)]
        chunk_urls = [urls_by_post_key[key] for key in chunk_keys]
        items = run_actor_post_urls("", chunk_urls, mode="reel")

        by_short: dict[str, dict] = {}
        by_post_key: dict[str, dict] = {}
        for item in items:
            source_url = str(item.get("url") or "").strip()
            provider_post_id = str(item.get("providerPostId") or item.get("postId") or "").strip()
            shortcode = (
                str(item.get("shortCode") or item.get("shortcode") or "").strip()
                or shortcode_from_media_id(provider_post_id)
                or shortcode_from_url(source_url)
            )
            if shortcode:
                by_short[shortcode.lower()] = item
            canonical = canonical_post_url(shortcode, source_url) or source_url
            item_post_key = _post_key_from_url(canonical)
            if item_post_key:
                by_post_key[item_post_key] = item

        for post_key in chunk_keys:
            row = rows_by_post_key[post_key]
            post_url = str(row.get("post_url") or "").strip()
            short = shortcode_from_url(post_url).lower()
            item = by_post_key.get(post_key) or (by_short.get(short) if short else None)
            if not item:
                missing += 1
                continue
            thumbnail_url, video_url, carousel_urls = _extract_media_refs(item)
            if not video_url:
                missing += 1
                continue
            engine._refresh_post_media(post_key, thumbnail_url, video_url, carousel_urls)
            row["video_url"] = video_url
            updated += 1
    engine.conn.commit()
    return {"selected": len(targets), "updated": updated, "missing": missing}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", action="store_true", help="actually capture media and run fingerprint/condensation")
    parser.add_argument("--target", type=int, default=TARGET_V5_TOTAL)
    parser.add_argument("--batch", type=int, default=10)
    parser.add_argument("--include-failed", action="store_true", help="allow stale capture_failed rows after cleaner sources are exhausted")
    args = parser.parse_args()

    _load_env()
    from apps.worker.app import config

    config.POSTGRES_DSN = os.environ["POSTGRES_DSN"]

    from apps.worker.app.fingerprint_intelligence import ensure_post_condensation, ensure_post_fingerprint
    from apps.worker.app.pure_engine import PureEngine

    engine = PureEngine()
    conn = engine.conn

    start_counts = _counts(conn)
    needed = max(0, int(args.target) - int(start_counts.get("v5_condensations") or 0))
    candidates = _candidate_rows(conn, include_failed=args.include_failed, limit=max(args.batch, needed + 5))
    print(json.dumps({
        "mode": "run" if args.run else "dry_run",
        "target": args.target,
        "start_counts": start_counts,
        "needed": needed,
        "candidate_count": len(candidates),
        "candidates": [
            {
                "post_key": row.get("post_key"),
                "posted_at": row.get("posted_at"),
                "views": row.get("views"),
                "has_stored_video_full": row.get("has_stored_video_full"),
                "has_readable_stored_video_full": row.get("has_readable_stored_video_full"),
                "has_pending_capture": row.get("has_pending_capture"),
                "has_capture_failed": row.get("has_capture_failed"),
                "statuses": row.get("statuses"),
                "has_v10_fp": row.get("has_v10_fp"),
                "caption": row.get("caption"),
            }
            for row in candidates[: max(args.batch, needed)]
        ],
    }, default=str, indent=2), flush=True)
    if not args.run or needed <= 0:
        return

    processed: list[dict] = []
    skipped_keys: set[str] = set()
    while int(_counts(conn).get("v5_condensations") or 0) < int(args.target):
        current_counts = _counts(conn)
        still_needed = max(0, int(args.target) - int(current_counts.get("v5_condensations") or 0))
        media_result = engine.prepare_feeder_intelligence_media(
            handle=HANDLE,
            limit=max(args.batch * 2, still_needed + 6),
            days=90,
            batch_size=max(1, min(args.batch, 8)),
            include_failed=args.include_failed,
            allow_private_refresh=False,
            dry_run=False,
        )
        print(json.dumps({"media_ready": media_result}, default=str), flush=True)
        rows = [
            row
            for row in _candidate_rows(conn, include_failed=args.include_failed, limit=max(args.batch * 2, still_needed + 6))
            if str(row.get("post_key") or "") not in skipped_keys
        ][: max(1, min(args.batch, still_needed + 3))]
        if not rows:
            break

        for row in rows:
            post_key = str(row.get("post_key") or "").strip()
            if not post_key or _has_v5(conn, post_key):
                continue
            result = {
                "post_key": post_key,
                "media_ready_batch": media_result,
                "readable_video_full": _has_readable_video_full(conn, post_key),
            }
            if not result["readable_video_full"]:
                result["skipped"] = "no_readable_video_full_after_capture"
                skipped_keys.add(post_key)
                processed.append(result)
                print(json.dumps(result, default=str), flush=True)
                continue
            try:
                fingerprint = ensure_post_fingerprint(conn, post_key)
                result["fingerprint_ok"] = bool(fingerprint)
                if fingerprint:
                    condensation = ensure_post_condensation(conn, post_key, fingerprint)
                    result["condensation_ok"] = bool(condensation)
                else:
                    result["condensation_ok"] = False
                conn.commit()
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                result["error"] = f"{type(exc).__name__}: {str(exc)[:240]}"
                skipped_keys.add(post_key)
            processed.append(result)
            print(json.dumps(result, default=str), flush=True)
            if int(_counts(conn).get("v5_condensations") or 0) >= int(args.target):
                break

    print(json.dumps({
        "done": True,
        "start_counts": start_counts,
        "end_counts": _counts(conn),
        "processed": processed,
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }, default=str, indent=2), flush=True)


if __name__ == "__main__":
    main()
