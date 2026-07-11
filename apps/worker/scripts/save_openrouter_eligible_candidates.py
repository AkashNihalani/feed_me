"""Save scraped media assets so posts become OpenRouter-eligible.

No OpenRouter calls. This only stages/captures media into the configured media
storage and checks whether each post now has the required stored asset.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
WORKER_DIR = ROOT / "apps" / "worker"
OUT_DIR = WORKER_DIR / "scripts" / "out" / "media_save"

if str(WORKER_DIR) not in sys.path:
    sys.path.insert(0, str(WORKER_DIR))

from scripts.run_mixed_media_fingerprint_test import _load_env  # noqa: E402

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402


RETENTION_DAYS = 120


def _lane(value: Any) -> str:
    media_type = str(value or "").strip().lower()
    if media_type in {"reel", "video"}:
        return "reel"
    if media_type in {"sidecar", "carousel", "carousel_album"}:
        return "carousel"
    if media_type in {"image", "photo"}:
        return "image"
    return media_type or "unknown"


def _carousel_urls(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item or "").strip()]
    return []


def _is_ready(row: dict[str, Any]) -> bool:
    lane = str(row.get("media_lane") or "")
    if lane == "reel":
        return int(row.get("r2_video_full") or 0) > 0
    if lane == "image":
        return int(row.get("r2_image") or 0) > 0
    if lane == "carousel":
        carousel_assets = int(row.get("carousel_assets") or 0)
        return carousel_assets > 0 and carousel_assets == int(row.get("r2_carousel") or 0)
    return False


def _ready_rows(conn: Any, post_keys: list[str]) -> dict[str, dict[str, Any]]:
    if not post_keys:
        return {}
    rows = conn.execute(
        """
        select
          a.post_key,
          count(*) filter (
            where a.asset_role = 'video_full'
              and lower(coalesce(a.storage_provider, 'supabase')) = 'r2'
              and a.status in ('active', 'purge_pending')
              and coalesce(a.storage_path, '') <> ''
          ) as r2_video_full,
          count(*) filter (
            where a.asset_role in ('display', 'thumbnail')
              and lower(coalesce(a.storage_provider, 'supabase')) = 'r2'
              and a.status in ('active', 'purge_pending')
              and coalesce(a.storage_path, '') <> ''
          ) as r2_image,
          count(*) filter (where a.asset_role like 'carousel_%%') as carousel_assets,
          count(*) filter (
            where a.asset_role like 'carousel_%%'
              and lower(coalesce(a.storage_provider, 'supabase')) = 'r2'
              and a.status in ('active', 'purge_pending')
              and coalesce(a.storage_path, '') <> ''
          ) as r2_carousel
        from public.post_media_assets a
        where a.post_key = any(%s)
        group by a.post_key
        """,
        (post_keys,),
    ).fetchall()
    return {str(row["post_key"]): dict(row) for row in rows}


def _fetch_candidates(conn: Any, *, days: int, created_days: int, handles: list[str], limit: int) -> list[dict[str, Any]]:
    posted_filter = "and p.posted_at >= now() - (%(days)s::int * interval '1 day')" if days > 0 else ""
    created_filter = "and p.created_at >= now() - (%(created_days)s::int * interval '1 day')" if created_days > 0 else ""
    handle_filter = "and lower(f.handle) = any(%(handles)s)" if handles else ""
    limit_clause = "limit %(limit)s" if limit > 0 else ""
    return [dict(row) for row in conn.execute(
        f"""
        with base as (
          select
            lower(f.handle) as handle,
            p.post_key,
            p.created_at,
            p.posted_at,
            lower(coalesce(p.media_type, '')) as media_type,
            case
              when lower(coalesce(p.media_type, '')) in ('reel', 'video') then 'reel'
              when lower(coalesce(p.media_type, '')) in ('sidecar', 'carousel', 'carousel_album') then 'carousel'
              when lower(coalesce(p.media_type, '')) in ('image', 'photo') then 'image'
              else lower(coalesce(nullif(p.media_type, ''), 'unknown'))
            end as media_lane,
            p.thumbnail_url,
            p.video_url,
            coalesce(p.carousel_urls, '[]'::jsonb) as carousel_urls
          from public.posts p
          join public.feeders f on f.id = p.feeder_id
          where true
            {posted_filter}
            {created_filter}
            {handle_filter}
        ),
        media as (
          select
            a.post_key,
            count(*) filter (
              where a.asset_role = 'video_full'
                and lower(coalesce(a.storage_provider, 'supabase')) = 'r2'
                and a.status in ('active', 'purge_pending')
                and coalesce(a.storage_path, '') <> ''
            ) as r2_video_full,
            count(*) filter (
              where a.asset_role in ('display', 'thumbnail')
                and lower(coalesce(a.storage_provider, 'supabase')) = 'r2'
                and a.status in ('active', 'purge_pending')
                and coalesce(a.storage_path, '') <> ''
            ) as r2_image,
            count(*) filter (where a.asset_role like 'carousel_%%') as carousel_assets,
            count(*) filter (
              where a.asset_role like 'carousel_%%'
                and lower(coalesce(a.storage_provider, 'supabase')) = 'r2'
                and a.status in ('active', 'purge_pending')
                and coalesce(a.storage_path, '') <> ''
            ) as r2_carousel,
            count(*) filter (
              where a.asset_role = 'video_full'
                and a.status in ('pending_capture', 'capture_failed', 'capturing')
                and coalesce(a.source_url, '') <> ''
            ) as source_video_full,
            count(*) filter (
              where a.asset_role in ('display', 'thumbnail')
                and a.status in ('pending_capture', 'capture_failed', 'capturing')
                and coalesce(a.source_url, '') <> ''
            ) as source_image,
            count(*) filter (
              where a.asset_role like 'carousel_%%'
                and a.status in ('pending_capture', 'capture_failed', 'capturing')
                and coalesce(a.source_url, '') <> ''
            ) as source_carousel
          from public.post_media_assets a
          group by a.post_key
        ),
        classified as (
          select
            b.*,
            coalesce(m.r2_video_full, 0) as r2_video_full,
            coalesce(m.r2_image, 0) as r2_image,
            coalesce(m.carousel_assets, 0) as carousel_assets,
            coalesce(m.r2_carousel, 0) as r2_carousel,
            case
              when b.media_lane = 'reel' then coalesce(m.r2_video_full, 0) > 0
              when b.media_lane = 'image' then coalesce(m.r2_image, 0) > 0
              when b.media_lane = 'carousel' then coalesce(m.carousel_assets, 0) > 0 and coalesce(m.carousel_assets, 0) = coalesce(m.r2_carousel, 0)
              else false
            end as openrouter_ready_now,
            case
              when b.media_lane = 'reel' then coalesce(m.source_video_full, 0) > 0 or coalesce(b.video_url, '') <> ''
              when b.media_lane = 'image' then coalesce(m.source_image, 0) > 0 or coalesce(b.thumbnail_url, '') <> ''
              when b.media_lane = 'carousel' then coalesce(m.source_carousel, 0) > 0 or jsonb_array_length(b.carousel_urls) > 0
              else false
            end as has_save_source
          from base b
          left join media m on m.post_key = b.post_key
        )
        select *
        from classified
        where not openrouter_ready_now
          and has_save_source
        order by created_at desc nulls last, posted_at desc nulls last, handle, media_lane, post_key
        {limit_clause}
        """,
        {"days": days, "created_days": created_days, "handles": handles, "limit": limit},
    ).fetchall()]


def _stage_post_level_sources(engine: Any, row: dict[str, Any]) -> None:
    lane = str(row["media_lane"])
    thumbnail_url = str(row.get("thumbnail_url") or "").strip() or None
    video_url = str(row.get("video_url") or "").strip() or None
    carousel_urls = _carousel_urls(row.get("carousel_urls"))
    if lane != "reel":
        video_url = None
    if lane != "carousel":
        carousel_urls = []
    engine._stage_post_media_assets(
        str(row["post_key"]).strip().lower(),
        row.get("posted_at"),
        thumbnail_url,
        video_url,
        carousel_urls,
        thumbnail_retention_days=RETENTION_DAYS if thumbnail_url and lane == "image" else None,
        full_video_retention_days=RETENTION_DAYS if video_url and lane == "reel" else None,
        carousel_retention_days=RETENTION_DAYS if carousel_urls and lane == "carousel" else None,
        stage_preview=False,
    )


def run(args: argparse.Namespace) -> None:
    _load_env()
    from app.pure_engine import PureEngine

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    handles = [value.strip().lower() for value in args.handles.split(",") if value.strip()]
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = OUT_DIR / f"save_openrouter_eligible_{run_id}.jsonl"
    summary_path = OUT_DIR / f"save_openrouter_eligible_{run_id}_summary.json"

    with psycopg.connect(os.environ["POSTGRES_DSN"], row_factory=dict_row, connect_timeout=20, autocommit=True) as conn:
        rows = _fetch_candidates(conn, days=args.days, created_days=args.created_days, handles=handles, limit=args.limit)

    summary: dict[str, Any] = {
        "days": args.days or None,
        "created_days": args.created_days or None,
        "handles": handles,
        "selected": len(rows),
        "attempted": 0,
        "ready_after": 0,
        "failed": 0,
        "by_handle": {},
        "by_media_type": {},
        "log_path": str(log_path),
    }

    engine = PureEngine()
    try:
        with log_path.open("a") as log:
            for index, row in enumerate(rows, start=1):
                handle = str(row["handle"])
                lane = str(row["media_lane"])
                for bucket in (
                    summary["by_handle"].setdefault(handle, {"attempted": 0, "ready_after": 0, "failed": 0}),
                    summary["by_media_type"].setdefault(lane, {"attempted": 0, "ready_after": 0, "failed": 0}),
                ):
                    bucket["attempted"] += 1
                status: dict[str, Any] = {"index": index, "total": len(rows), "handle": handle, "media_type": lane, "post_key": row["post_key"]}
                try:
                    with engine.conn.transaction():
                        _stage_post_level_sources(engine, row)
                    result = engine._capture_post_media_assets_for_post_keys([str(row["post_key"])], include_all_roles=True, stale_minutes=0)
                    status.update({"capture": result})
                except Exception as exc:
                    status.update({"error": str(exc)[:500]})

                with psycopg.connect(os.environ["POSTGRES_DSN"], row_factory=dict_row, connect_timeout=20, autocommit=True) as check_conn:
                    ready = _ready_rows(check_conn, [str(row["post_key"])]).get(str(row["post_key"]), {})
                ready.update({"media_lane": lane})
                ok = _is_ready(ready)
                status["openrouter_ready_after"] = ok
                summary["attempted"] += 1
                if ok:
                    summary["ready_after"] += 1
                    summary["by_handle"][handle]["ready_after"] += 1
                    summary["by_media_type"][lane]["ready_after"] += 1
                else:
                    summary["failed"] += 1
                    summary["by_handle"][handle]["failed"] += 1
                    summary["by_media_type"][lane]["failed"] += 1

                log.write(json.dumps(status, ensure_ascii=False, default=str) + "\n")
                log.flush()
                print(json.dumps(status, ensure_ascii=False, default=str), flush=True)
    finally:
        engine.close()

    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    print(json.dumps({"summary_path": str(summary_path), **summary}, ensure_ascii=False, default=str))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=120, help="Posted-at window. Use 0 for all time.")
    parser.add_argument("--created-days", type=int, default=0, help="Created-at/scraped window. Use 0 to ignore.")
    parser.add_argument("--handles", default="")
    parser.add_argument("--limit", type=int, default=0)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
