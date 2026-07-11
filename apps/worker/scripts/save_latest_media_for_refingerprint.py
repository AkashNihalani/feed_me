"""Save latest post media so posts can be refingerprinted later."""
from __future__ import annotations

import argparse
import json
import os
import sys
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


def _lane(media_type: str) -> str:
    value = (media_type or "").lower()
    if value in {"reel", "video"}:
        return "reel"
    if value in {"sidecar", "carousel", "carousel_album"}:
        return "carousel"
    if value in {"image", "photo"}:
        return "image"
    return "other"


def _fetch_latest(conn: Any, limit: int, handles: set[str]) -> list[dict[str, Any]]:
    handle_filter = "and lower(f.handle) = any(%s)" if handles else ""
    params: list[Any] = [sorted(handles)] if handles else []
    query = f"""
        with ranked as (
          select
            f.handle,
            p.post_key,
            p.post_url,
            p.posted_at,
            lower(coalesce(p.media_type, '')) as media_type,
            p.caption,
            p.duration_seconds,
            p.thumbnail_url,
            p.video_url,
            p.carousel_urls,
            row_number() over (
              partition by f.id
              order by p.posted_at desc nulls last, p.updated_at desc nulls last
            ) as rn
          from public.posts p
          join public.feeders f on f.id = p.feeder_id
          where coalesce(p.post_url, '') like 'http%%'
            and lower(coalesce(p.media_type, '')) in (
              'reel', 'video', 'sidecar', 'carousel', 'carousel_album', 'image', 'photo'
            )
            {handle_filter}
        )
        select *
        from ranked
        where rn <= %s
        order by lower(handle), rn
    """
    params.append(limit)
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _ready_map(conn: Any, post_keys: list[str]) -> dict[str, dict[str, Any]]:
    if not post_keys:
        return {}
    rows = conn.execute(
        """
        select
          post_key,
          count(*) filter (
            where asset_role = 'video_full'
              and status in ('active', 'purge_pending')
              and coalesce(storage_path, public_url, '') <> ''
          ) as ready_video_full,
          count(*) filter (
            where asset_role in ('display', 'thumbnail')
              and status in ('active', 'purge_pending')
              and coalesce(storage_path, public_url, '') <> ''
          ) as ready_image,
          count(*) filter (where asset_role like 'carousel_%%') as staged_carousel,
          count(*) filter (
            where asset_role like 'carousel_%%'
              and status in ('active', 'purge_pending')
              and coalesce(storage_path, public_url, '') <> ''
          ) as ready_carousel,
          count(*) filter (
            where asset_role like 'carousel_%%'
              and not (
                status in ('active', 'purge_pending')
                and coalesce(storage_path, public_url, '') <> ''
              )
          ) as not_ready_carousel
        from public.post_media_assets
        where post_key = any(%s)
        group by post_key
        """,
        (post_keys,),
    ).fetchall()
    return {str(row["post_key"]): dict(row) for row in rows}


def _is_ready(row: dict[str, Any], ready: dict[str, Any]) -> bool:
    lane = _lane(str(row.get("media_type") or ""))
    if lane == "reel":
        return int(ready.get("ready_video_full") or 0) >= 1
    if lane == "image":
        return int(ready.get("ready_image") or 0) >= 1
    if lane == "carousel":
        staged = int(ready.get("staged_carousel") or 0)
        ready_count = int(ready.get("ready_carousel") or 0)
        not_ready = int(ready.get("not_ready_carousel") or 0)
        return staged > 0 and ready_count >= staged and not_ready == 0
    return False


def _media_refs(row: dict[str, Any], item: dict[str, Any] | None) -> tuple[str | None, str | None, list[str]]:
    from app.pure_engine import _extract_media_refs

    if item:
        thumbnail_url, video_url, carousel_urls = _extract_media_refs(item)
    else:
        thumbnail_url = row.get("thumbnail_url")
        video_url = row.get("video_url")
        carousel_urls = row.get("carousel_urls") if isinstance(row.get("carousel_urls"), list) else []
    lane = _lane(str(row.get("media_type") or ""))
    if lane != "reel":
        video_url = None
    if lane != "carousel":
        carousel_urls = []
    return thumbnail_url, video_url, carousel_urls


def _save_one(engine: Any, row: dict[str, Any], *, refresh: bool) -> dict[str, Any]:
    from app.brightdata import run_post_urls

    post_key = str(row["post_key"]).strip().lower()
    item = None
    if refresh:
        items = run_post_urls([str(row["post_url"])])
        item = items[0] if items else None
    thumbnail_url, video_url, carousel_urls = _media_refs(row, item)
    lane = _lane(str(row.get("media_type") or ""))
    if lane == "reel" and not video_url:
        return {"status": "no_video_url"}
    if lane == "image" and not thumbnail_url:
        return {"status": "no_image_url"}
    if lane == "carousel" and not carousel_urls:
        return {"status": "no_carousel_urls"}

    engine._refresh_post_media(post_key, thumbnail_url, video_url, carousel_urls)
    engine._stage_post_media_assets(
        post_key,
        row.get("posted_at"),
        thumbnail_url,
        video_url,
        carousel_urls,
        thumbnail_retention_days=RETENTION_DAYS if thumbnail_url else None,
        full_video_retention_days=RETENTION_DAYS if video_url else None,
        carousel_retention_days=RETENTION_DAYS if carousel_urls else None,
        stage_preview=False,
    )
    engine.conn.commit()
    result = engine._capture_post_media_assets_for_post_keys([post_key], include_all_roles=True, stale_minutes=0)
    return {"status": "captured", **result}


def run(args: argparse.Namespace) -> None:
    _load_env()
    from app.pure_engine import PureEngine

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    handles = {value.strip().lower() for value in args.handles.split(",") if value.strip()}
    log_path = OUT_DIR / args.log_name
    summary: dict[str, Any] = {
        "target_per_feeder": args.limit,
        "selected": 0,
        "already_ready": 0,
        "attempted": 0,
        "ready_after": 0,
        "failed": 0,
        "by_handle": {},
    }

    with psycopg.connect(os.environ["POSTGRES_DSN"], row_factory=dict_row, connect_timeout=20, autocommit=True) as conn:
        rows = _fetch_latest(conn, args.limit, handles)
        ready = _ready_map(conn, [str(row["post_key"]) for row in rows])
    summary["selected"] = len(rows)

    engine = PureEngine()
    try:
        with log_path.open("a") as log:
            for index, row in enumerate(rows, start=1):
                handle = str(row["handle"])
                lane = _lane(str(row.get("media_type") or ""))
                bucket = summary["by_handle"].setdefault(handle, {"selected": 0, "already_ready": 0, "attempted": 0, "ready_after": 0, "failed": 0})
                bucket["selected"] += 1
                post_key = str(row["post_key"])
                if _is_ready(row, ready.get(post_key, {})):
                    summary["already_ready"] += 1
                    bucket["already_ready"] += 1
                    continue
                if args.max_attempts and summary["attempted"] >= args.max_attempts:
                    break
                status: dict[str, Any] = {"handle": handle, "post_key": post_key, "lane": lane, "index": index}
                try:
                    result = _save_one(engine, row, refresh=not args.no_brightdata)
                    status.update(result)
                except Exception as exc:
                    status.update({"status": "error", "error": str(exc)[:500]})

                with psycopg.connect(os.environ["POSTGRES_DSN"], row_factory=dict_row, connect_timeout=20, autocommit=True) as check_conn:
                    now_ready = _ready_map(check_conn, [post_key]).get(post_key, {})
                ok = _is_ready(row, now_ready)
                status["ready_after"] = ok
                summary["attempted"] += 1
                bucket["attempted"] += 1
                if ok:
                    summary["ready_after"] += 1
                    bucket["ready_after"] += 1
                else:
                    summary["failed"] += 1
                    bucket["failed"] += 1
                log.write(json.dumps(status, ensure_ascii=False, default=str) + "\n")
                log.flush()
                print(json.dumps(status, ensure_ascii=False, default=str), flush=True)
    finally:
        engine.close()

    summary_path = OUT_DIR / args.summary_name
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    print(json.dumps({"stage": "summary", "path": str(summary_path), **summary}, ensure_ascii=False, default=str))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=60)
    parser.add_argument("--handles", default="")
    parser.add_argument("--max-attempts", type=int, default=0)
    parser.add_argument("--no-brightdata", action="store_true")
    parser.add_argument("--log-name", default="save_latest_media_for_refingerprint.jsonl")
    parser.add_argument("--summary-name", default="save_latest_media_for_refingerprint_summary.json")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
