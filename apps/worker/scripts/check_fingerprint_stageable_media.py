"""Count posts whose stored R2 media can be staged into OpenRouter fingerprints.

Read-only. Mirrors the media requirements used by the mixed-media fingerprint
test: reels need video_full, images need display/thumbnail, carousels need
stored carousel_* assets.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[3]
WORKER_DIR = ROOT / "apps" / "worker"
OUT_DIR = WORKER_DIR / "scripts" / "out" / "fingerprint_stageable_media"

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - exercised in bare local shells.
    psycopg = None
    dict_row = None

ENV_PATHS = (WORKER_DIR / ".env", WORKER_DIR / ".env.vps-production")
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"


def _load_env() -> None:
    for env_path in ENV_PATHS:
        if not env_path.exists():
            continue
        for raw in env_path.read_text().splitlines():
            line = raw.strip()
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


def _handle_values(raw: str) -> list[str]:
    return [value.strip().lower() for value in raw.split(",") if value.strip()]


def _fetch_counts_sql(conn: Any, *, handles: list[str], days: int, sample_limit: int) -> dict[str, Any]:
    handle_filter = "and lower(f.handle) = any(%(handles)s)" if handles else ""
    days_filter = "and p.posted_at >= now() - (%(days)s::int * interval '1 day')" if days > 0 else ""
    params = {"handles": handles, "days": days, "sample_limit": sample_limit}
    rows = conn.execute(
        f"""
        with base as (
          select
            f.id as feeder_id,
            lower(f.handle) as handle,
            p.post_key,
            p.posted_at,
            case
              when lower(coalesce(p.media_type, '')) in ('reel', 'video') then 'reel'
              when lower(coalesce(p.media_type, '')) in ('sidecar', 'carousel', 'carousel_album') then 'carousel'
              when lower(coalesce(p.media_type, '')) in ('image', 'photo') then 'image'
              else lower(coalesce(nullif(p.media_type, ''), 'unknown'))
            end as media_lane
          from public.posts p
          join public.feeders f on f.id = p.feeder_id
          where true
            {handle_filter}
            {days_filter}
        ),
        media as (
          select
            a.post_key,
            count(*) filter (
              where lower(coalesce(a.storage_provider, 'supabase')) = 'r2'
                and a.status in ('active', 'purge_pending')
                and coalesce(a.storage_path, '') <> ''
            ) as r2_stored_assets,
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
            count(*) filter (
              where a.asset_role like 'carousel_%%'
            ) as carousel_assets,
            count(*) filter (
              where a.asset_role like 'carousel_%%'
                and lower(coalesce(a.storage_provider, 'supabase')) = 'r2'
                and a.status in ('active', 'purge_pending')
                and coalesce(a.storage_path, '') <> ''
            ) as r2_carousel,
            count(*) filter (
              where a.status in ('pending_capture', 'capture_failed', 'capturing')
            ) as capture_not_ready_assets
          from public.post_media_assets a
          group by a.post_key
        ),
        fingerprint as (
          select post_key, media_confidence
          from public.post_fingerprints
        ),
        classified as (
          select
            b.*,
            coalesce(m.r2_stored_assets, 0) as r2_stored_assets,
            coalesce(m.r2_video_full, 0) as r2_video_full,
            coalesce(m.r2_image, 0) as r2_image,
            coalesce(m.carousel_assets, 0) as carousel_assets,
            coalesce(m.r2_carousel, 0) as r2_carousel,
            coalesce(m.capture_not_ready_assets, 0) as capture_not_ready_assets,
            fp.post_key is not null as has_fingerprint,
            fp.media_confidence = 'high' as has_high_fingerprint,
            case
              when b.media_lane = 'reel' then coalesce(m.r2_video_full, 0) > 0
              when b.media_lane = 'image' then coalesce(m.r2_image, 0) > 0
              when b.media_lane = 'carousel' then coalesce(m.r2_carousel, 0) > 0
              else false
            end as openrouter_stageable_from_r2,
            case
              when b.media_lane = 'carousel' then coalesce(m.carousel_assets, 0) > 0 and coalesce(m.carousel_assets, 0) = coalesce(m.r2_carousel, 0)
              when b.media_lane = 'reel' then coalesce(m.r2_video_full, 0) > 0
              when b.media_lane = 'image' then coalesce(m.r2_image, 0) > 0
              else false
            end as complete_required_r2_media
          from base b
          left join media m on m.post_key = b.post_key
          left join fingerprint fp on fp.post_key = b.post_key
        ),
        grouped as (
          select
            handle,
            media_lane,
            count(*) as total_posts,
            count(*) filter (where r2_stored_assets > 0) as posts_with_any_r2_asset,
            count(*) filter (where openrouter_stageable_from_r2) as openrouter_stageable_from_r2,
            count(*) filter (where complete_required_r2_media) as complete_required_r2_media,
            count(*) filter (where has_fingerprint) as fingerprinted_posts,
            count(*) filter (where has_high_fingerprint) as high_confidence_fingerprints,
            count(*) filter (where not openrouter_stageable_from_r2 and capture_not_ready_assets > 0) as has_capture_backlog,
            max(posted_at) as latest_posted_at
          from classified
          group by handle, media_lane
        ),
        samples as (
          select handle, media_lane, post_key, posted_at, capture_not_ready_assets,
                 r2_video_full, r2_image, r2_carousel, carousel_assets
          from (
            select *,
                   row_number() over (
                     partition by handle, media_lane
                     order by posted_at desc nulls last, post_key
                   ) as rn
            from classified
            where not openrouter_stageable_from_r2
          ) s
          where rn <= %(sample_limit)s
        )
        select
          g.*,
          coalesce(
            jsonb_agg(
              jsonb_build_object(
                'post_key', s.post_key,
                'posted_at', s.posted_at,
                'capture_not_ready_assets', s.capture_not_ready_assets,
                'r2_video_full', s.r2_video_full,
                'r2_image', s.r2_image,
                'r2_carousel', s.r2_carousel,
                'carousel_assets', s.carousel_assets
              )
              order by s.posted_at desc nulls last
            ) filter (where s.post_key is not null),
            '[]'::jsonb
          ) as not_stageable_samples
        from grouped g
        left join samples s on s.handle = g.handle and s.media_lane = g.media_lane
        group by g.handle, g.media_lane, g.total_posts, g.posts_with_any_r2_asset,
                 g.openrouter_stageable_from_r2, g.complete_required_r2_media,
                 g.fingerprinted_posts, g.high_confidence_fingerprints,
                 g.has_capture_backlog, g.latest_posted_at
        order by g.handle, g.media_lane
        """,
        params,
    ).fetchall()

    by_handle: dict[str, Any] = {}
    totals = {
        "total_posts": 0,
        "posts_with_any_r2_asset": 0,
        "openrouter_stageable_from_r2": 0,
        "complete_required_r2_media": 0,
        "fingerprinted_posts": 0,
        "high_confidence_fingerprints": 0,
        "has_capture_backlog": 0,
    }
    for row in rows:
        item = dict(row)
        handle = str(item.pop("handle"))
        media_lane = str(item.pop("media_lane"))
        for key in totals:
            item[key] = int(item.get(key) or 0)
            totals[key] += item[key]
        by_handle.setdefault(handle, {})[media_lane] = item

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "postgres",
        "filters": {"handles": handles, "days": days or None},
        "totals": totals,
        "handles": by_handle,
    }


def _rest_rows(table: str, params: list[tuple[str, str]], *, order: str, page_size: int = 1000) -> list[dict[str, Any]]:
    base_url = os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "").rstrip("/")
    service_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
    if not base_url or not service_key:
        raise RuntimeError("REST fallback needs NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")

    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        query = urlencode([*params, ("order", order), ("limit", str(page_size)), ("offset", str(offset))])
        req = Request(
            f"{base_url}/rest/v1/{table}?{query}",
            headers={
                "apikey": service_key,
                "authorization": f"Bearer {service_key}",
                "accept": "application/json",
            },
        )
        try:
            with urlopen(req, timeout=30) as resp:
                page = json.loads(resp.read().decode("utf-8"))
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Supabase REST {table} failed: {exc.code} {body[:500]}") from exc
        if not isinstance(page, list):
            raise RuntimeError(f"Supabase REST {table} returned non-list response")
        rows.extend(page)
        if len(page) < page_size:
            return rows
        offset += page_size


def _media_lane(value: Any) -> str:
    media_type = str(value or "").strip().lower()
    if media_type in {"reel", "video"}:
        return "reel"
    if media_type in {"sidecar", "carousel", "carousel_album"}:
        return "carousel"
    if media_type in {"image", "photo"}:
        return "image"
    return media_type or "unknown"


def _posted_sort_key(row: dict[str, Any]) -> str:
    return str(row.get("posted_at") or "")


def _fetch_counts_rest(*, handles: list[str], days: int, sample_limit: int) -> dict[str, Any]:
    wanted_handles = set(handles)
    feeders = _rest_rows("feeders", [("select", "id,handle")], order="id.asc")
    feeder_by_id = {
        str(row.get("id")): str(row.get("handle") or "").lower()
        for row in feeders
        if row.get("id") is not None and str(row.get("handle") or "").strip()
    }
    if wanted_handles:
        feeder_by_id = {fid: handle for fid, handle in feeder_by_id.items() if handle in wanted_handles}

    post_params = [("select", "post_key,feeder_id,media_type,posted_at")]
    if days > 0:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        post_params.append(("posted_at", f"gte.{cutoff}"))
    posts = [
        row for row in _rest_rows("posts", post_params, order="post_key.asc")
        if str(row.get("feeder_id")) in feeder_by_id
    ]
    post_keys = {str(row.get("post_key") or "") for row in posts if row.get("post_key")}

    media: dict[str, dict[str, int]] = {}
    for asset in _rest_rows(
        "post_media_assets",
        [("select", "post_key,asset_role,storage_provider,storage_path,status")],
        order="id.asc",
    ):
        post_key = str(asset.get("post_key") or "")
        if post_key not in post_keys:
            continue
        bucket = media.setdefault(
            post_key,
            {
                "r2_stored_assets": 0,
                "r2_video_full": 0,
                "r2_image": 0,
                "carousel_assets": 0,
                "r2_carousel": 0,
                "capture_not_ready_assets": 0,
            },
        )
        role = str(asset.get("asset_role") or "").lower()
        status = str(asset.get("status") or "").lower()
        provider = str(asset.get("storage_provider") or "supabase").lower()
        has_path = bool(str(asset.get("storage_path") or "").strip())
        ready_r2 = provider == "r2" and status in {"active", "purge_pending"} and has_path
        if ready_r2:
            bucket["r2_stored_assets"] += 1
        if role == "video_full" and ready_r2:
            bucket["r2_video_full"] += 1
        if role in {"display", "thumbnail"} and ready_r2:
            bucket["r2_image"] += 1
        if role.startswith("carousel_"):
            bucket["carousel_assets"] += 1
            if ready_r2:
                bucket["r2_carousel"] += 1
        if status in {"pending_capture", "capture_failed", "capturing"}:
            bucket["capture_not_ready_assets"] += 1

    fingerprints = {
        str(row.get("post_key") or ""): str(row.get("media_confidence") or "")
        for row in _rest_rows("post_fingerprints", [("select", "post_key,media_confidence")], order="post_key.asc")
        if str(row.get("post_key") or "") in post_keys
    }

    classified = []
    for post in posts:
        post_key = str(post.get("post_key") or "")
        m = media.get(post_key, {})
        media_lane = _media_lane(post.get("media_type"))
        stageable = (
            (media_lane == "reel" and int(m.get("r2_video_full") or 0) > 0)
            or (media_lane == "image" and int(m.get("r2_image") or 0) > 0)
            or (media_lane == "carousel" and int(m.get("r2_carousel") or 0) > 0)
        )
        complete = (
            (media_lane == "carousel" and int(m.get("carousel_assets") or 0) > 0 and int(m.get("carousel_assets") or 0) == int(m.get("r2_carousel") or 0))
            or (media_lane == "reel" and int(m.get("r2_video_full") or 0) > 0)
            or (media_lane == "image" and int(m.get("r2_image") or 0) > 0)
        )
        classified.append({
            "handle": feeder_by_id[str(post.get("feeder_id"))],
            "media_lane": media_lane,
            "post_key": post_key,
            "posted_at": post.get("posted_at"),
            "r2_stored_assets": int(m.get("r2_stored_assets") or 0),
            "r2_video_full": int(m.get("r2_video_full") or 0),
            "r2_image": int(m.get("r2_image") or 0),
            "carousel_assets": int(m.get("carousel_assets") or 0),
            "r2_carousel": int(m.get("r2_carousel") or 0),
            "capture_not_ready_assets": int(m.get("capture_not_ready_assets") or 0),
            "has_fingerprint": post_key in fingerprints,
            "has_high_fingerprint": fingerprints.get(post_key) == "high",
            "openrouter_stageable_from_r2": stageable,
            "complete_required_r2_media": complete,
        })

    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    totals = {
        "total_posts": 0,
        "posts_with_any_r2_asset": 0,
        "openrouter_stageable_from_r2": 0,
        "complete_required_r2_media": 0,
        "fingerprinted_posts": 0,
        "high_confidence_fingerprints": 0,
        "has_capture_backlog": 0,
    }
    for item in classified:
        group = grouped.setdefault((item["handle"], item["media_lane"]), {
            "total_posts": 0,
            "posts_with_any_r2_asset": 0,
            "openrouter_stageable_from_r2": 0,
            "complete_required_r2_media": 0,
            "fingerprinted_posts": 0,
            "high_confidence_fingerprints": 0,
            "has_capture_backlog": 0,
            "latest_posted_at": None,
            "not_stageable_samples": [],
        })
        group["total_posts"] += 1
        group["posts_with_any_r2_asset"] += int(item["r2_stored_assets"] > 0)
        group["openrouter_stageable_from_r2"] += int(item["openrouter_stageable_from_r2"])
        group["complete_required_r2_media"] += int(item["complete_required_r2_media"])
        group["fingerprinted_posts"] += int(item["has_fingerprint"])
        group["high_confidence_fingerprints"] += int(item["has_high_fingerprint"])
        group["has_capture_backlog"] += int(not item["openrouter_stageable_from_r2"] and item["capture_not_ready_assets"] > 0)
        if not group["latest_posted_at"] or _posted_sort_key(item) > str(group["latest_posted_at"] or ""):
            group["latest_posted_at"] = item.get("posted_at")
        if not item["openrouter_stageable_from_r2"]:
            group["not_stageable_samples"].append({
                "post_key": item["post_key"],
                "posted_at": item["posted_at"],
                "capture_not_ready_assets": item["capture_not_ready_assets"],
                "r2_video_full": item["r2_video_full"],
                "r2_image": item["r2_image"],
                "r2_carousel": item["r2_carousel"],
                "carousel_assets": item["carousel_assets"],
            })

    by_handle: dict[str, Any] = {}
    for (handle, media_lane), group in sorted(grouped.items()):
        group["not_stageable_samples"] = sorted(group["not_stageable_samples"], key=_posted_sort_key, reverse=True)[:sample_limit]
        for key in totals:
            totals[key] += int(group[key])
        by_handle.setdefault(handle, {})[media_lane] = group

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "supabase_rest",
        "filters": {"handles": handles, "days": days or None},
        "totals": totals,
        "handles": by_handle,
    }


def run(args: argparse.Namespace) -> None:
    _load_env()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    handles = _handle_values(args.handles)
    if psycopg is not None:
        with psycopg.connect(os.environ["POSTGRES_DSN"], row_factory=dict_row, connect_timeout=20, autocommit=True) as conn:
            report = _fetch_counts_sql(conn, handles=handles, days=args.days, sample_limit=args.sample_limit)
    else:
        report = _fetch_counts_rest(handles=handles, days=args.days, sample_limit=args.sample_limit)

    out_path = Path(args.out) if args.out else OUT_DIR / f"stageable_media_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str))

    print(json.dumps({"path": str(out_path), **report["totals"]}, ensure_ascii=False, default=str))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handles", default="", help="Comma-separated handles. Defaults to all feeders.")
    parser.add_argument("--days", type=int, default=0, help="Only count posts from the last N days. Defaults to all time.")
    parser.add_argument("--sample-limit", type=int, default=5)
    parser.add_argument("--out", default="")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
