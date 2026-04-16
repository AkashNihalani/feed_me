from __future__ import annotations

import argparse
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.instagram import canonical_post_url, shortcode_from_media_id, shortcode_from_url
from app.pure_engine import _extract_media_refs, _post_key_from_url, _preview_extension_deadline, _to_dt
from app.scraper import run_actor_post_urls


DEFAULT_LIMIT = 250
DEFAULT_BATCH_SIZE = 25
DEFAULT_STALE_MINUTES = 30
DEFAULT_VERIFY_TIMEOUT_SECONDS = 900
DEFAULT_VERIFY_INTERVAL_SECONDS = 12


def _env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        value = value[1:-1]
    return value


SUPABASE_URL = _env("NEXT_PUBLIC_SUPABASE_URL").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = _env("SUPABASE_SERVICE_ROLE_KEY")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _rest_headers(*, prefer: str | None = None) -> dict[str, str]:
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if prefer:
        headers["Prefer"] = prefer
    return headers


def _rest_url(table: str) -> str:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise RuntimeError("Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
    return f"{SUPABASE_URL}/rest/v1/{table}"


def _chunked(items: list[str], size: int) -> list[list[str]]:
    chunk_size = max(1, size)
    return [items[idx: idx + chunk_size] for idx in range(0, len(items), chunk_size)]


def _get_all(table: str, params: dict[str, str], page_size: int = 1000) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        page_params = dict(params)
        page_params["limit"] = str(page_size)
        page_params["offset"] = str(offset)
        resp = requests.get(_rest_url(table), headers=_rest_headers(), params=page_params, timeout=60)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        rows.extend(batch)
        offset += len(batch)
        if len(batch) < page_size:
            break
    return rows


def _fetch_candidate_posts(cutoff_day: str, limit: int, stale_minutes: int) -> list[dict[str, Any]]:
    metric_rows = _get_all(
        "post_metrics",
        {
            "select": "post_key,business_date_ist,computed_at,checkpoint",
            "business_date_ist": f"gte.{cutoff_day}",
            "checkpoint": "in.(d1,d3,d7,d21)",
            "order": "business_date_ist.desc,computed_at.desc",
        },
    )
    if not metric_rows:
        return []

    post_keys: list[str] = []
    seen_keys: set[str] = set()
    for row in metric_rows:
        post_key = str(row.get("post_key") or "").strip().lower()
        if post_key and post_key not in seen_keys:
            seen_keys.add(post_key)
            post_keys.append(post_key)

    posts: dict[str, dict[str, Any]] = {}
    for chunk in _chunked(post_keys, 100):
        joined = ",".join(chunk)
        post_rows = requests.get(
            _rest_url("posts"),
            headers=_rest_headers(),
            params={
                "select": "post_key,post_url,posted_at,thumbnail_url,video_url,carousel_urls,media_type",
                "post_key": f"in.({joined})",
            },
            timeout=60,
        )
        post_rows.raise_for_status()
        for row in post_rows.json():
            key = str(row.get("post_key") or "").strip().lower()
            if key:
                posts[key] = row

    assets: dict[str, dict[str, Any]] = {}
    for chunk in _chunked(post_keys, 100):
        joined = ",".join(chunk)
        asset_rows = requests.get(
            _rest_url("post_media_assets"),
            headers=_rest_headers(),
            params={
                "select": "id,post_key,status,source_url,storage_provider,storage_bucket,storage_path,public_url,updated_at",
                "asset_role": "eq.preview_5s",
                "post_key": f"in.({joined})",
            },
            timeout=60,
        )
        asset_rows.raise_for_status()
        for row in asset_rows.json():
            key = str(row.get("post_key") or "").strip().lower()
            if key:
                assets[key] = row

    target_row = requests.get(
        _rest_url("post_media_assets"),
        headers=_rest_headers(),
        params={
            "select": "storage_provider,storage_bucket",
            "asset_role": "eq.preview_5s",
            "status": "eq.active",
            "storage_path": "not.is.null",
            "order": "updated_at.desc,id.desc",
            "limit": "1",
        },
        timeout=60,
    )
    target_row.raise_for_status()
    target_rows = target_row.json()
    inferred_provider = "r2"
    inferred_bucket = "feedmemedia"
    if target_rows:
        inferred_provider = str(target_rows[0].get("storage_provider") or inferred_provider).strip().lower() or inferred_provider
        inferred_bucket = str(target_rows[0].get("storage_bucket") or inferred_bucket).strip() or inferred_bucket

    candidates: list[dict[str, Any]] = []
    stale_before = datetime.now(timezone.utc) - timedelta(minutes=max(1, stale_minutes))
    for post_key in post_keys:
        post = posts.get(post_key)
        if not post:
            continue
        if str(post.get("media_type") or "").strip().lower() != "reel":
            continue
        if not str(post.get("post_url") or "").strip():
            continue

        asset = assets.get(post_key)
        if asset:
            status = str(asset.get("status") or "").strip().lower()
            updated_at = _to_dt(asset.get("updated_at"))
            is_stale_capture = status == "capturing" and (updated_at is None or updated_at <= stale_before)
            has_active = status == "active" and str(asset.get("storage_path") or "").strip() and (
                str(asset.get("storage_provider") or "").strip().lower() != "r2" or str(asset.get("public_url") or "").strip()
            )
            if has_active:
                continue
            if status not in ("capture_failed", "capturing", "pending_capture", "purge_failed", "purge_pending") and not is_stale_capture:
                continue
        candidates.append(
            {
                "post": post,
                "asset": asset,
                "storage_provider": str((asset or {}).get("storage_provider") or inferred_provider).strip().lower() or inferred_provider,
                "storage_bucket": str((asset or {}).get("storage_bucket") or inferred_bucket).strip() or inferred_bucket,
            }
        )
        if len(candidates) >= max(1, limit):
            break

    return candidates


def _patch_row(table: str, match: dict[str, str], payload: dict[str, Any]) -> None:
    resp = requests.patch(
        _rest_url(table),
        headers=_rest_headers(prefer="return=minimal"),
        params=match,
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()


def _insert_row(table: str, payload: dict[str, Any]) -> None:
    resp = requests.post(
        _rest_url(table),
        headers=_rest_headers(prefer="return=minimal"),
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()


def _build_item_maps(items: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_short: dict[str, dict[str, Any]] = {}
    by_post_key: dict[str, dict[str, Any]] = {}
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
    return by_short, by_post_key


def _restage_candidate(candidate: dict[str, Any], item: dict[str, Any] | None) -> bool:
    post = candidate["post"]
    asset = candidate.get("asset") or {}
    post_key = str(post.get("post_key") or "").strip().lower()
    fallback_thumbnail_url = str(post.get("thumbnail_url") or "").strip() or None
    fallback_video_url = str(post.get("video_url") or "").strip() or None
    raw_carousel_urls = post.get("carousel_urls")
    if isinstance(raw_carousel_urls, list):
        fallback_carousel_urls = [str(value).strip() for value in raw_carousel_urls if str(value).strip()]
    else:
        fallback_carousel_urls = []

    if item:
        thumbnail_url, video_url, carousel_urls = _extract_media_refs(item)
        thumbnail_url = thumbnail_url or fallback_thumbnail_url
        video_url = video_url or fallback_video_url
        carousel_urls = carousel_urls or fallback_carousel_urls
    else:
        thumbnail_url = fallback_thumbnail_url
        video_url = fallback_video_url
        carousel_urls = fallback_carousel_urls

    if not video_url:
        return False

    _patch_row(
        "posts",
        {"post_key": f"eq.{post_key}"},
        {
            "thumbnail_url": thumbnail_url,
            "video_url": video_url,
            "carousel_urls": carousel_urls or None,
            "updated_at": _now_iso(),
        },
    )

    posted_at = _to_dt(post.get("posted_at"))
    purge_after = _preview_extension_deadline(posted_at).isoformat()
    payload = {
        "post_key": post_key,
        "asset_role": "preview_5s",
        "source_url": video_url,
        "storage_provider": candidate["storage_provider"],
        "storage_bucket": candidate["storage_bucket"],
        "status": "pending_capture",
        "attempt": 0,
        "next_run_at": _now_iso(),
        "purge_after": purge_after,
        "storage_path": None,
        "public_url": None,
        "mime_type": None,
        "byte_size": None,
        "captured_at": None,
        "deleted_at": None,
        "last_error": None,
        "updated_at": _now_iso(),
    }

    asset_id = asset.get("id")
    if asset_id:
        _patch_row("post_media_assets", {"id": f"eq.{int(asset_id)}"}, payload)
    else:
        _insert_row("post_media_assets", payload)
    return True


def _fetch_statuses(post_keys: list[str]) -> dict[str, dict[str, Any]]:
    statuses: dict[str, dict[str, Any]] = {}
    for chunk in _chunked(post_keys, 100):
        joined = ",".join(chunk)
        resp = requests.get(
            _rest_url("post_media_assets"),
            headers=_rest_headers(),
            params={
                "select": "post_key,status,updated_at,last_error,public_url,storage_path",
                "asset_role": "eq.preview_5s",
                "post_key": f"in.({joined})",
            },
            timeout=60,
        )
        resp.raise_for_status()
        for row in resp.json():
            key = str(row.get("post_key") or "").strip().lower()
            if key:
                statuses[key] = row
    return statuses


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh April-14+ failed Fire reel preview sources via REST.")
    parser.add_argument("--day", type=str, required=True)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--stale-minutes", type=int, default=DEFAULT_STALE_MINUTES)
    parser.add_argument("--verify-timeout", type=int, default=DEFAULT_VERIFY_TIMEOUT_SECONDS)
    parser.add_argument("--verify-interval", type=int, default=DEFAULT_VERIFY_INTERVAL_SECONDS)
    args = parser.parse_args()

    candidates = _fetch_candidate_posts(args.day, max(1, args.limit), max(1, args.stale_minutes))
    print(f"Selected candidates: {len(candidates)}", flush=True)
    if not candidates:
        return 0

    by_post_key = {str(item["post"]["post_key"]).strip().lower(): item for item in candidates}
    ordered_keys = list(by_post_key.keys())

    updated = 0
    updated_keys: list[str] = []
    missing = 0
    for chunk_keys in _chunked(ordered_keys, max(1, args.batch_size)):
        chunk_urls = [str(by_post_key[key]["post"].get("post_url") or "").strip() for key in chunk_keys]
        items = run_actor_post_urls("", chunk_urls, mode="reel")
        by_short, item_by_post_key = _build_item_maps(items)

        for key in chunk_keys:
            candidate = by_post_key[key]
            post_url = str(candidate["post"].get("post_url") or "").strip()
            short = shortcode_from_url(post_url).lower()
            item = item_by_post_key.get(key) or (by_short.get(short) if short else None)
            if _restage_candidate(candidate, item):
                updated += 1
                updated_keys.append(key)
            else:
                missing += 1

    print(f"Restaged preview rows: {updated}; missing source refs: {missing}", flush=True)

    if updated <= 0:
        return 0

    deadline = time.time() + max(1, args.verify_timeout)
    pending = set(updated_keys)
    active = 0
    failed = 0
    while pending and time.time() < deadline:
        statuses = _fetch_statuses(sorted(pending))
        for key in list(pending):
            row = statuses.get(key)
            if not row:
                pending.remove(key)
                failed += 1
                continue
            status = str(row.get("status") or "").strip().lower()
            if status == "active" and str(row.get("storage_path") or "").strip():
                active += 1
                pending.remove(key)
            elif status == "capture_failed":
                failed += 1
                pending.remove(key)
        if pending:
            time.sleep(max(1, args.verify_interval))

    print(
        f"Verification summary: active={active} failed={failed} pending={len(pending)}",
        flush=True,
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
