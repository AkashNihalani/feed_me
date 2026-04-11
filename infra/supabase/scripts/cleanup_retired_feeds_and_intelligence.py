#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests


ROOT = Path(__file__).resolve().parents[3]
WEB_ENV = ROOT / "apps" / "web" / ".env.local"

REQUIRED_TAG_KEYS = {
    "mechanic",
    "opening_move",
    "proof_mode",
    "pacing",
    "style",
    "face",
    "language",
    "depth",
    "density",
    "text_overlay",
}
OPTIONAL_TAG_KEYS = {"audio_mode", "duration_bucket", "cta"}
INTERNAL_TAG_KEYS = {"_visual_source"}
LEGACY_TAG_KEYS = {"hook", "pillar", "format", "subject"}
ALLOWED_TAG_KEYS = REQUIRED_TAG_KEYS | OPTIONAL_TAG_KEYS | INTERNAL_TAG_KEYS


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def rest_headers() -> dict[str, str]:
    key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "count=exact",
    }


def rest_base() -> str:
    return os.environ["NEXT_PUBLIC_SUPABASE_URL"].rstrip("/")


def rest_request(method: str, path: str, *, params: dict[str, str] | None = None, json_body: Any = None) -> requests.Response:
    response = requests.request(
        method,
        f"{rest_base()}/rest/v1/{path}",
        headers=rest_headers(),
        params=params or {},
        json=json_body,
        timeout=30,
    )
    response.raise_for_status()
    return response


def fetch_all(path: str, params: dict[str, str], *, page_size: int = 1000) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        response = rest_request(
            "GET",
            path,
            params={**params, "limit": str(page_size), "offset": str(offset)},
        )
        page = response.json()
        if not isinstance(page, list):
            raise RuntimeError(f"Unexpected {path} response: {page!r}")
        rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return rows


def fetch_count(path: str, params: dict[str, str]) -> int:
    response = rest_request("GET", path, params={**params, "limit": "1"})
    content_range = response.headers.get("content-range", "")
    if "/" not in content_range:
        return 0
    return int(content_range.rsplit("/", 1)[1])


def fetch_retired_media_assets() -> list[dict[str, Any]]:
    removed_assets = fetch_all(
        "post_media_assets",
        {
            "select": "storage_bucket,storage_path,posts!inner(feeders!inner(status))",
            "posts.feeders.status": "eq.removed",
        },
    )
    archived_assets = fetch_all(
        "post_media_assets",
        {
            "select": "storage_bucket,storage_path,posts!inner(feeders!inner(feeds!inner(status)))",
            "posts.feeders.feeds.status": "eq.archived",
        },
    )
    seen: set[tuple[str, str]] = set()
    merged: list[dict[str, Any]] = []
    for row in removed_assets + archived_assets:
        bucket = str(row.get("storage_bucket") or "").strip()
        path = str(row.get("storage_path") or "").strip()
        if not bucket or not path:
            continue
        key = (bucket, path)
        if key in seen:
            continue
        seen.add(key)
        merged.append({"storage_bucket": bucket, "storage_path": path})
    return merged


def delete_storage_assets(assets: list[dict[str, Any]]) -> None:
    headers = {
        "apikey": os.environ["SUPABASE_SERVICE_ROLE_KEY"],
        "Authorization": f"Bearer {os.environ['SUPABASE_SERVICE_ROLE_KEY']}",
    }
    for row in assets:
        bucket = str(row["storage_bucket"])
        path = str(row["storage_path"])
        response = requests.delete(
            f"{rest_base()}/storage/v1/object/{quote(bucket, safe='')}/{quote(path, safe='/')}",
            headers=headers,
            timeout=30,
        )
        if response.status_code not in (200, 204, 404):
            response.raise_for_status()


def clean_intelligence_rows(*, apply: bool) -> tuple[int, Counter[str]]:
    rows = fetch_all(
        "post_intelligence",
        {"select": "post_key,tags,model_version,extracted_at"},
    )
    bad_post_keys: list[str] = []
    key_counts: Counter[str] = Counter()
    for row in rows:
        tags = row.get("tags") if isinstance(row.get("tags"), dict) else {}
        keys = set(tags.keys())
        key_counts.update(keys)
        if (keys & LEGACY_TAG_KEYS) or not REQUIRED_TAG_KEYS.issubset(keys) or bool(keys - ALLOWED_TAG_KEYS):
            post_key = str(row.get("post_key") or "").strip()
            if post_key:
                bad_post_keys.append(post_key)

    if apply:
        for post_key in bad_post_keys:
            rest_request("DELETE", "post_intelligence", params={"post_key": f"eq.{post_key}"})
    return len(bad_post_keys), key_counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean retired FeedMe data over Supabase HTTPS APIs.")
    parser.add_argument("--apply", action="store_true", help="Actually delete retired rows/assets and invalid intelligence rows.")
    args = parser.parse_args()

    load_env(WEB_ENV)
    if not os.environ.get("NEXT_PUBLIC_SUPABASE_URL") or not os.environ.get("SUPABASE_SERVICE_ROLE_KEY"):
        raise SystemExit("Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

    counts = {
        "removed_feeders": fetch_count("feeders", {"select": "id", "status": "eq.removed"}),
        "archived_feeds": fetch_count("feeds", {"select": "id", "status": "eq.archived"}),
        "posts_under_removed_feeders": fetch_count(
            "posts",
            {"select": "post_key,feeders!inner(status)", "feeders.status": "eq.removed"},
        ),
        "media_assets_under_removed_feeders": fetch_count(
            "post_media_assets",
            {"select": "id,posts!inner(feeders!inner(status))", "posts.feeders.status": "eq.removed"},
        ),
    }
    media_assets = fetch_retired_media_assets()
    invalid_intelligence_count, key_counts = clean_intelligence_rows(apply=args.apply)

    print("mode:", "apply" if args.apply else "dry-run")
    for key, value in counts.items():
        print(f"{key}: {value}")
    print("storage_objects_to_delete:", len(media_assets))
    print("invalid_or_legacy_intelligence_rows:", invalid_intelligence_count)
    print("intelligence_keys:", dict(sorted(key_counts.items())))

    if not args.apply:
        print("no changes made")
        return

    delete_storage_assets(media_assets)
    rest_request("DELETE", "feeds", params={"status": "eq.archived"})
    rest_request("DELETE", "feeders", params={"status": "eq.removed"})
    print("cleanup applied")


if __name__ == "__main__":
    main()
