from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


DEFAULT_PAGE_SIZE = 200
DEFAULT_CONCURRENCY = 6
DEFAULT_MAX_DURATION_SECONDS = 5.08
DEFAULT_VERIFY_TIMEOUT_SECONDS = 480
DEFAULT_VERIFY_INTERVAL_SECONDS = 12


def _env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        value = value[1:-1]
    return value


SUPABASE_URL = _env("NEXT_PUBLIC_SUPABASE_URL").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = _env("SUPABASE_SERVICE_ROLE_KEY")
MEDIA_PUBLIC_BASE_URL = _env("MEDIA_PUBLIC_BASE_URL").rstrip("/")


def _headers(*, prefer: str | None = None) -> dict[str, str]:
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


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _probe_duration_seconds(media_bytes: bytes, suffix: str = ".mp4") -> float | None:
    tmp = None
    try:
        tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
        tmp.write(media_bytes)
        tmp.close()
        proc = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                tmp.name,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if proc.returncode != 0:
            return None
        raw = (proc.stdout or "").strip()
        return float(raw) if raw else None
    except Exception:
        return None
    finally:
        if tmp and tmp.name:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass


def _resolve_asset_url(asset: dict[str, Any]) -> str | None:
    public_url = str(asset.get("public_url") or "").strip()
    if public_url:
        return public_url
    storage_path = str(asset.get("storage_path") or "").strip().lstrip("/")
    if storage_path and MEDIA_PUBLIC_BASE_URL:
        return f"{MEDIA_PUBLIC_BASE_URL}/{storage_path}"
    return None


@dataclass
class ProbeResult:
    asset_id: int
    status: str
    duration_seconds: float | None
    detail: str | None = None


def _probe_asset(asset: dict[str, Any], max_duration_seconds: float) -> ProbeResult:
    asset_id = int(asset["id"])
    url = _resolve_asset_url(asset)
    if not url:
        return ProbeResult(asset_id=asset_id, status="missing_url", duration_seconds=None, detail="missing public URL")

    try:
        resp = requests.get(url, timeout=60)
        if resp.status_code == 404:
            return ProbeResult(asset_id=asset_id, status="missing", duration_seconds=None, detail="stored object missing")
        resp.raise_for_status()
        body = resp.content
    except Exception as exc:
        return ProbeResult(asset_id=asset_id, status="fetch_failed", duration_seconds=None, detail=str(exc)[:300])

    if not body:
        return ProbeResult(asset_id=asset_id, status="missing", duration_seconds=None, detail="empty body")

    duration_seconds = _probe_duration_seconds(body)
    if duration_seconds is None:
        return ProbeResult(asset_id=asset_id, status="probe_failed", duration_seconds=None, detail="ffprobe failed")
    if duration_seconds > max_duration_seconds:
        return ProbeResult(
            asset_id=asset_id,
            status="overlong",
            duration_seconds=duration_seconds,
            detail=f"duration {duration_seconds:.3f}s > {max_duration_seconds:.3f}s",
        )
    return ProbeResult(asset_id=asset_id, status="valid", duration_seconds=duration_seconds)


def _fetch_assets(limit: int = 0, page_size: int = DEFAULT_PAGE_SIZE) -> list[dict[str, Any]]:
    assets: list[dict[str, Any]] = []
    offset = 0
    while True:
        batch_limit = page_size
        if limit > 0:
            remaining = limit - len(assets)
            if remaining <= 0:
                break
            batch_limit = min(batch_limit, remaining)
        resp = requests.get(
            _rest_url("post_media_assets"),
            headers=_headers(),
            params={
                "select": "id,post_key,asset_role,status,storage_provider,storage_bucket,storage_path,public_url,source_url,updated_at",
                "asset_role": "eq.preview_5s",
                "status": "eq.active",
                "order": "id.asc",
                "limit": str(batch_limit),
                "offset": str(offset),
            },
            timeout=60,
        )
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            break
        assets.extend(rows)
        offset += len(rows)
        if len(rows) < batch_limit:
            break
    return assets


def _patch_asset(asset_id: int, payload: dict[str, Any]) -> None:
    resp = requests.patch(
        _rest_url("post_media_assets"),
        headers=_headers(prefer="return=minimal"),
        params={"id": f"eq.{asset_id}"},
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()


def _requeue_asset(asset: dict[str, Any], probe: ProbeResult) -> None:
    now = _now_iso()
    detail = probe.detail or probe.status
    _patch_asset(
        int(asset["id"]),
        {
            "status": "pending_capture",
            "attempt": 0,
            "next_run_at": now,
            "storage_path": None,
            "public_url": None,
            "mime_type": None,
            "byte_size": None,
            "captured_at": None,
            "deleted_at": None,
            "last_error": f"preview repair via rest: {detail}"[:1000],
            "updated_at": now,
        },
    )


def _fetch_assets_by_ids(asset_ids: list[int]) -> dict[int, dict[str, Any]]:
    if not asset_ids:
        return {}
    lookup: dict[int, dict[str, Any]] = {}
    chunk_size = 100
    for start in range(0, len(asset_ids), chunk_size):
        chunk = asset_ids[start:start + chunk_size]
        joined = ",".join(str(asset_id) for asset_id in chunk)
        resp = requests.get(
            _rest_url("post_media_assets"),
            headers=_headers(),
            params={
                "select": "id,status,public_url,storage_path,updated_at,last_error",
                "id": f"in.({joined})",
            },
            timeout=60,
        )
        resp.raise_for_status()
        for row in resp.json():
            lookup[int(row["id"])] = row
    return lookup


def _verify_repaired(asset_ids: list[int], max_duration_seconds: float, timeout_seconds: int, interval_seconds: int) -> dict[str, Any]:
    deadline = time.time() + max(1, timeout_seconds)
    remaining = set(asset_ids)
    verified: dict[int, float] = {}
    failures: dict[int, str] = {}

    while remaining and time.time() < deadline:
        latest = _fetch_assets_by_ids(sorted(remaining))
        for asset_id in list(remaining):
            row = latest.get(asset_id)
            if not row:
                failures[asset_id] = "row missing after requeue"
                remaining.remove(asset_id)
                continue
            if row.get("status") != "active":
                continue
            url = _resolve_asset_url(row)
            if not url:
                continue
            probe = _probe_asset({"id": asset_id, **row}, max_duration_seconds)
            if probe.status == "valid" and probe.duration_seconds is not None:
                verified[asset_id] = probe.duration_seconds
                remaining.remove(asset_id)
            elif probe.status in ("overlong", "probe_failed", "fetch_failed", "missing", "missing_url"):
                failures[asset_id] = probe.detail or probe.status
                remaining.remove(asset_id)
        if remaining:
            time.sleep(max(1, interval_seconds))

    return {
        "verified": len(verified),
        "verified_assets": verified,
        "failed": len(failures),
        "failures": failures,
        "timed_out": sorted(remaining),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair overlong Fire preview clips via Supabase REST.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum number of active preview assets to scan. 0 = all.")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--max-duration", type=float, default=DEFAULT_MAX_DURATION_SECONDS)
    parser.add_argument("--verify-timeout", type=int, default=DEFAULT_VERIFY_TIMEOUT_SECONDS)
    parser.add_argument("--verify-interval", type=int, default=DEFAULT_VERIFY_INTERVAL_SECONDS)
    args = parser.parse_args()

    assets = _fetch_assets(limit=max(0, args.limit), page_size=max(1, args.page_size))
    print(f"Scanned candidate rows: {len(assets)}", flush=True)
    if not assets:
        print(json.dumps({"scanned": 0, "valid": 0, "requeued": 0, "verified": 0}, indent=2))
        return 0

    probes: dict[int, ProbeResult] = {}
    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as executor:
        futures = {
            executor.submit(_probe_asset, asset, float(args.max_duration)): asset
            for asset in assets
        }
        for future in as_completed(futures):
            asset = futures[future]
            result = future.result()
            probes[int(asset["id"])] = result

    valid = [probe for probe in probes.values() if probe.status == "valid"]
    to_requeue = [
        asset for asset in assets
        if probes[int(asset["id"])].status in ("overlong", "missing", "missing_url", "probe_failed", "fetch_failed")
    ]

    for asset in to_requeue:
        _requeue_asset(asset, probes[int(asset["id"])])

    verification = _verify_repaired(
        [int(asset["id"]) for asset in to_requeue],
        max_duration_seconds=float(args.max_duration),
        timeout_seconds=max(1, args.verify_timeout),
        interval_seconds=max(1, args.verify_interval),
    )

    summary = {
        "scanned": len(assets),
        "valid": len(valid),
        "requeued": len(to_requeue),
        "requeue_reasons": {
            status: sum(1 for probe in probes.values() if probe.status == status)
            for status in sorted({probe.status for probe in probes.values() if probe.status != "valid"})
        },
        "verified": verification["verified"],
        "failed": verification["failed"],
        "timed_out": len(verification["timed_out"]),
    }
    print(json.dumps(summary, indent=2), flush=True)
    if verification["failures"]:
        print("Verification failures:", flush=True)
        print(json.dumps(verification["failures"], indent=2), flush=True)
    if verification["timed_out"]:
        print("Verification timed out for:", flush=True)
        print(json.dumps(verification["timed_out"], indent=2), flush=True)

    return 0 if not verification["failures"] and not verification["timed_out"] else 1


if __name__ == "__main__":
    sys.exit(main())
