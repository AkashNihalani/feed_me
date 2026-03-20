import json
import time
import requests
from urllib.parse import quote

from .config import (
    APIFY_TOKEN,
    APIFY_ACTOR_ID,
    APIFY_RUN_TIMEOUT_SECONDS,
    APIFY_POLL_INTERVAL_SECONDS,
)

API_BASE = "https://api.apify.com/v2"


def _run_payload(input_payload: dict) -> list[dict]:
    actor_ref = quote(APIFY_ACTOR_ID, safe="")
    run_url = f"{API_BASE}/acts/{actor_ref}/runs?token={APIFY_TOKEN}"
    resp = requests.post(run_url, json=input_payload, timeout=60)
    resp.raise_for_status()
    run_id = resp.json().get("data", {}).get("id")
    if not run_id:
        raise RuntimeError("Apify run did not return run id")

    status = "RUNNING"
    check = None
    start = time.time()
    while status in ("RUNNING", "READY"):
        if time.time() - start > APIFY_RUN_TIMEOUT_SECONDS:
            raise TimeoutError("Apify run timeout")
        time.sleep(APIFY_POLL_INTERVAL_SECONDS)
        check = requests.get(f"{API_BASE}/actor-runs/{run_id}?token={APIFY_TOKEN}", timeout=30)
        check.raise_for_status()
        status = check.json().get("data", {}).get("status")

    if status != "SUCCEEDED":
        raise RuntimeError(f"Apify run failed: {status}")

    dataset_id = check.json().get("data", {}).get("defaultDatasetId") if check is not None else None
    if not dataset_id:
        raise RuntimeError("Apify missing dataset id")

    items = requests.get(f"{API_BASE}/datasets/{dataset_id}/items?clean=true&format=json", timeout=60)
    items.raise_for_status()
    data = items.json()
    return data if isinstance(data, list) else []


def run_actor_handle(handle: str, days_window: int = 2) -> list[dict]:
    clean = (handle or "").lstrip("@").strip()
    payload = {
        "addParentData": False,
        "directUrls": [f"https://www.instagram.com/{clean}/"],
        "resultsType": "posts",
        "searchType": "user",
        "resultsLimit": 100,
        "onlyPostsNewerThan": f"{max(1, int(days_window))} days",
    }
    return _run_payload(payload)


def run_actor_post_urls(handle: str, post_urls: list[str]) -> list[dict]:
    urls = [u.strip() for u in (post_urls or []) if (u or "").strip()]
    if not urls:
        return []
    payload = {
        "addParentData": False,
        "directUrls": urls,
        "resultsType": "posts",
        "resultsLimit": max(1, len(urls)),
    }
    return _run_payload(payload)
