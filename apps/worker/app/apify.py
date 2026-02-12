import json
import time
import requests

from .config import (
    APIFY_TOKEN,
    APIFY_ACTOR_ID,
    APIFY_INPUT_TEMPLATE_DAILY,
    APIFY_INPUT_TEMPLATE_WEEKLY,
    APIFY_INPUT_TEMPLATE_DETAILS,
    APIFY_INPUT_TEMPLATE_POST_URL,
    APIFY_RUN_TIMEOUT_SECONDS,
    APIFY_POLL_INTERVAL_SECONDS,
)

API_BASE = "https://api.apify.com/v2"


def _build_input(handle: str, run_type: str, post_url: str | None = None) -> dict:
    template = APIFY_INPUT_TEMPLATE_DAILY
    if run_type == "weekly":
        template = APIFY_INPUT_TEMPLATE_WEEKLY
    if run_type == "details":
        template = APIFY_INPUT_TEMPLATE_DETAILS
    if run_type == "post_url":
        template = APIFY_INPUT_TEMPLATE_POST_URL
    payload = template.replace("{handle}", handle).replace("{post_url}", post_url or "")
    return json.loads(payload)


def _run_payload(input_payload: dict) -> list[dict]:
    run_url = f"{API_BASE}/acts/{APIFY_ACTOR_ID}/runs?token={APIFY_TOKEN}"
    resp = requests.post(run_url, json=input_payload, timeout=60)
    resp.raise_for_status()
    run_id = resp.json().get("data", {}).get("id")
    if not run_id:
        raise RuntimeError("Apify run did not return a run id")

    status = "RUNNING"
    start = time.time()
    check = None
    while status in ("RUNNING", "READY"):
        if time.time() - start > APIFY_RUN_TIMEOUT_SECONDS:
            raise TimeoutError("Apify run timed out")
        time.sleep(APIFY_POLL_INTERVAL_SECONDS)
        check = requests.get(f"{API_BASE}/actor-runs/{run_id}?token={APIFY_TOKEN}", timeout=30)
        check.raise_for_status()
        status = check.json().get("data", {}).get("status")

    if status != "SUCCEEDED":
        raise RuntimeError(f"Apify run failed with status: {status}")

    dataset_id = check.json().get("data", {}).get("defaultDatasetId") if check is not None else None
    if not dataset_id:
        raise RuntimeError("Apify run missing dataset id")

    items_url = f"{API_BASE}/datasets/{dataset_id}/items?clean=true&format=json"
    items = requests.get(items_url, timeout=60)
    items.raise_for_status()
    return items.json()


def run_actor(handle: str, run_type: str, post_url: str | None = None) -> list[dict]:
    input_payload = _build_input(handle, run_type, post_url)
    return _run_payload(input_payload)


def run_actor_post_urls(handle: str, post_urls: list[str]) -> list[dict]:
    urls = [u.strip() for u in (post_urls or []) if (u or "").strip()]
    if not urls:
        return []
    input_payload = _build_input(handle, "post_url", post_url=urls[0])
    input_payload["directUrls"] = urls
    current_limit = int(input_payload.get("resultsLimit") or 0)
    input_payload["resultsLimit"] = max(current_limit, len(urls))
    return _run_payload(input_payload)


def run_actor_details(handle: str) -> dict:
    items = run_actor(handle, "details")
    if not items:
        return {}
    if isinstance(items, list):
        return items[0] if items else {}
    return items


def run_actor_post_url(handle: str, post_url: str) -> dict:
    items = run_actor(handle, "post_url", post_url=post_url)
    if not items:
        return {}
    if isinstance(items, list):
        return items[0] if items else {}
    return items
