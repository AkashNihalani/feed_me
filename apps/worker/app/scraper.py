from __future__ import annotations

from .brightdata import (
    run_handle as brightdata_run_handle,
    run_post_urls as brightdata_run_post_urls,
    run_reel_post_urls as brightdata_run_reel_post_urls,
)


def run_actor_handle(handle: str, days_window: int = 2, recent_post_ids: list[str] | None = None) -> list[dict]:
    return brightdata_run_handle(handle, recent_post_ids=recent_post_ids)


def run_actor_post_urls(handle: str, post_urls: list[str], mode: str = "post") -> list[dict]:
    if (mode or "").strip().lower() == "reel":
        return brightdata_run_reel_post_urls(post_urls)
    return brightdata_run_post_urls(post_urls)
