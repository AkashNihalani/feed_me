from .brightdata import run_handle as brightdata_run_handle, run_post_urls as brightdata_run_post_urls


def run_actor_handle(handle: str, days_window: int = 2) -> list[dict]:
    return brightdata_run_handle(handle)


def run_actor_post_urls(handle: str, post_urls: list[str]) -> list[dict]:
    return brightdata_run_post_urls(post_urls)
