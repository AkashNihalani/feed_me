from __future__ import annotations

import os
import sys
import types
import unittest
from pathlib import Path

os.environ.setdefault("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/db")
os.environ.setdefault("BRIGHTDATA_API_KEY", "test")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

brightdata = types.ModuleType("app.brightdata")
brightdata.run_handle = lambda *args, **kwargs: []
brightdata.run_post_urls = lambda *args, **kwargs: []
brightdata.run_reel_post_urls = lambda *args, **kwargs: []
sys.modules["app.brightdata"] = brightdata

from app import scraper  # noqa: E402


class ScraperRoutingTest(unittest.TestCase):
    def test_reel_mode_does_not_fallback_to_posts_when_reel_records_exist(self) -> None:
        calls: list[tuple[str, list[str]]] = []

        def reels(urls: list[str]) -> list[dict[str, str]]:
            calls.append(("reels", list(urls)))
            return [{"url": urls[0], "videoUrl": ""}]

        def posts(urls: list[str]) -> list[dict[str, str]]:
            calls.append(("posts", list(urls)))
            return [{"url": urls[0], "kind": "post"}]

        scraper.brightdata_run_reel_post_urls = reels
        scraper.brightdata_run_post_urls = posts

        result = scraper.run_actor_post_urls("", ["https://www.instagram.com/reel/abc/"], mode="reel")

        self.assertEqual(result, [{"url": "https://www.instagram.com/reel/abc/", "videoUrl": ""}])
        self.assertEqual(calls, [("reels", ["https://www.instagram.com/reel/abc/"])])

    def test_reel_mode_falls_back_to_posts_only_when_reels_returns_empty(self) -> None:
        calls: list[tuple[str, list[str]]] = []

        def reels(urls: list[str]) -> list[dict[str, str]]:
            calls.append(("reels", list(urls)))
            return []

        def posts(urls: list[str]) -> list[dict[str, str]]:
            calls.append(("posts", list(urls)))
            return [{"url": urls[0], "kind": "post"}]

        scraper.brightdata_run_reel_post_urls = reels
        scraper.brightdata_run_post_urls = posts

        result = scraper.run_actor_post_urls("", ["https://www.instagram.com/reel/abc/"], mode="reel")

        self.assertEqual(result, [{"url": "https://www.instagram.com/reel/abc/", "kind": "post"}])
        self.assertEqual(
            calls,
            [
                ("reels", ["https://www.instagram.com/reel/abc/"]),
                ("posts", ["https://www.instagram.com/reel/abc/"]),
            ],
        )


if __name__ == "__main__":
    unittest.main()
