from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

os.environ.setdefault("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/db")
os.environ.setdefault("BRIGHTDATA_API_KEY", "test")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.pure_engine import _choose_canonical_fire_metric  # noqa: E402
from app.signal_detection import Metric, Post, _metric_complete  # noqa: E402


class EngagementRateRankingTest(unittest.TestCase):
    def test_engagement_rate_is_the_only_canonical_metric(self) -> None:
        metric = _choose_canonical_fire_metric(
            {"likes": 999, "comments": 999, "engagement_rate": 0.0125, "engagement_rate_multiple": 1.4},
            "reel",
        )
        self.assertEqual(metric, "engagement_rate")

    def test_reel_metric_is_complete_without_views_when_engagement_is_ranked(self) -> None:
        post = Post("reel", 1, 1, "handle", "standard", None, "reel", None)
        post.metrics["d7"] = Metric(
            checkpoint="d7",
            business_date=None,
            percentile=10,
            views=None,
            likes=100,
            comments=10,
            views_baseline=None,
            likes_baseline=50,
            comments_baseline=5,
            views_x=None,
            likes_x=2,
            comments_x=2,
            engagement_rate=0.01,
            engagement_rate_baseline=0.005,
            engagement_rate_x=2,
        )
        self.assertTrue(_metric_complete(post, "d7"))


if __name__ == "__main__":
    unittest.main()
