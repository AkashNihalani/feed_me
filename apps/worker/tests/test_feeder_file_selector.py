from __future__ import annotations

import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/db")
os.environ.setdefault("BRIGHTDATA_API_KEY", "test")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.feeder_file_selector import (  # noqa: E402
    RECENT_CONTEXT_TARGET,
    REFERENCE_SLOT_TARGET,
    select_active_feeder_file,
)


def _make_post(
    key: str,
    days_ago: int,
    pct: float | None,
    *,
    views: int = 1000,
    exact: float | None = None,
    media_type: str = "reel",
) -> dict:
    base = datetime(2026, 5, 30, 12, 0, 0, tzinfo=timezone.utc).timestamp()
    posted = datetime.fromtimestamp(base - days_ago * 86400, tz=timezone.utc)
    return {
        "post_key": key,
        "posted_at": posted,
        "media_type": media_type,
        "views": views,
        "likes": views // 10,
        "comments": views // 100,
        "percentile_performance": pct,
        "percentile_performance_exact": exact if exact is not None else pct,
        "ranking_metric": views,
        "ranking_multiple": 1.0,
    }


def _assert_no_dupes(test: unittest.TestCase, result: dict) -> None:
    """Hard dedupe: no post appears twice within a lane or across the two."""
    for lane in ("reference", "recent_context"):
        keys = [e["post_key"] for e in result[lane]]
        test.assertEqual(len(keys), len(set(keys)), f"duplicate post_keys in {lane}")
    keys = [e["post_key"] for e in result["reference"]] + [
        e["post_key"] for e in result["recent_context"]
    ]
    test.assertEqual(len(keys), len(set(keys)), "post used in both lanes")


class FeederFileSelectorTest(unittest.TestCase):
    def test_mature_account_fills_full_30(self) -> None:
        mature = [
            _make_post(f"m{i:02d}", days_ago=i, pct=float((i * 2) % 90) + 1.0, views=5000 - i * 50)
            for i in range(45)
        ]
        res = select_active_feeder_file(mature, bar_percentile=30.0)

        _assert_no_dupes(self, res)
        self.assertEqual(res["counts"]["total"], 30)
        self.assertEqual(len(res["reference"]), REFERENCE_SLOT_TARGET)
        self.assertEqual(len(res["recent_context"]), RECENT_CONTEXT_TARGET)
        self.assertFalse(res["still_learning"])
        self.assertIs(res["ranked"], res["reference"])  # back-compat alias

        # Lanes are disjoint: the recent lane is exactly the newest 10.
        newest10 = {f"m{i:02d}" for i in range(RECENT_CONTEXT_TARGET)}
        self.assertEqual({e["post_key"] for e in res["recent_context"]}, newest10)

        for e in res["reference"]:
            self.assertEqual(e["memory_type"], "reference")
            self.assertEqual(e["occupying_slot"], "reference_slot")
            self.assertIn(e["primary"], {"n25", "n50", "n75", "full"})
            self.assertEqual(e["weight"], len(e["memberships"]))
            self.assertGreaterEqual(e["weight"], 1)
            self.assertNotIn(e["post_key"], newest10)  # never overlaps recent lane
        for e in res["recent_context"]:
            self.assertEqual(e["memory_type"], "recent_context")
            self.assertFalse(e["winner"])

    def test_cold_start_stays_light_no_padding(self) -> None:
        # 7 posts -> all 7 occupy the recent lane, leaving no non-recent stream.
        # The reference lane stays EMPTY: we never borrow from the recent lane to
        # fake depth. Hard dedupe means no post is reused.
        cold = [_make_post(f"c{i}", days_ago=i, pct=float(10 + i)) for i in range(7)]
        res = select_active_feeder_file(cold, bar_percentile=30.0)
        _assert_no_dupes(self, res)
        self.assertEqual(res["counts"]["reference"], 0)
        self.assertEqual(len(res["reference"]), 0)
        self.assertEqual(res["counts"]["recent_context"], 7)
        self.assertTrue(res["still_learning"])

    def test_strong_head_of_stream_wins_every_subset(self) -> None:
        # The strongest NON-recent post sits at the head of the stream, so each
        # nested subset re-encounters it. It takes exactly one slot, weight 4.
        dupe = [_make_post(f"d{i:02d}", days_ago=i, pct=float(50 + i)) for i in range(40)]
        dupe[10]["percentile_performance"] = 1.0
        dupe[10]["percentile_performance_exact"] = 1.0
        res = select_active_feeder_file(dupe, bar_percentile=30.0)
        _assert_no_dupes(self, res)
        d10 = [e for e in res["reference"] if e["post_key"] == "d10"]
        self.assertEqual(len(d10), 1)
        self.assertEqual(d10[0]["primary"], "n25")
        self.assertEqual(d10[0]["weight"], 4)
        self.assertEqual(d10[0]["memberships"], ["n25", "n50", "n75", "full"])
        self.assertTrue(d10[0]["winner"])

    def test_metric_tie_breaks_toward_recency(self) -> None:
        tie = [_make_post(f"t{i:02d}", days_ago=i, pct=float(40 + i)) for i in range(40)]
        for idx in (15, 16):  # both in the non-recent stream
            tie[idx]["percentile_performance_exact"] = 10.0
            tie[idx]["percentile_performance"] = 10.0
        res = select_active_feeder_file(tie, bar_percentile=30.0)
        order = [e["post_key"] for e in res["reference"]]
        self.assertIn("t15", order)
        self.assertIn("t16", order)
        self.assertLess(order.index("t15"), order.index("t16"))  # recent wins tie

    def test_weak_posts_still_fill_reference_without_a_bar(self) -> None:
        # No absolute bar gates the reference lane: even an all-weak account
        # fills its 20 reference slots, just tagged winner=False.
        weak = [_make_post(f"w{i:02d}", days_ago=i, pct=float(60 + (i % 30))) for i in range(40)]
        res = select_active_feeder_file(weak, bar_percentile=30.0)
        self.assertEqual(len(res["reference"]), REFERENCE_SLOT_TARGET)
        self.assertFalse(res["still_learning"])
        self.assertTrue(all(e["winner"] is False for e in res["reference"]))

    def test_small_account_stays_light_no_overlap(self) -> None:
        # 26 posts -> 10 recent + 16 non-recent. The nested subsets drain the
        # 16-post stream into the reference lane; there is no 30th post to reach,
        # so the feeder stays LIGHT at 16 (never padded from the recent lane).
        posts = [_make_post(f"s{i:02d}", days_ago=i, pct=float(10 + i % 40)) for i in range(26)]
        res = select_active_feeder_file(posts, bar_percentile=30.0)
        _assert_no_dupes(self, res)
        self.assertEqual(res["counts"]["non_recent_pool"], 16)
        self.assertEqual(len(res["reference"]), 16)
        self.assertEqual(len(res["recent_context"]), 10)
        self.assertTrue(res["still_learning"])
        # No reference post may overlap the recent lane (hard dedupe).
        recent_keys = {e["post_key"] for e in res["recent_context"]}
        self.assertTrue(all(e["post_key"] not in recent_keys for e in res["reference"]))

    def test_null_percentiles_never_tagged_winner(self) -> None:
        posts = [_make_post(f"n{i:02d}", days_ago=i, pct=None) for i in range(35)]
        res = select_active_feeder_file(posts, bar_percentile=30.0)
        self.assertTrue(all(e["winner"] is False for e in res["reference"]))


if __name__ == "__main__":
    unittest.main()
