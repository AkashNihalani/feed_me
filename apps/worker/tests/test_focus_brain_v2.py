from __future__ import annotations

import os
import sys
import types
import unittest

os.environ.setdefault("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/db")
os.environ.setdefault("BRIGHTDATA_API_KEY", "test")

try:
    import requests  # noqa: F401
except ModuleNotFoundError:
    sys.modules["requests"] = types.SimpleNamespace()

try:
    import psycopg.rows  # noqa: F401
except ModuleNotFoundError:
    psycopg = types.ModuleType("psycopg")
    rows = types.ModuleType("psycopg.rows")
    rows.dict_row = None
    psycopg.rows = rows
    sys.modules["psycopg"] = psycopg
    sys.modules["psycopg.rows"] = rows

from app import focus_brain as fb  # noqa: E402


class FocusBrainV2Test(unittest.TestCase):
    def test_confidence_formula_buckets(self) -> None:
        high = fb._confidence_for_candidate(5, -40, "stable", 0)
        low = fb._confidence_for_candidate(1, 0, "decaying", 18)

        self.assertEqual(high["bucket"], "high")
        self.assertGreater(high["score"], 0.70)
        self.assertEqual(low["bucket"], "low")
        self.assertLess(low["score"], 0.40)

    def test_metric_baselines_keep_d21_as_context(self) -> None:
        baselines = fb._metric_baselines([
            {"media_type": "reel", "views": 100, "likes": 10, "comments": 1, "percentile": 20, "d21_percentile": 18},
            {"media_type": "reel", "views": 200, "likes": 20, "comments": 2, "percentile": 40, "d21_percentile": 22},
        ])

        self.assertEqual(baselines["by_format"]["reel"]["median_d7_percentile"], 30)
        self.assertEqual(baselines["by_format"]["reel"]["median_d21_percentile"], 20)

    def test_lifecycle_transitions_do_not_skip_stable_to_weakening(self) -> None:
        emerging = fb._derive_lifecycle_status(
            "nothing",
            {
                "evidence_count_window": 2,
                "window_posts": 5,
                "last_seen_n_posts_ago": 0,
                "opportunities_since_seen": 0,
                "evidence_in_last_5_posts": 2,
                "evidence_in_last_7_posts": 2,
                "evidence_in_last_12_posts": 2,
                "evidence_in_last_18_posts": 2,
            },
        )
        watching = fb._derive_lifecycle_status(
            "stable",
            {
                "evidence_count_window": 5,
                "window_posts": 12,
                "last_seen_n_posts_ago": 5,
                "opportunities_since_seen": 5,
                "evidence_in_last_5_posts": 0,
                "evidence_in_last_7_posts": 2,
                "evidence_in_last_12_posts": 5,
                "evidence_in_last_18_posts": 5,
                "competing_pattern_strengthening": True,
            },
        )
        no_skip = fb._derive_lifecycle_status(
            "stable",
            {
                "evidence_count_window": 5,
                "window_posts": 12,
                "last_seen_n_posts_ago": 7,
                "opportunities_since_seen": 7,
                "evidence_in_last_5_posts": 0,
                "evidence_in_last_7_posts": 1,
                "evidence_in_last_12_posts": 5,
                "evidence_in_last_18_posts": 5,
                "competing_pattern_strengthening": False,
            },
        )

        self.assertEqual(emerging, "emerging")
        self.assertEqual(watching, "watching")
        self.assertEqual(no_skip, "stable")

    def test_not_this_boundary_scoring_rejects_bad_match(self) -> None:
        pattern = {
            "label": "Caption Hook",
            "summary": "Caption sets joke before action.",
            "content_signature": {
                "what_happens": "Caption frames the joke before action begins.",
                "hook_style": "Caption creates the premise before visual payoff.",
                "production_style": "Creator-shot reel with quick payoff.",
                "voice_tone": "Deadpan.",
                "key_craft_moves": ["Caption carries setup.", "Visual action delivers payoff."],
                "not_this": ["Product tutorial copy.", "Educational product explainer."],
            },
        }

        self.assertTrue(fb._valid_existing_pattern_match("caption joke action visual payoff", pattern))
        self.assertFalse(fb._valid_existing_pattern_match("product tutorial educational product explainer", pattern))

    def test_derived_views_require_medium_confidence(self) -> None:
        registry = [
            {
                "pattern_id": "good",
                "format": "reel",
                "metric_effects": {"vs_baseline_pp": -12, "confidence": {"bucket": "medium"}},
                "lifecycle": {"status": "strengthening"},
            },
            {
                "pattern_id": "weak",
                "format": "reel",
                "metric_effects": {"vs_baseline_pp": -40, "confidence": {"bucket": "low"}},
                "lifecycle": {"status": "stable"},
            },
        ]

        views = fb._compute_derived_views(registry)

        self.assertEqual(views["top_performers"], ["good"])
        self.assertEqual(views["media_strategies"]["reel"]["best"], ["good"])

    def test_delta_log_and_tweak_cap(self) -> None:
        previous = {
            "pattern_id": "p1",
            "lifecycle": {"status": "emerging"},
            "latest_tweak": "Old tweak.",
            "recent_tweaks": [
                {"tweak": "A"},
                {"tweak": "B"},
                {"tweak": "C"},
            ],
        }
        new = {
            "pattern_id": "p1",
            "lifecycle": {"status": "stable"},
            "latest_tweak": "New tweak.",
        }

        merged = fb._merge_recent_tweaks(previous, {"tweak": "D"})
        delta = fb._build_delta_log([previous], [new])

        self.assertEqual([item["tweak"] for item in merged], ["D", "A", "B"])
        self.assertEqual(delta["promoted"], [{"pattern_id": "p1", "from": "emerging", "to": "stable"}])
        self.assertEqual(delta["fresh_tweaks"], [{"pattern_id": "p1", "tweak": "New tweak."}])

    def test_language_and_hash_guards(self) -> None:
        clean, bad = fb._clean_language("This seems 2x better.", "Fallback.")

        self.assertTrue(bad)
        self.assertEqual(clean, "Fallback.")
        self.assertEqual(fb._sha({"b": 2, "a": 1}), fb._sha({"a": 1, "b": 2}))

    def test_feed_focus_v2_replication_uses_pointer_arrays(self) -> None:
        feeder_pattern = {
            "pattern_id": "p1",
            "format": "reel",
            "label": "Caption Hook",
            "summary": "Caption sets joke before action.",
            "content_signature": {"what_happens": "Caption joke.", "not_this": []},
            "metric_effects": {
                "vs_baseline_pp": -20,
                "confidence": {"score": 0.8, "bucket": "high"},
                "associated_metrics": {"comments_x_avg": 2.2},
            },
            "lifecycle": {"status": "stable", "last_seen_n_posts_ago": 0, "opportunities_since_seen": 0},
            "proof_post_keys": ["a"],
        }
        payload = {
            "feed": {"id": 10},
            "feeders": [
                {"id": 1, "role": "anchor", "pattern_registry": [feeder_pattern]},
                {"id": 2, "role": "standard", "pattern_registry": [{**feeder_pattern, "pattern_id": "p2"}]},
            ],
            "metric_rows": [],
            "metric_windows": {"common": {"median_percentile": 20}},
            "feeder_metric_windows": {},
        }

        result = fb._build_feed_focus_v2(
            payload=payload,
            current={},
            legacy_result={},
            compile_kind="full_rebuild",
            started_at=fb.datetime.now(fb.timezone.utc),
            version=1,
        )

        self.assertEqual(len(result["pattern_registry"]), 1)
        links = result["pattern_registry"][0]["replication"]["linked_feeder_patterns"]
        self.assertEqual(links, [{"feeder_id": 1, "pattern_id": "p1"}, {"feeder_id": 2, "pattern_id": "p2"}])
        self.assertEqual(result["derived_views"]["engagement_associations"]["comment_spiking"], [result["pattern_registry"][0]["pattern_id"]])


if __name__ == "__main__":
    unittest.main()
