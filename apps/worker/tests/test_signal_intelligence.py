from __future__ import annotations

import os
import sys
import types
import unittest
from datetime import date

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

from app import signal_intelligence as si  # noqa: E402


class SignalIntelligenceCardContractTest(unittest.TestCase):
    def test_metric_classification_describes_follower_drop_without_numbers(self) -> None:
        classification = si._metric_classification({
            "signal_type": "OWN_FOLLOWER_DROP",
            "metric_snapshot": {
                "net_7d": -2415,
                "volatility": 805,
                "latest_count": 100000,
            },
        })

        self.assertEqual(classification["direction"], "declining")
        self.assertEqual(classification["magnitude"], "sharp")
        self.assertEqual(classification["vs_baseline"], "below_normal")

    def test_card_contract_accepts_brutal_schema_without_llm_confidence(self) -> None:
        card = {
            "title": "Ads hit. Trust left.",
            "what_happened": "Followers fell sharply. The drop sat outside normal account noise.",
            "why": "Failed because campaign delivery crowded out lived-in proof.",
            "common_pattern": ["dense brand run", "shock reaction hooks", "thin craft proof"],
            "do_next": "Kill stacked ads. Build proof-led resets.",
            "watchout": "A familiar face cannot carry an unfamiliar sales rhythm.",
            "per_post_notes": ["Persona-led product ad, vertical scroll"],
            "pattern_type": "account_outlier",
        }

        self.assertEqual(si._card_schema_errors(card), [])

    def test_card_contract_rejects_numbers_hedges_and_internal_vocab(self) -> None:
        card = {
            "title": "Cross-Follower Movement Surged Hard",
            "what_happened": "Net loss of 2,415 followers over 7 days, a 3x increase.",
            "why": "Failed because it may have made the anchor look like a campaign cohort.",
            "common_pattern": ["challenger surge", "cohort leak proof", "anchor label misuse"],
            "do_next": "Adopt a character-first approach.",
            "watchout": "The feed_focus bible should not leak.",
            "per_post_notes": ["Maybelline #Ad"],
            "pattern_type": "account_outlier",
        }

        errors = si._card_schema_errors(card)

        self.assertIn("title_looks_title_case", errors)
        self.assertIn("what_happened_contains_number", errors)
        self.assertIn("why_contains_hedge", errors)
        self.assertIn("why_contains_internal_vocab", errors)
        self.assertIn("common_pattern[0]_contains_internal_vocab", errors)
        self.assertIn("watchout_contains_internal_vocab", errors)
        self.assertIn("do_next_not_two_imperatives", errors)

    def test_server_confidence_uses_evidence_not_llm_rating(self) -> None:
        confidence = si._computed_confidence(
            {"business_date_ist": date.today()},
            [
                {
                    "cohort": "a",
                    "post_key": f"post-{index}",
                    "focus_read": {"relation_to_feeder_md": {"matches": ["deadpan hook"], "deviates": []}},
                }
                for index in range(5)
            ],
            {"magnitude": "sharp"},
        )

        self.assertEqual(confidence, "high")

    def test_metric_snapshot_keys_are_language_safe_for_prompt(self) -> None:
        safe = si._language_safe_metric_snapshot({
            "anchor_median": 18,
            "challenger_avg_percentile": 9,
            "signal_count": 3,
        })

        self.assertEqual(set(safe), {"primary_account_median", "comparison_account_avg_percentile", "movement_count"})

    def test_card_contract_rejects_generic_pattern_buckets(self) -> None:
        card = {
            "title": "Caption hook landed cleanly.",
            "what_happened": "Reach rose sharply inside the reel lane.",
            "why": "Worked because the creator framed the product through a specific family persona.",
            "common_pattern": ["social tension / confidence", "movement / rhythmic pacing", "high energy"],
            "do_next": "Kill static setups. Build character-led skits.",
            "watchout": "Fast pacing fails when the payoff is vague.",
            "per_post_notes": ["Eldest daughter trope, product demonstration"],
            "pattern_type": "feed_aligned",
        }

        errors = si._card_schema_errors(card)

        self.assertIn("common_pattern_0_generic_bucket", errors)
        self.assertIn("common_pattern_1_generic_bucket", errors)

    def test_generic_pattern_repair_promotes_post_notes(self) -> None:
        card = {
            "title": "Caption hook landed cleanly.",
            "what_happened": "Reach rose sharply inside the reel lane.",
            "why": "Worked because the creator framed the product through a specific family persona.",
            "common_pattern": ["social tension / confidence"],
            "do_next": "Kill office testimonials. Build creator-led sketches.",
            "watchout": "Persona proof collapses when the product has no job.",
            "per_post_notes": [
                "Eldest daughter trope, product demonstration",
                "CGI anatomical animation, absorption claim",
            ],
            "pattern_type": "feed_aligned",
        }

        repaired = si._repair_common_patterns(card, ["common_pattern_0_generic_bucket"])

        self.assertEqual(
            repaired["common_pattern"],
            [
                "Eldest daughter trope, product demonstration",
                "CGI anatomical animation, absorption claim",
            ],
        )

    def test_common_pattern_repair_trims_extra_entries(self) -> None:
        card = {
            "common_pattern": [
                "one specific craft",
                "two specific craft",
                "three specific craft",
                "four specific craft",
                "five specific craft",
                "six specific craft",
            ],
            "per_post_notes": [],
        }

        repaired = si._repair_common_patterns(card, ["common_pattern_too_many"])

        self.assertEqual(repaired["common_pattern"], card["common_pattern"][:5])

    def test_common_pattern_rejects_slash_and_known_proper_nouns(self) -> None:
        card = {
            "title": "Caption hook landed cleanly.",
            "what_happened": "Reach rose sharply on a reel run built around creator-led product proof.",
            "why": "Worked because the creator framed the product through a specific family persona.",
            "common_pattern": ["satire / parody", "Devil-Wears-Prada parody", "generic proof frame"],
            "do_next": "Kill office testimonials. Build creator-led sketches.",
            "watchout": "Persona proof collapses when the product has no job.",
            "per_post_notes": ["Eldest daughter trope, product demonstration"],
            "pattern_type": "feed_aligned",
        }

        errors = si._card_schema_errors(card)

        self.assertIn("common_pattern_0_slash", errors)
        self.assertIn("common_pattern_1_proper_noun", errors)

    def test_static_language_is_rejected_outside_static_lane(self) -> None:
        card = {
            "title": "Caption hook landed cleanly.",
            "what_happened": "Reach rose sharply on creator-led reel proof.",
            "why": "Worked because the creator framed the product through a specific family persona.",
            "common_pattern": ["creator-led product proof", "family persona skit"],
            "do_next": "Kill static setups. Build creator-led sketches.",
            "watchout": "Persona proof collapses when the product has no job.",
            "per_post_notes": ["Eldest daughter trope, product demonstration"],
            "pattern_type": "feed_aligned",
        }

        errors = si._card_context_errors(
            card,
            {"signal_type": "OWN_BREAKOUT", "media_type": "reel"},
            [{"media_type": "reel"}],
        )

        self.assertIn("do_next_static_without_static_lane", errors)

    def test_tag_diversity_rejects_fourth_repeat(self) -> None:
        card = {"common_pattern": ["creator-led product proof"]}
        recent = [{"common_pattern": ["creator-led product proof"]} for _ in range(3)]

        errors = si._tag_diversity_errors(card, recent)

        self.assertIn("common_pattern_0_tag_reused", errors)


if __name__ == "__main__":
    unittest.main()
