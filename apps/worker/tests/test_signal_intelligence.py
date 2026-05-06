from __future__ import annotations

import os
import sys
import types
import unittest
from datetime import date
from pathlib import Path

os.environ.setdefault("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/db")
os.environ.setdefault("BRIGHTDATA_API_KEY", "test")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

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


class SignalIntelligenceV4CardContractTest(unittest.TestCase):
    def test_metric_classification_describes_follower_drop(self) -> None:
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

    def test_v4_card_contract_accepts_rulebook_card(self) -> None:
        card = {
            "title": "Confession ran ahead.",
            "read": "Premise landed in the first line, then the single cut held the emotional turn. The rulebook already had confession-led reels as the account's strongest comment driver, and this post stayed inside that lane without adding production noise.",
            "do_next": "Keep the bare voice memo structure. Rotate the subject, not the format.",
            "watchout": "Adding overlays would turn the confession into an explainer.",
            "per_post_notes": [
                "Pick: post#9821 opened with admission and closed with permission.",
                "Contrast: post#9520 used numbered tutorial framing.",
            ],
            "pattern_type": "account_aligned",
            "signal_type": "OWN_BREAKOUT_EARLY",
        }

        self.assertEqual(si._card_schema_errors(card), [])

    def test_v4_card_contract_rejects_old_tag_schema_and_internal_vocab(self) -> None:
        card = {
            "title": "Cross-Follower Movement Surged Hard",
            "what_happened": "The feed_focus bible says this signal created a cohort shift.",
            "why": "Worked because the fingerprint matched the memory candidate.",
            "mechanic_tags": [{"tag": "confession hooks"}],
            "common_pattern": ["confession hooks"],
            "do_next": "Repeat it.",
            "watchout": "The signal may keep moving.",
            "per_post_notes": [],
            "pattern_type": "account_aligned",
            "signal_type": "OWN_BREAKOUT",
        }

        normalized = si._normalize_card_tag_aliases(card)
        errors = si._card_schema_errors(normalized)

        self.assertNotIn("missing_read", errors)
        self.assertIn("read_contains_internal_vocab", errors)
        self.assertNotIn("missing_mechanic_tags", errors)

    def test_normalizer_coerces_object_post_notes_and_clamps_copy(self) -> None:
        card = {
            "title": "Direct persona payoff landed harder than expected today",
            "read": " ".join(["proof"] * 120),
            "do_next": " ".join(["repeat"] * 40),
            "watchout": " ".join(["avoid"] * 35),
            "per_post_notes": [
                {
                    "post_key": "post#1",
                    "role": "Pick",
                    "note": " ".join(["this"] * 40),
                }
            ],
            "pattern_type": "account_aligned",
            "signal_type": "OWN_SUSTAIN",
        }

        normalized = si._normalize_card_tag_aliases(card)

        self.assertEqual(si._card_schema_errors(normalized), [])
        self.assertEqual(len(normalized["per_post_notes"]), 1)
        self.assertIsInstance(normalized["per_post_notes"][0], str)
        self.assertLessEqual(si._word_count(normalized["per_post_notes"][0]), 24)
        self.assertTrue(normalized["read"].endswith("..."))

    def test_weak_analyst_vocab_is_rejected(self) -> None:
        card = {
            "title": "Relatable storytelling held d7",
            "read": "The posts sustained performance through relatable storytelling and high-conflict reactionary formats.",
            "do_next": "Keep leaning into personality-driven setups.",
            "watchout": "Avoid generic aesthetic showcases.",
            "per_post_notes": ["Pick: premium setting broken by casual behavior."],
            "pattern_type": "account_aligned",
            "signal_type": "OWN_SUSTAIN",
        }

        errors = si._card_schema_errors(card)

        self.assertIn("title_contains_weak_analyst_vocab", errors)
        self.assertIn("read_contains_weak_analyst_vocab", errors)
        self.assertIn("do_next_contains_weak_analyst_vocab", errors)

    def test_guardrail_repair_replaces_internal_and_weak_terms(self) -> None:
        card = {
            "title": "Signal from relatable storytelling",
            "read": "The signal shows engagement rising through relatable storytelling and aesthetic showcases.",
            "do_next": "Keep leaning into high-conflict reactionary formats with a lot of extra detail that should be trimmed before validation accepts the card output.",
            "watchout": "Avoid humble setups.",
            "per_post_notes": ["Fingerprint shows a high-conflict post."],
            "pattern_type": "account_aligned",
            "signal_type": "OWN_SUSTAIN",
        }

        repaired = si._normalize_card_tag_aliases(
            si._repair_guardrail_terms(card, si._card_schema_errors(card))
        )
        errors = si._card_schema_errors(repaired)

        self.assertNotIn("read_contains_internal_vocab", errors)
        self.assertNotIn("read_contains_weak_analyst_vocab", errors)
        self.assertLessEqual(si._word_count(repaired["do_next"]), 32)

    def test_server_confidence_uses_fingerprinted_evidence_not_focus_memory(self) -> None:
        confidence = si._computed_confidence(
            {"business_date_ist": date.today()},
            [
                {
                    "cohort": "a",
                    "post_key": f"post-{index}",
                    "fingerprint": {"synthesis": {"craft": "single-take reel"}},
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

    def test_static_language_is_rejected_outside_static_lane(self) -> None:
        card = {
            "title": "Caption hook landed cleanly.",
            "read": "Reach rose sharply on creator-led reel proof.",
            "do_next": "Kill static setups. Build creator-led sketches.",
            "watchout": "Persona proof collapses when the product has no job.",
            "per_post_notes": ["Pick: creator-led product demonstration."],
            "pattern_type": "feed_aligned",
            "signal_type": "OWN_BREAKOUT",
        }

        errors = si._card_context_errors(
            card,
            {"signal_type": "OWN_BREAKOUT", "media_type": "reel"},
            [{"media_type": "reel"}],
        )

        self.assertIn("do_next_static_without_static_lane", errors)


if __name__ == "__main__":
    unittest.main()
