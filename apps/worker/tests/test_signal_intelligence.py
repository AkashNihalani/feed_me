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
            "title": "polish became bait",
            "metric_line": "6 matching reels · avg top 9% D7 · pressure holding",
            "read": (
                "The polished setup still earns attention. The stronger reels separate later. "
                "Recent winners interrupt the bridal or studio fantasy almost immediately with sarcasm, embarrassment, gossip, or commentary that feels slightly too casual for the setting itself. "
                "The audience gets pulled into the situation instead of watching it safely. Earlier uploads asked to be admired; the newer winners invite judgment, quoting, or reaction instead. "
                "Visually similar choreography-heavy reels still pull reach, but once the frame stays emotionally composed, replies soften much earlier."
            ),
            "evidence_pressure": [
                "Studio embarrassment beat cleaner performance-led uploads despite weaker visual payoff.",
                "Wedding gossip kept the frame socially active longer than clean event documentation.",
            ],
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
            "metric_line": "3 posts · top 9% D7 · pressure holding",
            "evidence_pressure": [
                "Studio embarrassment beat cleaner uploads.",
                "Wedding gossip stayed socially active longer.",
            ],
            "signal_type": "OWN_BREAKOUT",
        }

        normalized = si._normalize_card_tag_aliases(card)
        errors = si._card_schema_errors(normalized)

        self.assertNotIn("missing_read", errors)
        self.assertIn("read_contains_internal_vocab", errors)
        self.assertNotIn("missing_do_next", errors)

    def test_normalizer_coerces_object_post_notes_and_clamps_copy(self) -> None:
        card = {
            "title": "Direct persona payoff landed harder than expected today",
            "read": " ".join(["proof"] * 120),
            "metric_line": " ".join(["metric"] * 30),
            "evidence_pressure": [
                {
                    "post_key": "post#1",
                    "role": "Pick",
                    "note": " ".join(["this"] * 40),
                },
                "Clean comparison stayed flatter after the setup landed.",
            ],
            "signal_type": "OWN_SUSTAIN",
        }

        normalized = si._normalize_card_tag_aliases(card)

        self.assertEqual(si._card_schema_errors(normalized), [])
        self.assertEqual(len(normalized["evidence_pressure"]), 2)
        self.assertIsInstance(normalized["evidence_pressure"][0], str)
        self.assertLessEqual(si._word_count(normalized["evidence_pressure"][0]), 24)
        self.assertLessEqual(si._word_count(normalized["metric_line"]), 16)

    def test_weak_analyst_vocab_is_rejected(self) -> None:
        card = {
            "title": "relatable story held d7",
            "metric_line": "3 posts · top 9% D7 · pressure holding",
            "read": "The posts sustained performance through relatable storytelling and high-conflict reactionary formats. The weak comparison posts tried the same topic without the same scene pressure, so the viewer stayed outside the moment instead of being pulled into it.",
            "evidence_pressure": [
                "Premium setting broke into casual behavior.",
                "Clean presentation stayed flatter.",
            ],
            "signal_type": "OWN_SUSTAIN",
        }

        errors = si._card_schema_errors(card)

        self.assertIn("title_contains_weak_analyst_vocab", errors)
        self.assertIn("read_contains_weak_analyst_vocab", errors)

    def test_guardrail_repair_replaces_internal_and_weak_terms(self) -> None:
        card = {
            "title": "signal from relatable storytelling",
            "metric_line": "3 triggers · top 9% D7 · pressure holding",
            "read": "The signal shows engagement rising through relatable storytelling and aesthetic showcases. The weaker posts stay clean, so the audience never has to take a position before the scene finishes explaining itself.",
            "evidence_pressure": [
                "Fingerprint shows a high-conflict post.",
                "Clean upload stayed flatter.",
            ],
            "signal_type": "OWN_SUSTAIN",
        }

        repaired = si._normalize_card_tag_aliases(
            si._repair_guardrail_terms(card, si._card_schema_errors(card))
        )
        errors = si._card_schema_errors(repaired)

        self.assertNotIn("read_contains_internal_vocab", errors)
        self.assertNotIn("read_contains_weak_analyst_vocab", errors)
        self.assertLessEqual(si._word_count(repaired["metric_line"]), 16)

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
            "metric_line": "3 posts · top 9% D7 · pressure holding",
            "evidence_pressure": [
                "Creator-led proof beat static setups.",
                "Product-only footage stayed flatter.",
            ],
            "signal_type": "OWN_BREAKOUT",
        }

        errors = si._card_context_errors(
            card,
            {"signal_type": "OWN_BREAKOUT", "media_type": "reel"},
            [{"media_type": "reel"}],
        )

        self.assertIn("evidence_pressure[0]_static_without_static_lane", errors)


if __name__ == "__main__":
    unittest.main()
