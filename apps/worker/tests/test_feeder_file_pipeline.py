from __future__ import annotations

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

os.environ.setdefault("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/db")
os.environ.setdefault("BRIGHTDATA_API_KEY", "test")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.feeder_file_pipeline import _canonicalize_patterns, _select_active_post_memories  # noqa: E402


class FeederFileCanonicalizationTest(unittest.TestCase):
    def test_post_key_wins_when_member_index_points_elsewhere(self) -> None:
        rows = [
            {"post_key": "p/ac-scalp"},
            {"post_key": "p/smoking"},
            {"post_key": "p/poo-elimination"},
            {"post_key": "p/office-deadline"},
        ]
        feed_file = {
            "patterns": [
                {
                    "pattern_id": "hidden-threat",
                    "members": [
                        {
                            "member_index": 2,
                            "post_key": "p/smoking",
                            "fit_type": "core",
                        }
                    ],
                },
                {
                    "pattern_id": "deadline-team-sport",
                    "members": [
                        {
                            "member_index": 3,
                            "post_key": "p/office-deadline",
                            "fit_type": "core",
                        }
                    ],
                },
            ]
        }

        result = _canonicalize_patterns(feed_file, rows)

        hidden_member = result["patterns"][0]["members"][0]
        deadline_member = result["patterns"][1]["members"][0]
        repairs = result["server_validation"]["member_mapping_repairs"]

        self.assertEqual(hidden_member["post_key"], "p/smoking")
        self.assertEqual(hidden_member["member_index"], 1)
        self.assertEqual(deadline_member["post_key"], "p/office-deadline")
        self.assertEqual(deadline_member["member_index"], 3)
        self.assertEqual(repairs[0]["index_resolved_to"], "p/poo-elimination")
        self.assertEqual(repairs[0]["post_key_resolved_to"], "p/smoking")
        self.assertEqual(repairs[0]["kept"], "post_key")

    def test_recent_posts_fill_unearned_winner_slots_before_recent_context(self) -> None:
        base = datetime(2026, 5, 1, tzinfo=timezone.utc)
        rows = [
            {
                "post_key": f"p/{index}",
                "posted_at": base + timedelta(days=index),
                "percentile_performance_exact": 18 if index <= 7 else 45,
            }
            for index in range(1, 31)
        ]

        selected = _select_active_post_memories(rows)

        self.assertEqual(len(selected), 30)
        counts: dict[str, int] = {}
        for row in selected:
            memory_type = row["post_memory"]["memory_type"]
            counts[memory_type] = counts.get(memory_type, 0) + 1

        self.assertEqual(counts["ranked_winner"], 7)
        self.assertEqual(counts["recent_fill"], 13)
        self.assertEqual(counts["recent_context"], 10)
        self.assertTrue(all(row["post_memory"]["winner"] for row in selected[:7]))
        self.assertFalse(any(row["post_memory"]["winner"] for row in selected[7:20]))
        self.assertEqual(selected[7]["post_memory"]["occupying_slot"], "ranked_winner_slot")

    def test_mature_feeder_keeps_twenty_winners_plus_ten_recent_context(self) -> None:
        base = datetime(2026, 5, 1, tzinfo=timezone.utc)
        rows = [
            {
                "post_key": f"p/{index}",
                "posted_at": base + timedelta(days=index),
                "percentile_performance_exact": 10 + (index / 10),
            }
            for index in range(1, 26)
        ] + [
            {
                "post_key": f"p/{index}",
                "posted_at": base + timedelta(days=index),
                "percentile_performance_exact": 50,
            }
            for index in range(26, 41)
        ]

        selected = _select_active_post_memories(rows)

        counts: dict[str, int] = {}
        for row in selected:
            memory_type = row["post_memory"]["memory_type"]
            counts[memory_type] = counts.get(memory_type, 0) + 1

        self.assertEqual(len(selected), 30)
        self.assertEqual(counts["ranked_winner"], 20)
        self.assertNotIn("recent_fill", counts)
        self.assertEqual(counts["recent_context"], 10)
        self.assertEqual(selected[20]["post_memory"]["occupying_slot"], "recent_context_slot")


if __name__ == "__main__":
    unittest.main()
