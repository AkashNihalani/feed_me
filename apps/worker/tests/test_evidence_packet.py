from __future__ import annotations

import os
import sys
import types
import unittest
from pathlib import Path

os.environ.setdefault("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/db")
os.environ.setdefault("BRIGHTDATA_API_KEY", "test")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    import psycopg.rows  # noqa: F401
except ModuleNotFoundError:
    psycopg = types.ModuleType("psycopg")
    rows = types.ModuleType("psycopg.rows")
    rows.dict_row = None
    psycopg.rows = rows
    sys.modules["psycopg"] = psycopg
    sys.modules["psycopg.rows"] = rows

from app import evidence_packet as ep  # noqa: E402


def row(
    post_key: str,
    percentile: float,
    *,
    media_type: str = "reel",
    comments_x: float = 1.0,
    likes_x: float = 1.0,
    views_x: float = 1.0,
    source_signal_types: list[str] | None = None,
    d3: float | None = None,
    d21: float | None = None,
) -> dict[str, object]:
    return {
        "post_key": post_key,
        "media_type": media_type,
        "caption": f"caption {post_key}",
        "checkpoint_metrics": {
            "d3": {"percentile_performance": d3 if d3 is not None else percentile},
            "d7": {
                "percentile_performance": percentile,
                "comments_multiple": comments_x,
                "likes_multiple": likes_x,
                "views_multiple": views_x,
            },
            "d21": {"percentile_performance": d21 if d21 is not None else percentile},
        },
        "source_signal_types": source_signal_types or [],
        "fingerprint": {"synthesis": {"craft": "test craft"}},
    }


class EvidencePacketSelectionTest(unittest.TestCase):
    def test_signal_residual_keeps_alerted_posts_outside_top_bottom_and_anomaly(self) -> None:
        rows = [row(f"top-{idx}", idx + 1, comments_x=1.0) for idx in range(8)]
        rows.extend(row(f"bottom-{idx}", 90 + idx, comments_x=1.0) for idx in range(8))
        residual = row("residual-comment", 42, comments_x=1.0, source_signal_types=["OWN_COMMENT_SPIKE"])
        rows.append(residual)

        selected = ep.select_packet_posts(rows, hard_cap=12, max_top_bottom_per_format=4)
        by_key = {item["post_key"]: item for item in selected}

        self.assertIn("residual-comment", by_key)
        self.assertEqual(by_key["residual-comment"]["group"], "signal_residual")
        self.assertIn("S_COMMENT_SPIKE", by_key["residual-comment"]["signal_types"])

    def test_annotations_are_checkpoint_aware(self) -> None:
        evergreen = row("evergreen", 44, d3=70, d21=18)
        early = row("early", 30, d3=4, d21=50)
        false_dawn = row("false-dawn", 8, d3=10, d21=40)

        self.assertIn("L_EVERGREEN", ep.annotate_signal_types(evergreen))
        self.assertIn("T_EARLY", ep.annotate_signal_types(early))
        self.assertIn("T_FALSE_DAWN", ep.annotate_signal_types(false_dawn))

    def test_view_outlier_is_reel_only(self) -> None:
        rows = [
            row("carousel-view", 50, media_type="sidecar", views_x=5.0),
            row("reel-view", 51, media_type="reel", views_x=5.0),
        ]

        selected = ep.select_packet_posts(rows, hard_cap=10, max_top_bottom_per_format=1)
        groups = {item["post_key"]: item["groups"] for item in selected}

        self.assertNotIn("view_outlier", groups["carousel-view"])
        self.assertIn("view_outlier", groups["reel-view"])


if __name__ == "__main__":
    unittest.main()
