from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

os.environ.setdefault("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/db")
os.environ.setdefault("BRIGHTDATA_API_KEY", "test")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.fingerprint_intelligence import _yaml_post_condensation_from_text  # noqa: E402


class PostCondensationValidationTest(unittest.TestCase):
    def test_word_count_drift_is_accepted_with_warning(self) -> None:
        fingerprint = {
            "post_key": "p/server",
            "caption": "original caption",
            "transcript": " ".join(["spoken"] * 190),
            "visual_sequence": [{"timestamp_range": "00:00-00:03", "description": "x"} for _ in range(8)],
            "audio_behavior": [],
        }
        reel = " ".join(["detail"] * 292)
        raw = f"""
post_condensed:
  post_key: p/server
  caption: |-
    original caption
  reel: |-
    {reel}
  standout_details:
    - one concrete moment
    - another concrete moment
"""

        parsed = _yaml_post_condensation_from_text(raw, post_key="p/server", fingerprint=fingerprint)

        self.assertIsNotNone(parsed)
        validation = parsed["server_validation"]
        self.assertTrue(validation["accepted"])
        self.assertEqual(validation["reel_word_count"], 292)
        self.assertIn("reel_word_count_over_target_within_tolerance", validation["warnings"])
        self.assertEqual(parsed["post_condensed"]["post_key"], "p/server")

    def test_model_post_key_is_overridden_instead_of_rejecting_output(self) -> None:
        raw = """
post_condensed:
  post_key: p/model-made-a-mistake
  caption: |-
    compact caption
  reel: |-
    The reel opens with a person holding a sign, then cuts to a second person reacting.
  standout_details:
    - The sign appears before the reaction cut.
"""

        parsed = _yaml_post_condensation_from_text(raw, post_key="p/server-truth", fingerprint={})

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["post_condensed"]["post_key"], "p/server-truth")
        self.assertIn("post_key_overridden_to_server_post_key", parsed["server_validation"]["warnings"])

    def test_lenient_recovery_handles_unquoted_colons_in_list_items(self) -> None:
        raw = """
post_condensed:
  post_key: p/server
  caption: |-
    caption
  reel: |-
    A speaker points to text on screen, pauses, and then the end card appears.
  standout_details:
    - Text appears as label: the exact timing is visible
    - End card: green background with campaign copy
"""

        parsed = _yaml_post_condensation_from_text(raw, post_key="p/server", fingerprint={})

        self.assertIsNotNone(parsed)
        self.assertEqual(len(parsed["post_condensed"]["standout_details"]), 2)
        self.assertTrue(parsed["server_validation"]["accepted"])

    def test_meta_is_preserved_from_v5_output(self) -> None:
        raw = """
post_condensed:
  post_key: p/server
  meta:
    duration_seconds: 42
    media_type: reel
    media_truncated: false
    observed_window: "0:00-0:42"
  caption: |-
    caption
  reel: |-
    A speaker opens to camera, points at a product, and cuts to a final end card.
  standout_details:
    - The product appears before the final end card.
"""

        parsed = _yaml_post_condensation_from_text(raw, post_key="p/server", fingerprint={})

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["post_condensed"]["meta"]["duration_seconds"], 42)
        self.assertEqual(parsed["post_condensed"]["meta"]["media_type"], "reel")
        self.assertFalse(parsed["post_condensed"]["meta"]["media_truncated"])
        self.assertEqual(parsed["post_condensed"]["meta"]["observed_window"], "0:00-0:42")

    def test_dict_list_item_is_coerced_to_readable_detail(self) -> None:
        raw = """
post_condensed:
  post_key: p/server
  caption: |-
    caption
  reel: |-
    A speaker cuts between three looks and ends by waving at the camera.
  standout_details:
    - {"The repeated comparison is the joke": "each cut frames a different object as the same trend"}
"""

        parsed = _yaml_post_condensation_from_text(raw, post_key="p/server", fingerprint={})

        self.assertIsNotNone(parsed)
        self.assertEqual(
            parsed["post_condensed"]["standout_details"][0],
            "The repeated comparison is the joke: each cut frames a different object as the same trend",
        )


if __name__ == "__main__":
    unittest.main()
