from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import intelligence_engine_prompts as prompts  # noqa: E402


class IntelligencePipelineLockTest(unittest.TestCase):
    def test_locked_engine_has_only_final_prompts_and_state(self) -> None:
        self.assertEqual(
            set(prompts.LOCKED_INTELLIGENCE_ENGINE),
            {"fingerprint", "reel_card_extractor", "d7_read", "bite_run", "state_update"},
        )
        self.assertEqual(
            prompts.LOCKED_INTELLIGENCE_ENGINE["fingerprint"]["version"],
            prompts.FINGERPRINT_PROMPT_VERSION,
        )

    def test_reel_cards_are_ten_at_a_time_and_map_six_fields(self) -> None:
        self.assertEqual(prompts.REEL_CARD_BATCH_SIZE, 10)
        self.assertEqual(prompts.FEEDER_FILE_MAX_REEL_CARDS, 30)
        self.assertEqual(len(prompts.REEL_CARD_PAYLOAD_FIELDS), 6)
        self.assertEqual(
            tuple(prompts.REEL_CARD_FIELD_MAP),
            prompts.REEL_CARD_PAYLOAD_FIELDS,
        )
        self.assertEqual(
            prompts.LOCKED_INTELLIGENCE_ENGINE["reel_card_extractor"]["version"],
            prompts.REEL_CONTEXT_EXTRACTOR_PROMPT_VERSION,
        )
        self.assertIn("completed D7", prompts.LOCKED_INTELLIGENCE_ENGINE["reel_card_extractor"]["cadence"])

    def test_d7_read_uses_the_locked_postmortem_prompt(self) -> None:
        self.assertIn("D7 READ - POST MORTEM", prompts.D7_READ_SYSTEM)
        self.assertFalse(hasattr(prompts, "D7_READ_SYSTEM_V6"))
        self.assertFalse(hasattr(prompts, "D7_READ_SYSTEM_V16"))
        self.assertFalse(hasattr(prompts, "D7_POSTMORTEM_SYSTEM_LIVE_V1"))
        self.assertEqual(
            prompts.LOCKED_INTELLIGENCE_ENGINE["d7_read"]["version"],
            prompts.D7_READ_PROMPT_VERSION,
        )
        self.assertIn("current feeder_file", prompts.LOCKED_INTELLIGENCE_ENGINE["d7_read"]["cadence"])

    def test_feeder_file_stays_newest_thirty_after_initial_ten(self) -> None:
        state = prompts.LOCKED_INTELLIGENCE_ENGINE["state_update"]
        self.assertIn("first 10", state["initial_feeder_file"])
        self.assertIn("append latest 10", state["after_reel_card_batch"])
        self.assertIn("newest 30", state["after_reel_card_batch"])
        self.assertIn("newest 30 always stay", state["capacity"])

    def test_bite_run_is_latest_ten_against_existing_thirty(self) -> None:
        bite_run = prompts.LOCKED_INTELLIGENCE_ENGINE["bite_run"]
        self.assertEqual(bite_run["version"], prompts.BITE_RUN_PROMPT_VERSION)
        self.assertEqual(bite_run["system"], "BITE_RUN_SYSTEM")
        self.assertEqual(bite_run["output_fields"][0], "headline")
        self.assertIn("n01-n10", bite_run["payload"]["new_drop"])
        self.assertIn("m01-m30", bite_run["payload"]["feeder_memory"])
        self.assertIn("rank and landing read", bite_run["payload"]["performance"])

    def test_final_bite_run_prompt_requires_headline(self) -> None:
        self.assertIn('"headline": ""', prompts.BITE_RUN_SYSTEM)
        self.assertLess(
            prompts.BITE_RUN_SYSTEM.index("headline\n5-8 words."),
            prompts.BITE_RUN_SYSTEM.index("reader_report\n20-30 words."),
        )
        self.assertIn("what this run exposed about the feeder", prompts.BITE_RUN_SYSTEM)

    def test_old_bite_memory_prompts_are_not_in_the_final_lock(self) -> None:
        lock_text = repr(prompts.LOCKED_INTELLIGENCE_ENGINE)
        for dead_prompt in (
            "post_condensation_v5_character_transfer",
            "feeder_file_chunk_v1",
            "feeder_file_merge_decision_v1",
            "feeder_file_cold_start_v8_1",
        ):
            self.assertNotIn(dead_prompt, lock_text)


if __name__ == "__main__":
    unittest.main()
