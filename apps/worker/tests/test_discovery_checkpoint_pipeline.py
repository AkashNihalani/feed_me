from datetime import date, datetime, timezone
import os
import pathlib
import sys
import unittest


WORKER_ROOT = pathlib.Path(__file__).resolve().parents[1]
os.environ.setdefault("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/db")
os.environ.setdefault("BRIGHTDATA_API_KEY", "test")
if str(WORKER_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKER_ROOT))

from app.pure_engine import PureEngine, _usable_discovery_items  # noqa: E402


class _Rows:
    def __init__(self, rows=None):
        self._rows = rows or []

    def fetchall(self):
        return self._rows


class _Connection:
    def __init__(self):
        self.completed = []
        self.skipped = []

    def execute(self, query, params=None):
        if "select id, checkpoint" in query:
            return _Rows([
                {"id": 12, "checkpoint": "d3", "attempt": 0, "next_run_at": datetime.now(timezone.utc)},
                {"id": 11, "checkpoint": "d1", "attempt": 0, "next_run_at": datetime.now(timezone.utc)},
            ])
        if "update public.checkpoint_jobs" in query:
            if "superseded:" in query:
                self.skipped.append(params[0])
            else:
                self.completed.append(params[0])
        return _Rows()


class DiscoveryCheckpointPipelineTests(unittest.TestCase):
    def test_discovery_payload_completes_latest_due_checkpoint_without_views(self):
        engine = PureEngine.__new__(PureEngine)
        engine.conn = _Connection()
        written = []
        engine._checkpoint_business_day_for_job = lambda *_: date(2026, 7, 15)
        engine._upsert_metric = lambda *args, **kwargs: written.append((args, kwargs)) or True

        touched = engine._satisfy_due_checkpoints_from_discovery(
            7,
            "7:post",
            datetime(2026, 7, 14, tzinfo=timezone.utc),
            {"likesCount": 100, "commentsCount": 5, "ownerFollowersCount": 1000},
            1000,
        )

        self.assertEqual([entry[0][1] for entry in written], ["d3"])
        self.assertTrue(all(entry[0][2] is None for entry in written))
        self.assertEqual(engine.conn.skipped, [11])
        self.assertEqual(engine.conn.completed, [12])
        self.assertEqual(touched, {(7, "d3", date(2026, 7, 15))})

    def test_empty_or_error_discovery_is_not_usable(self):
        self.assertEqual(_usable_discovery_items([]), ([], []))
        usable, errors = _usable_discovery_items([{"error": "temporary provider failure"}])
        self.assertEqual(usable, [])
        self.assertEqual(errors, ["temporary provider failure"])

    def test_migration_schedules_two_discoveries_and_all_checkpoints(self):
        migration = (
            WORKER_ROOT.parents[1]
            / "infra/supabase/migrations/20260715061500_two_discovery_checkpoint_pipeline.sql"
        ).read_text()
        self.assertIn("('d1'::text, 1), ('d3'::text, 3), ('d7'::text, 7), ('d21'::text, 21)", migration)
        self.assertIn("'feedme_discovery_1130_ist'", migration)
        self.assertIn("'feedme_discovery_2330_ist'", migration)
        self.assertIn("time '11:30'", migration)
        self.assertIn("time '23:30'", migration)
        self.assertIn("interval '1 day 12 hours'", migration)
        self.assertIn("drop trigger if exists trg_manage_hot_d21_checkpoint", migration)


if __name__ == "__main__":
    unittest.main()
