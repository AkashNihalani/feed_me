import re
import contextlib
import json
import hashlib
from datetime import datetime, timezone, timedelta
import psycopg
from psycopg.rows import dict_row

from .config import POSTGRES_DSN, SPREADSHEET_ID

@contextlib.contextmanager
def get_conn():
    with psycopg.connect(POSTGRES_DSN, row_factory=dict_row) as conn:
        yield conn


def init_db():
    with get_conn() as conn:
        # Pre-migration guard for existing DBs: ensure alert_dedupe_key exists
        # before schema.sql attempts to create a partial index that references it.
        conn.execute(
            """
            DO $$
            BEGIN
              IF EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema='public' AND table_name='alert_candidates'
              ) THEN
                ALTER TABLE alert_candidates
                ADD COLUMN IF NOT EXISTS alert_dedupe_key TEXT DEFAULT '';
              END IF;
            END $$;
            """
        )

        # Create/upgrade base schema first so ALTER statements below have tables available.
        with open("/app/app/schema.sql", "r", encoding="utf-8") as f:
            conn.execute(f.read())

        # Migrations for existing DBs (ensure columns exist before index creation)
        conn.execute("ALTER TABLE run_queue ADD COLUMN IF NOT EXISTS subscriber_id BIGINT")
        conn.execute("ALTER TABLE run_queue ADD COLUMN IF NOT EXISTS spreadsheet_id TEXT")
        conn.execute("ALTER TABLE handle_state ADD COLUMN IF NOT EXISTS subscriber_id BIGINT")
        conn.execute("ALTER TABLE post_snapshots ADD COLUMN IF NOT EXISTS media_type TEXT")
        conn.execute("ALTER TABLE post_snapshots ADD COLUMN IF NOT EXISTS d1_at TIMESTAMPTZ")
        conn.execute("ALTER TABLE post_snapshots ADD COLUMN IF NOT EXISTS d2_at TIMESTAMPTZ")
        conn.execute("ALTER TABLE post_snapshots ADD COLUMN IF NOT EXISTS d1_views INT")
        conn.execute("ALTER TABLE post_snapshots ADD COLUMN IF NOT EXISTS d2_views INT")
        conn.execute("ALTER TABLE post_snapshots ADD COLUMN IF NOT EXISTS d1_likes INT")
        conn.execute("ALTER TABLE post_snapshots ADD COLUMN IF NOT EXISTS d2_likes INT")
        conn.execute("ALTER TABLE post_snapshots ADD COLUMN IF NOT EXISTS d1_comments INT")
        conn.execute("ALTER TABLE post_snapshots ADD COLUMN IF NOT EXISTS d2_comments INT")
        conn.execute("ALTER TABLE post_embeddings ADD COLUMN IF NOT EXISTS niche_id TEXT")
        conn.execute("ALTER TABLE post_embeddings ADD COLUMN IF NOT EXISTS signal_type TEXT DEFAULT 'caption_semantic'")
        conn.execute("ALTER TABLE post_embeddings ADD COLUMN IF NOT EXISTS signal_version TEXT DEFAULT 'v1'")
        conn.execute("ALTER TABLE post_embeddings ADD COLUMN IF NOT EXISTS metadata_json JSONB DEFAULT '{}'::jsonb")
        conn.execute("ALTER TABLE post_embeddings ADD COLUMN IF NOT EXISTS feed_id BIGINT")
        conn.execute("ALTER TABLE post_embeddings ADD COLUMN IF NOT EXISTS feeder_id BIGINT")
        conn.execute("ALTER TABLE post_signals ADD COLUMN IF NOT EXISTS feed_id BIGINT")
        conn.execute("ALTER TABLE post_signals ADD COLUMN IF NOT EXISTS feeder_id BIGINT")
        conn.execute("ALTER TABLE post_checkpoint_metrics ADD COLUMN IF NOT EXISTS feed_id BIGINT")
        conn.execute("ALTER TABLE post_checkpoint_metrics ADD COLUMN IF NOT EXISTS feeder_id BIGINT")
        conn.execute("ALTER TABLE post_queue ADD COLUMN IF NOT EXISTS subscriber_id BIGINT")
        conn.execute("ALTER TABLE post_queue ADD COLUMN IF NOT EXISTS spreadsheet_id TEXT")
        conn.execute("ALTER TABLE post_queue ADD COLUMN IF NOT EXISTS handle TEXT")
        conn.execute("ALTER TABLE post_queue ADD COLUMN IF NOT EXISTS post_url TEXT")
        conn.execute("ALTER TABLE post_queue ADD COLUMN IF NOT EXISTS checkpoint TEXT")
        conn.execute("ALTER TABLE post_queue ADD COLUMN IF NOT EXISTS requires_d7_hot BOOLEAN DEFAULT FALSE")
        conn.execute("ALTER TABLE post_queue ADD COLUMN IF NOT EXISTS attempt INT DEFAULT 0")
        conn.execute("ALTER TABLE post_queue ADD COLUMN IF NOT EXISTS next_run_at TIMESTAMPTZ")
        conn.execute("ALTER TABLE post_queue ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending'")
        conn.execute("ALTER TABLE post_queue ADD COLUMN IF NOT EXISTS last_error TEXT")
        conn.execute("ALTER TABLE post_queue ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()")
        conn.execute("ALTER TABLE post_queue ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
        conn.execute("UPDATE post_queue SET next_run_at=NOW() WHERE next_run_at IS NULL")
        conn.execute("ALTER TABLE posts_core ADD COLUMN IF NOT EXISTS duration_seconds NUMERIC")
        conn.execute("CREATE TABLE IF NOT EXISTS handle_profile_metrics (id BIGSERIAL PRIMARY KEY, subscriber_id BIGINT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE, handle TEXT NOT NULL, profile_url TEXT, full_name TEXT, business_category TEXT, biography TEXT, followers_count BIGINT, follows_count BIGINT, posts_count BIGINT, verified BOOLEAN, profile_pic_url TEXT, sampled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE (subscriber_id, handle))")
        conn.execute("ALTER TABLE alert_candidates ADD COLUMN IF NOT EXISTS ui_tab TEXT DEFAULT 'flags'")
        conn.execute("ALTER TABLE alert_candidates ADD COLUMN IF NOT EXISTS alert_category TEXT DEFAULT 'velocity'")
        conn.execute("ALTER TABLE alert_candidates ADD COLUMN IF NOT EXISTS alert_color TEXT DEFAULT '#CCFF00'")
        conn.execute("ALTER TABLE alert_candidates ADD COLUMN IF NOT EXISTS alert_urgency TEXT DEFAULT 'today'")
        conn.execute("ALTER TABLE alert_candidates ADD COLUMN IF NOT EXISTS alert_dedupe_key TEXT DEFAULT ''")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS apify_health (
                id INT PRIMARY KEY,
                consecutive_failures INT NOT NULL DEFAULT 0,
                pause_until TIMESTAMPTZ,
                last_error TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        conn.execute(
            """
            INSERT INTO apify_health (id, consecutive_failures, pause_until, last_error, updated_at)
            VALUES (1, 0, NULL, NULL, NOW())
            ON CONFLICT (id) DO NOTHING
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS alert_candidates_dedupe_idx
            ON alert_candidates (feed_id, alert_dedupe_key)
            WHERE alert_dedupe_key <> ''
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS post_queue_next_run_idx
            ON post_queue (next_run_at)
            WHERE status IN ('pending','retry')
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS post_queue_unique_checkpoint
            ON post_queue (subscriber_id, handle, post_url, checkpoint)
            """
        )
        conn.execute(
            "ALTER TABLE post_embeddings DROP CONSTRAINT IF EXISTS post_embeddings_subscriber_id_handle_post_url_embedding_model_key"
        )
        conn.commit()

    ensure_default_subscriber()
    ensure_default_feed()
    _backfill_and_fix_constraints()


def _backfill_and_fix_constraints():
    # Older installs may have created rows before subscriber_id existed.
    # We backfill them into the default subscriber so ON CONFLICT(subscriber_id, handle) works.
    if not SPREADSHEET_ID:
        return
    with get_conn() as conn:
        sub = conn.execute(
            "SELECT id FROM subscribers WHERE spreadsheet_id=%s",
            (SPREADSHEET_ID,),
        ).fetchone()
        if not sub:
            conn.commit()
            return
        sub_id = sub["id"]

        # Backfill NULL subscriber_id/spreadsheet_id rows.
        conn.execute(
            "UPDATE handle_state SET subscriber_id=%s WHERE subscriber_id IS NULL",
            (sub_id,),
        )
        conn.execute(
            "UPDATE run_queue SET subscriber_id=%s WHERE subscriber_id IS NULL",
            (sub_id,),
        )
        conn.execute(
            "UPDATE run_queue SET spreadsheet_id=%s WHERE spreadsheet_id IS NULL",
            (SPREADSHEET_ID,),
        )
        conn.execute(
            "UPDATE run_log SET subscriber_id=%s WHERE subscriber_id IS NULL",
            (sub_id,),
        )
        conn.execute(
            "UPDATE run_log SET spreadsheet_id=%s WHERE spreadsheet_id IS NULL",
            (SPREADSHEET_ID,),
        )

        # Ensure every active subscriber has an active feed.
        conn.execute(
            """
            INSERT INTO feeds (subscriber_id, name, mode, max_feeders, status, created_at, updated_at)
            SELECT s.id, s.name || ' Feed', 'market', 15, 'active', NOW(), NOW()
            FROM subscribers s
            LEFT JOIN feeds f ON f.subscriber_id = s.id
            WHERE f.id IS NULL
            """
        )

        # Backfill feeders from known handles.
        conn.execute(
            """
            INSERT INTO feeders (feed_id, handle, role, status, created_at, updated_at, last_seen_at)
            SELECT f.id, hs.handle, 'standard', 'active', NOW(), NOW(), NOW()
            FROM handle_state hs
            JOIN feeds f ON f.subscriber_id = hs.subscriber_id
            ON CONFLICT (feed_id, handle) DO NOTHING
            """
        )

        # Fill feed/feeder references on historical rows.
        conn.execute(
            """
            UPDATE post_signals ps
            SET feed_id = f.id,
                feeder_id = fd.id
            FROM feeds f
            LEFT JOIN feeders fd ON fd.feed_id = f.id
            WHERE ps.subscriber_id = f.subscriber_id
              AND (fd.handle = ps.handle OR fd.handle IS NULL)
              AND (ps.feed_id IS NULL OR ps.feeder_id IS NULL)
            """
        )
        conn.execute(
            """
            UPDATE post_embeddings pe
            SET feed_id = f.id,
                feeder_id = fd.id
            FROM feeds f
            LEFT JOIN feeders fd ON fd.feed_id = f.id
            WHERE pe.subscriber_id = f.subscriber_id
              AND (fd.handle = pe.handle OR fd.handle IS NULL)
              AND (pe.feed_id IS NULL OR pe.feeder_id IS NULL)
            """
        )
        conn.execute(
            """
            UPDATE post_checkpoint_metrics pcm
            SET feed_id = f.id,
                feeder_id = fd.id
            FROM feeds f
            LEFT JOIN feeders fd ON fd.feed_id = f.id
            WHERE pcm.subscriber_id = f.subscriber_id
              AND (fd.handle = pcm.handle OR fd.handle IS NULL)
              AND (pcm.feed_id IS NULL OR pcm.feeder_id IS NULL)
            """
        )

        # Ensure composite PK exists on handle_state (required for ON CONFLICT in upsert_handle_state).
        conn.execute("ALTER TABLE handle_state DROP CONSTRAINT IF EXISTS handle_state_pkey")
        conn.execute(
            "ALTER TABLE handle_state ADD CONSTRAINT handle_state_pkey PRIMARY KEY (subscriber_id, handle)"
        )

        # Ensure the partial unique index for run_queue exists (required for ON CONFLICT DO NOTHING patterns).
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS run_queue_unique_pending
            ON run_queue (subscriber_id, handle)
            WHERE status IN ('pending','retry')
            """
        )

        conn.commit()


def ensure_default_subscriber():
    if not SPREADSHEET_ID:
        return
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT id FROM subscribers WHERE spreadsheet_id=%s",
            (SPREADSHEET_ID,),
        ).fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO subscribers (name, spreadsheet_id) VALUES (%s, %s)",
                ("Default", SPREADSHEET_ID),
            )
            conn.commit()


def ensure_default_feed():
    with get_conn() as conn:
        rows = conn.execute("SELECT id, name FROM subscribers WHERE status='active'").fetchall()
        for row in rows:
            conn.execute(
                """
                INSERT INTO feeds (subscriber_id, name, mode, max_feeders, status, created_at, updated_at)
                VALUES (%s, %s, 'market', 15, 'active', NOW(), NOW())
                ON CONFLICT (subscriber_id) DO NOTHING
                """,
                (row["id"], f"{row['name']} Feed"),
            )
        conn.commit()


def _get_feed_id(conn, subscriber_id: int) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE subscriber_id=%s AND status='active' ORDER BY id ASC LIMIT 1",
        (subscriber_id,),
    ).fetchone()
    if row:
        return row["id"]
    row = conn.execute(
        """
        INSERT INTO feeds (subscriber_id, name, mode, max_feeders, status, created_at, updated_at)
        VALUES (%s, %s, 'market', 15, 'active', NOW(), NOW())
        RETURNING id
        """,
        (subscriber_id, "Default Feed"),
    ).fetchone()
    return row["id"]


def _get_feeder_id(conn, feed_id: int, handle: str) -> int:
    row = conn.execute(
        "SELECT id FROM feeders WHERE feed_id=%s AND handle=%s LIMIT 1",
        (feed_id, handle),
    ).fetchone()
    if row:
        return row["id"]
    row = conn.execute(
        """
        INSERT INTO feeders (feed_id, handle, role, status, created_at, updated_at, last_seen_at)
        VALUES (%s, %s, 'standard', 'active', NOW(), NOW(), NOW())
        ON CONFLICT (feed_id, handle)
        DO UPDATE SET updated_at=NOW(), last_seen_at=NOW()
        RETURNING id
        """,
        (feed_id, handle),
    ).fetchone()
    return row["id"]


def _get_handle_registry_id(conn, subscriber_id: int, handle: str) -> int:
    row = conn.execute(
        """
        INSERT INTO handle_registry (subscriber_id, handle, status, first_seen_at, last_seen_at)
        VALUES (%s, %s, 'active', NOW(), NOW())
        ON CONFLICT (subscriber_id, handle)
        DO UPDATE SET
            status='active',
            last_seen_at=NOW()
        RETURNING id
        """,
        (subscriber_id, handle),
    ).fetchone()
    return row["id"]


def ensure_feeders_for_subscriber(subscriber_id: int, handles: list[str]):
    with get_conn() as conn:
        feed_id = _get_feed_id(conn, subscriber_id)
        clean = []
        for handle in handles:
            value = (handle or "").strip()
            if not value:
                continue
            clean.append(value)
            _get_feeder_id(conn, feed_id, value)
        conn.execute(
            """
            UPDATE feeders
            SET status='inactive', updated_at=NOW()
            WHERE feed_id=%s
              AND handle <> ALL(%s)
              AND status='active'
            """,
            (feed_id, clean or [""]),
        )
        conn.commit()


def upsert_handle_profile_metric(
    subscriber_id: int,
    handle: str,
    profile_url: str | None,
    full_name: str | None,
    business_category: str | None,
    biography: str | None,
    followers_count: int | None,
    follows_count: int | None,
    posts_count: int | None,
    verified: bool | None,
    profile_pic_url: str | None,
):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO handle_profile_metrics (
                subscriber_id, handle, profile_url, full_name, business_category, biography,
                followers_count, follows_count, posts_count, verified, profile_pic_url, sampled_at, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (subscriber_id, handle)
            DO UPDATE SET
                profile_url = EXCLUDED.profile_url,
                full_name = EXCLUDED.full_name,
                business_category = EXCLUDED.business_category,
                biography = EXCLUDED.biography,
                followers_count = EXCLUDED.followers_count,
                follows_count = EXCLUDED.follows_count,
                posts_count = EXCLUDED.posts_count,
                verified = EXCLUDED.verified,
                profile_pic_url = EXCLUDED.profile_pic_url,
                sampled_at = NOW()
            """,
            (
                subscriber_id,
                handle,
                profile_url,
                full_name,
                business_category,
                biography,
                followers_count,
                follows_count,
                posts_count,
                verified,
                profile_pic_url,
            ),
        )
        conn.commit()


def get_latest_followers(subscriber_id: int, handle: str) -> int | None:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT followers_count
            FROM handle_profile_metrics
            WHERE subscriber_id=%s AND handle=%s
            ORDER BY sampled_at DESC
            LIMIT 1
            """,
            (subscriber_id, handle),
        ).fetchone()
        if not row:
            return None
        value = row.get("followers_count")
        return int(value) if value not in (None, "") else None


def list_feeds():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT f.id, f.subscriber_id, f.name, f.mode, f.max_feeders, f.status, s.spreadsheet_id
            FROM feeds f
            JOIN subscribers s ON s.id = f.subscriber_id
            WHERE f.status='active' AND s.status='active'
            ORDER BY f.id ASC
            """
        ).fetchall()
        return rows


def get_feed_by_subscriber(subscriber_id: int):
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, subscriber_id, name, mode, max_feeders, status
            FROM feeds
            WHERE subscriber_id=%s
            ORDER BY id ASC
            LIMIT 1
            """,
            (subscriber_id,),
        ).fetchone()
        return row


def set_feed_anchor(subscriber_id: int, handle: str | None):
    with get_conn() as conn:
        feed_id = _get_feed_id(conn, subscriber_id)
        conn.execute(
            "UPDATE feeders SET role='standard', updated_at=NOW() WHERE feed_id=%s",
            (feed_id,),
        )
        if handle:
            feeder_id = _get_feeder_id(conn, feed_id, handle)
            conn.execute(
                "UPDATE feeders SET role='anchor', status='active', updated_at=NOW() WHERE id=%s",
                (feeder_id,),
            )
            conn.execute(
                "UPDATE feeds SET mode='anchor', updated_at=NOW() WHERE id=%s",
                (feed_id,),
            )
        else:
            conn.execute(
                "UPDATE feeds SET mode='market', updated_at=NOW() WHERE id=%s",
                (feed_id,),
            )
        conn.commit()


def list_subscribers():
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, name, spreadsheet_id FROM subscribers WHERE status='active' ORDER BY id ASC"
        ).fetchall()
        return rows


def upsert_handle_state(subscriber_id: int, handle: str, sheet_name: str, status: str, last_seen_post_id: str | None, last_error: str | None):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO handle_state (subscriber_id, handle, sheet_name, last_success_at, last_seen_post_id, last_status, last_error, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (subscriber_id, handle)
            DO UPDATE SET
                sheet_name = EXCLUDED.sheet_name,
                last_success_at = EXCLUDED.last_success_at,
                last_seen_post_id = EXCLUDED.last_seen_post_id,
                last_status = EXCLUDED.last_status,
                last_error = EXCLUDED.last_error,
                updated_at = NOW()
            """,
            (
                subscriber_id,
                handle,
                sheet_name,
                datetime.now(timezone.utc) if status == "success" else None,
                last_seen_post_id,
                status,
                last_error,
            ),
        )
        conn.commit()

def enqueue_handle(subscriber_id: int, spreadsheet_id: str, handle: str, run_type: str = "scheduled"):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO run_queue (subscriber_id, spreadsheet_id, handle, run_type, status)
            VALUES (%s, %s, %s, %s, 'pending')
            ON CONFLICT DO NOTHING
            """,
            (subscriber_id, spreadsheet_id, handle, run_type),
        )
        conn.commit()


def ensure_post_checkpoint_jobs(
    subscriber_id: int,
    spreadsheet_id: str,
    handle: str,
    post_url: str,
    posted_at,
):
    if not post_url or not posted_at:
        return
    with get_conn() as conn:
        checkpoints = [
            ("d3", posted_at + timedelta(days=3), False),
            ("d7", posted_at + timedelta(days=7), False),
            ("d21", posted_at + timedelta(days=21), True),
        ]
        for checkpoint, run_at, requires_d7_hot in checkpoints:
            conn.execute(
                """
                INSERT INTO post_queue (
                    subscriber_id, spreadsheet_id, handle, post_url, checkpoint,
                    requires_d7_hot, next_run_at, status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')
                ON CONFLICT (subscriber_id, handle, post_url, checkpoint) DO NOTHING
                """,
                (
                    subscriber_id,
                    spreadsheet_id,
                    handle,
                    post_url,
                    checkpoint,
                    requires_d7_hot,
                    run_at,
                ),
            )
        conn.commit()


def fetch_next_post_job():
    with get_conn() as conn:
        job = conn.execute(
            """
            SELECT *
            FROM post_queue
            WHERE status IN ('pending','retry')
              AND next_run_at <= NOW()
            ORDER BY next_run_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """
        ).fetchone()
        if not job:
            conn.commit()
            return None
        conn.execute(
            "UPDATE post_queue SET status='running', updated_at=NOW() WHERE id=%s",
            (job["id"],),
        )
        conn.commit()
        return job


def mark_post_job_success(job_id: int):
    with get_conn() as conn:
        conn.execute(
            "UPDATE post_queue SET status='done', last_error=NULL, updated_at=NOW() WHERE id=%s",
            (job_id,),
        )
        conn.commit()


def mark_post_job_retry(job_id: int, attempt: int, next_run_at, error: str):
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE post_queue
            SET status='retry', attempt=%s, next_run_at=%s, last_error=%s, updated_at=NOW()
            WHERE id=%s
            """,
            (attempt, next_run_at, error[:1000], job_id),
        )
        conn.commit()


def mark_post_job_failed(job_id: int, error: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE post_queue SET status='failed', last_error=%s, updated_at=NOW() WHERE id=%s",
            (error[:1000], job_id),
        )
        conn.commit()


def get_apify_pause_until():
    with get_conn() as conn:
        row = conn.execute(
            "SELECT pause_until FROM apify_health WHERE id=1"
        ).fetchone()
        conn.commit()
        return row["pause_until"] if row else None


def record_apify_success():
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE apify_health
            SET consecutive_failures=0,
                pause_until=NULL,
                last_error=NULL,
                updated_at=NOW()
            WHERE id=1
            """
        )
        conn.commit()


def record_apify_failure(error: str, trigger_failures: int, cooldown_hours: int):
    with get_conn() as conn:
        row = conn.execute(
            """
            UPDATE apify_health
            SET consecutive_failures = consecutive_failures + 1,
                last_error = %s,
                updated_at = NOW()
            WHERE id=1
            RETURNING consecutive_failures
            """,
            ((error or "")[:1000],),
        ).fetchone()
        failures = int(row["consecutive_failures"]) if row else 0
        pause_until = None
        if failures >= max(1, trigger_failures):
            pause_row = conn.execute(
                """
                UPDATE apify_health
                SET pause_until = NOW() + (%s || ' hours')::interval,
                    consecutive_failures = 0,
                    updated_at = NOW()
                WHERE id=1
                RETURNING pause_until
                """,
                (str(max(1, cooldown_hours)),),
            ).fetchone()
            pause_until = pause_row["pause_until"] if pause_row else None
        conn.commit()
        return failures, pause_until


def mark_post_job_skipped(job_id: int, reason: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE post_queue SET status='skipped', last_error=%s, updated_at=NOW() WHERE id=%s",
            (reason[:1000], job_id),
        )
        conn.commit()


def is_d7_hot(subscriber_id: int, handle: str, post_url: str) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT velocity_tag
            FROM post_signals
            WHERE subscriber_id=%s
              AND handle=%s
              AND post_url=%s
            LIMIT 1
            """,
            (subscriber_id, handle, post_url),
        ).fetchone()
        if not row:
            return False
        tag = row.get("velocity_tag") or ""
        return ("🔥" in tag) or ("🚀" in tag)


def fetch_next_job():
    with get_conn() as conn:
        job = conn.execute(
            """
            SELECT * FROM run_queue
            WHERE status IN ('pending','retry')
              AND next_run_at <= NOW()
            ORDER BY next_run_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """
        ).fetchone()
        if not job:
            conn.commit()
            return None
        conn.execute(
            "UPDATE run_queue SET status='running', updated_at=NOW() WHERE id=%s",
            (job["id"],),
        )
        conn.commit()
        return job


def log_run_start(subscriber_id: int, spreadsheet_id: str, handle: str, run_type: str) -> int:
    with get_conn() as conn:
        row = conn.execute(
            """
            INSERT INTO run_log (subscriber_id, spreadsheet_id, handle, run_type, status)
            VALUES (%s, %s, %s, %s, 'running')
            RETURNING id
            """,
            (subscriber_id, spreadsheet_id, handle, run_type),
        ).fetchone()
        conn.commit()
        return row["id"]


def log_run_finish(
    run_log_id: int,
    status: str,
    apify_items_returned: int,
    posts_upserted_count: int,
    posts_updated_count: int,
    last_error: str | None,
):
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE run_log
            SET status=%s,
                apify_items_returned=%s,
                posts_upserted_count=%s,
                posts_updated_count=%s,
                last_error=%s,
                finished_at=NOW()
            WHERE id=%s
            """,
            (
                status,
                apify_items_returned,
                posts_upserted_count,
                posts_updated_count,
                last_error[:1000] if last_error else None,
                run_log_id,
            ),
        )
        conn.commit()


def upsert_snapshot(
    subscriber_id: int,
    handle: str,
    post_url: str,
    media_type: str | None,
    posted_at,
    checkpoint: str,
    views: int | None,
    likes: int | None,
    comments: int | None,
):
    with get_conn() as conn:
        row = conn.execute(
            """
            INSERT INTO post_snapshots (
                subscriber_id, handle, post_url, media_type, posted_at
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (subscriber_id, handle, post_url) DO NOTHING
            RETURNING id
            """,
            (subscriber_id, handle, post_url, media_type, posted_at),
        ).fetchone()
        # Ensure row exists
        if not row:
            row = conn.execute(
                "SELECT id FROM post_snapshots WHERE subscriber_id=%s AND handle=%s AND post_url=%s",
                (subscriber_id, handle, post_url),
            ).fetchone()
        if not row:
            conn.commit()
            return

        field_map = {
            "d1": ("d1_at", "d1_views", "d1_likes", "d1_comments"),
            "d3": ("d3_at", "d3_views", "d3_likes", "d3_comments"),
            "d7": ("d7_at", "d7_views", "d7_likes", "d7_comments"),
            "d21": ("d21_at", "d21_views", "d21_likes", "d21_comments"),
        }
        if checkpoint not in field_map:
            conn.commit()
            return
        at_col, v_col, l_col, c_col = field_map[checkpoint]
        conn.execute(
            f"""
            UPDATE post_snapshots
            SET media_type = COALESCE(media_type, %s),
                {at_col} = COALESCE({at_col}, NOW()),
                {v_col} = %s,
                {l_col} = %s,
                {c_col} = %s,
                updated_at = NOW()
            WHERE subscriber_id=%s AND handle=%s AND post_url=%s
            """,
            (media_type, views, likes, comments, subscriber_id, handle, post_url),
        )
        conn.commit()


def get_snapshots(subscriber_id: int, handle: str, post_url: str):
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT * FROM post_snapshots
            WHERE subscriber_id=%s AND handle=%s AND post_url=%s
            """,
            (subscriber_id, handle, post_url),
        ).fetchone()
        return row


def upsert_post_signal(
    subscriber_id: int,
    handle: str,
    post_url: str,
    media_type: str | None,
    posted_at,
    caption: str | None,
    velocity_tag: str | None,
    velocity_stage: str | None,
    velocity_percentile: str | None,
):
    with get_conn() as conn:
        feed_id = _get_feed_id(conn, subscriber_id)
        feeder_id = _get_feeder_id(conn, feed_id, handle)
        conn.execute(
            """
            INSERT INTO post_signals (
                subscriber_id, feed_id, feeder_id, handle, post_url, media_type, posted_at, caption,
                velocity_tag, velocity_stage, velocity_percentile, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (subscriber_id, handle, post_url)
            DO UPDATE SET
                feed_id = EXCLUDED.feed_id,
                feeder_id = EXCLUDED.feeder_id,
                media_type = EXCLUDED.media_type,
                posted_at = COALESCE(EXCLUDED.posted_at, post_signals.posted_at),
                caption = EXCLUDED.caption,
                velocity_tag = EXCLUDED.velocity_tag,
                velocity_stage = EXCLUDED.velocity_stage,
                velocity_percentile = EXCLUDED.velocity_percentile,
                updated_at = NOW()
            """,
            (
                subscriber_id,
                feed_id,
                feeder_id,
                handle,
                post_url,
                media_type,
                posted_at,
                caption,
                velocity_tag,
                velocity_stage,
                velocity_percentile,
            ),
        )
        conn.commit()


def upsert_post_core(
    subscriber_id: int,
    handle: str,
    post_url: str,
    media_type: str | None,
    posted_at,
    caption: str | None,
    hashtags: str | None,
    caption_mentions: str | None,
    tagged_users: str | None,
    music_info: str | None,
    is_pinned: bool | None,
    paid_partnership: bool | None,
    sponsors: str | None,
    display_url: str | None,
    video_url: str | None,
    duration_seconds: float | None,
):
    with get_conn() as conn:
        feed_id = _get_feed_id(conn, subscriber_id)
        _get_feeder_id(conn, feed_id, handle)
        handle_registry_id = _get_handle_registry_id(conn, subscriber_id, handle)
        conn.execute(
            """
            INSERT INTO posts_core (
                subscriber_id, handle_id, handle, post_url, media_type, duration_seconds, posted_at,
                caption, hashtags, caption_mentions, tagged_users, music_info, is_pinned,
                paid_partnership, sponsors, display_url, video_url, last_scanned_at, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), NOW())
            ON CONFLICT (subscriber_id, handle, post_url)
            DO UPDATE SET
                handle_id = EXCLUDED.handle_id,
                media_type = EXCLUDED.media_type,
                duration_seconds = EXCLUDED.duration_seconds,
                posted_at = COALESCE(EXCLUDED.posted_at, posts_core.posted_at),
                caption = EXCLUDED.caption,
                hashtags = EXCLUDED.hashtags,
                caption_mentions = EXCLUDED.caption_mentions,
                tagged_users = EXCLUDED.tagged_users,
                music_info = EXCLUDED.music_info,
                is_pinned = EXCLUDED.is_pinned,
                paid_partnership = EXCLUDED.paid_partnership,
                sponsors = EXCLUDED.sponsors,
                display_url = EXCLUDED.display_url,
                video_url = EXCLUDED.video_url,
                last_scanned_at = NOW(),
                updated_at = NOW()
            """,
            (
                subscriber_id,
                handle_registry_id,
                handle,
                post_url,
                media_type,
                duration_seconds,
                posted_at,
                caption,
                hashtags,
                caption_mentions,
                tagged_users,
                music_info,
                is_pinned,
                paid_partnership,
                sponsors,
                display_url,
                video_url,
            ),
        )
        conn.commit()


def list_signal_posts_for_embedding(subscriber_id: int, tags: list[str], limit: int):
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT ps.subscriber_id, ps.feed_id, ps.feeder_id, ps.handle, ps.post_url, ps.media_type, ps.posted_at, ps.caption,
                   ps.velocity_tag, ps.velocity_stage, ps.velocity_percentile,
                   COALESCE(pc.views, 0) AS views,
                   COALESCE(pc.likes, 0) AS likes,
                   COALESCE(pc.comments, 0) AS comments
            FROM post_signals ps
            LEFT JOIN LATERAL (
                SELECT views, likes, comments
                FROM post_checkpoint_metrics pcm
                WHERE pcm.subscriber_id = ps.subscriber_id
                  AND pcm.handle = ps.handle
                  AND pcm.post_url = ps.post_url
                ORDER BY pcm.checkpoint_at DESC
                LIMIT 1
            ) pc ON TRUE
            WHERE ps.subscriber_id = %s
              AND ps.velocity_tag = ANY(%s)
            ORDER BY ps.updated_at DESC
            LIMIT %s
            """,
            (subscriber_id, tags, limit),
        ).fetchall()
        return rows


def embedding_exists(
    subscriber_id: int,
    handle: str,
    post_url: str,
    embedding_model: str,
    signal_type: str,
) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM post_embeddings
            WHERE subscriber_id=%s
              AND handle=%s
              AND post_url=%s
              AND embedding_model=%s
              AND signal_type=%s
            LIMIT 1
            """,
            (subscriber_id, handle, post_url, embedding_model, signal_type),
        ).fetchone()
        return bool(row)


def upsert_post_embedding(
    subscriber_id: int,
    handle: str,
    post_url: str,
    embedding_model: str,
    signal_type: str,
    signal_version: str,
    niche_id: str | None,
    metadata: dict | None,
    source_text: str,
    embedding: list[float],
):
    with get_conn() as conn:
        feed_id = _get_feed_id(conn, subscriber_id)
        feeder_id = _get_feeder_id(conn, feed_id, handle)
        conn.execute(
            """
            INSERT INTO post_embeddings (
                subscriber_id, feed_id, feeder_id, niche_id, handle, post_url, signal_type, signal_version,
                embedding_model, embedding_dim, embedding_json, source_text, metadata_json,
                created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, NOW(), NOW())
            ON CONFLICT (subscriber_id, handle, post_url, embedding_model, signal_type)
            DO UPDATE SET
                feed_id = EXCLUDED.feed_id,
                feeder_id = EXCLUDED.feeder_id,
                niche_id = EXCLUDED.niche_id,
                signal_version = EXCLUDED.signal_version,
                embedding_dim = EXCLUDED.embedding_dim,
                embedding_json = EXCLUDED.embedding_json,
                source_text = EXCLUDED.source_text,
                metadata_json = EXCLUDED.metadata_json,
                updated_at = NOW()
            """,
            (
                subscriber_id,
                feed_id,
                feeder_id,
                niche_id,
                handle,
                post_url,
                signal_type,
                signal_version,
                embedding_model,
                len(embedding),
                json.dumps(embedding),
                source_text,
                json.dumps(metadata or {}),
            ),
        )
        conn.commit()


def upsert_checkpoint_metric(
    subscriber_id: int,
    handle: str,
    post_url: str,
    checkpoint: str,
    stage_label: str,
    views: int | None,
    likes: int | None,
    comments: int | None,
    metric_value: float | None,
    velocity_value: float | None,
    velocity_tag: str | None,
    velocity_percentile: str | None,
    perf_score: str | None,
):
    with get_conn() as conn:
        feed_id = _get_feed_id(conn, subscriber_id)
        feeder_id = _get_feeder_id(conn, feed_id, handle)
        conn.execute(
            """
            INSERT INTO post_checkpoint_metrics (
                subscriber_id, feed_id, feeder_id, handle, post_url, checkpoint, checkpoint_at,
                stage_label, views, likes, comments, metric_value, velocity_value,
                velocity_tag, velocity_percentile, perf_score, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (subscriber_id, handle, post_url, checkpoint)
            DO UPDATE SET
                feed_id = EXCLUDED.feed_id,
                feeder_id = EXCLUDED.feeder_id,
                checkpoint_at = NOW(),
                stage_label = EXCLUDED.stage_label,
                views = EXCLUDED.views,
                likes = EXCLUDED.likes,
                comments = EXCLUDED.comments,
                metric_value = EXCLUDED.metric_value,
                velocity_value = EXCLUDED.velocity_value,
                velocity_tag = EXCLUDED.velocity_tag,
                velocity_percentile = EXCLUDED.velocity_percentile,
                perf_score = EXCLUDED.perf_score
            """,
            (
                subscriber_id,
                feed_id,
                feeder_id,
                handle,
                post_url,
                checkpoint,
                stage_label,
                views,
                likes,
                comments,
                metric_value,
                velocity_value,
                velocity_tag,
                velocity_percentile,
                perf_score,
            ),
        )
        conn.commit()


def mark_job_success(job_id: int):
    with get_conn() as conn:
        conn.execute(
            "UPDATE run_queue SET status='done', last_error=NULL, updated_at=NOW() WHERE id=%s",
            (job_id,),
        )
        conn.commit()


def mark_job_retry(job_id: int, attempt: int, next_run_at, error: str):
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE run_queue
            SET status='retry', attempt=%s, next_run_at=%s, last_error=%s, updated_at=NOW()
            WHERE id=%s
            """,
            (attempt, next_run_at, error[:1000], job_id),
        )
        conn.commit()


def mark_job_failed(job_id: int, error: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE run_queue SET status='failed', last_error=%s, updated_at=NOW() WHERE id=%s",
            (error[:1000], job_id),
        )
        conn.commit()


def refresh_feeder_pair_metrics(feed_id: int, window_days: int = 30):
    with get_conn() as conn:
        anchor = conn.execute(
            """
            SELECT id, handle
            FROM feeders
            WHERE feed_id=%s AND role='anchor' AND status='active'
            LIMIT 1
            """,
            (feed_id,),
        ).fetchone()
        if not anchor:
            conn.execute("DELETE FROM feeder_pair_metrics WHERE feed_id=%s", (feed_id,))
            conn.commit()
            return

        rows = conn.execute(
            """
            SELECT f.id AS feeder_id, f.handle
            FROM feeders f
            WHERE f.feed_id=%s
              AND f.status='active'
              AND f.id <> %s
            """,
            (feed_id, anchor["id"]),
        ).fetchall()

        for row in rows:
            anchor_metrics = conn.execute(
                """
                SELECT COALESCE(AVG(metric_value),0) AS avg_metric,
                       COALESCE(
                         SUM(velocity_value * (1.0 / (1.0 + GREATEST(0.0, EXTRACT(EPOCH FROM (NOW() - checkpoint_at)) / 86400.0))))
                         /
                         NULLIF(SUM(1.0 / (1.0 + GREATEST(0.0, EXTRACT(EPOCH FROM (NOW() - checkpoint_at)) / 86400.0))), 0),
                         0
                       ) AS avg_velocity,
                       COUNT(*) AS n
                FROM post_checkpoint_metrics
                WHERE feed_id=%s
                  AND feeder_id=%s
                  AND checkpoint_at >= NOW() - (%s || ' days')::interval
                """,
                (feed_id, anchor["id"], str(window_days)),
            ).fetchone()
            peer_metrics = conn.execute(
                """
                SELECT COALESCE(AVG(metric_value),0) AS avg_metric,
                       COALESCE(
                         SUM(velocity_value * (1.0 / (1.0 + GREATEST(0.0, EXTRACT(EPOCH FROM (NOW() - checkpoint_at)) / 86400.0))))
                         /
                         NULLIF(SUM(1.0 / (1.0 + GREATEST(0.0, EXTRACT(EPOCH FROM (NOW() - checkpoint_at)) / 86400.0))), 0),
                         0
                       ) AS avg_velocity,
                       COUNT(*) AS n
                FROM post_checkpoint_metrics
                WHERE feed_id=%s
                  AND feeder_id=%s
                  AND checkpoint_at >= NOW() - (%s || ' days')::interval
                """,
                (feed_id, row["feeder_id"], str(window_days)),
            ).fetchone()

            sample_size = int(anchor_metrics["n"] or 0) + int(peer_metrics["n"] or 0)
            velocity_delta = float(peer_metrics["avg_velocity"] or 0) - float(anchor_metrics["avg_velocity"] or 0)
            perf_delta = float(peer_metrics["avg_metric"] or 0) - float(anchor_metrics["avg_metric"] or 0)
            relation_score = (velocity_delta * 0.7) + (perf_delta * 0.3)
            conn.execute(
                """
                INSERT INTO feeder_pair_metrics (
                    feed_id, anchor_feeder_id, feeder_id, window_days,
                    velocity_delta, perf_delta, percentile_delta, relation_score,
                    sample_size, metadata_json, computed_at, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, NULL, %s, %s, %s::jsonb, NOW(), NOW(), NOW())
                ON CONFLICT (feed_id, anchor_feeder_id, feeder_id, window_days)
                DO UPDATE SET
                    velocity_delta = EXCLUDED.velocity_delta,
                    perf_delta = EXCLUDED.perf_delta,
                    relation_score = EXCLUDED.relation_score,
                    sample_size = EXCLUDED.sample_size,
                    metadata_json = EXCLUDED.metadata_json,
                    computed_at = NOW(),
                    updated_at = NOW()
                """,
                (
                    feed_id,
                    anchor["id"],
                    row["feeder_id"],
                    window_days,
                    velocity_delta,
                    perf_delta,
                    relation_score,
                    sample_size,
                    json.dumps(
                        {
                            "anchor_handle": anchor["handle"],
                            "peer_handle": row["handle"],
                        }
                    ),
                ),
            )
        conn.commit()


def upsert_alert_candidate(
    feed_id: int,
    feeder_id: int | None,
    ui_tab: str,
    alert_category: str,
    alert_color: str,
    alert_urgency: str,
    alert_family: str,
    alert_type: str,
    priority_score: float,
    impact_score: float,
    confidence_score: float,
    freshness_score: float,
    novelty_score: float,
    actionability_score: float,
    title: str,
    body: str,
    payload: dict,
):
    day_bucket = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    base = f"{feed_id}|{feeder_id or 0}|{alert_type}|{(title or '').strip().lower()}|{day_bucket}"
    dedupe_key = hashlib.sha256(base.encode("utf-8")).hexdigest()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO alert_candidates (
                feed_id, feeder_id, ui_tab, alert_category, alert_color, alert_urgency,
                alert_dedupe_key, alert_family, alert_type, priority_score,
                impact_score, confidence_score, freshness_score, novelty_score,
                actionability_score, title, body, payload, status, created_at
            )
            SELECT
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, 'candidate', NOW()
            WHERE NOT EXISTS (
                SELECT 1
                FROM alert_candidates ac
                WHERE ac.feed_id = %s
                  AND COALESCE(ac.feeder_id, 0) = COALESCE(%s, 0)
                  AND ac.alert_type = %s
                  AND ac.title = %s
                  AND ac.created_at >= NOW() - INTERVAL '24 hours'
                  AND ac.status IN ('candidate', 'selected', 'sent')
            )
            ON CONFLICT (feed_id, alert_dedupe_key) DO NOTHING
            """,
            (
                feed_id,
                feeder_id,
                ui_tab,
                alert_category,
                alert_color,
                alert_urgency,
                dedupe_key,
                alert_family,
                alert_type,
                priority_score,
                impact_score,
                confidence_score,
                freshness_score,
                novelty_score,
                actionability_score,
                title,
                body,
                json.dumps(payload or {}),
                feed_id,
                feeder_id,
                alert_type,
                title,
            ),
        )
        conn.commit()


def rebuild_signal_aggregates(feed_id: int, lookback_days: int = 30):
    windows = ["d1", "d2", "d3", "d7", "d21"]
    with get_conn() as conn:
        for window_key in windows:
            summary = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_rows,
                    COALESCE(
                      SUM(velocity_value * (1.0 / (1.0 + GREATEST(0.0, EXTRACT(EPOCH FROM (NOW() - checkpoint_at)) / 86400.0))))
                      /
                      NULLIF(SUM(1.0 / (1.0 + GREATEST(0.0, EXTRACT(EPOCH FROM (NOW() - checkpoint_at)) / 86400.0))), 0),
                      0
                    ) AS base_velocity,
                    MIN(checkpoint_at) AS source_start_at,
                    MAX(checkpoint_at) AS source_end_at
                FROM post_checkpoint_metrics
                WHERE feed_id=%s
                  AND checkpoint=%s
                  AND checkpoint_at >= NOW() - (%s || ' days')::interval
                  AND velocity_value IS NOT NULL
                """,
                (feed_id, window_key, str(lookback_days)),
            ).fetchone()
            total_rows = int(summary["total_rows"] or 0)
            if total_rows == 0:
                conn.execute(
                    "DELETE FROM signal_aggregates WHERE feed_id=%s AND window_key=%s",
                    (feed_id, window_key),
                )
                continue

            base_velocity = float(summary["base_velocity"] or 0)
            source_start_at = summary["source_start_at"]
            source_end_at = summary["source_end_at"]

            conn.execute(
                "DELETE FROM signal_aggregates WHERE feed_id=%s AND window_key=%s",
                (feed_id, window_key),
            )

            media_rows = conn.execute(
                """
                SELECT
                    COALESCE(pc.media_type, core.media_type, 'Unknown') AS signal_key,
                    COUNT(*) AS n,
                    COALESCE(
                      SUM(pc.velocity_value * (1.0 / (1.0 + GREATEST(0.0, EXTRACT(EPOCH FROM (NOW() - pc.checkpoint_at)) / 86400.0))))
                      /
                      NULLIF(SUM(1.0 / (1.0 + GREATEST(0.0, EXTRACT(EPOCH FROM (NOW() - pc.checkpoint_at)) / 86400.0))), 0),
                      0
                    ) AS avg_velocity
                FROM post_checkpoint_metrics pc
                LEFT JOIN posts_core core
                  ON core.subscriber_id = pc.subscriber_id
                 AND core.handle = pc.handle
                 AND core.post_url = pc.post_url
                WHERE pc.feed_id=%s
                  AND pc.checkpoint=%s
                  AND pc.checkpoint_at >= NOW() - (%s || ' days')::interval
                  AND pc.velocity_value IS NOT NULL
                GROUP BY COALESCE(pc.media_type, core.media_type, 'Unknown')
                HAVING COUNT(*) >= 2
                ORDER BY n DESC
                """,
                (feed_id, window_key, str(lookback_days)),
            ).fetchall()

            for row in media_rows:
                signal_key = (row["signal_key"] or "Unknown").strip()
                n = int(row["n"] or 0)
                adoption_rate = (n / total_rows) if total_rows > 0 else 0.0
                velocity_delta = float(row["avg_velocity"] or 0) - base_velocity
                confidence = min(1.0, (n / 15.0))
                saturation_score = max(0.0, min(1.0, adoption_rate * (1.0 if velocity_delta <= 0 else 0.5)))
                conn.execute(
                    """
                    INSERT INTO signal_aggregates (
                        feed_id, signal_type, signal_key, window_key,
                        adoption_rate, velocity_delta, saturation_score, confidence,
                        sample_size, source_start_at, source_end_at, created_at, updated_at
                    )
                    VALUES (%s, 'media_type', %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (feed_id, signal_type, signal_key, window_key)
                    DO UPDATE SET
                        adoption_rate = EXCLUDED.adoption_rate,
                        velocity_delta = EXCLUDED.velocity_delta,
                        saturation_score = EXCLUDED.saturation_score,
                        confidence = EXCLUDED.confidence,
                        sample_size = EXCLUDED.sample_size,
                        source_start_at = EXCLUDED.source_start_at,
                        source_end_at = EXCLUDED.source_end_at,
                        updated_at = NOW()
                    """,
                    (
                        feed_id,
                        signal_key,
                        window_key,
                        adoption_rate,
                        velocity_delta,
                        saturation_score,
                        confidence,
                        n,
                        source_start_at,
                        source_end_at,
                    ),
                )

            tag_rows = conn.execute(
                """
                SELECT
                    COALESCE(velocity_tag, 'none') AS signal_key,
                    COUNT(*) AS n,
                    COALESCE(
                      SUM(velocity_value * (1.0 / (1.0 + GREATEST(0.0, EXTRACT(EPOCH FROM (NOW() - checkpoint_at)) / 86400.0))))
                      /
                      NULLIF(SUM(1.0 / (1.0 + GREATEST(0.0, EXTRACT(EPOCH FROM (NOW() - checkpoint_at)) / 86400.0))), 0),
                      0
                    ) AS avg_velocity
                FROM post_checkpoint_metrics
                WHERE feed_id=%s
                  AND checkpoint=%s
                  AND checkpoint_at >= NOW() - (%s || ' days')::interval
                  AND velocity_value IS NOT NULL
                GROUP BY COALESCE(velocity_tag, 'none')
                HAVING COUNT(*) >= 2
                ORDER BY n DESC
                """,
                (feed_id, window_key, str(lookback_days)),
            ).fetchall()

            for row in tag_rows:
                signal_key = (row["signal_key"] or "none").strip()
                n = int(row["n"] or 0)
                adoption_rate = (n / total_rows) if total_rows > 0 else 0.0
                velocity_delta = float(row["avg_velocity"] or 0) - base_velocity
                confidence = min(1.0, (n / 12.0))
                saturation_score = max(0.0, min(1.0, adoption_rate * (1.0 if velocity_delta <= 0 else 0.5)))
                conn.execute(
                    """
                    INSERT INTO signal_aggregates (
                        feed_id, signal_type, signal_key, window_key,
                        adoption_rate, velocity_delta, saturation_score, confidence,
                        sample_size, source_start_at, source_end_at, created_at, updated_at
                    )
                    VALUES (%s, 'velocity_tag', %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (feed_id, signal_type, signal_key, window_key)
                    DO UPDATE SET
                        adoption_rate = EXCLUDED.adoption_rate,
                        velocity_delta = EXCLUDED.velocity_delta,
                        saturation_score = EXCLUDED.saturation_score,
                        confidence = EXCLUDED.confidence,
                        sample_size = EXCLUDED.sample_size,
                        source_start_at = EXCLUDED.source_start_at,
                        source_end_at = EXCLUDED.source_end_at,
                        updated_at = NOW()
                    """,
                    (
                        feed_id,
                        signal_key,
                        window_key,
                        adoption_rate,
                        velocity_delta,
                        saturation_score,
                        confidence,
                        n,
                        source_start_at,
                        source_end_at,
                    ),
                )
        conn.commit()


def rebuild_signal_aggregates_for_subscriber(subscriber_id: int | None = None, lookback_days: int = 30):
    with get_conn() as conn:
        if subscriber_id is None:
            feeds = conn.execute(
                "SELECT id FROM feeds WHERE status='active' ORDER BY id"
            ).fetchall()
        else:
            feeds = conn.execute(
                "SELECT id FROM feeds WHERE status='active' AND subscriber_id=%s ORDER BY id",
                (subscriber_id,),
            ).fetchall()
    for row in feeds:
        rebuild_signal_aggregates(row["id"], lookback_days)


def list_recent_alert_events(feed_id: int, hours: int = 72):
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT alert_type, 0::BIGINT AS feeder_id, created_at
            FROM alert_events
            WHERE subscriber_id = (
                SELECT subscriber_id FROM feeds WHERE id=%s
            )
              AND created_at >= NOW() - (%s || ' hours')::interval
            UNION ALL
            SELECT alert_type, COALESCE(feeder_id, 0) AS feeder_id, created_at
            FROM alert_candidates
            WHERE feed_id = %s
              AND created_at >= NOW() - (%s || ' hours')::interval
            ORDER BY created_at DESC
            """,
            (feed_id, str(hours), feed_id, str(hours)),
        ).fetchall()
        return rows


def get_or_init_alert_engine_state(feed_id: int):
    with get_conn() as conn:
        row = conn.execute(
            """
            INSERT INTO alert_engine_state (feed_id, created_at, updated_at)
            VALUES (%s, NOW(), NOW())
            ON CONFLICT (feed_id) DO UPDATE SET updated_at = NOW()
            RETURNING feed_id, last_hot_scan_at, last_pattern_scan_at
            """,
            (feed_id,),
        ).fetchone()
        conn.commit()
        return row


def mark_alert_engine_scan(feed_id: int, hot_scan_at=None, pattern_scan_at=None):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO alert_engine_state (feed_id, last_hot_scan_at, last_pattern_scan_at, created_at, updated_at)
            VALUES (%s, %s, %s, NOW(), NOW())
            ON CONFLICT (feed_id)
            DO UPDATE SET
                last_hot_scan_at = COALESCE(EXCLUDED.last_hot_scan_at, alert_engine_state.last_hot_scan_at),
                last_pattern_scan_at = COALESCE(EXCLUDED.last_pattern_scan_at, alert_engine_state.last_pattern_scan_at),
                updated_at = NOW()
            """,
            (feed_id, hot_scan_at, pattern_scan_at),
        )
        conn.commit()


def _shortcode_from_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    m = re.search(r"/(?:p|reel|tv)/([A-Za-z0-9_-]+)", u, flags=re.IGNORECASE)
    return m.group(1).lower() if m else ""


def get_post_signal_map(subscriber_id: int, handle: str) -> dict[str, dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT post_url, velocity_tag, velocity_percentile, velocity_stage
            FROM post_signals
            WHERE subscriber_id=%s
              AND lower(regexp_replace(handle, '^@', '')) = lower(regexp_replace(%s, '^@', ''))
            """,
            (subscriber_id, handle),
        ).fetchall()
    out: dict[str, dict] = {}
    for r in rows:
        post_url = (r.get("post_url") or "").strip()
        post_key = _shortcode_from_url(post_url)
        if not post_key:
            continue
        out[post_key] = {
            "velocity_tag": r.get("velocity_tag") or "",
            "velocity_percentile": r.get("velocity_percentile") or "",
            "velocity_stage": r.get("velocity_stage") or "",
        }
    return out


def run_retention_cleanup():
    with get_conn() as conn:
        conn.execute("DELETE FROM run_log WHERE started_at < NOW() - INTERVAL '90 days'")
        conn.execute("DELETE FROM post_signals WHERE updated_at < NOW() - INTERVAL '12 months'")
        conn.execute("DELETE FROM post_embeddings WHERE updated_at < NOW() - INTERVAL '12 months'")
        conn.execute("DELETE FROM post_snapshots WHERE updated_at < NOW() - INTERVAL '12 months'")
        conn.execute("DELETE FROM alert_events WHERE COALESCE(expires_at, created_at + INTERVAL '7 days') < NOW()")
        conn.commit()
