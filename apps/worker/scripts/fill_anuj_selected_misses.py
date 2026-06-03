from __future__ import annotations

import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ENV_PATH = ROOT / "apps" / "worker" / ".env"
SELECTED_MISSES = [
    "p/dxtk73kjeaw#f27",
    "p/dx6do9cspq9#f27",
    "p/dxy_ou6kc9k#f27",
]


def _load_env() -> None:
    if not ENV_PATH.exists():
        return
    for raw_line in ENV_PATH.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

    dsn = os.environ.get("POSTGRES_DSN", "")
    if "db.worqtdkvicuhmdgoncru.supabase.co" in dsn:
        dsn = dsn.replace(
            "db.worqtdkvicuhmdgoncru.supabase.co",
            "aws-1-ap-south-1.pooler.supabase.com",
        )
        dsn = dsn.replace("postgres:", "postgres.worqtdkvicuhmdgoncru:", 1)
        os.environ["POSTGRES_DSN"] = dsn


def _has_v5(conn, post_key: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            select exists (
              select 1
              from public.post_condensations
              where post_key = %s
                and model_version like 'openrouter:anthropic/claude-haiku-4.5:post_condensation_v5_character_transfer%%'
            ) as has_v5
            """,
            (post_key,),
        )
        return bool((cur.fetchone() or {}).get("has_v5"))


def main() -> None:
    _load_env()

    from apps.worker.app import config

    config.POSTGRES_DSN = os.environ["POSTGRES_DSN"]

    import psycopg
    from psycopg.rows import dict_row

    from apps.worker.app.fingerprint_intelligence import (
        ensure_post_condensation,
        ensure_post_fingerprint,
    )

    conn = psycopg.connect(os.environ["POSTGRES_DSN"], row_factory=dict_row)
    try:
        for post_key in SELECTED_MISSES:
            result = {"post_key": post_key}
            if _has_v5(conn, post_key):
                result["skipped"] = "already_v5"
                print(json.dumps(result, default=str), flush=True)
                continue

            try:
                fingerprint = ensure_post_fingerprint(conn, post_key)
                result["fingerprint_ok"] = bool(fingerprint)
                if fingerprint:
                    condensation = ensure_post_condensation(conn, post_key, fingerprint)
                    result["condensation_ok"] = bool(condensation)
                else:
                    result["condensation_ok"] = False
                conn.commit()
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                result["error"] = f"{type(exc).__name__}: {str(exc)[:240]}"
            print(json.dumps(result, default=str), flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
