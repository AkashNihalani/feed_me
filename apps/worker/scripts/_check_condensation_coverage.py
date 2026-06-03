"""Read-only coverage probe: how many d7 reel posts per hero feeder have a
completed post_condensation in feeder_file_model_calls. STRICTLY SELECT-ONLY."""
from __future__ import annotations

import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

WORKER_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_DIR))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

from app.feeder_prompts import POST_CONDENSATION_PROMPT_VERSION  # noqa: E402

ENV_PATH = WORKER_DIR / ".env"
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
POOLER_USER = "postgres.worqtdkvicuhmdgoncru"
HERO_FEEDERS = [("anuj.mp4", 27), ("traya.health", 22), ("taneesho", 15),
                ("thecroffleguys", 39), ("sufimotiwala", 14)]


def _read_dsn(path: Path) -> str:
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if line.startswith("export "):
            line = line[len("export "):]
        if line.startswith("POSTGRES_DSN="):
            v = line.split("=", 1)[1].strip()
            if (v[:1], v[-1:]) in {('"', '"'), ("'", "'")}:
                v = v[1:-1]
            return v
    raise RuntimeError("POSTGRES_DSN not found")


def _pooler(dsn: str) -> str:
    p = urlsplit(dsn)
    netloc = f"{POOLER_USER}:{p.password or ''}@{POOLER_HOST}:5432"
    path = p.path if p.path and p.path != "/" else "/postgres"
    return urlunsplit((p.scheme, netloc, path, "", ""))


SQL = """
    select
      count(*) as eligible_d7_reels,
      count(fmc.post_key) as with_condensation
    from public.posts p
    join public.post_metrics pm
      on pm.post_key = p.post_key and lower(pm.checkpoint) = 'd7'
    left join public.feeder_file_model_calls fmc
      on fmc.post_key = p.post_key
     and fmc.call_type = 'post_condensation'
     and fmc.prompt_version = %s
     and fmc.status = 'complete'
    where p.feeder_id = %s
      and lower(coalesce(p.media_type, '')) = 'reel'
      and p.posted_at >= now() - interval '90 days'
"""


def main() -> None:
    dsn = _pooler(_read_dsn(ENV_PATH))
    print(f"prompt_version={POST_CONDENSATION_PROMPT_VERSION}")
    with psycopg.connect(dsn, autocommit=True, connect_timeout=20) as conn:
        with conn.cursor() as cur:
            cur.execute("set default_transaction_read_only = on")
        for handle, fid in HERO_FEEDERS:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(SQL, (POST_CONDENSATION_PROMPT_VERSION, fid))
                r = cur.fetchone()
            print(f"  {handle:<16} id={fid:<3} eligible_d7_reels={r['eligible_d7_reels']:<4} "
                  f"with_condensation={r['with_condensation']}")


if __name__ == "__main__":
    main()
