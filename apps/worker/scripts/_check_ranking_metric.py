"""Read-only probe: what is ranking_metric built on, and how does ranking_multiple
relate to views/likes/comments? STRICTLY SELECT-ONLY."""
from __future__ import annotations

import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

WORKER_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_DIR))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

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


DISTINCT_SQL = """
    select lower(coalesce(ranking_metric, '<null>')) as ranking_metric, count(*) as n
    from public.post_metrics
    where lower(checkpoint) = 'd7'
    group by 1 order by n desc
"""

SAMPLE_SQL = """
    select p.post_key, pm.views, pm.likes, pm.comments,
           pm.ranking_metric, pm.ranking_multiple,
           pm.percentile_performance_exact
    from public.posts p
    join public.post_metrics pm
      on pm.post_key = p.post_key and lower(pm.checkpoint) = 'd7'
    where p.feeder_id = %s
      and lower(coalesce(p.media_type,'')) = 'reel'
      and p.posted_at >= now() - interval '90 days'
      and pm.ranking_multiple is not null
    order by pm.percentile_performance_exact asc nulls last
    limit 6
"""


def main() -> None:
    dsn = _pooler(_read_dsn(ENV_PATH))
    with psycopg.connect(dsn, autocommit=True, connect_timeout=20) as conn:
        with conn.cursor() as cur:
            cur.execute("set default_transaction_read_only = on")
        print("=== distinct ranking_metric across all d7 post_metrics ===")
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(DISTINCT_SQL)
            for r in cur.fetchall():
                print(f"  {r['ranking_metric']:<16} n={r['n']}")
        for handle, fid in HERO_FEEDERS:
            print(f"\n=== {handle} (id={fid}) strongest 6 by percentile ===")
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(SAMPLE_SQL, (fid,))
                rows = cur.fetchall()
            if not rows:
                print("  (none)")
                continue
            for r in rows:
                print(f"  {r['post_key']:<26} metric={str(r['ranking_metric']):<10} "
                      f"mult={r['ranking_multiple']!s:<7} pct={r['percentile_performance_exact']!s:<6} "
                      f"views={r['views']} likes={r['likes']} comments={r['comments']}")


if __name__ == "__main__":
    main()
