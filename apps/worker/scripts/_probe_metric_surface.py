"""Read-only schema + data-availability probe to ground the metric-intelligence
analysis: what columns/checkpoints/snapshots actually exist. SELECT-ONLY."""
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


def q(cur, label, sql, args=()):
    print(f"\n=== {label} ===")
    cur.execute(sql, args)
    rows = cur.fetchall()
    for r in rows:
        print("  " + "  ".join(f"{k}={v}" for k, v in r.items()))
    if not rows:
        print("  (none)")


def main() -> None:
    dsn = _pooler(_read_dsn(ENV_PATH))
    with psycopg.connect(dsn, autocommit=True, connect_timeout=20) as conn:
        with conn.cursor() as cur:
            cur.execute("set default_transaction_read_only = on")
        with conn.cursor(row_factory=dict_row) as cur:
            q(cur, "post_metrics columns",
              """select column_name, data_type from information_schema.columns
                 where table_schema='public' and table_name='post_metrics' order by ordinal_position""")
            q(cur, "posts columns",
              """select column_name, data_type from information_schema.columns
                 where table_schema='public' and table_name='posts' order by ordinal_position""")
            q(cur, "distinct checkpoints (post_metrics)",
              """select lower(checkpoint) as checkpoint, count(*) n from public.post_metrics
                 group by 1 order by n desc""")
            q(cur, "checkpoints per post (distribution)",
              """select n_checkpoints, count(*) as posts from (
                   select post_key, count(distinct lower(checkpoint)) n_checkpoints
                   from public.post_metrics group by post_key
                 ) s group by 1 order by 1""")
            q(cur, "all public tables",
              """select table_name from information_schema.tables
                 where table_schema='public' order by table_name""")
            q(cur, "non-null coverage of rich post_metrics fields (d7)",
              """select
                   count(*) n,
                   count(views_multiple) views_mult, count(likes_multiple) likes_mult,
                   count(comments_multiple) comments_mult,
                   count(views_percentile) views_pct, count(likes_percentile) likes_pct,
                   count(comments_percentile) comments_pct, count(feed_percentile) feed_pct,
                   count(delta_from_d1) delta_d1, count(hour_multiple) hour_mult,
                   count(metric_value) metric_value
                 from public.post_metrics where lower(checkpoint)='d7'""")
            q(cur, "sample velocity curve: anuj posts across checkpoints",
              """select pm.post_key, lower(pm.checkpoint) cp, pm.views, pm.likes, pm.comments,
                        pm.delta_from_d1, pm.percentile_performance_exact pct
                 from public.posts p
                 join public.post_metrics pm on pm.post_key=p.post_key
                 where p.feeder_id=27 and lower(coalesce(p.media_type,''))='reel'
                 order by p.posted_at desc, array_position(array['d1','d3','d7','d21'], lower(pm.checkpoint))
                 limit 16""")


if __name__ == "__main__":
    main()
