"""Read-only: run the PRODUCTION select_active_feeder_file over the hero feeders
and show, per feeder, how the 20 reference slots fill from the nested subsets,
plus the hard-dedupe guarantee that the reference and recent lanes never
overlap. Light feeders (fewer than 20 reference slots) are expected for
accounts with under ~30 d7 reels. STRICTLY SELECT-ONLY."""
from __future__ import annotations

import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

WORKER_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_DIR))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

from app.feeder_file_selector import select_active_feeder_file  # noqa: E402

ENV_PATH = WORKER_DIR / ".env"
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
POOLER_USER = "postgres.worqtdkvicuhmdgoncru"
HERO = [("anuj.mp4", 27), ("traya.health", 22), ("taneesho", 15),
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


DOMINANT_MEDIA_TYPE_SQL = """
    select lower(coalesce(p.media_type, '')) as media_type, count(*) as n
    from public.posts p
    join public.post_metrics pm on pm.post_key = p.post_key and lower(pm.checkpoint) = 'd7'
    where p.feeder_id = %s and p.posted_at >= now() - interval '90 days'
    group by 1 order by n desc limit 1
"""
ELIGIBLE_POSTS_SQL = """
    select distinct on (p.post_key)
      p.post_key, p.media_type, p.posted_at,
      pm.views, pm.likes, pm.comments,
      pm.percentile_performance, pm.percentile_performance_exact,
      pm.ranking_metric, pm.ranking_multiple
    from public.posts p
    join public.post_metrics pm on pm.post_key = p.post_key and lower(pm.checkpoint) = 'd7'
    where p.feeder_id = %s
      and lower(coalesce(p.media_type, '')) = %s
      and p.posted_at >= now() - interval '90 days'
    order by p.post_key, pm.computed_at desc nulls last
"""


def main() -> None:
    dsn = _pooler(_read_dsn(ENV_PATH))
    with psycopg.connect(dsn, autocommit=True, connect_timeout=20) as conn:
        with conn.cursor() as cur:
            cur.execute("set default_transaction_read_only = on")
        for handle, fid in HERO:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(DOMINANT_MEDIA_TYPE_SQL, (fid,))
                mt = cur.fetchone()
            if not mt:
                print(f"\n=== {handle} === no d7 posts")
                continue
            media = mt["media_type"]
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(ELIGIBLE_POSTS_SQL, (fid, media))
                rows = [dict(r) for r in cur.fetchall()]

            res = select_active_feeder_file(rows, bar_percentile=30.0)
            c = res["counts"]
            ref = res["reference"]
            from collections import Counter
            by_primary = Counter(e["primary"] for e in ref)
            weights = Counter(e["weight"] for e in ref)
            recent_keys = {e["post_key"] for e in res["recent_context"]}
            overlap = sum(1 for e in ref if e["post_key"] in recent_keys)

            light = "LIGHT" if c["reference"] < 20 else "full"
            print(f"\n=== {handle} (id={fid}, {media}) n={c['eligible']} "
                  f"non_recent={c['non_recent_pool']} ===")
            print(f"  reference={c['reference']}/20 ({light})  "
                  f"recent={c['recent_context']}/10  still_learning={res['still_learning']}")
            print(f"  by primary subset: {dict(by_primary)}")
            print(f"  weights (memberships per ref post): {dict(sorted(weights.items()))}")
            print(f"  reference<->recent overlap (must be 0, hard dedupe): {overlap}")


if __name__ == "__main__":
    main()
