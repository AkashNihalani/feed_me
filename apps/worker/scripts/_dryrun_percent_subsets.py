"""Read-only dry-run of the percent-subset feeder-file design.

Faithful to the proposed spec:
  recent: latest 10 d7-settled
  reference_memory: best 5 from each NESTED post-count subset of the 90d stream
    newest 25% / 50% / 75% / 100%  (expanding, newest-first)
  rank inside each subset: account-relative d7 perf -> views -> posted_at
  dedupe: a post is kept ONCE; extra subset hits add a membership and the slot
          is backfilled with the next-best post from the SAME subset
  recent overlap: a latest-10 post that also qualifies for reference is kept in
          recent, marked also_reference, and the reference slot is backfilled

Prints per-feeder: subset sizes, reference fill, calendar spread (30-day thirds),
weight/concentration, and recent<->reference overlap. STRICTLY SELECT-ONLY.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

WORKER_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_DIR))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

from app.feeder_file_selector import (  # noqa: E402
    LOWER_IS_STRONGER,
    _metric_value,
    _posted_ts,
    _post_key,
    _raw_percentile,
)

ENV_PATH = WORKER_DIR / ".env"
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
POOLER_USER = "postgres.worqtdkvicuhmdgoncru"
HERO = [("anuj.mp4", 27), ("traya.health", 22), ("taneesho", 15),
        ("thecroffleguys", 39), ("sufimotiwala", 14)]
NOW = datetime.now(timezone.utc)


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


def _views(p: dict) -> int:
    v = p.get("views")
    return int(v) if isinstance(v, (int, float)) else 0


def _rank_key(p: dict):
    m = _metric_value(p)
    oriented = m if LOWER_IS_STRONGER else -m
    return (oriented, -_posted_ts(p))


def _third(p: dict) -> str:
    pa = p.get("posted_at")
    if not isinstance(pa, datetime):
        return "?"
    if pa.tzinfo is None:
        pa = pa.replace(tzinfo=timezone.utc)
    age = (NOW - pa).days
    if age < 30:
        return "recent"
    if age < 60:
        return "mid"
    return "old"


def select_percent_subsets(posts, keep=5, recent_n=10):
    by_recency = sorted((dict(p) for p in posts), key=_posted_ts, reverse=True)
    n = len(by_recency)
    recent = by_recency[:recent_n]
    recent_keys = {_post_key(p) for p in recent}

    # reference candidates exclude the recent-10 lane entirely
    pool_stream = by_recency[recent_n:]
    m = len(pool_stream)

    def toppct(pct):
        c = max(1, round(m * pct))
        return pool_stream[:c]

    subsets = [
        ("n25", toppct(0.25)),
        ("n50", toppct(0.50)),
        ("n75", toppct(0.75)),
        ("full", pool_stream),
    ]
    sizes = {name: len(pool) for name, pool in subsets}

    ref_index: dict[str, dict] = {}
    ref_order: list[dict] = []
    also_reference: dict[str, list[str]] = {}
    dedupe_hits = 0
    per_subset_new: dict[str, int] = {}

    for name, pool in subsets:
        ranked = sorted(pool, key=_rank_key)
        taken = 0
        for p in ranked:
            if taken >= keep:
                break
            k = _post_key(p)
            if k in recent_keys:
                also_reference.setdefault(k, []).append(name)
                continue  # backfill: recent post doesn't consume a reference slot
            if k in ref_index:
                ref_index[k]["memberships"].append(name)
                dedupe_hits += 1
                continue  # dedupe: backfill with next-best
            entry = {
                "post_key": k,
                "posted_at": p.get("posted_at"),
                "d7_percentile": _raw_percentile(p),
                "views": _views(p),
                "primary": name,
                "memberships": [name],
                "third": _third(p),
            }
            ref_index[k] = entry
            ref_order.append(entry)
            taken += 1
        per_subset_new[name] = taken

    recent_entries = [
        {"post_key": _post_key(p), "posted_at": p.get("posted_at"),
         "d7_percentile": _raw_percentile(p), "third": _third(p),
         "also_reference": _post_key(p) in also_reference}
        for p in recent
    ]
    return {
        "n": n, "sizes": sizes, "reference": ref_order,
        "recent": recent_entries, "per_subset_new": per_subset_new,
        "dedupe_hits": dedupe_hits, "also_reference": also_reference,
    }


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

            res = select_percent_subsets(rows)
            ref = res["reference"]
            from collections import Counter
            third_ref = Counter(e["third"] for e in ref)
            third_all = Counter(
                [e["third"] for e in ref] + [e["third"] for e in res["recent"]]
            )
            weights = Counter(len(e["memberships"]) for e in ref)
            pcts = [e["d7_percentile"] for e in ref if e["d7_percentile"] is not None]

            print(f"\n=== {handle} (id={fid}, {media}) n={res['n']} ===")
            print(f"  subset sizes: 25%={res['sizes']['n25']} 50%={res['sizes']['n50']} "
                  f"75%={res['sizes']['n75']} full={res['sizes']['full']}")
            print(f"  reference filled: {len(ref)}/20   "
                  f"new per subset: {res['per_subset_new']}   dedupe_hits={res['dedupe_hits']}")
            print(f"  reference calendar spread (30d thirds): "
                  f"old={third_ref.get('old',0)} mid={third_ref.get('mid',0)} recent={third_ref.get('recent',0)}")
            print(f"  FULL 30 calendar spread:                "
                  f"old={third_all.get('old',0)} mid={third_all.get('mid',0)} recent={third_all.get('recent',0)}")
            print(f"  concentration (memberships per ref post): {dict(sorted(weights.items()))}  "
                  f"(weight>=2 means same post won multiple subsets)")
            if pcts:
                print(f"  reference quality: best d7%={min(pcts):.1f}  worst d7%={max(pcts):.1f}  "
                      f"median d7%={sorted(pcts)[len(pcts)//2]:.1f}")
            n_also = len(res["also_reference"])
            print(f"  recent<->reference overlap: {n_also} of the latest-10 also won a subset "
                  f"(kept in recent, slot backfilled)")
            # show the reference posts compactly, oldest-subset first
            print("  reference posts (date | d7% | primary | weight):")
            for e in sorted(ref, key=lambda x: (x["posted_at"] or NOW)):
                d = e["posted_at"].date().isoformat() if isinstance(e["posted_at"], datetime) else "?"
                pc = f"{e['d7_percentile']:5.1f}" if e["d7_percentile"] is not None else "  n/a"
                print(f"    {d}  {pc}  {e['primary']:<5} w={len(e['memberships'])} [{','.join(e['memberships'])}]")


if __name__ == "__main__":
    main()
