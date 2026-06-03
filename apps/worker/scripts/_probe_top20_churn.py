"""Read-only verification: how often does each feeder's TOP-20 actually shift?

Validates the architecture claim: "recent 10 + top 20 sharpest single-axis spikes
across 90d, and the 90d rollover self-refreshes the top over time."

Method (per feeder, pure simulation over real history):
  * magnitude(post) = sharpest SINGLE axis = max(views_mult, likes_mult, comments_mult).
  * Walk every eligible d7-reel forward in posting order. At each arrival, rebuild
    the trailing-90d window and take its top-20 by magnitude.
  * A window is "competitive" only once it holds >20 posts (before that the top-20
    is just every post, so there is no selection to churn).
  * Measure, across competitive arrivals:
      - join_rate   : share of NEW posts that immediately crack the top-20
      - churn/30d   : avg top-20 members replaced per 30 days
      - refresh_mo  : months for the set to fully turn over (20 / churn-per-month)
      - distinct    : distinct posts that ever held a top-20 slot (vs 20 slots)
  * Also reports current 90d window: size + how many clear a real-spike floor
    (>=3x, >=2x) so we see whether 20 REAL spikes even exist today.

STRICTLY SELECT-ONLY (set default_transaction_read_only = on). DSN never printed.
"""
from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

WORKER_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_DIR))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

ENV_PATH = WORKER_DIR / ".env"
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
POOLER_USER = "postgres.worqtdkvicuhmdgoncru"
WINDOW = timedelta(days=90)
TOP_N = 20


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


# every eligible d7 reel for a feeder, full history (NO 90d cap), latest d7 row
POSTS_SQL = """
    select distinct on (p.post_key)
      p.post_key, p.posted_at,
      pm.views_multiple, pm.likes_multiple, pm.comments_multiple
    from public.posts p
    join public.post_metrics pm
      on pm.post_key = p.post_key and lower(pm.checkpoint)='d7'
    where p.feeder_id = %s
      and lower(coalesce(p.media_type,''))='reel'
    order by p.post_key, pm.computed_at desc nulls last
"""

FEEDERS_SQL = """
    select distinct f.id as feeder_id, f.handle
    from public.feeders f
    join public.posts p on p.feeder_id = f.id
    join public.feeder_file_model_calls fmc
      on fmc.post_key = p.post_key and fmc.call_type='post_condensation'
     and fmc.status='complete'
    order by f.id
"""


def _mag(r) -> float | None:
    vals = [float(r[k]) for k in ("views_multiple", "likes_multiple", "comments_multiple")
            if r[k] is not None]
    return max(vals) if vals else None


def simulate(rows: list[dict]) -> dict:
    # rows already sorted by posted_at asc; only rankable (magnitude not None)
    posts = [(r["posted_at"], r["post_key"], _mag(r)) for r in rows]
    posts = [p for p in posts if p[2] is not None]
    n = len(posts)
    out = {
        "n_rank": n, "span_d": 0, "max_win": 0,
        "comp_arrivals": 0, "new_joins": 0,
        "entries": 0, "comp_span_d": 0.0,
        "distinct_top": set(), "ever_competitive": False,
    }
    if n == 0:
        return out
    out["span_d"] = (posts[-1][0] - posts[0][0]).days

    prev_top: set[str] = set()
    prev_comp = False
    first_comp_at = None
    last_comp_at = None
    for i in range(n):
        as_of = posts[i][0]
        lo = as_of - WINDOW
        window = [p for p in posts[: i + 1] if p[0] > lo]
        out["max_win"] = max(out["max_win"], len(window))
        top = sorted(window, key=lambda p: -p[2])[:TOP_N]
        top_keys = {p[1] for p in top}
        competitive = len(window) > TOP_N
        if competitive:
            out["ever_competitive"] = True
            out["distinct_top"] |= top_keys
            out["comp_arrivals"] += 1
            if posts[i][1] in top_keys:
                out["new_joins"] += 1
            if first_comp_at is None:
                first_comp_at = as_of
            last_comp_at = as_of
            if prev_comp:
                out["entries"] += len(top_keys - prev_top)
        prev_top, prev_comp = top_keys, competitive
    if first_comp_at and last_comp_at:
        out["comp_span_d"] = (last_comp_at - first_comp_at).days or 0.0
    return out


def current_window_inventory(rows: list[dict]):
    """Today's trailing-90d window: size + real-spike counts + the top-20 freshness."""
    if not rows:
        return 0, 0, 0, 0, 0
    now = max(r["posted_at"] for r in rows)  # proxy "as of latest post"
    lo = now - WINDOW
    win = [(r["post_key"], r["posted_at"], _mag(r)) for r in rows
           if r["posted_at"] > lo and _mag(r) is not None]
    n = len(win)
    s3 = sum(1 for _, _, m in win if m >= 3.0)
    s2 = sum(1 for _, _, m in win if m >= 2.0)
    top = sorted(win, key=lambda x: -x[2])[:TOP_N]
    fresh30 = sum(1 for k, ts, _ in top if ts > now - timedelta(days=30))
    return n, s3, s2, len(top), fresh30


def main() -> None:
    dsn = _pooler(_read_dsn(ENV_PATH))
    with psycopg.connect(dsn, autocommit=True, connect_timeout=20) as conn:
        with conn.cursor() as cur:
            cur.execute("set default_transaction_read_only = on")
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(FEEDERS_SQL)
            feeders = cur.fetchall()
            data = {}
            for f in feeders:
                cur.execute(POSTS_SQL, (f["feeder_id"],))
                rows = cur.fetchall()
                rows.sort(key=lambda r: r["posted_at"])
                data[f["feeder_id"]] = (f["handle"], rows)

    print(f"top-N = {TOP_N}   window = 90d   feeders = {len(feeders)}")
    print("magnitude = sharpest single axis = max(views/likes/comments multiple)\n")
    hdr = (f"{'fid':>4} {'handle':<18} {'rank':>4} {'span_d':>6} {'maxwin':>6} "
           f"{'cur90':>5} {'>=3x':>4} {'>=2x':>4} {'join%':>6} {'chn/30':>6} "
           f"{'refr_mo':>7} {'distinct':>8} {'fresh30':>7}")
    print(hdr)
    print("-" * len(hdr))

    agg = {"comp_arrivals": 0, "new_joins": 0, "entries": 0, "comp_span_d": 0.0}
    competitive_feeders = 0
    for fid, (handle, rows) in data.items():
        s = simulate(rows)
        cur90, s3, s2, topn, fresh30 = current_window_inventory(rows)
        join = (s["new_joins"] / s["comp_arrivals"] * 100) if s["comp_arrivals"] else None
        chn30 = (s["entries"] / (s["comp_span_d"] / 30)) if s["comp_span_d"] >= 30 else None
        refresh = (TOP_N / chn30) if chn30 and chn30 > 0 else None
        distinct = len(s["distinct_top"]) if s["ever_competitive"] else None
        if s["ever_competitive"]:
            competitive_feeders += 1
            agg["comp_arrivals"] += s["comp_arrivals"]
            agg["new_joins"] += s["new_joins"]
            agg["entries"] += s["entries"]
            agg["comp_span_d"] = max(agg["comp_span_d"], s["comp_span_d"])

        def f(x, fmt):
            return (fmt % x) if x is not None else "  -"
        print(f"{fid:>4} {str(handle):<18} {s['n_rank']:>4} {s['span_d']:>6} "
              f"{s['max_win']:>6} {cur90:>5} {s3:>4} {s2:>4} "
              f"{f(join, '%6.0f')} {f(chn30, '%6.1f')} {f(refresh, '%7.1f')} "
              f"{f(distinct, '%8d')} {fresh30:>7}")

    print("-" * len(hdr))
    gjoin = (agg["new_joins"] / agg["comp_arrivals"] * 100) if agg["comp_arrivals"] else 0
    print(f"\ncompetitive feeders (>20 in a 90d window, ever): "
          f"{competitive_feeders}/{len(feeders)}")
    print(f"pooled new-post join rate (competitive phase): {gjoin:.0f}%  "
          f"({agg['new_joins']}/{agg['comp_arrivals']} arrivals cracked the top-20)")
    print("\nreading guide:")
    print("  maxwin<=20  -> feeder never has >20 posts in 90d; 'top-20' = all its posts (no cut)")
    print("  join%       -> how often a fresh post displaces a current tool (set fluidity)")
    print("  chn/30      -> top-20 members replaced per 30 days")
    print("  refr_mo     -> months to fully turn the set over (20 / monthly churn)")
    print("  distinct    -> distinct posts that ever held a slot (>20 means it moved)")
    print("  fresh30     -> of today's top-20, how many are from the last 30 days")


if __name__ == "__main__":
    main()
