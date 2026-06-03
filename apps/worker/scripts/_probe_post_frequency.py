"""Read-only: reliable per-feeder POSTING FREQUENCY (reels), measured only over the
window we trust the worker to have captured completely.

Why a trusted window: the worker ran every day Mar 29 -> Apr 30 (verified), so any
reel POSTED in that span was captured live = complete. May had multi-day blackouts,
so a naive 'posts in last 30d' undercounts true cadence. We therefore anchor the
estimate on the clean window and use May only to show the depression.

Per feeder:
  trusted_*  reels with posted_at in [2026-03-29, 2026-05-01)  (33 clean days)
  may_*      reels with posted_at in [2026-05-01, 2026-06-01)  (blackout-affected)
  med_gap    median days between consecutive reels INSIDE the trusted window
  reliable   posts/week estimate (trusted window; falls back to full history if thin)
  -> days/post, days to reach 30 posts, expected reels in 90d, top-20 a real cut?

STRICTLY SELECT-ONLY. DSN never printed.
"""
from __future__ import annotations

import statistics
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

WORKER_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_DIR))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

ENV_PATH = WORKER_DIR / ".env"
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
POOLER_USER = "postgres.worqtdkvicuhmdgoncru"

# trusted = verified continuous daily worker operation
TRUSTED_LO = datetime(2026, 3, 29, tzinfo=timezone.utc)
TRUSTED_HI = datetime(2026, 5, 1, tzinfo=timezone.utc)
TRUSTED_DAYS = (TRUSTED_HI - TRUSTED_LO).days  # 33
MAY_LO = datetime(2026, 5, 1, tzinfo=timezone.utc)
MAY_HI = datetime(2026, 6, 1, tzinfo=timezone.utc)
MAY_DAYS = (datetime(2026, 5, 31, tzinfo=timezone.utc) - MAY_LO).days + 1  # 31


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


FEEDERS_SQL = """
    select distinct f.id as feeder_id, f.handle
    from public.feeders f
    join public.posts p on p.feeder_id = f.id
    join public.feeder_file_model_calls fmc
      on fmc.post_key = p.post_key and fmc.call_type='post_condensation'
     and fmc.status in ('complete','completed')
    order by f.id
"""

REELS_SQL = """
    select distinct on (p.post_key) p.post_key, p.posted_at
    from public.posts p
    where p.feeder_id = %s
      and lower(coalesce(p.media_type,''))='reel'
      and p.posted_at is not null
    order by p.post_key
"""


def _med_gap(ts: list[datetime]) -> float | None:
    ts = sorted(ts)
    if len(ts) < 2:
        return None
    gaps = [(ts[i + 1] - ts[i]).total_seconds() / 86400 for i in range(len(ts) - 1)]
    return round(statistics.median(gaps), 1)


def main() -> None:
    dsn = _pooler(_read_dsn(ENV_PATH))
    with psycopg.connect(dsn, autocommit=True, connect_timeout=20) as conn:
        with conn.cursor() as cur:
            cur.execute("set default_transaction_read_only = on")
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(FEEDERS_SQL)
            feeders = cur.fetchall()
            rows = {}
            for f in feeders:
                cur.execute(REELS_SQL, (f["feeder_id"],))
                rows[f["feeder_id"]] = (f["handle"], [r["posted_at"] for r in cur.fetchall()])

    print(f"trusted window: {TRUSTED_LO.date()} -> {TRUSTED_HI.date()}  "
          f"({TRUSTED_DAYS} clean days, worker verified daily)")
    print("frequency on reels, by posted_at. may = blackout-affected (for comparison only)\n")
    hdr = (f"{'fid':>4} {'handle':<18} {'trust_n':>7} {'trust/wk':>8} {'med_gap':>7} "
           f"{'may_n':>5} {'may/wk':>6}  {'RELIABLE/wk':>11} {'days/post':>9} "
           f"{'~days→30':>8} {'exp_90d':>7} {'top20?':>6}")
    print(hdr)
    print("-" * len(hdr))

    summary = []
    for fid, (handle, ts) in rows.items():
        trust = [t for t in ts if TRUSTED_LO <= t < TRUSTED_HI]
        may = [t for t in ts if MAY_LO <= t < MAY_HI]
        all_ts = sorted(ts)
        trust_n, may_n = len(trust), len(may)
        trust_wk = trust_n / TRUSTED_DAYS * 7
        may_wk = may_n / MAY_DAYS * 7
        med_gap = _med_gap(trust)

        thin = trust_n < 4
        if thin and len(all_ts) >= 2:
            span = (all_ts[-1] - all_ts[0]).days or 1
            reliable_wk = len(all_ts) / span * 7  # fallback: full-history rate
            src = "*"  # flag fallback
        else:
            reliable_wk = trust_wk
            src = " "
        days_per_post = (7 / reliable_wk) if reliable_wk > 0 else None
        days_to_30 = (30 * days_per_post) if days_per_post else None
        exp_90d = reliable_wk / 7 * 90
        top20 = "cut" if exp_90d > 20 else "ALL"

        def f(x, fmt):
            return (fmt % x) if x is not None else "    -"
        print(f"{fid:>4} {str(handle):<18} {trust_n:>7} {trust_wk:>8.1f} "
              f"{f(med_gap, '%7.1f')} {may_n:>5} {may_wk:>6.1f}  "
              f"{reliable_wk:>10.1f}{src} {f(days_per_post, '%9.1f')} "
              f"{f(days_to_30, '%8.0f')} {exp_90d:>7.0f} {top20:>6}")
        summary.append((handle, reliable_wk, exp_90d, top20, may_wk, trust_wk))

    print("-" * len(hdr))
    print("  * = trusted window too thin (<4 reels); reliable/wk fell back to full-history rate\n")

    # depression check: did May cadence drop vs the clean window?
    drops = [(h, tw, mw) for h, _, _, _, mw, tw in summary if tw > 0]
    print("May vs trusted (capture-completeness check):")
    for h, tw, mw in sorted(drops, key=lambda x: -x[1]):
        delta = (mw - tw) / tw * 100 if tw else 0
        flag = "  <- May looks under-captured" if delta <= -35 else ""
        print(f"  {h:<18} trusted {tw:>4.1f}/wk   may {mw:>4.1f}/wk   ({delta:+.0f}%){flag}")

    print("\nfeeders that will have a REAL top-20 cut in 90d (expected reels > 20):")
    cut = [h for h, _, e, t, *_ in summary if t == 'cut']
    allp = [h for h, _, e, t, *_ in summary if t == 'ALL']
    print(f"  real cut ({len(cut)}): {', '.join(cut)}")
    print(f"  file = all posts ({len(allp)}): {', '.join(allp)}")


if __name__ == "__main__":
    main()
