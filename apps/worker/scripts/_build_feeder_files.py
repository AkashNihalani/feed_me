"""Build NEW-FORMAT feeder files (metric_pools_v1) for every feeder that has at
least one completed post_condensation, and optionally persist them to Supabase.

Format per feeder file:
  overview     last-10 typical (median) multiple per axis vs 90d baseline
  recent_run   last 10 posts, chronological
  ranked       strongest 5 in last 25 / 50 / 75 / overall (dedup cascade)
Each post = condensed summary + 3 metrics-with-baseline + biggest anomalies.

SAFETY:
  * Default run is READ-ONLY (set default_transaction_read_only = on) and only
    writes local JSON previews to scripts/out/feeder_files/.
  * Pass --write to upsert into public.feeder_files. Rows are written with
    compile_version='metric_pools_v1' and status='draft' so they are INVISIBLE
    to the live reader (which filters status='active'). The 9 existing rows are
    never touched. Fully reversible:
        delete from public.feeder_files where compile_version='metric_pools_v1';
  * The DSN/password is never printed.

Usage:
  .venv/bin/python scripts/_build_feeder_files.py            # dry run (read-only)
  .venv/bin/python scripts/_build_feeder_files.py --write    # persist as drafts
"""
from __future__ import annotations

import json
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

WORKER_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_DIR))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

from app.feeder_prompts import POST_CONDENSATION_PROMPT_VERSION as PV  # noqa: E402

ENV_PATH = WORKER_DIR / ".env"
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
POOLER_USER = "postgres.worqtdkvicuhmdgoncru"
WINDOW_DAYS = 90
COMPILE_VERSION = "metric_pools_v1"
DRAFT_STATUS = "draft"
SOURCE_TAG = "metric_pool_builder_v1"
ACTIVE_WINDOW = "recent10_ranked_25_50_75_overall_90d"


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
     and fmc.status='complete' and fmc.prompt_version = %s
    order by f.id
"""

POSTS_SQL = """
    select distinct on (p.post_key)
      p.post_key, p.posted_at,
      left(coalesce(p.caption,''), 240) as caption,
      pm.views, pm.likes, pm.comments,
      pm.views_multiple, pm.likes_multiple, pm.comments_multiple,
      pm.percentile_performance_exact as pct,
      pm.delta_from_d1
    from public.posts p
    join public.post_metrics pm
      on pm.post_key = p.post_key and lower(pm.checkpoint)='d7'
    where p.feeder_id = %s
      and lower(coalesce(p.media_type,''))='reel'
      and p.posted_at >= now() - interval '%s days'
    order by p.post_key, pm.computed_at desc nulls last
"""

CONDENSE_SQL = """
    select fmc.post_key, fmc.parsed_output->'post_condensed' as pc
    from public.feeder_file_model_calls fmc
    where fmc.call_type='post_condensation' and fmc.status='complete'
      and fmc.prompt_version = %s and fmc.post_key = any(%s)
"""

UPSERT_SQL = """
    insert into public.feeder_files (
      feeder_id, feeder_handle, feed_file, compile_version,
      source_breakdown_version, active_window, status, source,
      generated_at, updated_at
    )
    values (%s, %s, %s::jsonb, %s, %s, %s, %s, %s, now(), now())
    on conflict (feeder_handle, compile_version) do update set
      feeder_id = excluded.feeder_id,
      feed_file = excluded.feed_file,
      source_breakdown_version = excluded.source_breakdown_version,
      active_window = excluded.active_window,
      status = excluded.status,
      source = excluded.source,
      updated_at = now()
    returning id
"""


def _m(x):
    return None if x is None else round(float(x), 1)


def _baseline_str(value, mult):
    if value is None:
        return None
    if mult is None:
        return f"{value:,}"
    return f"{value:,} ({mult:.1f}x baseline)"


def biggest_anomalies(p) -> list[str]:
    vm = float(p["views_multiple"]) if p["views_multiple"] is not None else None
    lm = float(p["likes_multiple"]) if p["likes_multiple"] is not None else None
    cm = float(p["comments_multiple"]) if p["comments_multiple"] is not None else None
    d = float(p["delta_from_d1"]) if p["delta_from_d1"] is not None else None
    views, likes = p["views"], p["likes"]
    axis: list[tuple[float, str]] = []
    if vm and vm >= 3:
        axis.append((vm, f"reach outlier - {vm:.1f}x view baseline"))
    if cm and cm >= 2.5:
        if vm and cm > vm * 1.3:
            axis.append((cm, f"conversation spike - {cm:.1f}x comments while views only {vm:.1f}x"))
        else:
            axis.append((cm, f"{cm:.1f}x comments - heavy discussion"))
    if views and likes and likes >= views:
        axis.append((99.0, "more likes than views - meme-level approval"))
    elif lm and lm >= 3 and (not vm or lm > vm * 1.3):
        axis.append((lm, f"approval spike - {lm:.1f}x likes, well above reach"))
    axis.sort(key=lambda t: -t[0])
    out = [s for _, s in axis[:2]]
    if d is not None and d >= 5:
        out.append("slow burn - kept climbing well past launch")
    elif d is not None and d <= -5:
        out.append("peaked early then faded")
    return out[:3] or ["steady - no standout deviation vs baseline"]


def build_feed_file(cur, feeder_id: int, handle: str) -> dict:
    cur.execute(POSTS_SQL % ("%s", WINDOW_DAYS), (feeder_id,))
    posts = cur.fetchall()
    keys = [p["post_key"] for p in posts]
    cond: dict = {}
    if keys:
        cur.execute(CONDENSE_SQL, (PV, keys))
        cond = {r["post_key"]: r["pc"] for r in cur.fetchall()}

    posts.sort(key=lambda p: p["posted_at"], reverse=True)
    now = datetime.now(timezone.utc)

    claimed: set[str] = set()
    recent = posts[:10]
    claimed.update(p["post_key"] for p in recent)
    ranked_pools: dict[str, list[dict]] = {}
    for name, n in (("last_25", 25), ("last_50", 50), ("last_75", 75), ("overall", len(posts))):
        cands = [p for p in posts[:n] if p["post_key"] not in claimed]
        cands.sort(key=lambda p: (p["pct"] if p["pct"] is not None else 999.0))
        top5 = cands[:5]
        ranked_pools[name] = top5
        claimed.update(p["post_key"] for p in top5)

    def _med(field):
        v = [float(p[field]) for p in recent if p[field] is not None]
        return round(statistics.median(v), 1) if v else None

    overview = {
        "basis": "last 10 posts, typical (median) vs 90d baseline",
        "views": f"{_med('views_multiple')}x baseline",
        "comments": f"{_med('comments_multiple')}x baseline",
        "likes": f"{_med('likes_multiple')}x baseline",
    }

    def entry(p):
        pc = cond.get(p["post_key"])
        return {
            "post_key": p["post_key"],
            "age_days": (now - p["posted_at"]).days,
            "caption": p["caption"],
            "summary": (pc or {}).get("reel"),
            "metrics": {
                "views": _baseline_str(p["views"], _m(p["views_multiple"])),
                "comments": _baseline_str(p["comments"], _m(p["comments_multiple"])),
                "likes": _baseline_str(p["likes"], _m(p["likes_multiple"])),
            },
            "biggest_anomalies": biggest_anomalies(p),
        }

    recent_entries = [entry(p) for p in recent]
    ranked_entries = {name: [entry(p) for p in pool] for name, pool in ranked_pools.items()}
    post_count = len(recent_entries) + sum(len(v) for v in ranked_entries.values())
    with_summary = sum(1 for e in recent_entries if e["summary"]) + sum(
        1 for v in ranked_entries.values() for e in v if e["summary"])

    feed_file = {
        "compile_version": COMPILE_VERSION,
        "feeder_id": feeder_id,
        "handle": handle,
        "media_type": "reel",
        "window_days": WINDOW_DAYS,
        "eligible_posts": len(posts),
        "post_count": post_count,
        "selection": "recent10 + ranked top5 across last25/50/75/overall (dedup cascade)",
        "overview": overview,
        "recent_run": recent_entries,
        "ranked": ranked_entries,
    }
    return feed_file, {"eligible": len(posts), "post_count": post_count, "with_summary": with_summary}


def main() -> None:
    write = "--write" in sys.argv
    dsn = _pooler(_read_dsn(ENV_PATH))
    out_dir = WORKER_DIR / "scripts" / "out" / "feeder_files"
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = psycopg.connect(dsn, autocommit=True, connect_timeout=20)
    try:
        if not write:
            with conn.cursor() as cur:
                cur.execute("set default_transaction_read_only = on")
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(FEEDERS_SQL, (PV,))
            feeders = cur.fetchall()

        print(f"mode: {'WRITE (status=draft, compile_version=' + COMPILE_VERSION + ')' if write else 'DRY-RUN (read-only, local JSON only)'}")
        print(f"feeders with condensations: {len(feeders)}\n")
        print(f"  {'fid':>4}  {'handle':<20} {'elig':>5} {'posts':>6} {'w/sum':>6}  {'written_id' if write else ''}")
        for f in feeders:
            with conn.cursor(row_factory=dict_row) as cur:
                feed_file, stats = build_feed_file(cur, f["feeder_id"], f["handle"])
            (out_dir / f"{f['handle']}.json").write_text(json.dumps(feed_file, indent=2, ensure_ascii=False))
            written = ""
            if write:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(UPSERT_SQL, (
                        f["feeder_id"], f["handle"], json.dumps(feed_file, ensure_ascii=False),
                        COMPILE_VERSION, PV, ACTIVE_WINDOW, DRAFT_STATUS, SOURCE_TAG,
                    ))
                    written = str(cur.fetchone()["id"])
            print(f"  {f['feeder_id']:>4}  {str(f['handle']):<20} {stats['eligible']:>5} "
                  f"{stats['post_count']:>6} {stats['with_summary']:>6}  {written}")
        print(f"\nlocal previews -> {out_dir}")
        if write:
            print(f"persisted {len(feeders)} draft feeder files. revert with:")
            print(f"  delete from public.feeder_files where compile_version='{COMPILE_VERSION}';")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
