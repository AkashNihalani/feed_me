"""Read-only builder: assemble a LIGHT, production-grade feeder file for anuj
(feeder 27) from the locked architecture.

Structure (nothing more):
  overview        last-10 vs 90d -> views/comments/likes as Nx baseline
  recent_run      last 10 posts, chronological
  ranked          strongest 5 in last 25 / 50 / 75 / overall (dedup cascade)

Each post = condensed summary + the 3 metrics with baselines + biggest anomalies.
Percentile is used only to RANK selection; it is never shown. Velocity is used
only to derive a "slow burn / peaked early" anomaly; the raw curve is not shown.

STRICTLY SELECT-ONLY. Writes the assembled file to scripts/out/.
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

from app.feeder_prompts import POST_CONDENSATION_PROMPT_VERSION  # noqa: E402

ENV_PATH = WORKER_DIR / ".env"
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
POOLER_USER = "postgres.worqtdkvicuhmdgoncru"
FEEDER_ID = 27
HANDLE = "anuj.mp4"
WINDOW_DAYS = 90


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
    where fmc.call_type = 'post_condensation'
      and fmc.status = 'complete'
      and fmc.prompt_version = %s
      and fmc.post_key = any(%s)
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
    """2-3 terse, computed metric deviations for THIS post. Empty-ish posts
    get a single 'steady' note. No content guessing — pure metric facts."""
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


def main() -> None:
    dsn = _pooler(_read_dsn(ENV_PATH))
    with psycopg.connect(dsn, autocommit=True, connect_timeout=20) as conn:
        with conn.cursor() as cur:
            cur.execute("set default_transaction_read_only = on")
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(POSTS_SQL % ("%s", WINDOW_DAYS), (FEEDER_ID,))
            posts = cur.fetchall()
            keys = [p["post_key"] for p in posts]
            cur.execute(CONDENSE_SQL, (POST_CONDENSATION_PROMPT_VERSION, keys))
            cond = {r["post_key"]: r["pc"] for r in cur.fetchall()}

    posts.sort(key=lambda p: p["posted_at"], reverse=True)
    now = datetime.now(timezone.utc)
    by_key = {p["post_key"]: p for p in posts}

    # ---- pools: recent 10 + dedup cascade ----
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

    # ---- overview: last-10 typical (median) multiple per axis vs 90d baseline ----
    def _med(field):
        v = [float(p[field]) for p in recent if p[field] is not None]
        return round(statistics.median(v), 1) if v else None

    overview = {
        "basis": "last 10 posts, typical (median) vs 90d baseline",
        "views": f"{_med('views_multiple')}x baseline",
        "comments": f"{_med('comments_multiple')}x baseline",
        "likes": f"{_med('likes_multiple')}x baseline",
    }

    def post_entry(p: dict) -> dict:
        key = p["post_key"]
        pc = cond.get(key)
        return {
            "post_key": key,
            "age_days": (now - p["posted_at"]).days,
            "caption": p["caption"],
            "summary": (pc or {}).get("reel"),  # condensed content; null if not yet condensed
            "metrics": {
                "views": _baseline_str(p["views"], _m(p["views_multiple"])),
                "comments": _baseline_str(p["comments"], _m(p["comments_multiple"])),
                "likes": _baseline_str(p["likes"], _m(p["likes_multiple"])),
            },
            "biggest_anomalies": biggest_anomalies(p),
        }

    feeder_file = {
        "feeder": {"handle": HANDLE, "media_type": "reel", "window_days": WINDOW_DAYS,
                   "eligible_posts": len(posts)},
        "overview": overview,
        "recent_run": [post_entry(p) for p in recent],
        "ranked": {name: [post_entry(p) for p in pool] for name, pool in ranked_pools.items()},
    }

    out_dir = WORKER_DIR / "scripts" / "out"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "anuj_feeder_file.json"
    out_path.write_text(json.dumps(feeder_file, indent=2, ensure_ascii=False))

    # ---- console preview ----
    print(f"eligible d7 reels (90d): {len(posts)}")
    print(f"\nOVERVIEW ({overview['basis']}):")
    print(f"  views {overview['views']}   comments {overview['comments']}   likes {overview['likes']}")

    def show(label, entries):
        print(f"\n{label}")
        for e in entries:
            c = "summary" if e["summary"] else "no-summary"
            print(f"  {e['post_key']:<26} {e['age_days']:>3}d  "
                  f"V {e['metrics']['views']:<26} C {e['metrics']['comments']:<22} L {e['metrics']['likes']:<24} [{c}]")
            for a in e["biggest_anomalies"]:
                print(f"        - {a}")

    show("RECENT_RUN (last 10, chronological):", feeder_file["recent_run"])
    for name, pool in feeder_file["ranked"].items():
        show(f"RANKED / {name}:", pool)
    print(f"\nfull JSON -> {out_path}")


if __name__ == "__main__":
    main()
