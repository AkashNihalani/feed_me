"""Read-only dry-run for the deterministic feeder-file selector.

- Reads POSTGRES_DSN from apps/worker/.env, rewrites it to the Supabase session
  pooler, connects with psycopg3 (autocommit), and for each of the 5 hero
  feeders pulls eligible posts (one row per post, latest d7 metric, dominant
  media_type, within 90 days), runs select_active_feeder_file, and prints a
  clean per-feeder report + a simulated "one inning ago" churn diff.
- STRICTLY READ-ONLY: only SELECT queries are issued; the session is forced
  read-only. Never echoes the password.
- Dumps the full structured result to /tmp/feeder_file_dryrun.json (falls back
  to ./feeder_file_dryrun.json next to this script if /tmp is not writable).

Run:  /Users/sky/feed_me/apps/worker/.venv/bin/python \
        /Users/sky/feed_me/apps/worker/scripts/feeder_file_dryrun.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

# Make the worker app importable so we use the SAME selector module.
WORKER_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_DIR))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

from app.feeder_file_selector import select_active_feeder_file  # noqa: E402

ENV_PATH = WORKER_DIR / ".env"

POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
POOLER_USER = "postgres.worqtdkvicuhmdgoncru"

HERO_FEEDERS = [
    ("anuj.mp4", 27),
    ("traya.health", 22),
    ("taneesho", 15),
    ("thecroffleguys", 39),
    ("sufimotiwala", 14),
]


# --- DSN handling ------------------------------------------------------------

def _read_dsn_from_env(path: Path) -> str:
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        if line.startswith("POSTGRES_DSN="):
            value = line.split("=", 1)[1].strip()
            if (value.startswith('"') and value.endswith('"')) or (
                value.startswith("'") and value.endswith("'")
            ):
                value = value[1:-1]
            return value
    raise RuntimeError("POSTGRES_DSN not found in .env")


def _rewrite_to_pooler(dsn: str) -> str:
    """Rewrite the direct-host DSN to the Supabase session pooler. Never logs pw."""
    parts = urlsplit(dsn)
    password = parts.password or ""
    netloc = f"{POOLER_USER}:{password}@{POOLER_HOST}:5432"
    path = parts.path if parts.path and parts.path != "/" else "/postgres"
    return urlunsplit((parts.scheme, netloc, path, "", ""))


# --- Data access (READ-ONLY) -------------------------------------------------

DOMINANT_MEDIA_TYPE_SQL = """
    select lower(coalesce(p.media_type, '')) as media_type, count(*) as n
    from public.posts p
    join public.post_metrics pm
      on pm.post_key = p.post_key
     and lower(pm.checkpoint) = 'd7'
    where p.feeder_id = %s
      and p.posted_at >= now() - interval '90 days'
    group by 1
    order by n desc
    limit 1
"""

# One row per post (latest d7 metric per post via distinct on), same media_type,
# within 90 days. Mirrors _build_d7_read_input eligibility.
ELIGIBLE_POSTS_SQL = """
    select distinct on (p.post_key)
      p.post_key,
      p.media_type,
      p.posted_at,
      pm.business_date_ist,
      pm.computed_at,
      pm.views,
      pm.likes,
      pm.comments,
      pm.percentile_performance,
      pm.percentile_performance_exact,
      pm.ranking_metric,
      pm.ranking_multiple
    from public.posts p
    join public.post_metrics pm
      on pm.post_key = p.post_key
     and lower(pm.checkpoint) = 'd7'
    where p.feeder_id = %s
      and lower(coalesce(p.media_type, '')) = %s
      and p.posted_at >= now() - interval '90 days'
    order by p.post_key, pm.computed_at desc nulls last
"""

# Empirical metric-direction probe: does LOW percentile correlate with HIGH views?
DIRECTION_PROBE_SQL = """
    select
      p.post_key,
      pm.views,
      pm.percentile_performance_exact,
      pm.percentile_performance
    from public.posts p
    join public.post_metrics pm
      on pm.post_key = p.post_key
     and lower(pm.checkpoint) = 'd7'
    where p.feeder_id = %s
      and lower(coalesce(p.media_type, '')) = %s
      and p.posted_at >= now() - interval '90 days'
      and coalesce(pm.percentile_performance_exact, pm.percentile_performance) is not null
      and pm.views is not null
    order by coalesce(pm.percentile_performance_exact, pm.percentile_performance) asc
"""


def _recency_key(row: dict) -> float:
    posted = row.get("posted_at")
    if isinstance(posted, datetime):
        return posted.timestamp()
    bd = row.get("business_date_ist")
    if bd is not None:
        try:
            return datetime.fromisoformat(f"{bd} 12:00:00+00:00").timestamp()
        except Exception:
            pass
    computed = row.get("computed_at")
    if isinstance(computed, datetime):
        return computed.timestamp()
    return 0.0


def _coerce_posted_at(row: dict) -> datetime | None:
    posted = row.get("posted_at")
    if isinstance(posted, datetime):
        return posted
    bd = row.get("business_date_ist")
    if bd is not None:
        try:
            return datetime.fromisoformat(f"{bd} 12:00:00+00:00")
        except Exception:
            pass
    computed = row.get("computed_at")
    if isinstance(computed, datetime):
        return computed
    return None


def _avg_days_between(posts: list[dict]) -> float | None:
    ts = sorted(_recency_key(p) for p in posts if _recency_key(p) > 0)
    if len(ts) < 2:
        return None
    span = ts[-1] - ts[0]
    return (span / (len(ts) - 1)) / 86400.0


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if isinstance(dt, datetime) else None


def _entry_brief(e: dict) -> dict:
    return {
        "post_key": e["post_key"],
        "d7_percentile": e["d7_percentile"],
        "tier": e["tier"],
        "role": e["role"],
        "memory_type": e["memory_type"],
        "winner": e["winner"],
        "posted_at": _iso(e.get("posted_at")),
        "views": e.get("views"),
    }


def _churn_diff(current: dict, prior: dict) -> dict:
    def winners(res: dict) -> dict[str, dict]:
        return {e["post_key"]: e for e in res["ranked"] if e["winner"]}

    cur_w = winners(current)
    pri_w = winners(prior)
    cur_keys = set(cur_w)
    pri_keys = set(pri_w)

    entered = [_entry_brief(cur_w[k]) for k in (cur_keys - pri_keys)]
    exited = [_entry_brief(pri_w[k]) for k in (pri_keys - cur_keys)]

    moved = []
    for k in cur_keys & pri_keys:
        c, p = cur_w[k], pri_w[k]
        if c["tier"] != p["tier"]:
            direction = (
                "climbing"
                if (c["tier"] is not None and p["tier"] is not None and c["tier"] < p["tier"])
                else "fading"
            )
            moved.append(
                {
                    "post_key": k,
                    "tier_before": p["tier"],
                    "tier_now": c["tier"],
                    "direction": direction,
                }
            )

    return {
        "entered": entered,
        "exited": exited,
        "moved": moved,
        "winners_before": len(pri_keys),
        "winners_now": len(cur_keys),
    }


# --- Reporting ---------------------------------------------------------------

def _print_group(title: str, entries: list[dict]) -> None:
    print(f"  {title} ({len(entries)}):")
    for e in entries:
        pct = e["d7_percentile"]
        pct_s = f"{pct:5.1f}" if isinstance(pct, (int, float)) else "  n/a"
        tier = e["tier"] if e["tier"] is not None else "-"
        print(
            f"    {e['post_key']:<28} d7%={pct_s}  tier={tier}  "
            f"role={e['role']:<16} posted={_iso(e.get('posted_at'))}"
        )


def _serialize(res: dict) -> dict:
    return {
        "ranked": [{**e, "posted_at": _iso(e.get("posted_at"))} for e in res["ranked"]],
        "recent_context": [
            {**e, "posted_at": _iso(e.get("posted_at"))} for e in res["recent_context"]
        ],
        "counts": res["counts"],
        "still_learning": res["still_learning"],
    }


def main() -> None:
    dsn = _rewrite_to_pooler(_read_dsn_from_env(ENV_PATH))
    print(f"Connecting to pooler host {POOLER_HOST} (password redacted)...")

    report: dict = {"generated_at": datetime.utcnow().isoformat(), "feeders": []}

    with psycopg.connect(dsn, autocommit=True, connect_timeout=20) as conn:
        with conn.cursor() as cur:
            cur.execute("set default_transaction_read_only = on")

        # --- Metric direction verification (first hero with enough data) ----
        direction = {}
        for handle, fid in HERO_FEEDERS:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(DOMINANT_MEDIA_TYPE_SQL, (fid,))
                mt_row = cur.fetchone()
            if not mt_row:
                continue
            media_type = mt_row["media_type"]
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(DIRECTION_PROBE_SQL, (fid, media_type))
                probe = cur.fetchall()
            if len(probe) >= 6:
                k = max(1, len(probe) // 3)
                strong = [r for r in probe[:k] if r["views"] is not None]
                weak = [r for r in probe[-k:] if r["views"] is not None]
                if strong and weak:
                    avg_strong = sum(r["views"] for r in strong) / len(strong)
                    avg_weak = sum(r["views"] for r in weak) / len(weak)
                    direction = {
                        "probe_feeder": handle,
                        "media_type": media_type,
                        "n": len(probe),
                        "avg_views_low_percentile_third": round(avg_strong, 1),
                        "avg_views_high_percentile_third": round(avg_weak, 1),
                        "lower_percentile_means_stronger": avg_strong > avg_weak,
                    }
                    break
        report["metric_direction"] = direction
        print("\n=== METRIC DIRECTION CHECK ===")
        print(json.dumps(direction, indent=2))

        # --- Per-hero selection + churn -------------------------------------
        for handle, fid in HERO_FEEDERS:
            print(f"\n=== FEEDER {handle} (id={fid}) ===")
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(DOMINANT_MEDIA_TYPE_SQL, (fid,))
                mt_row = cur.fetchone()
            if not mt_row:
                print("  no d7 posts in window")
                report["feeders"].append({"handle": handle, "feeder_id": fid, "eligible": 0})
                continue
            media_type = mt_row["media_type"]

            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(ELIGIBLE_POSTS_SQL, (fid, media_type))
                rows = [dict(r) for r in cur.fetchall()]

            for r in rows:
                r["posted_at"] = _coerce_posted_at(r)

            avg_gap = _avg_days_between(rows)
            cadence = {
                "media_type": media_type,
                "eligible_posts": len(rows),
                "avg_days_between_posts": round(avg_gap, 2) if avg_gap is not None else None,
            }
            if avg_gap:
                cadence["tier_windows_in_days"] = {
                    "tier1_last12": round(avg_gap * 12, 1),
                    "tier2_last30": round(avg_gap * 30, 1),
                    "tier3_last60": round(avg_gap * 60, 1),
                    "tier4_durable_90d": 90,
                }

            result = select_active_feeder_file(rows, bar_percentile=30.0)

            # Simulated one-inning-ago: same posts MINUS the 10 most-recent.
            by_recency = sorted(rows, key=_recency_key, reverse=True)
            prior_rows = by_recency[10:]
            prior_result = select_active_feeder_file(prior_rows, bar_percentile=30.0)
            churn = _churn_diff(result, prior_result)

            print(f"  media_type={media_type}  eligible={len(rows)}")
            if avg_gap is not None:
                print(
                    f"  cadence: ~{avg_gap:.2f} days/post -> "
                    f"tier1(12)~{avg_gap*12:.0f}d  tier2(30)~{avg_gap*30:.0f}d  "
                    f"tier3(60)~{avg_gap*60:.0f}d  tier4=90d"
                )
            print(f"  counts={result['counts']}  still_learning={result['still_learning']}")

            winners = [e for e in result["ranked"] if e["memory_type"] == "ranked_winner"]
            fills = [e for e in result["ranked"] if e["memory_type"] == "recent_fill"]
            _print_group("ranked_winner", winners)
            if fills:
                _print_group("recent_fill (loan)", fills)
            _print_group("recent_context", result["recent_context"])

            print("  churn vs one-inning-ago:")
            print(f"    entered: {[e['post_key'] for e in churn['entered']]}")
            print(f"    exited:  {[e['post_key'] for e in churn['exited']]}")
            print(f"    moved:   {churn['moved']}")

            report["feeders"].append(
                {
                    "handle": handle,
                    "feeder_id": fid,
                    "cadence": cadence,
                    "result": _serialize(result),
                    "prior_inning_result": _serialize(prior_result),
                    "churn": churn,
                }
            )

    payload = json.dumps(report, indent=2, default=str)
    out_path = Path("/tmp/feeder_file_dryrun.json")
    try:
        out_path.write_text(payload)
    except OSError:
        out_path = Path(__file__).resolve().parent / "feeder_file_dryrun.json"
        out_path.write_text(payload)
    print(f"\nWrote full structured result to {out_path}")


if __name__ == "__main__":
    main()
