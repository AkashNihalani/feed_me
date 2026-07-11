"""Read-only probe: replay the feeder-reader trigger families (A-F) over every
feeder's real history and measure weekly firing rates.

Purpose: calibrate thresholds so a weekly run yields a few quality cases, not
10 alerts a day. Attributes each trigger to the post's POSTING week (ISO), then
reports raw triggers -> merged cases (post x family dedup) -> budget-capped
selection per feeder-week.

Simulated triggers (deterministic-only; needs no LLM):
  A ceiling/floor : early_fire (D1 pct<=5), long_hold (D7&D21 top-10),
                    ceiling_punch (enters lane top-3 of trailing-90d pool, pool>=10),
                    floor_break (D7 pct>=95)
  B shape         : talk_post (comments_mult>=2.5 & >=1.8x likes_mult),
                    silent_approval (likes pct<=15 & comments pct>=60),
                    core_burn (ER pct<=15 & likes pct>35)
  C lifecycle     : late_bloomer (D7 delta_from_d1>=+25), fast_fade (D1<=10 -> D7>=50),
                    hour_edge (hour_multiple >=2.0 or <=0.5)
  D lanes         : lane_revival (prev 5 same-lane posts all pct>=50, this one <=10)
  E audience      : follower_spike (weekly gain >=3x trailing-8wk median & >=0.5% base)
  F the hand      : cadence_shift (week posts >=1.7x or <=0.5x trailing-4wk avg, min 3),
                    collab_differential (collab & |pct - solo median| >= 30),
                    cold_run_broken (>=5 consecutive pct>=50 then one <=20)

Not simulated (need state we don't keep yet): lane handover, depth/duration edge,
quadrant flip, deletion. STRICTLY SELECT-ONLY. DSN never printed.
"""
from __future__ import annotations

import statistics
import sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

WORKER_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_DIR))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

ENV_PATH = WORKER_DIR / ".env"
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
POOLER_USER = "postgres.worqtdkvicuhmdgoncru"
MAX_CASES_PER_WEEK = 5
TIGHT = "--tight" in sys.argv
# tightened profile: hour_edge >=3x/<=0.33 AND post landed top-20; ceiling_punch needs
# >=15-pool and 1.15x margin over displaced #3; floor_break needs a warm account
# (prev-5 median pct <= 60); talk_post needs 3.0x; late_bloomer needs +35.


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
    select p.post_key, p.feeder_id, p.media_type, p.posted_at, p.collab_post
    from public.posts p
    where p.posted_at is not null
    order by p.feeder_id, p.posted_at
"""

METRICS_SQL = """
    select distinct on (pm.post_key, pm.checkpoint)
      pm.post_key, lower(pm.checkpoint) as checkpoint,
      coalesce(pm.percentile_performance_exact, pm.percentile_performance) as pct,
      pm.likes_percentile, pm.comments_percentile, pm.engagement_rate_percentile,
      pm.likes_multiple, pm.comments_multiple, pm.engagement_rate_multiple,
      pm.ranking_multiple, pm.delta_from_d1, pm.hour_multiple
    from public.post_metrics pm
    order by pm.post_key, pm.checkpoint, pm.computed_at desc nulls last
"""

FEEDERS_SQL = "select id, handle from public.feeders where status='active'"

SNAPS_SQL = """
    select feeder_id, snapshot_date_ist as d, follower_count
    from public.feeder_follower_snapshots
    order by feeder_id, snapshot_date_ist
"""


def week_key(dt) -> str:
    iso = dt.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def main() -> None:
    conn = psycopg.connect(_pooler(_read_dsn(ENV_PATH)), row_factory=dict_row)
    conn.execute("set default_transaction_read_only = on")
    cur = conn.cursor()
    cur.execute(FEEDERS_SQL)
    feeders = {r["id"]: r["handle"] for r in cur.fetchall()}
    cur.execute(POSTS_SQL)
    posts_by_feeder: dict[int, list[dict]] = defaultdict(list)
    for r in cur.fetchall():
        if r["feeder_id"] in feeders:
            posts_by_feeder[r["feeder_id"]].append(r)
    cur.execute(METRICS_SQL)
    metrics: dict[str, dict[str, dict]] = defaultdict(dict)
    for r in cur.fetchall():
        metrics[r["post_key"]][r["checkpoint"]] = r
    cur.execute(SNAPS_SQL)
    snaps: dict[int, list[dict]] = defaultdict(list)
    for r in cur.fetchall():
        snaps[r["feeder_id"]].append(r)

    # trigger event: (feeder, week, post_key, family, trigger, strength)
    events: list[tuple] = []

    def emit(fid, wk, pk, family, trig, strength):
        events.append((fid, wk, pk, family, trig, strength))

    for fid, posts in posts_by_feeder.items():
        # -- per-post triggers, in posting order --
        lane_history: dict[str, list[float]] = defaultdict(list)  # pct history per lane
        all_run: list[float] = []  # consecutive pct>=50 run, all lanes
        # trailing 90d lane pool of (posted_at, ranking_multiple) for ceiling_punch
        lane_pool: dict[str, list[tuple]] = defaultdict(list)
        for p in posts:
            pk, mt = p["post_key"], (p["media_type"] or "reel").lower()
            wk = week_key(p["posted_at"])
            m = metrics.get(pk, {})
            d1, d7, d21 = m.get("d1"), m.get("d7"), m.get("d21")
            pct7 = float(d7["pct"]) if d7 and d7["pct"] is not None else None
            pct1 = float(d1["pct"]) if d1 and d1["pct"] is not None else None

            # A
            if pct1 is not None and pct1 <= 5:
                emit(fid, wk, pk, "A", "early_fire", 25)
            if d7 and d21 and pct7 is not None and pct7 <= 10 \
                    and d21["pct"] is not None and float(d21["pct"]) <= 10:
                emit(fid, wk, pk, "A", "long_hold", 20)
            if pct7 is not None and pct7 >= 95:
                prev5 = ([v for h in lane_history.values() for v in h])[-5:]
                warm = len(prev5) >= 5 and statistics.median(prev5) <= 60
                if not TIGHT or warm:
                    emit(fid, wk, pk, "A", "floor_break", 16)
            rm = d7 and d7["ranking_multiple"] is not None and float(d7["ranking_multiple"])
            cutoff = p["posted_at"] - timedelta(days=90)
            lane_pool[mt] = [(t, v) for t, v in lane_pool[mt] if t >= cutoff]
            min_pool = 15 if TIGHT else 10
            if rm and len(lane_pool[mt]) >= min_pool:
                better = sum(1 for _, v in lane_pool[mt] if v >= rm)
                third = sorted((v for _, v in lane_pool[mt]), reverse=True)[2]
                if better < 3 and (not TIGHT or rm >= 1.15 * third):
                    emit(fid, wk, pk, "A", "ceiling_punch", 30)
            if rm:
                lane_pool[mt].append((p["posted_at"], rm))

            # B (at D7)
            if d7:
                cm, lm = d7["comments_multiple"], d7["likes_multiple"]
                talk_floor = 3.0 if TIGHT else 2.5
                if cm is not None and lm is not None and float(cm) >= talk_floor \
                        and float(cm) >= 1.8 * max(float(lm), 0.01):
                    emit(fid, wk, pk, "B", "talk_post", 18)
                lp, cp = d7["likes_percentile"], d7["comments_percentile"]
                if lp is not None and cp is not None and float(lp) <= 15 and float(cp) >= 60:
                    emit(fid, wk, pk, "B", "silent_approval", 14)
                erp = d7["engagement_rate_percentile"]
                if erp is not None and lp is not None and float(erp) <= 15 and float(lp) > 35:
                    emit(fid, wk, pk, "B", "core_burn", 18)

            # C
            if d7 and d7["delta_from_d1"] is not None:
                dd = float(d7["delta_from_d1"])
                if dd >= (35 if TIGHT else 25):
                    emit(fid, wk, pk, "C", "late_bloomer", 16)
            if pct1 is not None and pct7 is not None and pct1 <= 10 and pct7 >= 50:
                emit(fid, wk, pk, "C", "fast_fade", 14)
            if d7 and d7["hour_multiple"] is not None:
                hm = float(d7["hour_multiple"])
                if TIGHT:
                    if (hm >= 3.0 or hm <= 0.33) and pct7 is not None and pct7 <= 20:
                        emit(fid, wk, pk, "C", "hour_edge", 8)
                elif hm >= 2.0 or hm <= 0.5:
                    emit(fid, wk, pk, "C", "hour_edge", 8)

            # D lane_revival
            hist = lane_history[mt]
            if pct7 is not None:
                if len(hist) >= 5 and all(v >= 50 for v in hist[-5:]) and pct7 <= 10:
                    emit(fid, wk, pk, "D", "lane_revival", 22)
                hist.append(pct7)

            # F collab / cold run
            if p["collab_post"] and pct7 is not None:
                solo = [v for h in lane_history.values() for v in h][:-1]
                if len(solo) >= 5:
                    med = statistics.median(solo)
                    if abs(pct7 - med) >= 30:
                        emit(fid, wk, pk, "F", "collab_differential", 15)
            if pct7 is not None:
                if len(all_run) >= 5 and all(v >= 50 for v in all_run) and pct7 <= 20:
                    emit(fid, wk, pk, "F", "cold_run_broken", 22)
                if pct7 >= 50:
                    all_run.append(pct7)
                else:
                    all_run = []

        # -- weekly aggregates: cadence shift (F), follower spike (E) --
        wk_counts: Counter = Counter(week_key(p["posted_at"]) for p in posts)
        weeks_sorted = sorted(wk_counts)
        for i, wk in enumerate(weeks_sorted):
            prior = weeks_sorted[max(0, i - 4):i]
            if len(prior) >= 3:
                avg = sum(wk_counts[w] for w in prior) / len(prior)
                n = wk_counts[wk]
                if avg >= 1 and n >= 3 and (n >= 1.7 * avg or n <= 0.5 * avg):
                    emit(fid, wk, "WEEK", "F", "cadence_shift", 12)
        ss = snaps.get(fid, [])
        weekly_gain: dict[str, int] = {}
        by_week: dict[str, list[int]] = defaultdict(list)
        for s in ss:
            by_week[week_key(s["d"])].append(s["follower_count"])
        wks = sorted(by_week)
        for a, b in zip(wks, wks[1:]):
            weekly_gain[b] = by_week[b][-1] - by_week[a][-1]
        gains = list(weekly_gain.items())
        for i, (wk, g) in enumerate(gains):
            trail = [abs(x) for _, x in gains[max(0, i - 8):i] if x]
            base = by_week[wk][-1]
            if len(trail) >= 4 and g >= 3 * statistics.median(trail) and g >= 0.005 * base:
                emit(fid, wk, "WEEK", "E", "follower_spike", 20)

    # ---- aggregate ----
    trig_counter = Counter((e[3], e[4]) for e in events)
    by_feeder_week: dict[tuple, list[tuple]] = defaultdict(list)
    for e in events:
        by_feeder_week[(e[0], e[1])].append(e)

    # every observed feeder-week (with >=1 post) is a row, even if 0 triggers
    all_fw = set()
    fw_posts: Counter = Counter()
    for fid, posts in posts_by_feeder.items():
        for p in posts:
            k = (fid, week_key(p["posted_at"]))
            all_fw.add(k)
            fw_posts[k] += 1

    print(f"feeders={len(posts_by_feeder)}  feeder-weeks observed={len(all_fw)}  "
          f"posts={sum(fw_posts.values())}  raw trigger events={len(events)}")
    print("\n=== Firing rate per trigger (whole history) ===")
    total_posts = sum(fw_posts.values())
    for (fam, trig), n in sorted(trig_counter.items()):
        print(f"  {fam} {trig:<20} {n:>4}  ({100*n/total_posts:.1f}% of posts)")

    print("\n=== Merged cases per feeder-week (post x family dedup) ===")
    dist: Counter = Counter()
    sel_total = 0
    for k in sorted(all_fw):
        evs = by_feeder_week.get(k, [])
        merged = {}
        for fid, wk, pk, fam, trig, s in evs:
            key = (pk, fam)
            if key not in merged or s > merged[key][0]:
                merged[key] = (s, trig)
        n = len(merged)
        sel = min(n, MAX_CASES_PER_WEEK)
        sel_total += sel
        dist[min(n, 10)] += 1
    for n in sorted(dist):
        label = f"{n}" if n < 10 else "10+"
        print(f"  {label:>3} cases : {dist[n]:>4} feeder-weeks  ({100*dist[n]/len(all_fw):.0f}%)")
    print(f"\n  avg merged cases per feeder-week : "
          f"{sum(len({(e[2], e[3]) for e in v}) for v in by_feeder_week.values())/len(all_fw):.2f}")
    print(f"  avg SELECTED (cap {MAX_CASES_PER_WEEK}) per feeder-week : {sel_total/len(all_fw):.2f}")

    print("\n=== Per feeder: weekly load (last 8 full weeks) ===")
    today = date.today()
    cur_iso = today.isocalendar()
    recent = set()
    for back in range(1, 9):
        d = today - timedelta(weeks=back)
        recent.add(week_key(d))
    hdr = f"  {'handle':<22} {'wks':>3} {'posts/wk':>8} {'raw/wk':>7} {'cases/wk':>8} {'sel/wk':>7} {'quiet%':>6}"
    print(hdr)
    rows = []
    for fid, handle in feeders.items():
        wks = [(fid, w) for w in recent if (fid, w) in all_fw]
        if not wks:
            continue
        raw = sum(len(by_feeder_week.get(k, [])) for k in wks)
        cases = sum(len({(e[2], e[3]) for e in by_feeder_week.get(k, [])}) for k in wks)
        sel = sum(min(len({(e[2], e[3]) for e in by_feeder_week.get(k, [])}), MAX_CASES_PER_WEEK) for k in wks)
        quiet = sum(1 for k in wks if not by_feeder_week.get(k))
        pw = sum(fw_posts[k] for k in wks) / len(wks)
        rows.append((handle, len(wks), pw, raw/len(wks), cases/len(wks), sel/len(wks), 100*quiet/len(wks)))
    for r in sorted(rows, key=lambda x: -x[4]):
        print(f"  {r[0]:<22} {r[1]:>3} {r[2]:>8.1f} {r[3]:>7.1f} {r[4]:>8.1f} {r[5]:>7.1f} {r[6]:>5.0f}%")

    print("\n=== Family share of merged cases ===")
    fam_counter = Counter()
    for v in by_feeder_week.values():
        for key in {(e[2], e[3]) for e in v}:
            fam_counter[key[1]] += 1
    tot = sum(fam_counter.values())
    for fam in "ABCDEF":
        n = fam_counter.get(fam, 0)
        print(f"  {fam} : {n:>4}  ({100*n/tot:.0f}%)")


if __name__ == "__main__":
    main()
