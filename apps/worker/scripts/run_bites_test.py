"""Run-bite generator test: read one account's latest 10-post run against its
merged feeder file + computed box-score, and produce 3-5 frontend insight cards.
"""
from __future__ import annotations
import json, os, statistics, sys
from pathlib import Path
import requests

WD = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WD))
import psycopg
from psycopg.rows import dict_row
from app.feeder_prompts import RUN_BITES_GENERATOR_SYSTEM_V1
OUT = WD / "scripts" / "out"
MODEL = "openai/gpt-5.4"

ACCOUNTS = {
    "srishtigargg": (20, "srishtigargg_merged_srishti_gpt54_merge_next10_retry.json"),
    "anuj.mp4": (27, "anuj_mp4_merged_gpt54_20post_from_10chunk_v1.json"),
    "lakmeindia": (35, "lakmeindia_merged_gpt54_20post_from_10chunk_v1.json"),
}


def _env():
    for raw in (WD/".env").read_text().splitlines():
        s=raw.strip()
        if s.startswith("export "): s=s[7:]
        if not s or s.startswith("#") or "=" not in s: continue
        k,v=s.split("=",1); os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

def _dsn():
    d=os.environ["POSTGRES_DSN"]
    if "db.worqtdkvicuhmdgoncru.supabase.co" in d:
        d=d.replace("db.worqtdkvicuhmdgoncru.supabase.co","aws-1-ap-south-1.pooler.supabase.com").replace("postgres:","postgres.worqtdkvicuhmdgoncru:",1)
    return d

def _call(system, user):
    r=requests.post(f"{os.environ.get('OPENROUTER_BASE_URL','https://openrouter.ai/api/v1').rstrip('/')}/chat/completions",
        headers={"Authorization":f"Bearer {os.environ['OPENROUTER_API_KEY']}","Content-Type":"application/json"},
        json={"model":MODEL,"messages":[{"role":"system","content":system},{"role":"user","content":json.dumps(user,ensure_ascii=False,indent=2)}],
              "temperature":0.3,"max_tokens":2500,"usage":{"include":True}},timeout=300)
    r.raise_for_status(); d=r.json(); return d["choices"][0]["message"]["content"]

def _strip(t):
    t=t.strip()
    if t.startswith("```"): t=t.split("\n",1)[1]
    return t[t.find("{"):t.rfind("}")+1]


def run(handle):
    fid, ffpath = ACCOUNTS[handle]
    ff = json.load(open(OUT/ffpath))
    amap = ff.get("source_alias_map", {})
    key_to_name = {}
    for al, nm in (ff.get("post_names") or {}).items():
        if al in amap: key_to_name[amap[al]] = nm
    # carrying move per post_key from core receipts
    carry = {}
    for b in ff["bites"]:
        for r in b.get("receipts", []):
            if r.get("weight") == "core":
                carry.setdefault(r.get("post_key"), b["name"].replace("_"," "))

    with psycopg.connect(_dsn(), connect_timeout=20) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""select distinct on (p.post_key) p.post_key, p.posted_at, p.caption, p.collab_post,
                pm.views_multiple vm, pm.comments_multiple cm, pm.delta_from_d1 dd, pm.percentile_performance_exact pct
                from posts p join post_metrics pm on pm.post_key=p.post_key and lower(pm.checkpoint)='d7'
                where p.feeder_id=%s and lower(coalesce(p.media_type,''))='reel'
                  and p.posted_at>=now()-interval '90 days'
                order by p.post_key, pm.computed_at desc nulls last""",(fid,))
            allrows=cur.fetchall()
            cur.execute("select hour_ist, median_views from feeder_hour_baselines where feeder_id=%s and lower(checkpoint)='d7' and lower(media_type)='reel' and median_views is not null order by median_views desc limit 3",(fid,))
            best_hours=[r["hour_ist"] for r in cur.fetchall()]
            cur.execute("select follower_count, snapshot_date_ist from feeder_follower_snapshots where feeder_id=%s order by snapshot_date_ist desc limit 1",(fid,))
            f_latest=cur.fetchone()
        rows=[r for r in allrows if r["pct"] is not None]
        rows.sort(key=lambda r: r["posted_at"], reverse=True)
        run_rows = rows[:10]
        # overall pool rank by pct (lower=better)
        bypct=sorted(rows, key=lambda r: float(r["pct"]))
        rankof={r["post_key"]: i+1 for i,r in enumerate(bypct)}
        pool=len(bypct)
        run_start=min(r["posted_at"] for r in run_rows).date()
        with psycopg.connect(_dsn(), connect_timeout=20) as c2, c2.cursor() as cur:
            cur.execute("select follower_count from feeder_follower_snapshots where feeder_id=%s and snapshot_date_ist<=%s order by snapshot_date_ist desc limit 1",(fid,run_start))
            r=cur.fetchone(); f_start=r[0] if r else None

    def toppct(k): return round(100*rankof[k]/pool)
    def hr(dt): return int(dt.astimezone().strftime("%H")) if dt.tzinfo else dt.hour
    import zoneinfo
    ist=zoneinfo.ZoneInfo("Asia/Kolkata")
    per_post=[]
    for r in run_rows:
        nm=key_to_name.get(r["post_key"]) or (r["caption"] or "")[:40]
        h=r["posted_at"].astimezone(ist).hour
        per_post.append({"post":nm, "placed":f"top {toppct(r['post_key'])}%",
            "views_vs_usual":round(float(r["vm"]),1) if r["vm"] is not None else None,
            "collab":bool(r["collab_post"]), "hour_ist":h,
            "legs":(r["dd"] is not None and r["dd"]>=5),
            "carried_by":carry.get(r["post_key"],"")})
    vms=[float(r["vm"]) for r in run_rows if r["vm"] is not None]
    cms=[float(r["cm"]) for r in run_rows if r["cm"] is not None]
    ranks=[toppct(r["post_key"]) for r in run_rows]
    fdelta=(f_latest["follower_count"]-f_start) if (f_latest and f_start) else None
    run_stats={
        "beat_usual_count": sum(1 for v in vms if v>=1.0), "of": len(run_rows),
        "peak_placed": f"top {min(ranks)}%", "typical_placed": f"top {round(statistics.median(ranks))}%",
        "views_vs_usual_median": round(statistics.median(vms),1) if vms else None,
        "comments_vs_usual_median": round(statistics.median(cms),1) if cms else None,
        "followers_net": fdelta,
        "posts_with_legs": sum(1 for p in per_post if p["legs"]),
        "best_hours_ist": best_hours,
        "posts_in_best_hours": sum(1 for p in per_post if p["hour_ist"] in best_hours),
        "per_post": per_post,
    }
    mem=[{"move":b["name"].replace("_"," "), "carries_or_supports":b.get("weights_tally",{}),
          "instances":[{"showed_up":r.get("how_it_shows_up"),"placed":r.get("rank_context",{}).get("overall")} for r in b.get("receipts",[])[:4]]}
         for b in ff["bites"]]

    payload={"account":{"handle":handle},"run_stats":run_stats,"move_memory":mem}
    raw=_call(RUN_BITES_GENERATOR_SYSTEM_V1.replace("{handle}",handle), payload)
    bites=json.loads(_strip(raw))["run_bites"]
    (OUT/f"{handle.replace('.','_')}_run_bites.json").write_text(json.dumps({"run_stats":run_stats,"run_bites":bites},indent=2,ensure_ascii=False))

    print("="*76)
    print(f"{handle}  | run: {run_stats['beat_usual_count']}/{run_stats['of']} beat usual · peak {run_stats['peak_placed']} · followers {fdelta:+d}" if fdelta is not None else f"{handle} | {run_stats['beat_usual_count']}/{run_stats['of']} beat usual")
    print(f"  best hours IST {best_hours} · posts in best hours {run_stats['posts_in_best_hours']}/10 · legs {run_stats['posts_with_legs']}/10")
    for b in bites:
        print(f"\n  [{b['kind'].upper()}]  {b['headline']}")
        print(f"   {b['explainer']}")
        print(f"   evidence: {b['evidence']}")


if __name__=="__main__":
    _env()
    for h in (sys.argv[1:] or list(ACCOUNTS)):
        run(h)
