"""D7 post-mortem test: read one held-out trigger reel against an account's
rolling feeder file (move memory) using D7_POSTMORTEM_SYSTEM_LIVE_V1.

Trigger = most recent v12-fingerprinted post for the account that is NOT already
a receipt in the feeder file (held out). Its receipts are stripped from the move
memory so the read can't see itself. Performance (rank_context + anomalies) is
worker-computed from post_metrics.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests

WORKER_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_DIR))
import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402
from app.feeder_prompts import D7_POSTMORTEM_SYSTEM_LIVE_V1, D7_READ_PROMPT_LIVE_VERSION  # noqa: E402

ENV = WORKER_DIR / ".env"
OUT = WORKER_DIR / "scripts" / "out"
MODEL = "openai/gpt-5.4"

# handle -> (feeder_id, feeder-file path)
ACCOUNTS = {
    "srishtigargg": (20, "srishtigargg_merged_srishti_gpt54_merge_next10_retry.json"),
    "anuj.mp4": (27, "anuj_mp4_feeder_file_cold_start_v8_1_gpt54.json"),
    "lakmeindia": (35, "lakmeindia_feeder_file_cold_start_v8_1_gpt54.json"),
}


def _env():
    for raw in ENV.read_text().splitlines():
        s = raw.strip()
        if s.startswith("export "): s = s[7:]
        if not s or s.startswith("#") or "=" not in s: continue
        k, v = s.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _dsn():
    d = os.environ["POSTGRES_DSN"]
    if "db.worqtdkvicuhmdgoncru.supabase.co" in d:
        d = d.replace("db.worqtdkvicuhmdgoncru.supabase.co", "aws-1-ap-south-1.pooler.supabase.com")
        d = d.replace("postgres:", "postgres.worqtdkvicuhmdgoncru:", 1)
    return d


def _read_phrase(cur, n_cur, ov, n_ov):
    batch = ("strongest in the current batch" if cur == 1 else "weakest of the current batch" if cur == n_cur
             else "upper third of the current batch" if cur/n_cur <= 1/3
             else "mid-pack in the current batch" if cur/n_cur <= 2/3 else "lower end of the current batch")
    f = ov/n_ov
    overall = ("top-tier overall" if f <= 0.1 else "upper-mid overall" if f <= 1/3 else "mid overall"
               if f <= 2/3 else "lower-mid overall" if f <= 0.9 else "bottom-tier overall")
    return f"{batch}, {overall}"


def _anomalies(m):
    vm = float(m["views_multiple"]) if m["views_multiple"] is not None else None
    lm = float(m["likes_multiple"]) if m["likes_multiple"] is not None else None
    cm = float(m["comments_multiple"]) if m["comments_multiple"] is not None else None
    d = float(m["delta_from_d1"]) if m.get("delta_from_d1") is not None else None
    views, likes = m.get("views"), m.get("likes")
    out = []
    if vm and vm >= 3: out.append((vm, f"reach outlier - {vm:.1f}x the account's usual views"))
    if cm and cm >= 2.5:
        out.append((cm, f"conversation spike - comments at {cm:.1f}x while views sat at {vm:.1f}x" if vm and cm > vm*1.3
                    else f"heavy discussion - comments at {cm:.1f}x the usual"))
    if views and likes and likes >= views: out.append((99.0, "more likes than views - meme-level approval"))
    elif lm and lm >= 3 and (not vm or lm > vm*1.3): out.append((lm, f"approval spike - likes at {lm:.1f}x, well above reach"))
    out.sort(key=lambda t: -t[0])
    lines = [s for _, s in out[:2]]
    if d is not None and d >= 5: lines.append("slow burn - kept climbing well past launch")
    elif d is not None and d <= -5: lines.append("peaked early then faded")
    return lines[:3]


def _overall_ranks(conn, feeder_id):
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""select distinct on (p.post_key) p.post_key, pm.percentile_performance_exact pct
            from posts p join post_metrics pm on pm.post_key=p.post_key and lower(pm.checkpoint)='d7'
            where p.feeder_id=%s and lower(coalesce(p.media_type,''))='reel'
              and p.posted_at >= now() - interval '90 days'
            order by p.post_key, pm.computed_at desc nulls last""", (feeder_id,))
        rows = [r for r in cur.fetchall() if r["pct"] is not None]
    rows.sort(key=lambda r: float(r["pct"]))
    return {r["post_key"]: i+1 for i, r in enumerate(rows)}, len(rows)


def _call(system, user):
    r = requests.post(f"{os.environ.get('OPENROUTER_BASE_URL','https://openrouter.ai/api/v1').rstrip('/')}/chat/completions",
        headers={"Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}", "Content-Type": "application/json"},
        json={"model": MODEL, "messages": [{"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False, indent=2)}],
            "temperature": 0.2, "max_tokens": 2000, "usage": {"include": True}}, timeout=300)
    r.raise_for_status()
    d = r.json()
    return d["choices"][0]["message"]["content"], (d.get("usage") or {})


def _strip(t):
    t = t.strip()
    if t.startswith("```"): t = t.split("\n",1)[1]; t = t[:t.rstrip().rfind("```")] if "```" in t else t
    return t[t.find("{"): t.rfind("}")+1]


def run(handle):
    feeder_id, ffpath = ACCOUNTS[handle]
    ff = json.load(open(OUT / ffpath))
    file_post_keys = {r.get("post_key") for b in ff.get("bites", []) for r in b.get("receipts", [])}
    file_aliases = ff.get("source_alias_map", {})
    file_post_keys |= set(file_aliases.values())

    with psycopg.connect(_dsn(), connect_timeout=20) as conn:
        oranks, pool = _overall_ranks(conn, feeder_id)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""select distinct on (fmc.post_key) fmc.post_key, fmc.parsed_output fp, p.posted_at, p.caption
                from feeder_file_model_calls fmc join posts p on p.post_key=fmc.post_key
                where fmc.call_type='fingerprint' and fmc.status='complete'
                  and fmc.prompt_version like 'fingerprint_v12%%' and p.feeder_id=%s
                order by fmc.post_key, fmc.completed_at desc""", (feeder_id,))
            fps = {r["post_key"]: r for r in cur.fetchall()}
        heldout = [k for k in fps if k not in file_post_keys and k in oranks]
        heldout.sort(key=lambda k: fps[k]["posted_at"], reverse=True)
        trigger = heldout[0] if heldout else max(fps, key=lambda k: fps[k]["posted_at"])
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""select distinct on (post_key) post_key, views, likes, comments,
                views_multiple, likes_multiple, comments_multiple, delta_from_d1
                from post_metrics where post_key=%s and lower(checkpoint)='d7'
                order by post_key, computed_at desc nulls last""", (trigger,))
            m = cur.fetchone()

    ranked_in_window = sorted([k for k in fps if k in oranks], key=lambda k: oranks[k])
    cur_pos = ranked_in_window.index(trigger)+1 if trigger in ranked_in_window else len(ranked_in_window)
    ov = oranks.get(trigger, pool)
    perf = {"rank_context": {"current": f"{cur_pos}/{len(ranked_in_window)}", "overall": f"{ov}/{pool}",
            "read": _read_phrase(cur_pos, len(ranked_in_window), ov, pool)}}
    anom = _anomalies(m) if m else []
    if anom: perf["anomalies"] = anom

    # move_memory = file bites minus any receipt of the trigger post
    mem = []
    for b in ff.get("bites", []):
        rec = [r for r in b.get("receipts", []) if r.get("post_key") != trigger]
        if rec:
            mem.append({"move": b["name"], "what_it_is": b.get("contract", {}),
                        "carries_vs_supports": b.get("weights_tally", {}),
                        "prior_instances": [{"how_it_showed_up": r.get("how_it_shows_up"),
                            "role": r.get("role_in_bite"), "ranked": r.get("rank_context", {})} for r in rec]})

    payload = {"account": {"handle": handle},
               "this_post": {"fingerprint": fps[trigger]["fp"], "performance": perf},
               "move_memory": mem}
    raw, usage = _call(D7_POSTMORTEM_SYSTEM_LIVE_V1.replace("{handle}", handle), payload)
    card = json.loads(_strip(raw))
    (OUT / f"{handle.replace('.','_')}_d7_postmortem.json").write_text(json.dumps({"trigger": trigger, "performance": perf, "card": card}, indent=2, ensure_ascii=False))

    print("="*74)
    print(f"{handle}  | trigger: {fps[trigger]['caption'][:46]!r}")
    print(f"  placed: {perf['rank_context']['read']}  | anomalies: {anom or 'none'}  | moves in memory: {len(mem)}  | {usage.get('total_tokens')} tok")
    print(f"\n  HEADLINE:  {card.get('headline')}")
    print(f"  TRIGGER:   {card.get('post_read')}")
    print(f"  FIT:       {card.get('fit')}")
    print(f"  RUN:       {card.get('account_momentum')}")
    print(f"  SIGNAL:    {card.get('signal')}")


if __name__ == "__main__":
    _env()
    for h in (sys.argv[1:] or list(ACCOUNTS)):
        run(h)
