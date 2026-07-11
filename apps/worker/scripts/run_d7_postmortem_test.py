"""D7 post-mortem smoke test using the production D7 payload builder."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

WORKER_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_DIR))
import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402
from app.fingerprint_intelligence import (  # noqa: E402
    _build_d7_read_input,
    _call_d7_read_model,
    _normalize_d7_read_mapping,
)

ENV = WORKER_DIR / ".env"
OUT = WORKER_DIR / "scripts" / "out"

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

def _overall_ranks(conn, feeder_id):
    with conn.cursor() as cur:
        cur.execute("""select distinct on (p.post_key) p.post_key, pm.percentile_performance_exact pct
            from posts p join post_metrics pm on pm.post_key=p.post_key and lower(pm.checkpoint)='d7'
            where p.feeder_id=%s and lower(coalesce(p.media_type,''))='reel'
              and p.posted_at >= now() - interval '90 days'
            order by p.post_key, pm.computed_at desc nulls last""", (feeder_id,))
        rows = [r for r in cur.fetchall() if r["pct"] is not None]
    rows.sort(key=lambda r: float(r["pct"]))
    return {r["post_key"]: i+1 for i, r in enumerate(rows)}, len(rows)

def run(handle):
    feeder_id, ffpath = ACCOUNTS[handle]
    ff = json.load(open(OUT / ffpath))
    file_post_keys = {r.get("post_key") for b in ff.get("bites", []) for r in b.get("receipts", [])}
    file_aliases = ff.get("source_alias_map", {})
    file_post_keys |= set(file_aliases.values())

    with psycopg.connect(_dsn(), connect_timeout=20, row_factory=dict_row) as conn:
        oranks, pool = _overall_ranks(conn, feeder_id)
        with conn.cursor() as cur:
            cur.execute("""select distinct on (fmc.post_key) fmc.post_key, fmc.parsed_output fp, p.posted_at, p.caption
                from feeder_file_model_calls fmc join posts p on p.post_key=fmc.post_key
                where fmc.call_type='fingerprint' and fmc.status='complete'
                  and fmc.prompt_version like 'fingerprint_v12%%' and p.feeder_id=%s
                order by fmc.post_key, fmc.completed_at desc""", (feeder_id,))
            fps = {r["post_key"]: r for r in cur.fetchall()}
        heldout = [k for k in fps if k not in file_post_keys and k in oranks]
        heldout.sort(key=lambda k: fps[k]["posted_at"], reverse=True)
        trigger = heldout[0] if heldout else max(fps, key=lambda k: fps[k]["posted_at"])
        payload = _build_d7_read_input(conn, trigger)
    if not payload:
        raise RuntimeError(f"{handle}: production D7 payload unavailable for {trigger}")
    result = _call_d7_read_model(payload, post_key=trigger)
    if not result:
        raise RuntimeError(f"{handle}: D7 model did not return a result")
    raw, parsed, error = result
    card = _normalize_d7_read_mapping(parsed, post_key=trigger) if parsed else None
    (OUT / f"{handle.replace('.','_')}_d7_postmortem.json").write_text(json.dumps({"trigger": trigger, "payload": payload, "card": card, "error": error, "raw": raw if not card else None}, indent=2, ensure_ascii=False, default=str))

    print("="*74)
    print(f"{handle}  | trigger: {fps[trigger]['caption'][:46]!r}")
    print(f"  feeder posts: {len(payload['feeder_file']['posts'])}")
    print(json.dumps(card or {"error": error}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    _env()
    for h in (sys.argv[1:] or list(ACCOUNTS)):
        run(h)
