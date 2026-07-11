"""Test: derive factual post cards (postcard_v2) from real DB fingerprints.

Selects up to 2 fingerprints per feeder (prefer 1 reel + 1 non-reel, richest
beats first), calls gpt-5.4-mini via OpenRouter with the v2 factual prompt,
writes cards to scripts/out/postcards_v2_factual/, then lints every card for
interpretation leakage (banned-language scan).

DB access is STRICTLY SELECT-ONLY. DSN never printed.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import requests

WORKER_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_DIR))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

ENV_PATHS = (WORKER_DIR / ".env", WORKER_DIR / ".env.vps-production")
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
POOLER_USER = "postgres.worqtdkvicuhmdgoncru"
PROMPT_PATH = WORKER_DIR / "scripts" / "postcard_v2_factual_prompt.md"
OUT_DIR = WORKER_DIR / "scripts" / "out" / "postcards_v2_factual"
DEFAULT_MODEL = "openai/gpt-5.4-mini"

WORD_CAPS = {"S": 140, "M": 240, "L": 280, "XL": 340}
TOKEN_CAPS = {"S": 400, "M": 600, "L": 700, "XL": 850}

BANNED_PATTERNS = [
    r"\brelatable\b", r"\bengaging\b", r"\bsatisfying\b", r"\baspirational\b",
    r"\bworks because\b", r"\bdesigned to\b", r"\bmeant to\b", r"\baims to\b",
    r"\bso that\b", r"\bsuggests\b", r"\bimplies\b", r"\blikely\b",
    r"\bseems (?:to|that)\b", r"\bappears (?:to|that)\b", r"\bviewers?\b", r"\baudience\b",
    r"\btakeaway\b", r"\breusable\b", r"\blesson\b", r"\beffective\b",
    r"\bclever\b", r"\bpremium\b", r"\bpolished\b", r"\bstrong\b",
]


def _load_env() -> None:
    for env_path in ENV_PATHS:
        if not env_path.exists():
            continue
        for raw in env_path.read_text().splitlines():
            line = raw.strip()
            if line.startswith("export "):
                line = line[len("export "):]
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _pooler(dsn: str) -> str:
    p = urlsplit(dsn)
    netloc = f"{POOLER_USER}:{p.password or ''}@{POOLER_HOST}:5432"
    path = p.path if p.path and p.path != "/" else "/postgres"
    return urlunsplit((p.scheme, netloc, path, "", ""))


SELECT_SQL = """
    select f.handle, p.feeder_id, p.post_key, p.media_type, p.duration_seconds,
           p.carousel_slide_count, pf.fingerprint
    from public.post_fingerprints pf
    join public.posts p on p.post_key = pf.post_key
    join public.feeders f on f.id = p.feeder_id
    where f.status = 'active'
    order by p.feeder_id, pf.generated_at desc
"""


def infer_tier(media_type: str, duration: float | None, slides: int | None) -> str:
    mt = (media_type or "").lower()
    if mt in ("image", "photo"):
        return "S"
    if mt == "carousel":
        n = slides or 0
        return "S" if n <= 3 else "M" if n <= 6 else "L" if n <= 10 else "XL"
    d = duration or 0
    return "S" if d <= 20 else "M" if d <= 45 else "L" if d <= 90 else "XL"


def pick_two(rows: list[dict]) -> list[dict]:
    """Prefer 1 reel + 1 non-reel; richest fingerprints (most beats) first."""
    def richness(r):
        fp = r["fingerprint"]
        if isinstance(fp, str):
            fp = json.loads(fp)
        return len(fp.get("visual_sequence") or []) + len(fp.get("visible_text") or [])
    reels = sorted([r for r in rows if (r["media_type"] or "").lower() == "reel"], key=richness, reverse=True)
    other = sorted([r for r in rows if (r["media_type"] or "").lower() != "reel"], key=richness, reverse=True)
    picked = []
    if reels:
        picked.append(reels[0])
    if other:
        picked.append(other[0])
    for r in reels[1:] + other[1:]:
        if len(picked) >= 2:
            break
        picked.append(r)
    return picked[:2]


def _call(system: str, payload: dict, model: str, max_tokens: int) -> str:
    resp = requests.post(
        f"{os.environ.get('OPENROUTER_BASE_URL', 'https://openrouter.ai/api/v1').rstrip('/')}/chat/completions",
        headers={"Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False, indent=2)},
            ],
            "temperature": 0.2,
            "max_tokens": max_tokens,
        },
        timeout=180,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"OpenRouter {resp.status_code}: {resp.text[:500]}")
    return str(resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")).strip()


def _word_count(text: str) -> int:
    return len([w for w in text.replace("/", " ").split() if w.strip()])


def write_card(system: str, item: dict, model: str) -> str:
    tier = item["tier"]
    fp = item["fingerprint"]
    payload = {
        "tier": tier,
        "handle": f"@{item['handle']}",
        "post_ref_without_suffix": item["post_key"].split("#", 1)[0],
        "fingerprint": fp,
    }
    text = _call(system, payload, model, TOKEN_CAPS[tier])
    for _ in range(2):
        if _word_count(text) <= WORD_CAPS[tier]:
            break
        text = _call(system, {**payload, "previous_card": text,
                              "rewrite_instruction": f"Rewrite under the Tier {tier} cap of {WORD_CAPS[tier]} words. Same sections. No new facts. Cut FLOW beats last."},
                     model, TOKEN_CAPS[tier])
    return text


def lint(text: str) -> list[str]:
    hits = []
    for pat in BANNED_PATTERNS:
        for m in re.finditer(pat, text, re.I):
            line = text[:m.start()].count("\n") + 1
            hits.append(f"L{line}: {m.group(0)}")
    return hits


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--per-feeder", type=int, default=2)
    ap.add_argument("--limit-feeders", type=int, default=0, help="0 = all")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    _load_env()
    system = PROMPT_PATH.read_text()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    dsn = os.environ["POSTGRES_DSN"]
    conn = psycopg.connect(_pooler(dsn), row_factory=dict_row)
    conn.execute("set default_transaction_read_only = on")
    cur = conn.cursor()
    cur.execute(SELECT_SQL)
    by_feeder: dict[int, list[dict]] = {}
    handles: dict[int, str] = {}
    for r in cur.fetchall():
        by_feeder.setdefault(r["feeder_id"], []).append(r)
        handles[r["feeder_id"]] = r["handle"]
    conn.close()

    items = []
    feeders = sorted(by_feeder)
    if args.limit_feeders:
        feeders = feeders[: args.limit_feeders]
    for fid in feeders:
        for r in pick_two(by_feeder[fid])[: args.per_feeder]:
            fp = r["fingerprint"]
            if isinstance(fp, str):
                fp = json.loads(fp)
            duration = r["duration_seconds"] or fp.get("duration_seconds")
            slides = r["carousel_slide_count"] or len(fp.get("slides") or []) or None
            if not duration and (r["media_type"] or "").lower() == "reel":
                # fall back to the last timestamp in the visual sequence
                seq = fp.get("visual_sequence") or []
                ends = []
                for beat in seq:
                    tr = str(beat.get("timestamp_range") or "")
                    m = re.findall(r"(\d+):(\d+)", tr)
                    if m:
                        ends.append(int(m[-1][0]) * 60 + int(m[-1][1]))
                duration = max(ends) if ends else None
            items.append({
                "handle": r["handle"], "post_key": r["post_key"],
                "media_type": r["media_type"], "fingerprint": fp,
                "tier": infer_tier(r["media_type"], duration, slides),
            })
    print(f"feeders with fingerprints: {len(feeders)}  cards to write: {len(items)}")

    results = []

    def job(item):
        safe = f"{item['handle']}_{item['post_key']}".replace("/", "_").replace("#", "_").replace(".", "_")
        path = OUT_DIR / f"{safe}.md"
        if path.exists() and not args.force:
            text = path.read_text()
            status = "cached"
        else:
            text = write_card(system, item, args.model)
            path.write_text(text)
            status = "written"
        return {**{k: item[k] for k in ("handle", "post_key", "media_type", "tier")},
                "status": status, "words": _word_count(text),
                "cap": WORD_CAPS[item["tier"]], "lint": lint(text), "file": str(path)}

    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(job, it): it for it in items}
        for fut in as_completed(futs):
            it = futs[fut]
            try:
                res = fut.result()
            except Exception as e:  # noqa: BLE001
                res = {"handle": it["handle"], "post_key": it["post_key"], "status": f"ERROR {e}"}
            results.append(res)
            print(json.dumps({k: res.get(k) for k in ("handle", "post_key", "tier", "status", "words", "cap")},
                             ensure_ascii=False), flush=True)

    (OUT_DIR / "_summary.json").write_text(json.dumps({"model": args.model, "items": results}, indent=1, ensure_ascii=False))
    over = [r for r in results if r.get("words", 0) > r.get("cap", 10**9)]
    dirty = [r for r in results if r.get("lint")]
    print(f"\ncards: {len(results)}  over-cap: {len(over)}  lint-hits: {len(dirty)}")
    for r in dirty:
        print(f"  {r['handle']} {r['post_key']}: {r['lint']}")


if __name__ == "__main__":
    main()
