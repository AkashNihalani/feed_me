"""Feeder Book smoke test for two 10-card updates.

Uses existing reel-card JSON, enriches it with D7 rank/landing, calls OpenRouter,
and writes payload/raw/parsed JSON under scripts/out/feeder_books.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[3]
WORKER_DIR = ROOT / "apps" / "worker"
CARD_DIR = WORKER_DIR / "scripts" / "out" / "reel_context_cards"
OUT_DIR = WORKER_DIR / "scripts" / "out" / "feeder_books"
ENV_PATHS = (WORKER_DIR / ".env", WORKER_DIR / ".env.vps-production")
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
DEFAULT_MODEL = "anthropic/claude-sonnet-4.6"
RUN_SIZE = 10
NEEDED_CARDS = 30

if str(WORKER_DIR) not in sys.path:
    sys.path.insert(0, str(WORKER_DIR))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402


PROMPT_VERSION = "feeder_book_update_v1"
SYSTEM_PROMPT = """
FEEDER BOOK UPDATE

You are not writing a report. You are updating a working theory of @{handle}.

The Feeder Book is the account's living strategic memory. It stores what the feeder is currently proving, what evidence supports it, what is still uncertain, and what would make a read false.

Do not write frontend copy.
Do not write a Bite Run.
Do not create content ideas.

Your job is only to update the Feeder Book.

INPUT

You are given:

* existing_feeder_book, if available
* feeder_file: recent reel cards
* new_drop: latest 10 reel cards
* performance: rank and landing read for latest 10

If existing_feeder_book is empty, create the first Feeder Book from new_drop and performance.

The latest 10 are the newest evidence.
The feeder_file gives continuity.
The existing_feeder_book is a prior theory, not something to defend.

Challenge every read. Defend only the evidence.

WHAT A READ IS

A read is an account truth.

It is not a pattern label.

Bad reads:

* "location jokes work"
* "props work"
* "food performs"
* "regional comedy"
* "campaign posts are strong"

Good reads:

* "Ordinary embarrassment cuts harder when one painfully exact proof point carries the feeling."
* "Campaigns survive when the brand enters after the scene has already earned attention."
* "Food becomes watchable here only when the dish does the pulling before host praise arrives."

A read must be:

* specific to this feeder
* supported by posts
* written as a working opinion
* narrow enough to be useful
* falsifiable

FEEDER BOOK SHAPE

Return:

{
"active_reads": [
{
"claim": "",
"because": [
{
"id": "",
"display_tag": "",
"age": "",
"evidence_note": ""
}
],
"tension": "",
"would_change_if": "",
"missed_runs": 0
}
],
"watching": [
{
"question": "",
"why_open": "",
"what_would_confirm": "",
"what_would_kill_it": ""
}
],
"sleeping": [
{
"claim": "",
"why_sleeping": "",
"last_real_proof": ""
}
],
"retired": [
{
"claim": "",
"why_retired": ""
}
],
"movement_notes": [
{
"type": "sharpened | narrowed | widened | weakened | new | sleeping | retired | unchanged",
"read": "",
"why": "",
"posts": [
{ "id": "", "display_tag": "", "age": "" }
]
}
]
}

UPDATE RULES

If there is no existing_feeder_book:

* create 3-6 active_reads
* create 2-5 watching questions
* do not overclaim from the first 10
* prefer "watching" when evidence is promising but thin

If there is an existing_feeder_book:

* test each active read against the latest 10
* update the claim only if the latest 10 sharpen, narrow, widen, weaken, split, merge, or contradict it
* increment missed_runs if the latest 10 do not refresh it
* reset missed_runs to 0 if fresh evidence meaningfully supports it
* move to sleeping if missed_runs reaches 3
* move to retired if would_change_if clearly fires

Do not add a new read if it is just a smaller, broader, or weaker version of an existing read.

Instead:

* sharpen the existing read
* narrow its condition
* merge duplicates
* split only when evidence shows two truths were mixed
* retire if the read is contradicted

Partial resemblance is not proof.

A post supports a read only if it meaningfully strengthens the claim, not because it vaguely matches.

One deep winner cannot rewrite the feeder. It can strengthen only the read it directly proves.

PERFORMANCE

Rank is gravity. Lower rank is better.

Use performance to size evidence, not to create reads blindly.

A top-ranked post matters more only if the reel actually proves the read.
A soft post can still matter if it exposes a limit.
A campaign, collab, trend, launch, or live moment can count if the feeder made it native.
If the moment or collaborator carried the result more than the feeder, note that.

POST REFS

Only cite real ids from feeder_file or new_drop.

Use:

{ "id": "", "display_tag": "", "age": "" }

Write display_tag yourself, under 5 words.
Age must be short and relative, based on posted_at.
evidence_note should preserve the useful nuance in one sentence.

VOICE

The Feeder Book is internal, not frontend.

Write clearly, sharply, and compactly.

No dashboard language.
No generic marketing language.
No poetic overreach.

Every claim should help a future run understand the feeder better.

RETURN ONLY JSON.
""".strip()


ACCOUNTS = {
    "anuj.mp4": "anuj_mp4",
    "traya.health": "traya_health",
}


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

    dsn = os.environ.get("POSTGRES_DSN", "")
    if "db.worqtdkvicuhmdgoncru.supabase.co" in dsn:
        dsn = dsn.replace("db.worqtdkvicuhmdgoncru.supabase.co", POOLER_HOST)
        dsn = dsn.replace("postgres:", "postgres.worqtdkvicuhmdgoncru:", 1)
        os.environ["POSTGRES_DSN"] = dsn


def _extract_json_object(text: str) -> str:
    raw = str(text or "").strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else ""
        if raw.rstrip().endswith("```"):
            raw = raw.rstrip()[:-3]
    start = raw.find("{")
    if start < 0:
        raise ValueError("no JSON object found")
    depth = 0
    in_string = False
    escape = False
    for index in range(start, len(raw)):
        char = raw[index]
        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
        elif char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return raw[start:index + 1]
    raise ValueError("unbalanced JSON object")


def _card_files(slug: str) -> list[Path]:
    return sorted(
        path for path in CARD_DIR.glob(f"{slug}*reel_context_extractor_v6_openai_gpt_5_4_mini.json")
        if ".payload." not in path.name and path.name.endswith(".json")
    )


def _load_cards(slug: str) -> dict[str, dict[str, Any]]:
    cards: dict[str, dict[str, Any]] = {}
    for path in _card_files(slug):
        data = json.loads(path.read_text())
        for card in data.get("cards") or []:
            post_key = str(card.get("post_key") or "").strip()
            if post_key and post_key not in cards:
                cards[post_key] = dict(card)
    return cards


def _metadata(conn: Any, post_keys: list[str]) -> dict[str, dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select distinct on (p.post_key)
              p.post_key, p.posted_at, p.post_url, p.caption, p.feeder_id, f.handle,
              coalesce(p.collab_post, false) collab_post,
              pm.views, pm.likes, pm.comments, pm.percentile_performance_exact,
              pm.percentile_performance, pm.computed_at
            from public.posts p
            join public.feeders f on f.id = p.feeder_id
            join public.post_metrics pm on pm.post_key = p.post_key and lower(pm.checkpoint) = 'd7'
            where p.post_key = any(%s)
              and lower(coalesce(p.media_type, '')) = 'reel'
            order by p.post_key, pm.computed_at desc nulls last
            """,
            (post_keys,),
        )
        rows = {str(row["post_key"]): dict(row) for row in cur.fetchall()}

    if not rows:
        return {}

    feeder_id = int(next(iter(rows.values()))["feeder_id"])
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select distinct on (p.post_key) p.post_key, pm.percentile_performance_exact, pm.views
            from public.posts p
            join public.post_metrics pm on pm.post_key = p.post_key and lower(pm.checkpoint) = 'd7'
            where p.feeder_id = %s
              and lower(coalesce(p.media_type, '')) = 'reel'
              and p.posted_at >= now() - interval '90 days'
            order by p.post_key, pm.computed_at desc nulls last
            """,
            (feeder_id,),
        )
        pool_rows = [
            dict(row) for row in cur.fetchall()
            if row.get("percentile_performance_exact") is not None
        ]

    ranked = sorted(
        pool_rows,
        key=lambda row: (float(row["percentile_performance_exact"]), -float(row.get("views") or 0)),
    )
    pool = len(ranked) or 1
    ranks = {str(row["post_key"]): index + 1 for index, row in enumerate(ranked)}
    for post_key, row in rows.items():
        row["rank"] = ranks.get(post_key)
        row["pool"] = pool
    return rows


def _landing(row: dict[str, Any]) -> str:
    rank = row.get("rank")
    pool = row.get("pool")
    pct = row.get("percentile_performance_exact") or row.get("percentile_performance")
    if rank and pool:
        return f"rank {rank} of {pool} across recent D7 reels; lower rank is better"
    if pct is not None:
        return f"D7 percentile {float(pct):.1f}; lower is better"
    return "D7 rank unavailable"


def _reel(card: dict[str, Any], meta: dict[str, Any]) -> dict[str, Any]:
    post_key = str(card["post_key"])
    posted_at = meta.get("posted_at")
    return {
        "id": post_key,
        "post_key": post_key,
        "url": meta.get("post_url"),
        "posted_at": posted_at.isoformat() if hasattr(posted_at, "isoformat") else posted_at,
        "caption": meta.get("caption"),
        "what_happened": card.get("summary"),
        "job": card.get("aim"),
        "driver": card.get("proof"),
        "start": card.get("open"),
        "end": card.get("close"),
        "form": card.get("package"),
        "collab": bool(meta.get("collab_post")),
    }


def _performance(reels: list[dict[str, Any]], meta: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for reel in reels:
        row = meta[reel["post_key"]]
        out.append(
            {
                "id": reel["id"],
                "rank": row.get("rank"),
                "pool": row.get("pool"),
                "landing": _landing(row),
                "views": row.get("views"),
                "likes": row.get("likes"),
                "comments": row.get("comments"),
            }
        )
    return out


def _call_openrouter(model: str, handle: str, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    if not os.environ.get("OPENROUTER_API_KEY"):
        raise RuntimeError("OPENROUTER_API_KEY is missing")
    resp = requests.post(
        f"{os.environ.get('OPENROUTER_BASE_URL', 'https://openrouter.ai/api/v1').rstrip('/')}/chat/completions",
        headers={
            "Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://feedme.local",
            "X-Title": "FeedMe Feeder Book Test",
        },
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT.replace("@{handle}", f"@{handle}")},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False, indent=2, default=str)},
            ],
            "temperature": float(os.getenv("FEEDER_BOOK_TEMPERATURE", "0.35")),
            "max_tokens": int(os.getenv("FEEDER_BOOK_MAX_TOKENS", "7000")),
            "usage": {"include": True},
        },
        timeout=300,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"OpenRouter status {resp.status_code}: {resp.text[:1200]}")
    raw = resp.json()["choices"][0]["message"]["content"]
    return raw, json.loads(_extract_json_object(raw))


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


def run_handle(conn: Any, handle: str, model: str, no_call: bool) -> dict[str, Any]:
    slug = ACCOUNTS[handle]
    cards = _load_cards(slug)
    meta = _metadata(conn, list(cards))
    usable = [post_key for post_key in cards if post_key in meta]
    usable.sort(key=lambda key: meta[key]["posted_at"])
    if len(usable) < NEEDED_CARDS:
        raise RuntimeError(f"{handle}: only {len(usable)} usable local D7 reel cards; need {NEEDED_CARDS}")

    selected = usable[:NEEDED_CARDS]
    windows = [selected[:RUN_SIZE], selected[RUN_SIZE:RUN_SIZE * 2]]
    feeder_file: list[dict[str, Any]] = []
    feeder_book: dict[str, Any] | None = None
    outputs = []

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for run_no, keys in enumerate(windows, start=1):
        new_drop = [_reel(cards[key], meta[key]) for key in keys]
        payload = {
            "account": {"handle": handle},
            "existing_feeder_book": feeder_book or {},
            "feeder_file": feeder_file,
            "new_drop": new_drop,
            "performance": _performance(new_drop, meta),
        }
        prefix = OUT_DIR / f"{slug}_feeder_book_run{run_no}"
        _write_json(prefix.with_suffix(".payload.json"), payload)
        if no_call:
            parsed = {}
            raw = ""
        else:
            raw, parsed = _call_openrouter(model, handle, payload)
            prefix.with_suffix(".raw.txt").write_text(raw)
            _write_json(prefix.with_suffix(".json"), parsed)
            feeder_book = parsed

        # ponytail: two-run test only; the final 10 selected cards are holdout evidence for a later third update.
        feeder_file = [*feeder_file, *new_drop][-NEEDED_CARDS:]
        outputs.append({"run": run_no, "payload": str(prefix.with_suffix(".payload.json")), "json": str(prefix.with_suffix(".json")) if parsed else None})

    summary = {
        "handle": handle,
        "model": model,
        "prompt_version": PROMPT_VERSION,
        "selected_cards": selected,
        "used_cards": windows[0] + windows[1],
        "holdout_cards": selected[RUN_SIZE * 2:],
        "outputs": outputs,
    }
    _write_json(OUT_DIR / f"{slug}_feeder_book_summary.json", summary)
    return summary


def _self_test() -> None:
    assert json.loads(_extract_json_object('```json\n{"ok": true}\n```')) == {"ok": True}
    assert json.loads(_extract_json_object('noise {"a": {"b": 1}} tail'))["a"]["b"] == 1


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handle", action="append", choices=sorted(ACCOUNTS), help="Default: anuj.mp4 and traya.health")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--no-call", action="store_true", help="Build payloads only.")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        _self_test()
        print("self-test ok")
        return

    _load_env()
    handles = args.handle or list(ACCOUNTS)
    with psycopg.connect(os.environ["POSTGRES_DSN"], connect_timeout=20, row_factory=dict_row, autocommit=True) as conn:
        for handle in handles:
            summary = run_handle(conn, handle, args.model, args.no_call)
            print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
