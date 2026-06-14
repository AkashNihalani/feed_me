"""Compile the anuj.mp4 cold-start feeder file under feeder_file_cold_start_v8_1.

Reuses the exact 10-post window of the v7 compile (same aliases, same v12
fingerprints) so the only variables that change against v7 are the prompt and
the performance representation: bands/metric_shape are replaced by
rank_context {current, overall, read} plus worker-computed anomalies.

Writes the result to scripts/out/anuj_feeder_file_cold_start_v8_1.json,
persists the model call, inserts the feeder_files row as status 'draft', and
marks any older v8-family draft for this feeder superseded. Production D7
reads stay on the active v7 file until a draft is reviewed and activated.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

WORKER_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_DIR))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

from app.feeder_prompts import (  # noqa: E402
    FEEDER_FILE_COLD_START_PROMPT_VERSION,
    FEEDER_FILE_COLD_START_SYSTEM_V8_1,
)

ENV_PATH = WORKER_DIR / ".env"
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"

# Per-account source config: feeder id, handle, and the active v7 cold-start
# file whose alias map + frozen ranks we reuse so only the prompt/model vary.
ACCOUNTS = {
    "anuj.mp4": {"feeder_id": 27, "source_v7_file_id": 34},
    "lakmeindia": {"feeder_id": 35, "source_v7_file_id": 35},
}
DEFAULT_MODEL = "anthropic/claude-opus-4.8"
DEFAULT_MAX_TOKENS = 20000


def _load_env() -> None:
    for raw in ENV_PATH.read_text().splitlines():
        line = raw.strip()
        if line.startswith("export "):
            line = line[len("export "):]
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _pooler_dsn() -> str:
    dsn = os.environ["POSTGRES_DSN"]
    if "db.worqtdkvicuhmdgoncru.supabase.co" in dsn:
        dsn = dsn.replace("db.worqtdkvicuhmdgoncru.supabase.co", POOLER_HOST)
        dsn = dsn.replace("postgres:", "postgres.worqtdkvicuhmdgoncru:", 1)
    return dsn


def _strip_fences(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = t.split("\n", 1)[1] if "\n" in t else ""
        if t.rstrip().endswith("```"):
            t = t.rstrip()[:-3]
    return t.strip()


def _extract_json_object(text: str) -> str:
    """Tolerate prose/fences around the JSON: return the first balanced {...}."""
    t = _strip_fences(text)
    start = t.find("{")
    if start == -1:
        raise ValueError("no JSON object found in model output")
    depth, in_str, esc = 0, False, False
    for i in range(start, len(t)):
        c = t[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
        elif c == '"':
            in_str = True
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return t[start:i + 1]
    raise ValueError("unbalanced JSON object in model output")


def _persist_call(conn, *, handle: str, model: str, system_prompt: str, payload: dict,
                  alias_map: dict, raw: str, parsed: dict | None,
                  status: str, error: str | None, started) -> int:
    """Always store the raw model output — a parse failure must not lose the call."""
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.feeder_file_model_calls
              (call_key, call_type, feeder_handle, model, prompt_version,
               system_prompt, user_payload, raw_output, parsed_output,
               status, error, started_at, completed_at)
            values (%s, 'feeder_file_compile', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            returning id
            """,
            (
                f"feeder_file_compile:{handle}:{FEEDER_FILE_COLD_START_PROMPT_VERSION}:{model}",
                handle,
                model,
                FEEDER_FILE_COLD_START_PROMPT_VERSION,
                system_prompt,
                json.dumps({"feeder_file_payload": payload, "alias_map": alias_map}, ensure_ascii=False),
                raw,
                json.dumps(parsed, ensure_ascii=False) if parsed is not None else None,
                status,
                error,
                started,
            ),
        )
        return cur.fetchone()[0]


def _read_phrase(cur: int, n_cur: int, ov: int, n_ov: int) -> str:
    if cur == 1:
        batch = "strongest in the current batch"
    elif cur == n_cur:
        batch = "weakest of the current batch"
    elif cur / n_cur <= 1 / 3:
        batch = "upper third of the current batch"
    elif cur / n_cur <= 2 / 3:
        batch = "mid-pack in the current batch"
    else:
        batch = "lower end of the current batch"

    f = ov / n_ov
    if f <= 0.1:
        overall = "top-tier overall"
    elif f <= 1 / 3:
        overall = "upper-mid overall"
    elif f <= 2 / 3:
        overall = "mid overall"
    elif f <= 0.9:
        overall = "lower-mid overall"
    else:
        overall = "bottom-tier overall"
    return f"{batch}, {overall}"


def _anomalies(m: dict) -> list[str]:
    vm = float(m["views_multiple"]) if m["views_multiple"] is not None else None
    lm = float(m["likes_multiple"]) if m["likes_multiple"] is not None else None
    cm = float(m["comments_multiple"]) if m["comments_multiple"] is not None else None
    d = float(m["delta_from_d1"]) if m.get("delta_from_d1") is not None else None
    views, likes = m.get("views"), m.get("likes")
    out: list[tuple[float, str]] = []
    if vm and vm >= 3:
        out.append((vm, f"reach outlier — {vm:.1f}x the account's usual views"))
    if cm and cm >= 2.5:
        if vm and cm > vm * 1.3:
            out.append((cm, f"conversation spike — comments at {cm:.1f}x while views sat at {vm:.1f}x"))
        else:
            out.append((cm, f"heavy discussion — comments at {cm:.1f}x the usual"))
    if views and likes and likes >= views:
        out.append((99.0, "more likes than views — meme-level approval"))
    elif lm and lm >= 3 and (not vm or lm > vm * 1.3):
        out.append((lm, f"approval spike — likes at {lm:.1f}x, well above reach"))
    out.sort(key=lambda t: -t[0])
    lines = [s for _, s in out[:2]]
    if d is not None and d >= 5:
        lines.append("slow burn — kept climbing well past launch")
    elif d is not None and d <= -5:
        lines.append("peaked early then faded")
    return lines[:3]


def _build_payload(conn, *, handle: str, source_v7_file_id: int) -> tuple[dict, dict[str, str]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("select feed_file from public.feeder_files where id = %s", (source_v7_file_id,))
        v7 = cur.fetchone()["feed_file"]

    alias_map: dict[str, str] = v7["source_alias_map"]
    keys = list(alias_map.values())

    # overall rank per alias, copied unchanged from the v7 receipts (frozen at D7).
    overall_rank: dict[str, str] = {}
    for bite in v7["bites"]:
        for r in bite["receipts"]:
            overall_rank.setdefault(r["post"], r["rank"])
    missing = [a for a in alias_map if a not in overall_rank]
    if missing:
        raise RuntimeError(f"no v7 receipt covers aliases: {missing}")

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "select post_key, posted_at, caption, duration_seconds from public.posts where post_key = any(%s)",
            (keys,),
        )
        posts_meta = {r["post_key"]: r for r in cur.fetchall()}

        cur.execute(
            """
            select distinct on (post_key)
              post_key, views, likes, comments,
              views_multiple, likes_multiple, comments_multiple, delta_from_d1
            from public.post_metrics
            where post_key = any(%s) and lower(checkpoint) = 'd7'
            order by post_key, computed_at desc nulls last
            """,
            (keys,),
        )
        metrics = {r["post_key"]: r for r in cur.fetchall()}

        cur.execute(
            """
            select distinct on (post_key) post_key, parsed_output
            from public.feeder_file_model_calls
            where call_type = 'fingerprint'
              and status = 'complete'
              and prompt_version like 'fingerprint_v12%%'
              and post_key = any(%s)
            order by post_key, completed_at desc
            """,
            (keys,),
        )
        fingerprints = {r["post_key"]: r["parsed_output"] for r in cur.fetchall()}

    missing_fp = [k for k in keys if k not in fingerprints]
    if missing_fp:
        raise RuntimeError(f"missing v12 fingerprints for: {missing_fp}")

    # current-batch rank: order the window by overall rank position.
    def _ov_pos(alias: str) -> int:
        return int(overall_rank[alias].split("/")[0])

    ordered = sorted(alias_map, key=_ov_pos)
    current_rank = {alias: i + 1 for i, alias in enumerate(ordered)}
    n_cur = len(alias_map)

    posts = []
    for alias, key in alias_map.items():
        meta = posts_meta[key]
        m = metrics[key]
        ov_pos, ov_n = (int(x) for x in overall_rank[alias].split("/"))
        cur_pos = current_rank[alias]
        performance: dict = {
            "rank_context": {
                "current": f"{cur_pos}/{n_cur}",
                "overall": overall_rank[alias],
                "read": _read_phrase(cur_pos, n_cur, ov_pos, ov_n),
            }
        }
        anomalies = _anomalies(m)
        if anomalies:
            performance["anomalies"] = anomalies
        posts.append({
            "alias": alias,
            "caption": meta["caption"],
            "posted_at": meta["posted_at"].isoformat(),
            "duration_seconds": meta["duration_seconds"],
            "fingerprint": fingerprints[key],
            "performance": performance,
        })
    posts.sort(key=lambda p: p["posted_at"], reverse=True)

    dates = sorted(p["posted_at"][:10] for p in posts)
    payload = {
        "account": {"handle": handle},
        "window": {
            "posts": len(posts),
            "from": dates[0],
            "to": dates[-1],
            "source": "same 10 v12/v12.1 fingerprints as the v7 compile; ranks frozen at D7 from production DB",
        },
        "posts": posts,
    }
    return payload, alias_map


def _call_model(system_prompt: str, payload: dict, model: str, max_tokens: int) -> tuple[str, dict]:
    resp = requests.post(
        f"{os.environ.get('OPENROUTER_BASE_URL', 'https://openrouter.ai/api/v1').rstrip('/')}/chat/completions",
        headers={
            "Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://feedme.local",
            "X-Title": "FeedMe Feeder File Cold Start",
        },
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False, indent=2)},
            ],
            "temperature": 0.1,
            "max_tokens": max_tokens,
            "usage": {"include": True},
        },
        timeout=600,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"OpenRouter status {resp.status_code}: {resp.text[:1200]}")
    data = resp.json()
    content = data["choices"][0]["message"]["content"]
    return content, data.get("usage") or {}


def _validate(file: dict, alias_map: dict[str, str]) -> list[str]:
    problems: list[str] = []
    aliases = set(alias_map)
    names = file.get("post_names") or {}
    if set(names) != aliases:
        problems.append(f"post_names aliases mismatch: {sorted(set(names) ^ aliases)}")

    bites = file.get("bites") or []
    bite_names = {b.get("name") for b in bites}
    kinds = {"earned": 0, "candidate": 0, "grammar": 0}
    for bite in bites:
        bname = bite.get("name", "?")
        kind = bite.get("kind")
        if kind in kinds:
            kinds[kind] += 1
        else:
            problems.append(f"{bname}: bad kind {kind!r}")
        if kind == "candidate" and not bite.get("ttl"):
            problems.append(f"{bname}: candidate without ttl")
        receipts = bite.get("receipts") or []
        tally = {"core": 0, "supporting": 0, "standby": 0}
        for r in receipts:
            w = r.get("weight")
            if w not in tally:
                problems.append(f"{bname}: bad weight {w!r}")
                continue
            tally[w] += 1
            for ax in r.get("axis_bites") or []:
                if ax not in bite_names:
                    problems.append(f"{bname}: axis_bites references unknown bite {ax!r}")
        declared = bite.get("weights_tally") or {}
        # standby tallies may exceed written receipts when collapsed into also_present_in
        for k in ("core", "supporting"):
            if declared.get(k) is not None and declared[k] < tally[k]:
                problems.append(f"{bname}: weights_tally.{k}={declared.get(k)} < receipts {tally[k]}")
        if kind == "candidate" and tally["core"] + tally["supporting"] < 1:
            problems.append(f"{bname}: candidate with no core/supporting receipt")
    if kinds["earned"] > 8 or kinds["candidate"] > 3 or kinds["grammar"] > 2:
        problems.append(f"caps exceeded: {kinds}")
    return problems


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--handle", default="anuj.mp4", choices=sorted(ACCOUNTS), help="account to compile")
    ap.add_argument("--model", default=DEFAULT_MODEL, help="OpenRouter model slug")
    ap.add_argument("--out-suffix", default="", help="suffix for the output filename, e.g. _sonnet")
    ap.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS, help="maximum output tokens")
    args = ap.parse_args()
    model = args.model
    handle = args.handle
    cfg = ACCOUNTS[handle]
    feeder_id = cfg["feeder_id"]
    slug = handle.replace(".", "_")

    _load_env()
    system_prompt = FEEDER_FILE_COLD_START_SYSTEM_V8_1.replace("{handle}", handle)

    with psycopg.connect(_pooler_dsn(), autocommit=True, connect_timeout=20) as conn:
        payload, alias_map = _build_payload(conn, handle=handle, source_v7_file_id=cfg["source_v7_file_id"])
        print(f"account: {handle} (feeder {feeder_id})")
        print(f"payload: {len(payload['posts'])} posts, window {payload['window']['from']} -> {payload['window']['to']}")
        print(f"model: {model}")

        started = datetime.now(timezone.utc)
        raw, usage = _call_model(system_prompt, payload, model, args.max_tokens)
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        print(f"model done in {elapsed:.0f}s, usage: {usage.get('total_tokens')} tokens")

        # Always dump raw to disk and persist the call before parsing — a parse
        # failure must never discard a paid model output.
        raw_path = WORKER_DIR / "scripts" / "out" / f"{slug}_feeder_file_cold_start_v8_1{args.out_suffix}.raw.txt"
        raw_path.write_text(raw)

        try:
            feed_file = json.loads(_extract_json_object(raw))
        except (json.JSONDecodeError, ValueError) as exc:
            call_id = _persist_call(
                conn, handle=handle, model=model, system_prompt=system_prompt, payload=payload,
                alias_map=alias_map, raw=raw, parsed=None, status="error",
                error=f"parse failed: {exc}", started=started,
            )
            print(f"PARSE FAILED: {exc}")
            print(f"raw saved -> {raw_path}; model call {call_id} stored as status=error")
            sys.exit(1)

        problems = _validate(feed_file, alias_map)
        for p in problems:
            print(f"VALIDATION: {p}")

        feed_file["source_model"] = model
        feed_file["source_alias_map"] = alias_map
        feed_file["source_prompt_version"] = FEEDER_FILE_COLD_START_PROMPT_VERSION
        feed_file["compile_usage"] = usage
        feed_file["compiled_elapsed_seconds"] = round(elapsed, 2)

        out_path = WORKER_DIR / "scripts" / "out" / f"{slug}_feeder_file_cold_start_v8_1{args.out_suffix}.json"
        out_path.write_text(json.dumps(feed_file, indent=2, ensure_ascii=False))
        print(f"written -> {out_path}")

        call_id = _persist_call(
            conn, handle=handle, model=model, system_prompt=system_prompt, payload=payload,
            alias_map=alias_map, raw=raw, parsed=feed_file,
            status="complete", error=None, started=started,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                update public.feeder_files
                   set status = 'superseded', updated_at = now()
                 where feeder_id = %s and status = 'draft'
                   and compile_version like 'feeder_file_cold_start_v8%%'
                """,
                (feeder_id,),
            )
            cur.execute(
                """
                insert into public.feeder_files
                  (feeder_id, feeder_handle, feed_file, compile_version, status,
                   source, generated_at, updated_at)
                values (%s, %s, %s, %s, 'draft', %s, now(), now())
                on conflict (feeder_handle, compile_version) do update
                  set feed_file = excluded.feed_file,
                      status = 'draft',
                      source = excluded.source,
                      generated_at = now(),
                      updated_at = now()
                returning id
                """,
                (
                    feeder_id,
                    handle,
                    json.dumps(feed_file, ensure_ascii=False),
                    FEEDER_FILE_COLD_START_PROMPT_VERSION,
                    f"compile_anuj_cold_start_v8.py handle={handle} model={model} (model call {call_id})",
                ),
            )
            row_id = cur.fetchone()[0]
        print(f"persisted: model call {call_id}, feeder_files row {row_id} (status=draft)")
        if problems:
            print(f"\n{len(problems)} validation problem(s) above — review before activating.")


if __name__ == "__main__":
    main()
