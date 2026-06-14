"""Chunk -> decision -> deterministic apply end-to-end test.

Running file  = the latest/local cold-start or rolling feeder file.
Chunk         = the N most recent v12-fingerprinted posts NOT already in that file.
Chunker       = GPT 5.4 + FEEDER_FILE_CHUNK_SYSTEM_V1 by default.
Merger        = GPT 5.4 + FEEDER_FILE_MERGE_SYSTEM_V1 decision plan by default.
Apply         = deterministic feeder_merge_apply.apply_merge_plan.

The merger is handed: in_window_post_keys (running + chunk), the current
file's stamped bites, and the chunk's stamped bites. It returns a compact
decision plan. The server applies that plan to produce the rolled feeder file.

Writes chunk + merge-plan + merged outputs to scripts/out/ and persists model
calls when the DB schema allows those experimental call types.
Read-only against posts/metrics/fingerprints; writes only model-call rows.
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
    FEEDER_FILE_CHUNK_PROMPT_VERSION,
    FEEDER_FILE_CHUNK_SYSTEM_V1,
    FEEDER_FILE_MERGE_PROMPT_VERSION,
    FEEDER_FILE_MERGE_SYSTEM_V1,
)
from app.feeder_merge_apply import apply_merge_plan, stamp_receipts  # noqa: E402

ENV_PATH = WORKER_DIR / ".env"
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
OUT = WORKER_DIR / "scripts" / "out"

ACCOUNTS = {"anuj.mp4": 27, "lakmeindia": 35}
DEFAULT_RUNNING_MODEL = "openai/gpt-5.4"
DEFAULT_CHUNKER_MODEL = "openai/gpt-5.4"
DEFAULT_MERGER_MODEL = "openai/gpt-5.4"
MAX_TOKENS = 20000


def _load_env() -> None:
    for raw in ENV_PATH.read_text().splitlines():
        line = raw.strip()
        if line.startswith("export "):
            line = line[len("export "):]
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _pooler_dsn() -> str:
    dsn = os.environ["POSTGRES_DSN"]
    if "db.worqtdkvicuhmdgoncru.supabase.co" in dsn:
        dsn = dsn.replace("db.worqtdkvicuhmdgoncru.supabase.co", POOLER_HOST)
        dsn = dsn.replace("postgres:", "postgres.worqtdkvicuhmdgoncru:", 1)
    return dsn


def _resolve_feeder_id(conn, handle: str) -> int:
    if handle in ACCOUNTS:
        return ACCOUNTS[handle]
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("select id from public.feeders where lower(handle)=lower(%s) limit 1", (handle,))
        row = cur.fetchone()
    if not row:
        raise RuntimeError(f"unknown feeder handle: {handle}")
    return int(row["id"])


def _extract_json_object(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = t.split("\n", 1)[1] if "\n" in t else ""
        if t.rstrip().endswith("```"):
            t = t.rstrip()[:-3]
    t = t.strip()
    start = t.find("{")
    if start == -1:
        raise ValueError("no JSON object in output")
    depth, in_str, esc = 0, False, False
    for i in range(start, len(t)):
        c = t[i]
        if in_str:
            esc = (c == "\\" and not esc)
            if c == '"' and not esc:
                in_str = False
        elif c == '"':
            in_str = True
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return t[start:i + 1]
    raise ValueError("unbalanced JSON object")


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
    overall = ("top-tier overall" if f <= 0.1 else "upper-mid overall" if f <= 1 / 3
               else "mid overall" if f <= 2 / 3 else "lower-mid overall" if f <= 0.9
               else "bottom-tier overall")
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


def _call(model: str, system: str, user: dict) -> tuple[str, dict, float]:
    started = datetime.now(timezone.utc)
    resp = requests.post(
        f"{os.environ.get('OPENROUTER_BASE_URL', 'https://openrouter.ai/api/v1').rstrip('/')}/chat/completions",
        headers={"Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}", "Content-Type": "application/json",
                 "HTTP-Referer": "https://feedme.local", "X-Title": "FeedMe Chunk/Merge"},
        json={"model": model, "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False, indent=2)},
        ], "temperature": 0.1, "max_tokens": MAX_TOKENS, "usage": {"include": True}},
        timeout=600,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"{model} status {resp.status_code}: {resp.text[:800]}")
    data = resp.json()
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    message = data["choices"][0].get("message") or {}
    content = message.get("content")
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError(f"{model} returned empty content: {json.dumps(data, ensure_ascii=False)[:2000]}")
    return content, (data.get("usage") or {}), elapsed


def _persist(conn, *, call_type, handle, model, prompt_version, system, user, raw, parsed, elapsed):
    with conn.cursor() as cur:
        try:
            cur.execute(
                """insert into public.feeder_file_model_calls
                   (call_key, call_type, feeder_handle, model, prompt_version, system_prompt,
                    user_payload, raw_output, parsed_output, status, started_at, completed_at)
                   values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now() - (%s || ' seconds')::interval, now())
                   returning id""",
                (f"{call_type}:{handle}:{prompt_version}:{model}", call_type, handle, model, prompt_version,
                 system, json.dumps(user, ensure_ascii=False), raw,
                 json.dumps(parsed, ensure_ascii=False) if parsed else None,
                 "complete" if parsed else "error", str(round(elapsed, 1))))
            return cur.fetchone()[0]
        except Exception as exc:
            print(f"  warn: skipped persisting {call_type} model call ({exc})")
            return None


def _overall_ranks(conn, feeder_id: int) -> tuple[dict[str, int], int]:
    """Rank every 90d D7 reel of the feeder by percentile (best=1)."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """select distinct on (p.post_key) p.post_key, pm.percentile_performance_exact as pct
               from posts p join post_metrics pm on pm.post_key=p.post_key and lower(pm.checkpoint)='d7'
               where p.feeder_id=%s and lower(coalesce(p.media_type,''))='reel'
                 and p.posted_at >= now() - interval '90 days'
               order by p.post_key, pm.computed_at desc nulls last""", (feeder_id,))
        rows = [r for r in cur.fetchall() if r["pct"] is not None]
    rows.sort(key=lambda r: float(r["pct"]))
    ranks = {r["post_key"]: i + 1 for i, r in enumerate(rows)}
    return ranks, len(rows)


def _build_chunk_payload(conn, *, handle, feeder_id, running_keys: set[str], n: int):
    overall_ranks, pool = _overall_ranks(conn, feeder_id)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """select distinct on (fmc.post_key) fmc.post_key, fmc.parsed_output as fp,
                      p.posted_at, p.caption, p.duration_seconds
               from feeder_file_model_calls fmc
               join posts p on p.post_key=fmc.post_key
               where fmc.call_type='fingerprint' and fmc.status='complete'
                 and fmc.prompt_version like 'fingerprint_v12%%' and p.feeder_id=%s
               order by fmc.post_key, fmc.completed_at desc""", (feeder_id,))
        fps = {r["post_key"]: r for r in cur.fetchall()}
        keys = [k for k in fps if k not in running_keys and k in overall_ranks]
        keys.sort(key=lambda k: fps[k]["posted_at"], reverse=True)
        chunk_keys = keys[:n]
        cur.execute(
            """select distinct on (post_key) post_key, views, likes, comments,
                      views_multiple, likes_multiple, comments_multiple, delta_from_d1
               from post_metrics where post_key = any(%s) and lower(checkpoint)='d7'
               order by post_key, computed_at desc nulls last""", (chunk_keys,))
        metrics = {r["post_key"]: r for r in cur.fetchall()}

    ordered = sorted(chunk_keys, key=lambda k: overall_ranks[k])
    cur_rank = {k: i + 1 for i, k in enumerate(ordered)}
    posts = []
    for k in sorted(chunk_keys, key=lambda k: fps[k]["posted_at"], reverse=True):
        m = metrics[k]
        perf = {"rank_context": {
            "current": f"{cur_rank[k]}/{len(chunk_keys)}",
            "overall": f"{overall_ranks[k]}/{pool}",
            "read": _read_phrase(cur_rank[k], len(chunk_keys), overall_ranks[k], pool)}}
        anom = _anomalies(m)
        if anom:
            perf["anomalies"] = anom
        posts.append({"alias": f"P{len(posts)+1:02d}", "post_key": k,
                      "caption": fps[k]["caption"], "posted_at": fps[k]["posted_at"].isoformat(),
                      "duration_seconds": fps[k]["duration_seconds"], "fingerprint": fps[k]["fp"],
                      "performance": perf})
    dates = sorted(p["posted_at"][:10] for p in posts)
    payload = {"account": {"handle": handle},
               "chunk": {"posts": len(posts), "from": dates[0], "to": dates[-1]},
               "posts": posts}
    alias_to_key = {p["alias"]: p["post_key"] for p in posts}
    return payload, alias_to_key, chunk_keys


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--handle", required=True)
    ap.add_argument("--chunk-size", type=int, default=5)
    ap.add_argument("--running-model", default=DEFAULT_RUNNING_MODEL)
    ap.add_argument("--running-prompt-version")
    ap.add_argument("--running-file")
    ap.add_argument("--chunker-model", default=DEFAULT_CHUNKER_MODEL)
    ap.add_argument("--merger-model", default=DEFAULT_MERGER_MODEL)
    ap.add_argument("--output-suffix", default="v1")
    ap.add_argument("--chunk-file")
    args = ap.parse_args()
    handle = args.handle
    slug = handle.replace(".", "_")
    chunk_file = OUT / f"{slug}_chunk_{args.output_suffix}.json"
    chunk_raw_file = OUT / f"{slug}_chunk_{args.output_suffix}.raw.txt"
    plan_file = OUT / f"{slug}_merge_plan_{args.output_suffix}.json"
    plan_raw_file = OUT / f"{slug}_merge_plan_{args.output_suffix}.raw.txt"
    merged_file = OUT / f"{slug}_merged_{args.output_suffix}.json"
    _load_env()

    with psycopg.connect(_pooler_dsn(), autocommit=True, connect_timeout=20) as conn:
        feeder_id = _resolve_feeder_id(conn, handle)
        if args.running_file:
            running = json.loads(Path(args.running_file).read_text())
            running_aliasmap = running.get("source_alias_map") or {}
        else:
            # running file = latest matching cold-start compile
            with conn.cursor(row_factory=dict_row) as cur:
                prompt_filter = ""
                params = [handle, args.running_model]
                if args.running_prompt_version:
                    prompt_filter = " and prompt_version=%s"
                    params.append(args.running_prompt_version)
                cur.execute(
                    f"""select parsed_output, user_payload from feeder_file_model_calls
                       where call_type='feeder_file_compile' and feeder_handle=%s
                         and model=%s and status='complete'{prompt_filter}
                       order by completed_at desc limit 1""", params)
                row = cur.fetchone()
            if not row:
                raise RuntimeError(f"no complete feeder_file_compile found for {handle} model={args.running_model}")
            running = row["parsed_output"]
            running_aliasmap = (row["user_payload"] or {}).get("alias_map") or running.get("source_alias_map") or {}
        if len(running_aliasmap) != 10:
            raise RuntimeError(f"running base must be exactly 10 posts; got {len(running_aliasmap)}")
        running_keys = set(running_aliasmap.values())
        print(f"running file: {len(running.get('bites', []))} bites over {len(running_keys)} posts")

        # CHUNK
        if args.chunk_file:
            chunk_out = json.loads(Path(args.chunk_file).read_text())
            chunk_alias_key = chunk_out.get("source_alias_map") or {}
            chunk_keys = list(chunk_alias_key.values())
            chunk_payload = {"account": {"handle": handle}, "posts": []}
            print(f"chunk: reused {Path(args.chunk_file).name} with {len(chunk_keys)} posts")
        else:
            chunk_payload, chunk_alias_key, chunk_keys = _build_chunk_payload(
                conn, handle=handle, feeder_id=feeder_id, running_keys=running_keys, n=args.chunk_size)
            print(f"chunk: {len(chunk_payload['posts'])} posts {chunk_payload['chunk']['from']}..{chunk_payload['chunk']['to']}")
            raw, usage, elapsed = _call(args.chunker_model, FEEDER_FILE_CHUNK_SYSTEM_V1.replace("{handle}", handle), chunk_payload)
            chunk_raw_file.write_text(raw)
            chunk_out = json.loads(_extract_json_object(raw))
            chunk_out["source_running_model"] = args.running_model
            chunk_out["source_model"] = args.chunker_model
            chunk_out["source_alias_map"] = chunk_alias_key
            chunk_file.write_text(json.dumps(chunk_out, indent=2, ensure_ascii=False))
            _persist(conn, call_type="feeder_file_chunk", handle=handle, model=args.chunker_model,
                     prompt_version=FEEDER_FILE_CHUNK_PROMPT_VERSION,
                     system=FEEDER_FILE_CHUNK_SYSTEM_V1.replace("{handle}", handle),
                     user=chunk_payload, raw=raw, parsed=chunk_out, elapsed=elapsed)
            print(f"  chunker: {elapsed:.0f}s, {usage.get('total_tokens')} tok, "
                  f"{len(chunk_out.get('bites', []))} bites: {[b['name'] for b in chunk_out.get('bites', [])]}")

        # MERGE DECISION + DETERMINISTIC APPLY
        in_window = sorted(running_keys | set(chunk_keys))
        current_for_merge = stamp_receipts(
            json.loads(json.dumps({
                "bites": running.get("bites", []),
                "post_names": running.get("post_names", {}),
            }, ensure_ascii=False)),
            "current",
            running_aliasmap,
        )
        chunk_for_merge = stamp_receipts(
            json.loads(json.dumps({
                "bites": chunk_out.get("bites", []),
                "post_names": chunk_out.get("post_names", {}),
            }, ensure_ascii=False)),
            "chunk",
            chunk_alias_key,
        )
        merge_payload = {"handle": handle,
                         "in_window_post_keys": in_window,
                         "current_file": current_for_merge,
                         "chunk": chunk_for_merge}
        raw_m, usage_m, elapsed_m = _call(args.merger_model, FEEDER_FILE_MERGE_SYSTEM_V1.replace("{handle}", handle), merge_payload)
        plan_raw_file.write_text(raw_m)
        plan = json.loads(_extract_json_object(raw_m))
        plan["_run"] = {"model": args.merger_model, "elapsed_seconds": round(elapsed_m, 1), "usage": usage_m}
        plan_file.write_text(json.dumps(plan, indent=2, ensure_ascii=False))
        merged = apply_merge_plan(merge_payload, plan)
        merged["source_running_model"] = args.running_model
        merged["source_chunker"] = args.chunker_model
        merged["source_merger"] = args.merger_model
        merged["source_merge_plan"] = plan_file.name
        merged_file.write_text(json.dumps(merged, indent=2, ensure_ascii=False))
        _persist(conn, call_type="feeder_file_merge", handle=handle, model=args.merger_model,
                 prompt_version=FEEDER_FILE_MERGE_PROMPT_VERSION,
                 system=FEEDER_FILE_MERGE_SYSTEM_V1.replace("{handle}", handle),
                 user=merge_payload, raw=raw_m, parsed=plan, elapsed=elapsed_m)
        print(f"  merger: {elapsed_m:.0f}s, {usage_m.get('total_tokens')} tok "
              f"(prompt {usage_m.get('prompt_tokens')}), plan -> merged {len(merged.get('bites', []))} bites")

        # quick diff
        run_names = {b["name"] for b in running.get("bites", [])}
        chunk_names = {b["name"] for b in chunk_out.get("bites", [])}
        merged_names = {b["name"] for b in merged.get("bites", [])}
        print("\n--- merge diff ---")
        print("carried/renamed from running :", sorted(run_names & merged_names))
        print("dropped from running        :", sorted(run_names - merged_names))
        print("new in merged (from chunk)  :", sorted(merged_names - run_names))
        print(f"warnings                   : {merged.get('merge_warnings', [])}")
        print(f"\nfiles -> {chunk_file.name}, {plan_file.name}, {merged_file.name}")


if __name__ == "__main__":
    main()
