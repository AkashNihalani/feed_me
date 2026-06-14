from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import requests
from psycopg.rows import dict_row

WORKER_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_DIR))

from app.feeder_prompts import FEEDER_FILE_CHUNK_PROMPT_VERSION, FEEDER_FILE_CHUNK_SYSTEM_V1  # noqa: E402

ENV_PATHS = (WORKER_DIR / ".env", WORKER_DIR / ".env.vps-production")
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
OUT = WORKER_DIR / "scripts" / "out"
DEFAULT_MODEL = "openai/gpt-5.4"
MAX_TOKENS = 20000


def _load_env() -> None:
    for env_path in ENV_PATHS:
        if not env_path.exists():
            continue
        for raw in env_path.read_text().splitlines():
            line = raw.strip()
            if line.startswith("export "):
                line = line[len("export ") :]
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


def _extract_json_object(text: str) -> str:
    body = text.strip()
    if body.startswith("```"):
        body = body.split("\n", 1)[1] if "\n" in body else ""
        if body.rstrip().endswith("```"):
            body = body.rstrip()[:-3]
    start = body.find("{")
    if start < 0:
        raise ValueError("no JSON object in chunk output")
    depth = 0
    in_string = False
    escape = False
    for idx in range(start, len(body)):
        char = body[idx]
        if in_string:
            escape = char == "\\" and not escape
            if char == '"' and not escape:
                in_string = False
        elif char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return body[start : idx + 1]
    raise ValueError("unbalanced JSON object in chunk output")


def _feeder_id(conn: psycopg.Connection, handle: str) -> int:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("select id from public.feeders where lower(handle)=lower(%s) limit 1", (handle,))
        row = cur.fetchone()
    if not row:
        raise RuntimeError(f"unknown feeder handle: {handle}")
    return int(row["id"])


def _overall_ranks(conn: psycopg.Connection, feeder_id: int) -> tuple[dict[str, int], int]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select distinct on (p.post_key) p.post_key, pm.percentile_performance_exact as pct
            from public.posts p
            join public.post_metrics pm on pm.post_key = p.post_key and lower(pm.checkpoint) = 'd7'
            where p.feeder_id = %s
              and lower(coalesce(p.media_type, '')) in ('reel', 'video')
              and p.posted_at >= now() - interval '90 days'
            order by p.post_key, pm.computed_at desc nulls last
            """,
            (feeder_id,),
        )
        rows = [row for row in cur.fetchall() if row["pct"] is not None]
    rows.sort(key=lambda row: float(row["pct"]))
    return {row["post_key"]: idx + 1 for idx, row in enumerate(rows)}, len(rows)


def _read_phrase(cur: int, n_cur: int, overall: int, n_overall: int) -> str:
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
    frac = overall / n_overall if n_overall else 1
    account = (
        "top-tier overall"
        if frac <= 0.1
        else "upper-mid overall"
        if frac <= 1 / 3
        else "mid overall"
        if frac <= 2 / 3
        else "lower-mid overall"
        if frac <= 0.9
        else "bottom-tier overall"
    )
    return f"{batch}, {account}"


def _anomalies(metrics: dict) -> list[str]:
    views_multiple = float(metrics["views_multiple"]) if metrics.get("views_multiple") is not None else None
    likes_multiple = float(metrics["likes_multiple"]) if metrics.get("likes_multiple") is not None else None
    comments_multiple = float(metrics["comments_multiple"]) if metrics.get("comments_multiple") is not None else None
    delta = float(metrics["delta_from_d1"]) if metrics.get("delta_from_d1") is not None else None
    views = metrics.get("views")
    likes = metrics.get("likes")
    out: list[tuple[float, str]] = []
    if views_multiple and views_multiple >= 3:
        out.append((views_multiple, f"reach outlier - {views_multiple:.1f}x usual views"))
    if comments_multiple and comments_multiple >= 2.5:
        out.append((comments_multiple, f"comment spike - {comments_multiple:.1f}x usual comments"))
    if views and likes and likes >= views:
        out.append((99, "more likes than views"))
    elif likes_multiple and likes_multiple >= 3 and (not views_multiple or likes_multiple > views_multiple * 1.3):
        out.append((likes_multiple, f"approval spike - {likes_multiple:.1f}x usual likes"))
    out.sort(key=lambda item: -item[0])
    lines = [line for _, line in out[:2]]
    if delta is not None and delta >= 5:
        lines.append("slow burn - kept climbing past launch")
    elif delta is not None and delta <= -5:
        lines.append("peaked early then faded")
    return lines[:3]


def _payload(conn: psycopg.Connection, *, handle: str, feeder_id: int, size: int) -> tuple[dict, dict[str, str]]:
    overall_ranks, overall_pool = _overall_ranks(conn, feeder_id)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select distinct on (fmc.post_key)
              fmc.post_key,
              fmc.parsed_output as fingerprint,
              p.posted_at,
              p.caption,
              p.duration_seconds
            from public.feeder_file_model_calls fmc
            join public.posts p on p.post_key = fmc.post_key
            where fmc.call_type = 'fingerprint'
              and fmc.status = 'complete'
              and fmc.prompt_version like 'fingerprint_v12%%'
              and p.feeder_id = %s
            order by fmc.post_key, fmc.completed_at desc nulls last
            """,
            (feeder_id,),
        )
        fingerprints = {row["post_key"]: row for row in cur.fetchall()}
        keys = [key for key in fingerprints if key in overall_ranks]
        keys.sort(key=lambda key: fingerprints[key]["posted_at"], reverse=True)
        keys = keys[:size]
        cur.execute(
            """
            select distinct on (post_key)
              post_key, views, likes, comments, views_multiple, likes_multiple,
              comments_multiple, delta_from_d1
            from public.post_metrics
            where post_key = any(%s)
              and lower(checkpoint) = 'd7'
            order by post_key, computed_at desc nulls last
            """,
            (keys,),
        )
        metrics = {row["post_key"]: row for row in cur.fetchall()}

    ranked = sorted(keys, key=lambda key: overall_ranks[key])
    current_ranks = {key: idx + 1 for idx, key in enumerate(ranked)}
    posts = []
    for key in keys:
        alias = f"P{len(posts) + 1:02d}"
        row = fingerprints[key]
        metric = metrics[key]
        performance = {
            "rank_context": {
                "current": f"{current_ranks[key]}/{len(keys)}",
                "overall": f"{overall_ranks[key]}/{overall_pool}",
                "read": _read_phrase(current_ranks[key], len(keys), overall_ranks[key], overall_pool),
            }
        }
        anomalies = _anomalies(metric)
        if anomalies:
            performance["anomalies"] = anomalies
        posts.append(
            {
                "alias": alias,
                "post_key": key,
                "caption": row["caption"],
                "posted_at": row["posted_at"].isoformat(),
                "duration_seconds": row["duration_seconds"],
                "fingerprint": row["fingerprint"],
                "performance": performance,
            }
        )
    if len(posts) != size:
        raise RuntimeError(f"{handle}: only {len(posts)} latest fingerprints available; need {size}")
    dates = sorted(post["posted_at"][:10] for post in posts)
    return (
        {"account": {"handle": handle}, "chunk": {"posts": len(posts), "from": dates[0], "to": dates[-1]}, "posts": posts},
        {post["alias"]: post["post_key"] for post in posts},
    )


def _call(model: str, system: str, payload: dict) -> tuple[str, dict, float]:
    started = datetime.now(timezone.utc)
    resp = requests.post(
        f"{os.environ.get('OPENROUTER_BASE_URL', 'https://openrouter.ai/api/v1').rstrip('/')}/chat/completions",
        headers={
            "Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://feedme.local",
            "X-Title": "FeedMe First Chunk",
        },
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False, indent=2, default=str)},
            ],
            "temperature": 0.1,
            "max_tokens": MAX_TOKENS,
            "usage": {"include": True},
        },
        timeout=600,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"{model} status {resp.status_code}: {resp.text[:800]}")
    data = resp.json()
    content = data["choices"][0]["message"]["content"]
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError(f"{model} returned empty chunk output")
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    return content, data.get("usage") or {}, elapsed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handle", required=True)
    parser.add_argument("--size", type=int, default=10)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--output-suffix", default="first_chunk")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    _load_env()
    OUT.mkdir(parents=True, exist_ok=True)
    slug = args.handle.replace(".", "_")
    payload_path = OUT / f"{slug}_chunk_payload_{args.output_suffix}.json"
    raw_path = OUT / f"{slug}_chunk_{args.output_suffix}.raw.txt"
    chunk_path = OUT / f"{slug}_chunk_{args.output_suffix}.json"

    with psycopg.connect(_pooler_dsn(), row_factory=dict_row, autocommit=True) as conn:
        feeder_id = _feeder_id(conn, args.handle)
        payload, alias_map = _payload(conn, handle=args.handle, feeder_id=feeder_id, size=args.size)
        payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        print(json.dumps({"payload_path": str(payload_path), "posts": len(payload["posts"]), "alias_map": alias_map}, indent=2))
        if args.dry_run:
            return

        system = FEEDER_FILE_CHUNK_SYSTEM_V1.replace("{handle}", args.handle)
        raw, usage, elapsed = _call(args.model, system, payload)
        raw_path.write_text(raw)
        chunk = json.loads(_extract_json_object(raw))
        chunk["source_model"] = args.model
        chunk["source_alias_map"] = alias_map
        chunk["source_prompt_version"] = FEEDER_FILE_CHUNK_PROMPT_VERSION
        chunk_path.write_text(json.dumps(chunk, ensure_ascii=False, indent=2, default=str))
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    insert into public.feeder_file_model_calls
                      (call_key, call_type, feeder_handle, model, prompt_version, system_prompt,
                       user_payload, raw_output, parsed_output, status, started_at, completed_at)
                    values (%s, 'feeder_file_chunk', %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb,
                            'complete', now() - (%s || ' seconds')::interval, now())
                    returning id
                    """,
                    (
                        f"feeder_file_chunk:{args.handle}:{args.output_suffix}:{args.model}",
                        args.handle,
                        args.model,
                        FEEDER_FILE_CHUNK_PROMPT_VERSION,
                        system,
                        json.dumps(payload, ensure_ascii=False, default=str),
                        raw,
                        json.dumps(chunk, ensure_ascii=False, default=str),
                        str(round(elapsed, 1)),
                    ),
                )
                call_id = cur.fetchone()["id"]
            except Exception as exc:
                call_id = None
                print(f"warn: skipped model-call persistence: {exc}")
        print(
            json.dumps(
                {
                    "chunk_path": str(chunk_path),
                    "raw_path": str(raw_path),
                    "model_call_id": call_id,
                    "elapsed_seconds": round(elapsed, 1),
                    "usage": usage,
                    "bites": [bite.get("name") for bite in chunk.get("bites", [])],
                },
                ensure_ascii=False,
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
