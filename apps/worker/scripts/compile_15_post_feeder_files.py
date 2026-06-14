"""Compile placeholder 15-post feeder files for Anuj and Lakme.

Uses the newest 15 D7-qualified reel/video posts with latest high-confidence
fingerprints, ordered newest-first in the payload. Runs one GPT 5.4 compile per
feeder and writes raw + parsed outputs to scripts/out without activating them.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[3]
WORKER_DIR = ROOT / "apps" / "worker"
if str(WORKER_DIR) not in sys.path:
    sys.path.insert(0, str(WORKER_DIR))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

from app.feeder_prompts import (  # noqa: E402
    FEEDER_FILE_COLD_START_PROMPT_VERSION,
    FEEDER_FILE_COLD_START_SYSTEM_V8_1,
)


ENV_PATHS = (
    WORKER_DIR / ".env",
    WORKER_DIR / ".env.vps-production",
)
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
HANDLES = ("anuj.mp4", "lakmeindia")
DEFAULT_MODEL = "openai/gpt-5.4"
WINDOW_SIZE = 15
MAX_TOKENS = 26000
COMPILE_VERSION = f"{FEEDER_FILE_COLD_START_PROMPT_VERSION}_15post_placeholder"


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


def _strip_fences(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = t.split("\n", 1)[1] if "\n" in t else ""
        if t.rstrip().endswith("```"):
            t = t.rstrip()[:-3]
    return t.strip()


def _extract_json_object(text: str) -> str:
    t = _strip_fences(text)
    start = t.find("{")
    if start == -1:
        raise ValueError("no JSON object found in model output")
    depth = 0
    in_str = False
    esc = False
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


def _anomalies(m: dict[str, Any]) -> list[str]:
    vm = float(m["views_multiple"]) if m.get("views_multiple") is not None else None
    lm = float(m["likes_multiple"]) if m.get("likes_multiple") is not None else None
    cm = float(m["comments_multiple"]) if m.get("comments_multiple") is not None else None
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


def _build_payload(conn: Any, *, handle: str, window_size: int) -> tuple[dict[str, Any], dict[str, str], int]:
    from app.fingerprint_intelligence import current_model_version

    fingerprint_model_version = current_model_version()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            with latest_d7 as (
              select distinct on (post_key)
                post_key,
                views,
                likes,
                comments,
                views_multiple,
                likes_multiple,
                comments_multiple,
                delta_from_d1,
                percentile_performance,
                percentile_performance_exact,
                computed_at
              from public.post_metrics
              where lower(checkpoint) = 'd7'
              order by post_key, computed_at desc nulls last
            ),
            eligible as (
              select
                p.post_key,
                p.posted_at,
                p.caption,
                p.duration_seconds,
                f.id as feeder_id,
                f.handle,
                d7.views,
                d7.likes,
                d7.comments,
                d7.views_multiple,
                d7.likes_multiple,
                d7.comments_multiple,
                d7.delta_from_d1,
                d7.percentile_performance,
                d7.percentile_performance_exact,
                pf.fingerprint
              from public.posts p
              join public.feeders f on f.id = p.feeder_id
              join latest_d7 d7 on d7.post_key = p.post_key
              join public.post_fingerprints pf on pf.post_key = p.post_key
              where lower(f.handle) = lower(%s)
                and lower(coalesce(p.media_type, '')) in ('reel', 'video')
                and pf.model_version = %s
                and pf.media_confidence = 'high'
            )
            select *
            from eligible
            order by posted_at desc nulls last
            limit %s
            """,
            (handle, fingerprint_model_version, window_size),
        )
        rows = [dict(row) for row in cur.fetchall()]

        cur.execute(
            """
            with latest_d7 as (
              select distinct on (pm.post_key)
                pm.post_key,
                pm.views,
                pm.computed_at
              from public.post_metrics pm
              where lower(pm.checkpoint) = 'd7'
              order by pm.post_key, pm.computed_at desc nulls last
            )
            select p.post_key
            from public.posts p
            join public.feeders f on f.id = p.feeder_id
            join latest_d7 d7 on d7.post_key = p.post_key
            where lower(f.handle) = lower(%s)
              and lower(coalesce(p.media_type, '')) in ('reel', 'video')
            """,
            (handle,),
        )
        total_d7_pool = len(cur.fetchall())

    if len(rows) < window_size:
        raise RuntimeError(f"{handle}: only {len(rows)} eligible latest-prompt D7 fingerprints; need {window_size}")

    def overall_pos(row: dict[str, Any]) -> float:
        raw = row.get("percentile_performance_exact")
        if raw is None:
            raw = row.get("percentile_performance")
        return float(raw) if raw is not None else 101.0

    ranked = sorted(rows, key=overall_pos)
    overall_rank = {row["post_key"]: f"{idx}/{total_d7_pool}" for idx, row in enumerate(ranked, start=1)}
    current_rank = {row["post_key"]: idx for idx, row in enumerate(ranked, start=1)}

    alias_map: dict[str, str] = {}
    posts: list[dict[str, Any]] = []
    n_cur = len(rows)
    for idx, row in enumerate(rows, start=1):
        alias = f"P{idx:02d}"
        post_key = row["post_key"]
        alias_map[alias] = post_key
        ov_pos, ov_n = (int(x) for x in overall_rank[post_key].split("/"))
        cur_pos = current_rank[post_key]
        performance: dict[str, Any] = {
            "rank_context": {
                "current": f"{cur_pos}/{n_cur}",
                "overall": overall_rank[post_key],
                "read": _read_phrase(cur_pos, n_cur, ov_pos, ov_n),
            }
        }
        anomalies = _anomalies(row)
        if anomalies:
            performance["anomalies"] = anomalies
        posts.append({
            "alias": alias,
            "caption": row.get("caption"),
            "posted_at": row["posted_at"].isoformat() if hasattr(row["posted_at"], "isoformat") else row["posted_at"],
            "duration_seconds": row.get("duration_seconds"),
            "fingerprint": row["fingerprint"],
            "performance": performance,
        })

    dates = sorted(str(post["posted_at"])[:10] for post in posts)
    payload = {
        "account": {"handle": handle},
        "window": {
            "posts": len(posts),
            "from": dates[0],
            "to": dates[-1],
            "source": "newest D7-qualified latest-prompt fingerprints; ranks frozen at D7 from production DB",
        },
        "posts": posts,
    }
    return payload, alias_map, int(rows[0]["feeder_id"])


def _call_model(system_prompt: str, payload: dict[str, Any], model: str) -> tuple[str, dict[str, Any], dict[str, Any]]:
    resp = requests.post(
        f"{os.environ.get('OPENROUTER_BASE_URL', 'https://openrouter.ai/api/v1').rstrip('/')}/chat/completions",
        headers={
            "Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://feedme.local",
            "X-Title": "FeedMe 15 Post Feeder File Compile",
        },
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False, indent=2, default=str)},
            ],
            "temperature": 0.1,
            "max_tokens": MAX_TOKENS,
            "usage": {"include": True},
        },
        timeout=900,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"OpenRouter status {resp.status_code}: {resp.text[:1200]}")
    data = resp.json()
    choice = data["choices"][0]
    return choice["message"]["content"], data.get("usage") or {}, {
        "id": data.get("id"),
        "finish_reason": choice.get("finish_reason"),
        "native_finish_reason": choice.get("native_finish_reason"),
    }


def _validate(file: dict[str, Any], alias_map: dict[str, str]) -> list[str]:
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
        tally = {"core": 0, "supporting": 0, "standby": 0}
        for receipt in bite.get("receipts") or []:
            weight = receipt.get("weight")
            if weight in tally:
                tally[weight] += 1
            else:
                problems.append(f"{bname}: bad weight {weight!r}")
            for axis in receipt.get("axis_bites") or []:
                if axis not in bite_names:
                    problems.append(f"{bname}: axis_bites references unknown bite {axis!r}")
        declared = bite.get("weights_tally") or {}
        for key, value in tally.items():
            if declared.get(key) is not None and declared[key] < value:
                problems.append(f"{bname}: weights_tally.{key}={declared.get(key)} < receipts {value}")
    if kinds["earned"] > 8 or kinds["candidate"] > 3 or kinds["grammar"] > 2:
        problems.append(f"caps exceeded: {kinds}")
    return problems


def _persist_call(
    conn: Any,
    *,
    handle: str,
    model: str,
    system_prompt: str,
    payload: dict[str, Any],
    alias_map: dict[str, str],
    raw: str,
    parsed: dict[str, Any] | None,
    status: str,
    error: str | None,
    started: datetime,
) -> int:
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
                f"feeder_file_compile:{handle}:{COMPILE_VERSION}:{model}",
                handle,
                model,
                COMPILE_VERSION,
                system_prompt,
                json.dumps({"feeder_file_payload": payload, "alias_map": alias_map}, ensure_ascii=False, default=str),
                raw,
                json.dumps(parsed, ensure_ascii=False, default=str) if parsed is not None else None,
                status,
                error,
                started,
            ),
        )
        return int(cur.fetchone()[0])


def _model_slug(model: str) -> str:
    return model.replace("/", "_").replace(".", "").replace("-", "_")


def _compile_one(conn: Any, *, handle: str, model: str) -> dict[str, Any]:
    payload, alias_map, feeder_id = _build_payload(conn, handle=handle, window_size=WINDOW_SIZE)
    system_prompt = FEEDER_FILE_COLD_START_SYSTEM_V8_1.replace("{handle}", handle)
    slug = handle.replace(".", "_")
    out_prefix = WORKER_DIR / "scripts" / "out" / f"{slug}_feeder_file_15post_{_model_slug(model)}"
    (out_prefix.with_suffix(".payload.json")).write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    (out_prefix.with_suffix(".system_prompt.txt")).write_text(system_prompt)

    print(json.dumps({
        "status": "calling",
        "handle": handle,
        "posts": len(payload["posts"]),
        "from": payload["window"]["from"],
        "to": payload["window"]["to"],
        "model": model,
    }), flush=True)

    started = datetime.now(timezone.utc)
    raw, usage, response_meta = _call_model(system_prompt, payload, model)
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    raw_path = out_prefix.with_suffix(".raw.txt")
    json_path = out_prefix.with_suffix(".json")
    raw_path.write_text(raw)

    parsed: dict[str, Any] | None = None
    problems: list[str]
    try:
        parsed = json.loads(_extract_json_object(raw))
        problems = _validate(parsed, alias_map)
        parsed["source_model"] = model
        parsed["source_alias_map"] = alias_map
        parsed["source_prompt_version"] = COMPILE_VERSION
        parsed["source_window_size"] = WINDOW_SIZE
        parsed["compile_usage"] = usage
        parsed["compile_response_meta"] = response_meta
        parsed["compiled_elapsed_seconds"] = round(elapsed, 2)
        json_path.write_text(json.dumps(parsed, indent=2, ensure_ascii=False, default=str))
        call_id = _persist_call(
            conn,
            handle=handle,
            model=model,
            system_prompt=system_prompt,
            payload=payload,
            alias_map=alias_map,
            raw=raw,
            parsed=parsed,
            status="complete" if not problems else "review",
            error="; ".join(problems)[:1000] if problems else None,
            started=started,
        )
    except Exception as exc:
        problems = [f"parse failed: {exc}"]
        call_id = _persist_call(
            conn,
            handle=handle,
            model=model,
            system_prompt=system_prompt,
            payload=payload,
            alias_map=alias_map,
            raw=raw,
            parsed=None,
            status="error",
            error=problems[0],
            started=started,
        )

    # Store as a draft placeholder under a distinct compile_version. It does not
    # supersede production/active files.
    row_id = None
    if parsed is not None:
        with conn.cursor() as cur:
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
                    json.dumps(parsed, ensure_ascii=False, default=str),
                    COMPILE_VERSION,
                    f"compile_15_post_feeder_files.py handle={handle} model={model} (model call {call_id})",
                ),
            )
            row_id = cur.fetchone()[0]

    return {
        "handle": handle,
        "status": "complete" if parsed is not None and not problems else "review",
        "posts": len(payload["posts"]),
        "raw": str(raw_path),
        "json": str(json_path) if parsed is not None else None,
        "payload": str(out_prefix.with_suffix(".payload.json")),
        "system_prompt": str(out_prefix.with_suffix(".system_prompt.txt")),
        "model_call_id": call_id,
        "feeder_file_row_id": row_id,
        "usage_total_tokens": usage.get("total_tokens"),
        "response_meta": response_meta,
        "elapsed_seconds": round(elapsed, 2),
        "problems": problems,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handle", action="append", choices=HANDLES, help="Handle to compile; repeatable")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="OpenRouter model slug")
    args = parser.parse_args()
    _load_env()

    handles = args.handle or list(HANDLES)
    with psycopg.connect(os.environ["POSTGRES_DSN"], autocommit=True, connect_timeout=20) as conn:
        results = [_compile_one(conn, handle=handle, model=args.model) for handle in handles]

    summary_path = WORKER_DIR / "scripts" / "out" / f"feeder_file_15post_{_model_slug(args.model)}_summary.json"
    summary_path.write_text(json.dumps(results, indent=2, ensure_ascii=False, default=str))
    print(json.dumps({"status": "summary_written", "path": str(summary_path), "results": results}, ensure_ascii=False, default=str), flush=True)


if __name__ == "__main__":
    main()
