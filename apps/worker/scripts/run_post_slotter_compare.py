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
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

WORKER_DIR = ROOT / "apps" / "worker"
ENV_PATH = WORKER_DIR / ".env"
PROMPT_PATH = Path("/Users/sky/.codex/attachments/3a317e5c-02cf-44f1-a226-e11bbc5e7358/pasted-text.txt")
OUT_DIR = WORKER_DIR / "scripts" / "out"
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"

DEFAULT_MODELS = ["openai/gpt-5.4", "deepseek/deepseek-v4-pro"]
DEFAULT_TRIGGER_POSTS = {
    "anuj.mp4": "p/dyjcvjqmpbo#f27",
    "lakmeindia": "p/dyzptg0mok8#f35",
}
FEEDER_FILES = {
    "anuj.mp4": OUT_DIR / "anuj_mp4_feeder_file_cold_start_v8_1_gpt54.json",
    "lakmeindia": OUT_DIR / "lakmeindia_feeder_file_cold_start_v8_1_gpt54.json",
}
MAX_TOKENS = 20000


def _load_env() -> None:
    if ENV_PATH.exists():
        for raw in ENV_PATH.read_text().splitlines():
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


def _model_slug(model: str) -> str:
    return model.replace("/", "_").replace(".", "").replace("-", "_")


def _handle_slug(handle: str) -> str:
    return handle.replace(".", "_")


def _compact_bites(feed_file: dict[str, Any]) -> list[dict[str, Any]]:
    bites = []
    for bite in feed_file.get("bites") or []:
        bites.append({
            "name": bite.get("name"),
            "tier": bite.get("tier"),
            "kind": bite.get("kind"),
            "contract": bite.get("contract") or {},
        })
    return bites


def _fetch_trigger(conn: Any, *, handle: str, post_key: str) -> dict[str, Any]:
    row = conn.execute(
        """
        select
          p.post_key,
          p.posted_at,
          p.caption,
          p.duration_seconds,
          f.handle,
          fmc.parsed_output as fingerprint
        from public.posts p
        join public.feeders f on f.id = p.feeder_id
        join lateral (
          select parsed_output
          from public.feeder_file_model_calls
          where call_type = 'fingerprint'
            and status = 'complete'
            and parsed_output is not null
            and prompt_version like 'fingerprint_v12%%'
            and post_key = p.post_key
          order by completed_at desc
          limit 1
        ) fmc on true
        where lower(f.handle) = lower(%s)
          and p.post_key = %s
        limit 1
        """,
        (handle, post_key),
    ).fetchone()
    if not row:
        raise RuntimeError(f"no cached v12 fingerprint found for {handle} trigger {post_key}")
    return dict(row)


def _build_payload(*, feed_file: dict[str, Any], trigger: dict[str, Any]) -> dict[str, Any]:
    handle = str(trigger["handle"])
    return {
        "account": {"handle": handle},
        "post": {
            "post_key": trigger["post_key"],
            "posted_at": trigger["posted_at"].isoformat() if hasattr(trigger["posted_at"], "isoformat") else trigger["posted_at"],
            "duration_seconds": trigger.get("duration_seconds"),
            "caption": trigger.get("caption"),
            "fingerprint": trigger["fingerprint"],
        },
        "feeder_bites": _compact_bites(feed_file),
    }


def _call_model(*, system_prompt: str, payload: dict[str, Any], model: str) -> tuple[str, dict[str, Any], dict[str, Any]]:
    resp = requests.post(
        f"{os.environ.get('OPENROUTER_BASE_URL', 'https://openrouter.ai/api/v1').rstrip('/')}/chat/completions",
        headers={
            "Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://feedme.local",
            "X-Title": "FeedMe Post Slotter Compare",
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
        timeout=600,
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


def _validate_slotter_output(parsed: dict[str, Any], feeder_bites: list[dict[str, Any]]) -> list[str]:
    problems: list[str] = []
    expected = {str(b["name"]) for b in feeder_bites}
    matched = [b.get("bite") for b in parsed.get("matched_bites") or []]
    clipped = [b.get("bite") for b in parsed.get("clipped_bites") or []]
    unmatched = parsed.get("unmatched") or []
    all_names = [str(x) for x in matched + clipped + unmatched]
    if set(all_names) != expected:
        problems.append(f"bite coverage mismatch: missing={sorted(expected - set(all_names))} extra={sorted(set(all_names) - expected)}")
    duplicates = sorted({name for name in all_names if all_names.count(name) > 1})
    if duplicates:
        problems.append(f"duplicate bite names: {duplicates}")
    if parsed.get("slotter_version") != "post_slotter_v1":
        problems.append(f"bad slotter_version: {parsed.get('slotter_version')!r}")
    if len(parsed.get("new_candidate_bites") or []) > 2:
        problems.append("too many new_candidate_bites")
    return problems


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handle", action="append", choices=sorted(FEEDER_FILES), help="Handle to run; repeatable")
    parser.add_argument("--model", action="append", dest="models", help="OpenRouter model; repeatable")
    parser.add_argument("--trigger-post-key", action="append", dest="trigger_pairs", help="Override as handle=post_key; repeatable")
    parser.add_argument("--prompt-path", default=str(PROMPT_PATH))
    args = parser.parse_args()

    _load_env()
    if not os.environ.get("OPENROUTER_API_KEY"):
        raise RuntimeError("OPENROUTER_API_KEY is not set")
    if not os.environ.get("POSTGRES_DSN"):
        raise RuntimeError("POSTGRES_DSN is not set")

    import psycopg
    from psycopg.rows import dict_row

    handles = args.handle or sorted(FEEDER_FILES)
    models = args.models or DEFAULT_MODELS
    trigger_posts = dict(DEFAULT_TRIGGER_POSTS)
    for pair in args.trigger_pairs or []:
        if "=" not in pair:
            raise RuntimeError("--trigger-post-key must be handle=post_key")
        handle, post_key = pair.split("=", 1)
        trigger_posts[handle.strip()] = post_key.strip()

    system_prompt = Path(args.prompt_path).read_text()
    summary: list[dict[str, Any]] = []

    with psycopg.connect(os.environ["POSTGRES_DSN"], row_factory=dict_row, autocommit=True, connect_timeout=20) as conn:
        for handle in handles:
            feed_file_path = FEEDER_FILES[handle]
            feed_file = json.loads(feed_file_path.read_text())
            feeder_bites = _compact_bites(feed_file)
            trigger = _fetch_trigger(conn, handle=handle, post_key=trigger_posts[handle])
            payload = _build_payload(feed_file=feed_file, trigger=trigger)
            payload_path = OUT_DIR / f"{_handle_slug(handle)}_post_slotter_v1_payload_gpt_feeder_file.json"
            payload_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str))

            print(json.dumps({
                "status": "payload_ready",
                "handle": handle,
                "trigger_post_key": trigger["post_key"],
                "bites": len(feeder_bites),
                "payload": str(payload_path),
            }, default=str), flush=True)

            for model in models:
                started = datetime.now(timezone.utc)
                print(json.dumps({"status": "calling", "handle": handle, "model": model}), flush=True)
                raw, usage, response_meta = _call_model(system_prompt=system_prompt, payload=payload, model=model)
                elapsed = (datetime.now(timezone.utc) - started).total_seconds()
                suffix = f"{_handle_slug(handle)}_post_slotter_v1_gpt_feeder_file_{_model_slug(model)}"
                raw_path = OUT_DIR / f"{suffix}.raw.txt"
                json_path = OUT_DIR / f"{suffix}.json"
                raw_path.write_text(raw)
                parsed: dict[str, Any] | None = None
                problems: list[str]
                try:
                    parsed = json.loads(_extract_json_object(raw))
                    parsed["_slotter_run_meta"] = {
                        "model": model,
                        "feeder_file_source": str(feed_file_path),
                        "trigger_post_key": trigger["post_key"],
                        "usage": usage,
                        "response_meta": response_meta,
                        "elapsed_seconds": round(elapsed, 2),
                    }
                    problems = _validate_slotter_output(parsed, feeder_bites)
                    json_path.write_text(json.dumps(parsed, indent=2, ensure_ascii=False, default=str))
                except Exception as exc:
                    problems = [f"parse failed: {exc}"]

                row = {
                    "status": "complete" if parsed and not problems else "review",
                    "handle": handle,
                    "model": model,
                    "trigger_post_key": trigger["post_key"],
                    "raw": str(raw_path),
                    "json": str(json_path) if parsed else None,
                    "usage_total_tokens": usage.get("total_tokens"),
                    "response_meta": response_meta,
                    "elapsed_seconds": round(elapsed, 2),
                    "problems": problems,
                }
                summary.append(row)
                print(json.dumps(row, ensure_ascii=False, default=str), flush=True)

    summary_path = OUT_DIR / "post_slotter_v1_compare_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False, default=str))
    print(json.dumps({"status": "summary_written", "path": str(summary_path)}), flush=True)


if __name__ == "__main__":
    main()
