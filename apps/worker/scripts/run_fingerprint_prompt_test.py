"""Run the compact reel fingerprint prompt on selected full-video reels."""
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
OUT_DIR = WORKER_DIR / "scripts" / "out" / "fingerprint_prompt_test_v2"
ENV_PATHS = (WORKER_DIR / ".env", WORKER_DIR / ".env.vps-production")
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
DEFAULT_HANDLES = ("traya.health", "srishtigargg", "anuj.mp4", "lakmeindia")
DEFAULT_MODEL = "google/gemini-3.5-flash"
PROMPT_VERSION = "reel_observation_fingerprint_compact_v2"

if str(WORKER_DIR) not in sys.path:
    sys.path.insert(0, str(WORKER_DIR))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402


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


def _system_prompt() -> str:
    return """
REEL OBSERVATION FINGERPRINT

You will receive one Instagram Reel with its caption and available media.

Your job is to create a compact observation fingerprint.

This is not analysis.

Do not interpret strategy.
Do not infer audience psychology.
Do not decide what the reel is trying to do.
Do not decide the bite.
Do not classify the reel into a format, engine, theme, pattern or content category.

Your only job is to preserve the reel so another model feels like it watched it once.

Return valid JSON only.

OUTPUT

{
  "post_key": "",
  "media_type": "reel",
  "duration_seconds": null,
  "media_truncated": false,
  "observed_window": "",
  "caption_core": "",
  "observed_note": "",
  "kept_lines": [],
  "kept_visuals": [],
  "references": [],
  "uncertainties": [],
  "media_confidence": "high"
}

FIELD RULES

duration_seconds

Copy supplied duration exactly when available.

If unavailable:

* leave null
* mention only if it affects understanding

Never estimate duration.

media_truncated

true only if supplied media exceeded 120 seconds and only the sampled section was observed.

Otherwise false.

observed_window

If truncated:

0:00-2:00

Otherwise use supplied observation span if known.

caption_core

If caption is short:

Copy it exactly.

If caption contains:

* SEO keyword blocks
* repeated hashtags
* product descriptions
* repeated CTA
* long event logistics

compress it.

Keep only:

* opening framing
* mentions
* campaign/event names
* important offer
* unusual wording

Cap:

70 words.

Do not interpret.

observed_note

The main observation.

This should read like someone describing the reel to another person who has never seen it.

Describe:

* who appears
* what happens
* important dialogue
* important actions
* what changes
* how the reel finishes

Preserve:

* timing
* contrast
* delivery
* reveal
* visual moments

Do NOT explain why they matter.

Do NOT infer meaning.

Target:

80-140 words.

Dense reels:

180 words maximum.

kept_lines

Only lines another model would regret losing.

Usually:

2-6.

Keep:

* punchlines
* repeated phrases
* memorable wording
* campaign claims
* product claims
* unusual dialogue

Not every subtitle.

kept_visuals

Only visuals that would change later understanding.

Examples:

* fake moustache disguise
* board reading "ITALY EXPANSION PROPOSAL"
* Stage-3 hair report
* nail rubbing demonstration
* AI-generated scalp visual
* payment terminal
* fake luxury bathrobe

Think:

"If this disappeared, another model would misunderstand the reel."

Usually:

3-6 items.

references

Recognizable outside references.

Examples:

* Balayam
* International Yoga Day
* Modi-Meloni meme
* World Cup
* Shark Tank
* IPL
* GTA

Empty array is valid.

uncertainties

Only uncertainty that affects understanding.

Ignore minor uncertainty.

WHAT TO PRESERVE

Preserve:

* memorable dialogue
* memorable visuals
* reveal
* delivery
* ending
* body language when it changes the scene
* objects/screens that carry the reel
* cultural references

Think:

"What would a human actually remember after watching this once?"

WHAT TO DROP

Drop:

* timestamps
* camera angles
* shot inventory
* generic environments
* repeated subtitle fragments
* repeated visible text
* generic editing notes
* full transcript
* generic hashtags
* SEO keyword blocks

STRICT BAN

Never write:

* job
* aim
* driver
* purpose
* bite
* hook
* strategy
* insight
* pattern
* engine
* theme
* payoff
* audience
* account
* works because
* this shows
* this suggests

Do not explain the joke.

Do not explain why something is funny.

Do not explain why something is persuasive.

Only preserve what happened.

GOLDEN RULE

Imagine another model will never watch this reel.

Give it just enough information that it feels like it did.

Do not make it think for the next model.

Do not remove the moments that make the reel itself unique.

Compress information.

Never compress understanding.

LENGTH

Average reel:

120-180 words.

Very dense reel:

220 words maximum.

The fingerprint should feel like remembered observation, not forensic documentation.

TEST OVERRIDES

If DURATION_SECONDS is above 120, you are observing only the sampled first 120 seconds: set media_truncated true and observed_window "0:00-2:00".
""".strip()


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


def _artifact_paths(handle_out: Path, index: int, post_key: str) -> dict[str, Path]:
    safe_key = post_key.replace("/", "_").replace("#", "_")
    base = handle_out / f"{index:02d}_{safe_key}"
    return {
        "payload": Path(f"{base}.payload.json"),
        "raw": Path(f"{base}.raw.txt"),
        "json": Path(f"{base}.json"),
        "meta": Path(f"{base}.meta.json"),
        "failure": Path(f"{base}.failure.json"),
    }


def _target_rows(conn: Any, handle: str, limit: int) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            with latest_d7 as (
              select distinct on (post_key) post_key, computed_at
              from public.post_metrics
              where lower(checkpoint) = 'd7'
              order by post_key, computed_at desc nulls last
            ),
            video_full as (
              select distinct post_key
              from public.post_media_assets
              where asset_role = 'video_full'
                and status in ('active', 'purge_pending')
                and coalesce(storage_path, public_url, '') <> ''
            )
            select p.post_key
            from public.posts p
            join public.feeders f on f.id = p.feeder_id
            join latest_d7 d7 on d7.post_key = p.post_key
            join video_full vf on vf.post_key = p.post_key
            where lower(f.handle) = lower(%s)
              and lower(coalesce(p.media_type, '')) in ('reel', 'video')
            order by p.posted_at desc nulls last
            limit %s
            """,
            (handle, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def _response_meta(data: dict[str, Any]) -> dict[str, Any]:
    choice = (data.get("choices") or [{}])[0] if isinstance(data.get("choices"), list) else {}
    return {
        "id": data.get("id"),
        "model": data.get("model"),
        "created": data.get("created"),
        "finish_reason": choice.get("finish_reason") if isinstance(choice, dict) else None,
        "usage": data.get("usage"),
    }


def _call_model(system: str, user_text: str, media_parts: list[dict[str, Any]], model: str) -> tuple[str, dict[str, Any] | None, dict[str, Any], str | None]:
    resp = requests.post(
        f"{os.environ.get('OPENROUTER_BASE_URL', 'https://openrouter.ai/api/v1').rstrip('/')}/chat/completions",
        headers={
            "Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://feedme.local",
            "X-Title": "FeedMe Fingerprint Prompt Test",
        },
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": [*media_parts, {"type": "text", "text": user_text}]},
            ],
            "temperature": 0.1,
            "max_tokens": int(os.getenv("FINGERPRINT_TEST_MAX_TOKENS", "8000")),
            "response_format": {"type": "json_object"},
        },
        timeout=240,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"OpenRouter status {resp.status_code}: {resp.text[:1200]}")
    data = resp.json()
    raw = data.get("choices", [{}])[0].get("message", {}).get("content", "")
    meta = _response_meta(data)
    try:
        return raw, json.loads(_extract_json_object(raw)), meta, None
    except Exception as exc:
        return raw, None, meta, f"{type(exc).__name__}: {str(exc)[:300]}"


def _normalize(parsed: dict[str, Any], post: dict[str, Any], post_key: str) -> dict[str, Any]:
    duration = post.get("duration_seconds")
    try:
        truncated = duration is not None and float(duration) > 120
    except Exception:
        truncated = False

    parsed["post_key"] = post_key
    parsed["media_type"] = "reel"
    parsed["duration_seconds"] = parsed.get("duration_seconds", duration)
    parsed["media_truncated"] = truncated
    parsed["observed_window"] = "0:00-2:00" if truncated else str(parsed.get("observed_window") or "")

    if not parsed.get("caption_core"):
        parsed["caption_core"] = str(post.get("caption") or "")
    parsed.pop("caption", None)
    parsed.pop("caption_was_compressed", None)
    return parsed


def run(limit: int, model: str, handles: tuple[str, ...], no_call: bool, force: bool) -> None:
    from app.fingerprint_intelligence import _fingerprint_media_parts, _post_media, _provider

    provider = _provider()
    if provider != "openrouter":
        raise RuntimeError(f"expected openrouter provider, got {provider!r}")
    system = _system_prompt()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {"prompt_version": PROMPT_VERSION, "model": model, "handles": {}}
    with psycopg.connect(os.environ["POSTGRES_DSN"], row_factory=dict_row, connect_timeout=20, autocommit=True) as conn:
        for handle in handles:
            rows = _target_rows(conn, handle, limit)
            handle_slug = handle.replace(".", "_")
            handle_out = OUT_DIR / handle_slug
            handle_out.mkdir(parents=True, exist_ok=True)
            results = []
            for index, row in enumerate(rows, start=1):
                post_key = str(row["post_key"])
                post = _post_media(conn, post_key)
                if not post:
                    results.append({"post_key": post_key, "status": "missing_post"})
                    continue
                user_text = "\n".join([
                    f"POST_KEY: {post_key}",
                    "MEDIA_TYPE: reel",
                    "VIDEO_SAMPLE_SECONDS: 120",
                    f"DURATION_SECONDS: {post.get('duration_seconds') or ''}",
                    f"DURATION_BUCKET: {post.get('duration_bucket') or ''}",
                    f"POSTED_AT: {post.get('posted_at') or ''}",
                    f"CAPTION: {str(post.get('caption') or '')}",
                ])
                paths = _artifact_paths(handle_out, index, post_key)
                payload_path = paths["payload"]
                json_path = paths["json"]
                raw_path = paths["raw"]
                meta_path = paths["meta"]
                failure_path = paths["failure"]
                payload_path.write_text(json.dumps({"system_prompt": system, "user_text": user_text}, ensure_ascii=False, indent=2, default=str))
                if no_call:
                    results.append({"post_key": post_key, "status": "payload_only", "payload": str(payload_path)})
                    continue
                if not force and json_path.exists():
                    results.append({"post_key": post_key, "status": "skipped_complete", "json": str(json_path)})
                    print(json.dumps({"handle": handle, **results[-1]}, ensure_ascii=False, default=str), flush=True)
                    continue
                if not force and failure_path.exists():
                    prior_failure = json.loads(failure_path.read_text())
                    if prior_failure.get("failure_kind") != "model_failed":
                        results.append({
                            "post_key": post_key,
                            "status": "skipped_non_model_failure",
                            "failure": str(failure_path),
                            "failure_kind": prior_failure.get("failure_kind"),
                        })
                        print(json.dumps({"handle": handle, **results[-1]}, ensure_ascii=False, default=str), flush=True)
                        continue
                try:
                    media_parts, media_hash, confidence = _fingerprint_media_parts(post, provider)
                    if not media_parts:
                        failure = {
                            "post_key": post_key,
                            "status": "failed",
                            "failure_kind": "media_failed",
                            "error": "video_full fetch/trim failed",
                            "payload": str(payload_path),
                        }
                        failure_path.write_text(json.dumps(failure, ensure_ascii=False, indent=2, default=str))
                        results.append(failure)
                        print(json.dumps({"handle": handle, **results[-1]}, ensure_ascii=False, default=str), flush=True)
                        continue
                    raw, parsed, meta, parse_error = _call_model(system, user_text, media_parts, model)
                    raw_path.write_text(raw or "")
                    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=str))
                    if parsed is None:
                        failure_kind = "token_cap" if meta.get("finish_reason") == "length" else "parser_failed"
                        failure = {
                            "post_key": post_key,
                            "status": "failed",
                            "failure_kind": failure_kind,
                            "error": parse_error or "model output did not parse",
                            "finish_reason": meta.get("finish_reason"),
                            "payload": str(payload_path),
                            "raw": str(raw_path),
                            "meta": str(meta_path),
                        }
                        failure_path.write_text(json.dumps(failure, ensure_ascii=False, indent=2, default=str))
                        results.append(failure)
                        print(json.dumps({"handle": handle, **results[-1]}, ensure_ascii=False, default=str), flush=True)
                        continue
                    parsed = _normalize(parsed, post, post_key)
                    json_path.write_text(json.dumps(parsed, ensure_ascii=False, indent=2, default=str))
                    if failure_path.exists():
                        failure_path.unlink()
                    results.append({
                        "post_key": post_key,
                        "status": "complete",
                        "json": str(json_path),
                        "media_hash": media_hash,
                        "media_confidence": confidence,
                        "media_truncated": parsed.get("media_truncated"),
                        "caption_was_compressed": parsed.get("caption_was_compressed"),
                    })
                except Exception as exc:
                    failure = {
                        "post_key": post_key,
                        "status": "failed",
                        "failure_kind": "model_failed",
                        "error": f"{type(exc).__name__}: {str(exc)[:300]}",
                        "payload": str(payload_path),
                    }
                    failure_path.write_text(json.dumps(failure, ensure_ascii=False, indent=2, default=str))
                    results.append(failure)
                print(json.dumps({"handle": handle, **results[-1]}, ensure_ascii=False, default=str), flush=True)
            summary["handles"][handle] = {"selected": len(rows), "results": results}
            (OUT_DIR / f"{handle_slug}_summary.json").write_text(json.dumps(summary["handles"][handle], ensure_ascii=False, indent=2, default=str))
    (OUT_DIR / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


def _self_test() -> None:
    assert json.loads(_extract_json_object('```json\n{"ok": true}\n```')) == {"ok": True}
    prompt = _system_prompt()
    assert "caption_core" in prompt
    assert "caption_was_compressed" not in prompt


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--handle", action="append", choices=DEFAULT_HANDLES)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--no-call", action="store_true")
    parser.add_argument("--force", action="store_true", help="rerun complete and non-model failed rows too")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    _load_env()
    if args.self_test:
        _self_test()
        print("self-test ok")
        return
    run(max(1, args.limit), args.model, tuple(args.handle or DEFAULT_HANDLES), args.no_call, args.force)


if __name__ == "__main__":
    main()
