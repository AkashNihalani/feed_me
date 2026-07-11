from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[3]
WORKER_DIR = ROOT / "apps" / "worker"
ENV_PATHS = (WORKER_DIR / ".env", WORKER_DIR / ".env.vps-production")
DEFAULT_PROMPT = WORKER_DIR / "scripts" / "postcard_writer_prompt.md"
DEFAULT_INPUT = WORKER_DIR / "scripts" / "out" / "media_logger_v4_flash_retry" / "media_logger_v4_flash_retry_selected_15_combined.json"
DEFAULT_OUT_DIR = WORKER_DIR / "scripts" / "out" / "postcards_v4_gpt54_mini"
DEFAULT_MODEL = "openai/gpt-5.4-mini"


WORD_CAPS = {"S": 180, "M": 350, "L": 380, "XL": 500}
TOKEN_CAPS = {"S": 450, "M": 650, "L": 850, "XL": 1100}


TIER_LIMITS = {
    "S": """HARD WORD CAPS

Tier S: absolute max 180 words

If output exceeds the tier cap, rewrite shorter before finalizing.
Never use the 500-word cap for S, M, or L.

TIER SHAPE

Tier S:
2–3 flow beats
2 levers
1–2 pressures""",
    "M": """HARD WORD CAPS

Tier M: absolute max 350 words

If output exceeds the tier cap, rewrite shorter before finalizing.
Never use the 500-word cap for S, M, or L.

TIER SHAPE

Tier M:
260–350 words
3–4 flow beats
2–3 levers
2 pressures""",
    "L": """HARD WORD CAPS

Tier L: absolute max 380 words

If output exceeds the tier cap, rewrite shorter before finalizing.
Never use the 500-word cap for S, M, or L.

TIER SHAPE

Tier L:
4–6 flow beats
4–5 levers
2–3 pressures""",
    "XL": """HARD WORD CAPS

Tier XL: absolute max 500 words

If output exceeds the tier cap, rewrite shorter before finalizing.
Never use the 500-word cap for S, M, or L.

TIER SHAPE

Tier XL:
6–8 flow beats
5–7 levers
3–4 pressures""",
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


def _post_ref(post_key: str) -> str:
    return post_key.split("#", 1)[0]


def _safe_name(value: str) -> str:
    return value.replace("/", "_").replace("#", "_").replace(".", "_")


def _render_system(prompt: str, tier: str) -> str:
    rendered, count = re.subn(
        r"HARD WORD CAPS\n\n.*?\nSTYLE",
        f"{TIER_LIMITS[tier]}\n\nSTYLE",
        prompt,
        count=1,
        flags=re.S,
    )
    if count != 1:
        return f"{prompt.rstrip()}\n\n{TIER_LIMITS[tier]}"
    return rendered


def _render_self_check(prompt: str) -> None:
    for tier in TIER_LIMITS:
        rendered = _render_system(prompt, tier)
        assert f"Tier {tier}:" in rendered
        for other in TIER_LIMITS:
            if other != tier:
                assert f"Tier {other}:" not in rendered


def _call_openrouter(system: str, payload: dict[str, Any], model: str, max_tokens: int) -> str:
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
        raise RuntimeError(f"OpenRouter {resp.status_code}: {resp.text[:800]}")
    return str(resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")).strip()


def _word_count(text: str) -> int:
    return len([w for w in text.replace("/", " ").split() if w.strip()])


def _write_postcard(system: str, fingerprint: dict[str, Any], tier: str, model: str) -> str:
    cap = WORD_CAPS[tier]
    text = _call_openrouter(system, {"fingerprint": fingerprint}, model, TOKEN_CAPS[tier])
    for _ in range(3):
        words = _word_count(text)
        if words <= cap:
            return text
        text = _call_openrouter(
            system,
            {
                "fingerprint": fingerprint,
                "previous_postcard": text,
                "rewrite_instruction": (
                    f"Rewrite the previous postcard under the Tier {tier} absolute cap of {cap} words. "
                    "Keep the same output sections. Do not add facts. Remove repeated evidence first."
                ),
            },
            model,
            TOKEN_CAPS[tier],
        )
    return text


def run(args: argparse.Namespace) -> None:
    _load_env()
    in_path = Path(args.input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    prompt = Path(args.prompt).read_text()
    _render_self_check(prompt)
    combined = json.loads(in_path.read_text())
    results: list[dict[str, Any]] = []

    for item in combined.get("items", []):
        tier = str(item.get("tier") or item.get("fingerprint", {}).get("tier") or "").strip()
        if tier not in TIER_LIMITS:
            raise RuntimeError(f"Unknown tier for {item.get('post_key')}: {tier}")
        fingerprint = dict(item.get("fingerprint") or {})
        fingerprint["tier"] = tier
        fingerprint["handle"] = f"@{str(item.get('handle') or fingerprint.get('handle') or '').lstrip('@')}"
        fingerprint["post_key"] = item.get("post_key")
        fingerprint["post_ref_without_suffix"] = _post_ref(str(item.get("post_key") or ""))

        post_key = str(item.get("post_key") or "")
        base = out_dir / f"{_safe_name(str(item.get('handle') or 'unknown'))}_{_safe_name(post_key)}"
        if base.with_suffix(".md").exists() and not args.force:
            text = base.with_suffix(".md").read_text()
            status = "skipped_complete"
        else:
            text = _write_postcard(_render_system(prompt, tier), fingerprint, tier, args.model)
            base.with_suffix(".md").write_text(text)
            status = "complete"
        result = {
            "handle": item.get("handle"),
            "post_key": post_key,
            "media_type": item.get("media_type"),
            "tier": tier,
            "status": status,
            "word_count": _word_count(text),
            "file": str(base.with_suffix(".md")),
            "postcard": text,
        }
        results.append(result)
        print(json.dumps({k: result[k] for k in ("handle", "post_key", "tier", "status", "word_count", "file")}, ensure_ascii=False), flush=True)

    out = {
        "model": args.model,
        "source_fingerprints": str(in_path),
        "prompt": str(Path(args.prompt)),
        "count": len(results),
        "items": results,
    }
    out_path = out_dir / "postcards_v4_gpt54_mini_selected_15_combined.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(json.dumps({"combined": str(out_path), "count": len(results)}, ensure_ascii=False))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--prompt", default=str(DEFAULT_PROMPT))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--model", default=os.getenv("POSTCARD_WRITER_MODEL", DEFAULT_MODEL))
    parser.add_argument("--force", action="store_true")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
