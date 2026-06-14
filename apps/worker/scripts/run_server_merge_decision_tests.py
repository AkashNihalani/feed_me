from __future__ import annotations

import json
import os
import re
import sys
import argparse
from datetime import datetime, timezone
from pathlib import Path

import requests

WORKER_DIR = Path(__file__).resolve().parents[1]
OUT = WORKER_DIR / "scripts" / "out"
ENV_PATH = WORKER_DIR / ".env"
PROMPT_PATH = WORKER_DIR / "scripts" / "feeder_file_server_merge_decision_prompt_temp.md"

RUNS_BY_HANDLE = {
    "anuj.mp4": [
        {
            "name": "anuj_gemini_chunk_deepseek_decision",
            "model": "deepseek/deepseek-v4-pro",
            "base": "anuj_mp4_feeder_file_cold_start_v8_1_flash_retry.json",
            "chunk": "anuj_mp4_chunk_v1.json",
        },
        {
            "name": "anuj_gpt54_chunk_deepseek_decision",
            "model": "deepseek/deepseek-v4-pro",
            "base": "anuj_mp4_feeder_file_cold_start_v8_1_gpt54.json",
            "chunk": "anuj_mp4_chunk_gpt54_10post_v1.json",
        },
        {
            "name": "anuj_gemini_native_decision",
            "model": "google/gemini-3.5-flash",
            "base": "anuj_mp4_feeder_file_cold_start_v8_1_flash_retry.json",
            "chunk": "anuj_mp4_chunk_v1.json",
        },
        {
            "name": "anuj_gpt54_native_decision",
            "model": "openai/gpt-5.4",
            "base": "anuj_mp4_feeder_file_cold_start_v8_1_gpt54.json",
            "chunk": "anuj_mp4_chunk_gpt54_10post_v1.json",
        },
    ],
    "lakmeindia": [
        {
            "name": "lakmeindia_gemini_chunk_deepseek_decision",
            "model": "deepseek/deepseek-v4-pro",
            "base": "lakmeindia_feeder_file_cold_start_v8_1_flash.json",
            "chunk": "lakmeindia_chunk_v1.json",
        },
        {
            "name": "lakmeindia_gpt54_chunk_deepseek_decision",
            "model": "deepseek/deepseek-v4-pro",
            "base": "lakmeindia_feeder_file_cold_start_v8_1_gpt54.json",
            "chunk": "lakmeindia_chunk_gpt54_10post_v1.json",
        },
        {
            "name": "lakmeindia_gemini_native_decision",
            "model": "google/gemini-3.5-flash",
            "base": "lakmeindia_feeder_file_cold_start_v8_1_flash.json",
            "chunk": "lakmeindia_chunk_v1.json",
        },
        {
            "name": "lakmeindia_gpt54_native_decision",
            "model": "openai/gpt-5.4",
            "base": "lakmeindia_feeder_file_cold_start_v8_1_gpt54.json",
            "chunk": "lakmeindia_chunk_gpt54_10post_v1.json",
        },
    ],
}


def load_env() -> None:
    for raw in ENV_PATH.read_text().splitlines():
        line = raw.strip()
        if line.startswith("export "):
            line = line[len("export ") :]
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def system_prompt(handle: str) -> str:
    text = PROMPT_PATH.read_text()
    match = re.search(r"```text\n(.*?)\n```", text, re.S)
    if not match:
        raise RuntimeError("prompt markdown does not contain a text fence")
    return match.group(1).replace("@{handle}", f"@{handle}")


def extract_json_object(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = t.split("\n", 1)[1] if "\n" in t else ""
        if t.rstrip().endswith("```"):
            t = t.rstrip()[:-3]
    start = t.find("{")
    if start == -1:
        raise ValueError("no JSON object found")
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
                return t[start : i + 1]
    raise ValueError("unbalanced JSON object")


def alias_from_post(post: str) -> str | None:
    matches = re.findall(r"\((?:current_|chunk_|c_)?(P\d{2})\)", post or "")
    return matches[-1] if matches else None


def compact_contract(contract):
    if not contract:
        return {}
    if isinstance(contract, dict):
        return contract
    if isinstance(contract, list):
        return {str(i + 1): str(v) for i, v in enumerate(contract)}
    return {"1": str(contract)}


def normalize_bites(data: dict, source: str) -> tuple[dict, list[dict]]:
    alias_map = data.get("source_alias_map") or {}
    bites = []
    for bite in data.get("bites", []):
        bite_name = bite.get("name", "")
        receipts = []
        for idx, receipt in enumerate(bite.get("receipts", []), 1):
            alias = alias_from_post(receipt.get("post", ""))
            post_key = alias_map.get(alias or "")
            receipts.append(
                {
                    "receipt_id": f"{source}:{bite_name}:{idx}",
                    "post_key": post_key,
                    "post": receipt.get("post"),
                    "date": receipt.get("date"),
                    "weight": receipt.get("weight"),
                    "how_it_shows_up": receipt.get("how_it_shows_up"),
                    "role_in_bite": receipt.get("role_in_bite"),
                    "axis_bites": receipt.get("axis_bites", []),
                }
            )
        bites.append(
            {
                "name": bite_name,
                "kind": bite.get("kind"),
                "tier": bite.get("tier"),
                "contract": compact_contract(bite.get("contract")),
                "weights_tally": bite.get("weights_tally", {}),
                "receipts": receipts,
            }
        )
    return alias_map, bites


def build_payload(handle: str, base_path: Path, chunk_path: Path) -> dict:
    base = json.loads(base_path.read_text())
    chunk = json.loads(chunk_path.read_text())
    base_aliases, current_bites = normalize_bites(base, "current")
    chunk_aliases, chunk_bites = normalize_bites(chunk, "chunk")
    in_window = sorted({*base_aliases.values(), *chunk_aliases.values()})
    return {
        "handle": handle,
        "in_window_post_keys": in_window,
        "current_file": {
            "post_names": base.get("post_names", {}),
            "bites": current_bites,
        },
        "chunk": {
            "post_names": chunk.get("post_names", {}),
            "bites": chunk_bites,
        },
    }


def call_model(model: str, prompt: str, payload: dict) -> tuple[str, dict, float]:
    started = datetime.now(timezone.utc)
    resp = requests.post(
        f"{os.environ.get('OPENROUTER_BASE_URL', 'https://openrouter.ai/api/v1').rstrip('/')}/chat/completions",
        headers={
            "Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://feedme.local",
            "X-Title": "FeedMe Server Merge Decision",
        },
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False, indent=2)},
            ],
            "temperature": 0.1,
            "max_tokens": 12000,
            "usage": {"include": True},
        },
        timeout=600,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"{model} status {resp.status_code}: {resp.text[:1200]}")
    data = resp.json()
    content = (data["choices"][0].get("message") or {}).get("content")
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError(f"{model} returned empty content: {json.dumps(data, ensure_ascii=False)[:2000]}")
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    return content, data.get("usage") or {}, elapsed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handle", default="anuj.mp4", choices=sorted(RUNS_BY_HANDLE))
    args = parser.parse_args()
    runs = RUNS_BY_HANDLE[args.handle]
    load_env()
    prompt = system_prompt(args.handle)
    summary = []
    for run in runs:
        payload = build_payload(args.handle, OUT / run["base"], OUT / run["chunk"])
        payload_path = OUT / f"{run['name']}.payload.json"
        raw_path = OUT / f"{run['name']}.raw.txt"
        out_path = OUT / f"{run['name']}.json"
        payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
        print(f"\n{run['name']} -> {run['model']}")
        print(f"  current bites: {len(payload['current_file']['bites'])}; chunk bites: {len(payload['chunk']['bites'])}")
        if out_path.exists():
            parsed = json.loads(out_path.read_text())
            decisions = parsed.get("summary", {})
            print(f"  skip existing parsed result, summary={decisions}")
            summary.append({"name": run["name"], "model": run["model"], "elapsed": None, "usage": None, "summary": decisions})
            continue
        raw, usage, elapsed = call_model(run["model"], prompt, payload)
        raw_path.write_text(raw)
        parsed = json.loads(extract_json_object(raw))
        parsed["_run"] = {
            "model": run["model"],
            "base": run["base"],
            "chunk": run["chunk"],
            "elapsed_seconds": round(elapsed, 1),
            "usage": usage,
        }
        out_path.write_text(json.dumps(parsed, ensure_ascii=False, indent=2))
        decisions = parsed.get("summary", {})
        print(f"  done: {elapsed:.1f}s, tokens={usage.get('total_tokens')}, summary={decisions}")
        summary.append({"name": run["name"], "model": run["model"], "elapsed": elapsed, "usage": usage, "summary": decisions})
    summary_name = args.handle.replace(".", "_")
    (OUT / f"{summary_name}_server_merge_decision_test_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2)
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
