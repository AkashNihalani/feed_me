"""Run a local cold-start feeder-file test from observation_v1 fingerprints."""
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
OUT_DIR = WORKER_DIR / "scripts" / "out" / "observation_cold_start"
ENV_PATHS = (WORKER_DIR / ".env", WORKER_DIR / ".env.vps-production")
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
DEFAULT_PROMPT = Path("/Users/sky/.codex/attachments/e0fabc88-89b1-4f2b-b752-3c3a518113b6/pasted-text.txt")
DEFAULT_MODELS = ("openai/gpt-5.4",)
PROMPT_VERSION = "feeder_file_cold_start_observation_v1_metric_line"

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


def _strip_prompt_intro(text: str) -> str:
    marker = "FEEDER FILE COLD START"
    idx = text.find(marker)
    return text[idx:].strip() if idx >= 0 else text.strip()


def _extract_json_object(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = t.split("\n", 1)[1] if "\n" in t else ""
        if t.rstrip().endswith("```"):
            t = t.rstrip()[:-3]
    start = t.find("{")
    if start < 0:
        raise ValueError("no JSON object found in model output")
    depth = 0
    in_str = False
    esc = False
    for index in range(start, len(t)):
        char = t[index]
        if in_str:
            if esc:
                esc = False
            elif char == "\\":
                esc = True
            elif char == '"':
                in_str = False
        elif char == '"':
            in_str = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return t[start:index + 1]
    raise ValueError("unbalanced JSON object in model output")


def _model_slug(model: str) -> str:
    return model.replace("/", "_").replace(".", "").replace("-", "_")


def _lane(media_type: str) -> str:
    m = (media_type or "").lower()
    if m in {"sidecar", "carousel"}:
        return "carousel"
    if m in {"image", "photo"}:
        return "image"
    return "reel"


def _lane_plural(lane: str) -> str:
    return {"reel": "reels", "carousel": "carousels", "image": "images"}[lane]


def _age_label(posted_at: datetime | None) -> str:
    if posted_at is None:
        return "age unknown"
    now = datetime.now(timezone.utc)
    days = max(0, int((now - posted_at.astimezone(timezone.utc)).total_seconds() // 86400))
    if days < 7:
        value, unit = max(1, days), "day"
    elif days < 60:
        value, unit = max(1, round(days / 7)), "week"
    else:
        value, unit = max(1, round(days / 30)), "month"
    return f"{value} {unit}{'' if value == 1 else 's'} ago"


def _band(rank: int | None, pool: int | None) -> str:
    if not rank or not pool:
        return "unknown"
    pct = rank / pool * 100
    if pct <= 10:
        return "peak"
    if pct <= 25:
        return "top"
    if pct <= 50:
        return "above_average"
    if pct <= 75:
        return "below_average"
    return "bottom"


def _band_label(band: str) -> str:
    return {
        "peak": "peak",
        "top": "top",
        "above_average": "above average",
        "below_average": "below average",
        "bottom": "bottom",
    }.get(band, "pending")


def _load_fingerprints(handle: str) -> dict[str, dict[str, Any]]:
    folder = OUT_DIR.parent / "observation_mixed_media_fingerprints" / handle.replace(".", "_")
    items: dict[str, dict[str, Any]] = {}
    for path in sorted(folder.glob("*.json")):
        try:
            data = json.loads(path.read_text())
        except Exception:
            continue
        post_key = str(data.get("post_key") or "").strip().lower()
        if data.get("fingerprint_schema_version") != "observation_v1" or not post_key:
            continue
        items[post_key] = data
    return items


def _alias_fingerprint(fingerprint: dict[str, Any], alias: str) -> dict[str, Any]:
    cleaned = dict(fingerprint)
    cleaned.pop("post_key", None)
    cleaned["post_alias"] = alias
    return cleaned


def _post_meta(conn: Any, handle: str, post_keys: list[str]) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """
        select p.post_key, p.posted_at, lower(coalesce(p.media_type, '')) as media_type
        from public.posts p
        join public.feeders f on f.id = p.feeder_id
        where lower(f.handle) = lower(%s)
          and p.post_key = any(%s)
        """,
        (handle, post_keys),
    ).fetchall()
    meta = {str(row["post_key"]).lower(): dict(row) for row in rows}

    for lane in ("reel", "carousel", "image"):
        lane_types = ("reel", "video") if lane == "reel" else ("sidecar", "carousel") if lane == "carousel" else ("image", "photo")
        pool = conn.execute(
            """
            select distinct on (p.post_key) p.post_key, pm.percentile_performance_exact, pm.percentile_performance, pm.views
            from public.posts p
            join public.feeders f on f.id = p.feeder_id
            join public.post_metrics pm on pm.post_key = p.post_key and lower(pm.checkpoint) = 'd7'
            where lower(f.handle) = lower(%s)
              and lower(coalesce(p.media_type, '')) = any(%s)
              and p.posted_at >= now() - interval '90 days'
              and coalesce(pm.percentile_performance_exact, pm.percentile_performance) is not null
            order by p.post_key, pm.computed_at desc nulls last
            """,
            (handle, list(lane_types)),
        ).fetchall()
        ranked = sorted(pool, key=lambda row: (float(row.get("percentile_performance_exact") or row.get("percentile_performance")), -float(row.get("views") or 0)))
        ranks = {str(row["post_key"]).lower(): index + 1 for index, row in enumerate(ranked)}
        for post_key, row in meta.items():
            if _lane(str(row.get("media_type") or "")) == lane:
                row["rank"] = ranks.get(post_key)
                row["pool"] = len(ranked)
    return meta


def _build_payload(
    conn: Any,
    handle: str,
    target: int,
    prompt_version: str,
    exclude_post_keys: set[str] | None = None,
) -> tuple[dict[str, Any], dict[str, str]]:
    fingerprints = _load_fingerprints(handle)
    for post_key in exclude_post_keys or set():
        fingerprints.pop(post_key.lower(), None)
    meta = _post_meta(conn, handle, list(fingerprints))
    ordered = sorted(
        fingerprints,
        key=lambda key: meta.get(key, {}).get("posted_at") or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )[:target]
    posts = []
    alias_map: dict[str, str] = {}
    for index, post_key in enumerate(ordered, start=1):
        alias = f"p{index:02d}"
        alias_map[alias] = post_key
        row = meta.get(post_key, {})
        lane = _lane(str(row.get("media_type") or fingerprints[post_key].get("media_type") or ""))
        band = _band(row.get("rank"), row.get("pool"))
        line = "pending"
        if row.get("rank") and row.get("pool"):
            line = f"Ranked {row['rank']} out of {row['pool']} {_lane_plural(lane)} - {_band_label(band)} performance for the window."
        posts.append(
            {
                "post_alias": alias,
                "media_type": lane,
                "age_label": _age_label(row.get("posted_at")),
                "performance_band": band,
                "performance_line": line,
                "fingerprint": _alias_fingerprint(fingerprints[post_key], alias),
            }
        )
    return {
        "handle": handle,
        "prompt_version": prompt_version,
        "basis": {
            "successful_posts_read": len(posts),
            "failed_posts": max(0, target - len(posts)),
            "performance_attached": any(post["performance_line"] != "pending" for post in posts),
            "rank_status": "attached" if any(post["performance_line"] != "pending" for post in posts) else "metrics_missing",
        },
        "media_counts": {
            "reel": sum(1 for post in posts if post["media_type"] == "reel"),
            "carousel": sum(1 for post in posts if post["media_type"] == "carousel"),
            "image": sum(1 for post in posts if post["media_type"] == "image"),
        },
        "performance_band_legend": {
            "peak": "top 10%",
            "top": "10-25%",
            "above_average": "25-50%",
            "below_average": "50-75%",
            "bottom": "75-100%",
            "unknown": "missing, too fresh, or too thin",
        },
        "posts": posts,
    }, alias_map


def _call_model(system_prompt: str, payload: dict[str, Any], model: str, max_tokens: int) -> tuple[str, dict[str, Any], dict[str, Any]]:
    resp = requests.post(
        f"{os.environ.get('OPENROUTER_BASE_URL', 'https://openrouter.ai/api/v1').rstrip('/')}/chat/completions",
        headers={
            "Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://feedme.local",
            "X-Title": "FeedMe Observation Cold Start Test",
        },
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False, indent=2, default=str)},
            ],
            "temperature": 0.1,
            "max_tokens": max_tokens,
            "usage": {"include": True},
        },
        timeout=900,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"OpenRouter {resp.status_code}: {resp.text[:1200]}")
    data = resp.json()
    choice = data["choices"][0]
    return choice["message"]["content"], data.get("usage") or {}, {
        "id": data.get("id"),
        "finish_reason": choice.get("finish_reason"),
        "native_finish_reason": choice.get("native_finish_reason"),
    }


def run(args: argparse.Namespace) -> None:
    _load_env()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    system_prompt = _strip_prompt_intro(Path(args.prompt).read_text())
    models = args.models or list(DEFAULT_MODELS)
    run_name = args.run_name or PROMPT_VERSION
    exclude_post_keys = {value.strip().lower() for value in args.exclude_post_keys.split(",") if value.strip()}
    with psycopg.connect(os.environ["POSTGRES_DSN"], row_factory=dict_row, connect_timeout=20) as conn:
        payload, alias_map = _build_payload(conn, args.handle, args.target, run_name, exclude_post_keys)

    slug = args.handle.replace(".", "_")
    payload_path = OUT_DIR / f"{slug}_{run_name}_payload.json"
    alias_map_path = OUT_DIR / f"{slug}_{run_name}_alias_map.json"
    payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    alias_map_path.write_text(json.dumps(alias_map, ensure_ascii=False, indent=2, default=str))
    print(json.dumps({"stage": "payload", "path": str(payload_path), "posts": len(payload["posts"]), "media_counts": payload["media_counts"]}), flush=True)
    if args.dry_run:
        return

    for model in models:
        model_slug = _model_slug(model)
        print(json.dumps({"stage": "model_start", "model": model}), flush=True)
        raw, usage, response_meta = _call_model(system_prompt, payload, model, args.max_tokens)
        raw_path = OUT_DIR / f"{slug}_{run_name}_{model_slug}.raw.txt"
        json_path = OUT_DIR / f"{slug}_{run_name}_{model_slug}.json"
        raw_path.write_text(raw)
        try:
            parsed = json.loads(_extract_json_object(raw))
        except Exception as exc:
            err_path = OUT_DIR / f"{slug}_{run_name}_{model_slug}.error.json"
            err_path.write_text(json.dumps({"model": model, "error": str(exc), "raw": str(raw_path), "usage": usage, "response_meta": response_meta}, indent=2, default=str))
            print(json.dumps({"stage": "model_error", "model": model, "error": str(exc), "raw": str(raw_path), "usage": usage, "response_meta": response_meta}), flush=True)
            continue
        parsed["source_model"] = model
        parsed["_response_meta"] = response_meta
        parsed["_usage"] = usage
        json_path.write_text(json.dumps(parsed, ensure_ascii=False, indent=2, default=str))
        print(json.dumps({"stage": "model_done", "model": model, "json": str(json_path), "usage": usage, "response_meta": response_meta}), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handle", default="anuj.mp4")
    parser.add_argument("--target", type=int, default=30)
    parser.add_argument("--prompt", default=str(DEFAULT_PROMPT))
    parser.add_argument("--model", action="append", dest="models")
    parser.add_argument("--run-name", default=PROMPT_VERSION)
    parser.add_argument("--exclude-post-keys", default="")
    parser.add_argument("--max-tokens", type=int, default=20000)
    parser.add_argument("--dry-run", action="store_true")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
