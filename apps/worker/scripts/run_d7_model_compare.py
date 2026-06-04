from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ENV_PATH = ROOT / "apps" / "worker" / ".env"

DEFAULT_POSTS = [
    "p/dyjcvjqmpbo#f27",  # Anuj: Meloni/Melody/Parle benchmark
    "p/dyzptg0mok8#f35",  # Lakme: generic no-collab Eyeconic product reel
]
DEFAULT_MODELS = [
    "anthropic/claude-haiku-4.5",
    "anthropic/claude-sonnet-4.6",
]


def _load_env() -> None:
    if not ENV_PATH.exists():
        return
    for raw_line in ENV_PATH.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

    dsn = os.environ.get("POSTGRES_DSN", "")
    if "db.worqtdkvicuhmdgoncru.supabase.co" in dsn:
        dsn = dsn.replace(
            "db.worqtdkvicuhmdgoncru.supabase.co",
            "aws-1-ap-south-1.pooler.supabase.com",
        )
        dsn = dsn.replace("postgres:", "postgres.worqtdkvicuhmdgoncru:", 1)
        os.environ["POSTGRES_DSN"] = dsn


def _post_meta(conn: Any, post_key: str) -> dict[str, Any]:
    row = conn.execute(
        """
        select p.post_key, p.post_url, p.posted_at, f.handle,
               pm.views, pm.likes, pm.comments
        from public.posts p
        join public.feeders f on f.id = p.feeder_id
        join public.post_metrics pm
          on pm.post_key = p.post_key
         and lower(pm.checkpoint) = 'd7'
        where p.post_key = %s
        order by pm.computed_at desc nulls last
        limit 1
        """,
        (post_key,),
    ).fetchone()
    return dict(row) if row else {"post_key": post_key}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--post-key", action="append", dest="post_keys", help="Post key to test; repeatable")
    parser.add_argument("--model", action="append", dest="models", help="OpenRouter model id; repeatable")
    parser.add_argument("--store", action="store_true", help="Store model-call rows. Default is no-store.")
    args = parser.parse_args()

    _load_env()

    import psycopg
    from psycopg.rows import dict_row

    from apps.worker.app import config
    from apps.worker.app import fingerprint_intelligence as fi
    from apps.worker.app.fingerprint_intelligence import (
        D7_READ_PROMPT_VERSION,
        _build_d7_read_input,
        _call_d7_read_model,
        _d7_fun_fact,
        _normalize_d7_read_mapping,
        _record_d7_read_model_call,
    )

    post_keys = args.post_keys or DEFAULT_POSTS
    models = args.models or DEFAULT_MODELS

    with psycopg.connect(os.environ["POSTGRES_DSN"], row_factory=dict_row, connect_timeout=20) as conn:
        for post_key in post_keys:
            payload = _build_d7_read_input(conn, post_key)
            if not payload:
                print(json.dumps({
                    "status": "payload_failed",
                    "post_key": post_key,
                    "meta": _post_meta(conn, post_key),
                }, default=str), flush=True)
                continue

            try:
                fun_fact = _d7_fun_fact(conn, post_key)
            except Exception as exc:
                fun_fact = {"kind": "error", "text": f"fun_fact failed: {exc}"}
            meta = _post_meta(conn, post_key)
            preflight = {
                "post_key": post_key,
                "url": meta.get("post_url"),
                "handle": meta.get("handle"),
                "posted_at": meta.get("posted_at"),
                "views": meta.get("views"),
                "likes": meta.get("likes"),
                "comments": meta.get("comments"),
                "prompt_version": D7_READ_PROMPT_VERSION,
                "payload": {
                    "collab_post": payload["this_post"].get("collab_post"),
                    "related_handles": payload["this_post"].get("related_handles"),
                    "vs_90d": payload["this_post"].get("vs_90d"),
                    "place_in_30": payload["this_post"].get("place_in_30"),
                    "recent_posts": len(payload.get("recent_posts") or []),
                    "momentum": payload.get("momentum"),
                    "concentration": payload.get("concentration"),
                    "splits": payload.get("splits"),
                    "fun_fact": fun_fact,
                },
            }

            for model in models:
                config.D7_READ_MODEL = model
                fi.D7_READ_MODEL = model
                print(json.dumps({"status": "calling", "model": model, **preflight}, default=str), flush=True)
                result = _call_d7_read_model(payload, post_key=post_key)
                if not result:
                    print(json.dumps({"status": "model_failed", "model": model, **preflight}, default=str), flush=True)
                    continue

                raw_output, parsed, call_error = result
                if fun_fact and isinstance(parsed, dict) and isinstance(parsed.get("d7_read"), dict):
                    parsed["d7_read"]["fun_fact"] = fun_fact
                normalized = _normalize_d7_read_mapping(parsed, post_key=post_key) if parsed else None

                if args.store:
                    _record_d7_read_model_call(
                        conn,
                        post_key=post_key,
                        user_payload=payload,
                        raw_output=raw_output,
                        parsed_output=parsed,
                        status="complete" if parsed else "failed",
                        error=call_error,
                    )
                    conn.commit()

                print(json.dumps({
                    "status": "complete" if normalized else "failed",
                    "model": model,
                    "stored": bool(args.store),
                    **preflight,
                    "d7_read": normalized,
                    "error": call_error,
                    "raw": None if normalized else raw_output,
                }, ensure_ascii=False, default=str, indent=2), flush=True)


if __name__ == "__main__":
    main()
