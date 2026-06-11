from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ENV_PATH = ROOT / "apps" / "worker" / ".env"


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
        ).replace("postgres:", "postgres.worqtdkvicuhmdgoncru:", 1)
        os.environ["POSTGRES_DSN"] = dsn


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handle", default="anuj.mp4")
    parser.add_argument("--post-key", help="rerun the production D7 read for this post (compute only, no store)")
    parser.add_argument("--store", action="store_true", help="persist the recomputed read so the frontend reflects it")
    args = parser.parse_args()

    _load_env()
    from apps.worker.app import config
    config.POSTGRES_DSN = os.environ["POSTGRES_DSN"]

    import psycopg
    from psycopg.rows import dict_row
    from apps.worker.app.fingerprint_intelligence import (
        D7_READ_PROMPT_VERSION,
        _build_d7_read_input,
        _call_d7_read_model,
        _d7_fun_fact,
        _normalize_d7_read_mapping,
        _record_d7_read_model_call,
    )

    conn = psycopg.connect(os.environ["POSTGRES_DSN"], row_factory=dict_row)
    try:
        if not args.post_key:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select p.post_key, p.posted_at, pm.business_date_ist, left(coalesce(p.caption,''), 90) cap
                    from public.posts p
                    join public.feeders f on f.id = p.feeder_id
                    join public.post_metrics pm on pm.post_key = p.post_key and lower(pm.checkpoint)='d7'
                    where f.handle = %s
                    order by p.posted_at desc nulls last
                    limit 15
                    """,
                    (args.handle,),
                )
                print(json.dumps([dict(r) for r in cur.fetchall()], default=str, indent=2))
            return

        payload = _build_d7_read_input(conn, args.post_key)
        if not payload:
            raise RuntimeError("could not build production D7 payload for this post")
        print("=== PROMPT VERSION:", D7_READ_PROMPT_VERSION, "===", flush=True)
        result = _call_d7_read_model(payload, post_key=args.post_key)
        if not result:
            raise RuntimeError("model returned nothing")
        raw_output, parsed, call_error = result
        fun_fact = _d7_fun_fact(conn, args.post_key)
        if fun_fact and isinstance(parsed, dict) and isinstance(parsed.get("d7_read"), dict):
            parsed["d7_read"]["fun_fact"] = fun_fact
        print("=== NEW READ ===")
        print(json.dumps(parsed, ensure_ascii=False, indent=2, default=str))
        if call_error:
            print("error:", call_error)
        if args.store and parsed:
            _record_d7_read_model_call(
                conn, post_key=args.post_key, user_payload=payload,
                raw_output=raw_output, parsed_output=parsed, status="complete", error=None,
            )
            conn.commit()
            print("=== STORED at", D7_READ_PROMPT_VERSION, "===")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
