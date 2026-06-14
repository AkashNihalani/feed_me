from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

WORKER_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_DIR))

from app.feeder_prompts import FEEDER_FILE_ROLLING_PROMPT_VERSION  # noqa: E402

ENV_PATH = WORKER_DIR / ".env"
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
OUT = WORKER_DIR / "scripts" / "out"

LOCKED_FILES = {
    "anuj.mp4": {
        "feeder_id": 27,
        "path": OUT / "anuj_mp4_feeder_file_rolling_gpt54_locked.json",
    },
    "lakmeindia": {
        "feeder_id": 35,
        "path": OUT / "lakmeindia_feeder_file_rolling_gpt54_locked.json",
    },
    "srishtigargg": {
        "feeder_id": 20,
        "path": OUT / "srishtigargg_merged_srishti_gpt54_merge_next10_retry.json",
    },
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


def pooler_dsn() -> str:
    dsn = os.environ["POSTGRES_DSN"]
    if "db.worqtdkvicuhmdgoncru.supabase.co" in dsn:
        dsn = dsn.replace("db.worqtdkvicuhmdgoncru.supabase.co", POOLER_HOST)
        dsn = dsn.replace("postgres:", "postgres.worqtdkvicuhmdgoncru:", 1)
    return dsn


def validate_feed_file(handle: str, feed_file: dict) -> list[str]:
    problems: list[str] = []
    if feed_file.get("handle") != handle:
        problems.append(f"handle mismatch: file has {feed_file.get('handle')!r}")
    bites = feed_file.get("bites")
    if not isinstance(bites, list) or not bites:
        problems.append("missing bites")
    seen_receipts: set[str] = set()
    for bite in bites or []:
        if not bite.get("name"):
            problems.append("bite missing name")
        receipts = bite.get("receipts")
        if not isinstance(receipts, list) or not receipts:
            problems.append(f"{bite.get('name')}: missing receipts")
            continue
        tally = {"core": 0, "supporting": 0, "standby": 0}
        for receipt in receipts:
            rid = receipt.get("receipt_id")
            if rid:
                if rid in seen_receipts:
                    problems.append(f"duplicate receipt_id {rid}")
                seen_receipts.add(rid)
            if not receipt.get("post_key"):
                problems.append(f"{bite.get('name')}: receipt missing post_key")
            weight = receipt.get("weight")
            if weight in tally:
                tally[weight] += 1
            else:
                problems.append(f"{bite.get('name')}: invalid weight {weight!r}")
        if bite.get("weights_tally") != tally:
            problems.append(f"{bite.get('name')}: weights_tally mismatch")
    return problems


def main() -> None:
    load_env()
    with psycopg.connect(pooler_dsn(), autocommit=True, connect_timeout=20) as conn:
        for handle, info in LOCKED_FILES.items():
            feed_file = json.loads(info["path"].read_text())
            problems = validate_feed_file(handle, feed_file)
            if problems:
                raise RuntimeError(f"{handle} validation failed: {problems}")

            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    select id, compile_version, status, updated_at
                    from public.feeder_files
                    where feeder_id = %s and lower(feeder_handle) = lower(%s)
                      and status = 'active'
                    order by updated_at desc nulls last, id desc
                    limit 5
                    """,
                    (info["feeder_id"], handle),
                )
                before = cur.fetchall()
                cur.execute(
                    """
                    insert into public.feeder_files
                      (feeder_id, feeder_handle, feed_file, compile_version, status,
                       source, generated_at, updated_at)
                    values (%s, %s, %s, %s, 'active', %s, now(), now())
                    on conflict (feeder_handle, compile_version) do update
                      set feed_file = excluded.feed_file,
                          status = 'active',
                          source = excluded.source,
                          generated_at = now(),
                          updated_at = now()
                    returning id, compile_version, status, updated_at
                    """,
                    (
                        info["feeder_id"],
                        handle,
                        json.dumps(feed_file, ensure_ascii=False),
                        FEEDER_FILE_ROLLING_PROMPT_VERSION,
                        f"locked GPT 5.4 rolling feeder file from {info['path'].name}",
                    ),
                )
                row = cur.fetchone()
            print(f"\n{handle}")
            print("before active:", [dict(r) for r in before])
            print("activated:", dict(row))
            print("file:", info["path"].name)
            print("bites:", len(feed_file.get("bites", [])), "warnings:", feed_file.get("merge_warnings", []))


if __name__ == "__main__":
    main()
