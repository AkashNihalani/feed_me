from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
ENV_PATH = ROOT / "apps" / "worker" / ".env"

TARGET_POST_KEYS = [
    "p/dyuqvxkqmx_#f27",
    "p/dyjcvjqmpbo#f27",
    "p/dyrsmbvmasv#f27",
    "p/dyajiggskq7#f27",
    "p/dx9nre-mkk3#f27",
    "p/dx7gl9pb0fm#f27",
    "p/dxtk73kjeaw#f27",
    "p/dxoh-undfw1#f27",
    "p/dxjldsojbr7#f27",
    "p/dxohqewjl7t#f27",
    "p/dx6do9cspq9#f27",
    "p/dxmdkfxqanx#f27",
    "p/dxkqxjkdeme#f27",
    "p/dw_u3zwjigx#f27",
    "p/dxy_ou6kc9k#f27",
    "p/dw_nu_6dkw0#f27",
    "p/dwl-tuqiein",
    "p/dwleceadd9x",
    "p/dxrbskqjf60#f27",
    "p/dx1mnvssijx#f27",
    "p/dwwzavqjoit",
    "p/dywjnk2mrli#f27",
]
TARGET_V5_TOTAL = int(os.environ.get("ANUJ_TARGET_V5_TOTAL", "31"))


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


def _counts(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              count(*) filter (where pf.post_key is not null) as v10_fps,
              count(*) filter (where pc.post_key is not null) as v5_condensations
            from public.posts p
            join public.feeders f on f.id = p.feeder_id
            left join public.post_fingerprints pf
              on pf.post_key = p.post_key
             and pf.model_version like 'openrouter:google/gemini-3.5-flash:fingerprint_v10_duration_context%%'
            left join public.post_condensations pc
              on pc.post_key = p.post_key
             and pc.model_version like 'openrouter:anthropic/claude-haiku-4.5:post_condensation_v5_character_transfer%%'
            where f.handle = 'anuj.mp4'
              and lower(coalesce(p.media_type, '')) in ('reel', 'video')
              and p.posted_at >= now() - interval '90 days'
            """
        )
        return dict(cur.fetchone() or {})


def _preflight(conn, post_key: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              exists (
                select 1
                from public.post_condensations
                where post_key = %s
                  and model_version like 'openrouter:anthropic/claude-haiku-4.5:post_condensation_v5_character_transfer%%'
              ) as has_v5,
              exists (
                select 1
                from public.post_media_assets
                where post_key = %s
                  and asset_role = 'video_full'
                  and status in ('active', 'purge_pending')
                  and coalesce(storage_path, public_url, '') <> ''
              ) as has_video,
              (
                select pf.fingerprint
                from public.post_fingerprints pf
                where pf.post_key = %s
                  and pf.model_version like 'openrouter:google/gemini-3.5-flash:fingerprint_v10_duration_context%%'
                order by pf.updated_at desc nulls last
                limit 1
              ) as existing_fingerprint
            """,
            (post_key, post_key, post_key),
        )
        return dict(cur.fetchone() or {})


def _target_verify(conn) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              p.post_key,
              p.posted_at,
              pc.post_key is not null as has_v5,
              exists (
                select 1
                from public.post_media_assets a
                where a.post_key = p.post_key
                  and a.asset_role = 'video_full'
                  and a.status in ('active', 'purge_pending')
                  and coalesce(a.storage_path, a.public_url, '') <> ''
              ) as has_video,
              left(coalesce((
                select a.last_error
                from public.post_media_assets a
                where a.post_key = p.post_key
                  and a.asset_role = 'video_full'
                order by a.updated_at desc nulls last
                limit 1
              ), ''), 160) as video_error
            from public.posts p
            join public.feeders f on f.id = p.feeder_id
            left join public.post_condensations pc
              on pc.post_key = p.post_key
             and pc.model_version like 'openrouter:anthropic/claude-haiku-4.5:post_condensation_v5_character_transfer%%'
            where f.handle = 'anuj.mp4'
              and p.post_key = any(%s)
            order by p.posted_at desc nulls last
            """,
            (TARGET_POST_KEYS,),
        )
        return [dict(row) for row in cur.fetchall()]


def main() -> None:
    _load_env()

    from apps.worker.app import config

    config.POSTGRES_DSN = os.environ["POSTGRES_DSN"]

    from apps.worker.app.fingerprint_intelligence import (
        ensure_post_condensation,
        ensure_post_fingerprint,
    )
    from apps.worker.app.pure_engine import PureEngine

    engine = PureEngine()
    conn = engine.conn
    start_counts = _counts(conn)
    print("start_counts", json.dumps(start_counts, default=str), flush=True)

    for index, post_key in enumerate(TARGET_POST_KEYS, start=1):
        current_counts = _counts(conn)
        if int(current_counts.get("v5_condensations") or 0) >= TARGET_V5_TOTAL:
            print("target_reached", json.dumps(current_counts, default=str), flush=True)
            break

        result = {"idx": index, "post_key": post_key}
        try:
            preflight = _preflight(conn, post_key)
            result["pre_v5"] = bool(preflight.get("has_v5"))
            result["pre_video"] = bool(preflight.get("has_video"))

            if not result["pre_v5"]:
                existing_fingerprint = preflight.get("existing_fingerprint")
                if isinstance(existing_fingerprint, dict):
                    fingerprint = existing_fingerprint
                    result["fingerprint_from_cache"] = True
                else:
                    if not result["pre_video"]:
                        repair = engine.repair_post_visual_media(post_key)
                        result["repair"] = {
                            key: repair.get(key)
                            for key in ("found", "staged", "captured", "failed", "retired")
                        }
                    fingerprint = ensure_post_fingerprint(conn, post_key)
                    result["fingerprint_from_cache"] = False

                result["fingerprint_ok"] = bool(fingerprint)
                if fingerprint:
                    condensation = ensure_post_condensation(conn, post_key, fingerprint)
                    result["condensation_ok"] = bool(condensation)
                else:
                    result["condensation_ok"] = False
            else:
                result["fingerprint_ok"] = True
                result["condensation_ok"] = True

            conn.commit()
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            result["error"] = f"{type(exc).__name__}: {str(exc)[:240]}"

        print(json.dumps(result, default=str), flush=True)
        time.sleep(0.5)

    print("end_counts", json.dumps(_counts(conn), default=str), flush=True)
    print("target_verify", json.dumps(_target_verify(conn), default=str, indent=2), flush=True)
    conn.close()


if __name__ == "__main__":
    main()
