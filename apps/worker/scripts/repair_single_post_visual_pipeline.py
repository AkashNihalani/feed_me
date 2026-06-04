from __future__ import annotations

import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ENV_PATH = ROOT / "apps" / "worker" / ".env"
DEFAULT_POST_KEY = "p/dxq_tfdcndg#f35"


def _load_env() -> None:
    if ENV_PATH.exists():
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


def _asset_summary(conn, post_key: str) -> list[dict]:
    rows = conn.execute(
        """
        select asset_role, status, storage_provider, storage_bucket, storage_path,
               public_url, byte_size, last_error, updated_at
        from public.post_media_assets
        where post_key = %s
          and asset_role in ('thumbnail', 'preview_5s', 'video_full')
        order by asset_role
        """,
        (post_key,),
    ).fetchall()
    return [dict(row) for row in rows]


def _state(conn, post_key: str) -> dict:
    row = conn.execute(
        """
        select
          p.post_key,
          p.post_url,
          p.posted_at,
          p.related_handles,
          p.collab_post,
          exists(select 1 from public.post_fingerprints pf where pf.post_key = p.post_key) as has_fingerprint,
          exists(
            select 1
            from public.post_condensations pc
            where pc.post_key = p.post_key
              and pc.condensation_version = 'post_condensation_v5_character_transfer'
          ) as has_condensation,
          exists(
            select 1
            from public.post_media_assets a
            where a.post_key = p.post_key
              and a.asset_role = 'video_full'
              and a.status in ('active', 'purge_pending')
              and coalesce(a.storage_path, a.public_url, '') <> ''
          ) as has_video_full
        from public.posts p
        where p.post_key = %s
        """,
        (post_key,),
    ).fetchone()
    return dict(row) if row else {"post_key": post_key, "found": False}


def main() -> None:
    post_key = (sys.argv[1] if len(sys.argv) > 1 else DEFAULT_POST_KEY).strip().lower()
    _load_env()

    # Keep this repair cheap enough for the current Lakme long-video failures.
    os.environ.setdefault("FINGERPRINT_VIDEO_SAMPLE_SECONDS", "15")
    os.environ.setdefault("FINGERPRINT_VIDEO_UPLOAD_MAX_BYTES", str(32 * 1024 * 1024))
    os.environ.setdefault("FINGERPRINT_VIDEO_INLINE_MAX_BYTES", str(20 * 1024 * 1024))

    from apps.worker.app.fingerprint_intelligence import ensure_post_condensation, ensure_post_fingerprint
    from apps.worker.app.pure_engine import PureEngine

    engine = PureEngine()
    conn = engine.conn

    print(json.dumps({"stage": "before", "state": _state(conn, post_key), "assets": _asset_summary(conn, post_key)}, default=str), flush=True)

    repair = engine.repair_post_visual_media(post_key)
    conn.commit()
    print(json.dumps({"stage": "repair_post_visual_media", "result": repair, "state": _state(conn, post_key), "assets": _asset_summary(conn, post_key)}, default=str), flush=True)

    state_after_repair = _state(conn, post_key)
    if not state_after_repair.get("has_video_full"):
        print(json.dumps({"stage": "stop", "reason": "video_full_not_available", "state": state_after_repair}, default=str), flush=True)
        return

    fingerprint = ensure_post_fingerprint(conn, post_key)
    conn.commit()
    print(json.dumps({"stage": "fingerprint", "ok": bool(fingerprint), "state": _state(conn, post_key)}, default=str), flush=True)
    if not fingerprint:
        return

    condensation = ensure_post_condensation(conn, post_key, fingerprint)
    conn.commit()
    print(json.dumps({"stage": "condensation", "ok": bool(condensation), "state": _state(conn, post_key)}, default=str), flush=True)


if __name__ == "__main__":
    main()
