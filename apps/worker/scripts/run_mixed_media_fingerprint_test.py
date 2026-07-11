"""Fingerprint-only mixed-media test lane.

Selects 30 posts per feeder, prefers stored video_full for reel slots, and
never runs post condensation.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[3]
WORKER_DIR = ROOT / "apps" / "worker"
OUT_DIR = WORKER_DIR / "scripts" / "out" / "observation_mixed_media_fingerprints"
ENV_PATHS = (WORKER_DIR / ".env", WORKER_DIR / ".env.vps-production")
POOLER_HOST = "aws-1-ap-south-1.pooler.supabase.com"
DEFAULT_HANDLES = ("anuj.mp4", "lakmeindia", "traya.health")
PROMPT_VERSION = "observation_v1"
CAROUSEL_TOTAL_MEDIA_BYTES = 24 * 1024 * 1024
CAROUSEL_MAX_SLIDES = 20
TIER_RULES = {
    "S": """Use:

* 2-3 progression_beats
* max 3 kept_lines
* max 6 kept_text
* max 6 kept_visuals
* max 3 references
* max 3 construction_notes

Keep it compact. Capture dominant subject, visible text, caption context, and ending.""",
    "M": """Use:

* 3-5 progression_beats
* max 5 kept_lines
* max 10 kept_text
* max 8 kept_visuals
* max 4 references
* max 4 construction_notes

Capture the main sequence and any product/proof/CTA entry.""",
    "L": """Use:

* 5-7 progression_beats
* max 8 kept_lines
* max 14 kept_text
* max 10 kept_visuals
* max 5 references
* max 5 construction_notes

Capture setup, turns, proof, product/CTA timing, and closing frame.""",
    "XL": """Use:

* 6-9 progression_beats
* max 10 kept_lines
* max 18 kept_text
* max 14 kept_visuals
* max 6 references
* max 6 construction_notes

Group repeated sections. Preserve major scene changes, recurring props/UI, proof sequences, and unresolved ending.""",
}
TIER_SIZE_RULES = {
    "S": """SERVER-INJECTED TIER SIZE - S

Tier is assigned from media size: image, reel under 10 seconds, or carousel up to 2 slides.
The JSON tier field must be exactly "S".
Keep the full JSON compact: about 150 words across caption_log, media_log, progression_log, kept_* fields, references, not_present, and uncertainties.
Use 2-3 progression_log items.
Caps: kept_lines <= 3, kept_text <= 6, kept_visuals <= 6, references <= 3.
Prefer short phrases over full sentences in arrays.""",
    "M": """SERVER-INJECTED TIER SIZE - M

Tier is assigned from media size: reel 10-30 seconds, or carousel 3-5 slides.
The JSON tier field must be exactly "M".
Keep the full JSON moderate: about 200-250 words across caption_log, media_log, progression_log, kept_* fields, references, not_present, and uncertainties.
Use 3-5 progression_log items.
Caps: kept_lines <= 5, kept_text <= 10, kept_visuals <= 8, references <= 4.
Preserve the main sequence and any product, proof, CTA, or ending entry.""",
    "L": """SERVER-INJECTED TIER SIZE - L

Tier is assigned from media size: reel 30-60 seconds, carousel 6-8 slides, or carousel with 1 video.
The JSON tier field must be exactly "L".
Keep the full JSON detailed but bounded: about 300-350 words across caption_log, media_log, progression_log, kept_* fields, references, not_present, and uncertainties.
Use 5-7 progression_log items.
Caps: kept_lines <= 8, kept_text <= 14, kept_visuals <= 10, references <= 5.
Preserve setup, major turns, proof/product timing, CTA timing, and closing frame.""",
    "XL": """SERVER-INJECTED TIER SIZE - XL

Tier is assigned from media size: reel over 60 seconds, carousel 9+ slides, or carousel with 2+ videos.
The JSON tier field must be exactly "XL".
Keep the full JSON complete but not bloated: about 400-450 words across caption_log, media_log, progression_log, kept_* fields, references, not_present, and uncertainties.
Use 6-9 progression_log items.
Caps: kept_lines <= 10, kept_text <= 18, kept_visuals <= 14, references <= 6.
Group repeated sections. Preserve major scene changes, recurring props/UI, proof sequences, text-heavy sections, and ending.""",
}

if str(WORKER_DIR) not in sys.path:
    sys.path.insert(0, str(WORKER_DIR))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

FALLBACK_PROMPTS = {
    "reel": """REEL FINGERPRINT
Return valid JSON only. Observe the supplied Instagram Reel neutrally. Do not infer strategy.
Capture caption, visible text, visual sequence, audio behavior, entities, notable details,
uncertainties, and media_confidence. Keep it compact but complete.""",
    "sidecar": """CAROUSEL FINGERPRINT
Return valid JSON only. Observe the supplied Instagram carousel slides neutrally. Do not infer strategy.
Capture slide-by-slide visible text, layout, products/people/objects, sequencing, caption,
notable details, uncertainties, and media_confidence. Keep it compact but complete.""",
    "image": """IMAGE FINGERPRINT
Return valid JSON only. Observe the supplied Instagram image neutrally. Do not infer strategy.
Capture visible text, composition, products/people/objects, caption, notable details,
uncertainties, and media_confidence. Keep it compact but complete.""",
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

    dsn = os.environ.get("POSTGRES_DSN", "")
    if "db.worqtdkvicuhmdgoncru.supabase.co" in dsn:
        dsn = dsn.replace("db.worqtdkvicuhmdgoncru.supabase.co", POOLER_HOST)
        dsn = dsn.replace("postgres:", "postgres.worqtdkvicuhmdgoncru:", 1)
        os.environ["POSTGRES_DSN"] = dsn
    os.environ.setdefault(
        "BRIGHTDATA_SNAPSHOT_TIMEOUT_SECONDS",
        os.getenv("MIXED_FINGERPRINT_BRIGHTDATA_TIMEOUT_SECONDS", "180"),
    )


def _prompt(media_type: str, prompt_dir: Path | None) -> str:
    key = "sidecar" if media_type in {"sidecar", "carousel"} else media_type
    if prompt_dir:
        if prompt_dir.is_file():
            return prompt_dir.read_text()
        names = [f"{key}.md"]
        if key == "sidecar":
            names.append("carousel.md")
        for name in names:
            path = prompt_dir / name
            if path.exists():
                return path.read_text()
    try:
        from app.intelligence_engine_prompts import OBSERVATION_FINGERPRINT_PROMPTS

        prompt_key = "carousel" if key == "sidecar" else key
        prompt = OBSERVATION_FINGERPRINT_PROMPTS.get(prompt_key)
        if prompt:
            return prompt
    except Exception:
        pass
    return FALLBACK_PROMPTS.get(key, FALLBACK_PROMPTS["image"])


def _tier(media_type: str, duration: Any, slide_count: int = 0, carousel_videos: int = 0) -> str:
    if media_type == "image":
        return "S"
    if media_type == "carousel":
        # Locked guards: video count overrides slide count.
        if slide_count >= 9 or carousel_videos >= 2:
            return "XL"
        if carousel_videos == 1:
            return "L"
        if slide_count <= 2:
            return "S"
        if slide_count <= 5:
            return "M"
        return "L"
    try:
        seconds = float(duration)
    except (TypeError, ValueError):
        seconds = 0
    if seconds and seconds < 10:
        return "S"
    if seconds <= 30:
        return "M"
    if seconds <= 60:
        return "L"
    return "XL"


def _tier_self_check() -> None:
    assert _tier("reel", 61) == "XL"
    assert _tier("carousel", None, slide_count=1, carousel_videos=2) == "XL"
    assert _tier("carousel", None, slide_count=2, carousel_videos=1) == "L"


def _carousel_media_mix(conn: Any, post_key: str) -> dict[str, int]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select lower(coalesce(mime_type, '')) as mime_type
            from public.post_media_assets
            where post_key = %s
              and asset_role like 'carousel_%%'
              and status in ('active', 'purge_pending')
              and lower(coalesce(storage_provider, 'supabase')) = 'r2'
              and coalesce(storage_path, '') <> ''
            order by asset_role
            """,
            (post_key,),
        )
        rows = cur.fetchall()
    mix = {"images": 0, "videos": 0, "unknown": 0}
    for row in rows:
        mime_type = str(row.get("mime_type") or "")
        if mime_type.startswith("image/"):
            mix["images"] += 1
        elif mime_type.startswith("video/"):
            mix["videos"] += 1
        else:
            mix["unknown"] += 1
    return mix


def _render_prompt(template: str, *, handle: str, tier: str) -> str:
    tier_rules = TIER_RULES[tier]
    if "assembly_notes" in template:
        tier_rules = tier_rules.replace("construction_notes", "assembly_notes")
    rendered = (
        template
        .replace("@{handle}", f"@{handle}")
        .replace("{tier}", tier)
        .replace("{tier_rules}", tier_rules)
    )
    if "media_logger_v4" in rendered:
        tier_size = TIER_SIZE_RULES[tier]
        depth_start = rendered.find("DEPTH\n")
        field_rules_start = rendered.find("FIELD RULES")
        if depth_start >= 0 and field_rules_start > depth_start:
            rendered = (
                rendered[:depth_start].rstrip()
                + "\n\n"
                + tier_size
                + "\n\n"
                + rendered[field_rules_start:].lstrip()
            )
        else:
            rendered, replaced = re.subn(
                r"\nSERVER-INJECTED TIER SIZE\s+[—-]\s+(?:S|M|L|XL)\n.*?\nFIELD RULES",
                f"\n{tier_size}\n\nFIELD RULES",
                rendered,
                count=1,
                flags=re.S,
            )
            if replaced == 0 and field_rules_start >= 0:
                rendered = (
                    rendered[:field_rules_start].rstrip()
                    + "\n\n"
                    + tier_size
                    + "\n\n"
                    + rendered[field_rules_start:].lstrip()
                )
            elif replaced == 0:
                rendered = f"{rendered.rstrip()}\n\n{tier_size}"
    return rendered


def _select_posts(conn: Any, handle: str, target: int) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            with latest as (
              select p.*, row_number() over (order by p.posted_at desc nulls last, p.updated_at desc nulls last) rn
              from public.posts p
              join public.feeders f on f.id = p.feeder_id
              where lower(f.handle) = lower(%s)
            ),
            non_reels as (
              select *
              from latest
              where rn <= %s
                and lower(coalesce(media_type, '')) not in ('reel', 'video')
            ),
            reel_slots as (
              select greatest(0, %s - count(*))::int as n
              from non_reels
            ),
            ready_reels as (
              select p.*
              from public.posts p
              join public.feeders f on f.id = p.feeder_id
              where lower(f.handle) = lower(%s)
                and lower(coalesce(p.media_type, '')) in ('reel', 'video')
                and exists (
                  select 1
                  from public.post_media_assets a
                  where a.post_key = p.post_key
                    and a.asset_role = 'video_full'
                    and a.status in ('active', 'purge_pending')
                    and coalesce(a.storage_path, a.public_url, '') <> ''
                )
              order by p.posted_at desc nulls last, p.updated_at desc nulls last
              limit (select n from reel_slots)
            )
            select post_key, post_url, posted_at, lower(coalesce(media_type, '')) as media_type,
                   caption, duration_seconds, thumbnail_url, video_url, carousel_urls
            from non_reels
            union all
            select post_key, post_url, posted_at, lower(coalesce(media_type, '')) as media_type,
                   caption, duration_seconds, thumbnail_url, video_url, carousel_urls
            from ready_reels
            order by media_type, posted_at desc nulls last
            """,
            (handle, target, target, handle),
        )
        return [dict(row) for row in cur.fetchall()]


def _media_refs_from_brightdata(rows: list[dict[str, Any]], *, force: bool = False) -> dict[str, dict[str, Any]]:
    from app.brightdata import run_post_urls

    needs_refresh = []
    for row in rows:
        if force:
            needs_refresh.append(row)
            continue
        media_type = str(row.get("media_type") or "").strip().lower()
        cached_carousel = row.get("carousel_urls")
        has_cached_carousel = isinstance(cached_carousel, list) and any(str(value or "").strip() for value in cached_carousel)
        has_cached_image = bool(str(row.get("thumbnail_url") or "").strip())
        if media_type in {"sidecar", "carousel"} and has_cached_carousel:
            continue
        if media_type == "image" and has_cached_image:
            continue
        needs_refresh.append(row)

    post_urls = [str(row.get("post_url") or "").strip() for row in needs_refresh if row.get("post_url")]
    if not post_urls:
        return {}
    print(json.dumps({"stage": "brightdata_refresh", "posts": len(post_urls)}), flush=True)
    refreshed = run_post_urls(post_urls)
    by_url: dict[str, dict[str, Any]] = {}
    for item in refreshed:
        url = str(item.get("url") or "").strip()
        if url:
            by_url[url.rstrip("/")] = item
    return by_url


def _suffix_for_mime(mime_type: str) -> str:
    if "png" in mime_type:
        return ".png"
    if "webp" in mime_type:
        return ".webp"
    if "video" in mime_type:
        return ".mp4"
    return ".jpg"


def _ffmpeg_bytes(payload: bytes, mime_type: str, out_suffix: str, args: list[str], timeout: int = 90) -> bytes | None:
    try:
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / f"in{_suffix_for_mime(mime_type)}"
            dst = Path(tmp) / f"out{out_suffix}"
            src.write_bytes(payload)
            proc = subprocess.run(
                ["ffmpeg", "-y", "-i", str(src), *args, str(dst)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=timeout,
                check=False,
            )
            if proc.returncode == 0 and dst.exists() and dst.stat().st_size > 100:
                return dst.read_bytes()
    except Exception:
        return None
    return None


def _fit_image(image_bytes: bytes, mime_type: str, max_bytes: int = 2 * 1024 * 1024) -> tuple[bytes, str]:
    if len(image_bytes) <= max_bytes and mime_type.startswith("image/"):
        return image_bytes, mime_type
    for width, quality in ((1600, 4), (1280, 6), (960, 8), (720, 10), (540, 14), (360, 18), (320, 24)):
        out = _ffmpeg_bytes(
            image_bytes,
            mime_type,
            ".jpg",
            ["-vf", f"scale='min({width},iw)':-2", "-frames:v", "1", "-q:v", str(quality)],
            timeout=45,
        )
        if out and len(out) <= max_bytes:
            return out, "image/jpeg"
    return image_bytes, mime_type if mime_type.startswith("image/") else "image/jpeg"


def _image_part(url: str, headers: dict[str, str] | None = None, *, max_bytes: int = 2 * 1024 * 1024) -> tuple[dict[str, Any] | None, str | None]:
    from app.fingerprint_intelligence import _data_url, _fetch_bytes

    fetched = _fetch_bytes(url, timeout=45, max_bytes=30 * 1024 * 1024, headers=headers)
    if not fetched:
        return None, None
    body, mime_type = fetched
    if not mime_type.startswith("image/"):
        mime_type = "image/jpeg"
    body, mime_type = _fit_image(body, mime_type, max_bytes=max_bytes)
    if len(body) > max_bytes:
        return None, None
    return {"type": "image_url", "image_url": {"url": _data_url(mime_type, body)}}, mime_type


def _fit_carousel_video(video_bytes: bytes, max_bytes: int = 2 * 1024 * 1024) -> bytes | None:
    if len(video_bytes) <= max_bytes:
        return video_bytes
    for seconds, width, fps, crf in (
        (30, 720, 12, 32),
        (20, 540, 10, 35),
        (15, 432, 8, 38),
        (10, 360, 6, 40),
    ):
        out = _ffmpeg_bytes(
            video_bytes,
            "video/mp4",
            ".mp4",
            [
                "-t",
                str(seconds),
                "-vf",
                f"fps={fps},scale='min({width},iw)':-2",
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                str(crf),
                "-an",
                "-movflags",
                "+faststart",
            ],
            timeout=90,
        )
        if out and len(out) <= max_bytes:
            return out
    return None


def _video_frame_part(video_bytes: bytes, max_bytes: int) -> dict[str, Any] | None:
    from app.fingerprint_intelligence import _data_url

    frame = _ffmpeg_bytes(
        video_bytes,
        "video/mp4",
        ".jpg",
        ["-vf", "scale='min(720,iw)':-2", "-frames:v", "1", "-q:v", "8"],
        timeout=45,
    )
    if not frame:
        return None
    frame, mime_type = _fit_image(frame, "image/jpeg", max_bytes=max_bytes)
    if len(frame) > max_bytes:
        return None
    return {"type": "image_url", "image_url": {"url": _data_url(mime_type, frame)}}


def _fresh_media_refs(row: dict[str, Any], item: dict[str, Any]) -> tuple[str | None, str | None, list[str]]:
    media_type = str(row.get("media_type") or "").strip().lower()
    thumbnail_url = (
        item.get("thumbnailUrl")
        or item.get("thumbnailSrc")
        or item.get("displayUrl")
        or item.get("imageUrl")
        or row.get("thumbnail_url")
    )
    video_url = item.get("videoUrl") or item.get("video_url") or row.get("video_url")
    carousel_urls: list[str] = []
    if media_type in {"sidecar", "carousel"}:
        carousel = item.get("childPosts") or item.get("sidecarImages") or item.get("carouselMedia") or row.get("carousel_urls") or []
        if isinstance(carousel, list):
            for child in carousel:
                value = child.get("displayUrl") or child.get("imageUrl") or child.get("videoUrl") if isinstance(child, dict) else child
                if value and str(value) not in carousel_urls:
                    carousel_urls.append(str(value))
        if not carousel_urls and thumbnail_url:
            carousel_urls.append(str(thumbnail_url))
    else:
        video_url = None

    def _clean(value: Any) -> str | None:
        text = str(value or "").strip()
        return text or None

    return _clean(thumbnail_url), _clean(video_url), carousel_urls


def _stage_and_capture_media(rows: list[dict[str, Any]], refreshed: dict[str, dict[str, Any]]) -> dict[str, Any]:
    from app.pure_engine import PureEngine

    engine = PureEngine()
    post_keys: list[str] = []
    try:
        with engine.conn.transaction():
            for row in rows:
                item = refreshed.get(str(row.get("post_url") or "").rstrip("/"), {})
                thumbnail_url, video_url, carousel_urls = _fresh_media_refs(row, item)
                post_key = str(row["post_key"]).strip().lower()
                if not post_key or (not thumbnail_url and not carousel_urls):
                    continue
                engine._refresh_post_media(post_key, thumbnail_url, video_url, carousel_urls)
                engine._stage_post_media_assets(
                    post_key,
                    row.get("posted_at"),
                    thumbnail_url,
                    video_url,
                    carousel_urls,
                    thumbnail_retention_days=120 if thumbnail_url else None,
                    carousel_retention_days=120 if carousel_urls else None,
                    stage_preview=False,
                )
                post_keys.append(post_key)
        result = engine._capture_post_media_assets_for_post_keys(
            post_keys,
            include_all_roles=True,
            stale_minutes=1,
        )
        return {"staged_posts": len(set(post_keys)), **result}
    finally:
        engine.close()


def _stored_image_parts(conn: Any, post_key: str, media_type: str) -> list[dict[str, Any]]:
    from app.fingerprint_intelligence import _fetch_bytes, _media_fetch_ref, _openrouter_video_part, _trim_video

    if media_type in {"sidecar", "carousel"}:
        query = """
            select asset_role, public_url, storage_path, storage_provider, storage_bucket, mime_type
            from public.post_media_assets
            where post_key = %s
              and asset_role like 'carousel_%%'
              and status in ('active', 'purge_pending')
              and coalesce(storage_path, public_url, '') <> ''
            order by asset_role
        """
        params = (post_key,)
    else:
        query = """
            select asset_role, public_url, storage_path, storage_provider, storage_bucket, mime_type
            from public.post_media_assets
            where post_key = %s
              and asset_role in ('display', 'thumbnail')
              and status in ('active', 'purge_pending')
              and coalesce(storage_path, public_url, '') <> ''
            order by case asset_role when 'display' then 0 else 1 end, updated_at desc nulls last
            limit 1
        """
        params = (post_key,)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, params)
        assets = cur.fetchall()

    parts: list[dict[str, Any]] = []
    slide_cap = max(768 * 1024, min(2 * 1024 * 1024, CAROUSEL_TOTAL_MEDIA_BYTES // max(1, min(len(assets), CAROUSEL_MAX_SLIDES))))
    for asset in assets[:CAROUSEL_MAX_SLIDES]:
        url, headers = _media_fetch_ref(asset)
        if not url:
            continue
        mime_type = str(asset.get("mime_type") or "").strip().lower()
        if mime_type.startswith("video/"):
            fetched = _fetch_bytes(url, timeout=60, max_bytes=50 * 1024 * 1024, headers=headers)
            trimmed = _trim_video(fetched[0]) if fetched else None
            fitted = _fit_carousel_video(trimmed, max_bytes=slide_cap) if trimmed else None
            part = (
                _openrouter_video_part(
                    fitted,
                    fetched[1] if fetched and fetched[1].startswith("video/") else "video/mp4",
                )
                if fitted
                else None
            )
            if part is None and trimmed:
                part = _video_frame_part(trimmed, slide_cap)
        else:
            part, _ = _image_part(url, headers=headers, max_bytes=slide_cap)
        if part:
            parts.append(part)
    return parts


def _media_parts(
    conn: Any,
    row: dict[str, Any],
    refreshed: dict[str, dict[str, Any]],
    *,
    require_stored_media: bool = False,
) -> tuple[list[dict[str, Any]], str]:
    from app.fingerprint_intelligence import _fingerprint_media_parts, _post_media

    media_type = str(row.get("media_type") or "").lower()
    if media_type in {"reel", "video"}:
        post = _post_media(conn, str(row["post_key"]))
        if not post:
            return [], "missing_video_full"
        parts, _, _ = _fingerprint_media_parts(post, "openrouter")
        return parts, "video_full" if parts else "video_full_fetch_failed"

    stored_parts = _stored_image_parts(conn, str(row["post_key"]), media_type)
    if stored_parts:
        return stored_parts, "stored_media_assets"
    if require_stored_media:
        return [], "missing_stored_media_assets"

    item = refreshed.get(str(row.get("post_url") or "").rstrip("/"), {})
    urls: list[str] = []
    if media_type in {"sidecar", "carousel"}:
        _, _, urls = _fresh_media_refs(row, item)
    else:
        url, _, _ = _fresh_media_refs(row, item)
        urls.append(str(url or ""))

    parts = []
    for url in [u for u in urls if u][:12]:
        part, _ = _image_part(url)
        if part:
            parts.append(part)
    source = "cached_images" if not item else "brightdata_or_cached_images"
    return parts, source if parts else "image_fetch_failed"


def _call_openrouter(system: str, user_text: str, media_parts: list[dict[str, Any]], model: str) -> tuple[str, dict[str, Any] | None]:
    resp = requests.post(
        f"{os.environ.get('OPENROUTER_BASE_URL', 'https://openrouter.ai/api/v1').rstrip('/')}/chat/completions",
        headers={"Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": [*media_parts, {"type": "text", "text": user_text}]},
            ],
            "temperature": 0.1,
            "max_tokens": int(os.getenv("MIXED_FINGERPRINT_MAX_TOKENS", "6000")),
            "response_format": {"type": "json_object"},
        },
        timeout=240,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"OpenRouter {resp.status_code}: {resp.text[:800]}")
    raw = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
    try:
        return raw, json.loads(raw)
    except Exception:
        return raw, None


def run(args: argparse.Namespace) -> None:
    _load_env()
    handles = tuple(h.strip() for h in args.handles.split(",") if h.strip())
    prompt_dir = Path(args.prompt_dir) if args.prompt_dir else None
    model = args.model
    out_dir = Path(args.out_dir) if args.out_dir else OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {"model": model, "prompt_version": args.prompt_version, "handles": {}}
    with psycopg.connect(os.environ["POSTGRES_DSN"], row_factory=dict_row, connect_timeout=20, autocommit=True) as conn:
        for handle in handles:
            rows = _select_posts(conn, handle, args.target)
            if args.post_keys:
                wanted = {value.strip().lower() for value in args.post_keys.split(",") if value.strip()}
                rows = [row for row in rows if str(row.get("post_key") or "").strip().lower() in wanted]
            if args.media_types:
                allowed = {value.strip().lower() for value in args.media_types.split(",") if value.strip()}
                rows = [row for row in rows if str(row.get("media_type") or "").strip().lower() in allowed]
            if args.one_each:
                picked = []
                for media_type in ("reel", "sidecar", "image"):
                    picked.extend([row for row in rows if row.get("media_type") == media_type][:1])
                rows = picked
            non_reels = [r for r in rows if str(r.get("media_type") or "") not in {"reel", "video"}]
            refreshed = (
                {}
                if args.require_stored_media
                else _media_refs_from_brightdata(non_reels, force=args.refresh_media) if args.run and non_reels else {}
            )
            if args.run and args.capture_media and non_reels:
                capture_result = _stage_and_capture_media(non_reels, refreshed)
                print(json.dumps({"stage": "media_capture", "handle": handle, **capture_result}), flush=True)
            handle_out = out_dir / handle.replace(".", "_")
            handle_out.mkdir(parents=True, exist_ok=True)
            results = []
            for index, row in enumerate(rows, start=1):
                post_key = str(row["post_key"])
                media_type = str(row.get("media_type") or "unknown")
                prompt_media_type = "carousel" if media_type in {"sidecar", "carousel"} else media_type
                status = {"post_key": post_key, "media_type": media_type}
                if not args.run:
                    results.append({**status, "status": "selected"})
                    continue
                print(json.dumps({"stage": "post_start", "handle": handle, **status}), flush=True)
                parts, source = _media_parts(conn, row, refreshed, require_stored_media=args.require_stored_media)
                if not parts:
                    results.append({**status, "status": "media_failed", "source": source})
                    print(json.dumps({"stage": "post_done", "handle": handle, **results[-1]}), flush=True)
                    continue
                carousel_mix = _carousel_media_mix(conn, post_key) if prompt_media_type == "carousel" else None
                inferred_tier = _tier(
                    prompt_media_type,
                    row.get("duration_seconds"),
                    slide_count=len(parts) if prompt_media_type == "carousel" else 0,
                    carousel_videos=(carousel_mix or {}).get("videos", 0),
                )
                base = handle_out / f"{index:02d}_{post_key.replace('/', '_').replace('#', '_')}"
                if base.with_suffix(".json").exists() and not args.force:
                    results.append({**status, "status": "skipped_complete", "json": str(base.with_suffix(".json"))})
                    print(json.dumps({"stage": "post_done", "handle": handle, **results[-1]}), flush=True)
                    continue
                media_count_line = ""
                if prompt_media_type == "image":
                    media_count_line = "IMAGE_COUNT: 1\nMEDIA_NOTE: This is a single-image post, not a carousel."
                elif prompt_media_type == "carousel":
                    media_count_line = "\n".join([
                        f"SLIDE_COUNT: {len(parts)}",
                        f"CAROUSEL_MEDIA_MIX: {json.dumps(carousel_mix or {'images': 0, 'videos': 0, 'unknown': len(parts)}, sort_keys=True)}",
                        "MEDIA_NOTE: This is a carousel/sidecar post; each media part is a slide in order and may be image or video.",
                    ])
                elif prompt_media_type == "reel":
                    duration = row.get("duration_seconds")
                    truncated = bool(duration is not None and float(duration) > 120)
                    media_count_line = "\n".join([
                        "MEDIA_NOTE: This is a reel/video post.",
                        "VIDEO_SAMPLE_SECONDS: 120",
                        f"DURATION_SECONDS: {duration or ''}",
                        f"MEDIA_TRUNCATED: {str(truncated).lower()}",
                    ])
                user_text = "\n".join([
                    f"HANDLE: @{handle}",
                    f"POST_ALIAS: {post_key}",
                    f"POST_KEY: {post_key}",
                    f"MEDIA_TYPE: {prompt_media_type}",
                    f"SERVER_TIER: {inferred_tier}",
                    media_count_line,
                    f"POSTED_AT: {row.get('posted_at') or ''}",
                    f"CAPTION: {str(row.get('caption') or '')}",
                ])
                system_prompt = _render_prompt(_prompt(media_type, prompt_dir), handle=handle, tier=inferred_tier)
                raw, parsed = _call_openrouter(system_prompt, user_text, parts, model)
                base.with_suffix(".raw.txt").write_text(raw or "")
                if parsed is None:
                    results.append({**status, "status": "parse_failed", "raw": str(base.with_suffix(".raw.txt"))})
                    print(json.dumps({"stage": "post_done", "handle": handle, **results[-1]}), flush=True)
                    continue
                if isinstance(parsed, list):
                    parsed = parsed[0] if len(parsed) == 1 and isinstance(parsed[0], dict) else {"items": parsed}
                if not isinstance(parsed, dict):
                    results.append({**status, "status": "parse_failed", "raw": str(base.with_suffix(".raw.txt"))})
                    print(json.dumps({"stage": "post_done", "handle": handle, **results[-1]}), flush=True)
                    continue
                parsed["post_key"] = post_key
                parsed["media_type"] = prompt_media_type
                parsed["tier"] = inferred_tier
                base.with_suffix(".json").write_text(json.dumps(parsed, ensure_ascii=False, indent=2, default=str))
                results.append({**status, "status": "complete", "json": str(base.with_suffix(".json")), "source": source, "tier": inferred_tier})
                print(json.dumps({"stage": "post_done", "handle": handle, **results[-1]}), flush=True)
            summary["handles"][handle] = {
                "selected": len(rows),
                "by_media": {m: sum(1 for r in rows if r.get("media_type") == m) for m in sorted({r.get("media_type") for r in rows})},
                "results": results,
            }
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


def main() -> None:
    _tier_self_check()
    parser = argparse.ArgumentParser()
    parser.add_argument("--handles", default=",".join(DEFAULT_HANDLES))
    parser.add_argument("--target", type=int, default=30)
    parser.add_argument("--model", default=os.getenv("MIXED_FINGERPRINT_MODEL", "google/gemini-3-flash-preview"))
    parser.add_argument("--prompt-version", default=PROMPT_VERSION)
    parser.add_argument("--prompt-dir", default=None, help="Directory with reel.md, carousel.md/sidecar.md, image.md")
    parser.add_argument("--out-dir", default=None, help="Output directory. Defaults to observation_mixed_media_fingerprints.")
    parser.add_argument("--media-types", default="", help="Comma-separated DB media types to run, e.g. image or reel,sidecar")
    parser.add_argument("--post-keys", default="", help="Comma-separated post_keys to run")
    parser.add_argument("--refresh-media", action="store_true", help="Force Bright Data refresh for selected non-reels")
    parser.add_argument("--capture-media", action="store_true", help="Stage and capture selected non-reel media before fingerprinting")
    parser.add_argument("--require-stored-media", action="store_true", help="Only fingerprint non-reels from stored media assets")
    parser.add_argument("--one-each", action="store_true", help="Smoke test: run at most one reel, sidecar, and image per feeder")
    parser.add_argument("--force", action="store_true", help="Re-run even if local JSON output exists")
    parser.add_argument("--run", action="store_true", help="Actually call Bright Data for non-reels and OpenRouter for fingerprints")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
