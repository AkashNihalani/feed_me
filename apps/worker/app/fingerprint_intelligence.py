from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
import tempfile
import time
from datetime import datetime
from typing import Any

import requests
from psycopg.rows import dict_row

from .config import (
    FEEDER_FINGERPRINT_MODEL,
    FEEDER_INTELLIGENCE_ENABLED,
    FEEDER_INTELLIGENCE_PROVIDER,
    GEMINI_API_KEY,
    MEDIA_PUBLIC_BASE_URL,
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
)
from .feeder_prompts import (
    FINGERPRINT_EXTRACTION_SYSTEM_V8,
    FINGERPRINT_PROMPT_VERSION,
    FINGERPRINT_SAMPLING_POLICY_VERSION,
)

_GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
_GEMINI_UPLOAD_URL = "https://generativelanguage.googleapis.com/upload/v1beta/files"
_GEMINI_FILE_URL = "https://generativelanguage.googleapis.com/v1beta/files/{name}"
_OPENROUTER_CHAT_URL = "/chat/completions"
_DEFAULT_OPENROUTER_MODEL = "google/gemini-3-flash-preview"
_DEFAULT_GEMINI_MODEL = "gemini-3-flash-preview"
_VIDEO_UPLOAD_MAX_BYTES = 50 * 1024 * 1024
_VIDEO_INLINE_MAX_BYTES = 20 * 1024 * 1024
_VIDEO_SAMPLE_SECONDS = 120


def is_enabled() -> bool:
    return FEEDER_INTELLIGENCE_ENABLED


def _provider() -> str | None:
    preferred = (FEEDER_INTELLIGENCE_PROVIDER or "auto").strip().lower()
    if preferred == "openrouter":
        return "openrouter" if OPENROUTER_API_KEY else None
    if preferred == "google":
        return "google" if GEMINI_API_KEY else None
    if OPENROUTER_API_KEY:
        return "openrouter"
    if GEMINI_API_KEY:
        return "google"
    return None


def _model(provider: str, model_override: str | None = None) -> str:
    explicit = (model_override or FEEDER_FINGERPRINT_MODEL or "").strip()
    if explicit:
        if provider == "google" and explicit.startswith("google/"):
            return explicit.split("/", 1)[1]
        return explicit
    return _DEFAULT_OPENROUTER_MODEL if provider == "openrouter" else _DEFAULT_GEMINI_MODEL


def current_model_version(*, model_override: str | None = None) -> str:
    provider = _provider() or "disabled"
    model = _model(provider, model_override) if provider != "disabled" else "none"
    return f"{provider}:{model}:{FINGERPRINT_PROMPT_VERSION}"


def _sha(value: Any) -> str:
    if value is None:
        value = ""
    if isinstance(value, bytes):
        payload = value
    elif isinstance(value, str):
        payload = value.encode("utf-8", errors="ignore")
    else:
        payload = json.dumps(value, sort_keys=True, default=str).encode("utf-8", errors="ignore")
    return hashlib.sha256(payload).hexdigest()


def _data_url(mime_type: str, payload: bytes) -> str:
    return f"data:{mime_type};base64,{base64.b64encode(payload).decode('ascii')}"


def _fetch_bytes(url: str | None, *, timeout: int = 30, max_bytes: int = 0) -> tuple[bytes, str] | None:
    if not url or not str(url).startswith(("http://", "https://")):
        return None
    try:
        resp = requests.get(
            str(url),
            timeout=timeout,
            headers={"User-Agent": "FeedMe/1.0", "Referer": "https://www.instagram.com/"},
            stream=bool(max_bytes),
        )
        resp.raise_for_status()
        content_type = (resp.headers.get("content-type") or "application/octet-stream").split(";", 1)[0].strip()
        if max_bytes:
            chunks: list[bytes] = []
            total = 0
            for chunk in resp.iter_content(chunk_size=256 * 1024):
                total += len(chunk)
                if total > max_bytes:
                    resp.close()
                    return None
                chunks.append(chunk)
            body = b"".join(chunks)
        else:
            body = resp.content
        if len(body) < 100:
            return None
        return body, content_type
    except Exception:
        return None


def _trim_video(video_bytes: bytes) -> bytes:
    tmp_in = None
    tmp_out = None
    try:
        tmp_in = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
        tmp_in.write(video_bytes)
        tmp_in.close()
        tmp_out = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
        tmp_out.close()
        cmd = ["ffmpeg", "-y", "-i", tmp_in.name, "-t", str(_VIDEO_SAMPLE_SECONDS), "-c", "copy", tmp_out.name]
        result = subprocess.run(cmd, capture_output=True, timeout=45)
        if result.returncode == 0:
            with open(tmp_out.name, "rb") as handle:
                trimmed = handle.read()
            if 100 <= len(trimmed) <= len(video_bytes):
                return trimmed
    except Exception:
        return video_bytes
    finally:
        for tmp in (tmp_in, tmp_out):
            if tmp:
                try:
                    os.unlink(tmp.name)
                except Exception:
                    pass
    return video_bytes


def _openrouter_video_part(payload: bytes, mime_type: str) -> dict[str, Any]:
    return {"type": "video_url", "video_url": {"url": _data_url(mime_type, payload)}}


def _upload_gemini_video(video_bytes: bytes, mime_type: str) -> str | None:
    try:
        resp = requests.post(
            _GEMINI_UPLOAD_URL,
            params={"key": GEMINI_API_KEY},
            headers={
                "X-Goog-Upload-Protocol": "resumable",
                "X-Goog-Upload-Command": "start",
                "X-Goog-Upload-Header-Content-Length": str(len(video_bytes)),
                "X-Goog-Upload-Header-Content-Type": mime_type,
                "Content-Type": "application/json",
            },
            json={"file": {"display_name": "feedme_reel_fingerprint"}},
            timeout=15,
        )
        resp.raise_for_status()
        upload_url = resp.headers.get("X-Goog-Upload-URL")
        if not upload_url:
            return None
        resp2 = requests.post(
            upload_url,
            headers={
                "X-Goog-Upload-Command": "upload, finalize",
                "X-Goog-Upload-Offset": "0",
                "Content-Type": mime_type,
            },
            data=video_bytes,
            timeout=120,
        )
        resp2.raise_for_status()
        file_info = resp2.json().get("file", {})
        file_name = file_info.get("name")
        file_uri = file_info.get("uri")
        if not file_name or not file_uri:
            return None
        for _ in range(30):
            check = requests.get(_GEMINI_FILE_URL.format(name=file_name), params={"key": GEMINI_API_KEY}, timeout=10)
            if check.status_code == 200:
                state = check.json().get("state", "")
                if state == "ACTIVE":
                    return file_uri
                if state == "FAILED":
                    return None
            time.sleep(2)
        return None
    except Exception:
        return None


def _gemini_video_part(payload: bytes, mime_type: str) -> dict[str, Any] | None:
    if len(payload) <= _VIDEO_INLINE_MAX_BYTES:
        return {"inline_data": {"mime_type": mime_type, "data": base64.b64encode(payload).decode("ascii")}}
    if len(payload) <= _VIDEO_UPLOAD_MAX_BYTES:
        uri = _upload_gemini_video(payload, mime_type)
        if uri:
            return {"file_data": {"mime_type": mime_type, "file_uri": uri}}
    return None


def _extract_text(payload: dict[str, Any], provider: str) -> str:
    if provider == "openrouter":
        content = payload.get("choices", [{}])[0].get("message", {}).get("content", "")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            return "\n".join(str(part.get("text") or "") for part in content if isinstance(part, dict)).strip()
        return ""
    return payload.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")


def _json_from_text(text: str) -> dict[str, Any] | None:
    raw = (text or "").strip()
    if raw.startswith("```"):
        lines = raw.splitlines()[1:]
        while lines and lines[-1].strip() == "```":
            lines.pop()
        raw = "\n".join(lines).strip()
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass
    start = raw.find("{")
    if start < 0:
        return None
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
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                try:
                    parsed = json.loads(raw[start:index + 1])
                    return parsed if isinstance(parsed, dict) else None
                except Exception:
                    return None
    return None


def _call_model(
    user_text: str,
    media_parts: list[dict[str, Any]],
    *,
    max_tokens: int = 3600,
    model_override: str | None = None,
) -> dict[str, Any] | None:
    provider = _provider()
    if not provider:
        return None
    model = _model(provider, model_override)
    try:
        if provider == "openrouter":
            payload = {
                "model": model,
                "messages": [
                    {"role": "system", "content": FINGERPRINT_EXTRACTION_SYSTEM_V8},
                    {"role": "user", "content": [*media_parts, {"type": "text", "text": user_text}]},
                ],
                "temperature": 0.1,
                "max_tokens": max_tokens,
                "response_format": {"type": "json_object"},
            }
            url = f"{OPENROUTER_BASE_URL.rstrip('/')}{_OPENROUTER_CHAT_URL}"
            headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"}
            resp = requests.post(url, headers=headers, json=payload, timeout=180)
            if resp.status_code >= 400:
                payload.pop("response_format", None)
                resp = requests.post(url, headers=headers, json=payload, timeout=180)
        else:
            resp = requests.post(
                _GEMINI_API_URL.format(model=model),
                params={"key": GEMINI_API_KEY},
                json={
                    "contents": [{"parts": [{"text": FINGERPRINT_EXTRACTION_SYSTEM_V8}, *media_parts, {"text": user_text}]}],
                    "generationConfig": {
                        "temperature": 0.1,
                        "maxOutputTokens": max_tokens,
                        "responseMimeType": "application/json",
                    },
                },
                timeout=180,
            )
        resp.raise_for_status()
        return _json_from_text(_extract_text(resp.json(), provider))
    except Exception as exc:
        print(f"[fingerprint] model call failed: {exc}")
        return None


def _media_public_url(row: dict[str, Any]) -> str | None:
    public_url = str(row.get("public_url") or "").strip()
    if public_url:
        return public_url
    base = (MEDIA_PUBLIC_BASE_URL or "").rstrip("/")
    path = str(row.get("storage_path") or "").strip().lstrip("/")
    if base and path:
        return f"{base}/{path}"
    return None


def _post_media(conn: Any, post_key: str) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select p.post_key, p.caption, lower(coalesce(p.media_type, '')) as media_type,
                   p.duration_seconds, p.duration_bucket, p.posted_at
            from public.posts p
            where p.post_key = %s
              and lower(coalesce(p.media_type, '')) in ('reel', 'video')
            limit 1
            """,
            (post_key,),
        )
        post = cur.fetchone()
        if not post:
            return None
        cur.execute(
            """
            select asset_role, public_url, storage_path
            from public.post_media_assets
            where post_key = %s
              and asset_role = 'video_full'
              and status in ('active', 'purge_pending')
              and coalesce(storage_path, public_url, '') <> ''
            order by updated_at desc nulls last
            limit 1
            """,
            (post_key,),
        )
        assets = cur.fetchall()

    video_url: str | None = None
    video_asset_role: str | None = None
    for asset in assets:
        url = _media_public_url(asset)
        if not url:
            continue
        video_url = url
        video_asset_role = str(asset.get("asset_role") or "").strip().lower()
        break

    return {**post, "video_url": video_url, "_video_asset_role": video_asset_role}


def _fingerprint_media_parts(post: dict[str, Any], provider: str) -> tuple[list[dict[str, Any]], str, str]:
    if str(post.get("media_type") or "").lower() not in {"reel", "video"}:
        return [], _sha(""), "low"
    source_role = str(post.get("_video_asset_role") or "").strip().lower()
    if source_role != "video_full":
        return [], _sha(""), "low"
    video_data = _fetch_bytes(post.get("video_url"), timeout=60, max_bytes=_VIDEO_UPLOAD_MAX_BYTES)
    if not video_data:
        return [], _sha(""), "low"
    video_bytes, mime_type = video_data
    if not mime_type.startswith("video/"):
        mime_type = "video/mp4"
    video_bytes = _trim_video(video_bytes)
    part = _openrouter_video_part(video_bytes, mime_type) if provider == "openrouter" else _gemini_video_part(video_bytes, mime_type)
    if not part:
        return [], _sha(""), "low"
    return [part], _sha(f"{source_role}:{_sha(video_bytes)}"), "high"


def ensure_post_fingerprint(conn: Any, post_key: str) -> dict[str, Any] | None:
    provider = _provider()
    if not provider or not is_enabled():
        return None
    post = _post_media(conn, post_key)
    if not post:
        return None

    caption = str(post.get("caption") or "")
    media_parts, media_hash, _confidence = _fingerprint_media_parts(post, provider)
    if not media_parts or media_hash == _sha(""):
        print(f"[fingerprint] skipped post_key={post_key}: missing active video_full media")
        return None

    caption_hash = _sha(caption)
    model_version = current_model_version()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select fingerprint
            from public.post_fingerprints
            where post_key = %s
              and media_source_hash = %s
              and caption_hash = %s
              and sampling_policy_version = %s
              and model_version = %s
            """,
            (post_key, media_hash, caption_hash, FINGERPRINT_SAMPLING_POLICY_VERSION, model_version),
        )
        existing = cur.fetchone()
        if existing and isinstance(existing.get("fingerprint"), dict):
            return existing["fingerprint"]

    try:
        conn.commit()
    except Exception:
        pass

    user_text = "\n".join([
        f"POST_KEY: {post_key}",
        f"MEDIA_TYPE: reel",
        f"DURATION_SECONDS: {post.get('duration_seconds') or ''}",
        f"DURATION_BUCKET: {post.get('duration_bucket') or ''}",
        f"POSTED_AT: {post.get('posted_at') or ''}",
        f"CAPTION: {caption[:6000] or '(no caption)'}",
    ])
    fingerprint = _call_model(user_text, media_parts)
    if not fingerprint:
        return None

    # Store the observation fingerprint directly. Earlier experiments nested
    # analysis under pool_clustering_fields; the feeder pipeline now keeps
    # interpretation in post_breakdowns instead.
    if isinstance(fingerprint.get("pool_clustering_fields"), dict):
        fingerprint = fingerprint["pool_clustering_fields"]
    fingerprint["post_key"] = str(fingerprint.get("post_key") or post_key)
    fingerprint["media_type"] = "reel"
    fingerprint["media_confidence"] = "high"
    fingerprint["fingerprint_status"] = {
        "media_confidence": fingerprint["media_confidence"],
        "model_version": model_version,
        "generated_at": datetime.utcnow().isoformat(timespec="microseconds") + "+00:00",
    }

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.post_fingerprints (
              post_key, fingerprint, media_source_hash, caption_hash,
              sampling_policy_version, model_version, media_confidence,
              generated_at, updated_at
            )
            values (%s, %s::jsonb, %s, %s, %s, %s, %s, now(), now())
            on conflict (post_key) do update set
              fingerprint = excluded.fingerprint,
              media_source_hash = excluded.media_source_hash,
              caption_hash = excluded.caption_hash,
              sampling_policy_version = excluded.sampling_policy_version,
              model_version = excluded.model_version,
              media_confidence = excluded.media_confidence,
              updated_at = now()
            """,
            (
                post_key,
                json.dumps(fingerprint),
                media_hash,
                caption_hash,
                FINGERPRINT_SAMPLING_POLICY_VERSION,
                model_version,
                fingerprint["media_confidence"],
            ),
        )
    conn.commit()
    return fingerprint


def _candidate_post_keys(conn: Any, *, feeder_id: int | None, limit: int, days: int) -> list[str]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            with reel_d7 as (
              select
                p.post_key,
                p.feeder_id,
                p.posted_at,
                least(
                  coalesce(pm.percentile_performance_exact, 101),
                  coalesce(pm.percentile_performance, 101)
                ) as d7_percentile,
                row_number() over (partition by p.feeder_id order by p.posted_at desc nulls last) as recent_rank,
                pf_high.post_key as high_fingerprint_post_key
              from public.posts p
              join public.post_metrics pm
                on pm.post_key = p.post_key
               and pm.checkpoint = 'd7'
              left join public.post_fingerprints pf_high
                on pf_high.post_key = p.post_key
               and pf_high.media_confidence = 'high'
              where lower(coalesce(p.media_type, '')) in ('reel', 'video')
                and p.posted_at >= now() - (%s::int * interval '1 day')
                and (%s::int is null or p.feeder_id = %s::int)
                and exists (
                  select 1
                  from public.post_media_assets pma
                  where pma.post_key = p.post_key
                    and pma.asset_role = 'video_full'
                    and pma.status in ('active', 'purge_pending')
                    and coalesce(pma.storage_path, pma.public_url, '') <> ''
                )
            ),
            scored as (
              select
                *,
                row_number() over (
                  partition by feeder_id
                  order by
                    case when recent_rank <= 10 then d7_percentile else null end asc nulls last,
                    posted_at desc nulls last
                ) as recent_performance_rank
              from reel_d7
            )
            select post_key
            from scored
            where (
                d7_percentile <= 25
                or (recent_rank <= 10 and recent_performance_rank <= 2)
              )
              and high_fingerprint_post_key is null
            order by d7_percentile asc nulls last, posted_at desc nulls last
            limit %s
            """,
            (max(1, int(days)), feeder_id, feeder_id, max(1, int(limit))),
        )
        rows = cur.fetchall()
    return [str(row["post_key"]) for row in rows if row.get("post_key")]


def fingerprint_reels(conn: Any, *, feeder_id: int | None = None, limit: int = 10, days: int = 90) -> dict[str, Any]:
    post_keys = _candidate_post_keys(conn, feeder_id=feeder_id, limit=limit, days=days)
    resolved = 0
    missing_media = 0
    failed = 0
    for post_key in post_keys:
        try:
            fingerprint = ensure_post_fingerprint(conn, post_key)
            if fingerprint:
                resolved += 1
            else:
                missing_media += 1
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            failed += 1
            print(f"[fingerprint] failed post_key={post_key}: {exc}")
    return {
        "selected": len(post_keys),
        "resolved": resolved,
        "missing_media": missing_media,
        "failed": failed,
        "post_keys": post_keys,
        "model_version": current_model_version(),
        "sampling_policy_version": FINGERPRINT_SAMPLING_POLICY_VERSION,
    }
