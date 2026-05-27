from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import subprocess
import tempfile
import time
from datetime import datetime
from typing import Any

import requests
import yaml
from psycopg.rows import dict_row

from .config import (
    FEEDER_FINGERPRINT_MODEL,
    FEEDER_INTELLIGENCE_ENABLED,
    FEEDER_INTELLIGENCE_PROVIDER,
    GEMINI_API_KEY,
    MEDIA_PUBLIC_BASE_URL,
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
    POST_BREAKDOWN_MODEL,
)
from .feeder_prompts import (
    FINGERPRINT_EXTRACTION_SYSTEM_V8,
    FINGERPRINT_PROMPT_VERSION,
    FINGERPRINT_SAMPLING_POLICY_VERSION,
    POST_BREAKDOWN_EXTRACTION_SYSTEM_V2,
    POST_BREAKDOWN_PROMPT_VERSION,
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
_POST_BREAKDOWN_MAX_TOKENS = int(os.getenv("POST_BREAKDOWN_MAX_TOKENS", "5000"))
_POST_BREAKDOWN_KEYS = {
    "post_key",
    "works_because",
    "opens_with",
    "holds_attention_by",
    "viewer_mode",
    "lands_as",
    "receipts",
}
_FINGERPRINT_LIST_FIELDS = (
    "visible_text",
    "visual_sequence",
    "audio_behavior",
    "edit_and_pacing",
    "environment_and_entities",
    "observed_alignments",
    "notable_observed_details",
    "uncertainties",
)
_FINGERPRINT_OBJECT_KEYS = (
    "fingerprint",
    "post_fingerprint",
    "reel_fingerprint",
    "observation_fingerprint",
    "output",
)


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


def _balanced_json_objects(raw: str) -> list[str]:
    candidates: list[str] = []
    start = raw.find("{")
    while start >= 0:
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
                    candidates.append(raw[start:index + 1])
                    break
        start = raw.find("{", start + 1)
    return candidates


def _parse_mapping_text(text: str) -> dict[str, Any] | None:
    raw = _strip_fences(text)
    sources: list[str] = [raw, *_fenced_blocks(raw), *_balanced_json_objects(raw)]
    seen: set[str] = set()
    for source in sources:
        candidate = source.strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        for parser in (json.loads, yaml.safe_load):
            try:
                parsed = parser(candidate)
            except Exception:
                continue
            if isinstance(parsed, dict):
                return parsed
    return None


def _json_from_text(text: str) -> dict[str, Any] | None:
    return _parse_mapping_text(text)


def _normalize_fingerprint_mapping(parsed: dict[str, Any], *, post_key: str, caption: str) -> dict[str, Any] | None:
    fingerprint = parsed
    for key in _FINGERPRINT_OBJECT_KEYS:
        value = fingerprint.get(key)
        if isinstance(value, dict):
            fingerprint = value
            break
    if isinstance(fingerprint.get("pool_clustering_fields"), dict):
        fingerprint = fingerprint["pool_clustering_fields"]
    if not isinstance(fingerprint, dict):
        return None

    normalized = dict(fingerprint)
    normalized["post_key"] = str(normalized.get("post_key") or post_key)
    normalized["media_type"] = "reel"
    normalized["caption"] = str(normalized.get("caption") or caption or "")
    normalized["transcript"] = str(normalized.get("transcript") or "")
    normalized["media_confidence"] = "high"

    for key in _FINGERPRINT_LIST_FIELDS:
        value = normalized.get(key)
        if value is None:
            normalized[key] = []
        elif isinstance(value, list):
            normalized[key] = value
        else:
            normalized[key] = [value]

    has_observation = bool(
        normalized.get("transcript")
        or normalized.get("visible_text")
        or normalized.get("visual_sequence")
        or normalized.get("audio_behavior")
        or normalized.get("edit_and_pacing")
        or normalized.get("environment_and_entities")
        or normalized.get("observed_alignments")
        or normalized.get("notable_observed_details")
    )
    return normalized if has_observation else None


def _strip_fences(value: str) -> str:
    text = str(value or "").strip()
    if text.startswith("```"):
        lines = text.splitlines()[1:]
        while lines and lines[-1].strip() == "```":
            lines.pop()
        text = "\n".join(lines).strip()
    return text


def _fenced_blocks(value: str) -> list[str]:
    blocks = re.findall(r"```(?:yaml|yml|json)?\s*\n(.*?)```", str(value or ""), flags=re.IGNORECASE | re.DOTALL)
    return [block.strip() for block in blocks if block.strip()]


def _mapping_sources(text: str, root_keys: tuple[str, ...]) -> list[str]:
    sources: list[str] = []
    seen: set[str] = set()

    def add(candidate: str):
        cleaned = _strip_fences(candidate)
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            sources.append(cleaned)

    add(text)
    for block in _fenced_blocks(text):
        add(block)

    for source in list(sources):
        for root_key in root_keys:
            index = source.find(root_key)
            if index >= 0:
                add(source[index:])

    return sources


def _normalize_post_breakdown_mapping(parsed: dict[str, Any]) -> dict[str, Any]:
    breakdown = parsed.get("post_breakdown")
    if isinstance(breakdown, dict):
        return parsed
    if _POST_BREAKDOWN_KEYS.intersection(parsed.keys()):
        return {"post_breakdown": parsed}
    for value in parsed.values():
        if isinstance(value, dict) and _POST_BREAKDOWN_KEYS.intersection(value.keys()):
            return {"post_breakdown": value}
    return parsed


def _yaml_mapping_from_text(text: str) -> dict[str, Any] | None:
    for source in _mapping_sources(text, ("post_breakdown:", '"post_breakdown"', "{")):
        parsed_json = _json_from_text(source)
        if parsed_json is not None:
            return _normalize_post_breakdown_mapping(parsed_json)
        for candidate in (source, _repair_yaml_lines(source)):
            parsed = _safe_yaml_mapping(candidate)
            if parsed is not None:
                return _normalize_post_breakdown_mapping(parsed)
    return None


def _repair_yaml_lines(text: str) -> str:
    lines = text.splitlines()
    repaired: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        stripped = line.strip()
        if stripped and re.match(r"^(→|->|=>)\s+", stripped):
            indent = line[: len(line) - len(line.lstrip())]
            safe = stripped.replace("\\", "\\\\").replace('"', '\\"')
            repaired.append(f'{indent}- "{safe}"')
            index += 1
            continue
        if stripped.startswith("- ") and not stripped[2:].startswith(("{", "[")):
            indent = line[: len(line) - len(line.lstrip())]
            value = stripped[2:].strip()
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*:\s", value):
                if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                    value = value[1:-1]
                continuation: list[str] = []
                next_index = index + 1
                while next_index < len(lines):
                    next_line = lines[next_index]
                    next_stripped = next_line.strip()
                    if not next_stripped:
                        continuation.append("")
                        next_index += 1
                        continue
                    next_indent_len = len(next_line) - len(next_line.lstrip())
                    if next_indent_len <= len(indent):
                        break
                    continuation.append(next_stripped)
                    next_index += 1
                repaired.append(f"{indent}- >")
                repaired.append(f"{indent}  {value}")
                for continuation_line in continuation:
                    repaired.append(f"{indent}  {continuation_line}" if continuation_line else "")
                index = next_index
                continue
        repaired.append(line)
        index += 1
    return "\n".join(repaired)


def _safe_yaml_mapping(text: str) -> dict[str, Any] | None:
    try:
        parsed = yaml.safe_load(text)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _call_model(
    user_text: str,
    media_parts: list[dict[str, Any]],
    *,
    max_tokens: int = 3600,
    model_override: str | None = None,
) -> dict[str, Any] | None:
    result = _call_fingerprint_model(user_text, media_parts, max_tokens=max_tokens, model_override=model_override)
    if not result:
        return None
    _, parsed, _ = result
    return parsed


def _call_fingerprint_model(
    user_text: str,
    media_parts: list[dict[str, Any]],
    *,
    max_tokens: int = 3600,
    model_override: str | None = None,
) -> tuple[str, dict[str, Any] | None, str | None] | None:
    provider = _provider()
    if not provider:
        return None
    model = _model(provider, model_override)
    content = ""
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
            data = resp.json()
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
            data = resp.json()
        resp.raise_for_status()
        content = _extract_text(data, provider)
        parsed = _json_from_text(content)
        if parsed is not None:
            return content, parsed, None
        finish_reason = ""
        choices = data.get("choices") if isinstance(data, dict) else None
        if isinstance(choices, list) and choices:
            finish_reason = str((choices[0] or {}).get("finish_reason") or "").strip()
        error = "fingerprint output did not parse as JSON/YAML mapping"
        if finish_reason:
            error = f"{error}; finish_reason={finish_reason}"
        return content, None, error
    except Exception as exc:
        print(f"[fingerprint] model call failed: {exc}")
        return content, None, str(exc)


def post_breakdown_model_version() -> str:
    model = (POST_BREAKDOWN_MODEL or "").strip() or "none"
    return f"openrouter:{model}:{POST_BREAKDOWN_PROMPT_VERSION}"


def _call_post_breakdown_model(fingerprint: dict[str, Any]) -> tuple[str, dict[str, Any] | None, str | None] | None:
    if not OPENROUTER_API_KEY:
        return None
    model = (POST_BREAKDOWN_MODEL or "").strip()
    if not model:
        return None
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": POST_BREAKDOWN_EXTRACTION_SYSTEM_V2},
            {"role": "user", "content": json.dumps({"post_fingerprint": fingerprint}, ensure_ascii=False, indent=2, default=str)},
        ],
        "temperature": 0.2,
        "max_tokens": _POST_BREAKDOWN_MAX_TOKENS,
    }
    content = ""
    try:
        resp = requests.post(
            f"{OPENROUTER_BASE_URL.rstrip('/')}{_OPENROUTER_CHAT_URL}",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://feedme.local",
                "X-Title": "FeedMe Post Breakdown Pipeline",
            },
            json=payload,
            timeout=180,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"OpenRouter status {resp.status_code}: {resp.text[:1200]}")
        data = resp.json()
        content = _extract_text(data, "openrouter")
        parsed = _yaml_mapping_from_text(content)
        if parsed:
            return _strip_fences(content), parsed, None
        finish_reason = ""
        choices = data.get("choices") if isinstance(data, dict) else None
        if isinstance(choices, list) and choices:
            finish_reason = str((choices[0] or {}).get("finish_reason") or "").strip()
        error = "post breakdown output did not parse as YAML mapping"
        if finish_reason:
            error = f"{error}; finish_reason={finish_reason}"
        return _strip_fences(content), None, error
    except Exception as exc:
        print(f"[post_breakdown] model call failed: {exc}")
        return _strip_fences(content), None, str(exc)


def _record_post_breakdown_model_call(
    conn: Any,
    *,
    post_key: str,
    fingerprint: dict[str, Any],
    raw_output: str,
    parsed_output: dict[str, Any] | None,
    status: str,
    error: str | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.feeder_file_model_calls (
              feeder_file_id, call_key, call_type, feeder_handle, post_key,
              model, prompt_version, system_prompt, user_payload,
              raw_output, parsed_output, status, error, started_at, completed_at, updated_at
            )
            select null, %s, 'post_breakdown', lower(coalesce(f.handle, '')), %s,
                   %s, %s, %s, %s::jsonb, %s, %s::jsonb,
                   %s, %s, now(), now(), now()
            from public.posts p
            left join public.feeders f on f.id = p.feeder_id
            where p.post_key = %s
            on conflict (call_type, post_key, prompt_version)
            where call_type = 'post_breakdown' and post_key is not null
            do update set
              feeder_handle = excluded.feeder_handle,
              model = excluded.model,
              system_prompt = excluded.system_prompt,
              user_payload = excluded.user_payload,
              raw_output = excluded.raw_output,
              parsed_output = excluded.parsed_output,
              status = excluded.status,
              error = excluded.error,
              completed_at = now(),
              updated_at = now()
            """,
            (
                f"post_breakdown:{post_key}",
                post_key,
                POST_BREAKDOWN_MODEL,
                POST_BREAKDOWN_PROMPT_VERSION,
                POST_BREAKDOWN_EXTRACTION_SYSTEM_V2,
                json.dumps({"post_fingerprint": fingerprint}, ensure_ascii=False),
                raw_output,
                json.dumps(parsed_output, ensure_ascii=False) if parsed_output is not None else None,
                status,
                (error or "")[:1000] if error else None,
                post_key,
            ),
        )
    conn.commit()


def _store_post_breakdown(
    conn: Any,
    *,
    post_key: str,
    fingerprint: dict[str, Any],
    raw_output: str,
    parsed: dict[str, Any],
) -> dict[str, Any] | None:
    breakdown = parsed.get("post_breakdown")
    if not isinstance(breakdown, dict):
        return None

    breakdown["post_key"] = str(breakdown.get("post_key") or post_key)
    fingerprint_hash = _sha(fingerprint)
    fingerprint_status = fingerprint.get("fingerprint_status") if isinstance(fingerprint.get("fingerprint_status"), dict) else {}
    source_fingerprint_model_version = str(fingerprint_status.get("model_version") or "")
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.post_breakdowns (
              post_key, breakdown, breakdown_version,
              source_fingerprint_model_version, source_fingerprint_hash,
              generated_at, updated_at
            )
            values (%s, %s::jsonb, %s, %s, %s, now(), now())
            on conflict (post_key) do update set
              breakdown = excluded.breakdown,
              breakdown_version = excluded.breakdown_version,
              source_fingerprint_model_version = excluded.source_fingerprint_model_version,
              source_fingerprint_hash = excluded.source_fingerprint_hash,
              updated_at = now()
            """,
            (
                post_key,
                json.dumps({"post_breakdown": breakdown}, ensure_ascii=False),
                POST_BREAKDOWN_PROMPT_VERSION,
                source_fingerprint_model_version,
                fingerprint_hash,
            ),
        )
    conn.commit()
    _record_post_breakdown_model_call(
        conn,
        post_key=post_key,
        fingerprint=fingerprint,
        raw_output=raw_output,
        parsed_output=parsed,
        status="complete",
        error=None,
    )
    return {"post_breakdown": breakdown}


def _recover_post_breakdown_from_stored_raw(
    conn: Any,
    *,
    post_key: str,
    fingerprint: dict[str, Any],
) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select raw_output
            from public.feeder_file_model_calls
            where call_type = 'post_breakdown'
              and post_key = %s
              and prompt_version = %s
              and coalesce(raw_output, '') <> ''
            order by updated_at desc nulls last, id desc
            limit 3
            """,
            (post_key, POST_BREAKDOWN_PROMPT_VERSION),
        )
        rows = cur.fetchall()

    for row in rows:
        raw_output = str(row.get("raw_output") or "")
        parsed = _yaml_mapping_from_text(raw_output)
        if not parsed:
            continue
        recovered = _store_post_breakdown(
            conn,
            post_key=post_key,
            fingerprint=fingerprint,
            raw_output=raw_output,
            parsed=parsed,
        )
        if recovered:
            return recovered
    return None


def ensure_post_breakdown(conn: Any, post_key: str, fingerprint: dict[str, Any] | None = None) -> dict[str, Any] | None:
    if not fingerprint:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                select fingerprint
                from public.post_fingerprints
                where post_key = %s
                  and media_confidence = 'high'
                limit 1
                """,
                (post_key,),
            )
            row = cur.fetchone()
        fingerprint = row.get("fingerprint") if row else None
    if not isinstance(fingerprint, dict):
        return None

    fingerprint_hash = _sha(fingerprint)
    fingerprint_status = fingerprint.get("fingerprint_status") if isinstance(fingerprint.get("fingerprint_status"), dict) else {}
    source_fingerprint_model_version = str(fingerprint_status.get("model_version") or "")
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select breakdown
            from public.post_breakdowns
            where post_key = %s
              and breakdown_version = %s
              and source_fingerprint_hash = %s
            """,
            (post_key, POST_BREAKDOWN_PROMPT_VERSION, fingerprint_hash),
        )
        existing = cur.fetchone()
        if existing and isinstance(existing.get("breakdown"), dict):
            return existing["breakdown"]

    recovered = _recover_post_breakdown_from_stored_raw(conn, post_key=post_key, fingerprint=fingerprint)
    if recovered:
        return recovered

    result = _call_post_breakdown_model(fingerprint)
    if not result:
        return None
    raw_output, parsed, call_error = result
    if parsed is None:
        _record_post_breakdown_model_call(
            conn,
            post_key=post_key,
            fingerprint=fingerprint,
            raw_output=raw_output,
            parsed_output=None,
            status="failed",
            error=call_error or "post breakdown output did not parse",
        )
        if call_error:
            print(f"[post_breakdown] model call failed: {call_error}")
        return None
    stored = _store_post_breakdown(
        conn,
        post_key=post_key,
        fingerprint=fingerprint,
        raw_output=raw_output,
        parsed=parsed,
    )
    if not stored:
        print(f"[post_breakdown] skipped post_key={post_key}: missing post_breakdown")
        _record_post_breakdown_model_call(
            conn,
            post_key=post_key,
            fingerprint=fingerprint,
            raw_output=raw_output,
            parsed_output=parsed,
            status="failed",
            error="missing post_breakdown",
        )
        return None
    return stored


def _existing_high_fingerprint(conn: Any, post_key: str) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select fingerprint
            from public.post_fingerprints
            where post_key = %s
              and media_confidence = 'high'
            limit 1
            """,
            (post_key,),
        )
        row = cur.fetchone()
    fingerprint = row.get("fingerprint") if row else None
    return fingerprint if isinstance(fingerprint, dict) else None


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


def _record_fingerprint_model_call(
    conn: Any,
    *,
    post_key: str,
    model_version: str,
    user_payload: dict[str, Any],
    raw_output: str,
    parsed_output: dict[str, Any] | None,
    status: str,
    error: str | None,
) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.feeder_file_model_calls (
                  feeder_file_id, call_key, call_type, feeder_handle, post_key,
                  model, prompt_version, system_prompt, user_payload,
                  raw_output, parsed_output, status, error, started_at, completed_at, updated_at
                )
                select null, %s, 'fingerprint', lower(coalesce(f.handle, '')), %s,
                       %s, %s, %s, %s::jsonb, %s, %s::jsonb,
                       %s, %s, now(), now(), now()
                from public.posts p
                left join public.feeders f on f.id = p.feeder_id
                where p.post_key = %s
                on conflict (call_type, post_key, prompt_version)
                where call_type = 'fingerprint' and post_key is not null
                do update set
                  feeder_handle = excluded.feeder_handle,
                  model = excluded.model,
                  system_prompt = excluded.system_prompt,
                  user_payload = excluded.user_payload,
                  raw_output = excluded.raw_output,
                  parsed_output = excluded.parsed_output,
                  status = excluded.status,
                  error = excluded.error,
                  completed_at = now(),
                  updated_at = now()
                """,
                (
                    f"fingerprint:{post_key}",
                    post_key,
                    model_version,
                    FINGERPRINT_PROMPT_VERSION,
                    FINGERPRINT_EXTRACTION_SYSTEM_V8,
                    json.dumps(user_payload, ensure_ascii=False),
                    raw_output,
                    json.dumps(parsed_output, ensure_ascii=False) if parsed_output is not None else None,
                    status,
                    (error or "")[:1000] if error else None,
                    post_key,
                ),
            )
        conn.commit()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[fingerprint] raw call record skipped post_key={post_key}: {exc}")


def _store_post_fingerprint(
    conn: Any,
    *,
    post_key: str,
    fingerprint: dict[str, Any],
    media_hash: str,
    caption_hash: str,
    model_version: str,
) -> dict[str, Any]:
    fingerprint["fingerprint_status"] = {
        "media_confidence": fingerprint.get("media_confidence") or "high",
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
                json.dumps(fingerprint, ensure_ascii=False),
                media_hash,
                caption_hash,
                FINGERPRINT_SAMPLING_POLICY_VERSION,
                model_version,
                str(fingerprint.get("media_confidence") or "high"),
            ),
        )
    conn.commit()
    return fingerprint


def _recover_fingerprint_from_stored_raw(
    conn: Any,
    *,
    post_key: str,
    caption: str,
    media_hash: str,
    caption_hash: str,
    model_version: str,
    user_payload: dict[str, Any],
) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select raw_output
            from public.feeder_file_model_calls
            where call_type = 'fingerprint'
              and post_key = %s
              and prompt_version = %s
              and coalesce(raw_output, '') <> ''
            order by updated_at desc nulls last, id desc
            limit 3
            """,
            (post_key, FINGERPRINT_PROMPT_VERSION),
        )
        rows = cur.fetchall()

    for row in rows:
        raw_output = str(row.get("raw_output") or "")
        parsed = _json_from_text(raw_output)
        if not parsed:
            continue
        fingerprint = _normalize_fingerprint_mapping(parsed, post_key=post_key, caption=caption)
        if not fingerprint:
            continue
        stored = _store_post_fingerprint(
            conn,
            post_key=post_key,
            fingerprint=fingerprint,
            media_hash=media_hash,
            caption_hash=caption_hash,
            model_version=model_version,
        )
        _record_fingerprint_model_call(
            conn,
            post_key=post_key,
            model_version=model_version,
            user_payload=user_payload,
            raw_output=raw_output,
            parsed_output=stored,
            status="complete",
            error=None,
        )
        return stored
    return None


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
    user_payload = {
        "post_key": post_key,
        "media_source_hash": media_hash,
        "caption_hash": caption_hash,
        "sampling_policy_version": FINGERPRINT_SAMPLING_POLICY_VERSION,
        "user_text": user_text,
    }
    recovered = _recover_fingerprint_from_stored_raw(
        conn,
        post_key=post_key,
        caption=caption,
        media_hash=media_hash,
        caption_hash=caption_hash,
        model_version=model_version,
        user_payload=user_payload,
    )
    if recovered:
        return recovered

    result = _call_fingerprint_model(user_text, media_parts)
    if not result:
        return None
    raw_output, parsed, call_error = result
    if parsed is None:
        _record_fingerprint_model_call(
            conn,
            post_key=post_key,
            model_version=model_version,
            user_payload=user_payload,
            raw_output=raw_output,
            parsed_output=None,
            status="failed",
            error=call_error or "fingerprint output did not parse",
        )
        if call_error:
            print(f"[fingerprint] model call failed post_key={post_key}: {call_error}")
        return None

    fingerprint = _normalize_fingerprint_mapping(parsed, post_key=post_key, caption=caption)
    if not fingerprint:
        _record_fingerprint_model_call(
            conn,
            post_key=post_key,
            model_version=model_version,
            user_payload=user_payload,
            raw_output=raw_output,
            parsed_output=parsed,
            status="failed",
            error="fingerprint output missing observable fields",
        )
        return None

    stored = _store_post_fingerprint(
        conn,
        post_key=post_key,
        fingerprint=fingerprint,
        media_hash=media_hash,
        caption_hash=caption_hash,
        model_version=model_version,
    )
    _record_fingerprint_model_call(
        conn,
        post_key=post_key,
        model_version=model_version,
        user_payload=user_payload,
        raw_output=raw_output,
        parsed_output=stored,
        status="complete",
        error=None,
    )
    return stored


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
                pf_high.post_key as high_fingerprint_post_key,
                pb.post_key as post_breakdown_post_key
              from public.posts p
              join public.post_metrics pm
                on pm.post_key = p.post_key
               and pm.checkpoint = 'd7'
              left join public.post_fingerprints pf_high
                on pf_high.post_key = p.post_key
               and pf_high.media_confidence = 'high'
              left join public.post_breakdowns pb
                on pb.post_key = p.post_key
              where lower(coalesce(p.media_type, '')) in ('reel', 'video')
                and p.posted_at >= now() - (%s::int * interval '1 day')
                and (%s::int is null or p.feeder_id = %s::int)
                and (
                  pf_high.post_key is not null
                  or exists (
                  select 1
                  from public.post_media_assets pma
                  where pma.post_key = p.post_key
                    and pma.asset_role = 'video_full'
                    and pma.status in ('active', 'purge_pending')
                    and coalesce(pma.storage_path, pma.public_url, '') <> ''
                  )
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
              and (high_fingerprint_post_key is null or post_breakdown_post_key is null)
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
    breakdowns = 0
    missing_media = 0
    failed = 0
    for post_key in post_keys:
        try:
            fingerprint = _existing_high_fingerprint(conn, post_key) or ensure_post_fingerprint(conn, post_key)
            if fingerprint:
                resolved += 1
                breakdown = ensure_post_breakdown(conn, post_key, fingerprint)
                if breakdown:
                    breakdowns += 1
                else:
                    failed += 1
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
        "post_breakdowns": breakdowns,
        "missing_media": missing_media,
        "failed": failed,
        "post_keys": post_keys,
        "model_version": current_model_version(),
        "sampling_policy_version": FINGERPRINT_SAMPLING_POLICY_VERSION,
    }
