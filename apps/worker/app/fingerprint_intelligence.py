from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import re
import statistics
import subprocess
import tempfile
import time
from datetime import date, datetime
from typing import Any
from urllib.parse import quote

import requests
import yaml
from psycopg.rows import dict_row

from .config import (
    D7_READ_MODEL,
    FEEDER_FINGERPRINT_MODEL,
    FEEDER_INTELLIGENCE_ENABLED,
    FEEDER_INTELLIGENCE_PROVIDER,
    GEMINI_API_KEY,
    MEDIA_PUBLIC_BASE_URL,
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
    POST_CONDENSATION_MODEL,
    R2_ACCESS_KEY_ID,
    R2_BUCKET,
    R2_ENDPOINT_URL,
    R2_REGION,
    R2_SECRET_ACCESS_KEY,
    SUPABASE_MEDIA_BUCKET,
    SUPABASE_SERVICE_ROLE_KEY,
    SUPABASE_URL,
)
from .feeder_prompts import (
    ACTIVE_COLD_START_COMPILE_VERSIONS,
    FEEDER_FILE_COLD_START_PROMPT_VERSION,
    POST_CONDENSATION_PROMPT_VERSION,
    POST_CONDENSATION_SYSTEM_V5,
)
from .intelligence_engine_prompts import (
    D7_READ_PROMPT_VERSION,
    D7_READ_SYSTEM,
    FEEDER_FILE_MAX_REEL_CARDS,
    FINGERPRINT_EXTRACTION_SYSTEM_V8,
    FINGERPRINT_PROMPT_VERSION,
    FINGERPRINT_SAMPLING_POLICY_VERSION,
)

_GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
_GEMINI_UPLOAD_URL = "https://generativelanguage.googleapis.com/upload/v1beta/files"
_GEMINI_FILE_URL = "https://generativelanguage.googleapis.com/v1beta/files/{name}"
_OPENROUTER_CHAT_URL = "/chat/completions"
_DEFAULT_OPENROUTER_MODEL = "google/gemini-3.5-flash"
_DEFAULT_GEMINI_MODEL = "gemini-3.5-flash"
_VIDEO_UPLOAD_MAX_BYTES = int(os.getenv("FINGERPRINT_VIDEO_UPLOAD_MAX_BYTES", str(50 * 1024 * 1024)))
_VIDEO_INLINE_MAX_BYTES = int(os.getenv("FINGERPRINT_VIDEO_INLINE_MAX_BYTES", str(20 * 1024 * 1024)))
_VIDEO_SAMPLE_SECONDS = int(os.getenv("FINGERPRINT_VIDEO_SAMPLE_SECONDS", "120"))
_FINGERPRINT_MAX_TOKENS = int(os.getenv("FINGERPRINT_MAX_TOKENS", "9000"))
_POST_CONDENSATION_MAX_TOKENS = int(os.getenv("POST_CONDENSATION_MAX_TOKENS", "2600"))
_POST_CONDENSATION_WORD_TOLERANCE = float(os.getenv("POST_CONDENSATION_WORD_TOLERANCE", "0.15"))
_D7_READ_MAX_TOKENS = int(os.getenv("D7_READ_MAX_TOKENS", "1800"))
_R2_CLIENT: Any | None = None
_POST_CONDENSATION_KEYS = {
    "post_key",
    "meta",
    "caption",
    "reel",
    "standout_details",
}
_D7_READ_KEYS = {
    "read",
    "scene",
    "fit",
    "recent_run",
}
_FINGERPRINT_LIST_FIELDS = (
    "visible_text",
    "visual_sequence",
    "audio_behavior",
    "cultural_references",
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


def _fetch_bytes(
    url: str | None,
    *,
    timeout: int = 30,
    max_bytes: int = 0,
    headers: dict[str, str] | None = None,
) -> tuple[bytes, str] | None:
    if not url or not str(url).startswith(("http://", "https://")):
        return None
    try:
        request_headers = {"User-Agent": "FeedMe/1.0", "Referer": "https://www.instagram.com/"}
        if headers:
            request_headers.update(headers)
        resp = requests.get(
            str(url),
            timeout=(min(10, timeout), timeout),
            headers=request_headers,
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
        parent = dict(fingerprint)
        cluster_fields = dict(fingerprint["pool_clustering_fields"])
        fingerprint = {**parent, **cluster_fields}
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

    has_structured_observation = bool(
        normalized.get("visible_text")
        or normalized.get("visual_sequence")
        or normalized.get("audio_behavior")
        or normalized.get("cultural_references")
        or normalized.get("edit_and_pacing")
        or normalized.get("environment_and_entities")
        or normalized.get("observed_alignments")
        or normalized.get("notable_observed_details")
    )
    return normalized if has_structured_observation else None


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


def _word_count(text: Any) -> int:
    return len(re.findall(r"\S+", str(text or "")))


def _infer_condensation_complexity(fingerprint: dict[str, Any]) -> tuple[str, int, int]:
    sequence = fingerprint.get("visual_sequence")
    audio = fingerprint.get("audio_behavior")
    transcript_words = _word_count(fingerprint.get("transcript"))
    sequence_count = len(sequence) if isinstance(sequence, list) else 0
    audio_count = len(audio) if isinstance(audio, list) else 0
    observed_density = sequence_count + audio_count

    if transcript_words >= 180 or sequence_count >= 8 or observed_density >= 11:
        return "dense", 200, 280
    if transcript_words >= 45 or sequence_count >= 3 or observed_density >= 4:
        return "standard", 140, 200
    return "simple", 60, 100


def _post_condensation_body(parsed: dict[str, Any]) -> dict[str, Any] | None:
    value = parsed.get("post_condensed")
    if isinstance(value, dict):
        return value
    if _POST_CONDENSATION_KEYS.intersection(parsed.keys()):
        return parsed
    for value in parsed.values():
        if isinstance(value, dict) and _POST_CONDENSATION_KEYS.intersection(value.keys()):
            return value
    return None


def _normalize_post_condensation_mapping(
    parsed: dict[str, Any],
    *,
    post_key: str,
    fingerprint: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    condensed = _post_condensation_body(parsed)
    if not isinstance(condensed, dict):
        return None

    warnings: list[str] = []
    model_post_key = str(condensed.get("post_key") or "").strip()
    if model_post_key and model_post_key != post_key:
        warnings.append("post_key_overridden_to_server_post_key")

    reel = str(condensed.get("reel") or "").strip()
    if not reel:
        return None

    caption_value = condensed.get("caption")
    if caption_value is None and isinstance(fingerprint, dict):
        caption_value = fingerprint.get("caption")
    caption = str(caption_value or "").strip()

    def coerce_detail(item: Any) -> str:
        if isinstance(item, dict):
            parts: list[str] = []
            for key, value in item.items():
                key_text = str(key or "").strip()
                value_text = str(value or "").strip()
                if key_text and value_text:
                    parts.append(f"{key_text}: {value_text}")
                elif key_text:
                    parts.append(key_text)
                elif value_text:
                    parts.append(value_text)
            return "; ".join(parts).strip()
        return str(item or "").strip()

    details_value = condensed.get("standout_details")
    if isinstance(details_value, list):
        details = [coerce_detail(item) for item in details_value if coerce_detail(item)]
    elif details_value:
        details = [coerce_detail(details_value)]
        warnings.append("standout_details_recovered_from_scalar")
    else:
        details = []
        warnings.append("standout_details_missing")

    meta_value = condensed.get("meta") if isinstance(condensed.get("meta"), dict) else {}
    if isinstance(fingerprint, dict):
        duration_seconds = meta_value.get("duration_seconds", fingerprint.get("duration_seconds"))
        media_type = meta_value.get("media_type", fingerprint.get("media_type") or "reel")
        media_truncated = meta_value.get("media_truncated", fingerprint.get("media_truncated") or False)
        observed_window = meta_value.get("observed_window", fingerprint.get("observed_window") or "")
    else:
        duration_seconds = meta_value.get("duration_seconds")
        media_type = meta_value.get("media_type") or "reel"
        media_truncated = meta_value.get("media_truncated") or False
        observed_window = meta_value.get("observed_window") or ""

    reel_words = _word_count(reel)
    complexity = "unknown"
    target_min = 0
    target_max = 0
    soft_max = 0
    if isinstance(fingerprint, dict):
        complexity, target_min, target_max = _infer_condensation_complexity(fingerprint)
        soft_max = int(round(target_max * (1 + _POST_CONDENSATION_WORD_TOLERANCE)))
        if reel_words > soft_max:
            warnings.append("reel_word_count_far_over_target")
        elif reel_words > target_max:
            warnings.append("reel_word_count_over_target_within_tolerance")

    normalized = {
        "post_key": post_key,
        "meta": {
            "duration_seconds": duration_seconds,
            "media_type": str(media_type or "reel"),
            "media_truncated": bool(media_truncated),
            "observed_window": str(observed_window or ""),
        },
        "caption": caption,
        "reel": reel,
        "standout_details": details,
    }
    validation = {
        "accepted": True,
        "warnings": warnings,
        "reel_word_count": reel_words,
        "standout_detail_count": len(details),
        "target_reel_word_budget": {
            "complexity": complexity,
            "min": target_min,
            "max": target_max,
            "soft_max": soft_max,
            "tolerance": _POST_CONDENSATION_WORD_TOLERANCE,
        },
    }
    return {"post_condensed": normalized, "server_validation": validation}


def _recover_post_condensation_from_text(text: str, *, post_key: str, fingerprint: dict[str, Any] | None = None) -> dict[str, Any] | None:
    source = _strip_fences(text)
    post_key_match = re.search(r"(?m)^\s*post_key:\s*(.+?)\s*$", source)
    recovered_post_key = (post_key_match.group(1).strip().strip("'\"") if post_key_match else post_key).strip()

    caption = ""
    caption_block = re.search(r"(?ms)^\s*caption:\s*\|-\s*\n(?P<body>.*?)(?=^\s*reel:\s*\|-)", source)
    if caption_block:
        caption = _dedent_yaml_block(caption_block.group("body")).strip()
    else:
        caption_inline = re.search(r"(?m)^\s*caption:\s*(.+?)\s*$", source)
        if caption_inline:
            caption = caption_inline.group(1).strip().strip("'\"")

    reel = ""
    reel_block = re.search(r"(?ms)^\s*reel:\s*\|-\s*\n(?P<body>.*?)(?=^\s*standout_details:\s*$)", source)
    if reel_block:
        reel = _dedent_yaml_block(reel_block.group("body")).strip()

    details: list[str] = []
    details_block = re.search(r"(?ms)^\s*standout_details:\s*\n(?P<body>.*)$", source)
    if details_block:
        for line in details_block.group("body").splitlines():
            stripped = line.strip()
            if stripped.startswith("- "):
                details.append(stripped[2:].strip().strip("'\""))

    if not reel:
        return None
    return _normalize_post_condensation_mapping(
        {
            "post_condensed": {
                "post_key": recovered_post_key,
                "caption": caption,
                "reel": reel,
                "standout_details": details,
            }
        },
        post_key=post_key,
        fingerprint=fingerprint,
    )


def _dedent_yaml_block(block: str) -> str:
    lines = block.splitlines()
    indents = [len(line) - len(line.lstrip(" ")) for line in lines if line.strip()]
    indent = min(indents) if indents else 0
    return "\n".join(line[indent:] if len(line) >= indent else line for line in lines)


def _yaml_post_condensation_from_text(
    text: str,
    *,
    post_key: str,
    fingerprint: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    for source in _mapping_sources(text, ("post_condensed:", '"post_condensed"', "{")):
        parsed_json = _json_from_text(source)
        if parsed_json is not None:
            normalized = _normalize_post_condensation_mapping(parsed_json, post_key=post_key, fingerprint=fingerprint)
            if normalized:
                return normalized
        for candidate in (source, _repair_yaml_lines(source)):
            parsed = _safe_yaml_mapping(candidate)
            if parsed is not None:
                normalized = _normalize_post_condensation_mapping(parsed, post_key=post_key, fingerprint=fingerprint)
                if normalized:
                    return normalized
    return _recover_post_condensation_from_text(text, post_key=post_key, fingerprint=fingerprint)


def _d7_read_body(parsed: dict[str, Any]) -> dict[str, Any] | None:
    value = parsed.get("d7_read")
    if isinstance(value, dict):
        return value
    if _D7_READ_KEYS.intersection(parsed.keys()):
        return parsed
    for value in parsed.values():
        if isinstance(value, dict) and _D7_READ_KEYS.intersection(value.keys()):
            return value
    return None


def _flatten_read_field(value: Any) -> str:
    """A read field is a single line of prose (hook baked in). Tolerate a
    legacy {headline, read} dict by folding it back into one string."""
    if isinstance(value, dict):
        headline = str(value.get("headline") or "").strip()
        read = str(value.get("read") or value.get("body") or value.get("text") or "").strip()
        return f"{headline} {read}".strip() if headline else read
    return str(value or "").strip()


def _normalize_d7_read_mapping(parsed: dict[str, Any], *, post_key: str) -> dict[str, Any] | None:
    body = _d7_read_body(parsed)
    if not isinstance(body, dict):
        return None

    read = str(body.get("read") or "").strip()
    scene = str(body.get("scene") or "").strip()
    fit = _flatten_read_field(body.get("fit"))
    recent_run = _flatten_read_field(body.get("recent_run") or body.get("run"))
    if read:
        out: dict[str, Any] = {
            "post_key": post_key,
            "read": read,
        }
        fun_fact = body.get("fun_fact")
        if isinstance(fun_fact, dict) and fun_fact.get("text"):
            out["fun_fact"] = fun_fact
        return {"d7_read": out}

    if not (scene and fit and recent_run):
        return None

    out: dict[str, Any] = {
        "post_key": post_key,
        "scene": scene,
        "fit": fit,
        "recent_run": recent_run,
    }
    # headline is the LLM's 3-6 word teaser (v16+); carry it when present so the
    # cached path keeps it. Older reads have no headline and render without one.
    headline = str(body.get("headline") or "").strip()
    if headline:
        out["headline"] = headline
    # fun_fact is worker-computed and merged into the stored output; carry it
    # through so the cached path doesn't drop it.
    fun_fact = body.get("fun_fact")
    if isinstance(fun_fact, dict) and fun_fact.get("text"):
        out["fun_fact"] = fun_fact
    return {"d7_read": out}


def _yaml_d7_read_from_text(text: str, *, post_key: str) -> dict[str, Any] | None:
    for source in _mapping_sources(text, ("d7_read:", '"d7_read"', "{")):
        parsed_json = _json_from_text(source)
        if parsed_json is not None:
            normalized = _normalize_d7_read_mapping(parsed_json, post_key=post_key)
            if normalized:
                return normalized
        for candidate in (source, _repair_yaml_lines(source)):
            parsed = _safe_yaml_mapping(candidate)
            if parsed is not None:
                normalized = _normalize_d7_read_mapping(parsed, post_key=post_key)
                if normalized:
                    return normalized
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
    max_tokens: int | None = None,
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
    max_tokens: int | None = None,
    model_override: str | None = None,
    _compact_retry: bool = False,
) -> tuple[str, dict[str, Any] | None, str | None] | None:
    provider = _provider()
    if not provider:
        return None
    model = _model(provider, model_override)
    token_budget = max_tokens or _FINGERPRINT_MAX_TOKENS
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
                "max_tokens": token_budget,
                "response_format": {"type": "json_object"},
            }
            url = f"{OPENROUTER_BASE_URL.rstrip('/')}{_OPENROUTER_CHAT_URL}"
            headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"}
            resp = requests.post(url, headers=headers, json=payload, timeout=180)
            if resp.status_code >= 400:
                payload.pop("response_format", None)
                resp = requests.post(url, headers=headers, json=payload, timeout=180)
            try:
                data = resp.json()
            except Exception:
                if "response_format" not in payload:
                    raise
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
                        "maxOutputTokens": token_budget,
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
        if finish_reason == "length" and not _compact_retry:
            compact_user_text = "\n".join([
                user_text,
                "",
                "LONG_REEL_RECOVERY_MODE:",
                "The previous output was too long. Return the same fingerprint schema, but compact.",
                "Do not include a full transcript. Keep transcript under 120 words.",
                "Use arrays with only the strongest observable visual/audio/edit/details.",
                "The JSON must be complete and valid. First character {, last character }.",
            ])
            retry = _call_fingerprint_model(
                compact_user_text,
                media_parts,
                max_tokens=token_budget,
                model_override=model_override,
                _compact_retry=True,
            )
            if retry:
                retry_content, retry_parsed, retry_error = retry
                joined = "\n\n--- COMPACT RETRY ---\n\n".join([content, retry_content]).strip()
                return joined, retry_parsed, retry_error
        return content, None, error
    except Exception as exc:
        print(f"[fingerprint] model call failed: {exc}")
        return content, None, str(exc)


def post_condensation_model_version() -> str:
    model = (POST_CONDENSATION_MODEL or "").strip() or "none"
    return f"openrouter:{model}:{POST_CONDENSATION_PROMPT_VERSION}"


def d7_read_model_version() -> str:
    model = (D7_READ_MODEL or "").strip() or "none"
    return f"openrouter:{model}:{D7_READ_PROMPT_VERSION}"


def _call_post_condensation_model(
    fingerprint: dict[str, Any],
    *,
    post_key: str,
) -> tuple[str, dict[str, Any] | None, str | None] | None:
    if not OPENROUTER_API_KEY:
        return None
    model = (POST_CONDENSATION_MODEL or "").strip()
    if not model:
        return None
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": POST_CONDENSATION_SYSTEM_V5},
            {"role": "user", "content": json.dumps({"post_fingerprint": fingerprint}, ensure_ascii=False, indent=2, default=str)},
        ],
        "temperature": 0.1,
        "max_tokens": _POST_CONDENSATION_MAX_TOKENS,
    }
    content = ""
    try:
        resp = requests.post(
            f"{OPENROUTER_BASE_URL.rstrip('/')}{_OPENROUTER_CHAT_URL}",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://feedme.local",
                "X-Title": "FeedMe Post Condensation Pipeline",
            },
            json=payload,
            timeout=180,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"OpenRouter status {resp.status_code}: {resp.text[:1200]}")
        data = resp.json()
        content = _extract_text(data, "openrouter")
        parsed = _yaml_post_condensation_from_text(content, post_key=post_key, fingerprint=fingerprint)
        if parsed:
            return _strip_fences(content), parsed, None
        finish_reason = ""
        choices = data.get("choices") if isinstance(data, dict) else None
        if isinstance(choices, list) and choices:
            finish_reason = str((choices[0] or {}).get("finish_reason") or "").strip()
        error = "post condensation output did not contain a usable reel"
        if finish_reason:
            error = f"{error}; finish_reason={finish_reason}"
        return _strip_fences(content), None, error
    except Exception as exc:
        print(f"[post_condensation] model call failed: {exc}")
        return _strip_fences(content), None, str(exc)


def _call_d7_read_model(
    user_payload: dict[str, Any],
    *,
    post_key: str,
) -> tuple[str, dict[str, Any] | None, str | None] | None:
    if not OPENROUTER_API_KEY:
        return None
    model = (D7_READ_MODEL or "").strip()
    if not model:
        return None
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": D7_READ_SYSTEM},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, indent=2, default=str)},
        ],
        "temperature": float(os.getenv("D7_READ_TEMPERATURE", "0.55")),
        "max_tokens": _D7_READ_MAX_TOKENS,
    }
    content = ""
    try:
        resp = requests.post(
            f"{OPENROUTER_BASE_URL.rstrip('/')}{_OPENROUTER_CHAT_URL}",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://feedme.local",
                "X-Title": "FeedMe D7 Read Pipeline",
            },
            json=payload,
            timeout=180,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"OpenRouter status {resp.status_code}: {resp.text[:1200]}")
        data = resp.json()
        content = _extract_text(data, "openrouter")
        parsed = _yaml_d7_read_from_text(content, post_key=post_key)
        if parsed:
            return _strip_fences(content), parsed, None
        finish_reason = ""
        choices = data.get("choices") if isinstance(data, dict) else None
        if isinstance(choices, list) and choices:
            finish_reason = str((choices[0] or {}).get("finish_reason") or "").strip()
        error = "d7 read output did not parse as YAML mapping"
        if finish_reason:
            error = f"{error}; finish_reason={finish_reason}"
        return _strip_fences(content), None, error
    except Exception as exc:
        print(f"[d7_read] model call failed: {exc}")
        return _strip_fences(content), None, str(exc)


def _record_post_condensation_model_call(
    conn: Any,
    *,
    post_key: str,
    fingerprint: dict[str, Any],
    raw_output: str,
    parsed_output: dict[str, Any] | None,
    status: str,
    error: str | None,
) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.feeder_file_model_calls (
              feeder_file_id, call_key, call_type, feeder_handle, post_key,
              model, prompt_version, system_prompt, user_payload,
              raw_output, parsed_output, status, error, started_at, completed_at, updated_at
            )
            select null, %s, 'post_condensation', lower(coalesce(f.handle, '')), %s,
                   %s, %s, %s, %s::jsonb, %s, %s::jsonb,
                   %s, %s, now(), now(), now()
            from public.posts p
            left join public.feeders f on f.id = p.feeder_id
            where p.post_key = %s
            on conflict (call_type, post_key, prompt_version)
            where call_type = 'post_condensation' and post_key is not null
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
            returning id
            """,
            (
                f"post_condensation:{post_key}",
                post_key,
                POST_CONDENSATION_MODEL,
                POST_CONDENSATION_PROMPT_VERSION,
                POST_CONDENSATION_SYSTEM_V5,
                json.dumps({"post_fingerprint": fingerprint}, ensure_ascii=False),
                raw_output,
                json.dumps(parsed_output, ensure_ascii=False) if parsed_output is not None else None,
                status,
                (error or "")[:1000] if error else None,
                post_key,
            ),
        )
        row = cur.fetchone()
    conn.commit()
    if not row:
        return None
    if isinstance(row, dict):
        return int(row["id"])
    return int(row[0])


def _record_d7_read_model_call(
    conn: Any,
    *,
    post_key: str,
    user_payload: dict[str, Any],
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
            select null, %s, 'd7_read', lower(coalesce(f.handle, '')), %s,
                   %s, %s, %s, %s::jsonb, %s, %s::jsonb,
                   %s, %s, now(), now(), now()
            from public.posts p
            left join public.feeders f on f.id = p.feeder_id
            where p.post_key = %s
            on conflict (call_type, post_key, prompt_version)
            where call_type = 'd7_read' and post_key is not null
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
                f"d7_read:{post_key}",
                post_key,
                D7_READ_MODEL,
                D7_READ_PROMPT_VERSION,
                D7_READ_SYSTEM,
                json.dumps(user_payload, ensure_ascii=False, default=str),
                raw_output,
                json.dumps(parsed_output, ensure_ascii=False) if parsed_output is not None else None,
                status,
                (error or "")[:1000] if error else None,
                post_key,
            ),
        )
    conn.commit()


def _fingerprint_source_metadata(fingerprint: dict[str, Any]) -> tuple[str, str]:
    fingerprint_hash = _sha(fingerprint)
    fingerprint_status = fingerprint.get("fingerprint_status") if isinstance(fingerprint.get("fingerprint_status"), dict) else {}
    source_fingerprint_model_version = str(fingerprint_status.get("model_version") or "")
    return fingerprint_hash, source_fingerprint_model_version


def _store_post_condensation(
    conn: Any,
    *,
    post_key: str,
    fingerprint: dict[str, Any],
    condensation: dict[str, Any],
    model_call_id: int | None = None,
) -> dict[str, Any] | None:
    normalized = _normalize_post_condensation_mapping(condensation, post_key=post_key, fingerprint=fingerprint)
    if not normalized:
        return None

    fingerprint_hash, source_fingerprint_model_version = _fingerprint_source_metadata(fingerprint)
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.post_condensations (
              post_key, condensation, condensation_version,
              source_fingerprint_model_version, source_fingerprint_hash,
              model_version, model_call_id, generated_at, updated_at
            )
            values (%s, %s::jsonb, %s, %s, %s, %s, %s, now(), now())
            on conflict (post_key) do update set
              condensation = excluded.condensation,
              condensation_version = excluded.condensation_version,
              source_fingerprint_model_version = excluded.source_fingerprint_model_version,
              source_fingerprint_hash = excluded.source_fingerprint_hash,
              model_version = excluded.model_version,
              model_call_id = coalesce(excluded.model_call_id, post_condensations.model_call_id),
              generated_at = case
                when post_condensations.condensation_version is distinct from excluded.condensation_version
                  or post_condensations.source_fingerprint_hash is distinct from excluded.source_fingerprint_hash
                then now()
                else post_condensations.generated_at
              end,
              updated_at = now()
            """,
            (
                post_key,
                json.dumps(normalized, ensure_ascii=False),
                POST_CONDENSATION_PROMPT_VERSION,
                source_fingerprint_model_version,
                fingerprint_hash,
                post_condensation_model_version(),
                model_call_id,
            ),
        )
    conn.commit()
    return normalized


def _recover_post_condensation_from_stored_raw(
    conn: Any,
    *,
    post_key: str,
    fingerprint: dict[str, Any],
) -> dict[str, Any] | None:
    fingerprint_hash, _source_fingerprint_model_version = _fingerprint_source_metadata(fingerprint)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select id, raw_output, user_payload
            from public.feeder_file_model_calls
            where call_type = 'post_condensation'
              and post_key = %s
              and prompt_version = %s
              and coalesce(raw_output, '') <> ''
            order by updated_at desc nulls last, id desc
            limit 3
            """,
            (post_key, POST_CONDENSATION_PROMPT_VERSION),
        )
        rows = cur.fetchall()

    for row in rows:
        user_payload = row.get("user_payload") if isinstance(row.get("user_payload"), dict) else {}
        source_fingerprint = user_payload.get("post_fingerprint") if isinstance(user_payload, dict) else None
        if isinstance(source_fingerprint, dict) and _sha(source_fingerprint) != fingerprint_hash:
            continue
        raw_output = str(row.get("raw_output") or "")
        parsed = _yaml_post_condensation_from_text(raw_output, post_key=post_key, fingerprint=fingerprint)
        if not parsed:
            continue
        model_call_id = _record_post_condensation_model_call(
            conn,
            post_key=post_key,
            fingerprint=fingerprint,
            raw_output=raw_output,
            parsed_output=parsed,
            status="complete",
            error=None,
        )
        return _store_post_condensation(
            conn,
            post_key=post_key,
            fingerprint=fingerprint,
            condensation=parsed,
            model_call_id=model_call_id or row.get("id"),
        )
    return None


def ensure_post_condensation(conn: Any, post_key: str, fingerprint: dict[str, Any] | None = None) -> dict[str, Any] | None:
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

    fingerprint_hash, _source_fingerprint_model_version = _fingerprint_source_metadata(fingerprint)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select condensation
            from public.post_condensations
            where post_key = %s
              and condensation_version = %s
              and source_fingerprint_hash = %s
            """,
            (post_key, POST_CONDENSATION_PROMPT_VERSION, fingerprint_hash),
        )
        existing = cur.fetchone()
        condensation = existing.get("condensation") if existing else None
        if isinstance(condensation, dict):
            normalized = _normalize_post_condensation_mapping(condensation, post_key=post_key, fingerprint=fingerprint)
            if normalized:
                return normalized

        cur.execute(
            """
            select id, parsed_output, user_payload
            from public.feeder_file_model_calls
            where call_type = 'post_condensation'
              and post_key = %s
              and prompt_version = %s
              and status = 'complete'
              and parsed_output is not null
            order by updated_at desc nulls last, id desc
            limit 1
            """,
            (post_key, POST_CONDENSATION_PROMPT_VERSION),
        )
        existing = cur.fetchone()
        parsed_output = existing.get("parsed_output") if existing else None
        if isinstance(parsed_output, dict):
            user_payload = existing.get("user_payload") if isinstance(existing.get("user_payload"), dict) else {}
            source_fingerprint = user_payload.get("post_fingerprint") if isinstance(user_payload, dict) else None
            if not isinstance(source_fingerprint, dict) or _sha(source_fingerprint) == fingerprint_hash:
                stored = _store_post_condensation(
                    conn,
                    post_key=post_key,
                    fingerprint=fingerprint,
                    condensation=parsed_output,
                    model_call_id=existing.get("id"),
                )
                if stored:
                    return stored

    recovered = _recover_post_condensation_from_stored_raw(conn, post_key=post_key, fingerprint=fingerprint)
    if recovered:
        return recovered
    try:
        conn.commit()
    except Exception:
        pass

    result = _call_post_condensation_model(fingerprint, post_key=post_key)
    if not result:
        return None
    raw_output, parsed, call_error = result
    if parsed is None:
        _record_post_condensation_model_call(
            conn,
            post_key=post_key,
            fingerprint=fingerprint,
            raw_output=raw_output,
            parsed_output=None,
            status="failed",
            error=call_error or "post condensation output did not parse",
        )
        if call_error:
            print(f"[post_condensation] model call failed: {call_error}")
        return None

    model_call_id = _record_post_condensation_model_call(
        conn,
        post_key=post_key,
        fingerprint=fingerprint,
        raw_output=raw_output,
        parsed_output=parsed,
        status="complete",
        error=None,
    )
    return _store_post_condensation(
        conn,
        post_key=post_key,
        fingerprint=fingerprint,
        condensation=parsed,
        model_call_id=model_call_id,
    )


def _condensed_post_payload(condensation: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(condensation, dict):
        return None
    body = _post_condensation_body(condensation)
    if not isinstance(body, dict):
        return None
    reel = str(body.get("reel") or "").strip()
    if not reel:
        return None
    return {
        "post_key": str(body.get("post_key") or "").strip(),
        "post_condensed": {
            "caption": str(body.get("caption") or "").strip(),
            "reel": reel,
            "standout_details": [str(item or "").strip() for item in body.get("standout_details") or [] if str(item or "").strip()]
            if isinstance(body.get("standout_details"), list)
            else [],
            "meta": body.get("meta") if isinstance(body.get("meta"), dict) else {},
        },
    }


def _d7_mult(row: dict[str, Any], axis: str) -> float | None:
    value = row.get(f"{axis}_multiple")
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _d7_metric(row: dict[str, Any], axis: str) -> int | float | None:
    value = row.get(axis)
    if value is not None:
        return value
    if axis == "comments" and (row.get("views") is not None or row.get("likes") is not None):
        return 0
    return None


def _d7_posted_ts(row: dict[str, Any]) -> float:
    value = row.get("posted_at")
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time()).timestamp()
    return 0.0


def _d7_posted_on(row: dict[str, Any]) -> str | None:
    posted_at = row.get("posted_at")
    if isinstance(posted_at, datetime):
        return posted_at.date().isoformat()
    if isinstance(posted_at, date):
        return posted_at.isoformat()
    business_date = row.get("business_date_ist")
    if business_date is not None:
        try:
            return business_date.isoformat()
        except AttributeError:
            return str(business_date)
    return None


def _d7_scene_body(condensation: Any) -> dict[str, Any] | None:
    payload = _condensed_post_payload(condensation if isinstance(condensation, dict) else None)
    if not payload:
        return None
    body = payload.get("post_condensed")
    return body if isinstance(body, dict) else None


def _d7_vs_usual(row: dict[str, Any]) -> dict[str, float | None]:
    """One post's metrics as a multiple of the account's 90-day usual, per axis."""
    out: dict[str, float | None] = {}
    for axis in ("views", "likes", "comments"):
        mult = _d7_mult(row, axis)
        out[axis] = round(mult, 1) if mult is not None else None
    return out


# --- D7 read payload: the "now" card ---
# The read sees the prior feeder file (current reality) + worker-computed
# triggers for the post that just hit D7. The trigger stays outside the feeder
# file, so the 31st post is compared against 30 prior posts.
_D7_MIN_RECENT = int(os.getenv("D7_READ_MIN_RECENT", "10"))      # prior posts below this -> no read yet
_D7_MEMORY_SIZE = int(os.getenv("D7_READ_MEMORY_SIZE", "30"))    # prior posts in the feeder file
_D7_RECENT_RUN_SCENES = int(os.getenv("D7_READ_RUN_SCENES", "30"))  # prior condensations fed for "the now"
_D7_CONCENTRATION_BAR = float(os.getenv("D7_READ_CONCENTRATION_BAR", "0.4"))  # one post carries the run


def _d7_is_num(value: Any) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def _d7_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _d7_median_mult(rows: list[dict[str, Any]], axis: str) -> float | None:
    vals = [m for m in (_d7_mult(r, axis) for r in rows) if m is not None]
    return round(statistics.median(vals), 2) if vals else None


def _d7_avg(*values: float | None) -> float | None:
    present = [v for v in values if v is not None]
    return sum(present) / len(present) if present else None


def _d7_momentum(recent_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Trailing windows (newest first) as median multiples of normal, per axis,
    plus a coarse trajectory pointer the read interprets (never prints)."""
    out: dict[str, Any] = {}
    for window in (5, 10, 15, 30):
        sub = recent_rows[:window]
        out[f"last_{window}"] = {
            axis: _d7_median_mult(sub, axis) for axis in ("views", "likes", "comments")
        }

    def _direction(fresh: float | None, broad: float | None) -> str:
        if fresh is None or broad is None or broad == 0:
            return "unknown"
        ratio = fresh / broad
        if ratio >= 1.15:
            return "up"
        if ratio <= 0.85:
            return "down"
        return "steady"

    reach = _direction(out["last_5"]["views"], out["last_30"]["views"])
    fresh_eng = _d7_avg(out["last_5"]["likes"], out["last_5"]["comments"])
    broad_eng = _d7_avg(out["last_30"]["likes"], out["last_30"]["comments"])
    out["trajectory"] = f"reach_{reach}_engagement_{_direction(fresh_eng, broad_eng)}"
    return out


def _d7_concentration(recent_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """How much of the run's whole reach a single post carries."""
    views = [float(v) for v in (_d7_metric(r, "views") for r in recent_rows) if _d7_is_num(v)]
    total = sum(views)
    if total <= 0 or not views:
        return {"top_post_share_views": None, "carried_by_few": False}
    share = round(max(views) / total, 2)
    return {"top_post_share_views": share, "carried_by_few": share >= _D7_CONCENTRATION_BAR}


def _d7_splits(recent_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Performance cut by collab vs organic inside the working memory."""
    collab = [r for r in recent_rows if bool(r.get("collab_post"))]
    organic = [r for r in recent_rows if not bool(r.get("collab_post"))]

    def _cut(rows: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "n": len(rows),
            "views": _d7_median_mult(rows, "views"),
            "comments": _d7_median_mult(rows, "comments"),
        }

    return {"collab": _cut(collab), "organic": _cut(organic)}


def _d7_place_in_memory(trigger: dict[str, Any], others: list[dict[str, Any]]) -> dict[str, Any]:
    """Where this post ranks inside the working memory, per axis. Deterministic so
    the read never miscounts what it beat."""
    beats: dict[str, int | None] = {}
    for axis in ("views", "likes", "comments"):
        trigger_value = _d7_metric(trigger, axis)
        if not _d7_is_num(trigger_value):
            beats[axis] = None
            continue
        trigger_num = float(trigger_value)
        comparable = [float(v) for v in (_d7_metric(r, axis) for r in others) if _d7_is_num(v)]
        beats[axis] = sum(1 for v in comparable if trigger_num > v)
    of = len(others) + 1
    rank_views = (of - beats["views"]) if beats.get("views") is not None else None
    return {"of": of, "beat": beats, "rank_views": rank_views}


# --- D7 fun_fact: one grounded, screenshot-worthy stat, worker-computed ---
# Never LLM-invented. The worker proves it from the data, the frontend renders it.
# Priority: rarity -> d1/d7 swing -> top record -> record-since -> streak -> velocity -> beat-last-X.
# Account-specific facts (records, streaks) outrank velocity: front-loading is
# near-universal for reels, so it only earns the box when it's genuinely extreme
# AND nothing account-grounded cleared. Otherwise it reads as generic trivia.
_D7_VELOCITY_FRONTLOAD = float(os.getenv("D7_FUNFACT_FRONTLOAD", "0.93"))   # share of d7 views by d1
_D7_VELOCITY_SLOWBURN = float(os.getenv("D7_FUNFACT_SLOWBURN", "1.12"))     # d7 / d3 still climbing
_D7_RECORD_MIN_DAYS = int(os.getenv("D7_FUNFACT_RECORD_DAYS", "21"))        # "best in N" needs >= this
_D7_RECORD_TOP_RANK = max(1, int(os.getenv("D7_FUNFACT_RECORD_TOP_RANK", "5")))
_D7_STREAK_MIN = int(os.getenv("D7_FUNFACT_STREAK_MIN", "3"))
_D7_STREAK_UP = float(os.getenv("D7_FUNFACT_STREAK_UP", "1.15"))
_D7_STREAK_DOWN = float(os.getenv("D7_FUNFACT_STREAK_DOWN", "0.85"))

_D7_AXIS_PAST = {"views": "watched", "likes": "liked", "comments": "talked-about"}


def _d7_weeks_phrase(days: int) -> str:
    if days >= 75:
        return "nearly three months"
    if days >= 55:
        return "two months"
    if days >= 40:
        return "six weeks"
    if days >= 26:
        return "a month"
    if days >= 18:
        return "three weeks"
    return f"{days} days"


def _d7_velocity_fact(checkpoints: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    d1 = checkpoints.get("d1") or {}
    d3 = checkpoints.get("d3") or {}
    d7 = checkpoints.get("d7") or {}
    v1, v3, v7 = d1.get("views"), d3.get("views"), d7.get("views")
    if not _d7_is_num(v1) or not _d7_is_num(v7) or float(v7) <= 0:
        return None
    share = float(v1) / float(v7)
    if share >= _D7_VELOCITY_FRONTLOAD:
        pct = round(share * 100)
        return {
            "kind": "velocity",
            "subtype": "front_loaded",
            "text": f"It front-loaded hard — {pct}% of its views came in the first 24 hours, "
                    f"and it had all but stopped climbing by day three.",
        }
    if _d7_is_num(v3) and float(v3) > 0 and float(v7) > float(v3) * _D7_VELOCITY_SLOWBURN:
        return {
            "kind": "velocity",
            "subtype": "slow_burn",
            "text": "A slow burn — it was still gathering views a week in, long after most posts go quiet.",
        }
    return None


_D7_RECORD_MIN_HISTORY = int(os.getenv("D7_FUNFACT_RECORD_HISTORY", "15"))
_D7_PCT_SWING = float(os.getenv("D7_FUNFACT_PCT_SWING", "20"))  # >= this move (either way), d1 -> d7


def _d7_pct(checkpoint: dict[str, Any]) -> float | None:
    value = checkpoint.get("ppe")
    if value is None:
        value = checkpoint.get("pp")
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _d7_pct_bucket(p: float) -> str:
    if p <= 15:
        return "the front of their pack"
    if p <= 35:
        return "the top third"
    if p <= 65:
        return "the middle of the pack"
    return "the back of the pack"


def _d7_percentile_swing_fact(checkpoints: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    """A large move in account-relative standing, d1 -> d7 (lower == stronger).
    Rise = late climber; drop = opened strong then faded. No landing requirement."""
    p1 = _d7_pct(checkpoints.get("d1") or {})
    p7 = _d7_pct(checkpoints.get("d7") or {})
    if p1 is None or p7 is None:
        return None
    delta = p1 - p7  # positive == got stronger
    if abs(delta) < _D7_PCT_SWING:
        return None
    rise = delta > 0
    sb, eb = _d7_pct_bucket(p1), _d7_pct_bucket(p7)
    if sb == eb:
        text = (
            "A late surge — it kept climbing the account's order after day one, "
            "overtaking a run of posts through the week."
        ) if rise else (
            "A fast fade — it slid down the account's order after day one, "
            "passed by a run of posts through the week."
        )
    elif rise:
        text = (f"A late climber — it kept gaining through the week, rising from {sb} "
                f"to {eb} between day one and day seven.")
    else:
        text = (f"It opened strong and faded — sliding from {sb} to {eb} between day one and "
                f"day seven. A fast burn that didn't hold.")
    return {"kind": "percentile_swing", "subtype": "rise" if rise else "drop",
            "from": round(p1), "to": round(p7), "text": text}


def _d7_record_fact(trigger: dict[str, Any], rows_90d: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Top-N 90d result on any axis - needs enough history to mean it."""
    if len(rows_90d) < _D7_RECORD_MIN_HISTORY:
        return None
    best: dict[str, Any] | None = None
    best_rank = _D7_RECORD_TOP_RANK + 1
    for axis in ("likes", "views", "comments"):
        tv = _d7_metric(trigger, axis)
        if not _d7_is_num(tv):
            continue
        tv = float(tv)
        better = [
            r for r in rows_90d
            if str(r.get("post_key") or "") != str(trigger.get("post_key") or "")
            and _d7_is_num(_d7_metric(r, axis)) and float(_d7_metric(r, axis)) > tv
        ]
        rank = len(better) + 1
        if rank > _D7_RECORD_TOP_RANK or rank >= best_rank:
            continue
        best_rank = rank
        if rank == 1:
            best = {"kind": "record", "subtype": "best", "axis": axis, "rank": rank,
                    "text": f"Their most-{_D7_AXIS_PAST[axis]} reel in three months."}
        else:
            best = {
                "kind": "record",
                "subtype": "top",
                "axis": axis,
                "rank": rank,
                "top": _D7_RECORD_TOP_RANK,
                "text": f"A top-{_D7_RECORD_TOP_RANK} {_D7_AXIS_PAST[axis]} reel in three months.",
            }
    return best


def _d7_record_since_fact(trigger: dict[str, Any], rows_90d: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Best on an axis vs a gap of >= _D7_RECORD_MIN_DAYS (not an all-time record)."""
    trigger_ts = _d7_posted_ts(trigger)
    best: dict[str, Any] | None = None
    best_score = -1.0
    for axis in ("likes", "views", "comments"):
        tv = _d7_metric(trigger, axis)
        if not _d7_is_num(tv):
            continue
        tv = float(tv)
        better = [
            r for r in rows_90d
            if str(r.get("post_key") or "") != str(trigger.get("post_key") or "")
            and _d7_is_num(_d7_metric(r, axis)) and float(_d7_metric(r, axis)) > tv
        ]
        if not better:
            continue  # top-record cases are handled by _d7_record_fact
        last_beat = max(_d7_posted_ts(r) for r in better)
        days = int(max(0.0, (trigger_ts - last_beat) / 86400.0))
        if days < _D7_RECORD_MIN_DAYS:
            continue
        rank = len(better) + 1
        score = days + max(0, 12 - rank) * 2.5
        if score > best_score:
            best_score = score
            best = {"kind": "record_since", "axis": axis, "rank": rank, "days": days,
                    "text": f"Their most-{_D7_AXIS_PAST[axis]} reel in {_d7_weeks_phrase(days)}."}
    return best


def _d7_streak_fact(recent_ordered: list[dict[str, Any]]) -> dict[str, Any] | None:
    def state(row: dict[str, Any]) -> str | None:
        m = _d7_mult(row, "views")
        if m is None:
            return None
        if m >= _D7_STREAK_UP:
            return "up"
        if m <= _D7_STREAK_DOWN:
            return "down"
        return "mid"
    if not recent_ordered:
        return None
    s0 = state(recent_ordered[0])
    if s0 not in ("up", "down"):
        return None
    streak = 1
    for row in recent_ordered[1:]:
        if state(row) == s0:
            streak += 1
        else:
            break
    if streak < _D7_STREAK_MIN:
        return None
    if s0 == "up":
        text = f"{streak} reels in a row above their usual now — a genuine hot streak."
    else:
        text = f"{streak} in a row under their usual — a real cold stretch."
    return {"kind": "streak", "subtype": s0, "len": streak, "text": text}


def _d7_rarity_fact(trigger: dict[str, Any], rows_90d: list[dict[str, Any]]) -> dict[str, Any] | None:
    tc, tl = _d7_metric(trigger, "comments"), _d7_metric(trigger, "likes")
    if _d7_is_num(tc) and _d7_is_num(tl) and float(tc) > float(tl):
        n = len(rows_90d)
        cnt = sum(
            1 for r in rows_90d
            if _d7_is_num(_d7_metric(r, "comments")) and _d7_is_num(_d7_metric(r, "likes"))
            and float(_d7_metric(r, "comments")) > float(_d7_metric(r, "likes"))
        )
        if cnt <= max(2, n * 0.1):
            return {"kind": "rarity", "subtype": "comments_over_likes",
                    "text": "More comments than likes — almost unheard of here. People came to talk, not just tap."}
    return None


def _d7_placement_fact(place: dict[str, Any]) -> dict[str, Any] | None:
    beat = place.get("beat") or {}
    of = max(0, int(place.get("of", 1)) - 1)
    axis = max(("views", "likes", "comments"), key=lambda a: beat.get(a) if beat.get(a) is not None else -1)
    b = beat.get(axis)
    if b is None or of <= 0:
        return None
    return {"kind": "placement", "axis": axis, "beat": b, "of": of,
            "text": f"Beat {b} of the last {of} on {axis}."}


def _d7_fun_fact(conn: Any, post_key: str) -> dict[str, Any] | None:
    """One grounded fun-fact for the card's box. Always returns at least a
    beat-last-X placement when metrics exist."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select p.feeder_id, p.media_type, p.posted_at,
                   pm.views, pm.likes, pm.comments,
                   pm.views_multiple, pm.likes_multiple, pm.comments_multiple
            from public.posts p
            join public.post_metrics pm
              on pm.post_key = p.post_key and lower(pm.checkpoint) = 'd7'
            where p.post_key = %s
            order by pm.computed_at desc nulls last
            limit 1
            """,
            (post_key,),
        )
        trigger = cur.fetchone()
        if not trigger:
            return None
        trigger = dict(trigger)
        trigger["post_key"] = post_key

        cur.execute(
            "select lower(checkpoint) ck, views, likes, comments, "
            "percentile_performance_exact ppe, percentile_performance pp "
            "from public.post_metrics where post_key = %s",
            (post_key,),
        )
        checkpoints = {
            str(r["ck"]): {
                "views": r["views"], "likes": r["likes"], "comments": r["comments"],
                "ppe": r["ppe"], "pp": r["pp"],
            }
            for r in cur.fetchall()
        }

        feeder_id = int(trigger["feeder_id"])
        media_type = str(trigger.get("media_type") or "reel").strip().lower()
        cur.execute(
            """
            select p.post_key, p.posted_at,
                   pm.views, pm.likes, pm.comments,
                   pm.views_multiple, pm.likes_multiple, pm.comments_multiple
            from public.posts p
            join public.post_metrics pm
              on pm.post_key = p.post_key and lower(pm.checkpoint) = 'd7'
            where p.feeder_id = %s and lower(coalesce(p.media_type, '')) = %s
              and p.posted_at >= %s::timestamptz - interval '90 days'
              and p.posted_at <= %s::timestamptz
            order by p.posted_at desc nulls last
            """,
            (feeder_id, media_type, trigger.get("posted_at"), trigger.get("posted_at")),
        )
        rows_90d = [dict(r) for r in cur.fetchall()]

    if not any(str(r.get("post_key") or "") == post_key for r in rows_90d):
        rows_90d.append(trigger)
    rows_90d.sort(key=_d7_posted_ts, reverse=True)

    trigger_ts = _d7_posted_ts(trigger)
    prior_rows = [
        r for r in rows_90d
        if str(r.get("post_key") or "") != post_key and _d7_posted_ts(r) < trigger_ts
    ]
    feeder_rows = prior_rows[:_D7_MEMORY_SIZE]
    place = _d7_place_in_memory(trigger, feeder_rows)
    history_rows = [trigger] + prior_rows

    # Fixed ladder, first that clears its bar wins. Account-specific facts come
    # before velocity (front-load): a record or streak is more screenshot-worthy
    # than near-universal front-loading, which now only wins when extreme.
    # rarity -> percentile swing -> record -> record-since -> streak -> velocity -> floor.
    return (
        _d7_rarity_fact(trigger, history_rows)
        or _d7_percentile_swing_fact(checkpoints)
        or _d7_record_fact(trigger, history_rows)
        or _d7_record_since_fact(trigger, history_rows)
        or _d7_streak_fact([trigger] + feeder_rows)
        or _d7_velocity_fact(checkpoints)
        or _d7_placement_fact(place)
    )


def _d7_metric_shape(views: Any, likes: Any, comments: Any) -> str:
    v = _d7_float(views) or 0.0
    l = _d7_float(likes) or 0.0
    c = _d7_float(comments) or 0.0
    vals = [v, l, c]
    if max(vals) < 0.9:
        return "soft across the board"
    if min(vals) >= 1.2 and max(vals) <= min(vals) * 1.35:
        return "balanced lift"
    if c >= max(v, l) * 1.3 and c >= 1.2:
        return "comments over-indexed"
    if l >= max(v, c) * 1.3 and l >= 1.2:
        return "likes over-indexed"
    if v >= max(l, c) * 1.3 and v >= 1.2:
        return "views carried it, engagement flat"
    if v >= 1.2 and l < 1.0:
        return "views carried it, likes flat"
    if max(vals) >= 1.2:
        return "uneven lift"
    return "dead normal"


def _d7_band_from_percentile(value: Any) -> str:
    pct = _d7_float(value)
    if pct is None:
        return "normal"
    if pct <= 25:
        return "hot"
    if pct >= 75:
        return "cold"
    return "normal"


def _d7_text_blob(*values: Any) -> str:
    parts: list[str] = []

    def add(value: Any) -> None:
        if isinstance(value, dict):
            for child in value.values():
                add(child)
        elif isinstance(value, list):
            for child in value:
                add(child)
        elif value is not None:
            parts.append(str(value))

    for value in values:
        add(value)
    return " ".join(parts).lower()


def _active_cold_start_feeder_file(conn: Any, *, feeder_id: int, handle: str) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select feed_file
            from public.feeder_files
            where feeder_id = %s
              and lower(feeder_handle) = lower(%s)
              and compile_version = any(%s)
              and status = 'active'
            order by updated_at desc nulls last, id desc
            limit 1
            """,
            (feeder_id, handle, list(ACTIVE_COLD_START_COMPILE_VERSIONS)),
        )
        row = cur.fetchone()
    feed_file = row.get("feed_file") if row else None
    return feed_file if isinstance(feed_file, dict) else None


def _d7_feeder_file_without_target(feed_file: dict[str, Any], post_key: str) -> dict[str, Any]:
    """Avoid self-reference when a target post is already present in memory."""
    out = copy.deepcopy(feed_file)
    if isinstance(out.get("posts"), list):
        out["posts"] = [post for post in out["posts"] if str((post or {}).get("post_key") or "") != post_key]
    alias_map = out.get("source_alias_map") if isinstance(out.get("source_alias_map"), dict) else {}
    target_aliases = {alias for alias, key in alias_map.items() if str(key) == post_key}
    if not target_aliases:
        return out
    for bite in out.get("bites", []) if isinstance(out.get("bites"), list) else []:
        receipts = bite.get("receipts")
        if not isinstance(receipts, list):
            continue
        bite["receipts"] = [r for r in receipts if str((r or {}).get("post")) not in target_aliases]
        bite["n_current_window"] = len(bite["receipts"])
    return out


_D7_REEL_CARD_FIELDS = (
    "summary",
    "aim",
    "aim_receipt",
    "proof",
    "proof_receipt",
    "open",
    "close",
    "package",
    "package_receipt",
)


def _d7_payload_time(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _d7_rank_landing(rank: int | None, pool: int) -> tuple[str, str]:
    if rank is None or pool <= 0:
        return "unknown", "unknown"
    pct = rank / pool
    if pct <= 0.25:
        return "overdelivered", "deep feeder hit"
    if pct <= 0.625:
        return "held", "useful middle"
    if pct <= 0.875:
        return "underdelivered", "soft, did not move the run"
    return "underdelivered", "low, missed the feeder"


def _d7_anomalies(row: dict[str, Any]) -> list[str]:
    shape = _d7_metric_shape(
        row.get("views_multiple"),
        row.get("likes_multiple"),
        row.get("comments_multiple"),
    )
    return [shape] if "over-indexed" in shape else []


def _d7_payload_performance(row: dict[str, Any], *, rank: int | None, pool: int) -> dict[str, Any]:
    job, landing = _d7_rank_landing(rank, pool)
    return {
        "job": job,
        "rank": f"{rank}/{pool}" if rank is not None and pool > 0 else "",
        "landing": landing,
        "anomalies": _d7_anomalies(row),
    }


def _d7_clean_performance(value: Any, row: dict[str, Any], *, rank: int | None, pool: int) -> dict[str, Any]:
    computed = _d7_payload_performance(row, rank=rank, pool=pool)
    if not isinstance(value, dict):
        return computed
    anomalies = value.get("anomalies")
    return {
        "job": str(value.get("job") or computed["job"]),
        "rank": str(value.get("rank") or computed["rank"]),
        "landing": str(value.get("landing") or computed["landing"]),
        "anomalies": [str(item) for item in anomalies if str(item).strip()] if isinstance(anomalies, list) else computed["anomalies"],
    }


def _d7_reel_card(value: Any) -> dict[str, str] | None:
    source = value.get("card") if isinstance(value, dict) and isinstance(value.get("card"), dict) else value
    if not isinstance(source, dict):
        return None
    card = {field: str(source.get(field) or "").strip() for field in _D7_REEL_CARD_FIELDS}
    aliases = {
        "summary": "what_happened",
        "aim": "job",
        "aim_receipt": "job_basis",
        "proof": "driver",
        "proof_receipt": "driver_basis",
        "package": "form",
        "package_receipt": "form_basis",
    }
    for field, alias in aliases.items():
        if not card[field]:
            card[field] = str(source.get(alias) or "").strip()
    if not all(card[field] for field in ("summary", "aim", "proof", "open", "close", "package")):
        return None
    return card


def _d7_numbered_feeder_posts(posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    kept = posts[-FEEDER_FILE_MAX_REEL_CARDS:]
    for idx, post in enumerate(kept, start=1):
        post["id"] = f"m{idx:02d}"
    return kept


def _d7_feeder_posts_from_file(
    feed_file: dict[str, Any] | None,
    *,
    post_key: str,
    rows_by_key: dict[str, dict[str, Any]],
    rank_lookup: dict[str, int],
    pool: int,
) -> list[dict[str, Any]]:
    raw_posts = feed_file.get("posts") if isinstance(feed_file, dict) and isinstance(feed_file.get("posts"), list) else []
    posts: list[dict[str, Any]] = []
    for item in raw_posts:
        if not isinstance(item, dict):
            continue
        key = str(item.get("post_key") or "").strip()
        key_norm = key.lower()
        if not key_norm or key_norm == post_key.lower():
            continue
        card = _d7_reel_card(item)
        if not card:
            continue
        row = rows_by_key.get(key_norm, {})
        posts.append(
            {
                "id": "",
                "url": item.get("url") or item.get("post_url") or row.get("post_url"),
                "card": card,
                "post_key": key or row.get("post_key"),
                "posted_at": _d7_payload_time(item.get("posted_at") or row.get("posted_at")),
                "performance": _d7_clean_performance(
                    item.get("performance"),
                    row,
                    rank=rank_lookup.get(key_norm),
                    pool=pool,
                ),
            }
        )
    return _d7_numbered_feeder_posts(posts)


def _d7_bite_match_context(feed_file: dict[str, Any], fingerprint: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    bites = feed_file.get("bites") if isinstance(feed_file.get("bites"), list) else []
    available_ids = {str(bite.get("bite_id") or "") for bite in bites if isinstance(bite, dict)}
    blob = _d7_text_blob(
        fingerprint.get("caption"),
        fingerprint.get("visible_text"),
        fingerprint.get("visual_sequence"),
        fingerprint.get("audio_behavior"),
        fingerprint.get("cultural_references"),
        fingerprint.get("edit_and_pacing"),
        fingerprint.get("environment_and_entities"),
        fingerprint.get("observed_alignments"),
        fingerprint.get("notable_observed_details"),
    )

    matched_ids: set[str] = set()
    clipped_ids: set[str] = set()
    role: dict[str, str] = {}

    def has_bite(bite_id: str) -> bool:
        return bite_id in available_ids

    if ("clubhouse" in blob or "audio-room" in blob or "profile bubble" in blob) and ("red arrow" in blob or "speaker" in blob):
        matched_ids.add("b_clubhouse_ui_debate")
        role["b_clubhouse_ui_debate"] = "Simulated audio-room UI and speaker switching are central to the reel."
    if ("black-and-white" in blob or "b&w" in blob or "monochrome" in blob) and any(term in blob for term in ("sad", "dramatic", "parody", "melancholic", "violin")):
        matched_ids.add("b_bw_filter_dramatic_beat")
        role["b_bw_filter_dramatic_beat"] = "Black-and-white treatment marks the dramatic or parody beat."
    elif "black-and-white" in blob or "b&w" in blob or "monochrome" in blob:
        clipped_ids.add("b_bw_filter_dramatic_beat")
        role["b_bw_filter_dramatic_beat"] = "Black-and-white appears, but not as the full dramatic/parody carrier from prior examples."
    if ("abrupt" in blob or "hard cut" in blob or "cuts to black" in blob) and any(term in blob for term in ("final word", "final line", "no outro", "no cta", "cuts abruptly")):
        matched_ids.add("b_abrupt_hard_cut_close")
        role["b_abrupt_hard_cut_close"] = "The reel exits on a hard final-word/no-outro cut."
    elif "logo" in blob and ("ends" in blob or "outro" in blob):
        clipped_ids.add("b_abrupt_hard_cut_close")
        role["b_abrupt_hard_cut_close"] = "The reel closes on a logo/outro instead of the usual hard final-word cut."
    if ("top text" in blob or "text overlay" in blob or "visible_text" in blob) and any(term in blob for term in ("0:00", "first frame", "opens", "premise", "pov")):
        matched_ids.add("b_cold_start_text_premise")
        role["b_cold_start_text_premise"] = "A cold opening text/premise frames the reel before the first real beat."
    if any(term in blob for term in ("single take", "gaze", "two characters", "both characters", "plays both", "dual character")):
        matched_ids.add("b_single_take_dual_character")
        role["b_single_take_dual_character"] = "One continuous-feel performance distinguishes characters through gaze, voice, or body language."
    if any(term in blob for term in ("bollywood", "sarabhai", "ta ra rum pum", "jab we met", "bombay (1995)", "film", "movie")):
        matched_ids.add("b_bollywood_trope_reference")
        role["b_bollywood_trope_reference"] = "A named film/show/Bollywood cue carries the joke or tonal turn."
    if any(term in blob for term in ("bmw", "versace", "cash", "balcony", "wine toast", "luxury")) and "montage" in blob:
        matched_ids.add("b_lifestyle_flex_montage")
        role["b_lifestyle_flex_montage"] = "A fast-cut luxury/flex montage is central to the reel."
    if "versace" in blob and "bathrobe" in blob:
        matched_ids.add("b_versace_bathrobe_persona")
        role["b_versace_bathrobe_persona"] = "The black Versace bathrobe persona is present in the execution."
    if "logo" in blob and "opens" in blob and "ends" in blob:
        matched_ids.add("b_brand_logo_bookend")
        role["b_brand_logo_bookend"] = "A brand/logo card bookends the execution."
    elif "logo" in blob or "outro" in blob:
        clipped_ids.add("b_brand_logo_bookend")
        role["b_brand_logo_bookend"] = "A logo/outro appears, but not as a full brand-bookend execution."

    # Lakme cold-start v7 bite memory.
    eyeconic_terms = any(
        term in blob
        for term in ("eyeconic", "lakme 9to5", "lakmē 9to5", "9to5", "eyeliner", "sharp wings", "kajal")
    )
    sorbet_terms = any(
        term in blob
        for term in ("sorbet", "lakme skin", "lakmē skin", "icy", "peach milk", "ice dunk")
    )
    teal_card = (
        any(term in blob for term in ("teal metallic card", "solid teal", "teal screen", "teal card", "metallic background"))
        and any(term in blob for term in ("logo", "end card", "hard cut", "sweeps in"))
    ) or ("female vo" in blob and ("lakme 9to5" in blob or "lakmē 9to5" in blob))
    if has_bite("b_teal_metallic_end_card"):
        if teal_card:
            matched_ids.add("b_teal_metallic_end_card")
            role["b_teal_metallic_end_card"] = "The Eyeconic/9to5 reel uses the teal metallic logo card as the closing brand stamp."
        elif eyeconic_terms:
            clipped_ids.add("b_teal_metallic_end_card")
            role["b_teal_metallic_end_card"] = "The Eyeconic/9to5 product is present, but the usual teal metallic logo-card close is missing."

    no_voiceover = any(term in blob for term in ("no spoken dialogue", "no voiceover", "no spoken voiceover"))
    if has_bite("b_brand_name_vo_signoff"):
        if (
            not no_voiceover
            and ("female vo" in blob or "voiceover" in blob)
            and ("lakme 9to5" in blob or "lakmē 9to5" in blob)
        ):
            matched_ids.add("b_brand_name_vo_signoff")
            role["b_brand_name_vo_signoff"] = "A minimal female voiceover speaks the Lakme 9to5 name over the sign-off."
        elif eyeconic_terms:
            clipped_ids.add("b_brand_name_vo_signoff")
            role["b_brand_name_vo_signoff"] = "The Eyeconic/9to5 product appears without the usual spoken Lakme 9to5 sign-off."

    if has_bite("b_white_logo_end_card") and (
        ("hard cut" in blob and "white screen" in blob and "lakm" in blob and "logo" in blob)
        or "white lakmē" in blob
        or "white lakme" in blob
    ):
        matched_ids.add("b_white_logo_end_card")
        role["b_white_logo_end_card"] = "The reel closes on the clean white LAKMĒ logo card."

    if has_bite("b_claim_proved_by_demo") and any(
        term in blob
        for term in (
            "smudgeproof",
            "waterproof",
            "transferproof",
            "water spray",
            "tissue press",
            "cotton pad",
            "finger rub",
            "swipe on hand",
            "swatched on hand",
            "visible-tissue",
        )
    ):
        matched_ids.add("b_claim_proved_by_demo")
        role["b_claim_proved_by_demo"] = "A product claim is shown through a physical application, comparison, or stress-test demo."

    if has_bite("b_static_claim_text_list") and any(
        term in blob
        for term in ("it does not", "12-item", "claim cards", "typed letter-by-letter", "typewriter", "stacked on-screen text")
    ):
        matched_ids.add("b_static_claim_text_list")
        role["b_static_claim_text_list"] = "On-screen claim text carries the product benefits rather than a spoken explanation."

    if has_bite("b_cold_start_top_text_premise") and any(fingerprint.get("visible_text") or []):
        first_visual = _d7_text_blob((fingerprint.get("visual_sequence") or [])[:1])
        if "0:00" in first_visual or "opens" in blob or "cold open" in blob or "split-screen" in first_visual:
            matched_ids.add("b_cold_start_top_text_premise")
            role["b_cold_start_top_text_premise"] = "The reel opens straight into action with visible text framing the product premise."

    if has_bite("b_holographic_sorbet_jar") and "sorbet" in blob and any(
        term in blob for term in ("holographic", "reflective silver", "silver finish", "jar")
    ):
        matched_ids.add("b_holographic_sorbet_jar")
        role["b_holographic_sorbet_jar"] = "The holographic sorbet jar is a central product hero, tied to the cooling/sorbet visual."

    if has_bite("b_one_swipe_glow_up_claim") and any(
        term in blob for term in ("one swipe", "single swipe", "single product pass", "instant glow", "instantly hydrated")
    ):
        matched_ids.add("b_one_swipe_glow_up_claim")
        role["b_one_swipe_glow_up_claim"] = "A single application gesture carries the instant result claim."

    if has_bite("b_heatwave_spf_occasion_hook") and any(
        term in blob for term in ("heatwave", "el niño", "uv index", "national sunscreen day", "weather app", "spf reminder")
    ):
        matched_ids.add("b_heatwave_spf_occasion_hook")
        role["b_heatwave_spf_occasion_hook"] = "A real-world weather or SPF occasion frames the product need."

    if has_bite("b_mock_phone_ui_overlay") and any(
        term in blob
        for term in ("weather app", "ios", "notification", "calendar", "schedule", "mock ig", "instagram widget", "digital calendar")
    ):
        matched_ids.add("b_mock_phone_ui_overlay")
        role["b_mock_phone_ui_overlay"] = "A familiar phone or calendar-style interface frames the product moment."

    matched: list[dict[str, Any]] = []
    clipped: list[dict[str, Any]] = []
    absent: list[dict[str, Any]] = []
    for bite in bites:
        if not isinstance(bite, dict):
            continue
        bite_id = str(bite.get("bite_id") or "")
        entry = copy.deepcopy(bite)
        if bite_id in matched_ids:
            entry["role_in_post"] = role.get(bite_id, "")
            matched.append(entry)
        elif bite_id in clipped_ids:
            entry["role_in_post"] = role.get(bite_id, "")
            clipped.append(entry)
        elif str(bite.get("bite_status") or "") != "baseline":
            entry["role_in_post"] = "Not present in this reel's observed format."
            absent.append(entry)
    return {"matched_bites": matched, "clipped_bites": clipped, "absent_bites": absent[:6]}


def _build_d7_read_input(conn: Any, post_key: str) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select
              p.post_key,
              p.feeder_id,
              p.caption,
              p.media_type,
              p.posted_at,
              p.post_url,
              f.handle,
              pm.views,
              pm.likes,
              pm.comments,
              pm.views_multiple,
              pm.likes_multiple,
              pm.comments_multiple,
              pm.percentile_performance_exact,
              pm.percentile_performance,
              pm.business_date_ist
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
        )
        trigger = cur.fetchone()
    if not trigger:
        return None

    trigger = dict(trigger)
    fingerprint = ensure_post_fingerprint(conn, post_key)
    if not fingerprint:
        return None

    feeder_id = int(trigger["feeder_id"])
    handle = str(trigger.get("handle") or "").strip()
    feed_file = _active_cold_start_feeder_file(conn, feeder_id=feeder_id, handle=handle)
    if not feed_file:
        return None
    feed_file = _d7_feeder_file_without_target(feed_file, post_key)

    media_type = str(trigger.get("media_type") or "reel").strip().lower()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select
              p.post_key,
              p.posted_at,
              p.post_url,
              pm.views,
              pm.likes,
              pm.comments,
              pm.views_multiple,
              pm.likes_multiple,
              pm.comments_multiple,
              pm.percentile_performance_exact,
              pm.percentile_performance,
              pm.business_date_ist,
              pm.computed_at
            from public.posts p
            join public.post_metrics pm
              on pm.post_key = p.post_key
             and lower(pm.checkpoint) = 'd7'
            where p.feeder_id = %s
              and lower(coalesce(p.media_type, '')) = %s
              and p.posted_at >= %s::timestamptz - interval '90 days'
              and p.posted_at <= %s::timestamptz
            order by p.posted_at desc nulls last
            """,
            (feeder_id, media_type, trigger.get("posted_at"), trigger.get("posted_at")),
        )
        rows = [dict(row) for row in cur.fetchall()]

    if not any(str(row.get("post_key") or "") == post_key for row in rows):
        rows.append(dict(trigger))
    rows.sort(key=_d7_posted_ts, reverse=True)

    ranked_rows = sorted(
        rows,
        key=lambda row: (
            _d7_float(row.get("percentile_performance_exact")) is None,
            _d7_float(row.get("percentile_performance_exact"))
            if _d7_float(row.get("percentile_performance_exact")) is not None
            else 10_000.0,
            -(_d7_float(_d7_metric(row, "views")) or 0.0),
        ),
    )
    pool = max(1, len(ranked_rows))
    rank_lookup = {str(row.get("post_key") or "").lower(): index + 1 for index, row in enumerate(ranked_rows)}
    rows_by_key = {str(row.get("post_key") or "").lower(): row for row in rows}
    trigger_rank = rank_lookup.get(post_key.lower(), pool)
    feeder_posts = _d7_feeder_posts_from_file(
        feed_file,
        post_key=post_key,
        rows_by_key=rows_by_key,
        rank_lookup=rank_lookup,
        pool=pool,
    )
    if not feeder_posts:
        return None

    return {
        "account": {"handle": handle},
        "this_post": {
            "caption": trigger.get("caption"),
            "post_key": post_key,
            "post_url": trigger.get("post_url"),
            "posted_at": _d7_payload_time(trigger.get("posted_at")),
            "fingerprint": fingerprint,
            "performance": _d7_payload_performance(
                trigger,
                rank=trigger_rank,
                pool=pool,
            ),
        },
        "feeder_file": {"posts": feeder_posts},
    }


def ensure_d7_read(conn: Any, post_key: str) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select parsed_output
            from public.feeder_file_model_calls
            where call_type = 'd7_read'
              and post_key = %s
              and prompt_version = %s
              and status = 'complete'
              and parsed_output is not null
            order by updated_at desc nulls last, id desc
            limit 1
            """,
            (post_key, D7_READ_PROMPT_VERSION),
        )
        existing = cur.fetchone()
    parsed_output = existing.get("parsed_output") if existing else None
    if isinstance(parsed_output, dict):
        normalized = _normalize_d7_read_mapping(parsed_output, post_key=post_key)
        if normalized:
            return normalized

    user_payload = _build_d7_read_input(conn, post_key)
    if not user_payload:
        return None
    result = _call_d7_read_model(user_payload, post_key=post_key)
    if not result:
        return None
    raw_output, parsed, call_error = result
    if parsed is None:
        _record_d7_read_model_call(
            conn,
            post_key=post_key,
            user_payload=user_payload,
            raw_output=raw_output,
            parsed_output=None,
            status="failed",
            error=call_error or "d7 read output did not parse",
        )
        return None

    # The fun_fact box is worker-computed (grounded), then merged into the stored
    # output so the frontend can render it without the LLM inventing a stat.
    try:
        fun_fact = _d7_fun_fact(conn, post_key)
    except Exception as exc:  # never let the box break the read
        print(f"[d7_read] fun_fact failed: {exc}")
        fun_fact = None
    if fun_fact and isinstance(parsed.get("d7_read"), dict):
        parsed["d7_read"]["fun_fact"] = fun_fact

    _record_d7_read_model_call(
        conn,
        post_key=post_key,
        user_payload=user_payload,
        raw_output=raw_output,
        parsed_output=parsed,
        status="complete",
        error=None,
    )
    return parsed


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


def _storage_authenticated_object_url(bucket: str, path: str) -> str | None:
    if not SUPABASE_URL or not bucket or not path:
        return None
    return f"{SUPABASE_URL.rstrip('/')}/storage/v1/object/authenticated/{bucket}/{quote(path, safe='/')}"


def _storage_read_headers() -> dict[str, str] | None:
    if not SUPABASE_SERVICE_ROLE_KEY:
        return None
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    }


def _r2_client():
    global _R2_CLIENT
    if _R2_CLIENT is not None:
        return _R2_CLIENT
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise RuntimeError("R2 media reads require boto3 in the worker environment") from exc

    if not (R2_ENDPOINT_URL and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY):
        raise RuntimeError("Missing R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, or R2_SECRET_ACCESS_KEY")

    _R2_CLIENT = boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name=R2_REGION or "auto",
        config=Config(signature_version="s3v4"),
    )
    return _R2_CLIENT


def _r2_signed_object_url(bucket: str, path: str, expires_seconds: int = 900) -> str | None:
    clean_bucket = (bucket or "").strip()
    clean_path = (path or "").strip().lstrip("/")
    if not clean_bucket or not clean_path:
        return None
    return _r2_client().generate_presigned_url(
        "get_object",
        Params={"Bucket": clean_bucket, "Key": clean_path},
        ExpiresIn=max(60, min(3600, int(expires_seconds))),
    )


def _media_fetch_ref(row: dict[str, Any]) -> tuple[str | None, dict[str, str] | None]:
    storage_provider = str(row.get("storage_provider") or "").strip().lower() or "supabase"
    bucket = str(row.get("storage_bucket") or SUPABASE_MEDIA_BUCKET or "").strip()
    path = str(row.get("storage_path") or "").strip().lstrip("/")
    if storage_provider == "r2" and path:
        url = _r2_signed_object_url(bucket or R2_BUCKET, path)
        if url:
            return url, None
        public_base = str(MEDIA_PUBLIC_BASE_URL or "").strip().rstrip("/")
        if public_base:
            return f"{public_base}/{path}", None

    public_url = str(row.get("public_url") or "").strip()
    if public_url:
        return public_url, None

    if storage_provider == "supabase" and path:
        url = _storage_authenticated_object_url(bucket, path)
        headers = _storage_read_headers()
        if url and headers:
            return url, headers

    return None, None


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
            select asset_role, public_url, storage_path, storage_provider, storage_bucket
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
    video_fetch_headers: dict[str, str] | None = None
    for asset in assets:
        url, headers = _media_fetch_ref(asset)
        if not url:
            continue
        video_url = url
        video_fetch_headers = headers
        video_asset_role = str(asset.get("asset_role") or "").strip().lower()
        break

    return {
        **post,
        "video_url": video_url,
        "_video_asset_role": video_asset_role,
        "_video_fetch_headers": video_fetch_headers,
    }


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
    video_data = _fetch_bytes(
        post.get("video_url"),
        timeout=60,
        max_bytes=_VIDEO_UPLOAD_MAX_BYTES,
        headers=post.get("_video_fetch_headers") if isinstance(post.get("_video_fetch_headers"), dict) else None,
    )
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
    try:
        conn.commit()
    except Exception:
        pass

    caption = str(post.get("caption") or "")
    caption_hash = _sha(caption)
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
        "media_source_hash": _sha(""),
        "caption_hash": caption_hash,
        "sampling_policy_version": FINGERPRINT_SAMPLING_POLICY_VERSION,
        "user_text": user_text,
    }
    media_parts, media_hash, _confidence = _fingerprint_media_parts(post, provider)
    if not media_parts or media_hash == _sha(""):
        model_version = current_model_version()
        user_payload = {
            "post_key": post_key,
            "media_source_hash": media_hash,
            "caption_hash": caption_hash,
            "sampling_policy_version": FINGERPRINT_SAMPLING_POLICY_VERSION,
            "user_text": user_text,
        }
        _record_fingerprint_model_call(
            conn,
            post_key=post_key,
            model_version=model_version,
            user_payload=user_payload,
            raw_output="",
            parsed_output=None,
            status="failed",
            error="video_full media unavailable, too large, or fetch timed out",
        )
        print(f"[fingerprint] skipped post_key={post_key}: video_full media unavailable or fetch failed")
        return None

    model_version = current_model_version()
    user_payload["media_source_hash"] = media_hash
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
    try:
        conn.commit()
    except Exception:
        pass

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

    duration_seconds = post.get("duration_seconds")
    media_truncated = False
    try:
        media_truncated = float(duration_seconds) > _VIDEO_SAMPLE_SECONDS if duration_seconds is not None else False
    except Exception:
        media_truncated = False
    fingerprint["duration_seconds"] = fingerprint.get("duration_seconds", duration_seconds)
    fingerprint["media_truncated"] = bool(fingerprint.get("media_truncated", media_truncated))
    fingerprint["observed_window"] = str(
        fingerprint.get("observed_window") or ("0:00-2:00" if media_truncated else "")
    )

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
                pc.post_key as post_condensation_post_key,
                fmc_d7.post_key as d7_read_post_key
              from public.posts p
              join public.post_metrics pm
                on pm.post_key = p.post_key
               and lower(pm.checkpoint) = 'd7'
              left join public.post_fingerprints pf_high
                on pf_high.post_key = p.post_key
               and pf_high.media_confidence = 'high'
              left join public.post_condensations pc
                on pc.post_key = p.post_key
               and pc.condensation_version = %s
              left join public.feeder_file_model_calls fmc_d7
                on fmc_d7.post_key = p.post_key
               and fmc_d7.call_type = 'd7_read'
               and fmc_d7.prompt_version = %s
               and fmc_d7.status = 'complete'
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
                (
                  d7_percentile <= 25
                  or (recent_rank <= 10 and recent_performance_rank <= 2)
                )
                and (
                  high_fingerprint_post_key is null
                  or post_condensation_post_key is null
                  or d7_read_post_key is null
                )
              )
            order by d7_percentile asc nulls last, posted_at desc nulls last
            limit %s
            """,
            (
                POST_CONDENSATION_PROMPT_VERSION,
                D7_READ_PROMPT_VERSION,
                max(1, int(days)),
                feeder_id,
                feeder_id,
                max(1, int(limit)),
            ),
        )
        rows = cur.fetchall()
    return [str(row["post_key"]) for row in rows if row.get("post_key")]


def fingerprint_reels(conn: Any, *, feeder_id: int | None = None, limit: int = 10, days: int = 90) -> dict[str, Any]:
    post_keys = _candidate_post_keys(conn, feeder_id=feeder_id, limit=limit, days=days)
    resolved = 0
    condensations = 0
    d7_reads = 0
    missing_media = 0
    failed = 0
    for post_key in post_keys:
        try:
            fingerprint = _existing_high_fingerprint(conn, post_key) or ensure_post_fingerprint(conn, post_key)
            if fingerprint:
                resolved += 1
                condensation = ensure_post_condensation(conn, post_key, fingerprint)
                if condensation:
                    condensations += 1
                    d7_read = ensure_d7_read(conn, post_key)
                    if d7_read:
                        d7_reads += 1
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
        "post_condensations": condensations,
        "d7_reads": d7_reads,
        "missing_media": missing_media,
        "failed": failed,
        "post_keys": post_keys,
        "model_version": current_model_version(),
        "sampling_policy_version": FINGERPRINT_SAMPLING_POLICY_VERSION,
    }
