from __future__ import annotations

import base64
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
    D7_READ_PROMPT_VERSION,
    D7_READ_SYSTEM_V6,
    FINGERPRINT_EXTRACTION_SYSTEM_V8,
    FINGERPRINT_PROMPT_VERSION,
    FINGERPRINT_SAMPLING_POLICY_VERSION,
    POST_CONDENSATION_PROMPT_VERSION,
    POST_CONDENSATION_SYSTEM_V5,
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
_POST_CONDENSATION_MAX_TOKENS = int(os.getenv("POST_CONDENSATION_MAX_TOKENS", "2600"))
_POST_CONDENSATION_WORD_TOLERANCE = float(os.getenv("POST_CONDENSATION_WORD_TOLERANCE", "0.15"))
_D7_READ_MAX_TOKENS = int(os.getenv("D7_READ_MAX_TOKENS", "1800"))
# Active D7 read prompt + the knobs that shape
# the feeder file the read sees.
_D7_MIN_POOL = int(os.getenv("D7_READ_MIN_POOL", "8"))          # below this -> thin_pool
_D7_SPIKE_UP = float(os.getenv("D7_READ_SPIKE_UP", "1.5"))      # axis moved up for this post
_D7_SPIKE_DOWN = float(os.getenv("D7_READ_SPIKE_DOWN", "0.6"))  # axis came in soft
_D7_RECORD_SPIKE_BAR = float(os.getenv("D7_READ_RECORD_BAR", "1.8"))  # earns a record slot
_D7_RECORD_MAX = int(os.getenv("D7_READ_RECORD_MAX", "20"))
_D7_RECORD_BAND_CAP = int(os.getenv("D7_READ_RECORD_BAND_CAP", "10"))  # max per 30d band
_D7_RECENT_UP = float(os.getenv("D7_READ_RECENT_UP", "1.15"))  # last-10 vs 90d -> up
_D7_RECENT_DOWN = float(os.getenv("D7_READ_RECENT_DOWN", "0.85"))  # -> down
# Account-relative standing bands (lower percentile == stronger). The band sets
# what the read's memory_match field owes; the prompt translates, never prints it.
_D7_BAND_DEEP = float(os.getenv("D7_READ_BAND_DEEP", "5"))         # detonation
_D7_BAND_WINNER = float(os.getenv("D7_READ_BAND_WINNER", "25"))    # clears the bar
_D7_BAND_CONTENDER = float(os.getenv("D7_READ_BAND_CONTENDER", "50"))  # within reach
_R2_CLIENT: Any | None = None
_POST_CONDENSATION_KEYS = {
    "post_key",
    "meta",
    "caption",
    "reel",
    "standout_details",
}
_D7_READ_KEYS = {
    "scene",
    "recent_run",
    "memory_match",
    "numbers",
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
            timeout=timeout,
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

    scene = str(body.get("scene") or "").strip()
    recent_run = _flatten_read_field(body.get("recent_run"))
    memory_match = _flatten_read_field(body.get("memory_match"))
    numbers = str(body.get("numbers") or "").strip()
    if not (scene and recent_run and memory_match and numbers):
        return None

    return {
        "d7_read": {
            "post_key": post_key,
            "scene": scene,
            "recent_run": recent_run,
            "memory_match": memory_match,
            "numbers": numbers,
        }
    }


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
    max_tokens: int = 6000,
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
    max_tokens: int = 6000,
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
            {"role": "system", "content": D7_READ_SYSTEM_V6},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, indent=2, default=str)},
        ],
        "temperature": 0.2,
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
                D7_READ_SYSTEM_V6,
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


def _d7_metric_anomaly(trigger_multiples: dict[str, float | None], pool_n: int) -> dict[str, Any]:
    """The one axis that broke from the account's usual on this post (server-decided)."""
    how_many_times = {
        axis: (round(mult, 1) if mult is not None else None)
        for axis, mult in trigger_multiples.items()
    }
    present = {axis: mult for axis, mult in trigger_multiples.items() if mult is not None}
    if pool_n < _D7_MIN_POOL or not present:
        primary = "thin_pool"
    else:
        ups = [axis for axis, mult in present.items() if mult >= _D7_SPIKE_UP]
        downs = [axis for axis, mult in present.items() if mult <= _D7_SPIKE_DOWN]
        if len(ups) >= 2:
            primary = "all"
        elif len(ups) == 1:
            primary = ups[0]
        elif downs:
            primary = "soft"
        else:
            primary = "flat"
    return {"primary": primary, "how-many-times": how_many_times}


def _d7_vs_usual(row: dict[str, Any]) -> dict[str, float | None]:
    """One post's metrics as a multiple of the account's 90-day usual, per axis."""
    out: dict[str, float | None] = {}
    for axis in ("views", "likes", "comments"):
        mult = _d7_mult(row, axis)
        out[axis] = round(mult, 1) if mult is not None else None
    return out


def _d7_group_vs_usual(last_10: list[dict[str, Any]]) -> dict[str, float | None]:
    """The last-10 group's median multiple vs the 90-day usual, per axis - how
    the recent run as a whole is holding up against the account's baseline."""
    out: dict[str, float | None] = {}
    for axis in ("views", "likes", "comments"):
        vals = [m for m in (_d7_mult(row, axis) for row in last_10) if m is not None]
        out[axis] = round(statistics.median(vals), 1) if vals else None
    return out


def _d7_recent_form(last_10: list[dict[str, Any]]) -> dict[str, Any]:
    """Last 10 against the 90-day usual, per axis: up / steady / down."""
    out: dict[str, str] = {}
    for axis in ("views", "likes", "comments"):
        vals = [m for m in (_d7_mult(row, axis) for row in last_10) if m is not None]
        if not vals:
            out[axis] = "unknown"
            continue
        median = statistics.median(vals)
        if median >= _D7_RECENT_UP:
            out[axis] = "up"
        elif median <= _D7_RECENT_DOWN:
            out[axis] = "down"
        else:
            out[axis] = "steady"
    return out


def _d7_recent_beats(trigger: dict[str, Any], recent_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """How this post placed against the recent run, per raw number.

    The model can read raw rows, but making this deterministic prevents off-by-one
    counting and gives the numbers field one sharp recent-placement fact.
    """
    axes: dict[str, dict[str, int | None]] = {}
    for axis in ("views", "likes", "comments"):
        trigger_value = trigger.get(axis)
        try:
            trigger_num = float(trigger_value)
        except (TypeError, ValueError):
            axes[axis] = {"beat": None, "total": 0}
            continue
        comparable = []
        for row in recent_rows:
            value = row.get(axis)
            try:
                comparable.append(float(value))
            except (TypeError, ValueError):
                continue
        axes[axis] = {
            "beat": sum(1 for value in comparable if trigger_num > value),
            "total": len(comparable),
        }
    return {
        "basis": "previous_10_posts",
        "axes": axes,
    }


def _d7_percentile_value(row: dict[str, Any]) -> float | None:
    """Account-relative D7 percentile, exact preferred. Lower == stronger."""
    for key in ("percentile_performance_exact", "percentile_performance"):
        val = row.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                continue
    return None


def _d7_standing(trigger: dict[str, Any]) -> dict[str, Any]:
    """The account-relative band for THIS post (server-decided, lower==stronger).

    Only the bifurcation - no worker-written phrasing, no derived numbers. The
    band sets what the read's memory_match field owes; the model reads the proof
    itself off the raw metrics already in the feeder file.
        deep/winner -> measure against the record
        contender   -> name the lever to cross into the winners
        noise        -> one line and stop
        unknown      -> hold back on account-relative placement
    """
    percentile = _d7_percentile_value(trigger)
    if percentile is None:
        band = "unknown"
    elif percentile <= _D7_BAND_DEEP:
        band = "deep"
    elif percentile <= _D7_BAND_WINNER:
        band = "winner"
    elif percentile <= _D7_BAND_CONTENDER:
        band = "contender"
    else:
        band = "noise"
    return {"band": band}


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
              f.handle,
              pm.views,
              pm.likes,
              pm.comments,
              pm.views_multiple,
              pm.likes_multiple,
              pm.comments_multiple,
              pm.percentile_performance,
              pm.percentile_performance_exact,
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

    trigger_scene = _d7_scene_body(ensure_post_condensation(conn, post_key))
    if not trigger_scene:
        return None

    feeder_id = int(trigger["feeder_id"])
    media_type = str(trigger.get("media_type") or "reel").strip().lower()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select
              p.post_key,
              p.posted_at,
              pm.views,
              pm.likes,
              pm.comments,
              pm.views_multiple,
              pm.likes_multiple,
              pm.comments_multiple,
              pm.business_date_ist,
              pc.condensation as condensation
            from public.posts p
            join public.post_metrics pm
              on pm.post_key = p.post_key
             and lower(pm.checkpoint) = 'd7'
            left join public.post_condensations pc
              on pc.post_key = p.post_key
             and pc.condensation_version = %s
            where p.feeder_id = %s
              and lower(coalesce(p.media_type, '')) = %s
              and p.posted_at >= now() - interval '90 days'
            order by p.posted_at desc nulls last
            """,
            (POST_CONDENSATION_PROMPT_VERSION, feeder_id, media_type),
        )
        rows = [dict(row) for row in cur.fetchall()]

    if not any(str(row.get("post_key") or "") == post_key for row in rows):
        rows.append(dict(trigger))
    rows.sort(key=_d7_posted_ts, reverse=True)

    # This post against the account's own usual.
    trigger_multiples = {axis: _d7_mult(trigger, axis) for axis in ("views", "likes", "comments")}
    metric_anomaly = _d7_metric_anomaly(trigger_multiples, len(rows))

    # Where this post stands in the account's own 90 days (drives the read's job).
    standing = _d7_standing(trigger)

    trigger_ts = _d7_posted_ts(trigger)
    previous_rows = [
        row
        for row in rows
        if str(row.get("post_key") or "") != post_key and _d7_posted_ts(row) < trigger_ts
    ]
    last_10_rows = previous_rows[:10]
    if len(last_10_rows) < 10:
        return None
    recent_form = _d7_recent_form(last_10_rows)
    # Keep the up/steady/down labels; add the group's baseline multiple alongside.
    recent_form["vs_usual"] = _d7_group_vs_usual(last_10_rows)

    # The feeder file the read places this post inside: the recent run + the record.
    last_10_keys = {str(row.get("post_key") or "") for row in last_10_rows}
    last_10: list[dict[str, Any]] = []
    for idx, row in enumerate(last_10_rows):
        scene = _d7_scene_body(row.get("condensation"))
        if not scene:
            continue
        last_10.append({
            "post_key": row.get("post_key"),
            "source_set": "last_10",
            "posts_ago": idx + 1,
            "posted_on": _d7_posted_on(row),
            "posted_on_source": "posts.posted_at",
            "views": row.get("views"),
            "likes": row.get("likes"),
            "comments": row.get("comments"),
            "vs_usual": _d7_vs_usual(row),
            "scene": scene,
        })
    if len(last_10) < 10:
        return None

    # The record: real spikes across 90 days, off the recent run, spread across time
    # so one hot phase can't flood it.
    candidates: list[tuple[float, dict[str, Any]]] = []
    for row in previous_rows:
        if str(row.get("post_key") or "") in last_10_keys:
            continue
        mags = [m for m in (_d7_mult(row, a) for a in ("views", "likes", "comments")) if m is not None]
        if not mags:
            continue
        magnitude = max(mags)
        if magnitude < _D7_RECORD_SPIKE_BAR:
            continue
        candidates.append((magnitude, row))
    candidates.sort(key=lambda item: item[0], reverse=True)

    band_counts = {0: 0, 1: 0, 2: 0}
    record: list[dict[str, Any]] = []
    for magnitude, row in candidates:
        if len(record) >= _D7_RECORD_MAX:
            break
        days_ago = int(max(0.0, (trigger_ts - _d7_posted_ts(row)) / 86400.0))
        band = min(2, days_ago // 30)
        if band_counts[band] >= _D7_RECORD_BAND_CAP:
            continue
        scene = _d7_scene_body(row.get("condensation"))
        if not scene:
            continue
        band_counts[band] += 1
        record.append({
            "post_key": row.get("post_key"),
            "source_set": "record",
            "posted_on": _d7_posted_on(row),
            "posted_on_source": "posts.posted_at",
            "views": row.get("views"),
            "likes": row.get("likes"),
            "comments": row.get("comments"),
            "vs_usual": _d7_vs_usual(row),
            "scene": scene,
        })

    return {
        "account": {"handle": str(trigger.get("handle") or "").strip()},
        "this_post": {
            "post_key": post_key,
            "posted_on": _d7_posted_on(trigger),
            "posted_on_source": "posts.posted_at",
            "caption": trigger.get("caption"),
            "views": trigger.get("views"),
            "likes": trigger.get("likes"),
            "comments": trigger.get("comments"),
            "vs_usual": _d7_vs_usual(trigger),
            "scene": trigger_scene,
        },
        "metric_anomaly": metric_anomaly,
        "standing": standing,
        "recent_form": recent_form,
        "recent_beats": _d7_recent_beats(trigger, last_10_rows),
        "feeder_file": {
            "last_10": last_10,
            "record": record,
        },
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
