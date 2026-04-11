"""
Post Intelligence — LLM-based semantic tag extraction for posts.

Uses Gemini multimodal models, preferring OpenRouter when configured and
falling back to Google direct Gemini when needed. For reels, sends the FULL
video (up to 120s / 50MB) so the model watches every frame — no sampling,
no guessing from thumbnails.
"""
from __future__ import annotations

import base64
import json
import os
import tempfile
import time
import subprocess
from typing import Any

import requests

from .config import (
    GEMINI_API_KEY,
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
    POST_INTELLIGENCE_ENABLED,
    POST_INTELLIGENCE_MODEL,
    POST_INTELLIGENCE_PROVIDER,
)

_GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
_GEMINI_UPLOAD_URL = "https://generativelanguage.googleapis.com/upload/v1beta/files"
_GEMINI_FILE_URL = "https://generativelanguage.googleapis.com/v1beta/files/{name}"
_OPENROUTER_CHAT_URL = "/chat/completions"

_DEFAULT_OPENROUTER_MODEL = "google/gemini-3-flash-preview"
_DEFAULT_GEMINI_MODEL = "gemini-3-flash-preview"

# Max video duration to process (seconds). Anything longer gets skipped.
_VIDEO_CAP_SECONDS = 120

# Gemini direct path can use inline bytes up to ~20MB, then File API until 50MB.
_VIDEO_INLINE_MAX_BYTES = 20 * 1024 * 1024
_VIDEO_UPLOAD_MAX_BYTES = 50 * 1024 * 1024

_EXTRACTION_PROMPT = """You are a social media structure analyst. You will be given a post caption, media type, and the actual visual content (full video for reels, full image set for images/carousels).

Your job is NOT to summarize the topic. Your job is to classify how the post is built so similar posts can be grouped later.

Analyze the FULL visual content from start to finish. Focus on:
- Mechanic: how the viewer receives value (reveal, process, reaction, story, comparison, showcase, etc.)
- Opening move: what appears in the first 1-3 seconds / first frame
- Proof mode: what makes the claim believable on-screen
- Pacing: how quickly the edit moves
- Audio mode: whether the reel uses direct speech, voiceover, natural live sound, music-led edit, ASMR, or almost no meaningful audio
- Style: ugc, studio, cinematic, screen recording, montage, or text-led
- Face presence: none, one person, or multiple people
- Text overlays: none, light support, or text-heavy frames
- Density: clean/minimal vs balanced vs busy
- Duration or depth: reel length bucket or carousel depth

Return ONLY a JSON object with these keys and exact enum values:

{
  "mechanic": one of REVEAL, PROCESS, REACTION, SHOWCASE, STORY, COMPARE, LIST, CHALLENGE, CONVERSE, ACCESS, ANNOUNCE, COLLAB, SOCIAL_PROOF, AESTHETIC, EDUCATE,
  "opening_move": one of RESULT_FIRST, PERSON_FIRST, TEXT_FIRST, OBJECT_FIRST, ACTION_FIRST, SCENE_FIRST,
  "proof_mode": one of LIVE_DEMO, VISUAL_RESULT, EXPERT_TALK, SOCIAL_PROOF, DATA_PROOF, ACCESS_PROOF, PROOF_NONE,
  "pacing": one of PACING_SLOW, PACING_MEDIUM, PACING_FAST,
  "audio_mode": one of AUDIO_DIRECT_SPEECH, AUDIO_VOICEOVER, AUDIO_SOURCE_LIVE, AUDIO_MUSIC_LED, AUDIO_ASMR, AUDIO_MINIMAL, or null if the media is not a reel,
  "style": one of STYLE_UGC, STYLE_STUDIO, STYLE_TEXT_DRIVEN, STYLE_MONTAGE, STYLE_CINEMATIC, STYLE_SCREEN_RECORD,
  "cta": one of CTA_ENGAGEMENT, CTA_TRAFFIC, CTA_PURCHASE, CTA_COMMUNITY, or null if none detected,
  "face": one of FACE_SINGLE, FACE_NONE, FACE_MULTIPLE,
  "language": ISO 639-1 code of the primary language in the caption (e.g. "en", "hi", "fr"). Use "mixed_X_Y" for code-switched content (e.g. "mixed_en_hi"),
  "depth": one of DEPTH_SINGLE, DEPTH_MINI, DEPTH_STANDARD, DEPTH_DEEP (carousel slide count context), or DEPTH_SINGLE for reels/images,
  "density": one of DENSITY_MINIMAL, DENSITY_MEDIUM, DENSITY_BUSY,
  "text_overlay": one of TEXT_NONE, TEXT_LIGHT, TEXT_HEAVY,
  "duration_bucket": one of DUR_SHORT (under 15s), DUR_MEDIUM (15-30s), DUR_LONG (30-60s), DUR_EXTENDED (60s+), or null if not a reel
}

Rules:
- Classify structure, not niche or topic.
- Do not infer whether audio is trending or popular on Instagram.
- Base style, face, density, text_overlay, pacing, opening_move, and proof_mode primarily on the visual media.
- For reels, audio_mode must come from the reel audio itself.
- Pick the single best match for each required field.
- Return ONLY the JSON object, with no explanation."""

_VALID_TAGS: dict[str, set[str]] = {
    "mechanic": {"REVEAL", "PROCESS", "REACTION", "SHOWCASE", "STORY", "COMPARE", "LIST", "CHALLENGE", "CONVERSE", "ACCESS", "ANNOUNCE", "COLLAB", "SOCIAL_PROOF", "AESTHETIC", "EDUCATE"},
    "opening_move": {"RESULT_FIRST", "PERSON_FIRST", "TEXT_FIRST", "OBJECT_FIRST", "ACTION_FIRST", "SCENE_FIRST"},
    "proof_mode": {"LIVE_DEMO", "VISUAL_RESULT", "EXPERT_TALK", "SOCIAL_PROOF", "DATA_PROOF", "ACCESS_PROOF", "PROOF_NONE"},
    "pacing": {"PACING_SLOW", "PACING_MEDIUM", "PACING_FAST"},
    "audio_mode": {"AUDIO_DIRECT_SPEECH", "AUDIO_VOICEOVER", "AUDIO_SOURCE_LIVE", "AUDIO_MUSIC_LED", "AUDIO_ASMR", "AUDIO_MINIMAL"},
    "style": {"STYLE_UGC", "STYLE_STUDIO", "STYLE_TEXT_DRIVEN", "STYLE_MONTAGE", "STYLE_CINEMATIC", "STYLE_SCREEN_RECORD"},
    "cta": {"CTA_ENGAGEMENT", "CTA_TRAFFIC", "CTA_PURCHASE", "CTA_COMMUNITY"},
    "face": {"FACE_SINGLE", "FACE_NONE", "FACE_MULTIPLE"},
    "depth": {"DEPTH_SINGLE", "DEPTH_MINI", "DEPTH_STANDARD", "DEPTH_DEEP"},
    "density": {"DENSITY_MINIMAL", "DENSITY_MEDIUM", "DENSITY_BUSY"},
    "text_overlay": {"TEXT_NONE", "TEXT_LIGHT", "TEXT_HEAVY"},
    "duration_bucket": {"DUR_SHORT", "DUR_MEDIUM", "DUR_LONG", "DUR_EXTENDED"},
}

_REQUIRED_SIGNAL_TAGS = {
    "mechanic",
    "opening_move",
    "proof_mode",
    "pacing",
    "style",
    "face",
    "language",
    "depth",
    "density",
    "text_overlay",
}


def is_enabled() -> bool:
    return POST_INTELLIGENCE_ENABLED


def _selected_provider() -> str:
    provider = (POST_INTELLIGENCE_PROVIDER or "auto").strip().lower()
    if provider == "openrouter":
        return "openrouter"
    if provider == "google":
        return "google"
    if OPENROUTER_API_KEY:
        return "openrouter"
    return "google"


def _selected_model() -> str:
    explicit = (POST_INTELLIGENCE_MODEL or "").strip()
    if explicit:
        return explicit
    if _selected_provider() == "openrouter":
        return _DEFAULT_OPENROUTER_MODEL
    return _DEFAULT_GEMINI_MODEL


def current_model_version(*, skipped: bool = False) -> str:
    if skipped:
        return "skipped"
    return f"{_selected_provider()}:{_selected_model()}"


def _fetch_bytes(
    url: str,
    timeout: int = 10,
    max_bytes: int = 0,
    headers: dict[str, str] | None = None,
) -> tuple[bytes, str] | None:
    """Fetch binary content from URL. Returns (bytes, content_type) or None."""
    if not url or not url.startswith(("http://", "https://")):
        return None

    req_headers = {"User-Agent": "FeedMe/1.0"}
    if headers:
        req_headers.update(headers)

    try:
        resp = requests.get(url, timeout=timeout, headers=req_headers, stream=bool(max_bytes))
        resp.raise_for_status()
        ct = (resp.headers.get("content-type") or "application/octet-stream").split(";", 1)[0].strip()

        if max_bytes:
            chunks: list[bytes] = []
            total = 0
            for chunk in resp.iter_content(chunk_size=256 * 1024):
                chunks.append(chunk)
                total += len(chunk)
                if total > max_bytes:
                    resp.close()
                    return None
            body = b"".join(chunks)
        else:
            body = resp.content

        if len(body) < 100:
            return None
        return body, ct
    except Exception:
        return None


def _probe_video_duration(video_bytes: bytes) -> float | None:
    """
    Probe video duration in seconds using ffprobe.
    Returns duration or None if ffprobe unavailable / fails.
    """
    tmp_in = None
    try:
        tmp_in = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
        tmp_in.write(video_bytes)
        tmp_in.close()

        probe = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", tmp_in.name],
            capture_output=True,
            timeout=10,
        )
        if probe.returncode != 0:
            return None

        fmt = json.loads(probe.stdout).get("format", {})
        return float(fmt.get("duration", 0))
    except Exception:
        return None
    finally:
        if tmp_in:
            try:
                os.unlink(tmp_in.name)
            except Exception:
                pass


def _upload_video_to_gemini(video_bytes: bytes, mime_type: str) -> str | None:
    """
    Upload video via Gemini File API for videos that exceed inline limit.
    Returns the file URI or None on failure.
    """
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
            json={"file": {"display_name": "feedme_reel"}},
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
            check = requests.get(
                _GEMINI_FILE_URL.format(name=file_name),
                params={"key": GEMINI_API_KEY},
                timeout=10,
            )
            if check.status_code == 200:
                state = check.json().get("state", "")
                if state == "ACTIVE":
                    return file_uri
                if state == "FAILED":
                    return None
            time.sleep(2)
        return None
    except Exception as exc:
        print(f"[post-intelligence] file upload failed: {exc}")
        return None


def _build_gemini_video_part(video_bytes: bytes, mime_type: str) -> dict | None:
    """Build a Gemini content part for a full video."""
    size = len(video_bytes)

    if size <= _VIDEO_INLINE_MAX_BYTES:
        return {
            "inline_data": {
                "mime_type": mime_type,
                "data": base64.b64encode(video_bytes).decode("ascii"),
            }
        }

    if size <= _VIDEO_UPLOAD_MAX_BYTES:
        file_uri = _upload_video_to_gemini(video_bytes, mime_type)
        if file_uri:
            return {
                "file_data": {
                    "mime_type": mime_type,
                    "file_uri": file_uri,
                }
            }

    return None


def _data_url(mime_type: str, payload: bytes) -> str:
    return f"data:{mime_type};base64,{base64.b64encode(payload).decode('ascii')}"


def _build_openrouter_image_part(image_bytes: bytes, mime_type: str) -> dict[str, Any]:
    return {
        "type": "image_url",
        "image_url": {
            "url": _data_url(mime_type, image_bytes),
        },
    }


def _build_openrouter_video_part(video_bytes: bytes, mime_type: str) -> dict[str, Any] | None:
    if len(video_bytes) > _VIDEO_UPLOAD_MAX_BYTES:
        return None
    return {
        "type": "video_url",
        "video_url": {
            "url": _data_url(mime_type, video_bytes),
        },
    }


def _build_carousel_parts(
    carousel_urls: list[str],
    *,
    provider: str,
    fetch_headers: dict[str, str] | None = None,
    max_images: int = 20,
) -> list[dict[str, Any]]:
    """Fetch and encode carousel slide images for the configured provider."""
    parts: list[dict[str, Any]] = []
    for url in carousel_urls[:max_images]:
        data = _fetch_bytes(url, timeout=8, headers=fetch_headers)
        if not data or not data[1].startswith("image/"):
            continue

        if provider == "openrouter":
            parts.append(_build_openrouter_image_part(data[0], data[1]))
        else:
            parts.append({
                "inline_data": {
                    "mime_type": data[1],
                    "data": base64.b64encode(data[0]).decode("ascii"),
                }
            })
    return parts


def _extract_response_text(payload: dict[str, Any], *, provider: str) -> str:
    if provider == "openrouter":
        content = (
            payload.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
        )
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            text_parts: list[str] = []
            for part in content:
                if isinstance(part, dict) and part.get("type") == "text" and isinstance(part.get("text"), str):
                    text_parts.append(part["text"])
                elif isinstance(part, str):
                    text_parts.append(part)
            return "\n".join(text_parts).strip()
        return ""

    return (
        payload.get("candidates", [{}])[0]
        .get("content", {})
        .get("parts", [{}])[0]
        .get("text", "")
    )


def _extract_json_text(raw: str) -> str:
    text = (raw or "").strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines:
            lines = lines[1:]
        while lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text


def extract_tags(
    caption: str,
    media_type: str,
    thumbnail_url: str | None = None,
    video_url: str | None = None,
    carousel_urls: list[str] | None = None,
    media_fetch_headers: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    """
    Extract semantic tags from a post via Gemini multimodal models.

    For reels: downloads the FULL video (up to 120s, 50MB cap).
    For sidecars: sends all available slide images + caption.
    For images: sends the current preview image + caption.
    Falls back to thumbnail + caption if richer media fetch fails.
    """
    provider = _selected_provider()
    if provider == "openrouter" and not OPENROUTER_API_KEY:
        return None
    if provider == "google" and not GEMINI_API_KEY:
        return None

    media = (media_type or "unknown").strip().lower()
    if media == "carousel":
        media = "sidecar"

    caption_trimmed = (caption or "").strip()[:2000]
    if not caption_trimmed:
        caption_trimmed = "(no caption)"

    user_message = f"Media type: {media or 'unknown'}\n\nCaption:\n{caption_trimmed}"

    parts: list[dict[str, Any]] = []
    visual_source = "none"

    if media == "reel" and video_url:
        video_data = _fetch_bytes(
            video_url,
            timeout=60,
            max_bytes=_VIDEO_UPLOAD_MAX_BYTES,
            headers=media_fetch_headers,
        )
        if video_data:
            video_bytes, video_ct = video_data
            if not video_ct.startswith("video/"):
                video_ct = "video/mp4"

            duration = _probe_video_duration(video_bytes)
            if duration is not None and duration > _VIDEO_CAP_SECONDS:
                print(f"[post-intelligence] skipped: video {duration:.0f}s exceeds {_VIDEO_CAP_SECONDS}s cap")
                return {
                    "_skipped": True,
                    "_skip_reason": "duration_exceeded",
                    "_video_duration": round(duration),
                    "_visual_source": "none",
                }

            if provider == "openrouter":
                video_part = _build_openrouter_video_part(video_bytes, video_ct)
                method = "data_url"
            else:
                video_part = _build_gemini_video_part(video_bytes, video_ct)
                method = "inline" if len(video_bytes) <= _VIDEO_INLINE_MAX_BYTES else "file_api"

            if video_part:
                size_mb = len(video_bytes) / (1024 * 1024)
                visual_source = f"video_full:{size_mb:.1f}mb:{method}"
                parts.append(video_part)

    elif media == "sidecar" and carousel_urls:
        slide_parts = _build_carousel_parts(
            carousel_urls,
            provider=provider,
            fetch_headers=media_fetch_headers,
        )
        if slide_parts:
            visual_source = f"carousel:{len(slide_parts)}slides"
            parts.extend(slide_parts)

    if visual_source == "none" and thumbnail_url:
        thumb_data = _fetch_bytes(thumbnail_url, timeout=8, headers=media_fetch_headers)
        if thumb_data and thumb_data[1].startswith("image/"):
            visual_source = "thumbnail"
            if provider == "openrouter":
                parts.append(_build_openrouter_image_part(thumb_data[0], thumb_data[1]))
            else:
                parts.append({
                    "inline_data": {
                        "mime_type": thumb_data[1],
                        "data": base64.b64encode(thumb_data[0]).decode("ascii"),
                    }
                })

    model = _selected_model()
    payload: dict[str, Any]
    request_kwargs: dict[str, Any]

    if provider == "openrouter":
        parts.append({"type": "text", "text": user_message})
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": _EXTRACTION_PROMPT},
                {"role": "user", "content": parts},
            ],
            "temperature": 0.1,
            "max_tokens": 512,
        }
        request_kwargs = {
            "url": f"{OPENROUTER_BASE_URL.rstrip('/')}{_OPENROUTER_CHAT_URL}",
            "headers": {
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
            },
            "json": payload,
        }
    else:
        parts.insert(0, {"text": _EXTRACTION_PROMPT})
        parts.append({"text": user_message})
        payload = {
            "contents": [{"parts": parts}],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": 512,
                "responseMimeType": "application/json",
            },
        }
        request_kwargs = {
            "url": _GEMINI_API_URL.format(model=model),
            "params": {"key": GEMINI_API_KEY},
            "json": payload,
        }

    try:
        req_timeout = 90 if "video_full" in visual_source else 15
        resp = requests.post(timeout=req_timeout, **request_kwargs)
        resp.raise_for_status()

        data = resp.json()
        text_out = _extract_response_text(data, provider=provider)
        tags = json.loads(_extract_json_text(text_out))
        if not isinstance(tags, dict):
            return None

        validated = _validate_tags(tags)
        if validated:
            if not _has_required_signal_tags(validated):
                return None
            validated["_visual_source"] = visual_source
        return validated if validated else None
    except Exception as exc:
        print(f"[post-intelligence] extraction failed ({provider}:{visual_source}): {exc}")
        return None


def _validate_tags(tags: dict[str, Any]) -> dict[str, Any]:
    """Validate extracted tags against known enums. Strip invalid values."""
    result: dict[str, Any] = {}
    for key, valid_set in _VALID_TAGS.items():
        val = tags.get(key)
        if isinstance(val, str) and val.upper() in valid_set:
            result[key] = val.upper()
        elif val is None and key in ("audio_mode", "cta", "duration_bucket"):
            result[key] = None

    lang = tags.get("language")
    if isinstance(lang, str) and len(lang) <= 20:
        result["language"] = lang.lower().strip()

    return result


def _has_required_signal_tags(tags: dict[str, Any]) -> bool:
    """Only persist rows that can participate in the current pattern engine."""
    for key in _REQUIRED_SIGNAL_TAGS:
        value = tags.get(key)
        if not isinstance(value, str) or not value.strip():
            return False
    return True
