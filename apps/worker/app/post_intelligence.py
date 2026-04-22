"""
Post Intelligence — LLM-based semantic tag extraction for posts.

Uses Gemini multimodal models, preferring OpenRouter when configured and
falling back to Google direct Gemini when needed.

Media contract:
  - reels must be analyzed from the full source video (up to 120s / 50MB)
  - carousels must be analyzed from the full slide set
  - images must be analyzed from the original post image source

If the required media source is unavailable, intelligence is skipped instead of
silently degrading to thumbnail- or caption-only inference.
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
_PROMPT_VERSION = "pi_v2"

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
- Reel-only signals: pacing, audio mode, duration bucket
- Image/carousel-only signals: density and depth
- Shared visual signals for all media: style, face presence, text overlays
- Face presence: none, one person, or multiple people
- Text overlays: none, light support, or text-heavy frames
- Density: clean/minimal vs balanced vs busy for static media only
- Duration or depth: reel length bucket OR image/carousel depth, not both

Return ONLY a JSON object with these keys and exact enum values:

{
  "mechanic": one of REVEAL, PROCESS, REACTION, SHOWCASE, STORY, COMPARE, LIST, CHALLENGE, CONVERSE, ACCESS, ANNOUNCE, COLLAB, SOCIAL_PROOF, AESTHETIC, EDUCATE,
  "opening_move": one of RESULT_FIRST, PERSON_FIRST, TEXT_FIRST, OBJECT_FIRST, ACTION_FIRST, SCENE_FIRST,
  "proof_mode": one of LIVE_DEMO, VISUAL_RESULT, EXPERT_TALK, SOCIAL_PROOF, DATA_PROOF, ACCESS_PROOF, PROOF_NONE,
  "pacing": one of PACING_SLOW, PACING_MEDIUM, PACING_FAST, or null if the media is not a reel,
  "audio_mode": one of AUDIO_DIRECT_SPEECH, AUDIO_VOICEOVER, AUDIO_SOURCE_LIVE, AUDIO_MUSIC_LED, AUDIO_ASMR, AUDIO_MINIMAL, or null if the media is not a reel,
  "style": one of STYLE_UGC, STYLE_STUDIO, STYLE_TEXT_DRIVEN, STYLE_MONTAGE, STYLE_CINEMATIC, STYLE_SCREEN_RECORD,
  "cta": one of CTA_ENGAGEMENT, CTA_TRAFFIC, CTA_PURCHASE, CTA_COMMUNITY, or null if none detected,
  "face": one of FACE_SINGLE, FACE_NONE, FACE_MULTIPLE,
  "language": ISO 639-1 code of the primary language in the caption (e.g. "en", "hi", "fr"). Use "mixed_X_Y" for code-switched content (e.g. "mixed_en_hi"),
  "depth": one of DEPTH_SINGLE, DEPTH_MINI, DEPTH_STANDARD, DEPTH_DEEP for images/carousels, or null if the media is a reel,
  "density": one of DENSITY_MINIMAL, DENSITY_MEDIUM, DENSITY_BUSY, or null if the media is a reel,
  "text_overlay": one of TEXT_NONE, TEXT_LIGHT, TEXT_HEAVY,
  "duration_bucket": one of DUR_SHORT (under 15s), DUR_MEDIUM (15-30s), DUR_LONG (30-60s), DUR_EXTENDED (60s+), or null if not a reel
}

Rules:
- Classify structure, not niche or topic.
- Do not infer whether audio is trending or popular on Instagram.
- Use only tags that are meaningful for the given media type. If a field does not make sense for that media, return null instead of guessing.
- Base style, face, text_overlay, opening_move, and proof_mode primarily on the visual media.
- For reels, use pacing based on edit rhythm across the reel. Do not infer pacing for images or carousels.
- For images and carousels, use density based on visual clutter in the frames. Do not use density for reels.
- For reels, use duration_bucket and set depth to null.
- For single images, set depth to DEPTH_SINGLE.
- For carousels, use depth based on slide count/context.
- For reels, audio_mode must come from the reel audio itself.
- Audio precedence for reels:
  - If understandable spoken dialogue or narration is meaningfully present, do NOT use AUDIO_MUSIC_LED.
  - Use AUDIO_DIRECT_SPEECH when a visible speaker's synced speech is the main audio layer.
  - Use AUDIO_VOICEOVER when off-camera narration or dubbed commentary is the main audio layer.
  - Use AUDIO_SOURCE_LIVE when live captured sound or ambient source audio leads and speech is not the main driver.
  - Use AUDIO_MUSIC_LED only when music is clearly the main driver and any speech is absent, incidental, background, or unintelligible.
  - Background music under clear dialogue still counts as speech-led, not music-led.
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

_COMMON_REQUIRED_SIGNAL_TAGS = {
    "mechanic",
    "opening_move",
    "proof_mode",
    "style",
    "face",
    "language",
    "text_overlay",
}

_MEDIA_REQUIRED_SIGNAL_TAGS = {
    "reel": {"pacing", "audio_mode", "duration_bucket"},
    "image": {"depth", "density"},
    "sidecar": {"depth", "density"},
}

_MEDIA_FORCED_NULL_TAGS = {
    "reel": {"density", "depth"},
    "image": {"pacing", "audio_mode", "duration_bucket"},
    "sidecar": {"pacing", "audio_mode", "duration_bucket"},
}

_ALWAYS_OPTIONAL_TAGS = {"cta"}


def is_enabled() -> bool:
    return POST_INTELLIGENCE_ENABLED


def _selected_provider(provider_override: str | None = None) -> str:
    provider = (provider_override or POST_INTELLIGENCE_PROVIDER or "auto").strip().lower()
    if provider == "openrouter":
        return "openrouter"
    if provider == "google":
        return "google"
    if OPENROUTER_API_KEY:
        return "openrouter"
    return "google"


def _selected_model(provider_override: str | None = None) -> str:
    explicit = (POST_INTELLIGENCE_MODEL or "").strip()
    if explicit:
        return explicit
    if _selected_provider(provider_override) == "openrouter":
        return _DEFAULT_OPENROUTER_MODEL
    return _DEFAULT_GEMINI_MODEL


def current_model_version(*, provider: str | None = None, skipped: bool = False) -> str:
    if skipped:
        return "skipped"
    selected_provider = _selected_provider(provider)
    return f"{selected_provider}:{_selected_model(selected_provider)}:{_PROMPT_VERSION}"


def _provider_chain() -> list[str]:
    preferred = _selected_provider()
    providers = [preferred]
    if preferred != "google" and GEMINI_API_KEY:
        providers.append("google")
    if preferred != "openrouter" and OPENROUTER_API_KEY:
        providers.append("openrouter")
    return providers


def _normalize_media_type(media_type: str | None) -> str:
    media = (media_type or "image").strip().lower()
    if media == "carousel":
        media = "sidecar"
    if media not in {"reel", "image", "sidecar"}:
        return "image"
    return media


def _required_signal_keys_for_media(media_type: str | None) -> set[str]:
    media = _normalize_media_type(media_type)
    return _COMMON_REQUIRED_SIGNAL_TAGS | _MEDIA_REQUIRED_SIGNAL_TAGS.get(media, set())


def _forced_null_tags_for_media(media_type: str | None) -> set[str]:
    media = _normalize_media_type(media_type)
    return _MEDIA_FORCED_NULL_TAGS.get(media, set())


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
) -> tuple[list[dict[str, Any]], int, int]:
    """Fetch and encode carousel slide images for the configured provider."""
    parts: list[dict[str, Any]] = []
    expected = len(carousel_urls[:max_images])
    fetched = 0
    for url in carousel_urls[:max_images]:
        data = _fetch_bytes(url, timeout=8, headers=fetch_headers)
        if not data or not data[1].startswith("image/"):
            continue
        fetched += 1

        if provider == "openrouter":
            parts.append(_build_openrouter_image_part(data[0], data[1]))
        else:
            parts.append({
                "inline_data": {
                    "mime_type": data[1],
                    "data": base64.b64encode(data[0]).decode("ascii"),
                }
            })
    return parts, expected, fetched


def _skip_result(
    *,
    reason: str,
    media_type: str,
    expected_source: str,
    actual_source: str = "missing",
    detail: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "_skipped": True,
        "_skip_reason": reason,
        "_media_type": media_type,
        "_expected_source": expected_source,
        "_visual_source": actual_source,
    }
    if detail:
        payload["_skip_detail"] = detail
    return payload


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
    providers = _provider_chain()
    if not providers:
        return None

    for idx, provider in enumerate(providers):
        result = _extract_tags_once(
            caption,
            media_type,
            thumbnail_url=thumbnail_url,
            video_url=video_url,
            carousel_urls=carousel_urls,
            media_fetch_headers=media_fetch_headers,
            provider=provider,
        )
        if result and result.get("_skipped"):
            return result
        if result:
            if idx > 0:
                print(f"[post-intelligence] fallback provider {provider} succeeded")
            return result
        if idx < len(providers) - 1:
            print(
                f"[post-intelligence] provider {provider} returned no usable tags; "
                f"trying {providers[idx + 1]}"
            )
    return None


def _extract_tags_once(
    caption: str,
    media_type: str,
    *,
    thumbnail_url: str | None = None,
    video_url: str | None = None,
    carousel_urls: list[str] | None = None,
    media_fetch_headers: dict[str, str] | None = None,
    provider: str,
) -> dict[str, Any] | None:
    """
    Extract semantic tags from a post via Gemini multimodal models.

    Strict source rules:
      - reels require the full source video (up to 120s / 50MB)
      - sidecars require the full carousel slide set
      - images require the original image source

    If those sources are unavailable, the extraction is skipped.
    """
    if provider == "openrouter" and not OPENROUTER_API_KEY:
        return None
    if provider == "google" and not GEMINI_API_KEY:
        return None

    media = _normalize_media_type(media_type)

    caption_trimmed = (caption or "").strip()[:2000]
    if not caption_trimmed:
        caption_trimmed = "(no caption)"

    user_message = f"Media type: {media or 'unknown'}\n\nCaption:\n{caption_trimmed}"

    parts: list[dict[str, Any]] = []
    visual_source = "none"

    if media == "reel":
        if not video_url:
            return _skip_result(
                reason="reel_missing_full_video",
                media_type=media,
                expected_source="full_video",
            )
        video_data = _fetch_bytes(
            video_url,
            timeout=60,
            max_bytes=_VIDEO_UPLOAD_MAX_BYTES,
            headers=media_fetch_headers,
        )
        if not video_data:
            return _skip_result(
                reason="reel_full_video_fetch_failed",
                media_type=media,
                expected_source="full_video",
            )

        video_bytes, video_ct = video_data
        if not video_ct.startswith("video/"):
            video_ct = "video/mp4"

        duration = _probe_video_duration(video_bytes)
        if duration is not None and duration > _VIDEO_CAP_SECONDS:
            print(f"[post-intelligence] skipped: video {duration:.0f}s exceeds {_VIDEO_CAP_SECONDS}s cap")
            return _skip_result(
                reason="duration_exceeded",
                media_type=media,
                expected_source="full_video",
                actual_source="video_full",
                detail=f"{round(duration)}s",
            )

        if provider == "openrouter":
            video_part = _build_openrouter_video_part(video_bytes, video_ct)
            method = "data_url"
        else:
            video_part = _build_gemini_video_part(video_bytes, video_ct)
            method = "inline" if len(video_bytes) <= _VIDEO_INLINE_MAX_BYTES else "file_api"

        if not video_part:
            return _skip_result(
                reason="reel_full_video_unusable",
                media_type=media,
                expected_source="full_video",
                actual_source="video_fetch_failed",
            )

        size_mb = len(video_bytes) / (1024 * 1024)
        visual_source = f"video_full:{size_mb:.1f}mb:{method}"
        parts.append(video_part)

    elif media == "sidecar":
        if not carousel_urls:
            return _skip_result(
                reason="carousel_missing_full_set",
                media_type=media,
                expected_source="carousel_full",
            )
        slide_parts, expected_slides, fetched_slides = _build_carousel_parts(
            carousel_urls,
            provider=provider,
            fetch_headers=media_fetch_headers,
            max_images=max(20, len(carousel_urls)),
        )
        if expected_slides == 0:
            return _skip_result(
                reason="carousel_missing_full_set",
                media_type=media,
                expected_source="carousel_full",
            )
        if fetched_slides != expected_slides:
            return _skip_result(
                reason="carousel_incomplete_source",
                media_type=media,
                expected_source="carousel_full",
                actual_source=f"carousel_partial:{fetched_slides}/{expected_slides}",
            )
        visual_source = f"carousel:{fetched_slides}slides"
        parts.extend(slide_parts)

    else:
        if not thumbnail_url:
            return _skip_result(
                reason="image_missing_source",
                media_type=media,
                expected_source="image_full",
            )
        image_data = _fetch_bytes(thumbnail_url, timeout=8, headers=media_fetch_headers)
        if not image_data or not image_data[1].startswith("image/"):
            return _skip_result(
                reason="image_source_fetch_failed",
                media_type=media,
                expected_source="image_full",
            )
        visual_source = "image_full"
        if provider == "openrouter":
            parts.append(_build_openrouter_image_part(image_data[0], image_data[1]))
        else:
            parts.append({
                "inline_data": {
                    "mime_type": image_data[1],
                    "data": base64.b64encode(image_data[0]).decode("ascii"),
                }
            })

    if not parts:
        return _skip_result(
            reason="no_usable_visual_source",
            media_type=media,
            expected_source="strict_media_source",
            actual_source=visual_source,
        )

    model = _selected_model(provider)
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

        validated = _validate_tags(tags, media)
        if validated:
            if not _has_required_signal_tags(validated, media):
                return None
            validated["_visual_source"] = visual_source
        return validated if validated else None
    except Exception as exc:
        print(f"[post-intelligence] extraction failed ({provider}:{visual_source}): {exc}")
        return None


def _validate_tags(tags: dict[str, Any], media_type: str | None = None) -> dict[str, Any]:
    """Validate extracted tags against known enums. Strip invalid values."""
    result: dict[str, Any] = {}
    forced_null_keys = _forced_null_tags_for_media(media_type)
    allowed_null_keys = forced_null_keys | _ALWAYS_OPTIONAL_TAGS
    for key, valid_set in _VALID_TAGS.items():
        if key in forced_null_keys:
            result[key] = None
            continue
        val = tags.get(key)
        if isinstance(val, str) and val.upper() in valid_set:
            result[key] = val.upper()
        elif val is None and key in allowed_null_keys:
            result[key] = None

    lang = tags.get("language")
    if isinstance(lang, str) and len(lang) <= 20:
        result["language"] = lang.lower().strip()

    return result


def _has_required_signal_tags(tags: dict[str, Any], media_type: str | None = None) -> bool:
    """Only persist rows that can participate in the current pattern engine."""
    for key in _required_signal_keys_for_media(media_type):
        value = tags.get(key)
        if not isinstance(value, str) or not value.strip():
            return False
    return True
