from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import subprocess
import tempfile
import time
from datetime import date, datetime, timezone
from typing import Any

import requests
from psycopg.rows import dict_row

from .config import (
    GEMINI_API_KEY,
    MEDIA_PUBLIC_BASE_URL,
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
    SIGNAL_INTELLIGENCE_ESCALATION_MODEL,
    SIGNAL_INTELLIGENCE_ENABLED,
    SIGNAL_INTELLIGENCE_MODEL,
    SIGNAL_INTELLIGENCE_PROVIDER,
)
from .focus_rulebook import signal_rulebook_context

_GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
_GEMINI_UPLOAD_URL = "https://generativelanguage.googleapis.com/upload/v1beta/files"
_GEMINI_FILE_URL = "https://generativelanguage.googleapis.com/v1beta/files/{name}"
_OPENROUTER_CHAT_URL = "/chat/completions"
_DEFAULT_OPENROUTER_MODEL = "google/gemini-3-flash-preview"
_DEFAULT_GEMINI_MODEL = "gemini-3-flash-preview"
_FP_PROMPT_VERSION = "fingerprint_v5_full_context"
_CARD_PROMPT_VERSION = "movement_thesis_v1_pressure"
_SAMPLING_POLICY_VERSION = "media_sample_v2_120s_all_slides"
_VIDEO_UPLOAD_MAX_BYTES = 50 * 1024 * 1024
_VIDEO_INLINE_MAX_BYTES = 20 * 1024 * 1024
_IMAGE_MAX_BYTES = 12 * 1024 * 1024
_VIDEO_SAMPLE_SECONDS = 120

_FINGERPRINT_SYSTEM = """Create a full neutral fingerprint of this Instagram post.
You receive only this post: caption, media, audio/video if available, and basic media metadata.
Do not use account history, feed context, metrics, alerts, or strategy assumptions.
Do not explain whether it worked. Do not compare it to prior posts.

Return only JSON:
{
  "post_id": "",
  "media_type": "reel|sidecar|image",
  "duration_seconds": null,
  "carousel_slide_count": null,
  "observed": {
    "caption": "verbatim caption text, or empty string",
    "transcript": "verbatim spoken text for video/reel, as complete as possible; empty for image/carousel with no speech",
    "audio_notes": "music, ambient sound, silence, voiceover style, sound effects, audio/text synchronization",
    "visual_notes": "detailed description: frame-by-frame for short reels, scene-level for long reels up to supplied duration, slide-by-slide for every carousel slide"
  },
  "synthesis": {
    "subject": "1-3 sentences: what the post is materially about",
    "craft": "1-3 sentences: structure, pacing, format choices, transitions, late payoff if present, distinctive moves",
    "voice": "1-3 sentences: tone, register, posture, how the speaker/brand positions itself",
    "proof": "1-3 sentences: evidence, credibility devices, scarcity, social proof, visual proof; or 'none'"
  },
  "media_confidence": "high|medium|low"
}"""

_CARD_SYSTEM = """You write Feed_Me movement theses.
The server owns metric truth. You explain what is outperforming what, by how much, and what changed inside the content to create that separation.

Inputs include:
- alert event and metric snapshot
- rulebook_context from the v4 focus rulebook
- evidence posts with full neutral fingerprints

Use the metric snapshot as boundary. Do not invent saves, shares, watch time, profile visits, or algorithm behavior.
Use every supplied evidence post. Main posts are picks; comparison posts are boundaries.
Do not create tags. Do not mention internal words like fingerprint, focus brain, memory candidate, cluster, or pattern registry.
Do not over-focus on hooks. If the reel works through a late payoff, audio turn, visual rhythm, satire, innuendo, carousel sequence, or proof device, name that instead.
Do not claim saves, shares, or private algorithm behavior unless explicitly provided.

The visible object is not an alert card. It is a defended movement thesis:
X is beating Y under Z condition.

The key move: collapse the evidence into ONE metric-backed behavioral displacement.
Do not list buckets like "gossip + satire + conflict + reactions." Those are ingredients, not the read.
Find the spine underneath them:
- premium setting -> behavior breaks the polish -> people react
- tension arrives before context -> viewer takes a side
- visual proof appears before explanation -> decision window compresses
- plain product setup -> interactive counting prompt -> comments become the task

Write from viewer pressure, not creator terminology.
Avoid generic analyst words: engagement, relatable, storytelling, personality-driven, high-conflict, reactionary format, aesthetic showcase, humble setup, content pillar.
Treat filters, hooks, formats, settings, and editing styles as implementation details. The read should explain what social/visual state the viewer enters and how fast.

Editorial shape:
- title: memorable behavioral interpretation, 2-6 words, not title case, no alert names
- metric_line: short proof line, e.g. "6 matching reels · avg top ~9% D7 · pressure holding"; no "triggered", "reinforced twice", or backend language
- read: 80-150 words. Make the argument move forward:
  1. what changed visibly
  2. why that changes viewer behavior
  3. why adjacent variants lose despite looking similar
  4. what this reveals about current account/feed pressure
- evidence_pressure: 3-5 bullets. Each bullet must add comparative proof, not repeat the read. No role labels.

Avoid symmetrical essay rhythm. Use selective emphasis. Do not over-explain the same insight.
Do not write tactical advice. No "do next" and no "watchout".
Metrics validate the argument; they should not dominate the language.
Never expose backend plumbing like alerts, triggers, cohorts, scopes, or duplicate grouping.

Shape:
- title: movement thesis
- metric_line: proof pressure in one line
- read: one continuous argument
- evidence_pressure: comparative evidence bullets

Return only JSON:
{
  "title": "",
  "metric_line": "",
  "read": "",
  "evidence_pressure": ["one comparative proof bullet"],
  "signal_type": ""
}"""

_SIGNAL_QUESTIONS = {
    "OWN_BREAKOUT_EARLY": "What single behavioral mechanism made these early breakout posts different from typical references, while keeping D3 uncertainty in mind?",
    "OWN_BREAKOUT": "What single behavioral mechanism made the breakout posts different from typical references?",
    "OWN_SUSTAIN": "What repeated mechanism is holding D7 performance, and what boundary makes it predictive instead of just descriptive?",
    "OWN_SUSTAIN_LONG": "What mechanism makes these posts durable beyond the first week?",
    "OWN_FADE": "What viewer-pressure mechanism is missing in recent weak posts compared with prior strong references?",
    "OWN_COMMENT_SPIKE": "What behavior in the content made replying feel like the natural action?",
    "OWN_LIKE_HEAVY": "What made these posts easy to approve quickly without much discussion?",
    "OWN_VIRAL_PASSIVE": "What made people keep watching or passing through without matching likes/comments?",
    "OWN_LATE_JUMP": "What delayed-payoff or context mechanism explains pickup compared with posts that did not jump?",
    "OWN_FOLLOWER_SPIKE": "What account activity in this window explains audience growth?",
    "OWN_FOLLOWER_DROP": "What account activity in this window explains audience loss?",
    "CROSS_MOMENTUM": "What common behavior is appearing across multiple feeders?",
    "CROSS_FORMAT_SHIFT": "Why is this media format currently more productive than other recent format references?",
    "CROSS_FOLLOWER_WAVE": "What shared activity explains audience movement across the feed?",
    "CROSS_MICRO_BREAKOUT": "What common breakout behavior is repeating across feeders?",
    "CROSS_MICRO_COMMENT_SPIKE": "What common content behavior is driving comments across feeders?",
    "CROSS_MICRO_LIKE_HEAVY": "What common content behavior is driving likes across feeders?",
    "CROSS_MICRO_VIRAL_PASSIVE": "What common content behavior is creating passive reach across feeders?",
    "CROSS_MICRO_FADE": "What common weakness is appearing across feeders?",
    "ANCHOR_GAP_WIDENING": "What is the primary account doing better than feed comparison posts, and what should the feed avoid?",
    "ANCHOR_GAP_CLOSING": "What are comparison accounts doing better than primary account examples, and what should the primary account avoid?",
    "ANCHOR_CHALLENGER_SURGE": "Why is this specific comparison account outperforming primary account examples, and what should the primary account avoid?",
    "ANCHOR_FOLLOWER_GAP": "What activity explains the follower-growth gap, and what should the slower side avoid?",
}


def _evidence_policy(signal: dict[str, Any]) -> dict[str, Any]:
    signal_type = str(signal.get("signal_type") or "")
    base = {
        "main_posts": "trigger posts: the posts that made the metric movement viable",
        "comparison_posts": "small visual examples only, not a statistical baseline",
        "rules": [
            "Use metric_snapshot for baseline and trigger numbers.",
            "Use comparison posts only to explain visual/content contrast.",
            "If main and comparison posts share the same behavior, say the movement is not separable yet instead of forcing a difference.",
        ],
    }
    overrides = {
        "OWN_BREAKOUT_EARLY": {
            "main_posts": "early breakout posts",
            "comparison_posts": "prior typical references from this feeder/media type",
        },
        "OWN_BREAKOUT": {
            "main_posts": "breakout posts",
            "comparison_posts": "prior typical references from this feeder/media type",
        },
        "OWN_FADE": {
            "main_posts": "recent weak posts",
            "comparison_posts": "prior strong examples from the same feeder/media type",
        },
        "OWN_LATE_JUMP": {
            "main_posts": "posts with delayed percentile pickup",
            "comparison_posts": "recent examples without a meaningful checkpoint jump",
        },
        "CROSS_FORMAT_SHIFT": {
            "main_posts": "hot posts in the rising media format",
            "comparison_posts": "recent examples from other media formats",
        },
        "ANCHOR_GAP_WIDENING": {
            "main_posts": "primary account winning posts",
            "comparison_posts": "feed examples from comparison accounts",
        },
        "ANCHOR_GAP_CLOSING": {
            "main_posts": "comparison account winning posts",
            "comparison_posts": "primary account examples",
        },
        "ANCHOR_CHALLENGER_SURGE": {
            "main_posts": "comparison account winning posts",
            "comparison_posts": "primary account examples",
        },
        "ANCHOR_FOLLOWER_GAP": {
            "main_posts": "posts from the side with stronger follower movement",
            "comparison_posts": "posts from the slower comparison side",
        },
    }
    return {**base, **overrides.get(signal_type, {})}


def _display_post_role(role: Any) -> str:
    normalized = str(role or "").strip().lower()
    labels = {
        "trigger_core": "trigger post",
        "trigger_support": "trigger post",
        "reference_typical": "typical comparison",
        "reference_strong": "strong comparison",
        "reference_no_jump": "steady comparison",
        "reference_other_format": "other-format comparison",
        "reference_anchor": "primary-account comparison",
        "reference_feed": "feed comparison",
        "reference_context": "comparison post",
    }
    return labels.get(normalized, "post reviewed")


def _display_trigger_kind(signal_type: Any) -> str:
    text = str(signal_type or "metric movement").strip().lower().replace("_", " ")
    replacements = {
        "own": "account",
        "cross": "feed-wide",
        "anchor": "primary-account",
        "challenger": "comparison-account",
        "breakout": "breakout",
        "sustain": "sustain",
        "fade": "fade",
        "follower": "audience",
    }
    words = [replacements.get(word, word) for word in text.split()]
    return " ".join(words) or "metric movement"


def is_enabled() -> bool:
    return SIGNAL_INTELLIGENCE_ENABLED


def _provider() -> str | None:
    preferred = (SIGNAL_INTELLIGENCE_PROVIDER or "auto").strip().lower()
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
    explicit = (model_override or SIGNAL_INTELLIGENCE_MODEL or "").strip()
    if explicit:
        if provider == "google" and explicit.startswith("google/"):
            return explicit.split("/", 1)[1]
        return explicit
    return _DEFAULT_OPENROUTER_MODEL if provider == "openrouter" else _DEFAULT_GEMINI_MODEL


def current_model_version(*, kind: str, model_override: str | None = None) -> str:
    provider = _provider() or "disabled"
    prompt = _FP_PROMPT_VERSION if kind == "fingerprint" else _CARD_PROMPT_VERSION
    return f"{provider}:{_model(provider, model_override) if provider != 'disabled' else 'none'}:{prompt}"


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
        ct = (resp.headers.get("content-type") or "application/octet-stream").split(";", 1)[0].strip()
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
        return body, ct
    except Exception:
        return None


def _trim_video(video_bytes: bytes, mime_type: str) -> bytes:
    tmp_in = None
    tmp_out = None
    try:
        tmp_in = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
        tmp_in.write(video_bytes)
        tmp_in.close()
        tmp_out = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
        tmp_out.close()
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            tmp_in.name,
            "-t",
            str(_VIDEO_SAMPLE_SECONDS),
            "-c",
            "copy",
            tmp_out.name,
        ]
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


def _openrouter_image_part(payload: bytes, mime_type: str) -> dict[str, Any]:
    return {"type": "image_url", "image_url": {"url": _data_url(mime_type, payload)}}


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
            json={"file": {"display_name": "feedme_signal_reel"}},
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
    system: str,
    user_text: str,
    media_parts: list[dict[str, Any]] | None = None,
    *,
    max_tokens: int = 700,
    model_override: str | None = None,
) -> dict[str, Any] | None:
    provider = _provider()
    if not provider:
        return None
    model = _model(provider, model_override)
    media_parts = media_parts or []
    try:
        if provider == "openrouter":
            content: list[dict[str, Any]] = list(media_parts)
            content.append({"type": "text", "text": user_text})
            payload = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": content},
                ],
                "temperature": 0.1,
                "max_tokens": max_tokens,
                "response_format": {"type": "json_object"},
            }
            url = f"{OPENROUTER_BASE_URL.rstrip('/')}{_OPENROUTER_CHAT_URL}"
            headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"}
            resp = requests.post(url, headers=headers, json=payload, timeout=120)
            if resp.status_code >= 400:
                payload.pop("response_format", None)
                resp = requests.post(url, headers=headers, json=payload, timeout=120)
        else:
            parts: list[dict[str, Any]] = [{"text": system}, *media_parts, {"text": user_text}]
            resp = requests.post(
                _GEMINI_API_URL.format(model=model),
                params={"key": GEMINI_API_KEY},
                json={
                    "contents": [{"parts": parts}],
                    "generationConfig": {"temperature": 0.1, "maxOutputTokens": max_tokens, "responseMimeType": "application/json"},
                },
                timeout=120,
            )
        resp.raise_for_status()
        return _json_from_text(_extract_text(resp.json(), provider))
    except Exception as exc:
        print(f"[signal-intelligence] model call failed: {exc}")
        return None


def _sample_carousel(urls: list[str]) -> list[str]:
    return [url for url in urls if str(url).strip()]


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
            select p.post_key, p.caption, lower(coalesce(p.media_type, 'image')) as media_type,
                   p.thumbnail_url, p.video_url, p.carousel_urls,
                   p.duration_seconds, p.duration_bucket, p.carousel_slide_count, p.depth_bucket,
                   fd.id as feeder_id, fd.feed_id, coalesce(fd.role, 'standard') as feeder_role,
                   fd.handle, fd.context_role, fd.context_note, fd.bio,
                   f.context_bible
            from public.posts p
            join public.feeders fd on fd.id = p.feeder_id
            join public.feeds f on f.id = fd.feed_id
            where p.post_key = %s
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
              and status in ('active', 'purge_pending')
              and coalesce(storage_path, '') <> ''
            order by asset_role
            """,
            (post_key,),
        )
        assets = cur.fetchall()

    video_url = str(post.get("video_url") or "").strip() or None
    video_asset_role = "source_video" if video_url else None
    preview_video_url: str | None = None
    thumbnail_url = str(post.get("thumbnail_url") or "").strip() or None
    carousel_urls = post.get("carousel_urls") if isinstance(post.get("carousel_urls"), list) else []
    asset_carousel: list[str] = []
    for asset in assets:
        role = str(asset.get("asset_role") or "").lower()
        url = _media_public_url(asset)
        if not url:
            continue
        if role == "video_full":
            video_url = url
            video_asset_role = "video_full"
        elif role == "preview_5s":
            preview_video_url = url
        elif role == "thumbnail":
            thumbnail_url = url
        elif role.startswith("carousel_"):
            asset_carousel.append(url)
    if str(post.get("media_type") or "").lower() == "reel" and video_asset_role != "video_full" and preview_video_url:
        video_url = preview_video_url
        video_asset_role = "preview_5s"
    if asset_carousel:
        carousel_urls = asset_carousel
    return {
        **post,
        "video_url": video_url,
        "thumbnail_url": thumbnail_url,
        "carousel_urls": carousel_urls,
        "_video_asset_role": video_asset_role,
    }


def _fingerprint_media_parts(post: dict[str, Any], provider: str) -> tuple[list[dict[str, Any]], str, str]:
    media_type = str(post.get("media_type") or "image").lower()
    source_bits: list[str] = []
    parts: list[dict[str, Any]] = []
    confidence = "low"
    if media_type in {"reel", "video"}:
        video_data = _fetch_bytes(post.get("video_url"), timeout=60, max_bytes=_VIDEO_UPLOAD_MAX_BYTES)
        if video_data:
            video_bytes, mime_type = video_data
            if not mime_type.startswith("video/"):
                mime_type = "video/mp4"
            video_bytes = _trim_video(video_bytes, mime_type)
            part = _openrouter_video_part(video_bytes, mime_type) if provider == "openrouter" else _gemini_video_part(video_bytes, mime_type)
            if part:
                parts.append(part)
                source_role = str(post.get("_video_asset_role") or "video").strip().lower()
                confidence = "medium" if source_role == "preview_5s" else "high"
                source_bits.append(f"{source_role}:{_sha(video_bytes)}")
    elif media_type in {"sidecar", "carousel"}:
        urls = _sample_carousel(post.get("carousel_urls") or [])
        fetched = 0
        for url in urls:
            image_data = _fetch_bytes(url, timeout=15, max_bytes=_IMAGE_MAX_BYTES)
            if not image_data:
                continue
            payload, mime_type = image_data
            if mime_type.startswith("image/"):
                parts.append(_openrouter_image_part(payload, mime_type) if provider == "openrouter" else {"inline_data": {"mime_type": mime_type, "data": base64.b64encode(payload).decode("ascii")}})
                source_bits.append(f"slide:{_sha(payload)}")
                fetched += 1
        confidence = "high" if fetched >= min(len(urls), 5) else "medium" if fetched > 0 else "low"
    else:
        image_data = _fetch_bytes(post.get("thumbnail_url"), timeout=15, max_bytes=_IMAGE_MAX_BYTES)
        if image_data and image_data[1].startswith("image/"):
            payload, mime_type = image_data
            parts.append(_openrouter_image_part(payload, mime_type) if provider == "openrouter" else {"inline_data": {"mime_type": mime_type, "data": base64.b64encode(payload).decode("ascii")}})
            source_bits.append(f"image:{_sha(payload)}")
            confidence = "high"
    return parts, _sha("|".join(source_bits)), confidence


def ensure_post_fingerprint(conn: Any, post_key: str) -> dict[str, Any] | None:
    provider = _provider()
    if not provider or not is_enabled():
        return None
    post = _post_media(conn, post_key)
    if not post:
        return None

    caption = str(post.get("caption") or "")
    media_parts, media_hash, confidence = _fingerprint_media_parts(post, provider)
    if not media_parts or media_hash == _sha(""):
        print(f"[signal-intelligence] fingerprint skipped post_key={post_key}: missing visual media")
        return None
    caption_hash = _sha(caption)
    model_version = current_model_version(kind="fingerprint")

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
            (post_key, media_hash, caption_hash, _SAMPLING_POLICY_VERSION, model_version),
        )
        existing = cur.fetchone()
        if existing and isinstance(existing.get("fingerprint"), dict):
            return existing["fingerprint"]

    # Release the DB transaction before the potentially slow media/model call so
    # the pooler does not hold an idle transaction while the LLM is thinking.
    try:
        conn.commit()
    except Exception:
        pass

    user_text = "\n".join([
        f"POST_ID: {post_key}",
        (
            f"MEDIA: {post.get('media_type') or 'unknown'} "
            f"duration_seconds={post.get('duration_seconds') or ''} "
            f"duration_bucket={post.get('duration_bucket') or ''} "
            f"carousel_slide_count={post.get('carousel_slide_count') or ''} "
            f"depth_bucket={post.get('depth_bucket') or ''}"
        ),
        f"CAPTION: {caption[:6000] or '(no caption)'}",
    ])
    fingerprint = _call_model(_FINGERPRINT_SYSTEM, user_text, media_parts, max_tokens=2600)
    if not fingerprint:
        return None
    media_confidence = confidence
    fingerprint["post_id"] = str(fingerprint.get("post_id") or post_key)
    fingerprint["media_type"] = str(fingerprint.get("media_type") or post.get("media_type") or "")
    fingerprint["duration_seconds"] = fingerprint.get("duration_seconds", post.get("duration_seconds"))
    fingerprint["carousel_slide_count"] = fingerprint.get("carousel_slide_count", post.get("carousel_slide_count"))
    fingerprint["media_confidence"] = media_confidence
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
            (post_key, json.dumps(fingerprint), media_hash, caption_hash, _SAMPLING_POLICY_VERSION, model_version, media_confidence),
        )
    conn.commit()
    return fingerprint


def _signal_payload(conn: Any, signal_id: int) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select s.*, f.context_bible
            from public.signals s
            join public.feeds f on f.id = s.feed_id
            where s.id = %s
              and s.status in ('pending', 'fresh', 'stale', 'error')
            limit 1
            """,
            (signal_id,),
        )
        signal = cur.fetchone()
        if not signal:
            return None
        cur.execute(
            """
            select sp.cohort, sp.rank, sp.role, sp.post_key,
                   p.post_url, p.caption, lower(coalesce(p.media_type, 'image')) as media_type,
                   fd.id as feeder_id, fd.feed_id, fd.handle, coalesce(fd.role, 'standard') as feeder_role,
                   fd.context_role, fd.context_note, fd.bio,
                   pf.fingerprint
            from public.signal_posts sp
            join public.posts p on p.post_key = sp.post_key
            join public.feeders fd on fd.id = p.feeder_id
            left join public.post_fingerprints pf on pf.post_key = sp.post_key
            where sp.signal_id = %s
            order by sp.cohort, sp.rank, sp.post_key
            """,
            (signal_id,),
        )
        posts = cur.fetchall()
    return {"signal": signal, "posts": posts}


def _recent_signal_cards(conn: Any, signal: dict[str, Any], signal_id: int, *, limit: int = 10) -> list[dict[str, Any]]:
    feed_id = signal.get("feed_id")
    if feed_id is None:
        return []
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select si.card
            from public.signal_intelligence si
            join public.signals s on s.id = si.signal_id
            where s.feed_id = %s
              and s.id <> %s
              and si.card is not null
            order by si.updated_at desc, si.signal_id desc
            limit %s
            """,
            (feed_id, signal_id, max(1, limit)),
        )
        rows = cur.fetchall()
    cards: list[dict[str, Any]] = []
    for row in rows:
        card = row.get("card")
        if isinstance(card, dict):
            cards.append(card)
    return cards


def _sample_hash(
    posts: list[dict[str, Any]],
    focus_context: dict[str, Any] | None = None,
    signal: dict[str, Any] | None = None,
) -> str:
    bits = []
    for row in posts:
        fingerprint = row.get("fingerprint") if isinstance(row.get("fingerprint"), dict) else {}
        bits.append(
            ":".join([
                str(row.get("cohort") or ""),
                str(row.get("role") or ""),
                str(row.get("post_key") or ""),
                _sha(fingerprint),
            ])
        )
    signal_context = {}
    if isinstance(signal, dict):
        signal_context = {
            "signal_type": signal.get("signal_type"),
            "scope": signal.get("scope"),
            "media_type": signal.get("media_type"),
            "sub_bucket": signal.get("sub_bucket"),
            "checkpoint": signal.get("checkpoint"),
            "business_date_ist": signal.get("business_date_ist"),
            "trigger_window_start": signal.get("trigger_window_start"),
            "trigger_window_end": signal.get("trigger_window_end"),
            "metric_snapshot": signal.get("metric_snapshot") or {},
            "metric_classification": signal.get("metric_classification") or _metric_classification(signal),
            "body": signal.get("body"),
            "context_bible": signal.get("context_bible") or "",
        }
    return _sha({
        "posts": bits,
        "focus_context": focus_context or {},
        "signal_context": signal_context,
    })


def _missing_fingerprint_post_keys(posts: list[dict[str, Any]]) -> list[str]:
    missing: list[str] = []
    for row in posts:
        post_key = str(row.get("post_key") or "").strip()
        if post_key and (not isinstance(row.get("fingerprint"), dict) or not row.get("fingerprint")):
            missing.append(post_key)
    return missing


def _numeric_percentiles(value: Any) -> list[float]:
    found: list[float] = []
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key or "").lower()
            if "percentile" in key_text or key_text in {"pctl", "avg_hot_percentile", "best_percentile"}:
                try:
                    if child is not None:
                        found.append(float(child))
                except (TypeError, ValueError):
                    pass
            found.extend(_numeric_percentiles(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(_numeric_percentiles(child))
    return [item for item in found if 0 <= item <= 100]


def _num(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
        return parsed if parsed == parsed and parsed not in {float("inf"), float("-inf")} else None
    except (TypeError, ValueError):
        return None


def _first_num(mapping: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _num(mapping.get(key))
        if value is not None:
            return value
    return None


def _label_from_score(score: int) -> str:
    if score >= 3:
        return "extreme"
    if score == 2:
        return "sharp"
    if score == 1:
        return "notable"
    return "minor"


def _metric_classification(signal: dict[str, Any]) -> dict[str, str]:
    snapshot = signal.get("metric_snapshot") if isinstance(signal.get("metric_snapshot"), dict) else {}
    signal_type = str(signal.get("signal_type") or "").upper()
    direction = "rising"
    if any(token in signal_type for token in ("DROP", "FADE", "LOSS", "CHURN")):
        direction = "declining"

    net_7d = _first_num(snapshot, "net_7d", "net_30d", "anchor_gain")
    feed_median_gain = _first_num(snapshot, "feed_median_gain")
    if net_7d is not None:
        direction = "rising" if net_7d > 0 else "declining" if net_7d < 0 else "stable"
    elif feed_median_gain is not None and net_7d is not None:
        direction = "rising" if net_7d >= feed_median_gain else "declining"

    magnitude_score = 0
    percentiles = _numeric_percentiles(snapshot)
    best_percentile = min(percentiles) if percentiles else None
    if best_percentile is not None:
        if best_percentile <= 5:
            magnitude_score = max(magnitude_score, 3)
        elif best_percentile <= 15:
            magnitude_score = max(magnitude_score, 2)
        elif best_percentile <= 30:
            magnitude_score = max(magnitude_score, 1)

    gap = abs(_first_num(snapshot, "gap") or 0)
    if gap >= 20:
        magnitude_score = max(magnitude_score, 3)
    elif gap >= 12:
        magnitude_score = max(magnitude_score, 2)
    elif gap >= 8:
        magnitude_score = max(magnitude_score, 1)

    volatility = abs(_first_num(snapshot, "volatility") or 0)
    weekly_rate = abs(_first_num(snapshot, "weekly_rate") or 0)
    latest_count = abs(_first_num(snapshot, "latest_count") or 0)
    net_abs = abs(_first_num(snapshot, "net_7d") or 0)
    ratios = []
    if volatility > 0 and net_abs > 0:
        ratios.append(net_abs / volatility)
    if weekly_rate > 0 and net_abs > 0:
        ratios.append(net_abs / weekly_rate)
    if latest_count > 0 and net_abs > 0:
        ratios.append(net_abs / latest_count * 100)
    ratio = max(ratios, default=0)
    if ratio >= 5:
        magnitude_score = max(magnitude_score, 3)
    elif ratio >= 3:
        magnitude_score = max(magnitude_score, 2)
    elif ratio >= 1.5:
        magnitude_score = max(magnitude_score, 1)

    rate = _first_num(snapshot, "affected_rate", "recent_feeder_rate", "recent_hot_rate")
    prior_rate = _first_num(snapshot, "prior_feeder_rate", "prior_hot_rate")
    if rate is not None:
        delta = rate - (prior_rate or 0)
        if rate >= 0.6 or delta >= 0.35:
            magnitude_score = max(magnitude_score, 3)
        elif rate >= 0.4 or delta >= 0.2:
            magnitude_score = max(magnitude_score, 2)
        elif rate >= 0.25 or delta >= 0.1:
            magnitude_score = max(magnitude_score, 1)

    count = int(_first_num(snapshot, "affected_feeders", "contributing_feeders", "post_count", "challenger_count") or 0)
    if rate is not None:
        breadth = "dominant" if rate >= 0.6 else "broad" if rate >= 0.3 else "isolated"
    elif count >= 5:
        breadth = "broad"
    else:
        breadth = "isolated"

    if direction == "stable":
        vs_baseline = "matches"
    elif direction == "declining":
        vs_baseline = "below_normal"
    else:
        vs_baseline = "above_normal"

    return {
        "magnitude": _label_from_score(magnitude_score),
        "direction": direction,
        "breadth": breadth,
        "vs_baseline": vs_baseline,
        "source": "server_derived_v1",
    }


def _tier1_metric_reason(signal: dict[str, Any]) -> str | None:
    values = _numeric_percentiles(signal.get("metric_snapshot") or {})
    for key in ("surface_percentile", "avg_hot_percentile", "best_percentile"):
        try:
            if signal.get(key) is not None:
                values.append(float(signal.get(key)))
        except (TypeError, ValueError):
            pass
    best = min(values) if values else None
    if best is not None and best <= 20:
        return f"tier1_metric_percentile_p{round(best, 2)}"
    return None


_CARD_STRING_FIELDS = ("title", "metric_line", "read", "signal_type")
_CARD_LIST_FIELDS = ("evidence_pressure",)
_CARD_COPY_FIELDS = ("title", "metric_line", "read")
_FORBIDDEN_INTERNAL = re.compile(
    r"\b(?:cohort|anchor|challenger|bible|feed_focus|signals?|pattern_id|cross[-\s]feed|"
    r"fingerprints?|trigger_core|trigger_support|reference_no_jump|reference_typical|"
    r"reference_strong|alerts?|triggers?|duplicates?|backend)\b",
    re.IGNORECASE,
)
_WEAK_ANALYST_VOCAB = re.compile(
    r"\b(?:engagement|relatable|storytelling|personality-driven|high-conflict|"
    r"reactionary\s+formats?|aesthetic\s+showcases?|humble\s+setups?|"
    r"content\s+pillars?|leaning\s+(?:heavily\s+)?into)\b",
    re.IGNORECASE,
)
_STATIC_TERM_RE = re.compile(r"\bstatics?\b", re.IGNORECASE)
_STATIC_MEDIA_TYPES = {"image", "photo", "picture", "static"}


def _word_count(text: Any) -> int:
    return len(re.findall(r"\b[\w'-]+\b", str(text or "")))


def _looks_title_case(text: str) -> bool:
    words = re.findall(r"\b[A-Za-z][A-Za-z'-]*\b", text)
    significant = [word for word in words if len(word) > 2 and not word.isupper()]
    if len(significant) < 3:
        return False
    titled = [word for word in significant if word[:1].isupper() and word[1:].islower()]
    return len(titled) >= max(3, len(significant) - 1)


def _card_copy_items(card: dict[str, Any]) -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = []
    for key in _CARD_COPY_FIELDS:
        value = card.get(key)
        if isinstance(value, str):
            items.append((key, value))
    for key in _CARD_LIST_FIELDS:
        values = card.get(key)
        if isinstance(values, list):
            for index, value in enumerate(values):
                if isinstance(value, str):
                    items.append((f"{key}[{index}]", value))
    return items


def _card_schema_errors(card: Any) -> list[str]:
    if not isinstance(card, dict):
        return ["card_not_object"]
    errors: list[str] = []
    for key in _CARD_STRING_FIELDS:
        if key not in card:
            errors.append(f"missing_{key}")
        elif not isinstance(card.get(key), str):
            errors.append(f"{key}_not_string")
    for key in _CARD_LIST_FIELDS:
        if key not in card:
            errors.append(f"missing_{key}")
        elif not isinstance(card.get(key), list):
            errors.append(f"{key}_not_list")
        else:
            for index, value in enumerate(card.get(key) or []):
                if not isinstance(value, str):
                    errors.append(f"{key}_{index}_not_string")
    title = str(card.get("title") or "")
    if _word_count(title) > 7:
        errors.append("title_too_long")
    if _looks_title_case(title):
        errors.append("title_looks_title_case")
    metric_line = str(card.get("metric_line") or "")
    if _word_count(metric_line) > 16:
        errors.append("metric_line_too_long")
    if re.search(r"\b(?:triggered|trigger|alert|reinforced twice|duplicate|backend)\b", metric_line, re.IGNORECASE):
        errors.append("metric_line_contains_backend_vocab")
    read_count = _word_count(card.get("read"))
    if read_count > 160:
        errors.append("read_too_long")
    if read_count < 45:
        errors.append("read_too_short")
    if isinstance(card.get("evidence_pressure"), list):
        if len(card.get("evidence_pressure") or []) > 5:
            errors.append("evidence_pressure_too_many")
        if len(card.get("evidence_pressure") or []) < 2:
            errors.append("evidence_pressure_too_few")
        for index, item in enumerate(card.get("evidence_pressure") or []):
            if isinstance(item, str) and _word_count(item) > 24:
                errors.append(f"evidence_pressure_{index}_too_long")
    for key, value in _card_copy_items(card):
        if _FORBIDDEN_INTERNAL.search(value):
            errors.append(f"{key}_contains_internal_vocab")
        if _WEAK_ANALYST_VOCAB.search(value):
            errors.append(f"{key}_contains_weak_analyst_vocab")
    return errors


def _static_allowed(signal: dict[str, Any], fingerprints: list[dict[str, Any]]) -> bool:
    signal_type = str(signal.get("signal_type") or "").upper()
    if "FORMAT_SHIFT" in signal_type:
        return True
    media_values = {str(signal.get("media_type") or "").strip().lower()}
    for post in fingerprints:
        media_values.add(str(post.get("media_type") or "").strip().lower())
    return bool(media_values & _STATIC_MEDIA_TYPES)


def _card_context_errors(card: Any, signal: dict[str, Any], fingerprints: list[dict[str, Any]]) -> list[str]:
    if not isinstance(card, dict) or _static_allowed(signal, fingerprints):
        return []
    errors: list[str] = []
    for key, value in _card_copy_items(card):
        if _STATIC_TERM_RE.search(value):
            errors.append(f"{key}_static_without_static_lane")
    return errors


def _repair_context_terms(card: Any, context_errors: list[str]) -> Any:
    if not isinstance(card, dict) or not any("static_without_static_lane" in error for error in context_errors):
        return card
    repaired = dict(card)

    def replace_static(text: str) -> str:
        text = re.sub(r"\bstatic product shots?\b", "plain product shots", text, flags=re.IGNORECASE)
        text = re.sub(r"\bstatic setups?\b", "plain setups", text, flags=re.IGNORECASE)
        text = re.sub(r"\bstatic shots?\b", "plain shots", text, flags=re.IGNORECASE)
        text = re.sub(r"\bstatics?\b", "plain posts", text, flags=re.IGNORECASE)
        return text

    for key in _CARD_COPY_FIELDS:
        if isinstance(repaired.get(key), str):
            repaired[key] = replace_static(str(repaired.get(key) or ""))
    for key in _CARD_LIST_FIELDS:
        if isinstance(repaired.get(key), list):
            repaired[key] = [
                replace_static(item) if isinstance(item, str) else item
                for item in repaired.get(key) or []
            ]
    return repaired


def _repair_guardrail_terms(card: Any, errors: list[str]) -> Any:
    if not isinstance(card, dict):
        return card
    if not any(
        marker in error
        for error in errors
        for marker in ("contains_internal_vocab", "contains_weak_analyst_vocab")
    ):
        return card
    repaired = dict(card)

    replacements = [
        (r"\bfingerprints?\b", "post reads"),
        (r"\bsignals?\b", "movement"),
        (r"\balerts?\b", "movement"),
        (r"\btriggers?\b", "checks"),
        (r"\bduplicates?\b", "repeats"),
        (r"\bbackend\b", "system"),
        (r"\bcohort\b", "evidence group"),
        (r"\banchor\b", "primary account"),
        (r"\bchallenger\b", "comparison account"),
        (r"\bengagement\b", "reaction"),
        (r"\brelatable\b", "recognizable"),
        (r"\bstorytelling\b", "setup"),
        (r"\bpersonality-driven\b", "behavior-led"),
        (r"\bhigh-conflict\b", "tension-first"),
        (r"\breactionary\s+formats?\b", "reaction setup"),
        (r"\baesthetic\s+showcases?\b", "clean presentation"),
        (r"\bhumble\s+setups?\b", "quiet setup"),
        (r"\bcontent\s+pillars?\b", "repeatable moves"),
        (r"\bleaning\s+(?:heavily\s+)?into\b", "using"),
    ]

    def replace_terms(text: str) -> str:
        out = text
        for pattern, replacement in replacements:
            out = re.sub(pattern, replacement, out, flags=re.IGNORECASE)
        return out

    for key in _CARD_COPY_FIELDS:
        if isinstance(repaired.get(key), str):
            repaired[key] = replace_terms(str(repaired.get(key) or ""))
    for key in _CARD_LIST_FIELDS:
        if isinstance(repaired.get(key), list):
            repaired[key] = [
                replace_terms(item) if isinstance(item, str) else item
                for item in repaired.get(key) or []
            ]
    return repaired


def _trim_words(text: Any, max_words: int) -> str:
    value = str(text or "").strip()
    words = re.findall(r"\S+", value)
    if len(words) <= max_words:
        return value
    boundary = re.search(r"^((?:[^.!?]+[.!?]){1,3})", value)
    if boundary:
        candidate = boundary.group(1).strip()
        if 0 < _word_count(candidate) <= max_words:
            return candidate
    trimmed = " ".join(words[:max_words]).rstrip(" ,;:.-")
    while _word_count(trimmed) > max_words and " " in trimmed:
        trimmed = trimmed.rsplit(" ", 1)[0].rstrip(" ,;:.-")
    return trimmed + "..."


def _stringify_post_note(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        post_key = str(value.get("post_key") or value.get("post") or value.get("id") or "").strip()
        role = str(value.get("role") or value.get("post_role") or value.get("type") or "").strip()
        note = ""
        for key in ("note", "read", "observation", "summary", "what_happened", "why", "body", "point"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                note = candidate.strip()
                break
        if not note:
            text_parts = [
                str(child).strip()
                for child in value.values()
                if isinstance(child, (str, int, float)) and str(child).strip()
            ]
            note = " ".join(text_parts[:3]).strip()
        prefix = " ".join(part for part in (role, post_key) if part).strip()
        return f"{prefix}: {note}".strip(": ").strip()
    if isinstance(value, list):
        return " ".join(str(item).strip() for item in value if str(item).strip())
    return str(value or "").strip()


def _normalize_card_tag_aliases(card: Any) -> Any:
    if not isinstance(card, dict):
        return card
    normalized = dict(card)
    if not isinstance(normalized.get("read"), str) or not normalized.get("read"):
        legacy_parts = [
            str(normalized.get("what_happened") or "").strip(),
            str(normalized.get("why") or "").strip(),
        ]
        normalized["read"] = " ".join(part for part in legacy_parts if part).strip()
    normalized.pop("what_happened", None)
    normalized.pop("why", None)
    normalized.pop("do_next", None)
    normalized.pop("watchout", None)
    normalized.pop("pattern_type", None)
    normalized.pop("per_post_notes", None)
    normalized.pop("mechanic_tags", None)
    normalized.pop("execution_tags", None)
    normalized.pop("common_pattern", None)
    normalized.pop("focus_memory_candidate", None)
    if isinstance(normalized.get("read"), str):
        normalized["read"] = _trim_words(normalized["read"], 160)
    if isinstance(normalized.get("metric_line"), str):
        normalized["metric_line"] = _trim_words(normalized["metric_line"], 16)
    if isinstance(normalized.get("title"), str):
        normalized["title"] = _trim_words(normalized["title"], 7)
    if "evidence_pressure" in normalized:
        notes = normalized.get("evidence_pressure")
        if isinstance(notes, list):
            normalized["evidence_pressure"] = [
                _trim_words(_stringify_post_note(item), 20)
                for item in notes[:5]
                if _stringify_post_note(item)
            ]
        elif notes is None:
            normalized["evidence_pressure"] = []
        else:
            note = _stringify_post_note(notes)
            normalized["evidence_pressure"] = [_trim_words(note, 24)] if note else []
    return normalized


def _card_validation_errors(
    card: Any,
    signal: dict[str, Any],
    fingerprints: list[dict[str, Any]],
    recent_cards: list[dict[str, Any]],
) -> list[str]:
    return [
        *_card_schema_errors(card),
        *_card_context_errors(card, signal, fingerprints),
    ]


def _business_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).date()
        except ValueError:
            try:
                return date.fromisoformat(value.strip()[:10])
            except ValueError:
                return None
    return None


def _recency_score(signal: dict[str, Any]) -> float:
    business_day = _business_date(signal.get("business_date_ist"))
    if not business_day:
        return 0.10
    age = max(0, (datetime.now(timezone.utc).date() - business_day).days)
    if age <= 7:
        return 0.18
    if age <= 14:
        return 0.14
    if age <= 30:
        return 0.09
    return 0.05


def _cohort_size_score(posts: list[dict[str, Any]]) -> float:
    keys = {
        str(row.get("post_key") or "")
        for row in posts
        if str(row.get("cohort") or "").lower() == "a" and str(row.get("post_key") or "").strip()
    }
    count = len(keys)
    if count >= 5:
        return 0.30
    if count >= 3:
        return 0.23
    if count >= 2:
        return 0.15
    if count == 1:
        return 0.08
    return 0.04


def _magnitude_score(metric_classification: dict[str, str]) -> float:
    return {
        "extreme": 0.34,
        "sharp": 0.28,
        "notable": 0.20,
        "minor": 0.10,
    }.get(str(metric_classification.get("magnitude") or "").lower(), 0.10)


def _pattern_stability_score(posts: list[dict[str, Any]]) -> float:
    trigger_posts = [row for row in posts if str(row.get("cohort") or "").lower() == "a"]
    if not trigger_posts:
        return 0.08
    fingerprinted = sum(1 for row in trigger_posts if isinstance(row.get("fingerprint"), dict) and row.get("fingerprint"))
    if fingerprinted >= 5:
        return 0.18
    if fingerprinted >= 3:
        return 0.15
    return 0.10


def _computed_confidence(signal: dict[str, Any], posts: list[dict[str, Any]], metric_classification: dict[str, str]) -> str:
    score = (
        _cohort_size_score(posts)
        + _magnitude_score(metric_classification)
        + _pattern_stability_score(posts)
        + _recency_score(signal)
    )
    if score >= 0.72:
        return "high"
    if score >= 0.45:
        return "medium"
    return "low"


def _deterministic_escalation_reasons(signal: dict[str, Any], posts: list[dict[str, Any]]) -> list[str]:
    reasons: list[str] = []
    tier1 = _tier1_metric_reason(signal)
    if tier1:
        reasons.append(tier1)
    return reasons


def _language_safe_metric_snapshot(value: Any) -> Any:
    replacements = {
        "anchor": "primary_account",
        "challenger": "comparison_account",
        "cohort": "evidence_group",
        "signal": "movement",
    }
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, child in value.items():
            safe_key = str(key)
            for needle, replacement in replacements.items():
                safe_key = re.sub(needle, replacement, safe_key, flags=re.IGNORECASE)
            out[safe_key] = _language_safe_metric_snapshot(child)
        return out
    if isinstance(value, list):
        return [_language_safe_metric_snapshot(child) for child in value]
    return value


def _card_user_text(
    signal: dict[str, Any],
    focus_context: dict[str, Any],
    fingerprints: list[dict[str, Any]],
    metric_classification: dict[str, str],
    escalation_reasons: list[str],
    card_model_override: str | None,
    recent_cards: list[dict[str, Any]] | None = None,
    validation_errors: list[str] | None = None,
) -> str:
    payload: dict[str, Any] = {
        "trigger_kind": _display_trigger_kind(signal.get("signal_type")),
        "question": _SIGNAL_QUESTIONS.get(str(signal.get("signal_type") or ""), "What explains this metric-triggered signal?"),
        "feed_context": signal.get("context_bible") or "(not provided)",
        "rulebook_context": focus_context,
        "media_type": signal.get("media_type"),
        "sub_bucket": signal.get("sub_bucket"),
        "signal_type": signal.get("signal_type"),
        "metric_snapshot": _language_safe_metric_snapshot(signal.get("metric_snapshot") or {}),
        "metric_classification": metric_classification,
        "evidence_policy": _evidence_policy(signal),
        "writing_rules": {
            "core_task": "write one movement thesis: X is beating Y under Z condition",
            "movement_test": "If the read says X, Y, Z worked, rewrite it as what those winners displaced or beat.",
            "sequence": "visible shift -> viewer behavior implication -> adjacent variant contrast -> account/feed pressure conclusion",
            "viewer_pressure_examples": [
                "premium setting -> tone break -> reaction",
                "tension before understanding -> viewer takes a side",
                "payoff before process -> decision window compresses",
                "numbered visual task -> comments become the action",
            ],
            "implementation_not_intelligence": [
                "filters",
                "hooks",
                "formats",
                "editing styles",
                "settings",
                "topic labels",
            ],
            "avoid_words": [
                "engagement",
                "relatable",
                "storytelling",
                "personality-driven",
                "high-conflict",
                "reactionary format",
                "aesthetic showcase",
                "humble setup",
                "reinforced twice",
                "triggered",
            ],
        },
        "model_route": {
            "escalated_to_pro": bool(card_model_override),
            "deterministic_reasons": escalation_reasons,
        },
        "cohort_posts": fingerprints,
    }
    if validation_errors:
        payload["rewrite_retry"] = {
            "validation_errors": validation_errors[:12],
            "instruction": (
                "Rewrite the whole card. Preserve the insight, but satisfy the output contract exactly. "
                "Return only title, metric_line, read, evidence_pressure, and signal_type. "
                "No do_next, no watchout, no pattern_type, no confidence field, no tag fields. "
                "Collapse the evidence into one metric-backed behavioral displacement instead of listing content buckets. "
                "Each paragraph must advance the argument, not paraphrase the same insight. "
                "Avoid weak analyst words like engagement, relatable, storytelling, personality-driven, high-conflict, reactionary format, aesthetic showcase, or humble setup. "
                "Use static language only when this signal's media lane or comparison posts are static/image. "
                "Replace anchor with primary account, challenger with comparison account, "
                "cohort with evidence group, signal with movement, and fingerprint with post read. "
                "Do not expose backend words like trigger, alert, duplicate, or reinforced twice."
            ),
        }
    return json.dumps(payload, default=str)


def resolve_signal_intelligence(conn: Any, signal_id: int | None = None, *, limit: int = 1) -> dict[str, int]:
    if not is_enabled():
        return {"selected": 0, "resolved": 0, "failed": 0}
    with conn.cursor(row_factory=dict_row) as cur:
        if signal_id is not None:
            cur.execute("select id from public.signals where id = %s", (signal_id,))
        else:
            cur.execute(
                """
                select id
                from public.signals
                where (
                    status in ('pending', 'stale')
                    or (status = 'error' and updated_at <= now() - interval '30 minutes')
                  )
                  and scope in ('own', 'cross', 'anchor')
                order by business_date_ist desc, created_at desc, id desc
                limit %s
                """,
                (max(1, limit),),
            )
        ids = [int(row["id"]) for row in cur.fetchall()]

    resolved = 0
    failed = 0
    for sid in ids:
        payload = _signal_payload(conn, sid)
        if not payload:
            continue
        posts = payload["posts"]
        for row in posts:
            if not isinstance(row.get("fingerprint"), dict) or not row.get("fingerprint"):
                ensure_post_fingerprint(conn, str(row.get("post_key") or ""))
        payload = _signal_payload(conn, sid)
        if not payload:
            continue
        posts = payload["posts"]
        missing_fingerprints = _missing_fingerprint_post_keys(posts)
        if missing_fingerprints:
            with conn.cursor() as cur:
                cur.execute("update public.signals set status = 'error', updated_at = now() where id = %s", (sid,))
            conn.commit()
            print(
                "[signal-intelligence] missing fingerprints "
                f"signal_id={sid} post_keys={missing_fingerprints[:8]}"
            )
            failed += 1
            continue
        signal = payload["signal"]
        posts = payload["posts"]
        focus_context = signal_rulebook_context(conn, signal, posts)
        metric_classification = _metric_classification(signal)
        computed_confidence = _computed_confidence(signal, posts, metric_classification)
        escalation_reasons = _deterministic_escalation_reasons(signal, posts)
        card_model_override = SIGNAL_INTELLIGENCE_ESCALATION_MODEL
        fingerprints = []
        for row in posts:
            fp = row.get("fingerprint") if isinstance(row.get("fingerprint"), dict) else {}
            fingerprints.append({
                "post_key": row.get("post_key"),
                "evidence_group": "main" if str(row.get("cohort") or "").lower() == "a" else "comparison",
                "post_role": _display_post_role(row.get("role")),
                "post_url": row.get("post_url"),
                "media_type": row.get("media_type"),
                "handle": row.get("handle"),
                "feeder_role": row.get("feeder_role"),
                "feeder_context_role": row.get("context_role"),
                "feeder_note": row.get("context_note"),
                "feeder_bio": row.get("bio"),
                "caption_excerpt": str(row.get("caption") or "")[:500],
                "fingerprint": fp,
            })
        recent_cards = _recent_signal_cards(conn, signal, sid)
        signal_for_hash = {
            **signal,
            "metric_classification": metric_classification,
        }
        sample_hash = _sample_hash(posts, focus_context, signal_for_hash)
        card_model = current_model_version(kind="card", model_override=card_model_override)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                select card
                from public.signal_intelligence
                where signal_id = %s and sample_hash = %s and model_version = %s
                """,
                (sid, sample_hash, card_model),
            )
            existing = cur.fetchone()
        if existing:
            with conn.cursor() as cur:
                cur.execute("update public.signals set status = 'fresh', updated_at = now() where id = %s", (sid,))
            conn.commit()
            resolved += 1
            continue

        user_text = _card_user_text(
            signal,
            focus_context,
            fingerprints,
            metric_classification,
            escalation_reasons,
            card_model_override,
            recent_cards=recent_cards,
        )
        card = _normalize_card_tag_aliases(_call_model(_CARD_SYSTEM, user_text, [], max_tokens=2200, model_override=card_model_override))
        if isinstance(card, dict) and not card.get("signal_type"):
            card["signal_type"] = str(signal.get("signal_type") or "")
        schema_errors = _card_validation_errors(card, signal, fingerprints, recent_cards)
        if schema_errors:
            if not card_model_override:
                escalation_reasons = [*escalation_reasons, f"schema_validation_failed:{','.join(schema_errors[:5])}"]
                card_model_override = SIGNAL_INTELLIGENCE_ESCALATION_MODEL
                card_model = current_model_version(kind="card", model_override=card_model_override)
                print(f"[signal-intelligence] escalating signal_id={sid} reasons={escalation_reasons}")
            else:
                escalation_reasons = [*escalation_reasons, f"schema_validation_retry:{','.join(schema_errors[:5])}"]
                print(f"[signal-intelligence] retrying signal_id={sid} reasons={escalation_reasons}")
            user_text = _card_user_text(
                signal,
                focus_context,
                fingerprints,
                metric_classification,
                escalation_reasons,
                card_model_override,
                recent_cards=recent_cards,
                validation_errors=schema_errors,
            )
            card = _normalize_card_tag_aliases(_call_model(_CARD_SYSTEM, user_text, [], max_tokens=2600, model_override=card_model_override))
            if isinstance(card, dict) and not card.get("signal_type"):
                card["signal_type"] = str(signal.get("signal_type") or "")
            schema_errors = _card_validation_errors(card, signal, fingerprints, recent_cards)
            if schema_errors and any("static_without_static_lane" in error for error in schema_errors):
                card = _normalize_card_tag_aliases(_repair_context_terms(card, schema_errors))
                schema_errors = _card_validation_errors(card, signal, fingerprints, recent_cards)
            if schema_errors and any(
                "contains_internal_vocab" in error or "contains_weak_analyst_vocab" in error or error.endswith("_too_long")
                for error in schema_errors
            ):
                card = _normalize_card_tag_aliases(_repair_guardrail_terms(card, schema_errors))
                schema_errors = _card_validation_errors(card, signal, fingerprints, recent_cards)
        if not card or schema_errors:
            with conn.cursor() as cur:
                cur.execute("update public.signals set status = 'error', updated_at = now() where id = %s", (sid,))
            conn.commit()
            if schema_errors:
                print(f"[signal-intelligence] card schema failed signal_id={sid} errors={schema_errors}")
            failed += 1
            continue
        card.pop("confidence", None)
        card.pop("why_it_may_have_happened", None)
        card.pop("focus_memory_candidate", None)
        focus_memory_candidate: dict[str, Any] = {}
        card["confidence"] = computed_confidence
        signal_status = "fresh"
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.signal_intelligence (
                  signal_id, card, sample_hash, model_version, focus_context,
                  focus_memory_candidate, generated_at, updated_at
                )
                values (%s, %s::jsonb, %s, %s, %s::jsonb, %s::jsonb, now(), now())
                on conflict (signal_id) do update set
                  card = excluded.card,
                  sample_hash = excluded.sample_hash,
                  model_version = excluded.model_version,
                  focus_context = excluded.focus_context,
                  focus_memory_candidate = excluded.focus_memory_candidate,
                  updated_at = now()
                """,
                (sid, json.dumps(card), sample_hash, card_model, json.dumps(focus_context), json.dumps(focus_memory_candidate)),
            )
            cur.execute("update public.signals set status = %s, updated_at = now() where id = %s", (signal_status, sid))
        conn.commit()
        resolved += 1
    return {"selected": len(ids), "resolved": resolved, "failed": failed}
