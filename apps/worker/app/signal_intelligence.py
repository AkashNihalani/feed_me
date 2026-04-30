from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
import tempfile
import time
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
from .focus_brain import (
    ensure_post_focus_read,
    feed_focus_context,
)

_GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
_GEMINI_UPLOAD_URL = "https://generativelanguage.googleapis.com/upload/v1beta/files"
_GEMINI_FILE_URL = "https://generativelanguage.googleapis.com/v1beta/files/{name}"
_OPENROUTER_CHAT_URL = "/chat/completions"
_DEFAULT_OPENROUTER_MODEL = "google/gemini-3-flash-preview"
_DEFAULT_GEMINI_MODEL = "gemini-3-flash-preview"
_FP_PROMPT_VERSION = "fingerprint_v4_neutral"
_CARD_PROMPT_VERSION = "signal_card_v5_deterministic_escalation"
_SAMPLING_POLICY_VERSION = "media_sample_v1"
_VIDEO_UPLOAD_MAX_BYTES = 50 * 1024 * 1024
_VIDEO_INLINE_MAX_BYTES = 20 * 1024 * 1024
_IMAGE_MAX_BYTES = 12 * 1024 * 1024
_VIDEO_SAMPLE_SECONDS = 90

_FINGERPRINT_SYSTEM = """Describe what this post actually contains.
Use visual evidence first. Caption is supporting context only. If media evidence is partial, say so.
Use only what you can see or read. Do not invent niche, brand, or campaign details.
Do not explain performance. Do not say if the post is good, bad, risky, emerging, or strategic.
Do not compare this post to the account's history. You are making a neutral fingerprint only.

Return only JSON:
{
  "content_summary": "",
  "topic": "",
  "audience_addressed": "",
  "hook": "",
  "opener": "",
  "payoff": "",
  "visual_sequence": "",
  "caption_role": "",
  "audio_or_text_driver": "",
  "emotional_trigger": "",
  "discussion_prompt": "",
  "craft_moves": [],
  "campaign_or_context_clues": [],
  "visual_read": {
    "opening_frame": "",
    "subject_focus": "",
    "setting": "",
    "camera_language": "",
    "production_style": "",
    "human_presence": "",
    "proof_shown": [],
    "pacing": "",
    "on_screen_text_style": ""
  },
  "caption_read": {
    "tone": "",
    "voice": "",
    "language_style": "",
    "specificity": "",
    "cta_style": "",
    "emotional_register": "",
    "caption_confidence": "high|medium|low"
  },
  "media_confidence": "high|medium|low"
}"""

_CARD_SYSTEM = """You analyze metric-triggered social signals for a social media tracking product.
Use the feed bible, feed focus capsule, focus reads, feeder context, metric snapshot, cohort policy, and post fingerprints only.
The metric_snapshot is the only statistical baseline truth. Do not calculate a new baseline from cohort posts.
Cohort B/reference posts are visual references only; they are not the statistical baseline.
Use visual evidence first. Caption is supporting context only.
Do not claim saves, shares, or private algorithm behavior unless those facts are explicitly provided.
Do not discount campaigns, collaborations, celebrity moments, or off-platform context when they visibly explain the metrics.
If content evidence does not explain the metric, say confidence is low.
When reference posts exist, watchout must say what to avoid or what the reference posts are missing.
Never mention internal cohort letters, database roles, or strings like trigger_core/reference_no_jump.
Use the feed's niche vocabulary.
Classify pattern_type as one of: account_aligned, feed_aligned, account_outlier, conflict_signal, unclear.
Every do_next must name observable content behaviors from the evidence, not generic advice.
Keep the card concise for a mobile UI:
- title: 5-10 words.
- what_happened: one sentence, max 35 words.
- why_it_may_have_happened: one sentence, max 35 words.
- common_pattern: 2-4 short phrases, max 4 words each.
- do_next: one concrete action sentence, max 24 words.
- watchout: one caution sentence, max 24 words.
- per_post_notes: max 5 short strings.
Return only JSON:
{
  "title": "",
  "what_happened": "",
  "why_it_may_have_happened": "",
  "common_pattern": [],
  "do_next": "",
  "watchout": "",
  "per_post_notes": [],
  "pattern_type": "account_aligned|feed_aligned|account_outlier|conflict_signal|unclear",
  "confidence": "high|medium|low",
  "focus_memory_candidate": {
    "candidate_patterns": [],
    "candidate_avoid": [],
    "candidate_collision": ""
  }
}"""

_SIGNAL_QUESTIONS = {
    "OWN_BREAKOUT_EARLY": "What makes the trigger posts different from the feeder's typical visual references, while keeping D3 uncertainty in mind?",
    "OWN_BREAKOUT": "What makes the trigger posts different from the feeder's typical visual references?",
    "OWN_SUSTAIN": "Why are these posts repeatedly holding strong D7 performance?",
    "OWN_SUSTAIN_LONG": "What makes these posts durable beyond the first week?",
    "OWN_FADE": "What is missing in recent weak posts compared with prior strong visual references?",
    "OWN_COMMENT_SPIKE": "What in the content likely made people reply more than usual?",
    "OWN_LIKE_HEAVY": "What made these posts easy to approve or like without much discussion?",
    "OWN_VIRAL_PASSIVE": "Why did these posts reach more people without matching likes/comments?",
    "OWN_LATE_JUMP": "What could explain delayed pickup compared with posts that did not jump?",
    "OWN_FOLLOWER_SPIKE": "What account activity in this window may explain audience growth?",
    "OWN_FOLLOWER_DROP": "What account activity in this window may explain audience loss?",
    "CROSS_MOMENTUM": "What common behavior is appearing across multiple feeders?",
    "CROSS_FORMAT_SHIFT": "Why is this media format currently more productive than other recent format references?",
    "CROSS_FOLLOWER_WAVE": "What shared activity may explain audience movement across the feed?",
    "CROSS_MICRO_BREAKOUT": "What common breakout behavior is repeating across feeders?",
    "CROSS_MICRO_COMMENT_SPIKE": "What common content behavior is driving comments across feeders?",
    "CROSS_MICRO_LIKE_HEAVY": "What common content behavior is driving likes across feeders?",
    "CROSS_MICRO_VIRAL_PASSIVE": "What common content behavior is creating passive reach across feeders?",
    "CROSS_MICRO_FADE": "What common weakness is appearing across feeders?",
    "ANCHOR_GAP_WIDENING": "What is the anchor doing better than feed reference posts, and what should the feed avoid?",
    "ANCHOR_GAP_CLOSING": "What are competitors doing better than anchor reference posts, and what should the anchor avoid?",
    "ANCHOR_CHALLENGER_SURGE": "Why is this specific competitor outperforming anchor reference posts, and what should the anchor avoid?",
    "ANCHOR_FOLLOWER_GAP": "What activity may explain the follower-growth gap, and what should the slower side avoid?",
}


def _cohort_policy(signal: dict[str, Any]) -> dict[str, Any]:
    signal_type = str(signal.get("signal_type") or "")
    base = {
        "cohort_a": "trigger posts: the posts that made the metric signal viable",
        "cohort_b": "reference posts: small visual examples only, not a statistical baseline",
        "rules": [
            "Use metric_snapshot for baseline and trigger numbers.",
            "Use cohort B only to explain visual/content contrast.",
            "If cohort A and B share the same content pattern, lower confidence instead of forcing a difference.",
        ],
    }
    overrides = {
        "OWN_BREAKOUT_EARLY": {
            "cohort_a": "early breakout posts",
            "cohort_b": "prior typical references from this feeder/media type",
        },
        "OWN_BREAKOUT": {
            "cohort_a": "breakout posts",
            "cohort_b": "prior typical references from this feeder/media type",
        },
        "OWN_FADE": {
            "cohort_a": "recent weak posts",
            "cohort_b": "prior strong references from the same feeder/media type",
        },
        "OWN_LATE_JUMP": {
            "cohort_a": "posts with delayed percentile pickup",
            "cohort_b": "recent references that did not show a meaningful checkpoint jump",
        },
        "CROSS_FORMAT_SHIFT": {
            "cohort_a": "hot posts in the rising media format",
            "cohort_b": "recent references from other media formats",
        },
        "ANCHOR_GAP_WIDENING": {
            "cohort_a": "anchor winning posts",
            "cohort_b": "feed reference posts from non-anchor accounts",
        },
        "ANCHOR_GAP_CLOSING": {
            "cohort_a": "non-anchor winning posts",
            "cohort_b": "anchor reference posts",
        },
        "ANCHOR_CHALLENGER_SURGE": {
            "cohort_a": "challenger winning posts",
            "cohort_b": "anchor reference posts",
        },
        "ANCHOR_FOLLOWER_GAP": {
            "cohort_a": "posts from the side with stronger follower movement",
            "cohort_b": "posts from the slower comparison side",
        },
    }
    return {**base, **overrides.get(signal_type, {})}


def _display_post_role(role: Any) -> str:
    normalized = str(role or "").strip().lower()
    labels = {
        "trigger_core": "trigger post",
        "trigger_support": "trigger post",
        "reference_typical": "typical reference",
        "reference_strong": "strong reference",
        "reference_no_jump": "steady reference",
        "reference_other_format": "other-format reference",
        "reference_anchor": "anchor reference",
        "reference_feed": "feed reference",
        "reference_context": "reference post",
    }
    return labels.get(normalized, "post reviewed")


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
    clean = [url for url in urls if str(url).strip()]
    if len(clean) <= 8:
        return clean
    selected: list[str] = []
    selected.extend(clean[:3])
    selected.extend(clean[-2:])
    middle = clean[3:-2]
    if middle:
        step = max(1, len(middle) // 3)
        selected.extend(middle[idx] for idx in range(0, len(middle), step)[:3])
    seen: set[str] = set()
    deduped: list[str] = []
    for url in selected:
        if url in seen:
            continue
        deduped.append(url)
        seen.add(url)
    return deduped[:8]


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
                   p.duration_bucket, p.depth_bucket,
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
        elif role == "thumbnail":
            thumbnail_url = url
        elif role.startswith("carousel_"):
            asset_carousel.append(url)
    if asset_carousel:
        carousel_urls = asset_carousel
    return {**post, "video_url": video_url, "thumbnail_url": thumbnail_url, "carousel_urls": carousel_urls}


def _fingerprint_media_parts(post: dict[str, Any], provider: str) -> tuple[list[dict[str, Any]], str, str]:
    media_type = str(post.get("media_type") or "image").lower()
    source_bits: list[str] = []
    parts: list[dict[str, Any]] = []
    confidence = "low"
    if media_type == "reel":
        video_data = _fetch_bytes(post.get("video_url"), timeout=60, max_bytes=_VIDEO_UPLOAD_MAX_BYTES)
        if video_data:
            video_bytes, mime_type = video_data
            if not mime_type.startswith("video/"):
                mime_type = "video/mp4"
            video_bytes = _trim_video(video_bytes, mime_type)
            part = _openrouter_video_part(video_bytes, mime_type) if provider == "openrouter" else _gemini_video_part(video_bytes, mime_type)
            if part:
                parts.append(part)
                confidence = "high"
                source_bits.append(f"video:{_sha(video_bytes)}")
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

    user_text = "\n".join([
        f"MEDIA: {post.get('media_type') or 'unknown'} · duration_bucket={post.get('duration_bucket') or ''} · depth_bucket={post.get('depth_bucket') or ''}",
        f"CAPTION: {caption[:2000] or '(no caption)'}",
    ])
    fingerprint = _call_model(_FINGERPRINT_SYSTEM, user_text, media_parts, max_tokens=900)
    if not fingerprint:
        return None
    media_confidence = str(fingerprint.get("media_confidence") or confidence).lower()
    if media_confidence not in {"high", "medium", "low"}:
        media_confidence = confidence
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
    ensure_post_focus_read(conn, post_key, fingerprint)
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
                   pf.fingerprint,
                   pfr.focus_read,
                   pfr.feeder_focus_version,
                   coalesce(ff.focus_version, 0) as current_feeder_focus_version,
                   ff.structured_patterns as feeder_structured_patterns
            from public.signal_posts sp
            join public.posts p on p.post_key = sp.post_key
            join public.feeders fd on fd.id = p.feeder_id
            left join public.post_fingerprints pf on pf.post_key = sp.post_key
            left join public.post_focus_reads pfr on pfr.post_key = sp.post_key
            left join public.feeder_focus ff on ff.feeder_id = fd.id
            where sp.signal_id = %s
            order by sp.cohort, sp.rank, sp.post_key
            """,
            (signal_id,),
        )
        posts = cur.fetchall()
    return {"signal": signal, "posts": posts}


def _sample_hash(
    posts: list[dict[str, Any]],
    focus_context: dict[str, Any] | None = None,
    signal: dict[str, Any] | None = None,
) -> str:
    bits = []
    for row in posts:
        fingerprint = row.get("fingerprint") if isinstance(row.get("fingerprint"), dict) else {}
        stored_focus_version = int(row.get("feeder_focus_version") or -1)
        current_focus_version = int(row.get("current_feeder_focus_version") or 0)
        focus_read = (
            row.get("focus_read")
            if current_focus_version > 0
            and stored_focus_version == current_focus_version
            and isinstance(row.get("focus_read"), dict)
            else {}
        )
        bits.append(
            ":".join([
                str(row.get("cohort") or ""),
                str(row.get("role") or ""),
                str(row.get("post_key") or ""),
                str(current_focus_version if focus_read else 0),
                _sha(fingerprint),
                _sha(focus_read),
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


def _stable_patterns(value: Any) -> list[dict[str, Any]]:
    data = value if isinstance(value, dict) else {}
    rows = data.get("patterns") if isinstance(data.get("patterns"), list) else []
    return [
        row for row in rows
        if isinstance(row, dict) and str(row.get("status") or "").lower() in {"stable", "core", "strengthening"}
    ]


def _focus_deviations(value: Any) -> list[str]:
    focus_read = value if isinstance(value, dict) else {}
    relation = focus_read.get("relation_to_feeder_md") if isinstance(focus_read.get("relation_to_feeder_md"), dict) else {}
    deviations = relation.get("deviates")
    if not isinstance(deviations, list):
        return []
    return [str(item).strip() for item in deviations if str(item or "").strip()]


def _alignment_deviation_reason(posts: list[dict[str, Any]]) -> str | None:
    for row in posts:
        deviations = _focus_deviations(row.get("focus_read"))
        if not deviations:
            continue
        stable = _stable_patterns(row.get("feeder_structured_patterns"))
        if not stable:
            continue
        return f"alignment_deviation_with_stable_focus:{row.get('post_key')}"
    return None


def _card_schema_errors(card: Any) -> list[str]:
    if not isinstance(card, dict):
        return ["card_not_object"]
    errors: list[str] = []
    for key in ("title", "what_happened", "why_it_may_have_happened", "do_next", "watchout", "confidence", "pattern_type"):
        if key not in card:
            errors.append(f"missing_{key}")
        elif not isinstance(card.get(key), str):
            errors.append(f"{key}_not_string")
    for key in ("common_pattern", "per_post_notes"):
        if key not in card:
            errors.append(f"missing_{key}")
        elif not isinstance(card.get(key), list):
            errors.append(f"{key}_not_list")
    if str(card.get("pattern_type") or "") not in {"account_aligned", "feed_aligned", "account_outlier", "conflict_signal", "unclear"}:
        errors.append("invalid_pattern_type")
    if str(card.get("confidence") or "").lower() not in {"high", "medium", "low"}:
        errors.append("invalid_confidence")
    if card.get("focus_memory_candidate") is not None and not isinstance(card.get("focus_memory_candidate"), dict):
        errors.append("focus_memory_candidate_not_object")
    return errors


def _deterministic_escalation_reasons(signal: dict[str, Any], posts: list[dict[str, Any]]) -> list[str]:
    reasons: list[str] = []
    tier1 = _tier1_metric_reason(signal)
    if tier1:
        reasons.append(tier1)
    alignment = _alignment_deviation_reason(posts)
    if alignment:
        reasons.append(alignment)
    return reasons


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
        for row in posts:
            fp = row.get("fingerprint") if isinstance(row.get("fingerprint"), dict) else {}
            stored_focus_version = int(row.get("feeder_focus_version") or -1)
            current_focus_version = int(row.get("current_feeder_focus_version") or 0)
            focus_read_stale = stored_focus_version != current_focus_version
            if fp and current_focus_version > 0 and (not isinstance(row.get("focus_read"), dict) or focus_read_stale):
                ensure_post_focus_read(conn, str(row.get("post_key") or ""), fp)
        payload = _signal_payload(conn, sid)
        if not payload:
            continue
        signal = payload["signal"]
        posts = payload["posts"]
        focus_context = feed_focus_context(conn, signal, posts)
        escalation_reasons = _deterministic_escalation_reasons(signal, posts)
        card_model_override = SIGNAL_INTELLIGENCE_ESCALATION_MODEL if escalation_reasons else None
        fingerprints = []
        for row in posts:
            fp = row.get("fingerprint") if isinstance(row.get("fingerprint"), dict) else {}
            stored_focus_version = int(row.get("feeder_focus_version") or -1)
            current_focus_version = int(row.get("current_feeder_focus_version") or 0)
            focus_read = (
                row.get("focus_read")
                if current_focus_version > 0
                and stored_focus_version == current_focus_version
                and isinstance(row.get("focus_read"), dict)
                else {}
            )
            fingerprints.append({
                "post_key": row.get("post_key"),
                "cohort": row.get("cohort"),
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
                "focus_read": focus_read,
                "feeder_focus_version": row.get("feeder_focus_version"),
                "current_feeder_focus_version": row.get("current_feeder_focus_version"),
            })
        sample_hash = _sample_hash(posts, focus_context, signal)
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

        user_text = json.dumps({
            "signal": signal.get("signal_type"),
            "question": _SIGNAL_QUESTIONS.get(str(signal.get("signal_type") or ""), "What explains this metric-triggered signal?"),
            "feed_bible": signal.get("context_bible") or "(not provided)",
            "feed_focus_context": focus_context,
            "media_type": signal.get("media_type"),
            "sub_bucket": signal.get("sub_bucket"),
            "metric_snapshot": signal.get("metric_snapshot") or {},
            "cohort_policy": _cohort_policy(signal),
            "model_route": {
                "escalated_to_pro": bool(card_model_override),
                "deterministic_reasons": escalation_reasons,
            },
            "cohort_posts": fingerprints,
        }, default=str)
        card = _call_model(_CARD_SYSTEM, user_text, [], max_tokens=2200, model_override=card_model_override)
        schema_errors = _card_schema_errors(card)
        if schema_errors and not card_model_override:
            escalation_reasons = [*escalation_reasons, f"schema_validation_failed:{','.join(schema_errors[:5])}"]
            card_model_override = SIGNAL_INTELLIGENCE_ESCALATION_MODEL
            card_model = current_model_version(kind="card", model_override=card_model_override)
            user_text = json.dumps({
                "signal": signal.get("signal_type"),
                "question": _SIGNAL_QUESTIONS.get(str(signal.get("signal_type") or ""), "What explains this metric-triggered signal?"),
                "feed_bible": signal.get("context_bible") or "(not provided)",
                "feed_focus_context": focus_context,
                "media_type": signal.get("media_type"),
                "sub_bucket": signal.get("sub_bucket"),
                "metric_snapshot": signal.get("metric_snapshot") or {},
                "cohort_policy": _cohort_policy(signal),
                "model_route": {
                    "escalated_to_pro": True,
                    "deterministic_reasons": escalation_reasons,
                },
                "cohort_posts": fingerprints,
            }, default=str)
            print(f"[signal-intelligence] escalating signal_id={sid} reasons={escalation_reasons}")
            card = _call_model(_CARD_SYSTEM, user_text, [], max_tokens=2600, model_override=card_model_override)
            schema_errors = _card_schema_errors(card)
        if not card or schema_errors:
            with conn.cursor() as cur:
                cur.execute("update public.signals set status = 'error', updated_at = now() where id = %s", (sid,))
            conn.commit()
            if schema_errors:
                print(f"[signal-intelligence] card schema failed signal_id={sid} errors={schema_errors}")
            failed += 1
            continue
        focus_memory_candidate = card.pop("focus_memory_candidate", {}) if isinstance(card.get("focus_memory_candidate"), dict) else {}
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
