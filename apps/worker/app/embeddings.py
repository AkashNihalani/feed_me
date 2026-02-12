from __future__ import annotations

import requests

from .config import (
    EMBEDDING_API_KEY,
    EMBEDDING_BASE_URL,
    OPENROUTER_SITE_URL,
    OPENROUTER_APP_NAME,
    OPENAI_EMBED_MODEL,
)


def build_embedding_text(
    handle: str,
    media_type: str | None,
    caption: str | None,
    velocity_tag: str | None,
    velocity_stage: str | None,
    velocity_percentile: str | None,
) -> str:
    return (
        f"handle: {handle or ''}\n"
        f"media_type: {media_type or ''}\n"
        f"velocity_tag: {velocity_tag or ''}\n"
        f"velocity_stage: {velocity_stage or ''}\n"
        f"velocity_percentile: {velocity_percentile or ''}\n"
        f"caption: {caption or ''}"
    ).strip()


def build_signal_texts(row: dict) -> dict[str, str]:
    handle = row.get("handle") or ""
    media_type = row.get("media_type") or ""
    caption = row.get("caption") or ""
    velocity_tag = row.get("velocity_tag") or ""
    velocity_stage = row.get("velocity_stage") or ""
    velocity_percentile = row.get("velocity_percentile") or ""
    views = row.get("views") or 0
    likes = row.get("likes") or 0
    comments = row.get("comments") or 0

    caption_text = build_embedding_text(
        handle=handle,
        media_type=media_type,
        caption=caption,
        velocity_tag=velocity_tag,
        velocity_stage=velocity_stage,
        velocity_percentile=velocity_percentile,
    )

    performance_text = (
        f"handle: {handle}\n"
        f"media_type: {media_type}\n"
        f"views: {views}\n"
        f"likes: {likes}\n"
        f"comments: {comments}\n"
        f"velocity_tag: {velocity_tag}\n"
        f"velocity_stage: {velocity_stage}\n"
        f"velocity_percentile: {velocity_percentile}"
    ).strip()

    return {
        "caption_semantic": caption_text,
        "performance_semantic": performance_text,
    }


def get_embedding(text: str) -> list[float]:
    if not EMBEDDING_API_KEY:
        raise RuntimeError("EMBEDDING_API_KEY/OPENROUTER_API_KEY/OPENAI_API_KEY is missing")
    if not text.strip():
        raise RuntimeError("embedding text is empty")

    url = f"{EMBEDDING_BASE_URL.rstrip('/')}/embeddings"
    headers = {
        "Authorization": f"Bearer {EMBEDDING_API_KEY}",
        "Content-Type": "application/json",
    }
    if "openrouter.ai" in EMBEDDING_BASE_URL:
        if OPENROUTER_SITE_URL:
            headers["HTTP-Referer"] = OPENROUTER_SITE_URL
        if OPENROUTER_APP_NAME:
            headers["X-Title"] = OPENROUTER_APP_NAME

    resp = requests.post(
        url,
        headers=headers,
        json={
            "model": OPENAI_EMBED_MODEL,
            "input": text,
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    items = data.get("data") or []
    if not items:
        raise RuntimeError("empty embedding response")
    emb = items[0].get("embedding") or []
    if not emb:
        raise RuntimeError("missing embedding vector")
    return emb
