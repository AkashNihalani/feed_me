from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from psycopg.rows import dict_row

from .config import APP_TIMEZONE
from .post_intelligence import (
    current_model_version as pi_model_version,
    extract_tags as pi_extract_tags,
    is_enabled as pi_enabled,
)


def extract_post_intelligence_for_checkpoint(
    conn: Any,
    feeder_id: int,
    checkpoint: str,
    business_date: date | None,
    *,
    app_timezone: str | None = None,
) -> None:
    """
    Extract structural tags only for hot D7 posts using the latest media URLs
    refreshed during the checkpoint scrape.
    Processes posts from this feeder/business day that do not already have
    intelligence tags so the pattern engine can build history over time.
    Avoids long-lived Supabase-cached videos/carousels.
    """
    if not pi_enabled():
        return

    cp = (checkpoint or "").lower()
    if cp != "d7":
        return

    if business_date is None:
        try:
            tz = ZoneInfo(app_timezone or APP_TIMEZONE or "Asia/Kolkata")
        except Exception:
            tz = timezone.utc
        business_date = datetime.now(tz).date()

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """SELECT p.post_key,
                      p.caption,
                      lower(coalesce(p.media_type, 'image')) as media_type,
                      p.posted_at,
                      p.thumbnail_url,
                      p.video_url,
                      p.carousel_urls,
                      pi.tags as existing_tags
               FROM public.posts p
               JOIN public.post_metrics pm ON pm.post_key = p.post_key
               LEFT JOIN public.post_intelligence pi ON pi.post_key = p.post_key
               WHERE p.feeder_id = %s
                 AND lower(pm.checkpoint) = %s
                 AND pm.business_date_ist = %s
                 AND pm.percentile_performance IS NOT NULL
                 AND pm.percentile_performance <= 35
                 AND (
                   pi.post_key IS NULL
                   OR NOT coalesce(pi.tags, '{}'::jsonb) ?& array[
                     'mechanic',
                     'opening_move',
                     'proof_mode',
                     'pacing',
                     'style',
                     'face',
                     'language',
                     'depth',
                     'density',
                     'text_overlay'
                   ]
                   OR coalesce(pi.tags, '{}'::jsonb) ?| array['hook', 'pillar', 'format', 'subject']
                 )
               ORDER BY p.posted_at DESC NULLS LAST, p.post_key DESC
               LIMIT 75""",
            (feeder_id, cp, business_date),
        )
        posts = cur.fetchall()

    for post in posts:
        post_key = str(post["post_key"])
        caption = post.get("caption") or ""
        media_type = str(post.get("media_type") or "image")

        try:
            video_url = None
            if media_type == "reel":
                video_url = str(post.get("video_url") or "").strip() or None
            carousel_urls = post.get("carousel_urls") if media_type in ("sidecar", "carousel") else []
            if not isinstance(carousel_urls, list):
                carousel_urls = []
            thumbnail_url = str(post.get("thumbnail_url") or "").strip() or None

            tags = pi_extract_tags(
                caption,
                media_type,
                thumbnail_url=thumbnail_url,
                video_url=video_url,
                carousel_urls=carousel_urls if carousel_urls else None,
                media_fetch_headers=None,
            )
            if tags and not tags.get("_skipped"):
                model = pi_model_version(skipped=False)
                conn.execute(
                    """INSERT INTO public.post_intelligence (post_key, tags, model_version, extracted_at)
                       VALUES (%s, %s::jsonb, %s, now())
                       ON CONFLICT (post_key) DO UPDATE
                       SET tags = excluded.tags,
                           model_version = excluded.model_version,
                           extracted_at = excluded.extracted_at""",
                    (post_key, json.dumps(tags), model),
                )
                conn.commit()
                print(
                    f"[post-intelligence] {cp.upper()} extracted tags for {post_key} "
                    f"({tags.get('_visual_source', 'unknown')})"
                )
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[post-intelligence] {cp.upper()} extraction failed for {post_key}: {exc}")
