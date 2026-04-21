from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from psycopg.rows import dict_row

from .config import APP_TIMEZONE, MEDIA_PUBLIC_BASE_URL
from .post_intelligence import (
    current_model_version as pi_model_version,
    extract_tags as pi_extract_tags,
    is_enabled as pi_enabled,
)
from .telegram import alert_post_intelligence_source_issue


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
    Enforces strict media source rules:
      - reels require the original full video url
      - sidecars require the full carousel slide set
      - images require the original image source
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
    current_intelligence_model = pi_model_version(skipped=False)

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """SELECT p.post_key,
                      p.post_url,
                      p.caption,
                      lower(coalesce(p.media_type, 'image')) as media_type,
                      p.posted_at,
                      coalesce(thumbnail_asset.public_url, p.thumbnail_url) as thumbnail_url,
                      nullif(btrim(p.video_url), '') as video_url,
                      p.carousel_urls,
                      f.handle,
                      pi.tags as existing_tags
               FROM public.posts p
               JOIN public.feeders f ON f.id = p.feeder_id
               JOIN public.post_metrics pm ON pm.post_key = p.post_key
               LEFT JOIN public.post_intelligence pi ON pi.post_key = p.post_key
               LEFT JOIN LATERAL (
                 SELECT coalesce(
                          assets.public_url,
                          case
                            when coalesce(%s, '') <> ''
                              then concat(
                                %s,
                                '/',
                                regexp_replace(coalesce(assets.storage_path, ''), '^/+', '')
                              )
                            else null
                          end
                        ) as public_url
                 FROM public.post_media_assets assets
                 WHERE assets.post_key = p.post_key
                   AND assets.asset_role = 'thumbnail'
                   AND assets.status in ('active', 'purge_pending')
                   AND coalesce(assets.storage_provider, 'supabase') = 'r2'
                   AND coalesce(assets.storage_path, '') <> ''
                 ORDER BY assets.updated_at desc nulls last, assets.id desc
                 LIMIT 1
               ) thumbnail_asset ON true
               WHERE p.feeder_id = %s
                 AND lower(pm.checkpoint) = %s
                 AND pm.business_date_ist = %s
                 AND pm.percentile_performance IS NOT NULL
                 AND pm.percentile_performance <= 35
                 AND (
                   pi.post_key IS NULL
                   OR coalesce(pi.model_version, '') <> %s
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
                   OR (
                     lower(coalesce(p.media_type, 'image')) = 'reel'
                     AND coalesce(pi.tags->>'_visual_source', '') NOT LIKE 'video_full:%'
                   )
                   OR (
                     lower(coalesce(p.media_type, 'image')) in ('sidecar', 'carousel')
                     AND coalesce(pi.tags->>'_visual_source', '') NOT LIKE 'carousel:%'
                   )
                   OR (
                     lower(coalesce(p.media_type, 'image')) NOT IN ('reel', 'sidecar', 'carousel')
                     AND coalesce(pi.tags->>'_visual_source', '') <> 'image_full'
                   )
                 )
               ORDER BY p.posted_at DESC NULLS LAST, p.post_key DESC
               LIMIT 75""",
            (
                (MEDIA_PUBLIC_BASE_URL or "").rstrip("/"),
                (MEDIA_PUBLIC_BASE_URL or "").rstrip("/"),
                feeder_id,
                cp,
                business_date,
                current_intelligence_model,
            ),
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
            if tags and tags.get("_skipped"):
                existing_tags = post.get("existing_tags") if isinstance(post.get("existing_tags"), dict) else {}
                existing_source = str(existing_tags.get("_visual_source") or "").strip()
                expected_source = str(tags.get("_expected_source") or "").strip() or None
                actual_source = str(tags.get("_visual_source") or "").strip() or None
                detail = str(tags.get("_skip_detail") or "").strip() or None
                reason = str(tags.get("_skip_reason") or "unknown").strip()
                alert_post_intelligence_source_issue(
                    handle=str(post.get("handle") or "").strip() or None,
                    post_key=post_key,
                    media_type=media_type,
                    reason=reason,
                    expected_source=expected_source,
                    actual_source=actual_source,
                    detail=detail,
                    post_url=str(post.get("post_url") or "").strip() or None,
                )
                if existing_source and (
                    existing_source == "none"
                    or existing_source == "thumbnail"
                    or (media_type == "reel" and not existing_source.startswith("video_full:"))
                    or (media_type in ("sidecar", "carousel") and not existing_source.startswith("carousel:"))
                    or (media_type not in ("reel", "sidecar", "carousel") and existing_source != "image_full")
                ):
                    conn.execute(
                        "delete from public.post_intelligence where post_key = %s",
                        (post_key,),
                    )
                    conn.commit()
                continue

            if tags and not tags.get("_skipped"):
                conn.execute(
                    """INSERT INTO public.post_intelligence (post_key, tags, model_version, extracted_at)
                       VALUES (%s, %s::jsonb, %s, now())
                       ON CONFLICT (post_key) DO UPDATE
                       SET tags = excluded.tags,
                           model_version = excluded.model_version,
                           extracted_at = excluded.extracted_at""",
                    (post_key, json.dumps(tags), current_intelligence_model),
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
