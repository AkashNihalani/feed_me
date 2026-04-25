begin;

-- Full-video assets are required by post_intelligence extraction. They must be
-- claimable by the normal media worker so failed/stale captures recover without
-- another Bright Data scrape.
create or replace function public.claim_post_media_assets_for_capture(p_limit integer default 25)
returns setof public.post_media_assets
language sql
security definer
set search_path = public
as $function$
  with picked as (
    select id
    from public.post_media_assets
    where (
        status in ('pending_capture', 'capture_failed')
        or (
          status = 'capturing'
          and updated_at <= now() - interval '5 minutes'
        )
      )
      and (
        lower(coalesce(asset_role, '')) in ('thumbnail', 'display', 'preview_5s', 'video_full')
        or lower(coalesce(asset_role, '')) like 'carousel_%'
      )
      and next_run_at <= now()
      and coalesce(source_url, '') <> ''
      and (purge_after is null or purge_after > now())
    order by
      case
        when lower(coalesce(asset_role, '')) = 'thumbnail' then 0
        when lower(coalesce(asset_role, '')) = 'display' then 1
        when lower(coalesce(asset_role, '')) = 'preview_5s' then 2
        when lower(coalesce(asset_role, '')) like 'carousel_%' then 3
        when lower(coalesce(asset_role, '')) = 'video_full' then 4
        else 9
      end,
      next_run_at asc,
      created_at asc,
      id asc
    for update skip locked
    limit greatest(1, p_limit)
  ), updated as (
    update public.post_media_assets assets
    set status = 'capturing',
        updated_at = now()
    from picked
    where assets.id = picked.id
    returning assets.*
  )
  select * from updated;
$function$;

delete from public.post_intelligence pi
using public.posts p
where p.post_key = pi.post_key
  and (
    coalesce(pi.tags->>'_visual_source', '') in ('', 'none', 'thumbnail')
    or (
      lower(coalesce(p.media_type, 'image')) = 'reel'
      and coalesce(pi.tags->>'_visual_source', '') not like 'video_full:%'
    )
    or (
      lower(coalesce(p.media_type, 'image')) in ('sidecar', 'carousel')
      and coalesce(pi.tags->>'_visual_source', '') !~ '^carousel:[0-9]+slides$'
    )
    or (
      lower(coalesce(p.media_type, 'image')) not in ('reel', 'sidecar', 'carousel')
      and coalesce(pi.tags->>'_visual_source', '') <> 'image_full'
    )
  );

delete from public.post_intelligence
where not coalesce((
    jsonb_typeof(tags) = 'object'
    and tags ?& array[
      'mechanic',
      'opening_move',
      'proof_mode',
      'pacing',
      'audio_mode',
      'style',
      'face',
      'language',
      'depth',
      'density',
      'text_overlay',
      'duration_bucket',
      '_visual_source'
    ]
    and not (tags ?| array['hook', 'pillar', 'format', 'subject'])
    and (tags - array[
      'mechanic',
      'opening_move',
      'proof_mode',
      'pacing',
      'audio_mode',
      'style',
      'cta',
      'face',
      'language',
      'depth',
      'density',
      'text_overlay',
      'duration_bucket',
      '_visual_source'
    ]) = '{}'::jsonb
    and coalesce((tags->>'mechanic') in (
      'REVEAL',
      'PROCESS',
      'REACTION',
      'SHOWCASE',
      'STORY',
      'COMPARE',
      'LIST',
      'CHALLENGE',
      'CONVERSE',
      'ACCESS',
      'ANNOUNCE',
      'COLLAB',
      'SOCIAL_PROOF',
      'AESTHETIC',
      'EDUCATE'
    ), false)
    and coalesce((tags->>'opening_move') in (
      'RESULT_FIRST',
      'PERSON_FIRST',
      'TEXT_FIRST',
      'OBJECT_FIRST',
      'ACTION_FIRST',
      'SCENE_FIRST'
    ), false)
    and coalesce((tags->>'proof_mode') in (
      'LIVE_DEMO',
      'VISUAL_RESULT',
      'EXPERT_TALK',
      'SOCIAL_PROOF',
      'DATA_PROOF',
      'ACCESS_PROOF',
      'PROOF_NONE'
    ), false)
    and (
      tags->'pacing' = 'null'::jsonb
      or coalesce((tags->>'pacing') in ('PACING_SLOW', 'PACING_MEDIUM', 'PACING_FAST'), false)
    )
    and (
      tags->'audio_mode' = 'null'::jsonb
      or coalesce((tags->>'audio_mode') in (
        'AUDIO_DIRECT_SPEECH',
        'AUDIO_VOICEOVER',
        'AUDIO_SOURCE_LIVE',
        'AUDIO_MUSIC_LED',
        'AUDIO_ASMR',
        'AUDIO_MINIMAL'
      ), false)
    )
    and coalesce((tags->>'style') in (
      'STYLE_UGC',
      'STYLE_STUDIO',
      'STYLE_TEXT_DRIVEN',
      'STYLE_MONTAGE',
      'STYLE_CINEMATIC',
      'STYLE_SCREEN_RECORD'
    ), false)
    and (
      not (tags ? 'cta')
      or tags->'cta' = 'null'::jsonb
      or coalesce((tags->>'cta') in (
        'CTA_ENGAGEMENT',
        'CTA_TRAFFIC',
        'CTA_PURCHASE',
        'CTA_COMMUNITY'
      ), false)
    )
    and coalesce((tags->>'face') in ('FACE_SINGLE', 'FACE_NONE', 'FACE_MULTIPLE'), false)
    and coalesce(length(tags->>'language') between 2 and 20, false)
    and (
      tags->'depth' = 'null'::jsonb
      or coalesce((tags->>'depth') in ('DEPTH_SINGLE', 'DEPTH_MINI', 'DEPTH_STANDARD', 'DEPTH_DEEP'), false)
    )
    and (
      tags->'density' = 'null'::jsonb
      or coalesce((tags->>'density') in ('DENSITY_MINIMAL', 'DENSITY_MEDIUM', 'DENSITY_BUSY'), false)
    )
    and coalesce((tags->>'text_overlay') in ('TEXT_NONE', 'TEXT_LIGHT', 'TEXT_HEAVY'), false)
    and (
      tags->'duration_bucket' = 'null'::jsonb
      or coalesce((tags->>'duration_bucket') in (
        'DUR_SHORT',
        'DUR_MEDIUM',
        'DUR_LONG',
        'DUR_EXTENDED'
      ), false)
    )
    and coalesce((
      tags->>'_visual_source' = 'image_full'
      or tags->>'_visual_source' ~ '^carousel:[0-9]+slides$'
      or tags->>'_visual_source' ~ '^video_full:[0-9]+([.][0-9]+)?mb:(data_url|inline|file_api)$'
    ), false)
  ), false);

alter table if exists public.post_intelligence
  drop constraint if exists post_intelligence_current_tags_check;

alter table if exists public.post_intelligence
  add constraint post_intelligence_current_tags_check
  check (
    jsonb_typeof(tags) = 'object'
    and tags ?& array[
      'mechanic',
      'opening_move',
      'proof_mode',
      'pacing',
      'audio_mode',
      'style',
      'face',
      'language',
      'depth',
      'density',
      'text_overlay',
      'duration_bucket',
      '_visual_source'
    ]
    and not (tags ?| array['hook', 'pillar', 'format', 'subject'])
    and (tags - array[
      'mechanic',
      'opening_move',
      'proof_mode',
      'pacing',
      'audio_mode',
      'style',
      'cta',
      'face',
      'language',
      'depth',
      'density',
      'text_overlay',
      'duration_bucket',
      '_visual_source'
    ]) = '{}'::jsonb
    and coalesce((tags->>'mechanic') in (
      'REVEAL',
      'PROCESS',
      'REACTION',
      'SHOWCASE',
      'STORY',
      'COMPARE',
      'LIST',
      'CHALLENGE',
      'CONVERSE',
      'ACCESS',
      'ANNOUNCE',
      'COLLAB',
      'SOCIAL_PROOF',
      'AESTHETIC',
      'EDUCATE'
    ), false)
    and coalesce((tags->>'opening_move') in (
      'RESULT_FIRST',
      'PERSON_FIRST',
      'TEXT_FIRST',
      'OBJECT_FIRST',
      'ACTION_FIRST',
      'SCENE_FIRST'
    ), false)
    and coalesce((tags->>'proof_mode') in (
      'LIVE_DEMO',
      'VISUAL_RESULT',
      'EXPERT_TALK',
      'SOCIAL_PROOF',
      'DATA_PROOF',
      'ACCESS_PROOF',
      'PROOF_NONE'
    ), false)
    and (
      tags->'pacing' = 'null'::jsonb
      or coalesce((tags->>'pacing') in ('PACING_SLOW', 'PACING_MEDIUM', 'PACING_FAST'), false)
    )
    and (
      tags->'audio_mode' = 'null'::jsonb
      or coalesce((tags->>'audio_mode') in (
        'AUDIO_DIRECT_SPEECH',
        'AUDIO_VOICEOVER',
        'AUDIO_SOURCE_LIVE',
        'AUDIO_MUSIC_LED',
        'AUDIO_ASMR',
        'AUDIO_MINIMAL'
      ), false)
    )
    and coalesce((tags->>'style') in (
      'STYLE_UGC',
      'STYLE_STUDIO',
      'STYLE_TEXT_DRIVEN',
      'STYLE_MONTAGE',
      'STYLE_CINEMATIC',
      'STYLE_SCREEN_RECORD'
    ), false)
    and (
      not (tags ? 'cta')
      or tags->'cta' = 'null'::jsonb
      or coalesce((tags->>'cta') in (
        'CTA_ENGAGEMENT',
        'CTA_TRAFFIC',
        'CTA_PURCHASE',
        'CTA_COMMUNITY'
      ), false)
    )
    and coalesce((tags->>'face') in ('FACE_SINGLE', 'FACE_NONE', 'FACE_MULTIPLE'), false)
    and coalesce(length(tags->>'language') between 2 and 20, false)
    and (
      tags->'depth' = 'null'::jsonb
      or coalesce((tags->>'depth') in ('DEPTH_SINGLE', 'DEPTH_MINI', 'DEPTH_STANDARD', 'DEPTH_DEEP'), false)
    )
    and (
      tags->'density' = 'null'::jsonb
      or coalesce((tags->>'density') in ('DENSITY_MINIMAL', 'DENSITY_MEDIUM', 'DENSITY_BUSY'), false)
    )
    and coalesce((tags->>'text_overlay') in ('TEXT_NONE', 'TEXT_LIGHT', 'TEXT_HEAVY'), false)
    and (
      tags->'duration_bucket' = 'null'::jsonb
      or coalesce((tags->>'duration_bucket') in (
        'DUR_SHORT',
        'DUR_MEDIUM',
        'DUR_LONG',
        'DUR_EXTENDED'
      ), false)
    )
    and coalesce((
      tags->>'_visual_source' = 'image_full'
      or tags->>'_visual_source' ~ '^carousel:[0-9]+slides$'
      or tags->>'_visual_source' ~ '^video_full:[0-9]+([.][0-9]+)?mb:(data_url|inline|file_api)$'
    ), false)
  );

commit;
