begin;

-- Keep post_intelligence as a signal-ready table only. Rows with the old
-- hook/pillar/format/subject contract, skipped payloads, or partial current
-- payloads should be re-extracted instead of treated as valid signal history.
delete from public.post_intelligence pi
where coalesce(pi.tags, '{}'::jsonb) ?| array['hook', 'pillar', 'format', 'subject']
   or not coalesce(pi.tags, '{}'::jsonb) ?& array[
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
   ];

update public.post_intelligence pi
set tags = cleaned.tags,
    extracted_at = now()
from (
  select
    post_key,
    coalesce(jsonb_object_agg(key, value), '{}'::jsonb) as tags
  from public.post_intelligence
  cross join lateral jsonb_each(tags)
  where key = any(array[
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
  ])
  group by post_key
) cleaned
where cleaned.post_key = pi.post_key
  and pi.tags is distinct from cleaned.tags;

alter table if exists public.post_intelligence
  drop constraint if exists post_intelligence_current_tags_check;

alter table if exists public.post_intelligence
  add constraint post_intelligence_current_tags_check
  check (
    tags ?& array[
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
    and tags->>'mechanic' in (
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
    )
    and tags->>'opening_move' in (
      'RESULT_FIRST',
      'PERSON_FIRST',
      'TEXT_FIRST',
      'OBJECT_FIRST',
      'ACTION_FIRST',
      'SCENE_FIRST'
    )
    and tags->>'proof_mode' in (
      'LIVE_DEMO',
      'VISUAL_RESULT',
      'EXPERT_TALK',
      'SOCIAL_PROOF',
      'DATA_PROOF',
      'ACCESS_PROOF',
      'PROOF_NONE'
    )
    and tags->>'pacing' in ('PACING_SLOW', 'PACING_MEDIUM', 'PACING_FAST')
    and (not (tags ? 'audio_mode') or tags->>'audio_mode' in (
      'AUDIO_DIRECT_SPEECH',
      'AUDIO_VOICEOVER',
      'AUDIO_SOURCE_LIVE',
      'AUDIO_MUSIC_LED',
      'AUDIO_ASMR',
      'AUDIO_MINIMAL'
    ))
    and tags->>'style' in (
      'STYLE_UGC',
      'STYLE_STUDIO',
      'STYLE_TEXT_DRIVEN',
      'STYLE_MONTAGE',
      'STYLE_CINEMATIC',
      'STYLE_SCREEN_RECORD'
    )
    and (not (tags ? 'cta') or tags->>'cta' in (
      'CTA_ENGAGEMENT',
      'CTA_TRAFFIC',
      'CTA_PURCHASE',
      'CTA_COMMUNITY'
    ))
    and tags->>'face' in ('FACE_SINGLE', 'FACE_NONE', 'FACE_MULTIPLE')
    and tags->>'depth' in ('DEPTH_SINGLE', 'DEPTH_MINI', 'DEPTH_STANDARD', 'DEPTH_DEEP')
    and tags->>'density' in ('DENSITY_MINIMAL', 'DENSITY_MEDIUM', 'DENSITY_BUSY')
    and tags->>'text_overlay' in ('TEXT_NONE', 'TEXT_LIGHT', 'TEXT_HEAVY')
    and (not (tags ? 'duration_bucket') or tags->>'duration_bucket' in (
      'DUR_SHORT',
      'DUR_MEDIUM',
      'DUR_LONG',
      'DUR_EXTENDED'
    ))
  );

commit;
