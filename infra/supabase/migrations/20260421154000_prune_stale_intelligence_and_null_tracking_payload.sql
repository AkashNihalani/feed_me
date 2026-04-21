begin;

-- Tracking Fire rows do not use signal_payload. Keep the column for Firewatch,
-- but stop defaulting normal tracking inserts to an empty JSON object.
alter table if exists public.fire_alerts
  alter column signal_payload drop default;

alter table if exists public.fire_alerts
  alter column signal_payload drop not null;

update public.fire_alerts
set signal_payload = null
where signal_code = 'slot_v3'
  and signal_payload = '{}'::jsonb;

-- Drop lingering pattern rows from the older modifier-based contract so only
-- the current mechanic + cues runtime survives.
delete from public.fire_alerts fa
where fa.signal_code in ('OWN_PATTERN', 'CROSS_PATTERN', 'ANCHOR_PATTERN')
  and (
    coalesce(fa.signal_payload->>'media_type', '') = ''
    or coalesce(fa.signal_payload->>'pattern_key', '') = ''
    or coalesce(fa.signal_payload->>'modifier_key', '') <> ''
    or case
      when jsonb_typeof(fa.signal_payload->'required_cues') = 'array'
        then jsonb_array_length(fa.signal_payload->'required_cues') < 2
      else true
    end
  );

-- Keep post_intelligence as a single, current-contract table only. Older
-- model versions can still be useful for pattern detection if they already use
-- the current tag keys and were extracted from a valid full-media source. The
-- real problem rows are the fallback-source ones.
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
      'text_overlay',
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
    and (
      tags->>'_visual_source' = 'image_full'
      or tags->>'_visual_source' ~ '^carousel:[0-9]+slides$'
      or tags->>'_visual_source' ~ '^video_full:[0-9]+([.][0-9]+)?mb:(data_url|inline|file_api)$'
    )
  );

create or replace function public.fn_process_checkpoint(
  p_feeder_id bigint,
  p_checkpoint text,
  p_business_date date
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_cp text := lower(coalesce(p_checkpoint, 'd1'));
  v_day date := coalesce(p_business_date, (now() at time zone 'Asia/Kolkata')::date);
  v_feed_id bigint;
  v_handle text;
  v_rows int := 0;
  r record;
  v_best_metric text;
  v_best_value bigint;
  v_best_percentile int;
  v_baseline bigint;
  v_multiple numeric;
  v_body text;
  v_alert_type text;
  v_dedupe text;
begin
  if v_cp not in ('d1', 'd3', 'd7', 'd21') then
    return 0;
  end if;

  select fd.feed_id, fd.handle
  into v_feed_id, v_handle
  from public.feeders fd
  where fd.id = p_feeder_id;

  if v_feed_id is null then
    return 0;
  end if;

  update public.fire_alerts fa
  set status = 'dropped',
      updated_at = now()
  where fa.feeder_id = p_feeder_id
    and fa.checkpoint = v_cp
    and fa.business_date_ist = v_day
    and fa.signal_code = 'slot_v3'
    and fa.context = 'own'
    and not exists (
      select 1
      from public.post_metrics pm
      join public.posts p on p.post_key = pm.post_key
      where p.feeder_id = p_feeder_id
        and pm.post_key = fa.post_key
        and pm.checkpoint = v_cp
        and pm.business_date_ist = v_day
        and public.fn_is_hot_percentile(pm.percentile_performance)
    );

  for r in
    select
      p.post_key,
      lower(coalesce(p.media_type, 'unknown')) as media_type,
      pm.views,
      pm.likes,
      pm.comments,
      pm.views_percentile,
      pm.likes_percentile,
      pm.comments_percentile,
      pm.percentile_performance,
      pm.delta_from_d1,
      fb.median_views,
      fb.median_likes,
      fb.median_comments
    from public.posts p
    join public.post_metrics pm
      on pm.post_key = p.post_key
     and pm.checkpoint = v_cp
     and pm.business_date_ist = v_day
    left join public.feeder_baselines fb
      on fb.feeder_id = p.feeder_id
     and fb.media_type = lower(coalesce(p.media_type, 'unknown'))
     and fb.checkpoint = pm.checkpoint
    where p.feeder_id = p_feeder_id
      and public.fn_is_hot_percentile(pm.percentile_performance)
    order by pm.percentile_performance asc, p.posted_at desc nulls last
  loop
    v_best_metric := 'views';
    v_best_value := r.views;
    v_best_percentile := coalesce(r.views_percentile, 101);
    v_baseline := r.median_views;

    if coalesce(r.likes_percentile, 101) < v_best_percentile then
      v_best_metric := 'likes';
      v_best_value := r.likes;
      v_best_percentile := coalesce(r.likes_percentile, 101);
      v_baseline := r.median_likes;
    end if;

    if coalesce(r.comments_percentile, 101) < v_best_percentile then
      v_best_metric := 'comments';
      v_best_value := r.comments;
      v_best_percentile := coalesce(r.comments_percentile, 101);
      v_baseline := r.median_comments;
    end if;

    v_multiple := case
      when v_best_value is not null and coalesce(v_baseline, 0) > 0
        then round(v_best_value::numeric / v_baseline::numeric, 2)
      else null
    end;

    v_alert_type := case
      when r.percentile_performance <= 10 then 'blaze'
      when r.percentile_performance <= 25 then 'burn'
      else 'spark'
    end;

    v_body := case
      when v_cp = 'd1' and v_multiple is not null then
        format(
          '@%s hit the top %s%% at %s with %s %s, %.2fx its usual %s.',
          upper(coalesce(v_handle, 'feed')),
          coalesce(r.percentile_performance, 0),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(v_best_metric),
          v_multiple,
          upper(v_best_metric)
        )
      when v_cp = 'd1' then
        format(
          '@%s hit the top %s%% at %s with %s %s.',
          upper(coalesce(v_handle, 'feed')),
          coalesce(r.percentile_performance, 0),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(v_best_metric)
        )
      when r.delta_from_d1 is not null then
        format(
          '@%s stayed in the top %s%% at %s with %s %s, %s points vs D1.',
          upper(coalesce(v_handle, 'feed')),
          coalesce(r.percentile_performance, 0),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(v_best_metric),
          case when r.delta_from_d1 > 0 then '+' || r.delta_from_d1::text else r.delta_from_d1::text end
        )
      else
        format(
          '@%s stayed in the top %s%% at %s with %s %s.',
          upper(coalesce(v_handle, 'feed')),
          coalesce(r.percentile_performance, 0),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(v_best_metric)
        )
    end;

    v_dedupe := format('slot_v3:%s:%s:%s:%s', v_feed_id, r.post_key, v_cp, v_day);

    insert into public.fire_alerts (
      dedupe_key,
      feed_id,
      feeder_id,
      post_key,
      checkpoint,
      business_date_ist,
      signal_code,
      context,
      alert_type,
      status,
      metric_key,
      metric_value,
      surface_percentile,
      surface_delta,
      body,
      signal_payload,
      created_at,
      updated_at
    )
    values (
      v_dedupe,
      v_feed_id,
      p_feeder_id,
      r.post_key,
      v_cp,
      v_day,
      'slot_v3',
      'own',
      v_alert_type,
      'new',
      v_best_metric,
      v_best_value,
      r.percentile_performance,
      r.delta_from_d1,
      v_body,
      null,
      now(),
      now()
    )
    on conflict (dedupe_key) do update
      set alert_type = excluded.alert_type,
          status = 'new',
          metric_key = excluded.metric_key,
          metric_value = excluded.metric_value,
          surface_percentile = excluded.surface_percentile,
          surface_delta = excluded.surface_delta,
          body = excluded.body,
          signal_payload = null,
          updated_at = now();

    v_rows := v_rows + 1;
  end loop;

  return v_rows;
end;
$$;

commit;
