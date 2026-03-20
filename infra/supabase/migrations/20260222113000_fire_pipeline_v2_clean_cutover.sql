begin;

-- Fire pipeline v2 clean cutover:
-- 1) canonical processor
-- 2) legacy entrypoints rerouted to canonical processor
-- 3) minimal alert contract trigger (no recompute side effects)

create or replace function public.fn_fire_upsert_row(
  p_dedupe_key text,
  p_feed_id bigint,
  p_feeder_id bigint,
  p_post_key text,
  p_checkpoint text,
  p_business_date_ist date,
  p_signal_code text,
  p_context text,
  p_family_group text,
  p_alert_type text,
  p_title text,
  p_body text,
  p_payload jsonb,
  p_surface_percentile integer,
  p_surface_delta integer,
  p_surface_handle text,
  p_surface_media_type text,
  p_metric_key text,
  p_metric_value bigint,
  p_surface_stamp text
) returns bigint
language plpgsql
security definer
set search_path = public
as $$
declare
  v_id bigint;
begin
  update public.fire_alerts fa
     set feed_id = p_feed_id,
         feeder_id = p_feeder_id,
         post_key = p_post_key,
         checkpoint = lower(coalesce(p_checkpoint,'d1')),
         business_date_ist = p_business_date_ist,
         signal_code = p_signal_code,
         context = p_context,
         family_group = p_family_group,
         alert_type = p_alert_type,
         title = p_title,
         headline = p_title,
         body = p_body,
         payload = coalesce(p_payload, '{}'::jsonb),
         intelligence_layers = coalesce(p_payload, '{}'::jsonb),
         priority_score = greatest(coalesce(fa.priority_score, 0), 1),
         intensity_score = greatest(coalesce(fa.intensity_score, 0), 1),
         status = 'new',
         surface_percentile = p_surface_percentile,
         surface_delta = p_surface_delta,
         surface_shift = p_surface_delta,
         surface_handle = p_surface_handle,
         surface_feeder = p_surface_handle,
         surface_media_type = p_surface_media_type,
         surface_checkpoint = upper(coalesce(p_checkpoint,'d1')),
         metric_key = p_metric_key,
         metric_value = p_metric_value,
         surface_metric_label = p_metric_key,
         surface_metric_value = p_metric_value,
         surface_stamp = p_surface_stamp,
         updated_at = now()
   where fa.dedupe_key = p_dedupe_key
   returning fa.id into v_id;

  if v_id is null then
    insert into public.fire_alerts (
      feed_id, feeder_id, post_key, checkpoint, business_date_ist,
      signal_code, context, family_group, alert_type,
      title, headline, body, payload, intelligence_layers,
      priority_score, intensity_score, status, dedupe_key,
      surface_percentile, surface_delta, surface_shift,
      surface_handle, surface_feeder, surface_media_type, surface_checkpoint,
      metric_key, metric_value, surface_metric_label, surface_metric_value,
      surface_stamp, created_at, updated_at
    ) values (
      p_feed_id, p_feeder_id, p_post_key, lower(coalesce(p_checkpoint,'d1')), p_business_date_ist,
      p_signal_code, p_context, p_family_group, p_alert_type,
      p_title, p_title, p_body, coalesce(p_payload, '{}'::jsonb), coalesce(p_payload, '{}'::jsonb),
      1, 1, 'new', p_dedupe_key,
      p_surface_percentile, p_surface_delta, p_surface_delta,
      p_surface_handle, p_surface_handle, p_surface_media_type, upper(coalesce(p_checkpoint,'d1')),
      p_metric_key, p_metric_value, p_metric_key, p_metric_value,
      p_surface_stamp, now(), now()
    )
    returning id into v_id;
  end if;

  return v_id;
end;
$$;

create or replace function public.fn_process_checkpoint(
  p_feeder_id bigint,
  p_checkpoint text,
  p_business_date date
) returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_cp text := lower(coalesce(p_checkpoint, 'd1'));
  v_day date := coalesce(p_business_date, (now() at time zone 'Asia/Kolkata')::date);
  v_rows int := 0;
  v_feed_id bigint;
  v_handle text;
  v_anchor_feeder_id bigint;
  v_anchor_median numeric;
  v_best_comp record;
  v_total_hot int := 0;
  v_reel_hot int := 0;
  v_feeders_hot int := 0;
  r record;
  v_signal text;
  v_alert_type text;
  v_surface_pct int;
  v_surface_delta int;
  v_hero_key text;
  v_hero_value bigint;
  v_hero_baseline bigint;
  v_hero_multiple numeric;
  v_resp_key text;
  v_resp_value bigint;
  v_resp_baseline bigint;
  v_resp_multiple numeric;
  v_resp_pct int;
  v_distribution_multiple numeric;
  v_current_pct int;
  v_d1 int;
  v_d3 int;
  v_d7 int;
  v_d21 int;
  v_thresh int;
  v_dedupe text;
  v_stamp text;
  v_title text;
  v_body text;
  v_payload jsonb;
begin
  if v_cp not in ('d1','d3','d7','d21') then
    return 0;
  end if;

  select fd.feed_id, fd.handle into v_feed_id, v_handle
  from public.feeders fd
  where fd.id = p_feeder_id;

  if v_feed_id is null then
    return 0;
  end if;

  for r in
    select
      pm.post_key,
      lower(pm.checkpoint) as checkpoint,
      lower(coalesce(p.media_type,'reel')) as media_type,
      coalesce(pm.percentile_performance, 100)::int as percentile_performance,
      pm.feed_percentile,
      pm.metric_value,
      pm.views,
      pm.likes,
      pm.comments,
      pm.interactions,
      pm.baseline_views,
      pm.baseline_likes,
      pm.baseline_comments,
      pm.baseline_interactions,
      pm.views_multiple,
      pm.likes_multiple,
      pm.comments_multiple,
      pm.interactions_multiple,
      pm.likes_percentile,
      pm.comments_percentile,
      pm.interactions_percentile,
      pm.rank_feed,
      pm.rank_all_time,
      pm.rank_recent_50,
      pm.d1_percentile,
      pm.d3_percentile,
      pm.d7_percentile,
      pm.d21_percentile,
      pm.best_in_days,
      pm.best_in_posts,
      pm.outperformed_last_20,
      pm.outperformed_last_50,
      pm.post_hour_ist,
      pm.hour_percentile_feeder,
      pm.hour_multiple_views,
      pm.hour_multiple_likes,
      pm.hour_multiple_comments,
      pm.is_best_hour_window,
      p.post_url,
      p.thumbnail_url
    from public.post_metrics pm
    join public.posts p on p.post_key = pm.post_key
    where p.feeder_id = p_feeder_id
      and lower(pm.checkpoint) = v_cp
      and coalesce(pm.captured_business_date_ist, pm.business_date_ist, (p.posted_at at time zone 'Asia/Kolkata')::date) = v_day
  loop
    v_surface_pct := r.percentile_performance;
    v_surface_delta := null;

    if r.media_type = 'reel' then
      v_hero_key := 'views';
      v_hero_value := coalesce(r.metric_value, r.views, 0);
      v_hero_baseline := nullif(coalesce(r.baseline_views, 0), 0);
      v_hero_multiple := coalesce(r.views_multiple, case when v_hero_baseline is null then null else v_hero_value::numeric / v_hero_baseline::numeric end);
      v_distribution_multiple := v_hero_multiple;
      v_resp_key := 'comments';
      v_resp_value := coalesce(r.comments, 0);
      v_resp_baseline := nullif(coalesce(r.baseline_comments, 0), 0);
      v_resp_multiple := coalesce(r.comments_multiple, case when v_resp_baseline is null then null else v_resp_value::numeric / v_resp_baseline::numeric end);
      v_resp_pct := coalesce(r.comments_percentile, 100);
    else
      v_hero_key := 'likes';
      v_hero_value := coalesce(r.likes, 0);
      v_hero_baseline := nullif(coalesce(r.baseline_likes, 0), 0);
      v_hero_multiple := coalesce(r.likes_multiple, case when v_hero_baseline is null then null else v_hero_value::numeric / v_hero_baseline::numeric end);
      v_distribution_multiple := v_hero_multiple;
      v_resp_key := 'interactions';
      v_resp_value := coalesce(r.interactions, coalesce(r.likes,0) + coalesce(r.comments,0), 0);
      v_resp_baseline := nullif(coalesce(r.baseline_interactions, 0), 0);
      v_resp_multiple := coalesce(r.interactions_multiple, case when v_resp_baseline is null then null else v_resp_value::numeric / v_resp_baseline::numeric end);
      v_resp_pct := coalesce(r.interactions_percentile, 100);
    end if;

    v_d1 := coalesce(r.d1_percentile, r.percentile_performance);
    v_d3 := coalesce(r.d3_percentile, r.percentile_performance);
    v_d7 := coalesce(r.d7_percentile, r.percentile_performance);
    v_d21 := coalesce(r.d21_percentile, r.percentile_performance);
    v_current_pct := case v_cp when 'd3' then v_d3 when 'd7' then v_d7 when 'd21' then v_d21 else r.percentile_performance end;
    v_surface_delta := case when v_cp = 'd1' then null else (v_d1 - v_current_pct) end;

    v_signal := null;
    if coalesce(v_hero_multiple, 0) >= 2 and v_surface_pct <= 10 then
      v_signal := 'breakout';
    elsif coalesce(v_resp_multiple, 0) >= 2 and coalesce(v_resp_pct, 100) <= 15 and coalesce(v_distribution_multiple, 999) <= 1.5 then
      v_signal := 'engagement';
    else
      v_thresh := case v_cp when 'd3' then 15 when 'd7' then 20 when 'd21' then 25 else 999 end;
      if v_cp in ('d3','d7','d21') and abs(coalesce(v_surface_delta, 0)) >= v_thresh then
        v_signal := 'trajectory';
      end if;
    end if;

    v_alert_type := case coalesce(v_signal, 'slot')
      when 'breakout' then 'blaze'
      when 'engagement' then 'burn'
      when 'trajectory' then 'burn'
      else 'spark'
    end;

    v_stamp := format('@%s · %s · %s · %s %s',
      upper(coalesce(v_handle,'feeder')),
      upper(coalesce(r.media_type,'post')),
      upper(v_cp),
      case when abs(v_hero_value) >= 1000000 then round(v_hero_value::numeric/1000000,1)::text || 'M'
           when abs(v_hero_value) >= 1000 then round(v_hero_value::numeric/1000,1)::text || 'K'
           else v_hero_value::text end,
      upper(v_hero_key)
    );

    v_title := case coalesce(v_signal, 'slot')
      when 'breakout' then 'Breakout'
      when 'engagement' then 'Engagement spike'
      when 'trajectory' then 'Trajectory shift'
      else 'Slot state'
    end;
    v_body := format('P%s at %s', coalesce(v_surface_pct, 100), upper(v_cp));

    v_payload := jsonb_build_object(
      'signal', coalesce(v_signal, 'slot'),
      'hero', jsonb_build_object('label', upper(v_hero_key), 'value', v_hero_value, 'baseline', v_hero_baseline, 'multiple', v_hero_multiple),
      'position', jsonb_build_object('percentile', v_surface_pct, 'feed_rank', r.rank_feed, 'rank_all_time', r.rank_all_time, 'rank_recent_50', r.rank_recent_50, 'shift', v_surface_delta),
      'response', jsonb_build_object('label', upper(v_resp_key), 'value', v_resp_value, 'baseline', v_resp_baseline, 'multiple', v_resp_multiple, 'percentile', v_resp_pct),
      'trajectory', case when v_cp='d1' then null else jsonb_build_object('d1', v_d1, 'd3', v_d3, 'd7', v_d7, 'd21', v_d21, 'current', v_current_pct, 'delta', v_surface_delta,
        'state', case when v_surface_delta >= 20 then 'accelerating' when v_surface_delta <= -20 then 'fading' else 'stable' end) end,
      'structural', jsonb_build_object('best_in_days', r.best_in_days, 'best_in_posts', r.best_in_posts, 'outperformed_last_20', r.outperformed_last_20, 'outperformed_last_50', r.outperformed_last_50, 'rank_all_time', r.rank_all_time),
      'timing', jsonb_build_object('hour', r.post_hour_ist, 'hour_percentile', r.hour_percentile_feeder,
        'hour_multiple', case when r.media_type='reel' then r.hour_multiple_views else coalesce(r.hour_multiple_likes, r.hour_multiple_comments) end,
        'is_peak', r.is_best_hour_window),
      'meta', jsonb_build_object('handle', v_handle, 'media_type', r.media_type, 'checkpoint', v_cp, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url)
    );

    v_dedupe := format('v2:%s:%s:%s', r.post_key, v_cp, v_day);

    perform public.fn_fire_upsert_row(
      v_dedupe, v_feed_id, p_feeder_id, r.post_key, v_cp, v_day,
      coalesce(v_signal, 'slot'), 'own', 'A', v_alert_type,
      v_title, v_body, v_payload,
      v_surface_pct, v_surface_delta, v_handle, r.media_type,
      v_hero_key, v_hero_value, v_stamp
    );

    v_rows := v_rows + 1;
  end loop;

  select min(fd.id) into v_anchor_feeder_id
  from public.feeders fd
  where fd.feed_id = v_feed_id and fd.status = 'active';

  select percentile_cont(0.5) within group(order by pm.percentile_performance)
    into v_anchor_median
  from public.post_metrics pm
  join public.posts p on p.post_key = pm.post_key
  where p.feeder_id = v_anchor_feeder_id
    and lower(pm.checkpoint) = v_cp
    and coalesce(pm.captured_business_date_ist, pm.business_date_ist, (p.posted_at at time zone 'Asia/Kolkata')::date) = v_day
    and pm.percentile_performance is not null;

  with feeder_medians as (
    select p.feeder_id, percentile_cont(0.5) within group(order by pm.percentile_performance) as med
    from public.post_metrics pm
    join public.posts p on p.post_key = pm.post_key
    where p.feeder_id in (select id from public.feeders where feed_id = v_feed_id and status='active')
      and lower(pm.checkpoint) = v_cp
      and coalesce(pm.captured_business_date_ist, pm.business_date_ist, (p.posted_at at time zone 'Asia/Kolkata')::date) = v_day
      and pm.percentile_performance is not null
    group by p.feeder_id
  )
  select fm.feeder_id, fm.med
  into v_best_comp
  from feeder_medians fm
  where fm.feeder_id <> v_anchor_feeder_id
  order by fm.med asc
  limit 1;

  if v_anchor_median is not null and v_best_comp.feeder_id is not null and (v_anchor_median - v_best_comp.med) >= 5 then
    perform public.fn_fire_upsert_row(
      format('v2:feed:%s:%s:%s:rival', v_feed_id, v_cp, v_day),
      v_feed_id, v_best_comp.feeder_id, null, v_cp, v_day,
      'rival', 'competitor', 'D', 'burn',
      'Rival gap', 'Competitor ahead of anchor baseline',
      jsonb_build_object('signal','rival','hero',jsonb_build_object('label','GAP','value', round(v_anchor_median - v_best_comp.med)::int),'meta',jsonb_build_object('checkpoint',v_cp,'business_date_ist',v_day)),
      least(100, greatest(1, round(v_best_comp.med)::int)),
      round(v_anchor_median - v_best_comp.med)::int,
      'feed', 'feed', 'gap', round(v_anchor_median - v_best_comp.med)::bigint,
      format('FEED · %s · RIVAL GAP %s', upper(v_cp), round(v_anchor_median - v_best_comp.med)::int)
    );
  end if;

  select
    count(*) filter (where pm.percentile_performance <= 20),
    count(*) filter (where pm.percentile_performance <= 20 and lower(coalesce(p.media_type,''))='reel'),
    count(distinct p.feeder_id) filter (where pm.percentile_performance <= 20)
  into v_total_hot, v_reel_hot, v_feeders_hot
  from public.post_metrics pm
  join public.posts p on p.post_key = pm.post_key
  where p.feeder_id in (select id from public.feeders where feed_id = v_feed_id and status='active')
    and lower(pm.checkpoint) = v_cp
    and coalesce(pm.captured_business_date_ist, pm.business_date_ist, (p.posted_at at time zone 'Asia/Kolkata')::date) = v_day;

  if v_total_hot > 0 and ((v_reel_hot::numeric / v_total_hot::numeric) >= 0.60 or v_feeders_hot >= 3) then
    perform public.fn_fire_upsert_row(
      format('v2:feed:%s:%s:%s:climate', v_feed_id, v_cp, v_day),
      v_feed_id, null, null, v_cp, v_day,
      'climate', 'macro', 'E', 'burn',
      'Feed climate shift', 'Distribution pressure changed at feed level',
      jsonb_build_object('signal','climate','hero',jsonb_build_object('label','REEL_SHARE', 'value', round((v_reel_hot::numeric / greatest(v_total_hot,1)::numeric)*100)::int),
                         'position',jsonb_build_object('hot_posts',v_total_hot,'hot_reels',v_reel_hot,'feeders_hot',v_feeders_hot),
                         'meta',jsonb_build_object('checkpoint',v_cp,'business_date_ist',v_day)),
      least(100, greatest(1, 100 - round((v_reel_hot::numeric / greatest(v_total_hot,1)::numeric)*100)::int)),
      null,
      'feed', 'feed', 'reel_share', round((v_reel_hot::numeric / greatest(v_total_hot,1)::numeric)*100)::bigint,
      format('FEED · %s · %s%% REELS', upper(v_cp), round((v_reel_hot::numeric / greatest(v_total_hot,1)::numeric)*100)::int)
    );
  end if;

  return v_rows;
end;
$$;

create or replace function public.enqueue_slot_state_alerts(
  p_feeder_id bigint,
  p_checkpoint text,
  p_business_date_ist date default null
) returns int
language plpgsql
security definer
set search_path = public
as $$
begin
  return public.fn_process_checkpoint(p_feeder_id, p_checkpoint, coalesce(p_business_date_ist, (now() at time zone 'Asia/Kolkata')::date));
end;
$$;

create or replace function public.fn_try_resolve_feed_signals(
  p_feed_id bigint,
  p_checkpoint text,
  p_business_date_ist date default null
) returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_day date := coalesce(p_business_date_ist, (now() at time zone 'Asia/Kolkata')::date);
  v_rows int := 0;
  r record;
begin
  for r in select id from public.feeders where feed_id = p_feed_id and status='active' loop
    v_rows := v_rows + coalesce(public.fn_process_checkpoint(r.id, p_checkpoint, v_day), 0);
  end loop;
  return v_rows;
end;
$$;

create or replace function public.fn_apply_fire_alert_contract()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  new.payload := coalesce(new.payload, '{}'::jsonb);
  new.intelligence_layers := coalesce(new.intelligence_layers, new.payload);

  if new.dedupe_key is null or btrim(new.dedupe_key) = '' then
    new.dedupe_key := coalesce(new.post_key, 'feed') || ':' || lower(coalesce(new.checkpoint,'d1')) || ':' || coalesce(new.business_date_ist::text, (now() at time zone 'Asia/Kolkata')::date::text);
  end if;

  if (new.surface_stamp is null or btrim(new.surface_stamp)='') then
    new.surface_stamp := format('@%s · %s · %s · %s %s',
      upper(coalesce(new.surface_handle, new.surface_feeder, 'feed')),
      upper(coalesce(new.surface_media_type, 'post')),
      upper(coalesce(new.checkpoint, 'd1')),
      coalesce(new.metric_value::text, '0'),
      upper(coalesce(new.metric_key, 'metric'))
    );
  end if;

  return new;
end;
$$;

drop trigger if exists trg_enqueue_phase1_fire_alerts on public.post_metrics;

create or replace function public.fn_get_fire_alerts(
  p_user_id uuid,
  p_days int default 7
)
returns setof public.fire_alerts
language sql
stable
security definer
set search_path = public
as $$
  select fa.*
  from public.fire_alerts fa
  where fa.status in ('new','sent')
    and fa.business_date_ist >= ((now() at time zone 'Asia/Kolkata')::date - greatest(coalesce(p_days,7),1) + 1)
  order by fa.business_date_ist desc, fa.created_at desc;
$$;

do $$
declare
  d date;
  r record;
  cp text;
begin
  for d in select gs::date from generate_series(date '2026-02-17', date '2026-02-21', interval '1 day') gs loop
    for r in select id from public.feeders where status='active' loop
      foreach cp in array array['d1','d3','d7','d21'] loop
        perform public.fn_process_checkpoint(r.id, cp, d);
      end loop;
    end loop;
  end loop;
end $$;

commit;
