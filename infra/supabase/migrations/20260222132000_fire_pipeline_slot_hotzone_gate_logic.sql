begin;

-- Slot hot-zone rule: only suppress >35 when no signal.
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
      pm.business_date_ist as anchor_business_date_ist,
      pm.captured_business_date_ist,
      p.post_url,
      p.thumbnail_url
    from public.post_metrics pm
    join public.posts p on p.post_key = pm.post_key
    where p.feeder_id = p_feeder_id
      and lower(pm.checkpoint) = v_cp
      and pm.captured_business_date_ist = v_day
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

    if v_signal is null and v_surface_pct > 35 then
      continue;
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
      'meta', jsonb_build_object('handle', v_handle, 'media_type', r.media_type, 'checkpoint', v_cp, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url, 'anchor_business_date_ist', r.anchor_business_date_ist, 'captured_business_date_ist', r.captured_business_date_ist)
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

  select fd.id into v_anchor_feeder_id
  from public.feeders fd
  where fd.feed_id = v_feed_id
    and fd.status = 'active'
    and fd.role = 'anchor'
  order by fd.id
  limit 1;

  select percentile_cont(0.5) within group(order by pm.percentile_performance)
    into v_anchor_median
  from public.post_metrics pm
  join public.posts p on p.post_key = pm.post_key
  where p.feeder_id = v_anchor_feeder_id
    and lower(pm.checkpoint) = v_cp
    and pm.captured_business_date_ist = v_day
    and pm.percentile_performance is not null;

  with feeder_medians as (
    select p.feeder_id, percentile_cont(0.5) within group(order by pm.percentile_performance) as med
    from public.post_metrics pm
    join public.posts p on p.post_key = pm.post_key
    where p.feeder_id in (select id from public.feeders where feed_id = v_feed_id and status='active')
      and lower(pm.checkpoint) = v_cp
      and pm.captured_business_date_ist = v_day
      and pm.percentile_performance is not null
    group by p.feeder_id
  )
  select fm.feeder_id, fm.med
  into v_best_comp
  from feeder_medians fm
  where fm.feeder_id <> v_anchor_feeder_id
  order by fm.med asc
  limit 1;

  if v_anchor_feeder_id is not null
     and v_anchor_median is not null
     and v_best_comp.feeder_id is not null
     and (v_anchor_median - v_best_comp.med) >= 5 then
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
    and pm.captured_business_date_ist = v_day;

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
