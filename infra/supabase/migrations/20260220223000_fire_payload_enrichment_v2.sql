-- Migration: Data-as-Headline Fire Alert Enrichment

create or replace function public.fn_resolve_post_signals(
  p_feed_id bigint,
  p_feeder_id bigint,
  p_checkpoint text,
  p_context text,
  p_family_group text,
  p_business_date_ist date default null
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_count int := 0;
  v_intensity int;
  v_reach_multiple numeric;
  v_reach_metric text;
  v_eff_gap numeric;
  v_day date := coalesce(p_business_date_ist, (now() at time zone 'Asia/Kolkata')::date);
  v_anchor_feeder_id bigint;
  v_anchor_handle text;
  v_gap_vs_anchor numeric;
  r record;
begin
  if p_checkpoint not in ('d1','d3','d7','d21') then
    return 0;
  end if;

  select fd.id, fd.handle
  into v_anchor_feeder_id, v_anchor_handle
  from public.feeders fd
  where fd.feed_id = p_feed_id
    and fd.role = 'anchor'
    and fd.status = 'active'
  limit 1;

  for r in
    with curr as (
      select
        pm.post_key,
        pm.checkpoint,
        pm.percentile_performance,
        pm.percentile_delta,
        pm.likes,
        pm.comments,
        pm.views,
        pm.likes_percentile,
        pm.comments_percentile,
        pm.views_percentile,
        pm.metric_value,
        p.media_type,
        p.posted_at,
        p.post_url,
        p.thumbnail_url,
        fd.handle,
        fd.id as feeder_id,
        fb.median_percentile,
        fb.p25_percentile,
        fb.p75_percentile,
        fb.median_likes,
        fb.median_comments,
        fb.median_views,
        fb.median_delta,
        fb.best_percentile_90d,
        fb.days_since_ceiling,
        fb.format_rank,
        fb.post_count_90d,
        (
          select count(*)
          from public.post_metrics pmx
          join public.posts px on px.post_key = pmx.post_key
          where px.feeder_id = fd.id
            and px.media_type = p.media_type
            and pmx.checkpoint = pm.checkpoint
            and pmx.metric_value is not null
            and px.posted_at < p.posted_at
            and px.posted_at >= p.posted_at - interval '90 days'
        ) as rank_pool_size,
        (
          select count(*)
          from (
            select pmx.metric_value
            from public.post_metrics pmx
            join public.posts px on px.post_key = pmx.post_key
            where px.feeder_id = fd.id
              and px.media_type = p.media_type
              and pmx.checkpoint = pm.checkpoint
              and pmx.metric_value is not null
              and px.posted_at < p.posted_at
            order by px.posted_at desc
            limit 20
          ) sub
          where sub.metric_value < pm.metric_value
        ) as rank_in_recent_20,
        (
          select count(*) + 1
          from public.post_metrics pmx
          join public.posts px on px.post_key = pmx.post_key
          where px.feeder_id = fd.id
            and px.media_type = p.media_type
            and pmx.checkpoint = pm.checkpoint
            and pmx.metric_value is not null
            and pmx.metric_value > pm.metric_value
        ) as all_time_rank,
        (
          select count(*)
          from public.post_metrics pmx
          join public.posts px on px.post_key = pmx.post_key
          where px.feeder_id = fd.id
            and px.media_type = p.media_type
            and pmx.checkpoint = 'd1'
            and pmx.percentile_performance <= 35
            and px.posted_at <= p.posted_at
            and px.posted_at >= p.posted_at - interval '60 days'
        ) as hot_streak_count,
        (
          select pm_d1.percentile_performance
          from public.post_metrics pm_d1
          where pm_d1.post_key = pm.post_key and pm_d1.checkpoint = 'd1'
          order by pm_d1.computed_at desc
          limit 1
        ) as d1_pct,
        (
          select pm_d3.percentile_performance
          from public.post_metrics pm_d3
          where pm_d3.post_key = pm.post_key and pm_d3.checkpoint = 'd3'
          order by pm_d3.computed_at desc
          limit 1
        ) as d3_pct,
        (
          select pm_d7.percentile_performance
          from public.post_metrics pm_d7
          where pm_d7.post_key = pm.post_key and pm_d7.checkpoint = 'd7'
          order by pm_d7.computed_at desc
          limit 1
        ) as d7_pct,
        (
          select pm_d21.percentile_performance
          from public.post_metrics pm_d21
          where pm_d21.post_key = pm.post_key and pm_d21.checkpoint = 'd21'
          order by pm_d21.computed_at desc
          limit 1
        ) as d21_pct
      from public.post_metrics pm
      join public.posts p on p.post_key = pm.post_key
      join public.feeders fd on fd.id = p.feeder_id
      left join public.feeder_baselines fb
        on fb.feeder_id = fd.id and fb.media_type = p.media_type and fb.checkpoint = pm.checkpoint
      where fd.id = p_feeder_id
        and pm.checkpoint = p_checkpoint
        and pm.percentile_performance is not null
        and (
           pm.percentile_performance <= 35 
           or pm.percentile_delta >= 6
           or (p_checkpoint = 'd1' and pm.percentile_performance >= 60)
           or (p_checkpoint in ('d3','d7','d21') and pm.percentile_performance > 35) 
        )
    )
    select * from curr
  loop
  
    -- A1 baseline displacement
    if r.median_percentile is not null and r.percentile_performance <= 35 then
      v_intensity := least(100, greatest(5, (r.median_percentile - r.percentile_performance) * 5));
      if (r.median_percentile - r.percentile_performance) >= 5 then
        
        v_reach_multiple := null;
        if r.media_type = 'reel' and r.median_views is not null and r.median_views > 0 and r.views is not null then
          v_reach_multiple := r.views::numeric / r.median_views::numeric;
        elsif r.median_likes is not null and r.median_likes > 0 and r.likes is not null then
          v_reach_multiple := r.likes::numeric / r.median_likes::numeric;
        end if;
        
        perform public.fn_upsert_fire_alert(
          v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
          v_context, 'A', 'A1_baseline_displacement', v_intensity,
          case when r.media_type = 'reel' then format('%s VIEWS', public.fn_format_number_k(r.views)) else format('%s LIKES', public.fn_format_number_k(r.likes)) end,
          'A1 body',
          v_business_date_ist,
          jsonb_build_object(
            'handle', r.handle, 'media_type', r.media_type, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url,
            'checkpoint', r.checkpoint, 'percentile', r.percentile_performance, 'baseline_median', r.median_percentile,
            'views', r.views, 'likes', r.likes, 'comments', r.comments, 'metric_value', r.metric_value,
            'rank_in_recent_20', r.rank_in_recent_20, 'all_time_rank', r.all_time_rank, 'post_count_90d', r.post_count_90d,
            'reach_multiple', round(v_reach_multiple, 2)
          )
        );
        v_count := v_count + 1;
      end if;
    end if;

    -- A2 ceiling break
    if r.percentile_performance <= 35 and coalesce(r.days_since_ceiling,0) >= 21 and (r.best_percentile_90d is null or r.percentile_performance < r.best_percentile_90d) then
      v_intensity := case
        when r.days_since_ceiling >= 120 then 90
        when r.days_since_ceiling >= 60 then 65
        else 42
      end;
      perform public.fn_upsert_fire_alert(
        v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
        v_context, 'A', 'A2_ceiling_break', v_intensity,
        case when r.media_type = 'reel' then format('%s VIEWS', public.fn_format_number_k(r.views)) else format('%s LIKES', public.fn_format_number_k(r.likes)) end,
        'A2 body',
        v_business_date_ist,
        jsonb_build_object(
          'handle', r.handle, 'media_type', r.media_type, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url,
          'days_since_ceiling', r.days_since_ceiling, 'best_90d', r.best_percentile_90d, 'percentile', r.percentile_performance,
          'views', r.views, 'likes', r.likes, 'comments', r.comments, 'metric_value', r.metric_value,
          'all_time_rank', r.all_time_rank
        )
      );
      v_count := v_count + 1;
    end if;

    -- A3 reach explosion
    v_reach_multiple := null;
    if r.percentile_performance <= 35 then
      if r.media_type = 'reel' and r.median_views is not null and r.median_views > 0 and r.views is not null then
        v_reach_multiple := r.views::numeric / r.median_views::numeric;
        v_reach_metric := 'views';
      elsif r.median_likes is not null and r.median_likes > 0 and r.likes is not null then
        v_reach_multiple := r.likes::numeric / r.median_likes::numeric;
        v_reach_metric := 'likes';
      end if;
    end if;

    if v_reach_multiple is not null and v_reach_multiple >= 1.5 then
      v_intensity := case
        when v_reach_multiple >= 5 then 90
        when v_reach_multiple >= 3 then 65
        else 42
      end;
      perform public.fn_upsert_fire_alert(
        v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
        v_context, 'A', 'A3_reach_explosion', v_intensity,
        case when r.media_type = 'reel' then format('%s VIEWS', public.fn_format_number_k(r.views)) else format('%s LIKES', public.fn_format_number_k(r.likes)) end,
        'A3 body',
        v_business_date_ist,
        jsonb_build_object(
          'handle', r.handle, 'media_type', r.media_type, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url,
          'reach_metric', v_reach_metric, 'reach_multiple', round(v_reach_multiple,2), 'checkpoint', r.checkpoint,
          'views', r.views, 'likes', r.likes, 'comments', r.comments, 'metric_value', r.metric_value,
          'rank_in_recent_20', r.rank_in_recent_20, 'all_time_rank', r.all_time_rank, 'post_count_90d', r.post_count_90d
        )
      );
      v_count := v_count + 1;
    end if;

    -- A4 engagement efficiency
    v_eff_gap := null;
    if r.percentile_performance <= 35 and r.comments_percentile is not null then
      if r.media_type = 'reel' and r.views_percentile is not null then
        v_eff_gap := r.views_percentile - r.comments_percentile;
      elsif r.likes_percentile is not null then
        v_eff_gap := r.likes_percentile - r.comments_percentile;
      end if;
    end if;

    if v_eff_gap is not null and v_eff_gap >= 12 then
      v_intensity := case
        when v_eff_gap >= 30 then 90
        when v_eff_gap >= 20 then 65
        else 42
      end;
      perform public.fn_upsert_fire_alert(
        v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
        v_context, 'A', 'A4_engagement_efficiency', v_intensity,
        format('%s COMMENTS', public.fn_format_number_k(r.comments)),
        'A4 body',
        v_business_date_ist,
        jsonb_build_object(
          'handle', r.handle, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url, 'media_type', r.media_type,
          'gap', v_eff_gap, 'comments_percentile', r.comments_percentile, 'percentile', r.percentile_performance,
          'likes_percentile', r.likes_percentile, 'views_percentile', r.views_percentile, 'checkpoint', r.checkpoint,
          'views', r.views, 'likes', r.likes, 'comments', r.comments, 'metric_value', r.metric_value
        )
      );
      v_count := v_count + 1;
    end if;

    -- A5 cold open (D1 only)
    if r.checkpoint = 'd1' and r.percentile_performance >= 60 then
      v_intensity := case
        when r.percentile_performance >= 90 then 90
        when r.percentile_performance >= 80 then 65
        else 42
      end;
      perform public.fn_upsert_fire_alert(
        v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
        v_context, 'A', 'A5_cold_open', v_intensity,
        case when r.media_type = 'reel' then format('%s VIEWS', public.fn_format_number_k(r.views)) else format('%s LIKES', public.fn_format_number_k(r.likes)) end,
        'A5 body',
        v_business_date_ist,
        jsonb_build_object(
          'handle', r.handle, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url, 'media_type', r.media_type,
          'percentile', r.percentile_performance,
          'views', r.views, 'likes', r.likes, 'comments', r.comments, 'metric_value', r.metric_value
        )
      );
      v_count := v_count + 1;
    end if;

    -- A6 consistency streak
    if r.percentile_performance <= 35 and coalesce(r.hot_streak_count,0) >= 3 then
      v_intensity := case
        when r.hot_streak_count >= 8 then 90
        when r.hot_streak_count >= 5 then 65
        else 42
      end;
      perform public.fn_upsert_fire_alert(
        v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
        v_context, 'A', 'A6_consistency_streak', v_intensity,
        format('%s-POST STREAK', r.hot_streak_count),
        'A6 body',
        v_business_date_ist,
        jsonb_build_object(
          'handle', r.handle, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url, 'media_type', r.media_type,
          'streak_count', r.hot_streak_count, 'percentile', r.percentile_performance, 'checkpoint', r.checkpoint,
          'views', r.views, 'likes', r.likes, 'comments', r.comments, 'metric_value', r.metric_value
        )
      );
      v_count := v_count + 1;
    end if;

    -- B1 structural acceleration
    if r.checkpoint in ('d3','d7','d21') and r.percentile_delta is not null and r.percentile_delta >= 6 and r.percentile_performance <= 35 then
      v_intensity := case
        when r.percentile_delta >= 25 then 90
        when r.percentile_delta >= 15 then 65
        else 42
      end;
      perform public.fn_upsert_fire_alert(
        v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
        v_context, 'B', 'B1_structural_acceleration', v_intensity,
        format('+%s POSITIONS', r.percentile_delta),
        'B1 body',
        v_business_date_ist,
        jsonb_build_object(
          'handle', r.handle, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url, 'media_type', r.media_type,
          'delta', r.percentile_delta, 'percentile', r.percentile_performance, 'checkpoint', r.checkpoint, 'd1_percentile', r.d1_pct,
          'views', r.views, 'likes', r.likes, 'comments', r.comments, 'metric_value', r.metric_value,
          'd3_pct', r.d3_pct, 'd7_pct', r.d7_pct, 'd21_pct', r.d21_pct, 'rank_in_recent_20', r.rank_in_recent_20
        )
      );
      v_count := v_count + 1;
    end if;

    -- B2 late bloomer
    if r.percentile_performance <= 35 and r.d1_pct is not null and r.d1_pct > 50 and r.percentile_performance is not null and (r.d1_pct - r.percentile_performance) >= 12 then
      v_intensity := case
        when r.checkpoint = 'd7' and r.percentile_performance <= 10 then 90
        when r.percentile_performance <= 35 then 65
        else 42
      end;
      perform public.fn_upsert_fire_alert(
        v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
        v_context, 'B', 'B2_late_bloomer', v_intensity,
        format('+%sPT RECOVERY', (r.d1_pct - r.percentile_performance)),
        'B2 body',
        v_business_date_ist,
        jsonb_build_object(
          'handle', r.handle, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url, 'media_type', r.media_type,
          'd1_percentile', r.d1_pct, 'current_percentile', r.percentile_performance, 'checkpoint', r.checkpoint,
          'views', r.views, 'likes', r.likes, 'comments', r.comments, 'metric_value', r.metric_value,
          'd3_pct', r.d3_pct, 'd7_pct', r.d7_pct, 'd21_pct', r.d21_pct, 'rank_in_recent_20', r.rank_in_recent_20
        )
      );
      v_count := v_count + 1;
    end if;

    -- B3 early fade
    if r.percentile_performance > 35 and r.d1_pct is not null and r.d1_pct <= 35 and r.percentile_performance is not null and (r.percentile_performance - r.d1_pct) >= 8 then
      v_intensity := case
        when (r.percentile_performance - r.d1_pct) >= 30 or r.percentile_performance > 60 then 90
        when (r.percentile_performance - r.d1_pct) >= 18 then 65
        else 42
      end;
      perform public.fn_upsert_fire_alert(
        v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
        v_context, 'B', 'B3_early_fade', v_intensity,
        format('-%sPT DROP', (r.percentile_performance - r.d1_pct)),
        'B3 body',
        v_business_date_ist,
        jsonb_build_object(
          'handle', r.handle, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url, 'media_type', r.media_type,
          'd1_percentile', r.d1_pct, 'current_percentile', r.percentile_performance, 'checkpoint', r.checkpoint,
          'views', r.views, 'likes', r.likes, 'comments', r.comments, 'metric_value', r.metric_value,
          'd3_pct', r.d3_pct, 'd7_pct', r.d7_pct, 'd21_pct', r.d21_pct
        )
      );
      v_count := v_count + 1;
    end if;

    -- B4 sustained strength
    if r.percentile_performance <= 35 and r.d1_pct is not null and r.d1_pct <= 35 then
      if (r.checkpoint = 'd3' and r.d3_pct is not null and r.d3_pct <= 35 and abs(r.d3_pct - r.d1_pct) <= 5)
         or (r.checkpoint = 'd7' and r.d3_pct is not null and r.d7_pct is not null and r.d3_pct <= 35 and r.d7_pct <= 35 and abs(r.d7_pct - r.d1_pct) <= 5)
         or (r.checkpoint = 'd21' and r.d3_pct is not null and r.d7_pct is not null and r.d21_pct is not null and r.d3_pct <= 35 and r.d7_pct <= 35 and r.d21_pct <= 35)
      then
        v_intensity := case when r.checkpoint='d21' then 90 when r.checkpoint='d7' then 65 else 42 end;
        perform public.fn_upsert_fire_alert(
          v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
          v_context, 'B', 'B4_sustained_strength', v_intensity,
          format('HELD TOP %s%%', r.percentile_performance),
          'B4 body',
          v_business_date_ist,
          jsonb_build_object(
            'handle', r.handle, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url, 'media_type', r.media_type, 'percentile', r.percentile_performance,
            'd1', r.d1_pct, 'd3', r.d3_pct, 'd7', r.d7_pct, 'd21', r.d21_pct, 'checkpoint', r.checkpoint,
            'views', r.views, 'likes', r.likes, 'comments', r.comments, 'metric_value', r.metric_value
          )
        );
        v_count := v_count + 1;
      end if;
    end if;

    -- competitor context mirror for non-anchor feeders (anchor-relative signal)
    if r.percentile_performance <= 35 and v_anchor_feeder_id is not null and r.feeder_id <> v_anchor_feeder_id then
      select fb_anchor.median_percentile - r.percentile_performance
      into v_gap_vs_anchor
      from public.feeder_baselines fb_anchor
      where fb_anchor.feeder_id = v_anchor_feeder_id
        and fb_anchor.media_type = r.media_type
        and fb_anchor.checkpoint = r.checkpoint;

      if v_gap_vs_anchor is not null and v_gap_vs_anchor >= 5 then
        v_intensity := case
          when v_gap_vs_anchor >= 20 then 90
          when v_gap_vs_anchor >= 12 then 65
          else 42
        end;

        perform public.fn_upsert_fire_alert(
          v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
          'competitor', 'D', 'D1_anchor_displacement', v_intensity,
          format('%sPT RIVAL GAP', v_gap_vs_anchor),
          'D1 body',
          v_business_date_ist,
          jsonb_build_object(
            'competitor_handle', r.handle, 'anchor_handle', v_anchor_handle,
            'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url, 'media_type', r.media_type, 'percentile', r.percentile_performance,
            'gap_vs_anchor', v_gap_vs_anchor, 'checkpoint', r.checkpoint,
            'views', r.views, 'likes', r.likes, 'comments', r.comments, 'metric_value', r.metric_value
          )
        );
        v_count := v_count + 1;
      end if;
    end if;

  end loop;

  return v_count;
end;
$$;

create or replace function public.fn_resolve_feed_signals(
  p_feed_id bigint,
  p_checkpoint text,
  p_business_date_ist date default null
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_count int := 0;
  v_day date := coalesce(p_business_date_ist, (now() at time zone 'Asia/Kolkata')::date);
  v_anchor_feeder_id bigint;
  v_anchor_handle text;
  v_total_hot int;
  v_reel_hot int;
  v_feeders_hot int;
  v_feeders_total int;
  v_comp record;
  v_anchor_median numeric;
  v_comp_median numeric;
  v_gap numeric;
begin
  if p_checkpoint not in ('d1','d3','d7','d21') then
    return 0;
  end if;

  select fd.id, fd.handle
  into v_anchor_feeder_id, v_anchor_handle
  from public.feeders fd
  where fd.feed_id = p_feed_id
    and fd.role = 'anchor'
    and fd.status = 'active'
  limit 1;

  select count(*)::int,
         count(*) filter (where p.media_type = 'reel')::int,
         count(distinct p.feeder_id)::int
  into v_total_hot, v_reel_hot, v_feeders_hot
  from public.post_metrics pm
  join public.posts p on p.post_key = pm.post_key
  join public.feeders fd on fd.id = p.feeder_id
  where fd.feed_id = p_feed_id
    and pm.checkpoint = p_checkpoint
    and pm.percentile_performance <= 35
    and (p.posted_at at time zone 'Asia/Kolkata')::date = v_day;

  select count(*)::int into v_feeders_total
  from public.feeders fd
  where fd.feed_id = p_feed_id and fd.status='active';

  -- E1 format takeover (macro)
  if coalesce(v_total_hot,0) > 0 then
    perform public.fn_upsert_fire_alert(
      p_feed_id, null, null, p_checkpoint,
      'macro', 'E', 'E1_format_takeover',
      case
        when (v_reel_hot::numeric / v_total_hot::numeric) >= 0.9 then 90
        when (v_reel_hot::numeric / v_total_hot::numeric) >= 0.75 then 65
        when (v_reel_hot::numeric / v_total_hot::numeric) >= 0.6 then 42
        else 0
      end,
      format('%s%% REELS', round((v_reel_hot::numeric / v_total_hot::numeric)*100)::int),
      'E1 body',
      v_day,
      jsonb_build_object('feed_id', p_feed_id, 'checkpoint', p_checkpoint, 'day', v_day,
                         'reel_hot', v_reel_hot, 'total_hot', v_total_hot)
    );
    v_count := v_count + 1;
  end if;

  -- E3 structural convergence
  if coalesce(v_feeders_hot,0) >= 3 then
    perform public.fn_upsert_fire_alert(
      p_feed_id, null, null, p_checkpoint,
      'macro', 'E', 'E3_structural_convergence',
      case
        when v_feeders_hot >= 7 then 90
        when v_feeders_hot >= 5 then 65
        else 42
      end,
      format('%s FEEDERS WARM', v_feeders_hot),
      'E3 body',
      v_day,
      jsonb_build_object('feed_id', p_feed_id, 'checkpoint', p_checkpoint, 'day', v_day,
                         'feeders_hot', v_feeders_hot, 'feeders_total', v_feeders_total)
    );
    v_count := v_count + 1;
  end if;

  -- D1/D2/D3 competitive comparisons vs anchor
  if v_anchor_feeder_id is not null then
    for v_comp in
      select fd.id as feeder_id, fd.handle
      from public.feeders fd
      where fd.feed_id = p_feed_id
        and fd.status = 'active'
        and fd.id <> v_anchor_feeder_id
    loop
      select percentile_cont(0.5) within group(order by pm.percentile_performance)
      into v_anchor_median
      from public.post_metrics pm
      join public.posts p on p.post_key = pm.post_key
      where p.feeder_id = v_anchor_feeder_id
        and pm.checkpoint = p_checkpoint
        and pm.percentile_performance is not null
        and p.posted_at >= now() - interval '30 days';

      select percentile_cont(0.5) within group(order by pm.percentile_performance)
      into v_comp_median
      from public.post_metrics pm
      join public.posts p on p.post_key = pm.post_key
      where p.feeder_id = v_comp.feeder_id
        and pm.checkpoint = p_checkpoint
        and pm.percentile_performance is not null
        and p.posted_at >= now() - interval '30 days';

      if v_anchor_median is not null and v_comp_median is not null then
        v_gap := v_anchor_median - v_comp_median;

        -- D1 anchor displacement
        if v_gap >= 5 then
          perform public.fn_upsert_fire_alert(
            p_feed_id, v_comp.feeder_id, null, p_checkpoint,
            'competitor', 'D', 'D1_anchor_displacement',
            case when v_gap >= 20 then 90 when v_gap >= 12 then 65 else 42 end,
            format('%sPT RIVAL GAP', round(v_gap)::int),
            'D1 feed body',
            v_day,
            jsonb_build_object('anchor_handle', v_anchor_handle, 'competitor_handle', v_comp.handle,
                               'anchor_median', round(v_anchor_median)::int, 'competitor_median', round(v_comp_median)::int,
                               'gap', round(v_gap)::int, 'checkpoint', p_checkpoint)
          );
          v_count := v_count + 1;
        end if;

        -- D2 gap closing (simple parity gate)
        if abs(v_gap) <= 15 then
          perform public.fn_upsert_fire_alert(
            p_feed_id, v_comp.feeder_id, null, p_checkpoint,
            'competitor', 'D', 'D2_competitive_gap_closing',
            case when abs(v_gap) <= 2 then 90 when abs(v_gap) <= 8 then 65 else 42 end,
            format('WITHIN %s PTS', abs(round(v_gap)::int)),
            'D2 body',
            v_day,
            jsonb_build_object('anchor_handle', v_anchor_handle, 'competitor_handle', v_comp.handle,
                               'gap', round(v_gap)::int, 'checkpoint', p_checkpoint)
          );
          v_count := v_count + 1;
        end if;

        -- D3 regime break (competitor median beats own 90d best threshold)
        if v_comp_median <= 20 then
          perform public.fn_upsert_fire_alert(
            p_feed_id, v_comp.feeder_id, null, p_checkpoint,
            'competitor', 'D', 'D3_competitor_regime_break',
            case when v_comp_median <= 8 then 90 when v_comp_median <= 14 then 65 else 42 end,
            format('TOP %s%% MEDIAN', round(v_comp_median)::int),
            'D3 body',
            v_day,
            jsonb_build_object('competitor_handle', v_comp.handle, 'competitor_median', round(v_comp_median)::int,
                               'checkpoint', p_checkpoint)
          );
          v_count := v_count + 1;
        end if;
      end if;
    end loop;
  end if;

  return v_count;
end;
$$;

create or replace function public.fn_compose_alert_body(
  p_signal_code text,
  p_payload jsonb,
  p_headline text,
  p_checkpoint text,
  p_fallback_body text
) returns text
language plpgsql
immutable
as $$
declare
  v_body text;
  v_cp text := upper(p_checkpoint);
  
  -- extracted payload vars
  v_metric_value numeric := (p_payload->>'metric_value')::numeric;
  v_views_val numeric := (p_payload->>'views')::numeric;
  v_likes_val numeric := (p_payload->>'likes')::numeric;
  v_comments_val numeric := (p_payload->>'comments')::numeric;
  v_media_type text := p_payload->>'media_type';
  v_media_label text := public.fn_media_label(v_media_type);
  
  -- extracted metrics
  v_metric_label text := case when v_media_type = 'reel' then 'views' else 'likes' end;
  v_metric_str text;
  
  -- ranking and comparing
  v_rank_recent numeric := (p_payload->>'rank_in_recent_20')::numeric;
  v_pool_size numeric := coalesce(least(20, (p_payload->>'post_count_90d')::numeric), 0);
  v_days numeric := (p_payload->>'days_since_ceiling')::numeric;
  v_percentile numeric := (p_payload->>'percentile')::numeric;
  v_streak_count numeric := (p_payload->>'streak_count')::numeric;
  
  -- gap and percentiles
  v_gap numeric := (p_payload->>'gap')::numeric;
  v_comments_pct numeric := (p_payload->>'comments_percentile')::numeric;
  v_reach_pct numeric := case when v_media_type = 'reel' then (p_payload->>'views_percentile')::numeric else (p_payload->>'likes_percentile')::numeric end;
  v_d1_pct numeric := (p_payload->>'d1_percentile')::numeric;
  v_d1_raw numeric := (p_payload->>'d1')::numeric;
  v_curr_pct numeric := (p_payload->>'current_percentile')::numeric;
  v_delta numeric := (p_payload->>'delta')::numeric;
  v_comp_handle text := p_payload->>'competitor_handle';
  v_gap_anchor numeric := (p_payload->>'gap_vs_anchor')::numeric;
  
  -- feed level
  v_reel_hot numeric := (p_payload->>'reel_hot')::numeric;
  v_total_hot numeric := (p_payload->>'total_hot')::numeric;
  v_feeders_hot numeric := (p_payload->>'feeders_hot')::numeric;
  v_feeders_total numeric := (p_payload->>'feeders_total')::numeric;
  v_anchor_handle text := p_payload->>'anchor_handle';
  v_comp_median numeric := (p_payload->>'competitor_median')::numeric;
  
  -- previous checkpoint logic for trajectory
  v_prev_cp text;
  v_prev_pct numeric;

begin
  if v_metric_value is not null then
    v_metric_str := format('%s %s', public.fn_format_number_k(v_metric_value), v_metric_label);
  end if;

  if v_cp = 'D3' then 
    v_prev_cp := 'D1'; v_prev_pct := (p_payload->>'d1_pct')::numeric;
  elsif v_cp = 'D7' then 
    v_prev_cp := 'D3'; v_prev_pct := (p_payload->>'d3_pct')::numeric;
  elsif v_cp = 'D21' then 
    v_prev_cp := 'D7'; v_prev_pct := (p_payload->>'d7_pct')::numeric;
  else
    v_prev_cp := 'D1'; v_prev_pct := v_d1_pct;
  end if;

  case p_signal_code
  
    -- A1 & A3 Peak performance (stronger than X recent)
    when 'A1_baseline_displacement', 'A3_reach_explosion' then
      if v_metric_str is not null and v_rank_recent is not null and v_pool_size >= 2 then
        v_body := format('%s — stronger than %s of your last %s %ss.', v_metric_str, v_rank_recent, v_pool_size, v_media_label);
      elsif v_metric_str is not null and v_percentile is not null then
        v_body := format('%s — top %s%% at %s.', v_metric_str, v_percentile, v_cp);
      end if;
      
    -- A2 Ceiling
    when 'A2_ceiling_break' then
      if v_metric_str is not null and v_days is not null then
        v_body := format('%s — strongest %s in %s days.', v_metric_str, v_media_label, v_days);
      elsif v_metric_str is not null and v_percentile is not null then
        v_body := format('%s — top %s%% at %s.', v_metric_str, v_percentile, v_cp);
      end if;
      
    -- A4 Efficiency
    when 'A4_engagement_efficiency' then
      if v_comments_val is not null and v_comments_pct is not null and v_reach_pct is not null then
        v_body := format('%s comments — comments top %s%%, reach top %s%%.', public.fn_format_number_k(v_comments_val), v_comments_pct, v_reach_pct);
      elsif v_gap is not null then
        v_body := format('Comments are %s positions ahead of reach.', v_gap);
      end if;

    -- A5 Cold Open
    when 'A5_cold_open' then
      if v_metric_str is not null and v_percentile is not null then
        v_body := format('%s — opened at bottom %s%% on day 1.', v_metric_str, v_percentile);
      end if;

    -- A6 Streak
    when 'A6_consistency_streak' then
      if v_streak_count is not null then
        v_body := format('%s %ss in a row inside the hot zone.', v_streak_count, v_media_label);
      end if;

    -- B1 Climb
    when 'B1_structural_acceleration' then
      if v_metric_str is not null and v_prev_pct is not null and v_percentile is not null then
        v_body := format('%s — climbed from P%s at %s to P%s at %s.', v_metric_str, v_prev_pct, v_prev_cp, v_percentile, v_cp);
      elsif v_metric_str is not null and v_delta is not null then
        v_body := format('%s — climbed %s positions since opening at D1.', v_metric_str, v_delta);
      end if;

    -- B2 Late
    when 'B2_late_bloomer' then
      if v_metric_str is not null and v_d1_pct is not null and v_curr_pct is not null then
        v_body := format('%s — opened at P%s, now reached P%s by %s.', v_metric_str, v_d1_pct, v_curr_pct, v_cp);
      end if;

    -- B3 Fade
    when 'B3_early_fade' then
      if v_metric_str is not null and v_d1_pct is not null and v_curr_pct is not null then
        v_body := format('%s — opened at P%s, dropping to P%s by %s.', v_metric_str, v_d1_pct, v_curr_pct, v_cp);
      end if;

    -- B4 Sustained
    when 'B4_sustained_strength' then
      if v_metric_str is not null then
        v_body := format('%s — held the hot zone from D1 through %s.', v_metric_str, v_cp);
      end if;

    -- D1 anchor  (Post level)
    when 'D1_anchor_displacement' then
      if v_comp_handle is not null and v_metric_str is not null and v_gap_anchor is not null then
        v_body := format('@%s posted %s — %s positions ahead of your baseline.', v_comp_handle, v_metric_str, v_gap_anchor);
      -- D1 anchor feed level fallback
      elsif v_comp_handle is not null and v_gap is not null and v_anchor_handle is not null then
        v_body := format('@%s median is %s positions ahead of @%s at %s.', v_comp_handle, v_gap, v_anchor_handle, v_cp);
      end if;

    -- D2 Gap Closing
    when 'D2_competitive_gap_closing' then
      if v_comp_handle is not null and v_gap is not null and v_anchor_handle is not null then
        v_body := format('@%s is within %s positions of @%s at %s.', v_comp_handle, v_gap, v_anchor_handle, v_cp);
      end if;

    -- D3 Regime Break
    when 'D3_competitor_regime_break' then
      if v_comp_handle is not null and v_comp_median is not null then
        v_body := format('@%s is now holding a median of top %s%% at %s.', v_comp_handle, v_comp_median, v_cp);
      end if;

    -- E1 Format
    when 'E1_format_takeover' then
      if v_reel_hot is not null and v_total_hot is not null and v_total_hot > 0 then
        v_body := format('Reels hold %s%% of hot-zone posts in this feed at %s.', round((v_reel_hot / v_total_hot) * 100)::int, v_cp);
      end if;

    -- E3 Convergence
    when 'E3_structural_convergence' then
      if v_feeders_hot is not null and v_feeders_total is not null then
        v_body := format('%s of %s feeders entered the hot zone in the same %s cycle.', v_feeders_hot, v_feeders_total, v_cp);
      end if;

    else
      v_body := null;

  end case;

  -- final fallback to safely handle null returns if something was missing
  if v_body is null then
    v_body := coalesce(nullif(trim(p_fallback_body), ''), format('%s at %s.', coalesce(nullif(trim(p_headline), ''), 'Signal'), v_cp));
  end if;

  return v_body;
end;
$$;
