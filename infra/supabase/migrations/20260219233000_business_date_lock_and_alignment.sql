-- Lock business-date context across retries and resolver runs.
-- Prevent post_metrics day drift after midnight and stabilize resolver date selection.

begin;

-- Keep compute trigger schema-safe for current engine expectations.
alter table public.post_metrics
  add column if not exists percentile_tag text,
  add column if not exists captured_business_date_ist date,
  add column if not exists d1_source text;

-- Align compute trigger with explicit business-date capture.
create or replace function public.tg_compute_post_metrics()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_media_type text;
  v_feeder_id bigint;
  v_feed_id bigint;
  v_follower_count bigint;
  v_mv numeric;
  v_pool_size int;
  v_rank int;
  v_pct int;
  v_feed_pool_size int;
  v_feed_rank int;
  v_feed_pct int;
  v_d1_pct int;
begin
  select p.media_type, p.feeder_id
  into v_media_type, v_feeder_id
  from public.posts p
  where p.post_key = new.post_key;

  if v_feeder_id is not null then
    select fd.feed_id, fd.follower_count
    into v_feed_id, v_follower_count
    from public.feeders fd
    where fd.id = v_feeder_id;
  end if;

  v_mv := public.fn_metric_value(coalesce(v_media_type, 'image'), new.views, new.likes, new.comments);

  v_pct := null;
  if v_mv is not null and v_feeder_id is not null then
    select count(*) + 1
    into v_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.post_key <> new.post_key
      and pm.metric_value is not null
      and pm.metric_value > v_mv;

    select count(*) + 1
    into v_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.metric_value is not null;

    if v_pool_size > 0 then
      v_pct := greatest(1, least(100, round((v_rank::numeric / v_pool_size) * 100)));
    end if;
  end if;

  v_feed_pct := null;
  if v_mv is not null and v_feed_id is not null then
    select count(*) + 1
    into v_feed_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    join public.feeders f2 on f2.id = p2.feeder_id
    where f2.feed_id = v_feed_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.post_key <> new.post_key
      and pm.metric_value is not null
      and pm.metric_value > v_mv;

    select count(*) + 1
    into v_feed_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    join public.feeders f2 on f2.id = p2.feeder_id
    where f2.feed_id = v_feed_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.metric_value is not null;

    if v_feed_pool_size > 0 then
      v_feed_pct := greatest(1, least(100, round((v_feed_rank::numeric / v_feed_pool_size) * 100)));
    end if;
  end if;

  select pm.percentile_performance
  into v_d1_pct
  from public.post_metrics pm
  where pm.post_key = new.post_key
    and pm.checkpoint = 'd1'
  order by pm.computed_at desc
  limit 1;

  new.metric_value := v_mv;
  new.percentile_performance := v_pct;
  new.percentile_tag := public.fn_percentile_tag(v_pct);
  new.feed_percentile := v_feed_pct;
  new.percentile_delta := case
    when new.checkpoint = 'd1' then null
    when v_d1_pct is null or v_pct is null then null
    else v_d1_pct - v_pct
  end;
  new.perf_score := case
    when v_mv is not null and v_follower_count is not null and v_follower_count > 0
      then round((v_mv / v_follower_count) * 100.0, 4)
    else null
  end;

  -- Business-date lock: if worker does not pass explicit date, fallback to computed_at day in IST.
  if new.captured_business_date_ist is null then
    new.captured_business_date_ist := (coalesce(new.computed_at, now()) at time zone 'Asia/Kolkata')::date;
  end if;

  if new.d1_source is null then
    if new.checkpoint = 'd1' then
      new.d1_source := 'on_time';
    elsif new.checkpoint = 'd2b' then
      new.d1_source := 'from_d2b';
    end if;
  end if;

  return new;
end;
$$;

-- Feed-level resolver should group by captured_business_date_ist for cycle alignment.
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
    and coalesce(pm.captured_business_date_ist, (p.posted_at at time zone 'Asia/Kolkata')::date) = v_day;

  select count(*)::int into v_feeders_total
  from public.feeders fd
  where fd.feed_id = p_feed_id and fd.status='active';

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
      'Format takeover',
      format('Reels hold %s%% of hot-zone posts in this feed at %s.', round((v_reel_hot::numeric / v_total_hot::numeric)*100)::int, upper(p_checkpoint)),
      v_day,
      jsonb_build_object('feed_id', p_feed_id, 'checkpoint', p_checkpoint, 'day', v_day,
                         'reel_hot', v_reel_hot, 'total_hot', v_total_hot)
    );
    v_count := v_count + 1;
  end if;

  if coalesce(v_feeders_hot,0) >= 3 then
    perform public.fn_upsert_fire_alert(
      p_feed_id, null, null, p_checkpoint,
      'macro', 'E', 'E3_structural_convergence',
      case
        when v_feeders_hot >= 7 then 90
        when v_feeders_hot >= 5 then 65
        else 42
      end,
      'Structural convergence',
      format('%s feeders entered hot zone in the same %s cycle.', v_feeders_hot, upper(p_checkpoint)),
      v_day,
      jsonb_build_object('feed_id', p_feed_id, 'checkpoint', p_checkpoint, 'day', v_day,
                         'feeders_hot', v_feeders_hot, 'feeders_total', v_feeders_total)
    );
    v_count := v_count + 1;
  end if;

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

        if v_gap >= 5 then
          perform public.fn_upsert_fire_alert(
            p_feed_id, v_comp.feeder_id, null, p_checkpoint,
            'competitor', 'D', 'D1_anchor_displacement',
            case when v_gap >= 20 then 90 when v_gap >= 12 then 65 else 42 end,
            'Anchor displacement',
            format('@%s median is %s points ahead of @%s at %s.', v_comp.handle, round(v_gap)::int, v_anchor_handle, upper(p_checkpoint)),
            v_day,
            jsonb_build_object('anchor_handle', v_anchor_handle, 'competitor_handle', v_comp.handle,
                               'anchor_median', round(v_anchor_median)::int, 'competitor_median', round(v_comp_median)::int,
                               'gap', round(v_gap)::int)
          );
          v_count := v_count + 1;
        end if;

        if abs(v_gap) <= 15 then
          perform public.fn_upsert_fire_alert(
            p_feed_id, v_comp.feeder_id, null, p_checkpoint,
            'competitor', 'D', 'D2_competitive_gap_closing',
            case when abs(v_gap) <= 2 then 90 when abs(v_gap) <= 8 then 65 else 42 end,
            'Gap closing',
            format('@%s is now within %s positions of @%s at %s.', v_comp.handle, abs(round(v_gap)::int), v_anchor_handle, upper(p_checkpoint)),
            v_day,
            jsonb_build_object('anchor_handle', v_anchor_handle, 'competitor_handle', v_comp.handle,
                               'gap', round(v_gap)::int)
          );
          v_count := v_count + 1;
        end if;

        if v_comp_median <= 20 then
          perform public.fn_upsert_fire_alert(
            p_feed_id, v_comp.feeder_id, null, p_checkpoint,
            'competitor', 'D', 'D3_competitor_regime_break',
            case when v_comp_median <= 8 then 90 when v_comp_median <= 14 then 65 else 42 end,
            'Competitor regime break',
            format('@%s is operating in a new structural tier at %s.', v_comp.handle, upper(p_checkpoint)),
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

-- Backfill captured business date for legacy rows once.
update public.post_metrics pm
set captured_business_date_ist = coalesce(
  pm.captured_business_date_ist,
  (coalesce(pm.computed_at, now()) at time zone 'Asia/Kolkata')::date
)
where pm.captured_business_date_ist is null;

update public.post_metrics
set d1_source = coalesce(d1_source, case when checkpoint = 'd1' then 'on_time' when checkpoint = 'd2b' then 'from_d2b' else null end)
where d1_source is null;

commit;
