begin;

-- Fix D1 timing hydration from post timestamps and make hour cohorts resilient enough to populate Fire cards.

create or replace function public.tg_compute_post_metrics()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_media_type text;
  v_feeder_id bigint;
  v_posted_at timestamptz;
  v_feed_id bigint;
  v_follower_count bigint;
  v_mv numeric;

  v_pool_size int;
  v_rank int;
  v_pct int;

  v_feed_pool_size int;
  v_feed_rank int;
  v_feed_pct int;

  v_likes_pool_size int;
  v_likes_rank int;
  v_likes_pct int;

  v_comments_pool_size int;
  v_comments_rank int;
  v_comments_pct int;

  v_views_pool_size int;
  v_views_rank int;
  v_views_pct int;

  v_d1_pct int;

  v_views_rank_feed int;
  v_likes_rank_feed int;
  v_comments_rank_feed int;

  v_best_last_n_views int;
  v_best_last_n_likes int;
  v_best_last_n_comments int;

  v_baseline_views bigint;
  v_baseline_likes bigint;
  v_baseline_comments bigint;

  v_hour_baseline_views bigint;
  v_hour_baseline_likes bigint;
  v_hour_baseline_comments bigint;

  v_hour_rank int;
  v_hour_pool int;

  v_rank_all_time int;
begin
  select p.media_type, p.feeder_id, p.posted_at
  into v_media_type, v_feeder_id, v_posted_at
  from public.posts p
  where p.post_key = new.post_key;

  if v_feeder_id is not null then
    select fd.feed_id, fd.follower_count
    into v_feed_id, v_follower_count
    from public.feeders fd
    where fd.id = v_feeder_id;
  end if;

  if v_posted_at is not null then
    new.business_date_ist := coalesce(new.business_date_ist, (v_posted_at at time zone 'Asia/Kolkata')::date);
    new.post_hour_ist := coalesce(new.post_hour_ist, extract(hour from (v_posted_at at time zone 'Asia/Kolkata'))::smallint);
    new.post_dow_ist := coalesce(new.post_dow_ist, extract(dow from (v_posted_at at time zone 'Asia/Kolkata'))::smallint);
  end if;

  v_mv := public.fn_metric_value(coalesce(v_media_type, 'image'), new.views, new.likes, new.comments);

  v_pct := null;
  if v_mv is not null and v_feeder_id is not null then
    select count(*) + 1 into v_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.post_key <> new.post_key
      and pm.metric_value is not null
      and pm.metric_value > v_mv;

    select count(*) + 1 into v_pool_size
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
  v_feed_rank := null;
  if v_mv is not null and v_feed_id is not null then
    select count(*) + 1 into v_feed_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    join public.feeders f2 on f2.id = p2.feeder_id
    where f2.feed_id = v_feed_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.post_key <> new.post_key
      and pm.metric_value is not null
      and pm.metric_value > v_mv;

    select count(*) + 1 into v_feed_pool_size
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

  v_likes_pct := null;
  if new.likes is not null and v_feeder_id is not null then
    select count(*) + 1 into v_likes_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.post_key <> new.post_key
      and pm.likes is not null
      and pm.likes > new.likes;

    select count(*) + 1 into v_likes_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.likes is not null;

    if v_likes_pool_size > 0 then
      v_likes_pct := greatest(1, least(100, round((v_likes_rank::numeric / v_likes_pool_size) * 100)));
    end if;
  end if;

  v_comments_pct := null;
  if new.comments is not null and v_feeder_id is not null then
    select count(*) + 1 into v_comments_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.post_key <> new.post_key
      and pm.comments is not null
      and pm.comments > new.comments;

    select count(*) + 1 into v_comments_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.comments is not null;

    if v_comments_pool_size > 0 then
      v_comments_pct := greatest(1, least(100, round((v_comments_rank::numeric / v_comments_pool_size) * 100)));
    end if;
  end if;

  v_views_pct := null;
  if new.views is not null and v_feeder_id is not null then
    select count(*) + 1 into v_views_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.post_key <> new.post_key
      and pm.views is not null
      and pm.views > new.views;

    select count(*) + 1 into v_views_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.views is not null;

    if v_views_pool_size > 0 then
      v_views_pct := greatest(1, least(100, round((v_views_rank::numeric / v_views_pool_size) * 100)));
    end if;
  end if;

  v_views_rank_feed := null;
  if new.views is not null and v_feed_id is not null then
    select count(*) + 1 into v_views_rank_feed
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    join public.feeders f2 on f2.id = p2.feeder_id
    where f2.feed_id = v_feed_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.post_key <> new.post_key
      and pm.views is not null
      and pm.views > new.views;
  end if;

  v_likes_rank_feed := null;
  if new.likes is not null and v_feed_id is not null then
    select count(*) + 1 into v_likes_rank_feed
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    join public.feeders f2 on f2.id = p2.feeder_id
    where f2.feed_id = v_feed_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.post_key <> new.post_key
      and pm.likes is not null
      and pm.likes > new.likes;
  end if;

  v_comments_rank_feed := null;
  if new.comments is not null and v_feed_id is not null then
    select count(*) + 1 into v_comments_rank_feed
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    join public.feeders f2 on f2.id = p2.feeder_id
    where f2.feed_id = v_feed_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.post_key <> new.post_key
      and pm.comments is not null
      and pm.comments > new.comments;
  end if;

  v_best_last_n_views := null;
  if new.views is not null and v_feeder_id is not null then
    select count(*) into v_best_last_n_views
    from (
      select pm.views
      from public.post_metrics pm
      join public.posts p2 on p2.post_key = pm.post_key
      where p2.feeder_id = v_feeder_id
        and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
        and pm.checkpoint = new.checkpoint
        and pm.post_key <> new.post_key
        and pm.views is not null
      order by pm.computed_at desc
      limit 50
    ) s
    where s.views < new.views;
  end if;

  v_best_last_n_likes := null;
  if new.likes is not null and v_feeder_id is not null then
    select count(*) into v_best_last_n_likes
    from (
      select pm.likes
      from public.post_metrics pm
      join public.posts p2 on p2.post_key = pm.post_key
      where p2.feeder_id = v_feeder_id
        and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
        and pm.checkpoint = new.checkpoint
        and pm.post_key <> new.post_key
        and pm.likes is not null
      order by pm.computed_at desc
      limit 50
    ) s
    where s.likes < new.likes;
  end if;

  v_best_last_n_comments := null;
  if new.comments is not null and v_feeder_id is not null then
    select count(*) into v_best_last_n_comments
    from (
      select pm.comments
      from public.post_metrics pm
      join public.posts p2 on p2.post_key = pm.post_key
      where p2.feeder_id = v_feeder_id
        and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
        and pm.checkpoint = new.checkpoint
        and pm.post_key <> new.post_key
        and pm.comments is not null
      order by pm.computed_at desc
      limit 50
    ) s
    where s.comments < new.comments;
  end if;

  -- Strict baselines from historical medians for the same feeder/media/checkpoint.
  select round(percentile_cont(0.5) within group (order by pm.views))::bigint
  into v_baseline_views
  from public.post_metrics pm
  join public.posts p2 on p2.post_key = pm.post_key
  where p2.feeder_id = v_feeder_id
    and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
    and pm.checkpoint = new.checkpoint
    and pm.post_key <> new.post_key
    and pm.views is not null;

  select round(percentile_cont(0.5) within group (order by pm.likes))::bigint
  into v_baseline_likes
  from public.post_metrics pm
  join public.posts p2 on p2.post_key = pm.post_key
  where p2.feeder_id = v_feeder_id
    and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
    and pm.checkpoint = new.checkpoint
    and pm.post_key <> new.post_key
    and pm.likes is not null;

  select round(percentile_cont(0.5) within group (order by pm.comments))::bigint
  into v_baseline_comments
  from public.post_metrics pm
  join public.posts p2 on p2.post_key = pm.post_key
  where p2.feeder_id = v_feeder_id
    and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
    and pm.checkpoint = new.checkpoint
    and pm.post_key <> new.post_key
    and pm.comments is not null;

  -- Strict timing metrics prefer same media/hour cohorts and fall back to same-hour across the feeder.
  v_hour_baseline_views := null;
  v_hour_baseline_likes := null;
  v_hour_baseline_comments := null;
  v_hour_rank := null;
  v_hour_pool := null;

  if new.post_hour_ist is not null and v_feeder_id is not null then
    select round(percentile_cont(0.5) within group (order by pm.views))::bigint
    into v_hour_baseline_views
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.post_hour_ist = new.post_hour_ist
      and pm.post_key <> new.post_key
      and pm.views is not null;

    if v_hour_baseline_views is null then
      select round(percentile_cont(0.5) within group (order by pm.views))::bigint
      into v_hour_baseline_views
      from public.post_metrics pm
      join public.posts p2 on p2.post_key = pm.post_key
      where p2.feeder_id = v_feeder_id
        and pm.checkpoint = new.checkpoint
        and pm.post_hour_ist = new.post_hour_ist
        and pm.post_key <> new.post_key
        and pm.views is not null;
    end if;

    select round(percentile_cont(0.5) within group (order by pm.likes))::bigint
    into v_hour_baseline_likes
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.post_hour_ist = new.post_hour_ist
      and pm.post_key <> new.post_key
      and pm.likes is not null;

    if v_hour_baseline_likes is null then
      select round(percentile_cont(0.5) within group (order by pm.likes))::bigint
      into v_hour_baseline_likes
      from public.post_metrics pm
      join public.posts p2 on p2.post_key = pm.post_key
      where p2.feeder_id = v_feeder_id
        and pm.checkpoint = new.checkpoint
        and pm.post_hour_ist = new.post_hour_ist
        and pm.post_key <> new.post_key
        and pm.likes is not null;
    end if;

    select round(percentile_cont(0.5) within group (order by pm.comments))::bigint
    into v_hour_baseline_comments
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.post_hour_ist = new.post_hour_ist
      and pm.post_key <> new.post_key
      and pm.comments is not null;

    if v_hour_baseline_comments is null then
      select round(percentile_cont(0.5) within group (order by pm.comments))::bigint
      into v_hour_baseline_comments
      from public.post_metrics pm
      join public.posts p2 on p2.post_key = pm.post_key
      where p2.feeder_id = v_feeder_id
        and pm.checkpoint = new.checkpoint
        and pm.post_hour_ist = new.post_hour_ist
        and pm.post_key <> new.post_key
        and pm.comments is not null;
    end if;

    if v_mv is not null then
      select count(*)
      into v_hour_pool
      from public.post_metrics pm
      join public.posts p2 on p2.post_key = pm.post_key
      where p2.feeder_id = v_feeder_id
        and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
        and pm.checkpoint = new.checkpoint
        and pm.post_hour_ist = new.post_hour_ist
        and pm.post_key <> new.post_key
        and pm.metric_value is not null;

      if coalesce(v_hour_pool, 0) = 0 then
        select count(*)
        into v_hour_pool
        from public.post_metrics pm
        join public.posts p2 on p2.post_key = pm.post_key
        where p2.feeder_id = v_feeder_id
          and pm.checkpoint = new.checkpoint
          and pm.post_hour_ist = new.post_hour_ist
          and pm.post_key <> new.post_key
          and pm.metric_value is not null;

        select count(*) + 1
        into v_hour_rank
        from public.post_metrics pm
        join public.posts p2 on p2.post_key = pm.post_key
        where p2.feeder_id = v_feeder_id
          and pm.checkpoint = new.checkpoint
          and pm.post_hour_ist = new.post_hour_ist
          and pm.post_key <> new.post_key
          and pm.metric_value is not null
          and pm.metric_value > v_mv;
      else
        select count(*) + 1
        into v_hour_rank
        from public.post_metrics pm
        join public.posts p2 on p2.post_key = pm.post_key
        where p2.feeder_id = v_feeder_id
          and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
          and pm.checkpoint = new.checkpoint
          and pm.post_hour_ist = new.post_hour_ist
          and pm.post_key <> new.post_key
          and pm.metric_value is not null
          and pm.metric_value > v_mv;
      end if;

      if coalesce(v_hour_pool, 0) > 0 then
        new.hour_percentile_feeder := greatest(1, least(100, round((v_hour_rank::numeric / (v_hour_pool + 1)::numeric) * 100)));
      else
        new.hour_percentile_feeder := null;
      end if;
    else
      new.hour_percentile_feeder := null;
    end if;
  else
    new.hour_percentile_feeder := null;
  end if;

  if new.views is not null and v_hour_baseline_views is not null and v_hour_baseline_views > 0 then
    new.hour_multiple_views := round(new.views::numeric / v_hour_baseline_views::numeric, 4);
  else
    new.hour_multiple_views := null;
  end if;

  if new.likes is not null and v_hour_baseline_likes is not null and v_hour_baseline_likes > 0 then
    new.hour_multiple_likes := round(new.likes::numeric / v_hour_baseline_likes::numeric, 4);
  else
    new.hour_multiple_likes := null;
  end if;

  if new.comments is not null and v_hour_baseline_comments is not null and v_hour_baseline_comments > 0 then
    new.hour_multiple_comments := round(new.comments::numeric / v_hour_baseline_comments::numeric, 4);
  else
    new.hour_multiple_comments := null;
  end if;

  select pm.percentile_performance
  into v_d1_pct
  from public.post_metrics pm
  where pm.post_key = new.post_key
    and pm.checkpoint = 'd1'
  order by pm.computed_at desc
  limit 1;

  v_rank_all_time := null;
  if v_mv is not null and v_feeder_id is not null then
    select count(*) + 1 into v_rank_all_time
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.post_key <> new.post_key
      and pm.metric_value is not null
      and pm.metric_value > v_mv;
  end if;

  new.metric_value := v_mv;
  new.percentile_performance := v_pct;
  new.percentile_tag := public.fn_percentile_tag(v_pct);
  new.feed_percentile := v_feed_pct;
  new.rank_feed := v_feed_rank;
  new.rank_all_time := v_rank_all_time;
  new.likes_percentile := v_likes_pct;
  new.comments_percentile := v_comments_pct;
  new.views_percentile := v_views_pct;
  new.baseline_views := v_baseline_views;
  new.baseline_likes := v_baseline_likes;
  new.baseline_comments := v_baseline_comments;
  new.views_multiple := case when new.views is not null and v_baseline_views is not null and v_baseline_views > 0 then round(new.views::numeric / v_baseline_views::numeric, 4) else null end;
  new.likes_multiple := case when new.likes is not null and v_baseline_likes is not null and v_baseline_likes > 0 then round(new.likes::numeric / v_baseline_likes::numeric, 4) else null end;
  new.comments_multiple := case when new.comments is not null and v_baseline_comments is not null and v_baseline_comments > 0 then round(new.comments::numeric / v_baseline_comments::numeric, 4) else null end;
  new.views_rank_feed := v_views_rank_feed;
  new.likes_rank_feed := v_likes_rank_feed;
  new.comments_rank_feed := v_comments_rank_feed;
  new.best_in_last_n_views := v_best_last_n_views;
  new.best_in_last_n_likes := v_best_last_n_likes;
  new.best_in_last_n_comments := v_best_last_n_comments;
  new.is_best_hour_window := case
    when new.hour_percentile_feeder is null then null
    when new.hour_percentile_feeder <= 35 then true
    else false
  end;
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

  return new;
end;
$$;


-- Recompute post_metrics-derived fields with corrected timing logic.
update public.post_metrics
set likes = likes
where computed_at >= now() - interval '60 days';

-- Rebuild recent canonical slot_v3 alerts so timing payloads reflect recomputed post_metrics.
delete from public.fire_alerts
where signal_code = 'slot_v3'
  and context = 'own'
  and created_at >= now() - interval '21 days';

do $$
declare
  d date;
  r record;
  cp text;
  v_from date := ((now() at time zone 'Asia/Kolkata')::date - 21);
  v_to date := ((now() at time zone 'Asia/Kolkata')::date - 1);
begin
  for d in select gs::date from generate_series(v_from, v_to, interval '1 day') gs loop
    for r in select id from public.feeders where status = 'active' loop
      foreach cp in array array['d1','d3','d7','d21'] loop
        perform public.fn_process_checkpoint(r.id, cp, d);
      end loop;
    end loop;
  end loop;
end $$;

commit;
