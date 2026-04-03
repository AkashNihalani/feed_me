begin;

create or replace function public.claim_checkpoint_jobs(p_limit int default 100)
returns setof public.checkpoint_jobs
language sql
security definer
set search_path = public
as $$
  with ctx as (
    select
      public.fn_checkpoint_hourly_cutoff(now()) as cutoff_utc,
      (now() at time zone 'Asia/Kolkata')::date as today_ist,
      ((now() at time zone 'Asia/Kolkata')::date - 30) as repair_floor_ist
  ), picked as (
    select cj.id
    from public.checkpoint_jobs cj
    join public.posts p on p.post_key = cj.post_key
    cross join ctx
    where cj.status in ('pending', 'retry')
      and public.fn_checkpoint_job_claimable(cj.attempt, cj.next_run_at, now())
      and (
        coalesce(
          public.fn_checkpoint_job_business_day(p.posted_at, cj.checkpoint, 'Asia/Kolkata'),
          (coalesce(cj.next_run_at, now()) at time zone 'Asia/Kolkata')::date
        ) >= ctx.today_ist
        or (
          lower(coalesce(cj.last_error, '')) like 'recovered missing checkpoint batch:%'
          and (p.posted_at at time zone 'Asia/Kolkata')::date >= ctx.repair_floor_ist
        )
      )
    order by
      case
        when coalesce(
          public.fn_checkpoint_job_business_day(p.posted_at, cj.checkpoint, 'Asia/Kolkata'),
          (coalesce(cj.next_run_at, now()) at time zone 'Asia/Kolkata')::date
        ) >= ctx.today_ist then 0
        else 1
      end,
      case
        when coalesce(
          public.fn_checkpoint_job_business_day(p.posted_at, cj.checkpoint, 'Asia/Kolkata'),
          (coalesce(cj.next_run_at, now()) at time zone 'Asia/Kolkata')::date
        ) >= ctx.today_ist
         and cj.next_run_at >= (ctx.cutoff_utc - interval '1 hour') then 0
        else 1
      end,
      cj.next_run_at asc,
      cj.attempt asc,
      cj.id asc
    for update skip locked
    limit greatest(1, p_limit)
  ), updated as (
    update public.checkpoint_jobs cj
    set status = 'running',
        updated_at = now()
    from picked
    where cj.id = picked.id
    returning cj.*
  )
  select * from updated;
$$;

update public.checkpoint_jobs cj
set status = 'retry',
    attempt = 0,
    next_run_at = now(),
    last_error = 'Recovered missing checkpoint batch: requeued for retry',
    resurrection_count = coalesce(cj.resurrection_count, 0) + 1,
    updated_at = now()
from public.posts p
where p.post_key = cj.post_key
  and cj.status = 'skipped'
  and lower(coalesce(cj.last_error, '')) like 'hard-skip:post missing in checkpoint batch%'
  and lower(coalesce(cj.checkpoint, '')) in ('d1', 'd3', 'd7', 'd21')
  and (p.posted_at at time zone 'Asia/Kolkata')::date >= ((now() at time zone 'Asia/Kolkata')::date - 30)
  and not exists (
    select 1
    from public.post_metrics pm
    where pm.post_key = cj.post_key
      and lower(pm.checkpoint) = lower(cj.checkpoint)
  );

create or replace function public.fn_feed_dashboard(
  p_feed_id bigint,
  p_weeks int,
  p_handle text default null
)
returns json
language sql
security definer
set search_path = public
as $$
  with params as (
    select
      case
        when p_weeks = 4 then 28
        when p_weeks = 12 then 84
        when p_weeks = 26 then 182
        when p_weeks = 52 then 364
        when p_weeks in (7, 30, 60, 90) then p_weeks
        else 30
      end as window_days,
      nullif(lower(trim(coalesce(p_handle, ''))), '') as handle,
      (timezone('Asia/Kolkata', now()))::date as today_ist
  ),
  bounds as (
    select
      window_days,
      handle,
      today_ist,
      (today_ist - (window_days - 1))::date as window_start_ist,
      (today_ist + 1)::date as window_end_exclusive_ist
    from params
  ),
  feeder_scope as (
    select
      fd.id,
      fd.handle,
      fd.role
    from public.feeders fd
    cross join bounds b
    where fd.feed_id = p_feed_id
      and fd.status = 'active'
      and (b.handle is null or lower(fd.handle) = b.handle)
  ),
  feeder_meta as (
    select
      coalesce(array_agg(fs.id order by fs.id), array[]::bigint[]) as feeder_ids,
      max(fs.id) filter (where fs.role = 'anchor') as anchor_feeder_id
    from feeder_scope fs
  ),
  post_scope as (
    select
      p.post_key,
      p.feeder_id,
      fs.handle,
      lower(coalesce(p.media_type, 'unknown')) as media_type,
      p.posted_at,
      (p.posted_at at time zone 'Asia/Kolkata')::date as posted_date_ist
    from public.posts p
    join feeder_scope fs on fs.id = p.feeder_id
    cross join bounds b
    where p.posted_at is not null
      and (p.posted_at at time zone 'Asia/Kolkata')::date >= b.window_start_ist
      and (p.posted_at at time zone 'Asia/Kolkata')::date < b.window_end_exclusive_ist
  ),
  latest_metrics as (
    select distinct on (pm.post_key)
      pm.post_key,
      pm.checkpoint,
      pm.percentile_performance,
      pm.views_percentile,
      pm.likes_percentile,
      pm.comments_percentile,
      pm.views,
      pm.computed_at
    from public.post_metrics pm
    join post_scope ps on ps.post_key = pm.post_key
    where pm.checkpoint in ('d1', 'd3', 'd7', 'd21')
    order by
      pm.post_key,
      case pm.checkpoint
        when 'd21' then 4
        when 'd7' then 3
        when 'd3' then 2
        when 'd1' then 1
        else 0
      end desc,
      pm.computed_at desc
  ),
  tracked_post_scope as (
    select
      ps.post_key,
      ps.feeder_id,
      ps.handle,
      ps.media_type,
      ps.posted_at,
      ps.posted_date_ist,
      lm.checkpoint,
      lm.percentile_performance,
      lm.views_percentile,
      lm.likes_percentile,
      lm.comments_percentile,
      lm.views,
      lm.computed_at
    from post_scope ps
    join latest_metrics lm on lm.post_key = ps.post_key
  ),
  ascent_series as (
    select coalesce(
      json_agg(
        json_build_object(
          'snapshot_date_ist', t.snapshot_date_ist,
          'follower_count', t.follower_count
        )
        order by t.snapshot_date_ist
      ),
      '[]'::json
    ) as payload
    from (
      select
        s.snapshot_date_ist,
        sum(s.follower_count)::bigint as follower_count
      from public.feeder_follower_snapshots s
      cross join feeder_meta fm
      cross join bounds b
      where s.snapshot_date_ist >= b.window_start_ist
        and s.snapshot_date_ist < b.window_end_exclusive_ist
        and (
          (fm.anchor_feeder_id is not null and s.feeder_id = fm.anchor_feeder_id)
          or (fm.anchor_feeder_id is null and s.feeder_id = any(fm.feeder_ids))
        )
      group by s.snapshot_date_ist
      order by s.snapshot_date_ist
    ) t
  ),
  frequency_series as (
    select coalesce(
      json_agg(
        json_build_object(
          'week_start_ist', t.week_start_ist,
          'post_count', t.post_count,
          'avg_percentile_performance', t.avg_percentile_performance,
          'avg_views_percentile', t.avg_views_percentile,
          'avg_likes_percentile', t.avg_likes_percentile,
          'avg_comments_percentile', t.avg_comments_percentile
        )
        order by t.week_start_ist
      ),
      '[]'::json
    ) as payload
    from (
      select
        date_trunc('week', (ps.posted_at at time zone 'Asia/Kolkata'))::date as week_start_ist,
        count(*)::int as post_count,
        round(avg(lm.percentile_performance)::numeric, 2) as avg_percentile_performance,
        round(avg(lm.views_percentile)::numeric, 2) as avg_views_percentile,
        round(avg(lm.likes_percentile)::numeric, 2) as avg_likes_percentile,
        round(avg(lm.comments_percentile)::numeric, 2) as avg_comments_percentile
      from post_scope ps
      left join latest_metrics lm on lm.post_key = ps.post_key
      group by 1
      order by 1
    ) t
  ),
  heatmap_daily as (
    select coalesce(
      json_agg(
        json_build_object(
          'day_ist', t.day_ist,
          'post_count', t.post_count
        )
        order by t.day_ist
      ),
      '[]'::json
    ) as payload
    from (
      select
        ps.posted_date_ist as day_ist,
        count(*)::int as post_count
      from post_scope ps
      group by 1
      order by 1
    ) t
  ),
  killzone_hours as (
    select coalesce(
      json_agg(
        json_build_object(
          'hour_ist', t.hour_ist,
          'post_count', t.post_count
        )
        order by t.hour_ist
      ),
      '[]'::json
    ) as payload
    from (
      select
        extract(hour from (ps.posted_at at time zone 'Asia/Kolkata'))::int as hour_ist,
        count(*)::int as post_count
      from post_scope ps
      group by 1
      order by 1
    ) t
  ),
  apex_mix as (
    select coalesce(
      json_agg(
        json_build_object(
          'media_type', t.media_type,
          'post_count', t.post_count,
          'share', t.share
        )
        order by t.post_count desc, t.media_type
      ),
      '[]'::json
    ) as payload
    from (
      select
        tps.media_type,
        count(*)::int as post_count,
        round((count(*)::numeric / nullif(sum(count(*)) over (), 0)), 4) as share
      from tracked_post_scope tps
      group by 1
    ) t
  ),
  scatter_points as (
    select coalesce(
      json_agg(
        json_build_object(
          'post_key', t.post_key,
          'days_ago', t.days_ago,
          'percentile_performance', t.percentile_performance,
          'views', t.views,
          'handle', t.handle,
          'posted_at_ist', t.posted_at_ist
        )
        order by t.posted_at desc
      ),
      '[]'::json
    ) as payload
    from (
      select
        tps.post_key,
        greatest(0, (b.today_ist - tps.posted_date_ist))::int as days_ago,
        tps.percentile_performance,
        tps.views,
        tps.handle,
        (tps.posted_at at time zone 'Asia/Kolkata') as posted_at_ist,
        tps.posted_at
      from tracked_post_scope tps
      cross join bounds b
      order by tps.posted_at desc
      limit 500
    ) t
  ),
  summary as (
    select json_build_object(
      'window_days', max(b.window_days),
      'window_start_ist', max(b.window_start_ist),
      'window_end_ist', max(b.window_end_exclusive_ist - 1),
      'post_count', count(ps.post_key),
      'posts_with_metrics', count(lm.post_key),
      'avg_percentile_performance', round(avg(lm.percentile_performance)::numeric, 2),
      'avg_views_percentile', round(avg(lm.views_percentile)::numeric, 2),
      'avg_likes_percentile', round(avg(lm.likes_percentile)::numeric, 2),
      'avg_comments_percentile', round(avg(lm.comments_percentile)::numeric, 2)
    ) as payload
    from bounds b
    left join post_scope ps on true
    left join latest_metrics lm on lm.post_key = ps.post_key
  )
  select json_build_object(
    'ascent_series', (select payload from ascent_series),
    'frequency_series', (select payload from frequency_series),
    'heatmap_daily', (select payload from heatmap_daily),
    'killzone_hours', (select payload from killzone_hours),
    'apex_mix', (select payload from apex_mix),
    'scatter_points', (select payload from scatter_points),
    'summary', (select payload from summary)
  );
$$;

commit;
