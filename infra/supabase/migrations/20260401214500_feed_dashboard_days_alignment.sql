begin;

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
        ps.media_type,
        count(*)::int as post_count,
        round((count(*)::numeric / nullif(sum(count(*)) over (), 0)), 4) as share
      from post_scope ps
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
        ps.post_key,
        greatest(0, (b.today_ist - ps.posted_date_ist))::int as days_ago,
        lm.percentile_performance,
        lm.views,
        ps.handle,
        (ps.posted_at at time zone 'Asia/Kolkata') as posted_at_ist,
        ps.posted_at
      from post_scope ps
      cross join bounds b
      left join latest_metrics lm on lm.post_key = ps.post_key
      order by ps.posted_at desc
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
