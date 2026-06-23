drop function if exists public.fn_feed_export_latest(bigint, text, date, date);

create function public.fn_feed_export_latest(
  p_feed_id bigint,
  p_handle text default null,
  p_start_date date default null,
  p_end_date date default null
)
returns table (
  post_key text,
  feeder_handle text,
  media_type text,
  posted_at timestamptz,
  caption text,
  thumbnail_url text,
  post_url text,
  likes bigint,
  comments bigint,
  views bigint,
  percentile_performance numeric,
  views_percentile int,
  likes_percentile int,
  comments_percentile int,
  delta_from_d1 int
)
language sql
security definer
set search_path = public
as $$
  with feeder_scope as (
    select
      fd.id,
      fd.handle
    from public.feeders fd
    where fd.feed_id = p_feed_id
      and fd.status = 'active'
      and (
        p_handle is null
        or lower(fd.handle) = lower(p_handle)
      )
  ),
  post_scope as (
    select
      fs.handle as feeder_handle,
      p.post_key,
      lower(coalesce(p.media_type, 'unknown')) as media_type,
      p.posted_at,
      p.caption,
      p.thumbnail_url,
      p.post_url
    from public.posts p
    join feeder_scope fs on fs.id = p.feeder_id
    where p.posted_at is not null
      and (
        p_start_date is null
        or (p.posted_at at time zone 'Asia/Kolkata')::date >= p_start_date
      )
      and (
        p_end_date is null
        or (p.posted_at at time zone 'Asia/Kolkata')::date <= p_end_date
      )
  ),
  latest_metrics as (
    select distinct on (pm.post_key)
      pm.post_key,
      pm.likes,
      pm.comments,
      pm.views,
      coalesce(pm.percentile_performance_exact, pm.percentile_performance::numeric) as percentile_performance,
      pm.views_percentile,
      pm.likes_percentile,
      pm.comments_percentile,
      pm.delta_from_d1,
      pm.computed_at,
      pm.checkpoint
    from public.post_metrics pm
    join post_scope ps on ps.post_key = pm.post_key
    order by
      pm.post_key,
      pm.computed_at desc,
      case pm.checkpoint
        when 'd21' then 4
        when 'd7' then 3
        when 'd3' then 2
        when 'd1' then 1
        else 0
      end desc
  )
  select
    ps.post_key,
    ps.feeder_handle,
    ps.media_type,
    ps.posted_at,
    ps.caption,
    ps.thumbnail_url,
    ps.post_url,
    lm.likes,
    lm.comments,
    lm.views,
    lm.percentile_performance,
    lm.views_percentile,
    lm.likes_percentile,
    lm.comments_percentile,
    lm.delta_from_d1
  from post_scope ps
  left join latest_metrics lm on lm.post_key = ps.post_key
  order by ps.posted_at desc nulls last, ps.post_url;
$$;
