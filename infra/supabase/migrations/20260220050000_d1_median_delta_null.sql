-- Make median_delta NULL for D1 baselines (delta is only meaningful post-D1)

begin;

create or replace function public.fn_refresh_feeder_baselines(
  p_feeder_id bigint,
  p_checkpoint text
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
begin
  with base as (
    select
      p.media_type,
      pm.post_key,
      pm.percentile_performance,
      pm.likes,
      pm.comments,
      pm.views,
      case
        when pm.checkpoint = 'd1' then null
        else coalesce(
          pm.percentile_delta,
          (
            select d1.percentile_performance - pm.percentile_performance
            from public.post_metrics d1
            where d1.post_key = pm.post_key
              and d1.checkpoint = 'd1'
            order by d1.computed_at desc
            limit 1
          )
        )
      end as resolved_delta,
      p.posted_at
    from public.post_metrics pm
    join public.posts p on p.post_key = pm.post_key
    where p.feeder_id = p_feeder_id
      and pm.checkpoint = p_checkpoint
      and pm.percentile_performance is not null
      and p.posted_at >= now() - interval '90 days'
  ),
  stats as (
    select
      media_type,
      percentile_cont(0.5) within group (order by percentile_performance)::int as median_percentile,
      percentile_cont(0.25) within group (order by percentile_performance)::int as p25_percentile,
      percentile_cont(0.75) within group (order by percentile_performance)::int as p75_percentile,
      percentile_cont(0.5) within group (order by likes)::bigint as median_likes,
      percentile_cont(0.5) within group (order by comments)::bigint as median_comments,
      percentile_cont(0.5) within group (order by views)::bigint as median_views,
      percentile_cont(0.5) within group (order by resolved_delta)::int as median_delta,
      min(percentile_performance)::int as best_percentile_90d,
      count(*)::int as post_count_90d
    from base
    group by media_type
  ),
  best_post as (
    select distinct on (b.media_type)
      b.media_type,
      b.post_key as best_percentile_post,
      (current_date - (b.posted_at at time zone 'Asia/Kolkata')::date)::int as days_since_ceiling
    from base b
    order by b.media_type, b.percentile_performance asc, b.posted_at desc
  ),
  ranked as (
    select
      s.*,
      dense_rank() over(order by s.median_percentile asc nulls last) as format_rank,
      count(*) over() as format_count
    from stats s
  )
  insert into public.feeder_baselines (
    feeder_id,
    media_type,
    checkpoint,
    median_percentile,
    p25_percentile,
    p75_percentile,
    median_likes,
    median_comments,
    median_views,
    median_delta,
    format_rank,
    format_count,
    best_percentile_90d,
    best_percentile_post,
    days_since_ceiling,
    post_count_90d,
    updated_at
  )
  select
    p_feeder_id,
    r.media_type,
    p_checkpoint,
    r.median_percentile,
    r.p25_percentile,
    r.p75_percentile,
    r.median_likes,
    r.median_comments,
    r.median_views,
    r.median_delta,
    r.format_rank,
    r.format_count,
    r.best_percentile_90d,
    bp.best_percentile_post,
    bp.days_since_ceiling,
    r.post_count_90d,
    now()
  from ranked r
  left join best_post bp on bp.media_type = r.media_type
  on conflict (feeder_id, media_type, checkpoint)
  do update set
    median_percentile = excluded.median_percentile,
    p25_percentile = excluded.p25_percentile,
    p75_percentile = excluded.p75_percentile,
    median_likes = excluded.median_likes,
    median_comments = excluded.median_comments,
    median_views = excluded.median_views,
    median_delta = excluded.median_delta,
    format_rank = excluded.format_rank,
    format_count = excluded.format_count,
    best_percentile_90d = excluded.best_percentile_90d,
    best_percentile_post = excluded.best_percentile_post,
    days_since_ceiling = excluded.days_since_ceiling,
    post_count_90d = excluded.post_count_90d,
    updated_at = now();

  get diagnostics v_rows = row_count;
  return coalesce(v_rows, 0);
end;
$$;

-- Refresh baselines now so D1 median_delta becomes NULL and post-D1 remains computed.
do $$
declare
  r record;
  cp text;
begin
  for r in select id from public.feeders where status = 'active' loop
    foreach cp in array array['d1','d3','d7','d21'] loop
      perform public.fn_refresh_feeder_baselines(r.id, cp);
    end loop;
  end loop;
end $$;

commit;
