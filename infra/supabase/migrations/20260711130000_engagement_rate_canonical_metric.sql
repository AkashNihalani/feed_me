begin;

with metric_source as (
  select
    pm.post_key,
    pm.checkpoint,
    pm.likes,
    pm.comments,
    coalesce(
      pm.business_date_ist,
      (coalesce(p.posted_at, pm.computed_at) at time zone 'Asia/Kolkata')::date,
      (now() at time zone 'Asia/Kolkata')::date
    ) as metric_day,
    pm.followers_at_metric,
    fd.follower_count
  from public.post_metrics pm
  join public.posts p on p.post_key = pm.post_key
  join public.feeders fd on fd.id = p.feeder_id
), resolved as (
  select
    ms.*,
    coalesce(ms.followers_at_metric, snapshot.follower_count, ms.follower_count) as resolved_followers
  from metric_source ms
  left join lateral (
    select s.follower_count
    from public.feeder_follower_snapshots s
    join public.posts p on p.post_key = ms.post_key
    where s.feeder_id = p.feeder_id
      and s.follower_count > 0
    order by
      case when s.snapshot_date_ist <= ms.metric_day then 0 else 1 end,
      abs(s.snapshot_date_ist - ms.metric_day),
      s.snapshot_date_ist desc
    limit 1
  ) snapshot on true
)
update public.post_metrics pm
set
  followers_at_metric = case when resolved.resolved_followers > 0 then resolved.resolved_followers else null end,
  engagement_count = case
    when resolved.likes is not null or resolved.comments is not null
      then greatest(0, coalesce(resolved.likes, 0) + coalesce(resolved.comments, 0))
    else null
  end,
  engagement_rate = case
    when resolved.resolved_followers > 0
      and (resolved.likes is not null or resolved.comments is not null)
      then round((greatest(0, coalesce(resolved.likes, 0) + coalesce(resolved.comments, 0))::numeric / resolved.resolved_followers), 8)
    else null
  end
from resolved
where pm.post_key = resolved.post_key
  and pm.checkpoint = resolved.checkpoint;

commit;
