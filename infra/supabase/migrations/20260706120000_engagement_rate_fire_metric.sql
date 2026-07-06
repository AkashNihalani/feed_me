begin;

alter table if exists public.post_metrics
  add column if not exists engagement_count bigint,
  add column if not exists followers_at_metric bigint,
  add column if not exists engagement_rate numeric(14,8),
  add column if not exists engagement_rate_baseline numeric(14,8),
  add column if not exists engagement_rate_multiple numeric(12,4),
  add column if not exists engagement_rate_percentile int;

with metric_source as (
  select
    pm.post_key,
    pm.checkpoint,
    pm.followers_at_metric,
    fd.follower_count as feeder_follower_count,
    coalesce(
      pm.business_date_ist,
      (coalesce(p.posted_at, pm.computed_at) at time zone 'Asia/Kolkata')::date,
      (now() at time zone 'Asia/Kolkata')::date
    ) as metric_day,
    case
      when pm.likes is not null or pm.comments is not null
        then greatest(0, coalesce(pm.likes, 0) + coalesce(pm.comments, 0))
      else null
    end as engagement_count
  from public.post_metrics pm
  join public.posts p on p.post_key = pm.post_key
  join public.feeders fd on fd.id = p.feeder_id
), backfill as (
  select
    ms.post_key,
    ms.checkpoint,
    ms.engagement_count,
    coalesce(ms.followers_at_metric, snap.follower_count, ms.feeder_follower_count) as followers_at_metric
  from metric_source ms
  left join lateral (
    select s.follower_count
    from public.feeder_follower_snapshots s
    join public.posts p on p.post_key = ms.post_key
    where s.feeder_id = p.feeder_id
    order by
      case when s.snapshot_date_ist <= ms.metric_day then 0 else 1 end,
      abs(s.snapshot_date_ist - ms.metric_day),
      s.snapshot_date_ist desc
    limit 1
  ) snap on true
)
update public.post_metrics pm
set
  followers_at_metric = case
    when backfill.followers_at_metric is not null then greatest(0, backfill.followers_at_metric)
    else pm.followers_at_metric
  end,
  engagement_count = coalesce(backfill.engagement_count, pm.engagement_count),
  engagement_rate = case
    when backfill.engagement_count is not null and coalesce(backfill.followers_at_metric, 0) > 0
      then round(backfill.engagement_count::numeric / backfill.followers_at_metric::numeric, 8)
    else pm.engagement_rate
  end
from backfill
where pm.post_key = backfill.post_key
  and pm.checkpoint = backfill.checkpoint
  and (
    pm.followers_at_metric is null
    or pm.engagement_count is null
    or pm.engagement_rate is null
  );

alter table if exists public.post_metrics
  drop constraint if exists post_metrics_ranking_metric_check;

alter table if exists public.post_metrics
  add constraint post_metrics_ranking_metric_check
  check (
    ranking_metric is null
    or lower(ranking_metric) in ('views', 'likes', 'comments', 'engagement_rate')
  );

commit;
