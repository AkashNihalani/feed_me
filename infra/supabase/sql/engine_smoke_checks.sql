select (select count(*) from public.feeds) as feeds,
       (select count(*) from public.feeders) as feeders,
       (select count(*) from public.posts) as posts,
       (select count(*) from public.post_metrics) as post_metrics,
       (select count(*) from public.run_jobs) as run_jobs,
       (select count(*) from public.checkpoint_jobs) as checkpoint_jobs,
       (select count(*) from public.fire_alerts) as fire_alerts;

select status, count(*) from public.run_jobs group by status order by status;
select status, checkpoint, count(*) from public.checkpoint_jobs group by status, checkpoint order by status, checkpoint;

select count(*) as due_run_jobs from public.run_jobs where status in ('pending','retry') and next_run_at <= now();
select count(*) as due_checkpoint_jobs from public.checkpoint_jobs where status in ('pending','retry') and next_run_at <= now();

select checkpoint, percentile_tag, count(*)
from public.post_metrics
group by checkpoint, percentile_tag
order by checkpoint, percentile_tag;
-- includes d2 buffer metrics and official d1/d3/d7/d21 checkpoints
