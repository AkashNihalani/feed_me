select (select count(*) from public.feeds) as feeds,
       (select count(*) from public.feeders) as feeders,
       (select count(*) from public.posts) as posts,
       (select count(*) from public.post_metrics) as post_metrics,
       (select count(*) from public.run_jobs) as run_jobs,
       (select count(*) from public.checkpoint_jobs) as checkpoint_jobs,
       (select count(*) from public.fire_alerts) as fire_alerts,
       (select count(*) from public.post_media_assets) as post_media_assets,
       (select count(*) from public.web_push_jobs) as web_push_jobs;

select status, count(*) from public.run_jobs group by status order by status;
select job_type, status, count(*) from public.run_jobs group by job_type, status order by job_type, status;
select status, checkpoint, count(*) from public.checkpoint_jobs group by status, checkpoint order by status, checkpoint;

select count(*) as due_run_jobs from public.run_jobs where status in ('pending','retry') and next_run_at <= now();
select count(*) as claimable_checkpoint_jobs
from public.checkpoint_jobs
where status in ('pending','retry')
  and public.fn_checkpoint_job_claimable(attempt, next_run_at, now());

select count(*) as staged_current_hour_checkpoint_jobs
from public.checkpoint_jobs
where status in ('pending','retry')
  and coalesce(attempt, 0) = 0
  and next_run_at >= date_trunc('hour', now())
  and next_run_at < date_trunc('hour', now()) + interval '1 hour';

select checkpoint, count(*)
from public.post_metrics
group by checkpoint
order by checkpoint;
