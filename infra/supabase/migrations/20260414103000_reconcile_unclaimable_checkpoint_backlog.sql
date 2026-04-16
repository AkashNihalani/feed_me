begin;

update public.checkpoint_jobs cj
set status = 'done',
    last_error = null,
    updated_at = now()
where cj.status in ('pending', 'retry')
  and exists (
    select 1
    from public.post_metrics pm
    where pm.post_key = cj.post_key
      and lower(pm.checkpoint) = lower(cj.checkpoint)
  );

update public.checkpoint_jobs cj
set status = 'skipped',
    last_error = 'Checkpoint skipped - outside live fire horizon',
    updated_at = now()
from public.posts p
where p.post_key = cj.post_key
  and cj.status in ('pending', 'retry')
  and coalesce(
    public.fn_checkpoint_job_business_day(p.posted_at, cj.checkpoint, 'Asia/Kolkata'),
    (coalesce(cj.next_run_at, now()) at time zone 'Asia/Kolkata')::date
  ) < (((now() at time zone 'Asia/Kolkata')::date) - 14);

commit;
