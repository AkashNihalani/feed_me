begin;

update public.checkpoint_jobs cj
set status = 'retry',
    attempt = 0,
    next_run_at = now(),
    last_error = 'Recovered BrightData account outage skip: requeued for retry',
    resurrection_count = coalesce(cj.resurrection_count, 0) + 1,
    updated_at = now()
from public.posts p
where p.post_key = cj.post_key
  and cj.status = 'skipped'
  and lower(coalesce(cj.checkpoint, '')) in ('d1', 'd3', 'd7', 'd21')
  and (
    lower(coalesce(cj.last_error, '')) like 'hard-skip:400 client error:%api.brightdata.com/datasets/v3/scrape%'
    or lower(coalesce(cj.last_error, '')) like 'hard-skip:%customer is not active%'
  )
  and (p.posted_at at time zone 'Asia/Kolkata')::date >= ((now() at time zone 'Asia/Kolkata')::date - 30)
  and not exists (
    select 1
    from public.post_metrics pm
    where pm.post_key = cj.post_key
      and lower(pm.checkpoint) = lower(cj.checkpoint)
  );

commit;
