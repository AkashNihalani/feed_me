-- FeedMe canonical scheduler (single source of truth)
-- 23:30 IST daily enqueue only (18:00 UTC)
select cron.schedule(
  'feedme_enqueue_daily_2330_ist_only',
  '0 18 * * *',
  $$select public.enqueue_daily_jobs(now());$$
);

-- Optional guardrail if worker is not always-on:
-- select cron.schedule('feedme_guardrails_every_minute', '* * * * *', $$select * from public.requeue_stale_jobs(30);$$);
