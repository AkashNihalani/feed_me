-- FeedMe canonical scheduler (BrightData exact-age contract)
select cron.schedule(
  'feedme_enqueue_daily_0005_ist',
  '35 18 * * *',
  $$select public.enqueue_daily_jobs(now());$$
);

select cron.schedule(
  'feedme_enqueue_daily_0030_ist_watchdog',
  '0 19 * * *',
  $$select public.enqueue_daily_jobs(now());$$
);

select cron.schedule(
  'feedme_repair_lane_0045_ist_watchdog',
  '15 19 * * *',
  $$select public.enqueue_repair_jobs_from_previous_day('Asia/Kolkata');$$
);

select cron.schedule(
  'feedme_enqueue_weekly_followers_mon_0010_ist',
  '40 18 * * 0',
  $$select public.enqueue_weekly_follower_jobs(now());$$
);

select cron.schedule(
  'feedme_enqueue_weekly_followers_mon_0030_ist_watchdog',
  '0 19 * * 0',
  $$select public.enqueue_weekly_follower_jobs(now());$$
);

-- Optional worker watchdog:
-- select cron.schedule('feedme_guardrails_every_minute', '* * * * *', $$select * from public.requeue_stale_jobs(30);$$);
