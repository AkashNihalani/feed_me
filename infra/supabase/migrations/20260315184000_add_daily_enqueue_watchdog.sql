begin;

-- Backup enqueue pass in case the 00:05 IST job is skipped/transiently fails.
-- Idempotent because enqueue_daily_jobs guards by business_date + done/open rows.
do $$
declare
  r record;
begin
  if to_regnamespace('cron') is null then
    raise notice 'pg_cron schema not found. Enable pg_cron and rerun migration.';
    return;
  end if;

  for r in
    select jobid
    from cron.job
    where jobname = 'feedme_enqueue_daily_0030_ist_watchdog'
  loop
    perform cron.unschedule(r.jobid);
  end loop;

  perform cron.schedule(
    'feedme_enqueue_daily_0030_ist_watchdog',
    '0 19 * * *', -- 00:30 IST
    $q$select public.enqueue_daily_jobs(now());$q$
  );
end $$;

commit;
