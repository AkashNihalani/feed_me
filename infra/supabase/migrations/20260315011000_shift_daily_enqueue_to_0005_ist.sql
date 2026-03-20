begin;

-- Scrape should run just after midnight IST so the previous day's D1
-- appears immediately on the new calendar day.
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
    where jobname in (
      'feedme_enqueue_daily_2330_ist_only',
      'feedme_enqueue_daily_0005_ist'
    )
  loop
    perform cron.unschedule(r.jobid);
  end loop;

  perform cron.schedule(
    'feedme_enqueue_daily_0005_ist',
    '35 18 * * *', -- 00:05 IST
    $q$select public.enqueue_daily_jobs(now());$q$
  );
end $$;

commit;
