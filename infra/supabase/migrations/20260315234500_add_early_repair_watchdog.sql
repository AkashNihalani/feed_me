begin;

-- Early repair watchdog right after midnight to recover missed daily enqueue quickly.
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
    where jobname = 'feedme_repair_lane_0045_ist_watchdog'
  loop
    perform cron.unschedule(r.jobid);
  end loop;

  perform cron.schedule(
    'feedme_repair_lane_0045_ist_watchdog',
    '15 19 * * *', -- 00:45 IST
    $q$select public.enqueue_repair_jobs_from_previous_day('Asia/Kolkata');$q$
  );
end $$;

commit;
