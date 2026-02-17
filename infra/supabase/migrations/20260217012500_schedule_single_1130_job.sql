DO $$
DECLARE
  r RECORD;
BEGIN
  IF to_regnamespace('cron') IS NULL THEN
    RAISE EXCEPTION 'pg_cron schema still not available after extension enable.';
  END IF;

  FOR r IN
    SELECT jobid, jobname
    FROM cron.job
    WHERE jobname LIKE 'feedme_%'
  LOOP
    PERFORM cron.unschedule(r.jobid);
  END LOOP;

  PERFORM cron.schedule(
    'feedme_enqueue_daily_2330_ist_only',
    '0 18 * * *',
    $q$select public.enqueue_daily_jobs(now());$q$
  );
END $$;
