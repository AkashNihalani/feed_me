-- Canonical scheduler policy for FeedMe engine:
-- - Exactly one daily enqueue at 23:30 IST (18:00 UTC)
-- - No weekly or legacy feedme_* cron jobs from previous flows

DO $$
DECLARE
  r RECORD;
BEGIN
  IF to_regnamespace('cron') IS NULL THEN
    RAISE NOTICE 'pg_cron schema not found. Enable pg_cron in Supabase and rerun migration.';
    RETURN;
  END IF;

  -- Remove old FeedMe cron jobs to avoid split-brain scheduling.
  FOR r IN
    SELECT jobid, jobname
    FROM cron.job
    WHERE jobname LIKE 'feedme_%'
  LOOP
    PERFORM cron.unschedule(r.jobid);
  END LOOP;

  -- Single source: enqueue daily at 23:30 IST (18:00 UTC)
  PERFORM cron.schedule(
    'feedme_enqueue_daily_2330_ist_only',
    '0 18 * * *',
    $q$select public.enqueue_daily_jobs(now());$q$
  );
END $$;
