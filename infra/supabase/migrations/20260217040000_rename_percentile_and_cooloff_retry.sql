-- Rename velocity_percentile → percentile_performance (idempotent)
-- Also add resurrection_count columns for cool-off retry system

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='post_metrics' AND column_name='velocity_percentile'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='post_metrics' AND column_name='percentile_performance'
  ) THEN
    ALTER TABLE public.post_metrics
      RENAME COLUMN velocity_percentile TO percentile_performance;
  END IF;
END $$;

ALTER TABLE public.post_metrics
  ADD COLUMN IF NOT EXISTS percentile_performance int;

ALTER TABLE public.run_jobs
  ADD COLUMN IF NOT EXISTS resurrection_count int NOT NULL DEFAULT 0;

ALTER TABLE public.checkpoint_jobs
  ADD COLUMN IF NOT EXISTS resurrection_count int NOT NULL DEFAULT 0;

CREATE OR REPLACE FUNCTION public.resurrect_failed_jobs(p_max_resurrections int DEFAULT 3)
RETURNS TABLE (run_rows int, checkpoint_rows int)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_run_rows int := 0;
  v_checkpoint_rows int := 0;
BEGIN
  UPDATE public.run_jobs
  SET status = 'retry',
      attempt = 0,
      next_run_at = now() + interval '5 minutes',
      last_error = format('Resurrected (attempt %s/%s) from failed state', resurrection_count + 1, p_max_resurrections),
      resurrection_count = resurrection_count + 1,
      updated_at = now()
  WHERE status = 'failed'
    AND resurrection_count < p_max_resurrections;
  GET DIAGNOSTICS v_run_rows = ROW_COUNT;

  UPDATE public.checkpoint_jobs
  SET status = 'retry',
      attempt = 0,
      next_run_at = now() + interval '5 minutes',
      last_error = format('Resurrected (attempt %s/%s) from failed state', resurrection_count + 1, p_max_resurrections),
      resurrection_count = resurrection_count + 1,
      updated_at = now()
  WHERE status = 'failed'
    AND resurrection_count < p_max_resurrections;
  GET DIAGNOSTICS v_checkpoint_rows = ROW_COUNT;

  RETURN QUERY SELECT v_run_rows, v_checkpoint_rows;
END;
$$;

DO $$
BEGIN
  IF to_regnamespace('cron') IS NOT NULL THEN
    PERFORM cron.unschedule(j.jobid)
    FROM cron.job j
    WHERE j.jobname = 'feedme_resurrect_failed';

    PERFORM cron.schedule(
      'feedme_resurrect_failed',
      '0 */6 * * *',
      $q$SELECT public.resurrect_failed_jobs(3);$q$
    );
  END IF;
END $$;
