-- P0 hotfix: make remote schema compatible with current worker runtime
-- Worker expects:
--   post_metrics.percentile_performance
--   run_jobs.resurrection_count
--   checkpoint_jobs.resurrection_count

-- 1) Ensure percentile_performance exists
ALTER TABLE public.post_metrics
  ADD COLUMN IF NOT EXISTS percentile_performance int;

-- 2) Backfill from legacy velocity_percentile when both columns exist
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='post_metrics' AND column_name='velocity_percentile'
  ) THEN
    UPDATE public.post_metrics
    SET percentile_performance = COALESCE(percentile_performance, velocity_percentile)
    WHERE percentile_performance IS NULL;
  END IF;
END $$;

-- 3) Ensure resurrection_count columns exist
ALTER TABLE public.run_jobs
  ADD COLUMN IF NOT EXISTS resurrection_count int NOT NULL DEFAULT 0;

ALTER TABLE public.checkpoint_jobs
  ADD COLUMN IF NOT EXISTS resurrection_count int NOT NULL DEFAULT 0;

-- 4) Keep percentile bounds check aligned
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'post_metrics_percentile_performance_check'
      AND conrelid = 'public.post_metrics'::regclass
  ) THEN
    ALTER TABLE public.post_metrics
      ADD CONSTRAINT post_metrics_percentile_performance_check
      CHECK (percentile_performance IS NULL OR (percentile_performance BETWEEN 1 AND 100));
  END IF;
END $$;
