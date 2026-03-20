-- DEPRECATED SNAPSHOT FILE
-- Do not run this file on production/dev databases.
-- Use migrations in infra/supabase/migrations as the only source of truth.
-- This file is retained only as historical reference.


-- ─── HELPERS ─────────────────────────────────────────────────

-- Derive a stable post key from an Instagram URL
CREATE OR REPLACE FUNCTION public.fn_post_key(p_url text)
RETURNS text
LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE v text;
BEGIN
  v := lower(coalesce(p_url, ''));
  v := regexp_replace(v, '^https?://(www\.)?instagram\.com/', '', 'i');
  v := split_part(v, '?', 1);
  v := split_part(v, '#', 1);
  v := trim(both '/' from v);
  RETURN v;
END;
$$;

-- Calculate when a checkpoint should fire (in IST by default)
CREATE OR REPLACE FUNCTION public.fn_checkpoint_due_at(
  p_posted_at timestamptz,
  p_days_after int,
  p_tz text DEFAULT 'Asia/Kolkata',
  p_hour int DEFAULT 23,
  p_minute int DEFAULT 30
)
RETURNS timestamptz
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  v_local_date date;
  v_target_local timestamp;
  v_target_utc timestamptz;
BEGIN
  IF p_posted_at IS NULL THEN RETURN now(); END IF;
  v_local_date := ((p_posted_at AT TIME ZONE p_tz)::date + greatest(0, p_days_after));
  v_target_local := (v_local_date::timestamp + make_interval(hours => p_hour, mins => p_minute));
  v_target_utc := (v_target_local AT TIME ZONE p_tz);
  IF v_target_utc < now() THEN RETURN now(); END IF;
  RETURN v_target_utc;
END;
$$;

-- Compute the main metric value from raw counts
CREATE OR REPLACE FUNCTION public.fn_metric_value(
  p_media_type text,
  p_views bigint,
  p_likes bigint,
  p_comments bigint
)
RETURNS numeric
LANGUAGE sql IMMUTABLE
AS $$
  SELECT CASE
    WHEN p_media_type = 'reel' AND p_views IS NOT NULL THEN p_views::numeric
    WHEN p_likes IS NOT NULL OR p_comments IS NOT NULL
      THEN (coalesce(p_likes, 0) + coalesce(p_comments, 0))::numeric
    ELSE NULL
  END;
$$;

-- Map percentile rank to emoji tag
CREATE OR REPLACE FUNCTION public.fn_percentile_tag(p_percentile int)
RETURNS text
LANGUAGE sql IMMUTABLE
AS $$
  SELECT CASE
    WHEN p_percentile IS NULL THEN ''
    WHEN p_percentile <= 5  THEN '🚀'
    WHEN p_percentile <= 20 THEN '🔥'
    WHEN p_percentile <= 35 THEN '✅'
    ELSE '😴'
  END;
$$;


-- ─── TRIGGER: AUTO-COMPUTE METRICS ──────────────────────────

CREATE OR REPLACE FUNCTION public.tg_compute_post_metrics()
RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public
AS $$
DECLARE
  v_media_type text;
  v_follower_count bigint;
  v_feeder_id bigint;
  v_mv numeric;
  v_pool_size int;
  v_rank int;
  v_pct int;
BEGIN
  SELECT p.media_type, p.feeder_id INTO v_media_type, v_feeder_id
  FROM public.posts p WHERE p.post_key = NEW.post_key;

  IF v_feeder_id IS NOT NULL THEN
    SELECT fd.follower_count INTO v_follower_count
    FROM public.feeders fd WHERE fd.id = v_feeder_id;
  END IF;

  v_mv := public.fn_metric_value(coalesce(v_media_type, 'image'), NEW.views, NEW.likes, NEW.comments);

  v_pct := NULL;
  IF v_mv IS NOT NULL AND v_feeder_id IS NOT NULL THEN
    SELECT count(*) + 1 INTO v_rank
    FROM public.post_metrics pm
    JOIN public.posts p2 ON p2.post_key = pm.post_key
    WHERE p2.feeder_id = v_feeder_id
      AND coalesce(p2.media_type, '') = coalesce(v_media_type, '')
      AND pm.checkpoint = NEW.checkpoint
      AND pm.post_key <> NEW.post_key
      AND pm.metric_value IS NOT NULL
      AND pm.metric_value > v_mv;

    SELECT count(*) INTO v_pool_size
    FROM public.post_metrics pm
    JOIN public.posts p2 ON p2.post_key = pm.post_key
    WHERE p2.feeder_id = v_feeder_id
      AND coalesce(p2.media_type, '') = coalesce(v_media_type, '')
      AND pm.checkpoint = NEW.checkpoint
      AND pm.metric_value IS NOT NULL;

    v_pool_size := v_pool_size + 1;
    IF v_pool_size > 0 THEN
      v_pct := greatest(1, least(100, round((v_rank::numeric / v_pool_size) * 100)));
    END IF;
  END IF;

  NEW.metric_value := v_mv;
  NEW.percentile_performance := v_pct;
  NEW.percentile_tag := public.fn_percentile_tag(v_pct);
  NEW.perf_score := CASE
    WHEN v_mv IS NOT NULL AND v_follower_count IS NOT NULL AND v_follower_count > 0
    THEN round((v_mv / v_follower_count) * 100.0, 4)
    ELSE NULL
  END;
  RETURN NEW;
END;
$$;


-- ─── JOB SCHEDULING ─────────────────────────────────────────

-- Create one run_job per active feeder
CREATE OR REPLACE FUNCTION public.enqueue_daily_jobs(p_run_at timestamptz DEFAULT now())
RETURNS int
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public
AS $$
DECLARE v_rows int := 0;
BEGIN
  INSERT INTO public.run_jobs (feeder_id, job_type, status, next_run_at)
  SELECT fd.id, 'daily', 'pending', p_run_at
  FROM public.feeders fd
  JOIN public.feeds f ON f.id = fd.feed_id
  WHERE f.status='active' AND fd.status='active'
  ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$;

-- Schedule D3, D7, D21 checkpoint jobs for a post
CREATE OR REPLACE FUNCTION public.enqueue_checkpoint_jobs(
  p_post_key text,
  p_posted_at timestamptz,
  p_tz text DEFAULT 'Asia/Kolkata',
  p_hour int DEFAULT 23,
  p_minute int DEFAULT 30
)
RETURNS int
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public
AS $$
DECLARE v_rows int := 0;
BEGIN
  INSERT INTO public.checkpoint_jobs (post_key, checkpoint, status, next_run_at)
  VALUES
    (p_post_key, 'd3',  'pending', public.fn_checkpoint_due_at(p_posted_at, 3, p_tz, p_hour, p_minute)),
    (p_post_key, 'd7',  'pending', public.fn_checkpoint_due_at(p_posted_at, 7, p_tz, p_hour, p_minute)),
    (p_post_key, 'd21', 'pending', public.fn_checkpoint_due_at(p_posted_at, 21, p_tz, p_hour, p_minute))
  ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$;


-- ─── JOB CLAIMING (worker calls these) ──────────────────────

-- Atomically pick and lock run_jobs (avoids double-processing)
CREATE OR REPLACE FUNCTION public.claim_run_jobs(p_limit int DEFAULT 25)
RETURNS setof public.run_jobs
LANGUAGE sql SECURITY DEFINER SET search_path = public
AS $$
  WITH picked AS (
    SELECT id FROM public.run_jobs
    WHERE status IN ('pending','retry') AND next_run_at <= now()
    ORDER BY next_run_at ASC, id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT greatest(1, p_limit)
  ), updated AS (
    UPDATE public.run_jobs r
    SET status='running', updated_at=now()
    FROM picked WHERE r.id = picked.id
    RETURNING r.*
  )
  SELECT * FROM updated;
$$;

-- Skip D21 jobs for posts that weren't hot at D7
CREATE OR REPLACE FUNCTION public.skip_unqualified_d21_jobs()
RETURNS int
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public
AS $$
DECLARE v_rows int := 0;
BEGIN
  UPDATE public.checkpoint_jobs cj
  SET status='skipped',
      last_error='D21 skipped — D7 was not hot (top 35%)',
      updated_at=now()
  WHERE cj.status IN ('pending','retry')
    AND cj.checkpoint='d21'
    AND cj.next_run_at <= now()
    AND NOT EXISTS (
      SELECT 1 FROM public.post_metrics pm
      WHERE pm.post_key = cj.post_key
        AND pm.checkpoint='d7'
        AND pm.percentile_tag IN ('✅','🔥','🚀')
    );
  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$;

-- Atomically pick and lock checkpoint_jobs
CREATE OR REPLACE FUNCTION public.claim_checkpoint_jobs(p_limit int DEFAULT 100)
RETURNS setof public.checkpoint_jobs
LANGUAGE sql SECURITY DEFINER SET search_path = public
AS $$
  WITH picked AS (
    SELECT id FROM public.checkpoint_jobs
    WHERE status IN ('pending','retry') AND next_run_at <= now()
    ORDER BY next_run_at ASC, id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT greatest(1, p_limit)
  ), updated AS (
    UPDATE public.checkpoint_jobs cj
    SET status='running', updated_at=now()
    FROM picked WHERE cj.id = picked.id
    RETURNING cj.*
  )
  SELECT * FROM updated;
$$;


-- ─── JOB RESULT SETTERS ─────────────────────────────────────

CREATE OR REPLACE FUNCTION public.set_run_job_result(
  p_job_id bigint, p_status text,
  p_attempt int DEFAULT NULL,
  p_next_run_at timestamptz DEFAULT NULL,
  p_error text DEFAULT NULL
)
RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path = public
AS $$
  UPDATE public.run_jobs SET
    status=p_status,
    attempt=coalesce(p_attempt, attempt),
    next_run_at=coalesce(p_next_run_at, next_run_at),
    last_error=CASE WHEN p_error IS NULL THEN NULL ELSE left(p_error, 1000) END,
    updated_at=now()
  WHERE id = p_job_id;
$$;

CREATE OR REPLACE FUNCTION public.set_checkpoint_job_result(
  p_job_id bigint, p_status text,
  p_attempt int DEFAULT NULL,
  p_next_run_at timestamptz DEFAULT NULL,
  p_error text DEFAULT NULL
)
RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path = public
AS $$
  UPDATE public.checkpoint_jobs SET
    status=p_status,
    attempt=coalesce(p_attempt, attempt),
    next_run_at=coalesce(p_next_run_at, next_run_at),
    last_error=CASE WHEN p_error IS NULL THEN NULL ELSE left(p_error, 1000) END,
    updated_at=now()
  WHERE id = p_job_id;
$$;


-- ─── SELF-HEALING ────────────────────────────────────────────

-- Rescue jobs stuck in 'running' for too long (worker crash recovery)
CREATE OR REPLACE FUNCTION public.requeue_stale_jobs(p_minutes int DEFAULT 30)
RETURNS TABLE (run_rows int, checkpoint_rows int)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public
AS $$
DECLARE
  v_run_rows int := 0;
  v_checkpoint_rows int := 0;
  v_minutes int := greatest(1, coalesce(p_minutes, 30));
BEGIN
  UPDATE public.run_jobs
  SET status='retry', next_run_at=now(),
      last_error=format('Watchdog: recovered stale job after %s min', v_minutes),
      updated_at=now()
  WHERE status='running'
    AND updated_at < now() - (v_minutes::text || ' minutes')::interval;
  GET DIAGNOSTICS v_run_rows = ROW_COUNT;

  UPDATE public.checkpoint_jobs
  SET status='retry', next_run_at=now(),
      last_error=format('Watchdog: recovered stale checkpoint after %s min', v_minutes),
      updated_at=now()
  WHERE status='running'
    AND updated_at < now() - (v_minutes::text || ' minutes')::interval;
  GET DIAGNOSTICS v_checkpoint_rows = ROW_COUNT;

  RETURN QUERY SELECT v_run_rows, v_checkpoint_rows;
END;
$$;

-- Bring failed jobs back to life (max 3 resurrections per job)
CREATE OR REPLACE FUNCTION public.resurrect_failed_jobs()
RETURNS TABLE(resurrected_run int, resurrected_checkpoint int)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public
AS $$
DECLARE v_run int := 0; v_cp int := 0;
BEGIN
  UPDATE public.run_jobs
  SET status = 'retry', next_run_at = now(),
      resurrection_count = coalesce(resurrection_count, 0) + 1,
      last_error = 'Resurrected by cool-off retry',
      updated_at = now()
  WHERE status = 'failed'
    AND coalesce(resurrection_count, 0) < 3
    AND updated_at < now() - interval '6 hours';
  GET DIAGNOSTICS v_run = ROW_COUNT;

  UPDATE public.checkpoint_jobs
  SET status = 'retry', next_run_at = now(),
      resurrection_count = coalesce(resurrection_count, 0) + 1,
      last_error = 'Resurrected by cool-off retry',
      updated_at = now()
  WHERE status = 'failed'
    AND coalesce(resurrection_count, 0) < 3
    AND updated_at < now() - interval '6 hours';
  GET DIAGNOSTICS v_cp = ROW_COUNT;

  RETURN QUERY SELECT v_run, v_cp;
END;
$$;


-- ─── CRON SCHEDULE ───────────────────────────────────────────
-- (Run this section only if cron jobs need resetting)

-- DO $$
-- BEGIN
--   -- Clear old FeedMe cron jobs
--   FOR r IN SELECT jobid FROM cron.job WHERE jobname LIKE 'feedme_%' LOOP
--     PERFORM cron.unschedule(r.jobid);
--   END LOOP;
--
--   -- Nightly job enqueue at 11:30 PM IST (6:00 PM UTC)
--   PERFORM cron.schedule(
--     'feedme_enqueue_daily',
--     '0 18 * * *',
--     $q$SELECT public.enqueue_daily_jobs(now());$q$
--   );
--
--   -- Resurrect failed jobs every 6 hours
--   PERFORM cron.schedule(
--     'feedme_resurrect_failed',
--     '0 */6 * * *',
--     $q$SELECT public.resurrect_failed_jobs();$q$
--   );
-- END $$;
