-- Architecture split: move metric computation from VPS worker into Supabase trigger
-- Worker now writes only raw views/likes/comments; Supabase computes the rest

-- 1) Rename velocity_value → growth_rate, then drop it
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='post_metrics' AND column_name='velocity_value'
  ) THEN
    ALTER TABLE public.post_metrics DROP COLUMN velocity_value;
  END IF;
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='post_metrics' AND column_name='growth_rate'
  ) THEN
    ALTER TABLE public.post_metrics DROP COLUMN growth_rate;
  END IF;
END $$;

-- 2) Helper: metric_value from raw counts
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
    WHEN p_likes IS NOT NULL OR p_comments IS NOT NULL THEN (coalesce(p_likes, 0) + coalesce(p_comments, 0))::numeric
    ELSE NULL
  END;
$$;

-- 3) Helper: percentile tag emoji
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

-- 4) Main trigger function: compute derived metrics on INSERT/UPDATE
CREATE OR REPLACE FUNCTION public.tg_compute_post_metrics()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
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
  -- Look up the post's media type and feeder
  SELECT p.media_type, p.feeder_id
  INTO v_media_type, v_feeder_id
  FROM public.posts p
  WHERE p.post_key = NEW.post_key;

  -- Look up the feeder's follower count
  IF v_feeder_id IS NOT NULL THEN
    SELECT fd.follower_count
    INTO v_follower_count
    FROM public.feeders fd
    WHERE fd.id = v_feeder_id;
  END IF;

  -- metric_value: views for reels, likes+comments for everything else
  v_mv := public.fn_metric_value(coalesce(v_media_type, 'image'), NEW.views, NEW.likes, NEW.comments);

  -- percentile_performance: rank by metric_value against peer pool
  v_pct := NULL;
  IF v_mv IS NOT NULL AND v_feeder_id IS NOT NULL THEN
    -- Count how many peers have HIGHER metric_value (lower rank = better)
    SELECT count(*) + 1
    INTO v_rank
    FROM public.post_metrics pm
    JOIN public.posts p2 ON p2.post_key = pm.post_key
    WHERE p2.feeder_id = v_feeder_id
      AND coalesce(p2.media_type, '') = coalesce(v_media_type, '')
      AND pm.checkpoint = NEW.checkpoint
      AND pm.post_key <> NEW.post_key
      AND pm.metric_value IS NOT NULL
      AND pm.metric_value > v_mv;

    SELECT count(*)
    INTO v_pool_size
    FROM public.post_metrics pm
    JOIN public.posts p2 ON p2.post_key = pm.post_key
    WHERE p2.feeder_id = v_feeder_id
      AND coalesce(p2.media_type, '') = coalesce(v_media_type, '')
      AND pm.checkpoint = NEW.checkpoint
      AND pm.metric_value IS NOT NULL;

    -- Include self in pool
    v_pool_size := v_pool_size + 1;

    IF v_pool_size > 0 THEN
      v_pct := greatest(1, least(100, round((v_rank::numeric / v_pool_size) * 100)));
    END IF;
  END IF;

  -- Write computed values into NEW row
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

-- 5) Attach BEFORE trigger (BEFORE so we can modify NEW)
DROP TRIGGER IF EXISTS trg_compute_post_metrics ON public.post_metrics;
CREATE TRIGGER trg_compute_post_metrics
  BEFORE INSERT OR UPDATE ON public.post_metrics
  FOR EACH ROW
  EXECUTE FUNCTION public.tg_compute_post_metrics();

-- 6) Drop unused helper if it exists
DROP FUNCTION IF EXISTS public.fn_checkpoint_days(text);
