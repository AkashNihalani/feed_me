-- Two-lane Fire alerts: tracking (checkpoint) + insight (contextual)
-- Prevents low-value duplicate copy between lanes.

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fire_alerts_alert_family_check'
      AND conrelid = 'public.fire_alerts'::regclass
  ) THEN
    ALTER TABLE public.fire_alerts DROP CONSTRAINT fire_alerts_alert_family_check;
  END IF;

  ALTER TABLE public.fire_alerts
    ADD CONSTRAINT fire_alerts_alert_family_check
    CHECK (alert_family IN ('tracking','insight'));
END $$;

CREATE OR REPLACE FUNCTION public.enqueue_phase1_fire_alert(
  p_post_key text,
  p_checkpoint text default null
)
RETURNS int
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_feed_id bigint;
  v_feeder_id bigint;
  v_handle text;
  v_post_url text;
  v_thumbnail_url text;
  v_display_url text;
  v_posted_at timestamptz;
  v_business_date_ist date;
  v_checkpoint text;
  v_pct int;
  v_tag text;
  v_type text;
  v_urgency text;
  v_layer text;
  v_confidence text;
  v_priority_tracking numeric;
  v_priority_insight numeric;
  v_d1_pct int;
  v_delta int;
  v_recent_hot int;
  v_recent_top20 int;
  v_recent_median int;
  v_tracking_title text;
  v_tracking_body text;
  v_insight_title text;
  v_insight_body text;
  v_inserted int := 0;
BEGIN
  SELECT
    fd.feed_id,
    fd.id,
    fd.handle,
    p.post_url,
    p.thumbnail_url,
    p.display_url,
    p.posted_at,
    pm.checkpoint,
    pm.percentile_performance,
    pm.percentile_tag
  INTO
    v_feed_id,
    v_feeder_id,
    v_handle,
    v_post_url,
    v_thumbnail_url,
    v_display_url,
    v_posted_at,
    v_checkpoint,
    v_pct,
    v_tag
  FROM public.post_metrics pm
  JOIN public.posts p ON p.post_key = pm.post_key
  JOIN public.feeders fd ON fd.id = p.feeder_id
  WHERE pm.post_key = p_post_key
    AND (p_checkpoint IS NULL OR pm.checkpoint = p_checkpoint)
  ORDER BY pm.computed_at DESC
  LIMIT 1;

  IF v_feed_id IS NULL THEN
    RETURN 0;
  END IF;
  IF v_checkpoint NOT IN ('d1','d3','d7') THEN
    RETURN 0;
  END IF;
  IF v_tag NOT IN ('✅','🔥','🚀') THEN
    RETURN 0;
  END IF;

  v_business_date_ist := CASE
    WHEN v_posted_at IS NOT NULL THEN (v_posted_at AT TIME ZONE 'Asia/Kolkata')::date
    ELSE (now() AT TIME ZONE 'Asia/Kolkata')::date
  END;

  v_type := public.fn_fire_alert_type_from_percentile_tag(v_tag);
  IF v_type IS NULL THEN
    RETURN 0;
  END IF;

  v_urgency := public.fn_fire_alert_urgency_from_type(v_type);
  v_layer := CASE WHEN v_checkpoint = 'd1' THEN 'layer_1' ELSE 'layer_5' END;
  v_confidence := CASE WHEN v_checkpoint = 'd7' THEN 'confirmed' ELSE 'early' END;

  SELECT pm.percentile_performance
  INTO v_d1_pct
  FROM public.post_metrics pm
  WHERE pm.post_key = p_post_key AND pm.checkpoint = 'd1'
  ORDER BY pm.computed_at DESC
  LIMIT 1;

  IF v_d1_pct IS NOT NULL AND v_pct IS NOT NULL THEN
    v_delta := v_d1_pct - v_pct;
  ELSE
    v_delta := NULL;
  END IF;

  SELECT
    count(*) FILTER (WHERE pm2.percentile_tag IN ('✅','🔥','🚀')),
    count(*) FILTER (WHERE pm2.percentile_performance IS NOT NULL AND pm2.percentile_performance <= 20),
    round(percentile_cont(0.5) WITHIN GROUP (ORDER BY pm2.percentile_performance))::int
  INTO
    v_recent_hot,
    v_recent_top20,
    v_recent_median
  FROM public.post_metrics pm2
  JOIN public.posts p2 ON p2.post_key = pm2.post_key
  WHERE p2.feeder_id = v_feeder_id
    AND pm2.checkpoint = 'd1'
    AND p2.posted_at >= (now() - interval '30 days')
    AND pm2.percentile_performance IS NOT NULL;

  v_priority_tracking := CASE v_type WHEN 'blaze' THEN 55 WHEN 'burn' THEN 45 ELSE 35 END;
  v_priority_insight := CASE v_type WHEN 'blaze' THEN 92 WHEN 'burn' THEN 78 ELSE 62 END;

  v_tracking_title := format('Checkpoint %s: Top %s%%', upper(v_checkpoint), coalesce(v_pct::text, '?'));
  v_tracking_body := format('@%s entered %s at %s. Monitor next checkpoint for confirmation.', coalesce(v_handle, 'handle'), coalesce(v_tag, '✅'), upper(v_checkpoint));

  IF v_checkpoint = 'd1' THEN
    v_insight_title := format('%s insight: Early pattern authority is forming', initcap(v_type));
    v_insight_body := format('In the last 30 days, @%s logged %s hot D1 posts with %s in Top 20%%. This post entered Top %s%%.', coalesce(v_handle, 'handle'), coalesce(v_recent_hot, 0), coalesce(v_recent_top20, 0), coalesce(v_pct::text, '?'));
  ELSIF v_delta IS NOT NULL AND v_delta > 0 THEN
    v_insight_title := format('%s insight: Post-publish momentum is strengthening', initcap(v_type));
    v_insight_body := format('@%s improved from D1 %s%% to %s %s%% (%s-point gain). Median D1 over 30d is %s%%.', coalesce(v_handle, 'handle'), coalesce(v_d1_pct::text, '?'), upper(v_checkpoint), coalesce(v_pct::text, '?'), v_delta, coalesce(v_recent_median::text, '?'));
  ELSIF v_delta IS NOT NULL AND v_delta < 0 THEN
    v_insight_title := format('%s insight: Early spike softened after publish', initcap(v_type));
    v_insight_body := format('@%s moved from D1 %s%% to %s %s%% (%s-point drop). Treat as conditional signal.', coalesce(v_handle, 'handle'), coalesce(v_d1_pct::text, '?'), upper(v_checkpoint), coalesce(v_pct::text, '?'), abs(v_delta));
  ELSE
    v_insight_title := format('%s insight: Checkpoint confirmed with baseline context', initcap(v_type));
    v_insight_body := format('@%s is at Top %s%% on %s. Last-30d D1 median is %s%%.', coalesce(v_handle, 'handle'), coalesce(v_pct::text, '?'), upper(v_checkpoint), coalesce(v_recent_median::text, '?'));
  END IF;

  INSERT INTO public.fire_alerts (
    feed_id, alert_type, title, body, payload, priority_score, status,
    dedupe_key, alert_family, alert_urgency, post_key, checkpoint,
    layer_key, confidence, percentile_tag, percentile_performance,
    business_date_ist, updated_at
  )
  VALUES (
    v_feed_id, v_type, v_tracking_title, v_tracking_body,
    jsonb_build_object(
      'lane', 'tracking',
      'post_key', p_post_key,
      'post_url', v_post_url,
      'thumbnail_url', coalesce(v_thumbnail_url, v_display_url),
      'display_url', v_display_url,
      'handle', v_handle,
      'checkpoint', v_checkpoint,
      'percentile_tag', v_tag,
      'percentile_performance', v_pct,
      'business_date_ist', v_business_date_ist,
      'd1_percentile', v_d1_pct,
      'delta_vs_d1', v_delta,
      'layer', v_layer,
      'confidence', v_confidence,
      'action', 'Monitor and wait for next checkpoint confirmation.'
    ),
    v_priority_tracking, 'new',
    format('tracking:%s:%s', p_post_key, v_checkpoint),
    'tracking', v_urgency, p_post_key, v_checkpoint,
    v_layer, v_confidence, v_tag, v_pct, v_business_date_ist, now()
  )
  ON CONFLICT (dedupe_key)
  DO UPDATE SET
    alert_type = excluded.alert_type,
    title = excluded.title,
    body = excluded.body,
    payload = excluded.payload,
    priority_score = excluded.priority_score,
    alert_family = excluded.alert_family,
    alert_urgency = excluded.alert_urgency,
    layer_key = excluded.layer_key,
    confidence = excluded.confidence,
    percentile_tag = excluded.percentile_tag,
    percentile_performance = excluded.percentile_performance,
    business_date_ist = excluded.business_date_ist,
    updated_at = now();

  v_inserted := v_inserted + 1;

  -- Insight lane only when there is enough historical context.
  IF coalesce(v_recent_hot, 0) >= 3 OR v_checkpoint IN ('d3','d7') THEN
    INSERT INTO public.fire_alerts (
      feed_id, alert_type, title, body, payload, priority_score, status,
      dedupe_key, alert_family, alert_urgency, post_key, checkpoint,
      layer_key, confidence, percentile_tag, percentile_performance,
      business_date_ist, updated_at
    )
    VALUES (
      v_feed_id, v_type, v_insight_title, v_insight_body,
      jsonb_build_object(
        'lane', 'insight',
        'post_key', p_post_key,
        'post_url', v_post_url,
        'thumbnail_url', coalesce(v_thumbnail_url, v_display_url),
        'display_url', v_display_url,
        'handle', v_handle,
        'checkpoint', v_checkpoint,
        'percentile_tag', v_tag,
        'percentile_performance', v_pct,
        'business_date_ist', v_business_date_ist,
        'd1_percentile', v_d1_pct,
        'delta_vs_d1', v_delta,
        'layer', v_layer,
        'confidence', v_confidence,
        'recent_hot_30d', coalesce(v_recent_hot, 0),
        'recent_top20_30d', coalesce(v_recent_top20, 0),
        'recent_median_30d', v_recent_median,
        'action', CASE
          WHEN v_type = 'blaze' THEN 'Scale this pattern immediately across next posts.'
          WHEN v_type = 'burn' THEN 'Replicate this structure in the next 24-48 hours.'
          ELSE 'Run one close variant to validate repeatability.'
        END
      ),
      v_priority_insight, 'new',
      format('insight:%s:%s', p_post_key, v_checkpoint),
      'insight', v_urgency, p_post_key, v_checkpoint,
      v_layer, v_confidence, v_tag, v_pct, v_business_date_ist, now()
    )
    ON CONFLICT (dedupe_key)
    DO UPDATE SET
      alert_type = excluded.alert_type,
      title = excluded.title,
      body = excluded.body,
      payload = excluded.payload,
      priority_score = excluded.priority_score,
      alert_family = excluded.alert_family,
      alert_urgency = excluded.alert_urgency,
      layer_key = excluded.layer_key,
      confidence = excluded.confidence,
      percentile_tag = excluded.percentile_tag,
      percentile_performance = excluded.percentile_performance,
      business_date_ist = excluded.business_date_ist,
      updated_at = now();

    v_inserted := v_inserted + 1;
  END IF;

  RETURN v_inserted;
END;
$$;
