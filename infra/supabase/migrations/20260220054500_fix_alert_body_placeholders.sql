-- Fix malformed alert body copy (no '?' placeholders)

begin;

create or replace function public.fn_compose_alert_body(
  p_signal_code text,
  p_checkpoint text,
  p_payload jsonb,
  p_fallback_body text,
  p_headline text
)
returns text
language plpgsql
immutable
as $$
declare
  v_cp text := upper(coalesce(p_checkpoint, p_payload->>'checkpoint', 'd1'));
  v_pct text := nullif(coalesce(p_payload->>'percentile', p_payload->>'percentile_performance', ''), '');
  v_comments_pct text := nullif(coalesce(p_payload->>'comments_percentile', ''), '');
  v_reach_pct text := nullif(coalesce(p_payload->>'views_percentile', p_payload->>'likes_percentile', ''), '');
  v_gap text := nullif(coalesce(p_payload->>'gap', p_payload->>'gap_vs_anchor', ''), '');
  v_delta text := nullif(coalesce(p_payload->>'delta', ''), '');
  v_days text := nullif(coalesce(p_payload->>'days_since_ceiling', ''), '');
  v_streak text := nullif(coalesce(p_payload->>'streak_count', ''), '');
  v_body text;
begin
  case p_signal_code
    when 'A4_engagement_efficiency' then
      if v_comments_pct is not null and v_reach_pct is not null then
        v_body := format('Comments quality is Top %s%% vs reach Top %s%% at %s.', v_comments_pct, v_reach_pct, v_cp);
      elsif v_gap is not null then
        v_body := format('Comments are %s positions ahead of reach at %s.', v_gap, v_cp);
      else
        v_body := format('Comments outran reach at %s.', v_cp);
      end if;
    when 'A1_baseline_displacement' then
      if v_pct is not null then
        v_body := format('Running at Top %s%% at %s versus your normal baseline.', v_pct, v_cp);
      else
        v_body := format('Outperforming baseline at %s.', v_cp);
      end if;
    when 'A3_reach_explosion' then
      v_body := format('Reach is materially above your normal level at %s.', v_cp);
    when 'A2_ceiling_break' then
      if v_days is not null then
        v_body := format('Best result in %s days at %s.', v_days, v_cp);
      else
        v_body := format('New ceiling set at %s.', v_cp);
      end if;
    when 'A6_consistency_streak' then
      if v_streak is not null then
        v_body := format('%s hot-zone posts in a row at %s.', v_streak, v_cp);
      else
        v_body := format('Consistency streak holding at %s.', v_cp);
      end if;
    when 'B1_structural_acceleration' then
      if v_delta is not null then
        v_body := format('Improved %s positions since D1 by %s.', v_delta, v_cp);
      else
        v_body := format('Momentum improved by %s.', v_cp);
      end if;
    when 'B2_late_bloomer' then
      v_body := format('Opened cold, now improving into %s.', v_cp);
    when 'B3_early_fade' then
      v_body := format('Early lift softened by %s.', v_cp);
    when 'B4_sustained_strength' then
      v_body := format('Hot-zone strength held through %s.', v_cp);
    when 'D1_anchor_displacement' then
      if v_gap is not null then
        v_body := format('Moved %s positions ahead of anchor at %s.', v_gap, v_cp);
      else
        v_body := format('Outperforming anchor at %s.', v_cp);
      end if;
    when 'D2_competitive_gap_closing' then
      if v_gap is not null then
        v_body := format('Within %s positions of anchor at %s.', abs(v_gap::numeric)::int, v_cp);
      else
        v_body := format('Gap closing versus anchor at %s.', v_cp);
      end if;
    when 'D3_competitor_regime_break' then
      v_body := format('Competitor entered stronger tier at %s.', v_cp);
    when 'E1_format_takeover' then
      v_body := format('Format dominance is visible at %s.', v_cp);
    when 'E3_structural_convergence' then
      v_body := format('Multiple feeders hit hot zone in the same %s cycle.', v_cp);
    else
      v_body := coalesce(nullif(trim(p_fallback_body), ''), format('%s at %s.', coalesce(nullif(trim(p_headline), ''), 'Signal'), v_cp));
  end case;

  v_body := regexp_replace(v_body, '\?+', '', 'g');
  v_body := regexp_replace(v_body, '\s{2,}', ' ', 'g');
  return left(trim(v_body), 220);
end;
$$;

create or replace function public.fn_upsert_fire_alert(
  p_feed_id bigint,
  p_feeder_id bigint,
  p_post_key text,
  p_checkpoint text,
  p_context text,
  p_family_group text,
  p_signal_code text,
  p_intensity int,
  p_headline text,
  p_body text,
  p_business_date_ist date,
  p_payload jsonb
)
returns bigint
language plpgsql
security definer
set search_path = public
as $$
declare
  v_alert_type text;
  v_dedupe_key text;
  v_id bigint;
  v_day date;
  v_score int;
  v_headline text;
  v_body text;
begin
  if p_context not in ('own','competitor','macro') then return null; end if;
  if p_family_group not in ('A','B','C','D','E') then return null; end if;

  v_day := coalesce(p_business_date_ist, (now() at time zone 'Asia/Kolkata')::date);
  v_score := greatest(0, least(100, coalesce(p_intensity, 0)));
  v_alert_type := public.fn_alert_type_from_intensity(v_score);

  v_dedupe_key := format('%s:%s:%s:%s:%s', coalesce(p_signal_code, 'signal'), coalesce(p_feeder_id::text, 'feed'), coalesce(p_checkpoint, 'na'), p_context, v_day::text);

  v_headline := case p_signal_code
    when 'A1_baseline_displacement' then 'Baseline break'
    when 'A2_ceiling_break' then 'Fresh high'
    when 'A3_reach_explosion' then 'Reach lift'
    when 'A4_engagement_efficiency' then 'Comments outran reach'
    when 'A5_cold_open' then 'Cold open drag'
    when 'A6_consistency_streak' then 'Consistency streak'
    when 'B1_structural_acceleration' then 'Structural acceleration'
    when 'B2_late_bloomer' then 'Late bloomer'
    when 'B3_early_fade' then 'Early fade'
    when 'B4_sustained_strength' then 'Sustained strength'
    when 'D1_anchor_displacement' then 'Anchor displacement'
    when 'D2_competitive_gap_closing' then 'Gap closing'
    when 'D3_competitor_regime_break' then 'Competitor regime break'
    when 'E1_format_takeover' then 'Format takeover'
    when 'E3_structural_convergence' then 'Structural convergence'
    else coalesce(nullif(trim(p_headline), ''), 'Signal')
  end;

  v_body := public.fn_compose_alert_body(p_signal_code, p_checkpoint, coalesce(p_payload, '{}'::jsonb), p_body, v_headline);

  insert into public.fire_alerts (
    feed_id, feeder_id, post_key, checkpoint, alert_type,
    signal_code, context, family_group, intensity_score,
    headline, title, body, payload, priority_score,
    status, dedupe_key, business_date_ist, created_at, updated_at
  ) values (
    p_feed_id, p_feeder_id, p_post_key, p_checkpoint, v_alert_type,
    p_signal_code, p_context, p_family_group, v_score,
    v_headline, v_headline, v_body, coalesce(p_payload, '{}'::jsonb), v_score,
    'new', v_dedupe_key, v_day, now(), now()
  )
  on conflict (dedupe_key)
  do update set
    intensity_score = greatest(coalesce(public.fire_alerts.intensity_score, 0), excluded.intensity_score),
    priority_score = greatest(coalesce(public.fire_alerts.priority_score, 0), excluded.priority_score),
    alert_type = public.fn_alert_type_from_intensity(greatest(coalesce(public.fire_alerts.intensity_score, 0), excluded.intensity_score)),
    headline = case when excluded.intensity_score >= coalesce(public.fire_alerts.intensity_score, 0) then excluded.headline else public.fire_alerts.headline end,
    title = case when excluded.intensity_score >= coalesce(public.fire_alerts.intensity_score, 0) then excluded.title else public.fire_alerts.title end,
    body = case when excluded.intensity_score >= coalesce(public.fire_alerts.intensity_score, 0) then excluded.body else public.fire_alerts.body end,
    payload = case when excluded.intensity_score >= coalesce(public.fire_alerts.intensity_score, 0) then excluded.payload else public.fire_alerts.payload end,
    post_key = case when excluded.intensity_score >= coalesce(public.fire_alerts.intensity_score, 0) then excluded.post_key else public.fire_alerts.post_key end,
    feeder_id = coalesce(public.fire_alerts.feeder_id, excluded.feeder_id),
    status = case when public.fire_alerts.status = 'dismissed' then 'new' else public.fire_alerts.status end,
    business_date_ist = excluded.business_date_ist,
    updated_at = now()
  returning id into v_id;

  return v_id;
end;
$$;

-- Repair existing active malformed bodies.
update public.fire_alerts fa
set body = public.fn_compose_alert_body(
    fa.signal_code,
    fa.checkpoint,
    coalesce(fa.payload, '{}'::jsonb),
    fa.body,
    coalesce(fa.headline, fa.title, 'Signal')
  ),
  updated_at = now()
where fa.status in ('new','sent')
  and (
    fa.body like '?%'
    or fa.body ilike '% ?%'
    or fa.body ilike '%triggered %'
    or fa.body ilike 'signal detected%'
    or fa.body ilike 'range break%'
    or fa.body ilike 'band shattered%'
  );

commit;
