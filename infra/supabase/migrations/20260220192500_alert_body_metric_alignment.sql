-- Align alert body metric labels with payload values and remove incorrect hardcoded units.

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
  v_days text := nullif(coalesce(p_payload->>'days_since_ceiling', p_payload->>'best_in_days', ''), '');
  v_streak text := nullif(coalesce(p_payload->>'streak_count', ''), '');

  v_metric_key text := lower(coalesce(p_payload->>'metric_key', p_payload->>'metric_name', p_payload->>'primary_metric', p_payload->>'reach_metric', ''));
  v_metric_value numeric := nullif(coalesce(p_payload->>'metric_value', ''), '')::numeric;
  v_views numeric := nullif(coalesce(p_payload->>'views', ''), '')::numeric;
  v_likes numeric := nullif(coalesce(p_payload->>'likes', ''), '')::numeric;
  v_comments numeric := nullif(coalesce(p_payload->>'comments', ''), '')::numeric;

  v_metric_label text := 'metric';
  v_metric_display text;
  v_body text;
begin
  if v_metric_key like '%comment%' then
    v_metric_label := 'comments';
    v_metric_value := coalesce(v_comments, v_metric_value);
  elsif v_metric_key like '%view%' or v_metric_key like '%reach%' then
    v_metric_label := 'views';
    v_metric_value := coalesce(v_views, v_metric_value);
  elsif v_metric_key like '%like%' then
    v_metric_label := 'likes';
    v_metric_value := coalesce(v_likes, v_metric_value);
  elsif p_signal_code = 'A4_engagement_efficiency' then
    v_metric_label := 'comments';
    v_metric_value := coalesce(v_comments, v_metric_value);
  elsif v_metric_value is not null and v_views is not null and v_metric_value = v_views then
    v_metric_label := 'views';
  elsif v_metric_value is not null and v_comments is not null and v_metric_value = v_comments then
    v_metric_label := 'comments';
  elsif v_metric_value is not null and v_likes is not null and v_metric_value = v_likes then
    v_metric_label := 'likes';
  elsif v_comments is not null then
    v_metric_label := 'comments';
    v_metric_value := v_comments;
  elsif v_views is not null then
    v_metric_label := 'views';
    v_metric_value := v_views;
  elsif v_likes is not null then
    v_metric_label := 'likes';
    v_metric_value := v_likes;
  end if;

  v_metric_display := case
    when v_metric_value is null then null
    when abs(v_metric_value) >= 1000000 then trim(to_char(v_metric_value / 1000000.0, 'FM999990D0')) || 'M'
    when abs(v_metric_value) >= 1000 then trim(to_char(v_metric_value / 1000.0, 'FM999990D0')) || 'K'
    else trim(to_char(v_metric_value, 'FM9999999990'))
  end;

  case p_signal_code
    when 'A4_engagement_efficiency' then
      if v_metric_display is not null and v_comments_pct is not null and v_reach_pct is not null then
        v_body := format('%s comments — C%s vs R%s at %s.', v_metric_display, v_comments_pct, v_reach_pct, v_cp);
      elsif v_metric_display is not null and v_pct is not null then
        v_body := format('%s comments — top %s%% at %s.', v_metric_display, v_pct, v_cp);
      else
        v_body := format('Comments outran reach at %s.', v_cp);
      end if;

    when 'A1_baseline_displacement' then
      if v_metric_display is not null and v_pct is not null then
        v_body := format('%s %s — top %s%% at %s.', v_metric_display, v_metric_label, v_pct, v_cp);
      elsif v_pct is not null then
        v_body := format('Top %s%% at %s versus baseline.', v_pct, v_cp);
      else
        v_body := format('Outperforming baseline at %s.', v_cp);
      end if;

    when 'A3_reach_explosion' then
      if v_metric_display is not null and v_pct is not null then
        v_body := format('%s %s — top %s%% at %s.', v_metric_display, v_metric_label, v_pct, v_cp);
      else
        v_body := format('Reach is materially above baseline at %s.', v_cp);
      end if;

    when 'A2_ceiling_break' then
      if v_metric_display is not null and v_days is not null then
        v_body := format('%s %s — best in %s days at %s.', v_metric_display, v_metric_label, v_days, v_cp);
      elsif v_days is not null then
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

-- Rewrite active rows so frontend + backend stay aligned immediately.
update public.fire_alerts fa
set body = public.fn_compose_alert_body(
    fa.signal_code,
    fa.checkpoint,
    coalesce(fa.payload, '{}'::jsonb),
    fa.body,
    coalesce(fa.headline, fa.title, 'Signal')
  ),
  updated_at = now()
where fa.status in ('new', 'sent');

commit;
