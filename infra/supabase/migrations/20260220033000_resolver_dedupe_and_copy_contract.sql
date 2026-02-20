-- Resolver dedupe + copy contract
-- Goal:
-- 1) One active alert per feeder/day/signal/checkpoint/context
-- 2) Keep highest severity on collisions
-- 3) Normalize generic copy to concise value-driven headlines

begin;

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
  v_pct text;
begin
  if p_context not in ('own','competitor','macro') then
    return null;
  end if;
  if p_family_group not in ('A','B','C','D','E') then
    return null;
  end if;

  v_day := coalesce(p_business_date_ist, (now() at time zone 'Asia/Kolkata')::date);
  v_score := greatest(0, least(100, coalesce(p_intensity, 0)));
  v_alert_type := public.fn_alert_type_from_intensity(v_score);

  -- Canonical user-facing dedupe scope (not post-scoped):
  -- signal + feeder + checkpoint + context + business day
  v_dedupe_key := format(
    '%s:%s:%s:%s:%s',
    coalesce(p_signal_code, 'signal'),
    coalesce(p_feeder_id::text, 'feed'),
    coalesce(p_checkpoint, 'na'),
    p_context,
    v_day::text
  );

  v_headline := case p_signal_code
    when 'A1_baseline_displacement' then 'Baseline break'
    when 'A2_ceiling_break' then 'Fresh high'
    when 'A3_reach_explosion' then 'Reach lift'
    when 'A4_engagement_efficiency' then 'Engagement lift'
    when 'A5_cold_open' then 'Cold open win'
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

  v_pct := nullif(coalesce(p_payload->>'percentile', p_payload->>'percentile_performance', ''), '');

  v_body := coalesce(nullif(trim(p_body), ''), '');
  if v_body = ''
     or v_body ilike 'signal detected%'
     or v_body ilike 'range break%'
     or v_body ilike 'band shattered%'
     or v_body ilike '%triggered %' then
    v_body := case
      when v_pct is not null then format('%s at %s. Top %s%%.', v_headline, upper(coalesce(p_checkpoint, 'd1')), v_pct)
      else format('%s at %s.', v_headline, upper(coalesce(p_checkpoint, 'd1')))
    end;
  end if;
  v_body := left(v_body, 220);

  insert into public.fire_alerts (
    feed_id,
    feeder_id,
    post_key,
    checkpoint,
    alert_type,
    signal_code,
    context,
    family_group,
    intensity_score,
    headline,
    title,
    body,
    payload,
    priority_score,
    status,
    dedupe_key,
    business_date_ist,
    created_at,
    updated_at
  )
  values (
    p_feed_id,
    p_feeder_id,
    p_post_key,
    p_checkpoint,
    v_alert_type,
    p_signal_code,
    p_context,
    p_family_group,
    v_score,
    v_headline,
    v_headline,
    v_body,
    coalesce(p_payload, '{}'::jsonb),
    v_score,
    'new',
    v_dedupe_key,
    v_day,
    now(),
    now()
  )
  on conflict (dedupe_key)
  do update set
    intensity_score = greatest(coalesce(public.fire_alerts.intensity_score, 0), excluded.intensity_score),
    priority_score = greatest(coalesce(public.fire_alerts.priority_score, 0), excluded.priority_score),
    alert_type = public.fn_alert_type_from_intensity(greatest(coalesce(public.fire_alerts.intensity_score, 0), excluded.intensity_score)),
    headline = case
      when excluded.intensity_score >= coalesce(public.fire_alerts.intensity_score, 0) then excluded.headline
      else public.fire_alerts.headline
    end,
    title = case
      when excluded.intensity_score >= coalesce(public.fire_alerts.intensity_score, 0) then excluded.title
      else public.fire_alerts.title
    end,
    body = case
      when excluded.intensity_score >= coalesce(public.fire_alerts.intensity_score, 0) then excluded.body
      else public.fire_alerts.body
    end,
    payload = case
      when excluded.intensity_score >= coalesce(public.fire_alerts.intensity_score, 0) then excluded.payload
      else public.fire_alerts.payload
    end,
    post_key = case
      when excluded.intensity_score >= coalesce(public.fire_alerts.intensity_score, 0) then excluded.post_key
      else public.fire_alerts.post_key
    end,
    feeder_id = coalesce(public.fire_alerts.feeder_id, excluded.feeder_id),
    status = case when public.fire_alerts.status = 'dismissed' then 'new' else public.fire_alerts.status end,
    business_date_ist = excluded.business_date_ist,
    updated_at = now()
  returning id into v_id;

  return v_id;
end;
$$;

-- One-time collapse of active resolver duplicates under canonical key.
with keyed as (
  select
    fa.id,
    format(
      '%s:%s:%s:%s:%s',
      coalesce(fa.signal_code, 'signal'),
      coalesce(fa.feeder_id::text, 'feed'),
      coalesce(fa.checkpoint, 'na'),
      coalesce(fa.context, 'own'),
      coalesce(fa.business_date_ist::text, (fa.created_at at time zone 'Asia/Kolkata')::date::text)
    ) as new_key,
    row_number() over (
      partition by
        coalesce(fa.signal_code, 'signal'),
        coalesce(fa.feeder_id::text, 'feed'),
        coalesce(fa.checkpoint, 'na'),
        coalesce(fa.context, 'own'),
        coalesce(fa.business_date_ist::text, (fa.created_at at time zone 'Asia/Kolkata')::date::text)
      order by
        case when fa.dedupe_key = format(
          '%s:%s:%s:%s:%s',
          coalesce(fa.signal_code, 'signal'),
          coalesce(fa.feeder_id::text, 'feed'),
          coalesce(fa.checkpoint, 'na'),
          coalesce(fa.context, 'own'),
          coalesce(fa.business_date_ist::text, (fa.created_at at time zone 'Asia/Kolkata')::date::text)
        ) then 0 else 1 end,
        coalesce(fa.intensity_score, 0) desc,
        fa.created_at desc,
        fa.id desc
    ) as rn
  from public.fire_alerts fa
  where fa.status in ('new','sent')
    and fa.signal_code ~ '^[A-E][0-9]_'
    and fa.context in ('own','competitor','macro')
    and fa.family_group in ('A','B','C','D','E')
)
update public.fire_alerts fa
set dedupe_key = k.new_key,
    updated_at = now()
from keyed k
where fa.id = k.id
  and k.rn = 1
  and fa.dedupe_key is distinct from k.new_key;

with keyed as (
  select
    fa.id,
    row_number() over (
      partition by
        coalesce(fa.signal_code, 'signal'),
        coalesce(fa.feeder_id::text, 'feed'),
        coalesce(fa.checkpoint, 'na'),
        coalesce(fa.context, 'own'),
        coalesce(fa.business_date_ist::text, (fa.created_at at time zone 'Asia/Kolkata')::date::text)
      order by coalesce(fa.intensity_score, 0) desc, fa.created_at desc, fa.id desc
    ) as rn
  from public.fire_alerts fa
  where fa.status in ('new','sent')
    and fa.signal_code ~ '^[A-E][0-9]_'
    and fa.context in ('own','competitor','macro')
    and fa.family_group in ('A','B','C','D','E')
)
update public.fire_alerts fa
set status = 'dismissed',
    updated_at = now()
from keyed k
where fa.id = k.id
  and k.rn > 1;

-- Normalize overly generic legacy resolver copy on active rows.
update public.fire_alerts
set
  title = case signal_code
    when 'A1_baseline_displacement' then 'Baseline break'
    when 'A2_ceiling_break' then 'Fresh high'
    when 'A3_reach_explosion' then 'Reach lift'
    when 'A4_engagement_efficiency' then 'Engagement lift'
    when 'A5_cold_open' then 'Cold open win'
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
    else title
  end,
  headline = case signal_code
    when 'A1_baseline_displacement' then 'Baseline break'
    when 'A2_ceiling_break' then 'Fresh high'
    when 'A3_reach_explosion' then 'Reach lift'
    when 'A4_engagement_efficiency' then 'Engagement lift'
    when 'A5_cold_open' then 'Cold open win'
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
    else headline
  end,
  body = case
    when body ilike 'signal detected%'
      or body ilike 'range break%'
      or body ilike 'band shattered%'
      or body ilike '%triggered %'
    then left(
      case
        when coalesce(payload->>'percentile', payload->>'percentile_performance') is not null
          then format('%s at %s. Top %s%%.',
            coalesce(headline, title, 'Signal'),
            upper(coalesce(payload->>'checkpoint', checkpoint, 'd1')),
            coalesce(payload->>'percentile', payload->>'percentile_performance')
          )
        else format('%s at %s.', coalesce(headline, title, 'Signal'), upper(coalesce(payload->>'checkpoint', checkpoint, 'd1')))
      end,
      220
    )
    else body
  end,
  updated_at = now()
where status in ('new','sent')
  and signal_code ~ '^[A-E][0-9]_';

commit;
