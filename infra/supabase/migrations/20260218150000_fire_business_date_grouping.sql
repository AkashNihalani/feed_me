-- Group Fire alerts by posting business day (IST), not alert creation timestamp.

alter table public.fire_alerts
  add column if not exists business_date_ist date;

create index if not exists fire_alerts_business_date_idx
  on public.fire_alerts(business_date_ist desc, feed_id, created_at desc);

-- Backfill from linked post timestamp.
update public.fire_alerts fa
set business_date_ist = (p.posted_at at time zone 'Asia/Kolkata')::date,
    updated_at = now()
from public.posts p
where fa.post_key = p.post_key
  and fa.business_date_ist is null
  and p.posted_at is not null;

-- Fallback for older rows without post linkage.
update public.fire_alerts
set business_date_ist = (created_at at time zone 'Asia/Kolkata')::date,
    updated_at = now()
where business_date_ist is null;

create or replace function public.enqueue_phase1_fire_alert(
  p_post_key text,
  p_checkpoint text default null
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_feed_id bigint;
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
  v_priority numeric;
  v_d1_pct int;
  v_delta int;
  v_title text;
  v_body text;
  v_dedupe text;
begin
  select
    fd.feed_id,
    fd.handle,
    p.post_url,
    p.thumbnail_url,
    p.display_url,
    p.posted_at,
    pm.checkpoint,
    pm.percentile_performance,
    pm.percentile_tag
  into
    v_feed_id,
    v_handle,
    v_post_url,
    v_thumbnail_url,
    v_display_url,
    v_posted_at,
    v_checkpoint,
    v_pct,
    v_tag
  from public.post_metrics pm
  join public.posts p on p.post_key = pm.post_key
  join public.feeders fd on fd.id = p.feeder_id
  where pm.post_key = p_post_key
    and (p_checkpoint is null or pm.checkpoint = p_checkpoint)
  order by pm.computed_at desc
  limit 1;

  if v_feed_id is null then
    return 0;
  end if;
  if v_checkpoint not in ('d1','d3','d7') then
    return 0;
  end if;
  if v_tag not in ('✅','🔥','🚀') then
    return 0;
  end if;

  v_business_date_ist := case
    when v_posted_at is not null then (v_posted_at at time zone 'Asia/Kolkata')::date
    else (now() at time zone 'Asia/Kolkata')::date
  end;

  v_type := public.fn_fire_alert_type_from_percentile_tag(v_tag);
  if v_type is null then
    return 0;
  end if;

  v_urgency := public.fn_fire_alert_urgency_from_type(v_type);
  v_layer := case when v_checkpoint = 'd1' then 'layer_1' else 'layer_5' end;
  v_confidence := case when v_checkpoint = 'd7' then 'confirmed' else 'early' end;

  select pm.percentile_performance
  into v_d1_pct
  from public.post_metrics pm
  where pm.post_key = p_post_key and pm.checkpoint = 'd1'
  order by pm.computed_at desc
  limit 1;

  if v_d1_pct is not null and v_pct is not null then
    v_delta := v_d1_pct - v_pct;
  else
    v_delta := null;
  end if;

  v_priority := case v_type when 'blaze' then 90 when 'burn' then 70 else 50 end;

  if v_checkpoint = 'd1' then
    v_title := format('%s signal: Top %s%% at D1', initcap(v_type), coalesce(v_pct::text, '?'));
    v_body := format('@%s entered %s at D1. Pattern is separating from baseline. Keep this creative direction active.', coalesce(v_handle, 'handle'), v_tag);
  elsif v_checkpoint in ('d3','d7') and v_delta is not null and v_delta > 0 then
    v_title := format('%s acceleration: D1 %s%% → %s %s%%', initcap(v_type), v_d1_pct, upper(v_checkpoint), v_pct);
    v_body := format('@%s is accelerating (%s%% better since D1). Momentum is strengthening post-publish.', coalesce(v_handle, 'handle'), v_delta);
  elsif v_checkpoint in ('d3','d7') and v_delta is not null and v_delta < 0 then
    v_title := format('%s watch: D1 %s%% → %s %s%%', initcap(v_type), v_d1_pct, upper(v_checkpoint), v_pct);
    v_body := format('@%s cooled after launch (%s%% weaker vs D1). Treat this as soft momentum and avoid over-scaling yet.', coalesce(v_handle, 'handle'), abs(v_delta));
  else
    v_title := format('%s checkpoint: %s Top %s%%', initcap(v_type), upper(v_checkpoint), coalesce(v_pct::text, '?'));
    v_body := format('@%s is in hot zone at %s. Monitor next checkpoint for momentum confirmation.', coalesce(v_handle, 'handle'), upper(v_checkpoint));
  end if;

  v_dedupe := format('phase1:%s:%s', p_post_key, v_checkpoint);

  insert into public.fire_alerts (
    feed_id, alert_type, title, body, payload, priority_score, status,
    dedupe_key, alert_family, alert_urgency, post_key, checkpoint,
    layer_key, confidence, percentile_tag, percentile_performance,
    business_date_ist, updated_at
  )
  values (
    v_feed_id, v_type, v_title, v_body,
    jsonb_build_object(
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
      'action', case
        when v_type = 'blaze' then 'Scale this pattern immediately across next posts.'
        when v_type = 'burn' then 'Replicate this structure in the next 24-48 hours.'
        else 'Monitor and test one close variant next.'
      end,
      'evidence', jsonb_build_array(
        format('Checkpoint %s', upper(v_checkpoint)),
        format('Percentile %s%%', coalesce(v_pct::text, '?')),
        format('Tag %s', v_tag)
      )
    ),
    v_priority, 'new',
    v_dedupe, 'velocity', v_urgency, p_post_key, v_checkpoint,
    v_layer, v_confidence, v_tag, v_pct, v_business_date_ist, now()
  )
  on conflict (dedupe_key)
  do update
    set alert_type = excluded.alert_type,
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

  return 1;
end;
$$;

-- Refresh recent alerts after function update so payload/date are aligned.
select public.backfill_phase1_fire_alerts(168);
