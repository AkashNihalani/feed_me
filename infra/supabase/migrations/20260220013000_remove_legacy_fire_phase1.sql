-- Remove legacy Phase1 Fire writer path entirely.
-- Keep only resolver-based Fire alerts (fn_upsert_fire_alert + resolver functions).

begin;

-- Legacy trigger/function chain.
drop trigger if exists trg_enqueue_phase1_fire_alerts on public.post_metrics;
drop function if exists public.tg_enqueue_phase1_fire_alerts();
drop function if exists public.enqueue_phase1_fire_alert(text, text);
drop function if exists public.backfill_phase1_fire_alerts(int);
drop function if exists public.fn_fire_alert_type_from_percentile_tag(text);
drop function if exists public.fn_fire_alert_urgency_from_type(text);

-- Archive remaining legacy rows (resolver rows always have signal_code).
update public.fire_alerts
set status = 'archived',
    updated_at = now()
where signal_code is null
  and coalesce(status, 'new') in ('new', 'sent');

commit;
