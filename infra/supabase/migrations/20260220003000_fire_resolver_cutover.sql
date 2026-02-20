-- Cutover: keep only Intelligence Resolver (V1) as Fire alert writer.
-- Legacy phase1 trigger writes tracking/insight rows that clash with resolver signal rows.

begin;

-- 1) Disable legacy post_metrics -> enqueue_phase1_fire_alert path.
drop trigger if exists trg_enqueue_phase1_fire_alerts on public.post_metrics;

-- 2) Hide legacy rows from active Fire stream.
-- Resolver rows always have signal_code; legacy rows do not.
update public.fire_alerts
set status = 'archived',
    updated_at = now()
where signal_code is null
  and coalesce(status, 'new') in ('new', 'sent')
  and (
    dedupe_key like 'phase1:%'
    or dedupe_key like 'tracking:%'
    or dedupe_key like 'insight:%'
  );

commit;
