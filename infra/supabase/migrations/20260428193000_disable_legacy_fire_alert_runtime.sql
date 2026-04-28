begin;

-- Retired runtime guard.
-- Fire cards now read post_metrics directly, and intelligence alerts now write
-- signals/signal_posts plus post_fingerprints/signal_intelligence. Keep this
-- compatibility function as a no-op so any stale/manual RPC call cannot recreate
-- legacy fire_alert payload writes or web-push side effects.
create or replace function public.fn_process_checkpoint(
  p_feeder_id bigint,
  p_checkpoint text,
  p_business_date date
)
returns int
language plpgsql
security definer
set search_path = public
as $$
begin
  return 0;
end;
$$;

drop trigger if exists trg_apply_fire_alert_contract on public.fire_alerts;
drop trigger if exists trg_enqueue_web_push_jobs on public.fire_alerts;
drop trigger if exists trg_fire_alerts_score_guard on public.fire_alerts;
drop trigger if exists trg_format_fire_alert_contract on public.fire_alerts;
drop trigger if exists trg_zz_fire_alert_rank_sync on public.fire_alerts;

delete from public.fire_alerts
where signal_code in ('OWN_PATTERN', 'CROSS_PATTERN', 'ANCHOR_PATTERN');

commit;
