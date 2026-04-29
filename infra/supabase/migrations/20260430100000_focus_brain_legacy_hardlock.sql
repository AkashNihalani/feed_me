begin;

-- Focus Brain is the only active intelligence pipeline:
-- signals/signal_posts -> post_fingerprints -> post_focus_reads ->
-- feeder_focus/feed_focus -> signal_intelligence.
--
-- fire_alerts is retained for metric tracking/status history only. The old
-- post_intelligence + fire_alert pattern payload path must not be callable or
-- writable anymore.

drop function if exists public.fn_enrich_pattern_signal(bigint);
drop function if exists public.fn_hot_d7_post_intelligence_ready(text);
drop function if exists public.tg_checkpoint_done_requires_hot_d7_intelligence();

drop view if exists public.v_post_tagged_performance;
drop view if exists public.v_fire_alert_surface;

drop table if exists public.post_intelligence cascade;

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

drop trigger if exists trg_checkpoint_done_requires_hot_d7_intelligence on public.checkpoint_jobs;
drop trigger if exists trg_apply_fire_alert_contract on public.fire_alerts;
drop trigger if exists trg_enqueue_web_push_jobs on public.fire_alerts;
drop trigger if exists trg_fire_alerts_score_guard on public.fire_alerts;
drop trigger if exists trg_format_fire_alert_contract on public.fire_alerts;
drop trigger if exists trg_zz_fire_alert_rank_sync on public.fire_alerts;

delete from public.fire_alerts
where signal_code in ('OWN_PATTERN', 'CROSS_PATTERN', 'ANCHOR_PATTERN');

alter table if exists public.fire_alerts
  drop column if exists pattern_signal,
  drop column if exists pattern_payload,
  drop column if exists signal_payload;

comment on table public.signal_intelligence is
  'Source of truth for LLM signal cards. Built from signal_posts, post_fingerprints, post_focus_reads, feeder_focus, and feed_focus.';

comment on table public.post_fingerprints is
  'Source of truth for per-post content fingerprints used by Focus Brain.';

comment on table public.fire_alerts is
  'Metric tracking/status history only. Legacy intelligence payload columns and pattern extraction are retired.';

commit;
