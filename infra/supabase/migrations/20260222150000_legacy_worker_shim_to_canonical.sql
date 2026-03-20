begin;

-- Legacy call path shim:
-- old worker sequence:
--   fn_refresh_feeder_baselines -> fn_resolve_post_signals -> enqueue_slot_state_alerts -> fn_try_resolve_feed_signals
-- We reroute to canonical resolver and neutralize duplicate legacy legs.

create or replace function public.fn_resolve_post_signals(
  p_feeder_id bigint,
  p_checkpoint text
)
returns int
language plpgsql
security definer
set search_path = public
as $$
begin
  -- No-op in v2 canonical path. Kept for backward compatibility with old worker binaries.
  return 0;
end;
$$;

create or replace function public.enqueue_slot_state_alerts(
  p_feeder_id bigint,
  p_checkpoint text,
  p_business_date_ist date default null
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_day date := coalesce(p_business_date_ist, (now() at time zone 'Asia/Kolkata')::date);
begin
  -- Canonical single entrypoint for post+feed alert resolution.
  perform public.fn_process_checkpoint(p_feeder_id, lower(coalesce(p_checkpoint, 'd1')), v_day);
  return 1;
end;
$$;

create or replace function public.fn_try_resolve_feed_signals(
  p_feed_id bigint,
  p_checkpoint text,
  p_business_date_ist date default null
)
returns int
language plpgsql
security definer
set search_path = public
as $$
begin
  -- Feed resolution runs inside fn_process_checkpoint; this avoids duplicate/legacy feed alerts.
  return 0;
end;
$$;

commit;
