begin;

alter table if exists public.fire_alerts
  add column if not exists signal_payload jsonb not null default '{}'::jsonb;

delete from public.fire_alerts
where signal_code is null
   or signal_code not in ('OWN_PATTERN', 'CROSS_PATTERN', 'ANCHOR_PATTERN');

alter table if exists public.fire_alerts
  alter column alert_type set default 'watch';

alter table if exists public.fire_alerts
  drop constraint if exists fire_alerts_alert_type_check;

alter table if exists public.fire_alerts
  add constraint fire_alerts_alert_type_check
  check (alert_type in ('watch', 'today', 'now'));

alter table if exists public.fire_alerts
  drop constraint if exists fire_alerts_context_check;

alter table if exists public.fire_alerts
  add constraint fire_alerts_context_check
  check (context in ('own', 'cross', 'anchor'));

alter table if exists public.fire_alerts
  drop constraint if exists fire_alerts_signal_code_check;

alter table if exists public.fire_alerts
  add constraint fire_alerts_signal_code_check
  check (signal_code in ('OWN_PATTERN', 'CROSS_PATTERN', 'ANCHOR_PATTERN'));

create index if not exists fire_alerts_signal_window_idx
  on public.fire_alerts(feed_id, business_date_ist desc, checkpoint, signal_code);

create or replace function public.fn_post_media_rollover_deadline(
  p_posted_at timestamptz,
  p_asset_role text
)
returns timestamptz
language sql
immutable
as $$
  select
    coalesce(p_posted_at, now()) +
    case
      when lower(coalesce(p_asset_role, '')) in ('thumbnail', 'display')
        or lower(coalesce(p_asset_role, '')) like 'carousel_%'
        then interval '90 days'
      else interval '8 days'
    end
$$;

update public.post_media_assets assets
set purge_after = public.fn_post_media_rollover_deadline(p.posted_at, assets.asset_role),
    updated_at = now()
from public.posts p
where p.post_key = assets.post_key
  and (
    lower(coalesce(assets.asset_role, '')) in ('thumbnail', 'display')
    or lower(coalesce(assets.asset_role, '')) like 'carousel_%'
  )
  and assets.purge_after is distinct from public.fn_post_media_rollover_deadline(p.posted_at, assets.asset_role);

commit;
