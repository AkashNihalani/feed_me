-- Fire pipeline guardrails
-- Enforce deterministic dedupe and provide a health check for single-writer integrity.

begin;

-- Remove known legacy writer triggers/functions if they still exist.
drop trigger if exists trg_enqueue_phase1_fire_alerts on public.post_metrics;
drop trigger if exists trg_run_fire_alert_resolver on public.post_metrics;

drop function if exists public.tg_enqueue_phase1_fire_alerts();
drop function if exists public.enqueue_phase1_fire_alert(text, text);
drop function if exists public.backfill_phase1_fire_alerts(int);
drop function if exists public.fn_fire_alert_type_from_percentile_tag(text);
drop function if exists public.fn_fire_alert_urgency_from_type(text);

-- Ensure every alert row has a deterministic key. Preserve legacy history via id-scoped key.
update public.fire_alerts
set dedupe_key = format('legacy:%s', id),
    updated_at = now()
where dedupe_key is null;

-- Protect invariant going forward.
alter table public.fire_alerts
  alter column dedupe_key set not null;

create unique index if not exists fire_alerts_dedupe_key_uniq
  on public.fire_alerts (dedupe_key);

-- Health function to validate writer + dedupe invariants in one query.
create or replace function public.fn_fire_pipeline_health()
returns table(
  enabled_fire_writer_triggers int,
  null_dedupe_keys int,
  duplicate_dedupe_groups int,
  active_duplicate_groups int
)
language sql
security definer
set search_path = public
as $$
  with trig as (
    select count(*)::int as c
    from pg_trigger t
    join pg_class rel on rel.oid = t.tgrelid
    join pg_namespace n on n.oid = rel.relnamespace
    where n.nspname = 'public'
      and rel.relname = 'post_metrics'
      and t.tgenabled = 'O'
      and not t.tgisinternal
      and t.tgname ilike '%fire%'
  ),
  nulls as (
    select count(*)::int as c
    from public.fire_alerts
    where dedupe_key is null
  ),
  dups as (
    select count(*)::int as c
    from (
      select dedupe_key
      from public.fire_alerts
      group by dedupe_key
      having count(*) > 1
    ) x
  ),
  active_dups as (
    select count(*)::int as c
    from (
      select dedupe_key
      from public.fire_alerts
      where status in ('new', 'sent')
      group by dedupe_key
      having count(*) > 1
    ) y
  )
  select trig.c, nulls.c, dups.c, active_dups.c
  from trig, nulls, dups, active_dups;
$$;

commit;
