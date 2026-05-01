begin;

-- Focus Brain V2 is additive and shadow-safe. Legacy columns remain the
-- runtime source of truth until the V2 cutover gate passes.

alter table if exists public.feeder_focus
  add column if not exists content_profile jsonb not null default '{}'::jsonb,
  add column if not exists metric_profile jsonb not null default '{}'::jsonb,
  add column if not exists pattern_registry jsonb not null default '[]'::jsonb,
  add column if not exists derived_views jsonb not null default '{}'::jsonb,
  add column if not exists delta_log jsonb not null default '{}'::jsonb,
  add column if not exists compile_meta jsonb not null default '{}'::jsonb;

create index if not exists feeder_focus_v2_presence_idx
  on public.feeder_focus(feed_id, focus_updated_at desc nulls last)
  where compile_meta <> '{}'::jsonb
    and pattern_registry <> '[]'::jsonb;

alter table if exists public.feed_focus
  add column if not exists content_profile jsonb not null default '{}'::jsonb,
  add column if not exists metric_profile jsonb not null default '{}'::jsonb,
  add column if not exists pattern_registry jsonb not null default '[]'::jsonb,
  add column if not exists convergence jsonb not null default '[]'::jsonb,
  add column if not exists divergence jsonb not null default '[]'::jsonb,
  add column if not exists derived_views jsonb not null default '{}'::jsonb,
  add column if not exists delta_log jsonb not null default '{}'::jsonb,
  add column if not exists compile_meta jsonb not null default '{}'::jsonb;

create index if not exists feed_focus_v2_presence_idx
  on public.feed_focus(focus_updated_at desc nulls last)
  where compile_meta <> '{}'::jsonb
    and pattern_registry <> '[]'::jsonb;

comment on column public.feeder_focus.pattern_registry is
  'Focus Brain V2 canonical feeder pattern registry. Legacy structured_patterns remains for compatibility until cutover.';

comment on column public.feed_focus.pattern_registry is
  'Focus Brain V2 canonical feed pattern registry with feeder-pattern replication pointers.';

commit;
