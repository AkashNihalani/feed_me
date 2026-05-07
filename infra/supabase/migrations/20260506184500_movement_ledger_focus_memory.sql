begin;

alter table if exists public.feeder_focus
  add column if not exists movement_ledger jsonb not null default '{}'::jsonb,
  add column if not exists account_memory jsonb not null default '{}'::jsonb,
  add column if not exists focus_state jsonb not null default '{}'::jsonb;

alter table if exists public.feed_focus
  add column if not exists movement_ledger jsonb not null default '{}'::jsonb,
  add column if not exists focus_state jsonb not null default '{}'::jsonb;

comment on column public.feeder_focus.movement_ledger is
  'Movement thesis memory: metric-backed competitive separations accumulated from signal intelligence packets.';

comment on column public.feed_focus.movement_ledger is
  'Feed-level movement thesis memory: metric-backed competitive separations accumulated from cohort and anchor signals.';

comment on column public.feeder_focus.account_memory is
  'Slow atmospheric account memory. Explains what movements mean for this feeder; never decides movement truth.';

comment on column public.feeder_focus.focus_state is
  'Rendered movement-first feeder focus: account pressure state synthesized from movement memory, account memory, and stored fingerprints.';

comment on column public.feed_focus.focus_state is
  'Rendered movement-first feed focus synthesized from cross-account movement memory.';

commit;
