-- Churn-snapshot table for the deterministic feeder-file selector.
--
-- Stores the full output of select_active_feeder_file() per feeder per build so
-- that later run-reports can diff inning-over-inning churn (winners entering /
-- exiting / climbing / fading). This is a write-only-from-pipeline log; the
-- selector itself stays pure and DB-free.
--
-- STAGED ONLY. Not wired into the live D7 read path. Apply after human review.

create table if not exists public.feeder_file_winner_snapshots (
  id bigserial primary key,
  feeder_id integer not null,
  built_at timestamptz not null default now(),
  selected jsonb not null,  -- the full selector output (ranked + recent_context + counts + still_learning)
  constraint feeder_file_winner_snapshots_selected_object_check
    check (jsonb_typeof(selected) = 'object')
);

create index if not exists feeder_file_winner_snapshots_feeder_built_idx
  on public.feeder_file_winner_snapshots (feeder_id, built_at desc);

comment on table public.feeder_file_winner_snapshots is
  'Per-feeder, per-build snapshot of the deterministic feeder-file selector output (20 ranked-winner slots + 10 recent-context slots). Used for inning-over-inning winner churn diffing in run reports. Pipeline-written only.';

comment on column public.feeder_file_winner_snapshots.selected is
  'Full select_active_feeder_file() result: {ranked, recent_context, counts, still_learning}.';
