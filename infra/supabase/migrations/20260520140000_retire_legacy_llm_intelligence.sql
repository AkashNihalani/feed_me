-- Retire legacy LLM intelligence surfaces.
-- The active LLM path is now:
--   qualifying D7 reels -> observation fingerprints -> post breakdowns -> feeder files.

drop table if exists public.signal_intelligence cascade;
drop table if exists public.post_focus_reads cascade;
drop table if exists public.feeder_focus cascade;
drop table if exists public.feed_focus cascade;
drop table if exists public.focus_compile_locks cascade;
drop table if exists public.post_intelligence cascade;

drop function if exists public.fn_hot_d7_post_intelligence_ready(text);

create table if not exists public.post_breakdowns (
  post_key text primary key references public.posts(post_key) on delete cascade,
  breakdown jsonb not null,
  breakdown_version text not null,
  source_fingerprint_model_version text,
  source_fingerprint_hash text,
  generated_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.feeder_files (
  id bigserial primary key,
  feeder_id bigint references public.feeders(id) on delete set null,
  feeder_handle text not null,
  feed_file jsonb not null,
  compile_version text not null,
  source_breakdown_version text,
  active_window text,
  status text not null default 'active',
  source text not null default 'pipeline',
  generated_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists feeder_files_handle_compile_version_uidx
  on public.feeder_files (feeder_handle, compile_version);

create index if not exists feeder_files_status_idx
  on public.feeder_files (status);

create index if not exists feeder_files_feed_file_gin_idx
  on public.feeder_files using gin (feed_file);

comment on table public.post_breakdowns is
  'Strategic compression layer generated from post_fingerprints for feeder-file clustering.';

comment on table public.feeder_files is
  'Rolling behavioral memory for one feeder, compiled from post_breakdowns and consumed by Feederboard.';
