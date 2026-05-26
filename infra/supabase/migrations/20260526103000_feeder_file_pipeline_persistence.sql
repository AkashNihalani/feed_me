-- Durable feeder file packaging layer.
-- Existing source layers remain:
--   post_fingerprints -> post_breakdowns -> feeder_files.feed_file
-- This migration adds the generated pattern/proof reads without duplicating proof rows.

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

create table if not exists public.feeder_file_patterns (
  id bigserial primary key,
  feeder_file_id bigint not null references public.feeder_files(id) on delete cascade,
  feeder_handle text not null,
  pattern_id text not null,
  status text not null default 'candidate',
  reason text,
  core_post_count integer not null default 0,
  support_post_count integer not null default 0,
  pattern_breakdown jsonb not null default '{}'::jsonb,
  members jsonb not null default '[]'::jsonb,
  pattern_read jsonb not null default '{}'::jsonb,
  proof_reads jsonb not null default '[]'::jsonb,
  pattern_model text,
  pattern_prompt_version text,
  proof_model text,
  proof_prompt_version text,
  generated_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint feeder_file_patterns_status_check
    check (status in ('active', 'candidate')),
  unique (feeder_file_id, pattern_id)
);

create index if not exists feeder_file_patterns_handle_status_idx
  on public.feeder_file_patterns (feeder_handle, status, updated_at desc);

create index if not exists feeder_file_patterns_file_idx
  on public.feeder_file_patterns (feeder_file_id, updated_at desc);

create table if not exists public.feeder_file_model_calls (
  id bigserial primary key,
  feeder_file_id bigint references public.feeder_files(id) on delete cascade,
  call_key text not null,
  call_type text not null,
  feeder_handle text not null,
  pattern_id text,
  post_key text,
  model text not null,
  prompt_version text not null,
  system_prompt text not null,
  user_payload jsonb not null,
  raw_output text,
  parsed_output jsonb,
  status text not null default 'running',
  error text,
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  updated_at timestamptz not null default now(),
  constraint feeder_file_model_calls_call_type_check
    check (call_type in ('post_breakdown', 'feeder_file_compile', 'pattern', 'proof')),
  unique (feeder_file_id, call_key)
);

alter table if exists public.feeder_file_model_calls
  alter column feeder_file_id drop not null;

alter table if exists public.feeder_file_model_calls
  drop constraint if exists feeder_file_model_calls_call_type_check;

alter table if exists public.feeder_file_model_calls
  add constraint feeder_file_model_calls_call_type_check
    check (call_type in ('post_breakdown', 'feeder_file_compile', 'pattern', 'proof'));

create index if not exists feeder_file_model_calls_file_idx
  on public.feeder_file_model_calls (feeder_file_id, started_at);

create index if not exists feeder_file_model_calls_lookup_idx
  on public.feeder_file_model_calls (feeder_handle, call_type, updated_at desc);

create unique index if not exists feeder_file_model_calls_post_breakdown_uidx
  on public.feeder_file_model_calls (call_type, post_key, prompt_version)
  where call_type = 'post_breakdown' and post_key is not null;

comment on table public.feeder_file_patterns is
  'One row per feeder file pattern. Stores backend pattern membership plus generated pattern/proof reads; invalid pools are kept as candidates.';

comment on table public.feeder_file_model_calls is
  'Exact prompt, payload, raw output, parsed output, and errors for feeder file model calls.';
