begin;

create table if not exists public.post_condensations (
  post_key text primary key references public.posts(post_key) on delete cascade,
  condensation jsonb not null,
  condensation_version text not null,
  source_fingerprint_model_version text,
  source_fingerprint_hash text not null default '',
  model_version text,
  model_call_id bigint references public.feeder_file_model_calls(id) on delete set null,
  generated_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists post_condensations_version_idx
  on public.post_condensations (condensation_version, updated_at desc);

create index if not exists post_condensations_source_fingerprint_idx
  on public.post_condensations (source_fingerprint_hash);

comment on table public.post_condensations is
  'Canonical per-post condensation artifacts generated from post_fingerprints. feeder_file_model_calls remains the model-call audit/recovery log.';

commit;
