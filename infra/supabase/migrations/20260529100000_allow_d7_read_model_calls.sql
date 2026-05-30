alter table if exists public.feeder_file_model_calls
  drop constraint if exists feeder_file_model_calls_call_type_check;

alter table if exists public.feeder_file_model_calls
  add constraint feeder_file_model_calls_call_type_check
    check (call_type in ('fingerprint', 'post_breakdown', 'post_condensation', 'd7_read', 'feeder_file_compile', 'pattern', 'proof'));

create unique index if not exists feeder_file_model_calls_d7_read_uidx
  on public.feeder_file_model_calls (call_type, post_key, prompt_version)
  where call_type = 'd7_read' and post_key is not null;

comment on table public.feeder_file_model_calls is
  'Durable LLM call log for feeder file intelligence: fingerprints, post breakdowns, post condensations, D7 reads, compiles, pattern packaging, and proof packaging.';
