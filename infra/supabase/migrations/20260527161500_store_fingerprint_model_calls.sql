begin;

alter table if exists public.feeder_file_model_calls
  drop constraint if exists feeder_file_model_calls_call_type_check;

alter table if exists public.feeder_file_model_calls
  add constraint feeder_file_model_calls_call_type_check
    check (call_type in ('fingerprint', 'post_breakdown', 'feeder_file_compile', 'pattern', 'proof'));

create unique index if not exists feeder_file_model_calls_fingerprint_uidx
  on public.feeder_file_model_calls (call_type, post_key, prompt_version)
  where call_type = 'fingerprint' and post_key is not null;

comment on table public.feeder_file_model_calls is
  'Audit log for feeder-file LLM calls, including raw outputs for fingerprint, post breakdown, compile, pattern, and proof calls so parser failures can be recovered without rerunning models.';

commit;
