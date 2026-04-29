begin;

create table if not exists public.focus_compile_locks (
  scope text not null check (scope in ('feeder', 'feed')),
  entity_id bigint not null,
  locked_until timestamptz,
  last_attempt_at timestamptz,
  last_success_at timestamptz,
  last_error text,
  attempt_count integer not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (scope, entity_id)
);

create index if not exists focus_compile_locks_due_idx
  on public.focus_compile_locks(scope, last_success_at, locked_until);

comment on table public.focus_compile_locks is
  'DB-backed timing and lock state for Focus Brain feeder/feed compilers. Prevents restart storms and duplicate compile attempts.';

commit;
