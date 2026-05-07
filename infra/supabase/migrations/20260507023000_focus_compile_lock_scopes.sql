begin;

alter table if exists public.focus_compile_locks
  drop constraint if exists focus_compile_locks_scope_check;

alter table if exists public.focus_compile_locks
  add constraint focus_compile_locks_scope_check
  check (scope in ('feeder', 'feed', 'feeder_focus_state'));

commit;
