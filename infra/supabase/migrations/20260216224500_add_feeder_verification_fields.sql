alter table if exists public.feeders
  add column if not exists profile_pic_url text,
  add column if not exists follower_count bigint,
  add column if not exists verification_status text not null default 'pending',
  add column if not exists verification_error text,
  add column if not exists verified_at timestamptz;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'feeders_verification_status_check'
      and conrelid = 'public.feeders'::regclass
  ) then
    alter table public.feeders
      add constraint feeders_verification_status_check
      check (verification_status in ('pending','verified','failed'));
  end if;
end $$;

create index if not exists feeders_verification_status_idx
  on public.feeders(verification_status);
