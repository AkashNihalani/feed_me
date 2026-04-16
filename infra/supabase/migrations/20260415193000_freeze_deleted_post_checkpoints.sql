alter table if exists public.posts
  add column if not exists availability_status text;

update public.posts
set availability_status = 'active'
where availability_status is null;

alter table if exists public.posts
  alter column availability_status set default 'active';

alter table if exists public.posts
  alter column availability_status set not null;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'posts_availability_status_check'
  ) then
    alter table public.posts
      add constraint posts_availability_status_check
      check (availability_status in ('active', 'deleted', 'unavailable'));
  end if;
end
$$;

alter table if exists public.posts
  add column if not exists availability_error text;

alter table if exists public.posts
  add column if not exists availability_checked_at timestamptz;

create index if not exists posts_availability_status_checked_idx
  on public.posts(availability_status, availability_checked_at desc nulls last);
