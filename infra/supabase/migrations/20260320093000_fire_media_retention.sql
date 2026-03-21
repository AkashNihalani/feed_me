begin;

do $$
begin
  if exists (
    select 1
    from pg_namespace
    where nspname = 'storage'
  ) then
    insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
    values (
      'fire-media',
      'fire-media',
      false,
      52428800,
      array[
        'image/jpeg',
        'image/png',
        'image/webp',
        'image/avif',
        'video/mp4',
        'video/quicktime',
        'audio/mpeg',
        'audio/mp4',
        'audio/aac',
        'audio/x-m4a'
      ]::text[]
    )
    on conflict (id) do update
      set public = excluded.public,
          file_size_limit = excluded.file_size_limit,
          allowed_mime_types = excluded.allowed_mime_types;
  end if;
end $$;

create table if not exists public.post_media_assets (
  id bigserial primary key,
  post_key text not null references public.posts(post_key) on delete cascade,
  asset_role text not null,
  source_url text,
  storage_bucket text,
  storage_path text,
  mime_type text,
  byte_size bigint,
  status text not null default 'pending_capture'
    check (status in ('pending_capture', 'capturing', 'active', 'capture_failed', 'purge_pending', 'purging', 'purge_failed', 'deleted')),
  attempt integer not null default 0,
  next_run_at timestamptz not null default now(),
  captured_at timestamptz,
  purge_after timestamptz,
  deleted_at timestamptz,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (post_key, asset_role)
);

create index if not exists post_media_assets_capture_due_idx
  on public.post_media_assets(next_run_at asc, id asc)
  where status in ('pending_capture', 'capture_failed');

create index if not exists post_media_assets_purge_due_idx
  on public.post_media_assets(purge_after asc, id asc)
  where status in ('active', 'purge_pending', 'purge_failed');

create index if not exists post_media_assets_post_key_idx
  on public.post_media_assets(post_key, asset_role, status);

create or replace function public.claim_post_media_assets_for_capture(p_limit integer default 25)
returns setof public.post_media_assets
language sql
security definer
set search_path = public
as $$
  with picked as (
    select id
    from public.post_media_assets
    where status in ('pending_capture', 'capture_failed')
      and next_run_at <= now()
      and coalesce(source_url, '') <> ''
      and (purge_after is null or purge_after > now())
    order by next_run_at asc, created_at asc, id asc
    for update skip locked
    limit greatest(1, p_limit)
  ), updated as (
    update public.post_media_assets assets
    set status = 'capturing',
        updated_at = now()
    from picked
    where assets.id = picked.id
    returning assets.*
  )
  select * from updated;
$$;

create or replace function public.claim_post_media_assets_for_purge(p_limit integer default 50)
returns setof public.post_media_assets
language sql
security definer
set search_path = public
as $$
  with picked as (
    select id
    from public.post_media_assets
    where status in ('active', 'purge_pending', 'purge_failed')
      and purge_after is not null
      and purge_after <= now()
      and coalesce(storage_path, '') <> ''
      and deleted_at is null
    order by purge_after asc, updated_at asc, id asc
    for update skip locked
    limit greatest(1, p_limit)
  ), updated as (
    update public.post_media_assets assets
    set status = 'purging',
        updated_at = now()
    from picked
    where assets.id = picked.id
    returning assets.*
  )
  select * from updated;
$$;

create or replace function public.tg_schedule_fire_media_retention_from_d7()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_hot boolean;
  v_deadline timestamptz;
begin
  if new.checkpoint <> 'd7' or new.percentile_performance is null then
    return new;
  end if;

  v_hot := new.percentile_performance <= 35;
  v_deadline := coalesce(new.computed_at, now()) + interval '24 hours';

  if v_hot then
    update public.post_media_assets
    set purge_after = v_deadline,
        updated_at = now(),
        status = case
          when status = 'deleted' then status
          when status in ('pending_capture', 'capturing', 'capture_failed') then status
          when coalesce(storage_path, '') = '' then 'pending_capture'
          else 'active'
        end,
        next_run_at = case
          when status in ('pending_capture', 'capture_failed') then now()
          else next_run_at
        end
    where post_key = new.post_key
      and status <> 'deleted';
  else
    update public.post_media_assets
    set purge_after = now(),
        next_run_at = now(),
        updated_at = now(),
        status = case
          when status = 'deleted' then status
          when coalesce(storage_path, '') = '' then 'deleted'
          else 'purge_pending'
        end,
        deleted_at = case
          when coalesce(storage_path, '') = '' then now()
          else deleted_at
        end
    where post_key = new.post_key
      and status <> 'deleted';
  end if;

  return new;
end;
$$;

drop trigger if exists trg_20_schedule_fire_media_retention_from_d7 on public.post_metrics;
create trigger trg_20_schedule_fire_media_retention_from_d7
after insert or update of checkpoint, percentile_performance, computed_at on public.post_metrics
for each row
execute function public.tg_schedule_fire_media_retention_from_d7();

commit;
