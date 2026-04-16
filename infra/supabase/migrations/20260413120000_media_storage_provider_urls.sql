alter table public.post_media_assets
  add column if not exists storage_provider text not null default 'supabase',
  add column if not exists public_url text;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'post_media_assets_storage_provider_chk'
      and conrelid = 'public.post_media_assets'::regclass
  ) then
    alter table public.post_media_assets
      add constraint post_media_assets_storage_provider_chk
      check (storage_provider in ('supabase', 'r2'));
  end if;
end $$;
