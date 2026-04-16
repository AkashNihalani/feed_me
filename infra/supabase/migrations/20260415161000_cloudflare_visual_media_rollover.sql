begin;

create or replace function public.fn_post_media_rollover_deadline(
  p_posted_at timestamptz,
  p_asset_role text
)
returns timestamptz
language sql
stable
as $$
  select (
    case
      when lower(coalesce(p_asset_role, '')) in ('thumbnail', 'display')
        then coalesce(p_posted_at, now()) + interval '90 days'
      when lower(coalesce(p_asset_role, '')) = 'preview_5s'
        then greatest(
          coalesce(p_posted_at, now()),
          timestamptz '2026-04-14 00:00:00+05:30'
        ) + interval '8 days'
      else coalesce(p_posted_at, now()) + interval '1 day'
    end
  );
$$;

create or replace function public.fn_post_media_rollover_deadline_for_post(
  p_post_key text,
  p_posted_at timestamptz,
  p_asset_role text
)
returns timestamptz
language sql
stable
as $$
  with hot_d7 as (
    select exists (
      select 1
      from public.post_metrics pm
      where pm.post_key = p_post_key
        and lower(coalesce(pm.checkpoint, '')) = 'd7'
        and public.fn_is_hot_percentile(pm.percentile_performance)
    ) as is_hot
  )
  select
    case
      when lower(coalesce(p_asset_role, '')) = 'preview_5s' and (select is_hot from hot_d7)
        then greatest(
          coalesce(p_posted_at, now()),
          timestamptz '2026-04-14 00:00:00+05:30'
        ) + interval '30 days'
      else public.fn_post_media_rollover_deadline(p_posted_at, p_asset_role)
    end;
$$;

create or replace function public.tg_schedule_fire_media_retention_from_d7()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if new.checkpoint not in ('d7', 'd21') then
    return new;
  end if;

  update public.post_media_assets assets
  set purge_after = public.fn_post_media_rollover_deadline_for_post(assets.post_key, p.posted_at, assets.asset_role),
      updated_at = now(),
      status = case
        when assets.status = 'deleted' then assets.status
        when coalesce(assets.storage_path, '') = '' then 'pending_capture'
        else 'active'
      end,
      next_run_at = case
        when assets.status in ('pending_capture', 'capture_failed') then now()
        else assets.next_run_at
      end,
      deleted_at = null,
      last_error = null
  from public.posts p
  where p.post_key = assets.post_key
    and assets.post_key = new.post_key
    and assets.status <> 'deleted'
    and lower(coalesce(assets.asset_role, '')) in ('thumbnail', 'display', 'preview_5s');

  return new;
end;
$$;

update public.post_media_assets assets
set purge_after = public.fn_post_media_rollover_deadline_for_post(
      assets.post_key,
      coalesce(p.posted_at, assets.captured_at, assets.created_at, now()),
      assets.asset_role
    ),
    updated_at = now()
from public.posts p
where p.post_key = assets.post_key
  and assets.status in ('pending_capture', 'capturing', 'active', 'capture_failed', 'purge_pending', 'purging', 'purge_failed')
  and lower(coalesce(assets.asset_role, '')) in ('thumbnail', 'display', 'preview_5s')
  and assets.purge_after is distinct from public.fn_post_media_rollover_deadline_for_post(
    assets.post_key,
    coalesce(p.posted_at, assets.captured_at, assets.created_at, now()),
    assets.asset_role
  );

update public.post_media_assets assets
set purge_after = now() - interval '1 minute',
    status = case
      when coalesce(assets.storage_path, '') <> '' then 'purge_pending'
      else 'deleted'
    end,
    attempt = 0,
    next_run_at = now(),
    deleted_at = case
      when coalesce(assets.storage_path, '') <> '' then null
      else coalesce(assets.deleted_at, now())
    end,
    last_error = 'cleanup: retired legacy full-media asset',
    updated_at = now()
where assets.status <> 'deleted'
  and (
    lower(coalesce(assets.asset_role, '')) = 'video'
    or lower(coalesce(assets.asset_role, '')) like 'carousel_%'
  );

commit;
