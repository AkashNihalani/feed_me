begin;

create or replace function public.fn_post_media_rollover_deadline(
  p_posted_at timestamptz,
  p_asset_role text
)
returns timestamptz
language sql
stable
as $$
  select coalesce(p_posted_at, now()) +
    case
      when lower(coalesce(p_asset_role, '')) in ('thumbnail', 'display')
        or lower(coalesce(p_asset_role, '')) like 'carousel_%'
        then interval '30 days'
      else interval '8 days'
    end;
$$;

update public.post_media_assets assets
set purge_after = public.fn_post_media_rollover_deadline(
      coalesce(p.posted_at, assets.captured_at, assets.created_at, now()),
      assets.asset_role
    ),
    updated_at = now()
from public.posts p
where p.post_key = assets.post_key
  and assets.status in ('pending_capture', 'capturing', 'active', 'capture_failed', 'purge_pending', 'purging', 'purge_failed')
  and (
    assets.purge_after is distinct from public.fn_post_media_rollover_deadline(
      coalesce(p.posted_at, assets.captured_at, assets.created_at, now()),
      assets.asset_role
    )
  );

commit;
