create or replace function public.claim_post_media_assets_for_capture(p_limit integer default 25)
returns setof public.post_media_assets
language sql
security definer
set search_path = public
as $function$
  with picked as (
    select id
    from public.post_media_assets
    where (
        status in ('pending_capture', 'capture_failed')
        or (
          status = 'capturing'
          and updated_at <= now() - interval '5 minutes'
        )
      )
      and (
        lower(coalesce(asset_role, '')) in ('thumbnail', 'display', 'preview_5s')
        or lower(coalesce(asset_role, '')) like 'carousel_%'
      )
      and next_run_at <= now()
      and coalesce(source_url, '') <> ''
      and (purge_after is null or purge_after > now())
    order by
      case
        when lower(coalesce(asset_role, '')) = 'thumbnail' then 0
        when lower(coalesce(asset_role, '')) = 'display' then 1
        when lower(coalesce(asset_role, '')) like 'carousel_%' then 2
        when lower(coalesce(asset_role, '')) = 'preview_5s' then 3
        else 9
      end,
      next_run_at asc,
      created_at asc,
      id asc
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
$function$;
