begin;

create or replace function public.claim_post_media_assets_for_purge(p_limit integer default 50)
returns setof public.post_media_assets
language sql
security definer
set search_path = public
as $function$
  with picked as (
    select id
    from public.post_media_assets
    where (
        status in ('active', 'purge_pending')
        or (
          status = 'purge_failed'
          and coalesce(next_run_at, now()) <= now()
        )
        or (
          status = 'purging'
          and updated_at <= now() - interval '5 minutes'
        )
      )
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
$function$;

commit;
