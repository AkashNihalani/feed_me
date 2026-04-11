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
        then interval '24 days'
      else interval '1 day'
    end;
$$;

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
      and lower(coalesce(asset_role, '')) = 'thumbnail'
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

create or replace function public.tg_schedule_fire_media_retention_from_d7()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if new.checkpoint <> 'd7' then
    return new;
  end if;

  update public.post_media_assets assets
  set purge_after = public.fn_post_media_rollover_deadline(p.posted_at, assets.asset_role),
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
    and lower(coalesce(assets.asset_role, '')) = 'thumbnail';

  return new;
end;
$$;

create or replace function public.prune_expired_fire_state()
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_today_ist date := (timezone('Asia/Kolkata', now()))::date;
  v_fire_rows int := 0;
  v_push_rows int := 0;
  v_media_rows int := 0;
  v_run_rows int := 0;
  v_checkpoint_rows int := 0;
begin
  delete from public.fire_alerts fa
  where fa.business_date_ist is not null
    and fa.business_date_ist <= (v_today_ist - 14);

  get diagnostics v_fire_rows = row_count;

  delete from public.web_push_jobs jobs
  where jobs.status in ('sent', 'failed', 'skipped')
    and coalesce(jobs.updated_at, jobs.created_at) <= now() - interval '14 days';

  get diagnostics v_push_rows = row_count;

  delete from public.run_jobs jobs
  where jobs.status in ('done', 'failed', 'skipped')
    and coalesce(jobs.updated_at, jobs.created_at) <= now() - interval '30 days';

  get diagnostics v_run_rows = row_count;

  delete from public.checkpoint_jobs jobs
  where jobs.status in ('done', 'failed', 'skipped')
    and coalesce(jobs.updated_at, jobs.created_at) <= now() - interval '30 days';

  get diagnostics v_checkpoint_rows = row_count;

  delete from public.post_media_assets assets
  where assets.status = 'deleted'
     or (
       assets.status = 'capture_failed'
       and assets.purge_after is not null
       and assets.purge_after <= now()
       and coalesce(assets.storage_path, '') = ''
     );

  get diagnostics v_media_rows = row_count;

  return jsonb_build_object(
    'fire_alerts_deleted', v_fire_rows,
    'web_push_jobs_deleted', v_push_rows,
    'run_jobs_deleted', v_run_rows,
    'checkpoint_jobs_deleted', v_checkpoint_rows,
    'post_media_assets_deleted', v_media_rows
  );
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
  and lower(coalesce(assets.asset_role, '')) = 'thumbnail'
  and assets.status in ('pending_capture', 'capturing', 'active', 'capture_failed', 'purge_pending', 'purging', 'purge_failed');

update public.post_media_assets assets
set purge_after = now() - interval '1 minute',
    status = case
      when coalesce(assets.storage_path, '') <> '' then 'purge_pending'
      else 'deleted'
    end,
    next_run_at = now(),
    deleted_at = case
      when coalesce(assets.storage_path, '') <> '' then null
      else coalesce(assets.deleted_at, now())
    end,
    last_error = 'cleanup: retired non-thumbnail cache',
    updated_at = now()
from public.posts p
where p.post_key = assets.post_key
  and lower(coalesce(assets.asset_role, '')) <> 'thumbnail'
  and assets.status <> 'deleted';

do $$
declare
  r record;
begin
  if not exists (
    select 1
    from pg_namespace
    where nspname = 'cron'
  ) then
    return;
  end if;

  for r in
    select jobid
    from cron.job
    where jobname in (
      'feedme_enqueue_poll_1205_ist',
      'feedme_enqueue_poll_1230_ist_watchdog'
    )
  loop
    perform cron.unschedule(r.jobid);
  end loop;
end;
$$;

select public.prune_expired_fire_state();

commit;
