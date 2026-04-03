begin;

create or replace function public.fn_post_media_rollover_deadline(
  p_posted_at timestamptz,
  p_asset_role text
)
returns timestamptz
language sql
stable
as $$
  select coalesce(p_posted_at, now()) + interval '8 days';
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
begin
  delete from public.fire_alerts fa
  where fa.business_date_ist is not null
    and fa.business_date_ist <= (v_today_ist - 7)
    and fa.signal_code = 'slot_v3'
    and fa.context = 'own';

  get diagnostics v_fire_rows = row_count;

  delete from public.web_push_jobs jobs
  where jobs.fire_alert_id is null
    and jobs.status in ('sent', 'failed', 'skipped')
    and coalesce(jobs.updated_at, jobs.created_at) <= now() - interval '8 days';

  get diagnostics v_push_rows = row_count;

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
  and assets.status in ('pending_capture', 'capturing', 'active', 'capture_failed', 'purge_pending', 'purging', 'purge_failed')
  and (
    assets.purge_after is null
    or assets.purge_after > public.fn_post_media_rollover_deadline(
      coalesce(p.posted_at, assets.captured_at, assets.created_at, now()),
      assets.asset_role
    )
  );

select public.prune_expired_fire_state();

commit;
