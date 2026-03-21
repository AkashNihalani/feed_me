begin;

create or replace function public.fn_post_media_rollover_deadline(p_posted_at timestamptz)
returns timestamptz
language sql
stable
as $$
  select coalesce(p_posted_at, now()) + interval '8 days';
$$;

update public.post_media_assets assets
set purge_after = public.fn_post_media_rollover_deadline(p.posted_at),
    updated_at = now(),
    status = case
      when assets.status = 'deleted' then assets.status
      when coalesce(assets.storage_path, '') = '' then 'pending_capture'
      else 'active'
    end,
    deleted_at = case
      when assets.status = 'deleted' then coalesce(assets.deleted_at, now())
      else null
    end
from public.posts p
where p.post_key = assets.post_key
  and assets.status <> 'deleted';

create or replace function public.tg_schedule_fire_media_retention_from_d7()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_deadline timestamptz;
begin
  if new.checkpoint <> 'd7' then
    return new;
  end if;

  select public.fn_post_media_rollover_deadline(p.posted_at)
  into v_deadline
  from public.posts p
  where p.post_key = new.post_key;

  update public.post_media_assets
  set purge_after = coalesce(v_deadline, purge_after, public.fn_post_media_rollover_deadline(null)),
      updated_at = now(),
      status = case
        when status = 'deleted' then status
        when coalesce(storage_path, '') = '' then 'pending_capture'
        else 'active'
      end,
      next_run_at = case
        when status in ('pending_capture', 'capture_failed') then now()
        else next_run_at
      end,
      deleted_at = null,
      last_error = null
  where post_key = new.post_key
    and status <> 'deleted';

  return new;
end;
$$;

drop trigger if exists trg_20_schedule_fire_media_retention_from_d7 on public.post_metrics;
create trigger trg_20_schedule_fire_media_retention_from_d7
after insert or update of checkpoint, percentile_performance, computed_at on public.post_metrics
for each row
execute function public.tg_schedule_fire_media_retention_from_d7();

commit;
