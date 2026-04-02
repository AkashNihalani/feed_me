begin;

create or replace function public.enqueue_checkpoint_jobs(
  p_post_key text,
  p_posted_at timestamptz,
  p_tz text default 'Asia/Kolkata',
  p_hour int default 23,
  p_minute int default 30,
  p_bucket_minutes int default 60
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
  v_due_at timestamptz;
  v_tracking_started_at timestamptz;
begin
  if p_posted_at is null then
    return 0;
  end if;

  select fd.created_at
  into v_tracking_started_at
  from public.posts p
  join public.feeders fd on fd.id = p.feeder_id
  where p.post_key = p_post_key;

  if v_tracking_started_at is not null and p_posted_at < v_tracking_started_at then
    return 0;
  end if;

  if p_posted_at < (now() - interval '7 days') then
    return 0;
  end if;

  v_due_at := public.fn_checkpoint_due_at(p_posted_at, 1, p_tz, p_hour, p_minute, p_bucket_minutes);
  if v_due_at is null then
    return 0;
  end if;

  insert into public.checkpoint_jobs (post_key, checkpoint, status, next_run_at, last_error)
  values (
    p_post_key,
    'd1',
    'pending',
    v_due_at,
    null
  )
  on conflict (post_key, checkpoint) do update
    set status = case
        when public.checkpoint_jobs.status in ('done', 'running') then public.checkpoint_jobs.status
        else 'pending'
      end,
      next_run_at = case
        when public.checkpoint_jobs.status in ('done', 'running') then public.checkpoint_jobs.next_run_at
        else excluded.next_run_at
      end,
      last_error = case
        when public.checkpoint_jobs.status in ('done', 'running') then public.checkpoint_jobs.last_error
        else null
      end,
      updated_at = now()
    where public.checkpoint_jobs.status not in ('done', 'running');

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

delete from public.posts p
using public.feeders fd
where fd.id = p.feeder_id
  and p.posted_at is not null
  and p.posted_at < fd.created_at;

commit;
