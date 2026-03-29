begin;

create or replace function public.enqueue_checkpoint_jobs(
  p_post_key text,
  p_posted_at timestamptz,
  p_tz text default 'Asia/Kolkata',
  p_hour int default 18,
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
begin
  with targets as (
    select 'd1'::text as checkpoint, public.fn_checkpoint_due_at(p_posted_at, 1, p_tz, p_hour, p_minute, p_bucket_minutes) as due_at
    union all
    select 'd3'::text as checkpoint, public.fn_checkpoint_due_at(p_posted_at, 3, p_tz, p_hour, p_minute, p_bucket_minutes) as due_at
    union all
    select 'd7'::text as checkpoint, public.fn_checkpoint_due_at(p_posted_at, 7, p_tz, p_hour, p_minute, p_bucket_minutes) as due_at
    union all
    select 'd21'::text as checkpoint, public.fn_checkpoint_due_at(p_posted_at, 21, p_tz, p_hour, p_minute, p_bucket_minutes) as due_at
  )
  insert into public.checkpoint_jobs (post_key, checkpoint, status, next_run_at, last_error)
  select
    p_post_key,
    checkpoint,
    case
      when due_at is null then 'skipped'
      when due_at + make_interval(mins => greatest(1, coalesce(p_bucket_minutes, 60))) <= now() then 'skipped'
      else 'pending'
    end,
    coalesce(due_at, now()),
    case
      when due_at is null then 'checkpoint skipped: missing posted_at'
      when due_at + make_interval(mins => greatest(1, coalesce(p_bucket_minutes, 60))) <= now()
        then 'checkpoint skipped: eligibility window already passed'
      else null
    end
  from targets
  on conflict (post_key, checkpoint) do update
    set status = case
        when excluded.status = 'skipped' then public.checkpoint_jobs.status
        when public.checkpoint_jobs.status in ('done', 'running') then public.checkpoint_jobs.status
        else 'pending'
      end,
      next_run_at = case
        when excluded.status = 'skipped' then public.checkpoint_jobs.next_run_at
        when public.checkpoint_jobs.status in ('done', 'running') then public.checkpoint_jobs.next_run_at
        else excluded.next_run_at
      end,
      last_error = case
        when excluded.status = 'skipped' then public.checkpoint_jobs.last_error
        when public.checkpoint_jobs.status in ('done', 'running') then public.checkpoint_jobs.last_error
        else null
      end,
      updated_at = now()
    where public.checkpoint_jobs.status not in ('done', 'running')
      and excluded.status <> 'skipped';

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

drop function if exists public.enqueue_checkpoint_jobs(text, timestamptz);

create or replace function public.enqueue_checkpoint_jobs(
  p_post_key text,
  p_posted_at timestamptz
)
returns int
language sql
security definer
set search_path = public
as $$
  select public.enqueue_checkpoint_jobs(
    p_post_key,
    p_posted_at,
    'Asia/Kolkata',
    23,
    30,
    60
  )
$$;

with job_windows as (
  select
    cj.id,
    public.fn_checkpoint_due_at(
      p.posted_at,
      case cj.checkpoint
        when 'd1' then 1
        when 'd3' then 3
        when 'd7' then 7
        when 'd21' then 21
        else null
      end,
      'Asia/Kolkata',
      23,
      30,
      60
    ) + interval '60 minutes' as window_end
  from public.checkpoint_jobs cj
  join public.posts p on p.post_key = cj.post_key
  where cj.status in ('pending', 'retry')
)
update public.checkpoint_jobs cj
set status = 'skipped',
    last_error = 'checkpoint skipped: eligibility window already passed',
    updated_at = now()
from job_windows jw
where cj.id = jw.id
  and jw.window_end is not null
  and jw.window_end <= now();

commit;
