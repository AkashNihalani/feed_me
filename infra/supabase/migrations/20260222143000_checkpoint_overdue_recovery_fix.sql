-- Permanent checkpoint recovery fix:
-- 1) Overdue checkpoints are queued as pending (not skipped)
-- 2) Existing skipped backlog checkpoints are revived in a bounded recovery window
-- 3) Explicit helper to revive backlog on demand

begin;

create or replace function public.enqueue_checkpoint_jobs(
  p_post_key text,
  p_posted_at timestamptz,
  p_tz text default 'Asia/Kolkata',
  p_hour int default 18,
  p_minute int default 0
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
    select 'd3'::text as checkpoint, public.fn_checkpoint_due_at(p_posted_at, 3, p_tz, p_hour, p_minute) as due_at
    union all
    select 'd7'::text as checkpoint, public.fn_checkpoint_due_at(p_posted_at, 7, p_tz, p_hour, p_minute) as due_at
    union all
    select 'd21'::text as checkpoint, public.fn_checkpoint_due_at(p_posted_at, 21, p_tz, p_hour, p_minute) as due_at
  )
  insert into public.checkpoint_jobs (post_key, checkpoint, status, next_run_at, last_error)
  select
    p_post_key,
    t.checkpoint,
    case when t.due_at is null then 'skipped' else 'pending' end as status,
    case
      when t.due_at is null then now()
      else greatest(t.due_at, now())
    end as next_run_at,
    case
      when t.due_at is null then 'checkpoint skipped: missing posted_at'
      else null
    end as last_error
  from targets t
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

create or replace function public.fn_revive_checkpoint_backlog(p_window_days int default 5)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
begin
  update public.checkpoint_jobs
  set status = 'pending',
      next_run_at = now(),
      last_error = null,
      updated_at = now()
  where checkpoint in ('d3','d7','d21')
    and status = 'skipped'
    and coalesce(last_error, '') in (
      'checkpoint skipped: historical checkpoint already passed',
      'checkpoint skipped: backlog cleanup after queue hardening'
    )
    and next_run_at >= now() - make_interval(days => greatest(1, p_window_days));

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

-- One-time revival for recent backlog affected by queue hardening.
select public.fn_revive_checkpoint_backlog(5);

commit;
