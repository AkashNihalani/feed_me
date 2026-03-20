-- Queue hardening:
-- 1) Prevent duplicate daily enqueue for same feeder/business_date
-- 2) Stop historical checkpoint jobs from becoming immediate due-now floods

begin;

-- Return true checkpoint target time (do not clamp to now).
create or replace function public.fn_checkpoint_due_at(
  p_posted_at timestamptz,
  p_days_after int,
  p_tz text default 'Asia/Kolkata',
  p_hour int default 18,
  p_minute int default 0
)
returns timestamptz
language plpgsql
stable
as $$
declare
  v_local_date date;
  v_target_local timestamp;
  v_target_utc timestamptz;
begin
  if p_posted_at is null then
    return null;
  end if;

  v_local_date := ((p_posted_at at time zone p_tz)::date + greatest(0, p_days_after));
  v_target_local := (v_local_date::timestamp + make_interval(hours => p_hour, mins => p_minute));
  v_target_utc := (v_target_local at time zone p_tz);
  return v_target_utc;
end;
$$;

-- Daily enqueue guard: one daily row per feeder per business day.
create or replace function public.enqueue_daily_jobs(p_run_at timestamptz default now())
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
  v_business_date date := ((p_run_at at time zone 'Asia/Kolkata')::date);
begin
  insert into public.run_jobs (feeder_id, job_type, status, next_run_at, business_date_ist)
  select fd.id, 'daily', 'pending', p_run_at, v_business_date
  from public.feeders fd
  join public.feeds f on f.id = fd.feed_id
  where f.status = 'active'
    and fd.status = 'active'
    and not exists (
      select 1
      from public.run_jobs rj
      where rj.feeder_id = fd.id
        and rj.business_date_ist = v_business_date
        and rj.job_type in ('daily', 'repair')
        and rj.status in ('pending', 'running', 'retry', 'done')
    );

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

-- Checkpoint enqueue guard:
-- - future targets -> pending
-- - past targets   -> skipped (recorded once; never retried/flooded)
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
    case when t.due_at is not null and t.due_at >= now() then 'pending' else 'skipped' end as status,
    coalesce(t.due_at, now()) as next_run_at,
    case
      when t.due_at is null then 'checkpoint skipped: missing posted_at'
      when t.due_at < now() then 'checkpoint skipped: historical checkpoint already passed'
      else null
    end as last_error
  from targets t
  on conflict (post_key, checkpoint) do nothing;

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

-- Backlog cleanup: old overdue checkpoint jobs should not keep creating retry storms.
update public.checkpoint_jobs
set status = 'skipped',
    last_error = 'checkpoint skipped: backlog cleanup after queue hardening',
    updated_at = now()
where status in ('pending', 'retry')
  and checkpoint in ('d3','d7','d21')
  and next_run_at < now() - interval '6 hours';

-- One open scrape lane (daily/repair) per feeder/business_date.
update public.run_jobs
set business_date_ist = (coalesce(next_run_at, created_at, now()) at time zone 'Asia/Kolkata')::date
where business_date_ist is null
  and job_type in ('daily', 'repair');

with ranked as (
  select
    id,
    row_number() over (
      partition by feeder_id, business_date_ist
      order by
        case status when 'running' then 0 when 'retry' then 1 when 'pending' then 2 else 3 end,
        next_run_at asc,
        id asc
    ) as rn
  from public.run_jobs
  where status in ('pending', 'running', 'retry')
    and job_type in ('daily', 'repair')
    and business_date_ist is not null
), dupes as (
  select id from ranked where rn > 1
)
update public.run_jobs rj
set status = 'skipped',
    last_error = 'dedupe: superseded by canonical scrape lane job',
    updated_at = now()
from dupes
where rj.id = dupes.id;

create unique index if not exists run_jobs_open_scrape_lane_unique_idx
on public.run_jobs(feeder_id, business_date_ist)
where status in ('pending', 'running', 'retry')
  and job_type in ('daily', 'repair')
  and business_date_ist is not null;

-- Canonical single-feeder enqueue helper for onboarding flows.
create or replace function public.enqueue_daily_job_for_feeder(
  p_feeder_id bigint,
  p_run_at timestamptz default now()
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
  v_business_date date := ((p_run_at at time zone 'Asia/Kolkata')::date);
begin
  insert into public.run_jobs (feeder_id, job_type, status, next_run_at, business_date_ist)
  select fd.id, 'daily', 'pending', p_run_at, v_business_date
  from public.feeders fd
  join public.feeds f on f.id = fd.feed_id
  where fd.id = p_feeder_id
    and fd.status = 'active'
    and f.status = 'active'
    and not exists (
      select 1
      from public.run_jobs rj
      where rj.feeder_id = fd.id
        and rj.business_date_ist = v_business_date
        and rj.job_type in ('daily', 'repair')
        and rj.status in ('pending', 'running', 'retry', 'done')
    );

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

commit;
