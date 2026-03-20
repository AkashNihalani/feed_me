-- Freeze daily run date (IST) and add repair lane for missed-day backfills

alter table public.run_jobs
  add column if not exists business_date_ist date default ((now() at time zone 'Asia/Kolkata')::date);

-- Expand job types to include repair jobs
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'run_jobs_job_type_check'
      AND conrelid = 'public.run_jobs'::regclass
  ) THEN
    ALTER TABLE public.run_jobs DROP CONSTRAINT run_jobs_job_type_check;
  END IF;
END $$;

ALTER TABLE public.run_jobs
  ADD CONSTRAINT run_jobs_job_type_check
  CHECK (job_type in ('daily','weekly','repair'));

create index if not exists run_jobs_business_date_idx
  on public.run_jobs(business_date_ist, job_type, status, next_run_at);

-- Keep daily enqueue setting the frozen business date for that run
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
  where f.status='active' and fd.status='active'
  on conflict do nothing;
  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

-- Mark unresolved daily jobs for the specified day as failed (for repair handoff)
create or replace function public.finalize_daily_jobs_for_day(p_business_date date)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
begin
  update public.run_jobs
  set status='failed',
      last_error=coalesce(last_error, 'Daily window cutoff reached; moved to repair lane'),
      updated_at=now()
  where job_type='daily'
    and business_date_ist = p_business_date
    and status in ('pending','retry');
  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

-- Enqueue repair jobs for feeders that failed on the previous business day
create or replace function public.enqueue_repair_jobs_from_previous_day(p_tz text default 'Asia/Kolkata')
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_day date := ((now() at time zone p_tz)::date - 1);
  v_finalized int := 0;
  v_enqueued int := 0;
begin
  v_finalized := public.finalize_daily_jobs_for_day(v_day);

  insert into public.run_jobs (feeder_id, job_type, status, next_run_at, business_date_ist, attempt, last_error)
  select distinct rj.feeder_id, 'repair', 'pending', now(), v_day, 0, null
  from public.run_jobs rj
  join public.feeders fd on fd.id = rj.feeder_id
  join public.feeds f on f.id = fd.feed_id
  where rj.job_type='daily'
    and rj.business_date_ist = v_day
    and rj.status='failed'
    and fd.status='active'
    and f.status='active'
    and not exists (
      select 1
      from public.run_jobs e
      where e.feeder_id = rj.feeder_id
        and e.business_date_ist = v_day
        and e.job_type in ('daily','repair')
        and e.status in ('pending','running','retry','done')
    );

  get diagnostics v_enqueued = row_count;

  return jsonb_build_object(
    'business_date_ist', v_day,
    'finalized_daily_jobs', v_finalized,
    'repair_jobs_enqueued', v_enqueued
  );
end;
$$;

-- 05:05 IST = 23:35 UTC previous day; run repair-lane handoff once per day
DO $$
BEGIN
  IF to_regnamespace('cron') IS NOT NULL THEN
    PERFORM cron.unschedule(j.jobid)
    FROM cron.job j
    WHERE j.jobname = 'feedme_repair_lane_0505_ist';

    PERFORM cron.schedule(
      'feedme_repair_lane_0505_ist',
      '35 23 * * *',
      $q$select public.enqueue_repair_jobs_from_previous_day('Asia/Kolkata');$q$
    );
  END IF;
END $$;
