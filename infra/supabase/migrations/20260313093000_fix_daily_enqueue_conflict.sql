begin;

-- Legacy open-lane unique index (feeder_id, job_type) can block new-day enqueue
-- whenever any old pending/retry row exists. Canonical uniqueness is by
-- (feeder_id, business_date_ist) in run_jobs_open_scrape_lane_unique_idx.
drop index if exists public.run_jobs_open_unique_idx;

create or replace function public.enqueue_daily_jobs(p_run_at timestamptz default now())
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
  v_business_date date := ((p_run_at at time zone 'Asia/Kolkata')::date - 1);
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
    )
  on conflict do nothing;

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

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
  v_business_date date := ((p_run_at at time zone 'Asia/Kolkata')::date - 1);
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
    )
  on conflict do nothing;

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

commit;
