begin;

alter table if exists public.run_jobs
  drop constraint if exists run_jobs_job_type_check;

alter table if exists public.run_jobs
  add constraint run_jobs_job_type_check
  check (job_type in ('daily', 'repair', 'poll', 'followers'));

create unique index if not exists run_jobs_open_followers_lane_unique_idx
  on public.run_jobs(feeder_id, business_date_ist)
  where status in ('pending', 'running', 'retry')
    and job_type = 'followers'
    and business_date_ist is not null;

create or replace function public.enqueue_poll_job_for_feeder(
  p_feeder_id bigint,
  p_run_at timestamptz default now()
)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_count integer := 0;
  v_business_date date := ((p_run_at at time zone 'Asia/Kolkata')::date);
begin
  insert into public.run_jobs (feeder_id, job_type, status, next_run_at, business_date_ist)
  select
    fd.id,
    'poll',
    'pending',
    p_run_at,
    v_business_date
  from public.feeders fd
  join public.feeds f on f.id = fd.feed_id
  where fd.id = p_feeder_id
    and fd.status = 'active'
    and f.status = 'active'
    and not exists (
      select 1
      from public.run_jobs rj
      where rj.feeder_id = fd.id
        and rj.job_type = 'poll'
        and rj.business_date_ist = v_business_date
        and rj.status in ('pending', 'running', 'retry', 'done')
    )
  on conflict do nothing;

  get diagnostics v_count = row_count;
  return v_count;
end;
$$;

create or replace function public.enqueue_weekly_follower_jobs(
  p_run_at timestamptz default now()
)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_count integer := 0;
  v_week_start date := date_trunc('week', timezone('Asia/Kolkata', p_run_at))::date;
begin
  insert into public.run_jobs (feeder_id, job_type, status, next_run_at, business_date_ist)
  select
    fd.id,
    'followers',
    'pending',
    p_run_at,
    v_week_start
  from public.feeders fd
  join public.feeds f on f.id = fd.feed_id
  where fd.status = 'active'
    and f.status = 'active'
    and not exists (
      select 1
      from public.run_jobs rj
      where rj.feeder_id = fd.id
        and rj.job_type = 'followers'
        and rj.business_date_ist = v_week_start
        and rj.status in ('pending', 'running', 'retry', 'done')
    )
  on conflict do nothing;

  get diagnostics v_count = row_count;
  return v_count;
end;
$$;

create or replace function public.enqueue_weekly_follower_job_for_feeder(
  p_feeder_id bigint,
  p_run_at timestamptz default now()
)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_count integer := 0;
  v_week_start date := date_trunc('week', timezone('Asia/Kolkata', p_run_at))::date;
begin
  insert into public.run_jobs (feeder_id, job_type, status, next_run_at, business_date_ist)
  select
    fd.id,
    'followers',
    'pending',
    p_run_at,
    v_week_start
  from public.feeders fd
  join public.feeds f on f.id = fd.feed_id
  where fd.id = p_feeder_id
    and fd.status = 'active'
    and f.status = 'active'
    and not exists (
      select 1
      from public.run_jobs rj
      where rj.feeder_id = fd.id
        and rj.job_type = 'followers'
        and rj.business_date_ist = v_week_start
        and rj.status in ('pending', 'running', 'retry', 'done')
    )
  on conflict do nothing;

  get diagnostics v_count = row_count;
  return v_count;
end;
$$;

create or replace function public.bootstrap_feeder_jobs(
  p_feeder_id bigint,
  p_run_at timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_daily integer := 0;
  v_followers integer := 0;
begin
  v_daily := coalesce(public.enqueue_daily_job_for_feeder(p_feeder_id, p_run_at), 0);
  v_followers := coalesce(public.enqueue_weekly_follower_job_for_feeder(p_feeder_id, p_run_at), 0);

  return jsonb_build_object(
    'daily', v_daily,
    'followers', v_followers
  );
end;
$$;

do $$
declare
  r record;
begin
  if to_regnamespace('cron') is null then
    raise notice 'pg_cron schema not found. Enable pg_cron and rerun migration.';
    return;
  end if;

  for r in
    select jobid
    from cron.job
    where jobname in (
      'feedme_enqueue_weekly_followers_mon_0010_ist',
      'feedme_enqueue_weekly_followers_mon_0030_ist_watchdog'
    )
  loop
    perform cron.unschedule(r.jobid);
  end loop;

  perform cron.schedule(
    'feedme_enqueue_weekly_followers_mon_0010_ist',
    '40 18 * * 0',
    $q$select public.enqueue_weekly_follower_jobs(now());$q$
  );

  perform cron.schedule(
    'feedme_enqueue_weekly_followers_mon_0030_ist_watchdog',
    '0 19 * * 0',
    $q$select public.enqueue_weekly_follower_jobs(now());$q$
  );
end $$;

commit;
