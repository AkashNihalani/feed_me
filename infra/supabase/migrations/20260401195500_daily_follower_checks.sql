begin;

create or replace function public.enqueue_daily_follower_jobs(
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
    'followers',
    'pending',
    p_run_at,
    v_business_date
  from public.feeders fd
  join public.feeds f on f.id = fd.feed_id
  where fd.status = 'active'
    and f.status = 'active'
    and not exists (
      select 1
      from public.run_jobs rj
      where rj.feeder_id = fd.id
        and rj.job_type = 'followers'
        and rj.business_date_ist = v_business_date
        and rj.status in ('pending', 'running', 'retry', 'done')
    )
  on conflict do nothing;

  get diagnostics v_count = row_count;
  return v_count;
end;
$$;

create or replace function public.enqueue_daily_follower_job_for_feeder(
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
    'followers',
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
        and rj.job_type = 'followers'
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
language sql
security definer
set search_path = public
as $$
  select public.enqueue_daily_follower_jobs(p_run_at);
$$;

create or replace function public.enqueue_weekly_follower_job_for_feeder(
  p_feeder_id bigint,
  p_run_at timestamptz default now()
)
returns integer
language sql
security definer
set search_path = public
as $$
  select public.enqueue_daily_follower_job_for_feeder(p_feeder_id, p_run_at);
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
  v_followers := coalesce(public.enqueue_daily_follower_job_for_feeder(p_feeder_id, p_run_at), 0);

  return jsonb_build_object(
    'daily', v_daily,
    'followers', v_followers
  );
end;
$$;

create or replace function public.tg_capture_feeder_follower_snapshot()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_business_date date := (timezone('Asia/Kolkata', now()))::date;
  v_count bigint := greatest(0, coalesce(new.follower_count, 0));
begin
  insert into public.feeder_follower_snapshots (feeder_id, week_start_ist, follower_count, captured_at)
  values (new.id, v_business_date, v_count, now())
  on conflict (feeder_id, week_start_ist) do update
    set follower_count = excluded.follower_count,
        captured_at = excluded.captured_at;

  return new;
end;
$$;

insert into public.feeder_follower_snapshots (feeder_id, week_start_ist, follower_count, captured_at)
select
  f.id,
  (timezone('Asia/Kolkata', now()))::date,
  greatest(0, coalesce(f.follower_count, 0))::bigint,
  now()
from public.feeders f
on conflict (feeder_id, week_start_ist) do update
  set follower_count = excluded.follower_count,
      captured_at = excluded.captured_at;

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
      'feedme_enqueue_weekly_followers_mon_0030_ist_watchdog',
      'feedme_enqueue_daily_followers_0010_ist',
      'feedme_enqueue_daily_followers_0030_ist_watchdog'
    )
  loop
    perform cron.unschedule(r.jobid);
  end loop;

  perform cron.schedule(
    'feedme_enqueue_daily_followers_0010_ist',
    '40 18 * * *',
    $q$select public.enqueue_daily_follower_jobs(now());$q$
  );

  perform cron.schedule(
    'feedme_enqueue_daily_followers_0030_ist_watchdog',
    '0 19 * * *',
    $q$select public.enqueue_daily_follower_jobs(now());$q$
  );
end $$;

commit;
