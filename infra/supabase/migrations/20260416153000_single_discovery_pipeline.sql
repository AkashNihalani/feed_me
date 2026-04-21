begin;

create or replace function public.enqueue_poll_jobs(
  p_run_at timestamptz default now()
)
returns integer
language sql
security definer
set search_path = public
as $$
  select 0::integer;
$$;

create or replace function public.enqueue_poll_job_for_feeder(
  p_feeder_id bigint,
  p_run_at timestamptz default now()
)
returns integer
language sql
security definer
set search_path = public
as $$
  select 0::integer;
$$;

create or replace function public.enqueue_daily_follower_jobs(
  p_run_at timestamptz default now()
)
returns integer
language sql
security definer
set search_path = public
as $$
  select 0::integer;
$$;

create or replace function public.enqueue_daily_follower_job_for_feeder(
  p_feeder_id bigint,
  p_run_at timestamptz default now()
)
returns integer
language sql
security definer
set search_path = public
as $$
  select 0::integer;
$$;

create or replace function public.enqueue_weekly_follower_jobs(
  p_run_at timestamptz default now()
)
returns integer
language sql
security definer
set search_path = public
as $$
  select 0::integer;
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
  select 0::integer;
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
begin
  v_daily := coalesce(public.enqueue_daily_job_for_feeder(p_feeder_id, p_run_at), 0);

  return jsonb_build_object(
    'daily', v_daily,
    'followers', 0,
    'poll', 0
  );
end;
$$;

update public.run_jobs
set status = 'skipped',
    last_error = 'retired: single daily discovery pipeline',
    updated_at = now()
where job_type in ('poll', 'followers')
  and status in ('pending', 'retry');

do $$
declare
  r record;
begin
  if to_regnamespace('cron') is null then
    return;
  end if;

  for r in
    select jobid
    from cron.job
    where jobname in (
      'feedme_enqueue_poll_1205_ist',
      'feedme_enqueue_poll_1230_ist_watchdog',
      'feedme_enqueue_weekly_followers_mon_0010_ist',
      'feedme_enqueue_weekly_followers_mon_0030_ist_watchdog',
      'feedme_enqueue_daily_followers_0010_ist',
      'feedme_enqueue_daily_followers_0030_ist_watchdog'
    )
  loop
    perform cron.unschedule(r.jobid);
  end loop;
end;
$$;

commit;
