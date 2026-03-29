-- FeedMe Bright Data schedule source of truth
-- -------------------------------------------
-- This file is the single readable contract for ingestion timing while
-- computation + fire alert logic remain unchanged.
--
-- Discovery:
--   00:05 IST  -> enqueue daily feeder discovery jobs
--   00:30 IST  -> idempotent daily watchdog
--   00:45 IST  -> repair watchdog for previous-day gaps
--   12:05 IST  -> enqueue poll feeder discovery jobs
--   12:30 IST  -> idempotent poll watchdog
--   feeder add -> enqueue immediate discovery bootstrap job
--   worker     -> Bright Data profile snapshot scrape with a 2-day overlap
--
-- Followers:
--   feeder add      -> enqueue immediate weekly follower snapshot job
--   Monday 00:10 IST -> enqueue weekly follower refresh jobs
--   Monday 00:30 IST -> idempotent weekly follower watchdog
--
-- Checkpoints:
--   D1 / D3 / D7 / D21 are scheduled from exact post age
--   next_run_at stays at the exact due timestamp
--   the worker may collect any jobs that become due within the next
--   60 minutes, which keeps checkpoint fetches batchable while staying
--   fair across timezones and strict about checkpoint age
--   official checkpoint rows are ignored if they land before the true age floor
--   (24h / 72h / 168h / 504h) or after the one-hour post-due buffer,
--   even if a legacy writer attempts them
--   overdue checkpoints keep their original due timestamp instead of being
--   restamped to "now", so Fire day assignment always follows true checkpoint day
--
-- Late discovery:
--   preserved via the repair lane and overdue checkpoint catch-up

begin;

create or replace function public.fn_checkpoint_due_at(
  p_posted_at timestamptz,
  p_days_after int,
  p_tz text default 'Asia/Kolkata',
  p_hour int default 18,
  p_minute int default 30,
  p_bucket_minutes int default 60
)
returns timestamptz
language plpgsql
stable
as $$
declare
  v_target_utc timestamptz;
begin
  if p_posted_at is null then
    return null;
  end if;

  v_target_utc := p_posted_at + make_interval(days => greatest(0, coalesce(p_days_after, 0)));
  return v_target_utc;
end;
$$;

do $$
begin
  if exists (
    select 1
    from pg_constraint
    where conname = 'checkpoint_jobs_checkpoint_check'
      and conrelid = 'public.checkpoint_jobs'::regclass
  ) then
    alter table public.checkpoint_jobs
      drop constraint checkpoint_jobs_checkpoint_check;
  end if;

  alter table public.checkpoint_jobs
    add constraint checkpoint_jobs_checkpoint_check
    check (checkpoint in ('d1', 'd3', 'd7', 'd21'));
end $$;

create or replace function public.enqueue_checkpoint_jobs(
  p_post_key text,
  p_posted_at timestamptz,
  p_tz text,
  p_hour int,
  p_minute int,
  p_bucket_minutes int
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
    t.checkpoint,
    case when t.due_at is null then 'skipped' else 'pending' end as status,
    coalesce(t.due_at, now()) as next_run_at,
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

drop function if exists public.enqueue_checkpoint_jobs(text, timestamptz, text, integer, integer);

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

create or replace function public.enqueue_poll_jobs(p_run_at timestamptz default now())
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
  where fd.status = 'active'
    and f.status = 'active'
    and not exists (
      select 1
      from public.run_jobs rj
      where rj.feeder_id = fd.id
        and rj.job_type = 'poll'
        and rj.business_date_ist = v_business_date
        and rj.status in ('pending', 'running', 'retry', 'done')
    );

  get diagnostics v_count = row_count;
  return v_count;
end;
$$;

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
    );

  get diagnostics v_count = row_count;
  return v_count;
end;
$$;

create unique index if not exists run_jobs_open_poll_lane_unique_idx
on public.run_jobs(feeder_id, business_date_ist)
where status in ('pending', 'running', 'retry')
  and job_type = 'poll'
  and business_date_ist is not null;

create unique index if not exists run_jobs_open_followers_lane_unique_idx
on public.run_jobs(feeder_id, business_date_ist)
where status in ('pending', 'running', 'retry')
  and job_type = 'followers'
  and business_date_ist is not null;

create or replace function public.enqueue_weekly_follower_jobs(p_run_at timestamptz default now())
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
    );

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
    );

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
      'feedme_enqueue_daily_2330_ist_only',
      'feedme_enqueue_daily_0005_ist',
      'feedme_enqueue_daily_0030_ist_watchdog',
      'feedme_repair_lane_0045_ist_watchdog',
      'feedme_enqueue_poll_1205_ist',
      'feedme_enqueue_poll_1230_ist_watchdog',
      'feedme_enqueue_weekly_followers_mon_0010_ist',
      'feedme_enqueue_weekly_followers_mon_0030_ist_watchdog'
    )
  loop
    perform cron.unschedule(r.jobid);
  end loop;

  perform cron.schedule(
    'feedme_enqueue_daily_0005_ist',
    '35 18 * * *',
    $q$select public.enqueue_daily_jobs(now());$q$
  );

  perform cron.schedule(
    'feedme_enqueue_daily_0030_ist_watchdog',
    '0 19 * * *',
    $q$select public.enqueue_daily_jobs(now());$q$
  );

  perform cron.schedule(
    'feedme_repair_lane_0045_ist_watchdog',
    '15 19 * * *',
    $q$select public.enqueue_repair_jobs_from_previous_day('Asia/Kolkata');$q$
  );

  perform cron.schedule(
    'feedme_enqueue_poll_1205_ist',
    '35 6 * * *',
    $q$select public.enqueue_poll_jobs(now());$q$
  );

  perform cron.schedule(
    'feedme_enqueue_poll_1230_ist_watchdog',
    '0 7 * * *',
    $q$select public.enqueue_poll_jobs(now());$q$
  );

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
create or replace function public.fn_is_hot_percentile(p_percentile int)
returns boolean
language sql
immutable
as $$
  select p_percentile is not null and p_percentile <= 35
$$;
