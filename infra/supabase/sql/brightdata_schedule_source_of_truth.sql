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
--   fresh posts bootstrap at D1 only, then advance D1 -> D3 -> D7
--   D21 is only enqueued after a post proves hot at D7
--   first-seen posts older than 7 days are not bootstrapped into checkpoints
--   posts older than a feeder's tracking start are treated as legacy and ignored
--   next_run_at stays at the exact due timestamp
--   fresh checkpoint rows are claimed in hourly batches:
--     during 18:00-18:59 the worker drains jobs due before 18:00,
--     including the 17:00-17:59 bucket plus any older backlog
--   stale backlog whose true checkpoint Fire day is already behind "today"
--   is skipped instead of being claimed, so live checkpoint runs always map
--   to the current Fire horizon users can actually see
--   retries may re-enter within the same hour once their retry timestamp is due
--   official checkpoint rows are ignored if they land before the true age floor
--   (24h / 72h / 168h / 504h), even if a legacy writer attempts them
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

create or replace function public.enqueue_followup_checkpoint(
  p_post_key text,
  p_completed_checkpoint text,
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
  v_posted_at timestamptz;
  v_next_checkpoint text;
  v_days_after int;
  v_due_at timestamptz;
  v_rows int := 0;
begin
  select p.posted_at
  into v_posted_at
  from public.posts p
  where p.post_key = p_post_key;

  if v_posted_at is null then
    return 0;
  end if;

  case lower(coalesce(p_completed_checkpoint, ''))
    when 'd1' then
      v_next_checkpoint := 'd3';
      v_days_after := 3;
    when 'd3' then
      v_next_checkpoint := 'd7';
      v_days_after := 7;
    else
      return 0;
  end case;

  v_due_at := public.fn_checkpoint_due_at(v_posted_at, v_days_after, p_tz, p_hour, p_minute, p_bucket_minutes);
  if v_due_at is null then
    return 0;
  end if;

  insert into public.checkpoint_jobs (post_key, checkpoint, status, next_run_at, last_error)
  values (
    p_post_key,
    v_next_checkpoint,
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

create or replace function public.tg_advance_checkpoint_chain()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  case lower(coalesce(new.checkpoint, ''))
    when 'd1' then
      perform public.enqueue_followup_checkpoint(new.post_key, 'd1');
    when 'd3' then
      perform public.enqueue_followup_checkpoint(new.post_key, 'd3');
    else
      null;
  end case;

  return new;
end;
$$;

drop trigger if exists trg_05_advance_checkpoint_chain on public.post_metrics;

create trigger trg_05_advance_checkpoint_chain
after insert or update of checkpoint, computed_at on public.post_metrics
for each row
execute function public.tg_advance_checkpoint_chain();

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

create or replace function public.enqueue_d21_checkpoint_if_hot(
  p_post_key text
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_posted_at timestamptz;
  v_due_at timestamptz;
  v_rows int := 0;
  v_status text;
  v_error text;
begin
  select p.posted_at
  into v_posted_at
  from public.posts p
  where p.post_key = p_post_key;

  if v_posted_at is null then
    return 0;
  end if;

  if not exists (
    select 1
    from public.post_metrics pm
    where pm.post_key = p_post_key
      and lower(pm.checkpoint) = 'd7'
      and public.fn_is_hot_percentile(pm.percentile_performance)
  ) then
    update public.checkpoint_jobs
    set status = 'skipped',
        last_error = 'D21 deferred - D7 not hot',
        updated_at = now()
    where post_key = p_post_key
      and checkpoint = 'd21'
      and status in ('pending', 'retry', 'running');
    get diagnostics v_rows = row_count;
    return v_rows;
  end if;

  v_due_at := public.fn_checkpoint_due_at(v_posted_at, 21, 'Asia/Kolkata', 23, 30, 60);
  if v_due_at is null then
    v_status := 'skipped';
    v_error := 'D21 skipped - missing posted_at';
  else
    v_status := 'pending';
    v_error := null;
  end if;

  insert into public.checkpoint_jobs (post_key, checkpoint, status, next_run_at, last_error)
  values (p_post_key, 'd21', v_status, coalesce(v_due_at, now()), v_error)
  on conflict (post_key, checkpoint) do update
    set status = case when public.checkpoint_jobs.status = 'done' then public.checkpoint_jobs.status else excluded.status end,
        next_run_at = case when public.checkpoint_jobs.status = 'done' then public.checkpoint_jobs.next_run_at else excluded.next_run_at end,
        last_error = case when public.checkpoint_jobs.status = 'done' then public.checkpoint_jobs.last_error else excluded.last_error end,
        updated_at = now()
    where public.checkpoint_jobs.status <> 'done';

  get diagnostics v_rows = row_count;
  return v_rows;
end;
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
