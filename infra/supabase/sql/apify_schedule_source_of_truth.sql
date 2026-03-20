-- FeedMe Apify schedule source of truth
-- ------------------------------------
-- This file is the single readable contract for ingestion timing while
-- computation + fire alert logic remain unchanged.
--
-- Daily discovery:
--   00:05 IST  -> enqueue daily feeder jobs for the previous IST business day
--   00:30 IST  -> idempotent enqueue watchdog
--   00:45 IST  -> repair watchdog for missed daily work
--   worker     -> Apify handle scrape with a 3-day lookback window
--
-- Checkpoints:
--   D3 / D7 / D21 remain the same checkpoint families
--   due_at is 18:30 IST using inclusive business-day semantics
--   worker batches all due checkpoint URLs into one evening Apify scrape
--
-- Late discovery:
--   preserved via the 3-day daily lookback and existing repair lane
--   compute logic remains untouched

begin;

create or replace function public.fn_checkpoint_due_at(
  p_posted_at timestamptz,
  p_days_after int,
  p_tz text default 'Asia/Kolkata',
  p_hour int default 18,
  p_minute int default 30
)
returns timestamptz
language plpgsql
stable
as $$
declare
  v_local_date date;
  v_target_local timestamp;
  v_target_utc timestamptz;
  v_inclusive_offset int;
begin
  if p_posted_at is null then
    return null;
  end if;

  v_inclusive_offset := greatest(0, coalesce(p_days_after, 0) - 1);
  v_local_date := ((p_posted_at at time zone p_tz)::date + v_inclusive_offset);
  v_target_local := (v_local_date::timestamp + make_interval(hours => p_hour, mins => p_minute));
  v_target_utc := (v_target_local at time zone p_tz);
  return v_target_utc;
end;
$$;

create or replace function public.enqueue_checkpoint_jobs(
  p_post_key text,
  p_posted_at timestamptz,
  p_tz text default 'Asia/Kolkata',
  p_hour int default 18,
  p_minute int default 30
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

create or replace function public.backfill_missing_checkpoint_jobs(
  p_days int default 30,
  p_tz text default 'Asia/Kolkata',
  p_hour int default 18,
  p_minute int default 30
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
  v_window interval := make_interval(days => greatest(1, coalesce(p_days, 30)));
begin
  insert into public.checkpoint_jobs (post_key, checkpoint, status, next_run_at)
  select
    p.post_key,
    cp.checkpoint,
    'pending',
    case cp.checkpoint
      when 'd3' then greatest(public.fn_checkpoint_due_at(p.posted_at, 3, p_tz, p_hour, p_minute), now())
      when 'd7' then greatest(public.fn_checkpoint_due_at(p.posted_at, 7, p_tz, p_hour, p_minute), now())
      when 'd21' then greatest(public.fn_checkpoint_due_at(p.posted_at, 21, p_tz, p_hour, p_minute), now())
    end
  from public.posts p
  cross join (values ('d3'), ('d7'), ('d21')) as cp(checkpoint)
  where p.posted_at is not null
    and p.posted_at >= now() - v_window
  on conflict (post_key, checkpoint) do nothing;

  get diagnostics v_rows = row_count;
  return coalesce(v_rows, 0);
end;
$$;

create or replace function public.enqueue_daily_jobs(p_run_at timestamptz default now())
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_count integer := 0;
begin
  insert into public.run_jobs (feeder_id, job_type, status, next_run_at, business_date_ist)
  select
    fd.id,
    'daily',
    'pending',
    p_run_at,
    ((p_run_at at time zone 'Asia/Kolkata')::date - 1)
  from public.feeders fd
  join public.feeds f on f.id = fd.feed_id
  where fd.status = 'active'
    and f.status = 'active'
    and not exists (
      select 1
      from public.run_jobs rj
      where rj.feeder_id = fd.id
        and rj.job_type in ('daily', 'repair')
        and rj.business_date_ist = ((p_run_at at time zone 'Asia/Kolkata')::date - 1)
        and rj.status in ('pending', 'running', 'retry', 'done')
    );

  get diagnostics v_count = row_count;
  return v_count;
end;
$$;

create or replace function public.enqueue_daily_job_for_feeder(
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
begin
  insert into public.run_jobs (feeder_id, job_type, status, next_run_at, business_date_ist)
  select
    fd.id,
    'daily',
    'pending',
    p_run_at,
    ((p_run_at at time zone 'Asia/Kolkata')::date - 1)
  from public.feeders fd
  join public.feeds f on f.id = fd.feed_id
  where fd.id = p_feeder_id
    and fd.status = 'active'
    and f.status = 'active'
    and not exists (
      select 1
      from public.run_jobs rj
      where rj.feeder_id = fd.id
        and rj.job_type in ('daily', 'repair')
        and rj.business_date_ist = ((p_run_at at time zone 'Asia/Kolkata')::date - 1)
        and rj.status in ('pending', 'running', 'retry', 'done')
    );

  get diagnostics v_count = row_count;
  return v_count;
end;
$$;

alter table if exists public.run_jobs
  drop constraint if exists run_jobs_job_type_check;

alter table if exists public.run_jobs
  add constraint run_jobs_job_type_check
  check (job_type = any (array['daily'::text, 'weekly'::text, 'repair'::text, 'poll'::text]));

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
      'feedme_repair_lane_0045_ist_watchdog'
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
end $$;

commit;
