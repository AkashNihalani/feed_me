begin;

update public.checkpoint_jobs
set status = 'skipped',
    last_error = 'retired at two-discovery pipeline cutover; no historical backfill',
    updated_at = now()
where status in ('pending', 'running', 'retry')
  and (
    next_run_at <= now()
    or (
      next_run_at = timestamptz '2026-07-16 00:00:00+00'
      and created_at < timestamptz '2026-07-15 00:00:00+00'
    )
  );

update public.run_jobs
set status = 'skipped',
    last_error = 'retired at two-discovery pipeline cutover',
    updated_at = now()
where status in ('pending', 'running', 'retry')
  and job_type in ('daily', 'repair', 'poll', 'followers');

alter table public.run_jobs
  add column if not exists discovery_slot text not null default 'legacy';

drop index if exists public.run_jobs_open_scrape_lane_unique_idx;

create unique index if not exists run_jobs_open_discovery_slot_unique_idx
  on public.run_jobs(feeder_id, business_date_ist, discovery_slot)
  where status in ('pending', 'running', 'retry')
    and job_type in ('daily', 'repair')
    and business_date_ist is not null;

create or replace function public.enqueue_daily_jobs(p_run_at timestamptz default now())
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_count integer := 0;
  v_local timestamp := p_run_at at time zone 'Asia/Kolkata';
  v_business_date date := v_local::date;
  v_slot text := case when extract(hour from v_local) < 12 then 'am' else 'pm' end;
begin
  insert into public.run_jobs (
    feeder_id, job_type, status, next_run_at, business_date_ist, discovery_slot
  )
  select
    fd.id, 'daily', 'pending', p_run_at, v_business_date, v_slot
  from public.feeders fd
  join public.feeds f on f.id = fd.feed_id
  where fd.status = 'active'
    and f.status = 'active'
    and not exists (
      select 1
      from public.run_jobs rj
      where rj.feeder_id = fd.id
        and rj.business_date_ist = v_business_date
        and rj.discovery_slot = v_slot
        and rj.job_type = 'daily'
    )
  on conflict do nothing;

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
  v_local timestamp := p_run_at at time zone 'Asia/Kolkata';
  v_business_date date := v_local::date;
  v_slot text := case when extract(hour from v_local) < 12 then 'am' else 'pm' end;
begin
  insert into public.run_jobs (
    feeder_id, job_type, status, next_run_at, business_date_ist, discovery_slot
  )
  select fd.id, 'daily', 'pending', p_run_at, v_business_date, v_slot
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
        and rj.discovery_slot = v_slot
        and rj.job_type = 'daily'
    )
  on conflict do nothing;

  get diagnostics v_count = row_count;
  return v_count;
end;
$$;

drop function if exists public.enqueue_checkpoint_jobs(text, timestamptz);
drop function if exists public.enqueue_checkpoint_jobs(text, timestamptz, text, integer, integer, integer);

create function public.enqueue_checkpoint_jobs(
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
  if p_posted_at is null then
    return 0;
  end if;

  with targets(checkpoint, days_after) as (
    values ('d1'::text, 1), ('d3'::text, 3), ('d7'::text, 7), ('d21'::text, 21)
  ), due as (
    select
      checkpoint,
      public.fn_checkpoint_due_at(
        p_posted_at, days_after, p_tz, p_hour, p_minute, p_bucket_minutes
      ) as due_at
    from targets
  )
  insert into public.checkpoint_jobs (post_key, checkpoint, status, next_run_at, last_error)
  select p_post_key, checkpoint, 'pending', due_at, null
  from due
  where due_at is not null
    and due_at + make_interval(mins => greatest(1, coalesce(p_bucket_minutes, 60))) > now()
  on conflict (post_key, checkpoint) do nothing;

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

create function public.enqueue_checkpoint_jobs(
  p_post_key text,
  p_posted_at timestamptz
)
returns int
language sql
security definer
set search_path = public
as $$
  select public.enqueue_checkpoint_jobs(
    p_post_key, p_posted_at, 'Asia/Kolkata', 23, 30, 60
  )
$$;

drop trigger if exists trg_05_advance_checkpoint_chain on public.post_metrics;
drop trigger if exists trg_manage_hot_d21_checkpoint on public.post_metrics;

create or replace function public.skip_unqualified_d21_jobs()
returns int
language sql
security definer
set search_path = public
as $$
  select 0::int
$$;

create or replace function public.fn_checkpoint_job_claimable(
  p_attempt int,
  p_next_run_at timestamptz,
  p_now timestamptz default now()
)
returns boolean
language sql
stable
as $$
  select case
    when p_next_run_at is null then false
    when coalesce(p_attempt, 0) = 0 then coalesce(p_now, now()) >= (
      (
        case
          when (p_next_run_at at time zone 'Asia/Kolkata')::time <= time '11:30'
            then date_trunc('day', p_next_run_at at time zone 'Asia/Kolkata') + interval '12 hours'
          when (p_next_run_at at time zone 'Asia/Kolkata')::time <= time '23:30'
            then date_trunc('day', p_next_run_at at time zone 'Asia/Kolkata') + interval '1 day'
          else date_trunc('day', p_next_run_at at time zone 'Asia/Kolkata') + interval '1 day 12 hours'
        end
      ) at time zone 'Asia/Kolkata'
    )
    else p_next_run_at <= coalesce(p_now, now())
  end
$$;

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
      'feedme_enqueue_daily_0005_ist',
      'feedme_enqueue_daily_0030_ist_watchdog',
      'feedme_repair_lane_0045_ist_watchdog',
      'feedme_repair_lane_0505_ist',
      'feedme_resurrect_failed',
      'feedme_discovery_1130_ist',
      'feedme_discovery_2330_ist'
    )
  loop
    perform cron.unschedule(r.jobid);
  end loop;

  perform cron.schedule(
    'feedme_discovery_1130_ist',
    '0 6 * * *',
    $q$select public.enqueue_daily_jobs(now());$q$
  );
  perform cron.schedule(
    'feedme_discovery_2330_ist',
    '0 18 * * *',
    $q$select public.enqueue_daily_jobs(now());$q$
  );
end;
$$;

commit;
