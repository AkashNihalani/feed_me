begin;

-- Align daily scrape lane with product behavior:
-- a run executed on IST day D represents business date D-1.
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
    );

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
    );

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

-- Slot-state queue must be scoped to exactly one business day to avoid cross-day churn.
create or replace function public.enqueue_slot_state_alerts(
  p_feeder_id bigint,
  p_checkpoint text,
  p_business_date_ist date default null
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  r record;
  v_count int := 0;
  v_cp text := lower(coalesce(p_checkpoint, 'd1'));
  v_day date := coalesce(p_business_date_ist, (now() at time zone 'Asia/Kolkata')::date);
begin
  if v_cp not in ('d1','d3','d7','d21') then
    return 0;
  end if;

  for r in
    select
      pm.post_key,
      pm.checkpoint,
      pm.percentile_performance,
      pm.feed_percentile,
      pm.percentile_delta,
      pm.metric_value,
      pm.views,
      pm.likes,
      pm.comments,
      pm.d1_percentile,
      pm.d3_percentile,
      pm.d7_percentile,
      pm.d21_percentile,
      pm.captured_business_date_ist,
      pm.business_date_ist,
      p.media_type,
      p.posted_at,
      p.post_url,
      p.thumbnail_url,
      fd.feed_id,
      fd.handle
    from public.post_metrics pm
    join public.posts p on p.post_key = pm.post_key
    join public.feeders fd on fd.id = p.feeder_id
    where fd.id = p_feeder_id
      and pm.checkpoint = v_cp
      and pm.percentile_performance is not null
      and coalesce(pm.captured_business_date_ist, pm.business_date_ist, (p.posted_at at time zone 'Asia/Kolkata')::date) = v_day
  loop
    perform public.fn_upsert_fire_alert(
      r.feed_id,
      p_feeder_id,
      r.post_key,
      v_cp,
      'own',
      'A',
      'S0_slot_state',
      greatest(1, least(100, 100 - coalesce(r.percentile_performance, 50))),
      format('Top %s%%', coalesce(r.percentile_performance, 0)),
      'Slot state',
      coalesce(r.captured_business_date_ist, r.business_date_ist, (r.posted_at at time zone 'Asia/Kolkata')::date, v_day),
      jsonb_build_object(
        'feeder', r.handle,
        'media_type', r.media_type,
        'checkpoint', r.checkpoint,
        'post_url', r.post_url,
        'thumbnail_url', r.thumbnail_url,
        'percentile', r.percentile_performance,
        'feed_percentile', r.feed_percentile,
        'delta', r.percentile_delta,
        'views', r.views,
        'likes', r.likes,
        'comments', r.comments,
        'metric_value', r.metric_value,
        'd1_pct', r.d1_percentile,
        'd3_pct', r.d3_percentile,
        'd7_pct', r.d7_percentile,
        'd21_pct', r.d21_percentile
      )
    );
    v_count := v_count + 1;
  end loop;

  return v_count;
end;
$$;

commit;
