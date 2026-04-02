begin;

create or replace function public.enqueue_checkpoint_jobs(
  p_post_key text,
  p_posted_at timestamptz,
  p_tz text default 'Asia/Kolkata',
  p_hour int default 18,
  p_minute int default 30,
  p_bucket_minutes int default 60
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
    checkpoint,
    case
      when due_at is null then 'skipped'
      else 'pending'
    end,
    coalesce(due_at, now()),
    case
      when due_at is null then 'checkpoint skipped: missing posted_at'
      else null
    end
  from targets
  on conflict (post_key, checkpoint) do update
    set status = case
        when public.checkpoint_jobs.status in ('done', 'running') then public.checkpoint_jobs.status
        else excluded.status
      end,
      next_run_at = case
        when public.checkpoint_jobs.status in ('done', 'running') then public.checkpoint_jobs.next_run_at
        else excluded.next_run_at
      end,
      last_error = case
        when public.checkpoint_jobs.status in ('done', 'running') then public.checkpoint_jobs.last_error
        else excluded.last_error
      end,
      updated_at = now()
    where public.checkpoint_jobs.status not in ('done', 'running');

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

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
    insert into public.checkpoint_jobs (post_key, checkpoint, status, next_run_at, last_error)
    values (p_post_key, 'd21', 'skipped', now(), 'D21 skipped - missing posted_at')
    on conflict (post_key, checkpoint) do update
      set status = 'skipped',
          next_run_at = public.checkpoint_jobs.next_run_at,
          last_error = 'D21 skipped - missing posted_at',
          updated_at = now()
      where public.checkpoint_jobs.status not in ('done');
    get diagnostics v_rows = row_count;
    return v_rows;
  end if;

  if not exists (
    select 1
    from public.post_metrics pm
    where pm.post_key = p_post_key
      and lower(pm.checkpoint) = 'd7'
      and public.fn_is_hot_percentile(pm.percentile_performance)
  ) then
    update public.checkpoint_jobs cj
    set status = 'skipped',
        last_error = 'D21 deferred - D7 not hot',
        updated_at = now()
    where cj.post_key = p_post_key
      and cj.checkpoint = 'd21'
      and cj.status in ('pending', 'retry', 'running');
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
  values (
    p_post_key,
    'd21',
    v_status,
    coalesce(v_due_at, now()),
    v_error
  )
  on conflict (post_key, checkpoint) do update
    set status = case
        when public.checkpoint_jobs.status = 'done' then public.checkpoint_jobs.status
        else excluded.status
      end,
      next_run_at = case
        when public.checkpoint_jobs.status = 'done' then public.checkpoint_jobs.next_run_at
        else excluded.next_run_at
      end,
      last_error = case
        when public.checkpoint_jobs.status = 'done' then public.checkpoint_jobs.last_error
        else excluded.last_error
      end,
      updated_at = now()
    where public.checkpoint_jobs.status <> 'done';

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

create or replace function public.tg_post_metrics_contract()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_posted_at timestamptz;
  v_due_at timestamptz;
  v_expected_day date;
  v_computed_at timestamptz := coalesce(new.computed_at, now());
  v_days_after int;
begin
  select p.posted_at
  into v_posted_at
  from public.posts p
  where p.post_key = new.post_key;

  v_days_after := case lower(coalesce(new.checkpoint, ''))
    when 'd1' then 1
    when 'd3' then 3
    when 'd7' then 7
    when 'd21' then 21
    else null
  end;

  if v_days_after is null then
    return null;
  end if;

  new.checkpoint := lower(new.checkpoint);
  new.computed_at := v_computed_at;

  if v_posted_at is not null then
    v_due_at := public.fn_checkpoint_due_at(v_posted_at, v_days_after, 'Asia/Kolkata', 23, 30, 60);
    if v_due_at is not null then
      if v_computed_at < v_due_at then
        return null;
      end if;

      v_expected_day := (v_due_at at time zone 'Asia/Kolkata')::date;
      new.business_date_ist := v_expected_day;
    end if;
  end if;

  if new.business_date_ist is null then
    new.business_date_ist := (v_computed_at at time zone 'Asia/Kolkata')::date;
  end if;

  return new;
end;
$$;

update public.checkpoint_jobs cj
set status = case
      when coalesce(cj.next_run_at, now()) <= now() then 'retry'
      else 'pending'
    end,
    next_run_at = coalesce(cj.next_run_at, now()),
    last_error = null,
    updated_at = now()
where cj.status = 'skipped'
  and cj.checkpoint in ('d1', 'd3', 'd7', 'd21')
  and (
    cj.last_error = 'checkpoint skipped: eligibility window already passed'
    or cj.last_error = 'D21 skipped - eligibility window already passed'
    or cj.last_error like 'hard-skip:checkpoint missed exact-age window%'
  );

commit;
