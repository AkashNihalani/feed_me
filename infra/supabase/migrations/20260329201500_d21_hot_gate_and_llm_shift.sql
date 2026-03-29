begin;

drop function if exists public.enqueue_checkpoint_jobs(text, timestamptz, text, integer, integer, integer);

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
  )
  insert into public.checkpoint_jobs (post_key, checkpoint, status, next_run_at, last_error)
  select
    p_post_key,
    t.checkpoint,
    case
      when t.due_at is null then 'skipped'
      when (t.due_at + make_interval(mins => greatest(1, coalesce(p_bucket_minutes, 60)))) <= now() then 'skipped'
      else 'pending'
    end as status,
    coalesce(t.due_at, now()) as next_run_at,
    case
      when t.due_at is null then 'checkpoint skipped: missing posted_at'
      when (t.due_at + make_interval(mins => greatest(1, coalesce(p_bucket_minutes, 60)))) <= now()
        then 'checkpoint skipped: eligibility window already passed'
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

drop function if exists public.enqueue_checkpoint_jobs(text, timestamptz);

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
  elsif (v_due_at + interval '60 minutes') <= now() then
    v_status := 'skipped';
    v_error := 'D21 skipped - eligibility window already passed';
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

create or replace function public.skip_unqualified_d21_jobs()
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
begin
  update public.checkpoint_jobs cj
  set status = 'skipped',
      last_error = 'D21 deferred - D7 not hot',
      updated_at = now()
  where cj.status in ('pending', 'retry', 'running')
    and cj.checkpoint = 'd21'
    and not exists (
      select 1
      from public.post_metrics pm
      where pm.post_key = cj.post_key
        and pm.checkpoint = 'd7'
        and public.fn_is_hot_percentile(pm.percentile_performance)
    );

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

create or replace function public.tg_manage_hot_d21_checkpoint()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if lower(coalesce(new.checkpoint, '')) <> 'd7' then
    return new;
  end if;

  if public.fn_is_hot_percentile(new.percentile_performance) then
    perform public.enqueue_d21_checkpoint_if_hot(new.post_key);
  else
    update public.checkpoint_jobs
    set status = 'skipped',
        last_error = 'D21 deferred - D7 not hot',
        updated_at = now()
    where post_key = new.post_key
      and checkpoint = 'd21'
      and status in ('pending', 'retry', 'running');
  end if;

  return new;
end;
$$;

drop trigger if exists trg_manage_hot_d21_checkpoint on public.post_metrics;

create trigger trg_manage_hot_d21_checkpoint
after insert or update of checkpoint, percentile_performance, computed_at on public.post_metrics
for each row
execute function public.tg_manage_hot_d21_checkpoint();

update public.checkpoint_jobs
set status = 'skipped',
    last_error = 'D21 deferred - awaiting hot D7',
    updated_at = now()
where checkpoint = 'd21'
  and status in ('pending', 'retry', 'running');

do $$
declare
  r record;
begin
  for r in
    select pm.post_key
    from public.post_metrics pm
    where lower(pm.checkpoint) = 'd7'
      and public.fn_is_hot_percentile(pm.percentile_performance)
  loop
    perform public.enqueue_d21_checkpoint_if_hot(r.post_key);
  end loop;
end $$;

commit;
