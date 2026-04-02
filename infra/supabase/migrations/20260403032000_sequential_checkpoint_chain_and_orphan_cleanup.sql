begin;

create or replace function public.enqueue_checkpoint_jobs(
  p_post_key text,
  p_posted_at timestamptz,
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
  v_rows int := 0;
  v_due_at timestamptz;
begin
  if p_posted_at is null then
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

create temporary table tmp_orphan_checkpoint_posts on commit drop as
select distinct src.post_key
from (
  select pm.post_key
  from public.post_metrics pm
  where lower(pm.checkpoint) in ('d3', 'd7', 'd21')
  union
  select fa.post_key
  from public.fire_alerts fa
  where lower(fa.checkpoint) in ('d3', 'd7', 'd21')
  union
  select cj.post_key
  from public.checkpoint_jobs cj
  where lower(cj.checkpoint) in ('d3', 'd7', 'd21')
) src
where not exists (
    select 1
    from public.checkpoint_jobs cj1
    where cj1.post_key = src.post_key
      and lower(cj1.checkpoint) = 'd1'
  )
  and not exists (
    select 1
    from public.post_metrics pm1
    where pm1.post_key = src.post_key
      and lower(pm1.checkpoint) = 'd1'
  )
  and not exists (
    select 1
    from public.fire_alerts fa1
    where fa1.post_key = src.post_key
      and lower(fa1.checkpoint) = 'd1'
  );

delete from public.fire_alerts fa
where fa.post_key in (select post_key from tmp_orphan_checkpoint_posts)
  and lower(fa.checkpoint) in ('d3', 'd7', 'd21');

delete from public.post_metrics pm
where pm.post_key in (select post_key from tmp_orphan_checkpoint_posts)
  and lower(pm.checkpoint) in ('d3', 'd7', 'd21');

delete from public.checkpoint_jobs cj
where cj.post_key in (select post_key from tmp_orphan_checkpoint_posts)
  and lower(cj.checkpoint) in ('d3', 'd7', 'd21');

do $$
declare
  r record;
begin
  for r in
    select distinct pm.post_key
    from public.post_metrics pm
    where lower(pm.checkpoint) = 'd1'
  loop
    perform public.enqueue_followup_checkpoint(r.post_key, 'd1');
  end loop;

  for r in
    select distinct pm.post_key
    from public.post_metrics pm
    where lower(pm.checkpoint) = 'd3'
  loop
    perform public.enqueue_followup_checkpoint(r.post_key, 'd3');
  end loop;

  for r in
    select distinct pm.post_key
    from public.post_metrics pm
    where lower(pm.checkpoint) = 'd7'
      and public.fn_is_hot_percentile(pm.percentile_performance)
  loop
    perform public.enqueue_d21_checkpoint_if_hot(r.post_key);
  end loop;
end $$;

commit;
