begin;

create or replace function public.fn_checkpoint_job_business_day(
  p_posted_at timestamptz,
  p_checkpoint text,
  p_tz text default 'Asia/Kolkata'
)
returns date
language plpgsql
stable
as $$
declare
  v_days_after int;
  v_due_at timestamptz;
begin
  v_days_after := case lower(coalesce(p_checkpoint, ''))
    when 'd1' then 1
    when 'd3' then 3
    when 'd7' then 7
    when 'd21' then 21
    else null
  end;

  if v_days_after is null or p_posted_at is null then
    return null;
  end if;

  v_due_at := public.fn_checkpoint_due_at(p_posted_at, v_days_after, p_tz, 23, 30, 60);
  if v_due_at is null then
    return null;
  end if;

  return (v_due_at at time zone p_tz)::date;
end;
$$;

create or replace function public.claim_checkpoint_jobs(p_limit int default 100)
returns setof public.checkpoint_jobs
language sql
security definer
set search_path = public
as $$
  with ctx as (
    select
      public.fn_checkpoint_hourly_cutoff(now()) as cutoff_utc,
      (now() at time zone 'Asia/Kolkata')::date as today_ist
  ), picked as (
    select cj.id
    from public.checkpoint_jobs cj
    join public.posts p on p.post_key = cj.post_key
    cross join ctx
    where cj.status in ('pending', 'retry')
      and public.fn_checkpoint_job_claimable(cj.attempt, cj.next_run_at, now())
      and coalesce(
        public.fn_checkpoint_job_business_day(p.posted_at, cj.checkpoint, 'Asia/Kolkata'),
        (coalesce(cj.next_run_at, now()) at time zone 'Asia/Kolkata')::date
      ) >= ctx.today_ist
    order by
      case
        when cj.next_run_at >= (ctx.cutoff_utc - interval '1 hour') then 0
        else 1
      end,
      cj.next_run_at asc,
      cj.attempt asc,
      cj.id asc
    for update skip locked
    limit greatest(1, p_limit)
  ), updated as (
    update public.checkpoint_jobs cj
    set status = 'running',
        updated_at = now()
    from picked
    where cj.id = picked.id
    returning cj.*
  )
  select * from updated;
$$;

update public.checkpoint_jobs cj
set status = 'skipped',
    last_error = 'Checkpoint skipped - outside live fire horizon',
    updated_at = now()
from public.posts p
where p.post_key = cj.post_key
  and cj.status in ('pending', 'retry')
  and coalesce(
    public.fn_checkpoint_job_business_day(p.posted_at, cj.checkpoint, 'Asia/Kolkata'),
    (coalesce(cj.next_run_at, now()) at time zone 'Asia/Kolkata')::date
  ) < ((now() at time zone 'Asia/Kolkata')::date);

commit;
