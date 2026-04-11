begin;

create or replace function public.claim_checkpoint_jobs(p_limit int default 100)
returns setof public.checkpoint_jobs
language sql
security definer
set search_path = public
as $$
  with ctx as (
    select
      public.fn_checkpoint_hourly_cutoff(now()) as cutoff_utc,
      (now() at time zone 'Asia/Kolkata')::date as today_ist,
      ((now() at time zone 'Asia/Kolkata')::date - 14) as live_floor_ist
  ), scoped as (
    select
      cj.id,
      cj.next_run_at,
      cj.attempt,
      coalesce(
        public.fn_checkpoint_job_business_day(p.posted_at, cj.checkpoint, 'Asia/Kolkata'),
        (coalesce(cj.next_run_at, now()) at time zone 'Asia/Kolkata')::date
      ) as business_day_ist
    from public.checkpoint_jobs cj
    join public.posts p on p.post_key = cj.post_key
    cross join ctx
    where cj.status in ('pending', 'retry')
      and public.fn_checkpoint_job_claimable(cj.attempt, cj.next_run_at, now())
      and not exists (
        select 1
        from public.post_metrics pm
        where pm.post_key = cj.post_key
          and lower(pm.checkpoint) = lower(cj.checkpoint)
      )
      and coalesce(
        public.fn_checkpoint_job_business_day(p.posted_at, cj.checkpoint, 'Asia/Kolkata'),
        (coalesce(cj.next_run_at, now()) at time zone 'Asia/Kolkata')::date
      ) >= ctx.live_floor_ist
  ), picked as (
    select s.id
    from scoped s
    cross join ctx
    order by
      case
        when s.business_day_ist >= ctx.today_ist then 0
        else 1
      end,
      case
        when s.business_day_ist >= ctx.today_ist
         and s.next_run_at >= (ctx.cutoff_utc - interval '1 hour') then 0
        else 1
      end,
      s.business_day_ist desc,
      s.next_run_at asc,
      s.attempt asc,
      s.id asc
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
set status = 'done',
    last_error = null,
    updated_at = now()
where cj.status in ('pending', 'retry')
  and exists (
    select 1
    from public.post_metrics pm
    where pm.post_key = cj.post_key
      and lower(pm.checkpoint) = lower(cj.checkpoint)
  );

commit;
