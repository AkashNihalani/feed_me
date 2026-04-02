begin;

create or replace function public.claim_checkpoint_jobs(p_limit int default 100)
returns setof public.checkpoint_jobs
language sql
security definer
set search_path = public
as $$
  with ctx as (
    select public.fn_checkpoint_hourly_cutoff(now()) as cutoff_utc
  ), picked as (
    select cj.id
    from public.checkpoint_jobs cj
    cross join ctx
    where cj.status in ('pending', 'retry')
      and public.fn_checkpoint_job_claimable(cj.attempt, cj.next_run_at, now())
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

commit;
