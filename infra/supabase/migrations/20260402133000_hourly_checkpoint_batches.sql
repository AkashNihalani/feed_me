begin;

create or replace function public.fn_checkpoint_hourly_cutoff(
  p_now timestamptz default now()
)
returns timestamptz
language sql
stable
as $$
  select date_trunc('hour', coalesce(p_now, now()))
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
    when coalesce(p_attempt, 0) > 0 then p_next_run_at <= coalesce(p_now, now())
    else p_next_run_at < public.fn_checkpoint_hourly_cutoff(coalesce(p_now, now()))
  end
$$;

create or replace function public.claim_checkpoint_jobs(p_limit int default 100)
returns setof public.checkpoint_jobs
language sql
security definer
set search_path = public
as $$
  with picked as (
    select id
    from public.checkpoint_jobs
    where status in ('pending', 'retry')
      and public.fn_checkpoint_job_claimable(attempt, next_run_at, now())
    order by next_run_at asc, attempt asc, id asc
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
      last_error = 'D21 skipped - D7 did not qualify',
      updated_at = now()
  where cj.status in ('pending', 'retry')
    and cj.checkpoint = 'd21'
    and public.fn_checkpoint_job_claimable(cj.attempt, cj.next_run_at, now())
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

commit;
