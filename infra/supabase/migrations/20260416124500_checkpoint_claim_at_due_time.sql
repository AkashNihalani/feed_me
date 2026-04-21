begin;

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
    else p_next_run_at <= coalesce(p_now, now())
  end
$$;

commit;
