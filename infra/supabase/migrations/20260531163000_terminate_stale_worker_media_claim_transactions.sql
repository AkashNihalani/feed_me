begin;

create or replace function public.terminate_stale_worker_media_claim_transactions(
  p_min_age interval default interval '2 minutes'
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
begin
  with terminated as (
    select pg_catalog.pg_terminate_backend(pid) as terminated
    from pg_catalog.pg_stat_activity
    where pid <> pg_catalog.pg_backend_pid()
      and datname = current_database()
      and state = 'idle in transaction'
      and xact_start is not null
      and xact_start <= now() - greatest(coalesce(p_min_age, interval '2 minutes'), interval '30 seconds')
      and (
        query ilike 'select * from public.claim_post_media_assets_for_capture(%'
        or query ilike 'select * from public.claim_post_media_assets_for_purge(%'
        or query ilike 'select public.claim_post_media_assets_for_capture(%'
        or query ilike 'select public.claim_post_media_assets_for_purge(%'
      )
  )
  select count(*)::int
  into v_rows
  from terminated
  where terminated;

  return coalesce(v_rows, 0);
end;
$$;

do $$
begin
  if to_regnamespace('cron') is null then
    raise notice 'pg_cron schema not found; stale worker media claim watchdog was not scheduled';
    return;
  end if;

  perform cron.unschedule(jobid)
  from cron.job
  where jobname = 'feedme_terminate_stale_worker_media_claims';

  perform cron.schedule(
    'feedme_terminate_stale_worker_media_claims',
    '* * * * *',
    $command$select public.terminate_stale_worker_media_claim_transactions(interval '2 minutes');$command$
  );
end;
$$;

commit;
