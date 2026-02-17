create or replace function public.get_feedme_cron_jobs()
returns table (
  jobid bigint,
  jobname text,
  schedule text,
  active boolean,
  command text
)
language sql
security definer
set search_path = public, cron
as $$
  select j.jobid::bigint,
         j.jobname::text,
         j.schedule::text,
         j.active,
         j.command::text
  from cron.job j
  where j.jobname like 'feedme_%'
  order by j.jobid;
$$;

revoke all on function public.get_feedme_cron_jobs() from public;
grant execute on function public.get_feedme_cron_jobs() to service_role;
