with required_tables(name) as (
  values ('feeds'),('feeders'),('posts'),('post_metrics'),('run_jobs'),('checkpoint_jobs'),('fire_alerts'),('engine_state')
)
select rt.name as table_name,
       case when t.table_name is null then 'MISSING' else 'OK' end as status
from required_tables rt
left join information_schema.tables t
  on t.table_schema='public' and t.table_name=rt.name
order by rt.name;

with required_functions(name) as (
  values ('fn_post_key'),('fn_checkpoint_due_at'),('enqueue_daily_jobs'),('enqueue_weekly_jobs'),('enqueue_checkpoint_jobs'),('claim_run_jobs'),('skip_unqualified_d21_jobs'),('claim_checkpoint_jobs'),('set_run_job_result'),('set_checkpoint_job_result'),('requeue_stale_jobs'),('finalize_daily_jobs_for_day'),('enqueue_repair_jobs_from_previous_day')
)
select rf.name as function_name,
       case when p.proname is null then 'MISSING' else 'OK' end as status
from required_functions rf
left join pg_proc p on p.proname = rf.name
left join pg_namespace n on n.oid = p.pronamespace and n.nspname='public'
order by rf.name;
