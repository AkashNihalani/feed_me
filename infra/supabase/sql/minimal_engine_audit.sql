with required_tables(name) as (
  values
    ('users'),
    ('feeds'),
    ('feeders'),
    ('posts'),
    ('post_metrics'),
    ('run_jobs'),
    ('checkpoint_jobs'),
    ('feeder_baselines'),
    ('feeder_hour_baselines'),
    ('feeder_follower_snapshots'),
    ('post_intelligence'),
    ('post_media_assets'),
    ('fire_alerts'),
    ('web_push_subscriptions'),
    ('web_push_jobs')
)
select rt.name as table_name,
       case when t.table_name is null then 'MISSING' else 'OK' end as status
from required_tables rt
left join information_schema.tables t
  on t.table_schema='public' and t.table_name=rt.name
order by rt.name;

with required_functions(name) as (
  values
    ('fn_checkpoint_due_at'),
    ('fn_metric_value'),
    ('fn_is_hot_percentile'),
    ('enqueue_daily_jobs'),
    ('enqueue_daily_job_for_feeder'),
    ('enqueue_poll_jobs'),
    ('enqueue_poll_job_for_feeder'),
    ('enqueue_weekly_follower_jobs'),
    ('enqueue_weekly_follower_job_for_feeder'),
    ('bootstrap_feeder_jobs'),
    ('enqueue_checkpoint_jobs'),
    ('claim_run_jobs'),
    ('claim_checkpoint_jobs'),
    ('claim_post_media_assets_for_capture'),
    ('claim_post_media_assets_for_purge'),
    ('claim_web_push_jobs'),
    ('set_run_job_result'),
    ('set_checkpoint_job_result'),
    ('requeue_stale_jobs'),
    ('finalize_daily_jobs_for_day'),
    ('enqueue_repair_jobs_from_previous_day'),
    ('skip_unqualified_d21_jobs'),
    ('fn_process_checkpoint'),
    ('fn_fire_alert_urgency'),
    ('fn_upsert_fire_signal'),
    ('fn_feed_dashboard')
)
select rf.name as function_name,
       case when p.proname is null then 'MISSING' else 'OK' end as status
from required_functions rf
left join pg_proc p on p.proname = rf.name
left join pg_namespace n on n.oid = p.pronamespace and n.nspname='public'
order by rf.name;
