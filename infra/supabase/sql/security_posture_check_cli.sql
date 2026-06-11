-- One-statement version of security_posture_check.sql for `supabase db query`.
-- Returns only failures plus a PASS summary row when no failures are found.

with
expected_private_tables(table_name) as (
  values
    ('feeds'),
    ('feeders'),
    ('posts'),
    ('run_jobs'),
    ('checkpoint_jobs'),
    ('post_metrics'),
    ('fire_alerts'),
    ('post_media_assets'),
    ('feeder_baselines'),
    ('feeder_hour_baselines'),
    ('feeder_follower_snapshots'),
    ('signals'),
    ('signal_posts'),
    ('post_fingerprints'),
    ('post_breakdowns'),
    ('post_condensations'),
    ('feeder_files'),
    ('feeder_file_patterns'),
    ('feeder_file_model_calls'),
    ('feeder_file_winner_snapshots')
),
private_tables(table_name) as (
  values
    ('feeds'),
    ('feeders'),
    ('posts'),
    ('run_jobs'),
    ('checkpoint_jobs'),
    ('post_metrics'),
    ('fire_alerts'),
    ('post_media_assets'),
    ('feeder_baselines'),
    ('feeder_hour_baselines'),
    ('feeder_follower_snapshots'),
    ('signals'),
    ('signal_posts'),
    ('post_fingerprints'),
    ('post_breakdowns'),
    ('post_condensations'),
    ('feeder_files'),
    ('feeder_file_patterns'),
    ('feeder_file_model_calls'),
    ('feeder_file_winner_snapshots'),
    ('post_intelligence'),
    ('signal_intelligence'),
    ('feeder_focus'),
    ('feed_focus'),
    ('post_focus_reads'),
    ('focus_compile_locks'),
    ('pipeline_audit_daily'),
    ('engine_state'),
    ('transactions')
),
checks as (
  select
    'FAIL'::text as severity,
    'private_table_rls'::text as check_name,
    e.table_name::text as object_name,
    case
      when c.oid is null then 'missing table'
      when c.relrowsecurity is not true then 'RLS disabled'
      else 'unknown'
    end::text as detail
  from expected_private_tables e
  left join pg_class c
    on c.oid = to_regclass(format('public.%I', e.table_name))
  where c.oid is not null
    and c.relrowsecurity is not true

  union all

  select
    'FAIL',
    'private_table_browser_grant',
    g.table_name::text,
    concat(g.grantee, ' has ', g.privilege_type)
  from information_schema.role_table_grants g
  join private_tables p on p.table_name = g.table_name
  where g.table_schema = 'public'
    and g.grantee in ('anon', 'authenticated', 'PUBLIC')

  union all

  select
    'FAIL',
    'public_function_browser_execute',
    concat(p.proname, '(', pg_get_function_identity_arguments(p.oid), ')'),
    concat(
      'anon=', has_function_privilege('anon', p.oid, 'execute'),
      ', authenticated=', has_function_privilege('authenticated', p.oid, 'execute')
    )
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = 'public'
    and (
      has_function_privilege('anon', p.oid, 'execute')
      or has_function_privilege('authenticated', p.oid, 'execute')
    )

  union all

  select
    'FAIL',
    'users_accounting_column_update',
    cp.column_name::text,
    concat(cp.grantee, ' has ', cp.privilege_type)
  from information_schema.column_privileges cp
  where cp.table_schema = 'public'
    and cp.table_name = 'users'
    and cp.grantee in ('anon', 'authenticated', 'PUBLIC')
    and cp.privilege_type = 'UPDATE'
    and cp.column_name in (
      'balance',
      'total_runs',
      'data_points',
      'success_rate',
      'twitter_posts_caught',
      'reddit_posts_caught'
    )

  union all

  select
    'FAIL',
    'platform_stats_public_read',
    'public.platform_stats',
    'anon lacks SELECT'
  where to_regclass('public.platform_stats') is not null
    and not has_table_privilege('anon', 'public.platform_stats', 'select')

  union all

  select
    'FAIL',
    'platform_stats_public_read',
    'public.platform_stats',
    'authenticated lacks SELECT'
  where to_regclass('public.platform_stats') is not null
    and not has_table_privilege('authenticated', 'public.platform_stats', 'select')

  union all

  select
    'FAIL',
    'users_own_profile_table_grants',
    'public.users',
    'authenticated lacks SELECT'
  where to_regclass('public.users') is null
     or not has_table_privilege('authenticated', 'public.users', 'select')
)
select severity, check_name, object_name, detail
from checks

union all

select
  'PASS',
  'summary',
  'security_posture',
  'No failures detected by CLI audit'
where not exists (select 1 from checks)
order by severity, check_name, object_name, detail;
