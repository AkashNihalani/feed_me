-- Read-only Supabase security posture check.
--
-- Run this in the Supabase SQL editor after applying:
--   infra/supabase/migrations/20260610120000_harden_public_schema_privileges.sql
--
-- The two most important result sets are:
--   2_private_table_browser_grants_should_be_empty
--   3_public_function_browser_execute_should_be_empty
-- Both should return zero rows.

-- 1) Existing private tables should have RLS enabled. Missing optional/staged
-- tables are reported as SKIP, not FAIL.
with expected_private_tables(table_name) as (
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
)
select
  '1_private_table_rls' as check_name,
  e.table_name,
  case
    when c.oid is null then 'SKIP: missing table'
    when c.relrowsecurity is not true then 'FAIL: RLS disabled'
    else 'PASS'
  end as result,
  coalesce(c.relrowsecurity, false) as rls_enabled,
  coalesce(c.relforcerowsecurity, false) as force_rls_enabled
from expected_private_tables e
left join pg_class c
  on c.oid = to_regclass(format('public.%I', e.table_name))
order by e.table_name;

-- 2) Browser roles should have no privileges on private tables.
with private_tables(table_name) as (
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
)
select
  '2_private_table_browser_grants_should_be_empty' as check_name,
  g.table_name,
  g.grantee,
  g.privilege_type
from information_schema.role_table_grants g
join private_tables p on p.table_name = g.table_name
where g.table_schema = 'public'
  and g.grantee in ('anon', 'authenticated', 'PUBLIC')
order by g.table_name, g.grantee, g.privilege_type;

-- 3) Browser roles should not be able to execute public functions/RPCs.
select
  '3_public_function_browser_execute_should_be_empty' as check_name,
  p.proname as function_name,
  pg_get_function_identity_arguments(p.oid) as identity_arguments,
  has_function_privilege('anon', p.oid, 'execute') as anon_can_execute,
  has_function_privilege('authenticated', p.oid, 'execute') as authenticated_can_execute
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
where n.nspname = 'public'
  and (
    has_function_privilege('anon', p.oid, 'execute')
    or has_function_privilege('authenticated', p.oid, 'execute')
  )
order by p.proname, identity_arguments;

-- 4) These are the only browser-readable public tables expected by the app.
select
  '4_allowed_browser_table_grants' as check_name,
  g.table_name,
  g.grantee,
  g.privilege_type
from information_schema.role_table_grants g
where g.table_schema = 'public'
  and g.table_name in ('users', 'web_push_subscriptions', 'web_push_jobs', 'platform_stats')
  and g.grantee in ('anon', 'authenticated', 'PUBLIC')
order by g.table_name, g.grantee, g.privilege_type;

-- 5) Users should not have UPDATE privilege on accounting columns.
select
  '5_users_column_privileges' as check_name,
  cp.column_name,
  cp.grantee,
  cp.privilege_type
from information_schema.column_privileges cp
where cp.table_schema = 'public'
  and cp.table_name = 'users'
  and cp.grantee in ('anon', 'authenticated', 'PUBLIC')
order by cp.grantee, cp.privilege_type, cp.column_name;

-- 6) Show RLS policies on the intentionally browser-reachable tables.
select
  '6_expected_rls_policies' as check_name,
  tablename,
  policyname,
  roles,
  cmd,
  qual,
  with_check
from pg_policies
where schemaname = 'public'
  and tablename in ('users', 'web_push_subscriptions', 'web_push_jobs', 'platform_stats')
order by tablename, policyname;
