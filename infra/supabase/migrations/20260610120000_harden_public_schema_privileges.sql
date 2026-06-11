-- Harden browser-visible Supabase privileges before beta.
--
-- The Next.js API and worker paths use service-role/direct database access for
-- product data. Browser roles should only reach explicitly public or own-profile
-- surfaces through RLS.

begin;

-- Future objects created by the migration role should start private. Grant
-- browser access explicitly in the migration that introduces a public surface.
alter default privileges in schema public revoke all on tables from public, anon, authenticated;
alter default privileges in schema public revoke all on sequences from public, anon, authenticated;
alter default privileges in schema public revoke execute on functions from public, anon, authenticated;
alter default privileges in schema public grant all on tables to service_role;
alter default privileges in schema public grant all on sequences to service_role;
alter default privileges in schema public grant execute on functions to service_role;

-- Current RPC/function posture: service role can run backend/worker RPCs; browser
-- roles cannot call SECURITY DEFINER maintenance functions directly.
revoke execute on all functions in schema public from public, anon, authenticated;
grant execute on all functions in schema public to service_role;

-- Service role remains the explicit backend escape hatch for API routes and
-- workers after browser grants are removed.
grant all privileges on all tables in schema public to service_role;
grant all privileges on all sequences in schema public to service_role;

-- Lock product, worker, model-output, and operational tables behind service
-- role/direct DB access. RLS adds a second guard in case broad grants are added
-- later by tooling or the dashboard.
do $$
declare
  table_name text;
  private_tables text[] := array[
    'feeds',
    'feeders',
    'posts',
    'run_jobs',
    'checkpoint_jobs',
    'post_metrics',
    'fire_alerts',
    'post_media_assets',
    'feeder_baselines',
    'feeder_hour_baselines',
    'feeder_follower_snapshots',
    'signals',
    'signal_posts',
    'post_fingerprints',
    'post_breakdowns',
    'post_condensations',
    'feeder_files',
    'feeder_file_patterns',
    'feeder_file_model_calls',
    'feeder_file_winner_snapshots',
    -- Retired or manually-created surfaces that may still exist in older envs.
    'post_intelligence',
    'signal_intelligence',
    'feeder_focus',
    'feed_focus',
    'post_focus_reads',
    'focus_compile_locks',
    'pipeline_audit_daily',
    'engine_state',
    'transactions'
  ];
begin
  foreach table_name in array private_tables loop
    if to_regclass(format('public.%I', table_name)) is not null then
      execute format('alter table public.%I enable row level security', table_name);
      execute format('revoke all privileges on table public.%I from public, anon, authenticated', table_name);
    end if;
  end loop;
end $$;

-- Sequence access is not useful to browser roles for private tables. Re-grant
-- only sequences tied to client-owned tables below.
revoke all privileges on all sequences in schema public from public, anon, authenticated;

-- Own-profile surface. Keep direct browser bootstrap working, but do not allow
-- clients to write accounting/usage counters after creation.
alter table if exists public.users enable row level security;

revoke all privileges on table public.users from public, anon, authenticated;
grant select on table public.users to authenticated;
grant insert (id, email, name, balance, email_notifications, fire_alert_threshold, pwa_push_enabled)
  on public.users to authenticated;
grant update (name, email_notifications, fire_alert_threshold, pwa_push_enabled)
  on public.users to authenticated;

do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'users'
      and column_name = 'avatar_url'
  ) then
    grant insert (avatar_url) on public.users to authenticated;
    grant update (avatar_url) on public.users to authenticated;
  end if;
end $$;

drop policy if exists "Users can read own profile" on public.users;
create policy "Users can read own profile"
  on public.users
  for select
  to authenticated
  using (auth.uid() = id);

drop policy if exists "Users can insert own profile" on public.users;
create policy "Users can insert own profile"
  on public.users
  for insert
  to authenticated
  with check (
    auth.uid() = id
    and balance = 1000
  );

drop policy if exists "Users can update own profile" on public.users;
create policy "Users can update own profile"
  on public.users
  for update
  to authenticated
  using (auth.uid() = id)
  with check (auth.uid() = id);

-- Existing push RLS policies are own-row scoped. These grants make that intent
-- reproducible without exposing push rows across users.
alter table if exists public.web_push_subscriptions enable row level security;
alter table if exists public.web_push_jobs enable row level security;

revoke all privileges on table public.web_push_subscriptions from public, anon, authenticated;
grant select, insert, update, delete on table public.web_push_subscriptions to authenticated;
grant usage, select on sequence public.web_push_subscriptions_id_seq to authenticated;

revoke all privileges on table public.web_push_jobs from public, anon, authenticated;
grant select on table public.web_push_jobs to authenticated;

-- Keep the intentionally public aggregate rollup public, and only that rollup.
alter table if exists public.platform_stats enable row level security;

drop policy if exists platform_stats_public_read on public.platform_stats;
create policy platform_stats_public_read
  on public.platform_stats
  for select
  to anon, authenticated
  using (true);

revoke all privileges on table public.platform_stats from public, anon, authenticated;
grant select on table public.platform_stats to anon, authenticated;

commit;
