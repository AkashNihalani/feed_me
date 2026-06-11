-- One-statement operational apply script for the beta security hardening.
--
-- Use only when normal `supabase db push` cannot be used because remote
-- migration history is already out of sync. This applies the same privilege/RLS
-- posture as:
--   infra/supabase/migrations/20260610120000_harden_public_schema_privileges.sql

do $hardening$
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
  execute 'alter default privileges in schema public revoke all on tables from public, anon, authenticated';
  execute 'alter default privileges in schema public revoke all on sequences from public, anon, authenticated';
  execute 'alter default privileges in schema public revoke execute on functions from public, anon, authenticated';
  execute 'alter default privileges in schema public grant all on tables to service_role';
  execute 'alter default privileges in schema public grant all on sequences to service_role';
  execute 'alter default privileges in schema public grant execute on functions to service_role';

  execute 'revoke execute on all functions in schema public from public, anon, authenticated';
  execute 'grant execute on all functions in schema public to service_role';

  execute 'grant all privileges on all tables in schema public to service_role';
  execute 'grant all privileges on all sequences in schema public to service_role';

  foreach table_name in array private_tables loop
    if to_regclass(format('public.%I', table_name)) is not null then
      execute format('alter table public.%I enable row level security', table_name);
      execute format('revoke all privileges on table public.%I from public, anon, authenticated', table_name);
    end if;
  end loop;

  execute 'revoke all privileges on all sequences in schema public from public, anon, authenticated';

  if to_regclass('public.users') is not null then
    execute 'alter table public.users enable row level security';
    execute 'revoke all privileges on table public.users from public, anon, authenticated';
    execute 'grant select on table public.users to authenticated';
    execute 'grant insert (id, email, name, balance, email_notifications, fire_alert_threshold, pwa_push_enabled) on public.users to authenticated';
    execute 'grant update (name, email_notifications, fire_alert_threshold, pwa_push_enabled) on public.users to authenticated';

    if exists (
      select 1
      from information_schema.columns c
      where c.table_schema = 'public'
        and c.table_name = 'users'
        and c.column_name = 'avatar_url'
    ) then
      execute 'grant insert (avatar_url) on public.users to authenticated';
      execute 'grant update (avatar_url) on public.users to authenticated';
    end if;

    execute 'drop policy if exists "Users can read own profile" on public.users';
    execute $ddl$
      create policy "Users can read own profile"
        on public.users
        for select
        to authenticated
        using (auth.uid() = id)
    $ddl$;

    execute 'drop policy if exists "Users can insert own profile" on public.users';
    execute $ddl$
      create policy "Users can insert own profile"
        on public.users
        for insert
        to authenticated
        with check (
          auth.uid() = id
          and balance = 1000
        )
    $ddl$;

    execute 'drop policy if exists "Users can update own profile" on public.users';
    execute $ddl$
      create policy "Users can update own profile"
        on public.users
        for update
        to authenticated
        using (auth.uid() = id)
        with check (auth.uid() = id)
    $ddl$;
  end if;

  if to_regclass('public.web_push_subscriptions') is not null then
    execute 'alter table public.web_push_subscriptions enable row level security';
    execute 'revoke all privileges on table public.web_push_subscriptions from public, anon, authenticated';
    execute 'grant select, insert, update, delete on table public.web_push_subscriptions to authenticated';
  end if;

  if to_regclass('public.web_push_jobs') is not null then
    execute 'alter table public.web_push_jobs enable row level security';
    execute 'revoke all privileges on table public.web_push_jobs from public, anon, authenticated';
    execute 'grant select on table public.web_push_jobs to authenticated';
  end if;

  if to_regclass('public.web_push_subscriptions_id_seq') is not null then
    execute 'grant usage, select on sequence public.web_push_subscriptions_id_seq to authenticated';
  end if;

  if to_regclass('public.platform_stats') is not null then
    execute 'alter table public.platform_stats enable row level security';
    execute 'drop policy if exists platform_stats_public_read on public.platform_stats';
    execute $ddl$
      create policy platform_stats_public_read
        on public.platform_stats
        for select
        to anon, authenticated
        using (true)
    $ddl$;
    execute 'revoke all privileges on table public.platform_stats from public, anon, authenticated';
    execute 'grant select on table public.platform_stats to anon, authenticated';
  end if;
end
$hardening$;
