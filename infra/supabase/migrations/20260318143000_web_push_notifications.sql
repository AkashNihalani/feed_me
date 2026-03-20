-- Real Web Push delivery for Fire alerts.

do $$
begin
  if to_regclass('public.users') is null then
    create table public.users (
      id uuid primary key references auth.users(id) on delete cascade,
      email text not null default '',
      name text not null default 'User',
      balance numeric not null default 0,
      total_runs integer not null default 0,
      data_points integer not null default 0,
      success_rate numeric not null default 0,
      email_notifications boolean not null default true,
      avatar_url text,
      twitter_posts_caught integer not null default 0,
      reddit_posts_caught integer not null default 0,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    );
  end if;
end
$$;

alter table public.users
  add column if not exists fire_alert_threshold integer not null default 25,
  add column if not exists pwa_push_enabled boolean not null default false;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'users_fire_alert_threshold_check'
      and conrelid = 'public.users'::regclass
  ) then
    alter table public.users
      add constraint users_fire_alert_threshold_check
      check (fire_alert_threshold between 1 and 100);
  end if;
end
$$;

create table if not exists public.web_push_subscriptions (
  id bigserial primary key,
  user_id uuid not null references auth.users(id) on delete cascade,
  endpoint text not null,
  p256dh_key text not null,
  auth_key text not null,
  user_agent text,
  enabled boolean not null default true,
  last_error text,
  last_seen_at timestamptz not null default now(),
  failed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (endpoint)
);

create index if not exists web_push_subscriptions_user_enabled_idx
  on public.web_push_subscriptions(user_id, enabled, updated_at desc);

create table if not exists public.web_push_jobs (
  id bigserial primary key,
  user_id uuid not null references auth.users(id) on delete cascade,
  kind text not null default 'fire' check (kind in ('fire', 'test')),
  fire_alert_id bigint references public.fire_alerts(id) on delete cascade,
  feed_id bigint references public.feeds(id) on delete cascade,
  dedupe_key text not null,
  payload jsonb not null default '{}'::jsonb,
  status text not null default 'pending' check (status in ('pending', 'running', 'retry', 'sent', 'failed', 'skipped')),
  attempt integer not null default 0,
  next_run_at timestamptz not null default now(),
  claimed_at timestamptz,
  sent_at timestamptz,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (dedupe_key)
);

create index if not exists web_push_jobs_due_idx
  on public.web_push_jobs(next_run_at asc, id asc)
  where status in ('pending', 'retry');

create index if not exists web_push_jobs_user_status_idx
  on public.web_push_jobs(user_id, status, created_at desc);

create or replace function public.claim_web_push_jobs(p_limit integer default 100)
returns setof public.web_push_jobs
language sql
security definer
set search_path = public
as $$
  with picked as (
    select id
    from public.web_push_jobs
    where status in ('pending', 'retry')
      and next_run_at <= now()
    order by next_run_at asc, created_at asc, id asc
    for update skip locked
    limit greatest(1, p_limit)
  ), updated as (
    update public.web_push_jobs jobs
    set status = 'running',
        claimed_at = now(),
        updated_at = now()
    from picked
    where jobs.id = picked.id
    returning jobs.*
  )
  select * from updated;
$$;

create or replace function public.tg_enqueue_web_push_jobs()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_user_id uuid;
  v_threshold integer := 25;
  v_enabled boolean := false;
begin
  select
    feeds.user_id,
    coalesce(users.fire_alert_threshold, 25),
    coalesce(users.pwa_push_enabled, false)
  into v_user_id, v_threshold, v_enabled
  from public.feeds feeds
  left join public.users users on users.id = feeds.user_id
  where feeds.id = new.feed_id;

  if v_user_id is null or not v_enabled then
    return new;
  end if;

  if new.signal_code <> 'slot_v3'
     or new.context <> 'own'
     or coalesce(new.surface_percentile, 101) > v_threshold
     or coalesce(new.status, 'new') in ('dropped', 'error', 'archived') then
    return new;
  end if;

  insert into public.web_push_jobs (
    user_id,
    kind,
    fire_alert_id,
    feed_id,
    dedupe_key,
    payload
  )
  values (
    v_user_id,
    'fire',
    new.id,
    new.feed_id,
    format('fire:%s:%s', v_user_id::text, new.id::text),
    jsonb_build_object(
      'business_date_ist', new.business_date_ist,
      'checkpoint', new.checkpoint,
      'surface_percentile', new.surface_percentile
    )
  )
  on conflict (dedupe_key) do nothing;

  return new;
end;
$$;

drop trigger if exists trg_enqueue_web_push_jobs on public.fire_alerts;
create trigger trg_enqueue_web_push_jobs
after insert or update on public.fire_alerts
for each row
execute function public.tg_enqueue_web_push_jobs();

alter table public.users enable row level security;
alter table public.web_push_subscriptions enable row level security;
alter table public.web_push_jobs enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'users'
      and policyname = 'Users can read own profile'
  ) then
    create policy "Users can read own profile"
      on public.users
      for select
      using (auth.uid() = id);
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'users'
      and policyname = 'Users can insert own profile'
  ) then
    create policy "Users can insert own profile"
      on public.users
      for insert
      with check (auth.uid() = id);
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'users'
      and policyname = 'Users can update own profile'
  ) then
    create policy "Users can update own profile"
      on public.users
      for update
      using (auth.uid() = id)
      with check (auth.uid() = id);
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'web_push_subscriptions'
      and policyname = 'Users can manage own web push subscriptions'
  ) then
    create policy "Users can manage own web push subscriptions"
      on public.web_push_subscriptions
      for all
      using (auth.uid() = user_id)
      with check (auth.uid() = user_id);
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'web_push_jobs'
      and policyname = 'Users can read own web push jobs'
  ) then
    create policy "Users can read own web push jobs"
      on public.web_push_jobs
      for select
      using (auth.uid() = user_id);
  end if;
end
$$;
