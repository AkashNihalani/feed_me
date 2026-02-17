-- FeedMe baseline minimal self-healing engine

create table if not exists public.feeds (
  id bigserial primary key,
  user_id uuid not null references auth.users(id) on delete cascade,
  name text not null,
  status text not null default 'active' check (status in ('active','paused','archived')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);



create index if not exists feeds_user_status_idx on public.feeds(user_id, status);

create table if not exists public.feeders (
  id bigserial primary key,
  feed_id bigint not null references public.feeds(id) on delete cascade,
  handle text not null,
  role text not null default 'standard' check (role in ('anchor','standard')),
  status text not null default 'active' check (status in ('active','paused','removed')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (feed_id, handle)
);



create unique index if not exists feeders_one_anchor_idx
on public.feeders(feed_id)
where role = 'anchor' and status = 'active';

create table if not exists public.posts (
  post_key text primary key,
  feeder_id bigint not null references public.feeders(id) on delete cascade,
  post_url text not null,
  media_type text,
  posted_at timestamptz,
  caption text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (feeder_id, post_url)
);
create index if not exists posts_feeder_posted_idx on public.posts(feeder_id, posted_at desc);

create table if not exists public.post_metrics (
  post_key text not null references public.posts(post_key) on delete cascade,
  checkpoint text not null check (checkpoint in ('d1','d2','d3','d7','d21')),
  views bigint,
  likes bigint,
  comments bigint,
  metric_value numeric,
  velocity_value numeric,
  velocity_percentile int check (velocity_percentile between 1 and 100),
  velocity_tag text,
  perf_score numeric,
  computed_at timestamptz not null default now(),
  primary key (post_key, checkpoint)
);
create index if not exists post_metrics_checkpoint_idx on public.post_metrics(checkpoint, computed_at desc);

create table if not exists public.run_jobs (
  id bigserial primary key,
  feeder_id bigint not null references public.feeders(id) on delete cascade,
  job_type text not null check (job_type in ('daily','weekly')),
  status text not null default 'pending' check (status in ('pending','running','retry','done','failed','skipped')),
  attempt int not null default 0,
  next_run_at timestamptz not null default now(),
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index if not exists run_jobs_due_idx
on public.run_jobs(next_run_at, id)
where status in ('pending','retry');
create unique index if not exists run_jobs_open_unique_idx
on public.run_jobs(feeder_id, job_type)
where status in ('pending','running','retry');

create table if not exists public.checkpoint_jobs (
  id bigserial primary key,
  post_key text not null references public.posts(post_key) on delete cascade,
  checkpoint text not null check (checkpoint in ('d3','d7','d21')),
  status text not null default 'pending' check (status in ('pending','running','retry','done','failed','skipped')),
  attempt int not null default 0,
  next_run_at timestamptz not null,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (post_key, checkpoint)
);
create index if not exists checkpoint_jobs_due_idx
on public.checkpoint_jobs(next_run_at, id)
where status in ('pending','retry');

create table if not exists public.fire_alerts (
  id bigserial primary key,
  feed_id bigint not null references public.feeds(id) on delete cascade,
  alert_type text not null check (alert_type in ('spark','burn','blaze')),
  title text not null,
  body text not null,
  payload jsonb not null default '{}'::jsonb,
  priority_score numeric not null default 0,
  status text not null default 'new' check (status in ('new','sent','dismissed')),
  created_at timestamptz not null default now(),
  sent_at timestamptz
);
create index if not exists fire_alerts_feed_created_idx on public.fire_alerts(feed_id, created_at desc);

create table if not exists public.engine_state (
  key text primary key,
  value_json jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);

create or replace function public.fn_post_key(p_url text)
returns text
language plpgsql
immutable
as $$
declare
  v text;
begin
  v := lower(coalesce(p_url, ''));
  v := regexp_replace(v, '^https?://(www\.)?instagram\.com/', '', 'i');
  v := split_part(v, '?', 1);
  v := split_part(v, '#', 1);
  v := trim(both '/' from v);
  return v;
end;
$$;

create or replace function public.fn_checkpoint_due_at(
  p_posted_at timestamptz,
  p_days_after int,
  p_tz text default 'Asia/Kolkata',
  p_hour int default 23,
  p_minute int default 30
)
returns timestamptz
language plpgsql
stable
as $$
declare
  v_local_date date;
  v_target_local timestamp;
  v_target_utc timestamptz;
begin
  if p_posted_at is null then
    return now();
  end if;
  v_local_date := ((p_posted_at at time zone p_tz)::date + greatest(0, p_days_after));
  v_target_local := (v_local_date::timestamp + make_interval(hours => p_hour, mins => p_minute));
  v_target_utc := (v_target_local at time zone p_tz);
  if v_target_utc < now() then
    return now();
  end if;
  return v_target_utc;
end;
$$;

create or replace function public.enqueue_daily_jobs(p_run_at timestamptz default now())
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
begin
  insert into public.run_jobs (feeder_id, job_type, status, next_run_at)
  select fd.id, 'daily', 'pending', p_run_at
  from public.feeders fd
  join public.feeds f on f.id = fd.feed_id
  where f.status='active' and fd.status='active'
  on conflict do nothing;
  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

create or replace function public.enqueue_weekly_jobs(p_run_at timestamptz default now())
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
begin
  insert into public.run_jobs (feeder_id, job_type, status, next_run_at)
  select fd.id, 'weekly', 'pending', p_run_at
  from public.feeders fd
  join public.feeds f on f.id = fd.feed_id
  where f.status='active' and fd.status='active'
  on conflict do nothing;
  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

create or replace function public.enqueue_checkpoint_jobs(
  p_post_key text,
  p_posted_at timestamptz,
  p_tz text default 'Asia/Kolkata',
  p_hour int default 23,
  p_minute int default 30
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
begin
  insert into public.checkpoint_jobs (post_key, checkpoint, status, next_run_at)
  values
    (p_post_key, 'd3', 'pending', public.fn_checkpoint_due_at(p_posted_at, 3, p_tz, p_hour, p_minute)),
    (p_post_key, 'd7', 'pending', public.fn_checkpoint_due_at(p_posted_at, 7, p_tz, p_hour, p_minute)),
    (p_post_key, 'd21', 'pending', public.fn_checkpoint_due_at(p_posted_at, 21, p_tz, p_hour, p_minute))
  on conflict do nothing;
  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

create or replace function public.claim_run_jobs(p_limit int default 25)
returns setof public.run_jobs
language sql
security definer
set search_path = public
as $$
  with picked as (
    select id from public.run_jobs
    where status in ('pending','retry') and next_run_at <= now()
    order by next_run_at asc, id asc
    for update skip locked
    limit greatest(1, p_limit)
  ), updated as (
    update public.run_jobs r
    set status='running', updated_at=now()
    from picked
    where r.id = picked.id
    returning r.*
  )
  select * from updated;
$$;

create or replace function public.skip_unqualified_d21_jobs()
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
begin
  update public.checkpoint_jobs cj
  set status='skipped',
      last_error='D21 skipped because D7 was not hot (top 35%)',
      updated_at=now()
  where cj.status in ('pending','retry')
    and cj.checkpoint='d21'
    and cj.next_run_at <= now()
    and not exists (
      select 1 from public.post_metrics pm
      where pm.post_key = cj.post_key
        and pm.checkpoint='d7'
        and pm.velocity_tag in ('✅','🔥','🚀')
    );
  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

create or replace function public.claim_checkpoint_jobs(p_limit int default 100)
returns setof public.checkpoint_jobs
language sql
security definer
set search_path = public
as $$
  with picked as (
    select id from public.checkpoint_jobs
    where status in ('pending','retry') and next_run_at <= now()
    order by next_run_at asc, id asc
    for update skip locked
    limit greatest(1, p_limit)
  ), updated as (
    update public.checkpoint_jobs cj
    set status='running', updated_at=now()
    from picked
    where cj.id = picked.id
    returning cj.*
  )
  select * from updated;
$$;

create or replace function public.set_run_job_result(
  p_job_id bigint,
  p_status text,
  p_attempt int default null,
  p_next_run_at timestamptz default null,
  p_error text default null
)
returns void
language sql
security definer
set search_path = public
as $$
  update public.run_jobs
  set status=p_status,
      attempt=coalesce(p_attempt, attempt),
      next_run_at=coalesce(p_next_run_at, next_run_at),
      last_error=case when p_error is null then null else left(p_error, 1000) end,
      updated_at=now()
  where id = p_job_id;
$$;

create or replace function public.set_checkpoint_job_result(
  p_job_id bigint,
  p_status text,
  p_attempt int default null,
  p_next_run_at timestamptz default null,
  p_error text default null
)
returns void
language sql
security definer
set search_path = public
as $$
  update public.checkpoint_jobs
  set status=p_status,
      attempt=coalesce(p_attempt, attempt),
      next_run_at=coalesce(p_next_run_at, next_run_at),
      last_error=case when p_error is null then null else left(p_error, 1000) end,
      updated_at=now()
  where id = p_job_id;
$$;

create or replace function public.requeue_stale_jobs(p_minutes int default 30)
returns table (run_rows int, checkpoint_rows int)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_run_rows int := 0;
  v_checkpoint_rows int := 0;
  v_minutes int := greatest(1, coalesce(p_minutes, 30));
begin
  update public.run_jobs
  set status='retry',
      next_run_at=now(),
      last_error=format('Recovered stale running job after %s minutes watchdog', v_minutes),
      updated_at=now()
  where status='running'
    and updated_at < now() - (v_minutes::text || ' minutes')::interval;
  get diagnostics v_run_rows = row_count;

  update public.checkpoint_jobs
  set status='retry',
      next_run_at=now(),
      last_error=format('Recovered stale running checkpoint job after %s minutes watchdog', v_minutes),
      updated_at=now()
  where status='running'
    and updated_at < now() - (v_minutes::text || ' minutes')::interval;
  get diagnostics v_checkpoint_rows = row_count;

  return query select v_run_rows, v_checkpoint_rows;
end;
$$;

alter table public.feeds enable row level security;
alter table public.feeders enable row level security;
alter table public.posts enable row level security;
alter table public.post_metrics enable row level security;
alter table public.fire_alerts enable row level security;
alter table public.run_jobs enable row level security;
alter table public.checkpoint_jobs enable row level security;
alter table public.engine_state enable row level security;

do $$
begin
  if not exists (select 1 from pg_policies where schemaname='public' and tablename='feeds' and policyname='Users can read own feeds') then
    create policy "Users can read own feeds" on public.feeds for select using (auth.uid() = user_id);
  end if;

  if not exists (select 1 from pg_policies where schemaname='public' and tablename='feeders' and policyname='Users can read own feeders') then
    create policy "Users can read own feeders"
      on public.feeders for select
      using (exists (select 1 from public.feeds f where f.id = feeders.feed_id and f.user_id = auth.uid()));
  end if;

  if not exists (select 1 from pg_policies where schemaname='public' and tablename='posts' and policyname='Users can read own posts') then
    create policy "Users can read own posts"
      on public.posts for select
      using (exists (
        select 1
        from public.feeders fd
        join public.feeds f on f.id = fd.feed_id
        where fd.id = posts.feeder_id and f.user_id = auth.uid()
      ));
  end if;

  if not exists (select 1 from pg_policies where schemaname='public' and tablename='post_metrics' and policyname='Users can read own post metrics') then
    create policy "Users can read own post metrics"
      on public.post_metrics for select
      using (exists (
        select 1
        from public.posts p
        join public.feeders fd on fd.id = p.feeder_id
        join public.feeds f on f.id = fd.feed_id
        where p.post_key = post_metrics.post_key and f.user_id = auth.uid()
      ));
  end if;

  if not exists (select 1 from pg_policies where schemaname='public' and tablename='fire_alerts' and policyname='Users can read own fire alerts') then
    create policy "Users can read own fire alerts"
      on public.fire_alerts for select
      using (exists (select 1 from public.feeds f where f.id = fire_alerts.feed_id and f.user_id = auth.uid()));
  end if;
end $$;
