begin;

create table if not exists public.users (
  id uuid primary key references auth.users(id) on delete cascade,
  email text not null default '',
  name text not null default 'User',
  balance numeric not null default 0,
  total_runs integer not null default 0,
  data_points integer not null default 0,
  success_rate numeric not null default 0,
  email_notifications boolean not null default true,
  fire_alert_threshold integer not null default 25,
  pwa_push_enabled boolean not null default false,
  avatar_url text,
  twitter_posts_caught integer not null default 0,
  reddit_posts_caught integer not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.users
  drop constraint if exists users_fire_alert_threshold_check;

alter table public.users
  add constraint users_fire_alert_threshold_check
  check (fire_alert_threshold between 1 and 100);

create table if not exists public.feeds (
  id bigserial primary key,
  user_id uuid not null references auth.users(id) on delete cascade,
  name text not null,
  status text not null default 'active' check (status in ('active', 'paused', 'archived')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists feeds_user_status_idx
  on public.feeds(user_id, status, created_at desc);

create table if not exists public.feeders (
  id bigserial primary key,
  feed_id bigint not null references public.feeds(id) on delete cascade,
  handle text not null,
  role text not null default 'standard' check (role in ('anchor', 'standard')),
  status text not null default 'active' check (status in ('active', 'paused', 'removed')),
  profile_pic_url text,
  follower_count bigint,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (feed_id, handle)
);

create index if not exists feeders_feed_status_idx
  on public.feeders(feed_id, status, updated_at desc);

create unique index if not exists feeders_one_anchor_idx
  on public.feeders(feed_id)
  where role = 'anchor' and status = 'active';

create table if not exists public.posts (
  post_key text primary key,
  feeder_id bigint not null references public.feeders(id) on delete cascade,
  provider_post_id text,
  post_url text not null,
  media_type text,
  posted_at timestamptz,
  caption text,
  thumbnail_url text,
  video_url text,
  carousel_urls jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (feeder_id, post_url)
);

create index if not exists posts_feeder_posted_idx
  on public.posts(feeder_id, posted_at desc, updated_at desc);

create index if not exists posts_provider_post_id_idx
  on public.posts(feeder_id, provider_post_id)
  where provider_post_id is not null;

create table if not exists public.run_jobs (
  id bigserial primary key,
  feeder_id bigint not null references public.feeders(id) on delete cascade,
  job_type text not null default 'daily' check (job_type in ('daily', 'repair', 'poll')),
  business_date_ist date,
  status text not null default 'pending' check (status in ('pending', 'running', 'retry', 'done', 'failed', 'skipped')),
  attempt int not null default 0,
  next_run_at timestamptz not null default now(),
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists run_jobs_due_idx
  on public.run_jobs(next_run_at asc, id asc)
  where status in ('pending', 'retry');

create table if not exists public.checkpoint_jobs (
  id bigserial primary key,
  post_key text not null references public.posts(post_key) on delete cascade,
  checkpoint text not null check (checkpoint in ('d1', 'd3', 'd7', 'd21')),
  status text not null default 'pending' check (status in ('pending', 'running', 'retry', 'done', 'failed', 'skipped')),
  attempt int not null default 0,
  next_run_at timestamptz not null default now(),
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (post_key, checkpoint)
);

create index if not exists checkpoint_jobs_due_idx
  on public.checkpoint_jobs(next_run_at asc, id asc)
  where status in ('pending', 'retry');

create table if not exists public.post_metrics (
  post_key text not null references public.posts(post_key) on delete cascade,
  checkpoint text not null check (checkpoint in ('d1', 'd3', 'd7', 'd21')),
  business_date_ist date,
  computed_at timestamptz not null default now(),
  views bigint,
  likes bigint,
  comments bigint,
  metric_value numeric,
  percentile_performance int,
  views_percentile int,
  likes_percentile int,
  comments_percentile int,
  feed_percentile int,
  delta_from_d1 int,
  primary key (post_key, checkpoint)
);

create table if not exists public.fire_alerts (
  id bigserial primary key,
  dedupe_key text not null,
  feed_id bigint not null references public.feeds(id) on delete cascade,
  feeder_id bigint not null references public.feeders(id) on delete cascade,
  post_key text not null references public.posts(post_key) on delete cascade,
  checkpoint text not null check (checkpoint in ('d1', 'd3', 'd7', 'd21')),
  business_date_ist date not null,
  signal_code text not null default 'slot_v3',
  context text not null default 'own',
  alert_type text not null default 'spark' check (alert_type in ('spark', 'burn', 'blaze')),
  status text not null default 'new' check (status in ('new', 'sent', 'dismissed', 'archived', 'dropped', 'error')),
  metric_key text,
  metric_value numeric,
  surface_percentile int,
  surface_delta int,
  body text not null default '',
  pattern_signal text,
  pattern_payload jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.post_media_assets (
  id bigserial primary key,
  post_key text not null references public.posts(post_key) on delete cascade,
  asset_role text not null,
  source_url text,
  storage_bucket text,
  storage_path text,
  mime_type text,
  byte_size bigint,
  status text not null default 'pending_capture'
    check (status in ('pending_capture', 'capturing', 'active', 'capture_failed', 'purge_pending', 'purging', 'purge_failed', 'deleted')),
  attempt integer not null default 0,
  next_run_at timestamptz not null default now(),
  captured_at timestamptz,
  purge_after timestamptz,
  deleted_at timestamptz,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (post_key, asset_role)
);

create index if not exists post_media_assets_capture_due_idx
  on public.post_media_assets(next_run_at asc, id asc)
  where status in ('pending_capture', 'capture_failed');

create index if not exists post_media_assets_purge_due_idx
  on public.post_media_assets(purge_after asc, id asc)
  where status in ('active', 'purge_pending', 'purge_failed');

create index if not exists post_media_assets_post_key_idx
  on public.post_media_assets(post_key, asset_role, status);

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

do $$
begin
  if exists (
    select 1
    from pg_namespace
    where nspname = 'storage'
  ) then
    insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
    values (
      'fire-media',
      'fire-media',
      false,
      52428800,
      array[
        'image/jpeg',
        'image/png',
        'image/webp',
        'image/avif',
        'video/mp4',
        'video/quicktime'
      ]::text[]
    )
    on conflict (id) do update
      set public = excluded.public,
          file_size_limit = excluded.file_size_limit,
          allowed_mime_types = excluded.allowed_mime_types;
  end if;
end $$;

drop view if exists public.v_fire_alert_surface;
drop view if exists public.v_post_tagged_performance;

drop trigger if exists trg_00_post_metrics_contract on public.post_metrics;
drop trigger if exists trg_compute_post_metrics on public.post_metrics;
drop trigger if exists trg_refresh_metric_baselines on public.post_metrics;
drop trigger if exists trg_20_schedule_fire_media_retention_from_d7 on public.post_metrics;
drop trigger if exists trg_capture_feeder_follower_snapshot on public.feeders;
drop trigger if exists trg_apply_fire_alert_contract on public.fire_alerts;
drop trigger if exists trg_sync_fire_alert_media_payload on public.posts;
drop trigger if exists trg_enqueue_web_push_jobs on public.fire_alerts;

drop table if exists public.pipeline_audit_daily;
drop table if exists public.engine_state;

drop function if exists public.enqueue_weekly_jobs(timestamptz);
drop function if exists public.fn_percentile_tag(int);
drop function if exists public.tg_sync_fire_alert_media_payload();
drop function if exists public.sync_fire_alert_media_payload_for_post(text);

drop index if exists public.posts_media_fetched_idx;

alter table if exists public.feeders
  add column if not exists profile_pic_url text,
  add column if not exists follower_count bigint;

alter table if exists public.feeders
  drop column if exists verification_status,
  drop column if exists verification_error,
  drop column if exists verified_at,
  drop column if exists profile_pic_fetched_at;

alter table if exists public.posts
  add column if not exists provider_post_id text,
  add column if not exists thumbnail_url text,
  add column if not exists video_url text,
  add column if not exists carousel_urls jsonb;

update public.posts
set carousel_urls = '[]'::jsonb
where carousel_urls is null;

alter table if exists public.posts
  alter column carousel_urls set default '[]'::jsonb;

alter table if exists public.posts
  drop column if exists display_url,
  drop column if exists audio_url,
  drop column if exists media_fetched_at;

alter table if exists public.post_metrics
  add column if not exists business_date_ist date,
  add column if not exists percentile_performance int,
  add column if not exists views_percentile int,
  add column if not exists likes_percentile int,
  add column if not exists comments_percentile int,
  add column if not exists feed_percentile int,
  add column if not exists delta_from_d1 int;

alter table if exists public.fire_alerts
  add column if not exists dedupe_key text,
  add column if not exists feeder_id bigint references public.feeders(id) on delete cascade,
  add column if not exists post_key text references public.posts(post_key) on delete cascade,
  add column if not exists checkpoint text,
  add column if not exists business_date_ist date,
  add column if not exists signal_code text default 'slot_v3',
  add column if not exists context text default 'own',
  add column if not exists metric_key text,
  add column if not exists metric_value numeric,
  add column if not exists surface_percentile int,
  add column if not exists surface_delta int,
  add column if not exists pattern_signal text,
  add column if not exists pattern_payload jsonb,
  add column if not exists updated_at timestamptz not null default now();

create table if not exists public.post_intelligence (
  post_key text primary key references public.posts(post_key) on delete cascade,
  tags jsonb not null default '{}'::jsonb,
  model_version text not null default 'gemini-2.0-flash',
  extracted_at timestamptz not null default now()
);

create table if not exists public.feeder_baselines (
  feeder_id bigint not null references public.feeders(id) on delete cascade,
  media_type text not null,
  checkpoint text not null,
  median_views bigint,
  median_likes bigint,
  median_comments bigint,
  updated_at timestamptz not null default now(),
  primary key (feeder_id, media_type, checkpoint)
);

create table if not exists public.feeder_hour_baselines (
  feeder_id bigint not null references public.feeders(id) on delete cascade,
  media_type text not null,
  checkpoint text not null,
  hour_ist smallint not null check (hour_ist between 0 and 23),
  median_views bigint,
  median_likes bigint,
  median_comments bigint,
  updated_at timestamptz not null default now(),
  primary key (feeder_id, media_type, checkpoint, hour_ist)
);

create table if not exists public.feeder_follower_snapshots (
  feeder_id bigint not null references public.feeders(id) on delete cascade,
  week_start_ist date not null,
  follower_count bigint not null default 0,
  captured_at timestamptz not null default now(),
  primary key (feeder_id, week_start_ist)
);

truncate table
  public.web_push_jobs,
  public.fire_alerts,
  public.post_intelligence,
  public.post_media_assets,
  public.checkpoint_jobs,
  public.run_jobs,
  public.post_metrics,
  public.feeder_baselines,
  public.feeder_hour_baselines,
  public.feeder_follower_snapshots,
  public.posts
restart identity cascade;

alter table if exists public.run_jobs
  drop constraint if exists run_jobs_job_type_check;

alter table if exists public.run_jobs
  add constraint run_jobs_job_type_check
  check (job_type in ('daily', 'repair', 'poll'));

alter table if exists public.checkpoint_jobs
  drop constraint if exists checkpoint_jobs_checkpoint_check;

alter table if exists public.checkpoint_jobs
  add constraint checkpoint_jobs_checkpoint_check
  check (checkpoint in ('d1', 'd3', 'd7', 'd21'));

alter table if exists public.post_metrics
  drop constraint if exists post_metrics_checkpoint_check;

alter table if exists public.post_metrics
  add constraint post_metrics_checkpoint_check
  check (checkpoint in ('d1', 'd3', 'd7', 'd21'));

alter table if exists public.feeder_baselines
  drop column if exists median_delta,
  drop column if exists format_rank,
  drop column if exists format_count,
  drop column if exists best_percentile_90d,
  drop column if exists best_percentile_post,
  drop column if exists days_since_ceiling,
  drop column if exists post_count_90d,
  drop column if exists median_percentile,
  drop column if exists p25_percentile,
  drop column if exists p75_percentile,
  drop column if exists median_metric_value,
  drop column if exists sample_size;

alter table if exists public.feeder_hour_baselines
  drop column if exists median_metric_value,
  drop column if exists sample_size;

alter table if exists public.post_metrics
  drop column if exists captured_business_date_ist,
  drop column if exists d1_source,
  drop column if exists percentile_tag,
  drop column if exists velocity_value,
  drop column if exists velocity_percentile,
  drop column if exists velocity_tag,
  drop column if exists perf_score,
  drop column if exists rank_overall,
  drop column if exists rank_feed,
  drop column if exists rank_all_time,
  drop column if exists post_hour_ist,
  drop column if exists post_dow_ist,
  drop column if exists baseline_views,
  drop column if exists baseline_likes,
  drop column if exists baseline_comments,
  drop column if exists views_multiple,
  drop column if exists likes_multiple,
  drop column if exists comments_multiple,
  drop column if exists views_rank_feed,
  drop column if exists likes_rank_feed,
  drop column if exists comments_rank_feed,
  drop column if exists best_in_last_n_views,
  drop column if exists best_in_last_n_likes,
  drop column if exists best_in_last_n_comments,
  drop column if exists rank_recent_50,
  drop column if exists best_in_days,
  drop column if exists best_in_posts,
  drop column if exists outperformed_last_20,
  drop column if exists outperformed_last_50,
  drop column if exists hour_baseline_views,
  drop column if exists hour_baseline_likes,
  drop column if exists hour_baseline_comments,
  drop column if exists hour_multiple_views,
  drop column if exists hour_multiple_likes,
  drop column if exists hour_multiple_comments,
  drop column if exists hour_percentile_feeder,
  drop column if exists is_best_hour_window,
  drop column if exists percentile_delta,
  drop column if exists percentile_delta_vs_d1;

alter table if exists public.fire_alerts
  drop column if exists title cascade,
  drop column if exists headline cascade,
  drop column if exists payload cascade,
  drop column if exists intelligence_layers cascade,
  drop column if exists priority_score cascade,
  drop column if exists intensity_score cascade,
  drop column if exists confidence cascade,
  drop column if exists surface_shift cascade,
  drop column if exists surface_handle cascade,
  drop column if exists surface_feeder cascade,
  drop column if exists surface_media_type cascade,
  drop column if exists surface_checkpoint cascade,
  drop column if exists surface_metric_label cascade,
  drop column if exists surface_metric_value cascade,
  drop column if exists surface_stamp cascade,
  drop column if exists alert_family cascade,
  drop column if exists alert_urgency cascade,
  drop column if exists layer_key cascade,
  drop column if exists captured_business_date_ist cascade,
  drop column if exists sent_at cascade;

drop index if exists public.run_jobs_open_unique_idx;
drop index if exists public.run_jobs_open_scrape_lane_unique_idx;
drop index if exists public.run_jobs_open_poll_unique_idx;

create unique index if not exists run_jobs_open_scrape_lane_unique_idx
  on public.run_jobs(feeder_id, business_date_ist)
  where status in ('pending', 'running', 'retry')
    and job_type in ('daily', 'repair')
    and business_date_ist is not null;

create unique index if not exists run_jobs_open_poll_unique_idx
  on public.run_jobs(feeder_id, business_date_ist)
  where status in ('pending', 'running', 'retry')
    and job_type = 'poll'
    and business_date_ist is not null;

create index if not exists run_jobs_business_date_idx
  on public.run_jobs(business_date_ist, job_type, status, next_run_at);

create index if not exists post_metrics_business_checkpoint_idx
  on public.post_metrics(business_date_ist, checkpoint, percentile_performance, computed_at desc);

create index if not exists post_metrics_post_key_idx
  on public.post_metrics(post_key, checkpoint, business_date_ist);

create index if not exists fire_alerts_scope_idx
  on public.fire_alerts(feed_id, feeder_id, business_date_ist, created_at desc);

create index if not exists fire_alerts_percentile_idx
  on public.fire_alerts(business_date_ist, surface_percentile, created_at desc);

create unique index if not exists fire_alerts_dedupe_idx
  on public.fire_alerts(dedupe_key);

create index if not exists fire_alerts_pattern_signal_idx
  on public.fire_alerts(pattern_signal)
  where pattern_signal is not null;

create index if not exists post_intelligence_tags_idx
  on public.post_intelligence using gin(tags);

create index if not exists feeder_follower_snapshots_week_idx
  on public.feeder_follower_snapshots(week_start_ist desc, feeder_id);

create index if not exists feeder_baselines_checkpoint_idx
  on public.feeder_baselines(checkpoint, feeder_id);

create index if not exists feeder_hour_baselines_checkpoint_idx
  on public.feeder_hour_baselines(checkpoint, feeder_id, hour_ist);

create or replace function public.fn_metric_value(
  p_media_type text,
  p_views bigint,
  p_likes bigint,
  p_comments bigint
)
returns numeric
language sql
immutable
as $$
  select case
    when lower(coalesce(p_media_type, 'image')) in ('reel', 'video')
      then greatest(coalesce(p_views, 0), coalesce(p_likes, 0), coalesce(p_comments, 0))
    when lower(coalesce(p_media_type, 'image')) in ('sidecar', 'carousel')
      then greatest(coalesce(p_likes, 0), coalesce(p_comments, 0), coalesce(p_views, 0))
    else greatest(coalesce(p_likes, 0), coalesce(p_comments, 0), coalesce(p_views, 0))
  end;
$$;

create or replace function public.fn_is_hot_percentile(p_percentile int)
returns boolean
language sql
immutable
as $$
  select p_percentile is not null and p_percentile <= 35
$$;

create or replace function public.fn_checkpoint_due_at(
  p_posted_at timestamptz,
  p_days_after int,
  p_tz text default 'Asia/Kolkata',
  p_hour int default 18,
  p_minute int default 30,
  p_bucket_minutes int default 60
)
returns timestamptz
language plpgsql
stable
as $$
begin
  if p_posted_at is null then
    return null;
  end if;

  return p_posted_at + make_interval(days => greatest(0, coalesce(p_days_after, 0)));
end;
$$;

create or replace function public.fn_checkpoint_due_at(
  p_posted_at timestamptz,
  p_days_after int,
  p_tz text default 'Asia/Kolkata',
  p_hour int default 18,
  p_minute int default 30
)
returns timestamptz
language plpgsql
stable
as $$
begin
  return public.fn_checkpoint_due_at(p_posted_at, p_days_after, p_tz, p_hour, p_minute, 60);
end;
$$;

create or replace function public.enqueue_checkpoint_jobs(
  p_post_key text,
  p_posted_at timestamptz,
  p_tz text default 'Asia/Kolkata',
  p_hour int default 18,
  p_minute int default 30,
  p_bucket_minutes int default 60
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
begin
  with targets as (
    select 'd1'::text as checkpoint, public.fn_checkpoint_due_at(p_posted_at, 1, p_tz, p_hour, p_minute, p_bucket_minutes) as due_at
    union all
    select 'd3'::text as checkpoint, public.fn_checkpoint_due_at(p_posted_at, 3, p_tz, p_hour, p_minute, p_bucket_minutes) as due_at
    union all
    select 'd7'::text as checkpoint, public.fn_checkpoint_due_at(p_posted_at, 7, p_tz, p_hour, p_minute, p_bucket_minutes) as due_at
    union all
    select 'd21'::text as checkpoint, public.fn_checkpoint_due_at(p_posted_at, 21, p_tz, p_hour, p_minute, p_bucket_minutes) as due_at
  )
  insert into public.checkpoint_jobs (post_key, checkpoint, status, next_run_at, last_error)
  select
    p_post_key,
    checkpoint,
    case
      when due_at is null then 'skipped'
      when due_at + make_interval(mins => greatest(1, coalesce(p_bucket_minutes, 60))) <= now() then 'skipped'
      else 'pending'
    end,
    coalesce(due_at, now()),
    case
      when due_at is null then 'checkpoint skipped: missing posted_at'
      when due_at + make_interval(mins => greatest(1, coalesce(p_bucket_minutes, 60))) <= now()
        then 'checkpoint skipped: eligibility window already passed'
      else null
    end
  from targets
  on conflict (post_key, checkpoint) do update
    set status = case
        when excluded.status = 'skipped' then public.checkpoint_jobs.status
        when public.checkpoint_jobs.status in ('done', 'running') then public.checkpoint_jobs.status
        else 'pending'
      end,
      next_run_at = case
        when excluded.status = 'skipped' then public.checkpoint_jobs.next_run_at
        when public.checkpoint_jobs.status in ('done', 'running') then public.checkpoint_jobs.next_run_at
        else excluded.next_run_at
      end,
      last_error = case
        when excluded.status = 'skipped' then public.checkpoint_jobs.last_error
        when public.checkpoint_jobs.status in ('done', 'running') then public.checkpoint_jobs.last_error
        else null
      end,
      updated_at = now()
    where public.checkpoint_jobs.status not in ('done', 'running')
      and excluded.status <> 'skipped';

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

drop function if exists public.enqueue_checkpoint_jobs(text, timestamptz);

create or replace function public.enqueue_checkpoint_jobs(
  p_post_key text,
  p_posted_at timestamptz
)
returns int
language sql
security definer
set search_path = public
as $$
  select public.enqueue_checkpoint_jobs(
    p_post_key,
    p_posted_at,
    'Asia/Kolkata',
    23,
    30,
    60
  )
$$;

create or replace function public.enqueue_daily_jobs(p_run_at timestamptz default now())
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_count integer := 0;
  v_business_date date := ((p_run_at at time zone 'Asia/Kolkata')::date - 1);
begin
  insert into public.run_jobs (feeder_id, job_type, status, next_run_at, business_date_ist)
  select
    fd.id,
    'daily',
    'pending',
    p_run_at,
    v_business_date
  from public.feeders fd
  join public.feeds f on f.id = fd.feed_id
  where fd.status = 'active'
    and f.status = 'active'
    and not exists (
      select 1
      from public.run_jobs rj
      where rj.feeder_id = fd.id
        and rj.business_date_ist = v_business_date
        and rj.job_type in ('daily', 'repair')
        and rj.status in ('pending', 'running', 'retry', 'done')
    )
  on conflict do nothing;

  get diagnostics v_count = row_count;
  return v_count;
end;
$$;

create or replace function public.enqueue_daily_job_for_feeder(
  p_feeder_id bigint,
  p_run_at timestamptz default now()
)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_count integer := 0;
  v_business_date date := ((p_run_at at time zone 'Asia/Kolkata')::date - 1);
begin
  insert into public.run_jobs (feeder_id, job_type, status, next_run_at, business_date_ist)
  select
    fd.id,
    'daily',
    'pending',
    p_run_at,
    v_business_date
  from public.feeders fd
  join public.feeds f on f.id = fd.feed_id
  where fd.id = p_feeder_id
    and fd.status = 'active'
    and f.status = 'active'
    and not exists (
      select 1
      from public.run_jobs rj
      where rj.feeder_id = fd.id
        and rj.business_date_ist = v_business_date
        and rj.job_type in ('daily', 'repair')
        and rj.status in ('pending', 'running', 'retry', 'done')
    )
  on conflict do nothing;

  get diagnostics v_count = row_count;
  return v_count;
end;
$$;

create or replace function public.finalize_daily_jobs_for_day(p_business_date date)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
begin
  update public.run_jobs
  set status = 'failed',
      last_error = coalesce(last_error, 'Daily window cutoff reached; moved to repair lane'),
      updated_at = now()
  where job_type = 'daily'
    and business_date_ist = p_business_date
    and status in ('pending', 'retry');

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

create or replace function public.enqueue_repair_jobs_from_previous_day(p_tz text default 'Asia/Kolkata')
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_day date := ((now() at time zone p_tz)::date - 1);
  v_finalized int := 0;
  v_enqueued int := 0;
begin
  v_finalized := public.finalize_daily_jobs_for_day(v_day);

  insert into public.run_jobs (feeder_id, job_type, status, next_run_at, business_date_ist, attempt, last_error)
  select distinct
    rj.feeder_id,
    'repair',
    'pending',
    now(),
    v_day,
    0,
    null
  from public.run_jobs rj
  join public.feeders fd on fd.id = rj.feeder_id
  join public.feeds f on f.id = fd.feed_id
  where rj.job_type = 'daily'
    and rj.business_date_ist = v_day
    and rj.status = 'failed'
    and fd.status = 'active'
    and f.status = 'active'
    and not exists (
      select 1
      from public.run_jobs e
      where e.feeder_id = rj.feeder_id
        and e.business_date_ist = v_day
        and e.job_type in ('daily', 'repair')
        and e.status in ('pending', 'running', 'retry', 'done')
    );

  get diagnostics v_enqueued = row_count;

  return jsonb_build_object(
    'business_date_ist', v_day,
    'finalized_daily_jobs', v_finalized,
    'repair_jobs_enqueued', v_enqueued
  );
end;
$$;

create or replace function public.enqueue_poll_jobs(p_run_at timestamptz default now())
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_count integer := 0;
  v_business_date date := ((p_run_at at time zone 'Asia/Kolkata')::date);
begin
  insert into public.run_jobs (feeder_id, job_type, status, next_run_at, business_date_ist)
  select
    fd.id,
    'poll',
    'pending',
    p_run_at,
    v_business_date
  from public.feeders fd
  join public.feeds f on f.id = fd.feed_id
  where fd.status = 'active'
    and f.status = 'active'
    and not exists (
      select 1
      from public.run_jobs rj
      where rj.feeder_id = fd.id
        and rj.job_type = 'poll'
        and rj.business_date_ist = v_business_date
        and rj.status in ('pending', 'running', 'retry', 'done')
    )
  on conflict do nothing;

  get diagnostics v_count = row_count;
  return v_count;
end;
$$;

create or replace function public.claim_run_jobs(p_limit int default 25)
returns setof public.run_jobs
language sql
security definer
set search_path = public
as $$
  with picked as (
    select id
    from public.run_jobs
    where status in ('pending', 'retry')
      and next_run_at <= now()
    order by next_run_at asc, id asc
    for update skip locked
    limit greatest(1, p_limit)
  ), updated as (
    update public.run_jobs r
    set status = 'running',
        updated_at = now()
    from picked
    where r.id = picked.id
    returning r.*
  )
  select * from updated;
$$;

create or replace function public.claim_checkpoint_jobs(p_limit int default 100)
returns setof public.checkpoint_jobs
language sql
security definer
set search_path = public
as $$
  with picked as (
    select id
    from public.checkpoint_jobs
    where status in ('pending', 'retry')
      and next_run_at <= now()
    order by next_run_at asc, id asc
    for update skip locked
    limit greatest(1, p_limit)
  ), updated as (
    update public.checkpoint_jobs cj
    set status = 'running',
        updated_at = now()
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
  set status = p_status,
      attempt = coalesce(p_attempt, attempt),
      next_run_at = coalesce(p_next_run_at, next_run_at),
      last_error = case when p_error is null then null else left(p_error, 1000) end,
      updated_at = now()
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
  set status = p_status,
      attempt = coalesce(p_attempt, attempt),
      next_run_at = coalesce(p_next_run_at, next_run_at),
      last_error = case when p_error is null then null else left(p_error, 1000) end,
      updated_at = now()
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
  set status = 'retry',
      next_run_at = now(),
      last_error = format('Recovered stale running job after %s minutes watchdog', v_minutes),
      updated_at = now()
  where status = 'running'
    and updated_at < now() - (v_minutes::text || ' minutes')::interval;
  get diagnostics v_run_rows = row_count;

  update public.checkpoint_jobs
  set status = 'retry',
      next_run_at = now(),
      last_error = format('Recovered stale running checkpoint job after %s minutes watchdog', v_minutes),
      updated_at = now()
  where status = 'running'
    and updated_at < now() - (v_minutes::text || ' minutes')::interval;
  get diagnostics v_checkpoint_rows = row_count;

  return query select v_run_rows, v_checkpoint_rows;
end;
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
  set status = 'skipped',
      last_error = 'D21 skipped - D7 did not qualify',
      updated_at = now()
  where cj.status in ('pending', 'retry')
    and cj.checkpoint = 'd21'
    and cj.next_run_at <= now()
    and not exists (
      select 1
      from public.post_metrics pm
      where pm.post_key = cj.post_key
        and pm.checkpoint = 'd7'
        and public.fn_is_hot_percentile(pm.percentile_performance)
    );

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

create or replace function public.tg_post_metrics_contract()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_posted_at timestamptz;
  v_due_at timestamptz;
  v_expected_day date;
  v_computed_at timestamptz := coalesce(new.computed_at, now());
  v_days_after int;
begin
  select p.posted_at
  into v_posted_at
  from public.posts p
  where p.post_key = new.post_key;

  v_days_after := case lower(coalesce(new.checkpoint, ''))
    when 'd1' then 1
    when 'd3' then 3
    when 'd7' then 7
    when 'd21' then 21
    else null
  end;

  if v_days_after is null then
    return null;
  end if;

  new.checkpoint := lower(new.checkpoint);
  new.computed_at := v_computed_at;

  if v_posted_at is not null then
    v_due_at := public.fn_checkpoint_due_at(v_posted_at, v_days_after, 'Asia/Kolkata', 23, 30, 60);
    if v_due_at is not null then
      if v_computed_at < v_due_at then
        return null;
      end if;

      if v_computed_at >= (v_due_at + interval '60 minutes') then
        return null;
      end if;

      v_expected_day := (v_due_at at time zone 'Asia/Kolkata')::date;
      new.business_date_ist := v_expected_day;
    end if;
  end if;

  if new.business_date_ist is null then
    new.business_date_ist := (v_computed_at at time zone 'Asia/Kolkata')::date;
  end if;

  return new;
end;
$$;

create or replace function public.tg_compute_post_metrics()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_media_type text;
  v_feeder_id bigint;
  v_feed_id bigint;
  v_mv numeric;
  v_pool_size int;
  v_rank int;
  v_feed_pool_size int;
  v_feed_rank int;
  v_views_pool_size int;
  v_views_rank int;
  v_likes_pool_size int;
  v_likes_rank int;
  v_comments_pool_size int;
  v_comments_rank int;
  v_d1_pct int;
begin
  select
    lower(coalesce(p.media_type, 'unknown')),
    p.feeder_id,
    fd.feed_id
  into v_media_type, v_feeder_id, v_feed_id
  from public.posts p
  join public.feeders fd on fd.id = p.feeder_id
  where p.post_key = new.post_key;

  v_mv := public.fn_metric_value(v_media_type, new.views, new.likes, new.comments);

  v_rank := null;
  v_pool_size := 0;
  if v_mv is not null and v_feeder_id is not null then
    select count(*) + 1
    into v_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and lower(coalesce(p2.media_type, 'unknown')) = v_media_type
      and pm.checkpoint = new.checkpoint
      and pm.metric_value is not null
      and pm.metric_value > v_mv;

    select count(*) + 1
    into v_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and lower(coalesce(p2.media_type, 'unknown')) = v_media_type
      and pm.checkpoint = new.checkpoint
      and pm.metric_value is not null;
  end if;

  v_feed_rank := null;
  v_feed_pool_size := 0;
  if v_mv is not null and v_feed_id is not null then
    select count(*) + 1
    into v_feed_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    join public.feeders fd2 on fd2.id = p2.feeder_id
    where fd2.feed_id = v_feed_id
      and lower(coalesce(p2.media_type, 'unknown')) = v_media_type
      and pm.checkpoint = new.checkpoint
      and pm.metric_value is not null
      and pm.metric_value > v_mv;

    select count(*) + 1
    into v_feed_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    join public.feeders fd2 on fd2.id = p2.feeder_id
    where fd2.feed_id = v_feed_id
      and lower(coalesce(p2.media_type, 'unknown')) = v_media_type
      and pm.checkpoint = new.checkpoint
      and pm.metric_value is not null;
  end if;

  v_views_rank := null;
  v_views_pool_size := 0;
  if new.views is not null and v_feeder_id is not null then
    select count(*) + 1
    into v_views_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and lower(coalesce(p2.media_type, 'unknown')) = v_media_type
      and pm.checkpoint = new.checkpoint
      and pm.views is not null
      and pm.views > new.views;

    select count(*) + 1
    into v_views_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and lower(coalesce(p2.media_type, 'unknown')) = v_media_type
      and pm.checkpoint = new.checkpoint
      and pm.views is not null;
  end if;

  v_likes_rank := null;
  v_likes_pool_size := 0;
  if new.likes is not null and v_feeder_id is not null then
    select count(*) + 1
    into v_likes_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and lower(coalesce(p2.media_type, 'unknown')) = v_media_type
      and pm.checkpoint = new.checkpoint
      and pm.likes is not null
      and pm.likes > new.likes;

    select count(*) + 1
    into v_likes_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and lower(coalesce(p2.media_type, 'unknown')) = v_media_type
      and pm.checkpoint = new.checkpoint
      and pm.likes is not null;
  end if;

  v_comments_rank := null;
  v_comments_pool_size := 0;
  if new.comments is not null and v_feeder_id is not null then
    select count(*) + 1
    into v_comments_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and lower(coalesce(p2.media_type, 'unknown')) = v_media_type
      and pm.checkpoint = new.checkpoint
      and pm.comments is not null
      and pm.comments > new.comments;

    select count(*) + 1
    into v_comments_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and lower(coalesce(p2.media_type, 'unknown')) = v_media_type
      and pm.checkpoint = new.checkpoint
      and pm.comments is not null;
  end if;

  select pm.percentile_performance
  into v_d1_pct
  from public.post_metrics pm
  where pm.post_key = new.post_key
    and pm.checkpoint = 'd1'
  limit 1;

  new.metric_value := v_mv;
  new.percentile_performance := case
    when v_pool_size > 0 then greatest(1, least(100, round((v_rank::numeric / v_pool_size::numeric) * 100)))
    else null
  end;
  new.feed_percentile := case
    when v_feed_pool_size > 0 then greatest(1, least(100, round((v_feed_rank::numeric / v_feed_pool_size::numeric) * 100)))
    else null
  end;
  new.views_percentile := case
    when v_views_pool_size > 0 then greatest(1, least(100, round((v_views_rank::numeric / v_views_pool_size::numeric) * 100)))
    else null
  end;
  new.likes_percentile := case
    when v_likes_pool_size > 0 then greatest(1, least(100, round((v_likes_rank::numeric / v_likes_pool_size::numeric) * 100)))
    else null
  end;
  new.comments_percentile := case
    when v_comments_pool_size > 0 then greatest(1, least(100, round((v_comments_rank::numeric / v_comments_pool_size::numeric) * 100)))
    else null
  end;
  new.delta_from_d1 := case
    when new.checkpoint = 'd1' then null
    when v_d1_pct is null or new.percentile_performance is null then null
    else v_d1_pct - new.percentile_performance
  end;

  return new;
end;
$$;

create or replace function public.fn_refresh_feeder_baselines(
  p_feeder_id bigint,
  p_checkpoint text
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
begin
  with base as (
    select
      lower(coalesce(p.media_type, 'unknown')) as media_type,
      pm.views,
      pm.likes,
      pm.comments
    from public.post_metrics pm
    join public.posts p on p.post_key = pm.post_key
    where p.feeder_id = p_feeder_id
      and pm.checkpoint = p_checkpoint
  ), stats as (
    select
      media_type,
      percentile_cont(0.5) within group (order by views)::bigint as median_views,
      percentile_cont(0.5) within group (order by likes)::bigint as median_likes,
      percentile_cont(0.5) within group (order by comments)::bigint as median_comments
    from base
    group by media_type
  )
  insert into public.feeder_baselines (
    feeder_id,
    media_type,
    checkpoint,
    median_views,
    median_likes,
    median_comments,
    updated_at
  )
  select
    p_feeder_id,
    media_type,
    p_checkpoint,
    median_views,
    median_likes,
    median_comments,
    now()
  from stats
  on conflict (feeder_id, media_type, checkpoint) do update
    set median_views = excluded.median_views,
        median_likes = excluded.median_likes,
        median_comments = excluded.median_comments,
        updated_at = now();

  delete from public.feeder_baselines fb
  where fb.feeder_id = p_feeder_id
    and fb.checkpoint = p_checkpoint
    and not exists (
      select 1
      from public.posts p
      join public.post_metrics pm on pm.post_key = p.post_key
      where p.feeder_id = p_feeder_id
        and pm.checkpoint = p_checkpoint
        and lower(coalesce(p.media_type, 'unknown')) = fb.media_type
    );

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

create or replace function public.fn_refresh_feeder_hour_baselines(
  p_feeder_id bigint,
  p_checkpoint text
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
begin
  with base as (
    select
      lower(coalesce(p.media_type, 'unknown')) as media_type,
      extract(hour from (p.posted_at at time zone 'Asia/Kolkata'))::smallint as hour_ist,
      pm.views,
      pm.likes,
      pm.comments
    from public.post_metrics pm
    join public.posts p on p.post_key = pm.post_key
    where p.feeder_id = p_feeder_id
      and pm.checkpoint = p_checkpoint
      and p.posted_at is not null
  ), stats as (
    select
      media_type,
      hour_ist,
      percentile_cont(0.5) within group (order by views)::bigint as median_views,
      percentile_cont(0.5) within group (order by likes)::bigint as median_likes,
      percentile_cont(0.5) within group (order by comments)::bigint as median_comments
    from base
    group by media_type, hour_ist
  )
  insert into public.feeder_hour_baselines (
    feeder_id,
    media_type,
    checkpoint,
    hour_ist,
    median_views,
    median_likes,
    median_comments,
    updated_at
  )
  select
    p_feeder_id,
    media_type,
    p_checkpoint,
    hour_ist,
    median_views,
    median_likes,
    median_comments,
    now()
  from stats
  on conflict (feeder_id, media_type, checkpoint, hour_ist) do update
    set median_views = excluded.median_views,
        median_likes = excluded.median_likes,
        median_comments = excluded.median_comments,
        updated_at = now();

  delete from public.feeder_hour_baselines fhb
  where fhb.feeder_id = p_feeder_id
    and fhb.checkpoint = p_checkpoint
    and not exists (
      select 1
      from public.posts p
      join public.post_metrics pm on pm.post_key = p.post_key
      where p.feeder_id = p_feeder_id
        and pm.checkpoint = p_checkpoint
        and lower(coalesce(p.media_type, 'unknown')) = fhb.media_type
        and extract(hour from (p.posted_at at time zone 'Asia/Kolkata'))::smallint = fhb.hour_ist
    );

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

create or replace function public.tg_refresh_metric_baselines()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_feeder_id bigint;
begin
  select feeder_id
  into v_feeder_id
  from public.posts
  where post_key = new.post_key;

  if v_feeder_id is not null then
    perform public.fn_refresh_feeder_baselines(v_feeder_id, new.checkpoint);
    perform public.fn_refresh_feeder_hour_baselines(v_feeder_id, new.checkpoint);
  end if;

  return null;
end;
$$;

create or replace function public.tg_capture_feeder_follower_snapshot()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_week_start date := date_trunc('week', timezone('Asia/Kolkata', now()))::date;
  v_count bigint := greatest(0, coalesce(new.follower_count, 0));
begin
  if tg_op = 'INSERT' or new.follower_count is distinct from old.follower_count then
    insert into public.feeder_follower_snapshots (feeder_id, week_start_ist, follower_count, captured_at)
    values (new.id, v_week_start, v_count, now())
    on conflict (feeder_id, week_start_ist) do update
      set follower_count = excluded.follower_count,
          captured_at = excluded.captured_at;
  end if;

  return new;
end;
$$;

create trigger trg_00_post_metrics_contract
before insert or update on public.post_metrics
for each row
execute function public.tg_post_metrics_contract();

create trigger trg_compute_post_metrics
before insert or update on public.post_metrics
for each row
execute function public.tg_compute_post_metrics();

create trigger trg_refresh_metric_baselines
after insert or update on public.post_metrics
for each row
execute function public.tg_refresh_metric_baselines();

create trigger trg_capture_feeder_follower_snapshot
after insert or update of follower_count on public.feeders
for each row
execute function public.tg_capture_feeder_follower_snapshot();

insert into public.feeder_follower_snapshots (feeder_id, week_start_ist, follower_count, captured_at)
select
  f.id,
  date_trunc('week', timezone('Asia/Kolkata', now()))::date,
  greatest(0, coalesce(f.follower_count, 0))::bigint,
  now()
from public.feeders f
on conflict (feeder_id, week_start_ist) do update
  set follower_count = excluded.follower_count,
      captured_at = excluded.captured_at;

create or replace function public.fn_post_media_rollover_deadline(
  p_posted_at timestamptz,
  p_asset_role text
)
returns timestamptz
language sql
stable
as $$
  select coalesce(p_posted_at, now()) +
    case
      when lower(coalesce(p_asset_role, '')) in ('thumbnail', 'display')
        or lower(coalesce(p_asset_role, '')) like 'carousel_%'
        then interval '30 days'
      else interval '8 days'
    end;
$$;

create or replace function public.claim_post_media_assets_for_capture(p_limit integer default 25)
returns setof public.post_media_assets
language sql
security definer
set search_path = public
as $$
  with picked as (
    select id
    from public.post_media_assets
    where status in ('pending_capture', 'capture_failed')
      and next_run_at <= now()
      and coalesce(source_url, '') <> ''
      and (purge_after is null or purge_after > now())
    order by next_run_at asc, created_at asc, id asc
    for update skip locked
    limit greatest(1, p_limit)
  ), updated as (
    update public.post_media_assets assets
    set status = 'capturing',
        updated_at = now()
    from picked
    where assets.id = picked.id
    returning assets.*
  )
  select * from updated;
$$;

create or replace function public.claim_post_media_assets_for_purge(p_limit integer default 50)
returns setof public.post_media_assets
language sql
security definer
set search_path = public
as $$
  with picked as (
    select id
    from public.post_media_assets
    where status in ('active', 'purge_pending', 'purge_failed')
      and purge_after is not null
      and purge_after <= now()
      and coalesce(storage_path, '') <> ''
      and deleted_at is null
    order by purge_after asc, updated_at asc, id asc
    for update skip locked
    limit greatest(1, p_limit)
  ), updated as (
    update public.post_media_assets assets
    set status = 'purging',
        updated_at = now()
    from picked
    where assets.id = picked.id
    returning assets.*
  )
  select * from updated;
$$;

create or replace function public.tg_schedule_fire_media_retention_from_d7()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if new.checkpoint <> 'd7' then
    return new;
  end if;

  update public.post_media_assets assets
  set purge_after = public.fn_post_media_rollover_deadline(p.posted_at, assets.asset_role),
      updated_at = now(),
      status = case
        when assets.status = 'deleted' then assets.status
        when coalesce(assets.storage_path, '') = '' then 'pending_capture'
        else 'active'
      end,
      next_run_at = case
        when assets.status in ('pending_capture', 'capture_failed') then now()
        else assets.next_run_at
      end,
      deleted_at = null,
      last_error = null
  from public.posts p
  where p.post_key = assets.post_key
    and assets.post_key = new.post_key
    and assets.status <> 'deleted';

  return new;
end;
$$;

create trigger trg_20_schedule_fire_media_retention_from_d7
after insert or update of checkpoint, percentile_performance, computed_at on public.post_metrics
for each row
execute function public.tg_schedule_fire_media_retention_from_d7();

create or replace function public.fn_process_checkpoint(
  p_feeder_id bigint,
  p_checkpoint text,
  p_business_date date
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_cp text := lower(coalesce(p_checkpoint, 'd1'));
  v_day date := coalesce(p_business_date, (now() at time zone 'Asia/Kolkata')::date);
  v_feed_id bigint;
  v_handle text;
  v_rows int := 0;
  r record;
  v_best_metric text;
  v_best_value bigint;
  v_best_percentile int;
  v_baseline bigint;
  v_multiple numeric;
  v_body text;
  v_alert_type text;
  v_dedupe text;
begin
  if v_cp not in ('d1', 'd3', 'd7', 'd21') then
    return 0;
  end if;

  select fd.feed_id, fd.handle
  into v_feed_id, v_handle
  from public.feeders fd
  where fd.id = p_feeder_id;

  if v_feed_id is null then
    return 0;
  end if;

  update public.fire_alerts fa
  set status = 'dropped',
      updated_at = now()
  where fa.feeder_id = p_feeder_id
    and fa.checkpoint = v_cp
    and fa.business_date_ist = v_day
    and fa.signal_code = 'slot_v3'
    and fa.context = 'own'
    and not exists (
      select 1
      from public.post_metrics pm
      join public.posts p on p.post_key = pm.post_key
      where p.feeder_id = p_feeder_id
        and pm.post_key = fa.post_key
        and pm.checkpoint = v_cp
        and pm.business_date_ist = v_day
        and public.fn_is_hot_percentile(pm.percentile_performance)
    );

  for r in
    select
      p.post_key,
      lower(coalesce(p.media_type, 'unknown')) as media_type,
      pm.views,
      pm.likes,
      pm.comments,
      pm.views_percentile,
      pm.likes_percentile,
      pm.comments_percentile,
      pm.percentile_performance,
      pm.delta_from_d1,
      fb.median_views,
      fb.median_likes,
      fb.median_comments
    from public.posts p
    join public.post_metrics pm
      on pm.post_key = p.post_key
     and pm.checkpoint = v_cp
     and pm.business_date_ist = v_day
    left join public.feeder_baselines fb
      on fb.feeder_id = p.feeder_id
     and fb.media_type = lower(coalesce(p.media_type, 'unknown'))
     and fb.checkpoint = pm.checkpoint
    where p.feeder_id = p_feeder_id
      and public.fn_is_hot_percentile(pm.percentile_performance)
    order by pm.percentile_performance asc, p.posted_at desc nulls last
  loop
    v_best_metric := 'views';
    v_best_value := r.views;
    v_best_percentile := coalesce(r.views_percentile, 101);
    v_baseline := r.median_views;

    if coalesce(r.likes_percentile, 101) < v_best_percentile then
      v_best_metric := 'likes';
      v_best_value := r.likes;
      v_best_percentile := coalesce(r.likes_percentile, 101);
      v_baseline := r.median_likes;
    end if;

    if coalesce(r.comments_percentile, 101) < v_best_percentile then
      v_best_metric := 'comments';
      v_best_value := r.comments;
      v_best_percentile := coalesce(r.comments_percentile, 101);
      v_baseline := r.median_comments;
    end if;

    v_multiple := case
      when v_best_value is not null and coalesce(v_baseline, 0) > 0
        then round(v_best_value::numeric / v_baseline::numeric, 2)
      else null
    end;

    v_alert_type := case
      when r.percentile_performance <= 10 then 'blaze'
      when r.percentile_performance <= 25 then 'burn'
      else 'spark'
    end;

    v_body := case
      when v_cp = 'd1' and v_multiple is not null then
        format(
          '@%s hit the top %s%% at %s with %s %s, %.2fx its usual %s.',
          upper(coalesce(v_handle, 'feed')),
          coalesce(r.percentile_performance, 0),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(v_best_metric),
          v_multiple,
          upper(v_best_metric)
        )
      when v_cp = 'd1' then
        format(
          '@%s hit the top %s%% at %s with %s %s.',
          upper(coalesce(v_handle, 'feed')),
          coalesce(r.percentile_performance, 0),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(v_best_metric)
        )
      when r.delta_from_d1 is not null then
        format(
          '@%s stayed in the top %s%% at %s with %s %s, %s points vs D1.',
          upper(coalesce(v_handle, 'feed')),
          coalesce(r.percentile_performance, 0),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(v_best_metric),
          case when r.delta_from_d1 > 0 then '+' || r.delta_from_d1::text else r.delta_from_d1::text end
        )
      else
        format(
          '@%s stayed in the top %s%% at %s with %s %s.',
          upper(coalesce(v_handle, 'feed')),
          coalesce(r.percentile_performance, 0),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(v_best_metric)
        )
    end;

    v_dedupe := format('slot_v3:%s:%s:%s:%s', v_feed_id, r.post_key, v_cp, v_day);

    insert into public.fire_alerts (
      dedupe_key,
      feed_id,
      feeder_id,
      post_key,
      checkpoint,
      business_date_ist,
      signal_code,
      context,
      alert_type,
      status,
      metric_key,
      metric_value,
      surface_percentile,
      surface_delta,
      body,
      created_at,
      updated_at,
      pattern_signal,
      pattern_payload
    )
    values (
      v_dedupe,
      v_feed_id,
      p_feeder_id,
      r.post_key,
      v_cp,
      v_day,
      'slot_v3',
      'own',
      v_alert_type,
      'new',
      v_best_metric,
      v_best_value,
      r.percentile_performance,
      r.delta_from_d1,
      v_body,
      now(),
      now(),
      null,
      null
    )
    on conflict (dedupe_key) do update
      set alert_type = excluded.alert_type,
          status = 'new',
          metric_key = excluded.metric_key,
          metric_value = excluded.metric_value,
          surface_percentile = excluded.surface_percentile,
          surface_delta = excluded.surface_delta,
          body = excluded.body,
          updated_at = now(),
          pattern_signal = null,
          pattern_payload = null;

    v_rows := v_rows + 1;
  end loop;

  return v_rows;
end;
$$;

create or replace view public.v_post_tagged_performance as
select
  pm.post_key,
  pm.checkpoint,
  pm.business_date_ist,
  pm.views,
  pm.likes,
  pm.comments,
  pm.metric_value,
  pm.percentile_performance,
  pm.views_percentile,
  pm.likes_percentile,
  pm.comments_percentile,
  least(
    coalesce(pm.percentile_performance, 100),
    coalesce(pm.views_percentile, 100),
    coalesce(pm.likes_percentile, 100),
    coalesce(pm.comments_percentile, 100)
  ) as best_percentile,
  p.feeder_id,
  lower(coalesce(p.media_type, 'unknown')) as media_type,
  p.posted_at,
  f.feed_id,
  f.role as feeder_role,
  pi.tags,
  pi.tags->>'hook' as tag_hook,
  pi.tags->>'pillar' as tag_pillar,
  pi.tags->>'format' as tag_format,
  pi.tags->>'style' as tag_style,
  pi.tags->>'cta' as tag_cta,
  pi.tags->>'depth' as tag_depth,
  pi.tags->>'density' as tag_density,
  pi.tags->>'language' as tag_language,
  pi.tags->>'duration_bucket' as tag_duration_bucket,
  pi.tags->>'face' as tag_face,
  pi.tags->>'text_overlay' as tag_text_overlay,
  pi.tags->>'subject' as tag_subject
from public.post_metrics pm
join public.posts p on p.post_key = pm.post_key
join public.feeders f on f.id = p.feeder_id
left join public.post_intelligence pi on pi.post_key = pm.post_key;

create or replace function public.fn_enrich_pattern_signal(
  p_fire_alert_id bigint
)
returns void
language plpgsql
set search_path = public
as $$
declare
  v_post_key text;
  v_feeder_id bigint;
  v_feed_id bigint;
  v_tags jsonb;
  v_hook text;
  v_format text;
  v_signal text := null;
  v_payload jsonb := null;
  v_combo_pct numeric;
  v_other_pct numeric;
  v_combo_count int;
  v_window_days int := 60;
  v_d1_avg numeric;
  v_d7_avg numeric;
  v_sb_count int;
begin
  select fa.post_key, fa.feeder_id, fa.feed_id
  into v_post_key, v_feeder_id, v_feed_id
  from public.fire_alerts fa
  where fa.id = p_fire_alert_id;

  if v_post_key is null then
    return;
  end if;

  select pi.tags
  into v_tags
  from public.post_intelligence pi
  where pi.post_key = v_post_key;

  if v_tags is null or v_tags = '{}'::jsonb then
    return;
  end if;

  v_hook := v_tags->>'hook';
  v_format := v_tags->>'format';

  if v_hook is not null and v_format is not null then
    select avg(vt.best_percentile)::numeric, count(*)::int
    into v_combo_pct, v_combo_count
    from public.v_post_tagged_performance vt
    where vt.feeder_id = v_feeder_id
      and vt.checkpoint = 'd1'
      and vt.tag_hook = v_hook
      and vt.tag_format = v_format
      and vt.posted_at >= now() - (v_window_days || ' days')::interval;

    if v_combo_count >= 3 and v_combo_pct <= 25 then
      select avg(vt.best_percentile)::numeric
      into v_other_pct
      from public.v_post_tagged_performance vt
      where vt.feeder_id = v_feeder_id
        and vt.checkpoint = 'd1'
        and not (vt.tag_hook = v_hook and vt.tag_format = v_format)
        and vt.posted_at >= now() - (v_window_days || ' days')::interval;

      v_signal := 'F1_own_formula';
      v_payload := jsonb_build_object(
        'signal_label', 'YOUR FORMULA',
        'row1', jsonb_build_array(
          jsonb_build_object('label', 'PATTERN', 'value', v_hook || ' + ' || v_format),
          jsonb_build_object('label', 'BASED ON', 'value', v_combo_count || ' posts'),
          jsonb_build_object('label', 'WINDOW', 'value', 'Last ' || v_window_days || ' days')
        ),
        'row2', jsonb_build_array(
          jsonb_build_object('label', 'THIS COMBO', 'value', 'TOP ' || round(v_combo_pct) || '% avg'),
          jsonb_build_object('label', 'ALL OTHER POSTS', 'value', 'TOP ' || round(coalesce(v_other_pct, 50)) || '% avg')
        ),
        'tags_raw', jsonb_build_array(v_hook, v_format),
        'combo_percentile', round(v_combo_pct),
        'other_percentile', round(coalesce(v_other_pct, 50)),
        'sample_size', v_combo_count
      );
    end if;
  end if;

  if v_signal is null and v_format is not null then
    select avg(vt.best_percentile)::numeric, count(*)::int
    into v_d7_avg, v_sb_count
    from public.v_post_tagged_performance vt
    where vt.feeder_id = v_feeder_id
      and vt.checkpoint = 'd7'
      and vt.tag_format = v_format
      and vt.posted_at >= now() - (v_window_days || ' days')::interval;

    if v_sb_count >= 3 and v_d7_avg <= 20 then
      select avg(vt.best_percentile)::numeric
      into v_d1_avg
      from public.v_post_tagged_performance vt
      where vt.feeder_id = v_feeder_id
        and vt.checkpoint = 'd1'
        and vt.tag_format = v_format
        and vt.posted_at >= now() - (v_window_days || ' days')::interval;

      if v_d1_avg >= 40 then
        v_signal := 'F5_slow_burn';
        v_payload := jsonb_build_object(
          'signal_label', 'SLOW BURN',
          'row1', jsonb_build_array(
            jsonb_build_object('label', 'PATTERN', 'value', v_format),
            jsonb_build_object('label', 'BASED ON', 'value', v_sb_count || ' posts with D7 data'),
            jsonb_build_object('label', 'WINDOW', 'value', 'Last ' || v_window_days || ' days')
          ),
          'row2', jsonb_build_array(
            jsonb_build_object('label', 'AT D1', 'value', 'TOP ' || round(v_d1_avg) || '% avg'),
            jsonb_build_object('label', 'AT D7', 'value', 'TOP ' || round(v_d7_avg) || '% avg')
          ),
          'tags_raw', jsonb_build_array(v_format),
          'combo_percentile', round(v_d7_avg),
          'other_percentile', round(v_d1_avg),
          'sample_size', v_sb_count
        );
      end if;
    end if;
  end if;

  if v_signal is not null then
    update public.fire_alerts
    set pattern_signal = v_signal,
        pattern_payload = v_payload,
        updated_at = now()
    where id = p_fire_alert_id;
  end if;
end;
$$;

create or replace view public.v_fire_alert_surface as
with core as (
  select
    fa.id,
    fa.dedupe_key,
    fa.feed_id,
    fa.feeder_id,
    fa.post_key,
    fa.checkpoint,
    fa.business_date_ist,
    fa.signal_code,
    fa.context,
    fa.alert_type,
    fa.status,
    fa.metric_key,
    fa.metric_value,
    fa.surface_percentile,
    fa.surface_delta,
    fa.body,
    fa.pattern_signal,
    fa.pattern_payload,
    fa.created_at,
    fa.updated_at,
    fd.handle,
    lower(coalesce(p.media_type, 'unknown')) as media_type,
    p.posted_at,
    p.post_url,
    p.thumbnail_url,
    extract(hour from (p.posted_at at time zone 'Asia/Kolkata'))::int as hour_ist,
    pm.views,
    pm.likes,
    pm.comments,
    pm.percentile_performance,
    pm.views_percentile,
    pm.likes_percentile,
    pm.comments_percentile,
    pm.feed_percentile,
    d1.percentile_performance as trajectory_d1,
    d3.percentile_performance as trajectory_d3,
    d7.percentile_performance as trajectory_d7,
    d21.percentile_performance as trajectory_d21,
    fb.median_views as views_baseline,
    fb.median_likes as likes_baseline,
    fb.median_comments as comments_baseline,
    fhb.median_views as hour_views_baseline,
    fhb.median_likes as hour_likes_baseline,
    fhb.median_comments as hour_comments_baseline,
    coalesce(pi.model_version = 'skipped', false) as intelligence_skipped
  from public.fire_alerts fa
  join public.posts p on p.post_key = fa.post_key
  join public.feeders fd on fd.id = fa.feeder_id
  left join public.post_metrics pm
    on pm.post_key = fa.post_key
   and pm.checkpoint = fa.checkpoint
  left join public.post_metrics d1
    on d1.post_key = fa.post_key
   and d1.checkpoint = 'd1'
  left join public.post_metrics d3
    on d3.post_key = fa.post_key
   and d3.checkpoint = 'd3'
  left join public.post_metrics d7
    on d7.post_key = fa.post_key
   and d7.checkpoint = 'd7'
  left join public.post_metrics d21
    on d21.post_key = fa.post_key
   and d21.checkpoint = 'd21'
  left join public.feeder_baselines fb
    on fb.feeder_id = fa.feeder_id
   and fb.media_type = lower(coalesce(p.media_type, 'unknown'))
   and fb.checkpoint = fa.checkpoint
  left join public.feeder_hour_baselines fhb
    on fhb.feeder_id = fa.feeder_id
   and fhb.media_type = lower(coalesce(p.media_type, 'unknown'))
   and fhb.checkpoint = fa.checkpoint
   and fhb.hour_ist = extract(hour from (p.posted_at at time zone 'Asia/Kolkata'))::int
  left join public.post_intelligence pi on pi.post_key = fa.post_key
)
select
  core.*,
  case
    when core.views is not null and coalesce(core.views_baseline, 0) > 0
      then round(core.views::numeric / core.views_baseline::numeric, 4)
    else null
  end as views_multiple,
  case
    when core.likes is not null and coalesce(core.likes_baseline, 0) > 0
      then round(core.likes::numeric / core.likes_baseline::numeric, 4)
    else null
  end as likes_multiple,
  case
    when core.comments is not null and coalesce(core.comments_baseline, 0) > 0
      then round(core.comments::numeric / core.comments_baseline::numeric, 4)
    else null
  end as comments_multiple,
  case
    when core.metric_key = 'likes' and core.likes is not null and coalesce(core.hour_likes_baseline, 0) > 0
      then round(core.likes::numeric / core.hour_likes_baseline::numeric, 4)
    when core.metric_key = 'comments' and core.comments is not null and coalesce(core.hour_comments_baseline, 0) > 0
      then round(core.comments::numeric / core.hour_comments_baseline::numeric, 4)
    when core.metric_key = 'views' and core.views is not null and coalesce(core.hour_views_baseline, 0) > 0
      then round(core.views::numeric / core.hour_views_baseline::numeric, 4)
    else null
  end as hour_multiple,
  case
    when core.surface_percentile is not null and cohort.feeder_pool_size > 0
      then greatest(1, least(cohort.feeder_pool_size, round((cohort.feeder_pool_size::numeric * core.surface_percentile::numeric) / 100.0)))::int
    else null
  end as feeder_rank,
  case
    when core.feed_percentile is not null and cohort.feed_pool_size > 0
      then greatest(1, least(cohort.feed_pool_size, round((cohort.feed_pool_size::numeric * core.feed_percentile::numeric) / 100.0)))::int
    else null
  end as feed_rank,
  recent.best_in_last_n,
  case
    when core.metric_key = 'likes' and hour_rank.hour_pool > 0 and hour_rank.hour_rank is not null
      then greatest(1, least(100, round((hour_rank.hour_rank::numeric / hour_rank.hour_pool::numeric) * 100)))::int
    when core.metric_key = 'comments' and hour_rank.hour_pool > 0 and hour_rank.hour_rank is not null
      then greatest(1, least(100, round((hour_rank.hour_rank::numeric / hour_rank.hour_pool::numeric) * 100)))::int
    when core.metric_key = 'views' and hour_rank.hour_pool > 0 and hour_rank.hour_rank is not null
      then greatest(1, least(100, round((hour_rank.hour_rank::numeric / hour_rank.hour_pool::numeric) * 100)))::int
    else null
  end as hour_percentile,
  anchor.anchor_handle,
  anchor.anchor_best_pct,
  case
    when anchor.anchor_best_pct is not null and core.surface_percentile is not null
      then core.surface_percentile - anchor.anchor_best_pct
    else null
  end as anchor_gap
from core
left join lateral (
  select
    count(*)::int as feeder_pool_size,
    (
      select count(*)::int
      from public.post_metrics pm3
      join public.posts p3 on p3.post_key = pm3.post_key
      join public.feeders fd3 on fd3.id = p3.feeder_id
      where fd3.feed_id = core.feed_id
        and lower(coalesce(p3.media_type, 'unknown')) = core.media_type
        and pm3.checkpoint = core.checkpoint
        and pm3.metric_value is not null
    ) as feed_pool_size
  from public.post_metrics pm2
  join public.posts p2 on p2.post_key = pm2.post_key
  where p2.feeder_id = core.feeder_id
    and lower(coalesce(p2.media_type, 'unknown')) = core.media_type
    and pm2.checkpoint = core.checkpoint
    and pm2.metric_value is not null
) cohort on true
left join lateral (
  select case core.metric_key
    when 'likes' then (
      select count(*)::int
      from (
        select pm.likes
        from public.post_metrics pm
        join public.posts p2 on p2.post_key = pm.post_key
        where p2.feeder_id = core.feeder_id
          and lower(coalesce(p2.media_type, 'unknown')) = core.media_type
          and pm.checkpoint = core.checkpoint
          and pm.post_key <> core.post_key
          and pm.likes is not null
        order by pm.computed_at desc
        limit 50
      ) s
      where core.likes is not null
        and s.likes < core.likes
    )
    when 'comments' then (
      select count(*)::int
      from (
        select pm.comments
        from public.post_metrics pm
        join public.posts p2 on p2.post_key = pm.post_key
        where p2.feeder_id = core.feeder_id
          and lower(coalesce(p2.media_type, 'unknown')) = core.media_type
          and pm.checkpoint = core.checkpoint
          and pm.post_key <> core.post_key
          and pm.comments is not null
        order by pm.computed_at desc
        limit 50
      ) s
      where core.comments is not null
        and s.comments < core.comments
    )
    else (
      select count(*)::int
      from (
        select pm.views
        from public.post_metrics pm
        join public.posts p2 on p2.post_key = pm.post_key
        where p2.feeder_id = core.feeder_id
          and lower(coalesce(p2.media_type, 'unknown')) = core.media_type
          and pm.checkpoint = core.checkpoint
          and pm.post_key <> core.post_key
          and pm.views is not null
        order by pm.computed_at desc
        limit 50
      ) s
      where core.views is not null
        and s.views < core.views
    )
  end as best_in_last_n
) recent on true
left join lateral (
  select
    case core.metric_key
      when 'likes' then count(*)::int
      when 'comments' then count(*)::int
      else count(*)::int
    end as hour_pool,
    case core.metric_key
      when 'likes' then (
        select count(*) + 1
        from public.post_metrics pm4
        join public.posts p4 on p4.post_key = pm4.post_key
        where p4.feeder_id = core.feeder_id
          and lower(coalesce(p4.media_type, 'unknown')) = core.media_type
          and pm4.checkpoint = core.checkpoint
          and extract(hour from (p4.posted_at at time zone 'Asia/Kolkata'))::int = core.hour_ist
          and pm4.likes is not null
          and core.likes is not null
          and pm4.likes > core.likes
      )
      when 'comments' then (
        select count(*) + 1
        from public.post_metrics pm4
        join public.posts p4 on p4.post_key = pm4.post_key
        where p4.feeder_id = core.feeder_id
          and lower(coalesce(p4.media_type, 'unknown')) = core.media_type
          and pm4.checkpoint = core.checkpoint
          and extract(hour from (p4.posted_at at time zone 'Asia/Kolkata'))::int = core.hour_ist
          and pm4.comments is not null
          and core.comments is not null
          and pm4.comments > core.comments
      )
      else (
        select count(*) + 1
        from public.post_metrics pm4
        join public.posts p4 on p4.post_key = pm4.post_key
        where p4.feeder_id = core.feeder_id
          and lower(coalesce(p4.media_type, 'unknown')) = core.media_type
          and pm4.checkpoint = core.checkpoint
          and extract(hour from (p4.posted_at at time zone 'Asia/Kolkata'))::int = core.hour_ist
          and pm4.views is not null
          and core.views is not null
          and pm4.views > core.views
      )
    end as hour_rank
  from public.post_metrics pm4
  join public.posts p4 on p4.post_key = pm4.post_key
  where p4.feeder_id = core.feeder_id
    and lower(coalesce(p4.media_type, 'unknown')) = core.media_type
    and pm4.checkpoint = core.checkpoint
    and extract(hour from (p4.posted_at at time zone 'Asia/Kolkata'))::int = core.hour_ist
    and (
      (core.metric_key = 'likes' and pm4.likes is not null)
      or (core.metric_key = 'comments' and pm4.comments is not null)
      or (core.metric_key = 'views' and pm4.views is not null)
    )
) hour_rank on true
left join lateral (
  select
    fd2.handle as anchor_handle,
    min(pm5.percentile_performance)::int as anchor_best_pct
  from public.feeders fd2
  join public.posts p5 on p5.feeder_id = fd2.id
  join public.post_metrics pm5 on pm5.post_key = p5.post_key
  where fd2.feed_id = core.feed_id
    and fd2.role = 'anchor'
    and fd2.status = 'active'
    and pm5.checkpoint = core.checkpoint
    and pm5.business_date_ist = core.business_date_ist
  group by fd2.handle
  order by min(pm5.percentile_performance) asc nulls last, fd2.handle
  limit 1
) anchor on true;

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

create trigger trg_enqueue_web_push_jobs
after insert or update on public.fire_alerts
for each row
execute function public.tg_enqueue_web_push_jobs();

create or replace function public.fn_feed_dashboard(
  p_feed_id bigint,
  p_weeks int,
  p_handle text default null
)
returns json
language plpgsql
security definer
set search_path = public
as $$
declare
  v_weeks int := case when p_weeks in (4, 12, 26, 52) then p_weeks else 4 end;
  v_handle text := nullif(lower(trim(coalesce(p_handle, ''))), '');
  v_week_start date := date_trunc('week', timezone('Asia/Kolkata', now()))::date;
  v_window_start date := (date_trunc('week', timezone('Asia/Kolkata', now()))::date - ((v_weeks - 1) * 7))::date;
  v_window_end date := (date_trunc('week', timezone('Asia/Kolkata', now()))::date + 7)::date;
  v_today_ist date := (timezone('Asia/Kolkata', now()))::date;
  v_feeder_ids bigint[];
  v_anchor_feeder_id bigint;
  v_ascent_series json;
  v_frequency_series json;
  v_heatmap_daily json;
  v_killzone_hours json;
  v_apex_mix json;
  v_scatter_points json;
begin
  select
    array_agg(fd.id order by fd.id),
    max(fd.id) filter (where fd.role = 'anchor')
  into v_feeder_ids, v_anchor_feeder_id
  from public.feeders fd
  where fd.feed_id = p_feed_id
    and fd.status = 'active'
    and (v_handle is null or lower(fd.handle) = v_handle);

  if coalesce(array_length(v_feeder_ids, 1), 0) = 0 then
    return json_build_object(
      'ascent_series', '[]'::json,
      'frequency_series', '[]'::json,
      'heatmap_daily', '[]'::json,
      'killzone_hours', '[]'::json,
      'apex_mix', '[]'::json,
      'scatter_points', '[]'::json
    );
  end if;

  if v_anchor_feeder_id is not null then
    select coalesce(
      json_agg(
        json_build_object(
          'week_start_ist', s.week_start_ist,
          'follower_count', s.follower_count
        )
        order by s.week_start_ist
      ),
      '[]'::json
    )
    into v_ascent_series
    from public.feeder_follower_snapshots s
    where s.feeder_id = v_anchor_feeder_id
      and s.week_start_ist >= v_window_start
      and s.week_start_ist < v_window_end;
  else
    select coalesce(
      json_agg(
        json_build_object(
          'week_start_ist', t.week_start_ist,
          'follower_count', t.follower_count
        )
        order by t.week_start_ist
      ),
      '[]'::json
    )
    into v_ascent_series
    from (
      select s.week_start_ist, sum(s.follower_count)::bigint as follower_count
      from public.feeder_follower_snapshots s
      where s.feeder_id = any(v_feeder_ids)
        and s.week_start_ist >= v_window_start
        and s.week_start_ist < v_window_end
      group by s.week_start_ist
      order by s.week_start_ist
    ) t;
  end if;

  select coalesce(
    json_agg(
      json_build_object(
        'week_start_ist', t.week_start_ist,
        'post_count', t.post_count,
        'avg_percentile_performance', t.avg_percentile_performance,
        'avg_views_percentile', t.avg_views_percentile,
        'avg_likes_percentile', t.avg_likes_percentile,
        'avg_comments_percentile', t.avg_comments_percentile
      )
      order by t.week_start_ist
    ),
    '[]'::json
  )
  into v_frequency_series
  from (
    select
      date_trunc('week', (p.posted_at at time zone 'Asia/Kolkata'))::date as week_start_ist,
      count(*)::int as post_count,
      round(avg(pm.percentile_performance)::numeric, 2) as avg_percentile_performance,
      round(avg(pm.views_percentile)::numeric, 2) as avg_views_percentile,
      round(avg(pm.likes_percentile)::numeric, 2) as avg_likes_percentile,
      round(avg(pm.comments_percentile)::numeric, 2) as avg_comments_percentile
    from public.posts p
    join public.feeders fd on fd.id = p.feeder_id
    left join public.post_metrics pm
      on pm.post_key = p.post_key
     and pm.checkpoint = 'd1'
    where fd.feed_id = p_feed_id
      and fd.status = 'active'
      and p.feeder_id = any(v_feeder_ids)
      and p.posted_at is not null
      and (p.posted_at at time zone 'Asia/Kolkata')::date >= v_window_start
      and (p.posted_at at time zone 'Asia/Kolkata')::date < v_window_end
    group by 1
    order by 1
  ) t;

  select coalesce(
    json_agg(
      json_build_object(
        'day_ist', t.day_ist,
        'post_count', t.post_count
      )
      order by t.day_ist
    ),
    '[]'::json
  )
  into v_heatmap_daily
  from (
    select
      (p.posted_at at time zone 'Asia/Kolkata')::date as day_ist,
      count(*)::int as post_count
    from public.posts p
    where p.feeder_id = any(v_feeder_ids)
      and p.posted_at is not null
      and (p.posted_at at time zone 'Asia/Kolkata')::date >= v_window_start
      and (p.posted_at at time zone 'Asia/Kolkata')::date < v_window_end
    group by 1
    order by 1
  ) t;

  select coalesce(
    json_agg(
      json_build_object(
        'hour_ist', t.hour_ist,
        'post_count', t.post_count
      )
      order by t.hour_ist
    ),
    '[]'::json
  )
  into v_killzone_hours
  from (
    select
      extract(hour from (p.posted_at at time zone 'Asia/Kolkata'))::int as hour_ist,
      count(*)::int as post_count
    from public.posts p
    where p.feeder_id = any(v_feeder_ids)
      and p.posted_at is not null
      and (p.posted_at at time zone 'Asia/Kolkata')::date >= v_window_start
      and (p.posted_at at time zone 'Asia/Kolkata')::date < v_window_end
    group by 1
    order by 1
  ) t;

  select coalesce(
    json_agg(
      json_build_object(
        'media_type', t.media_type,
        'post_count', t.post_count,
        'share', t.share
      )
      order by t.post_count desc, t.media_type
    ),
    '[]'::json
  )
  into v_apex_mix
  from (
    select
      lower(coalesce(p.media_type, 'unknown')) as media_type,
      count(*)::int as post_count,
      round((count(*)::numeric / nullif(sum(count(*)) over (), 0)), 4) as share
    from public.posts p
    where p.feeder_id = any(v_feeder_ids)
      and p.posted_at is not null
      and (p.posted_at at time zone 'Asia/Kolkata')::date >= v_window_start
      and (p.posted_at at time zone 'Asia/Kolkata')::date < v_window_end
    group by 1
  ) t;

  select coalesce(
    json_agg(
      json_build_object(
        'post_key', t.post_key,
        'days_ago', t.days_ago,
        'percentile_performance', t.percentile_performance,
        'views', t.views,
        'handle', t.handle,
        'posted_at_ist', t.posted_at_ist
      )
      order by t.posted_at desc
    ),
    '[]'::json
  )
  into v_scatter_points
  from (
    select
      p.post_key,
      greatest(0, (v_today_ist - (p.posted_at at time zone 'Asia/Kolkata')::date))::int as days_ago,
      pm.percentile_performance,
      pm.views,
      fd.handle,
      (p.posted_at at time zone 'Asia/Kolkata') as posted_at_ist,
      p.posted_at
    from public.posts p
    join public.feeders fd on fd.id = p.feeder_id
    left join public.post_metrics pm
      on pm.post_key = p.post_key
     and pm.checkpoint = 'd1'
    where p.feeder_id = any(v_feeder_ids)
      and p.posted_at is not null
      and (p.posted_at at time zone 'Asia/Kolkata')::date >= v_window_start
      and (p.posted_at at time zone 'Asia/Kolkata')::date < v_window_end
    order by p.posted_at desc
    limit 500
  ) t;

  return json_build_object(
    'ascent_series', coalesce(v_ascent_series, '[]'::json),
    'frequency_series', coalesce(v_frequency_series, '[]'::json),
    'heatmap_daily', coalesce(v_heatmap_daily, '[]'::json),
    'killzone_hours', coalesce(v_killzone_hours, '[]'::json),
    'apex_mix', coalesce(v_apex_mix, '[]'::json),
    'scatter_points', coalesce(v_scatter_points, '[]'::json)
  );
end;
$$;

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

commit;
