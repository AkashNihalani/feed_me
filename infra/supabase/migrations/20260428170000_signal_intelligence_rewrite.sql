begin;

-- Feed-level context captured from the onboarding brief. This is the only
-- persistent brand/niche context the LLM path needs for v1.
alter table if exists public.feeds
  add column if not exists context_brief jsonb not null default '{}'::jsonb,
  add column if not exists context_bible text not null default '';

alter table if exists public.feeders
  add column if not exists context_role text not null default 'standard',
  add column if not exists context_note text not null default '',
  add column if not exists bio text not null default '';

-- Deterministic strata. These replace LLM structural tags as detection inputs.
alter table if exists public.posts
  add column if not exists duration_seconds numeric(10,3),
  add column if not exists duration_bucket text,
  add column if not exists carousel_slide_count integer,
  add column if not exists depth_bucket text;

update public.posts
set carousel_slide_count = case
      when jsonb_typeof(carousel_urls) = 'array' then jsonb_array_length(carousel_urls)
      else carousel_slide_count
    end,
    depth_bucket = case
      when lower(coalesce(media_type, '')) in ('sidecar', 'carousel') then
        case
          when coalesce(carousel_slide_count, case when jsonb_typeof(carousel_urls) = 'array' then jsonb_array_length(carousel_urls) else null end) between 2 and 3 then 'DEPTH_MINI'
          when coalesce(carousel_slide_count, case when jsonb_typeof(carousel_urls) = 'array' then jsonb_array_length(carousel_urls) else null end) between 4 and 7 then 'DEPTH_STANDARD'
          when coalesce(carousel_slide_count, case when jsonb_typeof(carousel_urls) = 'array' then jsonb_array_length(carousel_urls) else null end) >= 8 then 'DEPTH_DEEP'
          else 'DEPTH_UNKNOWN'
        end
      else null
    end
where lower(coalesce(media_type, '')) in ('sidecar', 'carousel')
  and (carousel_slide_count is null or depth_bucket is null);

alter table if exists public.posts
  drop constraint if exists posts_duration_bucket_check;

alter table if exists public.posts
  add constraint posts_duration_bucket_check
  check (
    duration_bucket is null
    or duration_bucket in ('DUR_SHORT', 'DUR_MEDIUM', 'DUR_LONG', 'DUR_EXTENDED', 'DUR_UNKNOWN')
  );

alter table if exists public.posts
  drop constraint if exists posts_depth_bucket_check;

alter table if exists public.posts
  add constraint posts_depth_bucket_check
  check (
    depth_bucket is null
    or depth_bucket in ('DEPTH_MINI', 'DEPTH_STANDARD', 'DEPTH_DEEP', 'DEPTH_UNKNOWN')
  );

create table if not exists public.signals (
  id bigserial primary key,
  feed_id bigint not null references public.feeds(id) on delete cascade,
  feeder_id bigint references public.feeders(id) on delete cascade,
  scope text not null check (scope in ('own', 'cross', 'anchor')),
  signal_type text not null,
  signal_family text not null,
  media_type text,
  sub_bucket text,
  checkpoint text not null default 'daily',
  business_date_ist date not null,
  trigger_window_start date,
  trigger_window_end date,
  metric_snapshot jsonb not null default '{}'::jsonb,
  status text not null default 'pending'
    check (status in ('pending', 'fresh', 'stale', 'dismissed', 'suppressed_cap', 'suppressed_confidence', 'error')),
  body text not null default '',
  last_fired_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists signals_dedupe_idx
  on public.signals (
    scope,
    feed_id,
    feeder_id,
    signal_type,
    media_type,
    sub_bucket,
    checkpoint,
    business_date_ist
  )
  nulls not distinct;

create index if not exists signals_feed_window_idx
  on public.signals(feed_id, business_date_ist desc, scope, status);

create index if not exists signals_feeder_window_idx
  on public.signals(feeder_id, business_date_ist desc, scope, status)
  where feeder_id is not null;

create table if not exists public.signal_posts (
  signal_id bigint not null references public.signals(id) on delete cascade,
  post_key text not null references public.posts(post_key) on delete cascade,
  cohort text not null default 'a' check (cohort in ('a', 'b')),
  rank integer not null default 0,
  role text not null default 'support',
  primary key (signal_id, post_key, cohort)
);

create index if not exists signal_posts_post_key_idx
  on public.signal_posts(post_key, signal_id);

create table if not exists public.post_fingerprints (
  post_key text primary key references public.posts(post_key) on delete cascade,
  fingerprint jsonb not null,
  media_source_hash text not null default '',
  caption_hash text not null default '',
  sampling_policy_version text not null,
  model_version text not null,
  media_confidence text not null default 'low' check (media_confidence in ('high', 'medium', 'low')),
  generated_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.signal_intelligence (
  signal_id bigint primary key references public.signals(id) on delete cascade,
  card jsonb not null,
  sample_hash text not null,
  model_version text not null,
  generated_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Retire the D7 tag hardlock and the old structural-tag source of truth.
drop trigger if exists trg_checkpoint_done_requires_hot_d7_intelligence on public.checkpoint_jobs;
drop function if exists public.tg_checkpoint_done_requires_hot_d7_intelligence();
drop function if exists public.fn_hot_d7_post_intelligence_ready(text);

drop table if exists public.post_intelligence cascade;

-- Legacy FireWatch pattern payloads are no longer authoritative.
delete from public.fire_alerts
where signal_code in ('OWN_PATTERN', 'CROSS_PATTERN', 'ANCHOR_PATTERN');

commit;
