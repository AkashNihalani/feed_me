-- FeedMe V1 Intelligence Resolver
-- Fire all qualified signals, organize by context (own/competitor/macro)
-- No pool-health minimums (alerts should start immediately)

begin;

-- -------------------------------------------------------------------
-- Phase 0A: evolve fire_alerts in place
-- -------------------------------------------------------------------
alter table public.fire_alerts
  add column if not exists context text,
  add column if not exists family_group text,
  add column if not exists signal_code text,
  add column if not exists intensity_score int,
  add column if not exists headline text,
  add column if not exists feeder_id bigint references public.feeders(id),
  add column if not exists business_date_ist date;

create index if not exists fire_alerts_context_idx on public.fire_alerts(context);
create index if not exists fire_alerts_signal_code_idx on public.fire_alerts(signal_code);
create index if not exists fire_alerts_feeder_id_idx on public.fire_alerts(feeder_id);
create index if not exists fire_alerts_business_date_idx on public.fire_alerts(business_date_ist desc, feed_id, created_at desc);

-- Keep dedupe unique and reusable with new resolver keys.
create unique index if not exists fire_alerts_dedupe_key_uniq on public.fire_alerts(dedupe_key);

-- Relax NOT NULL for legacy columns that resolver no longer writes directly.
alter table public.fire_alerts
  alter column title drop not null,
  alter column body drop not null;

-- Remove legacy checks if still present.
do $$
begin
  if exists (
    select 1 from pg_constraint
    where conname = 'fire_alerts_alert_family_check'
      and conrelid = 'public.fire_alerts'::regclass
  ) then
    alter table public.fire_alerts drop constraint fire_alerts_alert_family_check;
  end if;

  if exists (
    select 1 from pg_constraint
    where conname = 'fire_alerts_alert_urgency_check'
      and conrelid = 'public.fire_alerts'::regclass
  ) then
    alter table public.fire_alerts drop constraint fire_alerts_alert_urgency_check;
  end if;

  if exists (
    select 1 from pg_constraint
    where conname = 'fire_alerts_confidence_check'
      and conrelid = 'public.fire_alerts'::regclass
  ) then
    alter table public.fire_alerts drop constraint fire_alerts_confidence_check;
  end if;
end $$;

-- -------------------------------------------------------------------
-- Phase 0B: post_metrics additions
-- -------------------------------------------------------------------
alter table public.post_metrics
  add column if not exists feed_percentile int,
  add column if not exists percentile_delta int;

-- -------------------------------------------------------------------
-- Helpers
-- -------------------------------------------------------------------
create or replace function public.fn_checkpoint_rank(p_checkpoint text)
returns int
language sql
immutable
as $$
  select case p_checkpoint
    when 'd1' then 1
    when 'd2b' then 2
    when 'd3' then 3
    when 'd7' then 4
    when 'd21' then 5
    else 99
  end
$$;

create or replace function public.fn_alert_type_from_intensity(p_intensity int)
returns text
language sql
immutable
as $$
  select case
    when p_intensity >= 70 then 'blaze'
    when p_intensity >= 40 then 'burn'
    else 'spark'
  end
$$;

create or replace function public.fn_media_label(p_media text)
returns text
language sql
immutable
as $$
  select case lower(coalesce(p_media, ''))
    when 'reel' then 'reel'
    when 'sidecar' then 'carousel'
    when 'image' then 'static'
    else coalesce(p_media, 'post')
  end
$$;

-- Central upsert for resolver rows.
create or replace function public.fn_upsert_fire_alert(
  p_feed_id bigint,
  p_feeder_id bigint,
  p_post_key text,
  p_checkpoint text,
  p_context text,
  p_family_group text,
  p_signal_code text,
  p_intensity int,
  p_headline text,
  p_body text,
  p_business_date_ist date,
  p_payload jsonb
)
returns bigint
language plpgsql
security definer
set search_path = public
as $$
declare
  v_alert_type text;
  v_dedupe_key text;
  v_id bigint;
begin
  if p_context not in ('own','competitor','macro') then
    return null;
  end if;
  if p_family_group not in ('A','B','C','D','E') then
    return null;
  end if;

  v_alert_type := public.fn_alert_type_from_intensity(greatest(0, least(100, coalesce(p_intensity, 0))));
  v_dedupe_key := format('%s:%s:%s:%s', p_signal_code, coalesce(p_post_key, 'feed'), coalesce(p_checkpoint, 'na'), p_context);

  insert into public.fire_alerts (
    feed_id,
    feeder_id,
    post_key,
    checkpoint,
    alert_type,
    signal_code,
    context,
    family_group,
    intensity_score,
    headline,
    title,
    body,
    payload,
    priority_score,
    status,
    dedupe_key,
    business_date_ist,
    created_at,
    updated_at
  )
  values (
    p_feed_id,
    p_feeder_id,
    p_post_key,
    p_checkpoint,
    v_alert_type,
    p_signal_code,
    p_context,
    p_family_group,
    greatest(0, least(100, coalesce(p_intensity, 0))),
    p_headline,
    p_headline,
    p_body,
    coalesce(p_payload, '{}'::jsonb),
    greatest(0, least(100, coalesce(p_intensity, 0))),
    'new',
    v_dedupe_key,
    p_business_date_ist,
    now(),
    now()
  )
  on conflict (dedupe_key)
  do update set
    alert_type = excluded.alert_type,
    intensity_score = excluded.intensity_score,
    headline = excluded.headline,
    title = excluded.title,
    body = excluded.body,
    payload = excluded.payload,
    priority_score = excluded.priority_score,
    business_date_ist = excluded.business_date_ist,
    updated_at = now()
  returning id into v_id;

  return v_id;
end;
$$;

-- -------------------------------------------------------------------
-- Feed percentile + delta computation in tg_compute_post_metrics
-- -------------------------------------------------------------------
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
  v_follower_count bigint;
  v_mv numeric;
  v_pool_size int;
  v_rank int;
  v_pct int;
  v_feed_pool_size int;
  v_feed_rank int;
  v_feed_pct int;
  v_d1_pct int;
begin
  select p.media_type, p.feeder_id
  into v_media_type, v_feeder_id
  from public.posts p
  where p.post_key = new.post_key;

  if v_feeder_id is not null then
    select fd.feed_id, fd.follower_count
    into v_feed_id, v_follower_count
    from public.feeders fd
    where fd.id = v_feeder_id;
  end if;

  v_mv := public.fn_metric_value(coalesce(v_media_type, 'image'), new.views, new.likes, new.comments);

  v_pct := null;
  if v_mv is not null and v_feeder_id is not null then
    select count(*) + 1
    into v_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.post_key <> new.post_key
      and pm.metric_value is not null
      and pm.metric_value > v_mv;

    select count(*) + 1
    into v_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.metric_value is not null;

    if v_pool_size > 0 then
      v_pct := greatest(1, least(100, round((v_rank::numeric / v_pool_size) * 100)));
    end if;
  end if;

  v_feed_pct := null;
  if v_mv is not null and v_feed_id is not null then
    select count(*) + 1
    into v_feed_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    join public.feeders f2 on f2.id = p2.feeder_id
    where f2.feed_id = v_feed_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.post_key <> new.post_key
      and pm.metric_value is not null
      and pm.metric_value > v_mv;

    select count(*) + 1
    into v_feed_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    join public.feeders f2 on f2.id = p2.feeder_id
    where f2.feed_id = v_feed_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.metric_value is not null;

    if v_feed_pool_size > 0 then
      v_feed_pct := greatest(1, least(100, round((v_feed_rank::numeric / v_feed_pool_size) * 100)));
    end if;
  end if;

  select pm.percentile_performance
  into v_d1_pct
  from public.post_metrics pm
  where pm.post_key = new.post_key
    and pm.checkpoint = 'd1'
  order by pm.computed_at desc
  limit 1;

  new.metric_value := v_mv;
  new.percentile_performance := v_pct;
  new.percentile_tag := public.fn_percentile_tag(v_pct);
  new.feed_percentile := v_feed_pct;
  new.percentile_delta := case
    when new.checkpoint = 'd1' then null
    when v_d1_pct is null or v_pct is null then null
    else v_d1_pct - v_pct
  end;
  new.perf_score := case
    when v_mv is not null and v_follower_count is not null and v_follower_count > 0
      then round((v_mv / v_follower_count) * 100.0, 4)
    else null
  end;

  return new;
end;
$$;

-- -------------------------------------------------------------------
-- Phase 1: feeder baselines refresh
-- -------------------------------------------------------------------
create table if not exists public.feeder_baselines (
  feeder_id bigint not null references public.feeders(id) on delete cascade,
  media_type text not null,
  checkpoint text not null,
  median_percentile int,
  p25_percentile int,
  p75_percentile int,
  median_likes bigint,
  median_comments bigint,
  median_views bigint,
  median_delta int,
  format_rank int,
  format_count int,
  best_percentile_90d int,
  best_percentile_post text,
  days_since_ceiling int,
  post_count_90d int,
  updated_at timestamptz not null default now(),
  primary key (feeder_id, media_type, checkpoint)
);

create index if not exists feeder_baselines_checkpoint_idx on public.feeder_baselines(checkpoint, feeder_id);

alter table public.feeder_baselines enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname='public' and tablename='feeder_baselines' and policyname='Users can read own feeder baselines'
  ) then
    create policy "Users can read own feeder baselines"
      on public.feeder_baselines for select
      using (
        exists (
          select 1
          from public.feeders fd
          join public.feeds f on f.id = fd.feed_id
          where fd.id = feeder_baselines.feeder_id
            and f.user_id = auth.uid()
        )
      );
  end if;
end $$;

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
      p.media_type,
      pm.post_key,
      pm.percentile_performance,
      pm.likes,
      pm.comments,
      pm.views,
      pm.percentile_delta,
      p.posted_at
    from public.post_metrics pm
    join public.posts p on p.post_key = pm.post_key
    where p.feeder_id = p_feeder_id
      and pm.checkpoint = p_checkpoint
      and pm.percentile_performance is not null
      and p.posted_at >= now() - interval '90 days'
  ),
  stats as (
    select
      media_type,
      percentile_cont(0.5) within group (order by percentile_performance)::int as median_percentile,
      percentile_cont(0.25) within group (order by percentile_performance)::int as p25_percentile,
      percentile_cont(0.75) within group (order by percentile_performance)::int as p75_percentile,
      percentile_cont(0.5) within group (order by likes)::bigint as median_likes,
      percentile_cont(0.5) within group (order by comments)::bigint as median_comments,
      percentile_cont(0.5) within group (order by views)::bigint as median_views,
      percentile_cont(0.5) within group (order by percentile_delta)::int as median_delta,
      min(percentile_performance)::int as best_percentile_90d,
      count(*)::int as post_count_90d
    from base
    group by media_type
  ),
  best_post as (
    select distinct on (b.media_type)
      b.media_type,
      b.post_key as best_percentile_post,
      (current_date - (b.posted_at at time zone 'Asia/Kolkata')::date)::int as days_since_ceiling
    from base b
    order by b.media_type, b.percentile_performance asc, b.posted_at desc
  ),
  ranked as (
    select
      s.*,
      dense_rank() over(order by s.median_percentile asc nulls last) as format_rank,
      count(*) over() as format_count
    from stats s
  )
  insert into public.feeder_baselines (
    feeder_id,
    media_type,
    checkpoint,
    median_percentile,
    p25_percentile,
    p75_percentile,
    median_likes,
    median_comments,
    median_views,
    median_delta,
    format_rank,
    format_count,
    best_percentile_90d,
    best_percentile_post,
    days_since_ceiling,
    post_count_90d,
    updated_at
  )
  select
    p_feeder_id,
    r.media_type,
    p_checkpoint,
    r.median_percentile,
    r.p25_percentile,
    r.p75_percentile,
    r.median_likes,
    r.median_comments,
    r.median_views,
    r.median_delta,
    r.format_rank,
    r.format_count,
    r.best_percentile_90d,
    bp.best_percentile_post,
    bp.days_since_ceiling,
    r.post_count_90d,
    now()
  from ranked r
  left join best_post bp on bp.media_type = r.media_type
  on conflict (feeder_id, media_type, checkpoint)
  do update set
    median_percentile = excluded.median_percentile,
    p25_percentile = excluded.p25_percentile,
    p75_percentile = excluded.p75_percentile,
    median_likes = excluded.median_likes,
    median_comments = excluded.median_comments,
    median_views = excluded.median_views,
    median_delta = excluded.median_delta,
    format_rank = excluded.format_rank,
    format_count = excluded.format_count,
    best_percentile_90d = excluded.best_percentile_90d,
    best_percentile_post = excluded.best_percentile_post,
    days_since_ceiling = excluded.days_since_ceiling,
    post_count_90d = excluded.post_count_90d,
    updated_at = now();

  get diagnostics v_rows = row_count;
  return coalesce(v_rows, 0);
end;
$$;

-- -------------------------------------------------------------------
-- Phase 2: Post-level resolver (A + B + competitor context)
-- -------------------------------------------------------------------
create or replace function public.fn_resolve_post_signals(
  p_feeder_id bigint,
  p_checkpoint text
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_count int := 0;
  v_feed_id bigint;
  v_anchor_feeder_id bigint;
  v_anchor_handle text;
  r record;
  v_context text;
  v_intensity int;
  v_business_date_ist date;
  v_reach_metric text;
  v_reach_multiple numeric;
  v_eff_gap int;
  v_gap_vs_anchor int;
begin
  if p_checkpoint not in ('d1','d3','d7','d21') then
    return 0;
  end if;

  select fd.feed_id into v_feed_id from public.feeders fd where fd.id = p_feeder_id;
  if v_feed_id is null then
    return 0;
  end if;

  select fd.id, fd.handle
  into v_anchor_feeder_id, v_anchor_handle
  from public.feeders fd
  where fd.feed_id = v_feed_id
    and fd.role = 'anchor'
    and fd.status = 'active'
  limit 1;

  for r in
    with curr as (
      select
        pm.post_key,
        pm.checkpoint,
        pm.percentile_performance,
        pm.percentile_delta,
        pm.likes,
        pm.comments,
        pm.views,
        pm.likes_percentile,
        pm.comments_percentile,
        pm.views_percentile,
        p.media_type,
        p.posted_at,
        p.post_url,
        p.thumbnail_url,
        fd.handle,
        fd.id as feeder_id,
        fb.median_percentile,
        fb.p25_percentile,
        fb.p75_percentile,
        fb.median_likes,
        fb.median_comments,
        fb.median_views,
        fb.median_delta,
        fb.best_percentile_90d,
        fb.days_since_ceiling,
        fb.format_rank,
        fb.post_count_90d,
        (
          select count(*)
          from public.post_metrics pmx
          join public.posts px on px.post_key = pmx.post_key
          where px.feeder_id = fd.id
            and px.media_type = p.media_type
            and pmx.checkpoint = 'd1'
            and pmx.percentile_performance <= 35
            and px.posted_at <= p.posted_at
            and px.posted_at >= p.posted_at - interval '60 days'
        ) as hot_streak_count,
        (
          select pm_d1.percentile_performance
          from public.post_metrics pm_d1
          where pm_d1.post_key = pm.post_key and pm_d1.checkpoint = 'd1'
          order by pm_d1.computed_at desc
          limit 1
        ) as d1_pct,
        (
          select pm_d3.percentile_performance
          from public.post_metrics pm_d3
          where pm_d3.post_key = pm.post_key and pm_d3.checkpoint = 'd3'
          order by pm_d3.computed_at desc
          limit 1
        ) as d3_pct,
        (
          select pm_d7.percentile_performance
          from public.post_metrics pm_d7
          where pm_d7.post_key = pm.post_key and pm_d7.checkpoint = 'd7'
          order by pm_d7.computed_at desc
          limit 1
        ) as d7_pct,
        (
          select pm_d21.percentile_performance
          from public.post_metrics pm_d21
          where pm_d21.post_key = pm.post_key and pm_d21.checkpoint = 'd21'
          order by pm_d21.computed_at desc
          limit 1
        ) as d21_pct
      from public.post_metrics pm
      join public.posts p on p.post_key = pm.post_key
      join public.feeders fd on fd.id = p.feeder_id
      left join public.feeder_baselines fb
        on fb.feeder_id = fd.id and fb.media_type = p.media_type and fb.checkpoint = pm.checkpoint
      where fd.id = p_feeder_id
        and pm.checkpoint = p_checkpoint
        and pm.percentile_performance is not null
        and pm.percentile_performance <= 35
    )
    select * from curr
  loop
    v_business_date_ist := case
      when r.posted_at is not null then (r.posted_at at time zone 'Asia/Kolkata')::date
      else (now() at time zone 'Asia/Kolkata')::date
    end;

    v_context := 'own';

    -- A1 baseline displacement
    if r.median_percentile is not null then
      v_intensity := least(100, greatest(5, (r.median_percentile - r.percentile_performance) * 5));
      if (r.median_percentile - r.percentile_performance) >= 5 then
        perform public.fn_upsert_fire_alert(
          v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
          v_context, 'A', 'A1_baseline_displacement', v_intensity,
          'Edge forming',
          format('This %s outperformed your recent baseline at %s.', public.fn_media_label(r.media_type), upper(r.checkpoint)),
          v_business_date_ist,
          jsonb_build_object('handle', r.handle, 'media_type', r.media_type, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url,
                             'checkpoint', r.checkpoint, 'percentile', r.percentile_performance, 'baseline_median', r.median_percentile)
        );
        v_count := v_count + 1;
      end if;
    end if;

    -- A2 ceiling break
    if coalesce(r.days_since_ceiling,0) >= 21 and (r.best_percentile_90d is null or r.percentile_performance < r.best_percentile_90d) then
      v_intensity := case
        when r.days_since_ceiling >= 120 then 90
        when r.days_since_ceiling >= 60 then 65
        else 42
      end;
      perform public.fn_upsert_fire_alert(
        v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
        v_context, 'A', 'A2_ceiling_break', v_intensity,
        'Fresh high',
        format('Strongest %s in %s days.', public.fn_media_label(r.media_type), r.days_since_ceiling),
        v_business_date_ist,
        jsonb_build_object('handle', r.handle, 'media_type', r.media_type, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url,
                           'days_since_ceiling', r.days_since_ceiling, 'best_90d', r.best_percentile_90d, 'percentile', r.percentile_performance)
      );
      v_count := v_count + 1;
    end if;

    -- A3 reach explosion
    v_reach_multiple := null;
    if r.media_type = 'reel' and r.median_views is not null and r.median_views > 0 and r.views is not null then
      v_reach_multiple := r.views::numeric / r.median_views::numeric;
      v_reach_metric := 'views';
    elsif r.median_likes is not null and r.median_likes > 0 and r.likes is not null then
      v_reach_multiple := r.likes::numeric / r.median_likes::numeric;
      v_reach_metric := 'likes';
    end if;

    if v_reach_multiple is not null and v_reach_multiple >= 1.5 then
      v_intensity := case
        when v_reach_multiple >= 5 then 90
        when v_reach_multiple >= 3 then 65
        else 42
      end;
      perform public.fn_upsert_fire_alert(
        v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
        v_context, 'A', 'A3_reach_explosion', v_intensity,
        'Reach lift',
        format('%s are %.1fx your normal %s baseline.', initcap(v_reach_metric), v_reach_multiple, public.fn_media_label(r.media_type)),
        v_business_date_ist,
        jsonb_build_object('handle', r.handle, 'media_type', r.media_type, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url,
                           'reach_metric', v_reach_metric, 'reach_multiple', round(v_reach_multiple,2), 'checkpoint', r.checkpoint)
      );
      v_count := v_count + 1;
    end if;

    -- A4 engagement efficiency
    v_eff_gap := null;
    if r.comments_percentile is not null then
      if r.media_type = 'reel' and r.views_percentile is not null then
        v_eff_gap := r.views_percentile - r.comments_percentile;
      elsif r.likes_percentile is not null then
        v_eff_gap := r.likes_percentile - r.comments_percentile;
      end if;
    end if;

    if v_eff_gap is not null and v_eff_gap >= 12 then
      v_intensity := case
        when v_eff_gap >= 30 then 90
        when v_eff_gap >= 20 then 65
        else 42
      end;
      perform public.fn_upsert_fire_alert(
        v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
        v_context, 'A', 'A4_engagement_efficiency', v_intensity,
        'Efficiency nudge',
        format('Comments are %s positions ahead of reach.', v_eff_gap),
        v_business_date_ist,
        jsonb_build_object('handle', r.handle, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url,
                           'gap', v_eff_gap, 'comments_percentile', r.comments_percentile,
                           'likes_percentile', r.likes_percentile, 'views_percentile', r.views_percentile)
      );
      v_count := v_count + 1;
    end if;

    -- A5 cold open (D1 only)
    if r.checkpoint = 'd1' and r.percentile_performance >= 60 then
      v_intensity := case
        when r.percentile_performance >= 90 then 90
        when r.percentile_performance >= 80 then 65
        else 42
      end;
      perform public.fn_upsert_fire_alert(
        v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
        v_context, 'A', 'A5_cold_open', v_intensity,
        'Soft open',
        format('This %s opened behind your recent range.', public.fn_media_label(r.media_type)),
        v_business_date_ist,
        jsonb_build_object('handle', r.handle, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url,
                           'percentile', r.percentile_performance)
      );
      v_count := v_count + 1;
    end if;

    -- A6 consistency streak
    if coalesce(r.hot_streak_count,0) >= 3 then
      v_intensity := case
        when r.hot_streak_count >= 8 then 90
        when r.hot_streak_count >= 5 then 65
        else 42
      end;
      perform public.fn_upsert_fire_alert(
        v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
        v_context, 'A', 'A6_consistency_streak', v_intensity,
        'Building rhythm',
        format('%s %s posts in a row inside hot zone.', r.hot_streak_count, public.fn_media_label(r.media_type)),
        v_business_date_ist,
        jsonb_build_object('handle', r.handle, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url,
                           'streak_count', r.hot_streak_count)
      );
      v_count := v_count + 1;
    end if;

    -- B1 structural acceleration
    if r.checkpoint in ('d3','d7','d21') and r.percentile_delta is not null and r.percentile_delta >= 6 then
      v_intensity := case
        when r.percentile_delta >= 25 then 90
        when r.percentile_delta >= 15 then 65
        else 42
      end;
      perform public.fn_upsert_fire_alert(
        v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
        v_context, 'B', 'B1_structural_acceleration', v_intensity,
        'Gaining ground',
        format('Climbed %s positions since D1.', r.percentile_delta),
        v_business_date_ist,
        jsonb_build_object('handle', r.handle, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url,
                           'delta', r.percentile_delta)
      );
      v_count := v_count + 1;
    end if;

    -- B2 late bloomer
    if r.d1_pct is not null and r.d1_pct > 50 and r.percentile_performance is not null and (r.d1_pct - r.percentile_performance) >= 12 then
      v_intensity := case
        when r.checkpoint = 'd7' and r.percentile_performance <= 10 then 90
        when r.percentile_performance <= 35 then 65
        else 42
      end;
      perform public.fn_upsert_fire_alert(
        v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
        v_context, 'B', 'B2_late_bloomer', v_intensity,
        'Late ignition',
        format('Opened cold at %s%%, now %s%% by %s.', r.d1_pct, r.percentile_performance, upper(r.checkpoint)),
        v_business_date_ist,
        jsonb_build_object('handle', r.handle, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url,
                           'd1_percentile', r.d1_pct, 'current_percentile', r.percentile_performance)
      );
      v_count := v_count + 1;
    end if;

    -- B3 early fade
    if r.d1_pct is not null and r.d1_pct <= 35 and r.percentile_performance is not null and (r.percentile_performance - r.d1_pct) >= 8 then
      v_intensity := case
        when (r.percentile_performance - r.d1_pct) >= 30 or r.percentile_performance > 35 then 90
        when (r.percentile_performance - r.d1_pct) >= 18 then 65
        else 42
      end;
      perform public.fn_upsert_fire_alert(
        v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
        v_context, 'B', 'B3_early_fade', v_intensity,
        'Momentum lost',
        format('Dropped %s positions since D1.', (r.percentile_performance - r.d1_pct)),
        v_business_date_ist,
        jsonb_build_object('handle', r.handle, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url,
                           'd1_percentile', r.d1_pct, 'current_percentile', r.percentile_performance)
      );
      v_count := v_count + 1;
    end if;

    -- B4 sustained strength
    if r.d1_pct is not null and r.d1_pct <= 35 then
      if (r.checkpoint = 'd3' and r.d3_pct is not null and r.d3_pct <= 35 and abs(r.d3_pct - r.d1_pct) <= 5)
         or (r.checkpoint = 'd7' and r.d3_pct is not null and r.d7_pct is not null and r.d3_pct <= 35 and r.d7_pct <= 35 and abs(r.d7_pct - r.d1_pct) <= 5)
         or (r.checkpoint = 'd21' and r.d3_pct is not null and r.d7_pct is not null and r.d21_pct is not null and r.d3_pct <= 35 and r.d7_pct <= 35 and r.d21_pct <= 35)
      then
        v_intensity := case when r.checkpoint='d21' then 90 when r.checkpoint='d7' then 65 else 42 end;
        perform public.fn_upsert_fire_alert(
          v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
          v_context, 'B', 'B4_sustained_strength', v_intensity,
          'Holding steady',
          format('Still in hot zone through %s with limited drift.', upper(r.checkpoint)),
          v_business_date_ist,
          jsonb_build_object('handle', r.handle, 'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url,
                             'd1', r.d1_pct, 'd3', r.d3_pct, 'd7', r.d7_pct, 'd21', r.d21_pct)
        );
        v_count := v_count + 1;
      end if;
    end if;

    -- competitor context mirror for non-anchor feeders (anchor-relative signal)
    if v_anchor_feeder_id is not null and r.feeder_id <> v_anchor_feeder_id then
      select fb_anchor.median_percentile - r.percentile_performance
      into v_gap_vs_anchor
      from public.feeder_baselines fb_anchor
      where fb_anchor.feeder_id = v_anchor_feeder_id
        and fb_anchor.media_type = r.media_type
        and fb_anchor.checkpoint = r.checkpoint;

      if v_gap_vs_anchor is not null and v_gap_vs_anchor >= 5 then
        v_intensity := case
          when v_gap_vs_anchor >= 20 then 90
          when v_gap_vs_anchor >= 12 then 65
          else 42
        end;

        perform public.fn_upsert_fire_alert(
          v_feed_id, r.feeder_id, r.post_key, r.checkpoint,
          'competitor', 'D', 'D1_anchor_displacement', v_intensity,
          'Anchor displacement',
          format('@%s is %s positions ahead of @%s at %s.', r.handle, v_gap_vs_anchor, coalesce(v_anchor_handle,'anchor'), upper(r.checkpoint)),
          v_business_date_ist,
          jsonb_build_object('competitor_handle', r.handle, 'anchor_handle', v_anchor_handle,
                             'post_url', r.post_url, 'thumbnail_url', r.thumbnail_url,
                             'gap_vs_anchor', v_gap_vs_anchor, 'checkpoint', r.checkpoint)
        );
        v_count := v_count + 1;
      end if;
    end if;

  end loop;

  return v_count;
end;
$$;

-- -------------------------------------------------------------------
-- Phase 3: Feed-level resolver (C + D + E)
-- -------------------------------------------------------------------
create or replace function public.fn_resolve_feed_signals(
  p_feed_id bigint,
  p_checkpoint text,
  p_business_date_ist date default null
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_count int := 0;
  v_day date := coalesce(p_business_date_ist, (now() at time zone 'Asia/Kolkata')::date);
  v_anchor_feeder_id bigint;
  v_anchor_handle text;
  v_total_hot int;
  v_reel_hot int;
  v_feeders_hot int;
  v_feeders_total int;
  v_comp record;
  v_anchor_median numeric;
  v_comp_median numeric;
  v_gap numeric;
  v_context_feeder bigint;
begin
  if p_checkpoint not in ('d1','d3','d7','d21') then
    return 0;
  end if;

  select fd.id, fd.handle
  into v_anchor_feeder_id, v_anchor_handle
  from public.feeders fd
  where fd.feed_id = p_feed_id
    and fd.role = 'anchor'
    and fd.status = 'active'
  limit 1;

  select count(*)::int,
         count(*) filter (where p.media_type = 'reel')::int,
         count(distinct p.feeder_id)::int
  into v_total_hot, v_reel_hot, v_feeders_hot
  from public.post_metrics pm
  join public.posts p on p.post_key = pm.post_key
  join public.feeders fd on fd.id = p.feeder_id
  where fd.feed_id = p_feed_id
    and pm.checkpoint = p_checkpoint
    and pm.percentile_performance <= 35
    and (p.posted_at at time zone 'Asia/Kolkata')::date = v_day;

  select count(*)::int into v_feeders_total
  from public.feeders fd
  where fd.feed_id = p_feed_id and fd.status='active';

  -- E1 format takeover (macro)
  if coalesce(v_total_hot,0) > 0 then
    perform public.fn_upsert_fire_alert(
      p_feed_id, null, null, p_checkpoint,
      'macro', 'E', 'E1_format_takeover',
      case
        when (v_reel_hot::numeric / v_total_hot::numeric) >= 0.9 then 90
        when (v_reel_hot::numeric / v_total_hot::numeric) >= 0.75 then 65
        when (v_reel_hot::numeric / v_total_hot::numeric) >= 0.6 then 42
        else 0
      end,
      'Format takeover',
      format('Reels hold %s%% of hot-zone posts in this feed at %s.', round((v_reel_hot::numeric / v_total_hot::numeric)*100)::int, upper(p_checkpoint)),
      v_day,
      jsonb_build_object('feed_id', p_feed_id, 'checkpoint', p_checkpoint, 'day', v_day,
                         'reel_hot', v_reel_hot, 'total_hot', v_total_hot)
    );
    v_count := v_count + 1;
  end if;

  -- E3 structural convergence
  if coalesce(v_feeders_hot,0) >= 3 then
    perform public.fn_upsert_fire_alert(
      p_feed_id, null, null, p_checkpoint,
      'macro', 'E', 'E3_structural_convergence',
      case
        when v_feeders_hot >= 7 then 90
        when v_feeders_hot >= 5 then 65
        else 42
      end,
      'Structural convergence',
      format('%s feeders entered hot zone in the same %s cycle.', v_feeders_hot, upper(p_checkpoint)),
      v_day,
      jsonb_build_object('feed_id', p_feed_id, 'checkpoint', p_checkpoint, 'day', v_day,
                         'feeders_hot', v_feeders_hot, 'feeders_total', v_feeders_total)
    );
    v_count := v_count + 1;
  end if;

  -- D1/D2/D3 competitive comparisons vs anchor
  if v_anchor_feeder_id is not null then
    for v_comp in
      select fd.id as feeder_id, fd.handle
      from public.feeders fd
      where fd.feed_id = p_feed_id
        and fd.status = 'active'
        and fd.id <> v_anchor_feeder_id
    loop
      select percentile_cont(0.5) within group(order by pm.percentile_performance)
      into v_anchor_median
      from public.post_metrics pm
      join public.posts p on p.post_key = pm.post_key
      where p.feeder_id = v_anchor_feeder_id
        and pm.checkpoint = p_checkpoint
        and pm.percentile_performance is not null
        and p.posted_at >= now() - interval '30 days';

      select percentile_cont(0.5) within group(order by pm.percentile_performance)
      into v_comp_median
      from public.post_metrics pm
      join public.posts p on p.post_key = pm.post_key
      where p.feeder_id = v_comp.feeder_id
        and pm.checkpoint = p_checkpoint
        and pm.percentile_performance is not null
        and p.posted_at >= now() - interval '30 days';

      if v_anchor_median is not null and v_comp_median is not null then
        v_gap := v_anchor_median - v_comp_median;

        -- D1 anchor displacement
        if v_gap >= 5 then
          perform public.fn_upsert_fire_alert(
            p_feed_id, v_comp.feeder_id, null, p_checkpoint,
            'competitor', 'D', 'D1_anchor_displacement',
            case when v_gap >= 20 then 90 when v_gap >= 12 then 65 else 42 end,
            'Anchor displacement',
            format('@%s median is %s points ahead of @%s at %s.', v_comp.handle, round(v_gap)::int, v_anchor_handle, upper(p_checkpoint)),
            v_day,
            jsonb_build_object('anchor_handle', v_anchor_handle, 'competitor_handle', v_comp.handle,
                               'anchor_median', round(v_anchor_median)::int, 'competitor_median', round(v_comp_median)::int,
                               'gap', round(v_gap)::int)
          );
          v_count := v_count + 1;
        end if;

        -- D2 gap closing (simple parity gate)
        if abs(v_gap) <= 15 then
          perform public.fn_upsert_fire_alert(
            p_feed_id, v_comp.feeder_id, null, p_checkpoint,
            'competitor', 'D', 'D2_competitive_gap_closing',
            case when abs(v_gap) <= 2 then 90 when abs(v_gap) <= 8 then 65 else 42 end,
            'Gap closing',
            format('@%s is now within %s positions of @%s at %s.', v_comp.handle, abs(round(v_gap)::int), v_anchor_handle, upper(p_checkpoint)),
            v_day,
            jsonb_build_object('anchor_handle', v_anchor_handle, 'competitor_handle', v_comp.handle,
                               'gap', round(v_gap)::int)
          );
          v_count := v_count + 1;
        end if;

        -- D3 regime break (competitor median beats own 90d best threshold)
        if v_comp_median <= 20 then
          perform public.fn_upsert_fire_alert(
            p_feed_id, v_comp.feeder_id, null, p_checkpoint,
            'competitor', 'D', 'D3_competitor_regime_break',
            case when v_comp_median <= 8 then 90 when v_comp_median <= 14 then 65 else 42 end,
            'Competitor regime break',
            format('@%s is operating in a new structural tier at %s.', v_comp.handle, upper(p_checkpoint)),
            v_day,
            jsonb_build_object('competitor_handle', v_comp.handle, 'competitor_median', round(v_comp_median)::int,
                               'checkpoint', p_checkpoint)
          );
          v_count := v_count + 1;
        end if;
      end if;
    end loop;
  end if;

  return v_count;
end;
$$;

create or replace function public.fn_try_resolve_feed_signals(
  p_feed_id bigint,
  p_checkpoint text,
  p_business_date_ist date default null
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_day date := coalesce(p_business_date_ist, (now() at time zone 'Asia/Kolkata')::date);
  v_lock_key bigint;
  v_pending int := 0;
  v_done int := 0;
  v_key text;
begin
  v_lock_key := ('x' || substr(md5(format('%s:%s:%s', p_feed_id, p_checkpoint, v_day)), 1, 16))::bit(64)::bigint;
  perform pg_advisory_xact_lock(v_lock_key);

  if p_checkpoint = 'd1' then
    select count(*)::int
    into v_pending
    from public.run_jobs rj
    join public.feeders fd on fd.id = rj.feeder_id
    where fd.feed_id = p_feed_id
      and rj.business_date_ist = v_day
      and rj.job_type in ('daily','repair')
      and rj.status in ('pending','running','retry');
  else
    select count(*)::int
    into v_pending
    from public.checkpoint_jobs cj
    join public.posts p on p.post_key = cj.post_key
    join public.feeders fd on fd.id = p.feeder_id
    where fd.feed_id = p_feed_id
      and cj.checkpoint = p_checkpoint
      and cj.status in ('pending','running','retry');
  end if;

  if v_pending > 0 then
    return 0;
  end if;

  v_key := format('resolver:feed:%s:%s:%s', p_feed_id, p_checkpoint, v_day);
  if exists(select 1 from public.engine_state es where es.key = v_key) then
    return 0;
  end if;

  v_done := public.fn_resolve_feed_signals(p_feed_id, p_checkpoint, v_day);

  insert into public.engine_state(key, value_json, updated_at)
  values (v_key, jsonb_build_object('count', v_done, 'feed_id', p_feed_id, 'checkpoint', p_checkpoint, 'business_date_ist', v_day), now())
  on conflict (key)
  do update set value_json = excluded.value_json, updated_at = now();

  return v_done;
end;
$$;

commit;
