begin;

-- ============================================================
-- Fire alerts contract v2 (slot card + 6 layers) + dedupe hardening
-- Additive only: no destructive drops
-- ============================================================

-- 1) Expand post_metrics contract (future-proof, columnized compute)
alter table public.post_metrics
  add column if not exists interactions bigint,
  add column if not exists business_date_ist date,
  add column if not exists post_hour_ist smallint,
  add column if not exists post_dow_ist smallint,
  add column if not exists likes_percentile int,
  add column if not exists comments_percentile int,
  add column if not exists views_percentile int,
  add column if not exists interactions_percentile int,
  add column if not exists rank_overall int,
  add column if not exists rank_feed int,
  add column if not exists rank_recent_20 int,
  add column if not exists rank_recent_50 int,
  add column if not exists rank_all_time int,
  add column if not exists pool_recent_20 int,
  add column if not exists pool_recent_50 int,
  add column if not exists pool_all_time int,
  add column if not exists baseline_views numeric,
  add column if not exists baseline_likes numeric,
  add column if not exists baseline_comments numeric,
  add column if not exists baseline_interactions numeric,
  add column if not exists views_multiple numeric,
  add column if not exists likes_multiple numeric,
  add column if not exists comments_multiple numeric,
  add column if not exists interactions_multiple numeric,
  add column if not exists views_delta_vs_baseline bigint,
  add column if not exists likes_delta_vs_baseline bigint,
  add column if not exists comments_delta_vs_baseline bigint,
  add column if not exists interactions_delta_vs_baseline bigint,
  add column if not exists percentile_delta_vs_d1 int,
  add column if not exists rank_delta_vs_d1 int,
  add column if not exists d1_percentile int,
  add column if not exists d3_percentile int,
  add column if not exists d7_percentile int,
  add column if not exists d21_percentile int,
  add column if not exists delta_d1_d3 int,
  add column if not exists delta_d3_d7 int,
  add column if not exists delta_d7_d21 int,
  add column if not exists trajectory_state text,
  add column if not exists best_in_days int,
  add column if not exists best_in_posts int,
  add column if not exists outperformed_last_20 int,
  add column if not exists outperformed_last_50 int,
  add column if not exists days_since_last_better int,
  add column if not exists hour_baseline_views numeric,
  add column if not exists hour_baseline_likes numeric,
  add column if not exists hour_baseline_comments numeric,
  add column if not exists hour_multiple_views numeric,
  add column if not exists hour_multiple_likes numeric,
  add column if not exists hour_multiple_comments numeric,
  add column if not exists hour_percentile_feeder int,
  add column if not exists is_best_hour_window boolean;

-- Keep core derived raw fields populated for new and existing rows.
update public.post_metrics
set interactions = coalesce(likes, 0) + coalesce(comments, 0)
where interactions is null;

update public.post_metrics pm
set business_date_ist = coalesce(
      pm.business_date_ist,
      (p.posted_at at time zone 'Asia/Kolkata')::date
    ),
    post_hour_ist = coalesce(pm.post_hour_ist, extract(hour from (p.posted_at at time zone 'Asia/Kolkata'))::smallint),
    post_dow_ist = coalesce(pm.post_dow_ist, extract(dow from (p.posted_at at time zone 'Asia/Kolkata'))::smallint)
from public.posts p
where p.post_key = pm.post_key
  and (
    pm.business_date_ist is null
    or pm.post_hour_ist is null
    or pm.post_dow_ist is null
  );

-- 2) Hour baselines table (timing layer)
create table if not exists public.feeder_hour_baselines (
  feeder_id bigint not null references public.feeders(id) on delete cascade,
  media_type text not null,
  checkpoint text not null,
  hour_ist smallint not null check (hour_ist between 0 and 23),
  median_views bigint,
  median_likes bigint,
  median_comments bigint,
  sample_size int not null default 0,
  updated_at timestamptz not null default now(),
  primary key (feeder_id, media_type, checkpoint, hour_ist)
);

create index if not exists feeder_hour_baselines_checkpoint_idx
  on public.feeder_hour_baselines(checkpoint, feeder_id, hour_ist);

-- 3) Fire alert render-contract columns
alter table public.fire_alerts
  add column if not exists surface_percentile int,
  add column if not exists surface_shift int,
  add column if not exists surface_feeder text,
  add column if not exists surface_media_type text,
  add column if not exists surface_checkpoint text,
  add column if not exists surface_metric_label text,
  add column if not exists surface_metric_value numeric,
  add column if not exists surface_stamp text,
  add column if not exists intelligence_layers jsonb not null default '{}'::jsonb;

create index if not exists fire_alerts_surface_feed_idx
  on public.fire_alerts(feed_id, created_at desc, surface_percentile);

create index if not exists fire_alerts_surface_feeder_idx
  on public.fire_alerts(feeder_id, created_at desc, surface_percentile);

-- 4) Build canonical 6-layer payload object for each alert row
create or replace function public.fn_build_fire_layers(
  p_post_key text,
  p_checkpoint text,
  p_feeder_id bigint,
  p_feed_id bigint,
  p_payload jsonb
)
returns jsonb
language plpgsql
stable
set search_path = public
as $$
declare
  v_pm record;
  v_fb record;
  v_pct int;
  v_feed_pct int;
  v_shift int;
  v_mv numeric;
  v_layers jsonb := '{}'::jsonb;
begin
  if p_post_key is not null and p_checkpoint is not null then
    select
      pm.percentile_performance,
      pm.feed_percentile,
      pm.percentile_delta,
      pm.metric_value,
      pm.views,
      pm.likes,
      pm.comments,
      pm.interactions,
      pm.likes_percentile,
      pm.comments_percentile,
      pm.views_percentile,
      pm.interactions_percentile,
      pm.rank_overall,
      pm.rank_feed,
      pm.rank_recent_20,
      pm.rank_recent_50,
      pm.rank_all_time,
      pm.pool_recent_20,
      pm.pool_recent_50,
      pm.pool_all_time,
      pm.baseline_views,
      pm.baseline_likes,
      pm.baseline_comments,
      pm.baseline_interactions,
      pm.views_multiple,
      pm.likes_multiple,
      pm.comments_multiple,
      pm.interactions_multiple,
      pm.views_delta_vs_baseline,
      pm.likes_delta_vs_baseline,
      pm.comments_delta_vs_baseline,
      pm.interactions_delta_vs_baseline,
      pm.d1_percentile,
      pm.d3_percentile,
      pm.d7_percentile,
      pm.d21_percentile,
      pm.delta_d1_d3,
      pm.delta_d3_d7,
      pm.delta_d7_d21,
      pm.trajectory_state,
      pm.best_in_days,
      pm.best_in_posts,
      pm.outperformed_last_20,
      pm.outperformed_last_50,
      pm.days_since_last_better,
      pm.post_hour_ist,
      pm.hour_baseline_views,
      pm.hour_baseline_likes,
      pm.hour_baseline_comments,
      pm.hour_multiple_views,
      pm.hour_multiple_likes,
      pm.hour_multiple_comments,
      pm.hour_percentile_feeder,
      pm.is_best_hour_window
    into v_pm
    from public.post_metrics pm
    where pm.post_key = p_post_key
      and pm.checkpoint = lower(p_checkpoint)
    limit 1;
  end if;

  if p_feeder_id is not null and p_checkpoint is not null then
    select
      fb.median_percentile,
      fb.median_views,
      fb.median_likes,
      fb.median_comments,
      fb.post_count_90d,
      fb.days_since_ceiling,
      fb.best_percentile_90d
    into v_fb
    from public.feeder_baselines fb
    where fb.feeder_id = p_feeder_id
      and fb.checkpoint = lower(p_checkpoint)
    order by fb.updated_at desc
    limit 1;
  end if;

  v_pct := coalesce(
    (p_payload->>'percentile')::int,
    (p_payload->>'percentile_performance')::int,
    v_pm.percentile_performance
  );
  v_feed_pct := coalesce((p_payload->>'feed_percentile')::int, v_pm.feed_percentile);
  v_shift := coalesce((p_payload->>'delta')::int, (p_payload->>'percentile_delta')::int, v_pm.percentile_delta);
  v_mv := coalesce((p_payload->>'metric_value')::numeric, v_pm.metric_value);

  v_layers := jsonb_build_object(
    'layer_1_position', jsonb_build_object(
      'percentile_feed', v_pct,
      'percentile_feed_scope', v_feed_pct,
      'rank_overall', v_pm.rank_overall,
      'rank_feed', v_pm.rank_feed,
      'rank_recent_20', coalesce(v_pm.rank_recent_20, (p_payload->>'rank_in_recent_20')::int),
      'rank_recent_50', v_pm.rank_recent_50,
      'rank_all_time', coalesce(v_pm.rank_all_time, (p_payload->>'all_time_rank')::int),
      'shift_since_d1', v_shift
    ),
    'layer_2_distribution', jsonb_build_object(
      'metric_value', v_mv,
      'baseline_views', coalesce(v_pm.baseline_views, v_fb.median_views),
      'baseline_likes', coalesce(v_pm.baseline_likes, v_fb.median_likes),
      'baseline_comments', coalesce(v_pm.baseline_comments, v_fb.median_comments),
      'baseline_interactions', coalesce(v_pm.baseline_interactions, null),
      'views_multiple', v_pm.views_multiple,
      'likes_multiple', v_pm.likes_multiple,
      'comments_multiple', v_pm.comments_multiple,
      'interactions_multiple', v_pm.interactions_multiple,
      'views_delta_vs_baseline', v_pm.views_delta_vs_baseline,
      'likes_delta_vs_baseline', v_pm.likes_delta_vs_baseline,
      'comments_delta_vs_baseline', v_pm.comments_delta_vs_baseline,
      'interactions_delta_vs_baseline', v_pm.interactions_delta_vs_baseline
    ),
    'layer_3_response', jsonb_build_object(
      'views', coalesce((p_payload->>'views')::bigint, v_pm.views),
      'likes', coalesce((p_payload->>'likes')::bigint, v_pm.likes),
      'comments', coalesce((p_payload->>'comments')::bigint, v_pm.comments),
      'interactions', v_pm.interactions,
      'views_percentile', v_pm.views_percentile,
      'likes_percentile', v_pm.likes_percentile,
      'comments_percentile', v_pm.comments_percentile,
      'interactions_percentile', v_pm.interactions_percentile
    ),
    'layer_4_trajectory', jsonb_build_object(
      'd1_percentile', coalesce(v_pm.d1_percentile, (p_payload->>'d1_pct')::int),
      'd3_percentile', coalesce(v_pm.d3_percentile, (p_payload->>'d3_pct')::int),
      'd7_percentile', coalesce(v_pm.d7_percentile, (p_payload->>'d7_pct')::int),
      'd21_percentile', coalesce(v_pm.d21_percentile, (p_payload->>'d21_pct')::int),
      'delta_d1_d3', v_pm.delta_d1_d3,
      'delta_d3_d7', v_pm.delta_d3_d7,
      'delta_d7_d21', v_pm.delta_d7_d21,
      'trajectory_state', v_pm.trajectory_state,
      'total_shift', v_shift
    ),
    'layer_5_structural', jsonb_build_object(
      'best_in_days', coalesce(v_pm.best_in_days, v_fb.days_since_ceiling),
      'best_in_posts', v_pm.best_in_posts,
      'outperformed_last_20', v_pm.outperformed_last_20,
      'outperformed_last_50', v_pm.outperformed_last_50,
      'days_since_last_better', coalesce(v_pm.days_since_last_better, v_fb.days_since_ceiling),
      'baseline_post_count_90d', v_fb.post_count_90d,
      'best_percentile_90d', v_fb.best_percentile_90d
    ),
    'layer_6_timing', jsonb_build_object(
      'post_hour_ist', v_pm.post_hour_ist,
      'hour_baseline_views', v_pm.hour_baseline_views,
      'hour_baseline_likes', v_pm.hour_baseline_likes,
      'hour_baseline_comments', v_pm.hour_baseline_comments,
      'hour_multiple_views', v_pm.hour_multiple_views,
      'hour_multiple_likes', v_pm.hour_multiple_likes,
      'hour_multiple_comments', v_pm.hour_multiple_comments,
      'hour_percentile_feeder', v_pm.hour_percentile_feeder,
      'is_best_hour_window', v_pm.is_best_hour_window
    )
  );

  return v_layers;
end;
$$;

-- 5) Normalize every alert row into slot-card render fields and 6-layer payload
create or replace function public.fn_apply_fire_alert_contract()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_handle text;
  v_media text;
  v_metric_label text;
  v_metric_value numeric;
  v_pct int;
  v_shift int;
  v_layers jsonb;
begin
  if new.feeder_id is not null and (new.surface_feeder is null or new.surface_feeder = '') then
    select fd.handle into v_handle from public.feeders fd where fd.id = new.feeder_id;
  else
    v_handle := new.surface_feeder;
  end if;

  if new.post_key is not null and (new.surface_media_type is null or new.surface_media_type = '') then
    select p.media_type into v_media from public.posts p where p.post_key = new.post_key;
  else
    v_media := new.surface_media_type;
  end if;

  v_pct := coalesce(
    new.surface_percentile,
    (new.payload->>'percentile')::int,
    (new.payload->>'percentile_performance')::int
  );
  v_shift := coalesce(new.surface_shift, (new.payload->>'delta')::int, (new.payload->>'percentile_delta')::int);

  v_metric_value := coalesce(
    new.surface_metric_value,
    (new.payload->>'metric_value')::numeric,
    case when lower(coalesce(v_media, '')) = 'reel' then (new.payload->>'views')::numeric else (new.payload->>'likes')::numeric end
  );

  v_metric_label := coalesce(
    new.surface_metric_label,
    case when lower(coalesce(v_media, '')) = 'reel' then 'views' else 'likes' end
  );

  v_layers := public.fn_build_fire_layers(new.post_key, new.checkpoint, new.feeder_id, new.feed_id, coalesce(new.payload, '{}'::jsonb));

  new.surface_percentile := v_pct;
  new.surface_shift := v_shift;
  new.surface_feeder := coalesce(new.surface_feeder, v_handle);
  new.surface_media_type := coalesce(new.surface_media_type, v_media);
  new.surface_checkpoint := coalesce(new.surface_checkpoint, upper(new.checkpoint));
  new.surface_metric_label := v_metric_label;
  new.surface_metric_value := v_metric_value;
  new.surface_stamp := coalesce(
    new.surface_stamp,
    format(
      '@%s · %s · %s · %s %s',
      upper(coalesce(v_handle, 'feeder')),
      upper(coalesce(v_media, 'post')),
      upper(coalesce(new.checkpoint, 'd1')),
      case when v_metric_value is null then '?' else public.fn_format_number_k(v_metric_value) end,
      upper(coalesce(v_metric_label, 'metric'))
    )
  );

  new.dedupe_key := format(
    '%s:%s:%s:%s:%s:%s:%s',
    coalesce(nullif(new.signal_code, ''), 'signal'),
    coalesce(nullif(new.context, ''), 'own'),
    coalesce(new.feed_id::text, 'feed'),
    coalesce(new.feeder_id::text, 'all'),
    coalesce(nullif(new.post_key, ''), 'feed'),
    coalesce(nullif(new.checkpoint, ''), 'na'),
    coalesce(new.business_date_ist::text, (new.created_at at time zone 'Asia/Kolkata')::date::text, (now() at time zone 'Asia/Kolkata')::date::text)
  );

  new.intelligence_layers := coalesce(v_layers, '{}'::jsonb);
  new.payload := coalesce(new.payload, '{}'::jsonb)
    || jsonb_build_object(
      'slot_state', jsonb_build_object(
        'percentile_feed', new.surface_percentile,
        'shift', new.surface_shift,
        'stamp', new.surface_stamp
      ),
      'stamps', jsonb_build_object(
        'feeder', new.surface_feeder,
        'media_type', new.surface_media_type,
        'checkpoint', new.surface_checkpoint,
        'metric_label', new.surface_metric_label,
        'metric_value', new.surface_metric_value
      ),
      'layers', new.intelligence_layers,
      'contract_version', 'v2_slot_layers'
    );

  return new;
end;
$$;

drop trigger if exists trg_apply_fire_alert_contract on public.fire_alerts;
create trigger trg_apply_fire_alert_contract
before insert or update of payload, post_key, checkpoint, feeder_id, feed_id,
  surface_percentile, surface_shift, surface_feeder, surface_media_type, surface_checkpoint,
  surface_metric_label, surface_metric_value, surface_stamp
on public.fire_alerts
for each row
execute function public.fn_apply_fire_alert_contract();

-- 6) Always generate a canonical slot-state alert for every feeder checkpoint row
create or replace function public.enqueue_slot_state_alerts(
  p_feeder_id bigint,
  p_checkpoint text,
  p_business_date_ist date default null
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  r record;
  v_count int := 0;
  v_cp text := lower(coalesce(p_checkpoint, 'd1'));
  v_day date := coalesce(p_business_date_ist, (now() at time zone 'Asia/Kolkata')::date);
begin
  if v_cp not in ('d1','d3','d7','d21') then
    return 0;
  end if;

  for r in
    select
      pm.post_key,
      pm.checkpoint,
      pm.percentile_performance,
      pm.feed_percentile,
      pm.percentile_delta,
      pm.metric_value,
      pm.views,
      pm.likes,
      pm.comments,
      pm.d1_percentile,
      pm.d3_percentile,
      pm.d7_percentile,
      pm.d21_percentile,
      p.media_type,
      p.posted_at,
      p.post_url,
      p.thumbnail_url,
      fd.feed_id,
      fd.handle
    from public.post_metrics pm
    join public.posts p on p.post_key = pm.post_key
    join public.feeders fd on fd.id = p.feeder_id
    where fd.id = p_feeder_id
      and pm.checkpoint = v_cp
      and pm.percentile_performance is not null
  loop
    perform public.fn_upsert_fire_alert(
      r.feed_id,
      p_feeder_id,
      r.post_key,
      v_cp,
      'own',
      'A',
      'S0_slot_state',
      greatest(1, least(100, 100 - coalesce(r.percentile_performance, 50))),
      format('Top %s%%', coalesce(r.percentile_performance, 0)),
      'Slot state',
      coalesce(p_business_date_ist, (r.posted_at at time zone 'Asia/Kolkata')::date, v_day),
      jsonb_build_object(
        'feeder', r.handle,
        'media_type', r.media_type,
        'checkpoint', r.checkpoint,
        'post_url', r.post_url,
        'thumbnail_url', r.thumbnail_url,
        'percentile', r.percentile_performance,
        'feed_percentile', r.feed_percentile,
        'delta', r.percentile_delta,
        'views', r.views,
        'likes', r.likes,
        'comments', r.comments,
        'metric_value', r.metric_value,
        'd1_pct', r.d1_percentile,
        'd3_pct', r.d3_percentile,
        'd7_pct', r.d7_percentile,
        'd21_pct', r.d21_percentile
      )
    );
    v_count := v_count + 1;
  end loop;

  return v_count;
end;
$$;

-- 7) Dedupe hardening + cleanup
update public.fire_alerts fa
set dedupe_key = format(
  '%s:%s:%s:%s:%s:%s:%s',
  coalesce(nullif(fa.signal_code, ''), 'signal'),
  coalesce(nullif(fa.context, ''), 'own'),
  coalesce(fa.feed_id::text, 'feed'),
  coalesce(fa.feeder_id::text, 'all'),
  coalesce(nullif(fa.post_key, ''), 'feed'),
  coalesce(nullif(fa.checkpoint, ''), 'na'),
  coalesce(fa.business_date_ist::text, (fa.created_at at time zone 'Asia/Kolkata')::date::text)
)
;

with ranked as (
  select
    id,
    row_number() over (
      partition by dedupe_key
      order by coalesce(intensity_score, 0) desc, coalesce(priority_score, 0) desc, coalesce(updated_at, created_at) desc, id desc
    ) as rn
  from public.fire_alerts
  where dedupe_key is not null
)
delete from public.fire_alerts fa
using ranked r
where fa.id = r.id
  and r.rn > 1;

create unique index if not exists fire_alerts_dedupe_key_uniq
  on public.fire_alerts(dedupe_key);

create or replace function public.fire_alerts_dedupe_health()
returns table(
  total_rows bigint,
  null_or_empty_dedupe bigint,
  duplicate_dedupe_groups bigint,
  duplicate_rows bigint
)
language sql
stable
set search_path = public
as $$
  with d as (
    select dedupe_key, count(*) as c
    from public.fire_alerts
    where dedupe_key is not null and btrim(dedupe_key) <> ''
    group by dedupe_key
  )
  select
    (select count(*) from public.fire_alerts) as total_rows,
    (select count(*) from public.fire_alerts where dedupe_key is null or btrim(dedupe_key) = '') as null_or_empty_dedupe,
    (select count(*) from d where c > 1) as duplicate_dedupe_groups,
    (select coalesce(sum(c - 1), 0) from d where c > 1) as duplicate_rows;
$$;

-- Backfill contract fields for existing active alerts (trigger will populate columns/payload).
update public.fire_alerts
set updated_at = now()
where status in ('new', 'sent');

commit;
