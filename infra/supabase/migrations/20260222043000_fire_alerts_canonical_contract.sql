begin;

-- Canonical fire_alerts contract (strict, frontend-ready, no on-the-fly compute)

alter table public.fire_alerts
  add column if not exists surface_delta smallint,
  add column if not exists surface_handle text,
  add column if not exists metric_value bigint,
  add column if not exists metric_key text,

  add column if not exists primary_signal_key text,
  add column if not exists primary_signal_value bigint,
  add column if not exists primary_signal_percentile smallint,
  add column if not exists primary_signal_rank_feed integer,
  add column if not exists primary_signal_rank_all_time integer,
  add column if not exists primary_signal_rank_recent_50 integer,
  add column if not exists primary_signal_shift_since_d1 smallint,

  add column if not exists distribution_metric_key text,
  add column if not exists distribution_metric_value bigint,
  add column if not exists distribution_baseline_value bigint,
  add column if not exists distribution_multiple numeric,
  add column if not exists distribution_percentile smallint,

  add column if not exists response_metric_key text,
  add column if not exists response_metric_value bigint,
  add column if not exists response_baseline_value bigint,
  add column if not exists response_multiple numeric,
  add column if not exists response_percentile smallint,

  add column if not exists trajectory_current_percentile smallint,
  add column if not exists trajectory_d1_percentile smallint,
  add column if not exists trajectory_d3_percentile smallint,
  add column if not exists trajectory_d7_percentile smallint,
  add column if not exists trajectory_delta_d1_d3 smallint,
  add column if not exists trajectory_delta_d3_d7 smallint,
  add column if not exists trajectory_total_shift smallint,
  add column if not exists trajectory_state text,

  add column if not exists structure_best_in_days integer,
  add column if not exists structure_best_in_posts integer,
  add column if not exists structure_outperformed_last_20 integer,
  add column if not exists structure_outperformed_last_50 integer,
  add column if not exists structure_rank_all_time integer,

  add column if not exists timing_post_hour smallint,
  add column if not exists timing_hour_baseline_value bigint,
  add column if not exists timing_hour_multiple numeric,
  add column if not exists timing_hour_percentile smallint,
  add column if not exists timing_is_peak_window boolean;

create or replace function public.fn_safe_div_num(p_num numeric, p_den numeric, p_default numeric default 0)
returns numeric
language sql
immutable
as $$
  select case when coalesce(p_den, 0) = 0 then p_default else p_num / p_den end;
$$;

create or replace function public.fn_apply_fire_alert_contract()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_pm record;
  v_fb record;
  v_fhb record;
  v_handle text;
  v_media text;
  v_cp text;
  v_surface_pct int;
  v_surface_delta int;
  v_metric_key text;
  v_metric_value bigint;

  v_dist_base bigint;
  v_dist_pct int;

  v_resp_key text;
  v_resp_value bigint;
  v_resp_base bigint;
  v_resp_pct int;

  v_curr int;
  v_d1 int;
  v_d3 int;
  v_d7 int;
  v_total int;

  v_post_count int;
  v_layers jsonb;
begin
  v_cp := lower(coalesce(new.checkpoint, 'd1'));

  select fd.handle into v_handle
  from public.feeders fd
  where fd.id = new.feeder_id;

  select p.media_type into v_media
  from public.posts p
  where p.post_key = new.post_key;

  select
    pm.percentile_performance,
    pm.feed_percentile,
    pm.percentile_delta_vs_d1,
    pm.metric_value,
    pm.views,
    pm.likes,
    pm.comments,
    pm.interactions,
    pm.rank_feed,
    pm.rank_all_time,
    pm.rank_recent_50,
    pm.views_percentile,
    pm.likes_percentile,
    pm.comments_percentile,
    pm.interactions_percentile,
    pm.baseline_views,
    pm.baseline_likes,
    pm.baseline_comments,
    pm.baseline_interactions,
    pm.views_multiple,
    pm.likes_multiple,
    pm.comments_multiple,
    pm.interactions_multiple,
    pm.d1_percentile,
    pm.d3_percentile,
    pm.d7_percentile,
    pm.delta_d1_d3,
    pm.delta_d3_d7,
    pm.best_in_days,
    pm.best_in_posts,
    pm.outperformed_last_20,
    pm.outperformed_last_50,
    pm.post_hour_ist,
    pm.hour_multiple_views,
    pm.hour_multiple_likes,
    pm.hour_multiple_comments,
    pm.hour_percentile_feeder,
    pm.is_best_hour_window
  into v_pm
  from public.post_metrics pm
  where pm.post_key = new.post_key
    and pm.checkpoint = v_cp
  limit 1;

  select
    fb.median_views,
    fb.median_likes,
    fb.median_comments,
    fb.post_count_90d
  into v_fb
  from public.feeder_baselines fb
  where fb.feeder_id = new.feeder_id
    and fb.checkpoint = v_cp
  order by fb.updated_at desc
  limit 1;

  -- Slot state canonical
  v_surface_pct := coalesce(new.surface_percentile, v_pm.percentile_performance, 50);

  if v_cp = 'd1' then
    v_surface_delta := null;
  else
    v_surface_delta := coalesce(new.surface_delta, new.surface_shift, v_pm.percentile_delta_vs_d1, 0);
  end if;

  v_metric_key := lower(coalesce(new.metric_key, new.surface_metric_label, case when lower(coalesce(v_media, '')) = 'reel' then 'views' else 'likes' end));

  v_metric_value := coalesce(
    new.metric_value,
    new.surface_metric_value::bigint,
    v_pm.metric_value::bigint,
    case
      when v_metric_key = 'views' then v_pm.views
      when v_metric_key = 'likes' then v_pm.likes
      when v_metric_key = 'comments' then v_pm.comments
      else v_pm.interactions
    end,
    0
  );

  new.surface_percentile := v_surface_pct;
  new.surface_delta := v_surface_delta;
  new.surface_shift := v_surface_delta;
  new.surface_handle := coalesce(new.surface_handle, new.surface_feeder, v_handle, 'feed');
  new.surface_feeder := coalesce(new.surface_feeder, new.surface_handle, v_handle, 'feed');
  new.surface_media_type := coalesce(new.surface_media_type, v_media, 'post');
  new.surface_checkpoint := upper(coalesce(new.surface_checkpoint, new.checkpoint, 'd1'));
  new.metric_key := v_metric_key;
  new.metric_value := v_metric_value;

  new.surface_metric_label := coalesce(new.surface_metric_label, v_metric_key);
  new.surface_metric_value := coalesce(new.surface_metric_value, v_metric_value);

  new.surface_stamp := format(
    '@%s · %s · %s · %s',
    upper(coalesce(new.surface_handle, 'feed')),
    upper(coalesce(new.surface_media_type, 'post')),
    upper(coalesce(new.surface_checkpoint, 'd1')),
    public.fn_format_number_k(coalesce(new.metric_value, 0))
  );

  -- Layer 1: position
  new.primary_signal_key := coalesce(v_metric_key, 'metric');
  new.primary_signal_value := coalesce(v_metric_value, 0);
  new.primary_signal_percentile := coalesce(v_surface_pct, 50);
  new.primary_signal_rank_feed := coalesce(v_pm.rank_feed, 1);
  new.primary_signal_rank_all_time := coalesce(v_pm.rank_all_time, 1);
  new.primary_signal_rank_recent_50 := coalesce(v_pm.rank_recent_50, 1);
  new.primary_signal_shift_since_d1 := v_surface_delta;

  -- Layer 2: distribution
  if v_metric_key = 'views' then
    v_dist_base := coalesce(v_pm.baseline_views::bigint, v_fb.median_views, 1);
    v_dist_pct := coalesce(v_pm.views_percentile, v_surface_pct, 50);
  elsif v_metric_key = 'comments' then
    v_dist_base := coalesce(v_pm.baseline_comments::bigint, v_fb.median_comments, 1);
    v_dist_pct := coalesce(v_pm.comments_percentile, v_surface_pct, 50);
  elsif v_metric_key = 'interactions' then
    v_dist_base := coalesce(v_pm.baseline_interactions::bigint, (coalesce(v_fb.median_likes,0) + coalesce(v_fb.median_comments,0))::bigint, 1);
    v_dist_pct := coalesce(v_pm.interactions_percentile, v_surface_pct, 50);
  else
    v_dist_base := coalesce(v_pm.baseline_likes::bigint, v_fb.median_likes, 1);
    v_dist_pct := coalesce(v_pm.likes_percentile, v_surface_pct, 50);
  end if;

  new.distribution_metric_key := v_metric_key;
  new.distribution_metric_value := coalesce(v_metric_value, 0);
  new.distribution_baseline_value := greatest(coalesce(v_dist_base, 1), 1);
  new.distribution_multiple := coalesce(public.fn_safe_div_num(new.distribution_metric_value, new.distribution_baseline_value, 0), 0);
  new.distribution_percentile := coalesce(v_dist_pct, 50);

  -- Layer 3: response
  if lower(coalesce(new.surface_media_type, '')) = 'reel' then
    v_resp_key := 'comments';
    v_resp_value := coalesce(v_pm.comments, 0);
    v_resp_base := greatest(coalesce(v_pm.baseline_comments::bigint, v_fb.median_comments, 1), 1);
    v_resp_pct := coalesce(v_pm.comments_percentile, v_surface_pct, 50);
  else
    v_resp_key := 'interactions';
    v_resp_value := coalesce(v_pm.interactions, coalesce(v_pm.likes,0) + coalesce(v_pm.comments,0), 0);
    v_resp_base := greatest(coalesce(v_pm.baseline_interactions::bigint, (coalesce(v_fb.median_likes,0) + coalesce(v_fb.median_comments,0))::bigint, 1), 1);
    v_resp_pct := coalesce(v_pm.interactions_percentile, v_surface_pct, 50);
  end if;

  new.response_metric_key := v_resp_key;
  new.response_metric_value := v_resp_value;
  new.response_baseline_value := v_resp_base;
  new.response_multiple := coalesce(public.fn_safe_div_num(new.response_metric_value, new.response_baseline_value, 0), 0);
  new.response_percentile := v_resp_pct;

  -- Layer 4: trajectory
  v_curr := coalesce(v_pm.percentile_performance, v_surface_pct, 50);
  v_d1 := coalesce(v_pm.d1_percentile, v_curr);
  v_d3 := coalesce(v_pm.d3_percentile, v_curr);
  v_d7 := coalesce(v_pm.d7_percentile, v_curr);

  v_total := coalesce(v_pm.percentile_delta_vs_d1, v_curr - v_d1, 0);

  new.trajectory_current_percentile := v_curr;
  new.trajectory_d1_percentile := v_d1;
  new.trajectory_d3_percentile := v_d3;
  new.trajectory_d7_percentile := v_d7;
  new.trajectory_delta_d1_d3 := coalesce(v_pm.delta_d1_d3, v_d3 - v_d1, 0);
  new.trajectory_delta_d3_d7 := coalesce(v_pm.delta_d3_d7, v_d7 - v_d3, 0);
  new.trajectory_total_shift := v_total;
  new.trajectory_state := case
    when v_total <= -20 then 'accelerating'
    when v_total >= 20 then 'fading'
    else 'stable'
  end;

  -- Layer 5: structural
  v_post_count := greatest(coalesce(v_fb.post_count_90d, 1), 1);

  new.structure_best_in_days := coalesce(v_pm.best_in_days, 0);
  new.structure_best_in_posts := coalesce(v_pm.best_in_posts, 0);
  new.structure_outperformed_last_20 := coalesce(v_pm.outperformed_last_20, least(v_post_count, 20));
  new.structure_outperformed_last_50 := coalesce(v_pm.outperformed_last_50, least(v_post_count, 50));
  new.structure_rank_all_time := coalesce(v_pm.rank_all_time, 1);

  -- Layer 6: timing
  new.timing_post_hour := coalesce(v_pm.post_hour_ist, 0);

  select
    fhb.median_views,
    fhb.median_likes,
    fhb.median_comments
  into v_fhb
  from public.feeder_hour_baselines fhb
  where fhb.feeder_id = new.feeder_id
    and fhb.media_type = lower(coalesce(new.surface_media_type, 'post'))
    and fhb.checkpoint = v_cp
    and fhb.hour_ist = new.timing_post_hour
  limit 1;

  if v_metric_key = 'views' then
    new.timing_hour_baseline_value := greatest(coalesce(v_fhb.median_views, v_fb.median_views, 1), 1);
    new.timing_hour_multiple := coalesce(v_pm.hour_multiple_views, public.fn_safe_div_num(new.metric_value, new.timing_hour_baseline_value, 0), 0);
  elsif v_metric_key = 'comments' then
    new.timing_hour_baseline_value := greatest(coalesce(v_fhb.median_comments, v_fb.median_comments, 1), 1);
    new.timing_hour_multiple := coalesce(v_pm.hour_multiple_comments, public.fn_safe_div_num(new.metric_value, new.timing_hour_baseline_value, 0), 0);
  else
    new.timing_hour_baseline_value := greatest(coalesce(v_fhb.median_likes, v_fb.median_likes, 1), 1);
    new.timing_hour_multiple := coalesce(v_pm.hour_multiple_likes, public.fn_safe_div_num(new.metric_value, new.timing_hour_baseline_value, 0), 0);
  end if;

  new.timing_hour_percentile := coalesce(v_pm.hour_percentile_feeder, v_surface_pct, 50);
  new.timing_is_peak_window := coalesce(v_pm.is_best_hour_window, false);

  v_layers := jsonb_build_object(
    'layer_1_position', jsonb_build_object(
      'primary_signal_key', new.primary_signal_key,
      'primary_signal_value', new.primary_signal_value,
      'primary_signal_percentile', new.primary_signal_percentile,
      'primary_signal_rank_feed', new.primary_signal_rank_feed,
      'primary_signal_rank_all_time', new.primary_signal_rank_all_time,
      'primary_signal_rank_recent_50', new.primary_signal_rank_recent_50,
      'primary_signal_shift_since_d1', new.primary_signal_shift_since_d1
    ),
    'layer_2_distribution', jsonb_build_object(
      'distribution_metric_key', new.distribution_metric_key,
      'distribution_metric_value', new.distribution_metric_value,
      'distribution_baseline_value', new.distribution_baseline_value,
      'distribution_multiple', new.distribution_multiple,
      'distribution_percentile', new.distribution_percentile
    ),
    'layer_3_response', jsonb_build_object(
      'response_metric_key', new.response_metric_key,
      'response_metric_value', new.response_metric_value,
      'response_baseline_value', new.response_baseline_value,
      'response_multiple', new.response_multiple,
      'response_percentile', new.response_percentile
    ),
    'layer_4_trajectory', jsonb_build_object(
      'trajectory_current_percentile', new.trajectory_current_percentile,
      'trajectory_d1_percentile', new.trajectory_d1_percentile,
      'trajectory_d3_percentile', new.trajectory_d3_percentile,
      'trajectory_d7_percentile', new.trajectory_d7_percentile,
      'trajectory_delta_d1_d3', new.trajectory_delta_d1_d3,
      'trajectory_delta_d3_d7', new.trajectory_delta_d3_d7,
      'trajectory_total_shift', new.trajectory_total_shift,
      'trajectory_state', new.trajectory_state
    ),
    'layer_5_structural', jsonb_build_object(
      'structure_best_in_days', new.structure_best_in_days,
      'structure_best_in_posts', new.structure_best_in_posts,
      'structure_outperformed_last_20', new.structure_outperformed_last_20,
      'structure_outperformed_last_50', new.structure_outperformed_last_50,
      'structure_rank_all_time', new.structure_rank_all_time
    ),
    'layer_6_timing', jsonb_build_object(
      'timing_post_hour', new.timing_post_hour,
      'timing_hour_baseline_value', new.timing_hour_baseline_value,
      'timing_hour_multiple', new.timing_hour_multiple,
      'timing_hour_percentile', new.timing_hour_percentile,
      'timing_is_peak_window', new.timing_is_peak_window
    )
  );

  new.intelligence_layers := v_layers;
  new.payload := coalesce(new.payload, '{}'::jsonb)
    || jsonb_build_object(
      'layers', v_layers,
      'contract_version', 'v3_canonical',
      'slot_state', jsonb_build_object(
        'surface_percentile', new.surface_percentile,
        'surface_delta', new.surface_delta,
        'surface_handle', new.surface_handle,
        'surface_media_type', new.surface_media_type,
        'checkpoint', new.surface_checkpoint,
        'metric_value', new.metric_value,
        'metric_key', new.metric_key,
        'stamp', new.surface_stamp
      )
    );

  return new;
end;
$$;

drop trigger if exists trg_apply_fire_alert_contract on public.fire_alerts;
create trigger trg_apply_fire_alert_contract
before insert or update of payload, post_key, checkpoint, feeder_id, feed_id,
  surface_percentile, surface_delta, surface_shift, surface_handle, surface_feeder,
  surface_media_type, surface_checkpoint, surface_metric_label, surface_metric_value,
  metric_key, metric_value, surface_stamp
on public.fire_alerts
for each row
execute function public.fn_apply_fire_alert_contract();

-- Backfill existing rows through resolver
update public.fire_alerts
set payload = coalesce(payload, '{}'::jsonb)
where status in ('new', 'sent');

commit;
