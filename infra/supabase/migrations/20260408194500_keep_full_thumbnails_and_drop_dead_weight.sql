begin;

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
        then interval '90 days'
      else interval '1 day'
    end;
$$;

update public.post_media_assets assets
set purge_after = public.fn_post_media_rollover_deadline(
      coalesce(p.posted_at, assets.captured_at, assets.created_at, now()),
      assets.asset_role
    ),
    updated_at = now()
from public.posts p
where p.post_key = assets.post_key
  and lower(coalesce(assets.asset_role, '')) = 'thumbnail'
  and assets.status in ('pending_capture', 'capturing', 'active', 'capture_failed', 'purge_pending', 'purging', 'purge_failed');

alter table if exists public.users
  drop column if exists total_runs,
  drop column if exists data_points,
  drop column if exists success_rate,
  drop column if exists avatar_url,
  drop column if exists twitter_posts_caught,
  drop column if exists reddit_posts_caught;

alter table if exists public.feeders
  drop column if exists graph_connection_id,
  drop column if exists ig_target_user_id,
  drop column if exists last_polled_at,
  drop column if exists next_poll_at,
  drop column if exists last_seen_media_ts,
  drop column if exists last_poll_error,
  drop column if exists poll_window_hour,
  drop column if exists poll_window_minute,
  drop column if exists tracking_status,
  drop column if exists source_type;

alter table if exists public.posts
  drop column if exists discovery_source,
  drop column if exists first_seen_at,
  drop column if exists last_seen_at,
  drop column if exists ig_media_id;

alter table if exists public.post_metrics
  drop column if exists interactions,
  drop column if exists interactions_percentile,
  drop column if exists rank_recent_20,
  drop column if exists pool_recent_20,
  drop column if exists pool_recent_50,
  drop column if exists pool_all_time,
  drop column if exists baseline_interactions,
  drop column if exists interactions_multiple,
  drop column if exists views_delta_vs_baseline,
  drop column if exists likes_delta_vs_baseline,
  drop column if exists comments_delta_vs_baseline,
  drop column if exists interactions_delta_vs_baseline,
  drop column if exists rank_delta_vs_d1,
  drop column if exists d1_percentile,
  drop column if exists d3_percentile,
  drop column if exists d7_percentile,
  drop column if exists d21_percentile,
  drop column if exists delta_d1_d3,
  drop column if exists delta_d3_d7,
  drop column if exists delta_d7_d21,
  drop column if exists trajectory_state,
  drop column if exists days_since_last_better;

commit;
