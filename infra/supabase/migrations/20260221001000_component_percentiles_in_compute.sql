-- Compute component percentiles (likes/comments/views) in the canonical metrics trigger.
-- Keeps a single compute path so alert copy payload always has populated component percentile fields.

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

  v_likes_pool_size int;
  v_likes_rank int;
  v_likes_pct int;

  v_comments_pool_size int;
  v_comments_rank int;
  v_comments_pct int;

  v_views_pool_size int;
  v_views_rank int;
  v_views_pct int;

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

  -- Primary metric percentile (existing contract)
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

  -- Feed percentile (existing contract)
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

  -- Component percentiles used by A4 and richer proof lines.
  v_likes_pct := null;
  if new.likes is not null and v_feeder_id is not null then
    select count(*) + 1
    into v_likes_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.post_key <> new.post_key
      and pm.likes is not null
      and pm.likes > new.likes;

    select count(*) + 1
    into v_likes_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.likes is not null;

    if v_likes_pool_size > 0 then
      v_likes_pct := greatest(1, least(100, round((v_likes_rank::numeric / v_likes_pool_size) * 100)));
    end if;
  end if;

  v_comments_pct := null;
  if new.comments is not null and v_feeder_id is not null then
    select count(*) + 1
    into v_comments_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.post_key <> new.post_key
      and pm.comments is not null
      and pm.comments > new.comments;

    select count(*) + 1
    into v_comments_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.comments is not null;

    if v_comments_pool_size > 0 then
      v_comments_pct := greatest(1, least(100, round((v_comments_rank::numeric / v_comments_pool_size) * 100)));
    end if;
  end if;

  v_views_pct := null;
  if new.views is not null and v_feeder_id is not null then
    select count(*) + 1
    into v_views_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.post_key <> new.post_key
      and pm.views is not null
      and pm.views > new.views;

    select count(*) + 1
    into v_views_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and coalesce(p2.media_type,'') = coalesce(v_media_type,'')
      and pm.checkpoint = new.checkpoint
      and pm.views is not null;

    if v_views_pool_size > 0 then
      v_views_pct := greatest(1, least(100, round((v_views_rank::numeric / v_views_pool_size) * 100)));
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
  new.likes_percentile := v_likes_pct;
  new.comments_percentile := v_comments_pct;
  new.views_percentile := v_views_pct;
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
