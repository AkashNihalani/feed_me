begin;

create or replace function public.fn_feed_bundle(
  p_user_id uuid
)
returns json
language sql
security definer
set search_path = public
as $$
  with feed_scope as (
    select
      f.id,
      f.name,
      f.created_at
    from public.feeds f
    where f.user_id = p_user_id
      and f.status = 'active'
  ),
  feeder_scope as (
    select
      fd.id,
      fd.feed_id,
      fd.handle,
      fd.role,
      fd.created_at,
      fd.profile_pic_url,
      fd.follower_count
    from public.feeders fd
    join feed_scope fs on fs.id = fd.feed_id
    where fd.status = 'active'
  ),
  tracked_posts as (
    select
      p.post_key,
      p.feeder_id,
      p.posted_at
    from public.posts p
    join feeder_scope fs on fs.id = p.feeder_id
    where p.posted_at is not null
      and p.posted_at >= fs.created_at
      and exists (
        select 1
        from public.post_metrics pm
        where pm.post_key = p.post_key
          and lower(coalesce(pm.checkpoint, '')) = 'd1'
      )
  ),
  ordered_metrics as (
    select
      pm.post_key,
      coalesce(pm.likes, 0)::bigint as likes,
      coalesce(pm.comments, 0)::bigint as comments,
      coalesce(pm.views, 0)::bigint as views,
      pm.computed_at,
      row_number() over (
        partition by pm.post_key
        order by pm.computed_at desc nulls last
      ) as rn
    from public.post_metrics pm
    join tracked_posts tp on tp.post_key = pm.post_key
  ),
  latest_metrics as (
    select
      om.post_key,
      om.likes,
      om.comments,
      om.views
    from ordered_metrics om
    where om.rn = 1
  ),
  previous_metrics as (
    select
      om.post_key,
      om.likes,
      om.comments,
      om.views
    from ordered_metrics om
    where om.rn = 2
  ),
  feeder_stats as (
    select
      fs.id as feeder_id,
      coalesce(sum(lm.likes), 0)::bigint as likes,
      coalesce(sum(lm.comments), 0)::bigint as comments,
      coalesce(sum(lm.views), 0)::bigint as views,
      count(tp.post_key)::int as posts_tracked
    from feeder_scope fs
    left join tracked_posts tp on tp.feeder_id = fs.id
    left join latest_metrics lm on lm.post_key = tp.post_key
    group by fs.id
  ),
  feed_feeder_rows as (
    select
      fs.feed_id,
      fs.id as feeder_id,
      fs.handle,
      fs.role,
      fs.profile_pic_url,
      fs.follower_count,
      coalesce(ft.likes, 0)::bigint as likes,
      coalesce(ft.comments, 0)::bigint as comments,
      coalesce(ft.views, 0)::bigint as views,
      coalesce(ft.posts_tracked, 0)::int as posts_tracked
    from feeder_scope fs
    left join feeder_stats ft on ft.feeder_id = fs.id
  ),
  feed_stats as (
    select
      ffr.feed_id,
      coalesce(sum(ffr.likes), 0)::bigint as likes,
      coalesce(sum(ffr.comments), 0)::bigint as comments,
      coalesce(sum(ffr.views), 0)::bigint as views,
      coalesce(sum(ffr.posts_tracked), 0)::int as posts_tracked
    from feed_feeder_rows ffr
    group by ffr.feed_id
  ),
  ticker_rows as (
    select
      fs.id as feeder_id,
      fs.handle,
      coalesce(sum(coalesce(lm.likes, 0) - coalesce(pm.likes, 0)), 0)::bigint as likes_delta,
      coalesce(sum(coalesce(lm.comments, 0) - coalesce(pm.comments, 0)), 0)::bigint as comments_delta,
      coalesce(sum(coalesce(lm.views, 0) - coalesce(pm.views, 0)), 0)::bigint as views_delta,
      count(tp.post_key) filter (
        where tp.posted_at >= now() - interval '7 days'
      )::int as posts_delta,
      (
        abs(coalesce(sum(coalesce(lm.likes, 0) - coalesce(pm.likes, 0)), 0))::numeric
        + abs(coalesce(sum(coalesce(lm.comments, 0) - coalesce(pm.comments, 0)), 0))::numeric
        + abs(coalesce(sum(coalesce(lm.views, 0) - coalesce(pm.views, 0)), 0))::numeric * 0.01
        + abs(count(tp.post_key) filter (where tp.posted_at >= now() - interval '7 days'))::numeric * 25
      ) as score
    from feeder_scope fs
    left join tracked_posts tp on tp.feeder_id = fs.id
    left join latest_metrics lm on lm.post_key = tp.post_key
    left join previous_metrics pm on pm.post_key = tp.post_key
    group by fs.id, fs.handle
  ),
  feeds_payload as (
    select coalesce(
      json_agg(
        json_build_object(
          'id', fs.id::text,
          'title', upper(coalesce(fs.name, 'UNTITLED FEED')),
          'feeders', coalesce((
            select json_agg(
              json_build_object(
                'handle', ffr.handle,
                'isAnchor', ffr.role = 'anchor',
                'profilePicUrl', ffr.profile_pic_url,
                'followerCount', ffr.follower_count,
                'metrics', json_build_object(
                  'likes', ffr.likes,
                  'comments', ffr.comments,
                  'views', ffr.views,
                  'postsTracked', ffr.posts_tracked
                )
              )
              order by case when ffr.role = 'anchor' then 0 else 1 end, lower(coalesce(ffr.handle, ''))
            )
            from feed_feeder_rows ffr
            where ffr.feed_id = fs.id
          ), '[]'::json),
          'metrics', json_build_object(
            'likes', coalesce(fst.likes, 0),
            'comments', coalesce(fst.comments, 0),
            'views', coalesce(fst.views, 0),
            'postsTracked', coalesce(fst.posts_tracked, 0)
          )
        )
        order by fs.created_at desc
      ),
      '[]'::json
    ) as payload
    from feed_scope fs
    left join feed_stats fst on fst.feed_id = fs.id
  ),
  ticker_payload as (
    select coalesce(
      json_agg(
        json_build_object(
          'id', tr.feeder_id::text,
          'handle', '@' || tr.handle,
          'likesDelta', tr.likes_delta,
          'commentsDelta', tr.comments_delta,
          'viewsDelta', tr.views_delta,
          'postsDelta', tr.posts_delta
        )
        order by tr.score desc, tr.handle
      ),
      '[]'::json
    ) as payload
    from (
      select *
      from ticker_rows
      where handle is not null
        and handle <> ''
      order by score desc, handle
      limit 8
    ) tr
  )
  select json_build_object(
    'feeds', (select payload from feeds_payload),
    'ticker', (select payload from ticker_payload)
  );
$$;

commit;
