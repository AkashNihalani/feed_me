-- Align the public login dashboard's "Checkpoints surfaced" total with the
-- tracking model: every post_metrics row is one post observed at one checkpoint.
--
-- The existing platform_stats.signals column is kept as the public API wire key
-- for compatibility, but it now stores checkpoint surfaces rather than rows from
-- public.signals.

begin;

create or replace function public.refresh_platform_stats()
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_accounts  bigint;
  v_posts     bigint;
  v_feeds     bigint;
  v_creators  bigint;
  v_signals   bigint;
  v_likes     numeric;
  v_comments  numeric;
  v_views     numeric;
begin
  select count(*) into v_accounts from public.feeders where status = 'active';

  select count(*) into v_posts   from public.posts;
  select count(*) into v_feeds   from public.feeds;
  select count(distinct user_id) into v_creators from public.feeds;
  select count(*) into v_signals from public.post_metrics;

  -- Engagement = the latest captured checkpoint per post (d21 > d7 > d3 > d1),
  -- so a post is counted once at its most recent observed value.
  with latest as (
    select distinct on (post_key)
      post_key, likes, comments, views
    from public.post_metrics
    order by
      post_key,
      case checkpoint
        when 'd21' then 4 when 'd7' then 3 when 'd3' then 2 when 'd1' then 1 else 0
      end desc,
      computed_at desc
  )
  select
    coalesce(sum(likes), 0),
    coalesce(sum(comments), 0),
    coalesce(sum(views), 0)
    into v_likes, v_comments, v_views
    from latest;

  insert into public.platform_stats as ps
      (id, accounts, posts, likes, comments, views, feeds, creators, signals, refreshed_at)
    values
      (1, v_accounts, v_posts, v_likes, v_comments, v_views, v_feeds, v_creators, v_signals, now())
  on conflict (id) do update set
    prev_accounts     = ps.accounts,
    prev_posts        = ps.posts,
    prev_likes        = ps.likes,
    prev_comments     = ps.comments,
    prev_views        = ps.views,
    prev_feeds        = ps.feeds,
    prev_creators     = ps.creators,
    prev_signals      = ps.signals,
    prev_refreshed_at = ps.refreshed_at,
    accounts     = excluded.accounts,
    posts        = excluded.posts,
    likes        = excluded.likes,
    comments     = excluded.comments,
    views        = excluded.views,
    feeds        = excluded.feeds,
    creators     = excluded.creators,
    signals      = excluded.signals,
    refreshed_at = excluded.refreshed_at;
end;
$$;

select public.refresh_platform_stats();

commit;
