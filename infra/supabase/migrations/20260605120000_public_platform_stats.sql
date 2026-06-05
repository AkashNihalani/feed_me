-- Public platform stats for the pre-login "live intelligence" dashboard.
--
-- A single-row rollup table refreshed every 10 minutes by pg_cron so the public
-- /api/stats/public endpoint is an O(1) read — no heavy aggregation ever runs on
-- the web request path. Numbers are real platform totals across all feeds.

begin;

create table if not exists public.platform_stats (
  id smallint primary key default 1,
  accounts bigint not null default 0,
  posts bigint not null default 0,
  likes numeric not null default 0,
  comments numeric not null default 0,
  views numeric not null default 0,
  feeds bigint not null default 0,
  creators bigint not null default 0,
  signals bigint not null default 0,
  refreshed_at timestamptz not null default now(),
  -- previous snapshot, so the public endpoint can derive a real growth rate
  prev_accounts bigint not null default 0,
  prev_posts bigint not null default 0,
  prev_likes numeric not null default 0,
  prev_comments numeric not null default 0,
  prev_views numeric not null default 0,
  prev_feeds bigint not null default 0,
  prev_creators bigint not null default 0,
  prev_signals bigint not null default 0,
  prev_refreshed_at timestamptz not null default now(),
  constraint platform_stats_singleton check (id = 1)
);

-- Recompute every boastable platform total into the singleton row.
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
  select count(*) into v_signals from public.signals;

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
    -- roll the existing row into the previous snapshot first
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

-- The rollup holds only aggregate, non-sensitive numbers — explicitly public-readable.
alter table public.platform_stats enable row level security;

drop policy if exists platform_stats_public_read on public.platform_stats;
create policy platform_stats_public_read
  on public.platform_stats
  for select
  using (true);

grant select on public.platform_stats to anon, authenticated;

-- Refreshing is a heavy aggregate — only the scheduler / service role should call it.
revoke execute on function public.refresh_platform_stats() from public;

-- Seed immediately so the endpoint has data the moment this ships.
select public.refresh_platform_stats();

-- Refresh every 10 minutes via pg_cron (same scheduling backbone as the rest).
do $$
declare
  r record;
begin
  if to_regnamespace('cron') is null then
    raise notice 'pg_cron schema not found. Enable pg_cron and rerun migration.';
    return;
  end if;

  for r in
    select jobid from cron.job where jobname = 'feedme_refresh_platform_stats_10m'
  loop
    perform cron.unschedule(r.jobid);
  end loop;

  perform cron.schedule(
    'feedme_refresh_platform_stats_10m',
    '*/10 * * * *',
    $q$select public.refresh_platform_stats();$q$
  );
end $$;

commit;
