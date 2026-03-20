begin;

create table if not exists public.feeder_follower_snapshots (
  feeder_id bigint not null references public.feeders(id) on delete cascade,
  week_start_ist date not null,
  follower_count bigint not null default 0,
  captured_at timestamptz not null default now(),
  primary key (feeder_id, week_start_ist)
);

create index if not exists feeder_follower_snapshots_week_idx
  on public.feeder_follower_snapshots (week_start_ist desc, feeder_id);

insert into public.feeder_follower_snapshots (feeder_id, week_start_ist, follower_count, captured_at)
select
  f.id,
  date_trunc('week', timezone('Asia/Kolkata', now()))::date as week_start_ist,
  greatest(0, coalesce(f.follower_count, 0))::bigint as follower_count,
  now()
from public.feeders f
on conflict (feeder_id, week_start_ist) do nothing;

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
      pm.post_hour_ist::int as hour_ist,
      count(*)::int as post_count
    from public.post_metrics pm
    join public.posts p on p.post_key = pm.post_key
    where p.feeder_id = any(v_feeder_ids)
      and pm.checkpoint = 'd1'
      and pm.post_hour_ist is not null
      and (p.posted_at at time zone 'Asia/Kolkata')::date >= v_window_start
      and (p.posted_at at time zone 'Asia/Kolkata')::date < v_window_end
    group by pm.post_hour_ist
    order by pm.post_hour_ist
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

grant execute on function public.fn_feed_dashboard(bigint, int, text) to authenticated, service_role;

commit;

