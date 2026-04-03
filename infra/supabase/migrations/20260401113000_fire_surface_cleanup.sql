begin;

create or replace function public.tg_compute_post_metrics()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_media_type text;
  v_feeder_id bigint;
  v_mv numeric;
  v_pool_size int;
  v_rank int;
  v_views_pool_size int;
  v_views_rank int;
  v_likes_pool_size int;
  v_likes_rank int;
  v_comments_pool_size int;
  v_comments_rank int;
  v_d1_pct int;
begin
  select
    lower(coalesce(p.media_type, 'unknown')),
    p.feeder_id
  into v_media_type, v_feeder_id
  from public.posts p
  where p.post_key = new.post_key;

  v_mv := public.fn_metric_value(v_media_type, new.views, new.likes, new.comments);

  v_rank := null;
  v_pool_size := 0;
  if v_mv is not null and v_feeder_id is not null then
    select count(*) + 1
    into v_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and lower(coalesce(p2.media_type, 'unknown')) = v_media_type
      and pm.checkpoint = new.checkpoint
      and pm.metric_value is not null
      and pm.metric_value > v_mv;

    select count(*) + 1
    into v_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and lower(coalesce(p2.media_type, 'unknown')) = v_media_type
      and pm.checkpoint = new.checkpoint
      and pm.metric_value is not null;
  end if;

  v_views_rank := null;
  v_views_pool_size := 0;
  if new.views is not null and v_feeder_id is not null then
    select count(*) + 1
    into v_views_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and lower(coalesce(p2.media_type, 'unknown')) = v_media_type
      and pm.checkpoint = new.checkpoint
      and pm.views is not null
      and pm.views > new.views;

    select count(*) + 1
    into v_views_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and lower(coalesce(p2.media_type, 'unknown')) = v_media_type
      and pm.checkpoint = new.checkpoint
      and pm.views is not null;
  end if;

  v_likes_rank := null;
  v_likes_pool_size := 0;
  if new.likes is not null and v_feeder_id is not null then
    select count(*) + 1
    into v_likes_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and lower(coalesce(p2.media_type, 'unknown')) = v_media_type
      and pm.checkpoint = new.checkpoint
      and pm.likes is not null
      and pm.likes > new.likes;

    select count(*) + 1
    into v_likes_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and lower(coalesce(p2.media_type, 'unknown')) = v_media_type
      and pm.checkpoint = new.checkpoint
      and pm.likes is not null;
  end if;

  v_comments_rank := null;
  v_comments_pool_size := 0;
  if new.comments is not null and v_feeder_id is not null then
    select count(*) + 1
    into v_comments_rank
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and lower(coalesce(p2.media_type, 'unknown')) = v_media_type
      and pm.checkpoint = new.checkpoint
      and pm.comments is not null
      and pm.comments > new.comments;

    select count(*) + 1
    into v_comments_pool_size
    from public.post_metrics pm
    join public.posts p2 on p2.post_key = pm.post_key
    where p2.feeder_id = v_feeder_id
      and lower(coalesce(p2.media_type, 'unknown')) = v_media_type
      and pm.checkpoint = new.checkpoint
      and pm.comments is not null;
  end if;

  select pm.percentile_performance
  into v_d1_pct
  from public.post_metrics pm
  where pm.post_key = new.post_key
    and pm.checkpoint = 'd1'
  limit 1;

  new.metric_value := v_mv;
  new.percentile_performance := case
    when v_pool_size > 0 then greatest(1, least(100, round((v_rank::numeric / v_pool_size::numeric) * 100)))
    else null
  end;
  new.feed_percentile := null;
  new.views_percentile := case
    when v_views_pool_size > 0 then greatest(1, least(100, round((v_views_rank::numeric / v_views_pool_size::numeric) * 100)))
    else null
  end;
  new.likes_percentile := case
    when v_likes_pool_size > 0 then greatest(1, least(100, round((v_likes_rank::numeric / v_likes_pool_size::numeric) * 100)))
    else null
  end;
  new.comments_percentile := case
    when v_comments_pool_size > 0 then greatest(1, least(100, round((v_comments_rank::numeric / v_comments_pool_size::numeric) * 100)))
    else null
  end;
  new.delta_from_d1 := case
    when new.checkpoint = 'd1' then null
    when v_d1_pct is null or new.percentile_performance is null then null
    else v_d1_pct - new.percentile_performance
  end;

  return new;
end;
$$;

create or replace function public.fn_process_checkpoint(
  p_feeder_id bigint,
  p_checkpoint text,
  p_business_date date
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_cp text := lower(coalesce(p_checkpoint, 'd1'));
  v_day date := coalesce(p_business_date, (now() at time zone 'Asia/Kolkata')::date);
  v_feed_id bigint;
  v_handle text;
  v_rows int := 0;
  r record;
  v_best_metric text;
  v_best_value bigint;
  v_baseline bigint;
  v_multiple numeric;
  v_views_multiple numeric;
  v_likes_multiple numeric;
  v_comments_multiple numeric;
  v_body text;
  v_alert_type text;
  v_dedupe text;
begin
  if v_cp not in ('d1', 'd3', 'd7', 'd21') then
    return 0;
  end if;

  select fd.feed_id, fd.handle
  into v_feed_id, v_handle
  from public.feeders fd
  where fd.id = p_feeder_id;

  if v_feed_id is null then
    return 0;
  end if;

  update public.fire_alerts fa
  set status = 'dropped',
      updated_at = now()
  where fa.feeder_id = p_feeder_id
    and fa.checkpoint = v_cp
    and fa.business_date_ist = v_day
    and fa.signal_code = 'slot_v3'
    and fa.context = 'own'
    and not exists (
      select 1
      from public.post_metrics pm
      join public.posts p on p.post_key = pm.post_key
      where p.feeder_id = p_feeder_id
        and pm.post_key = fa.post_key
        and pm.checkpoint = v_cp
        and pm.business_date_ist = v_day
        and public.fn_is_hot_percentile(pm.percentile_performance)
    );

  for r in
    select
      p.post_key,
      lower(coalesce(p.media_type, 'unknown')) as media_type,
      pm.views,
      pm.likes,
      pm.comments,
      pm.percentile_performance,
      pm.delta_from_d1,
      fb.median_views,
      fb.median_likes,
      fb.median_comments
    from public.posts p
    join public.post_metrics pm
      on pm.post_key = p.post_key
     and pm.checkpoint = v_cp
     and pm.business_date_ist = v_day
    left join public.feeder_baselines fb
      on fb.feeder_id = p.feeder_id
     and fb.media_type = lower(coalesce(p.media_type, 'unknown'))
     and fb.checkpoint = pm.checkpoint
    where p.feeder_id = p_feeder_id
      and public.fn_is_hot_percentile(pm.percentile_performance)
    order by pm.percentile_performance asc, p.posted_at desc nulls last
  loop
    v_views_multiple := case
      when r.views is not null and coalesce(r.median_views, 0) > 0
        then round(r.views::numeric / r.median_views::numeric, 2)
      else null
    end;
    v_likes_multiple := case
      when r.likes is not null and coalesce(r.median_likes, 0) > 0
        then round(r.likes::numeric / r.median_likes::numeric, 2)
      else null
    end;
    v_comments_multiple := case
      when r.comments is not null and coalesce(r.median_comments, 0) > 0
        then round(r.comments::numeric / r.median_comments::numeric, 2)
      else null
    end;

    v_best_metric := null;
    v_best_value := null;
    v_baseline := null;
    v_multiple := null;

    if r.media_type in ('reel', 'video') then
      if v_views_multiple is not null then
        v_best_metric := 'views';
        v_best_value := r.views;
        v_baseline := r.median_views;
        v_multiple := v_views_multiple;
      end if;
      if v_likes_multiple is not null and (v_multiple is null or v_likes_multiple > v_multiple) then
        v_best_metric := 'likes';
        v_best_value := r.likes;
        v_baseline := r.median_likes;
        v_multiple := v_likes_multiple;
      end if;
      if v_comments_multiple is not null and (v_multiple is null or v_comments_multiple > v_multiple) then
        v_best_metric := 'comments';
        v_best_value := r.comments;
        v_baseline := r.median_comments;
        v_multiple := v_comments_multiple;
      end if;
    else
      if v_likes_multiple is not null then
        v_best_metric := 'likes';
        v_best_value := r.likes;
        v_baseline := r.median_likes;
        v_multiple := v_likes_multiple;
      end if;
      if v_comments_multiple is not null and (v_multiple is null or v_comments_multiple > v_multiple) then
        v_best_metric := 'comments';
        v_best_value := r.comments;
        v_baseline := r.median_comments;
        v_multiple := v_comments_multiple;
      end if;
      if v_views_multiple is not null and (v_multiple is null or v_views_multiple > v_multiple) then
        v_best_metric := 'views';
        v_best_value := r.views;
        v_baseline := r.median_views;
        v_multiple := v_views_multiple;
      end if;
    end if;

    if v_best_metric is null then
      if r.media_type in ('reel', 'video') then
        if r.views is not null then
          v_best_metric := 'views';
          v_best_value := r.views;
          v_baseline := r.median_views;
        elsif r.likes is not null then
          v_best_metric := 'likes';
          v_best_value := r.likes;
          v_baseline := r.median_likes;
        elsif r.comments is not null then
          v_best_metric := 'comments';
          v_best_value := r.comments;
          v_baseline := r.median_comments;
        end if;
      else
        if r.likes is not null then
          v_best_metric := 'likes';
          v_best_value := r.likes;
          v_baseline := r.median_likes;
        elsif r.comments is not null then
          v_best_metric := 'comments';
          v_best_value := r.comments;
          v_baseline := r.median_comments;
        elsif r.views is not null then
          v_best_metric := 'views';
          v_best_value := r.views;
          v_baseline := r.median_views;
        end if;
      end if;

      if v_best_value is not null and coalesce(v_baseline, 0) > 0 then
        v_multiple := round(v_best_value::numeric / v_baseline::numeric, 2);
      end if;
    end if;

    v_alert_type := case
      when r.percentile_performance <= 10 then 'blaze'
      when r.percentile_performance <= 25 then 'burn'
      else 'spark'
    end;

    v_body := case
      when v_cp = 'd1' and v_multiple is not null then
        format(
          '@%s hit the top %s%% at %s with %s %s, %.2fx its usual %s.',
          upper(coalesce(v_handle, 'feed')),
          coalesce(r.percentile_performance, 0),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(v_best_metric),
          v_multiple,
          upper(v_best_metric)
        )
      when v_cp = 'd1' then
        format(
          '@%s hit the top %s%% at %s with %s %s.',
          upper(coalesce(v_handle, 'feed')),
          coalesce(r.percentile_performance, 0),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(v_best_metric)
        )
      when r.delta_from_d1 is not null then
        format(
          '@%s stayed in the top %s%% at %s with %s %s, %s points vs D1.',
          upper(coalesce(v_handle, 'feed')),
          coalesce(r.percentile_performance, 0),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(v_best_metric),
          case when r.delta_from_d1 > 0 then '+' || r.delta_from_d1::text else r.delta_from_d1::text end
        )
      else
        format(
          '@%s stayed in the top %s%% at %s with %s %s.',
          upper(coalesce(v_handle, 'feed')),
          coalesce(r.percentile_performance, 0),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(v_best_metric)
        )
    end;

    v_dedupe := format('slot_v3:%s:%s:%s:%s', v_feed_id, r.post_key, v_cp, v_day);

    insert into public.fire_alerts (
      dedupe_key,
      feed_id,
      feeder_id,
      post_key,
      checkpoint,
      business_date_ist,
      signal_code,
      context,
      alert_type,
      status,
      metric_key,
      metric_value,
      surface_percentile,
      surface_delta,
      body,
      created_at,
      updated_at,
      pattern_signal,
      pattern_payload
    )
    values (
      v_dedupe,
      v_feed_id,
      p_feeder_id,
      r.post_key,
      v_cp,
      v_day,
      'slot_v3',
      'own',
      v_alert_type,
      'new',
      coalesce(v_best_metric, 'views'),
      v_best_value,
      r.percentile_performance,
      r.delta_from_d1,
      v_body,
      now(),
      now(),
      null,
      null
    )
    on conflict (dedupe_key) do update
      set alert_type = excluded.alert_type,
          status = 'new',
          metric_key = excluded.metric_key,
          metric_value = excluded.metric_value,
          surface_percentile = excluded.surface_percentile,
          surface_delta = excluded.surface_delta,
          body = excluded.body,
          updated_at = now(),
          pattern_signal = null,
          pattern_payload = null;

    v_rows := v_rows + 1;
  end loop;

  return v_rows;
end;
$$;

drop view if exists public.v_fire_alert_surface;

create or replace view public.v_fire_alert_surface as
with core as (
  select
    fa.id,
    fa.dedupe_key,
    fa.feed_id,
    fa.feeder_id,
    fa.post_key,
    fa.checkpoint,
    fa.business_date_ist,
    fa.signal_code,
    fa.context,
    fa.alert_type,
    fa.status,
    fa.metric_key,
    fa.metric_value,
    fa.surface_percentile,
    fa.surface_delta,
    fa.body,
    fa.pattern_signal,
    fa.pattern_payload,
    fa.created_at,
    fa.updated_at,
    fd.handle,
    lower(coalesce(p.media_type, 'unknown')) as media_type,
    p.posted_at,
    p.post_url,
    p.thumbnail_url,
    extract(hour from (p.posted_at at time zone 'Asia/Kolkata'))::int as hour_ist,
    pm.views,
    pm.likes,
    pm.comments,
    d1.percentile_performance as trajectory_d1,
    d3.percentile_performance as trajectory_d3,
    d7.percentile_performance as trajectory_d7,
    d21.percentile_performance as trajectory_d21,
    fb.median_views as views_baseline,
    fb.median_likes as likes_baseline,
    fb.median_comments as comments_baseline,
    fhb.median_views as hour_views_baseline,
    fhb.median_likes as hour_likes_baseline,
    fhb.median_comments as hour_comments_baseline,
    coalesce(pi.model_version = 'skipped', false) as intelligence_skipped
  from public.fire_alerts fa
  join public.posts p on p.post_key = fa.post_key
  join public.feeders fd on fd.id = fa.feeder_id
  left join public.post_metrics pm
    on pm.post_key = fa.post_key
   and pm.checkpoint = fa.checkpoint
  left join public.post_metrics d1
    on d1.post_key = fa.post_key
   and d1.checkpoint = 'd1'
  left join public.post_metrics d3
    on d3.post_key = fa.post_key
   and d3.checkpoint = 'd3'
  left join public.post_metrics d7
    on d7.post_key = fa.post_key
   and d7.checkpoint = 'd7'
  left join public.post_metrics d21
    on d21.post_key = fa.post_key
   and d21.checkpoint = 'd21'
  left join public.feeder_baselines fb
    on fb.feeder_id = fa.feeder_id
   and fb.media_type = lower(coalesce(p.media_type, 'unknown'))
   and fb.checkpoint = fa.checkpoint
  left join public.feeder_hour_baselines fhb
    on fhb.feeder_id = fa.feeder_id
   and fhb.media_type = lower(coalesce(p.media_type, 'unknown'))
   and fhb.checkpoint = fa.checkpoint
   and fhb.hour_ist = extract(hour from (p.posted_at at time zone 'Asia/Kolkata'))::int
  left join public.post_intelligence pi on pi.post_key = fa.post_key
)
select
  core.*,
  case
    when core.views is not null and coalesce(core.views_baseline, 0) > 0
      then round(core.views::numeric / core.views_baseline::numeric, 4)
    else null
  end as views_multiple,
  case
    when core.likes is not null and coalesce(core.likes_baseline, 0) > 0
      then round(core.likes::numeric / core.likes_baseline::numeric, 4)
    else null
  end as likes_multiple,
  case
    when core.comments is not null and coalesce(core.comments_baseline, 0) > 0
      then round(core.comments::numeric / core.comments_baseline::numeric, 4)
    else null
  end as comments_multiple,
  case
    when core.metric_key = 'likes' and core.likes is not null and coalesce(core.hour_likes_baseline, 0) > 0
      then round(core.likes::numeric / core.hour_likes_baseline::numeric, 4)
    when core.metric_key = 'comments' and core.comments is not null and coalesce(core.hour_comments_baseline, 0) > 0
      then round(core.comments::numeric / core.hour_comments_baseline::numeric, 4)
    when core.metric_key = 'views' and core.views is not null and coalesce(core.hour_views_baseline, 0) > 0
      then round(core.views::numeric / core.hour_views_baseline::numeric, 4)
    else null
  end as hour_multiple,
  recent.best_in_last_n,
  case
    when core.metric_key = 'likes' and hour_rank.hour_pool > 0 and hour_rank.hour_rank is not null
      then greatest(1, least(100, round((hour_rank.hour_rank::numeric / hour_rank.hour_pool::numeric) * 100)))::int
    when core.metric_key = 'comments' and hour_rank.hour_pool > 0 and hour_rank.hour_rank is not null
      then greatest(1, least(100, round((hour_rank.hour_rank::numeric / hour_rank.hour_pool::numeric) * 100)))::int
    when core.metric_key = 'views' and hour_rank.hour_pool > 0 and hour_rank.hour_rank is not null
      then greatest(1, least(100, round((hour_rank.hour_rank::numeric / hour_rank.hour_pool::numeric) * 100)))::int
    else null
  end as hour_percentile
from core
left join lateral (
  select case core.metric_key
    when 'likes' then (
      select count(*)::int
      from (
        select pm.likes
        from public.post_metrics pm
        join public.posts p2 on p2.post_key = pm.post_key
        where p2.feeder_id = core.feeder_id
          and lower(coalesce(p2.media_type, 'unknown')) = core.media_type
          and pm.checkpoint = core.checkpoint
          and pm.post_key <> core.post_key
          and pm.likes is not null
        order by pm.computed_at desc
        limit 50
      ) s
      where core.likes is not null
        and s.likes < core.likes
    )
    when 'comments' then (
      select count(*)::int
      from (
        select pm.comments
        from public.post_metrics pm
        join public.posts p2 on p2.post_key = pm.post_key
        where p2.feeder_id = core.feeder_id
          and lower(coalesce(p2.media_type, 'unknown')) = core.media_type
          and pm.checkpoint = core.checkpoint
          and pm.post_key <> core.post_key
          and pm.comments is not null
        order by pm.computed_at desc
        limit 50
      ) s
      where core.comments is not null
        and s.comments < core.comments
    )
    else (
      select count(*)::int
      from (
        select pm.views
        from public.post_metrics pm
        join public.posts p2 on p2.post_key = pm.post_key
        where p2.feeder_id = core.feeder_id
          and lower(coalesce(p2.media_type, 'unknown')) = core.media_type
          and pm.checkpoint = core.checkpoint
          and pm.post_key <> core.post_key
          and pm.views is not null
        order by pm.computed_at desc
        limit 50
      ) s
      where core.views is not null
        and s.views < core.views
    )
  end as best_in_last_n
) recent on true
left join lateral (
  select
    count(*)::int as hour_pool,
    case core.metric_key
      when 'likes' then (
        select count(*) + 1
        from public.post_metrics pm4
        join public.posts p4 on p4.post_key = pm4.post_key
        where p4.feeder_id = core.feeder_id
          and lower(coalesce(p4.media_type, 'unknown')) = core.media_type
          and pm4.checkpoint = core.checkpoint
          and extract(hour from (p4.posted_at at time zone 'Asia/Kolkata'))::int = core.hour_ist
          and pm4.likes is not null
          and core.likes is not null
          and pm4.likes > core.likes
      )
      when 'comments' then (
        select count(*) + 1
        from public.post_metrics pm4
        join public.posts p4 on p4.post_key = pm4.post_key
        where p4.feeder_id = core.feeder_id
          and lower(coalesce(p4.media_type, 'unknown')) = core.media_type
          and pm4.checkpoint = core.checkpoint
          and extract(hour from (p4.posted_at at time zone 'Asia/Kolkata'))::int = core.hour_ist
          and pm4.comments is not null
          and core.comments is not null
          and pm4.comments > core.comments
      )
      else (
        select count(*) + 1
        from public.post_metrics pm4
        join public.posts p4 on p4.post_key = pm4.post_key
        where p4.feeder_id = core.feeder_id
          and lower(coalesce(p4.media_type, 'unknown')) = core.media_type
          and pm4.checkpoint = core.checkpoint
          and extract(hour from (p4.posted_at at time zone 'Asia/Kolkata'))::int = core.hour_ist
          and pm4.views is not null
          and core.views is not null
          and pm4.views > core.views
      )
    end as hour_rank
  from public.post_metrics pm4
  join public.posts p4 on p4.post_key = pm4.post_key
  where p4.feeder_id = core.feeder_id
    and lower(coalesce(p4.media_type, 'unknown')) = core.media_type
    and pm4.checkpoint = core.checkpoint
    and extract(hour from (p4.posted_at at time zone 'Asia/Kolkata'))::int = core.hour_ist
    and (
      (core.metric_key = 'likes' and pm4.likes is not null)
      or (core.metric_key = 'comments' and pm4.comments is not null)
      or (core.metric_key = 'views' and pm4.views is not null)
    )
) hour_rank on true;

commit;
