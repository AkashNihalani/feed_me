begin;

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
  v_visible boolean;
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
        and (
          v_cp in ('d1', 'd3', 'd7')
          or public.fn_is_hot_percentile(pm.percentile_performance)
        )
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
      fb.median_comments,
      public.fn_is_hot_percentile(pm.percentile_performance) as is_hot
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
    order by
      case
        when v_cp in ('d1', 'd3', 'd7') then 0
        when public.fn_is_hot_percentile(pm.percentile_performance) then 0
        else 1
      end,
      pm.percentile_performance asc,
      p.posted_at desc nulls last
  loop
    v_visible := (v_cp in ('d1', 'd3', 'd7')) or coalesce(r.is_hot, false);
    if not v_visible then
      continue;
    end if;

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
      when not coalesce(r.is_hot, false) then 'tracking'
      when r.percentile_performance <= 10 then 'blaze'
      when r.percentile_performance <= 25 then 'burn'
      else 'spark'
    end;

    v_body := case
      when not coalesce(r.is_hot, false) and v_multiple is not null then
        format(
          '@%s checked in at %s with %s %s, %sx its current baseline.',
          upper(coalesce(v_handle, 'feed')),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(coalesce(v_best_metric, 'views')),
          to_char(v_multiple, 'FM999999990.00')
        )
      when not coalesce(r.is_hot, false) then
        format(
          '@%s checked in at %s with %s %s.',
          upper(coalesce(v_handle, 'feed')),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(coalesce(v_best_metric, 'views'))
        )
      when v_cp = 'd1' and v_multiple is not null then
        format(
          '@%s hit the top %s%% at %s with %s %s, %sx its usual %s.',
          upper(coalesce(v_handle, 'feed')),
          coalesce(r.percentile_performance, 0),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(coalesce(v_best_metric, 'views')),
          to_char(v_multiple, 'FM999999990.00'),
          upper(coalesce(v_best_metric, 'views'))
        )
      when v_cp = 'd1' then
        format(
          '@%s hit the top %s%% at %s with %s %s.',
          upper(coalesce(v_handle, 'feed')),
          coalesce(r.percentile_performance, 0),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(coalesce(v_best_metric, 'views'))
        )
      when r.delta_from_d1 is not null then
        format(
          '@%s stayed in the top %s%% at %s with %s %s, %s points vs D1.',
          upper(coalesce(v_handle, 'feed')),
          coalesce(r.percentile_performance, 0),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(coalesce(v_best_metric, 'views')),
          case when r.delta_from_d1 > 0 then '+' || r.delta_from_d1::text else r.delta_from_d1::text end
        )
      else
        format(
          '@%s stayed in the top %s%% at %s with %s %s.',
          upper(coalesce(v_handle, 'feed')),
          coalesce(r.percentile_performance, 0),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(coalesce(v_best_metric, 'views'))
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
          pattern_signal = case
            when excluded.checkpoint = 'd7' and excluded.alert_type <> 'tracking' then null
            else public.fire_alerts.pattern_signal
          end,
          pattern_payload = case
            when excluded.checkpoint = 'd7' and excluded.alert_type <> 'tracking' then null
            else public.fire_alerts.pattern_payload
          end;

    v_rows := v_rows + 1;
  end loop;

  return v_rows;
end;
$$;

do $$
declare
  r record;
  v_recent_floor date := ((now() at time zone 'Asia/Kolkata')::date - 21);
begin
  for r in
    select distinct
      p.feeder_id,
      pm.checkpoint,
      pm.business_date_ist
    from public.post_metrics pm
    join public.posts p on p.post_key = pm.post_key
    where pm.checkpoint in ('d1', 'd3', 'd7', 'd21')
      and pm.business_date_ist >= v_recent_floor
  loop
    perform public.fn_process_checkpoint(r.feeder_id, r.checkpoint, r.business_date_ist);
  end loop;
end $$;

commit;
