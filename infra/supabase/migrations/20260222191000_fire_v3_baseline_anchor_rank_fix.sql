begin;

create or replace function public.fn_process_checkpoint(
  p_feeder_id bigint,
  p_checkpoint text,
  p_business_date date
) returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_cp text := lower(coalesce(p_checkpoint, 'd1'));
  v_day date := coalesce(p_business_date, (now() at time zone 'Asia/Kolkata')::date);
  v_rows int := 0;
  v_feed_id bigint;
  v_handle text;
  v_anchor_feeder_id bigint;
  v_anchor_handle text;

  r record;
  v_alert_type text;
  v_surface_pct int;
  v_surface_delta int;

  v_best_metric text;
  v_best_metric_value bigint;
  v_best_metric_baseline bigint;
  v_best_metric_multiple numeric;
  v_best_metric_percentile int;
  v_best_metric_rank_feed int;

  v_views_multiple numeric;
  v_likes_multiple numeric;
  v_comments_multiple numeric;

  v_timing_hour_multiple numeric;
  v_peak_window_text text;

  v_anchor_best_pct int;
  v_anchor_gap int;

  v_d1 int;
  v_d3 int;
  v_d7 int;

  v_dedupe text;
  v_stamp text;
  v_title text;
  v_body text;
  v_payload jsonb;
begin
  if v_cp not in ('d1','d3','d7','d21') then
    return 0;
  end if;

  select fd.feed_id, fd.handle
  into v_feed_id, v_handle
  from public.feeders fd
  where fd.id = p_feeder_id;

  if v_feed_id is null then
    return 0;
  end if;

  -- Prefer configured anchor; fallback to first active peer feeder in the same feed.
  select fd.id, fd.handle
  into v_anchor_feeder_id, v_anchor_handle
  from public.feeders fd
  where fd.feed_id = v_feed_id
    and fd.status = 'active'
    and fd.role = 'anchor'
  order by fd.id
  limit 1;

  if v_anchor_feeder_id is null then
    select fd.id, fd.handle
    into v_anchor_feeder_id, v_anchor_handle
    from public.feeders fd
    where fd.feed_id = v_feed_id
      and fd.status = 'active'
      and fd.id <> p_feeder_id
    order by fd.id
    limit 1;
  end if;

  -- Keep baselines fresh for current feeder/checkpoint before processing rows.
  perform public.fn_refresh_feeder_baselines(p_feeder_id, v_cp);

  -- Hydrate baseline_* in post_metrics so payload layers always have comparison context.
  update public.post_metrics pm
  set
    baseline_views = coalesce(fb.median_views, pm.baseline_views),
    baseline_likes = coalesce(fb.median_likes, pm.baseline_likes),
    baseline_comments = coalesce(fb.median_comments, pm.baseline_comments)
  from public.posts p
  join public.feeder_baselines fb
    on fb.feeder_id = p.feeder_id
   and lower(fb.media_type) = lower(coalesce(p.media_type, 'reel'))
   and lower(fb.checkpoint) = v_cp
  where p.post_key = pm.post_key
    and p.feeder_id = p_feeder_id
    and lower(pm.checkpoint) = v_cp
    and (
      pm.baseline_views is null
      or pm.baseline_likes is null
      or pm.baseline_comments is null
    );

  -- Keep rank_all_time populated for this feeder/checkpoint set.
  with ranked as (
    select
      pm.post_key,
      row_number() over (
        order by pm.metric_value desc nulls last, pm.computed_at desc, pm.post_key
      )::int as rank_num
    from public.post_metrics pm
    join public.posts p on p.post_key = pm.post_key
    where p.feeder_id = p_feeder_id
      and lower(pm.checkpoint) = v_cp
      and pm.metric_value is not null
  )
  update public.post_metrics pm
  set rank_all_time = r.rank_num
  from ranked r
  where pm.post_key = r.post_key
    and lower(pm.checkpoint) = v_cp;

  for r in
    select
      pm.post_key,
      lower(pm.checkpoint) as checkpoint,
      lower(coalesce(p.media_type,'reel')) as media_type,
      pm.percentile_performance,
      pm.percentile_delta,
      pm.metric_value,
      pm.views,
      pm.likes,
      pm.comments,
      pm.interactions,
      pm.baseline_views,
      pm.baseline_likes,
      pm.baseline_comments,
      pm.views_multiple,
      pm.likes_multiple,
      pm.comments_multiple,
      pm.views_percentile,
      pm.likes_percentile,
      pm.comments_percentile,
      pm.views_rank_feed,
      pm.likes_rank_feed,
      pm.comments_rank_feed,
      pm.best_in_last_n_views,
      pm.best_in_last_n_likes,
      pm.best_in_last_n_comments,
      pm.rank_feed,
      pm.rank_all_time,
      pm.d1_percentile,
      pm.d3_percentile,
      pm.d7_percentile,
      pm.post_hour_ist,
      pm.hour_percentile_feeder,
      pm.hour_multiple_views,
      pm.hour_multiple_likes,
      pm.hour_multiple_comments,
      pm.is_best_hour_window,
      coalesce(pm.business_date_ist, (p.posted_at at time zone 'Asia/Kolkata')::date) as anchor_business_date_ist,
      pm.captured_business_date_ist,
      p.post_url,
      p.thumbnail_url
    from public.post_metrics pm
    join public.posts p on p.post_key = pm.post_key
    where p.feeder_id = p_feeder_id
      and lower(pm.checkpoint) = v_cp
      and pm.captured_business_date_ist = v_day
  loop
    v_views_multiple := coalesce(
      r.views_multiple,
      case
        when nullif(coalesce(r.baseline_views, 0), 0) is null then null
        else coalesce(r.views,0)::numeric / nullif(coalesce(r.baseline_views, 0), 0)::numeric
      end
    );
    v_likes_multiple := coalesce(
      r.likes_multiple,
      case
        when nullif(coalesce(r.baseline_likes, 0), 0) is null then null
        else coalesce(r.likes,0)::numeric / nullif(coalesce(r.baseline_likes, 0), 0)::numeric
      end
    );
    v_comments_multiple := coalesce(
      r.comments_multiple,
      case
        when nullif(coalesce(r.baseline_comments, 0), 0) is null then null
        else coalesce(r.comments,0)::numeric / nullif(coalesce(r.baseline_comments, 0), 0)::numeric
      end
    );

    v_best_metric := 'views';
    if coalesce(r.likes_percentile, 101) < coalesce(r.views_percentile, 101)
       and coalesce(r.likes_percentile, 101) <= coalesce(r.comments_percentile, 101) then
      v_best_metric := 'likes';
    elsif coalesce(r.comments_percentile, 101) < coalesce(r.views_percentile, 101)
       and coalesce(r.comments_percentile, 101) < coalesce(r.likes_percentile, 101) then
      v_best_metric := 'comments';
    end if;

    if v_best_metric = 'likes' then
      v_best_metric_value := coalesce(r.likes, 0);
      v_best_metric_baseline := nullif(coalesce(r.baseline_likes, 0), 0);
      v_best_metric_multiple := v_likes_multiple;
      v_best_metric_percentile := coalesce(r.likes_percentile, r.percentile_performance, 100);
      v_best_metric_rank_feed := r.likes_rank_feed;
      v_timing_hour_multiple := coalesce(r.hour_multiple_likes, r.hour_multiple_comments, r.hour_multiple_views);
    elsif v_best_metric = 'comments' then
      v_best_metric_value := coalesce(r.comments, 0);
      v_best_metric_baseline := nullif(coalesce(r.baseline_comments, 0), 0);
      v_best_metric_multiple := v_comments_multiple;
      v_best_metric_percentile := coalesce(r.comments_percentile, r.percentile_performance, 100);
      v_best_metric_rank_feed := r.comments_rank_feed;
      v_timing_hour_multiple := coalesce(r.hour_multiple_comments, r.hour_multiple_likes, r.hour_multiple_views);
    else
      v_best_metric_value := coalesce(r.views, coalesce(r.metric_value, 0));
      v_best_metric_baseline := nullif(coalesce(r.baseline_views, 0), 0);
      v_best_metric_multiple := v_views_multiple;
      v_best_metric_percentile := coalesce(r.views_percentile, r.percentile_performance, 100);
      v_best_metric_rank_feed := r.views_rank_feed;
      v_timing_hour_multiple := coalesce(r.hour_multiple_views, r.hour_multiple_likes, r.hour_multiple_comments);
    end if;

    v_surface_pct := coalesce(v_best_metric_percentile, r.percentile_performance, 100);

    v_d1 := coalesce(r.d1_percentile, v_surface_pct);
    v_d3 := coalesce(r.d3_percentile, case when v_cp = 'd3' then v_surface_pct else null end, v_d1);
    v_d7 := coalesce(r.d7_percentile, case when v_cp = 'd7' then v_surface_pct else null end, v_d3, v_d1);
    v_surface_delta := case
      when v_cp='d1' then null
      else (v_d1 - coalesce(case when v_cp='d3' then v_d3 when v_cp='d7' then v_d7 else v_surface_pct end, v_surface_pct))
    end;

    v_peak_window_text := case
      when r.is_best_hour_window is true then 'BEST WINDOW'
      when r.is_best_hour_window is false then 'NON-PEAK'
      else 'N/A'
    end;

    v_anchor_best_pct := null;
    v_anchor_gap := null;
    if v_anchor_feeder_id is not null then
      select pm.percentile_performance
      into v_anchor_best_pct
      from public.post_metrics pm
      join public.posts p2 on p2.post_key = pm.post_key
      where p2.feeder_id = v_anchor_feeder_id
        and lower(pm.checkpoint) = v_cp
        and coalesce(p2.media_type,'') = coalesce(r.media_type,'')
        and pm.captured_business_date_ist between (v_day - 7) and v_day
        and pm.percentile_performance is not null
      order by pm.percentile_performance asc
      limit 1;

      if v_anchor_best_pct is not null then
        v_anchor_gap := v_surface_pct - v_anchor_best_pct;
      end if;
    end if;

    v_alert_type := case
      when v_surface_pct <= 10 then 'blaze'
      when v_surface_pct <= 25 then 'burn'
      else 'spark'
    end;

    v_stamp := format('@%s · %s · %s · %s %s',
      upper(coalesce(v_handle,'feeder')),
      upper(coalesce(r.media_type,'post')),
      upper(v_cp),
      case
        when abs(v_best_metric_value) >= 1000000 then round(v_best_metric_value::numeric/1000000,1)::text || 'M'
        when abs(v_best_metric_value) >= 1000 then round(v_best_metric_value::numeric/1000,1)::text || 'K'
        else v_best_metric_value::text
      end,
      upper(v_best_metric)
    );

    v_title := 'Post intelligence';
    v_body := format('%s at %s', upper(v_best_metric), upper(v_cp));

    v_payload := jsonb_build_object(
      'signal', 'standard',
      'best_metric', v_best_metric,
      'metrics', jsonb_build_object(
        'views', jsonb_build_object(
          'value', r.views,
          'baseline', r.baseline_views,
          'multiple', v_views_multiple,
          'percentile', r.views_percentile,
          'rank_feed', r.views_rank_feed,
          'best_in_last_n', r.best_in_last_n_views
        ),
        'likes', jsonb_build_object(
          'value', r.likes,
          'baseline', r.baseline_likes,
          'multiple', v_likes_multiple,
          'percentile', r.likes_percentile,
          'rank_feed', r.likes_rank_feed,
          'best_in_last_n', r.best_in_last_n_likes
        ),
        'comments', jsonb_build_object(
          'value', r.comments,
          'baseline', r.baseline_comments,
          'multiple', v_comments_multiple,
          'percentile', r.comments_percentile,
          'rank_feed', r.comments_rank_feed,
          'best_in_last_n', r.best_in_last_n_comments
        )
      ),
      'position', jsonb_build_object(
        'overall_percentile', v_surface_pct,
        'feed_rank', v_best_metric_rank_feed,
        'rank_all_time', r.rank_all_time
      ),
      'timing', jsonb_build_object(
        'hour', r.post_hour_ist,
        'peak_window', v_peak_window_text,
        'hour_percentile', r.hour_percentile_feeder,
        'hour_multiple', v_timing_hour_multiple
      ),
      'trajectory', case when v_cp='d1' then null else jsonb_build_object(
        'd1', v_d1,
        'd3', v_d3,
        'd7', v_d7,
        'delta', v_surface_delta
      ) end,
      'anchor', jsonb_build_object(
        'feeder_handle', v_anchor_handle,
        'best_pct', v_anchor_best_pct,
        'gap', v_anchor_gap
      ),
      'meta', jsonb_build_object(
        'handle', v_handle,
        'media_type', r.media_type,
        'checkpoint', v_cp,
        'post_url', r.post_url,
        'thumbnail_url', r.thumbnail_url,
        'anchor_business_date_ist', r.anchor_business_date_ist,
        'captured_business_date_ist', r.captured_business_date_ist
      )
    );

    v_dedupe := format('v3:%s:%s:%s', r.post_key, v_cp, v_day);

    perform public.fn_fire_upsert_row(
      v_dedupe, v_feed_id, p_feeder_id, r.post_key, v_cp, v_day,
      'slot_v3', 'own', 'A', v_alert_type,
      v_title, v_body, v_payload,
      v_surface_pct, v_surface_delta, v_handle, r.media_type,
      v_best_metric, v_best_metric_value, v_stamp
    );

    v_rows := v_rows + 1;
  end loop;

  return v_rows;
end;
$$;

-- One-time full-history backfill for rank_all_time.
with ranked as (
  select
    pm.post_key,
    pm.checkpoint,
    row_number() over (
      partition by p.feeder_id, lower(coalesce(p.media_type,'reel')), lower(pm.checkpoint)
      order by pm.metric_value desc nulls last, pm.computed_at desc, pm.post_key
    )::int as rank_num
  from public.post_metrics pm
  join public.posts p on p.post_key = pm.post_key
  where pm.metric_value is not null
)
update public.post_metrics pm
set rank_all_time = r.rank_num
from ranked r
where pm.post_key = r.post_key
  and pm.checkpoint = r.checkpoint
  and (pm.rank_all_time is distinct from r.rank_num);

-- One-time baseline hydration for recent rows with missing baseline columns.
update public.post_metrics pm
set
  baseline_views = coalesce(pm.baseline_views, fb.median_views),
  baseline_likes = coalesce(pm.baseline_likes, fb.median_likes),
  baseline_comments = coalesce(pm.baseline_comments, fb.median_comments)
from public.posts p
join public.feeder_baselines fb
  on fb.feeder_id = p.feeder_id
 and lower(fb.media_type) = lower(coalesce(p.media_type, 'reel'))
where p.post_key = pm.post_key
  and lower(fb.checkpoint) = lower(pm.checkpoint)
  and pm.captured_business_date_ist >= date '2026-02-17'
  and (
    pm.baseline_views is null
    or pm.baseline_likes is null
    or pm.baseline_comments is null
  );

-- Repair missing anchor_business_date_ist in existing v3 payloads.
update public.fire_alerts fa
set
  payload = jsonb_set(
    coalesce(fa.payload, '{}'::jsonb),
    '{meta,anchor_business_date_ist}',
    to_jsonb(coalesce(pm.business_date_ist, (p.posted_at at time zone 'Asia/Kolkata')::date)::text),
    true
  ),
  updated_at = now()
from public.post_metrics pm
join public.posts p on p.post_key = pm.post_key
where fa.dedupe_key like 'v3:%'
  and fa.post_key = pm.post_key
  and lower(fa.checkpoint) = lower(pm.checkpoint)
  and pm.captured_business_date_ist = fa.business_date_ist
  and coalesce(fa.payload->'meta'->>'anchor_business_date_ist','') = '';

commit;
