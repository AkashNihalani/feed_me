begin;

drop trigger if exists trg_format_fire_alert_contract on public.fire_alerts;
drop trigger if exists trg_fire_alerts_score_guard on public.fire_alerts;
drop trigger if exists trg_zz_fire_alert_rank_sync on public.fire_alerts;

drop function if exists public.fn_format_alert();
drop function if exists public.fn_fire_alert_rank_sync();
drop function if exists public.trg_fire_alerts_score_guard();

alter table if exists public.fire_alerts
  alter column family_group drop not null;

alter table if exists public.fire_alerts
  drop constraint if exists fire_alerts_alert_type_check;

alter table if exists public.fire_alerts
  add constraint fire_alerts_alert_type_check
  check (alert_type in ('tracking', 'spark', 'burn', 'blaze'));

alter table if exists public.fire_alerts
  drop constraint if exists fire_alerts_status_check;

alter table if exists public.fire_alerts
  add constraint fire_alerts_status_check
  check (status in ('new', 'sent', 'dismissed', 'archived', 'dropped', 'error'));

alter table if exists public.fire_alerts
  drop constraint if exists fire_alerts_checkpoint_check;

alter table if exists public.fire_alerts
  add constraint fire_alerts_checkpoint_check
  check (checkpoint in ('d1', 'd3', 'd7', 'd21'));

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
  v_best_percentile int;
  v_baseline bigint;
  v_multiple numeric;
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
    );

  for r in
    select
      p.post_key,
      lower(coalesce(p.media_type, 'unknown')) as media_type,
      pm.views,
      pm.likes,
      pm.comments,
      pm.views_percentile,
      pm.likes_percentile,
      pm.comments_percentile,
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
      case when public.fn_is_hot_percentile(pm.percentile_performance) then 0 else 1 end,
      pm.percentile_performance asc,
      p.posted_at desc nulls last
  loop
    v_visible := (v_cp in ('d1', 'd3', 'd7')) or coalesce(r.is_hot, false);
    if not v_visible then
      continue;
    end if;

    v_best_metric := 'views';
    v_best_value := r.views;
    v_best_percentile := coalesce(r.views_percentile, 101);
    v_baseline := r.median_views;

    if coalesce(r.likes_percentile, 101) < v_best_percentile then
      v_best_metric := 'likes';
      v_best_value := r.likes;
      v_best_percentile := coalesce(r.likes_percentile, 101);
      v_baseline := r.median_likes;
    end if;

    if coalesce(r.comments_percentile, 101) < v_best_percentile then
      v_best_metric := 'comments';
      v_best_value := r.comments;
      v_best_percentile := coalesce(r.comments_percentile, 101);
      v_baseline := r.median_comments;
    end if;

    v_multiple := case
      when v_best_value is not null and coalesce(v_baseline, 0) > 0
        then round(v_best_value::numeric / v_baseline::numeric, 2)
      else null
    end;

    v_alert_type := case
      when coalesce(r.is_hot, false) and r.percentile_performance <= 10 then 'blaze'
      when coalesce(r.is_hot, false) and r.percentile_performance <= 25 then 'burn'
      when coalesce(r.is_hot, false) then 'spark'
      else 'tracking'
    end;

    v_body := case
      when not coalesce(r.is_hot, false) and v_multiple is not null then
        format(
          '@%s checked in at %s with %s %s, %sx its current baseline.',
          upper(coalesce(v_handle, 'feed')),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(v_best_metric),
          to_char(v_multiple, 'FM999999990.00')
        )
      when not coalesce(r.is_hot, false) then
        format(
          '@%s checked in at %s with %s %s.',
          upper(coalesce(v_handle, 'feed')),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(v_best_metric)
        )
      when v_cp = 'd1' and v_multiple is not null then
        format(
          '@%s hit the top %s%% at %s with %s %s, %sx its usual %s.',
          upper(coalesce(v_handle, 'feed')),
          coalesce(r.percentile_performance, 0),
          upper(v_cp),
          coalesce(v_best_value, 0),
          upper(v_best_metric),
          to_char(v_multiple, 'FM999999990.00'),
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
      v_best_metric,
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
begin
  for r in
    select distinct p.feeder_id, pm.checkpoint, pm.business_date_ist
    from public.post_metrics pm
    join public.posts p on p.post_key = pm.post_key
    where pm.checkpoint in ('d1', 'd3', 'd7', 'd21')
  loop
    perform public.fn_process_checkpoint(r.feeder_id, r.checkpoint, r.business_date_ist);
  end loop;
end $$;

commit;
