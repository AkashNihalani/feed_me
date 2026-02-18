-- Backfill Phase 1 Fire alerts for already-computed hot checkpoints

create or replace function public.backfill_phase1_fire_alerts(p_hours int default 168)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  r record;
  v_rows int := 0;
  v_window interval := make_interval(hours => greatest(1, coalesce(p_hours, 168)));
begin
  for r in
    select pm.post_key, pm.checkpoint
    from public.post_metrics pm
    where pm.checkpoint in ('d1','d3','d7')
      and pm.percentile_tag in ('✅','🔥','🚀')
      and pm.computed_at >= now() - v_window
    order by pm.computed_at desc
  loop
    v_rows := v_rows + coalesce(public.enqueue_phase1_fire_alert(r.post_key, r.checkpoint), 0);
  end loop;

  return v_rows;
end;
$$;

select public.backfill_phase1_fire_alerts(168);
