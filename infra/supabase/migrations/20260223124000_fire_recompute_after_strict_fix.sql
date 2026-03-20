begin;

-- Recompute post_metrics-derived fields with strict trigger logic.
update public.post_metrics
set likes = likes
where computed_at >= now() - interval '45 days';

-- Clear prior slot_v3 rows to avoid unique-key collisions during strict rebuild.
delete from public.fire_alerts
where signal_code = 'slot_v3'
  and context = 'own';

-- Rebuild recent fire alerts from strict checkpoint compute.
do $$
declare
  d date;
  r record;
  cp text;
  v_from date := ((now() at time zone 'Asia/Kolkata')::date - 14);
  v_to date := ((now() at time zone 'Asia/Kolkata')::date - 1);
begin
  for d in select gs::date from generate_series(v_from, v_to, interval '1 day') gs loop
    for r in select id from public.feeders where status = 'active' loop
      foreach cp in array array['d1','d3','d7','d21'] loop
        perform public.fn_process_checkpoint(r.id, cp, d);
      end loop;
    end loop;
  end loop;
end $$;

commit;
