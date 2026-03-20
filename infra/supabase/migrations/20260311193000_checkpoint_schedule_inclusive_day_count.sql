begin;

-- Product checkpoint semantics are inclusive of the post day:
-- D1 = post day, D3 = post day + 2, D7 = post day + 6, D21 = post day + 20.
create or replace function public.fn_checkpoint_due_at(
  p_posted_at timestamptz,
  p_days_after int,
  p_tz text default 'Asia/Kolkata',
  p_hour int default 18,
  p_minute int default 0
)
returns timestamptz
language plpgsql
stable
as $$
declare
  v_local_date date;
  v_target_local timestamp;
  v_target_utc timestamptz;
  v_inclusive_offset int;
begin
  if p_posted_at is null then
    return null;
  end if;

  v_inclusive_offset := greatest(0, coalesce(p_days_after, 0) - 1);
  v_local_date := ((p_posted_at at time zone p_tz)::date + v_inclusive_offset);
  v_target_local := (v_local_date::timestamp + make_interval(hours => p_hour, mins => p_minute));
  v_target_utc := (v_target_local at time zone p_tz);
  return v_target_utc;
end;
$$;

-- Repair recent open checkpoint jobs so they follow the corrected inclusive schedule.
with recalculated as (
  select
    cj.id,
    case cj.checkpoint
      when 'd3' then public.fn_checkpoint_due_at(p.posted_at, 3)
      when 'd7' then public.fn_checkpoint_due_at(p.posted_at, 7)
      when 'd21' then public.fn_checkpoint_due_at(p.posted_at, 21)
      else null
    end as due_at
  from public.checkpoint_jobs cj
  join public.posts p on p.post_key = cj.post_key
  where cj.checkpoint in ('d3', 'd7', 'd21')
    and (
      cj.status in ('pending', 'retry')
      or (
        cj.status = 'skipped'
        and coalesce(cj.last_error, '') like 'checkpoint skipped: historical checkpoint already passed%'
      )
    )
    and p.posted_at >= now() - interval '45 days'
)
update public.checkpoint_jobs cj
set next_run_at = coalesce(r.due_at, cj.next_run_at),
    status = case when r.due_at is null then 'skipped' else 'pending' end,
    last_error = case when r.due_at is null then cj.last_error else null end,
    updated_at = now()
from recalculated r
where cj.id = r.id;

commit;
