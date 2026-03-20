-- Shift checkpoint execution window from 23:30 IST to 18:00 IST.
-- Daily scrape cron remains unchanged.

begin;

-- 1) New default due-time for future checkpoint enqueues
create or replace function public.enqueue_checkpoint_jobs(
  p_post_key text,
  p_posted_at timestamptz,
  p_tz text default 'Asia/Kolkata',
  p_hour int default 18,
  p_minute int default 0
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
begin
  insert into public.checkpoint_jobs (post_key, checkpoint, status, next_run_at)
  values
    (p_post_key, 'd3', 'pending', public.fn_checkpoint_due_at(p_posted_at, 3, p_tz, p_hour, p_minute)),
    (p_post_key, 'd7', 'pending', public.fn_checkpoint_due_at(p_posted_at, 7, p_tz, p_hour, p_minute)),
    (p_post_key, 'd21', 'pending', public.fn_checkpoint_due_at(p_posted_at, 21, p_tz, p_hour, p_minute))
  on conflict do nothing;
  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

-- 2) Shift existing future pending/retry checkpoint jobs to 18:00 IST of their checkpoint business day
update public.checkpoint_jobs cj
set next_run_at = (
  (
    (
      ((p.posted_at at time zone 'Asia/Kolkata')::date +
       case cj.checkpoint
         when 'd3' then 3
         when 'd7' then 7
         when 'd21' then 21
         else 0
       end
      )::timestamp
      + interval '18 hours'
    ) at time zone 'Asia/Kolkata'
  )
),
updated_at = now()
from public.posts p
where p.post_key = cj.post_key
  and cj.checkpoint in ('d3','d7','d21')
  and cj.status in ('pending','retry')
  and cj.next_run_at > now();

commit;
