begin;

-- Ensure ON CONFLICT (dedupe_key) is always valid for fire_alert writes.
-- Remove duplicate dedupe keys first, keeping newest row.
with ranked as (
  select id,
         row_number() over (
           partition by dedupe_key
           order by created_at desc nulls last, id desc
         ) as rn
  from public.fire_alerts
  where dedupe_key is not null
)
delete from public.fire_alerts fa
using ranked r
where fa.id = r.id
  and r.rn > 1;

create unique index if not exists fire_alerts_dedupe_key_uniq
  on public.fire_alerts(dedupe_key);

commit;
