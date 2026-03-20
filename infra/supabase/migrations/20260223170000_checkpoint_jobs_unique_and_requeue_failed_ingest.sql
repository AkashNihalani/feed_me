begin;

-- Restore the uniqueness contract required by enqueue_checkpoint_jobs ON CONFLICT (post_key, checkpoint).
-- If duplicates exist, keep the most recently updated row.
with ranked as (
  select id,
         row_number() over (
           partition by post_key, checkpoint
           order by updated_at desc nulls last, created_at desc nulls last, id desc
         ) as rn
  from public.checkpoint_jobs
)
delete from public.checkpoint_jobs cj
using ranked r
where cj.id = r.id
  and r.rn > 1;

create unique index if not exists checkpoint_jobs_post_key_checkpoint_uniq
  on public.checkpoint_jobs(post_key, checkpoint);

-- Ensure due-index exists for scheduler throughput.
create index if not exists checkpoint_jobs_due_idx
  on public.checkpoint_jobs(next_run_at, id)
  where status in ('pending','retry');

-- Requeue run jobs that were skipped/failed due to the missing unique-conflict contract.
-- Respect run_jobs_open_unique_idx by reopening only one latest row per (feeder_id, job_type)
-- and only when no other open row already exists for that pair.
with candidates as (
  select rj.*,
         row_number() over (
           partition by rj.feeder_id, rj.job_type
           order by rj.updated_at desc nulls last, rj.id desc
         ) as rn
  from public.run_jobs rj
  where rj.status in ('failed','skipped')
    and coalesce(rj.last_error,'') ilike '%no unique or exclusion constraint matching the ON CONFLICT specification%'
    and rj.updated_at >= now() - interval '14 days'
), pick as (
  select c.id
  from candidates c
  where c.rn = 1
    and not exists (
      select 1
      from public.run_jobs o
      where o.feeder_id = c.feeder_id
        and o.job_type = c.job_type
        and o.status in ('pending','running','retry')
    )
)
update public.run_jobs rj
set status = 'retry',
    next_run_at = now(),
    last_error = null,
    updated_at = now()
from pick p
where rj.id = p.id;

commit;
