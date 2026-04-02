begin;

create temporary table tmp_d1less_surface_posts on commit drop as
select
  src.post_key,
  p.posted_at,
  public.fn_checkpoint_due_at(p.posted_at, 1, 'Asia/Kolkata', 23, 30, 60) as d1_due_at
from (
  select pm.post_key
  from public.post_metrics pm
  where lower(pm.checkpoint) in ('d3', 'd7', 'd21')
  union
  select fa.post_key
  from public.fire_alerts fa
  where lower(fa.checkpoint) in ('d3', 'd7', 'd21')
) src
join public.posts p on p.post_key = src.post_key
where not exists (
  select 1
  from public.post_metrics pm1
  where pm1.post_key = src.post_key
    and lower(pm1.checkpoint) = 'd1'
);

delete from public.fire_alerts fa
where fa.post_key in (select post_key from tmp_d1less_surface_posts)
  and lower(fa.checkpoint) in ('d3', 'd7', 'd21');

delete from public.post_metrics pm
where pm.post_key in (select post_key from tmp_d1less_surface_posts)
  and lower(pm.checkpoint) in ('d3', 'd7', 'd21');

delete from public.checkpoint_jobs cj
where cj.post_key in (select post_key from tmp_d1less_surface_posts)
  and lower(cj.checkpoint) in ('d3', 'd7', 'd21');

update public.checkpoint_jobs cj
set status = 'pending',
    attempt = 0,
    next_run_at = coalesce(t.d1_due_at, now()),
    last_error = null,
    updated_at = now()
from tmp_d1less_surface_posts t
where cj.post_key = t.post_key
  and lower(cj.checkpoint) = 'd1'
  and t.posted_at >= (now() - interval '7 days')
  and coalesce(cj.status, '') <> 'done';

insert into public.checkpoint_jobs (post_key, checkpoint, status, attempt, next_run_at, last_error)
select
  t.post_key,
  'd1',
  'pending',
  0,
  coalesce(t.d1_due_at, now()),
  null
from tmp_d1less_surface_posts t
where t.posted_at >= (now() - interval '7 days')
  and not exists (
    select 1
    from public.checkpoint_jobs cj
    where cj.post_key = t.post_key
      and lower(cj.checkpoint) = 'd1'
  );

commit;
