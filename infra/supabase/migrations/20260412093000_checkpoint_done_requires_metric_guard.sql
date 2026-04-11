begin;

create or replace function public.set_checkpoint_job_result(
  p_job_id bigint,
  p_status text,
  p_attempt int default null,
  p_next_run_at timestamptz default null,
  p_error text default null
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_job record;
  v_status text := lower(coalesce(p_status, ''));
  v_fallback_status text;
  v_error text;
begin
  if v_status = 'done' then
    select cj.post_key, cj.checkpoint, cj.next_run_at
    into v_job
    from public.checkpoint_jobs cj
    where cj.id = p_job_id
    for update;

    if not found then
      return;
    end if;

    if exists (
      select 1
      from public.post_metrics pm
      where pm.post_key = v_job.post_key
        and lower(pm.checkpoint) = lower(v_job.checkpoint)
    ) then
      update public.checkpoint_jobs
      set status = 'done',
          attempt = coalesce(p_attempt, attempt),
          next_run_at = coalesce(p_next_run_at, next_run_at),
          last_error = case when p_error is null then null else left(p_error, 1000) end,
          updated_at = now()
      where id = p_job_id;
    else
      v_fallback_status := case
        when coalesce(v_job.next_run_at, now()) > now() then 'pending'
        else 'retry'
      end;
      v_error := coalesce(
        nullif(p_error, ''),
        'Rejected done transition: checkpoint metric row missing'
      );

      update public.checkpoint_jobs
      set status = v_fallback_status,
          attempt = coalesce(p_attempt, attempt),
          next_run_at = case
            when p_next_run_at is not null then p_next_run_at
            when v_job.next_run_at is not null and v_job.next_run_at > now() then v_job.next_run_at
            else now()
          end,
          last_error = left(v_error, 1000),
          updated_at = now()
      where id = p_job_id;
    end if;

    return;
  end if;

  update public.checkpoint_jobs
  set status = p_status,
      attempt = coalesce(p_attempt, attempt),
      next_run_at = coalesce(p_next_run_at, next_run_at),
      last_error = case when p_error is null then null else left(p_error, 1000) end,
      updated_at = now()
  where id = p_job_id;
end;
$$;

create or replace function public.tg_checkpoint_done_requires_metric()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if lower(coalesce(new.status, '')) = 'done'
     and not exists (
       select 1
       from public.post_metrics pm
       where pm.post_key = new.post_key
         and lower(pm.checkpoint) = lower(new.checkpoint)
     ) then
    new.status := case
      when coalesce(new.next_run_at, now()) > now() then 'pending'
      else 'retry'
    end;
    new.next_run_at := case
      when new.next_run_at is not null and new.next_run_at > now() then new.next_run_at
      else now()
    end;
    new.last_error := 'Rejected done transition: checkpoint metric row missing';
    new.updated_at := now();
  end if;

  return new;
end;
$$;

drop trigger if exists trg_checkpoint_done_requires_metric on public.checkpoint_jobs;

create trigger trg_checkpoint_done_requires_metric
before insert or update of status, post_key, checkpoint on public.checkpoint_jobs
for each row
execute function public.tg_checkpoint_done_requires_metric();

update public.checkpoint_jobs cj
set status = case
      when coalesce(cj.next_run_at, now()) > now() then 'pending'
      else 'retry'
    end,
    attempt = 0,
    next_run_at = case
      when coalesce(cj.next_run_at, now()) > now() then cj.next_run_at
      else now()
    end,
    last_error = 'Recovered done checkpoint without metric: requeued for metric capture',
    resurrection_count = coalesce(cj.resurrection_count, 0) + 1,
    updated_at = now()
from public.posts p
where p.post_key = cj.post_key
  and cj.status = 'done'
  and lower(coalesce(cj.checkpoint, '')) in ('d1', 'd3', 'd7', 'd21')
  and coalesce(
    public.fn_checkpoint_job_business_day(p.posted_at, cj.checkpoint, 'Asia/Kolkata'),
    (coalesce(cj.next_run_at, now()) at time zone 'Asia/Kolkata')::date
  ) >= ((now() at time zone 'Asia/Kolkata')::date - 14)
  and not exists (
    select 1
    from public.post_metrics pm
    where pm.post_key = cj.post_key
      and lower(pm.checkpoint) = lower(cj.checkpoint)
  );

commit;
