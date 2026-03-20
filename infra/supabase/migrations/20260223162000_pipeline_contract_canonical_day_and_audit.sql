begin;

-- 1) Canonical day contract for post_metrics
-- business_date_ist and captured_business_date_ist must represent the same IST business day.

update public.post_metrics
set captured_business_date_ist = coalesce(captured_business_date_ist, business_date_ist, (coalesce(computed_at, now()) at time zone 'Asia/Kolkata')::date),
    business_date_ist = coalesce(captured_business_date_ist, business_date_ist, (coalesce(computed_at, now()) at time zone 'Asia/Kolkata')::date)
where captured_business_date_ist is null
   or business_date_ist is null
   or captured_business_date_ist <> business_date_ist;

create or replace function public.tg_post_metrics_contract()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  -- Keep legacy d2 internal-only and deterministic.
  if new.checkpoint = 'd2' then
    if exists (
      select 1 from public.post_metrics pm
      where pm.post_key = new.post_key
        and pm.checkpoint = 'd1'
    ) then
      new.checkpoint := 'd2b';
    else
      new.checkpoint := 'd1';
    end if;
  end if;

  -- Canonical business-day stamp.
  if new.captured_business_date_ist is null and new.business_date_ist is null then
    new.captured_business_date_ist := (coalesce(new.computed_at, now()) at time zone 'Asia/Kolkata')::date;
    new.business_date_ist := new.captured_business_date_ist;
  elsif new.captured_business_date_ist is null then
    new.captured_business_date_ist := new.business_date_ist;
  elsif new.business_date_ist is null then
    new.business_date_ist := new.captured_business_date_ist;
  elsif new.business_date_ist <> new.captured_business_date_ist then
    -- captured_business_date_ist is the worker-supplied checkpoint day; keep it authoritative.
    new.business_date_ist := new.captured_business_date_ist;
  end if;

  if new.d1_source is null then
    if new.checkpoint = 'd1' then
      new.d1_source := 'on_time';
    elsif new.checkpoint = 'd2b' then
      new.d1_source := 'from_d2b';
    end if;
  end if;

  return new;
end;
$$;

drop trigger if exists trg_normalize_post_metrics_checkpoint on public.post_metrics;
drop trigger if exists trg_00_post_metrics_contract on public.post_metrics;
create trigger trg_00_post_metrics_contract
before insert or update on public.post_metrics
for each row
execute function public.tg_post_metrics_contract();

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'post_metrics_business_date_ist_not_null'
      and conrelid = 'public.post_metrics'::regclass
  ) then
    alter table public.post_metrics
      add constraint post_metrics_business_date_ist_not_null
      check (business_date_ist is not null);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'post_metrics_captured_business_date_ist_not_null'
      and conrelid = 'public.post_metrics'::regclass
  ) then
    alter table public.post_metrics
      add constraint post_metrics_captured_business_date_ist_not_null
      check (captured_business_date_ist is not null);
  end if;
end $$;

create or replace view public.v_posts_canonical as
select
  p.post_key,
  p.feeder_id,
  p.post_url,
  p.media_type,
  p.posted_at,
  (p.posted_at at time zone 'Asia/Kolkata')::date as posted_date_ist,
  p.created_at,
  p.updated_at
from public.posts p;

-- 2) Daily pipeline audit table + function
create table if not exists public.pipeline_audit_daily (
  id bigserial primary key,
  business_date_ist date not null,
  generated_at timestamptz not null default now(),
  total_posts int not null default 0,
  d1_rows int not null default 0,
  d3_rows int not null default 0,
  d7_rows int not null default 0,
  d21_rows int not null default 0,
  d2b_rows int not null default 0,
  non_d1_missing_d1 int not null default 0,
  metrics_without_posts int not null default 0,
  null_business_date_rows int not null default 0,
  mismatched_business_date_rows int not null default 0,
  notes jsonb not null default '{}'::jsonb,
  unique (business_date_ist)
);

create or replace function public.fn_pipeline_audit_day(p_day date default null)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_day date := coalesce(p_day, (now() at time zone 'Asia/Kolkata')::date);
  v_total_posts int := 0;
  v_d1 int := 0;
  v_d3 int := 0;
  v_d7 int := 0;
  v_d21 int := 0;
  v_d2b int := 0;
  v_non_d1_missing_d1 int := 0;
  v_metrics_without_posts int := 0;
  v_null_day int := 0;
  v_mismatch_day int := 0;
  v_payload jsonb;
begin
  select count(*)::int
  into v_total_posts
  from public.v_posts_canonical p
  where p.posted_date_ist = v_day;

  select
    count(*) filter (where pm.checkpoint = 'd1')::int,
    count(*) filter (where pm.checkpoint = 'd3')::int,
    count(*) filter (where pm.checkpoint = 'd7')::int,
    count(*) filter (where pm.checkpoint = 'd21')::int,
    count(*) filter (where pm.checkpoint = 'd2b')::int
  into v_d1, v_d3, v_d7, v_d21, v_d2b
  from public.post_metrics pm
  where pm.business_date_ist = v_day;

  select count(*)::int
  into v_non_d1_missing_d1
  from public.post_metrics pm
  where pm.business_date_ist = v_day
    and pm.checkpoint in ('d3','d7','d21')
    and not exists (
      select 1 from public.post_metrics d1
      where d1.post_key = pm.post_key
        and d1.checkpoint = 'd1'
    );

  select count(*)::int
  into v_metrics_without_posts
  from public.post_metrics pm
  where pm.business_date_ist = v_day
    and not exists (
      select 1 from public.posts p where p.post_key = pm.post_key
    );

  select count(*)::int
  into v_null_day
  from public.post_metrics pm
  where pm.business_date_ist is null
     or pm.captured_business_date_ist is null;

  select count(*)::int
  into v_mismatch_day
  from public.post_metrics pm
  where pm.business_date_ist is not null
    and pm.captured_business_date_ist is not null
    and pm.business_date_ist <> pm.captured_business_date_ist;

  v_payload := jsonb_build_object(
    'business_date_ist', v_day,
    'total_posts', v_total_posts,
    'd1_rows', v_d1,
    'd3_rows', v_d3,
    'd7_rows', v_d7,
    'd21_rows', v_d21,
    'd2b_rows', v_d2b,
    'non_d1_missing_d1', v_non_d1_missing_d1,
    'metrics_without_posts', v_metrics_without_posts,
    'null_business_date_rows', v_null_day,
    'mismatched_business_date_rows', v_mismatch_day
  );

  insert into public.pipeline_audit_daily (
    business_date_ist,
    total_posts,
    d1_rows,
    d3_rows,
    d7_rows,
    d21_rows,
    d2b_rows,
    non_d1_missing_d1,
    metrics_without_posts,
    null_business_date_rows,
    mismatched_business_date_rows,
    notes,
    generated_at
  )
  values (
    v_day,
    v_total_posts,
    v_d1,
    v_d3,
    v_d7,
    v_d21,
    v_d2b,
    v_non_d1_missing_d1,
    v_metrics_without_posts,
    v_null_day,
    v_mismatch_day,
    v_payload,
    now()
  )
  on conflict (business_date_ist)
  do update set
    total_posts = excluded.total_posts,
    d1_rows = excluded.d1_rows,
    d3_rows = excluded.d3_rows,
    d7_rows = excluded.d7_rows,
    d21_rows = excluded.d21_rows,
    d2b_rows = excluded.d2b_rows,
    non_d1_missing_d1 = excluded.non_d1_missing_d1,
    metrics_without_posts = excluded.metrics_without_posts,
    null_business_date_rows = excluded.null_business_date_rows,
    mismatched_business_date_rows = excluded.mismatched_business_date_rows,
    notes = excluded.notes,
    generated_at = now();

  return v_payload;
end;
$$;

commit;
