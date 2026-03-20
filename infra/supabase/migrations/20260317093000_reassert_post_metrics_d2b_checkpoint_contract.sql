begin;

-- Reassert the canonical post_metrics checkpoint contract.
-- Production drift showed the write trigger normalizing D2 -> D2B while the
-- table-level checkpoint constraint still rejected D2B rows.

do $$
begin
  if exists (
    select 1
    from pg_constraint
    where conname = 'post_metrics_checkpoint_check'
      and conrelid = 'public.post_metrics'::regclass
  ) then
    alter table public.post_metrics
      drop constraint post_metrics_checkpoint_check;
  end if;

  alter table public.post_metrics
    add constraint post_metrics_checkpoint_check
    check (checkpoint in ('d1', 'd2b', 'd3', 'd7', 'd21'));
end $$;

create or replace function public.tg_post_metrics_contract()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  -- Keep the D2 buffer internal-only. If an official D1 exists already,
  -- retain the replay as D2B; otherwise promote it to D1.
  if new.checkpoint = 'd2' then
    if exists (
      select 1
      from public.post_metrics pm
      where pm.post_key = new.post_key
        and pm.checkpoint = 'd1'
    ) then
      new.checkpoint := 'd2b';
    else
      new.checkpoint := 'd1';
    end if;
  end if;

  if new.captured_business_date_ist is null and new.business_date_ist is null then
    new.captured_business_date_ist := (coalesce(new.computed_at, now()) at time zone 'Asia/Kolkata')::date;
    new.business_date_ist := new.captured_business_date_ist;
  elsif new.captured_business_date_ist is null then
    new.captured_business_date_ist := new.business_date_ist;
  elsif new.business_date_ist is null then
    new.business_date_ist := new.captured_business_date_ist;
  elsif new.business_date_ist <> new.captured_business_date_ist then
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

drop trigger if exists trg_00_post_metrics_contract on public.post_metrics;
create trigger trg_00_post_metrics_contract
before insert or update on public.post_metrics
for each row
execute function public.tg_post_metrics_contract();

commit;
