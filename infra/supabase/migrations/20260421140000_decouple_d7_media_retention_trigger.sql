begin;

create or replace function public.tg_schedule_fire_media_retention_from_d7()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  -- Keep D7/D21 checkpoint writes off the critical Fire path. Retention and
  -- media rollover are handled by the worker/background repair flows instead.
  return new;
end;
$$;

commit;
