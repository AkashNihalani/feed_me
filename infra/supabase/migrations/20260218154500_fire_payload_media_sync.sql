-- Keep fire alert payload media synced from posts as a system-level invariant.

create or replace function public.sync_fire_alert_media_payload_for_post(p_post_key text)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_thumb text;
  v_display text;
  v_rows int := 0;
begin
  if p_post_key is null or p_post_key = '' then
    return 0;
  end if;

  select p.thumbnail_url, p.display_url
  into v_thumb, v_display
  from public.posts p
  where p.post_key = p_post_key;

  update public.fire_alerts fa
  set payload = jsonb_set(
                  jsonb_set(coalesce(fa.payload, '{}'::jsonb), '{thumbnail_url}', to_jsonb(coalesce(v_thumb, v_display, '')), true),
                  '{display_url}', to_jsonb(coalesce(v_display, v_thumb, '')), true
                ),
      updated_at = now()
  where fa.post_key = p_post_key;

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

create or replace function public.tg_sync_fire_alert_media_payload()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  perform public.sync_fire_alert_media_payload_for_post(new.post_key);
  return new;
end;
$$;

drop trigger if exists trg_sync_fire_alert_media_payload on public.posts;

create trigger trg_sync_fire_alert_media_payload
  after insert or update of thumbnail_url, display_url
  on public.posts
  for each row
  execute function public.tg_sync_fire_alert_media_payload();

-- Backfill existing alerts once.
with keys as (
  select distinct post_key
  from public.fire_alerts
  where post_key is not null
)
select public.sync_fire_alert_media_payload_for_post(post_key)
from keys;
