begin;

-- Forward policy:
-- - thumbnails/display images stay for the 90-day post window
-- - preview clips stay for 30 days from post date
-- - heavy/full video helper assets remain short-lived
--
-- This migration intentionally does not rewrite existing post_media_assets rows,
-- so already captured 5-second previews keep their current objects/deadlines.
create or replace function public.fn_post_media_rollover_deadline(
  p_posted_at timestamptz,
  p_asset_role text
)
returns timestamptz
language sql
stable
as $$
  select (
    case
      when lower(coalesce(p_asset_role, '')) in ('thumbnail', 'display')
        then coalesce(p_posted_at, now()) + interval '90 days'
      when lower(coalesce(p_asset_role, '')) = 'preview_5s'
        then greatest(
          coalesce(p_posted_at, now()),
          timestamptz '2026-04-14 00:00:00+05:30'
        ) + interval '30 days'
      else coalesce(p_posted_at, now()) + interval '1 day'
    end
  );
$$;

create or replace function public.fn_post_media_rollover_deadline_for_post(
  p_post_key text,
  p_posted_at timestamptz,
  p_asset_role text
)
returns timestamptz
language sql
stable
as $$
  select public.fn_post_media_rollover_deadline(p_posted_at, p_asset_role);
$$;

commit;
