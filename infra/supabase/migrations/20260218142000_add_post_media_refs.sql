-- Add canonical media references on posts for alert thumbnails + vector pipeline

alter table public.posts
  add column if not exists thumbnail_url text,
  add column if not exists display_url text,
  add column if not exists video_url text,
  add column if not exists carousel_urls jsonb,
  add column if not exists audio_url text,
  add column if not exists media_fetched_at timestamptz;

create index if not exists posts_media_fetched_idx
  on public.posts(media_fetched_at desc);

create index if not exists posts_posted_at_idx
  on public.posts(posted_at desc);
