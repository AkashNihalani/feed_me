alter table if exists public.feeders
  add column if not exists profile_pic_fetched_at timestamptz;
