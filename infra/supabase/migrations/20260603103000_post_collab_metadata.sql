alter table if exists public.posts
  add column if not exists related_handles jsonb not null default '[]'::jsonb;

alter table if exists public.posts
  add column if not exists collab_post boolean;

create index if not exists posts_collab_post_idx
  on public.posts (collab_post)
  where collab_post is true;

comment on column public.posts.related_handles is
  'Handles surfaced by scraper metadata around post ownership and collaborators. Used by D7 reads to flag potentially borrowed reach.';

comment on column public.posts.collab_post is
  'True when scraper metadata indicates the post may be a collaboration beyond the tracked account.';
