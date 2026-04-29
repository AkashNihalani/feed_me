begin;

-- Focus Brain stores long-lived account/feed memory separately from raw post
-- fingerprints. The fingerprint remains a neutral description of the post;
-- the focus read is a versioned alignment pass against the current feeder MD.

create table if not exists public.feeder_focus (
  feeder_id bigint primary key references public.feeders(id) on delete cascade,
  feed_id bigint not null references public.feeds(id) on delete cascade,

  structured_patterns jsonb not null default '{}'::jsonb,
  evidence_summary jsonb not null default '{}'::jsonb,

  focus_md_common text not null default '',
  focus_md_reel text not null default '',
  focus_md_image text not null default '',
  focus_md_carousel text not null default '',

  focus_version integer not null default 0,
  prompt_version text not null default '',
  model_version text not null default '',
  source_hash text not null default '',

  focus_updated_at timestamptz,
  last_full_rebuild_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists feeder_focus_feed_idx
  on public.feeder_focus(feed_id, focus_updated_at desc nulls last);

create table if not exists public.feed_focus (
  feed_id bigint primary key references public.feeds(id) on delete cascade,

  structured_patterns jsonb not null default '{}'::jsonb,
  anchor_lens jsonb not null default '{}'::jsonb,
  feed_metrics jsonb not null default '{}'::jsonb,
  proof_posts jsonb not null default '{}'::jsonb,

  focus_md text not null default '',
  capsule_common text not null default '',
  capsule_reel text not null default '',
  capsule_image text not null default '',
  capsule_carousel text not null default '',
  capsule_anchor text not null default '',
  capsule_cross text not null default '',

  focus_version integer not null default 0,
  prompt_version text not null default '',
  model_version text not null default '',
  source_hash text not null default '',

  focus_updated_at timestamptz,
  last_full_rebuild_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.post_focus_reads (
  post_key text primary key references public.posts(post_key) on delete cascade,
  feeder_id bigint not null references public.feeders(id) on delete cascade,
  feed_id bigint not null references public.feeds(id) on delete cascade,
  media_type text,

  focus_read jsonb not null default '{}'::jsonb,
  feeder_focus_version integer not null default 0,
  fingerprint_hash text not null default '',
  prompt_version text not null default '',
  model_version text not null default '',

  generated_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists post_focus_reads_feeder_idx
  on public.post_focus_reads(feeder_id, media_type, updated_at desc);

alter table if exists public.signal_intelligence
  add column if not exists focus_context jsonb not null default '{}'::jsonb,
  add column if not exists focus_memory_candidate jsonb not null default '{}'::jsonb;

commit;
