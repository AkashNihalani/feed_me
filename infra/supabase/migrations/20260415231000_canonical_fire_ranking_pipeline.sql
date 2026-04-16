begin;

alter table if exists public.post_metrics
  add column if not exists ranking_metric text,
  add column if not exists ranking_multiple numeric(12,4),
  add column if not exists percentile_performance_exact numeric(8,4),
  add column if not exists views_baseline bigint,
  add column if not exists likes_baseline bigint,
  add column if not exists comments_baseline bigint,
  add column if not exists views_multiple numeric(12,4),
  add column if not exists likes_multiple numeric(12,4),
  add column if not exists comments_multiple numeric(12,4),
  add column if not exists hour_multiple numeric(12,4);

alter table if exists public.post_metrics
  drop constraint if exists post_metrics_ranking_metric_check;

alter table if exists public.post_metrics
  add constraint post_metrics_ranking_metric_check
  check (
    ranking_metric is null
    or lower(ranking_metric) in ('views', 'likes', 'comments')
  );

drop trigger if exists trg_refresh_metric_baselines on public.post_metrics;

commit;
