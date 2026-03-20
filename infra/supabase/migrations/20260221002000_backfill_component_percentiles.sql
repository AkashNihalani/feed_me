-- Backfill component percentiles for existing rows.
-- Uses no-op assignment to invoke tg_compute_post_metrics.

update public.post_metrics
set likes = likes,
    comments = comments,
    views = views
where checkpoint in ('d1','d3','d7','d21')
  and (
    likes_percentile is null
    or comments_percentile is null
    or views_percentile is null
  );
