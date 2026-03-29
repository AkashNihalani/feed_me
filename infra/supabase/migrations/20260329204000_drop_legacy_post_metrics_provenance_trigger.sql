begin;

drop trigger if exists trg_set_post_metrics_provenance on public.post_metrics;
drop function if exists public.tg_set_post_metrics_provenance();

commit;
