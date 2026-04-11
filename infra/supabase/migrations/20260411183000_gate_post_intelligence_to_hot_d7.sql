begin;

-- Post intelligence is only signal-ready after a post proves hot at D7.
-- Remove any earlier checkpoint extractions and any D7 rows that missed the
-- hot gate so Fire cannot surface premature intelligence chrome.
delete from public.post_intelligence pi
where not exists (
  select 1
  from public.post_metrics pm
  where pm.post_key = pi.post_key
    and lower(pm.checkpoint) = 'd7'
    and public.fn_is_hot_percentile(pm.percentile_performance)
);

commit;
