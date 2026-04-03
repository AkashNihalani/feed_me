begin;

update public.checkpoint_jobs
set status = 'retry',
    attempt = greatest(coalesce(attempt, 0), 1),
    next_run_at = now(),
    last_error = null,
    updated_at = now()
where status = 'skipped'
  and (
    lower(coalesce(last_error, '')) like 'hard-skip:bright data json parse error:%'
    or lower(coalesce(last_error, '')) like 'hard-skip:extra data:%'
    or lower(coalesce(last_error, '')) like 'hard-skip:expecting value:%'
    or lower(coalesce(last_error, '')) like 'hard-skip:json decode%'
    or lower(coalesce(last_error, '')) like 'hard-skip:unterminated string%'
  );

commit;
