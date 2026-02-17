-- Terminology normalization: velocity -> percentile/checkpoint

-- 1) Rename velocity_tag -> percentile_tag if needed
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='post_metrics' AND column_name='velocity_tag'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='post_metrics' AND column_name='percentile_tag'
  ) THEN
    ALTER TABLE public.post_metrics
      RENAME COLUMN velocity_tag TO percentile_tag;
  END IF;
END $$;

ALTER TABLE public.post_metrics
  ADD COLUMN IF NOT EXISTS percentile_tag text;

-- Backfill in case both columns existed during transition
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='post_metrics' AND column_name='velocity_tag'
  ) THEN
    UPDATE public.post_metrics
    SET percentile_tag = COALESCE(percentile_tag, velocity_tag)
    WHERE percentile_tag IS NULL;
  END IF;
END $$;

-- 2) D21 hot gate should use percentile_tag terminology
CREATE OR REPLACE FUNCTION public.skip_unqualified_d21_jobs()
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows int := 0;
begin
  update public.checkpoint_jobs cj
  set status='skipped',
      last_error='D21 skipped because D7 was not hot (top 35%)',
      updated_at=now()
  where cj.status in ('pending','retry')
    and cj.checkpoint='d21'
    and cj.next_run_at <= now()
    and not exists (
      select 1 from public.post_metrics pm
      where pm.post_key = cj.post_key
        and pm.checkpoint='d7'
        and pm.percentile_tag in ('✅','🔥','🚀')
    );
  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;
