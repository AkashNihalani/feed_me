-- Permanent checkpoint normalization
-- Goal: D1 always represents the post's first official checkpoint row.
-- D2 becomes internal-only fallback marker (stored as d2b when D1 already exists).

-- 1) Ensure checkpoint constraint supports d2b and no raw d2 persistence.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'post_metrics_checkpoint_check'
      AND conrelid = 'public.post_metrics'::regclass
  ) THEN
    ALTER TABLE public.post_metrics DROP CONSTRAINT post_metrics_checkpoint_check;
  END IF;

  ALTER TABLE public.post_metrics
    ADD CONSTRAINT post_metrics_checkpoint_check
    CHECK (checkpoint IN ('d1','d2b','d3','d7','d21'));
END $$;

-- 2) Normalize incoming checkpoint rows at write time (DB-level, permanent).
CREATE OR REPLACE FUNCTION public.tg_normalize_post_metrics_checkpoint()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.checkpoint = 'd2' THEN
    IF EXISTS (
      SELECT 1
      FROM public.post_metrics pm
      WHERE pm.post_key = NEW.post_key
        AND pm.checkpoint = 'd1'
    ) THEN
      -- D1 already exists: keep fallback internally as d2b
      NEW.checkpoint := 'd2b';
    ELSE
      -- No D1 yet: promote fallback to official D1
      NEW.checkpoint := 'd1';
    END IF;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_normalize_post_metrics_checkpoint ON public.post_metrics;
CREATE TRIGGER trg_normalize_post_metrics_checkpoint
BEFORE INSERT OR UPDATE OF checkpoint
ON public.post_metrics
FOR EACH ROW
EXECUTE FUNCTION public.tg_normalize_post_metrics_checkpoint();

-- 3) One-time cleanup of existing rows.
-- 3a) Where d1 already exists, convert d2 -> d2b.
UPDATE public.post_metrics pm
SET checkpoint = 'd2b'
WHERE pm.checkpoint = 'd2'
  AND EXISTS (
    SELECT 1
    FROM public.post_metrics d1
    WHERE d1.post_key = pm.post_key
      AND d1.checkpoint = 'd1'
  );

-- 3b) Where d1 does not exist, convert d2 -> d1.
UPDATE public.post_metrics pm
SET checkpoint = 'd1'
WHERE pm.checkpoint = 'd2'
  AND NOT EXISTS (
    SELECT 1
    FROM public.post_metrics d1
    WHERE d1.post_key = pm.post_key
      AND d1.checkpoint = 'd1'
  );
