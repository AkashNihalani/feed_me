-- Fix foreign key constraint to allow schedule deletion
-- When a schedule is deleted, set schedule_id to NULL on related scrapes (preserving history)

-- 1. Drop the existing constraint
ALTER TABLE public.scrapes 
DROP CONSTRAINT IF EXISTS scrapes_schedule_id_fkey;

-- 2. Re-add with ON DELETE SET NULL
ALTER TABLE public.scrapes 
ADD CONSTRAINT scrapes_schedule_id_fkey 
FOREIGN KEY (schedule_id) REFERENCES public.scheduled_scrapes(id) ON DELETE SET NULL;
