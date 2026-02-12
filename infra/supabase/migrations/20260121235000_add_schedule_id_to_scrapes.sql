-- Add schedule_id column to scrapes table for linking scheduled runs
-- This enables tracking total_posts_caught per schedule after async completion

ALTER TABLE public.scrapes 
ADD COLUMN IF NOT EXISTS schedule_id integer REFERENCES public.scheduled_scrapes(id);

-- Index for faster lookups when updating schedule stats
CREATE INDEX IF NOT EXISTS idx_scrapes_schedule_id ON public.scrapes(schedule_id);
