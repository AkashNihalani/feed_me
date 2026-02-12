-- Add missing total_posts_caught column to scheduled_scrapes table
-- This column is required by the cron trigger to track cumulative post counts

ALTER TABLE public.scheduled_scrapes 
ADD COLUMN IF NOT EXISTS total_posts_caught integer DEFAULT 0;
