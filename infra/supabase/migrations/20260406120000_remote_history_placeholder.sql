begin;

-- This migration preserves remote migration history alignment.
-- The remote project already has version 20260406120000 recorded, but the
-- local repo no longer contains the original file. Keeping this as a no-op
-- placeholder lets future `supabase db push` runs proceed safely.

commit;
