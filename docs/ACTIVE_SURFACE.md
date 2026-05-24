# Active Surface

This project is being actively maintained around a small, stable contract:

## Product contract

- `Bright Data` is the ingestion backend.
- `Computation` stays untouched.
- `Fire alerts` stay untouched.
- `Daily discovery` runs at `12:05 AM IST` and `12:05 PM IST`.
- `Daily discovery` uses a `2-day` overlap.
- `Checkpoint jobs` are due from exact post age and rounded into `60-minute` buckets.
- `Repair lane` remains active for self-healing.

## Active backend files

- `apps/worker/app/brightdata.py`
- `apps/worker/app/scraper.py`
- `apps/worker/app/pure_engine.py`
- `apps/web/src/app/api/feed/route.ts`
- `apps/web/src/app/api/feed/dashboard/route.ts`

## Active SQL files

- `infra/supabase/sql/brightdata_schedule_source_of_truth.sql`
- `infra/supabase/migrations/20260327113000_brightdata_exact_checkpoint_schedule.sql`

## Active frontend surfaces

- `apps/web/src/app/page.tsx`
- `apps/web/src/app/fire/page.tsx`
- `apps/web/src/app/profile/page.tsx`
- `apps/web/src/components/TopNav.tsx`
- `apps/web/src/components/BottomNav.tsx`

## Notes on legacy migrations

Historical migrations are kept because production has already evolved through
them. They are not the size problem, and deleting them blindly would make the
project less safe. The canonical files above are the only ones we should treat
as day-to-day references.
