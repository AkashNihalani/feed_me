# Active Surface

This project is being actively maintained around a small, stable contract:

## Product contract

- `Apify` is the ingestion backend.
- `Computation` stays untouched.
- `Fire alerts` stay untouched.
- `Daily discovery` runs at `12:05 AM IST`.
- `Daily discovery` uses a `3-day` lookback.
- `Checkpoint jobs` are due at `6:30 PM IST`.
- `Repair lane` remains active for self-healing.

## Active backend files

- `/Users/Akash/feed_me/apps/worker/app/apify.py`
- `/Users/Akash/feed_me/apps/worker/app/pure_engine.py`
- `/Users/Akash/feed_me/apps/web/src/app/api/feed/route.ts`
- `/Users/Akash/feed_me/apps/web/src/app/api/feed/dashboard/route.ts`

## Active SQL files

- `/Users/Akash/feed_me/infra/supabase/sql/apify_schedule_source_of_truth.sql`
- `/Users/Akash/feed_me/infra/supabase/migrations/20260316200000_apify_schedule_contract_reset.sql`

## Active frontend surfaces

- `/Users/Akash/feed_me/apps/web/src/app/page.tsx`
- `/Users/Akash/feed_me/apps/web/src/app/fire/page.tsx`
- `/Users/Akash/feed_me/apps/web/src/app/profile/page.tsx`
- `/Users/Akash/feed_me/apps/web/src/components/TopNav.tsx`
- `/Users/Akash/feed_me/apps/web/src/components/BottomNav.tsx`

## Notes on legacy migrations

Historical migrations are kept because production has already evolved through
them. They are not the size problem, and deleting them blindly would make the
project less safe. The canonical files above are the only ones we should treat
as day-to-day references.
