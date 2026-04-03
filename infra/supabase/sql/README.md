# SQL Canon

Current SQL is intentionally small:

- `/Users/Akash/feed_me/infra/supabase/migrations/20260329110000_minimal_brightdata_schema_reset.sql`
  Canonical bootstrap for the clean BrightData-era schema.
- `/Users/Akash/feed_me/infra/supabase/sql/brightdata_schedule_source_of_truth.sql`
  Readable schedule contract.
- `/Users/Akash/feed_me/infra/supabase/sql/engine_cron_setup_template.sql`
  pg_cron template for the active enqueue/watchdog jobs.
- `/Users/Akash/feed_me/infra/supabase/sql/engine_smoke_checks.sql`
  Quick health checks after cutover.
- `/Users/Akash/feed_me/infra/supabase/sql/minimal_engine_audit.sql`
  Contract audit for required tables/functions.

Anything not listed here is legacy and should not be treated as source of truth.
