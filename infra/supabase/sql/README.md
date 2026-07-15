# SQL Canon

Current SQL is intentionally small:

- `infra/supabase/migrations/20260329110000_minimal_brightdata_schema_reset.sql`
  Canonical bootstrap for the clean BrightData-era schema.
- `infra/supabase/migrations/20260715061500_two_discovery_checkpoint_pipeline.sql`
  Canonical two-discovery schedule and D1–D21 checkpoint contract.
- `infra/supabase/sql/engine_smoke_checks.sql`
  Quick health checks after cutover.
- `infra/supabase/sql/minimal_engine_audit.sql`
  Contract audit for required tables/functions.

Anything not listed here is legacy and should not be treated as source of truth.
