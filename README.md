# FeedMe (Clean Rebuild)

Pure Supabase engine + Vercel frontend.

## Core model
- Feed = workspace
- Feeders = handles in a feed (1 to 15)
- Discovery runs every 12 hours with a 2-day overlap
- Official checkpoints: d1, d3, d7 scheduled from actual post age in 60-minute buckets
- D21 is queued for hot D7 posts, but intelligence is no longer gated on D7 tags
- Fire tracking cards come from deterministic checkpoint metrics
- Signal intelligence is detected with SQL/data rules, then explained from cached post fingerprints by the `resolve_signal_intelligence` worker mode
- Signal alert rules and guardrails live in `docs/signal-alert-rulebook.md`

## Structure
- `infra/supabase`: schema, queue functions, audit scripts
- `apps/worker`: pure engine processor (Bright Data + Supabase)
- `apps/web`: frontend shell for Vercel

## Quick start
1. Apply Supabase migration in `infra/supabase/migrations`
2. Configure worker env from `infra/.env.worker.example`
3. Enqueue discovery jobs and run worker

- Discovery runs at 00:05 IST and 12:05 IST; checkpoint jobs are due from exact post age.
- Signal detection runs after D3/D7/D21 checkpoints and after daily follower refreshes.
