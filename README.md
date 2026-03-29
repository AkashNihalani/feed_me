# FeedMe (Clean Rebuild)

Pure Supabase engine + Vercel frontend.

## Core model
- Feed = workspace
- Feeders = handles in a feed (1 to 15)
- Discovery runs every 12 hours with a 2-day overlap
- Official checkpoints: d1, d3, d7 scheduled from actual post age in 60-minute buckets
- D21 is only queued after a post is hot at D7
- Hot gate = top 35% (`✅`,`🔥`,`🚀`)
- Pattern intelligence is extracted at D21 for posts that qualified hot at D7

## Structure
- `infra/supabase`: schema, queue functions, audit scripts
- `apps/worker`: pure engine processor (Bright Data + Supabase)
- `apps/web`: frontend shell for Vercel

## Quick start
1. Apply Supabase migration in `infra/supabase/migrations`
2. Configure worker env from `infra/.env.worker.example`
3. Enqueue discovery jobs and run worker

- Discovery runs at 00:05 IST and 12:05 IST; checkpoint jobs are due from exact post age.
