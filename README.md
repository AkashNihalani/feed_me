# FeedMe (Clean Rebuild)

Pure Supabase engine + Vercel frontend.

## Core model
- Feed = workspace
- Feeders = handles in a feed (1 to 15)
- Daily scrape at 23:30 IST with 2-day fetch window
- Official checkpoints: d1, d3, d7, d21 (d2 is buffer-only from two-day nightly scrape)
- Hot gate = top 35% (`✅`,`🔥`,`🚀`)
- D21 runs only if D7 is hot

## Structure
- `infra/supabase`: schema, queue functions, audit scripts
- `apps/worker`: pure engine processor (Apify + Supabase)
- `apps/web`: frontend shell for Vercel

## Quick start
1. Apply Supabase migration in `infra/supabase/migrations`
2. Configure worker env from `infra/.env.worker.example`
3. Enqueue nightly jobs and run worker

- All feeders are enqueued at the same 23:30 IST burst; worker fans out scrapes in parallel.
