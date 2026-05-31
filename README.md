# FeedMe (Clean Rebuild)

Pure Supabase engine + Vercel frontend.

## Core model
- Feed = workspace
- Feeders = handles in a feed (1 to 15)
- Discovery runs every 12 hours with a 2-day overlap
- Official checkpoints: d1, d3, d7 scheduled from actual post age in 60-minute buckets
- D21 is queued for hot D7 posts
- Fire tracking cards come from deterministic checkpoint metrics
- Feeder intelligence has one LLM path: reels-only v8 fingerprints, then the feeder-file v2 compiler prompt

## Structure
- `infra/supabase`: schema, queue functions, audit scripts
- `apps/worker`: native Python worker (Bright Data + Supabase)
- `apps/web`: frontend shell for Vercel

## Quick start
1. Apply Supabase migration in `infra/supabase/migrations`
2. Configure worker env from `infra/.env.worker.example`
3. Create the worker venv and run the native worker

- Discovery runs at 00:05 IST and 12:05 IST; checkpoint jobs are due from exact post age.
- Signal detection remains deterministic and metric-only; feeder fingerprints are generated separately for qualifying D7 reels.

## Native worker
```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r apps/worker/requirements.txt
set -a; source apps/worker/.env; set +a
.venv/bin/python -m apps.worker.app.cli --mode worker
```

For production, install the systemd services:
```bash
FEEDME_HOME=/opt/feed_me \
FEEDME_USER=feedme \
FEEDME_GROUP=feedme \
FEEDME_ENV_FILE=/opt/feed_me/apps/worker/.env \
infra/scripts/install-native-worker-services.sh
```

For later deploys on the worker host:
```bash
cd /opt/feed_me
infra/scripts/deploy-native-worker.sh
```
