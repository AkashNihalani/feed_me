# FeedMe Runbook (Clean Rebuild)

## 1) Apply DB schema
```bash
cd infra
supabase link --project-ref worqtdkvicuhmdgoncru
supabase db push
```

## 2) Verify
Run in Supabase SQL Editor:
- `infra/supabase/sql/minimal_engine_audit.sql`
- `infra/supabase/sql/engine_smoke_checks.sql`

Feeder intelligence has one prompt pair:
- reels-only fingerprint extraction: `apps/worker/app/feeder_prompts.py`
- feeder-file compilation v2: `apps/worker/app/feeder_prompts.py`

## 3) Worker setup
```bash
cd /path/to/feed_me
python3 -m venv .venv
source .venv/bin/activate
pip install -r apps/worker/requirements.txt
```

Create env file from `infra/.env.worker.example` and export vars.

## 4) First real run
```bash
python3 -m apps.worker.app.cli --mode enqueue_daily
python3 -m apps.worker.app.cli --mode enqueue_poll
python3 -m apps.worker.app.cli --mode once
```

## 5) Continuous processor
```bash
python3 -m apps.worker.app.cli --mode worker
```

## 5b) Fingerprint processor
Run this as a separate process/container so LLM work never blocks scraping or checkpoints:
```bash
python3 -m apps.worker.app.cli --mode fingerprint_reels_worker
```

## 5c) Seed validated feeder files
Run after applying the legacy-retirement migration when the manually validated Feederboard reads need to exist in backend storage:
```bash
python3 -m apps.worker.app.cli --mode seed_official_feeder_files
```

## 6) Job behavior
- Discovery runs every 12 hours through Bright Data Instagram Posts API pulls with a 2-day overlap.
- D1/D3/D7: scheduled from actual post age and bucketed into 60-minute windows.
- D21: queued from hot D7 performance
- Stale `running` jobs auto-requeued every minute

- Official checkpoints stay: D1, D3, D7, D21.
- Historical D2 rows remain in the database, but new discovery no longer writes fresh D2 buffer metrics.
- Use RUN_JOB_CONCURRENCY to fan out feeder discovery jobs in parallel.
- Feeder intelligence source of truth:
  - deterministic detection writes metric signals only
  - v8 reels-only fingerprints write `post_fingerprints`
  - validated feeder files live in `feeder_files`
  - feeder-file compilation uses the v2 prompt contract only
  - legacy focus/rulebook/signal-card LLM compilation is retired
