# FeedMe Runbook (Clean Rebuild)

## 1) Apply DB schema
```bash
cd "/Users/Akash/Documents/Feed Me/infra"
supabase link --project-ref worqtdkvicuhmdgoncru
supabase db push
```

## 2) Verify
Run in Supabase SQL Editor:
- `/Users/Akash/Documents/Feed Me/infra/supabase/sql/minimal_engine_audit.sql`
- `/Users/Akash/Documents/Feed Me/infra/supabase/sql/engine_smoke_checks.sql`

## 3) Worker setup
```bash
cd "/Users/Akash/Documents/Feed Me"
python3 -m venv .venv
source .venv/bin/activate
pip install -r apps/worker/requirements.txt
```

Create env file from `infra/.env.worker.example` and export vars.

## 4) First real run
```bash
python3 -m apps.worker.app.cli --mode enqueue_daily
python3 -m apps.worker.app.cli --mode once
```

## 5) Continuous processor
```bash
python3 -m apps.worker.app.cli --mode worker
```

## 6) Job behavior
- D1/D2 nightly scrape: D1 for same-day posts, D2 buffer for previous-day captures (not official checkpoint).
- D3/D7: batch due checkpoint jobs
- D21: only if D7 tag is `✅` or `🔥` or `🚀`
- Stale `running` jobs auto-requeued every minute

- Official checkpoints stay: D1, D3, D7, D21.
- D2 is only safety-buffer metrics, still tagged for signal visibility.
- Use RUN_JOB_CONCURRENCY to fan out all feeder scrapes in parallel at nightly burst.
