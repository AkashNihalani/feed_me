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
- post breakdown extraction: `apps/worker/app/feeder_prompts.py`
- feeder-file compilation v2: `apps/worker/app/feeder_prompts.py`
- feeder-file frontend pattern/proof reads: `apps/worker/app/feeder_prompts.py`

## 3) Worker setup
```bash
cd /path/to/feed_me
python3 -m venv .venv
.venv/bin/python -m pip install -r apps/worker/requirements.txt
```

Create `apps/worker/.env` from `infra/.env.worker.example`.

## 4) First real run
```bash
set -a; source apps/worker/.env; set +a
.venv/bin/python -m apps.worker.app.cli --mode enqueue_daily
.venv/bin/python -m apps.worker.app.cli --mode enqueue_poll
.venv/bin/python -m apps.worker.app.cli --mode once
```

## 5) Continuous processor
```bash
set -a; source apps/worker/.env; set +a
.venv/bin/python -m apps.worker.app.cli --mode worker
```

## 5b) Fingerprint processor
Run this as a separate recovery process. The primary feeder file path is triggered from D7 metric processing; this worker catches old or interrupted qualifying posts:
```bash
set -a; source apps/worker/.env; set +a
.venv/bin/python -m apps.worker.app.cli --mode fingerprint_reels_worker
```

## 5c) Production systemd services
The worker runs natively from the repo venv. There is no container layer in production.

First install OS packages:
```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv ffmpeg git
```

Then install or refresh the services from the repo root:
```bash
FEEDME_HOME=/opt/feed_me \
FEEDME_USER=feedme \
FEEDME_GROUP=feedme \
FEEDME_ENV_FILE=/opt/feed_me/apps/worker/.env \
infra/scripts/install-native-worker-services.sh
```

Daily deploy from the worker host:
```bash
cd /opt/feed_me
infra/scripts/deploy-native-worker.sh
```

Useful checks:
```bash
systemctl status feedme-worker feedme-fingerprint-worker
journalctl -u feedme-worker -n 200 --no-pager
journalctl -u feedme-fingerprint-worker -n 200 --no-pager
```

## 5d) Compile feeder files
Run when a feeder has enough stored post breakdowns to form pattern candidates:
```bash
python3 -m apps.worker.app.cli --mode feeder_file_once --handle <handle> --limit 12 --days 90
```

For the first compile from existing stored fingerprints, use the recent-fingerprint path. This converts the exact selected fingerprints to post breakdowns first, then compiles from the same set:
```bash
python3 -m apps.worker.app.cli --mode feeder_file_recent_fingerprints_once --handles lakmeindia,anuj.mp4 --limit 10
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
  - D7 top 25% overall or top 20% among the last 10 posts triggers reel fingerprinting
  - fingerprints write `post_fingerprints`
  - post breakdowns write `post_breakdowns`
  - feeder-file memory keeps up to 100 reels per feeder, within the 90-day window and retained while D7 rank is top 35% or better
  - feeder-file compilation writes all active and candidate pools to `feeder_files`
  - `feeder_file_patterns` stores every active and candidate pool; only selected active pools get frontend pattern/proof reads
  - candidate pools promote to active at 3 core posts, or 2 core posts plus 1 support post
  - legacy rulebook/signal-card LLM compilation is retired
