# Instagram Tracker VPS Stack (Docker)

This stack runs a reliable scraper pipeline on a single VPS:
- Postgres for state + retry queue
- Worker container to process handles
- Scheduler container (cron) to enqueue daily jobs

## 1) Prereqs
- Docker + Docker Compose
- Apify API token + actor id
- Google Sheets API enabled
- Service account JSON

## 2) Google Sheets Setup
1. Create a Google Cloud project.
2. Enable **Google Sheets API**.
3. Create a **Service Account** and download the JSON key.
4. Share your spreadsheet with the service account email (editor access).
5. Place the JSON at `./secrets/service_account.json`.

## 3) Configure Environment
Copy `.env.example` to `.env` and fill in:
- `SPREADSHEET_ID`
- `APIFY_TOKEN`
- `APIFY_ACTOR_ID`

Optional:
- `APIFY_INPUT_TEMPLATE` to match your actor's input format.
- `APIFY_MAX_ITEMS` to limit posts per run.
- `IGNORE_SHEETS` for tabs you don't want processed.
- `TZ` for schedule timezone.

## 4) Start the Stack
```bash
docker compose up -d --build
```

## 5) Run Once (Manual)
If you want a manual run:
```bash
docker compose run --rm worker python -m app.cli --mode schedule
docker compose run --rm worker python -m app.cli --mode worker
```

## 6) Schedule
Default cron runs **daily at 02:00** server time.
Edit `worker/app/cron/scheduler.cron` to change the schedule.

## How It Works
- Each sheet tab name = Instagram handle
- Scheduler enqueues all handles
- Worker processes queue with retries
- Each handle tab is updated with dedupes + refreshed metrics

## Notes
- The data mapping is in `worker/app/sync.py` (`_normalize_item`).
- If Apify fields differ, adjust mapping there or update `APIFY_INPUT_TEMPLATE`.
