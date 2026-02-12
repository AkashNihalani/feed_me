# Feed Me Monorepo

Single source repository for the Feed Me product.

## Structure
- `apps/web` - Next.js app (frontend + API routes)
- `apps/worker` - Python worker (queueing, sync, embeddings, alerts)
- `infra/docker-compose.yml` - local worker stack (db + worker + scheduler)
- `infra/supabase/migrations` - Supabase SQL migrations
- `docs` - project docs

## Local development

### Web app
```bash
cd apps/web
npm install
npm run dev
```

### Worker stack
```bash
cd infra
cp .env.worker.example .env
# put Google service account at infra/secrets/service_account.json
mkdir -p secrets

docker compose up -d --build
```

## Deployment notes
- Vercel project root should be `apps/web`.
- Worker deploy target (VPS/VM) should run from `infra/docker-compose.yml`.

## Security
- Do not commit `.env`, `.env.local`, service account JSON, or API keys.
- Rotate any key previously stored in local plaintext env files.
