# FeedMe Command Hub Contract

This contract is the source of truth for the scheduled command-hub build.

## Build Boundary

- The command hub is a separate read-only admin surface.
- The main FeedMe app must remain untouched unless a change is strictly required to mount or hide chrome for `/command`.
- Existing Feed, Fire, Fund/Profile, login, onboarding, worker, and payment flows must keep their current behavior.
- Do not add direct action buttons or mutation flows in this phase.
- Do not write to existing app data from the dashboard UI.

## Read-Only Scope

The dashboard may:

- Add or replace `/command`.
- Add read-only API aggregation endpoints for the command hub.
- Read existing tables and existing APIs.
- Package existing account, engine, Fire, signal, intelligence, media, notification, and finance data into a premium admin view.
- Use filters, search, tabs, row selection, hover states, and detail drawers as read-only interactions.

The dashboard must not:

- Create, update, delete, retry, enqueue, pause, dismiss, charge, top up, or mutate any product data.
- Change worker execution behavior.
- Change app pricing or payment execution paths.
- Touch production records except through read-only queries.

## Product Context

The hub is for the founder/admin to understand everything associated with the app and account:

- Account graph: users, feeds, feeders, active/paused state, follower counts, context coverage.
- Engine health: run jobs, checkpoint jobs, status, attempts, lag, retry windows, last errors.
- Checkpoint surfaces: D1, D3, D7, D21 metrics and completion state.
- Fire and signals: alerts, signal families, hot posts, urgency, suppressed/stale/error states.
- Intelligence pipeline: fingerprints, condensations, D7 reads, post breakdowns, feeder files, model-call audit logs.
- Media and storage: asset capture, purge queues, storage provider state, byte sizes, failed captures.
- Notifications: push subscriptions and push jobs.
- Finance and stack usage: planned revenue, provider usage, usage gaps, and instrumentation plan.

## Finance Assumptions

- Planned pricing: INR 1499 per feeder.
- Razorpay is not live yet.
- Bright Data cost: USD 1.50 per 1000 records.
- Existing finance data is partial. Current real surfaces include user balance/top-up code paths and transaction references, but not a full cost ledger.

Missing instrumentation must be labeled honestly:

- AI token usage and model cost.
- Bright Data record counts by job/provider snapshot.
- Supabase cost/usage.
- Vercel traffic/build/runtime cost.
- R2/media storage and bandwidth cost.
- Server uptime, memory, CPU, and heartbeat history.
- Website traffic sources.

## Design Bar

- Premium, clean, modern, and operational.
- Dense but calm; useful for repeated review.
- Subtle vibrant punches, not a generic finance-dashboard clone.
- No fake screenshot blocks.
- Real data contracts first; honest placeholders only where instrumentation does not exist yet.
- Thoughtful loading, empty, error, and unavailable states.
- Clear visual separation between live data, estimated data, and not-yet-instrumented data.

## Review Target

The scheduled build should aim to be review-ready by 12:30 PM IST with:

- Branch or PR guidance.
- What is wired to real data.
- What remains mocked or pending instrumentation.
- Verification notes.
- Any blockers.
