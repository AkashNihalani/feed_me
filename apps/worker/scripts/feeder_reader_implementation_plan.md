# Feeder Reader — Implementation Plan (pipeline lock, 2026-07-11)

Companion to `feeder_reader_alert_families.md` (families + memory spec) and
`_probe_trigger_frequency.py` (threshold calibration, 2026-07-10 run: tight
profile → 1.5 merged cases/feeder-week, 43% quiet weeks, ~19 active
feeders/week).

LLM budget for the whole system: 2 calls per post EVER (fingerprint + post
card, both existing slots) + 1 call per feeder per active week. No per-case
runs, no frontend compiler run. Everything else is code.

## Locked invariants

These do not change while implementing the first production version:

1. One static reader system prompt, versioned independently from packet recipes.
2. One model call per feeder per active ISO week. A force trigger may run that
   call early only when the week has no completed visit; later events roll forward.
3. Every case is code-assembled as FACTS + LENS + CONTRAST SET + the same ASK.
4. The model returns exactly one relay per selected case. It never selects cases,
   derives metrics, chooses reference posts, or directly mutates memory.
5. Code validates and commits the complete run in one transaction. The model
   proposes memory content; code controls whether, where, and how long it lives.
6. Long-term memory is dossier + claims + bets. Recent episodic memory is the
   last 10 relays. Verdicts are the compact post-family index. Post cards are a
   warehouse artifact, not a memory register.
7. Full cards ride only for this week's subjects. References ride as display tag
   + mechanic line + verdict + stamped facts. Raw fingerprints never enter a run.
8. The assembled input has a hard 25,000-token ceiling. Overflow drops the
   lowest-value references, then lowest-priority case, then oldest relay. Cards
   and evidence blocks are never truncated.
9. Persist the exact payload/output plus system_prompt_version,
   packet_recipe_version, and card_version for every run.

---

## Phase 0 — Tables

### 0.1 Tracking (new state the triggers need)

```sql
-- every fired trigger, forever; reader_verdicts enforces one read per post/family
create table reader_trigger_events (
  id            bigserial primary key,
  source_key    text not null unique,     -- deterministic replay/idempotency key
  feeder_id     bigint not null references feeders(id) on delete cascade,
  post_key      text references posts(post_key) on delete cascade,
  family        text not null,           -- 'A'..'F'
  trigger       text not null,           -- 'ceiling_punch', 'follower_spike', ...
  week_start_ist date not null,
  checkpoint    text,                    -- 'd1'/'d7'/'d21' that produced it
  strength      int  not null,
  effect        jsonb not null,          -- code-measured: multiples, margins, run length, cohort medians
  case_id       bigint,                  -- filled when absorbed into a case
  fired_at      timestamptz not null default now()
);

-- one row per feeder x lane x ISO week; written by the weekly pass
create table reader_lane_weekly (
  feeder_id     bigint not null references feeders(id) on delete cascade,
  lane          text not null,           -- media_type
  week_start_ist date not null,
  posts         int  not null,
  median_pct    numeric,                 -- median percentile (d7)
  median_er_mult numeric,
  share_of_output numeric,               -- posts / all posts this week
  primary key (feeder_id, lane, week_start_ist)
);
-- unlocks: lane handover, lane revival baseline, depth/duration edge, quadrant flip

-- top-10 membership changes per feeder x lane, with the WHY
create table reader_rank_events (
  id            bigserial primary key,
  feeder_id     bigint not null references feeders(id) on delete cascade,
  lane          text not null,
  post_key      text not null references posts(post_key) on delete cascade,
  event         text not null,           -- 'entered' | 'left'
  reason        text not null,           -- 'merit' (beaten/beat) | 'ageout' (fell out of 90d)
  rank          int,                     -- rank at event time
  displaced_by  text,                    -- post_key, when reason='merit'
  margin        numeric,                 -- ranking_multiple ratio vs displaced #N
  week_start_ist date not null,
  created_at    timestamptz not null default now()
);

-- current top-10 state; events alone cannot answer "is this post top-10 now?"
create table reader_rank_state (
  feeder_id       bigint not null references feeders(id) on delete cascade,
  lane            text not null,
  post_key        text not null references posts(post_key) on delete cascade,
  rank            smallint not null check (rank between 1 and 10),
  ranking_multiple numeric(12,4) not null,
  window_start_ist date not null,
  updated_at      timestamptz not null default now(),
  primary key (feeder_id, lane, post_key),
  unique (feeder_id, lane, rank)
);

-- current streak per feeder x lane, updated on every new d7 row
create table reader_streaks (
  feeder_id     bigint not null references feeders(id) on delete cascade,
  lane          text not null,           -- plus lane='all' row
  direction     text not null,           -- 'above' | 'below' (median)
  length        int  not null,
  last_post_key text references posts(post_key) on delete set null,
  updated_at    timestamptz not null default now(),
  primary key (feeder_id, lane)
);

-- availability flips; one INSERT in the scraper where status changes
create table post_availability_events (
  id         bigserial primary key,
  post_key   text not null references posts(post_key) on delete cascade,
  feeder_id  bigint not null references feeders(id) on delete cascade,
  old_status text, new_status text not null,
  lane       text,
  rank_at_change smallint,
  changed_at timestamptz not null default now()
);
```

Availability events are inserted by an `AFTER UPDATE OF availability_status`
database trigger on `posts`, not by individual scraper paths. It snapshots the
current lane/rank so a later deletion case remains reproducible.

### 0.2 Post cards

```sql
create table post_cards (
  post_key     text primary key references posts(post_key) on delete cascade,
  card_md      text not null,            -- the factual card (see postcard_v2 prompt)
  mechanic_line text not null,           -- structured model output; never regex-derived
  tier         text not null,            -- S/M/L/XL
  display_tag  text,                     -- name registry: coined once, never re-coined
  model        text not null,
  prompt_version text not null,
  source_fingerprint_hash text not null,
  model_call_id bigint references feeder_file_model_calls(id) on delete set null,
  word_count   int check (word_count >= 0),
  created_at   timestamptz not null default now()
);
```

Cards are immutable and performance-blind. Ranks/percentiles are stamped onto
the payload AT ASSEMBLY TIME from post_metrics — never stored in the card.
`display_tag` doubles as the name registry (registry = this column).

### 0.3 Memory registers (per feeder)

```sql
create table reader_dossiers (
  feeder_id  bigint primary key references feeders(id) on delete cascade,
  dossier    text not null,              -- ~300 words, delta-updated, never regenerated
  updated_at timestamptz
);

create table reader_claims (
  id           bigserial primary key,
  feeder_id    bigint not null references feeders(id) on delete cascade,
  claim        text not null,            -- one prose sentence
  scope        text not null,            -- 'global' | lane | 'lane:<mt>'
  status       text not null default 'live',  -- live/reinforced/weakened/overturned/faded/archived
  evidence_score numeric not null default 0,  -- CODE-computed, never model-set
  born_week_start date not null,
  last_touched_week_start date not null
);
-- cap enforced in code: <=10 with status in (live, reinforced, weakened)

create table reader_claim_events (
  id        bigserial primary key,
  claim_id  bigint not null references reader_claims(id),
  case_id   bigint not null,
  delta     text not null,               -- confirms/weakens/breaks/proposes
  week_start_ist date not null
);

create table reader_claim_receipts (
  id          bigserial primary key,
  claim_id    bigint not null references reader_claims(id) on delete cascade,
  case_id     bigint not null references reader_cases(id) on delete cascade,
  post_key    text references posts(post_key) on delete set null,
  family      text not null,
  effect      jsonb not null,
  effect_norm numeric not null,
  observed_on date not null,
  unique (claim_id, case_id, post_key, family)
);

create table reader_bets (
  id         bigserial primary key,
  feeder_id  bigint not null references feeders(id) on delete cascade,
  claim_id   bigint references reader_claims(id) on delete set null,
  condition  text not null,              -- testable, deterministic-checkable where possible
  settle_by_week_start date not null,
  outcome    text,                       -- null=open, 'called'/'missed'/'void'
  settled_week_start date
);
-- cap in code: <=5 open; settlement is FIRST item of next visit

-- episodic memory: one relay per analyzed case, fixed format, stored forever;
-- the LAST 10 ship verbatim in every payload (the reader's recent voice)
create table reader_relays (
  id         bigserial primary key,
  feeder_id  bigint not null references feeders(id) on delete cascade,
  case_id    bigint not null references reader_cases(id) on delete cascade,
  week_start_ist date not null,
  seq        int not null,               -- per-feeder relay number (#47)
  relay_md   text not null,              -- SUBJECTS / READ / VERDICTS / LEDGER, ~150 words
  created_at timestamptz default now(),
  unique (case_id),
  unique (feeder_id, seq)
);
-- Relays = short-term working memory + continuity of voice (~3 weeks at busy
-- feeders). Claims = long-term spine. When a relay leaves the last-10 window
-- its durable content has already drained into claims/verdicts/dossier.

create table reader_verdicts (           -- thread index
  post_key  text not null,
  family    text not null,
  verdict   text not null,               -- one line
  case_id   bigint not null references reader_cases(id) on delete cascade,
  week_start_ist date not null,
  primary key (post_key, family)
);
```

### 0.4 Cases + runs

```sql
create table reader_cases (
  id          bigserial primary key,
  feeder_id   bigint not null references feeders(id) on delete cascade,
  week_start_ist date not null,
  families    text[] not null,
  triggers    jsonb not null,            -- merged trigger rows + effects
  subjects    text[] not null,           -- post_keys, <=4
  references_ text[] not null,           -- post_keys, <=6
  question    text not null,             -- deterministic template, framed vs dossier
  status      text not null default 'selected'  -- selected/answered/dropped
);

create table reader_runs (
  id          bigserial primary key,
  feeder_id   bigint not null references feeders(id) on delete cascade,
  week_start_ist date not null,
  mode        text not null,             -- 'visit' | 'state' (quiet week)
  payload     jsonb not null,            -- exact LLM input (audit)
  output      jsonb,                     -- the ONE JSON: relays + claims_touched + bets + dossier_delta
  model       text,
  system_prompt_version text not null,
  packet_recipe_version text not null,
  card_version text not null,
  status      text not null default 'pending',
  attempt     int not null default 0,
  last_error  text,
  started_at  timestamptz,
  completed_at timestamptz,
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now(),
  unique (feeder_id, week_start_ist)
);
```

`reader_runs.output` is the visit page's data source. Frontend compiles from it
deterministically (wrapped layout) — no compiler LLM.

---

## Phase 1 — Deterministic trackers (`app/reader_trackers.py`)

All pure functions over existing tables; invoked from two hooks:

1. **Per-D7-arrival hook** (where D7 metrics are computed today):
   - `update_streaks(feeder_id, post)` — extend/reset `reader_streaks` (lane + 'all').
   - `update_rank_ledger(feeder_id, lane)` — rebuild trailing-90d lane top-10 by
     ranking_multiple; diff vs stored membership; write `reader_rank_events`
     with reason: post present in both windows but pushed out = 'merit'
     (+displaced_by, margin), post older than 90d = 'ageout'.
2. **Weekly cron (Mon 06:00 IST, before the reader run)**:
   - `write_lane_weekly(feeder_id, week)` — one row per lane from post_metrics.
   - `follower_weekly(feeder_id)` — weekly gain series from feeder_follower_snapshots
     (computed on the fly; no table needed).
3. **Scraper**: on availability change, insert `post_availability_events` (1 line).

## Phase 2 — Trigger engine (`app/reader_triggers.py`)

Port `_probe_trigger_frequency.py` logic with the LOCKED TIGHT profile:

| Family | Trigger | Condition (all at latest checkpoint unless noted) | Strength |
|---|---|---|---|
| A | early_fire | D1 pct <= 5 | 25 |
| A | long_hold | D7 pct <= 10 AND D21 pct <= 10 | 20 |
| A | ceiling_punch | rank_event entered top-3, reason='merit', pool >= 15, margin >= 1.15x | 30 |
| A | throne_lost | rank_event left top-3, reason='merit' | 24 |
| A | floor_break | D7 pct >= 95 AND prev-5 median pct <= 60 (warm account) | 16 |
| B | talk_post | comments_mult >= 3.0 AND >= 1.8x likes_mult | 18 |
| B | silent_approval | likes pct <= 15 AND comments pct >= 60 | 14 |
| B | core_burn | ER pct <= 15 AND likes pct > 35 | 18 |
| C | late_bloomer | D7 delta_from_d1 >= +35 | 16 |
| C | fast_fade | D1 pct <= 10 AND D7 pct >= 50 | 14 |
| C | hour_edge | AGGREGATE-led only: >=3 posts this month share hour bucket with hour_mult >= 2 | 8 |
| D | lane_revival | prev 4 same-lane posts pct >= 40, this one <= 15 | 22 |
| D | lane_handover | dominant lane by share loses median-pct lead to another lane, 4wk vs prior 4wk (reader_lane_weekly) | 20 |
| D | depth_edge | a duration/depth bucket's median pct beats lane median by >= 20 pts over >= 6 posts/90d | 10 |
| E | follower_spike | weekly gain >= 3x trailing-8wk median AND >= 0.5% of base | 20 |
| E | quadrant_flip | sign flip of (ER trend x follower trend), 4wk windows | 14 |
| F | cadence_shift | week posts >= 1.7x or <= 0.5x trailing-4wk avg, min 3 posts | 12 |
| F | collab_differential | collab AND \|pct - solo median\| >= 30 | 15 |
| F | deletion | availability event: unavailable AND post was top-10 (rank ledger) | 18 |
| F | cold_run_broken | reader_streaks: >= 5 'below' then post pct <= 20 | 22 |

Every emit writes `reader_trigger_events` with `effect` jsonb (the measured
magnitudes) and a deterministic `source_key`. Replay duplicates use `INSERT ...
ON CONFLICT (source_key) DO NOTHING`. A post is excluded from another read in a
family only after `reader_verdicts` contains `(post_key, family)`.

## Phase 3 — Case builder (`app/reader_cases.py`)

Weekly, per feeder, code only:

1. Collect this week's uncased trigger events.
2. Merge: same (post, family) → one; cases sharing > 50% subjects → one
   compound case (families concatenate).
3. Group rule: >= 4 same-family single-post triggers this week → one grouped
   case (e.g. "4 posts hit top-10"), subjects = those posts (<= 4 + refs).
4. Attach references by family recipe (spec tables): RL/RA (recent 5), LT/LB
   (band residents), PT, PW (30-90d analogues) — all from post_metrics +
   reader_verdicts. References = card header + verdict line only.
5. **Select <= 5 by information gain**, not raw strength:
   `priority = strength * gain` where gain = 1.5 if it would settle an open bet
   or touches a claim with status 'weakened', 1.2 if it threatens/extends any
   live claim (scope overlap), 1.0 if novel, 0.5 if it re-confirms a claim
   already 'reinforced' with >= 3 receipts.
   Budgets: <= 4 subjects + <= 6 refs per case, <= 15 distinct posts/week,
   <= 8 fresh reads (posts with no verdict yet).
6. Write `reader_cases`; stamp case_id back onto trigger events.

### Case question = FACTS + LENS + ASK (all code-assembled)

ONE system prompt serves every alert; nothing about the ask is family-specific
except a one-line LENS. Per case:

- FACTS — the measured event in numbers (percentiles, multiples, margins,
  streak lengths). Model never derives numbers.
- LENS — one line by family:
  A: "Compare what the subject is doing against what it displaced / what
     usually sits in this band."
  B: "Read how the audience paid — comments vs likes vs ER — and what the
     subject asked them to do."
  C: "Read the aging curve: what about this construction fits a slow burn /
     fast fade here?"
  D: "Read this as a lane event, not a post event — what moved between the
     formats?"
  E: "Read the account level: who arrived in this window and what were they fed?"
  F: "Read the feeder's own decision — the move they made, and what landed."
- ASK (constant, verbatim): "What does this confirm, weaken, or add to what we
  know about how this account earns attention? Name the claims you touch.
  Write the relay."

The contrast set (which cards ride along) is the per-trigger packet recipe from
feeder_reader_alert_families.md — the question never forks, the evidence does.
Aggregate triggers must also have deterministic subject recipes: follower spike
uses representative posts from the spike window against the trailing baseline;
cadence shift uses the changed week against the prior four; quadrant flip uses
posts at both sides of the flip; lane handover uses representatives from both
lanes; hour edge uses the qualifying hour bucket against same-lane peers.

Gate: feeder gets a run if >= 1 selected case, else mode='state'.
Exception: ceiling_punch / follower_spike / deletion may force a mid-week run.

## Phase 4 — Payload + the one weekly run (`app/reader_payload.py`, prompt in `app/reader_prompts.py`)

Payload per feeder (ONE Sonnet call for all its cases):

```json
{
  "handle": "...", "week": "2026-W28",
  "dossier": "...",
  "claims": [{id, claim, scope, status, receipts_count, evidence_score}],
  "open_bets": [{id, condition, settle_by_week, deterministic_result?}],
  "memory_events": [...],          // code-generated callbacks: settlements FIRST
  "recent_relays": ["...x10"],     // last 10 relay_md verbatim — the reader's own voice
  "cases": [{
    "case_id": 1, "families": ["A","F"], "question": "...",
    "facts": {trigger effects, code-stamped ranks/percentiles/multiples},
    "subjects": [{post_key, display_tag, card_md, stamped_numbers}],
    "references": [{post_key, display_tag, mechanic_line, verdict, stamped_numbers}]
    // references ride at ~50 tok: tag + the card's MECHANIC FINGERPRINT line
    // + verdict + numbers. Never full cards, never fingerprints.
  }]
}
```

Output contract (strict JSON, validated in code):

```json
{
  "relays": [{                                   // ONE per analyzed case — the atomic unit
    "case_id": 1,
    "relay_md": "SUBJECTS/READ/VERDICTS/LEDGER, ~150 words",
    "title": "...", "beats": ["..."],           // wrapped-style visit content
    "post_refs": ["p/..."],                      // UI highlight
    "subject_verdicts": [{"post_key": "...", "family": "A", "verdict": "one line"}],
    "claims_touched": [{"claim_id": 3, "delta": "weakens"} | {"delta": "proposes", "claim": "...", "scope": "lane:reel"}]
  }],
  "bet_settlements": [{"bet_id": 2, "outcome": "called", "line": "..."}],
  "new_bets": [{"condition": "...", "settle_by_week": "..."}],
  "dossier_delta": "replace/append instructions, <= 60 words"
}
```

Payload budget (hard ceiling 25k tokens; worst-case heavy week ~12.6k):
dossier 400 + claims 600 + bets/events 400 + last-10 relays 2,000 + case
questions/facts 1,500 + subject cards (<=15/wk) 5,000 + references 1,200 +
system/voice prompt 1,500. Cards enter the payload ONLY as this week's
subjects; all historical posts ride as display_tag + verdict line.

Validation gates (retry once on failure):
- exactly one relay for every selected case; no missing, duplicate, or unknown case;
- every post ref belongs to that case's supplied subjects/references;
- every existing claim_id was supplied and every delta is allowed;
- every relay has claims_touched (>= 1) or one supported proposed claim;
- subject verdicts cover the required post-family pairs;
- output may quote only stamped numbers and may not alter them;
- semantic contradictions must be declared through claims_touched (prompt law;
  code validates the declaration shape, not arbitrary prose semantics);
- new display_tags only for posts without one.

Before the call, count the real assembled tokens. If over 25k: remove the
lowest-value references, then the lowest-priority case, then the oldest relay.
Never truncate a card, fact block, or evidence item midway.

## Phase 5 — Ledger commit (`app/reader_ledger.py`)

Single writer, one transaction, after the run:
1. Settle bets (outcome from deterministic check where possible, else model line).
2. Apply claim deltas; INSERT proposes (evict lowest-score if > 10 live).
3. Recompute `evidence_score = Σ receipt_effect_norm * 0.85^(weeks_since_receipt)`
   ± bet record bonus; status transitions from score thresholds + deltas;
   receipts all aged out of 90d AND score < floor → 'faded' → archive.
4. Write reader_verdicts (subjects only) + insert reader_relays. Acquire a
   feeder-scoped transaction advisory lock before assigning `max(seq) + 1`.
5. Apply dossier_delta.
6. Emit memory_events list for NEXT week's callbacks.

## Phase 6 — Visit compile (web, no LLM)

- `reader_runs.output` served via a new API route; visit page (wrapped layout,
  already in `/visit`) renders relays + card display_tags + thumbnails + math
  status. Quiet weeks render mode='state': dossier + cadence/rank numbers.

## Build order (each step shippable)

1. Phase 0 DDL + scraper availability hook.
2. Phase 1 trackers + backfill job (replay history to seed streaks, rank
   ledger, lane weekly — the probe script is the reference implementation).
3. Phase 2 triggers behind a dry-run flag; run 2 weeks shadow, compare vs probe.
4. Post-card backfill for the 90d window (postcard_v2 prompt, gpt-5.4-mini).
5. Phase 3 cases + Phase 4 payload; snapshot payloads to reader_runs BEFORE
   enabling the model (audit the inputs first).
6. Phase 4 run + Phase 5 commit on 3 pilot feeders (1 heavy: lakmeindia,
   1 mid: thecroffleguys, 1 quiet: taneesho). 2 weeks.
7. Phase 6 visit wiring; full rollout.

## Database delivery lock

Ship schema in two migrations, not one speculative bundle:

1. Tracking foundation: post cards, trigger events, lane weeks, rank state/events,
   streaks, availability events/trigger, constraints, indexes, and private RLS.
2. Reader runtime after shadow validation: cases, runs, dossiers, claims/events/
   receipts, bets, verdicts, and relays.

All reader tables are private worker surfaces: enable RLS, revoke browser roles,
and grant service-role access in the introducing migration. Use `bigint` for
feeder foreign keys and `date` week starts everywhere; format ISO week labels only
at the API/UI boundary.
