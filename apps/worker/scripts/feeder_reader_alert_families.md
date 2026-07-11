# Feeder Reader — Alert Families (locked spec)

The unit of analysis is **post × thread**, keyed `(post_key, family)`: a post may be
read once per family (a ceiling read in A, later a comment-behavior read in B),
never twice for the same family's lesson.

Pipeline: raw triggers → merge by subject overlap (>50% → one compound case) →
assign subject/reference roles → enforce weekly pool → write bites from cases.

Budgets: ≤4 subjects + ≤6 references per case · weekly pool ≤15 distinct posts
across all fired bites · ≤8 fresh reads per feeder-week. References ship as
existing card + stored verdict line (near-free). Aggregates are free — packets
include posts only for content questions.

## Sourcing pools (deterministic, worker-side)

| Pool | Definition |
|---|---|
| T | posts that tripped the trigger this week |
| RL / RA | recent lane (latest 5, same media type) / recent all (latest 5) |
| LT / LB | lane top band / bottom band (90d, top-10% / bottom-25%) |
| PT | overall pool top-band residents (90d) |
| PW | prior-window set (30–90d old: old failures, old winners, past analogues) |
| V | stored verdict lines — ride free with any referenced post |

## Family A · Ceiling & floor — what this account's best is made of (L1, ×L3)

| Trigger | Subjects | R new | R old | Source |
|---|---|---|---|---|
| Early fire — lane top-5% at D1 | 1 | 2 RL | 2 LT (V) | D1 percentile scan |
| The long hold — top-10% overall held D7→D21 | 1 (new durability thread) | 2 same-cohort faders | 2 past evergreens (PW) | trajectory cohort |
| Ceiling punch — new lane #1–3 | 1 | 1 RL | 3 displaced holders (V) | pool ledger |
| Floor break — bottom-5% while median holds | 1 | 2 RL | 2 LB | lane bottom |

## Family B · The bite's shape — how this audience pays (L2)

| Trigger | Subjects | R new | R old | Source |
|---|---|---|---|---|
| Talk post — comment share ≥2.5× usual | ≤2 | 1 like-carried peer, same band | 2 past comment-carried (PW) | axis divergence |
| Silent approval — likes high, comments dead | 1 | 1 | 2 | axis divergence |
| Core burn — ER pct ≫ raw pct | 1 | 2 RA | 1 | ER vs raw split |

## Family C · Lifecycle — how content ages here (L3)

| Trigger | Subjects | R new | R old | Source |
|---|---|---|---|---|
| Late bloomer — +25 pts D3→D7 | 1 | 2 instant-fire peers | 1 past bloomer (PW) | delta_from_d1 |
| Fast fade — D1 top decile → D7 sub-median | 1 | 2 stable peers, same D1 | 1 past fader | trajectory |
| Hour edge — hour_multiple extreme | 0–1 | 2 same-hour cohort | — | hour_multiple (aggregate-led) |

## Family D · Lanes & mix — where the power lives (L4 × L6)

| Trigger | Subjects | R new | R old | Source |
|---|---|---|---|---|
| Lane revival — failing lane enters top band | ≤3 | 1 RL | 3 old failed (PW, V) | lane band history |
| Lane handover — dominant lane's median crossed | 2 new leaders | 2 fading incumbents | 2 V each side (PW) | lane medians + share_delta |
| Depth/duration edge — a bucket outperforming | 0–2 | 2 per band | — | depth/duration buckets (aggregate-led) |

## Family E · Audience — who's arriving, what they eat (L5)

| Trigger | Subjects | R new | R old | Source |
|---|---|---|---|---|
| Spike attribution — follower jump window | ≤2 window posts | window V | 1 prior spike (PW) | follower series |
| Quadrant flip — ER trend × follower trend | 0 | 3 current-phase V | 3 prior-phase V | trend windows (aggregate-led) |

## Family F · The hand — the feeder's own moves (L6)

| Trigger | Subjects | R new | R old | Source |
|---|---|---|---|---|
| Cadence shift + landing | 0–2 | 3 first-week V | baseline numbers | rhythm_days, gap_vs_usual |
| Collab differential — ±30 pts vs solo median | 1 | 2 solo peers | 2 past collabs | collab_post flag |
| The deletion — top-band post goes unavailable | 0 | its V + rank history | — | availability_status flip |
| Cold run broken | 1 breaker | run V (free) | 1 past analogue (PW) | streak ledger |

## Framing rule

Every case question is asked against the dossier, never against the posts:
"Given [trigger facts], what does this confirm, break, or add to what we know
about how this account earns attention?" The claims relay ships in every payload;
the answer must position itself relative to prior claims.

- ✗ "Three carousels hit top 10% because they used before/afters."
- ✓ "The center of gravity moved. Reels kept asking for watch time; the carousels
  started paying receipts up front — and the audience switched seats. Third week
  running. The 'this account performs on video' claim is officially weakened."

## The evolving feeder memory (locked)

Five bounded registers (~1–2k tokens total; grows in claim quality, never volume):

1. **Dossier** (~300 words) — standing account read, updated by delta after each
   weekly run, never regenerated. Ships with every case call.
2. **Claim ledger** (≤10 live) — claim sentence + receipts (post × family) +
   status (live/reinforced/weakened/overturned/faded) + born/last-touched.
   Claims change only through cases. Faded → archive ("that era ended").
3. **Bet book** (≤5 open + lifetime tally) — testable condition + settle-by week.
   Settlement is mandatory and FIRST in the next visit (the cross-week callback).
4. **Name registry** — display_tag coined once per post, reused forever.
5. **Thread index** — one verdict line per (post, family) read; makes references
   free and re-analysis impossible.

Consistency mechanics:
- Every case payload = dossier + relevant claims + open bets touching its
  subjects + verdicts for its references. All bites in a run read one belief state.
- **No bite writes memory.** Cases emit findings + required `claims_touched`
  (confirms / weakens / breaks / proposes). A single ledger-update pass at the
  end of the run commits all deltas — one writer, no races. Genuine conflicts
  between cases become content ("two reads disagree — not calling it yet"),
  never silent double-writes.
- **Contradiction gate:** output opposing a live claim without naming it is
  rejected and retried. Contradiction allowed; unacknowledged contradiction not.
- "Getting better" is shown, not said: receipt counts grow, the bet tally is
  public (called/missed), claims carry revision history. The visit's callbacks
  are generated from a deterministic memory-events list (claim status changes,
  bet settlements, tally moves) — the worker hands the reader his "I told you"
  moments.
