# Signal Alert Rulebook

Last updated: 2026-04-29

This is the operating guide for the current signal-intelligence system. It replaces the old post-intelligence/tag-pattern alert pipeline.

## Source Of Truth

Detection is deterministic and SQL/data driven. The LLM never decides whether an alert should exist.

Live write tables:

- `post_metrics`: checkpoint metrics and percentiles.
- `feeder_follower_snapshots`: daily follower snapshots.
- `signals`: one detected signal row per viable alert candidate.
- `signal_posts`: max 5 sample posts attached to a signal.
- `post_fingerprints`: cached post-level multimodal fingerprint.
- `signal_intelligence`: cached user-facing card copy.

Retired intelligence path:

- `post_intelligence` is dropped by migration.
- `pattern_alerts.py`, `post_intelligence.py`, and `checkpoint_intelligence.py` are deleted.
- `fn_process_checkpoint` is retained only as a no-op compatibility function. It must not write `fire_alerts`.
- Old `OWN_PATTERN`, `CROSS_PATTERN`, and `ANCHOR_PATTERN` fire-alert rows are deleted by migration.

## Runtime Flow

1. Worker writes checkpoint rows to `post_metrics`.
2. Worker recomputes feeder checkpoint rankings.
3. Worker runs deterministic detection for D3, D7, D21, and follower daily signals.
4. Detection writes `signals` and `signal_posts`.
5. UI can show pending signal shells immediately.
6. Card intelligence is generated only by `resolve_signal_intelligence`.
7. `resolve_signal_intelligence` fingerprints missing posts, generates the card, and writes `signal_intelligence`.

Important cost boundary: normal detection has zero LLM cost. The always-on worker loop does not currently auto-run `resolve_signal_intelligence`; this prevents surprise LLM spend. Card generation is an explicit/manual worker mode until we add a controlled lazy endpoint or queue.

## Inputs We Have

Allowed metrics:

- views
- likes
- comments
- follower count snapshots
- checkpoint percentile ranks
- metric multiples from rolling baselines

Not available and not allowed in trigger logic or LLM claims:

- saves
- shares
- private algorithm explanations
- off-platform facts unless visible/provided

## Stratification

Own-account content signals are grouped by media type first. A reel signal uses the last matching reels, not the last mixed posts. A sidecar signal uses sidecars only. Image signals use images only.

- `reel`
- `sidecar`
- `image`

When enough history exists, the system adds deterministic sub-buckets:

- Reels: `DUR_SHORT`, `DUR_MEDIUM`, `DUR_LONG`, `DUR_EXTENDED`, `DUR_UNKNOWN`
- Carousels: `DEPTH_MINI`, `DEPTH_STANDARD`, `DEPTH_DEEP`, `DEPTH_UNKNOWN`
- Images: no sub-bucket

Current code uses a minimum of 7 posts in a `(media_type, sub_bucket)` stratum before applying the sub-bucket. If there are fewer than 7, it falls back to `(feeder, media_type)` only.

## Caps

All caps are 7-day rolling caps and count only visible/live statuses: `pending`, `fresh`, and `stale`.

- Own signals: max 2 per feeder per week.
- Cross-feed signals: max 5 per feed per week.
- Anchor signals: max 3 per feed per week.
- Same signal family: max 2 per week within the same scope subject.
- Follower/audience event signals: max 1 per 7-day business window for the same signal type/scope subject.

Suppressed rows may still be written with `status='suppressed_cap'` for auditability, but the UI does not fetch them as active cards.

## Overlap Suppression

After detection, content signals are de-duped inside the same lane before writing visible rows.

Same lane means:

- same feed
- same business date
- same checkpoint
- same media type
- same sub-bucket
- same scope
- for own signals, same feeder

Audience/follower signals are excluded from content-overlap suppression because they are account-level events.

Suppression rules:

- If trigger posts overlap by at least 2 posts and at least 67% of the smaller proof set, the lower-priority signal is suppressed.
- If all proof posts overlap by at least 3 posts and at least 67% of the smaller proof set, the lower-priority signal is suppressed.
- For the common anchor overlap between `ANCHOR_GAP_CLOSING` and `ANCHOR_CHALLENGER_SURGE`, the challenger card wins because it is more specific.

## Post Sampling Limits

Every signal sends at most 5 posts to the LLM.

- Single-cohort signals: up to 5 trigger posts.
- Comparison/reference signals: usually 3 trigger posts + 2 reference posts.
- Cross-feeder selections prefer one post per feeder.
- Ties generally prefer stronger percentile first, then more recent posts.

Reference posts are not statistical baselines. The statistical baseline is always in `metric_snapshot`.

Internal roles:

- `trigger_core`, `trigger_support`
- `reference_typical`, `reference_strong`, `reference_no_jump`
- `reference_other_format`, `reference_anchor`, `reference_feed`

These roles are internal only and must never be shown to users.

## Reference Selection

- Typical reference: posts closest to the feeder/media median in the prior window.
- Strong reference: prior top-performing posts.
- No-jump reference: posts that did not materially improve between checkpoints.
- Other-format reference: typical recent posts from other media formats.
- Anchor/feed reference: opposite-side examples for anchor comparisons.

## Own Signals

| Signal | Checkpoint | Trigger | Posts Sent |
|---|---:|---|---|
| `OWN_BREAKOUT_EARLY` | D3 | Last 7 D3 posts include at least 2 top-10% posts, and prior-30 D7 median is at least 35 | A: top 3 early breakouts. B: 2 typical prior references. |
| `OWN_BREAKOUT` | D7 | Last 10 D7 posts include at least 3 top-10% posts, and prior-30 D7 median is at least 35 | A: top 3 breakouts. B: 2 typical prior references. |
| `OWN_SUSTAIN` | D7 | Last 15 D7 posts include at least 5 top-15% posts, and prior-30 D7 median is over 40 | A: top 5 sustained posts. |
| `OWN_SUSTAIN_LONG` | D21 | Last 10 D21 posts include at least 3 top-25% posts | A: top 5 evergreen posts. |
| `OWN_FADE` | D7 | Last 10 D7 posts include 0 or 1 top-25% posts, and prior-30 D7 median is at most 30 | A: 3 weakest recent posts. B: 2 prior strong references. |
| `OWN_COMMENT_SPIKE` | D7 | Last 10 D7 posts include at least 4 posts with `comments_x >= 2.0`; reels must also have `views_x <= 1.3` when views multiple exists; comments must clear `max(10, prior_comment_median * 1.5)` | A: top 4 matching posts. |
| `OWN_LIKE_HEAVY` | D7 | Last 10 D7 posts include at least 4 posts with `likes_x >= 2.0`, `comments_x <= 0.8` when present, and reels not materially above usual views | A: top 4 matching posts. |
| `OWN_VIRAL_PASSIVE` | D7 | Reels only. Last 10 D7 posts include at least 3 posts with `views_x >= 2.5`, `likes_x <= 1.2` when present, and `comments_x <= 1.0` when present | A: top 3 matching reels. |
| `OWN_LATE_JUMP` | D7 or D21 | Recent 15 posts include at least 3 posts that improved by 25+ percentile points from D3 to D7 or D7 to D21 | A: top 3 jumpers. B: 2 no-jump references. |
| `OWN_FOLLOWER_SPIKE` | Daily | 7-day net gain is at least `max(50, 1% of latest followers)` and at least `max(5x trailing weekly rate, 50)` | A: latest 5 posts in the window. |
| `OWN_FOLLOWER_DROP` | Daily | 7-day net loss is at least `max(25, 0.5% of latest followers)` and at least 3x weekly volatility | A: latest 5 posts in the window. |

## Cross-Feed Signals

Cross-feed content signals require at least 5 active feeders in the feed.

| Signal | Checkpoint | Trigger | Posts Sent |
|---|---:|---|---|
| `CROSS_MOMENTUM` | D7 | In the last 14 days, at least 4 feeders and at least 40% of active feeders have a top-15% D7 post; rate is at least +15pp vs prior 60 days | A: latest hot post per contributing feeder, max 5. |
| `CROSS_FORMAT_SHIFT` | D7 | A media type has at least 5 recent posts and 10 prior posts; recent top-15% rate is at least 30% and +20pp vs prior 60 days | A: 3 hot posts in the productive format. B: 2 typical other-format references. |
| `CROSS_MICRO_BREAKOUT` | D7 | At least 3 feeders fired `OWN_BREAKOUT` or `OWN_BREAKOUT_EARLY` in 14 days | A: one breakout post per feeder, max 5. |
| `CROSS_MICRO_COMMENT_SPIKE` | D7 | At least 3 feeders fired `OWN_COMMENT_SPIKE` in 14 days | A: one matching post per feeder, max 5. |
| `CROSS_MICRO_LIKE_HEAVY` | D7 | At least 3 feeders fired `OWN_LIKE_HEAVY` in 14 days | A: one matching post per feeder, max 5. |
| `CROSS_MICRO_VIRAL_PASSIVE` | D7 | At least 3 feeders fired `OWN_VIRAL_PASSIVE` in 14 days | A: one matching post per feeder, max 5. |
| `CROSS_MICRO_FADE` | D7 | At least 3 feeders fired `OWN_FADE` in 14 days | A: one weak post per feeder, max 5. |
| `CROSS_FOLLOWER_WAVE` | Daily | At least 2 feeders and at least 30% of active feeders fired follower spike/drop signals in 14 days | A: latest post per affected feeder, max 5. |

## Anchor Signals

Anchor content signals require at least 10 anchor posts and 20 non-anchor posts in the loaded lookback. Median comparisons use D7 posts from the last 30 days and are same-media only. Same-media anchor comparisons require at least 5 anchor posts and 10 feed posts in that media type.

| Signal | Checkpoint | Trigger | Posts Sent |
|---|---:|---|---|
| `ANCHOR_GAP_WIDENING` | D7 | Same media type: `feed_median - anchor_median >= 8`, meaning the anchor is ahead because lower percentile is better | A: 3 anchor winners. B: 2 typical feed references. |
| `ANCHOR_GAP_CLOSING` | D7 | Same media type: `feed_median - anchor_median <= -8`, meaning non-anchor accounts are ahead | A: 3 feed winners. B: 2 typical anchor references. |
| `ANCHOR_CHALLENGER_SURGE` | D7 | Same media type: one competitor has at least 3 of last 10 D7 posts in that media type beating the anchor same-media median by 20+ percentile points | A: 3 challenger winners. B: 2 typical anchor references. |
| `ANCHOR_FOLLOWER_GAP` | Daily | Anchor 30-day follower gain is at least `max(2x feed median gain, 50)`, or feed median gain is at least `max(2x anchor gain, 50)` | A: latest posts from the stronger side. B: latest posts from the slower side. |

## LLM Stage 1: Post Fingerprint

Runs only inside `resolve_signal_intelligence` when a selected post has no current cached fingerprint.

Cache key:

- `post_key`
- media hash
- caption hash
- sampling policy version
- model version

Media caps:

- Reel/video: first 90 seconds, trimmed with ffmpeg when possible.
- Video inline cap: 20 MB.
- Video upload cap: 50 MB.
- Image cap: 12 MB.
- Carousel: first 3 slides, last 2 slides, and up to 3 middle slides, max 8 unique slides.

Fingerprint prompt guardrails:

- Describe what the post actually contains.
- Use visual evidence first.
- Caption is supporting context only.
- Use only what can be seen/read.
- Do not invent niche, brand, or campaign details.
- Do not explain performance.

Fingerprint JSON:

```json
{
  "topic": "",
  "audience_addressed": "",
  "hook": "",
  "opener": "",
  "payoff": "",
  "visual_sequence": "",
  "caption_role": "",
  "audio_or_text_driver": "",
  "emotional_trigger": "",
  "discussion_prompt": "",
  "craft_moves": [],
  "campaign_or_context_clues": [],
  "media_confidence": "high|medium|low"
}
```

## LLM Stage 2: Signal Card

Runs inside `resolve_signal_intelligence` after all needed fingerprints exist.

Stage 2 receives:

- `signal`
- tailored signal question
- `feeds.context_bible`
- `media_type`
- `sub_bucket`
- `metric_snapshot`
- cohort policy
- post fingerprints
- feeder handle
- feeder context role
- feeder context note
- feeder bio
- caption excerpt

Stage 2 hard rules:

- `metric_snapshot` is the only statistical baseline truth.
- Cohort B/reference posts are visual references only.
- Do not calculate a new baseline from sample posts.
- Do not claim saves, shares, or private algorithm behavior.
- If content evidence does not explain the metric, mark confidence low.
- If references exist, `watchout` must say what to avoid or what references are missing.
- Never expose internal cohort letters or database role strings.

Card JSON:

```json
{
  "title": "",
  "what_happened": "",
  "why_it_may_have_happened": "",
  "common_pattern": [],
  "do_next": "",
  "watchout": "",
  "per_post_notes": [],
  "confidence": "high|medium|low"
}
```

Low confidence cards are written to `signal_intelligence`, but the signal status becomes `suppressed_confidence`. The normal Fire UI fetches only `pending`, `fresh`, and `stale`, so low-confidence cards are hidden unless a separate considered tray is built.

## Context Form Contract

The feed form should write structured answers to `feeds.context_brief` and the editable final summary to `feeds.context_bible`.

Recommended `context_brief` shape:

```json
{
  "trackingMode": "Competitor watch",
  "accountType": "Local businesses",
  "market": "Mumbai bakeries",
  "audience": "young urban dessert buyers",
  "outcomes": ["Comments and debate", "Formats that travel"],
  "note": "Track launch cycles, celebrity visits, local slang, and campaign moments."
}
```

Rules:

- Context improves wording and interpretation; it must not gate alert detection.
- Outcome priorities may sort/highlight cards later; they must not hide other viable alerts.
- Users may skip context and add it later.
- `context_bible` is the "LLM brain" for the feed and should be user-editable before saving.

## Legacy Runtime Guardrails

The old LLM/tag pipeline must not run:

- No live import or call to `post_intelligence.py`.
- No live import or call to `pattern_alerts.py`.
- No live import or call to `checkpoint_intelligence.py`.
- No live writes to `post_intelligence`.
- No live writes to pattern-style `fire_alerts`.
- `fn_process_checkpoint` must not contain `insert into public.fire_alerts`.

Allowed legacy table references:

- Historical migrations.
- Read-only profile/history surfaces until those are fully migrated.
- Manual media repair helpers, only if they do not run LLM and are not scheduled.

## Verification Commands

Run these before shipping:

```bash
python3 -m compileall apps/worker/app
npm --prefix apps/web run lint
cd apps/web && npx tsc --noEmit
git diff --check
rg -n "post_intelligence|pattern_alerts|checkpoint_intelligence" apps/worker/app apps/web/src
rg -n "insert into public\\.fire_alerts|\\.from\\('post_intelligence'\\)" apps infra/supabase/sql docs README.md
```

Run Supabase audit SQL:

```bash
psql "$POSTGRES_DSN" -f infra/supabase/sql/minimal_engine_audit.sql
psql "$POSTGRES_DSN" -f infra/supabase/sql/engine_smoke_checks.sql
```
