# Feeder Reader: memory model and prototypes

**Current prototype: `feeder-reader.html`** (built by `build_feeder_reader.py`,
which reads the data embedded in `feeder-wall.html`). One screen, two parts.

**The hero** is the reader's dispatch for the run you pick ("Back with the
boys"), three numbers (top-25% count, best reel, longest streak beaten) and
the **pulse**: every reel in memory on one line, oldest to newest, higher =
ranked higher. Run bands sit under it, each with its typical rank, and the
best of each run is pinned with its cover. Picking a run slides a highlight
over its band. Tapping any reel draws its level line on the pulse: back
through every reel it beat, until it reaches one that did better.

**The wall** is the feed itself, laid out like an Instagram grid: newest
first, with each run as a divider. "By rank" re-sorts the same covers into
tiers (top 10%, top 25%, top half, bottom half) with one animated move. The
layers go on top of it:

| Layer | What the tiles show | Tap a reel | Tap a chip |
|---|---|---|---|
| **Rank** | #rank, ▲ beat the last N in a row, ▼ the last N did better | the trace counts back through the feed, across runs, numbering each reel it beat, and flags the one that did better | lights every reel in that tier |
| **Why** | what decided its rank: the idea, the moment, the faces or the craft | lights every reel decided the same way | the reader on that driver |
| **Bits** | which recurring thing it is (an IP, a campaign, a product line) | lights every other go at the same bit, on the wall and on the pulse | the reader on that bit |

A floating **reader card** carries the take for whatever is tapped. "Open"
sends the cover into a detail sheet.

**Language.** Feed Me is about Instagram feeds, so the copy stays in plain
feed terms: rank, top 10%, top quarter, bottom half, "beat the last 20",
"the last 9 did better", "kept scrolling". No food metaphors.

**The reader's voice.** Every take is written for someone who has never seen
the reel. It says what happens first, then why it ranked where it did, with a
number only where it settles the point. It sounds like someone who has
watched the whole feed: cheeky, specific, in the feed's own nouns ("the BMW
he can't stop roasting himself about"). It never sounds like a marketing
report. Each take names one **driver**, which the run-of-10 reader should
output alongside the take:

- **the idea**: was the joke or premise itself any good;
- **the moment**: was something already in the air (Mother's Day, a meme, a
  match);
- **the faces**: who is in it (crew, strangers, a celebrity);
- **the craft**: length, pacing, format, where the brand sits.

That is how the reader separates "the celebrity always lands" from "the
campaign hype carried it" from "the content itself was good". Code computes
how each driver ranks per account, so a claim like "all nine reels with his
crew landed in his top half" can be checked.

**Plain labels.** Postcard titles ("Reframe with punchline") are internal.
Every post also carries a `label` that says what happens ("“I’m in a bad
place” → “Andheri East”"). All user-facing copy uses labels.

**Motion.** Springs are simulated and sampled into native `linear()` easing
curves, falling back to cubic-béziers. The pulse line draws itself in and its
pins drop onto their peaks. Covers pop in as they scroll into view. Layer tags
wave across the wall in posting order when you switch layers. The trace hops
cover to cover along a rail with a running count. Re-sorting by rank moves
every cover to its new place. Covers fly into the detail sheet and back.
Ripples work on tap, and covers tilt with a glare on fine pointers only.
Everything is disabled under reduced motion.

The pipeline implication: each new D7 post gets a label, a driver, a take and
a bit (series), or it stays a one-off, when its run is read. Code computes
bands, ripples, counts and dates. The reader writes the takes, bit takes and
run dispatch, and rewrites the bit takes every run.

**Earlier prototype: `feeder-wall.html`** (built by `build_feeder_wall.py`).
The feed as a living wall: one row per run of ten, newest on top. Posts glow
by where they landed and dim when they landed low. Tapping a post sends a wave
through every post it beat and marks the post that stopped it. Viewpoint lenses
light the wall. Each post opens to its angle, a storyboard of its beats and
its twin. Thumbnails are mock covers built from each postcard's hook and scene.

### The angle layer (added after review)

Topics don't explain landings ("it's set in Mumbai"). The angle does. For each
new post, the run-of-10 reader writes:

- **Speaks for**: whose experience the post voices ("every Mumbaikar").
- **Pokes at**: who or what the joke is on ("Andheri East", "his own flex").
- **The feeling**: the thing the audience recognises, in one line
  ("Being in a bad place has a postcode").

Readings become **viewpoints**: a stance the account takes, with a "lands
when" side and an "other side". Each one comes with code-computed medians, for
example "laughing at his own city: top 28%" against "pointing at elsewhere:
top 57%". A post that landed but carries no angle (O Pedro, Baskin Robbins)
stays "not explained yet". It isn't credited to a theme it merely appears in.

The angle is interpretation, so it lives in the reader run with memory in
hand, not in the performance-blind postcard.

## Earlier prototype: `memory-skyline.html`

`memory-skyline.html` is a self-contained prototype (open it in a browser). It
runs on the repo's sample data: the 40 Anuj postcards and ranks from
`apps/web/src/data/terraReaderRuns/anuj-w03-request.json`, and the 10 Lakmé
posts from `apps/web/src/data/runSignals/lakmeindia_run_bites.json`.
`build_memory_skyline.py` regenerates the data block. Threads, readings and
per-post reader lines in it are hand-written stand-ins for model output.

## The four layers

| Layer | Owner | Unit | Changes when |
|---|---|---|---|
| Postcard | LLM, once per post at D7 | post | never (immutable, performance-blind) |
| Landing | code | post × memory version | every version (rank re-computed in the 90d lane) |
| Threads | LLM proposes, code counts | feed | a thread is coined once and reused forever |
| Readings | LLM, once per 10 new D7 posts | feed | each run: new / held / strengthened / sharpened / narrowed / recast |

### Landing (code only)

- **Rank**: D7 position among the same lane's posts in memory (newest 100, or
  all posts in 90 days when fewer). Shown as "top N%".
- **Beat streak** (called resistance in code), looking back through the same lane in posting order:
  - `ripple`: how many posts in a row this one beat;
  - `stopper`: the first post it did not beat, with that post's rank;
  - `shortOf`: how many posts in a row ranked above it.
- **Resistance label** (deterministic, from rank + resistance together):
  - *New ceiling*: beat everything in memory and top 10%.
  - *Breakout*: ripple ≥ 5 and top 25%.
  - *Broke a lull*: ripple ≥ 4 but outside the top 25%. It beat a soft
    patch, not the account's best.
  - *Strong company*: top 25%, ripple ≤ 1, stopped by a top-15% post. A good
    post that lost to an even better neighbour.
  - *Left behind*: shortOf ≥ 5 and in the bottom half.

### Threads (the remembered world)

A thread is something the feed keeps returning to: its cast (crew vs alone),
places (Mumbai as home), worlds (football, the content business), objects
(the BMW, the robe), money (paid partners), craft (grayscale, one-name
punchlines). Every postcard is tagged against the feed's thread registry when
it arrives. Code computes each thread's median landing. That median is what
turns an observation ("grayscale footage recurs") into something useful
("grayscale scenes land at a median of top 68%").

The postcard has to carry enough to tag threads: who is on screen (solo,
same actor playing both sides, crew, real strangers), named places, whether
a partner is paid, and recurring objects or costumes. `Named:` alone misses
unnamed recurring things (the Versace robe, the same actor in a filter).

### Readings (one model call per 10 matured posts)

Input: current readings with history, thread registry with code-computed
medians, the 10 new postcards with stamped landings, and references (title +
mechanic + landing line) for the posts each reading cites.

Output, validated in code:
- one landing line per new post: what happened and why it landed there, in
  the feed's own nouns;
- every existing reading moves or holds, with evidence and boundary post ids;
- new readings only with evidence on both sides of a contrast;
- a run note (headline + 3–4 sentences);
- thread proposals (new rows) and tags for the new posts.

Status is code-assigned, not model-assigned:
- *Emerging*: fewer than 4 evidence posts, or born this run.
- *Tested*: at least 4 evidence posts, with contrast on both sides, held across 2 runs.
- *Settled*: survived 3 runs without being narrowed or recast.

Posts no reading explains stay listed as "not explained yet".

## Notes on the constitution draft

Keep the identity prose. Make the laws operational: each law should map to a
check code or a validator can run. Gaps to close:

1. Rank and resistance are read together. "Lost to the post before it" means
   nothing until you know where that post landed.
2. Compare like with like: same lane, and say when one side is a paid format.
3. A thread must be lived with before it can explain anything. A name the
   feed has never used (UP) is not the same as one it lives with (Andheri East).
4. Unexplained posts stay unexplained.
5. Speak the feed's nouns, not its slang. This settles the conflict with the
   locked account-reader prompt ("do not imitate the account's surface voice").
6. Numbers come from code. The reader quotes stamped numbers and never derives them.
7. Status thresholds are numeric and set by code (Law X otherwise has no teeth).
