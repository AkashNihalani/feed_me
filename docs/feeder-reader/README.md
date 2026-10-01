# Feeder Reader: memory model and prototypes

**Current prototype: `feeder-reader.html`** (built by `build_feeder_reader.py`,
which reads the data embedded in `feeder-wall.html`). One screen: the wall.
Every reel in memory is on it, one row per run of ten, newest on top. Nothing
lives on a separate page. Insight comes as layers you switch on over the wall.

| Layer | What the tiles show | Tap a reel | Tap a chip |
|---|---|---|---|
| **Bite** | bite word, ▲ out-bit N in a row, ▼ N in a row bit harder | a wave runs back through everything it out-bit and stops at the reel that bit harder | lights every reel in that bite band |
| **Why** | what decided the bite: the idea, the moment, the faces or the craft | lights every reel decided the same way | the reader on that driver for this account |
| **Bits** | which recurring thing it is (the account's IP, campaign or product line) | lights every other go at the same bit: how often, and when | the reader on that bit |

Above the wall sits the **dispatch**: the reader's note on the run you're
looking at ("Back with the boys"), with a pip bar of the run's bite. Picking a
run lights its row. A floating **reader card** carries the take for whatever
is tapped. "Open" sends the cover into a detail sheet with the take, the bite
chart, the driver and every other go at the same bit.

**Bite** is the Feed Me word for where a reel landed at day 7 against the
account's own memory (it replaces "resistance" in all user-facing copy):

| Word | Band |
|---|---|
| Devoured | top 10% |
| Bit hard | top 25% |
| Nibbled | top half |
| Left on the plate | bottom half |

"Out-bit the 20 before it" is the ripple. "9 in a row bit harder" is shortOf.
The reel that ends a ripple is the one that "bit harder".

**The reader's voice.** Every take is written for someone who has never seen
the reel. It says what happens first, then why it got the bite it got, with a
number only where it settles the point. It sounds like someone who has watched
the whole feed: cheeky, specific, and in the feed's own nouns ("the BMW he
can't stop roasting himself about", "Andheri East is a commute every
Mumbaikar has suffered"). It never sounds like a marketing report. Each take
names one **driver**, which the run-of-10 reader should output alongside the
take:

- **the idea**: was the joke or premise itself any good;
- **the moment**: was something already in the air (Mother's Day, a meme, a
  match);
- **the faces**: who is in it (crew, strangers, a celebrity);
- **the craft**: length, pacing, format, where the brand sits.

That is how the reader separates "the celebrity always lands" from "the
campaign hype carried it" from "the content itself was good". Driver medians
are code-computed per account, so a claim like "all nine reels with his crew
landed in his top half" is checkable.

**Plain labels.** Postcard titles ("Reframe with punchline") are internal.
Every post also carries a `label` that says what happens ("“I’m in a bad
place” → “Andheri East”"). All user-facing copy uses labels.

**Motion.** Springs are simulated and sampled into native `linear()` easing
curves, falling back to cubic-béziers. Rows pop in as they scroll into view.
Layer tags wave across the wall in posting order when you switch layers. A
tapped reel sends a sweep back through what it out-bit. Covers fly into the
detail sheet and back. Ripples work on tap, and covers tilt with a glare on
fine pointers only. Everything is disabled under reduced motion.

The pipeline implication: each new D7 post gets a label, a driver, a take and
a bit (series), or it stays a one-off, when its run is read. Code computes
bands, ripples, counts and dates. The reader writes the takes, bit takes and
run dispatch, and rewrites the bit takes every run.

**Current prototype: `feeder-wall.html`** (built by `build_feeder_wall.py`).
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
- **Bite** (called resistance in code), looking back through the same lane in posting order:
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
