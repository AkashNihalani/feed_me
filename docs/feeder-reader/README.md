# Feeder Reader: memory model and prototypes

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
- **Resistance**, looking back through the same lane in posting order:
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
