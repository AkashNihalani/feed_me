# Feeder reader: three simulated runs per feeder (Oct 19)

Sonnet 5.5 and Haiku 5.5, as Claude agents, each did the first three runs of Lakmé (Sep 21 – Oct 1) and Anuj (Sep 27 – Oct 11) over the v13 fingerprints: 12 runs, no OpenRouter calls.

- **Inputs, built in code** (`apps/worker/scripts/read_run_sim.py`), the way a production run would get them:
  - the 10 new posts in full, and earlier posts as lean cards;
  - each post's place in its format (90-day pool, day-7 likes, falling back to day 21, 3 or 1, and saying so);
  - the things index with each thing's feeding/fading split, and twin candidates;
  - the run's own previous state.
- **The prompt:** `apps/worker/scripts/read_run_prompt.md`.
- **Checks in code:** every place quoted, every quote, sides, rule size, post ids in prose, banned words.
- **Outputs and the side-by-side sheet:** `apps/worker/scripts/out/read_sim/` (`review.md`).

Caveats: agents run inside Claude Code at medium effort, not as raw API calls with adaptive thinking, so the writing is close to production but the cost isn't measured. Runs 1–3 see only these 30 posts, so this is a new feeder's first three weeks, not a feeder with 100 posts of history.

## Verdict: Sonnet for the run

| Across 6 runs each | Sonnet | Haiku |
|---|---|---|
| Code flags | **0** | 7: 3 places that match no post, 2 feeding posts listed as sinks, 2 post ids in prose |
| Live rules per run | 3.0 | 2.2 |
| Breaks per rule | 0.8 | 1.4 (one rule carried 7) |
| Rules dropped or narrowed | 5 | 5 |
| Its own calls, settled | 1 wrong, 3 half | 1 wrong, 3 half |

Both models land on the same big picture for both accounts. The fingerprints carry the signal, and Haiku isn't wrong about the accounts. The difference is in the reasoning:

- **Sonnet finds the dividing line and corrects itself.** On Lakmé, run 2 read Aneet's short call-outs as beating her long episodes. Run 3 overturned it: Episodes 3 and 4 (44 and 47 seconds) took 2nd of 66 and 4th of 68. "Length was never the problem; a mistake you can picture is": a chilli for plump lips against stacked acids nobody can see (20th of 64). On Anuj, it dropped its own first rule, narrowed "paid posts all sink" to "paid posts sink when the sell shows" (the Don YG flex, paid but with no product in frame, held 51st of 102), and found the character read: "taking a side on the news lands" (his satire of a government that picks up critics, 1st of 104, against a news gag with no stance, 82nd of 104).
- **Haiku keeps a rule and piles up exceptions.** "Famous names sink when played or sold" ended run 3 with 9 lands, 13 sinks and 7 breaks. "Paid posts sink unless a rapper co-stars" rests on one post, and its own why says "one case is not a law". It invents places ("20th of 66"), the one failure a product sold on "we watched the feed" can't afford.
- **Cost:** at the batch prices in pipeline.md, three Sonnet runs are ₹45–63 a month against about ₹5 on Haiku. On ₹1,299 a feeder, that difference buys rules that hold.

Haiku stays the right model for the after-write check (pipeline.md), where code has already caught places and sides.

## Nudges for the reader

In the run prompt:

1. **The character line stays put.** Both models rewrote `who` every run ("…with fashion week coming", "…even the state"). It should change only when who the account is being changes; news belongs in the run.
2. **A rule's posts are cumulative.** Sonnet listed only each run's new posts under lands and sinks (Lakmé's rule 01 went 4, then 3, then 2), so the wall can't show "right 15 of 22". Lands, sinks and breaks list every post so far that the law speaks for.
3. **A rule with more breaks than sinks gets narrowed or dropped** that run, never carried. Code flags it if the model doesn't.
4. **The case that makes a rule different can't rest on one post** ("unless a rapper co-stars"); one case goes to still open.
5. **Keep:** the calls (specific and settled honestly), the twins, the breaks with reasons, the day-3 caveats, the plain Around rows and the Ranks "why here" lines. Those worked on both models.

In code, around the run:

6. **Bounce, don't publish.** A place that matches no post, a post on the wrong side, or a post id in prose sends the run back with the flags. My checker caught all 7 of Haiku's; Sonnet had none.
7. **Cap live rules at 5 in code too.** Sonnet's Anuj run 3 hit exactly 5.

## What the runs exposed upstream

- **Paid flags exist only for the posts we scraped.** The brand-deal rules on both accounts rest on Instagram's paid label, which the worker never stored, so 16 of Anuj's 30 posts have no flag. His J&B Scotch reel reads like a deal but counts as unpaid. Store `is_paid_partnership`, `coauthor_producers`, `tagged_users` and `length` from every scrape.
- **Names from captions need a check.** Anuj's best post ("raste pe chalte chalte yash dikh gaya", 2nd of 92) has the fingerprint calling Yash "a friend". If it's the actor Yash, the post is a parody of his fight scenes, and rules about famous names read it backwards. Worth one look.
- **Missing day-7 counts** (12 of Lakmé's 30, 8 of Anuj's) mean some places rest on day 1 or day 3. Both models noted it; the checkpoint gap should be fixed before the counts are trusted.

## Next

Apply nudges 1–4 to the prompt and the bounce to the check, then re-run Sonnet's three runs on both accounts (about the same agent time) and compare against this pass.

## Pass two: the server does the counting (Oct 19)

The same 12 runs on prompt v2 (`apps/worker/scripts/read_run_prompt_v2.md`) with a server apply layer (`apps/worker/scripts/read_run_sim_v2.py`). The model tags posts ([P07]) and never writes a place. The server:

- prints places from the tags;
- splits a rule's posts into fed and faded, keeps its earlier posts, and assigns ids;
- moves a rule resting on one post to still open, drops one with more posts against it than for it, and keeps the five live rules with the most posts;
- keeps a "held" rule's words, and keeps the character line unless the model says the character changed;
- computes storyline status and how each idea lands.

Outputs and the sheet: `apps/worker/scripts/out/read_sim_v2/` (`review.md`). Same inputs as pass one: the run-1 packs are byte-identical. One mid-test fix: from run 3 on, a new rule lists every post that backs it, not just the run's (the first wording lost a new rule's earlier evidence).

| Across 6 runs each | Sonnet, pass 1 | Sonnet, pass 2 | Haiku, pass 1 | Haiku, pass 2 |
|---|---|---|---|---|
| Errors that would publish (invented places, wrong sides, quotes not in a post) | 0 | 0 | **7** | **0** |
| Character line rewritten | 2 per feeder | 0 | 2 per feeder | 0 |
| Posts against a rule, on average (most on one rule) | 0.8 (3) | 0.5 (2) | 1.4 (7) | 0.4 (2) |
| Live rules that overlap | not checked | 4 pairs | not checked | 1 pair |
| The server had to fix in the draft | — | nothing | — | nothing |

**What it fixed.** Haiku's accuracy problems are gone. It can't misquote a place it never writes, and it can't put a post on the wrong side when the server sorts them. Both models now keep the character line, and the rules carry their posts from run to run. Neither model wrote a number, so the server never had to bounce one.

**What it cost.** On Lakmé, both models' rules got blunter. Sonnet's pass one found the dividing line in Aneet's posts: a mistake you can picture (a chilli for plump lips, 2nd of 66) against one you can't (stacked acids, 20th of 64). Pass two ended with "Aneet as Skin Goals Officer carries the feed" (14 posts) and "Aneet busts habits viewers already have" (10), largely the same posts, with no break between them. Three causes, all in the test setup:

1. **Fed or faded is too coarse on a hot streak.** 26 of Lakmé's 30 posts are in the top half, so every Aneet post counts as "for" and nothing breaks. Pass one compared places, which showed the gradient inside the top half.
2. **The line "a rule with nothing against it is suspicious" fell out of the v2 prompt.**
3. **"Don't reword a rule that still holds"** pushed the model to "stronger" instead of "narrowed".

On Anuj, whose posts split between feeding and fading, both models stayed sharp. Sonnet: "He lands when he names the week's outrage", "Rapper-style flexing lands when nothing is sold", "Football jokes land below his usual post". Haiku: "Wearing a named person fades; unnamed officials feed", "Performing a rap track in character feeds".

**Sonnet against Haiku now.** With the server in front, Haiku is accurate enough to publish. The difference left is depth. Sonnet reads stance ("names the week's outrage"); Haiku writes rules that say little ("Paid posts have fed so far") and pairs that overlap in meaning ("posts that sell a product fade" next to "paid posts fade").

## Pass three, if we run it

- **Rules see bands, not just sides.** The server lists a rule's posts by band (Top 10%, Top 25%, Top half, Bottom half), and the prompt asks what separates the best of a rule's posts from its weakest whenever they sit in different bands. Lakmé's gradient lives there.
- **A rule with nothing against it after 8 posts is suspicious again,** and the model has to look for its weakest case.
- **Overlapping rules bounce:** two live rules sharing most of their posts come back to be merged or split.
- **Narrowing beats "stronger"** when a rule's posts spread across three or more bands.

## Pass three: bands, and a bounce before publishing (Oct 19)

Prompt v3 (`apps/worker/scripts/read_run_prompt_v3.md`) and server v3 (`apps/worker/scripts/read_run_sim_v3.py`):

- **What the server shows the model:** each rule's posts by band.
- **What the prompt asks:** what separates a rule's higher posts from its lower ones; a rule with nothing against it is suspicious again; narrowing counts as learning.
- **What bounces a draft, once, back to the same agent with the notes:**
  - two rules standing on most of the same posts;
  - a rule with eight or more posts and nothing against it;
  - a rule spread over three bands still called "stronger";
  - posts on the wrong side of a one-way law ("…lands" listing faded posts);
  - second person (a character line in second person would otherwise be frozen);
  - written numbers, unknown tags, banned words.
- **What carries over:** anything left after the bounce goes into the next run's pack as server notes.
- **Prompt examples:** every example from these two accounts is gone (pass one and two had an Aneet and a Ronaldo example in the prompt).

Outputs and the sheet: `apps/worker/scripts/out/read_sim_v3/` (`review.md`).

| Across 6 runs each | Sonnet 1 | Sonnet 2 | **Sonnet 3** | Haiku 1 | Haiku 2 | **Haiku 3** |
|---|---|---|---|---|---|---|
| Errors that would publish | 0 | 0 | **0** | 7 | 0 | **0** |
| Live rules per run | 3.0 | 3.0 | 3.5 | 2.2 | 2.8 | 2.0 |
| Posts per rule | 4.2 | 5.0 | 3.7 | 7.4 | 4.8 | 6.3 |
| Posts against a rule, on average | 0.8 | 0.5 | **1.5** | 1.4 | 0.4 | **1.8** |
| Overlapping rule pairs in the published Read | — | 4 | **0** | — | 1 | **0** |
| Character line rewritten | 2 per feeder | 0 | 0 | 2 per feeder | 0 | 1 (to fix second person) |
| First drafts bounced | — | — | 4 of 6 (5 notes) | — | — | 4 of 6 (5 notes) |
| Post ids left bare in prose | — | 1 | 6 | — | 19 | 31 |

**What changed: the dividing lines came back, on both models.**

- **Lakmé, Sonnet:**
  - "Officer wins when she stops a tempting habit". The chilli-lip sketch (2nd of 66) and the "no-makeup look is a scam" friend (4th of 68) sit above the line. The morning-routine teaser (18th of 66) and the acids sketch (20th of 64) sit below it, each listed against with a reason.
  - "A shelf with no story behind it sinks". The twin is the same blush stick swatched down an arm (55th of 65) and packed into a phone case as a trend (21st of 65).
  - Also: "Known face lifts product carousels; events need none", "Fashion Week dates and schedules land high", "A paid label costs nothing; the idea decides".
- **Anuj, Sonnet:**
  - "A pointed stance on live news lands highest". His government satire is 1st of 104. The same Election Commissioner as a silent bathrobe lounge sank to 56th of 97, and a Ronaldo rant to 64th of 98.
  - "Robe flex lands with a woman beside him". Next to Don YG, or a woman fanning cash, it sits 21st to 51st. Alone, "the robe is a punchline" (89th of 95, 94th of 102).
  - Its run-1 rule "short, wordless reels land" was dropped by the server once the evidence went against it.
- **Lakmé, Haiku:** "Caught mid-act habits top; named call-outs feed". This is the same line as Sonnet's, which pass two missed on both models.
- **Anuj, Haiku:** "Spoofed real people fade; invented characters feed" is a sharp twin. But "Turn-led scenes feed; talk to camera fades" spans 16 posts across all four bands with 6 against: a rule too wide to say much.

**Sonnet against Haiku, final.** With the server in front and bands to read, Haiku is accurate and finds the big lines. Sonnet is still the better reader:

- **Tighter rules:** 3.7 posts each, against Haiku's 6.3.
- **Twins inside its rules,** and stance read as character ("says what he thinks").
- **Prose that names posts:** Haiku leaves five times as many bare ids.
- **One claim per law.** Haiku tends to fuse two claims into one law ("X feed; Y fade").

**Recommendation:** Sonnet runs the Read, with this server in front. Haiku becomes the cheaper fallback for the spend cap's top tier (pipeline.md: over ₹250 projected), where today's plan drops to fewer runs.

## Left for production, from pass three

1. **Bounce bare ids:** a post id outside a tag (Haiku: 31).
2. **Let a rule release a post.** The server keeps a rule's earlier posts, so the model can move one to against but can't take it off. An overlap from run 1 then can't be cleared. A rule needs a `release` list.
3. **One claim per law.** A law with two halves ("…feed; …fade") can't be side-checked; the contrast belongs in the why.
4. **Bounce rules that span every band** with many against (Haiku's 16-post rule).
5. **The renderer takes tag lists:** "[P01, P03]" now prints every place; it had printed them raw.
