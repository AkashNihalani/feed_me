# Read: the pipeline, and what it costs per feeder

> Oct 30 2026: when each part runs is now in `cadence.md` (a step per post, a read every 10 posts), and the feed page and
> Leader tier are in `feed.md`. The sections below remain the cost history.

**Decision (Oct 18 2026, replaces Oct 17).** Models: Gemini 3.8 Flash for fingerprints and Claude Sonnet 5.5 for the run, both on OpenRouter's Batch API.

Prices: Anthropic's model table (cached Oct 6, checked Oct 18); OpenRouter's Batch API docs and launch post (half price, 24-hour window, Anthropic Messages requests supported); and third-party listings for Gemini 3.8 Flash. OpenRouter's batch pages for Sonnet 5.5 and Gemini 3.8 Flash exist, but their prices didn't load, so confirm both before budgeting. ₹100 to the dollar as a buffer. A typical feeder is 30 posts a month. Budget: ₹150–200 per feeder per month, worst case included.

## The answer

Drop postcards. Gemini 3.8 Flash fingerprints each post on the day it goes up. Sonnet 5.5 writes each post's post mortem at its day 7, against the same memory the run keeps, with depth scaled to how surprising the result was. Every 10 posts, one Sonnet 5.5 batch call reads all 100 posts with every post mortem and writes the rule moves, the Feeder File's things and the run read. Code checks every quote, number and post id before anything is published. Steps and costs are in the next section.

- **About ₹161 a month** for a typical reel account (30 posts) with the savings below. A brand posting 60 twenty-slide video carousels a month costs **₹376–506** trimmed and **~₹1,271** untrimmed, the absolute worst case.
- Postcards would save about ₹17 a run (₹50 a month). That's the whole price of keeping every detail, and it's cheap.
- Sharpness over 100 posts comes from structure, not compression: each post in its own labelled block, code handing over every count, and code verifying every citation.

## Dividing it into steps (Oct 19, at the batch prices on OpenRouter)

Gemini 3.8 Flash batch is $0.375 in / $1.875 out per million tokens; Sonnet 5.5 batch is $1 / $5 (OpenRouter's model pages, Oct 19). Every step runs as a daily batch, and most batches finish within the hour.

| Step | Model | When | What it reads | Cost |
|---|---|---|---|---|
| 1. Fingerprint | Gemini 3.8 Flash | Day 0, the day it's posted. No choice: the Instagram link has to be live | The media | ₹0.45–0.75 a typical reel, ~₹1–1.8 a 2-minute reel, ~₹1.1–1.5 a 20-slide carousel |
| 2. Post mortem | Sonnet 5.5 | Day 7, when its place is final | Its fingerprint, ~6 posts code picks for it, a one-line index of the rest, and the reader's memory: the rules, the things, the open questions, the last call | Full ~₹3.3 (reel) to ~₹4.7 (carousel); short ~₹1.4 |
| 3. The run | Sonnet 5.5 | Every 10th post's day 7, at most once a week | The 10 new fingerprints in full, every post mortem, and the older posts in full (~₹28) or lean (~₹21) | Rules, the Feeder File's things and headline, the run read |
| 4. Check | Code, then Haiku 5.5 | After each write | The cited fingerprints only | ~₹0.4 |

**Why Sonnet writes the post mortems:**

- One reader, one memory, one voice.
- The post mortem applies the rules and things the run wrote. It says which rules the post kept or broke, and flags a post that fits no rule as an open question for the next run.
- Haiku would be applying rules it didn't write, in a voice that isn't the run's, at the moment people read the most.
- When a run changes its mind about a post, its post mortem shows "updated in run 5". The page never contradicts itself silently.

**What it really costs, with the Feeder File and the rules in every call (Oct 19).** Every Sonnet call carries the reader's memory (the Feeder File's things, the rules with their evidence, the open questions, the last call) on top of the fingerprints. Earlier tables assumed that was ~6k tokens with light thinking. Counted honestly it's ~20–30k, with real thinking, and that moves every number.

| Per feeder per month | Everything full | With the savings | Savings + caps |
|---|---|---|---|
| Typical reels, 30 posts | ~₹376 | **~₹161** | — |
| Typical reels, 60 posts | ~₹751 | ~₹272 | — |
| 20-slide carousels with video, 30 posts | ~₹635 | ~₹301 | ~₹224 |
| 20-slide carousels with video, 60 posts | **~₹1,271 (the absolute worst case, $12.71)** | ~₹506 | ~₹376 |

The worst case assumes:

- every post is a 20-slide carousel, half the slides video (120 seconds of video in all), images at high resolution;
- every post gets a full Sonnet post mortem;
- every run reads all 100 fingerprints and every post mortem in full;
- the memory is sent in full on every call, and thinking runs high.

Without a cap on video per post, a carousel of ten 60-second videos makes the fingerprint ~₹8 instead of ~₹3, and the 30-post worst case reaches ~₹800.

**The savings, in the order they matter:**

1. **Post mortem depth by surprise.** At day 7, code knows whether a post landed where its things predict. About a third of posts (the surprises, the top quarter, anything new) get the full post mortem. The rest get a short one: what it is, where it landed, the rule it confirms.
2. **Each call gets only the memory it needs.**
   - A full post mortem gets the rules and this post's things (~8k tokens), not the whole Feeder File.
   - A short one gets the rules' one-line laws (~2k).
   - The run gets everything, since it rewrites it.
3. **Lean where it's only context.** The post being judged and the run's 10 new posts are read in full. Comparison posts and older posts go in lean: code renders them from their own fingerprint fields at ~40% of the size, with no model rewriting them. The run reads the post mortems as one-liners.
4. **Medium thinking for Sonnet, low for Gemini, medium-resolution slides.**
5. **Heavy posters run every 15 posts** instead of every 10.

**The caps** (for heavy carousel accounts): read the first 10 slides and up to 60 seconds of video per post.

**What this means for the ₹200 budget:**

- It holds for the typical account (about 30 reels a month) with the savings: ~₹161.
- Posting volume is the biggest multiplier, because fingerprints and post mortems are paid per post.
- A brand posting 20-slide video carousels every day lands at ₹376–506 even with everything trimmed.

So either charge heavy carousel brands a higher tier, or treat ₹200 as the average across feeders and let the cheap ones carry the heavy ones.

## What today's feeders would cost (database scan, Oct 19)

**Plan:** ₹1,299 a feeder a month, with 30 posts included and a charge per post above that. The AI cost has to stay under ₹300 at the cap.

**What was scanned:** all 33 feeders, posts from 20 Jul to 18 Oct (read-only).

- **Carousel size** comes from `carousel_urls`: one URL per slide, `.mp4` marks a video slide. That's known for 326 carousels: median 5 slides, 90th percentile 11, and 11% of slides are video. The other 387 carousels store only one URL; they're given their feeder's typical size.
- **Reel length is recorded nowhere** (`duration_seconds` is empty), so each feeder is priced at 30-second and at 120-second reels, the cap. Length only changes the Gemini cost (₹0.7–1.7 a reel). The Sonnet side reads fingerprints whose length is capped either way.
- **Prices:** the trimmed pipeline above (gentle fingerprint limits, lean re-reads, post mortem depth by surprise, a run every 10 posts), at OpenRouter's batch prices.

| Feeder | Posts a month | Reels / carousels / images (90 days) | Typical carousel slides | AI cost at the 30-post cap, 30-s reels | Same, 120-s reels | Each post over 30 |
|---|---|---|---|---|---|---|
| sufimotiwala | 29.7 | 73 / 12 / 4 | 11 | ₹148 | ₹176 | ₹5.95 |
| anuj.mp4 | 45.3 | 106 / 22 / 8 | 10 | ₹148 | ₹175 | ₹5.83 |
| traya.health | 33.7 | 75 / 16 / 10 | 5 | ₹146 | ₹172 | ₹5.73 |
| hudabeauty | 28.7 | 63 / 19 / 4 | 7.5 | ₹143 | ₹168 | ₹5.85 |
| lakmeindia | 65.0 | 96 / 86 / 13 | 5 | ₹147 | ₹165 | ₹5.48 |
| bunshah | 28.0 | 72 / 1 / 11 | 5 | ₹137 | ₹164 | ₹5.87 |
| kaybykatrina | 40.0 | 72 / 32 / 16 | 2 | ₹143 | ₹164 | ₹5.48 |
| miabytanishq | 30.3 | 46 / 24 / 21 | 4 | ₹143 | ₹160 | ₹5.35 |
| kendrascott | 37.3 | 42 / 44 / 26 | 5.5 | ₹145 | ₹158 | ₹5.27 |
| theofficialpandora | 30.0 | 32 / 35 / 23 | 3.5 | ₹141 | ₹153 | ₹5.12 |
| saniyamirwani | 24.0 | 57 / 6 / 9 | 7 | ₹117 | ₹139 | ₹5.79 |
| trysugar | 27.3 | 24 / 27 / 31 | 4 | ₹128 | ₹137 | ₹5.00 |
| amamajewels | 23.3 | 33 / 37 / 0 | 5.5 | ₹116 | ₹129 | ₹5.51 |
| srishtigargg | 22.0 | 51 / 8 / 7 | 8 | ₹108 | ₹127 | ₹5.78 |
| mejuri | 23.3 | 25 / 35 / 10 | 5 | ₹111 | ₹121 | ₹5.19 |
| man.matters | 21.0 | 45 / 13 / 5 | 4 | ₹102 | ₹119 | ₹5.68 |
| waidonofficial | 20.0 | 50 / 4 / 6 | 5 | ₹98 | ₹117 | ₹5.85 |
| swarovski | 22.3 | 15 / 32 / 20 | 2.5 | ₹103 | ₹109 | ₹4.89 |
| pradabeauty | 20.0 | 29 / 8 / 23 | 2 | ₹94 | ₹105 | ₹5.26 |
| mr.misuu | 18.0 | 36 / 13 / 5 | 6.5 | ₹88 | ₹102 | ₹5.64 |
| reneeofficial | 17.3 | 38 / 14 / 0 | 5 | ₹86 | ₹100 | ₹5.80 |
| nutrafol | 18.3 | 29 / 18 / 8 | 4.5 | ₹89 | ₹100 | ₹5.47 |
| rohitraghv_ | 16.3 | 41 / 4 / 4 | 7.5 | ₹81 | ₹96 | ₹5.90 |
| taneesho | 16.7 | 37 / 12 / 1 | 7.5 | ₹82 | ₹96 | ₹5.78 |
| getdrnk.in | 16.7 | 28 / 15 / 7 | 4 | ₹81 | ₹92 | ₹5.51 |
| mokaiindia | 17.0 | 13 / 37 / 1 | 5 | ₹84 | ₹89 | ₹5.23 |
| kumarijewels | 15.0 | 23 / 16 / 6 | 4 | ₹72 | ₹81 | ₹5.38 |
| thecroffleguys | 11.7 | 32 / 3 / 0 | 5.5 | ₹58 | ₹70 | ₹5.99 |
| hims | 13.3 | 15 / 17 / 8 | 6 | ₹64 | ₹70 | ₹5.22 |
| the.rebel.kid | 11.3 | 27 / 3 / 4 | 5 | ₹55 | ₹66 | ₹5.81 |
| veronicasbombay | 11.3 | 17 / 16 / 1 | 5 | ₹55 | ₹61 | ₹5.42 |
| thebombaycanteen | 10.7 | 13 / 18 / 1 | 5 | ₹53 | ₹58 | ₹5.40 |
| letsgetbakedinbombay | 10.3 | 18 / 13 / 0 | 2 | ₹50 | ₹57 | ₹5.47 |

**What it says:**

- **At the cap:** the median feeder costs ~₹102–117 a month and the most expensive ~₹148–176. Every feeder stays under ₹180, even at 120-second reels.
- **If every feeder posted the full 30:** ₹147–180 each.
- **Your margin:** at ₹1,299, the AI is at most ~14% of the price.
- **Who goes over 30:** the median feeder posts 21 a month, and 6 of the 33 post more: Lakmé 65, Anuj 45, Kay by Katrina 40, Kendra Scott 37, Traya 34 and Mia by Tanishq 30.
- **Each post over the cap** costs ₹5–6 in AI (a fingerprint, a post mortem, a tenth of a run). Any overage price from about ₹25 keeps the same margin; ₹40 a post matches what an included post is worth (₹1,299 / 30 ≈ ₹43).
- **Without the trims** the same feeders would cost about 2.3 times as much (~₹250–400 at the cap), so the trims are what make ₹300 comfortable, not optional.
- **Low posters:** a feeder posting about 10 a month only fills a run every month or so, which is part of why it's cheap.

## Staying under ₹300 a month, whatever the account posts (Oct 19)

**The account that sets the ceiling:** half its posts are 120-second reels and half are 15-slide carousels with four 15-second videos. Video is read at default resolution and slides at medium.

**Where the money goes.** Each fingerprint is read about 17 times in its life: once in its own post mortem, about 6 times as a comparison for other posts, and in up to 10 runs while it's among the last 100 posts. So what gets re-sent matters more than how long the fingerprint is.

| Worst-case account, per month | Nothing trimmed | Trim the fingerprint only | Trim what's re-read only | Both |
|---|---|---|---|---|
| 30 posts, a run every 10 posts | ₹317 | ₹255 | ₹192 | **₹177** |
| 60 posts, a run every 10 | ₹635 | ₹509 | ₹384 | ₹353 |
| 60 posts, a run every 20 | ₹468 | ₹385 | ₹318 | **₹287** |
| 90 posts, a run every 30 | ₹618 | ₹516 | ₹444 | ₹397 |

For comparison, a typical account (30 posts, 30-second reels, both trims) costs ~₹145.

**1. Trim the fingerprint at the prompt, gently.** It's the only record and it's kept forever, so cut repetition, never facts.

- Every structured field stays whole: cast, brands, places, sounds, lines, looks, moments, format, stance, the summary line, and all on-screen text verbatim.
- What gets a budget is the narrative:
  - a reel gets at most 8 beats and about 900 words;
  - a carousel gets one line per slide, its text verbatim, at most 3 beats per video slide, and about 1,300 words;
  - the transcript is capped at 180 words for skits and 400 for talking heads.
- Gemini's thinking stays low.
- This saves about 15–20%.

**2. Trim what's re-read: the big saving, about 40%.** Nothing is lost, because full fingerprints stay stored.

- A post's full fingerprint is read only where it's judged: in its own post mortem, and in the run that covers it.
- Comparison posts and older posts go in as lean cards. Code builds them from the fingerprint's own fields: summary, things, two key quotes, place and side, in ~400 tokens. No model rewrites anything.
- Each call gets only the memory it needs:
  - a full post mortem gets the rules and this post's things;
  - a short one gets the rules' one-line laws;
  - only the run gets the whole Feeder File.
- The run reads post mortems as one-liners.
- Post mortems drop the one-line index of all 100 posts and use only the posts code picked.

**3. Space the runs by volume.** A run every 10 posts up to 30 posts a month; every 20 above that.

**4. A spend cap in code, so the ceiling is a guarantee, not an estimate.**

- **What it tracks:** each feeder's month-to-date spend, from the usage every API response reports.
- **What it projects:** month-end spend at the current posting rate.
- **What it does as the projection rises:**

| Projected month-end spend | What changes |
|---|---|
| Under ₹180 | Normal: full post mortems for surprises (about a third of posts), runs on schedule |
| ₹180–250 | Full post mortems only for top-quarter posts and the biggest surprises (at most 10 a month); runs every 20 posts |
| ₹250–300 | Every post mortem short; runs every 30 posts, at most one a week |
| Over ₹300 | Fingerprints continue (they can't wait). Post mortems become code-only (place, side, things), and the next run writes their words |

- **The guarantee:** at the start of each month it sets aside fingerprint money from last month's posting volume, so the cap holds even if the month ends heavy.

With the cap, ₹300 holds for any account up to about 90 posts a month. Beyond ~100 posts a month (news and meme pages posting four or more times a day), fingerprints alone take half the budget. Those accounts need per-post pricing or a lighter fingerprint, whatever the rest of the pipeline does.

## What runs today, and why it loses detail

The Read's own path is two steps, both through OpenRouter at standard price:

| Step | Model | Where | What it loses |
|---|---|---|---|
| Fingerprint | Gemini 3.5 Flash (`POST_INTELLIGENCE_MODEL` in both env files) | worker, `fingerprint_intelligence.py`, prompt v12.1 | Neutral JSON, 340–950 words. The transcript is capped at 180 words. Production hard-codes `MEDIA_TYPE: reel`. Carousels and images have been fingerprinted only in the mixed-media test lane (`scripts/run_mixed_media_fingerprint_test.py`): 62 of Lakmé's posts (38 reels, 15 carousels, 9 images), plus Anuj, Traya, Srishti and Kendra Scott |
| Postcard | GPT-5.4-mini | worker scripts: `run_postcard_writer.py`, `run_postcard_v2_factual_test.py`, not yet in the worker app | The lossy step: the reader only ever sees the postcard, so whatever GPT-5.4-mini leaves out (who plays whom, lines, sounds) is gone. rules.md already found one rule the reader couldn't see because of it |

The worker app still carries older paths from earlier designs: a post-condensation step (Claude Haiku 4.5 by default), feeder-file chunk and merge (GPT-5.4, 5–10 posts at a time) and a per-post D7 read (Claude Sonnet 4.6). They're more compression on the same pattern, and they retire with this plan. Nothing runs on batch yet.

## The pipeline

1. **At posting, within hours:** save the media (video or every slide) to R2, then Gemini 3.8 Flash writes the fingerprint (v13, below) as an OpenRouter batch job. Most batches finish within the hour, well before the links die, which takes days. Stored forever, never shortened.
2. **Day 7, code only, instant:**
   - rank in the window (per format), feeding or fading, what it beat and what beat it, streaks;
   - things pulled from the fingerprint's structured fields: who's in it, brands, places, sounds, lines, looks and moments, each with its posts, its split, and first and last seen;
   - candidate twins, from fields: the same thing on opposite sides, far apart (see Twins below);
   - the top-10 alert fires here.
3. **The run:** fires when the 10th new post reaches day 7, at most once a week. One request to Claude Sonnet 5.5 on OpenRouter's Batch API (or Anthropic's own Message Batches), with structured output.
   - **Input:**
     - the instructions;
     - the reader's memory: the rule ledger, the things registry (names, meanings, examples), open questions and the last call;
     - code's facts;
     - all 100 fingerprints, each in its own block headed `#a35 · 12 Jun · reel · 4th of 40 · feeding`.
   - **Output, in this order:**
     - the 10 post mortems;
     - rule moves (born, sharpened, narrowed, dropped, each with post ids);
     - things registry updates (new names, meanings and example lines, and any merges);
     - the run read and its call;
     - the Feeder File's headline.
4. **Verify:**
   - Code checks that every cited id exists, every quote appears in that post's fingerprint, every timestamp exists in its beats, and every place, date and number matches code.
   - Whatever code can't verify (who plays whom, "the payoff lands at 0:03") goes to Claude Haiku 5.5 with only the cited fingerprints.
   - A claim that fails is dropped, not published.
5. **Publish:** code renders the Feeder File, the rules and the wall. The model writes; it never counts.

## What it costs

| Per run (Sonnet 5.5 batch: $1 in, $5 out per MTok, the same through OpenRouter at half price) | Input | Output (incl. thinking) | Cost |
|---|---|---|---|
| Typical reel account | ~215k tokens (100 fingerprints ~1.3k each, +20% for Claude's tokenizer, + memory, facts and instructions) | ~30k | **~₹37** |
| Carousel-heavy (20-slide posts, ~3k a fingerprint) | ~385k | ~30k | ~₹54 |
| Same, with posts older than 30 sent lean* | ~230k | ~30k | ~₹38 |
| Check pass (Haiku 5.5, under 100k tokens: $0.05 / $0.25 batch) | ~60k | ~3k | ~₹0.4 |

*Lean = code renders an older post from its own fingerprint fields (summary line, things, key quotes) at about 40% of its size. No model rewrites it, and the full fingerprint is one id away for the check pass.

| Per feeder per month | Fingerprints | Runs | Checks | Total |
|---|---|---|---|---|
| Typical reels, 30 posts, 3 runs | ₹15 | ₹111 | ₹1 | **~₹127** |
| Carousel-heavy, 30 posts, 3 runs, old posts lean | ₹45 | ₹114 | ₹1 | **~₹160** |
| Heavy poster, 60 posts, capped at 4 runs | ₹30 | ₹148 | ₹2 | **~₹180** |
| Heavy poster, every 10 posts (6 runs), no cap | ₹30 | ₹222 | ₹2 | ~₹254 (over) |

Fingerprints are priced on Gemini 3.8 Flash at batch half price, from its listed $0.75 / $3.75 per MTok, an introductory rate until Dec 31 2026. That comes to ~₹0.45 for a 15-second reel, ~₹1 for a 2-minute reel and ~₹1.5 for a 20-slide carousel.

From Jan 1 2027 the listed price doubles to $1.50 / $7.50, which adds ~₹15 a month for a reel account and ~₹45 for a carousel-heavy one. That takes carousel-heavy to ~₹205, so send its slides at medium resolution (half the slide tokens) to stay under ₹200.

## The options compared (one run, typical account)

| Option | Cost a run | A month (3 runs + ₹16) | Verdict |
|---|---|---|---|
| Postcards, then Sonnet reads 100 postcards | ~₹20 | ~₹76 | Cheapest Sonnet route, and it loses the detail the Feeder File is built from (lines, sounds, who plays whom) |
| **All 100 fingerprints, one Sonnet 5.5 batch call** | **~₹37** | **~₹127** | **Recommended** |
| Same, split in two calls (post mortems, then the brain) | ~₹58 | ~₹190 | Only if one call proves less sharp; caching doesn't rescue it (in a batch, cache hits are best-effort; outside a batch the cache write costs more than the second call) |
| Opus 5.5 batch ($2 / $10) | ~₹73 | ~₹235 | Over budget |
| Haiku 5.5 batch (over 100k tokens: $0.25 / $1.25; its tokenizer counts ~30% more) | ~₹11 | ~₹50 | Worth testing; if it holds the depth bar, it's a third of the cost |
| GPT-6 Sol batch ($1 / $5, double past 272k tokens) | ~₹37 | ~₹127 | Same price for reels; carousel-heavy accounts cross 272k and double |
| Memory only (the new 10 in full, old posts from memory) | ~₹20 | ~₹76 | Saves no more than postcards (the output is most of the cost), and nothing old is ever re-read: the postcard problem again |

## Keeping 100 posts sharp

The risk at 200k tokens isn't attention, it's attribution: the right detail on the wrong post. The guards:

- Every fingerprint sits in its own block, headed by its id, date, format, place and side.
- Code hands over every count and list (things, splits, twins, beat lists), so the model reasons over facts instead of tallying them.
- The output order is fixed: post mortems first, then the account level. Every claim carries post ids, enforced by the output schema.
- Code verifies citations before a model is asked to; Haiku checks only what code can't.
- **Test before switching:** replay Anuj (40 posts) and Lakmé (89) through each option and count the errors per 100 claims the check catches. Run Sonnet 5.5 against Haiku 5.5 on the same replay.

## Fingerprint v13: what the Read now needs from it

Keep it neutral and performance-blind. Add structured fields that match the Feeder File's kinds, so code can pull the things out before any model runs:

- **cast:** who's in it, their role, whether the creator plays them, real person or actor;
- **brands:** name, and how it shows up (paid label, #ad, a tag, the product on screen);
- **places, sounds** (track and artist, or original audio), **lines** (catchphrases, slang and who says them), **looks and props, moments** (event, meme or news);
- **format;**
- **stance:** who the account is being, who the joke is on, the premise;
- **one plain summary line,** which code uses for the lean view.

Two fixes on top:

- Lift the transcript cap for talking heads and commentary (180 words cuts exactly what carries those posts).
- Fingerprint carousels and images in production, the way the mixed-media test lane already does.

### Checked against today's prompts (Oct 19)

There are two live prompt families: v12.1 (production, reels only, `bite_prompts.py`) and observation_v1 (the test lane's reel, image and carousel prompts in `intelligence_engine_prompts.py`). 103 reels were fingerprinted by both:

| What the Read needs | v12.1 | observation_v1 |
|---|---|---|
| Length (median words, without caption) | 472 | 277 |
| Lines say who speaks | 7 of 103 | 1 of 103 |
| Sound described | 90 | 54 (no audio field) |
| Track named | 2 | 5 |
| Length of the reel | estimated on 30, despite "never estimate" | written on 90, all guessed (none is supplied) |

- **Who says what gets lost.** In Anuj's "in a bad place rn" reel he plays both callers through a face filter. v12.1 gives "I'm in a really bad place" to the woman and "Andheri East" to him; observation_v1 gives both to him. The joke, and the rule "Every part himself", hang on that.
- **Things are free text.** Cast, brands, co-posts, places and moments sit in prose or in mixed lists (v12.1's entities put "Young man" next to "Smartwatch on left wrist"; a face filter was logged as a cultural reference), so code can't pull them at day 7.
- **No stance, no summary line.** Both ban saying what the post is about, so the run has to rebuild the premise from descriptions, and the lean cards have nothing to start from.
- **Length isn't known.** `posts.duration_seconds` is empty for all 5,876 posts, so the 120-second rule never fires. The fix belongs in code: read the length from the video file before the call.
- **Sound names should come from Instagram, not the model.** Gemini names a track in under 5% of reels. The worker keeps only co-authors (`related_handles`, 356 posts) from the scrape; whether the scrape also carries the audio name and the paid-partnership flag is unchecked.

Keep observation_v1's lean narrative, slide notes and verbatim text; add the v13 fields above as one shared block across reel, image and carousel; let lines carry their speaker.

### The test batch

No feeder has 30 consecutive posts past day 7 with media we can still reach. The best 30-post windows: kaybykatrina Oct 3–11 (24 usable, 23 in R2), Lakmé Sep 21–Oct 1 (21: 16 in R2, 5 on links that expire this week; 14 reels, 13 carousels, 3 images), Anuj Sep 27–Oct 11 (16). The gaps need their links refreshed (`scripts/save_latest_media_for_refingerprint.py` re-scrapes and saves to R2) or the runs go ahead with holes. Lakmé's newest 30 posts are a Lakmé Fashion Week burst (44 posts Oct 15–18, confirmed from the shortcodes), none past day 7 yet.

## Twins: code proposes, the model decides

A twin is the same thing landing on opposite sides: the same pun about Andheri East (6th) and UP (35th), the same Hya Beach Edit gloss as a creator's co-post (1st) and an office-diary reel (44th). Code doesn't decide what a twin means, and it doesn't need the post mortems to find candidates:

- **From fields, at day 7 (code).** With fingerprint v13, the shared element is a field: the same line, product, person, format or place. Two posts that share it and sit on opposite sides, far apart, are a candidate pair. That's a fact, like a rank. It's what the Feeder File's "On both sides" shows: the thing's best feeding post against its worst fading one, nothing claimed about why.
- **From meaning, in the run (the model).** The same premise told two ways often shares no field ("a man too sure of himself" in a parody and in a rant). The run gets code's candidate pairs and all 100 fingerprints. It confirms or drops each candidate, adds the twins only reading can see (each citing both post ids), and uses them in the post mortems and the rules, which it writes in the same call.
- **Then code checks** that both ids exist, sit on opposite sides, and that every quote about them is in their fingerprints.

There's no loop to wait on. Candidates come from fingerprints, which exist from the day a post goes up, so the first run works the same way as the fiftieth.

## Embeddings and RAG: not now

- **RAG, no.** Retrieval is for picking what fits in the context, and all 100 fingerprints fit (~215k of Sonnet's 1M tokens). A retrieval step would decide early which posts matter, which is the postcard problem again.
- **Embeddings, not for the read.** Fields find the twins that share something nameable; the run finds the rest by reading. An embedding of each fingerprint could add candidate look-alikes for almost nothing (100 fingerprints cost a fraction of a rupee), but the run would still have to judge them. Add it only if the replay shows the run missing twins.
- **Where they would earn their place: Leader.** Comparing feeds means thousands of posts across many feeders ("who else in this feed posts like this?"). That's search, and that's when embeddings and retrieval come in.

## Product trade-off: when the words arrive

With runs every 10 posts, a post mortem arrives with its run. For the first post of a run, that can be two to three weeks after it went up. The numbers (place, feeding or fading, what it beat) are there at day 7 from code. If you want words at day 7 too, Haiku 5.5 can write a short note per post from its own fingerprint and its five closest posts, at about ₹0.1 a post (₹3 a month); the run's post mortem replaces it.

## Moving production to this

1. **Move the calls to OpenRouter's Batch API:** `POST /api/v1/batches` with the `/v1/messages` endpoint for Sonnet, chat completions for Gemini. Each batch runs on one provider, and results are read by polling the batch. Anthropic's and Google's own batch APIs are the fallback.
2. **Fix the R2 capture queue first:** fingerprints can't be written once the links die.
3. **Fingerprint v13,** with carousels and images.
4. **Write the run's prompt and output schema,** then replay Anuj and Lakmé on Sonnet 5.5 and Haiku 5.5.
5. **Retire the GPT-5.4-mini postcard scripts and the worker's older paths** (condensation, feeder-file chunk and merge, the per-post D7 read).

Notes for the build:

- The run uses `claude-sonnet-5-5`, adaptive thinking, effort `high` (try `medium` on the replay), and `output_config.format` for the schema.
- Sonnet 5.5 can stop with a refusal, and the `fallbacks` parameter isn't accepted on Batches, so re-send a refused run on its own.

## Re-costed with real v13 sizes (Oct 19)

Measured on the 30 Lakmé fingerprints (docs/read/fingerprint.md, "Test run"). Sonnet sizes are estimated from the fingerprints' length (2,780 characters minified, ~1,000 Claude tokens; the lean card, ~330), since no Anthropic key is set up for exact counts. The written text is measured on the placeholder reads in `readerPreview.json` (Anuj: 40 posts, 4 runs).

**What gets written, and where:**

| Text | Size | Written by |
|---|---|---|
| Each post: label, take, line, post mortem (what it is, why it landed, what it did to the rules) | ~106 words, ~250 tokens with structure | The post mortem at day 7 |
| Each run: run read, dispatch, recap (why it went this way, bit hardest, left) | ~480 words | The run |
| Rules: law, because, do this, how each run moved it, the posts on each side | ~1,800 words for 5 rules | The run |
| Repeats: each idea's divider (the idea, how it usually lands, the reader's line) | ~600–900 words for ~10 ideas | The run |
| Ranks: each band's "Why here" (what's in it, why it lands here), plus each post's rule line | ~380 words, plus the post mortems' rule lines | The run |
| Feeder File headline and kinds, who, formula, open questions, stories | ~750 words | The run |
| Ranks' order, bands, places, splits, the things index, twin candidates | — | Code |

So a run that rewrites the whole account state writes ~4,000–4,500 words (~6–7k tokens) plus its own ~480, about 8k tokens with structure, before thinking. Output costs five times input, so the run's writing is the biggest line in the bill.

| Step | Tokens | Batch cost |
|---|---|---|
| Fingerprint (Gemini 3.8 Flash) | ~6.2k in (2.3k of it the prompt), ~1.2k out | ₹0.45 a post (₹45 per 100) |
| Post mortem (Sonnet 5.5) | ~8k in, ~0.3k written + 1–1.5k thinking | ₹1.4–1.7 |
| Run, rewriting all of the account state | ~61k in (10 full, 90 lean) or ~121k (all full); ~8k written + ~10k thinking | ₹15–21 |
| Run, writing only what changed | same in; ~3–4k written + ~10k thinking | ₹13–19 |

- **A month at the 30-post cap**, Lakmé-like media: 30 fingerprints ₹14, 30 post mortems ₹42–51, 3 runs ₹39–63, checks ~₹5: **about ₹100–135**.
- **Heavy media** (120-second reels, 15-slide carousels): fingerprints about triple and come out longer, so about ₹145–185.
- **A new feeder's first 100 posts:** ₹45 to fingerprint, then one run that writes everything from scratch: the account state and all 100 posts' texts (~33k written + ~15k thinking on ~121k in), about ₹36. About ₹80 in all; ₹205–235 if each post gets its own post mortem instead.
- **The saving worth building:** have the run return only what changed (rules born, sharpened or dropped; ideas and bands whose posts moved) and let code merge it into the stored state. It cuts ~₹2–3 a run and keeps unchanged text word-for-word stable between runs.
