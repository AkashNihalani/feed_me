# Read: when each part runs (Oct 30 2026)

Supersedes the run timing in `pipeline.md`. Costs at ₹100 to the dollar, Gemini 3.8 Flash batch and Sonnet 5.5 batch
($1 in, $5 out per million tokens).

## The idea in one line

Every post is a step, read in posting order, that moves the reader's notebook. A read is a chapter that narrates the
steps since the last one. No post is ever skipped and no read starts from a pile of posts.

## The jobs

| Job | When | Who | Cost |
|---|---|---|---|
| Collect | at posting, then day 1, 3, 7, 21; followers daily | Apify + code | ~₹1.40 a post |
| Fingerprint | day 0, within hours (links die in ~33 h) | Gemini Flash, daily batch | ~₹1 a post |
| Count | daily: every number on the page | code | free |
| Step | day 3 for each post, in posting order | Sonnet, small daily batch | ~₹0.80 a post |
| Re-step | day 7, only if the post moved enough to change its verdict | Sonnet, same batch | ~₹0.30 a post on average |
| Read | every 10 stepped posts (rules below) | Sonnet, one call | ~₹16 a read |
| Feed read | weekly, same day for every feed | Sonnet, one call per feed | ~₹16–20 a read |
| Leader alerts | daily | code first, Sonnet only on a hit | small |

## The step: one post, one move of the notebook

At day 3 each post gets one small call with its fingerprint, its counts against the usual post, its record (every other
post with the same regulars, its slice, its closest posts) and the notebook as it stands. It returns:

- the post mortem's take and read (marked "so far" until day 7);
- a notebook delta: which beliefs it confirms or breaks, whether it repeats an earlier attempt or tries something new,
  and any new regular it brings.

Code applies the delta, so the notebook moves post by post in posting order. At day 7 code compares the post's final
multiple with its day-3 one; if it crossed the usual post or moved by half or more, the post is re-stepped (new words,
corrected delta), otherwise its day-3 words stand. Day-21 shifts are left for the next read.

## The notebook (long memory)

- **Beliefs**: short statements with counts for and against ("ads sink when he pitches to camera: 9 for, 1 against").
  A new belief needs two posts before any page shows it.
- **Regulars**: people, brands, products, places and bits that come back, added from fingerprints once seen twice.
- **Open threads**: questions the next posts will answer ("does he go back to politics?").
- **Calls**: the last read's predictions, settled by code at the next read.
- **The chain**: one line per step (date · post · new attempt or repeat · multiple · what it changed), kept for the
  100-post window.

The 100-post window is working memory; the notebook is what survives a heavy week pushing posts through it.

## The read: a chapter every 10 posts

- **Trigger**: 10 new posts have had their step. Or 14 days have passed since the last read and at least 5 new posts
  have had their step. Never sooner than 3 days after the previous read, so a 53-post Fashion Week becomes two or three
  reads, not five. Under 5 new posts, no read: the numbers keep updating and every post still gets its step.
- **Input**: the 100 posts as compact fingerprints, code's facts for every part, the notebook, every chain line since the
  last read in order, and the last read's calls with how they settled.
- **Output**: the Story, the Strategy lines, the next calls, and notebook proposals that code accepts only on counts.
- **Continuity on the page**:
  - The Story opens on what changed since the last read. When a chapter covers 15 or more posts, one slide shows the
    turns in order ("cold for three days of backstage posts, then Aneet walked").
  - Strategy cards say New, Shifted or Held against the last read.
  - The streak trace already shows the order.

## A month

| Feeder | Posts | Reads | Monthly cost | Overage at ₹20 |
|---|---|---|---|---|
| Light | ~20 | 2 | ~₹100 | ₹0 |
| Anuj | ~45 | 4–5 | ~₹185 | ₹300 |
| Lakmē | ~75 | ~6 | ~₹320 | ₹900 |

Per post about ₹3.50 (collect, fingerprint, step, re-step), plus ₹16 a read. The 30 included posts come to about three
reads; every 10 posts past them cost about ₹51 and bring ₹200.
