# Read, in order (plan, Oct 25 2026)

Read does one job: explain an account through what only tracking every post can show. That means how often it makes each kind of post, what each kind gets, where its likes come from, what changed, and what moved followers. It says this one way, in the same order, every time.

Live proposal (Anuj and Lakmē, real posts and numbers, hand-written copy): https://claude.ai/artifact/Ef2DgcQJyp4m62brp7TqfY

## What's wrong today (measured on the working tree)

- **Every surface explains why.** Anuj's government reel (P27) shows up on 18 surfaces: 9 on the page (two stories, its tile, the run 3 sheet ten times, three other post mortems, the wall lit by two rules) and 9 in the Feeder File (ring, account page, two law pages, four thing pages, its post page). "218×" is printed about 25 times. Lakmē's chilli reel (P21) is on 17 surfaces, with "74×" about 25 times.
- **About 20 number formats, and they disagree.** Top N% (rank in the format's 90-day pool at day 7), N× / N% of usual, N of M beat, 1 in N, best of last N, none of the K before it, beat N, comments per 1,000 likes, N places apart, held N of M since, and more. Run 3's typical post is Top 50% on the breaker, 100% on its run sheet and 1.0× in the stories. "Beat his usual" means rank ≤ pool/2 on the wall, the map and the story rows, but x > 1 in the ledger, so one story says 5 of the last 10 and the next says 4. The lens note says Top % is "against his other posts in the same format over 90 days"; the footer says "against his own last 30" and still calls rules and post mortems hand-written placeholders. Lakmē's schedule carousel (Top 2%, 2,497 likes) reads as better than its 92× reel (Top 3%, 30,749 likes).
- **Rules state craft anyone could see** ("His 0:00 text holds the whole joke", "Open on one trend people really do"). The counted facts only Feed Me has are buried in dense sentences ("9 of 30 posts, 1 in 3, and 1 of 9 beat his usual, typically 39% of it, holding 2% of the likes").
- **"Beat its usual" stops sorting when an account changes.** 25 of Lakmē's 30 posts beat a usual set before the officer arrived.

## Move 1: one way to say numbers

| Say | Means |
|---|---|
| His usual | a typical post of the same format over the 90 days before it; said once at the top as likes ("His usual reel gets about 3,800 likes") |
| 218×, half his usual | one post against its usual; words below 1 (three quarters, two thirds, half, a third, a quarter, a fifth, a sixth, a tenth) |
| A hit / under half | 2× the usual or more / less than half; one hit rate per account ("1 in 4 of his posts is a hit") |
| 1 in 3, 6 of 9 | how often, out of the posts read or out of one kind |
| 87% of the likes | the only percent: a share of all likes in the read |
| 1 comment per 65 likes | talk, against the usual |
| +1,796 followers | that day, against a usual day |

Leaves Read: Top N%, percentiles, rank windows (best of last 50, beat 45 of 50, none of the 97 before it), typical 0.78×, comments per 1,000 likes, "13 of 13 on the side the rule says", ordinals. Stays: "his best in 90 days", "1 in 98 of his reels get here", and the wall trace's "beat all 26 before it".

Copy rules: one number per sentence, two per story; describe every cited post. Marks on tiles and the map: hit = colour cover + crimson mark, around = grey, under half = grey + faint mark.

Guards: if one post holds more than 40% of the likes, say so first and give shares without it. If most posts beat the usual, say the bar moved and compare kinds with each other.

## Move 2: one page, four zooms

1. **The read** (stories): what is this account and what's normal for it. Owns the account-level reasons.
2. **What it posts** (new on-page section): a row per kind with 1 in N, its posts as a cover strip (hits in colour), hits / under half / typical likes; opening a row shows what the kind is and its split ("when it works / when it doesn't"). The Feeder File is this section's map, opened from any row, flown to that kind.
3. **Every 10 posts** (the wall by run): headline, counts, "Changed my read", "My call last run" (called it / half right / missed). The trace stays.
4. **The post** (post mortem): one number row (likes · × usual · hit or not · followers that day), what happens, the lesson, its twin, talk only when it stands out.

Goes: the laws list, the Ranks lens, run sheets (folded into the run block), findings as their own thing, four takes on every claim (only "What to do" asks who you are), the placeholder footer. Stays as is: the story player and its motion, the wall and its trace, the Feeder File map.

A claim lives once: the story says it, the kind counts it, the post shows it.

## Move 3: seven stories, fixed order

1. The read: who it is + the usual. 2. Where the likes come from. 3. What it spends posts on. 4. What decides a hit: the split inside the obvious driver, counts on both sides (this is where generic claims get caught: "a celebrity face works" fails, "same officer, same tie: 74× when she stops a habit you can picture, 2.6× when she names a feeling" passes). 5. What's changing. 6. What the likes don't show (followers, talk, timing). 7. What to do (by relationship) + the next test. Then the Feeder File.

## What only tracking can show (code computes each with its guard)

Where the likes come from (share of posts vs share of likes) · hit rate per kind vs the account's rate (3+ posts) · one post carrying it (over 40%) · the split (3 a side or a twin pair, same format) · form per run · a kind changing (4+ posts) · follower days (over 4× day-to-day noise) · talk (2.5× either way) · staying power (day-1 share, 25 points off the usual) · timing (chance test across all slots) · wearing out (2+ each side). Never: reach, views, watched, sent, saved.

## Move 4: rules become splits

Each law moves onto the kind it splits with both sides counted. Anuj L2 becomes, on Brand deals: "The brand is only the set: 2 of 2 held his usual. The product is the story: 6 of 7 got under half." Lakmē L1 becomes, on Aneet's reels: "She stops a habit people really do: 65× to 92×. She reads a list or names a feeling: 1.9× and 2.6×." Dropped laws live in the run where they changed ("Changed my read: the costume isn't the reason").

## Build order

1. **One yardstick.** New `apps/web/src/components/read/readNumbers.ts` (×, words below 1, hit/around/under, 1 in N) used by `readerEngine.js` (tile, beatLine, postStats, plainStats, postExplain, runExplain, note), `feederSystemEngine.js` (heights, place(), dossPage chips, rule cards), `readProofs.js`, `ReadStories.tsx`; shade classes in `readTab.css`. One definition of "beat the usual" (x > 1) everywhere. A data check rejects the old forms. Done when no Top %, percentile or rank window shows anywhere in Read.
2. **Seven stories.** `read_now_pack.py` v5 (the eleven counts with guards), prompt v5 (seven fixed slots), `read_now_check.py` v5 (grammar, one number per sentence, a count in every headline), `read_v12_to_page.py`. Run Anuj, Lakmē, then Kendra Scott. Done when the generated stories read like the live proposal with no hand edits.
3. **What it posts.** One list of kinds for the read (run reader lanes = the map's "Kinds of post" wedge); the on-page section; a row opens the map at that kind. Done when every kind name on the page is a node on the map.
4. **Rules to splits.** Laws onto kinds; the law list leaves the map panel; law pages become a block on the kind page. Done when there is no separate rules list.
5. **Runs and post mortems.** Run block = headline, counts, changed, last call; run sheet folds in; Ranks retires. Post mortem = one number row and at most four sections.

## Open calls

- Feed and Lead still lead with Top %; follow later or keep it as Feed's sorting number (recommended: change Read first, decide after).
- Hit at 2×, under half at ½ (recommended; both sample accounts land at 1 hit in 4 over 90 days).
- Ranks: retire (recommended) or keep as a "best first" sort.
- Start with step 1 (recommended).
