# Read v5: laws, moves and findings (proposal, Oct 21 2026)

## What went wrong in v4

- **The rules stopped being rules.** v3's rules were laws that every run tested and narrowed: "Officer wins when she stops a *tempting* habit", "Brand posts sink when *the product is the plot*". v4 rewrote them as observations with receipts: "14 posts took 90% of the likes", "Sponsors get 39% of his usual". An observation has no posts that keep or break it, so the plate's kept/broke columns only fit twins, and "stronger this run" means nothing for a fact about last week.
- **Repeats lost its reading.** v3 named what the account *does* ("Playing both sides of a split: it works when the second voice stings in seconds"). v4 turned the ideas into topic buckets ("Football", "Sponsored posts") whose line points at a rule. Nothing is left to read.
- **The repetition was real, the fix was wrong.** Measured on the v4 data: each read is restated 13 to 32 times outside its own row; 10 (Anuj) and 19 (Lakmē) lines appear nearly word for word in two places (a rule's history = the run view's line); 6 of Lakmē's 7 Repeats ideas are a Feeder File node, post for post; a post shows up 8 to 9 times on average (Lakmē's Episode 1, 19). Replacing words with "(rule 03)" pointers removed the repetition and the reading with it. The fix is a different job per component, not fewer words.

## Five kinds of knowledge, one home each

| Kind | What it is | Lives in | How it changes |
|---|---|---|---|
| **Parts** | who and what is in the posts, with numbers | Feeder File map | every post |
| **Laws** (rules) | what this audience rewards: a condition and its outcome, with a record ("held 9 of 11") | Feeder File reading (merged) | each run: born, sharpened, narrowed, strained, dropped |
| **Moves** (Repeats) | what the account keeps making, named by what it does, with a read | the wall, Repeats | each run: another go; getting better or wearing out |
| **Findings** | what a run revealed: a twin, a follower jump, what sponsors got, an arc, a lane that fades | the run that revealed it, with the proof its kind needs | once; the law it moved cites it |
| **Posts** | the evidence | post mortem | |

## Merge the rules into the Feeder File: yes

A law is a pattern across parts and posts, and the map is where those are. The reading beside the map (under it on a phone) becomes: who the account is right now, then its 3 to 6 laws (number, law, record, a one-line receipt), then what it's made of (the wedge rows). Opening a law opens it in place, with why it works from the viewer's seat, its proof, and the posts that kept and broke it as cover strips; the map lights those posts and parts. The rules section under the Feeder File goes. A node's rule chip opens the law in the same panel.

## Findings live in the runs

A run is its 10 posts plus what they revealed (one to three findings), what that did to the laws, the call settled, and what to watch. Each finding gets the proof its kind needs:

- twin: both posts' beats on one clock (the punch at 0:05 against 0:18) and their likes
- followers: the daily follower line with the posts pinned on their days
- share: one bar of all the likes, the group's posts in red
- lane: where each of its posts landed against the usual
- money: each sponsored post as a share of the usual
- arc: the posts in order, hours apart
- teasers: each teaser joined to its episode

A law cites the finding that moved it: "sharpened in run 3: the same joke twice, 65×".

## Repeats gets its read back; Ranks goes

Moves named by what the account does (v3's naming), written to the v4 bar: what the move is, for a stranger; its versions with numbers; which ones land (the why stays in the law, named once); whether it's getting better or wearing out. Ranks goes: Feed ranks posts already, the map's heights and red frames show it, and every strip carries Top %.

## Anuj in this model

**Laws**
1. A stance lands when the verdict comes fast and names names. Held 3 of 4 (the commissioner told only in a caption sank). Proof: the same joke twice, 65×: the chant flips at 0:05 with five real names; the argument needs 18 seconds.
2. Followers come from his stances, not his reach. +2,349 in three days after the chant; −234 the week of the 154,313-like Yash bit.
3. Football lore sinks with his crowd. 0 of 5 at his usual, while Star Sports books him.
4. A sponsor's script sinks; his own format holds. The typical sponsored post gets 39% of his usual; the two at his usual were a price game he loses live and a collab already moving.
5. A collab lands when it brings something scarce. Don YG's unreleased verse Top 21%; the paid repeat the next day Top 50%.

**Moves:** playing both sides of a split; standing next to someone else's bit; showing off the big-shot life; taking a side on the week's news; acting out a scene with friends; cutting streams into reels; riding his football fandom.

**Findings:** run 1, the Yash bit drew 154,313 likes while followers slid, and three brand posts landed under half his usual; run 2, Don YG's verse opened an arc and the 93-second Sober skit drew 677; run 3, the twin (65×), the follower jump, football 0 for 3 as Star Sports booked him, the paid repeat trailing the premiere.

## The page

1. **Feeder File:** the map, and the reading: who it is → its laws → what it's made of. (Built Oct 21: the phone layout without the sheet, the wedge rows, node pages with numbers and a cover strip. To build: the laws in the reading, lighting the map.)
2. **The wall:** Runs and Repeats. A run's breaker carries its head and its main finding; its view carries the findings with their proofs, the law moves, the call settled, what to watch.
3. **Post mortem:** what it was and what was going on, the moments that decided it, where it landed, and chips for its move, the laws it kept or broke, and the finding it's part of.

## Pipeline

One reader run every 10 posts writes: the run's findings (kind, receipt, proof data, the viewer's-seat why), the law updates (citing findings), the move reads, the 10 post mortems. Code computes and checks every number.

## Decisions

1. Rules back to laws, merged into the Feeder File's reading, and the rules section removed?
2. Findings in the runs, each with its kind's proof?
3. Repeats back as moves with a read, and Ranks dropped?

## Built (Oct 21, same day)

Both feeders run on this model in the Read tab (uncommitted).

- **Data:** `apps/worker/scripts/read_v5_app_content_{anuj,lakme}.py` hold the words (laws, findings, moves, runs, post
  mortems); `read_v5_facts.py` reads every post's place, likes, its format's usual post, length, posting time and the daily
  follower line from the v4 reader pack; `read_v5_to_page.py` adds the numbers, checks every place the words quote, and
  writes `readerPreview.json` and both Feeder File files (`--apply`; the v4 files are kept as `*.v4.json`, v3 as `*.v3.json`).
- **Feeder File** (`feederSystemEngine.js`): the reading is the account, then "What his/its crowd rewards" (each law with
  its record, its state this run and its receipt; the dropped law struck through), then the wedges. A law opens its page:
  its proof, why it works, do this, what we can't see, its posts on each of its two sides as cover strips with a line
  each, how it came to be (each run opens that run's read), the parts it covers, and "Light its posts on the wall". The
  map lights the law's posts and parts. Node chips and "(law 03)" links open laws in place.
- **Proofs** (`readProofs.js`, styled under `.pf` in `readTab.css`): twin (shared clock, N rows), followers (daily bars,
  pinned posts, gaps marked), money (share of the usual, the usual and the typical drawn through), lane (where each
  landed, stacked when close), arc (in order, hours apart), pairs (teaser to episode; a crossing is a miss), share (every
  like as one bar). Each plays once in view on the read's motion.
- **Wall** (`readerEngine.js`): Runs and Repeats (Ranks gone). A run's breaker leads with its first finding; its read has
  the skyline, best and lowest, what it found (each finding with its proof and the laws it moved), what it did to the
  laws, the call settled, what to watch. Repeats are moves with a read and the laws they answer to. A post mortem lists
  the laws it kept or broke and the findings it's part of. Results read as Top % everywhere, as in Feed.
- **Records:** Anuj 4/4, 4/4, 5/5, 9/9, 7/7, plus "Clips cut from his streams sink" dropped in run 2. Lakmē 4/4, 3/4
  (strained in run 3 by the co-posted episode), 3/3, 3/3, 7/7, plus "Her face lifts a product card" dropped in run 3.
  Records this clean are a cold start fitted to 30 posts; the next runs are where the laws get tested.
- **Fixed on the way:** Lakmē's product cards land in its top fifth of carousels (Top 8–19%), so v4's "the face alone
  doesn't carry" was wrong by rank; it is now the dropped law, and "the sketch is what travels" lives in the share finding.

Revert to v4: copy `apps/web/src/data/*.v4.json` over `readerPreview.json`, `feederFileAnuj.json`, `feederFileLakme.json`.

Next: a reader prompt (v5) that writes this shape (laws with conditions and sides, findings with a kind and proof spec,
moves for every post) so code can build the proofs; a third account of another type.
