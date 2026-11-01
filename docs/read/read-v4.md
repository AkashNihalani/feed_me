# Read: refinement plan (Oct 21 2026)

Keep the page you have: the Feeder File, the rules with their thumbnail strips, the wall (Runs, Repeats, Ranks) and the post mortems. Trim where it repeats itself. Change how it explains the account, and tune the UI where it slows people down. Worked on Anuj (Sep 27 – Oct 11) and Lakmē (Sep 21 – Oct 1), 30 posts each.

## 1. Readings

### What's wrong, slot by slot

| Slot | Today | Refined |
|---|---|---|
| Feeder File node: Kareena | “Film actress and Lakmē brand ambassador, tagged on her posts.” | **2 posts: 1,072 and 639 likes. Three times a usual post, a thirtieth of an Aneet episode.** So: heritage, not reach. The attention moved to Aneet. |
| Feeder File node: Yash | “A friend of Anuj's who gets spotted walking down the street.” | **1 post, Top 3%: 154,313 likes.** The star the caption names. So: a reaction anyone can share travels, and brings no followers (rule 02). |
| Rule | “Robe flex lands with a woman beside him” | Dropped: it was one Don YG song across three posts in a day. New rule: **The premiere beat the paid repeat** (her unreleased verse Top 21%, the paid flex Top 50%). |
| Rule | “A pointed stance on live news lands highest”, with the commissioner post as the low case (counted at day 1) | **Same joke, a week later: 65× the likes.** 13,248 as an argument (punch at 0:18), 867,907 as a chant with real names (punch at 0:05). |
| Missing entirely | | **Only the post with his opinion grew him** (+2,349 followers in three days; −234 the week of the 154k-like Yash bit). **Booked for football, not followed for it** (0 of 5 at his usual). **Sponsors get 39% of his usual.** Lakmē: **14 posts took 90% of the likes**, **the teasers called the episodes**, **sketch days added no extra followers**. |
| Run line | “A mixed first run.” “Four rules are born, none dropped.” | **A prank everyone liked; followers slid.** Born: football lands under his usual, and sponsors get less than half of it. |
| Repeats line | “…It lands when a performer shares the frame.” | Ideas carry a verdict: “Four posts, and his only follower growth. The three where he says it out loud all land in his top quarter.” |
| Ranks “Why here” | “Each is a short scene with one clear target and a turn in seconds.” | Removed. Bands keep their counts and likes; the why lives in the rules and post mortems. |
| Post mortem | “A double standard in 12 seconds… the same story as the silent robe (56th of 97 reels) had no stance and sank.” | **What it is** with the week's context (posted during the CJP arrests; two of the five names were detained that week). **Why it landed here** with numbers and the rule it belongs to. **The moments that decided it**, marked on the beats (“the punch, at 0:05 of 12”). |

### The bar for every line

- **Every surface carries a takeaway.** Numbers plus what they mean, never a description of what the thing is.
- **Rules become reads with a kind**, five at most: twin (the same idea twice), crowd (what the likes hide), lane (why a whole lane feeds or fades), money (what sponsors get), arc (a campaign or collab step by step), craft (a move inside the posts); later shift (what the account is turning into, once it has about 50 posts). Each has a receipt (65×, 90%, 0/5), the reason from the viewer's seat (what they see in the first seconds, what they recognise, what makes them send or follow), a take anyone can act on, and what we can't see.
- **One claim, one home.** The Feeder File says what each thing does for the account and how it did. The rules say why. The wall proves it. Anywhere else, point to it (“rule 03”).
- **Post mortems keep context:** what happened and what was going on that week, why it landed there, the moments that decided it, what it changed.

### Prepared

Both accounts are rewritten in the page's own data format: 5 reads plus one dropped rule each, 7 ideas, 3 runs with their calls settled, every post mortem, and every Feeder File node with a verdict and a “So”. They're in `apps/worker/scripts/out/read_v4_page/`, built by `read_v4_to_page.py`. With `--apply` it writes them into `apps/web/src/data` and keeps the current files as `*.v3.json`.

## 2. UI refinements (same page)

### Feeder File

1. **Numbers on the nodes.** Each planet's label carries its post count and typical Top % (“Football · 5 · Top 79%”), coloured as now. That's the “oh, I get it” Lead and Feed already have.
2. **Fewer, truer nodes.** A planet is something in two or more posts, or a top-quarter post's main ingredient. The reader merges look-alikes (“Creators beside him” instead of four one-post planets). Everything else stays a small body, named only when its wedge is browsed. Around 15 planets at 100 posts.
3. **No endless panel on desktop.** The top block stays (the account line, four numbers, Feeds on / Fades on). Each wedge becomes one row (name, count, its strongest thing's verdict) that opens in place. A node's page still takes the panel.
4. **The takeaway first on a node's page.** Verdict, then “So”, then how it did (the bullet chart), its posts, what it does, seen with.
5. **Two stops on the phone, not three.** Peek (the account line and chips) and full (the reading), one flick apart. Tapping a node opens its page at full with the map shrunk to a strip above it, so the spread and the reading stay on screen together.
6. **At 100 posts**, the ring shows the latest three runs as covers and the older 70 as ticks on an outer ring that open on tap. Threads come only from covers, so the system stays readable.
7. **Rules and the map talk both ways.** A node's rule chip already opens its rule; opening a rule should also light its posts and nodes on the map.

### Rules

8. **Built, waiting on the new readings:** each row leads with the read's receipt, the columns take the read's own names (“The chant” / “The argument” instead of “Kept it” / “Broke it”), a “what we can't see” line sits under “Do this”, and the formula row (which restated the rules) goes.
9. **A proof visual per kind** inside the opened row, where the kept/broke ledges don't fit: a twin gets both posts' beats on one timeline (the punch at 0:05 against 0:18); crowd gets the follower line with the posts pinned; money gets each sponsored post as a share of the usual; a lane gets where its posts landed.

### Wall

10. **Runs:** keep the breakers and the run view. The breaker line says what the run taught; the run view speaks in the new voice: what it posted, why it went this way, what it did to the reads, the call settled.
11. **Repeats:** keep the dividers and their numbers; the line is a verdict, not a description.
12. **Ranks:** bands and likes ranges only.
13. **Post mortem:** keep it whole; the beats carry the reader's notes on the moments that decided the post (built).

### Everywhere

14. **One way to say a result:** Top % (as in Feed), plus “beat N of the other M” in the post mortem. No mix of “12th of 40”, “Top 30%” and “places” on one screen.
15. **The Read's own motion** for anything new (TEMPO, the carry curve, staggered rises). No new motion language.

## 3. Any account

The grammar doesn't change by account type; the reader names the lanes, nodes and wedges from what the account posts. The examples are illustrations, not data.

| Account | What the Feeder File holds | Reads you'd expect |
|---|---|---|
| Creator (Anuj) | characters, collabs, deals, signatures, the news he rides | twin: same joke, two cuts. crowd: likes vs follows. money: what sponsors get. |
| Beauty brand (Lakmē) | the face, episodes, products, events, templates | twin: two episodes, two hacks. arc: teaser → episode → card. crowd: share of likes. |
| Sports team | players, matchdays, transfers, kit, sponsors, rivals | twin: the same goal from the stands vs the broadcast. crowd: the derby win brought follows, the kit launch only likes. money: sponsor posts vs matchday posts. |
| Apple-style product brand | products, features, people, launches | twin: a feature shown as a demo vs a person's story. arc: keynote → feature drip. lane: what fades by day 4. |
| Streaming service | shows, cast, scenes, memes, release days | arc: teaser → trailer → release-day clip. craft: the quoted line in the first second. |
| Event company | lineups, venues, ticket drops, aftermovies | lane: timetables and names beat mood (Lakmē's schedule is this). crowd: ticket-drop days bring follows. |

## 4. Pipeline

- **The v4 reader prompt** (`read_run_prompt_v4.md`) adds a viewer's-seat pass for every post and writes five parts instead of v3's nine. Its pack (`read_run_sim_v4.py`) adds the follower line by day, the closest pairs by their words (where same-joke twins surface), arcs within a few days, and measurement notes.
- **Test, Oct 21 (Sonnet 5.5 as an agent):** on Lakmē it found the felt-hack vs acid-list twin on its own, with timestamps, plus two things the hand-written read missed: sketch days added followers no faster than ordinary days, and the product cards stay at 626 to 1,064 whatever their episode drew. It still wrote its lanes as descriptions, so the next prompt has to demand verdicts. The Anuj run stopped before writing anything: it hit the agent's output cap, so the test needs a rerun that writes in parts.
- **v4.1** writes the page's slots directly (read receipts, column names, node verdicts and “So” lines, idea verdicts, run lines, post mortems with beat notes). Code computes every receipt (ratios, follower moves, shares, counts) and checks every number before anything publishes.
- **Fingerprint fixes:** never give an unidentified song or person an earlier post's name (the record linked two different songs); name the public figure a caption names (Yash).
- **Data:** comments per post (stored, not yet in the pack), day-7 counts for every post, paid labels and co-authors from every scrape, daily follower snapshots without gaps.

## 5. Order of work

1. Apply the prepared readings and review them on your dev server (data only; the engine support is in and does nothing until then).
2. Feeder File: numbers on nodes, the panel as wedge rows, the node page order, two stops on the phone.
3. Proof visuals per read kind in the rule rows.
4. Reader prompt v4.1 and the server checks; test on a third account of another type.
5. The data and fingerprint fixes.

## Decisions for you

1. Apply the prepared readings to the Read tab now?
2. Phone Feeder File: two stops (peek and full), or a map strip above the page?
3. At 100 posts: about 15 planets, and older posts as ticks on the ring?
