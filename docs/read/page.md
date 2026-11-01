# Read: one job per layer (plan, Oct 18 2026)

The Read page for one feeder, top to bottom: the Feeder File, the rules, the wall. This plan says what each layer is for, what it may say, and how the Feeder File's four views change. It replaces section 1 of layers.md (the bar-and-follower hero there is gone). Worked on Anuj's placeholder read.

## Decided after this plan (Oct 18, step 1 built as a first draft)

- **Feeding / Fading is the page's one split.** A post is feeding if it landed above its account's usual post (the top half of its window), fading if under it. Anything with 2+ posts is mostly feeding (two thirds or more), mostly fading (a third or less), or on both sides. On both sides is where "what decides it" lives: its best feeding post against its worst fading one, with a chip to the rule that explains it. The hero shows the split; the rules explain it; the wall proves it post by post.
- **The sentence is gone**, and so is the "Why his posts land" header above the rules. Brain's panel is the split itself: a code kicker (handle, posts, dates, a week, typical length, paid), the reader's headline, then Feeding / Fading / On both sides. The rules' formula rows now say "are feeding" / "are fading".
- **Hex is by who's in it** (each post in exactly one group, `crews` in the data), and the hero shows performance but never explains it.
- Built: `feederFileEngine.js` (groups, the split, the hex packed in code, panels, cards, storyline traces, moment marks, rule chips, post mortem hand-off), `feederFile.css`, `feederFileAnuj.json` (crews, lines, heads, rules, an example line per thing), `readerEngine.js` (sentence removed, `ff:rule` opens a rule).

- **Then one map, two modes** (same day). Hex had no value and the views still repeated each other and the wall, so Rings and Brain became one map: rings by how often, a sector per kind, his posts round the rim as their own round covers. **Split** tilts it (what feeds him rises off the floor, what fades sinks); **Map** looks straight down. Drag only turns it. Spiral, Hex, storylines and the old who's-in-it groups left the hero (Runs and Repeats own time and ideas); who's in it is now a kind of thing. One panel: the account in a line, the headline, what feeding and fading mean, On both sides, then each kind. Kinds come from a fixed library with labels per account (brand deals for a creator, creator collabs for a brand, products and offers for a bank, sponsors for an event company).
- **Then a solar system** (Oct 20). The map's kinds and sectors read as random nodes ("the cheating boyfriend" means nothing on its own), so the hero became a system round a core. The reader names the **sun**: who the account is being right now (2–4 words, one line, 2–5 posts that show it; `scripts/read_hero_prompt.md`). Up to 12 **planets** are the things that keep turning up, each an extension of that core, on three orbits by how much they're its own: **own** (persona, the parts it plays, series, products), **regulars** (people, partners, formats it keeps coming back to), **borrowed** (outside names, news, moments, songs). One-offs are a faint belt of dust. Code does the rest (`scripts/read_hero_sim.py` finds the candidates, `read_sim_to_page.py` builds `version: 'system'` data). Built from the map's own parts (`feederSystemEngine.js`): the frosted floor is the account's usual post; a planet is a hex badge sized by its posts, standing above the floor and red when most of them landed, ink-edged when they split, under the floor and grey when most faded; the sun is a large hex holding its best cover, rimmed in red. **System** tilts it; **Chart** looks straight down with the heights folded flat, so every name reads. Tap a body and its posts glide out round it as moons, on a ring that faces you. A first build with photo discs, icon chips, pink glows and boxed sentence-case labels was rejected as cheap; the map's dialect (hex badges, hairline stems, tracked caps, faint halos, grey for what recedes) is the bar. Second pass, same day: the centre is the **account itself** (its profile picture in a crimson ring, over a faint crimson pool and an etched dial), captioned with what the reader says it is being; the persona, ambassador and signature look became own planets (Lakmé's core had absorbed Aneet, so she also seemed to sit in "borrowed"). **Every thing gets one home**: the reader sorts each one-off onto an orbit (or folds it into its range: every 9to5 shade joins Lakmē 9to5) and leaves out only noise, so one-offs are small hexes on their own orbit, not a far belt that read as borrowed. Planets are tied to the core by etched spokes. Opening a body glides the camera on the carry curve, draws a ring round it and brings its posts out one by one; ‹ › and the arrow keys step through the panel's order; closing glides back to the view you had before. Third pass, same day: the user found the panel amateur (insight lines cut off, opening a thing "not worth it", nodes with no real explanation), the target ring cheap, the threads lifeless, Chart pointless (you can tip the camera straight down), and the breakdown unstructured. Now everything sits in **five wedges that each answer one question** (Cast: who's on screen; Formats: what it keeps making; Products & deals: what it sells; Signatures: how you'd know it; References: what it rides), with orbits still meaning how much is its own. The reader writes **what** each planet is and **how** it shows up; code adds how it did (landed share, typical place against the account's usual as a bullet chart, seen-with, new). The panel is an editorial page (the account, a ruled strip of four numbers, Feeds on / Fades on, then each wedge in full words with a landed score), and opening a thing turns the panel to its page beside or under the system. No reticle or hover ring; moons fly out along curved threads with light running down them; rings sweep in with a bright head; light runs out along each spoke as its planet arrives. Chart is gone: drag tips the system down to overhead and the heights fold flat as it goes. Polish pass: every panel grid keeps to its column (a long one-off name had widened the panel 39px past its frame) and long pills wrap; labels and captions use hysteresis (clear for a few frames before showing, crowded a while before hiding) and moon labels keep their side, so nothing blinks while the camera glides; the open body's name sits outside its ring of moons; spokes are gentle swirled arcs, brightest at the core, and spokes and threads carry comets with a soft head and a fading tail (on arrival, looping for the open planet, now and then for the ones that landed); the panel slides in once and crossfades between pages. Checked on iOS Safari (simulator): 60fps, taps glide in, drag turns, closing glides back. Mobile pass: the user found phone navigation endless (map, then a long list, back and forth) and unlabelled dots random. On a narrow card the Feeder File is now one screen: the system on top (its buttons on it, pinch to zoom, zoom-out reaches the whole system), the wedges as chips, and a pager of pages under it (the overview with a jump list of the wedges, then one page per planet, each scrolling on its own, the next peeking in). Swiping a page opens its planet on the system; tapping a planet turns to its page; a post, the account or a passing thing opens over the pager, never the system. Browsing a wedge (its pages, its chip, its section of the desktop panel) lights its small bodies and names them on the system, and each planet's page lists its wedge's once-or-twice things. Sheet and connections pass: the user found the phone version too small (a small box to scroll, then sideways paging) and the dots unconnected ("I need to see what something did similar to something else"), and chose **full map + pull-up sheet**. On a narrow card the map fills the card, nearly edge to edge, sized so its foot clears the folded header and the app's bottom bar; it looks down a little more steeply (polar .74) and fits the whole system, wedge names included. The reading is a sheet pulled up over the map's foot (peek: the account, its landed score and the wedge chips; half; full): drag or flick the handle, tap it to step up; the camera keeps the system centred in the room above it. A chip filters the sheet to its wedge and turns the system until that wedge faces you, framed above the sheet, with only its things named. Opening a thing raises the sheet to half and frames the thing with its **web**: arcs to everything it shares posts with (thicker for 2+, red when those posts landed, light running out from it) and dotted arcs to the planets that **did alike** (typically land within seven places of it, same side of the usual, never already linked); the opened thing and its moons keep a floor on their size on screen. At rest the planets' arcs show faintly, so the system reads as connected before anything is opened. Its page adds "Seen with, in the same posts" and "Did alike". The transparent status bar and the app's glass nav stay as they are on every tab (checked in the Read tab on iOS Safari); only full screen covers them. Woven pass: the user still found it poor (most nodes not visible, the list carrying everything the map didn't, post circles covering the planet they flew out of) and said that without seeing relations flow through the feed the component is pointless. Now **the feed rings the system**: every post stands round the outside as its own 4:5 cover, higher above its ring the further it landed above the usual and hanging under it when it fell (red frame and stem for a top-quarter post, ink when it landed, grey when it fell), each placed beside what it's made of. **A thread runs from every post to every thing in it**, bowing in round the account, so what turns up together visibly shares posts; at rest the threads are grey (red only at full strength, never pale pink). There are no moons: opening a thing lifts its posts on the ring and draws its threads bold with light running in, and fainter the other threads of those posts and the things they reach (what it's seen with); opening a post lifts its card and names everything in it. It arrives the way the feed went up, post by post sending its threads in, then replays the feed one post at a time in posting order. The hex grid, spokes and floor lip are gone; planets are bigger; the phone looks down more steeply (polar .5); the camera fits by projecting each card's real extent and centres it in the room; on a phone the wedge and orbit captions only show for the wedge being browsed, and names try corner spots too. Text pass (Oct 21): the user found many nodes filler ("Ved Madison is only mentioned in dialogue: does he deserve a spot?"), while Don YG adds to the post and a footballer he talks about changes how the audience responds. The hero reader (prompt v4) now asks of every thing **would the post be a different post without it?**, judged from the post's premise and who does what: what the post turns on **carries** it, what says who the account is being **signals** it (only with the stance written: the robe is his big-shot costume, worn knowing it reads as the opposite), and anything named in dialogue, in the background, on a shelf, shown only as a logo or worn without remark is **passing** and gets no node. The pack gives the reader each post's premise and, for each thing, how it is there (seen, heard, text, caption; who plays it; beats; whether the premise names it). Every kept thing has a **does** line (what it does for its posts, with the account's stance), which leads each panel row and heads the thing's page ("What it does", or "What it says" for a signal). What carries a post or turns up in two or more is a planet; one-post signals are small bodies; names go to what carries before what signals.

The sections below are the plan as first written; where they talk about the sentence, the four views or Hex, read the above instead.

## What's wrong today

**1. One question, answered about 40 times.** Every block on the page explains why posts land. The reader has five explanations (the five rules), and the page says each of them again in seven to eleven places:

| The claim | Said in |
|---|---|
| Playing every part himself sinks him; a cast lifts him (rule 03) | rule 03, the sentence hero, Brain "Drags them", Rings "Habits that drag", Brain "Lifts his posts" (The Pitch crew), Repeats (Solo, With a cast, Two-person chats), Ranks bottom band, runs 2, 3 and 4 |
| Loud and wrong works, the good guy sinks (rule 04) | rule 04, the formula, the who line, Repeats (Football, Told off, Cities), Ranks bands 2 and 0, run 4 |
| The payoff has to come early (rule 01) | rule 01, Ranks bands 3, 2 and 1, Repeats (Strangers), runs 2, 3 and 4, the dropped rule |
| The product has to be the plot (rule 05) | rule 05, the sentence hero's brand ladder, Hex "Brand formats: never", Rings hits and duds, Repeats (Brand formats), Ranks band 0, the call |
| His audience's own world: Andheri East vs UP (rule 02) | rule 02, the sentence hero, Repeats (Cities), Ranks band 2, run 4, Spiral and Places in the Feeder File |

The overlap you feel isn't a copy problem. Every surface is allowed to explain why, so every surface does.

**2. The four views are the wall's three tabs drawn again.** Spiral is posting order with the last 10 lit, which is Runs. Hex is one patch per idea, which is Repeats. Brain's height is rank, which is Ranks, and its "lifts / drags" is the rules. Only Rings looks at something the wall doesn't. So their readings come out as the wall's and the rules' readings: Brain opens "Best run yet", Spiral opens "He's climbing", and both rest on the same fact (his last 10 typically land 9th). Brain's "Lifts his posts" and Rings' headline share the same line ("The big shot, 11th, without it 24th"), and both list "Plays every role" as the drag.

**3. The readings are written by code, and code can only count.** Every panel headline and row is a template filled from tag counts. Most tags are too thin for that: 22 of the 34 things appear in one post, 27 in two or fewer. So the panels publish noise as findings:
- "Hits he hasn't repeated" lists Mother's Day, Blinkit, Jimmy Cooks and Melodi: four rows, two posts (the Blinkit ad and the Parle parody).
- "Ruhee Dosani lands best", "Jimmy Cooks lands best": one post each.
- Brands: "Its posts typically land 1st." That's Blinkit's one post, not the brands'.
- "Alike, far apart: Parle parody 2nd vs Piano 39th." A marketing parody and a 14-second piano clip aren't alike; the pair comes from text similarity.
- "Strangers: always above his usual": its worst post is 20th against a usual of 20.5.
- "5 peoples", "3 lingos".

Nobody describes a post, so none of it passes the "a stranger can picture it" bar the rules already meet.

**4. Everything averages 40 posts, so the page describes May.** The big shot is called his engine (Rings), "his most-made idea, and his best" (the sentence hero and the Repeats divider), and the who line is "a man far too sure of himself". The big shot is in none of his last 10 posts, and those 10 are his best run (typically 9th, against about 21st before). In June he stopped working alone: The Pitch crew and his friends are in 4 of his last 10, while other people (strangers, two co-stars) carried 5 of the 30 before; football and five-second puns came in. No block on the page says so: the panels average the whole window, and Spiral's "Gone quiet" only counts things missing from the last 20 posts. The core of Feed Me is "the now", and the top of Read is a month behind.

**5. Two heroes.** The sentence hero (`reader.hero.parts`, rendered by `topHTML` in readerEngine) still sits under the Feeder File, so the page summarises Anuj twice before the rules.

## The fix: one question per layer, one home per claim

| Layer | Its question | Its unit | It says | It never says |
|---|---|---|---|---|
| **Feeder File** (hero) | What is this account, right now? | things: characters, people, places, lines, sounds, looks, brands, series, moments | what each thing is and means, where it shows up, who's in it, what started and stopped, how much of the feed | why a post lands. How well shows as colour and height, never as words. |
| **Rules** | Why do its posts land where they do? | mechanisms | the law, the twins, the kept / broke split, Do this | lists of what it posts |
| **Wall** | What happened, post by post? | posts | Runs: what each run was and what it did to the rules. Repeats: what each idea is and how often. Ranks: what each band holds. The post mortem. | a rule's sentence again (it links to the rule instead) |

**The one test:** a claim lives in one place. Posts and things can show up anywhere; a claim only shows up once, and everywhere else it's a link ("→ 03").

That gives Read a plain order: know the account (Feeder File) → get why it works (rules) → check the proof (wall).

## The Feeder File, rebuilt around that

The map, the four shapes, the motion, idle, the inset card, full screen: all stay. What changes is what each view is about and what its panel says.

### The header: status + the sentence

The sentence hero moves into the Feeder File and stops being a second hero. It's Brain's panel, so it's the first thing you read.

- **Kicker (code):** `@anuj.mp4 · 40 reels · May 2 – Jun 15 · 6 a week · 17 sec typical · 5 paid` (plus the follower change once snapshots are clean for the feeder).
- **The sentence (reader):** one now-weighted sentence, where every phrase is a door. Tapping one switches to its view and lights its things and posts on the map.

> A Mumbai comedian who made his name as **the guy sure he's made it**, joking for **Mumbai's marketing and creator crowd**. In June **he stopped working alone**: **an office series, football with friends, five-second puns**. **Five brands** paid for posts.

| Phrase | Opens | Lights |
|---|---|---|
| the guy sure he's made it | Rings | The big shot and its 8 posts |
| Mumbai's marketing and creator crowd | Rings | "IP", SoBo, Andheri East, Mumbai spots |
| he stopped working alone | Hex | The Pitch crew, his friends |
| an office series, football with friends, five-second puns | Spiral | what started in June |
| Five brands | Hex | the five paid posts |

### Four views, four questions

| View | Its question | The map | The panel |
|---|---|---|---|
| **Brain** (3D) | Who he is | as now: everything wired, height = how it landed | the sentence and the kicker |
| **Rings** | His world: what you'd need to know to get him | as now: the centre holds what's in most posts, sectors are kinds | a codebook: each thing in plain English, then the post it's in |
| **Hex** | Who's in it | covers packed **by who carries the post**, not by idea | the cast: faces, names, roles, what they do in his posts, dates |
| **Spiral** | What's changing | posting order; each series or storyline drawn as an arc from its first post to its last; moments as dated marks | just started / still going / not in his last 10 / moments he rode |

Hex moves off ideas because Repeats already groups the posts by idea, and you ruled out any lens that repeats a Repeats group. Grouped by who's in it, the same covers answer a question nothing else on the page answers, and the face crops already read as people.

### The four panels, drafted for Anuj

Placeholder copy to show the bar. Words by the reader, numbers by code.

**Brain: Who he is.** The kicker and the sentence above, with "Tap a phrase".

**Rings: His world.**
> **His jokes run on agency talk, a Versace bathrobe and a Ronaldo grudge.**
> - **The big shot** · character · 8 posts, May 4 – Jun 6. The man he plays most: a marketer, creator or rich kid far too pleased with himself. Parle's marketing boss pitches an "Italy expansion" off one meme (2nd of 40).
> - **"IP"** · lingo. Creator-speak for the one viral video they think they own. Two creators talk "IP" after a single hit (3rd).
> - **"Rigged"** · lingo · 2 posts. His word for Ronaldo's losses, "rigged for Messi" (5th); rivals throw it back at him in a street rap battle (16th).
> - **SoBo** · lingo. South Bombay, and the kids who never leave it; they panic outside their bubble (11th).
> - **Versace bathrobe** · look · 4 posts, not since Jun 6. The big shot's costume; he hosts a street-mic roast in it (20th).
> - **Sad Romance** · sound · 2 posts. The sad-violin meme track, for when he's the victim: his dad mocking his videos (34th).
> - **Andheri East** · place. The Mumbai suburb his audience commutes through; a five-second pun on it (6th). → 02
>
> Then every kind as a tile (Characters, Lingo, Places, Looks, Sounds), opening to all its things.

**Hex: Who's in it.**
> **Just him, until June. A crew or his friends are in 4 of his last 10.**
> - **The Pitch crew** · Jun 12–14. Four colleagues in his scripted office series: one blows her nose in a sock (4th), one "pitch meeting" turns out to be a Ludo game (7th).
> - **His friends** · Jun 6–14. On the turf (10th), at dinner at O Pedro (8th), at the Baskin Robbins counter (13th).
> - **Strangers** · May 4–25, none since. A roadside crash he "mediates" (14th), a rap battle at a cosmetics stall (16th), a street-mic roast (20th).
> - **Co-stars.** Ruhee Dosani, the other creator in the "IP" sketch (3rd); Ved Maddison, his rap-battle rival (16th).
> - **Brands, paid** · 5 of 40. Blinkit: his mom swaps him for an AI assistant and a Blinkit order wins her back (1st). Broadway: a store launch, "not Anuj's PA" (15th). District: a Jordan-or-Jackson mix-up (27th). Lotto x H&M: a jersey lookbook (38th). Startup Village: a rapid-fire panel (40th). → 05
> - **Just him, or him playing everyone.** The piano, a tattoo close-up, his dad (played by him), girls vs boys in four wigs. → 03

**Spiral: What's changing.**
> **No big-shot sketch since Jun 6. In came an office series, football and five-second puns.**
> - **Just started:** The Pitch, a scripted office series (Jun 12, 2 episodes). Five-second place puns (Jun 13, 2).
> - **Still going:** football (May 31 – Jun 14, 3 posts). Eating out with friends (Jun 6–7, 2).
> - **Not in his last 10:** the big shot (8 posts, last Jun 6). Street videos with strangers (4, last May 25). Getting told off at home (2, last May 29).
> - **Moments he rode:** Mother's Day (the Blinkit ad, May 5), the "Melodi" meme (the Parle parody, May 20), PSG winning without Mbappé (May 31), Ronaldo's losses and Mumbai's ₹248 crore flyover (both Jun 8), the emergency-alert test (May 2).

Each panel says something no other block on the page says, and none of them explains a landing. The colour on the map and on each row's place carries how well it did.

### The cards

- **A thing:** kind, name, how many posts, first and last date; what it means; where it shows up (each post's cover, place and one line on what happens); what it often comes with (only pairs seen 2+ times); a rule chip if a rule cites it. No with / without, no best-vs-worst diff.
- **A post:** place, date, paid; what it is; what it's made of; what's said and on screen (the fingerprint's lines, already in the data); "Open its post mortem", which goes to the wall's sheet, the one home for why that post landed. No look-alike diff.

### Copy rules for the Feeder File

1. Performance is shown, never said: red = top 10, ink = above his usual, faint = under, height in Brain, rims in Hex. No "lifts", "drags", "engine", "lands best", "works", "with it / without it".
2. Meaning first, then the post: "'IP' is creator-speak for…", then where he uses it.
3. Describe every cited post for a stranger, with its place ("4th of 40").
4. A thing needs 3+ posts before it gets a number of its own ("typically 11th"). With 1 or 2, show its posts' own places.
5. Weight the now: a red tick on what's new this run; "not in his last 10" on what stopped.
6. The reader writes every word; code writes every number, date and count.
7. Where a rule explains something, show its chip (→ 03), never its sentence.

## Below the hero

**Rules:** unchanged. They're the one home for why: law, twins, kept / broke, Do this, the formula, still open.

**Wall:** the design stays. Only its words follow the test:
- **Repeats dividers** say what the idea is (its posts described), how often, and its range; the why becomes rule chips. Cities: "Jokes about places: South Bombay kids outside their bubble (11th), a five-second Andheri East pun (6th), the same pun on UP (35th), a flyover rant (30th). → 02 · 04".
- **Ranks "Why here"** becomes computed chips: which rules the band's posts kept or broke, counted from each rule's own lists. Top 10%: "Kept 01 · 4 of 4, kept 04 · 3 of 4".
- **Run lines** say what the run was, and its rule moves as chips (01 stronger, 02 stronger, 03 narrowed), not the rule's sentence again.
- **Post mortems:** unchanged. Applying the rules to one post is their job.

## Any account

The views are questions, and the kinds and labels come from the account (the slot table in layers.md), so the same four work everywhere:

| | Rings: its world | Hex: who's in it | Spiral: what's changing |
|---|---|---|---|
| Creator (Anuj) | characters, lingo, sounds, props, places | crew, friends, strangers, brands | series started and stopped, moments ridden |
| Brand (Lakmé) | products, shades, looks, slogans | Kareena, Aneet, creators on co-posts, models, product-only shots | campaigns and launches: Eyeconic relaunch running, Green Matcha CC+ new, Icy Sorbet stopped |
| Talent agency (gamers) | games, in-jokes, setups, catchphrases | co-streamers, collabs, sponsors | new games, tournaments, sponsor runs |
| Event page | venues, artists, themes | artists, hosts, crowds | upcoming and past events |
| Dermatologist | conditions, ingredients, myths she busts | herself, patients, guests | new topics, seasonal (summer SPF) |

"Where the money is" lands in Hex for people who get paid (deals as partners) and in Spiral for brands (products and campaigns as storylines).

## The chatbot question

Don't put an open chat in Read now.

- **The blank box.** Most people don't know what to ask. Read's views are the questions already answered: who it is, its world, who's in it, what's changing, why it lands, what happened. Asking is tapping: every phrase, thing, rule, run and post opens its answer.
- **Cost.** A chat answer that reads the account's 100 fingerprints costs about ₹32 a question before caching (Sonnet 5.5 at standard price), more than a whole brain run on batch (about ₹21). A few curious questions would cost more than the feeder's month of reads (about ₹160), and chat cost grows per user, not per feeder.
- **Trust.** Every claim in Read passes the check pass. Chat answers don't.

Open chat fits Leader with credits, answering from the claim ledger (claims with their posts) rather than the raw fingerprints, so it's cheap and cites evidence.

## What the brain writes for it

All in the run every 10 posts, about 2k more output tokens (about ₹1 a run on batch).

- `brief`: the sentence, as parts; each phrase carries `{view, things[], posts[]}`.
- `things[]`: `{name, kind, meaning, example: {post, line}, rule?}`. The reader picks the things worth knowing (about 30–40) and writes the meaning and the example line. Code adds posts, counts, dates, first / last, new / not-in-last-10, and a typical place only at 3+ posts.
- `post.cast`: who carries the post (just them, them playing everyone, named people, a group, a brand's cast), read from the fingerprint's cast and who-plays-whom. Code groups the Hex patches; the reader names each patch's role.
- `storylines` (from `post.story` and each run's started / stopped) and `moments` (name, date, post): Spiral's arcs and marks.
- `views.{rings, hex, spiral}.head`: one headline each, 12 words or fewer, checked.
- Claim ids: every claim the brain writes has one home; other surfaces carry its id and render a chip. The check pass also flags why-words in any Feeder File field.
- Layout: no embeddings. Posts sit by the things they share (overlap of their things), which also replaces the text-similarity pairs.

## Build order

1. **Feeder File, on Anuj's placeholder data (frontend only).** Remove the second hero (`topHTML`) and move the sentence and kicker into Brain's panel with phrase doors. Rewrite the four panels to the spec (copy in feederFileAnuj.json, numbers from code). Regroup Hex by who's in it (hand-tag `cast` on Anuj's 40). Draw storyline arcs and moment marks in Spiral. New thing and post cards. Fix the counting bugs ("5 peoples", the kind line, hits counted by tag, the text-similarity pairs).
2. **The wall's words** to the test: Repeats dividers, Ranks chips, run rule-chips. Copy and small render changes, no redesign.
3. **The brain** writes `brief`, `things`, `cast`, `storylines`, `moments` and the view headlines, with claim ids and the why-word check. Then Lakmé as the brand test, once its carousels are fingerprinted (they never have been) and its follower counts are fixed.
4. **Leader** later: the read across a feed, "what it means for you", chat with credits.

## Decisions for you

1. Hex by who's in it (recommended), or keep ideas and accept that it repeats Repeats.
2. The Feeder File shows performance and never explains it (recommended), or it keeps a line of numbers about what lifts and drags.
3. Step 2, the wall's words: do it, or leave the wall's copy as it is.
