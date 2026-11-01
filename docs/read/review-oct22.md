# Read review (Oct 22 2026)

A review of the Read tab as it stands (v6–v9 data for Anuj and Lakmē, 30 posts each), against what it has to become:
one read that works for any feeder (Apple, Nike, Netflix, Kendall Jenner, MrBeast, Speed, Tiffany, the Lakers), every
10 posts, up to 100 posts, for a beginner and a veteran, whether you are the account or watching it.

## The short version

1. **The insight is there. The structure isn't.** The rules at their best ("a stance lands when the verdict is fast and
   names names", "a teaser's pull calls its episode") pass the stranger test and the veteran test. They are buried under
   the same five stories told again in every component.
2. **It repeats because six components each tell the whole story.** The page cuts the same evidence six ways: by
   thing, by format, by rule, by run, by rank and by post. Each cut is written as a full explanation, so each one
   re-explains. No rule says which component owns the "why".
3. **There is no front door.** Nothing tells you where to start, what matters most, or what changed since you last
   looked. The newest run, the reason to come back every 10 posts, sits at the bottom of the page.
4. **Nothing generates it yet.** Every word on the page is hand-written in two Python files. The only reader prompts
   (v4 run, hero) write an older, different shape. Today, "works for every feeder" means someone rewrites a content
   file per feeder.
5. **It assumes a mid-sized creator.** Follower-based rules, the "usual" by format alone, no outside context (a game
   result, a title's fandom, a YouTube video), co-posts with stars, 10 posts as a run: each of these breaks for at least
   one of the accounts above.

The fix keeps every component the user has approved (the map, the rules as laws, the wall, the post mortem, the
watcher's voice) and changes three things: who owns each claim, the order you meet them in, and a generator that
writes this shape for any account.

## What's working (keep)

- **Rules as laws with a twin.** Condition → outcome, two sides, the same idea told twice with different results. The
  65× chant against the 11-second argument is the best thing on the page: a beginner gets it, a veteran wouldn't have
  seen it.
- **Follows against likes.** "The Yash bit got 154K likes and he lost followers; the chant grew him" and "the episodes
  added followers like any other day" are what the numbers hide. A top-posts tool can't say them.
- **The viewer's seat.** "You wince before anyone talks", "it reads on mute", "you can send it as your own opinion":
  the why is told from the person scrolling, with timestamps.
- **Honesty.** Limits ("sends don't show in likes"), open questions, a dropped rule told as "my first guess was wrong",
  calls settled run by run.
- **Deal tags and the borrowed-crowd caution** on collabs.
- **The voice** is close: plain, specific, no dashboard nouns.

## Why it repeats (measured on the v6 data)

- About **6,000 words per feeder for 30 posts** (Anuj 6,191, Lakmē 5,867), across ~235 separate texts.
- **The same story, everywhere.** Each headline story is told or retold in 10 to 30 places across 8 to 10 components.
  The chant (P27) is linked or listed in **21 places**: rules 01 and 02 (each in its why and its posts), findings F7
  and F8, run 3's "bit hardest", the format "Playing both halves", the nodes "The government" and "The CJP protest",
  the top Ranks band, the post mortems of P27, P13 and P28, the map's post page and an open question. It's told again
  in words in the account line and in run 3's take, note and rule changes. Lakmē's P26 is in 20 places, P07 in 19.
- **Findings are rule events written twice.** All 20 findings (10 per feeder) exist to move a rule (born, sharpened,
  narrowed, dropped). Each is written as a head, a why and a proof chart in the run's sheet, and the same event is
  written again as the rule's life line, the run's "What changed in my read" row, and for two of Anuj's rules (01 and 05)
  the rule's proof is the same chart.
- **Formats and nodes re-argue the rules.** "Playing both halves" ends "it lands when the flip is fast and the target
  is real" (rule 01). Five of Anuj's 18 nodes are one-post brand deals whose text retells that post's mortem.
- **The post text is printed twice.** The Feeder File's post page and the post mortem show the identical "what
  happens" for all 30 posts (~1,100 words per feeder).
- **Ranks bands** describe their posts in prose that repeats the rules ("every football bit, almost every brand
  post…").

Trimming words won't fix this (v4 tried pointers and emptied the components). Each component needs its own job, and
the "why" needs one owner.

## Why it feels unstructured

- **Organised by data, not by question.** The cuts are thing / format / rule / run / rank / post. A reader asks
  different questions: what is this account, what's working, what changed, what do I do.
- **No hierarchy.** "Lecturing a friend" (2 posts, can't call it yet) sits in the same row style as "Playing both halves"
  (his best post in 90 days).
- **The "now" is at the bottom.** A returning reader has to reach the wall's newest breaker to learn what the last 10
  posts changed.
- **Too many nouns to learn.** Rules, proofs, records, findings, moves, wedges, orbits, nodes, runs, calls, bands, splits,
  trails, deals, open questions. A beginner has to learn the app before learning the account.
- **Records claim more than they know.** A rule's record counts the posts it was born from. Lakmē's rules 03 and 04
  say "held 3 of 3" although no post since they were written has tested them; Anuj's rule 04 "9 of 9" includes the
  three posts that created it.

## Why it won't work for every feeder yet

**Hand-made.** The words live in `read_v6_app_content_{anuj,lakme}.py`, along with hand-picked `DEALS`, `DROP_NODES`,
`NODE_RULE`, `CO_SAID` and fixed facts. `read_run_prompt_v4.md` writes lanes and kinded reads (no laws with sides, no
findings, no moves, no node reads, no watcher voice). `read_hero_prompt.md` bans every performance word ("lands",
"works", "best") that the v6 node text is built on. The Feeder File exists for two handles (`FILES` in
`FeederFile.tsx`).

**Built for one kind of account.**
- The "usual" is per format. Lakers' final-score graphics compete with highlights; Netflix's trailers for a huge title
  compete with a small title's memes.
- No outside context: a game result, a title's fandom, a celebrity's news, a YouTube video taking off. For most big
  accounts that is the biggest driver, and the read would mistake it for craft.
- Follower rules need a quiet follower line. At Nike's or Kendall's size, a day's follows don't move with one post.
- Co-posts with stars (Netflix with its casts, Tiffany with ambassadors, Nike with athletes) carry the partner's crowd
  and would top every split.
- The page types in `fingerprint.md` stop at creator, beauty brand, restaurant, bank, travel, clinic and events.
- Wedge names (cast, formats, products, signatures, references) fit a creator and a beauty brand; Apple's cast are
  photographers credited in the caption, which the hero rules call "passing".

**Doesn't scale to 100 posts.** Post mortems, nodes, runs and findings all grow with posts: at 100 posts it would be
~20,000 words, 10 run diaries and 40+ nodes.

**A fixed 10-post run reads at the wrong speed.** For the Lakers in season, 10 posts is one game day. For Kendall, it's
about a month.

## Stress test

The test for each: the read must name the obvious driver once (what a top-posts tool would tell you) and then find what
the numbers hide inside it.

| Feeder | What it posts | The obvious driver | What the read has to find inside it | What breaks today |
|---|---|---|---|---|
| Apple | Photographers' shots on iPhone, launch films | Launch weeks | Which kinds of shots and photographers travel within the same series | Photographers are caption credits ("passing"); no tech-brand type; 10 posts can be a month |
| Nike | Athletes, campaigns, drops, sport moments | The athlete and the result | For one athlete, the moment against the ad | Co-posts carry the athlete's crowd; results aren't in the pack; follower line has no signal |
| Netflix | Many titles a day: trailers, cast moments, memes | The title's fandom | Within a title, which kind of post wins | The usual mixes titles; no title as context; 10 posts is 2–3 days |
| Kendall Jenner | Photo dumps, campaigns for brands she fronts | Her life against her ads | Which deals she makes her own | Carousel-first; a run is a month; news about her isn't in the pack |
| MrBeast | Cuts of YouTube videos, Feastables, his show | How the video did on YouTube | Which cuts travel on Instagram alone | The cause is on another platform; must be a stated limit |
| Speed | Stream and tour clips, football, collabs | What blew up on stream | Which clips land with the Instagram crowd | Same as MrBeast; high volume |
| Tiffany & Co. | Ambassadors, campaigns, jewellery lines, events | The ambassador's fandom | Which product posts hold without a face | Borrowed crowds top every split; "a star helps" is what a veteran knows |
| Lakers | Game days: lineup, highlights, final score, features, partners | Wins and the star | Within wins (and within losses), what travels; partner posts as deals | Result is outside context; no sports type; season and offseason in one window |
| A SaaS app | Memes, trend-jacks, tips, launches | Trend-jacks beat tips | Which product moments survive as entertainment | Memes, tips and launches share one usual |

The skeleton survives all nine: a rule is a rule, a twin is a twin, a run is a run. What's missing is the adapters
below and a generator.

## The proposal

### 1. One front door: "The now"

The top of the reading (and the phone's dock line, expanded) becomes one block that fits a phone screen:

- **Who it's being** (one line, as now).
- **The gist**, three sentences: the obvious driver, what's working beyond it, what isn't. Each phrase links its rule.
- **Since the last run**, up to three changes, each one line linking where it's explained: a rule born, sharpened or
  broken; something new started or gone quiet; the call settled.
- **What I'm watching**, one line.

Anuj after run 3, written this way:

> **Mumbai comic. Booked for football. Followed for politics.**
> Anuj makes Hinglish sketches about Mumbai life, dating and the news, two a day. What grows him is taking a side on the
> week's news, fast and with names ([rule 01](L1), [rule 02](L2)). What he's booked for, football and scripted brand
> spots, is what his crowd scrolls past ([rule 03](L3), [rule 04](L4)).
>
> **Since run 2**
> - The drone joke, redone as a 12-second chant, did 65 times the likes and grew him ([rule 01](L1)).
> - My call held: a brand post holds his usual only inside his own formats ([rule 04](L4)).
> - New: Star Sports booked him. Nothing from it reached his usual.
>
> **Watching:** whether the people who came for the chant stay for his next politics post.

The Read index (all feeds) shows each feeder's "since the last run" line, which makes it a digest across feeders.

### 2. One owner per claim

A claim's headline can appear anywhere as a link. Its explanation lives in one place.

| Claim | Owner | Everywhere else |
|---|---|---|
| Why something lands or sinks | **The rule** | A rule chip ("Rule 01 · a fast verdict, with names") plus at most one clause about this post or thing |
| What a thing is and what the account goes for with it | **The node** | Its name, linked |
| What happens in a post | **The post mortem** (the only post page, map taps included) | A label of 6–10 words |
| What changed and when | **The run** | One line in the rule's life, linking the run |
| Numbers | **Code**, printed once per object | — |

What each component becomes:

- **Rule page:** the split → why from the viewer's seat, with the proof chart (its only copy) → the posts on each side,
  one line each → its life, one line per run, linking the run → the take → limits. The record counts only posts since
  the rule was written ("held 2 of 2 since run 2"); until a post tests it, it says "new".
- **Node page:** what it is, for a stranger → what the account is going for with it → how it's gone (code: ×5, how many
  landed, getting sharper or wearing out, best and lowest) → its posts → "Why: rule 01". No why paragraph. A thing in
  one post opens that post's mortem instead of a page of its own; one-post deals become rows under one "Brand deals"
  node.
- **Run sheet:** the take → what it served (code) → post by post → what changed in my read (one line per rule event,
  linking the rule) → facts this run found that no rule owns, with their proof (Lakmē's "the officer took 90% of the
  likes") → the call settled → watching. Findings stop being a separate card when they moved a rule.
- **Ranks:** the bands keep their posts and lose the prose. One line each, from code: what the band is made of ("7 of
  the 8 at the top are rule 01 or 02 posts; no football").
- **Post mortem:** what happens (the beats, with timestamps) → where it landed and what it beat → the deal strip → each
  rule it kept or broke, with one clause about this post ("the flip at 0:05, five names on screen").

The chant, before and after: 21 places today; after, four. The now block (one line), rule 01's page (the explanation and
the chart), run 3 (one event line), and P27's post mortem (what happens, one clause per rule).

Estimated: about 3,800 words per feeder at 30 posts (from about 6,000), with every explanation said once. At 100 posts the
first screen and the rules stay the same size; only the on-demand pages grow.

### 3. Fewer nouns

The reader learns four words: **rules**, **things**, **runs**, **posts**. Results are said one way: "Top X%" and "its
usual". "Finding", "proof", "move", "orbit", "band" and "planet" leave the interface. The map keeps its look; its
parts are just things and posts.

### 4. Takes by relationship

Following a feeder asks one question: *this is me / a rival / someone I might work with / someone I learn from*. Each
rule's take is written for each relationship and the page shows yours. Anuj's rule 04:

- **Him:** pitch brands your formats (the price game, the stream, the take), not their script.
- **A rival:** his sponsor slots are weak; football money is buying his name, not his crowd.
- **A brand:** buy a slot in his price game; don't send a script.
- **Learning:** the ad that worked was the one that played like his own post.

That's how one read serves an owner and a competitor, and what "implement" looks like.

### 5. Adapters for every kind of account

1. **Page types and jobs.** Add sports team, athlete, celebrity, musician, media/streaming, tech brand, luxury brand,
   SaaS/app and news/meme to the type table. Code gives each post a job from its fingerprint (own, sell, collab,
   announce, moment, feature, game day). The usual becomes same format and same job once there are 8 or more such
   posts; until then, same format.
2. **The obvious driver.** Code finds the single split that explains the most (a person, a title, a series, a result, a
   co-post) and the gist names it once. Rules must look inside it. A rule that only restates it ("posts with Zendaya do
   better") is sent back. This is the beginner/veteran split: the beginner gets the obvious, the veteran gets what's
   inside it.
3. **Outside context.** The pack carries what the fingerprint can read (a score on screen, a title name, an event) and
   the read may name it. When the cause sits elsewhere (YouTube, a stream, the news), the read says so as a limit
   instead of guessing.
4. **Borrowed crowds.** Co-posts stay flagged. A craft rule has to hold on the account's own posts, or say it doesn't.
5. **Follower signal test.** A follower rule is allowed only when a day's gain clears the account's own daily noise by a
   clear margin. Otherwise the read says once that, at this size, follows don't move with single posts.
6. **Labels from the type.** Wedge names come from a short library (cast → players / athletes / ambassadors /
   photographers; formats → series / game-day posts; products → drops / titles / features / deals), and so do the
   pronouns (he / she / they / it).
7. **Run length.** A run closes at 10 posts past day 7, or after 14 days with at least 5, whichever comes first. Heavy
   posters keep the weekly cap (one run over all the week's posts). Between runs, the post mortems at day 7 carry the
   story.
8. **Scale.** The latest run is open in full; older runs fold to their take. Nodes cap at about 20; the rest are pills in
   their wedge. Rules cap at 6.

### 6. Insights no person would find: code scans, the reader explains

A person watching the feed finds the twin. Code can test every post against every other on things a person can't hold
in their head across 100 posts. Each run, code runs these scans and hands the reader the ones that pass the guards as
candidates. The reader explains each, or dismisses it in one recorded line ("the episodes are long; it's the series,
not the length").

| Scan | From | What it can show |
|---|---|---|
| Time to premise | Fingerprint beats with timestamps | "The ones that landed say it by 0:03; the ones under his usual take 0:09" |
| Reads on mute | On-screen text in the first beat | Whether the premise survives with the sound off |
| Follow yield | Daily followers against likes | Which posts turn likes into follows |
| Repeat curve | Each go of a format or series, in order | Getting sharper, wearing out, or a one-off |
| The next post | The post after a big one | Whether the new crowd stays (P28 after the chant; P29 two hours later) |
| Spacing | Posts within 3 hours of another | Whether back-to-back posts eat each other |
| Who's in it | People, within the same format | With and without a face, a friend, a star |
| Length and slides | Code-measured length, slide count | Within one format and job, never across |
| Borrowed reach | Co-post flags | How much of the top is someone else's crowd |
| Deal yield | Paid and deal posts against the usual | What sponsors actually get |
| Concentration | Share of likes in the top few posts or one series | How fragile the account's reach is |
| The trail | What each post beat before something stopped it | The streak a post ended, and why |
| Language and sound | Lines by speaker, track or original audio | Hinglish against English; own sound against a trend |

The guards matter. Run on the 30 fingerprints with no grouping, the raw splits mislead. Lakmē's reels over 25 seconds
sit at a median Top 10% against Top 26% for shorter ones, but only because the episodes are long. Anuj's "words in the
first beat" split swings on 4 posts. So:

- Scans run within the obvious driver's groups (by job, series, title, result), never across them.
- At least 3 posts on each side, and a clear gap (twice the likes, or 25 points of Top %).
- A candidate becomes a rule only with a twin, two posts alike except for the thing tested.
- Its record counts only posts after it was written.

### 7. A generator for this shape

1. **Freeze the contract.** One schema for what the page reads: the now (who, gist, since, watching); rules (law,
   sides, stat, why, takes by relationship, limits, life); things (name, wedge, what, going for, rule); runs (take,
   events, run-only facts, call, watching); posts (label, beats, one clause per rule); open questions. Delete the
   fields nothing reads (`formula`, `form`, `stories`, `drivers`, `repeats`, `primer`, `dispatch`, `runRecaps`) and the
   engine branches for v3–v5 data.
2. **Prompt v5** writes exactly that from: the last run's state, the 10 new posts in full, the older ones lean, the
   page type and jobs, and code's candidates (scans, twins, follower spikes, the obvious driver).
3. **Code checks before publishing.** Numbers, links and quoted Top % (as `read_v6_to_page.py` does now), plus: any
   sentence that shares most of its words with text another component owns is sent back; records out of sample;
   banned words; a rule that restates the obvious driver is sent back.
4. **Replay.** Regenerate Anuj and Lakmē and put them beside the hand-written v6. Then one feeder of a different type
   (a team, a streaming brand or a mega brand) before any more interface work.

## Plan, in order

1. **Ownership pass on the two reads we have.** No new interface. Findings fold into rule lives and run events; the
   map's post page becomes the post mortem; nodes and formats lose their why paragraphs (rule chip instead); one-post
   deals fold under one node; Ranks bands get code lines; records count out of sample. Done when no explanation appears
   in more than one place and the words drop by a third or more.
2. **The now block** at the top of the reading and in the phone dock; the index rows show "since the last run".
3. **Fewer nouns** across the interface and one way to say results.
4. **The contract and prompt v5**, with code checks; replay on Anuj and Lakmē against the hand-written read.
5. **Adapters**: page types, jobs, the obvious driver, borrowed crowds, follower signal test, labels, run length.
6. **Scans** in code, handed to the run as candidates with the guards above.
7. **A third and fourth feeder of other types**, then the 100-post check (folded runs, capped nodes).
8. **Takes by relationship** (needs the follow question in the app).

Steps 1–3 are data and copy on the current engines. Steps 4–7 are the pipeline. Step 8 touches the follow flow.

## Not changing

The map and its motion, the wall with its trail, the post mortem's beats, rules as laws with two sides, the watcher's
voice, deal tags, and the phone sheet. This review is about what the words do and where they live, not the visuals.
