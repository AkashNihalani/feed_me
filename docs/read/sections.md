# Read in five sections (v12, Oct 27 2026)

The user's structure for the whole Read tab, to lock what we know about one feeder before cross-feed. Counted facts move
to Lead (a separate build). Read keeps what comes from the map: the Feeder File, its stories, the runs and the post
mortems.

## The Feeder File: Profile + four sections

The map keeps its shape: the account in the middle, wedges round it, posts round the outside. The wedges are now fixed
sections, the same for every account, in this order:

| cat | section | what goes in it |
| --- | --- | --- |
| -1 | **Profile** (the middle, the sun) | What the account is, what it's doing now, what it sells, what works and what doesn't across everything. |
| 0 | **Patterns** | Two shelves. **Kinds**: every post sits in exactly one kind (so they add up to all posts and "1 in N" holds), named the way a follower would say it; "One-offs" for posts that aren't a kind yet. **Repeats**: what comes back across kinds: a look or costume, a series title, a catchphrase, a recurring sound, a recurring subject (the news, football, a trend). |
| 1 | **People** | Who shows up: named people, friends and co-creators, celebrities, and characters the account plays. A person needs 2+ posts, or to be what a hit turns on. Groups are fine ("Friends and co-creators"). |
| 2 | **Promoted** | What's sold. For a creator: brand deals, collabs, paid posts, reviews, grouped by brand (a brand needs 2+ posts, or a hit; the rest roll into one node). For a brand: its products rolled up into lines or ranges (Lakmē 9to5, Hya Beach Edit), services, launches. |
| 3 | **Places** | Places shown or talked about, and settings seen often: an office, a street, a hotel room, a stream set, a runway. Only settings in 3+ posts become nodes. When nothing comes back, the section is left out entirely (no empty wedge). |

Rules for every node:
- Names are canonical: one spelling per person, brand, product line and place (Lakmē, never Lakmé and Lakmē both).
- Counts come from code: posts, how many beat the usual, typically × usual, best and worst.
- Copy follows `apps/worker/scripts/read_explain_prompt_v1.md`: plain words, every cited post described and linked as
  `[a description](Pnn)`, numbers said one way (× usual, beat N of the last N, 1 in N), never Top % or percentiles,
  never anything implying views, reach, watching, sends, shares or saves.

## Stories: the way into the Feeder File

One story per section, each one opening the map at its section:
Profile → Patterns → People → Promoted → Places (only if present) → What to do (per reader) → the Feeder File.
Each story: a claim (≤16 words), one number (`v` short, `u` ≤8 words), a why (≤90 words) with 2–3 proof posts. Patterns
also carries the kinds as rows (n posts · k beat).

## Runs: Recap · Reward · Review · Rules

- **Recap**: minimal context, ≤60 words. How this run stood against the runs before it, what has been happening on the
  account lately, and what this run makes of it.
- **Reward**: what the run earned as a whole, by code, against the runs before and the account's usual: followers gained
  per day over the run's days, the typical post's likes and comments against the usual, how many beat the usual. One
  line (≤25 words) says what it adds up to. Compare per day (10 posts can take 4 days or 3 weeks); never compare posts
  still collecting likes with mature ones without saying so.
- **Review**: what this run did, where it landed, what did better and worse and why (today's story, landing, the two posts
  that teach, the lesson).
- **Rules**: what this run changed in the rules (born, sharpened, narrowed, dropped) and how last run's call held.

## Post mortem: Review · Repeat · Replay

- **Review**: where it landed (one yardstick), what it was going for, how it landed and why, what it beat and what beat
  it (the trail and its explanation).
- **Repeat**: where it sits in the Feeder File: its kind (with that kind's count), its twin (the closest post) and the one
  difference, the repeats, people, promoted and places it carries (each opens its node on the map), the rules it tested,
  and the next post of its kind.
- **Replay**: the beats, second by second (what exists today).

## Data contract

### `apps/web/src/data/feederFile{Anuj,Lakme}.json` (all existing keys stay)

- `version`: `"sections"`.
- `sections`: `[{cat, k, name, q, line, n, hits: [beat, of]}]` for every section present, in cat order. `k` is
  `patterns|people|promoted|places`, `q` is the section's question in a few words ("What keeps coming back", "Who
  shows up", "What it sells", "Where it happens"), `line` is one cold sentence (≤14 words) saying what the section
  shows for this account, `n` is how many posts touch it.
- `planets[]`: `{name, cat, shelf, orbit, role, icon, ps, typx, hits, what, doss: {going, landing, take, each}}`.
  `cat` is 0–3 as above. `shelf` is `"kind"` or `"repeat"` for Patterns and `null` elsewhere. Every post index sits in
  exactly one `shelf: "kind"` planet. `ps` holds post indexes (0-based, as today).
- `sun`: as today (`name`, `line`, `ps`, `cover`, `doss: {going, landing, take}`) and now holds the Profile.

### `apps/web/src/data/readerPreview.json`, `feeders[i]`

- `reader.stories`: `[{kind: "profile" | "section" | "take" | "map", cat, label, title, line, v, u, why, proofs: [{id,
  label, line}], cover, rows, so}]`. `cat` is -1 for Profile and 0–3 for a section; `rows` (Patterns only) is
  `[{name, n, landed}]`; `so` (take only) is `{own, rival, partner, learn}`.
- `reader.runs[k].recap`: string (≤60 words, links allowed).
- `reader.runs[k].reward`: `{cells: [{v, u, hot}], line}`; `cells` are 2–4 ready-to-print numbers (`v` like
  `"+1,796"`, `u` like `"followers in 5 days, 9× a usual 5 days"`), `hot` true when it beats the account's usual.
- `posts[i].repeat`: `{kind: {name, n, beat}, twin: {id, line} | null, also: [{name, cat}], next: {id, line} | null,
  line}`; `also` names planets exactly as `planets[].name`, so a chip can open that node; `line` (≤30 words) ties the
  post back to its kind or a repeat.

## Seeds (hand-sorted Oct 26 in an earlier session; check against the fingerprints, change with a reason)

Anuj kinds (30 posts): Plays both sides P06 P09 P13 P19 P27 · Talks to the phone P11 P14 P16 P24 P28 · Stream clips P03
P05 P15 P20 P25 · Photo posts P04 P18 P23 P26 P30 · Plays a type P07 P08 P10 P29 · Mates' bits P01 P12 P17 · Rapper flex
P21 P22 · One-offs P02. Across kinds: the news P08 P13 P27 P28; deals P02 P05 P10 P12 P19 P21 P23 P25 P30; football P05
P09 P23 P29 P30; the patterned robe P05 P19 P20 P21 P22; Don YG P20 P21; Yash P01 P02.

Lakmē kinds (30 posts): Product posts P09 P16 P23 P27 P19 P22 P28 · Officer's calls P01 P03 P04 P11 P18 P24 · List
carousels P06 P10 P12 P13 P14 P17 · Skinterventions P07 P15 P21 P26 · Fashion Week P05 P25 P29 · Kareena P02 P08 ·
One-offs P20 P30. Across kinds: Aneet P01 P03 P04 P07 P09 P11 P15 P16 P18 P21 P23 P24 P26 P27; Hya Beach Edit P12 P13 P17
P19 P22 P28; paid P24 P25 P26.
