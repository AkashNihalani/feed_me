# The Feeder File model (proposal, Oct 26 2026)

> **Update, Oct 26 2026 (later):** The Feeding / Both ways / Fading columns below were a misread. The user meant **Feed** and **Fade** as the user's two inputs to the reader: **Feed** tells it to feed on a thing more (go deeper, lead with it), and **Fade** fades it from the reading. A note to tell it something or ask it something sits alongside them. The component must reach the finish of the best Obsidian graph work, and the columns sample was rejected on finish, as were the metro and the first graph. The structure of the map is still open. The next step is to match a specific reference the user picks, on a mature graph engine, before adding any logic.

## The problem

Every version so far drew the same flat bag of nodes: 12–14 "things" the reader found, sorted into five fuzzy wedges (People, Kinds of post, Products & deals, Trademarks, Talked about). A thing's wedge was a judgement call. "Set-up line at 0:00" is a kind of post, but "the bathrobe" is a trademark. All the nodes sat at one level, and a post could belong to three of them. With no structure in the data, no drawing could show one. Obsidian feels organised because notes live in a hierarchy (folders), carry tags that cut across it, link to each other, have properties, and are written by the user.

## The structure: feeding, fading, both ways (revised Oct 26, after review)

The first version split posts into four jobs (the show, the sell, the life, the news). Review rejected it as generic dashboard vocabulary, not Feed Me. The structure now uses Feed Me's own split.

**The line is the account's typical post now.** That is the middle post of the same format among the posts the Feeder File holds, said once as likes: "his typical reel now gets about 3,200 likes". A post **feeds** the account if it does better than that, and **fades** it if it does worse.

The 90-day usual is not used here, because it breaks on accounts that are changing. Against its 90-day usual, 26 of Lakmē's 30 posts "feed": the bar was set before the officer arrived. Against its typical post now, 14 of 30 feed, and the map splits.

**Every thing goes in one of three places, by its record:**
- **Feeding:** two in three of its posts or more feed.
- **Fading:** one in three or fewer feed.
- **Both ways:** it splits. This is where the rules live: what decides which way it goes.

Things are ordered by how much of the feed they are, so the top of Feeding is the biggest thing feeding the account. Posts hang from their things on the side they went, so a both-ways thing visibly forks. Tags (who, what about, signature, brand) are lenses across all three places, not regions. A thing moves between places as its record changes from run to run. That movement is how the map grows: "Brand deals moved from Fading to Both ways in run 3, when the robe lip-sync held."

### Tested on the sample Feeder Files (code, against each account's typical post now)

| | Feeding | Both ways | Fading |
|---|---|---|---|
| **Anuj** (typical reel 3,170 likes) | Set-up line at 0:00 (8 of 9 feed) · Talking to the phone (5 of 6) · The bathrobe (4 of 5) · News reaction reels (4 of 4) · Flexing rapper reels (3 of 3) | Friends and co-creators (3 of 8) · Playing both sides (3 of 5) · Photo carousels (2 of 5) · Stream and live clips (2 of 5) | Brand deals (3 of 9) · Drinks and nights out (1 of 6) · Anuj as a famous figure (1 of 3) |
| **Lakmē** (typical reel 1,371 likes) | Aneet as Skin Goals Officer (10 of 14) · Coming-soon end card (4 of 5) · Skinterventions episodes (3 of 4) · Fashion Week (2 of 3) · 9to5 CC Cream, Ice dunking, Peach Milk gel (2 of 2 each) | Kareena (1 of 2) · Peptide Lip I.V. (1 of 2) · Perfect Radiance serum (1 of 2) | Hya Beach Edit makeup (0 of 5) · Identity list carousels (0 of 4) · Eyeconic Kajal (1 of 3) · Beauty-rule scold carousels (0 of 2) |

**Decision this implies:** Read uses one yardstick, the account's typical post now. A hit is double it. The stories, kinds, runs and post mortems all use it. Feed keeps its 90-day multiple for now.

## The reader works with you: taps first, a note when taps can't say it

The vault in Obsidian is yours. Here the reader writes the file, and the user steers it. Steering works through **taps on any node** (a thing, a post, a rule, the account), with **a short note** only where taps can't say it.

| Tap | Means | What happens |
|---|---|---|
| **Feed me** | "Go deeper on this." | The reader digs into it every run. It leads the stories, and its rule gets tested first. |
| **Skip** | "I don't care about this." | It folds away on the map and drops out of stories and alerts. Its posts still count. |
| **Watch** | "Tell me when this moves." | An alert when it changes sides (Feeding, Both ways or Fading), or when its rule breaks. |
| **Not right** | "This is wrong." | You pick what's wrong: the post isn't this thing, it's paid, these two are the same thing, or the name is wrong. Code applies it at once, and the reader keeps it. |

**A note** is free text on a node, for the two things taps can't do:
- **Tell it something:** "Aneet Padda became the ambassador on Sep 21." The reader uses it as fact and says "you told me".
- **Ask it something:** "Why did Episode 2 fade?" It answers under the note in the next run.

The account's own note, "Why I follow this account", sets the default Feed me / Skip choices.

**Why taps first:**
- They're unambiguous, so code applies them without guessing.
- They're one thumb on a phone.
- They show on the map as marks.
- Across users, they show what people care about.

**Why not taps only:** a tap can't tell the reader a fact or ask why.

**Why not a text box only:** asks come out vague ("make it better"), the reader has to guess intent, and typing is slow on a phone.

The reader can push back with data: "You skipped Brand deals, but they're 1 in 3 of his posts and they fade him." Its replies thread under notes, so you can watch it learn what you want.

**Build notes:**
- Taps and notes are stored per user, feeder and node.
- The run pack gets a "What you asked for" section.
- Code applies Not right and Skip deterministically.
- The check makes sure every Feed me gets depth, every Watch gets an alert rule, and every question gets an answer.
- They stay private to the user or their team.

## What this means for the map

- Three places with fixed sides: Feeding on one side, Fading on the other, Both ways in between. They're the same for every account.
- Within each place, things are ordered by size. Posts hang from their things on the side they went, so both-ways things fork, and that fork is the rule.
- Tags are a lens bar: who, what about, signature, brand. Pick one and it threads across all three places.
- Growth: each new post lands on its thing's feeding or fading side, a new thing enters where its first posts put it, and things cross between places as their record changes. Each crossing is a moment the reader can name.
- Taps show as marks on the node: Feed me, Skip (folded), Watch. Notes show as a pin with the reader's reply.

## What the pipeline needs

- Code computes feeding/fading per post against the account's typical post now (same format), and places each thing by its record. The reader names the things and the rules; things it finds today map onto things (kinds and moves) or tags (who, about, signature, brand), or nothing (filler, per "a node earns its spot").
- Keep the per-run state files (`state{k}.json`) so growth can be replayed. Today the page build drops them.
- The notes table and the run-pack section described above.
