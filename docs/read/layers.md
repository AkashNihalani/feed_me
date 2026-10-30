# Read: three layers, for any kind of account

Read explains an account in three layers: what the account is doing, what each run did, and what each post did. The questions are the same for every account: brand, creator, news page, meme page, event page, sports team. Only the labels adapt, and the reader picks them from what the account actually posts.

## 1. The account: what it's doing right now

Top of the page, in this order. Every fact has one home; nothing is said twice.

1. **The hero: the feed in one picture.** The character line, one plain sentence on what the account makes, three numbers (followers and their change, likes on a typical post, posts a week), then the picture: every post as a bar in posting order, as tall as it did against the account's other posts in its format, with the runs shaded behind and the follower count drawn underneath on the same dates. Three or four numbered notes are pinned to the bars, runs and stretches of the follower line that matter, and listed under the picture with their covers. The pairing is the point: it shows at a glance which posts brought likes and which brought followers (Anuj's Blinkit ad: 151,265 likes, about 400 followers; his growth came two weeks later with his own satire). Every bar and count comes from code; the reader writes the plain sentence and the notes.
2. **What it posts** (only when it adds something): storylines grouped from `post.story`. It is skipped when the storylines are the wall's own Repeats under another name, as for a creator whose series are his ideas; it shows for a brand whose campaigns cut across its ideas.
3. **Around the feed:** who's in it, who it's for, what it rides, money, how it posts. Shut, each row is a plain question and a plain answer, with no numbers to decode. Open, it gives the detail a stranger needs (names, what each post literally is, how each did) and a "So" line.
4. **Why posts land:** the rules.
5. **Every post:** the wall (Runs, Repeats, Ranks), with a key that says what a post's tag means. Each run shows what it was made of; each Ranks band shows the likes its posts got.

How to write any of it: describe every post you cite so a stranger can picture it ("a colleague blows her nose in a sock", never "the sock gag"), give places as "4th of 40", explain through a comparison with a twin post, and don't restate a rule where a cross-reference will do ("rule 05 below").

Dropped on Oct 17: the glance grid and the "In short" takeaways (they restated Repeats and Ranks), and share of likes per storyline (paid posts distort it).

| Slot | The question | Brand | Creator | News page | Meme page | Event page | Sports team |
|---|---|---|---|---|---|---|---|
| Running now | What is it pushing right now, how much of the feed, how is it landing? | Campaign, launch | Series, recurring bit | Story, beat | Format, running joke | Event, line-up | Match, transfer, season story |
| Just started | What showed up for the first time? | New launch | New format | Breaking story | New template | Newly announced event | New signing, new kit |
| Went quiet | What stopped, and was it working? | Range dropped | Series dropped | Story faded | Format retired | Event over | Story closed |
| Who's in it | Who does it build on? | Ambassadors, creators | Crew, collaborators | Reporters, sources | Templates' faces | Artists, hosts | Players, coach |
| Who it's talking to | Who do its posts address, in what language? | Its buyer | His crowd | Its readers | Its in-group | Attendees | Fans |
| What it's riding | Which outside moments does it hook onto? | Holidays, trends | Memes, news | The news cycle | Viral moments | Season, city | Matchdays, rivals |
| Where the money is | What's paid, sponsored or sold? | Shop links, co-posts | Brand deals | Sponsored posts | Paid promos | Tickets, sponsors | Sponsors, merch |
| How it posts | How much, which formats, how long? | Same for all | | | | | |

The running, started and stopped slots are now the status on each storyline row. Every number comes from code; the reader writes the words.

## 2. The run: what each 10 posts did

Opens from a run on the wall.

- **What it posted:** posts by storyline and by format (also shown under the run on the wall).
- **New / Stopped:** storylines that started or stopped this run.
- **How it landed:** the typical post against the last run, post by post.
- **Why it went this way:** the best and worst, with the reason.
- **What it did to the rules:** born, stronger, narrowed, dropped.
- **The call:** last run's call settled, and the next one made.

## 3. The post: what each post did

Opens from any post.

- **Where it landed** against its own format a week after posting, and against the posts before it.
- **Its storyline** (the campaign, series, story, match or format it belongs to), and whether it was paid.
- **What it is**, then **why it landed there**, and what it changed in the rules.
- **What happens in it:** the postcard's beats, with timestamps.
- **Its idea:** the kind of post it is, and how that idea usually lands.

## What's built (Oct 17 sample)

- **Lakmé (brand):** 8 storylines (Eyeconic relaunch, Hya Beach Edit, brand-wide, Perfect Radiance and Peach Milk Icy Sorbet stopped, Green Matcha CC+ new, Sun Expert SPF stopped, Peptide Lip IV stopped) and five Around rows.
- **Anuj (creator):** 10 storylines (his series plus Brand deals; football and food new, street bits and both-sides chats stopped) and four Around rows (his money is the Brand deals storyline).
- Data: `reader.glance`, `reader.brief`, `reader.stories` (notes keyed by storyline name; a creator's falls back to the idea's own note), `reader.around`; `post.story`, `post.paid`.

## What the pipeline needs for it

The per-post ledger entry (pipeline.md) records the storyline markers: product names, campaign or series tags, episode names, event or match names, the people in it, paid markers, links and offers, the moment, the language and who the post addresses. Code groups posts into storylines, finds starts and stops, and computes every number. The reader names the storylines, picks their labels from what the account is, and writes the lines.
