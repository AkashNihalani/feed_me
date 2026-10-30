# Read: the Strategy in three acts (Oct 30 2026)

How `read17Engine.js` builds Strategy, the Story's opener and the wall's live lane. Every number is counted by the
engine from `public/read17/<acct>.json`; the words come from the read file's `voice` where it has them.

## The footing

- **The usual post**: the median of the latest 100 posts that have reached day 7, on each count. Every × is against it.
- **The last read**: not stored yet, so the engine rebuilds it as it stood the day before this week's chapter began
  (18 Oct for these files): the 100 posts up to then, counted only once they were a week old then.
- **Marks**: a card is **New** where the last read had no such card, **Shifted** where its number went up or down by
  half or more, or crossed the usual post by more than a hair (15%), and **Held** otherwise.

## Act 1, Slices

Each slice gets four numbers: views (plays), likes and comments against the usual post, and follows against the pace
(below). A card leads on the multiple it stands out on; follows stay in the four, since a count can't be set against a
multiple. Plays show only where two of the slice's posts have them, and lead only where three do.

## Act 2, Views · Likes · Comments · Follows

Each part is what moves its count. The candidates are the sides that hold up (the "looks real, isn't" group is left
out), the formats and the regulars, each as its posts against the posts without it. A candidate moves a count when:

- the gap is 1.4× or more (for follows, half the usual post's follows);
- 60% of its posts sit on its side of the rest's typical post, so one big post can't make it;
- it holds inside the slices, going the same way in two of every three slices that have posts on both sides;
- it has four posts a side (plays, three).

Follows ride the calendar, so a follows mover is tested inside the weeks instead of the slices, needs four posting days
a side, and is measured **against the pace**: a post's "48h after" is the follower change over its day and the two
after it (checked against `daily`: every post matches), so every post that day shares it. The pace is three days at
the median daily change of the ten days either side, the post's own three left out.

## Act 3, Streaks · Shifts · Frequency

- **Streaks**: six or more in a row over or under the usual post, on likes and on comments.
- **Shifts**: the newer 50 against the older 50. The typical post on each count (kept where it moved by a quarter and
  crossed or moved by half), a typical follower day, and each slice's post count (doubled or halved, by five or more)
  and likes.
- **Frequency**: unchanged.

## The Story's opener and the live lane

- **Since the last read**: this week's typical post against last week's, on plays, likes and comments, and the two
  weeks' follower gains. It leads on the one that moved most.
- **The live lane**: posts under a week old. At day 3 each gets its call: its likes against the usual post's likes
  times the share of day-7 likes a post has by day 3. The read file's `curve` holds those shares, counted from
  `data/checkpoints-<acct>.json` (Anuj 0.916, Lakmē 0.887).

## Voice keys

- `voice.since`: `{line, why}` for the opener.
- `voice.parts.<part>`: `{hl, sub}`; numbers in `hl` marked `**so**`.
- `voice.cards["<part>:<rowKey>"]`: `{kick, count, line, aside}`. Row keys: a slice's name; a side's
  `label|slice|metric`; `fmt:Reels`; `reg:<regular>`; in Follows also `t10` and a day's ISO date; in Shifts `all:lk`,
  `days` and `slice:<name>`; Streaks and Frequency as before.
- Cards fall back on the old keys where they are the same thing read the same way: `sides:*` in the part its metric
  belongs to (`ct` in Comments, the rest in Likes), `followers:*` in Follows.

## Thin for now

- Plays: 25 reels an account (the 27 Oct Apify check), so Views has one or two cards.
- Lakmē's follows movers lean on Fashion Week, the only big swing in its 100.
- The last read is rebuilt from today's counts, not the counts as they were that day.
