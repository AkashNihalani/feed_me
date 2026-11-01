# The Story: what's moving on the feed right now (Oct 30 2026)

The Story is the live part of the Read: the biggest anomalies on the feed, so nobody has to scroll or open panels to
know what's going on. Strategy below holds the depth.

## Shape

1. **Bookend: This week.** The four counts (views, likes, comments, follows) against the usual post and against the
   week before. Always first.
2. **Pointers: 3 to 6, biggest first.** Each comes from a fixed menu of kinds, and each kind always looks the same. The
   feed decides which kinds show and in what order; we decide the menu, the look and the rules.
3. **Bookends: Followers, Frequency, Formats, Next.** Always last, always in that order.

## The menu of pointers

| Kind | Unit | When it shows |
|---|---|---|
| Breakout | a post | far over its usual on any count, including a young post on its day-1 count |
| Sank | a post | far under, including likes far behind plays |
| Streak | a run of posts | six or more in a row on one side of the usual, running or just ended |
| Gone quiet | a group | things that carry the account (slices, regulars, patterns) with no post lately |
| Trend | the usual post itself | the typical post moving far from where it was months ago |
| Pattern | a group | a group of posts beating the rest, tested by code (e.g. a name in the caption) |
| Looks boosted | a post | likes or plays arriving late, with comments flat, between checkpoints |
| Late climb / Stalled | a post | growth after day 3 far above or below the account's usual climb |
| Called it | a call | last read's call settling, right or wrong |

## Rules

- **Size** is how far from the account's own usual (as a multiple) times how rare: a run of seven, a 29× jump between
  checkpoints.
- **At most two of one kind,** so a big week never becomes five breakouts in a row.
- **Each pointer says which way it went:**
  - crimson bar for good;
  - grey for bad;
  - plain for neither (Looks boosted).

  Opening a Story that is mostly crimson tells the user it's working before they read a word.
- **Every pointer taps through** to its post, its run or its group.
- **It updates daily as checkpoints land.** Code finds the pointers; the day-3 step writes their lines from the posts in
  them. Strategy refreshes every 10 posts.

## In the read files

`stories[]` holds the pointers and bookends. Each pointer sets:

- `ch` to its kind, one of: Breakout, Sank, Streak, Quiet, Trend, Pattern, Boosted;
- `tone` to `up`, `dn` or `none`;
- `media` to one of:
  - `post`: one post by its cover;
  - `grid`: up to four posts, with `labels` per post;
  - `weeks`: weekly bars against the usual post.

The engine adds the "Since the last read" card first.
