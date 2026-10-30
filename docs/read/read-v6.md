# Read v6: the watcher's read (built Oct 21 2026)

v5's model stays (laws in the Feeder File, findings with proofs in the runs). v6 changes how it reads and where it sits.

## What changed

- **Voice.** Every reading is written by someone who watched every post in order. It says what the account is doing,
  what it's after and what that's getting it. One number goes where it matters, and the account's own words appear
  where they help. First person shows up sparingly ("my first guess"), and the runs read like a diary that understands
  more each time.
- **Links.** Words in a reading can quote a post, a law, a run or a thing. The syntax is `[label](P27)`, `[label](L2)`,
  `[label](run3)` and `[label](node:Name)`, rendered by `linkText` in `readProofs.js` as thin underlined inline links.
  A post opens its post mortem, a law or a thing opens in the Feeder File, and a run opens its read. A dropped law's
  label stays plain.
- **Node pages.** A thing's page is now:
  1. its read: who or what it is for a stranger, what the account is going for with it, and what it got;
  2. the law behind it;
  3. its posts, best first, each with what happens in it and where it landed, with the account's usual drawn between
     the posts above it and those under it.

  The "How it did" numbers, the bullet chart, "What it does" and "Seen with" are gone. A post row opens its post
  mortem. The map's own post page now says what happens in the post.
- **No node says the same thing twice.** A cast or reference node that a move covers post for post is dropped.
  - Anuj: his friends, his parents and Yash are "Scenes with his people". The creators are "Beside someone else's
    bit". The boyfriends and Ronaldo/Messi are "Playing both halves". The Drake edit is "The big-shot life". The
    election commissioner is "Riding the news".
  - Lakmē: the woman mid-hack and the viral hacks are "Stopping a hack mid-act". The briefcase is the sign-off's six
    posts. Amit Aggarwal is "Announcing Fashion Week". The Crayon Café is "Showing what's new".
  - Result: Anuj has 18 nodes and Lakmē 18.
- **Moves are nodes.** The wall's Repeats became the Feeder File's Formats wedge.
- **The wall reads by Runs and Ranks.**
  - Each run's breaker carries its take.
  - Each run's read is: what I made of it, post by post, what I noticed (the findings with their proofs), what changed
    in my read, the call I'd made, and what I'm watching for.
  - Ranks bands carry one line each, with links.
  - A post mortem's move row opens that move in the Feeder File.
- **The law page leads with the original rule split.** Each side's posts sit on a ledge at the height they usually
  landed, between "his best" and "his worst", with the usual drawn across the middle and the fall between the two
  ledges. Then come why it works, what to do, the proof (folded), how I got here, where it shows up, and the light on
  the wall. A law about followers or teasers has no split, so its proof leads, open.
- **Phone pages open in a reading sheet.** On a narrow card a page comes up in a sheet nearly the full height of the
  screen instead of half a page under the map. The list stays where it was.
  - The sheet's bar steps ‹ › and closes with ×, the scrim, Escape, or dragging the bar down.
  - A post mortem opened from the sheet comes up over it.
  - Full screen on a phone uses the same sheet, laid over the map.
- **The Read index.** The Anuj row now mirrors his read: v6 laws, run 3's take and moves, the call and the watch.

## The Feeder File on a phone, and back (Oct 21, v6.1)

After the scroll fixes, the phone map had become a preview that opened full screen. The user rejected that: "the map
should be free to explore; the pop-up of the selection on the bottom, then pull to expand/contract"; the readings under
the map can be "a nice bold strip"; and back felt inconsistent and didn't return you to your place.

- **Phone layout.** The Feeder File is the map plus one bold strip. The strip is the account's line in white on ink,
  "5 laws · 18 things · 30 posts" and an arrow up; it opens the whole reading.
  - The map takes a sideways drag (it turns), a pinch (zoom) and a tap (labels count); an up-or-down swipe scrolls the
    page.
- **The selection sheet.** A thing tapped on the map comes up as a peek (its head and first lines), with the map in
  view above it and what's open lit.
  - Pull it up to read at full height; pull it down to peek again, and down once more to put it away. A flick goes the
    way it was flicked; a tap on the bar toggles peek and full.
  - The strip opens the reading at full height; a page opened from there keeps the sheet's height.
  - While the sheet is up the page holds still and the app header stays folded (`ff:hold`, heard by readerEngine), and
    every gesture on the map is the map's. A tap on empty map, × or Escape puts it all away.
- **The peek is a glance** (the user: "still too big… it should have the title and some glanceable data points"). It
  shows the bar and the page's head and nothing more: what kind of thing it is, the title (clamped to two lines), and two
  or three chips.
  - A thing: posts and dates, how many landed, its typical Top %.
  - A law: its record, and its number ("65× the likes, same joke").
  - A post: date and length, likes, paid.
  - The account: posts in how many days, how many landed above the usual, posts a week.

  The peek's height is measured from that head, about a quarter of the screen. The heads are shared, so desktop pages
  show the same chips under their titles. The whole reading has no peek; folded, it's the strip.
- **Phone readings happen only in full screen** (Oct 22: the in-page pop-up "does the same thing again"; "keep the pop-up
  itself the toggle to go full screen").
  - In the page the Feeder File is the map plus the bold strip. The strip takes the map full screen, and so does a tap
    on a thing on the map, which then opens its glance there.
  - In full screen the strip sits at the foot of the map and opens the whole reading; the glance sheet peeks and pulls
    over the map; the map centres in the room above the glance.
  - A post mortem or a run's read opened from there comes up over full screen (`body.ff-full` lifts the read's sheet to
    z 2100). Lighting a law's posts on the wall leaves full screen first.
- **Full screen opens and closes as one move** (both widths). The card moves into a layer over the page, and that layer
  is clipped to the card's frame. The clip opens to the screen on the carry curve (680ms × tempo).
  - Meanwhile the camera keeps the map the size and place it had on the card (distance scaled by canvas height and
    field of view, view offset by the change in centre), then settles into the full-screen framing.
  - The card's head, the map's key and buttons, and the reading or strip glide from their old places to their new ones.
  - Closing reverses all of it and puts the card back at the end, where it lines up exactly.
  - With reduced motion it just switches.
- **Back** (both widths). Every page opened from the reading (a row, a chip, a link) or from the map goes on a trail.
  - Back is labelled with where it goes ("← Playing both halves", "← Everything") and lands exactly where you were in
    it: its scroll, and the wedge you had open.
  - ‹ › move in place. Closing empties the trail.
  - A law or a thing opened from a post mortem or a run's read keeps that sheet on the trail: back reopens it on the
    page where it was (`rd:law` / `rd:node` now carry `{ id, from }`; the Feeder File sends `ff:reopen`).
  - On a phone the sheet shows back only when there's somewhere to go back to.
- **Desktop.** Unchanged from the scroll fix: the map is pinned, the page scrolls the reading, and a plain wheel scrolls
  the page.

## Rules, proofs and deals as things you can see (Oct 22, v7)

The user: phone full screen "has nothing by default, only on clicks"; desktop "feels more text… they see text only to open
to see more text"; laws should be called rules, and "follower time stamps, impact etc." should be their own components.
Mid-build they added: collabs and ads should be marked clearly across the whole page, for brands and creators, since a
collab with a big account can be "blessed with a massive spike".

- **Laws are called rules** everywhere a reader sees them ("Rule 02", "The rules it tested", "The rule behind it"). The
  data keeps `laws`.
- **A rule is a card.** The card shows:
  - its kicker ("Rule 02 · sharpened in run 3");
  - the rule in a few words;
  - its number in red ("+2,349 followers, after the chant");
  - a small picture, its record ("Held 4 of 4").

  The picture is the rule's split when it has one: where its posts usually land against the others, on one track from
  best to worst, with the usual a tick. Otherwise it's a small version of its proof (followers day by day, each paid post
  against the usual, the two posts as bars, the posts in order, each teaser beside its episode).
- **A proof is its own card.** It shows the chart with its numbers on it: likes on the bars ("868K", "13K"), each paid
  post's share of the usual, and the usual named.
  - On desktop, "The proof" sits under the rules with the whole charts.
  - On a phone each rule is followed by its proof, unless the rule's own card already draws that proof (a rule with no
    split). So no card repeats another.
- **Phone full screen opens with something to read.** A rail of cards sits at the foot of the map:
  1. the account, with its numbers;
  2. each rule, then its proof;
  3. "Everything", which opens the whole reading.

  The card in the middle lights its posts and things on the map, and the map centres in the room above the rail. Tapping
  a card opens its page.
- **Desktop:** pointing at a card lights it on the map.
- **Deals, everywhere a post shows.** A post is one of three kinds of deal:
  - a **collab**: Instagram's co-post. It sits on the partner's feed too, and they share one like count.
  - **paid**: Instagram's paid-partnership label.
  - an **ad**: a brand deal the read spotted with no label, because the caption sells or the post is the ad.

  Where it shows:
  - A wall tile carries a corner tag: a collab in white so it reads first, paid or ad in dark.
  - A post card and its chips carry the tag too.
  - A post mortem gets a strip under its title: the tag, the partners ("with @aneetpadda_ · paid"), and a line on what
    sharing likely did.
  - In the Feeder File, a post row carries the tag, and a post's page has a chip and the same strip.
  - The account's numbers count deals: "9 brand deals, 5 as collabs" for Anuj, "3 partnerships, 2 collabs" for Lakmē.
  - A move's header counts its collabs and ads.
- **The collab line** is code-made from where the post landed against the usual:
  - well above (3× or more): "…most of that is likely their crowd";
  - above: "…part of this is likely their crowd";
  - about the usual: "…still only did his usual";
  - under: "…and still landed under his usual".

  A collab whose why already says what the partner's crowd did (`CO_SAID`) keeps only how a collab counts. We don't have
  partners' follower counts, so the line never claims a size.
- **Data.** Each post gets:
  - `paid`, from the facts;
  - `co`, the co-posters' handles, from the facts;
  - `deal`, from the content script's `DEALS` (Anuj: J&B, Star Sports ×2, Amazon; Lakmē none, since every post is its
    own);
  - `coNote`.

  `DEALS_WORD` names the account's deals. The shared helpers `dealOf` and `dealWith` live in `readProofs.js`.

## Smooth on a phone, rules open directly, what it's made of up front (Oct 22, v8)

The user, on the v7 preview: "the whole animation is not smooth and premium specially in mobile"; "the rules cannot have
the same pop up menu. They just open directly"; "the formats characters etc are way too buried in the menu".

- **Every card on the rail opens straight to its page at full height:** a rule, a proof, a wedge, the account,
  Everything. There's no glance first, and pulled down it goes (`data-ffnopeek`). A thing tapped on the map still opens
  as a glance with the map above it.
- **What it's made of leads.** Each wedge (Cast, Formats, Products & deals, Signatures, References) is a card. It shows
  the wedge's question ("What he keeps making"), its top three things with how many of their posts landed ("3/5"), and
  how many more there are.
  - On the phone rail they come right after the account, before the rules. The wedge in the middle lights on the map,
    softly, the way a wedge's row does.
  - A wedge card's head opens the wedge's own page: all its things, and the ones in a single post as pills.
  - In the reading ("What his posts are made of", above the rules, on desktop and in the phone's Everything), a card
    opens in place to all its things and the map turns to face that wedge.
- **Why it wasn't smooth** (measured in the test page):
  - Going full screen resized the WebGL canvas mid-move. The page stalled over a second in the test browser, and on a
    phone that's a dropped start.
  - The camera flew while a main-thread clip animated.
  - The map drew every frame under a sheet that covered it.
  - The rail read layout on every scroll frame.
  - Labels were restyled every frame even when nothing changed.
- **The fixes:**
  - **The map is screen-sized on a phone.** The canvas is the screen's size all the time, centred on the card and cropped
    by it. Only the part you can see is drawn (scissor).
  - **Full screen opens the frame round the map.**
    - Going full screen is edge to edge (`.ff-pn`): the frame's clip opens to the screen on the carry curve.
    - The map layer (canvas and labels) glides from where it sat to where it will rest, and the map isn't drawn during the
      move (`holdT`).
    - When it lands, the view takes the layer's offset in the frame the layer comes home, so nothing jumps.
    - The strip rides the frame's foot down and dissolves, and the rail's first cards rise in one after another as the
      frame finishes opening. Closing is the same, backwards.
  - **The map's size stays put on a phone.** It fits 90% of the width on the card and in full screen alike.
  - **Motion starts on a clean frame.** Content goes in first and the motion starts two frames later (`afterPaint`), so no
    motion starts on a heavy frame. That covers the sheet rise, a stepped page, and full screen.
  - **A page at full height holds the map** (`holdS`), and glides in flight pause with it.
  - **The rail finds its middle card from `scrollLeft` alone.**
  - **Labels are styled only when they change.**
- Fixed on the way: the split chart's "Its usual" moves to the free side when a ledge's caption sits on it (Lakmē's
  rule 01 on a phone).

## One sheet on a phone, and a full screen Safari can run (Oct 22, v9)

The user, on an iPhone in Safari: "always has cards blocking a big chunk of the screen"; "the exit animation for the
component on mobile is not optimised on safari ios"; and the rail of cards repeated the reading above it, while its
"Everything" card opened the same things again as a list.

- **One sheet replaces the rail and Everything.** In full screen on a phone the reading is a sheet with three heights:
  - **Docked** (where you land): the grab bar and one line of who this is: the picture, @handle, the one-liner on one
    line, and "5 rules · 18 things · 30 posts". It's about 90px plus the safe area, so the map has nearly the whole
    screen. The picture opens the account's page; the rest of the line raises the reading to half.
  - **Half**: the reading fills the bottom 56%: the account's line and numbers, what his posts are made of, the rules,
    the proofs. The map centres in the room above it. As the list scrolls, the card at the top of what you can see
    lights on the map (a wedge softly), worked out from the list's scroll against where its cards sit, read once.
  - **Full**: the reading at full height. The map holds still under it.
  - Drag or flick between them; a tap on the grab bar (or the dock line) goes docked ↔ half.
- **A card or a row in the list opens its page at full height**, straight away. Back returns to the list where it was
  (its scroll and its height). Pulled down, a page goes back to the list (a list left at full comes back at half, so the
  map shows).
- **A thing tapped on the map** still opens as a glance over the map. Closing it (×, a pull down, a tap on empty map,
  Escape) docks the sheet. Escape from the dock leaves full screen.
- **Nothing shows twice.** The rail and its CSS are gone, and so are the Everything card and the account card. In the
  list, the account's picture, handle and name live only in the dock line; the head under it starts with what he's
  doing. A rule whose own card already draws its proof (a rule with no split) has no separate proof card on a phone.
  On a phone a wedge card opens its page, as the rail's did. Back from a page says "← The reading" on a phone.
- **Full screen opens and closes with transforms only** ("60fps via subtraction; transform/opacity only"). iOS Safari
  doesn't run clip-path animations off the main thread.
  - The layer over the page is now a frame (`overflow: hidden`) that moves by translate and scale, from the card's rect
    to the screen. The layer inside it (`.ff-pin`) scales back so nothing in it stretches.
  - The map layer, the card's head and buttons, and the strip's stand-in glide inside it, each from where it sat to
    where it lands.
  - Every step is precomputed: 24 keyframes sampled on the carry curve, played linear. So the frame and everything
    counter-moving inside it stay locked together.
  - The frame's corners are set once, as elliptical radii in its own space, so they read 24px on the card. At full
    screen they go square, where an iPhone's own rounded screen covers them.
  - The page is no longer locked with `overflow: hidden` on the root, which moved Safari's bars and viewport. The
    full-screen layer holds itself with `touch-action: none` and `overscroll-behavior: none`.
- **The close lands cheaply.** Nothing is drawn or laid out when it starts. The map keeps the picture it has (held), and
  the frame and map layer carry the difference. The view goes back to the card's only at the end, in the same frame the
  card is put back. The sheet sinks first; the frame follows 40% of the way into the sink.
- **The choreography is kept.** On open the strip rides the frame's foot out and the dock rises once the frame is
  mostly open. On close the dock sinks first and the strip comes back up with the foot. Every motion starts after
  `afterPaint`.
- **Desktop is unchanged**: its reading keeps its order (account → made of → rules → proofs) and its full screen
  (`enterFull` / `exitFull`) still opens by clip.
- **Not checked on a device.** The test browser can't run iOS Safari. Whether Safari keeps the scale on its compositor
  with a WebGL canvas inside a rounded, clipped layer, and how the dock sits with the browser's bottom bar, need a look
  on the phone.

## A selection you can read on a phone, a map that holds 100 posts, the list on two shelves (Oct 22, v10)

The user, on an iPhone: selecting something "it's very hard to tell… There's not much distinction"; "many labels of other
nodes popping up"; "too many threads"; the light on threads "feels very low FPS"; "in 100 posts, how would it look?"; and
"too much scrolling in feeder file specially for mobile".

- **What's open comes forward; the rest goes well back (phone).** Things that aren't part of it drop to near nothing,
  greyer and up to a third smaller; posts that aren't its own shrink to small grey cards; its own things stand a touch
  bigger and the open one wears a wider halo. A wide card keeps the softer split. All by the existing vis lerp, so it
  glides.
- **Labels (phone):** only what's open and its direct things. Nothing it's merely seen with, no other planets, and post
  names only when a post is open. The hold-in/hold-out logic is kept, so names don't flicker.
- **Threads (phone):** only the open thing's own threads. "What it's seen with" threads and the dotted "did alike" arcs
  are gone on a phone; everything else thins to almost nothing.
- **Light that glides.** The light used to step 1/16th of a thread at a time and every segment was rewritten by the
  CPU every frame. Now each thread is laid once. Each frame writes a few numbers per thread into a small table: strength,
  red, where its light is, how far it has drawn in, which layer. The line shader lights every pixel from that table, so
  the light travels smoothly at the screen's own rate. It has a soft head and a long tail, and the open thing's threads
  breathe a little under it. The bold layer only gets the open thing's threads, copied when the selection changes.
  Cost (test browser): the loom's JS fell from about 0.06–0.17 ms to about 0.01–0.03 ms a frame, with no allocations.
- **100 posts.** Tested on a harness-only set (Anuj's 30 posts copied and jittered to 100, plus 16 new small things).
  - The ring stands at most 30 covers: the last two runs of 10, then the best of the rest. Every other post is a small
    dot at the head of its stem, coloured by where it landed. It stands its cover while it's lit or open. Why: the
    newest runs are what the reader is reading, the best are what the rules lean on, and 30 covers is how today's map
    already reads. A mark takes about a quarter of a card's room, so covers keep their size.
  - A planet needs a post in every 40 (two at 80+ posts), and a wedge stands at most 8. The rest lie on the floor as
    small hexes, still in the reading and still openable. The reading calls them "Also, smaller".
  - At rest only the covers' threads show, fainter past about 80 of them; the replay plays the covers.
  - Fewer names at rest as the feed grows (14 beside the reading and 8 on a phone at 30 posts; about 8 and 4 at 100).
  - At 30 posts every post is a card and nothing above changes the resting map.
- **The phone list is two shelves.** Under the dock line: "What his posts are made of" (the wedge cards) and "What his
  crowd rewards" (the rule cards). Each is a row that snaps card by card, the next one peeking.
  - Both fit the half-height sheet on a 390×844 screen, so the list doesn't scroll up and down. It still scrolls on a
    screen too short for both.
  - The card a shelf has snapped to lights on the map (a wedge softly), from the shelf touched last. That's worked out
    from `scrollLeft` against card offsets read once.
  - A card opens its page at full height. Back returns with both shelves where they were (`sx` on the trail).
  - Proof cards are off the phone list; each rule's page leads with its proof.
  - The account's paragraph, its numbers, the map's key and the rules let go moved to the account's page (the dock's
    picture). Desktop's reading is unchanged.
- **Not checked on a device.** iOS Safari's handling of the shader (float textures read in the vertex shader), how fast
  the shelves snap under a thumb, and how the thread light looks at 120Hz need a look on the phone.

## Fixed on the way

- Yash is one of Anuj's friends (the fingerprints show "friend spotted on street", and he's on screen in the J&B reel),
  not a star. The finding, the post's why and the nodes now say so.
- Clicking a post anywhere in the Feeder File's reading never opened its post mortem. The engine marked its open page
  with `data-sel`, and the read's router treats that attribute as a wall selection. It is `data-ffsel` now.
- The split's class names clashed with the share proof's (`.pf-sl`, `.pf-sg`, `.pf-sp`). The split now uses `.pf-x*`.
- Lakmē's proof heading said "Two of them, side by side" over four posts. A twin's heading now counts its rows.
- Lakmē's account said "3 co-posts", but one of the three is paid without a co-post. It now says "3 partnerships, 2
  collabs".

## Files

- Words: `apps/worker/scripts/read_v6_app_content_{anuj,lakme}.py` (LAWS, FINDINGS, RUNS, MOVES, NODE_TEXT,
  DROP_NODES, WHY, BANDS).
- Build: `read_v6_to_page.py [--apply]` adds the numbers and checks every link and every quoted Top %.
  - It writes `readerPreview.json`, both Feeder File files and the index row in `readerFeedPreview.json`.
  - The v5 files are kept as `*.v5.json`.
- Engines:
  - `feederSystemEngine.js`: node and law pages, the sheet, links, and `rd:node` / `rd:law`.
  - `readerEngine.js`: Runs + Ranks, run notes and labels, links, and the move row that opens its node.
  - `readProofs.js`: `linkText`, `plainText` and the `split` kind.
- Styles: `.ql` and `.pf-x*` in `readTab.css`; `.ffx`, `.ffs-prs`, `.ffs-pf`, `.ffs-dock` and `.ff-pin` in `feederFile.css`.

Revert to v5: copy `apps/web/src/data/*.v5.json` over `readerPreview.json`, `feederFileAnuj.json`,
`feederFileLakme.json` and `readerFeedPreview.json`. The engines still read v5 data (the v6 parts switch on
`reader.version >= 6`).
