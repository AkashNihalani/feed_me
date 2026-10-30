# Fingerprint v13: one shape for every post

Oct 19 2026. Replaces v12.1 (reels only) and observation_v1 (three prompts, three shapes). Why: [pipeline.md](pipeline.md), "Checked against today's prompts".

## The idea

Every post gets the same keys in the same order, whatever it is. The run reads 100 of them side by side, so it should find the same thing in the same place every time: what the post is first, then the named things it carries, then the post as it plays.

Only two things change by post:

- **Where a moment sits** (`at`): a time for a reel, a slide for a carousel, the frame for an image.
- **How much there is**: a 6-second skit gets 3 beats, a 15-slide carousel gets 10.

The page type never changes the shape. It changes what counts as a product and which details must stay exact (see below).

## The shape

```json
{
  "summary": "",
  "premise": "",
  "voice": "",
  "made": "",
  "things": [
    { "kind": "", "name": "", "note": "", "by": "", "from": "", "in": [] }
  ],
  "beats": [
    { "at": "", "shows": "", "said": [{ "by": "", "line": "" }], "text": [] }
  ],
  "caption_adds": "",
  "unsure": [],
  "confidence": "high"
}
```

Code wraps it in a header the model never writes: post key, handle, date, format, length in seconds (read from the file), slide count and which slides are video, what was observed (all, the first 2:00, slides 1–10 of 15), co-authors from the scrape, and the prompt version.

### The top four: what the post is

| Key | What goes in it | Limit |
|---|---|---|
| `summary` | The post for a stranger: who, what, the line or product that carries it | 25 words |
| `premise` | The setup and the turn as shown. For a sell: what's offered and how it's shown | 40 words |
| `voice` | Who the account is being: the persona, who speaks, the tone | 25 words |
| `made` | Camera, edit, layout, type, light | 25 words |

`premise` and `voice` are what the old prompts banned, and what the Read runs on: who the account is being, who the joke lands on. They stay observation: what the post sets up, never why it works or how it did.

### `things`: the index code reads

Each named thing in the post, once, with its exact name. Code pulls these at day 7 without a model: who's in it, brand deals, places, sounds and the rest, each with its posts and its split.

| `kind` | What it holds | Feeder File kind it feeds |
|---|---|---|
| `person` | Anyone seen or heard. `self` for the account's own face. Characters get `by` (who plays them) | Who's in it · The parts he plays |
| `product` | What's offered: product and shade, service, plan, package, dish, treatment, ticket. `note` keeps the specifics | What it's pushing |
| `brand` | A brand named, tagged, shown as the subject, or paid. `note` says how: paid label, #ad, co-post, tag, shown, said | Brand deals · Who it co-posts with |
| `place` | City, area, venue, store, destination | Where |
| `sound` | The track (title, artist), original audio, or voiceover; `note` says how it's used and quotes the hook | Sounds |
| `moment` | Event, festival, season, campaign, trend, meme, news, a film or show referenced | Moments it rides |
| `look` | Clothes, styling, props, sets worth recognising again. A worn brand stays in the name: "Versace robe" | His look |
| `device` | Named formats and tricks: a face filter for a second character, split screen, fake phone UI, POV camera frame, a series title and episode | How it's made |
| `line` | Words that could come back: catchphrases, slogans, campaign lines, campaign hashtags. `by` says who says or writes it | His words · Its lines |

- `from` is how we know it: `seen`, `heard`, `text` (on screen), `caption`, `tag`, or `known` (a public figure recognised but not named anywhere). `known` names go in `unsure` too.
- `in` lists the beats it appears in (1 is the first beat). Code uses it for timing: "the product shows up at 0:12 of 0:40".
- Exact names, always: the song title, the shade, the package name, the dish, the event. If the model isn't sure of a track, it describes it and quotes the hook. It never guesses an artist.
- Where-to-buy lists and standard hashtags go in `caption_adds`, not `things`.

### `beats`: the post as it plays

| Format | `at` | Beats |
|---|---|---|
| Reel | `0:00-0:02` | Beat 1 is always the opening (the first ~2 seconds). Group fast cuts that do one thing |
| Carousel | `slide 1`, `slides 4-7`, `slide 3 · 0:00-0:05` for video slides | One per slide, or per group of look-alike slides. Beat 1 is the cover |
| Image | `frame` | One, two at most (the whole frame, then a detail) |

- `shows` is what's on screen in that stretch.
- `said` keeps spoken or sung lines verbatim, each with who says it. `by` uses the same names as `things` (`self`, "the girlfriend", "Kareena Kapoor Khan").
- `text` keeps on-screen text verbatim.

### The rest

- `caption_adds`: what the caption adds that the media doesn't: credits, offer terms, dates, where to buy, an ad label, an instruction to comment. The full caption stays on the post row; this is never a copy of it.
- `unsure`: anything guessed or unclear that changes the reading.
- `confidence`: `high`, `medium` or `low`, for how well the media was seen.

## What the request tells the model

So it knows what to keep exact, without seeing any numbers:

- **The page type**, set once per feeder. Nothing stores it today: `feeders.bio` is empty for all 33.
- **Known names** from this account's earlier fingerprints, about 40, with their kinds: "use the same spelling if it's the same thing; never force a match". This is what lets code count Kareena or Hya Beach Edit across 100 posts.
- **Media facts from code**: length, slides and their types, the observed window, co-authors, and the audio name if the scrape has one.

It never gets likes, ranks or anything about performance.

### What to keep exact, by page type

The prompt carries this table. The shape is the same for all of them.

| Page | Keep exact |
|---|---|
| Creator | The parts they play and who plays them, recurring people, catchphrases, places, songs, outfits, brands that pay |
| Beauty or fashion brand | Product names, shades and variants, faces (celebrities, creators, models), campaigns, collabs |
| Restaurant or café | Dishes, drinks, prices, outlet or area, offers, chefs and staff, events |
| Bank or fintech | The product (card, account, loan), rates, fees, rewards, offers, terms, app screens |
| Travel | Destination, package name, nights, inclusions, price, dates, hotels, airlines |
| Clinic | Treatment, doctor, condition, before and after, price or offer, location |
| Events | Event name, date, venue, lineup, ticket tiers and prices |

## Size, by what code measures

| Tier | When | Beats | Things | About |
|---|---|---|---|---|
| S | Image, reel under 10 s, carousel of 1–2 slides | up to 3 | up to 10 | 250 words |
| M | Reel 10–30 s, carousel of 3–5 | up to 5 | up to 15 | 350 words |
| L | Reel 30–60 s, carousel of 6–8 or with one video | up to 7 | up to 20 | 500 words |
| XL | Reel over 60 s, carousel of 9+ or with 2+ videos | up to 10 | up to 25 | 700 words |

- **Words** count what the model writes, not the keys. The examples below come to about 250 (the 6-second reel), 330 (the 4-slide carousel) and 200 (the image).
- **Spoken words:** up to 180 words of quotes in total; 400 when talk carries the post (talking head, interview, explainer).
- **Caps from the cost plan:** reels are read up to 2:00, carousels up to 10 slides, and up to 60 seconds of each video slide.
- **Cost:** a typical reel lands near 450 words, about 1.2–1.4k tokens with the JSON, close to what the cost plan already assumed. The new part is the `things` index, and that's what lets code build the lean cards and the day-7 facts.

## Three posts in it

These are rebuilt from the fingerprints we already have, to show the shape, not new model output.

**Anuj, a 6-second reel** (p/dzhyigqbvs0). Who says what follows v12.1's reading.

```json
{
  "summary": "A man on a call plays both sides: his girlfriend says she's in a bad place, he answers 'Andheri East.'",
  "premise": "Setup: she means she feels low. Turn: he takes 'bad place' as a location and names a Mumbai neighbourhood, deadpan.",
  "voice": "Self playing both roles: a soft, filtered girlfriend and a flat, deadpan boyfriend.",
  "made": "Handheld low-angle selfie, natural indoor light, glitchy jump cuts between the two roles, one text line on top, ends on the punchline.",
  "things": [
    { "kind": "person", "name": "self", "note": "plays the boyfriend", "from": "seen", "in": [1, 3] },
    { "kind": "person", "name": "the girlfriend", "note": "the caller", "by": "self", "from": "seen", "in": [2] },
    { "kind": "place", "name": "Andheri East", "note": "Mumbai neighbourhood; the punchline", "from": "heard", "in": [3] },
    { "kind": "line", "name": "in a bad place rn", "note": "on screen and in the caption", "by": "self", "from": "text", "in": [1, 2, 3] },
    { "kind": "device", "name": "face filter for a second character", "from": "seen", "in": [2] },
    { "kind": "sound", "name": "original audio", "note": "dialogue only, her voice slightly distorted", "from": "heard", "in": [1, 2, 3] }
  ],
  "beats": [
    { "at": "0:00-0:01", "shows": "Low-angle close-up: he holds a pink-cased phone to his ear; white curtains, a ceiling fan.", "said": [{ "by": "self", "line": "Hey." }], "text": ["in a bad place rn"] },
    { "at": "0:01-0:03", "shows": "Quick glitchy cuts to him as the girlfriend, looking into the camera.", "said": [{ "by": "the girlfriend", "line": "Hey babe, I'm in a really bad place right now." }], "text": [] },
    { "at": "0:03-0:06", "shows": "Back to him; he looks down, then off to the side, flat-faced. It ends on the line.", "said": [{ "by": "self", "line": "Damn. Andheri East." }], "text": [] }
  ],
  "caption_adds": "Repeats the on-screen line with a sad emoji.",
  "unsure": ["The girlfriend's look: a face filter or a wig and makeup; the cuts are too quick."],
  "confidence": "high"
}
```

**Lakmé, a 4-slide carousel** (p/daap84ndcde).

```json
{
  "summary": "Four beach collages pairing Lakmē 9to5 Hya Beach Edit makeup with Mia by Tanishq gold jewellery.",
  "premise": "A co-branded summer look: the makeup and the jewellery shown side by side on sand, sea and poolside, product shots mixed with a model wearing both.",
  "voice": "The brand as a summer stylist. No one speaks; the slides carry the names.",
  "made": "Bright, high-contrast collages with white borders, 4–5 photos a slide, studio product shots mixed with lifestyle shots; blues, beiges and pinks.",
  "things": [
    { "kind": "brand", "name": "Mia by Tanishq", "note": "co-branded: logo on slide 1, named in the caption", "from": "text", "in": [1] },
    { "kind": "product", "name": "Lakmē 9to5 Hya Beach Edit", "note": "lip tints, cushion compacts, blush sticks; 'Sunkissed'", "from": "text", "in": [1, 2, 3, 4] },
    { "kind": "product", "name": "Mia by Tanishq gold jewellery", "note": "earrings, bracelets, layered necklaces, heart studs; shell and sun motifs", "from": "seen", "in": [1, 2, 3, 4] },
    { "kind": "person", "name": "a model", "note": "wears both; face close-ups, sun hat", "from": "seen", "in": [2, 4] },
    { "kind": "place", "name": "beach and poolside", "note": "sand, shells, waves, a pool", "from": "seen", "in": [1, 2, 3, 4] },
    { "kind": "line", "name": "Summer Stackin'", "note": "campaign line", "from": "caption", "in": [] },
    { "kind": "device", "name": "white-border photo collage", "note": "every slide", "from": "seen", "in": [1, 2, 3, 4] },
    { "kind": "look", "name": "lip tints pinned to a clothesline over the sea", "from": "seen", "in": [3] }
  ],
  "beats": [
    { "at": "slide 1", "shows": "Collage: pink makeup sticks stacked on sand, a gold earring, a hand in gold bracelets on sand, a lip tint held against blue sky.", "said": [], "text": ["LAKMĒ 9TO5 x Mia by TANISHQ", "HYA BEACH EDIT"] },
    { "at": "slide 2", "shows": "Cushion compacts on sand with shells, the model's eye and cheek, the model in a sun hat, glossy pink lips, necklaces on wet sand.", "said": [], "text": ["LAKMĒ 9TO5 HYA BEACH EDIT"] },
    { "at": "slide 3", "shows": "A blush stick on a cheek, four lip tints on a clothesline over the sea, jewellery on sand, heart studs.", "said": [], "text": ["LAKMĒ 9TO5"] },
    { "at": "slide 4", "shows": "A blush stick in a denim pocket, the model in layered necklaces, an open cushion compact, six lip tints by a pool.", "said": [], "text": ["LAKMĒ 9TO5"] }
  ],
  "caption_adds": "Names the collab and the 'Sunkissed' edit; tells you to stack rings and bracelets and finish with a sun-kissed glow.",
  "unsure": [],
  "confidence": "high"
}
```

If the scrape lists Mia by Tanishq as a co-author, code adds "co-post" to that brand's note from the header. The model doesn't have to spot it.

**Lakmé, an image** (p/dzmfpjbtho-).

```json
{
  "summary": "A meme: a crying white cat with long, perfect lashes holds an iced matcha in front of an erupting volcano, under 'Cried and how, but the mascara stayed.'",
  "premise": "Setup: crying through a disaster. Turn: the lashes survive it. A mascara claim told as a cat meme.",
  "voice": "The brand in meme speak, as someone who's had 'multiple breakdowns'.",
  "made": "Edited composite in a meme layout: black text on a white header, soft front light on the cat against glowing lava.",
  "things": [
    { "kind": "product", "name": "Lakmē Eyeconic", "note": "the range named in the caption; 'mascara' in the meme text", "from": "caption", "in": [] },
    { "kind": "moment", "name": "babushka cat meme", "note": "a cat in a floral headscarf", "from": "seen", "in": [1] },
    { "kind": "line", "name": "Cried and how, but the mascara stayed", "note": "meme text", "by": "self", "from": "text", "in": [1] },
    { "kind": "look", "name": "pink floral headscarf, iced matcha latte", "note": "on and in the hands of the cat", "from": "seen", "in": [1] }
  ],
  "beats": [
    { "at": "frame", "shows": "A white cat with human-like edits, tear-filled eyes and long intact lashes, a pink floral headscarf, an iced matcha with a straw; behind it, a volcano erupting orange lava.", "said": [], "text": ["Cried and how,", "but the mascara stayed"] }
  ],
  "caption_adds": "'Multiple breakdowns later and still flawless lashes'; the Eyeconic range is on sale at Tira, Myntra Beauty, Nykaa and in stores.",
  "unsure": [],
  "confidence": "high"
}
```

## What it keeps and drops

- **Kept from observation_v1:** neutral watching, the slide-by-slide notes, verbatim on-screen text, the lean length, uncertainty and confidence.
- **Kept from v12.1:** sound gets its own place, and a track is named the same way everywhere.
- **Dropped:** v12.1's separate visual, audio and edit lists (they become beats plus `made`), the mixed entities list (now `things` by kind), the free-floating "alignments" (now `premise`), and observation_v1's `kept_lines`, `kept_text` and `kept_visuals` (lines and text move into the beats where they happen; visuals into `look` and `device` things). Neither model writes a length anymore.

## Open before the test

1. **Page type:** we set it for the test feeder by hand; later, the first run can propose it and the user confirms.
2. **Audio name and paid label from the scrape:** check whether Bright Data's post record carries them. If so, code passes them in and the model copies them.
3. **One prompt or three:** one prompt with a short section per format keeps the shape identical. That's the recommendation.

## Test run: Lakmé, Sep 21 – Oct 1 (Oct 19)

30 posts in a row (14 reels, 13 carousels, 3 images), all past day 7, on Gemini 3.8 Flash with the schema enforced (`scripts/run_fingerprint_v13_test.py`, prompt `scripts/fingerprint_v13_prompt.md`, outputs and `review.md` in `scripts/out/fingerprint_v13/lakmeindia/`).

- **Cost:** $0.27 for the 30 at standard price, about ₹0.9 a post; batch halves it. Every call is in `ledger.jsonl`: 51 paid calls in all (a first pass on 21 posts, a second pass with the prompt fixes, then the 9 gap posts), none failed. $0.45 in total.
- **Media:** 16 posts from R2, 5 from live Instagram links, 9 from one scrape of just those links (no database writes). All 30 are now cached on disk, so a re-run needs no live link and no new scrape.
- **What it keeps:** exact shades (Red Marsala, Cherry Cola, Nude Nector), product names, prices (Forever Matte Lip Crayon, Rs. 399), partners (FDCI, NEXA, Reliance Brands, Bru), Hinglish lines verbatim with who says them, characters ("Poo" and "Geet", played by Kareena Kapoor Khan), the series and its episodes ("Skinterventions Ft. Aneet"), and the moments (Lakmē Fashion Week, Ganpati season).
- **The index works:** across the 30, code can already count Aneet Padda (14 posts), "Skin Goals Officer" (14), "Skinterventions Ft. Aneet" (9), the white vest and necktie (12) and the Hya Beach Edit products, with no model.
- **Fixed by the second pass:** Aneet written as `self` (6 to 0), stores filed as brands (4 to 0), series cards with no series entry (3 to 0), "Lakmē 9to5" filed as a brand (6 to 0).
- **Left to code:** names that differ only by accents or capitals (Lakmē/Lakmé) are merged when the index is built; a beat reference past the last beat is dropped (one reel); the paid label comes from Instagram's own flag in the header, not the model's note (it missed it on 2 of 3).

**What the scrape carries that the worker throws away:** the reels dataset returns `length`, `is_paid_partnership`, `coauthor_producers` and `tagged_users`; the posts dataset returns `post_content` (slides in order, each marked photo or video) and `videos_duration`. Neither returns an audio name, so sounds stay the model's job. The worker keeps only co-authors today (`related_handles`).

## Test run: Anuj, Sep 27 – Oct 11 (Oct 19)

His newest 30 posts past day 7 (24 reels, 5 carousels, 1 image), with two prompt fixes first: every audible post gets a sound entry, and unnamed people ("a friend in a blue shirt") stay out of the known names so they can't merge across posts. 16 posts came from R2 or live links; the other 14 reels from one scrape of just those links. 30 of 30 calls succeeded, $0.28.

- **The creator side works:** characters he plays carry `by: self` (Ronaldo with a baby-face filter and Messi; "the cheating boyfriend" and "the loyal boyfriend"; Gyanesh Kumar in a parody); 146 spoken lines, all with a speaker, 82 his own; exact songs ("Pehli Nazar Mein — Atif Aslam", "Baat Baaki — Asha Bhosle & Bappi Lahiri"); brand deals with exact products (REDMI Note 17 Pro 5G with Anup Soni, J&B Rare Scotch).
- **Sound is fixed:** 24 of the 24 audible posts have a sound entry (Lakmé's run had 14 of 21).
- **The index is thinner than Lakmé's:** this stretch is livestreams, parodies and paid collabs, so few things repeat (Yash 2, Gyanesh Kumar 2, the black robe and sunglasses 5). His friends are mostly unnamed in the posts ("a friend in a blue shirt"), so the crew can't be counted unless captions or tags name them.
- **Paid label:** Instagram's flag is known for the 14 scraped reels (5 paid). For the other 16 it's unknown, because the worker never stored it; J&B looks like a deal but isn't flagged.
- **Small slips:** prose fields sometimes say "self" instead of his name, which a stranger can't read; one parody lists the account and the character twice; 5 posts used banned words ("hook", "viral").

Spend so far, from the ledgers: 81 Gemini calls, all successful, $0.74 (Lakmé $0.45 over two passes and the gaps; Anuj $0.28), plus three small Bright Data scrapes covering 23 posts.
