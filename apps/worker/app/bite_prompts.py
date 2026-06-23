"""Bite cycle prompts — the ONLY prompts for the lane pipeline.

Fingerprint (the shared observation layer) lives here as the single source;
feeder_prompts.py re-imports it. Everything else in here is the 4-lane x 3-step
bite cycle: cold_start (seed) / allot (select) / write (receipts).
"""
from __future__ import annotations


# ===== FINGERPRINT — shared observation layer (moved verbatim) =====
FINGERPRINT_PROMPT_VERSION = "fingerprint_v12_1_schema_locked"
FINGERPRINT_SAMPLING_POLICY_VERSION = "media_sample_v2_120s_all_slides"


FINGERPRINT_EXTRACTION_SYSTEM_V8 = """REEL FINGERPRINT EXTRACTION

You will receive one Instagram Reel with its caption and available media.

Your job:
create a neutral observation fingerprint.

This is the raw observation layer. Do not interpret strategy, classify content, infer audience psychology,
or decide what pattern it belongs to. Capture only what can be seen, heard, read, or directly aligned
between caption, visuals, transcript, edit, and audio.

Return valid JSON only.

OUTPUT

{
  "post_key": "",
  "media_type": "reel",
  "duration_seconds": null,
  "media_truncated": false,
  "observed_window": "",
  "caption": "",
  "transcript": "",
  "visible_text": [],
  "visual_sequence": [
    {
      "timestamp_range": "",
      "description": ""
    }
  ],
  "audio_behavior": [
    {
      "timestamp_range": "",
      "description": ""
    }
  ],
  "cultural_references": [
    {
      "reference": "",
      "channel": "",
      "timestamp": "",
      "co_occurring": ""
    }
  ],
  "edit_and_pacing": [],
  "environment_and_entities": [],
  "observed_alignments": [],
  "notable_observed_details": [],
  "uncertainties": [],
  "media_confidence": "high"
}

FIELD RULES

duration_seconds:
Copy the supplied original duration exactly when present. If no duration is
supplied, leave it null and note the gap in uncertainties. NEVER estimate a
duration from the visual sequence.

media_truncated:
true only when the supplied duration is above 120 seconds and you are observing
only the sampled first 120 seconds. Otherwise false.

observed_window:
If media_truncated is true, use "0:00-2:00". Otherwise use the observed duration
span if clear, or leave empty.

caption:
Copy the caption text as supplied. Preserve code-switching, slang, punctuation, mentions, and hashtags.

transcript:
Do NOT write a full transcript. For talk-heavy reels, write a compact spoken-content digest
with only the most important exact phrases quoted. Preserve Hinglish, slang, names, and
repeated phrases when they carry the reel, but cap this field at 180 words. If the reel
has more speech than fits, summarize the rest in neutral observation language and add an
uncertainty like "speech compressed to fit fingerprint budget".

visible_text:
Exact on-screen text strings only.

visual_sequence:
Describe what happens on screen in order. Use compact timestamp ranges. Each description should include
framing, subject, action, visible objects, and any visible text if relevant.

Describe what a viewer would actually remember, not just what is technically in frame:
- body language, facial expressions, delivery, character work, parody cues
- interaction dynamics: who leads, who reacts, who performs for whom
- product/food/place details when the object or environment is the content
- transitions that carry meaning: hard cut, slow zoom, reveal, repetition

audio_behavior:
Describe music, spoken delivery, silence, sound effects, beat drops, audio-text sync, and tonal changes.
Capture how things sound, not just what plays.

MUSIC IDENTIFICATION applies to every track that plays. When a track is
recognizable, name it. When it is not, quote any audible hook lyric verbatim and
describe its sound signature precisely enough to recognize the same track in a
future reel. If a track or sound is identified anywhere in this fingerprint,
including cultural_references, name it identically everywhere it is mentioned.
Never describe a track generically in audio_behavior while naming it elsewhere.

cultural_references:
Recognizable nods to something outside the reel itself that a viewer is meant
to clock: a film, song-as-reference, show, news beat, meme, trend, internet
moment, public figure, brand cameo, or current event. Each entry has exactly
four fields:
  reference    - named as exactly as possible; films get (year)
  channel      - exactly one of "audio", "visual", "caption", "cross-modal"
  timestamp    - timestamp/range, or "caption"
  co_occurring - what is being said, shown, or written at that beat

If a cue is just mood music, keep it in audio_behavior, not here. A trend or
aesthetic qualifies only when specific observable markers carry it; name those
markers in co_occurring. Empty cultural_references is valid.

edit_and_pacing:
Observable editing only: jump cuts, snap cuts, zooms, overlays, filters, slow motion, repeated loops,
hard cuts, split screens, or changes in shot duration.

environment_and_entities:
People, products, props, locations, brands, objects, wardrobe, devices, UI elements, screens.

observed_alignments:
An array of PLAIN STRINGS only. Use this when two or more observable elements
line up or contradict each other. Never return objects or key-value structures.
Examples:
- voiceover claims skill while visuals show failure
- caption reframes the visual as sarcasm
- lyric hits exactly when facial expression changes
- tissue comes away clean after product contact
Limit to the 3-5 strongest alignments.

notable_observed_details:
Concrete facts another model could use as receipts. Do not explain why they matter.

IMPORTANT

- Reels only. If non-reel media is supplied, set media_type to "reel" and put the uncertainty in uncertainties.
- Do not mention views, likes, comments, ranking, account history, or alerts.
- Do not use clustering language.
- Do not create pattern names.
- Field shapes are a contract: arrays of strings stay arrays of strings, and
  objects keep exactly the keys shown. Never add keys or substitute objects
  where strings are specified.
- Do not call anything "proof," "payoff," "viewer psychology," or "strategy" unless those exact words appear in the post.
- If the post is sarcastic, ironic, performative, or fictional, capture the observable cues that reveal that.

WORD BUDGET

Total fingerprint body excluding caption and visible_text: 340-950 words. The transcript
field is included in this cap via the 180-word transcript limit above. Simple product reels
should land around 400. Dense multi-character skits around 800-900. Let content dictate
length within the budget, but never exhaust the response on verbatim speech.

LONG REEL / BIG PAYLOAD DISCIPLINE

If the reel has dense dialogue, many cuts, or approaches the 120-second sampling window:
- compress speech first; do not transcribe line by line
- prefer 6-10 strong visual_sequence entries over exhaustive shot inventory
- prefer 4-8 audio_behavior/edit details over full narration
- always finish the JSON object; a compact complete fingerprint beats a long incomplete one
"""


# ===== BITE CYCLE (4 lanes x 3 steps) =====
#
# Each lane (visual / voice / premise / text) runs three operations:
#   cold_start  first chunk only — discover the lane's bites, author the seed file
#   allot       every later chunk — match each post to existing bites, or flag new
#   write       author the receipts that go into the feeder + bite files
#
# A prompt = LANE_DEF + shared discipline blocks + one OPERATION block. The runner
# substitutes the handle via .replace("{handle}", handle) — NOT str.format — so the
# JSON braces below stay literal.

BITE_CYCLE_VERSION = "bite_cycle_v1"

# ---- shared block: what every operation is handed ----
_GIVEN = """WHAT YOU ARE GIVEN (JSON)
account.handle
chunk { posts, from, to }
posts[] newest first, each with:
  alias        "P01".. Cite posts ONLY by alias and the name you mint.
  posted_at, duration_seconds, caption
  fingerprint  the neutral observation record. MINE it; never repeat it wholesale.
  performance  worker-computed, copied never adjusted:
    rank_context { current, overall, read }
    anomalies    present ONLY when the worker flagged a deviation.
"""

# ---- shared block: what a bite IS (the altitude rule) ----
_OVERVIEW_RULE = """WHAT A BITE IS — OVERVIEW ALTITUDE
A bite is ONE invariant premise, named as a recognizable move or format. Span every
kind of account — a challenge channel, a news desk, a musician, a brand, a
commentator, a food page — never assume a niche. The premise stays fixed across
every appearance; that fixedness is what makes it a bite. It is specific (one clear,
checkable premise) yet execution-agnostic (it can be done many ways).
  . Executions are NOT bites. A sea-level run and a mountain run are the same
    "challenge with escalating stakes"; a literal demo and a before/after are the
    same "claim proven on camera"; a football-final take and an awards-night take
    are the same "rides a live event's hype". ALL execution variance lives in the
    receipt (how_it_shows_up), never in a second bite.
  . The SUBJECT shown is part of the execution, never a new bite. A recipe page's
    ingredient close-up montage is the SAME bite whether the dish is pasta or
    pancakes; a travel page's drone reveal is the SAME bite whether it opens on a
    beach or a city. What it depicts, or who performs it, goes in the receipt —
    never splits into two bites.
  . Two bites are never "similar". Either they share a premise (then they are ONE
    bite, different executions) or they have different premises (then they are
    different bites). There is no in-between to reconcile.
  . Pitch the premise between useless-broad ("uses humor", "shows the product") and
    too-narrow ("applies product on the left cheek"). The target is the named move a
    viewer would recognize recurring across posts, whatever the account.
"""

# ---- shared block: weight (lane-specific core line injected) ----
_WEIGHT = """WEIGHT — how much the bite carried THIS post, within this lane (removal test)
  core        {core_line} Remove it and the post is not the same. ONE core max.
  supporting  present and shaping the post, but it survives without it.
  standby     present, removable with no real change. KEEP these — the baseline the
              core executions are read against.
Weight is the per-execution importance, not success. Whether a post ranked high or
low lives in rank_context, never in the weight. Different executions of one bite
take different weights across posts — that difference is the read, never a reason to
split the bite.
"""

# ---- shared block: receipt laws ----
_RECEIPT_LAWS = """RECEIPT LAWS
1. Timestamped & self-contained. how_it_shows_up anchors to a timestamp or a
   fingerprint quote, references only its own post, and states magnitude in
   absolutes (durations, counts, exact lines/props) so it survives after every
   other post rolls out of the window.
2. Capture the variant axis. Record the execution specifics that set THIS
   appearance apart from other executions of the same bite — that is what the
   account intelligence compares later. "explainer reel" throws the comparison
   away; "female presenter, hand-drawn graphics, opens cold on a question" keeps it.
3. Role justifies weight. role_in_bite says what the bite did to this post and why
   its weight is what it is.
"""

# ---- shared block: length & proof caps ----
_CAPS = """LENGTH & PROOF CAPS
  . Max 3 receipts per bite — the clearest, most spread-apart executions. Even a
    long video gives enough proof in 3 timestamped executions; never list more.
  . Keep prose tight: each contract condition <= 18 words; how_it_shows_up <= 40
    words; role_in_bite <= 25 words. One sentence where one will do.
"""

# ---- per-lane definitions: walls + what-earns (overview altitude) + core line ----
_LANE_DEFS = {
    "visual": {
        "core_line": "the post's look is carried by this bite.",
        "def": """VISUAL LANE — the whole visual treatment the account builds
FIRST read WHICH KIND of reel this is, because the craft differs:
  . GRAPHIC / ANIMATED (motion graphics, screen-built, text-driven, animated
    explainer): track the design system — animation style, motion, transitions,
    typography-as-motion, colour/graphic language, UI or screen treatment.
  . REAL-WORLD / SHOT (filmed people, places, products): track the edit and frame —
    cuts, pacing, framing, composition, camera moves, recurring props/wardrobe/
    setting, on-camera presence.
Across both, catch recurring visual moves: shot grammar, a visual device
(split-screen, grayscale pivot, slow-mo hero, before/after wipe), a MEME cutaway or
insert cut in to land a reaction or switch the mood, and mood- or pace-shifting cuts
that keep it engaging or carry the narrative.
NOT yours: the meaning of words (text lane), anything heard (voice lane), what drives
the post (engine lane). On-screen text: the words belong to the text lane — you take
it ONLY when its typographic STYLING is itself a recurring look.
Overview examples (teach altitude, do not copy): "animated explainer style" is one
bite whether it charts data or illustrates a story — the design language is the move.
"meme cutaway reaction" is one bite whatever meme is cut to. "before/after reveal" is
one bite across a room makeover or a fitness change.""",
    },
    "voice": {
        "core_line": "the post is carried by its delivery or its audio moment.",
        "def": """VOICE LANE — everything HEARD: delivery, tonality, and functional audio
LED by VOCAL DELIVERY and tonality (deadpan, sarcastic, exaggerated, hyped, sincere,
dry — observed delivery, never a strategy claim) and recurring spoken signatures or
catchphrases. ALSO catch FUNCTIONAL AUDIO: audio signatures (a composed jingle, a
co-opted trending sound that recurs as a trigger, a house music mood) AND mood/pace
devices — a sound effect (an explosion, a riser, a whoosh, a record-scratch, a
sting), a beat drop or hard music cut used to SWITCH the mood, lift the pace, or
punctuate the narrative. Silence used as a device counts.
NOT yours: the meaning of the words (text lane), what is on screen (visual lane), what
drives the post (engine lane). You own the DELIVERY of a line and the SOUND, never the words'
content.
Overview examples (teach altitude, do not copy): "deadpan list delivery" is one bite
whether the list is chores or red flags. "SFX mood-switch punctuation" is one bite
whether it's an explosion on a reveal or a record-scratch on a turn. "branded audio
tag" is one bite whether a sung jingle or a borrowed trigger sound.""",
    },
    "engine": {
        "core_line": "the post is built on this engine.",
        "def": """ENGINE LANE — what drives the post and makes it land
Read PAST what is shown to what the post is DOING to land, and why a viewer reacts.
Mine the fingerprint's caption, transcript, and cultural_references to catch the move
AND what it rides. The recurring move lives at overview altitude; the specific topic,
trend, or event is the execution (receipt), never its own bite.
Catch moves like: a deliberately divisive or controversial take built to provoke
disagreement (ragebait); latching onto a live social moment for reach — a political
beat, a Bollywood or pop-culture reference, a sports final, a war or news cycle, a
trending name; riding a meme or format wave; a practice's benefit made the point (a
yoga page leveraging deep breathing for stress release); a confessional self-own
reframed as relatable; a claim proven on camera; stakes escalated by a stated rule.
  . NAME WHAT IT RIDES — but only content moves. Riding a trend, meme, event, or
    controversy IS an engine (a content move). A collab or a promoted push is NOT an
    engine — it is borrowed distribution, handled by the performance layer. A collab
    post still runs on a concept engine underneath: find THAT, and note the collab
    only as context in the receipt. Never make "it's a collab" its own bite.
  . Read the layer beneath, never invent it. Ground every read in an observable cue —
    the divisive claim in the caption, the named event in the references, the
    escalation in the transcript. No cue, no claim.
NOT yours: what is on screen (visual lane), how it is said or scored (voice lane),
the exact wording (text lane). You own the conceptual move and its framing.
Overview examples (teach altitude, do not copy): "rides a live social moment" is one
bite whether a political take, a Bollywood reference, or a trending name — the receipt
names which. "macro food showcase" is one bite whether shot as crisp ASMR cuts or a
silent hero pan — ASMR is the variant, the receipt carries it. "the product earns its
claim on camera" is one bite whether a stress test or a before/after.""",
    },
    "text": {
        "core_line": "the words carry the post.",
        "def": """TEXT LANE — every WORD the account writes, and what the words DO
You own written language on BOTH surfaces (caption and on-screen text). Catch two
things:
  . what the words DO: a caption that reframes the visual, an on-screen line that
    lands the joke, a recurring caption structure (a TLDR opener, a cold hook).
  . the VOICE of the writing and shifts in it: register and tonality — and a move
    away from formal/salesy copy toward casual, meme-y, first-person; code-switching
    or Hinglish; a consistent written persona. A brand loosening its written tone is
    itself a bite worth catching.
NOT yours: how words are spoken (voice lane), how text is styled as a look (visual
lane), what drives the post (engine lane). Ignore noise — hashtag blocks, boilerplate
CTAs, SEO tails.
Overview examples (teach altitude, do not copy): "caption reframes the literal
visual" is one bite whether a pun or a sarcastic turn. "casual first-person brand
voice" is one bite whether a meme caption or a self-aware aside — the shift off
formal product copy is the move. "on-screen punchline line" is one bite across any
joke the footage sets up.""",
    },
}

# ---- operation blocks ----
_OP_COLD_START = """OPERATION — COLD START (SEED)
This is the first {lane} read of @{handle}: there is no memory yet. Discover the
lane's bites from these posts and author the seed file. Mint every bite at overview
altitude and fold all execution variance into receipts. Admit a bite only if it
recurs across 2+ posts OR is core to at least one post; a one-off background detail
is noise — drop it.

OUTPUT — return ONLY this JSON. First char "{", last "}". No prose, no markdown.
{
  "lens": "{lane}", "stage": "cold_start", "handle": "",
  "chunk": { "posts": 0, "from": "", "to": "" },
  "post_names": { "P01": "" },
  "bites": [
    {
      "name": "snake_case_overview_premise",
      "contract": { "1": "invariant premise condition", "2": "..." },
      "weights_tally": { "core": 0, "supporting": 0, "standby": 0 },
      "receipts": [
        {
          "post": "minted name (P01)", "date": "",
          "rank_context": { "current": "", "overall": "", "read": "" },
          "anomalies": ["only if present for this post"],
          "weight": "core | supporting | standby",
          "how_it_shows_up": "timestamped, execution specifics, variant axis",
          "role_in_bite": "what it did; argues the weight"
        }
      ]
    }
  ]
}
"""

_OP_ALLOT = """OPERATION — ALLOT (SELECT)
You are handed the {lane} bites already on file (name + contract only) and a new
chunk. For EACH post, decide — in full view of every existing premise — which bites
it instantiates, and whether it carries a premise not yet on file. Do NOT write
receipts here; this is allotment only.
  . A post instantiates a bite when it satisfies that bite's invariant premise, by
    ANY execution. Allot it there — never spawn a near-duplicate of an existing bite.
  . A post may instantiate more than one bite.
  . Flag a NEW bite only when the post's premise matches no existing contract. Give
    it a one-line overview-altitude premise, judged against what already exists, so
    it is genuinely a different move and not an execution of a bite on file.
  . CONSOLIDATE NEW BITES ACROSS THE CHUNK. Before flagging a new bite, check the new
    bites you have already flagged for OTHER posts in this chunk. If two posts share
    the same move at overview altitude — e.g. a drone destination reveal of a beach
    and one of a mountain are both "drone destination reveal" — flag it ONCE under
    one working_name and list that same working_name on every post that runs it.
    Never emit two new bites that differ only by the subject they depict or who
    performs. When unsure whether a post is a new move or a wider execution of one
    you have flagged, prefer the wider one.

ALSO GIVEN:
existing_bites[] : { name, contract }   // {lane} bites on file; no receipts

OUTPUT — return ONLY this JSON. First char "{", last "}". No prose, no markdown.
{
  "lens": "{lane}", "stage": "allot", "handle": "",
  "post_names": { "P01": "" },
  "allotments": [
    {
      "post": "minted name (P01)",
      "matched": ["existing_bite_name", "..."],
      "new": [ { "working_name": "snake_case_overview", "premise": "one line" } ]
    }
  ]
}
"""

_OP_WRITE = """OPERATION — WRITE (RECEIPTS)
You are handed the allotment for this chunk: each post, the existing {lane} bites it
was allotted to (with their contracts), and any new premises flagged for it. Author
what goes into the {lane} feeder and bite files.
  . For each MATCHED bite: write the receipt — how_it_shows_up (execution specifics +
    variant axis), weight (the removal test), role_in_bite. Do NOT alter the bite's
    contract; you are adding one execution to an existing premise.
  . For each NEW premise: confirm it is not an execution of an existing bite, then
    mint the overview-altitude contract and its first receipt.

ALSO GIVEN:
allotments[] : { post, matched: [ { name, contract } ], new: [ { working_name, premise } ] }

OUTPUT — return ONLY this JSON. First char "{", last "}". No prose, no markdown.
{
  "lens": "{lane}", "stage": "write", "handle": "",
  "post_names": { "P01": "" },
  "bites": [
    {
      "name": "existing_or_new_bite_name",
      "status": "matched | new",
      "contract": { "1": "echo for matched; mint at overview altitude for new" },
      "receipts": [
        {
          "post": "minted name (P01)", "date": "",
          "rank_context": { "current": "", "overall": "", "read": "" },
          "anomalies": ["only if present for this post"],
          "weight": "core | supporting | standby",
          "how_it_shows_up": "timestamped, execution specifics, variant axis",
          "role_in_bite": "what it did; argues the weight"
        }
      ]
    }
  ]
}
"""


def _assemble(lane: str, op: str) -> str:
    d = _LANE_DEFS[lane]
    head = d["def"] + "\n\n" + _OVERVIEW_RULE
    if op == "cold_start":
        body = "\n" + _WEIGHT.replace("{core_line}", d["core_line"]) + "\n" + _RECEIPT_LAWS + "\n" + _CAPS + "\n" + _GIVEN + "\n" + _OP_COLD_START
    elif op == "allot":
        # allot is selection only — no weight ladder, no receipt laws
        body = "\n" + _GIVEN + "\n" + _OP_ALLOT
    elif op == "write":
        body = "\n" + _WEIGHT.replace("{core_line}", d["core_line"]) + "\n" + _RECEIPT_LAWS + "\n" + _CAPS + "\n" + _GIVEN + "\n" + _OP_WRITE
    else:
        raise ValueError(op)
    return (head + "\n\n" + body).replace("{lane}", lane)


# registry: BITE_PROMPTS[lane][op] -> system prompt (with {handle} placeholder)
BITE_PROMPTS = {
    lane: {op: _assemble(lane, op) for op in ("cold_start", "allot", "write")}
    for lane in _LANE_DEFS
}
BITE_PROMPT_VERSIONS = {
    lane: {op: f"bite_{lane}_{op}_v1" for op in ("cold_start", "allot", "write")}
    for lane in _LANE_DEFS
}
