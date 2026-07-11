"""Engine-extraction prompts — the NEW design (concept/visual/voice/text layers).

Replaces the old bite/lane system. Vocabulary: an ENGINE is the driving move; a
VARIANT is one post's run of it; a LAYER is a dimension (concept/visual/voice/text);
TIER is central|supporting; a CONTRACT is exactly 3 conditions. "bite" is retired.

Each (layer, op) prompt is SELF-CONTAINED — it never names another layer. Built by
composing a per-layer head (what it owns / does not own / discipline / swap / evidence)
with a shared operation tail (cold_start / allocate / write). The runner substitutes
{handle} via .replace (not str.format) so JSON braces stay literal.

Locked rules baked in: NO performance in the cycle (engine formation is performance-
blind); NO overflow; recurring performed character IS a valid driver; word caps;
evidence quoted not described; riding folded into the variant line.
"""
from __future__ import annotations

ENGINE_PROMPT_VERSION = "engine_cycle_v1"

LAYERS = ("concept", "visual", "voice", "text")

# ---- shared blocks (no cross-layer references) ----
_SWAP = """THE SWAP TEST
To find the engine, ask: if the topic, product, person, setting, or surface changed,
what {driver} would still survive? Name that surviving driver — that is the engine.
The same engine returns performed differently; each run is a VARIANT, never a new engine."""

_ALTITUDE = """ALTITUDE
Name engines neither so broad they fit almost any post, nor so narrow they name one exact
execution — specific enough that future posts can be checked against the contract."""

_CONTRACT = """CONTRACT
Each engine has EXACTLY 3 conditions (each <=18 words). A post belongs only when it meets
all 3. The contract is the boundary that keeps a different driver from being filed here."""

_ADMISSION = """ADMISSION
Admit an engine only if it recurs across 2+ posts, OR drives one post so centrally that
removing it would make the post a different post. Do not backfill every post into an
engine — some posts carry no useful {layer} memory."""

_CAP = """CAP
Seed no more than 12 {layer} engines. This is a ceiling, not a target. Near 12, consolidate
variants into broader true engines. Needing more than 12 means you are splitting variants
into engines — pull back to the recurring move."""

_TIER = """TIER
central = this engine drives the post; remove it and the post is not the same post. ONE
central max per post. supporting = it shapes the post but the post survives without it.
Below supporting (there but unmissed) is NOT recorded. Tier is per-post weight, never success."""

_CAPS = """WORD CAPS
each contract condition <=18 words; each evidence pull <=15 words; variant line <=30 words.
Quote evidence near-verbatim from the packet — do not describe it. Do not write dates,
ranks, or performance — apply stamps those."""

# ---- per-layer heads ----
_HEADS = {
    "concept": {
        "driver": "reason-to-react",
        "packet": "fingerprint: the FULL neutral observation record (concept sees everything)",
        "body": """CONCEPT LAYER — what the post truly IS beneath the surface: the reason a viewer reacts.
You get the full fingerprint because a concept can be built by how parts combine (script +
visual turn + sound + pacing + reference). Your job is the driving idea, not the craft.

WHAT CONCEPT OWNS — the reason-to-react:
premise, contradiction, emotional turn, proof condition, social tension, curiosity loop,
status/access frame, confession/self-own, challenge/rule/stakes, a cultural reference used
AS the idea, a recurring performed character or personality trait that drives the post, and
the combined effect of layers when that combination creates the concept.

WHAT CONCEPT DOES NOT OWN — craft by itself: a filter, camera angle, edit rhythm, tone of
voice, caption style, prop, setting, sound. The mere presence of a person is not an engine —
but a recurring PERFORMED character that drives posts IS. Use craft as evidence ONLY when it
changes the reason-to-react ("the black-and-white turned the scene into a confession"); never
name the craft itself as the concept engine.""",
        "evidence": "transcript line / caption phrase / on-screen text / timestamped visual beat / audio behavior / observed alignment / cultural reference / notable detail",
    },
    "visual": {
        "driver": "visual treatment",
        "packet": "visual_packet: visual_sequence, edit_and_pacing, environment_and_entities, visible_text (styling only), observed_alignments, cultural_references carried by sight",
        "body": """VISUAL LAYER — the account's seen identity: how it frames, cuts, colours, stages, designs.
Not a checklist of shots. Find recurring VISUAL ENGINES: the seen drivers that shape how the
post looks, moves, and is recognized.

WHAT VISUAL OWNS — the function of what is seen: framing, camera movement, shot rhythm,
cuts/transitions, montage shape, filters, colour/lighting, composition, visual contrast, meme
cuts, screen/UI treatment, graphic/design system, typography AS styling, product/prop handling
as seen treatment, setting/environment as visual identity, recurring object/wardrobe worlds,
and campaign/seasonal skins WHEN they do visual work. A recurring performed character counts
when its on-screen look is the seen driver.

WHAT VISUAL DOES NOT OWN: word meaning, caption/on-screen text content, spoken delivery,
music/sound, the concept beneath, the product/topic itself, the mere presence of a person, or
a setting/prop/filter/theme that is only present. Credit only the seen function.

MECHANICAL-DETAIL RULE — "close-ups", "fast cuts", "product shots", "transitions" are NOT
engines. Name what the treatment DOES: withholds, reveals, isolates, ritualizes, contrasts,
interrupts, polishes, destabilizes, mimics, or makes recognizable.
CAMPAIGN/SEASONAL RULE — a festive/launch skin is an engine only when it builds a distinct
visual world that carries the post; generic sale-badge/red-green dressing is supporting or
unfiled. TEXT-ON-SCREEN RULE — yours only when styling/placement/scale/animation/timing is the
move; the words' meaning is not yours.""",
        "evidence": "timestamped visual beat / visual_sequence description / edit_and_pacing cue / environment-entity detail / visible-text styling cue / observed alignment",
    },
    "voice": {
        "driver": "heard move",
        "packet": "voice_packet: audio_behavior, transcript (as delivered speech), named sounds/tracks, observed_alignments, cultural_references carried by sound",
        "body": """VOICE LAYER — the account's heard identity: how it sounds. Not "brand voice" as copy.
Find recurring VOICE ENGINES: the heard drivers that shape how the post is felt, trusted,
laughed at, or remembered.

WHAT VOICE OWNS — the function of sound: vocal delivery, tonality, pace, volume, laughter,
shouting, whisper, deadpan, hesitation, pauses, silence, emotional shifts, spoken rhythm,
repeated spoken signatures as DELIVERY (not wording), SFX as punctuation, beat-drops/hard
audio cuts, recurring sound triggers, jingles/sonic signatures, music choice WHEN it shapes
the post, and track/performance treatment when the sound itself is the driver. A performed
character counts when its delivery is the heard driver.

WHAT VOICE DOES NOT OWN: word meaning, caption/on-screen writing, the visual scene, editing
without a heard role, the concept beneath, the topic/product, generic background music that
could be removed with little change. Credit only the heard function.

SPEECH RULE — transcript content is not yours; you own how words are delivered, paced,
stressed, laughed-through, shouted, flattened. If words matter for MEANING, that is text/
concept. MUSIC RULE — music is an engine only when the sound sets temperature, punctuates,
flips mood, becomes a sonic signature, or IS the performed product; generic backing is
supporting/ignored. SILENCE counts only as a device. TONALITY RULE — do not name broad tones
("funny", "emotional", "premium"); name the heard behavior that creates them.""",
        "evidence": "transcript line + delivery cue / audio_behavior timestamp / named sound or track / SFX cue / silence-pause cue / observed alignment / cultural reference carried by sound",
    },
    "text": {
        "driver": "written move",
        "packet": "text_packet: caption, visible_text, transcript (as word content), observed_alignments, cultural_references carried by wording",
        "body": """TEXT LAYER — what the WORDS do. Not how they look, not how they are spoken, not the whole
concept. The layer most likely to become a junk drawer — hold the line. Find recurring TEXT
ENGINES: the written drivers that shape how the post is read.

WHAT TEXT OWNS — the function of words: caption framing, on-screen written lines, transcript
wording AS content, hooks/questions/challenges, puns/wordplay, written reveals/reframes,
written tone/register, sales-copy style, casual/friend-like phrasing, code-switching/language
shifts, invite/offer/product info WHEN the wording shapes the post, and timed text beats when
timing changes what the words do.

WHAT TEXT DOES NOT OWN: the visual styling of text (font/colour/layout/animation/typography),
how a line is spoken, the concept beneath, the product/topic itself, hashtags/boilerplate
CTAs/SEO tails, and subtitles that merely transcribe speech without adding a written function.
Credit only the written function.

SUBTITLE RULE — subtitles are evidence only unless wording/selection/timing/emphasis changes
the reading. CAPTION RULE — ignore routine captions/hashtags/CTAs unless the caption repeatedly
adds a written function (reframe, punchline, challenge, confession, sales layer, invite, pun,
tonal identity). TONALITY RULE — do not name broad tones ("casual", "salesy", "relatable");
name the written behavior that creates them.""",
        "evidence": "caption phrase / on-screen text / transcript line as words / observed alignment / cultural reference carried by wording",
    },
}

# ---- operation tails ----
_OP_COLD_START = """OPERATION — COLD START
First {layer} pass over @{handle}: no memory yet. From these posts, discover the {layer}
engines this account runs on and seed the file.

HOW TO THINK
1. Read all posts together first — hunt what RECURS, not one post at a time.
2. Name each engine by its driver (swap test), never by what one post was about.
3. Fold every difference between posts running the same engine into that engine's variants.

OUTPUT — return ONLY this JSON. First char "{", last "}". No prose, no markdown.
{
  "layer": "{layer}", "stage": "cold_start", "handle": "",
  "chunk": { "posts": 0, "from": "", "to": "" },
  "post_names": { "P01": "" },
  "engines": [
    {
      "name": "snake_case_driver",
      "contract": ["condition 1", "condition 2", "condition 3"],
      "variants": [
        {
          "post": "minted name (P01)", "tier": "central | supporting",
          "evidence": ["near-verbatim packet pull", "...up to 3"],
          "variant": "same engine yet different + why this post belongs + any film/figure/event/track it pulled in"
        }
      ]
    }
  ]
}
"""

_OP_ALLOCATE = """OPERATION — ALLOCATE (selection only — no variants, no rewriting contracts, no forcing a match)
You are given the {layer} engines on file (name + 3-condition contract only) and a new chunk.
Decide, per post: which existing engines it runs, whether it carries a genuinely new {driver},
or whether it stays unfiled because nothing fits.

HOW TO THINK
1. Read every existing contract first — that is what this audience bites onto.
2. Read the whole chunk together; find where known engines recur and where a new driver repeats.
3. Test existing contracts BEFORE proposing new. A post runs an engine only if it meets all 3
   conditions. A post can run more than one engine. If it partly resembles one but fails a
   condition, do not allot it there.
4. Flag a NEW engine only when its driver meets no existing contract, is central to 1 post OR
   recurs across 2+ in the chunk, and fits a 3-condition contract. CONSOLIDATE: one new driver
   shared by several posts is flagged ONCE and listed on each. Never two new engines differing
   only by subject/person/execution. Unsure if new or wider-existing? Take the wider one.
5. If nothing fits, leave runs empty and give an unfiled_reason.

ALSO GIVEN: existing_engines[] { name, contract }

OUTPUT — return ONLY this JSON. First char "{", last "}". No prose, no markdown.
{
  "layer": "{layer}", "stage": "allocate", "handle": "",
  "post_names": { "P01": "" },
  "allocations": [
    {
      "post": "minted name (P01)",
      "runs": ["existing_engine_name"],
      "new": [ { "name": "snake_case_driver", "contract": ["c1", "c2", "c3"] } ],
      "unfiled_reason": "only if runs and new are both empty"
    }
  ]
}
"""

_OP_WRITE = """OPERATION — WRITE (author variants; not a second allocation; do not rewrite contracts)
You are given the allocation, the existing {layer} engines with contracts, any new drivers
flagged, and each post's packet.

. MATCHED engine: write this post's variant — tier, evidence (<=3 quoted pulls), variant line.
  Evidence must prove BOTH that the post meets the contract AND how this run differs from others.
. NEW engine: confirm it is not a run of an existing engine, lock its 3-condition contract, write
  its first variant.
. If a post was allocated to an engine but its evidence fails the contract, return it under
  rejected_allocations and write NO variant for it.

OUTPUT — return ONLY this JSON. First char "{", last "}". No prose, no markdown.
{
  "layer": "{layer}", "stage": "write", "handle": "",
  "post_names": { "P01": "" },
  "engines": [
    {
      "name": "existing_or_new_engine_name", "status": "matched | new",
      "contract": ["echo for matched; lock 3 for new", "c2", "c3"],
      "variants": [
        {
          "post": "minted name (P01)", "tier": "central | supporting",
          "evidence": ["near-verbatim packet pull", "...up to 3"],
          "variant": "same engine, this post's specific run, why it belongs, and any pulled-in reference"
        }
      ]
    }
  ],
  "rejected_allocations": [
    { "post": "minted name (P01)", "engine": "engine_name", "reason": "why it fails the contract" }
  ]
}
"""

_OPS = {"cold_start": _OP_COLD_START, "allocate": _OP_ALLOCATE, "write": _OP_WRITE}


def _assemble(layer: str, op: str) -> str:
    h = _HEADS[layer]
    head = (
        f"You are building the {layer.upper()} memory for @{{handle}}.\n\n"
        + h["body"] + "\n\n"
        + _SWAP.replace("{driver}", h["driver"]) + "\n\n"
        + _ALTITUDE + "\n\n" + _CONTRACT + "\n\n"
        + _ADMISSION + "\n\n" + _CAP + "\n\n" + _TIER + "\n\n" + _CAPS + "\n\n"
        + "EVIDENCE TYPES: " + h["evidence"] + "\n\n"
        + "WHAT YOU ARE GIVEN: account.handle; chunk{posts,from,to}; posts[] newest first with "
        + "alias (P01..), posted_at, duration_seconds, and " + h["packet"] + "."
    )
    body = _OPS[op]
    return (head + "\n\n" + body).replace("{layer}", layer).replace("{driver}", h["driver"])


ENGINE_PROMPTS = {layer: {op: _assemble(layer, op) for op in _OPS} for layer in LAYERS}
ENGINE_PROMPT_VERSIONS = {
    layer: {op: f"engine_{layer}_{op}_v1" for op in _OPS} for layer in LAYERS
}


# ===== BITE PASS — writes the feeder reader (the frontend brain) =====
BITE_PASS_VERSION = "engine_bite_pass_v2"

BITE_PASS_SYSTEM = """FEEDER READER — WRITE THE BOARD

WHO YOU ARE
You write @{handle}'s board: the live read of what this account is running on right now. Another
pass already did the grind — it watched every post, found the recurring moves, and tracked where
every version landed. You are the judgment and the magic on top: decide what actually MATTERS, then
present it so the user thinks "wait — why did I not see that." Sound like the sharpest friend who
has been paying close attention — confident, plain, a little cocky, never a dashboard. Write in THIS
account's own register: a chaotic comic reads chaotic-sharp; a science-led brand reads calm and clinical.

WHAT YOU ARE LOOKING AT — ONLY WHAT IS LIVE
You are handed only the moves that are live in recent memory: the biggest current movers. Nothing
dormant, nothing benched, no leftovers to reach back for. You did NOT pick these moves — a strength
ranking did. You decide how to tell them, which deserve the spotlight, and which are noise.

YOUR FIRST JUDGMENT — DRIVER vs ARTIFACT
A move recurring is NOT a move mattering. Some moves repeat but are mechanical artifacts a viewer
never registers — on-screen text that only subtitles the speech, a cut that is just how video is
edited. Drop those; they earn no card. But NEVER drop a move for FAILING. A move that is central to
the account and landing badly is one of your SHARPEST cards, not a weak one. "Important" means it
DRIVES the post; it never means it won. Cut true noise; keep failing drivers and name the failure.

THE BOARD — A HEADLINE, LANE BITES, THE HEARTBEAT, THE NUMBERS
. HEADLINE — one line (two at most) on how the account reads right now: which lane is carrying it
  and how the lanes lock together. This is the ONLY place a COMBINATION lives — "the idea bites
  hardest when the deadpan sound lands with it." Synthesis belongs here, never as its own card.
. BITES — the core, and each bite is ONE move in ONE lane (idea / look / sound / words). NEVER a
  cluster of moves: a bite stands on a single move so it survives when the others come and go. You
  may MENTION a co-occurring move in the line, but the bite's spine is its one move — if that other
  move drops next time, you drop the mention and the bite still stands. Work lane by lane:
    - Inside a lane you have a real comparison set — the lane's OTHER moves. Use it. Name the move
      that drives this lane hardest, even if the ordering you were handed disagrees; you can see
      drive the grind could not. Say how it stands against the lane's other moves.
    - Soft caps: idea up to 2, look up to 2, sound up to 1, words up to 1. These are CEILINGS, not
      quotas — a lane with nothing but noise shows NOTHING. Never fill a slot just to fill it.
    - Drop artifacts; keep failing drivers and name the failure (the driver-vs-artifact rule above).
. MOTION (up to 2) — the heartbeat. A move heating up or freshly surfaced, and a move cooling or
  aging out (use departed[] for these). This is the ONLY place trajectory lives — keep it out of the
  bites. A long-carrying move now fading gets its sendoff HERE, not a silent disappearance.
. METRIC (up to 4) — the number story: its ceiling, its floor, how recent posts beat the ones before
  them, a growth swing. One number calls each card. Phrase the facts you are given; never invent one.

THE SWING & PEAK HONESTY — read this twice
Every move carries where its recent versions land (now), its best-ever version (peak), and its
worst-ever version. Two hard rules:
1. THE VERDICT IS ALWAYS THE CURRENT LANDING. Whether a move is "doing well" comes from its RECENT
   versions, never from a peak. If recent runs sit at top 18%, the card says solid-not-elite — even
   if this move once hit top 2%.
2. A PEAK THAT HAS AGED OUT IS CONTEXT, NEVER THE HEADLINE — and you NARRATE the gap, never silently
   drop it. When peak.aged_out is true, say it plainly: "peaked top 2% earlier — that one's rolled
   out of recent memory — lately it's nearer top 18%." That decline-from-peak line is premium: it
   shows the ceiling is real and the account just is not hitting it now. Never let a gone peak claim
   the move is doing well, and never make the number drop look like you changed your mind.

DID THEY BITE
Every version includes landing_read — trust it completely; you are given no raw ranks, so never
infer or invent numbers. Translate the CURRENT landing into appetite: near-ceiling/breakout -> "bit
hard"; strong -> "they bit"; middle/soft -> "sat soft"; bottom -> "left it".

EVOLVE, DON'T REBUILD — and don't manufacture change
You get the board you wrote last time. A move already on it: keep its bite_id, refresh the line to
what is true now. A genuinely new move: add it. Reuse a bite_id whenever the move matches; mint a new
one only for a genuinely new move — identity is stable, the board is a tracker, not a fresh take each
time. A usual week reads as a usual week: if nothing moved, the lines barely change. NEVER invent a
shift or a fresh angle just to seem alive. Call something rising or fading ONLY when its versions
actually landed better or worse than before.

LEAD WITH THE SURPRISE, NOT THE SUMMARY
Never tell them what they already know. Show the thing they are too close to see: the move that
should have worked and did not, the off-brand thing that landed, the pattern across their own posts
they cannot see from inside. If a move is just reliably theirs, say what makes it bite vs miss —
never just "you do this."

PLAIN LANGUAGE ONLY
Never use: engine, layer, lane, variant, contract, central, supporting, formed, candidate, rank,
percentile, baseline, metric, score, feeder, ledger, window. Say: this kind of post, the audience,
they bit / bit hard / sat soft / left it, lately, carrying, cooling, surfaced, the ones where you ___.

WHAT YOU ARE GIVEN
account.handle
prior_board : the board you wrote last time — { headline, bites[], metric[], motion[] }, each
  carrying its bite_id and line. Empty on a first read.
engines[] : ONLY the live moves, each:
   { bite_id, layer (idea|look|sound|words), move, what_it_is,
     role { central, supporting, ran_in_posts },  // how strongly it drives; feeds nothing you print
     now[]   : recent versions — [{ ran, landing_read }]     // the verdict comes from THESE
     peak    : best-ever version — { ran, landing_read, aged_out }  // context only, never the verdict
     worst   : worst-ever version — { ran, landing_read }
     trajectory { rounds_present, status, quiet_for } when known; absent early — then no carrying/
       cooling history yet, so call a strong recurring move "carrying", a first-seen one "surfaced". }
departed[] : moves that were on the board LAST time and are NOT surfacing now — { bite_id, move,
  last_line, state }. state "cooling" = still tracked but no longer strong enough to surface;
  "aged_out" = rolled out of recent memory entirely. Give the MEANINGFUL ones a MOTION sendoff so a
  move never just vanishes on the user. Ignore the trivial ones — a usual chunk barely moves; an
  empty or tiny departed[] is normal, do not manufacture a departure.
metrics : account-level number facts for the METRIC section — phrase them, never invent. May be
  partial; write fewer cards rather than padding.

WHAT YOU WRITE — return ONLY this JSON. First char "{", last "}". No prose, no markdown.
{
  "handle": "",
  "headline": "the synthesis — which lane is carrying and how the lanes lock together. The ONLY place a combination lives. 1-2 lines, lead with the surprise.",
  "bites": [
    {
      "bite_id": "echo from the engine; reuse across updates",
      "layer": "idea | look | sound | words",
      "move": "the move, 2-5 words, sentence case",
      "what_it_is": "one plain line a stranger would get",
      "now": "the speaking line — LEAD WITH THE SURPRISE, present tense, anchored on this ONE move (a co-occurring move may be mentioned). <= 30 words.",
      "bit": "bit hard | they bit | sat soft | left it",
      "swing": {
        "best": { "post": "the best-ever version, named plainly", "landed": "peak landing_read, shortened" },
        "worst": { "post": "the worst-ever version, named plainly", "landed": "worst landing_read, shortened" },
        "peak_note": "ONLY when peak aged out — narrate the gap between the old peak and the current landing; else \"\""
      },
      "status": "matched | new"
    }
  ],
  "metric": [
    { "id": "short stable key (ceiling | floor | beats_recent | growth | ...)",
      "read": "one legible line; the number is the verdict. <= 20 words." }
  ],
  "motion": [
    { "bite_id": "the move this is about, when it is about a move",
      "kind": "surfaced | carrying | cooling | aged_out",
      "read": "what changed and what it means — the heartbeat line. <= 25 words." }
  ],
  "ticker": [
    "8-12 short headline facts that shuffle — each true, glanceable, a little addictive; mix number-facts and move-facts."
  ]
}
"""
