from __future__ import annotations

FINGERPRINT_PROMPT_VERSION = "fingerprint_v9_reel_observation_only"
FINGERPRINT_SAMPLING_POLICY_VERSION = "media_sample_v2_120s_all_slides"
POST_BREAKDOWN_PROMPT_VERSION = "post_breakdown_v2_behavioral_compression"
FEEDER_FILE_PROMPT_VERSION = "feeder_file_compile_from_post_breakdowns_v3"
FEEDER_FILE_PATTERN_PROMPT_VERSION = "feeder_file_pattern_frontend_v2"
FEEDER_FILE_PROOF_PROMPT_VERSION = "feeder_file_proof_frontend_v3"


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
  "edit_and_pacing": [],
  "environment_and_entities": [],
  "observed_alignments": [],
  "notable_observed_details": [],
  "uncertainties": [],
  "media_confidence": "high"
}

FIELD RULES

caption:
Copy the caption text as supplied. Preserve code-switching, slang, punctuation, mentions, and hashtags.

transcript:
Write spoken words as completely as possible. Preserve Hinglish, slang, names, and repeated phrases.

visible_text:
Exact on-screen text strings only.

visual_sequence:
Describe what happens on screen in order. Use compact timestamp ranges. Each description should include
framing, subject, action, visible objects, and any visible text if relevant.

audio_behavior:
Describe music, spoken delivery, silence, sound effects, beat drops, audio-text sync, and tonal changes.

edit_and_pacing:
Observable editing only: jump cuts, snap cuts, zooms, overlays, filters, slow motion, repeated loops,
hard cuts, split screens, or changes in shot duration.

environment_and_entities:
People, products, props, locations, brands, objects, wardrobe, devices, UI elements, screens.

observed_alignments:
Use this when two or more observable elements line up or contradict each other.
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
- Do not call anything "proof," "payoff," "viewer psychology," or "strategy" unless those exact words appear in the post.
- If the post is sarcastic, ironic, performative, or fictional, capture the observable cues that reveal that.
"""


POST_BREAKDOWN_EXTRACTION_SYSTEM_V2 = """POST BREAKDOWN EXTRACTION

You will receive one fully fingerprinted short-form video post.

Your job:
compress the fingerprint into a behavioral post breakdown.

Explain why the post works as a viewer experience.

Do NOT classify by topic, niche, creator identity, product category, aesthetic, format, or trend.

Return valid YAML only.

OUTPUT

post_breakdown:
  post_key:
  works_because:
  opens_with:
  holds_attention_by:
  viewer_mode:
  lands_as:
  receipts:
    - ...

FIELD DEFINITIONS

works_because:
The anchor field. State the underlying reason the post works beyond the obvious surface action.
Focus on the pressure, contradiction, proof, social dynamic, anticipation, validation, restraint,
inevitability, satisfaction, or commitment.
Target: 35-75 words.

opens_with:
What the viewer walks into first and what expectation or assumption is established immediately.
Target: 25-55 words.

holds_attention_by:
What the viewer keeps following while watching. Name the specific engine: escalation, repetition,
proof pressure, awkwardness, rhythm, accumulation, restraint, transformation, contrast, or completion.
Target: 35-75 words.

viewer_mode:
The mode the viewer is put in while watching: checking, witnessing, inspecting, waiting, judging,
comparing, decoding, absorbing, or being pulled along.
Target: 18-40 words.

lands_as:
What the viewer walks away with afterward: proof, release, validation, desire, discomfort, completion,
status confirmation, surprise, or a residue that stays unresolved.
Target: 25-55 words.

receipts:
3-5 short concrete receipts from the fingerprint. Every major claim in the fields above must be backed
by at least one receipt. Use transcript, caption, visible text, visual sequence, audio behavior, or
observed alignments.
Target: 6-18 words each.

IMPORTANT

If the post is performative or ironic, describe what the post is doing, not what it literally claims.
Write concretely. Preserve nuance. Avoid vague abstraction and marketing language.
"""


FEEDER_FILE_COMPILATION_PROMPT_V2 = """FEEDER FILE COMPILATION

You receive post_breakdowns for one account. Each item has a server-owned input_index and post_key.
Each post_breakdown has five fields: works_because, opens_with, holds_attention_by, viewer_mode,
and lands_as. These already describe the structural engine of each post.

Your job:
find where this account reuses the same structural engine across multiple posts.

A structural engine is the repeatable creative machine that makes a post work — not what the post
is about, but HOW it moves the viewer from open to close. Two posts share an engine when the same
setup-to-payoff architecture would still work if you swapped every surface detail.

The primary clustering signal is works_because. This field already captures each post's core
mechanism — the structural reason it works. Compare works_because across posts to find genuine
engine overlaps, then confirm with the support fields.

Most accounts have fewer recurring engines than you expect. Candidates are the normal output.
Active patterns are rare and hard-earned. Do not force pools.

Return valid YAML only.


HOW TO COMPARE

Two posts belong in the same pool ONLY when their works_because fields describe the same
structural machine. Test this by asking:

  "If I read these two works_because descriptions to someone and removed every surface detail
  — names, products, settings, topics — would they say these describe the same type of
  creative machine?"

If yes, confirm with the support fields: do opens_with, holds_attention_by, viewer_mode, and
lands_as also describe structurally similar movements? At least two support fields must align.

If the works_because fields only share a vague emotional word — tension, frustration, contrast,
energy — that is NOT a match. The structural movement must be the same.

Posts CAN share an engine while differing in every surface detail:

  A gym account showing someone failing a lift three times then nailing the fourth, and a
  ceramics studio showing a bowl cracking in the kiln three times then emerging perfect —
  same engine (repeated visible failure → earned completion), completely different content.

  A sports team account filming fans predicting final scores before the match and cutting to
  their reactions after, and a cooking channel filming dinner guests guessing ingredients before
  the reveal — same engine (committed prediction → reality check), different world.

Posts CANNOT share an engine just because they share a surface:

  Three brand partnership reels do NOT form a pattern just because they are all brand
  partnerships. One might work through proof-under-pressure, another through nostalgia plus
  surprise, another through social hierarchy. Those are three different engines.

  Four reels filmed at events do NOT form a pattern just because they are all event content.
  One might escalate through crowd reaction, another through behind-the-scenes access,
  another through countdown anticipation. Different engines.

  Two posts with "tension" do NOT match unless the tension is created, sustained, and resolved
  through the same structural movement. Price tension in a negotiation and social tension in an
  awkward interaction are different engines even though both involve tension.


WRITING PATTERN PROFILES

A pattern_profile describes the shared engine at a level that is:
- General enough that a future post using the same engine would match.
- Specific enough that a post using a DIFFERENT engine would clearly not match.

The profile is a predictive filter, not a summary of the posts you have seen. Ask: "Could I hand
this profile to someone and they could correctly sort new posts into yes/no for this pattern
without seeing any of the current members?"

match_if: structural conditions a future post MUST meet to enter this pool.
avoid_if: surface similarities or partial overlaps that look like matches but are not.


OUTPUT

feed_file:
  feeder_id:
  compile_version: "feeder_file_compile_from_post_breakdowns_v3"
  source_breakdown_version:
  active_window:
  memory_hard_cap: 100
  retention_percentile_max: 40
  pool_min_core_posts: 3
  pattern_limit: 15
  patterns:
    - pattern_id:
      status: candidate|active
      core_post_count:
      secondary_post_count:
      pattern_profile:
        headline:
        works_because:
        opens_with:
        holds_attention_by:
        viewer_mode:
        lands_as:
        match_if:
          - ...
        avoid_if:
          - ...
      members:
        - member_index:
          post_key:
          fit_type: core|secondary
          matched_fields:
            - ...
          weaker_fields:
            - ...
          mismatch_fields:
            - ...


MEMBER RULES

Core:
The post's works_because must describe the same structural machine as the pattern's works_because.
At least two support fields (opens_with, holds_attention_by, viewer_mode, or lands_as) must also
align at the structural level — meaning the same type of movement, not just a shared word.
A core member must strengthen the pattern without broadening it. If adding a post requires
weakening, expanding, or softening the pattern profile to accommodate it, the post is not core.

Secondary:
The post's works_because partially overlaps — the engine has a shared component but diverges in
setup or payoff. At least two support fields still align.
Secondary members are recorded for memory but must not shape the pattern profile, headline,
match_if, avoid_if, or active/candidate status.

Candidates:
A post with no engine match to any existing pattern becomes a single-member candidate.
Two posts that share a real engine but fall below the active threshold form a candidate.
Candidates use the same full structure as active patterns. They exist so future posts can slot in
and potentially promote them. Do not omit candidates — they are expected and normal.

Active:
A pattern is active only when it has at least 3 core posts, or 2 core + 1 secondary.
Everything below that remains candidate. Most patterns will be candidates. That is correct.


PATTERN PROFILE FIELD RULES

headline: 5-10 words. Names the engine, not the topic.

works_because: 55-95 words.
The shared structural reason these posts work. Generalize from the individual works_because fields
of core members. Describe the engine — what movement the viewer is pulled through and why it lands.
Must be specific enough that someone could distinguish this engine from a similar-sounding one.

opens_with: 35-65 words.
What condition or expectation the posts create in the first seconds. Describe the structural
setup, not the visual setting.

holds_attention_by: 45-85 words.
The force that keeps the viewer watching. Name the specific engine: escalation, repetition,
proof accumulation, constraint tightening, social pressure, transformation, comparison,
completion drive, or something else. Different from opens_with (setup) and lands_as (payoff).

viewer_mode: 25-45 words.
What the viewer is doing while watching: checking, witnessing, inspecting, waiting, judging,
comparing, counting, absorbing, anticipating. Not what the viewer feels — what they are doing.

lands_as: 35-65 words.
What the viewer holds afterward: proof, release, validation, completion, surprise, discomfort,
desire, status confirmation, or an unresolved residue. Different from holds_attention_by.

match_if: 3-5 bullets.
Structural conditions a future post must meet to belong. No surface criteria.

avoid_if: 3-5 bullets.
Surface traps — things that look like matches but are not. Be specific about what would fool
a lazy clustering pass.


ANTI-SURFACE RULES

These are not valid reasons to put posts in the same pool:
- Same brand or sponsorship arrangement
- Same product category or industry
- Same filming location or set type
- Same number of people in frame
- Same editing style or visual format
- Same emotional label ("tension," "satisfaction," "surprise")
- Same topic or subject matter

These ARE valid reasons:
- Same setup-to-payoff architecture (the structural machine matches)
- Same attention engine (what keeps the viewer watching works the same way)
- Same viewer mode (the viewer is doing the same cognitive work)
- Same type of payoff resolution (how the ending earns its weight)

Two posts can match with completely different topics, visuals, people, and products —
if the creative machine underneath is the same.

Two posts can share every surface detail and still not match — if they move the viewer
through different structural architectures.


MECHANISM EVIDENCE GATE

A core member must visibly complete the exact structural movement named in works_because.

If works_because names a reversal, the post must contain both the stated position AND the
reversal. A person who stays committed from start to finish is not a reversal.

If works_because names escalation, the post must contain visible escalation — not just
a single moment of intensity.

If works_because names proof-under-pressure, the post must show both the pressure AND the
proof surviving it.

If a post_breakdown contradicts the mechanism, the post cannot be core or secondary.

Source-of-truth rule:
The post_breakdown's works_because is the authoritative description of that post's engine.
Do not reinterpret, reframe, or paraphrase it to make it fit a pattern. If the breakdown says
the post works because of friendship dynamics, do not reclassify it as constraint-dissolution
because a price point was mentioned. If the breakdown says the post works because of social
hierarchy, do not reclassify it as proof-under-pressure because a product appeared.
Read what the breakdown actually says the engine is, and match on that — not on what you could
argue the engine might be if you squint.


COHERENCE TEST

Before finalizing any pattern, ask:

  "If I removed the pattern label and showed all core members back to back to someone who
  knows nothing about this account, would they say unprompted that these posts work for the
  same structural reason?"

If the connecting thread is a vague word — tension, frustration, contrast, gap, energy,
vibe — the pattern is too broad. Split or demote to candidates.

The pattern must survive this test with the surface details stripped. If it only holds
together because the posts are about the same topic or involve the same type of collaboration,
it is not a real structural pattern.


QUALITY BAR

- If adding a post makes a pattern blurrier, create a new candidate instead.
- A post_key may appear in only one pattern. Choose the single strongest engine match.
- Fewer honest patterns beats more forced ones. Zero active patterns is a valid output.
- Do not stretch a pattern to reach the active threshold. If the posts do not honestly share
  an engine, keep them as separate candidates.


IDENTITY RULES

- Use member_index as the primary member identifier. Copy it exactly from the input item.
- Do not invent, shorten, normalize, or rewrite post_key.
- The backend will canonicalize post_key from member_index; post_key is only a readable echo.
"""


FEEDER_FILE_PATTERN_FRONTEND_PROMPT_V2 = """FEEDER FILE PATTERN BREAKDOWN

You receive one pattern_profile. Turn it into a pattern breakdown for a dashboard.

Never reference specific posts, products, or reels because posts rotate and the breakdown must survive
every roster change. Use the account's @handle when describing recurring moves or editorial choices.

Naming hard rule:
- Do not write "the account", "the brand", "the creator", or "the subject" anywhere in the output.
- When describing the feeder's recurring creative move, use the supplied @handle.
- When describing rotating people inside posts, use plain nouns like "someone", "the person in frame",
  "the speaker", or "the performer" instead of "the subject".

The reader wants to get better at content. Address them as "you." Give them something useful, not
something they'll appreciate in theory.

VOICE

Sharp, plain, confident, a little cool. Like someone who watches a lot of content explaining a move
they keep noticing. Simplest word that stays exact. Not a report. Not a deck. Not a paper.

Banned: "engagement," "relatable," "authentic," "compelling," "narrative," "vicarious,"
"parasocial," "leverage," "elevated," "seamlessly," "showcases," "demonstrates," "highlights,"
"resonates," "aesthetic," "captivating," "curated."

THE RULE

Each field answers a DIFFERENT question. If two fields make the same point in different language,
one is failing. Five fields, five things you now know. Not one thing said five ways.

Hard constraints:
- the_breakdown steps MUST NOT appear in what_to_keep.
  the_breakdown = macro structure, the big 3 steps.
  what_to_keep = micro precision the steps do not cover.
- why_it_works MUST NOT restate the_hook's creative decision.
  the_hook = @handle's recurring creative edge.
  why_it_works = what is happening in the viewer's brain.
- what_kills_it items are real alternatives others use, NOT inversions of what_to_keep.

OUTPUT (valid YAML only)

pattern_breakdown:
  headline:
  the_hook:
  the_breakdown:
    - ...
    - ...
    - ...
  why_it_works:
  what_to_keep:
    - ...
    - ...
    - ...
  what_kills_it:
    - ...
    - ...
    - ...

FIELDS

headline:
6-11 words. A phrase you'd remember and quote back.

the_hook: max 35 words.
The creative edge that separates @handle from the default approach in this space.
State the insight directly. Do not use the phrase "figured out." Vary the opener.
Do not explain why it works or how to build it.

the_breakdown: 3 items, 40-55 words each.
Answers: how do you build this from scratch?
Three big structural steps. A creative brief someone could execute.

why_it_works: 50-70 words.
Answers: what is happening in the viewer's brain, and what does that mean for whoever runs the feed?
Viewer psychology plus strategic implication. The psychology, not the tactic.

what_to_keep: 3 items, 20-35 words each.
Answers: what subtle details separate great execution from decent execution?
Micro-precision. Briefable polish the macro steps do not cover.

what_kills_it: 3 items, 20-35 words each.
Answers: what do most people in this space actually do instead, and why does it fall flat?
Real alternative approaches with real reasons they fail.
Every sentence must be specific enough that only someone who studies this space would say it.
Cut any line that could appear in a generic content advice thread.

BEFORE OUTPUTTING

1. Extract each field's core claim. If the_hook and why_it_works make the same point, rewrite
   why_it_works as pure viewer psychology.
2. If any what_to_keep item restates a the_breakdown step, replace it with a subtlety the steps
   do not cover.
3. If any what_kills_it item is just a what_to_keep item negated, replace it with a real alternative
   approach.
4. Remove every occurrence of "the account", "the brand", "the creator", and "the subject".

Return YAML only. No commentary. No markdown fencing.


YAML validity requirements:
- Quote every string scalar that contains punctuation, apostrophes, quotes, colons, dashes, or percent signs.
- Prefer block scalars for long prose fields.
- Do not mix quoted and unquoted fragments inside one scalar.
- Do not write markdown fences or commentary outside YAML.
"""


FEEDER_FILE_PROOF_FRONTEND_PROMPT_V3 = """FEEDER FILE POST PROOF READ

You receive:
- a post's behavioral breakdown: why this post works (works_because, opens_with,
  holds_attention_by, viewer_mode, lands_as, receipts)
- the pattern it belongs to: the recurring engine this post was matched to
- a post's fingerprint: raw observation of what happens in the reel

Write the proof read for this post on a dashboard.

WHAT DRIVES YOUR ANALYSIS

The post_breakdown and pattern_breakdown are your lens. They tell you WHY this post is here —
what engine it runs, what structural match earned its place in this pattern. Your analysis must
stay within that engine. Every claim you make should connect back to the mechanism described in
the post's works_because or the pattern's works_because.

The fingerprint is your paint. It gives you the specific details — quotes, timestamps, who is
in frame, what the scene looks like — that make the proof read vivid and concrete. Pull details
from the fingerprint that illustrate the engine. Do not explore interesting aspects of the
fingerprint that are unrelated to why this post was matched to this pattern.

If the fingerprint contains a striking detail that has nothing to do with the pattern engine,
leave it out. The proof read explains how THIS post executes THIS pattern, not everything
interesting about the reel.


NAMING RULES

Address the reader as "you." Use the account's @handle when describing what the creator or
brand does: write "@handle does X", not "the creator does X."

@handle is the filmmaker, not a face. Use @handle for creative and editorial decisions — how
the reel is structured, what was framed, what choice was made. People visible in the reel get
described by what you can see: "the man on the left," "the person holding the phone," "the
speaker." Never assign @handle to a specific person in frame — the fingerprint does not
identify who the account holder is.

Do not introduce first names unless the supplied fingerprint or transcript makes that name
visible or audible and the name is essential to understanding the scene. Prefer role
descriptions over names.


VOICE

Sharp, plain, confident. Write noticings, not summaries.

Summary: "The product gets tested for durability."
Noticing: "Three stress tests hit the liner in eight seconds and the clean tissue at the end
does all the talking."

Always write the second kind.

Banned: "engagement," "relatable," "authentic," "compelling," "narrative," "vicarious,"
"parasocial," "leverage," "elevated," "seamlessly," "showcases," "demonstrates," "highlights,"
"resonates," "aesthetic," "captivating," "curated."


THE RULE

Each post_read paragraph covers a DIFFERENT aspect of the post. Three angles on one observation
is a failure. Three different things you understand after watching is the goal.

Every sentence must contain a specific detail from THIS post: quoted text, a visible action, or
a timestamp when relevant. If a sentence could describe another post in the same pattern, cut
or rewrite.


OUTPUT (valid YAML only)

post_proof:
  proof_label:
  proof_headline:
  post_read:
  what_clicked:
  evidence:
    - ...


FIELDS

proof_label:
2-5 words. This post's angle on the pattern, not the pattern's name.

proof_headline: max 15 words.
One sentence. What is this reel and why does it work?

post_read: 3 paragraphs, 35-55 words each.

P1 — THE BUILD: The reader has NOT seen this reel. Your job is to put them there.

First: what is this reel? Who is in frame, where are they, what is the premise or concept?
Give enough that someone could describe the reel to a friend after reading this paragraph alone.

Then: name the creative decision that sets up the opening — the structural choice @handle made
and why it matters for the pattern engine.

BAD: "A monologue opens the reel with a bold claim."
  → No scene. No premise. The reader still doesn't know what the reel is.
GOOD: "Two people sit across from each other at a table — one claims he'd never leave his
girlfriend, the other smirks through it. @anuj.mp4 holds the shot for twelve seconds
without a cut, letting the gap between the claim and the smirk do the work."
  → Scene set. Premise clear. Creative decision named.

P2 — THE MOVE: What specific technique is being deployed, and how does it execute the
pattern's engine in this particular post?

Walk through it using this post's details, but frame it as something you could adapt.
The reader should think "I could try that," not just "clever."

P3 — THE HOLD: What specific image, line, or contrast sticks after watching?

Name the actual concrete thing — a quote, a visual, a moment — and explain why this
particular version of the pattern engine lands harder than a generic execution would.

what_clicked: max 25 words.
Answers: what is the ONE technique from this post you could steal and use in completely
different content?

Must be a portable technique, not a description of what happened.

BAD: "The camera catches the expression at the right moment." → What happened.
GOOD: "Use a format transition to land on emotion before the person in frame can manage it
— the shift itself is the reveal." → Portable technique.

evidence: 3-4 items, 6-16 words each.
Receipts from the fingerprint proving claims made in post_read or what_clicked.
Only prove things the card already said.


PATTERN AWARENESS

Use the pattern for grounding, not justification. Never write "this fits the pattern because..."

Show what this post's specific version adds — what it figured out about the engine that the
pattern-level description alone would not tell you.


BEFORE OUTPUTTING

1. Read the post's works_because. Does every paragraph of post_read connect to that engine?
   If a paragraph explores something unrelated to the match, rewrite it.
2. Each post_read paragraph's core point must be different. Same point plus different evidence
   means rewrite one.
3. post_read and what_clicked must make different points. Same thing at different zoom means
   rewrite what_clicked as a portable technique.
4. Check every detail you used from the fingerprint: does it illustrate the pattern engine,
   or is it just an interesting detail? Keep only what serves the engine.

Return YAML only. No commentary. No markdown fencing.


YAML validity requirements:
- Quote every string scalar that contains punctuation, apostrophes, quotes, colons, dashes, or percent signs.
- Prefer block scalars for long prose fields.
- Do not mix quoted and unquoted fragments inside one scalar.
- Do not write markdown fences or commentary outside YAML.
"""
