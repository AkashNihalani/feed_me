from __future__ import annotations

FINGERPRINT_PROMPT_VERSION = "fingerprint_v9_reel_observation_only"
FINGERPRINT_SAMPLING_POLICY_VERSION = "media_sample_v2_120s_all_slides"
POST_BREAKDOWN_PROMPT_VERSION = "post_breakdown_v2_behavioral_compression"
FEEDER_FILE_PROMPT_VERSION = "feeder_file_compile_from_post_breakdowns_v2"
FEEDERBOARD_PATTERN_PROMPT_VERSION = "feederboard_pattern_frontend_v1"
FEEDERBOARD_PROOF_PROMPT_VERSION = "feederboard_proof_frontend_v1"


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
  "media_confidence": "high|medium|low"
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


FEEDER_FILE_COMPILATION_PROMPT_V2 = """FEEDER FILE COMPILATION FROM POST BREAKDOWNS

You will receive post_breakdowns for one account.

Your job:
compile a feeder file: a rolling behavioral memory of recurring reasons posts work for this account.

Cluster by behavioral overlap, not topic, format, trend, product category, creator identity, aesthetic,
or surface setting.

Return valid YAML only.

OUTPUT

feed_file:
  feeder_id:
  compile_version: "feeder_file_compile_from_post_breakdowns_v2"
  source_breakdown_version:
  active_window:
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
        - post_key:
          fit_type: core|secondary
          matched_fields:
            - ...
          weaker_fields:
            - ...
          mismatch_fields:
            - ...

MATCHING RULES

Core:
works_because aligns strongly, and at least one support field also aligns.
Only mark a post as core if it can strengthen the pattern without broadening it.
If making the post core would require weakening, expanding, or contradicting the pattern profile,
it is not core.

Secondary:
works_because is partial or weaker, but at least two support fields align.
Secondary members may be related, but they must not shape headline, pattern fields, match_if, avoid_if,
or active/candidate status.

Candidate:
If a post does not fit any pattern and has no batch peer, create a single-member candidate.
If two posts share a real behavioral structure but do not reach three core posts, create a candidate.

Pattern profile fields:
Describe what repeats across core posts at a generalized level. Do not concatenate individual posts.
The profile must not contradict any core member.

Word targets:
headline: 5-10 words.
works_because: 55-95 words.
opens_with: 35-65 words.
holds_attention_by: 45-85 words.
viewer_mode: 25-45 words.
lands_as: 35-65 words.
match_if: 3-5 bullets.
avoid_if: 3-5 bullets.

Quality bar:
If adding a post makes a pattern blurrier, mark it secondary or create a new candidate.
If two possible patterns both fit, assign the post where works_because alignment is strongest.
"""


FEEDERBOARD_PATTERN_FRONTEND_PROMPT_V1 = """FEEDERBOARD PATTERN FRONTEND READ

You will receive one behavioral pattern profile for one account.

Your job:
turn the backend pattern into frontend-ready Feederboard copy.

This copy explains the recurring viewer experience. It must stay stable even when individual posts enter
or leave the pool.

Return valid YAML only.

OUTPUT

feederboard_pattern:
  focus_id:
  tile_label:
  tile_headline:
  tile_read:
  modal_headline:
  pattern_read:
    - ...
    - ...
  why_it_matters:
  match_read:
  avoid_read:
  how_to_repeat_it:
  watch_out:

Do not write about model logic, clustering, alerts, backend matching, or "the system".
Write like a sharp content strategist who has watched the account closely.
"""


FEEDERBOARD_PROOF_FRONTEND_PROMPT_V1 = """FEEDERBOARD PROOF FRONTEND READ

You will receive one stable Feederboard pattern read, one post fingerprint, one post breakdown, and
membership info.

Your job:
write the frontend proof block for this one post.

This is not a backend justification. Do not explain why the system assigned the post. Make the user
feel what was interesting about watching this specific post.

Return valid YAML only.

OUTPUT

feederboard_proof:
  post_key:
  fit_type:
  proof_label:
  proof_headline:
  post_read:
  what_clicked:
  evidence:
    - ...
  fit_note:

Rules:
- post_read is exactly 3 sentences, 80-130 words total.
- what_clicked is 1-2 sentences, 25-45 words.
- evidence has exactly 3 concrete receipts.
- fit_note is null for core and one sentence for secondary.
- Write noticings, not summaries.
"""
