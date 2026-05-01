from __future__ import annotations

import hashlib
import json
import re
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from psycopg.rows import dict_row

from .config import (
    FOCUS_BRAIN_COMPILE_INTERVAL_SECONDS,
    FOCUS_COMPILER_MODEL,
    FOCUS_REBUILD_INTERVAL_DAYS,
    FOCUS_REBUILD_MODEL,
    FOCUS_V2_SLICES_ENABLED,
    GEMINI_API_KEY,
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
    SIGNAL_INTELLIGENCE_MODEL,
    SIGNAL_INTELLIGENCE_PROVIDER,
)

_OPENROUTER_CHAT_URL = "/chat/completions"
_GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
_DEFAULT_FLASH_MODEL = "google/gemini-3-flash-preview"
_FOCUS_READ_PROMPT_VERSION = "focus_read_v1"
_FEEDER_FOCUS_PROMPT_VERSION = "feeder_focus_v1"
_FEED_FOCUS_PROMPT_VERSION = "feed_focus_v1"
_FOCUS_SCHEMA_VERSION = "v2_shadow"
_STATS_BUILDER_VERSION = "stats_builder_v1"
_VALIDATOR_VERSION = "v1.4"
_FOCUS_V2_COMPILER_PROMPT_VERSION = "compiler_v3_voice_calibrated"
_EMPTY_MEDIA_SOURCE_HASH = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
_LLM_INPUT_SOFT_TOKEN_LIMIT = 8000
_MAX_EVIDENCE_POSTS = 160
_MAX_PATTERNS_PER_BUCKET = 5
_FEEDER_COMMON_WORD_LIMIT = 80
_FEEDER_FORMAT_WORD_LIMIT = 90
_FEED_COMMON_WORD_LIMIT = 60
_FEED_CAPSULE_WORD_LIMIT = 70
_PATTERN_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "by", "content", "for", "from", "in", "into", "is", "led",
    "of", "on", "or", "post", "posts", "reel", "reels", "the", "their", "to", "video", "videos",
    "with", "without",
}
_PATTERN_SYNONYMS = {
    "behind": "bts",
    "backstage": "bts",
    "process": "process",
    "factory": "factory",
    "founder": "founder",
    "human": "human",
    "humanized": "human",
    "person": "human",
    "people": "human",
    "staff": "staff",
    "worker": "staff",
    "workers": "staff",
    "employee": "staff",
    "employees": "staff",
    "proof": "proof",
    "testing": "testing",
    "test": "testing",
    "lab": "lab",
    "laboratory": "lab",
    "ingredient": "ingredient",
    "ingredients": "ingredient",
    "hygiene": "hygiene",
    "clean": "hygiene",
    "ugc": "ugc",
    "handheld": "handheld",
    "camera": "camera",
    "voiceover": "voiceover",
    "dialogue": "dialogue",
    "caption": "caption",
    "question": "question",
}

_FOCUS_READ_SYSTEM = """You add a narrow alignment read for a social post.
Do not judge performance. Do not say if the post is good, bad, emerging, risky, or strategic.
Compare the neutral fingerprint to the supplied feeder focus only.
Return JSON only:
{
  "relation_to_feeder_md": {
    "matches": [],
    "deviates": [],
    "unclear": []
  },
  "notes": []
}
Rules:
- "matches" are visible/caption traits that are already present in the feeder focus.
- "deviates" are visible/caption traits present in this post but not in the feeder focus.
- "unclear" is for things the fingerprint does not prove.
- Keep each item short and observable.
- Never mention private algorithm behavior, saves, shares, or unsupported facts."""

_FEEDER_COMPILER_SYSTEM = """You update a 90-day feeder/account rulebook for social media intelligence.
You receive the previous rulebook, Flash-compressed evidence_buckets, Stage B memory candidates, server stats, and metric windows.
Lower percentile is better: p10 beats p40, and p80 is weak.
server_pattern_stats is authoritative for last_seen_days, opportunities_since_seen, evidence counts, and trigger support. Use it instead of doing calendar math yourself.
Return JSON only:
{
  "structured_patterns": {
    "patterns": [
      {
        "pattern_id": "",
        "format": "reel|image|carousel|common",
        "summary": "",
        "status": "stable|strengthening|emerging|watchlist|weakening|decaying|archived",
        "canonical_terms": [],
        "evidence_count_90d": 0,
        "evidence_count_30d": 0,
        "evidence_count_7d": 0,
        "last_seen_days": 0,
        "opportunities_since_seen": 0,
        "media_post_count_30d": 0,
        "trigger_support": {
          "breakout": 0,
          "comment_spike": 0,
          "like_heavy": 0,
          "viral_passive": 0,
          "fade": 0,
          "anchor": 0,
          "cross": 0
        },
        "metric_note": "",
        "proof_post_keys": []
      }
    ]
  },
  "evidence_summary": {},
  "focus_md_common": "",
  "focus_md_reel": "",
  "focus_md_image": "",
  "focus_md_carousel": ""
}
Rules:
- Preserve nuance. Do not collapse "BTS + founder emotion + process proof" into just "founder-led".
- Reuse existing pattern IDs when the idea is semantically similar. Merge instead of creating duplicates.
- Hard caps after merging: max 5 stable, 5 strengthening/emerging, 5 watchlist, 5 avoid/weakening/decaying active patterns.
- 90-day repeated evidence is the rulebook; last 7 days is an update/watchlist unless repeated or numerically strong.
- Only claim current direction when metric_windows supports it. Cite numbers inline in the MD.
- Pattern decay is opportunity-based: 10 later posts of the same format=weakening, 18=decaying, 25=archived. Time only supports decay when the account has enough posting volume; 75d is always archived near rolling-window expiry.
- Full rebuild mode must clean aggressively: dedupe near-identical patterns, archive stale patterns, and remove repetitive prose.
- Keep each MD section compact enough to inject into another model: common <= 80 words; each format <= 90 words.
- Every action must name observable behaviors from evidence, not generic advice. Use concrete examples from fingerprints."""

_FEED_COMPILER_SYSTEM = """You update a 90-day feed/category rulebook for social media intelligence.
You receive the user's Focus intent, feeder rulebooks, feed metric windows, anchor comparisons, and recent signal candidates.
Lower percentile is better: p10 beats p40, and p80 is weak.
feeder_metric_windows contains per-feeder, per-format numbers. Use those for cross-account direction claims.
Return JSON only:
{
  "structured_patterns": {},
  "anchor_lens": {},
  "divergence": [],
  "feed_metrics": {},
  "proof_posts": {},
  "focus_md": "",
  "capsule_common": "",
  "capsule_reel": "",
  "capsule_image": "",
  "capsule_carousel": "",
  "capsule_anchor": "",
  "capsule_cross": ""
}
Rules:
- This is not a summary of all feeders. It is the cross-feed/category truth.
- Reuse existing pattern IDs when the idea is semantically similar. Merge instead of creating duplicates.
- Hard caps after merging: max 5 stable, 5 strengthening/emerging, 5 watchlist, 5 avoid/weakening/decaying active patterns per format.
- Preserve contradictions. If Reels rise feed-wide but one feeder still wins with carousels, say so.
- Track divergence explicitly, e.g. account_status=emerging, feed_status=strengthening, alignment=market_ahead_of_account.
- Current direction must cite metric window numbers from the input.
- If an anchor exists, include anchor-vs-feed gaps in anchor_lens and capsule_anchor.
- Capsules are small runtime snippets for a cheaper alert model. common <= 60 words, format/scope capsules <= 70 words each.
- Every "do next" idea must name observable behavior and concrete examples from fingerprints."""

_EVIDENCE_BUCKET_SYSTEM = """You compress a small bucket of social post fingerprints for a weekly memory compiler.
You receive no more than 10 posts from the same media type and nearby dates.
Do not invent. Preserve nuance, especially caption tone, visual style, human presence, proof, and emotional trigger.
Return JSON only:
{
  "bucket_summary": "",
  "strong_patterns": [],
  "weak_patterns": [],
  "caption_tone_notes": [],
  "visual_style_notes": [],
  "metric_notes": [],
  "proof_post_keys": []
}
Rules:
- Mention exact observable behaviors, not generic strategy labels.
- Separate strong from weak based only on supplied percentile values; lower percentile is better.
- Keep the entire response compact."""

_FOCUS_V2_COMPILER_SYSTEM = """You are the language and pattern-recognition layer of a social-media intelligence brain.
The server has already computed every baseline, evidence count, confidence bucket, and lifecycle transition.
Your job is two things, in this order:

First, match each candidate to an existing pattern from previous_focus.pattern_registry, or mark it NEW.
Second, write the user-facing language for patterns and content_profile.

You DO NOT write any number, percentage, multiplier, pp, K, M, or count.
You DO NOT decide lifecycle status, evidence counts, confidence, baselines, or metric effects.
You DO NOT invent pattern_ids. Existing pattern_id from previous_focus, or NEW. Nothing else.

VOICE:
You write like a cold-eyed analyst documenting an account for another analyst.
Specific, observational, declarative. No hedging. No warmth. No strategy-speak.
Name what the camera, caption, edit, sound, face, prop, or sequence actually does.

GOOD voice:
- "Caption sets up the joke before any action begins. Visual delivers the punchline in the opening beat."
- "Phone-shot vlogs dropped from dominant to occasional. Studio framing now leads."
- "Implicit CTAs only. Series naming invites returning viewers without asking."

BAD voice:
- "Caption hook reel." Too short for content_signature.
- "This account appears to have shifted toward deliberate productions." Hedged and abstract.
- "Strong engagement on creative content." Vague, abstract, useless.

Forbidden words:
may, could, likely, perhaps, suggests, appears, seems, somewhat, fairly, quite, tends to, broadly, generally.

Forbidden constructions:
- Strategy labels without observable behavior. Do not write "authentic" or "premium positioning"; name the visible behavior.
- Adjectives without grounding. Do not write "strong" or "compelling" unless the craft is named.
- Hedged comparisons. Say what is visible, not what might be happening.

PATTERN MATCHING:
For each candidate in compiled_stats.pattern_candidates:

Step one - try to match an existing pattern_id from previous_focus.pattern_registry.
A match is valid only if both conditions hold:
- The candidate's hook style and production approach are semantically equivalent to the existing pattern signature.
- The candidate does not fit the existing pattern's not_this lines better than its positive signature.

Step two - if no clean match exists, return pattern_id_or_match: "NEW".
The server will assign a stable pattern_id.

Do NOT match loosely. Shared format plus one shared craft move is not enough.
Match only when the structural premise of the post is the same.

CONTENT_SIGNATURE FIELDS:
- what_happens: one or two sentences, at least four words, no more than thirty words. Describe the structural action of the post. Begin with the subject behavior, not the outcome.
- hook_style: one sentence, at least four words, no more than sixteen words. Name the opening device: text, face, sound, motion, or prop.
- production_style: one sentence, at least four words, no more than sixteen words. Name camera style, edit cadence, lighting, or sound source.
- voice_tone: one phrase or short sentence, no more than twelve words. Name the tonal register.
- key_craft_moves: two to five bullets. Each bullet is three to eight words and names reproducible behavior.
- not_this: two to four bullets. Each bullet is four to ten words and names what looks similar but is not this pattern.

The not_this lines are the pattern edges. Without them, future compiles collapse different patterns into one.

CONTENT_PROFILE_UPDATES:
- voice.dominant_tone, voice.register, and voice.language_mix: short labels. Inherit prior values unless recent evidence contradicts them.
- voice.tone_range: two or three secondary tone labels.
- voice.cta_style: one sentence, no more than fourteen words. Describe how the account asks for engagement, even when implicit.
- production.by_format.<format>: one sentence per evidenced format, no more than fourteen words. Describe the dominant production approach.
- production.human_presence: one phrase, no more than ten words.
- evolution_notes: one or two lines, no more than eighteen words each. Observational only. Cite concrete shifts from server labels.

INPUTS:
- previous_focus: prior compiled focus, possibly empty for full rebuilds.
- compiled_stats: candidates with server_classification labels, fingerprint summaries, proof_post_keys, and lifecycle phase.
- evidence_buckets: pre-clustered fingerprint summaries.
- new_signal_cards: signal cards with tweak text when available.

OUTPUT JSON ONLY, exactly this shape:
{
  "patterns_proposed": [
    {
      "pattern_id_or_match": "<existing pattern_id from previous_focus, or NEW>",
      "candidate_id": "<server-supplied candidate_id from compiled_stats>",
      "label": "<two to four words>",
      "summary": "<one declarative sentence, no more than twelve words>",
      "content_signature": {
        "what_happens": "<one or two sentences, no more than thirty words>",
        "hook_style": "<one sentence, no more than sixteen words>",
        "production_style": "<one sentence, no more than sixteen words>",
        "voice_tone": "<one phrase, no more than twelve words>",
        "key_craft_moves": ["<three to eight words>", "..."],
        "not_this": ["<four to ten words>", "..."]
      }
    }
  ],
  "content_profile_updates": {
    "voice": {
      "dominant_tone": "",
      "tone_range": [],
      "register": "",
      "language_mix": "",
      "cta_style": ""
    },
    "production": {
      "by_format": { "reel": "", "carousel": "", "image": "" },
      "human_presence": ""
    },
    "evolution_notes": []
  }
}"""


def _sha(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _focus_log(event: str, **fields: Any) -> None:
    payload = {"event": event, **fields}
    print(f"[focus-brain] {json.dumps(payload, sort_keys=True, default=str)}")


def _word_count(text: Any) -> int:
    return len(re.findall(r"\S+", str(text or "").strip()))


def _cap_words(text: Any, limit: int, label: str) -> tuple[str, bool, int]:
    clean = re.sub(r"\s+", " ", str(text or "").strip())
    words = clean.split()
    if len(words) <= limit:
        return clean, False, len(words)
    clipped = " ".join(words[:limit])
    boundary = max(clipped.rfind("."), clipped.rfind("!"), clipped.rfind("?"))
    if boundary >= max(24, int(len(clipped) * 0.55)):
        clipped = clipped[: boundary + 1].strip()
    _focus_log("capsule_truncated", label=label, original_words=len(words), limit=limit, final_words=_word_count(clipped))
    return clipped, True, _word_count(clipped)


def _cap_focus_texts(result: dict[str, Any], *, kind: str) -> dict[str, Any]:
    capped = dict(result)
    if kind == "feeder":
        limits = {
            "focus_md_common": _FEEDER_COMMON_WORD_LIMIT,
            "focus_md_reel": _FEEDER_FORMAT_WORD_LIMIT,
            "focus_md_image": _FEEDER_FORMAT_WORD_LIMIT,
            "focus_md_carousel": _FEEDER_FORMAT_WORD_LIMIT,
        }
    else:
        limits = {
            "capsule_common": _FEED_COMMON_WORD_LIMIT,
            "capsule_reel": _FEED_CAPSULE_WORD_LIMIT,
            "capsule_image": _FEED_CAPSULE_WORD_LIMIT,
            "capsule_carousel": _FEED_CAPSULE_WORD_LIMIT,
            "capsule_anchor": _FEED_CAPSULE_WORD_LIMIT,
            "capsule_cross": _FEED_CAPSULE_WORD_LIMIT,
        }
    for field, limit in limits.items():
        capped[field], _, _ = _cap_words(capped.get(field) or "", limit, f"{kind}.{field}")
    return capped


def _capsule_word_counts(result: dict[str, Any], fields: list[str]) -> dict[str, int]:
    return {field: _word_count(result.get(field) or "") for field in fields}


def _pattern_terms(value: Any) -> list[str]:
    raw = str(value or "").lower()
    words = re.findall(r"[a-z0-9]+", raw)
    terms: list[str] = []
    for word in words:
        if word in _PATTERN_STOPWORDS or len(word) <= 2:
            continue
        normalized = _PATTERN_SYNONYMS.get(word, word)
        if normalized in _PATTERN_STOPWORDS:
            continue
        terms.append(normalized)
    return sorted(set(terms))


def _pattern_id(format_key: str, summary: Any, existing_id: str | None = None) -> str:
    if existing_id:
        return existing_id
    terms = _pattern_terms(summary)
    base = "_".join(terms[:5]) or "pattern"
    digest = hashlib.sha1("|".join(terms).encode("utf-8")).hexdigest()[:6]
    prefix = "common" if str(format_key or "").lower() == "common" else _media_key(format_key)
    return f"{prefix}_{base}_{digest}"


def _pattern_similarity(a: Any, b: Any) -> float:
    left = set(_pattern_terms(a))
    right = set(_pattern_terms(b))
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _merge_pattern_rows(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    merged = dict(a)
    if len(str(b.get("summary") or "")) > len(str(a.get("summary") or "")):
        merged["summary"] = b.get("summary")
    for key in ("evidence_count_90d", "evidence_count_30d", "evidence_count_7d"):
        merged[key] = max(int(a.get(key) or 0), int(b.get(key) or 0))
    a_seen = a.get("last_seen_days")
    b_seen = b.get("last_seen_days")
    seen_values = [int(value) for value in (a_seen, b_seen) if value is not None]
    if seen_values:
        merged["last_seen_days"] = min(seen_values)
    merged["proof_post_keys"] = list(dict.fromkeys([
        *[str(value) for value in (a.get("proof_post_keys") or []) if value],
        *[str(value) for value in (b.get("proof_post_keys") or []) if value],
    ]))[:12]
    merged["canonical_terms"] = sorted(set([
        *[str(value) for value in (a.get("canonical_terms") or []) if value],
        *[str(value) for value in (b.get("canonical_terms") or []) if value],
        *_pattern_terms(a.get("summary")),
        *_pattern_terms(b.get("summary")),
    ]))[:12]
    support: dict[str, int] = {}
    for source in (a.get("trigger_support") or {}, b.get("trigger_support") or {}):
        if not isinstance(source, dict):
            continue
        for key, value in source.items():
            support[str(key)] = support.get(str(key), 0) + int(value or 0)
    if support:
        merged["trigger_support"] = support
    return merged


def _normalize_patterns(structured_patterns: Any, *, cap_per_format: bool = False) -> dict[str, Any]:
    data = structured_patterns if isinstance(structured_patterns, dict) else {}
    rows = data.get("patterns") if isinstance(data.get("patterns"), list) else []
    merged: list[dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        fmt = _media_key(row.get("format") or "common")
        if str(row.get("format") or "").lower() == "common":
            fmt = "common"
        row["format"] = fmt
        row["pattern_id"] = _pattern_id(fmt, row.get("summary"), str(row.get("pattern_id") or "") or None)
        row["canonical_terms"] = sorted(set([*(row.get("canonical_terms") or []), *_pattern_terms(row.get("summary"))]))[:12]
        matched = False
        for idx, existing in enumerate(merged):
            if existing.get("format") != row.get("format"):
                continue
            if existing.get("pattern_id") == row.get("pattern_id") or _pattern_similarity(existing.get("summary"), row.get("summary")) >= 0.52:
                merged[idx] = _merge_pattern_rows(existing, row)
                matched = True
                break
        if not matched:
            merged.append(row)

    def bucket(row: dict[str, Any]) -> str:
        status = str(row.get("status") or "watchlist").lower()
        if status == "stable":
            return "stable"
        if status in {"strengthening", "emerging"}:
            return "growth"
        if status == "watchlist":
            return "watchlist"
        return "avoid"

    def score(row: dict[str, Any]) -> tuple[int, int, int, int]:
        status_priority = {"stable": 4, "strengthening": 3, "emerging": 2, "watchlist": 1, "weakening": 0, "decaying": -1, "archived": -2}
        return (
            status_priority.get(str(row.get("status") or "").lower(), 0),
            int(row.get("evidence_count_30d") or 0),
            int(row.get("evidence_count_90d") or 0),
            -int(row.get("last_seen_days") or 999),
        )

    capped: list[dict[str, Any]] = []
    format_groups = sorted({str(row.get("format") or "common") for row in merged}) if cap_per_format else ["__all__"]
    for format_group in format_groups:
        group_rows = [row for row in merged if not cap_per_format or str(row.get("format") or "common") == format_group]
        for bucket_name in ("stable", "growth", "watchlist", "avoid"):
            bucket_rows = [row for row in group_rows if bucket(row) == bucket_name]
            capped.extend(sorted(bucket_rows, key=score, reverse=True)[:_MAX_PATTERNS_PER_BUCKET])
    data["patterns"] = capped
    return data


def _pattern_summary(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("summary", "pattern", "title", "name"):
            if value.get(key):
                return str(value.get(key))
        return json.dumps(value, sort_keys=True, default=str)[:220]
    return str(value or "")


def _candidate_kind(signal_type: Any) -> str:
    value = str(signal_type or "").upper()
    if "COMMENT" in value:
        return "comment_spike"
    if "LIKE" in value:
        return "like_heavy"
    if "VIRAL" in value or "VIEW" in value:
        return "viral_passive"
    if "FADE" in value:
        return "fade"
    if "ANCHOR" in value:
        return "anchor"
    if "CROSS" in value:
        return "cross"
    if "BREAKOUT" in value or "SUSTAIN" in value:
        return "breakout"
    return "other"


def _aggregate_memory_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        candidate = row.get("focus_memory_candidate")
        if not isinstance(candidate, dict):
            continue
        items: list[Any] = []
        for key in ("candidate_patterns", "candidate_avoid"):
            values = candidate.get(key)
            if isinstance(values, list):
                items.extend(values)
        for item in items:
            summary = _pattern_summary(item)
            terms = _pattern_terms(summary)
            if not terms:
                continue
            key = hashlib.sha1("|".join(terms).encode("utf-8")).hexdigest()[:10]
            bucket = grouped.setdefault(key, {
                "merged_candidate": summary,
                "canonical_terms": terms,
                "evidence_posts": [],
                "frequency": 0,
                "trigger_support": {},
                "trigger_types": [],
                "source_signal_ids": [],
                "collisions": [],
            })
            if len(summary) > len(str(bucket.get("merged_candidate") or "")):
                bucket["merged_candidate"] = summary
            bucket["frequency"] = int(bucket.get("frequency") or 0) + 1
            kind = _candidate_kind(row.get("signal_type"))
            support = bucket.setdefault("trigger_support", {})
            support[kind] = int(support.get(kind) or 0) + 1
            if row.get("signal_type") and row.get("signal_type") not in bucket["trigger_types"]:
                bucket["trigger_types"].append(row.get("signal_type"))
            if row.get("signal_id") and row.get("signal_id") not in bucket["source_signal_ids"]:
                bucket["source_signal_ids"].append(row.get("signal_id"))
            if isinstance(item, dict):
                for post_key in item.get("evidence_post_keys") or item.get("proof_post_keys") or []:
                    if post_key and post_key not in bucket["evidence_posts"]:
                        bucket["evidence_posts"].append(post_key)
            collision = candidate.get("candidate_collision")
            if collision and collision not in bucket["collisions"]:
                bucket["collisions"].append(collision)
    return sorted(grouped.values(), key=lambda item: (int(item.get("frequency") or 0), len(item.get("evidence_posts") or [])), reverse=True)[:30]


def _provider() -> str | None:
    preferred = (SIGNAL_INTELLIGENCE_PROVIDER or "auto").strip().lower()
    if preferred == "openrouter":
        return "openrouter" if OPENROUTER_API_KEY else None
    if preferred == "google":
        return "google" if GEMINI_API_KEY else None
    if OPENROUTER_API_KEY:
        return "openrouter"
    if GEMINI_API_KEY:
        return "google"
    return None


def _runtime_model() -> str:
    return (SIGNAL_INTELLIGENCE_MODEL or _DEFAULT_FLASH_MODEL).strip() or _DEFAULT_FLASH_MODEL


def _google_model_name(model: str) -> str:
    return model.split("/", 1)[1] if model.startswith("google/") else model


def _json_from_text(text: str) -> dict[str, Any] | None:
    raw = (text or "").strip()
    for candidate in _json_candidates_from_text(raw):
        try:
            parsed = json.loads(candidate)
        except Exception:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def _json_candidates_from_text(text: str) -> list[str]:
    raw = (text or "").strip()
    if not raw:
        return []

    candidates = [raw]
    for match in re.finditer(r"```(?:json)?\s*(.*?)```", raw, re.IGNORECASE | re.DOTALL):
        fenced = (match.group(1) or "").strip()
        if fenced:
            candidates.append(fenced)
    candidates.extend(_balanced_json_object_candidates(raw))

    seen: set[str] = set()
    unique: list[str] = []
    for candidate in candidates:
        clean = candidate.strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        unique.append(clean)
    return unique


def _balanced_json_object_candidates(text: str) -> list[str]:
    candidates: list[str] = []
    start: int | None = None
    depth = 0
    in_string = False
    escaped = False

    for idx, ch in enumerate(text or ""):
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            if depth == 0:
                start = idx
            depth += 1
            continue
        if ch != "}" or depth == 0:
            continue

        depth -= 1
        if depth == 0 and start is not None:
            candidates.append(text[start : idx + 1])
            start = None

    return sorted(candidates, key=len, reverse=True)


def _extract_text(payload: dict[str, Any], provider: str) -> str:
    if provider == "openrouter":
        content = payload.get("choices", [{}])[0].get("message", {}).get("content", "")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            return "\n".join(str(part.get("text") or "") for part in content if isinstance(part, dict)).strip()
        return ""
    return payload.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")


def _call_text_model(system: str, payload: dict[str, Any], *, model: str, max_tokens: int = 1600) -> dict[str, Any] | None:
    parsed, _ = _call_text_model_raw(system, payload, model=model, max_tokens=max_tokens)
    return parsed


def _call_text_model_raw(
    system: str,
    payload: dict[str, Any],
    *,
    model: str,
    max_tokens: int = 1600,
) -> tuple[dict[str, Any] | None, str]:
    provider = _provider()
    if not provider:
        return None, ""
    user_text = json.dumps(payload, default=str)
    try:
        if provider == "openrouter":
            resp = requests.post(
                f"{OPENROUTER_BASE_URL.rstrip('/')}{_OPENROUTER_CHAT_URL}",
                headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": user_text},
                    ],
                    "temperature": 0.1,
                    "max_tokens": max_tokens,
                },
                timeout=180,
            )
        else:
            resp = requests.post(
                _GEMINI_API_URL.format(model=_google_model_name(model)),
                params={"key": GEMINI_API_KEY},
                json={
                    "contents": [{"parts": [{"text": system}, {"text": user_text}]}],
                    "generationConfig": {
                        "temperature": 0.1,
                        "maxOutputTokens": max_tokens,
                        "responseMimeType": "application/json",
                    },
                },
                timeout=180,
            )
        resp.raise_for_status()
        text = _extract_text(resp.json(), provider)
        parsed = _json_from_text(text)
        if parsed is None:
            _focus_log(
                "model_json_malformed",
                provider=provider,
                model=model,
                text_excerpt=text[:320],
            )
        return parsed, text
    except Exception as exc:
        print(f"[focus-brain] model call failed: {exc}")
        return None, ""


def _call_text_model_with_json_retry(
    system: str,
    payload: dict[str, Any],
    *,
    model: str,
    max_tokens: int = 1600,
) -> tuple[dict[str, Any] | None, str, bool]:
    parsed, raw = _call_text_model_raw(system, payload, model=model, max_tokens=max_tokens)
    if parsed is not None:
        return parsed, raw, False
    retry_system = (
        f"{system}\n\nYour last response was malformed. Return valid JSON only. "
        "No prose, no markdown fences, no commentary."
    )
    parsed, raw = _call_text_model_raw(retry_system, payload, model=model, max_tokens=max_tokens)
    return parsed, raw, parsed is None


def _media_key(media_type: Any) -> str:
    value = str(media_type or "").strip().lower()
    if value in {"sidecar", "carousel"}:
        return "carousel"
    if value == "reel":
        return "reel"
    return "image"


def _median(values: list[float]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return None
    return round(float(statistics.median(clean)), 2)


def _age_days(posted_at: Any, now: datetime | None = None) -> int:
    ref = now or datetime.now(timezone.utc)
    if isinstance(posted_at, datetime):
        parsed = posted_at
    elif isinstance(posted_at, str) and posted_at.strip():
        parsed = _parse_dt(posted_at)
        if not parsed:
            return 999
    else:
        return 999
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0, (ref - parsed.astimezone(timezone.utc)).days)


def _metric_windows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    out: dict[str, Any] = {}
    for media in sorted({_media_key(row.get("media_type")) for row in rows} | {"reel", "image", "carousel"}):
        media_rows = [row for row in rows if _media_key(row.get("media_type")) == media]
        out[media] = {}
        for days in (7, 30, 90):
            window_rows = []
            for row in media_rows:
                if _age_days(row.get("posted_at"), now) <= days:
                    window_rows.append(row)
            out[media][f"{days}d"] = {
                "post_count": len(window_rows),
                "median_percentile": _median([row.get("percentile") for row in window_rows if row.get("percentile") is not None]),
                "median_views": _median([row.get("views") for row in window_rows if row.get("views") is not None]),
                "median_likes": _median([row.get("likes") for row in window_rows if row.get("likes") is not None]),
                "median_comments": _median([row.get("comments") for row in window_rows if row.get("comments") is not None]),
            }
    return out


def _num(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _mean(values: list[Any]) -> float | None:
    clean = [_num(value) for value in values]
    clean = [value for value in clean if value is not None]
    if not clean:
        return None
    return round(sum(clean) / len(clean), 4)


def _pctl(values: list[Any], percentile: float) -> float | None:
    clean = sorted(value for value in (_num(item) for item in values) if value is not None)
    if not clean:
        return None
    if len(clean) == 1:
        return round(clean[0], 4)
    idx = (len(clean) - 1) * percentile
    lo = int(idx)
    hi = min(lo + 1, len(clean) - 1)
    frac = idx - lo
    return round(clean[lo] * (1 - frac) + clean[hi] * frac, 4)


def _norm(value: float | None, *, target: float) -> float:
    if value is None or target <= 0:
        return 0.0
    return max(0.0, min(float(value) / target, 1.0))


def _confidence_bucket(score: float) -> str:
    if score > 0.70:
        return "high"
    if score >= 0.40:
        return "medium"
    return "low"


def _valid_focus_evidence_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    valid: list[dict[str, Any]] = []
    for row in rows:
        fingerprint = row.get("fingerprint")
        media_hash = str(row.get("media_source_hash") or "").strip()
        if not isinstance(fingerprint, dict) or not fingerprint:
            continue
        if not media_hash or media_hash == _EMPTY_MEDIA_SOURCE_HASH:
            continue
        valid.append(row)
    return valid


def _count_by_format(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"reel": 0, "image": 0, "carousel": 0}
    for row in rows:
        counts[_media_key(row.get("media_type"))] = counts.get(_media_key(row.get("media_type")), 0) + 1
    return counts


def _format_mix(counts: dict[str, int]) -> dict[str, Any]:
    total = sum(counts.values())
    if total <= 0:
        return {"by_format": counts, "pct_by_format": {}, "evolution_30d": "stable"}
    return {
        "by_format": counts,
        "pct_by_format": {key: round(value / total, 4) for key, value in counts.items()},
        "evolution_30d": "stable",
    }


def _metric_baselines(rows: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {"by_format": {}, "by_sub_bucket": {}}
    for media in ("reel", "image", "carousel"):
        bucket = [row for row in rows if _media_key(row.get("media_type")) == media]
        out["by_format"][media] = {
            "post_count": len(bucket),
            "p10_views": _pctl([row.get("views") for row in bucket], 0.90),
            "p50_views": _pctl([row.get("views") for row in bucket], 0.50),
            "p90_views": _pctl([row.get("views") for row in bucket], 0.10),
            "p10_likes": _pctl([row.get("likes") for row in bucket], 0.90),
            "p50_likes": _pctl([row.get("likes") for row in bucket], 0.50),
            "p90_likes": _pctl([row.get("likes") for row in bucket], 0.10),
            "p10_comments": _pctl([row.get("comments") for row in bucket], 0.90),
            "p50_comments": _pctl([row.get("comments") for row in bucket], 0.50),
            "p90_comments": _pctl([row.get("comments") for row in bucket], 0.10),
            "median_d7_percentile": _median([row.get("percentile") for row in bucket]),
            "median_d21_percentile": _median([row.get("d21_percentile") for row in bucket]),
        }
    return out


def _metric_trends(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    trends: list[dict[str, Any]] = []
    for media in ("reel", "image", "carousel"):
        media_rows = sorted(
            [row for row in rows if _media_key(row.get("media_type")) == media],
            key=lambda row: _parse_dt(row.get("posted_at")) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        if len(media_rows) < 10:
            continue
        recent = _median([row.get("percentile") for row in media_rows[:5]])
        previous = _median([row.get("percentile") for row in media_rows[5:10]])
        if recent is None or previous is None:
            continue
        delta = round(recent - previous, 2)
        if delta <= -10:
            direction = "rising"
        elif delta >= 10:
            direction = "declining"
        elif abs(delta) <= 5:
            direction = "stable"
        else:
            direction = "watching"
        trends.append({
            "dimension": f"{media}.d7_percentile",
            "direction": direction,
            "recent_window_posts": 5,
            "previous_window_posts": 5,
            "delta_pp": delta,
        })
    return trends


def _follower_trends(conn: Any, feeder_id: int) -> dict[str, Any]:
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                select snapshot_date_ist, follower_count
                from public.feeder_follower_snapshots
                where feeder_id = %s
                  and snapshot_date_ist >= (now() at time zone 'Asia/Kolkata')::date - interval '90 days'
                order by snapshot_date_ist asc
                """,
                (feeder_id,),
            )
            rows = cur.fetchall()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"current_count": None, "avg_weekly_delta": None, "trend_30d": "unknown"}
    if not rows:
        return {"current_count": None, "avg_weekly_delta": None, "trend_30d": "unknown"}
    counts = [int(row.get("follower_count") or 0) for row in rows]
    weekly_delta = (counts[-1] - counts[0]) / max(len(rows) / 7, 1)
    last_30 = counts[-30:] if len(counts) >= 30 else counts
    trend_delta = last_30[-1] - last_30[0] if len(last_30) >= 2 else 0
    if trend_delta > max(100, abs(last_30[0]) * 0.005 if last_30 else 0):
        trend = "rising"
    elif trend_delta < -max(100, abs(last_30[0]) * 0.005 if last_30 else 0):
        trend = "declining"
    else:
        trend = "stable"
    return {
        "current_count": counts[-1],
        "avg_weekly_delta": round(weekly_delta, 2),
        "trend_30d": trend,
    }


def _cadence_profile(rows: list[dict[str, Any]]) -> dict[str, Any]:
    parsed = [dt for row in rows for dt in [_parse_dt(row.get("posted_at"))] if dt is not None]
    if len(parsed) < 2:
        return {"posts_per_week_avg": len(parsed), "trend_4w": "stable"}
    span_days = max(1, (max(parsed) - min(parsed)).days)
    return {"posts_per_week_avg": round(len(parsed) / max(span_days / 7, 1), 2), "trend_4w": "stable"}


def _compact_evidence_post(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "post_key": row.get("post_key"),
        "posted_at": row.get("posted_at"),
        "media_type": _media_key(row.get("media_type")),
        "percentile": row.get("percentile"),
        "views": row.get("views"),
        "likes": row.get("likes"),
        "comments": row.get("comments"),
        "signal_types": row.get("signal_types") or [],
        "fingerprint": row.get("fingerprint") or {},
        "focus_read": row.get("focus_read") or {},
    }


def _week_key(value: Any) -> str:
    parsed = _parse_dt(value)
    if not parsed:
        return "unknown"
    year, week, _ = parsed.isocalendar()
    return f"{year}-W{week:02d}"


def _date_range(rows: list[dict[str, Any]]) -> dict[str, str]:
    dates = sorted(
        parsed.date().isoformat()
        for row in rows
        for parsed in [_parse_dt(row.get("posted_at"))]
        if parsed is not None
    )
    if not dates:
        return {"from": "", "to": ""}
    return {"from": dates[0], "to": dates[-1]}


def _fallback_bucket_summary(bucket: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(bucket, key=lambda row: row.get("percentile") or 999)
    return {
        "bucket_summary": "Backend fallback summary from compact evidence; model bucket summary unavailable.",
        "strong_patterns": [
            str((row.get("fingerprint") or {}).get("content_summary") or (row.get("fingerprint") or {}).get("topic") or row.get("post_key"))
            for row in ordered[:3]
        ],
        "weak_patterns": [
            str((row.get("fingerprint") or {}).get("content_summary") or (row.get("fingerprint") or {}).get("topic") or row.get("post_key"))
            for row in ordered[-3:]
        ],
        "caption_tone_notes": [],
        "visual_style_notes": [],
        "metric_notes": [
            f"{row.get('post_key')}: p{row.get('percentile')}"
            for row in ordered[:5]
        ],
        "proof_post_keys": [str(row.get("post_key") or "") for row in ordered[:8] if row.get("post_key")],
    }


def _bucket_evidence_for_compiler(evidence_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in evidence_rows:
        key = (_media_key(row.get("media_type")), _week_key(row.get("posted_at")))
        grouped.setdefault(key, []).append(row)

    summaries: list[dict[str, Any]] = []
    for (media, week), rows in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1])):
        ordered = sorted(rows, key=lambda row: _parse_dt(row.get("posted_at")) or datetime.min.replace(tzinfo=timezone.utc))
        for idx in range(0, len(ordered), 10):
            bucket = ordered[idx: idx + 10]
            compact_posts = [_compact_evidence_post(row) for row in bucket]
            result = _call_text_model(
                _EVIDENCE_BUCKET_SYSTEM,
                {
                    "media_type": media,
                    "week": week,
                    "date_range": _date_range(bucket),
                    "posts": compact_posts,
                },
                model=_runtime_model(),
                max_tokens=700,
            ) or _fallback_bucket_summary(bucket)
            summaries.append({
                "bucket_key": f"{media}:{week}:{idx // 10 + 1}",
                "media_type": media,
                "week": week,
                "date_range": _date_range(bucket),
                "post_count": len(bucket),
                **result,
            })
    return summaries


def _row_pattern_text(row: dict[str, Any]) -> str:
    bits: list[str] = [
        str(row.get("caption") or ""),
        json.dumps(row.get("fingerprint") or {}, sort_keys=True, default=str),
        json.dumps(row.get("focus_read") or {}, sort_keys=True, default=str),
    ]
    return " ".join(bits)


def _pattern_match(pattern_terms: set[str], row_terms: set[str]) -> bool:
    if not pattern_terms or not row_terms:
        return False
    overlap = len(pattern_terms & row_terms)
    return overlap / max(1, len(pattern_terms)) >= 0.45 or overlap / max(1, len(pattern_terms | row_terms)) >= 0.28


def _pattern_stat_rows(structured_patterns: Any, evidence_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    data = structured_patterns if isinstance(structured_patterns, dict) else {}
    patterns = data.get("patterns") if isinstance(data.get("patterns"), list) else []
    now = datetime.now(timezone.utc)
    row_terms = [(row, set(_pattern_terms(_row_pattern_text(row)))) for row in evidence_rows]
    stats: list[dict[str, Any]] = []
    for item in patterns:
        if not isinstance(item, dict):
            continue
        terms = set(_pattern_terms(" ".join([
            str(item.get("summary") or ""),
            " ".join(str(value) for value in (item.get("canonical_terms") or []) if value),
        ])))
        item_format = str(item.get("format") or "common").lower()
        relevant_rows = [
            row for row in evidence_rows
            if item_format == "common" or _media_key(row.get("media_type")) == _media_key(item_format)
        ]
        relevant_post_keys = {str(row.get("post_key") or "") for row in relevant_rows}
        media_post_count_30d = sum(1 for row in relevant_rows if _age_days(row.get("posted_at"), now) <= 30)
        matched = [
            row for row, terms_for_row in row_terms
            if str(row.get("post_key") or "") in relevant_post_keys and _pattern_match(terms, terms_for_row)
        ]
        if not matched:
            stats.append({
                "pattern_id": item.get("pattern_id") or _pattern_id(item.get("format") or "common", item.get("summary")),
                "summary": item.get("summary") or "",
                "canonical_terms": sorted(terms)[:12],
                "server_evidence_count_90d": 0,
                "server_evidence_count_30d": 0,
                "server_evidence_count_7d": 0,
                "server_last_seen_days": int(item.get("last_seen_days") or 999),
                "server_trigger_support": {},
                "server_format_support": {},
                "server_opportunities_since_seen": len(relevant_rows),
                "server_media_post_count_30d": media_post_count_30d,
                "server_proof_post_keys": [],
            })
            continue
        ages = [_age_days(row.get("posted_at"), now) for row in matched]
        matched_dates = [parsed for row in matched for parsed in [_parse_dt(row.get("posted_at"))] if parsed is not None]
        latest_match = max(matched_dates) if matched_dates else None
        opportunities_since_seen = 0
        if latest_match is not None:
            opportunities_since_seen = sum(
                1
                for row in relevant_rows
                for parsed in [_parse_dt(row.get("posted_at"))]
                if parsed is not None and parsed > latest_match
            )
        trigger_support: dict[str, int] = {}
        format_support: dict[str, int] = {}
        proof_posts: list[str] = []
        for row in sorted(matched, key=lambda value: (_age_days(value.get("posted_at"), now), value.get("percentile") or 999)):
            fmt = _media_key(row.get("media_type"))
            format_support[fmt] = format_support.get(fmt, 0) + 1
            for signal_type in row.get("signal_types") or []:
                kind = _candidate_kind(signal_type)
                trigger_support[kind] = trigger_support.get(kind, 0) + 1
            post_key = str(row.get("post_key") or "")
            if post_key and post_key not in proof_posts:
                proof_posts.append(post_key)
        stats.append({
            "pattern_id": item.get("pattern_id") or _pattern_id(item.get("format") or "common", item.get("summary")),
            "summary": item.get("summary") or "",
            "canonical_terms": sorted(terms)[:12],
            "server_evidence_count_90d": len(matched),
            "server_evidence_count_30d": sum(1 for age in ages if age <= 30),
            "server_evidence_count_7d": sum(1 for age in ages if age <= 7),
            "server_last_seen_days": min(ages) if ages else 999,
            "server_trigger_support": trigger_support,
            "server_format_support": format_support,
            "server_opportunities_since_seen": opportunities_since_seen,
            "server_media_post_count_30d": media_post_count_30d,
            "server_proof_post_keys": proof_posts[:12],
        })
    return stats


def _apply_server_pattern_stats(structured_patterns: Any, evidence_rows: list[dict[str, Any]]) -> dict[str, Any]:
    data = structured_patterns if isinstance(structured_patterns, dict) else {}
    patterns = data.get("patterns") if isinstance(data.get("patterns"), list) else []
    stats = {str(row.get("pattern_id") or ""): row for row in _pattern_stat_rows(data, evidence_rows)}
    updated: list[dict[str, Any]] = []
    archived = [row for row in (data.get("archived_patterns") or []) if isinstance(row, dict)]
    for item in patterns:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        stat = stats.get(str(row.get("pattern_id") or ""))
        if stat:
            row["evidence_count_90d"] = int(stat.get("server_evidence_count_90d") or 0)
            row["evidence_count_30d"] = int(stat.get("server_evidence_count_30d") or 0)
            row["evidence_count_7d"] = int(stat.get("server_evidence_count_7d") or 0)
            row["last_seen_days"] = int(stat.get("server_last_seen_days") or 999)
            row["trigger_support"] = stat.get("server_trigger_support") or {}
            row["format_support"] = stat.get("server_format_support") or {}
            row["opportunities_since_seen"] = int(stat.get("server_opportunities_since_seen") or 0)
            row["media_post_count_30d"] = int(stat.get("server_media_post_count_30d") or 0)
            if stat.get("server_proof_post_keys"):
                row["proof_post_keys"] = stat.get("server_proof_post_keys")
        seen = int(row.get("last_seen_days") or 999)
        opportunities = int(row.get("opportunities_since_seen") or 0)
        media_post_count_30d = int(row.get("media_post_count_30d") or 0)
        status = str(row.get("status") or "watchlist").lower()
        if opportunities >= 25 or seen >= 75:
            status = "archived"
        elif opportunities >= 18 or (seen >= 50 and media_post_count_30d >= 4):
            status = "decaying"
        elif (opportunities >= 10 or (seen >= 25 and media_post_count_30d >= 4)) and status not in {"decaying", "archived"}:
            status = "weakening"
        row["status"] = status
        if status == "archived":
            archived.append(row)
        else:
            updated.append(row)
    data["patterns"] = updated
    data["archived_patterns"] = archived[-40:]
    return data


def _positive_signature_text(pattern: dict[str, Any]) -> str:
    signature = pattern.get("content_signature") if isinstance(pattern.get("content_signature"), dict) else {}
    values: list[str] = [
        str(pattern.get("label") or ""),
        str(pattern.get("summary") or ""),
        str(signature.get("what_happens") or ""),
        str(signature.get("hook_style") or ""),
        str(signature.get("production_style") or ""),
        str(signature.get("voice_tone") or ""),
        " ".join(str(value) for value in (signature.get("key_craft_moves") or []) if value),
    ]
    return " ".join(values)


def _negative_signature_text(pattern: dict[str, Any]) -> str:
    signature = pattern.get("content_signature") if isinstance(pattern.get("content_signature"), dict) else {}
    return " ".join(str(value) for value in (signature.get("not_this") or []) if value)


def _boundary_scores(candidate_text: str, pattern: dict[str, Any]) -> tuple[float, float]:
    return (
        _pattern_similarity(candidate_text, _positive_signature_text(pattern)),
        _pattern_similarity(candidate_text, _negative_signature_text(pattern)),
    )


def _valid_existing_pattern_match(candidate_text: str, existing_pattern: dict[str, Any]) -> bool:
    positive_score, negative_score = _boundary_scores(candidate_text, existing_pattern)
    return positive_score > negative_score


def _legacy_status_to_v2(value: Any) -> str:
    status = str(value or "emerging").strip().lower()
    if status == "watchlist":
        return "watching"
    if status in {"emerging", "strengthening", "stable", "watching", "weakening", "decaying", "archived", "conflict"}:
        return status
    return "emerging"


def _v2_status_to_legacy(value: Any) -> str:
    status = str(value or "emerging").strip().lower()
    if status == "watching":
        return "watchlist"
    return status


def _lifecycle_rank(status: Any) -> int:
    order = {
        "archived": -3,
        "decaying": -2,
        "weakening": -1,
        "watching": 0,
        "emerging": 1,
        "strengthening": 2,
        "stable": 3,
        "conflict": 3,
    }
    return order.get(str(status or "").lower(), 0)


def _derive_lifecycle_status(previous_status: Any, candidate: dict[str, Any]) -> str:
    prev = _legacy_status_to_v2(previous_status)
    evidence_count = int(candidate.get("evidence_count_window") or 0)
    window_posts = int(candidate.get("window_posts") or 0)
    last_seen = int(candidate.get("last_seen_n_posts_ago") or 999)
    opportunities = int(candidate.get("opportunities_since_seen") or 0)
    evidence_last_5 = int(candidate.get("evidence_in_last_5_posts") or 0)
    evidence_last_7 = int(candidate.get("evidence_in_last_7_posts") or 0)
    evidence_last_12 = int(candidate.get("evidence_in_last_12_posts") or 0)
    evidence_last_18 = int(candidate.get("evidence_in_last_18_posts") or 0)
    strong_single = bool(candidate.get("single_strong_signal"))
    window_expired = bool(candidate.get("window_expired"))
    competing = bool(candidate.get("competing_pattern_strengthening"))

    if prev == "archived":
        return "archived"
    if evidence_last_18 == 0 or window_expired or opportunities >= 18:
        return "archived" if prev == "decaying" else "decaying"
    if prev == "decaying":
        return "decaying"
    if evidence_last_12 <= 1 and opportunities >= 12:
        return "decaying" if prev == "weakening" else "weakening"
    if prev == "weakening":
        return "weakening"
    if evidence_last_7 <= 1 and opportunities >= 7:
        return "weakening" if prev == "watching" else prev
    if prev == "stable" and last_seen >= 5 and competing:
        return "watching"
    if evidence_count >= 5 and window_posts >= 10:
        return "stable" if prev in {"strengthening", "stable"} else "strengthening"
    if evidence_last_12 >= 5:
        return "strengthening"
    if evidence_last_5 >= 2 or strong_single:
        return "emerging" if prev in {"emerging", "watching"} else prev
    return prev if prev not in {"watching"} else "watching"


def _metric_direction(delta_pp: float | None) -> str:
    if delta_pp is None:
        return "stable"
    if delta_pp <= -10:
        return "rising"
    if delta_pp >= 10:
        return "declining"
    return "stable"


def _confidence_for_candidate(cohort_size: int, vs_baseline_pp: float | None, status: str, last_seen: int) -> dict[str, Any]:
    metric_magnitude = min(abs(float(vs_baseline_pp or 0)) / 40, 1.0)
    stability_factor = {
        "stable": 1.0,
        "strengthening": 0.75,
        "emerging": 0.45,
        "watching": 0.4,
        "weakening": 0.35,
        "decaying": 0.25,
        "archived": 0.1,
        "conflict": 0.45,
    }.get(status, 0.35)
    recency_factor = 1.0 - min(max(last_seen, 0), 18) / 18
    score = (
        0.40 * _norm(cohort_size, target=5)
        + 0.25 * metric_magnitude
        + 0.20 * stability_factor
        + 0.15 * recency_factor
    )
    rounded = round(score, 4)
    return {
        "score": rounded,
        "bucket": _confidence_bucket(rounded),
        "components": {
            "cohort_size": round(_norm(cohort_size, target=5), 4),
            "metric_magnitude": round(metric_magnitude, 4),
            "pattern_stability": round(stability_factor, 4),
            "recency": round(recency_factor, 4),
        },
    }


def _associated_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "views_x_avg": _mean([row.get("views_multiple") for row in rows]),
        "comments_x_avg": _mean([row.get("comments_multiple") for row in rows]),
        "likes_x_avg": _mean([row.get("likes_multiple") for row in rows]),
        "follower_delta_avg": None,
    }


def _engagement_skew(metrics: dict[str, Any]) -> list[str]:
    skew: list[str] = []
    if _num(metrics.get("views_x_avg")) is not None and float(metrics["views_x_avg"]) >= 2.0:
        skew.append("view_spiking")
    if _num(metrics.get("comments_x_avg")) is not None and float(metrics["comments_x_avg"]) >= 2.0:
        skew.append("comment_spiking")
    if _num(metrics.get("likes_x_avg")) is not None and float(metrics["likes_x_avg"]) >= 2.0:
        skew.append("like_heavy")
    return skew


def _content_text_from_fingerprint(row: dict[str, Any]) -> str:
    fp = row.get("fingerprint") if isinstance(row.get("fingerprint"), dict) else {}
    bits = [
        row.get("caption"),
        fp.get("content_summary"),
        fp.get("topic"),
        fp.get("hook"),
        fp.get("payoff"),
        fp.get("visual_sequence"),
        fp.get("emotional_trigger"),
        fp.get("caption_role"),
        " ".join(str(value) for value in (fp.get("craft_moves") or []) if value) if isinstance(fp.get("craft_moves"), list) else "",
    ]
    return " ".join(str(bit) for bit in bits if bit)


def _candidate_rows_for_terms(rows: list[dict[str, Any]], terms: set[str], media_type: str) -> list[dict[str, Any]]:
    matched: list[dict[str, Any]] = []
    for row in rows:
        if media_type != "common" and _media_key(row.get("media_type")) != media_type:
            continue
        if _pattern_match(terms, set(_pattern_terms(_content_text_from_fingerprint(row)))):
            matched.append(row)
    return matched


def _build_candidate_from_summary(
    *,
    summary: str,
    media_type: str,
    rows: list[dict[str, Any]],
    format_baseline: float | None,
    previous_pattern: dict[str, Any] | None = None,
    source: str = "pattern",
) -> dict[str, Any] | None:
    clean_summary = re.sub(r"\s+", " ", str(summary or "").strip())
    if not clean_summary:
        return None
    terms = set(_pattern_terms(clean_summary))
    if not terms:
        return None
    matched = _candidate_rows_for_terms(rows, terms, media_type)
    if not matched and previous_pattern:
        proof = {str(value) for value in (previous_pattern.get("proof_post_keys") or []) if value}
        matched = [row for row in rows if str(row.get("post_key") or "") in proof]
    if not matched:
        matched = [row for row in rows if media_type == "common" or _media_key(row.get("media_type")) == media_type][:2]
    if not matched:
        return None

    ordered = sorted(matched, key=lambda row: _parse_dt(row.get("posted_at")) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    median_percentile = _median([row.get("percentile") for row in ordered])
    vs_baseline = round(float(median_percentile) - float(format_baseline), 2) if median_percentile is not None and format_baseline is not None else None
    relevant_rows = sorted(
        [row for row in rows if media_type == "common" or _media_key(row.get("media_type")) == media_type],
        key=lambda row: _parse_dt(row.get("posted_at")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    matched_keys = {str(row.get("post_key") or "") for row in ordered}
    latest_match = _parse_dt(ordered[0].get("posted_at")) if ordered else None
    opportunities_since_seen = 0
    if latest_match:
        opportunities_since_seen = sum(
            1
            for row in relevant_rows
            for parsed in [_parse_dt(row.get("posted_at"))]
            if parsed is not None and parsed > latest_match
        )
    evidence_last_5 = sum(1 for row in relevant_rows[:5] if str(row.get("post_key") or "") in matched_keys)
    evidence_last_7 = sum(1 for row in relevant_rows[:7] if str(row.get("post_key") or "") in matched_keys)
    evidence_last_12 = sum(1 for row in relevant_rows[:12] if str(row.get("post_key") or "") in matched_keys)
    evidence_last_18 = sum(1 for row in relevant_rows[:18] if str(row.get("post_key") or "") in matched_keys)
    previous_status = (
        (previous_pattern.get("lifecycle") or {}).get("status")
        if previous_pattern and isinstance(previous_pattern.get("lifecycle"), dict)
        else previous_pattern.get("status") if previous_pattern else "emerging"
    )
    candidate_lifecycle = {
        "evidence_count_window": len(ordered),
        "window_posts": len(relevant_rows),
        "last_seen_n_posts_ago": opportunities_since_seen,
        "opportunities_since_seen": opportunities_since_seen,
        "evidence_in_last_5_posts": evidence_last_5,
        "evidence_in_last_7_posts": evidence_last_7,
        "evidence_in_last_12_posts": evidence_last_12,
        "evidence_in_last_18_posts": evidence_last_18,
        "single_strong_signal": any(row.get("signal_types") for row in ordered[:1]) and (vs_baseline is not None and vs_baseline <= -15),
        "window_expired": False,
        "competing_pattern_strengthening": False,
    }
    status = _derive_lifecycle_status(previous_status, candidate_lifecycle)
    associated = _associated_metrics(ordered)
    confidence = _confidence_for_candidate(len(ordered), vs_baseline, status, opportunities_since_seen)
    proof = [str(row.get("post_key") or "") for row in ordered if row.get("post_key")][:5]
    first_seen = min((_parse_dt(row.get("posted_at")) for row in ordered if _parse_dt(row.get("posted_at"))), default=None)
    latest_seen = max((_parse_dt(row.get("posted_at")) for row in ordered if _parse_dt(row.get("posted_at"))), default=None)
    trigger_support: dict[str, int] = {}
    for row in ordered:
        for signal_type in row.get("signal_types") or []:
            kind = _candidate_kind(signal_type)
            trigger_support[kind] = trigger_support.get(kind, 0) + 1
    candidate_id = f"cand_{hashlib.sha1(f'{media_type}|{clean_summary}|{source}'.encode('utf-8')).hexdigest()[:10]}"
    return {
        "candidate_id": candidate_id,
        "format": media_type,
        "summary_seed": clean_summary,
        "source": source,
        "existing_pattern_id": previous_pattern.get("pattern_id") if previous_pattern else None,
        "evidence_post_keys": proof,
        "fingerprint_summary": clean_summary,
        "server_classification": {
            "evidence_density": "strong" if len(ordered) >= 5 else "moderate" if len(ordered) >= 2 else "weak",
            "metric_direction": _metric_direction(vs_baseline),
            "vs_baseline": "outperforms" if vs_baseline is not None and vs_baseline <= -10 else "underperforms" if vs_baseline is not None and vs_baseline >= 10 else "matches",
            "lifecycle_phase": status,
            "engagement_skew": _engagement_skew(associated),
        },
        "server_fields": {
            "metric_effects": {
                "median_d7_percentile_in_pattern": median_percentile,
                "vs_baseline_pp": vs_baseline,
                "associated_metrics": associated,
                "confidence": confidence,
            },
            "lifecycle": {
                "status": status,
                "evidence_count_window": len(ordered),
                "window_posts": len(relevant_rows),
                "last_seen_n_posts_ago": opportunities_since_seen,
                "opportunities_since_seen": opportunities_since_seen,
                "first_seen_at": first_seen.isoformat() if first_seen else None,
                "promoted_at": latest_seen.isoformat() if status in {"strengthening", "stable"} and latest_seen else None,
                "trigger_support": trigger_support,
            },
            "proof_post_keys": proof,
        },
    }


def _previous_pattern_registry(current: dict[str, Any]) -> list[dict[str, Any]]:
    registry = current.get("pattern_registry")
    return [dict(item) for item in registry if isinstance(item, dict)] if isinstance(registry, list) else []


def _legacy_patterns_as_v2(current: dict[str, Any]) -> list[dict[str, Any]]:
    existing = _previous_pattern_registry(current)
    if existing:
        return existing
    structured = current.get("structured_patterns") if isinstance(current.get("structured_patterns"), dict) else {}
    rows = structured.get("patterns") if isinstance(structured.get("patterns"), list) else []
    registry: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        summary = str(row.get("summary") or "").strip()
        fmt = _media_key(row.get("format") or "common")
        if str(row.get("format") or "").lower() == "common":
            fmt = "common"
        pattern_id = str(row.get("pattern_id") or "") or _pattern_id(fmt, summary)
        label = " ".join(_pattern_terms(summary)[:2]).replace("_", " ").title() or "Pattern"
        registry.append({
            "pattern_id": pattern_id,
            "format": fmt,
            "label": label[:48],
            "summary": summary,
            "content_signature": {
                "what_happens": summary,
                "hook_style": "",
                "production_style": "",
                "voice_tone": "",
                "key_craft_moves": [],
                "not_this": [],
            },
            "metric_effects": {},
            "lifecycle": {
                "status": _legacy_status_to_v2(row.get("status")),
                "evidence_count_window": int(row.get("evidence_count_90d") or 0),
                "window_posts": int(row.get("media_post_count_30d") or 0),
                "last_seen_n_posts_ago": int(row.get("opportunities_since_seen") or 0),
                "opportunities_since_seen": int(row.get("opportunities_since_seen") or 0),
                "first_seen_at": None,
                "promoted_at": None,
                "trigger_support": row.get("trigger_support") or {},
            },
            "proof_post_keys": row.get("proof_post_keys") or [],
            "latest_tweak": "",
            "recent_tweaks": [],
        })
    return registry


def _build_content_profile(result: dict[str, Any], rows: list[dict[str, Any]], llm_updates: dict[str, Any] | None = None) -> dict[str, Any]:
    updates = llm_updates if isinstance(llm_updates, dict) else {}
    counts = _count_by_format(rows)
    voice = updates.get("voice") if isinstance(updates.get("voice"), dict) else {}
    production = updates.get("production") if isinstance(updates.get("production"), dict) else {}
    format_mix = _format_mix(counts)
    notes = updates.get("evolution_notes") if isinstance(updates.get("evolution_notes"), list) else []
    if not notes:
        notes = [
            str(result.get("focus_md_common") or "").strip().split(".")[0][:160]
        ] if str(result.get("focus_md_common") or "").strip() else []
    notes, _ = _clean_language_list(notes, limit=2, max_words=18)
    tone_range, _ = _clean_language_list(voice.get("tone_range"), limit=3, max_words=3)
    return {
        "voice": {
            "dominant_tone": _clean_language(voice.get("dominant_tone"), "", max_words=3)[0],
            "tone_range": tone_range,
            "register": _clean_language(voice.get("register"), "", max_words=3)[0],
            "language_mix": _clean_language(voice.get("language_mix"), "", max_words=4)[0],
            "cta_style": _clean_language(voice.get("cta_style"), "", max_words=14)[0],
        },
        "production": _clean_profile_production(production),
        "format_mix": format_mix,
        "evolution_notes": [str(note) for note in notes if str(note).strip()][:2],
    }


def _build_metric_profile(conn: Any, feeder_id: int, rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "baselines": _metric_baselines(rows),
        "metric_trends": _metric_trends(rows),
        "follower_trends": _follower_trends(conn, feeder_id),
        "cadence": _cadence_profile(rows),
    }


def _stats_builder_output(
    *,
    conn: Any,
    feeder: dict[str, Any],
    rows: list[dict[str, Any]],
    current: dict[str, Any],
    result: dict[str, Any],
    evidence_buckets: list[dict[str, Any]],
    memory_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    feeder_id = int(feeder.get("feeder_id") or feeder.get("id") or 0)
    previous_registry = _legacy_patterns_as_v2(current)
    format_baselines = {
        media: (_metric_baselines(rows).get("by_format", {}).get(media) or {}).get("median_d7_percentile")
        for media in ("reel", "image", "carousel")
    }
    candidates: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for pattern in previous_registry:
        fmt = str(pattern.get("format") or "common")
        baseline = format_baselines.get(_media_key(fmt)) if fmt != "common" else _median([row.get("percentile") for row in rows])
        candidate = _build_candidate_from_summary(
            summary=str(pattern.get("summary") or pattern.get("label") or ""),
            media_type=fmt,
            rows=rows,
            format_baseline=baseline,
            previous_pattern=pattern,
            source="previous_registry",
        )
        if candidate and candidate["candidate_id"] not in seen_ids:
            candidates.append(candidate)
            seen_ids.add(candidate["candidate_id"])

    structured = result.get("structured_patterns") if isinstance(result.get("structured_patterns"), dict) else {}
    for pattern in structured.get("patterns") or []:
        if not isinstance(pattern, dict):
            continue
        fmt = _media_key(pattern.get("format") or "common")
        if str(pattern.get("format") or "").lower() == "common":
            fmt = "common"
        baseline = format_baselines.get(_media_key(fmt)) if fmt != "common" else _median([row.get("percentile") for row in rows])
        candidate = _build_candidate_from_summary(
            summary=str(pattern.get("summary") or ""),
            media_type=fmt,
            rows=rows,
            format_baseline=baseline,
            source="legacy_compiler",
        )
        if candidate and candidate["candidate_id"] not in seen_ids:
            candidates.append(candidate)
            seen_ids.add(candidate["candidate_id"])

    for item in memory_candidates[:20]:
        if not isinstance(item, dict):
            continue
        summary = str(item.get("merged_candidate") or "").strip()
        if not summary:
            continue
        candidate = _build_candidate_from_summary(
            summary=summary,
            media_type="common",
            rows=rows,
            format_baseline=_median([row.get("percentile") for row in rows]),
            source="signal_memory",
        )
        if candidate and candidate["candidate_id"] not in seen_ids:
            candidates.append(candidate)
            seen_ids.add(candidate["candidate_id"])

    if not candidates:
        for bucket in evidence_buckets[:12]:
            if not isinstance(bucket, dict):
                continue
            for key in ("strong_patterns", "weak_patterns"):
                values = bucket.get(key) if isinstance(bucket.get(key), list) else []
                for value in values[:2]:
                    candidate = _build_candidate_from_summary(
                        summary=str(value or ""),
                        media_type=_media_key(bucket.get("media_type")),
                        rows=rows,
                        format_baseline=format_baselines.get(_media_key(bucket.get("media_type"))),
                        source=f"bucket_{key}",
                    )
                    if candidate and candidate["candidate_id"] not in seen_ids:
                        candidates.append(candidate)
                        seen_ids.add(candidate["candidate_id"])

    counts = _count_by_format(rows)
    return {
        "meta": {
            "feeder_id": feeder_id,
            "handle": feeder.get("handle"),
            "role": feeder.get("role"),
            "window_post_count": {"total": len(rows), "by_format": counts},
        },
        "metric_profile": _build_metric_profile(conn, feeder_id, rows),
        "previous_registry": previous_registry,
        "pattern_candidates": candidates[:30],
        "evidence_buckets": [
            {
                "bucket_key": bucket.get("bucket_key"),
                "media_type": bucket.get("media_type"),
                "bucket_summary": bucket.get("bucket_summary"),
                "strong_patterns": bucket.get("strong_patterns") or [],
                "weak_patterns": bucket.get("weak_patterns") or [],
                "caption_tone_notes": bucket.get("caption_tone_notes") or [],
                "visual_style_notes": bucket.get("visual_style_notes") or [],
            }
            for bucket in evidence_buckets[:24]
            if isinstance(bucket, dict)
        ],
    }


def _llm_payload_from_stats(stats: dict[str, Any], format_filter: str | None = None) -> dict[str, Any]:
    candidates = stats.get("pattern_candidates") if isinstance(stats.get("pattern_candidates"), list) else []
    if format_filter:
        candidates = [item for item in candidates if _media_key(item.get("format")) == format_filter]
    previous_registry = stats.get("previous_registry") if isinstance(stats.get("previous_registry"), list) else []
    previous_slice = [
        {
            "pattern_id": item.get("pattern_id"),
            "format": item.get("format"),
            "label": item.get("label"),
            "summary": item.get("summary"),
            "content_signature": item.get("content_signature") or {},
        }
        for item in previous_registry[:30]
        if isinstance(item, dict)
    ]
    return {
        "previous_registry": previous_slice,
        "compiled_stats": {
            "meta": stats.get("meta") or {},
            "pattern_candidates": [
                {
                    "candidate_id": item.get("candidate_id"),
                    "format": item.get("format"),
                    "evidence_post_keys": item.get("evidence_post_keys") or [],
                    "server_classification": item.get("server_classification") or {},
                    "fingerprint_summary": item.get("fingerprint_summary") or item.get("summary_seed") or "",
                }
                for item in candidates
                if isinstance(item, dict)
            ],
        },
        "evidence_buckets": [
            bucket for bucket in (stats.get("evidence_buckets") or [])
            if not format_filter or _media_key(bucket.get("media_type")) == format_filter
        ],
    }


def _estimated_tokens(value: Any) -> int:
    return max(1, len(json.dumps(value, default=str)) // 4)


def _compile_v2_language(
    stats: dict[str, Any],
    *,
    model: str,
    system: str = _FOCUS_V2_COMPILER_SYSTEM,
) -> tuple[dict[str, Any] | None, str, bool, dict[str, Any]]:
    payload = _llm_payload_from_stats(stats)
    if _estimated_tokens(payload) <= _LLM_INPUT_SOFT_TOKEN_LIMIT:
        result, raw, failed = _call_text_model_with_json_retry(
            system,
            payload,
            model=model,
            max_tokens=2200,
        )
        return result, raw, failed, payload

    merged: dict[str, Any] = {"patterns_proposed": [], "content_profile_updates": {}}
    raw_parts: list[str] = []
    failed_any = False
    payloads: list[dict[str, Any]] = []
    for media in ("reel", "image", "carousel"):
        scoped = _llm_payload_from_stats(stats, format_filter=media)
        if not scoped["compiled_stats"]["pattern_candidates"]:
            continue
        result, raw, failed = _call_text_model_with_json_retry(
            system,
            scoped,
            model=model,
            max_tokens=1600,
        )
        payloads.append(scoped)
        raw_parts.append(raw)
        failed_any = failed_any or failed
        if not isinstance(result, dict):
            continue
        patterns = result.get("patterns_proposed") if isinstance(result.get("patterns_proposed"), list) else []
        merged["patterns_proposed"].extend(patterns)
        if not merged.get("content_profile_updates") and isinstance(result.get("content_profile_updates"), dict):
            merged["content_profile_updates"] = result.get("content_profile_updates") or {}
    return merged, "\n".join(raw_parts), failed_any, {"format_chunked_payloads": payloads}


_HEDGE_RE = re.compile(
    r"\b(may|could|likely|perhaps|suggests|appears|seems|somewhat|fairly|quite|tends\s+to|broadly|generally)\b",
    re.IGNORECASE,
)
_METRIC_TEXT_RE = re.compile(r"(\d|%|\bpp\b|\bx\b|\bK\b|\bM\b)")
_SIGNATURE_TEXT_BUDGETS = {
    "what_happens": {"min": 4, "max": 30},
    "hook_style": {"min": 4, "max": 16},
    "production_style": {"min": 4, "max": 16},
    "voice_tone": {"min": 1, "max": 12},
}


def _clean_language(
    value: Any,
    fallback: str = "",
    *,
    min_words: int = 0,
    max_words: int | None = None,
) -> tuple[str, bool]:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    invalid = False

    def violates(candidate: str) -> bool:
        if _HEDGE_RE.search(candidate) or _METRIC_TEXT_RE.search(candidate):
            return True
        if min_words and _word_count(candidate) < min_words:
            return True
        return False

    if violates(text):
        invalid = True
        text = fallback
        if violates(text):
            text = ""
    if max_words is not None and text and _word_count(text) > max_words:
        invalid = True
        text = " ".join(text.split()[:max_words])
    return text, invalid


def _clean_language_list(
    values: Any,
    *,
    limit: int,
    fallback: list[str] | None = None,
    min_words: int = 0,
    max_words: int | None = None,
) -> tuple[list[str], bool]:
    source = values if isinstance(values, list) and values else (fallback or [])
    cleaned: list[str] = []
    invalid = False
    for idx, value in enumerate(source[:limit]):
        fallback_value = (fallback or [""])[idx] if fallback and idx < len(fallback) else ""
        text, bad = _clean_language(value, fallback_value, min_words=min_words, max_words=max_words)
        invalid = invalid or bad
        if text:
            cleaned.append(text)
    return cleaned, invalid


def _sanitize_language_tree(value: Any) -> Any:
    if isinstance(value, str):
        cleaned, _ = _clean_language(value, "")
        return cleaned
    if isinstance(value, list):
        return [item for item in (_sanitize_language_tree(item) for item in value) if item not in ("", [], {})]
    if isinstance(value, dict):
        return {str(key): _sanitize_language_tree(item) for key, item in value.items()}
    return value


def _clean_profile_production(value: Any) -> dict[str, Any]:
    source = value if isinstance(value, dict) else {}
    by_format_in = source.get("by_format") if isinstance(source.get("by_format"), dict) else {}
    by_format: dict[str, str] = {}
    for media in ("reel", "carousel", "image"):
        text, _ = _clean_language(by_format_in.get(media), "", max_words=14)
        if text:
            by_format[media] = text
    human_presence, _ = _clean_language(source.get("human_presence"), "", max_words=10)
    return {"by_format": by_format, "human_presence": human_presence}


def _focus_v2_version_stale(compile_meta: Any) -> bool:
    meta = compile_meta if isinstance(compile_meta, dict) else {}
    return (
        meta.get("focus_schema_version") != _FOCUS_SCHEMA_VERSION
        or meta.get("validator_version") != _VALIDATOR_VERSION
        or meta.get("compiler_prompt_version") != _FOCUS_V2_COMPILER_PROMPT_VERSION
    )


def _fallback_label(summary: str) -> str:
    terms = [term for term in _pattern_terms(summary) if not term.isdigit()]
    if not terms:
        return "Content Pattern"
    if len(terms) == 1:
        return f"{terms[0].title()} Pattern"
    return " ".join(term.title() for term in terms[:3])


def _fallback_signature(summary: str) -> dict[str, Any]:
    safe_summary = _clean_language(summary, "", min_words=4, max_words=30)[0] or "Observable structure defines this content pattern."
    return {
        "what_happens": safe_summary,
        "hook_style": "Opening device carries the first visible premise.",
        "production_style": "Production approach stays consistent across the pattern.",
        "voice_tone": "Neutral observational register.",
        "key_craft_moves": ["Primary craft move repeats"],
        "not_this": ["Not a loose format match"],
    }


def _candidate_text(candidate: dict[str, Any]) -> str:
    return " ".join([
        str(candidate.get("summary_seed") or ""),
        str(candidate.get("fingerprint_summary") or ""),
    ])


def _merge_recent_tweaks(previous: dict[str, Any], new_tweak: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    values = [item for item in (previous.get("recent_tweaks") or []) if isinstance(item, dict)]
    if new_tweak and new_tweak.get("tweak"):
        values.insert(0, new_tweak)
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in values:
        key = str(item.get("tweak") or "")
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped[:3]


def _build_pattern_registry(
    *,
    stats: dict[str, Any],
    llm_result: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    candidates = {
        str(item.get("candidate_id") or ""): item
        for item in (stats.get("pattern_candidates") or [])
        if isinstance(item, dict) and item.get("candidate_id")
    }
    previous = {
        str(item.get("pattern_id") or ""): item
        for item in (stats.get("previous_registry") or [])
        if isinstance(item, dict) and item.get("pattern_id")
    }
    proposals = (
        llm_result.get("patterns_proposed")
        if isinstance(llm_result, dict) and isinstance(llm_result.get("patterns_proposed"), list)
        else []
    )
    proposals_by_candidate = {
        str(item.get("candidate_id") or ""): item
        for item in proposals
        if isinstance(item, dict) and item.get("candidate_id")
    }
    invalid_language = 0
    rejected_matches = 0
    registry: list[dict[str, Any]] = []
    used_ids: set[str] = set()

    for candidate_id, candidate in candidates.items():
        proposal = proposals_by_candidate.get(candidate_id) or {}
        candidate_summary = str(candidate.get("summary_seed") or candidate.get("fingerprint_summary") or "").strip()
        requested_id = str(proposal.get("pattern_id_or_match") or candidate.get("existing_pattern_id") or "NEW").strip()
        existing_pattern = previous.get(requested_id) if requested_id != "NEW" else None
        if existing_pattern and not _valid_existing_pattern_match(_candidate_text(candidate), existing_pattern):
            existing_pattern = None
            requested_id = "NEW"
            rejected_matches += 1
        if existing_pattern:
            pattern_id = str(existing_pattern.get("pattern_id"))
        else:
            pattern_id = _pattern_id(candidate.get("format") or "common", candidate_summary)
        while pattern_id in used_ids:
            pattern_id = f"{pattern_id}_{len(used_ids) + 1}"
        used_ids.add(pattern_id)

        old = previous.get(pattern_id) or existing_pattern or {}
        fallback_summary = str(old.get("summary") or candidate_summary)
        summary, bad = _clean_language(proposal.get("summary"), fallback_summary, min_words=4, max_words=12)
        invalid_language += int(bad)
        if not summary:
            summary = "Observed content pattern repeats."
        label, bad = _clean_language(
            proposal.get("label"),
            str(old.get("label") or _fallback_label(summary)),
            min_words=2,
            max_words=4,
        )
        invalid_language += int(bad)
        if not label:
            label = _fallback_label(summary)
        signature_in = proposal.get("content_signature") if isinstance(proposal.get("content_signature"), dict) else {}
        old_sig = old.get("content_signature") if isinstance(old.get("content_signature"), dict) else {}
        fallback_sig = old_sig or _fallback_signature(summary)
        signature: dict[str, Any] = {}
        for key in ("what_happens", "hook_style", "production_style", "voice_tone"):
            budget = _SIGNATURE_TEXT_BUDGETS[key]
            value, bad = _clean_language(
                signature_in.get(key) or fallback_sig.get(key),
                str(fallback_sig.get(key) or ""),
                min_words=int(budget["min"]),
                max_words=int(budget["max"]),
            )
            signature[key] = value
            invalid_language += int(bad)
        moves, bad = _clean_language_list(
            signature_in.get("key_craft_moves"),
            limit=5,
            fallback=fallback_sig.get("key_craft_moves") or ["Primary craft move repeats"],
            min_words=3,
            max_words=8,
        )
        if len(moves) < 2:
            invalid_language += 1
            moves = [*moves, "Secondary craft behavior repeats"][:2]
        signature["key_craft_moves"] = moves
        invalid_language += int(bad)
        not_this, bad = _clean_language_list(
            signature_in.get("not_this"),
            limit=4,
            fallback=fallback_sig.get("not_this") or ["Not a loose format match"],
            min_words=4,
            max_words=10,
        )
        if len(not_this) < 2:
            invalid_language += 1
            not_this = [*not_this, "Not any post sharing the format"][:2]
        signature["not_this"] = not_this
        invalid_language += int(bad)

        server_fields = candidate.get("server_fields") if isinstance(candidate.get("server_fields"), dict) else {}
        lifecycle = dict(server_fields.get("lifecycle") or {})
        metric_effects = dict(server_fields.get("metric_effects") or {})
        registry.append({
            "pattern_id": pattern_id,
            "format": candidate.get("format") or "common",
            "label": label,
            "summary": summary,
            "content_signature": signature,
            "metric_effects": metric_effects,
            "lifecycle": lifecycle,
            "proof_post_keys": server_fields.get("proof_post_keys") or [],
            "latest_tweak": str(old.get("latest_tweak") or ""),
            "recent_tweaks": _merge_recent_tweaks(old),
        })

    _apply_conflict_status(registry)
    return registry, {
        "invalid_language_fields": invalid_language,
        "not_this_rejected_matches": rejected_matches,
    }


def _apply_conflict_status(registry: list[dict[str, Any]]) -> None:
    for idx, left in enumerate(registry):
        left_status = str((left.get("lifecycle") or {}).get("status") or "")
        if left_status != "stable":
            continue
        for right in registry[idx + 1:]:
            if left.get("format") != right.get("format"):
                continue
            right_status = str((right.get("lifecycle") or {}).get("status") or "")
            if right_status != "stable":
                continue
            left_positive, left_negative = _boundary_scores(_positive_signature_text(right), left)
            right_positive, right_negative = _boundary_scores(_positive_signature_text(left), right)
            if left_negative >= left_positive and right_negative >= right_positive:
                left.setdefault("lifecycle", {})["status"] = "conflict"
                right.setdefault("lifecycle", {})["status"] = "conflict"


def _compute_derived_views(pattern_registry: list[dict[str, Any]]) -> dict[str, Any]:
    top: list[str] = []
    bottom: list[str] = []
    media = {
        "reel": {"best": [], "worst": []},
        "image": {"best": [], "worst": []},
        "carousel": {"best": [], "worst": []},
    }
    engagement = {
        "follower_spiking": [],
        "follower_dropping": [],
        "view_spiking": [],
        "comment_spiking": [],
        "like_heavy": [],
    }
    for pattern in pattern_registry:
        pid = str(pattern.get("pattern_id") or "")
        if not pid:
            continue
        fmt = _media_key(pattern.get("format"))
        effects = pattern.get("metric_effects") if isinstance(pattern.get("metric_effects"), dict) else {}
        confidence = effects.get("confidence") if isinstance(effects.get("confidence"), dict) else {}
        confidence_bucket = str(confidence.get("bucket") or "low")
        vs = _num(effects.get("vs_baseline_pp"))
        status = str((pattern.get("lifecycle") or {}).get("status") or "")
        if confidence_bucket in {"medium", "high"} and status in {"stable", "strengthening"} and vs is not None and vs <= -10:
            top.append(pid)
            media.setdefault(fmt, {"best": [], "worst": []})["best"].append(pid)
        if confidence_bucket in {"medium", "high"} and status in {"weakening", "decaying"} and vs is not None and vs >= 10:
            bottom.append(pid)
            media.setdefault(fmt, {"best": [], "worst": []})["worst"].append(pid)
        associated = effects.get("associated_metrics") if isinstance(effects.get("associated_metrics"), dict) else {}
        if _num(associated.get("views_x_avg")) is not None and float(associated["views_x_avg"]) >= 2.0:
            engagement["view_spiking"].append(pid)
        if _num(associated.get("comments_x_avg")) is not None and float(associated["comments_x_avg"]) >= 2.0:
            engagement["comment_spiking"].append(pid)
        if _num(associated.get("likes_x_avg")) is not None and float(associated["likes_x_avg"]) >= 2.0:
            engagement["like_heavy"].append(pid)
    return {
        "top_performers": top,
        "bottom_performers": bottom,
        "media_strategies": media,
        "engagement_associations": engagement,
    }


def _build_delta_log(previous_registry: list[dict[str, Any]], new_registry: list[dict[str, Any]], *, fallback: str | None = None, validator_notes: dict[str, Any] | None = None) -> dict[str, Any]:
    prev = {str(item.get("pattern_id") or ""): item for item in previous_registry if isinstance(item, dict)}
    new = {str(item.get("pattern_id") or ""): item for item in new_registry if isinstance(item, dict)}
    promoted: list[dict[str, Any]] = []
    demoted: list[dict[str, Any]] = []
    fresh_tweaks: list[dict[str, Any]] = []
    for pid, item in new.items():
        if pid not in prev:
            continue
        old_status = str((prev[pid].get("lifecycle") or {}).get("status") or "")
        new_status = str((item.get("lifecycle") or {}).get("status") or "")
        if _lifecycle_rank(new_status) > _lifecycle_rank(old_status):
            promoted.append({"pattern_id": pid, "from": old_status, "to": new_status})
        elif _lifecycle_rank(new_status) < _lifecycle_rank(old_status):
            demoted.append({"pattern_id": pid, "from": old_status, "to": new_status})
        if item.get("latest_tweak") and item.get("latest_tweak") != prev[pid].get("latest_tweak"):
            fresh_tweaks.append({"pattern_id": pid, "tweak": item.get("latest_tweak")})
    out = {
        "added_patterns": [item for pid, item in new.items() if pid not in prev],
        "promoted": promoted,
        "demoted": demoted,
        "archived": [item for pid, item in prev.items() if pid not in new],
        "fresh_tweaks": fresh_tweaks,
        "metric_trend_changes": [],
    }
    if fallback:
        out["compiler_fallback"] = fallback
    if validator_notes:
        out["validator_notes"] = validator_notes
    return out


def _legacy_focus_from_v2(pattern_registry: list[dict[str, Any]], content_profile: dict[str, Any]) -> dict[str, str]:
    active = [
        item for item in pattern_registry
        if str((item.get("lifecycle") or {}).get("status") or "") in {"stable", "strengthening", "emerging", "conflict"}
    ]
    ordered = sorted(
        active,
        key=lambda item: (
            _lifecycle_rank((item.get("lifecycle") or {}).get("status")),
            _num(((item.get("metric_effects") or {}).get("confidence") or {}).get("score")) or 0,
        ),
        reverse=True,
    )

    def sentence(pattern: dict[str, Any]) -> str:
        label = str(pattern.get("label") or "Pattern")
        summary = str(pattern.get("summary") or "").strip()
        return f"{label}: {summary}" if summary else label

    common_lines = [sentence(item) for item in ordered[:4]]
    notes = content_profile.get("evolution_notes") if isinstance(content_profile.get("evolution_notes"), list) else []
    common = " ".join([*common_lines, *[str(note) for note in notes[:1] if note]])
    result = {"focus_md_common": _cap_words(common, _FEEDER_COMMON_WORD_LIMIT, "feeder.v2.common")[0]}
    for media in ("reel", "image", "carousel"):
        lines = [sentence(item) for item in ordered if _media_key(item.get("format")) == media][:4]
        result[f"focus_md_{media}"] = _cap_words(" ".join(lines), _FEEDER_FORMAT_WORD_LIMIT, f"feeder.v2.{media}")[0]
    return result


def _build_compile_meta(
    *,
    entity: dict[str, Any],
    version: int,
    model: str,
    compile_kind: str,
    started_at: datetime,
    stats: dict[str, Any],
    llm_payload: dict[str, Any],
    llm_raw: str,
    validated_payload: dict[str, Any],
    prompt: str = _FOCUS_V2_COMPILER_SYSTEM,
) -> dict[str, Any]:
    return {
        **entity,
        "version": version,
        "compiled_at": datetime.now(timezone.utc).isoformat(),
        "focus_schema_version": _FOCUS_SCHEMA_VERSION,
        "stats_builder_version": _STATS_BUILDER_VERSION,
        "validator_version": _VALIDATOR_VERSION,
        "compiler_prompt_version": _FOCUS_V2_COMPILER_PROMPT_VERSION,
        "model_version": model,
        "compiled_stats_hash": _sha(stats),
        "llm_input_hash": _sha({"prompt": prompt, "payload": llm_payload}),
        "llm_output_hash": _sha(llm_raw),
        "validated_output_hash": _sha(validated_payload),
        "compile_kind": compile_kind,
        "compile_duration_ms": int((datetime.now(timezone.utc) - started_at).total_seconds() * 1000),
    }


def _build_feeder_focus_v2(
    *,
    conn: Any,
    feeder: dict[str, Any],
    rows: list[dict[str, Any]],
    current: dict[str, Any],
    legacy_result: dict[str, Any],
    evidence_buckets: list[dict[str, Any]],
    memory_candidates: list[dict[str, Any]],
    model: str,
    compile_kind: str,
    started_at: datetime,
    version: int,
) -> dict[str, Any]:
    stats = _stats_builder_output(
        conn=conn,
        feeder=feeder,
        rows=rows,
        current=current,
        result=legacy_result,
        evidence_buckets=evidence_buckets,
        memory_candidates=memory_candidates,
    )
    llm_result, llm_raw, json_failed, llm_payload = _compile_v2_language(stats, model=model)
    previous_registry = stats.get("previous_registry") if isinstance(stats.get("previous_registry"), list) else []
    registry, validator_notes = _build_pattern_registry(stats=stats, llm_result=llm_result)
    language_retry_json_failed = False
    language_retry_count = 0
    while int(validator_notes.get("invalid_language_fields") or 0) > 0 and not json_failed and language_retry_count < 2:
        language_retry_count += 1
        retry_system = (
            f"{_FOCUS_V2_COMPILER_SYSTEM}\n\n"
            "Your previous output violated field budgets, used forbidden hedging, or used metric-like text. "
            "Rewrite every user-visible string with the required minimum detail, without digits, percentages, "
            "multipliers, pp, K, M, or hedge words."
        )
        retry_result, retry_raw, retry_failed, retry_payload = _compile_v2_language(stats, model=model, system=retry_system)
        retry_registry, retry_notes = _build_pattern_registry(stats=stats, llm_result=retry_result)
        if int(retry_notes.get("invalid_language_fields") or 0) < int(validator_notes.get("invalid_language_fields") or 0):
            llm_result = retry_result
            llm_payload = retry_payload
            registry = retry_registry
            validator_notes = retry_notes
        llm_raw = "\n".join(part for part in (llm_raw, retry_raw) if part)
        language_retry_json_failed = language_retry_json_failed or retry_failed
    if language_retry_count:
        validator_notes["invalid_language_retry_count"] = language_retry_count
    if language_retry_json_failed:
        validator_notes["invalid_language_retry_json_failed"] = True
    content_updates = (
        llm_result.get("content_profile_updates")
        if isinstance(llm_result, dict) and isinstance(llm_result.get("content_profile_updates"), dict)
        else {}
    )
    content_profile = _build_content_profile(legacy_result, rows, content_updates)
    metric_profile = stats.get("metric_profile") or _build_metric_profile(conn, int(feeder.get("feeder_id") or 0), rows)
    derived = _compute_derived_views(registry)
    fallback = "json_parse" if json_failed else "invalid_language" if int(validator_notes.get("invalid_language_fields") or 0) > 0 else None
    delta = _build_delta_log(previous_registry, registry, fallback=fallback, validator_notes=validator_notes)
    validated = {
        "content_profile": content_profile,
        "metric_profile": metric_profile,
        "pattern_registry": registry,
        "derived_views": derived,
        "delta_log": delta,
    }
    compile_meta = _build_compile_meta(
        entity={
            "feeder_id": int(feeder.get("feeder_id") or 0),
            "handle": feeder.get("handle"),
            "role": feeder.get("role"),
            "window_post_count": (stats.get("meta") or {}).get("window_post_count") or {},
        },
        version=version,
        model=model,
        compile_kind=compile_kind,
        started_at=started_at,
        stats=stats,
        llm_payload=llm_payload,
        llm_raw=llm_raw,
        validated_payload=validated,
    )
    return {**validated, "compile_meta": compile_meta, "legacy_focus": _legacy_focus_from_v2(registry, content_profile)}


def _fetch_post_context(conn: Any, post_key: str) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select p.post_key, p.caption, p.post_url, p.posted_at,
                   lower(coalesce(p.media_type, 'image')) as media_type,
                   fd.id as feeder_id, fd.feed_id, fd.handle,
                   coalesce(fd.role, 'standard') as feeder_role,
                   f.context_bible
            from public.posts p
            join public.feeders fd on fd.id = p.feeder_id
            join public.feeds f on f.id = fd.feed_id
            where p.post_key = %s
            limit 1
            """,
            (post_key,),
        )
        return cur.fetchone()


def _serialize_focus_v2_slice(row: dict[str, Any], media_key: str, *, max_tokens: int = 2000) -> str:
    content = row.get("content_profile") if isinstance(row.get("content_profile"), dict) else {}
    metric = row.get("metric_profile") if isinstance(row.get("metric_profile"), dict) else {}
    registry = row.get("pattern_registry") if isinstance(row.get("pattern_registry"), list) else []
    active_patterns = []
    for pattern in registry:
        if not isinstance(pattern, dict) or _media_key(pattern.get("format")) != media_key:
            continue
        status = str((pattern.get("lifecycle") or {}).get("status") or "")
        if status in {"archived", "decaying"}:
            continue
        effects = pattern.get("metric_effects") if isinstance(pattern.get("metric_effects"), dict) else {}
        confidence = effects.get("confidence") if isinstance(effects.get("confidence"), dict) else {}
        active_patterns.append({
            "pattern_id": pattern.get("pattern_id"),
            "label": pattern.get("label"),
            "summary": pattern.get("summary"),
            "status": status,
            "confidence": confidence.get("bucket"),
            "recent_tweaks": pattern.get("recent_tweaks") or [],
        })
    payload = {
        "content_profile": {
            "voice": content.get("voice") if isinstance(content.get("voice"), dict) else {},
            "production_for_media": ((content.get("production") or {}).get("by_format") or {}).get(media_key)
            if isinstance(content.get("production"), dict)
            else {},
            "evolution_notes": content.get("evolution_notes") or [],
        },
        "media_metric_context": {
            "trend": [
                item for item in (metric.get("metric_trends") or [])
                if isinstance(item, dict) and str(item.get("dimension") or "").startswith(f"{media_key}.")
            ],
        },
        "patterns": active_patterns[:8],
    }
    text = json.dumps(payload, ensure_ascii=True, default=str)
    while _estimated_tokens(text) > max_tokens and active_patterns:
        active_patterns.pop()
        payload["patterns"] = active_patterns
        text = json.dumps(payload, ensure_ascii=True, default=str)
    return text


def feeder_focus_slice(conn: Any, feeder_id: int, media_type: Any) -> dict[str, Any]:
    key = _media_key(media_type)
    field = {
        "reel": "focus_md_reel",
        "image": "focus_md_image",
        "carousel": "focus_md_carousel",
    }[key]
    v2_columns = ", content_profile, metric_profile, pattern_registry" if FOCUS_V2_SLICES_ENABLED else ""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            select focus_version, focus_md_common, {field} as focus_md{v2_columns}
            from public.feeder_focus
            where feeder_id = %s
            limit 1
            """,
            (feeder_id,),
        )
        row = cur.fetchone()
    if not row:
        return {"version": 0, "text": "", "media_key": key}
    if FOCUS_V2_SLICES_ENABLED and isinstance(row.get("pattern_registry"), list):
        text = _serialize_focus_v2_slice(dict(row), key)
        if text.strip():
            return {
                "version": int(row.get("focus_version") or 0),
                "text": text,
                "media_key": key,
                "source": "v2",
            }
    parts = [str(row.get("focus_md_common") or "").strip(), str(row.get("focus_md") or "").strip()]
    return {
        "version": int(row.get("focus_version") or 0),
        "text": "\n".join(part for part in parts if part),
        "media_key": key,
        "source": "legacy",
    }


def ensure_post_focus_read(conn: Any, post_key: str, fingerprint: dict[str, Any]) -> dict[str, Any]:
    post = _fetch_post_context(conn, post_key)
    if not post:
        return {}
    focus = feeder_focus_slice(conn, int(post.get("feeder_id") or 0), post.get("media_type"))
    if int(focus.get("version") or 0) <= 0 or not str(focus.get("text") or "").strip():
        # First-week behavior: fingerprints can exist before feeder focus exists.
        # Do not persist a fake alignment read; the card layer will treat focus as unavailable.
        return {}
    fingerprint_hash = _sha(fingerprint)
    model_version = f"{_provider() or 'disabled'}:{_runtime_model()}:{_FOCUS_READ_PROMPT_VERSION}"
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select focus_read
            from public.post_focus_reads
            where post_key = %s
              and feeder_focus_version = %s
              and fingerprint_hash = %s
              and model_version = %s
              and prompt_version = %s
            limit 1
            """,
            (post_key, focus["version"], fingerprint_hash, model_version, _FOCUS_READ_PROMPT_VERSION),
        )
        existing = cur.fetchone()
    if existing and isinstance(existing.get("focus_read"), dict):
        return existing["focus_read"]

    focus_read = _call_text_model(
        _FOCUS_READ_SYSTEM,
        {
            "feeder_focus": focus["text"],
            "post": {
                "handle": post.get("handle"),
                "media_type": post.get("media_type"),
                "caption_excerpt": str(post.get("caption") or "")[:800],
                "fingerprint": fingerprint,
            },
        },
        model=_runtime_model(),
        max_tokens=500,
    ) or {}

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.post_focus_reads (
              post_key, feeder_id, feed_id, media_type, focus_read,
              feeder_focus_version, fingerprint_hash, prompt_version, model_version,
              generated_at, updated_at
            )
            values (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, now(), now())
            on conflict (post_key) do update set
              feeder_id = excluded.feeder_id,
              feed_id = excluded.feed_id,
              media_type = excluded.media_type,
              focus_read = excluded.focus_read,
              feeder_focus_version = excluded.feeder_focus_version,
              fingerprint_hash = excluded.fingerprint_hash,
              prompt_version = excluded.prompt_version,
              model_version = excluded.model_version,
              updated_at = now()
            """,
            (
                post_key,
                int(post.get("feeder_id") or 0),
                int(post.get("feed_id") or 0),
                post.get("media_type"),
                json.dumps(focus_read),
                focus["version"],
                fingerprint_hash,
                _FOCUS_READ_PROMPT_VERSION,
                model_version,
            ),
        )
    conn.commit()
    return focus_read


def store_post_focus_read(
    conn: Any,
    *,
    post_key: str,
    feeder_id: int,
    feed_id: int,
    media_type: Any,
    fingerprint: dict[str, Any],
    focus_read: dict[str, Any],
    feeder_focus_version: int,
    model_version: str,
) -> None:
    if not post_key or not isinstance(focus_read, dict):
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.post_focus_reads (
              post_key, feeder_id, feed_id, media_type, focus_read,
              feeder_focus_version, fingerprint_hash, prompt_version, model_version,
              generated_at, updated_at
            )
            values (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, now(), now())
            on conflict (post_key) do update set
              feeder_id = excluded.feeder_id,
              feed_id = excluded.feed_id,
              media_type = excluded.media_type,
              focus_read = excluded.focus_read,
              feeder_focus_version = excluded.feeder_focus_version,
              fingerprint_hash = excluded.fingerprint_hash,
              prompt_version = excluded.prompt_version,
              model_version = excluded.model_version,
              updated_at = now()
            """,
            (
                post_key,
                feeder_id,
                feed_id,
                str(media_type or ""),
                json.dumps(focus_read),
                feeder_focus_version,
                _sha(fingerprint),
                _FOCUS_READ_PROMPT_VERSION,
                model_version,
            ),
        )


def _role_line(signal: dict[str, Any], posts: list[dict[str, Any]]) -> str:
    scope = str(signal.get("scope") or "")
    signal_type = str(signal.get("signal_type") or "")
    handles = sorted({str(row.get("handle") or "").strip() for row in posts if row.get("handle")})
    if scope == "cross":
        return "Role: cross-feed signal; explain the shared behavior across involved feeders."
    if scope == "anchor":
        if signal_type == "ANCHOR_CHALLENGER_SURGE":
            return "Role: challenger-vs-anchor signal; explain what the challenger is doing that the anchor should learn from or avoid."
        return "Role: anchor comparison; explain the gap between anchor and non-anchor content."
    if handles:
        return f"Role: @{handles[0]} is the account being evaluated; frame advice as account-specific."
    return "Role: account-level signal; frame advice as account-specific."


def feed_focus_context(conn: Any, signal: dict[str, Any], posts: list[dict[str, Any]]) -> dict[str, Any]:
    media = _media_key(signal.get("media_type") or (posts[0].get("media_type") if posts else None))
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select focus_version, capsule_common, capsule_reel, capsule_image,
                   capsule_carousel, capsule_anchor, capsule_cross
            from public.feed_focus
            where feed_id = %s
            limit 1
            """,
            (int(signal.get("feed_id") or 0),),
        )
        row = cur.fetchone()
    if not row:
        return {
            "feed_focus_version": 0,
            "common": signal.get("context_bible") or "",
            "format": "",
            "scope": "",
            "role": _role_line(signal, posts),
        }
    format_capsule = {
        "reel": row.get("capsule_reel"),
        "image": row.get("capsule_image"),
        "carousel": row.get("capsule_carousel"),
    }.get(media, "")
    scope = str(signal.get("scope") or "")
    scope_capsule = row.get("capsule_anchor") if scope == "anchor" else row.get("capsule_cross") if scope == "cross" else ""
    return {
        "feed_focus_version": int(row.get("focus_version") or 0),
        "common": row.get("capsule_common") or signal.get("context_bible") or "",
        "format": format_capsule or "",
        "scope": scope_capsule or "",
        "role": _role_line(signal, posts),
    }


def _fetch_feeder_evidence(conn: Any, feeder_id: int) -> tuple[dict[str, Any] | None, list[dict[str, Any]], list[dict[str, Any]]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select fd.id as feeder_id, fd.feed_id, fd.handle, coalesce(fd.role, 'standard') as role,
                   f.name as feed_name, f.context_bible
            from public.feeders fd
            join public.feeds f on f.id = fd.feed_id
            where fd.id = %s
            limit 1
            """,
            (feeder_id,),
        )
        feeder = cur.fetchone()
        if not feeder:
            return None, [], []
        cur.execute(
            """
            select p.post_key, p.post_url, p.caption, p.posted_at,
                   lower(coalesce(p.media_type, 'image')) as media_type,
                   coalesce(pm.percentile_performance_exact, pm.percentile_performance::numeric) as percentile,
                   pm.views, pm.likes, pm.comments,
                   pm.views_multiple, pm.likes_multiple, pm.comments_multiple,
                   coalesce(d21.percentile_performance_exact, d21.percentile_performance::numeric) as d21_percentile,
                   d21.views_multiple as d21_views_multiple,
                   d21.likes_multiple as d21_likes_multiple,
                   d21.comments_multiple as d21_comments_multiple,
                   pf.fingerprint, pf.media_source_hash, pf.media_confidence,
                   pfr.focus_read,
                   array(
                     select distinct s.signal_type
                     from public.signal_posts sp
                     join public.signals s on s.id = sp.signal_id
                     where sp.post_key = p.post_key
                       and s.created_at >= now() - interval '90 days'
                   ) as signal_types
            from public.posts p
            join public.post_metrics pm on pm.post_key = p.post_key and pm.checkpoint = 'd7'
            left join public.post_metrics d21 on d21.post_key = p.post_key and d21.checkpoint = 'd21'
            left join public.post_fingerprints pf on pf.post_key = p.post_key
            left join public.post_focus_reads pfr on pfr.post_key = p.post_key
            where p.feeder_id = %s
              and p.posted_at >= now() - interval '90 days'
              and pm.percentile_performance is not null
            order by p.posted_at desc nulls last, pm.percentile_performance asc
            """,
            (feeder_id,),
        )
        rows = cur.fetchall()
    evidence = _valid_focus_evidence_rows([dict(row) for row in rows])
    all_evidence = list(evidence)
    if len(evidence) > _MAX_EVIDENCE_POSTS:
        recent = evidence[:40]
        best = sorted(evidence, key=lambda row: row.get("percentile") or 999)[:60]
        worst = sorted(evidence, key=lambda row: row.get("percentile") or -1, reverse=True)[:40]
        selected: dict[str, dict[str, Any]] = {}
        for row in [*recent, *best, *worst]:
            selected[str(row.get("post_key"))] = row
        evidence = list(selected.values())[:_MAX_EVIDENCE_POSTS]
    return dict(feeder), evidence, all_evidence


def _fetch_current_feeder_focus(conn: Any, feeder_id: int) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("select * from public.feeder_focus where feeder_id = %s limit 1", (feeder_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _full_rebuild_due(current: dict[str, Any]) -> bool:
    if not current:
        return True
    last = _parse_dt(current.get("last_full_rebuild_at"))
    if not last:
        return True
    interval_days = max(1, int(FOCUS_REBUILD_INTERVAL_DAYS or 45))
    return datetime.now(timezone.utc) - last >= timedelta(days=interval_days)


def _claim_focus_compile_lock(conn: Any, scope: str, entity_id: int, *, force: bool = False) -> bool:
    interval = timedelta(seconds=max(3600, int(FOCUS_BRAIN_COMPILE_INTERVAL_SECONDS or 604800)))
    lock_window = timedelta(minutes=30)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            insert into public.focus_compile_locks (
              scope, entity_id, locked_until, last_attempt_at, attempt_count, created_at, updated_at
            )
            values (%s, %s, now() + %s, now(), 1, now(), now())
            on conflict (scope, entity_id) do update set
              locked_until = excluded.locked_until,
              last_attempt_at = now(),
              attempt_count = public.focus_compile_locks.attempt_count + 1,
              updated_at = now()
            where
              (public.focus_compile_locks.locked_until is null or public.focus_compile_locks.locked_until < now())
              and (
                %s
                or public.focus_compile_locks.last_success_at is null
                or public.focus_compile_locks.last_success_at <= now() - %s
              )
            returning scope
            """,
            (scope, entity_id, lock_window, force, interval),
        )
        row = cur.fetchone()
    conn.commit()
    return bool(row)


def _mark_focus_compile_lock(
    conn: Any,
    scope: str,
    entity_id: int,
    *,
    success: bool,
    error: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.focus_compile_locks
            set locked_until = null,
                last_success_at = case when %s then now() else last_success_at end,
                last_error = case when %s then null else left(%s, 1000) end,
                updated_at = now()
            where scope = %s and entity_id = %s
            """,
            (success, success, error or "", scope, entity_id),
        )
    conn.commit()


def _fetch_stage_b_candidates(conn: Any, *, feed_id: int, feeder_id: int | None = None) -> list[dict[str, Any]]:
    params: list[Any] = [feed_id]
    feeder_join = ""
    feeder_where = ""
    if feeder_id is not None:
        feeder_join = """
            join public.signal_posts sp on sp.signal_id = s.id
            join public.posts p on p.post_key = sp.post_key
        """
        feeder_where = "and p.feeder_id = %s"
        params.append(feeder_id)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            select distinct s.id as signal_id, s.signal_type, s.scope, s.media_type,
                   s.business_date_ist, si.focus_memory_candidate
            from public.signal_intelligence si
            join public.signals s on s.id = si.signal_id
            {feeder_join}
            where s.feed_id = %s
              {feeder_where}
              and si.updated_at >= now() - interval '30 days'
              and si.focus_memory_candidate <> '{{}}'::jsonb
            order by s.business_date_ist desc nulls last, s.id desc
            limit 40
            """,
            tuple(params),
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def compile_feeder_focus(conn: Any, feeder_id: int | None = None, *, limit: int = 20, full_rebuild: bool = False) -> dict[str, int]:
    with conn.cursor(row_factory=dict_row) as cur:
        if feeder_id is not None:
            cur.execute("select id from public.feeders where id = %s", (feeder_id,))
        else:
            cur.execute(
                """
                select fd.id
                from public.feeders fd
                join public.feeds f on f.id = fd.feed_id
                where fd.status = 'active' and f.status = 'active'
                order by coalesce(fd.updated_at, fd.created_at) desc nulls last, fd.id desc
                limit %s
                """,
                (max(1, limit),),
            )
        feeder_ids = [int(row["id"]) for row in cur.fetchall()]

    selected = len(feeder_ids)
    compiled = 0
    skipped = 0
    failed = 0
    empty_evidence = 0
    for fid in feeder_ids:
        if not _claim_focus_compile_lock(conn, "feeder", fid, force=full_rebuild):
            skipped += 1
            _focus_log("compile_lock_skip", scope="feeder", entity_id=fid)
            continue
        try:
            started_at = datetime.now(timezone.utc)
            feeder, evidence, all_evidence = _fetch_feeder_evidence(conn, fid)
            if not feeder or not evidence:
                skipped += 1
                empty_evidence += 1
                _mark_focus_compile_lock(
                    conn,
                    "feeder",
                    fid,
                    success=False,
                    error="empty_fingerprint_evidence_waiting_for_first_focus",
                )
                _focus_log("compile_skip_empty_evidence", scope="feeder", entity_id=fid)
                continue
            current = _fetch_current_feeder_focus(conn, fid) or {}
            rebuild_now = bool(full_rebuild or _full_rebuild_due(current))
            compile_kind = "full_rebuild" if rebuild_now else "weekly_update"
            model = FOCUS_REBUILD_MODEL if rebuild_now else FOCUS_COMPILER_MODEL
            previous_patterns = current.get("structured_patterns") or {}
            metric_windows = _metric_windows(all_evidence)
            server_pattern_stats = _pattern_stat_rows(previous_patterns, all_evidence)
            memory_candidates = _aggregate_memory_candidates(_fetch_stage_b_candidates(
                conn,
                feed_id=int(feeder.get("feed_id") or 0),
                feeder_id=fid,
            ))
            evidence_signature = [
                {
                    "post_key": row.get("post_key"),
                    "posted_at": row.get("posted_at"),
                    "media_type": row.get("media_type"),
                    "percentile": row.get("percentile"),
                    "fingerprint_hash": _sha(row.get("fingerprint") or {}),
                    "focus_read_hash": _sha(row.get("focus_read") or {}),
                    "signal_types": row.get("signal_types") or [],
                }
                for row in all_evidence
            ]
            source_payload = {
                "mode": compile_kind,
                "feeder": feeder,
                "previous_focus": {
                    "structured_patterns": previous_patterns,
                    "focus_md_common": current.get("focus_md_common") or "",
                    "focus_md_reel": current.get("focus_md_reel") or "",
                    "focus_md_image": current.get("focus_md_image") or "",
                    "focus_md_carousel": current.get("focus_md_carousel") or "",
                },
                "metric_windows": metric_windows,
                "server_pattern_stats": server_pattern_stats,
                "evidence_population": {
                    "fingerprinted_90d": len(all_evidence),
                    "sampled_for_llm": len(evidence),
                },
                "evidence_signature": evidence_signature,
                "stage_b_memory_candidates": memory_candidates,
            }
            source_hash = _sha(source_payload)
            v2_version_stale = _focus_v2_version_stale(current.get("compile_meta"))
            if not rebuild_now and not v2_version_stale and current.get("source_hash") == source_hash:
                skipped += 1
                _mark_focus_compile_lock(conn, "feeder", fid, success=True)
                _focus_log(
                    "source_hash_hit",
                    scope="feeder",
                    entity_id=fid,
                    evidence_count=len(evidence),
                    full_evidence_count=len(all_evidence),
                )
                continue
            evidence_buckets = _bucket_evidence_for_compiler(evidence)
            payload = {
                **source_payload,
                "evidence_buckets": evidence_buckets,
            }
            patterns_before = len((previous_patterns.get("patterns") if isinstance(previous_patterns, dict) else []) or [])
            result = _call_text_model(_FEEDER_COMPILER_SYSTEM, payload, model=model, max_tokens=2600)
            if not result:
                failed += 1
                _mark_focus_compile_lock(conn, "feeder", fid, success=False, error="model_failed_or_malformed")
                _focus_log("compile_failed", scope="feeder", entity_id=fid, reason="model_failed_or_malformed")
                continue
            result = _cap_focus_texts(result, kind="feeder")
            raw_patterns = result.get("structured_patterns") or {}
            structured_patterns = _normalize_patterns(raw_patterns)
            structured_patterns = _apply_server_pattern_stats(structured_patterns, all_evidence)
            structured_patterns = _normalize_patterns(structured_patterns)
            patterns_after = len((structured_patterns.get("patterns") if isinstance(structured_patterns, dict) else []) or [])
            version = int(current.get("focus_version") or 0) + 1
            v2 = _build_feeder_focus_v2(
                conn=conn,
                feeder=feeder,
                rows=all_evidence,
                current=current,
                legacy_result={**result, "structured_patterns": structured_patterns},
                evidence_buckets=evidence_buckets,
                memory_candidates=memory_candidates,
                model=model,
                compile_kind=compile_kind,
                started_at=started_at,
                version=version,
            )
            legacy_focus = v2.get("legacy_focus") if isinstance(v2.get("legacy_focus"), dict) else {}
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into public.feeder_focus (
                      feeder_id, feed_id, structured_patterns, evidence_summary,
                      focus_md_common, focus_md_reel, focus_md_image, focus_md_carousel,
                      focus_version, prompt_version, model_version, source_hash,
                      content_profile, metric_profile, pattern_registry, derived_views, delta_log, compile_meta,
                      focus_updated_at, last_full_rebuild_at, created_at, updated_at
                    )
                    values (%s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, now(),
                            case when %s then now() else null end, now(), now())
                    on conflict (feeder_id) do update set
                      feed_id = excluded.feed_id,
                      structured_patterns = excluded.structured_patterns,
                      evidence_summary = excluded.evidence_summary,
                      focus_md_common = excluded.focus_md_common,
                      focus_md_reel = excluded.focus_md_reel,
                      focus_md_image = excluded.focus_md_image,
                      focus_md_carousel = excluded.focus_md_carousel,
                      focus_version = excluded.focus_version,
                      prompt_version = excluded.prompt_version,
                      model_version = excluded.model_version,
                      source_hash = excluded.source_hash,
                      content_profile = excluded.content_profile,
                      metric_profile = excluded.metric_profile,
                      pattern_registry = excluded.pattern_registry,
                      derived_views = excluded.derived_views,
                      delta_log = excluded.delta_log,
                      compile_meta = excluded.compile_meta,
                      focus_updated_at = now(),
                      last_full_rebuild_at = case when %s then now() else public.feeder_focus.last_full_rebuild_at end,
                      updated_at = now()
                    """,
                    (
                        fid,
                        int(feeder.get("feed_id") or 0),
                        json.dumps(structured_patterns),
                        json.dumps(result.get("evidence_summary") or {}),
                        str(legacy_focus.get("focus_md_common") or result.get("focus_md_common") or ""),
                        str(legacy_focus.get("focus_md_reel") or result.get("focus_md_reel") or ""),
                        str(legacy_focus.get("focus_md_image") or result.get("focus_md_image") or ""),
                        str(legacy_focus.get("focus_md_carousel") or result.get("focus_md_carousel") or ""),
                        version,
                        _FEEDER_FOCUS_PROMPT_VERSION,
                        model,
                        source_hash,
                        json.dumps(v2.get("content_profile") or {}),
                        json.dumps(v2.get("metric_profile") or {}),
                        json.dumps(v2.get("pattern_registry") or []),
                        json.dumps(v2.get("derived_views") or {}),
                        json.dumps(v2.get("delta_log") or {}),
                        json.dumps(v2.get("compile_meta") or {}),
                        rebuild_now,
                        rebuild_now,
                    ),
                )
            conn.commit()
            _mark_focus_compile_lock(conn, "feeder", fid, success=True)
            _focus_log(
                "compile_success",
                scope="feeder",
                entity_id=fid,
                mode=compile_kind,
                model=model,
                evidence_count=len(evidence),
                full_evidence_count=len(all_evidence),
                bucket_count=len(evidence_buckets),
                patterns_before=patterns_before,
                patterns_after=patterns_after,
                v2_patterns=len(v2.get("pattern_registry") or []),
                word_counts=_capsule_word_counts({**result, **legacy_focus}, ["focus_md_common", "focus_md_reel", "focus_md_image", "focus_md_carousel"]),
            )
            compiled += 1
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            failed += 1
            _mark_focus_compile_lock(conn, "feeder", fid, success=False, error=str(exc))
            _focus_log("compile_failed", scope="feeder", entity_id=fid, reason=str(exc)[:500])
    return {
        "selected": selected,
        "compiled": compiled,
        "skipped": skipped,
        "failed": failed,
        "empty_evidence": empty_evidence,
    }


def _feed_pattern_text(pattern: dict[str, Any]) -> str:
    return " ".join([
        str(pattern.get("label") or ""),
        str(pattern.get("summary") or ""),
        _positive_signature_text(pattern),
    ])


def _feeder_v2_patterns(feeders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for feeder in feeders:
        registry = feeder.get("pattern_registry") if isinstance(feeder.get("pattern_registry"), list) else []
        for pattern in registry:
            if not isinstance(pattern, dict):
                continue
            status = str((pattern.get("lifecycle") or {}).get("status") or "")
            if status in {"archived", "decaying"}:
                continue
            rows.append({
                "feeder_id": int(feeder.get("id") or 0),
                "handle": feeder.get("handle"),
                "role": feeder.get("role"),
                "pattern": pattern,
            })
    return rows


def _cluster_feed_patterns(pattern_rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    clusters: list[list[dict[str, Any]]] = []
    for row in pattern_rows:
        pattern = row.get("pattern") if isinstance(row.get("pattern"), dict) else {}
        fmt = _media_key(pattern.get("format"))
        text = _feed_pattern_text(pattern)
        placed = False
        for cluster in clusters:
            seed = cluster[0].get("pattern") if isinstance(cluster[0].get("pattern"), dict) else {}
            if fmt != _media_key(seed.get("format")):
                continue
            if _pattern_similarity(text, _feed_pattern_text(seed)) >= 0.45:
                cluster.append(row)
                placed = True
                break
        if not placed:
            clusters.append([row])
    return clusters


def _aggregate_metric_effects(patterns: list[dict[str, Any]]) -> dict[str, Any]:
    effects = [
        pattern.get("metric_effects")
        for pattern in patterns
        if isinstance(pattern.get("metric_effects"), dict)
    ]
    associated_rows = [
        effect.get("associated_metrics")
        for effect in effects
        if isinstance(effect.get("associated_metrics"), dict)
    ]
    confidence_scores = [
        _num((effect.get("confidence") or {}).get("score"))
        for effect in effects
        if isinstance(effect.get("confidence"), dict)
    ]
    confidence_score = _mean(confidence_scores) or 0.0
    return {
        "median_d7_percentile_in_pattern": _mean([effect.get("median_d7_percentile_in_pattern") for effect in effects]),
        "vs_baseline_pp": _mean([effect.get("vs_baseline_pp") for effect in effects]),
        "associated_metrics": {
            "views_x_avg": _mean([item.get("views_x_avg") for item in associated_rows]),
            "comments_x_avg": _mean([item.get("comments_x_avg") for item in associated_rows]),
            "likes_x_avg": _mean([item.get("likes_x_avg") for item in associated_rows]),
            "follower_delta_avg": _mean([item.get("follower_delta_avg") for item in associated_rows]),
        },
        "confidence": {
            "score": round(float(confidence_score), 4),
            "bucket": _confidence_bucket(float(confidence_score)),
        },
    }


def _feed_lifecycle_from_patterns(patterns: list[dict[str, Any]]) -> dict[str, Any]:
    statuses = [str((pattern.get("lifecycle") or {}).get("status") or "") for pattern in patterns]
    stable_count = sum(1 for status in statuses if status in {"stable", "conflict"})
    strengthening_count = sum(1 for status in statuses if status == "strengthening")
    if stable_count >= 2:
        status = "stable"
    elif stable_count + strengthening_count >= 2:
        status = "strengthening"
    else:
        status = "emerging"
    proof = []
    support: dict[str, int] = {}
    for pattern in patterns:
        proof.extend(str(value) for value in (pattern.get("proof_post_keys") or []) if value)
        trigger_support = (pattern.get("lifecycle") or {}).get("trigger_support")
        if isinstance(trigger_support, dict):
            for key, value in trigger_support.items():
                support[str(key)] = support.get(str(key), 0) + int(value or 0)
    return {
        "status": status,
        "evidence_count_window": len({key for key in proof if key}),
        "window_posts": len(patterns),
        "last_seen_n_posts_ago": min(
            [int((pattern.get("lifecycle") or {}).get("last_seen_n_posts_ago") or 999) for pattern in patterns] or [999]
        ),
        "opportunities_since_seen": min(
            [int((pattern.get("lifecycle") or {}).get("opportunities_since_seen") or 999) for pattern in patterns] or [999]
        ),
        "first_seen_at": min(
            [str((pattern.get("lifecycle") or {}).get("first_seen_at") or "") for pattern in patterns if (pattern.get("lifecycle") or {}).get("first_seen_at")] or [None]
        ),
        "promoted_at": None,
        "trigger_support": support,
    }


def _feed_content_profile(payload: dict[str, Any]) -> dict[str, Any]:
    feeders = payload.get("feeders") if isinstance(payload.get("feeders"), list) else []
    tones: dict[str, int] = {}
    production_styles: set[str] = set()
    for feeder in feeders:
        content = feeder.get("content_profile") if isinstance(feeder.get("content_profile"), dict) else {}
        voice = content.get("voice") if isinstance(content.get("voice"), dict) else {}
        tone = str(voice.get("dominant_tone") or "").strip()
        if tone:
            tones[tone] = tones.get(tone, 0) + 1
        production = content.get("production") if isinstance(content.get("production"), dict) else {}
        by_format = production.get("by_format") if isinstance(production.get("by_format"), dict) else {}
        for value in by_format.values():
            if isinstance(value, dict) and value.get("dominant"):
                production_styles.add(str(value.get("dominant")))
    dominant = [key for key, _ in sorted(tones.items(), key=lambda item: item[1], reverse=True)[:5]]
    style_count = len(production_styles)
    return {
        "dominant_voice_signatures": dominant,
        "production_diversity": "high" if style_count >= 6 else "medium" if style_count >= 3 else "low",
        "format_mix_feed_level": _format_mix(_count_by_format(payload.get("metric_rows") or [])),
        "evolution_notes": [],
    }


def _feed_metric_profile(payload: dict[str, Any]) -> dict[str, Any]:
    rows = payload.get("metric_rows") if isinstance(payload.get("metric_rows"), list) else []
    feeder_windows = payload.get("feeder_metric_windows") if isinstance(payload.get("feeder_metric_windows"), dict) else {}
    medians = [
        _num(((value.get("metric_windows") or {}).get("common") or {}).get("median_percentile"))
        for value in feeder_windows.values()
        if isinstance(value, dict)
    ]
    clean = [float(value) for value in medians if value is not None]
    if len(clean) >= 2:
        volatility = statistics.pstdev(clean)
        volatility_label = "high" if volatility >= 20 else "medium" if volatility >= 10 else "low"
    else:
        volatility_label = "unknown"
    return {
        "baselines": _metric_baselines(rows),
        "metric_trends": _metric_trends(rows),
        "cross_feeder_volatility": volatility_label,
    }


def _feed_anchor_lens(payload: dict[str, Any], registry: list[dict[str, Any]], legacy_anchor_lens: dict[str, Any]) -> dict[str, Any]:
    feeders = payload.get("feeders") if isinstance(payload.get("feeders"), list) else []
    anchor = next((row for row in feeders if str(row.get("role") or "").lower() == "anchor"), None)
    if not anchor:
        return legacy_anchor_lens or {}
    feed_median = (((payload.get("metric_windows") or {}).get("common") or {}).get("median_percentile"))
    feeder_windows = payload.get("feeder_metric_windows") if isinstance(payload.get("feeder_metric_windows"), dict) else {}
    anchor_median = (((feeder_windows.get(str(anchor.get("id"))) or {}).get("metric_windows") or {}).get("common") or {}).get("median_percentile")
    gap = round(float(anchor_median) - float(feed_median), 2) if _num(anchor_median) is not None and _num(feed_median) is not None else None
    linked = []
    for pattern in registry:
        replication = pattern.get("replication") if isinstance(pattern.get("replication"), dict) else {}
        links = replication.get("linked_feeder_patterns") if isinstance(replication.get("linked_feeder_patterns"), list) else []
        if any(int(link.get("feeder_id") or 0) == int(anchor.get("id") or 0) for link in links if isinstance(link, dict)):
            linked.append(pattern.get("pattern_id"))
    out = dict(legacy_anchor_lens or {})
    out.update({
        "anchor_feeder_id": int(anchor.get("id") or 0),
        "gap_pp": gap,
        "direction": out.get("direction") or "stable",
        "linked_anchor_pattern_ids": [str(value) for value in linked if value],
    })
    return out


def _build_feed_focus_v2(
    *,
    payload: dict[str, Any],
    current: dict[str, Any],
    legacy_result: dict[str, Any],
    compile_kind: str,
    started_at: datetime,
    version: int,
) -> dict[str, Any]:
    feeders = payload.get("feeders") if isinstance(payload.get("feeders"), list) else []
    pattern_rows = _feeder_v2_patterns(feeders)
    clusters = _cluster_feed_patterns(pattern_rows)
    registry: list[dict[str, Any]] = []
    divergence: list[dict[str, Any]] = []
    for cluster in clusters:
        feeder_ids = {int(row.get("feeder_id") or 0) for row in cluster}
        patterns = [row.get("pattern") for row in cluster if isinstance(row.get("pattern"), dict)]
        if len(feeder_ids) < 2:
            pattern = patterns[0] if patterns else {}
            effects = pattern.get("metric_effects") if isinstance(pattern.get("metric_effects"), dict) else {}
            confidence = effects.get("confidence") if isinstance(effects.get("confidence"), dict) else {}
            if str(confidence.get("bucket") or "") == "high":
                divergence.append({
                    "feeder_id": int(cluster[0].get("feeder_id") or 0),
                    "pattern_id": pattern.get("pattern_id"),
                    "note": str(pattern.get("summary") or ""),
                    "evidence_posts": len(pattern.get("proof_post_keys") or []),
                })
            continue
        seed = patterns[0]
        summary = str(seed.get("summary") or "")
        fmt = _media_key(seed.get("format"))
        feed_pattern_id = f"feed_{_pattern_id(fmt, summary)}"
        proof_keys: list[str] = []
        for pattern in patterns:
            proof_keys.extend(str(value) for value in (pattern.get("proof_post_keys") or []) if value)
        registry.append({
            "pattern_id": feed_pattern_id,
            "format": fmt,
            "label": seed.get("label") or _fallback_label(summary),
            "summary": summary,
            "content_signature": seed.get("content_signature") or _fallback_signature(summary),
            "replication": {
                "feeder_count": len(feeder_ids),
                "linked_feeder_patterns": [
                    {
                        "feeder_id": int(row.get("feeder_id") or 0),
                        "pattern_id": (row.get("pattern") or {}).get("pattern_id"),
                    }
                    for row in cluster
                ],
            },
            "metric_effects": _aggregate_metric_effects(patterns),
            "lifecycle": _feed_lifecycle_from_patterns(patterns),
            "proof_post_keys": list(dict.fromkeys(proof_keys))[:8],
        })
    _apply_conflict_status(registry)
    convergence = [
        {
            "summary": pattern.get("summary"),
            "format": pattern.get("format"),
            "feed_pattern_id": pattern.get("pattern_id"),
            "feeder_ids": [
                int(link.get("feeder_id") or 0)
                for link in ((pattern.get("replication") or {}).get("linked_feeder_patterns") or [])
                if isinstance(link, dict)
            ],
            "evidence_posts": len(pattern.get("proof_post_keys") or []),
            "status": (pattern.get("lifecycle") or {}).get("status"),
        }
        for pattern in registry
    ]
    previous_registry = current.get("pattern_registry") if isinstance(current.get("pattern_registry"), list) else []
    content_profile = _feed_content_profile(payload)
    metric_profile = _feed_metric_profile(payload)
    derived = _compute_derived_views(registry)
    anchor_lens = _feed_anchor_lens(payload, registry, legacy_result.get("anchor_lens") if isinstance(legacy_result.get("anchor_lens"), dict) else {})
    delta = _build_delta_log(previous_registry, registry)
    validated = {
        "content_profile": content_profile,
        "metric_profile": metric_profile,
        "pattern_registry": registry,
        "convergence": convergence,
        "divergence": divergence,
        "anchor_lens": anchor_lens,
        "derived_views": derived,
        "delta_log": delta,
    }
    stats = {
        "feed": payload.get("feed") or {},
        "active_feeders_count": len(feeders),
        "replicated_pattern_count": len(registry),
        "divergence_count": len(divergence),
    }
    compile_meta = _build_compile_meta(
        entity={
            "feed_id": int((payload.get("feed") or {}).get("id") or 0),
            "active_feeders_count": len(feeders),
        },
        version=version,
        model="server",
        compile_kind=compile_kind,
        started_at=started_at,
        stats=stats,
        llm_payload={"server_feed_focus_v2": stats},
        llm_raw="",
        validated_payload=validated,
        prompt="server_feed_focus_v2",
    )
    return {**validated, "compile_meta": compile_meta}


def _fetch_feed_payload(conn: Any, feed_id: int) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("select id, name, context_bible from public.feeds where id = %s limit 1", (feed_id,))
        feed = cur.fetchone()
        if not feed:
            return None
        cur.execute(
            """
            select fd.id, fd.handle, coalesce(fd.role, 'standard') as role,
                   ff.structured_patterns, ff.evidence_summary,
                   ff.focus_md_common, ff.focus_md_reel, ff.focus_md_image, ff.focus_md_carousel,
                   ff.focus_version, ff.content_profile, ff.metric_profile, ff.pattern_registry,
                   ff.derived_views, ff.delta_log, ff.compile_meta
            from public.feeders fd
            left join public.feeder_focus ff on ff.feeder_id = fd.id
            where fd.feed_id = %s and fd.status = 'active'
            order by case when fd.role = 'anchor' then 0 else 1 end, fd.id
            """,
            (feed_id,),
        )
        feeders = cur.fetchall()
        cur.execute(
            """
            select p.post_key, p.posted_at, lower(coalesce(p.media_type, 'image')) as media_type,
                   fd.id as feeder_id, fd.handle, coalesce(fd.role, 'standard') as role,
                   coalesce(pm.percentile_performance_exact, pm.percentile_performance::numeric) as percentile,
                   pm.views, pm.likes, pm.comments,
                   pm.views_multiple, pm.likes_multiple, pm.comments_multiple,
                   coalesce(d21.percentile_performance_exact, d21.percentile_performance::numeric) as d21_percentile,
                   d21.views_multiple as d21_views_multiple,
                   d21.likes_multiple as d21_likes_multiple,
                   d21.comments_multiple as d21_comments_multiple
            from public.posts p
            join public.feeders fd on fd.id = p.feeder_id
            join public.post_metrics pm on pm.post_key = p.post_key and pm.checkpoint = 'd7'
            left join public.post_metrics d21 on d21.post_key = p.post_key and d21.checkpoint = 'd21'
            where fd.feed_id = %s
              and p.posted_at >= now() - interval '90 days'
              and pm.percentile_performance is not null
            order by p.posted_at desc nulls last
            """,
            (feed_id,),
        )
        metric_rows = cur.fetchall()
    metric_dicts = [dict(row) for row in metric_rows]
    feeder_metric_windows: dict[str, Any] = {}
    for feeder_row in feeders:
        feeder_id = int(feeder_row.get("id") or 0)
        feeder_rows = [row for row in metric_dicts if int(row.get("feeder_id") or 0) == feeder_id]
        feeder_metric_windows[str(feeder_id)] = {
            "handle": feeder_row.get("handle"),
            "role": feeder_row.get("role"),
            "metric_windows": _metric_windows(feeder_rows),
        }
    return {
        "feed": dict(feed),
        "feeders": [dict(row) for row in feeders],
        "metric_rows": metric_dicts,
        "metric_windows": _metric_windows(metric_dicts),
        "feeder_metric_windows": feeder_metric_windows,
    }


def _fetch_current_feed_focus(conn: Any, feed_id: int) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("select * from public.feed_focus where feed_id = %s limit 1", (feed_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def compile_feed_focus(conn: Any, feed_id: int | None = None, *, limit: int = 10, full_rebuild: bool = False) -> dict[str, int]:
    with conn.cursor(row_factory=dict_row) as cur:
        if feed_id is not None:
            cur.execute("select id from public.feeds where id = %s", (feed_id,))
        else:
            cur.execute(
                """
                select id
                from public.feeds
                where status = 'active'
                order by coalesce(updated_at, created_at) desc nulls last, id desc
                limit %s
                """,
                (max(1, limit),),
            )
        feed_ids = [int(row["id"]) for row in cur.fetchall()]

    selected = len(feed_ids)
    compiled = 0
    skipped = 0
    failed = 0
    no_feeder_focus = 0
    for fid in feed_ids:
        if not _claim_focus_compile_lock(conn, "feed", fid, force=full_rebuild):
            skipped += 1
            _focus_log("compile_lock_skip", scope="feed", entity_id=fid)
            continue
        try:
            started_at = datetime.now(timezone.utc)
            payload = _fetch_feed_payload(conn, fid)
            if not payload:
                skipped += 1
                _mark_focus_compile_lock(conn, "feed", fid, success=True)
                _focus_log("compile_skip_empty_payload", scope="feed", entity_id=fid)
                continue
            focused_feeders = [
                row for row in (payload.get("feeders") or [])
                if int(row.get("focus_version") or 0) > 0
                and (
                    str(row.get("focus_md_common") or "").strip()
                    or str(row.get("focus_md_reel") or "").strip()
                    or str(row.get("focus_md_image") or "").strip()
                    or str(row.get("focus_md_carousel") or "").strip()
                )
            ]
            if not focused_feeders:
                skipped += 1
                no_feeder_focus += 1
                _mark_focus_compile_lock(
                    conn,
                    "feed",
                    fid,
                    success=False,
                    error="no_compiled_feeder_focus_available",
                )
                _focus_log("compile_skip_no_feeder_focus", scope="feed", entity_id=fid)
                continue
            current = _fetch_current_feed_focus(conn, fid) or {}
            rebuild_now = bool(full_rebuild or _full_rebuild_due(current))
            compile_kind = "full_rebuild" if rebuild_now else "weekly_update"
            model = FOCUS_REBUILD_MODEL if rebuild_now else FOCUS_COMPILER_MODEL
            previous_patterns = current.get("structured_patterns") or {}
            payload["mode"] = compile_kind
            payload["previous_focus"] = {
                "structured_patterns": previous_patterns,
                "anchor_lens": current.get("anchor_lens") or {},
                "focus_md": current.get("focus_md") or "",
                "capsules": {
                    "common": current.get("capsule_common") or "",
                    "reel": current.get("capsule_reel") or "",
                    "image": current.get("capsule_image") or "",
                    "carousel": current.get("capsule_carousel") or "",
                    "anchor": current.get("capsule_anchor") or "",
                    "cross": current.get("capsule_cross") or "",
                },
            }
            payload["stage_b_memory_candidates"] = _aggregate_memory_candidates(_fetch_stage_b_candidates(conn, feed_id=fid))
            source_hash = _sha(payload)
            v2_version_stale = _focus_v2_version_stale(current.get("compile_meta"))
            if not rebuild_now and not v2_version_stale and current.get("source_hash") == source_hash:
                skipped += 1
                _mark_focus_compile_lock(conn, "feed", fid, success=True)
                _focus_log(
                    "source_hash_hit",
                    scope="feed",
                    entity_id=fid,
                    feeder_count=len(payload.get("feeders") or []),
                )
                continue
            patterns_before = len((previous_patterns.get("patterns") if isinstance(previous_patterns, dict) else []) or [])
            legacy_payload = {
                **payload,
                "feeders": [
                    {
                        "id": row.get("id"),
                        "handle": row.get("handle"),
                        "role": row.get("role"),
                        "structured_patterns": row.get("structured_patterns") or {},
                        "evidence_summary": row.get("evidence_summary") or {},
                        "focus_md_common": row.get("focus_md_common") or "",
                        "focus_md_reel": row.get("focus_md_reel") or "",
                        "focus_md_image": row.get("focus_md_image") or "",
                        "focus_md_carousel": row.get("focus_md_carousel") or "",
                        "focus_version": row.get("focus_version") or 0,
                    }
                    for row in (payload.get("feeders") or [])
                ],
            }
            result = _call_text_model(_FEED_COMPILER_SYSTEM, legacy_payload, model=model, max_tokens=2600)
            if not result:
                failed += 1
                _mark_focus_compile_lock(conn, "feed", fid, success=False, error="model_failed_or_malformed")
                _focus_log("compile_failed", scope="feed", entity_id=fid, reason="model_failed_or_malformed")
                continue
            result = _cap_focus_texts(result, kind="feed")
            structured_patterns = _normalize_patterns(result.get("structured_patterns") or {}, cap_per_format=True)
            patterns_after = len((structured_patterns.get("patterns") if isinstance(structured_patterns, dict) else []) or [])
            version = int(current.get("focus_version") or 0) + 1
            v2 = _build_feed_focus_v2(
                payload=payload,
                current=current,
                legacy_result={**result, "structured_patterns": structured_patterns},
                compile_kind=compile_kind,
                started_at=started_at,
                version=version,
            )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into public.feed_focus (
                      feed_id, structured_patterns, anchor_lens, feed_metrics, proof_posts,
                      focus_md, capsule_common, capsule_reel, capsule_image, capsule_carousel,
                      capsule_anchor, capsule_cross, focus_version, prompt_version, model_version,
                      source_hash, content_profile, metric_profile, pattern_registry, convergence,
                      divergence, derived_views, delta_log, compile_meta,
                      focus_updated_at, last_full_rebuild_at, created_at, updated_at
                    )
                    values (%s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
                            %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, now(),
                            case when %s then now() else null end, now(), now())
                    on conflict (feed_id) do update set
                      structured_patterns = excluded.structured_patterns,
                      anchor_lens = excluded.anchor_lens,
                      feed_metrics = excluded.feed_metrics,
                      proof_posts = excluded.proof_posts,
                      focus_md = excluded.focus_md,
                      capsule_common = excluded.capsule_common,
                      capsule_reel = excluded.capsule_reel,
                      capsule_image = excluded.capsule_image,
                      capsule_carousel = excluded.capsule_carousel,
                      capsule_anchor = excluded.capsule_anchor,
                      capsule_cross = excluded.capsule_cross,
                      focus_version = excluded.focus_version,
                      prompt_version = excluded.prompt_version,
                      model_version = excluded.model_version,
                      source_hash = excluded.source_hash,
                      content_profile = excluded.content_profile,
                      metric_profile = excluded.metric_profile,
                      pattern_registry = excluded.pattern_registry,
                      convergence = excluded.convergence,
                      divergence = excluded.divergence,
                      derived_views = excluded.derived_views,
                      delta_log = excluded.delta_log,
                      compile_meta = excluded.compile_meta,
                      focus_updated_at = now(),
                      last_full_rebuild_at = case when %s then now() else public.feed_focus.last_full_rebuild_at end,
                      updated_at = now()
                    """,
                    (
                        fid,
                        json.dumps(structured_patterns),
                        json.dumps(result.get("anchor_lens") or {}),
                        json.dumps(result.get("feed_metrics") or payload.get("metric_windows") or {}),
                        json.dumps(result.get("proof_posts") or {}),
                        str(result.get("focus_md") or ""),
                        str(result.get("capsule_common") or ""),
                        str(result.get("capsule_reel") or ""),
                        str(result.get("capsule_image") or ""),
                        str(result.get("capsule_carousel") or ""),
                        str(result.get("capsule_anchor") or ""),
                        str(result.get("capsule_cross") or ""),
                        version,
                        _FEED_FOCUS_PROMPT_VERSION,
                        model,
                        source_hash,
                        json.dumps(v2.get("content_profile") or {}),
                        json.dumps(v2.get("metric_profile") or {}),
                        json.dumps(v2.get("pattern_registry") or []),
                        json.dumps(v2.get("convergence") or []),
                        json.dumps(v2.get("divergence") or []),
                        json.dumps(v2.get("derived_views") or {}),
                        json.dumps(v2.get("delta_log") or {}),
                        json.dumps(v2.get("compile_meta") or {}),
                        rebuild_now,
                        rebuild_now,
                    ),
                )
            conn.commit()
            _mark_focus_compile_lock(conn, "feed", fid, success=True)
            _focus_log(
                "compile_success",
                scope="feed",
                entity_id=fid,
                mode=compile_kind,
                model=model,
                feeder_count=len(payload.get("feeders") or []),
                patterns_before=patterns_before,
                patterns_after=patterns_after,
                v2_patterns=len(v2.get("pattern_registry") or []),
                word_counts=_capsule_word_counts(result, [
                    "capsule_common",
                    "capsule_reel",
                    "capsule_image",
                    "capsule_carousel",
                    "capsule_anchor",
                    "capsule_cross",
                ]),
            )
            compiled += 1
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            failed += 1
            _mark_focus_compile_lock(conn, "feed", fid, success=False, error=str(exc))
            _focus_log("compile_failed", scope="feed", entity_id=fid, reason=str(exc)[:500])
    return {
        "selected": selected,
        "compiled": compiled,
        "skipped": skipped,
        "failed": failed,
        "no_feeder_focus": no_feeder_focus,
    }
