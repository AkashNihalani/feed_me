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
You receive the previous rulebook, all available 90-day evidence rows, this week's evidence, Stage B memory candidates, and metric windows.
Lower percentile is better: p10 beats p40, and p80 is weak.
server_pattern_stats is authoritative for last_seen_days, evidence counts, and trigger support. Use it instead of doing calendar math yourself.
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
- Pattern decay is strict: unseen 25d=weakening, 50d=decaying, 75d=archived.
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
    if raw.startswith("```"):
        lines = raw.splitlines()[1:]
        while lines and lines[-1].strip() == "```":
            lines.pop()
        raw = "\n".join(lines).strip()
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


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
    provider = _provider()
    if not provider:
        return None
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
        return parsed
    except Exception as exc:
        print(f"[focus-brain] model call failed: {exc}")
        return None


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
        matched = [row for row, terms_for_row in row_terms if _pattern_match(terms, terms_for_row)]
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
                "server_proof_post_keys": [],
            })
            continue
        ages = [_age_days(row.get("posted_at"), now) for row in matched]
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
            if stat.get("server_proof_post_keys"):
                row["proof_post_keys"] = stat.get("server_proof_post_keys")
        seen = int(row.get("last_seen_days") or 999)
        status = str(row.get("status") or "watchlist").lower()
        if seen >= 75:
            status = "archived"
        elif seen >= 50:
            status = "decaying"
        elif seen >= 25 and status not in {"decaying", "archived"}:
            status = "weakening"
        row["status"] = status
        if status == "archived":
            archived.append(row)
        else:
            updated.append(row)
    data["patterns"] = updated
    data["archived_patterns"] = archived[-40:]
    return data


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


def feeder_focus_slice(conn: Any, feeder_id: int, media_type: Any) -> dict[str, Any]:
    key = _media_key(media_type)
    field = {
        "reel": "focus_md_reel",
        "image": "focus_md_image",
        "carousel": "focus_md_carousel",
    }[key]
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            select focus_version, focus_md_common, {field} as focus_md
            from public.feeder_focus
            where feeder_id = %s
            limit 1
            """,
            (feeder_id,),
        )
        row = cur.fetchone()
    if not row:
        return {"version": 0, "text": "", "media_key": key}
    parts = [str(row.get("focus_md_common") or "").strip(), str(row.get("focus_md") or "").strip()]
    return {
        "version": int(row.get("focus_version") or 0),
        "text": "\n".join(part for part in parts if part),
        "media_key": key,
    }


def ensure_post_focus_read(conn: Any, post_key: str, fingerprint: dict[str, Any]) -> dict[str, Any]:
    post = _fetch_post_context(conn, post_key)
    if not post:
        return {}
    focus = feeder_focus_slice(conn, int(post.get("feeder_id") or 0), post.get("media_type"))
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

    if not focus["text"]:
        focus_read = {
            "relation_to_feeder_md": {
                "matches": [],
                "deviates": [],
                "unclear": ["no compiled feeder focus available"],
            },
            "notes": [],
        }
    else:
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
                   pm.percentile_performance as percentile,
                   pm.views, pm.likes, pm.comments,
                   pf.fingerprint,
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
    evidence: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row.get("fingerprint"), dict):
            continue
        evidence.append(dict(row))
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
    for fid in feeder_ids:
        if not _claim_focus_compile_lock(conn, "feeder", fid, force=full_rebuild):
            skipped += 1
            _focus_log("compile_lock_skip", scope="feeder", entity_id=fid)
            continue
        try:
            feeder, evidence, all_evidence = _fetch_feeder_evidence(conn, fid)
            if not feeder or not evidence:
                skipped += 1
                _mark_focus_compile_lock(conn, "feeder", fid, success=True)
                _focus_log("compile_skip_empty_evidence", scope="feeder", entity_id=fid)
                continue
            current = _fetch_current_feeder_focus(conn, fid) or {}
            rebuild_now = bool(full_rebuild or _full_rebuild_due(current))
            model = FOCUS_REBUILD_MODEL if rebuild_now else FOCUS_COMPILER_MODEL
            previous_patterns = current.get("structured_patterns") or {}
            payload = {
                "mode": "full_rebuild" if rebuild_now else "weekly_update",
                "feeder": feeder,
                "previous_focus": {
                    "structured_patterns": previous_patterns,
                    "focus_md_common": current.get("focus_md_common") or "",
                    "focus_md_reel": current.get("focus_md_reel") or "",
                    "focus_md_image": current.get("focus_md_image") or "",
                    "focus_md_carousel": current.get("focus_md_carousel") or "",
                },
                "metric_windows": _metric_windows(all_evidence),
                "server_pattern_stats": _pattern_stat_rows(previous_patterns, all_evidence),
                "evidence_population": {
                    "fingerprinted_90d": len(all_evidence),
                    "sampled_for_llm": len(evidence),
                },
                "evidence_posts": evidence,
                "stage_b_memory_candidates": _aggregate_memory_candidates(_fetch_stage_b_candidates(
                    conn,
                    feed_id=int(feeder.get("feed_id") or 0),
                    feeder_id=fid,
                )),
            }
            source_hash = _sha(payload)
            if not rebuild_now and current.get("source_hash") == source_hash:
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
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into public.feeder_focus (
                      feeder_id, feed_id, structured_patterns, evidence_summary,
                      focus_md_common, focus_md_reel, focus_md_image, focus_md_carousel,
                      focus_version, prompt_version, model_version, source_hash,
                      focus_updated_at, last_full_rebuild_at, created_at, updated_at
                    )
                    values (%s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, now(),
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
                      focus_updated_at = now(),
                      last_full_rebuild_at = case when %s then now() else public.feeder_focus.last_full_rebuild_at end,
                      updated_at = now()
                    """,
                    (
                        fid,
                        int(feeder.get("feed_id") or 0),
                        json.dumps(structured_patterns),
                        json.dumps(result.get("evidence_summary") or {}),
                        str(result.get("focus_md_common") or ""),
                        str(result.get("focus_md_reel") or ""),
                        str(result.get("focus_md_image") or ""),
                        str(result.get("focus_md_carousel") or ""),
                        version,
                        _FEEDER_FOCUS_PROMPT_VERSION,
                        model,
                        source_hash,
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
                mode="full_rebuild" if rebuild_now else "weekly_update",
                model=model,
                evidence_count=len(evidence),
                full_evidence_count=len(all_evidence),
                patterns_before=patterns_before,
                patterns_after=patterns_after,
                word_counts=_capsule_word_counts(result, ["focus_md_common", "focus_md_reel", "focus_md_image", "focus_md_carousel"]),
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
    return {"selected": selected, "compiled": compiled, "skipped": skipped, "failed": failed}


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
                   ff.focus_version
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
                   pm.percentile_performance as percentile, pm.views, pm.likes, pm.comments
            from public.posts p
            join public.feeders fd on fd.id = p.feeder_id
            join public.post_metrics pm on pm.post_key = p.post_key and pm.checkpoint = 'd7'
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
    for fid in feed_ids:
        if not _claim_focus_compile_lock(conn, "feed", fid, force=full_rebuild):
            skipped += 1
            _focus_log("compile_lock_skip", scope="feed", entity_id=fid)
            continue
        try:
            payload = _fetch_feed_payload(conn, fid)
            if not payload:
                skipped += 1
                _mark_focus_compile_lock(conn, "feed", fid, success=True)
                _focus_log("compile_skip_empty_payload", scope="feed", entity_id=fid)
                continue
            current = _fetch_current_feed_focus(conn, fid) or {}
            rebuild_now = bool(full_rebuild or _full_rebuild_due(current))
            model = FOCUS_REBUILD_MODEL if rebuild_now else FOCUS_COMPILER_MODEL
            previous_patterns = current.get("structured_patterns") or {}
            payload["mode"] = "full_rebuild" if rebuild_now else "weekly_update"
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
            if not rebuild_now and current.get("source_hash") == source_hash:
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
            result = _call_text_model(_FEED_COMPILER_SYSTEM, payload, model=model, max_tokens=2600)
            if not result:
                failed += 1
                _mark_focus_compile_lock(conn, "feed", fid, success=False, error="model_failed_or_malformed")
                _focus_log("compile_failed", scope="feed", entity_id=fid, reason="model_failed_or_malformed")
                continue
            result = _cap_focus_texts(result, kind="feed")
            structured_patterns = _normalize_patterns(result.get("structured_patterns") or {}, cap_per_format=True)
            patterns_after = len((structured_patterns.get("patterns") if isinstance(structured_patterns, dict) else []) or [])
            version = int(current.get("focus_version") or 0) + 1
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into public.feed_focus (
                      feed_id, structured_patterns, anchor_lens, feed_metrics, proof_posts,
                      focus_md, capsule_common, capsule_reel, capsule_image, capsule_carousel,
                      capsule_anchor, capsule_cross, focus_version, prompt_version, model_version,
                      source_hash, focus_updated_at, last_full_rebuild_at, created_at, updated_at
                    )
                    values (%s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, now(), case when %s then now() else null end, now(), now())
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
                mode="full_rebuild" if rebuild_now else "weekly_update",
                model=model,
                feeder_count=len(payload.get("feeders") or []),
                patterns_before=patterns_before,
                patterns_after=patterns_after,
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
    return {"selected": selected, "compiled": compiled, "skipped": skipped, "failed": failed}
