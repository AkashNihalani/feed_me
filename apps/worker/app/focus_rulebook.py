from __future__ import annotations

import hashlib
import json
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
    SIGNAL_INTELLIGENCE_ENABLED,
    SIGNAL_INTELLIGENCE_PROVIDER,
)
from .evidence_packet import (
    _fetch_rows as fetch_recent_evidence_rows,
    annotate_signal_types,
    build_feed_evidence_packet,
    build_feeder_evidence_packet,
    build_feeder_warm_start_packet,
    packet_post_keys_needing_fingerprints,
)
from .ticker_facts import ticker_facts_from_feed_rulebook, ticker_facts_from_feeder_rulebook

_OPENROUTER_CHAT_URL = "/chat/completions"
_GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
_DEFAULT_PRO_MODEL = "google/gemini-3.1-pro-preview"
_FEEDER_RULEBOOK_PROMPT_VERSION = "feeder_rulebook_v4"
_FEEDER_FOCUS_STATE_PROMPT_VERSION = "feeder_focus_state_v3_memory_discipline"
_FEED_RULEBOOK_PROMPT_VERSION = "feed_rulebook_v4"
_POST_FOCUS_PROMPT_VERSION = "post_focus_read_v2_mechanism_spine"
_RULEBOOK_HARD_CAP = 100

_FEEDER_RULEBOOK_SYSTEM = """You compile Feed_Me feeder rulebooks.

You are not detecting alerts. The server already did that. You are reading a bounded evidence packet:
- Top and bottom posts per media format
- anomaly slices for comments, likes, views, followers
- signal_residual posts that fired an alert but fell outside the main slices
- typical reference posts used as baseline contrast in warm-start compiles
- metric windows, cadence, follower trends, and previous rulebook

Write rules from evidence, not taste. Do not invent saves, shares, watch time, profile visits, or algorithm behavior.
Do not create pattern tags, lifecycle states, clusters, or named registries. This is not a tag system.
Use media-type boundaries: reel rules belong in reels; carousel rules belong in carousels; statics rules belong in statics.
Respect checkpoint meaning. D3 is early momentum, D7 is normal ranking proof, D21 is evergreen/late-tail proof.
If a format has thin evidence, say "Insufficient evidence" inside that format.

Voice:
- cold analyst, clean and direct
- descriptive but not academic
- no generic strategy labels
- cite concrete post ids or visible post mechanics inside each rule
- trajectory language when something changed: "was X, now Y"
- "comments structurally low" is allowed when data supports it; do not call that failure by default
- previous_rulebook is continuity, not a cage. Preserve identity unless evidence clearly contradicts it.

Return only JSON:
{
  "account_read": "3-5 sentences, observed identity only",
  "reels": {"dos": [], "donts": []},
  "carousels": {"dos": [], "donts": []},
  "statics": {"dos": [], "donts": []},
  "by_performance_axis": {
    "comments": "",
    "likes": "",
    "views": "",
    "followers": "",
    "early_momentum": "",
    "evergreen_or_long_tail": "",
    "decay_or_dropoff": "",
    "format_movement": "",
    "cadence_movement": ""
  },
  "shifts_since_last_compile": [],
  "known_unknowns": []
}"""

_FEEDER_FOCUS_STATE_SYSTEM = """You compile Feed_Me feeder focus state.

This is not a dos/don't rulebook. It is not a tag system. It is the account's current pressure memory, built from:
- stored neutral post fingerprints
- current movement ledger
- slow account memory
- deterministic metric context
- recent signal-card footnotes

Core philosophy:
- Movements own truth. Account context only explains what a movement means for this account.
- Do not invent movements. Consolidate duplicate movement variants when they clearly describe the same separation.
- Do not merge two movements just because they share atmosphere. Merge only when the same pressure point, metric axis, and weaker-variant contrast repeat.
- Do not turn the account into an archetype. The account read is atmospheric memory, not destiny.
- Account memory stores recurring surface conditions. Movement memory stores the active separation.
- Do not write strategy advice, recommendations, dos/donts, or content pillars.
- Store raw pressure facts. Do not use lifecycle labels like active, emerging, dominant, holding, weakening.
- current_pressure_interpretation is not a full account summary. It is the highest-confidence current competitive separation for this account.
- axis_map values must be movement_id strings only. No prose inside axis_map.
- movement_health is memory hygiene. Prefer raw facts over labels.
- movement_scope marks where the movement is actually valid. Do not make a reel movement sound account-wide if only reels support it.
- If evidence is thin or contradictory, keep the uncertainty visible.
- Full rebuild mode ignores previous wording and rebuilds from 90d evidence. Weekly update mode preserves stable account language unless evidence clearly moves it.
- If fewer than 8 usable fingerprints are supplied, soften account memory language: "current evidence clusters around", "recent pressure points suggest", "not enough archive yet to call this stable".
- Low-sample focus may still contain movements, but it must not harden a creator/brand essence from a small pilot set.

The writing should feel like field notes from obsessive pattern study: specific, comparative, socially aware, and earned from supplied evidence.
Avoid generic AI/strategy language. Do not use: engagement, relatable, storytelling, content strategy, personality-driven, audience resonates, viral, hooks, trends, optimize, leverage, best practice, mechanism, behavioral contrast, social dynamic, viewer pressure, aesthetic showcase, audience, retention, cognitive friction, visceral, weaponize, weaponizing, trojan horse, high-energy, high-production.
Prefer concrete scene pressure over abstractions. "The studio stops feeling polished once the friends mute her" is better than "behavioral disruption sustains retention."
For voice_contract, describe repeated observed behavior. Do not write intention words like promises, invites, designed to, wants to.
For audience_role, describe the entry condition plainly: "People enter rooms that look formal but stop behaving formally." Avoid theatrical metaphor.
Movement titles should be mechanical memory primitives, not polished thesis copy:
- good: "formal setup + social undercut", "product prop + choice prompt", "early argument + late context"
- avoid: "formal visuals punctured by unserious behavior", "the room's prestige collapses"
current_pressure_interpretation should be 70-130 words. pressure_read should be 35-75 words.

Return only JSON:
{
  "feeder_focus": {
    "account_read": {
      "surface_world": "",
      "voice_contract": "",
      "audience_role": "",
      "proof_style": "",
      "identity_tension": "",
      "current_gravity": "",
      "confidence_basis": {
        "movement_support": 0,
        "contrast_clarity": "weak|medium|strong",
        "axis_alignment": [],
        "contradictions": 0
      }
    },
    "current_pressure_interpretation": "",
    "pressure_hierarchy": [
      {
        "movement_id": "",
        "title": "",
        "pressure_read": "",
        "metric_pressure": "",
        "evidence_posts": [],
        "contrast_posts": [],
        "evidence_pressure": [],
        "pressure_state": {
          "recent_reinforcements": 0,
          "recent_contradictions": 0,
          "gap_strength": null,
          "axis_consistency": "low|medium|high",
          "last_reinforced_at": ""
        },
        "movement_health": {
          "recent_support": 0,
          "recent_contradictions": 0,
          "last_major_separation": "",
          "days_since_reinforced": null,
          "decay_risk": "low|medium|high|unknown"
        },
        "movement_scope": {
          "formats": [],
          "duration_buckets": [],
          "contexts": [],
          "axes": []
        },
        "confidence_basis": {
          "support_posts": 0,
          "contrast_posts": 0,
          "contrast_clarity": "weak|medium|strong",
          "axis_alignment": [],
          "contradictions": 0
        }
      }
    ],
    "axis_map": {
      "comments": [],
      "views": [],
      "likes": [],
      "followers": [],
      "durability": []
    },
    "recent_shifts": [],
    "movement_contradictions": [],
    "unresolved_edges": [],
    "evidence_gaps": [],
    "pressure_uncertainties": [],
    "untested_variants": [],
    "known_false_signals": [],
    "known_limits": []
  }
}"""

_POST_FOCUS_SYSTEM = """You write Feed_Me post-focus reads for one Fire card.

This is not a neutral fingerprint. The fingerprint already says what is in the post.
Your job is to explain what happened, how it compares to the feeder's rulebook/history, and why the metric result makes sense or looks suspicious.

Inputs include:
- one post with full neutral fingerprint
- checkpoint metrics and signal annotations for that post
- the current feeder rulebook slice for the post's media type

Hard rules:
- Subject is exactly one post. Do not turn this into a feed-wide signal.
- Percentiles are ranks: lower is better. Multiples above 1.0 are above the feeder baseline.
- Do not invent saves, shares, watch time, profile visits, or algorithm behavior.
- Do not mention internal words like fingerprint, focus brain, packet, tags, clustering, or pattern registry.
- Do not over-focus on hooks. If the post works through late payoff, audio, visual rhythm, satire, innuendo, carousel sequence, or proof device, name that instead.
- If rulebook evidence is thin, say so directly and keep confidence low/medium.
- Keep it concise enough for a card, but make the thinking specific.
- Do not list category buckets. Name the one viewer-pressure mechanism inside this post: what state the viewer enters, what shifts, and why the metric result follows.
- Avoid generic analyst words: engagement, relatable, storytelling, personality-driven, high-conflict, reactionary format, aesthetic showcase, humble setup.

Return only JSON:
{
  "verdict": "one sharp line, <= 26 words",
  "matches": ["1-3 short lines naming what aligned with the feeder rulebook"],
  "deviates": ["1-3 short lines naming what broke from the feeder rulebook or explains downside"],
  "unclear": ["0-2 short lines for thin evidence or ambiguity"],
  "notes": ["0-2 short metric/content receipts"],
  "do_next": "one concrete next move, <= 22 words",
  "watchout": "one boundary or failure mode, <= 22 words",
  "confidence": "low|medium|high"
}"""

_FEED_RULEBOOK_SYSTEM = """You compile Feed_Me feed rulebooks.

You are reading a bounded cross-account evidence packet plus the previous feed rulebook. The server owns metrics; you explain what the evidence means for the feed.
Do not create pattern tags, lifecycle states, clusters, or named registries. This is prose rulebook state, not memory tagging.
Use media-type boundaries and do not average away divergences. If only one feeder wins with a move, call it a divergence, not a feed rule.
Respect checkpoint meaning. D3 is early momentum, D7 is normal ranking proof, D21 is evergreen/late-tail proof.
Do not invent saves, shares, watch time, profile visits, or algorithm behavior.

Voice:
- strategist note, not dashboard recap
- cold analyst, concrete, modern
- explain what changed, who is leading/stalling/challenging, and what the rule boundary is
- previous_rulebook is continuity. Preserve read unless evidence clearly moves.

Return only JSON:
{
  "feed_read": "3-5 sentences, observed niche/feed identity only",
  "reels": {"cross_feed_dos": [], "cross_feed_donts": [], "divergences": []},
  "carousels": {"cross_feed_dos": [], "cross_feed_donts": [], "divergences": []},
  "statics": {"cross_feed_dos": [], "cross_feed_donts": [], "divergences": []},
  "by_performance_axis": {
    "comments": "",
    "likes": "",
    "views": "",
    "followers": "",
    "early_momentum": "",
    "evergreen_or_long_tail": "",
    "decay_or_dropoff": "",
    "format_movement": "",
    "cadence_movement": ""
  },
  "cohort_dynamics": {
    "leading": [],
    "challenging": [],
    "stalling": []
  },
  "anchor_vs_feed": "",
  "capsules": {
    "common": "",
    "reel": "",
    "carousel": "",
    "image": "",
    "anchor": "",
    "cross": ""
  },
  "shifts_since_last_compile": [],
  "known_unknowns": []
}"""


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


def _model(provider: str, model: str | None = None) -> str:
    explicit = (model or FOCUS_COMPILER_MODEL or _DEFAULT_PRO_MODEL).strip()
    if provider == "google" and explicit.startswith("google/"):
        return explicit.split("/", 1)[1]
    return explicit


def _sha(value: Any) -> str:
    if value is None:
        value = ""
    if isinstance(value, str):
        payload = value.encode("utf-8", errors="ignore")
    else:
        payload = json.dumps(value, sort_keys=True, default=str).encode("utf-8", errors="ignore")
    return hashlib.sha256(payload).hexdigest()


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
        pass
    start = raw.find("{")
    if start < 0:
        return None
    depth = 0
    in_string = False
    escape = False
    for index in range(start, len(raw)):
        char = raw[index]
        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                try:
                    parsed = json.loads(raw[start:index + 1])
                    return parsed if isinstance(parsed, dict) else None
                except Exception:
                    return None
    return None


def _call_json_model(system: str, payload: dict[str, Any], *, model: str, max_tokens: int = 5200) -> dict[str, Any] | None:
    provider = _provider()
    if not provider:
        return None
    resolved_model = _model(provider, model)
    user_text = json.dumps(payload, default=str)
    try:
        if provider == "openrouter":
            resp = requests.post(
                f"{OPENROUTER_BASE_URL.rstrip('/')}{_OPENROUTER_CHAT_URL}",
                headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": resolved_model,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": user_text},
                    ],
                    "temperature": 0.1,
                    "max_tokens": max_tokens,
                    "response_format": {"type": "json_object"},
                },
                timeout=180,
            )
            if resp.status_code >= 400:
                retry_payload = resp.request.body
                data = json.loads(retry_payload.decode("utf-8") if isinstance(retry_payload, bytes) else retry_payload or "{}")
                data.pop("response_format", None)
                resp = requests.post(
                    f"{OPENROUTER_BASE_URL.rstrip('/')}{_OPENROUTER_CHAT_URL}",
                    headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"},
                    json=data,
                    timeout=180,
                )
            resp.raise_for_status()
            content = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
            return _json_from_text(content if isinstance(content, str) else json.dumps(content))

        resp = requests.post(
            _GEMINI_API_URL.format(model=resolved_model),
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
        text = resp.json().get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
        return _json_from_text(text)
    except Exception as exc:
        print(f"[focus-rulebook] model call failed: {exc}")
        return None


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
    return datetime.now(timezone.utc) - last >= timedelta(days=max(1, int(FOCUS_REBUILD_INTERVAL_DAYS or 45)))


def _claim_compile_lock(conn: Any, scope: str, entity_id: int, *, force: bool = False) -> bool:
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


def _mark_compile_lock(conn: Any, scope: str, entity_id: int, *, success: bool, error: str | None = None) -> None:
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


def _current_feeder_focus(conn: Any, feeder_id: int) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("select * from public.feeder_focus where feeder_id = %s limit 1", (feeder_id,))
        row = cur.fetchone()
    return dict(row) if row else {}


def _current_feed_focus(conn: Any, feed_id: int) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("select * from public.feed_focus where feed_id = %s limit 1", (feed_id,))
        row = cur.fetchone()
    return dict(row) if row else {}


def _signal_card_footnotes(conn: Any, *, feed_id: int, feeder_id: int | None = None, limit: int = 40) -> list[dict[str, Any]]:
    feeder_join = ""
    feeder_where = ""
    params: list[Any] = [feed_id]
    if feeder_id is not None:
        feeder_join = """
            join public.signal_posts sp on sp.signal_id = s.id
            join public.posts p on p.post_key = sp.post_key
        """
        feeder_where = "and p.feeder_id = %s"
        params.append(feeder_id)
    params.append(max(1, limit))
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            select distinct s.id as signal_id, s.signal_type, s.scope, s.media_type,
                   s.business_date_ist, si.card
            from public.signal_intelligence si
            join public.signals s on s.id = si.signal_id
            {feeder_join}
            where s.feed_id = %s
              {feeder_where}
              and si.updated_at >= now() - interval '45 days'
            order by s.business_date_ist desc nulls last, s.id desc
            limit %s
            """,
            tuple(params),
        )
        rows = [dict(row) for row in cur.fetchall()]
    out: list[dict[str, Any]] = []
    for row in rows:
        card = row.get("card") if isinstance(row.get("card"), dict) else {}
        out.append({
            "signal_id": row.get("signal_id"),
            "signal_type": row.get("signal_type"),
            "scope": row.get("scope"),
            "media_type": row.get("media_type"),
            "business_date_ist": row.get("business_date_ist"),
            "title": card.get("title"),
            "read": card.get("read") or card.get("what_happened"),
            "do_next": card.get("do_next"),
            "watchout": card.get("watchout"),
            "per_post_notes": card.get("per_post_notes") if isinstance(card.get("per_post_notes"), list) else [],
            "pattern_type": card.get("pattern_type"),
        })
    return out


def _ensure_packet_fingerprints(conn: Any, packet: dict[str, Any]) -> int:
    from .signal_intelligence import current_model_version, ensure_post_fingerprint

    required_model_version = current_model_version(kind="fingerprint")
    missing = packet_post_keys_needing_fingerprints(packet, required_model_version=required_model_version)
    if not missing:
        return 0

    created = 0
    for post_key in missing:
        if ensure_post_fingerprint(conn, post_key):
            created += 1
    return created


def _compress_fingerprint(fingerprint: Any) -> Any:
    if not isinstance(fingerprint, dict):
        return {}
    observed = fingerprint.get("observed") if isinstance(fingerprint.get("observed"), dict) else {}
    synthesis = fingerprint.get("synthesis") if isinstance(fingerprint.get("synthesis"), dict) else {}
    return {
        "observed": {
            "caption": str(observed.get("caption") or "")[:1200],
            "transcript": str(observed.get("transcript") or "")[:2500],
            "audio_notes": str(observed.get("audio_notes") or "")[:900],
            "visual_notes": str(observed.get("visual_notes") or "")[:2500],
        },
        "synthesis": synthesis,
    }


def _compile_packet_for_prompt(packet: dict[str, Any]) -> dict[str, Any]:
    compact = dict(packet)
    posts = []
    for post in packet.get("posts") or []:
        if not isinstance(post, dict):
            continue
        item = dict(post)
        item["fingerprint"] = _compress_fingerprint(item.get("fingerprint"))
        posts.append(item)
    compact["posts"] = posts
    return compact


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _checkpoint_metric(row: dict[str, Any], checkpoint: str, key: str) -> float | None:
    metrics = _as_dict(row.get("checkpoint_metrics"))
    checkpoint_metrics = metrics.get(str(checkpoint or "").lower())
    if not isinstance(checkpoint_metrics, dict):
        return None
    try:
        value = checkpoint_metrics.get(key)
        parsed = float(value)
        return parsed if parsed == parsed and parsed not in {float("inf"), float("-inf")} else None
    except (TypeError, ValueError):
        return None


def _best_checkpoint_pressure(row: dict[str, Any]) -> dict[str, Any]:
    best: tuple[float, str] | None = None
    for checkpoint in ("d3", "d7", "d21"):
        value = _checkpoint_metric(row, checkpoint, "percentile_performance_exact")
        if value is None:
            value = _checkpoint_metric(row, checkpoint, "percentile_performance")
        if value is None:
            continue
        if best is None or value < best[0]:
            best = (value, checkpoint)
    d7 = _checkpoint_metric(row, "d7", "percentile_performance_exact")
    if d7 is None:
        d7 = _checkpoint_metric(row, "d7", "percentile_performance")
    return {
        "best_percentile": round(best[0], 2) if best else None,
        "best_checkpoint": best[1] if best else None,
        "d7_percentile": round(d7, 2) if d7 is not None else None,
        "views_multiple_d7": _checkpoint_metric(row, "d7", "views_multiple"),
        "likes_multiple_d7": _checkpoint_metric(row, "d7", "likes_multiple"),
        "comments_multiple_d7": _checkpoint_metric(row, "d7", "comments_multiple"),
        "views_percentile_d7": _checkpoint_metric(row, "d7", "views_percentile"),
        "likes_percentile_d7": _checkpoint_metric(row, "d7", "likes_percentile"),
        "comments_percentile_d7": _checkpoint_metric(row, "d7", "comments_percentile"),
    }


def _compact_focus_post(row: dict[str, Any]) -> dict[str, Any]:
    fingerprint = _compress_fingerprint(row.get("fingerprint"))
    observed = fingerprint.get("observed") if isinstance(fingerprint.get("observed"), dict) else {}
    fingerprint["observed"] = {
        "caption": str(observed.get("caption") or "")[:700],
        "transcript": str(observed.get("transcript") or "")[:1200],
        "audio_notes": str(observed.get("audio_notes") or "")[:450],
        "visual_notes": str(observed.get("visual_notes") or "")[:1400],
    }
    return {
        "post_key": row.get("post_key"),
        "post_url": row.get("post_url"),
        "posted_at": row.get("posted_at"),
        "media_type": row.get("media_type"),
        "duration_seconds": row.get("duration_seconds"),
        "duration_bucket": row.get("duration_bucket"),
        "carousel_slide_count": row.get("carousel_slide_count"),
        "carousel_depth_bucket": row.get("depth_bucket"),
        "caption_excerpt": str(row.get("caption") or "")[:320],
        "metric_pressure": _best_checkpoint_pressure(row),
        "source_signal_types": _as_list(row.get("source_signal_types")),
        "signal_types": annotate_signal_types(row),
        "fingerprint": fingerprint,
    }


def _movement_post_keys(ledger: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    movements = ledger.get("movements") if isinstance(ledger.get("movements"), dict) else {}
    for movement in movements.values():
        if not isinstance(movement, dict):
            continue
        for field in ("support_posts", "contrast_posts"):
            for key in _as_list(movement.get(field)):
                text = str(key or "").strip()
                if text:
                    keys.add(text)
    return keys


def _movement_entries_for_prompt(ledger: dict[str, Any]) -> list[dict[str, Any]]:
    movements = ledger.get("movements") if isinstance(ledger.get("movements"), dict) else {}
    out: list[dict[str, Any]] = []
    for movement_id, movement in movements.items():
        if not isinstance(movement, dict):
            continue
        pressure_state = movement.get("pressure_state") if isinstance(movement.get("pressure_state"), dict) else {}
        out.append({
            "movement_id": str(movement.get("movement_id") or movement_id),
            "title": movement.get("title"),
            "scope": movement.get("scope"),
            "media_type": movement.get("media_type"),
            "main_axis": movement.get("main_axis"),
            "signal_types": _as_list(movement.get("signal_types")),
            "first_seen": movement.get("first_seen"),
            "last_seen": movement.get("last_seen"),
            "canonical_summary": movement.get("canonical_summary"),
            "metric_line": movement.get("metric_line"),
            "evidence_pressure": _as_list(movement.get("evidence_pressure"))[:6],
            "support_posts": _as_list(movement.get("support_posts"))[:12],
            "contrast_posts": _as_list(movement.get("contrast_posts"))[:12],
            "movement_health": movement.get("movement_health") if isinstance(movement.get("movement_health"), dict) else {},
            "movement_scope": movement.get("movement_scope") if isinstance(movement.get("movement_scope"), dict) else {},
            "pressure_state": {
                "recent_reinforcements": pressure_state.get("recent_reinforcements"),
                "current_gap_strength": pressure_state.get("current_gap_strength") or pressure_state.get("gap_strength"),
                "last_seen": pressure_state.get("last_seen") or movement.get("last_seen"),
                "confidence": pressure_state.get("confidence"),
                "contradiction_load": pressure_state.get("contradiction_load") or pressure_state.get("recent_contradictions") or 0,
            },
        })
    out.sort(key=lambda item: (str(item.get("last_seen") or ""), str(item.get("movement_id") or "")), reverse=True)
    return out[:20]


def _build_feeder_focus_state_packet(
    conn: Any,
    feeder_id: int,
    *,
    full_rebuild: bool,
    post_cap: int,
) -> dict[str, Any]:
    current = _current_feeder_focus(conn, feeder_id)
    ledger = _as_dict(current.get("movement_ledger"))
    account_memory = _as_dict(current.get("account_memory"))
    current_focus_state = {} if full_rebuild else _as_dict(current.get("focus_state"))
    rows = [
        row
        for row in fetch_recent_evidence_rows(conn, feeder_id=feeder_id, window_days=90)
        if isinstance(row.get("fingerprint"), dict) and row.get("fingerprint")
    ]
    support_keys = _movement_post_keys(ledger)
    selected: list[dict[str, Any]] = []
    selected_keys: set[str] = set()

    def add_row(row: dict[str, Any]) -> None:
        key = str(row.get("post_key") or "")
        if not key or key in selected_keys:
            return
        selected.append(row)
        selected_keys.add(key)

    # Keep ledger evidence even when it is older or not among the most recent posts.
    for row in rows:
        if str(row.get("post_key") or "") in support_keys:
            add_row(row)
    for row in rows:
        if len(selected) >= max(1, post_cap):
            break
        add_row(row)

    feeder = {}
    if rows:
        first = rows[0]
        feeder = {
            "feeder_id": first.get("feeder_id"),
            "feed_id": first.get("feed_id"),
            "handle": first.get("handle"),
            "role": first.get("feeder_role"),
            "context_role": first.get("context_role"),
            "context_note": first.get("context_note"),
            "bio": first.get("bio"),
        }
    else:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("select id as feeder_id, feed_id, handle, role, context_role, context_note, bio from public.feeders where id = %s", (feeder_id,))
            fetched = cur.fetchone()
            feeder = dict(fetched) if fetched else {"feeder_id": feeder_id}

    return {
        "mode": "full_rebuild" if full_rebuild else "weekly_nudge",
        "prompt_version": _FEEDER_FOCUS_STATE_PROMPT_VERSION,
        "feeder": feeder,
        "current_focus_state": current_focus_state,
        "current_account_memory": account_memory,
        "movement_ledger": {
            "version": ledger.get("version"),
            "updated_at": ledger.get("updated_at"),
            "movements": _movement_entries_for_prompt(ledger),
        },
        "legacy_rulebook_context": {
            "account_read": (_as_dict(current.get("structured_patterns"))).get("account_read"),
            "by_performance_axis": (_as_dict(current.get("structured_patterns"))).get("by_performance_axis"),
        },
        "metric_profile": _as_dict(current.get("metric_profile")),
        "recent_signal_footnotes": _signal_card_footnotes(
            conn,
            feed_id=int(feeder.get("feed_id") or 0),
            feeder_id=feeder_id,
            limit=12,
        ),
        "fingerprint_knowledge_base": {
            "window_days": 90,
            "total_fingerprinted_posts_seen": len(rows),
            "posts_sent": len(selected),
            "post_cap": max(1, post_cap),
            "posts": [_compact_focus_post(row) for row in selected],
        },
        "compile_rules": {
            "account_context_is_atmospheric_memory": True,
            "movements_own_truth": True,
            "do_not_write_dos_donts": True,
            "do_not_create_pattern_tags": True,
            "full_rebuild_ignores_previous_wording": bool(full_rebuild),
            "weekly_nudge_preserves_stable_language": not full_rebuild,
        },
    }


def _normalize_list(value: Any, fallback: str = "Insufficient evidence.") -> list[str]:
    if isinstance(value, list):
        clean = [str(item).strip() for item in value if str(item or "").strip()]
        return clean[:8]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return [fallback]


def _clip_words(value: Any, limit: int) -> str:
    text = " ".join(str(value or "").split())
    if not text:
        return ""
    words = text.split()
    if len(words) <= limit:
        return text
    return " ".join(words[:limit]).rstrip(".,;:") + "..."


def _short_list(value: Any, *, limit: int = 3, words: int = 24) -> list[str]:
    if isinstance(value, list):
        source = value
    elif value:
        source = [value]
    else:
        source = []
    out: list[str] = []
    seen: set[str] = set()
    for item in source:
        text = _clip_words(item, words)
        if not text or text in seen:
            continue
        out.append(text)
        seen.add(text)
        if len(out) >= limit:
            break
    return out


def _normalize_post_focus_read(value: Any) -> dict[str, Any]:
    data = value if isinstance(value, dict) else {}
    verdict = _clip_words(data.get("verdict") or data.get("read") or data.get("title"), 26)
    matches = _short_list(data.get("matches"), limit=3, words=24)
    if verdict and verdict not in matches:
        matches = [verdict, *matches][:3]
    deviates = _short_list(data.get("deviates"), limit=3, words=24)
    unclear = _short_list(data.get("unclear"), limit=2, words=22)
    notes = _short_list(data.get("notes"), limit=2, words=22)
    do_next = _clip_words(data.get("do_next"), 22)
    watchout = _clip_words(data.get("watchout"), 22)
    if do_next:
        notes.append(f"Do next: {do_next}")
    if watchout:
        notes.append(f"Watchout: {watchout}")

    confidence = str(data.get("confidence") or "medium").strip().lower()
    if confidence not in {"low", "medium", "high"}:
        confidence = "medium"

    return {
        "source": "post_focus",
        "source_label": "Focus read",
        "verdict": verdict,
        "matches": matches,
        "deviates": deviates,
        "unclear": unclear,
        "notes": notes[:4],
        "do_next": do_next,
        "watchout": watchout,
        "confidence": confidence,
    }


def _normalize_feeder_rulebook(value: Any) -> dict[str, Any]:
    data = value if isinstance(value, dict) else {}
    out = {
        "account_read": str(data.get("account_read") or "").strip(),
        "reels": {
            "dos": _normalize_list((data.get("reels") or {}).get("dos") if isinstance(data.get("reels"), dict) else []),
            "donts": _normalize_list((data.get("reels") or {}).get("donts") if isinstance(data.get("reels"), dict) else [], fallback="No strong don'ts yet."),
        },
        "carousels": {
            "dos": _normalize_list((data.get("carousels") or {}).get("dos") if isinstance(data.get("carousels"), dict) else []),
            "donts": _normalize_list((data.get("carousels") or {}).get("donts") if isinstance(data.get("carousels"), dict) else [], fallback="No strong don'ts yet."),
        },
        "statics": {
            "dos": _normalize_list((data.get("statics") or {}).get("dos") if isinstance(data.get("statics"), dict) else []),
            "donts": _normalize_list((data.get("statics") or {}).get("donts") if isinstance(data.get("statics"), dict) else [], fallback="No strong don'ts yet."),
        },
        "by_performance_axis": data.get("by_performance_axis") if isinstance(data.get("by_performance_axis"), dict) else {},
        "shifts_since_last_compile": _normalize_list(data.get("shifts_since_last_compile"), fallback="No meaningful shift since last compile."),
        "known_unknowns": _normalize_list(data.get("known_unknowns"), fallback="No major unknowns flagged."),
    }
    if not out["account_read"]:
        out["account_read"] = "Insufficient identity evidence. More fingerprinted posts are needed before the account read can be trusted."
    return out


def _normalize_feed_rulebook(value: Any) -> dict[str, Any]:
    data = value if isinstance(value, dict) else {}

    def section(name: str) -> dict[str, list[str]]:
        raw = data.get(name) if isinstance(data.get(name), dict) else {}
        return {
            "cross_feed_dos": _normalize_list(raw.get("cross_feed_dos"), fallback="Insufficient evidence."),
            "cross_feed_donts": _normalize_list(raw.get("cross_feed_donts"), fallback="No strong don'ts yet."),
            "divergences": _normalize_list(raw.get("divergences"), fallback="No major divergence flagged."),
        }

    capsules = data.get("capsules") if isinstance(data.get("capsules"), dict) else {}
    out = {
        "feed_read": str(data.get("feed_read") or "").strip(),
        "reels": section("reels"),
        "carousels": section("carousels"),
        "statics": section("statics"),
        "by_performance_axis": data.get("by_performance_axis") if isinstance(data.get("by_performance_axis"), dict) else {},
        "cohort_dynamics": data.get("cohort_dynamics") if isinstance(data.get("cohort_dynamics"), dict) else {"leading": [], "challenging": [], "stalling": []},
        "anchor_vs_feed": str(data.get("anchor_vs_feed") or "").strip(),
        "capsules": {
            "common": str(capsules.get("common") or "").strip(),
            "reel": str(capsules.get("reel") or "").strip(),
            "carousel": str(capsules.get("carousel") or "").strip(),
            "image": str(capsules.get("image") or "").strip(),
            "anchor": str(capsules.get("anchor") or "").strip(),
            "cross": str(capsules.get("cross") or "").strip(),
        },
        "shifts_since_last_compile": _normalize_list(data.get("shifts_since_last_compile"), fallback="No meaningful shift since last compile."),
        "known_unknowns": _normalize_list(data.get("known_unknowns"), fallback="No major unknowns flagged."),
    }
    if not out["feed_read"]:
        out["feed_read"] = "Insufficient feed evidence. More active feeders or fingerprinted posts are needed before the feed read can be trusted."
    if not out["capsules"]["common"]:
        out["capsules"]["common"] = out["feed_read"][:500]
    return out


def _md_list(items: list[str]) -> str:
    return "\n".join(f"- {item}" for item in items if item)


def _render_feeder_common(rulebook: dict[str, Any]) -> str:
    axes = rulebook.get("by_performance_axis") if isinstance(rulebook.get("by_performance_axis"), dict) else {}
    bits = [
        "## Account Read",
        str(rulebook.get("account_read") or ""),
        "## By Performance Axis",
        *[f"- **{key.replace('_', ' ').title()}**: {value}" for key, value in axes.items() if str(value or "").strip()],
        "## Shifts Since Last Compile",
        _md_list(rulebook.get("shifts_since_last_compile") or []),
        "## Known Unknowns",
        _md_list(rulebook.get("known_unknowns") or []),
    ]
    return "\n".join(part for part in bits if str(part or "").strip())


def _render_feeder_format(rulebook: dict[str, Any], name: str) -> str:
    section = rulebook.get(name) if isinstance(rulebook.get(name), dict) else {}
    title = {"reels": "Reels", "carousels": "Carousels", "statics": "Statics"}.get(name, name.title())
    return "\n".join([
        f"## {title} Dos",
        _md_list(section.get("dos") or []),
        f"## {title} Donts",
        _md_list(section.get("donts") or []),
    ]).strip()


def _render_feed_md(rulebook: dict[str, Any]) -> str:
    bits = ["## Feed Read", str(rulebook.get("feed_read") or "")]
    for key, title in (("reels", "Reels"), ("carousels", "Carousels"), ("statics", "Statics")):
        section = rulebook.get(key) if isinstance(rulebook.get(key), dict) else {}
        bits.extend([
            f"## {title} Dos",
            _md_list(section.get("cross_feed_dos") or []),
            f"## {title} Donts",
            _md_list(section.get("cross_feed_donts") or []),
            f"## {title} Divergences",
            _md_list(section.get("divergences") or []),
        ])
    axes = rulebook.get("by_performance_axis") if isinstance(rulebook.get("by_performance_axis"), dict) else {}
    bits.extend(["## By Performance Axis", *[f"- **{key.replace('_', ' ').title()}**: {value}" for key, value in axes.items() if str(value or "").strip()]])
    bits.extend(["## Anchor vs Feed", str(rulebook.get("anchor_vs_feed") or "")])
    bits.extend(["## Known Unknowns", _md_list(rulebook.get("known_unknowns") or [])])
    return "\n".join(part for part in bits if str(part or "").strip())


def _select_feeder_ids(conn: Any, feeder_id: int | None, limit: int) -> list[int]:
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
        return [int(row["id"]) for row in cur.fetchall()]


def _select_feed_ids(conn: Any, feed_id: int | None, limit: int) -> list[int]:
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
        return [int(row["id"]) for row in cur.fetchall()]


def compile_feeder_focus(
    conn: Any,
    feeder_id: int | None = None,
    *,
    limit: int = 20,
    full_rebuild: bool = False,
    warm_start: bool = False,
    post_cap: int | None = None,
) -> dict[str, int]:
    if not SIGNAL_INTELLIGENCE_ENABLED:
        return {"selected": 0, "compiled": 0, "skipped": 0, "failed": 0}
    feeder_ids = _select_feeder_ids(conn, feeder_id, limit)
    selected = len(feeder_ids)
    compiled = skipped = failed = empty_evidence = 0
    for fid in feeder_ids:
        if not _claim_compile_lock(conn, "feeder", fid, force=bool(full_rebuild or warm_start)):
            skipped += 1
            continue
        try:
            packet_cap = max(1, int(post_cap or (15 if warm_start else _RULEBOOK_HARD_CAP)))
            packet = (
                build_feeder_warm_start_packet(conn, fid, hard_cap=packet_cap)
                if warm_start
                else build_feeder_evidence_packet(conn, fid, hard_cap=packet_cap)
            )
            _ensure_packet_fingerprints(conn, packet)
            packet = (
                build_feeder_warm_start_packet(conn, fid, hard_cap=packet_cap)
                if warm_start
                else build_feeder_evidence_packet(conn, fid, hard_cap=packet_cap)
            )
            if not packet.get("posts"):
                skipped += 1
                empty_evidence += 1
                _mark_compile_lock(conn, "feeder", fid, success=False, error="empty_evidence_packet")
                continue

            current = _current_feeder_focus(conn, fid)
            rebuild_now = bool(full_rebuild or _full_rebuild_due(current))
            compile_kind = "warm_start" if warm_start else "full_rebuild" if rebuild_now else "weekly_update"
            model = FOCUS_REBUILD_MODEL if rebuild_now else FOCUS_COMPILER_MODEL
            previous_rulebook = {} if rebuild_now else current.get("structured_patterns") or {}
            prompt_packet = _compile_packet_for_prompt(packet)
            footnotes = _signal_card_footnotes(
                conn,
                feed_id=int((packet.get("feeder") or {}).get("feed_id") or 0),
                feeder_id=fid,
            )
            source_hash = _sha({
                "prompt_version": _FEEDER_RULEBOOK_PROMPT_VERSION,
                "packet": {
                    "summary": packet.get("summary"),
                    "post_keys": [post.get("post_key") for post in packet.get("posts") or []],
                    "post_fingerprint_hashes": [_sha(post.get("fingerprint")) for post in packet.get("posts") or []],
                },
                "footnotes": footnotes,
                "mode": compile_kind,
                "warm_start": warm_start,
                "post_cap": packet_cap,
            })
            if not rebuild_now and current.get("source_hash") == source_hash:
                skipped += 1
                _mark_compile_lock(conn, "feeder", fid, success=True)
                continue

            result = _call_json_model(
                _FEEDER_RULEBOOK_SYSTEM,
                {
                    "mode": compile_kind,
                    "previous_rulebook": previous_rulebook,
                    "evidence_packet": prompt_packet,
                    "signal_card_footnotes_since_last_compile": footnotes,
                    "hard_rules": {
                        "saves_and_shares": "out_of_scope",
                        "no_pattern_tags": True,
                        "hard_cap_posts": packet_cap,
                        "warm_start": warm_start,
                    },
                },
                model=model,
                max_tokens=5200,
            )
            if not result:
                failed += 1
                _mark_compile_lock(conn, "feeder", fid, success=False, error="model_failed_or_malformed")
                continue
            rulebook = _normalize_feeder_rulebook(result)
            current_version = int(current.get("focus_version") or 0)
            version = current_version + 1
            derived_views = {
                "ticker_facts": ticker_facts_from_feeder_rulebook(rulebook, packet),
                "capsule": str(rulebook.get("account_read") or "")[:700],
            }
            compile_meta = {
                "architecture": "focus_rulebook_v4",
                "compile_kind": compile_kind,
                "warm_start": warm_start,
                "post_cap": packet_cap,
                "packet_summary": packet.get("summary"),
                "compiled_at": datetime.now(timezone.utc).isoformat(),
            }
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
                        int((packet.get("feeder") or {}).get("feed_id") or 0),
                        json.dumps(rulebook),
                        json.dumps(packet.get("summary") or {}),
                        _render_feeder_common(rulebook),
                        _render_feeder_format(rulebook, "reels"),
                        _render_feeder_format(rulebook, "statics"),
                        _render_feeder_format(rulebook, "carousels"),
                        version,
                        _FEEDER_RULEBOOK_PROMPT_VERSION,
                        model,
                        source_hash,
                        json.dumps({"account_read": rulebook.get("account_read"), "by_performance_axis": rulebook.get("by_performance_axis")}),
                        json.dumps({key: packet.get(key) for key in ("metric_windows", "follower_trends", "cadence", "format_mix")}),
                        json.dumps([]),
                        json.dumps(derived_views),
                        json.dumps({"shifts_since_last_compile": rulebook.get("shifts_since_last_compile") or []}),
                        json.dumps(compile_meta),
                        rebuild_now,
                        rebuild_now,
                    ),
                )
            conn.commit()
            _mark_compile_lock(conn, "feeder", fid, success=True)
            compiled += 1
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            failed += 1
            _mark_compile_lock(conn, "feeder", fid, success=False, error=str(exc))
    return {"selected": selected, "compiled": compiled, "skipped": skipped, "failed": failed, "empty_evidence": empty_evidence}


def _normalize_confidence_basis(value: Any) -> dict[str, Any]:
    data = value if isinstance(value, dict) else {}
    contrast_clarity = str(data.get("contrast_clarity") or "weak").strip().lower()
    if contrast_clarity not in {"weak", "medium", "strong"}:
        contrast_clarity = "weak"
    try:
        movement_support = int(data.get("movement_support") or data.get("support_posts") or 0)
    except Exception:
        movement_support = 0
    try:
        contradictions = int(data.get("contradictions") or 0)
    except Exception:
        contradictions = 0
    return {
        "movement_support": max(0, movement_support),
        "contrast_clarity": contrast_clarity,
        "axis_alignment": _dedupe_strings(_as_list(data.get("axis_alignment")), limit=8),
        "contradictions": max(0, contradictions),
    }


def _dedupe_strings(values: list[Any], *, limit: int = 8) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
        if len(out) >= limit:
            break
    return out


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _normalize_movement_health(value: Any, pressure_state: dict[str, Any], confidence_basis: dict[str, Any]) -> dict[str, Any]:
    data = value if isinstance(value, dict) else {}
    decay_risk = str(data.get("decay_risk") or "unknown").strip().lower()
    if decay_risk not in {"low", "medium", "high", "unknown"}:
        decay_risk = "unknown"
    days_raw = data.get("days_since_reinforced")
    try:
        days_since = int(days_raw) if days_raw is not None and str(days_raw).strip() != "" else None
    except Exception:
        days_since = None
    return {
        "recent_support": max(0, _safe_int(data.get("recent_support"), _safe_int(confidence_basis.get("support_posts"), 0))),
        "recent_contradictions": max(0, _safe_int(data.get("recent_contradictions"), _safe_int(pressure_state.get("recent_contradictions"), 0))),
        "last_major_separation": str(data.get("last_major_separation") or pressure_state.get("last_reinforced_at") or "").strip(),
        "days_since_reinforced": days_since,
        "decay_risk": decay_risk,
    }


def _normalize_movement_scope(value: Any, fallback_media: Any, fallback_axes: list[str]) -> dict[str, list[str]]:
    data = value if isinstance(value, dict) else {}

    def clean_list(key: str, *, limit: int = 8) -> list[str]:
        values = [str(item).strip().lower() for item in _as_list(data.get(key)) if str(item or "").strip()]
        seen: set[str] = set()
        out: list[str] = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            out.append(value)
            if len(out) >= limit:
                break
        return out

    formats = clean_list("formats")
    if not formats:
        media = _media_capsule_key(fallback_media)
        formats = [media] if media else []
    axes = clean_list("axes")
    for axis in fallback_axes:
        axis_key = str(axis or "").strip().lower()
        if axis_key == "d7":
            axis_key = "durability"
        if axis_key and axis_key not in axes:
            axes.append(axis_key)
    return {
        "formats": formats[:6],
        "duration_buckets": clean_list("duration_buckets", limit=6),
        "contexts": clean_list("contexts", limit=10),
        "axes": axes[:8],
    }


def _normalize_pressure_hierarchy(items: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in _as_list(items):
        if not isinstance(item, dict):
            continue
        pressure_state = item.get("pressure_state") if isinstance(item.get("pressure_state"), dict) else {}
        confidence_basis = item.get("confidence_basis") if isinstance(item.get("confidence_basis"), dict) else {}
        axis_consistency = str(pressure_state.get("axis_consistency") or "medium").strip().lower()
        if axis_consistency not in {"low", "medium", "high"}:
            axis_consistency = "medium"
        contrast_clarity = str(confidence_basis.get("contrast_clarity") or "weak").strip().lower()
        if contrast_clarity not in {"weak", "medium", "strong"}:
            contrast_clarity = "weak"
        pressure_state_out = {
            "recent_reinforcements": int(pressure_state.get("recent_reinforcements") or 0),
            "recent_contradictions": int(pressure_state.get("recent_contradictions") or 0),
            "gap_strength": pressure_state.get("gap_strength"),
            "axis_consistency": axis_consistency,
            "last_reinforced_at": str(pressure_state.get("last_reinforced_at") or pressure_state.get("last_seen") or "").strip(),
        }
        confidence_basis_out = {
            "support_posts": int(confidence_basis.get("support_posts") or 0),
            "contrast_posts": int(confidence_basis.get("contrast_posts") or 0),
            "contrast_clarity": contrast_clarity,
            "axis_alignment": _dedupe_strings(_as_list(confidence_basis.get("axis_alignment")), limit=8),
            "contradictions": int(confidence_basis.get("contradictions") or 0),
        }
        out.append({
            "movement_id": str(item.get("movement_id") or "").strip(),
            "title": str(item.get("title") or "").strip(),
            "pressure_read": str(item.get("pressure_read") or item.get("why_it_matters_now") or "").strip(),
            "metric_pressure": str(item.get("metric_pressure") or "").strip(),
            "evidence_posts": _dedupe_strings(_as_list(item.get("evidence_posts")), limit=12),
            "contrast_posts": _dedupe_strings(_as_list(item.get("contrast_posts")), limit=12),
            "evidence_pressure": _short_list(item.get("evidence_pressure"), limit=6, words=28),
            "pressure_state": pressure_state_out,
            "movement_health": _normalize_movement_health(item.get("movement_health"), pressure_state_out, confidence_basis_out),
            "movement_scope": _normalize_movement_scope(
                item.get("movement_scope"),
                item.get("media_type"),
                confidence_basis_out.get("axis_alignment") or [],
            ),
            "confidence_basis": confidence_basis_out,
        })
        if len(out) >= 8:
            break
    return out


def _normalize_axis_map(value: Any, hierarchy: list[dict[str, Any]]) -> dict[str, list[str]]:
    data = value if isinstance(value, dict) else {}
    out: dict[str, list[str]] = {}
    for axis in ("comments", "views", "likes", "followers", "durability"):
        ids = [
            str(item).strip()
            for item in _as_list(data.get(axis))
            if str(item or "").strip().startswith("movement:")
        ][:8]
        out[axis] = ids
    for movement in hierarchy:
        movement_id = str(movement.get("movement_id") or "").strip()
        if not movement_id:
            continue
        axes = list(_as_list((movement.get("confidence_basis") or {}).get("axis_alignment")))
        axes.extend(_as_list((movement.get("movement_scope") or {}).get("axes")))
        for axis in axes:
            axis_key = str(axis or "").strip().lower()
            if axis_key == "d7":
                axis_key = "durability"
            if axis_key not in out:
                continue
            if movement_id not in out[axis_key]:
                out[axis_key].append(movement_id)
    return {axis: values[:8] for axis, values in out.items()}


def _normalize_feeder_focus_state(value: Any) -> dict[str, Any]:
    root = value.get("feeder_focus") if isinstance(value, dict) and isinstance(value.get("feeder_focus"), dict) else value
    data = root if isinstance(root, dict) else {}
    account_read = data.get("account_read") if isinstance(data.get("account_read"), dict) else {}
    hierarchy = _normalize_pressure_hierarchy(data.get("pressure_hierarchy"))
    focus = {
        "account_read": {
            "surface_world": str(account_read.get("surface_world") or "").strip(),
            "voice_contract": str(account_read.get("voice_contract") or "").strip(),
            "audience_role": str(account_read.get("audience_role") or "").strip(),
            "proof_style": str(account_read.get("proof_style") or "").strip(),
            "identity_tension": str(account_read.get("identity_tension") or "").strip(),
            "current_gravity": str(account_read.get("current_gravity") or "").strip(),
            "confidence_basis": _normalize_confidence_basis(account_read.get("confidence_basis")),
        },
        "current_pressure_interpretation": str(data.get("current_pressure_interpretation") or "").strip(),
        "pressure_hierarchy": hierarchy,
        "axis_map": _normalize_axis_map(data.get("axis_map"), hierarchy),
        "recent_shifts": _short_list(data.get("recent_shifts"), limit=8, words=38),
        "movement_contradictions": _short_list(data.get("movement_contradictions"), limit=8, words=38),
        "unresolved_edges": _short_list(data.get("unresolved_edges"), limit=8, words=38),
        "evidence_gaps": _short_list(data.get("evidence_gaps"), limit=8, words=34),
        "pressure_uncertainties": _short_list(data.get("pressure_uncertainties"), limit=8, words=34),
        "untested_variants": _short_list(data.get("untested_variants"), limit=8, words=34),
        "known_false_signals": _short_list(data.get("known_false_signals"), limit=8, words=34),
        "known_limits": _short_list(data.get("known_limits"), limit=8, words=34),
    }
    if not focus["pressure_hierarchy"]:
        focus["unresolved_edges"].append("No defended movement hierarchy yet. More signal-backed separations needed.")
    return focus


def _focus_state_common_text(focus_state: dict[str, Any]) -> str:
    account = focus_state.get("account_read") if isinstance(focus_state.get("account_read"), dict) else {}
    lines = ["# Movement Focus"]
    if focus_state.get("current_pressure_interpretation"):
        lines.extend(["", str(focus_state.get("current_pressure_interpretation"))])
    for key, label in (
        ("surface_world", "Surface world"),
        ("voice_contract", "Voice contract"),
        ("audience_role", "Audience role"),
        ("proof_style", "Proof style"),
        ("identity_tension", "Identity tension"),
        ("current_gravity", "Current gravity"),
    ):
        value = str(account.get(key) or "").strip()
        if value:
            lines.append(f"- **{label}**: {value}")
    return "\n".join(lines).strip()


def compile_feeder_focus_state(
    conn: Any,
    feeder_id: int | None = None,
    *,
    limit: int = 20,
    full_rebuild: bool = False,
    post_cap: int | None = None,
) -> dict[str, int]:
    if not SIGNAL_INTELLIGENCE_ENABLED:
        return {"selected": 0, "compiled": 0, "skipped": 0, "failed": 0}
    feeder_ids = _select_feeder_ids(conn, feeder_id, limit)
    selected = len(feeder_ids)
    compiled = skipped = failed = empty_evidence = 0
    for fid in feeder_ids:
        if not _claim_compile_lock(conn, "feeder", fid, force=bool(full_rebuild)):
            skipped += 1
            continue
        try:
            packet_cap = max(8, int(post_cap or 60))
            packet = _build_feeder_focus_state_packet(conn, fid, full_rebuild=full_rebuild, post_cap=packet_cap)
            posts_sent = int(((packet.get("fingerprint_knowledge_base") or {}).get("posts_sent")) or 0)
            if posts_sent <= 0:
                skipped += 1
                empty_evidence += 1
                _mark_compile_lock(conn, "feeder", fid, success=False, error="empty_fingerprint_knowledge_base")
                continue

            current = _current_feeder_focus(conn, fid)
            source_hash = _sha({
                "prompt_version": _FEEDER_FOCUS_STATE_PROMPT_VERSION,
                "mode": packet.get("mode"),
                "post_keys": [
                    post.get("post_key")
                    for post in ((packet.get("fingerprint_knowledge_base") or {}).get("posts") or [])
                    if isinstance(post, dict)
                ],
                "post_fingerprint_hashes": [
                    _sha(post.get("fingerprint"))
                    for post in ((packet.get("fingerprint_knowledge_base") or {}).get("posts") or [])
                    if isinstance(post, dict)
                ],
                "movement_ledger": packet.get("movement_ledger"),
                "account_memory": packet.get("current_account_memory"),
            })
            derived_views = _as_dict(current.get("derived_views"))
            if not full_rebuild and str(derived_views.get("focus_state_source_hash") or "") == source_hash:
                skipped += 1
                _mark_compile_lock(conn, "feeder", fid, success=True)
                continue

            result = _call_json_model(
                _FEEDER_FOCUS_STATE_SYSTEM,
                packet,
                model=FOCUS_REBUILD_MODEL if full_rebuild else FOCUS_COMPILER_MODEL,
                max_tokens=6200,
            )
            if not result:
                failed += 1
                _mark_compile_lock(conn, "feeder", fid, success=False, error="model_failed_or_malformed")
                continue
            focus_state = _normalize_feeder_focus_state(result)
            current_version = int(current.get("focus_version") or 0)
            version = current_version + 1
            now_iso = datetime.now(timezone.utc).isoformat()
            account_memory = _as_dict(current.get("account_memory"))
            account_memory.update({
                **(focus_state.get("account_read") if isinstance(focus_state.get("account_read"), dict) else {}),
                "version": "account_memory_v1",
                "philosophy": "atmospheric_memory_not_decision_memory",
                "updated_at": now_iso,
            })
            derived_views.update({
                "movement_feeder_focus": focus_state,
                "focus_state_source_hash": source_hash,
                "capsule": str(focus_state.get("current_pressure_interpretation") or "")[:700],
            })
            compile_meta = {
                "architecture": "movement_focus_state_v1",
                "compile_kind": "full_rebuild" if full_rebuild else "weekly_nudge",
                "post_cap": packet_cap,
                "fingerprinted_posts_seen": ((packet.get("fingerprint_knowledge_base") or {}).get("total_fingerprinted_posts_seen")),
                "posts_sent": posts_sent,
                "compiled_at": now_iso,
            }
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into public.feeder_focus (
                      feeder_id, feed_id, focus_state, account_memory, evidence_summary,
                      focus_md_common, focus_version, prompt_version, model_version,
                      derived_views, compile_meta, focus_updated_at,
                      last_full_rebuild_at, created_at, updated_at
                    )
                    values (%s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, %s, %s,
                            %s::jsonb, %s::jsonb, now(), case when %s then now() else null end, now(), now())
                    on conflict (feeder_id) do update set
                      feed_id = coalesce(excluded.feed_id, public.feeder_focus.feed_id),
                      focus_state = excluded.focus_state,
                      account_memory = excluded.account_memory,
                      evidence_summary = coalesce(public.feeder_focus.evidence_summary, '{}'::jsonb) || excluded.evidence_summary,
                      focus_md_common = excluded.focus_md_common,
                      focus_version = excluded.focus_version,
                      prompt_version = excluded.prompt_version,
                      model_version = excluded.model_version,
                      derived_views = excluded.derived_views,
                      compile_meta = excluded.compile_meta,
                      focus_updated_at = now(),
                      last_full_rebuild_at = case when %s then now() else public.feeder_focus.last_full_rebuild_at end,
                      updated_at = now()
                    """,
                    (
                        fid,
                        int((packet.get("feeder") or {}).get("feed_id") or 0),
                        json.dumps(focus_state),
                        json.dumps(account_memory),
                        json.dumps({"movement_focus": {
                            "fingerprinted_posts_seen": ((packet.get("fingerprint_knowledge_base") or {}).get("total_fingerprinted_posts_seen")),
                            "posts_sent": posts_sent,
                            "compiled_at": now_iso,
                        }}),
                        _focus_state_common_text(focus_state),
                        version,
                        _FEEDER_FOCUS_STATE_PROMPT_VERSION,
                        FOCUS_REBUILD_MODEL if full_rebuild else FOCUS_COMPILER_MODEL,
                        json.dumps(derived_views),
                        json.dumps(compile_meta),
                        full_rebuild,
                        full_rebuild,
                    ),
                )
            conn.commit()
            _mark_compile_lock(conn, "feeder", fid, success=True)
            compiled += 1
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            failed += 1
            _mark_compile_lock(conn, "feeder", fid, success=False, error=str(exc))
    return {"selected": selected, "compiled": compiled, "skipped": skipped, "failed": failed, "empty_evidence": empty_evidence}


def compile_feed_focus(conn: Any, feed_id: int | None = None, *, limit: int = 10, full_rebuild: bool = False) -> dict[str, int]:
    if not SIGNAL_INTELLIGENCE_ENABLED:
        return {"selected": 0, "compiled": 0, "skipped": 0, "failed": 0}
    feed_ids = _select_feed_ids(conn, feed_id, limit)
    selected = len(feed_ids)
    compiled = skipped = failed = empty_evidence = 0
    for fid in feed_ids:
        if not _claim_compile_lock(conn, "feed", fid, force=full_rebuild):
            skipped += 1
            continue
        try:
            packet = build_feed_evidence_packet(conn, fid, hard_cap=_RULEBOOK_HARD_CAP)
            _ensure_packet_fingerprints(conn, packet)
            packet = build_feed_evidence_packet(conn, fid, hard_cap=_RULEBOOK_HARD_CAP)
            if not packet.get("posts"):
                skipped += 1
                empty_evidence += 1
                _mark_compile_lock(conn, "feed", fid, success=False, error="empty_evidence_packet")
                continue

            current = _current_feed_focus(conn, fid)
            rebuild_now = bool(full_rebuild or _full_rebuild_due(current))
            model = FOCUS_REBUILD_MODEL if rebuild_now else FOCUS_COMPILER_MODEL
            previous_rulebook = {} if rebuild_now else current.get("structured_patterns") or {}
            footnotes = _signal_card_footnotes(conn, feed_id=fid)
            source_hash = _sha({
                "prompt_version": _FEED_RULEBOOK_PROMPT_VERSION,
                "packet": {
                    "summary": packet.get("summary"),
                    "post_keys": [post.get("post_key") for post in packet.get("posts") or []],
                    "post_fingerprint_hashes": [_sha(post.get("fingerprint")) for post in packet.get("posts") or []],
                },
                "footnotes": footnotes,
                "mode": "full_rebuild" if rebuild_now else "weekly_update",
            })
            if not rebuild_now and current.get("source_hash") == source_hash:
                skipped += 1
                _mark_compile_lock(conn, "feed", fid, success=True)
                continue

            result = _call_json_model(
                _FEED_RULEBOOK_SYSTEM,
                {
                    "mode": "full_rebuild" if rebuild_now else "weekly_update",
                    "previous_rulebook": previous_rulebook,
                    "evidence_packet": _compile_packet_for_prompt(packet),
                    "signal_card_footnotes_since_last_compile": footnotes,
                    "hard_rules": {
                        "saves_and_shares": "out_of_scope",
                        "no_pattern_tags": True,
                        "hard_cap_posts": _RULEBOOK_HARD_CAP,
                    },
                },
                model=model,
                max_tokens=6200,
            )
            if not result:
                failed += 1
                _mark_compile_lock(conn, "feed", fid, success=False, error="model_failed_or_malformed")
                continue
            rulebook = _normalize_feed_rulebook(result)
            capsules = rulebook.get("capsules") if isinstance(rulebook.get("capsules"), dict) else {}
            current_version = int(current.get("focus_version") or 0)
            version = current_version + 1
            derived_views = {
                "ticker_facts": ticker_facts_from_feed_rulebook(rulebook, packet),
            }
            compile_meta = {
                "architecture": "focus_rulebook_v4",
                "compile_kind": "full_rebuild" if rebuild_now else "weekly_update",
                "packet_summary": packet.get("summary"),
                "compiled_at": datetime.now(timezone.utc).isoformat(),
            }
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
                        json.dumps(rulebook),
                        json.dumps({"anchor_vs_feed": rulebook.get("anchor_vs_feed")}),
                        json.dumps(packet.get("metric_windows") or {}),
                        json.dumps({"selected_post_keys": [post.get("post_key") for post in packet.get("posts") or []]}),
                        _render_feed_md(rulebook),
                        str(capsules.get("common") or ""),
                        str(capsules.get("reel") or ""),
                        str(capsules.get("image") or ""),
                        str(capsules.get("carousel") or ""),
                        str(capsules.get("anchor") or ""),
                        str(capsules.get("cross") or ""),
                        version,
                        _FEED_RULEBOOK_PROMPT_VERSION,
                        model,
                        source_hash,
                        json.dumps({"feed_read": rulebook.get("feed_read"), "by_performance_axis": rulebook.get("by_performance_axis")}),
                        json.dumps({key: packet.get(key) for key in ("metric_windows", "follower_trends", "cadence", "format_mix")}),
                        json.dumps([]),
                        json.dumps(rulebook.get("cohort_dynamics") or {}),
                        json.dumps({"divergences": {
                            key: (rulebook.get(key) or {}).get("divergences", [])
                            for key in ("reels", "carousels", "statics")
                            if isinstance(rulebook.get(key), dict)
                        }}),
                        json.dumps(derived_views),
                        json.dumps({"shifts_since_last_compile": rulebook.get("shifts_since_last_compile") or []}),
                        json.dumps(compile_meta),
                        rebuild_now,
                        rebuild_now,
                    ),
                )
            conn.commit()
            _mark_compile_lock(conn, "feed", fid, success=True)
            compiled += 1
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            failed += 1
            _mark_compile_lock(conn, "feed", fid, success=False, error=str(exc))
    return {"selected": selected, "compiled": compiled, "skipped": skipped, "failed": failed, "empty_evidence": empty_evidence}


def _fetch_post_focus_candidates(
    conn: Any,
    *,
    post_key: str | None = None,
    feeder_id: int | None = None,
    limit: int = 5,
) -> list[dict[str, Any]]:
    where: list[str] = []
    params: list[Any] = []
    if post_key:
        where.append("p.post_key = %s")
        params.append(str(post_key).strip().lower())
    else:
        where.append("p.posted_at >= now() - interval '120 days'")
        where.append("(coalesce(m.best_percentile, 101) <= 35 or coalesce(array_length(st.source_signal_types, 1), 0) > 0)")
        if feeder_id is not None:
            where.append("fd.id = %s")
            params.append(feeder_id)
    params.append(max(1, int(limit)))
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            select
              p.post_key, p.post_url, p.caption, lower(coalesce(p.media_type, 'image')) as media_type,
              p.posted_at, p.duration_seconds, p.duration_bucket, p.carousel_slide_count, p.depth_bucket,
              fd.id as feeder_id, fd.feed_id, fd.handle,
              pf.fingerprint, pf.model_version as fingerprint_model_version,
              pf.media_source_hash as fingerprint_media_source_hash,
              ff.focus_version as feeder_focus_version,
              m.checkpoint_metrics, m.best_percentile, m.last_metric_at,
              coalesce(st.source_signal_types, '{{}}'::text[]) as source_signal_types,
              pfr.fingerprint_hash as existing_source_hash,
              pfr.prompt_version as existing_prompt_version,
              pfr.model_version as existing_model_version,
              pfr.feeder_focus_version as existing_feeder_focus_version
            from public.posts p
            join public.feeders fd on fd.id = p.feeder_id
            left join public.post_fingerprints pf on pf.post_key = p.post_key
            left join public.feeder_focus ff on ff.feeder_id = fd.id
            left join public.post_focus_reads pfr on pfr.post_key = p.post_key
            left join lateral (
              select
                coalesce(
                  jsonb_object_agg(
                    lower(pm.checkpoint),
                    jsonb_build_object(
                      'checkpoint', pm.checkpoint,
                      'business_date_ist', pm.business_date_ist,
                      'computed_at', pm.computed_at,
                      'views', pm.views,
                      'likes', pm.likes,
                      'comments', pm.comments,
                      'metric_value', pm.metric_value,
                      'percentile_performance', pm.percentile_performance,
                      'percentile_performance_exact', pm.percentile_performance_exact,
                      'views_percentile', pm.views_percentile,
                      'likes_percentile', pm.likes_percentile,
                      'comments_percentile', pm.comments_percentile,
                      'feed_percentile', pm.feed_percentile,
                      'ranking_metric', pm.ranking_metric,
                      'ranking_multiple', pm.ranking_multiple,
                      'views_multiple', pm.views_multiple,
                      'likes_multiple', pm.likes_multiple,
                      'comments_multiple', pm.comments_multiple,
                      'hour_multiple', pm.hour_multiple
                    )
                  ) filter (where pm.checkpoint is not null),
                  '{{}}'::jsonb
                ) as checkpoint_metrics,
                min(coalesce(pm.percentile_performance_exact, pm.percentile_performance)) as best_percentile,
                max(pm.computed_at) as last_metric_at
              from public.post_metrics pm
              where pm.post_key = p.post_key
                and lower(pm.checkpoint) in ('d1', 'd3', 'd7', 'd21')
            ) m on true
            left join lateral (
              select coalesce(
                array_remove(array_agg(distinct s.signal_type), null),
                '{{}}'::text[]
              ) as source_signal_types
              from public.signal_posts sp
              join public.signals s on s.id = sp.signal_id
              where sp.post_key = p.post_key
                and s.created_at >= now() - interval '90 days'
            ) st on true
            where {' and '.join(where)}
            order by
              case when pfr.post_key is null then 0 else 1 end,
              coalesce(m.best_percentile, 101) asc,
              m.last_metric_at desc nulls last,
              p.posted_at desc nulls last
            limit %s
            """,
            tuple(params),
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def _post_focus_model_version() -> str:
    provider = _provider()
    if not provider:
        return ""
    return f"{provider}:{_model(provider, FOCUS_COMPILER_MODEL)}:{_POST_FOCUS_PROMPT_VERSION}"


def resolve_post_focus_reads(
    conn: Any,
    post_key: str | None = None,
    *,
    feeder_id: int | None = None,
    limit: int = 5,
) -> dict[str, Any]:
    if not SIGNAL_INTELLIGENCE_ENABLED:
        return {"selected": 0, "resolved": 0, "skipped": 0, "failed": 0, "post_keys": []}
    from .signal_intelligence import ensure_post_fingerprint

    candidates = _fetch_post_focus_candidates(conn, post_key=post_key, feeder_id=feeder_id, limit=limit)
    selected = len(candidates)
    resolved = skipped = failed = 0
    resolved_keys: list[str] = []
    model_version = _post_focus_model_version()
    if not model_version:
        return {"selected": selected, "resolved": 0, "skipped": 0, "failed": selected, "post_keys": []}

    for row in candidates:
        key = str(row.get("post_key") or "").strip().lower()
        if not key:
            skipped += 1
            continue
        try:
            fingerprint = row.get("fingerprint") if isinstance(row.get("fingerprint"), dict) else None
            if not fingerprint:
                fingerprint = ensure_post_fingerprint(conn, key)
            if not isinstance(fingerprint, dict) or not fingerprint:
                failed += 1
                continue

            checkpoint_metrics = row.get("checkpoint_metrics") if isinstance(row.get("checkpoint_metrics"), dict) else {}
            annotation_row = {
                "media_type": row.get("media_type"),
                "checkpoint_metrics": checkpoint_metrics,
                "source_signal_types": row.get("source_signal_types") if isinstance(row.get("source_signal_types"), list) else [],
            }
            signal_types = annotate_signal_types(annotation_row)
            feed_id = int(row.get("feed_id") or 0)
            fid = int(row.get("feeder_id") or 0)
            media_type = str(row.get("media_type") or "image").strip().lower()
            rulebook_slice = feeder_rulebook_slice(conn, fid, media_type) if fid else {}
            feeder_focus_version = int(rulebook_slice.get("focus_version") or row.get("feeder_focus_version") or 0)
            source_hash = _sha({
                "prompt_version": _POST_FOCUS_PROMPT_VERSION,
                "post_key": key,
                "fingerprint": fingerprint,
                "checkpoint_metrics": checkpoint_metrics,
                "signal_types": signal_types,
                "feeder_focus_version": feeder_focus_version,
                "rulebook_text": rulebook_slice.get("text") or "",
            })
            if (
                str(row.get("existing_source_hash") or "") == source_hash
                and str(row.get("existing_prompt_version") or "") == _POST_FOCUS_PROMPT_VERSION
                and str(row.get("existing_model_version") or "") == model_version
                and int(row.get("existing_feeder_focus_version") or 0) == feeder_focus_version
            ):
                skipped += 1
                continue

            payload = {
                "post": {
                    "post_key": key,
                    "post_url": row.get("post_url"),
                    "caption_excerpt": " ".join(str(row.get("caption") or "").split())[:800],
                    "media_type": media_type,
                    "posted_at": row.get("posted_at"),
                    "duration_seconds": row.get("duration_seconds"),
                    "duration_bucket": row.get("duration_bucket"),
                    "carousel_slide_count": row.get("carousel_slide_count"),
                    "carousel_depth_bucket": row.get("depth_bucket"),
                    "feeder_handle": row.get("handle"),
                },
                "metrics": {
                    "best_percentile": row.get("best_percentile"),
                    "checkpoint_metrics": checkpoint_metrics,
                    "source_signal_types": row.get("source_signal_types") if isinstance(row.get("source_signal_types"), list) else [],
                    "signal_types": signal_types,
                },
                "fingerprint": _compress_fingerprint(fingerprint),
                "feeder_rulebook_slice": rulebook_slice,
                "output_surface": "Fire card Post Read panel and intelligence dialog",
            }
            result = _call_json_model(
                _POST_FOCUS_SYSTEM,
                payload,
                model=FOCUS_COMPILER_MODEL,
                max_tokens=3600,
            )
            if not result:
                failed += 1
                continue
            focus_read = _normalize_post_focus_read(result)
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
                      generated_at = now(),
                      updated_at = now()
                    """,
                    (
                        key,
                        fid,
                        feed_id,
                        media_type,
                        json.dumps(focus_read),
                        feeder_focus_version,
                        source_hash,
                        _POST_FOCUS_PROMPT_VERSION,
                        model_version,
                    ),
                )
            conn.commit()
            resolved += 1
            resolved_keys.append(key)
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[post-focus] failed post_key={key}: {exc}")
            failed += 1
    return {"selected": selected, "resolved": resolved, "skipped": skipped, "failed": failed, "post_keys": resolved_keys}


def _media_capsule_key(media_type: Any) -> str:
    value = str(media_type or "").lower()
    if value in {"sidecar", "carousel"}:
        return "carousel"
    if value in {"reel", "video"}:
        return "reel"
    return "image"


def _fetch_feeder_rulebook(conn: Any, feeder_id: int) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select feeder_id, feed_id, focus_version, structured_patterns,
                   focus_md_common, focus_md_reel, focus_md_image, focus_md_carousel,
                   derived_views, movement_ledger, account_memory, focus_state
            from public.feeder_focus
            where feeder_id = %s
            limit 1
            """,
            (feeder_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else {}


def _fetch_feed_rulebook(conn: Any, feed_id: int) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select feed_id, focus_version, structured_patterns, focus_md,
                   capsule_common, capsule_reel, capsule_image, capsule_carousel,
                   capsule_anchor, capsule_cross, derived_views, movement_ledger, focus_state
            from public.feed_focus
            where feed_id = %s
            limit 1
            """,
            (feed_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else {}


def signal_rulebook_context(conn: Any, signal: dict[str, Any], posts: list[dict[str, Any]]) -> dict[str, Any]:
    feed_id = int(signal.get("feed_id") or 0)
    media_key = _media_capsule_key(signal.get("media_type"))
    feed_focus = _fetch_feed_rulebook(conn, feed_id) if feed_id else {}
    feed_rulebook = feed_focus.get("structured_patterns") if isinstance(feed_focus.get("structured_patterns"), dict) else {}
    context: dict[str, Any] = {
        "architecture": "focus_rulebook_v4",
        "scope": signal.get("scope"),
        "signal_type": signal.get("signal_type"),
        "media_type": signal.get("media_type"),
        "feed": {
            "focus_version": feed_focus.get("focus_version") or 0,
            "feed_read": feed_rulebook.get("feed_read") if isinstance(feed_rulebook, dict) else "",
            "focus_state": feed_focus.get("focus_state") if isinstance(feed_focus.get("focus_state"), dict) else {},
            "movement_memory": feed_focus.get("movement_ledger") if isinstance(feed_focus.get("movement_ledger"), dict) else {},
            "capsules": {
                "common": feed_focus.get("capsule_common") or "",
                "media": feed_focus.get(f"capsule_{media_key}") or "",
                "anchor": feed_focus.get("capsule_anchor") or "",
                "cross": feed_focus.get("capsule_cross") or "",
            },
        },
        "feed_rulebook_available": bool(feed_focus),
        "feeder_rulebooks": [],
    }
    feeder_ids: list[int] = []
    for row in posts:
        try:
            fid = int(row.get("feeder_id") or 0)
        except Exception:
            fid = 0
        if fid and fid not in feeder_ids:
            feeder_ids.append(fid)
    for fid in feeder_ids[:8]:
        focus = _fetch_feeder_rulebook(conn, fid)
        rulebook = focus.get("structured_patterns") if isinstance(focus.get("structured_patterns"), dict) else {}
        context["feeder_rulebooks"].append({
            "feeder_id": fid,
            "focus_version": focus.get("focus_version") or 0,
            "account_read": rulebook.get("account_read") if isinstance(rulebook, dict) else "",
            "focus_state": focus.get("focus_state") if isinstance(focus.get("focus_state"), dict) else {},
            "account_memory": focus.get("account_memory") if isinstance(focus.get("account_memory"), dict) else {},
            "movement_memory": focus.get("movement_ledger") if isinstance(focus.get("movement_ledger"), dict) else {},
            "common": focus.get("focus_md_common") or "",
            "media": focus.get({
                "reel": "focus_md_reel",
                "carousel": "focus_md_carousel",
                "image": "focus_md_image",
            }.get(media_key, "focus_md_image")) or "",
            "available": bool(focus),
        })
    return context


def feeder_rulebook_slice(conn: Any, feeder_id: int, media_type: Any) -> dict[str, Any]:
    focus = _fetch_feeder_rulebook(conn, feeder_id)
    media_key = _media_capsule_key(media_type)
    return {
        "focus_version": focus.get("focus_version") or 0,
        "rulebook": focus.get("structured_patterns") if isinstance(focus.get("structured_patterns"), dict) else {},
        "focus_state": focus.get("focus_state") if isinstance(focus.get("focus_state"), dict) else {},
        "account_memory": focus.get("account_memory") if isinstance(focus.get("account_memory"), dict) else {},
        "movement_memory": focus.get("movement_ledger") if isinstance(focus.get("movement_ledger"), dict) else {},
        "text": "\n".join([
            str(focus.get("focus_md_common") or ""),
            str(focus.get({
                "reel": "focus_md_reel",
                "carousel": "focus_md_carousel",
                "image": "focus_md_image",
            }.get(media_key, "focus_md_image")) or ""),
        ]).strip(),
    }
