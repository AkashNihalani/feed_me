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
    build_feed_evidence_packet,
    build_feeder_evidence_packet,
    packet_post_keys_needing_fingerprints,
)
from .ticker_facts import ticker_facts_from_feed_rulebook, ticker_facts_from_feeder_rulebook

_OPENROUTER_CHAT_URL = "/chat/completions"
_GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
_DEFAULT_PRO_MODEL = "google/gemini-3.1-pro-preview"
_FEEDER_RULEBOOK_PROMPT_VERSION = "feeder_rulebook_v4"
_FEED_RULEBOOK_PROMPT_VERSION = "feed_rulebook_v4"
_RULEBOOK_HARD_CAP = 100

_FEEDER_RULEBOOK_SYSTEM = """You compile Feed_Me feeder rulebooks.

You are not detecting alerts. The server already did that. You are reading a bounded evidence packet:
- Top and bottom posts per media format
- anomaly slices for comments, likes, views, followers
- signal_residual posts that fired an alert but fell outside the main slices
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
    missing = packet_post_keys_needing_fingerprints(packet)
    if not missing:
        return 0
    from .signal_intelligence import ensure_post_fingerprint

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


def _normalize_list(value: Any, fallback: str = "Insufficient evidence.") -> list[str]:
    if isinstance(value, list):
        clean = [str(item).strip() for item in value if str(item or "").strip()]
        return clean[:8]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return [fallback]


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


def compile_feeder_focus(conn: Any, feeder_id: int | None = None, *, limit: int = 20, full_rebuild: bool = False) -> dict[str, int]:
    if not SIGNAL_INTELLIGENCE_ENABLED:
        return {"selected": 0, "compiled": 0, "skipped": 0, "failed": 0}
    feeder_ids = _select_feeder_ids(conn, feeder_id, limit)
    selected = len(feeder_ids)
    compiled = skipped = failed = empty_evidence = 0
    for fid in feeder_ids:
        if not _claim_compile_lock(conn, "feeder", fid, force=full_rebuild):
            skipped += 1
            continue
        try:
            packet = build_feeder_evidence_packet(conn, fid, hard_cap=_RULEBOOK_HARD_CAP)
            _ensure_packet_fingerprints(conn, packet)
            packet = build_feeder_evidence_packet(conn, fid, hard_cap=_RULEBOOK_HARD_CAP)
            if not packet.get("posts"):
                skipped += 1
                empty_evidence += 1
                _mark_compile_lock(conn, "feeder", fid, success=False, error="empty_evidence_packet")
                continue

            current = _current_feeder_focus(conn, fid)
            rebuild_now = bool(full_rebuild or _full_rebuild_due(current))
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
                "mode": "full_rebuild" if rebuild_now else "weekly_update",
            })
            if not rebuild_now and current.get("source_hash") == source_hash:
                skipped += 1
                _mark_compile_lock(conn, "feeder", fid, success=True)
                continue

            result = _call_json_model(
                _FEEDER_RULEBOOK_SYSTEM,
                {
                    "mode": "full_rebuild" if rebuild_now else "weekly_update",
                    "previous_rulebook": previous_rulebook,
                    "evidence_packet": prompt_packet,
                    "signal_card_footnotes_since_last_compile": footnotes,
                    "hard_rules": {
                        "saves_and_shares": "out_of_scope",
                        "no_pattern_tags": True,
                        "hard_cap_posts": _RULEBOOK_HARD_CAP,
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
                "compile_kind": "full_rebuild" if rebuild_now else "weekly_update",
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
                   derived_views
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
                   capsule_anchor, capsule_cross, derived_views
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
        "text": "\n".join([
            str(focus.get("focus_md_common") or ""),
            str(focus.get({
                "reel": "focus_md_reel",
                "carousel": "focus_md_carousel",
                "image": "focus_md_image",
            }.get(media_key, "focus_md_image")) or ""),
        ]).strip(),
    }
