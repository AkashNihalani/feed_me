from __future__ import annotations

import json
import re
from hashlib import sha256
from typing import Any

import requests
import yaml
from psycopg.rows import dict_row

from .config import (
    FEEDER_FILE_COMPILER_MODEL,
    FEEDER_FILE_MEMORY_DAYS,
    FEEDER_FILE_MEMORY_LIMIT,
    FEEDER_FILE_RETENTION_PERCENTILE_MAX,
    FEEDER_FILE_SLOT_BATCH_PERCENT,
    FEEDER_FILE_PATTERN_MODEL,
    FEEDER_FILE_PROOF_MODEL,
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
)
from .feeder_prompts import (
    FEEDER_FILE_COMPILATION_PROMPT_V2,
    FEEDER_FILE_PATTERN_FRONTEND_PROMPT_V2,
    FEEDER_FILE_PATTERN_PROMPT_VERSION,
    FEEDER_FILE_PROOF_FRONTEND_PROMPT_V3,
    FEEDER_FILE_PROOF_PROMPT_VERSION,
    FEEDER_FILE_PROMPT_VERSION,
)
from .fingerprint_intelligence import ensure_post_breakdown

_OPENROUTER_CHAT_URL = "/chat/completions"


def _normalize_handle(handle: str | None) -> str:
    return str(handle or "").strip().lower().replace("@", "")


def _prompt_handle(handle: str) -> str:
    clean = _normalize_handle(handle)
    return f"@{clean}" if clean else ""


def _strip_fences(value: str) -> str:
    text = str(value or "").strip()
    if text.startswith("```"):
        lines = text.splitlines()[1:]
        while lines and lines[-1].strip() == "```":
            lines.pop()
        text = "\n".join(lines).strip()
    return text


_YAML_ROOT_KEYS = ("feed_file:", "pattern_breakdown:", "post_proof:")


def _fenced_blocks(value: str) -> list[str]:
    blocks = re.findall(r"```(?:yaml|yml|json)?\s*\n(.*?)```", str(value or ""), flags=re.IGNORECASE | re.DOTALL)
    return [block.strip() for block in blocks if block.strip()]


def _mapping_sources(raw_output: str) -> list[str]:
    sources: list[str] = []
    seen: set[str] = set()

    def add(candidate: str):
        cleaned = _strip_fences(candidate)
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            sources.append(cleaned)

    add(raw_output)
    for block in _fenced_blocks(raw_output):
        add(block)

    for source in list(sources):
        for root_key in _YAML_ROOT_KEYS:
            index = source.find(root_key)
            if index >= 0:
                add(source[index:])
        json_index = source.find("{")
        if json_index >= 0:
            add(source[json_index:])

    return sources


def _repair_yaml_lines(text: str) -> str:
    lines = text.splitlines()
    repaired: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        stripped = line.strip()
        if stripped and re.match(r"^(→|->|=>)\s+", stripped):
            indent = line[: len(line) - len(line.lstrip())]
            safe = stripped.replace("\\", "\\\\").replace('"', '\\"')
            repaired.append(f'{indent}- "{safe}"')
            index += 1
            continue
        if stripped.startswith("- ") and not stripped[2:].startswith(("{", "[")):
            indent = line[: len(line) - len(line.lstrip())]
            value = stripped[2:].strip()
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*:\s", value):
                if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                    value = value[1:-1]
                continuation: list[str] = []
                next_index = index + 1
                while next_index < len(lines):
                    next_line = lines[next_index]
                    next_stripped = next_line.strip()
                    if not next_stripped:
                        continuation.append("")
                        next_index += 1
                        continue
                    next_indent_len = len(next_line) - len(next_line.lstrip())
                    if next_indent_len <= len(indent):
                        break
                    continuation.append(next_stripped)
                    next_index += 1
                repaired.append(f"{indent}- >")
                repaired.append(f"{indent}  {value}")
                for continuation_line in continuation:
                    repaired.append(f"{indent}  {continuation_line}" if continuation_line else "")
                index = next_index
                continue
        repaired.append(line)
        index += 1
    return "\n".join(repaired)


def _safe_yaml_mapping(text: str) -> dict[str, Any] | None:
    try:
        parsed = yaml.safe_load(text)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _parse_yaml_mapping(raw_output: str) -> dict[str, Any]:
    for source in _mapping_sources(raw_output):
        for candidate in (source, _repair_yaml_lines(source)):
            parsed = _safe_yaml_mapping(candidate)
            if parsed is not None:
                return parsed

    raise RuntimeError("model output did not parse to a YAML mapping")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _json_hash(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return sha256(payload.encode("utf-8")).hexdigest()


def _fmt_num(value: Any) -> str:
    try:
        number = float(value)
    except Exception:
        return ""
    if number >= 1_000_000:
        return f"{number / 1_000_000:.1f}M"
    if number >= 1_000:
        return f"{number / 1_000:.1f}K"
    return str(int(number))


def _top_pct(value: Any) -> str:
    try:
        return f"Top {float(value):.1f}%"
    except Exception:
        return ""


def feeder_file_slot_batch_size(current_reel_count: int) -> int:
    base = max(1, int(FEEDER_FILE_SLOT_BATCH_PERCENT))
    return max(1, int(current_reel_count or 0) // base)


def _metric_payload(row: dict[str, Any]) -> dict[str, Any]:
    metrics = {
        "checkpoint": "d7",
        "views": row.get("views"),
        "likes": row.get("likes"),
        "comments": row.get("comments"),
        "views_percentile": row.get("views_percentile"),
        "likes_percentile": row.get("likes_percentile"),
        "comments_percentile": row.get("comments_percentile"),
        "percentile_performance": row.get("percentile_performance"),
        "percentile_performance_exact": row.get("percentile_performance_exact"),
        "views_baseline": row.get("views_baseline"),
        "likes_baseline": row.get("likes_baseline"),
        "comments_baseline": row.get("comments_baseline"),
        "views_multiple": row.get("views_multiple"),
        "likes_multiple": row.get("likes_multiple"),
        "comments_multiple": row.get("comments_multiple"),
    }
    display = []
    rank = _top_pct(row.get("percentile_performance_exact") or row.get("percentile_performance"))
    if rank:
        display.append({"label": "Rank", "value": rank, "detail": "recent history", "accent": True})
    views = _fmt_num(row.get("views"))
    if views:
        display.append({"label": "Views", "value": views, "detail": "selected post"})
    comments = _fmt_num(row.get("comments"))
    if comments:
        display.append({"label": "Comments", "value": comments, "detail": "conversation signal"})
    metrics["display"] = display
    return metrics


def _metric_cards(row: dict[str, Any]) -> list[dict[str, Any]]:
    return list(_metric_payload(row).get("display") or [])


def _resolve_feeder(conn: Any, *, feeder_id: int | None, handle: str | None) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        if feeder_id is not None:
            cur.execute(
                """
                select id, handle
                from public.feeders
                where id = %s
                limit 1
                """,
                (feeder_id,),
            )
        else:
            clean = _normalize_handle(handle)
            cur.execute(
                """
                select id, handle
                from public.feeders
                where lower(coalesce(handle, '')) = %s
                order by id desc
                limit 1
                """,
                (clean,),
            )
        row = cur.fetchone()
    if not row:
        label = feeder_id if feeder_id is not None else handle
        raise RuntimeError(f"feeder not found: {label}")
    return {"id": int(row["id"]), "handle": _normalize_handle(row["handle"])}


def _post_input_select() -> str:
    return """
      p.post_key,
      p.caption,
      p.posted_at,
      lower(coalesce(p.media_type, '')) as media_type,
      pb.breakdown,
      pb.breakdown_version,
      pb.source_fingerprint_model_version,
      pb.source_fingerprint_hash,
      pf.fingerprint,
      pf.model_version as fingerprint_model_version,
      pm.views,
      pm.likes,
      pm.comments,
      pm.views_percentile,
      pm.likes_percentile,
      pm.comments_percentile,
      pm.percentile_performance,
      pm.percentile_performance_exact,
      pm.views_baseline,
      pm.likes_baseline,
      pm.comments_baseline,
      pm.views_multiple,
      pm.likes_multiple,
      pm.comments_multiple
    """


def _load_post_inputs(conn: Any, *, feeder_id: int, days: int, limit: int) -> list[dict[str, Any]]:
    scoped_limit = min(max(1, int(limit)), max(1, int(FEEDER_FILE_MEMORY_LIMIT)))
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            select {_post_input_select()}
            from public.post_breakdowns pb
            join public.posts p
              on p.post_key = pb.post_key
            left join public.post_fingerprints pf
              on pf.post_key = pb.post_key
            left join public.post_metrics pm
              on pm.post_key = pb.post_key
             and pm.checkpoint = 'd7'
            where p.feeder_id = %s
              and lower(coalesce(p.media_type, '')) in ('reel', 'video')
              and p.posted_at >= now() - (%s::int * interval '1 day')
              and least(
                coalesce(pm.percentile_performance_exact, 101),
                coalesce(pm.percentile_performance, 101)
              ) <= %s
            order by
              least(
                coalesce(pm.percentile_performance_exact, 101),
                coalesce(pm.percentile_performance, 101)
              ) asc,
              p.posted_at desc nulls last
            limit %s
            """,
            (feeder_id, max(1, int(days)), float(FEEDER_FILE_RETENTION_PERCENTILE_MAX), scoped_limit),
        )
        return [dict(row) for row in cur.fetchall()]


def _load_post_inputs_by_keys(conn: Any, post_keys: list[str]) -> dict[str, dict[str, Any]]:
    keys = [str(key).strip() for key in post_keys if str(key or "").strip()]
    if not keys:
        return {}
    expanded_keys: list[str] = []
    for key in keys:
        if key not in expanded_keys:
            expanded_keys.append(key)
        if not key.startswith("p/"):
            prefixed = f"p/{key}"
            if prefixed not in expanded_keys:
                expanded_keys.append(prefixed)
        else:
            unprefixed = key[2:]
            if unprefixed not in expanded_keys:
                expanded_keys.append(unprefixed)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            select {_post_input_select()}
            from public.post_breakdowns pb
            join public.posts p
              on p.post_key = pb.post_key
            left join public.post_fingerprints pf
              on pf.post_key = pb.post_key
            left join public.post_metrics pm
              on pm.post_key = pb.post_key
             and pm.checkpoint = 'd7'
            where pb.post_key = any(%s::text[])
            """,
            (expanded_keys,),
        )
        rows = [dict(row) for row in cur.fetchall()]
    try:
        conn.commit()
    except Exception:
        pass
    by_key: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row["post_key"])
        by_key[key] = row
        if key.startswith("p/"):
            by_key.setdefault(key[2:], row)
        else:
            by_key.setdefault(f"p/{key}", row)
    return by_key


def _load_recent_high_fingerprints(conn: Any, *, feeder_id: int, limit: int, days: int) -> list[dict[str, Any]]:
    scoped_limit = min(max(1, int(limit)), max(1, int(FEEDER_FILE_MEMORY_LIMIT)))
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select
              p.post_key,
              p.posted_at,
              pf.fingerprint,
              pb.post_key is not null as has_breakdown
            from public.posts p
            join public.post_fingerprints pf
              on pf.post_key = p.post_key
             and pf.media_confidence = 'high'
            join public.post_metrics pm
              on pm.post_key = p.post_key
             and lower(pm.checkpoint) = 'd7'
            left join public.post_breakdowns pb
              on pb.post_key = p.post_key
            where p.feeder_id = %s
              and lower(coalesce(p.media_type, '')) in ('reel', 'video')
              and p.posted_at >= now() - (%s::int * interval '1 day')
              and least(
                coalesce(pm.percentile_performance_exact, 101),
                coalesce(pm.percentile_performance, 101)
              ) <= %s
            order by p.posted_at desc nulls last, pf.updated_at desc nulls last
            limit %s
            """,
            (
                feeder_id,
                max(1, int(days)),
                float(FEEDER_FILE_RETENTION_PERCENTILE_MAX),
                scoped_limit,
            ),
        )
        return [dict(row) for row in cur.fetchall()]


def _source_breakdown_version(rows: list[dict[str, Any]]) -> str:
    versions = sorted({str(row.get("breakdown_version") or "").strip() for row in rows if row.get("breakdown_version")})
    if not versions:
        return ""
    if len(versions) == 1:
        return versions[0]
    return ",".join(versions)


def _breakdown_body(row: dict[str, Any]) -> dict[str, Any]:
    breakdown = row.get("breakdown") or {}
    if isinstance(breakdown, dict) and isinstance(breakdown.get("post_breakdown"), dict):
        return breakdown["post_breakdown"]
    return breakdown if isinstance(breakdown, dict) else {}


def _compile_post_payloads(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        payloads.append(
            {
                "input_index": index,
                "post_key": str(row.get("post_key") or ""),
                "post_breakdown": _breakdown_body(row),
            }
        )
    return payloads


def _row_lookup(rows: list[dict[str, Any]]) -> tuple[dict[int, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_index: dict[int, dict[str, Any]] = {}
    by_key: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(rows):
        by_index[index] = row
        key = str(row.get("post_key") or "").strip()
        if not key:
            continue
        by_key[key] = row
        if key.startswith("p/"):
            by_key.setdefault(key[2:], row)
        else:
            by_key.setdefault(f"p/{key}", row)
    return by_index, by_key


def _member_fit_type(member: dict[str, Any]) -> str:
    fit_type = str(member.get("fit_type") or "core").strip().lower()
    if fit_type == "secondary":
        return "support"
    return fit_type or "core"


def _member_fit_rank(member: dict[str, Any]) -> int:
    fit_type = _member_fit_type(member)
    if fit_type == "core":
        return 0
    if fit_type in {"support", "secondary"}:
        return 1
    return 2


def _dedupe_pattern_members(patterns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    claims: dict[str, list[tuple[tuple[int, int, int], int, int, str, str]]] = {}
    pattern_ids: dict[int, str] = {}
    for pattern_index, pattern in enumerate(patterns):
        if not isinstance(pattern, dict):
            continue
        pattern_id = str(pattern.get("pattern_id") or f"pattern_{pattern_index + 1}")
        pattern_ids[pattern_index] = pattern_id
        members = pattern.get("members") or []
        if not isinstance(members, list):
            continue
        core_count, support_count = _pattern_counts(pattern)
        pattern_rank = 0 if _pattern_is_valid(core_count, support_count) else 1
        for member_index, member in enumerate(members):
            if not isinstance(member, dict):
                continue
            post_key = str(member.get("post_key") or "").strip()
            if not post_key:
                continue
            key = _frontend_post_key(post_key)
            rank = (_member_fit_rank(member), pattern_rank, pattern_index)
            claims.setdefault(key, []).append((rank, pattern_index, member_index, pattern_id, post_key))

    owners: dict[str, tuple[int, int, str]] = {}
    for key, key_claims in claims.items():
        key_claims.sort(key=lambda claim: claim[0])
        _, pattern_index, member_index, pattern_id, _ = key_claims[0]
        owners[key] = (pattern_index, member_index, pattern_id)

    dropped: list[dict[str, Any]] = []
    for pattern_index, pattern in enumerate(patterns):
        if not isinstance(pattern, dict):
            continue
        members = pattern.get("members") or []
        if not isinstance(members, list):
            members = []
        pattern_id = pattern_ids.get(pattern_index, str(pattern.get("pattern_id") or f"pattern_{pattern_index + 1}"))
        unique_members: list[dict[str, Any]] = []
        for member_index, member in enumerate(members):
            if not isinstance(member, dict):
                continue
            post_key = str(member.get("post_key") or "").strip()
            key = _frontend_post_key(post_key)
            owner = owners.get(key)
            if owner is None or owner[:2] == (pattern_index, member_index):
                unique_members.append(member)
                continue
            dropped.append(
                {
                    "post_key": post_key,
                    "dropped_from_pattern": pattern_id,
                    "kept_in_pattern": owner[2],
                    "fit_type": _member_fit_type(member),
                }
            )
        pattern["members"] = unique_members
        core_count, support_count = _pattern_counts(pattern)
        pattern["core_post_count"] = core_count
        pattern["secondary_post_count"] = support_count
        pattern["status"] = "active" if _pattern_is_valid(core_count, support_count) else "candidate"
    return dropped


def _canonicalize_patterns(feed_file: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    index_rows, key_rows = _row_lookup(rows)
    patterns = feed_file.get("patterns") or []
    if not isinstance(patterns, list):
        return feed_file
    for pattern in patterns:
        if not isinstance(pattern, dict):
            continue
        members = pattern.get("members") or []
        if not isinstance(members, list):
            continue
        canonical_members: list[dict[str, Any]] = []
        for member in members:
            if not isinstance(member, dict):
                continue
            row = None
            raw_index = member.get("member_index", member.get("input_index"))
            try:
                if raw_index is not None:
                    row = index_rows.get(int(raw_index))
            except Exception:
                row = None
            if row is None:
                post_key = str(member.get("post_key") or "").strip()
                row = key_rows.get(post_key)
            if row is None:
                continue
            fixed_member = {**member}
            fixed_member["member_index"] = next((idx for idx, candidate in index_rows.items() if candidate is row), None)
            fixed_member["post_key"] = str(row.get("post_key") or "")
            canonical_members.append(fixed_member)
        pattern["members"] = canonical_members
        core_count, support_count = _pattern_counts(pattern)
        pattern["core_post_count"] = core_count
        pattern["secondary_post_count"] = support_count
        pattern["status"] = "active" if _pattern_is_valid(core_count, support_count) else "candidate"
    dropped_members = _dedupe_pattern_members(patterns)
    if dropped_members:
        validation = feed_file.get("server_validation")
        if not isinstance(validation, dict):
            validation = {}
        validation["duplicate_member_drops"] = dropped_members
        feed_file["server_validation"] = validation
    return feed_file


def _canonicalize_patterns_from_loaded_rows(
    patterns: list[dict[str, Any]],
    post_rows: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    for pattern in patterns:
        if not isinstance(pattern, dict):
            continue
        members = pattern.get("members") or []
        if not isinstance(members, list):
            continue
        canonical_members: list[dict[str, Any]] = []
        for member in members:
            if not isinstance(member, dict):
                continue
            post_key = str(member.get("post_key") or "").strip()
            row = post_rows.get(post_key)
            if row is None:
                continue
            canonical_members.append({**member, "post_key": str(row.get("post_key") or post_key)})
        pattern["members"] = canonical_members
        core_count, support_count = _pattern_counts(pattern)
        pattern["core_post_count"] = core_count
        pattern["secondary_post_count"] = support_count
        pattern["status"] = "active" if _pattern_is_valid(core_count, support_count) else "candidate"
    _dedupe_pattern_members(patterns)
    return patterns


def _upsert_feeder_file(
    conn: Any,
    *,
    feeder_id: int,
    feeder_handle: str,
    feed_file: dict[str, Any],
    source_breakdown_version: str,
    active_window: str,
    source: str = "pipeline",
) -> int:
    compile_version = str(feed_file.get("compile_version") or FEEDER_FILE_PROMPT_VERSION)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            insert into public.feeder_files (
              feeder_id, feeder_handle, feed_file, compile_version,
              source_breakdown_version, active_window, status, source,
              generated_at, updated_at
            )
            values (%s, %s, %s::jsonb, %s, %s, %s, 'active', %s, now(), now())
            on conflict (feeder_handle, compile_version) do update set
              feeder_id = excluded.feeder_id,
              feed_file = excluded.feed_file,
              source_breakdown_version = excluded.source_breakdown_version,
              active_window = excluded.active_window,
              status = 'active',
              source = excluded.source,
              updated_at = now()
            returning id
            """,
            (
                feeder_id,
                feeder_handle,
                _json(feed_file),
                compile_version,
                source_breakdown_version,
                active_window,
                source,
            ),
        )
        feeder_file_id = int(cur.fetchone()["id"])
    conn.commit()
    return feeder_file_id


def _compile_feeder_file_from_rows(
    conn: Any,
    *,
    feeder_id: int,
    feeder_handle: str,
    rows: list[dict[str, Any]],
    active_window: str,
    pattern_limit: int,
) -> dict[str, Any]:
    if len(rows) < 3:
        raise RuntimeError(f"need at least 3 stored post_breakdowns, found {len(rows)}")
    source_version = _source_breakdown_version(rows)
    feeder_file_id = _upsert_feeder_file(
        conn,
        feeder_id=feeder_id,
        feeder_handle=feeder_handle,
        feed_file={"compile_version": FEEDER_FILE_PROMPT_VERSION, "patterns": []},
        source_breakdown_version=source_version,
        active_window=active_window,
    )
    compile_payload = {
        "feeder_id": feeder_handle,
        "handle": feeder_handle,
        "source_breakdown_version": source_version,
        "active_window": active_window,
        "post_breakdowns": _compile_post_payloads(rows),
    }
    compile_call_id, compile_parsed = _call_openrouter(
        conn,
        feeder_file_id=feeder_file_id,
        call_key="feeder_file_compile",
        call_type="feeder_file_compile",
        feeder_handle=feeder_handle,
        pattern_id=None,
        post_key=None,
        model=FEEDER_FILE_COMPILER_MODEL,
        prompt_version=FEEDER_FILE_PROMPT_VERSION,
        system_prompt=FEEDER_FILE_COMPILATION_PROMPT_V2,
        user_payload=compile_payload,
        max_tokens=7000,
    )
    del compile_call_id
    feed_file = compile_parsed.get("feed_file")
    if not isinstance(feed_file, dict):
        raise RuntimeError("feeder file compiler did not return feed_file")
    feed_file = _canonicalize_patterns(feed_file, rows)
    feeder_file_id = _upsert_feeder_file(
        conn,
        feeder_id=feeder_id,
        feeder_handle=feeder_handle,
        feed_file=feed_file,
        source_breakdown_version=source_version,
        active_window=active_window,
    )
    selected_patterns = _patterns_by_id(feed_file, None, pattern_limit)
    result = _package_patterns(
        conn,
        feeder_file_id=feeder_file_id,
        feeder_handle=feeder_handle,
        selected_patterns=selected_patterns,
        frontend_pattern_limit=pattern_limit,
    )
    return {
        **result,
        "post_count": len(rows),
        "active_window": active_window,
    }


def _latest_feeder_file(
    conn: Any,
    *,
    feeder_id: int | None,
    feeder_handle: str,
    compile_version: str | None = None,
) -> dict[str, Any]:
    params: list[Any] = [feeder_handle]
    where = ["lower(coalesce(feeder_handle, '')) = %s", "status = 'active'"]
    if feeder_id is not None:
        where.append("(feeder_id = %s or feeder_id is null)")
        params.append(feeder_id)
    if compile_version:
        where.append("compile_version = %s")
        params.append(compile_version)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            select id, feeder_id, feeder_handle, feed_file, compile_version,
                   source_breakdown_version, active_window
            from public.feeder_files
            where {' and '.join(where)}
            order by updated_at desc nulls last, generated_at desc nulls last, id desc
            limit 1
            """,
            params,
        )
        row = cur.fetchone()
    if not row:
        raise RuntimeError(f"feeder file not found for @{feeder_handle}")
    return dict(row)


def _record_call_start(
    conn: Any,
    *,
    feeder_file_id: int | None,
    call_key: str,
    call_type: str,
    feeder_handle: str,
    pattern_id: str | None,
    post_key: str | None,
    model: str,
    prompt_version: str,
    system_prompt: str,
    user_payload: dict[str, Any],
) -> int:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            insert into public.feeder_file_model_calls (
              feeder_file_id, call_key, call_type, feeder_handle, pattern_id, post_key,
              model, prompt_version, system_prompt, user_payload,
              status, started_at, updated_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, 'running', now(), now())
            on conflict (feeder_file_id, call_key) do update set
              call_type = excluded.call_type,
              feeder_handle = excluded.feeder_handle,
              pattern_id = excluded.pattern_id,
              post_key = excluded.post_key,
              model = excluded.model,
              prompt_version = excluded.prompt_version,
              system_prompt = excluded.system_prompt,
              user_payload = excluded.user_payload,
              raw_output = null,
              parsed_output = null,
              status = 'running',
              error = null,
              started_at = now(),
              completed_at = null,
              updated_at = now()
            returning id
            """,
            (
                feeder_file_id,
                call_key,
                call_type,
                feeder_handle,
                pattern_id,
                post_key,
                model,
                prompt_version,
                system_prompt,
                _json(user_payload),
            ),
        )
        call_id = int(cur.fetchone()["id"])
    conn.commit()
    return call_id


def _record_call_done(
    conn: Any,
    call_id: int,
    *,
    raw_output: str | None = None,
    parsed_output: dict[str, Any] | None = None,
    error: str | None = None,
    status_override: str | None = None,
) -> None:
    status = status_override or ("failed" if error else "complete")
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.feeder_file_model_calls
            set raw_output = %s,
                parsed_output = %s::jsonb,
                status = %s,
                error = %s,
                completed_at = now(),
                updated_at = now()
            where id = %s
            """,
            (
                raw_output,
                _json(parsed_output) if parsed_output is not None else None,
                status,
                error,
                call_id,
            ),
        )
    conn.commit()


_PROOF_CONTEST_PHRASES = (
    "cannot produce a valid proof",
    "can't produce a valid proof",
    "cannot create a valid proof",
    "cannot write a valid proof",
    "does not execute this pattern",
    "doesn't execute this pattern",
    "does not fit this pattern",
    "doesn't fit this pattern",
    "does not match this pattern",
    "doesn't match this pattern",
    "would be fabricated",
    "would require fabrication",
    "fabricated or incoherent",
    "not a true member",
    "not a valid proof",
)


def _proof_contest_reason(
    raw_output: str | None,
    parsed_output: dict[str, Any] | None = None,
) -> str | None:
    if parsed_output and isinstance(parsed_output.get("post_proof"), dict):
        return None
    normalized = re.sub(r"\s+", " ", str(raw_output or "").strip().lower())
    if not normalized:
        return None
    for phrase in _PROOF_CONTEST_PHRASES:
        if phrase in normalized:
            return f"contested proof: proof model said the post does not match the pattern ({phrase})"
    return None


def _recover_stored_model_call(
    conn: Any,
    *,
    feeder_file_id: int | None,
    call_key: str,
    call_type: str,
    feeder_handle: str,
    prompt_version: str,
) -> tuple[int, dict[str, Any]] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select id, raw_output
            from public.feeder_file_model_calls
            where call_key = %s
              and call_type = %s
              and lower(feeder_handle) = lower(%s)
              and prompt_version = %s
              and (%s::bigint is null or feeder_file_id = %s::bigint)
              and coalesce(raw_output, '') <> ''
            order by updated_at desc nulls last, id desc
            limit 3
            """,
            (call_key, call_type, feeder_handle, prompt_version, feeder_file_id, feeder_file_id),
        )
        rows = cur.fetchall()

    for row in rows:
        raw_output = str(row.get("raw_output") or "")
        try:
            parsed = _parse_yaml_mapping(raw_output)
        except Exception:
            continue
        call_id = int(row["id"])
        _record_call_done(conn, call_id, raw_output=raw_output, parsed_output=parsed)
        print(
            "[feeder-file-model] "
            f"recovered call_type={call_type} handle={feeder_handle} call_key={call_key}",
            flush=True,
        )
        return call_id, parsed
    return None


def _call_openrouter(
    conn: Any,
    *,
    feeder_file_id: int | None,
    call_key: str,
    call_type: str,
    feeder_handle: str,
    pattern_id: str | None,
    post_key: str | None,
    model: str,
    prompt_version: str,
    system_prompt: str,
    user_payload: dict[str, Any],
    max_tokens: int,
) -> tuple[int, dict[str, Any]]:
    if not OPENROUTER_API_KEY:
        raise RuntimeError("OPENROUTER_API_KEY is required for feeder file model calls")
    if call_type == "proof" and prompt_version != "feeder_file_proof_frontend_v3":
        raise RuntimeError(f"frontend proof calls must use v3 prompt, got {prompt_version}")
    recovered = _recover_stored_model_call(
        conn,
        feeder_file_id=feeder_file_id,
        call_key=call_key,
        call_type=call_type,
        feeder_handle=feeder_handle,
        prompt_version=prompt_version,
    )
    if recovered is not None:
        return recovered
    print(
        "[feeder-file-model] "
        f"start call_type={call_type} handle={feeder_handle} pattern_id={pattern_id or ''} "
        f"post_key={post_key or ''} model={model} prompt_version={prompt_version}",
        flush=True,
    )
    call_id = _record_call_start(
        conn,
        feeder_file_id=feeder_file_id,
        call_key=call_key,
        call_type=call_type,
        feeder_handle=feeder_handle,
        pattern_id=pattern_id,
        post_key=post_key,
        model=model,
        prompt_version=prompt_version,
        system_prompt=system_prompt,
        user_payload=user_payload,
    )
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, indent=2, default=str)},
        ],
        "temperature": 0.2,
        "max_tokens": max_tokens,
    }
    raw_output: str | None = None
    parsed: dict[str, Any] | None = None
    try:
        response = requests.post(
            f"{OPENROUTER_BASE_URL.rstrip('/')}{_OPENROUTER_CHAT_URL}",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://feedme.local",
                "X-Title": "FeedMe Feeder File Pipeline",
            },
            json=body,
            timeout=180,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"OpenRouter status {response.status_code}: {response.text[:1200]}")
        data = response.json()
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        if isinstance(content, list):
            content = "\n".join(str(part.get("text") or "") for part in content if isinstance(part, dict))
        raw_output = _strip_fences(str(content))
        parsed = _parse_yaml_mapping(raw_output)
        contest_reason = _proof_contest_reason(raw_output, parsed) if call_type == "proof" else None
        _record_call_done(
            conn,
            call_id,
            raw_output=raw_output,
            parsed_output=parsed,
            error=contest_reason,
            status_override="contested" if contest_reason else None,
        )
        print(
            "[feeder-file-model] "
            f"{'contested' if contest_reason else 'complete'} call_type={call_type} "
            f"handle={feeder_handle} pattern_id={pattern_id or ''} "
            f"post_key={post_key or ''}",
            flush=True,
        )
        return call_id, parsed
    except Exception as exc:
        contest_reason = _proof_contest_reason(raw_output, parsed) if call_type == "proof" else None
        _record_call_done(
            conn,
            call_id,
            raw_output=raw_output,
            parsed_output=parsed,
            error=contest_reason or str(exc),
            status_override="contested" if contest_reason else None,
        )
        print(
            "[feeder-file-model] "
            f"{'contested' if contest_reason else 'failed'} call_type={call_type} "
            f"handle={feeder_handle} pattern_id={pattern_id or ''} "
            f"post_key={post_key or ''} error={str(exc)[:240]}",
            flush=True,
        )
        raise


def _qualifying_patterns(feed_file: dict[str, Any], pattern_limit: int) -> list[dict[str, Any]]:
    patterns = feed_file.get("patterns") or []
    if not isinstance(patterns, list):
        return []

    def qualifies(pattern: dict[str, Any]) -> bool:
        core, support = _pattern_counts(pattern)
        return _pattern_is_valid(core, support)

    selected = [pattern for pattern in patterns if isinstance(pattern, dict) and qualifies(pattern)]
    selected.sort(
        key=lambda pattern: (
            str(pattern.get("status") or "") != "active",
            -_pattern_counts(pattern)[0],
            -_pattern_counts(pattern)[1],
            str(pattern.get("pattern_id") or ""),
        )
    )
    return selected[: max(1, int(pattern_limit))]


def _pattern_counts(pattern: dict[str, Any]) -> tuple[int, int]:
    members = pattern.get("members") or []
    if isinstance(members, list):
        core = sum(
            1
            for member in members
            if isinstance(member, dict) and str(member.get("fit_type") or "core").lower() == "core"
        )
        support = sum(
            1
            for member in members
            if isinstance(member, dict) and str(member.get("fit_type") or "").lower() in {"support", "secondary"}
        )
        if core or support:
            return core, support
    return int(pattern.get("core_post_count") or 0), int(
        pattern.get("support_post_count") or pattern.get("secondary_post_count") or 0
    )


def _pattern_is_valid(core_count: int, support_count: int) -> bool:
    return core_count >= 3 or (core_count >= 2 and support_count >= 1)


def _pattern_candidate_reason(core_count: int, support_count: int) -> str:
    return (
        "candidate until the pool has at least 3 core posts or 2 core posts plus 1 support post "
        f"(currently {core_count} core, {support_count} support)"
    )


def _patterns_by_id(feed_file: dict[str, Any], pattern_id: str | None, pattern_limit: int) -> list[dict[str, Any]]:
    patterns = feed_file.get("patterns") or []
    if not isinstance(patterns, list):
        return []
    if pattern_id:
        wanted = str(pattern_id)
        return [pattern for pattern in patterns if isinstance(pattern, dict) and str(pattern.get("pattern_id")) == wanted]
    valid: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    for pattern in patterns:
        if not isinstance(pattern, dict):
            continue
        core, support = _pattern_counts(pattern)
        if _pattern_is_valid(core, support):
            valid.append(pattern)
        else:
            candidates.append(pattern)
    valid.sort(
        key=lambda pattern: (
            str(pattern.get("status") or "") != "active",
            -_pattern_counts(pattern)[0],
            -_pattern_counts(pattern)[1],
            str(pattern.get("pattern_id") or ""),
        )
    )
    candidates.sort(key=lambda pattern: str(pattern.get("pattern_id") or ""))
    return valid + candidates


def _member_post_keys(patterns: list[dict[str, Any]]) -> list[str]:
    keys: list[str] = []
    for pattern in patterns:
        members = pattern.get("members") or []
        if not isinstance(members, list):
            continue
        for member in members:
            if not isinstance(member, dict):
                continue
            fit_type = str(member.get("fit_type") or "core").lower()
            post_key = str(member.get("post_key") or "").strip()
            if fit_type in {"core", "support", "secondary"} and post_key and post_key not in keys:
                keys.append(post_key)
    return keys


def _pattern_card(
    *,
    feeder_handle: str,
    pattern: dict[str, Any],
    pattern_breakdown: dict[str, Any],
    proofs: list[dict[str, Any]],
) -> dict[str, Any]:
    pattern_id = str(pattern.get("pattern_id") or "pattern")
    core, support = _pattern_counts(pattern)
    headline = str(pattern_breakdown.get("headline") or pattern.get("tile_headline") or "Pattern read")
    hook = str(pattern_breakdown.get("the_hook") or "")
    return {
        "account": _prompt_handle(feeder_handle),
        "accountLabel": _prompt_handle(feeder_handle),
        "accountMeta": f"{core} core posts" + (f" + {support} support" if support else ""),
        "pattern_id": pattern_id,
        "patternMetrics": [
            {"label": "Proofs", "value": str(len(proofs)), "detail": "post proof reads", "accent": True},
            {"label": "Core", "value": str(core), "detail": "pattern members"},
            {"label": "Support", "value": str(support), "detail": "nearby proof"},
        ],
        "pattern": {
            "pattern_id": pattern_id,
            "tile_label": headline,
            "tile_headline": headline,
            "tile_read": hook,
            "modal_headline": headline,
            "the_hook": hook,
            "the_breakdown": pattern_breakdown.get("the_breakdown") or [],
            "why_it_works": str(pattern_breakdown.get("why_it_works") or ""),
            "what_to_keep": pattern_breakdown.get("what_to_keep") or [],
            "what_kills_it": pattern_breakdown.get("what_kills_it") or [],
        },
        "proofs": proofs,
    }


def _proof_card(
    *,
    post_key: str,
    fit_type: str,
    proof: dict[str, Any],
    metrics: list[dict[str, Any]],
    index: int,
    total: int,
) -> dict[str, Any]:
    return {
        "post_key": post_key,
        "proof_label": str(proof.get("proof_label") or f"Proof {index + 1}"),
        "proof_headline": str(proof.get("proof_headline") or ""),
        "post_read": str(proof.get("post_read") or ""),
        "what_clicked": str(proof.get("what_clicked") or ""),
        "evidence": proof.get("evidence") if isinstance(proof.get("evidence"), list) else [],
        "metrics": [
            {"label": "Proof", "value": f"{index + 1}/{total}", "detail": fit_type, "accent": True},
            *metrics,
        ],
    }


def _backend_pattern_breakdown(pattern: dict[str, Any]) -> dict[str, Any]:
    for key in ("pattern_breakdown", "pattern_profile"):
        value = pattern.get(key)
        if isinstance(value, dict):
            return value
    return {
        key: pattern.get(key)
        for key in ("headline", "works_because", "opens_with", "holds_attention_by", "viewer_mode", "lands_as")
        if pattern.get(key) is not None
    }


def _upsert_feeder_file_pattern(
    conn: Any,
    *,
    feeder_file_id: int,
    feeder_handle: str,
    pattern: dict[str, Any],
    status: str,
    reason: str | None,
    pattern_read: dict[str, Any] | None = None,
    proof_reads: list[dict[str, Any]] | None = None,
) -> None:
    pattern_id = str(pattern.get("pattern_id") or "pattern")
    core_count, support_count = _pattern_counts(pattern)
    members = pattern.get("members") if isinstance(pattern.get("members"), list) else []
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            insert into public.feeder_file_patterns (
              feeder_file_id, feeder_handle, pattern_id, status, reason,
              core_post_count, support_post_count, pattern_breakdown, members,
              pattern_read, proof_reads,
              pattern_model, pattern_prompt_version, proof_model, proof_prompt_version,
              generated_at, updated_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
                    %s, %s, %s, %s, now(), now())
            on conflict (feeder_file_id, pattern_id) do update set
              feeder_handle = excluded.feeder_handle,
              status = excluded.status,
              reason = excluded.reason,
              core_post_count = excluded.core_post_count,
              support_post_count = excluded.support_post_count,
              pattern_breakdown = excluded.pattern_breakdown,
              members = excluded.members,
              pattern_read = excluded.pattern_read,
              proof_reads = excluded.proof_reads,
              pattern_model = excluded.pattern_model,
              pattern_prompt_version = excluded.pattern_prompt_version,
              proof_model = excluded.proof_model,
              proof_prompt_version = excluded.proof_prompt_version,
              updated_at = now()
            """,
            (
                feeder_file_id,
                feeder_handle,
                pattern_id,
                status,
                reason,
                core_count,
                support_count,
                _json(_backend_pattern_breakdown(pattern)),
                _json(members),
                _json(pattern_read or {}),
                _json(proof_reads or []),
                FEEDER_FILE_PATTERN_MODEL if pattern_read else None,
                FEEDER_FILE_PATTERN_PROMPT_VERSION if pattern_read else None,
                FEEDER_FILE_PROOF_MODEL if proof_reads else None,
                FEEDER_FILE_PROOF_PROMPT_VERSION if proof_reads else None,
            ),
        )
    conn.commit()


def _frontend_post_key(value: Any) -> str:
    return str(value or "").strip().lower()


def _unique_frontend_proof_members(
    members: list[dict[str, Any]],
    claimed_post_keys: set[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    unique_members: list[dict[str, Any]] = []
    dropped_post_keys: list[str] = []
    for member in members:
        post_key = str(member.get("post_key") or "").strip()
        key = _frontend_post_key(post_key)
        if not key:
            continue
        if key in claimed_post_keys:
            dropped_post_keys.append(post_key)
            continue
        claimed_post_keys.add(key)
        unique_members.append(member)
    return unique_members, dropped_post_keys


def _package_patterns(
    conn: Any,
    *,
    feeder_file_id: int,
    feeder_handle: str,
    selected_patterns: list[dict[str, Any]],
    frontend_pattern_limit: int = 3,
) -> dict[str, Any]:
    if not selected_patterns:
        raise RuntimeError("no feeder file patterns found")
    print(
        "[feeder-file] "
        f"package handle={feeder_handle} feeder_file_id={feeder_file_id} patterns={len(selected_patterns)}",
        flush=True,
    )
    post_keys = _member_post_keys(selected_patterns)
    post_rows = _load_post_inputs_by_keys(conn, post_keys)
    selected_patterns = _canonicalize_patterns_from_loaded_rows(selected_patterns, post_rows)
    print(
        "[feeder-file] "
        f"package loaded_post_rows={len(post_rows)} member_keys={len(post_keys)}",
        flush=True,
    )

    frontend_active_ids: set[str] = set()
    for pattern in selected_patterns:
        if not isinstance(pattern, dict) or not _pattern_is_valid(*_pattern_counts(pattern)):
            continue
        frontend_active_ids.add(str(pattern.get("pattern_id") or "pattern"))
        if len(frontend_active_ids) >= max(1, int(frontend_pattern_limit)):
            break

    active_ids: list[str] = []
    candidate_ids: list[str] = []
    active_count = 0
    frontend_count = 0
    proof_count = 0
    deduped_proof_count = 0
    skipped_proof_count = 0
    claimed_frontend_post_keys: set[str] = set()
    for pattern in selected_patterns:
        pattern_id = str(pattern.get("pattern_id") or f"pattern_{active_count + len(candidate_ids) + 1}")
        core_count, support_count = _pattern_counts(pattern)
        print(
            "[feeder-file] "
            f"package pattern={pattern_id} core={core_count} support={support_count}",
            flush=True,
        )
        if not _pattern_is_valid(core_count, support_count):
            candidate_ids.append(pattern_id)
            _upsert_feeder_file_pattern(
                conn,
                feeder_file_id=feeder_file_id,
                feeder_handle=feeder_handle,
                pattern=pattern,
                status="candidate",
                reason=_pattern_candidate_reason(core_count, support_count),
            )
            continue

        active_ids.append(pattern_id)
        active_count += 1
        if pattern_id not in frontend_active_ids:
            _upsert_feeder_file_pattern(
                conn,
                feeder_file_id=feeder_file_id,
                feeder_handle=feeder_handle,
                pattern=pattern,
                status="active",
                reason="active pattern kept for feeder file memory; frontend read not generated in this display batch",
            )
            continue

        pattern_payload = {
            "account": _prompt_handle(feeder_handle),
            "handle": _prompt_handle(feeder_handle),
            "pattern_profile": _backend_pattern_breakdown(pattern),
        }
        _, pattern_parsed = _call_openrouter(
            conn,
            feeder_file_id=feeder_file_id,
            call_key=f"pattern:{pattern_id}",
            call_type="pattern",
            feeder_handle=feeder_handle,
            pattern_id=pattern_id,
            post_key=None,
            model=FEEDER_FILE_PATTERN_MODEL,
            prompt_version=FEEDER_FILE_PATTERN_PROMPT_VERSION,
            system_prompt=FEEDER_FILE_PATTERN_FRONTEND_PROMPT_V2,
            user_payload=pattern_payload,
            max_tokens=1800,
        )
        pattern_breakdown = pattern_parsed.get("pattern_breakdown")
        if not isinstance(pattern_breakdown, dict):
            raise RuntimeError(f"pattern call did not return pattern_breakdown for {pattern_id}")

        members = pattern.get("members") or []
        if not isinstance(members, list):
            members = []
        skipped_mismatch_keys = [
            str(member.get("post_key") or "").strip()
            for member in members
            if isinstance(member, dict)
            and str(member.get("fit_type") or "").lower() in {"support", "secondary"}
            and member.get("mismatch_fields")
            and str(member.get("post_key") or "").strip()
        ]
        if skipped_mismatch_keys:
            skipped_proof_count += len(skipped_mismatch_keys)
            print(
                "[feeder-file] "
                f"package pattern={pattern_id} skipped_mismatch_proofs={','.join(skipped_mismatch_keys)}",
                flush=True,
            )
        proof_members = [
            member
            for member in members
            if isinstance(member, dict)
            and str(member.get("fit_type") or "core").lower() in {"core", "support", "secondary"}
            and str(member.get("post_key") or "").strip()
            and (
                str(member.get("fit_type") or "core").lower() == "core"
                or not member.get("mismatch_fields")
            )
        ]
        proof_members, dropped_duplicate_keys = _unique_frontend_proof_members(
            proof_members,
            claimed_frontend_post_keys,
        )
        if dropped_duplicate_keys:
            deduped_proof_count += len(dropped_duplicate_keys)
            print(
                "[feeder-file] "
                f"package pattern={pattern_id} dropped_duplicate_proofs={','.join(dropped_duplicate_keys)}",
                flush=True,
            )
        if not proof_members:
            _upsert_feeder_file_pattern(
                conn,
                feeder_file_id=feeder_file_id,
                feeder_handle=feeder_handle,
                pattern=pattern,
                status="active",
                reason="active pattern kept for feeder file memory; frontend read skipped because all proof posts were already claimed by earlier pools",
            )
            continue
        missing = [str(member.get("post_key") or "").strip() for member in proof_members if str(member.get("post_key") or "").strip() not in post_rows]
        if missing:
            raise RuntimeError(
                f"missing post_breakdowns for feeder file proof posts in {pattern_id}: {', '.join(missing)}"
            )
        proofs: list[dict[str, Any]] = []
        for index, member in enumerate(proof_members):
            fit_type = str(member.get("fit_type") or "core").lower()
            if fit_type == "secondary":
                fit_type = "support"
            post_key = str(member.get("post_key") or "").strip()
            row = post_rows[post_key]
            proof_payload = {
                "account": _prompt_handle(feeder_handle),
                "pattern_context": {
                    "account": _prompt_handle(feeder_handle),
                    "pattern_id": pattern_id,
                    "pattern_breakdown": pattern_breakdown,
                },
                "post_fingerprint": row.get("fingerprint") or {},
                "post_breakdown": _breakdown_body(row),
                "membership": member,
            }
            try:
                _, proof_parsed = _call_openrouter(
                    conn,
                    feeder_file_id=feeder_file_id,
                    call_key=f"proof:{pattern_id}:{post_key}",
                    call_type="proof",
                    feeder_handle=feeder_handle,
                    pattern_id=pattern_id,
                    post_key=post_key,
                    model=FEEDER_FILE_PROOF_MODEL,
                    prompt_version=FEEDER_FILE_PROOF_PROMPT_VERSION,
                    system_prompt=FEEDER_FILE_PROOF_FRONTEND_PROMPT_V3,
                    user_payload=proof_payload,
                    max_tokens=1800,
                )
            except Exception as exc:
                skipped_proof_count += 1
                print(
                    "[feeder-file] "
                    f"package pattern={pattern_id} skipped_proof={post_key} reason={str(exc)[:240]}",
                    flush=True,
                )
                continue
            proof = proof_parsed.get("post_proof")
            if not isinstance(proof, dict):
                skipped_proof_count += 1
                print(
                    "[feeder-file] "
                    f"package pattern={pattern_id} skipped_proof={post_key} reason=missing post_proof",
                    flush=True,
                )
                continue
            proofs.append(
                _proof_card(
                    post_key=post_key,
                    fit_type=fit_type,
                    proof=proof,
                    metrics=_metric_cards(row),
                    index=index,
                    total=len(proof_members),
                )
            )
            proof_count += 1

        if not proofs:
            _upsert_feeder_file_pattern(
                conn,
                feeder_file_id=feeder_file_id,
                feeder_handle=feeder_handle,
                pattern=pattern,
                status="active",
                reason="active pattern kept for feeder file memory; frontend read skipped because no proof reads survived validation",
            )
            continue

        pattern_card = _pattern_card(
            feeder_handle=feeder_handle,
            pattern=pattern,
            pattern_breakdown=pattern_breakdown,
            proofs=proofs,
        )
        _upsert_feeder_file_pattern(
            conn,
            feeder_file_id=feeder_file_id,
            feeder_handle=feeder_handle,
            pattern=pattern,
            status="active",
            reason=None,
            pattern_read=pattern_card["pattern"],
            proof_reads=proofs,
        )
        frontend_count += 1

    return {
        "feeder_file_id": feeder_file_id,
        "feeder_handle": feeder_handle,
        "patterns": active_count,
        "frontend_patterns": frontend_count,
        "candidates": len(candidate_ids),
        "proofs": proof_count,
        "deduped_proofs": deduped_proof_count,
        "skipped_proofs": skipped_proof_count,
        "selected_pattern_ids": active_ids,
        "candidate_pattern_ids": candidate_ids,
    }


def package_feeder_file_once(
    conn: Any,
    *,
    feeder_id: int | None = None,
    handle: str | None = None,
    compile_version: str | None = None,
    pattern_id: str | None = None,
    pattern_limit: int = 3,
) -> dict[str, Any]:
    feeder = _resolve_feeder(conn, feeder_id=feeder_id, handle=handle)
    feeder_file = _latest_feeder_file(
        conn,
        feeder_id=int(feeder["id"]),
        feeder_handle=str(feeder["handle"]),
        compile_version=compile_version,
    )
    feeder_file_id = int(feeder_file["id"])
    feed_file = feeder_file.get("feed_file") or {}
    if not isinstance(feed_file, dict):
        raise RuntimeError("stored feeder file is not a JSON object")
    selected_patterns = _patterns_by_id(feed_file, pattern_id, pattern_limit)
    return _package_patterns(
        conn,
        feeder_file_id=feeder_file_id,
        feeder_handle=str(feeder_file.get("feeder_handle") or feeder["handle"]),
        selected_patterns=selected_patterns,
        frontend_pattern_limit=pattern_limit,
    )


def run_feeder_file_once(
    conn: Any,
    *,
    feeder_id: int | None = None,
    handle: str | None = None,
    limit: int = FEEDER_FILE_MEMORY_LIMIT,
    days: int = FEEDER_FILE_MEMORY_DAYS,
    pattern_limit: int = 3,
) -> dict[str, Any]:
    feeder = _resolve_feeder(conn, feeder_id=feeder_id, handle=handle)
    feeder_handle = str(feeder["handle"])
    scoped_limit = min(max(1, int(limit)), max(1, int(FEEDER_FILE_MEMORY_LIMIT)))
    active_window = (
        f"{max(1, int(days))}d_top{scoped_limit}_retention"
        f"{float(FEEDER_FILE_RETENTION_PERCENTILE_MAX):g}"
    )
    rows = _load_post_inputs(conn, feeder_id=int(feeder["id"]), days=days, limit=scoped_limit)
    return _compile_feeder_file_from_rows(
        conn,
        feeder_id=int(feeder["id"]),
        feeder_handle=feeder_handle,
        rows=rows,
        active_window=active_window,
        pattern_limit=pattern_limit,
    )


def run_feeder_file_from_recent_fingerprints_once(
    conn: Any,
    *,
    feeder_id: int | None = None,
    handle: str | None = None,
    limit: int = 10,
    days: int = FEEDER_FILE_MEMORY_DAYS,
    pattern_limit: int = 3,
) -> dict[str, Any]:
    scoped_limit = min(max(1, int(limit)), max(1, int(FEEDER_FILE_MEMORY_LIMIT)))
    feeder = _resolve_feeder(conn, feeder_id=feeder_id, handle=handle)
    feeder_handle = str(feeder["handle"])
    fingerprint_rows = _load_recent_high_fingerprints(
        conn,
        feeder_id=int(feeder["id"]),
        limit=scoped_limit,
        days=days,
    )
    if len(fingerprint_rows) < scoped_limit:
        raise RuntimeError(
            f"need {scoped_limit} high-confidence fingerprints for @{feeder_handle}, found {len(fingerprint_rows)}"
        )

    selected_post_keys: list[str] = []
    created_breakdowns = 0
    ready_breakdowns = 0
    failed_post_keys: list[str] = []
    for row in fingerprint_rows:
        post_key = str(row.get("post_key") or "").strip()
        if not post_key:
            continue
        selected_post_keys.append(post_key)
        existed = bool(row.get("has_breakdown"))
        print(
            "[feeder-file] "
            f"post_breakdown check handle={feeder_handle} post_key={post_key} existed={existed}",
            flush=True,
        )
        breakdown = ensure_post_breakdown(conn, post_key, row.get("fingerprint"))
        if breakdown:
            ready_breakdowns += 1
            if not existed:
                created_breakdowns += 1
        else:
            failed_post_keys.append(post_key)

    if ready_breakdowns < scoped_limit:
        raise RuntimeError(
            f"only {ready_breakdowns}/{scoped_limit} selected fingerprints have post_breakdowns for @{feeder_handle}"
        )

    rows_by_key = _load_post_inputs_by_keys(conn, selected_post_keys)
    compile_rows = [rows_by_key[key] for key in selected_post_keys if key in rows_by_key]
    if len(compile_rows) < scoped_limit:
        missing = [key for key in selected_post_keys if key not in rows_by_key]
        raise RuntimeError(f"missing stored post_breakdowns for compile: {', '.join(missing)}")

    print(
        "[feeder-file] "
        f"compile handle={feeder_handle} post_count={len(compile_rows)} "
        f"created_breakdowns={created_breakdowns} ready_breakdowns={ready_breakdowns}",
        flush=True,
    )
    result = _compile_feeder_file_from_rows(
        conn,
        feeder_id=int(feeder["id"]),
        feeder_handle=feeder_handle,
        rows=compile_rows,
        active_window=(
            f"recent{scoped_limit}_high_fingerprints_"
            f"{max(1, int(days))}d_retention{float(FEEDER_FILE_RETENTION_PERCENTILE_MAX):g}"
        ),
        pattern_limit=pattern_limit,
    )
    return {
        **result,
        "feeder_handle": feeder_handle,
        "selected_post_keys": selected_post_keys,
        "created_breakdowns": created_breakdowns,
        "ready_breakdowns": ready_breakdowns,
        "failed_post_keys": failed_post_keys,
    }
