"""Deterministic server-side application of a feeder-file merge decision plan.

The merge model (FEEDER_FILE_MERGE_SYSTEM_V1) returns only a compact decision
plan. This module APPLIES it — folding cited receipts verbatim, rolling the
window, recomputing tallies/tiers, and enforcing caps. Receipts are never
rewritten here either: they are copied byte-for-byte from the source objects,
which is what makes the conveyor belt's evidence immutable by construction.

Cold start is the same path: apply a plan whose chunk_bite_decisions are all
`add_new` against an empty current_file.

Pure / deterministic: no model calls, no DB.
"""
from __future__ import annotations

import copy
from typing import Any

CAPS = {"earned": 8, "candidate": 3, "grammar": 2}

_ALIAS_IN_POST = __import__("re").compile(r"\(([^()]+)\)\s*$")


def stamp_receipts(file_obj: dict[str, Any], source: str, alias_map: dict[str, str]) -> dict[str, Any]:
    """Add receipt_id (`<source>:<bite>:<i>`) and resolve post_key from the
    `post` field's trailing "(P0n)" alias. Run on the chunk and the current
    file before building the merge payload so the plan can cite receipt_ids."""
    for bite in file_obj.get("bites", []):
        for i, r in enumerate(bite.get("receipts", []), start=1):
            r.setdefault("receipt_id", f"{source}:{bite['name']}:{i}")
            if not r.get("post_key"):
                m = _ALIAS_IN_POST.search(str(r.get("post", "")))
                alias = m.group(1).strip() if m else ""
                r["post_key"] = alias_map.get(alias, "")
    return file_obj


def _receipts_by_id(bite: dict[str, Any]) -> dict[str, dict]:
    return {r["receipt_id"]: r for r in bite.get("receipts", []) if r.get("receipt_id")}


def _select(bite: dict[str, Any], receipt_ids: list[str] | None, policy: str) -> list[dict]:
    pool = bite.get("receipts", [])
    if policy == "append_none":
        return []
    if policy == "append_core_only":
        return [r for r in pool if r.get("weight") == "core"]
    if policy == "append_selected":
        ids = set(receipt_ids or [])
        return [r for r in pool if r.get("receipt_id") in ids]
    # append_all (default)
    return list(pool)


def _newest_date(bite: dict[str, Any]) -> str:
    return max((str(r.get("date") or "") for r in bite.get("receipts", [])), default="")


def _recompute(bite: dict[str, Any]) -> None:
    """Tally + tier + kind from surviving receipts. Mutates in place."""
    tally = {"core": 0, "supporting": 0, "standby": 0}
    for r in bite.get("receipts", []):
        w = r.get("weight")
        if w in tally:
            tally[w] += 1
    bite["weights_tally"] = tally
    cs = tally["core"] + tally["supporting"]
    if cs == 0:
        # only standby (or empty) -> grammar if it still has receipts
        bite["kind"] = "grammar"
        bite["tier"] = "provisional"
    elif cs == 1:
        bite["kind"] = "candidate"
        bite["tier"] = "candidate"
    else:
        bite["kind"] = "earned"
        bite["tier"] = "emerging" if cs == 2 else "provisional"


def _roll(current_file: dict[str, Any], in_window: set[str]) -> list[dict]:
    """Drop receipts whose post rolled out of the window; keep bites that still
    have at least one receipt."""
    survivors: list[dict] = []
    for bite in current_file.get("bites", []):
        kept = [r for r in bite.get("receipts", []) if r.get("post_key") in in_window]
        if kept:
            b = copy.deepcopy(bite)
            b["receipts"] = kept
            survivors.append(b)
    return survivors


def _enforce_caps(bites: list[dict]) -> tuple[list[dict], list[str]]:
    dropped: list[str] = []
    kept: list[dict] = []
    for kind, cap in CAPS.items():
        group = [b for b in bites if b.get("kind") == kind]
        # rank best-first: more core, then more supporting, then newer, then base before chunk
        group.sort(key=lambda b: (
            b.get("weights_tally", {}).get("core", 0),
            b.get("weights_tally", {}).get("supporting", 0),
            _newest_date(b),
            0 if b.get("_origin") == "base" else -1,
        ), reverse=True)
        kept.extend(group[:cap])
        dropped.extend(b["name"] for b in group[cap:])
    return kept, dropped


def apply_merge_plan(payload: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    handle = payload.get("handle", "")
    in_window = set(payload.get("in_window_post_keys", []))
    current = copy.deepcopy(payload.get("current_file", {}) or {})
    chunk = payload.get("chunk", {}) or {}

    # 1-2. start from current, roll the window
    bites = _roll(current, in_window)
    by_name: dict[str, dict] = {}
    for b in bites:
        b["_origin"] = "base"
        by_name[b["name"]] = b

    chunk_by_name = {b["name"]: b for b in chunk.get("bites", [])}
    # destination of each chunk bite after add/strengthen, for fold resolution
    dest_of_chunk: dict[str, dict] = {}
    folds: list[dict] = []
    warnings: list[str] = []

    # 3-7. apply chunk bite decisions (folds deferred to a second pass)
    for dec in plan.get("chunk_bite_decisions", []):
        cb = dec.get("chunk_bite")
        action = dec.get("decision")
        src = chunk_by_name.get(cb)
        if src is None:
            warnings.append(f"chunk bite '{cb}' not found in chunk; skipped")
            continue
        sel = _select(src, dec.get("receipt_ids"), dec.get("receipt_policy", "append_all"))

        if action == "strengthen_existing":
            tgt = by_name.get(dec.get("target_bite"))
            if tgt is None:
                warnings.append(f"strengthen target '{dec.get('target_bite')}' missing; treated as add_new")
                action = "add_new"
            else:
                tgt["receipts"].extend(copy.deepcopy(sel))
                dest_of_chunk[cb] = tgt
        if action == "add_new":
            nb = {"name": src["name"], "kind": src.get("kind", "candidate"),
                  "tier": src.get("tier", "candidate"), "contract": src.get("contract", {}),
                  "weights_tally": {}, "receipts": copy.deepcopy(sel), "_origin": "chunk"}
            by_name[nb["name"]] = nb
            dest_of_chunk[cb] = nb
        elif action == "fold_into_chunk_parent":
            folds.append(dec)
        elif action == "suggest_current_rename":
            tgt = by_name.get(dec.get("target_bite"))
            if tgt is not None:
                if dec.get("proposed_name"):
                    by_name.pop(tgt["name"], None)
                    tgt["name"] = dec["proposed_name"]
                    by_name[tgt["name"]] = tgt
                if dec.get("proposed_contract"):
                    tgt["contract"] = dec["proposed_contract"]
        # discard_chunk_bite: nothing

    # second pass: folds append into their parent's destination (if it landed)
    for dec in folds:
        parent = dec.get("target_chunk_bite")
        dest = dest_of_chunk.get(parent)
        src = chunk_by_name.get(dec.get("chunk_bite"))
        if dest is not None and src is not None and dec.get("receipt_policy") != "append_none":
            sel = _select(src, dec.get("receipt_ids"), dec.get("receipt_policy", "append_none"))
            dest["receipts"].extend(copy.deepcopy(sel))

    # current bite decisions (explicit only)
    for dec in plan.get("current_bite_decisions", []):
        name = dec.get("current_bite")
        action = dec.get("decision")
        b = by_name.get(name)
        if b is None:
            continue
        if action in ("drop_rolled_off", "drop_redundant"):
            by_name.pop(name, None)
        elif action == "merge_into_current":
            tgt = by_name.get(dec.get("target_bite"))
            if tgt is not None:
                tgt["receipts"].extend(b["receipts"])
            by_name.pop(name, None)
        elif action == "suggest_rename" and dec.get("proposed_name"):
            by_name.pop(name, None)
            b["name"] = dec["proposed_name"]
            if dec.get("proposed_contract"):
                b["contract"] = dec["proposed_contract"]
            by_name[b["name"]] = b

    # 8-9. recompute tally/tier/kind; drop empties
    final = [b for b in by_name.values() if b.get("receipts")]
    win = max(len(in_window), 1)
    for b in final:
        _recompute(b)
        # Coverage rule: a non-carrying move (core==0) that blankets most of the
        # window is account grammar, not a credited device — ubiquity = no
        # discriminative power, whatever its supporting/standby label.
        if b["weights_tally"]["core"] == 0:
            coverage = len({r.get("post_key") for r in b["receipts"]}) / win
            if coverage >= 0.7:
                b["kind"] = "grammar"
                b["tier"] = "provisional"

    # 10. caps + ordering
    final, dropped = _enforce_caps(final)
    if dropped:
        warnings.append("dropped over cap: " + ", ".join(dropped))
    final.sort(key=lambda b: (
        {"earned": 0, "candidate": 1, "grammar": 2}.get(b.get("kind"), 3),
        -b.get("weights_tally", {}).get("core", 0),
        -b.get("weights_tally", {}).get("supporting", 0),
    ))
    for b in final:
        b.pop("_origin", None)

    # window + post_names from surviving receipts
    post_keys, dates = set(), []
    for b in final:
        for r in b["receipts"]:
            post_keys.add(r.get("post_key"))
            if r.get("date"):
                dates.append(str(r["date"])[:10])
    names = {**(current.get("post_names") or {}), **(chunk.get("post_names") or {})}
    structure = {k: {"count": sum(1 for b in final if b.get("kind") == k), "cap": v} for k, v in
                 [("earned", 8), ("candidate", 3), ("grammar", 2)]}
    # rename structure key candidates->candidates for schema parity
    structure = {"earned": structure["earned"], "candidates": structure["candidate"], "grammar": structure["grammar"]}

    return {
        "feeder_file_version": "rolling_v1",
        "handle": handle,
        "window": {"posts": len(in_window),
                   "from": min(dates) if dates else "",
                   "to": max(dates) if dates else "",
                   "maturity": "rolling"},
        "structure": structure,
        "post_names": names,
        "bites": final,
        "merge_warnings": warnings,
    }


if __name__ == "__main__":
    import json
    import sys
    pl = json.load(open(sys.argv[1]))
    plan = json.load(open(sys.argv[2]))
    out = apply_merge_plan(pl, plan)
    print(json.dumps(out, indent=2, ensure_ascii=False))
