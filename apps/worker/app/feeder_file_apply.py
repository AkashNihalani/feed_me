"""Deterministic enforcement of the cold-start feeder file (post-LLM).

The feeder-file model PROPOSES pools (moves / registers / craft_signatures /
weak_edges). This module ENFORCES the contract the prompt can only request: the
3-example FORMATION FLOOR and the TOP-5-per-field cutoff. Pools are kept or dropped
WHOLE, by counting distinct example posts — the model's prose is never rewritten, so a
cap is a fact, not a hope. Every drop is recorded in trim_log for QA. Mirrors
feeder_merge_apply.py.

Strength ranking when more than 5 survive:
  moves   -> carded-mechanic count, then distinct example posts
  others  -> distinct example posts

Pure / deterministic: no model calls, no DB.
"""
from __future__ import annotations

import copy
from typing import Any

FLOOR = 3          # minimum distinct example posts to FORM anything
TOP_N = 5          # max pools kept per field
SIMPLE_FIELDS = ("registers", "craft_signatures", "weak_edges")


def _distinct_posts(receipts: Any) -> set[str]:
    out: set[str] = set()
    for r in (receipts or []):
        if isinstance(r, dict):
            pk = str(r.get("post_key") or "").strip()
            if pk:
                out.add(pk)
    return out


def _label(pool: dict) -> str:
    return str(pool.get("id") or pool.get("title") or pool.get("device") or "?")


def _trim_simple(pools: list[dict], field: str, log: list[dict]) -> list[dict]:
    survivors: list[dict] = []
    for p in (pools or []):
        n = len(_distinct_posts(p.get("receipts")))
        if n < FLOOR:
            log.append({"field": field, "id": _label(p), "reason": f"below floor ({n} < {FLOOR})"})
            continue
        p["_examples"] = n
        survivors.append(p)
    survivors.sort(key=lambda p: p["_examples"], reverse=True)
    kept, dropped = survivors[:TOP_N], survivors[TOP_N:]
    for p in dropped:
        log.append({"field": field, "id": _label(p), "reason": f"over top-{TOP_N} (by examples)"})
    for p in kept:
        p.pop("_examples", None)
    return kept


def _trim_moves(moves: list[dict], log: list[dict]) -> list[dict]:
    survivors: list[dict] = []
    for m in (moves or []):
        carded: list[dict] = []
        folded_posts: set[str] = set()
        for mech in (m.get("mechanics") or []):
            posts = _distinct_posts(mech.get("receipts"))
            folded_posts |= posts                       # folded mechanics still count toward the move
            if len(posts) >= FLOOR:
                carded.append(mech)
            else:
                log.append({"field": "moves", "id": _label(m),
                            "mechanic": str(mech.get("id") or mech.get("mechanic") or "?"),
                            "reason": f"mechanic below floor ({len(posts)} < {FLOOR}); folded into move"})
        move_posts = _distinct_posts(m.get("receipts")) | folded_posts
        if len(move_posts) < FLOOR and not carded:
            log.append({"field": "moves", "id": _label(m),
                        "reason": f"move below floor ({len(move_posts)} < {FLOOR}, no carded mechanic)"})
            continue
        m["mechanics"] = carded
        m["_mech"] = len(carded)
        m["_examples"] = len(move_posts)
        survivors.append(m)
    survivors.sort(key=lambda m: (m["_mech"], m["_examples"]), reverse=True)
    kept, dropped = survivors[:TOP_N], survivors[TOP_N:]
    for m in dropped:
        log.append({"field": "moves", "id": _label(m), "reason": f"over top-{TOP_N} (by mechanics, then examples)"})
    for m in kept:
        m.pop("_mech", None)
        m.pop("_examples", None)
    return kept


def _prune_dangling_refs(out: dict[str, Any]) -> None:
    """Drop references that now point at trimmed-away pools, so the merge layer never
    chases a dead id."""
    live = set()
    for m in out.get("moves", []):
        if m.get("id"):
            live.add(m["id"])
    for field in SIMPLE_FIELDS:
        for p in out.get(field, []):
            if p.get("id"):
                live.add(p["id"])
    move_ids = {m["id"] for m in out.get("moves", []) if m.get("id")}
    craft_ids = {c["id"] for c in out.get("craft_signatures", []) if c.get("id")}
    for m in out.get("moves", []):
        if isinstance(m.get("craft_used"), list):
            m["craft_used"] = [c for c in m["craft_used"] if c in craft_ids]
    for r in out.get("registers", []):
        if isinstance(r.get("rides_moves"), list):
            r["rides_moves"] = [mid for mid in r["rides_moves"] if mid in move_ids]
    for e in out.get("evidence_index", []):
        if isinstance(e.get("supports"), list):
            e["supports"] = [s for s in e["supports"] if s in live]


def apply_feeder_cutoff(feeder: dict[str, Any]) -> dict[str, Any]:
    """Enforce floor + top-5 on a raw feeder-file JSON. Returns a NEW object with
    trimmed fields and a trim_log[]. Input is not mutated."""
    out = copy.deepcopy(feeder)
    log: list[dict] = []
    out["moves"] = _trim_moves(out.get("moves") or [], log)
    for field in SIMPLE_FIELDS:
        out[field] = _trim_simple(out.get(field) or [], field, log)
    _prune_dangling_refs(out)
    out["trim_log"] = log
    return out


if __name__ == "__main__":
    import json
    import sys
    data = json.load(open(sys.argv[1]))
    print(json.dumps(apply_feeder_cutoff(data), indent=2, ensure_ascii=False))
