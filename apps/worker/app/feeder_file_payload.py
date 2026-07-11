"""Pre-LLM payload assembly for the feeder file: within-lane ranks + the Opus input.

The feeder-file prompt judges landing only from a server-computed within-lane rank, so
the model never invents or re-ranks a number. This module computes those ranks
deterministically and assembles the exact JSON the cold-start prompt expects.

The DB fetch lives in the worker, not here: the caller supplies, per post, a numeric
performance `metric` (higher = better — views, or your existing composite) and the
`media_type`. within_lane_ranks ranks each post against the same-media-type pool you
pass in (the 90-day ranking window), producing the "reel N/POOL" strings the prompt reads.

Pure / deterministic: no model calls, no DB.
"""
from __future__ import annotations

from typing import Any


def within_lane_ranks(pool: list[dict[str, Any]]) -> dict[str, str]:
    """pool: [{post_key, media_type, metric}], metric numeric (higher = better).
    Returns {post_key: "reel N/POOL"} ranked WITHIN each media_type, 1 = best. Posts
    with a missing metric sort last. POOL is the count in that lane."""
    by_type: dict[str, list[dict[str, Any]]] = {}
    for p in pool:
        mt = str(p.get("media_type") or "reel").strip().lower()
        by_type.setdefault(mt, []).append(p)
    ranks: dict[str, str] = {}
    for mt, items in by_type.items():
        ordered = sorted(items, key=lambda p: (p.get("metric") is None, -(p.get("metric") or 0)))
        total = len(ordered)
        for i, p in enumerate(ordered, start=1):
            pk = str(p.get("post_key") or "").strip()
            if pk:
                ranks[pk] = f"{mt} {i}/{total}"
    return ranks


def build_feeder_payload(
    handle: str,
    posts: list[dict[str, Any]],
    pool: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Assemble the cold-start feeder-file LLM input.

    posts: the window to read — [{post_key, media_type, posted_at, metric, fingerprint}].
    pool:  the 90-day same-lane ranking pool — [{post_key, media_type, metric}]. When
           omitted, the posts rank within themselves (fine for a small bank test, but in
           production pass the full 90d pool so ranks reflect real standing).
    """
    rank_pool = pool if pool is not None else [
        {"post_key": p.get("post_key"), "media_type": p.get("media_type"), "metric": p.get("metric")}
        for p in posts
    ]
    ranks = within_lane_ranks(rank_pool)
    out_posts: list[dict[str, Any]] = []
    for p in posts:
        pk = str(p.get("post_key") or "").strip()
        out_posts.append({
            "post_key": pk,
            "media_type": str(p.get("media_type") or "reel").strip().lower(),
            "posted_at": p.get("posted_at"),
            "rank": ranks.get(pk, ""),
            "fingerprint": p.get("fingerprint"),
        })
    return {"handle": handle, "posts": out_posts}


if __name__ == "__main__":
    import json
    import sys
    payload = json.load(open(sys.argv[1]))  # {handle, posts:[...], pool?:[...]}
    out = build_feeder_payload(payload["handle"], payload["posts"], payload.get("pool"))
    print(json.dumps(out, indent=2, ensure_ascii=False))
