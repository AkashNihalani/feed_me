"""Deterministic, pure-Python selector for a feeder account's "feeder file memory".

This module is intentionally free of any database imports. It takes a list of
already-fetched, already-eligible posts (one row per post, latest d7 metric,
same media_type, within the recency window) and chooses the 30 posts that make
up the feeder file memory:

    20 reference slots + 10 recent-context slots

The two lanes are DISJOINT. The recent-context lane is simply the newest
``RECENT_CONTEXT_TARGET`` posts by recency. The reference lane is drawn from the
*rest* of the stream (everything older than the recent lane), so the freshest
posts never compete for both.

The 20 reference slots are filled from NESTED expanding "newest-percent"
subsets of the non-recent stream:

    newest 25% of the non-recent stream  -> best 5
    newest 50% of the non-recent stream  -> best 5
    newest 75% of the non-recent stream  -> best 5
    the full non-recent stream           -> best 5

Subsets are processed tightest -> widest. Within each subset, candidates are
ranked by account-relative d7 performance only, with recency as the tiebreaker
(no views axis). A post takes exactly ONE reference slot -- the tightest
subset that claims it. When a wider subset re-encounters an already-claimed
post it records an extra *membership* (raising the post's ``weight``) and the
freed slot is backfilled with the next-best UNCLAIMED post from the same
subset. ``weight`` therefore measures how durable a post is across the account's
history depth (a post that wins every subset has weight 4).

No absolute winner bar gates the reference lane: each subset contributes its
best 5 even if the account is having a weak stretch. ``bar_percentile`` is still
used to TAG whether a chosen post clears the account-relative winner bar, but it
never blocks selection.

Hard dedupe / light feeders
---------------------------
The two lanes are ALWAYS disjoint and a post is never used twice -- the recent
lane is the newest 10 and the reference lane only ever draws from the stream
older than that. An account with fewer than ~30 d7 reels simply keeps a light
feeder (fewer than 20 reference slots, ``still_learning=True``); we never borrow
from the recent lane to fake depth. Vacant reference slots are backfilled only
from genuinely unclaimed non-recent posts, which the widest ("full") subset
already sweeps in -- so a slot stays vacant only when the posts truly are not
there.

Metric direction
----------------
In this system LOWER percentile means STRONGER (top of the account). The
comparison direction is captured in a single constant, ``LOWER_IS_STRONGER``,
so it is trivial to flip if the empirical check ever says otherwise.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

# --- Tunable selection shape -------------------------------------------------

# Lower account-relative percentile == stronger post (top of the account).
# Empirically verified against production (see dry-run report). Flip this single
# constant if the metric direction is ever inverted.
LOWER_IS_STRONGER: bool = True

RECENT_CONTEXT_TARGET: int = 10
REFERENCE_SLOT_TARGET: int = 20

# Nested "newest-percent" subsets of the non-recent stream, tightest -> widest.
# Each contributes up to SUBSET_KEEP distinct posts; len(SUBSET_PERCENTS) *
# SUBSET_KEEP must equal REFERENCE_SLOT_TARGET.
SUBSET_PERCENTS: tuple[float, ...] = (0.25, 0.50, 0.75, 1.00)
SUBSET_KEEP: int = 5

# Back-compat alias: some callers/tests import the old name.
WINNER_SLOT_TARGET: int = REFERENCE_SLOT_TARGET

# Sentinel used for missing percentiles so they always sort to the weak end
# regardless of metric direction.
_MISSING_PERCENTILE = 101.0


# --- Helpers -----------------------------------------------------------------

def _posted_ts(post: dict[str, Any]) -> float:
    """Recency key as a float timestamp. Missing/invalid -> 0.0 (oldest)."""
    value = post.get("posted_at")
    if isinstance(value, datetime):
        # Treat naive datetimes as UTC so all posts compare on one axis.
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.timestamp()
    return 0.0


def _metric_value(post: dict[str, Any]) -> float:
    """Account-relative d7 metric, exact preferred, then plain. Missing -> weak."""
    exact = post.get("percentile_performance_exact")
    if exact is not None:
        try:
            return float(exact)
        except (TypeError, ValueError):
            pass
    plain = post.get("percentile_performance")
    if plain is not None:
        try:
            return float(plain)
        except (TypeError, ValueError):
            pass
    return _MISSING_PERCENTILE


def _raw_percentile(post: dict[str, Any]) -> Optional[float]:
    """The raw percentile actually used (for tagging), or None if absent."""
    exact = post.get("percentile_performance_exact")
    if exact is not None:
        try:
            return float(exact)
        except (TypeError, ValueError):
            pass
    plain = post.get("percentile_performance")
    if plain is not None:
        try:
            return float(plain)
        except (TypeError, ValueError):
            pass
    return None


def _strength_rank_key(post: dict[str, Any]) -> tuple[float, float]:
    """Sort key: stronger first, recency breaks ties (more recent wins).

    Python sorts ascending, so we return a tuple where the first element is the
    metric oriented so smaller == stronger, and the second is negative recency
    so the most-recent post wins a tie.
    """
    metric = _metric_value(post)
    oriented = metric if LOWER_IS_STRONGER else -metric
    return (oriented, -_posted_ts(post))


def _subset_name(pct: float) -> str:
    """Stable short label for a newest-percent subset (e.g. 0.25 -> 'n25')."""
    if pct >= 1.0:
        return "full"
    return f"n{int(round(pct * 100))}"


def _clears_bar(post: dict[str, Any], bar_percentile: float) -> bool:
    """True if the post is a real winner (clears the account-relative bar)."""
    raw = _raw_percentile(post)
    if raw is None:
        return False
    if LOWER_IS_STRONGER:
        return raw <= bar_percentile
    return raw >= (100.0 - bar_percentile)


def _post_key(post: dict[str, Any]) -> str:
    return str(post.get("post_key") or "")


# --- Main entry --------------------------------------------------------------

def select_active_feeder_file(
    posts: list[dict[str, Any]],
    *,
    bar_percentile: float = 30.0,
) -> dict[str, Any]:
    """Choose the 30-post feeder file memory for one feeder account.

    Parameters
    ----------
    posts:
        Eligible posts (already filtered to one row per post, latest d7 metric,
        same media_type, within the recency window). Each dict should carry:
        post_key, posted_at (datetime), media_type, views, likes, comments,
        percentile_performance, percentile_performance_exact, ranking_metric,
        ranking_multiple.
    bar_percentile:
        Account-relative winner bar (default 30.0 == top 30%). Used only to TAG
        a chosen post's ``winner`` flag; it never gates selection.

    Returns
    -------
    dict with keys: ``reference`` (slot-ordered list), ``recent_context``
    (list), ``counts`` (dict), ``still_learning`` (bool). ``ranked`` is kept as
    an alias of ``reference`` for back-compat.
    """
    # Defensive copy; never mutate caller input.
    pool_all = [dict(p) for p in posts]

    # Most-recent-first ordering is the backbone for both lanes.
    by_recency = sorted(pool_all, key=_posted_ts, reverse=True)

    # --- Recent-context lane: the newest N posts ----------------------------
    recent_slice = by_recency[:RECENT_CONTEXT_TARGET]
    recent_context = [_recent_entry(post) for post in recent_slice]

    # --- Reference lane: drawn from the stream OLDER than the recent lane ----
    # For a mature account (>= REFERENCE_SLOT_TARGET non-recent posts) this is a
    # pure, disjoint lane: the nested subsets fill all 20 slots and the freshest
    # posts never get swallowed out of the subsets.
    pool_stream = by_recency[RECENT_CONTEXT_TARGET:]
    stream_size = len(pool_stream)

    ref_index: dict[str, dict[str, Any]] = {}
    reference: list[dict[str, Any]] = []

    for pct in SUBSET_PERCENTS:
        if stream_size == 0:
            break
        name = _subset_name(pct)
        count = max(1, round(stream_size * pct))
        subset = pool_stream[:count]
        ranked_subset = sorted(subset, key=_strength_rank_key)

        taken = 0
        for post in ranked_subset:
            if taken >= SUBSET_KEEP:
                break
            key = _post_key(post)
            existing = ref_index.get(key)
            if existing is not None:
                # Already claimed by a tighter subset: record the extra
                # membership (raising weight) and backfill this slot with the
                # next-best UNCLAIMED post -- do NOT consume a slot here.
                existing["memberships"].append(name)
                existing["weight"] = len(existing["memberships"])
                continue
            entry = _reference_entry(post, primary=name, bar_percentile=bar_percentile)
            ref_index[key] = entry
            reference.append(entry)
            taken += 1

    # No padding: the two lanes are ALWAYS disjoint and no post is ever used
    # twice. When the non-recent stream cannot supply 20 distinct posts (an
    # account with fewer than ~30 d7 reels), the feeder simply stays light -- we
    # never borrow from the recent lane to fake depth. Backfill of any vacant
    # slot already happens inside the subset pass: the widest ("full") subset
    # sweeps every unclaimed non-recent post, so a slot is only vacant when
    # there genuinely are not enough posts to fill it.
    still_learning = (
        len(reference) < REFERENCE_SLOT_TARGET
        or len(recent_context) < RECENT_CONTEXT_TARGET
    )

    counts = {
        "reference": len(reference),
        "recent_context": len(recent_context),
        "total": len(reference) + len(recent_context),
        "eligible": len(pool_all),
        "non_recent_pool": stream_size,
    }

    return {
        "reference": reference,
        "ranked": reference,  # back-compat alias
        "recent_context": recent_context,
        "counts": counts,
        "still_learning": still_learning,
    }


def _reference_entry(
    post: dict[str, Any], *, primary: str, bar_percentile: float
) -> dict[str, Any]:
    """Build a reference-lane entry, seeded with one subset membership."""
    return {
        "post_key": _post_key(post),
        "posted_at": post.get("posted_at"),
        "metric": _metric_value(post),
        "d7_percentile": _raw_percentile(post),
        "memory_type": "reference",
        "winner": _clears_bar(post, bar_percentile),
        "occupying_slot": "reference_slot",
        "primary": primary,
        "memberships": [primary],
        "weight": 1,
        "views": post.get("views"),
        "likes": post.get("likes"),
        "comments": post.get("comments"),
        "ranking_metric": post.get("ranking_metric"),
        "ranking_multiple": post.get("ranking_multiple"),
    }


def _recent_entry(post: dict[str, Any]) -> dict[str, Any]:
    """Build a recent-context-lane entry."""
    return {
        "post_key": _post_key(post),
        "posted_at": post.get("posted_at"),
        "metric": _metric_value(post),
        "d7_percentile": _raw_percentile(post),
        "memory_type": "recent_context",
        "winner": False,
        "occupying_slot": "recent_context_slot",
        "primary": None,
        "memberships": [],
        "weight": 0,
        "views": post.get("views"),
        "likes": post.get("likes"),
        "comments": post.get("comments"),
        "ranking_metric": post.get("ranking_metric"),
        "ranking_multiple": post.get("ranking_multiple"),
    }


# --- Smoke test (no database) ------------------------------------------------

def _make_post(
    key: str,
    days_ago: int,
    pct: Optional[float],
    *,
    views: int = 1000,
    exact: Optional[float] = None,
    media_type: str = "reel",
) -> dict[str, Any]:
    posted = datetime(2026, 5, 30, 12, 0, 0, tzinfo=timezone.utc)
    posted = posted.replace(day=max(1, 30 - days_ago % 28))
    # Use a real, monotonic timestamp by subtracting days from an epoch base.
    base = datetime(2026, 5, 30, 12, 0, 0, tzinfo=timezone.utc).timestamp()
    posted = datetime.fromtimestamp(base - days_ago * 86400, tz=timezone.utc)
    return {
        "post_key": key,
        "posted_at": posted,
        "media_type": media_type,
        "views": views,
        "likes": views // 10,
        "comments": views // 100,
        "percentile_performance": pct,
        "percentile_performance_exact": exact if exact is not None else pct,
        "ranking_metric": views,
        "ranking_multiple": 1.0,
    }


def _assert_no_dupes(result: dict[str, Any]) -> None:
    """Hard dedupe: no post appears twice, in either lane or across them."""
    keys = [e["post_key"] for e in result["reference"]] + [
        e["post_key"] for e in result["recent_context"]
    ]
    assert len(keys) == len(set(keys)), f"duplicate post_keys across slots: {keys}"


def _run_smoke_tests() -> None:
    # (a) Mature account: 45 posts. recent=10, reference=20, lanes disjoint.
    mature: list[dict[str, Any]] = []
    for i in range(45):
        pct = float((i * 2) % 90) + 1.0
        mature.append(_make_post(f"m{i:02d}", days_ago=i, pct=pct, views=5000 - i * 50))
    res_a = select_active_feeder_file(mature, bar_percentile=30.0)
    _assert_no_dupes(res_a)
    assert res_a["counts"]["total"] == 30, res_a["counts"]
    assert len(res_a["reference"]) == 20, len(res_a["reference"])
    assert len(res_a["recent_context"]) == 10, len(res_a["recent_context"])
    assert res_a["ranked"] is res_a["reference"]  # back-compat alias
    # Lanes are disjoint: the recent 10 are exactly the newest 10.
    newest10 = {f"m{i:02d}" for i in range(10)}
    assert {e["post_key"] for e in res_a["recent_context"]} == newest10
    for e in res_a["reference"]:
        assert e["memory_type"] == "reference"
        assert e["occupying_slot"] == "reference_slot"
        assert e["primary"] in {"n25", "n50", "n75", "full"}
        assert e["weight"] == len(e["memberships"]) >= 1
        assert e["post_key"] not in newest10
    for e in res_a["recent_context"]:
        assert e["memory_type"] == "recent_context" and e["winner"] is False

    # (b) Cold start: 7 posts. No non-recent stream, so the reference lane stays
    # empty -- the feeder is light, never padded with recent-lane duplicates.
    cold = [_make_post(f"c{i}", days_ago=i, pct=float(10 + i)) for i in range(7)]
    res_b = select_active_feeder_file(cold, bar_percentile=30.0)
    _assert_no_dupes(res_b)
    assert res_b["counts"]["total"] == 7, res_b["counts"]
    assert res_b["counts"]["reference"] == 0, res_b["counts"]
    assert res_b["still_learning"] is True

    # (c) Weight: the strongest NON-recent post sits at the head of the stream,
    # so every subset re-encounters it. It takes one slot with weight 4.
    dupe = [_make_post(f"d{i:02d}", days_ago=i, pct=float(50 + i)) for i in range(40)]
    dupe[10]["percentile_performance"] = 1.0  # head of the non-recent stream
    dupe[10]["percentile_performance_exact"] = 1.0
    res_c = select_active_feeder_file(dupe, bar_percentile=30.0)
    _assert_no_dupes(res_c)
    d10 = [e for e in res_c["reference"] if e["post_key"] == "d10"]
    assert len(d10) == 1, d10
    assert d10[0]["primary"] == "n25", d10[0]  # claimed by the tightest subset
    assert d10[0]["weight"] == 4, d10[0]  # won every nested subset
    assert d10[0]["winner"] is True

    # (d) Tie on the metric: two equal-percentile posts in the non-recent
    # stream; the more-recent one ranks ahead.
    tie = [_make_post(f"t{i:02d}", days_ago=i, pct=float(40 + i)) for i in range(40)]
    for idx in (15, 16):
        tie[idx]["percentile_performance_exact"] = 10.0
        tie[idx]["percentile_performance"] = 10.0
    res_d = select_active_feeder_file(tie, bar_percentile=30.0)
    _assert_no_dupes(res_d)
    order = [e["post_key"] for e in res_d["reference"]]
    if "t15" in order and "t16" in order:
        assert order.index("t15") < order.index("t16"), order  # recent wins tie

    print("OK: all selector smoke tests passed.")
    print("  mature:", res_a["counts"], "still_learning=", res_a["still_learning"])
    print("  cold  :", res_b["counts"], "still_learning=", res_b["still_learning"])
    print("  dupe  :", res_c["counts"], "still_learning=", res_c["still_learning"])
    print("  tie   :", res_d["counts"], "still_learning=", res_d["still_learning"])


if __name__ == "__main__":
    _run_smoke_tests()
