from __future__ import annotations

from typing import Any


def _clean(value: Any, limit: int = 140) -> str:
    text = " ".join(str(value or "").split())
    return text[:limit].strip()


def _axis_facts(axes: dict[str, Any], *, prefix: str = "") -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    for axis in ("comments", "likes", "views", "followers", "early_momentum", "evergreen_or_long_tail", "decay_or_dropoff"):
        text = _clean(axes.get(axis), 150)
        if text:
            facts.append({
                "kind": "performance_axis",
                "axis": axis,
                "text": f"{prefix}{text}" if prefix else text,
                "score": 0.70 if axis in {"comments", "views", "followers"} else 0.55,
            })
    return facts


def _shift_facts(shifts: list[Any]) -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    for shift in shifts[:5]:
        text = _clean(shift, 150)
        if text:
            facts.append({"kind": "rulebook_shift", "text": text, "score": 0.82})
    return facts


def _metric_window_facts(packet: dict[str, Any]) -> list[dict[str, Any]]:
    windows = packet.get("metric_windows") if isinstance(packet.get("metric_windows"), dict) else {}
    facts: list[dict[str, Any]] = []
    for fmt, fmt_windows in windows.items():
        if not isinstance(fmt_windows, dict):
            continue
        current = fmt_windows.get("30d") if isinstance(fmt_windows.get("30d"), dict) else {}
        if not current:
            continue
        hit_rate = current.get("d7_top15_rate")
        median = current.get("d7_median_percentile")
        count = current.get("post_count")
        if hit_rate is not None and count:
            facts.append({
                "kind": "metric_window",
                "format": fmt,
                "text": f"{fmt}: top-15 hit rate {round(float(hit_rate) * 100)}% across {count} posts",
                "score": 0.64,
            })
        elif median is not None and count:
            facts.append({
                "kind": "metric_window",
                "format": fmt,
                "text": f"{fmt}: D7 median percentile {round(float(median), 1)} across {count} posts",
                "score": 0.50,
            })
    return facts


def ticker_facts_from_feeder_rulebook(rulebook: dict[str, Any], packet: dict[str, Any]) -> list[dict[str, Any]]:
    axes = rulebook.get("by_performance_axis") if isinstance(rulebook.get("by_performance_axis"), dict) else {}
    facts = [
        *_shift_facts(rulebook.get("shifts_since_last_compile") if isinstance(rulebook.get("shifts_since_last_compile"), list) else []),
        *_axis_facts(axes),
        *_metric_window_facts(packet),
    ]
    return sorted(facts, key=lambda item: float(item.get("score") or 0), reverse=True)[:12]


def ticker_facts_from_feed_rulebook(rulebook: dict[str, Any], packet: dict[str, Any]) -> list[dict[str, Any]]:
    axes = rulebook.get("by_performance_axis") if isinstance(rulebook.get("by_performance_axis"), dict) else {}
    facts = [
        *_shift_facts(rulebook.get("shifts_since_last_compile") if isinstance(rulebook.get("shifts_since_last_compile"), list) else []),
        *_axis_facts(axes, prefix="Feed: "),
        *_metric_window_facts(packet),
    ]
    anchor = _clean(rulebook.get("anchor_vs_feed"), 150)
    if anchor:
        facts.append({"kind": "anchor_vs_feed", "text": anchor, "score": 0.78})
    return sorted(facts, key=lambda item: float(item.get("score") or 0), reverse=True)[:14]
