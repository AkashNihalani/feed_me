"""Replay saved three-week account fixtures through the locked Account Reader prompt."""
from __future__ import annotations

import json
import re
import sys
import argparse
from pathlib import Path

WORKER_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_DIR))

from scripts.run_reader_replay import _load_env, _openrouter_full, parse_json  # noqa: E402

MODEL = "openai/gpt-5.6-terra"
PROMPT = (WORKER_DIR / "scripts" / "account_reader_locked_prompt.md").read_text()
ANUJ_SOURCE = WORKER_DIR / "scripts" / "out" / "feeder_reader_v6_three_week_terra" / "anuj_mp4"
TRAYA_SOURCE = WORKER_DIR / "scripts" / "out" / "feeder_reader_v2_two_leg" / "traya_health" / "02_update_2026-W26" / "payload.json"
OUT_ROOT = WORKER_DIR / "scripts" / "out" / "account_reader_motion_three_week_terra"


def source_payloads(handle: str) -> list[dict]:
    if handle == "anuj.mp4":
        return [json.loads(path.read_text()) for path in sorted(ANUJ_SOURCE.glob("*/payload.json"))]

    fixture = json.loads(TRAYA_SOURCE.read_text())
    cards = fixture["evidence_pack"]
    triggers_by_ref = {
        ref: {
            "post_id": ref,
            "trigger_context": focus.get("trigger_context") or {},
            "facts": focus.get("facts") or [],
        }
        for focus in fixture.get("required_focus") or []
        for ref in focus.get("focus_refs") or []
    }
    payloads: list[dict] = []
    for index, age in enumerate(("4 weeks ago", "3 weeks ago", "2 weeks ago"), 1):
        new_indexes = [i for i, card in enumerate(cards) if card["posted"] == age]
        if not new_indexes:
            raise RuntimeError(f"traya fixture has no cards for {age}")

        def row(card: dict) -> dict:
            return {
                "post_id": card["card_id"],
                "post_title": card["post_title"],
                "posted": card["posted"],
                "lane": card["lane"],
                "newly_settled": card["posted"] == age,
                "coverage": card.get("coverage") or {"scope": "full"},
                "performance": card.get("performance") or {},
                "post_card": card["post_card"],
            }

        new_cards = [row(cards[i]) for i in new_indexes]
        archive = [row(card) for card in cards[max(new_indexes) + 1:]]
        payloads.append({
            "account": fixture["account"],
            "week": {
                "label": f"SIM-W{index:02d}",
                "simulation": True,
                "source_card_age": age,
                "settled_post_count": len(new_cards),
            },
            "active_feeder_files": [],
            "new_posts": new_cards,
            "trigger_flags": [triggers_by_ref[card["post_id"]] for card in new_cards if card["post_id"] in triggers_by_ref],
            "archive_posts": archive,
            "evidence_history": [],
            "recent_packages": [],
        })
    return payloads


def clean_card(item: dict, trigger_by_id: dict[str, dict]) -> dict:
    performance = item.get("performance") or {}
    card = re.sub(r"(?mi)^\s*Post Ref:.*(?:\n|$)", "", item.get("post_card") or "").strip()
    result = {
        "title": item["post_title"],
        "posted": item.get("posted"),
        "lane": item.get("lane"),
        "recent_rank": performance.get("recent_window"),
        "overall_rank": performance.get("overall_90d"),
        "post_card": card,
    }
    trigger = trigger_by_id.get(item.get("post_id"))
    if trigger:
        result["trigger"] = {
            "facts": trigger.get("facts") or [],
            "context": trigger.get("trigger_context") or {},
        }
    return {key: value for key, value in result.items() if value not in (None, [], {})}


def previous_refs(previous_bites: list[dict]) -> list[str]:
    refs: list[str] = []
    for bite in previous_bites:
        for field in ("evidence_refs", "counterevidence_refs"):
            refs.extend(bite.get(field) or [])
        refs.extend((bite.get("reinterpretation") or {}).get("evidence_refs") or [])
    return list(dict.fromkeys(refs))


def build_request(source: dict, previous_outputs: list[dict]) -> dict:
    triggers = {item["post_id"]: item for item in source.get("trigger_flags") or []}
    new = [clean_card(item, triggers) for item in source["new_posts"]]
    archive = [clean_card(item, triggers) for item in source["archive_posts"]]
    by_title = {item["title"]: item for item in archive}

    selected = list(new)
    seen = {item["title"] for item in selected}
    previous_portraits = [
        {
            "run_offset": offset,
            "relationship": "immediately_previous" if offset == 1 else "two_runs_ago",
            "bites": previous_outputs[-offset].get("bites") or [],
        }
        for offset in range(1, min(2, len(previous_outputs)) + 1)
    ]
    prior_bites = [bite for portrait in previous_portraits for bite in portrait["bites"]]
    for title in previous_refs(prior_bites):
        if title in by_title and title not in seen:
            selected.append(by_title[title])
            seen.add(title)
    for item in archive:
        if len(selected) >= 40:
            break
        if item["title"] not in seen:
            selected.append(item)
            seen.add(item["title"])

    return {
        "account": source["account"],
        "week": source["week"],
        "current_posts": selected,
        "previous_portraits": previous_portraits,
    }


def validate(output: dict, request: dict) -> list[str]:
    issues: list[str] = []
    titles = {item["title"] for item in request["current_posts"]}
    bites = output.get("bites") or []
    if not 1 <= len(bites) <= 4:
        issues.append(f"bites: {len(bites)}, expected 1-4")
    allowed = {"new", "held", "strengthened", "sharpened", "narrowed", "recast"}
    portraits = request.get("previous_portraits") or []
    immediate_bites = {
        bite.get("bite_id"): bite
        for portrait in portraits
        if portrait.get("run_offset") == 1
        for bite in portrait.get("bites") or []
    }
    immediate_ids = set(immediate_bites)
    for bite in bites:
        if bite.get("movement") not in allowed:
            issues.append(f"{bite.get('bite_id')}: invalid movement")
        if bite.get("movement") == "new" and bite.get("bite_id") in immediate_ids:
            issues.append(f"{bite.get('bite_id')}: existing immediate Bite marked new")
        if bite.get("movement") != "new" and bite.get("bite_id") not in immediate_ids:
            issues.append(f"{bite.get('bite_id')}: continuing movement lacks immediate previous Bite")
        if (bite.get("movement") in {"held", "strengthened"}
                and bite.get("title") != (immediate_bites.get(bite.get("bite_id")) or {}).get("title")):
            issues.append(f"{bite.get('bite_id')}: {bite.get('movement')} must preserve the previous title")
        refs = bite.get("evidence_refs") or []
        if len(refs) < 2:
            issues.append(f"{bite.get('bite_id')}: fewer than 2 evidence refs")
        for field in ("evidence_refs", "counterevidence_refs"):
            for ref in bite.get(field) or []:
                if ref not in titles:
                    issues.append(f"{bite.get('bite_id')}: unknown {field} title {ref!r}")
        reinterpretation = bite.get("reinterpretation")
        if bite.get("movement") in {"sharpened", "narrowed", "recast"} and not reinterpretation:
            issues.append(f"{bite.get('bite_id')}: movement requires reinterpretation")
        if bite.get("movement") in {"new", "held"} and reinterpretation is not None:
            issues.append(f"{bite.get('bite_id')}: movement requires null reinterpretation")
        for ref in (reinterpretation or {}).get("evidence_refs") or []:
            if ref not in titles:
                issues.append(f"{bite.get('bite_id')}: unknown reinterpretation title {ref!r}")
    for index, observation in enumerate(output.get("observations") or [], 1):
        refs = observation.get("post_refs") or []
        if len(refs) < 3:
            issues.append(f"observation {index}: fewer than 3 post refs")
        for ref in refs:
            if ref not in titles:
                issues.append(f"observation {index}: unknown post title {ref!r}")
    if any("retir" in json.dumps(value).lower() for value in output.values()):
        issues.append("output mentions retirement")
    return issues


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--handle", choices=("anuj.mp4", "traya.health"), default="anuj.mp4")
    args = parser.parse_args()
    _load_env()
    out = OUT_ROOT / args.handle.replace(".", "_")
    out.mkdir(parents=True, exist_ok=True)
    (out / "prompt.md").write_text(PROMPT)
    (out / "model.txt").write_text(MODEL)
    outputs: list[dict] = []
    summary: list[dict] = []

    for index, source in enumerate(source_payloads(args.handle), 1):
        request = build_request(source, outputs)
        week_dir = out / f"{index:02d}_{source['week']['label']}"
        week_dir.mkdir(exist_ok=True)
        (week_dir / "request.json").write_text(json.dumps(request, indent=2, ensure_ascii=False))

        output_path = week_dir / "output.json"
        if output_path.exists():
            output = json.loads(output_path.read_text())
            outputs.append(output)
            summary.append({
                "week": source["week"]["label"],
                "posts": len(request["current_posts"]),
                "previous_portraits": len(request["previous_portraits"]),
                "immediate_previous_bites": len(request["previous_portraits"][0]["bites"]) if request["previous_portraits"] else 0,
                "header": output["this_week"]["header"],
                "bites": [{"id": bite["bite_id"], "movement": bite["movement"], "title": bite["title"]}
                          for bite in output["bites"]],
            })
            print(source["week"]["label"], "->", output["this_week"]["header"], "[cached]")
            continue

        text, finish = _openrouter_full(PROMPT, request, MODEL, 9000, temperature=0.35, attempts=1)
        (week_dir / "output_raw.txt").write_text(text)
        output = parse_json(text)
        issues = validate(output, request)
        (week_dir / "validation.json").write_text(json.dumps(issues, indent=2))
        (week_dir / "finish_reason.txt").write_text(finish)
        if issues:
            raise RuntimeError(f"{source['week']['label']} failed validation: {issues}")

        output.pop("_finish_reason", None)
        output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False))
        outputs.append(output)
        summary.append({
            "week": source["week"]["label"],
            "posts": len(request["current_posts"]),
            "previous_portraits": len(request["previous_portraits"]),
            "immediate_previous_bites": len(request["previous_portraits"][0]["bites"]) if request["previous_portraits"] else 0,
            "header": output["this_week"]["header"],
            "bites": [{"id": bite["bite_id"], "movement": bite["movement"], "title": bite["title"]}
                      for bite in output["bites"]],
        })
        print(source["week"]["label"], "->", output["this_week"]["header"])

    (out / "simulation_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print("artifacts:", out)


if __name__ == "__main__":
    main()
