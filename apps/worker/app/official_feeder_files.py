from __future__ import annotations

import json
from typing import Any

from psycopg.rows import dict_row


OFFICIAL_FEEDER_FILE_VERSION = "manual_feeder_file_v1_2026_05_24"
OFFICIAL_FEEDER_FILE_SOURCE = "manual_seed_from_validated_feederboard"


OFFICIAL_FEEDER_FILES: list[dict[str, Any]] = [
    {
        "feeder_id": "saniyamirwani",
        "handle": "saniyamirwani",
        "compile_version": OFFICIAL_FEEDER_FILE_VERSION,
        "source_breakdown_version": "post_breakdown_v2_sonnet_4_6",
        "active_window": "90d_top100",
        "pool_min_core_posts": 3,
        "feederboard_focus": {
            "focus_id": "pattern_02",
            "tile_label": "Caught Performing",
            "tile_headline": "Showing the feeling you're pretending not to have",
            "tile_read": (
                "The viewer walks into a scene that reads as normal, then a reframe arrives early enough "
                "to prime inspection instead of passive watching. Attention holds in the gap between the "
                "performance and what the frame quietly gives away."
            ),
            "proof_post_keys": [
                "p/dxxb6rrif3e#f16",
                "p/dxewewacicl#f16",
                "p/dxtkzzzcfna#f16",
            ],
        },
        "patterns": [
            {
                "pattern_id": "pattern_02",
                "status": "active",
                "core_post_count": 3,
                "secondary_post_count": 0,
                "pattern_profile": {
                    "headline": "showing the feeling you're pretending not to have",
                    "works_because": (
                        "The post works because a performed emotional state is contradicted by the frame itself. "
                        "The viewer is not waiting for a confession; they are inspecting the face, caption, lens, "
                        "or POV for the feeling the performer is trying to keep socially hidden."
                    ),
                    "opens_with": (
                        "It opens inside a scene that can pass as normal social behavior before an early caption, "
                        "frame choice, or performance detail tells the viewer what feeling to inspect."
                    ),
                    "holds_attention_by": (
                        "It holds attention by widening the gap between the performed cover and the visible tell. "
                        "The viewer keeps scanning for the slip: a face cracking, a camera editorializing, or a "
                        "status fantasy quietly failing to match reality."
                    ),
                    "viewer_mode": (
                        "Inspecting and witnessing; the viewer feels in on the hidden read before the post says it directly."
                    ),
                    "lands_as": (
                        "Recognition. The viewer leaves with the small satisfaction of seeing the real feeling before "
                        "the performer fully admits it."
                    ),
                    "match_if": [
                        "A visible gap exists between performed feeling and actual feeling.",
                        "A reframe primes inspection rather than passive watching.",
                        "A visual confirm closes the emotional gap.",
                        "The hidden feeling is recognizable without explanation.",
                    ],
                    "avoid_if": [
                        "The post states the feeling directly without making the viewer inspect a performance.",
                        "The visual confirm never arrives.",
                        "The tone stays sincere throughout.",
                        "The structure is direct confession rather than performed cover.",
                    ],
                },
                "members": [
                    {
                        "post_key": "p/dxxb6rrif3e#f16",
                        "fit_type": "core",
                        "matched_fields": ["works_because", "holds_attention_by", "viewer_mode", "lands_as"],
                        "weaker_fields": ["opens_with"],
                        "mismatch_fields": [],
                    },
                    {
                        "post_key": "p/dxewewacicl#f16",
                        "fit_type": "core",
                        "matched_fields": ["works_because", "holds_attention_by", "viewer_mode", "lands_as"],
                        "weaker_fields": ["opens_with"],
                        "mismatch_fields": [],
                    },
                    {
                        "post_key": "p/dxtkzzzcfna#f16",
                        "fit_type": "core",
                        "matched_fields": ["works_because", "holds_attention_by", "viewer_mode", "lands_as"],
                        "weaker_fields": ["opens_with"],
                        "mismatch_fields": [],
                    },
                ],
            }
        ],
    },
    {
        "feeder_id": "trysugar",
        "handle": "trysugar",
        "compile_version": OFFICIAL_FEEDER_FILE_VERSION,
        "source_breakdown_version": "post_breakdown_v2_sonnet_4_6",
        "active_window": "90d_top100",
        "pool_min_core_posts": 3,
        "feederboard_focus": {
            "focus_id": "enumerated-shade-range-completion",
            "tile_label": "Shade Range Closer",
            "tile_headline": "Turn a full range into a loop viewers must finish",
            "tile_read": (
                "The viewer is handed a countable set and immediately understands the sequence has to close. "
                "Each shade runs its own small arc — reveal, apply, react — and the viewer stays to finish "
                "the comparison they started."
            ),
            "proof_post_keys": [
                "p/dye-ckenrme#f10",
                "p/dxrbg0zilbr#f10",
                "p/dxmvkifcctz#f10",
            ],
        },
        "patterns": [
            {
                "pattern_id": "enumerated-shade-range-completion",
                "status": "active",
                "core_post_count": 3,
                "secondary_post_count": 0,
                "pattern_profile": {
                    "headline": "turn a full range into a loop viewers must finish",
                    "works_because": (
                        "The post works because the viewer is given a named, countable set and immediately "
                        "understands that the sequence has to close. The range becomes a task: watch every unit, "
                        "compare the variable, and finish the decision loop."
                    ),
                    "opens_with": (
                        "It opens by declaring the set through all tubes, a numbered shade, or a calendar slot, "
                        "so the viewer knows the post is not a single product beat."
                    ),
                    "holds_attention_by": (
                        "It holds attention through repeated micro-loops. Each shade appears, lands on skin, and "
                        "resolves before the next one starts, while the count keeps the viewer aware of how much "
                        "of the sequence remains."
                    ),
                    "viewer_mode": (
                        "Comparing and tracking; the viewer is actively narrowing options rather than receiving a recommendation."
                    ),
                    "lands_as": (
                        "Completion. The viewer leaves with the feeling that the whole range has been seen and a "
                        "shortlist has been self-constructed."
                    ),
                    "match_if": [
                        "Multiple shades are presented as a named, countable sequence.",
                        "Each shade runs through the same micro-loop.",
                        "The viewer is positioned as comparator, not recipient.",
                        "The post closes with a completion signal for the full set.",
                    ],
                    "avoid_if": [
                        "Only one or two shades appear.",
                        "Shades are background texture rather than the active sequence.",
                        "The post tells the viewer which shade to choose.",
                        "The range is open-ended with no count or closure.",
                    ],
                },
                "members": [
                    {
                        "post_key": "p/dye-ckenrme#f10",
                        "fit_type": "core",
                        "matched_fields": ["works_because", "opens_with", "holds_attention_by", "viewer_mode", "lands_as"],
                        "weaker_fields": [],
                        "mismatch_fields": [],
                    },
                    {
                        "post_key": "p/dxrbg0zilbr#f10",
                        "fit_type": "core",
                        "matched_fields": ["works_because", "opens_with", "holds_attention_by", "viewer_mode", "lands_as"],
                        "weaker_fields": [],
                        "mismatch_fields": [],
                    },
                    {
                        "post_key": "p/dxmvkifcctz#f10",
                        "fit_type": "core",
                        "matched_fields": ["works_because", "opens_with", "holds_attention_by", "viewer_mode", "lands_as"],
                        "weaker_fields": [],
                        "mismatch_fields": [],
                    },
                ],
            }
        ],
    },
    {
        "feeder_id": "anuj_mp4",
        "handle": "anuj.mp4",
        "compile_version": OFFICIAL_FEEDER_FILE_VERSION,
        "source_breakdown_version": "post_breakdown_v2_sonnet_4_6",
        "active_window": "90d_top100",
        "pool_min_core_posts": 3,
        "feederboard_focus": {
            "focus_id": "gap_between_claimed_and_visible",
            "tile_label": "Self-Image Collapse",
            "tile_headline": "The claim and the proof arrive together",
            "tile_read": (
                "The viewer watches someone perform a confident version of themselves while the evidence against "
                "that version accumulates in the same frame. No twist, no resolution — just the gap held open until "
                "the weight of it lands."
            ),
            "proof_post_keys": [
                "p/dxy_ou6kc9k#f27",
                "p/dxoh-undfw1#f27",
                "p/dxjldsojbr7#f27",
            ],
        },
        "patterns": [
            {
                "pattern_id": "gap_between_claimed_and_visible",
                "status": "active",
                "core_post_count": 3,
                "secondary_post_count": 1,
                "pattern_profile": {
                    "headline": "the claim and the proof arrive together",
                    "works_because": (
                        "The post works because someone performs a confident self-image while evidence against "
                        "that self-image accumulates in the same frame. The viewer does not discover a twist; they "
                        "confirm the gap as the subject keeps talking or escalating."
                    ),
                    "opens_with": (
                        "It opens with the subject already mid-claim, mid-confession, or mid-performance, giving the "
                        "viewer a clear version of who the subject believes they are."
                    ),
                    "holds_attention_by": (
                        "It holds attention by letting contradiction sit visibly beside the claim. The subject keeps "
                        "performing certainty while another person, the camera, or their own body language quietly "
                        "proves the claim unstable."
                    ),
                    "viewer_mode": (
                        "Confirming and judging; the viewer holds the contradiction without needing the post to name it."
                    ),
                    "lands_as": (
                        "Evidence. The viewer leaves with a quotable or visual receipt of the gap between self-image "
                        "and reality."
                    ),
                    "match_if": [
                        "A subject performs a clear self-image before contradiction appears.",
                        "Contradicting evidence is visible in the same frame or exchange.",
                        "The subject never acknowledges or closes the gap.",
                        "Viewer satisfaction comes from confirmation, not surprise.",
                    ],
                    "avoid_if": [
                        "The subject knowingly performs the gap for the camera from the start.",
                        "The contradiction is withheld as a final twist.",
                        "The subject admits fault, grows, or resolves the tension.",
                        "An outside voice explains the gap instead of letting the viewer hold it.",
                    ],
                },
                "members": [
                    {
                        "post_key": "p/dxy_ou6kc9k#f27",
                        "fit_type": "core",
                        "matched_fields": ["works_because", "opens_with", "holds_attention_by", "viewer_mode", "lands_as"],
                        "weaker_fields": [],
                        "mismatch_fields": [],
                    },
                    {
                        "post_key": "p/dxoh-undfw1#f27",
                        "fit_type": "core",
                        "matched_fields": ["works_because", "opens_with", "holds_attention_by", "viewer_mode", "lands_as"],
                        "weaker_fields": [],
                        "mismatch_fields": [],
                    },
                    {
                        "post_key": "p/dxjldsojbr7#f27",
                        "fit_type": "core",
                        "matched_fields": ["works_because", "opens_with", "holds_attention_by", "viewer_mode", "lands_as"],
                        "weaker_fields": [],
                        "mismatch_fields": [],
                    },
                    {
                        "post_key": "p/dxmdkfxqanx#f27",
                        "fit_type": "secondary",
                        "matched_fields": ["holds_attention_by", "viewer_mode", "lands_as"],
                        "weaker_fields": ["works_because"],
                        "mismatch_fields": ["opens_with"],
                    },
                ],
            }
        ],
    },
]


def upsert_official_feeder_files(conn: Any) -> dict[str, Any]:
    """Seed validated feeder files so the backend has the same canonical reads as Feederboard."""
    selected = len(OFFICIAL_FEEDER_FILES)
    upserted = 0
    with conn.cursor(row_factory=dict_row) as cur:
        for feed_file in OFFICIAL_FEEDER_FILES:
            handle = str(feed_file["handle"]).strip().lower()
            cur.execute(
                """
                select id
                from public.feeders
                where lower(coalesce(handle, '')) = %s
                order by id desc
                limit 1
                """,
                (handle,),
            )
            feeder_row = cur.fetchone()
            feeder_id = feeder_row["id"] if feeder_row else None
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
                  status = excluded.status,
                  source = excluded.source,
                  updated_at = now()
                """,
                (
                    feeder_id,
                    handle,
                    json.dumps(feed_file),
                    str(feed_file["compile_version"]),
                    str(feed_file.get("source_breakdown_version") or ""),
                    str(feed_file.get("active_window") or ""),
                    OFFICIAL_FEEDER_FILE_SOURCE,
                ),
            )
            upserted += 1
    conn.commit()
    return {
        "selected": selected,
        "upserted": upserted,
        "compile_version": OFFICIAL_FEEDER_FILE_VERSION,
        "source": OFFICIAL_FEEDER_FILE_SOURCE,
    }
