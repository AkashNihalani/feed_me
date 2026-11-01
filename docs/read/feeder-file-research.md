# Feeder File: research plan (Oct 26 2026)

Goal: the Feeder File becomes the one component that shows an account at a glance, with clear grouping, nodes and connections the user can pick through ("what is similar, and why"), a structure that visibly grows with every post and run, and a finish that matches the best Obsidian graph and mind-map work.

## What has been tried, and why it failed

| Attempt | Verdict | Why |
|---|---|---|
| 3D solar system (current, `feederSystemEngine.js`) | "way too random" | Positions mean nothing a stranger can read (orbits = how much it is its own, place in a wedge is arbitrary); about 70 threads at once; no time, so no growth; rules sit beside the map, not on it. |
| Metro map (time left→right, a line per thing, rules as forks) | "too basic, doesn't feel like what the feed is about" | Legible but abstract: an analytics chart with no feed imagery and no sense of the account's character. |
| Graph v1 (account at centre, six territories, numbered thing rings, cover tiles) | "amateur and basic… levels behind in finish and finesse" | Designed from the data outward: fat numbered rings read as a bubble chart, crowded labels, flat canvas, no physics or depth. |

The lesson: set the visual bar first from real references, then fit the data to it.

## What the research found (two agents, Oct 26)

**Design patterns** (the web research was light: 3 searches. ExcaliBrain, Graph Analysis, Extended Graph and storyline sources were checked; the rest needs verifying in step 1):
- A local graph with depth (Obsidian, Juggl, TheBrain). The global graph is for orientation only.
- Typed relations in fixed directions (ExcaliBrain: parents above, children below, friends left, siblings right). Position carries meaning.
- Named groups or territories (Canvas groups, Heptabase sections).
- Growth replayed by the user over stable positions (Obsidian time-lapse; its own physics re-shuffles, so ours must not).
- "Similar, and why" as a ranked list whose reason is the shared neighbours (Graph Analysis plugin: Jaccard / Adamic-Adar).
- Type shown by shape and ink weight, not hue (Extended Graph). Edges drawn only for the selected node. Count badges on nodes.
- Don't copy: the global force hairball, idle motion, rainbow groups, 3D for readability, embedding "related" lists with no reason, pinch-pan traps on phones.

**Engineering** (read of the repo):
- The data already supports a graph. Things carry post lists and bands (`planets[].ps`, `cat`), rules carry both sides and their history (`laws[].lands/sinks/life/born`), and posts carry results.
- The per-run snapshots exist: `apps/worker/scripts/out/read_v9/<slug>/state{1,2,3}.json`. `read_v11_to_page.py` throws them away. Keep them (stable thing ids, `born`/`dropped`, sibling and similar posts).
- Swap-in point: a `version: 'graph'` branch in `FeederFile.tsx` (lines 76–89). The engine keeps the `{unmount, open, full}` contract, so `ReadTab.tsx` needs no change. Keep the solar system selectable behind the flag.
- Recommended stack: deterministic layout (seeded, optional d3-force settle that runs once off-screen) with a 2D or WebGL renderer in the engine's plain-JS style. three.js is already installed if glow or bloom needs WebGL.
- Estimate: 10–14 working days in six reviewable steps.

## The bar, as checks

- Finish: hairline links (0.5–1px) with alpha falling off along their length; soft additive glow on what matters; subtle node size range; labels small and light, showing by zoom level; no heavy text halos; a dark-first canvas with a designed light variant.
- Motion: 60fps on iPhone Safari. Hover dims the rest over about 250ms. The camera glides on the carry curve. Dragging a node pulls its neighbours with damped physics, which then come to rest. Nothing moves when idle.
- Meaning: at full view a stranger can say what the account makes, who is in it, what it sells and what works, without tapping. Every position has a reason that can be stated in one line.

## Research steps

1. **Reference board (1 day).** Collect 15–20 real examples: Obsidian graph showcases (forum "Share & showcase", r/ObsidianMD), Juggl, ExcaliBrain, Extended Graph, 3D Graph, Graph Analysis, Canvas; and Cosmograph, Nomic Atlas, Kumu, Neo4j Bloom, Heptabase. Capture stills and short screen recordings. Note what makes each premium (line weight, glow, type, density, motion). You pick the top three, and that becomes the visual target.
2. **Look study (1–2 days).** Three small sketches on Anuj's real data, at the chosen bar, with the same interactions: (a) a luminous force graph with drag physics; (b) an ExcaliBrain-style structured focus view; (c) a territory constellation. No side page, no plumbing. You pick one, or a hybrid (for example (c) as the overview and (b) on tap).
3. **Tech spike (1 day).** Canvas 2D against WebGL (three.js) for glow, 30–100 cover sprites and 60fps on a real iPhone. Measure, then decide.
4. **Data spike (1 day).** Emit per-run snapshots, stable thing ids, similar-post edges (shared things + same result) and siblings into the page data from `state{k}.json`.
5. **Build.** Follow the six steps in the engineering read: data, static overview, selection and local graph, filters, time-lapse, phone and polish.

## Open questions

- Which three references are the bar?
- Is a dark-first canvas acceptable inside the light theme? (Today's rule is "light mode first-class, never a forced dark hero".)
- Does the Feeder File become the centre of the Read tab, or stay a full-screen layer opened from the stories and the wall?

Prototypes kept for reference (not published): session scratchpad `metro/` and `brain/`.
