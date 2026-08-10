# Read dashboard audit — 2026-07-09

## Scope

Review of the existing feed dashboard and the Read page across all-feeds, feed, feeder, and post-evidence states.

## Verdict

The visual system is distinctive and premium, and the All feeds → Feed → Feeder interaction model is a strong base. The current Read page is still better at answering **who is ahead** than **what is working and why**. Its most valuable product opportunity is to promote cross-feed patterns and evidence above the leaderboard.

## Highest-impact findings

1. Add a short executive read at the top: 3–5 pattern cards that name the winning move, show where it repeats, quantify the lift, and provide proof posts.
2. Make the leaderboard secondary. It is useful context, but it should support the pattern story rather than define the page.
3. Use a distinct content hierarchy at each level: cross-feed patterns at All, within-feed patterns at Feed, and post/run diagnosis at Feeder.
4. Replace or relabel unverified metrics. The Read page currently derives `Top %` from a custom engagement score and labels an engagement-derived estimate as `Follower Growth`.
5. Reduce the hero/header footprint and increase microcopy size/contrast. The dashboard should expose more decision-relevant evidence in the first viewport.
6. Clarify comparison bases: raw totals favor large, high-volume accounts; default to normalized performance and allow absolute volume as a secondary view.

## Accessibility risks visible in the captured states

- Several captions are 7–10px with low-opacity text on dark backgrounds.
- The visually fixed scope header is portaled after the main content in DOM order.
- Timeframe selection is visual but is not consistently exposed as a selected state.
- Strong use of semantic buttons, `aria-pressed`, `aria-expanded`, descriptive labels, large touch targets, and reduced-motion handling are positive foundations.

## Evidence

- `00-existing-feed-dashboard.png`
- `01-all-feeds-overview.png`
- `02-feed-overview-creators-stable.png`
- `03-feeder-spotlight-rebel.png`
- `04-feeder-evidence-lower.png`
- `05-weekly-growth-and-evidence.png`

Screenshot review cannot confirm keyboard order, screen-reader announcements, zoom reflow, or final contrast ratios; those require direct accessibility testing.
