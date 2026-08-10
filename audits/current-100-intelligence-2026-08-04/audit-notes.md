# Feed Me Current-100 Intelligence Review

Date: 2026-08-04

## Scope

Review the existing Feeder Reader cover, 90-day orbit, account portrait,
post-card evidence rail, and current field summary. The user goal is to make
the entire current rolling account legible and explorable, not merely to
publish a prose summary of the latest change.

## Captured steps

1. `01-reader-cover.png` — healthy. The run, account, memory count, and current
   headline are immediately clear.
2. `02-current-orbit.png` — weak as an account model. It shows only the current
   run's five landings inside the larger memory, so the rings read as a sparse
   rank plot rather than the account's current hundred-post world.
3. `03-post-card-evidence.png` — healthy evidence interaction. The featured
   post and receipts are concrete and selectable, but the horizontal rail does
   not reveal the chronological pressure corridor or why one post is the
   boundary.
4. `04-account-portrait.png` — healthy fast read. Three active readings and
   their movements are easy to scan, but the user must accept the prose without
   seeing the shape of the evidence across the complete retained world.
5. `05-current-field.png` — clear writing, limited depth. It reports the chosen
   synthesis and three observations but cannot show the rest of the account,
   counterpressure, unexplained posts, or how the current state formed.

## Main product finding

The current experiment is a strong editorial reader but not yet a visual
account model. It progressively reveals conclusions; it does not let the user
inspect the whole retained world and discover the pressure themselves.

The primary intelligence surface should keep all current Postcards present and
use progressive visual lenses:

- Chronology: all posts inside the current rolling window, visibly entering and
  leaving in ten-post updates.
- Meaning: two to four current readings bound to supporting, boundary, and
  unexplained Postcards.
- Pressure: a selected post's consecutive same-media ripple, stopper, current
  rolling rank, and material checkpoint trajectory.
- Evolution: compare adjacent ten-post world versions without treating an old
  portrait as current truth.

A literal always-on knowledge graph would become a hairball. Connections should
appear only for the selected reading or post. The default view should remain a
calm map of the whole current world.

## Accessibility risks visible in screenshots

- Several uppercase labels and rail receipts are extremely small and low
  contrast on black; these need larger readable defaults and contrast testing.
- The evidence rail is horizontally scrollable but has no strong visible cue
  that more receipts continue offscreen.
- Motion carries meaning across beats; reduced-motion mode exists in code, but
  the static replacement must communicate entry, exit, and selected state just
  as clearly.
- Screenshot review cannot verify keyboard traversal, focus order, screen-reader
  grouping, live-region behavior, contrast ratios, or zoom/reflow resilience.

## Recommended architecture

Use an immutable performance-blind Postcard store plus a versioned deterministic
post-state ledger. Recompute the current world after every ten newly matured D7
posts: all eligible posts when the rolling 90-day set is below 100, otherwise
the newest 100. Store entry, cap-exit, and age-out as distinct events.

For each post, retain lane-scoped D7 comparator facts, recent consecutive ripple
and stopper, three-band ripple trace, rolling rank with denominator, and
checkpoint trajectory. Bind each current reading to supporting and boundary
Postcard IDs. Rebind those readings to the current window on every version;
prior portraits are provenance, never authority.
