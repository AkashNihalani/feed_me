# Lead Preview Design QA

source visual truth path: ./source-visual-truth.png

implementation screenshot paths:

- ./desktop-feederboard-source-size.png
- ./mobile-feederboard-final.png
- ./mobile-post-mortem-final.png

viewport:

- Desktop comparison: 1487 × 1058 CSS pixels, matching the source image dimensions.
- Mobile: 390 × 844 CSS pixels.

state: Dark theme, All Feedboards, 30D, first Feederboard row expanded. Visual captures used a temporary in-memory payload matching the production feed, feeder-post, and dashboard contracts because the browser session was unauthenticated. The payload and query switch were removed before handoff; the delivered route uses only authenticated real data.

## Full-view comparison evidence

The source visual and ./desktop-feederboard-source-size.png were opened together in one comparison input at the same dimensions. The implementation keeps the source's bold Feed Me header, circular feed rail, three headline leaders, monochrome black/white palette, restrained rose accent, large numerical jumps, compact board rhythm, and media-led hierarchy. The persistent bottom navigation and fixed/compressing header are intentional app-chrome additions requested after the source was created.

The Feederboard deliberately diverges from the source's wide proof strip: every post is presented in the Fire tab's vertical 4:5 format. The selected row now reads as one ranked performance sentence before opening into an editorial dossier, rather than as a generic nine-column SaaS table.

## Focused region comparison evidence

- Feederboard and mobile hierarchy: ./mobile-feederboard-final.png verifies ghost rank, ring avatar, dominant Top %, supporting baseline multiple, rose selection spine, expansion staging, fixed header, and fixed bottom nav at the constrained breakpoint.
- Post Mortem: ./mobile-post-mortem-final.png verifies the preserved stack/left-exit composition after conversion to 4:5 cards.
- A separate desktop crop was not needed because the source-sized 1487 × 1058 capture renders the collapsed row, selected row, metric ledger, and all three proof posts at readable scale.

## Required fidelity surfaces

- Fonts and typography: The implementation uses the app's existing display/body stack, heavy tabular numerals, tight display tracking, and whisper-small uppercase labels. Hierarchy now jumps from ghost rank to identity to Top % and baseline multiple instead of distributing equal weight across table cells.
- Spacing and layout rhythm: The page is one continuous scoreboard surface with hairline chapter separators. Selected-row height, rose edge, staggered expansion, proof overlap, and compact sibling rows follow the Read/Fire rhythm. Mobile measured 390 px root width, 390 px scroll width, and no horizontal overflow.
- Colors and visual tokens: Near-black surfaces, white/grey hierarchy, and one rose accent match the requested monotone direction. Feed-specific colors are absent from the board.
- Image quality and asset fidelity: Content posts, follower-window thumbnails, and Post Mortem cards render at a measured 0.8 width/height ratio (4:5). Leader media also uses a 4:5 mask. Production imagery comes from authenticated feeder/post media URLs; the temporary QA sample imagery is not shipped.
- Copy and content: Top % remains exclusive to overall post performance. Likes, comments, engagement rate, posts, and follower movement remain raw numbers; baseline multiples provide context without inventing metric-specific percentile labels.

## Comparison history

### Iteration 1

- Earlier finding: [P2] The Feederboard read like a generic nine-column report and gave rank, identity, Top %, baseline, and secondary metrics equal visual weight.
- Fix made: Removed the nine-column table treatment, added oversized ghost ranks and performance-strength fills, grouped secondary metrics into one response trail, and made the selected row visibly taller and brighter.
- Post-fix visual evidence: ./desktop-feederboard-source-size.png.

### Iteration 2

- Earlier finding: [P2] At 390 px, the app-shell grid allowed an intrinsic content width of roughly 1080 px, pushing Top %, baseline, and the chevron beyond the viewport.
- Fix made: Restored an explicit 100vw route boundary while retaining min-width zero and hidden horizontal overflow.
- Post-fix visual evidence: ./mobile-feederboard-final.png. Browser measurements after the fix were root width 390 px, main width 390 px, scroll width 390 px, viewport 390 px.

### Iteration 3

- Earlier finding: [P2] Expanded proof cards, follower-window thumbnails, and Post Mortem cards used square or variable landscape frames, conflicting with Fire's 4:5 post language.
- Fix made: Converted every content thumbnail/card to 4:5 and retained the existing Post Mortem stack poses and mask/exit timings.
- Post-fix visual evidence: ./mobile-post-mortem-final.png. Browser measurements were proof 192.6 × 240.7, follower window 44.8 × 56, and stack 302.4 × 378 CSS pixels.

## Findings

No actionable P0, P1, or P2 differences remain. The vertical proof cards, fixed app chrome, and larger selected-row dossier are intentional requirements rather than design drift.

## Primary interactions tested

- Desktop and mobile: clicking a proof post kept the active Feederboard row expanded and did not change the local URL when no external post URL was present.
- Clicking the open dossier outside its media changed aria-expanded to false and removed the expansion after its 650 ms exit sequence.
- Post Mortem stack advanced from the 5% / comments card to the 8% / likes card after the preserved left-exit transition.
- Header and bottom navigation remained fixed before and after 760 px of route scrolling; header top and nav bottom both stayed at 20 px on desktop.
- Browser console error/warning check returned no entries.

## Console and build evidence

- ESLint passed for the comparison page and feeder-post endpoint.
- TypeScript passed with npx tsc --noEmit.
- Next.js production build passed and generated /read/lead-preview.
- Browser-rendered desktop and mobile evidence is saved at the paths above.

## Open Questions

- Re-capture the same states with the user's authenticated production dataset before promoting this comparison route into the permanent Lead tab. This is a P3 evidence gap, not a UI blocker.

## Implementation Checklist

- [x] Preserve the existing /read route.
- [x] Keep fixed/compressing top chrome and fixed bottom app navigation.
- [x] Give collapsed rows native Read-style rank and performance hierarchy.
- [x] Expand or collapse the selected row on tap.
- [x] Prevent post-thumbnail taps from collapsing the selected row.
- [x] Use 4:5 for all content post thumbnails and card-stack faces.
- [x] Preserve Post Mortem's staggered mask/left-exit motion.
- [x] Pass desktop, mobile, interaction, console, lint, type, and production-build checks.

## Follow-up Polish

- [P3] Repeat the source-sized capture with the authenticated production feed data before final Lead-tab wiring.

final result: passed
