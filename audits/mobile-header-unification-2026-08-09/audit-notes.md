# Mobile Header Unification — Analysis and Plan

## Scope

- Compare the mobile Lead header with the mobile individual Feed dashboard header.
- Apply the recommendation to mobile web and installed PWA only.
- Exclude desktop, the Feed homepage/list header, and the Feeder Reader subview.

## Evidence

- Lead expanded mobile chrome: 152px; content begins at roughly 172px.
- Individual Feed expanded mobile chrome: 222px; dashboard content offset: 260px.
- Feed therefore reserves 70px more chrome and 88px more space before content. On an 844px-tall phone, the content offset grows from about 20% of the viewport to about 31%.
- Feed spends that space on a persistent 40px four-option timeframe control plus a minimum 94px feeder rail. Lead puts timeframe selection in the top row and uses 60/64px mobile circles.

## Recommendation

Use Lead's two-level mobile structure for the individual Feed dashboard:

1. A single persistent 44px top row containing back, feed/scope title, compact `Top 55%` status, and a compact `30D` timeframe trigger.
2. A horizontally scrolling feeder rail below it, using Lead's 60px inactive / 64px active mobile circle sizing and 70px item width.

Tapping the timeframe trigger opens the other ranges; the four options no longer occupy a permanent row. When the page compresses on scroll, only the feeder rail hides. The same top row remains visible, so back, scope, performance, and timeframe do not morph into a different information layout.

## Implementation Plan

1. Extract Lead's compact mobile timeframe picker into one shared mobile-only control. Keep Lead's existing appearance and keyboard/focus behavior unchanged, and use it from Feed. Leave both desktop segmented controls untouched.
2. Replace Feed's mobile four-option timeframe row and dual expanded/collapsed title layers with one stable top row. Keep the existing data and handlers for selected feed, selected feeder, timeframe, top percentile, and post count.
3. Give Feed's mobile feeder buttons Lead's dimensions: 70px item width, 60px inactive circle, 64px active circle, and 9px label. Preserve Feed's full-feed stack, feeder images, anchor badge, active ring, horizontal scrolling, and haptics. Retain the current 82px / 70–76px sizing at desktop widths.
4. Align only the individual Feed dashboard's mobile height contract with Lead: target 152px expanded chrome, 68px compressed chrome, and approximately 172px content offset including the existing safe-area treatment. Do not change the Feed homepage constants or Reader header constants.
5. Match Lead's scroll behavior: use a single persistent top row, fade/clip the feeder rail, preserve a fixed content offset during the animation, and use the existing reduced-motion path.

## Acceptance Checks

- At 360px, 390px, and 430px widths, the title truncates cleanly and no top-row control overlaps.
- At the top of the page, the first dashboard card starts about 88px higher than today.
- Back, `Top %`, timeframe, and current scope remain visible in the compressed state.
- Timeframe changes still refresh the dashboard and `Top %`; feeder changes still update the scope and active ring.
- Every interactive control has at least a 44px hit target, visible keyboard focus, correct labels, Escape-to-close behavior, and a reduced-motion fallback.
- Verify Safari/Chrome mobile web and standalone PWA safe areas, dynamic browser chrome, scroll collapse/expand, dark mode, and route return to the Feed homepage.
- Confirm no visual change at `lg`/desktop, on the Feed homepage/list, or in the Feeder Reader subview.

## Files Expected to Change

- `apps/web/src/components/tabs/FeedTab.tsx`
- `apps/web/src/app/lead/page.tsx` (shared picker extraction only; no intended visual change)
- One small shared mobile timeframe-picker component
- `apps/web/src/app/globals.css` only if a scoped Feed-detail compressed-height variable cannot remain local to `FeedTab.tsx`
