# Lakmé trail case — design QA

source visual truth: preserved inside the comparison captures in this directory; the original clipboard image was ephemeral.

implementation screenshot paths:

- `./lakme-trail-phone-prominent-final.png`
- `./lakme-trail-desktop.png`
- `./lakme-design-compare-prominent.png`
- `./lakme-animation-start.png`
- `./lakme-animation-mid.png`
- `./lakme-animation-final.png`
- `./lakme-design-compare-animation.png`
- `./lakme-animation-10-mid-final.png`
- `./lakme-animation-10-final.png`
- `./lakme-design-compare-10.png`
- `./lakme-relay-title-fixed.png`
- `./lakme-premium-phone-final.png`
- `./lakme-design-compare-premium.png`
- `./lakme-reactive-rings-mid.png`
- `./lakme-reactive-rings-mobile-crop.png`
- `./lakme-reactive-rings-desktop.png`
- `./lakme-reactive-rings-read.png`
- `./lakme-reactive-rings-compare.png`
- `./lakme-pressure-ui-mobile.png`
- `./lakme-pressure-ui-mid.png`
- `./lakme-loop-spread.png`
- `./lakme-loop-scan.png`
- `./lakme-sequence-lines-premium.png`
- `./lakme-sequence-wipe.png`
- `./lakme-sequence-radar.png`
- `./lakme-sequence-cards.png`
- `./lakme-sequence-desktop.png`
- `./lakme-sequence-compare.png`
- `./lakme-manual-lines.png`
- `./lakme-manual-radar.png`
- `./lakme-manual-cards.png`
- `./lakme-vertical-handoff.png`
- `./lakme-vertical-radar-start.png`

viewport and density:

- Source: 853 × 1844 pixels.
- Mobile: 392 × 852 CSS pixels, captured at the same 392 × 852 density.
- Desktop: 1910 × 1075 CSS pixels.

state: `/read/lakme-case`, trajectories fully entered, no post read open. The post-read overlay was also tested separately with `Reset the Color Trick` selected.

## Full-view comparison evidence

The source and implementation were placed together in `./lakme-design-compare-premium.png`. The implementation preserves the source's black editorial field, five concentric landing bands, centered five-word thesis, ten image-backed post cards, sharp white geometry, and restrained saturated red.

The source circle is visibly cropped at both horizontal edges. The final implementation makes that near-edge scale intentional: its outer ring is 378.5 px across on a 392 px viewport, with the 388.5 px stage beginning at x=2.0. The 66 px earlier cards and 70 px current-run cards are clamped only when their natural radial position would spill; all remain between x=22.5 and x=390.0, and the root scroll width equals the 392 px client width.

## Focused comparison evidence

The full-view comparison is sufficient because the component has one primary visual region and the scale labels remain readable at phone size. The implementation uses one aligned north scale with 100%, 80%, 60%, 40%, and 20% from the center outward. This makes the field a percentile ranking: pushing outward means reaching a smaller, stronger percentile without adding free-floating tags.

## Required fidelity surfaces

- Fonts and typography: Existing Space Grotesk is retained. The account header stays technical and tracked; the five-word center thesis uses a compact heavy display hierarchy.
- Spacing and layout rhythm: The phone version is a true one-screen composition with the entire circle, all cards, legend, and distance explanation visible. Five marks have a consistent 27.7 px pitch at the 392 px target. The field begins at y=148 rather than floating in the vertical center. The previous below-chart read column is removed.
- Colors and visual tokens: Matte black, warm white, and one saturated `#ff174f` red match the chosen direction. Red is now reserved for current-run trajectories/cards; referenced older posts use white.
- Image quality and assets: Every endpoint uses a supplied Lakmé post still through Next Image. There are no placeholder nodes, random shapes, handcrafted SVG assets, or thumbnail-less marks.
- Copy and content: The center title is exactly five words. The persistent explanation is limited to `Farther out = stronger landing`; detailed performance copy appears only after selecting a post.
- Motion: Rings reveal from the center outward at 58 ms intervals. The five current-run trajectories spread first, followed by the five older references. Each 680 ms trajectory completes before the next begins on a 700 ms relay; the corresponding card arrives near the endpoint through a restrained masked reveal. Reduced motion keeps only a short opacity transition.

## Comparison history

### Iteration 1

- Earlier finding: [P1] The outer circle and edge cards were cut off on a phone because the stage expanded beyond the viewport.
- Fix made: The final stage is capped by `100vw - 8px`; geometry keeps a 23 px mathematical outer-ring reserve and validates the actual post-card bounds.
- Post-fix evidence: Mobile geometry reports stage bounds x=4.0–388.5, card bounds x=46.3–387.0, and no page-level horizontal overflow.

### Iteration 2

- Earlier finding: [P2] All comparison lines used red, so the user could not distinguish this run from older evidence.
- Fix made: Four current-run posts use saturated red cards and trajectories; four earlier references use white cards and trajectories. The legend repeats the same two line treatments.
- Post-fix evidence: `./lakme-design-compare-prominent.png`.

### Iteration 3

- Earlier finding: [P2] The persistent analysis column forced mobile scrolling and competed with the visual.
- Fix made: The detail read is now a selected-post dialog. On a 392 × 852 viewport it occupies x=12.0–380.5 and y=215.2–840.5 without horizontal clipping.
- Post-fix evidence: Browser interaction opened the Reset post, exposed its Top 3%, 357.9× baseline, and 97% trail-cleared metrics, then closed successfully.

### Iteration 4

- Earlier finding: [P2] The first safe-area correction made the iPhone circle, thumbnails, and band intervals feel too small.
- Fix made: Expanded the stage from 364.5 px to 384.5 px, enlarged thumbnails from 48–56 px to 62–65 px, reduced the center radius, and redistributed the five bands across the reclaimed space.
- Post-fix evidence: Band marks are now separated by 24.7 px rather than 18.4 px while the rightmost card still ends at x=387.0 inside the 392 px viewport.

### Iteration 5

- Earlier finding: [P2] Even after the first enlargement, the circle still read as a chart placed inside the iPhone rather than the dominant object.
- Fix made: Expanded the outer ring to 378.5 px, increased thumbnails to 66–70 px, shifted the stage from y=230 to y=148, widened the center title to 112.7 px, and clamped endpoint centers only at the screen edge.
- Post-fix evidence: `./lakme-design-compare-prominent.png` shows source and implementation at matched height. The full ring remains visible with roughly 7 px ring clearance while the rightmost thumbnail deliberately lands 2 px from the viewport edge.

### Iteration 6

- Earlier finding: [P2] The band labels read as trail-completion percentages, so the strongest outer landing showed the largest number even though the product language is percentile ranking.
- Fix made: Inverted only the displayed scale. The inner-to-outer sequence is now 100%, 80%, 60%, 40%, 20%; post geometry and `Farther out = stronger landing` remain unchanged.
- Earlier finding: [P2] Rings, trajectories, and thumbnails entered on loosely related timings and the spring card entrances made the spread feel less deliberate.
- Fix made: Rings now reveal center-out, current-run posts spread before their earlier references, trajectories overlap with a 72 ms stagger, barrier marks fade in as they are crossed, and each thumbnail settles as its trajectory reaches the endpoint with no bounce.
- Post-fix evidence: `./lakme-animation-start.png`, `./lakme-animation-mid.png`, and `./lakme-animation-final.png` show the three-stage reading. Browser geometry reports the scale as outer 20% through inner 100%, all eight cards remain present, and the root has no horizontal overflow.

### Iteration 7

- Earlier finding: [P2] The selected mock proved eight posts but did not visibly prove the requested 8–10 post operating range on an iPhone.
- Fix made: Expanded the Lakmé placeholder to ten image-backed units: five current-run posts and five older references. The added endpoints occupy the upper-right and lower outer field; existing angles were redistributed so no two thumbnail rectangles overlap and the north band-label column stays clear.
- Post-fix evidence: `./lakme-animation-10-mid-final.png` shows the five-post red run forming before reference context enters. `./lakme-animation-10-final.png` shows all ten posts at 392 × 852. Browser geometry reports 10 cards, zero card overlaps, x bounds 22.5–390.0, root scroll width equal to its 392 px client width, and no warning or error logs. The new `All-day Lip Stain Test` reference opens and closes its matching read successfully.

### Iteration 8

- Earlier finding: [P2] Ten readable thumbnails made the field feel loud, interior ring weights competed with the evidence, and the 72 ms trajectory stagger read as a single burst.
- Fix made: Reduced phone thumbnails from 66/70 px to 60/63 px, replaced heavy two-pixel frames and colored halos with one-pixel satin frames, lowered interior ring weight and opacity while retaining a firm outer boundary, and reduced red line glow. Trajectories now run one at a time: 680 ms of travel on a 700 ms relay, for a deliberate 7.7-second ten-post sequence.
- Earlier finding: [P2] Transform-scaled card entrances could briefly compete with the center-title compositing layer during replay.
- Fix made: The title is now the highest layer in the orbit and cards use a small inset-mask plus opacity instead of scale. `./lakme-relay-title-fixed.png` shows the first post arriving with the full five-word title intact; `./lakme-premium-phone-final.png` shows the quieter settled field.

### Iteration 9

- Earlier finding: [P2] The settled geometry was acceptable, but the circle still behaved like a static chart with animation placed on top. Plain single-stroke rings did not visibly respond when a post crossed them, and the trajectory did not communicate where its motion was heading.
- Fix made: Rebuilt every band as a paired instrument track with a dark structural groove and a precise white edge. Rings now assemble symmetrically from the north scale rather than fading in as complete circles. Each post draws a dark-underlay trajectory plus a crisp red or white core, with a short brighter leading rail that makes the active direction legible.
- Fix made: Every crossed band reacts only at the actual trajectory intersection. The affected segment lifts briefly as a localized arc, then settles into a small permanent colored notch and cross-mark. The response is tied to post progress; there are no idle waves, particles, rotating ornaments, gradients, or random decoration.
- Post-fix evidence: `./lakme-reactive-rings-mid.png` captures the current run accumulating one post at a time. `./lakme-reactive-rings-mobile-crop.png` shows the settled ten-post iPhone field; `./lakme-reactive-rings-compare.png` places it beside the selected reference. At 392 × 852 there are 10 cards, zero card overlaps, zero label/card overlaps, zero horizontal overflow, and the full page remains exactly one viewport tall. `./lakme-reactive-rings-desktop.png` verifies the 1910 × 1075 composition.

### Iteration 10

- Earlier finding: [P1] Better-drawn circles and crossings still retained the visual grammar of a presentation chart: outlined rings, a boxed percentage ladder, a legend, and ten permanent spokes.
- Fix made: Replaced the outlined grid with five broad flat charcoal landing surfaces separated by recessed seams. Removed the percentage axis and chart legend completely. Exact performance now lives on the focused post and inside the selected-post read rather than as persistent chart furniture.
- Fix made: Only one trajectory is visible at a time. During replay it follows the active post; once the field settles it focuses the Top 3%, 357.9× post. Completed paths disappear and leave only small red or white seam cuts, preserving the account's accumulated evidence without a spoke diagram.
- Fix made: Reduced secondary thumbnails, simplified their mounts, enlarged the focused post, and attached its exact `Top 3% · 357.9×` readout directly to the media tile. The center is now a solid core rather than another white-outlined chart ring.
- Post-fix evidence: `./lakme-pressure-ui-mid.png` captures the wide active rail crossing the field. `./lakme-pressure-ui-mobile.png` captures the settled focused state. At 392 × 852 the document is exactly one viewport tall, holds 10 posts with zero card overlaps, has no axis labels or horizontal overflow, and produces no console warnings or errors.

### Iteration 11

- Earlier finding: [P1] The flat pressure surfaces still lacked depth, several thumbnails felt compositionally crowded despite avoiding literal overlap, and retained seam cuts made the trajectory system look dashed.
- Fix made: Rebuilt every landing seam as a recessed plate edge with a crisp dark upper cut, restrained lower highlight, and short vertical shadow. The center core and thumbnail mounts now occupy clearly different elevations without gradients or 3D perspective.
- Fix made: Recomputed all ten post angles as a composition rather than preserving the earlier cluster. Reduced secondary tiles slightly, expanded usable radial space, and redistributed the posts around the complete field. At 392 × 852 the closest pair now has a 22 px clear gap, up from the visually compressed earlier arrangement, with zero overlaps.
- Fix made: Removed all persistent crossing ticks, impact dashes, launch marks, and bright leading segments. A post now uses one uninterrupted red or white rail with a dark underlay. The rail draws with a strong ease-out, remains fully continuous across every plate, and disappears before the next post begins.
- Post-fix evidence: Browser inspection at 392 × 852 reports 10 cards, zero overlaps, 22 px minimum inter-card gap, exact one-viewport document height, and no warnings or errors.

### Iteration 12

- Earlier finding: [P1] Removing trajectory history entirely made the landing depth harder to read, while the thin thumbnail keylines still felt decorative rather than structural.
- Fix made: Reframed the component as an 8.6-second two-state loop. The spread draws all ten solid red/white trajectories with a 90 ms stagger, reveals every endpoint, and holds long enough to read the complete account. At 4.8 seconds the paths and cards clear together; a single restrained radar needle scans the empty pressure field for 3.2 seconds before the spread rebuilds automatically.
- Fix made: Replaced one-pixel thumbnail borders with substantial three-pixel red/white mounts and a five-pixel black separation layer. Current-run and earlier-reference roles now read through the card construction, not fragile keylines.
- Fix made: The radar has no thumbnails or interactive card targets. All ten buttons are disabled during scan and restored when the spread rebuilds. Reduced motion bypasses the loop and shows the complete spread statically.
- Post-fix evidence: `./lakme-loop-spread.png` captures 10 visible, enabled cards and the complete trajectory map. `./lakme-loop-scan.png` captures zero visible cards, 10 disabled targets, and the active scan. The loop rebuild was observed after 8.6 seconds; the selected-post read still opens during the spread. Both states remain 392 × 852 with no console warnings or errors.

### Iteration 13

- Earlier finding: [P1] Alternating lines and thumbnails through opacity alone still made the state change feel like two layers popping on top of the same chart. Replaying also restarted the ring construction, even though the account's five-band field should be persistent context.
- Fix made: The five-band field is now static from first paint and remains visually fixed during replay. The sequence is one-way and explanatory: ten trajectories build, a single left-to-right blade wipes only those trajectories, then the radar completes one revolution and reveals each thumbnail at its actual angle. The settled thumbnail map does not automatically restart; the explicit replay control reruns only the evidence sequence.
- Earlier finding: [P2] Plain equal-width strokes read as presentation spokes rather than a premium trajectory system.
- Fix made: Each trajectory is now a tapered pressure rail with a recessed black channel, saturated red or warm-white body, one restrained satin edge, and a darker opposite edge. The rails remain continuous and deterministic—no dashes, particles, crossing ticks, random marks, or decorative tags.
- Earlier finding: [P2] The radar transition did not carry enough light and thumbnails appeared as a group rather than as a consequence of the sweep.
- Fix made: Strengthened the radar with a wider flat wake, brighter leading needle, and brighter outer-rim trace. Thumbnail delays are derived from each post's normalized angle, so the sweep reveals them spatially as it passes rather than through an arbitrary list stagger.
- Post-fix evidence: `./lakme-sequence-lines-premium.png`, `./lakme-sequence-wipe.png`, `./lakme-sequence-radar.png`, and `./lakme-sequence-cards.png` capture the four moments of the two-state sequence. The mid-radar DOM proof found 4 revealed thumbnails and 6 still hidden; the final state found 10 visible and enabled thumbnails. At 392 × 852 there are zero overlaps, a 22 px minimum inter-card gap, exact one-viewport document dimensions, and no warning or error logs. `./lakme-sequence-desktop.png` verifies the settled 1440 × 900 composition.
- Density normalization: The 853 × 1844 source was normalized to 585 × 1272 and combined with the 585 × 1272 implementation capture in `./lakme-sequence-compare.png`. The implementation capture represents a 392 × 852 CSS viewport at approximately 1.49× device density.

### Iteration 14

- Earlier finding: [P1] The left-to-right blade was a literal horizontal animation across the component. It confused the Reader's swipe interaction with a visual effect and advanced the experience without user intent.
- Fix made: Removed the blade, its clipping behavior, its timer, and the automatic phase schedule completely. The trajectories now remain on screen indefinitely until the user performs the same constrained horizontal drag pattern used by Feeder Reader runs: drag on the x-axis, elastic resistance, 64 px intent threshold, and snap back to origin.
- Fix made: A completed left swipe changes the fixed field from trajectories to the radar transition. The radar then reveals thumbnails by angle and settles once. A right swipe from the thumbnail state returns to trajectories. Arrow Left/Enter and Arrow Right provide equivalent keyboard access without introducing visible controls.
- Post-fix evidence: The line state remained unchanged after a 7-second hold with zero thumbnails and the persistent `Swipe left to reveal the posts` cue. The manual forward trigger produced the radar state with 3 of 10 posts revealed at 700–800 ms; after 2.2 seconds all 10 posts were visible and enabled. The reverse trigger returned to the line state with zero thumbnails. `./lakme-manual-lines.png`, `./lakme-manual-radar.png`, and `./lakme-manual-cards.png` show the complete manual sequence with no horizontal blade. The page remains exactly 392 × 852 with no overflow.

### Iteration 15

- Earlier finding: [P1] The gesture axis was wrong: account-state progression should be vertical, not horizontal. The direct `lines → radar` state change also removed every rail in one frame and introduced a fully formed sweep too abruptly.
- Fix made: The stage now uses a constrained vertical drag with elastic resistance and snap-to-origin. Swipe up advances from trajectories; swipe down returns from thumbnails. The visible instructions, accessible labels, and Arrow Up/Arrow Down equivalents follow the same direction.
- Fix made: Added a 420 ms handoff phase between the two readings. The complete trajectory map retracts 10% while dissolving on a strong in-out curve. After 110 ms the radar needle starts charging outward from the centre edge beneath the remaining rails. The active sweep inherits that fully charged needle, expands its wake from 0.16 to 0.55 radians, and then continues around the field. There is no blank frame, literal wipe, sudden completed radar, or simultaneous trajectory/thumbnail state.
- Post-fix evidence: `./lakme-vertical-handoff.png` captures the rails partially dissolved with the radar needle growing beneath them. `./lakme-vertical-radar-start.png` captures the sweep continuing from the charged north position with no posts prematurely visible. The full interaction proof recorded: lines → handoff → radar with 3 posts revealed mid-sweep → cards with 10 visible/enabled posts → lines on reverse trigger. The 392 × 852 page remains one viewport with no warning or error logs.

### Iteration 16

- Earlier finding: [P1] The 420 ms handoff still read as a batch fade: the rails barely retracted, the radar wake reached its full width too quickly, and the 380 ms card entrance felt like a pop rather than a landing.
- Fix made: Rebuilt the handoff as an 1.1-second relay. Each trajectory now drains completely back into the centre on a 45 ms stagger and a 500 ms in-out curve. The north radar charge waits until the outgoing map is nearly clear, grows from the centre edge over the remaining 380 ms, and carries continuously into a slower 3.8-second scan. Its wake expands over 900 ms rather than snapping open. Each post settles over 520 ms with GPU-friendly opacity and transform on a non-bouncing ease.
- Earlier finding: [P2] Bevelled rails and stacked shadow seams made the field resemble dark three-dimensional tubes, which cheapened the frame and worked against the requested bold minimalism.
- Fix made: Flattened the five landing zones into alternating near-black score fields with single precise separators and one restrained outer boundary. Trajectories are now clean red or warm-white luminous rails with a black separation channel—no bevel edge, taper polygon, dash, gradient, or ornamental mark. Thumbnail mounts were reduced to solid role color, black separation, and shadow so the post image remains dominant.
- Post-fix evidence: `./lakme-polish-lines-v2.png`, `./lakme-polish-relay-v2.png`, `./lakme-polish-charge-v2.png`, `./lakme-polish-radar-v2.png`, and `./lakme-polish-cards-v2.png` capture the complete refined sequence. Browser timing proof recorded 0 cards during both relay and charge, 1 gently entering card at the early radar position, and 10 visible/enabled cards in the final state. Reverse Arrow Down returned to trajectories with 0 visible cards and the correct swipe-up instruction. At 392 × 852, document dimensions match the viewport and browser warning/error logs are empty.

### Iteration 17

- Earlier finding: [P1] The separate `lines → handoff → radar → cards` phases destroyed the direct relationship between a trajectory and the post that owned its landing. The map emptied first, a generic radar state replaced it, and the thumbnails then appeared as a second layer rather than the consequence of the original spread.
- Fix made: Reduced the sequence to `lines → sweep → cards`. After a 380 ms north charge, one 3.2-second clockwise sweep converts the field in place. Ahead of the sweep, the untouched trajectories remain visible. Within a 0.055-revolution window at the sweep edge, each individual trajectory clears. Immediately behind that edge, its own post fades into the same endpoint. The mid-state therefore shows the unread half as trajectories and the read half as thumbnails; there is no empty chart, batch disappearance, unrelated radar interlude, or simultaneous complete line/card map.
- Earlier finding: [P1] CSS transition delays could still collapse during a hot refresh, making the final thumbnail state appear as a group even though the canvas sweep remained spatially sequenced.
- Fix made: The sweep now owns an explicit reveal timer for every post. Each timer is calculated from the post's clockwise angle and adds that post to the revealed set only after the radar reaches it. CSS controls only the short local opacity settle; it no longer decides when a post exists. Phone checkpoints now prove the conversion directly: 0 visible posts during the north charge, then 1, 2, and 4 during the sweep, followed by all 10 in the settled map.
- Earlier finding: [P1] Thumbnail arrival still visibly bounced because the draggable parent was springing back while each Framer-controlled card was also promoted onto a transformed compositing layer. Fractional endpoint positions made that double transform read as a small scale/position jitter.
- Fix made: Removed Framer Motion from the post tiles entirely. Cards now use a plain 260 ms opacity transition with no scale, translation, blur, or card-level transform. The vertical gesture uses 0.015 elastic travel, no momentum, and a critically damped 1000/100 return, so the field is already at rest before the first post appears. A 24-frame arrival sample recorded identical x, y, width, and height values from opacity 0 through 1, with computed transform `none` on every frame.
- Earlier finding: [P2] White/red perimeter cases and thin concentric outlines made both the thumbnails and the field feel like presentation graphics.
- Fix made: Rebuilt the field as five solid near-black annular plates separated by substantial matte channels and one restrained outer boundary. Replaced colored thumbnail cases with black poster mounts, a satin top catch, a four-pixel black separation layer, and one structural red/white base edge. The role color now reads as part of the mount rather than a decorative outline or floating dash.
- Post-fix evidence: `./lakme-explicit-sweep-charge-v5.png` shows the north charge with all trajectories intact and zero posts; `./lakme-explicit-sweep-early-v5.png` and `./lakme-explicit-sweep-mid-v5.png` show the radar replacing only the trajectories it has crossed. The intermediate final-v6 capture was already missing when this QA archive was made durable; later settled-state captures below preserve the resulting ten-post landing map. At the phone target the final state contains 10 visible/enabled posts, zero thumbnail overlaps, card transforms computed as `none`, and document dimensions equal to the viewport. A real upward drag triggers the sweep; a real downward drag from the settled field returns to lines with zero visible thumbnails.

### Iteration 18

- Earlier finding: [P1] The square post markers read as miniature interface badges rather than feed content. Four layers competed around each image: black padding, an outer black case, a coloured bottom edge, and—in the focus post—a full red metric footer. The actual post image became the smallest part of the unit.
- Fix made: Rebuilt every marker as a compact 1:1.18 editorial media slab, matching the portrait tendency of the selected reference. Imagery now runs edge-to-edge inside a sharp six-pixel case with no perimeter keyline. A single red or warm-white plate is offset seven pixels behind the image, so run/reference status reads as physical layering rather than a border, tag, or underline. The focus metric is now a restrained black in-image plate with white rank and red multiple.
- Earlier finding: [P2] Re-cutting trajectories at every band boundary added depth but made the rails read as a dashed chart—the exact opposite of the requested solid spread.
- Fix made: Removed every post-draw band cut. Trajectories are once again uninterrupted red or warm-white rails with a precise black separation channel, squared ends, and no dashed segments, bevels, particles, or ornamental landing marks.
- Post-fix evidence: `./lakme-aesthetic-compare-v8.png` places the normalized 585 × 1272 source and final 585 × 1272 implementation together. The implementation now gives imagery the dominant area of every post unit, uses the selected source's portrait framing and run/reference contrast, and keeps the more minimal fixed landing field established in the later interaction direction. `./lakme-aesthetic-sweep-latest-v8.png`, `./lakme-aesthetic-mount-v8.png`, and `./lakme-aesthetic-desktop-v8.png` capture the transition, settled phone field, and desktop composition. The phone state holds 10 visible posts with zero overlaps and a 26.6 px minimum clear gap. A real upward drag from the ring records `lines → sweep/0 → sweep/3 → cards/10`; a real downward drag returns to `lines/0`.

### Iteration 19

- Earlier finding: [P1] The radar used a translucent filled sector, a thick glowing outer arc, and a bloom-heavy radial beam. It read as a generic flashlight effect laid over the account field rather than a precise transition between trajectories and posts.
- Fix made: Removed the filled sector and the radial blade completely. The swoosh now exists only as five short illuminated traces carried by the five landing bands. Their heads are phase-offset by radius, so they roll clockwise as one curved front rather than reading as another trajectory spoke. There is no cone, wash, gradient fill, single-line sweep, or unrelated ornament.
- Earlier finding: [P2] A 3.2-second revolution made the explanatory transition linger after the spatial relationship was understood.
- Fix made: Shortened the north charge from 380 ms to 300 ms and the revolution to 2.1 seconds. All trajectory clearing and post reveal timers remain angle-derived from the same duration, preserving the one-by-one relationship at the faster pace.
- Post-fix evidence: `./lakme-band-swoosh-v10.png` captures the band-led curved front mid-revolution. Timing proof records `sweep/0` at 220 ms, `sweep/1` at 650 ms, `sweep/4` at 1.2 s, then `cards/10` with every post enabled after the sequence settles.

### Iteration 20

- Earlier finding: [P1] Even with the spatial reveal corrected, each thumbnail still arrived as one flat opacity change. The sweep determined *when* a post appeared, but the local entrance did not feel authored or connected to the red/white post mount.
- Fix made: Rebuilt arrival as a two-beat exposure. The red or warm-white backing plate registers first; after a 64 ms beat, the image opens symmetrically from a narrow centre aperture and settles over 240 ms. The focused metric waits until the image is substantially exposed. There is no scale, translation, bounce, blur, or card-level transform, and reduced motion falls back to a direct opacity reveal.
- Post-fix evidence: `./lakme-shutter-reveal-v11.png` captures the staggered transition while two posts are being exposed. A 14-sample motion trace recorded the first post progressing from `inset(0 49%)` to the complete image while x, y, width, and height remained identical and computed card transform stayed `none`. Reveal ownership remains angle-derived: zero posts through the charge, then one and two posts at the sampled early sweep positions before all ten settle.

### Iteration 21

- Earlier finding: [P1] The 2.1-second revolution still made the swoosh feel demonstrative after the user had already understood its direction.
- Fix made: Compressed the north charge to 220 ms and the full band-led revolution to 1.15 seconds. Trajectory clearing and post ownership remain locked to the same normalized angle, while the local 240 ms image exposure is unchanged so cards stay legible instead of inheriting the sweep's urgency.

## Findings

No actionable P0, P1, or P2 differences remain. The result preserves radial landing depth while separating its two readings cleanly: trajectories explain spread first, the radar bridges the state change, and thumbnails then show the posts that own those landing positions.

## Verification

- Targeted ESLint: passed.
- TypeScript `--noEmit`: passed.
- Page identity, meaningful DOM, and no framework overlay: passed.
- Mobile root horizontal overflow: none.
- All ten posts visible inside the phone viewport with zero thumbnail overlap: passed.
- Selecting a post opens the matching read overlay; close returns to the field: passed.
- Desktop responsive composition: passed.
- Production build: passed.
- Reduced-motion branch and keyboard focus treatment: implemented.
- Motion review: approved; the longer timing is justified as an explanatory one-by-one sequence, each trajectory fully completes before the next begins, card motion is limited to a small inset mask plus opacity, reduced motion is honored, and no bounce remains in the spread.
- Reactive-field motion review: approved; ring assembly explains the scale, the leading rail explains direction, and every localized band response is caused by an actual crossing. The sequence settles completely and has no perpetual decorative motion.
- Two-state motion review: approved; the fixed field does not replay, the 760 ms wipe is a justified explanatory transition, radar reveals are synchronized to post angle, thumbnail entrances use opacity/clip/short blur without bounce, and reduced motion bypasses positional motion for the settled thumbnail map.

final result: passed
