/* ─────────────────────────────────────────────────────────────
   FLY — the page's own pieces of motion that are not the Feed's:
   the page carried by one transform while its scroll is set once,
   and a floor under its height while it glides (the browser would
   clamp the scroll mid-flight).
   ───────────────────────────────────────────────────────────── */

import { TRAVEL_EASE } from './Feed';

/* the page may not get shorter than this while it glides */
export function holdHeight(el: HTMLElement | null, px: number) {
  const v = `${Math.round(px)}px`;
  if (el && el.style.minHeight !== v) el.style.minHeight = v;
}

/* the scroll is set at once, and the page is carried from where it stood to its new place by one transform over the
   same glide the tiles use (their flights are written in the same frame, so the two move as one) */
export function carryPage(root: HTMLElement, dy: number, dur: number) {
  if (Math.abs(dy) < 0.5 || !dur) return null;
  return root.animate([{ transform: `translate3d(0, ${dy}px, 0)` }, { transform: 'translate3d(0, 0, 0)' }], { duration: dur, easing: TRAVEL_EASE, fill: 'backwards' });
}
