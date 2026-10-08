/* ─────────────────────────────────────────────────────────────
   PAGE ARRIVAL — a tab coming back on screen arrives piece by
   piece, never as one block: every [data-arrive] piece that is
   on screen rises into place on its own, in reading order, on the
   page beat (lib/motion), while the header adapts above it.

   Only transform and opacity; only what is on screen; fill
   "backwards" holds each piece hidden until its turn and leaves
   no style behind. Returns how many pieces it moved (none: the
   caller can fall back to its own arrival).
   ───────────────────────────────────────────────────────────── */

import { BEAT_ARRIVE_S, BEAT_EASE_CSS, beatDelay } from '@/lib/motion';

const RISE_PX = 14;
const ARRIVAL_ID = 'tab-arrival';

export function arrivePieces(root: HTMLElement | null): number {
  if (!root || typeof window === 'undefined') return 0;
  if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return 0;
  const height = window.innerHeight;
  // every read first, every write after
  const pieces = Array.from(root.querySelectorAll<HTMLElement>('[data-arrive]'))
    .map((el) => ({ el, rect: el.getBoundingClientRect() }))
    .filter(({ rect }) => rect.width > 0 && rect.height > 0 && rect.bottom > 0 && rect.top < height)
    .sort((a, b) => a.rect.top - b.rect.top || a.rect.left - b.rect.left);
  pieces.forEach(({ el }, step) => {
    // an arrival replaces any still playing (a quick switch back, or an effect run twice)
    el.getAnimations().forEach((animation) => {
      if (animation.id === ARRIVAL_ID) animation.cancel();
    });
    el.animate(
      [{ opacity: 0, transform: `translateY(${RISE_PX}px)` }, { opacity: 1, transform: 'none' }],
      { id: ARRIVAL_ID, duration: BEAT_ARRIVE_S * 1000, delay: beatDelay(step) * 1000, easing: BEAT_EASE_CSS, fill: 'backwards' },
    );
  });
  return pieces.length;
}
