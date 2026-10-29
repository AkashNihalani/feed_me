/* ─────────────────────────────────────────────────────────────
   THE READ'S LAYER MOTION — the Feeder File opens out of what was
   tapped (a story's "On the map", its card) and closes back into
   it, in one move, the way an iPhone app opens out of its icon. The
   layer is a frame that grows from that rect to the screen (and
   shrinks back), its corners rounding as the button's; what's in it
   scales with it evenly (never squashed), centred in the frame, the
   frame cropping what's wider or taller than it. It fades in as it
   leaves the button and out as it lands in it, so the two hand over
   to each other. Transforms and opacity only, sampled on the curve
   and played linear, so the frame and what's in it stay locked
   together (Safari runs them off the main thread).
   ───────────────────────────────────────────────────────────── */

import { ms } from '@/lib/tempo';

export type Box = { left: number; top: number; width: number; height: number };

function bezier(x1: number, y1: number, x2: number, y2: number) {
  const cx = 3 * x1, bx = 3 * (x2 - x1) - cx, ax = 1 - cx - bx;
  const cy = 3 * y1, by = 3 * (y2 - y1) - cy, ay = 1 - cy - by;
  const sx = (t: number) => ((ax * t + bx) * t + cx) * t;
  const sy = (t: number) => ((ay * t + by) * t + cy) * t;
  const dx = (t: number) => (3 * ax * t + 2 * bx) * t + cx;
  return (x: number) => {
    let t = x;
    for (let k = 0; k < 8; k++) {
      const e = sx(t) - x;
      const d = dx(t);
      if (Math.abs(e) < 1e-5 || Math.abs(d) < 1e-6) break;
      t -= e / d;
    }
    return sy(Math.min(1, Math.max(0, t)));
  };
}
// (eased in and out, so the zoom is seen growing: a fast-out curve would pop it to full and then crawl)
const CURVE = bezier(0.4, 0, 0.2, 1);
const STEPS = 24;
const f = (v: number) => v.toFixed(3);

/* the keyframes of a move of the frame from rect A to rect B (P is the frame's own rect at rest, the screen), its
   on-screen corner radius going from rA to rB. The frame scales to its rect (its shape); what's in it is scaled back to
   an even scale (the larger of the frame's two), centred on the frame */
function moveKF(A: Box, B: Box, P: Box, rA: number, rB: number) {
  const frame: Keyframe[] = [];
  const inner: Keyframe[] = [];
  const cx = P.left + P.width / 2;
  const cy = P.top + P.height / 2;
  for (let k = 0; k <= STEPS; k++) {
    const u = k / STEPS;
    const c = CURVE(u);
    const w = A.width + (B.width - A.width) * c;
    const h = A.height + (B.height - A.height) * c;
    const x = A.left + (B.left - A.left) * c - P.left;
    const y = A.top + (B.top - A.top) * c - P.top;
    const sx = w / P.width;
    const sy = h / P.height;
    const s = Math.max(sx, sy);
    const r = Math.min(rA + (rB - rA) * c, w / 2, h / 2);
    // (the content's centre on the frame's: p → (frame centre) + s·(p − screen centre), undone by the frame's own move)
    const tx = (P.left + x + w / 2 - s * cx - x) / sx;
    const ty = (P.top + y + h / 2 - s * cy - y) / sy;
    frame.push({ offset: u, transform: `translate(${f(x)}px, ${f(y)}px) scale(${sx.toFixed(5)}, ${sy.toFixed(5)})`, borderRadius: `${f(r / sx)}px / ${f(r / sy)}px` });
    inner.push({ offset: u, transform: `translate(${f(tx)}px, ${f(ty)}px) scale(${(s / sx).toFixed(5)}, ${(s / sy).toFixed(5)})` });
  }
  return { frame, inner };
}

const screen = (): Box => ({ left: 0, top: 0, width: window.innerWidth, height: window.innerHeight });
// where it opens from (or closes into): what was tapped, if it's on screen; else a box a little in from the screen's edges
export function originBox(from: (() => DOMRect | null) | null): Box {
  const P = screen();
  const r = from?.() ?? null;
  if (r && r.width > 8 && r.height > 8 && r.bottom > 0 && r.top < P.height && r.right > 0 && r.left < P.width) {
    return { left: r.left, top: r.top, width: r.width, height: r.height };
  }
  return { left: P.width * 0.06, top: P.height * 0.08, width: P.width * 0.88, height: P.height * 0.84 };
}

const reduced = () => window.matchMedia('(prefers-reduced-motion: reduce)').matches;

/* open: the frame grows from the origin to the screen as it fades in */
export function openLayer(frame: HTMLElement, inner: HTMLElement, from: Box, radius = 24): Animation | null {
  frame.style.visibility = '';
  if (reduced()) return null;
  const P = screen();
  const K = moveKF(from, P, P, radius, 0);
  const o: KeyframeAnimationOptions = { duration: ms(430), easing: 'linear' };
  const a = frame.animate(K.frame, o);
  inner.animate(K.inner, o);
  frame.animate([{ opacity: 0 }, { opacity: 1, offset: 0.3 }, { opacity: 1 }], { duration: ms(430), easing: 'linear' });
  return a;
}

/* close: it shrinks back into the origin, fading as it lands; then() once it's gone (it stays at its end until then) */
export function closeLayer(frame: HTMLElement, inner: HTMLElement, into: Box, then: () => void, radius = 24) {
  if (reduced()) { then(); return; }
  frame.getAnimations().forEach((a) => a.cancel());
  inner.getAnimations().forEach((a) => a.cancel());
  const P = screen();
  const K = moveKF(P, into, P, 0, radius);
  const o: KeyframeAnimationOptions = { duration: ms(400), easing: 'linear', fill: 'forwards' };
  const a = frame.animate(K.frame, o);
  inner.animate(K.inner, o);
  frame.animate([{ opacity: 1 }, { opacity: 1, offset: 0.62 }, { opacity: 0 }], { duration: ms(400), easing: 'linear', fill: 'forwards' });
  // (it's hidden before it's let go: let go first, it would stand at full size for a frame before the layer is put away)
  const end = () => {
    frame.style.visibility = 'hidden';
    then();
    frame.getAnimations().forEach((x) => x.cancel());
    inner.getAnimations().forEach((x) => x.cancel());
  };
  a.finished.then(end, end);
}
