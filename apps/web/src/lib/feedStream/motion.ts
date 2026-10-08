/* ─────────────────────────────────────────────────────────────
   FEED MOTION — how the Feed moves between its views (a zoom, a
   new pick or order). The posts are the same elements in every
   view (one element per post, its picture drawn once), so a move
   is never a copy: each piece is laid out where it now belongs and
   carried there, as a transform, from where it was (FLIP).

   Three moves, one vocabulary:
   carry  — a piece both views show travels from its old box to
            its new one (its picture keeping its own shape inside a
            box that changes shape);
   leave  — what has no place on the new screen holds where it was
            and goes: a short drift, a touch smaller, dissolving;
   enter  — what is new to the screen arrives where it now is: from
            a touch smaller, dissolving in, a ring at a time from
            the point the move centres on.

   All of it is transform and opacity on the compositor (WAAPI),
   sampled from one curve chosen for its evenness: it is moving
   from the first frame, it never jolts (its peak speed is 2.5× its
   average, the old spring's was 3.4× and came in the first tenth),
   and it lands without a long crawl. Everything starts together,
   in the frame the new view is first drawn, and nothing heavy runs
   while it moves. will-change only while moving.

   On WebKit a flight is held first (startFlight(hold)): WebKit runs
   the animations in another process and starts their clock in the
   frame that draws the new view, before that frame (every picture
   painted into its layer, the layers handed over) reaches the
   screen. A heavy frame ate the first third of every move, which
   arrived as a jump and then a settle. Held, every piece shows its
   start for that frame, and releaseFlight sets them all moving
   together once it is on screen: the whole glide is seen.
   ───────────────────────────────────────────────────────────── */

export type Rect = { left: number; top: number; width: number; height: number };

// how long a carry takes, and what leaves and arrives
export const CARRY_MS = 500;
export const LEAVE_MS = 320;
export const ENTER_MS = 340;
// a leaving piece stays whole this far into its time (what arrives comes in over it, so the screen never dims), then
// dissolves
const LEAVE_HOLD = 0.35;
// what arrives starts a beat in, a ring at a time out from the centre of the move
const ENTER_DELAY_MS = 30;
const RING_PX = 180;
const RING_STEP_MS = 16;
const RING_MAX = 6;
// a leaving piece shrinks to this and drifts this far (away from the centre of the move); an arriving one grows from it
const AWAY_SCALE = 0.94;
const DRIFT_PX = 28;
const SAMPLES = 24;

const LEAVE_EASE = 'cubic-bezier(0.3, 0, 0.6, 1)';
const QUICK_LEAVE_MS = 200;
const QUICK_LEAVE_EASE = 'cubic-bezier(0.4, 0, 1, 1)';
const ENTER_EASE = 'cubic-bezier(0.2, 0.7, 0.2, 1)';

/* The carry's curve (cubic-bezier 0.3, 0.3, 0.2, 1), solved here so its samples can drive a picture's counter-scale
   too (the counter-scale is not itself a bezier) */
function bezier(x1: number, y1: number, x2: number, y2: number): (x: number) => number {
  const cx = 3 * x1;
  const bx = 3 * (x2 - x1) - cx;
  const ax = 1 - cx - bx;
  const cy = 3 * y1;
  const by = 3 * (y2 - y1) - cy;
  const ay = 1 - cy - by;
  const sampleX = (s: number) => ((ax * s + bx) * s + cx) * s;
  const sampleY = (s: number) => ((ay * s + by) * s + cy) * s;
  const slopeX = (s: number) => (3 * ax * s + 2 * bx) * s + cx;
  return (x: number) => {
    if (x <= 0) return 0;
    if (x >= 1) return 1;
    let s = x;
    for (let i = 0; i < 8; i += 1) {
      const error = sampleX(s) - x;
      if (Math.abs(error) < 1e-6) return sampleY(s);
      const slope = slopeX(s);
      if (Math.abs(slope) < 1e-6) break;
      s -= error / slope;
    }
    let lo = 0;
    let hi = 1;
    s = x;
    for (let i = 0; i < 40 && Math.abs(sampleX(s) - x) > 1e-6; i += 1) {
      if (sampleX(s) < x) lo = s;
      else hi = s;
      s = (lo + hi) / 2;
    }
    return sampleY(s);
  };
}

export const carryCurve = bezier(0.3, 0.3, 0.2, 1);
export const CARRY_EASE_CSS = 'cubic-bezier(0.3, 0.3, 0.2, 1)';

export function reducedMotion(): boolean {
  try {
    return typeof window !== 'undefined' && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  } catch {
    return false;
  }
}

export function rectOf(el: Element): Rect {
  const rect = el.getBoundingClientRect();
  return { left: rect.left, top: rect.top, width: rect.width, height: rect.height };
}

export function onScreen(rect: Rect, width: number, height: number, margin = 0): boolean {
  return rect.left + rect.width > -margin && rect.left < width + margin && rect.top + rect.height > -margin && rect.top < height + margin;
}

const centreOf = (rect: Rect) => ({ x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 });

/* ── a flight: every animation of one move, ended together ── */

// hold: how long its animations wait at their start, at most, for releaseFlight (0: they start at once)
export type Flight = { animations: Animation[]; cleanups: Array<() => void>; ended: boolean; hold: number };

export function startFlight(hold = 0): Flight {
  return { animations: [], cleanups: [], ended: false, hold };
}

// a held flight's pieces all set moving now, from their start (each keeps its own delay after it)
export function releaseFlight(flight: Flight) {
  const hold = flight.hold;
  if (flight.ended || hold <= 0) return;
  flight.hold = 0;
  flight.animations.forEach((animation) => {
    const now = animation.currentTime;
    if (typeof now !== 'number' || now < hold) animation.currentTime = hold;
  });
}

// any other animation of a move, held with the rest (its delay counts from the release)
export function run(flight: Flight, el: Element, frames: Keyframe[], timing: KeyframeAnimationOptions) {
  flight.animations.push(el.animate(frames, { ...timing, delay: (Number(timing.delay) || 0) + flight.hold }));
}

// resolves when every animation in it has finished (or been cancelled)
export function flightDone(flight: Flight): Promise<void> {
  return Promise.allSettled(flight.animations.map((animation) => animation.finished)).then(() => undefined);
}

// lands it at once (another move, the tab hiding): every piece is itself again
export function endFlight(flight: Flight | null) {
  if (!flight || flight.ended) return;
  flight.ended = true;
  flight.animations.forEach((animation) => animation.cancel());
  flight.cleanups.forEach((clean) => clean());
}

function moving(flight: Flight, el: HTMLElement, origin: string) {
  const style = el.style;
  const previous = { origin: style.transformOrigin, will: style.willChange };
  style.transformOrigin = origin;
  style.willChange = 'transform, opacity';
  flight.cleanups.push(() => {
    style.transformOrigin = previous.origin;
    style.willChange = previous.will;
  });
}

const box = (rect: Rect, at: Rect) => `translate3d(${(rect.left - at.left).toFixed(2)}px, ${(rect.top - at.top).toFixed(2)}px, 0px) scale(${(rect.width / Math.max(at.width, 0.01)).toFixed(4)}, ${(rect.height / Math.max(at.height, 0.01)).toFixed(4)})`;

function lerp(from: Rect, to: Rect, e: number): Rect {
  return {
    left: from.left + (to.left - from.left) * e,
    top: from.top + (to.top - from.top) * e,
    width: from.width + (to.width - from.width) * e,
    height: from.height + (to.height - from.height) * e,
  };
}

/* A piece both views show, carried from `from` to `to` (screen rects). It is laid out at `at` (its screen rect with
   no transform: usually `to`). A picture inside a box changing shape keeps its own shape: scaled evenly by the larger
   of the box's two scales, about its centre, and cropped by the box. fade: 'in' / 'out' over the carry's second or
   first half (a carry only part of the way, from or to a place far off screen) */
export function carry(
  flight: Flight,
  el: HTMLElement,
  at: Rect,
  from: Rect,
  to: Rect,
  options: { picture?: HTMLElement | null; duration?: number; delay?: number; fade?: 'in' | 'out' } = {},
) {
  if (at.width <= 0 || at.height <= 0) return;
  const duration = options.duration ?? CARRY_MS;
  moving(flight, el, '0 0');
  const frames: Keyframe[] = [];
  const counter: Keyframe[] = [];
  const reshape = Math.abs(from.width / Math.max(from.height, 0.01) - to.width / Math.max(to.height, 0.01)) > 0.01;
  for (let k = 0; k <= SAMPLES; k += 1) {
    const offset = k / SAMPLES;
    const e = carryCurve(offset);
    const rect = lerp(from, to, e);
    const frame: Keyframe = { offset, transform: box(rect, at) };
    if (options.fade === 'out') frame.opacity = Math.max(0, Math.min(1, 1 - (offset - 0.25) / 0.45));
    if (options.fade === 'in') frame.opacity = Math.max(0, Math.min(1, (offset - 0.3) / 0.45));
    frames.push(frame);
    if (reshape) {
      const sx = rect.width / at.width;
      const sy = rect.height / at.height;
      const even = Math.max(sx, sy);
      counter.push({ offset, transform: `scale(${(even / sx).toFixed(4)}, ${(even / sy).toFixed(4)})` });
    }
  }
  const timing: KeyframeAnimationOptions = { duration, delay: (options.delay ?? 0) + flight.hold, easing: 'linear', fill: 'backwards' };
  flight.animations.push(el.animate(frames, timing));
  const picture = options.picture;
  if (reshape && picture) {
    moving(flight, picture, '50% 50%');
    flight.animations.push(picture.animate(counter, timing));
  }
}

/* What has no place on the new screen: held where it was (`from`, screen), it drifts a little away from `centre`,
   shrinks a touch and, once what arrives is coming in over it, dissolves. It stays gone (fill) until the flight ends */
export function leave(
  flight: Flight,
  el: HTMLElement,
  at: Rect,
  from: Rect,
  centre: { x: number; y: number } | null,
  // a piece's own old chrome (a tile's number on a post going elsewhere) goes at once instead: nothing replaces it
  options: { quick?: boolean } = {},
) {
  if (at.width <= 0 || at.height <= 0) return;
  moving(flight, el, '0 0');
  const middle = centreOf(from);
  let dx = 0;
  let dy = 0;
  if (centre) {
    const length = Math.hypot(middle.x - centre.x, middle.y - centre.y);
    if (length > 1) {
      dx = ((middle.x - centre.x) / length) * DRIFT_PX;
      dy = ((middle.y - centre.y) / length) * DRIFT_PX;
    }
  }
  const away: Rect = {
    left: middle.x + dx - (from.width * AWAY_SCALE) / 2,
    top: middle.y + dy - (from.height * AWAY_SCALE) / 2,
    width: from.width * AWAY_SCALE,
    height: from.height * AWAY_SCALE,
  };
  const frames: Keyframe[] = options.quick
    ? [{ transform: box(from, at), opacity: 1 }, { transform: box(away, at), opacity: 0 }]
    : [
        { offset: 0, transform: box(from, at), opacity: 1 },
        { offset: LEAVE_HOLD, opacity: 1 },
        { offset: 1, transform: box(away, at), opacity: 0 },
      ];
  flight.animations.push(el.animate(frames, options.quick
    ? { duration: QUICK_LEAVE_MS, delay: flight.hold, easing: QUICK_LEAVE_EASE, fill: 'both' }
    : { duration: LEAVE_MS, delay: flight.hold, easing: LEAVE_EASE, fill: 'both' }));
}

/* What is new to the screen: where it now is (`at`), from a touch smaller and nearer `centre`, dissolving in, a ring
   later the further it is from it. Holds unseen until its turn (fill backwards) */
export function enter(flight: Flight, el: HTMLElement, at: Rect, centre: { x: number; y: number } | null, extraDelay = 0) {
  if (at.width <= 0 || at.height <= 0) return;
  moving(flight, el, '0 0');
  const middle = centreOf(at);
  let dx = 0;
  let dy = 0;
  let ring = 0;
  if (centre) {
    const length = Math.hypot(middle.x - centre.x, middle.y - centre.y);
    ring = Math.min(RING_MAX, Math.round(length / RING_PX));
    if (length > 1) {
      dx = ((centre.x - middle.x) / length) * DRIFT_PX * 0.6;
      dy = ((centre.y - middle.y) / length) * DRIFT_PX * 0.6;
    }
  }
  const near: Rect = {
    left: middle.x + dx - (at.width * AWAY_SCALE) / 2,
    top: middle.y + dy - (at.height * AWAY_SCALE) / 2,
    width: at.width * AWAY_SCALE,
    height: at.height * AWAY_SCALE,
  };
  flight.animations.push(el.animate(
    [{ transform: box(near, at), opacity: 0 }, { transform: 'none', opacity: 1 }],
    { duration: ENTER_MS, delay: ENTER_DELAY_MS + ring * RING_STEP_MS + extraDelay + flight.hold, easing: ENTER_EASE, fill: 'backwards' },
  ));
}

/* A piece that stays itself but whose place on screen changed with the page (the profile over a zoom that moves the
   scroll): carried by translation only */
export function slide(flight: Flight, el: HTMLElement, at: Rect, from: Rect) {
  const dx = from.left - at.left;
  const dy = from.top - at.top;
  if (Math.abs(dx) < 0.5 && Math.abs(dy) < 0.5) return;
  moving(flight, el, '0 0');
  const frames: Keyframe[] = [];
  for (let k = 0; k <= SAMPLES; k += 1) {
    const offset = k / SAMPLES;
    const e = carryCurve(offset);
    frames.push({ offset, transform: `translate3d(${(dx * (1 - e)).toFixed(2)}px, ${(dy * (1 - e)).toFixed(2)}px, 0px)` });
  }
  flight.animations.push(el.animate(frames, { duration: CARRY_MS, delay: flight.hold, easing: 'linear', fill: 'backwards' }));
}
