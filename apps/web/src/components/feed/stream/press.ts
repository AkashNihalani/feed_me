import type { PointerEvent as ReactPointerEvent, TouchEvent as ReactTouchEvent } from 'react';

/* ─────────────────────────────────────────────────────────────
   PRESS — a thumbnail's give under the finger. CSS :active can't
   be it: on iOS it holds for the whole touch, a scroll included,
   so the thumbnail a scroll started on shrank while the page
   moved and sprang back when the finger lifted (a jitter under
   the finger on every scroll). The browser says when a touch
   becomes a scroll: it cancels the pointer. So the press comes a
   beat after a finger lands (most scrolls are under way by then)
   and lets go the moment the pointer is cancelled, lifted or
   leaves. A mouse presses at once.

   Spread `press` on the element that is the group: it sets
   [data-pressed] on itself, and the piece that gives is styled
   group-data-[pressed]:scale-… from it.
   ───────────────────────────────────────────────────────────── */

const PRESS_DELAY_MS = 90;
const timers = new WeakMap<HTMLElement, number>();

function release(el: HTMLElement) {
  const timer = timers.get(el);
  if (timer) window.clearTimeout(timer);
  timers.delete(el);
  if (el.dataset.pressed !== undefined) delete el.dataset.pressed;
}

function onPointerDown(event: ReactPointerEvent<HTMLElement>) {
  const el = event.currentTarget;
  release(el);
  if (!event.isPrimary || event.button > 0) return;
  if (event.pointerType === 'mouse') {
    el.dataset.pressed = '';
    return;
  }
  timers.set(el, window.setTimeout(() => {
    timers.delete(el);
    el.dataset.pressed = '';
  }, PRESS_DELAY_MS));
}

function onPointerEnd(event: ReactPointerEvent<HTMLElement>) {
  release(event.currentTarget);
}

export const press = {
  onPointerDown,
  onPointerUp: onPointerEnd,
  onPointerCancel: onPointerEnd,
  onPointerLeave: onPointerEnd,
};

/* ── a tap while the page glides ──
   A touch that lands while the page is still gliding (a swipe's momentum, its snap settling onto a card, a glide the
   page makes itself) is spent by iOS on stopping it: no click follows, so a card seemed to want a second tap. The Feed
   notes every scroll here (notePageScroll); a touch that begins within a moment of one (pageGliding) is a card's to
   take as its tap when it lifts where it landed (PostCell) */
const GLIDE_GAP_MS = 90;
let scrolledAt = Number.NEGATIVE_INFINITY;

export function notePageScroll() {
  scrolledAt = performance.now();
}

export function pageGliding(): boolean {
  return performance.now() - scrolledAt < GLIDE_GAP_MS;
}

/* The same for the header, which stays put over a page that scrolls under it (any tab's: the window scrolls): a touch
   that begins while the page glides and lifts where it landed is a tap on the button under it, so the header's circles,
   its control and who you're on never want a second tap after a scroll. Spread `glideTaps` on the bar */
const TAP_SLOP_PX = 10;
const TAP_MAX_MS = 500;
let watching = false;
let glideTouch: { x: number; y: number; t: number } | null = null;

export function watchPageGlide() {
  if (watching || typeof window === 'undefined') return;
  watching = true;
  window.addEventListener('scroll', notePageScroll, { passive: true });
}

export const glideTaps = {
  onTouchStart(event: ReactTouchEvent<HTMLElement>) {
    const touch = event.touches[0];
    glideTouch = event.touches.length === 1 && touch && pageGliding() ? { x: touch.clientX, y: touch.clientY, t: performance.now() } : null;
  },
  onTouchEnd(event: ReactTouchEvent<HTMLElement>) {
    const start = glideTouch;
    glideTouch = null;
    const touch = event.changedTouches[0];
    if (!start || !touch || event.touches.length) return;
    if (Math.hypot(touch.clientX - start.x, touch.clientY - start.y) > TAP_SLOP_PX || performance.now() - start.t > TAP_MAX_MS) return;
    const control = (event.target as Element).closest('button');
    if (!control || control.disabled) return;
    // (no click of the browser's own follows; one from a pointer, not the keys: detail 1)
    event.preventDefault();
    control.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, detail: 1, view: window }));
  },
  onTouchCancel() {
    glideTouch = null;
  },
};
