'use client';

/* Lead's side of the ledger (components/motion/Ledger): the list itself is shared with the header's rail; what is
   here is the page under a list that changed height. */

import { carryCurve, reducedMotion, releaseFlight, startFlight } from '@/lib/feedStream/motion';
import { markScrollJump } from '@/lib/useCompressedOnScroll';
import { FLIGHT_HOLD_MS, afterDrawn, holdsFlights } from '@/lib/webkitMotion';
import { ledgerMoveTiming } from '@/components/motion/Ledger';

export { LEDGER_DEAL_MS, LEDGER_MOVE_MS, LEDGER_STEP_MS, Ledger, ledgerDelay } from '@/components/motion/Ledger';

const SLIDE_SAMPLES = 24;

function scrollerOf(el: HTMLElement): HTMLElement | null {
  const page = el.closest<HTMLElement>('[data-testid="lead-v3-page"]');
  return page && page.scrollHeight > page.clientHeight + 10 ? page : null;
}

// the floating header's lower edge: what is under it isn't seen
function headerBottom() {
  return document.querySelector('.fm-tab-header-shell')?.getBoundingClientRect().bottom ?? 0;
}

/* a part of the page on its way to where it now is: how far it still has to go, worked out from its curve (not read
   back from the page), so a second change while it moves sets it off again from where it is */
type Slide = { animation: Animation; from: number; delay: number; duration: number };
const sliding = new WeakMap<Element, Slide>();

function offsetOf(el: Element) {
  const slide = sliding.get(el);
  if (!slide || slide.animation.playState !== 'running') return 0;
  const progress = (Number(slide.animation.currentTime ?? 0) - slide.delay) / slide.duration;
  return slide.from * (1 - carryCurve(Math.max(0, Math.min(1, progress))));
}

/* When a piece of the page changes height (a list re-dealt to more or fewer rows, a row opening), what is under it
   doesn't jump: the parts of the page under it that are on screen travel from where they were to where they now
   are, on the list's own clock (ledgerMoveTiming: a deal's travel or a row's room), as compositor transforms. When
   the piece is above what the reader sees, the page keeps its place instead: the scroll takes up the difference
   (what the browser hasn't already taken up itself, clamping at the page's end), before the frame is drawn, and the
   header isn't told it was a scroll of the reader's. Watches `el` (a ResizeObserver: called after layout, before
   the frame is drawn). Returns a stop. */
export function keepBelow(el: HTMLElement) {
  let height = el.offsetHeight;
  let releases: Array<() => void> = [];
  const scroller = () => scrollerOf(el);
  const scrollNow = () => {
    const page = scroller();
    return page ? page.scrollTop : window.scrollY;
  };
  // where the page was last scrolled to by a scroll the browser told us about
  let seenScroll = scrollNow();
  const onScroll = () => {
    seenScroll = scrollNow();
  };
  const page = el.closest<HTMLElement>('[data-testid="lead-v3-page"]');
  window.addEventListener('scroll', onScroll, { passive: true });
  page?.addEventListener('scroll', onScroll, { passive: true });

  const observer = new ResizeObserver(() => {
    const next = el.offsetHeight;
    const delta = next - height;
    const was = height;
    height = next;
    // a page hidden (another tab in front) or shown again isn't a change of the board's
    if (!was || !next || Math.abs(delta) < 1 || reducedMotion()) return;
    follow(delta);
  });
  // a list inside it scrolled the page to keep a tapped row under the finger: what is under it moved with the scroll
  // even when the list kept its height (one row closing above as another opens below)
  const onAnchor = () => {
    const next = el.offsetHeight;
    const delta = next - height;
    // taken up here, so the observer (after layout) doesn't take it up again
    height = next;
    if (!reducedMotion()) follow(delta);
  };
  el.addEventListener('ledger:anchor', onAnchor);

  function follow(delta: number) {
    const rect = el.getBoundingClientRect();
    const view = window.innerHeight;
    const top = headerBottom();
    // a scroll made this frame that the browser hasn't reported yet (the list keeping a tapped row under the finger)
    // has moved everything here too: where the piece's end was seen
    const unseen = scrollNow() - seenScroll;
    const before = rect.bottom - delta + unseen;
    if (before <= top && rect.bottom <= top) {
      // above what the reader sees: what they are looking at stays where it is
      const already = scrollNow() - seenScroll;
      const still = delta - already;
      if (Math.abs(still) < 1) return;
      markScrollJump('keep', 240);
      const own = scroller();
      if (own) own.scrollTop += still;
      else window.scrollBy({ top: still, behavior: 'instant' });
      seenScroll = scrollNow();
      return;
    }
    // nothing under it on screen before or after: nothing to see move
    if (Math.min(before, rect.bottom) >= view) return;

    // every read first, then every move; how far on screen the parts under it just moved
    const shift = delta - unseen;
    if (Math.abs(shift) < 1) return;
    const parts: Array<{ part: HTMLElement; from: number }> = [];
    for (let sibling = el.nextElementSibling; sibling; sibling = sibling.nextElementSibling) {
      if (!(sibling instanceof HTMLElement)) continue;
      const box = sibling.getBoundingClientRect();
      // the first part of the page below the screen both before and after ends it
      if (box.top >= view && box.top - shift >= view) break;
      if (box.bottom <= 0 && box.bottom - shift <= 0) continue;
      parts.push({ part: sibling, from: offsetOf(sibling) - shift });
    }
    if (!parts.length) return;
    const duration = ledgerMoveTiming();
    const flight = startFlight(holdsFlights() ? FLIGHT_HOLD_MS : 0);
    for (const { part, from } of parts) {
      sliding.get(part)?.animation.cancel();
      const frames: Keyframe[] = [];
      for (let k = 0; k <= SLIDE_SAMPLES; k += 1) {
        const offset = k / SLIDE_SAMPLES;
        frames.push({ offset, transform: `translate3d(0, ${(from * (1 - carryCurve(offset))).toFixed(2)}px, 0)` });
      }
      const animation = part.animate(frames, { duration, delay: flight.hold, easing: 'linear', fill: 'backwards' });
      flight.animations.push(animation);
      sliding.set(part, { animation, from, delay: flight.hold, duration });
    }
    // (a held slide's clock reads its hold until it is released and moves on from it: offsetOf counts from there)
    if (flight.hold) releases.push(afterDrawn(() => releaseFlight(flight)));
    seenScroll = scrollNow();
  }
  observer.observe(el);
  return () => {
    observer.disconnect();
    el.removeEventListener('ledger:anchor', onAnchor);
    window.removeEventListener('scroll', onScroll);
    page?.removeEventListener('scroll', onScroll);
    releases.forEach((cancel) => cancel());
    releases = [];
  };
}
