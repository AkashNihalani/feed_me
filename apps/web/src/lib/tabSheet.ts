/* ─────────────────────────────────────────────────────────────
   TAB SWITCH — how the page changes when you pick another tab.

   A phone settles the tab in as it always has (AppTabHost: one
   transform on the tab, Lead's pieces arriving, Read's own
   arrival). From a laptop up (the side rail) the tab you pick is
   there at once, as you left it, and its page comes in as its own
   pieces rising into place: every block on screen (a card, a
   section, a row) a touch from below, a row at a time from the
   top, on one even beat, every time, cached or not (riseIn).
   Transform alone (no fade: iOS and WebKit draw a fading subtree
   with its shadows afresh every frame), set off only once the
   frame that shows the tab is on screen, and a finger or a wheel
   lands it at once.
   ───────────────────────────────────────────────────────────── */

import { endFlight, releaseFlight, run, startFlight } from '@/lib/feedStream/motion';
import { ms } from '@/lib/tempo';

// rise: how far each piece comes from, how long it travels, and the beat from one row to the next
const RISE_PX = 16;
const RISE_MS = ms(520);
const RISE_EASE = 'cubic-bezier(0.2, 0.7, 0.2, 1)';
const ROW_STEP_MS = ms(42);
const ROW_MAX = 5;
// pieces whose tops are this close share a row (a beat)
const ROW_SLOP_PX = 12;
// the most pieces one arrival moves (a dense grid: the rest of a row rides its beat anyway)
const PIECES_MAX = 48;
// a frame that shows the tab can be slow: the pieces hold at their start until it is on screen, at most this long
const RISE_HOLD_MS = 800;
// the top strip under a status bar never moves (Safari colours the bar from what is painted up there)
const STATUS_CLEAR_PX = 64;

const reduced = () => typeof window.matchMedia === 'function' && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

// a laptop up (the side rail): a switch rises (riseIn); a phone settles the tab as it always has (AppTabHost)
export function wideTabMotion(): boolean {
  return typeof window.matchMedia === 'function' && window.matchMedia('(min-width: 1024px)').matches;
}

// the tab you're on tapped again: nothing to switch
export function stayOnTab() {}

/* The tab on screen about to give way to another: a page still moving on its own lands first (Feed's order sliding:
   FeedStream listens for fm:tab-leave). Resolves at once */
export async function leaveTab(from: string, to: string, dir: number): Promise<void> {
  void from;
  void to;
  void dir;
  window.dispatchEvent(new Event('fm:tab-leave'));
}

/* ── rise ──────────────────────────────────────────────────── */

/* The tab a switch is bringing on screen, from the moment the tab host renders it until its arrival is set up: a tab
   whose page plays an entrance of its own (Read's engine, mounting again on a reveal) stays quiet when the rise is
   its arrival */
let switching: string | null = null;
let shownTab: string | null = null;
// the tab host rendering `tab`: a switch when another one was shown
export function noteTab(tab: string) {
  switching = shownTab !== null && shownTab !== tab ? tab : null;
}
// ...and showing it (its arrival set up)
export function settleTab(tab: string) {
  shownTab = tab;
  switching = null;
}
export function isSwitchingTo(tab: string) {
  return switching === tab;
}

/* the page's blocks on screen: a container taller than most of the screen is gone into (a column, a list, Feed's
   layer of cards), anything else on screen is a block (a card, a section, a row) */
function blocksOf(root: HTMLElement, screen: number): HTMLElement[] {
  const found: HTMLElement[] = [];
  const visit = (el: Element, depth: number) => {
    for (const child of Array.from(el.children)) {
      if (!(child instanceof HTMLElement) || found.length >= PIECES_MAX) continue;
      if (child.matches('[data-gone], [data-still-frame], [data-still-skip], script, style, template')) continue;
      const rect = child.getBoundingClientRect();
      if (rect.width < 1 || rect.height < 1 || rect.bottom <= 0 || rect.top >= screen) continue;
      if (rect.height > screen * 0.75 && child.children.length && depth < 8) visit(child, depth + 1);
      else if (rect.height >= 12) found.push(child);
    }
  };
  visit(root, 0);
  return found;
}

/* The tab host revealing `page` (laid out, scrolled to its place, its CSS entrances settled): its blocks rise into place.
   Returns how to land it at once, or null when there is nothing on screen to move */
export function riseIn(page: HTMLElement): (() => void) | null {
  if (reduced()) return null;
  const screen = window.innerHeight;
  // every read first, then every write
  const pieces = blocksOf(page, screen)
    .map((el) => ({ el, rect: el.getBoundingClientRect() }))
    .filter(({ rect }) => rect.top >= STATUS_CLEAR_PX)
    .sort((a, b) => a.rect.top - b.rect.top || a.rect.left - b.rect.left);
  if (!pieces.length) return null;
  const flight = startFlight(RISE_HOLD_MS);
  let row = -1;
  let rowTop = Number.NEGATIVE_INFINITY;
  pieces.forEach(({ el, rect }) => {
    if (rect.top - rowTop > ROW_SLOP_PX) {
      row += 1;
      rowTop = rect.top;
    }
    const delay = Math.min(row, ROW_MAX) * ROW_STEP_MS;
    // (added to whatever the piece already wears, so nothing it is placed by is lost)
    run(flight, el, [{ transform: `translate3d(0px, ${RISE_PX}px, 0px)` }, { transform: 'translate3d(0px, 0px, 0px)' }], { duration: RISE_MS, delay, easing: RISE_EASE, fill: 'backwards', composite: 'add' });
  });
  let frameId = 0;
  const land = () => {
    window.cancelAnimationFrame(frameId);
    window.removeEventListener('wheel', land);
    window.removeEventListener('touchmove', land);
    endFlight(flight);
  };
  window.addEventListener('wheel', land, { passive: true });
  window.addEventListener('touchmove', land, { passive: true });
  // set off once the frame that shows the tab is on screen: two frames on
  frameId = window.requestAnimationFrame(() => {
    frameId = window.requestAnimationFrame(() => releaseFlight(flight));
  });
  void Promise.allSettled(flight.animations.map((animation) => animation.finished)).then(() => {
    window.removeEventListener('wheel', land);
    window.removeEventListener('touchmove', land);
  });
  return land;
}
