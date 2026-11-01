/* ─────────────────────────────────────────────────────────────
   TAB PUSH — the page under a phone's tab switch from the dock:
   the page you're on is a card that lifts and slides away the way
   your thumb goes, and the next page is there under it, its rows
   coming in a beat behind. On the same clock as the dock's swipe
   (Dock: its hero's ring drawn round by the thumb, and the switch
   when it closes).

   Nothing heavy happens while anything moves. A finger landing on
   the dock copies the page as it stands, held out of sight
   (holdPush, lib/feedStream/stillFrame): copying lays a whole page
   out again, and done then, while the finger is still, the swipe
   that may follow has a quiet main thread. As the thumb draws the
   ring round, the copy, laid over the page, eases back a touch,
   its trailing edge lifting with a soft shadow (scrubPush). Let go
   short and it settles back and goes (dropPush); a tap or a pull
   lets it go unseen (releasePush).

   The ring closed (leavePush), the card lifts on, slowly, on the
   compositor, while the tab is switched under it as any switch
   does it (so the tab you leave is put away as it always was,
   keeping its place). However long that takes, the card is already
   moving. Once the next page has been drawn under it (playPush,
   from the tab host: AppTabHost; WebKit starts an animation's clock
   in the frame that first draws it, so nothing starts in the
   switch's own heavy frame: lib/webkitMotion), the card slides
   away, uncovering it, as the dim over it lifts; then its rows
   come in.

   The header, on screen before and after, stays and adapts as it
   always has; one that leaves the screen slides away with the
   card, and one that comes on is uncovered with the page. The
   copies are fixed over the screen, so the page changing its scroll
   under them is never seen, and they are no bar to Safari
   (stillFrame). They stop short of the screen's top and foot: iOS
   26 Safari takes a fixed layer at either edge for a bar of the
   page's and paints its status bar or its toolbar solid in the
   layer's colour, giving up the see-through bars. It draws nothing
   fixed under them anyway, though the page goes on under both, so
   the card's table (the page's colour, and the dim over the next
   page) is a sheet of the page itself, laid over the whole screen
   where the screen is, under the card. Transform and opacity
   alone; the live page is never moved (its fixed pieces would be
   thrown out of place).
   ───────────────────────────────────────────────────────────── */

import { stillFrame, type StillFrame } from '@/lib/feedStream/stillFrame';
import { afterDrawn } from '@/lib/webkitMotion';
import { ms } from '@/lib/tempo';

// the card under the thumb, the ring drawn all the way round (k 1): this far back, this much smaller, this faded
const DRAG: PoseSpec = { px: 18, scale: 0.97, fade: 0.1 };
// the ring closed: the card lifts on this far while the next tab is made, easing to a stop
const LIFT: PoseSpec = { px: 30, scale: 0.95, fade: 0.16 };
const LIFT_MS = ms(520);
const LIFT_EASE = 'cubic-bezier(0, 0, 0.2, 1)';
// the next page drawn: the card slides away as the dim over the page lifts; the page's colour under the card clears
// first, so the page shows round it
const PEEL_MS = ms(460);
const PEEL_EASE = 'cubic-bezier(0.45, 0, 0.15, 1)';
const VEIL_MS = ms(160);
const DIM = 0.12;
// the rows come in once the card is on its way
const ROWS_AFTER = 0.18;
// let go short: back into place
const DROP_MS = ms(320);
const DROP_EASE = 'cubic-bezier(0.4, 0, 0.2, 1)';
// the soft shadow the card's trailing edge casts on what is under it
const SHADOW_PX = 44;
// the copies: over the header (100), under the layers' frost and the dock; the page's goes under a header that stays.
// They stop this far short of the screen's top and foot (the bars' own strips: stillFrame)
const COPY_Z = 150;
const BAR_CLEAR = 12;
const UNDER_HEADER_Z = 50;
// a switch that never comes (it should not) lets the copies go
const GIVE_UP_MS = 2400;
// the card's table reaches this far past the screen, up and down: a page still gliding under it (a flick just before
// the swipe: iOS tells the page's scroll late while it glides) stays covered
const TABLE_OVERSCAN = 1000;

type Pose = { x: number; scale: number; fade: number };
type PoseSpec = { px: number; scale: number; fade: number };

/* a copy on screen: its layer (fixed over the screen) holding the card (the page's copy) and the shadow its trailing
   edge casts; and under it, a card's table: the page's colour (until the next page is drawn) over the dim over the
   next page */
type Table = { el: HTMLElement; veil: HTMLElement; dim: HTMLElement };
type Copy = { frame: StillFrame; card: HTMLElement | null; shadow: HTMLElement; table: Table | null; pose: Pose };

export type TabPush = {
  // the tab the switch leaves, and the side the next one lies on (1: the right; the card goes the other way)
  from: string;
  dir: 1 | -1;
  page: Copy | null;
  // its header, when that was on screen
  header: Copy | null;
  // the card lifting while the next tab is made
  lift: Animation | null;
  // told as the card starts away (the dock lets go of what it shows of the switch)
  onReveal: (() => void) | null;
};

let held: { from: string; page: Copy | null; scrollY: number; shown: boolean } | null = null;
let handed: TabPush | null = null;
let handedTimer = 0;

const REST: Pose = { x: 0, scale: 1, fade: 0 };
// a pose toward the side the card goes (`dir` 1: the card going left)
const toward = (spec: PoseSpec, dir: number): Pose => ({ x: -dir * spec.px, scale: spec.scale, fade: spec.fade });

// the header over the page (AppShell), when any of it is on screen
function headerOnScreen(): HTMLElement | null {
  const header = document.querySelector<HTMLElement>('[data-fm-header-layer]');
  const box = header?.getBoundingClientRect();
  return header && box && box.height > 0 && box.bottom > 0 && box.top < window.innerHeight ? header : null;
}

function layer(style: Partial<CSSStyleDeclaration>) {
  const el = document.createElement('div');
  el.setAttribute('aria-hidden', 'true');
  Object.assign(el.style, { position: 'absolute', pointerEvents: 'none', ...style });
  return el;
}

/* The card's table: a sheet of the page itself over the whole screen and well past it (iOS 26 Safari draws nothing
   fixed under its status bar or its toolbar, though the page goes on under both), just under the card. Laid where the
   screen is now, and again after the page scrolls under it */
function tableFor(zIndex: number): Table {
  const el = layer({ left: '0', right: '0', height: `calc(100lvh + ${2 * TABLE_OVERSCAN + 96}px)`, zIndex: String(zIndex - 1), display: 'none' });
  const dim = layer({ inset: '0', background: '#000', opacity: '0', willChange: 'opacity' });
  const veil = layer({ inset: '0', background: 'var(--fm-page)', willChange: 'opacity' });
  el.append(dim, veil);
  document.body.appendChild(el);
  return { el, veil, dim };
}

function placeTable(table: Table | null) {
  if (table) table.el.style.top = `${(window.scrollY - TABLE_OVERSCAN).toFixed(1)}px`;
}

function setZ(copy: Copy, zIndex: number) {
  copy.frame.element.style.zIndex = String(zIndex);
  if (copy.table) copy.table.el.style.zIndex = String(zIndex - 1);
}

function showCopy(copy: Copy) {
  copy.frame.show();
  if (!copy.table) return;
  placeTable(copy.table);
  copy.table.el.style.display = 'block';
}

function removeCopy(copy: Copy | null) {
  copy?.frame.remove();
  copy?.table?.el.remove();
}

// `el` as it stands on screen, held out of sight; the card a layer of its own, on its page's colour, turning about the
// screen's middle
function copyOf(el: HTMLElement | null, zIndex: number, card = true): Copy | null {
  if (!el) return null;
  const frame = stillFrame(el, { zIndex, freeze: true, held: true, edge: BAR_CLEAR, foot: BAR_CLEAR });
  const cover = frame.element;
  const copy = cover.firstElementChild as HTMLElement | null;
  if (copy) {
    const box = el.getBoundingClientRect();
    Object.assign(copy.style, {
      willChange: 'transform, opacity',
      transformOrigin: `${(window.innerWidth / 2 - box.left).toFixed(1)}px ${(window.innerHeight / 2 - box.top).toFixed(1)}px`,
    });
    if (card) copy.style.background = 'var(--fm-page)';
  }
  const shadow = layer({ top: '0', bottom: '0', left: '0', width: `${SHADOW_PX}px`, opacity: '0', willChange: 'transform, opacity' });
  cover.appendChild(shadow);
  return { frame, card: copy, shadow, table: card ? tableFor(zIndex) : null, pose: { ...REST } };
}

// the card at a pose, its trailing edge's shadow beside it (`dir` 1: the card going left, its right edge trailing)
function cardFrames(pose: Pose, dir: number) {
  const width = window.innerWidth;
  const edge = width / 2 + (dir * width * pose.scale) / 2 + pose.x;
  return {
    card: { transform: `translate3d(${pose.x.toFixed(1)}px, 0, 0) scale(${pose.scale.toFixed(4)})`, opacity: 1 - pose.fade },
    shadow: { transform: `translate3d(${(dir > 0 ? edge : edge - SHADOW_PX).toFixed(1)}px, 0, 0)` },
  };
}

function shadowLook(shadow: HTMLElement, dir: number) {
  shadow.style.background = `linear-gradient(to ${dir > 0 ? 'right' : 'left'}, rgb(0 0 0 / 0.13), rgb(0 0 0 / 0.05) 35%, rgb(0 0 0 / 0))`;
}

function setPose(copy: Copy, pose: Pose, dir: number, shadowOpacity: number) {
  copy.pose = pose;
  const frames = cardFrames(pose, dir);
  if (copy.card) {
    copy.card.style.transform = String(frames.card.transform);
    copy.card.style.opacity = String(frames.card.opacity);
  }
  copy.shadow.style.transform = String(frames.shadow.transform);
  copy.shadow.style.opacity = shadowOpacity.toFixed(3);
}

// a pose `k` of the way from `a` to `b`
const between = (a: Pose, b: Pose, k: number): Pose => ({ x: a.x + (b.x - a.x) * k, scale: a.scale + (b.scale - a.scale) * k, fade: a.fade + (b.fade - a.fade) * k });

// the card's pose where it stands now (mid-animation: as drawn), held there inline so a motion can go on from it
function poseNow(copy: Copy, motion: Animation | null, dir: number, fallback: Pose): Pose {
  if (!motion || !copy.card) return copy.pose;
  const timing = motion.effect?.getComputedTiming();
  const k = typeof timing?.progress === 'number' ? timing.progress : 1;
  // (the lift's own curve, sampled: progress is already eased by the keyframe timing below)
  const pose = between(copy.pose, fallback, k);
  motion.cancel();
  setPose(copy, pose, dir, 1);
  return pose;
}

function dropHanded() {
  window.clearTimeout(handedTimer);
  const was = handed;
  handed = null;
  was?.lift?.cancel();
  removeCopy(was?.page ?? null);
  removeCopy(was?.header ?? null);
  was?.onReveal?.();
}

/* ── under the thumb ── */

// a finger on the dock: the page copied now, while it is still, and held out of sight (under the header, if that is on
// screen: it stays put over the page)
export function holdPush(from: string) {
  releasePush();
  const tab = document.querySelector<HTMLElement>(`[data-tab="${from}"]`);
  held = { from, page: copyOf(tab, headerOnScreen() ? UNDER_HEADER_Z : COPY_Z), scrollY: window.scrollY, shown: false };
}

// not a swipe after all (a tap, a pull): the copy goes unseen
export function releasePush() {
  removeCopy(held?.page ?? null);
  held = null;
}

// the thumb `k` of the way round (0 → 1), toward the tab on the `dir` side: the card eases back that far
export function scrubPush(k: number, dir: number) {
  const page = held?.page;
  if (!held || !page) return;
  // (the page moved since it was copied, a glide still running: the copy no longer shows it, and is let go)
  if (Math.abs(window.scrollY - held.scrollY) > 1) {
    releasePush();
    return;
  }
  if (!held.shown) {
    if (k < 0.01) return;
    held.shown = true;
    shadowLook(page.shadow, dir);
    showCopy(page);
  }
  setPose(page, between(REST, toward(DRAG, dir), k), dir, k);
}

// let go short: the card settles back into place, then the copy goes (the page under it is the same)
export function dropPush() {
  const was = held;
  held = null;
  const page = was?.page;
  if (!was || !page) return;
  if (!was.shown || !page.card) {
    removeCopy(page);
    return;
  }
  const dir = Math.sign(page.pose.x) < 0 ? 1 : -1;
  const from = cardFrames(page.pose, dir);
  const rest = cardFrames(REST, dir);
  const timing: KeyframeAnimationOptions = { duration: DROP_MS, easing: DROP_EASE, fill: 'forwards' };
  page.card.animate([from.card, rest.card], timing);
  // (the table clears over the same page it was laid over)
  page.table?.veil.animate([{ opacity: 1 }, { opacity: 0 }], timing);
  page.shadow.animate([{ ...from.shadow, opacity: page.shadow.style.opacity || 0 }, { ...rest.shadow, opacity: 0 }], timing).finished.then(
    () => removeCopy(page),
    () => removeCopy(page),
  );
}

/* ── the switch ── */

/* The ring closed on the tab on the `dir` side (or a switch from elsewhere: the page copied now). The card lifts on,
   on the compositor, until the next page is drawn under it. Called a frame or two before the URL changes, so the lift
   is under way before the switch's work begins. `onReveal`: told as the card starts away (or the switch never comes) */
export function leavePush(from: string, dir: 1 | -1, onReveal?: () => void) {
  dropHanded();
  if (!held || held.from !== from || !held.page) holdPush(from);
  const fresh = held;
  held = null;
  const page = fresh?.page ?? null;
  // the header, if it is on screen: copied over the page's copy, it either stays (the live one, adapting) or slides
  // away with the card, once the next page is there (playPush)
  const header = copyOf(headerOnScreen(), COPY_Z + 1, false);
  if (header) showCopy(header);
  let lift: Animation | null = null;
  if (page?.card) {
    if (!fresh?.shown) shadowLook(page.shadow, dir);
    showCopy(page);
    const from = cardFrames(page.pose, dir);
    const to = cardFrames(toward(LIFT, dir), dir);
    const timing: KeyframeAnimationOptions = { duration: LIFT_MS, easing: LIFT_EASE, fill: 'forwards' };
    lift = page.card.animate([from.card, to.card], timing);
    page.shadow.animate([{ ...from.shadow, opacity: page.shadow.style.opacity || 0 }, { ...to.shadow, opacity: 1 }], timing);
  }
  handed = { from, dir, page, header, lift, onReveal: onReveal ?? null };
  handedTimer = window.setTimeout(dropHanded, GIVE_UP_MS);
}

// the switch away from `from`, if the dock handed one over
export function takePush(from: string): TabPush | null {
  if (!handed || handed.from !== from) return null;
  const push = handed;
  handed = null;
  window.clearTimeout(handedTimer);
  return push;
}

/* The next page there (`into`, the tab now shown, under the card): once it has been drawn, the card slides away the
   way the thumb went, uncovering it, as the dim over it lifts and the page's colour round the card clears; its rows
   come in (`arrive`) once the card is on its way. A header still on screen is the live one, adapting: its copy goes at
   once, and the card goes under it; one that has left the screen slides away with the card. Returns a stop that lands
   it all at once */
export function playPush(into: HTMLElement, push: TabPush, arrive?: () => void) {
  const { page, header, dir } = push;
  // (the page has its new scroll by now: the table is laid where the screen is again)
  placeTable(page?.table ?? null);
  if (header && headerOnScreen()) {
    removeCopy(header);
    if (page) setZ(page, UNDER_HEADER_Z);
  }
  const keepHeader = Boolean(header && header.frame.element.isConnected);
  const motions: Animation[] = [];
  let rows = 0;
  let over = false;

  const peel = () => {
    push.onReveal?.();
    const width = window.innerWidth;
    const away = (from: Pose): Pose => ({ ...from, x: -dir * (width * 1.04 + SHADOW_PX) });
    const timing: KeyframeAnimationOptions = { duration: PEEL_MS, easing: PEEL_EASE, fill: 'forwards' };
    if (page?.card) {
      const from = poseNow(page, push.lift, dir, toward(LIFT, dir));
      const a = cardFrames(from, dir);
      const b = cardFrames(away(from), dir);
      motions.push(
        page.card.animate([a.card, b.card], timing),
        page.shadow.animate([{ ...a.shadow, opacity: 1 }, { ...b.shadow, opacity: 0.6 }], timing),
      );
      if (page.table) {
        page.table.dim.style.opacity = String(DIM);
        motions.push(
          page.table.veil.animate([{ opacity: 1 }, { opacity: 0 }], { duration: VEIL_MS, easing: 'ease-out', fill: 'forwards' }),
          page.table.dim.animate([{ opacity: DIM }, { opacity: 0 }], timing),
        );
      }
    }
    if (keepHeader && header?.card) {
      const a = cardFrames(REST, dir);
      const b = cardFrames(away(REST), dir);
      motions.push(header.card.animate([a.card, b.card], timing));
    }
    // the rows a beat behind the card (never in the frame the card sets off in: it is the card's)
    rows = window.setTimeout(() => arrive?.(), PEEL_MS * ROWS_AFTER);
    motions[0]?.finished.then(stop, stop);
    if (!motions.length) stop();
  };

  const stop = () => {
    if (over) return;
    over = true;
    cancelDrawn();
    window.clearTimeout(rows);
    push.lift?.cancel();
    motions.forEach((motion) => motion.cancel());
    removeCopy(page);
    removeCopy(header);
  };

  // nothing starts in the switch's own heavy frame (its clock would start before it reached the screen): once the
  // next page is on screen, under the card
  const cancelDrawn = afterDrawn(peel);
  return stop;
}
