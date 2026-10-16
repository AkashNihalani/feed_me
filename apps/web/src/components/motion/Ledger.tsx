'use client';

/* ─────────────────────────────────────────────────────────────
   LEDGER — how the app's ranked lists and rails move: Lead's
   Feederboard rows and the cards under it, and the story rail of
   circles in the shared header.

   A list re-dealt (a new scope, a new window, a new view) plays
   as one cascade, first item first, a beat an item. Each place
   hands over: what leaves it lifts away, and a moment later what
   takes it comes in, so a place never holds two items at once.
   An item that stays but changed places travels to its new one on
   Feed's carry curve, on its new place's beat; the bar behind a
   row (data-ledger-sweep) glows as it sweeps to its new length;
   its numbers roll on the same beat (SlotText's delay:
   ledgerDelay). A change that isn't a deal (a row opening) makes
   room: everything that moved moves together, on one clock with
   the page under the list (ledgerMoveTiming).

   Everything is transform and opacity on the compositor (WAAPI,
   replace composite). The old items are read where they were seen,
   before React changes the page (getSnapshotBeforeUpdate: their
   places, and copies of the ones leaving); every read is done
   before anything is set moving, in the frame that first draws the
   new list, and what is off screen before and after isn't moved at
   all (its numbers just show their new values). On WebKit the
   moves are held until that frame is on screen (lib/webkitMotion).
   A list that is shown again (a tab coming back) doesn't deal
   again: it was already there.
   ───────────────────────────────────────────────────────────── */

import { Component, createRef, type ReactNode } from 'react';
import { CARRY_EASE_CSS, reducedMotion, releaseFlight, run, startFlight, type Flight } from '@/lib/feedStream/motion';
import { ms } from '@/lib/tempo';
import { markScrollJump } from '@/lib/useCompressedOnScroll';
import { FLIGHT_HOLD_MS, afterDrawn, holdsFlights } from '@/lib/webkitMotion';
import { cn } from '@/lib/utils';

// a re-dealt list plays first item first, a beat an item (a long list never trails past its tenth)
export const LEDGER_STEP_MS = ms(55);
const LEDGER_MAX_STEPS = 9;
export const ledgerDelay = (order: number) => Math.min(Math.max(order, 0), LEDGER_MAX_STEPS) * LEDGER_STEP_MS;
// a place hands over: what arrives starts this long after what leaves it
const HANDOFF_MS = ms(170);
// what arrives comes into its place, settling slowly
export const LEDGER_ENTER_MS = ms(580);
export const LEDGER_ENTER_EASE = 'cubic-bezier(0.2, 0.7, 0.2, 1)';
const RISE_PX = 22;
// what leaves lifts a little and is gone before what takes its place is halfway in
const LEAVE_MS = ms(260);
const LEAVE_EASE = 'cubic-bezier(0.4, 0, 0.8, 0.6)';
const LIFT_PX = 12;
// an item changing places in a deal
export const LEDGER_MOVE_MS = ms(640);
// items making room (a row opening or closing): all together, with the page under the list
export const LEDGER_ROOM_MS = ms(520);
// the bar behind a row glows as it sweeps to its new length, a little after its row
const SWEEP_MS = ms(760);
const SWEEP_AFTER_MS = ms(90);
// how long a full deal takes, to its last bar settling (what follows a deal waits for it)
export const LEDGER_DEAL_MS = HANDOFF_MS + LEDGER_MAX_STEPS * LEDGER_STEP_MS + SWEEP_AFTER_MS + SWEEP_MS;
// what is this far off screen, before and after, isn't moved
const SCREEN_MARGIN_PX = 120;
/* A fan (a rail of circles) moves as one glide, all of it at once, on Feed's carry curve: the item that stays (the
   feed you tapped) glides to its new place, and everything else comes out of, or goes back into, the place it was
   tapped in. What arrives slides out from there to its own place (from behind it: the rail stacks first over last),
   so the far ones travel furthest and the rail is seen to open out of the feed; what leaves slides back into that
   place and is gone as it tucks in. No beats, no ripple, no bounce: one gesture, the way Feed's posts move. */
export const FAN_MS = ms(600);
const FAN_SCALE = 0.94;

// a move whose frame didn't come for this long (a page in the background) is stale: the list just shows what it is
// (well past the slowest commit a switch makes, so a heavy one still plays its deal)
const STALE_MS = 2500;

/* The clock the list's last moves ran on, for the page under it (keepBelow, read in the same frame, after layout):
   a deal's travel, or the room a row makes. */
let lastMove = { at: -1, duration: LEDGER_MOVE_MS };
export function ledgerMoveTiming(fallback = LEDGER_MOVE_MS) {
  return performance.now() - lastMove.at < 80 ? lastMove.duration : fallback;
}
function stampMove(duration: number) {
  lastMove = { at: performance.now(), duration };
}

// where an item was seen: relative to the list's box on screen (so a list that scrolled in between, a rail sent back
// to its start, still carries each item from where it was seen), and on screen (to tell what was off it)
type Place = { x: number; y: number; width: number; height: number; screenX: number; screenY: number; opacity: number; sweep: string | null; order: number };
type Leaving = Place & { id: string; copy: HTMLElement };
// `at`: when the first of the commits it covers was read (a move that couldn't start for a long while is let go)
type Snapshot = { places: Map<string, Place>; leaving: Leaving[]; dealt: boolean; at: number };

export type LedgerProps = {
  // the items' ids in order; each item is a direct child carrying its own as data-ledger
  ids: readonly string[];
  // changes when the list is re-dealt (a new scope, window or view): its moves play as the cascade
  deal: string;
  // anything else that moves the items (a row opening, the layout changing): its moves make room together
  version?: unknown;
  // rows (y) or items in a row (x): which way things lift away and come in
  axis?: 'x' | 'y';
  // where arrivals come from along the axis, in px (rows rise from below; a rail's circles come out of the one before)
  rise?: number;
  // a list that scrolls sideways goes back to its start when it is re-dealt, so the deal is seen from its first item
  startOnDeal?: boolean;
  // play the very first deal when the list first appears (a page's board does; a header that is always there doesn't)
  dealOnMount?: boolean;
  // the item to keep under the reader's finger when room is made (the row just opened: a row closing above it doesn't
  // carry it away up the page; the page scrolls by the difference instead)
  anchor?: string;
  // a rail of circles (axis x): one glide out of, or back into, the place of the item that stays (see FAN_MS)
  fan?: boolean;
  // the beat between one item and the next in a deal, and how long what stays travels (defaults: LEDGER_STEP_MS,
  // LEDGER_MOVE_MS)
  step?: number;
  moveMs?: number;
  className?: string;
  // what the list is, to a screen reader (a rail of circles is a group)
  role?: string;
  label?: string;
  children: ReactNode;
};

const sameIds = (a: readonly string[], b: readonly string[]) => a.length === b.length && a.every((id, index) => id === b[index]);

function itemsOf(list: HTMLElement) {
  return Array.from(list.children).filter((child): child is HTMLElement => child instanceof HTMLElement && Boolean(child.dataset.ledger));
}

const sweepOf = (item: HTMLElement) => item.querySelector<HTMLElement>('[data-ledger-sweep]');

// a list skipped by content-visibility (a band far off screen) has nothing to show moving, and reading it would lay it out
const skipped = (list: HTMLElement) => typeof list.checkVisibility === 'function' && !list.checkVisibility({ contentVisibilityAuto: true } as CheckVisibilityOptions);

function offScreen(x: number, y: number, width: number, height: number) {
  return y + height < -SCREEN_MARGIN_PX || y > window.innerHeight + SCREEN_MARGIN_PX || x + width < -SCREEN_MARGIN_PX || x > window.innerWidth + SCREEN_MARGIN_PX;
}

/* the nearest thing that scrolls the list up and down (the page, or the window) */
function scrollBy(list: HTMLElement, top: number) {
  for (let el = list.parentElement; el && el !== document.body; el = el.parentElement) {
    const style = getComputedStyle(el);
    if (/(auto|scroll)/.test(style.overflowY) && el.scrollHeight > el.clientHeight + 1) {
      el.scrollTop += top;
      return;
    }
  }
  window.scrollBy({ top, behavior: 'instant' });
}

/* a copy of an item caught mid-move (a ring drawing, a plate lighting) keeps where each moving part is: a clone
   copies no animation, so each is written into the copy as it is now */
function frozenCopy(item: HTMLElement) {
  const copy = item.cloneNode(true) as HTMLElement;
  const moving = new Set(item.getAnimations({ subtree: true }).map((animation) => (animation.effect as KeyframeEffect | null)?.target).filter((target): target is Element => Boolean(target) && target !== item));
  if (!moving.size) return copy;
  const from = Array.from(item.querySelectorAll('*'));
  const to = copy.querySelectorAll<HTMLElement | SVGElement>('*');
  moving.forEach((target) => {
    const index = from.indexOf(target);
    const twin = index < 0 ? null : to[index];
    if (!twin) return;
    const style = getComputedStyle(target);
    twin.style.transform = style.transform;
    twin.style.opacity = style.opacity;
  });
  return copy;
}

/* a copy of an item on its way out shows what the item showed, still: no roll, no rise, no snap point */
function stillCopy(copy: HTMLElement) {
  copy.removeAttribute('data-ledger');
  copy.removeAttribute('data-ledger-still');
  copy.classList.add('ld-ghost');
  copy.querySelectorAll('[data-testid]').forEach((el) => el.removeAttribute('data-testid'));
  copy.querySelectorAll('[id]').forEach((el) => el.removeAttribute('id'));
  copy.querySelectorAll('.fm-slot-roll__face--old').forEach((el) => el.remove());
  copy.querySelectorAll('.fm-slot-roll__slot--changed').forEach((el) => el.classList.remove('fm-slot-roll__slot--changed'));
  copy.style.scrollSnapAlign = 'none';
}

export class Ledger extends Component<LedgerProps> {
  private list = createRef<HTMLDivElement>();
  private layer = createRef<HTMLDivElement>();
  // the items' own moves: the next deal ends them and sets each off again from where it is
  private rows: Flight | null = null;
  // copies leaving, each on its own (a new deal lets them finish)
  private copies = new Set<Animation>();
  // copies hanging below the list's new end hold its height until they have gone
  private holding = new Set<Animation>();
  private pending: Snapshot | null = null;
  private frame = 0;
  private releases = new Set<() => void>();
  // a list shown again (Activity reveals the tab: componentDidMount runs again on the same instance) doesn't deal
  // again; set once something has played (StrictMode's rehearsal mount cancels its first deal, the real one remakes it)
  private dealt = false;
  // when each item still on its way in is due to start, so room made in the middle of a deal keeps its hand-over
  private arrivals = new Map<string, number>();

  componentDidMount() {
    if (this.dealt || this.props.dealOnMount === false) return;
    // the first deal: every item arrives, first item first
    this.schedule({ places: new Map(), leaving: [], dealt: true, at: performance.now() });
  }

  getSnapshotBeforeUpdate(prev: LedgerProps): Snapshot | null {
    const { ids, deal, version } = this.props;
    if (prev.deal === deal && prev.version === version && sameIds(prev.ids, ids)) return null;
    const list = this.list.current;
    if (!list || reducedMotion() || skipped(list)) return null;
    const box = list.getBoundingClientRect();
    const next = new Set(ids);
    // only an item still on its way can be part-faded (reading opacity restyles; at rest it is 1)
    const restless = Boolean(this.rows?.animations.some((animation) => animation.playState === 'running'));
    const places = new Map<string, Place>();
    const leaving: Leaving[] = [];
    itemsOf(list).forEach((item, order) => {
      // an item left still by an earlier deal lets go of the mark before this deal's numbers are written, so they
      // roll if it is on screen now (a roll still running from the last deal keeps it: that one was held back)
      if (!item.querySelector('.fm-slot-roll__slot--changed')) item.removeAttribute('data-ledger-still');
      const id = item.dataset.ledger as string;
      const rect = item.getBoundingClientRect();
      const place: Place = {
        x: rect.left - box.left,
        y: rect.top - box.top,
        width: rect.width,
        height: rect.height,
        screenX: rect.left,
        screenY: rect.top,
        opacity: restless ? Number(getComputedStyle(item).opacity) || 0 : 1,
        sweep: sweepOf(item)?.style.transform ?? null,
        order,
      };
      places.set(id, place);
      if (!next.has(id) && !offScreen(rect.left, rect.top, rect.width, rect.height)) leaving.push({ ...place, id, copy: frozenCopy(item) });
    });
    return { places, leaving, dealt: prev.deal !== deal, at: performance.now() };
  }

  componentDidUpdate(prev: LedgerProps, _state: unknown, snapshot: Snapshot | null) {
    // a list that starts over on a deal does so whatever the motion (reduced, skipped or stale), before the frame
    const list = this.list.current;
    if (list && this.props.startOnDeal && prev.deal !== this.props.deal) {
      list.scrollLeft = 0;
      list.scrollTop = 0;
    }
    if (snapshot) this.schedule(snapshot);
  }

  componentWillUnmount() {
    window.cancelAnimationFrame(this.frame);
    this.pending = null;
    this.releases.forEach((cancel) => cancel());
    this.releases.clear();
    this.endRows();
    this.copies.forEach((animation) => animation.cancel());
  }

  /* Two commits before the moves start (a pick landing on a window switch): the items are where the first saw them
     (places and copies both), and an item the first brought in that never reached the screen isn't seen leaving. */
  private schedule(snapshot: Snapshot) {
    // a move still waiting from long ago (its frame never came: a page in the background) is let go, not merged
    const pending = this.pending && performance.now() - this.pending.at < STALE_MS ? this.pending : null;
    if (pending) {
      const ids = new Set(this.props.ids);
      snapshot = {
        places: pending.places,
        leaving: [
          ...pending.leaving.filter((gone) => !ids.has(gone.id)),
          ...snapshot.leaving
            .filter((gone) => pending.places.has(gone.id) && !pending.leaving.some((one) => one.id === gone.id))
            .map((gone) => ({ ...gone, ...(pending.places.get(gone.id) as Place) })),
        ],
        dealt: pending.dealt || snapshot.dealt,
        at: pending.at,
      };
    }
    this.pending = snapshot;
    window.cancelAnimationFrame(this.frame);
    // read and moved in the frame that first draws the new list (the browser lays it out there anyway)
    this.frame = window.requestAnimationFrame(() => {
      const next = this.pending;
      this.pending = null;
      // the list has been seen whatever plays now (a reveal later doesn't deal it again)
      this.dealt = true;
      if (next && performance.now() - next.at < STALE_MS) this.play(next);
    });
  }

  // the items' last moves stop (a held release still to come can't bring them back)
  private endRows() {
    const rows = this.rows;
    if (!rows) return;
    rows.ended = true;
    rows.animations.forEach((animation) => animation.cancel());
    this.rows = null;
  }

  private play(snapshot: Snapshot) {
    const list = this.list.current;
    const layer = this.layer.current;
    if (!list || !layer || reducedMotion() || skipped(list)) return;
    this.dealt = true;
    const { axis = 'y', startOnDeal = false, anchor, fan = false } = this.props;
    const across = axis === 'x';
    const rise = this.props.rise ?? RISE_PX;
    const step = this.props.step ?? LEDGER_STEP_MS;
    const moveMs = this.props.moveMs ?? LEDGER_MOVE_MS;
    const beatOf = (order: number) => Math.min(Math.max(order, 0), LEDGER_MAX_STEPS) * step;
    this.endRows();
    if (startOnDeal && snapshot.dealt && (list.scrollLeft || list.scrollTop)) {
      list.scrollLeft = 0;
      list.scrollTop = 0;
    }
    // room made under the reader's finger: the anchor stays where it was tapped, the page scrolls by the difference
    const anchorWas = !snapshot.dealt && anchor ? snapshot.places.get(anchor) : undefined;
    if (anchorWas && !offScreen(anchorWas.screenX, anchorWas.screenY, anchorWas.width, anchorWas.height)) {
      const anchorItem = itemsOf(list).find((item) => item.dataset.ledger === anchor);
      const shift = anchorItem ? anchorItem.getBoundingClientRect().top - anchorWas.screenY : 0;
      if (Math.abs(shift) > 1) {
        markScrollJump('keep', 240);
        scrollBy(list, shift);
        // the page under the list moved with that scroll even if the list kept its height: it is told (keepBelow)
        list.dispatchEvent(new CustomEvent('ledger:anchor', { bubbles: true, detail: shift }));
      }
    }

    /* every read first (one layout), then every move */
    const box = list.getBoundingClientRect();
    const scroll = { left: list.scrollLeft, top: list.scrollTop };
    const items = itemsOf(list).map((item, order) => {
      const rect = item.getBoundingClientRect();
      return { item, order, id: item.dataset.ledger as string, rect, x: rect.left - box.left, y: rect.top - box.top, sweep: sweepOf(item) };
    });
    // where the list's own content ends now (a height still held from a deal before isn't it)
    const natural = items.reduce((end, { y, rect }) => Math.max(end, y + scroll.top + rect.height), 0);
    const now = performance.now();
    if (snapshot.dealt) this.arrivals.clear();

    const hold = holdsFlights() ? FLIGHT_HOLD_MS : 0;
    const rows = startFlight(hold);
    const copies = startFlight(hold);
    this.rows = rows;

    // a fan's step from one item to the next, and where the gesture comes from: the place the item that stays (the
    // one nearest, when several do) was in before
    const pitch = items.length > 1 ? Math.abs(items[1].x - items[0].x) || items[0].rect.width : (items[0]?.rect.width ?? 64) + 8;
    const holders = fan ? items.filter(({ id }) => snapshot.places.has(id)).map(({ id }) => (snapshot.places.get(id) as Place).x) : [];
    const originFor = (x: number) => (holders.length ? holders.reduce((best, there) => (Math.abs(there - x) < Math.abs(best - x) ? there : best)) : x - pitch);

    // what leaves: a copy where it was seen, lifting away on its place's beat (under what takes its place)
    let lowest = 0;
    for (const gone of snapshot.leaving) {
      const { copy } = gone;
      stillCopy(copy);
      Object.assign(copy.style, { position: 'absolute', left: `${gone.x + scroll.left}px`, top: `${gone.y + scroll.top}px`, width: `${gone.width}px`, height: `${gone.height}px`, margin: '0' });
      layer.appendChild(copy);
      if (fan) {
        // back into the place the gesture comes from (under every item: it tucks in behind), gone as it gets there
        const into = originFor(gone.x) - gone.x;
        run(copies, copy, [
          { offset: 0, transform: 'translate3d(0px, 0, 0) scale(1)', opacity: gone.opacity },
          { offset: 0.45, opacity: gone.opacity },
          { offset: 1, transform: `translate3d(${into.toFixed(1)}px, 0, 0) scale(${FAN_SCALE})`, opacity: 0 },
        ], { duration: FAN_MS, easing: CARRY_EASE_CSS, fill: 'both' });
      } else {
        const lift = across ? `translate3d(${-Math.sign(rise || 1) * LIFT_PX}px, 0, 0) scale(0.94)` : `translate3d(0, ${-LIFT_PX}px, 0)`;
        run(copies, copy, [
          { offset: 0, transform: 'translate3d(0, 0, 0)', opacity: gone.opacity },
          { offset: 0.72, opacity: 0 },
          { offset: 1, transform: lift, opacity: 0 },
        ], { duration: LEAVE_MS, delay: beatOf(gone.order), easing: LEAVE_EASE, fill: 'both' });
      }
      const animation = copies.animations[copies.animations.length - 1];
      this.copies.add(animation);
      // a copy hanging below the list's new end keeps the list that tall until it has gone: the page under the list
      // closes up after the rows have lifted out, not over them
      const bottom = gone.y + scroll.top + gone.height;
      if (bottom > natural + 1) {
        lowest = Math.max(lowest, bottom);
        this.hold(list, animation);
      }
      const done = () => {
        copy.remove();
        this.copies.delete(animation);
      };
      animation.finished.then(done, done);
    }

    // what stays travels from where it was seen; what is new comes in a moment after its place was handed over;
    // what is off screen before and after isn't moved (and its numbers don't roll)
    const handoff = snapshot.leaving.length ? HANDOFF_MS : 0;
    for (const { item, order, id, rect, x, y, sweep } of items) {
      const was = snapshot.places.get(id);
      const hidden = offScreen(rect.left, rect.top, rect.width, rect.height) && (!was || offScreen(was.screenX, was.screenY, was.width, was.height));
      if (hidden) {
        item.setAttribute('data-ledger-still', '');
        continue;
      }
      if (!item.querySelector('.fm-slot-roll__slot--changed')) item.removeAttribute('data-ledger-still');
      const from = (dx: number, dy: number) => (across
        ? `translate3d(${(dx + rise).toFixed(1)}px, ${dy.toFixed(1)}px, 0) scale(0.94)`
        : `translate3d(${dx.toFixed(1)}px, ${(dy + rise).toFixed(1)}px, 0)`);
      if (!was && fan) {
        // out of the place the gesture comes from, to its own place, as one with everything else
        this.arrivals.set(id, now);
        run(rows, item, [
          { offset: 0, transform: `translate3d(${(originFor(x) - x).toFixed(1)}px, 0, 0) scale(${FAN_SCALE})`, opacity: 0 },
          { offset: 0.22, opacity: 1 },
          { offset: 1, transform: 'translate3d(0px, 0, 0) scale(1)', opacity: 1 },
        ], { duration: FAN_MS, easing: CARRY_EASE_CSS, fill: 'backwards' });
        continue;
      }
      if (!was) {
        const delay = handoff + beatOf(order);
        this.arrivals.set(id, now + delay);
        run(rows, item, [{ transform: from(0, 0), opacity: 0 }, { transform: 'translate3d(0, 0, 0)', opacity: 1 }], { duration: LEDGER_ENTER_MS, delay, easing: LEDGER_ENTER_EASE, fill: 'backwards' });
        if (sweep) run(rows, sweep, [{ offset: 0, transform: 'scaleX(0)', opacity: 0 }, { offset: 0.45, opacity: 1 }], { duration: SWEEP_MS, delay: delay + SWEEP_AFTER_MS, easing: CARRY_EASE_CSS, fill: 'backwards' });
        continue;
      }
      // a deal moves each on its place's beat, as the list saw it; room is made all at once, with the page under the
      // list, as the reader saw it (on screen: the page may have scrolled to keep the anchor still)
      const beat = snapshot.dealt ? beatOf(order) : 0;
      const dx = snapshot.dealt ? was.x - x : was.screenX - rect.left;
      const dy = snapshot.dealt ? was.y - y : was.screenY - rect.top;
      // still waiting to come in when room was made: it keeps its turn (a place never holds two items at once)
      const due = !snapshot.dealt && was.opacity < 0.99 ? this.arrivals.get(id) : undefined;
      if (due != null && due > now) {
        run(rows, item, [{ transform: from(dx, dy), opacity: 0 }, { transform: 'translate3d(0, 0, 0)', opacity: 1 }], { duration: LEDGER_ENTER_MS, delay: due - now, easing: LEDGER_ENTER_EASE, fill: 'backwards' });
      } else if (fan && snapshot.dealt && Math.abs(dx) > 0.5) {
        // the feed you tapped glides to its new place, with everything else
        run(rows, item, [{ transform: `translate3d(${dx.toFixed(1)}px, 0, 0)` }, { transform: 'translate3d(0px, 0, 0)' }], { duration: FAN_MS, easing: CARRY_EASE_CSS, fill: 'backwards' });
      } else if (Math.abs(dx) > 0.5 || Math.abs(dy) > 0.5 || was.opacity < 0.99) {
        run(rows, item, [
          { transform: `translate3d(${dx.toFixed(1)}px, ${dy.toFixed(1)}px, 0)`, opacity: was.opacity },
          { transform: 'translate3d(0, 0, 0)', opacity: 1 },
        ], { duration: snapshot.dealt ? moveMs : LEDGER_ROOM_MS, delay: beat, easing: CARRY_EASE_CSS, fill: 'backwards' });
        // one coming up from below the list's new end holds its height until it has arrived
        const oldBottom = (snapshot.dealt ? was.y : was.screenY - box.top) + scroll.top + was.height;
        if (oldBottom > natural + 1) {
          lowest = Math.max(lowest, oldBottom);
          this.hold(list, rows.animations[rows.animations.length - 1]);
        }
      }
      if (sweep && snapshot.dealt && was.sweep && was.sweep !== sweep.style.transform) {
        run(rows, sweep, [{ offset: 0, transform: was.sweep }, { offset: 0.45, opacity: 1 }], { duration: SWEEP_MS, delay: beat + SWEEP_AFTER_MS, easing: CARRY_EASE_CSS, fill: 'backwards' });
      }
    }
    if (lowest) list.style.minHeight = `${Math.ceil(Math.max(lowest, parseFloat(list.style.minHeight) || 0))}px`;
    stampMove(snapshot.dealt ? moveMs : LEDGER_ROOM_MS);

    // on WebKit everything set off together once the frame that draws it is on screen: the moves above, and the CSS
    // fades the switch started in the items (a first place's light passing on, a label lighting)
    if (hold) {
      // only what this switch started (what was already under way keeps going); the list's own moves are held above
      const own = new Set([...rows.animations, ...copies.animations]);
      const fades = list.getAnimations({ subtree: true })
        .filter((animation) => !own.has(animation) && animation.playState === 'running' && (animation.pending || Number(animation.currentTime ?? 0) < 20));
      fades.forEach((animation) => {
        animation.pause();
        animation.currentTime = 0;
      });
      const cancel = afterDrawn(() => {
        this.releases.delete(cancel);
        releaseFlight(rows);
        releaseFlight(copies);
        fades.forEach((animation) => {
          if (animation.playState === 'paused') animation.play();
        });
      });
      this.releases.add(cancel);
    }
  }

  // the list keeps the height it had while `animation` plays (what leaves below its new end, or comes up from there);
  // once nothing holds it, it lets go and the page under it closes up, on the room's clock
  private hold(list: HTMLElement, animation: Animation) {
    this.holding.add(animation);
    const done = () => {
      if (this.holding.delete(animation) && !this.holding.size) {
        stampMove(LEDGER_ROOM_MS);
        list.style.minHeight = '';
      }
    };
    animation.finished.then(done, done);
  }

  render() {
    const { className, role, label, children } = this.props;
    return (
      <div ref={this.list} role={role} aria-label={label} className={cn('relative', className)}>
        {/* the copies of what is leaving, under the items that take their places */}
        <div ref={this.layer} aria-hidden="true" inert className="pointer-events-none absolute inset-0 overflow-clip" />
        {children}
      </div>
    );
  }
}
