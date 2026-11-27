/* ─────────────────────────────────────────────────────────────
   FEED STREAM LANDING — what a zoom is about, and where it opens
   the page. Pure, over two layouts (the view being left and the
   one coming): the zoom (FeedStream.zoomTo) and what makes its
   pictures ready (the warm-ups) ask the same question here.

   A zoom keeps your place, and your place is one of three things:
   - the start of the posts, while it is on screen and no post was
     picked: it holds still (the page's start stays the page's
     start), whatever post happens to lie in the middle;
   - a post: the one tapped or pinched, the card in view, else the
     one in the middle of the screen. It stays where it stands;
   - a day. MONTH is a month's best posts, best first: what lies in
     its middle says nothing of where you are in time (its best
     post may be three weeks into the feed). With no post picked,
     where you are in it is the month on screen: its newest day.

   Wherever it opens, a group's heading never hangs low with another
   group's foot over it, a month opens at its heading, and the post
   it is about lands whole, clear of the header and of the nav.
   ───────────────────────────────────────────────────────────── */

import { ZOOM_ORDER, groupKeyOf, shiftDay, type FeedMode } from './contract';
import {
  dayAtProfile,
  dayIndexAt,
  dayRestTop,
  idOf,
  indexForDay,
  indexOfSlot,
  itemIndexAt,
  nearestPost,
  sectionIndexAt,
  type StreamLayout,
} from './layout';

// the post a zoom is about lands at least this far clear of the header and the nav
export const TILE_CLEAR_PX = 24;

export type ZoomPlace = {
  // the start of the posts is on screen: it holds still
  start: boolean;
  // the post it is about (day:slot)
  zid: string | null;
  // ...or, with no post, the day
  day: string | null;
};

/* A zoom that has landed, kept for the next one: the post (or the day) it was about and where that stood on screen; what
   it came from, where that page was (the layer's top) and where the post lay in it (its middle; null: not there); and the
   scroll it landed at (null: not yet placed) */
export type ZoomMemory = {
  zid: string | null;
  day: string | null;
  screenY: number;
  from: FeedMode;
  fromTop: number;
  fromY: number | null;
  to: FeedMode;
  scroll: number | null;
};

// where a post lies in a layout (its middle), for its ZoomMemory
export function middleOf(layout: StreamLayout, zid: string | null): number | null {
  const at = zid ? indexOfSlot(layout, zid) : -1;
  return at >= 0 ? layout.items[at].y + layout.items[at].h / 2 : null;
}

// where the page is, in the layer's terms (top: the scroll less the layer's origin, base; the page's own top is -base),
// and the screen it is read on
export type ZoomView = {
  top: number;
  base: number;
  // the header's bottom edge as it stands, the nav's top edge, and the screen's height (viewport px)
  head: number;
  foot: number;
  height: number;
  // the middle of the band between the two, or where the fingers are (x: the layout's, y: the viewport's)
  point: { x: number; y: number };
};

// a Day view that snaps: one card a swipe, the profile a stop of its own (a phone's, a tablet's)
export function snaps(layout: StreamLayout): boolean {
  return layout.mode === 'today' && layout.mediaWidth == null;
}

/* The start of the posts is on screen: not yet gone under the header. (A Day view that snaps is at its start only on the
   profile, nearer that stop than its first card's rest, where the card has the screen to itself. Not dayAtProfile: that
   is half a card off the first one, and a tall screen's half a card is more than the whole profile) */
export function atStart(layout: StreamLayout, { top, base, head }: ZoomView): boolean {
  if (snaps(layout)) return top + base < (dayRestTop(layout, 0) + base) / 2;
  return -top >= head - 1;
}

/* The page as the last zoom left it (a Day view that snaps: on the same card, however it got back to it) */
export function unmoved(layout: StreamLayout, memory: ZoomMemory, scrollY: number, top: number): boolean {
  if (memory.to !== layout.mode || memory.scroll == null) return false;
  if (Math.abs(scrollY - memory.scroll) < 2) return true;
  if (!snaps(layout) || !memory.zid || dayAtProfile(layout, top)) return false;
  const card = layout.items[dayIndexAt(layout, top)];
  return Boolean(card) && idOf(card) === memory.zid;
}

// a zoom straight back to what the last one came from, nothing picked, the page as that one left it
export function straightBack(layout: StreamLayout, next: FeedMode, memory: ZoomMemory | null, scrollY: number, top: number): ZoomMemory | null {
  return memory && memory.from === next && unmoved(layout, memory, scrollY, top) ? memory : null;
}

/* Where you are, for a zoom to `next`. picked: the post tapped or pinched. pointed: the fingers' place (a pinch), with
   or without a post under them */
export function placeOf(
  layout: StreamLayout,
  next: FeedMode | null,
  view: ZoomView,
  memory: ZoomMemory | null,
  scrollY: number,
  picked: string | null = null,
  pointed = false,
): ZoomPlace {
  const { top, height, point } = view;
  const onPost = (zid: string | null): ZoomPlace => ({ start: false, zid, day: zid ? zid.split(':')[0] : null });
  const out = next != null && ZOOM_ORDER.indexOf(next) > ZOOM_ORDER.indexOf(layout.mode);
  const start = atStart(layout, view);
  // (a post pinched at the start, on the way out: the start still holds, the post only made sure of)
  if (picked) return start && out ? { ...onPost(picked), start: true } : onPost(picked);
  if (start || (pointed && point.y < -top)) return { start: true, zid: null, day: null };
  /* (a Day view is about one post at a time, wherever the fingers are: the card in view, before anything the last zoom
     remembers. A zoom into the Day view on a month remembers a day, no post, and a move out of it about no post had
     nothing to fly on top: the grid's own order stacked the card the one in view covered over it, for the whole move) */
  if (layout.mode === 'today') {
    const card = layout.items[dayIndexAt(layout, top)];
    return onPost(card ? idOf(card) : null);
  }
  if (memory && (memory.zid || memory.day) && unmoved(layout, memory, scrollY, top)) return { start: false, zid: memory.zid, day: memory.day };
  if (layout.mode === 'month') {
    const under = pointed ? layout.items[itemIndexAt(layout, top + point.y)] : undefined;
    if (!under || under.kind !== 'tile') {
      const section = layout.items[sectionIndexAt(layout, top + point.y)];
      const day = section?.day ?? null;
      // the month you came out from: back where you were in it
      if (memory?.to === 'month' && memory.zid && day && groupKeyOf(memory.zid.split(':')[0], 'month') === groupKeyOf(day, 'month')) return onPost(memory.zid);
      return { start: false, zid: null, day };
    }
  }
  const at = nearestPost(layout, point.x, top + point.y, height);
  return onPost(at >= 0 ? idOf(layout.items[at]) : null);
}

/* A zoom straight back: the page as it was, moved only by as much as its post has (days that have come in since can have
   rearranged what is over it) */
export function returnTop(target: StreamLayout, memory: ZoomMemory): number {
  const at = memory.zid ? indexOfSlot(target, memory.zid) : -1;
  if (at >= 0 && snaps(target)) return dayRestTop(target, at);
  const now = middleOf(target, memory.zid);
  return memory.fromTop + (now != null && memory.fromY != null ? now - memory.fromY : 0);
}

/* A place's group in a grid (its header's item). A month coming into WEEK opens at its newest week; one mostly the next
   month's (a month ending on a Monday) has the next month's days first, the month's own at its foot, screens down: there
   the week before it, the month's first of its own */
function groupFor(target: StreamLayout, place: ZoomPlace): number {
  const { day } = place;
  if (!day) return -1;
  const at = indexForDay(target, day);
  const section = target.items[at];
  if (place.zid || target.mode !== 'week' || !section?.day) return at;
  const month = day.slice(0, 7);
  if (section.day.slice(0, 7) === month || shiftDay(groupKeyOf(day, 'week'), 3).slice(0, 7) === month) return at;
  const older = indexForDay(target, shiftDay(groupKeyOf(day, 'week'), -1));
  return older >= 0 && target.items[older].day?.slice(0, 7) === month ? older : at;
}

/* Where the page goes in the new view (the layer's top), for a place in the old one. anchorY: where the post it is about
   stands on screen */
export function landingTop(target: StreamLayout, place: ZoomPlace, view: ZoomView, anchorY: number): number {
  const { top, base, head, foot } = view;
  const focalAt = place.zid ? indexOfSlot(target, place.zid) : -1;
  const focal = focalAt >= 0 ? target.items[focalAt] : null;
  // (a view that snaps rests on the profile or on its first card, never between the two)
  if (place.start && snaps(target) && !focal) return atStart(target, view) ? -base : dayRestTop(target, 0);
  if (target.mode === 'today') {
    if (place.start && !focal) return top;
    if (focalAt >= 0) return dayRestTop(target, focalAt);
    return place.day ? dayRestTop(target, Math.max(0, indexForDay(target, place.day))) : top;
  }
  const floor = foot - TILE_CLEAR_PX;
  const ceiling = head + TILE_CLEAR_PX;
  if (place.start) return focal ? Math.max(top, focal.y + focal.h - floor) : top;
  const group = place.day ? groupFor(target, place) : -1;
  // the group's heading under the header's edge
  const heading = group >= 0 ? target.items[group].y - head : top;
  // no post of its own here (MONTH shows highlights; a month itself, coming in from it): its group, at its heading
  if (!focal) return heading;
  // WEEK: the post stays where it stands (its tile's middle there), the heading never lower than the header's edge;
  // MONTH: at its heading, a month being about a screen. Then the tile whole
  let to = target.mode === 'month' ? heading : Math.max(heading, focal.y + focal.h / 2 - anchorY);
  if (focal.y + focal.h - to > floor) to = focal.y + focal.h - floor;
  if (focal.y - to < ceiling) to = focal.y - ceiling;
  return to;
}
