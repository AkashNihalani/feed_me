/* ─────────────────────────────────────────────────────────────
   FEED STREAM LAYOUT — where every piece of the Feed tab sits.

   Pure: from the zoom, the index (every day's posts as their
   top %, `tops`, in display order) and whichever days have
   loaded, plus the viewport and the chrome's insets, it places
   every piece as an absolute box, sorted by y. A day that hasn't
   loaded is laid out exactly from its index entry, so the page
   is its true height from the first frame and nothing moves when
   the posts arrive (their slots draw placeholders meanwhile).
   Lookups are binary searches over the sorted boxes, so the page
   never measures the DOM to know what is on screen.

   Every zoom is laid out in the same coordinates: the layer's,
   which starts under the page's profile (the profile heads every
   zoom, the same element at the same place, so it holds still
   through a zoom).

   today (DAY): a column of posts, one per post, each resting with
   its centre on the focus line (focusY: the middle of the band
   between the folded header and the nav). A phone's post is a
   card filling that band (one swipe, one post: the page snaps).
   From READING_FROM wide a post is a row: its media in a 4:5
   frame (mediaWidth) with its reading beside it, rows a gap apart,
   scrolled freely; the row on the focus line is the one in view.
   week: a header per week and every post in a grid of 3:4 cells,
   in rows, with a rhythm: now and then a two-row block led by a
   winner drawn 2×2, on alternate sides, the rest of the block
   round it (arrange). Never a hole: only a week's last row may be
   short.
   month: the highlights, not the feed again: a header per month
   and that month's best posts in a few rows (its best one 2×2, the
   rest one cell each, best first), then a "+N" tile for the rest
   that opens it in WEEK.
   ───────────────────────────────────────────────────────────── */

import {
  ZOOM_COLUMNS,
  WINNER_MAX,
  ZOOM_GROUP,
  groupKeyOf,
  type FeedMode,
  type GroupUnit,
  type StreamDay,
  type StreamPost,
} from './contract';

/* ── geometry ──────────────────────────────────────────────── */

// a group's header: a rule, a kicker over a big title (taller on a wide screen, where the title is bigger), and the
// air between one group's last row and the next group's rule
export function sectionHeaderHeight(width: number) {
  return width >= 1024 ? 96 : 72;
}
export const SECTION_GAP = 20;
export const GRID_GAP = 2;
export const GRID_MAX_WIDTH = 1400;
// a phone's Day card: the band between the folded header and the nav, less this on every side, cards this far apart
export const REEL_INSET = 8;
export const REEL_GAP = 12;
// wider than a phone (and narrower than a row), a card is a "theatre": 9:16 in the middle, never wider than this
export const THEATRE_MAX_WIDTH = 520;
// between the page's profile and the first group (or the first post)
export const PROFILE_GAP = 10;
// from this wide the Day view is a column of rows: each post's media beside its reading
export const READING_FROM = 1024;
export const READING_GAP = 48;
const READING_MIN = 380;
const READING_MAX = 560;
const READING_SHARE = 0.3;
// a row's air from the folded header and the nav, and between one row and the next
const ROW_INSET = 24;
const ROW_GAP = 72;
// the media's shapes: a reel, and every post's frame in the Day view (Instagram's portrait post)
export const REEL_ASPECT = 9 / 16;
export const POST_ASPECT = 4 / 5;
// a row's 4:5 frame: never taller than this (the pictures are 640px; much past it they only blur), and always leaving
// this much margin each side (the date lives there)
export const MEDIA_MAX_HEIGHT = 720;
const WIDE_MARGIN_MIN = 120;
const MEDIA_MIN = 240;

const CELL_ASPECT = 4 / 3; // cells are 3:4 (height over width)
// month shows each month's highlights in this many rows
const HIGHLIGHT_ROWS = 3;
const THEATRE_FROM = 600;
const MIN_CARD_HEIGHT = 240;
const BOTTOM_PAD = 16;
// the Day view's first post sits this far under the profile
const DAY_TOP = 12;

export type GridMode = Exclude<FeedMode, 'today'>;

// where the chrome sits: the open header's bottom edge, the folded header's, and the nav's top edge (from the bottom)
export type StreamInsets = { top: number; topCompressed: number; bottom: number };

export type LayoutKind = 'section' | 'tile' | 'reel' | 'more';

export type LayoutItem = {
  kind: LayoutKind;
  // stable within a scope: r:<day>:<slot>, s:<group>, t:<day>:<slot>, m:<group>
  key: string;
  // a post's day and its place in the day (orderDayPosts); a section's newest day
  day: string | null;
  slot: number;
  // set once the day's posts have loaded
  postKey?: string;
  // a "+N" tile (month): how many of the group's posts it stands for
  more?: number;
  // a tile's size in cells (1 × 1 for anything else)
  cols: number;
  rows: number;
  x: number;
  y: number;
  w: number;
  h: number;
};

export type LayoutSection = { key: string; unit: GroupUnit; count: number; best: number | null };

export type LayoutInput = {
  mode: FeedMode;
  days: readonly StreamDay[];
  // the loaded days, each in orderDayPosts order
  posts: ReadonlyMap<string, StreamPost[]>;
  width: number;
  height: number;
  insets: StreamInsets;
};

export type StreamLayout = {
  mode: FeedMode;
  width: number;
  height: number;
  insets: StreamInsets;
  items: LayoutItem[];
  totalHeight: number;
  // the grids
  columns: number;
  gap: number;
  contentX: number;
  contentWidth: number;
  cellWidth: number;
  cellHeight: number;
  sections: ReadonlyMap<string, LayoutSection>;
  // today: a post rests with its centre on focusY (viewport px); posts are `stride` apart, each cardWidth × cardHeight
  focusY: number;
  stride: number;
  cardWidth: number;
  cardHeight: number;
  // a row (wide): the media frame's width at the row's left (the reading fills the rest); null: a phone's card
  mediaWidth: number | null;
  // lookups (built once per layout)
  bottoms: Float64Array; // running max of y + h, so "first item reaching below y" is a binary search
  byPost: ReadonlyMap<string, number>;
  byKey: ReadonlyMap<string, number>;
};

export function gridColumns(mode: GridMode, width: number): number {
  return ZOOM_COLUMNS[mode][width < 768 ? 0 : width < 1100 ? 1 : 2];
}

function sidePad(width: number) {
  if (width < 768) return 2;
  if (width < 1100) return 12;
  return 20;
}

// the content column at a width (the grids' and the page profile's), and the gap under the open header
export function contentBox(width: number): { contentX: number; contentWidth: number; topPad: number } {
  const contentWidth = Math.max(0, Math.min(width - sidePad(width) * 2, GRID_MAX_WIDTH));
  return { contentX: (width - contentWidth) / 2, contentWidth, topPad: width >= 1024 ? 12 : 8 };
}

// a post's identity across zooms: its day and its place in the day (loaded or not)
export function slotId(day: string, slot: number): string {
  return `${day}:${slot}`;
}

function dayLength(day: StreamDay, loaded: StreamPost[] | undefined): number {
  if (loaded) return loaded.length;
  return Array.isArray(day.tops) && day.tops.length ? day.tops.length : day.count;
}

function topAt(day: StreamDay, loaded: StreamPost[] | undefined, slot: number): number | null {
  if (loaded) return loaded[slot]?.topPercent ?? null;
  return Array.isArray(day.tops) ? day.tops[slot] ?? null : null;
}

type Place = { row: number; column: number; cols: number; rows: number };

/* WEEK's grid: rows of single cells, and now and then a two-row block led by the best winner among the posts it
   would hold, drawn 2×2 on alternate sides block after block (left, right, left), the rest of the block filling round
   it in order. A block is only made when there are posts enough to fill it, so there is never a hole: only the last
   row may be short. Places are returned in the posts' order (a winner moves up a few places to lead its block). */
function arrange(tops: ReadonlyArray<number | null>, columns: number): { places: Place[]; rows: number } {
  const places: Place[] = new Array(tops.length);
  // the single cells beside a 2×2 in a two-row block
  const around = columns >= 3 ? 2 * (columns - 2) : 0;
  let row = 0;
  let at = 0;
  let blocks = 0;
  while (at < tops.length) {
    let lead = -1;
    if (around && tops.length - at >= 1 + around) {
      for (let k = at; k < at + 1 + around; k += 1) {
        const top = tops[k];
        if (top != null && top <= WINNER_MAX && (lead < 0 || top < (tops[lead] as number))) lead = k;
      }
    }
    if (lead >= 0) {
      const leadColumn = blocks % 2 === 0 ? 0 : columns - 2;
      places[lead] = { row, column: leadColumn, cols: 2, rows: 2 };
      const cells: Array<{ row: number; column: number }> = [];
      for (let r = 0; r < 2; r += 1) {
        for (let column = 0; column < columns; column += 1) {
          if (column < leadColumn || column >= leadColumn + 2) cells.push({ row: row + r, column });
        }
      }
      let cell = 0;
      for (let k = at; k < at + 1 + around; k += 1) {
        if (k === lead) continue;
        places[k] = { ...cells[cell], cols: 1, rows: 1 };
        cell += 1;
      }
      at += 1 + around;
      row += 2;
      blocks += 1;
    } else {
      const take = Math.min(columns, tops.length - at);
      for (let column = 0; column < take; column += 1) places[at + column] = { row, column, cols: 1, rows: 1 };
      at += take;
      row += 1;
    }
  }
  return { places, rows: row };
}

/* First fit, row by row, from the first row with room (CSS grid's dense auto-placement for items that only say
   their span). Rows are bitmasks of taken cells. */
function pack(spans: ReadonlyArray<{ cols: number; rows: number }>, columns: number) {
  const taken: number[] = [];
  const full = (1 << columns) - 1;
  let open = 0;
  const places = spans.map(({ cols, rows }) => {
    const width = Math.min(cols, columns);
    const run = (1 << width) - 1;
    for (let row = open; ; row += 1) {
      for (let column = 0; column + width <= columns; column += 1) {
        const mask = run << column;
        let fits = true;
        for (let k = 0; k < rows; k += 1) {
          if (((taken[row + k] ?? 0) & mask) !== 0) {
            fits = false;
            break;
          }
        }
        if (!fits) continue;
        for (let k = 0; k < rows; k += 1) taken[row + k] = (taken[row + k] ?? 0) | mask;
        while ((taken[open] ?? 0) === full) open += 1;
        return { row, column, cols: width, rows };
      }
    }
  });
  return { places, rows: taken.length };
}

/* The Day view's geometry at a size: the focus line, and a post's box (a phone's card, a theatre card, or a wide
   screen's row of media + reading) */
function dayGeometry(width: number, height: number, insets: StreamInsets) {
  const bandTop = insets.topCompressed;
  const bandBottom = height - insets.bottom;
  const band = Math.max(MIN_CARD_HEIGHT, bandBottom - bandTop);
  const focusY = (bandTop + bandBottom) / 2;
  if (width >= READING_FROM) {
    const reading = Math.min(READING_MAX, Math.max(READING_MIN, width * READING_SHARE));
    let frame = Math.min(MEDIA_MAX_HEIGHT, band - ROW_INSET * 2);
    let media = frame * POST_ASPECT;
    const room = width - reading - READING_GAP - WIDE_MARGIN_MIN * 2;
    if (media > room) {
      media = Math.max(MEDIA_MIN, room);
      frame = media / POST_ASPECT;
    }
    return { focusY, cardWidth: media + READING_GAP + reading, cardHeight: frame, mediaWidth: media as number | null, stride: frame + ROW_GAP };
  }
  const cardHeight = band - REEL_INSET * 2;
  const cardWidth = width < THEATRE_FROM
    ? Math.max(0, width - REEL_INSET * 2)
    : Math.min(cardHeight * REEL_ASPECT, THEATRE_MAX_WIDTH, width - REEL_INSET * 2);
  return { focusY, cardWidth, cardHeight, mediaWidth: null as number | null, stride: cardHeight + REEL_GAP };
}

export function computeLayout({ mode, days, posts, width, height, insets }: LayoutInput): StreamLayout {
  const { contentX, contentWidth } = contentBox(width);
  const items: LayoutItem[] = [];
  const sections = new Map<string, LayoutSection>();
  const day = dayGeometry(width, height, insets);

  // the grids
  const columns = mode === 'today' ? 1 : gridColumns(mode, width);
  const cellWidth = mode === 'today' ? 0 : Math.max(0, (contentWidth - GRID_GAP * (columns - 1)) / columns);
  const cellHeight = cellWidth * CELL_ASPECT;

  let totalHeight = height;
  if (mode === 'today') {
    const x = (width - day.cardWidth) / 2;
    let index = 0;
    for (const entry of days) {
      const loaded = posts.get(entry.day);
      const count = dayLength(entry, loaded);
      for (let slot = 0; slot < count; slot += 1) {
        items.push({
          kind: 'reel',
          key: `r:${entry.day}:${slot}`,
          day: entry.day,
          slot,
          postKey: loaded?.[slot]?.key,
          cols: 1,
          rows: 1,
          x,
          y: DAY_TOP + index * day.stride,
          w: day.cardWidth,
          h: day.cardHeight,
        });
        index += 1;
      }
    }
    // the last post can rest on the focus line: the page ends a focus line's distance from the screen's foot past it
    totalHeight = index ? DAY_TOP + (index - 1) * day.stride + day.cardHeight / 2 + (height - day.focusY) : height;
  } else {
    const unit = ZOOM_GROUP[mode];
    const cell = (column: number) => contentX + column * (cellWidth + GRID_GAP);
    let y = 0;
    let at = 0;
    while (at < days.length) {
      const key = groupKeyOf(days[at].day, unit);
      const group: StreamDay[] = [];
      while (at < days.length && groupKeyOf(days[at].day, unit) === key) {
        group.push(days[at]);
        at += 1;
      }
      let slots: Array<{ day: string; slot: number; postKey?: string; top: number | null }> = [];
      let spans: Array<{ cols: number; rows: number }> = [];
      let count = 0;
      let best: number | null = null;
      for (const entry of group) {
        const loaded = posts.get(entry.day);
        const length = dayLength(entry, loaded);
        count += loaded ? loaded.length : entry.count;
        if (entry.best != null && (best == null || entry.best < best)) best = entry.best;
        for (let slot = 0; slot < length; slot += 1) {
          const top = topAt(entry, loaded, slot);
          slots.push({ day: entry.day, slot, postKey: loaded?.[slot]?.key, top });
        }
      }
      if (!slots.length) continue;
      // month: the month's highlights only, best first (measured before not yet measured, newer first among equals)
      let more = 0;
      if (mode === 'month') {
        const budget = HIGHLIGHT_ROWS * columns;
        const ranked = slots
          .map((entry, order) => ({ entry, order }))
          .sort((a, b) => (a.entry.top ?? 1000) - (b.entry.top ?? 1000) || a.order - b.order)
          .map(({ entry }) => entry);
        const bigCells = columns >= 4 ? 4 : 1;
        // everything fits: no "+N" tile; otherwise one cell is kept for it
        const fitsAll = bigCells + (ranked.length - 1) <= budget;
        const shown = fitsAll ? ranked.length : Math.max(1, budget - bigCells);
        slots = ranked.slice(0, shown);
        spans = slots.map((_, index) => (index === 0 && bigCells === 4 ? { cols: 2, rows: 2 } : { cols: 1, rows: 1 }));
        more = ranked.length - slots.length;
        if (more > 0) spans.push({ cols: 1, rows: 1 });
      }
      sections.set(key, { key, unit, count, best });
      const headerHeight = sectionHeaderHeight(width);
      items.push({ kind: 'section', key: `s:${key}`, day: group[0].day, slot: -1, cols: 1, rows: 1, x: contentX, y, w: contentWidth, h: headerHeight });
      y += headerHeight;
      const { places, rows } = mode === 'week' ? arrange(slots.map((entry) => entry.top), columns) : pack(spans, columns);
      const tiles: LayoutItem[] = places.map((place, index) => {
        const box = {
          cols: place.cols,
          rows: place.rows,
          x: cell(place.column),
          y: y + place.row * (cellHeight + GRID_GAP),
          w: place.cols * cellWidth + (place.cols - 1) * GRID_GAP,
          h: place.rows * cellHeight + (place.rows - 1) * GRID_GAP,
        };
        // the last place, past the highlights: the rest of the month
        if (index >= slots.length) return { kind: 'more' as const, key: `m:${key}`, day: group[0].day, slot: -1, more, ...box };
        return { kind: 'tile' as const, key: `t:${slots[index].day}:${slots[index].slot}`, day: slots[index].day, slot: slots[index].slot, postKey: slots[index].postKey, ...box };
      });
      tiles.sort((a, b) => a.y - b.y || a.x - b.x);
      items.push(...tiles);
      y += rows * (cellHeight + GRID_GAP) - GRID_GAP + SECTION_GAP;
    }
    totalHeight = y + insets.bottom + BOTTOM_PAD;
  }

  const bottoms = new Float64Array(items.length);
  const byPost = new Map<string, number>();
  const byKey = new Map<string, number>();
  let reach = Number.NEGATIVE_INFINITY;
  items.forEach((item, index) => {
    reach = Math.max(reach, item.y + item.h);
    bottoms[index] = reach;
    byKey.set(item.key, index);
    if (item.postKey) byPost.set(item.postKey, index);
  });

  return {
    mode,
    width,
    height,
    insets,
    items,
    totalHeight,
    columns,
    gap: GRID_GAP,
    contentX,
    contentWidth,
    cellWidth,
    cellHeight,
    sections,
    focusY: day.focusY,
    stride: day.stride,
    cardWidth: day.cardWidth,
    cardHeight: day.cardHeight,
    mediaWidth: mode === 'today' ? day.mediaWidth : null,
    bottoms,
    byPost,
    byKey,
  };
}

// the fixed placeholders drawn before the index arrives (no data, no shimmer)
export function skeletonLayout(mode: FeedMode, width: number, height: number, insets: StreamInsets): StreamLayout {
  const columns = mode === 'today' ? 3 : gridColumns(mode, width);
  const tops = (count: number) => Array.from({ length: count }, () => null);
  const days: StreamDay[] = [
    { day: '2000-01-02', count: columns * 2, best: null, covers: [], tops: tops(columns * 2) },
    { day: '2000-01-01', count: columns * 4, best: null, covers: [], tops: tops(columns * 4) },
  ];
  return computeLayout({ mode, days, posts: new Map(), width, height, insets });
}

/* ── lookups ───────────────────────────────────────────────── */

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

// the items overlapping [y0, y1): a contiguous run of the sorted list, as [from, to)
export function itemRange(layout: StreamLayout, y0: number, y1: number): [number, number] {
  const { items, bottoms } = layout;
  let lo = 0;
  let hi = items.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (bottoms[mid] > y0) hi = mid;
    else lo = mid + 1;
  }
  const from = lo;
  hi = items.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (items[mid].y < y1) lo = mid + 1;
    else hi = mid;
  }
  return [from, Math.max(from, lo)];
}

// the item at y (or, in a gap, the next one below it)
export function itemIndexAt(layout: StreamLayout, y: number): number {
  const { items } = layout;
  if (!items.length) return -1;
  const [from, to] = itemRange(layout, y, y + 1);
  for (let index = from; index < to; index += 1) {
    const item = items[index];
    if (item.y <= y && item.y + item.h > y) return index;
  }
  return Math.min(from, items.length - 1);
}

// the days whose posts overlap [y0, y1) (in display order)
export function daysInRange(layout: StreamLayout, y0: number, y1: number): string[] {
  const [from, to] = itemRange(layout, y0, y1);
  const seen = new Set<string>();
  const days: string[] = [];
  for (let index = from; index < to; index += 1) {
    const day = layout.items[index].day;
    if (day && !seen.has(day)) {
      seen.add(day);
      days.push(day);
    }
  }
  return days;
}

/* ── the Day view's column ────────────────────────────────────
   Positions here are the layer's: `top` is the page's scroll less the layer's origin. A post rests when its centre is
   on the focus line. */

// where the layer's scroll (top) is when post i rests on the focus line
export function dayRestTop(layout: StreamLayout, index: number): number {
  const item = layout.items[clamp(index, 0, Math.max(0, layout.items.length - 1))];
  return item ? item.y + item.h / 2 - layout.focusY : 0;
}

// the post nearest the focus line at a scroll (0 when the profile is in view: see dayAtProfile)
export function dayIndexAt(layout: StreamLayout, top: number): number {
  if (!layout.items.length || layout.stride <= 0) return 0;
  return clamp(Math.round((top - dayRestTop(layout, 0)) / layout.stride), 0, layout.items.length - 1);
}

// the profile, not a post, is what is in view: the scroll is more than half a post above the first post's rest
export function dayAtProfile(layout: StreamLayout, top: number): boolean {
  return top < dayRestTop(layout, 0) - layout.stride / 2;
}

// a row's 4:5 media frame (wide Day), in the layer's coordinates: at the row's left, its whole height
export function rowMediaBox(layout: StreamLayout, item: LayoutItem): { x: number; y: number; w: number; h: number } {
  return { x: item.x, y: item.y, w: layout.mediaWidth ?? item.w, h: item.h };
}

// a post's item in a layout, by its identity across zooms (day:slot)
export function indexOfSlot(layout: StreamLayout, id: string): number {
  return layout.byKey.get(`${layout.mode === 'today' ? 'r' : 't'}:${id}`) ?? -1;
}

export function idOf(item: LayoutItem): string | null {
  if ((item.kind === 'tile' || item.kind === 'reel') && item.day) return slotId(item.day, item.slot);
  return null;
}

// the post nearest a point (layout coordinates)
export function nearestPost(layout: StreamLayout, x: number, y: number, reach: number): number {
  const [from, to] = itemRange(layout, y - reach, y + reach);
  let best = -1;
  let bestDistance = Number.POSITIVE_INFINITY;
  for (let index = from; index < to; index += 1) {
    const item = layout.items[index];
    if (item.kind !== 'tile' && item.kind !== 'reel') continue;
    const dx = item.x + item.w / 2 - x;
    const dy = item.y + item.h / 2 - y;
    const distance = dx * dx + dy * dy;
    if (distance < bestDistance) {
      bestDistance = distance;
      best = index;
    }
  }
  return best;
}

/* ── holding still ─────────────────────────────────────────────
   When the layout changes under the reader (a day loads with sizes its index didn't have, a refresh, a rotation),
   the piece at the reading line keeps its place on screen. Offsets are in layout coordinates. */

export type StreamAnchor = { key: string; postKey: string | null; id: string | null; offset: number };

export function anchorAt(layout: StreamLayout, top: number, line: number): StreamAnchor | null {
  if (!layout.items.length) return null;
  const index = layout.mode === 'today' ? dayIndexAt(layout, top) : itemIndexAt(layout, top + line);
  const item = layout.items[index];
  if (!item) return null;
  // the Day view: how far the scroll sits from the post's rest, so a swipe in flight is never pulled back to it
  const offset = layout.mode === 'today' ? dayRestTop(layout, index) - top : item.y - top;
  return { key: item.key, postKey: item.postKey ?? null, id: idOf(item), offset };
}

// where the anchor now is: its new top (layout coordinates), or null when it is gone
export function topForAnchor(layout: StreamLayout, anchor: StreamAnchor): number | null {
  let index = anchor.postKey ? layout.byPost.get(anchor.postKey) : undefined;
  if (index == null) index = layout.byKey.get(anchor.key);
  if (index == null && anchor.id) index = indexOfSlot(layout, anchor.id);
  if (index == null || index < 0) return null;
  if (layout.mode === 'today') return dayRestTop(layout, index) - anchor.offset;
  return layout.items[index].y - anchor.offset;
}

/* ── keeping your place in time ────────────────────────────────
   A new pick, a new order, or a zoom whose post isn't in the new page (MONTH shows highlights only) keeps the day
   you were on rather than going back to the top. */

// the day at the reading line (null: still in the profile, above the posts)
export function dayAt(layout: StreamLayout, top: number, line: number): string | null {
  if (!layout.items.length) return null;
  if (layout.mode === 'today') {
    if (dayAtProfile(layout, top)) return null;
    return layout.items[dayIndexAt(layout, top)]?.day ?? null;
  }
  if (top + line < 0) return null;
  const item = layout.items[itemIndexAt(layout, top + line)];
  return item ? item.day : null;
}

// where that day now is: the first post on it (or the nearest older day); in a grid, the header of the group holding
// it (or the nearest older group). Past the end: the last one
export function indexForDay(layout: StreamLayout, day: string): number {
  const { items } = layout;
  if (layout.mode === 'today') {
    let last = -1;
    for (let index = 0; index < items.length; index += 1) {
      const item = items[index];
      if (!item.day) continue;
      last = index;
      if (item.day <= day) return index;
    }
    return last;
  }
  const unit = ZOOM_GROUP[layout.mode];
  const wanted = groupKeyOf(day, unit);
  let last = -1;
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    if (item.kind !== 'section' || !item.day) continue;
    last = index;
    if (groupKeyOf(item.day, unit) <= wanted) return index;
  }
  return last;
}

/* ── what to mount ─────────────────────────────────────────────
   The Day view: the post in view and two either side (one either side for a first frame). The grids: the screen and
   a margin above and below it, narrower the denser the zoom. A first frame (narrow: a mount, a new scope, a zoom) is
   the screen alone. `top` is the layer's scroll. */

const MARGIN: Record<GridMode, number> = { week: 1, month: 0.75 };

export function mountWindow(layout: StreamLayout, top: number, height: number, narrow: boolean): { y0: number; y1: number } {
  if (layout.mode === 'today') {
    if (!layout.items.length) return { y0: 0, y1: height };
    const at = dayIndexAt(layout, top);
    const reach = narrow ? 1 : 2;
    const first = layout.items[Math.max(0, at - reach)];
    const last = layout.items[Math.min(layout.items.length - 1, at + reach)];
    return { y0: Math.min(first.y, top), y1: Math.max(last.y + last.h, top + height) };
  }
  if (narrow) return { y0: top, y1: top + height };
  const margin = height * MARGIN[layout.mode];
  return { y0: top - margin, y1: top + height + margin };
}
