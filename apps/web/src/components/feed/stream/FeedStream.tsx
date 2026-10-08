'use client';

/* ─────────────────────────────────────────────────────────────
   FEED STREAM — the page. The document itself scrolls (as on Lead
   and Read), so content runs under Safari's translucent bars and
   the toolbar folds away as you read. Under the page's profile
   (the same element in every view), the posts are laid out by the
   pure engine (lib/feedStream/layout) as absolute boxes in one
   layer, and only the ones near the screen are mounted.

   Every post is one element (PostCell), keyed by the post, in
   every view: WEEK and MONTH (grids), and the Day view — a phone's
   one-swipe-one-post cards (the page snaps), a wide screen's
   column of rows scrolled freely, the row on the focus line in the
   spotlight (its reading in, the rest dimmed).

   A move between views (a zoom: the header's control, a pinch, a
   tap on a tile, a key; a new pick or order) is FLIP on those same
   elements: what is on screen is measured, the new view is laid
   out and drawn in one synchronous commit (the old view's leftover
   cells and chrome drawn as they were), the scroll goes where the
   new view opens, and before the first paint every piece is
   carried from where it was, or leaves, or arrives
   (lib/feedStream/motion). Nothing is copied but a departing
   profile, nothing waits on a picture mid-move, nothing heavy runs
   while it moves.

   Nothing reads layout while scrolling: the scroll handler reads
   window.scrollY once a frame and only sets state when what is
   mounted, loading or in view changes. Leaving the tab keeps your
   place; coming back restores it before the first paint.
   ───────────────────────────────────────────────────────────── */

import '@/components/feed/stream/stream.css';
import {
  memo,
  useCallback,
  useEffect,
  useImperativeHandle,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type ReactNode,
  type Ref,
} from 'react';
import { flushSync } from 'react-dom';
import DayStamp from '@/components/feed/stream/DayStamp';
import MoreTile from '@/components/feed/stream/MoreTile';
import PostCell, { PlaceholderCell, type CellShape } from '@/components/feed/stream/PostCell';
import ProfileHeader from '@/components/feed/stream/ProfileHeader';
import SectionHeader from '@/components/feed/stream/SectionHeader';
import { profileHeaderHeight } from '@/components/feed/stream/profileHeaderSize';
import {
  groupLabel,
  groupParts,
  highlightOf,
  istDay,
  mediaProxyUrl,
  scopeKey,
  shiftDay,
  type FeedMode,
  type FeedOrder,
  type StreamFeeder,
  type StreamIndex,
  type StreamPost,
  type StreamScope,
} from '@/lib/feedStream/contract';
import {
  PROFILE_GAP,
  anchorAt,
  computeLayout,
  contentBox,
  dayAt,
  dayAtProfile,
  dayIndexAt,
  dayRestTop,
  daysInRange,
  idOf,
  indexForDay,
  indexOfSlot,
  itemRange,
  mountWindow,
  nearestPost,
  skeletonLayout,
  slotId,
  topForAnchor,
  type LayoutItem,
  type StreamInsets,
  type StreamLayout,
} from '@/lib/feedStream/layout';
import {
  CARRY_MS,
  carry,
  endFlight,
  enter,
  flightDone,
  leave,
  onScreen,
  rectOf,
  reducedMotion,
  releaseFlight,
  run,
  startFlight,
  type Flight,
  type Rect,
} from '@/lib/feedStream/motion';
import { getDayPosts, peekStreamIndex, prefetchDays, subscribeStream, useDayPosts } from '@/lib/feedStream/store';
import { isWarm, warmAhead, warmPictures } from '@/lib/feedStream/warm';
import { usePinch, type PinchDirection, type PinchFocal } from '@/lib/feedStream/usePinch';
import { breakpointOf, defaultModeFor, readStoredMode, springBack, stepMode, toggledMode, writeStoredMode, type ViewBreakpoint } from '@/lib/feedStream/zoom';
import { useAppHaptics } from '@/lib/haptics';
import { acquireRootPageScroll } from '@/lib/rootScrollMode';
import { pinToScreen, stillFrame, unsnap, whenDrawn } from '@/lib/feedStream/stillFrame';
import { markScrollJump, type ScrollJump } from '@/lib/useCompressedOnScroll';

const useIsoLayoutEffect = typeof window !== 'undefined' ? useLayoutEffect : useEffect;

// the quiet after the last scroll event that counts as settled (when 'scrollend' doesn't come)
const SETTLE_MS = 120;
// the reading line sits this far under the header
const LINE_GAP = 8;
// how long a warm keeps waiting for its days
const WARM_MS = 5000;
// a zoom waits at most this long for posts it opens on that haven't loaded (the screen holds still meanwhile)
const ZOOM_LOAD_MS = 220;
// and at most this long for the pictures of the posts it brings on screen, so they arrive as pictures (not dark tiles
// that develop on the way: on a phone, the zoom's press has warmed them a moment before)
const PICTURE_WAIT_MS = 160;
// a new pick waits at most this long for its posts near the day you're on (the screen holds still meanwhile)
const PICK_WAIT_MS = 450;
// a new pick asks first for the days around the one you were on: this many, the first this many ahead of it
const SEED_DAYS = 18;
const SEED_AHEAD = 3;
// a move into the Day view: the spotlight (the row's reading) comes on this far into it
const SPOT_AFTER_MS = 200;
// a post carried further than this many screens goes only this many screens of the way, fading (out, or in)
const FAR = 1.15;
const PART = 0.4;
// the longest a carry takes (a post crossing the whole screen)
const LONG_CARRY_MS = 760;
// a live pinch past the last zoom only gives this much (a rubber band)
const PINCH_EDGE = 0.35;
// a live pinch this far from 1 has shown which way it is going
const PINCH_WARM_AT = 0.04;
const NO_DAYS: string[] = [];
const SNAP_CLASS = 'fm-feed-root-scroll';
/* WebKit (every iOS browser, Safari on a Mac) draws the stretch of page a scroll the page makes itself lands on only
   once its UI process has scrolled there: a jump further than it has drawn around the screen shows the page's black
   for 2–7 frames. So there a move that lands the page elsewhere holds the scroll (the page drawn where it lands by an
   offset, nothing scrolls) and hands it over when it has landed, under a still copy of the screen (stillFrame) */
const HOLD_FROM_PX = 240;
/* The copy stays until the page under it is drawn: its frames coming at their pace again (at most STILL_SETTLE_MS), and
   at least STILL_HOLD_MS after the scroll was handed over, which is how long WebKit can take to draw a phone's screen of
   pictures where the page now is (measured: ~260ms on the iPhone simulator, a dev build). Then it dissolves (an
   identical page under it: nothing seen), quicker under a finger */
const STILL_SETTLE_MS = 600;
const STILL_HOLD_MS = 450;
// an even dissolve: pictures that finished loading under the copy come up gently, not in one frame
const STILL_OUT_MS = 240;
const STILL_OUT_EASE = 'cubic-bezier(0.4, 0, 0.6, 1)';
const STILL_TOUCH_OUT_MS = 90;
/* A scroll the page makes itself further than this many screens (Feed tapped again, a highlight) goes on WebKit as a
   jump under a copy of the screen, which drifts the way the page goes from the tap on, stays whole while WebKit draws
   where the page now is, then dissolves: a glide that long outruns WebKit's drawing (blank for a moment mid-way) */
const GLIDE_MAX_SCREENS = 1.2;
const STILL_FADE_HOLD_MS = 300;
const STILL_FADE_MS = 280;
const STILL_FADE_DRIFT = 56;
const STILL_FADE_EASE = 'cubic-bezier(0.3, 0, 0.5, 1)';
// under the header (z 100) and the nav (z 180)
const STILL_Z = 90;
/* WebKit: a move's pieces wait at their start (at most this long) until the frame that draws the new view is on screen,
   then all set off together (motion.ts, releaseFlight): their clock would otherwise start before that frame is shown */
const FLIGHT_HOLD_MS = 800;
/* The pictures a scroll is heading for are fetched before their posts go up (warm.ts, warmAhead): a picture takes a
   round trip to /api/media and another to the store (a second or more on a phone's network), and posts only go up a
   screen ahead, so a fling's pictures used to arrive as it slowed down, popping in one by one where the eye could
   finally follow. A fling comes to rest about MOMENTUM_MS of its speed further on (iOS's deceleration, 0.998 a ms):
   the screen there goes first, then the way to it, and AHEAD_SCREENS past either end. A fling heading past the days
   already asked for asks for the landing's days too */
const MOMENTUM_MS = 500;
const AHEAD_SCREENS = 1.5;
// a speed read over a longer gap than this is stale: the page was at rest
const SPEED_GAP_MS = 100;

/* ── types ─────────────────────────────────────────────────── */

// who the feed is about, for its profile (worked out by the tab from the feeds and the pick)
export type StreamProfileView = {
  title: string;
  subtitle: string;
  feeder: StreamFeeder | null;
  feeds: Array<{ id: string; title: string; feederCount: number }>;
  faces: Array<{ handle: string; profilePicUrl: string | null }>;
  fresh: boolean;
};

export type FeedStreamHandle = {
  // zoom (the focal: the post nearest the middle of the screen, or the profile at the top)
  setMode: (mode: FeedMode) => void;
  // back to the newest post (a re-tap on the nav)
  scrollToNewest: () => void;
  // a new pick or order coming: its posts near the day you're on are fetched and their pictures decoded (a moment at
  // most). The screen doesn't change
  prepare: (next: StreamScope) => Promise<void>;
  // ...and shown: the screen as it is now is kept for the move, and apply() (the tab's own state) shows the new pick in
  // the same commit. profile: the profile is someone else's
  commit: (apply: () => void, options: { profile: boolean }) => void;
  // a pick or a zoom about to be taken (a hover, a press): its posts and the pictures it opens on are fetched now
  warm: (next: { scope?: StreamScope; mode?: FeedMode }) => void;
};

// what the stream shows instead of posts (no feeds, no posts, an error), with or without the profile over it
export type FeedStreamBlank = { node: ReactNode; profile: boolean };

export type FeedStreamProps = {
  ref?: Ref<FeedStreamHandle>;
  scope: StreamScope;
  index: StreamIndex | null;
  feeders: ReadonlyMap<string, StreamFeeder>;
  profile: StreamProfileView;
  // the order as chosen (the profile's tabs show it at once; the posts follow once the move is ready)
  order: FeedOrder;
  // installed (the PWA)
  standalone: boolean;
  minHeight: string;
  headerCompressed: boolean;
  blank: FeedStreamBlank | null;
  // keys (G, M, Esc, - +, ↑/↓/J/K) answer only while nothing is open over the stream
  keyboard: boolean;
  onModeChange: (mode: FeedMode) => void;
  onOrderChange: (order: FeedOrder) => void;
  onOpenPost: (post: StreamPost) => void;
  onManage: () => void;
  onLead: () => void;
  onRead: () => void;
};

type Metrics = { width: number; height: number; insets: StreamInsets };
// what is mounted: a band of layer y, for one scope and one zoom (narrow: the screen only, for a first frame)
type Win = { gen: string; mode: FeedMode; y0: number; y1: number; narrow: boolean };
type DayWin = { gen: string; key: string; days: string[] };
// a section header's words (groupLabel / groupParts)
type Heading = { label: string; kicker: string; title: string; first: boolean };

/* One cell to draw, everything it needs: kept from one render to the next, so a move out of a view draws the old
   view's cells as they were */
type Spec =
  | {
      kind: 'post';
      key: string;
      zid: string | null;
      post: StreamPost;
      mode: FeedMode;
      shape: CellShape;
      x: number;
      y: number;
      // its place in the layout (the Day view: the post's place in the column)
      index: number;
      eager: boolean;
      standout: boolean;
      dayStart: string | null;
      feederHandle: string | null;
    }
  | { kind: 'placeholder'; key: string; mode: FeedMode; shape: CellShape; x: number; y: number }
  | { kind: 'section'; key: string; x: number; y: number; w: number; h: number; heading: Heading; count: number; best: number | null; order: FeedOrder }
  | { kind: 'more'; key: string; x: number; y: number; w: number; h: number; day: string; more: number };

// where a cell and its layers were on screen
type Shot = Partial<Record<'cell' | 'frame' | 'bg' | 'chips' | 'chrome' | 'reading', Rect>>;

// a move between views in progress: what the screen was, and where the page goes
type Move = {
  id: number;
  kind: 'zoom' | 'pick';
  from: FeedMode;
  to: FeedMode;
  shots: Map<string, Shot>;
  profile: Rect | null;
  leaving: Spec[];
  // where the move centres on the old screen (what was tapped, pinched, or the middle): what leaves goes away from it
  centre: { x: number; y: number };
  focalKey: string | null;
  // the page's scroll once landed, and the layer's origin it was worked out for
  scroll: number;
  origin: number;
  // a pick for someone else: the old profile's copy, going
  profileCopy: HTMLElement | null;
};

type ZoomOptions = { focal?: { zid?: string | null; postKey?: string | null }; point?: { x: number; y: number }; remember?: boolean; waited?: boolean };

type PieceActions = {
  openTile: (post: StreamPost, element: HTMLElement) => void;
  openCard: (post: StreamPost) => void;
  // a MONTH group's "+N": WEEK, at that month's newest week
  openWeek: (day: string, element: HTMLElement) => void;
  manage: () => void;
  lead: () => void;
  read: () => void;
  orderChange: (order: FeedOrder) => void;
  openHighlight: (key: string) => void;
};

/* ── helpers ───────────────────────────────────────────────── */

function readingLine(insets: StreamInsets, folded: boolean): number {
  return (folded ? insets.topCompressed : insets.top) + LINE_GAP;
}

// an element's top in the page, from the offset chain (transforms, like a live pinch, don't count)
function pageTop(el: HTMLElement): number {
  let top = 0;
  for (let node: HTMLElement | null = el; node; node = node.offsetParent as HTMLElement | null) top += node.offsetTop;
  return top;
}

function sameInsets(a: StreamInsets, b: StreamInsets) {
  return Math.abs(a.top - b.top) < 0.5 && Math.abs(a.topCompressed - b.topCompressed) < 0.5 && Math.abs(a.bottom - b.bottom) < 0.5;
}

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));
const sleep = (ms: number) => new Promise<void>((resolve) => window.setTimeout(resolve, ms));

let webkitScrolling: boolean | null = null;
function holdsScroll(): boolean {
  if (webkitScrolling == null) {
    const ua = typeof navigator === 'undefined' ? '' : navigator.userAgent;
    const ios = /iP(hone|od|ad)/.test(ua) || (/Macintosh/.test(ua) && typeof navigator !== 'undefined' && navigator.maxTouchPoints > 1);
    const safari = /AppleWebKit/.test(ua) && !/Chrome|Chromium|CriOS|Edg|OPR|Firefox|FxiOS/.test(ua);
    webkitScrolling = ios || safari;
  }
  return webkitScrolling;
}

/* A live pinch shrinks the page about the fingers: around it shows what is behind it, the app's page (white in the light
   theme). For as long as it is shrunk, what is behind it is the stream's own black */
const backdrops = new WeakMap<HTMLElement, string>();
function pinchBackdrop(root: HTMLElement | null, on: boolean) {
  const parent = root?.parentElement;
  if (!root || !parent) return;
  if (on) {
    if (!backdrops.has(parent)) backdrops.set(parent, parent.style.backgroundColor);
    parent.style.backgroundColor = getComputedStyle(root).backgroundColor;
  } else if (backdrops.has(parent)) {
    parent.style.backgroundColor = backdrops.get(parent) ?? '';
    backdrops.delete(parent);
  }
}

// the page's snap, held off (after a scroll the page made itself, or a move): inline, over html.fm-feed-root-scroll
function holdSnapStyle(hold: boolean) {
  const value = hold ? 'none' : '';
  document.documentElement.style.scrollSnapType = value;
  document.body.style.scrollSnapType = value;
}

// the loaded days of a scope, as the layout wants them
function loadedPosts(scope: StreamScope, index: StreamIndex): Map<string, StreamPost[]> {
  const posts = new Map<string, StreamPost[]>();
  for (const entry of index.days) {
    const loaded = getDayPosts(scope, entry.day);
    if (loaded) posts.set(entry.day, loaded);
  }
  return posts;
}

/* What a view would show first: the posters near where it opens (a post, or a day) for a scope at a zoom, and the days
   it needs that haven't loaded. Laid out exactly as the page would be, so a pick or a zoom can have its pictures ready
   before anything moves. null: the scope's index isn't here yet */
function viewPictures(scope: StreamScope, mode: FeedMode, size: Metrics, place: { day: string | null; zid: string | null }): { srcs: string[]; missing: string[] } | null {
  const index = peekStreamIndex(scope);
  if (!index) return null;
  const posts = loadedPosts(scope, index);
  const layout = computeLayout({ mode, days: index.days, posts, width: size.width, height: size.height, insets: size.insets });
  const focal = place.zid ? indexOfSlot(layout, place.zid) : -1;
  const at = focal >= 0 ? focal : place.day ? Math.max(0, indexForDay(layout, place.day)) : 0;
  const item = layout.items[at];
  if (!item) return { srcs: [], missing: [] };
  const [first, last] = itemRange(layout, item.y + item.h / 2 - size.height, item.y + item.h / 2 + size.height);
  const srcs: string[] = [];
  const missing = new Set<string>();
  for (let place2 = first; place2 < last; place2 += 1) {
    const entry = layout.items[place2];
    if ((entry.kind !== 'tile' && entry.kind !== 'reel') || !entry.day) continue;
    const loaded = posts.get(entry.day);
    if (!loaded) {
      missing.add(entry.day);
      continue;
    }
    const src = loaded[entry.slot]?.thumbnailUrl;
    if (src) srcs.push(src);
  }
  return { srcs, missing: [...missing] };
}

/* Resolves once a view is ready to be shown (its days in, the pictures it opens on decoded) or the budget is spent */
function viewReady(scope: StreamScope, mode: FeedMode, size: Metrics, place: { day: string | null; zid: string | null }, budget: number): Promise<void> {
  return new Promise((resolve) => {
    const started = performance.now();
    let done = false;
    let unsubscribe: (() => void) | null = null;
    let timer = 0;
    const finish = () => {
      if (done) return;
      done = true;
      unsubscribe?.();
      window.clearTimeout(timer);
      resolve();
    };
    const check = () => {
      const view = viewPictures(scope, mode, size, place);
      if (!view) return;
      if (view.missing.length) {
        prefetchDays(scope, view.missing);
        return;
      }
      unsubscribe?.();
      unsubscribe = null;
      void warmPictures(view.srcs, Math.max(0, budget - (performance.now() - started))).then(finish);
    };
    timer = window.setTimeout(finish, budget);
    unsubscribe = subscribeStream(check);
    check();
  });
}

// where a post sits in the index: a loaded day holding it, a day whose covers name it, or the one day whose tops hold
// its top % (its slot is that top's place, in display order)
function locatePost(index: StreamIndex, scope: StreamScope, key: string, topPercent: number | null): { day: string; slot: number } | null {
  for (const day of index.days) {
    const loaded = getDayPosts(scope, day.day);
    const slot = loaded ? loaded.findIndex((post) => post.key === key) : -1;
    if (slot >= 0) return { day: day.day, slot };
  }
  for (const day of index.days) {
    const cover = day.covers.find((candidate) => candidate.key === key);
    if (!cover) continue;
    const slot = Array.isArray(day.tops) ? day.tops.indexOf(cover.topPercent) : -1;
    return { day: day.day, slot: Math.max(0, slot) };
  }
  if (topPercent == null) return null;
  for (const day of index.days) {
    const slot = Array.isArray(day.tops) ? day.tops.indexOf(topPercent) : -1;
    if (slot >= 0) return { day: day.day, slot };
  }
  return null;
}

// the days a view wants loaded at a scroll (the layer's top): three screens either way, as the scroll asks for them
function wantedDays(gen: string, layout: StreamLayout, top: number, height: number): DayWin {
  const days = daysInRange(layout, top - height * 3, top + height * 4);
  return { gen, key: `${gen}|${days.join(',')}`, days };
}

/* Where every cell (and each of its layers) is on screen now, live moves and a pinch included */
function snapshot(layer: HTMLElement): Map<string, Shot> {
  const shots = new Map<string, Shot>();
  layer.querySelectorAll<HTMLElement>(':scope > [data-cell]').forEach((el) => {
    if (el.dataset.gone !== undefined) return;
    const shot: Shot = { cell: rectOf(el) };
    el.querySelectorAll<HTMLElement>(':scope > [data-layer], :scope > [data-layer="frame"] > [data-layer]').forEach((part) => {
      const name = part.dataset.layer ?? '';
      if (name === 'frame' || name === 'bg' || name === 'chips' || name === 'chrome' || name === 'reading') shot[name] = rectOf(part);
    });
    shots.set(el.dataset.cell ?? '', shot);
  });
  return shots;
}

const centreOf = (rect: Rect) => ({ x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 });

// a rect `fraction` of the way from `from` to `to`
function partway(from: Rect, to: Rect, fraction: number): Rect {
  return {
    left: from.left + (to.left - from.left) * fraction,
    top: from.top + (to.top - from.top) * fraction,
    width: from.width + (to.width - from.width) * fraction,
    height: from.height + (to.height - from.height) * fraction,
  };
}

/* A layer carried from where it was to where it is now: the whole way when that is near, only part of the way (going,
   or coming, fading) when one end is far off screen */
function carryLayer(flight: Flight, el: HTMLElement, was: Rect, vw: number, vh: number, picture: HTMLElement | null = null, now: Rect = rectOf(el)) {
  const before = onScreen(was, vw, vh);
  const after = onScreen(now, vw, vh);
  if (!before && !after) return;
  const a = centreOf(was);
  const b = centreOf(now);
  const distance = Math.hypot(b.x - a.x, b.y - a.y);
  // on screen at both ends, it makes the whole trip however far (part of the way would end in a pop); a long trip
  // takes a little longer, so nothing ever streaks across the screen
  if (distance <= FAR * vh || (before && after)) {
    carry(flight, el, now, was, now, { picture, duration: Math.min(LONG_CARRY_MS, CARRY_MS + Math.max(0, distance - 480) * 0.3) });
    return;
  }
  const fraction = (PART * vh) / distance;
  if (before) carry(flight, el, now, was, partway(was, now, fraction), { picture, fade: 'out' });
  else carry(flight, el, now, partway(now, was, fraction), now, { picture, fade: 'in' });
}

/* A layer of the view being left, held where it stood and let go */
function letGo(flight: Flight, el: HTMLElement, was: Rect | undefined, now: Rect = rectOf(el)) {
  if (!was) return;
  leave(flight, el, now, was, null, { quick: true });
}

/* ── the stream ────────────────────────────────────────────── */

function FeedStream({
  ref,
  scope,
  index,
  feeders,
  profile,
  order,
  minHeight,
  headerCompressed,
  blank,
  keyboard,
  onModeChange,
  onOrderChange,
  onOpenPost,
  onManage,
  onLead,
  onRead,
}: FeedStreamProps) {
  const gen = scopeKey(scope);
  // the scope without its order: the profile stays through a change of order
  const who = scopeKey({ feedId: scope.feedId, handle: scope.handle });
  const shownOrder: FeedOrder = scope.order ?? 'posted';
  const { play } = useAppHaptics();

  const rootRef = useRef<HTMLDivElement | null>(null);
  const layerRef = useRef<HTMLDivElement | null>(null);
  const probeTopRef = useRef<HTMLDivElement | null>(null);
  const probeFoldRef = useRef<HTMLDivElement | null>(null);
  const probeNavRef = useRef<HTMLDivElement | null>(null);
  const probeViewRef = useRef<HTMLDivElement | null>(null);
  const [profileEl, setProfileEl] = useState<HTMLDivElement | null>(null);

  const [metrics, setMetrics] = useState<Metrics | null>(null);
  // where the layer starts in the page (under the profile)
  const [origin, setOrigin] = useState(0);
  const [mode, setMode] = useState<FeedMode | null>(null);
  const [win, setWin] = useState<Win | null>(null);
  const [dayWin, setDayWin] = useState<DayWin | null>(null);
  // a new pick's days, asked for before it has a window of its own
  const [seed, setSeed] = useState<{ gen: string; days: string[] } | null>(null);
  // the Day view: the post in view (on a phone, the card at rest; on a wide screen, the row on the focus line), and
  // whether the profile (not a post) is what is in view
  const [active, setActive] = useState(0);
  const [atProfile, setAtProfile] = useState(true);
  // the wide Day view's spotlight is on (off for a moment as a move lands)
  const [spotOn, setSpotOn] = useState(true);
  const [move, setMove] = useState<Move | null>(null);

  // what the handlers read (kept current after every commit, never written during render)
  const layoutRef = useRef<StreamLayout | null>(null);
  const metricsRef = useRef<Metrics | null>(null);
  const originRef = useRef(0);
  const modeRef = useRef<FeedMode | null>(null);
  const genRef = useRef(gen);
  const inputsRef = useRef<{ index: StreamIndex | null; posts: ReadonlyMap<string, StreamPost[]>; scope: StreamScope }>({ index: null, posts: new Map(), scope });
  const compressedRef = useRef(headerCompressed);
  const specsRef = useRef<Spec[]>([]);
  const renderedRef = useRef<{ layout: StreamLayout; gen: string; from: number; to: number } | null>(null);
  const dayWinRef = useRef<DayWin | null>(null);
  const wideRef = useRef<{ layout: StreamLayout; from: number; to: number } | null>(null);
  // the scroll's speed (px a ms, read once a frame) and the pictures last asked for ahead of it
  const speedRef = useRef({ y: 0, t: 0, v: 0 });
  const aheadRef = useRef<{ layout: StreamLayout; from: number; to: number; land: number } | null>(null);
  const callbacksRef = useRef({ onModeChange, onOpenPost, onManage, onLead, onRead, onOrderChange, play });
  const savedYRef = useRef(0);
  const visibleRef = useRef(false);
  const touchingRef = useRef(false);
  // a scroll in progress the page didn't jump itself: a finger and its momentum (from the touch), or a glide (from its
  // start), until the scroll settles
  const scrollingRef = useRef(false);
  const activeRef = useRef(0);
  const atProfileRef = useRef(true);
  const movingRef = useRef(false);
  const settledTopRef = useRef(0);
  const moveRef = useRef<Move | null>(null);
  const moveSeqRef = useRef(0);
  const flightRef = useRef<Flight | null>(null);
  const spotTimerRef = useRef(0);
  const landedRef = useRef(0);
  const pinchRef = useRef<{ focal: PinchFocal; scale: number } | null>(null);
  const prevRef = useRef<{ layout: StreamLayout; origin: number } | null>(null);
  const breakpointRef = useRef<ViewBreakpoint | null>(null);
  const snapOnRef = useRef(false);
  const snapHeldRef = useRef(false);
  // a move holding the scroll (WebKit): how far the page is drawn from where it is, to hand over when it lands
  const handoverRef = useRef<{ by: number; mode: ScrollJump } | null>(null);
  const stillRef = useRef<{ remove: () => void; follow: () => void } | null>(null);
  const zoomToRef = useRef<((next: FeedMode, options?: ZoomOptions) => void) | null>(null);
  const zoomWaitRef = useRef<object | null>(null);
  const warmingRef = useRef(new Map<string, () => void>());
  // a new pick being made ready: it, and where it opens (the day you were on, or where you are in the profile)
  const pickRef = useRef<{ next: StreamScope; day: string | null; keepScroll: boolean } | null>(null);
  const seekRef = useRef<{ key: string; until: number } | null>(null);
  const arrivedRef = useRef(false);

  /* ── data ── */

  // the days whose posts are wanted: the window's, or (a new pick's first frames, before it has a window) its seed
  const wantDays = dayWin && dayWin.gen === gen ? dayWin.days : seed && seed.gen === gen ? seed.days : NO_DAYS;
  const posts = useDayPosts(scope, wantDays);

  const layout = useMemo(
    () => (mode && metrics && index && !blank ? computeLayout({ mode, days: index.days, posts, width: metrics.width, height: metrics.height, insets: metrics.insets }) : null),
    [mode, metrics, index, posts, blank],
  );
  const skeleton = useMemo(
    () => (!index && !blank && mode && metrics ? skeletonLayout(mode, metrics.width, metrics.height, metrics.insets) : null),
    [index, blank, mode, metrics],
  );
  const headings = useMemo(() => {
    const today = istDay();
    const map = new Map<string, Heading>();
    const first = layout?.items.find((item) => item.kind === 'section')?.key.slice(2) ?? null;
    layout?.sections.forEach((section) => map.set(section.key, {
      label: groupLabel(section.key, section.unit, today),
      ...groupParts(section.key, section.unit, today),
      first: section.key === first,
    }));
    return map;
  }, [layout]);
  // the first post is "Today's standout" when it is the newest day's highlight
  const firstStandout = useMemo(() => {
    const newest = index?.days[0];
    const dayPosts = newest ? posts.get(newest.day) : undefined;
    if (!dayPosts?.length) return false;
    const highlight = highlightOf(dayPosts);
    return highlight != null && highlight.key === dayPosts[0].key;
  }, [index, posts]);

  const drawn = layout ?? skeleton;
  const mountView = drawn && metrics
    ? (layout && win && win.gen === gen && win.mode === layout.mode ? win : mountWindow(drawn, 0, metrics.height, true))
    : null;
  const [mountFrom, mountTo] = drawn && mountView ? itemRange(drawn, mountView.y0, mountView.y1) : [0, 0];

  /* ── the snap (a phone's Day view) ── */

  const applySnap = useCallback((on: boolean) => {
    snapOnRef.current = on;
    document.documentElement.classList.toggle(SNAP_CLASS, on);
  }, []);

  /* The snap comes on only under a finger: held (inline none) whenever the page turns it on or scrolls itself, and let
     back by the next touch that moves the page (takeSnap). iOS Safari keeps the card the page rests on as a place in its
     list of snap points (the fourth one), set when a swipe or a scroll ends with the snap on, and whenever the page is
     still it puts the page back on that place in the list as it now stands. A scroll made with the snap held never sets
     it, and the list changes as cards mount: let back while still, the page is pulled to whatever card is fourth now
     (WebKit: WKWebView _createVisibleContentRectUpdate, scrollOffsetSnappedToNearestSnapPoint). Let back during a
     swipe, the swipe's end sets it afresh */
  const holdSnap = useCallback(() => {
    snapHeldRef.current = true;
    holdSnapStyle(true);
  }, []);

  /* A move holding the scroll (WebKit), handed over: the offset it was drawn at comes off and the page scrolls by as
     much, so nothing moves on screen. When it lands, under a still copy of the screen; anything that needs the page
     where it really is (another move, a scroll, a pick) takes it over at once first */
  const handOver = useCallback(() => {
    const pending = handoverRef.current;
    if (!pending) return;
    handoverRef.current = null;
    const root = rootRef.current;
    if (root) root.style.top = '';
    markScrollJump(pending.mode);
    holdSnap();
    window.scrollTo(0, window.scrollY + pending.by);
    stillRef.current?.follow();
    savedYRef.current = window.scrollY;
    settledTopRef.current = window.scrollY;
  }, [holdSnap]);

  const dropStill = useCallback(() => {
    stillRef.current?.remove();
    stillRef.current = null;
  }, []);

  /* Through a move the page never gets shorter under the scroll: a view shorter than where the page is would have the
     browser pull the scroll up the moment it is drawn (a jump the page didn't make, blank on WebKit). The page's height
     is kept until the move has landed; the scroll it lands at is always inside the new view's own */
  const keepHeight = useCallback(() => {
    const layer = layerRef.current;
    if (layer) layer.style.minHeight = `${layer.offsetHeight}px`;
  }, []);
  const releaseHeight = useCallback(() => {
    const layer = layerRef.current;
    if (layer) layer.style.minHeight = '';
  }, []);

  // a scroll the page makes itself
  const scrollPage = useCallback((top: number, smooth: boolean) => {
    handOver();
    holdSnap();
    if (smooth) scrollingRef.current = true;
    window.scrollTo({ top, behavior: smooth ? 'smooth' : 'auto' });
    stillRef.current?.follow();
  }, [handOver, holdSnap]);

  /* ── the scroll ── */

  // once a frame while scrolling: what to mount, what to load, which post is in view. A move holds it all still
  const syncFrame = useCallback((scrollY: number, keepNarrow = false) => {
    const current = layoutRef.current;
    const size = metricsRef.current;
    if (!current || !size || moveRef.current) return;
    const scopeGen = genRef.current;
    const top = scrollY - originRef.current;
    const view = mountWindow(current, top, size.height, keepNarrow);
    const [from, to] = itemRange(current, view.y0, view.y1);
    const rendered = renderedRef.current;
    if (!rendered || rendered.layout !== current || rendered.gen !== scopeGen || rendered.from !== from || rendered.to !== to) {
      renderedRef.current = { layout: current, gen: scopeGen, from, to };
      setWin({ gen: scopeGen, mode: current.mode, y0: view.y0, y1: view.y1, narrow: keepNarrow });
    }
    // the days within three screens either way are asked for (worked out again only when that range moves)
    const [wideFrom, wideTo] = itemRange(current, top - size.height * 3, top + size.height * 4);
    const wide = wideRef.current;
    if (!wide || wide.layout !== current || wide.from !== wideFrom || wide.to !== wideTo) {
      wideRef.current = { layout: current, from: wideFrom, to: wideTo };
      const days = daysInRange(current, top - size.height * 3, top + size.height * 4);
      const key = `${scopeGen}|${days.join(',')}`;
      if (dayWinRef.current?.key !== key) {
        dayWinRef.current = { gen: scopeGen, key, days };
        setDayWin(dayWinRef.current);
      }
    }
    if (current.mode !== 'today') return;
    const inProfile = dayAtProfile(current, top);
    if (inProfile !== atProfileRef.current) {
      atProfileRef.current = inProfile;
      setAtProfile(inProfile);
    }
    if (current.mediaWidth != null) {
      // a wide Day view scrolls freely: the row on the focus line is the one in view, as it goes
      const at = dayIndexAt(current, top);
      if (at !== activeRef.current) {
        activeRef.current = at;
        setActive(at);
      }
    } else if (!movingRef.current && Math.abs(scrollY - settledTopRef.current) > 2) {
      movingRef.current = true;
    }
  }, []);

  /* the pictures the scroll is heading for, fetched ahead (MOMENTUM_MS): where it will come to rest first, then the way
     there. `measured` (once a frame while scrolling) reads the speed; otherwise (the posts changed under a still page)
     the last speed stands. Asked again only when the range, or the landing (to a quarter screen), moves */
  const lookAhead = useCallback((scrollY: number, measured: boolean) => {
    const current = layoutRef.current;
    const size = metricsRef.current;
    if (!current || !size || moveRef.current) return;
    const speed = speedRef.current;
    if (measured) {
      const now = performance.now();
      const gap = now - speed.t;
      const fresh = gap > 0 && gap < SPEED_GAP_MS;
      const v = fresh ? (scrollY - speed.y) / gap : 0;
      // smoothed (frames come unevenly), though a scroll starting from rest isn't held back by the last one's speed
      speed.v = fresh ? speed.v * 0.5 + v * 0.5 : v;
      speed.y = scrollY;
      speed.t = now;
    }
    const top = scrollY - originRef.current;
    const room = Math.max(0, current.totalHeight - size.height);
    const land = Math.min(room, Math.max(0, top + speed.v * MOMENTUM_MS));
    const y0 = Math.min(top, land) - size.height * AHEAD_SCREENS;
    const y1 = Math.max(top, land) + size.height * (1 + AHEAD_SCREENS);
    const [from, to] = itemRange(current, y0, y1);
    const step = Math.round(land / (size.height / 4));
    const last = aheadRef.current;
    if (last && last.layout === current && last.from === from && last.to === to && last.land === step) return;
    aheadRef.current = { layout: current, from, to, land: step };
    const centre = land + size.height / 2;
    const scope = inputsRef.current.scope;
    const days = new Map<string, StreamPost[] | undefined>();
    const wanted: Array<{ src: string; far: number }> = [];
    for (let at = from; at < to; at += 1) {
      const item = current.items[at];
      if (!item.day || item.slot < 0) continue;
      if (!days.has(item.day)) days.set(item.day, getDayPosts(scope, item.day));
      const post = days.get(item.day)?.[item.slot];
      if (!post) continue;
      const far = Math.abs(item.y + item.h / 2 - centre);
      if (post.thumbnailUrl) wanted.push({ src: post.thumbnailUrl, far });
      // a carousel's first slide goes up over its poster when the Day view comes to rest on it: there already
      const first = current.mode === 'today' && post.slides.length > 1 ? post.slides[0] : null;
      if (first && !first.video) wanted.push({ src: mediaProxyUrl(post.key, first.role), far });
    }
    wanted.sort((a, b) => a.far - b.far);
    warmAhead(wanted.map((entry) => entry.src));
    // a fling going further than the days already asked for (three screens back, four on): the landing's days too
    if (Math.abs(land - top) > size.height * 2) prefetchDays(scope, daysInRange(current, land - size.height, land + size.height * 2));
  }, []);

  // the scroll came to rest: a phone's Day view, the card on the focus line becomes the one in view
  const settle = useCallback(() => {
    const current = layoutRef.current;
    if (!touchingRef.current) scrollingRef.current = false;
    if (!current || !visibleRef.current || touchingRef.current || moveRef.current || pinchRef.current) return;
    const scrollY = window.scrollY;
    savedYRef.current = scrollY;
    syncFrame(scrollY);
    if (current.mode !== 'today' || current.mediaWidth != null) return;
    settledTopRef.current = scrollY;
    movingRef.current = false;
    const at = dayIndexAt(current, scrollY - originRef.current);
    if (at !== activeRef.current) {
      activeRef.current = at;
      setActive(at);
      callbacksRef.current.play('snapLock');
    }
  }, [syncFrame]);

  /* ── moves ── */

  // a live pinch's transform comes off the page
  const releaseLayer = useCallback(() => {
    const root = rootRef.current;
    if (!root) return;
    root.style.transform = '';
    root.style.transformOrigin = '';
    root.style.willChange = '';
    pinchBackdrop(root, false);
  }, []);

  // a move lands (or is cut short): every piece is itself, the old view's leftovers go, the page is live again (its
  // snap, though, still held until a finger moves it: holdSnap). A held scroll is handed over first
  const endMove = useCallback(() => {
    handOver();
    holdSnap();
    releaseHeight();
    endFlight(flightRef.current);
    flightRef.current = null;
    window.clearTimeout(spotTimerRef.current);
    if (!moveRef.current) return;
    moveRef.current.profileCopy?.remove();
    moveRef.current = null;
    setMove(null);
    setSpotOn(true);
    if (visibleRef.current) {
      savedYRef.current = window.scrollY;
      syncFrame(window.scrollY);
    }
  }, [handOver, holdSnap, releaseHeight, syncFrame]);

  // a still copy of the screen over the page (stillFrame); a finger or a wheel lets it go at once (after `onTouch`)
  const coverPage = useCallback((root: HTMLElement, onTouch?: () => void) => {
    dropStill();
    const still = stillFrame(root, { zIndex: STILL_Z });
    const letGo = () => {
      onTouch?.();
      if (stillRef.current === entry) entry.fadeOut(STILL_TOUCH_OUT_MS, 0);
    };
    const listen = { capture: true, passive: true } as const;
    window.addEventListener('touchstart', letGo, listen);
    window.addEventListener('wheel', letGo, listen);
    const unlisten = () => {
      window.removeEventListener('touchstart', letGo, listen);
      window.removeEventListener('wheel', letGo, listen);
    };
    const entry = {
      shown: still.shown,
      follow: still.follow,
      remove: () => {
        unlisten();
        still.remove();
      },
      fadeOut: (ms: number, dy: number, easing?: string, hold?: number) => {
        unlisten();
        if (stillRef.current === entry) stillRef.current = null;
        still.fadeOut(ms, dy, easing, hold);
      },
    };
    stillRef.current = entry;
    return entry;
  }, [dropStill]);

  /* A move that held the scroll lands: a still copy of the screen goes over the page, the scroll is handed over under
     it, and the copy dissolves once WebKit has drawn the page where it now is. A finger or a wheel takes the page back
     at once; another move takes over by itself (zoomTo, commit) */
  const landHeld = useCallback((flight: Flight) => {
    const root = rootRef.current;
    if (!root || !visibleRef.current) {
      endMove();
      return;
    }
    let landed = false;
    const land = () => {
      if (landed) return;
      landed = true;
      if (flightRef.current === flight) endMove();
    };
    const entry = coverPage(root, land);
    void entry.shown.then(async () => {
      if (stillRef.current !== entry) return;
      land();
      const handed = performance.now();
      await whenDrawn(root, STILL_SETTLE_MS);
      await sleep(Math.max(0, STILL_HOLD_MS - (performance.now() - handed)));
      if (stillRef.current === entry) entry.fadeOut(STILL_OUT_MS, 0, STILL_OUT_EASE);
    });
  }, [coverPage, endMove]);

  // where the layer will start for a scope (under the open header, its profile and the gap after it)
  const originFor = useCallback((size: Metrics, forScope: StreamScope) => {
    const root = rootRef.current;
    const column = contentBox(size.width);
    return (root ? pageTop(root) : 0) + size.insets.top + column.topPad + profileHeaderHeight(column.contentWidth, forScope) + PROFILE_GAP;
  }, []);

  /* A zoom. What is on screen is measured where it is (a live pinch included), the new view laid out and drawn in one
     commit at the scroll it opens at, and the move (below) carries every piece from where it was */
  const zoomTo = useCallback((next: FeedMode, options: ZoomOptions = {}) => {
    const current = modeRef.current;
    const pinchScale = pinchRef.current?.scale ?? 1;
    pinchRef.current = null;
    const root = rootRef.current;
    if (!current || next === current) {
      if (root && root.style.transform) void springBack(root, pinchScale).then(() => pinchBackdrop(root, false));
      return;
    }
    const size = metricsRef.current;
    if (size && options.remember !== false) writeStoredMode(breakpointOf(size.width), next);
    const was = layoutRef.current;
    const layer = layerRef.current;
    const days = inputsRef.current.index?.days;
    if (!size || !was || !layer || !days || !visibleRef.current) {
      releaseLayer();
      modeRef.current = next;
      setMode(next);
      return;
    }
    // the page where it really is (a move's held scroll handed over, its still copy let go)
    handOver();
    dropStill();
    const scrollY = window.scrollY;
    const vh = window.innerHeight;
    const base = originRef.current;
    const point = options.point ?? {
      x: window.innerWidth / 2,
      y: ((compressedRef.current ? size.insets.topCompressed : size.insets.top) + vh - size.insets.bottom) / 2,
    };

    // the focal: what was tapped or pinched; else the profile, when it is what you're looking at; else the post
    // nearest the middle of the band
    let zid = options.focal?.zid ?? null;
    if (!zid && options.focal?.postKey) {
      const at = was.byPost.get(options.focal.postKey);
      zid = at != null ? idOf(was.items[at]) : null;
    }
    let keepProfile = false;
    if (!zid && !options.point) {
      if (current === 'today') keepProfile = dayAtProfile(was, scrollY - base);
      else {
        const profileRect = profileEl?.getBoundingClientRect();
        keepProfile = Boolean(profileRect && profileRect.height > 0 && profileRect.bottom > point.y);
      }
    }
    if (!zid && !keepProfile) {
      const at = nearestPost(was, point.x, scrollY - base + point.y, vh);
      zid = at >= 0 ? idOf(was.items[at]) : null;
    }

    // the new view, laid out now, and where the page goes in it: the focal stays where it is on screen (into the Day
    // view, it comes to rest on the focus line); the profile, when that is what you're looking at, stays put
    const scope2 = inputsRef.current.scope;
    const index2 = inputsRef.current.index;
    const targetPosts = index2 ? loadedPosts(scope2, index2) : inputsRef.current.posts;
    const target = computeLayout({ mode: next, days, posts: targetPosts, width: size.width, height: size.height, insets: size.insets });
    const focalAt = zid ? indexOfSlot(target, zid) : -1;
    const focalDay = zid ? zid.split(':')[0] : null;
    const focalFrame = zid ? layer.querySelector<HTMLElement>(`:scope > [data-zid="${zid}"] > [data-layer="frame"]`) : null;
    const anchor = focalFrame ? centreOf(rectOf(focalFrame)) : point;
    let top: number;
    if (keepProfile) top = scrollY - base;
    else if (next === 'today') top = dayRestTop(target, focalAt >= 0 ? focalAt : Math.max(0, focalDay ? indexForDay(target, focalDay) : 0));
    else if (focalAt >= 0) top = target.items[focalAt].y + target.items[focalAt].h / 2 - anchor.y;
    else {
      // the focal isn't on the new page (MONTH shows highlights): its group, under the open header
      const place = focalDay ? indexForDay(target, focalDay) : -1;
      top = place >= 0 ? target.items[place].y - size.insets.top : scrollY - base;
    }
    const scroll = clamp(base + top, 0, Math.max(0, base + target.totalHeight - vh));

    // the posts it opens on must be here, or their cells would arrive as placeholders (a moment at most; a pinch goes
    // at once, the page is in the fingers)
    zoomWaitRef.current = null;
    if (!options.waited && pinchScale === 1 && !reducedMotion()) {
      const [first, last] = itemRange(target, scroll - base, scroll - base + vh);
      const missing = new Set<string>();
      for (let at = first; at < last; at += 1) {
        const item = target.items[at];
        if ((item.kind === 'tile' || item.kind === 'reel') && !item.postKey && item.day) missing.add(item.day);
      }
      const again = () => zoomToRef.current?.(next, { ...options, focal: { zid, postKey: options.focal?.postKey ?? null }, waited: true });
      if (missing.size) {
        const token = {};
        zoomWaitRef.current = token;
        prefetchDays(scope2, [...missing]);
        void viewReady(scope2, next, size, { day: focalDay, zid }, ZOOM_LOAD_MS).then(() => {
          if (zoomWaitRef.current !== token) return;
          zoomWaitRef.current = null;
          again();
        });
        return;
      }
      // the pictures of the posts a grid brings on screen (those not on it already, decoded). Not into the Day view:
      // the post it opens on is the one on screen, its neighbours only peek
      const cold: string[] = [];
      for (let at = next === 'today' ? last : first; at < last; at += 1) {
        const item = target.items[at];
        if ((item.kind !== 'tile' && item.kind !== 'reel') || !item.postKey || !item.day) continue;
        const src = targetPosts.get(item.day)?.[item.slot]?.thumbnailUrl;
        if (!src || isWarm(src)) continue;
        if (layer.querySelector(`:scope > [data-cell="p:${CSS.escape(item.postKey)}"] img[data-loaded]`)) continue;
        cold.push(src);
      }
      if (cold.length) {
        const token = {};
        zoomWaitRef.current = token;
        void warmPictures(cold, PICTURE_WAIT_MS).then(() => {
          if (zoomWaitRef.current !== token) return;
          zoomWaitRef.current = null;
          again();
        });
        return;
      }
    }

    // the screen as it stands, then the new view in one commit
    const shots = snapshot(layer);
    const profileShot = profileEl ? rectOf(profileEl) : null;
    endFlight(flightRef.current);
    flightRef.current = null;
    window.clearTimeout(spotTimerRef.current);
    moveRef.current?.profileCopy?.remove();
    releaseLayer();
    const reduce = reducedMotion();
    const view = mountWindow(target, scroll - base, vh, true);
    const nextMove: Move | null = reduce ? null : {
      id: (moveSeqRef.current += 1),
      kind: 'zoom',
      from: current,
      to: next,
      shots,
      profile: profileShot,
      leaving: specsRef.current,
      centre: anchor,
      focalKey: focalAt >= 0 ? target.items[focalAt].postKey ?? null : null,
      scroll,
      origin: base,
      profileCopy: null,
    };
    moveRef.current = nextMove;
    const dayNow = next === 'today' ? (focalAt >= 0 ? focalAt : dayIndexAt(target, scroll - base)) : activeRef.current;
    const profileNow = next === 'today' ? dayAtProfile(target, scroll - base) : atProfileRef.current;
    // the days the new view opens on, asked for in the same commit (so it is drawn with its posts, not placeholders)
    const wanted = wantedDays(genRef.current, target, scroll - base, vh);
    if (!reduce) keepHeight();
    flushSync(() => {
      modeRef.current = next;
      setMode(next);
      setWin({ gen: genRef.current, mode: next, y0: view.y0, y1: view.y1, narrow: true });
      dayWinRef.current = wanted;
      wideRef.current = null;
      setDayWin(wanted);
      renderedRef.current = null;
      activeRef.current = dayNow;
      setActive(dayNow);
      atProfileRef.current = profileNow;
      setAtProfile(profileNow);
      movingRef.current = false;
      setSpotOn(reduce);
      setMove(nextMove);
    });
    if (reduce) {
      window.scrollTo(0, scroll);
      savedYRef.current = window.scrollY;
    }
  }, [dropStill, handOver, keepHeight, profileEl, releaseLayer]);
  useIsoLayoutEffect(() => {
    zoomToRef.current = zoomTo;
  });

  /* A move, drawn: the page goes where it opens, and before the first paint every piece is carried from where it was,
     or leaves, or arrives. The spotlight comes on a moment in; the rest of the page goes live once it has landed */
  useIsoLayoutEffect(() => {
    if (!move || landedRef.current === move.id) return;
    landedRef.current = move.id;
    const layer = layerRef.current;
    if (!layer) {
      endMove();
      return;
    }
    // the layer's place now (a new pick's profile may be another height): the scroll keeps what it was worked out for
    const at = pageTop(layer);
    if (Math.abs(at - originRef.current) >= 0.5) {
      originRef.current = at;
      setOrigin(at);
    }
    const jumpMode: ScrollJump = move.to === 'today' ? 'place' : 'keep';
    const goal = Math.max(0, move.scroll + (at - move.origin));
    const room = Math.max(0, (document.scrollingElement ?? document.documentElement).scrollHeight - window.innerHeight);
    const by = Math.min(goal, room) - window.scrollY;
    const root = rootRef.current;
    if (root && holdsScroll() && Math.abs(by) >= HOLD_FROM_PX) {
      // WebKit: drawn where it lands by an offset, nothing scrolled; the scroll handed over once it has landed. The
      // header answers now, as if the page were already there (the old profile's copy, inside the page, stays put)
      const copyAt = move.profileCopy ? rectOf(move.profileCopy) : null;
      root.style.top = `${(-by).toFixed(2)}px`;
      if (move.profileCopy && copyAt) pinToScreen(move.profileCopy, copyAt);
      handoverRef.current = { by, mode: jumpMode };
      markScrollJump(jumpMode, 4000, window.scrollY + by);
      savedYRef.current = window.scrollY + by;
      settledTopRef.current = window.scrollY + by;
    } else {
      markScrollJump(jumpMode);
      holdSnap();
      // the old profile's copy stays where it stood on screen
      const copyAt = move.profileCopy ? rectOf(move.profileCopy) : null;
      window.scrollTo(0, goal);
      if (move.profileCopy && copyAt) pinToScreen(move.profileCopy, copyAt);
      savedYRef.current = window.scrollY;
      settledTopRef.current = window.scrollY;
    }

    const vw = window.innerWidth;
    const vh = window.innerHeight;
    const flight = startFlight(holdsScroll() ? FLIGHT_HOLD_MS : 0);
    flightRef.current = flight;
    // a finger on the screen while the scroll is held: handed over before the finger moves the page (a scroll the page
    // makes while a finger is moving it can be lost, and the page would jump by all of it)
    if (handoverRef.current) {
      const touched = () => handOver();
      const listen = { capture: true, passive: true } as const;
      window.addEventListener('touchstart', touched, listen);
      window.addEventListener('wheel', touched, listen);
      flight.cleanups.push(() => {
        window.removeEventListener('touchstart', touched, listen);
        window.removeEventListener('wheel', touched, listen);
      });
    }
    // what leaves goes away from where the move started; what arrives comes in a ring at a time from where it lands
    const focalFrame = move.focalKey ? layer.querySelector<HTMLElement>(`:scope > [data-cell="p:${move.focalKey}"] > [data-layer="frame"]`) : null;
    const landing = focalFrame ? centreOf(rectOf(focalFrame)) : move.centre;

    // every piece measured where it now is before any is set moving (a measure after an animation is added has WebKit
    // work its styles out again, once for every piece)
    const steps: Array<() => void> = [];
    layer.querySelectorAll<HTMLElement>(':scope > [data-cell]').forEach((el) => {
      const shot = move.shots.get(el.dataset.cell ?? '');
      // the old view's leftovers: held where they were, and let go
      if (el.dataset.gone !== undefined) {
        const was = shot?.cell;
        if (was && onScreen(was, vw, vh)) {
          const now = rectOf(el);
          steps.push(() => leave(flight, el, now, was, move.centre));
        }
        return;
      }
      const frameEl = el.querySelector<HTMLElement>(':scope > [data-layer="frame"]');
      if (el.dataset.kind === 'post' && frameEl && shot?.frame) {
        const frameWas = shot.frame;
        const frameNow = rectOf(frameEl);
        const picture = frameEl.querySelector<HTMLElement>('[data-picture]');
        steps.push(() => carryLayer(flight, frameEl, frameWas, vw, vh, picture, frameNow));
        const bg = el.querySelector<HTMLElement>(':scope > [data-layer="bg"]');
        if (bg) {
          const bgWas = shot.bg ?? frameWas;
          const bgNow = rectOf(bg);
          steps.push(() => carryLayer(flight, bg, bgWas, vw, vh, null, bgNow));
        }
        ([['chips-out', shot.chips], ['chrome-out', shot.chrome], ['reading-out', shot.reading]] as const).forEach(([name, was]) => {
          const out = el.querySelector<HTMLElement>(`:scope > [data-layer="${name}"]`);
          if (!out || !was) return;
          const now = rectOf(out);
          steps.push(() => letGo(flight, out, was, now));
        });
        return;
      }
      // a header, a "+N" tile, a placeholder both views hold: carried whole
      if (shot?.cell) {
        const was = shot.cell;
        const now = rectOf(el);
        steps.push(() => carryLayer(flight, el, was, vw, vh, null, now));
        return;
      }
      const now = rectOf(el);
      if (onScreen(now, vw, vh)) steps.push(() => enter(flight, el, now, landing));
    });
    const profileNow = profileEl ? rectOf(profileEl) : null;
    steps.forEach((step) => step());

    // the page's profile: the same element, moved with the page (a pick for someone else: the old one's copy goes as
    // the new one comes in)
    if (profileEl && profileNow) {
      if (move.profileCopy) {
        const copy = move.profileCopy;
        run(flight, copy, [{ opacity: 1 }, { opacity: 0 }], { duration: 180, easing: 'cubic-bezier(0.4, 0, 1, 1)', fill: 'both' });
        flight.cleanups.push(() => copy.remove());
        if (onScreen(profileNow, vw, vh)) {
          run(flight, profileEl, [{ opacity: 0, transform: 'translate3d(0px, 12px, 0px)' }, { opacity: 1, transform: 'none' }], {
            duration: 420,
            delay: 90,
            easing: 'cubic-bezier(0.2, 0.7, 0.2, 1)',
            fill: 'backwards',
          });
        }
      } else if (move.profile) {
        // moved with the page; from (or to) far off screen only part of the way, fading
        carryLayer(flight, profileEl, move.profile, vw, vh, null, profileNow);
      }
    }

    // under way: the spotlight comes on a moment in
    const under = () => {
      window.clearTimeout(spotTimerRef.current);
      spotTimerRef.current = window.setTimeout(() => setSpotOn(true), SPOT_AFTER_MS);
    };
    if (flight.hold) {
      // held at its start for the frame that draws it, then set moving once that frame is on screen (WebKit only
      // starts the next frame once the last is shown)
      requestAnimationFrame(() => requestAnimationFrame(() => {
        if (flightRef.current !== flight || flight.ended) return;
        releaseFlight(flight);
        under();
      }));
    } else {
      under();
    }
    // landed (with a backstop, should an animation never report)
    const backstop = window.setTimeout(() => {
      if (flightRef.current === flight) endMove();
    }, LONG_CARRY_MS + 600 + flight.hold);
    void flightDone(flight).then(() => {
      window.clearTimeout(backstop);
      if (flightRef.current !== flight) return;
      if (handoverRef.current) landHeld(flight);
      else endMove();
    });
  }, [move, endMove, handOver, holdSnap, landHeld, profileEl]);

  /* ── a new pick or order ── */

  // its posts near the day you're on (and the pictures they open on): fetched while the screen holds still
  const prepare = useCallback((next: StreamScope): Promise<void> => {
    // the day you're on, read where the page really is
    handOver();
    const current = layoutRef.current;
    const size = metricsRef.current;
    if (!current || !size || !visibleRef.current) {
      pickRef.current = { next, day: null, keepScroll: false };
      return Promise.resolve();
    }
    const day = dayAt(current, window.scrollY - originRef.current, readingLine(size.insets, compressedRef.current));
    pickRef.current = { next, day, keepScroll: !day && window.scrollY > 1 };
    const around = day ? Array.from({ length: SEED_DAYS }, (_, step) => shiftDay(day, SEED_AHEAD - step)) : [];
    if (around.length) prefetchDays(next, around);
    setSeed({ gen: scopeKey(next), days: around });
    return viewReady(next, current.mode, size, { day, zid: null }, PICK_WAIT_MS);
  }, [handOver]);

  // ...and shown: the screen as it is now is kept for the move, and apply() shows the new pick in the same commit
  const commit = useCallback((apply: () => void, options: { profile: boolean }) => {
    handOver();
    dropStill();
    const pick = pickRef.current;
    const size = metricsRef.current;
    const layer = layerRef.current;
    const current = modeRef.current;
    const nextIndex = pick ? peekStreamIndex(pick.next) : null;
    if (!pick || !size || !layer || !current || !nextIndex || !visibleRef.current || reducedMotion()) {
      endMove();
      flushSync(apply);
      if (pick && !pick.keepScroll && !pick.day) window.scrollTo(0, 0);
      return;
    }
    const target = computeLayout({ mode: current, days: nextIndex.days, posts: loadedPosts(pick.next, nextIndex), width: size.width, height: size.height, insets: size.insets });
    const nextOrigin = originFor(size, pick.next);
    const vh = window.innerHeight;
    // the day you were on (or the nearest older one it has posts on); in the profile, where you are; else the top
    let scroll = pick.keepScroll ? window.scrollY : 0;
    if (pick.day) {
      const at = indexForDay(target, pick.day);
      if (at >= 0) scroll = current === 'today' ? nextOrigin + dayRestTop(target, at) : nextOrigin + target.items[at].y - size.insets.top;
    }
    scroll = clamp(scroll, 0, Math.max(0, nextOrigin + target.totalHeight - vh));
    const shots = snapshot(layer);
    const profileShot = profileEl ? rectOf(profileEl) : null;
    endFlight(flightRef.current);
    flightRef.current = null;
    window.clearTimeout(spotTimerRef.current);
    moveRef.current?.profileCopy?.remove();
    // someone else's profile: the old one's copy, on an overlay where it stood, goes as the new one comes in
    let profileCopy: HTMLElement | null = null;
    if (options.profile && profileEl && profileShot && onScreen(profileShot, window.innerWidth, vh)) {
      profileCopy = profileEl.cloneNode(true) as HTMLElement;
      profileCopy.removeAttribute('data-profile');
      profileCopy.removeAttribute('data-arrive');
      unsnap(profileCopy);
      // an element of the page pinned where the profile stood, not a fixed one, and inside the stream (iOS Safari
      // would paint the status bar in its colour, or draw the page again: stillFrame.ts)
      profileCopy.setAttribute('data-still-skip', '');
      Object.assign(profileCopy.style, {
        position: 'absolute',
        left: '0px',
        top: '0px',
        width: `${profileShot.width}px`,
        height: `${profileShot.height}px`,
        margin: '0',
        zIndex: '20',
        pointerEvents: 'none',
      } satisfies Partial<CSSStyleDeclaration>);
      (rootRef.current ?? document.body).appendChild(profileCopy);
      pinToScreen(profileCopy, profileShot);
    }
    const view = mountWindow(target, scroll - nextOrigin, vh, true);
    const wanted = wantedDays(scopeKey(pick.next), target, scroll - nextOrigin, vh);
    const dayNow = current === 'today' ? dayIndexAt(target, scroll - nextOrigin) : activeRef.current;
    const profileNow = current === 'today' ? dayAtProfile(target, scroll - nextOrigin) : atProfileRef.current;
    const nextMove: Move = {
      id: (moveSeqRef.current += 1),
      kind: 'pick',
      from: current,
      to: current,
      shots,
      profile: options.profile ? null : profileShot,
      leaving: specsRef.current,
      centre: { x: window.innerWidth / 2, y: (size.insets.topCompressed + vh - size.insets.bottom) / 2 },
      focalKey: null,
      scroll,
      origin: nextOrigin,
      profileCopy,
    };
    moveRef.current = nextMove;
    keepHeight();
    flushSync(() => {
      setWin({ gen: scopeKey(pick.next), mode: current, y0: view.y0, y1: view.y1, narrow: true });
      dayWinRef.current = wanted;
      wideRef.current = null;
      setDayWin(wanted);
      renderedRef.current = null;
      activeRef.current = dayNow;
      setActive(dayNow);
      atProfileRef.current = profileNow;
      setAtProfile(profileNow);
      movingRef.current = false;
      setSpotOn(false);
      setMove(nextMove);
      apply();
    });
  }, [dropStill, endMove, handOver, keepHeight, originFor, profileEl]);

  /* A pick or a zoom about to be taken (a hover, a press): the days it opens on are fetched and, as they arrive, the
     pictures it would show first are decoded, for a few seconds at most. Never while just scrolling */
  const warmView = useCallback((next: { scope?: StreamScope; mode?: FeedMode }) => {
    const current = layoutRef.current;
    const size = metricsRef.current;
    if (!current || !size || !modeRef.current || !visibleRef.current) return;
    const forScope = next.scope ?? inputsRef.current.scope;
    const forMode = next.mode ?? modeRef.current;
    const day = dayAt(current, window.scrollY - originRef.current, readingLine(size.insets, compressedRef.current));
    const near = next.scope ? -1 : nearestPost(current, window.innerWidth / 2, window.scrollY - originRef.current + window.innerHeight / 2, window.innerHeight);
    const zid = near >= 0 ? idOf(current.items[near]) : null;
    const key = `${scopeKey(forScope)}|${forMode}|${day ?? ''}|${zid ?? ''}`;
    const warming = warmingRef.current;
    if (warming.has(key)) return;
    // one warm at a time: the latest intent is the one that matters
    warming.forEach((stopOther) => stopOther());
    if (day && scopeKey(forScope) !== scopeKey(inputsRef.current.scope)) prefetchDays(forScope, Array.from({ length: 8 }, (_, step) => shiftDay(day, -step)));
    const warmed = new Set<string>();
    let unsubscribe: (() => void) | null = null;
    let deadline = 0;
    let frame = 0;
    const stop = () => {
      unsubscribe?.();
      unsubscribe = null;
      window.clearTimeout(deadline);
      cancelAnimationFrame(frame);
      warming.delete(key);
    };
    const attempt = () => {
      frame = 0;
      const view = viewPictures(forScope, forMode, size, { day, zid });
      if (!view) return;
      const fresh = view.srcs.filter((src) => !warmed.has(src));
      fresh.forEach((src) => warmed.add(src));
      if (fresh.length) void warmPictures(fresh);
      if (view.missing.length) prefetchDays(forScope, view.missing);
      else stop();
    };
    warming.set(key, stop);
    deadline = window.setTimeout(stop, WARM_MS);
    unsubscribe = subscribeStream(() => {
      if (!frame) frame = requestAnimationFrame(attempt);
    });
    attempt();
  }, []);
  useEffect(() => () => warmingRef.current.forEach((stop) => stop()), []);

  /* ── going places ── */

  /* A scroll the page makes itself to somewhere else on it: a glide, or on WebKit, further than GLIDE_MAX_SCREENS, a jump
     under a still copy of the screen that drifts the way the page goes, waits for WebKit to draw, and dissolves */
  const goTo = useCallback((top: number) => {
    handOver();
    const smooth = !reducedMotion();
    const root = rootRef.current;
    const distance = top - window.scrollY;
    if (!smooth || !root || !holdsScroll() || Math.abs(distance) <= window.innerHeight * GLIDE_MAX_SCREENS) {
      scrollPage(top, smooth);
      return;
    }
    const entry = coverPage(root);
    void entry.shown.then(() => {
      if (stillRef.current !== entry) return;
      scrollPage(top, false);
      entry.fadeOut(STILL_FADE_MS, distance < 0 ? STILL_FADE_DRIFT : -STILL_FADE_DRIFT, STILL_FADE_EASE, STILL_FADE_HOLD_MS);
    });
  }, [coverPage, handOver, scrollPage]);

  const scrollToNewest = useCallback(() => {
    handOver();
    if (window.scrollY <= 1) return;
    const current = layoutRef.current;
    const size = metricsRef.current;
    const smooth = !reducedMotion();
    if (holdsScroll()) {
      goTo(0);
      return;
    }
    // from far down, jump to a couple of screens from the top first: the glide never mounts the whole feed on its way
    const near = current?.mode === 'today' ? originRef.current + dayRestTop(current, 2) : (size?.height ?? 800) * 2;
    if (smooth && window.scrollY > near * 1.5) scrollPage(near, false);
    scrollPage(0, smooth);
  }, [goTo, handOver, scrollPage]);

  // the Day view: one post on or back (a key)
  const stepPost = useCallback((direction: 1 | -1) => {
    const current = layoutRef.current;
    if (!current || current.mode !== 'today' || moveRef.current) return;
    const top = window.scrollY - originRef.current;
    const from = dayAtProfile(current, top) ? -1 : dayIndexAt(current, top);
    const to = clamp(from + direction, -1, current.items.length - 1);
    if (to === from) return;
    scrollPage(to < 0 ? 0 : originRef.current + dayRestTop(current, to), !reducedMotion());
  }, [scrollPage]);

  // a highlight in the profile: the Day view, at that post (its day loads first)
  const openHighlight = useCallback((key: string) => {
    const { index: currentIndex, scope: currentScope } = inputsRef.current;
    if (!currentIndex) return;
    const highlight = currentIndex.profile?.highlights.find((candidate) => candidate.key === key);
    const found = locatePost(currentIndex, currentScope, key, highlight?.topPercent ?? null);
    if (!found) return;
    prefetchDays(currentScope, [found.day]);
    seekRef.current = { key, until: Date.now() + 8000 };
    const zid = slotId(found.day, found.slot);
    const current = layoutRef.current;
    if (current?.mode === 'today') {
      const at = indexOfSlot(current, zid);
      if (at >= 0) goTo(originRef.current + dayRestTop(current, at));
      return;
    }
    zoomTo('today', { focal: { zid, postKey: key } });
  }, [goTo, zoomTo]);

  useImperativeHandle(ref, () => ({
    setMode: (next: FeedMode) => zoomTo(next),
    scrollToNewest,
    prepare,
    commit,
    warm: warmView,
  }), [commit, prepare, scrollToNewest, warmView, zoomTo]);

  const actions = useMemo<PieceActions>(() => ({
    openTile: (post, element) => {
      const rect = element.getBoundingClientRect();
      const zid = element.closest<HTMLElement>('[data-zid]')?.dataset.zid ?? null;
      zoomTo('today', { focal: { zid, postKey: post.key }, point: { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 } });
    },
    openCard: (post) => callbacksRef.current.onOpenPost(post),
    openWeek: (day, element) => {
      const rect = element.getBoundingClientRect();
      zoomTo('week', { focal: { zid: slotId(day, 0) }, point: { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 } });
    },
    manage: () => callbacksRef.current.onManage(),
    lead: () => callbacksRef.current.onLead(),
    read: () => callbacksRef.current.onRead(),
    orderChange: (next) => callbacksRef.current.onOrderChange(next),
    openHighlight,
  }), [openHighlight, zoomTo]);

  /* ── effects (layout effects run in this order, before paint, and again whenever the tab comes back) ── */

  // the page is the one scroller, at every width (as on Read). The body stays overflow-visible: an overflowing body
  // becomes a scroll container of its own, and the page's snap would stop seeing the cards
  useIsoLayoutEffect(() => {
    const release = acquireRootPageScroll();
    const body = document.body;
    const previous = body.style.overflow;
    body.style.overflow = 'visible';
    return () => {
      body.style.overflow = previous;
      release();
    };
  }, []);

  // the tab came back: back where you were (AppTabHost leaves a [data-tab-scroll="own"] tab's scroll to it). Hiding:
  // nothing keeps running, and nothing of the Feed's is left on the page
  useIsoLayoutEffect(() => {
    visibleRef.current = true;
    holdSnap();
    if (Math.abs(window.scrollY - savedYRef.current) > 1) {
      markScrollJump();
      scrollPage(savedYRef.current, false);
    }
    const root = rootRef.current;
    return () => {
      visibleRef.current = false;
      touchingRef.current = false;
      pinchRef.current = null;
      endMove();
      dropStill();
      releaseLayer();
      snapHeldRef.current = false;
      holdSnapStyle(false);
      root?.querySelectorAll('video').forEach((video) => video.pause());
    };
  }, [dropStill, endMove, holdSnap, releaseLayer, scrollPage]);

  // keep what the handlers read current
  useIsoLayoutEffect(() => {
    layoutRef.current = layout;
    metricsRef.current = metrics;
    originRef.current = origin;
    genRef.current = gen;
    inputsRef.current = { index, posts, scope };
    compressedRef.current = headerCompressed;
    callbacksRef.current = { onModeChange, onOpenPost, onManage, onLead, onRead, onOrderChange, play };
    if (!moveRef.current) renderedRef.current = layout && mountView ? { layout, gen, from: mountFrom, to: mountTo } : null;
  });

  // the viewport and the chrome around it (the open header's bottom, the folded header's, the nav's top, the small
  // viewport: stable while Safari's toolbar folds), and where the layer starts in the page
  const measure = useCallback(() => {
    const root = rootRef.current;
    if (!root) return;
    const width = root.clientWidth;
    const read = (node: HTMLElement | null, fallback: number) => (node ? node.getBoundingClientRect().height : 0) || fallback;
    const height = read(probeViewRef.current, window.innerHeight);
    if (!width || !height) return;
    const desktop = width >= 1024;
    const insets: StreamInsets = {
      top: read(probeTopRef.current, desktop ? 188 : 162),
      topCompressed: read(probeFoldRef.current, desktop ? 88 : 78),
      bottom: read(probeNavRef.current, desktop ? 90 : 86),
    };
    setMetrics((current) => (
      current && Math.abs(current.width - width) < 0.5 && Math.abs(current.height - height) < 0.5 && sameInsets(current.insets, insets)
        ? current
        : { width, height, insets }
    ));
    const layer = layerRef.current;
    if (layer && !moveRef.current) {
      const next = pageTop(layer);
      setOrigin((current) => (Math.abs(current - next) < 1 ? current : next));
    }
  }, []);

  useIsoLayoutEffect(() => {
    measure();
    if (typeof ResizeObserver === 'undefined') {
      window.addEventListener('resize', measure);
      return () => window.removeEventListener('resize', measure);
    }
    const observer = new ResizeObserver(() => measure());
    [rootRef.current, probeTopRef.current, probeFoldRef.current, probeNavRef.current, probeViewRef.current, profileEl].forEach((node) => {
      if (node) observer.observe(node);
    });
    return () => observer.disconnect();
  }, [measure, profileEl]);

  // the zoom: the one last used at this breakpoint (the Day view on a phone, the week from 1024px, by default)
  useIsoLayoutEffect(() => {
    if (!metrics) return;
    const breakpoint = breakpointOf(metrics.width);
    if (breakpointRef.current === breakpoint) return;
    const first = breakpointRef.current === null;
    breakpointRef.current = breakpoint;
    const wanted = readStoredMode(breakpoint) ?? defaultModeFor(breakpoint);
    if (first || !modeRef.current) {
      modeRef.current = wanted;
      setMode(wanted);
      return;
    }
    if (wanted !== modeRef.current) {
      endMove();
      modeRef.current = wanted;
      setMode(wanted);
    }
  }, [metrics, endMove]);

  // a new scope: what was measured against the last one is let go
  useIsoLayoutEffect(() => {
    dayWinRef.current = null;
    if (!moveRef.current) seekRef.current = null;
  }, [gen]);

  // a phone's Day view snaps one post per swipe (switched on only once a move has landed: snapping mid-move would pull
  // the page to whichever card is mounted)
  const phoneDay = mode === 'today' && Boolean(layout) && layout?.mediaWidth == null;

  /* What the phone's Day view snaps to: a mark at every post, always there, however few of the cards are mounted. iOS
     Safari keeps the post the page rests on as a place in the list of snap points (the fourth, say), and whenever the
     page is still it puts the page back on whatever is at that place now. With the cards themselves as the snap points
     the list changed with every card mounted or let go: Safari moved the page to the new fourth, which mounted and let
     go of cards, so it moved again, card after card, on its own (WebKit: WKWebView _createVisibleContentRectUpdate,
     RemoteScrollingCoordinatorProxyIOS scrollOffsetSnappedToNearestSnapPoint). A list that never changes as the page
     moves keeps every place in it the same post */
  // (the Day view's posts sit at even steps: the marks change only with how many there are, never as days load. And
  // only once a move has landed: the snap is held through a move and until a finger moves the page, and a thousand marks
  // put up or taken down in a move's first frame would only hold it up)
  const markCount = phoneDay && layout ? layout.items.length : 0;
  const markTop = markCount && layout ? layout.items[0].y : 0;
  const markStride = layout?.stride ?? 0;
  const markHeight = layout?.cardHeight ?? 0;
  const [marks, setMarks] = useState<{ count: number; top: number; stride: number; height: number } | null>(null);
  useIsoLayoutEffect(() => {
    if (move) return;
    setMarks((current) => {
      if (!markCount) return null;
      if (current && current.count === markCount && current.top === markTop && current.stride === markStride && current.height === markHeight) return current;
      return { count: markCount, top: markTop, stride: markStride, height: markHeight };
    });
  }, [move, markCount, markTop, markStride, markHeight]);
  const snapMarks = useMemo(() => {
    if (!marks) return null;
    return Array.from({ length: marks.count }, (_, at) => (
      <div
        key={at}
        aria-hidden="true"
        data-still-skip=""
        className="pointer-events-none absolute left-0 w-px"
        style={{ top: marks.top + at * marks.stride, height: marks.height, scrollSnapAlign: 'center', scrollSnapStop: 'always' }}
      />
    ));
  }, [marks]);
  const insets = metrics?.insets ?? null;
  useIsoLayoutEffect(() => {
    if (!phoneDay || move || !insets) return undefined;
    const html = document.documentElement;
    html.style.setProperty('--fm-feed-snap-top', `${insets.topCompressed}px`);
    html.style.setProperty('--fm-feed-snap-bottom', `${insets.bottom}px`);
    holdSnap();
    applySnap(!pinchRef.current);
    return () => {
      applySnap(false);
      html.style.removeProperty('--fm-feed-snap-top');
      html.style.removeProperty('--fm-feed-snap-bottom');
    };
  }, [phoneDay, move, insets?.topCompressed, insets?.bottom, applySnap, holdSnap]);

  // the layout changed under the reader (a day loaded with sizes its index didn't have, a rotation): the piece at the
  // reading line holds still
  const narrowWindow = !win || win.gen !== gen || win.mode !== layout?.mode || win.narrow;
  useIsoLayoutEffect(() => {
    const prev = prevRef.current;
    if (!layout) return;
    const nowOrigin = originRef.current;
    prevRef.current = { layout, origin: nowOrigin };
    if (moveRef.current) return;
    if (!prev || (prev.layout === layout && prev.origin === nowOrigin) || prev.layout.mode !== layout.mode) {
      if (!prev || prev.layout !== layout) {
        syncFrame(window.scrollY, narrowWindow);
        lookAhead(window.scrollY, false);
      }
      return;
    }
    let scrollY = window.scrollY;
    const line = readingLine(prev.layout.insets, compressedRef.current);
    const top = scrollY - prev.origin;
    // only when reading the posts (looking at the profile, the page simply grows under it), and never under a scroll in
    // progress: a scroll the page makes while a finger, momentum or a glide moves it stops that scroll dead on iOS (and
    // with the snap held, it ends between two cards). Such a change (rare: a day loading with sizes its index didn't
    // have) shows as it is
    if (!scrollingRef.current && top + line >= 0 && !(layout.mode === 'today' && dayAtProfile(prev.layout, top))) {
      const anchor = anchorAt(prev.layout, top, line);
      const next = anchor ? topForAnchor(layout, anchor) : null;
      if (next != null) {
        const target = Math.max(0, nowOrigin + next);
        if (Math.abs(target - scrollY) >= 0.5) {
          markScrollJump();
          scrollPage(target, false);
          scrollY = window.scrollY;
          savedYRef.current = scrollY;
          settledTopRef.current = scrollY;
        }
      }
    }
    syncFrame(scrollY, narrowWindow);
    // posts that just loaded ahead of the scroll: their pictures too
    lookAhead(scrollY, false);
  }, [layout, origin, narrowWindow, scrollPage, syncFrame, lookAhead]);

  // a highlight opened before its day had loaded: once it has, the post in view is that post
  useIsoLayoutEffect(() => {
    const seek = seekRef.current;
    if (!seek || !layout || layout.mode !== 'today' || moveRef.current) return;
    if (Date.now() > seek.until) {
      seekRef.current = null;
      return;
    }
    const at = layout.byPost.get(seek.key);
    if (at == null) return;
    seekRef.current = null;
    if (dayIndexAt(layout, window.scrollY - originRef.current) === at && !dayAtProfile(layout, window.scrollY - originRef.current)) return;
    markScrollJump('place');
    scrollPage(originRef.current + dayRestTop(layout, at), false);
    savedYRef.current = window.scrollY;
    settledTopRef.current = window.scrollY;
    activeRef.current = at;
    setActive(at);
    syncFrame(window.scrollY);
  }, [layout, scrollPage, syncFrame]);

  // the first posts on screen (the tab opening) come in, a ring at a time from the top
  useIsoLayoutEffect(() => {
    if (!layout || !layout.items.length || moveRef.current || arrivedRef.current || !visibleRef.current) return;
    arrivedRef.current = true;
    const layer = layerRef.current;
    if (!layer || reducedMotion()) return;
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    const flight = startFlight(holdsScroll() ? FLIGHT_HOLD_MS : 0);
    const from = { x: vw / 2, y: 0 };
    const shown = Array.from(layer.querySelectorAll<HTMLElement>(':scope > [data-cell]'), (el) => [el, rectOf(el)] as const)
      .filter(([, now]) => onScreen(now, vw, vh));
    shown.forEach(([el, now]) => enter(flight, el, now, from));
    if (flight.hold) requestAnimationFrame(() => requestAnimationFrame(() => releaseFlight(flight)));
    void flightDone(flight).then(() => endFlight(flight));
  }, [layout]);

  // tell the tab which zoom is on (its header control shows it)
  useIsoLayoutEffect(() => {
    if (mode) callbacksRef.current.onModeChange(mode);
  }, [mode]);

  // the scroll: window.scrollY once a frame; settled on 'scrollend' or after a quiet spell
  useIsoLayoutEffect(() => {
    let frame = 0;
    let timer = 0;
    const tick = () => {
      frame = 0;
      if (!visibleRef.current || pinchRef.current) return;
      const scrollY = window.scrollY;
      savedYRef.current = scrollY;
      syncFrame(scrollY);
      lookAhead(scrollY, true);
    };
    const quiet = () => {
      timer = 0;
      settle();
    };
    // a person moving the page takes the snap back (holdSnap): mid-swipe, so the swipe ends on a card it snapped to
    const takeSnap = () => {
      if (!snapHeldRef.current) return;
      snapHeldRef.current = false;
      holdSnapStyle(false);
    };
    const onScroll = () => {
      if (touchingRef.current) takeSnap();
      if (!frame) frame = window.requestAnimationFrame(tick);
      if (timer) window.clearTimeout(timer);
      timer = window.setTimeout(quiet, SETTLE_MS);
    };
    const onScrollEnd = () => {
      if (timer) {
        window.clearTimeout(timer);
        timer = 0;
      }
      settle();
    };
    const onTouchStart = () => {
      touchingRef.current = true;
      scrollingRef.current = true;
    };
    const onTouchEnd = (event: TouchEvent) => {
      if (event.touches.length) return;
      touchingRef.current = false;
      if (timer) window.clearTimeout(timer);
      timer = window.setTimeout(quiet, SETTLE_MS);
    };
    window.addEventListener('scroll', onScroll, { passive: true });
    window.addEventListener('scrollend', onScrollEnd, { passive: true });
    window.addEventListener('touchstart', onTouchStart, { passive: true });
    window.addEventListener('touchend', onTouchEnd, { passive: true });
    window.addEventListener('touchcancel', onTouchEnd, { passive: true });
    window.addEventListener('wheel', takeSnap, { passive: true });
    return () => {
      if (frame) window.cancelAnimationFrame(frame);
      if (timer) window.clearTimeout(timer);
      window.removeEventListener('scroll', onScroll);
      window.removeEventListener('scrollend', onScrollEnd);
      window.removeEventListener('touchstart', onTouchStart);
      window.removeEventListener('touchend', onTouchEnd);
      window.removeEventListener('touchcancel', onTouchEnd);
      window.removeEventListener('wheel', takeSnap);
    };
  }, [lookAhead, settle, syncFrame]);

  // a first frame mounts only the screen; the rest of the window a frame later
  useEffect(() => {
    if (!layout || !metrics || move || !narrowWindow) return undefined;
    const frame = window.requestAnimationFrame(() => {
      if (moveRef.current || !visibleRef.current) return;
      const view = mountWindow(layout, window.scrollY - originRef.current, metrics.height, false);
      renderedRef.current = null;
      setWin({ gen, mode: layout.mode, y0: view.y0, y1: view.y1, narrow: false });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [gen, layout, metrics, narrowWindow, move]);

  // the keys: G the Day view ⇄ the week, M the month, Esc the week, - and + a zoom out or in, ↑ ↓ J K a post at a time
  useEffect(() => {
    if (!keyboard) return undefined;
    const onKey = (event: KeyboardEvent) => {
      if (event.defaultPrevented || event.metaKey || event.ctrlKey || event.altKey) return;
      const target = event.target as HTMLElement | null;
      if (target && (target.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName))) return;
      const current = modeRef.current;
      if (!current) return;
      const go = (next: FeedMode | null) => {
        if (!next) return;
        event.preventDefault();
        zoomTo(next);
      };
      switch (event.key) {
        case 'g':
        case 'G':
          go(toggledMode(current));
          break;
        case 'm':
        case 'M':
          go(current === 'month' ? null : 'month');
          break;
        case '-':
        case '_':
          go(stepMode(current, 1));
          break;
        case '=':
        case '+':
          go(stepMode(current, -1));
          break;
        case 'Escape': {
          // only from the page itself: Escape in a menu or a sheet is that control's
          const fromPage = !target || target === document.body || Boolean(rootRef.current?.contains(target));
          if (fromPage && current !== 'week') zoomTo('week');
          break;
        }
        case 'ArrowDown':
        case 'j':
        case 'J':
          if (current === 'today') {
            event.preventDefault();
            stepPost(1);
          }
          break;
        case 'ArrowUp':
        case 'k':
        case 'K':
          if (current === 'today') {
            event.preventDefault();
            stepPost(-1);
          }
          break;
        default:
          break;
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [keyboard, stepPost, zoomTo]);

  /* ── the live pinch: one transform on the page, about the fingers ── */

  const onPinchStart = useCallback((focal: PinchFocal) => {
    const root = rootRef.current;
    if (!root || !visibleRef.current || !modeRef.current) return;
    endMove();
    pinchRef.current = { focal, scale: 1 };
    if (snapOnRef.current) applySnap(false);
    const rect = root.getBoundingClientRect();
    root.style.transformOrigin = `${focal.x - rect.left}px ${focal.y - rect.top}px`;
    root.style.willChange = 'transform';
    pinchBackdrop(root, true);
  }, [applySnap, endMove]);

  const onPinchScale = useCallback((scale: number) => {
    const root = rootRef.current;
    const pinch = pinchRef.current;
    const current = modeRef.current;
    if (!root || !pinch || !current) return;
    // past the last zoom either way, only a rubber band
    const room = scale < 1 ? stepMode(current, 1) : stepMode(current, -1);
    const shown = room ? scale : 1 + (scale - 1) * PINCH_EDGE;
    pinch.scale = shown;
    root.style.transform = `scale(${shown})`;
    // the way it is going known, the zoom it heads for gets its pictures fetched while the fingers are still down
    if (room && Math.abs(scale - 1) > PINCH_WARM_AT) warmView({ mode: room });
  }, [warmView]);

  const onPinchEnd = useCallback((direction: PinchDirection | 0) => {
    const pinch = pinchRef.current;
    const current = modeRef.current;
    const root = rootRef.current;
    if (!pinch || !current || !root) return;
    const next = direction ? stepMode(current, direction) : null;
    if (next) {
      zoomTo(next, { focal: { zid: pinch.focal.zid, postKey: pinch.focal.postKey }, point: { x: pinch.focal.x, y: pinch.focal.y } });
      return;
    }
    pinchRef.current = null;
    void springBack(root, pinch.scale).then(() => {
      pinchBackdrop(root, false);
      if (modeRef.current === 'today' && layoutRef.current?.mediaWidth == null) {
        holdSnap();
        applySnap(true);
      }
    });
  }, [applySnap, holdSnap, zoomTo]);

  usePinch(rootRef, { enabled: Boolean(layout), onStart: onPinchStart, onScale: onPinchScale, onEnd: onPinchEnd });

  /* ── render ── */

  const column = metrics ? contentBox(metrics.width) : null;
  const profileHeight = column ? profileHeaderHeight(column.contentWidth, scope) : 0;
  const showProfile = Boolean(column && metrics && (!blank || blank.profile));
  const wideDay = layout?.mode === 'today' && layout.mediaWidth != null;
  // a phone's Day view snaps to the profile too (at the very top)
  const profileSnap: CSSProperties = phoneDay && metrics && column
    ? { scrollSnapAlign: 'start', scrollMarginTop: metrics.insets.top + column.topPad - metrics.insets.topCompressed }
    : {};
  const profileHeader = showProfile && column && metrics ? (
    <div style={{ paddingTop: metrics.insets.top + column.topPad }}>
      <div
        key={who}
        ref={setProfileEl}
        data-profile=""
        data-arrive=""
        className="mx-auto"
        style={{ width: column.contentWidth, height: profileHeight, ...profileSnap }}
      >
        <ProfileHeader
          variant="page"
          scope={scope}
          title={profile.title}
          subtitle={profile.subtitle}
          feeder={profile.feeder}
          feeds={profile.feeds}
          faces={profile.faces}
          profile={index?.profile ?? null}
          fresh={profile.fresh}
          order={order}
          onOrderChange={actions.orderChange}
          width={column.contentWidth}
          onLead={actions.lead}
          onRead={actions.read}
          onManage={actions.manage}
          onOpenPost={actions.openHighlight}
        />
      </div>
      <div style={{ height: PROFILE_GAP }} />
    </div>
  ) : null;

  // what to draw: a spec per mounted item (the next move starts from these)
  const specs: Spec[] = [];
  if (!blank && drawn && metrics && mountView) {
    const shapeOf = (item: LayoutItem): CellShape => ({
      w: item.w,
      h: item.h,
      cols: item.cols,
      rows: item.rows,
      mediaWidth: drawn.mediaWidth ?? 0,
      wide: drawn.mode === 'today' && drawn.mediaWidth != null,
    });
    for (let at = mountFrom; at < mountTo; at += 1) {
      const item = drawn.items[at];
      if (item.y + item.h <= mountView.y0) continue;
      if (item.kind === 'section') {
        const section = layout?.sections.get(item.key.slice(2));
        const heading = section ? headings.get(section.key) : undefined;
        if (!section || !heading) continue;
        specs.push({ kind: 'section', key: item.key, x: item.x, y: item.y, w: item.w, h: item.h, heading, count: section.count, best: section.best, order: shownOrder });
        continue;
      }
      if (item.kind === 'more') {
        if (item.day) specs.push({ kind: 'more', key: item.key, x: item.x, y: item.y, w: item.w, h: item.h, day: item.day, more: item.more ?? 0 });
        continue;
      }
      const post = layout && item.day && item.slot >= 0 ? posts.get(item.day)?.[item.slot] ?? null : null;
      if (!post) {
        specs.push({ kind: 'placeholder', key: `x:${item.day}:${item.slot}`, mode: drawn.mode, shape: shapeOf(item), x: item.x, y: item.y });
        continue;
      }
      const inDay = drawn.mode === 'today';
      specs.push({
        kind: 'post',
        key: `p:${post.key}`,
        zid: idOf(item),
        post,
        mode: drawn.mode,
        shape: shapeOf(item),
        x: item.x,
        y: item.y,
        index: at,
        // the first screen of a grid; every card the Day view has mounted (the next ones load before a swipe brings them)
        eager: inDay || origin + item.y < metrics.height,
        standout: inDay && at === 0 && firstStandout,
        dayStart: inDay && item.slot === 0 && item.day && drawn.mediaWidth == null ? item.day : null,
        feederHandle: inDay || (!scope.handle && item.cols === 2 && item.rows === 2) ? post.handle : null,
      });
    }
  }
  // the old view's cells this move still draws (those the new view doesn't: they go), and each shared post's old self
  const specKeys = new Set(specs.map((spec) => spec.key));
  const oldSpecs = new Map<string, Spec>();
  move?.leaving.forEach((spec) => oldSpecs.set(spec.key, spec));
  const gone = move ? move.leaving.filter((spec) => !specKeys.has(spec.key)) : [];
  // the move's two views differ in kind (a grid and the Day view): each post's chrome changes, and comes in
  const crossing = Boolean(move && (move.from === 'today') !== (move.to === 'today'));

  const drawSpec = (spec: Spec, going: boolean) => {
    if (spec.kind === 'section') {
      return (
        <div key={spec.key} data-cell={spec.key} data-kind="section" data-gone={going ? '' : undefined} data-arrive="" className="st-cell" style={{ left: spec.x, top: spec.y, width: spec.w, height: spec.h }}>
          <SectionHeader label={spec.heading.label} kicker={spec.heading.kicker} title={spec.heading.title} first={spec.heading.first} count={spec.count} best={spec.best} order={spec.order} width={spec.w} height={spec.h} />
        </div>
      );
    }
    if (spec.kind === 'more') {
      return (
        <div key={spec.key} data-cell={spec.key} data-kind="more" data-gone={going ? '' : undefined} data-arrive="" className="st-cell" style={{ left: spec.x, top: spec.y, width: spec.w, height: spec.h }}>
          <MoreTile count={spec.more} span="this month" width={spec.w} height={spec.h} onOpen={(element) => actions.openWeek(spec.day, element)} />
        </div>
      );
    }
    if (spec.kind === 'placeholder') {
      return (
        <PlaceholderCell
          key={spec.key}
          cellKey={spec.key}
          mode={spec.mode}
          x={spec.x}
          y={spec.y}
          w={spec.shape.w}
          h={spec.shape.h}
          mediaWidth={spec.shape.mediaWidth}
          wide={spec.shape.wide}
          gone={going}
        />
      );
    }
    const was = going ? undefined : oldSpecs.get(spec.key);
    const wasPost = was?.kind === 'post' && was.mode !== spec.mode ? was : null;
    const inDay = spec.mode === 'today';
    return (
      <PostCell
        key={spec.key}
        cellKey={spec.key}
        zid={spec.zid}
        post={spec.post}
        mode={spec.mode}
        x={spec.x}
        y={spec.y}
        w={spec.shape.w}
        h={spec.shape.h}
        cols={spec.shape.cols}
        rows={spec.shape.rows}
        mediaWidth={spec.shape.mediaWidth}
        wide={spec.shape.wide}
        leaving={wasPost ? wasPost.mode : null}
        leavingShape={wasPost ? wasPost.shape : null}
        arriving={!going && crossing}
        focal={!going && move?.focalKey === spec.post.key}
        // the post the Day view rests on plays, and goes on playing while a swipe carries it away (stopping it at the
        // swipe's start swapped the picture under the finger): the next one takes over once the page settles on it
        active={!going && inDay && spec.index === active && !atProfile && !move}
        lit={!going && inDay && spec.shape.wide && spec.index === active}
        spot={!going && inDay && spec.shape.wide && spotOn && spec.index === active}
        standout={spec.standout}
        dayStart={spec.dayStart}
        eager={spec.eager}
        feeder={spec.feederHandle ? feeders.get(spec.feederHandle) ?? null : null}
        gone={going}
        onOpenTile={actions.openTile}
        onOpenCard={actions.openCard}
      />
    );
  };

  let content: ReactNode = null;
  let layerHeight: number | string = 0;
  if (blank) {
    content = (
      <div
        data-arrive=""
        className="flex min-h-[50vh] items-center justify-center px-6 py-10"
        style={{
          paddingTop: showProfile ? undefined : (metrics?.insets.top ?? 162) + (column?.topPad ?? 8),
          paddingBottom: (metrics?.insets.bottom ?? 86) + 16,
        }}
      >
        {blank.node}
      </div>
    );
    layerHeight = 'auto';
  } else if (drawn && metrics) {
    content = [...specs.map((spec) => drawSpec(spec, false)), ...gone.map((spec) => drawSpec(spec, true))];
    layerHeight = layout ? layout.totalHeight : Math.max(metrics.height - origin, 0);
  }

  // a wide Day view: the day of the post in view, big in the margin beside the column (when the margin has room)
  let stamp: ReactNode = null;
  if (layout && metrics && wideDay) {
    const gutter = (metrics.width - layout.cardWidth) / 2;
    const width = gutter - 72;
    if (width >= 120) {
      const item = layout.items[Math.min(active, layout.items.length - 1)];
      const day = item?.day ?? null;
      const count = day ? index?.days.find((entry) => entry.day === day)?.count ?? 0 : 0;
      // a column down the layer's left margin, the stamp pinned in it on the focus line (sticky: it rides the page)
      stamp = (
        <div aria-hidden="true" className="pointer-events-none absolute top-0 z-[5]" style={{ left: 24, width, height: layout.totalHeight }}>
          <div className="sticky" style={{ top: layout.focusY - layout.cardHeight / 2, height: layout.cardHeight }}>
            <DayStamp day={day} slot={item?.slot ?? 0} count={count} hidden={Boolean(move) || atProfile} width={width} />
          </div>
        </div>
      );
    }
  }

  // what this render draws (a move out of it starts from these)
  useIsoLayoutEffect(() => {
    if (!moveRef.current) specsRef.current = specs;
  });

  return (
    <div
      ref={rootRef}
      data-tab-scroll="own"
      className="fm-stream relative w-full"
      style={{ minHeight, background: 'var(--st-bg)', overflowAnchor: 'none' }}
    >
      {/* where the chrome sits, in the same terms AppShell and BottomNav place it (safe areas, the PWA fix, the
          header's heights for this tab), and the small viewport: measured on resize, never while scrolling */}
      <div aria-hidden="true" data-still-skip="" className="pointer-events-none invisible fixed left-0 top-0 h-0 w-0 overflow-hidden">
        <div
          ref={probeTopRef}
          className="w-px h-[calc(10px+env(safe-area-inset-top)+var(--pwa-top-fix,0px)+var(--fm-mobile-header-chrome-height,152px))] sm:h-[calc(14px+env(safe-area-inset-top)+var(--pwa-top-fix,0px)+var(--fm-mobile-header-chrome-height,152px))] md:h-[calc(20px+var(--pwa-top-fix,0px)+var(--fm-mobile-header-chrome-height,152px))] lg:h-[calc(20px+var(--pwa-top-fix,0px)+var(--fm-desktop-header-chrome-height,168px))]"
        />
        <div
          ref={probeFoldRef}
          className="w-px h-[calc(10px+env(safe-area-inset-top)+var(--pwa-top-fix,0px)+var(--fm-mobile-header-chrome-compressed-height,68px))] sm:h-[calc(14px+env(safe-area-inset-top)+var(--pwa-top-fix,0px)+var(--fm-mobile-header-chrome-compressed-height,68px))] md:h-[calc(20px+var(--pwa-top-fix,0px)+var(--fm-mobile-header-chrome-compressed-height,68px))]"
        />
        <div ref={probeNavRef} className="w-px h-[calc(86px+env(safe-area-inset-bottom))] md:h-[90px]" />
        <div ref={probeViewRef} className="w-px h-[100svh]" />
      </div>
      {profileHeader}
      <div ref={layerRef} className="relative isolate w-full" style={{ height: layerHeight }} aria-busy={!index && !blank}>
        {snapMarks}
        {content}
        {stamp}
      </div>
    </div>
  );
}

export default memo(FeedStream);
