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
import DayLayer, { note as dayNote, type DayLayerHandle, type DayStart } from '@/components/feed/stream/DayLayer';
import DayStamp from '@/components/feed/stream/DayStamp';
import MoreTile from '@/components/feed/stream/MoreTile';
import PostCell, { PlaceholderCell, type CellShape } from '@/components/feed/stream/PostCell';
import ProfileHeader from '@/components/feed/stream/ProfileHeader';
import SectionHeader from '@/components/feed/stream/SectionHeader';
import { notePageScroll } from '@/components/feed/stream/press';
import { cardPage, frameRect, rankFlight, rankIn } from '@/components/feed/stream/flightParts';
import { profileHeaderHeight } from '@/components/feed/stream/profileHeaderSize';
import {
  groupLabel,
  groupParts,
  highlightOf,
  istDay,
  mediaProxyUrl,
  scopeKey,
  shiftDay,
  ZOOM_ORDER,
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
  READING_FROM,
  THEATRE_FROM,
  skeletonLayout,
  slotId,
  topForAnchor,
  type LayoutItem,
  type StreamInsets,
  type StreamLayout,
} from '@/lib/feedStream/layout';
import { TILE_CLEAR_PX, landingTop, middleOf, placeOf, returnTop, straightBack, type ZoomMemory, type ZoomPlace, type ZoomView } from '@/lib/feedStream/landing';
import {
  CARRY_EASE_CSS,
  CARRY_MS,
  LEAVE_MS,
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
import { daysSettled, getDayPosts, peekStreamIndex, prefetchDays, subscribeStream, useDayPosts } from '@/lib/feedStream/store';
import { HELD, isWarm, warmAhead, warmPictures, warmSpare } from '@/lib/feedStream/warm';
import { usePinch, type PinchDirection, type PinchFocal } from '@/lib/feedStream/usePinch';
import { warmWash } from '@/components/feed/stream/PostMedia';
import { breakpointOf, initialMode, springBack, stepMode, toggledMode, writeStoredMode, type ViewBreakpoint } from '@/lib/feedStream/zoom';
import { useAppHaptics } from '@/lib/haptics';
import { acquireRootPageScroll } from '@/lib/rootScrollMode';
import { pinToScreen, stillFrame, unsnap, whenDrawn, type StillFrame } from '@/lib/feedStream/stillFrame';
import { markScrollJump, type ScrollJump } from '@/lib/useCompressedOnScroll';
import { TEMPO, ms } from '@/lib/tempo';
import { afterDrawn } from '@/lib/webkitMotion';
import { LAUNCH_CLASS, announceFirstScreen, launchHolds } from '@/lib/launch';

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
// a scope never loaded before: how long the screen holds for its index, then for the posts and pictures it opens on
const INDEX_WAIT_MS = 1200;
const COLD_WAIT_MS = 700;
// the tab opening waits at most this long for its first screen's posts and their pictures (its cells held unseen)
const ARRIVAL_WAIT_MS = 700;
// ...and, its pictures' wait over, holds on (still unseen) till that screen's posts are all in, at most this long from
// the start: an entrance begun on placeholders would turn them into posts under it
const ARRIVAL_MAX_MS = 1500;
/* ...and, launched on what the device kept from last time, waits this long (from the start) for the fresh index and
   the screen's days: an entrance played on the kept ones would be laid out again as they land, under the eye */
const KEPT_WAIT_MS = 1200;
// ...and decodes only that screen: half a screen either side of its middle post (a full-size decode() costs memory a
// phone hasn't much of; the rest decode as their <img>s, at the size they're drawn)
const ARRIVAL_REACH = 0.5;
// several zooms warmed at once (the phone's menu opening) decode at most this many pictures between them (half of what
// warm.ts holds: the likely next zoom's are never pushed out); the rest are only fetched
const MENU_DECODES = HELD / 2;
// at rest, the other zooms are made ready where each would open: MONTH first (its highlights come from all over each
// month: the most days to load), then the other
const PREWARM_ORDER: readonly FeedMode[] = ['month', 'week', 'today'];
// ...on an idle moment (where there is no idle callback, Safari: after this quiet spell)
const PREWARM_QUIET_MS = 1200;
// the most of its pictures a stopped warm-up leaves for the next one to ask for again
const PREWARM_LEFT_MAX = 128;
// a new pick asks first for the days around the one you were on: this many, the first this many ahead of it
const SEED_DAYS = 18;
const SEED_AHEAD = 3;
// a move into the Day view: the spotlight (the row's reading) comes on this far into it
const SPOT_AFTER_MS = ms(200);
// a post replacing its placeholder (its day just loaded) dissolves in this long; more than SWAP_IN_MAX on screen at
// once come in as they are (a dissolve each is a layer each, in a heavy commit)
const SWAP_IN_MS = ms(200);
const SWAP_IN_EASE = 'cubic-bezier(0.2, 0.7, 0.2, 1)';
const SWAP_IN_MAX = 8;
// above everything in a post's frame (its ring, its dimming, a tile's button)
const SWAP_COVER_Z = '7';
// a post carried further than this many screens goes only this many screens of the way, fading (out, or in)
const FAR = 1.15;
const PART = 0.4;
// the longest a carry takes (a post crossing the whole screen)
const LONG_CARRY_MS = ms(760);
// how long a zoom's focal is held where it landed while its days come in (focalHoldRef)
const FOCAL_HOLD_MS = 1200;
// out of a phone's Day view, a card's ground (its colour under its picture) is gone this soon
const GROUND_OUT_MS = ms(140);
// a zoom's new headings come in this long after the old ones set off (their quick leave is most of the way out by then)
const WORDS_AFTER_MS = ms(150);
/* A post carried between a grid and the Day view changes its chrome on the way, on its carry's own clock: a tile's
   (its shadows, its number, its name) goes with the picture it lies on and is gone by CHROME_OUT of the way; what it
   gets comes in from CHROME_IN and is whole as the picture lands. Before, the new chrome was a CSS entrance that set
   off a third of a second in and ran on a quarter of a second past the landing: every tile's shadow and number came
   in after its picture, one after another, and the scroll's hand-over (its light band under Safari's bars) waited on
   it */
const CHROME_OUT = 0.3;
const CHROME_IN = 0.45;
// (a card's words one after another, this much of the way apart, from a little below where they stand)
const CHROME_STEP = 0.06;
const CHROME_RISE_PX = 10;
const CHROME_EASE = 'cubic-bezier(0.2, 0.7, 0.2, 1)';
// out of a card, the ground under the grid goes to the page's own from this far into the move (watchCardEdge's dark)
const GROUND_LIGHT_AT = 0.45;
// said on window as a move's pieces set off (the dock's frost waits for it: shell/Dock)
const PAGE_MOVES_EVENT = 'fm:page-moves';
// a live pinch past the last zoom only gives this much (a rubber band)
const PINCH_EDGE = 0.35;
// a live pinch this far from 1 has shown which way it is going
const PINCH_WARM_AT = 0.04;
/* The live pinch is a paused animation on the page, its time set to the fingers' scale, never an inline scale: WebKit
   draws a page whose scale is written frame by frame afresh at every scale (the whole screen, every frame: a pinch
   on Safari dropped most of its frames), and one an animation scales once, the compositor scaling it from there. One
   way at a time (smaller, or larger), so a pinch together is never drawn larger than the page is */
const PINCH_LOW = 0.6;
const PINCH_HIGH = 1.6;
const PINCH_SPAN_MS = 1000;
// fingers come down a hair either side of 1: nothing is set up until the pinch is this far from it, and it turns the
// other way only once it is this far past 1 (each turn draws the page afresh)
const PINCH_BAND = 0.05;
const NO_DAYS: string[] = [];
// the page's own colour while a phone's Day card is up (globals.css, [data-feed-card]): the cards' dark
/* A move between a phone's Day cards and a grid: the colours under the clock follow the card the move carries (the
   stream's own ground, [data-card-edge]/[data-card-dim] in stream.css, and the page's colours Safari tints its bars
   from). Mid-move a card is its 4:5 (the picture drawn on up under the clock, its head, comes back once it rests), so
   the strip over it was the stream's light ground: a white band over the dark card from a zoom's first frame out of
   the Day view (the card held still meanwhile: WebKit's start) and through the last ~13 frames of one into it, then a
   snap to dark as it landed. Where the card is drawn is read each frame, until it crosses; the move hands the colours
   back as it lands */
const EDGE_SLACK_PX = 8;
function watchCardEdge(flight: Flight, root: HTMLElement, frame: HTMLElement, rest: Rect, toCard: boolean, vw: number) {
  const covers = () => {
    const box = frame.getBoundingClientRect();
    return box.top <= rest.top + EDGE_SLACK_PX && box.left <= EDGE_SLACK_PX && box.right >= vw - EDGE_SLACK_PX;
  };
  let raf = 0;
  // (once the card covers the screen's top, or once it no longer does)
  const when = (covering: boolean, then: () => void) => {
    const look = () => {
      raf = 0;
      if (flight.ended) return;
      if (covers() === covering) {
        then();
        return;
      }
      raf = requestAnimationFrame(look);
    };
    look();
  };
  flight.cleanups.push(() => {
    if (raf) cancelAnimationFrame(raf);
  });
  /* Safari tints the strip under its clock from the page's colours (cardPage, a glide: globals.css), over whatever is
     under it. Coming to a card they go to the cards' dark as it sets off, with the ground the other posts leave into
     (stream.css, [data-card-dim]), so the tint darkens as the card grows. Turned only once the card covered the top, it
     lay over the card's head as a light grey bar for the last third of the move, then went dark */
  if (toCard) {
    cardPage(true);
    root.toggleAttribute('data-card-dim', true);
    when(true, () => root.toggleAttribute('data-card-edge', true));
    return;
  }
  /* Going to a grid, the cards' dark stays the ground the whole way, under the grid coming in, and goes to the page's
     own only as the move lands, as a glide (stream.css), by then behind a screen of posts. Handed back the moment the
     card's top left the screen's, it put the page's light all round the card as it set off: in the light theme a white
     screen for a third of a second, the posts above and below coming in over it. Safari's tint does go then, gliding to
     the page's own while the posts come in under it, so it is the page's as the move lands: turned after, it went light
     over the landed grid, and the band the scroll's hand-over leaves (shadeBand) showed dark under it */
  cardPage(true);
  root.toggleAttribute('data-card-edge', true);
  when(false, () => cardPage(false));
}
// a phone's Day card snaps by its head: this much of it
const MARK_HEAD = 120;
const SNAP_CLASS = 'fm-feed-root-scroll';
/* WebKit (every iOS browser, Safari on a Mac) draws the stretch of page a scroll the page makes itself lands on only
   once its UI process has scrolled there: a jump further than it has drawn around the screen shows the page's black
   for 2–7 frames. So there a move that lands the page elsewhere holds the scroll (the page drawn where it lands by an
   offset, nothing scrolls) and hands it over when it has landed, under a still copy of the screen (stillFrame).
   (Measured again on the iPhone simulator, Oct 22: the hand-over blanks the Feed for 3–7 frames whenever it comes,
   as its pieces set off too, and in steps; only the copy hides it. Safari's bars, which no fixed copy reaches, still
   show it in their strips) */
const HOLD_FROM_PX = 240;
/* The copy stays until the page under it is drawn: its frames coming at their pace again (at most STILL_SETTLE_MS), and
   at least STILL_HOLD_MS after the scroll was handed over, which is how long WebKit can take to draw a phone's screen of
   pictures where the page now is (measured: ~260ms on the iPhone simulator, a dev build). Then it dissolves (an
   identical page under it: nothing seen), quicker under a finger */
const STILL_SETTLE_MS = 600;
const STILL_HOLD_MS = 450;
/* Meanwhile WebKit shows, where it hasn't drawn yet, the colour of the surface the page lies on (the app's page,
   AppShell's [data-route-surface]: the page's own light). The still copy hides that everywhere but the strips under
   Safari's status bar and toolbar, where nothing fixed is drawn: a band of the light page there for 3–6 frames after
   every held move had landed (measured: 242 under a Day card, 243 over a week's tiles). Handed over, the surface is
   the cards' dark until the page is drawn again (globals.css, [data-handing]) */
let bandAsk = 0;
function shadeBand(page: HTMLElement | null) {
  const html = document.documentElement;
  // (the band lies under Safari's tint, which is the page's colours: the cards' dark under a card, else the page's own,
  // which is the surface's already)
  if (html.dataset.feedCard === undefined) return;
  const ask = (bandAsk += 1);
  html.dataset.handing = '';
  const clear = () => {
    if (ask === bandAsk) delete html.dataset.handing;
  };
  if (!page) {
    window.setTimeout(clear, STILL_SETTLE_MS);
    return;
  }
  void whenDrawn(page, STILL_SETTLE_MS).then(() => requestAnimationFrame(() => requestAnimationFrame(clear)));
}
/* The copy shows everything at its end (stillFrame: no animation plays in it), so what is still on its way when the
   pieces are home (a picture developing, a reading coming in) would all snap there at once as it
   goes up: it is let finish first, the page held still meanwhile, at most this long */
const LAND_SETTLE_MS = ms(450);
// an even dissolve: pictures that finished loading under the copy come up gently, not in one frame (left at its own
// pace, not TEMPO's: it dissolves over an identical page, and a copy over a live page should be gone soon)
const STILL_OUT_MS = 240;
const STILL_OUT_EASE = 'cubic-bezier(0.4, 0, 0.6, 1)';
const STILL_TOUCH_OUT_MS = 90;
/* A scroll the page makes itself further than this many screens (Feed tapped again, a highlight) goes on WebKit as a
   jump under a copy of the screen, which drifts the way the page goes from the tap on, stays whole while WebKit draws
   where the page now is, then dissolves: a glide that long outruns WebKit's drawing (blank for a moment mid-way) */
const GLIDE_MAX_SCREENS = 1.2;
const STILL_FADE_HOLD_MS = 300;
const STILL_FADE_MS = ms(280);
const STILL_FADE_DRIFT = 56;
const STILL_FADE_EASE = 'cubic-bezier(0.3, 0, 0.5, 1)';
// under the header (z 100) and the nav (z 180)
const STILL_Z = 90;
/* ...and short of the screen's foot by this much (stillFrame's foot): one running on past it is taken by Safari 26 for a
   bar of the page's, and its toolbar filled solid in its colour as a move landed (measured: a light band over the
   screen's foot for 3 frames landing on Month, 8 landing on a Day card). Short of it, the toolbar stays clear glass over
   the page under it (as a tab's copies do: tabPush BAR_CLEAR) */
const STILL_FOOT = 12;
/* A new order (Newest posts · Latest results) slides in sideways, as a tab's pages do: the list you were on (a still
   copy of it) goes out one side as the new one comes in from the other, edge to edge, on the carry curve, the way the
   tab you tapped lies. The profile and its tabs stay themselves. (Someone else's posts never slide: the cards come and
   go in place, as they always have) */
const SLIDE_MS = ms(460);
// how far a finger travels before a touch is a scroll (that lands a slide) rather than a tap (that may turn it round)
const TAP_SLOP_PX = 10;
/* WebKit: a move's pieces wait at their start (at most this long) until the frame that draws the new view is on screen,
   then all set off together (motion.ts, releaseFlight): their clock would otherwise start before that frame is shown */
const FLIGHT_HOLD_MS = 800;
/* Out of a phone's Day card a move holds the scroll no longer: the page goes where the grid opens first, under a still
   copy of the card (it covers the screen, so the few frames WebKit leaves the strips under Safari's bars bare are the
   cards' dark, unseen), and the move sets off once the page is drawn there, the copy gone in that frame. Handed over as
   it landed, the bare strips came over the week's posts: a band of the page's light under the toolbar. The move waits
   at most this long for it */
const COVER_HOLD_MS = 2000;
// said on window as the page jumps under that copy, with how far (the dock moves its frost with it: shell/Dock)
const PAGE_JUMP_EVENT = 'fm:page-jump';
/* A move's own scroll is told to the header as a jump (it never folds or unfolds the header by which way it went), with
   where it lands, until the frame that carries it is on screen (then the usual moment more), at most this long: a slow
   first frame would otherwise have the header read it as the reader's */
const JUMP_MARK_MS = 1000;
/* ...and it is told where the page lands only as the pieces set off (a fold begun in the frame that first draws the new
   view would arrive already folded: WebKit starts its clock before that heavy frame is shown), for this long: the
   move's own scroll events are in by then, and the reader's next scroll counts again */
const JUMP_ANSWER_MS = 240;
// a phone (or a tablet): the profile stands this far under the open header, its face clear of the glass, in every zoom
const PROFILE_HEADROOM = 16;
// a pinch let go waits at most this long for the pictures its warm-up was already decoding (the page held in the
// fingers meanwhile), so the zoom lands on pictures, not dark tiles that develop on the way
const PINCH_PICTURE_WAIT_MS = 120;
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
  // the same commit. profile: the profile is someone else's; slide: a new order, its list coming in from that side (1:
  // from the right; SLIDE_MS)
  commit: (apply: () => void, options: { profile: boolean; slide?: StreamSlide }) => void;
  // a pick or a zoom (or zooms) about to be taken (a hover, a press): its posts and the pictures it opens on are
  // fetched now
  warm: (next: { scope?: StreamScope; modes?: readonly FeedMode[] }) => void;
};

// a new order's list sliding in: from the right (1) or the left (-1)
export type StreamSlide = 1 | -1;

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
  onManage: () => void;
};

// left: where the stream's column starts in the window (past the side rail from a laptop up; 0 on a phone)
type Metrics = { width: number; height: number; insets: StreamInsets; left: number };
// the window the stream is in (its column plus what lies left of it): what its breakpoints and Day rows go by, as the
// app's chrome does
const viewOf = (size: Metrics) => size.width + size.left;
/* where the stream's column starts in the window, from the layout alone: a transform over the page (a pinch shrinking
   it about the fingers, a tab settling in) never moves it, so it never reads as the window changing and laying the
   stream out again */
function columnLeft(el: HTMLElement) {
  let left = 0;
  for (let node: HTMLElement | null = el; node; node = node.offsetParent as HTMLElement | null) left += node.offsetLeft;
  return Math.round(left);
}
const wideOf = (size: Metrics) => viewOf(size) >= READING_FROM;
/* A phone's Day view is a layer over the grid (DayLayer), never the page turned into its cards: on a phone the page's
   own zoom is always a grid's, and Day lies over it. (?day=page: the page's own cards, to compare) */
const dayLayerOn = (size: Metrics | null): boolean =>
  Boolean(size && size.width < THEATRE_FROM && !wideOf(size)) && !(typeof window !== 'undefined' && window.location.search.includes('day=page'));
// a tile Day closes onto where it stands: any of it showing more than this far under the screen's top and over its foot
const DAY_TILE_TOP = 90;
const DAY_TILE_FOOT = 90;
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
      date: string | null;
      feederHandle: string | null;
      // the feed is one feeder's
      solo: boolean;
    }
  | { kind: 'placeholder'; key: string; mode: FeedMode; shape: CellShape; x: number; y: number }
  | { kind: 'section'; key: string; x: number; y: number; w: number; h: number; heading: Heading; count: number; best: number | null; order: FeedOrder }
  | { kind: 'more'; key: string; x: number; y: number; w: number; h: number; day: string; more: number };

// where a cell and its layers were on screen
// (a phone card going under the next one in the Day view: how dimmed it was as the move began)
type Shot = Partial<Record<'cell' | 'frame' | 'bg' | 'chips' | 'chrome' | 'reading', Rect>> & { dim?: number; grown?: number; below?: number };

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
  // a new order sliding in (StreamSlide), with the still copy of the list it slides out, and where the list stood when
  // it began (another order's slide cut short mid-way: on from there, not from rest)
  slide: { dir: StreamSlide; cover: StillFrame; from: number } | null;
  // what the new view was laid out from, held until it lands: a day loading mid-glide would turn a placeholder into a
  // post (another element) under the move. What loads meanwhile comes in once it has landed
  posts: ReadonlyMap<string, StreamPost[]>;
  index: StreamIndex;
  // out of a phone's Day card: the still copy of the card the page goes where it lands under, before anything moves
  cover: { remove: () => void } | null;
};

// place: what it is about, when that is known (a month's "+N"; a zoom that waited for its posts or pictures); covered:
// the card's still copy already up (jumpUnderCard)
type ZoomOptions = { focal?: { zid?: string | null; postKey?: string | null }; point?: { x: number; y: number }; remember?: boolean; waited?: boolean; place?: ZoomPlace; covered?: { remove: () => void } };

type PieceActions = {
  openTile: (post: StreamPost, element: HTMLElement) => void;
  // a card that isn't the one in view (a tap on it): the Day view glides it there (true: it is the one in view now,
  // and the tap goes on to read it: PostCell's onFocusCard)
  focusCard: (post: StreamPost, gliding?: boolean) => boolean;
  // a MONTH group's "+N": WEEK, at that month's newest week
  openWeek: (day: string, element: HTMLElement) => void;
  manage: () => void;
  openHighlight: (key: string) => void;
};

/* ── helpers ───────────────────────────────────────────────── */

function readingLine(insets: StreamInsets, folded: boolean): number {
  return (folded ? insets.topCompressed : insets.top) + LINE_GAP;
}

/* The screen a zoom starts from (landing.ts), the page at `scrollY` and the layer's origin at `base`: the header's edge as
   it stands, the nav's, and the middle of the band between them, or where the fingers are */
function zoomView(size: Metrics, scrollY: number, base: number, folded: boolean, point: { x: number; y: number } | null = null): ZoomView {
  const height = window.innerHeight;
  const head = folded ? size.insets.topCompressed : size.insets.top;
  const foot = height - size.insets.bottom;
  return { top: scrollY - base, base, head, foot, height, point: point ? { x: point.x - size.left, y: point.y } : { x: size.width / 2, y: (head + foot) / 2 } };
}

// the space above the profile, under the open header: PROFILE_HEADROOM narrower than a desktop's column
function profilePad(width: number): number {
  return width >= 1024 ? contentBox(width).topPad : PROFILE_HEADROOM;
}

// an element's top in the page, from the offset chain (transforms, like a live pinch, don't count)
function pageTop(el: HTMLElement): number {
  let top = 0;
  for (let node: HTMLElement | null = el; node; node = node.offsetParent as HTMLElement | null) top += node.offsetTop;
  return top;
}

function sameInsets(a: StreamInsets, b: StreamInsets) {
  return Math.abs(a.top - b.top) < 0.5 && Math.abs(a.topCompressed - b.topCompressed) < 0.5 && Math.abs(a.bottom - b.bottom) < 0.5 && Math.abs((a.dock ?? 0) - (b.dock ?? 0)) < 0.5 && Math.abs((a.screen ?? 0) - (b.screen ?? 0)) < 0.5 && Math.abs((a.over ?? 0) - (b.over ?? 0)) < 0.5 && Math.abs((a.under ?? 0) - (b.under ?? 0)) < 0.5;
}

/* How much of the page an iPhone's browser shows past the viewport it reports: over it, under the status bar, and under
   it, round the toolbar (iOS draws the page behind both, and reports neither: every safe area is 0 there). The tallest
   status bar there is, and the rest of the screen past the large viewport and the shortest one: what is a little more
   than shows only ever falls off the screen. Installed, the page is the whole screen: none */
const BAR_OVER = 62;
const BAR_OVER_LEAST = 47;
function pageBeyond(screen: number): { over: number; under: number } {
  const ios = typeof navigator !== 'undefined' && /iP(hone|od)/.test(navigator.userAgent);
  if (!ios || inStandalone()) return { over: 0, under: 0 };
  return { over: BAR_OVER, under: Math.max(0, Math.round(window.screen.height - screen - BAR_OVER_LEAST)) };
}
function inStandalone() {
  return window.matchMedia('(display-mode: standalone)').matches || Boolean((navigator as Navigator & { standalone?: boolean }).standalone);
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

// an animation that follows the scroll (a scroll-driven one): never one of a move's or an entrance's, and it has no end
function followsScroll(animation: Animation): boolean {
  return Boolean(animation.timeline) && !(animation.timeline instanceof DocumentTimeline);
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

type Place = { day: string | null; zid: string | null };

// where a place is in a layout: its post, else its day, else the top
function placeIn(layout: StreamLayout, place: Place): LayoutItem | undefined {
  const focal = place.zid ? indexOfSlot(layout, place.zid) : -1;
  return layout.items[focal >= 0 ? focal : place.day ? Math.max(0, indexForDay(layout, place.day)) : 0];
}

/* What a view would show first: the posters near where it opens (a post, or a day) for a scope at a zoom, nearest it
   first, and the days it needs that haven't loaded (and where in it that place is: y, its middle; and the layout).
   Laid out exactly as the page would be, so a pick or a zoom can have its pictures ready before anything moves. reach:
   how far either side of the place, in screens. null: the scope's index isn't here yet */
function viewPictures(scope: StreamScope, mode: FeedMode, size: Metrics, place: Place, reach = 1): { srcs: string[]; missing: string[]; y: number; layout: StreamLayout } | null {
  const index = peekStreamIndex(scope);
  if (!index) return null;
  const posts = loadedPosts(scope, index);
  const layout = computeLayout({ mode, days: index.days, posts, width: size.width, height: size.height, insets: size.insets, wide: wideOf(size) });
  const item = placeIn(layout, place);
  if (!item) return { srcs: [], missing: [], y: 0, layout };
  const middle = item.y + item.h / 2;
  const [first, last] = itemRange(layout, middle - size.height * reach, middle + size.height * reach);
  const near: Array<{ src: string; far: number }> = [];
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
    if (src) near.push({ src, far: Math.abs(entry.y + entry.h / 2 - middle) });
  }
  near.sort((a, b) => a.far - b.far);
  return { srcs: near.map((entry) => entry.src), missing: [...missing], y: middle, layout };
}

// every post on a screen (the layer from `top`, `height` tall) has its day loaded: none is drawn as a placeholder
function screenLoaded(layout: StreamLayout, top: number, height: number): boolean {
  const [first, last] = itemRange(layout, top, top + height);
  for (let at = first; at < last; at += 1) {
    const item = layout.items[at];
    if ((item.kind === 'tile' || item.kind === 'reel') && !item.postKey) return false;
  }
  return true;
}

/* A held move's pieces home: whatever in the page is still running (a picture developing, a reading coming in), let
   finish before the still copy is taken (LAND_SETTLE_MS at most; never what loops for good). Resolves at once under a
   finger or a wheel, and when the flight ends (another move, the tab hidden) */
function whenSettled(flight: Flight, root: HTMLElement): Promise<void> {
  const running = (animation: Animation) => {
    if (animation.playState !== 'running') return false;
    const end = animation.effect?.getComputedTiming().endTime;
    const now = animation.currentTime;
    return typeof end === 'number' && Number.isFinite(end) && typeof now === 'number' && end > now;
  };
  const settling = new Set<Animation>();
  document.getAnimations().forEach((animation) => {
    const target = animation.effect instanceof KeyframeEffect ? animation.effect.target : null;
    if (target && root.contains(target) && running(animation)) settling.add(animation);
  });
  if (!settling.size || flight.ended) return Promise.resolve();
  return new Promise((resolve) => {
    let over = false;
    let timer = 0;
    const listen = { capture: true, passive: true } as const;
    const done = () => {
      if (over) return;
      over = true;
      window.clearTimeout(timer);
      window.removeEventListener('touchstart', done, listen);
      window.removeEventListener('wheel', done, listen);
      resolve();
    };
    timer = window.setTimeout(done, LAND_SETTLE_MS);
    window.addEventListener('touchstart', done, listen);
    window.addEventListener('wheel', done, listen);
    flight.cleanups.push(done);
    void Promise.allSettled(Array.from(settling, (animation) => animation.finished)).then(done);
  });
}

// `run` once the page is idle (Safari has no idle callback: after a quiet spell); returns a cancel
function whenIdle(run: () => void): () => void {
  if (typeof window.requestIdleCallback === 'function') {
    const id = window.requestIdleCallback(run, { timeout: 4000 });
    return () => window.cancelIdleCallback(id);
  }
  const id = window.setTimeout(run, PREWARM_QUIET_MS);
  return () => window.clearTimeout(id);
}

/* Resolves once a view is ready to be shown (its days in, the pictures it opens on decoded) or the budget is spent */
// a scope's index in (asked for if it isn't), or `budget` gone
function indexReady(scope: StreamScope, budget: number): Promise<void> {
  return new Promise((resolve) => {
    if (peekStreamIndex(scope)) {
      resolve();
      return;
    }
    let unsubscribe: (() => void) | null = null;
    let timer = 0;
    let done = false;
    const finish = () => {
      if (done) return;
      done = true;
      unsubscribe?.();
      window.clearTimeout(timer);
      resolve();
    };
    timer = window.setTimeout(finish, budget);
    unsubscribe = subscribeStream(() => {
      if (peekStreamIndex(scope)) finish();
    });
  });
}

function viewReady(scope: StreamScope, mode: FeedMode, size: Metrics, place: Place, budget: number, reach = 1): Promise<void> {
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
      const view = viewPictures(scope, mode, size, place, reach);
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
  // the Day view's stack: each card's place in it (--st-z), the yield the scroll gives it, and its shot
  const stack: Array<{ z: number; by: number; opacity: number; cell: Rect; shot: Shot }> = [];
  layer.querySelectorAll<HTMLElement>(':scope > [data-cell]').forEach((el) => {
    if (el.dataset.gone !== undefined) return;
    const cell = rectOf(el);
    const shot: Shot = { cell };
    el.querySelectorAll<HTMLElement>(':scope > [data-layer], :scope > [data-layer="frame"] > [data-layer]').forEach((part) => {
      const name = part.dataset.layer ?? '';
      if (name === 'frame') {
        shot.frame = frameRect(part);
        shot.grown = Number(part.dataset.grown) || 0;
        shot.below = Number(part.dataset.below) || 0;
      }
      else if (name === 'bg' || name === 'chips' || name === 'chrome' || name === 'reading') shot[name] = rectOf(part);
    });
    // a phone card going under the next one in the Day view (its boxes above are measured as it stands): how dimmed
    if (el.dataset.depth !== undefined) {
      const style = getComputedStyle(el);
      const dim = el.querySelector<HTMLElement>(':scope > [data-depth-dim]');
      shot.dim = dim ? Number(getComputedStyle(dim).opacity) || 0 : 0;
      const by = style.transform === 'none' ? 0 : new DOMMatrixReadOnly(style.transform).m42;
      stack.push({ z: Number(el.style.getPropertyValue('--st-z')) || 0, by, opacity: Number(style.opacity), cell, shot });
    }
    shots.set(el.dataset.cell ?? '', shot);
  });
  /* One wholly unseen (faded, or under the cards over it in the stack: none of it on screen above the next one's top) is
     where the scroll alone would have it: over the top of the screen. Measured where it stood, it was carried, chrome and
     all, from under the card in view: once the move turns the stack off, the card that card covered showed over the grid
     coming in, or flew up out from behind it */
  const vh = typeof window === 'undefined' ? 0 : window.innerHeight;
  stack.forEach((card) => {
    const { shot, by, cell } = card;
    if (!by) return;
    // (the cards over it reach up as far as their pictures do: drawn up under the status bar, over the cell's top)
    const over = stack.reduce((top, other) => (other.z > card.z ? Math.min(top, other.cell.top, other.shot.frame?.top ?? Infinity) : top), Infinity);
    const seen = Math.min(cell.top + cell.height, over, vh) - Math.max(cell.top, 0);
    if (card.opacity >= 0.05 && seen > 1) return;
    (['cell', 'frame', 'bg', 'chrome'] as const).forEach((name) => {
      const rect = shot[name];
      if (rect) shot[name] = { ...rect, top: rect.top - by };
    });
    shot.dim = 0;
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
  // on screen at both ends, it makes the whole trip however far (part of the way would end in a pop)
  if (distance <= FAR * vh || (before && after)) {
    carry(flight, el, now, was, now, { picture, duration: carryTime(was, now) });
    return;
  }
  const fraction = (PART * vh) / distance;
  if (before) carry(flight, el, now, was, partway(was, now, fraction), { picture, fade: 'out' });
  else carry(flight, el, now, partway(now, was, fraction), now, { picture, fade: 'in' });
}

// how long a layer's whole trip takes: a long one a little longer, so nothing ever streaks across the screen
function carryTime(was: Rect, now: Rect): number {
  const a = centreOf(was);
  const b = centreOf(now);
  return Math.min(LONG_CARRY_MS, CARRY_MS + Math.max(0, Math.hypot(b.x - a.x, b.y - a.y) - 480) * 0.3 * TEMPO);
}

// a zoom that takes `from` (a post's tile) onto `to` (its card): where it puts any other rect of the same view
function zoomMap(from: Rect, to: Rect): (rect: Rect) => Rect {
  const k = to.width / Math.max(from.width, 0.01);
  return (rect) => ({ left: to.left + (rect.left - from.left) * k, top: to.top + (rect.top - from.top) * k, width: rect.width * k, height: rect.height * k });
}

// ...and how long carryLayer takes it (part of the way, from or to far off screen, a plain carry's time; 0: not at all)
function tripOf(was: Rect, now: Rect, vw: number, vh: number): number {
  const before = onScreen(was, vw, vh);
  const after = onScreen(now, vw, vh);
  if (!before && !after) return 0;
  const a = centreOf(was);
  const b = centreOf(now);
  return Math.hypot(b.x - a.x, b.y - a.y) <= FAR * vh || (before && after) ? carryTime(was, now) : CARRY_MS;
}

/* The chrome a post gets as a move carries it between a grid and the Day view, in on its carry's clock (`duration`):
   a tile's (in its frame, so it rides the picture's box) as one, a card's or a row's words one after another, each from
   a little below; all of it whole as the picture lands */
function bringChrome(flight: Flight, el: HTMLElement, duration: number) {
  const chips = el.querySelector<HTMLElement>(':scope > [data-layer="frame"] > [data-layer="chips"]');
  if (chips) run(flight, chips, [{ opacity: 0 }, { opacity: 0, offset: CHROME_IN, easing: CHROME_EASE }, { opacity: 1 }], { duration, fill: 'backwards' });
  const words = el.querySelectorAll<HTMLElement>(':scope > :is([data-layer="chrome"], [data-layer="reading"]) :is([data-st], [data-st-fade])');
  const low = `translate3d(0px, ${CHROME_RISE_PX}px, 0px)`;
  words.forEach((word) => {
    if (word.dataset.stFade !== undefined) {
      run(flight, word, [{ opacity: 0 }, { opacity: 0, offset: CHROME_IN, easing: CHROME_EASE }, { opacity: 1 }], { duration, fill: 'backwards' });
      return;
    }
    const from = Math.min(0.8, CHROME_IN + (Number(word.style.getPropertyValue('--i')) || 0) * CHROME_STEP);
    run(flight, word, [
      { offset: 0, opacity: 0, transform: low },
      { offset: from, opacity: 0, transform: low, easing: CHROME_EASE },
      { offset: 1, opacity: 1, transform: 'translate3d(0px, 0px, 0px)' },
    ], { duration, fill: 'backwards' });
  });
}

/* A tile's own chrome (its `-out` copy) as its post grows into a card: carried with the picture's box (from where it
   stood, `was`, to the frame's place in the card, `to`), gone by CHROME_OUT of the way. Held where it stood, the number
   and the shadow were left behind as the picture flew off from under them */
function rideOut(flight: Flight, el: HTMLElement, at: Rect, was: Rect, to: Rect, duration: number) {
  if (at.width <= 0 || at.height <= 0) return;
  carry(flight, el, at, was, to, { duration });
  run(flight, el, [{ opacity: 1 }, { opacity: 0, offset: CHROME_OUT }, { opacity: 0 }], { duration, easing: 'linear', fill: 'both' });
}

/* A layer of the view being left, held where it stood and let go (from as faded as it stood: a card sitting back) */
function letGo(flight: Flight, el: HTMLElement, was: Rect | undefined, now: Rect = rectOf(el), opacity?: number) {
  if (!was) return;
  leave(flight, el, now, was, null, { quick: true, opacity });
}

/* ── the Day view's stack (a phone's: stream.css, st-yield) ──
   The next card comes up over the one in view; the one in view gives way under it and darkens a touch. The scroll draws
   it (a view timeline); a move turns it off while it flies, letting a card's dimming go from where it was */

/* A card that was going under the next one as a move began (its shot's dim): the dimming is let go as it flies; one
   leaving keeps it as it goes */
function easeDepthOut(flight: Flight, el: HTMLElement, shot: Shot | undefined, leaving: boolean) {
  if (!shot || !shot.dim || shot.dim < 0.01) return;
  const dim = el.querySelector<HTMLElement>(':scope > [data-depth-dim]');
  if (!dim) return;
  const timing: KeyframeAnimationOptions = leaving
    ? { duration: LEAVE_MS, easing: 'linear', fill: 'both' }
    : { duration: CARRY_MS, easing: CARRY_EASE_CSS, fill: 'backwards' };
  run(flight, dim, [{ opacity: shot.dim }, { opacity: leaving ? shot.dim : 0 }], timing);
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
  onManage,
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
  const probeDockRef = useRef<HTMLDivElement | null>(null);
  const probeViewRef = useRef<HTMLDivElement | null>(null);
  const probeScreenRef = useRef<HTMLDivElement | null>(null);
  const [profileEl, setProfileEl] = useState<HTMLDivElement | null>(null);
  /* the profile element as it is in the page right now: a pick for someone else mounts a new one (key: who) in the very
     commit that starts the move, so the move (and what starts one) reads it here, never from a render's state, which
     still holds the one that just left */
  const profileRef = useRef<HTMLDivElement | null>(null);
  const attachProfile = useCallback((el: HTMLDivElement | null) => {
    profileRef.current = el;
    setProfileEl(el);
  }, []);

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
  // the last move to land: what it was laid out from (shown on while nothing under it has changed: see shownPosts)
  const [landed, setLanded] = useState<{ gen: string; posts: ReadonlyMap<string, StreamPost[]>; index: StreamIndex } | null>(null);
  // the tab's opening coming in: what its first screen was laid out from, held (like a move's) until its entrance is
  // done. A day or a fresh index landing mid-way would turn an entering placeholder into its post (another element,
  // there at once) or move the column under it; what came in meanwhile is let in once it is over
  const [entrance, setEntrance] = useState<{ gen: string; posts: ReadonlyMap<string, StreamPost[]>; index: StreamIndex } | null>(null);

  // what the handlers read (kept current after every commit, never written during render)
  const layoutRef = useRef<StreamLayout | null>(null);
  const metricsRef = useRef<Metrics | null>(null);
  const originRef = useRef(0);
  // where the stream's column starts on screen: 0 on a phone, past the side rail from a laptop up (SideRail). The
  // layout's x is the column's; a point on screen (a tap, a pinch, the middle of the view) is this much further right
  const leftRef = useRef(0);
  const modeRef = useRef<FeedMode | null>(null);
  const genRef = useRef(gen);
  const inputsRef = useRef<{ index: StreamIndex | null; posts: ReadonlyMap<string, StreamPost[]>; scope: StreamScope }>({ index: null, posts: new Map(), scope });
  const compressedRef = useRef(headerCompressed);
  const specsRef = useRef<Spec[]>([]);
  // what the last commit mounted (and the window it was drawn with, when it was the window asked for)
  const renderedRef = useRef<{ layout: StreamLayout; gen: string; from: number; to: number; win: Win | null } | null>(null);
  const dayWinRef = useRef<DayWin | null>(null);
  const wideRef = useRef<{ layout: StreamLayout; from: number; to: number } | null>(null);
  // the scroll's speed (px a ms, read once a frame) and the pictures last asked for ahead of it
  const speedRef = useRef({ y: 0, t: 0, v: 0 });
  const aheadRef = useRef<{ layout: StreamLayout; from: number; to: number; land: number } | null>(null);
  const callbacksRef = useRef({ onModeChange, onManage, play });
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
  // the tab's opening (its first posts coming in, ring): a move started meanwhile ends it, so no piece answers to both
  const openingRef = useRef<Flight | null>(null);
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
  // the hand-over a finger put off till the page is at rest (landCarried), for settle, declared before it
  const landCarriedRef = useRef<() => void>(() => undefined);
  // a zoom waiting for its posts or pictures: its token
  const zoomWaitRef = useRef<object | null>(null);
  const warmingRef = useRef(new Map<string, () => void>());
  // a new pick being made ready: it, and where it opens (the day you were on, or where you are in the profile)
  const pickRef = useRef<{ next: StreamScope; day: string | null; keepScroll: boolean } | null>(null);
  const seekRef = useRef<{ key: string; until: number } | null>(null);
  const arrivedRef = useRef(false);
  // the tab opening, its cells held unseen until its first screen's posts are in (a token: none, or a newer one, and
  // that wait is over); whether the last commit drew the skeleton (a launch with no index yet); the window still narrow
  const holdRef = useRef<object | null>(null);
  const skeletonPaintedRef = useRef(false);
  const narrowRef = useRef(true);
  // the other zooms being made ready at rest (its stop), and per zoom where it last was (the layout's size, the place)
  const prewarmRef = useRef<(() => void) | null>(null);
  const prewarmedRef = useRef(new Map<FeedMode, { key: string; y: number; layout: StreamLayout }>());
  // ...and the pictures a stopped one hadn't fetched yet (the next asks for them again, with no layout of its own)
  const prewarmLeftRef = useRef<string[]>([]);
  // a zoom straight back (the page not moved since the last one landed) keeps that zoom's focal, where it stood
  const lastZoomRef = useRef<ZoomMemory | null>(null);
  /* the post a zoom was about (what was pinched or tapped), held where it landed while its days come in: their posts can
     arrange a week's tiles differently from the index the zoom laid them out from, and the piece at the reading line
     holding still (below) would let the focal itself move rows. Let go once you scroll, or a few seconds on */
  const focalHoldRef = useRef<{ id: string; postKey: string | null; until: number; scroll: number | null } | null>(null);
  // the slots drawn as placeholders last time (day:slot): one drawn as its post now has had its day load; and the
  // placeholders' copies still dissolving off the posts that took their place
  const placeholdersRef = useRef<{ view: string; slots: Set<string> }>({ view: '', slots: new Set() });
  const coversRef = useRef(new Set<HTMLElement>());
  // a phone's Day view, the layer over the grid (DayLayer): up, from which post; its handle
  const [dayOpen, setDayOpen] = useState<DayStart | null>(null);
  const dayOpenRef = useRef<DayStart | null>(null);
  const dayLayerRef = useRef<DayLayerHandle | null>(null);
  const daySeqRef = useRef(0);
  // where Day's layer is put: the tab's own box, beside the stream (the grid's zoom never carries it)
  const [dayHost, setDayHost] = useState<HTMLElement | null>(null);

  /* ── data ── */

  // the days whose posts are wanted: the window's, or (a new pick's first frames, before it has a window) its seed
  const wantDays = dayWin && dayWin.gen === gen ? dayWin.days : seed && seed.gen === gen ? seed.days : NO_DAYS;
  // every day of the scope that has loaded (the map changes only when one of the wanted days, or the index, does): a
  // loaded day is laid out from its posts wherever it is, as a move lays it out, never from its index entry again
  const posts = useDayPosts(scope, wantDays, { loaded: true });
  /* Once a move has landed, what it was laid out from goes on being shown while every day it draws is as it was
     (nothing loaded or changed under it since; days off screen loading meanwhile change nothing): the same layout, so
     the landing's frame neither lays the feed out again nor reads where the page is after its own writes (WebKit works
     the page's styles out afresh for that). The first day drawn that differs brings in the live posts, for good */
  const held = !move && landed && landed.gen === gen && landed.index === index ? landed : null;
  const opening = !move && entrance && entrance.gen === gen ? entrance : null;
  // through a move, what it was laid out from (no cell changes kind mid-glide); through the tab's opening, its first
  // screen's; once a move lands, that (held) or the live posts
  const frozen = move ?? opening ?? held;
  const shownPosts = frozen?.posts ?? posts;
  const shownIndex = frozen?.index ?? index;

  const layout = useMemo(
    () => (mode && metrics && shownIndex && !blank ? computeLayout({ mode, days: shownIndex.days, posts: shownPosts, width: metrics.width, height: metrics.height, insets: metrics.insets, wide: wideOf(metrics) }) : null),
    [mode, metrics, shownIndex, shownPosts, blank],
  );
  const skeleton = useMemo(
    () => (!index && !blank && mode && metrics ? skeletonLayout(mode, metrics.width, metrics.height, metrics.insets, wideOf(metrics)) : null),
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
    const newest = shownIndex?.days[0];
    const dayPosts = newest ? shownPosts.get(newest.day) : undefined;
    if (!dayPosts?.length) return false;
    const highlight = highlightOf(dayPosts);
    return highlight != null && highlight.key === dayPosts[0].key;
  }, [shownIndex, shownPosts]);

  const drawn = layout ?? skeleton;
  const mountView = drawn && metrics
    ? (layout && win && win.gen === gen && win.mode === layout.mode ? win : mountWindow(drawn, 0, metrics.height, true))
    : null;
  const [mountFrom, mountTo] = drawn && mountView ? itemRange(drawn, mountView.y0, mountView.y1) : [0, 0];
  // (the landed posts let go: another scope or index, or a day it draws loaded or changed since)
  let landedStale = Boolean(landed && !move && !held);
  if (held && frozen === held && drawn) {
    for (let at = mountFrom; at < mountTo && !landedStale; at += 1) {
      const day = drawn.items[at].day;
      if (day && held.posts.get(day) !== posts.get(day)) landedStale = true;
    }
  }
  if (landedStale) setLanded(null);

  /* ── the snap (a phone's Day view) ── */

  const applySnap = useCallback((on: boolean) => {
    snapOnRef.current = on;
    document.documentElement.classList.toggle(SNAP_CLASS, on);
  }, []);

  /* The snap comes on only under a finger: held (inline none) whenever the page turns it on or scrolls itself, and let
     back by the next touch (armSnap). iOS Safari keeps the card the page rests on as a place in its list of snap points
     (the fourth one), set when a swipe or a scroll ends with the snap on, and whenever the page is still it puts the
     page back on that place in the list as it now stands. A scroll made with the snap held never sets it, and the list
     changes as cards mount: let back while still, the page is pulled to whatever card is fourth now (WebKit: WKWebView
     _createVisibleContentRectUpdate, scrollOffsetSnappedToNearestSnapPoint) */
  const holdSnap = useCallback(() => {
    snapHeldRef.current = true;
    holdSnapStyle(true);
  }, []);

  /* ...let back as a finger lands, before it moves the page: iOS snaps a swipe by the snap it began with, and a quick
     flick sends no scroll while the finger is down, so a snap let back on the swipe's first scroll left every flick free
     to coast past card after card. The page is still there, so its place in the list is set first, the one way a page
     can set it: a scroll made with the snap on (a pixel off and back, in the same task: nothing is drawn between) */
  const armSnap = useCallback(() => {
    if (!snapHeldRef.current || !snapOnRef.current || moveRef.current || pinchRef.current || handoverRef.current) return;
    snapHeldRef.current = false;
    const y = window.scrollY;
    holdSnapStyle(false);
    window.scrollTo(0, y + 1);
    window.scrollTo(0, y);
  }, []);

  /* where the page is in its own terms: its scroll, plus the offset a held move draws it at (handed over at rest when a
     finger took the page first: endMove's keepHeld). Everything that maps the scroll onto the layout reads this */
  const pageY = useCallback(() => window.scrollY + (handoverRef.current?.by ?? 0), []);

  /* A move holding the scroll (WebKit), handed over: the offset it was drawn at comes off and the page scrolls by as
     much, so nothing moves on screen. When it lands, under a still copy of the screen; anything that needs the page
     where it really is (another move, a scroll, a pick) takes it over at once first. Never under a finger that is
     moving the page: iOS starts the pan from where the page was, and the scroll made meanwhile is lost (the page
     back where it was, the posts thousands of px off: a blank page, the place lost). So a finger keeps the offset,
     and it is handed over once the page is at rest (landCarried) */
  const handOver = useCallback(() => {
    const pending = handoverRef.current;
    if (!pending) return;
    handoverRef.current = null;
    const root = rootRef.current;
    if (root) root.style.top = '';
    markScrollJump(pending.mode);
    holdSnap();
    shadeBand(root);
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
      const next: Win = { gen: scopeGen, mode: current.mode, y0: view.y0, y1: view.y1, narrow: keepNarrow };
      const was = rendered?.win;
      renderedRef.current = { layout: current, gen: scopeGen, from, to, win: next };
      // (only laid out again, the window the same: what is drawn is this already, no render for it)
      if (!was || was.gen !== next.gen || was.mode !== next.mode || was.y0 !== next.y0 || was.y1 !== next.y1 || was.narrow !== next.narrow) {
        setWin(next);
      }
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

  /* ── made ready ahead ── */

  // the tab opening's hold let go: its cells seen again. done: its arrival is over (a move took over); otherwise it
  // starts again the next time it can (the tab hidden mid-wait, StrictMode's second run, a new scope)
  const dropHold = useCallback((done: boolean) => {
    if (!holdRef.current) return;
    holdRef.current = null;
    if (done) arrivedRef.current = true;
    const layer = layerRef.current;
    if (layer) delete layer.dataset.holding;
  }, []);

  const stopPrewarm = useCallback(() => {
    const stop = prewarmRef.current;
    prewarmRef.current = null;
    stop?.();
  }, []);

  /* The other zooms made ready while the feed is at rest (nothing moving or under a finger, the screen's own posts in),
     so the first switch to one lands on posts and pictures, not on blocks that develop on the way: on an idle moment,
     where each would open from here, one zoom at a time, MONTH first. Its days are asked for (they go after the
     screen's own), and as they come in its pictures are fetched, two at a time behind everything else (warm.ts,
     warmSpare). A zoom is made ready again only once the place has moved a screen or more in it (told on the layout it
     was last made ready on: a reading pause lays nothing out to find it is ready already). A finger, a scroll, an
     intent or a move stops it: the pictures it hadn't fetched yet are let go (the next one asks for them again, after
     its own, with no layout for them), and a zoom it hadn't finished is made ready again next time */
  const prewarm = useCallback(() => {
    stopPrewarm();
    if (!visibleRef.current || !arrivedRef.current) return;
    let stopped = false;
    let finished = false;
    let cancelIdle = () => {};
    let unsubscribe: (() => void) | null = null;
    let frame = 0;
    let deadline = 0;
    // the zooms still to make ready (null: not begun, the screen's own posts not in yet), and the place they open at
    let todo: FeedMode[] | null = null;
    let place: Place = { day: null, zid: null };
    // the days a zoom is waiting on (laid out again once they are all in, not as each one lands)
    let pending: { mode: FeedMode; days: string[] } | null = null;
    // the zooms laid out afresh, and those of them whose pictures have all been asked for
    const marked: FeedMode[] = [];
    const completed: FeedMode[] = [];
    const wanted: string[] = [];
    const queued = new Set<string>();
    // the last one's pictures not yet fetched, taken over (asked for after this one's own)
    let carried = false;
    const stop = () => {
      if (stopped) return;
      stopped = true;
      cancelIdle();
      unsubscribe?.();
      window.clearTimeout(deadline);
      cancelAnimationFrame(frame);
      // what wasn't fetched yet waits for the next one (with the last one's, unless this one took those over)
      const left = warmSpare([]);
      prewarmLeftRef.current = [...new Set(carried ? left : [...left, ...prewarmLeftRef.current])].slice(0, PREWARM_LEFT_MAX);
      if (!finished) marked.forEach((forMode) => {
        if (!completed.includes(forMode)) prewarmedRef.current.delete(forMode);
      });
    };
    const end = () => {
      if (prewarmRef.current === stop) prewarmRef.current = null;
      stop();
    };
    const attempt = () => {
      frame = 0;
      if (stopped) return;
      const current = layoutRef.current;
      const size = metricsRef.current;
      const mode = modeRef.current;
      const resting = visibleRef.current && !moveRef.current && !pinchRef.current && !touchingRef.current && !scrollingRef.current && !zoomWaitRef.current;
      if (!current || !size || !mode || !resting) {
        end();
        return;
      }
      const scope = inputsRef.current.scope;
      if (!todo) {
        // (where the page rests, as last read: no read of the page's own after whatever was last written to it)
        const top = savedYRef.current - originRef.current;
        if (!screenLoaded(current, top, size.height)) return;
        // where a zoom from here opens, as zoomTo places it
        place = placeOf(current, null, zoomView(size, savedYRef.current, originRef.current, compressedRef.current), lastZoomRef.current, savedYRef.current);
        todo = PREWARM_ORDER.filter((other) => other !== mode);
        wait();
      }
      const key = `${genRef.current}|${size.width}|${size.height}`;
      while (todo.length) {
        const forMode = todo[0];
        if (pending && pending.mode === forMode && pending.days.some((day) => !getDayPosts(scope, day))) return;
        pending = null;
        if (!marked.includes(forMode)) {
          // made ready from about here already (where the place is on the layout it was made ready on)
          const last = prewarmedRef.current.get(forMode);
          const was = last && last.key === key ? placeIn(last.layout, place) : undefined;
          if (last && was && Math.abs(was.y + was.h / 2 - last.y) < size.height) {
            todo.shift();
            continue;
          }
        }
        const view = viewPictures(scope, forMode, size, place);
        if (!view) {
          end();
          return;
        }
        if (!marked.includes(forMode)) {
          prewarmedRef.current.set(forMode, { key, y: view.y, layout: view.layout });
          marked.push(forMode);
        }
        view.srcs.forEach((src) => {
          if (queued.has(src)) return;
          queued.add(src);
          wanted.push(src);
        });
        warmSpare(wanted);
        // its days first (again once they are in), then the next zoom
        if (view.missing.length) {
          prefetchDays(scope, view.missing);
          pending = { mode: forMode, days: view.missing };
          return;
        }
        if (marked.includes(forMode)) completed.push(forMode);
        todo.shift();
        wait();
      }
      // the last one's pictures it hadn't fetched yet, after this one's own
      if (!carried) {
        carried = true;
        const before = wanted.length;
        prewarmLeftRef.current.forEach((src) => {
          if (queued.has(src)) return;
          queued.add(src);
          wanted.push(src);
        });
        prewarmLeftRef.current = [];
        if (wanted.length > before) warmSpare(wanted);
      }
      finished = true;
      unsubscribe?.();
      unsubscribe = null;
      window.clearTimeout(deadline);
    };
    // a wait (the screen's own posts, a zoom's days) lasts WARM_MS at most: then on to the next zoom (or, the screen's
    // posts never in, nothing)
    const wait = () => {
      window.clearTimeout(deadline);
      deadline = window.setTimeout(() => {
        if (!todo) {
          end();
          return;
        }
        todo.shift();
        wait();
        attempt();
      }, WARM_MS);
    };
    cancelIdle = whenIdle(() => {
      if (stopped) return;
      unsubscribe = subscribeStream(() => {
        if (!frame && !stopped) frame = requestAnimationFrame(attempt);
      });
      wait();
      attempt();
    });
    prewarmRef.current = stop;
  }, [stopPrewarm]);

  // the scroll came to rest: a phone's Day view, the card on the focus line becomes the one in view
  const settle = useCallback(() => {
    const current = layoutRef.current;
    if (!touchingRef.current) scrollingRef.current = false;
    if (!current || !visibleRef.current || touchingRef.current || moveRef.current || pinchRef.current) return;
    /* a held scroll a finger kept: handed over now the page is at rest (under a copy). A phone's Day view first comes to
       rest on a card: a swipe that ran with the snap still held (one begun before a move landed, or one the snap couldn't
       be let back for: armSnap) is snapped by nothing, and coasts to a stop between two cards. It glides onto the
       nearest, and a held scroll is handed over once that glide is at rest */
    const carried = handoverRef.current;
    if (carried || snapHeldRef.current) {
      const size = metricsRef.current;
      const top = pageY() - originRef.current;
      if (current.mode === 'today' && current.mediaWidth == null && size && !dayAtProfile(current, top)) {
        const rest = dayRestTop(current, dayIndexAt(current, top));
        if (Math.abs(rest - top) > 2) {
          holdSnap();
          scrollingRef.current = true;
          window.scrollTo({ top: originRef.current + rest - (carried?.by ?? 0), behavior: 'smooth' });
          return;
        }
      }
      if (carried) landCarriedRef.current();
    }
    const scrollY = pageY();
    savedYRef.current = scrollY;
    syncFrame(scrollY);
    // at rest: the other zooms are made ready from here (once the place has moved enough in them)
    prewarm();
    if (current.mode !== 'today' || current.mediaWidth != null) return;
    settledTopRef.current = scrollY;
    movingRef.current = false;
    const at = dayIndexAt(current, scrollY - originRef.current);
    if (at !== activeRef.current) {
      activeRef.current = at;
      setActive(at);
      callbacksRef.current.play('snapLock');
    }
  }, [holdSnap, pageY, prewarm, syncFrame]);

  /* ── moves ── */

  // a live pinch's transform comes off the page
  /* a pinch let go short springs the page back (zoom.ts springBack): kept here, so a new pinch or a move takes the page
     at once instead of under a spring still running (its scale would hide theirs, and its end would wipe their origin) */
  const springRef = useRef<Animation | null>(null);
  const cancelSpring = useCallback(() => {
    springRef.current?.cancel();
    springRef.current = null;
  }, []);
  // the live pinch's animation (PINCH_LOW…PINCH_HIGH), which way it runs, and the scale it is held at
  const pinchAnimRef = useRef<{ animation: Animation; from: number; to: number; scale: number } | null>(null);
  const holdPinch = useCallback((root: HTMLElement, scale: number) => {
    let held = pinchAnimRef.current;
    if (!held && Math.abs(scale - 1) < PINCH_BAND) return;
    const wasDown = held ? held.to === 1 : scale <= 1;
    const down = wasDown ? scale < 1 + PINCH_BAND : scale <= 1 - PINCH_BAND;
    if (!held || (held.to === 1) !== down) {
      held?.animation.cancel();
      const [from, to] = down ? [PINCH_LOW, 1] : [1, PINCH_HIGH];
      const animation = root.animate([{ transform: `scale(${from})` }, { transform: `scale(${to})` }], { duration: PINCH_SPAN_MS, easing: 'linear', fill: 'both' });
      animation.pause();
      held = { animation, from, to, scale };
      pinchAnimRef.current = held;
    }
    held.scale = scale;
    held.animation.currentTime = ((clamp(scale, held.from, held.to) - held.from) / (held.to - held.from)) * PINCH_SPAN_MS;
  }, []);
  const dropPinch = useCallback(() => {
    pinchAnimRef.current?.animation.cancel();
    pinchAnimRef.current = null;
  }, []);
  // the page springs back; once it is home (not cut short, no new pinch on it) the backdrop goes and `then` runs
  const springHome = useCallback((root: HTMLElement, scale: number, then?: () => void) => {
    // (the pinch's own animation hands over to the spring in the same frame)
    dropPinch();
    const spring = springBack(root, scale);
    springRef.current = spring.animation;
    void spring.done.then((ran) => {
      if (!ran || springRef.current !== spring.animation || pinchRef.current) return;
      springRef.current = null;
      pinchBackdrop(root, false);
      then?.();
    });
  }, [dropPinch]);

  // the Day view's snap back on once a pinch is over without a zoom (the pinch held it off)
  const resnap = useCallback(() => {
    const current = layoutRef.current;
    if (modeRef.current === 'today' && current?.mode === 'today' && current.mediaWidth == null) {
      holdSnap();
      applySnap(true);
    }
  }, [applySnap, holdSnap]);

  /* A zoom waiting for its posts or pictures (a moment) gives way to anything the reader does meanwhile (a finger, a
     wheel, a scroll the page makes itself, a pick, the tab going): it never lands late, somewhere they have moved on
     from. A page still held from a pinch springs back (instant: something is about to cover the page) */
  const cancelZoomWait = useCallback((instant = false) => {
    if (!zoomWaitRef.current) return;
    zoomWaitRef.current = null;
    const root = rootRef.current;
    const held = pinchAnimRef.current;
    if (!root || pinchRef.current || !held) return;
    if (instant) {
      dropPinch();
      root.style.transformOrigin = '';
      pinchBackdrop(root, false);
      resnap();
      return;
    }
    springHome(root, held.scale, resnap);
  }, [dropPinch, resnap, springHome]);

  /* An order still sliding when another move takes the page (a zoom, a pick, a pinch): the old list's copy goes on
     sliding out on its own, never popped off, and the new move carries the rest from where it stands */
  // copies handed off and still sliding out (gone at once when the tab is left: they lie over the whole screen)
  const handedRef = useRef(new Set<StillFrame>());
  const dropHanded = useCallback(() => {
    handedRef.current.forEach((cover) => cover.remove());
    handedRef.current.clear();
  }, []);
  const handSlideOff = useCallback(() => {
    const move = moveRef.current;
    const sliding = move?.slide;
    const flight = flightRef.current;
    if (!move || !sliding) return;
    move.slide = null;
    const cover = sliding.cover;
    const out = flight && !flight.ended ? flight.animations.find((animation) => (animation.effect as KeyframeEffect | null)?.target === cover.element) : undefined;
    // (the tab leaving: nothing to see it go)
    if (!flight || !out || out.playState === 'finished' || !visibleRef.current) {
      cover.remove();
      return;
    }
    flight.animations = flight.animations.filter((animation) => animation !== out);
    // (still held for WebKit's first frame: it sets off now, as the rest would have)
    if (flight.hold && typeof out.currentTime === 'number' && out.currentTime < flight.hold) out.currentTime = flight.hold;
    handedRef.current.add(cover);
    const drop = () => {
      handedRef.current.delete(cover);
      cover.remove();
    };
    out.finished.then(drop, drop);
  }, []);
  // (the desktop's tab switch copies the page: a copy handed off is gone first, as the running slide lands: cut)
  useEffect(() => {
    window.addEventListener('fm:tab-leave', dropHanded);
    return () => window.removeEventListener('fm:tab-leave', dropHanded);
  }, [dropHanded]);
  // the running order slide's own way to land (its list, its copy and the order's underline together)
  const slideCutRef = useRef<(() => void) | null>(null);
  // a scroll the page makes itself (Feed tapped again, a highlight) lands an order still sliding first
  const landSlide = useCallback(() => {
    if (moveRef.current?.slide) slideCutRef.current?.();
  }, []);

  /* keepLayer: a zoom's commit leaves the page its own layer (a pinch gave it one) for the move's first frame: let go in
     the heaviest frame of the move, every picture would be painted again into the page's own tiles at once (blank on
     WebKit till it is). The move lets it go once it is under way (dropLayer) */
  const releaseLayer = useCallback((keepLayer = false) => {
    const root = rootRef.current;
    if (!root) return;
    cancelSpring();
    dropPinch();
    root.style.transform = '';
    root.style.transformOrigin = '';
    if (!keepLayer) root.style.willChange = '';
    pinchBackdrop(root, false);
  }, [cancelSpring, dropPinch]);
  const dropLayer = useCallback(() => {
    const root = rootRef.current;
    if (root && !pinchRef.current && !pinchAnimRef.current) root.style.willChange = '';
  }, []);

  /* a move lands (or is cut short): every piece is itself, the old view's leftovers go, the page is live again. A held
     scroll is handed over first (holding the snap: a scroll the page makes), unless a finger is on the page
     (`keepHeld`: one that has just come down, before the window hears it): then the page goes on where it is drawn, by
     its offset, and the scroll is handed over once it is at rest (handOver, landCarried). The snap is otherwise left as
     it is: the move's own scroll held it already, and a finger that has taken the page since (a swipe as the move
     lands) keeps it, so the swipe still ends on a card instead of coasting to a stop between two */
  const endMove = useCallback((keepHeld = false) => {
    /* a phone's zoom into the Day view landing on a card: the page's colours are the cards' at once (underCard takes them
       on from the next commit), before the hand-over below scrolls the page under the still copy and WebKit shows the
       bare page for a few frames, under the status bar too, where the card's picture now reaches: dark, not a white
       flash between the card flying in and its head */
    if (moveRef.current?.to === 'today' && layoutRef.current?.deck && !atProfileRef.current) cardPage(true);
    const keep = (keepHeld || touchingRef.current) && handoverRef.current !== null;
    // whether this landing hands a held scroll over itself (then where it really landed is read below)
    const handing = handoverRef.current !== null && !keep;
    if (!keep) handOver();
    // (kept: the header reads the reader's scroll from here on, from where the page really is, keeping its fold; it
    // was told where the move lands, in the page's own terms, which the real scroll isn't till the hand-over)
    else markScrollJump('keep', 60, window.scrollY);
    releaseHeight();
    handSlideOff();
    endFlight(flightRef.current);
    flightRef.current = null;
    endFlight(openingRef.current);
    openingRef.current = null;
    window.clearTimeout(spotTimerRef.current);
    const landing = moveRef.current;
    if (!landing) return;
    landing.profileCopy?.remove();
    landing.slide?.cover.remove();
    moveRef.current = null;
    setMove(null);
    // what it was laid out from stays on screen (the page isn't laid out again as it lands)
    setLanded({ gen: genRef.current, posts: landing.posts, index: landing.index });
    setSpotOn(true);
    // where it really landed (a held scroll just handed over), read once, after this landing's own writes (hidden: where
    // it was kept). A zoom straight back reads it only from that hand-over: otherwise the move already noted where it
    // lands, and a scroll the reader made during the glide must not count as 'where the last zoom left it'
    const visible = visibleRef.current;
    const scrollY = visible ? pageY() : savedYRef.current;
    if (lastZoomRef.current && handing) lastZoomRef.current.scroll = scrollY;
    if (focalHoldRef.current && handing) focalHoldRef.current.scroll = scrollY;
    dropLayer();
    if (visible) {
      savedYRef.current = scrollY;
      syncFrame(scrollY);
      // the pictures around where it landed, fetched ahead
      lookAhead(scrollY, false);
      // landed: the other zooms are made ready from here (not while a held scroll is still to be handed over)
      if (!handoverRef.current) prewarm();
    }
  }, [dropLayer, handOver, handSlideOff, lookAhead, pageY, prewarm, releaseHeight, syncFrame]);

  /* a still copy of the screen over the page (stillFrame). A finger or a wheel lets it go at once (after `onTouch`),
     even mid-dissolve: a scroll under a copy still on screen moves the page under a picture that stands still, which
     reads as the page frozen */
  const coverPage = useCallback((root: HTMLElement, onTouch?: () => void) => {
    dropStill();
    /* (laid in the app's own layer, with the dock and its frost, under them as the page is: over the body, it covered the
       frost a range was picked through, the card under it bright and sharp for the frames the copy stood, Nov 27) */
    const still = stillFrame(root, { zIndex: STILL_Z, foot: STILL_FOOT, into: root.closest<HTMLElement>('.fm-app-shell-root') ?? undefined });
    let quick = false;
    const letGo = () => {
      onTouch?.();
      if (quick) return;
      quick = true;
      unlisten();
      if (stillRef.current === entry) stillRef.current = null;
      void still.fadeOut(STILL_TOUCH_OUT_MS, 0);
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
      // (still listening as it goes: a touch mid-dissolve takes it off quickly)
      fadeOut: (ms: number, dy: number, easing?: string, hold?: number) => {
        if (stillRef.current === entry) stillRef.current = null;
        void still.fadeOut(ms, dy, easing, hold).then(unlisten);
      },
    };
    stillRef.current = entry;
    return entry;
  }, [dropStill]);

  /* A move that held the scroll lands: a still copy of the screen goes over the page, the scroll is handed over under
     it, and the copy dissolves once WebKit has drawn the page where it now is. A finger or a wheel takes the page back
     at once; another move takes over by itself (zoomTo, commit) */
  const landHeld = useCallback((flight: Flight) => {
    /* (a card landing: the page's colours the cards' a frame or two ahead of the hand-over, so Safari's haze under the
       status bar is already dark as the page is handed over: endMove. For 3–4 frames of that hand-over WebKit still
       leaves the strip over the viewport unpainted, a pale band where the card's picture reaches; nothing in the page
       colours it, measured: not the layer's ground nor the stream's) */
    if (moveRef.current?.to === 'today' && layoutRef.current?.deck && !atProfileRef.current) cardPage(true);
    const root = rootRef.current;
    if (!root || !visibleRef.current) {
      endMove();
      return;
    }
    let landed = false;
    // (a finger coming down: the move lands, its scroll kept as drawn, for the page to be handed over at rest)
    const land = (byFinger: boolean) => {
      if (landed) return;
      landed = true;
      if (flightRef.current === flight) endMove(byFinger);
    };
    const entry = coverPage(root, () => land(true));
    void entry.shown.then(async () => {
      if (stillRef.current !== entry) return;
      land(false);
      const handed = performance.now();
      await whenDrawn(root, STILL_SETTLE_MS);
      await sleep(Math.max(0, STILL_HOLD_MS - (performance.now() - handed)));
      if (stillRef.current === entry) entry.fadeOut(STILL_OUT_MS, 0, STILL_OUT_EASE);
    });
  }, [coverPage, endMove]);

  /* A held scroll kept through a finger's scroll (endMove's keepHeld), handed over once the page is at rest: under a
     still copy, as a landing's (the jump WebKit would show blank), which dissolves once the page is drawn where it now
     is. The header keeps its fold (the reader has had the page for a while). A finger first: at once, under it */
  const landCarried = useCallback(() => {
    const pending = handoverRef.current;
    if (!pending || moveRef.current) return;
    pending.mode = 'keep';
    const root = rootRef.current;
    if (!root || !visibleRef.current) {
      handOver();
      return;
    }
    const entry = coverPage(root);
    void entry.shown.then(async () => {
      if (stillRef.current !== entry) return;
      // (a finger since: the page is its again, and the offset stays till the next rest)
      if (touchingRef.current) {
        entry.fadeOut(STILL_TOUCH_OUT_MS, 0);
        return;
      }
      handOver();
      const handed = performance.now();
      await whenDrawn(root, STILL_SETTLE_MS);
      await sleep(Math.max(0, STILL_HOLD_MS - (performance.now() - handed)));
      if (stillRef.current === entry) entry.fadeOut(STILL_OUT_MS, 0, STILL_OUT_EASE);
    });
  }, [coverPage, handOver]);
  useIsoLayoutEffect(() => {
    landCarriedRef.current = landCarried;
  });

  // where the layer will start for a scope (under the open header, its profile and the gap after it)
  const originFor = useCallback((size: Metrics, forScope: StreamScope) => {
    const root = rootRef.current;
    const column = contentBox(size.width);
    return (root ? pageTop(root) : 0) + size.insets.top + profilePad(size.width) + profileHeaderHeight(column.contentWidth, forScope) + PROFILE_GAP;
  }, []);

  /* A zoom. What is on screen is measured where it is (a live pinch included), the new view laid out and drawn in one
     commit at the scroll it opens at, and the move (below) carries every piece from where it was */
  /* ── a phone's Day: the layer over the grid ── */

  const openDay = useCallback((key: string | null, day: string | null, instant: boolean) => {
    if (dayOpenRef.current) return;
    daySeqRef.current += 1;
    const start: DayStart = { key, day, seq: daySeqRef.current, instant, at: performance.now() };
    dayOpenRef.current = start;
    setDayOpen(start);
  }, []);
  // the layer gone (closed, or a width with no layer): the page as it was
  const closedDay = useCallback(() => {
    dayOpenRef.current = null;
    setDayOpen(null);
  }, []);
  const dropDay = useCallback(() => {
    if (!dayOpenRef.current) return;
    const root = rootRef.current;
    if (root) {
      root.style.opacity = '';
      root.style.pointerEvents = '';
      root.style.transform = '';
      root.style.transformOrigin = '';
    }
    cardPage(false);
    closedDay();
  }, [closedDay]);
  // the post in the grid nearest the reading line (Day opens on it when nothing was tapped)
  const postInView = useCallback((): { key: string | null; day: string | null } => {
    const current = layoutRef.current;
    const size = metricsRef.current;
    if (!current || !size) return { key: null, day: null };
    const at = nearestPost(current, size.width / 2, pageY() - originRef.current + size.height * 0.4, size.height);
    const item = at >= 0 ? current.items[at] : undefined;
    return { key: item?.postKey ?? null, day: item?.day ?? null };
  }, [pageY]);
  const dayPage = useCallback(() => rootRef.current, []);
  useIsoLayoutEffect(() => {
    setDayHost(rootRef.current?.parentElement ?? null);
  }, []);
  const dayTile = useCallback((key: string) => layerRef.current?.querySelector<HTMLElement>(`:scope > [data-cell="${CSS.escape(`p:${key}`)}"]`) ?? null, []);
  // the page's scroll that brings a post's tile on screen (null: it is on screen already, or the grid hasn't it)
  const placeDayTile = useCallback((key: string): number | null => {
    const current = layoutRef.current;
    const at = current?.byPost.get(key);
    if (!current || at == null) return null;
    const item = current.items[at];
    const vh = window.innerHeight;
    const top = originRef.current + item.y - window.scrollY;
    if (top + item.h >= DAY_TILE_TOP && top <= vh - DAY_TILE_FOOT) return null;
    const room = Math.max(0, (document.scrollingElement ?? document.documentElement).scrollHeight - vh);
    return clamp(Math.round(originRef.current + item.y + item.h / 2 - vh * 0.45), 0, room);
  }, []);

  const zoomTo = useCallback((next: FeedMode, options: ZoomOptions = {}) => {
    const current = modeRef.current;
    const pinchScale = pinchRef.current?.scale ?? 1;
    pinchRef.current = null;
    const root = rootRef.current;
    // (the card's copy this zoom put up, taken down by any way out of it but the move)
    const uncover = () => {
      if (options.covered && stillRef.current === options.covered) dropStill();
      else options.covered?.remove();
    };
    // a phone: Day is the layer over the grid (DayLayer), the page's own zoom always a grid's
    if (dayLayerOn(metricsRef.current)) {
      const size = metricsRef.current;
      if (next === 'today') {
        uncover();
        if (dayOpenRef.current) return;
        if (size && options.remember !== false) writeStoredMode(breakpointOf(viewOf(size)), 'today');
        const focalKey = options.focal?.postKey ?? null;
        dayNote(`zoomTo today focal ${focalKey ? focalKey.slice(0, 9) : 'none'} ${new Error().stack?.split('\n').slice(2, 4).map((line) => line.trim().slice(0, 40)).join(' < ') ?? ''}`);
        const at = focalKey ? layoutRef.current?.byPost.get(focalKey) : undefined;
        const place = focalKey ? { key: focalKey, day: at != null ? layoutRef.current?.items[at]?.day ?? null : null } : postInView();
        openDay(place.key, place.day, false);
        return;
      }
      if (dayOpenRef.current) {
        uncover();
        if (size && options.remember !== false) writeStoredMode(breakpointOf(viewOf(size)), next);
        dayNote(`zoomTo ${next} closes ${new Error().stack?.split('\n').slice(2, 4).map((line) => line.trim().slice(0, 40)).join(' < ') ?? ''}`);
        // (out onto the grid it lies over; another grid than that one, a zoom between grids after)
        dayLayerRef.current?.close(() => {
          if (next !== modeRef.current) zoomToRef.current?.(next, { remember: false });
        });
        return;
      }
    }
    if (!current || next === current) {
      uncover();
      // (a page held from a pinch springs home from where it is held, the Day view's snap back on)
      if (root && pinchAnimRef.current) {
        zoomWaitRef.current = null;
        springHome(root, pinchScale !== 1 ? pinchScale : pinchAnimRef.current.scale, resnap);
      }
      return;
    }
    // the network is the zoom's now. A tab still opening keeps its cells unseen while the zoom waits for its days or
    // pictures (below), and lets them go as the move starts: the move brings in what it shows
    stopPrewarm();
    const size = metricsRef.current;
    if (size && options.remember !== false) writeStoredMode(breakpointOf(viewOf(size)), next);
    const was = layoutRef.current;
    const layer = layerRef.current;
    const index2 = inputsRef.current.index;
    if (!size || !was || !layer || !index2 || !visibleRef.current) {
      uncover();
      dropHold(true);
      releaseLayer();
      lastZoomRef.current = null;
      modeRef.current = next;
      setMode(next);
      return;
    }
    // the page where it really is (a move's held scroll handed over, its still copy let go; not the card's copy this zoom
    // put up to go out under)
    handOver();
    if (!options.covered) dropStill();
    const scrollY = window.scrollY;
    const vh = window.innerHeight;
    const base = originRef.current;
    const viewed = zoomView(size, scrollY, base, compressedRef.current, options.point);
    const point = options.point ?? { x: size.left + viewed.point.x, y: viewed.point.y };

    /* What the zoom is about (landing.ts): what was tapped or pinched; else, straight back (nothing picked, the page as
       the last zoom left it), that zoom's post again, where it stood then (picked afresh from the middle of the screen,
       a round trip would come back on a neighbour, or lose your week to a month's highlights); else where you are: the
       start of the posts while it is on screen, the post in view, or the month */
    let picked = options.focal?.zid ?? null;
    if (!picked && options.focal?.postKey) {
      const at = was.byPost.get(options.focal.postKey);
      picked = at != null ? idOf(was.items[at]) : null;
    }
    const last = lastZoomRef.current;
    const back = !options.focal && !options.point ? straightBack(was, next, last, scrollY, scrollY - base) : null;
    const place: ZoomPlace = back
      ? { start: false, zid: back.zid, day: back.day }
      : options.place ?? placeOf(was, next, viewed, last, scrollY, picked, Boolean(options.point));
    /* (straight back out of the Day view: the page goes back where it was, and the card in view is still what flies on
       top, even when the zoom in remembered a day and no post: without it the grid's own order stacked the card it
       covered over it) */
    const cardInView = back && !back.zid && was.mode === 'today' && !dayAtProfile(was, scrollY - base) ? was.items[dayIndexAt(was, scrollY - base)] : null;
    const zid = place.zid ?? (cardInView ? idOf(cardInView) : null);

    // the new view, laid out now, and where the page goes in it
    const scope2 = inputsRef.current.scope;
    const targetPosts = loadedPosts(scope2, index2);
    const target = computeLayout({ mode: next, days: index2.days, posts: targetPosts, width: size.width, height: size.height, insets: size.insets, wide: wideOf(size) });
    const focalAt = zid ? indexOfSlot(target, zid) : -1;
    const focalFrame = zid ? layer.querySelector<HTMLElement>(`:scope > [data-zid="${zid}"] > [data-layer="frame"]`) : null;
    const anchor = focalFrame ? centreOf(frameRect(focalFrame)) : point;
    // (a post not on this screen, the one a month was come out from: where it stood before that)
    const anchorY = !focalFrame && zid && last?.zid === zid ? last.screenY : anchor.y;
    const top = back ? returnTop(target, back) : landingTop(target, place, viewed, anchorY);
    const scroll = clamp(base + top, 0, Math.max(0, base + target.totalHeight - vh));

    /* The posts it opens on must be here, or their cells would arrive as placeholders (a moment at most). A pinch never
       waits for days (the page is in the fingers), only, briefly, for the pictures its warm-up has been decoding since
       the fingers showed which way it was going (PINCH_PICTURE_WAIT_MS), the page held where the fingers left it */
    const pinched = pinchScale !== 1;
    zoomWaitRef.current = null;
    // into the Day view: the washes of the card it rests on and the next one made ahead (nothing waits for them)
    const cardAt = next !== 'today' ? -1 : focalAt >= 0 ? focalAt : dayIndexAt(target, scroll - base);
    if (cardAt >= 0) {
      [cardAt, cardAt + 1].forEach((at) => {
        const item = target.items[at];
        if (item?.day && item.slot >= 0) warmWash(targetPosts.get(item.day)?.[item.slot]?.thumbnailUrl);
      });
    }
    if (!options.waited && !reducedMotion()) {
      const [first, last] = itemRange(target, scroll - base, scroll - base + vh);
      const missing = new Set<string>();
      for (let at = first; at < last; at += 1) {
        const item = target.items[at];
        if ((item.kind === 'tile' || item.kind === 'reel') && !item.postKey && item.day) missing.add(item.day);
      }
      // (about what it was about; a zoom straight back finds its memory again: the screen holds still meanwhile)
      const again = () => zoomToRef.current?.(next, { ...options, place: back ? undefined : place, waited: true });
      if (missing.size && !pinched) {
        const token = {};
        zoomWaitRef.current = token;
        prefetchDays(scope2, [...missing]);
        void viewReady(scope2, next, size, place, ZOOM_LOAD_MS).then(() => {
          if (zoomWaitRef.current !== token) return;
          zoomWaitRef.current = null;
          again();
        });
        return;
      }
      // the pictures of the posts the new view brings on screen (those not on it already, decoded). Not into the Day
      // view on a post that is on screen: its neighbours only peek
      const cold: string[] = [];
      for (let at = next === 'today' && focalFrame ? last : first; at < last; at += 1) {
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
        void warmPictures(cold, pinched ? PINCH_PICTURE_WAIT_MS : PICTURE_WAIT_MS).then(() => {
          if (zoomWaitRef.current !== token) return;
          zoomWaitRef.current = null;
          again();
        });
        return;
      }
    }

    /* Out of a phone's Day card (a card on screen, the page going far): the card's still copy goes up first, and the zoom
       goes on once it is on screen (the move then puts the page where it lands under it: COVER_HOLD_MS). A finger first,
       or another zoom: the copy goes, and nothing moves. A pinch keeps its own way (the page is in the fingers). (Tried
       into a card too, Nov 27: the page under the copy took ~0.5s to settle after the Day view mounted, a tap standing
       still that long, and the copy of a light grid turned Safari's bars white for a beat. Into a card keeps the held
       way, handed over as it lands) */
    if (!options.covered && !pinched && root && current === 'today' && next !== 'today' && size.width < THEATRE_FROM && holdsScroll() && !reducedMotion() && Math.abs(scroll - scrollY) >= HOLD_FROM_PX && !dayAtProfile(was, scrollY - base)) {
      const token = {};
      zoomWaitRef.current = token;
      const cover = coverPage(root, () => {
        if (zoomWaitRef.current === token) zoomWaitRef.current = null;
      });
      void cover.shown.then(() => {
        if (zoomWaitRef.current !== token) return;
        zoomWaitRef.current = null;
        zoomToRef.current?.(next, { ...options, place: back ? undefined : place, waited: true, covered: cover });
      });
      return;
    }

    // the screen as it stands, then the new view in one commit
    dropHold(true);
    const shots = snapshot(layer);
    const profileShot = profileRef.current ? rectOf(profileRef.current) : null;
    handSlideOff();
    endFlight(flightRef.current);
    flightRef.current = null;
    endFlight(openingRef.current);
    openingRef.current = null;
    window.clearTimeout(spotTimerRef.current);
    moveRef.current?.profileCopy?.remove();
    moveRef.current?.slide?.cover.remove();
    // (the page keeps its own layer through the commit: the move lets it go once under way)
    releaseLayer(true);
    // this zoom's post, for one straight back (its scroll set once the page is where it lands)
    lastZoomRef.current = { zid, day: place.day, screenY: anchorY, from: current, fromTop: scrollY - base, fromY: middleOf(was, zid), to: next, scroll: null };
    // (a month opens at its heading, and the start of the posts is the page's: there the heading holds, or the top)
    focalHoldRef.current = zid && focalAt >= 0 && next !== 'month' && !place.start ? { id: zid, postKey: target.items[focalAt].postKey ?? null, until: performance.now() + FOCAL_HOLD_MS, scroll: null } : null;
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
      slide: null,
      posts: targetPosts,
      index: index2,
      cover: options.covered ?? null,
    };
    moveRef.current = nextMove;
    // the move brings in what it shows: the tab's opening is over (held or not yet begun, it would hide what landed)
    arrivedRef.current = true;
    const dayNow = cardAt >= 0 ? cardAt : activeRef.current;
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
      // (the tab's opening, if still coming in, is the move's now: what it held is let go with it)
      setEntrance(null);
      setMove(nextMove);
    });
    if (reduce) {
      uncover();
      dropLayer();
      window.scrollTo(0, scroll);
      savedYRef.current = window.scrollY;
      if (lastZoomRef.current) lastZoomRef.current.scroll = window.scrollY;
    }
  }, [coverPage, dropHold, dropLayer, dropStill, handOver, handSlideOff, keepHeight, openDay, postInView, releaseLayer, resnap, springHome, stopPrewarm]);
  useIsoLayoutEffect(() => {
    zoomToRef.current = zoomTo;
  });

  /* A move, drawn: the page goes where it opens, and before the first paint every piece is carried from where it was,
     or leaves, or arrives. The spotlight comes on a moment in; the rest of the page goes live once it has landed */
  useIsoLayoutEffect(() => {
    if (!move || landedRef.current === move.id) return;
    landedRef.current = move.id;
    // a placeholder's copy still dissolving would stay where its post now is, not go with it
    coversRef.current.forEach((cover) => cover.remove());
    coversRef.current.clear();
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
    if (move.cover) {
      // (under the card's copy: the page where it lands now, the bare strips the cards' dark; the dock's frost with it)
      holdSnap();
      shadeBand(root);
      window.dispatchEvent(new CustomEvent(PAGE_JUMP_EVENT, { detail: { by } }));
      window.scrollTo(0, goal);
      savedYRef.current = window.scrollY;
      settledTopRef.current = window.scrollY;
    } else if (root && holdsScroll() && Math.abs(by) >= HOLD_FROM_PX) {
      // WebKit: drawn where it lands by an offset, nothing scrolled; the scroll handed over once it has landed, under a
      // still copy (landHeld). The old profile's copy, inside the page, stays put
      const copyAt = move.profileCopy ? rectOf(move.profileCopy) : null;
      root.style.top = `${(-by).toFixed(2)}px`;
      if (move.profileCopy && copyAt) pinToScreen(move.profileCopy, copyAt);
      handoverRef.current = { by, mode: jumpMode };
      savedYRef.current = window.scrollY + by;
      settledTopRef.current = window.scrollY + by;
    } else {
      holdSnap();
      // the old profile's copy stays where it stood on screen
      const copyAt = move.profileCopy ? rectOf(move.profileCopy) : null;
      window.scrollTo(0, goal);
      if (move.profileCopy && copyAt) pinToScreen(move.profileCopy, copyAt);
      savedYRef.current = window.scrollY;
      settledTopRef.current = window.scrollY;
    }
    /* The header (and the phone's bottom bar) keep their fold through the frame that draws the new view, the move's own
       scroll not read as the reader's ('keep'), and are told where the page lands as the pieces set off (answerHeader):
       a fold begun in this heavy frame would arrive already folded on WebKit (the rail gone in one frame, the glass a
       frame or two later) */
    markScrollJump('keep', JUMP_MARK_MS);
    const landingY = savedYRef.current;
    // (a held scroll is the move's until it is handed over, as it lands: the header goes on reading where it lands
    // till then)
    const answerHeader = () => markScrollJump(jumpMode, handoverRef.current ? 4000 : JUMP_ANSWER_MS, landingY);
    // a zoom: the scroll its page lands at (a zoom straight back taken mid-glide keeps its focal too)
    if (move.kind === 'zoom' && lastZoomRef.current) lastZoomRef.current.scroll = savedYRef.current;
    if (move.kind === 'zoom' && focalHoldRef.current) focalHoldRef.current.scroll = savedYRef.current;

    const vw = window.innerWidth;
    const vh = window.innerHeight;
    const flight = startFlight(move.cover ? COVER_HOLD_MS : holdsScroll() ? FLIGHT_HOLD_MS : 0);
    flightRef.current = flight;
    /* A finger (or a wheel) on the page while a zoom or a pick is still moving takes the page at once: the move lands
       where it was going, and the page is live under the finger (a page still moving mounts nothing new as it scrolls:
       a swipe began just as a zoom landed scrolled into black). Two fingers are a pinch, which takes the page itself
       (onPinchStart); a pinch on a trackpad (ctrl + wheel) too. A new order's slide has its own way (below) */
    if (!move.slide) {
      const grab = (event: Event) => {
        if ('touches' in event && (event as TouchEvent).touches.length > 1) return;
        if ('ctrlKey' in event && (event as WheelEvent).ctrlKey && event.type === 'wheel') return;
        // (its held scroll kept: handed over under the finger, the scroll would be lost to the pan iOS begins)
        if (flightRef.current === flight && !flight.ended) endMove(true);
      };
      const listen = { capture: true, passive: true } as const;
      window.addEventListener('touchstart', grab, listen);
      window.addEventListener('wheel', grab, listen);
      flight.cleanups.push(() => {
        window.removeEventListener('touchstart', grab, listen);
        window.removeEventListener('wheel', grab, listen);
      });
    }
    /* A new order: the copy of the list it was shows over the page (it was drawn unseen), and goes out one side as the
       new list comes in from the other, edge to edge, under the profile, which stays. The old view's leftovers are the
       copy's: unseen in the page. Nothing else moves */
    // the order's underline (and the card's pill), CSS transitions set off by this commit: held with the slide
    let glides: Animation[] = [];
    if (move.slide) {
      const { dir, cover, from } = move.slide;
      cover.show();
      // (unless a move that took the page over handed it off to slide out on its own: handSlideOff)
      flight.cleanups.push(() => {
        if (move.slide) move.slide.cover.remove();
      });
      const span = metricsRef.current ? metricsRef.current.width + leftRef.current : vw;
      const target = layer;
      const was = target.style.willChange;
      target.style.willChange = 'transform';
      // the list off to one side never makes the page wider (the root page scroll would pan sideways under a finger)
      const body = document.body;
      const wasX = body.style.overflowX;
      body.style.overflowX = 'clip';
      flight.cleanups.push(() => {
        target.style.willChange = was;
        body.style.overflowX = wasX;
      });
      glides = (profileRef.current ? Array.from(profileRef.current.querySelectorAll<HTMLElement>('[data-order-glide]')) : [])
        .flatMap((el) => el.getAnimations())
        .filter((animation) => animation instanceof CSSTransition);
      if (flight.hold) {
        glides.forEach((animation) => {
          animation.pause();
          animation.currentTime = 0;
        });
        flight.cleanups.push(() => glides.forEach((animation) => {
          if (animation.playState === 'paused') animation.play();
        }));
      }
      /* a finger that moves, a wheel, or the tab being left (the desktop's tab switch copies the page) takes the page
         at once: the slide lands where it is going. A finger that only lands doesn't: a tap on the other order lets
         that order take the slide over from where it stands (commit: midSlide) */
      const cut = () => {
        if (flightRef.current !== flight || flight.ended) return;
        [...flight.animations, ...glides].forEach((animation) => {
          try {
            animation.finish();
          } catch {
            // one with nowhere to finish (cancelled since)
          }
        });
      };
      slideCutRef.current = cut;
      // (a finger moves a few px in any tap: only one that travels past this, or a second finger, is a scroll)
      let downX = 0;
      let downY = 0;
      const down = (event: TouchEvent) => {
        // (a second finger is a pinch coming: the slide lands before the pinch takes the page)
        if (event.touches.length > 1) {
          cut();
          return;
        }
        const touch = event.touches[0];
        if (touch) {
          downX = touch.clientX;
          downY = touch.clientY;
        }
      };
      const moved = (event: TouchEvent) => {
        const touch = event.touches[0];
        if (event.touches.length > 1 || !touch || Math.hypot(touch.clientX - downX, touch.clientY - downY) > TAP_SLOP_PX) cut();
      };
      // the page scrolled any other way (the keys, the scrollbar): the copy is fixed, so the slide lands first (a held
      // WebKit scroll is the move's own)
      const startY = window.scrollY;
      const scrolled = () => {
        if (!handoverRef.current && Math.abs(window.scrollY - startY) > 2) cut();
      };
      const listen = { capture: true, passive: true } as const;
      window.addEventListener('touchstart', down, listen);
      window.addEventListener('touchmove', moved, listen);
      window.addEventListener('wheel', cut, listen);
      window.addEventListener('scroll', scrolled, { passive: true });
      window.addEventListener('fm:tab-leave', cut);
      flight.cleanups.push(() => {
        window.removeEventListener('touchstart', down, listen);
        window.removeEventListener('touchmove', moved, listen);
        window.removeEventListener('wheel', cut, listen);
        window.removeEventListener('scroll', scrolled);
        window.removeEventListener('fm:tab-leave', cut);
        if (slideCutRef.current === cut) slideCutRef.current = null;
      });
      // from where the screen stands (a slide cut short mid-way goes on from there, the copy and the list still edge to
      // edge), in the time the distance left takes
      const start = from + dir * span;
      const timing: KeyframeAnimationOptions = { duration: SLIDE_MS * Math.max(0.35, Math.min(1, Math.abs(start) / Math.max(span, 1))), easing: CARRY_EASE_CSS };
      layer.querySelectorAll<HTMLElement>(':scope > [data-cell][data-gone]').forEach((el) => {
        run(flight, el, [{ opacity: 0 }, { opacity: 0 }], { ...timing, fill: 'both' });
      });
      run(flight, target, [{ transform: `translate3d(${start.toFixed(1)}px, 0px, 0px)` }, { transform: 'translate3d(0px, 0px, 0px)' }], { ...timing, fill: 'backwards' });
      run(flight, cover.element, [{ transform: `translate3d(${from.toFixed(1)}px, 0px, 0px)` }, { transform: `translate3d(${(-dir * span).toFixed(1)}px, 0px, 0px)` }], { ...timing, fill: 'both' });
    }

    // what leaves goes away from where the move started; what arrives comes in a ring at a time from where it lands
    const focalFrame = move.focalKey ? layer.querySelector<HTMLElement>(`:scope > [data-cell="p:${move.focalKey}"] > [data-layer="frame"]`) : null;
    const focalRect = focalFrame ? rectOf(focalFrame) : null;
    const landing = focalRect ? centreOf(focalRect) : move.centre;

    // every piece measured where it now is before any is set moving (a measure after an animation is added has WebKit
    // work its styles out again, once for every piece)
    const steps: Array<() => void> = [];
    const phone = (metricsRef.current?.width ?? vw) < THEATRE_FROM;
    /* (a zoom's headings and "+N" tiles are words: the old ones go at once and the new come in a beat after, so two never
       stand over each other. Held whole while the new came in, "23–29 Nov" and "November", their two BEST pills and post
       counts, read over each other for a sixth of a second) */
    const words = (el: HTMLElement) => move.kind === 'zoom' && (el.dataset.kind === 'section' || el.dataset.kind === 'more');
    // a grid and the Day view: each post the move carries changes its chrome on the way (bringChrome)
    const crossingMove = (move.from === 'today') !== (move.to === 'today');
    /* ...and on a phone the grid moves as one surface with the card, as a camera would: going into the card it is
       carried on with the zoom that takes the post's tile onto the card, out past the screen's edges, dissolving;
       coming out of one it comes in from where that zoom put it as the card shrinks into its place. One map for every
       piece and one clock (the card's carry): before, each tile came or went on its own, a ring at a time, and the grid
       never read as one thing */
    const focalWas = move.focalKey ? move.shots.get(`p:${move.focalKey}`)?.frame ?? null : null;
    const toCard = move.to === 'today';
    // (the card's own 4:5, not its top grown up under the clock: that is what its tile shows)
    const focalWindow = focalFrame ? frameRect(focalFrame) : null;
    const camera = phone && crossingMove && focalWas && focalWindow ? zoomMap(toCard ? focalWas : focalWindow, toCard ? focalWindow : focalWas) : null;
    const cameraTrip = focalWas && focalRect ? tripOf(focalWas, focalRect, vw, vh) || CARRY_MS : CARRY_MS;
    if (!move.slide) layer.querySelectorAll<HTMLElement>(':scope > [data-cell]').forEach((el) => {
      const shot = move.shots.get(el.dataset.cell ?? '');
      // the old view's leftovers: held where they were, and let go
      if (el.dataset.gone !== undefined) {
        const was = shot?.cell;
        if (was && onScreen(was, vw, vh)) {
          const now = rectOf(el);
          if (camera && toCard) {
            steps.push(() => carry(flight, el, now, was, camera(was), { duration: cameraTrip, fade: 'out' }));
            return;
          }
          const quick = words(el);
          steps.push(() => leave(flight, el, now, was, move.centre, { quick }));
          steps.push(() => easeDepthOut(flight, el, shot, true));
        }
        return;
      }
      const frameEl = el.querySelector<HTMLElement>(':scope > [data-layer="frame"]');
      if (el.dataset.kind === 'post' && frameEl && shot?.frame) {
        const frameWas = shot.frame;
        const frameNow = rectOf(frameEl);
        /* (a post both views hold but the card the move is about: a card off screen in the Day view. Out of it, its tile
           comes in with the grid; into it, its tile goes with the grid, the card it becomes unseen) */
        if (camera && el.dataset.focal === undefined) {
          if (!toCard) {
            const now = rectOf(el);
            if (onScreen(now, vw, vh)) steps.push(() => carry(flight, el, now, camera(now), now, { duration: cameraTrip, fade: 'in' }));
          } else if (onScreen(frameWas, vw, vh)) {
            steps.push(() => carry(flight, frameEl, frameNow, frameWas, camera(frameWas), { duration: cameraTrip, fade: 'out', picture: frameEl.querySelector<HTMLElement>('[data-picture]') }));
          }
          return;
        }
        const picture = frameEl.querySelector<HTMLElement>('[data-picture]');
        /* A card coming in with its own top grown up under the clock (a reel's frame above its 4:5: PostCell): carried
           from its tile as if that top stood over the tile (the tile's picture is the card's 4:5, so nothing in it shifts),
           the top cut off at first and opened as it grows, on the carry's curve. Going back, the same the other way: its
           tile wears that top (PostCell, cardTop), carried from the whole card, cut off as it shrinks, and the shade
           over it under the clock goes with it */
        const grownPx = el.dataset.focal !== undefined ? Number(frameEl.dataset.grown) || 0 : 0;
        const belowPx = el.dataset.focal !== undefined ? Number(frameEl.dataset.below) || 0 : 0;
        const toWas = frameWas.width / Math.max(frameNow.width, 0.01);
        const tall = grownPx > 0 ? (toCard ? grownPx * toWas : shot.grown ?? 0) : 0;
        const deep = belowPx > 0 ? (toCard ? belowPx * toWas : shot.below ?? 0) : 0;
        const fromRect = tall || deep ? { ...frameWas, top: frameWas.top - tall, height: frameWas.height + tall + deep } : frameWas;
        steps.push(() => carryLayer(flight, frameEl, fromRect, vw, vh, picture, frameNow));
        const trip = tripOf(frameWas, frameNow, vw, vh);
        if (grownPx > 0 || belowPx > 0) {
          const round = getComputedStyle(frameEl).borderRadius || '0px';
          const open = `inset(0px 0px 0px 0px round ${round})`;
          const shut = `inset(${grownPx}px 0px ${belowPx}px 0px round ${round})`;
          const timing: KeyframeAnimationOptions = { duration: trip || CARRY_MS, easing: CARRY_EASE_CSS, fill: 'backwards' };
          steps.push(() => run(flight, frameEl, [{ clipPath: toCard ? shut : open }, { clipPath: toCard ? open : shut }], timing));
          const shade = toCard ? null : frameEl.querySelector<HTMLElement>(':scope > [data-back-shade]');
          if (shade) steps.push(() => run(flight, shade, [{ opacity: 1 }, { opacity: 0, offset: 0.5 }, { opacity: 0 }], { ...timing, easing: 'linear' }));
        }
        if (crossingMove && trip) steps.push(() => bringChrome(flight, el, trip));
        const bg = el.querySelector<HTMLElement>(':scope > [data-layer="bg"]');
        if (bg) {
          const bgWas = shot.bg ?? frameWas;
          const bgNow = rectOf(bg);
          steps.push(() => carryLayer(flight, bg, bgWas, vw, vh, null, bgNow));
          // a phone card's ground is most of a screen of its colour: only the post the move is about brings it, growing
          // out from under its picture; going to a grid, it is let go before the picture is far on its way
          if (phone) {
            const own = el.dataset.focal !== undefined || (move.to === 'today' && move.kind === 'pick' && rectOf(el).top > -TILE_CLEAR_PX);
            if (move.to !== 'today') steps.push(() => run(flight, bg, [{ opacity: own ? 1 : 0 }, { opacity: 0 }], { duration: GROUND_OUT_MS, easing: 'linear', fill: 'both' }));
            else if (!own) steps.push(() => run(flight, bg, [{ opacity: 0 }, { opacity: 0 }], { duration: CARRY_MS, fill: 'both' }));
          }
        }
        ([['chips-out', shot.chips], ['chrome-out', shot.chrome], ['reading-out', shot.reading]] as const).forEach(([name, was]) => {
          const out = el.querySelector<HTMLElement>(`:scope > [data-layer="${name}"]`);
          if (!out || !was) return;
          const now = rectOf(out);
          // (a tile's own chrome, its post growing into a card: it goes with the picture it lies on, not left behind)
          if (name === 'chips-out' && crossingMove && trip) steps.push(() => rideOut(flight, out, now, was, frameNow, trip));
          else steps.push(() => letGo(flight, out, was, now));
        });
        // (the number the move is about, flying from where it stood to where it lands)
        if (camera && el.dataset.focal !== undefined && trip) {
          const outName = toCard ? 'chips-out' : 'chrome-out';
          const outEl = el.querySelector<HTMLElement>(`:scope > [data-layer="${outName}"]`);
          const outWas = toCard ? shot.chips : shot.chrome;
          if (outEl && outWas) {
            const outNow = rectOf(outEl);
            const from = rankIn(el, outName, { x: outWas.left - outNow.left, y: outWas.top - outNow.top });
            const to = rankIn(el, toCard ? 'chrome' : 'frame"] > [data-layer="chips');
            const fly = from && to ? rankFlight(layer, from, to) : null;
            if (fly) steps.push(() => fly(flight, trip));
          }
        }
        steps.push(() => easeDepthOut(flight, el, shot, false));
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
      // (out of a card: what the grid brings comes in with it, from where the zoom put it)
      if (camera && !toCard) {
        if (onScreen(now, vw, vh)) steps.push(() => carry(flight, el, now, camera(now), now, { duration: cameraTrip, fade: 'in' }));
        return;
      }
      // (a phone card over the one in view is under the status bar only, and unseen from the landing on: never seen)
      if (phone && move.to === 'today' && now.top < -TILE_CLEAR_PX) {
        steps.push(() => run(flight, el, [{ opacity: 0 }, { opacity: 0 }], { duration: CARRY_MS, fill: 'both' }));
        return;
      }
      if (onScreen(now, vw, vh)) steps.push(() => enter(flight, el, now, landing, words(el) ? WORDS_AFTER_MS : 0));
    });
    const profileNode = profileRef.current;
    const profileNow = profileNode ? rectOf(profileNode) : null;
    steps.forEach((step) => step());
    // (a phone's card and a grid: the colour under the clock follows the card the move carries, as it goes)
    if (phone && crossingMove && root) {
      // where it rests as a card: where it lands (to a card), or where it set off from (from one)
      const rest = toCard ? focalRect : focalWas;
      // (a phone's Day view is its cards: the layout read by handlers is still the one being left here)
      if (focalFrame && rest) watchCardEdge(flight, root, focalFrame, rest, toCard, vw);
      else cardPage(false);
      /* Out of a card the ground under the grid, the cards' dark through the move, goes to the page's own from part of
         the way on (a glide: stream.css), the posts coming in over it by then: on the move's clock, so it is the page's
         as the move lands. Let go only as it landed, the gaps between the posts lit up after it, the last thing to move */
      if (!toCard && camera) {
        const cue = root.animate([{ visibility: 'visible' }, { visibility: 'visible' }], { duration: cameraTrip * GROUND_LIGHT_AT, delay: flight.hold });
        flight.animations.push(cue);
        // (its glide timed to end as the move lands: on the stream's own 360ms it finished a beat after)
        void cue.finished.then(() => {
          if (flight.ended) return;
          root.style.transitionDuration = `${Math.round(cameraTrip * (1 - GROUND_LIGHT_AT))}ms`;
          root.removeAttribute('data-card-edge');
        }, () => undefined);
      }
    }

    // the page's profile: the same element, moved with the page (a pick for someone else: the old one's copy goes as
    // the new one comes in)
    if (profileNode && profileNow) {
      if (move.profileCopy) {
        const copy = move.profileCopy;
        // (the new one is there at once, under it: the copy is what moves)
        run(flight, copy, [{ opacity: 1 }, { opacity: 0 }], { duration: ms(180), easing: 'cubic-bezier(0.4, 0, 1, 1)', fill: 'both' });
        flight.cleanups.push(() => copy.remove());
      } else if (move.profile) {
        /* moved with the page; from (or to) far off screen only part of the way, fading. A phone's zoom between a card
           and a grid takes it the camera's way, as it takes the grid's posts, when one end has it out of sight (it slid
           on its own way meanwhile, against the grid zooming beside it) */
        const before = onScreen(move.profile, vw, vh);
        const after = onScreen(profileNow, vw, vh);
        if (camera && toCard && before && !after) carry(flight, profileNode, profileNow, move.profile, camera(move.profile), { duration: cameraTrip, fade: 'out' });
        else if (camera && !toCard && after && !before) carry(flight, profileNode, profileNow, camera(profileNow), profileNow, { duration: cameraTrip, fade: 'in' });
        else carryLayer(flight, profileNode, move.profile, vw, vh, null, profileNow);
      }
    }

    // under way: the spotlight comes on a moment in
    const under = () => {
      window.clearTimeout(spotTimerRef.current);
      spotTimerRef.current = window.setTimeout(() => setSpotOn(true), SPOT_AFTER_MS);
    };
    /* (the page setting off, said to whatever is waiting on it: the dock lifts the frost a range was picked over as the
       new view comes in, so Safari's bars, which take their look from what is under them, go from the frost to the new
       view once, never back to the old one in between) */
    const setOff = () => window.dispatchEvent(new Event(PAGE_MOVES_EVENT));
    if (flight.hold) {
      // held at its start for the frame that draws it, then set moving once that frame is on screen (WebKit only
      // starts the next frame once the last is shown)
      const go = () => {
        if (flightRef.current !== flight || flight.ended) return;
        answerHeader();
        releaseFlight(flight);
        glides.forEach((animation) => {
          if (animation.playState === 'paused') animation.play();
        });
        under();
        setOff();
        // the page's own layer (a pinch's) let go now its first frame is on screen
        dropLayer();
        // (out of a card: its copy goes in the frame the move sets off in, the same picture under it)
        if (move.cover && stillRef.current === move.cover) dropStill();
      };
      // (under the card's copy: once the page is drawn where it now is)
      if (move.cover && root) void whenDrawn(root, STILL_SETTLE_MS).then(() => requestAnimationFrame(go));
      else requestAnimationFrame(() => requestAnimationFrame(go));
    } else {
      under();
      setOff();
      afterDrawn(() => {
        if (flightRef.current === flight || flightRef.current === null) answerHeader();
        dropLayer();
      });
    }
    // landed (with a backstop, should an animation never report): every piece home, its chrome with it
    const backstop = window.setTimeout(() => {
      if (flightRef.current === flight) endMove();
    }, LONG_CARRY_MS + 600 + flight.hold);
    void flightDone(flight).then(() => {
      window.clearTimeout(backstop);
      if (flightRef.current !== flight) return;
      if (!handoverRef.current || !root) {
        endMove();
        return;
      }
      // held: what is still on its way plays out first (the page held still), so the copy is taken at rest
      void whenSettled(flight, root).then(() => {
        if (flightRef.current !== flight) return;
        if (handoverRef.current) landHeld(flight);
        else endMove();
      });
    });
  }, [move, dropLayer, dropStill, endMove, handOver, holdSnap, landHeld]);

  /* ── a new pick or order ── */

  // its posts near the day you're on (and the pictures they open on): fetched while the screen holds still
  const prepare = useCallback((next: StreamScope): Promise<void> => {
    // the day you're on, read where the page really is (the network the pick's now)
    cancelZoomWait();
    handOver();
    stopPrewarm();
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
    /* A scope seen before (this session, or kept on the device: keep.ts) is at hand: its posts and pictures get a
       moment. The first time, its index has to come from the server (a second or more on a phone's network): the
       screen holds still until it is in (the tapped circle shows it is coming), and the posts and pictures it opens
       on get longer. Never a cut to a skeleton: the move plays once the page is there */
    const cold = !peekStreamIndex(next);
    return (cold ? indexReady(next, INDEX_WAIT_MS) : Promise.resolve())
      .then(() => viewReady(next, current.mode, size, { day, zid: null }, cold ? COLD_WAIT_MS : PICK_WAIT_MS));
  }, [cancelZoomWait, handOver, stopPrewarm]);

  // ...and shown: the screen as it is now is kept for the move, and apply() shows the new pick in the same commit
  const commit = useCallback((apply: () => void, options: { profile: boolean; slide?: StreamSlide }) => {
    cancelZoomWait();
    handOver();
    dropStill();
    stopPrewarm();
    dropHold(true);
    // a pinch's spring still running (or a pinch begun while the pick was being made ready): the page is let go first,
    // so it is measured as it is drawn (the spring's end never turns the Day view's snap on under the move; the rest of
    // that pinch does nothing, as it does when a zoom takes the page)
    pinchRef.current = null;
    releaseLayer();
    // other posts: the last zoom's focal means nothing here
    lastZoomRef.current = null;
    focalHoldRef.current = null;
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
    const nextPosts = loadedPosts(pick.next, nextIndex);
    const target = computeLayout({ mode: current, days: nextIndex.days, posts: nextPosts, width: size.width, height: size.height, insets: size.insets, wide: wideOf(size) });
    const nextOrigin = originFor(size, pick.next);
    const vh = window.innerHeight;
    // the day you were on (or the nearest older one it has posts on); in the profile, where you are; else the top (a
    // grid's group under the header's edge as it stands: its jump keeps the fold)
    let scroll = pick.keepScroll ? window.scrollY : 0;
    if (pick.day) {
      const at = indexForDay(target, pick.day);
      const headerEdge = compressedRef.current ? size.insets.topCompressed : size.insets.top;
      if (at >= 0) scroll = current === 'today' ? nextOrigin + dayRestTop(target, at) : nextOrigin + target.items[at].y - headerEdge;
    }
    // a new order slides in under the tabs: they keep their place while they are on screen; read further down, the new
    // list opens at its start, under the header's edge
    const slideAsk = options.slide ?? null;
    if (slideAsk) {
      const headerEdge = compressedRef.current ? size.insets.topCompressed : size.insets.top;
      const listTop = originRef.current - window.scrollY;
      if (listTop >= headerEdge) scroll = window.scrollY;
      else scroll = current === 'today' ? nextOrigin + dayRestTop(target, 0) : nextOrigin - headerEdge;
    }
    scroll = clamp(scroll, 0, Math.max(0, nextOrigin + target.totalHeight - vh));
    const shots = snapshot(layer);
    const profileEl = profileRef.current;
    const profileShot = profileEl ? rectOf(profileEl) : null;
    // an order tapped while the last one is still sliding: where its list stands on screen, read before that slide ends
    const midSlide = slideAsk && moveRef.current?.slide && flightRef.current && !flightRef.current.ended
      ? new DOMMatrixReadOnly(getComputedStyle(layer).transform).m41
      : 0;
    // (not an order: an order still sliding hands its copy off; a new order takes the screen from where it stands)
    if (!slideAsk) handSlideOff();
    endFlight(flightRef.current);
    flightRef.current = null;
    endFlight(openingRef.current);
    openingRef.current = null;
    window.clearTimeout(spotTimerRef.current);
    moveRef.current?.profileCopy?.remove();
    moveRef.current?.slide?.cover.remove();
    /* a new order: the list as it stands, copied (still, its pictures as canvases), to go out one side as the new one
       comes in; cut at the list's top, so the profile and its tabs over it stay themselves */
    const root = rootRef.current;
    let slideCover: StillFrame | null = null;
    if (slideAsk && root) {
      // (the strip at the screen's top it leaves alone is a phone's status bar: from a laptop up, nothing is left bare)
      slideCover = stillFrame(root, { zIndex: STILL_Z, held: true, edge: wideOf(size) ? 0 : undefined });
      const line = Math.max(0, layer.getBoundingClientRect().top - (parseFloat(slideCover.element.style.top) || 0));
      slideCover.element.style.clipPath = `inset(${line.toFixed(1)}px 0px 0px 0px)`;
    }
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
      centre: { x: leftRef.current + size.width / 2, y: (size.insets.topCompressed + vh - size.insets.bottom) / 2 },
      focalKey: null,
      scroll,
      origin: nextOrigin,
      profileCopy,
      slide: slideAsk && slideCover ? { dir: slideAsk, cover: slideCover, from: midSlide } : null,
      posts: nextPosts,
      index: nextIndex,
      cover: null,
    };
    moveRef.current = nextMove;
    // the move brings in what it shows: the tab's opening is over (held or not yet begun, it would hide what landed)
    arrivedRef.current = true;
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
      setEntrance(null);
      setMove(nextMove);
      apply();
    });
  }, [cancelZoomWait, dropHold, dropStill, endMove, handOver, handSlideOff, keepHeight, originFor, releaseLayer, stopPrewarm]);

  /* A pick or a zoom about to be taken (a hover, a press; the phone's zoom menu opening: every other zoom, in the one
     warm): the days it opens on are fetched and, as they arrive, the pictures it would show first are decoded, for a
     few seconds at most. Several zooms at once: only a zoom next to this one is decoded (from WEEK, both), nearest the
     place first and MENU_DECODES at most between them; the rest are only fetched, behind what the screen wants
     (warmSpare). One zoom laid out a frame (the phone's menu opening is drawn on the main thread), and a zoom whose
     days are all in isn't laid out again as the others' come in. Never while just scrolling */
  // place: where the zooms open (a pinch's post under the fingers), else from where the page is
  const warmView = useCallback((next: { scope?: StreamScope; modes?: readonly FeedMode[]; place?: Place }) => {
    const current = layoutRef.current;
    const size = metricsRef.current;
    const mode = modeRef.current;
    if (!current || !size || !mode || !visibleRef.current) return;
    // something real is coming: the idle warm-up gives way
    stopPrewarm();
    const forScope = next.scope ?? inputsRef.current.scope;
    const forModes = next.modes?.length ? next.modes : [mode];
    // where the page is, as last read (kept every scroll frame; a held move's, where it lands): an intent comes in the
    // middle of other work (a menu opening), and a read of the page's own would have WebKit work its styles out for it
    const top = savedYRef.current - originRef.current;
    // (a zoom opens where zoomTo places it; a pick, at the day you're on)
    const here: Place = next.place ?? (next.scope
      ? { day: dayAt(current, top, readingLine(size.insets, compressedRef.current)), zid: null }
      : placeOf(current, null, zoomView(size, savedYRef.current, originRef.current, compressedRef.current), lastZoomRef.current, savedYRef.current));
    const { day, zid } = here;
    const key = `${scopeKey(forScope)}|${forModes.join(',')}|${day ?? ''}|${zid ?? ''}`;
    const warming = warmingRef.current;
    if (warming.has(key)) return;
    // one warm at a time: the latest intent is the one that matters
    warming.forEach((stopOther) => stopOther());
    if (day && scopeKey(forScope) !== scopeKey(inputsRef.current.scope)) prefetchDays(forScope, Array.from({ length: 8 }, (_, step) => shiftDay(day, -step)));
    const warmed = new Set<string>();
    const several = forModes.length > 1;
    let decodes = several ? MENU_DECODES : Number.POSITIVE_INFINITY;
    const spared: string[] = [];
    // the zooms whose days were all in (all asked for: not laid out again as the others' days come in); the ones still
    // to look at this round, one a frame; and whether days came in during it (another round once it is done)
    const settled = new Set<FeedMode>();
    let round: FeedMode[] = [];
    let dirty = false;
    let unsubscribe: (() => void) | null = null;
    let deadline = 0;
    let frame = 0;
    const stop = () => {
      unsubscribe?.();
      unsubscribe = null;
      window.clearTimeout(deadline);
      cancelAnimationFrame(frame);
      frame = 0;
      warming.delete(key);
    };
    const attempt = () => {
      frame = 0;
      if (!round.length) {
        round = forModes.filter((forMode) => !settled.has(forMode));
        dirty = false;
      }
      const forMode = round[0];
      if (forMode) {
        const view = viewPictures(forScope, forMode, size, { day, zid });
        // (the scope's index not here yet: looked at again as the store changes)
        if (!view) return;
        round.shift();
        const fresh = view.srcs.filter((src) => !warmed.has(src));
        fresh.forEach((src) => warmed.add(src));
        const nextTo = !several || Math.abs(ZOOM_ORDER.indexOf(forMode) - ZOOM_ORDER.indexOf(mode)) === 1;
        const decode = nextTo ? fresh.slice(0, decodes) : [];
        decodes -= decode.length;
        if (decode.length) void warmPictures(decode);
        if (fresh.length > decode.length) {
          spared.push(...fresh.slice(decode.length));
          warmSpare(spared);
        }
        if (view.missing.length) prefetchDays(forScope, view.missing);
        else settled.add(forMode);
      }
      // the next zoom (or, days in meanwhile, another round) on the next frame
      if (round.length || dirty) {
        frame = requestAnimationFrame(attempt);
        return;
      }
      if (settled.size === forModes.length) stop();
    };
    warming.set(key, stop);
    deadline = window.setTimeout(stop, WARM_MS);
    unsubscribe = subscribeStream(() => {
      // (a round under way: the zooms it has looked at already are looked at again once it is done)
      if (frame) dirty = true;
      else frame = requestAnimationFrame(attempt);
    });
    attempt();
  }, [stopPrewarm]);
  useEffect(() => () => warmingRef.current.forEach((stop) => stop()), []);

  /* ── going places ── */

  /* A scroll the page makes itself to somewhere else on it: a glide, or on WebKit, further than GLIDE_MAX_SCREENS, a jump
     under a still copy of the screen that drifts the way the page goes, waits for WebKit to draw, and dissolves */
  const goTo = useCallback((top: number) => {
    cancelZoomWait(true);
    landSlide();
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
  }, [cancelZoomWait, coverPage, handOver, landSlide, scrollPage]);

  const scrollToNewest = useCallback(() => {
    if (dayOpenRef.current) {
      dayLayerRef.current?.toNewest();
      return;
    }
    cancelZoomWait(true);
    landSlide();
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
  }, [cancelZoomWait, goTo, handOver, landSlide, scrollPage]);

  // the Day view: one post on or back (a key)
  const stepPost = useCallback((direction: 1 | -1) => {
    const current = layoutRef.current;
    if (!current || current.mode !== 'today' || moveRef.current) return;
    const top = pageY() - originRef.current;
    const from = dayAtProfile(current, top) ? -1 : dayIndexAt(current, top);
    const to = clamp(from + direction, -1, current.items.length - 1);
    if (to === from) return;
    scrollPage(to < 0 ? 0 : originRef.current + dayRestTop(current, to), !reducedMotion());
  }, [pageY, scrollPage]);

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
    focusCard: (post, gliding = false) => {
      const current = layoutRef.current;
      const at = current?.mode === 'today' ? current.byPost.get(post.key) : undefined;
      if (!current || at == null || moveRef.current) return false;
      const top = pageY() - originRef.current;
      const rest = dayRestTop(current, at);
      /* the card the page rests on before the scroll that brought it there has settled (the one in view is only told
         then: settle), or the one a tap stopped the page gliding onto, short of its rest: it is the one in view now,
         and the tap goes on to read it (it glides on to its rest if it stopped short). A card peeking either side
         glides into view */
      const inView = gliding || (!dayAtProfile(current, top) && dayIndexAt(current, top) === at);
      if (inView && activeRef.current !== at) {
        activeRef.current = at;
        setActive(at);
      }
      if (Math.abs(rest - top) > 2) scrollPage(originRef.current + rest, !reducedMotion());
      return inView;
    },
    openWeek: (day, element) => {
      const rect = element.getBoundingClientRect();
      zoomTo('week', { place: { start: false, zid: null, day }, point: { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 } });
    },
    manage: () => callbacksRef.current.onManage(),
    openHighlight,
  }), [openHighlight, pageY, scrollPage, zoomTo]);

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

  // where a load opens is the Feed's to say (where you were, else the top): the browser puts back no scroll of its own,
  // which on a reload deep in the feed came once the page had grown, after the profile and the arrival were drawn
  useIsoLayoutEffect(() => {
    const session = window.history;
    if (!('scrollRestoration' in session)) return undefined;
    const previous = session.scrollRestoration;
    session.scrollRestoration = 'manual';
    return () => {
      session.scrollRestoration = previous;
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
      // an order's old list still sliding out lies over the whole screen: it goes with the tab
      dropHanded();
      // (a zoom still waiting never lands on a hidden tab)
      zoomWaitRef.current = null;
      releaseLayer();
      // an opening still held starts again when the tab is back (StrictMode's second run, too)
      dropHold(false);
      stopPrewarm();
      snapHeldRef.current = false;
      holdSnapStyle(false);
      root?.querySelectorAll('video').forEach((video) => video.pause());
    };
  }, [dropHanded, dropHold, dropStill, endMove, holdSnap, releaseLayer, scrollPage, stopPrewarm]);

  // keep what the handlers read current
  useIsoLayoutEffect(() => {
    layoutRef.current = layout;
    metricsRef.current = metrics;
    originRef.current = origin;
    genRef.current = gen;
    inputsRef.current = { index, posts, scope };
    compressedRef.current = headerCompressed;
    callbacksRef.current = { onModeChange, onManage, play };
    if (!moveRef.current) renderedRef.current = layout && mountView ? { layout, gen, from: mountFrom, to: mountTo, win: mountView === win ? win : null } : null;
  });

  // the viewport and the chrome around it (the open header's bottom, the folded header's, the nav's top, the small
  // viewport: stable while Safari's toolbar folds), and where the layer starts in the page
  const measure = useCallback(() => {
    const root = rootRef.current;
    if (!root) return;
    const width = root.clientWidth;
    const left = columnLeft(root);
    leftRef.current = left;
    const read = (node: HTMLElement | null, fallback: number) => (node ? node.getBoundingClientRect().height : 0) || fallback;
    const height = read(probeViewRef.current, window.innerHeight);
    if (!width || !height) return;
    const desktop = width + left >= 1024;
    const screen = read(probeScreenRef.current, height);
    const insets: StreamInsets = {
      top: read(probeTopRef.current, desktop ? 188 : 162),
      topCompressed: read(probeFoldRef.current, desktop ? 88 : 78),
      bottom: read(probeNavRef.current, desktop ? 90 : 86),
      dock: read(probeDockRef.current, 20),
      screen,
      ...pageBeyond(screen),
    };
    setMetrics((current) => (
      current && Math.abs(current.width - width) < 0.5 && Math.abs(current.height - height) < 0.5 && sameInsets(current.insets, insets) && current.left === left
        ? current
        : { width, height, insets, left }
    ));
    const layer = layerRef.current;
    if (layer && !moveRef.current) {
      // (in the page's own terms: a held scroll's offset, kept through a finger's scroll, isn't where the layer lies)
      const next = pageTop(layer) + (handoverRef.current?.by ?? 0);
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
    [rootRef.current, probeTopRef.current, probeFoldRef.current, probeNavRef.current, probeDockRef.current, probeViewRef.current, probeScreenRef.current, profileEl].forEach((node) => {
      if (node) observer.observe(node);
    });
    return () => observer.disconnect();
  }, [measure, profileEl]);

  // the zoom: the one last used at this breakpoint (the Day view on a phone, the week from 1024px, by default)
  useIsoLayoutEffect(() => {
    if (!metrics) return;
    const breakpoint = breakpointOf(viewOf(metrics));
    if (breakpointRef.current === breakpoint) return;
    const first = breakpointRef.current === null;
    breakpointRef.current = breakpoint;
    const wanted = initialMode(viewOf(metrics));
    // (a phone's Day is the layer over a grid: the page is the week under it)
    const layered = wanted === 'today' && dayLayerOn(metrics);
    const pageWanted: FeedMode = layered ? 'week' : wanted;
    if (!dayLayerOn(metrics)) dropDay();
    if (first || !modeRef.current) {
      modeRef.current = pageWanted;
      setMode(pageWanted);
      if (layered) openDay(null, null, true);
      return;
    }
    if (pageWanted !== modeRef.current) {
      endMove();
      modeRef.current = pageWanted;
      setMode(pageWanted);
    }
    if (layered) openDay(null, null, true);
  }, [metrics, endMove, dropDay, openDay]);

  // a new scope: what was measured against the last one is let go (an opening still held waits for the new one's posts)
  useIsoLayoutEffect(() => {
    dayWinRef.current = null;
    if (!moveRef.current) seekRef.current = null;
    dropHold(false);
    stopPrewarm();
    prewarmLeftRef.current = [];
  }, [gen, dropHold, stopPrewarm]);

  // the last zoom's focal holds only for the scope and the screen it was taken on
  useIsoLayoutEffect(() => {
    lastZoomRef.current = null;
  }, [gen, metrics]);

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
  // a stack's cards rest at their top (just under the status bar); a theatre's in the band's middle. A stack's mark is
  // only its card's head: one as tall as its card is taller than the snap's room (the browser's bars in), and the page
  // may then rest anywhere along it
  const markAlign: 'start' | 'center' = layout?.deck ? 'start' : 'center';
  const markHeight = layout?.deck ? MARK_HEAD : layout?.cardHeight ?? 0;
  const [marks, setMarks] = useState<{ count: number; top: number; stride: number; height: number; align: 'start' | 'center' } | null>(null);
  useIsoLayoutEffect(() => {
    if (move) return;
    setMarks((current) => {
      if (!markCount) return null;
      if (current && current.count === markCount && current.top === markTop && current.stride === markStride && current.height === markHeight && current.align === markAlign) return current;
      return { count: markCount, top: markTop, stride: markStride, height: markHeight, align: markAlign };
    });
  }, [move, markCount, markTop, markStride, markHeight, markAlign]);
  const snapMarks = useMemo(() => {
    if (!marks) return null;
    return Array.from({ length: marks.count }, (_, at) => (
      <div
        key={at}
        aria-hidden="true"
        data-still-skip=""
        className="pointer-events-none absolute left-0 w-px"
        // (one swipe, one post: a flick never carries past the next card)
        style={{ top: marks.top + at * marks.stride, height: marks.height, scrollSnapAlign: marks.align, scrollSnapStop: 'always' }}
      />
    ));
  }, [marks]);
  // (the dock sits on a card there, never on the page: no light round it. dock.css)
  useIsoLayoutEffect(() => {
    if (!layout?.deck) return undefined;
    const html = document.documentElement;
    html.dataset.feedDeck = '';
    return () => {
      delete html.dataset.feedDeck;
    };
  }, [layout?.deck]);
  /* ...and while a card is up (past the profile, not mid-move), the page's own colours are the cards' dark: iOS Safari
     tints the edge of its status bar and its toolbar from them (its theme colour, the page's background), and a light
     one hazes the top of the card's head, under the clock. Back at the profile or out of the Day view, the page's own
     again (globals.css, [data-feed-card]) */
  const underCard = Boolean(layout?.deck) && !atProfile && !move;
  // (a move between a phone's cards and a grid sets them itself, from where the card it carries is: watchCardEdge)
  const crossingCards = Boolean(move) && (move?.from === 'today') !== (move?.to === 'today');
  useIsoLayoutEffect(() => {
    // (landed: the card's own head is drawn under the clock again)
    if (!move) {
      rootRef.current?.removeAttribute('data-card-edge');
      rootRef.current?.removeAttribute('data-card-dim');
      if (rootRef.current) rootRef.current.style.transitionDuration = '';
    }
    // (Day as the layer over the grid sets them itself)
    if (crossingCards || dayOpenRef.current) return;
    if (underCard) cardPage(true);
    // (taken early by a landing that, after all, doesn't rest on a card: endMove; or a move setting off from a card)
    else if (document.documentElement.dataset.feedCard !== undefined) cardPage(false);
  }, [underCard, move, crossingCards]);
  useIsoLayoutEffect(() => () => cardPage(false), []);
  const insets = metrics?.insets ?? null;
  // what the Day view's cards snap to: a phone's rest line (its cards rest at their top, under the status bar:
  // layout.restTop); a
  // theatre's band (under the folded header, down to the nav), its cards centred in it
  const deckOn = Boolean(layout?.deck);
  const snapTop = insets ? (deckOn && layout ? layout.restTop : insets.topCompressed) : undefined;
  const snapBottom = insets ? (deckOn ? insets.dock ?? insets.bottom : insets.bottom) : undefined;
  useIsoLayoutEffect(() => {
    if (!phoneDay || move || snapTop == null || snapBottom == null) return undefined;
    const html = document.documentElement;
    html.style.setProperty('--fm-feed-snap-top', `${snapTop}px`);
    html.style.setProperty('--fm-feed-snap-bottom', `${snapBottom}px`);
    // held as it comes on (a still page would be snapped at once by WebKit, by its own old place in the list), unless
    // a finger has taken it and the page is still on the move under it (a swipe begun as a move landed: it keeps the
    // snap, and ends on a card)
    if (snapHeldRef.current || !scrollingRef.current) holdSnap();
    applySnap(!pinchRef.current);
    return () => {
      applySnap(false);
      html.style.removeProperty('--fm-feed-snap-top');
      html.style.removeProperty('--fm-feed-snap-bottom');
    };
  }, [phoneDay, move, snapTop, snapBottom, applySnap, holdSnap]);

  // the layout changed under the reader (a day loaded with sizes its index didn't have, a rotation): the piece at the
  // reading line holds still
  const narrowWindow = !win || win.gen !== gen || win.mode !== layout?.mode || win.narrow;
  useIsoLayoutEffect(() => {
    narrowRef.current = narrowWindow;
  }, [narrowWindow]);
  useIsoLayoutEffect(() => {
    const prev = prevRef.current;
    if (!layout) return;
    const nowOrigin = originRef.current;
    prevRef.current = { layout, origin: nowOrigin };
    if (moveRef.current) return;
    if (!prev || (prev.layout === layout && prev.origin === nowOrigin) || prev.layout.mode !== layout.mode) {
      if (!prev || prev.layout !== layout) {
        syncFrame(pageY(), narrowWindow);
        lookAhead(pageY(), false);
      }
      return;
    }
    let scrollY = pageY();
    const line = readingLine(prev.layout.insets, compressedRef.current);
    const top = scrollY - prev.origin;
    // only when reading the posts (looking at the profile, the page simply grows under it), and never under a scroll in
    // progress: a scroll the page makes while a finger, momentum or a glide moves it stops that scroll dead on iOS (and
    // with the snap held, it ends between two cards). Such a change (rare: a day loading with sizes its index didn't
    // have) shows as it is
    // a zoom's focal still held where it landed (focalHoldRef): it is what holds still, not the reading line's piece
    const hold = focalHoldRef.current;
    if (hold && (performance.now() > hold.until || (hold.scroll != null && Math.abs(scrollY - hold.scroll) > 4))) focalHoldRef.current = null;
    const focal = focalHoldRef.current;
    const focalAt = focal ? prev.layout.byPost.get(focal.postKey ?? '') ?? indexOfSlot(prev.layout, focal.id) : -1;
    if (focal && focalAt >= 0 && !scrollingRef.current) {
      const item = prev.layout.items[focalAt];
      const offset = layout.mode === 'today' ? dayRestTop(prev.layout, focalAt) - top : item.y - top;
      const next = topForAnchor(layout, { key: item.key, postKey: item.postKey ?? null, id: idOf(item), offset });
      if (next != null) {
        const target = Math.max(0, nowOrigin + next);
        if (Math.abs(target - scrollY) >= 0.5) {
          markScrollJump();
          scrollPage(target, false);
          scrollY = window.scrollY;
          savedYRef.current = scrollY;
          settledTopRef.current = scrollY;
        }
        focal.scroll = scrollY;
      }
    } else if (!scrollingRef.current && scrollY >= 1 && top + line >= 0 && !(layout.mode === 'today' && dayAtProfile(prev.layout, top))) {
      // (not at the page's very top: there the top stays the top. The tab opening draws the profile in only once the
      // layer has been placed without it, and read against that place the reading line was the first post's: the page
      // went down by the profile, and then by every post a fresh index put over that one)
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

  // a highlight opened before its day had loaded: once it has, the post in view is that post (looked at again as a move
  // lands: a landing keeps its layout)
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
    if (dayIndexAt(layout, pageY() - originRef.current) === at && !dayAtProfile(layout, pageY() - originRef.current)) return;
    markScrollJump('place');
    scrollPage(originRef.current + dayRestTop(layout, at), false);
    savedYRef.current = window.scrollY;
    settledTopRef.current = window.scrollY;
    activeRef.current = at;
    setActive(at);
    syncFrame(window.scrollY);
  }, [layout, move, scrollPage, syncFrame]);

  /* The first posts on screen (the tab opening) come in, a ring at a time from the top, once they are here: till then
     (their days loading, the screen's pictures decoding: ARRIVAL_WAIT_MS at most, then it comes in as it is) the layer's cells
     are held unseen (the profile above them stays), so the entrance plays on the posts, not on blocks they then
     replace. A launch that drew the skeleton keeps the slots it showed as they are (each post comes up over its
     slot as its day loads, below) and brings in only what is new. Then the other zooms are made ready */
  useIsoLayoutEffect(() => {
    if (!layout || !layout.items.length || moveRef.current || arrivedRef.current || holdRef.current || !visibleRef.current) return;
    const layer = layerRef.current;
    const size = metricsRef.current;
    if (!layer || !size || reducedMotion()) {
      arrivedRef.current = true;
      announceFirstScreen();
      prewarm();
      return;
    }
    // (a skeleton drawn under the launch veil was never seen: the posts are waited for, as with none)
    const skeletonShown = skeletonPaintedRef.current && !document.documentElement.classList.contains(LAUNCH_CLASS);
    // freeze: what it was laid out from is held until it is in (entrance; not over a skeleton's slots, which take
    // their posts as they come)
    const ring = (into: HTMLElement, freeze: boolean) => {
      arrivedRef.current = true;
      // the app's first screen is in: a launch veil goes, and this entrance plays as it does
      announceFirstScreen();
      const vw = window.innerWidth;
      const vh = window.innerHeight;
      const flight = startFlight(holdsScroll() ? FLIGHT_HOLD_MS : 0);
      endFlight(openingRef.current);
      openingRef.current = flight;
      const from = { x: leftRef.current + (metricsRef.current?.width ?? vw) / 2, y: 0 };
      // (not a piece already coming in some other way: the tab's own arrival, AppTabHost's)
      const shown = Array.from(into.querySelectorAll<HTMLElement>(':scope > [data-cell]'), (el) => [el, rectOf(el)] as const)
        .filter(([el, now]) => onScreen(now, vw, vh) && !(skeletonShown && el.dataset.kind === 'placeholder') && !el.getAnimations().some((animation) => !followsScroll(animation)));
      // (a phone card stands where the Day view's depth has it: it only dissolves in)
      shown.forEach(([el, now]) => enter(flight, el, now, from, 0, el.dataset.depth !== undefined));
      const { posts: shownNow, index: indexNow } = inputsRef.current;
      const kept = freeze && shown.length && indexNow ? { gen: genRef.current, posts: shownNow, index: indexNow } : null;
      if (kept) setEntrance(kept);
      if (flight.hold) requestAnimationFrame(() => requestAnimationFrame(() => releaseFlight(flight)));
      void flightDone(flight).then(() => {
        if (openingRef.current === flight) openingRef.current = null;
        endFlight(flight);
        // in: what came in meanwhile is let in (a move since let it go already)
        if (kept) setEntrance((current) => (current === kept ? null : current));
        prewarm();
      });
    };
    if (skeletonShown) {
      ring(layer, false);
      return;
    }
    const token = {};
    holdRef.current = token;
    layer.dataset.holding = '';
    const heldAt = performance.now();
    const top = window.scrollY - originRef.current;
    const near = nearestPost(layout, size.width / 2, top + window.innerHeight / 2, window.innerHeight);
    void viewReady(inputsRef.current.scope, layout.mode, size, { day: null, zid: near >= 0 ? idOf(layout.items[near]) : null }, ARRIVAL_WAIT_MS, ARRIVAL_REACH).then(() => {
      // a frame on: what came in is drawn, and the rest of the window has gone up (not mounted under the entrance)
      let frames = 0;
      const go = () => requestAnimationFrame(() => {
        if (holdRef.current !== token) return;
        frames += 1;
        // its screen's posts not all in yet (a slow network): held on, unseen, till they are (ARRIVAL_MAX_MS at most;
        // under the launch's veil, for as long as the veil is up). Where the page rests, as last read: no read of the
        // page's own
        const current = layoutRef.current;
        const sizeNow = metricsRef.current;
        const top = savedYRef.current - originRef.current;
        const ready = Boolean(current && sizeNow && screenLoaded(current, top, sizeNow.height))
          && (!current || !sizeNow || performance.now() - heldAt >= KEPT_WAIT_MS || daysSettled(inputsRef.current.scope, daysInRange(current, top, top + sizeNow.height)));
        // in: the launch's veil may go (and this entrance plays as it does)
        if (ready) announceFirstScreen();
        const veiled = launchHolds();
        const loading = !ready && (veiled || performance.now() - heldAt < ARRIVAL_MAX_MS);
        // (nor while a zoom asked for meanwhile waits for its days: the zoom's move brings the cells in)
        if ((narrowRef.current && frames < 4) || loading || veiled || zoomWaitRef.current) {
          go();
          return;
        }
        const live = layerRef.current;
        dropHold(false);
        if (live) ring(live, true);
      });
      go();
    });
  }, [layout]);

  // whether this commit drew the skeleton (the next one's arrival asks)
  useIsoLayoutEffect(() => {
    skeletonPaintedRef.current = Boolean(skeleton);
  });

  // nothing to show (no feeds yet, an error): that is the first screen, and a launch veil goes
  useEffect(() => {
    if (blank) announceFirstScreen();
  }, [blank]);

  // tell the tab which zoom is on (its header control shows it)
  useIsoLayoutEffect(() => {
    if (mode) callbacksRef.current.onModeChange(dayOpen ? 'today' : mode);
  }, [mode, dayOpen]);

  // the scroll: window.scrollY once a frame; settled on 'scrollend' or after a quiet spell
  useIsoLayoutEffect(() => {
    let frame = 0;
    let timer = 0;
    const tick = () => {
      frame = 0;
      if (!visibleRef.current || pinchRef.current) return;
      const scrollY = pageY();
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
      // (a touch that lands now lands on a page still gliding: press.ts)
      notePageScroll();
      if (touchingRef.current) takeSnap();
      // the page on the move: the other zooms are made ready once it rests again (settle)
      stopPrewarm();
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
    const onTouchStart = (event: TouchEvent) => {
      touchingRef.current = true;
      scrollingRef.current = true;
      if (event.touches.length === 1) armSnap();
      stopPrewarm();
      // (a zoom still waiting for its posts gives way to the finger)
      cancelZoomWait();
      // (and a zoom's focal is no longer held: the page is the finger's)
      focalHoldRef.current = null;
      /* a touch ends on the element it began on, and one let go of while the finger is down (a grid tile's button, gone
         as a pinch lands in the Day view) is out of the page: its end never reaches the window, and the page would go
         on thinking a finger is down (nothing settles; a scroll the page makes would let its snap go). Heard there too,
         once an element, and acted on there only when it has left the page (otherwise the window hears it as ever) */
      const target = event.target;
      if (target instanceof Node && !touchTargets.has(target)) {
        touchTargets.add(target);
        target.addEventListener('touchend', onTargetEnd, { passive: true });
        target.addEventListener('touchcancel', onTargetEnd, { passive: true });
      }
    };
    const touchTargets = new Set<Node>();
    const unhearTarget = (target: Node) => {
      touchTargets.delete(target);
      target.removeEventListener('touchend', onTargetEnd);
      target.removeEventListener('touchcancel', onTargetEnd);
    };
    function onTargetEnd(event: Event) {
      const target = event.currentTarget;
      if (target instanceof Node) unhearTarget(target);
      // (still in the page: the window hears it too, and acts on it there)
      if (target instanceof Node && target.isConnected) return;
      onTouchEnd(event as TouchEvent);
    }
    const onWheel = (event: WheelEvent) => {
      if (!event.ctrlKey) cancelZoomWait();
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
    window.addEventListener('wheel', onWheel, { passive: true });
    return () => {
      window.removeEventListener('wheel', onWheel);
      touchTargets.forEach(unhearTarget);
      if (frame) window.cancelAnimationFrame(frame);
      if (timer) window.clearTimeout(timer);
      window.removeEventListener('scroll', onScroll);
      window.removeEventListener('scrollend', onScrollEnd);
      window.removeEventListener('touchstart', onTouchStart);
      window.removeEventListener('touchend', onTouchEnd);
      window.removeEventListener('touchcancel', onTouchEnd);
      window.removeEventListener('wheel', takeSnap);
    };
  }, [armSnap, cancelZoomWait, lookAhead, pageY, settle, stopPrewarm, syncFrame]);

  // a first frame mounts only the screen; the rest of the window a frame later
  useEffect(() => {
    if (!layout || !metrics || move || !narrowWindow) return undefined;
    const frame = window.requestAnimationFrame(() => {
      if (moveRef.current || !visibleRef.current) return;
      const view = mountWindow(layout, pageY() - originRef.current, metrics.height, false);
      renderedRef.current = null;
      setWin({ gen, mode: layout.mode, y0: view.y0, y1: view.y1, narrow: false });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [gen, layout, metrics, narrowWindow, move, pageY]);

  // the keys: G the Day view ⇄ the week, M the month, Esc the week, ← and → (or < and >) step the zoom in the rail's
  // order (← shorter: Month to Week to Day, → longer), as they step Lead's time range; - out and + in; ↑ ↓ J K a post at a time
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
        // (a key held down zooms once, not on through every zoom)
        if (!event.repeat) zoomTo(next);
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
        // ← → follow the rail's order, shorter on the left: ← in toward the Day, → out toward the Month
        case 'ArrowRight':
        case '>':
        case '-':
        case '_':
          go(stepMode(current, 1));
          break;
        case 'ArrowLeft':
        case '<':
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

  const onPinchStart = useCallback((fingers: PinchFocal) => {
    const root = rootRef.current;
    if (!root || !visibleRef.current || !modeRef.current) return;
    // a zoom waiting gives way; a page still springing home is let go before anything here is measured (a copy over
    // the page goes too: the page is the fingers')
    zoomWaitRef.current = null;
    if (pinchAnimRef.current || springRef.current) {
      cancelSpring();
      dropPinch();
      root.style.transformOrigin = '';
    }
    dropStill();
    /* The Day view is about one post at a time, the card on the focus line: a pinch there is about that card wherever
       the fingers are (on its foot, between two cards, on one peeking past it), and the page shrinks about its middle,
       so a loose pinch never sends the zoom to another post */
    let focal = fingers;
    const current = layoutRef.current;
    const onCard = modeRef.current === 'today' && !atProfileRef.current ? current?.items[activeRef.current] : undefined;
    const cardId = onCard ? idOf(onCard) : null;
    if (onCard && cardId) {
      const frame = layerRef.current?.querySelector<HTMLElement>(`:scope > [data-zid="${cardId}"] > [data-layer="frame"]`);
      const middle = frame ? centreOf(frameRect(frame)) : { x: fingers.x, y: fingers.y };
      focal = { x: middle.x, y: middle.y, zid: cardId, postKey: onCard.postKey ?? null };
    }
    // a spring still running from the last pinch lets go first: the page is this one's, and its box is read unscaled
    cancelSpring();
    endMove();
    dropHold(true);
    pinchRef.current = { focal, scale: 1 };
    if (snapOnRef.current) applySnap(false);
    const rect = root.getBoundingClientRect();
    root.style.transformOrigin = `${focal.x - rect.left}px ${focal.y - rect.top}px`;
    root.style.willChange = 'transform';
    pinchBackdrop(root, true);
  }, [applySnap, cancelSpring, dropHold, dropPinch, dropStill, endMove]);

  const onPinchScale = useCallback((scale: number) => {
    const root = rootRef.current;
    const pinch = pinchRef.current;
    const current = modeRef.current;
    if (!root || !pinch || !current) return;
    // past the last zoom either way, only a rubber band
    const room = scale < 1 ? stepMode(current, 1) : stepMode(current, -1);
    const shown = room ? scale : 1 + (scale - 1) * PINCH_EDGE;
    pinch.scale = shown;
    holdPinch(root, shown);
    // the way it is going known, the zoom it heads for gets its pictures fetched while the fingers are still down
    if (room && Math.abs(scale - 1) > PINCH_WARM_AT) {
      const zid = pinch.focal.zid;
      warmView({ modes: [room], place: zid ? { day: zid.split(':')[0] ?? null, zid } : undefined });
    }
  }, [holdPinch, warmView]);

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
    springHome(root, pinch.scale, resnap);
  }, [resnap, springHome, zoomTo]);

  // (not under Day's layer: the page under it is the grid, unseen, and its pinch held it zoomed and zoomed it on its own)
  usePinch(rootRef, { enabled: Boolean(layout) && !dayOpen, onStart: onPinchStart, onScale: onPinchScale, onEnd: onPinchEnd });

  /* ── render ── */

  const column = metrics ? contentBox(metrics.width) : null;
  const profileHeight = column ? profileHeaderHeight(column.contentWidth, scope) : 0;
  const showProfile = Boolean(column && metrics && (!blank || blank.profile));
  const wideDay = layout?.mode === 'today' && layout.mediaWidth != null;
  const profileTop = metrics ? metrics.insets.top + profilePad(metrics.width) : 0;
  // a phone's Day view snaps to the profile too, at the very top (the snap's line is the cards' rest line, under the
  // status bar)
  const profileSnap: CSSProperties = phoneDay && metrics && column && layout
    ? { scrollSnapAlign: 'start', scrollMarginTop: profileTop - (layout.deck ? layout.focusY - layout.cardHeight / 2 : metrics.insets.topCompressed) }
    : {};
  const profileHeader = showProfile && column && metrics ? (
    <div style={{ paddingTop: profileTop }}>
      <div
        key={who}
        ref={attachProfile}
        data-profile=""
        data-arrive=""
        className="mx-auto"
        style={{ width: column.contentWidth, height: profileHeight, ...profileSnap }}
      >
        <ProfileHeader
          scope={scope}
          title={profile.title}
          subtitle={profile.subtitle}
          feeder={profile.feeder}
          feeds={profile.feeds}
          faces={profile.faces}
          profile={index?.profile ?? null}
          fresh={profile.fresh}
          width={column.contentWidth}
          onManage={actions.manage}
          onOpenPost={actions.openHighlight}
        />
      </div>
      <div style={{ height: PROFILE_GAP }} />
    </div>
  ) : null;

  // what to draw: a spec per mounted item (the next move starts from these)
  const specs: Spec[] = [];
  // the posts drawn so far: a post is one cell (a day still listing one that has moved to another day, as the store
  // catches up, shows its old place as a slot), never two under one key (React would leave strays of them on the page)
  const drawnPosts = new Set<string>();
  if (!blank && drawn && metrics && mountView) {
    const shapeOf = (item: LayoutItem): CellShape => ({
      w: item.w,
      h: item.h,
      cols: item.cols,
      rows: item.rows,
      mediaWidth: drawn.mediaWidth ?? 0,
      wide: drawn.mode === 'today' && drawn.mediaWidth != null,
      clear: drawn.footClear,
      lift: drawn.lift,
      rise: drawn.rise,
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
      const post = layout && item.day && item.slot >= 0 ? shownPosts.get(item.day)?.[item.slot] ?? null : null;
      if (!post || drawnPosts.has(post.key)) {
        specs.push({ kind: 'placeholder', key: `x:${item.day}:${item.slot}`, mode: drawn.mode, shape: shapeOf(item), x: item.x, y: item.y });
        continue;
      }
      drawnPosts.add(post.key);
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
        // every phone card wears the day it went up (in the results order too, where its place is the day its result
        // landed: its tab says when the post is from)
        date: inDay && drawn.mediaWidth == null ? post.day || null : null,
        feederHandle: inDay || (!scope.handle && item.cols === 2 && item.rows === 2) ? post.handle : null,
        solo: Boolean(scope.handle),
      });
    }
  }
  // the old view's cells this move still draws (those the new view doesn't: they go), and each shared post's old self
  const specKeys = new Set(specs.map((spec) => spec.key));
  const oldSpecs = new Map<string, Spec>();
  move?.leaving.forEach((spec) => oldSpecs.set(spec.key, spec));
  const gone = move ? move.leaving.filter((spec) => !specKeys.has(spec.key)) : [];

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
          lift={spec.mode === 'today' && drawn?.deck ? drawn.lift : 0}
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
        cardTop={wasPost?.mode === 'today' && !going && move?.focalKey === spec.post.key ? (move.shots.get(spec.key)?.grown ?? 0) : 0}
        cardBelow={wasPost?.mode === 'today' && !going && move?.focalKey === spec.post.key ? (move.shots.get(spec.key)?.below ?? 0) : 0}
        leavingDate={wasPost ? wasPost.date : null}
        leavingFeeder={wasPost?.feederHandle ? feeders.get(wasPost.feederHandle) ?? null : null}
        focal={!going && move?.focalKey === spec.post.key}
        // the post the Day view rests on plays, and goes on playing while a swipe carries it away (stopping it at the
        // swipe's start swapped the picture under the finger): the next one takes over once the page settles on it
        active={!going && inDay && spec.index === active && !atProfile && !move}
        lit={!going && inDay && spec.shape.wide && spec.index === active}
        spot={!going && inDay && spec.shape.wide && spotOn && spec.index === active}
        standout={spec.standout}
        date={spec.date}
        eager={spec.eager}
        feeder={spec.feederHandle ? feeders.get(spec.feederHandle) ?? null : null}
        solo={spec.solo}
        gone={going}
        depth={!going && inDay && Boolean(drawn?.deck) && !move}
        order={spec.index}
        clear={inDay && drawn?.deck ? drawn.footClear : 0}
        head={inDay && drawn?.deck ? drawn.head : 0}
        lift={inDay && drawn?.deck ? drawn.lift : 0}
        rise={inDay && drawn?.deck ? drawn.rise : 0}
        onOpenTile={actions.openTile}
        onFocusCard={actions.focusCard}
      />
    );
  };

  // what a phone's Day view stacks by (stream.css, st-yield): the line its cards rest on, and a stride
  const depthVars = drawn?.deck
    ? ({ '--st-rest': `${drawn.restTop}px`, '--st-stride': `${drawn.stride}px` } as CSSProperties)
    : null;

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
    /* Every cell in one order that never changes (by its key; they are placed absolutely, the order only stacks pieces
       that cross mid-move), so React never moves one in the page, only adds and removes. A cell moved in the page is
       taken out and put back, and its CSS animations start over: in layout order, a leftover the landed view mounted
       again above the cards a move brought had React move those cards instead, and their running animations started
       over */
    const cells: Array<{ spec: Spec; going: boolean }> = [...specs.map((spec) => ({ spec, going: false })), ...gone.map((spec) => ({ spec, going: true }))];
    cells.sort((a, b) => (a.spec.key < b.spec.key ? -1 : a.spec.key > b.spec.key ? 1 : 0));
    content = cells.map(({ spec, going }) => drawSpec(spec, going));
    layerHeight = layout ? layout.totalHeight : Math.max(metrics.height - origin, 0);
  }

  /* a wide Day view: the day of the post in view, big in the margin beside the column (when the margin has room). From a
     laptop up the margin runs on to the window's edge, under the side rail: the rail lies over the page as glass, so
     the date is there whether the rail is shut or open over it, at the same size, never coming and going with it */
  let stamp: ReactNode = null;
  if (layout && metrics && wideDay) {
    const gutter = (metrics.width - layout.cardWidth) / 2;
    const width = gutter + metrics.left - 72;
    // the numeral is sized to the margin the shut rail leaves clear (the column's edge is 16px short of the page's)
    const clear = metrics.left > 0 ? gutter - 48 : width;
    if (width >= 120) {
      const item = layout.items[Math.min(active, layout.items.length - 1)];
      const day = item?.day ?? null;
      const count = day ? index?.days.find((entry) => entry.day === day)?.count ?? 0 : 0;
      // a column down the layer's left margin, the stamp pinned in it on the focus line (sticky: it rides the page)
      stamp = (
        <div aria-hidden="true" className="pointer-events-none absolute top-0 z-[5]" style={{ left: 24 - metrics.left, width, height: layout.totalHeight }}>
          <div className="sticky" style={{ top: layout.focusY - layout.cardHeight / 2, height: layout.cardHeight }}>
            <DayStamp day={day} slot={item?.slot ?? 0} count={count} hidden={Boolean(move) || atProfile} width={clear} />
          </div>
        </div>
      );
    }
  }


  // what this render draws (a move out of it starts from these)
  useIsoLayoutEffect(() => {
    if (!moveRef.current) specsRef.current = specs;
  });

  /* A post whose day loaded where its placeholder stood (a move's held posts let in once it has landed, a scroll onto
     days still loading) comes up in place: a copy of the placeholder laid over it dissolves away, one layer a post (the
     post, its frame and a row's dimming are drawn as they are under it). Not during a move (its posts are held), a
     scroll, a held scroll or a still copy over the page; only on screen, and more than SWAP_IN_MAX at once come in as
     they are */
  useIsoLayoutEffect(() => {
    // (only against the same posts at the same zoom: another scope's or zoom's day:slot is another post, or another place)
    const view = `${gen}|${drawn?.mode ?? ''}`;
    const last = placeholdersRef.current;
    const was = last.view === view ? last.slots : null;
    const now = new Set<string>();
    specs.forEach((spec) => {
      if (spec.kind === 'placeholder') now.add(spec.key.slice(2));
    });
    placeholdersRef.current = { view, slots: now };
    const layer = layerRef.current;
    // (not while the tab's opening is held: its entrance brings them in)
    if (!was?.size || !layer || moveRef.current || holdRef.current || handoverRef.current || stillRef.current || !visibleRef.current) return;
    const swapped = specs.filter((spec): spec is Extract<Spec, { kind: 'post' }> => spec.kind === 'post' && spec.zid !== null && was.has(spec.zid));
    // (the page moving: a finger or its momentum, a glide, a wheel, a scroll frame a moment ago)
    if (!swapped.length || scrollingRef.current || performance.now() - speedRef.current.t < SETTLE_MS || reducedMotion()) return;
    // where the page is, as last read (not read again after this commit's writes)
    const top = savedYRef.current - originRef.current;
    const vh = window.innerHeight;
    const shown = swapped.filter((spec) => spec.y < top + vh && spec.y + spec.shape.h > top);
    if (!shown.length || shown.length > SWAP_IN_MAX) return;
    const like = (el: HTMLElement, frame: HTMLElement) => {
      el.className = 'st-placeholder';
      Object.assign(el.style, { position: 'absolute', left: frame.style.left, top: frame.style.top, width: frame.style.width, height: frame.style.height, borderRadius: frame.style.borderRadius });
    };
    // WebKit starts a dissolve's clock in the frame that first draws it, before that (heavy: a day just came in) frame
    // is on screen, and would show it already partway: held whole (the placeholder, as it was) until it is, like a move
    const fades = startFlight(holdsScroll() ? FLIGHT_HOLD_MS : 0);
    shown.forEach((spec) => {
      const cell = layer.querySelector<HTMLElement>(`:scope > [data-cell="${CSS.escape(spec.key)}"]`);
      const frame = cell?.querySelector<HTMLElement>(':scope > [data-layer="frame"]');
      if (!cell || !frame) return;
      // the placeholder as it stood: a phone's card, its block with its frame on it; a tile or a row, its frame
      const cover = document.createElement('div');
      if (spec.mode === 'today' && !spec.shape.wide) {
        cover.className = 'st-placeholder';
        Object.assign(cover.style, { position: 'absolute', inset: '0px', borderRadius: '28px' });
        const inner = document.createElement('div');
        like(inner, frame);
        cover.appendChild(inner);
      } else {
        like(cover, frame);
      }
      // (unseen once its dissolve is over, even before it is taken out)
      Object.assign(cover.style, { zIndex: SWAP_COVER_Z, pointerEvents: 'none', opacity: '0' });
      cover.setAttribute('aria-hidden', 'true');
      cell.appendChild(cover);
      coversRef.current.add(cover);
      const gone = () => {
        coversRef.current.delete(cover);
        cover.remove();
      };
      run(fades, cover, [{ opacity: 1 }, { opacity: 0 }], { duration: SWAP_IN_MS, easing: SWAP_IN_EASE, fill: 'backwards' });
      void fades.animations[fades.animations.length - 1].finished.then(gone, gone);
    });
    if (fades.hold && fades.animations.length) afterDrawn(() => releaseFlight(fades));
  });

  return (
    <div
      ref={rootRef}
      data-tab-scroll="own"
      className="fm-stream relative w-full"
      style={{ minHeight, background: 'var(--st-bg)', overflowAnchor: 'none' }}
    >
      {/* where the chrome sits, in the same terms AppShell and BottomNav place it (safe areas, the PWA fix, the
          header's heights for this tab), and the small viewport: measured on resize, never while scrolling. From a
          laptop up nothing sits over the page (SideRail): open or folded, the line is the window's top, less the gap
          the page keeps there */}
      <div aria-hidden="true" data-still-skip="" className="pointer-events-none invisible fixed left-0 top-0 h-0 w-0 overflow-hidden">
        <div
          ref={probeTopRef}
          className="w-px h-[calc(10px+env(safe-area-inset-top)+var(--pwa-top-fix,0px)+var(--fm-mobile-header-chrome-height,152px))] sm:h-[calc(14px+env(safe-area-inset-top)+var(--pwa-top-fix,0px)+var(--fm-mobile-header-chrome-height,152px))] md:h-[calc(20px+var(--pwa-top-fix,0px)+var(--fm-mobile-header-chrome-height,152px))] lg:h-[calc(var(--fm-desk-top)-12px)]"
        />
        <div
          ref={probeFoldRef}
          className="w-px h-[calc(10px+env(safe-area-inset-top)+var(--pwa-top-fix,0px)+var(--fm-mobile-header-chrome-compressed-height,68px))] sm:h-[calc(14px+env(safe-area-inset-top)+var(--pwa-top-fix,0px)+var(--fm-mobile-header-chrome-compressed-height,68px))] md:h-[calc(20px+var(--pwa-top-fix,0px)+var(--fm-mobile-header-chrome-compressed-height,68px))] lg:h-[calc(var(--fm-desk-top)-12px)]"
        />
        <div ref={probeNavRef} className="w-px h-[calc(86px+env(safe-area-inset-bottom))] md:h-[90px] lg:h-[24px]" />
        {/* the Day view's foot on a phone: the folded nav's circle sits in the corner, the card goes on to the safe area */}
        <div ref={probeDockRef} className="w-px h-[calc(20px+env(safe-area-inset-bottom))]" />
        <div ref={probeViewRef} className="w-px h-[100svh]" />
        {/* ...and the whole screen, as with the browser's bars away (a phone's Day card runs on to its foot) */}
        <div ref={probeScreenRef} className="w-px h-[100lvh]" />
      </div>
      {profileHeader}
      {dayOpen && metrics && index && dayHost ? (
        <DayLayer
          ref={dayLayerRef}
          start={dayOpen}
          scope={scope}
          index={index}
          feeders={feeders}
          size={metrics}
          host={dayHost}
          page={dayPage}
          tileOf={dayTile}
          placeTile={placeDayTile}
          onClosed={closedDay}
        />
      ) : null}
      <div
        ref={layerRef}
        className="relative isolate w-full"
        style={{ height: layerHeight, ...depthVars }}
        aria-busy={!index && !blank}
      >
        {snapMarks}
        {content}
        {stamp}
      </div>
    </div>
  );
}

export default memo(FeedStream);
