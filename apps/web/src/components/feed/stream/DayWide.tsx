'use client';

/* ─────────────────────────────────────────────────────────────
   DAY WIDE — the Day view on a wide window, as a layer over the
   grid (as a phone's DayLayer is): the page stays the Week or the
   Month it was, and the Day stands over the window, under the
   floating side rail. Its drawing is WideDay's (lib/feedStream/
   dayWide); this gives it the posts (the days round the one in
   view, as they load) and flies it in and out.

   In and out it moves as a phone's Day does, on the same move
   (dayMove): its own picture carried from the tile's picture to
   its place (and back), the grid zooming under it, the page's
   ground coming over, the post's number flying between the
   tile's corner and the Day's, its words in late and out early.
   Out, the page is first put where the post's tile is, unseen
   under the Day.
   ───────────────────────────────────────────────────────────── */

import { useCallback, useEffect, useImperativeHandle, useLayoutEffect, useMemo, useRef, useState, type Ref } from 'react';
import { daysAround, type DayLayerHandle, type DayStart } from '@/components/feed/stream/DayLayer';
import { DAY_HOLD_MS, DAY_MOVE_MS, PRIME_FRAMES, frames, layDayMove } from '@/components/feed/stream/dayMove';
import type { RankEnd } from '@/components/feed/stream/flightParts';
import { formatRate } from '@/components/feed/stream/PostReading';
import { mediaProxyUrl, type StreamFeeder, type StreamIndex, type StreamPost, type StreamScope } from '@/lib/feedStream/contract';
import { WideDay, type WidePost, type WideRead } from '@/lib/feedStream/dayWide';
import { computeLayout, type StreamInsets } from '@/lib/feedStream/layout';
import { endFlight, flightDone, reducedMotion, releaseFlight, startFlight, type Flight } from '@/lib/feedStream/motion';
import { useDayPosts } from '@/lib/feedStream/store';
import '@/components/feed/stream/dayWide.css';

// the days asked for either side of the post in view, in posts
const DAYS_AROUND = 8;
// an open waits this long at most for its post's day to come in (then opens on what there is)
const OPEN_WAIT_MS = 2000;
const CP = ['D1', 'D3', 'D7', 'D21'];
const KIND: Record<string, string> = { reel: 'Reel', carousel: 'Carousel', image: 'Post', unknown: 'Post' };
const FIRST_READ_MS = 24 * 60 * 60 * 1000;
// the swiped-far way out: how long the page is given to draw the tile it went to
const TILE_WAIT_FRAMES = 24;
// what the Day's picture wears and a tile doesn't (its mist, its shade, a carousel's dots and arrows, a reel's preview)
const PICTURE_ONLY = ':scope > :is(.dw-sink, .dw-mist, .dw-cnt, .dw-dots, .dw-arw, .dw-video)';
const NO_POSTS: ReadonlyMap<string, StreamPost[]> = new Map();

type DayWideProps = {
  ref?: Ref<DayLayerHandle>;
  start: DayStart;
  scope: StreamScope;
  index: StreamIndex;
  feeders: ReadonlyMap<string, StreamFeeder>;
  // where the stream's column starts in the window (past the side rail)
  left: number;
  // the grid under it (given way as the Day comes, back as it goes)
  page: () => HTMLElement | null;
  // a post's tile in the grid, when it is drawn
  tileOf: (key: string) => HTMLElement | null;
  // the page's scroll that brings a post's tile on screen (null: it is on screen, or the grid hasn't it)
  placeTile: (key: string) => number | null;
  onClosed: () => void;
  // a post's @handle or face (not on a feeder's own page)
  onFeeder?: (handle: string) => void;
};

// its first read: a day after it went up, by the stored time (the worker plans the read from it)
function waitLine(postedAt: string): string {
  const left = Date.parse(postedAt) + FIRST_READ_MS - Date.now();
  if (!Number.isFinite(left) || left <= 0) return 'First read due soon';
  if (left < 60 * 60 * 1000) return `First read in ${Math.max(1, Math.ceil(left / 60_000))}m`;
  return `First read in ${Math.ceil(left / 3_600_000)}h`;
}

function wideOf(post: StreamPost, feeder: StreamFeeder | null): WidePost {
  const ranked = post.results.filter((result) => result.topPercent != null && Number.isFinite(result.topPercent));
  const last = ranked.length - 1;
  const reads: WideRead[] = ranked.map((result, at) => ({
    cp: CP.indexOf(result.checkpoint),
    top: result.topPercent,
    // (the latest read's figures are the post's own, sent whole)
    likes: result.likes ?? (at === last ? post.likes : null),
    com: result.comments ?? (at === last ? post.comments : null),
    er: result.engagementRate ?? (at === last ? post.engagementRate : null),
    mul: result.multiple ?? (at === last ? post.multiple : null),
  }));
  const cover = post.thumbnailUrl ?? mediaProxyUrl(post.key, 'thumbnail');
  // a carousel's pictures (its video slides are the phone's to play: here its pictures alone)
  const stills = post.slides.filter((slide) => !slide.video).map((slide) => mediaProxyUrl(post.key, slide.role));
  return {
    key: post.key,
    handle: post.handle,
    face: feeder?.profilePicUrl ?? null,
    kind: KIND[post.mediaType] ?? 'Post',
    day: post.day,
    reads,
    wait: reads.length ? null : waitLine(post.postedAt),
    caption: post.caption ?? '',
    url: post.url,
    slides: stills.length > 1 ? stills : [cover],
    preview: post.previewUrl,
  };
}

// a tile on the window
const onScreen = (el: Element) => {
  const r = el.getBoundingClientRect();
  return r.width > 0 && r.height > 0 && r.bottom > 0 && r.top < window.innerHeight && r.right > 0 && r.left < window.innerWidth;
};

/* The Day's number as a move flies it (flightParts: rankFlight): the whole of it, and its first figure (the column
   showing one, its figure at rest), so the flight lines up the two figures' baselines. None while it isn't shown (a
   post still measuring) */
function dayRank(root: HTMLElement): RankEnd | null {
  const el = root.querySelector<HTMLElement>('.dw-num');
  if (!el || el.style.visibility === 'hidden' || Number(el.style.opacity || 1) < 0.5) return null;
  const column = Array.from(el.querySelectorAll<HTMLElement>('.dw-col')).find((col) => col.getBoundingClientRect().width > 0.5);
  const value = column?.firstElementChild as HTMLElement | null | undefined;
  return value && value.textContent ? { el, value, shift: { x: 0, y: 0 } } : null;
}

/* The layer unseen and untouchable, by its opacity as well: the picture in view sets its own visibility, which shows
   through a hidden parent (it stood over the grid while an open waited for its post's day, and could show a frame before
   a move in) */
function unseen(root: HTMLElement, on: boolean) {
  root.style.visibility = on ? 'hidden' : '';
  root.style.opacity = on ? '0' : '';
  root.style.pointerEvents = on ? 'none' : '';
}

function DayWide({ ref, start, scope, index, feeders, left, page, tileOf, placeTile, onClosed, onFeeder }: DayWideProps) {
  // the layer's own root, on the body (the engine draws into it)
  const rootRef = useRef<HTMLDivElement | null>(null);
  const engineRef = useRef<WideDay | null>(null);
  const [near, setNear] = useState(0);
  const closingRef = useRef(false);
  // the open last made, and on which engine (React's second run of an effect in development makes a new one: opened on
  // the first, the layer it kept was never opened)
  const openedRef = useRef<{ seq: number; engine: WideDay | null }>({ seq: -1, engine: null });
  // when the open now waiting was asked for (its wait counts from then, however many days come in meanwhile)
  const askedRef = useRef<{ seq: number; at: number } | null>(null);
  // the move under way (ended with the layer, however it goes: its grid zoom is held on the page till then)
  const flightRef = useRef<Flight | null>(null);
  const onFeederRef = useRef(onFeeder);
  useLayoutEffect(() => {
    onFeederRef.current = onFeeder;
  });

  /* ── the posts: the days round the one in view, as they load (placed by the index's counts: near enough to pick the
     days, with DAYS_AROUND to spare) ── */
  const startGuess = useMemo(() => {
    if (!start.day) return 0;
    let at = 0;
    for (const entry of index.days) {
      if (entry.day === start.day) return at;
      at += entry.count;
    }
    return 0;
  }, [index, start.day]);
  const centre = Math.max(near, 0) || startGuess;
  const wantDays = daysAround(index, NO_POSTS, centre - DAYS_AROUND, centre + DAYS_AROUND, start.day);
  const live = useDayPosts(scope, wantDays, { loaded: true });
  const insets = useMemo<StreamInsets>(() => ({ top: 0, topCompressed: 0, bottom: 0 }), []);
  const ordered = useMemo(() => {
    // (the Day's order is the layout's: a phone's cards, one a post)
    const layout = computeLayout({ mode: 'today', days: index.days, posts: live, width: 400, height: 800, insets, wide: false });
    const out: WidePost[] = [];
    for (const item of layout.items) {
      const post = item.day && item.slot >= 0 ? live.get(item.day)?.[item.slot] : undefined;
      if (post) out.push(wideOf(post, feeders.get(post.handle) ?? null));
    }
    return out;
  }, [index, live, feeders, insets]);

  /* ── the engine ── */
  useLayoutEffect(() => {
    const root = document.createElement('div');
    rootRef.current = root;
    // (unseen till its move in has laid its first frame: nothing of it shows before the move)
    unseen(root, true);
    document.body.append(root);
    const engine = new WideDay(root, {
      onWho: scope.handle ? null : (handle) => onFeederRef.current?.(handle),
      onRest: () => setNear(engine.index),
      formatRate,
    });
    engineRef.current = engine;
    engine.resize(window.innerWidth, window.innerHeight, left);
    const onResize = () => engine.resize(window.innerWidth, window.innerHeight, left);
    window.addEventListener('resize', onResize);
    return () => {
      window.removeEventListener('resize', onResize);
      endFlight(flightRef.current);
      flightRef.current = null;
      engine.dispose();
      engineRef.current = null;
      rootRef.current = null;
      root.remove();
      // (gone however it went, a window narrowed past it included: the grid it gave way to is the page again)
      const grid = page();
      if (grid) {
        grid.style.opacity = '';
        grid.style.transform = '';
        grid.style.transformOrigin = '';
      }
    };
    // (one engine for the layer's life; its scope never changes under it: a pick closes the Day first)
  }, [left, scope.handle, page]);

  /* A post opened before its day had come in (a slow network, past OPEN_WAIT_MS): the Day opens on what there is, and
     goes to the post once it comes, unless it has been moved off where it opened (before: it stayed on the newest post,
     and closed onto that post's tile, the page jumping to the top) */
  const lateRef = useRef<{ key: string; from: string | null } | null>(null);
  const catchUp = useCallback(() => {
    const engine = engineRef.current;
    const late = lateRef.current;
    if (!engine || !late || engine.busy || closingRef.current) return;
    if (engine.keyAt(engine.index) !== late.from) {
      lateRef.current = null;
      return;
    }
    engine.goTo(late.key);
    if (engine.keyAt(engine.index) === late.key) lateRef.current = null;
  }, []);

  useLayoutEffect(() => {
    engineRef.current?.setPosts(ordered);
    catchUp();
  }, [ordered, catchUp]);

  /* ── in ── */
  useEffect(() => {
    const engine = engineRef.current;
    const root = rootRef.current;
    const opened = () => openedRef.current.seq === start.seq && openedRef.current.engine === engine;
    if (!engine || !root || opened()) return undefined;
    const has = start.key ? ordered.some((post) => post.key === start.key) : ordered.length > 0;
    // (an open under way runs on as days come in: only the layer going ends it)
    const gone = () => engineRef.current !== engine;
    let timer = 0;
    const fadeIn = async (reduce: boolean) => {
      unseen(root, false);
      root.style.opacity = '0';
      engine.busy = true;
      await frames(2);
      const fade = root.animate([{ opacity: 0 }, { opacity: 1 }], { duration: reduce ? 1 : DAY_MOVE_MS * 0.6, easing: 'ease-out', fill: 'both' });
      await fade.finished.catch(() => undefined);
      if (gone()) return;
      root.style.opacity = '';
      fade.cancel();
      const grid = page();
      if (grid) grid.style.opacity = '0';
      engine.busy = false;
      catchUp();
    };
    const open = async () => {
      if (gone() || opened()) return;
      openedRef.current = { seq: start.seq, engine };
      if (start.key) engine.goTo(start.key);
      // (its post not come yet: no flight from its tile with another post's picture, the Day comes in where it is)
      const found = !start.key || engine.keyAt(engine.index) === start.key;
      if (!found && start.key) lateRef.current = { key: start.key, from: engine.keyAt(engine.index) };
      const tile = start.key ? tileOf(start.key) : null;
      const tileFrame = tile?.querySelector<HTMLElement>(':scope > [data-layer="frame"]') ?? null;
      const frame = engine.pictureEl();
      const grid = page();
      const reduce = reducedMotion() || start.instant;
      if (reduce || !found || !tile || !tileFrame || !frame || !grid || !onScreen(tile)) {
        await fadeIn(reduce);
        return;
      }
      engine.busy = true;
      // (a picture still coming in is given a moment: one that has come is drawn as it is)
      const picture = frame.querySelector<HTMLImageElement>('img');
      if (picture && !picture.complete) await Promise.race([picture.decode().catch(() => undefined), new Promise((resolve) => window.setTimeout(resolve, 200))]);
      if (gone()) return;
      engine.flying(true);
      const flight = startFlight(DAY_HOLD_MS);
      flightRef.current = flight;
      const grounds = root.querySelector<HTMLElement>('.dw-grounds');
      const read = root.querySelector<HTMLElement>('.dw-read');
      const pictures = root.querySelector<HTMLElement>('.dw-pics');
      const { wear } = layDayMove(flight, {
        way: 'in',
        layer: root,
        frame,
        pictures: 'img',
        tile,
        tileFrame,
        page: grid,
        rank: dayRank(root),
        ground: { el: grounds ?? root, clear: { opacity: 0 }, whole: { opacity: 1 } },
        words: read ? [read] : [],
        wears: Array.from(frame.querySelectorAll<HTMLElement>(PICTURE_ONLY)),
        wide: pictures ? { pictures } : undefined,
      });
      // (laid at its first frame, held PRIME_FRAMES, then run)
      unseen(root, false);
      await frames(PRIME_FRAMES);
      releaseFlight(flight);
      await flightDone(flight);
      if (gone() || flightRef.current !== flight) return;
      flightRef.current = null;
      // (the grid stays given way under the Day: it comes back as the Day goes)
      grid.style.opacity = '0';
      grid.style.transform = '';
      grid.style.transformOrigin = '';
      endFlight(flight);
      wear?.remove();
      engine.flying(false);
      engine.busy = false;
    };
    if (askedRef.current?.seq !== start.seq) askedRef.current = { seq: start.seq, at: performance.now() };
    if (has) void open();
    else timer = window.setTimeout(() => void open(), Math.max(0, askedRef.current.at + OPEN_WAIT_MS - performance.now()));
    return () => window.clearTimeout(timer);
  }, [ordered, start, tileOf, page, catchUp]);

  /* ── out: onto the post in view's tile ── */
  const close = useCallback(async (then?: () => void) => {
    const engine = engineRef.current;
    const root = rootRef.current;
    if (!engine || !root || closingRef.current) return;
    closingRef.current = true;
    engine.busy = true;
    const key = engine.keyAt(engine.index);
    const grid = page();
    const reduce = reducedMotion();
    // the page put where the tile is, unseen under the Day
    if (key) {
      const to = placeTile(key);
      if (to != null) {
        window.scrollTo(0, to);
        for (let wait = 0; wait < TILE_WAIT_FRAMES && !tileOf(key); wait += 1) await frames(1);
        await frames(2);
      }
    }
    const tile = key ? tileOf(key) : null;
    const tileFrame = tile?.querySelector<HTMLElement>(':scope > [data-layer="frame"]') ?? null;
    const frame = engine.pictureEl();
    // (a move in still under way lands where it was going first)
    endFlight(flightRef.current);
    if (!reduce && tile && tileFrame && frame && grid && onScreen(tile)) {
      engine.flying(true);
      const flight = startFlight(DAY_HOLD_MS);
      flightRef.current = flight;
      const grounds = root.querySelector<HTMLElement>('.dw-grounds');
      const read = root.querySelector<HTMLElement>('.dw-read');
      const pictures = root.querySelector<HTMLElement>('.dw-pics');
      const { wear } = layDayMove(flight, {
        way: 'out',
        layer: root,
        frame,
        pictures: 'img',
        tile,
        tileFrame,
        page: grid,
        rank: dayRank(root),
        ground: { el: grounds ?? root, clear: { opacity: 0 }, whole: { opacity: 1 } },
        words: read ? [read] : [],
        wears: Array.from(frame.querySelectorAll<HTMLElement>(PICTURE_ONLY)),
        wide: pictures ? { pictures } : undefined,
      });
      await frames(PRIME_FRAMES);
      releaseFlight(flight);
      await flightDone(flight);
      if (engineRef.current !== engine) return;
      flightRef.current = null;
      // (the grid as it rests, the tile under the picture as it lands, and the Day taken down in the same frame)
      unseen(root, true);
      grid.style.opacity = '';
      grid.style.transform = '';
      grid.style.transformOrigin = '';
      endFlight(flight);
      wear?.remove();
    } else {
      if (grid) grid.style.opacity = '';
      const fade = root.animate([{ opacity: 1 }, { opacity: 0 }], { duration: reduce ? 1 : DAY_MOVE_MS * 0.6, easing: 'linear', fill: 'both' });
      await fade.finished.catch(() => undefined);
      unseen(root, true);
      fade.cancel();
    }
    onClosed();
    then?.();
  }, [page, placeTile, tileOf, onClosed]);

  useImperativeHandle(ref, () => ({
    close: (then?: () => void) => void close(then),
    toNewest: () => engineRef.current?.to(0),
  }), [close]);

  // (the engine draws the layer itself, in its own root on the body)
  return null;
}

export default DayWide;
