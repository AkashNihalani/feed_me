'use client';

/* ─────────────────────────────────────────────────────────────
   DAY LAYER — a phone's Day view, as a layer over the grid.

   The page never becomes the cards: it stays the grid it was
   (WEEK or MONTH), and Day is a layer of the page laid over the
   screen where the page stands (from under the clock to under
   the toolbar), a scroller of its own, one card a swipe, the
   same cards the page drew before (PostCell: the 5:8 frame,
   slides, previews, the stack as it scrolls).

   Nothing scrolls the page to come or go, so nothing is copied
   and nothing is left unpainted: in, the tile's picture grows to
   its card as the grid zooms out behind it; out, the card's
   picture flies back into its tile as the grid zooms back in
   (swiped far, the page first goes to that tile behind the
   whole-screen card, the layer with it, in one frame). The post's
   number flies with it both ways (flightParts: rankFlight).

   Each move is laid out at its first frame and held there two
   frames before its clock runs (the first paint of a new layer
   would eat its start), and while it runs a swipe is refused: one
   Safari hands to the page would carry the layer away with it.
   The dock folds with the layer's scroll as it does with the
   page's (setScrollSource).
   ───────────────────────────────────────────────────────────── */

import { useCallback, useImperativeHandle, useLayoutEffect, useMemo, useRef, useState, type CSSProperties, type Ref } from 'react';
import { createPortal, flushSync } from 'react-dom';
import PostCell, { PlaceholderCell } from '@/components/feed/stream/PostCell';
import { DAY_HOLD_MS, DAY_MOVE_MS, PRIME_FRAMES, frames, layDayMove, shifted } from '@/components/feed/stream/dayMove';
import { cardPage, rankIn } from '@/components/feed/stream/flightParts';
import { notePageScroll } from '@/components/feed/stream/press';
import type { StreamFeeder, StreamIndex, StreamPost, StreamScope } from '@/lib/feedStream/contract';
import { computeLayout, type StreamInsets } from '@/lib/feedStream/layout';
import { endFlight, flightDone, reducedMotion, releaseFlight, run, startFlight, type Flight } from '@/lib/feedStream/motion';
import { useDayPosts } from '@/lib/feedStream/store';
import { markScrollJump, setScrollSource } from '@/lib/useCompressedOnScroll';

// one move in or out, and how long it is held at its first frame (dayMove)
const MOVE_MS = DAY_MOVE_MS;
const HOLD_MS = DAY_HOLD_MS;
// the cards mounted either side of the one in view (the next one loads before a swipe brings it)
const AROUND = 2;
// the days asked for either side of it, in cards
const DAYS_AROUND = 6;
// the scroll settled this long: the card it rests on is the one in view (it plays)
const SETTLE_MS = 140;
// the page's colours go to the cards' dark this far into the move in, and back this far into the move out
const DARK_IN_AT = 0.4;
const DARK_OUT_AT = 0.5;
/* The layer's own ground (the cards' dark), the one ground of the whole screen through a move: it comes over the
   zooming grid evenly, so nothing of the page (its gaps, three times their size, the strip under the clock) is left
   light round the card, and lifts evenly as the grid zooms back (dayMove). The card's own ground comes in once its
   picture covers most of the screen, and goes before the ground lifts */
const GROUND = 'rgb(20, 20, 22)';
const GROUND_CLEAR = 'rgba(20, 20, 22, 0)';
// the swiped-far way out: how long the page is given to draw the tile it went to
const TILE_WAIT_FRAMES = 24;
/* (a move setting off, said as the page's own moves say it: the dock holds its frost over a range picked from it till
   then, and without it let go only after its fallback wait, so a move from the dock flew under the frost) */
const PAGE_MOVES_EVENT = 'fm:page-moves';
const setOff = () => window.dispatchEvent(new Event(PAGE_MOVES_EVENT));

export type DayStart = { key: string | null; day: string | null; seq: number; instant: boolean; at?: number };

export type DayLayerHandle = {
  // out to the grid, onto the post in view's tile; `then` once landed
  close: (then?: () => void) => void;
  // back to the newest post (a re-tap on the nav)
  toNewest: () => void;
};

type DayLayerProps = {
  ref?: Ref<DayLayerHandle>;
  start: DayStart;
  scope: StreamScope;
  index: StreamIndex;
  feeders: ReadonlyMap<string, StreamFeeder>;
  size: { width: number; height: number; insets: StreamInsets };
  // where the layer is put (the tab's own box, beside the stream: the grid's zoom never carries it)
  host: HTMLElement;
  // the grid under it (zoomed and faded as the layer comes and goes)
  page: () => HTMLElement | null;
  // a post's tile in the grid, when it is drawn
  tileOf: (key: string) => HTMLElement | null;
  // the page's scroll that brings a post's tile on screen (null: it is on screen, or the grid hasn't it)
  placeTile: (key: string) => number | null;
  onClosed: () => void;
  // a card's @handle tapped (not on a feeder's own page)
  onFeeder?: (handle: string) => void;
};

// ?daydebug: the open's and close's timings, on screen
export function note(text: string) {
  if (typeof window === 'undefined' || !window.location.search.includes('daydebug')) return;
  let out = document.getElementById('fm-day-debug');
  if (!out) {
    out = document.createElement('pre');
    out.id = 'fm-day-debug';
    out.style.cssText = 'position:fixed;left:8px;top:120px;z-index:9999;margin:0;padding:6px 8px;border-radius:8px;background:rgba(0,0,0,.75);color:#0f0;font:600 11px/1.35 ui-monospace,monospace;pointer-events:none;white-space:pre';
    document.body.appendChild(out);
  }
  const lines = (out.textContent ?? '').split('\n').filter(Boolean);
  lines.push(`${performance.now().toFixed(0)} ${text}`);
  out.textContent = lines.slice(-14).join('\n');
}

/* A move that hasn't landed well past its time (a frame budget blown, an animation held by something else) is landed
   where it was going: never a screen left half way */
function watch(flight: Flight, label: string) {
  window.setTimeout(() => {
    if (flight.ended) return;
    const late = flight.animations.filter((animation) => animation.playState !== 'finished' && animation.playState !== 'idle');
    if (!late.length) return;
    note(`${label} landed late: ${late.map((animation) => `${animation.playState}:${Math.round(Number(animation.currentTime) || 0)}`).join(' ')}`);
    late.forEach((animation) => {
      try {
        animation.finish();
      } catch {
        animation.cancel();
      }
    });
  }, HOLD_MS + MOVE_MS + 900);
}

// what a card's picture wears that its tile doesn't (its melt into the ground, the bleed at its foot, the shade under the
// clock): in late on the way in, gone early on the way out (dayMove)
const CARD_ONLY = ':scope > .st-card-scrim, :scope > .st-status-shade';

// where an element's top is in the page
const pageTop = (el: HTMLElement) => el.getBoundingClientRect().top + window.scrollY;

/* While a move runs a swipe is refused outright (a non-passive touchmove): Safari gives a swipe made over a grid still
   zooming to the page, not the layer, and the layer, part of the page, slid away with it */
const refuse = (event: TouchEvent) => event.preventDefault();
function still(on: boolean) {
  if (on) document.addEventListener('touchmove', refuse, { passive: false });
  else document.removeEventListener('touchmove', refuse);
}

/* The viewport's height as it is now (100dvh: short of 100lvh by as much as the browser's bars are in), the cards' reading
   cleared of the dock where it stands now (layout: StreamInsets.live). Measured as the layer is drawn, so a move in sets
   off from the reading where it will rest */
const LIVE_PROBE = 'position:fixed;left:0;top:0;width:1px;height:100dvh;visibility:hidden;pointer-events:none';
function liveHeight(): number | undefined {
  if (typeof document === 'undefined') return undefined;
  const probe = document.createElement('div');
  probe.style.cssText = LIVE_PROBE;
  document.body.appendChild(probe);
  const height = probe.getBoundingClientRect().height;
  probe.remove();
  return height || undefined;
}

// the days whose posts the cards from `from` to `to` are (counted from the index, a loaded day by its posts)
export function daysAround(index: StreamIndex, posts: ReadonlyMap<string, StreamPost[]>, from: number, to: number, also: string | null): string[] {
  const days: string[] = [];
  let at = 0;
  for (const entry of index.days) {
    const count = posts.get(entry.day)?.length ?? entry.count;
    if (at + count > from && at <= to) days.push(entry.day);
    at += count;
    if (at > to) break;
  }
  if (also && !days.includes(also)) days.push(also);
  return days;
}

function DayLayer({ ref, start, scope, index, feeders, size, host, page, tileOf, placeTile, onClosed, onFeeder }: DayLayerProps) {
  const over = size.insets.over ?? 0;
  const layerRef = useRef<HTMLDivElement | null>(null);
  const deckRef = useRef<HTMLDivElement | null>(null);
  const postsRef = useRef<ReadonlyMap<string, StreamPost[]>>(new Map());
  // the card the deck is at (rounded, as it scrolls), and the one it rests on (it plays)
  const [near, setNear] = useState<number | null>(null);
  const [active, setActive] = useState<number | null>(null);
  // a move under way (the stack off), and the way in over (the card in view plays from then, on through the way out)
  const [moving, setMoving] = useState(true);
  const [inDone, setInDone] = useState(false);
  // the way out under way (what the cards are laid out from is held, as on the way in)
  const [leaving, setLeaving] = useState(false);
  /* The post a move carries: its card keeps the shape it rests in all through the move (PostCell's `arriving`: a reel's
     frame grown up under the clock, opened and shut by the move's clip). Without it the card flew as its bare 4:5 with
     the stack off, its top under the clock came in one frame after the move in had landed, and went in one frame as the
     move out set off */
  const [moveKey, setMoveKey] = useState<string | null>(start.key);
  const movingRef = useRef(true);
  const nearRef = useRef<number | null>(null);
  const restKeyRef = useRef<string | null>(start.key);
  const settleRef = useRef(0);
  // how far the page was shifted down under the clock as it opened at its very top (and back as it closes)
  const nudgeRef = useRef(0);
  const closingRef = useRef(false);
  const flightRef = useRef<Flight | null>(null);
  // the copy of a tile's shade and ▶ riding the card's frame through a move (tileWear)
  const wearRef = useRef<HTMLElement | null>(null);
  // where the layer stands in the host (the screen where the page stood as it opened)
  const [top] = useState(() => Math.round(window.scrollY - over - pageTop(host)));
  // the scroll the layer last moved the page to itself (its shift at the top, the way out onto a far tile)
  const ownYRef = useRef<number | null>(null);

  /* The page scrolled by anything but the layer (the stream putting you back where you were as it opens, or holding the
     reading line as days come in under it): the layer is part of the page and went with it, so it is put back over the
     screen. Past the top by then, the shift it opened with is let go (the page is unseen under it) */
  useLayoutEffect(() => {
    const pin = () => {
      const layer = layerRef.current;
      if (!layer || closingRef.current) return;
      const y = Math.round(window.scrollY);
      if (ownYRef.current !== y && nudgeRef.current && y >= over) {
        const root = page();
        if (root) root.style.transform = '';
        nudgeRef.current = 0;
      }
      layer.style.top = `${Math.round(y - over - pageTop(host))}px`;
    };
    window.addEventListener('scroll', pin, { passive: true });
    return () => window.removeEventListener('scroll', pin);
  }, [host, over, page]);

  /* ── the cards ── */

  // the days wanted: round the card in view (where it opened, before the deck has moved)
  const startGuess = useMemo(() => {
    if (!start.day) return 0;
    let at = 0;
    for (const entry of index.days) {
      if (entry.day === start.day) return at;
      at += postsRef.current.get(entry.day)?.length ?? entry.count;
    }
    return 0;
  }, [index, start.day]);
  const centre = near ?? startGuess;
  const wantDays = daysAround(index, postsRef.current, centre - DAYS_AROUND, centre + DAYS_AROUND, start.day);
  const livePosts = useDayPosts(scope, wantDays, { loaded: true });
  postsRef.current = livePosts;
  /* Through a move the cards are laid out from what they were as it began: a day coming in mid-flight (its posts not as
     many as its index said) would move the card in flight to another place in the list. What came in meanwhile is let
     in once it has landed, the deck kept on its post (below) */
  const heldRef = useRef(livePosts);
  const holding = !inDone || leaving;
  if (!holding) heldRef.current = livePosts;
  const posts = holding ? heldRef.current : livePosts;

  // (the bars coming in or going while it is up: Safari's toolbar tapped out, a phone turned)
  const [live, setLive] = useState(liveHeight);
  useLayoutEffect(() => {
    const probe = document.createElement('div');
    probe.setAttribute('aria-hidden', 'true');
    probe.style.cssText = LIVE_PROBE;
    document.body.appendChild(probe);
    const read = () => {
      const now = probe.getBoundingClientRect().height;
      if (now) setLive((was) => (was != null && Math.abs(was - now) < 0.5 ? was : now));
    };
    const observer = typeof ResizeObserver === 'undefined' ? null : new ResizeObserver(read);
    observer?.observe(probe);
    window.addEventListener('resize', read);
    return () => {
      observer?.disconnect();
      window.removeEventListener('resize', read);
      probe.remove();
    };
  }, []);
  const insets = useMemo(() => (live ? { ...size.insets, live } : size.insets), [size.insets, live]);
  const layout = useMemo(
    () => computeLayout({ mode: 'today', days: index.days, posts, width: size.width, height: size.height, insets, wide: false }),
    [index, posts, size.width, size.height, insets],
  );
  const layoutRef = useRef(layout);
  layoutRef.current = layout;
  const stride = layout.stride;
  const head = layout.head;
  const height = layout.head + layout.cardHeight;
  const count = layout.items.length;
  const startAt = useMemo(() => (start.key ? layout.byPost.get(start.key) ?? 0 : 0), [layout, start.key]);
  const at = near ?? startAt;

  /* ── the scroll ── */

  const settle = useCallback(() => {
    const deck = deckRef.current;
    const now = layoutRef.current;
    if (!deck || movingRef.current) return;
    const rest = Math.max(0, Math.min(now.items.length - 1, Math.round(deck.scrollTop / Math.max(now.stride, 1))));
    restKeyRef.current = now.items[rest]?.postKey ?? restKeyRef.current;
    setActive(rest);
  }, []);

  const onScroll = useCallback(() => {
    const deck = deckRef.current;
    if (!deck) return;
    notePageScroll();
    const next = Math.max(0, Math.min(count - 1, Math.round(deck.scrollTop / Math.max(stride, 1))));
    if (next !== nearRef.current) {
      nearRef.current = next;
      setNear(next);
    }
    window.clearTimeout(settleRef.current);
    settleRef.current = window.setTimeout(settle, SETTLE_MS);
  }, [count, settle, stride]);

  // a day loading under the deck can move the card in view in the list (its posts not as many as its index said): the
  // deck keeps that card, not its number
  useLayoutEffect(() => {
    const deck = deckRef.current;
    const key = restKeyRef.current;
    if (!deck || !key || movingRef.current) return;
    const place = layout.byPost.get(key);
    if (place == null) return;
    const now = Math.round(deck.scrollTop / Math.max(stride, 1));
    if (place !== now && Math.abs(deck.scrollTop - now * stride) < 2) {
      deck.scrollTop = place * stride;
      nearRef.current = place;
      setNear(place);
      setActive(place);
    }
  }, [layout, stride, inDone]);

  /* ── in ── */

  const opened = useRef(-1);
  useLayoutEffect(() => {
    if (opened.current === start.seq) return;
    opened.current = start.seq;
    note(`open effect (+${start.at != null ? (performance.now() - start.at).toFixed(0) : '?'} from tap)`);
    const deck = deckRef.current;
    const layer = layerRef.current;
    const root = page();
    if (!deck || !layer) return;
    deck.scrollTop = startAt * stride;
    nearRef.current = startAt;
    restKeyRef.current = start.key ?? layout.items[startAt]?.postKey ?? null;
    still(true);
    setScrollSource(deck);
    if (root) root.style.pointerEvents = 'none';
    // at the page's very top the clock's strip is above the page: the page goes down under it, held where it is by a
    // shift (nothing moves on screen), so the card can reach up under the clock; back as Day closes
    const nudge = root ? Math.max(0, Math.round(over - window.scrollY)) : 0;
    if (root && nudge) {
      markScrollJump('keep', 400);
      ownYRef.current = Math.round(window.scrollY + nudge);
      window.scrollTo(0, window.scrollY + nudge);
      root.style.transform = shifted(nudge);
      nudgeRef.current = nudge;
      layer.style.top = `${Math.round(window.scrollY - over - pageTop(host))}px`;
    }
    const landed = () => {
      movingRef.current = false;
      setMoving(false);
      setInDone(true);
      setActive(startAt);
      still(false);
    };
    const key = restKeyRef.current;
    const tile = key ? tileOf(key) : null;
    const tileFrame = tile?.querySelector<HTMLElement>(':scope > [data-layer="frame"]') ?? null;
    note(`key ${key ? key.slice(0, 10) : 'none'} startAt ${startAt} tile ${tile ? 'yes' : 'no'} frame ${tileFrame ? 'yes' : 'no'} instant ${start.instant}`);
    if (start.instant || reducedMotion() || !tileFrame || !root) {
      cardPage(true);
      layer.style.backgroundColor = GROUND;
      if (root) root.style.opacity = '0';
      landed();
      return;
    }
    // (unseen but there: a touch in these first frames lands on the layer, never on the page)
    layer.style.opacity = '0';
    void (async () => {
      const cell = deck.querySelector<HTMLElement>(`[data-cell="${CSS.escape(`p:${key}`)}"]`);
      const frame = cell?.querySelector<HTMLElement>(':scope > [data-layer="frame"]') ?? null;
      const picture = frame?.querySelector<HTMLImageElement>('img.st-img') ?? frame?.querySelector<HTMLImageElement>('img') ?? null;
      // (a picture still coming in is given a moment: one that has come is drawn as it is)
      if (picture && !picture.complete) await Promise.race([picture.decode().catch(() => undefined), new Promise((resolve) => window.setTimeout(resolve, 200))]);
      note(`picture ${picture ? (picture.complete ? 'ready' : 'late') : 'none'}`);
      note(`cell ${cell ? 'yes' : 'no'} frame ${frame ? 'yes' : 'no'} cards ${deck.querySelectorAll('[data-cell]').length}`);
      if (!cell || !frame) {
        layer.style.opacity = '';
        cardPage(true);
        layer.style.backgroundColor = GROUND;
        root.style.opacity = '0';
        landed();
        return;
      }
      const nudge = nudgeRef.current;
      const flight = startFlight(HOLD_MS);
      flightRef.current = flight;
      wearRef.current?.remove();
      // (from the tile's picture just as the tile draws it, cut to the tile's box, wearing what the tile wears)
      const bg = cell.querySelector<HTMLElement>(':scope > [data-layer="bg"]');
      const { wear } = layDayMove(flight, {
        way: 'in',
        layer,
        frame,
        tile: tile ?? tileFrame,
        tileFrame,
        page: root,
        shift: nudge,
        rank: rankIn(cell, 'chrome'),
        ground: { el: layer, clear: { backgroundColor: GROUND_CLEAR }, whole: { backgroundColor: GROUND } },
        words: Array.from(cell.querySelectorAll<HTMLElement>(':scope > :is([data-layer="chrome"], [data-layer="reading"])')),
        wears: Array.from(frame.querySelectorAll<HTMLElement>(CARD_ONLY)),
        late: bg ? [bg] : [],
      });
      wearRef.current = wear;
      layer.style.opacity = '';
      note('laid out');
      await frames(PRIME_FRAMES);
      releaseFlight(flight);
      setOff();
      watch(flight, 'open');
      note('released');
      window.setTimeout(() => {
        if (!closingRef.current) cardPage(true);
      }, MOVE_MS * DARK_IN_AT);
      await flightDone(flight);
      if (flightRef.current !== flight) return;
      // the grid stays unseen under the cards (shifted as it was, if it was), the ground theirs
      root.style.opacity = '0';
      layer.style.backgroundColor = GROUND;
      root.style.transform = nudge ? shifted(nudge) : '';
      endFlight(flight);
      flightRef.current = null;
      if (wearRef.current === wear) {
        wear?.remove();
        wearRef.current = null;
      }
      landed();
    })();
    // (once per opening)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [start.seq]);

  /* ── out ── */

  const close = useCallback((then?: () => void) => {
    const deck = deckRef.current;
    const layer = layerRef.current;
    const root = page();
    if (!deck || !layer || closingRef.current) return;
    closingRef.current = true;
    note('close asked');
    window.clearTimeout(settleRef.current);
    // (a move in still under way lands where it was going first)
    if (flightRef.current) {
      endFlight(flightRef.current);
      flightRef.current = null;
    }
    still(true);
    movingRef.current = true;
    const rest = Math.max(0, Math.min(count - 1, Math.round(deck.scrollTop / Math.max(stride, 1))));
    const key = layout.items[rest]?.postKey ?? null;
    // (the stack off for the move, the card it carries kept in its rest shape: nothing of it changes as it sets off)
    flushSync(() => {
      setMoving(true);
      setLeaving(true);
      setMoveKey(key);
    });
    const done = () => {
      layer.style.visibility = 'hidden';
      if (root) {
        root.style.opacity = '';
        root.style.pointerEvents = '';
        root.style.transformOrigin = '';
        root.style.transform = '';
      }
      const nudge = nudgeRef.current;
      nudgeRef.current = 0;
      if (nudge) {
        markScrollJump('keep', 400);
        window.scrollTo(0, Math.max(0, window.scrollY - nudge));
      }
      cardPage(false);
      setScrollSource(null);
      still(false);
      onClosed();
      then?.();
    };
    if (!root || !key || reducedMotion()) {
      done();
      return;
    }
    void (async () => {
      // swiped far: the page goes to the post's tile behind the whole-screen card, the layer with it, in one frame
      const goal = placeTile(key);
      if (goal != null) {
        nudgeRef.current = 0;
        root.style.transform = '';
        markScrollJump('keep', 600);
        const hostTop = pageTop(host);
        layer.style.top = `${Math.round(goal - over - hostTop)}px`;
        ownYRef.current = Math.round(goal);
        window.scrollTo(0, goal);
        for (let wait = 0; wait < TILE_WAIT_FRAMES && !tileOf(key); wait += 1) await frames(1);
        await frames(2);
      }
      const tile = tileOf(key);
      const tileFrame = tile?.querySelector<HTMLElement>(':scope > [data-layer="frame"]') ?? null;
      const cell = deck.querySelector<HTMLElement>(`[data-cell="${CSS.escape(`p:${key}`)}"]`);
      const frame = cell?.querySelector<HTMLElement>(':scope > [data-layer="frame"]') ?? null;
      const nudge = nudgeRef.current;
      const flight = startFlight(HOLD_MS);
      flightRef.current = flight;
      if (!tile || !tileFrame || !cell || !frame) {
        // (no tile to land on: the cards dissolve over the grid coming back)
        run(flight, layer, [{ opacity: 1 }, { opacity: 0 }], { duration: MOVE_MS * 0.6, easing: 'linear', fill: 'both' });
        run(flight, root, [{ opacity: 0 }, { opacity: 1 }], { duration: MOVE_MS * 0.6, easing: 'linear', fill: 'both' });
      } else {
        // (onto the tile's picture just as the tile draws it, cut to the tile's box, wearing what the tile wears: the
        // tile takes over from it with nothing moving and nothing coming in; the cards that aren't it go at once)
        wearRef.current?.remove();
        const bg = cell.querySelector<HTMLElement>(':scope > [data-layer="bg"]');
        const { wear } = layDayMove(flight, {
          way: 'out',
          layer,
          frame,
          tile,
          tileFrame,
          page: root,
          shift: nudge,
          rank: rankIn(cell, 'chrome'),
          ground: { el: layer, clear: { backgroundColor: GROUND_CLEAR }, whole: { backgroundColor: GROUND } },
          words: Array.from(cell.querySelectorAll<HTMLElement>(':scope > :is([data-layer="chrome"], [data-layer="reading"])')),
          wears: Array.from(frame.querySelectorAll<HTMLElement>(CARD_ONLY)),
          late: bg ? [bg] : [],
          others: Array.from(deck.querySelectorAll<HTMLElement>(':scope > div > [data-cell]')).filter((other) => other !== cell),
        });
        wearRef.current = wear;
      }
      note('close laid out');
      await frames(PRIME_FRAMES);
      releaseFlight(flight);
      setOff();
      watch(flight, 'close');
      note('close released');
      window.setTimeout(() => cardPage(false), MOVE_MS * DARK_OUT_AT);
      await flightDone(flight);
      note('close landed');
      layer.style.visibility = 'hidden';
      endFlight(flight);
      flightRef.current = null;
      done();
    })();
  }, [count, host, layout, onClosed, over, page, placeTile, stride, tileOf]);

  const toNewest = useCallback(() => {
    const deck = deckRef.current;
    if (!deck || movingRef.current) return;
    deck.scrollTo({ top: 0, behavior: reducedMotion() ? 'auto' : 'smooth' });
  }, []);

  useImperativeHandle(ref, () => ({ close, toNewest }), [close, toNewest]);

  // a card that isn't the one in view (a tap on it): the deck brings it there (true: it is the one in view, the tap
  // goes on to read it)
  const focusCard = useCallback((post: StreamPost, gliding = false) => {
    const deck = deckRef.current;
    const place = layout.byPost.get(post.key);
    if (!deck || place == null || movingRef.current) return false;
    const now = deck.scrollTop;
    const rest = place * stride;
    const inView = gliding || Math.round(now / Math.max(stride, 1)) === place;
    if (Math.abs(rest - now) > 2) deck.scrollTo({ top: rest, behavior: reducedMotion() ? 'auto' : 'smooth' });
    return inView;
  }, [layout, stride]);
  const openTile = useCallback(() => undefined, []);

  /* (gone with the layer: the page's own scroll and colours again, a swipe free, and a move still under way ended, so
     nothing of it lands on the page after: a window widened or narrowed past a phone's mid-move left the grid unseen) */
  useLayoutEffect(() => () => {
    endFlight(flightRef.current);
    flightRef.current = null;
    window.clearTimeout(settleRef.current);
    setScrollSource(null);
    still(false);
  }, []);

  /* ── drawn ── */

  const cards = [];
  // (opening, the card alone: what the tap waits on is drawn first, the ones either side once it has landed)
  const around = inDone ? AROUND : 0;
  const from = Math.max(0, at - around);
  const to = Math.min(count, at + around + 1);
  for (let place = from; place < to; place += 1) {
    const item = layout.items[place];
    const y = head + place * stride;
    const post = item.day ? posts.get(item.day)?.[item.slot] ?? null : null;
    if (!post) {
      cards.push(
        <PlaceholderCell key={`x:${item.day}:${item.slot}`} cellKey={`x:${item.day}:${item.slot}`} mode="today" x={item.x} y={y} w={item.w} h={item.h} mediaWidth={0} wide={false} lift={layout.lift} />,
      );
      continue;
    }
    // (the card a move carries, in its rest shape through the move: see moveKey)
    const carried = moving && moveKey != null && post.key === moveKey;
    cards.push(
      <PostCell
        key={`p:${post.key}`}
        cellKey={`p:${post.key}`}
        zid={`${item.day}:${item.slot}`}
        post={post}
        mode="today"
        x={item.x}
        y={y}
        w={item.w}
        h={item.h}
        cols={1}
        rows={1}
        mediaWidth={0}
        wide={false}
        leaving={carried ? 'week' : null}
        leavingShape={null}
        focal={carried}
        active={inDone && active === place}
        lit={false}
        spot={false}
        standout={false}
        date={post.day || null}
        eager
        feeder={feeders.get(post.handle) ?? null}
        solo={Boolean(scope.handle)}
        depth={!moving}
        order={place}
        clear={layout.footClear}
        head={layout.head}
        lift={layout.lift}
        rise={layout.rise}
        onOpenTile={openTile}
        onFocusCard={focusCard}
        onFeeder={onFeeder}
      />,
    );
  }
  // one snap point a card, where it rests (its picture up to the screen's top)
  const marks = [];
  for (let place = 0; place < count; place += 1) marks.push(<span key={place} className="st-day-mark" style={{ top: place * stride }} />);

  const vars = { '--st-rest': `${head}px`, '--st-stride': `${stride}px` } as CSSProperties;
  return createPortal(
    <div ref={layerRef} className="fm-stream st-day-layer" data-day-layer="" style={{ top, height }}>
      <div ref={deckRef} className="st-day-deck" onScroll={onScroll}>
        <div className="relative w-full" style={{ height: Math.max(height, (count - 1) * stride + height), ...vars }}>
          {marks}
          {cards}
        </div>
      </div>
    </div>,
    host,
  );
}

export default DayLayer;
