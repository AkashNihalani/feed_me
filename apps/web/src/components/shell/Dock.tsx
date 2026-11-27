'use client';

/* ─────────────────────────────────────────────────────────────
   DOCK — the phone's one piece of chrome for Feed, Lead and Read.
   Its hero is who you're on, the rail's own circle (a feeder's
   face, a feed's badge, or Feed Me's wedge for all of them), then
   a line with where they sit and the tab's choice (Feed's zoom,
   Lead's range, Read's part), and the tab's icon in a small disc.
   On a phone it is all of the tabs' chrome: there is no top
   section, so the page keeps the whole screen. Scrolled down, the
   bar folds into its handle, the hero alone in the middle, a size
   up; scrolled back up (or at the page's top or foot) it opens
   again, in step with Safari's own toolbar. The handle takes the
   same swipes, pulls and taps as the bar.

   Swipe it sideways for the next tab: the hero's ring dims to a
   track and is drawn round again in the accent as the thumb goes,
   the bar presses in, the page, a card now, eases back a touch
   toward the thumb's side (lib/tabPush; it was copied the moment
   the finger landed, so the swipe itself has nothing heavy to do),
   and over the dock, clear of the thumb, a pill comes up with the
   three tabs, its one red mark holding to the tab you're on, then
   going to the one the thumb heads for: all on the one progress,
   the ring's. Past halfway, or the ring drawn all the way round,
   is the switch: the hero takes it with a press, the icon turns
   into the next tab's, the card lifts on while the next tab is
   made under it, then slides away, uncovering it, the pill goes
   as it does, and the ring turns back to ink. Let go before, and
   it all settles back.

   Pull it down for the tab's choice (Lead's range, Feed's zoom,
   Read's part): the same pill with the choices in it, opened on
   the one you're on; slide along it and let go (dockPull).

   Pull it up, or tap it, and the page frosts over while the
   dock's parts rise out of it as layers (a tap's at the pace they
   sink back at): the choice spreads out of
   its line, in its place; the hero flies up into a row of the
   rail's circles, the others coming out from behind it, the
   nearer first; the tab words rise over them. Slide onto one and
   let go, or let go first and tap. A feeder picked has the rail's
   ring drawn round it before the layers sink back with it; a feed
   picked deals its feeders out of it, in place.

   The circles are the header rail's own (TabHeader: Plate,
   StoryRing, RailFace, RailLabel), so they look and answer the
   same. What moves every frame (--p, the circles' places, the
   ring, the mark) is written straight onto elements that are
   layers of their own, never through React. Nothing fixed here
   touches the screen's edges, and the frost lies in the page
   (dock.css): iOS Safari tints its bars from fixed boxes at the
   edges, and would paint them solid.
   ───────────────────────────────────────────────────────────── */

import { useCallback, useEffect, useLayoutEffect, useRef, useState, type CSSProperties, type MouseEvent as ReactMouseEvent, type PointerEvent as ReactPointerEvent } from 'react';
import { useReducedMotion } from 'framer-motion';
import { BookOpenText, LayoutGrid, Trophy } from 'lucide-react';
import { press } from '@/components/feed/stream/press';
import { FAN_MS } from '@/components/motion/Ledger';
import { Plate, RailFace, RailLabel, StoryRing, useShownHeader, useWideChrome, whoOf, type HeaderFeed, type HeaderFeeder, type RailFaceOf, type TabHeaderConfig } from '@/components/shell/TabHeader';
import { titleCase } from '@/lib/feedLabels';
import { CARRY_EASE_CSS, carryCurve, evenCurve } from '@/lib/feedStream/motion';
import { useFreshHandles } from '@/lib/feedStream/fresh';
import { playHaptic } from '@/lib/haptics';
import { dropPush, holdPush, leavePush, releasePush, scrubPush } from '@/lib/tabPush';
import { openMe } from '@/lib/meDoor';
import { leaveTab } from '@/lib/tabSheet';
import { ms } from '@/lib/tempo';
import { currentScrollSource, liveScrollJump, onScrollSource } from '@/lib/useCompressedOnScroll';
import { DockPull, Glide, PRESS, coolRing, pillAt, pillShown, posePill, springBar, type PullDom } from './dockPull';
import { DockMe } from './MeLine';
import './dock.css';
import './dockPull.css';

export type DockTab = 'lead' | 'feed' | 'read';

// the three tabs in the swipe's order
const TABS: readonly DockTab[] = ['lead', 'feed', 'read'];
const HREF: Record<DockTab, string> = { lead: '/lead', feed: '/', read: '/read' };
const NAME: Record<DockTab, string> = { lead: 'Lead', feed: 'Feed', read: 'Read' };
const ICON = { lead: Trophy, feed: LayoutGrid, read: BookOpenText } as const;

// a pull this long (pt) opens the layers all the way; then they stay open under the finger for the rest of the drag
const PULL = 70;
// one circle to the next along the row (the rail's: a 70pt circle and its gap); the tab names over it stand as far apart,
// so the first three circles each sit under one
const STEP = 76;
// the row starts under the first tab (Lead), a step left of the middle: its middle never comes before the second circle.
// A pick further along stands in the middle
const firstTh = (count: number) => (count > 1 ? 1 : 0);
// the hero: the rail's 64pt plate, drawn at the bar's 56
const HERO_SCALE = 56 / 64;
// a swipe this long (pt) along the dock draws the ring all the way round, and that is the switch (as is a flick)
const RING = 96;
const FLICK = 0.45;
// the ring follows the thumb a breath behind (s): eased once a frame, it draws smooth however the touches come in
const RING_LAG = 0.05;
// the ring closed: the next tab held lit over the dock until the old page is gone (the push's clock), then let go
const HUD_OUT_MS = ms(300);
// the layers rising or sinking on their own (a tap, a pick, a scroll): the same time up as down
const LAYERS_MS = ms(440);
/* a pull: the layers follow the finger a breath behind (`PULL_K`, a critically damped spring's), never faster than
   `PULL_V` (the whole way per s) and gathering that pace at `PULL_A`: a slow pull holds them, a flick sends them up at
   about the pace a tap does, never in a frame */
const PULL_K = 620;
const PULL_V = 1.95;
const PULL_A = 14;
// let go past this much of the pull, they open
const PULL_TAKES = 0.32;
// scrolled down this far one way (pt), the bar folds into its handle; back up as far, or at the page's top or foot, it
// opens again (Safari's toolbar does the same, so the two move together)
const FOLD_TRAVEL = 36;
const FOLD_MS = ms(420);
// a face, a chip or a word must have been under the finger this long to be taken on letting go: a flick past never is
const REST_MS = 70;
// a feeder picked: its ring drawn round before the layers sink back with it
const PICK_HOLD_MS = ms(420);
// a tab or a choice picked: the layers sink first, then the page moves (the page's work never holds them up)
const APPLY_MS = ms(280);
/* A choice picked (a time frame): the frost stays whole while the layers sink and the page makes ready, and lifts as the
   page sets off (FeedStream says so: PAGE_MOVES_EVENT), over the move's first part. Safari takes the look of its bars from
   what is under them: let go before the move, the old view came back under them for a moment, and they turned to it and
   then to the new one, two re-tints in a second. Held, they go from the frost to the new view once. A page that never
   says (no move, a tab with no such event) lets it go after FROST_WAIT_MS */
const PAGE_MOVES_EVENT = 'fm:page-moves';
// ...and as the page jumps under a still copy before it moves (Feed out of a Day card), with how far: the frost, which
// lies in the page, goes with the screen
const PAGE_JUMP_EVENT = 'fm:page-jump';
const FROST_LIFT_MS = ms(420);
const FROST_WAIT_MS = 1600;
// circles dealt out of a feed: the rail's fan clock, a beat between one and the next
const DEAL_STEP_MS = ms(40);

// a circle on the rail, in the rail's own order: the wedge (All feeds, or back to them), a feed's badge, a feeder
type Face = { key: string; label: string; litLabel: string; art: RailFaceOf; feed?: HeaderFeed; feeder?: HeaderFeeder };

function railOf(config: TabHeaderConfig): Face[] {
  const feeder = (one: HeaderFeeder): Face => ({ key: `feeder:${one.handle}`, label: one.handle, litLabel: `@${one.handle}`, art: { kind: 'feeder', feeder: one }, feeder: one });
  if (!config.feeds) return (config.pendingFeeders ?? []).map(feeder);
  const { activeFeed } = whoOf(config.feeds, config.feedId, config.handle);
  const badge = (feed: HeaderFeed): Face => ({ key: `feed:${feed.id}`, label: titleCase(feed.title), litLabel: titleCase(feed.title), art: { kind: 'feed', feed }, feed });
  // (one wedge either way: opening a feed turns it to point back, as the header's does)
  const wedgeLabel = activeFeed ? 'BACK' : 'FEEDS';
  const wedge: Face = { key: 'all', label: wedgeLabel, litLabel: wedgeLabel, art: { kind: 'all', back: Boolean(activeFeed) } };
  return activeFeed ? [wedge, badge(activeFeed), ...activeFeed.feeders.map(feeder)] : [wedge, ...config.feeds.map(badge)];
}

const useIsoLayoutEffect = typeof window !== 'undefined' ? useLayoutEffect : useEffect;
const reducedMotion = () => typeof window !== 'undefined' && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
const clamp = (value: number, low: number, high: number) => Math.min(high, Math.max(low, value));
// the tabs' mark holds to its stop, then goes, and is nearly home before the ring is closed (the pull's own feel)
const magnet = (t: number) => t ** 3 / (t ** 3 + (1 - t) ** 3);

// (`ring`: how far round a sideways swipe has drawn the hero's ring, 0 → 1; `raw`: how much of a pull up the finger has made)
type Gesture = { id: number; x0: number; y0: number; mode: null | 'pull' | 'swipe' | 'drop'; wasOpen: boolean; locked: boolean; lastX: number; lastT: number; v: number; ring: number; raw: number };
type RingParts = { right: HTMLElement | null; left: HTMLElement | null; lead: HTMLElement | null; start: HTMLElement | null; end: HTMLElement | null };

// the swipe's marks: the dock plain again with none of them
const RING_KEYS = ['swiping', 'ringDir', 'ringTo', 'ringShut'] as const;
function clearRing(bar: HTMLElement) {
  RING_KEYS.forEach((key) => delete bar.dataset[key]);
}

/* `me`: Me is on screen, over `tab` (the one it was opened from, the way back): the dock says so, its disc is the way
   back, and no tab is the one you're on */
export default function Dock({ tab, me = false }: { tab: DockTab; me?: boolean }) {
  const shown = useShownHeader(me ? 'me' : tab);
  const wide = useWideChrome();
  const reduce = Boolean(useReducedMotion());
  const fresh = useFreshHandles();
  const config = shown?.config ?? null;
  const [open, setOpen] = useState(false);

  const rootRef = useRef<HTMLDivElement | null>(null);
  const frostRef = useRef<HTMLDivElement | null>(null);
  // the frost kept whole through a choice's move (the layers' --p no longer draws it): how it is let go
  const frostHold = useRef<{ lift: Animation | null; stop: () => void } | null>(null);
  /* Raised over a phone's Day card, the layers and their frost are the cards' dark in either theme (dock.css
     [data-on-card]): the card is dark in both, and the light theme's frost washed it out, Safari's toolbar turning
     light over it and back as a pick lifted it. Decided as they start to rise, kept till the frost is gone (a pick's
     move takes the page off the card meanwhile) */
  const cardLayers = useRef(false);
  const markCard = useCallback((on: boolean) => {
    if (cardLayers.current === on) return;
    cardLayers.current = on;
    [rootRef.current, frostRef.current].forEach((el) => {
      if (!el) return;
      if (on) el.dataset.onCard = '';
      else delete el.dataset.onCard;
    });
  }, []);
  const barRef = useRef<HTMLButtonElement | null>(null);
  const faceRef = useRef<HTMLSpanElement | null>(null);
  // the bar's glass, the handle's own (folded), and what the bar carries beside the handle (the line, the tab's disc)
  const glassRef = useRef<HTMLSpanElement | null>(null);
  const padRef = useRef<HTMLSpanElement | null>(null);
  const restRef = useRef<HTMLSpanElement | null>(null);
  const rowRef = useRef<HTMLDivElement | null>(null);
  const trackRef = useRef<HTMLDivElement | null>(null);
  const hudRef = useRef<HTMLDivElement | null>(null);
  // the tabs' pill: its mark, the bright words the mark shows, and where each is on its way (dockPull's springs)
  const tabMarkRef = useRef<HTMLDivElement | null>(null);
  const tabBrightRef = useRef<HTMLDivElement | null>(null);
  const tabPill = useRef({ rise: new Glide(0), mark: new Glide(TABS.indexOf(tab)), seg: 64 });
  const ring = useRef<RingParts>({ right: null, left: null, lead: null, start: null, end: null });

  // what moves every frame, kept out of React
  const motion = useRef({ p: 0, raf: 0 });
  // the layers on their way to the finger (a pull): where it has them, and the pace they go at
  const follow = useRef({ to: 0, v: 0, raf: 0, last: 0, at: null as [number, number, number] | null });
  const row = useRef({ th: 0, tv: 0, target: null as number | null, raf: 0, last: 0, edge: 0, fly: -1, flyX: 0, flyY: 0 });
  // where each circle stood last drawn (by key): what stays when the rail is dealt again glides on from there
  const lastX = useRef(new Map<string, number>());
  const gesture = useRef<Gesture | null>(null);
  const hot = useRef<{ key: string | null; since: number; at: [number, number] | null }>({ key: null, since: 0, at: null });
  const openAt = useRef(0);
  // where and when the tap that opened the layers let go: iOS sends that tap's click a beat later, to whatever is under
  // the finger by then (folded, the chips still gathered in the middle, under the handle), and it is no pick
  const openTap = useRef<{ x: number; y: number; t: number } | null>(null);
  const isOpenTap = (event: ReactMouseEvent<HTMLElement>) => {
    const tap = openTap.current;
    if (!tap) return false;
    openTap.current = null;
    return event.timeStamp - tap.t < 600 && Math.hypot(event.clientX - tap.x, event.clientY - tap.y) < 24;
  };
  /* the swipe: how far round the ring is drawn (eased toward the thumb's `target`), which way, from which tab to which
     (none past the ends: the thumb then only `nudge`s the mark), and whether the thumb is still on it */
  const swipe = useRef({ shown: 0, target: 0, way: 1 as 1 | -1, from: 'feed' as DockTab, to: null as DockTab | null, nudge: 0, held: false, raf: 0, last: 0 });
  const laterTimer = useRef(0);
  // the tab you're on, for what settles after a switch has already changed it
  const tabRef = useRef(tab);
  // the pull down for the tab's choice (dockPull), its parts, and the choice each tab was last on
  const pull = useRef<DockPull | null>(null);
  const pillRef = useRef<HTMLDivElement | null>(null);
  const pillMarkRef = useRef<HTMLDivElement | null>(null);
  const pillBrightRef = useRef<HTMLDivElement | null>(null);
  const chosen = useRef<Partial<Record<DockTab, { now: string | number; last: string | number | null }>>>({});
  // folded into the handle (the page scrolled down), and how far the page has gone one way since it last turned
  const foldedRef = useRef(false);
  const scrolled = useRef({ y: 0, travel: 0 });

  const faces = config ? railOf(config) : [];
  const { activeFeed, pickedFeeder, pickKey } = whoOf(config?.feeds ?? null, config?.feedId, config?.handle ?? null);
  const pickIndex = faces.findIndex((face) => face.key === pickKey);
  const choice = config?.choice ?? null;
  const back = config?.back ?? null;
  const here = TABS.indexOf(tab);
  // (a flick goes back to the one before: told as the choice changes, however it was changed; a pull tells it once, as
  // it ends, not each stop it passed)
  const choiceNow = useRef(choice);
  useIsoLayoutEffect(() => {
    choiceNow.current = choice;
  });
  const choiceValue = choice?.value ?? null;
  useEffect(() => {
    if (choiceValue == null || pull.current?.active) return;
    const was = chosen.current[tab];
    if (!was) chosen.current[tab] = { now: choiceValue, last: null };
    else if (was.now !== choiceValue) chosen.current[tab] = { now: choiceValue, last: was.now };
  }, [tab, choiceValue]);
  const isMarked = (handle: string) => (config?.marked ? config.marked(handle) : fresh.has(handle));

  /* ── the row of circles: each in its place along the row, the one you're on in the middle. Opening, it flies up out
     of the hero, and the others come out from behind it, the nearer first; closing, they fold back into it and it flies
     home. The row's ends fade where the feed runs on ── */
  const drawFaces = useCallback(() => {
    const track = trackRef.current;
    if (!track) return;
    const p = motion.current.p;
    const { th, fly, flyX, flyY } = row.current;
    const half = (track.parentElement?.clientWidth ?? 360) / 2;
    track.querySelectorAll<HTMLElement>('[data-i]').forEach((el) => {
      const i = Number(el.dataset.i);
      const x = (i - th) * STEP;
      lastX.current.set(el.dataset.key ?? '', x);
      if (i === fly && p < 1) {
        // (home before the spring's long tail: it lands, then sits still until the hero takes over)
        const e = carryCurve(clamp((p - 0.04) / 0.56, 0, 1));
        el.style.transform = `translate3d(${(flyX + (x - flyX) * e).toFixed(1)}px, ${(flyY * (1 - e)).toFixed(1)}px, 0) scale(${(HERO_SCALE + (1 - HERO_SCALE) * e).toFixed(4)})`;
        el.style.opacity = p > 0.0005 ? '1' : '0';
        el.style.setProperty('--name', clamp((e - 0.8) / 0.2, 0, 1).toFixed(3));
        delete el.dataset.gone;
        return;
      }
      const q = carryCurve(clamp((p - 0.28 - 0.06 * Math.min(4, Math.abs(i - th))) / 0.5, 0, 1));
      const shown = q * clamp((half - Math.abs(x) + 6) / 44, 0, 1);
      if (shown < 0.002) {
        el.style.opacity = '0';
        el.dataset.gone = '';
        return;
      }
      delete el.dataset.gone;
      el.style.transform = `translate3d(${(x * (0.35 + 0.65 * q)).toFixed(1)}px, ${((1 - q) * 10).toFixed(1)}px, 0) scale(${(0.8 + 0.2 * q).toFixed(4)})`;
      el.style.opacity = shown.toFixed(3);
      el.style.removeProperty('--name');
    });
  }, []);

  const writeP = useCallback((p: number) => {
    motion.current.p = p;
    rootRef.current?.style.setProperty('--p', p.toFixed(4));
    const frost = frostRef.current;
    // (held through a choice's move: it lifts on its own clock)
    if (frost && !frostHold.current) {
      frost.style.opacity = clamp(p * 1.5, 0, 1).toFixed(3);
      // only drawn while there is something to frost for
      if (p > 0.001) {
        if (frost.dataset.live === undefined) markCard(document.documentElement.dataset.feedCard !== undefined);
        frost.dataset.live = '';
      } else {
        delete frost.dataset.live;
        markCard(false);
      }
    }
    drawFaces();
  }, [drawFaces, markCard]);

  /* ── the frost held through a choice's move, and lifted as the page sets off (or let go at once) ── */
  const dropFrostHold = useCallback(() => {
    const hold = frostHold.current;
    if (!hold) return;
    frostHold.current = null;
    hold.stop();
    hold.lift?.cancel();
    const frost = frostRef.current;
    if (!frost) return;
    const p = motion.current.p;
    frost.style.opacity = clamp(p * 1.5, 0, 1).toFixed(3);
    if (p > 0.001) frost.dataset.live = '';
    else {
      delete frost.dataset.live;
      markCard(false);
    }
  }, [markCard]);
  const holdFrost = useCallback(() => {
    const frost = frostRef.current;
    if (!frost || reducedMotion()) return;
    dropFrostHold();
    frost.style.opacity = '1';
    frost.dataset.live = '';
    let timer = 0;
    const lift = () => {
      const hold = frostHold.current;
      if (!hold || hold.lift) return;
      window.removeEventListener(PAGE_MOVES_EVENT, lift);
      window.clearTimeout(timer);
      // (where the screen is now: a page that moved its scroll as it set off)
      if (frost.parentElement) frost.style.top = `${-frost.parentElement.getBoundingClientRect().top}px`;
      hold.lift = frost.animate([{ opacity: 1 }, { opacity: 0 }], { duration: FROST_LIFT_MS, easing: 'cubic-bezier(0.4, 0, 0.6, 1)', fill: 'forwards' });
      hold.lift.onfinish = () => {
        if (frostHold.current !== hold) return;
        hold.stop();
        frostHold.current = null;
        hold.lift?.cancel();
        frost.style.opacity = '0';
        if (motion.current.p <= 0.001) {
          delete frost.dataset.live;
          markCard(false);
        }
      };
    };
    const jump = (event: Event) => {
      const by = (event as CustomEvent<{ by?: number }>).detail?.by ?? 0;
      frost.style.top = `${(parseFloat(frost.style.top) || 0) + by}px`;
    };
    window.addEventListener(PAGE_MOVES_EVENT, lift);
    window.addEventListener(PAGE_JUMP_EVENT, jump);
    timer = window.setTimeout(lift, FROST_WAIT_MS);
    frostHold.current = {
      lift: null,
      stop: () => {
        window.removeEventListener(PAGE_MOVES_EVENT, lift);
        window.removeEventListener(PAGE_JUMP_EVENT, jump);
        window.clearTimeout(timer);
      },
    };
  }, [dropFrostHold, markCard]);
  useEffect(() => dropFrostHold, [dropFrostHold]);

  /* the layers rising or sinking on their own. From rest (a tap, a pick, a scroll) on one even curve whose way back is
     the mirror of its way out, so they go up at the pace they come down; let go mid-pull, on from where the finger left
     them, settling. Part of the way takes that part of the time */
  const glideTo = useCallback((target: number, fromFinger = false) => {
    const m = motion.current;
    cancelAnimationFrame(m.raf);
    m.raf = 0;
    cancelAnimationFrame(follow.current.raf);
    follow.current.raf = 0;
    const from = m.p;
    if (reducedMotion() || Math.abs(target - from) < 0.001) {
      writeP(target);
      return;
    }
    const duration = LAYERS_MS * Math.max(0.4, Math.abs(target - from));
    const curve = fromFinger ? carryCurve : evenCurve;
    const start = performance.now();
    const step = (now: number) => {
      const k = clamp((now - start) / duration, 0, 1);
      writeP(from + (target - from) * curve(k));
      m.raf = k < 1 ? requestAnimationFrame(step) : 0;
    };
    m.raf = requestAnimationFrame(step);
  }, [writeP]);

  /* ── what the finger is over: a circle (generously round its plate), a chip or a word ── */
  const markHot = useCallback((x: number, y: number, t: number) => {
    const root = rootRef.current;
    if (!root) return;
    hot.current.at = [x, y];
    let best: HTMLElement | null = null;
    let bestD = Infinity;
    root.querySelectorAll<HTMLElement>('[data-pick]').forEach((el) => {
      const face = el.dataset.i != null;
      if (face && (el.dataset.gone != null || Number(el.style.opacity || 1) < 0.6)) return;
      // (a circle by its plate, not its name under it)
      const target = face ? el.querySelector<HTMLElement>('.fm-dock-deal > :first-child') ?? el : el;
      const r = target.getBoundingClientRect();
      const cx = r.left + r.width / 2;
      const cy = r.top + r.height / 2;
      const d = Math.hypot(x - cx, y - cy);
      const inside = face ? d < r.width / 2 + 10 : x >= r.left - 4 && x <= r.right + 4 && y >= r.top - 8 && y <= r.bottom + 8;
      if (inside && d < bestD) {
        best = el;
        bestD = d;
      }
    });
    const key = (best as HTMLElement | null)?.dataset.pick ?? null;
    if (key === hot.current.key) return;
    root.querySelectorAll('[data-hot]').forEach((el) => el.removeAttribute('data-hot'));
    (best as HTMLElement | null)?.setAttribute('data-hot', '');
    hot.current.key = key;
    hot.current.since = t;
    // (a tick for each one the finger lands on, as a picker's)
    if (key) playHaptic('snapLock');
  }, []);

  /* ── the layers pulled up: after the finger, at a limited pace (PULL_K, PULL_V, PULL_A). Steps are a frame's at most,
     so a frame the page was busy in never makes them jump ── */
  const followTo = useCallback((to: number) => {
    const f = follow.current;
    f.to = to;
    if (reducedMotion()) {
      writeP(to);
      return;
    }
    if (f.raf) return;
    f.last = performance.now();
    const damp = 2 * Math.sqrt(PULL_K);
    const step = (now: number) => {
      let left = clamp((now - f.last) / 1000, 0.004, 0.02);
      f.last = now;
      let { p } = motion.current;
      const was = p;
      for (; left > 0; left -= 0.004) {
        const h = Math.min(0.004, left);
        const pull = PULL_K * (f.to - p) - damp * f.v;
        // (gathering pace is eased; slowing down is the spring's own)
        const speedingUp = pull * f.v >= 0;
        f.v = clamp(f.v + (speedingUp ? clamp(pull, -PULL_A, PULL_A) : pull) * h, -PULL_V, PULL_V);
        p += f.v * h;
      }
      if (Math.abs(f.to - p) < 0.002 && Math.abs(f.v) < 0.02) {
        p = f.to;
        f.v = 0;
      }
      writeP(p);
      // (the finger waiting on a circle as they arrive under it)
      if (was <= 0.6 && p > 0.6 && f.at) markHot(f.at[0], f.at[1], f.at[2]);
      f.raf = p === f.to ? 0 : requestAnimationFrame(step);
    };
    f.raf = requestAnimationFrame(step);
  }, [markHot, writeP]);

  /* ── the swipe, as far as it has got: the hero's ring drawn round (the rail's ring, its halves turning into view
     behind windows: transforms alone), and over the dock the tabs' pill, its mark on its way from the tab you're on to
     the one the thumb heads for ── */
  const paintRing = useCallback(() => {
    const { shown, way, to, nudge } = swipe.current;
    const angle = 360 * shown;
    const { right, left, lead, start, end } = ring.current;
    if (right && left && lead && start && end) {
      right.style.transform = `rotate(${(-180 + Math.min(angle, 180)).toFixed(2)}deg)`;
      right.style.opacity = angle > 0.5 ? '1' : '0';
      left.style.transform = `rotate(${(-180 + Math.max(angle - 180, 0)).toFixed(2)}deg)`;
      left.style.opacity = angle > 180 ? '1' : '0';
      lead.style.transform = `rotate(${angle.toFixed(2)}deg)`;
      start.style.opacity = end.style.opacity = angle > 0.5 ? '1' : '0';
    }
    // the page eases back with it (past the ends, only a little, and holds)
    const k = to ? shown : nudge * 0.3;
    scrubPush(k, way);
    const hud = hudRef.current;
    const mark = tabMarkRef.current;
    const bright = tabBrightRef.current;
    if (!hud || !mark || !bright) return;
    const pill = tabPill.current;
    posePill(hud, mark, bright, pill.mark.x * pill.seg, pill.rise.x);
    // (the bar presses in with it, and eases out as it goes back; closed, it has sprung home: closeRing)
    const bar = barRef.current;
    if (bar && !('ringShut' in bar.dataset)) {
      const press = PRESS * clamp(pill.rise.x, 0, 1);
      bar.style.transform = press > 0.0004 ? `scale(${(1 - press).toFixed(4)})` : '';
    }
  }, []);

  // eased toward the thumb once a frame; unwound all the way with the thumb gone, the dock is plain again
  const easeRing = useCallback(() => {
    const s = swipe.current;
    if (s.raf) return;
    s.last = performance.now();
    const step = (now: number) => {
      const dt = clamp((now - s.last) / 1000, 0.004, 0.05);
      s.last = now;
      s.shown += (s.target - s.shown) * (1 - Math.exp(-dt / RING_LAG));
      if (Math.abs(s.target - s.shown) < 0.002) s.shown = s.target;
      // the pill on the pull's own springs: up over the first stretch of the swipe, its mark with the ring
      const { rise, mark } = tabPill.current;
      const at = TABS.indexOf(s.from);
      rise.to = s.held ? clamp((s.to ? s.target : s.nudge) * 2.4, 0, 1) : 0;
      mark.to = s.held ? (s.to ? at + (TABS.indexOf(s.to) - at) * magnet(s.target) : at + s.way * 0.05 * s.nudge) : at;
      rise.step(dt, s.held ? 460 : 260);
      mark.step(dt, 520);
      const gliding = rise.moving || mark.moving;
      paintRing();
      if (s.shown !== s.target || gliding) {
        s.raf = requestAnimationFrame(step);
        return;
      }
      s.raf = 0;
      if (!s.held && !s.shown && barRef.current) clearRing(barRef.current);
    };
    s.raf = requestAnimationFrame(step);
  }, [paintRing]);

  /* ── the row turning: held at an end mid-slide it rolls on; swiped, it carries on and settles on a circle ── */
  const turnRow = useCallback(() => {
    const r = row.current;
    if (r.raf) return;
    r.last = performance.now();
    const step = (now: number) => {
      const dt = clamp((now - r.last) / 1000, 0.004, 0.032);
      r.last = now;
      const count = trackRef.current?.querySelectorAll('[data-i]').length ?? 1;
      if (r.edge) {
        r.th = clamp(r.th + r.edge * 4.5 * dt, firstTh(count) - 0.3, count - 0.7);
      } else if (r.target != null) {
        r.tv += (-190 * (r.th - r.target) - 26 * r.tv) * dt;
        r.th += r.tv * dt;
        if (Math.abs(r.th - r.target) < 0.002 && Math.abs(r.tv) < 0.01) {
          r.th = r.target;
          r.target = null;
        }
      }
      drawFaces();
      if (r.edge && hot.current.at) markHot(hot.current.at[0], hot.current.at[1], now);
      r.raf = r.edge || r.target != null ? requestAnimationFrame(step) : 0;
    };
    r.raf = requestAnimationFrame(step);
  }, [drawFaces, markHot]);

  const settleRow = useCallback((v = 0) => {
    const r = row.current;
    const count = trackRef.current?.querySelectorAll('[data-i]').length ?? 1;
    r.target = clamp(Math.round(r.th + v * 0.16), firstTh(count), count - 1);
    r.tv = v;
    r.edge = 0;
    turnRow();
  }, [turnRow]);

  const clearHot = useCallback(() => {
    rootRef.current?.querySelectorAll('[data-hot]').forEach((el) => el.removeAttribute('data-hot'));
    hot.current = { key: null, since: 0, at: null };
  }, []);

  /* ── open and shut ── */
  // the layers made ready to rise: the frost laid where the screen is now, and the row turned to the one you're on,
  // whose circle flies up out of the hero
  const readyLayers = () => {
    // (a frost still held from the last choice is the layers' own again)
    dropFrostHold();
    const frost = frostRef.current;
    if (frost?.parentElement) frost.style.top = `${-frost.parentElement.getBoundingClientRect().top}px`;
    const r = row.current;
    r.th = Math.max(firstTh(faces.length), pickIndex);
    r.fly = pickIndex;
    r.target = null;
    const face = faceRef.current?.getBoundingClientRect();
    const track = trackRef.current?.getBoundingClientRect();
    if (face && track) {
      r.flyX = face.left + face.width / 2 - track.left;
      r.flyY = face.top + face.height / 2 - track.top;
    }
    openAt.current = window.scrollY;
    setOpen(true);
  };

  const openLayers = (fromFinger = false) => {
    readyLayers();
    // (let go of, they go on at the pace they had)
    if (fromFinger) followTo(1);
    else glideTo(1);
  };

  const closeLayers = useCallback((fromFinger = false) => {
    setOpen(false);
    clearHot();
    row.current.edge = 0;
    glideTo(0, fromFinger);
  }, [clearHot, glideTo]);

  // something to do once the layers have sunk (or a pick's ring has been drawn round): one at a time
  const later = (fn: () => void, wait: number) => {
    window.clearTimeout(laterTimer.current);
    laterTimer.current = window.setTimeout(fn, wait);
  };

  /* The rail dealt again (a feed opened, back to All feeds): the row turns to the one you're on, what stays glides on
     from where it was, and what is new comes out from behind the one you're on, a beat after the one nearer it. On the
     compositor (WAAPI, on each circle's own inner layer): the page under the layers is busy with the same pick. A new
     pick alone moves nothing: its ring is drawn round where it is */
  const faceKeys = faces.map((face) => face.key).join('|');
  const dealtKeys = useRef(faceKeys);
  useIsoLayoutEffect(() => {
    const r = row.current;
    r.fly = pickIndex;
    const dealt = dealtKeys.current !== faceKeys;
    dealtKeys.current = faceKeys;
    if (dealt) {
      r.th = Math.max(firstTh(faces.length), pickIndex);
      r.target = null;
      const track = trackRef.current;
      if (track && motion.current.p > 0.5 && !reducedMotion()) {
        const hub = Math.max(0, pickIndex);
        track.querySelectorAll<HTMLElement>('[data-i]').forEach((el) => {
          const deal = el.firstElementChild as HTMLElement | null;
          if (!deal) return;
          const i = Number(el.dataset.i);
          const was = lastX.current.get(el.dataset.key ?? '');
          const x = (i - r.th) * STEP;
          deal.animate(
            was != null
              ? [{ transform: `translate3d(${(was - x).toFixed(1)}px, 0, 0)` }, { transform: 'none' }]
              : [{ transform: `translate3d(${((hub - i) * STEP).toFixed(1)}px, 0, 0) scale(0.6)`, opacity: 0 }, { transform: 'none', opacity: 1 }],
            { duration: FAN_MS, delay: was != null ? 0 : DEAL_STEP_MS * Math.abs(i - hub), easing: CARRY_EASE_CSS, fill: 'backwards' },
          );
        });
      }
      lastX.current.clear();
    }
    drawFaces();
  }, [faceKeys, pickIndex, drawFaces]);

  // the tabs' mark on the tab you're on, for the next swipe to set off from
  const mounted = Boolean(config) && !wide;
  useIsoLayoutEffect(() => {
    tabRef.current = tab;
    const s = swipe.current;
    if (!hudRef.current || s.held || s.to) return;
    s.from = tab;
    tabPill.current.mark.set(TABS.indexOf(tab));
    paintRing();
  }, [tab, mounted, paintRing]);

  /* ── scrolled down, the bar folds into its handle: its glass draws in to the middle and what it carries goes with it,
     while the handle glides to the middle a size up, on a glass of its own; scrolled back up, it opens again. One curve
     and one beat both ways, carried on from wherever a change of mind finds it (dock.css: data-folded) ── */
  const fold = useCallback((folded: boolean) => {
    if (foldedRef.current === folded) return;
    foldedRef.current = folded;
    const root = rootRef.current;
    const glass = glassRef.current;
    const rest = restRef.current;
    const face = faceRef.current;
    const pad = padRef.current;
    if (!root || !glass || !rest || !face || !pad) return;
    // where each part is (a fold still under way included), then where it rests now
    const pose = (el: HTMLElement) => {
      const style = getComputedStyle(el);
      return { transform: style.transform, translate: style.translate, scale: style.scale, opacity: Number(style.opacity) };
    };
    const from = { glass: pose(glass), rest: pose(rest), face: pose(face), pad: pose(pad) };
    [glass, rest, face, pad].forEach((el) => el.getAnimations().forEach((animation) => {
      if (animation.id === 'fold') animation.cancel();
    }));
    if (folded) root.dataset.folded = '';
    else delete root.dataset.folded;
    if (reducedMotion()) return;
    const to = { glass: pose(glass), rest: pose(rest), face: pose(face) };
    const glide: KeyframeAnimationOptions = { id: 'fold', duration: FOLD_MS, easing: CARRY_EASE_CSS };
    const beat: KeyframeAnimationOptions = { id: 'fold', duration: FOLD_MS, easing: 'linear' };
    // what leaves goes over the first part; what comes waits for the second (never two things fading across each other)
    const leave = (at: number): Keyframe[] => [{ opacity: at, easing: 'cubic-bezier(0.4, 0, 1, 1)' }, { opacity: 0, offset: 0.35 }, { opacity: 0 }];
    const come = (at: number): Keyframe[] => [{ opacity: at }, { opacity: at, offset: 0.45, easing: 'cubic-bezier(0, 0, 0.2, 1)' }, { opacity: 1 }];
    glass.animate([{ transform: from.glass.transform }, { transform: to.glass.transform }], glide);
    rest.animate([{ transform: from.rest.transform }, { transform: to.rest.transform }], glide);
    face.animate([{ translate: from.face.translate, scale: from.face.scale }, { translate: to.face.translate, scale: to.face.scale }], glide);
    rest.animate(folded ? leave(from.rest.opacity) : come(from.rest.opacity), beat);
    pad.animate(folded ? come(from.pad.opacity) : leave(from.pad.opacity), beat);
  }, []);

  // a tab arrived at opens with the whole bar (what it says has changed), however far the last one was scrolled
  useIsoLayoutEffect(() => {
    scrolled.current.y = window.scrollY;
    scrolled.current.travel = 0;
    fold(false);
  }, [tab, me, fold]);

  /* the page scrolling: down a way, the bar folds; back up a way, or at the page's top or foot, it opens. A passive
     listener and an attribute: nothing renders while the page scrolls */
  useEffect(() => {
    if (!mounted) return undefined;
    // (a layer scrolling in the page's place, Feed's Day on a phone: its scroll is the one followed while it is up)
    let source = currentScrollSource();
    const yOf = () => (source ? source.scrollTop : window.scrollY);
    const onScroll = () => {
      const s = scrolled.current;
      const y = yOf();
      const dy = y - s.y;
      s.y = y;
      // (the layers up, or a swipe on the dock: they have it)
      if (motion.current.p > 0 || swipe.current.held) return;
      /* (a jump the page makes itself is no direction: Feed's zoom handing its held scroll over as it lands went up a
         whole screen in one step, and the bar unfolded a beat after the move had landed. It keeps its fold, as the
         header does; one asking for the whole screen folds it) */
      const jumping = liveScrollJump();
      if (jumping) {
        s.travel = 0;
        if (jumping === 'fold') fold(true);
        return;
      }
      const foot = source ? source.scrollHeight - source.clientHeight : document.documentElement.scrollHeight - window.innerHeight;
      if (y < 24 || y > foot - 48) {
        s.travel = 0;
        fold(false);
        return;
      }
      // what runs the same way adds up; a turn starts the count again
      s.travel = dy * s.travel > 0 ? s.travel + dy : dy;
      if (s.travel > FOLD_TRAVEL) fold(true);
      else if (s.travel < -FOLD_TRAVEL) fold(false);
    };
    // (the source changing: measured afresh from where the new one is, its first scroll no direction)
    const follow = () => {
      source?.removeEventListener('scroll', onScroll);
      source = currentScrollSource();
      source?.addEventListener('scroll', onScroll, { passive: true });
      scrolled.current.y = yOf();
      scrolled.current.travel = 0;
    };
    source?.addEventListener('scroll', onScroll, { passive: true });
    const unfollow = onScrollSource(follow);
    window.addEventListener('scroll', onScroll, { passive: true });
    return () => {
      unfollow();
      source?.removeEventListener('scroll', onScroll);
      window.removeEventListener('scroll', onScroll);
    };
  }, [mounted, fold]);

  // open, a scroll of the page, Escape, or another tab shuts it
  useEffect(() => {
    if (!open) return undefined;
    const onScroll = () => {
      if (Math.abs(window.scrollY - openAt.current) > 24) closeLayers();
    };
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') closeLayers();
    };
    window.addEventListener('scroll', onScroll, { passive: true });
    window.addEventListener('keydown', onKey);
    return () => {
      window.removeEventListener('scroll', onScroll);
      window.removeEventListener('keydown', onKey);
    };
  }, [open, closeLayers]);

  useEffect(() => () => {
    cancelAnimationFrame(motion.current.raf);
    cancelAnimationFrame(follow.current.raf);
    cancelAnimationFrame(row.current.raf);
    cancelAnimationFrame(swipe.current.raf);
    window.clearTimeout(laterTimer.current);
    pull.current?.dispose();
  }, []);

  /* ── the swipe let go. Short, it unwinds and the tabs over the dock sink away. Closed (`to`), drawn whole, the ring
     is the hero's own again: the hero takes the switch with a press, the tab's icon turns into `to`'s (dock.css), and
     `to` holds lit over the dock until the card starts away (lib/tabPush tells: sinkHud), then sinks. Transform and
     opacity alone, on the compositor: the switch is busy under it ── */
  const unwindRing = () => {
    const s = swipe.current;
    s.held = false;
    s.target = 0;
    s.nudge = 0;
    dropPush();
    easeRing();
  };

  const closeRing = (to: DockTab) => {
    const s = swipe.current;
    cancelAnimationFrame(s.raf);
    Object.assign(s, { raf: 0, held: false, shown: 1, target: 1, to, nudge: 0 });
    // (what is left of the pill's way, and of its mark's, goes on the compositor, since the switch is busy under it; the
    // bar, pressed in, springs home)
    const hud = hudRef.current;
    const mark = tabMarkRef.current;
    const bright = tabBrightRef.current;
    if (hud && mark && bright) {
      const { rise, mark: along, seg } = tabPill.current;
      const r = rise.x;
      const x = along.x * seg;
      const end = TABS.indexOf(to) * seg;
      rise.set(1);
      along.set(TABS.indexOf(to));
      if (barRef.current) {
        barRef.current.dataset.ringShut = '';
        springBar(barRef.current, PRESS * clamp(r, 0, 1));
      }
      if (!reducedMotion()) {
        const glide: KeyframeAnimationOptions = { duration: ms(260), easing: CARRY_EASE_CSS };
        if (r < 0.98) hud.animate([{ transform: pillAt(r), opacity: pillShown(r) }, { transform: pillAt(1), opacity: 1 }], glide);
        if (Math.abs(x - end) > 0.5) {
          mark.animate([{ transform: `translate3d(${x.toFixed(2)}px, 0, 0)` }, { transform: `translate3d(${end.toFixed(2)}px, 0, 0)` }], glide);
          bright.animate([{ transform: `translate3d(${(-x).toFixed(2)}px, 0, 0)` }, { transform: `translate3d(${(-end).toFixed(2)}px, 0, 0)` }], glide);
        }
      }
    }
    paintRing();
    const bar = barRef.current;
    if (!bar) return;
    delete bar.dataset.swiping;
    bar.dataset.ringTo = to;
    bar.dataset.ringShut = '';
    if (!reducedMotion()) {
      faceRef.current?.animate(
        [{ transform: 'scale(1)' }, { transform: 'scale(0.92)', offset: 0.32 }, { transform: 'scale(1.02)', offset: 0.72 }, { transform: 'scale(1)' }],
        { duration: ms(600), easing: CARRY_EASE_CSS },
      );
    }
  };

  // the card under way: the tabs over the dock sink, and the ring gives its red back to the hero's own (drawn whole under
  // it: nothing else on screen changes). A swipe begun meanwhile keeps what it has drawn
  const sinkHud = () => {
    const plain = () => {
      const s = swipe.current;
      if (s.held) return;
      s.from = tabRef.current;
      s.to = null;
      s.shown = s.target = 0;
      tabPill.current.rise.set(0);
      tabPill.current.mark.set(TABS.indexOf(s.from));
      paintRing();
      if (barRef.current) clearRing(barRef.current);
    };
    const hud = hudRef.current;
    const settle = () => {
      if (swipe.current.held) return;
      if (hud) hud.style.visibility = 'hidden';
      if (barRef.current) coolRing(barRef.current, plain);
      else plain();
    };
    if (!hud) return settle();
    const out = hud.animate(
      [{ opacity: 1, transform: pillAt(1) }, { opacity: 0, transform: pillAt(0) }],
      { duration: HUD_OUT_MS, easing: CARRY_EASE_CSS, fill: 'forwards' },
    );
    const done = () => {
      settle();
      out.cancel();
    };
    out.finished.then(done, done);
  };

  /* ── going to a tab: the page is sent on its way (lib/tabPush; a closed ring has sent it already), then the URL
     changes in place, as the bar's did ── */
  const goTab = (next: DockTab, sent = false) => {
    if (next === tab && !me) return;
    const dir = TABS.indexOf(next) > here ? 1 : -1;
    // (a move of the page's own lands first, as from the side rail: Feed's)
    void leaveTab(me ? 'me' : tab, next, dir);
    // (Me goes as it came, over the tab: no card of it slides away)
    if (!sent && !me && !reducedMotion()) leavePush(tab, dir);
    try {
      sessionStorage.setItem('feedme:intent', HREF[next]);
    } catch {}
    playHaptic('navSwitch');
    window.history.pushState(null, '', HREF[next]);
  };

  // the ring closed on a tab: the card lifts at once, and two frames on (the press it takes, and the card's lift, under
  // way on the compositor) the tab's own work begins
  const switchTo = (next: DockTab) => {
    closeRing(next);
    const sent = !me && !reducedMotion();
    if (sent) leavePush(tab, TABS.indexOf(next) > here ? 1 : -1, sinkHud);
    requestAnimationFrame(() => requestAnimationFrame(() => {
      goTab(next, sent);
      if (!sent) sinkHud();
    }));
  };

  /* ── a pick. A feeder: its ring is drawn round, then the layers sink back with it. A feed, or back to All feeds:
     the rail is dealt again in place. A tab or a choice: the layers sink first, then the page moves (the dock takes a
     choice with a pulse). What you're already on changes nothing ── */
  const pulse = () => {
    if (reducedMotion()) return;
    barRef.current?.animate([{ scale: '1' }, { scale: '1.045', offset: 0.35 }, { scale: '1' }], { duration: ms(480), easing: CARRY_EASE_CSS });
  };
  const choose = (key: string) => {
    if (!config) return;
    if (key === 'me') {
      if (me) return;
      closeLayers();
      playHaptic('navSwitch');
      later(() => void leaveTab(tab, 'me', 1).then(openMe), APPLY_MS);
      return;
    }
    if (key.startsWith('tab:')) {
      const next = key.slice(4) as DockTab;
      if (next === tab && !me) return;
      closeLayers();
      later(() => goTab(next), APPLY_MS);
      return;
    }
    if (key.startsWith('choice:')) {
      if (!choice) return;
      const value = choice.options[Number(key.slice(7))];
      if (value === choice.value) return;
      playHaptic('pick');
      // (the frost stays till the page sets off: Safari's bars go from it to the new view, never back to the old)
      holdFrost();
      closeLayers();
      pulse();
      later(() => choice.onChange(value), APPLY_MS);
      return;
    }
    const face = key.slice(5);
    if (face === pickKey) return;
    playHaptic('pick');
    if (face === 'all') return config.onAll();
    if (face.startsWith('feed:')) return config.onFeed(face.slice(5));
    if (face.startsWith('feeder:')) {
      config.onFeeder(face.slice(7));
      later(() => {
        closeLayers();
        pulse();
      }, PICK_HOLD_MS);
    }
  };

  /* ── the pull down: the tab's choice as a row of stops, opened on the one you're on ── */
  const pullable = choice != null && choice.options.length > 1 && choice.options.length <= 4;
  const pullWords = choice ? choice.options.map((value) => {
    const word = choice.label(value);
    return /^[A-Z]{3,}$/.test(word) ? word[0] + word.slice(1).toLowerCase() : word;
  }) : [];
  const startPull = (event: ReactPointerEvent<HTMLElement>) => {
    const bar = barRef.current;
    const face = faceRef.current;
    const pill = pillRef.current;
    const mark = pillMarkRef.current;
    const bright = pillBrightRef.current;
    if (!choice || !bar || !face || !pill || !mark || !bright) return;
    releasePush();
    fold(false);
    // (the swipe's ring, unwound and still: the pull draws it now)
    cancelAnimationFrame(swipe.current.raf);
    Object.assign(swipe.current, { raf: 0, shown: 0, target: 0 });
    const { options } = choice;
    const at = Math.max(0, options.indexOf(choice.value));
    const was = chosen.current[tab]?.last;
    const back = was == null ? -1 : options.indexOf(was);
    const last = back >= 0 && back !== at ? back : at > 0 ? at - 1 : at + 1;
    const dom: PullDom = { bar, face, ring: ring.current, pill, mark, bright };
    pull.current ??= new DockPull();
    pull.current.begin(dom, {
      count: options.length,
      at,
      last,
      live: tab === 'lead',
      onPick: (index, way) => {
        document.querySelector<HTMLElement>('.fm-lead')?.style.setProperty('--slit-way', String(way));
        // (the page's own handler as it is now: the page moves under the thumb, and one from before it moved would take
        // the way back for no change)
        choiceNow.current?.onChange(options[index]);
      },
      onDone: (index) => {
        if (index !== at) chosen.current[tab] = { now: options[index], last: options[at] };
      },
    }, event.clientX, gesture.current?.lastT ?? event.timeStamp);
  };

  /* A second finger anywhere: it is a pinch on the page, never the dock's (the thumb of one resting here would switch
     tabs). Told by the touches themselves: the dock's own pointer may have been the first to land */
  const dropGesture = useRef<() => void>(() => {});
  useEffect(() => {
    dropGesture.current = () => {
      const g = gesture.current;
      if (!g) return;
      gesture.current = null;
      barRef.current?.classList.remove('is-dragging');
      if (g.mode === 'swipe') return unwindRing();
      releasePush();
      if (g.mode === 'drop') pull.current?.end(0, true);
      else if (g.mode === 'pull' && !g.wasOpen) closeLayers(true);
      clearHot();
    };
  });
  // (said on the page while a dock is on it: dockPull.css keeps a pinch from Safari)
  useEffect(() => {
    const root = document.documentElement;
    root.dataset.fmDock = '';
    return () => {
      delete root.dataset.fmDock;
    };
  }, []);
  useEffect(() => {
    const onTouches = (event: TouchEvent) => {
      if (event.touches.length > 1) dropGesture.current();
    };
    window.addEventListener('touchstart', onTouches, { passive: true, capture: true });
    return () => window.removeEventListener('touchstart', onTouches, { capture: true });
  }, []);

  /* ── the dock's own gestures: sideways for the next tab, up for the layers, down for the choice, a tap to open ── */
  const onDown = (event: ReactPointerEvent<HTMLElement>) => {
    if (event.button !== 0) return;
    // (a second finger's: the dock is never part of a pinch)
    if (!event.isPrimary) return;
    event.currentTarget.setPointerCapture(event.pointerId);
    gesture.current = { id: event.pointerId, x0: event.clientX, y0: event.clientY, mode: null, wasOpen: open, locked: false, lastX: event.clientX, lastT: event.timeStamp, v: 0, ring: 0, raw: 0 };
    // the page copied now, while the finger is still: a swipe that follows has nothing heavy to do (lib/tabPush)
    if (!open && !me && !reducedMotion()) holdPush(tab);
  };

  // a swipe begun: the ring dims to a track, and the tabs rise over the dock, the one you're on lit
  const startSwipe = () => {
    const s = swipe.current;
    s.held = true;
    s.from = tab;
    const bright = tabBrightRef.current;
    if (bright) {
      // (the last switch's tabs, still sinking, and its ring, still cooling, give way)
      [hudRef.current, tabMarkRef.current, bright, barRef.current?.querySelector('.fm-dock-ring')].forEach((el) => el?.getAnimations().forEach((animation) => animation.cancel()));
      const pill = tabPill.current;
      pill.seg = bright.offsetWidth / TABS.length || 64;
      // (from rest; one caught on its way back, or still standing from the last switch, goes on from there)
      if (!s.raf) {
        if (!barRef.current || !('ringShut' in barRef.current.dataset)) pill.rise.set(0);
        pill.mark.set(TABS.indexOf(tab));
      }
    }
    const bar = barRef.current;
    if (bar) {
      clearRing(bar);
      bar.dataset.swiping = '';
    }
  };

  const onMove = (event: ReactPointerEvent<HTMLElement>) => {
    const g = gesture.current;
    if (!g || event.pointerId !== g.id) return;
    const dx = event.clientX - g.x0;
    const dy = g.y0 - event.clientY;
    if (!g.mode && Math.hypot(dx, dy) > 10) {
      if (!g.wasOpen && Math.abs(dx) > Math.abs(dy) * 1.2) {
        g.mode = 'swipe';
        startSwipe();
      } else if (dy > 0 || g.wasOpen) {
        g.mode = 'pull';
        releasePush();
        if (!g.wasOpen) {
          // the finger takes the layers from wherever the spring had them
          cancelAnimationFrame(motion.current.raf);
          motion.current.raf = 0;
          follow.current.v = 0;
          follow.current.at = null;
          readyLayers();
        }
      } else if (pullable) {
        g.mode = 'drop';
        startPull(event);
      }
      barRef.current?.classList.add('is-dragging');
    }
    if (g.mode === 'drop') {
      pull.current?.move(-dy, event.clientX);
      return;
    }
    if (g.mode === 'pull') {
      if (!g.wasOpen) {
        const raw = Math.max(0, dy) / PULL;
        if (raw >= 0.92 && !g.locked) {
          g.locked = true;
          playHaptic('snapLock');
        }
        g.raw = raw;
        follow.current.at = [event.clientX, event.clientY, event.timeStamp];
        followTo(g.locked ? Math.max(1, 1 + (raw - 1) * 0.12) : raw);
      }
      if (motion.current.p > 0.6 || g.wasOpen) markHot(event.clientX, event.clientY, event.timeStamp);
      // resting at either end of the row turns it on toward that side
      const rowBox = rowRef.current?.getBoundingClientRect();
      const inRow = Boolean(rowBox && event.clientY >= rowBox.top && event.clientY <= rowBox.bottom);
      const edge = rowBox && inRow && motion.current.p > 0.85 ? (event.clientX < rowBox.left + 30 ? -1 : event.clientX > rowBox.right - 30 ? 1 : 0) : 0;
      if (edge !== row.current.edge) {
        row.current.edge = edge;
        row.current.target = null;
        if (edge) turnRow();
      }
    } else if (g.mode === 'swipe') {
      /* the ring draws round with the thumb, a breath behind it, and the thumb taking it all the way round is the
         switch, there and then. Past the first and the last tab there is none to draw: the mark only gives a little
         toward the thumb and holds. The dock itself stays put (its glass moved would be its blur drawn again, every
         frame) */
      const next = here + (dx < 0 ? 1 : -1);
      const free = next >= 0 && next < TABS.length;
      g.v = (event.clientX - g.lastX) / Math.max(8, event.timeStamp - g.lastT);
      g.lastX = event.clientX;
      g.lastT = event.timeStamp;
      g.ring = free ? Math.min(1, Math.abs(dx) / RING) : 0;
      const bar = barRef.current;
      if (bar) bar.dataset.ringDir = dx < 0 ? 'next' : 'back';
      if (g.ring >= 1) {
        gesture.current = null;
        bar?.classList.remove('is-dragging');
        switchTo(TABS[next]);
        return;
      }
      const s = swipe.current;
      Object.assign(s, { target: g.ring, way: dx < 0 ? 1 : -1, to: free ? TABS[next] : null, nudge: free ? 0 : Math.sqrt(Math.min(1, Math.abs(dx) / RING)) });
      easeRing();
    }
  };

  const onUp = (event: ReactPointerEvent<HTMLElement>, cancelled = false) => {
    const g = gesture.current;
    if (!g || event.pointerId !== g.id) return;
    gesture.current = null;
    barRef.current?.classList.remove('is-dragging');
    const dx = event.clientX - g.x0;
    if (row.current.edge || row.current.raf) settleRow(0);

    if (g.mode === 'drop') {
      pull.current?.end(event.timeStamp, cancelled);
      return;
    }
    if (g.mode === 'swipe') {
      // let go short of closing the ring: past halfway, or a flick on the way, closes it anyway; anything less unwinds it
      const next = here + (dx < 0 ? 1 : -1);
      // (what is lit is what you get: past halfway the mark is on the next tab)
      const lit = g.ring >= 0.5;
      if (!cancelled && next >= 0 && next < TABS.length && (lit || (dx * g.v > 0 && Math.abs(g.v) > FLICK))) {
        switchTo(TABS[next]);
        return;
      }
      unwindRing();
      return;
    }
    releasePush();
    if (cancelled) {
      if (!g.wasOpen) closeLayers(true);
      return;
    }
    if (g.mode === 'pull') {
      const key = hot.current.key;
      const rested = event.timeStamp - hot.current.since > REST_MS;
      clearHot();
      if (key && rested) return choose(key);
      if (g.wasOpen) return;
      // (by where the finger took them, not how far they have come)
      if (g.locked || g.raw > PULL_TAKES) openLayers(true);
      else closeLayers(true);
      return;
    }
    // a tap
    if (open) return closeLayers();
    // (on Me: the disc is the way back to the tab it opened over, and the rest opens the layers)
    const disc = me && !foldedRef.current ? barRef.current?.querySelector('.fm-dock-tab')?.getBoundingClientRect() : null;
    if (disc && event.clientX >= disc.left - 8 && event.clientX <= disc.right + 14) return goTab(tab);
    // (inside something: a tap is the way back out of it)
    if (back && !me) return back.onBack();
    openTap.current = { x: event.clientX, y: event.clientY, t: event.timeStamp };
    openLayers();
  };

  /* ── open, the row scrolls under a sideways swipe on it (a tap still picks) ── */
  const spin = useRef<{ id: number; x0: number; y0: number; th0: number; moved: boolean; lastX: number; lastT: number; v: number } | null>(null);
  const swallow = useRef(false);
  const onRowDown = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (!open || gesture.current) return;
    spin.current = { id: event.pointerId, x0: event.clientX, y0: event.clientY, th0: row.current.th, moved: false, lastX: event.clientX, lastT: event.timeStamp, v: 0 };
  };
  const onRowMove = (event: ReactPointerEvent<HTMLDivElement>) => {
    const s = spin.current;
    if (!s || event.pointerId !== s.id) return;
    const dx = event.clientX - s.x0;
    if (!s.moved && Math.abs(dx) > 8 && Math.abs(dx) > Math.abs(event.clientY - s.y0)) {
      s.moved = true;
      event.currentTarget.setPointerCapture(event.pointerId);
      row.current.target = null;
    }
    if (!s.moved) return;
    const count = trackRef.current?.querySelectorAll('[data-i]').length ?? 1;
    let th = s.th0 - dx / STEP;
    const first = firstTh(count);
    if (th < first) th = first + (th - first) * 0.3;
    if (th > count - 1) th = count - 1 + (th - (count - 1)) * 0.3;
    s.v = (-(event.clientX - s.lastX) / STEP) / Math.max(0.008, (event.timeStamp - s.lastT) / 1000);
    s.lastX = event.clientX;
    s.lastT = event.timeStamp;
    row.current.th = th;
    drawFaces();
  };
  const onRowUp = (event: ReactPointerEvent<HTMLDivElement>) => {
    const s = spin.current;
    if (!s || event.pointerId !== s.id) return;
    spin.current = null;
    if (!s.moved) return;
    // the click this release may fire is not a tap
    swallow.current = true;
    window.setTimeout(() => {
      swallow.current = false;
    }, 0);
    settleRow(clamp(s.v, -12, 12));
  };

  const onLayerClick = (event: ReactMouseEvent<HTMLElement>) => {
    if (swallow.current || isOpenTap(event)) return;
    const el = (event.target as HTMLElement).closest<HTMLElement>('[data-pick]');
    if (el?.dataset.pick) choose(el.dataset.pick);
  };

  if (wide || !config) return null;

  const heroArt: RailFaceOf = pickedFeeder ? { kind: 'feeder', feeder: pickedFeeder } : activeFeed ? { kind: 'feed', feed: activeFeed } : { kind: 'all', back: false };
  const heroName = pickedFeeder ? `@${pickedFeeder.handle}` : activeFeed ? titleCase(activeFeed.title) : 'All feeds';
  const where = pickedFeeder
    ? activeFeed ? titleCase(activeFeed.title) : ''
    : activeFeed ? `${activeFeed.feeders.length} feeders` : config.feeds ? `${config.feeds.length} feeds` : '';
  const sub = [back ? back.note : where, choice ? choice.label(choice.value) : null].filter(Boolean).join(' · ');
  const line = back ? back.label : heroName;

  return (
    <>
      {/* the page frosted under the layers: a tap on it, outside them, shuts them */}
      <div ref={frostRef} aria-hidden="true" className="fm-dock-frost" data-open={open ? '' : undefined} onClick={(event) => {
        if (!isOpenTap(event)) closeLayers();
      }} />
      <div ref={rootRef} className="fm-dock" data-open={open ? '' : undefined} data-away={config.dockAway ? '' : undefined} style={{ '--fm-dock-step': `${STEP}px` } as CSSProperties}>
        <div className="fm-dock-layers" role="dialog" aria-label="Tabs, feeders and choices" aria-hidden={!open} onClick={onLayerClick}>
          {/* Me, over the tabs and apart from them: the last to rise */}
          <DockMe on={me} open={open} />
          {/* the tabs, rising over the row: the one you're on first, the others a beat behind */}
          <div className="fm-dock-words">
            {TABS.map((one, index) => (
              <button
                key={one}
                type="button"
                data-pick={`tab:${one}`}
                className={one === tab && !me ? 'is-on' : undefined}
                style={{ '--st': (0.3 + 0.08 * Math.abs(index - here)).toFixed(2) } as CSSProperties}
              >
                {NAME[one]}
              </button>
            ))}
          </div>
          {/* the rail's circles, the one you're on in the middle (a release after a slide of the row is no tap) */}
          <div
            ref={rowRef}
            className="fm-dock-faces"
            onPointerDown={onRowDown}
            onPointerMove={onRowMove}
            onPointerUp={onRowUp}
            onPointerCancel={onRowUp}
            onClickCapture={(event) => {
              if (swallow.current) event.stopPropagation();
            }}
          >
            <div ref={trackRef} className="fm-dock-track">
              {faces.map((face, index) => {
                const lit = face.key === pickKey;
                const marked = face.feeder ? isMarked(face.feeder.handle) : face.feed ? face.feed.feeders.some((one) => isMarked(one.handle)) : false;
                return (
                  <button
                    key={face.key}
                    type="button"
                    data-pick={`face:${face.key}`}
                    data-key={face.key}
                    data-i={index}
                    // (first over last from the one you're on: what comes out of it comes out from behind it)
                    style={{ zIndex: lit ? 40 : 30 - Math.min(29, Math.abs(index - pickIndex)) }}
                    {...press}
                    className="fm-dock-item group"
                    aria-label={face.litLabel}
                    aria-pressed={lit}
                  >
                    <span className="fm-dock-deal">
                      <Plate lit={lit} marked={marked} armed={false} reduce={reduce}>
                        <StoryRing on={lit} reduce={reduce} />
                        <RailFace face={face.art} reduce={reduce} />
                      </Plate>
                      <RailLabel text={face.label} litText={face.litLabel} lit={lit} reduce={reduce} className={face.feeder ? 'lowercase' : 'uppercase'} />
                    </span>
                  </button>
                );
              })}
            </div>
          </div>
          {/* the choice: the dock's second line spread out, in the dock's place */}
          {choice ? (
            <div className="fm-dock-chips" style={{ '--n': choice.options.length } as CSSProperties}>
              {choice.options.map((value, index) => (
                <button
                  key={String(value)}
                  type="button"
                  data-pick={`choice:${index}`}
                  className={value === choice.value ? 'is-on' : undefined}
                  style={{ '--i': index } as CSSProperties}
                  aria-label={`${choice.name}: ${choice.label(value)}`}
                >
                  {choice.label(value)}
                </button>
              ))}
            </div>
          ) : null}
        </div>

        {/* the tabs: while a swipe is under way, over the dock and clear of the thumb, the mark heading for the next */}
        <div ref={hudRef} className="fm-pull-pill is-tabs" aria-hidden="true" style={{ '--n': TABS.length } as CSSProperties}>
          <div className="fm-pull-words">{TABS.map((one) => <span key={one}>{NAME[one]}</span>)}</div>
          <div ref={tabMarkRef} className="fm-pull-mark">
            <div ref={tabBrightRef} className="fm-pull-words fm-pull-bright">{TABS.map((one) => <span key={one}>{NAME[one]}</span>)}</div>
          </div>
        </div>

        {/* the pill of stops a pull down brings up over the bar */}
        {pullable ? (
          <div ref={pillRef} className="fm-pull-pill" aria-hidden="true" style={{ '--n': pullWords.length } as CSSProperties}>
            <div className="fm-pull-words">{pullWords.map((word) => <span key={word}>{word}</span>)}</div>
            <div ref={pillMarkRef} className="fm-pull-mark">
              <div ref={pillBrightRef} className="fm-pull-words fm-pull-bright">{pullWords.map((word) => <span key={word}>{word}</span>)}</div>
            </div>
          </div>
        ) : null}

        <button
          ref={barRef}
          type="button"
          className="fm-dock-bar"
          aria-expanded={open}
          aria-label={`${me ? line : back ? `Back to ${line.toLowerCase()}` : heroName}${sub ? `, ${sub}` : ''}, ${me ? `over ${NAME[tab]}` : NAME[tab]}. Swipe for the next tab; pull up${back && !me ? '' : ' or tap'} for feeders and choices${choice ? `; pull down for ${choice.name.toLowerCase()}` : ''}`}
          onPointerDown={onDown}
          onPointerMove={onMove}
          onPointerUp={(event) => onUp(event)}
          onPointerCancel={(event) => onUp(event, true)}
          onKeyDown={(event) => {
            if (event.key === 'ArrowLeft' && here > 0) goTab(TABS[here - 1]);
            if (event.key === 'ArrowRight' && here < TABS.length - 1) goTab(TABS[here + 1]);
          }}
          onClick={(event) => {
            // a key press (the pointer's own taps are handled on letting go)
            if (event.detail === 0) (open ? closeLayers : back && !me ? back.onBack : openLayers)();
          }}
        >
          {/* its glass and the still plate of its shadow (folded, it draws in behind the handle) */}
          <span ref={glassRef} className="fm-dock-glass" aria-hidden="true" />
          {pullable ? <span className="fm-pull-grip" aria-hidden="true" /> : null}
          {/* the hero: the rail's circle for who you're on, its ring at rest and the one a swipe draws round over it; folded,
              the handle, on a glass of its own */}
          <span ref={faceRef} className="fm-dock-face">
            <span ref={padRef} className="fm-dock-pad" aria-hidden="true" />
            <span key={back ? `back:${pickKey}` : pickKey} className="fm-dock-hero">
              <Plate lit marked={false} armed={false} reduce={reduce}>
                <span className="fm-dock-story">
                  <StoryRing on reduce={reduce} />
                </span>
                <span className="fm-dock-ring" aria-hidden="true">
                  <span className="fm-dock-ring-half">
                    <span
                      ref={(node) => {
                        ring.current.right = node;
                      }}
                      className="fm-dock-ring-arc"
                    >
                      <svg viewBox="0 0 100 100">
                        <path d="M 50 5 A 45 45 0 0 1 50 95" />
                      </svg>
                    </span>
                  </span>
                  <span className="fm-dock-ring-half is-left">
                    <span
                      ref={(node) => {
                        ring.current.left = node;
                      }}
                      className="fm-dock-ring-arc"
                    >
                      <svg viewBox="0 0 100 100">
                        <path d="M 50 95 A 45 45 0 0 1 50 5" />
                      </svg>
                    </span>
                  </span>
                  <span
                    ref={(node) => {
                      ring.current.start = node;
                    }}
                    className="fm-dock-ring-cap"
                  />
                  <span
                    ref={(node) => {
                      ring.current.lead = node;
                    }}
                    className="fm-dock-ring-lead"
                  >
                    <span
                      ref={(node) => {
                        ring.current.end = node;
                      }}
                      className="fm-dock-ring-cap"
                    />
                  </span>
                </span>
                {/* eslint-disable-next-line @next/next/no-img-element -- a picture drawn on this page */}
                {back?.art ? <img className="fm-dock-back" src={back.art} alt="" draggable={false} /> : <RailFace face={heroArt} reduce={reduce} pressable={false} />}
              </Plate>
            </span>
          </span>
          <span ref={restRef} className="fm-dock-rest">
            {/* the line: who you're on and where (each rolls in afresh as it changes) */}
            <span className="fm-dock-text">
              <b key={line}>{line}</b>
              {sub ? <span key={sub}>{sub}</span> : null}
            </span>
            {/* the tab you're on: every tab's icon in its disc, the one you're on shown (or the one a closed ring turned it
                into) */}
            <span className="fm-dock-tab" aria-hidden="true">
              {TABS.map((one) => {
                const Icon = ICON[one];
                return <Icon key={one} data-icon={one} className={one === tab ? 'is-on' : undefined} strokeWidth={2.4} />;
              })}
            </span>
          </span>
        </button>
      </div>
    </>
  );
}
