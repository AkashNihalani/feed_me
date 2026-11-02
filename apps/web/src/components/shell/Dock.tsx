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
   track and is drawn round again as the thumb goes, the page, a
   card now, eases back a touch toward the thumb's side
   (lib/tabPush; it was copied the moment the finger landed, so
   the swipe itself has nothing heavy to do), and over the dock,
   clear of the thumb, the three tabs show with a mark sliding
   toward the one it heads for: all on the one progress, the
   ring's. The ring drawn all the way round is the switch: the hero
   takes it with a press, the icon turns into the next tab's, the
   card lifts on while the next tab is made under it, then slides
   away, uncovering it, and the tabs over the dock sink as it goes.
   Let go short and it all settles back.

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
import { leaveTab } from '@/lib/tabSheet';
import { ms } from '@/lib/tempo';
import './dock.css';

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

// (`ring`: how far round a sideways swipe has drawn the hero's ring, 0 → 1)
type Gesture = { id: number; x0: number; y0: number; mode: null | 'pull' | 'swipe'; wasOpen: boolean; locked: boolean; lastX: number; lastT: number; v: number; ring: number };
type RingParts = { right: HTMLElement | null; left: HTMLElement | null; lead: HTMLElement | null; start: HTMLElement | null; end: HTMLElement | null };

// the swipe's marks: the dock plain again with none of them
const RING_KEYS = ['swiping', 'ringDir', 'ringTo', 'ringShut'] as const;
function clearRing(bar: HTMLElement) {
  RING_KEYS.forEach((key) => delete bar.dataset[key]);
}

export default function Dock({ tab }: { tab: DockTab }) {
  const shown = useShownHeader(tab);
  const wide = useWideChrome();
  const reduce = Boolean(useReducedMotion());
  const fresh = useFreshHandles();
  const config = shown?.config ?? null;
  const [open, setOpen] = useState(false);

  const rootRef = useRef<HTMLDivElement | null>(null);
  const frostRef = useRef<HTMLDivElement | null>(null);
  const barRef = useRef<HTMLButtonElement | null>(null);
  const faceRef = useRef<HTMLSpanElement | null>(null);
  // the bar's glass, the handle's own (folded), and what the bar carries beside the handle (the line, the tab's disc)
  const glassRef = useRef<HTMLSpanElement | null>(null);
  const padRef = useRef<HTMLSpanElement | null>(null);
  const restRef = useRef<HTMLSpanElement | null>(null);
  const rowRef = useRef<HTMLDivElement | null>(null);
  const trackRef = useRef<HTMLDivElement | null>(null);
  const hudRef = useRef<HTMLDivElement | null>(null);
  const markRef = useRef<HTMLElement | null>(null);
  const litRefs = useRef<Partial<Record<DockTab, HTMLElement | null>>>({});
  const ring = useRef<RingParts>({ right: null, left: null, lead: null, start: null, end: null });

  // what moves every frame, kept out of React
  const motion = useRef({ p: 0, raf: 0 });
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
     (none past the ends: the thumb then only `nudge`s the mark), whether the thumb is still on it, and where the tab
     names sit over the dock (their middles, measured as a swipe starts) */
  const swipe = useRef({ shown: 0, target: 0, way: 1 as 1 | -1, from: 'feed' as DockTab, to: null as DockTab | null, nudge: 0, held: false, raf: 0, last: 0, marks: null as Record<DockTab, number> | null });
  const laterTimer = useRef(0);
  // the tab you're on, for what settles after a switch has already changed it
  const tabRef = useRef(tab);
  // folded into the handle (the page scrolled down), and how far the page has gone one way since it last turned
  const foldedRef = useRef(false);
  const scrolled = useRef({ y: 0, travel: 0 });

  const faces = config ? railOf(config) : [];
  const { activeFeed, pickedFeeder, pickKey } = whoOf(config?.feeds ?? null, config?.feedId, config?.handle ?? null);
  const pickIndex = faces.findIndex((face) => face.key === pickKey);
  const choice = config?.choice ?? null;
  const here = TABS.indexOf(tab);
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
    if (frost) {
      frost.style.opacity = clamp(p * 1.5, 0, 1).toFixed(3);
      // only drawn while there is something to frost for
      if (p > 0.001) frost.dataset.live = '';
      else delete frost.dataset.live;
    }
    drawFaces();
  }, [drawFaces]);

  /* the layers rising or sinking on their own. From rest (a tap, a pick, a scroll) on one even curve whose way back is
     the mirror of its way out, so they go up at the pace they come down; let go mid-pull, on from where the finger left
     them, settling. Part of the way takes that part of the time */
  const glideTo = useCallback((target: number, fromFinger = false) => {
    const m = motion.current;
    cancelAnimationFrame(m.raf);
    m.raf = 0;
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

  /* ── the swipe, as far as it has got: the hero's ring drawn round (the rail's ring, its halves turning into view
     behind windows: transforms alone), and over the dock the mark on its way from the tab you're on to the one the
     thumb heads for, their names trading places ── */
  const paintRing = useCallback(() => {
    const { shown, way, from, to, nudge, marks } = swipe.current;
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
    // the tabs over the dock come up with the thumb's first move, and go as the ring unwinds
    const hud = hudRef.current;
    if (hud) {
      const seen = clamp((to ? shown : nudge) * 4, 0, 1);
      hud.style.opacity = seen.toFixed(3);
      hud.style.transform = `translate3d(0, ${((1 - seen) * 6).toFixed(1)}px, 0)`;
    }
    const mark = markRef.current;
    if (mark && marks) {
      const x = to ? marks[from] + (marks[to] - marks[from]) * shown : marks[from] + way * 9 * nudge;
      mark.style.transform = `translate3d(${x.toFixed(1)}px, 0, 0)`;
    }
    TABS.forEach((one) => {
      const lit = litRefs.current[one];
      if (lit) lit.style.opacity = (one === from ? 1 - (to ? shown : 0) : one === to ? shown : 0).toFixed(3);
    });
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
      paintRing();
      if (s.shown !== s.target) {
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
    glideTo(1, fromFinger);
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

  // the tabs' mark on the tab you're on, measured once the dock is drawn, for the next swipe to set off from
  const mounted = Boolean(config) && !wide;
  useIsoLayoutEffect(() => {
    tabRef.current = tab;
    const hud = hudRef.current;
    const s = swipe.current;
    if (!hud || s.held || s.to) return;
    const marks = {} as Record<DockTab, number>;
    hud.querySelectorAll<HTMLElement>('[data-hud]').forEach((el) => {
      marks[el.dataset.hud as DockTab] = el.offsetLeft + el.offsetWidth / 2;
    });
    s.marks = marks;
    s.from = tab;
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
  }, [tab, fold]);

  /* the page scrolling: down a way, the bar folds; back up a way, or at the page's top or foot, it opens. A passive
     listener and an attribute: nothing renders while the page scrolls */
  useEffect(() => {
    if (!mounted) return undefined;
    const onScroll = () => {
      const s = scrolled.current;
      const y = window.scrollY;
      const dy = y - s.y;
      s.y = y;
      // (the layers up, or a swipe on the dock: they have it)
      if (motion.current.p > 0 || swipe.current.held) return;
      const foot = document.documentElement.scrollHeight - window.innerHeight;
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
    window.addEventListener('scroll', onScroll, { passive: true });
    return () => window.removeEventListener('scroll', onScroll);
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
    cancelAnimationFrame(row.current.raf);
    cancelAnimationFrame(swipe.current.raf);
    window.clearTimeout(laterTimer.current);
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
    const mark = markRef.current;
    const was = mark?.style.transform ?? '';
    cancelAnimationFrame(s.raf);
    Object.assign(s, { raf: 0, held: false, shown: 1, target: 1, to, nudge: 0 });
    paintRing();
    // (a tab's name tapped: the mark slides there, as the thumb would have taken it)
    if (mark && was && was !== mark.style.transform && !reducedMotion()) {
      mark.animate([{ transform: was }, { transform: mark.style.transform }], { duration: ms(340), easing: CARRY_EASE_CSS });
    }
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

  // the card under way: the tabs over the dock sink, and the dock is plain again (the ring at rest is the hero's own,
  // drawn whole under this one: nothing else on screen changes). A swipe begun meanwhile keeps what it has drawn
  const sinkHud = () => {
    const settle = () => {
      const s = swipe.current;
      if (s.held) return;
      s.from = tabRef.current;
      s.to = null;
      s.shown = s.target = 0;
      paintRing();
      if (barRef.current) clearRing(barRef.current);
    };
    const hud = hudRef.current;
    if (!hud) return settle();
    const out = hud.animate(
      [{ opacity: Number(hud.style.opacity || 1), transform: hud.style.transform || 'none' }, { opacity: 0, transform: 'translate3d(0, 6px, 0)' }],
      { duration: HUD_OUT_MS, easing: CARRY_EASE_CSS, fill: 'forwards' },
    );
    const done = () => {
      out.cancel();
      settle();
    };
    out.finished.then(done, done);
  };

  /* ── going to a tab: the page is sent on its way (lib/tabPush; a closed ring has sent it already), then the URL
     changes in place, as the bar's did ── */
  const goTab = (next: DockTab, sent = false) => {
    if (next === tab) return;
    const dir = TABS.indexOf(next) > here ? 1 : -1;
    // (a move of the page's own lands first, as from the side rail: Feed's)
    void leaveTab(tab, next, dir);
    if (!sent && !reducedMotion()) leavePush(tab, dir);
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
    const sent = !reducedMotion();
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
    if (key.startsWith('tab:')) {
      const next = key.slice(4) as DockTab;
      if (next === tab) return;
      closeLayers();
      later(() => goTab(next), APPLY_MS);
      return;
    }
    if (key.startsWith('choice:')) {
      if (!choice) return;
      const value = choice.options[Number(key.slice(7))];
      if (value === choice.value) return;
      playHaptic('pick');
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

  /* ── the dock's own gestures: sideways for the next tab, up for the layers, a tap to open them ── */
  const onDown = (event: ReactPointerEvent<HTMLElement>) => {
    if (event.button !== 0) return;
    event.currentTarget.setPointerCapture(event.pointerId);
    gesture.current = { id: event.pointerId, x0: event.clientX, y0: event.clientY, mode: null, wasOpen: open, locked: false, lastX: event.clientX, lastT: event.timeStamp, v: 0, ring: 0 };
    // the page copied now, while the finger is still: a swipe that follows has nothing heavy to do (lib/tabPush)
    if (!open && !reducedMotion()) holdPush(tab);
  };

  // a swipe begun: the ring dims to a track, and the tabs rise over the dock, the one you're on lit
  const startSwipe = () => {
    // (the last switch's tabs, still sinking, give way)
    hudRef.current?.getAnimations().forEach((animation) => animation.cancel());
    const s = swipe.current;
    s.held = true;
    s.from = tab;
    const hud = hudRef.current;
    if (hud) {
      const marks = {} as Record<DockTab, number>;
      hud.querySelectorAll<HTMLElement>('[data-hud]').forEach((el) => {
        marks[el.dataset.hud as DockTab] = el.offsetLeft + el.offsetWidth / 2;
      });
      s.marks = marks;
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
          readyLayers();
        }
      }
      barRef.current?.classList.add('is-dragging');
    }
    if (g.mode === 'pull') {
      if (!g.wasOpen) {
        const raw = Math.max(0, dy) / PULL;
        if (raw >= 0.92 && !g.locked) {
          g.locked = true;
          playHaptic('snapLock');
        }
        writeP(g.locked ? Math.max(1, 1 + (raw - 1) * 0.12) : raw);
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
      if (free) easeRing();
      else paintRing();
    }
  };

  const onUp = (event: ReactPointerEvent<HTMLElement>, cancelled = false) => {
    const g = gesture.current;
    if (!g || event.pointerId !== g.id) return;
    gesture.current = null;
    barRef.current?.classList.remove('is-dragging');
    const dx = event.clientX - g.x0;
    if (row.current.edge || row.current.raf) settleRow(0);

    if (g.mode === 'swipe') {
      // let go short of closing the ring: a flick on the way closes it anyway, and anything less unwinds it
      const next = here + (dx < 0 ? 1 : -1);
      if (!cancelled && next >= 0 && next < TABS.length && dx * g.v > 0 && Math.abs(g.v) > FLICK) {
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
      if (motion.current.p > 0.32) openLayers(true);
      else closeLayers(true);
      return;
    }
    // a tap
    if (open) return closeLayers();
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
  const sub = [where, choice ? choice.label(choice.value) : null].filter(Boolean).join(' · ');

  return (
    <>
      {/* the page frosted under the layers: a tap on it, outside them, shuts them */}
      <div ref={frostRef} aria-hidden="true" className="fm-dock-frost" data-open={open ? '' : undefined} onClick={(event) => {
        if (!isOpenTap(event)) closeLayers();
      }} />
      <div ref={rootRef} className="fm-dock" data-open={open ? '' : undefined} style={{ '--fm-dock-step': `${STEP}px` } as CSSProperties}>
        <div className="fm-dock-layers" role="dialog" aria-label="Tabs, feeders and choices" aria-hidden={!open} onClick={onLayerClick}>
          {/* the tabs, rising over the row: the one you're on first, the others a beat behind */}
          <div className="fm-dock-words">
            {TABS.map((one, index) => (
              <button
                key={one}
                type="button"
                data-pick={`tab:${one}`}
                className={one === tab ? 'is-on' : undefined}
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
        <div ref={hudRef} className="fm-dock-hud" aria-hidden="true">
          {TABS.map((one) => (
            <span key={one} data-hud={one}>
              {NAME[one]}
              <b
                ref={(node) => {
                  litRefs.current[one] = node;
                }}
                style={{ opacity: one === tab ? 1 : 0 }}
              >
                {NAME[one]}
              </b>
            </span>
          ))}
          <i ref={markRef} />
        </div>

        <button
          ref={barRef}
          type="button"
          className="fm-dock-bar"
          aria-expanded={open}
          aria-label={`${heroName}${sub ? `, ${sub}` : ''}, ${NAME[tab]}. Swipe for the next tab; pull up or tap for feeders and choices`}
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
            if (event.detail === 0) (open ? closeLayers : openLayers)();
          }}
        >
          {/* its glass and the still plate of its shadow (folded, it draws in behind the handle) */}
          <span ref={glassRef} className="fm-dock-glass" aria-hidden="true" />
          {/* the hero: the rail's circle for who you're on, its ring at rest and the one a swipe draws round over it; folded,
              the handle, on a glass of its own */}
          <span ref={faceRef} className="fm-dock-face">
            <span ref={padRef} className="fm-dock-pad" aria-hidden="true" />
            <span key={pickKey} className="fm-dock-hero">
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
                <RailFace face={heroArt} reduce={reduce} pressable={false} />
              </Plate>
            </span>
          </span>
          <span ref={restRef} className="fm-dock-rest">
            {/* the line: who you're on and where (each rolls in afresh as it changes) */}
            <span className="fm-dock-text">
              <b key={heroName}>{heroName}</b>
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
