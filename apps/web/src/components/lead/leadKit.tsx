'use client';

/* What Lead's bands share: media that never swaps in one frame, words that rise in and lift out, the scrub that
   reads a chart under a finger or a pointer, a curve that bends to its new shape instead of being redrawn, and the
   band's head (eyebrow, title, the number it leads with). Motion is transform and opacity on the page beat
   (lib/motion); the curve is the one exception, written straight to its path off React's render. */

import { CSSProperties, ReactNode, RefObject, startTransition, useCallback, useEffect, useLayoutEffect, useRef, useState, useSyncExternalStore } from 'react';
import { useAppHaptics } from '@/lib/haptics';
import { shade } from '@/components/read/readShade';
import type { Shot } from './leadModels';
import { cn } from '@/lib/utils';
import { CARRY_EASE_CSS, CARRY_MS, carryCurve, reducedMotion, releaseFlight, run, startFlight } from '@/lib/feedStream/motion';
import { ms } from '@/lib/tempo';
import { FLIGHT_HOLD_MS, afterDrawn, holdsFlights } from '@/lib/webkitMotion';
import { LEDGER_DEAL_MS } from '@/components/motion/Ledger';

/* ── numbers and dates ─────────────────────────────────────────────────── */

export function compact(value: number, digits = 1) {
  const absolute = Math.abs(value);
  const sign = value < 0 ? '−' : '';
  const trim = (text: string) => text.replace(/\.0+$/, '').replace(/(\.\d*[1-9])0+$/, '$1');
  if (absolute >= 1_000_000) return `${sign}${trim((absolute / 1_000_000).toFixed(absolute >= 10_000_000 ? 1 : 2))}M`;
  if (absolute >= 1_000) return `${sign}${trim((absolute / 1_000).toFixed(absolute >= 100_000 ? 0 : digits))}K`;
  return `${sign}${Math.round(absolute)}`;
}

export function signed(value: number | null) {
  if (value == null) return '—';
  if (Math.round(value) === 0) return '0';
  return value > 0 ? `+${compact(value)}` : compact(value);
}

export function topLabel(value: number | null) {
  return value == null ? '—' : String(Math.max(1, Math.round(value)));
}

const dayFormat = new Intl.DateTimeFormat('en-GB', { weekday: 'short', day: 'numeric', month: 'short', timeZone: 'Asia/Kolkata' });
const shortFormat = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', timeZone: 'Asia/Kolkata' });

export function dayLabel(key: string) {
  return dayFormat.format(new Date(`${key}T12:00:00+05:30`)).replace(',', '');
}

export function shortDay(key: string) {
  return shortFormat.format(new Date(`${key}T12:00:00+05:30`));
}

const timeFormat = new Intl.DateTimeFormat('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'Asia/Kolkata' });
const momentFormat = new Intl.DateTimeFormat('en-GB', { weekday: 'short', day: 'numeric', month: 'short', timeZone: 'Asia/Kolkata' });

/* a post's time on the IST clock ("2:14 PM"), and with its day ("Thu 8 Oct · 2:14 PM") */
export function timeLabel(ms: number) {
  return timeFormat.format(new Date(ms));
}

export function momentLabel(ms: number) {
  return `${momentFormat.format(new Date(ms)).replace(',', '')} · ${timeLabel(ms)}`;
}

export function hoursLabel(hours: number | null) {
  if (hours == null) return '—';
  if (hours < 1) return `${Math.max(1, Math.round(hours * 60))}m`;
  if (hours < 48) return `${hours < 10 ? hours.toFixed(1).replace(/\.0$/, '') : Math.round(hours)}h`;
  return `${(hours / 24).toFixed(hours < 240 ? 1 : 0).replace(/\.0$/, '')}d`;
}

export function blockLabel(block: number, long = false) {
  const hour = block * 3;
  const name = (h: number) => {
    const twelve = h % 12 === 0 ? 12 : h % 12;
    return `${twelve}${long ? (h < 12 || h === 24 ? ' AM' : ' PM') : h < 12 || h === 24 ? 'a' : 'p'}`;
  };
  if (!long) return name(hour);
  const end = hour + 3;
  const sameHalf = (hour < 12) === (end < 12 || end === 24) && end !== 12;
  return sameHalf ? `${name(hour).replace(/ (AM|PM)$/, '')}–${name(end)}` : `${name(hour)}–${name(end)}`;
}

/* ── media ─────────────────────────────────────────────────────────────── */

/* The incoming picture develops over the outgoing one inside its fixed frame: it is decoded off the main thread
   first, then fades in on the compositor (CSS opacity, no script per frame); the one under it stays until the next
   change, or fades out when no picture replaces it. A picture the proxy can't find (not captured, link expired) leaves the frame's own ground showing.
   `settle` adds the large-picture grammar: the new one also eases down from a touch larger. */
// pictures the proxy couldn't find, for the life of the page: no frame asks for one again
const missing = new Set<string>();

export function CrossfadeImage({ src, className, eager = false, settle = false }: { src: string | null; className?: string; eager?: boolean; settle?: boolean }) {
  const [failed, setFailed] = useState<string | null>(null);
  const live = src && src !== failed && !missing.has(src) ? src : null;
  // the picture this one replaces, kept as state set while rendering (it commits with the new one, not after)
  const [pair, setPair] = useState<{ current: string | null; previous: string | null }>({ current: live, previous: null });
  if (pair.current !== live) setPair({ current: live, previous: pair.current });
  const under = pair.previous && pair.previous !== live ? pair.previous : null;
  const img = cn('lead-img absolute inset-0 h-full w-full object-cover', settle && 'lead-img--settle');
  // both pictures are keyed by their address: the one leaving keeps its element (and its developed state) underneath
  return (
    <span aria-hidden="true" className={cn('pointer-events-none absolute inset-0 block overflow-hidden', className)}>
      {under ? (
        // eslint-disable-next-line @next/next/no-img-element -- authenticated media proxy URL
        <img key={under} src={under} alt="" ref={live ? develop : fadeOut} className={img} />
      ) : null}
      {live ? (
        // eslint-disable-next-line @next/next/no-img-element -- authenticated media proxy URL
        <img
          key={live}
          src={live}
          alt=""
          ref={develop}
          loading={eager ? 'eager' : 'lazy'}
          decoding="async"
          onLoad={(event) => develop(event.currentTarget)}
          onError={() => {
            missing.add(live);
            setFailed(live);
          }}
          className={img}
        />
      ) : null}
    </span>
  );
}

/* A picture shows once it is decoded (off the main thread), by a mark on the element itself: its arrival renders
   nothing, and the fade is a CSS transition on the compositor. */
function develop(img: HTMLImageElement | null) {
  if (!img) return;
  delete img.dataset.leaving;
  if (img.dataset.shown != null || !img.complete || !img.naturalWidth) return;
  (img.decode ? img.decode() : Promise.resolve()).catch(() => undefined).then(() => {
    const show = () => { if (img.dataset.leaving == null) img.dataset.shown = ''; };
    // on WebKit the fade waits for the frame that draws it (a cached picture decodes inside a switch's heavy frame)
    if (holdsFlights()) afterDrawn(show);
    else show();
  });
}

/* a picture leaving with nothing coming in fades out, so the frame's own ground shows instead of the last scope's post */
function fadeOut(img: HTMLImageElement | null) {
  if (!img) return;
  img.dataset.leaving = '';
  delete img.dataset.shown;
}

/* a post as the bands show it: always 4:5, its Top % in the ramp's shade, who and when underneath */
export function PostTile({ shot, className, when }: { shot: Shot; className?: string; when?: string }) {
  const percentile = shot.ranked?.latestPercentile ?? null;
  const tone = percentile != null ? shade(percentile) : null;
  return (
    <span className={cn('relative block aspect-[4/5] overflow-hidden rounded-[14px] bg-(--fm-media)', className)}>
      <span aria-hidden="true" className="absolute inset-0 grid place-items-center text-[8px] font-black uppercase tracking-[0.12em] text-white/30">{shot.format || 'post'}</span>
      <CrossfadeImage src={shot.thumb} />
      <span aria-hidden="true" className="absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,.28)_0%,transparent_30%,transparent_52%,rgba(0,0,0,.86)_100%)]" />
      {/* the ramp's own number colour: the soft steps carry ink, the solid ones white */}
      <span className="absolute left-1.5 top-1.5 rounded-[7px] px-1.5 py-0.5 text-[9px] font-black tabular-nums" style={{ background: tone ? tone.c : 'rgba(0,0,0,.55)', color: tone ? tone.tx : '#ffffff' }}>
        {percentile != null ? `${topLabel(percentile)}%` : 'new'}
      </span>
      <span className="absolute inset-x-0 bottom-0 p-1.5 sm:p-2">
        <span className="block truncate text-[9px] font-black leading-tight text-white sm:text-[10px]">@{shot.handle}</span>
        {when ? <span className="mt-0.5 block truncate text-[8px] font-bold leading-tight text-white/64 sm:text-[9px]">{when}</span> : null}
      </span>
    </span>
  );
}

/* A short line that changes: the new words rise in (a CSS animation, on the compositor) and the old simply give
   way, so nothing is measured and nothing waits on script. The words it first showed arrive with their band. */
export function Swap({ value, className }: { value: string; className?: string }) {
  const [first] = useState(value);
  const changed = value !== first;
  return <span key={value} ref={changed ? holdArrival : undefined} className={cn('lead-swap', changed && 'lead-swap-in', className)}>{value}</span>;
}

/* on WebKit a CSS arrival waits at its start for the frame that draws it (lib/webkitMotion); a ref */
export function holdArrival(el: HTMLElement | null) {
  if (!el || !holdsFlights()) return;
  el.style.animationPlayState = 'paused';
  afterDrawn(() => {
    el.style.animationPlayState = '';
  });
}

/* A band's first view: its pieces ([class*=lead-rise], ordered by --rise) rise into place the first time it comes
   on screen, in CSS. From then on the band is "dealt": its swaps animate. */
export function useReveal<T extends Element>(amount = 0.15) {
  const [state, setState] = useState<'idle' | 'shown' | 'dealt'>('idle');
  const observer = useRef<IntersectionObserver | null>(null);
  const timer = useRef<number | null>(null);
  const ref = useCallback((el: T | null) => {
    observer.current?.disconnect();
    observer.current = null;
    if (!el) return;
    const watch = new IntersectionObserver((entries) => {
      if (!entries.some((entry) => entry.isIntersecting)) return;
      watch.disconnect();
      setState((current) => (current === 'idle' ? 'shown' : current));
      // once its last piece has risen (--rise 3: 40 + 3 × 60 + 480, at the app's tempo), and a little after
      timer.current = window.setTimeout(() => setState('dealt'), ms(700) + 150);
    }, { threshold: amount });
    watch.observe(el);
    observer.current = watch;
  }, [amount]);
  useEffect(() => () => {
    if (timer.current) window.clearTimeout(timer.current);
  }, []);
  return [ref, state] as const;
}

/* ── the band's head ───────────────────────────────────────────────────── */

export function Eyebrow({ icon, children, className }: { icon?: ReactNode; children: ReactNode; className?: string }) {
  return (
    <div className={cn('flex items-center gap-2 text-fg/44 light:text-fg/52', className)}>
      {icon}
      <span className="text-[8px] font-black uppercase tracking-[0.22em]">{children}</span>
    </div>
  );
}

export function Delta({ up, children, className }: { up: boolean | null; children: ReactNode; className?: string }) {
  return (
    // a rise: the ▲ carries the accent, the words stay ink (small type is ink)
    <span className={cn('inline-flex items-center gap-1 text-[9px] font-black uppercase tracking-[0.12em]', up ? 'text-fg' : 'text-fg/44 light:text-fg/52', className)}>
      {up == null ? null : <i className={cn('not-italic text-[7px]', up && 'text-[var(--fm-accent)]')}>{up ? '▲' : '▼'}</i>}
      {children}
    </span>
  );
}

/* ── first view ────────────────────────────────────────────────────────── */

/* ── away from the screen ──────────────────────────────────────────────── */

// Safari has no idle callback
type IdleWindow = Omit<Window, 'requestIdleCallback' | 'cancelIdleCallback'> & { requestIdleCallback?: (fn: (deadline: IdleDeadline) => void, options?: { timeout: number }) => number; cancelIdleCallback?: (id: number) => void };

/* Small jobs run while nothing else is: they start once whatever just happened has had time to finish moving
   (`after`), then a few per idle moment (the browser's idle callback where it has one; elsewhere just after a frame
   has gone out), each only when there is room for it in the frame, so building ahead never takes a frame from what
   is moving. Returns a cancel. */
export function whenIdle(jobs: Array<() => unknown>, after = 900) {
  const win = window as IdleWindow;
  let index = 0;
  let stopped = false;
  let handle = 0;
  const run = (deadline: IdleDeadline | null) => {
    if (stopped) return;
    const until = performance.now() + 5;
    let ran = 0;
    while (index < jobs.length) {
      // the first job of a slot that waited too long runs anyway; every other one needs the room
      const room = deadline ? deadline.timeRemaining() > 8 || (deadline.didTimeout && ran === 0) : ran === 0 || performance.now() < until;
      if (!room) break;
      jobs[index]();
      index += 1;
      ran += 1;
    }
    schedule();
  };
  const schedule = () => {
    if (stopped || index >= jobs.length) return;
    if (win.requestIdleCallback) handle = win.requestIdleCallback(run, { timeout: 4000 });
    else handle = window.setTimeout(() => window.requestAnimationFrame(() => window.setTimeout(() => run(null), 0)), 80);
  };
  const start = window.setTimeout(schedule, after);
  return () => {
    stopped = true;
    window.clearTimeout(start);
    if (win.requestIdleCallback) win.cancelIdleCallback?.(handle);
    else window.clearTimeout(handle);
  };
}

/* A band takes a switch in the same render as the rest of the page while any of it is on screen: everything on
   screen changes in one commit, and the motion that follows (held on WebKit until that frame is shown, see
   lib/webkitMotion) plays with nothing else committing under it. Off screen it keeps what it shows, and takes the
   switch once that motion has played and the page is idle, or the moment it comes on screen. Scrolling past a band
   with nothing new costs nothing. Returns the band's ref and its props. */
export function useHeldNear<T>(value: T) {
  const [held, setHeld] = useState(value);
  const [seen, setSeen] = useState(true);
  // on screen it shows the latest at once (and keeps it, for when it leaves the screen)
  if (seen && held !== value) setHeld(value);

  useEffect(() => {
    if (seen || held === value) return undefined;
    const take = () => startTransition(() => setHeld(value));
    const win = window as IdleWindow;
    let idle = 0;
    // after the switch's motion on screen has played (the Feederboard's deal is the longest)
    const settled = window.setTimeout(() => {
      if (win.requestIdleCallback) idle = win.requestIdleCallback(take, { timeout: 4000 });
      else idle = window.setTimeout(take, 0);
    }, LEDGER_DEAL_MS + 100);
    return () => {
      window.clearTimeout(settled);
      if (win.requestIdleCallback) win.cancelIdleCallback?.(idle);
      else window.clearTimeout(idle);
    };
  }, [held, seen, value]);

  const observer = useRef<IntersectionObserver | null>(null);
  const ref = useCallback((el: Element | null) => {
    observer.current?.disconnect();
    observer.current = null;
    if (!el) return;
    const watch = new IntersectionObserver((entries) => {
      setSeen(entries.some((entry) => entry.isIntersecting));
    });
    watch.observe(el);
    observer.current = watch;
  }, []);

  return [ref, seen ? value : held] as const;
}

/* ── the pick ──────────────────────────────────────────────────────────── */

/* what a reader has picked on a chart: hovered (lets go when the mouse leaves) or pinned (stays). Both belong to the
   window and scope they were picked in, so a new deal starts clean without an effect resetting them. */
export function usePick<T>(dealKey: string) {
  const [state, setState] = useState<{ deal: string; hover: T | null; pinned: T | null }>({ deal: dealKey, hover: null, pinned: null });
  const live = state.deal === dealKey ? state : { deal: dealKey, hover: null, pinned: null };
  const set = useCallback((key: 'hover' | 'pinned', value: T | null | ((current: T | null) => T | null)) => {
    setState((current) => {
      const base = current.deal === dealKey ? current : { deal: dealKey, hover: null, pinned: null };
      const next = typeof value === 'function' ? (value as (current: T | null) => T | null)(base[key]) : value;
      return { ...base, [key]: next };
    });
  }, [dealKey]);
  const setHover = useCallback((value: T | null) => set('hover', value), [set]);
  const setPinned = useCallback((value: T | null | ((current: T | null) => T | null)) => set('pinned', value), [set]);
  return { hover: live.hover, pinned: live.pinned, setHover, setPinned };
}

/* ── the scrub ─────────────────────────────────────────────────────────── */

/* Reads a horizontal chart: a pointer hovering, or a finger dragging sideways (a finger moving up or down is left
   to scroll the page). Each new column ticks once under a finger. A tap picks a column; a mouse leaving lets go. */
export function useScrub(count: number, onPick: (index: number | null, mode: 'hover' | 'pin') => void, inset = 0) {
  const haptics = useAppHaptics();
  const state = useRef<{ id: number; x: number; y: number; mode: 'wait' | 'scrub' | 'scroll'; last: number | null } | null>(null);
  const lastIndex = useRef<string | null>(null);

  // the columns sit `inset` px in from each side of the element
  const indexAt = useCallback((element: HTMLElement, clientX: number) => {
    const rect = element.getBoundingClientRect();
    const t = Math.min(0.9999, Math.max(0, (clientX - rect.left - inset) / Math.max(1, rect.width - inset * 2)));
    return Math.min(count - 1, Math.floor(t * count));
  }, [count, inset]);

  // hover follows a mouse and lets go when it leaves; a click, a tap or a finger's drag pins the column
  const pick = useCallback((index: number | null, mode: 'hover' | 'pin', touch = false) => {
    const id = `${mode}:${index}`;
    if (id === lastIndex.current) return;
    lastIndex.current = id;
    if (touch && index != null) haptics.play('snapLock');
    onPick(index, mode);
  }, [haptics, onPick]);

  return {
    style: { touchAction: 'pan-y' } as CSSProperties,
    onPointerDown: (event: React.PointerEvent<HTMLElement>) => {
      // each press starts fresh: the pin may have moved since (a spike row, a switch), so the same column picks again
      lastIndex.current = null;
      if (event.pointerType === 'mouse') {
        pick(indexAt(event.currentTarget, event.clientX), 'pin');
        return;
      }
      state.current = { id: event.pointerId, x: event.clientX, y: event.clientY, mode: 'wait', last: null };
    },
    onPointerMove: (event: React.PointerEvent<HTMLElement>) => {
      if (event.pointerType === 'mouse') {
        if (event.buttons) pick(indexAt(event.currentTarget, event.clientX), 'pin');
        else pick(indexAt(event.currentTarget, event.clientX), 'hover');
        return;
      }
      const touch = state.current;
      if (!touch || touch.id !== event.pointerId) return;
      if (touch.mode === 'wait') {
        const dx = Math.abs(event.clientX - touch.x);
        const dy = Math.abs(event.clientY - touch.y);
        if (dx < 6 && dy < 6) return;
        touch.mode = dx > dy ? 'scrub' : 'scroll';
        if (touch.mode === 'scrub') {
          try {
            event.currentTarget.setPointerCapture(event.pointerId);
          } catch {
            // the finger is already gone: the move below still reads where it was
          }
        }
      }
      if (touch.mode === 'scrub') pick(indexAt(event.currentTarget, event.clientX), 'pin', true);
    },
    onPointerUp: (event: React.PointerEvent<HTMLElement>) => {
      const touch = state.current;
      if (touch && touch.id === event.pointerId && touch.mode === 'wait') pick(indexAt(event.currentTarget, event.clientX), 'pin', true);
      state.current = null;
    },
    onPointerCancel: () => {
      state.current = null;
    },
    onPointerLeave: (event: React.PointerEvent<HTMLElement>) => {
      if (event.pointerType === 'mouse') pick(null, 'hover');
    },
  };
}

/* ── wide screens ──────────────────────────────────────────────────────── */

const WIDE = '(min-width: 1024px)';
const subscribeWide = (change: () => void) => {
  const query = window.matchMedia(WIDE);
  query.addEventListener('change', change);
  return () => query.removeEventListener('change', change);
};

/* the desktop layout (Tailwind's lg): a piece with a phone and a desktop form renders only the one on screen */
export function useWide() {
  return useSyncExternalStore(subscribeWide, () => window.matchMedia(WIDE).matches, () => false);
}

/* ── rows that trade places ────────────────────────────────────────────── */

// rows set off a beat apart, top first, so a re-ranked ladder settles in order rather than all at once
const CARRY_STEP_MS = ms(30);

/* A list whose rows trade places (a new range re-ranks them, a row opening pushes the rest down): after each commit
   every row that moved is carried from where it was seen to where it now sits, on the Feed's carry curve, as a
   compositor transform (WAAPI), each a beat after the row above it. The rows' places are read once, in the frame that
   first draws them (the browser lays the page out there anyway; read in the middle of the commit it would have to
   lay it out twice), and nothing runs while they move. Rows carry their id in data-carry; `version` changes whenever
   their places may have. */
// the rows on their way: their animation, how far they set off from, and their beat
const carrying = new WeakMap<HTMLElement, { flight: Animation; from: number; delay: number }>();

export function useCarry(listRef: RefObject<HTMLElement | null>, version: unknown) {
  const places = useRef(new Map<string, number>());
  useLayoutEffect(() => {
    let release = () => {};
    const frame = window.requestAnimationFrame(() => {
      const list = listRef.current;
      if (!list) return;
      const next = new Map<string, number>();
      const moves: Array<[HTMLElement, number]> = [];
      for (const row of Array.from(list.children)) {
        if (!(row instanceof HTMLElement) || !row.dataset.carry) continue;
        // a row still on its way is carried on from where it is now (worked out from its curve, not read back)
        let offset = 0;
        const on = carrying.get(row);
        if (on && on.flight.playState === 'running') {
          const progress = (Number(on.flight.currentTime ?? 0) - on.delay) / CARRY_MS;
          offset = on.from * (1 - carryCurve(Math.max(0, Math.min(1, progress))));
          on.flight.cancel();
        }
        carrying.delete(row);
        const top = row.offsetTop;
        const was = places.current.get(row.dataset.carry);
        next.set(row.dataset.carry, top);
        if (was != null && Math.abs(was + offset - top) > 0.5) moves.push([row, was + offset - top]);
      }
      places.current = next;
      if (!moves.length || reducedMotion()) return;
      const flight = startFlight(holdsFlights() ? FLIGHT_HOLD_MS : 0);
      moves.forEach(([row, from], order) => {
        const delay = order * CARRY_STEP_MS;
        run(flight, row, [{ transform: `translate3d(0, ${from.toFixed(1)}px, 0)` }, { transform: 'translate3d(0, 0, 0)' }], { duration: CARRY_MS, easing: CARRY_EASE_CSS, delay, fill: 'backwards' });
        carrying.set(row, { flight: flight.animations[flight.animations.length - 1], from, delay: delay + flight.hold });
      });
      if (flight.hold) release = afterDrawn(() => releaseFlight(flight));
    });
    return () => {
      window.cancelAnimationFrame(frame);
      release();
    };
  }, [listRef, version]);
}
