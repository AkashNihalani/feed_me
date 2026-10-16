'use client';

/* ─────────────────────────────────────────────────────────────
   TAB HEADER — the one top section the tabs share, drawn ONCE in
   the app's glass capsule (TabHeaderHost, from AppShell) and fed
   by whichever tab is on screen (useTabHeader). Switching tabs
   never swaps it out: the same header adapts. Only what differs
   moves: the tab's name rolls letter by letter (LEAD → READ),
   the eyebrow rolls, the control on the right crosses over; the
   rail stays put, because every tab shares the pick (tabScope).

   The title row: the tab's name, a hairline, then where you are
   (an eyebrow on desktop over the scope), and the tab's 44px
   control. Under it, the story rail: the wedge (all feeds, or
   back), then your feeds; open one and its badge moves to the
   front while its feeders fly out of it. The rail folds away as
   you scroll down and comes back as you scroll up (compressed,
   from useCompressedOnScroll: 150 / 64 / 30).

   Everything moves on the page beat (lib/motion), the same clock
   as the page's pieces, so a feeder flying out and the row it
   summons land together. A tab that choreographs its own page
   change (Read) holds the header in step: while `leaving`, what
   was tapped arms at once, the ring you were on draws back and
   the scope lets go of its name; they return with the page.
   ───────────────────────────────────────────────────────────── */

import { memo, useCallback, useEffect, useId, useLayoutEffect, useRef, useState, useSyncExternalStore, type MouseEvent as ReactMouseEvent, type ReactNode, type Ref, type SyntheticEvent } from 'react';
import { createPortal } from 'react-dom';
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion';
import { ChevronDown } from 'lucide-react';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import { press } from '@/components/feed/stream/press';
import { FAN_MS, Ledger } from '@/components/motion/Ledger';
import { CARRY_EASE_CSS, carryCurve } from '@/lib/feedStream/motion';
import { afterDrawn, holdsFlights } from '@/lib/webkitMotion';
import SlotText from '@/components/SlotText';
import { feedInitials, titleCase } from '@/lib/feedLabels';
import { BEAT_EASE, BEAT_LEAVE_EASE, BEAT_LEAVE_S, HEADER_ROUTE_MORPH, beatArrive, beatLeave } from '@/lib/motion';
import { ms, sec, slowSpring } from '@/lib/tempo';
import { useResolvedTheme } from '@/lib/theme';
import { cn } from '@/lib/utils';

export const SOFT_EASE = BEAT_EASE;
export const LEAVE_EASE = [0.4, 0, 1, 1] as const;
export const SLOT_SPRING = slowSpring({ type: 'spring', stiffness: 540, damping: 42, mass: 0.7 } as const);
const REDUCED = { duration: 0.14, ease: SOFT_EASE } as const;
const RING_R = 45;
// the rail re-dealt (a feed opening or closing) is one glide, the ledger's fan (already at the app's tempo): the wedge
// turns with it
const RAIL_MOVE_MS = FAN_MS;

// a value rolling in its slot: a step forward comes up from below, a step back comes down
export const SLOT_ROLL = {
  enter: (dir: number) => ({ transform: `translate3d(0, ${dir * 112}%, 0)` }),
  center: { transform: 'translate3d(0, 0%, 0)' },
  exit: (dir: number) => ({ transform: `translate3d(0, ${dir * -112}%, 0)` }),
};

// the surface of the 44px control on the right of the title row
// (a sunken well: --fm-well* are the theme's, globals.css)
export const HEADER_CONTROL = 'relative z-10 flex h-11 shrink-0 touch-manipulation items-center justify-center gap-1 overflow-hidden rounded-[14px] border border-(--fm-well-edge) bg-(--fm-well) font-black uppercase text-fg shadow-(--fm-well-shade) outline-none focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]';

/* Every circle in the rail sits in one fixed box, the size of the one you're on, so nothing in the rail ever changes
   size: the one you're on is the plate at full size, the rest are the same plate scaled down. Every circle is built
   whole, ring and all, before anything is tapped: a pick only moves what is already there, on the compositor
   (transform and opacity: CSS transitions and WAAPI), so nothing is added to the page, laid out or painted afresh in
   the frames the motion plays in. */
const PLATE = 'relative grid h-[64px] w-[64px] place-items-center rounded-full p-1 [will-change:transform] lg:h-[76px] lg:w-[76px]';
const PLATE_LIT = 'bg-[#FFE4EA] shadow-[0_10px_26px_-20px_rgb(var(--fm-accent-rgb)/0.85)] dark:bg-[#3F0F1B]';
const PLATE_REST = 'bg-black/[0.07] dark:bg-white/[0.11]';
const PLATE_MARKED = 'bg-[linear-gradient(140deg,rgb(var(--fm-accent-rgb)/0.9),rgb(var(--fm-accent-rgb)/0.28))]';
// the plate's colour: rest, marked and lit are three layers that cross-fade (opacity) rather than one background
// repainted every frame
const PLATE_LAYER = 'pointer-events-none absolute inset-0 rounded-full';
// a resting plate: 60 of 64px on a phone, 70 of 76px on desktop
const PLATE_REST_SCALE = 0.93;
/* One clock for a pick: the plate settling, its ground, the label and the glow lighting all run this long, and the
   ring draws round on a little more of it. The curve is Feed's carry curve: moving from the first frame, at its
   fastest midway, never a jolt at the start. (A curve that does most of its travel in its first frames showed a
   late first frame as a jump, then crawled: the ring "stuttered" in.) */
const PICK_MS = ms(520);
const PICK_EASE = CARRY_EASE_CSS;
// what was tapped while its page leaves swells a touch, with a little give
const ARMED_EASE = 'cubic-bezier(0.34, 1.4, 0.64, 1)';
const pickFade = (reduce: boolean) => (reduce ? undefined : `opacity ${PICK_MS}ms ${PICK_EASE}`);
/* A tap presses the face in. Not CSS :active: on iOS it holds for the whole touch, a swipe of the rail included, so
   the circle a swipe started on shrank while the rail moved and sprang back at the lift. `press` (Feed's) presses a
   beat after a finger lands and lets go the moment the touch becomes a swipe. */
const FACE_PRESS = 'transition-transform duration-150 ease-out group-data-[pressed]:scale-[0.95]';
// a circle reached from the keys shows it on its face (inside the circle: the rail's scroll would clip a ring outside it)
const FACE_FOCUS = 'group-focus-visible:ring-2 group-focus-visible:ring-inset group-focus-visible:ring-[var(--fm-accent-bright)]';

// each circle is a layer of its own (the rail moves them), stacked first over last so a circle coming out of the one
// before it comes out from behind it
const ITEM_CLASS = 'group relative flex w-[70px] shrink-0 flex-col items-center gap-1 border-0 bg-transparent p-0 text-inherit outline-none [user-select:none] [will-change:transform] [-webkit-tap-highlight-color:transparent] lg:w-[82px] lg:gap-[5px]';
const LABEL_CLASS = 'block w-full overflow-hidden text-ellipsis whitespace-nowrap text-center text-[9px] font-black leading-none lg:text-[10px]';
const LABEL_LIT = 'text-[var(--fm-accent)] dark:text-[var(--fm-accent-bright)]';
const LABEL_REST = 'text-black/42 dark:text-white/44';

export type HeaderFeeder = { handle: string; profilePicUrl?: string | null };
export type HeaderFeed = { id: string; title: string; feeders: HeaderFeeder[] };

/* On WebKit a pick's CSS fades start their clocks in the frame that commits them, before that frame reaches the
   screen (lib/webkitMotion): after a heavy commit (Read lands its page with the pick) their start was lost. A pick's
   fades inside `ref` are held at their start and set going once the frame is on screen. */
function useHeldPick(ref: { current: HTMLElement | null }, key: unknown) {
  const first = useRef(true);
  useIsomorphicLayoutEffect(() => {
    if (first.current) {
      first.current = false;
      return undefined;
    }
    const el = ref.current;
    if (!el || !holdsFlights() || typeof CSSTransition === 'undefined') return undefined;
    // only the fades this pick started (one already under way from an earlier pick keeps going)
    const fades = el.getAnimations({ subtree: true }).filter((animation) => animation instanceof CSSTransition && (animation.pending || Number(animation.currentTime ?? 0) < 20));
    if (!fades.length) return undefined;
    fades.forEach((fade) => {
      fade.pause();
      fade.currentTime = 0;
    });
    return afterDrawn(() => fades.forEach((fade) => fade.play()));
  }, [key]);
}

/* a circle's plate: full size and lit for the one you're on, scaled down for the rest; armed (tapped, waiting for
   the page) it swells a touch */
function Plate({ lit, marked, armed, reduce, children }: { lit: boolean; marked: boolean; armed: boolean; reduce: boolean; children: ReactNode }) {
  const scale = (lit ? 1 : PLATE_REST_SCALE) * (armed && !reduce ? 1.06 : 1);
  const fade = pickFade(reduce);
  return (
    <span className={PLATE} style={{ transform: `scale(${scale})`, transition: reduce ? undefined : `transform ${PICK_MS}ms ${armed ? ARMED_EASE : PICK_EASE}` }}>
      <span aria-hidden="true" className={cn(PLATE_LAYER, PLATE_REST)} style={{ opacity: !lit && !marked ? 1 : 0, transition: fade }} />
      <span aria-hidden="true" className={cn(PLATE_LAYER, PLATE_MARKED)} style={{ opacity: !lit && marked ? 1 : 0, transition: fade }} />
      <span aria-hidden="true" className={cn(PLATE_LAYER, PLATE_LIT)} style={{ opacity: lit ? 1 : 0, transition: fade }} />
      {children}
    </span>
  );
}

/* a circle's name: grey, and rose for the one you're on — two copies cross-fading (a colour change would repaint) */
function RailLabel({ text, lit, reduce, className }: { text: string; lit: boolean; reduce: boolean; className: string }) {
  const fade = pickFade(reduce);
  return (
    <span className="grid w-full">
      <span className={cn(LABEL_CLASS, className, LABEL_REST, '[grid-area:1/1]')} style={{ opacity: lit ? 0 : 1, transition: fade }}>{text}</span>
      <span aria-hidden="true" className={cn(LABEL_CLASS, className, LABEL_LIT, '[grid-area:1/1]')} style={{ opacity: lit ? 1 : 0, transition: fade }}>{text}</span>
    </span>
  );
}

/* ── the ring, drawn on the compositor ──────────────────────────────────────
   An SVG stroke drawing itself on (its dash offset redrawn every frame) is main-thread paint: it lags whenever the
   page is busy. So the ring is built from parts that only ever rotate: two half-rings, each turning into its own
   half of the circle behind a window that shows only that half, and a dot at each end for the round caps. Every
   moving part is a rotation (or a cap's opacity) the browser runs on the compositor (WAAPI). Every circle has its
   ring, drawn back, from the start: a pick sweeps it round (or back) and adds nothing to the page. */

function cubicBezier(x1: number, y1: number, x2: number, y2: number) {
  const along = (t: number, a: number, b: number) => 3 * (1 - t) * (1 - t) * t * a + 3 * (1 - t) * t * t * b + t * t * t;
  return (x: number) => {
    let lo = 0;
    let hi = 1;
    for (let k = 0; k < 24; k += 1) {
      const mid = (lo + hi) / 2;
      if (along(mid, x1, x2) < x) lo = mid;
      else hi = mid;
    }
    return along((lo + hi) / 2, y1, y2);
  };
}

const RING_SAMPLES = 36;
// the ring sweeps round on the carry curve, a touch longer than the rest of the pick; it draws back quicker, evenly
const RING_DRAW_MS = ms(600);
const RING_UNDRAW_MS = ms(380);

function ringFrames(angleAt: (t: number) => number) {
  const first: Keyframe[] = [];
  const second: Keyframe[] = [];
  const lead: Keyframe[] = [];
  for (let k = 0; k <= RING_SAMPLES; k += 1) {
    const offset = k / RING_SAMPLES;
    const angle = angleAt(offset);
    first.push({ offset, transform: `rotate(${-180 + Math.min(angle, 180)}deg)` });
    second.push({ offset, transform: `rotate(${-180 + Math.max(angle - 180, 0)}deg)` });
    lead.push({ offset, transform: `rotate(${angle}deg)` });
  }
  return { first, second, lead };
}

const undrawCurve = cubicBezier(0.4, 0, 0.6, 1);
const RING_DRAW = ringFrames((t) => 360 * carryCurve(t));
const RING_UNDRAW = ringFrames((t) => 360 * (1 - undrawCurve(t)));
// the ring at rest, drawn round or drawn back (what each part shows once a sweep is over)
const RING_POSE = {
  on: { first: 'rotate(0deg)', second: 'rotate(0deg)', lead: 'rotate(360deg)', caps: 1 },
  off: { first: 'rotate(-180deg)', second: 'rotate(-180deg)', lead: 'rotate(0deg)', caps: 0 },
};
const RING_ARC = 'block h-full w-full';
const RING_CAP = 'absolute left-1/2 top-[1.25%] h-[7.5%] w-[7.5%] -translate-x-1/2 rounded-full bg-[var(--fm-accent)]';

function RingDraw({ on, reduce }: { on: boolean; reduce: boolean }) {
  const first = useRef<HTMLSpanElement | null>(null);
  const second = useRef<HTMLSpanElement | null>(null);
  const lead = useRef<HTMLSpanElement | null>(null);
  const start = useRef<HTMLSpanElement | null>(null);
  const end = useRef<HTMLSpanElement | null>(null);
  // the sweep it started last (kept here: asking the parts for their animations would restyle mid-switch)
  const drawing = useRef<Animation[]>([]);
  const shown = useRef(on);

  useIsomorphicLayoutEffect(() => {
    if (shown.current === on) return undefined;
    shown.current = on;
    const parts = [first.current, second.current, lead.current, start.current, end.current];
    if (parts.some((part) => !part)) return undefined;
    drawing.current.forEach((animation) => animation.cancel());
    drawing.current = [];
    // reduced motion: the ring is simply there or not (its parts take their pose)
    if (reduce) return undefined;
    const frames = on ? RING_DRAW : RING_UNDRAW;
    // each part rests in its new pose (its style): the sweep only fills in until it gets there
    const timing: KeyframeAnimationOptions = { duration: on ? RING_DRAW_MS : RING_UNDRAW_MS, easing: 'linear', fill: 'backwards' };
    const caps: Keyframe[] = on
      ? [{ opacity: 0 }, { opacity: 1, offset: 0.04 }, { opacity: 1 }]
      : [{ opacity: 1 }, { opacity: 1, offset: 0.9 }, { opacity: 0 }];
    drawing.current = [
      first.current!.animate(frames.first, timing),
      second.current!.animate(frames.second, timing),
      lead.current!.animate(frames.lead, timing),
      start.current!.animate(caps, timing),
      end.current!.animate(caps, timing),
    ];
    // on WebKit the sweep waits at its start for the frame that draws it (a heavy one, on Read)
    if (!holdsFlights()) return undefined;
    const sweep = drawing.current;
    sweep.forEach((animation) => animation.pause());
    return afterDrawn(() => sweep.forEach((animation) => {
      if (animation.playState === 'paused') animation.play();
    }));
  }, [on, reduce]);

  const pose = on ? RING_POSE.on : RING_POSE.off;
  return (
    <>
      {/* the faint track the ring draws along: there while the ring is */}
      <svg className="absolute inset-0 block h-full w-full" viewBox="0 0 100 100" style={{ opacity: on ? 1 : 0, transition: reduce ? undefined : `opacity ${PICK_MS}ms ${PICK_EASE}` }}>
        <circle cx="50" cy="50" r={RING_R} fill="none" stroke="rgb(var(--fm-accent-rgb)/0.22)" strokeWidth="7.5" />
      </svg>
      {/* the right half: a half-ring turning into view, top round to bottom */}
      <span className="absolute inset-y-0 right-0 w-1/2 overflow-hidden">
        <span ref={first} className="absolute inset-y-0 right-0 w-[200%] origin-center" style={{ transform: pose.first }}>
          <svg className={RING_ARC} viewBox="0 0 100 100">
            <path d="M 50 5 A 45 45 0 0 1 50 95" fill="none" stroke="var(--fm-accent)" strokeWidth="7.5" />
          </svg>
        </span>
      </span>
      {/* the left half: bottom round to top */}
      <span className="absolute inset-y-0 left-0 w-1/2 overflow-hidden">
        <span ref={second} className="absolute inset-y-0 left-0 w-[200%] origin-center" style={{ transform: pose.second }}>
          <svg className={RING_ARC} viewBox="0 0 100 100">
            <path d="M 50 95 A 45 45 0 0 1 50 5" fill="none" stroke="var(--fm-accent)" strokeWidth="7.5" />
          </svg>
        </span>
      </span>
      {/* the round caps: one where the ring starts, one riding its leading end */}
      <span ref={start} className={RING_CAP} style={{ opacity: pose.caps }} />
      <span ref={lead} className="absolute inset-0 origin-center" style={{ transform: pose.lead }}>
        <span ref={end} className={RING_CAP} style={{ opacity: pose.caps }} />
      </span>
    </>
  );
}

/* the rose ring round what you're on: it sweeps round when you pick it and back when you pick something else (the
   old one drawing back as the new one draws on), and its soft glow comes up and goes with it, on the pick's clock */
function StoryRing({ on, reduce, glow = true }: { on: boolean; reduce: boolean; glow?: boolean }) {
  return (
    <>
      {glow ? (
        <span
          aria-hidden="true"
          className="pointer-events-none absolute inset-[2px] rounded-full shadow-[0_3px_8px_-1px_rgb(var(--fm-accent-rgb)/0.42)]"
          style={{ opacity: on ? 1 : 0, transition: reduce ? undefined : `opacity ${PICK_MS}ms ${PICK_EASE}` }}
        />
      ) : null}
      <span aria-hidden="true" className="pointer-events-none absolute inset-0 z-10">
        <RingDraw on={on} reduce={reduce} />
      </span>
    </>
  );
}

/* what you tapped, while the page leaves: the ring's faint track, waiting for the ring to draw */
function ArmTrack({ reduce }: { reduce: boolean }) {
  return (
    <span aria-hidden="true" className={cn('pointer-events-none absolute inset-0 z-10', !reduce && 'fm-rail-come')}>
      <svg className="block h-full w-full overflow-visible" viewBox="0 0 100 100">
        <circle cx="50" cy="50" r={RING_R} fill="none" stroke="rgb(var(--fm-accent-rgb)/0.32)" strokeWidth="7.5" />
      </svg>
    </span>
  );
}

/* the wedge: "Feeds" while you're looking at all of them, "Back" once you've opened one */
const WedgeButton = memo(function WedgeButton({ active, current, armed, leaving, reduce, onBack }: { active: boolean; current: boolean; armed: boolean; leaving: boolean; reduce: boolean; onBack: () => void }) {
  const lit = (current && !leaving) || armed;
  const ref = useRef<HTMLButtonElement | null>(null);
  useHeldPick(ref, `${lit}|${active}`);
  return (
    <button
      ref={ref}
      type="button"
      // not `disabled`: Back taken from the keys would drop the focus to the page as the button turns off
      aria-disabled={!active}
      tabIndex={active ? 0 : -1}
      data-rail-all={active ? '' : undefined}
      onClick={() => {
        if (active) onBack();
      }}
      {...(active ? press : {})}
      className={cn(ITEM_CLASS, 'z-[40] aria-disabled:cursor-default')}
      aria-label={active ? 'Back to all feeds' : 'All feeds'}
    >
      <Plate lit={lit} marked={false} armed={armed} reduce={reduce}>
        <StoryRing on={current && !leaving} reduce={reduce} glow={false} />
        {armed ? <ArmTrack reduce={reduce} /> : null}
        <span className={cn('relative z-20 grid h-full w-full place-items-center overflow-hidden rounded-full border-[2px] border-white', active && FACE_PRESS, FACE_FOCUS)} style={{ background: 'var(--lead-wedge-surface)' }}>
          {/* it turns to point back as a feed opens, on the rail's clock */}
          <span
            className="grid h-[38px] w-[38px] place-items-center leading-none lg:h-[42px] lg:w-[42px]"
            style={{ transform: active ? 'rotate(180deg)' : 'rotate(0deg)', transition: reduce ? undefined : `transform ${RAIL_MOVE_MS}ms ${PICK_EASE}` }}
          >
            <svg aria-hidden viewBox="0 0 42 42" className="block h-[38px] w-[38px] lg:h-[42px] lg:w-[42px]">
              <path
                d="M 21 21 L 35.87 12.76 A 17 17 0 1 0 35.87 29.24 Z"
                fill="none"
                stroke="var(--lead-wedge-mark)"
                strokeWidth="3"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
          </span>
        </span>
      </Plate>
      <RailLabel text={active ? 'BACK' : 'FEEDS'} lit={lit} reduce={reduce} className="uppercase" />
    </button>
  );
});

/* a feed: its initials in the badge. Opening it, the badge travels to the front while the other feeds fold away, and
   its feeders fan out from behind it (the rail is a ledger in fan mode: components/motion/Ledger) */
const FeedBadge = memo(function FeedBadge({ feed, marked, selected, armed, leaving, reduce, ariaLabel, stack, onPick }: { feed: HeaderFeed; marked: boolean; selected: boolean; armed: boolean; leaving: boolean; reduce: boolean; ariaLabel: string; stack: number; onPick: (id: string) => void }) {
  const lit = (selected && !leaving) || armed;
  const ref = useRef<HTMLButtonElement | null>(null);
  useHeldPick(ref, lit);
  return (
    <button
      ref={ref}
      type="button"
      data-ledger={`rail:feed:${feed.id}`}
      data-rail-badge={feed.id}
      onClick={() => onPick(feed.id)}
      {...press}
      className={ITEM_CLASS}
      style={{ zIndex: stack }}
      aria-pressed={selected}
      aria-label={ariaLabel}
    >
      <Plate lit={lit} marked={marked} armed={armed} reduce={reduce}>
        <StoryRing on={selected && !leaving} reduce={reduce} />
        {armed ? <ArmTrack reduce={reduce} /> : null}
        <span
          className={cn('relative z-20 grid h-full w-full place-items-center overflow-hidden rounded-full border-[2px] border-white text-[12px] font-black leading-none dark:border-[var(--fm-ink)]', FACE_PRESS, FACE_FOCUS)}
          style={{
            color: 'var(--fm-accent)',
            background: 'radial-gradient(circle at 30% 18%, rgb(var(--fm-accent-rgb) / 0.12), transparent 58%), linear-gradient(135deg,#f7f7f8,#cfd3dc)',
          }}
        >
          {feedInitials(feed.title)}
        </span>
      </Plate>
      <RailLabel text={titleCase(feed.title)} lit={lit} reduce={reduce} className="uppercase" />
    </button>
  );
});

/* a feeder: fans out from behind the circle before it when its feed opens (the first from behind the feed's badge),
   one after another, and folds back behind it as the feed closes */
const FeederCircle = memo(function FeederCircle({ feeder, marked, selected, armed, leaving, reduce, ariaLabel, stack, onPick }: { feeder: HeaderFeeder; marked: boolean; selected: boolean; armed: boolean; leaving: boolean; reduce: boolean; ariaLabel: string; stack: number; onPick: (handle: string) => void }) {
  const lit = (selected && !leaving) || armed;
  const ref = useRef<HTMLButtonElement | null>(null);
  useHeldPick(ref, lit);
  return (
    <button
      ref={ref}
      type="button"
      data-ledger={`rail:feeder:${feeder.handle}`}
      data-rail-feeder={feeder.handle}
      onClick={() => onPick(feeder.handle)}
      {...press}
      className={ITEM_CLASS}
      style={{ zIndex: stack }}
      aria-pressed={selected}
      aria-label={ariaLabel}
    >
      <Plate lit={lit} marked={marked} armed={armed} reduce={reduce}>
        <StoryRing on={selected && !leaving} reduce={reduce} />
        {armed ? <ArmTrack reduce={reduce} /> : null}
        <span className={cn('relative z-20 flex h-full w-full items-center justify-center overflow-hidden rounded-full border-[2px] border-white bg-[linear-gradient(135deg,#fce7f3,#fff1f2)] text-[var(--fm-accent-deeper)] dark:border-[var(--fm-ink)] dark:bg-[linear-gradient(135deg,#1c1917,#18181b)] dark:text-[var(--fm-accent-soft)]', FACE_PRESS, FACE_FOCUS)}>
          {/* decoded ahead (TabHeader), and asked for at once: a face never arrives after its circle */}
          <FeederStoryAvatar feeder={{ handle: feeder.handle, profilePicUrl: feeder.profilePicUrl }} loading="eager" />
        </span>
      </Plate>
      <RailLabel text={feeder.handle} lit={lit} reduce={reduce} className="lowercase" />
    </button>
  );
});

/* a line of the title row: the new one rises in as the old one goes; while a page leaves, the old one lets go a
   beat into the leave and the new one waits for the page. The two share one cell while they cross (no stylesheet
   popped in and out of the page mid-switch, as popLayout does). An exiting line keeps the props of its last render,
   so whether the page is leaving reaches it through AnimatePresence's custom */
type SlotCustom = { leaving: boolean; travel: number; reduce: boolean };
const TITLE_SLOT = {
  exit: ({ leaving, travel, reduce }: SlotCustom) => (reduce
    ? { opacity: 0, transition: { duration: 0.12 } }
    : { opacity: 0, transform: `translateY(${-travel * 0.8}px)`, transition: { duration: BEAT_LEAVE_S, delay: leaving ? sec(0.18) : 0, ease: BEAT_LEAVE_EASE } }),
};

function TitleSlot({ text, leaving, reduce, className, travel = 10 }: { text: string | null; leaving: boolean; reduce: boolean; className: string; travel?: number }) {
  const custom: SlotCustom = { leaving, travel, reduce };
  return (
    <div className="grid min-w-0">
      <AnimatePresence initial={false} custom={custom}>
        {text ? (
          <motion.div
            key={text}
            custom={custom}
            variants={TITLE_SLOT}
            initial={reduce ? { opacity: 0 } : { opacity: 0, transform: `translateY(${travel}px)` }}
            animate={{ opacity: 1, transform: 'translateY(0px)' }}
            exit="exit"
            transition={reduce ? { duration: 0.14 } : beatArrive(0)}
            className={cn('[grid-area:1/1]', className)}
          >
            {text}
          </motion.div>
        ) : null}
      </AnimatePresence>
    </div>
  );
}

const decodedFaces = new Set<string>();

export type TabHeaderConfig = {
  title: string;
  // desktop only, over the scope
  eyebrow?: string | null;
  // where you are: all feeds, a feed, or @handle
  label: string;
  // the 44px control on the right
  control?: ReactNode;
  compressed: boolean;
  className?: string;
  // null while they load: the rail then holds pendingFeeders
  feeds: HeaderFeed[] | null;
  pendingFeeders?: HeaderFeeder[];
  // the open feed (null or undefined: all feeds) and the picked feeder
  feedId: string | null | undefined;
  handle: string | null;
  // a feeder with something to open wears the rose story ring (and so does its feed)
  marked?: (handle: string) => boolean;
  feederLabel?: (handle: string) => string;
  // the page is leaving for what was just tapped: 'all', 'feed:<id>' or 'feeder:<handle>' (armed)
  leaving?: boolean;
  armed?: string | null;
  onAll: () => void;
  onFeed: (id: string) => void;
  onFeeder: (handle: string) => void;
  // a pick about to be made (a hover, a press, focus on the rail): the tab can get it ready
  onIntent?: (pick: { feedId: string | null; handle: string | null }) => void;
  // for a tab that lays something over its title row (Read's readout): the row, its base layer and the overlay
  rowRef?: Ref<HTMLDivElement>;
  rowId?: string;
  rowClassName?: string;
  baseClassName?: string;
  rowOverlay?: ReactNode;
  railId?: string;
};

function TabHeader({
  tab,
  title,
  eyebrow,
  label,
  control,
  compressed,
  className,
  feeds,
  pendingFeeders = [],
  feedId,
  handle,
  marked,
  feederLabel,
  leaving = false,
  armed = null,
  onAll,
  onFeed,
  onFeeder,
  onIntent,
  rowRef,
  rowId,
  rowClassName,
  baseClassName,
  rowOverlay,
  railId,
}: TabHeaderConfig & { tab: string }) {
  const reduce = Boolean(useReducedMotion());
  const activeFeed = feeds && feedId ? feeds.find((feed) => feed.id === feedId) || null : null;
  const isMarked = (feederHandle: string) => Boolean(marked?.(feederHandle));
  const nameFeeder = (feederHandle: string) => (feederLabel ? feederLabel(feederHandle) : `@${feederHandle}`);

  // every face on the rail is fetched and decoded ahead, so a feeder flying out of its badge never waits on (or
  // decodes mid-flight) its picture
  useEffect(() => {
    feeds?.forEach((feed) => feed.feeders.forEach((feeder) => {
      const src = feeder.profilePicUrl;
      if (!src || decodedFaces.has(src)) return;
      decodedFaces.add(src);
      const image = new Image();
      image.decoding = 'async';
      image.src = src;
      image.decode?.().catch(() => undefined);
    }));
  }, [feeds]);

  // a pick about to be made: the rail item the pointer comes onto (once per visit), or that is pressed or focused
  const intentAt = useRef<Element | null>(null);
  const intent = (event: SyntheticEvent) => {
    if (!onIntent) return;
    const item = (event.target as Element | null)?.closest?.('button');
    if (!item) return;
    if (event.type === 'pointerover' && item === intentAt.current) return;
    intentAt.current = item;
    const button = item as HTMLElement;
    if (button.dataset.railBadge) onIntent({ feedId: button.dataset.railBadge, handle: null });
    else if (button.dataset.railFeeder) onIntent({ feedId: activeFeed?.id ?? null, handle: button.dataset.railFeeder });
    else if (button.dataset.railAll != null) onIntent({ feedId: null, handle: null });
  };

  // what the rail holds: the feeds, or the open feed's badge and its feeders (while the feeds load, the feeders it had)
  const railFeeds = !feeds ? [] : activeFeed ? [activeFeed] : feeds;
  const railFeeders = !feeds ? pendingFeeders : activeFeed ? activeFeed.feeders : [];
  // (their own kind of ids: a page's lists use feed:… too, and a lookup on the page must never land in the header)
  const railIds = [...railFeeds.map((feed) => `rail:feed:${feed.id}`), ...railFeeders.map((feeder) => `rail:feeder:${feeder.handle}`)];
  // opening or closing a feed re-deals it (a feeder picked inside a feed only moves the rings)
  const railDeal = !feeds ? 'loading' : activeFeed ? `feed:${activeFeed.id}` : 'all';

  // the tab's handlers, read when a circle is tapped: the circles keep one stable callback each, so a page publishing
  // its header again (every render of the page) doesn't re-render a circle that hasn't changed
  const handlers = useRef({ onAll, onFeed, onFeeder });
  useIsomorphicLayoutEffect(() => {
    handlers.current = { onAll, onFeed, onFeeder };
  });
  const pickAll = useCallback(() => handlers.current.onAll(), []);
  const pickFeed = useCallback((id: string) => handlers.current.onFeed(id), []);
  const pickFeeder = useCallback((feederHandle: string) => handlers.current.onFeeder(feederHandle), []);

  const wedge = (
    <WedgeButton
      active={Boolean(activeFeed)}
      current={Boolean(feeds) && !activeFeed && !handle}
      armed={armed === 'all'}
      leaving={leaving}
      reduce={reduce}
      onBack={pickAll}
    />
  );

  // first over last: a circle coming out of the one before it comes out from behind it
  const stackOf = (index: number) => 30 - Math.min(index, 29);
  const circle = (feeder: HeaderFeeder, index: number) => (
    <FeederCircle
      key={`feeder:${feeder.handle}`}
      feeder={feeder}
      marked={isMarked(feeder.handle)}
      selected={handle === feeder.handle}
      armed={armed === `feeder:${feeder.handle}`}
      leaving={leaving}
      reduce={reduce}
      ariaLabel={nameFeeder(feeder.handle)}
      stack={stackOf(railFeeds.length + index)}
      onPick={pickFeeder}
    />
  );

  return (
    <header className={cn('h-full px-3 py-2 sm:px-4 lg:px-5 lg:py-2.5', className)} aria-label={titleCase(title)}>
        <div className="relative z-10 flex h-full flex-col gap-1.5 pt-1 lg:gap-2 lg:pt-[14px]">
          {/* the title row. A tab that writes into it from outside React (Read toggles a class on it) gets a class
              list that never changes while it is on screen, so a re-render never overwrites what it wrote */}
          <div ref={rowRef} id={rowId} className={cn('relative h-11 min-h-11 lg:h-12 lg:min-h-12', rowClassName)}>
            <div className={cn('absolute inset-0 flex items-center justify-between gap-2', baseClassName)}>
              <div className="flex min-w-0 items-center gap-2.5">
                {/* the tab's name: switching tabs rolls only the letters that differ (LEAD → READ moves one) */}
                <h1 className="fm-depth-title shrink-0 text-[22px] font-black leading-[0.88] tracking-[0.12em] text-fg lg:text-[26px] lg:tracking-[0.14em]">
                  <SlotText value={title} align="left" />
                </h1>
                <div className="flex h-7 min-w-0 flex-col justify-center border-l border-fg/10 pl-2.5">
                  {eyebrow ? (
                    <div className="hidden lg:block">
                      <TitleSlot
                        text={leaving ? null : eyebrow}
                        leaving={leaving}
                        reduce={reduce}
                        travel={6}
                        className="truncate text-[8px] font-black uppercase leading-none tracking-[0.18em] text-[var(--fm-accent-text)]"
                      />
                    </div>
                  ) : null}
                  <TitleSlot
                    text={leaving ? null : label}
                    leaving={leaving}
                    reduce={reduce}
                    className="max-w-[150px] truncate text-[14px] font-black leading-none tracking-[-0.025em] text-fg/88 sm:max-w-[210px] sm:text-[16px] lg:mt-0.5"
                  />
                </div>
              </div>
              {/* each tab's own control; switching tabs crosses one over to the other */}
              <AnimatePresence initial={false} mode="popLayout">
                {control ? (
                  <motion.div
                    key={tab}
                    className="flex shrink-0 items-center"
                    initial={reduce ? { opacity: 0 } : { opacity: 0, transform: 'translateY(8px)' }}
                    animate={{ opacity: 1, transform: 'translateY(0px)', transition: reduce ? REDUCED : beatArrive(0) }}
                    exit={reduce ? { opacity: 0, transition: REDUCED } : { opacity: 0, transform: 'translateY(-6px)', transition: beatLeave(0) }}
                  >
                    {control}
                  </motion.div>
                ) : null}
              </AnimatePresence>
            </div>
            {rowOverlay}
          </div>

          {/* The story rail. As the header folds, the circles roll up under the title row: the edge they slide under
              stays still and only they move (a transform, on the glass's own clock, so they stay inside it); nothing
              reshapes on the way. The rail itself is a ledger (components/motion/Ledger): opening a feed, its badge
              travels to the front while the other feeds hand their places to its feeders, one after another, out of
              the badge, on the beat the page's rows deal on. */}
          <div id={railId} className="min-w-0 overflow-hidden" style={{ pointerEvents: compressed ? 'none' : 'auto' }}>
            <div
              className="flex min-w-0 items-start gap-3"
              style={{
                transform: compressed ? 'translate3d(0, calc(-100% - 10px), 0)' : 'translate3d(0, 0, 0)',
                transition: reduce ? undefined : 'transform 340ms cubic-bezier(0.32, 0.72, 0, 1)',
              }}
              onPointerOver={intent}
              onPointerDown={intent}
              onFocus={intent}
              onPointerLeave={() => { intentAt.current = null; }}
            >
              <div className="hidden sm:block">{wedge}</div>
              <Ledger
                ids={railIds}
                deal={railDeal}
                axis="x"
                fan
                moveMs={RAIL_MOVE_MS}
                startOnDeal
                dealOnMount={false}
                role="group"
                label={activeFeed ? `Feeders in ${titleCase(activeFeed.title)}` : 'Feeds'}
                className="hide-scrollbar flex min-w-0 flex-1 items-start gap-2 overflow-x-auto pb-0.5 pr-3 lg:gap-3 lg:pb-1"
              >
                <div className="sm:hidden">{wedge}</div>
                {railFeeds.map((feed, index) => (
                  <FeedBadge
                    key={`feed:${feed.id}`}
                    stack={stackOf(index)}
                    feed={feed}
                    marked={feed.feeders.some((feeder) => isMarked(feeder.handle))}
                    selected={feed.id === activeFeed?.id && !handle}
                    armed={armed === `feed:${feed.id}`}
                    leaving={leaving}
                    reduce={reduce}
                    ariaLabel={feed.id !== activeFeed?.id ? `Open ${titleCase(feed.title)}` : handle ? `Show every feeder in ${titleCase(feed.title)}` : `${titleCase(feed.title)}, every feeder`}
                    onPick={pickFeed}
                  />
                ))}
                {railFeeders.map(circle)}
              </Ledger>
            </div>
          </div>
        </div>
    </header>
  );
}

/* ── one header for every tab ──────────────────────────────────────────────
   A tab says what its header shows (useTabHeader, every render); the shell draws the one header (TabHeaderHost)
   for the tab on screen. Kept in an external store so a tab publishing re-renders only the header, never the shell
   or the other tabs.

   A hidden tab's last word is stale (the pick may have moved since), so on a switch the header keeps showing what it
   was until the arriving tab speaks again, which it does before the first paint. The header never passes through a
   stale state, so the only things that move are the things that really differ between the tabs. */

let headerRevision = 0;
const headerConfigs = new Map<string, { config: TabHeaderConfig; revision: number }>();
const headerListeners = new Set<() => void>();

function subscribeHeader(listener: () => void) {
  headerListeners.add(listener);
  return () => {
    headerListeners.delete(listener);
  };
}

const useIsomorphicLayoutEffect = typeof window !== 'undefined' ? useLayoutEffect : useEffect;

export function useTabHeader(tab: string, config: TabHeaderConfig) {
  // before paint, so the header never shows a frame of the tab's old state
  useIsomorphicLayoutEffect(() => {
    headerRevision += 1;
    headerConfigs.set(tab, { config, revision: headerRevision });
    headerListeners.forEach((listener) => listener());
  });
}

// the tabs that wear this header (the others still bring their own through AppHeader)
const TAB_HEADER_TABS = new Set(['lead', 'read', 'feed']);

export function TabHeaderHost({ tab, onCompressed }: { tab: string | null; onCompressed: (compressed: boolean) => void }) {
  const reduce = Boolean(useReducedMotion());
  const entry = useSyncExternalStore(
    subscribeHeader,
    () => (tab ? headerConfigs.get(tab) ?? null : null),
    () => null,
  );
  // what the tab on screen has said since it came on screen (anything older is stale)
  const arrived = useRef<{ tab: string | null; revision: number }>({ tab, revision: headerRevision });
  if (arrived.current.tab !== tab) arrived.current = { tab, revision: headerRevision };
  const config = entry && entry.revision > arrived.current.revision ? entry.config : null;
  // until then, the header stays as it was
  const last = useRef<{ tab: string; config: TabHeaderConfig } | null>(null);
  const wears = Boolean(tab && TAB_HEADER_TABS.has(tab));
  // (arriving from a tab with its own header there is nothing to stay as: it comes in fresh)
  if (!wears) last.current = null;
  if (tab && config) last.current = { tab, config };
  // every tab's rail comes from the same feeds (/api/feed): a tab still loading its copy keeps the rail on the ones
  // the header already has, so a first visit doesn't empty the rail and refill it
  const knownFeeds = useRef<HeaderFeed[] | null>(null);
  if (config?.feeds) knownFeeds.current = config.feeds;
  const picked = wears ? (config && tab ? { tab, config } : last.current) : null;
  const shown = picked && !picked.config.feeds && knownFeeds.current
    ? { tab: picked.tab, config: { ...picked.config, feeds: knownFeeds.current } }
    : picked;
  const compressed = Boolean(shown?.config.compressed);

  // in the same commit as the rail (a layout effect: the shell's update lands before the frame), so the glass edge
  // and the circles folding under the title start together
  useIsomorphicLayoutEffect(() => {
    if (shown) onCompressed(compressed);
  }, [Boolean(shown), compressed, onCompressed]);

  if (!shown) return null;
  // arriving from a tab with its own header, it comes in as headers always have; between tabs that share it, it stays
  return (
    <motion.div
      className="absolute inset-0"
      initial={reduce ? false : HEADER_ROUTE_MORPH.initial}
      animate={reduce ? { opacity: 1 } : HEADER_ROUTE_MORPH.animate}
      style={{ transformOrigin: '50% 0%' }}
    >
      <TabHeader tab={shown.tab} {...shown.config} />
    </motion.div>
  );
}

/* ── the time range: a button that grows into its menu on a phone, a segmented track from sm up ── */

/* the same control serves any short choice (Lead's 30D, Feed's TODAY · WEEK · 30D · 90D): a value's label defaults
   to the time range's "30D", and labels longer than that widen the trigger, its menu and the track's segments */
const timeframeLabel = (value: string | number) => `${value}D`;
const isRoomy = <T extends string | number>(options: readonly T[], label: (value: T) => string) => options.some((option) => label(option).length > 3);

/* On a phone the choice is a button that grows down into its menu, as Read's view switch does (readerEngine,
   lnmOpen): a box laid exactly over the button wears its face (the value and the chevron, where they were; the button
   itself hides under it), so nothing moves as it opens; then it grows down to show the other values under a hairline,
   one after another. A pick rolls its value into the face while the box closes back into the button, and reaches the
   tab once the box has mostly closed, so the change (a zoom, on Feed) never fights the menu for frames. A tap
   outside, Escape or a scroll closes it the same way. Read's curves and sizes. */
// a critically damped spring: Read's sheet (readerEngine, EZ.sheet)
const MENU_SPRING = (t: number) => (t >= 1 ? 1 : 1 - (1 + 7.5 * t) * Math.exp(-7.5 * t));
const MENU_OPEN = { duration: sec(0.56), ease: MENU_SPRING };
const MENU_CLOSE = { duration: sec(0.38), ease: [0.32, 0.72, 0, 1] as const };
const MENU_ROW = 40;
const MENU_PAD = 6;
// the pick reaches the tab once the box has mostly closed (on the close's clock)
const MENU_APPLY_MS = ms(300);
// a tab whose switch is a heavy render takes the pick once the box has closed (the close's 380ms and its frame), so
// that render can't stall the close's last stretch
export const MENU_APPLY_AFTER_CLOSE_MS = ms(380) + 40;
// a pick the tab doesn't take stops being worn after this
const MENU_PICK_KEEP_MS = 1800;
// literal per theme: framer can't interpolate a var() inside a shadow
const MENU_SHADOW_REST = '0px 0px 0px 0px rgba(0, 0, 0, 0), 0px 0px 0px 0px rgba(255, 23, 79, 0)';
const MENU_SHADOW_OPEN = {
  dark: '0px 22px 44px -18px rgba(0, 0, 0, 0.95), 0px 0px 34px -22px rgba(255, 23, 79, 0.6)',
  light: '0px 22px 44px -18px rgba(15, 23, 42, 0.3), 0px 0px 34px -22px rgba(255, 23, 79, 0.32)',
} as const;

type MenuBox<T> = { top: number; right: number; width: number; height: number; scrollY: number; others: T[]; keys: boolean };

// a value in its slot, rolling when it changes (the button's face, and the open menu's)
function ValueSlot({ text, dir, roomy, reduce }: { text: string; dir: number; roomy: boolean; reduce: boolean }) {
  return (
    <span className={cn('relative grid h-[1.08em] shrink-0 place-items-center overflow-hidden leading-none', roomy ? 'w-[68px]' : 'w-8')}>
      <AnimatePresence initial={false} custom={dir}>
        <motion.span
          key={text}
          custom={dir}
          variants={SLOT_ROLL}
          initial={reduce ? false : 'enter'}
          animate="center"
          exit={reduce ? { transform: 'translate3d(0, 0%, 0)' } : 'exit'}
          transition={reduce ? { duration: 0 } : SLOT_SPRING}
          className="absolute inset-0 grid place-items-center tabular-nums text-fg"
        >
          {text}
        </motion.span>
      </AnimatePresence>
    </span>
  );
}

function TimeframePicker<T extends string | number>({ value, options, pending, label, name, applyMs = MENU_APPLY_MS, onChange }: { value: T; options: readonly T[]; pending: boolean; label: (value: T) => string; name?: string; applyMs?: number; onChange: (value: T) => void }) {
  const reduce = Boolean(useReducedMotion());
  const theme = useResolvedTheme();
  const roomy = isRoomy(options, label);
  const panelId = useId();
  // the button's place, read from its slot (the button itself may be mid-press, scaled)
  const slotRef = useRef<HTMLDivElement | null>(null);
  const triggerRef = useRef<HTMLButtonElement | null>(null);
  const menuRef = useRef<HTMLDivElement | null>(null);
  const optionRefs = useRef<Array<HTMLButtonElement | null>>([]);
  const onChangeRef = useRef(onChange);
  const timersRef = useRef<number[]>([]);
  const refocusRef = useRef(false);
  // the box while it is on screen: opening, open, or closing back into the button
  const [box, setBox] = useState<MenuBox<T> | null>(null);
  const [open, setOpen] = useState(false);
  // a pick on its way: worn by the face and the button until the tab has it
  const [picked, setPicked] = useState<T | null>(null);
  if (picked !== null && picked === value) setPicked(null);
  const shown = picked ?? value;
  // which way the value rolls: a step on through the options comes up from below
  const [roll, setRoll] = useState({ shown, dir: 1 });
  if (roll.shown !== shown) setRoll({ shown, dir: options.indexOf(shown) >= options.indexOf(roll.shown) ? 1 : -1 });

  useIsomorphicLayoutEffect(() => {
    onChangeRef.current = onChange;
  });
  useEffect(() => () => timersRef.current.forEach((timer) => window.clearTimeout(timer)), []);

  // while it is open: a tap outside, Escape, a scroll or the window changing closes it (back into the button); opened
  // from the keys, the first value has the focus
  useEffect(() => {
    if (!open || !box) return undefined;
    const close = () => setOpen(false);
    const closeFromOutside = (event: PointerEvent) => {
      const target = event.target as Node | null;
      if (target && !slotRef.current?.contains(target) && !menuRef.current?.contains(target)) close();
    };
    const closeFromKeyboard = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      refocusRef.current = true;
      close();
    };
    const closeFromScroll = () => {
      if (Math.abs(window.scrollY - box.scrollY) > 8) close();
    };
    document.addEventListener('pointerdown', closeFromOutside, true);
    document.addEventListener('keydown', closeFromKeyboard);
    window.addEventListener('scroll', closeFromScroll, { passive: true });
    window.addEventListener('resize', close);
    window.addEventListener('orientationchange', close);
    const frame = box.keys ? window.requestAnimationFrame(() => optionRefs.current[0]?.focus({ preventScroll: true })) : 0;
    return () => {
      document.removeEventListener('pointerdown', closeFromOutside, true);
      document.removeEventListener('keydown', closeFromKeyboard);
      window.removeEventListener('scroll', closeFromScroll);
      window.removeEventListener('resize', close);
      window.removeEventListener('orientationchange', close);
      if (frame) window.cancelAnimationFrame(frame);
    };
  }, [open, box]);

  const toggle = (event: ReactMouseEvent<HTMLButtonElement>) => {
    if (box) {
      setOpen(false);
      return;
    }
    const rect = slotRef.current?.getBoundingClientRect();
    if (!rect || !rect.width) return;
    // a click with no pointer behind it (Enter, Space) came from the keys: focus goes into the menu and back
    const keys = event.detail === 0;
    refocusRef.current = keys;
    setBox({
      top: rect.top,
      right: window.innerWidth - rect.right,
      width: rect.width,
      height: rect.height,
      scrollY: window.scrollY,
      others: options.filter((option) => option !== value),
      keys,
    });
    setOpen(true);
  };

  const pick = (option: T) => {
    if (option === shown) {
      setOpen(false);
      return;
    }
    // the face rolls to it first, then the box closes round it (a frame later, so the closing box wears the new value)
    setPicked(option);
    window.requestAnimationFrame(() => setOpen(false));
    timersRef.current.push(
      window.setTimeout(() => onChangeRef.current(option), reduce ? 0 : applyMs),
      window.setTimeout(() => setPicked((current) => (current === option ? null : current)), applyMs + MENU_PICK_KEEP_MS),
    );
  };

  const moveFocus = (index: number, step: number) => {
    const count = box?.others.length ?? 0;
    if (count) optionRefs.current[(index + step + count) % count]?.focus();
  };

  const menu = typeof document !== 'undefined' ? createPortal(
    <AnimatePresence
      onExitComplete={() => {
        setBox(null);
        if (!refocusRef.current) return;
        refocusRef.current = false;
        window.requestAnimationFrame(() => triggerRef.current?.focus({ preventScroll: true }));
      }}
    >
      {open && box ? (
        <motion.div
          key="menu"
          ref={menuRef}
          id={panelId}
          role="menu"
          aria-label={name ?? 'Time range'}
          className="fixed z-[160] overflow-hidden text-fg [-webkit-tap-highlight-color:transparent]"
          style={{ top: box.top, right: box.right, width: box.width, backgroundColor: 'var(--fm-well)' }}
          initial={reduce ? false : { height: box.height, borderRadius: 14, boxShadow: MENU_SHADOW_REST }}
          animate={{
            height: box.height + MENU_PAD * 2 + box.others.length * MENU_ROW,
            borderRadius: 16,
            boxShadow: MENU_SHADOW_OPEN[theme],
            transition: reduce ? { duration: 0 } : MENU_OPEN,
          }}
          exit={reduce
            ? { opacity: 0, transition: { duration: 0 } }
            : { height: box.height, borderRadius: 14, boxShadow: MENU_SHADOW_REST, transition: MENU_CLOSE }}
        >
          {/* the menu's own surface, coming up over the button's */}
          <motion.span
            aria-hidden="true"
            className="pointer-events-none absolute inset-0 bg-[linear-gradient(180deg,rgba(255,250,251,.985),rgba(255,255,255,.975))] backdrop-blur-[18px] dark:bg-[linear-gradient(180deg,rgba(22,16,19,.985),rgba(6,6,7,.975))]"
            initial={reduce ? false : { opacity: 0 }}
            animate={{ opacity: 1, transition: reduce ? { duration: 0 } : { duration: sec(0.22), ease: 'easeOut' } }}
            exit={reduce ? undefined : { opacity: [1, 1, 0], transition: { duration: sec(0.4), times: [0, 0.6, 1] } }}
          />
          <span aria-hidden="true" className="pointer-events-none absolute left-[10px] right-[10px] h-px bg-fg/[0.08]" style={{ top: box.height }} />
          <div className="absolute left-0 z-[1] grid gap-[2px] px-1 py-[6px]" style={{ top: box.height, width: box.width }}>
            {box.others.map((option, index) => (
              <motion.button
                key={String(option)}
                ref={(node) => { optionRefs.current[index] = node; }}
                type="button"
                role="menuitem"
                initial={reduce ? false : { opacity: 0, transform: 'translateY(-10px)' }}
                animate={{
                  opacity: 1,
                  transform: 'translateY(0px)',
                  transition: reduce ? { duration: 0 } : { duration: sec(0.42), delay: sec(0.09 + index * 0.055), ease: SOFT_EASE },
                }}
                exit={reduce ? undefined : { opacity: 0, transition: { duration: sec(0.14), ease: 'easeIn' } }}
                onClick={() => pick(option)}
                onKeyDown={(event) => {
                  if (event.key === 'ArrowDown' || event.key === 'ArrowRight') {
                    event.preventDefault();
                    moveFocus(index, 1);
                  } else if (event.key === 'ArrowUp' || event.key === 'ArrowLeft') {
                    event.preventDefault();
                    moveFocus(index, -1);
                  } else if (event.key === 'Home' || event.key === 'End') {
                    event.preventDefault();
                    optionRefs.current[event.key === 'Home' ? 0 : box.others.length - 1]?.focus();
                  }
                }}
                className="grid h-[38px] w-full touch-manipulation place-items-center rounded-[11px] text-[16px] font-black uppercase leading-none tabular-nums tracking-[-0.02em] text-fg/[0.58] outline-none transition-[background-color,color,scale] duration-150 focus-visible:bg-fg/[0.08] focus-visible:text-fg active:scale-[0.96] active:bg-[rgb(255_23_79/0.85)] active:text-white"
              >
                {label(option)}
              </motion.button>
            ))}
          </div>
          {/* the face: the button's own insides, where they were */}
          <button
            type="button"
            tabIndex={-1}
            aria-label="Close"
            onClick={() => setOpen(false)}
            className="absolute right-0 top-0 z-[2] flex touch-manipulation items-center justify-center gap-1 text-[16px] font-black uppercase tracking-[-0.02em] text-fg outline-none"
            style={{ width: box.width, height: box.height }}
          >
            <ValueSlot text={label(shown)} dir={roll.dir} roomy={roomy} reduce={reduce} />
            <motion.span
              className="grid place-items-center"
              initial={reduce ? false : { transform: 'rotate(0deg)' }}
              animate={{ transform: 'rotate(180deg)', transition: reduce ? { duration: 0 } : { duration: sec(0.48), ease: MENU_SPRING } }}
              exit={reduce ? undefined : { transform: 'rotate(0deg)', transition: MENU_CLOSE }}
            >
              <ChevronDown className="h-3.5 w-3.5 text-fg/54" aria-hidden="true" />
            </motion.span>
          </button>
          {/* its edge: the button's hairline and inner shade, all the way round */}
          <span aria-hidden="true" className="pointer-events-none absolute inset-0 z-[3] rounded-[inherit] shadow-[inset_0_0_0_1px_var(--fm-well-edge),var(--fm-well-shade)]" />
        </motion.div>
      ) : null}
    </AnimatePresence>,
    document.body,
  ) : null;

  return (
    <div ref={slotRef} className="shrink-0 sm:hidden">
      <button
        ref={triggerRef}
        type="button"
        aria-haspopup="menu"
        aria-expanded={open}
        aria-controls={open ? panelId : undefined}
        aria-label={name ? `${name}: ${label(shown)}` : `Time range: ${shown} days`}
        aria-busy={pending}
        onClick={toggle}
        // under the box while the menu is out (the box wears its face), so nothing moves
        style={box ? { visibility: 'hidden' } : undefined}
        className={cn(HEADER_CONTROL, roomy ? 'w-[108px]' : 'w-[76px]', 'text-[16px] tracking-[-0.02em] transition-transform duration-150 active:scale-[0.97]')}
      >
        <ValueSlot text={label(shown)} dir={roll.dir} roomy={roomy} reduce={reduce} />
        <span className="grid place-items-center">
          <ChevronDown className="h-3.5 w-3.5 text-fg/54" aria-hidden="true" />
        </span>
      </button>
      {menu}
    </div>
  );
}

/* The selected range's pill moves on the spring it always had (stiffness 420, damping 34, mass 0.82: settled in about a
   third of a second, no visible overshoot), sampled once here into keyframes, so it plays as a compositor transform
   (WAAPI) and a page busy re-rendering under it can't hold it up. The samples keep the spring's shape; only its
   total time is played out by TEMPO. */
const PILL_MOVE = (() => {
  const step = 1 / 1000;
  let x = 0;
  let v = 0;
  let t = 0;
  const path = [0];
  while (t < 1) {
    v += ((-420 * (x - 1) - 34 * v) / 0.82) * step;
    x += v * step;
    t += step;
    path.push(x);
    if (Math.abs(1 - x) < 0.0005 && Math.abs(v) < 0.02) break;
  }
  const samples = 32;
  return { ms: ms(t * 1000), at: Array.from({ length: samples + 1 }, (_, k) => (k === samples ? 1 : path[Math.round((k / samples) * (path.length - 1))])) };
})();

// how far along the spring is, a fraction of its time in
function pillProgress(time: number) {
  const at = PILL_MOVE.at;
  const exact = Math.max(0, Math.min(1, time)) * (at.length - 1);
  const k = Math.min(at.length - 2, Math.floor(exact));
  return at[k] + (at[k + 1] - at[k]) * (exact - k);
}

// the pill over the option at `position` (fractions are on the way): one cell over is its own width plus the gaps
const pillPlace = (position: number) => `translateX(calc(${position.toFixed(4)} * (100% + 6px)))`;

function TimeframePill({ index, count }: { index: number; count: number }) {
  const reduce = Boolean(useReducedMotion());
  const ref = useRef<HTMLSpanElement | null>(null);
  const move = useRef<{ from: number; to: number; animation: Animation | null }>({ from: index, to: index, animation: null });
  useIsomorphicLayoutEffect(() => {
    const el = ref.current;
    const last = move.current;
    if (!el || last.to === index) return;
    // a pill still on its way sets off again from where it is
    let from = last.to;
    if (last.animation && last.animation.playState === 'running') {
      from = last.from + (last.to - last.from) * pillProgress(Number(last.animation.currentTime ?? 0) / PILL_MOVE.ms);
      last.animation.cancel();
    }
    const animation = reduce
      ? null
      : el.animate(PILL_MOVE.at.map((progress, k) => ({ offset: k / (PILL_MOVE.at.length - 1), transform: pillPlace(from + (index - from) * progress) })), { duration: PILL_MOVE.ms, easing: 'linear' });
    move.current = { from, to: index, animation };
  }, [index, reduce]);
  return (
    <span
      ref={ref}
      aria-hidden="true"
      className="pointer-events-none absolute bottom-[3px] left-[3px] top-[3px] rounded-[10px] bg-[var(--fm-accent)] shadow-[inset_0_1px_0_rgb(255_255_255_/_0.32)]"
      style={{ width: `calc(${100 / count}% - 6px)`, transform: pillPlace(index) }}
    />
  );
}

export function TimeframeControl<T extends string | number>({ value, options, pending = false, label = timeframeLabel, name, applyMs, onChange }: { id: string; value: T; options: readonly T[]; pending?: boolean; label?: (value: T) => string; name?: string; applyMs?: number; onChange: (value: T) => void }) {
  // a pick on its way: the pill and the pressed segment move at the tap, in a render of their own, before the tab's
  // (possibly heavy) switch arrives with the value; worn until the value arrives, or for a while if it never does
  const [picked, setPicked] = useState<T | null>(null);
  if (picked !== null && picked === value) setPicked(null);
  useEffect(() => {
    if (picked === null) return undefined;
    const id = window.setTimeout(() => setPicked(null), MENU_PICK_KEEP_MS);
    return () => window.clearTimeout(id);
  }, [picked]);
  const shown = picked ?? value;
  const index = options.indexOf(shown);
  return (
    <>
      <TimeframePicker value={value} options={options} pending={pending} label={label} name={name} applyMs={applyMs} onChange={onChange} />
      <div
        className="relative hidden h-11 shrink-0 rounded-[14px] border border-fg/[0.07] bg-(--fm-well) shadow-(--fm-well-shade) sm:grid"
        style={{ width: options.length * (isRoomy(options, label) ? 66 : 46), gridTemplateColumns: `repeat(${options.length}, minmax(0, 1fr))` }}
        role="group"
        aria-label={name ?? 'Time range'}
      >
        {index >= 0 ? <TimeframePill index={index} count={options.length} /> : null}
        {options.map((option) => (
          <button
            key={option}
            type="button"
            onClick={() => {
              if (option !== shown) setPicked(option);
              onChange(option);
            }}
            aria-busy={pending}
            aria-pressed={shown === option}
            className={cn('relative grid h-full place-items-center rounded-[10px] text-[13px] font-black uppercase leading-none tracking-[0.06em] transition-colors', shown === option ? 'text-white' : 'text-fg/52 hover:text-fg/72')}
          >
            <span className="relative z-10">{label(option)}</span>
          </button>
        ))}
      </div>
    </>
  );
}
