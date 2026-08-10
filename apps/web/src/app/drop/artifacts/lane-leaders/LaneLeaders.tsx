'use client';

import Link from 'next/link';
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion';
import type { CSSProperties } from 'react';
import { useEffect, useRef, useState } from 'react';

export type LaneLeaderPost = {
  id: string;
  title: string;
  thumbnail?: string;
  url?: string;
};

export type LaneLeaderMode = 'landing' | 'volume';
type Mode = LaneLeaderMode;
type LaneId = 'carousel' | 'reel' | 'image';
type Lane = {
  id: LaneId;
  label: string;
  average: number;
  share: number;
  posts: LaneLeaderPost[];
};

const EASE = [0.22, 1, 0.36, 1] as const;
const HEADLINE_VARIANTS = {
  enter: (direction: number) => ({
    opacity: 0,
    transform: `translate3d(0, ${direction > 0 ? '16vh' : '-16vh'}, 0)`,
  }),
  center: {
    opacity: 1,
    transform: 'translate3d(0, 0, 0)',
    transition: { duration: 0.65, ease: EASE },
  },
  exit: (direction: number) => ({
    opacity: 0,
    transform: `translate3d(0, ${direction > 0 ? '-12vh' : '12vh'}, 0)`,
    transition: { duration: 0.4, ease: EASE },
  }),
} as const;
const MAX_SHARE = 52;
const MAX_LANDING_STRENGTH = 100 - 14;
const WHEEL_COOLDOWN_MS = 360;
const WHEEL_MIN_DELTA = 24;
const SWIPE_MIN_DISTANCE = 46;
const METRIC_DELAY_MS = 760;

const SLIDES = {
  landing: {
    headline: 'Carousels lead the landing.',
    context: 'Settled D7 · last six per lane',
    footer: 'Lower average percentile leads',
  },
  volume: {
    headline: 'Reels carried the week.',
    context: 'Output · this week',
    footer: 'Rail level follows weekly share',
  },
} as const;

const FALLBACK_POSTS: LaneLeaderPost[] = Array.from({ length: 12 }, (_, index) => ({
  id: `fallback-${index}`,
  title: `Post ${index + 1}`,
}));

const TILE_BACKGROUNDS = [
  'linear-gradient(145deg, #4c0820, #E11D48 135%)',
  'linear-gradient(145deg, #0e2966, #2563eb 140%)',
  'linear-gradient(145deg, #0d5138, #10b981 140%)',
  'linear-gradient(145deg, #783a1c, #f97316 140%)',
];

function makeLanes(posts: LaneLeaderPost[]): Lane[] {
  const source = (posts.length ? posts : FALLBACK_POSTS).slice(0, 12);
  const groups = [[], [], []] as LaneLeaderPost[][];
  source.forEach((post, index) => groups[index % groups.length].push(post));
  groups.forEach((group, index) => {
    if (!group.length) group.push(source[index % source.length]);
  });

  return [
    { id: 'carousel', label: 'Carousel', average: 14, share: 19, posts: groups[0] },
    { id: 'reel', label: 'Reel', average: 27, share: 52, posts: groups[1] },
    { id: 'image', label: 'Image', average: 41, share: 29, posts: groups[2] },
  ];
}

function landingLevel(average: number): number {
  if (average <= 20) return 4;
  if (average <= 35) return 3;
  if (average <= 50) return 2;
  return 1;
}

function RailFace({ lane }: { lane: Lane }) {
  const visiblePosts = lane.posts.slice(0, 4);
  const vacantSlots = Math.max(0, 4 - visiblePosts.length);

  return (
    <div className="absolute inset-0 overflow-hidden rounded-[18px] border border-white/16 bg-[#151515]">
      <div data-lane-track className="lane-leader-track grid h-full grid-cols-4">
        {visiblePosts.map((post, index) => (
          <span
            key={`${post.id}:${index}`}
            className="relative bg-cover bg-center"
            style={{
              backgroundImage: post.thumbnail
                ? `url("${post.thumbnail}"), ${TILE_BACKGROUNDS[index]}`
                : TILE_BACKGROUNDS[index],
            }}
          >
            <span className="absolute inset-y-0 right-0 w-px bg-white/12" />
          </span>
        ))}
        {Array.from({ length: vacantSlots }, (_, index) => (
          <span
            aria-hidden="true"
            key={`vacant:${index}`}
            className="border-l border-white/8 bg-[linear-gradient(145deg,rgba(225,29,72,0.16),rgba(255,255,255,0.035))]"
          />
        ))}
      </div>
      <span className="absolute inset-y-0 left-0 w-[48%] bg-gradient-to-r from-black/88 via-black/54 to-transparent" />
      <span className="absolute inset-y-0 left-4 flex items-center text-[15px] font-black leading-none tracking-[-0.035em] text-white sm:left-5 sm:text-[17px]">
        {lane.label}
      </span>
    </div>
  );
}

function LaneRail({
  lane,
  mode,
  metricMode,
  reduced,
  index,
}: {
  lane: Lane;
  mode: Mode;
  metricMode: Mode;
  reduced: boolean;
  index: number;
}) {
  const landingStrength = 100 - lane.average;
  const scale = mode === 'landing' ? landingStrength / MAX_LANDING_STRENGTH : lane.share / MAX_SHARE;
  const level = mode === 'landing'
    ? landingLevel(lane.average)
    : Math.min(4, Math.max(1, Math.ceil(scale * 4)));
  const levelScale = level / 4;
  const mobileWidth = `calc(${levelScale * 100}% + ${(1 - levelScale) * 60}px)`;
  const desktopWidth = `calc(${levelScale * 100}% + ${(1 - levelScale) * 88}px)`;
  const metric = metricMode === 'landing' ? lane.average : lane.share;

  return (
    <div
      data-lane-row={lane.id}
      data-lane-level={level}
      data-lane-rank={index}
      data-post-count={lane.posts.length}
      style={{
        '--lane-row-mobile': mobileWidth,
        '--lane-row-desktop': desktopWidth,
        zIndex: 3 - index,
      } as CSSProperties}
      className={`lane-leader-row absolute inset-x-0 top-0 ${reduced ? '' : 'will-change-[transform,width]'}`}
    >
      <div className="flex w-full items-center gap-2 sm:gap-4">
        <div data-lane-rail className="lane-leader-rail relative min-w-0 flex-1">
          <RailFace lane={lane} />
        </div>

        <div data-lane-metric className="w-[52px] shrink-0 text-right sm:w-[72px]">
          <span className="block text-[7px] font-black uppercase tracking-[0.16em] text-white/32 sm:text-[8px]">
            {metricMode === 'landing' ? 'Avg D7' : 'Share'}
          </span>
          <span className="mt-1 block overflow-hidden">
            <AnimatePresence mode="wait">
              <motion.span
                data-lane-metric-figure
                key={metricMode}
                initial={reduced ? { opacity: 0 } : { opacity: 0, transform: 'translateY(105%)' }}
                animate={reduced
                  ? { opacity: 1, transition: { duration: 0.12, ease: EASE } }
                  : {
                      opacity: 1,
                      transform: 'translateY(0%)',
                      transition: { duration: 0.2, delay: index * 0.06, ease: EASE },
                    }}
                exit={reduced
                  ? { opacity: 0, transition: { duration: 0.1, ease: EASE } }
                  : {
                      opacity: 0,
                      transform: 'translateY(-60%)',
                      transition: { duration: 0.14, ease: EASE },
                    }}
                className="block text-[31px] font-black leading-none tracking-[-0.07em] text-white sm:text-[38px]"
              >
                {metric}<span className="ml-0.5 text-[0.48em] text-white/52">%</span>
              </motion.span>
            </AnimatePresence>
          </span>
        </div>
      </div>
    </div>
  );
}

export default function LaneLeaders({
  posts,
  mode: controlledMode,
  embedded = false,
  contextLabel = 'Feeder Reader',
}: {
  posts: LaneLeaderPost[];
  mode?: LaneLeaderMode;
  embedded?: boolean;
  contextLabel?: string;
}) {
  const [localMode, setLocalMode] = useState<Mode>('landing');
  const mode = controlledMode ?? localMode;
  const [metricMode, setMetricMode] = useState<Mode>(mode);
  const reduced = Boolean(useReducedMotion());
  const gestureRef = useRef<{ x: number; y: number } | null>(null);
  const wheelLockRef = useRef(0);
  const ownsNavigation = !embedded && controlledMode == null;
  const lanes = makeLanes(posts);
  const ordered = lanes.slice().sort((a, b) => mode === 'landing' ? a.average - b.average : b.share - a.share);
  const slide = SLIDES[mode];
  const slideIndex = mode === 'landing' ? 0 : 1;
  const headlineDirection = mode === 'volume' ? 1 : -1;

  useEffect(() => {
    const metricTimer = window.setTimeout(() => setMetricMode(mode), reduced ? 0 : METRIC_DELAY_MS);
    return () => window.clearTimeout(metricTimer);
  }, [mode, reduced]);

  const onWheel = (event: React.WheelEvent<HTMLElement>) => {
    if (Math.abs(event.deltaY) < WHEEL_MIN_DELTA) return;
    const next: Mode = event.deltaY > 0 ? 'volume' : 'landing';
    if (next === mode) return;
    const now = performance.now();
    if (now - wheelLockRef.current < WHEEL_COOLDOWN_MS) return;
    wheelLockRef.current = now;
    setLocalMode(next);
  };

  const onPointerDown = (event: React.PointerEvent<HTMLElement>) => {
    if (event.pointerType === 'mouse' && event.button !== 0) return;
    event.currentTarget.setPointerCapture(event.pointerId);
    gestureRef.current = { x: event.clientX, y: event.clientY };
  };

  const onPointerUp = (event: React.PointerEvent<HTMLElement>) => {
    const start = gestureRef.current;
    gestureRef.current = null;
    if (event.currentTarget.hasPointerCapture(event.pointerId)) event.currentTarget.releasePointerCapture(event.pointerId);
    if (!start) return;
    const deltaY = start.y - event.clientY;
    const deltaX = start.x - event.clientX;
    if (Math.abs(deltaY) < SWIPE_MIN_DISTANCE || Math.abs(deltaY) < Math.abs(deltaX) * 1.2) return;
    const next: Mode = deltaY > 0 ? 'volume' : 'landing';
    if (next === mode) return;
    setLocalMode(next);
  };

  const Root = embedded ? 'section' : 'main';

  return (
    <Root
      aria-label={`Lane leaders · slide ${slideIndex + 1} of 2 · ${mode}`}
      data-embedded={embedded ? 'true' : 'false'}
      onWheel={ownsNavigation ? onWheel : undefined}
      onPointerDown={ownsNavigation ? onPointerDown : undefined}
      onPointerUp={ownsNavigation ? onPointerUp : undefined}
      onPointerCancel={ownsNavigation ? () => { gestureRef.current = null; } : undefined}
      className={`lane-leader-page flex w-full touch-pan-x items-center overflow-x-hidden bg-black px-3 text-white sm:px-8 lg:px-12 ${embedded ? 'box-border h-[100dvh] min-h-0' : 'min-h-[100svh] min-h-[100dvh]'}`}
      style={{
        paddingTop: 'max(28px, env(safe-area-inset-top, 0px))',
        paddingBottom: 'calc(72px + env(safe-area-inset-bottom, 0px))',
      }}
    >
      <style>{`
        .lane-leader-stack {
          --lane-height: clamp(96px, calc(31.25vw - 26.25px), 112px);
          --lane-gap: 14px;
          height: calc(var(--lane-height) + var(--lane-height) + var(--lane-height) + var(--lane-gap) + var(--lane-gap));
        }
        .lane-leader-rail { height: var(--lane-height); }
        .lane-leader-row {
          width: var(--lane-row-mobile);
          transition: width 700ms cubic-bezier(0.77, 0, 0.175, 1), transform 700ms cubic-bezier(0.77, 0, 0.175, 1);
        }
        .lane-leader-row[data-lane-rank='0'] { transform: translate3d(0, 0, 0); }
        .lane-leader-row[data-lane-rank='1'] { transform: translate3d(0, calc(var(--lane-height) + var(--lane-gap)), 0); }
        .lane-leader-row[data-lane-rank='2'] { transform: translate3d(0, calc(var(--lane-height) + var(--lane-height) + var(--lane-gap) + var(--lane-gap)), 0); }
        @media (prefers-reduced-motion: reduce) {
          .lane-leader-row { transition: none; }
        }
        .lane-leader-track { width: calc(100vw - 84px); }
        @media (min-width: 640px) {
          .lane-leader-stack { --lane-height: 140px; --lane-gap: 18px; }
          .lane-leader-row { width: var(--lane-row-desktop); }
          .lane-leader-track { width: 452px; }
          .lane-leader-page[data-embedded='true'] .lane-leader-title-row { margin-top: 28px; }
          .lane-leader-page[data-embedded='true'] .lane-leader-section { margin-top: 32px; }
          .lane-leader-page[data-embedded='true'] .lane-leader-stack { --lane-height: 124px; --lane-gap: 14px; }
          .lane-leader-page[data-embedded='true'] .lane-leader-footer { margin-top: 24px; }
        }
        @media (max-height: 620px) and (max-width: 639px) {
          .lane-leader-page { align-items: flex-start; }
          .lane-leader-title-row { margin-top: 14px; }
          .lane-leader-section { margin-top: 18px; }
          .lane-leader-stack { --lane-height: 84px; --lane-gap: 8px; margin-top: 10px; }
          .lane-leader-footer { margin-top: 12px; padding-top: 12px; }
        }
      `}</style>
      <span className="sr-only" aria-live="polite">Slide {slideIndex + 1} of 2: {mode}</span>
      <div className="mx-auto w-full max-w-[720px]">
        <div className="flex items-center justify-between gap-5">
          <span className="text-[9px] font-black uppercase tracking-[0.2em] text-[#E11D48]">Lane leaders</span>
          {embedded ? (
            <span className="text-[8px] font-black uppercase tracking-[0.16em] text-white/34">
              {contextLabel}
            </span>
          ) : (
            <Link href="/drop/artifacts" className="text-[8px] font-black uppercase tracking-[0.16em] text-white/34 transition-colors hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]">
              All artifacts
            </Link>
          )}
        </div>

        <div className="lane-leader-title-row mt-7 flex items-end justify-between gap-6 sm:mt-10">
          <AnimatePresence custom={headlineDirection} mode="popLayout" initial={false}>
            <motion.h1
              key={mode}
              custom={headlineDirection}
              variants={HEADLINE_VARIANTS}
              initial={reduced ? { opacity: 0 } : 'enter'}
              animate={reduced
                ? { opacity: 1, transition: { duration: 0.16, ease: EASE } }
                : 'center'}
              exit={reduced
                ? { opacity: 0, transition: { duration: 0.12, ease: EASE } }
                : 'exit'}
              className="max-w-[12ch] text-[clamp(34px,9vw,72px)] font-black leading-[0.88] tracking-[-0.06em]"
            >
              {slide.headline}
            </motion.h1>
          </AnimatePresence>
          <span className="hidden shrink-0 text-[9px] font-black uppercase tracking-[0.16em] text-white/34 sm:block">
            0{slideIndex + 1} / 02
          </span>
        </div>

        <section aria-label={`${mode} lane ranking`} className="lane-leader-section mt-9 w-full sm:mt-12 sm:max-w-[540px]">
          <div className="flex items-center justify-between gap-5 border-b border-white/10 pb-3">
            <span className="text-[8px] font-black uppercase tracking-[0.17em] text-white/32">
              {slide.context}
            </span>
            <span className="text-[8px] font-black uppercase tracking-[0.17em] text-white/24">
              0{slideIndex + 1} / 02 · {embedded ? 'scroll' : 'swipe'}
            </span>
          </div>

          <div data-order-mode={mode} className="lane-leader-stack relative mt-4 sm:mt-5">
            {lanes.map((lane) => (
              <LaneRail
                key={lane.id}
                lane={lane}
                mode={mode}
                metricMode={metricMode}
                reduced={reduced}
                index={ordered.findIndex((item) => item.id === lane.id)}
              />
            ))}
          </div>
        </section>

        <div className="lane-leader-footer mt-7 flex w-full items-center justify-between gap-5 border-t border-white/10 pt-4 sm:mt-10 sm:max-w-[540px]">
          <span className="text-[8px] font-black uppercase tracking-[0.15em] text-white/30">
            {slide.footer}
          </span>
          {embedded ? (
            <div className="flex h-11 items-center gap-5" aria-label={`Lane leader slide ${slideIndex + 1} of 2`}>
              {(['landing', 'volume'] as const).map((item) => (
                <span
                  aria-hidden="true"
                  key={item}
                  className={`block h-[2px] w-5 ${mode === item ? 'bg-[#E11D48]' : 'bg-white/22'}`}
                />
              ))}
            </div>
          ) : (
            <div className="flex items-center" aria-label="Lane leader slides">
              {(['landing', 'volume'] as const).map((item) => (
                <button
                  key={item}
                  type="button"
                  onClick={() => setLocalMode(item)}
                  aria-label={`Go to ${item} slide`}
                  aria-current={mode === item ? 'step' : undefined}
                  className="grid h-11 w-11 place-items-center focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]"
                >
                  <span className={`block h-[2px] w-5 transition-colors ${mode === item ? 'bg-[#E11D48]' : 'bg-white/22'}`} />
                </button>
              ))}
            </div>
          )}
        </div>
      </div>
    </Root>
  );
}
