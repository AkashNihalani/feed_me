'use client';

/* ─────────────────────────────────────────────────────────────
   THE RECORDS DESK — streaks, records and firsts as a full-bleed
   deck. One record per slide; the visual is the record's own
   mechanism: a threshold crossed, a shelf entered, a chain
   extended, a drought ended. Every fact here is server math over
   run history — the gate that lets a slide print is the novelty
   check, so this layer cannot repeat itself.

   Grammar (the Lane Leaders bar):
   · one idea, one screen, giant headline, hairline chrome
   · the posts are the material — thumbnails inside the shapes
   · layout settles first, numerals roll after (METRIC_DELAY)
   · wheel / swipe / arrows between slides, markers in the footer
   ───────────────────────────────────────────────────────────── */

import Link from 'next/link';
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion';
import { useCallback, useEffect, useRef, useState } from 'react';
import Odometer from '@/components/login/Odometer';
import { cn } from '@/lib/utils';

export type RecordPost = {
  id: string;
  title: string;
  thumbnail?: string;
  url?: string;
};

const EASE = [0.22, 1, 0.36, 1] as const;
const METRIC_DELAY_MS = 760;
const WHEEL_COOLDOWN_MS = 640;
const WHEEL_MIN_DELTA = 24;
const SWIPE_MIN_DISTANCE = 46;

const DECK_VARIANTS = {
  enter: (direction: number) => ({
    opacity: 0,
    transform: `translate3d(0, ${direction > 0 ? '14vh' : '-14vh'}, 0)`,
  }),
  center: {
    opacity: 1,
    transform: 'translate3d(0, 0, 0)',
    transition: { duration: 0.6, ease: EASE },
  },
  exit: (direction: number) => ({
    opacity: 0,
    transform: `translate3d(0, ${direction > 0 ? '-10vh' : '10vh'}, 0)`,
    transition: { duration: 0.36, ease: EASE },
  }),
} as const;

const RISE = {
  hidden: { opacity: 0, transform: 'translate3d(0, 22px, 0)' },
  visible: {
    opacity: 1,
    transform: 'translate3d(0, 0, 0)',
    transition: { duration: 0.6, ease: EASE },
  },
} as const;

const STAGE = {
  hidden: {},
  visible: { transition: { staggerChildren: 0.14, delayChildren: 0.1 } },
} as const;

const TILE_BACKGROUNDS = [
  'linear-gradient(145deg, #4c0820, #E11D48 135%)',
  'linear-gradient(145deg, #0e2966, #2563eb 140%)',
  'linear-gradient(145deg, #0d5138, #10b981 140%)',
  'linear-gradient(145deg, #783a1c, #f97316 140%)',
];

const FALLBACK_POSTS: RecordPost[] = Array.from({ length: 14 }, (_, index) => ({
  id: `fallback-${index}`,
  title: `Post ${index + 1}`,
}));

function tileBackground(post: RecordPost, index: number) {
  const fallback = TILE_BACKGROUNDS[index % TILE_BACKGROUNDS.length];
  return post.thumbnail ? `url("${post.thumbnail}"), ${fallback}` : fallback;
}

/* One post as a tile — the only visual atom on the desk. */
function Tile({
  post,
  index = 0,
  accent = false,
  dim = false,
  label,
  className,
}: {
  post: RecordPost;
  index?: number;
  accent?: boolean;
  dim?: boolean;
  label?: string;
  className?: string;
}) {
  return (
    <span
      className={cn(
        'relative block overflow-hidden rounded-[12px] border bg-[#131313] bg-cover bg-center',
        accent ? 'border-[#E11D48]/75 shadow-[0_22px_50px_-20px_rgba(225,29,72,0.55)]' : 'border-white/14',
        dim && 'opacity-40 grayscale',
        className,
      )}
      style={{ aspectRatio: '4 / 5', backgroundImage: tileBackground(post, index) }}
    >
      <span className="absolute inset-x-0 bottom-0 h-1/2 bg-gradient-to-t from-black/78 to-transparent" />
      <span className="absolute inset-x-0 top-0 h-px bg-white/25" />
      <span className="absolute inset-x-1.5 bottom-1.5 line-clamp-2 text-[8px] font-black leading-[1.05] tracking-[-0.02em] text-white/82">
        {post.title}
      </span>
      {label ? (
        <span className={cn(
          'absolute left-0 top-0 px-1.5 py-1 text-[6.5px] font-black uppercase tracking-[0.13em]',
          accent ? 'bg-[#E11D48] text-white' : 'bg-black/72 text-white/72',
        )}>
          {label}
        </span>
      ) : null}
    </span>
  );
}

function Eyebrow({ children, accent = false }: { children: React.ReactNode; accent?: boolean }) {
  return (
    <span className={cn('text-[8px] font-black uppercase tracking-[0.17em]', accent ? 'text-[#E11D48]' : 'text-white/32')}>
      {children}
    </span>
  );
}

/* Metric block — rolls only after the layout has settled. */
function Metric({
  value,
  prefix,
  suffix,
  caption,
  reduced,
  align = 'right',
}: {
  value: number;
  prefix?: string;
  suffix?: string;
  caption: string;
  reduced: boolean;
  align?: 'left' | 'right';
}) {
  return (
    <span className={cn('block', align === 'right' ? 'text-right' : 'text-left')}>
      <span className={cn('flex items-baseline font-black leading-none text-white', align === 'right' ? 'justify-end' : 'justify-start')}>
        {prefix ? <span className="mr-1 text-[15px] tracking-[-0.03em] text-white/45 sm:text-[18px]">{prefix}</span> : null}
        <Odometer
          value={value}
          animateOnMount
          revealDelayMs={reduced ? 0 : METRIC_DELAY_MS}
          className="text-[clamp(40px,10vw,68px)] tabular-nums tracking-[-0.07em]"
        />
        {suffix ? <span className="ml-0.5 text-[0.44em] text-[clamp(40px,10vw,68px)] font-black text-white/50">{suffix}</span> : null}
      </span>
      <span className="mt-2 block text-[8px] font-black uppercase tracking-[0.16em] text-white/32">{caption}</span>
    </span>
  );
}

/* ── 01 · PERSONAL BEST — a threshold, standing; a post, crossing it.
   The dashed line is the old record. The card starts below it and
   physically climbs past; the line concedes with an accent flash. */
function PersonalBestStage({ posts, reduced }: { posts: RecordPost[]; reduced: boolean }) {
  return (
    <div className="relative h-[min(46vh,380px)] w-full">
      {/* the old record — a line that has held */}
      <motion.div
        className="absolute inset-x-0 top-[30%]"
        initial={reduced ? false : { opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.5, ease: EASE }}
      >
        <span className="block border-t border-dashed border-white/28" />
        <span className="mt-2 flex items-baseline justify-between">
          <span className="text-[8px] font-black uppercase tracking-[0.16em] text-white/34">
            old best · top 7% · stood 11 runs
          </span>
          <motion.span
            className="text-[8px] font-black uppercase tracking-[0.16em] text-[#E11D48]"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: reduced ? 0 : 1.15, duration: 0.4, ease: EASE }}
          >
            beaten
          </motion.span>
        </span>
      </motion.div>

      {/* the run's other posts — settled, below the line */}
      <div className="absolute bottom-0 left-0 flex items-end">
        {posts.slice(1, 4).map((post, index) => (
          <motion.div
            key={post.id}
            className={cn('w-[clamp(52px,13vw,72px)]', index > 0 && '-ml-3')}
            style={{ zIndex: 3 - index }}
            initial={reduced ? false : { opacity: 0, transform: 'translate3d(0, 26px, 0)' }}
            animate={{ opacity: 1, transform: 'translate3d(0, 0, 0)' }}
            transition={{ delay: reduced ? 0 : 0.25 + index * 0.09, duration: 0.6, ease: EASE }}
          >
            <Tile post={post} index={index + 1} dim />
          </motion.div>
        ))}
        <span className="mb-1 ml-3 text-[8px] font-black uppercase tracking-[0.15em] text-white/28">the rest of the run</span>
      </div>

      {/* the record breaker — starts under the line, ends above it */}
      <motion.div
        className="absolute left-[54%] top-0 w-[clamp(108px,27vw,150px)] sm:left-[58%]"
        initial={reduced ? { opacity: 0 } : { opacity: 0, transform: 'translate3d(0, 52%, 0) scale(0.94)' }}
        animate={{ opacity: 1, transform: 'translate3d(0, 0%, 0) scale(1)' }}
        transition={{ delay: reduced ? 0 : 0.45, duration: 0.95, ease: EASE }}
      >
        <Tile post={posts[0]} accent label="new best · top 4%" />
      </motion.div>

      <div className="absolute bottom-0 right-0">
        <Metric value={4} prefix="top" suffix="%" caption="best landing on record" reduced={reduced} />
      </div>
    </div>
  );
}

/* ── 02 · CLIMB STREAK — three run rails, each reaching further.
   Width is the metric: the rail IS the average landing. */
function ClimbStage({ posts, reduced }: { posts: RecordPost[]; reduced: boolean }) {
  const runs = [
    { run: '05', pct: 44, width: '56%', posts: posts.slice(0, 3) },
    { run: '06', pct: 31, width: '74%', posts: posts.slice(3, 6) },
    { run: '07', pct: 18, width: '100%', posts: posts.slice(6, 9), accent: true },
  ];
  return (
    <div className="w-full">
      {runs.map((item, index) => (
        <motion.div
          key={item.run}
          className="mt-3 first:mt-0 sm:mt-4"
          initial={reduced ? false : { opacity: 0, transform: 'translate3d(-28px, 0, 0)' }}
          animate={{ opacity: 1, transform: 'translate3d(0, 0, 0)' }}
          transition={{ delay: reduced ? 0 : 0.2 + index * 0.22, duration: 0.7, ease: EASE }}
        >
          <div className="flex items-center gap-3 sm:gap-4">
            <div
              className={cn(
                'relative h-[clamp(58px,14vw,84px)] min-w-0 overflow-hidden rounded-[14px] border',
                item.accent ? 'border-[#E11D48]/70' : 'border-white/14',
              )}
              style={{ width: item.width }}
            >
              <div className="grid h-full grid-cols-3">
                {item.posts.map((post, postIndex) => (
                  <span
                    key={`${post.id}:${postIndex}`}
                    className="relative bg-cover bg-center"
                    style={{ backgroundImage: tileBackground(post, postIndex) }}
                  >
                    <span className="absolute inset-y-0 right-0 w-px bg-white/12" />
                  </span>
                ))}
              </div>
              <span className="absolute inset-y-0 left-0 w-[52%] bg-gradient-to-r from-black/85 via-black/50 to-transparent" />
              <span className="absolute inset-y-0 left-3.5 flex items-center text-[11px] font-black uppercase tracking-[0.14em] text-white sm:left-4 sm:text-[12px]">
                run {item.run}
              </span>
            </div>
            <span className={cn(
              'shrink-0 text-[19px] font-black tabular-nums leading-none tracking-[-0.05em] sm:text-[24px]',
              item.accent ? 'text-[#E11D48]' : 'text-white/38',
            )}>
              {item.pct}<span className="text-[0.55em] text-white/40">%</span>
            </span>
          </div>
        </motion.div>
      ))}
      <motion.div
        className="mt-6 flex items-baseline justify-between border-t border-white/10 pt-4 sm:mt-8"
        initial={reduced ? false : { opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: reduced ? 0 : 0.95, duration: 0.5, ease: EASE }}
      >
        <span className="text-[8px] font-black uppercase tracking-[0.16em] text-white/32">further right is closer to the top</span>
        <Metric value={3} caption="runs climbing, unbroken" reduced={reduced} />
      </motion.div>
    </div>
  );
}

/* ── 03 · A FIRST ON RECORD — the top band is a shelf reels owned.
   A carousel arrives from below and takes the vacant slot. */
function FirstStage({ posts, reduced }: { posts: RecordPost[]; reduced: boolean }) {
  return (
    <div className="w-full">
      <motion.div
        initial={reduced ? false : { opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.5, ease: EASE }}
      >
        <div className="flex items-baseline justify-between">
          <Eyebrow accent>the top quarter</Eyebrow>
          <Eyebrow>reels only — until this run</Eyebrow>
        </div>
        <div className="mt-3 grid grid-cols-4 gap-2 rounded-[18px] border border-white/14 p-2.5 sm:gap-3 sm:p-3">
          {posts.slice(0, 3).map((post, index) => (
            <Tile key={post.id} post={post} index={index} label="reel" />
          ))}
          {/* the vacant slot the first fills */}
          <motion.div
            className="relative"
            initial={reduced ? { opacity: 0 } : { opacity: 0, transform: 'translate3d(0, 96px, 0) scale(0.92)' }}
            animate={{ opacity: 1, transform: 'translate3d(0, 0, 0) scale(1)' }}
            transition={{ delay: reduced ? 0 : 0.55, duration: 0.85, ease: EASE }}
          >
            <Tile post={posts[3] ?? posts[0]} index={3} accent label="carousel" />
          </motion.div>
        </div>
      </motion.div>
      <div className="mt-6 flex items-end justify-between gap-6 sm:mt-8">
        <motion.p
          className="max-w-[30ch] text-[11px] font-black leading-relaxed text-white/45 sm:text-[13px]"
          initial={reduced ? false : { opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: reduced ? 0 : 1.05, duration: 0.5, ease: EASE }}
        >
          58 posts in the memory. This is the first carousel to land in its top quarter.
        </motion.p>
        <Metric value={1} prefix="no." caption="first of its kind on record" reduced={reduced} />
      </div>
    </div>
  );
}

/* ── 04 · STREAK ALIVE — a chain of landings, lit one by one.
   The count is the news; the last tile carries the accent. */
function StreakStage({ posts, reduced }: { posts: RecordPost[]; reduced: boolean }) {
  const chain = posts.slice(0, 7);
  return (
    <div className="w-full">
      <div className="flex items-end gap-1.5 sm:gap-2.5">
        {chain.map((post, index) => {
          const last = index === chain.length - 1;
          return (
            <motion.div
              key={`${post.id}:${index}`}
              className="min-w-0 flex-1"
              initial={reduced ? false : { opacity: 0, transform: 'translate3d(0, 20px, 0) scale(0.9)' }}
              animate={{ opacity: 1, transform: 'translate3d(0, 0, 0) scale(1)' }}
              transition={{ delay: reduced ? 0 : 0.18 + index * 0.11, duration: 0.55, ease: EASE }}
            >
              <Tile post={post} index={index} accent={last} />
              <span className={cn(
                'mt-2 block text-center text-[8px] font-black tabular-nums uppercase tracking-[0.12em]',
                last ? 'text-[#E11D48]' : 'text-white/30',
              )}>
                {last ? 'this run' : `−${chain.length - 1 - index}`}
              </span>
            </motion.div>
          );
        })}
      </div>
      <motion.div
        className="mt-7 flex items-end justify-between border-t border-white/10 pt-4 sm:mt-9"
        initial={reduced ? false : { opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: reduced ? 0 : 1, duration: 0.5, ease: EASE }}
      >
        <span className="max-w-[26ch] text-[11px] font-black leading-relaxed text-white/45 sm:text-[13px]">
          Every settled post since run 05 has landed in the top half.
        </span>
        <Metric value={7} caption="straight · a record" reduced={reduced} />
      </motion.div>
    </div>
  );
}

/* ── 05 · THE DROUGHT BROKE — three runs of empty top band,
   then a post rises into it. Absence drawn, then ended. */
function DroughtStage({ posts, reduced }: { posts: RecordPost[]; reduced: boolean }) {
  const runs = [
    { run: '04', posts: posts.slice(0, 2) },
    { run: '05', posts: posts.slice(2, 4) },
    { run: '06', posts: posts.slice(4, 6) },
    { run: '07', posts: posts.slice(6, 8), breaks: true },
  ];
  return (
    <div className="w-full">
      <div className="grid grid-cols-4 gap-2.5 sm:gap-5">
        {runs.map((item, index) => (
          <motion.div
            key={item.run}
            className="min-w-0"
            initial={reduced ? false : { opacity: 0, transform: 'translate3d(0, 18px, 0)' }}
            animate={{ opacity: 1, transform: 'translate3d(0, 0, 0)' }}
            transition={{ delay: reduced ? 0 : 0.15 + index * 0.1, duration: 0.55, ease: EASE }}
          >
            {/* the top band — empty until the last run */}
            <div className={cn(
              'relative h-[clamp(64px,16vw,92px)] rounded-[12px] border border-dashed',
              item.breaks ? 'border-[#E11D48]/60' : 'border-white/16',
            )}>
              {item.breaks ? (
                <motion.div
                  className="absolute inset-1"
                  initial={reduced ? { opacity: 0 } : { opacity: 0, transform: 'translate3d(0, 105%, 0)' }}
                  animate={{ opacity: 1, transform: 'translate3d(0, 0, 0)' }}
                  transition={{ delay: reduced ? 0 : 0.85, duration: 0.8, ease: EASE }}
                >
                  <span
                    className="block h-full w-full rounded-[9px] border border-[#E11D48]/75 bg-cover bg-center"
                    style={{ backgroundImage: tileBackground(item.posts[0] ?? posts[0], 0) }}
                  />
                </motion.div>
              ) : (
                <span className="absolute inset-0 grid place-items-center text-[7px] font-black uppercase tracking-[0.14em] text-white/22">
                  empty
                </span>
              )}
            </div>
            {/* where the runs actually landed */}
            <div className="mt-2 flex gap-1.5">
              {item.posts.map((post, postIndex) => (
                <span
                  key={`${post.id}:${postIndex}`}
                  className={cn(
                    'block h-[clamp(30px,8vw,44px)] min-w-0 flex-1 rounded-[7px] border border-white/10 bg-cover bg-center',
                    !item.breaks && 'opacity-45 grayscale',
                  )}
                  style={{ backgroundImage: tileBackground(post, postIndex + 1) }}
                />
              ))}
            </div>
            <span className={cn(
              'mt-2 block text-center text-[8px] font-black uppercase tracking-[0.13em]',
              item.breaks ? 'text-[#E11D48]' : 'text-white/30',
            )}>
              run {item.run}
            </span>
          </motion.div>
        ))}
      </div>
      <motion.div
        className="mt-6 flex items-baseline justify-between border-t border-white/10 pt-4 sm:mt-8"
        initial={reduced ? false : { opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: reduced ? 0 : 1.25, duration: 0.5, ease: EASE }}
      >
        <span className="text-[8px] font-black uppercase tracking-[0.16em] text-white/32">the top band · run by run</span>
        <span className="text-[8px] font-black uppercase tracking-[0.16em] text-[#E11D48]">drought over · 3 runs</span>
      </motion.div>
    </div>
  );
}

/* ── 06 · LANE CHARACTER — the one artifact the reader writes.
   Two lanes, each with its standing read; a read is re-earned
   only when evidence breaks it, so it carries tenure like a Bite. */
function CharacterStage({ posts, reduced }: { posts: RecordPost[]; reduced: boolean }) {
  const lanes = [
    {
      label: 'Reels',
      read: 'Where the account plays — skits, hecklers, escalations that need an audience.',
      meta: 'read held · 3 runs',
      posts: posts.slice(0, 4),
      accent: false,
    },
    {
      label: 'Carousels',
      read: 'Where it sells — kits, claims, step-by-step proof in a formal voice.',
      meta: 'recast this run',
      posts: posts.slice(4, 8),
      accent: true,
    },
  ];
  return (
    <div className="w-full">
      {lanes.map((lane, index) => (
        <motion.div
          key={lane.label}
          className="mt-6 first:mt-0 sm:mt-8"
          initial={reduced ? false : { opacity: 0, transform: 'translate3d(0, 24px, 0)' }}
          animate={{ opacity: 1, transform: 'translate3d(0, 0, 0)' }}
          transition={{ delay: reduced ? 0 : 0.2 + index * 0.28, duration: 0.7, ease: EASE }}
        >
          <div className={cn(
            'relative h-[clamp(64px,15vw,88px)] overflow-hidden rounded-[16px] border',
            lane.accent ? 'border-[#E11D48]/65' : 'border-white/14',
          )}>
            <div className="grid h-full grid-cols-4">
              {lane.posts.map((post, postIndex) => (
                <span
                  key={`${post.id}:${postIndex}`}
                  className="relative bg-cover bg-center"
                  style={{ backgroundImage: tileBackground(post, postIndex) }}
                >
                  <span className="absolute inset-y-0 right-0 w-px bg-white/12" />
                </span>
              ))}
            </div>
            <span className="absolute inset-y-0 left-0 w-[46%] bg-gradient-to-r from-black/88 via-black/52 to-transparent" />
            <span className="absolute inset-y-0 left-4 flex items-center text-[14px] font-black leading-none tracking-[-0.03em] text-white sm:text-[16px]">
              {lane.label}
            </span>
          </div>
          <div className="mt-3 flex items-baseline justify-between gap-5">
            <p className="min-w-0 max-w-[44ch] text-[13px] font-black leading-snug tracking-[-0.01em] text-white/78 sm:text-[15px]">
              “{lane.read}”
            </p>
            <span className={cn(
              'shrink-0 text-[8px] font-black uppercase tracking-[0.14em]',
              lane.accent ? 'text-[#E11D48]' : 'text-white/34',
            )}>
              {lane.meta}
            </span>
          </div>
        </motion.div>
      ))}
      <motion.p
        className="mt-6 border-t border-white/10 pt-4 text-[8px] font-black uppercase tracking-[0.16em] text-white/32 sm:mt-8"
        initial={reduced ? false : { opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: reduced ? 0 : 0.95, duration: 0.5, ease: EASE }}
      >
        reader-written · numbers stamped by code · re-earned only when evidence breaks it
      </motion.p>
    </div>
  );
}

/* ── the deck ────────────────────────────────────────────────── */

type SlideDef = {
  id: string;
  kicker: string;
  headline: string[];
  gate: string;
  Stage: (props: { posts: RecordPost[]; reduced: boolean }) => React.ReactElement;
};

const SLIDE_DEFS: SlideDef[] = [
  {
    id: 'personal-best',
    kicker: 'record · the memory’s ceiling',
    headline: ['Best landing', 'on record.'],
    gate: 'Prints only when the 90-day best is beaten — silent every other week.',
    Stage: PersonalBestStage,
  },
  {
    id: 'climb',
    kicker: 'streak · run over run',
    headline: ['Three runs,', 'climbing.'],
    gate: 'Prints while the average landing improves — dies the week it flattens.',
    Stage: ClimbStage,
  },
  {
    id: 'first',
    kicker: 'first · never before',
    headline: ['Carousels', 'cracked the top.'],
    gate: 'Prints on any first: a lane’s first top-band post, a first clean sweep, a first no. 1.',
    Stage: FirstStage,
  },
  {
    id: 'streak-alive',
    kicker: 'streak · still going',
    headline: ['Seven straight,', 'top half.'],
    gate: 'Prints when a streak extends its own record — the count is the news.',
    Stage: StreakStage,
  },
  {
    id: 'drought',
    kicker: 'return · absence, ended',
    headline: ['The drought', 'broke.'],
    gate: 'Prints when a gap ends. The longer the silence, the louder this slide.',
    Stage: DroughtStage,
  },
  {
    id: 'character',
    kicker: 'lane character · the reader speaks',
    headline: ['Reels play.', 'Carousels sell.'],
    gate: 'The one slide the reader writes. Movement-gated like a Bite — never re-generated on a quiet week.',
    Stage: CharacterStage,
  },
];

export default function Records({ posts }: { posts: RecordPost[] }) {
  const reduced = Boolean(useReducedMotion());
  const source = posts.length >= 9 ? posts : [...posts, ...FALLBACK_POSTS];
  const [active, setActive] = useState(0);
  const [direction, setDirection] = useState(1);
  const activeRef = useRef(0);
  const wheelLockRef = useRef(0);
  const gestureRef = useRef<{ x: number; y: number } | null>(null);

  const goTo = useCallback((index: number) => {
    const clamped = Math.max(0, Math.min(SLIDE_DEFS.length - 1, index));
    if (clamped === activeRef.current) return;
    setDirection(clamped > activeRef.current ? 1 : -1);
    activeRef.current = clamped;
    setActive(clamped);
  }, []);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.defaultPrevented) return;
      const forward = event.key === 'ArrowDown' || event.key === 'ArrowRight' || event.key === 'PageDown' || event.key === ' ';
      const backward = event.key === 'ArrowUp' || event.key === 'ArrowLeft' || event.key === 'PageUp';
      if (!forward && !backward) return;
      event.preventDefault();
      goTo(activeRef.current + (forward ? 1 : -1));
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [goTo]);

  const onWheel = (event: React.WheelEvent<HTMLElement>) => {
    if (Math.abs(event.deltaY) < WHEEL_MIN_DELTA) return;
    const now = performance.now();
    if (now - wheelLockRef.current < WHEEL_COOLDOWN_MS) return;
    wheelLockRef.current = now;
    goTo(activeRef.current + (event.deltaY > 0 ? 1 : -1));
  };

  const onPointerDown = (event: React.PointerEvent<HTMLElement>) => {
    if (event.pointerType === 'mouse' && event.button !== 0) return;
    if ((event.target as HTMLElement).closest('button, a')) return;
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
    const now = performance.now();
    if (now - wheelLockRef.current < WHEEL_COOLDOWN_MS) return;
    wheelLockRef.current = now;
    goTo(activeRef.current + (deltaY > 0 ? 1 : -1));
  };

  const slide = SLIDE_DEFS[active];
  const SlideStage = slide.Stage;

  return (
    <main
      aria-label={`Records desk · slide ${active + 1} of ${SLIDE_DEFS.length}`}
      onWheel={onWheel}
      onPointerDown={onPointerDown}
      onPointerUp={onPointerUp}
      onPointerCancel={() => { gestureRef.current = null; }}
      className="relative flex h-[100dvh] w-full touch-pan-x items-center overflow-hidden bg-black px-5 text-white sm:px-8 lg:px-12"
      style={{
        paddingTop: 'max(24px, env(safe-area-inset-top, 0px))',
        paddingBottom: 'calc(64px + env(safe-area-inset-bottom, 0px))',
      }}
    >
      <span className="sr-only" aria-live="polite">
        Slide {active + 1} of {SLIDE_DEFS.length}: {slide.headline.join(' ')}
      </span>

      {/* fixed chrome */}
      <div
        className="absolute inset-x-5 z-20 flex items-center justify-between gap-5 sm:inset-x-8 lg:inset-x-12"
        style={{ top: 'max(24px, env(safe-area-inset-top, 0px))' }}
      >
        <span className="text-[9px] font-black uppercase tracking-[0.2em] text-[#E11D48]">Records desk</span>
        <span className="flex items-center gap-5">
          <span className="hidden text-[8px] font-black uppercase tracking-[0.16em] text-white/28 sm:block">
            server math · prints only when history breaks
          </span>
          <Link
            href="/drop"
            className="text-[8px] font-black uppercase tracking-[0.16em] text-white/34 transition-colors hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]"
          >
            Back to Reader
          </Link>
        </span>
      </div>

      {/* the slide */}
      <div className="relative mx-auto h-full w-full max-w-[720px]">
        <AnimatePresence initial={false} custom={direction} mode="popLayout">
          <motion.div
            key={slide.id}
            custom={direction}
            variants={DECK_VARIANTS}
            initial={reduced ? { opacity: 0 } : 'enter'}
            animate={reduced ? { opacity: 1, transition: { duration: 0.16, ease: EASE } } : 'center'}
            exit={reduced ? { opacity: 0, transition: { duration: 0.12, ease: EASE } } : 'exit'}
            className="absolute inset-0 flex items-center"
          >
            <motion.div className="w-full" variants={STAGE} initial="hidden" animate="visible">
              <motion.div variants={RISE}>
                <Eyebrow accent>{slide.kicker}</Eyebrow>
                <span className="ml-4 text-[8px] font-black uppercase tracking-[0.16em] text-white/28">
                  {String(active + 1).padStart(2, '0')} / {String(SLIDE_DEFS.length).padStart(2, '0')}
                </span>
              </motion.div>
              <motion.h1
                variants={RISE}
                className="mt-4 max-w-[12ch] text-[clamp(34px,8.6vw,68px)] font-black leading-[0.88] tracking-[-0.06em] sm:mt-5"
              >
                {slide.headline.map((line) => (
                  <span key={line} className="block">{line}</span>
                ))}
              </motion.h1>
              <motion.div variants={RISE} className="mt-7 sm:mt-10">
                <SlideStage posts={source} reduced={reduced} />
              </motion.div>
            </motion.div>
          </motion.div>
        </AnimatePresence>
      </div>

      {/* footer — the gate is the caption */}
      <div
        className="absolute inset-x-5 z-20 flex items-end justify-between gap-6 border-t border-white/10 pt-3.5 sm:inset-x-8 lg:inset-x-12"
        style={{ bottom: 'calc(20px + env(safe-area-inset-bottom, 0px))' }}
      >
        <AnimatePresence mode="wait" initial={false}>
          <motion.span
            key={slide.id}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1, transition: { duration: 0.3, ease: EASE } }}
            exit={{ opacity: 0, transition: { duration: 0.15, ease: EASE } }}
            className="max-w-[52ch] text-[8px] font-black uppercase leading-relaxed tracking-[0.14em] text-white/32"
          >
            {slide.gate}
          </motion.span>
        </AnimatePresence>
        <div className="flex shrink-0 items-center" aria-label="Record slides">
          {SLIDE_DEFS.map((item, index) => (
            <button
              key={item.id}
              type="button"
              onClick={() => goTo(index)}
              aria-label={`Go to ${item.headline.join(' ')}`}
              aria-current={index === active ? 'step' : undefined}
              className="grid h-10 w-7 place-items-center focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]"
            >
              <span className={cn('block h-[2px] w-4 transition-colors duration-300', index === active ? 'bg-[#E11D48]' : 'bg-white/22')} />
            </button>
          ))}
        </div>
      </div>
    </main>
  );
}
