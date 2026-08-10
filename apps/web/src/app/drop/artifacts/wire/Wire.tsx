'use client';

/* ─────────────────────────────────────────────────────────────
   THE WIRE — trigger artifacts beyond the D7 verdict. Every
   slide is a signal the worker already detects, drawn as its own
   mechanism: an early post outrunning its same-age cohort, a
   verdict amended at D21, a three-week hold, a fall, a comment
   storm, a follower wave, a top-ten displacement.

   Rules of the desk:
   · D7 stays the only judgment. Everything early is same-age
     cohort only; everything late is an amendment, never a rewrite.
   · pure server math — the signal taxonomy fires the slide,
     the gate line in the footer is the print condition.
   · grammar as the Reader: one idea per screen, posts as the
     material, layout settles first, numerals roll after.
   ───────────────────────────────────────────────────────────── */

import Link from 'next/link';
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion';
import { useCallback, useEffect, useRef, useState } from 'react';
import Odometer from '@/components/login/Odometer';
import { cn } from '@/lib/utils';

export type WirePost = {
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

const FALLBACK_POSTS: WirePost[] = Array.from({ length: 14 }, (_, index) => ({
  id: `fallback-${index}`,
  title: `Post ${index + 1}`,
}));

function tileBackground(post: WirePost, index: number) {
  const fallback = TILE_BACKGROUNDS[index % TILE_BACKGROUNDS.length];
  return post.thumbnail ? `url("${post.thumbnail}"), ${fallback}` : fallback;
}

function Tile({
  post,
  index = 0,
  accent = false,
  dim = false,
  label,
  className,
}: {
  post: WirePost;
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

/* ── 01 · IGNITION — a day-one post outrunning its own cohort.
   Judged only against other day-ones; the D7 verdict stays shut. */
function IgnitionStage({ posts, reduced }: { posts: WirePost[]; reduced: boolean }) {
  return (
    <div className="relative h-[min(44vh,360px)] w-full">
      {/* the same-age band it left behind */}
      <div className="absolute inset-x-0 bottom-0">
        <span className="block border-t border-white/12" />
        <div className="mt-3 flex items-end gap-2">
          {posts.slice(1, 5).map((post, index) => (
            <motion.div
              key={post.id}
              className="w-[clamp(44px,11vw,60px)]"
              initial={reduced ? false : { opacity: 0, transform: 'translate3d(0, 16px, 0)' }}
              animate={{ opacity: 1, transform: 'translate3d(0, 0, 0)' }}
              transition={{ delay: reduced ? 0 : 0.2 + index * 0.07, duration: 0.5, ease: EASE }}
            >
              <Tile post={post} index={index + 1} dim />
            </motion.div>
          ))}
          <span className="mb-1 ml-2 text-[8px] font-black uppercase tracking-[0.15em] text-white/28">
            every other day-one post
          </span>
        </div>
      </div>

      {/* the top 5% line it crossed */}
      <motion.div
        className="absolute inset-x-0 top-[16%]"
        initial={reduced ? false : { opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.5, ease: EASE }}
      >
        <span className="block border-t border-dashed border-[#E11D48]/55" />
        <span className="mt-2 block text-[8px] font-black uppercase tracking-[0.16em] text-[#E11D48]">
          same-age top 5%
        </span>
      </motion.div>

      {/* the post — leaves the cohort fast, slight overshoot */}
      <motion.div
        className="absolute left-[8%] top-0 w-[clamp(96px,24vw,132px)] sm:left-[14%]"
        initial={reduced ? { opacity: 0 } : { opacity: 0, transform: 'translate3d(0, 86%, 0) scale(0.94)' }}
        animate={reduced
          ? { opacity: 1 }
          : { opacity: 1, transform: ['translate3d(0, 86%, 0) scale(0.94)', 'translate3d(0, -3%, 0) scale(1.01)', 'translate3d(0, 0%, 0) scale(1)'] }}
        transition={reduced ? { duration: 0.2 } : { delay: 0.5, duration: 1.05, times: [0, 0.82, 1], ease: EASE }}
      >
        <Tile post={posts[0]} accent label="one day old" />
      </motion.div>

      {/* the withheld verdict */}
      <motion.div
        className="absolute right-0 top-[6%] w-[clamp(120px,32vw,170px)] rounded-[14px] border border-white/16 p-3.5 sm:p-4"
        initial={reduced ? false : { opacity: 0, transform: 'translate3d(0, 14px, 0)' }}
        animate={{ opacity: 1, transform: 'translate3d(0, 0, 0)' }}
        transition={{ delay: reduced ? 0 : 0.95, duration: 0.55, ease: EASE }}
      >
        <Metric value={5} prefix="top" suffix="%" caption="of posts its exact age" reduced={reduced} align="left" />
        <span className="mt-3 block border-t border-white/10 pt-2.5 text-[7.5px] font-black uppercase tracking-[0.14em] text-white/34">
          D7 verdict · still closed
        </span>
      </motion.div>
    </div>
  );
}

/* ── 02 · THE LATE JUMP — the verdict closed at D7; the post
   didn't stop. D21 files an amendment beside it, never over it. */
function LateJumpStage({ posts, reduced }: { posts: WirePost[]; reduced: boolean }) {
  return (
    <div className="w-full">
      <div className="relative flex items-end justify-between gap-4">
        <motion.div
          className="w-[clamp(88px,22vw,124px)]"
          initial={reduced ? false : { opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ duration: 0.5, ease: EASE }}
        >
          <Tile post={posts[0]} dim label="D7 · top 48%" />
          <span className="mt-2 block text-[8px] font-black uppercase tracking-[0.14em] text-white/30">
            the verdict, on record
          </span>
        </motion.div>

        {/* the climb line */}
        <motion.span
          aria-hidden
          className="mb-14 hidden h-px min-w-0 flex-1 origin-left bg-gradient-to-r from-white/15 to-[#E11D48]/70 sm:block"
          initial={reduced ? false : { transform: 'scaleX(0)' }}
          animate={{ transform: 'scaleX(1)' }}
          transition={{ delay: reduced ? 0 : 0.5, duration: 0.7, ease: EASE }}
        />

        <motion.div
          className="w-[clamp(112px,28vw,156px)]"
          initial={reduced ? { opacity: 0 } : { opacity: 0, transform: 'translate3d(-36px, 26px, 0) scale(0.95)' }}
          animate={{ opacity: 1, transform: 'translate3d(0, 0, 0) scale(1)' }}
          transition={{ delay: reduced ? 0 : 0.65, duration: 0.85, ease: EASE }}
        >
          <Tile post={posts[0]} accent label="D21 · top 17%" />
          <span className="mt-2 block text-[8px] font-black uppercase tracking-[0.14em] text-[#E11D48]">
            the amendment
          </span>
        </motion.div>
      </div>

      <motion.div
        className="mt-7 flex items-end justify-between border-t border-white/10 pt-4 sm:mt-9"
        initial={reduced ? false : { opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: reduced ? 0 : 1, duration: 0.5, ease: EASE }}
      >
        <span className="max-w-[26ch] text-[11px] font-black leading-relaxed text-white/45 sm:text-[13px]">
          The D7 read stays exactly as written. D21 gets filed beside it.
        </span>
        <Metric value={31} prefix="+" caption="percentile points, after the verdict" reduced={reduced} />
      </motion.div>
    </div>
  );
}

/* ── 03 · THE HOLD — the same post in the top band at every
   checkpoint. Three weeks without giving the spot back. */
function SustainStage({ posts, reduced }: { posts: WirePost[]; reduced: boolean }) {
  const checkpoints = ['D1', 'D3', 'D7', 'D21'];
  return (
    <div className="w-full">
      <div className="relative">
        {/* the band it never left */}
        <motion.span
          aria-hidden
          className="absolute inset-x-0 top-0 h-px origin-left bg-[#E11D48]/60"
          initial={reduced ? false : { transform: 'scaleX(0)' }}
          animate={{ transform: 'scaleX(1)' }}
          transition={{ delay: reduced ? 0 : 0.25, duration: 1.1, ease: EASE }}
        />
        <div className="grid grid-cols-4 gap-2.5 pt-4 sm:gap-5">
          {checkpoints.map((checkpoint, index) => {
            const current = index === checkpoints.length - 1;
            return (
              <motion.div
                key={checkpoint}
                className="min-w-0"
                initial={reduced ? false : { opacity: 0, transform: 'translate3d(0, 22px, 0)' }}
                animate={{ opacity: 1, transform: 'translate3d(0, 0, 0)' }}
                transition={{ delay: reduced ? 0 : 0.3 + index * 0.18, duration: 0.6, ease: EASE }}
              >
                <Tile post={posts[0]} accent={current} dim={!current} className={cn(!current && '!opacity-70')} />
                <span className={cn(
                  'mt-2 block text-center text-[9px] font-black uppercase tracking-[0.14em]',
                  current ? 'text-[#E11D48]' : 'text-white/34',
                )}>
                  {checkpoint}
                </span>
                <span className={cn(
                  'block text-center text-[8px] font-black tabular-nums uppercase tracking-[0.1em]',
                  current ? 'text-white/70' : 'text-white/26',
                )}>
                  top {[4, 6, 8, 9][index]}%
                </span>
              </motion.div>
            );
          })}
        </div>
      </div>
      <motion.div
        className="mt-6 flex items-end justify-between border-t border-white/10 pt-4 sm:mt-8"
        initial={reduced ? false : { opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: reduced ? 0 : 1.1, duration: 0.5, ease: EASE }}
      >
        <span className="max-w-[26ch] text-[11px] font-black leading-relaxed text-white/45 sm:text-[13px]">
          Checked four times across three weeks. Never left the top 10%.
        </span>
        <Metric value={21} caption="days holding the band" reduced={reduced} />
      </motion.div>
    </div>
  );
}

/* ── 04 · THE FADE — a hot start giving it all back. The fall is
   drawn honestly: the same tile stepping down, losing its color. */
function FadeStage({ posts, reduced }: { posts: WirePost[]; reduced: boolean }) {
  const steps = [
    { checkpoint: 'D1', band: 'top 8%', offset: 0, dim: false },
    { checkpoint: 'D3', band: 'top 34%', offset: 34, dim: false },
    { checkpoint: 'D7', band: 'top 71%', offset: 78, dim: true },
  ];
  return (
    <div className="w-full">
      <div className="relative h-[min(38vh,300px)]">
        {/* the bands it fell through */}
        {[0, 44, 88].map((top, index) => (
          <span
            key={top}
            aria-hidden
            className="absolute inset-x-0 border-t border-dashed border-white/10"
            style={{ top: `${top + 4}%` }}
          >
            <span className="absolute right-0 top-1 text-[7px] font-black uppercase tracking-[0.14em] text-white/22">
              {['the top', 'the middle', 'the floor'][index]}
            </span>
          </span>
        ))}
        <div className="grid h-full grid-cols-3 items-start gap-3 sm:gap-6">
          {steps.map((step, index) => (
            <motion.div
              key={step.checkpoint}
              className="mx-auto w-[clamp(72px,19vw,104px)]"
              initial={reduced ? false : { opacity: 0, transform: 'translate3d(0, -14px, 0)' }}
              animate={{ opacity: 1, transform: `translate3d(0, ${step.offset}%, 0)` }}
              transition={{ delay: reduced ? 0 : 0.3 + index * 0.24, duration: 0.75, ease: EASE }}
            >
              <Tile post={posts[0]} dim={step.dim} accent={index === 0} label={`${step.checkpoint} · ${step.band}`} />
            </motion.div>
          ))}
        </div>
      </div>
      <motion.div
        className="mt-5 flex items-end justify-between border-t border-white/10 pt-4 sm:mt-7"
        initial={reduced ? false : { opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: reduced ? 0 : 1.1, duration: 0.5, ease: EASE }}
      >
        <span className="max-w-[26ch] text-[11px] font-black leading-relaxed text-white/45 sm:text-[13px]">
          The hook bought the first day. The next six asked for more.
        </span>
        <Metric value={63} prefix="−" caption="percentile points, D1 to D7" reduced={reduced} />
      </motion.div>
    </div>
  );
}

/* ── 05 · THE SPLIT — likes and comments disagree about the same
   week. One post got applause; one started a conversation. */
function SplitStage({ posts, reduced }: { posts: WirePost[]; reduced: boolean }) {
  const rows = [
    {
      post: posts[0],
      mode: 'conversation',
      note: 'comments 7× its baseline · likes flat',
      bars: [{ label: 'comments', width: '92%', accent: true }, { label: 'likes', width: '24%', accent: false }],
    },
    {
      post: posts[1] ?? posts[0],
      mode: 'applause',
      note: 'likes 4× its baseline · comments quiet',
      bars: [{ label: 'likes', width: '78%', accent: true }, { label: 'comments', width: '16%', accent: false }],
    },
  ];
  return (
    <div className="w-full">
      {rows.map((row, rowIndex) => (
        <motion.div
          key={row.mode}
          className="mt-6 flex items-center gap-4 first:mt-0 sm:mt-8 sm:gap-6"
          initial={reduced ? false : { opacity: 0, transform: 'translate3d(0, 22px, 0)' }}
          animate={{ opacity: 1, transform: 'translate3d(0, 0, 0)' }}
          transition={{ delay: reduced ? 0 : 0.2 + rowIndex * 0.26, duration: 0.65, ease: EASE }}
        >
          <div className="w-[clamp(72px,18vw,100px)] shrink-0">
            <Tile post={row.post} index={rowIndex} accent={rowIndex === 0} />
          </div>
          <div className="min-w-0 flex-1">
            <span className={cn(
              'text-[13px] font-black uppercase tracking-[0.1em] sm:text-[15px]',
              rowIndex === 0 ? 'text-[#E11D48]' : 'text-white',
            )}>
              {row.mode}
            </span>
            {row.bars.map((bar, barIndex) => (
              <div key={bar.label} className="mt-2.5 flex items-center gap-3">
                <span className="w-[62px] shrink-0 text-[8px] font-black uppercase tracking-[0.14em] text-white/34">
                  {bar.label}
                </span>
                <span className="relative block h-[9px] min-w-0 flex-1 overflow-hidden rounded-full bg-white/[0.07]">
                  <motion.span
                    className={cn('absolute inset-y-0 left-0 origin-left rounded-full', bar.accent ? 'bg-[#E11D48]' : 'bg-white/30')}
                    style={{ width: bar.width, boxShadow: bar.accent ? '0 0 14px rgba(225,29,72,0.4)' : undefined }}
                    initial={reduced ? false : { transform: 'scaleX(0)' }}
                    animate={{ transform: 'scaleX(1)' }}
                    transition={{ delay: reduced ? 0 : 0.55 + rowIndex * 0.26 + barIndex * 0.12, duration: 0.7, ease: EASE }}
                  />
                </span>
              </div>
            ))}
            <span className="mt-2 block text-[8px] font-black uppercase tracking-[0.13em] text-white/30">{row.note}</span>
          </div>
        </motion.div>
      ))}
      <motion.p
        className="mt-6 border-t border-white/10 pt-4 text-[8px] font-black uppercase tracking-[0.16em] text-white/32 sm:mt-8"
        initial={reduced ? false : { opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: reduced ? 0 : 1.15, duration: 0.5, ease: EASE }}
      >
        each measured against its own baseline · same D7 window
      </motion.p>
    </div>
  );
}

/* ── 06 · THE WAVE — the audience left its normal range. The count
   rolls; the week's posts stand under it, observed, not credited. */
function WaveStage({ posts, reduced }: { posts: WirePost[]; reduced: boolean }) {
  return (
    <div className="w-full">
      <div className="flex items-end justify-between gap-6 border-b border-white/12 pb-5">
        <span>
          <Eyebrow>seven days ago</Eyebrow>
          <span className="mt-2 block text-[20px] font-black tabular-nums tracking-[-0.04em] text-white/30 sm:text-[24px]">
            950,000
          </span>
        </span>
        <span className="min-w-0 text-right">
          <Eyebrow accent>now</Eyebrow>
          <Odometer
            value={958747}
            animateOnMount
            revealDelayMs={reduced ? 0 : METRIC_DELAY_MS}
            className="mt-2 block text-[clamp(36px,9vw,60px)] font-black tabular-nums tracking-[-0.06em] text-white"
          />
        </span>
      </div>
      <motion.div
        className="mt-4 flex flex-wrap items-baseline justify-between gap-4"
        initial={reduced ? false : { opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: reduced ? 0 : 0.9, duration: 0.5, ease: EASE }}
      >
        <span className="text-[17px] font-black tracking-[-0.03em] text-[#E11D48] sm:text-[20px]">+8,747</span>
        <span className="text-[8px] font-black uppercase tracking-[0.15em] text-[#E11D48]">
          5.6× its usual week · best in the 90-day memory
        </span>
      </motion.div>
      <motion.div
        className="mt-7 sm:mt-9"
        initial={reduced ? false : { opacity: 0, transform: 'translate3d(0, 18px, 0)' }}
        animate={{ opacity: 1, transform: 'translate3d(0, 0, 0)' }}
        transition={{ delay: reduced ? 0 : 0.55, duration: 0.6, ease: EASE }}
      >
        <Eyebrow>the week it happened · observed, not attributed</Eyebrow>
        <div className="mt-3 flex gap-2 sm:gap-3">
          {posts.slice(0, 5).map((post, index) => (
            <div key={`${post.id}:${index}`} className="w-[clamp(48px,12vw,68px)]">
              <Tile post={post} index={index} />
            </div>
          ))}
        </div>
      </motion.div>
    </div>
  );
}

/* ── 07 · THE SWAP — the overall top ten is finite. A new post
   forced its way in; something with tenure went out the bottom. */
function SwapStage({ posts, reduced }: { posts: WirePost[]; reduced: boolean }) {
  const standing = [
    { rank: '08', post: posts[2] ?? posts[0] },
    { rank: '09', post: posts[0], enters: true },
    { rank: '10', post: posts[3] ?? posts[0] },
  ];
  return (
    <div className="w-full max-w-[560px]">
      {standing.map((row, index) => (
        <motion.div
          key={row.rank}
          className={cn(
            'flex items-center gap-4 border-t py-3 sm:py-3.5',
            row.enters ? 'border-[#E11D48]/40' : 'border-white/10',
          )}
          initial={reduced
            ? false
            : row.enters
              ? { opacity: 0, transform: 'translate3d(-44px, 0, 0)' }
              : { opacity: 0, transform: 'translate3d(0, 14px, 0)' }}
          animate={{ opacity: 1, transform: 'translate3d(0, 0, 0)' }}
          transition={{ delay: reduced ? 0 : row.enters ? 0.62 : 0.2 + index * 0.09, duration: row.enters ? 0.8 : 0.5, ease: EASE }}
        >
          <span className={cn('w-8 shrink-0 text-[13px] font-black tabular-nums', row.enters ? 'text-[#E11D48]' : 'text-white/30')}>
            {row.rank}
          </span>
          <div className="w-[46px] shrink-0">
            <Tile post={row.post} index={index} accent={row.enters} />
          </div>
          <span className={cn('min-w-0 flex-1 truncate text-[13px] font-black tracking-tight sm:text-[15px]', row.enters ? 'text-white' : 'text-white/55')}>
            {row.post.title}
          </span>
          {row.enters ? (
            <span className="shrink-0 text-[8px] font-black uppercase tracking-[0.13em] text-[#E11D48]">entered this run</span>
          ) : null}
        </motion.div>
      ))}

      {/* the displaced — pushed below the line, tenure on the stone */}
      <motion.div
        className="flex items-center gap-4 border-t-2 border-white/16 py-3 sm:py-3.5"
        initial={reduced ? false : { opacity: 0 }}
        animate={{ opacity: 0.5 }}
        transition={{ delay: reduced ? 0 : 1.05, duration: 0.6, ease: EASE }}
      >
        <span className="w-8 shrink-0 text-[13px] font-black tabular-nums text-white/25">11</span>
        <div className="w-[46px] shrink-0">
          <Tile post={posts[4] ?? posts[0]} index={3} dim />
        </div>
        <span className="min-w-0 flex-1 truncate text-[13px] font-black tracking-tight text-white/40 line-through decoration-white/30 sm:text-[15px]">
          {(posts[4] ?? posts[0]).title}
        </span>
        <span className="shrink-0 text-[8px] font-black uppercase tracking-[0.13em] text-white/30">
          out · 34 days inside
        </span>
      </motion.div>
      <motion.p
        className="mt-4 text-[8px] font-black uppercase tracking-[0.15em] text-white/30"
        initial={reduced ? false : { opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: reduced ? 0 : 1.2, duration: 0.5, ease: EASE }}
      >
        beaten by a stronger post — not aged out
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
  Stage: (props: { posts: WirePost[]; reduced: boolean }) => React.ReactElement;
};

const SLIDE_DEFS: SlideDef[] = [
  {
    id: 'ignition',
    kicker: 'trigger · one day in',
    headline: ['One day old.', 'Already loud.'],
    gate: 'OWN_BREAKOUT_EARLY · fires only against posts the exact same age. The D7 verdict stays closed.',
    Stage: IgnitionStage,
  },
  {
    id: 'late-jump',
    kicker: 'trigger · after the verdict',
    headline: ['It kept', 'going.'],
    gate: 'OWN_LATE_JUMP · a 25-point climb from D7 to D21 files an amendment — the original read is never rewritten.',
    Stage: LateJumpStage,
  },
  {
    id: 'sustain',
    kicker: 'trigger · the hold',
    headline: ['Three weeks,', 'no drop.'],
    gate: 'OWN_SUSTAIN_LONG · top band held at D1, D3, D7 and D21. Prints once, at D21 — endurance is the news.',
    Stage: SustainStage,
  },
  {
    id: 'fade',
    kicker: 'trigger · the fall',
    headline: ['Hot start.', 'Cold finish.'],
    gate: 'OWN_FADE · a 30-point slide from the D1 band to D7. The fall prints with the same weight as the rise.',
    Stage: FadeStage,
  },
  {
    id: 'split',
    kicker: 'layered · likes × comments',
    headline: ['Applause, or', 'conversation.'],
    gate: 'OWN_COMMENT_SPIKE / OWN_LIKE_HEAVY · one multiple clears 3× baseline while the other stays flat.',
    Stage: SplitStage,
  },
  {
    id: 'wave',
    kicker: 'audience · the week moved',
    headline: ['The audience', 'surged.'],
    gate: 'OWN_FOLLOWER_SPIKE · the weekly delta clears the account’s own baseline multiple. Observed, never attributed.',
    Stage: WaveStage,
  },
  {
    id: 'swap',
    kicker: 'ledger · the top ten',
    headline: ['Something got', 'knocked out.'],
    gate: 'Displacement ledger · prints on merit exits only — a post aging out of the 90 days is not a story.',
    Stage: SwapStage,
  },
];

export default function Wire({ posts }: { posts: WirePost[] }) {
  const reduced = Boolean(useReducedMotion());
  const source = posts.length >= 6 ? posts : [...posts, ...FALLBACK_POSTS];
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
      aria-label={`The wire · slide ${active + 1} of ${SLIDE_DEFS.length}`}
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

      <div
        className="absolute inset-x-5 z-20 flex items-center justify-between gap-5 sm:inset-x-8 lg:inset-x-12"
        style={{ top: 'max(24px, env(safe-area-inset-top, 0px))' }}
      >
        <span className="text-[9px] font-black uppercase tracking-[0.2em] text-[#E11D48]">The wire</span>
        <span className="flex items-center gap-5">
          <span className="hidden text-[8px] font-black uppercase tracking-[0.16em] text-white/28 sm:block">
            triggers beyond the verdict · server math only
          </span>
          <Link
            href="/drop/artifacts/records"
            className="text-[8px] font-black uppercase tracking-[0.16em] text-white/34 transition-colors hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]"
          >
            Records desk
          </Link>
        </span>
      </div>

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
        <div className="flex shrink-0 items-center" aria-label="Wire slides">
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
