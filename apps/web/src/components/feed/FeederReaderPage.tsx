'use client';

/* ─────────────────────────────────────────────────────────────
   FEEDER READER PAGE — the read the cover opens into.
   Each run is 10 posts read AGAINST what's driving the account:
   the within-run bars, the badass triad (what moved /
   what mattered / the door — with proof), THE SHIFT (what these 10 did
   to the moves driving the account now), the posts, and the metrics.

   Desktop: monument — run bars + metrics | the read.
   Mobile: a full-bleed card DECK (stacked backs, flick anywhere to change
   run) with a segmented takeover (Read · Shift · Posts) so nothing buries
   the read and the run-switch is always a swipe away. Feels like an app.
   We never say "last 30 / recent memory" — it just feels like we know.
   ───────────────────────────────────────────────────────────── */

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { motion, AnimatePresence, useReducedMotion, type PanInfo } from 'framer-motion';
import { cn } from '@/lib/utils';
import Odometer, { SlotNumber } from '@/components/login/Odometer';

type Trend = 'up' | 'down' | 'flat';
type ShiftDir = 'up' | 'down' | 'flat' | 'new';
type Shift = { move: string; delta: string; read: string; dir: ShiftDir };

type RunCard = {
  id: string; deckLabel: string; live: boolean; postsIn?: number;
  span: string; // duration + when it ran, e.g. "Ran 6 days · ended 2d ago"
  avg: number; usual: number; trend: Trend;
  trajectory: number[];
  headline: string; moved: string; mattered: string; door: string;
  shift: Shift[];
};

const POSTS_PER_RUN = 10;
export const FEEDER_READER_CURRENT_AVG = 37;

const RUNS: RunCard[] = [
  {
    id: 'now', span: 'Day 3 · started 26h ago', deckLabel: 'this run · live', live: true, postsIn: 3, avg: FEEDER_READER_CURRENT_AVG, usual: 24, trend: 'down',
    trajectory: [40, 44, 31],
    headline: 'A top 1% that flatters a coin-flip.',
    moved: 'Your sharpest post in weeks was the mom/son/AI ad-sketch — top 1%. Soak it in.',
    mattered: 'But that wasn’t the idea, it was the delivery — thin premise, the voices carried it. You’ve run that roleplay move ten times and it swings top 1% to top 81%. Not a format, a coin flip on phrasing.',
    door: 'Your most reliable idea — faux-tycoon status comedy — hit top 7% and 14%, and you ran it twice. It’s gathering dust while you roll dice on roleplay. Build the status premise inside the character format. Every near-ceiling post already has both — you’ve just never done it on purpose.',
    shift: [
      { move: 'Character roleplay', delta: 'most-run · volatile', read: 'No driver earned — still a coin flip.', dir: 'flat' },
      { move: 'Status comedy', delta: 'held · 2 strong', read: 'One more and it locks as a driver.', dir: 'up' },
      { move: 'The dark cold-open', delta: 'under every top post', read: 'You lean on it without noticing.', dir: 'new' },
    ],
  },
  {
    id: 'r1', span: 'Ran 6 days · ended 2d ago', deckLabel: 'last run', live: false, avg: 33, usual: 22, trend: 'down',
    trajectory: [6, 9, 13, 21, 29, 37, 44, 47, 50, 54],
    headline: 'Front-loaded, then out of gas.',
    moved: 'Hot open — first three near the ceiling — then a clean slide to the floor by the close.',
    mattered: 'You spent the strong idea early and coasted. The back half was filler, and the room left before the end.',
    door: 'Spread the strong bits — don’t dump them all in the first three and hope the back half carries.',
    shift: [
      { move: 'Status comedy', delta: 'cooled', read: 'Burned it early; nothing left to hold the close.', dir: 'down' },
      { move: 'Topical reactions', delta: 'sank again', read: 'Third run they’ve dragged the floor.', dir: 'down' },
    ],
  },
  {
    id: 'r2', span: 'Ran 5 days · ended 9d ago', deckLabel: 'two runs back', live: false, avg: 39, usual: 21, trend: 'flat',
    trajectory: [44, 40, 7, 46, 41, 43, 38, 42, 45, 47],
    headline: 'One spike, otherwise flat.',
    moved: 'Nine posts sat mid-to-cold. One — the cricket reaction — broke top 7% and dragged the average up.',
    mattered: 'Be honest: that wasn’t you, it was the match. Strip it and this is your flattest run in a while.',
    door: 'Don’t read the spike as a pattern. The flat nine are the real story — and they’re all missing a driver.',
    shift: [
      { move: 'Live-moment ride', delta: 'borrowed', read: 'Reach was the match’s, not yours.', dir: 'flat' },
      { move: 'Status comedy', delta: 'absent', read: 'Your sharpest idea sat out the whole run.', dir: 'down' },
    ],
  },
  {
    id: 'r3', span: 'Ran 7 days · ended 16d ago', deckLabel: 'three runs back', live: false, avg: 11, usual: 20, trend: 'up',
    trajectory: [8, 6, 11, 9, 14, 7, 12, 10, 15, 13],
    headline: 'Best run in weeks — steady heat.',
    moved: 'Ten posts, ten landed top 15%. No dead weight, no fluke spike — just consistent.',
    mattered: 'Every one leaned on the status-comedy bit. Your sharpest tool, used the whole way through.',
    door: 'This is the bar. Whatever you’ve changed since — change it back.',
    shift: [
      { move: 'Status comedy', delta: 'locked · 4 strong', read: 'This is the run that made it a driver.', dir: 'up' },
      { move: 'Character roleplay', delta: 'in support', read: 'Carried the idea instead of replacing it.', dir: 'up' },
    ],
  },
  {
    id: 'r4', span: 'Ran 6 days · ended 24d ago', deckLabel: 'four runs back', live: false, avg: 23, usual: 19, trend: 'flat',
    trajectory: [44, 40, 33, 27, 22, 17, 14, 11, 9, 7],
    headline: 'Slow start, strong finish.',
    moved: 'Opened cold and built — the back half climbed steadily to near the ceiling.',
    mattered: 'You found the groove late. The posts that worked all dropped the polish and got loose.',
    door: 'Start where you finished — open loose, skip the warm-up.',
    shift: [{ move: 'Loose, unscripted bits', delta: 'surfaced', read: 'The looser it got, the harder it hit.', dir: 'new' }],
  },
];

const TREND_GLYPH: Record<Trend, string> = { up: '▲', down: '▼', flat: '—' };
const SHIFT_GLYPH: Record<ShiftDir, string> = { up: '▲', down: '▼', flat: '—', new: '＋' };
const heightFor = (l: number) => Math.max(0.08, Math.min(1, 1 - l / 62));
const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value));

// One-line read per post, by where it sits in the run and the band it landed in.
// "rank" is the feeder-file percentile (lower = better).
function postTag(rank: number, index: number, total: number): string {
  if (index === 0) return rank <= 12 ? 'the hot open' : 'a cold open';
  if (index === total - 1) return rank <= 20 ? 'stuck the landing' : 'the floor';
  if (rank <= 8) return 'the spike';
  if (rank <= 16) return 'near the ceiling';
  if (rank <= 30) return 'holding';
  if (rank <= 45) return 'cooling';
  return 'dead weight';
}

// Mock per-post metrics derived from feeder-file rank (better rank → more reach).
// Swap for real run-report fields later; shape stays the same.
function statFor(rank: number) {
  const reach = Math.max(46000, Math.round((105 - rank) * 13200));
  return { views: reach, likes: Math.round(reach * 0.092), comments: Math.round(reach * 0.0023) };
}

// Split into the numeric part and the unit suffix. The suffix (K/M) is rendered
// outside the slot animation — SlotNumber gives marks a thin fixed-width slot
// that clips wide letters like M/K.
function compactParts(n: number): { num: string; suffix: string } {
  if (n >= 1e6) return { num: (n / 1e6).toFixed(1).replace(/\.0$/, ''), suffix: 'M' };
  if (n >= 1e3) return { num: String(Math.round(n / 1e3)), suffix: 'K' };
  return { num: String(n), suffix: '' };
}

function Eyebrow({ children, accent = false, className }: { children: React.ReactNode; accent?: boolean; className?: string }) {
  return <span className={cn('text-[10px] font-black uppercase tracking-[0.22em]', accent ? 'text-[#E11D48]' : 'text-foreground/40', className)}>{children}</span>;
}

function RunMeta({ run }: { run: RunCard }) {
  return (
    <div className="flex flex-wrap items-center gap-x-2 gap-y-1.5 sm:gap-2.5">
      {run.live ? (
        <span className="inline-flex items-center gap-1 rounded-full bg-[#E11D48] px-2 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-white">
          <span className="h-1 w-1 rounded-full bg-white fm-live-dot" /> Live · {run.postsIn}/{POSTS_PER_RUN}
        </span>
      ) : (
        <span className="rounded-full border border-foreground/12 px-2 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/40">Sealed · {POSTS_PER_RUN} posts</span>
      )}
      <span className="inline-flex items-baseline font-black leading-none text-foreground">
        <span className="mr-1.5 self-center text-[10px] font-black uppercase tracking-[0.2em] text-foreground/40">avg</span>
        <Odometer value={run.avg} className="inline-flex min-w-[1.22em] text-[34px] sm:text-[46px] lg:text-[78px] xl:text-[88px]" />
        <span className="text-[0.36em] text-[#E11D48]">%</span>
      </span>
      <span className="inline-flex items-baseline gap-1 text-[10px] font-black uppercase tracking-[0.12em] text-foreground/45">
        <span className={cn(run.trend === 'down' ? 'text-[#E11D48]' : run.trend === 'up' ? 'text-foreground/70' : 'text-foreground/40')}>{TREND_GLYPH[run.trend]}</span>
        vs usual <span className="text-foreground/65"><Odometer value={run.usual} className="inline-flex" /></span>%
      </span>
    </div>
  );
}

const SOFT_EASE = [0.16, 0.9, 0.2, 1] as const;

// Premium one-by-one reveal — each element slots in after the previous. Explicit
// objects (not variant labels) so it always fires on mount, even nested inside the
// run-switch slide. Tuned so it reads sequential without leaving a blank screen.
const HERO_STEP = 0.07;
const HERO_BASE = 0.05;
function revealAt(delay: number, reduce: boolean) {
  if (reduce) return {};
  return {
    initial: { opacity: 0, y: 12 },
    animate: { opacity: 1, y: 0 },
    transition: { duration: 0.46, ease: SOFT_EASE, delay },
  };
}
function heroReveal(i: number, reduce: boolean) {
  return revealAt(HERO_BASE + i * HERO_STEP, reduce);
}

type RunBarSlot = {
  index: number;
  rank: number | null;
  fill: number;
  posted: boolean;
};

type BarTone = {
  color: string;
  glow: string;
  glowStrength: number;
  label: string;
  pulse: boolean;
};

function postStrength(rank: number, run: RunCard): number {
  const scores = run.trajectory.map((rank) => 101 - rank);
  const minScore = Math.min(...scores);
  const maxScore = Math.max(...scores);
  const spread = Math.max(22, maxScore - minScore);
  const mid = (minScore + maxScore) / 2;
  const domainMin = clamp(mid - spread / 2, 1, 101 - spread);
  const domainMax = domainMin + spread;
  return clamp(0.18 + clamp((101 - rank - domainMin) / (domainMax - domainMin), 0, 1) * 0.78, 0.18, 0.96);
}

function barToneForRank(rank: number, run: RunCard): BarTone {
  const fill = postStrength(rank, run);
  if (fill >= 0.88 || rank <= 8) {
    return {
      color: '#F71852',
      glow: 'rgba(247,24,82,0.42)',
      glowStrength: 0.74,
      label: 'text-[#FF5C84]',
      pulse: true,
    };
  }
  if (fill >= 0.68 || rank <= 18) {
    return {
      color: '#D41444',
      glow: 'rgba(225,29,72,0.28)',
      glowStrength: 0.54,
      label: 'text-[#FB7185]',
      pulse: true,
    };
  }
  if (fill >= 0.42 || rank <= 36) {
    return {
      color: '#9F1239',
      glow: 'rgba(159,18,57,0.2)',
      glowStrength: 0.34,
      label: 'text-foreground/46',
      pulse: false,
    };
  }
  return {
    color: rank >= 55 ? '#1A050A' : '#4A0B1A',
    glow: 'rgba(74,11,26,0.16)',
    glowStrength: 0.2,
    label: 'text-foreground/34',
    pulse: false,
  };
}

function runBarSlots(run: RunCard): { slots: RunBarSlot[]; baseline: number } {
  const slots: RunBarSlot[] = Array.from({ length: POSTS_PER_RUN }, (_, index) => {
    const rank = run.trajectory[index] ?? null;
    if (rank == null) return { index, rank, fill: 0, posted: false };
    return { index, rank, fill: postStrength(rank, run), posted: true };
  });
  return { slots, baseline: postStrength(run.usual, run) };
}

function RunBars({
  run,
  selectedIndex,
  onSelect,
  reduce,
  mode,
}: {
  run: RunCard;
  selectedIndex: number | null;
  onSelect: (i: number) => void;
  reduce: boolean;
  mode: 'desktop' | 'mobile';
}) {
  const { slots, baseline } = runBarSlots(run);
  const compact = mode === 'mobile';
  const graphDelay = compact ? HERO_BASE + 6 * HERO_STEP : 0.1;
  const stagger = compact ? 0.064 : 0.086;
  const lastPostedIndex = Math.max(0, run.trajectory.length - 1);
  const bestIndex = run.trajectory.indexOf(Math.min(...run.trajectory));
  const pulseIndex = run.live ? lastPostedIndex : bestIndex;

  return (
    <motion.div
      {...(reduce ? {} : { initial: { opacity: 0, y: 8 }, animate: { opacity: 1, y: 0 }, transition: { duration: 0.36, ease: SOFT_EASE, delay: graphDelay } })}
      className={cn('relative w-full', compact ? 'h-[clamp(170px,42vw,220px)]' : 'h-full min-h-[250px]')}
    >
      <div className="pointer-events-none absolute inset-x-0 top-0 flex justify-between text-[8px] font-black uppercase tracking-[0.14em] text-foreground/28">
        <span>open</span>
        <span>close</span>
      </div>
      <motion.span
        aria-hidden
        className="pointer-events-none absolute left-0 right-0 z-20 h-px origin-left bg-foreground/24"
        style={{ bottom: `${baseline * 100}%` }}
        initial={reduce ? false : { scaleX: 0, opacity: 0 }}
        animate={{ scaleX: 1, opacity: 1 }}
        transition={reduce ? { duration: 0 } : { delay: graphDelay + 0.72, duration: 0.48, ease: SOFT_EASE }}
      />
      <span
        className="pointer-events-none absolute right-0 z-20 -translate-y-1/2 rounded-full bg-background/70 px-1.5 py-0.5 text-[8px] font-black uppercase tracking-[0.12em] text-foreground/38 backdrop-blur"
        style={{ bottom: `${baseline * 100}%` }}
      >
        usual {run.usual}%
      </span>
      <div className={cn('absolute inset-x-0 bottom-0 top-5 grid grid-cols-10 items-end', compact ? 'gap-1.5' : 'gap-2.5 xl:gap-3')}>
        {slots.map((slot) => {
          const active = selectedIndex === slot.index;
          const best = slot.index === bestIndex;
          const fill = slot.posted ? slot.fill : 0;
          const fillDelay = reduce || !slot.posted ? 0 : graphDelay + 0.07 + slot.index * stagger;
          const tone = slot.rank == null ? null : barToneForRank(slot.rank, run);
          const shouldPulse = Boolean(tone?.pulse && slot.index === pulseIndex);
          return (
            <button
              key={slot.index}
              type="button"
              disabled={!slot.posted}
              aria-label={slot.posted ? `Post ${slot.index + 1}, top ${slot.rank}%` : `Future post ${slot.index + 1}`}
              onClick={(event) => {
                event.stopPropagation();
                if (slot.posted) onSelect(slot.index);
              }}
              className={cn(
                'group/bar relative flex h-full min-w-0 items-end justify-center rounded-[12px] outline-none transition-transform duration-300 focus-visible:ring-2 focus-visible:ring-[#E11D48]/40',
                active ? 'scale-x-[1.06]' : 'scale-x-100',
                slot.posted ? 'cursor-pointer' : 'cursor-default',
              )}
            >
              {slot.posted ? (
                <span className="absolute inset-x-0 bottom-0 top-0 rounded-[12px] bg-[#1A050A]/24 shadow-[inset_0_1px_0_rgba(255,255,255,0.026),inset_0_-16px_26px_rgba(0,0,0,0.34)] dark:bg-[#1A050A]/34" />
              ) : (
                <span className="absolute inset-x-[28%] bottom-0 h-1.5 rounded-full bg-[#1A050A]/24 dark:bg-white/[0.035]" />
              )}
              {slot.posted && (
                <>
                  <motion.span
                    aria-hidden
                    className="absolute inset-x-[10%] bottom-0 rounded-t-[14px] blur-[16px]"
                    style={{ height: `${Math.max(18, fill * 82)}%`, backgroundColor: tone?.glow }}
                    initial={reduce ? false : { opacity: 0 }}
                    animate={shouldPulse && !reduce ? { opacity: [tone?.glowStrength ?? 0.3, 0.86, tone?.glowStrength ?? 0.3] } : { opacity: active || best ? (tone?.glowStrength ?? 0.42) : (tone?.glowStrength ?? 0.24) * 0.72 }}
                    transition={shouldPulse && !reduce ? { delay: graphDelay + 0.2 + lastPostedIndex * stagger, duration: 1.2, ease: 'easeInOut' } : { duration: reduce ? 0 : 0.28 }}
                  />
                  {active && <span aria-hidden className="absolute -inset-x-1 bottom-0 top-4 rounded-[15px] bg-[#F71852]/[0.055] shadow-[0_0_34px_rgba(247,24,82,0.24)]" />}
                  <motion.span
                    aria-hidden
                    className="absolute inset-x-[18%] bottom-0 origin-bottom rounded-t-[9px] shadow-[inset_0_1px_0_rgba(255,255,255,0.16),inset_0_-18px_24px_rgba(26,5,10,0.42),0_10px_24px_rgba(225,29,72,0.11)]"
                    style={{
                      height: '100%',
                      backgroundColor: tone?.color,
                      backgroundImage: 'linear-gradient(180deg,rgba(255,255,255,0.13),rgba(255,255,255,0.02) 32%,rgba(0,0,0,0.28) 100%)',
                    }}
                    initial={reduce ? false : { scaleY: 0 }}
                    animate={{ scaleY: fill }}
                    transition={reduce ? { duration: 0 } : { delay: fillDelay, duration: compact ? 0.72 : 0.82, ease: [0.16, 0.9, 0.22, 1] }}
                  />
                  {(active || best || (run.live && slot.index === lastPostedIndex)) && (
                    <span
                      className={cn(
                        'pointer-events-none absolute left-1/2 z-30 -translate-x-1/2 whitespace-nowrap text-[8px] font-black uppercase tracking-[0.08em]',
                        active || best ? 'text-[#F71852]' : tone?.label,
                        compact ? 'text-[7px]' : '',
                      )}
                      style={{ bottom: `${Math.min(96, fill * 100 + 4)}%` }}
                    >
                      {slot.rank}%
                    </span>
                  )}
                </>
              )}
            </button>
          );
        })}
      </div>
    </motion.div>
  );
}

function Triad({ run, reduce, delay = 0 }: { run: RunCard; reduce: boolean; delay?: number }) {
  const step = 0.08;
  return (
    <div className="min-w-0 max-w-full">
      <motion.h3 {...revealAt(delay, reduce)} className="max-w-full text-[24px] font-black leading-[1.06] tracking-tight text-foreground break-words sm:text-[30px] lg:text-[32px] xl:text-[36px]">{run.headline}</motion.h3>
      <div className="mt-5 space-y-3.5 lg:mt-6 lg:space-y-5">
        {[['What moved', run.moved], ['What mattered', run.mattered]].map(([l, b], i) => (
          <motion.div key={l} {...revealAt(delay + (i + 1) * step, reduce)} className="border-t border-foreground/10 pt-3.5 lg:pt-5">
            <Eyebrow>{l}</Eyebrow>
            <p className="mt-1.5 max-w-full text-[14.5px] font-black leading-snug tracking-tight text-foreground break-words sm:text-[15.5px] lg:text-[18px] xl:text-[19px]">{b}</p>
          </motion.div>
        ))}
        <motion.div {...revealAt(delay + 3 * step, reduce)} className="border-t border-foreground/10 pt-3.5 lg:pt-5">
          <div className="flex items-baseline gap-2.5">
            <span className="shrink-0 text-[18px] font-black leading-none text-[#E11D48]">↳</span>
            <div className="min-w-0"><Eyebrow accent>The door</Eyebrow><p className="mt-1 max-w-full text-[15px] font-black leading-snug tracking-tight text-foreground break-words sm:text-[16px] lg:text-[18px] xl:text-[19px]">{run.door}</p></div>
          </div>
        </motion.div>
      </div>
    </div>
  );
}

function ShiftList({ run, reduce, delay = 0 }: { run: RunCard; reduce: boolean; delay?: number }) {
  const step = 0.08;
  return (
    <div className="min-w-0 max-w-full">
      <motion.div {...revealAt(delay, reduce)} className="flex flex-wrap items-center gap-x-2 gap-y-1"><Eyebrow accent>What this run shifted</Eyebrow><span className="text-[9px] font-black uppercase tracking-[0.14em] text-foreground/30">· driving you now</span></motion.div>
      <div className="mt-1 lg:mt-2">
        {run.shift.map((s, i) => {
          const hot = s.dir === 'up' || s.dir === 'new';
          return (
            <motion.div key={s.move} {...revealAt(delay + (i + 1) * step, reduce)} className="border-t border-foreground/8 py-3 lg:py-4">
              <div className="flex items-center gap-2.5 lg:gap-3">
                <span className={cn('w-3 shrink-0 text-[12px] font-black lg:text-[15px]', hot ? 'text-[#E11D48]' : 'text-foreground/45')}>{SHIFT_GLYPH[s.dir]}</span>
                <span className="min-w-0 flex-1 text-[14px] font-black tracking-tight text-foreground lg:text-[18px]">{s.move}</span>
                <span className={cn('max-w-[44%] shrink-0 text-right text-[9px] font-black uppercase leading-tight tracking-[0.12em] lg:text-[11px]', hot ? 'text-[#E11D48]' : 'text-foreground/45')}>{s.delta}</span>
              </div>
              <p className="mt-1 max-w-full pl-[22px] text-[12.5px] font-bold leading-snug text-foreground/50 break-words lg:pl-[27px] lg:text-[16px]">{s.read}</p>
            </motion.div>
          );
        })}
      </div>
    </div>
  );
}

function RunMetrics({ run, reduce, baseIndex = 0 }: { run: RunCard; reduce: boolean; baseIndex?: number }) {
  const ceiling = Math.min(...run.trajectory);
  const floor = Math.max(...run.trajectory);
  const beat = run.trajectory.filter((l) => l < run.usual).length;
  const cells = [
    { k: 'ceiling', v: `top ${ceiling}%` },
    { k: 'floor', v: `top ${floor}%` },
    { k: 'beat your usual', v: `${beat}/${run.trajectory.length}` },
    { k: 'followers', v: '+1.2k' },
  ];
  return (
    <div className="grid grid-cols-2 gap-2 lg:gap-3">
      {cells.map((c, i) => (
        <motion.div key={c.k} {...heroReveal(baseIndex + i, reduce)} className="rounded-[12px] border border-foreground/10 bg-foreground/[0.03] px-3 py-2.5 lg:rounded-[16px] lg:px-4 lg:py-4">
          <div className="text-[20px] font-black leading-none tracking-tight text-foreground lg:text-[32px] xl:text-[36px]">{c.v}</div>
          <div className="mt-1.5 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/45 lg:mt-2 lg:text-[10px]">{c.k}</div>
        </motion.div>
      ))}
    </div>
  );
}

function runMetricCells(run: RunCard) {
  const ceiling = Math.min(...run.trajectory);
  const floor = Math.max(...run.trajectory);
  const beat = run.trajectory.filter((l) => l < run.usual).length;
  return [
    { k: 'ceiling top', v: `${ceiling}%` },
    { k: 'floor top', v: `${floor}%` },
    { k: 'beat usual', v: `${beat}/${run.trajectory.length}` },
    { k: 'followers', v: '+1.2k' },
  ];
}

function PostPreviewThumb({ rank, index }: { rank: number; index: number }) {
  const score = clamp((101 - rank) / 100, 0, 1);
  return (
    <div
      className="relative w-[58px] shrink-0 overflow-hidden rounded-[13px] border border-white/[0.12] shadow-[0_14px_30px_rgba(0,0,0,0.16),0_0_26px_rgba(225,29,72,0.13)]"
      style={{
        aspectRatio: '9 / 12',
        background: `linear-gradient(180deg, rgba(255,122,154,${(0.3 + score * 0.32).toFixed(2)}), rgba(225,29,72,${(0.2 + score * 0.24).toFixed(2)}) 48%, #130407 100%)`,
      }}
    >
      <span aria-hidden className="absolute inset-x-0 top-0 h-px bg-white/34" />
      <span aria-hidden className="absolute -bottom-4 left-1/2 h-12 w-12 -translate-x-1/2 rounded-full bg-[#E11D48]/38 blur-xl" />
      <span className="absolute left-2 top-2 text-[9px] font-black tabular-nums text-white/76">#{index + 1}</span>
      <span className="absolute bottom-2 left-2 inline-flex items-baseline font-black leading-none text-white">
        <span className="text-[19px]">{rank}</span>
        <span className="ml-[0.02em] text-[9px] text-white/70">%</span>
      </span>
    </div>
  );
}

function DesktopScoreHud({ run, selected, reduce, onSelect, onClose }: { run: RunCard; selected: number | null; reduce: boolean; onSelect: (i: number) => void; onClose: () => void }) {
  const postIndex = selected == null ? null : Math.min(selected, run.trajectory.length - 1);
  const rank = postIndex == null ? null : run.trajectory[postIndex];
  const postStats = rank == null ? null : statFor(rank);
  const showPost = postIndex != null && rank != null && postStats != null;
  const cells = showPost
    ? [
        { k: 'views', v: compactParts(postStats.views).num, suffix: compactParts(postStats.views).suffix },
        { k: 'comments', v: compactParts(postStats.comments).num, suffix: compactParts(postStats.comments).suffix },
        { k: 'likes', v: compactParts(postStats.likes).num, suffix: compactParts(postStats.likes).suffix },
        { k: 'post', v: `#${postIndex + 1}` },
      ]
    : runMetricCells(run);

  return (
    <motion.div
      {...heroReveal(0, reduce)}
      className="pointer-events-auto relative z-10 flex h-full min-h-[228px] w-[clamp(220px,18vw,260px)] shrink-0 flex-col justify-between py-2 pr-5"
    >
      <div className="min-h-[132px]">
        <div className="flex items-center justify-between gap-3">
          <Eyebrow accent={run.live}>{showPost ? postTag(rank, postIndex, run.trajectory.length) : run.live ? 'this run' : 'sealed run'}</Eyebrow>
          <span className={cn('text-[10px] font-black uppercase tracking-[0.14em]', run.trend === 'down' ? 'text-[#E11D48]' : run.trend === 'up' ? 'text-foreground/65' : 'text-foreground/35')}>
            {TREND_GLYPH[run.trend]}
          </span>
        </div>

        {showPost ? (
          <div className="mt-4 grid grid-cols-[58px_minmax(0,1fr)] items-center gap-3">
            <PostPreviewThumb rank={rank} index={postIndex} />
            <div className="min-w-0">
              <div className="flex items-end gap-1.5 text-foreground">
                <span className="mb-1.5 text-[9px] font-black uppercase tracking-[0.18em] text-foreground/40">rank</span>
                <span className="inline-flex items-baseline font-black leading-none text-[46px] xl:text-[50px]">
                  <Odometer value={rank} animateOnMount className="inline-flex min-w-[1.12em]" />
                  <span className="ml-[0.03em] text-[0.34em] text-[#E11D48]">%</span>
                </span>
              </div>
              <p className="mt-1 text-[9px] font-black uppercase leading-snug tracking-[0.12em] text-foreground/42">
                Post {postIndex + 1} of {run.trajectory.length}
              </p>
            </div>
          </div>
        ) : (
          <>
            <div className="mt-4 flex items-end gap-2 text-foreground">
              <span className="mb-2 text-[10px] font-black uppercase tracking-[0.2em] text-foreground/40">avg</span>
              <span className="inline-flex items-baseline font-black leading-none text-[56px] xl:text-[62px] 2xl:text-[66px]">
                <Odometer value={run.avg} animateOnMount className="inline-flex min-w-[1.14em]" />
                <span className="ml-[0.03em] text-[0.34em] text-[#E11D48]">%</span>
              </span>
            </div>
            <p className="mt-2 min-h-[30px] text-[10px] font-black uppercase leading-snug tracking-[0.12em] text-foreground/42">
              {run.trajectory.length}/{POSTS_PER_RUN} posts mapped · usual {run.usual}%
            </p>
          </>
        )}
      </div>

      <div>
        <div className="grid grid-cols-2 gap-x-5 gap-y-3 border-t border-foreground/10 pt-3.5">
          {cells.map((cell) => (
            <div key={cell.k} className="min-w-0">
              <div className="flex min-w-0 items-baseline text-[20px] font-black leading-none tracking-tight text-foreground xl:text-[22px]">
                {'suffix' in cell ? (
                  <>
                    <span className="truncate">{cell.v}</span>
                    {cell.suffix && <span className="ml-[0.03em] text-[0.72em] text-foreground/62">{cell.suffix}</span>}
                  </>
                ) : (
                  <span className="truncate">{cell.v}</span>
                )}
              </div>
              <div className="mt-1.5 truncate text-[8px] font-black uppercase tracking-[0.14em] text-foreground/38 xl:text-[9px]">{cell.k}</div>
            </div>
          ))}
        </div>
        <div className="mt-4 flex h-7 items-center gap-1">
          {showPost ? (
            <>
              <HeroNavButton label="Previous post" glyph="‹" disabled={postIndex === 0} onClick={() => onSelect(postIndex - 1)} />
              <HeroNavButton label="Next post" glyph="›" disabled={postIndex === run.trajectory.length - 1} onClick={() => onSelect(postIndex + 1)} />
              <HeroNavButton label="Back to run" glyph="×" onClick={onClose} />
            </>
          ) : (
            <span className="text-[9px] font-black uppercase tracking-[0.16em] text-foreground/28">click a bar for post metrics</span>
          )}
        </div>
      </div>
    </motion.div>
  );
}

function ReaderMonument({ runs, index, setIndex, reduce }: { runs: RunCard[]; index: number; setIndex: (i: number) => void; reduce: boolean }) {
  const [dir, setDir] = useState(0);
  const [selected, setSelected] = useState<number | null>(null);
  const run = runs[index];
  const go = (nextIndex: number, direction: number) => {
    setDir(direction);
    setIndex((nextIndex + runs.length) % runs.length);
    setSelected(null);
  };
  const pickPost = (i: number) => setSelected(Math.max(0, Math.min(run.trajectory.length - 1, i)));
  const onDragEnd = (_: unknown, info: PanInfo) => {
    if (info.offset.x < -55) go(index + 1, 1);
    else if (info.offset.x > 55) go(index - 1, -1);
  };

  return (
    <motion.section
      drag={reduce ? false : 'x'}
      dragConstraints={{ left: 0, right: 0 }}
      dragElastic={0.12}
      dragSnapToOrigin
      onDragEnd={onDragEnd}
      onClick={() => { if (selected != null) setSelected(null); }}
      style={{
        touchAction: 'pan-y',
        background: 'linear-gradient(180deg, rgba(247,24,82,0.028), rgba(255,255,255,0.012) 28%, rgba(0,0,0,0.024) 100%)',
      }}
      className="fm-depth-glass relative h-[360px] cursor-grab overflow-hidden rounded-[32px] p-5 text-left text-foreground active:cursor-grabbing xl:h-[clamp(380px,40dvh,430px)] xl:p-6 2xl:h-[460px]"
    >
      <div aria-hidden className="pointer-events-none absolute inset-x-0 top-0 -z-10 h-20 bg-[#F71852]/[0.024]" />
      <div className="relative z-20 mb-3 flex items-start justify-between gap-6">
        <div className="min-w-0">
          <Eyebrow accent={run.live}>{run.deckLabel}</Eyebrow>
          <div className="mt-1 truncate text-[9px] font-black uppercase tracking-[0.14em] text-foreground/35">{run.span}</div>
        </div>
        <div className="flex items-center justify-end gap-1.5">
          {runs.map((r, i) => (
            <button key={r.id} type="button" aria-label={r.deckLabel} onClick={(e) => { e.stopPropagation(); go(i, i > index ? 1 : -1); }} className={cn('h-1.5 rounded-full transition-all', i === index ? 'w-5 bg-[#E11D48]' : 'w-1.5 bg-foreground/20 hover:bg-foreground/35')} />
          ))}
        </div>
      </div>

      <AnimatePresence mode="wait" custom={dir} initial={false}>
        <motion.div
          key={run.id}
          custom={dir}
          variants={reduce ? undefined : slideVariants}
          initial={reduce ? false : 'enter'}
          animate="center"
          exit={reduce ? undefined : 'exit'}
          transition={{ duration: 0.28, ease: SOFT_EASE }}
          className="absolute inset-x-5 bottom-5 top-[58px] xl:inset-x-6 xl:bottom-6"
        >
          <DesktopScoreHud run={run} selected={selected} reduce={reduce} onSelect={pickPost} onClose={() => setSelected(null)} />
          <div className="absolute inset-y-3 left-[clamp(250px,21vw,292px)] right-0 z-0 xl:inset-y-4">
            <RunBars run={run} selectedIndex={selected} onSelect={pickPost} reduce={reduce} mode="desktop" />
          </div>
        </motion.div>
      </AnimatePresence>
    </motion.section>
  );
}

// ── the stat hero — one block that crossfades between the run's numbers and a
//    selected post's thumbnail + metrics. Run and post are built to the same
//    height so the grid stack never resizes between states.

// A metric tile that stays dedicated to its metric but rotates its value between
// this post and the account's usual, on the Odometer slot animation. Domino:
// each tile flips after its own delay so the row cascades, never all at once.
function MetricTile({ label, post, usual, cycle, delayMs, reduce }: { label: string; post: number; usual: number; cycle: number; delayMs: number; reduce: boolean }) {
  const [showUsual, setShowUsual] = useState(false);
  useEffect(() => {
    const target = cycle % 2 === 1;
    const t = window.setTimeout(() => setShowUsual(target), delayMs);
    return () => window.clearTimeout(t);
  }, [cycle, delayMs]);
  const { num, suffix } = compactParts(showUsual ? usual : post);
  return (
    <div className="overflow-hidden rounded-[12px] border border-foreground/10 bg-foreground/[0.03] px-2.5 py-2">
      <div className="truncate text-[8px] font-black uppercase tracking-[0.12em] text-foreground/40">{label}</div>
      <div className="mt-1 flex items-baseline text-[19px] font-black leading-none tracking-tight text-foreground tabular-nums">
        {reduce ? num : <SlotNumber text={num} animateOnMount={false} revealDelayMs={0} />}
        {suffix && <span className="ml-[0.02em] text-[0.72em] text-foreground/70">{suffix}</span>}
      </div>
      <div className={cn('mt-1 truncate text-[7px] font-black uppercase tracking-[0.1em] transition-colors duration-500', showUsual ? 'text-foreground/35' : 'text-[#E11D48]')}>{showUsual ? 'vs usual' : 'this post'}</div>
    </div>
  );
}

function HeroNavButton({ label, glyph, disabled, onClick }: { label: string; glyph: string; disabled?: boolean; onClick: () => void }) {
  return (
    <button type="button" aria-label={label} disabled={disabled} onClick={(e) => { e.stopPropagation(); onClick(); }} className="inline-flex h-7 w-7 items-center justify-center rounded-[8px] border border-foreground/12 bg-foreground/[0.03] text-[15px] font-black leading-none text-foreground/70 transition-colors hover:bg-foreground/[0.07] disabled:opacity-25">
      {glyph}
    </button>
  );
}

function PostHero({ run, index, reduce, onSelect, onClose, active }: { run: RunCard; index: number; reduce: boolean; onSelect: (i: number) => void; onClose: () => void; active: boolean }) {
  const total = run.trajectory.length;
  const rank = run.trajectory[index];
  const s = statFor(rank);
  const usual = statFor(run.usual);
  const w = heightFor(rank);
  const [cycle, setCycle] = useState(0);
  useEffect(() => {
    if (!active) return;
    const id = window.setInterval(() => setCycle((c) => c + 1), 5000);
    return () => window.clearInterval(id);
  }, [active]);
  return (
    <div>
      <div className="flex items-stretch gap-3">
        <motion.div
          {...heroReveal(0, reduce)}
          className="relative w-[72px] shrink-0 overflow-hidden rounded-[13px] border border-white/[0.14]"
          style={{ aspectRatio: '9 / 12', background: `linear-gradient(180deg, rgba(225,29,72,${(0.2 + w * 0.6).toFixed(2)}), #120406)` }}
        >
          <span className="absolute left-2 top-2 text-[10px] font-black tabular-nums text-white/75">#{index + 1}</span>
        </motion.div>
        <div className="flex min-w-0 flex-1 flex-col justify-center">
          <div className="flex items-start justify-between gap-2">
            <motion.div {...heroReveal(1, reduce)} className="min-w-0">
              <div className="flex items-baseline gap-1.5 text-foreground">
                <span className="text-[10px] font-black uppercase tracking-[0.16em] text-foreground/40">rank</span>
                <span className="inline-flex items-baseline text-[34px] font-black leading-none">
                  <Odometer value={rank} animateOnMount className="inline-flex min-w-[1.1em]" />
                  <span className="text-[0.36em] text-[#E11D48]">% top</span>
                </span>
              </div>
              <div className="mt-1.5 truncate text-[11px] font-black uppercase tracking-[0.12em] text-foreground/55">{postTag(rank, index, total)}</div>
            </motion.div>
            <motion.div {...heroReveal(2, reduce)} className="flex shrink-0 items-center gap-1">
              <HeroNavButton label="Previous post" glyph="‹" disabled={index === 0} onClick={() => onSelect(index - 1)} />
              <HeroNavButton label="Next post" glyph="›" disabled={index === total - 1} onClick={() => onSelect(index + 1)} />
              <HeroNavButton label="Back to run" glyph="×" onClick={onClose} />
            </motion.div>
          </div>
        </div>
      </div>
      <div className="mt-2.5 grid grid-cols-3 gap-2">
        <motion.div {...heroReveal(3, reduce)}><MetricTile label="views" post={s.views} usual={usual.views} cycle={cycle} delayMs={0} reduce={reduce} /></motion.div>
        <motion.div {...heroReveal(4, reduce)}><MetricTile label="comments" post={s.comments} usual={usual.comments} cycle={cycle} delayMs={380} reduce={reduce} /></motion.div>
        <motion.div {...heroReveal(5, reduce)}><MetricTile label="likes" post={s.likes} usual={usual.likes} cycle={cycle} delayMs={760} reduce={reduce} /></motion.div>
      </div>
    </div>
  );
}

function HeroZone({ run, selected, onSelect, onClose, reduce }: { run: RunCard; selected: number | null; onSelect: (i: number) => void; onClose: () => void; reduce: boolean }) {
  // Both layers stay mounted in one grid cell; we only crossfade opacity between
  // them. The cell is always sized to the taller layer, so toggling run↔post
  // never resizes the block — no layout shift / page stretch, on any device.
  const showPost = selected != null;
  const postIndex = Math.min(selected ?? 0, run.trajectory.length - 1);
  return (
    <div className="grid">
      <motion.div
        className="col-start-1 row-start-1 min-w-0"
        animate={{ opacity: showPost ? 0 : 1 }}
        transition={reduce ? { duration: 0 } : { duration: 0.3, ease: SOFT_EASE }}
        style={{ pointerEvents: showPost ? 'none' : 'auto' }}
        aria-hidden={showPost}
      >
        <motion.div {...heroReveal(0, reduce)}><RunMeta run={run} /></motion.div>
        <div className="mt-4"><RunMetrics run={run} reduce={reduce} baseIndex={1} /></div>
      </motion.div>
      <motion.div
        className="col-start-1 row-start-1 min-w-0"
        animate={{ opacity: showPost ? 1 : 0 }}
        transition={reduce ? { duration: 0 } : { duration: 0.3, ease: SOFT_EASE }}
        style={{ pointerEvents: showPost ? 'auto' : 'none' }}
        aria-hidden={!showPost}
      >
        <PostHero run={run} index={postIndex} reduce={reduce} onSelect={onSelect} onClose={onClose} active={showPost} />
      </motion.div>
    </div>
  );
}

// ── mobile: one cohesive gesture — swipe sideways anywhere = change run,
//    scroll down = the full read. No competing tap-tabs. ───────────────────
const slideVariants = {
  enter: (d: number) => ({ opacity: 0, x: d >= 0 ? 32 : -32 }),
  center: { opacity: 1, x: 0 },
  exit: (d: number) => ({ opacity: 0, x: d >= 0 ? -32 : 32 }),
};

function MobileDeck({ runs, index, setIndex, reduce }: { runs: RunCard[]; index: number; setIndex: (i: number) => void; reduce: boolean }) {
  const [dir, setDir] = useState(0);
  const [selected, setSelected] = useState<number | null>(null);
  const run = runs[index];
  const go = (ni: number, d: number) => {
    if (ni < 0 || ni >= runs.length || ni === index) return;
    setDir(d);
    setIndex(ni);
    setSelected(null);
  };
  const pickPost = (i: number) => setSelected(Math.max(0, Math.min(run.trajectory.length - 1, i)));
  const onDragEnd = (_: unknown, info: PanInfo) => {
    if (info.offset.x < -55) go(index + 1, 1);
    else if (info.offset.x > 55) go(index - 1, -1);
  };
  return (
    <div className="min-w-0 max-w-full overflow-x-hidden lg:hidden">
      <div className="mb-3 flex items-start justify-between">
        <div className="min-w-0">
          <Eyebrow accent={run.live}>{run.deckLabel}</Eyebrow>
          <div className="mt-1 truncate text-[9px] font-black uppercase tracking-[0.14em] text-foreground/35">{run.span}</div>
        </div>
        <div className="flex items-center gap-1.5">
          {runs.map((r, i) => (
            <button key={r.id} type="button" aria-label={r.deckLabel} onClick={() => go(i, i > index ? 1 : -1)} className={cn('h-1.5 rounded-full transition-all', i === index ? 'w-5 bg-[#E11D48]' : 'w-1.5 bg-foreground/20')} />
          ))}
        </div>
      </div>

      {/* one card. drag it sideways from ANYWHERE to change run; scroll to read.
          tapping anywhere (except a bar/nav) returns a selected post to the run. */}
      <motion.div
        drag={reduce ? false : 'x'}
        dragConstraints={{ left: 0, right: 0 }}
        dragElastic={0.16}
        dragSnapToOrigin
        onDragEnd={onDragEnd}
        onClick={() => { if (selected != null) setSelected(null); }}
        style={{ touchAction: 'pan-y' }}
        className="fm-depth-glass relative max-w-full cursor-grab overflow-hidden rounded-[22px] p-4 active:cursor-grabbing sm:rounded-[24px] sm:p-5"
      >
        <div aria-hidden className="pointer-events-none absolute inset-0 -z-10 opacity-90" style={{ background: 'radial-gradient(ellipse 56% 46% at 100% 0%, rgba(225,29,72,0.1), transparent 56%)' }} />
        <AnimatePresence mode="wait" custom={dir} initial={false}>
          <motion.div
            key={run.id}
            custom={dir}
            variants={reduce ? undefined : slideVariants}
            initial={reduce ? false : 'enter'}
            animate="center"
            exit={reduce ? undefined : 'exit'}
            transition={{ duration: 0.28, ease: [0.16, 0.9, 0.2, 1] }}
            className="min-w-0 max-w-full overflow-hidden"
          >
            <HeroZone run={run} selected={selected} onSelect={pickPost} onClose={() => setSelected(null)} reduce={reduce} />
            <motion.div {...heroReveal(6, reduce)} className="mt-5 sm:mt-6">
              <div className="flex items-baseline justify-between gap-2">
                <Eyebrow className="shrink-0 whitespace-nowrap">The run, post by post</Eyebrow>
                <span className="shrink-0 whitespace-nowrap text-[9px] font-black uppercase tracking-[0.14em] text-foreground/30">{selected == null ? 'tap a bar' : 'tap to close'}</span>
              </div>
              <div className="mt-2"><RunBars run={run} selectedIndex={selected} onSelect={pickPost} reduce={reduce} mode="mobile" /></div>
            </motion.div>
            <div className="mt-6 sm:mt-7"><Triad run={run} reduce={reduce} delay={HERO_BASE + 8 * HERO_STEP} /></div>
            <div className="mt-6 sm:mt-7"><ShiftList run={run} reduce={reduce} delay={HERO_BASE + 11 * HERO_STEP} /></div>
          </motion.div>
        </AnimatePresence>
      </motion.div>
      <p className="mt-3 text-center text-[9px] font-black uppercase tracking-[0.2em] text-foreground/25">swipe sideways to change run · scroll for the full read</p>
    </div>
  );
}

// ── desktop split ──────────────────────────────────────────────────────────
function DesktopSplit({ runs, index, setIndex, reduce }: { runs: RunCard[]; index: number; setIndex: (i: number) => void; reduce: boolean }) {
  const run = runs[index];

  return (
    <div className="hidden lg:block">
      <ReaderMonument runs={runs} index={index} setIndex={setIndex} reduce={reduce} />

      <AnimatePresence mode="wait" initial={false}>
        <motion.div
          key={`read-${run.id}`}
          initial={reduce ? false : { opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          exit={reduce ? undefined : { opacity: 0, y: -8 }}
          transition={{ duration: 0.28, ease: SOFT_EASE }}
          className="mt-7 grid min-w-0 gap-10 xl:grid-cols-[minmax(0,0.86fr)_minmax(320px,0.44fr)] xl:gap-12"
        >
          <article className="min-w-0 max-w-[900px]">
            <Triad run={run} reduce={reduce} delay={HERO_BASE + 2 * HERO_STEP} />
          </article>
          <aside className="min-w-0 xl:pt-2">
            <ShiftList run={run} reduce={reduce} delay={HERO_BASE + 6 * HERO_STEP} />
          </aside>
        </motion.div>
      </AnimatePresence>
    </div>
  );
}

export default function FeederReaderPage({ onBack, backLabel = 'Dashboard', feederLabel = '@anuj', showHeader = true }: { onBack?: () => void; backLabel?: string; feederLabel?: string; showHeader?: boolean } = {}) {
  const reduce = Boolean(useReducedMotion());
  const [index, setIndex] = useState(0);
  const backClass = 'flex min-w-0 items-center gap-1.5 text-[10px] font-black uppercase tracking-[0.16em] text-foreground/45 no-underline transition-colors hover:text-foreground/70 sm:text-[11px] sm:tracking-[0.2em]';
  return (
    // Cap the reader so large screens feel expansive without stretching the read.
    <div className="mx-auto min-w-0 max-w-full overflow-x-hidden text-foreground lg:max-w-[1180px] xl:max-w-[1280px] 2xl:max-w-[1360px]">
      {showHeader && (
        <div className="mb-6 grid min-w-0 grid-cols-[1fr_auto_1fr] items-center gap-2">
          {onBack ? (
            <button type="button" onClick={onBack} className={cn(backClass, 'bg-transparent p-0 text-left')}><span className="text-[14px]">‹</span><span className="truncate">{backLabel}</span></button>
          ) : (
            <Link href="/" className={backClass}><span className="text-[14px]">‹</span><span className="truncate">{backLabel}</span></Link>
          )}
          <div className="flex min-w-0 items-center gap-2"><span className="h-2 w-2 shrink-0 rounded-full bg-[#E11D48] fm-live-dot" /><span className="truncate text-[10px] font-black uppercase tracking-[0.18em] text-foreground/55 sm:text-[11px] sm:tracking-[0.26em]">Feeder Reader</span></div>
          <span className="min-w-0 truncate text-right text-[11px] font-black tracking-tight text-foreground/70">{feederLabel}</span>
        </div>
      )}
      <MobileDeck runs={RUNS} index={index} setIndex={setIndex} reduce={reduce} />
      <DesktopSplit runs={RUNS} index={index} setIndex={setIndex} reduce={reduce} />
    </div>
  );
}
