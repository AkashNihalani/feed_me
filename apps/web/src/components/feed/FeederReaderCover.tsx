'use client';

/* ─────────────────────────────────────────────────────────────
   FEEDER READER COVER — the dashboard scroll-stopper.
   A big chunky horizontal strip of recent runs that lights up F1-start
   style on entry — each block's fill = that run's average rank weight.
   Past runs are sealed blocks; the current run is a live block charging
   to how-deep-we-are (3/10), filled to the heat it's earned so far.
   Number is framed against the recent baseline so "cooling" means
   something. The line states what's happening — no pep talk.
   Light/dark via tokens. Mock data for now.
   ───────────────────────────────────────────────────────────── */

import Link from 'next/link';
import { motion, useReducedMotion } from 'framer-motion';
import { cn } from '@/lib/utils';
import Odometer from '@/components/login/Odometer';

type Run = { when: string; avgRank: number };
type VisualTier = 0 | 1 | 2 | 3 | 4;

const PAST: Run[] = [
  { when: '6 wk', avgRank: 12 },
  { when: '4 wk', avgRank: 6 },
  { when: '3 wk', avgRank: 24 },
  { when: '2 wk', avgRank: 46 },
];
const CURRENT = { avgRank: 34, postsIn: 3, postsTotal: 10 };
// Mock server-side verdict. Keep this short: the visible numbers already carry the detail.
const SERVER_LINES = ['Pace is leaking.', 'Needs a breaker.'];
const VISIBLE_AVGS = [...PAST.map((run) => run.avgRank), CURRENT.avgRank];
const BEST_TO_WORST = [...VISIBLE_AVGS].sort((a, b) => a - b);

function tierFor(avg: number, live = false): VisualTier {
  const rank = BEST_TO_WORST.indexOf(avg);
  if (!live && (rank === 0 || avg <= 8)) return 4;
  if (!live && (rank === 1 || avg <= 14)) return 3;
  if (avg <= 20) return 2;
  if (avg <= 40) return 1;
  return 0;
}

// Hard fill language: recent hierarchy decides who owns the tape; absolute avg
// keeps weak chunks from glowing just because the field is bad.
function ember(tier: VisualTier) {
  const palette = {
    4: {
      fill: '#F71852',
      glow: 62,
      glowA: 0.78,
      fillInset: 0.4,
      pulseA: 0.9,
      text: 'text-white',
      pulse: true,
    },
    3: {
      fill: '#D41444',
      glow: 46,
      glowA: 0.58,
      fillInset: 0.3,
      pulseA: 0.68,
      text: 'text-white/92',
      pulse: true,
    },
    2: {
      fill: '#9F1239',
      glow: 26,
      glowA: 0.32,
      fillInset: 0.2,
      pulseA: 0,
      text: 'text-white/78',
      pulse: false,
    },
    1: {
      fill: '#4A0B1A',
      glow: 12,
      glowA: 0.12,
      fillInset: 0.1,
      pulseA: 0,
      text: 'text-white/58',
      pulse: false,
    },
    0: {
      fill: '#1A050A',
      glow: 4,
      glowA: 0.06,
      fillInset: 0.08,
      pulseA: 0,
      text: 'text-white/42',
      pulse: false,
    },
  }[tier];
  const fillShadow = `0 0 ${palette.glow}px rgba(225,29,72,${palette.glowA}), inset 0 1px 0 rgba(255,255,255,${palette.fillInset})`;
  return {
    fill: palette.fill,
    glow: palette.glow,
    glowA: palette.glowA,
    fillShadow,
    pulseShadow: palette.pulse
      ? `0 0 ${palette.glow + 18}px rgba(255,45,93,${palette.pulseA}), 0 0 ${palette.glow + 42}px rgba(225,29,72,0.34), inset 0 1px 0 rgba(255,255,255,${Math.min(palette.fillInset + 0.12, 0.52)})`
      : fillShadow,
    shouldPulse: palette.pulse,
    text: palette.text,
  };
}

const SLOT_FILL = { duration: 0.82, ease: [0.18, 0.86, 0.22, 1] as const };

function SealedBlock({ run, index, reduce }: { run: Run; index: number; reduce: boolean }) {
  const e = ember(tierFor(run.avgRank));
  const delay = 0.22 + index * 0.34;
  const chamberShadow = `0 0 ${Math.round(e.glow * 0.28)}px rgba(225,29,72,${e.glowA * 0.28}), inset 0 0 0 1px rgba(255,255,255,0.06)`;
  return (
    <div className="flex min-w-0 flex-col items-center gap-1.5 lg:flex-1 lg:gap-2">
      <div
        className="relative h-[64px] w-full overflow-hidden rounded-[12px] border border-black/20 bg-[#09090B] sm:h-20 sm:rounded-[14px] dark:border-white/[0.08]"
        style={{ boxShadow: chamberShadow }}
      >
        <motion.span
          aria-hidden
          className="absolute inset-y-0 left-0 w-full origin-left rounded-[11px] will-change-transform"
          style={{
            background: e.fill,
          }}
          initial={reduce ? false : { scaleX: 0, boxShadow: e.fillShadow }}
          animate={{
            scaleX: 1,
            boxShadow: !reduce && e.shouldPulse ? [e.fillShadow, e.pulseShadow, e.fillShadow] : e.fillShadow,
          }}
          transition={
            reduce
              ? { duration: 0 }
              : {
                  scaleX: { delay, ...SLOT_FILL },
                  boxShadow: e.shouldPulse
                    ? { delay: delay + SLOT_FILL.duration + 1.1, duration: 3.1, repeat: Infinity, repeatDelay: 2.4, ease: 'easeInOut' }
                    : { duration: 0 },
                }
          }
        />
        <span className="absolute inset-x-0 top-0 h-px bg-white/28" />
        <span className="absolute inset-x-0 bottom-0 h-1 bg-black/35" />
        <span className={cn('absolute left-2 top-2 inline-flex items-baseline text-[21px] font-black leading-none tabular-nums sm:left-2.5 sm:top-2.5 sm:text-[25px]', e.text)}>
          {run.avgRank}
          <span className="ml-[0.04em] text-[0.44em]">%</span>
        </span>
        <span className="absolute bottom-2 left-2 text-[7px] font-black uppercase tracking-[0.14em] text-white/48 sm:bottom-2.5 sm:left-2.5 sm:text-[8px]">avg</span>
      </div>
      <span className="text-[9px] font-black uppercase tracking-[0.16em] text-foreground/40">{run.when}</span>
    </div>
  );
}

function LiveBlock() {
  return (
    <div className="flex min-w-0 flex-col items-center gap-1.5 lg:flex-[1.45] lg:gap-2">
      <div
        className="fm-live-chunk-slot relative isolate h-[64px] w-full rounded-[12px] sm:h-20 sm:rounded-[14px]"
      >
        <span aria-hidden className="fm-live-chunk-slot__sweep" />
        <span aria-hidden className="fm-live-chunk-slot__mask rounded-[10px] sm:rounded-[12px]" />
        {/* Heat fill — lives strictly INSIDE the mask inset, so the rotating
            streak ring (sweep showing through the 2px border) is never touched. */}
        <span
          aria-hidden
          className="pointer-events-none absolute inset-[2px] z-[1] rounded-[10px] sm:rounded-[12px]"
          style={{
            background: 'radial-gradient(ellipse 92% 130% at 50% 118%, #F71852 0%, #B01038 52%, #5E0B22 100%)',
            boxShadow: 'inset 0 1px 0 rgba(255,255,255,0.22), inset 0 -10px 18px -10px rgba(0,0,0,0.55)',
          }}
        />
        <span className="fm-live-chunk-slot__content absolute inset-0 z-[2] flex items-center justify-center text-[21px] font-black leading-none tabular-nums text-white sm:text-[26px]">
          {CURRENT.postsIn}/{CURRENT.postsTotal}
        </span>
      </div>
      <span className="text-[8px] font-black uppercase tracking-[0.14em] text-[#E11D48] sm:text-[9px] sm:tracking-[0.16em]">now · live</span>
    </div>
  );
}

export default function FeederReaderCover({ onOpen, className }: { onOpen?: () => void; className?: string }) {
  const reduce = Boolean(useReducedMotion());

  const coverClass = cn(
    'fm-depth-glass group/cover relative block w-full overflow-hidden rounded-[24px] px-4 py-5 text-left text-foreground no-underline sm:rounded-[28px] sm:p-6',
    className,
  );

  const body = (
    <>
        <div
          aria-hidden
          className="pointer-events-none absolute inset-0 -z-10 opacity-90"
          style={{ background: 'radial-gradient(ellipse 60% 70% at 100% 0%, rgba(225,29,72,0.1), transparent 58%)' }}
        />

        {/* header */}
        <div className="flex items-center justify-between gap-4">
          <div className="flex min-w-0 items-center gap-2.5">
            <span className="h-2 w-2 rounded-full bg-[#E11D48] fm-live-dot" />
            <span className="truncate text-[12px] font-black uppercase tracking-[0.26em] text-foreground/52 sm:text-[11px]">Feeder Reader</span>
          </div>
          <span className="shrink-0 text-right text-[10px] font-black uppercase tracking-[0.16em] text-foreground/38">
            <span className="hidden sm:inline">last 6 weeks · 50 posts</span>
            <span className="sm:hidden">50 posts</span>
          </span>
        </div>

        {/* the score strip — chunky blocks filling by average rank */}
        <div className="mt-6 grid grid-cols-5 items-start gap-1.5 lg:flex lg:gap-2.5">
          {PAST.map((run, i) => (
            <SealedBlock key={run.when} run={run} index={i} reduce={reduce} />
          ))}
          <LiveBlock />
        </div>

        {/* server verdict + current reading */}
        <div className="mt-6 flex items-end justify-between gap-3 sm:mt-6 sm:gap-4">
          <p className="min-w-0 flex-1 text-[clamp(26px,7.4vw,36px)] font-black leading-[0.96] tracking-tight sm:text-[clamp(28px,4.4vw,40px)]">
            {SERVER_LINES.map((line) => (
              <span key={line} className="block">
                {line}
              </span>
            ))}
          </p>
          <div className="shrink-0 text-right">
            <span className="inline-flex items-baseline justify-end font-black leading-none text-foreground text-[clamp(40px,11vw,58px)] sm:text-[clamp(48px,8vw,68px)]">
              <span className="mr-1.5 self-center text-[0.18em] font-black uppercase tracking-[0.22em] text-foreground/40">avg</span>
              <Odometer value={CURRENT.avgRank} animateOnMount revealDelayMs={120} className="inline-flex min-w-[1.24em] overflow-visible" />
              <span className="ml-[0.04em] text-[0.32em] text-[#E11D48]">%</span>
            </span>
            <span className="mt-1 block text-[9px] font-black uppercase tracking-[0.16em] text-foreground/38">current</span>
          </div>
        </div>

        {/* open */}
        <div className="mt-6 flex flex-wrap items-end justify-between gap-4 border-t border-foreground/10 pt-5 sm:mt-5 sm:pt-4">
          <p className="max-w-[300px] text-[13px] font-black uppercase leading-snug tracking-[0.14em] text-foreground/42 sm:text-[12px]">
            Posts, swings, and the field behind this chunk.
          </p>
          <span className="inline-flex items-center gap-1.5 text-[11px] font-black uppercase tracking-[0.16em] text-[#E11D48] transition-transform duration-300 group-hover/cover:translate-x-0.5">
            Open breakdown
            <span className="text-[13px]">→</span>
          </span>
        </div>
    </>
  );

  if (onOpen) {
    return (
      <button type="button" onClick={onOpen} className={coverClass} data-feeder-reader-cover="true">
        {body}
      </button>
    );
  }

  return (
    <Link href="/read" className={coverClass} data-feeder-reader-cover="true">
      {body}
    </Link>
  );
}
