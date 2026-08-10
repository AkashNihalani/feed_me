'use client';

/* ─────────────────────────────────────────────────────────────
   FEEDER READER COVER — the dashboard scroll-stopper.
   A chunky horizontal strip of recent bite runs that lights up
   F1-start style on entry — each block is one run, its fill weight
   and COLOUR set by how close that run landed to baseline, using the
   reader's landing tiers (deep hit = crimson, held / soft / low =
   neutral and dimming). Past runs are sealed blocks; the current run
   is a live block charging to how-deep-we-are. The trajectory itself
   lives one tap away, on the page.
   Light/dark via tokens. Mock run history for now.
   ───────────────────────────────────────────────────────────── */

import Link from 'next/link';
import { useRef } from 'react';
import { motion, useInView, useReducedMotion } from 'framer-motion';
import { cn } from '@/lib/utils';
import Odometer from '@/components/login/Odometer';

type Run = { when: string; avgRank: number };
type Tier = 0 | 1 | 2 | 3;

const PAST: Run[] = [
  { when: '6 wk', avgRank: 12 },
  { when: '4 wk', avgRank: 6 },
  { when: '3 wk', avgRank: 24 },
  { when: '2 wk', avgRank: 46 },
];
const CURRENT = { avgRank: 34, postsIn: 3, postsTotal: 10 };
const SERVER_LINES = ['Pace is leaking.', 'Needs a breaker.'];

// Closeness to baseline decides the tier — the same way the reader ranks a
// landing. Lower avg rank = closer to the ceiling = a deeper bite.
function tierFor(avg: number): Tier {
  if (avg <= 14) return 3; // deep hit — well past baseline
  if (avg <= 28) return 2; // held — around baseline
  if (avg <= 42) return 1; // soft — slipping under
  return 0; // low
}

// Tiers on the always-dark chamber: deep is the crimson punch, the rest are
// neutral and progressively dimmer — mirrors the page's deep / held / soft / low.
function slot(tier: Tier) {
  const palette = {
    3: { fill: '#F71852', text: 'text-white', glow: 50, glowA: 0.6, pulse: true },
    2: { fill: '#55555C', text: 'text-white/90', glow: 0, glowA: 0, pulse: false },
    1: { fill: '#3A3A41', text: 'text-white/78', glow: 0, glowA: 0, pulse: false },
    0: { fill: '#2A2A30', text: 'text-white/58', glow: 0, glowA: 0, pulse: false },
  }[tier];
  const fillShadow = palette.glow
    ? `0 0 ${palette.glow}px rgba(247,24,82,${palette.glowA}), inset 0 1px 0 rgba(255,255,255,0.34)`
    : 'inset 0 1px 0 rgba(255,255,255,0.08)';
  return {
    fill: palette.fill,
    text: palette.text,
    pulse: palette.pulse,
    fillShadow,
    pulseShadow: palette.pulse
      ? `0 0 ${palette.glow + 20}px rgba(255,45,93,0.86), 0 0 ${palette.glow + 44}px rgba(225,29,72,0.34), inset 0 1px 0 rgba(255,255,255,0.5)`
      : fillShadow,
    chamberShadow: palette.glow
      ? `0 0 ${Math.round(palette.glow * 0.3)}px rgba(247,24,82,0.26), inset 0 0 0 1px rgba(255,255,255,0.06)`
      : 'inset 0 0 0 1px rgba(255,255,255,0.06)',
  };
}

const SLOT_FILL = { duration: 0.82, ease: [0.18, 0.86, 0.22, 1] as const };

function SealedBlock({ run, index, reduce, play }: { run: Run; index: number; reduce: boolean; play: boolean }) {
  const s = slot(tierFor(run.avgRank));
  const delay = 0.22 + index * 0.34;
  return (
    <div className="flex min-w-0 flex-col items-center gap-1.5 lg:flex-1 lg:gap-2">
      <div
        className="relative h-[72px] w-full overflow-hidden rounded-[12px] border border-black/20 bg-[#09090B] sm:h-20 sm:rounded-[14px] lg:h-28 xl:h-32 dark:border-white/[0.08]"
        style={{ boxShadow: s.chamberShadow }}
      >
        <motion.span
          aria-hidden
          className="absolute inset-y-0 left-0 w-full origin-left rounded-[11px] will-change-transform"
          style={{ background: s.fill }}
          initial={reduce ? false : { scaleX: 0, boxShadow: s.fillShadow }}
          animate={{
            scaleX: reduce || play ? 1 : 0,
            boxShadow: !reduce && play && s.pulse ? [s.fillShadow, s.pulseShadow, s.fillShadow] : s.fillShadow,
          }}
          transition={
            reduce
              ? { duration: 0 }
              : {
                  scaleX: play ? { delay, ...SLOT_FILL } : { duration: 0 },
                  boxShadow: play && s.pulse
                    ? { delay: delay + SLOT_FILL.duration + 1.1, duration: 3.1, repeat: Infinity, repeatDelay: 2.4, ease: 'easeInOut' }
                    : { duration: 0 },
                }
          }
        />
        <span className="absolute inset-x-0 top-0 h-px bg-white/22" />
        <span className="absolute inset-x-0 bottom-0 h-1 bg-black/35" />
        <span className={cn('absolute left-2 top-2 inline-flex items-baseline text-[24px] font-black leading-none tabular-nums sm:left-2.5 sm:top-2.5 sm:text-[28px] lg:text-[38px] xl:text-[44px]', s.text)}>
          {run.avgRank}
          <span className="ml-[0.04em] text-[0.44em]">%</span>
        </span>
        <span className="absolute bottom-2 left-2 text-[7px] font-black uppercase tracking-[0.14em] text-white/48 sm:bottom-2.5 sm:left-2.5 sm:text-[8px]">avg</span>
      </div>
      <span className="text-[9px] font-black uppercase tracking-[0.16em] text-foreground/40">{run.when}</span>
    </div>
  );
}

function LiveBlock({ reduce, play }: { reduce: boolean; play: boolean }) {
  const s = slot(tierFor(CURRENT.avgRank));
  const progress = Math.min(1, Math.max(0, CURRENT.postsIn / CURRENT.postsTotal));
  const delay = 0.22 + PAST.length * 0.34;

  return (
    <div className="col-span-4 flex min-w-0 flex-col items-center gap-1.5 lg:col-span-1 lg:flex-[1.45] lg:gap-2">
      <div className="fm-live-chunk-slot relative isolate h-[72px] w-full rounded-[12px] sm:h-20 sm:rounded-[14px] lg:h-28 xl:h-32">
        <span aria-hidden className="fm-live-chunk-slot__sweep" />
        <span aria-hidden className="fm-live-chunk-slot__mask rounded-[10px] sm:rounded-[12px]" />
        <motion.span
          aria-hidden
          className="pointer-events-none absolute inset-[2px] z-[1] origin-left rounded-[10px] will-change-transform sm:rounded-[12px]"
          style={{ background: s.fill, boxShadow: s.fillShadow }}
          initial={reduce ? false : { scaleX: 0 }}
          animate={{ scaleX: reduce || play ? progress : 0 }}
          transition={reduce ? { duration: 0 } : play ? { delay, ...SLOT_FILL } : { duration: 0 }}
        />
        <span className="fm-live-chunk-slot__content flex items-center justify-center text-[30px] font-black leading-none tabular-nums text-white sm:text-[34px] lg:text-[48px] xl:text-[56px]">
          {CURRENT.postsIn}/{CURRENT.postsTotal}
        </span>
      </div>
      <span className="text-[8px] font-black uppercase tracking-[0.14em] text-[#E11D48] sm:text-[9px] sm:tracking-[0.16em]">now · live</span>
    </div>
  );
}

export default function FeederReaderCover({ onOpen, className }: { onOpen?: () => void; className?: string }) {
  const reduce = Boolean(useReducedMotion());
  const viewRef = useRef<HTMLSpanElement>(null);
  const play = useInView(viewRef, { once: true, amount: 0.35 });

  const coverClass = cn(
    'fm-depth-glass group/cover relative block w-full overflow-hidden rounded-[24px] px-4 py-5 text-left text-foreground no-underline sm:rounded-[28px] sm:p-6',
    className,
  );

  const body = (
    <>
      <span ref={viewRef} aria-hidden className="pointer-events-none absolute inset-0" />
      <div aria-hidden className="pointer-events-none absolute inset-0 -z-10 opacity-90" style={{ background: 'radial-gradient(ellipse 60% 70% at 100% 0%, rgba(225,29,72,0.1), transparent 58%)' }} />

      {/* header */}
      <div className="flex items-center justify-between gap-4">
        <div className="flex min-w-0 items-center gap-2.5">
          <span className="h-2 w-2 rounded-full bg-[#E11D48] fm-live-dot" />
          <span className="truncate text-[12px] font-black uppercase tracking-[0.26em] text-foreground/52 sm:text-[11px]">Feeder Reader</span>
        </div>
        <span className="shrink-0 text-right text-[10px] font-black uppercase tracking-[0.16em] text-foreground/38">
          <span className="hidden sm:inline">recent bite runs</span>
          <span className="sm:hidden">5 runs</span>
        </span>
      </div>

      {/* the run strip — one block per recent bite run, coloured by how it landed */}
      <div className="mt-6 grid grid-cols-4 items-start gap-2 lg:flex lg:gap-3 xl:gap-4">
        {PAST.map((run, i) => (
          <SealedBlock key={run.when} run={run} index={i} reduce={reduce} play={play} />
        ))}
        <LiveBlock reduce={reduce} play={play} />
      </div>

      {/* server verdict + current reading */}
      <div className="mt-6 flex items-end justify-between gap-3 sm:gap-4">
        <p className="min-w-0 flex-1 text-[clamp(26px,7.4vw,36px)] font-black leading-[0.96] tracking-tight sm:text-[clamp(28px,4.4vw,40px)]">
          {SERVER_LINES.map((line) => (
            <span key={line} className="block">{line}</span>
          ))}
        </p>
        <div className="shrink-0 text-right">
          <span className="inline-flex items-baseline justify-end font-black leading-none text-foreground text-[clamp(44px,12vw,64px)] sm:text-[clamp(56px,7vw,86px)] lg:text-[clamp(92px,6vw,128px)]">
            <span className="mr-1.5 self-center text-[0.18em] font-black uppercase tracking-[0.22em] text-foreground/40">avg</span>
            <Odometer value={CURRENT.avgRank} animateOnMount revealDelayMs={120} className="inline-flex min-w-[1.24em] overflow-visible" />
            <span className="ml-[0.04em] text-[0.32em] text-[#E11D48]">%</span>
          </span>
          <span className="mt-1 block text-[9px] font-black uppercase tracking-[0.16em] text-foreground/38">current</span>
        </div>
      </div>

      {/* open */}
      <div className="mt-6 flex flex-wrap items-end justify-between gap-4 border-t border-foreground/10 pt-5 sm:mt-5 sm:pt-4">
        <p className="max-w-[320px] text-[13px] font-black uppercase leading-snug tracking-[0.14em] text-foreground/42 sm:text-[12px]">
          The trajectory, what got bit, and the moves.
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
