'use client';

import { memo, useLayoutEffect, useMemo, useRef, type RefObject } from 'react';
import { motion, AnimatePresence, useReducedMotion } from 'framer-motion';
import {
  ArrowDown,
  ArrowUp,
  CalendarDays,
  Circle,
  Clock,
  Film,
  Flame,
  Image as ImageIcon,
  Layers,
  Sparkles,
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { BEAT_ARRIVE_S, BEAT_EASE_CSS, BEAT_MAX_STEPS, beatArrive, beatDelay, beatLeave } from '@/lib/motion';
import { PostingPatternPayload, Timeframe } from './dashboardTypes';

/* ═══════════════════════════════════════════════════════════════════
   POSTING PULSE  v4
   Headline → Activity strip → Stat chips → Contributors | Fingerprint
   ═══════════════════════════════════════════════════════════════════ */

type Props = {
  pattern: PostingPatternPayload | null | undefined;
  timeframe?: Timeframe;
};

const EASE = [0.22, 1, 0.36, 1] as const;
const LAYOUT_SPRING = { type: 'spring' as const, stiffness: 280, damping: 30, mass: 0.86 };

// Swaps (a value, the headline, the lower panel) run on the page beat as opacity plus ONE transform string, which
// framer hands to the compositor; independent y/scale props would tick on the main thread.
const SWAP_HIDDEN = { opacity: 0, transform: 'translateY(10px) scale(0.98)' } as const;
const SWAP_SHOWN = { opacity: 1, transform: 'translateY(0px) scale(1)' } as const;
const SWAP_GONE = { opacity: 0, transform: 'translateY(-8px) scale(0.98)' } as const;
const ENTRY_EXIT = {
  initial: SWAP_HIDDEN,
  animate: { ...SWAP_SHOWN, transition: beatArrive(0) },
  exit: { ...SWAP_GONE, transition: beatLeave(0) },
} as const;
const PANEL_ENTRY_EXIT = {
  initial: { opacity: 0, transform: 'translateY(14px) scale(0.985)' },
  animate: { opacity: 1, transform: 'translateY(0px) scale(1)', transition: beatArrive(0) },
  exit: { opacity: 0, transform: 'translateY(-10px) scale(0.985)', transition: beatLeave(0) },
} as const;
const CHIP_EXIT = { opacity: 0, transform: 'translateY(10px) scale(0.96)', transition: beatLeave(0) } as const;
const CELL_EXIT = { opacity: 0, transform: 'translateY(10px) scale(0.97)', transition: beatLeave(0) } as const;
// Rows and media chips keep a position-only layout glide (they genuinely reorder when the ranking flips). Their exit
// is opacity-only so it never fights the projection transform.
const REORDER_TRANSITION = { layout: LAYOUT_SPRING, opacity: { duration: 0.18, ease: EASE } } as const;

// a sweep across a strip: index 0 → the first beat step, the last index → the last, however many bars there are
function sweepStep(index: number, count: number): number {
  return count > 1 ? (index / (count - 1)) * BEAT_MAX_STEPS : 0;
}

function reducedMotionPreferred(): boolean {
  return typeof window !== 'undefined' && window.matchMedia?.('(prefers-reduced-motion: reduce)').matches === true;
}

const BAR_DEAL_ID = 'fm-bar-deal';

/**
 * Bars sit at their real size (static height/width). A bar whose size changes is re-dealt: it grows back out of its
 * base on the page beat with ONE transform animation (compositor-only; no height/width tween, no per-frame JS). The
 * first pass grows every bar when `growOnMount`, otherwise it only notes the resting sizes.
 * Bars carry data-bar (key), data-bar-size (what counts as a change), data-bar-step (their slot on the beat) and
 * data-bar-axis="x" to grow from the left edge instead of the base.
 */
function useBarDeal(rootRef: RefObject<HTMLElement | null>, trigger: unknown, growOnMount: boolean) {
  const sizesRef = useRef<Map<string, string> | null>(null);

  useLayoutEffect(() => {
    const root = rootRef.current;
    if (!root) return;
    const bars = Array.from(root.querySelectorAll<HTMLElement>('[data-bar]'));
    const previous = sizesRef.current;
    const next = new Map<string, string>();
    for (const bar of bars) next.set(bar.dataset.bar ?? '', bar.dataset.barSize ?? '');
    sizesRef.current = next;
    if ((!previous && !growOnMount) || reducedMotionPreferred()) return;

    for (const bar of bars) {
      const key = bar.dataset.bar ?? '';
      const size = next.get(key) ?? '';
      if (!(Number(size) > 0) || previous?.get(key) === size) continue;
      for (const running of bar.getAnimations()) {
        if (running.id === BAR_DEAL_ID) running.cancel();
      }
      const axis = bar.dataset.barAxis === 'x' ? 'X' : 'Y';
      bar.animate(
        [{ transform: `scale${axis}(0)` }, { transform: `scale${axis}(1)` }],
        {
          id: BAR_DEAL_ID,
          duration: BEAT_ARRIVE_S * 1000,
          delay: beatDelay(Number(bar.dataset.barStep) || 0) * 1000,
          easing: BEAT_EASE_CSS,
          fill: 'backwards',
        },
      );
    }
  }, [growOnMount, rootRef, trigger]);
}

/* ── Media icon (lucide, not emoji) ── */

function MediaGlyph({
  type,
  className,
  strokeWidth = 2.4,
}: {
  type: string | null | undefined;
  className?: string;
  strokeWidth?: number;
}) {
  const base = cn('h-3 w-3 shrink-0', className);
  if (!type || type === 'unknown') return <Circle className={base} strokeWidth={strokeWidth} />;
  const l = type.toLowerCase();
  if (l === 'video' || l === 'reel') return <Film className={base} strokeWidth={strokeWidth} />;
  if (l === 'image' || l === 'photo') return <ImageIcon className={base} strokeWidth={strokeWidth} />;
  if (l.includes('carousel') || l.includes('sidecar')) return <Layers className={base} strokeWidth={strokeWidth} />;
  return <Circle className={base} strokeWidth={strokeWidth} />;
}

function mediaLabel(type: string | null | undefined): string {
  if (!type || type === 'unknown') return 'Mix';
  if (type === 'sidecar/carousel') return 'Carousel';
  const l = type.toLowerCase();
  if (l === 'video') return 'Reel';
  return type.replace(/\b\w/g, (c) => c.toUpperCase());
}

function windowLabel(tf: Timeframe): string {
  if (tf === '7D') return 'this week';
  if (tf === '30D') return 'this month';
  if (tf === '60D') return 'in 60 days';
  return 'this quarter';
}

function timeframeDays(tf: Timeframe): number {
  if (tf === '7D') return 7;
  if (tf === '30D') return 30;
  if (tf === '60D') return 60;
  return 90;
}

function formatGapShort(hours: number | null | undefined): string {
  const numeric = hours == null ? NaN : Number(hours);
  if (!Number.isFinite(numeric)) return '--';
  if (numeric < 1) return '<1h';
  if (numeric < 24) return `${Math.round(numeric)}h`;
  const days = numeric / 24;
  return days >= 10 ? `${Math.round(days)}d` : `${days.toFixed(1)}d`;
}

function formatCadence(v: number): string {
  const numeric = Number(v);
  if (!Number.isFinite(numeric)) return '--';
  return numeric >= 10 ? `${Math.round(numeric)}` : numeric.toFixed(1);
}

function buildHeadline(p: PostingPatternPayload, tf: Timeframe): string {
  const totalPosts = p.rhythm_days.reduce((s, d) => s + Math.max(0, d.post_count), 0);
  const windowText = windowLabel(tf);
  const daysSince = p.days_since_last_post;
  const delta = p.delta_percent;

  let gapSuffix = '';
  if (daysSince != null && daysSince > 0) {
    gapSuffix = daysSince === 1 ? ' · last post yesterday' : ` · last post ${daysSince}d ago`;
  } else if (daysSince === 0) {
    gapSuffix = ' · posted today';
  }

  if (p.status === 'insufficient_data') return 'Just getting started — pattern forming';
  if (p.status === 'dormant') {
    return daysSince != null
      ? `Silent for ${daysSince} day${daysSince === 1 ? '' : 's'} — longest gap yet`
      : 'No recent posts detected';
  }

  let deltaClause = '';
  if (delta != null && Math.abs(delta) >= 5) {
    deltaClause = delta > 0
      ? ` — ${Math.round(delta)}% hotter than usual`
      : ` — ${Math.abs(Math.round(delta))}% cooler than usual`;
  } else {
    deltaClause = ' — same energy as usual';
  }

  return `${totalPosts} post${totalPosts === 1 ? '' : 's'} ${windowText}${deltaClause}${gapSuffix}`;
}

/* ── Dot grouping for activity strip ── */

type DotGroup = { label: string; days: Array<{ day: string; count: number; index: number }> };

function groupDots(rhythmDays: PostingPatternPayload['rhythm_days'], tf: Timeframe): DotGroup[] {
  const days = rhythmDays.map((d, index) => ({ day: d.day_ist, count: Math.max(0, d.post_count), index }));

  if (tf === '7D') {
    return days.map((d) => {
      const date = new Date(d.day + 'T00:00:00+05:30');
      const label = date.toLocaleDateString('en-US', { weekday: 'narrow', timeZone: 'Asia/Kolkata' });
      return { label, days: [d] };
    });
  }

  const groups: DotGroup[] = [];
  for (let i = 0; i < days.length; i += 7) {
    const chunk = days.slice(i, i + 7);
    groups.push({ label: `W${Math.floor(i / 7) + 1}`, days: chunk });
  }
  return groups;
}

/* ── Fingerprint stat derivations (for single-feeder view) ── */

const WEEKDAY_LONG = ['Sundays', 'Mondays', 'Tuesdays', 'Wednesdays', 'Thursdays', 'Fridays', 'Saturdays'];

function computePeakDay(rhythmDays: PostingPatternPayload['rhythm_days']): string | null {
  const buckets = [0, 0, 0, 0, 0, 0, 0];
  let any = false;
  for (const d of rhythmDays) {
    const count = Math.max(0, d.post_count);
    if (count <= 0) continue;
    any = true;
    const date = new Date(d.day_ist + 'T00:00:00+05:30');
    const dow = date.getDay();
    buckets[dow] += count;
  }
  if (!any) return null;
  let maxIdx = 0;
  for (let i = 1; i < 7; i++) {
    if (buckets[i] > buckets[maxIdx]) maxIdx = i;
  }
  return WEEKDAY_LONG[maxIdx];
}

function computeActiveDays(rhythmDays: PostingPatternPayload['rhythm_days']): number {
  return rhythmDays.reduce((n, d) => n + (d.post_count > 0 ? 1 : 0), 0);
}

/* ── Tokens ── */

const CHIP_CLASS = cn(
  'rounded-[18px] px-3 py-3 text-center sm:rounded-[14px] sm:px-3 sm:py-2.5',
  'bg-gradient-to-b from-white/70 to-white/40 border border-white/70',
  'shadow-[inset_0_1px_0_rgba(255,255,255,0.8),0_2px_8px_rgba(15,23,42,0.04)]',
  'dark:from-white/[0.05] dark:to-white/[0.02] dark:border-white/[0.06]',
  'dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04),0_4px_12px_rgba(0,0,0,0.2)]',
);

const CHIP_LABEL = 'text-[10px] font-black uppercase leading-[1.02] tracking-[0.14em] text-foreground/45 dark:text-white/38 sm:text-[10px]';
const CHIP_VALUE = 'mt-1.5 text-[18px] font-black leading-none tracking-[-0.04em] text-foreground dark:text-white sm:mt-1 sm:text-[18px] lg:text-[18px]';
const SECTION_LABEL = 'text-[10px] font-black uppercase tracking-[0.14em] text-foreground/38 dark:text-white/32 sm:text-[10px]';

function statGridClass(total: number): string {
  if (total <= 2) return 'grid-cols-6 sm:grid-cols-2';
  if (total === 3) return 'grid-cols-6 sm:grid-cols-3';
  if (total === 4) return 'grid-cols-6 sm:grid-cols-4';
  return 'grid-cols-6 sm:grid-cols-5';
}

function statSpanClass(total: number, index: number): string {
  if (total <= 2) return 'col-span-3 sm:col-span-1';
  if (total === 3) return 'col-span-2 sm:col-span-1';
  if (total === 4) return 'col-span-3 sm:col-span-1';
  if (total === 5) return index >= 3 ? 'col-span-3 sm:col-span-1' : 'col-span-2 sm:col-span-1';
  return 'col-span-2 sm:col-span-1';
}

// `step` staggers sibling values on the beat, so a re-dealt card settles in reading order.
function AnimatedValue({
  value,
  className,
  step = 0,
}: {
  value: string;
  className?: string;
  step?: number;
}) {
  const reduceMotion = useReducedMotion();

  if (reduceMotion) {
    return <span className={className}>{value}</span>;
  }

  return (
    <span className="relative block overflow-hidden">
      <AnimatePresence initial={false} mode="popLayout">
        <motion.span
          key={value}
          initial={SWAP_HIDDEN}
          animate={{ ...SWAP_SHOWN, transition: beatArrive(step) }}
          exit={{ ...SWAP_GONE, transition: beatLeave(step) }}
          className={cn('block', className)}
        >
          {value}
        </motion.span>
      </AnimatePresence>
    </span>
  );
}

/* ── Small atoms ── */

function DeltaPill({ value }: { value: number }) {
  const positive = value > 0;
  const Icon = positive ? ArrowUp : ArrowDown;
  return (
    <span
      className={cn(
        'inline-flex items-center gap-0.5 rounded-full px-1.5 py-[3px] text-[10px] font-black tabular-nums leading-none',
        positive
          ? 'bg-[var(--fm-accent)]/10 text-[var(--fm-accent)] dark:bg-[var(--fm-accent)]/15'
          : 'bg-foreground/[0.06] text-foreground/55 dark:bg-white/[0.06] dark:text-white/45',
      )}
    >
      <Icon className="h-2.5 w-2.5" strokeWidth={3.2} />
      {Math.abs(Math.round(value))}%
    </span>
  );
}

function StatusDot({ status }: { status: PostingPatternPayload['feeder_rows'][number]['status'] }) {
  const isDormant = status === 'dormant' || status === 'insufficient_data';
  return (
    <span
      className={cn(
        'h-1.5 w-1.5 shrink-0 rounded-full',
        isDormant
          ? 'bg-foreground/20 dark:bg-white/20'
          : status === 'accelerating'
            ? 'bg-[var(--fm-accent)] shadow-[0_0_6px_rgb(var(--fm-accent-rgb)/0.45)]'
            : 'bg-foreground/45 dark:bg-white/45',
      )}
    />
  );
}

/* ── Component ── */

function FeedPostingPattern({ pattern, timeframe = '7D' }: Props) {
  const prefersReducedMotion = useReducedMotion();
  const reduceMotion = Boolean(prefersReducedMotion);
  const cardRef = useRef<HTMLDivElement>(null);
  // every bar on the card (activity strip, media fills, contributor bars) grows in on the beat on arrival, and
  // re-deals when a new pattern / timeframe changes its size
  const barTrigger = useMemo(() => ({ pattern, timeframe }), [pattern, timeframe]);
  useBarDeal(cardRef, barTrigger, true);
  const maxCount = useMemo(() => {
    const counts = pattern?.rhythm_days?.map((d) => Math.max(0, d.post_count)) ?? [];
    return Math.max(1, ...counts);
  }, [pattern]);

  const dotGroups = useMemo(
    () => groupDots(pattern?.rhythm_days ?? [], timeframe),
    [pattern?.rhythm_days, timeframe],
  );

  const totalDays = pattern?.rhythm_days?.length ?? 0;
  const useCompactDots = totalDays > 30;

  const mediaMix = useMemo(() => {
    if (!pattern?.media_mix?.length) {
      const seen = new Map<string, number>();
      for (const row of pattern?.feeder_rows ?? []) {
        const t = row.dominant_media_type;
        if (t && t !== 'unknown') seen.set(t, (seen.get(t) || 0) + 1);
      }
      const total = Array.from(seen.values()).reduce((s, v) => s + v, 0) || 1;
      return Array.from(seen.entries())
        .map(([type, count]) => ({ type, count, pct: Math.round((count / total) * 100) }))
        .sort((a, b) => b.count - a.count);
    }
    return pattern.media_mix;
  }, [pattern]);

  const maxFeederRate = useMemo(() => {
    if (!pattern) return 1;
    return Math.max(1, ...pattern.feeder_rows.map((r) => r.posts_per_week_current));
  }, [pattern]);

  const topMediaPct = useMemo(() => {
    if (!mediaMix.length) return -1;
    return Math.max(...mediaMix.map((m) => m.pct));
  }, [mediaMix]);

  const peakDay = useMemo(
    () => computePeakDay(pattern?.rhythm_days ?? []),
    [pattern?.rhythm_days],
  );
  const activeDays = useMemo(
    () => computeActiveDays(pattern?.rhythm_days ?? []),
    [pattern?.rhythm_days],
  );

  /* ── Empty state ── */
  if (!pattern) {
    return (
      <div className="fm-depth-glass relative flex h-full w-full flex-col justify-between overflow-hidden rounded-[22px] p-3 sm:p-3.5 lg:p-4">
        <div className="relative z-10">
          <span className="fm-label fm-depth-title">Posting Pulse</span>
          <div className="mt-5 text-[22px] font-black leading-tight text-foreground dark:text-white sm:text-[28px]">
            Just getting started
          </div>
          <p className="mt-3 max-w-[26rem] text-[12px] font-semibold leading-5 text-foreground/52 dark:text-white/48">
            Activity appears after posts land in the current window. Check back soon.
          </p>
        </div>
      </div>
    );
  }

  const headline = buildHeadline(pattern, timeframe);
  const rows = pattern.feeder_rows.slice(0, 4);
  const singleScope = rows.length <= 1;
  const statCount = 2 + mediaMix.length;

  /* ── Fingerprint stats (used when one feeder or none in list) ── */
  const signatureMix = mediaMix[0] ?? null;
  const windowDays = timeframeDays(timeframe);

  return (
    <div ref={cardRef} className="fm-depth-glass relative flex h-full w-full flex-col overflow-hidden rounded-[22px] p-3 sm:p-3.5 lg:p-4">
      <div className="relative z-10 flex h-full min-h-0 flex-col">

        {/* ── Label ── */}
        <span className="fm-label fm-depth-title">Posting Pulse</span>

        {/* ── Conversational Headline ── */}
        <AnimatePresence initial={false} mode="popLayout">
          <motion.p
            key={headline}
            {...(reduceMotion ? {} : ENTRY_EXIT)}
            className="mt-2 text-[clamp(15px,3vw,20px)] font-black leading-[1.25] tracking-[-0.04em] text-foreground dark:text-white"
          >
            {headline}
          </motion.p>
        </AnimatePresence>

        {/* ── Activity Strip ── */}
        <div
          className={cn(
            'mt-3.5 rounded-[14px] px-2.5 py-2.5 sm:px-3',
            'bg-black/[0.03] border border-black/[0.04]',
            'shadow-[inset_0_2px_4px_rgba(0,0,0,0.04)]',
            'dark:bg-white/[0.03] dark:border-white/[0.04]',
            'dark:shadow-[inset_0_2px_6px_rgba(0,0,0,0.3)]',
          )}
        >
          <div className="flex items-end gap-[3px] sm:gap-1" style={{ height: useCompactDots ? 28 : 36 }}>
            {dotGroups.map((group, gi) => (
              <div key={gi} className="flex flex-1 items-end gap-[2px]">
                {group.days.map((d) => {
                  const hasPosts = d.count > 0;
                  const sizePct = hasPosts ? 30 + (d.count / maxCount) * 70 : 0;
                  const restingHeight = useCompactDots ? 2 : 4;
                  return (
                    <div
                      key={d.day}
                      data-bar={`day:${d.day}`}
                      data-bar-size={hasPosts ? sizePct.toFixed(2) : restingHeight}
                      data-bar-step={sweepStep(d.index, totalDays)}
                      className={cn(
                        'flex-1 origin-bottom rounded-[6px]',
                        hasPosts
                          ? 'bg-[var(--fm-accent)] shadow-[0_0_8px_rgb(var(--fm-accent-rgb)/0.2)] dark:shadow-[0_0_10px_rgb(var(--fm-accent-rgb)/0.3)]'
                          : 'bg-black/8 dark:bg-white/10 rounded-full',
                      )}
                      style={{
                        height: hasPosts ? `${sizePct}%` : restingHeight,
                        minHeight: hasPosts ? 4 : undefined,
                        opacity: hasPosts ? undefined : 0.2,
                      }}
                      title={`${d.count} post${d.count === 1 ? '' : 's'} · ${d.day}`}
                    />
                  );
                })}
                {!useCompactDots && gi < dotGroups.length - 1 && timeframe !== '7D' && (
                  <div className="mx-[1px] h-full w-px shrink-0 bg-foreground/[0.05] dark:bg-white/[0.05]" />
                )}
              </div>
            ))}
          </div>
          <div className="mt-1.5 flex">
            {dotGroups.map((group, i) => (
              <div
                key={i}
                className="flex-1 text-center text-[10px] font-black uppercase tracking-[0.14em] text-foreground/30 dark:text-white/22 sm:text-[8px]"
              >
                {group.label}
              </div>
            ))}
          </div>
        </div>

        {/* ── Stat Chips ── */}
        <div
          className={cn(
            'mt-3 grid gap-1.5 sm:gap-2',
            statGridClass(statCount),
          )}
        >
          <AnimatePresence initial={false} mode="popLayout">
            <motion.div
              key="cadence"
              initial={false}
              exit={reduceMotion ? undefined : CHIP_EXIT}
              className={cn(CHIP_CLASS, statSpanClass(statCount, 0), 'min-h-[84px] sm:min-h-[76px]')}
            >
              <div className={cn(CHIP_LABEL, 'flex min-h-[20px] items-center justify-center gap-1')}>
                <Flame className="h-2.5 w-2.5" strokeWidth={2.8} />
                Cadence
              </div>
              <div className={CHIP_VALUE}>
                <AnimatedValue value={formatCadence(pattern.posts_per_week_current)} className="inline tabular-nums" step={0} />
                <span className="ml-0.5 text-[10px] font-black text-foreground/45 dark:text-white/40 sm:text-[12px]">/wk</span>
              </div>
            </motion.div>

            <motion.div
              key="gap"
              initial={false}
              exit={reduceMotion ? undefined : CHIP_EXIT}
              className={cn(CHIP_CLASS, statSpanClass(statCount, 1), 'min-h-[84px] sm:min-h-[76px]')}
            >
              <div className={cn(CHIP_LABEL, 'flex min-h-[20px] items-center justify-center gap-1')}>
                <Clock className="h-2.5 w-2.5" strokeWidth={2.8} />
                Avg gap
              </div>
              <AnimatedValue value={formatGapShort(pattern.usual_gap_hours)} className={cn(CHIP_VALUE, 'tabular-nums')} step={1} />
            </motion.div>

          {mediaMix.map((item, i) => {
            const isTop = item.pct === topMediaPct && mediaMix.length > 1;
            return (
              <motion.div
                layout="position"
                key={item.type}
                initial={false}
                exit={reduceMotion ? undefined : { opacity: 0 }}
                transition={REORDER_TRANSITION}
                className={cn(CHIP_CLASS, statSpanClass(statCount, i + 2), 'relative min-h-[84px] overflow-hidden sm:min-h-[76px]')}
              >
                {/* Fill bar showing share (static width; grows in via scaleX on the beat) */}
                <div
                  data-bar={`mix:${item.type}`}
                  data-bar-size={item.pct}
                  data-bar-step={4 + i}
                  data-bar-axis="x"
                  className="pointer-events-none absolute inset-y-0 left-0 origin-left rounded-[14px] bg-[var(--fm-accent)]/[0.07] dark:bg-[var(--fm-accent)]/[0.12]"
                  style={{ width: `${item.pct}%` }}
                />
                {/* Top accent bar for the dominant media type */}
                {isTop && (
                  <span className="pointer-events-none absolute inset-x-3 top-0 h-[1.5px] rounded-b-full bg-[var(--fm-accent)]/70" />
                )}
                <div
                  className={cn(
                    CHIP_LABEL,
                    'relative z-10 flex min-h-[20px] items-center justify-center gap-1',
                    isTop && 'text-[var(--fm-accent)]/80 dark:text-[var(--fm-accent)]',
                  )}
                >
                  <MediaGlyph type={item.type} strokeWidth={2.6} className="h-2.5 w-2.5" />
                  {mediaLabel(item.type)}
                </div>
                <AnimatedValue value={`${item.pct}%`} className={cn(CHIP_VALUE, 'tabular-nums relative z-10')} step={i + 2} />
              </motion.div>
            );
          })}
          </AnimatePresence>
        </div>

        {/* ── Lower Section ── */}
        <div className="relative mt-3 flex min-h-[158px] flex-1 flex-col overflow-hidden border-t border-black/6 pt-3 dark:border-white/[0.06] sm:min-h-[166px] lg:min-h-[172px]">
          <AnimatePresence initial={false}>
            {singleScope ? (
              /* POSTING FINGERPRINT — single feeder or no breakdown */
              <motion.div
                key="fingerprint"
                initial={reduceMotion ? false : PANEL_ENTRY_EXIT.initial}
                animate={PANEL_ENTRY_EXIT.animate}
                exit={reduceMotion ? undefined : PANEL_ENTRY_EXIT.exit}
                className="absolute inset-x-0 top-3 bottom-0 flex min-h-0 flex-col"
              >
                <div className="flex items-center gap-1.5">
                  <Sparkles className="h-3 w-3 text-foreground/40 dark:text-white/32" strokeWidth={2.8} />
                  <span className={SECTION_LABEL}>Posting fingerprint</span>
                </div>
                <div className="mt-2 grid flex-1 grid-cols-2 gap-1.5 sm:gap-2">
                  <AnimatePresence initial={false} mode="popLayout">
                    <FingerprintCell
                      key={`peak-day:${peakDay ?? 'none'}`}
                      icon={<CalendarDays className="h-3 w-3" strokeWidth={2.4} />}
                      label="Peak day"
                      value={peakDay ?? '—'}
                    />
                    <FingerprintCell
                      key={`typical-gap:${formatGapShort(pattern.usual_gap_hours)}`}
                      icon={<Clock className="h-3 w-3" strokeWidth={2.4} />}
                      label="Typical gap"
                      value={formatGapShort(pattern.usual_gap_hours)}
                      tabular
                    />
                    <FingerprintCell
                      key={`signature:${signatureMix?.type ?? 'none'}:${signatureMix?.pct ?? 0}`}
                      icon={<MediaGlyph type={signatureMix?.type} className="h-3 w-3" />}
                      label="Signature"
                      value={
                        signatureMix
                          ? `${mediaLabel(signatureMix.type)} · ${signatureMix.pct}%`
                          : '—'
                      }
                      accent={!!signatureMix}
                    />
                    <FingerprintCell
                      key={`days-active:${activeDays}:${windowDays}`}
                      icon={<Flame className="h-3 w-3" strokeWidth={2.4} />}
                      label="Days active"
                      value={`${activeDays} of ${windowDays}`}
                      tabular
                    />
                  </AnimatePresence>
                </div>
              </motion.div>
            ) : (
              /* TOP CONTRIBUTORS — multiple feeders */
              <motion.div
                key="contributors"
                initial={reduceMotion ? false : PANEL_ENTRY_EXIT.initial}
                animate={PANEL_ENTRY_EXIT.animate}
                exit={reduceMotion ? undefined : PANEL_ENTRY_EXIT.exit}
                className="absolute inset-x-0 top-3 bottom-0 flex min-h-0 flex-col"
              >
                <div className="flex items-center justify-between gap-2">
                  <span className={SECTION_LABEL}>Top contributors</span>
                  <span className="text-[10px] font-bold uppercase tracking-[0.14em] text-foreground/32 dark:text-white/24">
                    posts / week
                  </span>
                </div>
                <div className="mt-2 space-y-2">
                  <AnimatePresence initial={false} mode="popLayout">
                    {rows.map((row, ri) => {
                      const barPct =
                        maxFeederRate > 0
                          ? Math.max(4, (row.posts_per_week_current / maxFeederRate) * 100)
                          : 4;
                      const isDormant = row.status === 'dormant' || row.status === 'insufficient_data';
                      const feederMedia = row.dominant_media_type;
                      const showDelta =
                        row.delta_percent != null && Math.abs(row.delta_percent) >= 5 && !isDormant;

                      return (
                        <motion.div
                          layout="position"
                          key={row.feeder_id}
                          initial={false}
                          exit={reduceMotion ? undefined : { opacity: 0 }}
                          transition={REORDER_TRANSITION}
                        >
                          <div className="flex items-center justify-between gap-2">
                            <div className="flex min-w-0 items-center gap-1.5">
                              <StatusDot status={row.status} />
                              <span className="truncate text-[12px] font-black tracking-tight text-foreground dark:text-white">
                                @{row.handle}
                              </span>
                              {feederMedia && feederMedia !== 'unknown' && (
                                <span className="hidden items-center gap-0.5 text-[10px] font-bold text-foreground/45 dark:text-white/38 sm:inline-flex">
                                  <span className="text-foreground/20 dark:text-white/18">·</span>
                                  <MediaGlyph type={feederMedia} className="h-2.5 w-2.5" />
                                  {mediaLabel(feederMedia)}
                                </span>
                              )}
                            </div>
                            <div className="flex shrink-0 items-center gap-1.5">
                              <span
                                className={cn(
                                  'text-[12px] font-black tabular-nums',
                                  isDormant
                                    ? 'text-foreground/40 dark:text-white/30'
                                    : 'text-foreground/75 dark:text-white/70',
                                )}
                              >
                                <AnimatedValue
                                  value={
                                    isDormant
                                      ? row.days_since_last_post != null
                                        ? `${row.days_since_last_post}d quiet`
                                        : 'quiet'
                                      : `${formatCadence(row.posts_per_week_current)}/wk`
                                  }
                                  step={ri}
                                />
                              </span>
                              {showDelta && <DeltaPill value={row.delta_percent!} />}
                            </div>
                          </div>
                          <div
                            className={cn(
                              'relative mt-1.5 h-[6px] w-full overflow-hidden rounded-full',
                              'bg-black/[0.05] dark:bg-white/[0.06]',
                            )}
                          >
                            {/* static width; grows in via scaleX on the beat */}
                            <div
                              data-bar={`row:${row.feeder_id}`}
                              data-bar-size={isDormant ? 2 : barPct.toFixed(2)}
                              data-bar-step={3 + ri}
                              data-bar-axis="x"
                              className={cn(
                                'absolute inset-y-0 left-0 origin-left rounded-full',
                                isDormant
                                  ? 'bg-foreground/10 dark:bg-white/8'
                                  : 'bg-gradient-to-r from-[var(--fm-accent)]/85 to-[var(--fm-accent)] shadow-[0_0_10px_rgb(var(--fm-accent-rgb)/0.22)] dark:shadow-[0_0_12px_rgb(var(--fm-accent-rgb)/0.32)]',
                              )}
                              style={{ width: isDormant ? '2%' : `${barPct}%` }}
                            />
                          </div>
                        </motion.div>
                      );
                    })}
                  </AnimatePresence>
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </div>
      </div>
    </div>
  );
}

/* ── Fingerprint cell ── */

function FingerprintCell({
  icon,
  label,
  value,
  tabular,
  accent,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  tabular?: boolean;
  accent?: boolean;
}) {
  return (
    <motion.div
      initial={false}
      exit={CELL_EXIT}
      className={cn(
        'relative flex min-h-[62px] items-center gap-2 rounded-[14px] px-2.5 py-2 sm:min-h-[64px] sm:gap-2.5 sm:rounded-[14px] sm:px-2.5 sm:py-2.5 lg:min-h-[66px]',
        'bg-black/[0.02] border border-black/[0.04]',
        'dark:bg-white/[0.02] dark:border-white/[0.04]',
      )}
    >
      <span
        className={cn(
          'flex h-7 w-7 shrink-0 items-center justify-center rounded-[10px] sm:h-7 sm:w-7 sm:rounded-[10px]',
          accent
            ? 'bg-[var(--fm-accent)]/10 text-[var(--fm-accent)] dark:bg-[var(--fm-accent)]/15'
            : 'bg-black/[0.04] text-foreground/55 dark:bg-white/[0.06] dark:text-white/45',
        )}
      >
        {icon}
      </span>
      <div className="min-w-0 flex-1">
        <div className="text-[8px] font-black uppercase tracking-[0.14em] text-foreground/42 dark:text-white/34 sm:text-[8px]">
          {label}
        </div>
        <AnimatedValue
          value={value}
          className={cn(
            'mt-0.5 text-[14px] font-black leading-[1.05] tracking-[-0.04em] text-foreground dark:text-white sm:text-[16px] lg:text-[16px]',
            tabular && 'tabular-nums',
          )}
        />
      </div>
    </motion.div>
  );
}

const MemoizedFeedPostingPattern = memo(FeedPostingPattern);
MemoizedFeedPostingPattern.displayName = 'FeedPostingPattern';

export default MemoizedFeedPostingPattern;
