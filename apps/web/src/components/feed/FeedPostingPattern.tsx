'use client';

import { useMemo } from 'react';
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
const SPRING = { type: 'spring' as const, stiffness: 300, damping: 26, mass: 0.8 };
const LAYOUT_SPRING = { type: 'spring' as const, stiffness: 280, damping: 30, mass: 0.86 };
const ENTRY_EXIT = {
  initial: { opacity: 0, y: 10, scale: 0.98 },
  animate: { opacity: 1, y: 0, scale: 1 },
  exit: { opacity: 0, y: -8, scale: 0.98 },
} as const;

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

type DotGroup = { label: string; days: Array<{ day: string; count: number }> };

function groupDots(rhythmDays: PostingPatternPayload['rhythm_days'], tf: Timeframe): DotGroup[] {
  const days = rhythmDays.map((d) => ({ day: d.day_ist, count: Math.max(0, d.post_count) }));

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
  'rounded-[16px] px-3 py-3 text-center sm:rounded-[14px] sm:px-3 sm:py-2.5',
  'bg-gradient-to-b from-white/70 to-white/40 border border-white/70',
  'shadow-[inset_0_1px_0_rgba(255,255,255,0.8),0_2px_8px_rgba(15,23,42,0.04)]',
  'dark:from-white/[0.05] dark:to-white/[0.02] dark:border-white/[0.06]',
  'dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04),0_4px_12px_rgba(0,0,0,0.2)]',
);

const CHIP_LABEL = 'text-[10px] font-black uppercase leading-[1.02] tracking-[0.13em] text-foreground/45 dark:text-white/38 sm:text-[9px]';
const CHIP_VALUE = 'mt-1.5 text-[17px] font-black leading-none tracking-[-0.03em] text-foreground dark:text-white sm:mt-1 sm:text-[17px] lg:text-[18px]';
const SECTION_LABEL = 'text-[10px] font-black uppercase tracking-[0.14em] text-foreground/38 dark:text-white/32 sm:text-[9px]';

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

function AnimatedValue({
  value,
  className,
}: {
  value: string;
  className?: string;
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
          initial={{ opacity: 0, y: 10, scale: 0.98 }}
          animate={{ opacity: 1, y: 0, scale: 1 }}
          exit={{ opacity: 0, y: -8, scale: 0.98 }}
          transition={SPRING}
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
        'inline-flex items-center gap-0.5 rounded-full px-1.5 py-[3px] text-[9px] font-black tabular-nums leading-none',
        positive
          ? 'bg-[#E11D48]/10 text-[#E11D48] dark:bg-[#E11D48]/15'
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
            ? 'bg-[#E11D48] shadow-[0_0_6px_rgba(225,29,72,0.45)]'
            : 'bg-foreground/45 dark:bg-white/45',
      )}
    />
  );
}

/* ── Component ── */

export default function FeedPostingPattern({ pattern, timeframe = '7D' }: Props) {
  const prefersReducedMotion = useReducedMotion();
  const reduceMotion = Boolean(prefersReducedMotion);
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
          <div className="mt-5 text-[24px] font-black leading-tight text-foreground dark:text-white sm:text-[28px]">
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
    <motion.div layout className="fm-depth-glass relative flex h-full w-full flex-col overflow-hidden rounded-[22px] p-3 sm:p-3.5 lg:p-4">
      <motion.div layout className="relative z-10 flex h-full min-h-0 flex-col">

        {/* ── Label ── */}
        <span className="fm-label fm-depth-title">Posting Pulse</span>

        {/* ── Conversational Headline ── */}
        <AnimatePresence initial={false} mode="popLayout">
          <motion.p
            key={headline}
            {...(reduceMotion ? {} : ENTRY_EXIT)}
            transition={SPRING}
            className="mt-2 text-[clamp(15px,3vw,20px)] font-black leading-[1.25] tracking-[-0.02em] text-foreground dark:text-white"
          >
            {headline}
          </motion.p>
        </AnimatePresence>

        {/* ── Activity Strip ── */}
        <motion.div
          layout
          className={cn(
            'mt-3.5 rounded-[14px] px-2.5 py-2.5 sm:px-3',
            'bg-black/[0.03] border border-black/[0.04]',
            'shadow-[inset_0_2px_4px_rgba(0,0,0,0.04)]',
            'dark:bg-white/[0.03] dark:border-white/[0.04]',
            'dark:shadow-[inset_0_2px_6px_rgba(0,0,0,0.3)]',
          )}
          transition={{ layout: LAYOUT_SPRING }}
        >
          <div className="flex items-end gap-[3px] sm:gap-1" style={{ height: useCompactDots ? 28 : 36 }}>
            {dotGroups.map((group, gi) => (
              <div key={gi} className="flex flex-1 items-end gap-[2px]">
                {group.days.map((d, di) => {
                  const hasPosts = d.count > 0;
                  const sizePct = hasPosts ? 30 + (d.count / maxCount) * 70 : 0;
                  return (
                    <motion.div
                      key={d.day}
                      initial={{ height: 0, opacity: 0 }}
                      animate={{
                        height: hasPosts ? `${sizePct}%` : useCompactDots ? 2 : 4,
                        opacity: hasPosts ? 1 : 0.2,
                      }}
                      transition={{ ...SPRING, delay: Math.min((gi * group.days.length + di) * 0.015, 0.3) }}
                      className={cn(
                        'flex-1 rounded-[4px]',
                        hasPosts
                          ? 'bg-[#E11D48] shadow-[0_0_8px_rgba(225,29,72,0.2)] dark:shadow-[0_0_10px_rgba(225,29,72,0.3)]'
                          : 'bg-black/8 dark:bg-white/10 rounded-full',
                      )}
                      style={{ minHeight: hasPosts ? 4 : undefined }}
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
                className="flex-1 text-center text-[9px] font-black uppercase tracking-[0.14em] text-foreground/30 dark:text-white/22 sm:text-[8px]"
              >
                {group.label}
              </div>
            ))}
          </div>
        </motion.div>

        {/* ── Stat Chips ── */}
        <motion.div
          layout
          className={cn(
            'mt-3 grid gap-1.5 sm:gap-2',
            statGridClass(statCount),
          )}
          transition={{ layout: LAYOUT_SPRING }}
        >
          <AnimatePresence initial={false} mode="popLayout">
            <motion.div
              key="cadence"
              layout
              initial={false}
              exit={reduceMotion ? undefined : { opacity: 0, scale: 0.96, y: 10 }}
              transition={{
                layout: LAYOUT_SPRING,
                opacity: { duration: 0.18, ease: EASE },
                scale: { duration: 0.22, ease: EASE },
                y: { duration: 0.22, ease: EASE },
              }}
              className={cn(CHIP_CLASS, statSpanClass(statCount, 0), 'min-h-[84px] sm:min-h-[76px]')}
            >
              <div className={cn(CHIP_LABEL, 'flex min-h-[20px] items-center justify-center gap-1')}>
                <Flame className="h-2.5 w-2.5" strokeWidth={2.8} />
                Cadence
              </div>
              <div className={CHIP_VALUE}>
                <AnimatedValue value={formatCadence(pattern.posts_per_week_current)} className="inline tabular-nums" />
                <span className="ml-0.5 text-[10px] font-black text-foreground/45 dark:text-white/40 sm:text-[11px]">/wk</span>
              </div>
            </motion.div>

            <motion.div
              key="gap"
              layout
              initial={false}
              exit={reduceMotion ? undefined : { opacity: 0, scale: 0.96, y: 10 }}
              transition={{
                layout: LAYOUT_SPRING,
                opacity: { duration: 0.18, ease: EASE },
                scale: { duration: 0.22, ease: EASE },
                y: { duration: 0.22, ease: EASE },
              }}
              className={cn(CHIP_CLASS, statSpanClass(statCount, 1), 'min-h-[84px] sm:min-h-[76px]')}
            >
              <div className={cn(CHIP_LABEL, 'flex min-h-[20px] items-center justify-center gap-1')}>
                <Clock className="h-2.5 w-2.5" strokeWidth={2.8} />
                Avg gap
              </div>
              <AnimatedValue value={formatGapShort(pattern.usual_gap_hours)} className={cn(CHIP_VALUE, 'tabular-nums')} />
            </motion.div>

          {mediaMix.map((item, i) => {
            const isTop = item.pct === topMediaPct && mediaMix.length > 1;
            return (
              <motion.div
                layout
                key={item.type}
                initial={false}
                exit={reduceMotion ? undefined : { opacity: 0, scale: 0.96, y: 10 }}
                transition={{
                  layout: LAYOUT_SPRING,
                  opacity: { duration: 0.18, ease: EASE },
                  scale: { duration: 0.22, ease: EASE },
                  y: { duration: 0.22, ease: EASE },
                }}
                className={cn(CHIP_CLASS, statSpanClass(statCount, i + 2), 'relative min-h-[84px] overflow-hidden sm:min-h-[76px]')}
              >
                {/* Fill bar showing share */}
                <motion.div
                  initial={{ width: 0 }}
                  animate={{ width: `${item.pct}%` }}
                  transition={{ duration: 0.6, ease: EASE, delay: 0.35 + i * 0.05 }}
                  className="pointer-events-none absolute inset-y-0 left-0 rounded-[14px] bg-[#E11D48]/[0.07] dark:bg-[#E11D48]/[0.12]"
                />
                {/* Top accent bar for the dominant media type */}
                {isTop && (
                  <span className="pointer-events-none absolute inset-x-3 top-0 h-[1.5px] rounded-b-full bg-[#E11D48]/70" />
                )}
                <div
                  className={cn(
                    CHIP_LABEL,
                    'relative z-10 flex min-h-[20px] items-center justify-center gap-1',
                    isTop && 'text-[#E11D48]/80 dark:text-[#E11D48]',
                  )}
                >
                  <MediaGlyph type={item.type} strokeWidth={2.6} className="h-2.5 w-2.5" />
                  {mediaLabel(item.type)}
                </div>
                <AnimatedValue value={`${item.pct}%`} className={cn(CHIP_VALUE, 'tabular-nums relative z-10')} />
              </motion.div>
            );
          })}
          </AnimatePresence>
        </motion.div>

        {/* ── Lower Section ── */}
        <motion.div
          layout
          className="mt-3 flex min-h-0 flex-1 flex-col overflow-hidden border-t border-black/6 pt-3 dark:border-white/[0.06]"
          transition={{ layout: LAYOUT_SPRING }}
        >
          <AnimatePresence initial={false} mode="popLayout">
            {singleScope ? (
              /* POSTING FINGERPRINT — single feeder or no breakdown */
              <motion.div
                key="fingerprint"
                layout
                {...(reduceMotion ? {} : ENTRY_EXIT)}
                transition={{ ...SPRING, layout: LAYOUT_SPRING }}
                className="flex h-full min-h-0 flex-col"
              >
                <div className="flex items-center gap-1.5">
                  <Sparkles className="h-3 w-3 text-foreground/40 dark:text-white/32" strokeWidth={2.8} />
                  <span className={SECTION_LABEL}>Posting fingerprint</span>
                </div>
                <motion.div layout className="mt-2 grid flex-1 grid-cols-2 gap-1.5 sm:gap-2" transition={{ layout: LAYOUT_SPRING }}>
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
                </motion.div>
              </motion.div>
            ) : (
              /* TOP CONTRIBUTORS — multiple feeders */
              <motion.div
                key="contributors"
                layout
                {...(reduceMotion ? {} : ENTRY_EXIT)}
                transition={{ ...SPRING, layout: LAYOUT_SPRING }}
                className="flex h-full min-h-0 flex-col"
              >
                <div className="flex items-center justify-between gap-2">
                  <span className={SECTION_LABEL}>Top contributors</span>
                  <span className="text-[9px] font-bold uppercase tracking-[0.12em] text-foreground/32 dark:text-white/24">
                    posts / week
                  </span>
                </div>
                <motion.div layout className="mt-2 space-y-2" transition={{ layout: LAYOUT_SPRING }}>
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
                          layout
                          key={row.feeder_id}
                          initial={false}
                          exit={reduceMotion ? undefined : { opacity: 0, x: -10, scale: 0.985 }}
                          transition={{
                            layout: LAYOUT_SPRING,
                            opacity: { duration: 0.18, ease: EASE },
                            x: { duration: 0.22, ease: EASE },
                            scale: { duration: 0.22, ease: EASE },
                          }}
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
                                  'text-[11px] font-black tabular-nums',
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
                            <motion.div
                              initial={{ width: 0 }}
                              animate={{ width: isDormant ? '2%' : `${barPct}%` }}
                              transition={{ duration: 0.6, ease: EASE, delay: 0.28 + ri * 0.06 }}
                              className={cn(
                                'absolute inset-y-0 left-0 rounded-full',
                                isDormant
                                  ? 'bg-foreground/10 dark:bg-white/8'
                                  : 'bg-gradient-to-r from-[#E11D48]/85 to-[#E11D48] shadow-[0_0_10px_rgba(225,29,72,0.22)] dark:shadow-[0_0_12px_rgba(225,29,72,0.32)]',
                              )}
                            />
                          </div>
                        </motion.div>
                      );
                    })}
                  </AnimatePresence>
                </motion.div>
              </motion.div>
            )}
          </AnimatePresence>
        </motion.div>
      </motion.div>
    </motion.div>
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
      layout
      initial={false}
      exit={{ opacity: 0, scale: 0.97, y: 10 }}
      transition={{
        layout: LAYOUT_SPRING,
        opacity: { duration: 0.18, ease: EASE },
        scale: { duration: 0.22, ease: EASE },
        y: { duration: 0.22, ease: EASE },
      }}
      className={cn(
        'relative flex min-h-[82px] items-center gap-3 rounded-[14px] px-3 py-3 sm:min-h-[78px] sm:gap-2.5 sm:rounded-[12px] sm:px-2.5 sm:py-2.5',
        'bg-black/[0.02] border border-black/[0.04]',
        'dark:bg-white/[0.02] dark:border-white/[0.04]',
      )}
    >
      <span
        className={cn(
          'flex h-8 w-8 shrink-0 items-center justify-center rounded-[10px] sm:h-7 sm:w-7 sm:rounded-[9px]',
          accent
            ? 'bg-[#E11D48]/10 text-[#E11D48] dark:bg-[#E11D48]/15'
            : 'bg-black/[0.04] text-foreground/55 dark:bg-white/[0.06] dark:text-white/45',
        )}
      >
        {icon}
      </span>
      <div className="min-w-0 flex-1">
        <div className="text-[9px] font-black uppercase tracking-[0.14em] text-foreground/42 dark:text-white/34 sm:text-[8px]">
          {label}
        </div>
        <AnimatedValue
          value={value}
          className={cn(
            'mt-1 text-[17px] font-black leading-[1.02] tracking-[-0.025em] text-foreground dark:text-white sm:text-[15px]',
            tabular && 'tabular-nums',
          )}
        />
      </div>
    </motion.div>
  );
}
