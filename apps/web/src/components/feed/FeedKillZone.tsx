'use client';

import {
  memo,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent,
  type PointerEvent,
  type RefObject,
} from 'react';
import { motion } from 'framer-motion';
import {
  BEAT_ARRIVE_S,
  BEAT_EASE,
  BEAT_EASE_CSS,
  BEAT_LEAVE_EASE,
  BEAT_LEAVE_S,
  BEAT_MAX_STEPS,
  beatArrive,
  beatDelay,
} from '@/lib/motion';
import { KillzoneDayPoint, KillzonePoint } from './dashboardTypes';

const DAY_ORDER = [1, 2, 3, 4, 5, 6, 0];
const DAY_LABELS = ['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'];
const ROSE = '#E11D48';
const HOUR_COUNT = 24;

// Each view keeps its own ambient glow on its own layer; the toggle crossfades their opacity (compositor-only)
// instead of tweening one radial-gradient background every frame.
const HOUR_GLOW = 'radial-gradient(ellipse 70% 70% at 50% 105%, rgba(225,29,72,0.10), transparent 72%)';
const DAY_GLOW = 'radial-gradient(ellipse 65% 75% at 50% 105%, rgba(225,29,72,0.18), transparent 68%)';
const GLOW_FADE = { duration: BEAT_ARRIVE_S, ease: BEAT_EASE };

// The hour and day views swap on the page beat: the incoming one settles on BEAT_ARRIVE, the outgoing one leaves
// faster on BEAT_LEAVE. Opacity plus one transform string, so framer hands both to the compositor.
const VIEW_ARRIVE = { duration: BEAT_ARRIVE_S, ease: BEAT_EASE, delay: beatDelay(0) };
const VIEW_LEAVE = { duration: BEAT_LEAVE_S, ease: BEAT_LEAVE_EASE };
const HOURS_SHOWN = { opacity: 1, transform: 'translateY(0px) scale(1)' };
const HOURS_HIDDEN = { opacity: 0, transform: 'translateY(-18px) scale(0.97)' };
const DAYS_SHOWN = { opacity: 1, transform: 'translateY(0px) scale(1)' };
const DAYS_HIDDEN = { opacity: 0, transform: 'translateY(24px) scale(0.98)' };
const LABEL_HIDDEN = { opacity: 0, transform: 'translateY(6px)' };
const LABEL_SHOWN = { opacity: 1, transform: 'translateY(0px)' };
const BADGE_SPRING = { type: 'spring', stiffness: 360, damping: 26 } as const;

function hourLabel(hour: number): string {
  const normalized = ((hour % 24) + 24) % 24;
  const suffix = normalized >= 12 ? 'PM' : 'AM';
  const twelve = normalized % 12 === 0 ? 12 : normalized % 12;
  return `${twelve} ${suffix}`;
}

function topLabel(percentile: number | null | undefined): string {
  return percentile == null || !Number.isFinite(percentile) ? '--' : `Top ${Math.round(percentile)}%`;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function dayPerformanceScore(row: Pick<KillzoneDayPoint, 'avg_percentile_performance'>): number {
  const percentile = row.avg_percentile_performance;
  if (percentile == null || !Number.isFinite(percentile)) return Number.NEGATIVE_INFINITY;
  return 101 - clamp(percentile, 1, 100);
}

function reducedMotionPreferred(): boolean {
  return typeof window !== 'undefined' && window.matchMedia?.('(prefers-reduced-motion: reduce)').matches === true;
}

const BAR_DEAL_ID = 'fm-bar-deal';

/**
 * Bars sit at their real size (static height). A bar whose size changes is re-dealt: it grows back up from its base
 * on the page beat with ONE transform animation (compositor-only; no height tween, no per-frame JS). The first pass
 * grows every bar when `growOnMount`, otherwise it only notes the resting sizes, so a board that mounts holding its
 * data sits still and a board that mounts empty grows in when its data lands.
 * Bars carry data-bar (key), data-bar-size (what counts as a change) and data-bar-step (their slot on the beat).
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

function buildHourModel(hours: KillzonePoint[]) {
  const byHour = new Map<number, number>();
  for (let h = 0; h < HOUR_COUNT; h++) byHour.set(h, 0);
  for (const row of hours || []) {
    const hour = Number(row.hour_ist);
    if (!Number.isFinite(hour) || hour < 0 || hour > 23) continue;
    byHour.set(hour, Number(row.post_count) || 0);
  }

  const totalPosts = Array.from(byHour.values()).reduce((s, v) => s + v, 0);
  const hasData = totalPosts > 0;

  let peakStart = 18;
  let peakCount = 0;
  if (hasData) {
    let bestCount = -1;
    for (let h = 0; h < HOUR_COUNT; h++) {
      const score = (byHour.get(h) || 0) + (byHour.get((h + 1) % 24) || 0);
      if (score > bestCount) {
        bestCount = score;
        peakStart = h;
        peakCount = score;
      }
    }
  }
  const peakEnd = (peakStart + 2) % 24;
  const arcShare = hasData ? Math.max(8, Math.round((peakCount / totalPosts) * 100)) : null;
  const maxCount = hasData ? Math.max(1, ...Array.from(byHour.values())) : 0;

  const bars = Array.from({ length: HOUR_COUNT }, (_, h) => {
    const count = byHour.get(h) || 0;
    const pct = !hasData ? 0 : count <= 0 ? 4 : (count / maxCount) * 100;
    const isKill = h >= peakStart && h < peakStart + 2;
    const isKillWrap = peakStart + 2 > 24 && h < (peakStart + 2) % 24;
    const inZone = hasData && (isKill || isKillWrap);
    const intensity = maxCount > 0 ? count / maxCount : 0;
    const fillStyle = count > 0
      ? {
          background: inZone ? ROSE : `rgb(var(--fm-accent-rgb)/${Math.min(0.72, 0.2 + intensity * 0.42)})`,
          boxShadow: inZone
            ? '0 0 12px rgb(var(--fm-accent-rgb)/0.34)'
            : `0 0 8px rgb(var(--fm-accent-rgb)/${Math.min(0.24, 0.08 + intensity * 0.12)})`,
        }
      : undefined;
    return { hour: h, count, pct, fillStyle };
  });

  return { hasData, peakStart, peakEnd, peakCount, arcShare, bars };
}

function buildDayModel(days: KillzoneDayPoint[] | undefined) {
  const dayByIndex = new Map<number, KillzoneDayPoint>();
  for (const day of days || []) {
    const index = Number(day.day_of_week);
    if (!Number.isFinite(index) || index < 0 || index > 6) continue;
    dayByIndex.set(index, day);
  }
  const dayRows = DAY_ORDER.map((day) => {
    const row = dayByIndex.get(day);
    return {
      day_of_week: day,
      day_label: row?.day_label || DAY_LABELS[day],
      post_count: Math.max(0, Number(row?.post_count) || 0),
      metric_count: Math.max(0, Number(row?.metric_count) || 0),
      avg_percentile_performance: row?.avg_percentile_performance ?? null,
    };
  });

  const hasDayData = dayRows.some((row) => row.post_count > 0);
  const bestDay = [...dayRows]
    .filter((row) => row.avg_percentile_performance != null || row.post_count > 0)
    .sort((a, b) => {
      const qualityDiff = dayPerformanceScore(b) - dayPerformanceScore(a);
      if (qualityDiff !== 0) return qualityDiff;
      return b.post_count - a.post_count;
    })[0];
  const dayTopPercentile = bestDay?.avg_percentile_performance ?? null;
  const maxDayQuality = Math.max(
    1,
    ...dayRows.map((row) => (row.avg_percentile_performance == null ? 0 : 101 - clamp(row.avg_percentile_performance, 1, 100))),
  );
  const maxDayPosts = Math.max(1, ...dayRows.map((row) => row.post_count));

  const bars = dayRows.map((row) => {
    const quality = row.avg_percentile_performance == null ? 0 : 101 - clamp(row.avg_percentile_performance, 1, 100);
    const qualityHeight = quality > 0 ? 24 + (quality / maxDayQuality) * 72 : row.post_count > 0 ? 12 : 4;
    const postWeight = row.post_count / maxDayPosts;
    const isBest = bestDay?.day_of_week === row.day_of_week && hasDayData;
    const opacity = row.avg_percentile_performance == null
      ? row.post_count > 0 ? 0.28 : 0.12
      : clamp(0.22 + (quality / 100) * 0.58 + postWeight * 0.12, 0.24, 0.92);
    return {
      ...row,
      isBest,
      height: clamp(qualityHeight, 4, 96),
      // color and depth switch statically with the data; only the bar's growth animates
      fillStyle: {
        backgroundColor: `rgba(225,29,72,${opacity})`,
        boxShadow: isBest ? '0 10px 28px rgba(225,29,72,0.24)' : '0 8px 18px rgba(225,29,72,0.08)',
      },
    };
  });

  return { bestDay, dayTopPercentile, bars };
}

function FeedKillZone({
  hours,
  days,
}: {
  hours: KillzonePoint[];
  days?: KillzoneDayPoint[];
}) {
  const [dayMode, setDayMode] = useState(false);
  // read only inside the click handler, so it never needs to re-render the card
  const lastPointerTypeRef = useRef<string>('mouse');
  const hourBarsRef = useRef<HTMLDivElement>(null);
  const dayBarsRef = useRef<HTMLDivElement>(null);

  const hourModel = useMemo(() => buildHourModel(hours), [hours]);
  const dayModel = useMemo(() => buildDayModel(days), [days]);
  useBarDeal(hourBarsRef, hourModel, false);
  useBarDeal(dayBarsRef, dayModel, false);

  const { hasData, peakStart, peakEnd, peakCount, arcShare } = hourModel;
  const { bestDay, dayTopPercentile } = dayModel;

  const primaryLabel = dayMode
    ? (bestDay?.day_label || '--')
    : hasData
      ? hourLabel(peakStart)
      : '--';
  const secondaryLabel = dayMode
    ? topLabel(dayTopPercentile)
    : hasData
      ? hourLabel(peakEnd)
      : '--';

  const handlePointerEnter = (event: PointerEvent<HTMLDivElement>) => {
    lastPointerTypeRef.current = event.pointerType;
    if (event.pointerType === 'mouse') setDayMode(true);
  };

  const handlePointerLeave = (event: PointerEvent<HTMLDivElement>) => {
    if (event.pointerType === 'mouse') setDayMode(false);
  };

  const handleClick = () => {
    if (lastPointerTypeRef.current !== 'mouse') setDayMode((value) => !value);
  };

  const handleKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key !== 'Enter' && event.key !== ' ') return;
    event.preventDefault();
    setDayMode((value) => !value);
  };

  return (
    <div
      className="fm-depth-glass group/killzone relative flex h-full w-full cursor-pointer flex-col overflow-hidden rounded-[22px] p-3 outline-none transition-transform duration-300 ease-out focus-visible:ring-2 focus-visible:ring-[var(--fm-accent)]/45 sm:p-3.5 lg:p-4"
      role="button"
      tabIndex={0}
      aria-pressed={dayMode}
      aria-label="Toggle kill zone between hourly and day performance view"
      onPointerEnter={handlePointerEnter}
      onPointerLeave={handlePointerLeave}
      onPointerDown={(event) => {
        lastPointerTypeRef.current = event.pointerType;
      }}
      onClick={handleClick}
      onKeyDown={handleKeyDown}
    >
      <div aria-hidden className="pointer-events-none absolute inset-0 opacity-70">
        <motion.div
          className="absolute inset-0"
          style={{ background: HOUR_GLOW }}
          initial={false}
          animate={{ opacity: dayMode ? 0 : 1 }}
          transition={GLOW_FADE}
        />
        <motion.div
          className="absolute inset-0"
          style={{ background: DAY_GLOW }}
          initial={false}
          animate={{ opacity: dayMode ? 1 : 0 }}
          transition={GLOW_FADE}
        />
      </div>

      <div className="relative z-10 flex h-full flex-col">
        <div className="mb-2 flex items-center justify-between">
          <span className="fm-label fm-depth-title">Kill Zone</span>
          <motion.span
            className="rounded-full border border-[var(--fm-accent-bright)]/55 bg-[var(--fm-accent)]/12 px-2 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-[var(--fm-accent-deep)] dark:border-[var(--fm-accent-bright)]/30 dark:bg-[var(--fm-accent)]/16 dark:text-white/78"
            initial={false}
            animate={{ transform: dayMode ? 'scale(1.03)' : 'scale(1)' }}
            transition={BADGE_SPRING}
          >
            {dayMode ? 'Day pulse' : 'Best window'}
          </motion.span>
        </div>

        <div className="mb-2.5 flex items-center gap-2">
          <div className="flex min-w-0 items-baseline gap-1.5">
            <motion.span
              key={primaryLabel}
              initial={LABEL_HIDDEN}
              animate={LABEL_SHOWN}
              transition={beatArrive(0)}
              className="text-[22px] font-black leading-none tracking-[-0.04em] text-foreground fm-depth-title dark:text-white sm:text-[22px]"
            >
              {primaryLabel}
            </motion.span>
            <span className="text-[10px] font-black uppercase tracking-[0.14em] text-foreground/28 dark:text-white/28">
              {dayMode ? 'at' : 'to'}
            </span>
            <motion.span
              key={secondaryLabel}
              initial={LABEL_HIDDEN}
              animate={LABEL_SHOWN}
              transition={beatArrive(1)}
              className="truncate text-[22px] font-black leading-none tracking-[-0.04em] text-foreground fm-depth-title dark:text-white sm:text-[22px]"
            >
              {secondaryLabel}
            </motion.span>
          </div>

          <div className="ml-auto flex items-center gap-1.5">
            <div className="fm-depth-chip rounded-[10px] px-2 py-1 text-right">
              <div className="text-[8px] font-black uppercase tracking-[0.14em] text-foreground/34 dark:text-white/30">
                {dayMode ? 'Best' : 'Share'}
              </div>
              <div className="text-[14px] font-black leading-none text-[var(--fm-accent)] fm-depth-title dark:text-[var(--fm-accent-bright)]">
                {dayMode ? topLabel(dayTopPercentile) : arcShare == null ? '--' : `${arcShare}%`}
              </div>
            </div>
            <div className="fm-depth-chip rounded-[10px] px-2 py-1 text-right">
              <div className="text-[8px] font-black uppercase tracking-[0.14em] text-foreground/34 dark:text-white/30">Posts</div>
              <div className="text-[14px] font-black leading-none text-foreground fm-depth-title dark:text-white">
                {dayMode ? Math.max(0, bestDay?.post_count || 0) : Math.max(0, peakCount)}
              </div>
            </div>
          </div>
        </div>

        <div className="relative min-h-[80px] flex-1 overflow-hidden">
          <motion.div
            ref={hourBarsRef}
            className="absolute inset-0 flex items-end gap-[2px]"
            initial={false}
            animate={dayMode ? HOURS_HIDDEN : HOURS_SHOWN}
            transition={dayMode ? VIEW_LEAVE : VIEW_ARRIVE}
          >
            {hourModel.bars.map((bar) => (
              <div key={bar.hour} className="relative flex h-full flex-1 flex-col items-center justify-end">
                <div
                  data-bar={bar.hour}
                  data-bar-size={bar.pct.toFixed(2)}
                  data-bar-step={(bar.hour / (HOUR_COUNT - 1)) * BEAT_MAX_STEPS}
                  className={`w-full origin-bottom rounded-t-[3px] ${
                    bar.count > 0 ? '' : 'bg-black/10 dark:bg-white/12'
                  }`}
                  style={{ height: `${bar.pct}%`, ...bar.fillStyle }}
                />
                {bar.hour % 6 === 0 && (
                  <div className="mt-1 text-[8px] font-black text-foreground/28 dark:text-white/24">
                    {bar.hour === 0 ? '12A' : bar.hour === 6 ? '6A' : bar.hour === 12 ? '12P' : '6P'}
                  </div>
                )}
              </div>
            ))}
          </motion.div>

          <motion.div
            ref={dayBarsRef}
            className="absolute inset-0 flex items-end gap-1.5"
            initial={false}
            animate={dayMode ? DAYS_SHOWN : DAYS_HIDDEN}
            transition={dayMode ? VIEW_ARRIVE : VIEW_LEAVE}
          >
            {dayModel.bars.map((bar, index) => (
              <div key={bar.day_of_week} className="flex h-full min-w-0 flex-1 flex-col items-center justify-end">
                <div
                  data-bar={bar.day_of_week}
                  data-bar-size={bar.height.toFixed(2)}
                  data-bar-step={index}
                  className="relative w-full origin-bottom overflow-hidden rounded-t-[6px] border border-[var(--fm-accent-bright)]/25"
                  style={{ height: `${bar.height}%`, ...bar.fillStyle }}
                >
                  <div className="absolute inset-x-0 top-0 h-1/3 bg-white/28 dark:bg-white/16" />
                  {bar.isBest && <div className="absolute inset-x-1 top-1 h-1 rounded-full bg-white/55" />}
                </div>
                <div className="mt-1 flex flex-col items-center leading-none">
                  <span className={`text-[8px] font-black tracking-[0.14em] ${bar.isBest ? 'text-[var(--fm-accent)] dark:text-[var(--fm-accent-bright)]' : 'text-foreground/30 dark:text-white/28'}`}>
                    {bar.day_label.slice(0, 3)}
                  </span>
                  <span className="mt-0.5 text-[8px] font-black text-foreground/24 dark:text-white/22">
                    {bar.metric_count > 0 ? `${Math.round(bar.avg_percentile_performance || 0)}%` : `${bar.post_count}`}
                  </span>
                </div>
              </div>
            ))}
          </motion.div>
        </div>
      </div>
    </div>
  );
}

const MemoizedFeedKillZone = memo(FeedKillZone);
MemoizedFeedKillZone.displayName = 'FeedKillZone';

export default MemoizedFeedKillZone;
