'use client';

import { useState, type KeyboardEvent, type PointerEvent } from 'react';
import { motion } from 'framer-motion';
import { KillzoneDayPoint, KillzonePoint } from './dashboardTypes';

const DAY_ORDER = [1, 2, 3, 4, 5, 6, 0];
const ROSE = '#E11D48';

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

export default function FeedKillZone({
  hours,
  days,
}: {
  hours: KillzonePoint[];
  days?: KillzoneDayPoint[];
}) {
  const [dayMode, setDayMode] = useState(false);
  const [lastPointerType, setLastPointerType] = useState<string>('mouse');

  const byHour = new Map<number, number>();
  for (let h = 0; h < 24; h++) byHour.set(h, 0);
  for (const row of hours || []) {
    const hour = Number(row.hour_ist);
    if (!Number.isFinite(hour) || hour < 0 || hour > 23) continue;
    byHour.set(hour, Number(row.post_count) || 0);
  }

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
      day_label: row?.day_label || ['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'][day],
      post_count: Math.max(0, Number(row?.post_count) || 0),
      metric_count: Math.max(0, Number(row?.metric_count) || 0),
      avg_percentile_performance: row?.avg_percentile_performance ?? null,
    };
  });

  const totalPosts = Array.from(byHour.values()).reduce((s, v) => s + v, 0);
  const hasData = totalPosts > 0;
  const hasDayData = dayRows.some((row) => row.post_count > 0);

  let peakStart = 18;
  let peakCount = 0;
  if (hasData) {
    let bestCount = -1;
    for (let h = 0; h < 24; h++) {
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

  const hourKeys = Array.from({ length: 24 }, (_, i) => i);
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
    setLastPointerType(event.pointerType);
    if (event.pointerType === 'mouse') setDayMode(true);
  };

  const handlePointerLeave = (event: PointerEvent<HTMLDivElement>) => {
    if (event.pointerType === 'mouse') setDayMode(false);
  };

  const handleClick = () => {
    if (lastPointerType !== 'mouse') setDayMode((value) => !value);
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
      onPointerDown={(event) => setLastPointerType(event.pointerType)}
      onClick={handleClick}
      onKeyDown={handleKeyDown}
    >
      <motion.div
        aria-hidden
        className="pointer-events-none absolute inset-0 opacity-70"
        animate={{
          background: dayMode
            ? 'radial-gradient(ellipse 65% 75% at 50% 105%, rgba(225,29,72,0.18), transparent 68%)'
            : 'radial-gradient(ellipse 70% 70% at 50% 105%, rgba(225,29,72,0.10), transparent 72%)',
        }}
        transition={{ duration: 0.45, ease: [0.22, 1, 0.36, 1] }}
      />

      <div className="relative z-10 flex h-full flex-col">
        <div className="mb-2 flex items-center justify-between">
          <span className="fm-label fm-depth-title">Kill Zone</span>
          <motion.span
            className="rounded-full border border-[var(--fm-accent-bright)]/55 bg-[var(--fm-accent)]/12 px-2 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-[var(--fm-accent-deep)] dark:border-[var(--fm-accent-bright)]/30 dark:bg-[var(--fm-accent)]/16 dark:text-white/78"
            animate={{ scale: dayMode ? 1.03 : 1 }}
            transition={{ type: 'spring', stiffness: 360, damping: 26 }}
          >
            {dayMode ? 'Day pulse' : 'Best window'}
          </motion.span>
        </div>

        <div className="mb-2.5 flex items-center gap-2">
          <div className="flex min-w-0 items-baseline gap-1.5">
            <motion.span
              key={primaryLabel}
              initial={{ y: 6, opacity: 0 }}
              animate={{ y: 0, opacity: 1 }}
              transition={{ type: 'spring', stiffness: 420, damping: 32 }}
              className="text-[22px] font-black leading-none tracking-[-0.04em] text-foreground fm-depth-title dark:text-white sm:text-[22px]"
            >
              {primaryLabel}
            </motion.span>
            <span className="text-[10px] font-black uppercase tracking-[0.14em] text-foreground/28 dark:text-white/28">
              {dayMode ? 'at' : 'to'}
            </span>
            <motion.span
              key={secondaryLabel}
              initial={{ y: 6, opacity: 0 }}
              animate={{ y: 0, opacity: 1 }}
              transition={{ type: 'spring', stiffness: 420, damping: 32 }}
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
            className="absolute inset-0 flex items-end gap-[2px]"
            animate={{
              y: dayMode ? -18 : 0,
              opacity: dayMode ? 0 : 1,
              scale: dayMode ? 0.97 : 1,
            }}
            transition={{ duration: 0.48, ease: [0.22, 1, 0.36, 1] }}
          >
            {hourKeys.map((h) => {
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

              return (
                <div key={h} className="relative flex h-full flex-1 flex-col items-center justify-end">
                  <div
                    className={`w-full rounded-t-[3px] transition-all duration-500 ease-out ${
                      count > 0 ? '' : 'bg-black/10 dark:bg-white/12'
                    }`}
                    style={{ height: `${pct}%`, ...fillStyle }}
                  />
                  {h % 6 === 0 && (
                    <div className="mt-1 text-[8px] font-black text-foreground/28 dark:text-white/24">
                      {h === 0 ? '12A' : h === 6 ? '6A' : h === 12 ? '12P' : '6P'}
                    </div>
                  )}
                </div>
              );
            })}
          </motion.div>

          <motion.div
            className="absolute inset-0 flex items-end gap-1.5"
            animate={{
              y: dayMode ? 0 : 24,
              opacity: dayMode ? 1 : 0,
              scale: dayMode ? 1 : 0.98,
            }}
            transition={{ duration: 0.5, ease: [0.22, 1, 0.36, 1] }}
          >
            {dayRows.map((row, index) => {
              const quality = row.avg_percentile_performance == null ? 0 : 101 - clamp(row.avg_percentile_performance, 1, 100);
              const qualityHeight = quality > 0 ? 24 + (quality / maxDayQuality) * 72 : row.post_count > 0 ? 12 : 4;
              const postWeight = row.post_count / maxDayPosts;
              const isBest = bestDay?.day_of_week === row.day_of_week && hasDayData;
              const opacity = row.avg_percentile_performance == null
                ? row.post_count > 0 ? 0.28 : 0.12
                : clamp(0.22 + (quality / 100) * 0.58 + postWeight * 0.12, 0.24, 0.92);

              return (
                <div key={row.day_of_week} className="flex h-full min-w-0 flex-1 flex-col items-center justify-end">
                  <motion.div
                    className="relative w-full overflow-hidden rounded-t-[6px] border border-[var(--fm-accent-bright)]/25"
                    initial={false}
                    animate={{
                      height: `${clamp(qualityHeight, 4, 96)}%`,
                      backgroundColor: `rgba(225,29,72,${opacity})`,
                      boxShadow: isBest ? '0 10px 28px rgba(225,29,72,0.24)' : '0 8px 18px rgba(225,29,72,0.08)',
                    }}
                    transition={{
                      height: { duration: 0.55, delay: dayMode ? index * 0.025 : 0, ease: [0.22, 1, 0.36, 1] },
                      backgroundColor: { duration: 0.45 },
                      boxShadow: { duration: 0.45 },
                    }}
                  >
                    <div className="absolute inset-x-0 top-0 h-1/3 bg-white/28 dark:bg-white/16" />
                    {isBest && <div className="absolute inset-x-1 top-1 h-1 rounded-full bg-white/55" />}
                  </motion.div>
                  <div className="mt-1 flex flex-col items-center leading-none">
                    <span className={`text-[8px] font-black tracking-[0.14em] ${isBest ? 'text-[var(--fm-accent)] dark:text-[var(--fm-accent-bright)]' : 'text-foreground/30 dark:text-white/28'}`}>
                      {row.day_label.slice(0, 3)}
                    </span>
                    <span className="mt-0.5 text-[8px] font-black text-foreground/24 dark:text-white/22">
                      {row.metric_count > 0 ? `${Math.round(row.avg_percentile_performance || 0)}%` : `${row.post_count}`}
                    </span>
                  </div>
                </div>
              );
            })}
          </motion.div>
        </div>
      </div>
    </div>
  );
}
