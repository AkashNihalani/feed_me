'use client';

import { useEffect, useId, useMemo, useRef, useState } from 'react';
import { animate, motion, useMotionValue, useSpring } from 'framer-motion';
import { AscentPoint, Timeframe } from './dashboardTypes';

type ChartPoint = {
  label: string;
  fullLabel: string;
  followers: number;
  x: number;
  y: number;
  deltaFromPrevious: number;
  deltaPercentFromPrevious: number;
  previousLabel: string | null;
};

const ASCENT_ACCENT = '#E11D48';
const ASCENT_ACCENT_RGB = '225 29 72';
const TREND_PATHS = {
  flat: 'M 13 32 L 51 32 M 51 32 L 51 32 M 51 32 L 51 32',
  rise: 'M 32 53 L 32 12 M 32 12 L 21 23 M 32 12 L 43 23',
  drop: 'M 32 11 L 32 52 M 32 52 L 21 41 M 32 52 L 43 41',
} as const;

type TrendDirection = keyof typeof TREND_PATHS;

function formatSignedFollowers(value: number): string {
  if (!Number.isFinite(value) || Math.abs(value) < 0.5) return '0';
  const rounded = Math.round(value);
  const formatted = new Intl.NumberFormat('en-US').format(Math.abs(rounded));
  return `${rounded > 0 ? '+' : '-'}${formatted}`;
}

function signedPercent(value: number): string {
  if (!Number.isFinite(value) || Math.abs(value) < 0.05) return '0.0%';
  return `${value > 0 ? '+' : '-'}${Math.abs(value).toFixed(1)}%`;
}

function formatFollowers(value: number): string {
  return new Intl.NumberFormat('en-US').format(Math.max(0, Math.round(value)));
}

function toLabel(snapshotDate: string): string {
  const date = new Date(`${snapshotDate}T00:00:00`);
  if (Number.isNaN(date.getTime())) return snapshotDate.toUpperCase();
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }).toUpperCase();
}

function shouldShowAxisLabel(index: number, total: number): boolean {
  if (total <= 1) return true;
  if (index === 0 || index === total - 1) return true;
  const targetLabels = total <= 7 ? total : total <= 30 ? 5 : 6;
  const step = Math.max(1, Math.ceil((total - 1) / Math.max(1, targetLabels - 1)));
  return index % step === 0;
}

function TrendGlyph({ direction }: { direction: TrendDirection }) {
  const isFlat = direction === 'flat';
  const path = TREND_PATHS[direction];

  return (
    <motion.svg
      aria-hidden="true"
      viewBox="0 0 64 64"
      className="h-10 w-12 overflow-hidden sm:h-11 sm:w-14 lg:h-12 lg:w-16"
      initial={false}
      animate={{ scale: isFlat ? 0.92 : 1 }}
      transition={{ type: 'spring', stiffness: 240, damping: 22, mass: 0.74 }}
    >
      <motion.g initial={false} animate={{ y: 0 }} transition={{ type: 'spring', stiffness: 260, damping: 24, mass: 0.74 }}>
        <motion.path
          fill="none"
          stroke="currentColor"
          strokeWidth="5.2"
          strokeLinecap="round"
          strokeLinejoin="round"
          vectorEffect="non-scaling-stroke"
          initial={{ d: TREND_PATHS.flat, opacity: 1 }}
          animate={{ d: path, opacity: 1 }}
          transition={{ type: 'spring', stiffness: 220, damping: 24, mass: 0.8 }}
        />
      </motion.g>
    </motion.svg>
  );
}

export default function FeedAscentChart({ timeframe, series }: { timeframe: Timeframe; series: AscentPoint[] }) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const gradientId = useId().replace(/[:]/g, '');

  const [hoverIndex, setHoverIndex] = useState<number | null>(null);
  const [selectedIndex, setSelectedIndex] = useState<number | null>(null);
  const [touchScrubbing, setTouchScrubbing] = useState(false);
  const [animatedDeltaCount, setAnimatedDeltaCount] = useState(0);
  const [animatedDeltaPercent, setAnimatedDeltaPercent] = useState(0);

  const deltaCountValue = useMotionValue(0);
  const deltaCountSpring = useSpring(deltaCountValue, { stiffness: 220, damping: 28, mass: 0.78 });
  const deltaPercentValue = useMotionValue(0);
  const deltaPercentSpring = useSpring(deltaPercentValue, { stiffness: 210, damping: 26, mass: 0.78 });

  const data = useMemo(() => {
    if (!Array.isArray(series) || series.length === 0) {
      return [{ snapshot_date_ist: timeframe, follower_count: 0 }];
    }
    return series;
  }, [series, timeframe]);

  const chart = useMemo(() => {
    const counts = data.map((point) => Number(point.follower_count) || 0);
    const min = Math.min(...counts);
    const max = Math.max(...counts);
    const range = Math.max(1, max - min);

    const points: ChartPoint[] = data.map((point, index) => {
      const followers = Number(point.follower_count) || 0;
      const previous = index > 0 ? Number(data[index - 1]?.follower_count) || 0 : followers;
      const deltaFromPrevious = index > 0 ? followers - previous : 0;
      const deltaPercentFromPrevious = index > 0 && previous > 0
        ? ((followers - previous) / previous) * 100
        : 0;

      return {
        label: toLabel(point.snapshot_date_ist),
        fullLabel: point.snapshot_date_ist,
        followers,
        x: (index / Math.max(1, data.length - 1)) * 100,
        y: 34 - ((followers - min) / range) * 26,
        deltaFromPrevious,
        deltaPercentFromPrevious,
        previousLabel: index > 0 ? toLabel(data[index - 1]!.snapshot_date_ist) : null,
      };
    });

    const line = points.map((point, index) => `${index === 0 ? 'M' : 'L'} ${point.x} ${point.y}`).join(' ');
    const area = `${line} L 100 38 L 0 38 Z`;
    const firstFollowers = points[0]?.followers ?? 0;
    const lastFollowers = points[points.length - 1]?.followers ?? 0;
    const overallDeltaCount = lastFollowers - firstFollowers;
    const overallDeltaPercent = firstFollowers > 0 ? ((lastFollowers - firstFollowers) / firstFollowers) * 100 : 0;

    return {
      points,
      line,
      area,
      latestFollowers: lastFollowers,
      overallDeltaCount,
      overallDeltaPercent,
    };
  }, [data]);

  const displayIndex = hoverIndex ?? selectedIndex;
  const displayPoint = displayIndex == null ? null : chart.points[displayIndex] ?? null;
  const displayDeltaCount = displayPoint ? displayPoint.deltaFromPrevious : chart.overallDeltaCount;
  const displayDeltaPercent = displayPoint ? displayPoint.deltaPercentFromPrevious : chart.overallDeltaPercent;
  const displayFollowers = displayPoint?.followers ?? chart.latestFollowers;
  const direction: TrendDirection = displayDeltaCount > 0 ? 'rise' : displayDeltaCount < 0 ? 'drop' : 'flat';
  const directionLabel = direction === 'rise' ? 'Rise' : direction === 'drop' ? 'Drop' : 'Flat';
  const contextLine = displayPoint
    ? displayPoint.previousLabel
      ? `${displayPoint.label} vs ${displayPoint.previousLabel}`
      : `${displayPoint.label} starting point`
    : `Net over ${timeframe}`;
  const followersLabel = displayPoint ? `Followers on ${displayPoint.label}` : 'Current total';

  useEffect(() => {
    const stopCount = deltaCountSpring.on('change', (value) => setAnimatedDeltaCount(value));
    const stopPercent = deltaPercentSpring.on('change', (value) => setAnimatedDeltaPercent(value));
    const countControls = animate(deltaCountValue, displayDeltaCount, { duration: 0.34, ease: 'easeOut' });
    const percentControls = animate(deltaPercentValue, displayDeltaPercent, { duration: 0.34, ease: 'easeOut' });
    return () => {
      stopCount();
      stopPercent();
      countControls.stop();
      percentControls.stop();
    };
  }, [
    deltaCountSpring,
    deltaCountValue,
    deltaPercentSpring,
    deltaPercentValue,
    displayDeltaCount,
    displayDeltaPercent,
  ]);

  const resolveIndex = (clientX: number): number | null => {
    const rect = rootRef.current?.getBoundingClientRect();
    if (!rect) return null;
    const x = Math.max(0, Math.min(rect.width, clientX - rect.left));
    const ratio = rect.width <= 0 ? 0 : x / rect.width;
    return Math.max(0, Math.min(chart.points.length - 1, Math.round(ratio * (chart.points.length - 1))));
  };

  return (
    <div className="fm-depth-glass relative flex h-full w-full flex-col overflow-hidden rounded-[22px] p-3 sm:p-3.5 lg:p-4">
      <div className="relative z-10 flex flex-1 flex-col">
        <div className="flex flex-col gap-2.5 sm:gap-3 lg:flex-row lg:items-end lg:justify-between">
          <div className="min-w-0 flex-1">
            <div className="fm-label fm-depth-title">Follower Growth</div>
            <div className="mt-1.5 flex flex-wrap items-end gap-x-3 gap-y-1.5">
              <div className="text-[clamp(26px,5.5vw,42px)] font-black leading-[0.86] tracking-[-0.06em] text-black dark:text-white">
                {formatSignedFollowers(animatedDeltaCount)}
              </div>
              <div className="pb-0.5 text-[9px] font-black uppercase tracking-[0.14em] text-foreground/42">
                {contextLine}
              </div>
            </div>
          </div>

          <div className="grid grid-cols-3 gap-2 sm:gap-2.5 lg:flex lg:max-w-[420px] lg:flex-wrap lg:items-stretch lg:justify-end">
            <div
              className="inline-flex min-h-[64px] min-w-0 flex-col items-center justify-center gap-0.5 overflow-hidden rounded-[16px] border border-black/7 bg-white/68 px-2 py-1.5 shadow-[0_10px_22px_rgba(0,0,0,0.08),inset_0_1px_0_rgba(255,255,255,0.8)] sm:min-h-[68px] sm:px-2.5 lg:min-h-[72px] lg:rounded-[18px] lg:px-4 lg:py-2 dark:border-white/8 dark:bg-white/[0.04] dark:shadow-[0_12px_24px_rgba(0,0,0,0.34),inset_0_1px_0_rgba(255,255,255,0.06)]"
            >
              <div className="flex items-center justify-center text-[#E11D48]">
                <TrendGlyph direction={direction} />
              </div>
              <div className="min-w-0 text-center">
                <div className="text-[6px] font-black uppercase tracking-[0.14em] text-foreground/38 sm:text-[7px] dark:text-white/32">
                  Trend
                </div>
                <div className="mt-0.5 text-[9px] font-black uppercase tracking-[0.12em] text-foreground sm:text-[10px] dark:text-white">
                  {directionLabel}
                </div>
              </div>
            </div>

            <div className="min-w-0 rounded-[16px] border border-black/7 bg-black/[0.04] px-2 py-2 text-center shadow-[inset_0_1px_0_rgba(255,255,255,0.66)] sm:px-2.5 lg:min-w-[170px] lg:flex-1 lg:rounded-[18px] lg:px-3.5 lg:py-3 lg:text-left dark:border-white/8 dark:bg-white/[0.03] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]">
              <div className="truncate text-[7px] font-black uppercase tracking-[0.1em] text-foreground/38 sm:text-[8px] sm:tracking-[0.14em] lg:tracking-[0.16em]">{followersLabel}</div>
              <div className="mt-1 text-[15px] font-black leading-none tracking-[-0.055em] text-foreground sm:text-[17px] lg:mt-2 lg:text-[22px] dark:text-white xl:text-[24px]">
                {formatFollowers(displayFollowers)}
              </div>
            </div>

            <div className="min-w-0 rounded-[16px] border border-black/6 bg-black/[0.03] px-2 py-2 text-center shadow-[inset_0_1px_0_rgba(255,255,255,0.56)] sm:px-2.5 lg:min-w-[110px] lg:rounded-[18px] lg:px-3.5 lg:py-3 lg:text-left dark:border-white/8 dark:bg-white/[0.03] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.03)]">
              <div className="text-[7px] font-black uppercase tracking-[0.1em] text-foreground/38 sm:text-[8px] sm:tracking-[0.16em]">Change</div>
              <div className="mt-1 text-[13px] font-black uppercase tracking-[0.04em] text-foreground sm:text-[14px] lg:mt-2 lg:text-[16px] lg:tracking-[0.08em] dark:text-white xl:text-[17px]">
                {signedPercent(animatedDeltaPercent)}
              </div>
            </div>
          </div>
        </div>

        <div
          ref={rootRef}
          className="relative mt-4 min-h-0 flex-1 overflow-hidden rounded-[18px] border border-black/8 bg-[linear-gradient(180deg,rgba(190,190,190,0.26),rgba(128,128,128,0.11))] px-3 pb-5 pt-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.72),inset_0_-18px_26px_rgba(0,0,0,0.05)] dark:border-white/8 dark:bg-[linear-gradient(180deg,rgba(24,24,24,0.96),rgba(10,10,10,0.98))] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04),inset_0_-18px_30px_rgba(0,0,0,0.42)]"
          style={{ minHeight: 148 }}
          onMouseMove={(event) => {
            const nextIndex = resolveIndex(event.clientX);
            if (nextIndex == null) return;
            setHoverIndex(nextIndex);
          }}
          onMouseLeave={() => setHoverIndex(null)}
          onClick={(event) => {
            const nextIndex = resolveIndex(event.clientX);
            if (nextIndex == null) return;
            setSelectedIndex((current) => (current === nextIndex ? null : nextIndex));
          }}
          onPointerDown={(event) => {
            if (event.pointerType === 'mouse') return;
            const nextIndex = resolveIndex(event.clientX);
            if (nextIndex == null) return;
            setTouchScrubbing(true);
            setSelectedIndex(nextIndex);
            setHoverIndex(nextIndex);
          }}
          onPointerMove={(event) => {
            if (event.pointerType === 'mouse' || !touchScrubbing) return;
            const nextIndex = resolveIndex(event.clientX);
            if (nextIndex == null) return;
            setSelectedIndex(nextIndex);
            setHoverIndex(nextIndex);
          }}
          onPointerUp={(event) => {
            if (event.pointerType === 'mouse') return;
            setTouchScrubbing(false);
            setHoverIndex(null);
          }}
          onPointerCancel={() => {
            setTouchScrubbing(false);
            setHoverIndex(null);
          }}
        >
          <svg className="h-full w-full" preserveAspectRatio="none" viewBox="0 0 100 40">
            <defs>
              <linearGradient id={`ascentFill-${gradientId}`} x1="0%" y1="0%" x2="0%" y2="100%">
                <stop offset="0%" stopColor={ASCENT_ACCENT} stopOpacity="0.24" />
                <stop offset="100%" stopColor={ASCENT_ACCENT} stopOpacity="0.08" />
              </linearGradient>
            </defs>

            <line x1="0" y1="10" x2="100" y2="10" stroke="currentColor" strokeWidth="0.45" className="text-black/16 dark:text-white/10" />
            <line x1="0" y1="20" x2="100" y2="20" stroke="currentColor" strokeWidth="0.45" className="text-black/16 dark:text-white/10" />
            <line x1="0" y1="30" x2="100" y2="30" stroke="currentColor" strokeWidth="0.45" className="text-black/16 dark:text-white/10" />

            <motion.path
              d={chart.area}
              fill={`url(#ascentFill-${gradientId})`}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ duration: 0.35 }}
            />
            <motion.path
              d={chart.line}
              fill="none"
              stroke={ASCENT_ACCENT}
              strokeWidth="1.55"
              strokeLinecap="round"
              strokeLinejoin="round"
              initial={{ pathLength: 0, opacity: 0 }}
              animate={{ pathLength: 1, opacity: 1 }}
              transition={{ duration: 0.72, ease: 'easeOut' }}
            />

            {displayPoint ? (
              <>
                <rect
                  x={displayPoint.x - 1.15}
                  y="4"
                  width="2.3"
                  height="34"
                  rx="1.15"
                  fill={`rgb(${ASCENT_ACCENT_RGB} / 0.14)`}
                />
                <line
                  x1={displayPoint.x}
                  y1="5"
                  x2={displayPoint.x}
                  y2="37"
                  stroke={ASCENT_ACCENT}
                  strokeOpacity="0.72"
                  strokeWidth="0.7"
                  strokeLinecap="round"
                />
                <circle
                  cx={displayPoint.x}
                  cy={displayPoint.y}
                  r="2.75"
                  fill="var(--background)"
                />
                <circle
                  cx={displayPoint.x}
                  cy={displayPoint.y}
                  r="1.85"
                  fill={ASCENT_ACCENT}
                />
              </>
            ) : null}
          </svg>

          <div className="pointer-events-none absolute bottom-0 left-0 right-0 h-4 px-[2px] text-[8px] font-black uppercase tracking-[0.1em] text-foreground/34">
            {chart.points.map((point, index) => (
              shouldShowAxisLabel(index, chart.points.length) ? (
                <span key={`${point.fullLabel}-${index}`} className="absolute -translate-x-1/2" style={{ left: `${point.x}%` }}>
                  {point.label}
                </span>
              ) : null
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
