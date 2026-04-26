'use client';

import { memo, useEffect, useId, useMemo, useRef, useState } from 'react';
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

type ChartSelection = {
  signature: string;
  index: number;
};

const ASCENT_ACCENT = '#E11D48';
const TREND_PATHS = {
  flat: 'M 13 32 L 51 32 M 51 32 L 51 32 M 51 32 L 51 32',
  rise: 'M 32 53 L 32 12 M 32 12 L 21 23 M 32 12 L 43 23',
  drop: 'M 32 11 L 32 52 M 32 52 L 21 41 M 32 52 L 43 41',
} as const;

type TrendDirection = keyof typeof TREND_PATHS;

function formatDeltaFollowers(value: number): string {
  if (!Number.isFinite(value) || Math.abs(value) < 0.5) return '0';
  return new Intl.NumberFormat('en-US').format(Math.abs(Math.round(value)));
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

function buildChartLine(points: ChartPoint[]): string {
  if (points.length === 0) return '';
  if (points.length === 1) return `M 0 ${points[0]!.y} L 100 ${points[0]!.y}`;

  return points.map((point, index) => `${index === 0 ? 'M' : 'L'} ${point.x} ${point.y}`).join(' ');
}

function hashText(value: string): string {
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) {
    hash = (hash * 31 + value.charCodeAt(index)) % 2_147_483_647;
  }
  return hash.toString(36);
}

function TrendGlyph({ direction }: { direction: TrendDirection }) {
  const isFlat = direction === 'flat';
  const path = TREND_PATHS[direction];

  return (
    <motion.div
      aria-hidden="true"
      className="inline-flex h-[1em] w-[1em] items-center justify-center text-[#E11D48]"
      initial={false}
      animate={{ scale: isFlat ? 0.92 : 1 }}
      transition={{ type: 'spring', stiffness: 240, damping: 22, mass: 0.74 }}
    >
      <svg
        viewBox="0 0 64 64"
        className="h-[1em] w-[1em] overflow-visible"
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
      </svg>
    </motion.div>
  );
}

function FeedAscentChart({ timeframe, series }: { timeframe: Timeframe; series: AscentPoint[] }) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const gradientId = useId().replace(/[:]/g, '');

  const [hoverSelection, setHoverSelection] = useState<ChartSelection | null>(null);
  const [selectedSelection, setSelectedSelection] = useState<ChartSelection | null>(null);
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
        x: data.length <= 1 ? 50 : (index / Math.max(1, data.length - 1)) * 100,
        y: 34 - ((followers - min) / range) * 26,
        deltaFromPrevious,
        deltaPercentFromPrevious,
        previousLabel: index > 0 ? toLabel(data[index - 1]!.snapshot_date_ist) : null,
      };
    });

    const line = buildChartLine(points);
    const areaStartX = points.length <= 1 ? 0 : points[0]?.x ?? 0;
    const areaEndX = points.length <= 1 ? 100 : points[points.length - 1]?.x ?? 100;
    const area = `${line} L ${areaEndX} 38 L ${areaStartX} 38 Z`;
    const firstFollowers = points[0]?.followers ?? 0;
    const lastFollowers = points[points.length - 1]?.followers ?? 0;
    const overallDeltaCount = lastFollowers - firstFollowers;
    const overallDeltaPercent = firstFollowers > 0 ? ((lastFollowers - firstFollowers) / firstFollowers) * 100 : 0;

    return {
      points,
      line,
      area,
      signature: `${timeframe}-${hashText(points.map((point) => `${point.fullLabel}:${point.followers}`).join('|'))}`,
      latestFollowers: lastFollowers,
      overallDeltaCount,
      overallDeltaPercent,
    };
  }, [data, timeframe]);

  const hoverIndex = hoverSelection?.signature === chart.signature ? hoverSelection.index : null;
  const selectedIndex = selectedSelection?.signature === chart.signature ? selectedSelection.index : null;
  const displayIndex = hoverIndex ?? selectedIndex;
  const displayPoint = displayIndex == null ? null : chart.points[displayIndex] ?? null;
  const displayDeltaCount = displayPoint ? displayPoint.deltaFromPrevious : chart.overallDeltaCount;
  const displayDeltaPercent = displayPoint ? displayPoint.deltaPercentFromPrevious : chart.overallDeltaPercent;
  const displayFollowers = displayPoint?.followers ?? chart.latestFollowers;
  const direction: TrendDirection = displayDeltaCount > 0 ? 'rise' : displayDeltaCount < 0 ? 'drop' : 'flat';
  const contextLine = displayPoint
    ? displayPoint.previousLabel
      ? `${displayPoint.label} vs ${displayPoint.previousLabel}`
      : `${displayPoint.label} starting point`
    : `Net over ${timeframe}`;
  const lineRevealId = `ascentLineReveal-${gradientId}-${chart.signature}`;

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
        <div className="flex flex-col gap-3.5 sm:gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div className="min-w-0 flex-1">
            <div className="fm-label fm-depth-title">Follower Growth</div>
            <div className="mt-2 grid grid-cols-[1em_minmax(0,1fr)] items-center gap-x-3 text-[clamp(38px,10vw,64px)] leading-none sm:gap-x-4 lg:text-[clamp(44px,4.6vw,72px)]">
              <div className="flex h-[1em] w-[1em] shrink-0 items-center justify-center">
                <TrendGlyph direction={direction} />
              </div>
              <div className="min-w-0 font-black leading-none tracking-[-0.07em] tabular-nums text-black dark:text-white">
                {formatDeltaFollowers(animatedDeltaCount)}
              </div>
            </div>
            <div className="mt-1.5 text-[9px] font-black uppercase tracking-[0.14em] text-foreground/42 dark:text-white/36">
              {contextLine}
            </div>
          </div>

          <div className="w-full lg:max-w-[360px]">
            <div className="grid grid-cols-2 gap-4 border-t border-black/8 pt-3 dark:border-white/10 lg:border-l lg:border-t-0 lg:pl-4 lg:pt-0">
              <div className="min-w-0">
                <div className="text-[8px] font-black uppercase tracking-[0.16em] text-foreground/38 dark:text-white/32">
                  Current Total
                </div>
                <div className="mt-1.5 text-[21px] font-black leading-none tracking-[-0.05em] tabular-nums text-foreground sm:text-[24px] lg:text-[28px] dark:text-white">
                {formatFollowers(displayFollowers)}
                </div>
              </div>
              <div className="min-w-0">
                <div className="text-[8px] font-black uppercase tracking-[0.16em] text-foreground/38 dark:text-white/32">
                  Growth
                </div>
                <div className="mt-1.5 text-[19px] font-black leading-none tracking-[-0.045em] tabular-nums text-foreground sm:text-[21px] lg:text-[24px] dark:text-white">
                  {signedPercent(animatedDeltaPercent)}
                </div>
              </div>
            </div>
          </div>
        </div>

        <div
          ref={rootRef}
          className="relative mt-3.5 min-h-0 flex-1 overflow-hidden rounded-[18px] border border-black/8 bg-[linear-gradient(180deg,rgba(252,252,252,0.88),rgba(234,234,234,0.58))] px-3 pb-5 pt-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.76),inset_0_-12px_20px_rgba(0,0,0,0.04)] dark:border-white/8 dark:bg-[linear-gradient(180deg,rgba(20,20,20,0.95),rgba(6,6,6,0.99))] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04),inset_0_-18px_30px_rgba(0,0,0,0.42)]"
          style={{ minHeight: 148 }}
          onMouseMove={(event) => {
            const nextIndex = resolveIndex(event.clientX);
            if (nextIndex == null) return;
            setHoverSelection({ signature: chart.signature, index: nextIndex });
          }}
          onMouseLeave={() => setHoverSelection(null)}
          onClick={(event) => {
            const nextIndex = resolveIndex(event.clientX);
            if (nextIndex == null) return;
            setSelectedSelection((current) => (
              current?.signature === chart.signature && current.index === nextIndex
                ? null
                : { signature: chart.signature, index: nextIndex }
            ));
          }}
          onPointerDown={(event) => {
            if (event.pointerType === 'mouse') return;
            const nextIndex = resolveIndex(event.clientX);
            if (nextIndex == null) return;
            setTouchScrubbing(true);
            setSelectedSelection({ signature: chart.signature, index: nextIndex });
            setHoverSelection({ signature: chart.signature, index: nextIndex });
          }}
          onPointerMove={(event) => {
            if (event.pointerType === 'mouse' || !touchScrubbing) return;
            const nextIndex = resolveIndex(event.clientX);
            if (nextIndex == null) return;
            setSelectedSelection({ signature: chart.signature, index: nextIndex });
            setHoverSelection({ signature: chart.signature, index: nextIndex });
          }}
          onPointerUp={(event) => {
            if (event.pointerType === 'mouse') return;
            setTouchScrubbing(false);
            setHoverSelection(null);
          }}
          onPointerCancel={() => {
            setTouchScrubbing(false);
            setHoverSelection(null);
          }}
        >
          <svg className="h-full w-full" preserveAspectRatio="none" viewBox="0 0 100 40">
            <defs>
              <linearGradient id={`ascentFill-${gradientId}`} x1="0%" y1="0%" x2="0%" y2="100%">
                <stop offset="0%" stopColor={ASCENT_ACCENT} stopOpacity="0.12" />
                <stop offset="100%" stopColor={ASCENT_ACCENT} stopOpacity="0.02" />
              </linearGradient>
              <linearGradient id={`ascentStroke-${gradientId}`} x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" stopColor="#FB7185" stopOpacity="0.72" />
                <stop offset="46%" stopColor={ASCENT_ACCENT} stopOpacity="0.96" />
                <stop offset="100%" stopColor="#BE123C" stopOpacity="0.94" />
              </linearGradient>
              <clipPath id={lineRevealId} key={lineRevealId}>
                <rect
                  className="fm-ascent-line-clip"
                  x="-1"
                  y="0"
                  width="102"
                  height="40"
                />
              </clipPath>
            </defs>

            <line x1="0" y1="10" x2="100" y2="10" stroke="currentColor" strokeWidth="0.45" className="text-black/16 dark:text-white/10" />
            <line x1="0" y1="20" x2="100" y2="20" stroke="currentColor" strokeWidth="0.45" className="text-black/16 dark:text-white/10" />
            <line x1="0" y1="30" x2="100" y2="30" stroke="currentColor" strokeWidth="0.45" className="text-black/16 dark:text-white/10" />

            <g key={chart.signature} clipPath={`url(#${lineRevealId})`}>
              <path
                d={chart.area}
                fill={`url(#ascentFill-${gradientId})`}
              />
              <g>
                <path
                  d={chart.line}
                  fill="none"
                  stroke={ASCENT_ACCENT}
                  strokeOpacity="0.2"
                  strokeWidth="3.2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  vectorEffect="non-scaling-stroke"
                />
                <path
                  d={chart.line}
                  fill="none"
                  stroke={`url(#ascentStroke-${gradientId})`}
                  strokeWidth="1.24"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  vectorEffect="non-scaling-stroke"
                />
              </g>
            </g>

            {displayPoint ? (
              <>
                <line
                  x1={displayPoint.x}
                  y1="5"
                  x2={displayPoint.x}
                  y2="37"
                  stroke={ASCENT_ACCENT}
                  strokeOpacity="0.5"
                  strokeWidth="0.52"
                  strokeLinecap="round"
                  vectorEffect="non-scaling-stroke"
                />
              </>
            ) : null}
          </svg>

          {displayPoint ? (
            <div className="pointer-events-none absolute bottom-5 left-3 right-3 top-3 z-[4]">
              <div
                className="absolute h-4 w-4 -translate-x-1/2 -translate-y-1/2 rounded-full bg-[#E11D48]/18 shadow-[0_0_0_1px_rgba(225,29,72,0.2),0_8px_20px_rgba(225,29,72,0.22)] dark:bg-[#E11D48]/24 dark:shadow-[0_0_0_1px_rgba(255,255,255,0.1),0_9px_22px_rgba(225,29,72,0.32)]"
                style={{
                  left: `${displayPoint.x}%`,
                  top: `${(displayPoint.y / 40) * 100}%`,
                }}
              >
                <span className="absolute left-1/2 top-1/2 h-2.5 w-2.5 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-[var(--background)] bg-[#E11D48] shadow-[0_0_0_1px_rgba(225,29,72,0.42)] dark:border-[#050505]" />
              </div>
            </div>
          ) : null}

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

const MemoizedFeedAscentChart = memo(FeedAscentChart);
MemoizedFeedAscentChart.displayName = 'FeedAscentChart';

export default MemoizedFeedAscentChart;
