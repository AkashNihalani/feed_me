'use client';

import { useEffect, useId, useMemo, useRef, useState } from 'react';
import { animate, motion, useMotionValue, useSpring } from 'framer-motion';
import { AscentPoint, Timeframe } from './dashboardTypes';

type ChartPoint = {
  label: string;
  followers: number;
};

function compact(value: number): string {
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return String(Math.round(value));
}

function toLabel(weekStart: string): string {
  const d = new Date(`${weekStart}T00:00:00`);
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }).toUpperCase();
}

function shouldShowAxisLabel(index: number, total: number): boolean {
  if (total <= 1) return true;
  if (index === 0 || index === total - 1) return true;
  const targetLabels = total <= 7 ? total : total <= 30 ? 5 : 6;
  const step = Math.max(1, Math.ceil((total - 1) / Math.max(1, targetLabels - 1)));
  return index % step === 0;
}

export default function FeedAscentChart({ timeframe, series }: { timeframe: Timeframe; series: AscentPoint[] }) {
  const data = useMemo<ChartPoint[]>(() => {
    if (!Array.isArray(series) || series.length === 0) {
      return [{ label: timeframe, followers: 0 }];
    }
    return series.map((point) => ({
      label: toLabel(point.snapshot_date_ist),
      followers: Number(point.follower_count) || 0,
    }));
  }, [series, timeframe]);
  const rootRef = useRef<HTMLDivElement | null>(null);
  const gradientId = useId().replace(/[:]/g, '');

  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const [touchScrubbing, setTouchScrubbing] = useState(false);
  const [animatedDelta, setAnimatedDelta] = useState(0);

  const deltaValue = useMotionValue(0);
  const deltaSpring = useSpring(deltaValue, { stiffness: 170, damping: 24, mass: 0.8 });

  const chart = useMemo(() => {
    const min = Math.min(...data.map((d) => d.followers));
    const max = Math.max(...data.map((d) => d.followers));
    const range = Math.max(1, max - min);

    const points = data.map((d, i) => {
      const x = (i / Math.max(1, data.length - 1)) * 100;
      const y = 34 - ((d.followers - min) / range) * 26;
      return { ...d, x, y };
    });

    const line = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ');
    const area = `${line} L 100 38 L 0 38 Z`;
    const base = points[0].followers;
    const delta = base > 0 ? ((points[points.length - 1].followers - base) / base) * 100 : 0;

    return { points, line, area, delta };
  }, [data]);

  useEffect(() => {
    const stop = deltaSpring.on('change', (v) => setAnimatedDelta(v));
    const controls = animate(deltaValue, chart.delta, { duration: 0.7, ease: 'easeOut' });
    return () => {
      stop();
      controls.stop();
    };
  }, [chart.delta, deltaSpring, deltaValue]);

  const hoverPoint = activeIndex == null ? null : chart.points[activeIndex];

  const pickIndexFromPointer = (clientX: number) => {
    const rect = rootRef.current?.getBoundingClientRect();
    if (!rect) return;
    const x = Math.max(0, Math.min(rect.width, clientX - rect.left));
    const ratio = rect.width <= 0 ? 0 : x / rect.width;
    const idx = Math.round(ratio * (chart.points.length - 1));
    setActiveIndex(Math.max(0, Math.min(chart.points.length - 1, idx)));
  };

  return (
    <div className="fm-depth-glass relative flex h-full w-full flex-col overflow-hidden rounded-[22px] p-3.5 sm:p-4 lg:p-5">

      <div className="relative z-10 flex flex-1 flex-col">
        {/* Header + hero value — compact */}
        <div className="mb-1 flex items-end justify-between gap-2">
          <div>
            <div className="fm-label fm-depth-title">Ascent</div>
            <div className="mt-0.5 flex items-end gap-2">
              <div className="text-[clamp(42px,11vw,64px)] font-black leading-[0.85] text-black dark:text-[#CCFF00] fm-depth-title">
                {animatedDelta >= 0 ? '+' : ''}{animatedDelta.toFixed(1)}%
              </div>
              <div className="pb-1.5 text-[11px] font-black uppercase tracking-[0.12em] text-foreground/50 fm-depth-title">Past {timeframe}</div>
            </div>
          </div>
        </div>

        {/* Chart fills remaining space */}
        <div
          ref={rootRef}
          className="relative mt-1 min-h-0 flex-1"
          style={{ minHeight: 120 }}
          onMouseMove={(e) => pickIndexFromPointer(e.clientX)}
          onMouseLeave={() => !touchScrubbing && setActiveIndex(null)}
          onPointerDown={(e) => {
            setTouchScrubbing(true);
            pickIndexFromPointer(e.clientX);
          }}
          onPointerMove={(e) => {
            if (touchScrubbing) pickIndexFromPointer(e.clientX);
          }}
          onPointerUp={() => {
            setTouchScrubbing(false);
            setActiveIndex(null);
          }}
          onPointerCancel={() => {
            setTouchScrubbing(false);
            setActiveIndex(null);
          }}
        >
          <svg className="h-full w-full" preserveAspectRatio="none" viewBox="0 0 100 40">
            <motion.path
              d={chart.area}
              fill={`url(#ascentFill-${gradientId})`}
              initial={{ opacity: 0 }}
              animate={{ opacity: 0.48 }}
              transition={{ duration: 0.35 }}
            />
            <motion.path
              d={chart.line}
              fill="none"
              stroke="currentColor"
              strokeWidth="2.8"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="text-[#CCFF00] drop-shadow-none dark:drop-shadow-[0_2px_6px_rgba(204,255,0,0.3)]"
              initial={{ pathLength: 0, opacity: 0 }}
              animate={{ pathLength: 1, opacity: 1 }}
              transition={{ duration: 0.72, ease: 'easeOut' }}
            />
            <defs>
              <linearGradient id={`ascentFill-${gradientId}`} x1="0%" y1="0%" x2="0%" y2="100%">
                <stop offset="0%" stopColor="#CCFF00" stopOpacity="0.25" />
                <stop offset="100%" stopColor="#CCFF00" stopOpacity="0" />
              </linearGradient>
            </defs>

            <line x1="0" y1="10" x2="100" y2="10" stroke="currentColor" strokeWidth="0.45" className="text-foreground/14" />
            <line x1="0" y1="20" x2="100" y2="20" stroke="currentColor" strokeWidth="0.45" className="text-foreground/14" />
            <line x1="0" y1="30" x2="100" y2="30" stroke="currentColor" strokeWidth="0.45" className="text-foreground/14" />

            {hoverPoint ? (
              <>
                <line x1={hoverPoint.x} y1="4" x2={hoverPoint.x} y2="38" stroke="rgba(204,255,0,0.9)" strokeWidth="0.95" style={{ filter: 'drop-shadow(0 0 8px rgba(204,255,0,0.85))' }} />
                <line x1={hoverPoint.x} y1="4" x2={hoverPoint.x} y2="38" stroke="rgba(255,255,255,0.5)" strokeWidth="0.6" strokeDasharray="1.2 1.2" />
                <circle cx={hoverPoint.x} cy={hoverPoint.y} r="1.6" fill="#CCFF00" style={{ filter: 'drop-shadow(0 0 8px rgba(204,255,0,0.95))' }} />
                <circle cx={hoverPoint.x} cy={hoverPoint.y} r="2.8" fill="rgba(204,255,0,0.26)" />
              </>
            ) : null}
          </svg>

          {hoverPoint ? (
            <div
              className="fm-depth-chip pointer-events-none absolute -translate-x-1/2 rounded-[10px] px-2.5 py-1.5 text-[10px] font-black uppercase tracking-[0.08em]"
              style={{ left: `${hoverPoint.x}%`, top: 2 }}
            >
              <div className="text-foreground/68">{hoverPoint.label}</div>
              <div className="text-foreground">{compact(hoverPoint.followers)} followers</div>
            </div>
          ) : null}

          <div className="pointer-events-none absolute bottom-0 left-0 right-0 h-4 px-[2px] text-[9px] font-black uppercase tracking-[0.1em] text-foreground/34">
            {chart.points.map((p, index) => (
              shouldShowAxisLabel(index, chart.points.length) ? (
                <span key={`${p.label}-${index}`} className="absolute -translate-x-1/2" style={{ left: `${p.x}%` }}>
                  {p.label}
                </span>
              ) : null
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
