'use client';

import { useEffect, useState } from 'react';
import { motion, useMotionValue, useSpring } from 'framer-motion';
import { FrequencyPoint, Timeframe } from './dashboardTypes';

type MetricRow = { label: string; value: number };
const SCALE_MAX = 3.5;

function avg(values: Array<number | null | undefined>): number | null {
  const clean = values.filter((v): v is number => typeof v === 'number' && Number.isFinite(v));
  if (clean.length === 0) return null;
  return clean.reduce((sum, v) => sum + v, 0) / clean.length;
}

function percentileToRatio(value: number | null): number {
  if (value === null) return 1;
  return Math.max(0.4, Math.min(SCALE_MAX, (101 - value) / 35));
}

function AnimatedMultiplier({ value }: { value: number }) {
  const mv = useMotionValue(0);
  const spring = useSpring(mv, { stiffness: 320, damping: 28, mass: 0.9 });
  const [renderValue, setRenderValue] = useState(0);

  useEffect(() => {
    const unsub = spring.on('change', (v) => setRenderValue(v));
    mv.set(value);
    return unsub;
  }, [mv, spring, value]);

  return <>{renderValue.toFixed(1)}x</>;
}

export default function FeedVelocityBars({ series }: { timeframe: Timeframe; series: FrequencyPoint[] }) {
  const metrics: MetricRow[] = [
    { label: 'Views', value: percentileToRatio(avg(series.map((r) => r.avg_views_percentile))) },
    { label: 'Likes', value: percentileToRatio(avg(series.map((r) => r.avg_likes_percentile))) },
    { label: 'Comments', value: percentileToRatio(avg(series.map((r) => r.avg_comments_percentile))) },
  ];

  return (
    <div className="fm-depth-glass relative flex h-full w-full flex-col overflow-hidden rounded-[22px] p-3.5 sm:p-4">
      <div className="relative z-10 flex flex-1 flex-col">
        <div className="mb-2.5 flex items-center justify-between">
          <span className="fm-label fm-depth-title">Pulse</span>
          <span className="fm-depth-chip rounded-full px-2 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/40 dark:text-white/40">
            1.0x baseline
          </span>
        </div>

        <div className="grid flex-1 grid-cols-3 gap-2 sm:gap-2.5">
          {metrics.map((metric) => {
            const fillPct = Math.max(16, Math.min(100, (metric.value / SCALE_MAX) * 100));
            const isAbove = metric.value >= 1;

            return (
              <div key={metric.label} className="flex min-h-[140px] sm:min-h-[160px] flex-col">
                {/* Bar track — relative container with explicit flex-1 */}
                <div className="relative flex-1 overflow-hidden rounded-[16px] border border-black/6 bg-white shadow-[inset_0_1px_0_rgba(255,255,255,0.9),0_4px_12px_rgba(0,0,0,0.04)] dark:border-white/6 dark:bg-[#0a0a0a] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04),0_4px_12px_rgba(0,0,0,0.2)]">
                  {/* Baseline mark */}
                  <div className="absolute inset-x-0 z-20 border-t border-dashed border-black/12 dark:border-white/12"
                    style={{ bottom: `${(1 / SCALE_MAX) * 100}%` }} />

                  {/* Fill bar — anchored to bottom with absolute positioning */}
                  <motion.div
                    className={`absolute inset-x-0 bottom-0 rounded-t-[12px] ${
                      isAbove
                        ? 'bg-[#CCFF00] shadow-[0_-4px_20px_rgba(204,255,0,0.3)] dark:shadow-[0_-4px_24px_rgba(204,255,0,0.25)]'
                        : 'bg-black/8 dark:bg-white/10'
                    }`}
                    initial={{ height: 0 }}
                    animate={{ height: `${fillPct}%` }}
                    transition={{ duration: 0.8, ease: [0.25, 0.1, 0.25, 1] as const, delay: 0.2 }}
                  >
                    {/* Top shine */}
                    <div className={`h-[3px] w-full rounded-t-[12px] ${isAbove ? 'bg-white/40 dark:bg-white/20' : 'bg-white/20 dark:bg-white/5'}`} />
                  </motion.div>
                </div>

                {/* Label */}
                <div className="mt-2 text-center">
                  <div className={`text-[clamp(18px,3.5vw,24px)] font-black leading-none tracking-[-0.03em] fm-depth-title ${
                    isAbove ? 'text-foreground dark:text-[#CCFF00]' : 'text-foreground/50 dark:text-white/40'
                  }`}>
                    <AnimatedMultiplier value={metric.value} />
                  </div>
                  <div className="mt-0.5 text-[9px] font-black uppercase tracking-[0.16em] text-foreground/40 dark:text-white/36 fm-depth-title">
                    {metric.label}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
