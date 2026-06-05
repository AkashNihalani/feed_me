'use client';

/* ─────────────────────────────────────────────
   LIVE DASHBOARD — pre-login platform telemetry

   Real totals from /api/stats/public, shown as one synchronized dashboard
   shift: all tiles update together, settle together, then rest.
   One identity: white ground, ink numbers, ONE solid-red flagship tile.
   Bold red used sparingly — no washed glows. Alive on mobile (no hover).
   ───────────────────────────────────────────── */

import { useEffect, useRef, useState } from 'react';
import { motion } from 'framer-motion';
import { TrendingUp } from 'lucide-react';
import { cn } from '@/lib/utils';
import Odometer from './Odometer';
import { type LiveMetrics, type LivePlatformState, type MetricAnchor, type MetricKey } from '@/lib/useLiveStats';

const RED = '#E11D48';
const INK = '#0B0B0F';
const APPLE_EASE = [0.32, 0.72, 0, 1] as const;

const METRICS: Array<{ key: MetricKey; label: string }> = [
  { key: 'accounts', label: 'Accounts tracked' },
  { key: 'signals', label: 'Signals surfaced' },
  { key: 'posts', label: 'Posts analyzed' },
  { key: 'likes', label: 'Likes tracked' },
  { key: 'views', label: 'Views tracked' },
  { key: 'comments', label: 'Comments tracked' },
];
const FLAGSHIP: MetricKey = 'accounts';
const DASHBOARD_SHIFT_MS = 9_000;
const DASHBOARD_REST_MS = 15_000;

const WHITE_TILE: React.CSSProperties = {
  background: '#ffffff',
  border: '1px solid rgba(14,19,28,0.09)',
  boxShadow: '0 1px 2px rgba(15,23,42,0.04), 0 14px 30px -18px rgba(15,23,42,0.22)',
};
const RED_TILE: React.CSSProperties = {
  background: RED,
  boxShadow: '0 16px 32px -16px rgba(15,23,42,0.42)', // neutral depth, NOT a red glow
};

type DashboardValues = Partial<Record<MetricKey, number | null>>;

function projectedValue(anchor: MetricAnchor | null | undefined, fetchedAt: number) {
  const base = anchor?.value;
  if (base == null) return null;
  const rate = anchor?.ratePerSec ?? 0;
  const elapsedSec = Math.max(0, (Date.now() - fetchedAt) / 1000);
  return Math.round(base + rate * elapsedSec);
}

function snapshotValues(metrics: LiveMetrics | null, fetchedAt: number): DashboardValues {
  const next: DashboardValues = {};
  for (const metric of METRICS) {
    next[metric.key] = projectedValue(metrics?.[metric.key], fetchedAt);
  }
  return next;
}

function StatTile({
  value,
  climbing,
  label,
  flagship,
  delay,
}: {
  value: number | null;
  climbing: boolean;
  label: string;
  flagship: boolean;
  delay: number;
}) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.6, ease: APPLE_EASE, delay }}
      className="relative flex min-h-[116px] flex-col justify-between overflow-hidden rounded-[18px] p-[22px] sm:min-h-[136px] sm:p-6 lg:min-h-[158px] lg:rounded-[20px] lg:p-7 xl:min-h-[178px]"
      style={flagship ? RED_TILE : WHITE_TILE}
    >
      {flagship && (
        <div
          className="pointer-events-none absolute inset-0"
          style={{ background: 'linear-gradient(180deg, rgba(255,255,255,0.20) 0%, transparent 40%, rgba(0,0,0,0.10) 100%)' }}
        />
      )}

      <div className="relative flex items-center justify-between">
        <span
          className="text-[9px] font-black uppercase tracking-[0.2em] lg:text-[10px]"
          style={{ color: flagship ? 'rgba(255,255,255,0.82)' : 'rgba(11,11,15,0.42)' }}
        >
          {label}
        </span>
        {climbing && (
          <TrendingUp size={13} strokeWidth={3} style={{ color: flagship ? 'rgba(255,255,255,0.85)' : RED }} />
        )}
      </div>

      <div className="relative mt-4 font-black tracking-[-0.02em] text-[28px] sm:text-[34px] lg:text-[42px] xl:text-[52px]">
        <Odometer value={value} color={flagship ? '#ffffff' : INK} durationMs={DASHBOARD_SHIFT_MS} />
      </div>
    </motion.div>
  );
}

export default function LiveDashboard({ state, className }: { state: LivePlatformState; className?: string }) {
  const { metrics, fetchedAt } = state;
  const metricsRef = useRef(metrics);
  const fetchedAtRef = useRef(fetchedAt);
  const [dashboardValues, setDashboardValues] = useState<DashboardValues>(() => snapshotValues(metrics, fetchedAt));

  useEffect(() => {
    metricsRef.current = metrics;
    fetchedAtRef.current = fetchedAt;
  }, [metrics, fetchedAt]);

  useEffect(() => {
    if (!metrics) return;

    let cancelled = false;
    let timeout: ReturnType<typeof setTimeout> | undefined;
    let frame = 0;

    frame = requestAnimationFrame(() => {
      if (!cancelled) setDashboardValues(snapshotValues(metrics, fetchedAt));
    });

    const scheduleRest = () => {
      timeout = setTimeout(() => {
        if (cancelled) return;
        setDashboardValues(snapshotValues(metricsRef.current, fetchedAtRef.current));
        timeout = setTimeout(() => {
          if (!cancelled) scheduleRest();
        }, DASHBOARD_SHIFT_MS);
      }, DASHBOARD_REST_MS);
    };

    scheduleRest();

    return () => {
      cancelled = true;
      cancelAnimationFrame(frame);
      if (timeout) clearTimeout(timeout);
    };
  }, [metrics, fetchedAt]);

  return (
    <div className={cn('w-full', className)} data-login-live-dashboard>
      <div className="mb-4 flex items-center gap-2.5">
        <span className="relative flex h-2 w-2">
          <span className="absolute inline-flex h-full w-full animate-ping rounded-full opacity-70" style={{ background: RED }} />
          <span className="relative inline-flex h-2 w-2 rounded-full" style={{ background: RED }} />
        </span>
        <h2 className="text-[22px] font-black leading-none tracking-[-0.045em] sm:text-[25px] lg:text-[28px]" style={{ color: INK }}>
          Currently <span style={{ color: RED }}>feeding</span> on
        </h2>
      </div>

      {/* The dashboard — equal bento tiles, exact rolling odometers */}
      <div className="grid grid-cols-2 gap-3.5 sm:gap-4 lg:gap-5">
        {METRICS.map((metric, index) => (
          <StatTile
            key={metric.key}
            value={dashboardValues[metric.key] ?? null}
            climbing={(metrics?.[metric.key]?.ratePerSec ?? 0) > 0}
            label={metric.label}
            flagship={metric.key === FLAGSHIP}
            delay={0.06 + index * 0.05}
          />
        ))}
      </div>
    </div>
  );
}
