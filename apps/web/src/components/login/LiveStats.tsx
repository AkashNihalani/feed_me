'use client';

/* ─────────────────────────────────────────────
   LIVE DASHBOARD — pre-login platform telemetry

   Real totals from /api/stats/public, shown as a ranked number pyramid:
   smallest count at the top, largest count at the bottom, left edge aligned.
   Values update discretely every 5-7s with slot-number motion, then rest.
   ───────────────────────────────────────────── */

import { useEffect, useRef, useState } from 'react';
import { motion } from 'framer-motion';
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
const DASHBOARD_MIN_UPDATE_MS = 5_000;
const DASHBOARD_MAX_UPDATE_MS = 7_000;
const ROW_NUMBER_CLASS = 'text-[clamp(40px,3vw,50px)]';

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

function nextDashboardUpdateDelay() {
  return DASHBOARD_MIN_UPDATE_MS + Math.round(Math.random() * (DASHBOARD_MAX_UPDATE_MS - DASHBOARD_MIN_UPDATE_MS));
}

function StatRow({
  value,
  label,
  rank,
  total,
}: {
  value: number | null;
  label: string;
  rank: number;
  total: number;
}) {
  const isBottom = rank === total - 1;

  return (
    <motion.div
      layout
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ layout: { duration: 0.55, ease: APPLE_EASE }, opacity: { duration: 0.4, ease: APPLE_EASE }, y: { duration: 0.5, ease: APPLE_EASE } }}
      className={cn(
        'group relative grid min-h-[clamp(58px,7.5vh,74px)] grid-cols-1 items-end overflow-hidden border-t border-black/[0.07] py-[clamp(6px,0.8vh,10px)]',
        isBottom && 'border-b border-black/[0.07]',
      )}
    >
      <div
        className="pointer-events-none absolute left-0 top-0 h-px transition-all duration-500"
        style={{
          width: `${Math.max(14, Math.round(((rank + 1) / total) * 100))}%`,
          background: isBottom ? RED : 'rgba(225,29,72,0.34)',
        }}
      />

      <div className="relative min-w-0">
        <div className="mb-1 flex items-center sm:mb-1.5">
          <span className="text-[9px] font-black uppercase tracking-[0.22em] text-black/42 lg:text-[10px] xl:text-[11px]">
            {label}
          </span>
        </div>

        <div className={cn('font-black leading-none tracking-[-0.045em] text-black', ROW_NUMBER_CLASS)}>
          <Odometer value={value} color={INK} animateOnMount revealDelayMs={140 + rank * 70} />
        </div>
      </div>
    </motion.div>
  );
}

export default function LiveDashboard({ state, className }: { state: LivePlatformState; className?: string }) {
  const { metrics, fetchedAt } = state;
  const metricsRef = useRef(metrics);
  const fetchedAtRef = useRef(fetchedAt);
  const [dashboardValues, setDashboardValues] = useState<DashboardValues>(() => snapshotValues(metrics, fetchedAt));
  const rankedMetrics = METRICS
    .map((metric, index) => ({
      ...metric,
      index,
      value: dashboardValues[metric.key] ?? null,
    }))
    .sort((a, b) => {
      const av = a.value ?? Number.POSITIVE_INFINITY;
      const bv = b.value ?? Number.POSITIVE_INFINITY;
      return av === bv ? a.index - b.index : av - bv;
    });

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
        if (!cancelled) scheduleRest();
      }, nextDashboardUpdateDelay());
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
      <div className="mb-2.5 flex items-center gap-3 xl:mb-3 2xl:mb-4">
        <span className="relative flex h-3.5 w-3.5 items-center justify-center">
          <span className="absolute inline-flex h-full w-full animate-ping rounded-full opacity-80" style={{ background: RED }} />
          <span className="absolute h-3.5 w-3.5 rounded-full opacity-20" style={{ background: RED, boxShadow: '0 0 18px rgba(225,29,72,0.75)' }} />
          <span className="relative inline-flex h-2.5 w-2.5 rounded-full ring-2 ring-[#E11D48]/20" style={{ background: RED, boxShadow: '0 0 10px rgba(225,29,72,0.45)' }} />
        </span>
        <h2 className="text-[clamp(22px,1.8vw,32px)] font-black leading-none tracking-[-0.045em]" style={{ color: INK }}>
          Currently <span style={{ color: RED }}>feeding</span> on
        </h2>
      </div>

      {/* Ranked number pyramid — left edge fixed, value magnitude grows downward. */}
      <div className="relative">
        {rankedMetrics.map((metric, index) => (
          <StatRow
            key={metric.key}
            value={metric.value}
            label={metric.label}
            rank={index}
            total={rankedMetrics.length}
          />
        ))}
      </div>
    </div>
  );
}
