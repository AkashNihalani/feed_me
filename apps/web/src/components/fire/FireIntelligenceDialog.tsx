'use client';

import { useEffect, useMemo } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { ExternalLink } from 'lucide-react';
import { FireItem } from './types';
import { compact } from './fireLogicHelpers';

type FireIntelligenceDialogProps = {
  item: FireItem | null;
  onClose: () => void;
};

function asRecord(v: unknown): Record<string, unknown> {
  return v && typeof v === 'object' && !Array.isArray(v) ? (v as Record<string, unknown>) : {};
}

function num(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const next = Number(v.replace('%', '').trim());
    return Number.isFinite(next) ? next : null;
  }
  return null;
}

function text(v: unknown): string {
  return typeof v === 'string' ? v : '';
}

function compactOrDash(v: number | null): string {
  return v == null || !Number.isFinite(v) ? '--' : compact(v);
}

function multipleOrDash(v: number | null): string {
  return v == null || !Number.isFinite(v) ? '--' : `${v.toFixed(2)}x`;
}

function topPercentOrDash(v: number | null): string {
  return v == null || !Number.isFinite(v) ? '--' : `Top ${Math.round(v)}%`;
}

function hourAmPm(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return '--';
  const normalized = ((Math.round(v) % 24) + 24) % 24;
  const suffix = normalized >= 12 ? 'PM' : 'AM';
  const twelve = normalized % 12 === 0 ? 12 : normalized % 12;
  return `${twelve} ${suffix}`;
}

function metricLabel(metric: string): string {
  if (metric === 'views') return 'Views';
  if (metric === 'likes') return 'Likes';
  if (metric === 'comments') return 'Comments';
  return metric.toUpperCase();
}

function signedShift(delta: number | null): string {
  if (delta == null || !Number.isFinite(delta)) return '--';
  const rounded = Math.round(delta);
  if (rounded > 0) return `+${rounded}`;
  if (rounded < 0) return `${rounded}`;
  return '0';
}

function shiftTone(delta: number | null): { label: string } {
  if (delta == null || !Number.isFinite(delta) || Math.round(delta) === 0) return { label: 'Flat' };
  return delta > 0 ? { label: 'Improving' } : { label: 'Cooling' };
}

function latestTrajectoryPoint(points: Array<number | null>): number | null {
  for (let index = points.length - 1; index >= 0; index -= 1) {
    if (points[index] != null && Number.isFinite(points[index] as number)) {
      return points[index];
    }
  }
  return null;
}

function firstTrajectoryPoint(points: Array<number | null>): number | null {
  for (const point of points) {
    if (point != null && Number.isFinite(point)) return point;
  }
  return null;
}

/* ── Subcomponents ── */

function MetaBadge({ value }: { value: string }) {
  return (
    <span className="rounded-full border border-white/[0.12] bg-black/40 px-2.5 py-1 text-[9px] font-bold uppercase tracking-[0.14em] text-white/80 backdrop-blur-sm dark:border-white/[0.12] dark:bg-black/40 dark:text-white/80">
      {value}
    </span>
  );
}

function SectionTag({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-[9px] font-bold uppercase tracking-[0.2em] text-neutral-400 dark:text-white/36">
      {children}
    </div>
  );
}

function SupportMetricRow({
  label,
  value,
  multiple,
  accent = false,
}: {
  label: string;
  value: string;
  multiple: string;
  accent?: boolean;
}) {
  return (
    <div className="flex items-center justify-between gap-3 rounded-2xl border border-neutral-200/80 bg-neutral-50/60 px-4 py-3 dark:border-white/[0.06] dark:bg-white/[0.025]">
      <div className="min-w-0">
        <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-neutral-400 dark:text-white/36">
          {label}
        </div>
        <div className="mt-0.5 text-[13px] font-semibold tabular-nums text-neutral-600 dark:text-white/60">
          {value}
        </div>
      </div>
      <div
        className={
          accent
            ? 'text-[26px] font-black leading-none tracking-[-0.04em] text-black dark:text-[#CCFF00]'
            : 'text-[26px] font-black leading-none tracking-[-0.04em] text-neutral-800 dark:text-white/90'
        }
      >
        {multiple}
      </div>
    </div>
  );
}

function CompactStat({
  label,
  value,
  accent = false,
}: {
  label: string;
  value: string;
  accent?: boolean;
}) {
  return (
    <div className="rounded-2xl border border-neutral-200/80 bg-neutral-50/60 px-3.5 py-3 dark:border-white/[0.06] dark:bg-white/[0.025]">
      <div className="text-[9px] font-bold uppercase tracking-[0.18em] text-neutral-400 dark:text-white/36">
        {label}
      </div>
      <div
        className={
          accent
            ? 'mt-1.5 text-[20px] font-black leading-none tracking-[-0.03em] text-black dark:text-[#CCFF00]'
            : 'mt-1.5 text-[20px] font-black leading-none tracking-[-0.03em] text-neutral-800 dark:text-white/90'
        }
      >
        {value}
      </div>
    </div>
  );
}

function TrajectoryBadge({ delta }: { delta: number | null }) {
  const tone = shiftTone(delta);
  // Flat
  if (delta == null || !Number.isFinite(delta) || Math.round(delta) === 0) {
    return <span className="text-[10px] font-bold uppercase tracking-[0.14em] text-neutral-400 dark:text-white/50">{tone.label}</span>;
  }
  // Improving
  if (delta > 0) {
    return <span className="text-[10px] font-bold uppercase tracking-[0.14em] text-emerald-600 dark:text-[#CCFF00]">{tone.label}</span>;
  }
  // Cooling
  return <span className="text-[10px] font-bold uppercase tracking-[0.14em] text-orange-500 dark:text-[#ff8a65]">{tone.label}</span>;
}

export default function FireIntelligenceDialog({ item, onClose }: FireIntelligenceDialogProps) {
  useEffect(() => {
    if (!item) return;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [item, onClose]);

  const stats = useMemo(() => {
    if (!item) return null;
    const payload = asRecord(item.payload);
    const metrics = asRecord(payload.metrics);
    const timing = asRecord(payload.timing);
    const trajectory = asRecord(payload.trajectory);
    const bestMetric = (text(payload.best_metric) || item.metricKey || 'views').toUpperCase();
    const bestMetricObj = asRecord(metrics[bestMetric.toLowerCase()]);
    const value = num(bestMetricObj.value) ?? item.metricValue;
    const baseline = num(bestMetricObj.baseline);
    const multiple = num(bestMetricObj.multiple);
    const bestInLastN = num(bestMetricObj.best_in_last_n);
    const hour = num(timing.hour);
    const hourPct = num(timing.hour_percentile);
    const hourMult = num(timing.hour_multiple);
    const d1 = num(trajectory.d1);
    const d3 = num(trajectory.d3);
    const d7 = num(trajectory.d7);
    const d21 = num(trajectory.d21);
    const delta = num(trajectory.delta) ?? item.trajectoryDeltaPercentile;
    const trajectoryPoints = [d1, d3, d7, d21];
    const currentTrajectory = latestTrajectoryPoint(trajectoryPoints) ?? item.surfacePercentile;
    const firstTrajectory = firstTrajectoryPoint(trajectoryPoints);
    const supportMetrics = ['views', 'likes', 'comments']
      .filter((metric) => metric !== bestMetric.toLowerCase())
      .map((metric) => {
        const metricObj = asRecord(metrics[metric]);
        const metricMultiple = num(metricObj.multiple);
        const metricValue = num(metricObj.value);
        return {
          key: metric,
          label: metricLabel(metric),
          multiple: multipleOrDash(metricMultiple),
          value: compactOrDash(metricValue),
        };
      });

    return {
      bestMetric,
      value,
      baseline,
      multiple,
      bestInLastN,
      hour,
      hourPct,
      hourMult,
      d1,
      d3,
      d7,
      d21,
      delta,
      currentTrajectory,
      firstTrajectory,
      supportMetrics,
      checkpoint: item.checkpoint.toUpperCase(),
      handle: `@${(item.surfaceHandle || 'FEEDER').replace(/^@+/, '').toUpperCase()}`,
      mediaType: (item.surfaceMediaType || 'POST').toUpperCase(),
      isD1: item.checkpoint.toUpperCase() === 'D1',
    };
  }, [item]);

  return (
    <AnimatePresence>
      {item && stats && (
        <motion.div
          className="fixed inset-0 z-[260] hidden items-center justify-center px-8 py-8 lg:flex"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.18, ease: [0.22, 1, 0.36, 1] }}
          onClick={onClose}
        >
          {/* Backdrop — dark overlay only, no blur */}
          <motion.div
            className="absolute inset-0 bg-black/60 dark:bg-black/72"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
          />

          {/* Dialog */}
          <motion.div
            initial={{ opacity: 0, y: 16, scale: 0.98 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 10, scale: 0.98 }}
            transition={{ duration: 0.22, ease: [0.22, 1, 0.36, 1] }}
            className="relative z-10 overflow-hidden rounded-3xl border border-neutral-200/60 shadow-2xl dark:border-white/[0.08] dark:shadow-[0_30px_80px_rgba(0,0,0,0.6)]"
            style={{ width: 'min(800px, calc(100vw - 4rem))', maxHeight: 'min(600px, calc(100vh - 4rem))' }}
          >
            <div className="grid h-full min-h-[520px] grid-cols-[320px_minmax(0,1fr)]">
              {/* ── Left: Thumbnail Panel (clear view) ── */}
              <div className="relative min-h-[520px] overflow-hidden bg-black">
                {item.thumbnailUrl ? (
                  <img
                    src={item.thumbnailUrl}
                    alt=""
                    className="absolute inset-0 h-full w-full object-cover"
                  />
                ) : (
                  <div className="absolute inset-0 bg-[radial-gradient(circle_at_30%_20%,rgba(204,255,0,0.14),transparent_50%),linear-gradient(180deg,#161616_0%,#050505_100%)]" />
                )}
                {/* Bottom gradient for legibility */}
                <div className="absolute inset-0 bg-[linear-gradient(180deg,transparent_0%,transparent_30%,rgba(0,0,0,0.7)_70%,rgba(0,0,0,0.92)_100%)]" />

                {/* Bottom content over thumbnail */}
                <div className="absolute inset-x-0 bottom-0 z-10 p-5">
                  <div className="flex flex-wrap gap-1.5">
                    <MetaBadge value={stats.handle} />
                    <MetaBadge value={stats.mediaType} />
                    <MetaBadge value={stats.checkpoint} />
                  </div>
                  <div className="mt-3 text-[36px] font-black leading-[0.9] tracking-[-0.04em] text-white">
                    {compactOrDash(stats.value)}
                  </div>
                  <div className="mt-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-white/50">
                    {stats.bestMetric}
                  </div>
                </div>
              </div>

              {/* ── Right: Intelligence Panel — frosted glass over thumbnail bleed ── */}
              <div className="relative flex min-h-[520px] flex-col overflow-hidden bg-white/72 backdrop-blur-2xl dark:bg-black/72">
                <div className="pointer-events-none absolute inset-0 overflow-hidden">
                  {item.thumbnailUrl ? (
                    <>
                      <img
                        src={item.thumbnailUrl}
                        alt=""
                        className="absolute inset-0 h-full w-full scale-110 object-cover blur-2xl opacity-18 dark:opacity-14"
                      />
                      <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(255,255,255,0.58),rgba(255,255,255,0.82))] dark:bg-[linear-gradient(180deg,rgba(8,8,8,0.52),rgba(8,8,8,0.82))]" />
                    </>
                  ) : (
                    <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(255,255,255,0.72),rgba(255,255,255,0.88))] dark:bg-[linear-gradient(180deg,rgba(12,12,12,0.68),rgba(12,12,12,0.84))]" />
                  )}
                </div>

                <div className="relative flex flex-1 flex-col overflow-y-auto p-6">
                  {/* Header */}
                  <div>
                    <SectionTag>Intelligence Window</SectionTag>
                    <p className="mt-1.5 text-[12px] font-medium leading-relaxed text-neutral-500 dark:text-white/40">
                      Signal vs baseline for this post
                    </p>
                  </div>

                  {/* Hero Metric — neon green base, black text */}
                  <div className="mt-5 rounded-2xl bg-[#CCFF00] px-5 py-4">
                    <div className="text-[9px] font-bold uppercase tracking-[0.2em] text-black/40">
                      Hero Metric
                    </div>
                    <div className="mt-3 flex items-end justify-between gap-4">
                      <div>
                        <div className="text-[42px] font-black leading-none tracking-[-0.05em] text-black">
                          {compactOrDash(stats.value)}
                        </div>
                        <div className="mt-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-black/44">
                          {stats.bestMetric}
                        </div>
                      </div>
                      <div className="text-right">
                        <div className="text-[30px] font-black leading-none tracking-[-0.04em] text-black">
                          {multipleOrDash(stats.multiple)}
                        </div>
                        <div className="mt-1 text-[11px] font-medium text-black/44">
                          {compactOrDash(stats.baseline)} usual
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Supporting Metrics */}
                  <div className="mt-4">
                    <div className="flex items-center justify-between gap-3">
                      <SectionTag>Supporting Metrics</SectionTag>
                      <div className="text-[9px] font-bold uppercase tracking-[0.14em] text-neutral-300 dark:text-white/24">
                        {stats.bestInLastN == null ? 'Best in -- posts' : `Best in ${Math.max(1, Math.round(stats.bestInLastN))} posts`}
                      </div>
                    </div>
                    <div className="mt-2.5 space-y-2">
                      {stats.supportMetrics.map((metric, index) => (
                        <SupportMetricRow
                          key={metric.key}
                          label={metric.label}
                          value={metric.value}
                          multiple={metric.multiple}
                          accent={index === 0}
                        />
                      ))}
                    </div>
                  </div>

                  {/* Divider */}
                  <div className="my-4 h-px w-full bg-neutral-200/80 dark:bg-white/[0.06]" />

                  {/* Timing or Trajectory */}
                  {stats.isD1 ? (
                    <div>
                      <SectionTag>Timing</SectionTag>
                      <div className="mt-2.5 grid grid-cols-3 gap-2">
                        <CompactStat label="Post Time" value={hourAmPm(stats.hour)} accent />
                        <CompactStat label="Hour %" value={stats.hourPct == null ? '--' : `Top ${Math.round(stats.hourPct)}%`} />
                        <CompactStat label="Hour Mult." value={multipleOrDash(stats.hourMult)} />
                      </div>
                    </div>
                  ) : (
                    <div>
                      <div className="flex items-center justify-between gap-3">
                        <SectionTag>Trajectory</SectionTag>
                        <TrajectoryBadge delta={stats.delta} />
                      </div>
                      <div className="mt-2.5 rounded-2xl border border-neutral-200/80 bg-neutral-50/60 px-4 py-4 dark:border-white/[0.06] dark:bg-white/[0.025]">
                        <div className="flex items-end justify-between gap-4">
                          <div>
                            <div className="text-[36px] font-black leading-none tracking-[-0.05em] text-neutral-900 dark:text-white">
                              {signedShift(stats.delta)}
                            </div>
                            <div className="mt-1 text-[11px] font-medium text-neutral-400 dark:text-white/36">
                              Shift vs first checkpoint
                            </div>
                          </div>
                          <div className="text-right">
                            <div className="text-[17px] font-black leading-none tracking-[-0.03em] text-neutral-800 dark:text-white/90">
                              {topPercentOrDash(stats.currentTrajectory)}
                            </div>
                            <div className="mt-1 text-[11px] font-medium text-neutral-400 dark:text-white/36">
                              {stats.firstTrajectory == null || stats.currentTrajectory == null ? 'Awaiting data' : 'Current position'}
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  )}

                  {/* Open Post CTA */}
                  <div className="mt-auto pt-5">
                    <button
                      type="button"
                      onClick={(event) => {
                        event.stopPropagation();
                        if (item.postUrl) window.open(item.postUrl, '_blank', 'noreferrer');
                      }}
                      className="flex w-full items-center justify-center gap-2 rounded-2xl bg-[#CCFF00] px-5 py-3 text-[11px] font-black uppercase tracking-[0.16em] text-black shadow-[0_8px_24px_rgba(204,255,0,0.12)] transition-all hover:shadow-[0_12px_32px_rgba(204,255,0,0.2)] active:scale-[0.995]"
                    >
                      Open Post
                      <ExternalLink size={14} />
                    </button>
                  </div>
                </div>
              </div>
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
