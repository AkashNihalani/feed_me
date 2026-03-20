'use client';

import { useEffect, useMemo } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { ExternalLink, Share2, X } from 'lucide-react';
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

function MetricTile({
  label,
  value,
  accent = false,
  compact = false,
}: {
  label: string;
  value: string;
  accent?: boolean;
  compact?: boolean;
}) {
  return (
    <div className={accent
      ? 'rounded-[24px] border border-[#CCFF00]/20 bg-[#CCFF00] p-4 shadow-[0_18px_36px_rgba(204,255,0,0.2),inset_0_1px_0_rgba(255,255,255,0.85)] dark:border-[#CCFF00]/10'
      : 'rounded-[24px] border border-black/8 bg-white/[0.46] p-4 shadow-[0_20px_42px_rgba(0,0,0,0.12),inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/8 dark:bg-black/38 dark:shadow-[0_18px_42px_rgba(0,0,0,0.4),inset_0_1px_0_rgba(255,255,255,0.06)]'}>
      <div className={accent
        ? 'text-[10px] font-black uppercase tracking-[0.18em] text-black/55'
        : 'text-[10px] font-black uppercase tracking-[0.18em] text-black/42 dark:text-white/34'}>
        {label}
      </div>
      <div className={accent
        ? `mt-3 ${compact ? 'text-[clamp(22px,2.4vw,32px)]' : 'text-[clamp(24px,3vw,38px)]'} font-black leading-[0.9] tracking-[-0.04em] text-black`
        : `mt-3 ${compact ? 'text-[clamp(18px,2vw,28px)]' : 'text-[clamp(20px,2.4vw,34px)]'} font-black leading-[0.92] tracking-[-0.04em] text-black dark:text-white`}>
        {value}
      </div>
    </div>
  );
}

function MetricPill({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-[20px] border border-black/8 bg-white/[0.42] px-4 py-3 shadow-[0_16px_32px_rgba(0,0,0,0.1),inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/8 dark:bg-white/[0.05] dark:shadow-[0_16px_34px_rgba(0,0,0,0.34),inset_0_1px_0_rgba(255,255,255,0.06)]">
      <div className="text-[9px] font-black uppercase tracking-[0.18em] text-black/42 dark:text-white/34">{label}</div>
      <div className="mt-2 text-[24px] font-black leading-[0.92] tracking-[-0.04em] text-black dark:text-white">{value}</div>
    </div>
  );
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
    const position = asRecord(payload.position);
    const timing = asRecord(payload.timing);
    const trajectory = asRecord(payload.trajectory);
    const bestMetric = (text(payload.best_metric) || item.metricKey || 'views').toUpperCase();
    const bestMetricObj = asRecord(metrics[bestMetric.toLowerCase()]);
    const value = num(bestMetricObj.value) ?? item.metricValue;
    const baseline = num(bestMetricObj.baseline);
    const multiple = num(bestMetricObj.multiple);
    const feedRank = num(bestMetricObj.rank_feed) ?? num(position.feed_rank);
    const feedPercent = num(bestMetricObj.feed_percentile) ?? num(position.feed_percentile) ?? num(bestMetricObj.percentile) ?? item.surfacePercentile;
    const feederRank = num(position.feeder_rank) ?? num(position.rank_overall) ?? num(position.rank_all_time);
    const bestInLastN = num(bestMetricObj.best_in_last_n);
    const hour = num(timing.hour);
    const hourPct = num(timing.hour_percentile);
    const hourMult = num(timing.hour_multiple);
    const d1 = num(trajectory.d1);
    const d3 = num(trajectory.d3);
    const d7 = num(trajectory.d7);
    const delta = num(trajectory.delta) ?? item.trajectoryDeltaPercentile;
    const supportMetrics = ['views', 'likes', 'comments']
      .filter((metric) => metric !== bestMetric.toLowerCase())
      .map((metric) => {
        const metricObj = asRecord(metrics[metric]);
        const metricMultiple = num(metricObj.multiple);
        return {
          key: metric,
          label: metric === 'views' ? 'View Rate' : metric === 'likes' ? 'Like Rate' : 'Comment Rate',
          value: metricMultiple == null ? 'x--' : `${metricMultiple.toFixed(2)}x`,
        };
      });

    return {
      bestMetric,
      value,
      baseline,
      multiple,
      feedRank,
      feedPercent,
      feederRank,
      bestInLastN,
      hour,
      hourPct,
      hourMult,
      d1,
      d3,
      d7,
      delta,
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
          transition={{ duration: 0.22, ease: [0.22, 1, 0.36, 1] }}
          onClick={onClose}
        >
          <div className="absolute inset-0 bg-black/30 backdrop-blur-[20px] dark:bg-black/54" />

          <motion.div
            initial={{ opacity: 0, y: 20, scale: 0.97 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 18, scale: 0.97 }}
            transition={{ duration: 0.24, ease: [0.22, 1, 0.36, 1] }}
            className="relative z-10 flex w-full max-w-[920px] flex-col overflow-hidden rounded-[34px] border border-white/85 bg-[rgba(255,255,255,0.52)] text-black shadow-[0_42px_120px_rgba(0,0,0,0.26),inset_0_1px_0_rgba(255,255,255,0.86)] backdrop-blur-[54px] backdrop-saturate-[190%] dark:border-white/10 dark:bg-[rgba(8,8,8,0.52)] dark:text-white dark:shadow-[0_42px_120px_rgba(0,0,0,0.58),inset_0_1px_0_rgba(255,255,255,0.08)]"
            style={{ width: 'min(920px, calc(100vw - 5rem))', maxHeight: 'min(700px, calc(100vh - 5rem))' }}
          >
            {item.thumbnailUrl ? (
              <div className="pointer-events-none absolute inset-0">
                <img src={item.thumbnailUrl} alt="cover" className="h-full w-full scale-[1.06] object-cover opacity-12 dark:opacity-10" />
                <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_left,rgba(204,255,0,0.16),transparent_34%),radial-gradient(circle_at_top_right,rgba(255,255,255,0.22),transparent_28%),linear-gradient(180deg,rgba(255,255,255,0.18),rgba(255,255,255,0.54))] dark:bg-[radial-gradient(circle_at_top_left,rgba(204,255,0,0.14),transparent_32%),radial-gradient(circle_at_top_right,rgba(255,255,255,0.06),transparent_26%),linear-gradient(180deg,rgba(0,0,0,0.16),rgba(0,0,0,0.82))]" />
              </div>
            ) : (
              <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_left,rgba(204,255,0,0.16),transparent_34%),radial-gradient(circle_at_top_right,rgba(255,255,255,0.22),transparent_28%),linear-gradient(180deg,rgba(255,255,255,0.32),rgba(255,255,255,0.56))] dark:bg-[radial-gradient(circle_at_top_left,rgba(204,255,0,0.12),transparent_35%),radial-gradient(circle_at_top_right,rgba(255,255,255,0.05),transparent_24%),linear-gradient(180deg,rgba(15,15,15,0.72),rgba(0,0,0,0.92))]" />
            )}

            <div className="relative z-10 grid h-full grid-rows-[auto_1fr_auto] gap-5 px-7 pb-7 pt-7">
            <div className="flex items-start justify-between gap-5">
              <div className="max-w-[560px]">
                <div className="text-[11px] font-black uppercase tracking-[0.22em] text-black/55 dark:text-[#CCFF00]/90">
                  Intelligence Window
                </div>
                <div className="mt-3 text-[clamp(40px,4.8vw,66px)] font-black leading-[0.9] tracking-[-0.06em] text-black dark:text-white">
                  {compact(stats.value)} {stats.bestMetric}
                </div>
                <div className="mt-4 flex flex-wrap items-center gap-3">
                  <span className="rounded-full bg-[#CCFF00] px-4 py-2 text-[13px] font-black uppercase tracking-[0.12em] text-black shadow-[0_10px_24px_rgba(204,255,0,0.18)]">
                    {stats.multiple == null ? '--' : `${stats.multiple.toFixed(2)}x`} multiple
                  </span>
                  <span className="text-[20px] font-black tracking-[-0.03em] text-black/68 dark:text-white/76">
                    {compact(stats.baseline)} usual
                  </span>
                </div>
                <div className="mt-5 flex flex-wrap items-center gap-2">
                  <span className="rounded-[12px] border border-black/8 bg-white/[0.42] px-3 py-1.5 text-[10px] font-black uppercase tracking-[0.16em] text-black/72 shadow-[0_12px_28px_rgba(0,0,0,0.08),inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/10 dark:bg-white/[0.06] dark:text-white/72 dark:shadow-[0_14px_30px_rgba(0,0,0,0.28),inset_0_1px_0_rgba(255,255,255,0.06)]">
                    {stats.handle}
                  </span>
                  <span className="rounded-[12px] border border-black/8 bg-white/[0.42] px-3 py-1.5 text-[10px] font-black uppercase tracking-[0.16em] text-black/58 shadow-[0_12px_28px_rgba(0,0,0,0.08),inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/10 dark:bg-white/[0.06] dark:text-white/54 dark:shadow-[0_14px_30px_rgba(0,0,0,0.28),inset_0_1px_0_rgba(255,255,255,0.06)]">
                    {stats.mediaType}
                  </span>
                  <span className="rounded-[12px] border border-black/8 bg-white/[0.42] px-3 py-1.5 text-[10px] font-black uppercase tracking-[0.16em] text-black/58 shadow-[0_12px_28px_rgba(0,0,0,0.08),inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/10 dark:bg-white/[0.06] dark:text-white/54 dark:shadow-[0_14px_30px_rgba(0,0,0,0.28),inset_0_1px_0_rgba(255,255,255,0.06)]">
                    {stats.checkpoint}
                  </span>
                </div>
              </div>

              <div className="flex items-center gap-3">
                <button
                  type="button"
                  onClick={() => {
                    if (!item.postUrl) return;
                    navigator.clipboard?.writeText(item.postUrl).catch(() => undefined);
                  }}
                  className="flex h-11 w-11 items-center justify-center rounded-full border border-black/8 bg-white/[0.42] text-black/48 shadow-[0_12px_26px_rgba(0,0,0,0.12),inset_0_1px_0_rgba(255,255,255,0.78)] transition-colors dark:border-white/10 dark:bg-white/[0.06] dark:text-white/46 dark:shadow-[0_10px_24px_rgba(0,0,0,0.34),inset_0_1px_0_rgba(255,255,255,0.06)]"
                >
                  <Share2 size={18} />
                </button>
                <button
                  type="button"
                  onClick={onClose}
                  className="flex h-11 w-11 items-center justify-center rounded-full border border-black/8 bg-white/[0.42] text-black/48 shadow-[0_12px_26px_rgba(0,0,0,0.12),inset_0_1px_0_rgba(255,255,255,0.78)] transition-colors dark:border-white/10 dark:bg-white/[0.06] dark:text-white/46 dark:shadow-[0_10px_24px_rgba(0,0,0,0.34),inset_0_1px_0_rgba(255,255,255,0.06)]"
                >
                  <X size={18} />
                </button>
              </div>
            </div>

            <div className="grid min-h-0 grid-cols-12 gap-4">
              <div className="col-span-4 grid auto-rows-fr grid-cols-2 gap-4">
                <MetricTile
                  label="Performance"
                  value={stats.bestInLastN == null ? 'BEST IN -- POSTS' : `BEST IN ${Math.max(1, Math.round(stats.bestInLastN))} POSTS`}
                  compact
                />
                <MetricTile
                  label="Feed Rank"
                  value={
                    stats.feedRank == null
                      ? '--'
                      : `#${Math.round(stats.feedRank)}${stats.feedPercent == null ? '' : ` · ${Math.round(stats.feedPercent)}%`}`
                  }
                  compact
                />
                <MetricTile label="Feeder Rank" value={stats.feederRank == null ? '--' : `#${Math.round(stats.feederRank)}`} accent compact />
              </div>

              <div className="col-span-5 flex flex-col rounded-[28px] border border-black/8 bg-white/[0.42] p-4 shadow-[0_22px_46px_rgba(0,0,0,0.12),inset_0_1px_0_rgba(255,255,255,0.76)] backdrop-blur-[24px] dark:border-white/8 dark:bg-black/34 dark:shadow-[0_18px_42px_rgba(0,0,0,0.35),inset_0_1px_0_rgba(255,255,255,0.06)]">
                <div className="flex items-center justify-between">
                  <div className="text-[10px] font-black uppercase tracking-[0.18em] text-black/42 dark:text-white/34">
                    {stats.isD1 ? 'Timing' : 'Trajectory'}
                  </div>
                  {stats.delta != null && (
                    <div className="rounded-full bg-[#CCFF00]/18 px-3 py-1 text-[10px] font-black uppercase tracking-[0.16em] text-black/72 dark:bg-[#CCFF00]/12 dark:text-[#CCFF00]">
                      {stats.delta <= 0 ? '+' : '-'}{Math.abs(Math.round(stats.delta))} trending
                    </div>
                  )}
                </div>

                <div className="mt-4 grid flex-1 grid-cols-3 gap-3">
                  {stats.isD1 ? (
                    <>
                      <div className="rounded-[22px] border border-black/8 bg-white/[0.4] px-4 py-4 shadow-[0_12px_28px_rgba(0,0,0,0.08),inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/8 dark:bg-white/[0.04] dark:shadow-[0_10px_22px_rgba(0,0,0,0.22),inset_0_1px_0_rgba(255,255,255,0.05)]">
                        <div className="text-[10px] font-black uppercase tracking-[0.16em] text-black/42 dark:text-white/34">Posting Hour</div>
                        <div className="mt-3 text-[26px] font-black tracking-[-0.04em] text-black dark:text-white">
                          {stats.hour == null ? '--:--' : `${String(Math.round(stats.hour)).padStart(2, '0')}:00`}
                        </div>
                      </div>
                      <div className="rounded-[22px] border border-black/8 bg-white/[0.4] px-4 py-4 shadow-[0_12px_28px_rgba(0,0,0,0.08),inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/8 dark:bg-white/[0.04] dark:shadow-[0_10px_22px_rgba(0,0,0,0.22),inset_0_1px_0_rgba(255,255,255,0.05)]">
                        <div className="text-[10px] font-black uppercase tracking-[0.16em] text-black/42 dark:text-white/34">Hour Percentile</div>
                        <div className="mt-3 text-[26px] font-black tracking-[-0.04em] text-black dark:text-white">
                          {stats.hourPct == null ? 'P--' : `P${Math.round(stats.hourPct)}`}
                        </div>
                      </div>
                      <div className="rounded-[22px] border border-black/8 bg-white/[0.4] px-4 py-4 shadow-[0_12px_28px_rgba(0,0,0,0.08),inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/8 dark:bg-white/[0.04] dark:shadow-[0_10px_22px_rgba(0,0,0,0.22),inset_0_1px_0_rgba(255,255,255,0.05)]">
                        <div className="text-[10px] font-black uppercase tracking-[0.16em] text-black/42 dark:text-white/34">Hour Multiple</div>
                        <div className="mt-3 text-[26px] font-black tracking-[-0.04em] text-black dark:text-white">
                          {stats.hourMult == null ? 'x--' : `x${stats.hourMult.toFixed(2)}`}
                        </div>
                      </div>
                    </>
                  ) : (
                    <>
                      {[
                        { label: 'D1', value: stats.d1 },
                        { label: 'D3', value: stats.d3 },
                        { label: 'D7', value: stats.d7 },
                      ].map((point) => (
                        <div key={point.label} className="rounded-[22px] border border-black/8 bg-white/[0.4] px-4 py-4 shadow-[0_12px_28px_rgba(0,0,0,0.08),inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/8 dark:bg-white/[0.04] dark:shadow-[0_10px_22px_rgba(0,0,0,0.22),inset_0_1px_0_rgba(255,255,255,0.05)]">
                          <div className="text-[10px] font-black uppercase tracking-[0.16em] text-black/42 dark:text-white/34">{point.label}</div>
                          <div className="mt-3 text-[26px] font-black tracking-[-0.04em] text-black dark:text-white">
                            {point.value == null ? '--' : Math.round(point.value)}
                          </div>
                        </div>
                      ))}
                    </>
                  )}
                </div>
                <div className="mt-3 grid grid-cols-2 gap-3">
                  <MetricPill label="Signal Baseline" value={compact(stats.baseline)} />
                  <MetricPill label="Signal Multiple" value={stats.multiple == null ? 'x--' : `${stats.multiple.toFixed(2)}x`} />
                </div>
              </div>

              <div className="col-span-3 grid auto-rows-fr gap-4">
                {stats.supportMetrics.map((metric, index) => (
                  <MetricTile
                    key={metric.key}
                    label={metric.label}
                    value={metric.value}
                    accent={index === 0}
                    compact
                  />
                ))}
              </div>
            </div>

            <button
              type="button"
              onClick={(event) => {
                event.stopPropagation();
                if (item.postUrl) window.open(item.postUrl, '_blank', 'noreferrer');
              }}
              className="flex w-full items-center justify-center gap-3 rounded-full bg-[#CCFF00] px-6 py-4 text-[14px] font-black uppercase tracking-[0.22em] text-black shadow-[0_22px_40px_rgba(204,255,0,0.16),inset_0_1px_0_rgba(255,255,255,0.8)] transition-transform hover:scale-[0.995] active:scale-[0.99]"
            >
              Open Post
              <ExternalLink size={18} />
            </button>
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
