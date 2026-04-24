'use client';

import Link from 'next/link';
import { useEffect, useMemo, useRef, useState } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { ChevronLeft, ChevronRight, ExternalLink } from 'lucide-react';
import { FireItem } from './types';
import { compact } from './fireLogicHelpers';
import {
  metricLabel,
  orderedSupportMetricsFromPayload,
  resolveBestMetricFromPayload,
} from './fireMetricDisplay';
import { parseFirewatchData } from './firewatchUtils';
import { getFireSignalMeta, getPatternMechanicLabel } from '@/lib/fireSignals';

type FireIntelligenceDialogProps = {
  item: FireItem | null;
  onClose: () => void;
  onPrevious?: () => void;
  onNext?: () => void;
  canPrevious?: boolean;
  canNext?: boolean;
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

function mediaProxyUrl(postKey: string, role = 'thumbnail'): string {
  const key = postKey.trim();
  if (!key) return '';
  const params = new URLSearchParams({ postKey: key, role });
  return `/api/media?${params.toString()}`;
}

function withRetryBust(url: string, seed: string): string {
  const value = url.trim();
  if (!value) return '';
  try {
    const target = new URL(value, typeof window !== 'undefined' ? window.location.origin : 'https://feedmemore.vercel.app');
    target.searchParams.set('_retry', seed);
    const href = target.toString();
    return href.startsWith('http') && value.startsWith('/') ? `${target.pathname}${target.search}` : href;
  } catch {
    const joiner = value.includes('?') ? '&' : '?';
    return `${value}${joiner}_retry=${encodeURIComponent(seed)}`;
  }
}

function buildFeederArchiveHref(item: FireItem | null): string | null {
  if (!item || item.cardKind === 'firewatch') return null;
  const feedId = item.feedId;
  const handle = String(item.surfaceHandle || item.handle || '').replace(/^@+/, '').trim().toLowerCase();
  if (!Number.isFinite(feedId) || !handle) return null;
  return `/feed/${feedId}/feeder/${encodeURIComponent(handle)}`;
}

function compactOrDash(v: number | null): string {
  return v == null || !Number.isFinite(v) ? '--' : compact(v);
}

function percentText(v: number | null): string {
  return v == null || !Number.isFinite(v) ? '--' : `${Math.round(v)}%`;
}

function multipleOrDash(v: number | null): string {
  return v == null || !Number.isFinite(v) ? '--' : `${v.toFixed(2)}x`;
}

function liftText(v: number | null): string {
  return v == null || !Number.isFinite(v) ? '--' : `${v.toFixed(1)}x`;
}

function compactCount(v: number | null, suffix: string): string {
  return v == null || !Number.isFinite(v) ? `-- ${suffix}` : `${Math.round(v)} ${suffix}`;
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

function TrackingInfoBadge({ value }: { value: string }) {
  return (
    <span className="inline-flex items-center rounded-full border border-neutral-200/80 bg-white/70 px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-neutral-600 dark:border-white/[0.08] dark:bg-white/[0.05] dark:text-white/62">
      {value}
    </span>
  );
}

function TrackingArchivePill({
  href,
  label,
}: {
  href: string | null;
  label: string;
}) {
  const content = (
    <span className="fm-dialog-handle-pill inline-flex max-w-full items-center rounded-full border border-white/[0.18] bg-black/46 px-4 py-2 text-[11px] font-black uppercase tracking-[0.18em] text-white shadow-[0_12px_28px_rgba(0,0,0,0.28),inset_0_1px_0_rgba(255,255,255,0.24)] backdrop-blur-[20px] dark:border-white/[0.16] dark:bg-black/52">
      <span className="relative z-10 block max-w-[180px] truncate sm:max-w-[220px]">
        {label}
      </span>
    </span>
  );

  if (!href) return content;

  return (
    <Link href={href} className="shrink-0" onClick={(event) => event.stopPropagation()}>
      {content}
    </Link>
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
            ? 'text-[26px] font-black leading-none tracking-[-0.04em] text-[#E11D48]'
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
            ? 'mt-1.5 text-[20px] font-black leading-none tracking-[-0.03em] text-black dark:text-[#E11D48]'
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
    return <span className="text-[10px] font-bold uppercase tracking-[0.14em] text-emerald-600 dark:text-[#E11D48]">{tone.label}</span>;
  }
  // Cooling
  return <span className="text-[10px] font-bold uppercase tracking-[0.14em] text-orange-500 dark:text-[#ff8a65]">{tone.label}</span>;
}

export default function FireIntelligenceDialog({
  item,
  onClose,
  onPrevious,
  onNext,
  canPrevious = false,
  canNext = false,
}: FireIntelligenceDialogProps) {
  const [previewReady, setPreviewReady] = useState(false);
  const [previewFailed, setPreviewFailed] = useState(false);
  const [previewPlaying, setPreviewPlaying] = useState(false);
  const [useThumbnailFallback, setUseThumbnailFallback] = useState(false);
  const [thumbnailRetrySeed, setThumbnailRetrySeed] = useState(0);
  const [usePreviewFallback, setUsePreviewFallback] = useState(false);
  const [previewRetrySeed, setPreviewRetrySeed] = useState(0);
  const previewRef = useRef<HTMLVideoElement | null>(null);
  const previewSessionRef = useRef(0);
  const previewRetryTimeoutRef = useRef<number | null>(null);

  useEffect(() => {
    if (!item) return;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
      if (event.key === 'ArrowLeft' && canPrevious) {
        event.preventDefault();
        onPrevious?.();
      }
      if (event.key === 'ArrowRight' && canNext) {
        event.preventDefault();
        onNext?.();
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [item, onClose, onPrevious, onNext, canPrevious, canNext]);

  const stats = useMemo(() => {
    if (!item) return null;
    const payload = asRecord(item.payload);
    const metrics = asRecord(payload.metrics);
    const timing = asRecord(payload.timing);
    const trajectory = asRecord(payload.trajectory);
    const meta = asRecord(payload.meta);
    const bestMetric = resolveBestMetricFromPayload(
      metrics,
      text(payload.best_metric) || item.metricKey || 'views',
      item.surfaceMediaType || item.mediaType,
    );
    const bestMetricObj = asRecord(metrics[bestMetric]);
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
    const supportMetrics = orderedSupportMetricsFromPayload(
      metrics,
      bestMetric,
      item.surfaceMediaType || item.mediaType,
    ).map((metric) => ({
      key: metric.key,
      label: metricLabel(metric.key),
      multiple: multipleOrDash(metric.multiple),
      value: compactOrDash(metric.value),
    }));
    const patternAlerts = Array.isArray(meta.pattern_alerts)
      ? meta.pattern_alerts
        .map((entry) => asRecord(entry))
        .map((entry) => {
          const signalCode = text(entry.signal_code);
          const signalMeta = getFireSignalMeta(signalCode);
          const patternName = text(entry.pattern_name);
          const cues = Array.isArray(entry.cues)
            ? entry.cues
              .map((cue) => asRecord(cue))
              .map((cue) => text(cue.label))
              .filter(Boolean)
              .slice(0, 4)
            : [];
          const supportPosts = Array.isArray(entry.support_posts)
            ? entry.support_posts
              .map((post) => asRecord(post))
              .map((post) => ({
                postKey: text(post.post_key),
                handle: text(post.handle),
                mediaType: text(post.media_type).toUpperCase(),
                postUrl: text(post.post_url),
                thumbnailUrl: text(post.thumbnail_url),
              }))
              .filter((post) => post.postKey || post.postUrl)
            : [];
          return {
            signalCode,
            signalLabel: signalMeta?.shortLabel || signalCode || 'Pattern Alert',
            patternLabel: getPatternMechanicLabel(patternName) || signalMeta?.headline || signalCode || 'Pattern Alert',
            context: text(entry.context).toUpperCase() || 'OWN',
            alertType: text(entry.alert_type).toUpperCase() || 'WATCH',
            body: text(entry.body),
            percentile: num(entry.surface_percentile),
            matchCount: num(entry.match_count),
            feedersCount: num(entry.feeders_count),
            avgHotPercentile: num(entry.avg_hot_percentile),
            baselineShare: num(entry.baseline_share),
            recentLift: num(entry.recent_lift),
            contrastGap: num(entry.contrast_gap),
            anchorAvgPercentile: num(entry.anchor_avg_percentile),
            anchorGap: num(entry.anchor_gap),
            cues,
            supportPosts,
          };
        })
        .filter((entry) => entry.signalCode || entry.body || entry.patternLabel)
      : [];

    return {
      bestMetric: metricLabel(bestMetric),
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
      patternAlerts,
      hideSignalChrome: item.hideSignalChrome === true,
    };
  }, [item]);

  const firewatch = useMemo(
    () => (item && item.cardKind === 'firewatch' ? parseFirewatchData(item) : null),
    [item],
  );

  const previewUrl = (item?.previewUrl || '').trim();
  const directThumbnailUrl = (item?.thumbnailUrl || '').trim();
  const thumbnailFallbackUrl = mediaProxyUrl(item?.postKey || '');
  const previewFallbackUrl = mediaProxyUrl(item?.postKey || '', 'preview_5s');
  const thumbnailBaseUrl = useThumbnailFallback
    ? (thumbnailFallbackUrl || directThumbnailUrl)
    : (directThumbnailUrl || thumbnailFallbackUrl);
  const thumbnailUrl = useMemo(() => {
    if (!thumbnailBaseUrl) return '';
    return thumbnailRetrySeed > 0
      ? withRetryBust(thumbnailBaseUrl, `${item?.id || 'fire'}-thumb-${thumbnailRetrySeed}`)
      : thumbnailBaseUrl;
  }, [item?.id, thumbnailBaseUrl, thumbnailRetrySeed]);
  const resolvedPreviewUrl = useMemo(() => {
    const baseUrl = usePreviewFallback ? (previewFallbackUrl || previewUrl) : (previewUrl || previewFallbackUrl);
    if (!baseUrl) return '';
    return previewRetrySeed > 0
      ? withRetryBust(baseUrl, `${item?.id || 'fire'}-preview-${previewRetrySeed}`)
      : baseUrl;
  }, [item?.id, previewFallbackUrl, previewRetrySeed, previewUrl, usePreviewFallback]);
  const canSwitchToPreviewFallback = Boolean(
    previewFallbackUrl
    && previewUrl
    && previewUrl !== previewFallbackUrl,
  );
  const canPreview = Boolean(
    item
    && resolvedPreviewUrl
    && ((item.surfaceMediaType || item.mediaType || '').toUpperCase() === 'REEL'),
  );
  const shouldRenderPreview = canPreview && !previewFailed;
  const trackingArchiveHref = useMemo(() => buildFeederArchiveHref(item), [item]);

  useEffect(() => {
    if (previewRetryTimeoutRef.current) clearTimeout(previewRetryTimeoutRef.current);
    let cancelled = false;
    queueMicrotask(() => {
      if (cancelled) return;
      setPreviewReady(false);
      setPreviewFailed(false);
      setPreviewPlaying(false);
      setUseThumbnailFallback(false);
      setThumbnailRetrySeed(0);
      setUsePreviewFallback(false);
      setPreviewRetrySeed(0);
    });
    return () => {
      cancelled = true;
    };
  }, [item?.id, previewUrl, directThumbnailUrl]);

  useEffect(() => {
    previewSessionRef.current += 1;
    if (previewRetryTimeoutRef.current) {
      clearTimeout(previewRetryTimeoutRef.current);
      previewRetryTimeoutRef.current = null;
    }
  }, [item?.id, resolvedPreviewUrl]);

  useEffect(() => () => {
    if (previewRetryTimeoutRef.current) clearTimeout(previewRetryTimeoutRef.current);
  }, []);

  useEffect(() => {
    const video = previewRef.current;
    if (!item || !video || !canPreview || !resolvedPreviewUrl) return undefined;
    video.load();
    const frame = window.requestAnimationFrame(() => {
      if (previewRef.current !== video) return;
      if (video.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA) {
        setPreviewReady(true);
        setPreviewFailed(false);
      }
    });
    return () => window.cancelAnimationFrame(frame);
  }, [item, canPreview, resolvedPreviewUrl]);

  useEffect(() => {
    const video = previewRef.current;
    if (!item || !video || !canPreview || previewFailed) return;
    if (!previewReady) return;
    const session = previewSessionRef.current;
    let cancelled = false;
    let playRetried = false;

    const attemptPlayback = () => {
      if (cancelled || session !== previewSessionRef.current || previewRef.current !== video) return;
      const playPromise = video.play();
      if (playPromise && typeof playPromise.catch === 'function') {
        playPromise.catch((error) => {
          if (cancelled || session !== previewSessionRef.current || previewRef.current !== video) return;
          const errorName = error instanceof DOMException ? error.name : '';
          if (errorName === 'AbortError') return;
          if (!playRetried) {
            playRetried = true;
            previewRetryTimeoutRef.current = window.setTimeout(() => {
              previewRetryTimeoutRef.current = null;
              attemptPlayback();
            }, 140);
            return;
          }
          setPreviewPlaying(false);
          setPreviewReady(false);
          if (previewRetrySeed === 0 && resolvedPreviewUrl) {
            setPreviewRetrySeed(1);
            return;
          }
          if (!usePreviewFallback && canSwitchToPreviewFallback) {
            setUsePreviewFallback(true);
            setPreviewRetrySeed(0);
            return;
          }
          setPreviewFailed(true);
        });
      }
    };

    attemptPlayback();

    return () => {
      cancelled = true;
      if (previewRetryTimeoutRef.current) {
        clearTimeout(previewRetryTimeoutRef.current);
        previewRetryTimeoutRef.current = null;
      }
      setPreviewPlaying(false);
      video.pause();
      try {
        video.currentTime = 0;
      } catch {
        // ignore seek reset failures
      }
    };
  }, [canPreview, canSwitchToPreviewFallback, item, previewFailed, previewReady, previewRetrySeed, resolvedPreviewUrl, usePreviewFallback]);

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

          {canPrevious ? (
            <button
              type="button"
              onClick={(event) => {
                event.stopPropagation();
                onPrevious?.();
              }}
              className="absolute left-5 top-1/2 z-20 inline-flex h-11 w-11 -translate-y-1/2 items-center justify-center rounded-[8px] border border-white/12 bg-black/42 text-white/88 shadow-[0_12px_30px_rgba(0,0,0,0.28)] transition hover:bg-black/58"
              aria-label="Previous post"
            >
              <ChevronLeft size={18} />
            </button>
          ) : null}

          {canNext ? (
            <button
              type="button"
              onClick={(event) => {
                event.stopPropagation();
                onNext?.();
              }}
              className="absolute right-5 top-1/2 z-20 inline-flex h-11 w-11 -translate-y-1/2 items-center justify-center rounded-[8px] border border-white/12 bg-black/42 text-white/88 shadow-[0_12px_30px_rgba(0,0,0,0.28)] transition hover:bg-black/58"
              aria-label="Next post"
            >
              <ChevronRight size={18} />
            </button>
          ) : null}

          {/* Dialog */}
          <AnimatePresence initial={false} mode="wait">
            <motion.div
              key={item.id}
              initial={{ opacity: 0, y: 16, scale: 0.98 }}
              animate={{ opacity: 1, y: 0, scale: 1 }}
              exit={{ opacity: 0, y: 10, scale: 0.98 }}
              transition={{ duration: 0.22, ease: [0.22, 1, 0.36, 1] }}
              onClick={(event) => event.stopPropagation()}
              className="relative z-10 overflow-hidden rounded-3xl border border-neutral-200/60 shadow-2xl dark:border-white/[0.08] dark:shadow-[0_30px_80px_rgba(0,0,0,0.6)]"
              style={{ width: 'min(800px, calc(100vw - 4rem))', maxHeight: 'min(600px, calc(100vh - 4rem))' }}
            >
            <div className="grid h-full min-h-[520px] grid-cols-[320px_minmax(0,1fr)]">
              {/* ── Left: Thumbnail Panel (clear view) ── */}
              <div className="relative min-h-[520px] overflow-hidden bg-black">
                {thumbnailUrl ? (
                    <img
                      src={thumbnailUrl}
                      alt=""
                      className="absolute inset-0 h-full w-full object-cover"
                    style={{
                      opacity: shouldRenderPreview && previewPlaying ? 0 : 1,
                      transition: 'opacity 180ms ease',
                    }}
                    onError={() => {
                      if (!useThumbnailFallback && thumbnailFallbackUrl && directThumbnailUrl && directThumbnailUrl !== thumbnailFallbackUrl) {
                        setUseThumbnailFallback(true);
                        setThumbnailRetrySeed(1);
                        return;
                      }
                      setThumbnailRetrySeed((current) => current + 1);
                    }}
                  />
                ) : (
                  <div className="absolute inset-0 bg-[radial-gradient(circle_at_30%_20%,rgba(225,29,72,0.14),transparent_50%),linear-gradient(180deg,#161616_0%,#050505_100%)]" />
                )}
                {shouldRenderPreview ? (
                  <video
                    key={`${item.id}:${resolvedPreviewUrl}`}
                    ref={previewRef}
                    src={resolvedPreviewUrl}
                    poster={thumbnailUrl}
                    muted
                    playsInline
                    loop
                    preload="auto"
                    className="absolute inset-0 h-full w-full object-cover"
                    style={{
                      opacity: previewPlaying ? 1 : 0,
                      transition: 'opacity 180ms ease',
                    }}
                    onLoadedMetadata={() => undefined}
                    onLoadedData={(event) => {
                      if (event.currentTarget !== previewRef.current) return;
                      setPreviewReady(true);
                      setPreviewFailed(false);
                    }}
                    onCanPlay={(event) => {
                      if (event.currentTarget !== previewRef.current) return;
                      setPreviewReady(true);
                      setPreviewFailed(false);
                    }}
                    onCanPlayThrough={(event) => {
                      if (event.currentTarget !== previewRef.current) return;
                      setPreviewReady(true);
                      setPreviewFailed(false);
                    }}
                    onPlaying={(event) => {
                      if (event.currentTarget !== previewRef.current) return;
                      setPreviewPlaying(true);
                      setPreviewFailed(false);
                    }}
                    onPause={(event) => {
                      if (event.currentTarget !== previewRef.current) return;
                      setPreviewPlaying(false);
                    }}
                    onWaiting={(event) => {
                      if (event.currentTarget !== previewRef.current) return;
                      setPreviewPlaying(false);
                    }}
                    onStalled={(event) => {
                      if (event.currentTarget !== previewRef.current) return;
                      setPreviewPlaying(false);
                    }}
                    onError={(event) => {
                      if (event.currentTarget !== previewRef.current) return;
                      setPreviewPlaying(false);
                      setPreviewReady(false);
                      if (previewRetrySeed === 0 && resolvedPreviewUrl) {
                        setPreviewRetrySeed(1);
                        return;
                      }
                      if (!usePreviewFallback && canSwitchToPreviewFallback) {
                        setUsePreviewFallback(true);
                        setPreviewRetrySeed(0);
                        return;
                      }
                      setPreviewFailed(true);
                    }}
                  />
                ) : null}
                {firewatch ? (
                  <>
                    <div className="absolute inset-0 bg-[linear-gradient(180deg,transparent_0%,transparent_30%,rgba(0,0,0,0.7)_70%,rgba(0,0,0,0.92)_100%)]" />
                    <div className="absolute inset-x-0 bottom-0 z-10 p-5">
                      <div className="flex flex-wrap gap-1.5">
                        <MetaBadge value="Firewatch" />
                        <MetaBadge value={firewatch.familyLabel} />
                        <MetaBadge value={firewatch.feedName} />
                        <MetaBadge value={firewatch.mediaType} />
                        <MetaBadge value="D7" />
                      </div>
                      <div className="mt-3 text-[36px] font-black leading-[0.9] tracking-[-0.04em] text-white">
                        {percentText(firewatch.avgHotPercentile)}
                      </div>
                      <div className="mt-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-white/50">
                        Avg Top
                      </div>
                    </div>
                  </>
                ) : null}
              </div>

              {/* ── Right: Intelligence Panel — frosted glass over thumbnail bleed ── */}
              <div className="relative flex min-h-[520px] flex-col overflow-hidden bg-white/72 backdrop-blur-2xl dark:bg-black/72">
                <div className="pointer-events-none absolute inset-0 overflow-hidden">
                  {thumbnailUrl ? (
                    <>
                      <img
                        src={thumbnailUrl}
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
                  {firewatch ? (
                    <>
                      <div>
                        <SectionTag>Firewatch</SectionTag>
                        <div className="mt-2 text-[26px] font-black leading-[0.94] tracking-[-0.04em] text-neutral-900 dark:text-white">
                          {firewatch.patternLabel}
                        </div>
                        <div className="mt-1.5 flex flex-wrap gap-1.5">
                          <MetaBadge value={firewatch.familyLabel} />
                          <MetaBadge value={firewatch.feedName} />
                          <MetaBadge value={firewatch.mediaType} />
                        </div>
                      </div>

                      <div className="mt-4 grid grid-cols-2 gap-3">
                        <CompactStat label="Avg Top" value={percentText(firewatch.avgHotPercentile)} accent />
                        <CompactStat label="Winners" value={compactCount(firewatch.matchCount, 'hot')} />
                        <CompactStat
                          label={firewatch.feedersCount != null && firewatch.feedersCount > 1 ? 'Spread' : 'Format'}
                          value={firewatch.feedersCount != null && firewatch.feedersCount > 1 ? compactCount(firewatch.feedersCount, 'feeders') : firewatch.mediaType}
                        />
                        <CompactStat
                          label={firewatch.anchorGap != null ? 'Anchor Gap' : '14d Lift'}
                          value={firewatch.anchorGap != null ? `+${Math.round(firewatch.anchorGap)}` : liftText(firewatch.recentLift)}
                        />
                      </div>

                      <div className="mt-4">
                        <div className="flex items-center justify-between gap-3">
                          <SectionTag>Shared Tags</SectionTag>
                          {firewatch.baselineShare != null && (
                            <div className="text-[9px] font-bold uppercase tracking-[0.14em] text-neutral-300 dark:text-white/24">
                              Baseline {Math.round(firewatch.baselineShare * 100)}%
                            </div>
                          )}
                        </div>
                        <div className="mt-2 flex flex-wrap gap-1.5">
                          {[...(firewatch.requiredCues.length > 0 ? firewatch.requiredCues : firewatch.cues)].slice(0, 6).map((cue) => (
                            <div
                              key={`firewatch-cue-${cue}`}
                              className="rounded-full border border-neutral-200/80 bg-white/70 px-2.5 py-1 text-[9px] font-bold uppercase tracking-[0.12em] text-neutral-500 dark:border-white/[0.06] dark:bg-white/[0.045] dark:text-white/58"
                            >
                              {cue}
                            </div>
                          ))}
                        </div>
                      </div>

                      {firewatch.supportPosts.length > 0 && (
                        <div className="mt-4">
                          <SectionTag>Supporting Posts</SectionTag>
                          <div className="hide-scrollbar mt-2 flex gap-2 overflow-x-auto pb-1">
                            {firewatch.supportPosts.map((post) => (
                              <a
                                key={`support-${post.postKey}`}
                                href={post.postUrl}
                                target="_blank"
                                rel="noreferrer"
                                className="shrink-0"
                              >
                                <div className="h-24 w-20 overflow-hidden rounded-2xl border border-neutral-200/80 bg-neutral-200/60 dark:border-white/[0.06] dark:bg-white/[0.025]">
                                  <img src={post.thumbnailUrl} alt="" className="h-full w-full object-cover" />
                                </div>
                                <div className="mt-1 max-w-20 truncate text-[9px] font-bold uppercase tracking-[0.12em] text-neutral-500 dark:text-white/54">
                                  @{(post.handle || 'feed').toUpperCase()}
                                </div>
                              </a>
                            ))}
                          </div>
                        </div>
                      )}

                      {firewatch.anchorSupportPosts.length > 0 && (
                        <div className="mt-4">
                          <SectionTag>Anchor Compare</SectionTag>
                          <div className="hide-scrollbar mt-2 flex gap-2 overflow-x-auto pb-1">
                            {firewatch.anchorSupportPosts.map((post) => (
                              <a
                                key={`anchor-${post.postKey}`}
                                href={post.postUrl}
                                target="_blank"
                                rel="noreferrer"
                                className="shrink-0"
                              >
                                <div className="h-24 w-20 overflow-hidden rounded-2xl border border-neutral-200/80 bg-neutral-200/60 dark:border-white/[0.06] dark:bg-white/[0.025]">
                                  <img src={post.thumbnailUrl} alt="" className="h-full w-full object-cover" />
                                </div>
                                <div className="mt-1 max-w-20 truncate text-[9px] font-bold uppercase tracking-[0.12em] text-neutral-500 dark:text-white/54">
                                  @{(post.handle || 'feed').toUpperCase()}
                                </div>
                              </a>
                            ))}
                          </div>
                        </div>
                      )}
                    </>
                  ) : (
                    <>
                      {stats.patternAlerts.length > 0 && (
                        <div>
                          <div className="flex items-center justify-between gap-3">
                            <SectionTag>Pattern Alerts</SectionTag>
                            <div className="text-[9px] font-bold uppercase tracking-[0.14em] text-neutral-300 dark:text-white/24">
                              {stats.patternAlerts.length} active
                            </div>
                          </div>
                          <div className="mt-2.5 space-y-2">
                            {stats.patternAlerts.map((patternAlert) => (
                              <div
                                key={`${patternAlert.signalCode}:${patternAlert.context}:${patternAlert.alertType}`}
                                className="rounded-2xl border border-neutral-200/80 bg-neutral-50/60 px-4 py-3 dark:border-white/[0.06] dark:bg-white/[0.025]"
                              >
                                <div className="flex items-center justify-between gap-3">
                                  <div className="text-[12px] font-black tracking-[-0.02em] text-neutral-800 dark:text-white/90">
                                    {patternAlert.patternLabel}
                                  </div>
                                  <div className="text-[9px] font-bold uppercase tracking-[0.14em] text-neutral-400 dark:text-white/36">
                                    {patternAlert.context} · {patternAlert.alertType}
                                  </div>
                                </div>
                                <div className="mt-2 flex flex-wrap gap-1.5">
                                  {patternAlert.matchCount != null && (
                                    <MetaBadge value={`${Math.round(patternAlert.matchCount)} Winners`} />
                                  )}
                                  {patternAlert.feedersCount != null && patternAlert.feedersCount > 1 && (
                                    <MetaBadge value={`${Math.round(patternAlert.feedersCount)} Feeders`} />
                                  )}
                                  {patternAlert.avgHotPercentile != null && (
                                    <MetaBadge value={`Avg Top ${Math.round(patternAlert.avgHotPercentile)}%`} />
                                  )}
                                  {patternAlert.anchorGap != null && (
                                    <MetaBadge value={`Gap +${Math.round(patternAlert.anchorGap)} pts`} />
                                  )}
                                  {patternAlert.recentLift != null && patternAlert.recentLift > 0 && (
                                    <MetaBadge value={`14d Lift ${patternAlert.recentLift.toFixed(1)}x`} />
                                  )}
                                  {patternAlert.baselineShare != null && (
                                    <MetaBadge value={`Baseline ${Math.round(patternAlert.baselineShare * 100)}%`} />
                                  )}
                                </div>
                                {(patternAlert.body || patternAlert.percentile != null) && (
                                  <div className="mt-1 text-[11px] font-medium leading-relaxed text-neutral-500 dark:text-white/40">
                                    {patternAlert.body || `Top ${Math.round(patternAlert.percentile || 0)}%`}
                                  </div>
                                )}
                                {patternAlert.cues.length > 0 && (
                                  <div className="mt-2 flex flex-wrap gap-1.5">
                                    {patternAlert.cues.map((cue) => (
                                      <div
                                        key={`${patternAlert.signalCode}:${cue}`}
                                        className="rounded-full border border-neutral-200/80 bg-white/70 px-2.5 py-1 text-[9px] font-bold uppercase tracking-[0.12em] text-neutral-500 dark:border-white/[0.06] dark:bg-white/[0.045] dark:text-white/58"
                                      >
                                        {cue}
                                      </div>
                                    ))}
                                  </div>
                                )}
                                {patternAlert.supportPosts.length > 0 && (
                                  <div className="mt-2.5">
                                    <div className="text-[9px] font-bold uppercase tracking-[0.14em] text-neutral-400 dark:text-white/36">
                                      Supporting Posts
                                    </div>
                                    <div className="hide-scrollbar mt-2 flex gap-2 overflow-x-auto pb-1">
                                      {patternAlert.supportPosts.map((post) => (
                                        <a
                                          key={`${patternAlert.signalCode}:${post.postKey || post.postUrl}`}
                                          href={post.postUrl || '#'}
                                          target="_blank"
                                          rel="noreferrer"
                                          className="shrink-0"
                                        >
                                          <div className="h-24 w-20 overflow-hidden rounded-2xl border border-neutral-200/80 bg-neutral-200/60 dark:border-white/[0.06] dark:bg-white/[0.025]">
                                            {post.thumbnailUrl ? (
                                              <img src={post.thumbnailUrl} alt="" className="h-full w-full object-cover" />
                                            ) : null}
                                          </div>
                                          <div className="mt-1 max-w-20 truncate text-[9px] font-bold uppercase tracking-[0.12em] text-neutral-500 dark:text-white/54">
                                            @{(post.handle || 'feed').toUpperCase()}
                                          </div>
                                        </a>
                                      ))}
                                    </div>
                                  </div>
                                )}
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </>
                  )}

                  {!firewatch && (
                    <>
                      <div className={`${stats.patternAlerts.length > 0 ? 'mt-4 ' : ''}mb-3 flex min-w-0 flex-wrap items-center gap-2`}>
                        <TrackingArchivePill href={trackingArchiveHref} label={stats.handle} />
                        <TrackingInfoBadge value={stats.mediaType} />
                        <TrackingInfoBadge value={stats.checkpoint} />
                      </div>

                      {/* Hero Metric — accent base, white text */}
                      <div className="rounded-2xl bg-[#E11D48] px-4 py-3.5">
                        <div className="text-[9px] font-bold uppercase tracking-[0.2em] text-white/68">
                          Hero Metric
                        </div>
                        <div className="mt-2.5 flex items-end justify-between gap-4">
                          <div>
                            <div className="text-[38px] font-black leading-none tracking-[-0.05em] text-white">
                              {compactOrDash(stats.value)}
                            </div>
                            <div className="mt-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-white/70">
                              {stats.bestMetric}
                            </div>
                          </div>
                          <div className="text-right">
                            <div className="text-[28px] font-black leading-none tracking-[-0.04em] text-white">
                              {multipleOrDash(stats.multiple)}
                            </div>
                            <div className="mt-1 text-[11px] font-medium text-white/70">
                              {compactOrDash(stats.baseline)} usual
                            </div>
                          </div>
                        </div>
                      </div>

                      {/* Supporting Metrics */}
                      <div className="mt-3.5">
                        <div className="flex items-center justify-between gap-3">
                          <SectionTag>Supporting Metrics</SectionTag>
                          <div className="text-[9px] font-bold uppercase tracking-[0.14em] text-neutral-300 dark:text-white/24">
                            {stats.bestInLastN == null ? 'Best in -- posts' : `Best in ${Math.max(1, Math.round(stats.bestInLastN))} posts`}
                          </div>
                        </div>
                        <div className="mt-2 space-y-2">
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
                      <div className="my-3.5 h-px w-full bg-neutral-200/80 dark:bg-white/[0.06]" />

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
                    </>
                  )}

                  {/* Open Post CTA */}
                  <div className="mt-auto pt-5">
                    <button
                      type="button"
                      onClick={(event) => {
                        event.stopPropagation();
                        if (item.postUrl) window.open(item.postUrl, '_blank', 'noreferrer');
                      }}
                      className="flex w-full items-center justify-center gap-2 rounded-2xl bg-[#E11D48] px-5 py-3 text-[11px] font-black uppercase tracking-[0.16em] text-white shadow-[0_8px_24px_rgba(225,29,72,0.12)] transition-all hover:shadow-[0_12px_32px_rgba(225,29,72,0.2)] active:scale-[0.995]"
                    >
                      {firewatch ? 'Open Hero Post' : 'Open Post'}
                      <ExternalLink size={14} />
                    </button>
                  </div>
                </div>
              </div>
            </div>
            </motion.div>
          </AnimatePresence>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
