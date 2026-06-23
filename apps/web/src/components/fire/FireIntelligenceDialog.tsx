'use client';

/* eslint-disable @next/next/no-img-element -- Fire media previews use direct dynamic Instagram/R2 URLs. */

import Link from 'next/link';
import { useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { AnimatePresence, motion } from 'framer-motion';
import { ChevronLeft, ChevronRight, ExternalLink } from 'lucide-react';
import { FireItem } from './types';
import { compact } from './fireLogicHelpers';
import {
  metricLabel,
  orderedSupportMetricsFromPayload,
  resolveBestMetricFromPayload,
} from './fireMetricDisplay';

type FireIntelligenceDialogProps = {
  item: FireItem | null;
  onClose: () => void;
  onPrevious?: () => void;
  onNext?: () => void;
  canPrevious?: boolean;
  canNext?: boolean;
};

const FIRE_DIALOG_SHARED_SPRING = { type: 'spring', stiffness: 420, damping: 42, mass: 0.9 } as const;
const FIRE_BACKDROP_EASE = [0.25, 0.1, 0.25, 1] as const;

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

function textList(v: unknown, max = 4): string[] {
  const values = Array.isArray(v) ? v : v ? [v] : [];
  return Array.from(
    new Set(
      values
        .map((entry) => text(entry).trim())
        .filter(Boolean),
    ),
  ).slice(0, max);
}

function parsePostContextRead(meta: Record<string, unknown>) {
  const read = asRecord(meta.post_read);
  const source = text(read.source);
  const headline = text(read.headline).trim();
  const metricContext = text(read.metric_context).trim();
  const readText = text(read.read).trim();
  const direction = text(read.direction).trim();
  const scene = text(read.scene).trim() || readText;
  const fit = text(read.fit).trim() || text(read.memory_match).trim();
  const recentRun = text(read.recent_run).trim();
  const funFactRecord = asRecord(read.fun_fact);
  // Worker-computed stat, kept distinct from the LLM headline (v16+).
  const funFact = text(funFactRecord.text).trim() || text(read.fun_fact).trim() || text(read.numbers).trim();
  const matches = textList(read.matches);
  const deviates = textList(read.deviates);
  const unclear = textList(read.unclear, 3);
  const notes = textList(read.notes, 3);
  const isD7Read = source === 'd7_read' || Boolean(headline || metricContext || readText || direction || scene || fit || recentRun || funFact);
  const sourceLabel = text(read.source_label) || (source === 'post_fingerprint' ? 'Fingerprint' : isD7Read ? 'D7 Post Mortem' : 'Context Layer');
  if (!isD7Read && matches.length === 0 && deviates.length === 0 && unclear.length === 0 && notes.length === 0) {
    return null;
  }
  return { matches, deviates, unclear, notes, sourceLabel, source, headline, metricContext, readText, direction, scene, fit, recentRun, funFact, isD7Read };
}

type PostContextRead = NonNullable<ReturnType<typeof parsePostContextRead>>;

function d7ReadSections(read: PostContextRead) {
  const directionFallback = !read.recentRun && !read.fit ? read.direction : '';
  return [
    {
      label: 'Scene',
      eyebrow: 'the reel itself',
      value: read.scene || read.readText,
    },
    {
      label: 'Fit',
      eyebrow: 'this post vs the account',
      value: read.fit || read.metricContext,
    },
    {
      label: 'Run',
      eyebrow: 'the account lately',
      value: read.recentRun || directionFallback,
    },
  ].filter((section) => section.value.trim());
}

function D7VerdictBar({
  read,
  onOpen,
}: {
  read: PostContextRead;
  onOpen: () => void;
}) {
  const verdict = read.headline || read.funFact || read.metricContext;
  if (!verdict) return null;
  return (
    <button
      type="button"
      onClick={(event) => {
        event.stopPropagation();
        onOpen();
      }}
      className="relative mt-2.5 block w-full overflow-hidden rounded-2xl border border-[#E11D48]/18 bg-[#17060b] px-4 py-3.5 text-left shadow-[0_18px_42px_rgba(225,29,72,0.14),inset_0_1px_0_rgba(255,255,255,0.14)] transition-transform active:scale-[0.99] dark:border-[#E11D48]/22 dark:bg-[#120408]"
    >
      <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_left,rgba(225,29,72,0.40),transparent_46%),linear-gradient(135deg,rgba(255,255,255,0.11),transparent_46%)]" />
      <div className="relative flex items-center justify-between gap-3">
        <div className="flex items-center gap-2 text-[9px] font-black uppercase tracking-[0.2em] text-white/54">
          <span className="h-1.5 w-1.5 rounded-full bg-[#fb7185] shadow-[0_0_10px_rgba(225,29,72,0.5)]" />
          Post Mortem
        </div>
        <span className="inline-flex shrink-0 items-center gap-1 rounded-full border border-white/12 bg-white/10 px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-white/64">
          Open Read
          <ChevronRight size={12} strokeWidth={3} />
        </span>
      </div>
      <p className="relative mt-2 line-clamp-2 text-[22px] font-black leading-[1.06] text-white">
        {verdict}
      </p>
    </button>
  );
}

function D7ReadView({
  itemId,
  read,
  sourceLabel,
  onBack,
}: {
  itemId: string;
  read: PostContextRead;
  sourceLabel: string;
  onBack: () => void;
}) {
  const verdict = read.headline || read.funFact || read.metricContext;
  const sections = d7ReadSections(read);
  const [activeIndex, setActiveIndex] = useState(0);
  const safeIndex = sections.length === 0 ? 0 : Math.min(activeIndex, sections.length - 1);
  const active = sections[safeIndex];

  return (
    <motion.div
      className="flex min-h-0 flex-1 flex-col"
      initial={{ opacity: 0, x: 16 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ duration: 0.22, ease: [0.22, 1, 0.36, 1] }}
    >
      <div className="flex items-center justify-between gap-3">
        <button
          type="button"
          onClick={(event) => {
            event.stopPropagation();
            onBack();
          }}
          className="inline-flex items-center gap-1 rounded-full border border-neutral-200/80 bg-white/70 py-1 pl-1.5 pr-3 text-[9px] font-black uppercase tracking-[0.14em] text-neutral-600 transition hover:bg-white dark:border-white/[0.08] dark:bg-white/[0.05] dark:text-white/62"
        >
          <ChevronLeft size={13} strokeWidth={3} />
          Stats
        </button>
        <div className="text-[9px] font-bold uppercase tracking-[0.14em] text-[#E11D48]/80">
          {sourceLabel}
        </div>
      </div>

      {verdict ? (
        <div className="relative mt-3 overflow-hidden rounded-2xl border border-[#E11D48]/18 bg-[#17060b] px-4 py-3.5 shadow-[0_18px_42px_rgba(225,29,72,0.14),inset_0_1px_0_rgba(255,255,255,0.14)] dark:border-[#E11D48]/22 dark:bg-[#120408]">
          <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_left,rgba(225,29,72,0.40),transparent_44%),linear-gradient(135deg,rgba(255,255,255,0.11),transparent_46%)]" />
          <div className="relative text-[9px] font-black uppercase tracking-[0.2em] text-white/52">
            Trigger vs recent 30
          </div>
          <p className="relative mt-2 text-[26px] font-black leading-[1.04] text-white">
            {verdict}
          </p>
          {read.metricContext ? (
            <p className="relative mt-2 text-[11px] font-black uppercase tracking-[0.14em] text-[#fb7185]">
              {read.metricContext}
            </p>
          ) : null}
        </div>
      ) : null}

      {active ? (
        <>
          <div
            role="tablist"
            aria-label="Post mortem reads"
            className="mt-3 flex gap-1.5 rounded-full border border-neutral-200/80 bg-neutral-100/70 p-1 dark:border-white/[0.08] dark:bg-white/[0.05]"
          >
            {sections.map((section, index) => {
              const isActive = index === safeIndex;
              return (
                <button
                  key={`${itemId}-dialog-readview-tab-${section.label}`}
                  type="button"
                  role="tab"
                  aria-selected={isActive}
                  onClick={(event) => {
                    event.stopPropagation();
                    setActiveIndex(index);
                  }}
                  className={
                    isActive
                      ? 'flex-1 rounded-full bg-[#E11D48] px-3 py-1.5 text-[10px] font-black uppercase tracking-[0.14em] text-white shadow-[0_6px_16px_rgba(225,29,72,0.24)] transition-colors'
                      : 'flex-1 rounded-full px-3 py-1.5 text-[10px] font-black uppercase tracking-[0.14em] text-neutral-500 transition-colors hover:text-neutral-700 dark:text-white/42 dark:hover:text-white/64'
                  }
                >
                  {section.label}
                </button>
              );
            })}
          </div>

          <div className="mt-3 min-h-0 flex-1 overflow-y-auto overscroll-contain rounded-2xl border border-neutral-200/80 bg-neutral-50/74 px-4 py-3.5 shadow-[0_10px_24px_rgba(15,23,42,0.08),inset_0_1px_0_rgba(255,255,255,0.78)] dark:border-white/[0.07] dark:bg-white/[0.04]">
            <motion.div
              key={`${itemId}-dialog-readview-pane-${safeIndex}`}
              initial={{ opacity: 0, y: 6 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.2, ease: [0.22, 1, 0.36, 1] }}
            >
              <div className="text-[9px] font-black uppercase tracking-[0.16em] text-[#E11D48]/80">
                {active.eyebrow}
              </div>
              <p className="mt-2 text-[14px] font-semibold leading-relaxed text-neutral-800 dark:text-white/68">
                {active.value}
              </p>
            </motion.div>
          </div>
        </>
      ) : null}
    </motion.div>
  );
}

function mediaProxyUrl(postKey: string, role = 'thumbnail'): string {
  const key = postKey.trim();
  if (!key) return '';
  const params = new URLSearchParams({ postKey: key, role, v: 'fire-v7' });
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
  if (!item) return null;
  const feedId = item.feedId;
  const handle = String(item.surfaceHandle || item.handle || '').replace(/^@+/, '').trim().toLowerCase();
  if (!Number.isFinite(feedId) || !handle) return null;
  return `/feed/${feedId}/feeder/${encodeURIComponent(handle)}`;
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

function firstTrajectoryPoint(points: Array<number | null>): number | null {
  for (const point of points) {
    if (point != null && Number.isFinite(point)) return point;
  }
  return null;
}

/* ── Subcomponents ── */

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
  baseline,
  accent = false,
}: {
  label: string;
  value: string;
  multiple: string;
  baseline: string;
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
      <div className="shrink-0 text-right">
        <div
          className={
            accent
              ? 'text-[26px] font-black leading-none tracking-[-0.04em] text-[#E11D48]'
              : 'text-[26px] font-black leading-none tracking-[-0.04em] text-neutral-800 dark:text-white/90'
          }
        >
          {multiple}
        </div>
        {baseline !== '--' ? (
          <div className="mt-1 text-[12px] font-semibold leading-none text-neutral-400 dark:text-white/42">
            {baseline} usual
          </div>
        ) : null}
      </div>
    </div>
  );
}

function CompactStat({
  label,
  value,
  detail,
  accent = false,
}: {
  label: string;
  value: string;
  detail?: string | null;
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
            ? 'mt-1.5 flex min-w-0 items-baseline gap-1.5 text-[20px] font-black leading-none tracking-[-0.03em] text-black dark:text-[#E11D48]'
            : 'mt-1.5 flex min-w-0 items-baseline gap-1.5 text-[20px] font-black leading-none tracking-[-0.03em] text-neutral-800 dark:text-white/90'
        }
      >
        <span>{value}</span>
        {detail ? (
          <span className="min-w-0 truncate text-[11px] font-semibold tracking-normal text-neutral-400 dark:text-white/42">
            {detail}
          </span>
        ) : null}
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
  const [expandedPostMortemId, setExpandedPostMortemId] = useState<string | null>(null);
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
    const position = asRecord(payload.position);
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
    const hourMult = num(timing.hour_multiple);
    const d1 = num(trajectory.d1);
    const d3 = num(trajectory.d3);
    const d7 = num(trajectory.d7);
    const d21 = num(trajectory.d21);
    const delta = num(trajectory.delta) ?? item.trajectoryDeltaPercentile;
    const trajectoryPoints = [d1, d3, d7, d21];
    const currentTrajectory =
      item.surfacePercentile ??
      num(position.overall_percentile) ??
      num(position.percentile);
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
      baseline: compactOrDash(metric.baseline),
    }));
    const postContextRead = parsePostContextRead(meta);

    return {
      bestMetric: metricLabel(bestMetric),
      value,
      baseline,
      multiple,
      bestInLastN,
      hour,
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
      postContextRead,
      hideSignalChrome: item.hideSignalChrome === true,
    };
  }, [item]);

  const dialogStyle = {
    width: 'min(clamp(700px, 52vw, 980px), calc(100vw - 6rem))',
    height: 'min(clamp(560px, 62dvh, 740px), calc(100dvh - 8rem))',
    maxHeight: 'calc(100dvh - 8rem)',
  };
  const dialogGridClass = 'grid h-full min-h-0 grid-cols-[clamp(300px,23vw,430px)_minmax(0,1fr)]';
  const dialogPanelMinHeightClass = 'min-h-0';

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
  const postMortemReadOpen = Boolean(
    item && stats?.postContextRead?.isD7Read && expandedPostMortemId === item.id,
  );

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

  const dialog = (
    <AnimatePresence>
      {item && stats && (
        <motion.div
          className="fixed inset-0 hidden items-center justify-center px-12 py-16 lg:flex"
          style={{ zIndex: 2147483000 }}
          initial={{ opacity: 1 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 1 }}
          transition={{ duration: 0.34, ease: FIRE_BACKDROP_EASE }}
          onClick={onClose}
        >
          {/* Backdrop */}
          <motion.div
            className="absolute inset-0 bg-white/44 backdrop-blur-lg will-change-opacity dark:bg-black/72"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.46, ease: FIRE_BACKDROP_EASE }}
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
          <motion.div
            key={item.id}
            layoutId={`fire-card-dialog-${item.id}`}
            transition={{ layout: FIRE_DIALOG_SHARED_SPRING }}
            onClick={(event) => event.stopPropagation()}
            className="relative z-10 overflow-hidden rounded-[22px] border border-neutral-200/60 shadow-2xl dark:border-white/[0.08] dark:shadow-[0_30px_80px_rgba(0,0,0,0.6)]"
            style={dialogStyle}
          >
            <div className={dialogGridClass}>
              {/* ── Left: Thumbnail Panel (clear view) ── */}
              <div className={`relative ${dialogPanelMinHeightClass} overflow-hidden bg-black`}>
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
              </div>

              {/* ── Right: Intelligence Panel — frosted glass over thumbnail bleed ── */}
              <div className={`relative flex ${dialogPanelMinHeightClass} flex-col overflow-hidden bg-white/72 backdrop-blur-2xl dark:bg-black/72`}>
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

                <div className="relative flex min-h-0 flex-1 flex-col overflow-hidden p-6">
                  {postMortemReadOpen && stats.postContextRead ? (
                    <D7ReadView
                      itemId={item.id}
                      read={stats.postContextRead}
                      sourceLabel={stats.postContextRead.sourceLabel}
                      onBack={() => setExpandedPostMortemId(null)}
                    />
                  ) : (
                    <div className="flex min-h-0 flex-1 flex-col overflow-y-auto overscroll-contain">
                    <>
                      <div className="mb-3 flex min-w-0 flex-wrap items-center gap-2">
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
                            <div className="mt-1 text-[13px] font-semibold text-white/76">
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
                              baseline={metric.baseline}
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
                          <div className={`mt-2.5 grid gap-2 ${stats.hourMult == null ? 'grid-cols-1' : 'grid-cols-2'}`}>
                            <CompactStat label="Post Time" value={hourAmPm(stats.hour)} accent />
                            {stats.hourMult == null ? null : (
                              <CompactStat
                                label="Time Lift"
                                value={multipleOrDash(stats.hourMult)}
                                detail={`vs usual ${stats.hour == null ? 'same-hour' : hourAmPm(stats.hour)} posts`}
                              />
                            )}
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

                      {stats.postContextRead && (
                        stats.postContextRead.isD7Read ? (
                          <div className="mt-4">
                            <D7VerdictBar
                              read={stats.postContextRead}
                              onOpen={() => setExpandedPostMortemId(item.id)}
                            />
                          </div>
                        ) : (
                          <div className="mt-4">
                            <div className="flex items-center justify-between gap-3">
                              <SectionTag>Post Read</SectionTag>
                              <div className="text-[9px] font-bold uppercase tracking-[0.14em] text-[#E11D48]/80">
                                {stats.postContextRead.sourceLabel}
                              </div>
                            </div>
                            <div className="mt-2.5 rounded-2xl border border-neutral-200/80 bg-neutral-50/62 px-4 py-4 dark:border-white/[0.06] dark:bg-white/[0.025]">
                              {stats.postContextRead.matches[0] ? (
                                <p className="text-[15px] font-semibold leading-relaxed text-neutral-800 dark:text-white/74">
                                  {stats.postContextRead.matches[0]}
                                </p>
                              ) : null}
                              <div className="mt-3 grid gap-2">
                                {[...stats.postContextRead.matches.slice(1, 4), ...stats.postContextRead.deviates.slice(0, 3), ...stats.postContextRead.notes.slice(0, 2)].slice(0, 5).map((line) => (
                                  <div
                                    key={`${item.id}-post-read-${line}`}
                                    className="rounded-xl bg-white/64 px-3 py-2 text-[12px] font-medium leading-relaxed text-neutral-600 dark:bg-white/[0.04] dark:text-white/52"
                                  >
                                    {line}
                                  </div>
                                ))}
                              </div>
                            </div>
                          </div>
                        )
                      )}
                    </>
                    </div>
                  )}

                  {/* CTA */}
                  <div className="pt-5">
                    <button
                      type="button"
                      onClick={(event) => {
                        event.stopPropagation();
                        if (item.postUrl) window.open(item.postUrl, '_blank', 'noreferrer');
                      }}
                      className="flex w-full items-center justify-center gap-2 rounded-2xl bg-[#E11D48] px-5 py-3 text-[11px] font-black uppercase tracking-[0.16em] text-white shadow-[0_8px_24px_rgba(225,29,72,0.12)] transition-all hover:shadow-[0_12px_32px_rgba(225,29,72,0.2)] active:scale-[0.995]"
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

  const portalTarget = typeof document === 'undefined' ? null : document.body;
  return portalTarget ? createPortal(dialog, portalTarget) : null;
}
