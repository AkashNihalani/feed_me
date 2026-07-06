'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { AnimatePresence, motion, useAnimationControls } from 'framer-motion';
import { ChevronLeft, ChevronRight, Lock, Pause, Play } from 'lucide-react';
import { FireItem } from './types';
import {
  formatMetricValue,
  metricLabel,
  orderedSupportMetricsFromPayload,
  resolveBestMetricFromPayload,
} from '@/components/fire/fireMetricDisplay';
import { useAppHaptics } from '@/lib/haptics';

const PREVIEW_MAX_SECONDS = 5;
const THUMBNAIL_FAILURE_TTL_MS = 10 * 60 * 1000;
const thumbnailFailureCache = new Map<string, number>();

export type FireCard3DProps = {
  item: FireItem;
  forcedOpen?: boolean;
  highlighted?: boolean;
  layoutMode?: 'mobile' | 'desktop';
  mobileAutoplayEnabled?: boolean;
  showMobileAutoplayToggle?: boolean;
  onOpenDetails?: () => void;
  onToggleMobileAutoplay?: (next: boolean) => void;
  onBeforeOpenPost?: (itemId: string) => void;
  onOpenStateChange?: (itemId: string, isOpen: boolean) => void;
};

function asRec(v: unknown): Record<string, unknown> {
  return v && typeof v === 'object' && !Array.isArray(v) ? (v as Record<string, unknown>) : {};
}

function num(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const n = Number(v.replace('%', '').trim());
    if (Number.isFinite(n)) return n;
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
  const read = asRec(meta.post_read);
  const source = text(read.source);
  const headline = text(read.headline).trim();
  const metricContext = text(read.metric_context).trim();
  const readText = text(read.read).trim();
  const direction = text(read.direction).trim();
  const scene = text(read.scene).trim() || readText;
  const fit = text(read.fit).trim() || text(read.memory_match).trim();
  const recentRun = text(read.recent_run).trim();
  const funFactRecord = asRec(read.fun_fact);
  // The worker-computed stat. Kept distinct from the LLM headline (v16+) — they
  // are two different things now: the stat is its own section in the read.
  const funFact = text(funFactRecord.text).trim() || text(read.fun_fact).trim() || text(read.numbers).trim();
  const matches = textList(read.matches);
  const deviates = textList(read.deviates);
  const unclear = textList(read.unclear, 2);
  const notes = textList(read.notes, 2);
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
      className="relative mt-2 mb-2 block w-full overflow-hidden rounded-[18px] border border-[var(--fm-accent)]/22 bg-[#17060b] px-3 py-2.5 text-left shadow-[0_14px_30px_rgb(var(--fm-accent-rgb)/0.16),inset_0_1px_0_rgba(255,255,255,0.16)] transition-transform active:scale-[0.99] dark:border-[var(--fm-accent)]/24 dark:bg-[#120408] sm:mb-3"
    >
      <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_left,rgb(var(--fm-accent-rgb)/0.42),transparent_50%),linear-gradient(135deg,rgba(255,255,255,0.10),transparent_44%)]" />
      <div className="relative flex items-center justify-between gap-2">
        <div className="flex items-center gap-1.5 text-[8px] font-black uppercase tracking-[0.14em] text-white/56 sm:text-[10px]">
          <span className="h-1.5 w-1.5 rounded-full bg-[var(--fm-accent-bright)] shadow-[0_0_10px_rgb(var(--fm-accent-rgb)/0.5)]" />
          Post Mortem
        </div>
        <span className="inline-flex shrink-0 items-center gap-0.5 rounded-full border border-white/14 bg-white/10 px-2 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-white/70 sm:text-[8px]">
          Read
          <ChevronRight size={10} strokeWidth={3} />
        </span>
      </div>
      <p className="relative mt-1.5 line-clamp-2 text-[18px] font-black leading-[1.06] tracking-[-0.04em] text-white sm:text-[22px]">
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
  const hero = read.headline || read.funFact || read.metricContext;
  const showStat = Boolean(read.funFact && read.headline);
  const sections = d7ReadSections(read);
  const scrollRef = useRef<HTMLDivElement | null>(null);
  const [activeIndex, setActiveIndex] = useState(0);
  const safeIndex = sections.length === 0 ? 0 : Math.min(activeIndex, sections.length - 1);

  const handleScroll = () => {
    const el = scrollRef.current;
    if (!el) return;
    const width = el.clientWidth || 1;
    const next = Math.max(0, Math.round(el.scrollLeft / width));
    setActiveIndex((prev) => (prev === next ? prev : next));
  };

  const scrollToIndex = (index: number) => {
    const el = scrollRef.current;
    if (!el) return;
    el.scrollTo({ left: index * el.clientWidth, behavior: 'smooth' });
  };

  return (
    <motion.div className="flex min-h-0 flex-1 flex-col">
      {/* Eyebrow */}
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-1.5 text-[8px] font-black uppercase tracking-[0.14em] text-[var(--fm-accent)] sm:text-[10px]">
          <span className="h-1.5 w-1.5 rounded-full bg-[var(--fm-accent)] shadow-[0_0_10px_rgb(var(--fm-accent-rgb)/0.42)]" />
          {sourceLabel}
        </div>
        {sections.length > 1 ? (
          <div className="flex items-center gap-1 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/32 dark:text-white/30 sm:text-[10px]">
            Swipe
            <ChevronRight size={11} strokeWidth={3} />
          </div>
        ) : null}
      </div>

      {/* Headline hero — bold Feed Me red */}
      {hero ? (
        <div className="relative mt-2 shrink-0 overflow-hidden rounded-[18px] bg-[var(--fm-accent)] px-3.5 py-3.5 shadow-[0_16px_34px_rgb(var(--fm-accent-rgb)/0.32),inset_0_2px_4px_rgba(255,255,255,0.34),inset_0_-3px_8px_rgba(136,19,55,0.4)] sm:px-4 sm:py-4">
          <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(255,255,255,0.22),transparent_54%)]" />
          <div className="relative text-[8px] font-black uppercase tracking-[0.22em] text-white/68 sm:text-[10px]">
            The Verdict
          </div>
          <p className="relative mt-1 text-[22px] font-black leading-[0.98] tracking-[-0.04em] text-white drop-shadow-sm sm:text-[28px]">
            {hero}
          </p>
        </div>
      ) : null}

      {/* fun_fact — grounded stat as its own section */}
      {showStat ? (
        <div className="mt-2 shrink-0 rounded-[14px] border border-[var(--fm-accent)]/14 bg-white/82 px-3 py-2.5 shadow-[inset_0_1px_0_rgba(255,255,255,0.8)] dark:border-[var(--fm-accent)]/22 dark:bg-white/[0.06]">
          <div className="text-[8px] font-black uppercase tracking-[0.14em] text-[var(--fm-accent)]/72 sm:text-[10px]">
            By the numbers
          </div>
          <p className="mt-1 text-[12px] font-semibold leading-[1.32] text-foreground/74 dark:text-white/64 sm:text-[14px]">
            {read.funFact}
          </p>
        </div>
      ) : null}

      {/* Swipe pager — Trigger / Fit / Run */}
      {sections.length > 0 ? (
        <div
          ref={scrollRef}
          onScroll={handleScroll}
          className="hide-scrollbar mt-2 flex min-h-0 flex-1 snap-x snap-mandatory overflow-x-auto overflow-y-hidden overscroll-x-contain scroll-smooth"
        >
          {sections.map((section) => (
            <div
              key={`${itemId}-readpane-${section.label}`}
              className="flex h-full w-full shrink-0 snap-center flex-col px-0.5"
            >
              <div className="hide-scrollbar flex min-h-0 flex-1 flex-col overflow-y-auto rounded-[18px] border border-black/[0.05] bg-white/90 px-3.5 py-3 shadow-[0_10px_22px_rgba(15,23,42,0.08),inset_0_1px_0_rgba(255,255,255,0.85)] dark:border-white/10 dark:bg-white/[0.1]">
                <div className="flex items-baseline gap-1.5">
                  <span className="text-[14px] font-black uppercase tracking-[0.06em] text-[var(--fm-accent)] sm:text-[14px]">
                    {section.label}
                  </span>
                  <span className="text-[8px] font-black uppercase tracking-[0.14em] text-foreground/34 dark:text-white/32 sm:text-[10px]">
                    {section.eyebrow}
                  </span>
                </div>
                <p className="mt-2 text-[14px] font-semibold leading-[1.36] text-foreground/80 dark:text-white/70 sm:text-[14px]">
                  {section.value}
                </p>
              </div>
            </div>
          ))}
        </div>
      ) : null}

      {/* Animated pagination */}
      {sections.length > 1 ? (
        <div className="mt-2.5 flex shrink-0 items-center justify-center gap-1.5">
          {sections.map((section, index) => {
            const isActive = index === safeIndex;
            return (
              <button
                key={`${itemId}-dot-${section.label}`}
                type="button"
                aria-label={`Go to ${section.label}`}
                onClick={(event) => {
                  event.stopPropagation();
                  scrollToIndex(index);
                }}
                className={[
                  'h-1.5 rounded-full transition-all duration-300 ease-out',
                  isActive
                    ? 'w-6 bg-[var(--fm-accent)] shadow-[0_0_10px_rgb(var(--fm-accent-rgb)/0.42)]'
                    : 'w-1.5 bg-foreground/16 dark:bg-white/20',
                ].join(' ')}
              />
            );
          })}
        </div>
      ) : null}

      {/* Back to Stats — bottom */}
      <button
        type="button"
        onClick={(event) => {
          event.stopPropagation();
          onBack();
        }}
        className="mt-2.5 inline-flex w-full shrink-0 items-center justify-center gap-1 rounded-[14px] border border-black/[0.06] bg-black/[0.03] py-2.5 text-[10px] font-black uppercase tracking-[0.14em] text-foreground/56 transition-transform active:scale-[0.99] dark:border-white/10 dark:bg-white/[0.05] dark:text-white/56 sm:text-[10px]"
      >
        <ChevronLeft size={13} strokeWidth={3} />
        Back to Stats
      </button>
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

function isRetryVariantOf(url: string, baseUrl: string): boolean {
  const value = url.trim();
  const base = baseUrl.trim();
  if (!value || !base) return false;
  if (value === base) return true;
  try {
    const origin = typeof window !== 'undefined' ? window.location.origin : 'https://feedmemore.vercel.app';
    const current = new URL(value, origin);
    const target = new URL(base, origin);
    current.searchParams.delete('_retry');
    target.searchParams.delete('_retry');
    return current.pathname === target.pathname && current.searchParams.toString() === target.searchParams.toString();
  } catch {
    return value.startsWith(`${base}&`) || value.startsWith(`${base}?`);
  }
}

function isThumbnailFailureCached(key: string): boolean {
  if (!key) return false;
  const failedAt = thumbnailFailureCache.get(key);
  if (!failedAt) return false;
  if (Date.now() - failedAt <= THUMBNAIL_FAILURE_TTL_MS) return true;
  thumbnailFailureCache.delete(key);
  return false;
}

function rememberThumbnailFailure(key: string) {
  if (key) thumbnailFailureCache.set(key, Date.now());
}

function clampPreviewProgress(currentTime: number, duration = PREVIEW_MAX_SECONDS): number {
  if (!Number.isFinite(duration) || duration <= 0) return 0;
  return Math.max(0, Math.min(1, currentTime / duration));
}

function previewCycleDuration(video: HTMLVideoElement): number {
  const duration = Number.isFinite(video.duration) && video.duration > 0
    ? Math.min(video.duration, PREVIEW_MAX_SECONDS)
    : PREVIEW_MAX_SECONDS;
  return duration > 0 ? duration : PREVIEW_MAX_SECONDS;
}

function previewProgressBackground(progress: number): string {
  const degrees = `${Math.max(0, Math.min(360, progress * 360))}deg`;
  return `conic-gradient(rgba(255,255,255,0.98) 0deg ${degrees}, rgba(255,255,255,0.18) ${degrees} 360deg)`;
}

type VideoFrameAwareElement = HTMLVideoElement & {
  requestVideoFrameCallback?: (callback: () => void) => number;
  cancelVideoFrameCallback?: (handle: number) => void;
};

export function FireCard3D({
  item,
  forcedOpen = false,
  highlighted = false,
  layoutMode = 'mobile',
  mobileAutoplayEnabled = true,
  showMobileAutoplayToggle = false,
  onOpenDetails,
  onToggleMobileAutoplay,
  onBeforeOpenPost,
  onOpenStateChange,
}: FireCard3DProps) {
  const { play } = useAppHaptics();
  const thumbnailFailureKey = item.postKey || item.id;
  const initialThumbnailFailed = isThumbnailFailureCached(thumbnailFailureKey);
  const [openLocal, setOpenLocal] = useState(false);
  const [expandedPostMortemId, setExpandedPostMortemId] = useState<string | null>(null);
  const [imgDead, setImgDead] = useState(initialThumbnailFailed);
  const [previewReady, setPreviewReady] = useState(false);
  const [previewFailed, setPreviewFailed] = useState(false);
  const [previewPlaying, setPreviewPlaying] = useState(false);
  const [usePreviewFallback, setUsePreviewFallback] = useState(false);
  const [previewRetrySeed, setPreviewRetrySeed] = useState(0);
  const [isPrimed, setIsPrimed] = useState(false);
  const [thumbnailUrl, setThumbnailUrl] = useState(initialThumbnailFailed ? '' : text(item.thumbnailUrl));
  const primedTimeoutRef = useRef<number | null>(null);
  const previewRef = useRef<HTMLVideoElement | null>(null);
  const previewProgressRingRef = useRef<HTMLSpanElement | null>(null);
  const previewProgressRef = useRef(0);
  const previewSessionRef = useRef(0);
  const previewRetryTimeoutRef = useRef<number | null>(null);
  const thumbnailRetryCountRef = useRef(0);
  const lockControls = useAnimationControls();
  const isDesktopCard = layoutMode === 'desktop';
  const isCardInteractive = highlighted || forcedOpen;
  const warmupGate = item.warmupGate;
  const isLocked = warmupGate?.isLocked === true;
  const isOpen = !isLocked && (forcedOpen || (isCardInteractive && openLocal));
  const showPrimed = !isLocked && isCardInteractive && isPrimed;
  const warmupProgress = Math.max(0, Math.min(warmupGate?.count ?? 0, warmupGate?.required ?? 0));

  useEffect(() => () => {
    if (primedTimeoutRef.current) clearTimeout(primedTimeoutRef.current);
    if (previewRetryTimeoutRef.current) clearTimeout(previewRetryTimeoutRef.current);
  }, []);

  useEffect(() => {
    if (!isLocked) return;
    const timer = window.setTimeout(() => {
      setOpenLocal(false);
      setIsPrimed(false);
    }, 0);
    return () => window.clearTimeout(timer);
  }, [isLocked]);

  const payload = asRec(item.payload);
  const metrics = asRec(payload.metrics);
  const timing = asRec(payload.timing);
  const meta = asRec(payload.meta);

  const bestMetric = resolveBestMetricFromPayload(
    metrics,
    text(payload.best_metric) || item.metricKey || 'engagement_rate',
  );
  const bestMetricObj = asRec(metrics[bestMetric]);

  const value = num(bestMetricObj.value) ?? item.metricValue;
  const baseline = num(bestMetricObj.baseline);
  const multiple = num(bestMetricObj.multiple);
  const heroMetricLabel = metricLabel(bestMetric, 'short').toUpperCase();
  const heroMetricValue = formatMetricValue(bestMetric, value, '--');
  const heroBaselineLabel = baseline == null ? '-- usual' : `${formatMetricValue(bestMetric, baseline, '--')} usual`;
  const heroMultipleLabel = multiple == null ? '--' : `${multiple.toFixed(2)}x`;
  const supportMetrics = orderedSupportMetricsFromPayload(
    metrics,
    bestMetric,
  ).map((metric) => ({
    key: metric.key,
    label: metricLabel(metric.key, 'singular'),
    value: metric.value,
    multiple: metric.multiple,
    baseline: metric.baseline,
    valueLabel: formatMetricValue(metric.key, metric.value, '--'),
    baselineLabel: formatMetricValue(metric.key, metric.baseline, '--'),
    multipleLabel: metric.multiple == null ? '--' : `${metric.multiple.toFixed(2)}x`,
  }));

  const bestInLastN = num(bestMetricObj.best_in_last_n);

  const hour = num(timing.hour);
  const hourMult = num(timing.hour_multiple);
  const hourDisplay = hour == null ? '--' : (() => { const h = ((Math.round(hour) % 24) + 24) % 24; const suffix = h >= 12 ? 'PM' : 'AM'; const twelve = h % 12 === 0 ? 12 : h % 12; return `${twelve} ${suffix}`; })();

  const delta = item.trajectoryDeltaPercentile;
  const currentTrajectory = item.surfacePercentile;

  // Lower Top % is better; the API delta is first checkpoint minus current checkpoint.
  const isPositiveShift = delta != null && delta > 0;
  const displayDeltaStr = delta == null
    ? '--'
    : delta === 0
      ? '0'
      : delta > 0
        ? `+${Math.abs(Math.round(delta))}`
        : `-${Math.abs(Math.round(delta))}`;

  const cp = item.checkpoint.toUpperCase();
  const isD1 = cp === 'D1';
  const lockedHandle = `@${(item.surfaceHandle || 'FEEDER').replace(/^@+/, '').toUpperCase()}`;
  const lockedMediaType = (item.surfaceMediaType || item.mediaType || 'POST').toUpperCase();
  const signalContextLabel = item.signalContext.toUpperCase();
  const previewUrl = text(item.previewUrl);
  const thumbnailFallbackUrl = mediaProxyUrl(text(item.postKey));
  const previewFallbackUrl = mediaProxyUrl(text(item.postKey), 'preview_5s');
  const previewBaseUrl = usePreviewFallback
    ? (previewFallbackUrl || previewUrl)
    : (previewUrl || previewFallbackUrl);
  const resolvedPreviewUrl = useMemo(() => {
    if (!previewBaseUrl) return '';
    return previewRetrySeed > 0
      ? withRetryBust(previewBaseUrl, `${item.id}-preview-${previewRetrySeed}`)
      : previewBaseUrl;
  }, [item.id, previewBaseUrl, previewRetrySeed]);
  const canSwitchToPreviewFallback = Boolean(
    previewFallbackUrl
    && previewUrl
    && previewUrl !== previewFallbackUrl,
  );
  const canPreview = !isLocked && !previewFailed && lockedMediaType === 'REEL' && Boolean(resolvedPreviewUrl);
  const canRenderInlinePreview = canPreview && !isDesktopCard;
  const shouldPlayPreview = canRenderInlinePreview && mobileAutoplayEnabled && highlighted;
  const showAutoplayToggle = !isDesktopCard && showMobileAutoplayToggle && lockedMediaType === 'REEL' && Boolean(previewUrl || previewFallbackUrl);
  const shouldMountInlinePreview = canRenderInlinePreview && (shouldPlayPreview || previewPlaying);
  const inlinePreviewPreload = 'auto' as const;
  const heroMetricStamp = heroMetricValue;
  const hideSignalChrome = item.hideSignalChrome === true;
  const postContextRead = parsePostContextRead(meta);
  const showPostMortemStreak = postContextRead?.isD7Read === true;
  const readOpen = showPostMortemStreak && expandedPostMortemId === item.id;

  const handleCardActivate = () => {
    if (isLocked) {
      play('snapLock');
      void lockControls.start({
        x: [0, -8, 8, -6, 6, -3, 3, 0],
        rotate: [0, -3, 3, -2, 2, 0],
        transition: { duration: 0.36, ease: 'easeInOut' },
      });
      return;
    }
    if (isDesktopCard) {
      onOpenDetails?.();
      return;
    }
    const nextOpen = !openLocal;
    setOpenLocal(nextOpen);
    onOpenStateChange?.(item.id, nextOpen);
  };

  const applyPreviewProgress = (progress: number) => {
    const clamped = Math.max(0, Math.min(1, progress));
    if (Math.abs(previewProgressRef.current - clamped) < 0.002) return;
    previewProgressRef.current = clamped;
    if (previewProgressRingRef.current) {
      previewProgressRingRef.current.style.background = previewProgressBackground(clamped);
    }
  };

  const resetPreviewProgress = () => {
    previewProgressRef.current = 0;
    if (previewProgressRingRef.current) {
      previewProgressRingRef.current.style.background = previewProgressBackground(0);
    }
  };

  useEffect(() => {
    if (previewRetryTimeoutRef.current) clearTimeout(previewRetryTimeoutRef.current);
    const frame = window.requestAnimationFrame(() => {
      setPreviewReady(false);
      setPreviewFailed(false);
      setPreviewPlaying(false);
      setUsePreviewFallback(false);
      setPreviewRetrySeed(0);
      resetPreviewProgress();
    });
    return () => window.cancelAnimationFrame(frame);
  }, [item.id, previewUrl, previewFallbackUrl]);

  useEffect(() => {
    previewSessionRef.current += 1;
    if (previewRetryTimeoutRef.current) {
      clearTimeout(previewRetryTimeoutRef.current);
      previewRetryTimeoutRef.current = null;
    }
  }, [item.id, resolvedPreviewUrl]);

  useEffect(() => {
    const frame = window.requestAnimationFrame(() => {
      thumbnailRetryCountRef.current = 0;
      if (isThumbnailFailureCached(thumbnailFailureKey)) {
        setImgDead(true);
        setThumbnailUrl('');
        return;
      }
      setImgDead(false);
      setThumbnailUrl(text(item.thumbnailUrl) || thumbnailFallbackUrl);
    });
    return () => window.cancelAnimationFrame(frame);
  }, [item.id, item.thumbnailUrl, thumbnailFallbackUrl, thumbnailFailureKey]);

  useEffect(() => {
    if (!highlighted || !imgDead || !thumbnailFallbackUrl) return;
    if (thumbnailRetryCountRef.current >= 2) return;
    const timeoutId = window.setTimeout(() => {
      thumbnailRetryCountRef.current += 1;
      setImgDead(false);
      setThumbnailUrl(withRetryBust(thumbnailFallbackUrl, `${item.id}-${Date.now()}`));
    }, 450);
    return () => window.clearTimeout(timeoutId);
  }, [highlighted, imgDead, item.id, thumbnailFallbackUrl]);

  useEffect(() => {
    const video = previewRef.current;
    if (!video || !resolvedPreviewUrl) return;
    video.load();
    if (video.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA) {
      const frame = window.requestAnimationFrame(() => {
        if (previewRef.current !== video) return;
        setPreviewReady(true);
        setPreviewFailed(false);
      });
      return () => window.cancelAnimationFrame(frame);
    }
  }, [item.id, resolvedPreviewUrl]);

  useEffect(() => {
    const video = previewRef.current;
    if (!video) return;
    if (previewRetryTimeoutRef.current) {
      clearTimeout(previewRetryTimeoutRef.current);
      previewRetryTimeoutRef.current = null;
    }
    if (!shouldPlayPreview) {
      video.pause();
      try {
        video.currentTime = 0;
      } catch {
        // ignore seek reset failures
      }
      const frame = window.requestAnimationFrame(() => {
        if (previewRef.current !== video) return;
        setPreviewPlaying(false);
        resetPreviewProgress();
      });
      return () => window.cancelAnimationFrame(frame);
    }
    if (!previewReady || previewFailed || !resolvedPreviewUrl) {
      return;
    }
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
    };
  }, [canSwitchToPreviewFallback, previewFailed, previewReady, previewRetrySeed, resolvedPreviewUrl, shouldPlayPreview, usePreviewFallback]);

  useEffect(() => {
    const video = previewRef.current;
    if (!video || !shouldPlayPreview || !previewPlaying) return;

    const frameVideo = video as VideoFrameAwareElement;
    let frameId = 0;
    const syncPreviewProgress = () => {
      const cycleDuration = previewCycleDuration(video);
      const cycleTime = cycleDuration > 0 ? video.currentTime % cycleDuration : 0;
      applyPreviewProgress(clampPreviewProgress(cycleTime, cycleDuration));
      if (frameVideo.requestVideoFrameCallback) {
        frameId = frameVideo.requestVideoFrameCallback(syncPreviewProgress);
        return;
      }
      frameId = window.requestAnimationFrame(syncPreviewProgress);
    };

    if (frameVideo.requestVideoFrameCallback) {
      frameId = frameVideo.requestVideoFrameCallback(syncPreviewProgress);
      return () => {
        if (frameVideo.cancelVideoFrameCallback && frameId) {
          frameVideo.cancelVideoFrameCallback(frameId);
        }
      };
    }

    frameId = window.requestAnimationFrame(syncPreviewProgress);
    return () => window.cancelAnimationFrame(frameId);
  }, [previewPlaying, shouldPlayPreview, resolvedPreviewUrl]);

  return (
    <motion.div
      role="button"
      tabIndex={0}
      onClick={handleCardActivate}
      onKeyDown={(event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          handleCardActivate();
        }
      }}
      className={isDesktopCard
        ? 'fm-fire-card-shell relative block w-full aspect-[5/6] 2xl:aspect-[11/14] overflow-hidden rounded-[22px] 2xl:rounded-[22px] text-left'
        : 'relative block w-full aspect-[4/5] overflow-hidden rounded-[28px] text-left fm-depth-glass sm:rounded-[34px]'}
      style={{
        WebkitTapHighlightColor: 'transparent',
        maxHeight: isDesktopCard ? undefined : 'var(--fire-card-max-height, 78svh)',
        aspectRatio: isDesktopCard ? undefined : 'var(--fire-card-aspect, 4 / 5)',
        boxShadow: isDesktopCard
          ? highlighted
            ? '0 24px 46px rgba(0,0,0,0.34)'
            : '0 14px 28px rgba(0,0,0,0.24)'
          : highlighted
            ? '0 18px 38px rgba(0,0,0,0.24)'
            : '0 10px 22px rgba(0,0,0,0.16)',
        willChange: isDesktopCard ? 'auto' : 'transform',
      }}
      whileTap={{ scale: 0.994 }}
      transition={{ duration: 0.08, ease: [0.22, 1, 0.36, 1] }}
    >
      {shouldMountInlinePreview ? (
        <video
          key={`${item.id}:${resolvedPreviewUrl}`}
          ref={previewRef}
          src={resolvedPreviewUrl}
          poster={thumbnailUrl}
          muted
          playsInline
          loop
          preload={inlinePreviewPreload}
          className="absolute inset-0 h-full w-full object-cover"
          style={{
            opacity: previewPlaying && shouldPlayPreview ? 1 : 0,
            transition: 'opacity 160ms ease',
            pointerEvents: 'none',
          }}
          onLoadedData={(event) => {
            if (event.currentTarget !== previewRef.current) return;
            setPreviewReady(true);
            setPreviewFailed(false);
          }}
          onLoadedMetadata={() => undefined}
          onCanPlay={(event) => {
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
            resetPreviewProgress();
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

      {thumbnailUrl && !imgDead ? (
        <motion.img
          src={thumbnailUrl}
          alt=""
          className="absolute inset-0 h-full w-full object-cover"
          loading={isDesktopCard ? 'lazy' : 'eager'}
          fetchPriority={highlighted ? 'high' : 'auto'}
          decoding="async"
          onError={() => {
            if (thumbnailFallbackUrl && !isRetryVariantOf(thumbnailUrl, thumbnailFallbackUrl)) {
              thumbnailRetryCountRef.current += 1;
              setThumbnailUrl(withRetryBust(thumbnailFallbackUrl, `${item.id}-${Date.now()}`));
              return;
            }
            rememberThumbnailFailure(thumbnailFailureKey);
            setThumbnailUrl('');
            setImgDead(true);
          }}
          animate={{
            scale: isOpen ? 1.022 : highlighted ? 1.01 : 1,
            opacity: canRenderInlinePreview && previewPlaying && shouldPlayPreview ? 0 : 1,
          }}
          transition={{ duration: 0.16, ease: [0.22, 1, 0.36, 1] }}
          style={{ willChange: isDesktopCard ? 'auto' : 'transform' }}
        />
      ) : (
        <div className="absolute inset-0 bg-black" />
      )}

      <div
        className="absolute inset-0"
        style={{
          background:
            'linear-gradient(180deg, rgba(0,0,0,0.18) 0%, rgba(0,0,0,0.1) 40%, rgba(0,0,0,0.68) 100%), radial-gradient(120% 90% at 18% 8%, rgba(0,0,0,0.3) 0%, rgba(0,0,0,0) 58%)',
        }}
      />

      {isLocked && (
        <div className="absolute inset-0 bg-black/40 dark:bg-black/54" />
      )}

      {showPostMortemStreak && (
        <span
          aria-hidden="true"
          className="fm-fire-card-cover-streak z-[4]"
        />
      )}

      <motion.div
        className={isDesktopCard ? 'absolute left-3 top-3 z-10 2xl:left-4 2xl:top-4' : 'absolute left-4 top-8 z-10 md:top-4'}
        style={{ marginTop: 'var(--pwa-top-pad)' }}
        animate={{
          opacity: isLocked ? 0.24 : !isDesktopCard && isOpen ? 0.08 : 1,
          y: !isDesktopCard && isOpen ? -10 : 0,
          scale: isLocked ? 0.96 : !isDesktopCard && isOpen ? 0.95 : 1,
        }}
        transition={{ duration: 0.22, ease: [0.22, 1, 0.36, 1] }}
      >
        <div className={isDesktopCard
          ? 'text-[clamp(46px,5vw,92px)] font-black leading-[0.82] tracking-[-0.04em] text-white drop-shadow-[0_10px_18px_rgba(0,0,0,0.58)]'
          : 'text-[clamp(108px,30vw,210px)] font-black leading-[0.76] tracking-[-0.04em] text-white drop-shadow-[0_10px_18px_rgba(0,0,0,0.58)]'}>
          {item.surfacePercentile == null ? '--' : Math.round(item.surfacePercentile)}
          <span className="ml-1 align-top text-[0.42em]">%</span>
        </div>
      </motion.div>

      {(!hideSignalChrome || showAutoplayToggle) && (
        <motion.div
          className={isDesktopCard ? 'absolute right-3 top-3 z-10 2xl:right-4 2xl:top-4' : 'absolute right-4 top-8 z-10 md:top-4'}
          style={{ marginTop: 'var(--pwa-top-pad)' }}
          animate={{
            opacity: isLocked ? 0.4 : !isDesktopCard && isOpen ? 0.24 : 1,
            y: !isDesktopCard && isOpen ? -8 : 0,
          }}
          transition={{ duration: 0.18, ease: [0.22, 1, 0.36, 1] }}
        >
          <div className="flex flex-col items-end gap-1.5">
            {!hideSignalChrome ? (
              <div className={isDesktopCard
                ? 'fm-fire-card-pill rounded-full px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-white/88 2xl:px-3 2xl:py-1.5 2xl:text-[8px] 2xl:tracking-[0.14em]'
                : 'rounded-full border border-white/32 bg-black/36 px-3 py-1.5 text-[8px] font-black uppercase tracking-[0.14em] text-white/88 shadow-[0_10px_24px_rgba(0,0,0,0.26),inset_0_1px_0_rgba(255,255,255,0.16)] backdrop-blur-[18px]'}
              >
                {signalContextLabel}
              </div>
            ) : null}
            {showAutoplayToggle ? (
              <button
                type="button"
                onClick={(event) => {
                  event.preventDefault();
                  event.stopPropagation();
                  if (!mobileAutoplayEnabled) {
                    setPreviewFailed(false);
                    setPreviewReady(false);
                    setPreviewPlaying(false);
                    resetPreviewProgress();
                    if (previewUrl || previewFallbackUrl) {
                      setPreviewRetrySeed((current) => current + 1);
                    }
                  }
                  onToggleMobileAutoplay?.(!mobileAutoplayEnabled);
                }}
                className="pointer-events-auto relative inline-flex h-10 w-10 items-center justify-center rounded-full border border-white/28 bg-white/14 text-white shadow-[0_10px_24px_rgba(0,0,0,0.24),inset_0_1px_0_rgba(255,255,255,0.34)] backdrop-blur-[18px]"
                aria-pressed={mobileAutoplayEnabled}
                aria-label={mobileAutoplayEnabled ? 'Pause reel previews' : 'Play reel previews'}
              >
                <span
                  ref={previewProgressRingRef}
                  aria-hidden="true"
                  className="absolute inset-0 rounded-full"
                  style={{
                    background: previewProgressBackground(0),
                    WebkitMask: 'radial-gradient(farthest-side, transparent calc(100% - 3px), #000 calc(100% - 3px))',
                    mask: 'radial-gradient(farthest-side, transparent calc(100% - 3px), #000 calc(100% - 3px))',
                    opacity: mobileAutoplayEnabled ? 1 : 0.72,
                  }}
                />
                <span className="absolute inset-[3px] rounded-full border border-white/18 bg-black/32 backdrop-blur-[18px]" aria-hidden="true" />
                <span className="relative z-10 inline-flex items-center justify-center">
                {mobileAutoplayEnabled ? <Pause size={14} strokeWidth={2.6} /> : <Play size={14} strokeWidth={2.6} fill="currentColor" />}
              </span>
            </button>
            ) : null}
          </div>
        </motion.div>
      )}

      <motion.div
        className={isDesktopCard
          ? 'absolute inset-x-2.5 bottom-2.5 z-10 2xl:inset-x-3 2xl:bottom-3.5'
          : 'absolute inset-x-3 bottom-8 z-10 md:bottom-6'}
        animate={{
          opacity: isLocked ? 0.14 : !isDesktopCard && isOpen ? 0.1 : 1,
          y: !isDesktopCard && isOpen ? 10 : 0,
        }}
        transition={{ duration: 0.18, ease: [0.22, 1, 0.36, 1] }}
      >
        <div className={isDesktopCard
          ? 'fm-fire-card-panel rounded-[14px] px-2.5 py-1.5 text-white/92 2xl:rounded-[14px] 2xl:px-3 2xl:py-2'
          : 'rounded-[14px] border border-white/38 bg-white/14 px-3 py-2.5 text-white/92 shadow-[0_10px_24px_rgba(0,0,0,0.24),inset_0_1px_0_rgba(255,255,255,0.48)] backdrop-blur-[16px]'}
        >
          <div className={isDesktopCard
            ? 'flex items-center justify-between gap-1.5 2xl:gap-2'
            : 'flex items-center justify-between gap-2'}
          >
            <span className={isDesktopCard
              ? 'truncate text-[8px] font-black uppercase tracking-[0.14em] text-white/90 2xl:text-[10px] 2xl:tracking-[0.14em]'
              : 'truncate text-[10px] font-black uppercase tracking-[0.14em] text-white/92'}
            >
              {lockedHandle}
            </span>
            <div className={isDesktopCard
              ? 'flex shrink-0 items-center gap-1 2xl:gap-2'
              : 'flex shrink-0 items-center gap-2'}
            >
              <span className={isDesktopCard
                ? 'rounded-[6px] bg-white/10 px-1 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-white/72 2xl:rounded-[6px] 2xl:px-1.5 2xl:tracking-[0.14em] 2xl:text-[8px]'
                : 'rounded-[6px] bg-white/10 px-1.5 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-white/78'}
              >
                {lockedMediaType}
              </span>
              <span className={isDesktopCard
                ? 'rounded-[6px] bg-white/10 px-1 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-white/72 2xl:rounded-[6px] 2xl:px-1.5 2xl:tracking-[0.14em] 2xl:text-[8px]'
                : 'rounded-[6px] bg-white/10 px-1.5 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-white/78'}
              >
                {heroMetricStamp} {heroMetricLabel}
              </span>
              <span className={isDesktopCard
                ? 'rounded-[6px] bg-white/10 px-1 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-white/72 2xl:rounded-[6px] 2xl:px-1.5 2xl:tracking-[0.14em] 2xl:text-[8px]'
                : 'rounded-[6px] bg-white/10 px-1.5 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-white/78'}
              >
                {cp}
              </span>
            </div>
          </div>
        </div>
      </motion.div>

      <AnimatePresence initial={false}>
        {isLocked && warmupGate && (
          <motion.div
            className="pointer-events-none absolute inset-0 z-20 flex items-center justify-center px-4 sm:px-5"
            initial={{ opacity: 0, scale: 0.96 }}
            animate={{ opacity: 1, scale: 1 }}
            exit={{ opacity: 0, scale: 0.96 }}
            transition={{ type: 'spring', stiffness: 340, damping: 28, mass: 0.7 }}
          >
            <motion.div
              animate={lockControls}
              className={[
                'w-full max-w-[240px] rounded-[22px] border px-4 py-4 text-center',
                /* Light: frosted white glass — image bleeds through softly */
                'border-white/60 bg-white/85',
                'shadow-[0_18px_40px_rgba(0,0,0,0.10),0_1px_0_rgba(255,255,255,0.88)_inset,0_-1px_0_rgba(0,0,0,0.04)_inset]',
                /* Dark: deep translucent black — image bleeds through with depth */
                'dark:border-white/[0.08] dark:bg-[rgba(8,8,10,0.88)]',
                'dark:shadow-[0_20px_48px_rgba(0,0,0,0.52),0_1px_0_rgba(255,255,255,0.06)_inset,0_-1px_0_rgba(0,0,0,0.5)_inset]',
              ].join(' ')}
            >
              {/* Lock icon with breathing pulse */}
              <motion.div
                animate={{ scale: [1, 1.06, 1], opacity: [0.85, 1, 0.85] }}
                transition={{ duration: 2.8, repeat: Infinity, ease: 'easeInOut' }}
                className={[
                  'mx-auto flex h-12 w-12 items-center justify-center rounded-full',
                  'border border-[var(--fm-accent)]/30 bg-[var(--fm-accent)]/12',
                  'shadow-[0_8px_22px_rgb(var(--fm-accent-rgb)/0.18),inset_0_1px_0_rgba(255,255,255,0.7)]',
                  'dark:border-[var(--fm-accent)]/22 dark:bg-[var(--fm-accent)]/10',
                  'dark:shadow-[0_8px_26px_rgb(var(--fm-accent-rgb)/0.14),inset_0_1px_0_rgba(255,255,255,0.06)]',
                ].join(' ')}
              >
                <Lock className="h-5 w-5 text-[var(--fm-accent)]" strokeWidth={2.4} />
              </motion.div>

              {/* Handle · Media · Checkpoint pill */}
              <div className="mt-3 flex justify-center">
                <div
                  className={[
                    'inline-flex max-w-full items-center gap-2 rounded-full border px-3 py-1.5',
                    'border-black/8 bg-black/[0.04] shadow-[inset_0_1px_0_rgba(255,255,255,0.6)]',
                    'dark:border-white/[0.06] dark:bg-white/[0.05] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]',
                  ].join(' ')}
                >
                  <span className="max-w-[102px] truncate text-[8px] font-black uppercase tracking-[0.14em] text-black/68 dark:text-white/72">
                    {lockedHandle}
                  </span>
                  <span className="h-1 w-1 shrink-0 rounded-full bg-[var(--fm-accent)]/40 dark:bg-[var(--fm-accent)]/30" />
                  <span className="shrink-0 text-[8px] font-black uppercase tracking-[0.14em] text-black/50 dark:text-white/52">
                    {lockedMediaType}
                  </span>
                  <span className="h-1 w-1 shrink-0 rounded-full bg-[var(--fm-accent)]/40 dark:bg-[var(--fm-accent)]/30" />
                  <span className="shrink-0 text-[8px] font-black uppercase tracking-[0.14em] text-black/50 dark:text-white/52">
                    {cp}
                  </span>
                </div>
              </div>

              <div className="mt-3 text-[18px] font-black leading-[0.92] tracking-[-0.04em] text-black dark:text-white">
                {warmupGate.headline}
              </div>
              <div className="mt-1 text-[12px] font-semibold leading-[1.25] text-black/55 dark:text-white/52">
                {warmupGate.body}
              </div>

              {/* Progress track */}
              <div className="mt-4 flex items-center gap-1.5">
                {Array.from({ length: warmupGate.required }, (_, index) => {
                  const isActive = index < warmupProgress;
                  return (
                    <span
                      key={`warmup-${item.id}-${index}`}
                      className={[
                        'h-1.5 flex-1 rounded-full transition-colors duration-200',
                        isActive
                          ? 'bg-[var(--fm-accent)] shadow-[0_0_10px_rgb(var(--fm-accent-rgb)/0.28)]'
                          : 'bg-black/10 dark:bg-white/10',
                      ].join(' ')}
                    />
                  );
                })}
              </div>

              <div className="mt-2 text-[10px] font-black uppercase tracking-[0.14em] text-black/48 dark:text-white/44">
                {warmupGate.progressLabel}
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>

      <AnimatePresence initial={false}>
        {!isDesktopCard && isOpen && (
          <motion.div
            className="absolute inset-x-2 top-2 bottom-2 z-20"
            initial={{ opacity: 0, y: 10, scale: 0.986 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -6, scale: 0.992 }}
            transition={{ type: 'spring', stiffness: 420, damping: 34, mass: 0.58 }}
            style={{ willChange: 'transform, opacity' }}
          >
            <div className="relative flex h-full flex-col overflow-hidden rounded-[22px] border border-white/80 bg-white/92 p-2 sm:p-3 shadow-[0_32px_80px_rgba(0,0,0,0.34),inset_0_1px_0_rgba(255,255,255,0.95),inset_0_-16px_32px_rgba(255,255,255,0.1)] dark:border-white/[0.08] dark:bg-[rgba(10,10,10,0.93)] dark:shadow-[0_40px_100px_rgba(0,0,0,0.8),inset_0_1px_0_rgba(255,255,255,0.1),inset_0_-1px_0_rgba(0,0,0,0.5)]">
              <div className="pointer-events-none absolute inset-0 rounded-[22px] bg-gradient-to-br from-white/90 via-white/40 to-transparent dark:from-white/10 dark:via-white/[0.02] dark:to-transparent" />
              <div className="pointer-events-none absolute inset-[1px] rounded-[22px] z-0 dark:hidden" style={{ boxShadow: 'inset 0 2px 4px rgba(255,255,255,0.7), inset 0 -2px 6px rgba(0,0,0,0.04)' }} />
              
              <div className="relative z-10 flex min-h-0 flex-1 flex-col">
              <AnimatePresence mode="wait" initial={false}>
              {readOpen && postContextRead ? (
                <motion.div
                  key="d7-read"
                  className="flex min-h-0 flex-1 flex-col"
                  initial={{ opacity: 0, y: 12, scale: 0.985 }}
                  animate={{ opacity: 1, y: 0, scale: 1 }}
                  exit={{ opacity: 0, y: -10, scale: 0.985 }}
                  transition={{ duration: 0.4, ease: [0.16, 1, 0.3, 1] }}
                  style={{ willChange: 'transform, opacity' }}
                >
                <D7ReadView
                  itemId={item.id}
                  read={postContextRead}
                  sourceLabel={postContextRead.sourceLabel}
                  onBack={() => setExpandedPostMortemId(null)}
                />
                </motion.div>
              ) : (
                <motion.div
                  key="d7-stats"
                  className="flex min-h-0 flex-1 flex-col"
                  initial={{ opacity: 0, y: 12, scale: 0.985 }}
                  animate={{ opacity: 1, y: 0, scale: 1 }}
                  exit={{ opacity: 0, y: -10, scale: 0.985 }}
                  transition={{ duration: 0.4, ease: [0.16, 1, 0.3, 1] }}
                  style={{ willChange: 'transform, opacity' }}
                >
              <div className="hide-scrollbar min-h-0 flex-1 touch-pan-y overflow-y-auto overscroll-y-contain pr-0.5">
              <div className="mb-2 sm:mb-3 overflow-hidden rounded-[18px] border border-[var(--fm-accent)]/10 bg-[var(--fm-accent)] p-2.5 shadow-[0_8px_24px_rgb(var(--fm-accent-rgb)/0.35),inset_0_2px_4px_rgba(255,255,255,0.8),inset_0_-2px_4px_rgba(136,19,55,0.4)] dark:shadow-[0_12px_32px_rgb(var(--fm-accent-rgb)/0.25),inset_0_2px_4px_rgba(255,255,255,0.8),inset_0_-2px_4px_rgba(136,19,55,0.4)] sm:p-3">
                <div className="grid grid-cols-[minmax(0,1fr)_minmax(92px,auto)] items-stretch gap-2">
                  <div className="min-w-0 py-0.5">
                    <div className="text-[10px] sm:text-[10px] font-black uppercase tracking-[0.14em] text-white/72">Performance</div>
                    <div className="mt-1 truncate text-[clamp(25px,7.3vw,40px)] font-black leading-[0.86] tracking-[-0.04em] text-white drop-shadow-sm">
                      {heroMetricValue} {heroMetricLabel}
                    </div>
                    <div className="mt-1 text-[12px] font-black uppercase leading-none tracking-[0.14em] text-white opacity-75 sm:text-[12px]">
                      {heroBaselineLabel}
                    </div>
                  </div>
                  <div className="flex min-w-[92px] flex-col justify-center rounded-[14px] border border-white/26 bg-white/14 px-2.5 py-1.5 text-right shadow-[inset_0_1px_0_rgba(255,255,255,0.28),0_10px_22px_rgba(136,19,55,0.16)]">
                    <div className="text-[clamp(30px,8.6vw,46px)] font-black leading-[0.82] tracking-[-0.04em] text-white drop-shadow-[0_8px_16px_rgba(136,19,55,0.22)]">
                      {heroMultipleLabel}
                    </div>
                    <div className="mt-1 text-[8px] sm:text-[10px] font-black uppercase leading-none tracking-[0.14em] text-white/66">
                      Multiple
                    </div>
                  </div>
                </div>
              </div>

              <motion.div
                className="grid grid-cols-12 gap-1 sm:gap-1.5"
                initial={{ opacity: 0.98, y: 4 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.12, ease: [0.22, 1, 0.36, 1] }}
              >
                {/* ── Supporting Metrics (matches desktop) ── */}
                <div className="col-span-12">
                  <div className="rounded-[14px] border border-white/70 bg-white/72 p-2.5 shadow-[0_14px_30px_rgba(0,0,0,0.2),inset_0_1px_0_rgba(255,255,255,0.82),inset_0_-8px_18px_rgb(var(--fm-accent-rgb)/0.05)] dark:border-white/18 dark:bg-black/58 dark:shadow-[0_16px_34px_rgba(0,0,0,0.62),inset_0_1px_0_rgba(255,255,255,0.12)]">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-1.5 text-[8px] sm:text-[10px] font-black uppercase tracking-[0.14em] text-foreground/72">
                        <span className="h-1.5 w-1.5 rounded-full bg-[var(--fm-accent)] shadow-[0_0_10px_rgb(var(--fm-accent-rgb)/0.45)]" />
                        Supporting Metrics
                      </div>
                      <div className="rounded-full bg-[var(--fm-accent)]/10 px-2 py-0.5 text-[8px] sm:text-[8px] font-black uppercase tracking-[0.14em] text-[var(--fm-accent)]">
                        {bestInLastN == null ? 'Best in — posts' : `Best in ${Math.max(1, Math.round(bestInLastN))} posts`}
                      </div>
                    </div>
                    <div className={[
                      'mt-2 grid gap-1.5',
                      supportMetrics.length === 1 ? 'grid-cols-1' : 'grid-cols-2',
                    ].join(' ')}>
                      {supportMetrics.map((metric) => {
                        return (
                          <div
                            key={metric.key}
                            className="rounded-[14px] border border-black/[0.04] bg-white/86 px-3 py-2 shadow-[0_8px_18px_rgba(15,23,42,0.08),inset_0_1px_0_rgba(255,255,255,0.82)] dark:border-white/10 dark:bg-white/[0.11] dark:shadow-[0_10px_22px_rgba(0,0,0,0.34),inset_0_1px_0_rgba(255,255,255,0.08)]"
                          >
                            <div className="text-[8px] sm:text-[10px] font-black uppercase tracking-[0.14em] text-foreground/54 dark:text-white/46">
                              {metric.label}
                            </div>
                            <div className="mt-1 flex items-end justify-between gap-3">
                              <div className="min-w-0">
                                <div className="text-[22px] sm:text-[22px] font-black leading-none text-foreground/96 dark:text-white/92">
                                  {metric.valueLabel}
                                </div>
                                <div className="mt-1 text-[10px] sm:text-[10px] font-black uppercase tracking-[0.14em] text-foreground/48 dark:text-white/42">
                                  {metric.baseline == null ? 'Tracked' : `${metric.baselineLabel} usual`}
                                </div>
                              </div>
                              <div className="text-right">
                                <div className="text-[22px] sm:text-[28px] font-black leading-none text-[var(--fm-accent)] drop-shadow-[0_8px_16px_rgb(var(--fm-accent-rgb)/0.16)]">
                                  {metric.multipleLabel}
                                </div>
                                <div className="mt-0.5 text-[8px] sm:text-[8px] font-black uppercase tracking-[0.14em] text-[var(--fm-accent)]/60">
                                  Lift
                                </div>
                              </div>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                </div>

                {/* ── Timing (D1) or Trajectory (D3+) — matches desktop ── */}
                {isD1 ? (
                  <>
                    <div className="col-span-12">
                      <div className="rounded-[14px] border border-white/70 bg-white/70 p-2.5 shadow-[0_14px_30px_rgba(0,0,0,0.18),inset_0_1px_0_rgba(255,255,255,0.82)] dark:border-white/18 dark:bg-black/56 dark:shadow-[0_16px_34px_rgba(0,0,0,0.58),inset_0_1px_0_rgba(255,255,255,0.12)]">
                        <div className="text-[8px] sm:text-[10px] font-black uppercase tracking-[0.14em] text-foreground/72">Timing</div>
                        <div className={`mt-1.5 grid gap-1 ${hourMult == null ? 'grid-cols-1' : 'grid-cols-2'}`}>
                          <div className="rounded-[14px] border border-white/60 bg-white/80 p-2 shadow-[inset_0_1px_0_rgba(255,255,255,0.7)] dark:border-white/10 dark:bg-white/12">
                            <div className="text-[8px] sm:text-[10px] font-black uppercase tracking-[0.14em] text-foreground/56">Post Time</div>
                            <div className="mt-1 text-[22px] sm:text-[28px] font-black leading-none text-foreground/96">
                              {hourDisplay}
                            </div>
                          </div>
                          {hourMult == null ? null : (
                            <div className="rounded-[14px] border border-[var(--fm-accent)]/12 bg-white/80 p-2 shadow-[inset_0_1px_0_rgba(255,255,255,0.7)] dark:border-[var(--fm-accent)]/18 dark:bg-white/12">
                            <div className="text-[8px] sm:text-[10px] font-black uppercase tracking-[0.14em] text-foreground/56">Time Lift</div>
                              <div className="mt-1 flex min-w-0 items-end gap-1.5 font-black leading-none">
                                <span className="text-[22px] sm:text-[28px] text-[var(--fm-accent)]">{`${hourMult.toFixed(2)}x`}</span>
                                <span className="min-w-0 truncate pb-0.5 text-[10px] sm:text-[12px] font-black uppercase tracking-[0.06em] text-foreground/48">
                                  vs usual {hour === null ? 'same-hour' : hourDisplay} posts
                                </span>
                              </div>
                            </div>
                          )}
                        </div>
                      </div>
                    </div>
                  </>
                ) : (
                  <>
                    <div className="col-span-12">
                      <div className="rounded-[14px] border border-white/70 bg-white/70 p-2.5 shadow-[0_14px_30px_rgba(0,0,0,0.18),inset_0_1px_0_rgba(255,255,255,0.82)] dark:border-white/18 dark:bg-black/56 dark:shadow-[0_16px_34px_rgba(0,0,0,0.58),inset_0_1px_0_rgba(255,255,255,0.12)]">
                        <div className="flex items-center justify-between">
                          <div className="text-[8px] sm:text-[10px] font-black uppercase tracking-[0.14em] text-foreground/72">Trajectory</div>
                          <div className={`rounded-full px-2 py-0.5 text-[8px] sm:text-[10px] font-black uppercase tracking-[0.14em] ${
                            delta != null && delta > 0 ? 'bg-emerald-500/10 text-emerald-600 dark:bg-[var(--fm-accent)]/14 dark:text-[var(--fm-accent)]'
                            : delta != null && delta < 0 ? 'bg-orange-500/12 text-orange-500 dark:bg-[#ff8a65]/12 dark:text-[#ff8a65]'
                            : 'bg-black/[0.04] text-foreground/40 dark:bg-white/[0.06]'
                          }`}>
                            {delta == null || Math.round(delta) === 0 ? 'Flat' : delta > 0 ? 'Improving' : 'Cooling'}
                          </div>
                        </div>
                        <div className="mt-2 grid grid-cols-2 gap-1.5">
                          <div>
                            <div className="rounded-[14px] border border-white/60 bg-white/78 p-2 shadow-[inset_0_1px_0_rgba(255,255,255,0.7)] dark:border-white/10 dark:bg-white/12">
                              <div className={`text-[34px] sm:text-[34px] font-black leading-none ${
                                isPositiveShift ? 'text-[var(--fm-accent)]' : 'text-foreground/96 dark:text-white/92'
                              }`}>
                                {displayDeltaStr}
                              </div>
                              <div className="mt-1 text-[8px] sm:text-[10px] font-black uppercase leading-tight tracking-[0.14em] text-foreground/40">
                                Shift vs first
                              </div>
                            </div>
                          </div>
                          <div className="text-right">
                            <div className="rounded-[14px] border border-[var(--fm-accent)]/12 bg-white/78 p-2 shadow-[inset_0_1px_0_rgba(255,255,255,0.7)] dark:border-[var(--fm-accent)]/18 dark:bg-white/12">
                              <div className="text-[22px] sm:text-[28px] font-black leading-none text-[var(--fm-accent)]">
                                {currentTrajectory == null ? '--' : `Top ${Math.round(currentTrajectory)}%`}
                              </div>
                              <div className="mt-1 text-[8px] sm:text-[10px] font-black uppercase leading-tight tracking-[0.14em] text-foreground/40">
                                Current position
                              </div>
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  </>
                )}
              </motion.div>
              {postContextRead && (
                postContextRead.isD7Read ? (
                  <D7VerdictBar
                    read={postContextRead}
                    onOpen={() => setExpandedPostMortemId(item.id)}
                  />
                ) : (
                <div className="mt-2 mb-2 sm:mb-3 rounded-[18px] border border-white/70 bg-white/76 p-2.5 shadow-[0_14px_30px_rgba(0,0,0,0.18),inset_0_1px_0_rgba(255,255,255,0.82),inset_0_-8px_18px_rgb(var(--fm-accent-rgb)/0.04)] dark:border-white/18 dark:bg-black/58 dark:shadow-[0_16px_34px_rgba(0,0,0,0.62),inset_0_1px_0_rgba(255,255,255,0.12)] sm:p-3">
                  <div className="flex items-center justify-between gap-2">
                    <div className="flex items-center gap-1.5 text-[10px] sm:text-[10px] font-black uppercase tracking-[0.14em] text-[var(--fm-accent)]">
                      <span className="h-2 w-2 rounded-full bg-[var(--fm-accent)] shadow-[0_0_10px_rgb(var(--fm-accent-rgb)/0.42)]" />
                      Post Read
                    </div>
                    <div className="rounded-full bg-black/[0.04] px-2 py-0.5 text-[8px] sm:text-[10px] font-black uppercase tracking-[0.14em] text-foreground/42 dark:bg-white/[0.06] dark:text-white/38">
                      {postContextRead.sourceLabel}
                    </div>
                  </div>

                  {postContextRead.matches.length > 0 && (
                    <div className="mt-2 rounded-[14px] border border-[var(--fm-accent)]/12 bg-white/86 px-2.5 py-2 shadow-[0_8px_18px_rgba(15,23,42,0.08),inset_0_1px_0_rgba(255,255,255,0.82)] dark:border-[var(--fm-accent)]/18 dark:bg-white/[0.1]">
                      <div className="text-[10px] sm:text-[10px] font-black uppercase tracking-[0.14em] text-[var(--fm-accent)]/72">
                        Key Read
                      </div>
                      <p className="mt-1 text-[16px] sm:text-[16px] font-black leading-[1.08] tracking-[-0.04em] text-foreground/88 dark:text-white/80">
                        {postContextRead.matches[0]}
                      </p>
                    </div>
                  )}

                  <div className="mt-2 grid gap-1.5">
                    {[
                      ...postContextRead.matches.slice(1, 3).map((line) => ({ label: 'Match', line, tone: 'text-foreground/44' })),
                      ...postContextRead.deviates.slice(0, 2).map((line) => ({ label: 'Deviation', line, tone: 'text-[var(--fm-accent)]/68' })),
                      ...postContextRead.notes.slice(0, 1).map((line) => ({ label: 'Note', line, tone: 'text-foreground/44' })),
                    ]
                      .slice(0, 4)
                      .map((field) => (
                        <div
                          key={`${item.id}-read-${field.label}-${field.line}`}
                          className="rounded-[14px] border border-black/[0.04] bg-white/74 px-2.5 py-2 shadow-[inset_0_1px_0_rgba(255,255,255,0.7)] dark:border-white/10 dark:bg-white/[0.08]"
                        >
                          <div className={`text-[10px] sm:text-[10px] font-black uppercase tracking-[0.14em] ${field.tone}`}>
                            {field.label}
                          </div>
                          <p className="mt-1 text-[14px] sm:text-[14px] font-semibold leading-[1.24] text-foreground/70 dark:text-white/60">
                            {field.line}
                          </p>
                        </div>
                      ))}
                  </div>
                </div>
                )
              )}
              </div>
                </motion.div>
              )}
              </AnimatePresence>

                <div className="col-span-12 mt-1 sm:mt-2">
                  <div
                    onClick={(event) => {
                      event.stopPropagation();
                      if (showPrimed && item.postUrl) {
                        onBeforeOpenPost?.(item.id);
                        window.open(item.postUrl, '_blank', 'noreferrer');
                        setIsPrimed(false);
                      } else {
                        event.preventDefault();
                        if (primedTimeoutRef.current) clearTimeout(primedTimeoutRef.current);
                        setIsPrimed(true);
                        primedTimeoutRef.current = window.setTimeout(() => setIsPrimed(false), 3000);
                      }
                    }}
                    className="group relative cursor-pointer pointer-events-auto flex h-10 sm:h-13 w-full items-center justify-center rounded-[18px] overflow-hidden bg-black dark:bg-[#111] shadow-[0_16px_32px_rgba(0,0,0,0.4),inset_0_2px_4px_rgba(255,255,255,0.2)] dark:shadow-[0_16px_40px_rgba(0,0,0,0.6),inset_0_1px_2px_rgba(255,255,255,0.08)] transition-transform active:scale-[0.96]"
                  >
                    <motion.div 
                      initial={{ y: '100%' }}
                      animate={{ y: showPrimed ? '0%' : '100%' }}
                      transition={{ duration: 0.3, ease: 'easeOut' }}
                      className="absolute inset-0 bg-[var(--fm-accent)] shadow-[inset_0_2px_4px_rgba(255,255,255,0.8)] z-0"
                    />
                    <span 
                      className={`relative z-10 text-[12px] font-black uppercase tracking-[0.22em] transition-colors duration-300 ${showPrimed ? 'text-black drop-shadow-sm' : 'text-[var(--fm-accent)] drop-shadow-[0_0_8px_rgb(var(--fm-accent-rgb)/0.3)] dark:text-white dark:drop-shadow-none'}`}
                    >
                      {showPrimed ? 'Tap To Open' : 'Open Post'}
                    </span>
                  </div>
                </div>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}
