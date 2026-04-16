'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { AnimatePresence, motion, useAnimationControls } from 'framer-motion';
import { Lock, Pause, Play } from 'lucide-react';
import { FireItem } from './types';
import { compact } from '@/components/fire/fireLogicHelpers';
import {
  metricLabel,
  orderedSupportMetricsFromPayload,
  resolveBestMetricFromPayload,
} from '@/components/fire/fireMetricDisplay';
import { useAppHaptics } from '@/lib/haptics';

const PREVIEW_MAX_SECONDS = 5;

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
}: FireCard3DProps) {
  const { play } = useAppHaptics();
  const [openLocal, setOpenLocal] = useState(false);
  const [imgDead, setImgDead] = useState(false);
  const [previewReady, setPreviewReady] = useState(false);
  const [previewFailed, setPreviewFailed] = useState(false);
  const [previewPlaying, setPreviewPlaying] = useState(false);
  const [usePreviewFallback, setUsePreviewFallback] = useState(false);
  const [previewRetrySeed, setPreviewRetrySeed] = useState(0);
  const [isPrimed, setIsPrimed] = useState(false);
  const [thumbnailUrl, setThumbnailUrl] = useState(text(item.thumbnailUrl));
  const primedTimeoutRef = useRef<number | null>(null);
  const previewRef = useRef<HTMLVideoElement | null>(null);
  const previewProgressRingRef = useRef<HTMLSpanElement | null>(null);
  const previewProgressRef = useRef(0);
  const previewSessionRef = useRef(0);
  const previewRetryTimeoutRef = useRef<number | null>(null);
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
  const trajectory = asRec(payload.trajectory);
  const meta = asRec(payload.meta);

  const bestMetric = resolveBestMetricFromPayload(
    metrics,
    text(payload.best_metric) || item.metricKey || 'views',
    item.surfaceMediaType || item.mediaType,
  );
  const bestMetricObj = asRec(metrics[bestMetric]);

  const value = num(bestMetricObj.value) ?? item.metricValue;
  const baseline = num(bestMetricObj.baseline);
  const multiple = num(bestMetricObj.multiple);
  const supportMetrics = orderedSupportMetricsFromPayload(
    metrics,
    bestMetric,
    item.surfaceMediaType || item.mediaType,
  ).map((metric) => ({
    key: metric.key,
    label: metricLabel(metric.key, 'singular'),
    value: metric.multiple == null ? 'x--' : `${metric.multiple.toFixed(2)}x`,
  }));

  const bestInLastN = num(bestMetricObj.best_in_last_n);

  const hour = num(timing.hour);
  const hourPct = num(timing.hour_percentile);
  const hourMult = num(timing.hour_multiple);

  const d1 = num(trajectory.d1);
  const d3 = num(trajectory.d3);
  const d7 = num(trajectory.d7);
  const rawDelta = num(trajectory.delta) ?? item.trajectoryDeltaPercentile;
  const trajectorySeries = [d1, d3, d7];
  const firstTraj = trajectorySeries.find((v) => v != null) ?? null;
  const lastTraj = [...trajectorySeries].reverse().find((v) => v != null) ?? null;
  const computedDelta =
    firstTraj != null && lastTraj != null && trajectorySeries.filter((v) => v != null).length > 1
      ? lastTraj - firstTraj
      : null;
  const delta = computedDelta ?? rawDelta;

  // Lower percentile is better. Negative delta means improvement.
  const isPositiveShift = delta != null && delta < 0;
  const displayDeltaStr = delta == null
    ? '--'
    : delta === 0
      ? '0'
      : delta < 0
        ? `+${Math.abs(Math.round(delta))}`
        : `-${Math.abs(Math.round(delta))}`;

  const cp = item.checkpoint.toUpperCase();
  const isD1 = cp === 'D1';
  const lockedHandle = `@${(item.surfaceHandle || 'FEEDER').replace(/^@+/, '').toUpperCase()}`;
  const lockedMediaType = (item.surfaceMediaType || item.mediaType || 'POST').toUpperCase();
  const signalContextLabel = item.signalContext.toUpperCase();
  const signalLabel = item.signalLabel || 'Signal';
  const signalHeadline = item.signalHeadline || item.title || signalLabel;
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
  const inlinePreviewPreload: 'auto' = 'auto';
  const heroMetricStamp = value == null ? '--' : compact(value);
  const hideSignalChrome = item.hideSignalChrome === true;
  const patternAlerts = Array.isArray(meta.pattern_alerts)
    ? meta.pattern_alerts
      .map((entry) => asRec(entry))
      .filter((entry) => Object.keys(entry).length > 0)
    : [];
  const primaryPattern = patternAlerts.length > 0 ? patternAlerts[0] : null;
  const patternCueLabels = primaryPattern && Array.isArray(primaryPattern.cues)
    ? primaryPattern.cues
      .map((cue) => asRec(cue))
      .map((cue) => text(cue.label))
      .filter(Boolean)
      .slice(0, 4)
    : [];
  const supportPosts = primaryPattern && Array.isArray(primaryPattern.support_posts)
    ? primaryPattern.support_posts
      .map((post) => asRec(post))
      .map((post) => ({
        postKey: text(post.post_key),
        handle: text(post.handle),
        mediaType: text(post.media_type).toUpperCase(),
        postUrl: text(post.post_url),
        thumbnailUrl: text(post.thumbnail_url),
      }))
      .filter((post) => post.postKey && post.postUrl)
      .slice(0, 4)
    : [];
  const patternMatchCount = num(primaryPattern?.match_count);
  const patternFeedersCount = num(primaryPattern?.feeders_count);
  const patternAverage = num(primaryPattern?.avg_hot_percentile);
  const patternBaselineShare = num(primaryPattern?.baseline_share);
  const patternRecentLift = num(primaryPattern?.recent_lift);
  const patternAnchorGap = num(primaryPattern?.anchor_gap);

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
    setOpenLocal((v) => !v);
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
    setPreviewReady(false);
    setPreviewFailed(false);
    setPreviewPlaying(false);
    setUsePreviewFallback(false);
    setPreviewRetrySeed(0);
    resetPreviewProgress();
  }, [item.id, previewUrl, previewFallbackUrl]);

  useEffect(() => {
    previewSessionRef.current += 1;
    if (previewRetryTimeoutRef.current) {
      clearTimeout(previewRetryTimeoutRef.current);
      previewRetryTimeoutRef.current = null;
    }
  }, [item.id, resolvedPreviewUrl]);

  useEffect(() => {
    setImgDead(false);
    setThumbnailUrl(text(item.thumbnailUrl) || thumbnailFallbackUrl);
  }, [item.id, item.thumbnailUrl, thumbnailFallbackUrl]);

  useEffect(() => {
    if (!highlighted || !imgDead || !thumbnailFallbackUrl) return;
    const timeoutId = window.setTimeout(() => {
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
      setPreviewReady(true);
      setPreviewFailed(false);
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
      setPreviewPlaying(false);
      resetPreviewProgress();
      video.pause();
      try {
        video.currentTime = 0;
      } catch {
        // ignore seek reset failures
      }
      return;
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
        ? 'relative block w-full aspect-[11/14] overflow-hidden rounded-[24px] text-left fm-depth-glass'
        : 'relative block w-full aspect-[4/5] overflow-hidden rounded-[26px] text-left fm-depth-glass sm:rounded-[32px]'}
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
        willChange: 'transform',
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
            if (thumbnailFallbackUrl && thumbnailUrl !== thumbnailFallbackUrl) {
              setThumbnailUrl(withRetryBust(thumbnailFallbackUrl, `${item.id}-${Date.now()}`));
              return;
            }
            setImgDead(true);
          }}
          animate={{
            scale: isOpen ? 1.022 : highlighted ? 1.01 : 1,
            opacity: canRenderInlinePreview && previewPlaying && shouldPlayPreview ? 0 : 1,
          }}
          transition={{ duration: 0.16, ease: [0.22, 1, 0.36, 1] }}
          style={{ willChange: 'transform' }}
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

      <motion.div
        className={isDesktopCard ? 'absolute left-4 top-4 z-10' : 'absolute left-4 top-8 z-10 md:top-4'}
        style={{ marginTop: 'var(--pwa-top-pad)' }}
        animate={{
          opacity: isLocked ? 0.24 : !isDesktopCard && isOpen ? 0.08 : 1,
          y: !isDesktopCard && isOpen ? -10 : 0,
          scale: isLocked ? 0.96 : !isDesktopCard && isOpen ? 0.95 : 1,
        }}
        transition={{ duration: 0.22, ease: [0.22, 1, 0.36, 1] }}
      >
        <div className={isDesktopCard
          ? 'text-[clamp(50px,4.6vw,82px)] font-black leading-[0.82] tracking-[-0.055em] text-white drop-shadow-[0_10px_18px_rgba(0,0,0,0.58)]'
          : 'text-[clamp(108px,30vw,210px)] font-black leading-[0.76] tracking-[-0.055em] text-white drop-shadow-[0_10px_18px_rgba(0,0,0,0.58)]'}>
          {item.surfacePercentile == null ? '--' : Math.round(item.surfacePercentile)}
          <span className="ml-1 align-top text-[0.42em]">%</span>
        </div>
      </motion.div>

      {(!hideSignalChrome || showAutoplayToggle) && (
        <motion.div
          className={isDesktopCard ? 'absolute right-4 top-4 z-10' : 'absolute right-4 top-8 z-10 md:top-4'}
          style={{ marginTop: 'var(--pwa-top-pad)' }}
          animate={{
            opacity: isLocked ? 0.4 : !isDesktopCard && isOpen ? 0.24 : 1,
            y: !isDesktopCard && isOpen ? -8 : 0,
          }}
          transition={{ duration: 0.18, ease: [0.22, 1, 0.36, 1] }}
        >
          <div className="flex flex-col items-end gap-1.5">
            {!hideSignalChrome ? (
              <div className="rounded-full border border-white/32 bg-black/36 px-3 py-1.5 text-[8px] font-black uppercase tracking-[0.16em] text-white/88 shadow-[0_10px_24px_rgba(0,0,0,0.26),inset_0_1px_0_rgba(255,255,255,0.16)] backdrop-blur-[18px]">
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
          ? 'absolute inset-x-3 bottom-3.5 z-10'
          : 'absolute inset-x-3 bottom-8 z-10 md:bottom-6'}
        animate={{
          opacity: isLocked ? 0.14 : !isDesktopCard && isOpen ? 0.1 : 1,
          y: !isDesktopCard && isOpen ? 10 : 0,
        }}
        transition={{ duration: 0.18, ease: [0.22, 1, 0.36, 1] }}
      >
        <div className={isDesktopCard
          ? 'rounded-[14px] border border-white/24 bg-black/42 px-3 py-2 text-white/92 shadow-[0_12px_24px_rgba(0,0,0,0.34),inset_0_1px_0_rgba(255,255,255,0.12)] backdrop-blur-[18px]'
          : 'rounded-[14px] border border-white/38 bg-white/14 px-3 py-2.5 text-white/92 shadow-[0_10px_24px_rgba(0,0,0,0.24),inset_0_1px_0_rgba(255,255,255,0.48)] backdrop-blur-[16px]'}
        >
          <div className="flex items-center justify-between gap-2">
            <span className={isDesktopCard
              ? 'truncate text-[9px] font-black uppercase tracking-[0.1em] text-white/90'
              : 'truncate text-[10px] font-black uppercase tracking-[0.1em] text-white/92'}
            >
              {lockedHandle}
            </span>
            <div className="flex shrink-0 items-center gap-2">
              <span className={isDesktopCard
                ? 'rounded-[6px] bg-white/10 px-1.5 py-0.5 text-[8px] font-black uppercase tracking-[0.1em] text-white/72'
                : 'rounded-[6px] bg-white/10 px-1.5 py-0.5 text-[8px] font-black uppercase tracking-[0.1em] text-white/78'}
              >
                {lockedMediaType}
              </span>
              <span className={isDesktopCard
                ? 'rounded-[6px] bg-white/10 px-1.5 py-0.5 text-[8px] font-black uppercase tracking-[0.1em] text-white/72'
                : 'rounded-[6px] bg-white/10 px-1.5 py-0.5 text-[8px] font-black uppercase tracking-[0.1em] text-white/78'}
              >
                {heroMetricStamp} {bestMetric.toUpperCase()}
              </span>
              <span className={isDesktopCard
                ? 'rounded-[6px] bg-white/10 px-1.5 py-0.5 text-[8px] font-black uppercase tracking-[0.1em] text-white/72'
                : 'rounded-[6px] bg-white/10 px-1.5 py-0.5 text-[8px] font-black uppercase tracking-[0.1em] text-white/78'}
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
                'w-full max-w-[240px] rounded-[24px] border px-4 py-4 text-center',
                /* Light: frosted white glass — image bleeds through softly */
                'border-white/60 bg-white/52 backdrop-blur-[16px] backdrop-saturate-[160%]',
                'shadow-[0_18px_40px_rgba(0,0,0,0.10),0_1px_0_rgba(255,255,255,0.88)_inset,0_-1px_0_rgba(0,0,0,0.04)_inset]',
                /* Dark: deep translucent black — image bleeds through with depth */
                'dark:border-white/[0.08] dark:bg-[rgba(6,6,6,0.52)] dark:backdrop-blur-[16px] dark:backdrop-saturate-[140%]',
                'dark:shadow-[0_20px_48px_rgba(0,0,0,0.52),0_1px_0_rgba(255,255,255,0.06)_inset,0_-1px_0_rgba(0,0,0,0.5)_inset]',
              ].join(' ')}
            >
              {/* Lock icon with breathing pulse */}
              <motion.div
                animate={{ scale: [1, 1.06, 1], opacity: [0.85, 1, 0.85] }}
                transition={{ duration: 2.8, repeat: Infinity, ease: 'easeInOut' }}
                className={[
                  'mx-auto flex h-12 w-12 items-center justify-center rounded-full',
                  'border border-[#E11D48]/30 bg-[#E11D48]/12',
                  'shadow-[0_8px_22px_rgba(225,29,72,0.18),inset_0_1px_0_rgba(255,255,255,0.7)]',
                  'dark:border-[#E11D48]/22 dark:bg-[#E11D48]/10',
                  'dark:shadow-[0_8px_26px_rgba(225,29,72,0.14),inset_0_1px_0_rgba(255,255,255,0.06)]',
                ].join(' ')}
              >
                <Lock className="h-5 w-5 text-[#E11D48]" strokeWidth={2.4} />
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
                  <span className="h-1 w-1 shrink-0 rounded-full bg-[#E11D48]/40 dark:bg-[#E11D48]/30" />
                  <span className="shrink-0 text-[8px] font-black uppercase tracking-[0.14em] text-black/50 dark:text-white/52">
                    {lockedMediaType}
                  </span>
                  <span className="h-1 w-1 shrink-0 rounded-full bg-[#E11D48]/40 dark:bg-[#E11D48]/30" />
                  <span className="shrink-0 text-[8px] font-black uppercase tracking-[0.14em] text-black/50 dark:text-white/52">
                    {cp}
                  </span>
                </div>
              </div>

              <div className="mt-3 text-[18px] font-black leading-[0.92] tracking-[-0.03em] text-black dark:text-white">
                {warmupGate.headline}
              </div>
              <div className="mt-1 text-[11px] font-semibold leading-[1.25] text-black/55 dark:text-white/52">
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
                          ? 'bg-[#E11D48] shadow-[0_0_10px_rgba(225,29,72,0.28)]'
                          : 'bg-black/10 dark:bg-white/10',
                      ].join(' ')}
                    />
                  );
                })}
              </div>

              <div className="mt-2 text-[10px] font-black uppercase tracking-[0.16em] text-black/48 dark:text-white/44">
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
            <div className="relative flex h-full flex-col overflow-hidden rounded-[24px] border border-white/80 bg-white/70 p-2 sm:p-3 shadow-[0_32px_80px_rgba(0,0,0,0.34),inset_0_1px_0_rgba(255,255,255,0.95),inset_0_-16px_32px_rgba(255,255,255,0.1)] backdrop-blur-[48px] backdrop-saturate-[220%] dark:border-white/[0.08] dark:bg-[rgba(10,10,10,0.75)] dark:shadow-[0_40px_100px_rgba(0,0,0,0.8),inset_0_1px_0_rgba(255,255,255,0.1),inset_0_-1px_0_rgba(0,0,0,0.5)]">
              <div className="pointer-events-none absolute inset-0 rounded-[24px] bg-gradient-to-br from-white/90 via-white/40 to-transparent dark:from-white/10 dark:via-white/[0.02] dark:to-transparent" />
              <div className="pointer-events-none absolute inset-[1px] rounded-[23px] z-0 dark:hidden" style={{ boxShadow: 'inset 0 2px 4px rgba(255,255,255,0.7), inset 0 -2px 6px rgba(0,0,0,0.04)' }} />
              
              <div className="relative z-10 flex min-h-0 flex-1 flex-col">
              <div className="hide-scrollbar min-h-0 flex-1 overflow-y-auto pr-0.5">
              {!hideSignalChrome && (
                <div className="mb-2 sm:mb-3 rounded-[16px] border border-white/60 bg-white/56 p-2.5 sm:p-3 shadow-[0_12px_28px_rgba(0,0,0,0.18),inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/18 dark:bg-black/42 dark:shadow-[0_14px_30px_rgba(0,0,0,0.58),inset_0_1px_0_rgba(255,255,255,0.1)]">
                  <div className="flex items-center justify-between gap-2">
                    <div className="text-[8px] sm:text-[9px] font-black uppercase tracking-[0.16em] text-foreground/62">
                      {signalLabel}
                    </div>
                    <div className="text-[8px] sm:text-[9px] font-black uppercase tracking-[0.16em] text-foreground/44">
                      {signalContextLabel}
                    </div>
                  </div>
                  <div className="mt-1.5 text-[16px] sm:text-[18px] font-black leading-[0.94] tracking-[-0.025em] text-foreground/95">
                    {signalHeadline}
                  </div>
                  <div className="mt-1.5 text-[10px] sm:text-[11px] font-semibold leading-[1.35] text-foreground/62">
                    {item.whyNow}
                  </div>
                </div>
              )}

              {primaryPattern && (
                <div className="mb-2 sm:mb-3 rounded-[16px] border border-white/60 bg-white/50 p-2.5 sm:p-3 shadow-[0_12px_28px_rgba(0,0,0,0.16),inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/18 dark:bg-black/38 dark:shadow-[0_14px_30px_rgba(0,0,0,0.56),inset_0_1px_0_rgba(255,255,255,0.1)]">
                  <div className="flex items-center justify-between gap-2">
                    <div className="text-[8px] sm:text-[9px] font-black uppercase tracking-[0.16em] text-foreground/62">
                      Pattern Proof
                    </div>
                    <div className="text-[8px] sm:text-[9px] font-black uppercase tracking-[0.16em] text-foreground/42">
                      {patternMatchCount == null ? '--' : `${Math.round(patternMatchCount)} winners`}
                    </div>
                  </div>
                  <div className="mt-2 flex flex-wrap gap-1.5">
                    {patternAverage != null && (
                      <span className="rounded-full bg-black/8 px-2 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/74 dark:bg-white/10 dark:text-white/78">
                        Avg top {Math.round(patternAverage)}%
                      </span>
                    )}
                    {patternFeedersCount != null && patternFeedersCount > 1 && (
                      <span className="rounded-full bg-black/8 px-2 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/74 dark:bg-white/10 dark:text-white/78">
                        {Math.round(patternFeedersCount)} feeders
                      </span>
                    )}
                    {patternAnchorGap != null && (
                      <span className="rounded-full bg-black/8 px-2 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/74 dark:bg-white/10 dark:text-white/78">
                        Gap +{Math.round(patternAnchorGap)} pts
                      </span>
                    )}
                    {patternRecentLift != null && patternRecentLift > 0 && (
                      <span className="rounded-full bg-black/8 px-2 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/74 dark:bg-white/10 dark:text-white/78">
                        14d lift {patternRecentLift.toFixed(1)}x
                      </span>
                    )}
                    {patternBaselineShare != null && (
                      <span className="rounded-full bg-black/8 px-2 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/74 dark:bg-white/10 dark:text-white/78">
                        Baseline {Math.round(patternBaselineShare * 100)}%
                      </span>
                    )}
                  </div>
                  {patternCueLabels.length > 0 && (
                    <div className="mt-2 flex flex-wrap gap-1.5">
                      {patternCueLabels.map((label) => (
                        <span
                          key={`${item.id}-${label}`}
                          className="rounded-full border border-black/8 bg-white/62 px-2 py-1 text-[8px] font-black uppercase tracking-[0.13em] text-foreground/72 shadow-[inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/12 dark:bg-white/10 dark:text-white/72"
                        >
                          {label}
                        </span>
                      ))}
                    </div>
                  )}
                  {supportPosts.length > 0 && (
                    <div className="mt-2.5">
                      <div className="text-[8px] sm:text-[9px] font-black uppercase tracking-[0.16em] text-foreground/48">
                        Supporting Posts
                      </div>
                      <div className="hide-scrollbar mt-2 -mx-0.5 flex gap-2 overflow-x-auto pb-1">
                        {supportPosts.map((post) => (
                          <button
                            key={`${item.id}-${post.postKey}`}
                            type="button"
                            onClick={(event) => {
                              event.stopPropagation();
                              if (post.postUrl) window.open(post.postUrl, '_blank', 'noreferrer');
                            }}
                            className="shrink-0 text-left"
                          >
                            <div className="h-20 w-16 overflow-hidden rounded-[12px] border border-white/55 bg-black/12 shadow-[0_8px_18px_rgba(0,0,0,0.18)] dark:border-white/12 dark:bg-white/8">
                              {post.thumbnailUrl ? (
                                <img src={post.thumbnailUrl} alt="" className="h-full w-full object-cover" />
                              ) : (
                                <div className="h-full w-full bg-black/12 dark:bg-white/8" />
                              )}
                            </div>
                            <div className="mt-1 max-w-16 truncate text-[8px] font-black uppercase tracking-[0.13em] text-foreground/68 dark:text-white/70">
                              @{(post.handle || 'feed').toUpperCase()}
                            </div>
                          </button>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              )}

              <div className="mb-2 sm:mb-3 rounded-[16px] bg-[#E11D48] p-2.5 sm:p-3 shadow-[0_8px_24px_rgba(225,29,72,0.35),inset_0_2px_4px_rgba(255,255,255,0.8),inset_0_-2px_4px_rgba(136,19,55,0.4)] dark:shadow-[0_12px_32px_rgba(225,29,72,0.25),inset_0_2px_4px_rgba(255,255,255,0.8),inset_0_-2px_4px_rgba(136,19,55,0.4)] border border-[#E11D48]/10">
                <div className="text-[9px] sm:text-[10px] font-black uppercase tracking-[0.16em] text-white/72">Performance</div>
                <div className="mt-0.5 text-[clamp(28px,8.2vw,46px)] font-black leading-[0.88] tracking-[-0.04em] text-white drop-shadow-sm">
                  {compact(value)} {bestMetric.toUpperCase()}
                </div>
                <div className="mt-1 flex items-end gap-2 text-[clamp(11px,3.3vw,19px)] font-black leading-none">
                  <span className="text-white/72">{compact(baseline)} USUAL</span>
                  <span className="text-white">{multiple == null ? '--' : multiple.toFixed(2)}× MULTIPLE</span>
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
                  <div className="rounded-[11px] border border-white/55 bg-white/45 p-2 sm:p-2.5 shadow-[0_12px_28px_rgba(0,0,0,0.22),inset_0_1px_0_rgba(255,255,255,0.7),inset_0_-8px_16px_rgba(255,255,255,0.12)] dark:border-white/22 dark:bg-black/45 dark:shadow-[0_14px_30px_rgba(0,0,0,0.6),inset_0_1px_0_rgba(255,255,255,0.16)]">
                    <div className="flex items-center justify-between">
                      <div className="text-[7px] sm:text-[8px] font-black uppercase tracking-[0.14em] text-foreground/70">Supporting Metrics</div>
                      <div className="text-[7px] sm:text-[8px] font-bold uppercase tracking-[0.12em] text-foreground/40">
                        {bestInLastN == null ? 'Best in — posts' : `Best in ${Math.max(1, Math.round(bestInLastN))} posts`}
                      </div>
                    </div>
                    <div className="mt-1.5 grid grid-cols-2 gap-1">
                      {supportMetrics.map((metric) => {
                        const metricObj = asRec(metrics[metric.key]);
                        const metricValue = num(metricObj.value);
                        return (
                          <div key={metric.key} className="rounded-[8px] bg-white/55 p-1.5 sm:p-2 shadow-[inset_0_1px_0_rgba(255,255,255,0.6)] dark:bg-white/14">
                            <div className="text-[7px] sm:text-[8px] font-black uppercase tracking-[0.14em] text-foreground/60">{metric.label}</div>
                            <div className="mt-0.5 flex items-end justify-between gap-1">
                              <div className="text-[14px] sm:text-[16px] font-black leading-none text-foreground/90">
                                {metricValue == null ? '--' : compact(metricValue)}
                              </div>
                              <div className="text-[16px] sm:text-[20px] font-black leading-none tracking-[-0.03em] text-foreground/95">
                                {metric.value}
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
                      <div className="rounded-[11px] border border-white/55 bg-white/45 p-2 sm:p-2.5 shadow-[0_12px_28px_rgba(0,0,0,0.22),inset_0_1px_0_rgba(255,255,255,0.7)] dark:border-white/22 dark:bg-black/45 dark:shadow-[0_14px_30px_rgba(0,0,0,0.6),inset_0_1px_0_rgba(255,255,255,0.16)]">
                        <div className="text-[7px] sm:text-[8px] font-black uppercase tracking-[0.14em] text-foreground/70">Timing</div>
                        <div className="mt-1.5 grid grid-cols-3 gap-1">
                          <div className="rounded-[8px] bg-white/55 p-1.5 sm:p-2 shadow-[inset_0_1px_0_rgba(255,255,255,0.6)] dark:bg-white/14">
                            <div className="text-[7px] sm:text-[8px] font-black uppercase tracking-[0.12em] text-foreground/60">Post Time</div>
                            <div className="mt-0.5 text-[16px] sm:text-[20px] font-black leading-none text-foreground/95">
                              {hour == null ? '--' : (() => { const h = ((Math.round(hour) % 24) + 24) % 24; const suffix = h >= 12 ? 'PM' : 'AM'; const twelve = h % 12 === 0 ? 12 : h % 12; return `${twelve} ${suffix}`; })()}
                            </div>
                          </div>
                          <div className="rounded-[8px] bg-white/55 p-1.5 sm:p-2 shadow-[inset_0_1px_0_rgba(255,255,255,0.6)] dark:bg-white/14">
                            <div className="text-[7px] sm:text-[8px] font-black uppercase tracking-[0.12em] text-foreground/60">Hour %</div>
                            <div className="mt-0.5 text-[16px] sm:text-[20px] font-black leading-none text-foreground/95">
                              {hourPct == null ? '--' : `Top ${Math.round(hourPct)}%`}
                            </div>
                          </div>
                          <div className="rounded-[8px] bg-white/55 p-1.5 sm:p-2 shadow-[inset_0_1px_0_rgba(255,255,255,0.6)] dark:bg-white/14">
                            <div className="text-[7px] sm:text-[8px] font-black uppercase tracking-[0.12em] text-foreground/60">Hour Mult.</div>
                            <div className="mt-0.5 text-[16px] sm:text-[20px] font-black leading-none text-foreground/95">
                              {hourMult == null ? '--' : `${hourMult.toFixed(2)}x`}
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  </>
                ) : (
                  <>
                    <div className="col-span-12">
                      <div className="rounded-[11px] border border-white/55 bg-white/45 p-2 sm:p-2.5 shadow-[0_12px_28px_rgba(0,0,0,0.22),inset_0_1px_0_rgba(255,255,255,0.7)] dark:border-white/22 dark:bg-black/45 dark:shadow-[0_14px_30px_rgba(0,0,0,0.6),inset_0_1px_0_rgba(255,255,255,0.16)]">
                        <div className="flex items-center justify-between">
                          <div className="text-[7px] sm:text-[8px] font-black uppercase tracking-[0.14em] text-foreground/70">Trajectory</div>
                          <div className={`text-[8px] sm:text-[9px] font-bold uppercase tracking-[0.12em] ${
                            delta != null && delta < 0 ? 'text-emerald-600 dark:text-[#E11D48]'
                            : delta != null && delta > 0 ? 'text-orange-500 dark:text-[#ff8a65]'
                            : 'text-foreground/40'
                          }`}>
                            {delta == null || Math.round(delta) === 0 ? 'Flat' : delta < 0 ? 'Improving' : 'Cooling'}
                          </div>
                        </div>
                        <div className="mt-1.5 flex items-end justify-between gap-3">
                          <div>
                            <div className={`text-[28px] sm:text-[32px] font-black leading-none tracking-[-0.04em] ${
                              isPositiveShift ? 'text-foreground dark:text-[#E11D48]' : 'text-foreground/95'
                            }`}>
                              {displayDeltaStr}
                            </div>
                            <div className="mt-0.5 text-[8px] sm:text-[9px] font-medium text-foreground/40">
                              Shift vs first checkpoint
                            </div>
                          </div>
                          <div className="text-right">
                            <div className="text-[16px] sm:text-[18px] font-black leading-none tracking-[-0.03em] text-foreground/90">
                              {lastTraj == null ? '--' : `Top ${Math.round(lastTraj)}%`}
                            </div>
                            <div className="mt-0.5 text-[8px] sm:text-[9px] font-medium text-foreground/40">
                              Current position
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  </>
                )}
              </motion.div>
              </div>

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
                    className="group relative cursor-pointer pointer-events-auto flex h-10 sm:h-13 w-full items-center justify-center rounded-[16px] overflow-hidden bg-black dark:bg-[#111] shadow-[0_16px_32px_rgba(0,0,0,0.4),inset_0_2px_4px_rgba(255,255,255,0.2)] dark:shadow-[0_16px_40px_rgba(0,0,0,0.6),inset_0_1px_2px_rgba(255,255,255,0.08)] transition-transform active:scale-[0.96]"
                  >
                    <motion.div 
                      initial={{ y: '100%' }}
                      animate={{ y: showPrimed ? '0%' : '100%' }}
                      transition={{ duration: 0.3, ease: 'easeOut' }}
                      className="absolute inset-0 bg-[#E11D48] shadow-[inset_0_2px_4px_rgba(255,255,255,0.8)] z-0"
                    />
                    <span 
                      className={`relative z-10 text-[11px] font-black uppercase tracking-[0.2em] transition-colors duration-300 ${showPrimed ? 'text-black drop-shadow-sm' : 'text-[#E11D48] drop-shadow-[0_0_8px_rgba(225,29,72,0.3)] dark:text-white dark:drop-shadow-none'}`}
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
