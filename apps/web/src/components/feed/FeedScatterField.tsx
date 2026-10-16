'use client';

import {
  memo,
  startTransition,
  useDeferredValue,
  useId,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { Eye, Film, Grid2X2, Heart, Image as ImageIcon, Layers, MessageCircle } from 'lucide-react';
import {
  BEAT_ARRIVE_S,
  BEAT_EASE,
  BEAT_EASE_CSS,
  BEAT_LEAVE_EASE,
  BEAT_LEAVE_S,
  BEAT_MAX_STEPS,
  beatDelay,
} from '@/lib/motion';
import { ScatterPoint } from './dashboardTypes';

type Blip = {
  id: string;
  x: number;
  y: number;
  isCompetitor: boolean;
  percentile: number | null;
  handle: string;
  date: string;
  views: number;
  likes: number | null;
  comments: number | null;
  mediaType: PointMediaType;
  daysAgo: number;
  postIndex: number; // 0 = most recent
};

function formatPointDate(value: string | null): string {
  if (!value) return 'N/A';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return 'N/A';
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

const VISIBLE_POST_LIMIT = 15;
type MediaFilter = 'all' | 'reel' | 'image' | 'carousel';
type PointMediaType = Exclude<MediaFilter, 'all'> | 'unknown';
const TOP_ZONE_PERCENT = 50;

const MEDIA_FILTERS = [
  { key: 'all', label: 'All', Icon: Grid2X2 },
  { key: 'reel', label: 'Reels', Icon: Film },
  { key: 'image', label: 'Images', Icon: ImageIcon },
  { key: 'carousel', label: 'Carousels', Icon: Layers },
] satisfies Array<{ key: MediaFilter; label: string; Icon: typeof Grid2X2 }>;

// Tooltip: opacity plus one transform string (compositor), on the beat.
const TIP_HIDDEN = { opacity: 0, transform: 'translateY(10px) scale(1)' };
const TIP_SHOWN = { opacity: 1, transform: 'translateY(0px) scale(1)', transition: { duration: BEAT_ARRIVE_S, ease: BEAT_EASE } };
const TIP_GONE = { opacity: 0, transform: 'translateY(0px) scale(0.95)', transition: { duration: BEAT_LEAVE_S, ease: BEAT_LEAVE_EASE } };

const DOT_ENTER_ID = 'fm-scatter-dot-enter';
const DOT_MOVE_ID = 'fm-scatter-dot-move';
// The three latest posts get one slow ring "breath" after they land — the old 3.6s pulse, played once.
const RING_BREATH_MS = 3600;

// The compact layout reads the viewport through useSyncExternalStore, so a dot mounts at its final spot on the first
// client render instead of jumping there one effect later.
const COMPACT_QUERY = '(max-width: 768px)';
let compactQueryList: MediaQueryList | null = null;

function compactMedia(): MediaQueryList {
  compactQueryList ??= window.matchMedia(COMPACT_QUERY);
  return compactQueryList;
}

function subscribeCompact(onChange: () => void) {
  const media = compactMedia();
  if (typeof media.addEventListener === 'function') {
    media.addEventListener('change', onChange);
    return () => media.removeEventListener('change', onChange);
  }
  media.addListener(onChange);
  return () => media.removeListener(onChange);
}

function getCompact() {
  return compactMedia().matches;
}

function getCompactOnServer() {
  return false;
}

function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max);
}

function normalizePointMediaType(value: string | null | undefined): PointMediaType {
  const normalized = (value || '').trim().toLowerCase();
  if (!normalized) return 'unknown';
  if (normalized.includes('sidecar') || normalized.includes('carousel')) return 'carousel';
  if (normalized.includes('reel') || normalized.includes('video')) return 'reel';
  if (normalized.includes('image') || normalized.includes('photo')) return 'image';
  return 'unknown';
}

function mediaLabel(value: PointMediaType): string {
  if (value === 'reel') return 'Reel';
  if (value === 'image') return 'Image';
  if (value === 'carousel') return 'Carousel';
  return 'Post';
}

function formatCompactMetric(value: number | null | undefined): string {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) return '--';
  if (numeric >= 1_000_000) return `${(numeric / 1_000_000).toFixed(numeric >= 10_000_000 ? 0 : 1)}M`;
  if (numeric >= 1_000) return `${(numeric / 1_000).toFixed(numeric >= 100_000 ? 0 : 1)}K`;
  return `${Math.round(numeric)}`;
}

function tooltipMetrics(point: Blip) {
  const base = [
    { key: 'likes', label: 'Likes', value: point.likes, Icon: Heart },
    { key: 'comments', label: 'Comms', value: point.comments, Icon: MessageCircle },
  ];
  if (point.mediaType === 'reel') {
    return [
      { key: 'views', label: 'Views', value: point.views, Icon: Eye },
      ...base,
    ];
  }
  return base;
}

function layoutPointX(index: number, count: number, compact: boolean) {
  const edgePadding = compact ? 10 : 8;
  if (count <= 1) return 100 - edgePadding;
  const latestToOlderRatio = 1 - index / Math.max(1, count - 1);
  return edgePadding + latestToOlderRatio * (100 - edgePadding * 2);
}

function reducedMotionPreferred(): boolean {
  return typeof window !== 'undefined' && window.matchMedia?.('(prefers-reduced-motion: reduce)').matches === true;
}

function FeedScatterField({ points }: { points: ScatterPoint[] }) {
  const instanceId = useId().replace(/[:]/g, '');
  const [mediaFilter, setMediaFilter] = useState<MediaFilter>('all');
  const [hoveredPoint, setHoveredPoint] = useState<Blip | null>(null);
  const isCompactViewport = useSyncExternalStore(subscribeCompact, getCompact, getCompactOnServer);
  const deferredMediaFilter = useDeferredValue(mediaFilter);
  const plotRef = useRef<HTMLDivElement>(null);
  const dotPositionsRef = useRef<Map<string, { x: number; y: number }> | null>(null);
  const ringIdsRef = useRef<Set<string>>(new Set());

  const allPoints = useMemo<Blip[]>(() => {
    const src = Array.isArray(points) ? points : [];
    const sorted = [...src].sort((a, b) => {
      const dayDiff = (Number(a.days_ago) || 0) - (Number(b.days_ago) || 0);
      if (dayDiff !== 0) return dayDiff;
      return (b.posted_at_ist || '').localeCompare(a.posted_at_ist || '');
    });

    return sorted.map((p, index) => {
      const percentile = typeof p.percentile_performance === 'number' ? p.percentile_performance : null;
      const daysAgo = Math.max(0, Number(p.days_ago) || 0);

      return {
        id: p.post_key || `${p.handle}-${p.days_ago}`,
        x: 50,
        y: percentile === null ? 48 : clamp(100 - percentile, 9, 91),
        isCompetitor: false,
        percentile,
        handle: p.handle || 'unknown',
        date: formatPointDate(p.posted_at_ist),
        views: Math.max(0, Number(p.views) || 0),
        likes: p.likes == null ? null : Math.max(0, Number(p.likes) || 0),
        comments: p.comments == null ? null : Math.max(0, Number(p.comments) || 0),
        mediaType: normalizePointMediaType(p.media_type),
        daysAgo,
        postIndex: index,
      };
    });
  }, [points]);

  const filteredPoints = useMemo(
    () => (
      deferredMediaFilter === 'all'
        ? allPoints
        : allPoints.filter((point) => point.mediaType === deferredMediaFilter)
    ),
    [allPoints, deferredMediaFilter],
  );

  const visiblePoints = useMemo(
    () => (
      filteredPoints
        .slice(0, Math.min(VISIBLE_POST_LIMIT, filteredPoints.length))
        .map((point, index, selectedPoints) => ({
          ...point,
          x: layoutPointX(index, selectedPoints.length, isCompactViewport),
        }))
    ),
    [filteredPoints, isCompactViewport],
  );

  const filterLabel = MEDIA_FILTERS.find((item) => item.key === mediaFilter)?.label ?? 'Posts';

  const activeHoveredPoint = useMemo(() => {
    if (!hoveredPoint) return null;
    return visiblePoints.some((point) => point.id === hoveredPoint.id) ? hoveredPoint : null;
  }, [hoveredPoint, visiblePoints]);
  const hoveredMetrics = useMemo(
    () => (activeHoveredPoint ? tooltipMetrics(activeHoveredPoint) : []),
    [activeHoveredPoint],
  );

  // Dots sit statically at left/bottom with their resting scale. Hovering re-renders only the tooltip, not these.
  const dots = useMemo(() => visiblePoints.map((blip, selectionIndex) => {
    const isTop = (blip.percentile ?? 100) <= TOP_ZONE_PERCENT;
    const dotSize = isCompactViewport
      ? (isTop ? 14 : 12)
      : (isTop ? 15.5 : 13.5);
    const restTransform = isTop ? 'scale(1.08)' : 'scale(1)';
    const restOpacity = isTop ? 1 : 0.92;
    const shouldPulse = selectionIndex < Math.min(3, visiblePoints.length);

    return (
      <button
        key={blip.id}
        type="button"
        data-dot={blip.id}
        data-x={blip.x}
        data-y={blip.y}
        data-rest={restTransform}
        data-rest-opacity={restOpacity}
        onClick={() => setHoveredPoint(blip)}
        onMouseEnter={() => setHoveredPoint(blip)}
        onMouseLeave={() => setHoveredPoint(null)}
        className="absolute -translate-x-1/2 -translate-y-1/2 transition-[scale] duration-500 hover:scale-110 hover:z-50 focus:outline-none"
        style={{
          left: `${blip.x}%`,
          bottom: `${blip.y}%`,
          zIndex: isTop ? 30 : 20,
          opacity: restOpacity,
          transform: restTransform,
        }}
      >
        <div
          className={[
            'rounded-full',
            isTop
              ? 'bg-[var(--fm-accent-bright)] shadow-[0_0_0_1px_rgba(255,255,255,0.2),0_0_18px_rgb(var(--fm-accent-rgb)/0.22)] dark:bg-[var(--fm-accent)] dark:shadow-[0_0_0_1px_rgba(255,255,255,0.22),0_0_22px_rgb(var(--fm-accent-rgb)/0.45)]'
              : 'bg-[var(--fm-accent-bright)]/88 shadow-[0_0_0_1px_rgba(255,255,255,0.16),0_0_14px_rgb(var(--fm-accent-rgb)/0.14)] dark:bg-[var(--fm-accent)]/88 dark:shadow-[0_0_0_1px_rgba(255,255,255,0.16),0_0_16px_rgb(var(--fm-accent-rgb)/0.3)]'
          ].join(' ')}
          style={{ height: dotSize, width: dotSize, opacity: restOpacity }}
        />
        {shouldPulse && (
          <div
            data-ring
            className="absolute inset-[-7px] rounded-full border border-[var(--fm-accent-bright)]/34 opacity-0 dark:border-[var(--fm-accent)]/42"
          />
        )}
      </button>
    );
  }), [isCompactViewport, visiblePoints]);

  // One WAAPI pass per change (compositor-only, nothing per frame and nothing looping): a new dot settles in on the
  // page beat, a dot that moved (filter or data change) glides from where it was — a transform FLIP, since left/bottom
  // stay static — and a ring that just joined the latest three breathes once after its dot lands.
  useLayoutEffect(() => {
    const plot = plotRef.current;
    if (!plot) return;
    const nodes = Array.from(plot.querySelectorAll<HTMLElement>('[data-dot]'));
    const previous = dotPositionsRef.current;
    const previousRings = ringIdsRef.current;
    const nextPositions = new Map<string, { x: number; y: number }>();
    const nextRings = new Set<string>();
    for (const node of nodes) {
      const id = node.dataset.dot ?? '';
      nextPositions.set(id, { x: Number(node.dataset.x), y: Number(node.dataset.y) });
      if (node.querySelector('[data-ring]')) nextRings.add(id);
    }
    dotPositionsRef.current = nextPositions;
    ringIdsRef.current = nextRings;
    if (reducedMotionPreferred()) return;

    const width = plot.clientWidth;
    const height = plot.clientHeight;
    nodes.forEach((node, index) => {
      const id = node.dataset.dot ?? '';
      const to = nextPositions.get(id);
      if (!to) return;
      const from = previous?.get(id);
      const rest = node.dataset.rest ?? 'scale(1)';
      // latest first, sweeping toward the oldest across the beat
      const delay = beatDelay(nodes.length > 1 ? (index / (nodes.length - 1)) * BEAT_MAX_STEPS : 0) * 1000;
      const timing = { duration: BEAT_ARRIVE_S * 1000, delay, easing: BEAT_EASE_CSS, fill: 'backwards' as const };

      if (!from) {
        node.animate(
          [
            { opacity: 0, transform: 'scale(0.6)' },
            { opacity: Number(node.dataset.restOpacity) || 1, transform: rest },
          ],
          { ...timing, id: DOT_ENTER_ID },
        );
      } else if (from.x !== to.x || from.y !== to.y) {
        const running = node.getAnimations();
        // a dot still arriving keeps its entrance and lands at the new spot instead of restarting
        if (!running.some((animation) => animation.id === DOT_ENTER_ID)) {
          for (const animation of running) {
            if (animation.id === DOT_MOVE_ID) animation.cancel();
          }
          const dx = ((from.x - to.x) / 100) * width;
          const dy = ((to.y - from.y) / 100) * height;
          node.animate(
            [{ transform: `translate(${dx}px, ${dy}px) ${rest}` }, { transform: `translate(0px, 0px) ${rest}` }],
            { ...timing, id: DOT_MOVE_ID },
          );
        }
      }

      // as its dot settles, keeping the old pulse's 80ms offset between the three rings
      const ring = previousRings.has(id) ? null : node.querySelector<HTMLElement>('[data-ring]');
      ring?.animate(
        [
          { opacity: 0, transform: 'scale(1)', easing: 'ease-in-out' },
          { opacity: 0.28, transform: 'scale(1.4)', easing: 'ease-in-out' },
          { opacity: 0, transform: 'scale(1)' },
        ],
        { duration: RING_BREATH_MS, delay: delay + BEAT_ARRIVE_S * 1000 * 0.6 + index * 80 },
      );
    });
  }, [visiblePoints]);

  return (
    <div
      className="fm-depth-glass relative flex h-full w-full flex-col overflow-hidden rounded-[22px] p-3 sm:p-3.5 lg:p-4"
    >
      <div className="relative z-10 mb-2.5 flex flex-col gap-2.5">
        <div className="flex items-start justify-between gap-3">
          <div>
            <span className="fm-depth-title text-[12px] font-black uppercase leading-none tracking-[0.14em] text-foreground/55 dark:text-white/46 sm:text-[12px]">
              Percentage Map
            </span>
            <div className="mt-1 flex items-baseline gap-1.5">
              <span className="text-[16px] font-black leading-none tracking-[-0.04em] text-foreground dark:text-white sm:text-[18px]">
                Latest {VISIBLE_POST_LIMIT}
              </span>
              <span className="text-[8px] font-black uppercase tracking-[0.14em] text-foreground/36 dark:text-white/30">
                {mediaFilter === 'all' ? 'posts' : filterLabel}
              </span>
            </div>
          </div>
          <div className="rounded-full border border-black/6 bg-white/60 px-2.5 py-1 text-right shadow-[0_5px_14px_rgba(15,23,42,0.04),inset_0_1px_0_rgba(255,255,255,0.8)] dark:border-white/8 dark:bg-white/[0.05]">
            <div className="text-[8px] font-black uppercase leading-none tracking-[0.14em] text-foreground/34 dark:text-white/28">
              showing
            </div>
            <div className="mt-0.5 text-[12px] font-black leading-none tabular-nums text-[var(--fm-accent)]">
              {visiblePoints.length}/{filteredPoints.length}
            </div>
          </div>
        </div>

        <div className="hide-scrollbar flex items-center gap-1 overflow-x-auto rounded-[14px] border border-black/6 bg-black/[0.025] p-[3px] shadow-[inset_0_1px_3px_rgba(15,23,42,0.035)] dark:border-white/8 dark:bg-white/[0.04]">
          {MEDIA_FILTERS.map(({ key, label, Icon }) => (
            <button
              key={key}
              type="button"
              title={label}
              aria-label={`Show ${label.toLowerCase()} in percentage map`}
              onClick={() => {
                startTransition(() => setMediaFilter(key));
              }}
              className={`relative inline-flex min-w-fit items-center gap-1.5 rounded-[14px] px-2.5 py-1.25 text-[10px] font-black uppercase tracking-[0.14em] transition-[scale] duration-150 ease-out active:scale-[0.96] sm:px-3 sm:py-1.5 ${mediaFilter === key ? 'z-10 text-white' : 'z-0 text-foreground/42 dark:text-white/40'}`}
            >
              {mediaFilter === key && (
                // A real shared-element move between options of different widths: the projection keeps the pill's
                // rounded ends true while it slides and resizes, which a scaleX slide can't.
                <motion.span
                  layoutId={`scatter-media-pill-bg-${instanceId}`}
                  className="absolute inset-0 rounded-[14px] bg-[var(--fm-accent)] shadow-[0_10px_20px_-10px_rgb(var(--fm-accent-rgb)/0.55)] dark:shadow-[0_10px_20px_-10px_rgb(var(--fm-accent-rgb)/0.45)]"
                  transition={{ type: 'spring', stiffness: 420, damping: 32, mass: 0.8 }}
                />
              )}
              <Icon className="relative z-10 h-3 w-3" strokeWidth={2.6} />
              <span className="relative z-10">{label}</span>
            </button>
          ))}
        </div>
      </div>

      {/* Chart area */}
      <div className="relative min-h-[208px] w-full flex-1 overflow-hidden rounded-[14px] fm-depth-inner sm:min-h-[228px]">
        <div
          className="absolute inset-x-0 border-t border-black/14 dark:border-white/12"
          style={{ top: `${TOP_ZONE_PERCENT}%` }}
        >
          <span className="absolute -top-3.5 right-1.5 rounded-full bg-white/72 px-1.5 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/38 shadow-[0_3px_10px_rgba(15,23,42,0.04)] dark:bg-black/35 dark:text-white/32">
            Top 50%
          </span>
        </div>

        <div ref={plotRef} className="absolute inset-x-4 inset-y-6 sm:inset-x-5 sm:inset-y-6 lg:inset-x-6">
          {visiblePoints.length === 0 && (
            <div className="absolute inset-0 flex items-center justify-center px-5 text-center text-[10px] font-black uppercase tracking-[0.14em] text-foreground/32 dark:text-white/28">
              No posts in this filter
            </div>
          )}
          {dots}
        </div>

        <div className="pointer-events-none absolute bottom-1.5 left-3 right-3 flex items-center justify-between text-[8px] font-black uppercase tracking-[0.14em] text-foreground/30 sm:bottom-2 sm:left-4 sm:right-4">
          <span>Older</span>
          <span>Latest</span>
        </div>

        {/* Tooltip — solid fill, no backdrop blur */}
        <AnimatePresence>
          {activeHoveredPoint && (
            <motion.div
              initial={TIP_HIDDEN}
              animate={TIP_SHOWN}
              exit={TIP_GONE}
              className="pointer-events-none absolute left-1/2 top-3 z-50 min-w-[168px] -translate-x-1/2 rounded-[10px] border border-black/10 bg-white/[0.96] px-3.5 py-2.5 shadow-[0_10px_26px_rgba(0,0,0,0.12)] dark:border-white/10 dark:bg-[var(--fm-ink)]/[0.97] dark:shadow-[0_12px_30px_rgba(0,0,0,0.72)]"
            >
              <div className="mb-2 flex items-center justify-between">
                <span className="text-[10px] font-black uppercase tracking-[0.14em] text-black/50 dark:text-white/40">{activeHoveredPoint.date}</span>
                <span className="text-[12px] font-black text-black/62 dark:text-white/72">{activeHoveredPoint.percentile === null ? '--' : `${Math.round(activeHoveredPoint.percentile)}%`}</span>
              </div>
              <div className="text-[16px] font-black tracking-normal text-black dark:text-white">@{activeHoveredPoint.handle}</div>
              <div className="mt-1 text-[10px] font-black uppercase tracking-[0.14em] text-black/42 dark:text-white/36">
                {mediaLabel(activeHoveredPoint.mediaType)}
              </div>
              <div className="mt-2 grid gap-1.5" style={{ gridTemplateColumns: `repeat(${hoveredMetrics.length}, minmax(0, 1fr))` }}>
                {hoveredMetrics.map(({ key, label, value, Icon }) => (
                  <div
                    key={key}
                    className="rounded-[10px] border border-black/[0.06] bg-black/[0.025] px-2 py-1.5 text-center dark:border-white/10 dark:bg-white/[0.05]"
                  >
                    <div className="flex items-center justify-center gap-1 text-[8px] font-black uppercase tracking-[0.14em] text-black/38 dark:text-white/34">
                      <Icon className="h-2.5 w-2.5" strokeWidth={2.5} />
                      {label}
                    </div>
                    <div className="mt-1 text-[12px] font-black leading-none tabular-nums text-black dark:text-white">
                      {formatCompactMetric(value)}
                    </div>
                  </div>
                ))}
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}

const MemoizedFeedScatterField = memo(FeedScatterField);
MemoizedFeedScatterField.displayName = 'FeedScatterField';

export default MemoizedFeedScatterField;
