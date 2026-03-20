'use client';

import { CSSProperties, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Loader2, SlidersHorizontal } from 'lucide-react';
import { AnimatePresence, motion } from 'framer-motion';
import ChronoTabs from '@/components/fire/ChronoTabs';
import FluidDeck from '@/components/fire/FluidDeck';
import FireIntelligenceDialog from '@/components/fire/FireIntelligenceDialog';
import ZSpaceFilter from '@/components/fire/ZSpaceFilter';
import {
  AlertUrgency,
  FireAlertItem,
  FireFeedOption,
  FireFilterState,
  FireFilterThreshold,
  FireLayers,
} from '@/components/fire/types';
import { useAppHaptics } from '@/lib/haptics';
import { cn } from '@/lib/utils';
import { getCache, setCache } from '@/lib/pageCache';

type AlertRow = Record<string, unknown>;

const TERMINAL_STATUSES = new Set(['dropped', 'error', 'archived']);
const APPLE_EASE = [0.32, 0.72, 0, 1] as const;
const FIRE_META_CACHE_KEY = 'fire:meta:v2';
const FIRE_STATE_CACHE_KEY = 'fire:state:v2';
const FIRE_PAGE_CACHE_PREFIX = 'fire:page:v2';
const FIRE_CACHE_TTL = 2 * 60 * 1000;
const CHECKPOINT_ORDER = ['D1', 'D3', 'D7', 'D21'];

function asRecord(v: unknown): Record<string, unknown> {
  return v && typeof v === 'object' && !Array.isArray(v) ? (v as Record<string, unknown>) : {};
}

function asString(v: unknown): string {
  if (typeof v === 'string') return v;
  if (typeof v === 'number') return String(v);
  return '';
}

function asNumber(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const n = Number(v.replace('%', '').trim());
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function pickBestMetric(metrics: Record<string, unknown>, preferred: string): string {
  const candidates = ['views', 'likes', 'comments'];
  const normalized = preferred.trim().toLowerCase();
  const order = normalized && candidates.includes(normalized)
    ? [normalized, ...candidates.filter((metric) => metric !== normalized)]
    : candidates;

  for (const metric of order) {
    const metricData = asRecord(metrics[metric]);
    if (asNumber(metricData.value) != null) return metric;
  }
  for (const metric of order) {
    const metricData = asRecord(metrics[metric]);
    if (asNumber(metricData.percentile) != null) return metric;
  }
  return order[0] ?? 'views';
}

function compactNumber(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return '?';
  if (Math.abs(v) >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
  if (Math.abs(v) >= 1e3) return `${(v / 1e3).toFixed(1)}K`;
  return String(Math.round(v));
}

function toMediaProxyUrl(url: string): string {
  const src = url.trim();
  return src ? `/api/media?url=${encodeURIComponent(src)}` : '';
}

function timeAgoText(iso: string): string {
  const ts = new Date(iso).getTime();
  if (Number.isNaN(ts)) return '';
  const h = Math.floor((Date.now() - ts) / 3600000);
  if (h < 1) return 'just now';
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

function percentileToTag(p: number | null): string {
  if (p == null) return '😴';
  if (p <= 5) return '🚀';
  if (p <= 20) return '🔥';
  if (p <= 35) return '✅';
  return '😴';
}

function normalizeMediaLabel(raw: string): string {
  const s = raw.trim().toLowerCase();
  if (!s) return 'POST';
  if (s.includes('reel')) return 'REEL';
  if (s.includes('sidecar') || s.includes('carousel')) return 'CAROUSEL';
  if (s.includes('image') || s.includes('photo')) return 'IMAGE';
  return s.toUpperCase();
}

function toIstDayKey(d: string | Date): string {
  const date = typeof d === 'string' ? new Date(d) : d;
  const formatter = (dt: Date) => new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Kolkata',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(dt);
  return Number.isNaN(date.getTime()) ? formatter(new Date()) : formatter(date);
}

function inferUrgency(alertType: string, p: number | null): AlertUrgency {
  const t = alertType.toLowerCase();
  if (t === 'blaze') return 'now';
  if (t === 'burn') return 'today';
  if (t === 'spark') return 'watch';
  if (p != null && p <= 10) return 'now';
  if (p != null && p <= 25) return 'today';
  return 'watch';
}

function formatDelta(n: number | null, cp: string): string | undefined {
  if (cp.toLowerCase() === 'd1' || n == null) return undefined;
  const x = Math.round(n);
  return x > 0 ? `+${x}` : x < 0 ? `${x}` : '0';
}

function sortNumberList(values: number[]): number[] {
  return [...values].sort((a, b) => a - b);
}

function sortCheckpointList(values: string[]): string[] {
  return [...values].sort((a, b) => {
    const ai = CHECKPOINT_ORDER.indexOf(a);
    const bi = CHECKPOINT_ORDER.indexOf(b);
    if (ai === -1 && bi === -1) return a.localeCompare(b);
    if (ai === -1) return 1;
    if (bi === -1) return -1;
    return ai - bi;
  });
}

function flattenSelectedFeederIds(filters: FireFilterState): number[] {
  return sortNumberList(
    Object.entries(filters.selectedFeederIdsByFeed)
      .filter(([feedId]) => filters.selectedFeedIds.includes(Number(feedId)))
      .flatMap(([, feederIds]) => feederIds),
  );
}

function serializeFilters(filters: FireFilterState): string {
  const normalizedFeederState = Object.entries(filters.selectedFeederIdsByFeed)
    .filter(([, feederIds]) => feederIds.length > 0)
    .sort(([a], [b]) => Number(a) - Number(b))
    .map(([feedId, feederIds]) => [feedId, sortNumberList(feederIds)]);

  return JSON.stringify({
    threshold: filters.threshold,
    selectedFeedIds: sortNumberList(filters.selectedFeedIds),
    selectedFeederIdsByFeed: normalizedFeederState,
    selectedCheckpoints: sortCheckpointList(filters.selectedCheckpoints),
  });
}

function pruneFilters(filters: FireFilterState, feeds: FireFeedOption[]): FireFilterState {
  const validFeedIds = new Set(feeds.map((feed) => feed.id));
  const validFeederIdsByFeed = new Map<number, Set<number>>();
  for (const feed of feeds) {
    validFeederIdsByFeed.set(feed.id, new Set(feed.feeders.map((feeder) => feeder.id)));
  }

  const selectedFeedIds = sortNumberList(filters.selectedFeedIds.filter((feedId) => validFeedIds.has(feedId)));
  const selectedFeedSet = new Set(selectedFeedIds);
  const selectedFeederIdsByFeed: Record<string, number[]> = {};

  for (const [feedId, feederIds] of Object.entries(filters.selectedFeederIdsByFeed)) {
    const numericFeedId = Number(feedId);
    if (!selectedFeedSet.has(numericFeedId)) continue;
    const validFeederIds = validFeederIdsByFeed.get(numericFeedId);
    if (!validFeederIds) continue;
    const nextFeederIds = sortNumberList(feederIds.filter((feederId) => validFeederIds.has(feederId)));
    if (nextFeederIds.length > 0) selectedFeederIdsByFeed[feedId] = nextFeederIds;
  }

  return {
    threshold: filters.threshold,
    selectedFeedIds,
    selectedFeederIdsByFeed,
    selectedCheckpoints: sortCheckpointList(filters.selectedCheckpoints),
  };
}

function buildFeedSummaryLabel(filters: FireFilterState): string {
  const feederCount = Object.values(filters.selectedFeederIdsByFeed).reduce((sum, feederIds) => sum + feederIds.length, 0);
  if (filters.selectedFeedIds.length === 0) return 'ALL FEEDS';
  if (feederCount === 0) return `${filters.selectedFeedIds.length} FEEDS`;
  return `${filters.selectedFeedIds.length} FEEDS · ${feederCount} FEEDERS`;
}

function buildDesktopSelectionChips(filters: FireFilterState, feeds: FireFeedOption[]): { key: string; label: string }[] {
  const feedMap = new Map(feeds.map((feed) => [feed.id, feed]));
  const chips: { key: string; label: string }[] = [];

  for (const feedId of sortNumberList(filters.selectedFeedIds)) {
    const feed = feedMap.get(feedId);
    if (!feed) continue;
    const selectedFeederIds = sortNumberList(filters.selectedFeederIdsByFeed[String(feedId)] || []);
    if (selectedFeederIds.length === 0) {
      chips.push({ key: `feed-${feedId}`, label: feed.name });
      continue;
    }

    const feederMap = new Map(feed.feeders.map((feeder) => [feeder.id, feeder]));
    for (const feederId of selectedFeederIds) {
      const feeder = feederMap.get(feederId);
      if (!feeder) continue;
      chips.push({ key: `feeder-${feedId}-${feederId}`, label: `@${feeder.handle.toUpperCase()}` });
    }
  }

  return chips;
}

function normalizeAlertRow(row: AlertRow): FireAlertItem | null {
  const payload = asRecord(row.payload);
  const explicit = asRecord(row.intelligence_layers);
  const layers = Object.keys(explicit).length > 0 ? explicit : asRecord(payload.layers);
  const status = asString(row.status).trim().toLowerCase();
  if (status && TERMINAL_STATUSES.has(status)) return null;

  const checkpoint = asString(row.checkpoint) || asString(row.surface_checkpoint) || asString(asRecord(payload.meta).checkpoint) || 'd1';
  const surfaceHandle = asString(row.surface_handle) || asString(row.surface_feeder) || asString(asRecord(payload.meta).handle);
  const surfaceMediaType = normalizeMediaLabel(
    asString(row.surface_media_type) || asString(row.surface_mediaType) || asString(asRecord(payload.meta).media_type),
  );
  const metrics = asRecord(payload.metrics);
  const bestMetric = pickBestMetric(metrics, asString(payload.best_metric) || asString(row.metric_key) || 'views');
  const bestMetricData = asRecord(metrics[bestMetric]);
  const position = asRecord(payload.position);
  const surfacePercentile =
    asNumber(position.overall_percentile) ??
    asNumber(position.percentile) ??
    asNumber(row.surface_percentile) ??
    asNumber(bestMetricData.percentile);
  const metricValue = asNumber(bestMetricData.value) ?? asNumber(row.surface_metric_value) ?? asNumber(row.metric_value);
  const surfaceDelta = asNumber(row.surface_delta) ?? asNumber(row.surface_shift);
  const meta = asRecord(payload.meta);
  const businessDateKey =
    asString(row.captured_business_date_ist) ||
    asString(row.business_date_ist) ||
    asString(meta.captured_business_date_ist) ||
    asString(meta.business_date_ist) ||
    asString(meta.anchor_business_date_ist) ||
    toIstDayKey(asString(row.created_at));
  const title = `@${surfaceHandle ? surfaceHandle.toUpperCase() : 'FEEDER'} · ${surfaceMediaType} · ${checkpoint.toUpperCase()} · ${compactNumber(metricValue)} ${bestMetric.toUpperCase()}`;

  return {
    id: `alert-${asString(row.id) || asString(row.dedupe_key) || Math.random().toString(36).slice(2)}`,
    postKey: asString(row.post_key),
    family: 'insight',
    urgency: inferUrgency(asString(row.alert_type), surfacePercentile),
    color: '#FF6B00',
    handle: `@${surfaceHandle || 'feed'}`,
    title,
    whyNow: asString(row.body) || '',
    action: '',
    percentileTag: percentileToTag(surfacePercentile),
    mediaType: surfaceMediaType,
    stage: checkpoint.toUpperCase(),
    percentile: surfacePercentile == null ? undefined : String(Math.round(surfacePercentile)),
    delta: formatDelta(surfaceDelta, checkpoint),
    evidence: [],
    timeAgo: timeAgoText(asString(row.created_at)),
    createdAt: asString(row.created_at),
    postUrl: asString(meta.post_url ?? payload.post_url) || 'https://instagram.com',
    thumbnailUrl: toMediaProxyUrl(asString(meta.thumbnail_url ?? payload.thumbnail_url)),
    businessDateKey,
    businessDateIst: businessDateKey,
    status,
    surfacePercentile,
    surfaceDelta,
    trajectoryDeltaPercentile: surfaceDelta,
    surfaceHandle,
    surfaceMediaType,
    checkpoint: checkpoint.toUpperCase(),
    metricValue,
    metricKey: bestMetric,
    payload,
    layers: layers as FireLayers,
    stamp: {
      handle: surfaceHandle,
      mediaType: surfaceMediaType,
      checkpoint: checkpoint.toUpperCase(),
      metricLabel: bestMetric.toUpperCase(),
      metricValue,
    },
  };
}

async function fetchMeta(): Promise<{ days: string[]; feeds: FireFeedOption[] }> {
  const res = await fetch('/api/fire?mode=meta');
  if (!res.ok) throw new Error(`Meta fetch failed: ${res.status}`);
  const data = await res.json();
  return { days: data.days ?? [], feeds: data.feeds ?? [] };
}

async function fetchPage(
  day: string,
  filters: FireFilterState,
  cursor: number,
): Promise<{ items: FireAlertItem[]; hasMore: boolean; total: number; availableCheckpoints: string[] }> {
  const params = new URLSearchParams({ day, threshold: filters.threshold, cursor: String(cursor) });
  const feedIds = sortNumberList(filters.selectedFeedIds);
  const feederIds = flattenSelectedFeederIds(filters);

  if (feedIds.length > 0) params.set('feed_ids', feedIds.join(','));
  if (feederIds.length > 0) params.set('feeder_ids', feederIds.join(','));
  if (filters.selectedCheckpoints.length > 0) params.set('checkpoints', sortCheckpointList(filters.selectedCheckpoints).join(','));

  const res = await fetch(`/api/fire?${params}`);
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }

  const { rows, hasMore, total, availableCheckpoints } = await res.json();
  const items = (rows as AlertRow[]).map(normalizeAlertRow).filter((row): row is FireAlertItem => row !== null);
  return {
    items,
    hasMore: hasMore ?? false,
    total: total ?? 0,
    availableCheckpoints: sortCheckpointList((availableCheckpoints ?? []).map((value: string) => String(value).toUpperCase())),
  };
}

export default function FirePage() {
  const { play } = useAppHaptics();
  const headerRef = useRef<HTMLDivElement>(null);
  const cursorRef = useRef(0);
  const fetchKeyRef = useRef('');

  const [pickerDays, setPickerDays] = useState<string[]>([]);
  const [availableFeeds, setAvailableFeeds] = useState<FireFeedOption[]>([]);
  const [availableCheckpoints, setAvailableCheckpoints] = useState<string[]>([]);
  const [cards, setCards] = useState<FireAlertItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [total, setTotal] = useState(0);
  const [selectedDay, setSelectedDay] = useState('');
  const [isZSpaceOpen, setIsZSpaceOpen] = useState(false);
  const [filters, setFilters] = useState<FireFilterState>({
    threshold: 'ALL',
    selectedFeedIds: [],
    selectedFeederIdsByFeed: {},
    selectedCheckpoints: [],
  });
  const [headerHeight, setHeaderHeight] = useState(168);
  const [desktopModalCard, setDesktopModalCard] = useState<FireAlertItem | null>(null);

  const hasActiveFilters = filters.threshold !== 'ALL'
    || filters.selectedFeedIds.length > 0
    || Object.keys(filters.selectedFeederIdsByFeed).length > 0
    || filters.selectedCheckpoints.length > 0;

  const feedSummaryLabel = useMemo(() => buildFeedSummaryLabel(filters), [filters]);
  const desktopSelectionChips = useMemo(() => buildDesktopSelectionChips(filters, availableFeeds), [filters, availableFeeds]);

  useEffect(() => {
    const cachedState = getCache<{
      pickerDays: string[];
      availableFeeds: FireFeedOption[];
      selectedDay: string;
      filters: FireFilterState;
      cards: FireAlertItem[];
      availableCheckpoints: string[];
      hasMore: boolean;
      total: number;
      cursor: number;
    }>(FIRE_STATE_CACHE_KEY, FIRE_CACHE_TTL);

    if (!cachedState) return;

    setPickerDays(cachedState.pickerDays || []);
    setAvailableFeeds(cachedState.availableFeeds || []);
    setSelectedDay(cachedState.selectedDay || '');
    setFilters(pruneFilters(cachedState.filters || {
      threshold: 'ALL',
      selectedFeedIds: [],
      selectedFeederIdsByFeed: {},
      selectedCheckpoints: [],
    }, cachedState.availableFeeds || []));
    setCards(cachedState.cards || []);
    setAvailableCheckpoints(sortCheckpointList(cachedState.availableCheckpoints || []));
    setHasMore(Boolean(cachedState.hasMore));
    setTotal(cachedState.total || 0);
    cursorRef.current = cachedState.cursor || (cachedState.cards || []).length;
    setLoading(false);
  }, []);

  useEffect(() => {
    const node = headerRef.current;
    if (!node) return;

    const updateHeight = () => setHeaderHeight(Math.ceil(node.getBoundingClientRect().height));
    updateHeight();

    const observer = typeof ResizeObserver === 'undefined' ? null : new ResizeObserver(updateHeight);
    observer?.observe(node);
    window.addEventListener('resize', updateHeight);
    return () => {
      observer?.disconnect();
      window.removeEventListener('resize', updateHeight);
    };
  }, [pickerDays, availableCheckpoints.length, filters.selectedFeedIds.length, filters.selectedCheckpoints.length]);

  useEffect(() => {
    if (!selectedDay) return;
    setCache(FIRE_STATE_CACHE_KEY, {
      pickerDays,
      availableFeeds,
      selectedDay,
      filters,
      cards,
      availableCheckpoints,
      hasMore,
      total,
      cursor: cursorRef.current,
    });
  }, [pickerDays, availableFeeds, selectedDay, filters, cards, availableCheckpoints, hasMore, total]);

  useEffect(() => {
    document.title = 'Fire';
  }, []);

  useEffect(() => {
    const updatePwaPad = () => {
      const isStandalone = window.matchMedia('(display-mode: standalone)').matches
        || Boolean((window.navigator as Navigator & { standalone?: boolean }).standalone);
      const vv = window.visualViewport;
      const offset = Math.max(0, Math.round(vv?.offsetTop ?? 0));
      const pad = offset + 8;
      document.documentElement.style.setProperty('--pwa-top-pad', isStandalone ? `${pad}px` : '0px');
      document.documentElement.style.setProperty('--pwa-top-fix', isStandalone ? `${-(offset + 6)}px` : '0px');
      document.documentElement.style.setProperty('--pwa-bottom-pad', isStandalone ? '-24px' : '0px');
    };
    updatePwaPad();
    const mql = window.matchMedia('(display-mode: standalone)');
    const handler = () => updatePwaPad();
    mql.addEventListener?.('change', handler);
    window.addEventListener('resize', handler);
    return () => {
      mql.removeEventListener?.('change', handler);
      window.removeEventListener('resize', handler);
    };
  }, []);

  const refreshMeta = useCallback(async () => {
    const cached = getCache<{ days: string[]; feeds: FireFeedOption[] }>(FIRE_META_CACHE_KEY, FIRE_CACHE_TTL);
    if (cached) {
      setAvailableFeeds(cached.feeds ?? []);
      if ((cached.days ?? []).length > 0) {
        setPickerDays((prevDays) => {
          const prevTop = prevDays[0] || '';
          setSelectedDay((current) => {
            if (!current) return cached.days[0];
            if (!cached.days.includes(current)) return cached.days[0];
            if (current === prevTop) return cached.days[0];
            return current;
          });
          return cached.days;
        });
      }
      return;
    }

    const meta = await fetchMeta();
    setCache(FIRE_META_CACHE_KEY, meta);
    setAvailableFeeds(meta.feeds);
    if (meta.days.length > 0) {
      setPickerDays((prevDays) => {
        const prevTop = prevDays[0] || '';
        setSelectedDay((current) => {
          if (!current) return meta.days[0];
          if (!meta.days.includes(current)) return meta.days[0];
          if (current === prevTop) return meta.days[0];
          return current;
        });
        return meta.days;
      });
    }
  }, []);

  useEffect(() => {
    let mounted = true;
    const run = async () => {
      try {
        await refreshMeta();
      } catch (err) {
        if (!mounted) return;
        console.error('[FirePage] Meta fetch error:', err);
        setError(err instanceof Error ? err.message : 'Failed to load');
        setLoading(false);
      }
    };

    run();

    const intervalId = window.setInterval(run, 60_000);
    const onVisibility = () => {
      if (document.visibilityState === 'visible') run();
    };
    window.addEventListener('focus', run);
    document.addEventListener('visibilitychange', onVisibility);

    return () => {
      mounted = false;
      window.clearInterval(intervalId);
      window.removeEventListener('focus', run);
      document.removeEventListener('visibilitychange', onVisibility);
    };
  }, [refreshMeta]);

  useEffect(() => {
    setFilters((current) => pruneFilters(current, availableFeeds));
  }, [availableFeeds]);

  useEffect(() => {
    setFilters((current) => ({
      ...current,
      selectedCheckpoints: current.selectedCheckpoints.filter((checkpoint) => availableCheckpoints.includes(checkpoint)),
    }));
  }, [availableCheckpoints]);

  useEffect(() => {
    setDesktopModalCard((current) => {
      if (!current) return null;
      return cards.find((card) => card.id === current.id) || null;
    });
  }, [cards]);

  useEffect(() => {
    if (!selectedDay) return;

    let mounted = true;
    const normalizedFilters = pruneFilters(filters, availableFeeds);
    const key = `${selectedDay}:${serializeFilters(normalizedFilters)}`;
    const cacheKey = `${FIRE_PAGE_CACHE_PREFIX}:${key}`;
    fetchKeyRef.current = key;

    const cached = getCache<{
      items: FireAlertItem[];
      hasMore: boolean;
      total: number;
      cursor: number;
      availableCheckpoints: string[];
    }>(cacheKey, FIRE_CACHE_TTL);

    if (cached) {
      setCards(cached.items);
      setHasMore(cached.hasMore);
      setTotal(cached.total);
      setAvailableCheckpoints(sortCheckpointList(cached.availableCheckpoints || []));
      cursorRef.current = cached.cursor;
      setLoading(false);
      setError(null);
      return () => { mounted = false; };
    }

    cursorRef.current = 0;
    setLoading(true);
    setCards([]);
    setHasMore(false);
    setError(null);

    (async () => {
      try {
        const result = await fetchPage(selectedDay, normalizedFilters, 0);
        if (!mounted || fetchKeyRef.current !== key) return;
        setCards(result.items);
        setHasMore(result.hasMore);
        setTotal(result.total);
        setAvailableCheckpoints(result.availableCheckpoints);
        cursorRef.current = result.items.length;
        setCache(cacheKey, {
          items: result.items,
          hasMore: result.hasMore,
          total: result.total,
          cursor: result.items.length,
          availableCheckpoints: result.availableCheckpoints,
        });
      } catch (err) {
        if (!mounted || fetchKeyRef.current !== key) return;
        setError(err instanceof Error ? err.message : 'Failed to load alerts');
      } finally {
        if (mounted && fetchKeyRef.current === key) setLoading(false);
      }
    })();

    return () => { mounted = false; };
  }, [selectedDay, filters, availableFeeds]);

  const handleLoadMore = useCallback(async () => {
    if (loadingMore || !hasMore) return;
    setLoadingMore(true);
    const key = fetchKeyRef.current;
    try {
      const result = await fetchPage(selectedDay, filters, cursorRef.current);
      if (fetchKeyRef.current !== key) return;
      setCards((prev) => {
        const nextCards = [...prev, ...result.items];
        setCache(`${FIRE_PAGE_CACHE_PREFIX}:${key}`, {
          items: nextCards,
          hasMore: result.hasMore,
          total: result.total,
          cursor: nextCards.length,
          availableCheckpoints: result.availableCheckpoints,
        });
        return nextCards;
      });
      setHasMore(result.hasMore);
      setTotal(result.total);
      setAvailableCheckpoints(result.availableCheckpoints);
      cursorRef.current += result.items.length;
    } catch (err) {
      console.error('[FirePage] Load more error:', err);
    } finally {
      setLoadingMore(false);
    }
  }, [filters, hasMore, loadingMore, selectedDay]);

  const rootStyle = {
    '--fire-header-height': `${headerHeight}px`,
    '--fire-bottom-clearance': '208px',
  } as CSSProperties;

  return (
    <motion.div
      initial={{ opacity: 0, y: 14, scale: 0.992 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      transition={{ duration: 0.42, ease: APPLE_EASE }}
      className="relative h-[100svh] w-full overflow-hidden bg-background text-foreground select-none md:h-[100dvh]"
      style={rootStyle}
    >
      <div className="pointer-events-none fixed inset-0 z-0 bg-white dark:bg-[#030303]" />

      <ZSpaceFilter
        isOpen={isZSpaceOpen}
        onClose={() => setIsZSpaceOpen(false)}
        filters={filters}
        availableFeeds={availableFeeds}
        availableCheckpoints={availableCheckpoints}
        onChange={setFilters}
      />

      <div className="h-full w-full">
        <div
          ref={headerRef}
          className="pointer-events-auto absolute inset-x-0 top-0 z-[100] flex flex-col items-center px-2 pt-[calc(10px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] sm:px-4 sm:pt-[calc(14px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] md:pt-[calc(20px+var(--pwa-top-fix,0px))] lg:px-4"
        >
          <div className="relative fm-tab-header-shell">
            <div className={cn(
              'relative w-full overflow-hidden rounded-[28px] border border-white/80 border-t-white/90 bg-white/65 backdrop-blur-[40px] backdrop-saturate-[180%]',
              'shadow-[0_1px_0_rgba(255,255,255,0.95)_inset,0_-1px_0_rgba(0,0,0,0.03)_inset,0_4px_8px_rgba(0,0,0,0.03),0_12px_28px_-4px_rgba(0,0,0,0.08),0_32px_64px_-12px_rgba(0,0,0,0.1)]',
              'dark:border-white/[0.07] dark:border-t-white/[0.12] dark:bg-[rgba(6,6,6,0.68)]',
              'dark:shadow-[0_1px_0_rgba(255,255,255,0.06)_inset,0_-1px_0_rgba(0,0,0,0.5)_inset,0_8px_16px_rgba(0,0,0,0.4),0_24px_48px_-8px_rgba(0,0,0,0.6)]',
            )}>
              <div
                className="pointer-events-none absolute inset-0 z-0 rounded-[28px] transition-opacity dark:opacity-0"
                style={{ background: 'linear-gradient(180deg, rgba(255,255,255,0.45) 0%, rgba(255,255,255,0) 30%, rgba(0,0,0,0.015) 100%)' }}
              />
              <div
                className="pointer-events-none absolute inset-[1px] z-0 rounded-[27px] dark:hidden"
                style={{ boxShadow: 'inset 0 2px 4px rgba(255,255,255,0.7), inset 0 -2px 6px rgba(0,0,0,0.04)' }}
              />

              <div className="relative z-10 px-3.5 py-3 sm:px-4 sm:py-3.5 lg:px-5 lg:py-2.5">
                <div className="flex flex-col gap-2.5 lg:hidden">
                  <div className="flex items-center justify-between gap-3">
                    <h1 className="shrink-0 text-[30px] font-black leading-none tracking-[0.14em] text-black sm:text-[38px] dark:text-white fm-depth-title">
                      FIRE
                    </h1>
                    <motion.button
                      whileTap={{ scale: 0.92 }}
                      onClick={() => {
                        play('snapLock');
                        setIsZSpaceOpen(true);
                      }}
                      className={cn(
                        'relative flex shrink-0 items-center justify-center rounded-[14px] border border-white/70 bg-white/60 p-2 shadow-[0_2px_6px_rgba(0,0,0,0.08),0_1px_0_rgba(255,255,255,0.9)_inset,0_-1px_0_rgba(0,0,0,0.04)_inset]',
                        'dark:border-white/10 dark:bg-white/[0.06] dark:shadow-[0_2px_8px_rgba(0,0,0,0.4),0_1px_0_rgba(255,255,255,0.06)_inset]',
                        hasActiveFilters ? 'text-lime dark:text-lime' : 'text-black/58 dark:text-white/45',
                      )}
                    >
                      <SlidersHorizontal size={20} />
                    </motion.button>
                  </div>
                  <div className="min-w-0 pointer-events-auto">
                    <ChronoTabs days={pickerDays} activeDay={selectedDay} onChange={setSelectedDay} />
                  </div>
                </div>

                <div className="hidden flex-col gap-2 lg:flex">
                  <div className="grid grid-cols-[auto_minmax(0,1fr)_auto] items-center gap-3">
                    <div className="text-[22px] font-black uppercase tracking-[0.14em] text-black dark:text-white fm-depth-title">
                      FIRE
                    </div>
                    <div className="flex justify-center">
                      <ChronoTabs days={pickerDays} activeDay={selectedDay} onChange={setSelectedDay} compact />
                    </div>
                    <div className="flex items-center justify-end">
                      <div className="min-w-[150px] rounded-[18px] border border-black/6 bg-white/68 px-3.5 py-2 text-right shadow-[0_10px_22px_rgba(0,0,0,0.08),inset_0_1px_0_rgba(255,255,255,0.8)] dark:border-white/10 dark:bg-white/[0.07] dark:shadow-[0_12px_24px_rgba(0,0,0,0.34),inset_0_1px_0_rgba(255,255,255,0.06)]">
                        <div className="text-[8px] font-black uppercase tracking-[0.22em] text-black/38 dark:text-white/32">
                          Signals
                        </div>
                        <div className="mt-0.5 flex items-end justify-end gap-1.5">
                          <span className="text-[28px] font-black leading-none tracking-[-0.06em] text-black dark:text-white">
                            {total}
                          </span>
                          <span className="mb-0.5 rounded-full bg-[#CCFF00]/22 px-1.5 py-0.5 text-[7px] font-black uppercase tracking-[0.18em] text-black/70 dark:bg-[#CCFF00]/16 dark:text-[#CCFF00]">
                            {filters.threshold === 'ALL' ? 'ALL' : `TOP ${filters.threshold}`}
                          </span>
                        </div>
                        <div className="mt-0.5 text-[8px] font-black uppercase tracking-[0.16em] text-black/44 dark:text-white/38">
                          {selectedDay || '--'}
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="flex flex-wrap items-center gap-2 overflow-hidden rounded-[18px] border border-black/5 bg-black/[0.035] px-2.5 py-2 shadow-[inset_0_2px_8px_rgba(0,0,0,0.04)] dark:border-white/8 dark:bg-white/[0.03] dark:shadow-[inset_0_2px_8px_rgba(0,0,0,0.3)]">
                    <div className="flex items-center gap-1 rounded-[14px] border border-black/5 bg-white/58 p-1 shadow-[0_4px_12px_rgba(0,0,0,0.06),inset_0_1px_0_rgba(255,255,255,0.75)] dark:border-white/8 dark:bg-white/[0.05] dark:shadow-[0_8px_18px_rgba(0,0,0,0.35),inset_0_1px_0_rgba(255,255,255,0.06)]">
                      {(['10', '25', '50', 'ALL'] as FireFilterThreshold[]).map((threshold) => {
                        const isActive = filters.threshold === threshold;
                        return (
                          <motion.button
                            key={threshold}
                            type="button"
                            whileTap={{ scale: 0.95 }}
                            onClick={() => setFilters((current) => ({ ...current, threshold }))}
                            className={cn(
                              'rounded-[11px] px-2.5 py-1.5 text-[9px] font-black uppercase tracking-[0.16em] transition-colors duration-200',
                              isActive
                                ? 'bg-[#CCFF00] text-black shadow-[0_6px_14px_rgba(204,255,0,0.22),inset_0_1px_0_rgba(255,255,255,0.75)]'
                                : 'text-black/55 dark:text-white/45',
                            )}
                          >
                            {threshold === 'ALL' ? 'ALL SIGNALS' : `TOP ${threshold}`}
                          </motion.button>
                        );
                      })}
                    </div>

                    <motion.button
                      type="button"
                      whileTap={{ scale: 0.96 }}
                      onClick={() => setIsZSpaceOpen(true)}
                      className="rounded-[14px] border border-black/6 bg-white/58 px-3.5 py-1.5 text-[9px] font-black uppercase tracking-[0.16em] text-black shadow-[0_4px_12px_rgba(0,0,0,0.06),inset_0_1px_0_rgba(255,255,255,0.75)] dark:border-white/8 dark:bg-white/[0.05] dark:text-white dark:shadow-[0_8px_18px_rgba(0,0,0,0.35),inset_0_1px_0_rgba(255,255,255,0.06)]"
                    >
                      {feedSummaryLabel}
                    </motion.button>

                    {desktopSelectionChips.map((chip) => (
                      <div
                        key={chip.key}
                        className="rounded-[12px] border border-black/6 bg-white/60 px-2.5 py-1.5 text-[9px] font-black uppercase tracking-[0.14em] text-black shadow-[0_4px_10px_rgba(0,0,0,0.05),inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/8 dark:bg-white/[0.05] dark:text-white/78 dark:shadow-[0_8px_18px_rgba(0,0,0,0.32),inset_0_1px_0_rgba(255,255,255,0.06)]"
                      >
                        {chip.label}
                      </div>
                    ))}

                    <div className="ml-auto flex flex-wrap items-center gap-2">
                      <motion.button
                        type="button"
                        whileTap={{ scale: 0.95 }}
                        onClick={() => setFilters((current) => ({ ...current, selectedCheckpoints: [] }))}
                        className={cn(
                          'rounded-[12px] px-2.5 py-1.5 text-[9px] font-black uppercase tracking-[0.16em] transition-colors duration-200',
                          filters.selectedCheckpoints.length === 0
                            ? 'bg-black text-white shadow-[0_6px_18px_rgba(0,0,0,0.18)] dark:bg-[#CCFF00] dark:text-black dark:shadow-[0_8px_20px_rgba(204,255,0,0.22)]'
                            : 'border border-black/6 bg-white/58 text-black/56 shadow-[0_4px_12px_rgba(0,0,0,0.06),inset_0_1px_0_rgba(255,255,255,0.75)] dark:border-white/8 dark:bg-white/[0.05] dark:text-white/46 dark:shadow-[0_8px_18px_rgba(0,0,0,0.35),inset_0_1px_0_rgba(255,255,255,0.06)]',
                        )}
                      >
                        ALL
                      </motion.button>
                      {availableCheckpoints.map((checkpoint) => {
                        const isSelected = filters.selectedCheckpoints.includes(checkpoint);
                        return (
                          <motion.button
                            key={checkpoint}
                            type="button"
                            whileTap={{ scale: 0.95 }}
                            onClick={() => {
                              setFilters((current) => ({
                                ...current,
                                selectedCheckpoints: current.selectedCheckpoints.includes(checkpoint)
                                  ? current.selectedCheckpoints.filter((value) => value !== checkpoint)
                                  : [...current.selectedCheckpoints, checkpoint],
                              }));
                            }}
                            className={cn(
                              'rounded-[12px] px-2.5 py-1.5 text-[9px] font-black uppercase tracking-[0.16em] transition-colors duration-200',
                              isSelected
                                ? 'bg-black text-white shadow-[0_6px_18px_rgba(0,0,0,0.18)] dark:bg-[#CCFF00] dark:text-black dark:shadow-[0_8px_20px_rgba(204,255,0,0.22)]'
                                : 'border border-black/6 bg-white/58 text-black/56 shadow-[0_4px_12px_rgba(0,0,0,0.06),inset_0_1px_0_rgba(255,255,255,0.75)] dark:border-white/8 dark:bg-white/[0.05] dark:text-white/46 dark:shadow-[0_8px_18px_rgba(0,0,0,0.35),inset_0_1px_0_rgba(255,255,255,0.06)]',
                            )}
                          >
                            {checkpoint}
                          </motion.button>
                        );
                      })}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className="h-full w-full">
          {loading ? (
            <div className="flex h-full w-full items-center justify-center">
              <Loader2 className="h-12 w-12 animate-spin text-lime" />
            </div>
          ) : error ? (
            <div className="flex h-full w-full items-center justify-center px-6 text-center">
              <div className="rounded-2xl border border-red-400/40 bg-red-500/10 px-6 py-5 text-sm font-semibold tracking-wide text-red-600 dark:text-red-300">
                FIRE DATA UNAVAILABLE: {error}
              </div>
            </div>
          ) : (
            <div className="mx-auto h-full w-full">
              <AnimatePresence mode="popLayout" initial={false}>
                <motion.div
                  key={`${selectedDay}-${serializeFilters(filters)}`}
                  initial={{ opacity: 0, y: 10, scale: 0.996 }}
                  animate={{ opacity: 1, y: 0, scale: 1 }}
                  exit={{ opacity: 0, y: -8, scale: 0.996 }}
                  transition={{ duration: 0.3, ease: APPLE_EASE }}
                  className="h-full"
                >
                  {cards.length === 0 ? (
                    <div className="flex h-full w-full items-center justify-center px-6 text-center">
                      <div className="rounded-2xl border border-white/30 bg-white/20 px-6 py-4 text-xs font-black uppercase tracking-[0.2em] text-foreground/70 dark:border-white/14 dark:bg-black/24">
                        No alerts for this selection
                      </div>
                    </div>
                  ) : (
                    <FluidDeck
                      cards={cards}
                      hasMore={hasMore}
                      loadingMore={loadingMore}
                      onLoadMore={handleLoadMore}
                      onOpenCard={setDesktopModalCard}
                    />
                  )}
                </motion.div>
              </AnimatePresence>
            </div>
          )}
        </div>
      </div>

      <FireIntelligenceDialog item={desktopModalCard} onClose={() => setDesktopModalCard(null)} />

      {process.env.NODE_ENV !== 'production' && (
        <div className="pointer-events-none absolute bottom-3 left-3 z-[140] rounded-lg border border-white/30 bg-black/55 px-2.5 py-2 text-[10px] font-mono leading-tight text-lime/95 dark:border-white/20">
          <div>cards: {cards.length} / {total}</div>
          <div>day: {selectedDay || '--'}</div>
          <div>filters: {serializeFilters(filters)}</div>
        </div>
      )}
    </motion.div>
  );
}
