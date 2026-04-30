'use client';

import { CSSProperties, startTransition, type SetStateAction, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ArrowUpDown, Loader2, SlidersHorizontal } from 'lucide-react';
import { motion } from 'framer-motion';
import ChronoTabs from '@/components/fire/ChronoTabs';
import FluidDeck from '@/components/fire/FluidDeck';
import FireIntelligenceDialog from '@/components/fire/FireIntelligenceDialog';
import ZSpaceFilter from '@/components/fire/ZSpaceFilter';
import {
  metricMultipleFromPayload,
  metricValueFromPayload,
  resolveBestMetricFromPayload,
} from '@/components/fire/fireMetricDisplay';
import {
  AlertUrgency,
  FireAlertItem,
  FireFeedOption,
  FireMediaFilter,
  FireFilterState,
  FireSortMode,
  FireLayers,
  FireWarmupGate,
  WarmupMediaBucket,
} from '@/components/fire/types';
import { useAppHaptics } from '@/lib/haptics';
import { HEADER_STAGGER_CONTAINER, HEADER_ROW } from '@/lib/motion';
import { getFireSignalMeta } from '@/lib/fireSignals';
import { useCompressedOnScroll } from '@/lib/useCompressedOnScroll';
import { cn } from '@/lib/utils';
import { getCache, readCache, setCache } from '@/lib/pageCache';
import { getVisualViewportEventTarget } from '@/lib/visualViewport';
import { acquireDocumentClass, acquireRootPageScroll } from '@/lib/rootScrollMode';

type AlertRow = Record<string, unknown>;
type WarmupSummary = Record<string, number>;

const TERMINAL_STATUSES = new Set(['dropped', 'error', 'archived']);
const FIRE_SORT_OPTIONS: { label: string; value: FireSortMode }[] = [
  { label: 'PERCENTILE', value: 'best' },
  { label: 'RECENT', value: 'recent' },
];
const APPLE_EASE = [0.32, 0.72, 0, 1] as const;
const FIRE_CHROME_PILL_SPRING = { type: 'spring', stiffness: 420, damping: 32, mass: 0.82 } as const;
const FIRE_META_CACHE_KEY = 'fire:meta:v5';
const FIRE_STATE_CACHE_KEY = 'fire:state:v9';
const FIRE_PAGE_CACHE_PREFIX = 'fire:page:v11';
const FIRE_CACHE_TTL = 10 * 60 * 1000;
const FIRE_LIVE_CACHE_TTL = 15 * 1000;
const FIRE_META_REFRESH_INTERVAL_MS = 5 * 60 * 1000;
const CHECKPOINT_ORDER = ['D1', 'D3', 'D7', 'D21'];
const WARMUP_REQUIRED = 3;
const FIRE_INITIAL_BATCH_SIZE = 20;
const FIRE_BACKGROUND_PREFETCH_DAY_COUNT_DESKTOP = 2;
const FIRE_BACKGROUND_PREFETCH_DAY_COUNT_MOBILE = 0;
const FIRE_BOOTSTRAP_PREFETCH_DAY_COUNT_DESKTOP = 3;
const FIRE_BOOTSTRAP_PREFETCH_DAY_COUNT_MOBILE = 1;
const FIRE_PREFETCH_VISIBLE_THUMBNAILS_DESKTOP = 6;
const FIRE_PREFETCH_VISIBLE_THUMBNAILS_MOBILE = 2;
const FIRE_MEDIA_FILTER_OPTIONS: { label: string; value: FireMediaFilter }[] = [
  { label: 'IMAGES', value: 'IMAGE' },
  { label: 'CAROUSELS', value: 'CAROUSEL' },
  { label: 'REELS', value: 'REEL' },
  { label: 'ALL', value: 'ALL' },
];

function isStandaloneDisplayMode(): boolean {
  if (typeof window === 'undefined') return false;
  return window.matchMedia('(display-mode: standalone)').matches
    || Boolean((window.navigator as Navigator & { standalone?: boolean }).standalone);
}

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

function normalizeWarmupBucket(raw: string): WarmupMediaBucket | null {
  const normalized = raw.trim().toLowerCase();
  if (!normalized) return null;
  if (normalized.includes('reel') || normalized.includes('video')) return 'REEL';
  if (normalized.includes('sidecar') || normalized.includes('carousel')) return 'CAROUSEL';
  if (normalized.includes('image') || normalized.includes('photo')) return 'IMAGE';
  return null;
}

function buildWarmupSummaryKey(feederId: number, bucket: WarmupMediaBucket): string {
  return `${feederId}:${bucket}`;
}

function buildWarmupGate(item: FireAlertItem, warmupSummary: WarmupSummary): FireWarmupGate | null {
  const feederId = item.feederId ?? NaN;
  if (!Number.isFinite(feederId)) return null;
  const bucket = normalizeWarmupBucket(item.surfaceMediaType || item.mediaType || '');
  if (!bucket) return null;

  const count = Math.max(0, warmupSummary[buildWarmupSummaryKey(feederId, bucket)] ?? 0);
  const cappedCount = Math.min(count, WARMUP_REQUIRED);

  return {
    bucket,
    count,
    required: WARMUP_REQUIRED,
    isLocked: count < WARMUP_REQUIRED,
    headline: 'Still warming up',
    body: `Unlocks after ${WARMUP_REQUIRED} posts in this format.`,
    progressLabel: `${cappedCount}/${WARMUP_REQUIRED} ready`,
  };
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

function firePageCacheTtl(day: string) {
  return day === toIstDayKey(new Date()) ? FIRE_LIVE_CACHE_TTL : FIRE_CACHE_TTL;
}

function inferUrgency(alertType: string, p: number | null): AlertUrgency {
  const t = alertType.toLowerCase();
  if (t === 'now' || t === 'today' || t === 'watch') return t;
  if (p != null && p <= 10) return 'now';
  if (p != null && p <= 25) return 'today';
  return 'watch';
}

function normalizeSignalContext(value: string): 'own' | 'cross' | 'anchor' {
  const normalized = value.trim().toLowerCase();
  if (normalized === 'cross' || normalized === 'anchor') return normalized;
  return 'own';
}

function signalColor(context: 'own' | 'cross' | 'anchor'): string {
  if (context === 'cross') return '#E11D48';
  if (context === 'anchor') return '#4DA3FF';
  return '#FF6B00';
}

function fallbackSignalLabel(code: string): string {
  return code
    .trim()
    .replace(/_/g, ' ')
    .replace(/\s+/g, ' ')
    .toLowerCase()
    .replace(/\b\w/g, (char) => char.toUpperCase());
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

function normalizeFireMediaFilter(value: unknown): FireMediaFilter {
  const normalized = String(value || 'ALL').trim().toUpperCase();
  return normalized === 'IMAGE' || normalized === 'CAROUSEL' || normalized === 'REEL' ? normalized : 'ALL';
}

function mediaFilterLabel(value: FireMediaFilter): string {
  if (value === 'IMAGE') return 'IMAGES';
  if (value === 'CAROUSEL') return 'CAROUSELS';
  if (value === 'REEL') return 'REELS';
  return 'ALL';
}

function flattenSelectedFeederIds(filters: FireFilterState): number[] {
  return sortNumberList(
    Object.entries(filters.selectedFeederIdsByFeed)
      .filter(([feedId]) => filters.selectedFeedIds.includes(Number(feedId)))
      .flatMap(([, feederIds]) => feederIds),
  );
}

function createDefaultFireFilters(): FireFilterState {
  return {
    threshold: 'ALL',
    mediaFilter: 'ALL',
    sort: 'best',
    selectedFeedIds: [],
    selectedFeederIdsByFeed: {},
    selectedCheckpoints: [],
  };
}

function serializeFilters(filters: FireFilterState): string {
  const normalizedFeederState = Object.entries(filters.selectedFeederIdsByFeed)
    .filter(([, feederIds]) => feederIds.length > 0)
    .sort(([a], [b]) => Number(a) - Number(b))
    .map(([feedId, feederIds]) => [feedId, sortNumberList(feederIds)]);

  return JSON.stringify({
    threshold: filters.threshold,
    mediaFilter: filters.mediaFilter,
    sort: filters.sort,
    selectedFeedIds: sortNumberList(filters.selectedFeedIds),
    selectedFeederIdsByFeed: normalizedFeederState,
    selectedCheckpoints: sortCheckpointList(filters.selectedCheckpoints),
  });
}

function filterFireAlertItems(items: FireAlertItem[], filters: FireFilterState): FireAlertItem[] {
  const thresholdLimit = filters.threshold === 'ALL' ? null : Number.parseInt(filters.threshold, 10);
  const selectedFeedIds = filters.selectedFeedIds.length > 0 ? new Set(sortNumberList(filters.selectedFeedIds)) : null;
  const selectedFeederIdsList = flattenSelectedFeederIds(filters);
  const selectedFeederIds = selectedFeederIdsList.length > 0 ? new Set(selectedFeederIdsList) : null;
  const selectedCheckpoints = filters.selectedCheckpoints.length > 0
    ? new Set(sortCheckpointList(filters.selectedCheckpoints).map((checkpoint) => checkpoint.toUpperCase()))
    : null;

  return items.filter((item) => {
    if (thresholdLimit != null) {
      const percentile = item.surfacePercentileExact ?? item.surfacePercentile;
      if (percentile == null || percentile > thresholdLimit) return false;
    }

    if (filters.mediaFilter !== 'ALL') {
      const mediaLabel = normalizeMediaLabel(item.surfaceMediaType || item.mediaType);
      if (mediaLabel !== filters.mediaFilter) return false;
    }

    if (selectedFeedIds && (!item.feedId || !selectedFeedIds.has(item.feedId))) return false;
    if (selectedFeederIds && (!item.feederId || !selectedFeederIds.has(item.feederId))) return false;

    if (selectedCheckpoints) {
      const checkpoint = String(item.checkpoint || '').trim().toUpperCase();
      if (!checkpoint || !selectedCheckpoints.has(checkpoint)) return false;
    }

    return true;
  });
}

function pickBackgroundPrefetchDays(days: string[], selectedDay: string, count: number): string[] {
  if (days.length === 0 || count <= 0) return [];

  const selectedIndex = days.indexOf(selectedDay);
  if (selectedIndex === -1) {
    return days.slice(0, count);
  }

  const picks: string[] = [];
  let offset = 1;
  while (picks.length < count) {
    const nextIndex = selectedIndex + offset;
    const prevIndex = selectedIndex - offset;
    let added = false;

    if (nextIndex < days.length) {
      picks.push(days[nextIndex]);
      added = true;
      if (picks.length >= count) break;
    }

    if (prevIndex >= 0) {
      picks.push(days[prevIndex]);
      added = true;
      if (picks.length >= count) break;
    }

    if (!added) break;
    offset += 1;
  }

  return picks;
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
    mediaFilter: normalizeFireMediaFilter(filters.mediaFilter),
    sort: filters.sort,
    selectedFeedIds,
    selectedFeederIdsByFeed,
    selectedCheckpoints: sortCheckpointList(filters.selectedCheckpoints),
  };
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

function parseTimeValue(value: string | undefined): number {
  return value ? Date.parse(value) || 0 : 0;
}

function sortFireAlertItems(items: FireAlertItem[], sortMode: FireSortMode): FireAlertItem[] {
  return [...items].sort((a, b) => {
    if (sortMode === 'recent') {
      const aRecent = parseTimeValue(a.postedAt || a.createdAt);
      const bRecent = parseTimeValue(b.postedAt || b.createdAt);
      if (bRecent !== aRecent) return bRecent - aRecent;
      return parseTimeValue(b.createdAt) - parseTimeValue(a.createdAt);
    }

    const aPercentile = a.surfacePercentileExact ?? a.surfacePercentile ?? Number.POSITIVE_INFINITY;
    const bPercentile = b.surfacePercentileExact ?? b.surfacePercentile ?? Number.POSITIVE_INFINITY;
    if (aPercentile !== bPercentile) return aPercentile - bPercentile;

    const aMetrics = asRecord(a.payload.metrics);
    const bMetrics = asRecord(b.payload.metrics);
    const aBestMetric = resolveBestMetricFromPayload(aMetrics, a.metricKey || asString(a.payload.best_metric), a.surfaceMediaType || a.mediaType);
    const bBestMetric = resolveBestMetricFromPayload(bMetrics, b.metricKey || asString(b.payload.best_metric), b.surfaceMediaType || b.mediaType);
    const aMultiple = metricMultipleFromPayload(aMetrics, aBestMetric) ?? Number.NEGATIVE_INFINITY;
    const bMultiple = metricMultipleFromPayload(bMetrics, bBestMetric) ?? Number.NEGATIVE_INFINITY;
    if (aMultiple !== bMultiple) return bMultiple - aMultiple;

    const aValue = metricValueFromPayload(aMetrics, aBestMetric) ?? a.metricValue ?? Number.NEGATIVE_INFINITY;
    const bValue = metricValueFromPayload(bMetrics, bBestMetric) ?? b.metricValue ?? Number.NEGATIVE_INFINITY;
    if (aValue !== bValue) return bValue - aValue;

    const aPostedAt = parseTimeValue(a.postedAt || a.createdAt);
    const bPostedAt = parseTimeValue(b.postedAt || b.createdAt);
    if (bPostedAt !== aPostedAt) return bPostedAt - aPostedAt;

    return parseTimeValue(b.createdAt) - parseTimeValue(a.createdAt);
  });
}

function fireAlertDeduplicationKey(item: FireAlertItem): string {
  const postKey = item.postKey?.trim();
  if (postKey) return postKey;
  return item.id;
}

function dedupeFireAlertItems(items: FireAlertItem[]): FireAlertItem[] {
  const deduped = new Map<string, FireAlertItem>();
  for (const item of items) {
    const key = fireAlertDeduplicationKey(item);
    if (!deduped.has(key)) {
      deduped.set(key, item);
    }
  }
  return Array.from(deduped.values());
}

function fireStateShellStyle(): CSSProperties {
  return {
    marginTop: 'calc(var(--fire-header-height, 168px) + 16px)',
    minHeight: 'calc(var(--fire-app-height, 100dvh) - var(--fire-header-height, 168px) - var(--fire-bottom-clearance, 88px) - 24px)',
  };
}

function normalizeAlertRow(row: AlertRow): FireAlertItem | null {
  const payload = asRecord(row.payload);
  const layers = asRecord(payload.layers);
  const status = asString(row.status).trim().toLowerCase();
  if (status && TERMINAL_STATUSES.has(status)) return null;

  const meta = asRecord(payload.meta);
  const rawCardKind = asString(meta.card_kind);
  if (rawCardKind && rawCardKind !== 'tracking') return null;
  const cardKind = 'tracking' as const;
  const checkpoint = asString(row.checkpoint) || asString(meta.checkpoint) || 'd1';
  const surfaceHandle = asString(row.surface_handle) || asString(meta.handle);
  const surfaceMediaType = normalizeMediaLabel(
    asString(row.surface_media_type) || asString(row.surface_mediaType) || asString(meta.media_type),
  );
  const metrics = asRecord(payload.metrics);
  const bestMetric = resolveBestMetricFromPayload(
    metrics,
    asString(payload.best_metric) || asString(row.metric_key) || 'views',
    surfaceMediaType,
  );
  const bestMetricData = asRecord(metrics[bestMetric]);
  const position = asRecord(payload.position);
  const surfacePercentile =
    asNumber(position.percentile) ??
    asNumber(row.surface_percentile) ??
    asNumber(bestMetricData.percentile);
  const surfacePercentileExact =
    asNumber(position.overall_percentile) ??
    asNumber(row.surface_percentile_exact) ??
    surfacePercentile;
  const metricValue = asNumber(bestMetricData.value) ?? asNumber(row.surface_metric_value) ?? asNumber(row.metric_value);
  const surfaceDelta = asNumber(row.surface_delta);
  const createdAt = asString(row.created_at);
  const postedAt = asString(row.posted_at) || asString(meta.posted_at) || createdAt;
  const businessDateKey =
    asString(row.business_date_ist) ||
    asString(meta.business_date_ist) ||
    toIstDayKey(createdAt);
  const signalCode = asString(row.signal_code) || asString(meta.signal_code) || 'UNKNOWN_SIGNAL';
  const signalContext = normalizeSignalContext(asString(row.context) || asString(meta.signal_context));
  const signalMeta = getFireSignalMeta(signalCode);
  const hideSignalChrome = true;
  const cueLabels: string[] = [];
  const signalLabel = '';
  const signalHeadline = signalMeta?.headline || fallbackSignalLabel(signalCode);
  const resolvedThumbnailUrl =
    asString(row.thumbnail_url)
    || asString(row.resolved_thumbnail_url)
    || asString(meta.resolved_thumbnail_url)
    || asString(meta.thumbnail_url);
  const resolvedPreviewUrl =
    asString(row.resolved_preview_url)
    || asString(meta.resolved_preview_url)
    || asString(row.preview_url)
    || asString(meta.preview_url);
  const title = `@${surfaceHandle ? surfaceHandle.toUpperCase() : 'FEEDER'} · ${checkpoint.toUpperCase()}`;

  return {
    id: `alert-${asString(row.id) || asString(row.dedupe_key) || Math.random().toString(36).slice(2)}`,
    cardKind,
    postKey: asString(row.post_key),
    feedId: asNumber(row.feed_id) ?? undefined,
    feederId: asNumber(row.feeder_id) ?? undefined,
    signalCode,
    signalContext,
    signalLabel,
    signalHeadline,
    urgency: inferUrgency(asString(row.alert_type), surfacePercentileExact),
    color: signalColor(signalContext),
    handle: `@${surfaceHandle || 'feed'}`,
    title,
    action: '',
    percentileTag: percentileToTag(surfacePercentileExact),
    mediaType: surfaceMediaType,
    stage: checkpoint.toUpperCase(),
    percentile: surfacePercentile == null ? undefined : String(Math.round(surfacePercentile)),
    delta: formatDelta(surfaceDelta, checkpoint),
    evidence: cueLabels,
    timeAgo: timeAgoText(createdAt),
    createdAt,
    postUrl: asString(meta.post_url) || 'https://instagram.com',
    thumbnailUrl: resolvedThumbnailUrl,
    previewUrl: resolvedPreviewUrl,
    businessDateKey,
    businessDateIst: businessDateKey,
    status,
    surfacePercentile,
    surfacePercentileExact,
    surfaceDelta,
    trajectoryDeltaPercentile: surfaceDelta,
    surfaceHandle,
    surfaceMediaType,
    checkpoint: checkpoint.toUpperCase(),
    postedAt,
    metricValue,
    metricKey: bestMetric,
    payload,
    layers: layers as FireLayers,
    intelligenceSkipped: row.intelligence_skipped === true,
    hideSignalChrome,
    stamp: {
      handle: surfaceHandle,
      mediaType: surfaceMediaType,
      checkpoint: checkpoint.toUpperCase(),
      metricLabel: bestMetric.toUpperCase(),
      metricValue,
    },
  };
}

async function fetchMeta(): Promise<{ days: string[]; feeds: FireFeedOption[]; warmupSummary: WarmupSummary }> {
  const res = await fetch('/api/fire?mode=meta', { cache: 'no-store' });
  if (!res.ok) throw new Error(`Meta fetch failed: ${res.status}`);
  const data = await res.json();
  return { days: data.days ?? [], feeds: data.feeds ?? [], warmupSummary: data.warmupSummary ?? {} };
}

function normalizePagePayload(data: {
  rows?: AlertRow[];
  hasMore?: boolean;
  total?: number;
  availableCheckpoints?: string[];
  snapshotToken?: string | null;
}): { items: FireAlertItem[]; hasMore: boolean; total: number; availableCheckpoints: string[]; snapshotToken: string | null } {
  const items = dedupeFireAlertItems(
    ((data.rows || []) as AlertRow[])
      .map(normalizeAlertRow)
      .filter((row): row is FireAlertItem => row !== null),
  );
  return {
    items,
    hasMore: data.hasMore ?? false,
    total: data.total ?? 0,
    availableCheckpoints: sortCheckpointList((data.availableCheckpoints ?? []).map((value: string) => String(value).toUpperCase())),
    snapshotToken: typeof data.snapshotToken === 'string' && data.snapshotToken.trim() ? data.snapshotToken : null,
  };
}

type FirePagePayload = ReturnType<typeof normalizePagePayload>;

async function fetchBootstrap(pageSize: number, prefetchDays: number): Promise<{
  days: string[];
  feeds: FireFeedOption[];
  warmupSummary: WarmupSummary;
  initialDay: string;
  initialPage: FirePagePayload;
  prefetchedPages: Array<{ day: string; payload: FirePagePayload }>;
}> {
  const params = new URLSearchParams({
    mode: 'bootstrap',
    pageSize: String(pageSize),
    prefetchDays: String(prefetchDays),
  });
  const res = await fetch(`/api/fire?${params.toString()}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`Bootstrap fetch failed: ${res.status}`);
  const data = await res.json();
  return {
    days: data.days ?? [],
    feeds: data.feeds ?? [],
    warmupSummary: data.warmupSummary ?? {},
    initialDay: typeof data.initialDay === 'string' ? data.initialDay : '',
    initialPage: normalizePagePayload(data.initialPage ?? {}),
    prefetchedPages: Array.isArray(data.prefetchedPages)
      ? data.prefetchedPages
        .map((entry: Record<string, unknown>): { day: string; payload: FirePagePayload } => ({
          day: typeof entry.day === 'string' ? entry.day : '',
          payload: normalizePagePayload(entry.payload ?? {}),
        }))
        .filter((entry: { day: string; payload: FirePagePayload }) => Boolean(entry.day))
      : [],
  };
}

function prewarmThumbnails(payloads: FirePagePayload[], visibleCount: number) {
  if (typeof window === 'undefined') return;

  const urls = Array.from(
    new Set(
      payloads
        .flatMap((payload) => payload.items.slice(0, visibleCount))
        .map((item) => item.thumbnailUrl?.trim() || '')
        .filter(Boolean),
    ),
  );

  if (urls.length === 0) return;

  const schedule = typeof window.requestIdleCallback === 'function'
    ? (fn: () => void) => window.requestIdleCallback(fn, { timeout: 1500 })
    : (fn: () => void) => window.setTimeout(fn, 120);

  schedule(() => {
    for (const url of urls) {
      const image = new Image();
      image.decoding = 'async';
      image.src = url;
    }
  });
}

async function fetchPage(
  day: string,
  filters: FireFilterState,
  cursor: number,
  snapshotToken: string | null,
  pageSize = FIRE_INITIAL_BATCH_SIZE,
): Promise<{ items: FireAlertItem[]; hasMore: boolean; total: number; availableCheckpoints: string[]; snapshotToken: string | null }> {
  const feedIds = sortNumberList(filters.selectedFeedIds);
  const feederIds = flattenSelectedFeederIds(filters);
  const checkpoints = sortCheckpointList(filters.selectedCheckpoints);

  const res = await fetch('/api/fire', {
    method: 'POST',
    cache: 'no-store',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      day,
      threshold: filters.threshold,
      mediaFilter: filters.mediaFilter,
      sort: filters.sort,
      cursor,
      pageSize,
      feedIds,
      feederIds,
      checkpoints,
      snapshotToken,
    }),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(body.error || `HTTP ${res.status}`);
  }

  return normalizePagePayload(await res.json());
}

export default function FirePage() {
  const { play } = useAppHaptics();
  const headerRef = useRef<HTMLDivElement>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const cursorRef = useRef(0);
  const fetchKeyRef = useRef('');
  const snapshotTokenRef = useRef<string | null>(null);
  const backgroundPrefetchKeyRef = useRef<string | null>(null);
  const lastMetaRefreshAtRef = useRef(0);
  const cardsRef = useRef<FireAlertItem[]>([]);

  const [pickerDays, setPickerDays] = useState<string[]>([]);
  const [availableFeeds, setAvailableFeeds] = useState<FireFeedOption[]>([]);
  const [availableCheckpoints, setAvailableCheckpoints] = useState<string[]>([]);
  const [warmupSummary, setWarmupSummary] = useState<WarmupSummary>({});
  const [cards, setCards] = useState<FireAlertItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [total, setTotal] = useState(0);
  const [selectedDay, setSelectedDay] = useState('');
  const [isZSpaceOpen, setIsZSpaceOpen] = useState(false);
  const [filters, setFilters] = useState<FireFilterState>(() => createDefaultFireFilters());
  const [headerHeight, setHeaderHeight] = useState(168);
  const [desktopModalCard, setDesktopModalCard] = useState<FireAlertItem | null>(null);
  const [isStandaloneMode, setIsStandaloneMode] = useState(false);
  const [isDesktopViewport, setIsDesktopViewport] = useState(false);
  const [initialDataReady, setInitialDataReady] = useState(false);
  const useBrowserPageScroll = !isDesktopViewport;
  const useRootSnap = useBrowserPageScroll && isStandaloneMode;
  const headerCompressed = useCompressedOnScroll(contentRef, useBrowserPageScroll && !isDesktopViewport, 24);

  const hasActiveFilters = filters.mediaFilter !== 'ALL'
    || filters.selectedFeedIds.length > 0
    || Object.keys(filters.selectedFeederIdsByFeed).length > 0
    || filters.selectedCheckpoints.length > 0;

  const updateFilters = useCallback((updater: SetStateAction<FireFilterState>) => {
    startTransition(() => {
      setFilters(updater);
    });
  }, []);

  const desktopSelectionChips = useMemo(() => buildDesktopSelectionChips(filters, availableFeeds), [filters, availableFeeds]);
  const deckResetKey = useMemo(() => `${selectedDay}:${serializeFilters(filters)}`, [selectedDay, filters]);
  const handleSelectedDayChange = useCallback((nextDay: string) => {
    startTransition(() => {
      setSelectedDay(nextDay);
    });
  }, []);
  const displayCards = useMemo(
    () => sortFireAlertItems(
      filterFireAlertItems(
        dedupeFireAlertItems(cards).map((card) => ({ ...card, warmupGate: buildWarmupGate(card, warmupSummary) })),
        filters,
      ),
      filters.sort,
    ),
    [cards, filters, warmupSummary],
  );
  const desktopModalIndex = useMemo(
    () => (desktopModalCard ? displayCards.findIndex((card) => card.id === desktopModalCard.id) : -1),
    [desktopModalCard, displayCards],
  );

  useEffect(() => {
    cardsRef.current = cards;
  }, [cards]);

  useEffect(() => {
    let mounted = true;
    const initialViewportIsDesktop = typeof window !== 'undefined' && window.matchMedia('(min-width: 1024px)').matches;
    type FireCachedState = {
      pickerDays: string[];
      availableFeeds: FireFeedOption[];
      selectedDay: string;
      filters: FireFilterState;
      cards: FireAlertItem[];
      availableCheckpoints: string[];
      warmupSummary: WarmupSummary;
      hasMore: boolean;
      total: number;
      cursor: number;
      snapshotToken: string | null;
    };
    const cachedStateEntry = readCache<FireCachedState>(FIRE_STATE_CACHE_KEY);
    const cachedState = cachedStateEntry
      && Date.now() - cachedStateEntry.ts <= firePageCacheTtl(cachedStateEntry.data.selectedDay || '')
      ? cachedStateEntry.data
      : null;

    if (cachedState) {
      const cachedCards = dedupeFireAlertItems(cachedState.cards || []);
      setPickerDays(cachedState.pickerDays || []);
      setAvailableFeeds(cachedState.availableFeeds || []);
      setSelectedDay(cachedState.selectedDay || '');
      setFilters(pruneFilters(cachedState.filters || createDefaultFireFilters(), cachedState.availableFeeds || []));
      setCards(cachedCards);
      setAvailableCheckpoints(sortCheckpointList(cachedState.availableCheckpoints || []));
      setWarmupSummary(cachedState.warmupSummary || {});
      setHasMore(Boolean(cachedState.hasMore));
      setTotal(cachedState.total || 0);
      cursorRef.current = cachedCards.length;
      snapshotTokenRef.current = typeof cachedState.snapshotToken === 'string' && cachedState.snapshotToken.trim()
        ? cachedState.snapshotToken
        : null;
      setLoading(false);
      setInitialDataReady(true);
      return () => { mounted = false; };
    }

    (async () => {
      try {
        const bootstrap = await fetchBootstrap(
          FIRE_INITIAL_BATCH_SIZE,
          initialViewportIsDesktop
            ? FIRE_BOOTSTRAP_PREFETCH_DAY_COUNT_DESKTOP
            : FIRE_BOOTSTRAP_PREFETCH_DAY_COUNT_MOBILE,
        );
        if (!mounted) return;

        const defaultFilters = createDefaultFireFilters();
        const nextDays = bootstrap.days || [];
        const nextFeeds = bootstrap.feeds || [];
        const nextWarmupSummary = bootstrap.warmupSummary || {};
        const initialDay = bootstrap.initialDay || nextDays[0] || '';
        const initialPayload = bootstrap.initialPage || {
          items: [],
          hasMore: false,
          total: 0,
          availableCheckpoints: [],
          snapshotToken: null,
        };
        const prefetchedPages = bootstrap.prefetchedPages || [];

        setCache(FIRE_META_CACHE_KEY, { days: nextDays, feeds: nextFeeds, warmupSummary: nextWarmupSummary });
        for (const entry of prefetchedPages) {
          setCache(`${FIRE_PAGE_CACHE_PREFIX}:${entry.day}:${serializeFilters(defaultFilters)}`, {
            items: entry.payload.items,
            hasMore: entry.payload.hasMore,
            total: entry.payload.total,
            cursor: entry.payload.items.length,
            availableCheckpoints: entry.payload.availableCheckpoints,
            snapshotToken: entry.payload.snapshotToken,
          });
        }
        setCache(`${FIRE_PAGE_CACHE_PREFIX}:${initialDay}:${serializeFilters(defaultFilters)}`, {
          items: initialPayload.items,
          hasMore: initialPayload.hasMore,
          total: initialPayload.total,
          cursor: initialPayload.items.length,
          availableCheckpoints: initialPayload.availableCheckpoints,
          snapshotToken: initialPayload.snapshotToken,
        });
        prewarmThumbnails(
          prefetchedPages.length > 0 ? prefetchedPages.map((entry) => entry.payload) : [initialPayload],
          initialViewportIsDesktop ? FIRE_PREFETCH_VISIBLE_THUMBNAILS_DESKTOP : FIRE_PREFETCH_VISIBLE_THUMBNAILS_MOBILE,
        );

        setPickerDays(nextDays);
        setAvailableFeeds(nextFeeds);
        setWarmupSummary(nextWarmupSummary);
        setSelectedDay(initialDay);
        setFilters(defaultFilters);
        setCards(initialPayload.items);
        setAvailableCheckpoints(initialPayload.availableCheckpoints);
        setHasMore(initialPayload.hasMore);
        setTotal(initialPayload.total);
        cursorRef.current = initialPayload.items.length;
        snapshotTokenRef.current = initialPayload.snapshotToken;
        setError(null);
      } catch (err) {
        if (!mounted) return;
        console.error('[FirePage] Bootstrap fetch error:', err);
      } finally {
        if (mounted) {
          setLoading(false);
          setInitialDataReady(true);
        }
      }
    })();

    return () => { mounted = false; };
  }, []);

  useEffect(() => {
    if (!initialDataReady || pickerDays.length === 0) return;

    const defaultFilters = createDefaultFireFilters();
    const defaultFilterKey = serializeFilters(defaultFilters);
    const prefetchKey = `${pickerDays.join(',')}:${selectedDay}:${isDesktopViewport ? 'desktop' : 'mobile'}`;
    if (backgroundPrefetchKeyRef.current === prefetchKey) return;
    backgroundPrefetchKeyRef.current = prefetchKey;

    let cancelled = false;
    let timeoutId: number | null = null;
    let idleId: number | null = null;

    const schedule = (task: () => void) => {
      if (typeof window.requestIdleCallback === 'function') {
        idleId = window.requestIdleCallback(task, { timeout: 1200 });
        return;
      }
      timeoutId = window.setTimeout(task, 180);
    };

    schedule(() => {
      void (async () => {
        const prewarmedPayloads: FirePagePayload[] = [];
        const daysToPrefetch = pickBackgroundPrefetchDays(
          pickerDays,
          selectedDay,
          isDesktopViewport
            ? FIRE_BACKGROUND_PREFETCH_DAY_COUNT_DESKTOP
            : FIRE_BACKGROUND_PREFETCH_DAY_COUNT_MOBILE,
        );

        for (const day of daysToPrefetch) {
          if (cancelled || day === selectedDay) continue;
          const cacheKey = `${FIRE_PAGE_CACHE_PREFIX}:${day}:${defaultFilterKey}`;
          const cached = getCache<{
            items: FireAlertItem[];
            hasMore: boolean;
            total: number;
            cursor: number;
            availableCheckpoints: string[];
            snapshotToken: string | null;
          }>(cacheKey, firePageCacheTtl(day));
          if (cached) continue;

          try {
            const result = await fetchPage(day, defaultFilters, 0, null, FIRE_INITIAL_BATCH_SIZE);
            if (cancelled) return;
            setCache(cacheKey, {
              items: result.items,
              hasMore: result.hasMore,
              total: result.total,
              cursor: result.items.length,
              availableCheckpoints: result.availableCheckpoints,
              snapshotToken: result.snapshotToken,
            });
            prewarmedPayloads.push(result);
          } catch (prefetchError) {
            if (cancelled) return;
            console.error('[FirePage] Day prefetch error:', prefetchError);
          }
        }

        if (!cancelled && prewarmedPayloads.length > 0) {
          prewarmThumbnails(
            prewarmedPayloads,
            isDesktopViewport ? FIRE_PREFETCH_VISIBLE_THUMBNAILS_DESKTOP : FIRE_PREFETCH_VISIBLE_THUMBNAILS_MOBILE,
          );
        }
      })();
    });

    return () => {
      cancelled = true;
      if (timeoutId != null) window.clearTimeout(timeoutId);
      if (idleId != null && typeof window.cancelIdleCallback === 'function') {
        window.cancelIdleCallback(idleId);
      }
    };
  }, [initialDataReady, isDesktopViewport, pickerDays, selectedDay]);

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
  }, [headerCompressed, pickerDays, availableCheckpoints.length, filters.mediaFilter, filters.selectedFeedIds.length, filters.selectedCheckpoints.length]);

  useEffect(() => {
    if (!initialDataReady) return;
    if (!selectedDay) return;
    setCache(FIRE_STATE_CACHE_KEY, {
      pickerDays,
      availableFeeds,
      selectedDay,
      filters,
      cards,
      availableCheckpoints,
      warmupSummary,
      hasMore,
      total,
      cursor: cursorRef.current,
      snapshotToken: snapshotTokenRef.current,
    });
  }, [initialDataReady, pickerDays, availableFeeds, selectedDay, filters, cards, availableCheckpoints, warmupSummary, hasMore, total]);

  useEffect(() => {
    document.title = 'Fire';
  }, []);

  useEffect(() => {
    const standaloneMql = window.matchMedia('(display-mode: standalone)');
    const desktopMql = window.matchMedia('(min-width: 1024px)');
    const syncMode = () => {
      setIsStandaloneMode(isStandaloneDisplayMode());
      setIsDesktopViewport(desktopMql.matches);
    };
    syncMode();
    standaloneMql.addEventListener?.('change', syncMode);
    desktopMql.addEventListener?.('change', syncMode);
    window.addEventListener('resize', syncMode);
    return () => {
      standaloneMql.removeEventListener?.('change', syncMode);
      desktopMql.removeEventListener?.('change', syncMode);
      window.removeEventListener('resize', syncMode);
    };
  }, []);

  useEffect(() => {
    return acquireDocumentClass('fm-fire-immersive');
  }, []);

  useEffect(() => {
    const html = document.documentElement;
    html.classList.toggle('fm-fire-root-scroll', useRootSnap);
    return () => {
      html.classList.remove('fm-fire-root-scroll');
    };
  }, [useRootSnap]);

  useEffect(() => {
    const html = document.documentElement;
    html.style.setProperty('--fire-header-height', `${headerHeight}px`);
    html.style.setProperty('--fire-bottom-clearance', isStandaloneMode ? '86px' : '88px');
    return () => {
      html.style.removeProperty('--fire-header-height');
      html.style.removeProperty('--fire-bottom-clearance');
    };
  }, [headerHeight, isStandaloneMode]);

  useEffect(() => {
    if (!useBrowserPageScroll) return undefined;
    return acquireRootPageScroll();
  }, [useBrowserPageScroll]);

  useEffect(() => {
    const syncViewportHeight = () => {
      const vv = window.visualViewport;
      const visualHeight = Math.round(vv?.height ?? 0);
      const innerHeight = Math.round(window.innerHeight);
      const clientHeight = Math.round(document.documentElement.clientHeight);
      const nextHeight = isStandaloneDisplayMode()
        ? Math.max(innerHeight, clientHeight, visualHeight)
        : visualHeight || innerHeight || clientHeight;
      document.documentElement.style.setProperty('--fire-app-height', `${nextHeight}px`);
    };
    const viewport = getVisualViewportEventTarget();
    syncViewportHeight();
    viewport?.addEventListener('resize', syncViewportHeight);
    viewport?.addEventListener('scroll', syncViewportHeight);
    window.addEventListener('resize', syncViewportHeight);
    return () => {
      viewport?.removeEventListener('resize', syncViewportHeight);
      viewport?.removeEventListener('scroll', syncViewportHeight);
      window.removeEventListener('resize', syncViewportHeight);
    };
  }, []);

  useEffect(() => {
    const updatePwaPad = () => {
      const isStandalone = isStandaloneDisplayMode();
      const vv = window.visualViewport;
      const offset = Math.max(0, Math.round(vv?.offsetTop ?? 0));
      const pad = offset + 8;
      document.documentElement.style.setProperty('--pwa-top-pad', isStandalone ? `${pad}px` : '0px');
      document.documentElement.style.setProperty('--pwa-top-fix', isStandalone ? `${-(offset + 6)}px` : '0px');
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
    const cached = getCache<{ days: string[]; feeds: FireFeedOption[]; warmupSummary: WarmupSummary }>(FIRE_META_CACHE_KEY, FIRE_CACHE_TTL);
    if (cached) {
      setAvailableFeeds((prev) => {
        const next = cached.feeds ?? [];
        return JSON.stringify(prev) === JSON.stringify(next) ? prev : next;
      });
      setWarmupSummary((prev) => {
        const next = cached.warmupSummary ?? {};
        return JSON.stringify(prev) === JSON.stringify(next) ? prev : next;
      });
      if ((cached.days ?? []).length > 0) {
        setPickerDays((prevDays) => {
          const prevTop = prevDays[0] || '';
          setSelectedDay((current) => {
            if (!current) return cached.days[0];
            if (!cached.days.includes(current)) return cached.days[0];
            if (current === prevTop && prevTop !== cached.days[0]) return cached.days[0];
            return current;
          });
          if (JSON.stringify(prevDays) === JSON.stringify(cached.days)) return prevDays;
          return cached.days;
        });
      }

      return;
    }

    let meta: Awaited<ReturnType<typeof fetchMeta>>;
    try {
      meta = await fetchMeta();
    } catch (error) {
      if (cached) return;
      throw error;
    }

    setCache(FIRE_META_CACHE_KEY, meta);
    setAvailableFeeds((prev) => {
      const next = meta.feeds ?? [];
      return JSON.stringify(prev) === JSON.stringify(next) ? prev : next;
    });
    setWarmupSummary((prev) => {
      const next = meta.warmupSummary ?? {};
      return JSON.stringify(prev) === JSON.stringify(next) ? prev : next;
    });
    if (meta.days.length > 0) {
      setPickerDays((prevDays) => {
        const prevTop = prevDays[0] || '';
        setSelectedDay((current) => {
          if (!current) return meta.days[0];
          if (!meta.days.includes(current)) return meta.days[0];
          if (current === prevTop && prevTop !== meta.days[0]) return meta.days[0];
          return current;
        });
        if (JSON.stringify(prevDays) === JSON.stringify(meta.days)) return prevDays;
        return meta.days;
      });
    }
  }, []);

  useEffect(() => {
    if (!initialDataReady) return;
    let mounted = true;
    const run = async () => {
      try {
        await refreshMeta();
        lastMetaRefreshAtRef.current = Date.now();
      } catch (err) {
        if (!mounted) return;
        console.error('[FirePage] Meta fetch error:', err);
        setError(err instanceof Error ? err.message : 'Failed to load');
        setLoading(false);
      }
    };

    run();

    const runIfStale = () => {
      if (Date.now() - lastMetaRefreshAtRef.current < FIRE_META_REFRESH_INTERVAL_MS) return;
      void run();
    };
    const onVisibility = () => {
      if (document.visibilityState === 'visible') runIfStale();
    };
    const onFocus = () => runIfStale();
    document.addEventListener('visibilitychange', onVisibility);
    window.addEventListener('focus', onFocus);

    return () => {
      mounted = false;
      document.removeEventListener('visibilitychange', onVisibility);
      window.removeEventListener('focus', onFocus);
    };
  }, [initialDataReady, refreshMeta]);

  useEffect(() => {
    setFilters((current) => {
      const pruned = pruneFilters(current, availableFeeds);
      return serializeFilters(pruned) === serializeFilters(current) ? current : pruned;
    });
  }, [availableFeeds]);

  useEffect(() => {
    setFilters((current) => {
      const next = current.selectedCheckpoints.filter((checkpoint) => availableCheckpoints.includes(checkpoint));
      return JSON.stringify(next) === JSON.stringify(current.selectedCheckpoints) ? current : { ...current, selectedCheckpoints: next };
    });
  }, [availableCheckpoints]);

  useEffect(() => {
    setDesktopModalCard((current) => {
      if (!current) return null;
      const nextCard = displayCards.find((card) => card.id === current.id) || null;
      if (!nextCard?.warmupGate?.isLocked) return nextCard;
      return null;
    });
  }, [displayCards]);

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
      snapshotToken: string | null;
    }>(cacheKey, firePageCacheTtl(selectedDay));

    if (cached) {
      const cachedItems = dedupeFireAlertItems(cached.items || []);
      setCards(cachedItems);
      setHasMore(cached.hasMore);
      setTotal(cached.total);
      setAvailableCheckpoints((prev) => {
        const next = sortCheckpointList(cached.availableCheckpoints || []);
        return JSON.stringify(prev) === JSON.stringify(next) ? prev : next;
      });
      cursorRef.current = cachedItems.length;
      snapshotTokenRef.current = typeof cached.snapshotToken === 'string' && cached.snapshotToken.trim()
        ? cached.snapshotToken
        : null;
      setLoading(false);
      setIsRefreshing(false);
      setError(null);
      return () => { mounted = false; };
    }

    cursorRef.current = 0;
    snapshotTokenRef.current = null;
    const preserveVisibleCards = cardsRef.current.length > 0;
    if (preserveVisibleCards) {
      setIsRefreshing(true);
    } else {
      setLoading(true);
      setCards([]);
      setHasMore(false);
    }
    setError(null);

    (async () => {
      try {
        const result = await fetchPage(selectedDay, normalizedFilters, 0, null, FIRE_INITIAL_BATCH_SIZE);
        if (!mounted || fetchKeyRef.current !== key) return;
        setCards(result.items);
        setHasMore(result.hasMore);
        setTotal(result.total);
        setAvailableCheckpoints((prev) => {
          const next = result.availableCheckpoints;
          return JSON.stringify(prev) === JSON.stringify(next) ? prev : next;
        });
        cursorRef.current = result.items.length;
        snapshotTokenRef.current = result.snapshotToken;
        setCache(cacheKey, {
          items: result.items,
          hasMore: result.hasMore,
          total: result.total,
          cursor: result.items.length,
          availableCheckpoints: result.availableCheckpoints,
          snapshotToken: result.snapshotToken,
        });
      } catch (err) {
        if (!mounted || fetchKeyRef.current !== key) return;
        if (!preserveVisibleCards) {
          setError(err instanceof Error ? err.message : 'Failed to load alerts');
        } else {
          console.error('[FirePage] Refresh error:', err);
        }
      } finally {
        if (mounted && fetchKeyRef.current === key) {
          setLoading(false);
          setIsRefreshing(false);
        }
      }
    })();

    return () => { mounted = false; };
  }, [initialDataReady, selectedDay, filters, availableFeeds]);

  const handleLoadMore = useCallback(async () => {
    if (loadingMore || !hasMore || !selectedDay) return;
    setLoadingMore(true);
    const key = fetchKeyRef.current;
    try {
      const normalizedFilters = pruneFilters(filters, availableFeeds);
      const result = await fetchPage(
        selectedDay,
        normalizedFilters,
        cursorRef.current,
        snapshotTokenRef.current,
        FIRE_INITIAL_BATCH_SIZE,
      );
      if (fetchKeyRef.current !== key) return;
      let nextCursor = cursorRef.current;
      setCards((prev) => {
        const nextCards = dedupeFireAlertItems([...prev, ...result.items]);
        nextCursor = nextCards.length;
        setCache(`${FIRE_PAGE_CACHE_PREFIX}:${key}`, {
          items: nextCards,
          hasMore: result.hasMore,
          total: result.total,
          cursor: nextCards.length,
          availableCheckpoints: result.availableCheckpoints,
          snapshotToken: result.snapshotToken,
        });
        return nextCards;
      });
      setHasMore(result.hasMore);
      setTotal(result.total);
      setAvailableCheckpoints(result.availableCheckpoints);
      snapshotTokenRef.current = result.snapshotToken;
      cursorRef.current = nextCursor;
    } catch (err) {
      console.error('[FirePage] Load more error:', err);
    } finally {
      setLoadingMore(false);
    }
  }, [availableFeeds, filters, hasMore, loadingMore, selectedDay]);

  const openPreviousDesktopCard = useCallback(() => {
    setDesktopModalCard((current) => {
      if (!current) return current;
      const currentIndex = displayCards.findIndex((card) => card.id === current.id);
      if (currentIndex <= 0) return current;
      return displayCards[currentIndex - 1] || current;
    });
  }, [displayCards]);

  const openNextDesktopCard = useCallback(() => {
    setDesktopModalCard((current) => {
      if (!current) return current;
      const currentIndex = displayCards.findIndex((card) => card.id === current.id);
      if (currentIndex < 0 || currentIndex >= displayCards.length - 1) return current;
      return displayCards[currentIndex + 1] || current;
    });
  }, [displayCards]);

  const rootStyle = {
    '--fire-header-height': `${headerHeight}px`,
    '--fire-bottom-clearance': isStandaloneMode ? '86px' : '88px',
    height: useBrowserPageScroll ? undefined : 'var(--fire-app-height, 100dvh)',
    minHeight: useBrowserPageScroll ? '100dvh' : undefined,
  } as CSSProperties;

  return (
    <motion.div
      data-fire-immersive="true"
      initial={false}
      animate={{ opacity: 1 }}
      transition={{ duration: 0.24, ease: APPLE_EASE }}
      className={cn(
        'fm-fire-page-surface relative w-full text-foreground select-none',
        useBrowserPageScroll ? 'overflow-visible' : 'overflow-hidden',
      )}
      style={rootStyle}
    >
      <ZSpaceFilter
        isOpen={isZSpaceOpen}
        onClose={() => setIsZSpaceOpen(false)}
        filters={filters}
        availableFeeds={availableFeeds}
        availableCheckpoints={availableCheckpoints}
        onChange={updateFilters}
      />

      <motion.div
        ref={headerRef}
        variants={HEADER_STAGGER_CONTAINER}
        initial="initial"
        animate="animate"
        className={cn(
          'pointer-events-auto inset-x-0 top-0 z-[100] flex flex-col items-center px-2 pt-[calc(10px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] sm:px-4 sm:pt-[calc(14px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] md:pt-[calc(20px+var(--pwa-top-fix,0px))] lg:px-4',
          useBrowserPageScroll ? 'fixed' : 'absolute',
        )}
      >
        <div className="relative fm-tab-header-shell">
          <div className={cn('fm-depth-chrome fm-depth-chrome--header w-full', headerCompressed && 'fm-depth-chrome--header-compressed')}>
            <div className="relative z-10 px-3.5 sm:px-4 lg:px-5">
              <div className={cn(
                'flex flex-col transition-[gap] duration-300 ease-[cubic-bezier(0.32,0.72,0,1)] lg:hidden',
                headerCompressed ? 'gap-1.5' : 'gap-2.5',
              )}>
                <motion.div variants={HEADER_ROW} className="flex items-center justify-between gap-3">
                  <motion.h1
                    animate={{
                      scale: headerCompressed ? 0.74 : 1,
                      opacity: headerCompressed ? 0.9 : 1,
                    }}
                    transition={{ type: 'spring', stiffness: 340, damping: 34 }}
                    className="origin-left shrink-0 text-[30px] font-black leading-none tracking-[0.14em] text-black will-change-transform sm:text-[38px] dark:text-white fm-depth-title"
                  >
                    FIRE
                  </motion.h1>
                  <div className="flex items-center gap-2">
                    <motion.button
                      whileTap={{ scale: 0.95 }}
                      type="button"
                      onClick={() => updateFilters((current) => ({ ...current, sort: current.sort === 'best' ? 'recent' : 'best' }))}
                      className="flex shrink-0 items-center gap-1.5 rounded-[14px] border border-white/70 bg-white/60 px-2.5 py-2 text-[9px] font-black uppercase tracking-[0.16em] text-black/70 shadow-[0_2px_6px_rgba(0,0,0,0.08),0_1px_0_rgba(255,255,255,0.9)_inset,0_-1px_0_rgba(0,0,0,0.04)_inset] dark:border-white/10 dark:bg-white/[0.06] dark:text-white/68 dark:shadow-[0_2px_8px_rgba(0,0,0,0.4),0_1px_0_rgba(255,255,255,0.06)_inset]"
                    >
                      <ArrowUpDown size={14} />
                      <span>{filters.sort === 'best' ? 'PCTL' : 'RECENT'}</span>
                    </motion.button>
                    <motion.button
                      whileTap={{ scale: 0.92 }}
                      onClick={() => {
                        play('snapLock');
                        setIsZSpaceOpen(true);
                      }}
                      className={cn(
                        'relative flex shrink-0 items-center justify-center rounded-[14px] border border-white/70 bg-white/60 p-2 shadow-[0_2px_6px_rgba(0,0,0,0.08),0_1px_0_rgba(255,255,255,0.9)_inset,0_-1px_0_rgba(0,0,0,0.04)_inset]',
                        'dark:border-white/10 dark:bg-white/[0.06] dark:shadow-[0_2px_8px_rgba(0,0,0,0.4),0_1px_0_rgba(255,255,255,0.06)_inset]',
                        hasActiveFilters ? 'text-[#E11D48]' : 'text-black/58 dark:text-white/45',
                      )}
                    >
                      <SlidersHorizontal size={20} />
                    </motion.button>
                  </div>
                </motion.div>
                <motion.div variants={HEADER_ROW} className="min-w-0 pointer-events-auto">
                  <ChronoTabs days={pickerDays} activeDay={selectedDay} onChange={handleSelectedDayChange} />
                </motion.div>
              </div>

              <div className="hidden flex-col gap-2 lg:flex">
                <div className="grid grid-cols-[minmax(148px,auto)_minmax(0,1fr)_auto] items-center gap-2.5">
                  <div className="text-[28px] font-black uppercase tracking-[0.18em] text-black dark:text-white fm-depth-title">
                    FIRE
                  </div>
                  <div className="flex justify-center px-0.5">
                    <ChronoTabs days={pickerDays} activeDay={selectedDay} onChange={handleSelectedDayChange} compact />
                  </div>
                  <div className="flex items-center justify-end">
                      <div className="min-w-[110px] rounded-[18px] border border-black/6 bg-white/68 px-2.5 py-1.5 text-center shadow-[0_10px_22px_rgba(0,0,0,0.08),inset_0_1px_0_rgba(255,255,255,0.8)] dark:border-white/10 dark:bg-white/[0.07] dark:shadow-[0_12px_24px_rgba(0,0,0,0.34),inset_0_1px_0_rgba(255,255,255,0.06)]">
                      <div className="text-[8px] font-black uppercase tracking-[0.22em] text-black/38 dark:text-white/32">
                        Post Type
                      </div>
                      <div className="mt-0.5 flex items-end justify-center gap-1">
                        <span className="text-[46px] font-black leading-[0.82] tracking-[-0.1em] text-black dark:text-white">
                          {total}
                        </span>
                        <span className="mb-1.5 rounded-full bg-[#E11D48] px-1.5 py-0.5 text-[7px] font-black uppercase tracking-[0.18em] text-white shadow-[0_4px_12px_rgba(225,29,72,0.22)]">
                          {mediaFilterLabel(filters.mediaFilter)}
                        </span>
                      </div>
                      <div className="-mt-1 text-[8px] font-black uppercase tracking-[0.16em] text-black/44 dark:text-white/38">
                        {selectedDay || '--'}
                      </div>
                    </div>
                  </div>
                </div>

                <div className="flex flex-wrap items-center gap-2 overflow-hidden rounded-[18px] border border-black/5 bg-black/[0.035] px-2.5 py-2 shadow-[inset_0_2px_8px_rgba(0,0,0,0.04)] dark:border-white/8 dark:bg-white/[0.03] dark:shadow-[inset_0_2px_8px_rgba(0,0,0,0.3)]">
                  <div className="flex items-center gap-1 rounded-[14px] border border-black/5 bg-white/58 p-1 shadow-[0_4px_12px_rgba(0,0,0,0.06),inset_0_1px_0_rgba(255,255,255,0.75)] dark:border-white/8 dark:bg-white/[0.05] dark:shadow-[0_8px_18px_rgba(0,0,0,0.35),inset_0_1px_0_rgba(255,255,255,0.06)]">
                    {FIRE_MEDIA_FILTER_OPTIONS.map((option) => {
                      const isActive = filters.mediaFilter === option.value;
                      return (
                        <motion.button
                            key={option.value}
                            type="button"
                            whileTap={{ scale: 0.95 }}
                            onClick={() => updateFilters((current) => ({ ...current, mediaFilter: option.value }))}
                            className={cn(
                              'relative overflow-hidden rounded-[11px] px-2.5 py-1.5 text-[9px] font-black uppercase tracking-[0.16em] transition-colors duration-200',
                              isActive
                                ? 'text-white'
                                : 'text-black/55 dark:text-white/45',
                            )}
                          >
                            {isActive && (
                              <motion.span
                                layoutId="fire-media-filter-pill-bg"
                                className="absolute inset-0 rounded-[11px] bg-[#E11D48] shadow-[0_6px_14px_rgba(225,29,72,0.22),inset_0_1px_0_rgba(255,255,255,0.75)] dark:shadow-[0_8px_20px_rgba(225,29,72,0.22),0_10px_24px_rgba(0,0,0,0.26)]"
                                transition={FIRE_CHROME_PILL_SPRING}
                              />
                            )}
                            <span className="relative z-10">{option.label}</span>
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
                      Feed Filter
                    </motion.button>

                    <div className="flex items-center gap-1 rounded-[14px] border border-black/6 bg-white/58 p-1 shadow-[0_4px_12px_rgba(0,0,0,0.06),inset_0_1px_0_rgba(255,255,255,0.75)] dark:border-white/8 dark:bg-white/[0.05] dark:shadow-[0_8px_18px_rgba(0,0,0,0.35),inset_0_1px_0_rgba(255,255,255,0.06)]">
                      {FIRE_SORT_OPTIONS.map((option) => {
                        const isActive = filters.sort === option.value;
                        return (
                          <motion.button
                            key={option.value}
                            type="button"
                            whileTap={{ scale: 0.95 }}
                            onClick={() => updateFilters((current) => ({ ...current, sort: option.value }))}
                            className={cn(
                              'relative overflow-hidden rounded-[11px] px-2.5 py-1.5 text-[9px] font-black uppercase tracking-[0.16em] transition-colors duration-200',
                              isActive
                                ? 'text-white'
                                : 'text-black/55 dark:text-white/45',
                            )}
                          >
                            {isActive && (
                              <motion.span
                                layoutId="fire-sort-pill-bg"
                                className="absolute inset-0 rounded-[11px] bg-[#E11D48] shadow-[0_6px_14px_rgba(225,29,72,0.22),inset_0_1px_0_rgba(255,255,255,0.75)] dark:shadow-[0_8px_20px_rgba(225,29,72,0.22),0_10px_24px_rgba(0,0,0,0.26)]"
                                transition={FIRE_CHROME_PILL_SPRING}
                              />
                            )}
                            <span className="relative z-10">{option.label}</span>
                          </motion.button>
                        );
                      })}
                    </div>

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
                        onClick={() => updateFilters((current) => ({ ...current, selectedCheckpoints: [] }))}
                        className={cn(
                          'rounded-[12px] px-2.5 py-1.5 text-[9px] font-black uppercase tracking-[0.16em] transition-colors duration-200',
                          filters.selectedCheckpoints.length === 0
                            ? 'bg-black text-white shadow-[0_6px_18px_rgba(0,0,0,0.18)] dark:bg-[#E11D48] dark:text-white dark:shadow-[0_8px_20px_rgba(225,29,72,0.22)]'
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
                              updateFilters((current) => ({
                                ...current,
                                selectedCheckpoints: current.selectedCheckpoints.includes(checkpoint)
                                  ? current.selectedCheckpoints.filter((value) => value !== checkpoint)
                                  : [...current.selectedCheckpoints, checkpoint],
                              }));
                            }}
                            className={cn(
                              'rounded-[12px] px-2.5 py-1.5 text-[9px] font-black uppercase tracking-[0.16em] transition-colors duration-200',
                              isSelected
                                ? 'bg-black text-white shadow-[0_6px_18px_rgba(0,0,0,0.18)] dark:bg-[#E11D48] dark:text-white dark:shadow-[0_8px_20px_rgba(225,29,72,0.22)]'
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
      </motion.div>

      <div ref={contentRef} className={cn(useBrowserPageScroll ? 'w-full' : 'h-full w-full')}>
        {loading ? (
          <div className="flex w-full items-center justify-center" style={fireStateShellStyle()}>
            <Loader2 className="h-12 w-12 animate-spin text-[#E11D48]" />
          </div>
        ) : error ? (
          <div className="flex w-full items-center justify-center px-6 text-center" style={fireStateShellStyle()}>
            <div className="rounded-2xl border border-red-400/40 bg-red-500/10 px-6 py-5 text-sm font-semibold tracking-wide text-red-600 dark:text-red-300">
              FIRE DATA UNAVAILABLE: {error}
            </div>
          </div>
        ) : (
          <div className={cn('mx-auto w-full', useBrowserPageScroll ? 'min-h-[100dvh]' : 'h-full')}>
            <div className={useBrowserPageScroll ? 'min-h-[100dvh]' : 'h-full'}>
              {displayCards.length === 0 ? (
                <div className="flex w-full items-center justify-center px-6 text-center" style={fireStateShellStyle()}>
                  <div className="rounded-2xl border border-white/30 bg-white/20 px-6 py-4 text-xs font-black uppercase tracking-[0.2em] text-foreground/70 dark:border-white/14 dark:bg-black/24">
                    {isRefreshing ? 'Updating fire view' : 'No alerts for this selection'}
                  </div>
                </div>
              ) : (
                <FluidDeck
                  cards={displayCards}
                  hasMore={hasMore}
                  loadingMore={loadingMore}
                  onLoadMore={handleLoadMore}
                  onOpenCard={setDesktopModalCard}
                  usePageScroll={useBrowserPageScroll}
                  resetKey={deckResetKey}
                  total={total}
                />
              )}
            </div>
          </div>
        )}
      </div>

      <FireIntelligenceDialog
        item={desktopModalCard}
        onClose={() => setDesktopModalCard(null)}
        onPrevious={openPreviousDesktopCard}
        onNext={openNextDesktopCard}
        canPrevious={desktopModalIndex > 0}
        canNext={desktopModalIndex >= 0 && desktopModalIndex < displayCards.length - 1}
      />

      {process.env.NODE_ENV !== 'production' && (
        <div className="pointer-events-none absolute bottom-3 left-3 z-[140] rounded-lg border border-white/30 bg-black/55 px-2.5 py-2 text-[10px] font-mono leading-tight text-[#E11D48] dark:border-white/20">
          <div>cards: {cards.length} / {total}</div>
          <div>day: {selectedDay || '--'}</div>
          <div>filters: {serializeFilters(filters)}</div>
        </div>
      )}
    </motion.div>
  );
}
