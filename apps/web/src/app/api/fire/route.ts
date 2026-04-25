import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';
import { createClient as createServerClient } from '@/lib/supabase/server';
import { getPatternCueLabel, getPatternMechanicLabel } from '@/lib/fireSignals';
import { privateJsonResponse } from '@/lib/privateJsonResponse';
import { withServerRouteCache } from '@/lib/serverRouteCache';

export const dynamic = 'force-dynamic';

const PAGE_SIZE = 30;
const CHECKPOINT_ORDER = ['D1', 'D3', 'D7', 'D21'];
const TRACKING_CHECKPOINTS = ['d1', 'd3', 'd7', 'd21'] as const;
const DEFAULT_TRACKING_CHECKPOINTS = ['d1', 'd3', 'd7'] as const;
const POSTS_PAGE_SIZE = 1000;
const POST_KEY_CHUNK_SIZE = 250;
const WARMUP_META_PAGE_SIZE = 1000;
const WARMUP_METRIC_CHUNK_SIZE = 250;
const TRACKING_SIGNAL_CODE = 'TRACKING_BASE';
const HOT_PERCENTILE_MAX = 35;
const PREVIEW_CAPTURE_START_DAY = (process.env.FIRE_PREVIEW_START_DAY || '2026-04-14').trim();
const FIRE_BOOTSTRAP_DAY_COUNT = 7;
const FIRE_DEFAULT_BOOTSTRAP_PAGE_SIZE = 20;
const FIRE_POST_LOOKBACK_DAYS = 35;
const FIRE_ACTIVE_STATE_TTL_MS = 2 * 60 * 1000;
const FIRE_META_TTL_MS = 10 * 60 * 1000;
const FIRE_PAGE_TTL_MS = 5 * 60 * 1000;
const FIRE_BOOTSTRAP_PREFETCH_DAY_COUNT = 3;
const FIRE_MAX_BOOTSTRAP_PREFETCH_DAY_COUNT = 3;
type TrackingCheckpoint = (typeof TRACKING_CHECKPOINTS)[number];
type DefaultTrackingCheckpoint = (typeof DEFAULT_TRACKING_CHECKPOINTS)[number];

function toIstDayKey(date: Date): string {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Kolkata',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
}

function shiftIstDayKey(base: Date, offsetDays: number): string {
  const [year, month, day] = toIstDayKey(base).split('-').map((part) => parseInt(part, 10));
  const utc = new Date(Date.UTC(year, month - 1, day));
  utc.setUTCDate(utc.getUTCDate() + offsetDays);
  return utc.toISOString().slice(0, 10);
}

function todayIstDayKey(): string {
  return shiftIstDayKey(new Date(), 0);
}

function buildRecentDayKeys(count: number): string[] {
  const keys: string[] = [];
  const now = new Date();
  for (let i = 0; i < count; i++) {
    keys.push(shiftIstDayKey(now, -i));
  }
  return keys;
}

function shiftDayKey(dayKey: string, offsetDays: number): string {
  const [year, month, day] = String(dayKey || '').split('-').map((part) => parseInt(part, 10));
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
    return todayIstDayKey();
  }
  const utc = new Date(Date.UTC(year, month - 1, day));
  utc.setUTCDate(utc.getUTCDate() + offsetDays);
  return utc.toISOString().slice(0, 10);
}

function istDayStartUtcIso(dayKey: string): string {
  const safeDayKey = /^\d{4}-\d{2}-\d{2}$/.test(dayKey) ? dayKey : todayIstDayKey();
  return new Date(`${safeDayKey}T00:00:00+05:30`).toISOString();
}

function firePostedAfterUtcIso(dayKey: string): string {
  return istDayStartUtcIso(shiftDayKey(dayKey, -FIRE_POST_LOOKBACK_DAYS));
}

function parseCsvNumbers(value: string | null): number[] {
  if (!value) return [];
  return Array.from(
    new Set(
      value
        .split(',')
        .map((part) => Number.parseInt(part.trim(), 10))
        .filter(Number.isFinite),
    ),
  );
}

function parseCsvStrings(value: string | null): string[] {
  if (!value) return [];
  return Array.from(
    new Set(
      value
        .split(',')
        .map((part) => part.trim().toUpperCase())
        .filter(Boolean),
    ),
  );
}

function serializeNumberList(values: number[]): string {
  return [...new Set(values)].sort((a, b) => a - b).join(',');
}

function serializeStringList(values: string[]): string {
  return [...new Set(values)].sort((a, b) => a.localeCompare(b)).join(',');
}

function normalizeCheckpoint(value: unknown): string {
  return typeof value === 'string' ? value.trim().toUpperCase() : '';
}

function sortCheckpoints(values: string[]): string[] {
  return [...values].sort((a, b) => {
    const ai = CHECKPOINT_ORDER.indexOf(a);
    const bi = CHECKPOINT_ORDER.indexOf(b);
    if (ai === -1 && bi === -1) return a.localeCompare(b);
    if (ai === -1) return 1;
    if (bi === -1) return -1;
    return ai - bi;
  });
}

function normalizeTrackingCheckpoint(value: unknown): TrackingCheckpoint | '' {
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return normalized === 'd1' || normalized === 'd3' || normalized === 'd7' || normalized === 'd21'
    ? normalized
    : '';
}

function resolveRequestedTrackingCheckpoints(values: string[]): DefaultTrackingCheckpoint[] {
  const allowed = new Set<DefaultTrackingCheckpoint>(DEFAULT_TRACKING_CHECKPOINTS);
  const normalized = values
    .map((value) => normalizeTrackingCheckpoint(value))
    .filter((value): value is DefaultTrackingCheckpoint => allowed.has(value as DefaultTrackingCheckpoint));

  return normalized.length > 0 ? Array.from(new Set(normalized)) : [...DEFAULT_TRACKING_CHECKPOINTS];
}

function parseIsoTime(value: unknown): number {
  if (typeof value !== 'string' || !value) return 0;
  const ts = Date.parse(value);
  return Number.isFinite(ts) ? ts : 0;
}

function percentileValue(row: Record<string, unknown>): number {
  const value = row.surface_percentile;
  return typeof value === 'number' && Number.isFinite(value) ? value : Number.POSITIVE_INFINITY;
}

function percentileExactValue(row: Record<string, unknown>): number {
  const value = row.surface_percentile_exact;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  return percentileValue(row);
}

function nullableNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function nullableString(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value : null;
}

function recordValue(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function peakWindowLabel(hour: number | null): string | null {
  if (hour == null || !Number.isFinite(hour)) return null;
  if (hour >= 5 && hour < 12) return 'Morning';
  if (hour >= 12 && hour < 17) return 'Afternoon';
  if (hour >= 17 && hour < 22) return 'Evening';
  return 'Late Night';
}

function normalizeWarmupMediaBucket(value: unknown): 'REEL' | 'CAROUSEL' | 'IMAGE' | null {
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
  if (!normalized) return null;
  if (normalized === 'reel' || normalized === 'video') return 'REEL';
  if (normalized === 'carousel' || normalized === 'sidecar') return 'CAROUSEL';
  if (normalized === 'image' || normalized === 'photo') return 'IMAGE';
  return null;
}

function buildWarmupSummaryKey(feederId: number, bucket: 'REEL' | 'CAROUSEL' | 'IMAGE'): string {
  return `${feederId}:${bucket}`;
}

function normalizeSurfaceMediaType(value: unknown): string {
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return normalized || 'unknown';
}

function buildBaselineKey(feederId: number, mediaType: string, checkpoint: string): string {
  return `${feederId}:${normalizeSurfaceMediaType(mediaType)}:${checkpoint.toLowerCase()}`;
}

function buildHourBaselineKey(feederId: number, mediaType: string, checkpoint: string, hour: number): string {
  return `${buildBaselineKey(feederId, mediaType, checkpoint)}:${hour}`;
}

function buildFireCardKey(postKey: string, checkpoint: string, businessDay: string): string {
  return `${postKey}:${checkpoint.toLowerCase()}:${businessDay}`;
}

function buildSyntheticAlertId(feedId: number, postKey: string, checkpoint: string, businessDay: string): string {
  return `tracking:${feedId}:${postKey}:${checkpoint.toLowerCase()}:${businessDay}`;
}

function alertDeduplicationKey(row: AlertSurfaceRow): string {
  const postKey = nullableString(row.post_key);
  if (postKey) return postKey;
  return nullableString(row.dedupe_key) || String(row.id);
}

function dedupeSortedAlertRows(rows: AlertSurfaceRow[]): AlertSurfaceRow[] {
  const deduped = new Map<string, AlertSurfaceRow>();
  for (const row of rows) {
    const key = alertDeduplicationKey(row);
    if (!deduped.has(key)) {
      deduped.set(key, row);
    }
  }
  return Array.from(deduped.values());
}

function publicMediaUrlFromPath(path: string | null | undefined): string | null {
  const base = (process.env.MEDIA_PUBLIC_BASE_URL || '').trim().replace(/\/$/, '');
  const cleanPath = typeof path === 'string' ? path.trim().replace(/^\/+/, '') : '';
  if (!base || !cleanPath) return null;
  return `${base}/${cleanPath.split('/').map(encodeURIComponent).join('/')}`;
}

function previewCaptureAllowedForBusinessDay(businessDay: string | null | undefined): boolean {
  const day = typeof businessDay === 'string' ? businessDay.trim() : '';
  if (!day || !PREVIEW_CAPTURE_START_DAY) return true;
  return day >= PREVIEW_CAPTURE_START_DAY;
}

function metricValueFromPostMetric(row: FirePostMetricRow, metric: FireMetricKey): number | null {
  if (metric === 'views') return nullableNumber(row.views);
  if (metric === 'likes') return nullableNumber(row.likes);
  return nullableNumber(row.comments);
}

function baselineValueFromRow(row: FireFeederBaselineRow | null | undefined, metric: FireMetricKey): number | null {
  if (!row) return null;
  if (metric === 'views') return nullableNumber(row.median_views);
  if (metric === 'likes') return nullableNumber(row.median_likes);
  return nullableNumber(row.median_comments);
}

function hourBaselineValueFromRow(row: FireFeederHourBaselineRow | null | undefined, metric: FireMetricKey): number | null {
  if (!row) return null;
  if (metric === 'views') return nullableNumber(row.median_views);
  if (metric === 'likes') return nullableNumber(row.median_likes);
  return nullableNumber(row.median_comments);
}

function computeMultiple(value: number | null, baseline: number | null): number | null {
  if (value == null || baseline == null || !Number.isFinite(value) || !Number.isFinite(baseline) || baseline <= 0) {
    return null;
  }
  return Math.round((value / baseline) * 10000) / 10000;
}

function patternAlertPriority(value: string | null | undefined): number {
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
  if (normalized === 'now' || normalized === 'blaze') return 0;
  if (normalized === 'today' || normalized === 'burn') return 1;
  if (normalized === 'watch' || normalized === 'spark') return 2;
  if (normalized === 'tracking') return 3;
  return 4;
}

function humanizeSignalCode(code: string | null | undefined): string {
  const normalized = typeof code === 'string' ? code.trim() : '';
  if (!normalized) return 'Pattern Alert';
  return normalized
    .replace(/^(OWN|CROSS|ANCHOR)_\d+_/, '')
    .replace(/_/g, ' ')
    .toLowerCase()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function isHotPercentile(value: number | null): boolean {
  return value != null && Number.isFinite(value) && value <= HOT_PERCENTILE_MAX;
}

function isMissingColumnError(error: unknown, columnName: string): boolean {
  return isMissingColumnReferenceError(error, 'fire_alerts', columnName);
}

function isMissingColumnReferenceError(error: unknown, tableName: string, columnName: string): boolean {
  const message = error instanceof Error
    ? error.message
    : typeof error === 'object' && error !== null && 'message' in error
      ? String((error as { message?: unknown }).message || '')
      : String(error || '');
  return message.toLowerCase().includes(`column ${tableName}.${columnName}`.toLowerCase());
}

function shouldFallbackToLegacyFireDayLoad(error: unknown): boolean {
  const message = error instanceof Error
    ? error.message
    : typeof error === 'object' && error !== null && 'message' in error
      ? String((error as { message?: unknown }).message || '')
      : String(error || '');
  const normalized = message.toLowerCase();
  return normalized.includes('relationship')
    || normalized.includes('not embedded')
    || normalized.includes('post.feeder_id')
    || normalized.includes('post_metrics');
}

type ActiveFeedRow = { id: number; name: string | null };
type ActiveFeederRow = { id: number; feed_id: number; handle: string | null; created_at: string | null };
type FireMetricCheckpointRow = { post_key: string | null; checkpoint: string | null };
type AlertSurfaceRow = {
  id: number | string;
  dedupe_key: string | null;
  feed_id: number;
  feeder_id: number;
  post_key: string | null;
  checkpoint: string;
  business_date_ist: string;
  card_kind?: 'tracking' | 'firewatch';
  signal_code: string | null;
  context: string | null;
  alert_type: string | null;
  status: string | null;
  metric_key: string | null;
  metric_value: number | string | null;
  surface_percentile: number | null;
  surface_percentile_exact?: number | null;
  surface_delta: number | null;
  feed_rank: number | null;
  feeder_rank: number | null;
  anchor_handle: string | null;
  anchor_best_pct: number | null;
  anchor_gap: number | null;
  body: string | null;
  created_at: string;
  updated_at: string;
  handle: string | null;
  media_type: string | null;
  posted_at: string | null;
  post_url: string | null;
  thumbnail_url: string | null;
  preview_url?: string | null;
  resolved_thumbnail_url?: string | null;
  resolved_preview_url?: string | null;
  views: number | null;
  likes: number | null;
  comments: number | null;
  views_baseline: number | null;
  likes_baseline: number | null;
  comments_baseline: number | null;
  views_multiple: number | null;
  likes_multiple: number | null;
  comments_multiple: number | null;
  hour_ist: number | null;
  hour_percentile: number | null;
  hour_multiple: number | null;
  best_in_last_n: number | null;
  trajectory_d1: number | null;
  trajectory_d3: number | null;
  trajectory_d7: number | null;
  trajectory_d21: number | null;
  intelligence_skipped: boolean | null;
  pattern_alerts?: FirePatternSummary[] | null;
  is_hot?: boolean | null;
  has_intelligence?: boolean | null;
  hide_signal_chrome?: boolean | null;
};

type WarmupPostRow = {
  post_key: string | null;
  feeder_id: number | string | null;
  media_type: string | null;
  posted_at: string | null;
};

type FireTrackedPostRow = {
  post_key: string | null;
  feeder_id: number | string | null;
  media_type: string | null;
  posted_at: string | null;
  post_url: string | null;
  thumbnail_url: string | null;
  video_url: string | null;
};

type FirePostMetricRow = {
  post_key: string | null;
  checkpoint: string | null;
  business_date_ist: string | null;
  computed_at: string | null;
  views: number | null;
  likes: number | null;
  comments: number | null;
  metric_value: number | string | null;
  percentile_performance: number | null;
  percentile_performance_exact?: number | null;
  ranking_metric?: string | null;
  ranking_multiple?: number | null;
  views_percentile: number | null;
  likes_percentile: number | null;
  comments_percentile: number | null;
  views_baseline?: number | null;
  likes_baseline?: number | null;
  comments_baseline?: number | null;
  views_multiple?: number | null;
  likes_multiple?: number | null;
  comments_multiple?: number | null;
  hour_multiple?: number | null;
  feed_percentile: number | null;
  delta_from_d1: number | null;
};

type FireDayMetricJoinedRow = FirePostMetricRow & {
  post?: {
    feeder_id: number | string | null;
    media_type: string | null;
    posted_at: string | null;
    post_url: string | null;
    thumbnail_url: string | null;
    video_url: string | null;
  } | null;
};

type FireFeederBaselineRow = {
  feeder_id: number | string | null;
  media_type: string | null;
  checkpoint: string | null;
  median_views: number | null;
  median_likes: number | null;
  median_comments: number | null;
};

type FireFeederHourBaselineRow = {
  feeder_id: number | string | null;
  media_type: string | null;
  checkpoint: string | null;
  hour_ist: number | null;
  median_views: number | null;
  median_likes: number | null;
  median_comments: number | null;
};

type FireIntelligenceRow = {
  post_key: string | null;
  model_version: string | null;
};

type FirePatternSupportPreview = {
  post_key: string;
  handle: string | null;
  media_type: string | null;
  post_url: string | null;
  thumbnail_url: string | null;
  posted_at: string | null;
};

type MediaAssetUrlRow = {
  post_key: string | null;
  asset_role: string | null;
  storage_provider: string | null;
  storage_path: string | null;
  public_url: string | null;
  mime_type: string | null;
  purge_after: string | null;
};

type ResolvedMediaUrls = {
  thumbnailUrls: Map<string, string>;
  previewUrls: Map<string, string>;
};

type FirePatternAlertRow = {
  id: number | string;
  dedupe_key: string | null;
  feed_id: number | string | null;
  feeder_id: number | string | null;
  post_key: string | null;
  checkpoint: string | null;
  business_date_ist: string | null;
  signal_code: string | null;
  context: string | null;
  alert_type: string | null;
  status: string | null;
  metric_key: string | null;
  metric_value: number | string | null;
  surface_percentile: number | null;
  surface_delta: number | null;
  body: string | null;
  signal_payload: Record<string, unknown> | null;
  created_at: string | null;
  updated_at: string | null;
};

type FirePatternSummary = {
  signal_code: string | null;
  context: string | null;
  alert_type: string | null;
  body: string | null;
  surface_percentile: number | null;
  created_at: string | null;
  pattern_name?: string | null;
  modifier_key?: string | null;
  modifier_value?: string | null;
  media_type?: string | null;
  pattern_key?: string | null;
  match_count?: number | null;
  feeders_count?: number | null;
  avg_hot_percentile?: number | null;
  baseline_share?: number | null;
  recent_lift?: number | null;
  contrast_gap?: number | null;
  anchor_avg_percentile?: number | null;
  anchor_gap?: number | null;
  cues?: Array<{ key: string; value: string; label: string }> | null;
  required_cues?: Array<{ key: string; value: string; label: string }> | null;
  support_posts?: FirePatternSupportPreview[] | null;
  anchor_support_posts?: FirePatternSupportPreview[] | null;
};

type FirePatternGroup = {
  primaryAlert: FirePatternSummary | null;
  summaries: FirePatternSummary[];
  summaryBody: string | null;
};

type FireMetricKey = 'views' | 'likes' | 'comments';

function metricPreferenceOrder(mediaType: string | null): FireMetricKey[] {
  const normalized = (mediaType || '').trim().toLowerCase();
  if (normalized === 'reel' || normalized === 'video') return ['views', 'likes', 'comments'];
  return ['likes', 'comments', 'views'];
}

function rowMetricValue(row: AlertSurfaceRow, metric: FireMetricKey): number | null {
  return metric === 'views' ? nullableNumber(row.views) : metric === 'likes' ? nullableNumber(row.likes) : nullableNumber(row.comments);
}

function rowMetricBaseline(row: AlertSurfaceRow, metric: FireMetricKey): number | null {
  return metric === 'views'
    ? nullableNumber(row.views_baseline)
    : metric === 'likes'
      ? nullableNumber(row.likes_baseline)
      : nullableNumber(row.comments_baseline);
}

function rowMetricMultiple(row: AlertSurfaceRow, metric: FireMetricKey): number | null {
  return metric === 'views'
    ? nullableNumber(row.views_multiple)
    : metric === 'likes'
      ? nullableNumber(row.likes_multiple)
      : nullableNumber(row.comments_multiple);
}

function deriveBestMetric(row: AlertSurfaceRow): FireMetricKey {
  const preferred = (nullableString(row.metric_key) || '').toLowerCase();
  const ordered = metricPreferenceOrder(row.media_type);
  if ((preferred === 'views' || preferred === 'likes' || preferred === 'comments') && rowMetricValue(row, preferred) != null) {
    return preferred;
  }
  let bestMetric: FireMetricKey | null = null;
  let bestMultiple: number | null = null;

  for (const metric of ordered) {
    const multiple = rowMetricMultiple(row, metric);
    if (multiple == null) continue;
    if (bestMultiple == null || multiple > bestMultiple) {
      bestMetric = metric;
      bestMultiple = multiple;
    }
  }

  if (bestMetric) return bestMetric;

  for (const metric of ordered) {
    if (rowMetricValue(row, metric) != null) return metric;
  }

  return ordered[0];
}

function buildMetricPayload(row: AlertSurfaceRow, metric: FireMetricKey, bestMetric: FireMetricKey) {
  const isBestMetric = metric === bestMetric;
  const value = rowMetricValue(row, metric);
  const baseline = rowMetricBaseline(row, metric);
  const multiple = rowMetricMultiple(row, metric);

  return {
    value,
    baseline,
    multiple,
    best_in_last_n: isBestMetric ? nullableNumber(row.best_in_last_n) : null,
  };
}

function serializeAlertRow(row: AlertSurfaceRow): Record<string, unknown> {
  const bestMetric = deriveBestMetric(row);
  const bestMetricValue = rowMetricValue(row, bestMetric);
  const primaryPattern = row.pattern_alerts?.[0] ?? null;
  const payload = {
    best_metric: bestMetric,
    metrics: {
      views: buildMetricPayload(row, 'views', bestMetric),
      likes: buildMetricPayload(row, 'likes', bestMetric),
      comments: buildMetricPayload(row, 'comments', bestMetric),
    },
    position: {
      overall_percentile: nullableNumber(row.surface_percentile_exact) ?? nullableNumber(row.surface_percentile),
      percentile: nullableNumber(row.surface_percentile),
      shift: nullableNumber(row.surface_delta),
      feed_rank: nullableNumber(row.feed_rank),
      feeder_rank: nullableNumber(row.feeder_rank),
      anchor_gap: nullableNumber(row.anchor_gap),
    },
    timing: {
      hour: nullableNumber(row.hour_ist),
      peak_window: peakWindowLabel(nullableNumber(row.hour_ist)),
      hour_percentile: nullableNumber(row.hour_percentile),
      hour_multiple: nullableNumber(row.hour_multiple),
    },
    trajectory: {
      d1: nullableNumber(row.trajectory_d1),
      d3: nullableNumber(row.trajectory_d3),
      d7: nullableNumber(row.trajectory_d7),
      d21: nullableNumber(row.trajectory_d21),
      delta: nullableNumber(row.surface_delta),
    },
    meta: {
      card_kind: row.card_kind || 'tracking',
      feed_name: nullableString(row.handle),
      handle: nullableString(row.handle),
      media_type: nullableString(row.media_type),
      checkpoint: nullableString(row.checkpoint),
      post_url: nullableString(row.post_url),
      thumbnail_url: nullableString(row.resolved_thumbnail_url),
      preview_url: nullableString(row.resolved_preview_url),
      resolved_thumbnail_url: nullableString(row.resolved_thumbnail_url),
      resolved_preview_url: nullableString(row.resolved_preview_url),
      business_date_ist: nullableString(row.business_date_ist),
      alert_type: nullableString(row.alert_type),
      signal_code: nullableString(row.signal_code),
      signal_context: nullableString(row.context),
      anchor_handle: nullableString(row.anchor_handle),
      pattern_alert_count: row.pattern_alerts?.length ?? 0,
      pattern_alerts: row.pattern_alerts ?? [],
      firewatch: primaryPattern ? {
        family_label: firewatchFamilyLabel(primaryPattern.context),
        pattern_label: getPatternMechanicLabel(primaryPattern.pattern_name) || humanizeSignalCode(primaryPattern.signal_code),
        media_type: primaryPattern.media_type ?? nullableString(row.media_type),
        support_posts: primaryPattern.support_posts ?? [],
        anchor_support_posts: primaryPattern.anchor_support_posts ?? [],
        required_cues: primaryPattern.required_cues ?? [],
        cues: primaryPattern.cues ?? [],
        match_count: primaryPattern.match_count ?? null,
        feeders_count: primaryPattern.feeders_count ?? null,
        avg_hot_percentile: primaryPattern.avg_hot_percentile ?? null,
        baseline_share: primaryPattern.baseline_share ?? null,
        recent_lift: primaryPattern.recent_lift ?? null,
        anchor_gap: primaryPattern.anchor_gap ?? null,
        anchor_avg_percentile: primaryPattern.anchor_avg_percentile ?? null,
        pattern_key: primaryPattern.pattern_key ?? null,
      } : null,
      is_hot: Boolean(row.is_hot),
      has_intelligence: Boolean(row.has_intelligence),
      hide_signal_chrome: Boolean(row.hide_signal_chrome),
    },
  };

  return {
    id: row.id,
    dedupe_key: row.dedupe_key,
    feed_id: row.feed_id,
    feeder_id: row.feeder_id,
    post_key: row.post_key,
    checkpoint: row.checkpoint,
    business_date_ist: row.business_date_ist,
    signal_code: row.signal_code,
    context: row.context,
    alert_type: row.alert_type,
    status: row.status,
    surface_percentile: nullableNumber(row.surface_percentile),
    surface_percentile_exact: nullableNumber(row.surface_percentile_exact),
    surface_delta: nullableNumber(row.surface_delta),
    metric_key: bestMetric,
    metric_value: bestMetricValue ?? nullableNumber(row.metric_value),
    body: row.body ?? '',
    created_at: row.created_at,
    updated_at: row.updated_at,
    posted_at: row.posted_at,
    thumbnail_url: nullableString(row.resolved_thumbnail_url),
    preview_url: nullableString(row.resolved_preview_url),
    surface_handle: row.handle,
    surface_media_type: row.media_type,
    surface_checkpoint: row.checkpoint,
    intelligence_skipped: Boolean(row.intelligence_skipped),
    payload,
  };
}

function sortPatternAlertRows(rows: FirePatternAlertRow[]): FirePatternAlertRow[] {
  return [...rows].sort((a, b) => {
    const aPriority = patternAlertPriority(a.alert_type);
    const bPriority = patternAlertPriority(b.alert_type);
    if (aPriority !== bPriority) return aPriority - bPriority;

    const aPercentile = nullableNumber(a.surface_percentile) ?? Number.POSITIVE_INFINITY;
    const bPercentile = nullableNumber(b.surface_percentile) ?? Number.POSITIVE_INFINITY;
    if (aPercentile !== bPercentile) return aPercentile - bPercentile;

    return parseIsoTime(nullableString(b.created_at)) - parseIsoTime(nullableString(a.created_at));
  });
}

function firewatchFamilyLabel(context: string | null | undefined): string {
  const normalized = typeof context === 'string' ? context.trim().toLowerCase() : '';
  if (normalized === 'cross') return 'Feed Pattern';
  if (normalized === 'anchor') return 'Anchor Gap';
  return 'Repeating Winner';
}

function summarizePatternAlertRow(
  row: FirePatternAlertRow,
  supportPreviewByKey: Map<string, FirePatternSupportPreview>,
): FirePatternSummary {
  const payload = recordValue(row.signal_payload);
  const mapCueList = (value: unknown) => (
    Array.isArray(value)
      ? value
        .map((cue) => recordValue(cue))
        .map((cue) => {
          const key = nullableString(cue.key);
          const cueValue = nullableString(cue.value);
          const label = getPatternCueLabel(key, cueValue);
          if (!key || !cueValue || !label) return null;
          return { key, value: cueValue, label };
        })
        .filter((cue): cue is { key: string; value: string; label: string } => cue !== null)
      : []
  );
  const resolvePreviews = (value: unknown) => (
    Array.isArray(value)
      ? value
        .map((entry) => nullableString(entry))
        .filter((entry): entry is string => Boolean(entry))
        .map((postKey) => supportPreviewByKey.get(postKey) || null)
        .filter((preview): preview is FirePatternSupportPreview => preview !== null)
      : []
  );

  return {
    signal_code: nullableString(row.signal_code),
    context: nullableString(row.context),
    alert_type: nullableString(row.alert_type),
    body: nullableString(row.body),
    surface_percentile: nullableNumber(row.surface_percentile),
    created_at: nullableString(row.created_at),
    pattern_name: nullableString(payload.pattern_name),
    modifier_key: nullableString(payload.modifier_key),
    modifier_value: nullableString(payload.modifier_value),
    media_type: nullableString(payload.media_type),
    pattern_key: nullableString(payload.pattern_key),
    match_count: nullableNumber(payload.match_count),
    feeders_count: nullableNumber(payload.feeders_count),
    avg_hot_percentile: nullableNumber(payload.avg_hot_percentile),
    baseline_share: nullableNumber(payload.baseline_share),
    recent_lift: nullableNumber(payload.recent_lift),
    contrast_gap: nullableNumber(payload.contrast_gap),
    anchor_avg_percentile: nullableNumber(payload.anchor_avg_percentile),
    anchor_gap: nullableNumber(payload.anchor_gap),
    cues: mapCueList(payload.cues),
    required_cues: mapCueList(payload.required_cues),
    support_posts: resolvePreviews(payload.support_post_keys),
    anchor_support_posts: resolvePreviews(payload.anchor_support_post_keys),
  } satisfies FirePatternSummary;
}

function buildPatternGroup(
  rows: FirePatternAlertRow[],
  supportPreviewByKey: Map<string, FirePatternSupportPreview>,
): FirePatternGroup {
  if (rows.length === 0) {
    return { primaryAlert: null, summaries: [], summaryBody: null };
  }

  const deduped = new Map<string, FirePatternAlertRow>();
  for (const row of sortPatternAlertRows(rows)) {
    const payload = recordValue(row.signal_payload);
    const signatureKey = `${nullableString(payload.mechanic)}:${nullableString(payload.modifier_key)}:${nullableString(payload.modifier_value)}`;
    const key = `${nullableString(row.signal_code) || 'UNKNOWN'}:${nullableString(row.context) || 'own'}:${signatureKey}`;
    if (!deduped.has(key)) deduped.set(key, row);
  }

  const sorted = sortPatternAlertRows(Array.from(deduped.values()));
  const summaries = sorted.map((row) => summarizePatternAlertRow(row, supportPreviewByKey));
  const primaryAlert = summaries[0] ?? null;

  if (!primaryAlert) {
    return { primaryAlert: null, summaries, summaryBody: null };
  }

  const primaryPatternLabel = getPatternMechanicLabel(primaryAlert.pattern_name) || humanizeSignalCode(primaryAlert.signal_code);
  const statBits = [
    primaryAlert.match_count != null ? `${Math.round(primaryAlert.match_count)} hot posts` : null,
    primaryAlert.feeders_count != null && primaryAlert.feeders_count > 1 ? `${Math.round(primaryAlert.feeders_count)} feeders` : null,
    primaryAlert.avg_hot_percentile != null ? `avg top ${Math.round(primaryAlert.avg_hot_percentile)}%` : null,
    primaryAlert.anchor_gap != null ? `gap +${Math.round(primaryAlert.anchor_gap)} pts` : null,
  ].filter(Boolean);

  if (summaries.length === 1) {
    return {
      primaryAlert,
      summaries,
      summaryBody: [primaryPatternLabel, ...statBits].filter(Boolean).join(' · '),
    };
  }

  const preview = summaries
    .map((summary) => getPatternMechanicLabel(summary.pattern_name) || humanizeSignalCode(summary.signal_code))
    .filter(Boolean)
    .slice(0, 2)
    .join(' · ');

  return {
    primaryAlert,
    summaries,
    summaryBody: `${summaries.length} patterns active · ${preview}`,
  };
}

async function fetchWarmupSummary(
  sb: { from: ReturnType<typeof createClient>['from'] },
  activeFeederIds: number[],
  feederCreatedAtById: Map<number, string | null>,
  preloadedPosts?: WarmupPostRow[],
): Promise<Record<string, number>> {
  if (activeFeederIds.length === 0) return {};

  const rows: WarmupPostRow[] = preloadedPosts ? [...preloadedPosts] : [];

  if (!preloadedPosts) {
    for (let start = 0; ; start += WARMUP_META_PAGE_SIZE) {
      const { data, error } = await sb
        .from('posts')
        .select('post_key,feeder_id,media_type,posted_at')
        .in('feeder_id', activeFeederIds)
        .order('feeder_id', { ascending: true })
        .order('post_key', { ascending: true })
        .range(start, start + WARMUP_META_PAGE_SIZE - 1);

      if (error) throw error;

      const batch = (data || []) as WarmupPostRow[];
      rows.push(...batch);
      if (batch.length < WARMUP_META_PAGE_SIZE) break;
    }
  }

  const eligibleRows: Array<{ postKey: string; feederId: number; bucket: 'REEL' | 'CAROUSEL' | 'IMAGE' }> = [];
  const eligiblePostKeys: string[] = [];
  const seenEligiblePostKeys = new Set<string>();
  for (const row of rows) {
    const postKey = typeof row.post_key === 'string' ? row.post_key.trim() : '';
    const feederId = Number(row.feeder_id);
    if (!postKey || !Number.isFinite(feederId)) continue;

    const bucket = normalizeWarmupMediaBucket(row.media_type);
    if (!bucket) continue;

    const feederCreatedAtTs = parseIsoTime(feederCreatedAtById.get(feederId));
    const postedAtTs = parseIsoTime(row.posted_at);
    if (feederCreatedAtTs <= 0 || postedAtTs <= 0 || postedAtTs < feederCreatedAtTs) {
      continue;
    }

    if (seenEligiblePostKeys.has(postKey)) continue;
    seenEligiblePostKeys.add(postKey);
    eligibleRows.push({ postKey, feederId, bucket });
    eligiblePostKeys.push(postKey);
  }

  if (eligiblePostKeys.length === 0) return {};

  const d1PostKeys = new Set<string>();
  for (let start = 0; start < eligiblePostKeys.length; start += WARMUP_METRIC_CHUNK_SIZE) {
    const chunk = eligiblePostKeys.slice(start, start + WARMUP_METRIC_CHUNK_SIZE);
    const { data, error } = await sb
      .from('post_metrics')
      .select('post_key,checkpoint')
      .in('post_key', chunk)
      .in('checkpoint', ['d1', 'D1']);

    if (error) throw error;

    for (const row of (data || []) as FireMetricCheckpointRow[]) {
      const postKey = typeof row.post_key === 'string' ? row.post_key.trim() : '';
      if (postKey) d1PostKeys.add(postKey);
    }
  }

  const counts: Record<string, number> = {};
  for (const row of eligibleRows) {
    if (!d1PostKeys.has(row.postKey)) continue;
    const summaryKey = buildWarmupSummaryKey(row.feederId, row.bucket);
    counts[summaryKey] = (counts[summaryKey] ?? 0) + 1;
  }

  return counts;
}

async function fetchTrackedPosts(
  sb: { from: ReturnType<typeof createClient>['from'] },
  feederIds: number[],
  options?: {
    postedAfter?: string | null;
  },
): Promise<FireTrackedPostRow[]> {
  if (feederIds.length === 0) return [];

  const rows: FireTrackedPostRow[] = [];
  for (let start = 0; ; start += POSTS_PAGE_SIZE) {
    let query = sb
      .from('posts')
      .select('post_key,feeder_id,media_type,posted_at,post_url,thumbnail_url,video_url')
      .in('feeder_id', feederIds)
      .order('feeder_id', { ascending: true })
      .order('post_key', { ascending: true })
      .range(start, start + POSTS_PAGE_SIZE - 1);

    const postedAfter = typeof options?.postedAfter === 'string' ? options.postedAfter.trim() : '';
    if (postedAfter) {
      query = query.gte('posted_at', postedAfter);
    }

    const { data, error } = await query;

    if (error) throw error;

    const batch = (data || []) as FireTrackedPostRow[];
    rows.push(...batch);
    if (batch.length < POSTS_PAGE_SIZE) break;
  }

  return rows;
}

async function fetchCurrentDayTrackingRows(
  sb: { from: ReturnType<typeof createClient>['from'] },
  feederIds: number[],
  day: string,
  checkpoints: string[],
): Promise<{ posts: FireTrackedPostRow[]; metricRows: FirePostMetricRow[] }> {
  if (feederIds.length === 0 || checkpoints.length === 0) {
    return { posts: [], metricRows: [] };
  }

  const canonicalFields = [
    'post_key',
    'checkpoint',
    'business_date_ist',
    'computed_at',
    'views',
    'likes',
    'comments',
    'metric_value',
    'percentile_performance',
    'percentile_performance_exact',
    'ranking_metric',
    'ranking_multiple',
    'views_percentile',
    'likes_percentile',
    'comments_percentile',
    'views_baseline',
    'likes_baseline',
    'comments_baseline',
    'views_multiple',
    'likes_multiple',
    'comments_multiple',
    'hour_multiple',
    'feed_percentile',
    'delta_from_d1',
    'post:posts!inner(feeder_id,media_type,posted_at,post_url,thumbnail_url,video_url)',
  ].join(',');
  const legacyFields = [
    'post_key',
    'checkpoint',
    'business_date_ist',
    'computed_at',
    'views',
    'likes',
    'comments',
    'metric_value',
    'percentile_performance',
    'views_percentile',
    'likes_percentile',
    'comments_percentile',
    'feed_percentile',
    'delta_from_d1',
    'post:posts!inner(feeder_id,media_type,posted_at,post_url,thumbnail_url,video_url)',
  ].join(',');

  let data: FireDayMetricJoinedRow[] | null = null;
  let error: unknown = null;
  try {
    const canonicalResult = await sb
      .from('post_metrics')
      .select(canonicalFields)
      .eq('business_date_ist', day)
      .in('checkpoint', checkpoints)
      .in('post.feeder_id', feederIds);

    data = (canonicalResult.data || []) as FireDayMetricJoinedRow[];
    error = canonicalResult.error;

    if (
      error &&
      (
        isMissingColumnReferenceError(error, 'post_metrics', 'percentile_performance_exact') ||
        isMissingColumnReferenceError(error, 'post_metrics', 'ranking_metric') ||
        isMissingColumnReferenceError(error, 'post_metrics', 'views_baseline')
      )
    ) {
      const legacyResult = await sb
        .from('post_metrics')
        .select(legacyFields)
        .eq('business_date_ist', day)
        .in('checkpoint', checkpoints)
        .in('post.feeder_id', feederIds);
      data = (legacyResult.data || []) as FireDayMetricJoinedRow[];
      error = legacyResult.error;
    }
  } catch (caughtError) {
    error = caughtError;
  }

  if (error) {
    if (!shouldFallbackToLegacyFireDayLoad(error)) throw error;

    const posts = await fetchTrackedPosts(sb, feederIds, {
      postedAfter: firePostedAfterUtcIso(day),
    });
    const postKeys = Array.from(
      new Set(
        posts
          .map((row) => (typeof row.post_key === 'string' ? row.post_key.trim() : ''))
          .filter(Boolean),
      ),
    );
    const metricRows = await fetchMetricRowsForPostKeys(sb, postKeys, {
      businessDay: day,
      checkpoints,
    });
    return { posts, metricRows };
  }

  const posts: FireTrackedPostRow[] = [];
  const metricRows: FirePostMetricRow[] = [];
  const seenPosts = new Set<string>();
  for (const row of data || []) {
    const postKey = typeof row.post_key === 'string' ? row.post_key.trim() : '';
    if (!postKey) continue;

    metricRows.push({
      post_key: postKey,
      checkpoint: row.checkpoint ?? null,
      business_date_ist: row.business_date_ist ?? null,
      computed_at: row.computed_at ?? null,
      views: row.views ?? null,
      likes: row.likes ?? null,
      comments: row.comments ?? null,
      metric_value: row.metric_value ?? null,
      percentile_performance: row.percentile_performance ?? null,
      percentile_performance_exact: row.percentile_performance_exact ?? null,
      ranking_metric: row.ranking_metric ?? null,
      ranking_multiple: row.ranking_multiple ?? null,
      views_percentile: row.views_percentile ?? null,
      likes_percentile: row.likes_percentile ?? null,
      comments_percentile: row.comments_percentile ?? null,
      views_baseline: row.views_baseline ?? null,
      likes_baseline: row.likes_baseline ?? null,
      comments_baseline: row.comments_baseline ?? null,
      views_multiple: row.views_multiple ?? null,
      likes_multiple: row.likes_multiple ?? null,
      comments_multiple: row.comments_multiple ?? null,
      hour_multiple: row.hour_multiple ?? null,
      feed_percentile: row.feed_percentile ?? null,
      delta_from_d1: row.delta_from_d1 ?? null,
    });

    if (seenPosts.has(postKey)) continue;
    seenPosts.add(postKey);
    posts.push({
      post_key: postKey,
      feeder_id: row.post?.feeder_id ?? null,
      media_type: row.post?.media_type ?? null,
      posted_at: row.post?.posted_at ?? null,
      post_url: row.post?.post_url ?? null,
      thumbnail_url: row.post?.thumbnail_url ?? null,
      video_url: row.post?.video_url ?? null,
    });
  }

  return { posts, metricRows };
}

function directMediaUrl(row: MediaAssetUrlRow): string | null {
  const publicUrl = nullableString(row.public_url);
  if (publicUrl) return publicUrl;
  return row.storage_provider === 'r2' ? publicMediaUrlFromPath(row.storage_path) : null;
}

async function fetchStoredMediaUrls(
  sb: { from: ReturnType<typeof createClient>['from'] },
  postKeys: string[],
): Promise<ResolvedMediaUrls> {
  const thumbnailUrls = new Map<string, string>();
  const previewUrls = new Map<string, string>();
  const uniquePostKeys = Array.from(new Set(postKeys.map((key) => key.trim()).filter(Boolean)));
  if (uniquePostKeys.length === 0) {
    return { thumbnailUrls, previewUrls };
  }

  const rolePriority = new Map([
    ['thumbnail', 0],
    ['display', 1],
    ['carousel_0', 2],
  ]);
  const chosenThumbnailPriority = new Map<string, number>();

  for (let start = 0; start < uniquePostKeys.length; start += POST_KEY_CHUNK_SIZE) {
    const chunk = uniquePostKeys.slice(start, start + POST_KEY_CHUNK_SIZE);
    const { data, error } = await sb
      .from('post_media_assets')
      .select('post_key,asset_role,storage_provider,storage_path,public_url,mime_type,purge_after')
      .in('post_key', chunk)
      .in('asset_role', ['thumbnail', 'display', 'carousel_0', 'preview_5s'])
      .in('status', ['active', 'purge_pending']);

    if (error) throw error;

    for (const row of (data || []) as MediaAssetUrlRow[]) {
      const postKey = nullableString(row.post_key);
      const role = nullableString(row.asset_role);
      if (!postKey || !role) continue;

      const purgeAfter = row.purge_after ? Date.parse(row.purge_after) : Number.POSITIVE_INFINITY;
      if (Number.isFinite(purgeAfter) && purgeAfter <= Date.now()) continue;

      const mimeType = nullableString(row.mime_type)?.toLowerCase() || '';
      const url = directMediaUrl(row);
      if (!url) continue;

      if (role === 'preview_5s') {
        if (mimeType && !mimeType.startsWith('video/')) continue;
        previewUrls.set(postKey, url);
        continue;
      }

      if (mimeType && !mimeType.startsWith('image/')) continue;
      const priority = rolePriority.get(role) ?? Number.POSITIVE_INFINITY;
      const currentPriority = chosenThumbnailPriority.get(postKey) ?? Number.POSITIVE_INFINITY;
      if (priority < currentPriority) {
        thumbnailUrls.set(postKey, url);
        chosenThumbnailPriority.set(postKey, priority);
      }
    }
  }

  return { thumbnailUrls, previewUrls };
}

async function fetchMetricRowsForPostKeys(
  sb: { from: ReturnType<typeof createClient>['from'] },
  postKeys: string[],
  options: {
    businessDay?: string;
    checkpoints: string[];
  },
): Promise<FirePostMetricRow[]> {
  if (postKeys.length === 0 || options.checkpoints.length === 0) return [];

  const canonicalFields = [
    'post_key',
    'checkpoint',
    'business_date_ist',
    'computed_at',
    'views',
    'likes',
    'comments',
    'metric_value',
    'percentile_performance',
    'percentile_performance_exact',
    'ranking_metric',
    'ranking_multiple',
    'views_percentile',
    'likes_percentile',
    'comments_percentile',
    'views_baseline',
    'likes_baseline',
    'comments_baseline',
    'views_multiple',
    'likes_multiple',
    'comments_multiple',
    'hour_multiple',
    'feed_percentile',
    'delta_from_d1',
  ].join(',');
  const legacyFields = [
    'post_key',
    'checkpoint',
    'business_date_ist',
    'computed_at',
    'views',
    'likes',
    'comments',
    'metric_value',
    'percentile_performance',
    'views_percentile',
    'likes_percentile',
    'comments_percentile',
    'feed_percentile',
    'delta_from_d1',
  ].join(',');
  const rows: FirePostMetricRow[] = [];
  for (let start = 0; start < postKeys.length; start += POST_KEY_CHUNK_SIZE) {
    const chunk = postKeys.slice(start, start + POST_KEY_CHUNK_SIZE);
    const fetchChunk = async (selectedFields: string) => {
      let query = sb
        .from('post_metrics')
        .select(selectedFields)
        .in('post_key', chunk)
        .in('checkpoint', options.checkpoints);

      if (options.businessDay) {
        query = query.eq('business_date_ist', options.businessDay);
      }
      return query;
    };

    let { data, error } = await fetchChunk(canonicalFields);
    if (
      error &&
      (
        isMissingColumnReferenceError(error, 'post_metrics', 'percentile_performance_exact') ||
        isMissingColumnReferenceError(error, 'post_metrics', 'ranking_metric') ||
        isMissingColumnReferenceError(error, 'post_metrics', 'views_baseline')
      )
    ) {
      const legacyResult = await fetchChunk(legacyFields);
      data = legacyResult.data;
      error = legacyResult.error;
    }

    if (error) throw error;
    rows.push(...((data || []) as FirePostMetricRow[]));
  }

  return rows;
}

async function fetchIntelligenceRowsForPostKeys(
  sb: { from: ReturnType<typeof createClient>['from'] },
  postKeys: string[],
): Promise<FireIntelligenceRow[]> {
  if (postKeys.length === 0) return [];

  const rows: FireIntelligenceRow[] = [];
  for (let start = 0; start < postKeys.length; start += POST_KEY_CHUNK_SIZE) {
    const chunk = postKeys.slice(start, start + POST_KEY_CHUNK_SIZE);
    const { data, error } = await sb
      .from('post_intelligence')
      .select('post_key,model_version')
      .in('post_key', chunk);

    if (error) throw error;
    rows.push(...((data || []) as FireIntelligenceRow[]));
  }

  return rows;
}

async function fetchFeederBaselineRows(
  sb: { from: ReturnType<typeof createClient>['from'] },
  feederIds: number[],
  checkpoints: string[],
): Promise<FireFeederBaselineRow[]> {
  if (feederIds.length === 0 || checkpoints.length === 0) return [];
  const { data, error } = await sb
    .from('feeder_baselines')
    .select('feeder_id,media_type,checkpoint,median_views,median_likes,median_comments')
    .in('feeder_id', feederIds)
    .in('checkpoint', checkpoints);

  if (error) throw error;
  return (data || []) as FireFeederBaselineRow[];
}

async function fetchFeederHourBaselineRows(
  sb: { from: ReturnType<typeof createClient>['from'] },
  feederIds: number[],
  checkpoints: string[],
): Promise<FireFeederHourBaselineRow[]> {
  if (feederIds.length === 0 || checkpoints.length === 0) return [];
  const { data, error } = await sb
    .from('feeder_hour_baselines')
    .select('feeder_id,media_type,checkpoint,hour_ist,median_views,median_likes,median_comments')
    .in('feeder_id', feederIds)
    .in('checkpoint', checkpoints);

  if (error) throw error;
  return (data || []) as FireFeederHourBaselineRow[];
}

async function fetchPatternRowsForPostKeys(
  sb: { from: ReturnType<typeof createClient>['from'] },
  effectiveFeedIds: number[],
  effectiveFeederIds: number[],
  day: string,
  postKeys: string[],
): Promise<FirePatternAlertRow[]> {
  if (effectiveFeedIds.length === 0 || effectiveFeederIds.length === 0 || postKeys.length === 0) return [];

  const rows: FirePatternAlertRow[] = [];
  const baseFields = [
    'id',
    'dedupe_key',
    'feed_id',
    'feeder_id',
    'post_key',
    'checkpoint',
    'business_date_ist',
    'signal_code',
    'context',
    'alert_type',
    'status',
    'metric_key',
    'metric_value',
    'surface_percentile',
    'surface_delta',
    'body',
    'created_at',
    'updated_at',
  ];

  const fetchChunk = async (chunk: string[], payloadField: string) => sb
    .from('fire_alerts')
    .select([...baseFields, payloadField].join(','))
    .in('feed_id', effectiveFeedIds)
    .in('feeder_id', effectiveFeederIds)
    .eq('business_date_ist', day)
    .in('post_key', chunk)
    .in('signal_code', ['OWN_PATTERN', 'CROSS_PATTERN', 'ANCHOR_PATTERN'])
    .not('status', 'in', '("dropped","error","archived")');

  for (let start = 0; start < postKeys.length; start += POST_KEY_CHUNK_SIZE) {
    const chunk = postKeys.slice(start, start + POST_KEY_CHUNK_SIZE);
    let { data, error } = await fetchChunk(chunk, 'signal_payload');

    if (error && isMissingColumnError(error, 'signal_payload')) {
      const fallback = await fetchChunk(chunk, 'signal_payload:pattern_payload');
      data = fallback.data;
      error = fallback.error;
    }

    if (error && isMissingColumnError(error, 'pattern_payload')) {
      console.warn('[/api/fire] Pattern payload column unavailable; continuing without pattern alerts.');
      continue;
    }

    if (error) throw error;
    rows.push(...((data || []) as FirePatternAlertRow[]));
  }

  return rows;
}

async function fetchPatternSupportPreviews(
  sb: { from: ReturnType<typeof createClient>['from'] },
  postKeys: string[],
): Promise<Map<string, FirePatternSupportPreview>> {
  const previews = new Map<string, FirePatternSupportPreview>();
  if (postKeys.length === 0) return previews;

  const feederIds = new Set<number>();
  const rows: Array<FireTrackedPostRow & { feeder_id: number | string | null }> = [];
  for (let start = 0; start < postKeys.length; start += POST_KEY_CHUNK_SIZE) {
    const chunk = postKeys.slice(start, start + POST_KEY_CHUNK_SIZE);
    const { data, error } = await sb
      .from('posts')
      .select('post_key,feeder_id,media_type,posted_at,post_url,thumbnail_url')
      .in('post_key', chunk);

    if (error) throw error;
    const batch = (data || []) as Array<FireTrackedPostRow & { feeder_id: number | string | null }>;
    rows.push(...batch);
    for (const row of batch) {
      const feederId = Number(row.feeder_id);
      if (Number.isFinite(feederId)) feederIds.add(feederId);
    }
  }

  const feederHandleById = new Map<number, string | null>();
  if (feederIds.size > 0) {
    const { data, error } = await sb
      .from('feeders')
      .select('id,handle')
      .in('id', Array.from(feederIds));
    if (error) throw error;
    for (const row of (data || []) as Array<{ id: number | string | null; handle: string | null }>) {
      const feederId = Number(row.id);
      if (Number.isFinite(feederId)) feederHandleById.set(feederId, nullableString(row.handle));
    }
  }

  for (const row of rows) {
    const postKey = nullableString(row.post_key);
    const feederId = Number(row.feeder_id);
    if (!postKey || !Number.isFinite(feederId)) continue;
    previews.set(postKey, {
      post_key: postKey,
      handle: feederHandleById.get(feederId) || null,
      media_type: nullableString(row.media_type),
      post_url: nullableString(row.post_url),
      thumbnail_url: nullableString(row.thumbnail_url),
      posted_at: nullableString(row.posted_at),
    });
  }

  const { thumbnailUrls } = await fetchStoredMediaUrls(sb, rows.map((row) => nullableString(row.post_key)).filter((value): value is string => Boolean(value)));
  for (const [postKey, preview] of previews) {
    preview.thumbnail_url = thumbnailUrls.get(postKey) || preview.thumbnail_url || null;
  }

  return previews;
}

async function fetchFirewatchPatternRows(
  sb: { from: ReturnType<typeof createClient>['from'] },
  effectiveFeedIds: number[],
  effectiveFeederIds: number[],
  day: string,
): Promise<FirePatternAlertRow[]> {
  if (effectiveFeedIds.length === 0 || effectiveFeederIds.length === 0) return [];

  const baseFields = [
    'id',
    'dedupe_key',
    'feed_id',
    'feeder_id',
    'post_key',
    'checkpoint',
    'business_date_ist',
    'signal_code',
    'context',
    'alert_type',
    'status',
    'metric_key',
    'metric_value',
    'surface_percentile',
    'surface_delta',
    'body',
    'created_at',
    'updated_at',
  ];

  let { data, error } = await sb
    .from('fire_alerts')
    .select([...baseFields, 'signal_payload'].join(','))
    .in('feed_id', effectiveFeedIds)
    .in('feeder_id', effectiveFeederIds)
    .eq('business_date_ist', day)
    .eq('checkpoint', 'd7')
    .in('signal_code', ['OWN_PATTERN', 'CROSS_PATTERN', 'ANCHOR_PATTERN'])
    .not('status', 'in', '("dropped","error","archived")');

  if (error && isMissingColumnError(error, 'signal_payload')) {
    const fallback = await sb
      .from('fire_alerts')
      .select([...baseFields, 'signal_payload:pattern_payload'].join(','))
      .in('feed_id', effectiveFeedIds)
      .in('feeder_id', effectiveFeederIds)
      .eq('business_date_ist', day)
      .eq('checkpoint', 'd7')
      .in('signal_code', ['OWN_PATTERN', 'CROSS_PATTERN', 'ANCHOR_PATTERN'])
      .not('status', 'in', '("dropped","error","archived")');
    data = fallback.data;
    error = fallback.error;
  }

  if (error && isMissingColumnError(error, 'pattern_payload')) {
    console.warn('[/api/fire] Firewatch payload column unavailable; continuing without Firewatch cards.');
    return [];
  }
  if (error) throw error;

  return (data || []) as FirePatternAlertRow[];
}

function buildFirewatchPatternGroupKey(row: FirePatternAlertRow): string {
  const payload = recordValue(row.signal_payload);
  const patternKey = nullableString(payload.pattern_key);
  if (patternKey) {
    return [
      String(row.feed_id || ''),
      nullableString(row.business_date_ist) || '',
      nullableString(row.context) || 'own',
      patternKey,
    ].join(':');
  }

  const cues = Array.isArray(payload.required_cues) ? payload.required_cues : payload.cues;
  const cueSignature = Array.isArray(cues)
    ? cues
      .map((cue) => recordValue(cue))
      .map((cue) => `${nullableString(cue.key) || ''}:${nullableString(cue.value) || ''}`)
      .filter(Boolean)
      .slice(0, 2)
      .join('|')
    : '';

  return [
    String(row.feed_id || ''),
    nullableString(row.business_date_ist) || '',
    nullableString(row.context) || 'own',
    nullableString(payload.pattern_name) || '',
    nullableString(payload.media_type) || '',
    cueSignature,
  ].join(':');
}

function buildFirewatchRows(options: {
  patternRows: FirePatternAlertRow[];
  supportPreviewByKey: Map<string, FirePatternSupportPreview>;
  feedNamesById: Map<number, string>;
}): AlertSurfaceRow[] {
  if (options.patternRows.length === 0) return [];

  const grouped = new Map<string, FirePatternAlertRow[]>();
  for (const row of options.patternRows) {
    const key = buildFirewatchPatternGroupKey(row);
    const bucket = grouped.get(key) || [];
    bucket.push(row);
    grouped.set(key, bucket);
  }

  const rows: AlertSurfaceRow[] = [];

  for (const bucket of grouped.values()) {
    const sortedRows = sortPatternAlertRows(bucket);
    const primaryRow = sortedRows[0];
    if (!primaryRow) continue;

    const primarySummary = summarizePatternAlertRow(primaryRow, options.supportPreviewByKey);
    const feedId = Number(primaryRow.feed_id);
    const feederId = Number(primaryRow.feeder_id);
    const feedName = options.feedNamesById.get(feedId) || 'UNTITLED FEED';
    const heroPostKey = nullableString(primaryRow.post_key);
    const heroPreview = heroPostKey ? options.supportPreviewByKey.get(heroPostKey) || null : null;
    const supportPosts = new Map<string, FirePatternSupportPreview>();
    const anchorSupportPosts = new Map<string, FirePatternSupportPreview>();

    const collectPreview = (preview: FirePatternSupportPreview | null | undefined, bucketMap: Map<string, FirePatternSupportPreview>) => {
      if (!preview?.post_key) return;
      if (!bucketMap.has(preview.post_key)) bucketMap.set(preview.post_key, preview);
    };

    collectPreview(heroPreview, supportPosts);
    for (const row of sortedRows) {
      const summary = summarizePatternAlertRow(row, options.supportPreviewByKey);
      if (row.post_key && typeof row.post_key === 'string') {
        collectPreview(options.supportPreviewByKey.get(row.post_key), supportPosts);
      }
      for (const preview of summary.support_posts || []) collectPreview(preview, supportPosts);
      for (const preview of summary.anchor_support_posts || []) collectPreview(preview, anchorSupportPosts);
    }

    const allSupportPosts = Array.from(supportPosts.values()).slice(0, 6);
    const allAnchorSupportPosts = Array.from(anchorSupportPosts.values()).slice(0, 4);
    const primaryPatternLabel = getPatternMechanicLabel(primarySummary.pattern_name) || humanizeSignalCode(primarySummary.signal_code);
    const familyLabel = firewatchFamilyLabel(primarySummary.context);
    const statBits = [
      primarySummary.match_count != null ? `${Math.round(primarySummary.match_count)} hot` : null,
      primarySummary.feeders_count != null && primarySummary.feeders_count > 1 ? `${Math.round(primarySummary.feeders_count)} feeders` : null,
      primarySummary.anchor_gap != null ? `gap +${Math.round(primarySummary.anchor_gap)}` : null,
      primarySummary.recent_lift != null ? `${primarySummary.recent_lift.toFixed(1)}x lift` : null,
    ].filter(Boolean);

    rows.push({
      id: `firewatch:${feedId}:${buildFirewatchPatternGroupKey(primaryRow)}`,
      dedupe_key: `firewatch:${feedId}:${buildFirewatchPatternGroupKey(primaryRow)}`,
      feed_id: feedId,
      feeder_id: Number.isFinite(feederId) ? feederId : 0,
      post_key: null,
      checkpoint: 'd7',
      business_date_ist: nullableString(primaryRow.business_date_ist) || '',
      card_kind: 'firewatch',
      signal_code: nullableString(primarySummary.signal_code) || 'CROSS_PATTERN',
      context: nullableString(primarySummary.context) || 'own',
      alert_type: nullableString(primarySummary.alert_type) || 'watch',
      status: 'new',
      metric_key: null,
      metric_value: primarySummary.match_count ?? null,
      surface_percentile: primarySummary.avg_hot_percentile ?? primarySummary.surface_percentile ?? null,
      surface_percentile_exact: primarySummary.avg_hot_percentile ?? primarySummary.surface_percentile ?? null,
      surface_delta: null,
      feed_rank: null,
      feeder_rank: null,
      anchor_handle: null,
      anchor_best_pct: primarySummary.anchor_avg_percentile ?? null,
      anchor_gap: primarySummary.anchor_gap ?? null,
      body: [familyLabel, primaryPatternLabel, ...statBits].filter(Boolean).join(' · '),
      created_at: nullableString(primarySummary.created_at) || nullableString(primaryRow.created_at) || new Date().toISOString(),
      updated_at: nullableString(primaryRow.updated_at) || nullableString(primaryRow.created_at) || new Date().toISOString(),
      handle: feedName,
      media_type: nullableString(primarySummary.media_type) || heroPreview?.media_type || null,
      posted_at: heroPreview?.posted_at || null,
      post_url: heroPreview?.post_url || null,
      thumbnail_url: heroPreview?.thumbnail_url || null,
      preview_url: null,
      resolved_thumbnail_url: heroPreview?.thumbnail_url || null,
      resolved_preview_url: null,
      views: null,
      likes: null,
      comments: null,
      views_baseline: null,
      likes_baseline: null,
      comments_baseline: null,
      views_multiple: null,
      likes_multiple: null,
      comments_multiple: null,
      hour_ist: null,
      hour_percentile: null,
      hour_multiple: null,
      best_in_last_n: null,
      trajectory_d1: null,
      trajectory_d3: null,
      trajectory_d7: primarySummary.avg_hot_percentile ?? null,
      trajectory_d21: null,
      intelligence_skipped: false,
      pattern_alerts: [{
        ...primarySummary,
        support_posts: allSupportPosts,
        anchor_support_posts: allAnchorSupportPosts,
      }],
      is_hot: true,
      has_intelligence: true,
      hide_signal_chrome: false,
    });
  }

  return rows;
}

async function fetchRecentTrackingMetricRows(
  sb: { from: ReturnType<typeof createClient>['from'] },
  postKeys: string[],
  startIstDayKey: string,
): Promise<FirePostMetricRow[]> {
  if (postKeys.length === 0) return [];

  const rows: FirePostMetricRow[] = [];
  for (let start = 0; start < postKeys.length; start += POST_KEY_CHUNK_SIZE) {
    const chunk = postKeys.slice(start, start + POST_KEY_CHUNK_SIZE);
    const { data, error } = await sb
      .from('post_metrics')
      .select('post_key,checkpoint,business_date_ist,percentile_performance')
      .in('post_key', chunk)
      .in('checkpoint', [...DEFAULT_TRACKING_CHECKPOINTS])
      .gte('business_date_ist', startIstDayKey);

    if (error) throw error;
    rows.push(...((data || []) as FirePostMetricRow[]));
  }

  return rows;
}

function dedupeMetricRows(rows: FirePostMetricRow[]): FirePostMetricRow[] {
  const deduped = new Map<string, FirePostMetricRow>();
  for (const row of rows) {
    const postKey = typeof row.post_key === 'string' ? row.post_key.trim() : '';
    const checkpoint = normalizeTrackingCheckpoint(row.checkpoint);
    const businessDay = typeof row.business_date_ist === 'string' ? row.business_date_ist.trim() : '';
    if (!postKey || !checkpoint || !businessDay) continue;

    const key = buildFireCardKey(postKey, checkpoint, businessDay);
    const current = deduped.get(key);
    if (!current || parseIsoTime(row.computed_at) > parseIsoTime(current.computed_at)) {
      deduped.set(key, row);
    }
  }
  return Array.from(deduped.values());
}

function requiresLegacyBaselineFallback(row: FirePostMetricRow): boolean {
  return (
    nullableNumber(row.views_baseline) == null ||
    nullableNumber(row.likes_baseline) == null ||
    nullableNumber(row.comments_baseline) == null ||
    nullableString(row.ranking_metric) == null ||
    nullableNumber(row.percentile_performance_exact) == null
  );
}

function buildSyntheticFireRows(options: {
  posts: FireTrackedPostRow[];
  currentMetricRows: FirePostMetricRow[];
  allMetricRows: FirePostMetricRow[];
  feederRows: ActiveFeederRow[];
  baselineRows: FireFeederBaselineRow[];
  hourBaselineRows: FireFeederHourBaselineRow[];
  intelligenceRows: FireIntelligenceRow[];
}): AlertSurfaceRow[] {
  const feederById = new Map<number, ActiveFeederRow>(
    options.feederRows.map((row) => [Number(row.id), row]),
  );
  const postByKey = new Map<string, FireTrackedPostRow>();
  for (const post of options.posts) {
    const postKey = typeof post.post_key === 'string' ? post.post_key.trim() : '';
    if (postKey) postByKey.set(postKey, post);
  }

  const trajectoryByPost = new Map<string, Partial<Record<TrackingCheckpoint, FirePostMetricRow>>>();
  for (const row of options.allMetricRows) {
    const postKey = typeof row.post_key === 'string' ? row.post_key.trim() : '';
    const checkpoint = normalizeTrackingCheckpoint(row.checkpoint);
    if (!postKey || !checkpoint) continue;
    const bucket = trajectoryByPost.get(postKey) || {};
    const current = bucket[checkpoint];
    if (!current || parseIsoTime(row.computed_at) > parseIsoTime(current.computed_at)) {
      bucket[checkpoint] = row;
    }
    trajectoryByPost.set(postKey, bucket);
  }

  const baselineByKey = new Map<string, FireFeederBaselineRow>();
  for (const row of options.baselineRows) {
    const feederId = Number(row.feeder_id);
    const checkpoint = normalizeTrackingCheckpoint(row.checkpoint);
    if (!Number.isFinite(feederId) || !checkpoint) continue;
    baselineByKey.set(buildBaselineKey(feederId, String(row.media_type || ''), checkpoint), row);
  }

  const hourBaselineByKey = new Map<string, FireFeederHourBaselineRow>();
  for (const row of options.hourBaselineRows) {
    const feederId = Number(row.feeder_id);
    const checkpoint = normalizeTrackingCheckpoint(row.checkpoint);
    const hour = nullableNumber(row.hour_ist);
    if (!Number.isFinite(feederId) || !checkpoint || hour == null) continue;
    hourBaselineByKey.set(buildHourBaselineKey(feederId, String(row.media_type || ''), checkpoint, hour), row);
  }

  const intelligenceByPostKey = new Map<string, FireIntelligenceRow>();
  for (const row of options.intelligenceRows) {
    const postKey = typeof row.post_key === 'string' ? row.post_key.trim() : '';
    if (postKey) intelligenceByPostKey.set(postKey, row);
  }

  const rows: AlertSurfaceRow[] = [];
  for (const metricRow of dedupeMetricRows(options.currentMetricRows)) {
    const postKey = typeof metricRow.post_key === 'string' ? metricRow.post_key.trim() : '';
    const checkpoint = normalizeTrackingCheckpoint(metricRow.checkpoint);
    const businessDay = typeof metricRow.business_date_ist === 'string' ? metricRow.business_date_ist.trim() : '';
    if (!postKey || !checkpoint || !businessDay) continue;

    const post = postByKey.get(postKey);
    if (!post) continue;

    const feederId = Number(post.feeder_id);
    const feeder = feederById.get(feederId);
    if (!Number.isFinite(feederId) || !feeder) continue;

    const mediaType = normalizeSurfaceMediaType(post.media_type);
    const postedAt = nullableString(post.posted_at);
    const parsedHour = postedAt
      ? Number.parseInt(
        new Intl.DateTimeFormat('en-GB', {
          timeZone: 'Asia/Kolkata',
          hour: '2-digit',
          hour12: false,
        }).format(new Date(postedAt)),
        10,
      )
      : NaN;
    const hourIst = Number.isFinite(parsedHour) ? parsedHour : null;
    const baseline = baselineByKey.get(buildBaselineKey(feederId, mediaType, checkpoint)) || null;
    const hourBaseline = hourIst == null
      ? null
      : hourBaselineByKey.get(buildHourBaselineKey(feederId, mediaType, checkpoint, hourIst)) || null;
    const intelligenceModelVersion = intelligenceByPostKey.get(postKey)?.model_version ?? null;
    const surfacePercentile = nullableNumber(metricRow.percentile_performance);
    const surfacePercentileExact = nullableNumber(metricRow.percentile_performance_exact) ?? surfacePercentile;
    const displayPercentile = surfacePercentileExact ?? surfacePercentile;
    const hasIntelligence = Boolean(intelligenceModelVersion && intelligenceModelVersion !== 'skipped');
    const isHot = isHotPercentile(displayPercentile);

    const views = nullableNumber(metricRow.views);
    const likes = nullableNumber(metricRow.likes);
    const comments = nullableNumber(metricRow.comments);
    const viewsBaseline = nullableNumber(metricRow.views_baseline) ?? baselineValueFromRow(baseline, 'views');
    const likesBaseline = nullableNumber(metricRow.likes_baseline) ?? baselineValueFromRow(baseline, 'likes');
    const commentsBaseline = nullableNumber(metricRow.comments_baseline) ?? baselineValueFromRow(baseline, 'comments');
    const viewsMultiple = nullableNumber(metricRow.views_multiple) ?? computeMultiple(views, viewsBaseline);
    const likesMultiple = nullableNumber(metricRow.likes_multiple) ?? computeMultiple(likes, likesBaseline);
    const commentsMultiple = nullableNumber(metricRow.comments_multiple) ?? computeMultiple(comments, commentsBaseline);

    const rowSeed = {
      id: buildSyntheticAlertId(Number(feeder.feed_id), postKey, checkpoint, businessDay),
      dedupe_key: buildSyntheticAlertId(Number(feeder.feed_id), postKey, checkpoint, businessDay),
      feed_id: Number(feeder.feed_id),
      feeder_id: feederId,
      post_key: postKey,
      checkpoint,
      business_date_ist: businessDay,
      card_kind: 'tracking',
      signal_code: TRACKING_SIGNAL_CODE,
      context: 'own',
      alert_type: 'watch',
      status: 'new',
      metric_key: nullableString(metricRow.ranking_metric),
      metric_value: nullableNumber(metricRow.metric_value),
      surface_percentile: displayPercentile,
      surface_percentile_exact: displayPercentile,
      surface_delta: nullableNumber(metricRow.delta_from_d1),
      feed_rank: null,
      feeder_rank: null,
      anchor_handle: null,
      anchor_best_pct: null,
      anchor_gap: null,
      body: '',
      created_at: nullableString(metricRow.computed_at) || postedAt || new Date().toISOString(),
      updated_at: nullableString(metricRow.computed_at) || postedAt || new Date().toISOString(),
      handle: nullableString(feeder.handle),
      media_type: mediaType,
      posted_at: postedAt,
      post_url: nullableString(post.post_url),
      thumbnail_url: nullableString(post.thumbnail_url),
      preview_url: nullableString(post.video_url),
      views,
      likes,
      comments,
      views_baseline: viewsBaseline,
      likes_baseline: likesBaseline,
      comments_baseline: commentsBaseline,
      views_multiple: viewsMultiple,
      likes_multiple: likesMultiple,
      comments_multiple: commentsMultiple,
      hour_ist: hourIst,
      hour_percentile: null,
      hour_multiple: null,
      best_in_last_n: null,
      trajectory_d1: nullableNumber(trajectoryByPost.get(postKey)?.d1?.percentile_performance),
      trajectory_d3: nullableNumber(trajectoryByPost.get(postKey)?.d3?.percentile_performance),
      trajectory_d7: nullableNumber(trajectoryByPost.get(postKey)?.d7?.percentile_performance),
      trajectory_d21: nullableNumber(trajectoryByPost.get(postKey)?.d21?.percentile_performance),
      intelligence_skipped: intelligenceByPostKey.get(postKey)?.model_version === 'skipped',
      pattern_alerts: null,
      is_hot: isHot,
      has_intelligence: hasIntelligence,
      hide_signal_chrome: false,
    } as AlertSurfaceRow;

    const bestMetric = deriveBestMetric(rowSeed);
    const bestValue = metricValueFromPostMetric(metricRow, bestMetric) ?? nullableNumber(metricRow.metric_value);
    const hourMultiple = nullableNumber(metricRow.hour_multiple)
      ?? computeMultiple(bestValue, hourBaselineValueFromRow(hourBaseline, bestMetric));
    rowSeed.metric_key = bestMetric;
    rowSeed.metric_value = bestValue;
    rowSeed.hour_multiple = hourMultiple;

    rows.push(rowSeed);
  }

  return rows;
}

async function loadTrackingFireRows(
  sb: { from: ReturnType<typeof createClient>['from'] },
  options: {
    day: string;
    feederRows: ActiveFeederRow[];
    effectiveFeedIds: number[];
    effectiveFeederIds: number[];
    trackingCheckpoints: DefaultTrackingCheckpoint[];
  },
): Promise<AlertSurfaceRow[]> {
  const { posts, metricRows } = await fetchCurrentDayTrackingRows(
    sb,
    options.effectiveFeederIds,
    options.day,
    [...options.trackingCheckpoints],
  );
  const dedupedDayMetricRows = dedupeMetricRows(metricRows);
  const dayPostKeys = Array.from(
    new Set(
      dedupedDayMetricRows
        .map((row) => (typeof row.post_key === 'string' ? row.post_key.trim() : ''))
        .filter(Boolean),
    ),
  );

  if (dayPostKeys.length === 0) return [];

  const needsLegacyBaselineFallback = dedupedDayMetricRows.some(requiresLegacyBaselineFallback);
  const [allMetricRows, intelligenceRows, baselineRows, hourBaselineRows] = await Promise.all([
    fetchMetricRowsForPostKeys(sb, dayPostKeys, { checkpoints: [...TRACKING_CHECKPOINTS] }),
    fetchIntelligenceRowsForPostKeys(sb, dayPostKeys),
    needsLegacyBaselineFallback
      ? fetchFeederBaselineRows(sb, options.effectiveFeederIds, [...options.trackingCheckpoints])
      : Promise.resolve([] as FireFeederBaselineRow[]),
    needsLegacyBaselineFallback
      ? fetchFeederHourBaselineRows(sb, options.effectiveFeederIds, [...options.trackingCheckpoints])
      : Promise.resolve([] as FireFeederHourBaselineRow[]),
  ]);

  return buildSyntheticFireRows({
    posts,
    currentMetricRows: dedupedDayMetricRows,
    allMetricRows,
    feederRows: options.feederRows,
    baselineRows,
    hourBaselineRows,
    intelligenceRows,
  });
}

type FireActiveState = {
  normalizedFeeds: ActiveFeedRow[];
  normalizedFeeders: ActiveFeederRow[];
  activeFeedIds: number[];
  activeFeederIds: number[];
  feederCreatedAtById: Map<number, string | null>;
};

type FirePageRequestState = {
  day: string;
  threshold: '10' | '25' | '50' | 'ALL';
  mediaFilter: 'IMAGE' | 'CAROUSEL' | 'REEL' | 'ALL';
  sort: 'best' | 'recent';
  cursor: number;
  pageSize?: number;
  requestedFeedIds: number[];
  requestedFeederIds: number[];
  requestedCheckpoints: string[];
};

function parseNumberArray(value: unknown): number[] {
  if (!Array.isArray(value)) return [];
  return Array.from(
    new Set(
      value
        .map((entry) => Number.parseInt(String(entry), 10))
        .filter(Number.isFinite),
    ),
  );
}

function parseStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return Array.from(
    new Set(
      value
        .map((entry) => String(entry || '').trim().toUpperCase())
        .filter(Boolean),
    ),
  );
}

function parseCursorValue(value: unknown): number {
  return Math.max(0, Number.parseInt(String(value ?? '0'), 10) || 0);
}

function parsePageSizeValue(value: unknown, fallback = PAGE_SIZE): number {
  const parsed = Number.parseInt(String(value ?? fallback), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(1, Math.min(parsed, PAGE_SIZE));
}

function parseBootstrapPrefetchDayCount(value: unknown): number {
  const parsed = Number.parseInt(String(value ?? FIRE_BOOTSTRAP_PREFETCH_DAY_COUNT), 10);
  if (!Number.isFinite(parsed)) return FIRE_BOOTSTRAP_PREFETCH_DAY_COUNT;
  return Math.max(1, Math.min(parsed, FIRE_MAX_BOOTSTRAP_PREFETCH_DAY_COUNT));
}

function normalizeThresholdValue(value: unknown): '10' | '25' | '50' | 'ALL' {
  const normalized = String(value || 'ALL').trim().toUpperCase();
  return normalized === '10' || normalized === '25' || normalized === '50' ? normalized : 'ALL';
}

function normalizeMediaFilterValue(value: unknown): 'IMAGE' | 'CAROUSEL' | 'REEL' | 'ALL' {
  const normalized = String(value || 'ALL').trim().toUpperCase();
  return normalized === 'IMAGE' || normalized === 'CAROUSEL' || normalized === 'REEL' ? normalized : 'ALL';
}

function mediaFilterForValue(value: unknown): 'IMAGE' | 'CAROUSEL' | 'REEL' | null {
  const normalized = String(value || '').trim().toLowerCase();
  if (!normalized) return null;
  if (normalized.includes('reel') || normalized.includes('video')) return 'REEL';
  if (normalized.includes('carousel') || normalized.includes('sidecar')) return 'CAROUSEL';
  if (normalized.includes('image') || normalized.includes('photo')) return 'IMAGE';
  return null;
}

function normalizeSortValue(value: unknown): 'best' | 'recent' {
  return String(value || '').trim().toLowerCase() === 'recent' ? 'recent' : 'best';
}

function emptyFirePagePayload(day: string, cursor: number) {
  return {
    rows: [],
    total: 0,
    hasMore: false,
    day,
    cursor,
    availableCheckpoints: [],
    snapshotToken: null,
  };
}

function resolveFireScope(
  activeFeedIds: number[],
  normalizedFeeders: ActiveFeederRow[],
  requestedFeedIds: number[],
  requestedFeederIds: number[],
) {
  const feedIdSet = new Set(activeFeedIds);
  const effectiveFeedIds = requestedFeedIds.length > 0
    ? requestedFeedIds.filter((id) => feedIdSet.has(id))
    : activeFeedIds;

  const feedersForFeeds = normalizedFeeders.filter((row) => effectiveFeedIds.includes(Number(row.feed_id)));
  const feederIdSet = new Set(feedersForFeeds.map((row) => Number(row.id)));
  const effectiveFeederIds = requestedFeederIds.length > 0
    ? requestedFeederIds.filter((id) => feederIdSet.has(id))
    : Array.from(feederIdSet);

  return {
    effectiveFeedIds,
    effectiveFeederIds,
  };
}

async function loadActiveFireState(
  sb: { from: ReturnType<typeof createClient>['from'] },
  userId: string,
): Promise<FireActiveState> {
  const { data: activeFeeds, error: activeFeedsErr } = await sb
    .from('feeds')
    .select('id,name')
    .eq('user_id', userId)
    .eq('status', 'active')
    .limit(2000);
  if (activeFeedsErr) throw activeFeedsErr;

  const normalizedFeeds = (activeFeeds ?? []) as ActiveFeedRow[];
  const activeFeedIds = normalizedFeeds.map((row) => Number(row.id)).filter(Number.isFinite);
  if (activeFeedIds.length === 0) {
    return {
      normalizedFeeds,
      normalizedFeeders: [],
      activeFeedIds: [],
      activeFeederIds: [],
      feederCreatedAtById: new Map<number, string | null>(),
    };
  }

  const { data: activeFeeders, error: activeFeedersErr } = await sb
    .from('feeders')
    .select('id,feed_id,handle,created_at')
    .eq('status', 'active')
    .in('feed_id', activeFeedIds)
    .limit(5000);
  if (activeFeedersErr) throw activeFeedersErr;

  const normalizedFeeders = (activeFeeders ?? []) as ActiveFeederRow[];
  const activeFeederIds = normalizedFeeders.map((row) => Number(row.id)).filter(Number.isFinite);

  return {
    normalizedFeeds,
    normalizedFeeders,
    activeFeedIds,
    activeFeederIds,
    feederCreatedAtById: new Map<number, string | null>(
      normalizedFeeders.map((row) => [Number(row.id), row.created_at ?? null]),
    ),
  };
}

async function loadCachedActiveFireState(
  sb: { from: ReturnType<typeof createClient>['from'] },
  userId: string,
): Promise<FireActiveState> {
  return withServerRouteCache(
    `fire:active:${userId}`,
    FIRE_ACTIVE_STATE_TTL_MS,
    () => loadActiveFireState(sb, userId),
  );
}

function fireMetaCacheKey(userId: string) {
  return `fire:meta:${userId}:${FIRE_BOOTSTRAP_DAY_COUNT}`;
}

function firePageCacheKey(userId: string, requestState: FirePageRequestState) {
  return [
    `fire:page:${userId}`,
    requestState.day,
    requestState.threshold,
    requestState.mediaFilter,
    requestState.sort,
    String(parseCursorValue(requestState.cursor)),
    String(parsePageSizeValue(requestState.pageSize, PAGE_SIZE)),
    serializeNumberList(requestState.requestedFeedIds),
    serializeNumberList(requestState.requestedFeederIds),
    serializeStringList(requestState.requestedCheckpoints),
  ].join(':');
}

async function buildCachedTrackingFirePagePayload(
  sb: { from: ReturnType<typeof createClient>['from'] },
  userId: string,
  activeState: FireActiveState,
  requestState: FirePageRequestState,
) {
  return withServerRouteCache(
    firePageCacheKey(userId, requestState),
    FIRE_PAGE_TTL_MS,
    () => buildTrackingFirePagePayload(sb, activeState, requestState),
  );
}

async function buildCachedFireMetaPayload(
  sb: { from: ReturnType<typeof createClient>['from'] },
  userId: string,
  activeState: FireActiveState,
  recentKeys = buildRecentDayKeys(FIRE_BOOTSTRAP_DAY_COUNT),
) {
  return withServerRouteCache(
    fireMetaCacheKey(userId),
    FIRE_META_TTL_MS,
    () => buildFireMetaPayload(sb, activeState, recentKeys),
  );
}

async function buildTrackingFirePagePayload(
  sb: { from: ReturnType<typeof createClient>['from'] },
  activeState: FireActiveState,
  requestState: FirePageRequestState,
) {
  const scope = resolveFireScope(
    activeState.activeFeedIds,
    activeState.normalizedFeeders,
    requestState.requestedFeedIds,
    requestState.requestedFeederIds,
  );

  if (requestState.requestedFeedIds.length > 0 && scope.effectiveFeedIds.length === 0) {
    return emptyFirePagePayload(requestState.day, requestState.cursor);
  }

  if (scope.effectiveFeederIds.length === 0) {
    return emptyFirePagePayload(requestState.day, requestState.cursor);
  }

  const selectedTrackingCheckpoints = resolveRequestedTrackingCheckpoints(requestState.requestedCheckpoints);
  const [trackingRows, firewatchPatternRows] = await Promise.all([
    loadTrackingFireRows(sb, {
      day: requestState.day,
      feederRows: activeState.normalizedFeeders,
      effectiveFeedIds: scope.effectiveFeedIds,
      effectiveFeederIds: scope.effectiveFeederIds,
      trackingCheckpoints: selectedTrackingCheckpoints,
    }),
    fetchFirewatchPatternRows(
      sb,
      scope.effectiveFeedIds,
      scope.effectiveFeederIds,
      requestState.day,
    ),
  ]);
  const firewatchSupportKeys = Array.from(
    new Set(
      firewatchPatternRows.flatMap((row) => {
        const payload = recordValue(row.signal_payload);
        const keys = [
          nullableString(row.post_key),
          ...(Array.isArray(payload.support_post_keys) ? payload.support_post_keys.map((value) => nullableString(value)) : []),
          ...(Array.isArray(payload.anchor_support_post_keys) ? payload.anchor_support_post_keys.map((value) => nullableString(value)) : []),
        ];
        return keys.filter((value): value is string => Boolean(value));
      }),
    ),
  );
  const firewatchSupportPreviewByKey = await fetchPatternSupportPreviews(sb, firewatchSupportKeys);
  const feedNamesById = new Map<number, string>(
    activeState.normalizedFeeds.map((feed) => [Number(feed.id), String(feed.name || 'UNTITLED FEED').toUpperCase()]),
  );
  const firewatchRows = buildFirewatchRows({
    patternRows: firewatchPatternRows,
    supportPreviewByKey: firewatchSupportPreviewByKey,
    feedNamesById,
  });
  const rows = [...trackingRows, ...firewatchRows];

  const thresholdLimit = requestState.threshold === 'ALL' ? null : Number.parseInt(requestState.threshold, 10);
  const thresholdedRows = thresholdLimit == null
    ? rows
    : rows.filter((row) => {
      const percentile = nullableNumber(row.surface_percentile);
      return percentile != null && percentile <= thresholdLimit;
    });

  const mediaFilteredRows = requestState.mediaFilter === 'ALL'
    ? thresholdedRows
    : thresholdedRows.filter((row) => {
      const mediaFilter = mediaFilterForValue(row.media_type);
      return mediaFilter === requestState.mediaFilter;
    });

  const availableCheckpoints = sortCheckpoints(
    Array.from(
      new Set(
        mediaFilteredRows
          .map((row) => normalizeCheckpoint(row.checkpoint))
          .filter(Boolean),
      ),
    ),
  );

  let filteredRows = mediaFilteredRows;
  if (selectedTrackingCheckpoints.length > 0) {
    const requestedCheckpointSet = new Set(selectedTrackingCheckpoints.map((checkpoint) => checkpoint.toUpperCase()));
    filteredRows = filteredRows.filter((row) => requestedCheckpointSet.has(normalizeCheckpoint(row.checkpoint)));
  }

  const sortedRows = [...filteredRows].sort((a, b) => {
    if (requestState.sort === 'recent') {
      const aPostedAt = parseIsoTime(a.posted_at);
      const bPostedAt = parseIsoTime(b.posted_at);
      if (bPostedAt !== aPostedAt) return bPostedAt - aPostedAt;
    }

    const aPercentile = percentileExactValue(a);
    const bPercentile = percentileExactValue(b);
    if (aPercentile !== bPercentile) return aPercentile - bPercentile;

    const aMetric = deriveBestMetric(a);
    const bMetric = deriveBestMetric(b);
    const aMultiple = rowMetricMultiple(a, aMetric) ?? Number.NEGATIVE_INFINITY;
    const bMultiple = rowMetricMultiple(b, bMetric) ?? Number.NEGATIVE_INFINITY;
    if (aMultiple !== bMultiple) return bMultiple - aMultiple;

    const aMetricValue = rowMetricValue(a, aMetric) ?? Number.NEGATIVE_INFINITY;
    const bMetricValue = rowMetricValue(b, bMetric) ?? Number.NEGATIVE_INFINITY;
    if (aMetricValue !== bMetricValue) return bMetricValue - aMetricValue;

    const aPostedAt = parseIsoTime(a.posted_at);
    const bPostedAt = parseIsoTime(b.posted_at);
    if (bPostedAt !== aPostedAt) return bPostedAt - aPostedAt;

    return parseIsoTime(b.created_at) - parseIsoTime(a.created_at);
  });

  const pageSize = parsePageSizeValue(requestState.pageSize, PAGE_SIZE);
  // Collapse repeated checkpoint variants of the same post after sorting so
  // Fire shows one winning card per post while keeping checkpoint filters intact.
  const dedupedRows = dedupeSortedAlertRows(sortedRows);
  const total = dedupedRows.length;
  const pagedRows = dedupedRows.slice(requestState.cursor, requestState.cursor + pageSize);
  const hasMore = requestState.cursor + pagedRows.length < total;
  const { thumbnailUrls: storedThumbnailUrls, previewUrls: storedPreviewUrls } = await fetchStoredMediaUrls(
    sb,
    pagedRows.map((row) => row.post_key).filter((value): value is string => Boolean(value)),
  );

  return {
    rows: pagedRows.map((row) => serializeAlertRow({
      ...row,
      resolved_thumbnail_url: row.post_key
        ? storedThumbnailUrls.get(row.post_key) || nullableString(row.thumbnail_url) || null
        : nullableString(row.thumbnail_url) || null,
      resolved_preview_url: previewCaptureAllowedForBusinessDay(nullableString(row.business_date_ist))
        ? row.post_key
          ? storedPreviewUrls.get(row.post_key) || null
          : null
        : null,
    })),
    total,
    hasMore,
    day: requestState.day,
    cursor: requestState.cursor,
    availableCheckpoints,
    snapshotToken: null,
  };
}

async function buildFireMetaPayload(
  sb: { from: ReturnType<typeof createClient>['from'] },
  activeState: FireActiveState,
  recentKeys = buildRecentDayKeys(FIRE_BOOTSTRAP_DAY_COUNT),
) {
  const feedersByFeed = new Map<number, { id: number; handle: string }[]>();
  for (const feeder of activeState.normalizedFeeders) {
    const feedId = Number(feeder.feed_id);
    if (!Number.isFinite(feedId)) continue;
    const bucket = feedersByFeed.get(feedId) || [];
    bucket.push({ id: Number(feeder.id), handle: String(feeder.handle || '') });
    feedersByFeed.set(feedId, bucket);
  }

  const feeds = activeState.normalizedFeeds
    .map((feed) => ({
      id: Number(feed.id),
      name: String(feed.name || 'UNTITLED FEED').toUpperCase(),
      feeders: (feedersByFeed.get(Number(feed.id)) || [])
        .filter((feeder) => Number.isFinite(feeder.id) && feeder.handle)
        .sort((a, b) => a.handle.localeCompare(b.handle)),
    }))
    .sort((a, b) => a.name.localeCompare(b.name));

  const warmupSummary = await fetchWarmupSummary(
    sb,
    activeState.activeFeederIds,
    activeState.feederCreatedAtById,
  );

  return {
    days: recentKeys,
    scopes: [],
    feeds,
    dayCounts: {},
    warmupSummary,
  };
}

export async function GET(request: NextRequest) {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!url || !key) {
    return NextResponse.json({ error: 'Missing Supabase config' }, { status: 500 });
  }

  const authClient = await createServerClient();
  const { data: authData, error: authErr } = await authClient.auth.getUser();
  if (authErr || !authData?.user) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }
  const userId = authData.user.id;

  const supabase = createClient(url, key);
  const params = request.nextUrl.searchParams;
  const mode = params.get('mode'); // 'meta' returns only days + scopes
  const recentKeys = buildRecentDayKeys(FIRE_BOOTSTRAP_DAY_COUNT);
  let activeState: FireActiveState;
  try {
    activeState = await loadCachedActiveFireState(supabase, userId);
  } catch (error) {
    console.error('[/api/fire] Active state query error:', error);
    const message = error instanceof Error ? error.message : 'Failed to load fire scope';
    return NextResponse.json({ error: message }, { status: 500 });
  }

  if (activeState.activeFeedIds.length === 0 || activeState.activeFeederIds.length === 0) {
    if (mode === 'meta' || mode === 'bootstrap') {
      return privateJsonResponse(request, {
        days: recentKeys,
        scopes: [],
        feeds: [],
        dayCounts: {},
        warmupSummary: {},
        initialDay: recentKeys[0] || todayIstDayKey(),
        initialPage: emptyFirePagePayload(recentKeys[0] || todayIstDayKey(), 0),
      }, {
        maxAgeSeconds: 60,
        staleWhileRevalidateSeconds: 600,
      });
    }
    return privateJsonResponse(
      request,
      emptyFirePagePayload(params.get('day') || todayIstDayKey(), 0),
      {
        maxAgeSeconds: 60,
        staleWhileRevalidateSeconds: 600,
      },
    );
  }

  // ─── META MODE ─────────────────────────────────────────────
  // Returns available days plus nested feed / feeder options.
  if (mode === 'meta') {
    try {
      return privateJsonResponse(
        request,
        await buildCachedFireMetaPayload(supabase, userId, activeState, recentKeys),
        {
          maxAgeSeconds: 60,
          staleWhileRevalidateSeconds: 600,
        },
      );
    } catch (error) {
      console.error('[/api/fire?mode=meta] Error:', error);
      const message = error instanceof Error ? error.message : 'Failed to load fire meta';
      return NextResponse.json({ error: message }, { status: 500 });
    }
  }

  if (mode === 'bootstrap') {
    try {
      const pageSize = parsePageSizeValue(params.get('pageSize'), FIRE_DEFAULT_BOOTSTRAP_PAGE_SIZE);
      const prefetchDayCount = parseBootstrapPrefetchDayCount(params.get('prefetchDays'));
      const initialDay = recentKeys[0] || todayIstDayKey();
      const bootstrapDays = recentKeys.slice(0, prefetchDayCount);
      const [meta, prefetchedPages] = await Promise.all([
        buildCachedFireMetaPayload(supabase, userId, activeState, recentKeys),
        Promise.all(
          bootstrapDays.map(async (day) => ({
            day,
            payload: await buildCachedTrackingFirePagePayload(supabase, userId, activeState, {
              day,
              threshold: 'ALL',
              mediaFilter: 'ALL',
              sort: 'best',
              cursor: 0,
              pageSize,
              requestedFeedIds: [],
              requestedFeederIds: [],
              requestedCheckpoints: [],
            }),
          })),
        ),
      ]);
      const initialPage = prefetchedPages.find((entry) => entry.day === initialDay)?.payload
        || prefetchedPages[0]?.payload
        || emptyFirePagePayload(initialDay, 0);
      return privateJsonResponse(request, {
        ...meta,
        initialDay,
        initialPage,
        prefetchedPages,
      }, {
        maxAgeSeconds: 60,
        staleWhileRevalidateSeconds: 600,
      });
    } catch (error) {
      console.error('[/api/fire?mode=bootstrap] Error:', error);
      const message = error instanceof Error ? error.message : 'Failed to bootstrap fire';
      return NextResponse.json({ error: message }, { status: 500 });
    }
  }

  try {
    const payload = await buildCachedTrackingFirePagePayload(supabase, userId, activeState, {
      day: params.get('day') || todayIstDayKey(),
      threshold: normalizeThresholdValue(params.get('threshold')),
      mediaFilter: normalizeMediaFilterValue(params.get('mediaFilter')),
      sort: normalizeSortValue(params.get('sort')),
      cursor: parseCursorValue(params.get('cursor')),
      pageSize: parsePageSizeValue(params.get('pageSize')),
      requestedFeedIds: parseCsvNumbers(params.get('feed_ids')),
      requestedFeederIds: parseCsvNumbers(params.get('feeder_ids')),
      requestedCheckpoints: parseCsvStrings(params.get('checkpoints')),
    });
    return privateJsonResponse(request, payload, {
      maxAgeSeconds: 60,
      staleWhileRevalidateSeconds: 600,
    });
  } catch (error) {
    console.error('[/api/fire] Error:', error);
    const message = error instanceof Error ? error.message : 'Failed to load fire alerts';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function POST(request: NextRequest) {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!url || !key) {
    return NextResponse.json({ error: 'Missing Supabase config' }, { status: 500 });
  }

  const authClient = await createServerClient();
  const { data: authData, error: authErr } = await authClient.auth.getUser();
  if (authErr || !authData?.user) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const supabase = createClient(url, key);
  const body = await request.json().catch(() => ({}));
  const day = typeof body?.day === 'string' && body.day.trim() ? body.day.trim() : todayIstDayKey();
  let activeState: FireActiveState;
  try {
    activeState = await loadCachedActiveFireState(supabase, authData.user.id);
  } catch (error) {
    console.error('[/api/fire:POST] Active state query error:', error);
    const message = error instanceof Error ? error.message : 'Failed to load fire scope';
    return NextResponse.json({ error: message }, { status: 500 });
  }

  if (activeState.activeFeedIds.length === 0 || activeState.activeFeederIds.length === 0) {
    return NextResponse.json(emptyFirePagePayload(day, parseCursorValue(body?.cursor)));
  }

  try {
    const payload = await buildCachedTrackingFirePagePayload(supabase, authData.user.id, activeState, {
      day,
      threshold: normalizeThresholdValue(body?.threshold),
      mediaFilter: normalizeMediaFilterValue(body?.mediaFilter),
      sort: normalizeSortValue(body?.sort),
      cursor: parseCursorValue(body?.cursor),
      pageSize: parsePageSizeValue(body?.pageSize),
      requestedFeedIds: parseNumberArray(body?.feedIds),
      requestedFeederIds: parseNumberArray(body?.feederIds),
      requestedCheckpoints: parseStringArray(body?.checkpoints),
    });
    return NextResponse.json(payload);
  } catch (error) {
    console.error('[/api/fire:POST] Error:', error);
    const message = error instanceof Error ? error.message : 'Failed to load fire alerts';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
