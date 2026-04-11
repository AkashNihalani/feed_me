import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';
import { createClient as createServerClient } from '@/lib/supabase/server';
import { getPatternCueLabel, getPatternMechanicLabel } from '@/lib/fireSignals';

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

function buildMediaProxyUrl(postKey: string | null | undefined, url: string | null | undefined, role = 'thumbnail'): string | null {
  const safePostKey = typeof postKey === 'string' ? postKey.trim() : '';
  const safeUrl = typeof url === 'string' ? url.trim() : '';
  if (!safePostKey || !safeUrl) return null;
  return `/api/media/post/${encodeURIComponent(safePostKey)}?role=${encodeURIComponent(role)}&url=${encodeURIComponent(safeUrl)}`;
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

function signedDeltaLabel(value: number | null): string | null {
  if (value == null || !Number.isFinite(value)) return null;
  const rounded = Math.round(value);
  if (rounded > 0) return `+${rounded}`;
  if (rounded < 0) return String(rounded);
  return '0';
}

function percentileLabel(value: number | null): string | null {
  if (value == null || !Number.isFinite(value)) return null;
  return `top ${Math.round(value)}%`;
}

function isHotPercentile(value: number | null): boolean {
  return value != null && Number.isFinite(value) && value <= HOT_PERCENTILE_MAX;
}

function buildTrackingBody(options: {
  checkpoint: string;
  handle: string | null;
  bestMetric: FireMetricKey;
  bestValue: number | null;
  percentile: number | null;
  multiple: number | null;
  deltaFromD1: number | null;
  patternSummaryBody: string | null;
}): string {
  if (options.patternSummaryBody) return options.patternSummaryBody;

  const handle = `@${(options.handle || 'feed').toUpperCase()}`;
  const checkpoint = options.checkpoint.toUpperCase();
  const metricLabel = options.bestMetric.toUpperCase();
  const metricValue = options.bestValue == null ? '--' : compactNumber(options.bestValue);
  const percentile = percentileLabel(options.percentile);
  const multiple = options.multiple == null ? null : `${options.multiple.toFixed(2)}x`;
  const delta = signedDeltaLabel(options.deltaFromD1);

  if (options.checkpoint.toLowerCase() === 'd1') {
    if (percentile && multiple) return `${handle} checked in at ${checkpoint} with ${metricValue} ${metricLabel}, ${percentile} and ${multiple} its usual ${metricLabel}.`;
    if (percentile) return `${handle} checked in at ${checkpoint} with ${metricValue} ${metricLabel}, ${percentile}.`;
    return `${handle} checked in at ${checkpoint} with ${metricValue} ${metricLabel}.`;
  }

  if (percentile && delta) return `${handle} checked in at ${checkpoint} with ${metricValue} ${metricLabel}, ${percentile} and ${delta} vs D1.`;
  if (percentile) return `${handle} checked in at ${checkpoint} with ${metricValue} ${metricLabel}, ${percentile}.`;
  return `${handle} checked in at ${checkpoint} with ${metricValue} ${metricLabel}.`;
}

function compactNumber(value: number): string {
  if (!Number.isFinite(value)) return '--';
  if (Math.abs(value) >= 1000000) return `${(value / 1000000).toFixed(value % 1000000 === 0 ? 0 : 1)}M`;
  if (Math.abs(value) >= 1000) return `${(value / 1000).toFixed(value % 1000 === 0 ? 0 : 1)}K`;
  return Math.round(value).toString();
}

function isMissingColumnError(error: unknown, columnName: string): boolean {
  const message = error instanceof Error
    ? error.message
    : typeof error === 'object' && error !== null && 'message' in error
      ? String((error as { message?: unknown }).message || '')
      : String(error || '');
  return message.toLowerCase().includes(`column fire_alerts.${columnName}`.toLowerCase());
}

type ActiveFeedRow = { id: number; name: string | null };
type ActiveFeederRow = { id: number; feed_id: number; handle: string | null; created_at: string | null };
type FireMetricCheckpointRow = { post_key: string | null; checkpoint: string | null };
type AlertSurfaceRow = {
  id: number | string;
  dedupe_key: string | null;
  feed_id: number;
  feeder_id: number;
  post_key: string;
  checkpoint: string;
  business_date_ist: string;
  signal_code: string | null;
  context: string | null;
  alert_type: string | null;
  status: string | null;
  metric_key: string | null;
  metric_value: number | string | null;
  surface_percentile: number | null;
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
  views_percentile: number | null;
  likes_percentile: number | null;
  comments_percentile: number | null;
  feed_percentile: number | null;
  delta_from_d1: number | null;
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
  match_count?: number | null;
  feeders_count?: number | null;
  avg_hot_percentile?: number | null;
  baseline_share?: number | null;
  recent_lift?: number | null;
  contrast_gap?: number | null;
  anchor_avg_percentile?: number | null;
  anchor_gap?: number | null;
  cues?: Array<{ key: string; value: string; label: string }> | null;
  support_posts?: FirePatternSupportPreview[] | null;
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

  if ((preferred === 'views' || preferred === 'likes' || preferred === 'comments') && rowMetricValue(row, preferred) != null) {
    return preferred;
  }

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
  const payload = {
    best_metric: bestMetric,
    metrics: {
      views: buildMetricPayload(row, 'views', bestMetric),
      likes: buildMetricPayload(row, 'likes', bestMetric),
      comments: buildMetricPayload(row, 'comments', bestMetric),
    },
    position: {
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
      handle: nullableString(row.handle),
      media_type: nullableString(row.media_type),
      checkpoint: nullableString(row.checkpoint),
      post_url: nullableString(row.post_url),
      thumbnail_url: nullableString(row.thumbnail_url),
      business_date_ist: nullableString(row.business_date_ist),
      alert_type: nullableString(row.alert_type),
      signal_code: nullableString(row.signal_code),
      signal_context: nullableString(row.context),
      anchor_handle: nullableString(row.anchor_handle),
      pattern_alert_count: row.pattern_alerts?.length ?? 0,
      pattern_alerts: row.pattern_alerts ?? [],
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
    surface_delta: nullableNumber(row.surface_delta),
    metric_key: bestMetric,
    metric_value: bestMetricValue ?? nullableNumber(row.metric_value),
    body: row.body ?? '',
    created_at: row.created_at,
    updated_at: row.updated_at,
    posted_at: row.posted_at,
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
  const summaries = sorted.map((row) => {
    const payload = recordValue(row.signal_payload);
    const cues = Array.isArray(payload.cues)
      ? payload.cues
        .map((cue) => recordValue(cue))
        .map((cue) => {
          const key = nullableString(cue.key);
          const value = nullableString(cue.value);
          const label = getPatternCueLabel(key, value);
          if (!key || !value || !label) return null;
          return { key, value, label };
        })
        .filter((cue): cue is { key: string; value: string; label: string } => cue !== null)
      : [];
    const supportPostKeys = Array.isArray(payload.support_post_keys)
      ? payload.support_post_keys.map((value) => nullableString(value)).filter((value): value is string => Boolean(value))
      : [];
    const supportPosts = supportPostKeys
      .map((postKey) => supportPreviewByKey.get(postKey) || null)
      .filter((preview): preview is FirePatternSupportPreview => preview !== null);
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
      match_count: nullableNumber(payload.match_count),
      feeders_count: nullableNumber(payload.feeders_count),
      avg_hot_percentile: nullableNumber(payload.avg_hot_percentile),
      baseline_share: nullableNumber(payload.baseline_share),
      recent_lift: nullableNumber(payload.recent_lift),
      contrast_gap: nullableNumber(payload.contrast_gap),
      anchor_avg_percentile: nullableNumber(payload.anchor_avg_percentile),
      anchor_gap: nullableNumber(payload.anchor_gap),
      cues,
      support_posts: supportPosts,
    } satisfies FirePatternSummary;
  });
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
): Promise<FireTrackedPostRow[]> {
  if (feederIds.length === 0) return [];

  const rows: FireTrackedPostRow[] = [];
  for (let start = 0; ; start += POSTS_PAGE_SIZE) {
    const { data, error } = await sb
      .from('posts')
      .select('post_key,feeder_id,media_type,posted_at,post_url,thumbnail_url')
      .in('feeder_id', feederIds)
      .order('feeder_id', { ascending: true })
      .order('post_key', { ascending: true })
      .range(start, start + POSTS_PAGE_SIZE - 1);

    if (error) throw error;

    const batch = (data || []) as FireTrackedPostRow[];
    rows.push(...batch);
    if (batch.length < POSTS_PAGE_SIZE) break;
  }

  return rows;
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

  const rows: FirePostMetricRow[] = [];
  for (let start = 0; start < postKeys.length; start += POST_KEY_CHUNK_SIZE) {
    const chunk = postKeys.slice(start, start + POST_KEY_CHUNK_SIZE);
    let query = sb
      .from('post_metrics')
      .select([
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
      ].join(','))
      .in('post_key', chunk)
      .in('checkpoint', options.checkpoints);

    if (options.businessDay) {
      query = query.eq('business_date_ist', options.businessDay);
    }

    const { data, error } = await query;
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
      thumbnail_url: buildMediaProxyUrl(postKey, nullableString(row.thumbnail_url)),
      posted_at: nullableString(row.posted_at),
    });
  }

  return previews;
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

function buildSyntheticFireRows(options: {
  posts: FireTrackedPostRow[];
  currentMetricRows: FirePostMetricRow[];
  allMetricRows: FirePostMetricRow[];
  feederRows: ActiveFeederRow[];
  baselineRows: FireFeederBaselineRow[];
  hourBaselineRows: FireFeederHourBaselineRow[];
  intelligenceRows: FireIntelligenceRow[];
  patternRows: FirePatternAlertRow[];
  supportPreviewByKey: Map<string, FirePatternSupportPreview>;
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

  const patternRowsByCard = new Map<string, FirePatternAlertRow[]>();
  for (const row of options.patternRows) {
    const postKey = typeof row.post_key === 'string' ? row.post_key.trim() : '';
    const checkpoint = normalizeTrackingCheckpoint(row.checkpoint);
    const businessDay = typeof row.business_date_ist === 'string' ? row.business_date_ist.trim() : '';
    if (!postKey || checkpoint !== 'd7' || !businessDay) continue;
    const key = buildFireCardKey(postKey, checkpoint, businessDay);
    const bucket = patternRowsByCard.get(key) || [];
    bucket.push(row);
    patternRowsByCard.set(key, bucket);
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
    const hasIntelligence = Boolean(intelligenceModelVersion && intelligenceModelVersion !== 'skipped');
    const isHot = isHotPercentile(nullableNumber(metricRow.percentile_performance));

    const views = nullableNumber(metricRow.views);
    const likes = nullableNumber(metricRow.likes);
    const comments = nullableNumber(metricRow.comments);
    const viewsBaseline = baselineValueFromRow(baseline, 'views');
    const likesBaseline = baselineValueFromRow(baseline, 'likes');
    const commentsBaseline = baselineValueFromRow(baseline, 'comments');
    const viewsMultiple = computeMultiple(views, viewsBaseline);
    const likesMultiple = computeMultiple(likes, likesBaseline);
    const commentsMultiple = computeMultiple(comments, commentsBaseline);

    const rowSeed = {
      id: buildSyntheticAlertId(Number(feeder.feed_id), postKey, checkpoint, businessDay),
      dedupe_key: buildSyntheticAlertId(Number(feeder.feed_id), postKey, checkpoint, businessDay),
      feed_id: Number(feeder.feed_id),
      feeder_id: feederId,
      post_key: postKey,
      checkpoint,
      business_date_ist: businessDay,
      signal_code: TRACKING_SIGNAL_CODE,
      context: 'own',
      alert_type: 'watch',
      status: 'new',
      metric_key: null,
      metric_value: nullableNumber(metricRow.metric_value),
      surface_percentile: nullableNumber(metricRow.percentile_performance),
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
    const hourMultiple = computeMultiple(bestValue, hourBaselineValueFromRow(hourBaseline, bestMetric));
    const patternGroup = checkpoint === 'd7'
      ? buildPatternGroup(
        patternRowsByCard.get(buildFireCardKey(postKey, checkpoint, businessDay)) || [],
        options.supportPreviewByKey,
      )
      : { primaryAlert: null, summaries: [], summaryBody: null };
    const primaryAlert = patternGroup.primaryAlert;
    const hasPatternInsights = patternGroup.summaries.length > 0;
    const showInsightChrome = checkpoint === 'd7' && isHot && hasPatternInsights;

    rowSeed.signal_code = showInsightChrome
      ? nullableString(primaryAlert?.signal_code) || TRACKING_SIGNAL_CODE
      : TRACKING_SIGNAL_CODE;
    rowSeed.context = showInsightChrome
      ? nullableString(primaryAlert?.context) || 'own'
      : 'own';
    rowSeed.alert_type = nullableString(primaryAlert?.alert_type) || 'watch';
    rowSeed.metric_key = bestMetric;
    rowSeed.metric_value = bestValue;
    rowSeed.anchor_handle = null;
    rowSeed.anchor_best_pct = showInsightChrome ? nullableNumber(primaryAlert?.anchor_avg_percentile) : null;
    rowSeed.anchor_gap = showInsightChrome ? nullableNumber(primaryAlert?.anchor_gap) : null;
    rowSeed.hour_multiple = hourMultiple;
    rowSeed.body = buildTrackingBody({
      checkpoint,
      handle: feeder.handle,
      bestMetric,
      bestValue,
      percentile: nullableNumber(metricRow.percentile_performance),
      multiple: rowMetricMultiple(rowSeed, bestMetric),
      deltaFromD1: nullableNumber(metricRow.delta_from_d1),
      patternSummaryBody: patternGroup.summaryBody,
    });
    rowSeed.pattern_alerts = showInsightChrome && hasPatternInsights ? patternGroup.summaries : null;
    rowSeed.hide_signal_chrome = !showInsightChrome;

    if (primaryAlert && showInsightChrome) {
      rowSeed.created_at = nullableString(primaryAlert.created_at) || rowSeed.created_at;
    }

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
  const posts = await fetchTrackedPosts(sb, options.effectiveFeederIds);
  const postKeys = Array.from(
    new Set(
      posts
        .map((row) => (typeof row.post_key === 'string' ? row.post_key.trim() : ''))
        .filter(Boolean),
    ),
  );

  const currentMetricRows = await fetchMetricRowsForPostKeys(sb, postKeys, {
    businessDay: options.day,
    checkpoints: [...options.trackingCheckpoints],
  });
  const dedupedDayMetricRows = dedupeMetricRows(currentMetricRows);
  const dayPostKeys = Array.from(
    new Set(
      dedupedDayMetricRows
        .map((row) => (typeof row.post_key === 'string' ? row.post_key.trim() : ''))
        .filter(Boolean),
    ),
  );

  if (dayPostKeys.length === 0) return [];

  const [allMetricRows, intelligenceRows, baselineRows, hourBaselineRows, patternRows] = await Promise.all([
    fetchMetricRowsForPostKeys(sb, dayPostKeys, { checkpoints: [...TRACKING_CHECKPOINTS] }),
    fetchIntelligenceRowsForPostKeys(sb, dayPostKeys),
    fetchFeederBaselineRows(sb, options.effectiveFeederIds, [...options.trackingCheckpoints]),
    fetchFeederHourBaselineRows(sb, options.effectiveFeederIds, [...options.trackingCheckpoints]),
    fetchPatternRowsForPostKeys(sb, options.effectiveFeedIds, options.effectiveFeederIds, options.day, dayPostKeys),
  ]);
  const supportPostKeys = Array.from(
    new Set(
      patternRows.flatMap((row) => {
        const payload = recordValue(row.signal_payload);
        if (!Array.isArray(payload.support_post_keys)) return [];
        return payload.support_post_keys
          .map((value) => nullableString(value))
          .filter((value): value is string => Boolean(value));
      }),
    ),
  );
  const supportPreviewByKey = await fetchPatternSupportPreviews(sb, supportPostKeys);

  return buildSyntheticFireRows({
    posts: posts.filter((row) => dayPostKeys.includes(String(row.post_key || '').trim())),
    currentMetricRows: dedupedDayMetricRows,
    allMetricRows,
    feederRows: options.feederRows,
    baselineRows,
    hourBaselineRows,
    intelligenceRows,
    patternRows,
    supportPreviewByKey,
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
  sort: 'best' | 'recent';
  cursor: number;
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

function normalizeThresholdValue(value: unknown): '10' | '25' | '50' | 'ALL' {
  const normalized = String(value || 'ALL').trim().toUpperCase();
  return normalized === '10' || normalized === '25' || normalized === '50' ? normalized : 'ALL';
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
  const rows = await loadTrackingFireRows(sb, {
    day: requestState.day,
    feederRows: activeState.normalizedFeeders,
    effectiveFeedIds: scope.effectiveFeedIds,
    effectiveFeederIds: scope.effectiveFeederIds,
    trackingCheckpoints: [...DEFAULT_TRACKING_CHECKPOINTS],
  });

  const thresholdLimit = requestState.threshold === 'ALL' ? null : Number.parseInt(requestState.threshold, 10);
  const thresholdedRows = thresholdLimit == null
    ? rows
    : rows.filter((row) => {
      const percentile = nullableNumber(row.surface_percentile);
      return percentile != null && percentile <= thresholdLimit;
    });

  const availableCheckpoints = sortCheckpoints(
    Array.from(
      new Set(
        thresholdedRows
          .map((row) => normalizeCheckpoint(row.checkpoint))
          .filter(Boolean),
      ),
    ),
  );

  let filteredRows = thresholdedRows;
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

    const aPercentile = percentileValue(a);
    const bPercentile = percentileValue(b);
    if (aPercentile !== bPercentile) return aPercentile - bPercentile;

    return parseIsoTime(b.created_at) - parseIsoTime(a.created_at);
  });

  const total = sortedRows.length;
  const pagedRows = sortedRows.slice(requestState.cursor, requestState.cursor + PAGE_SIZE);
  const hasMore = requestState.cursor + pagedRows.length < total;

  return {
    rows: pagedRows.map((row) => serializeAlertRow(row)),
    total,
    hasMore,
    day: requestState.day,
    cursor: requestState.cursor,
    availableCheckpoints,
    snapshotToken: null,
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
  const recentKeys = buildRecentDayKeys(7);
  let activeState: FireActiveState;
  try {
    activeState = await loadActiveFireState(supabase, userId);
  } catch (error) {
    console.error('[/api/fire] Active state query error:', error);
    const message = error instanceof Error ? error.message : 'Failed to load fire scope';
    return NextResponse.json({ error: message }, { status: 500 });
  }

  if (activeState.activeFeedIds.length === 0 || activeState.activeFeederIds.length === 0) {
    if (mode === 'meta') {
      return NextResponse.json({ days: recentKeys, scopes: [], feeds: [], dayCounts: {}, warmupSummary: {} });
    }
    return NextResponse.json(emptyFirePagePayload(params.get('day') || todayIstDayKey(), 0));
  }

  // ─── META MODE ─────────────────────────────────────────────
  // Returns available days plus nested feed / feeder options.
  if (mode === 'meta') {
    const windowDays = 14;
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - windowDays);
    const startIstDayKey = toIstDayKey(startDate);

    const daysSet = new Set<string>();
    const dayCounts: Record<string, number> = {};
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

    try {
      const trackedPosts = await fetchTrackedPosts(supabase, activeState.activeFeederIds);
      const trackedPostKeys = Array.from(
        new Set(
          trackedPosts
            .map((row) => (typeof row.post_key === 'string' ? row.post_key.trim() : ''))
            .filter(Boolean),
        ),
      );
      const dayRows = await fetchRecentTrackingMetricRows(supabase, trackedPostKeys, startIstDayKey);

      for (const row of dayRows) {
        const checkpoint = normalizeTrackingCheckpoint(row.checkpoint);
        const businessDay = typeof row.business_date_ist === 'string' ? row.business_date_ist.trim() : '';
        const postKey = typeof row.post_key === 'string' ? row.post_key.trim() : '';
        if (!checkpoint || !DEFAULT_TRACKING_CHECKPOINTS.includes(checkpoint as DefaultTrackingCheckpoint) || !businessDay || !postKey) continue;
        daysSet.add(businessDay);
        dayCounts[businessDay] = (dayCounts[businessDay] ?? 0) + 1;
      }

      for (const k of recentKeys) daysSet.add(k);
      const days = Array.from(daysSet).sort((a, b) => b.localeCompare(a)).slice(0, 7);
      const warmupSummary = await fetchWarmupSummary(
        supabase,
        activeState.activeFeederIds,
        activeState.feederCreatedAtById,
        trackedPosts,
      );

      return NextResponse.json({ days, scopes: [], feeds, dayCounts, warmupSummary });
    } catch (error) {
      console.error('[/api/fire?mode=meta] Error:', error);
      const message = error instanceof Error ? error.message : 'Failed to load fire meta';
      return NextResponse.json({ error: message }, { status: 500 });
    }
  }

  try {
    const payload = await buildTrackingFirePagePayload(supabase, activeState, {
      day: params.get('day') || todayIstDayKey(),
      threshold: normalizeThresholdValue(params.get('threshold')),
      sort: normalizeSortValue(params.get('sort')),
      cursor: parseCursorValue(params.get('cursor')),
      requestedFeedIds: parseCsvNumbers(params.get('feed_ids')),
      requestedFeederIds: parseCsvNumbers(params.get('feeder_ids')),
      requestedCheckpoints: parseCsvStrings(params.get('checkpoints')),
    });
    return NextResponse.json(payload);
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
    activeState = await loadActiveFireState(supabase, authData.user.id);
  } catch (error) {
    console.error('[/api/fire:POST] Active state query error:', error);
    const message = error instanceof Error ? error.message : 'Failed to load fire scope';
    return NextResponse.json({ error: message }, { status: 500 });
  }

  if (activeState.activeFeedIds.length === 0 || activeState.activeFeederIds.length === 0) {
    return NextResponse.json(emptyFirePagePayload(day, parseCursorValue(body?.cursor)));
  }

  try {
    const payload = await buildTrackingFirePagePayload(supabase, activeState, {
      day,
      threshold: normalizeThresholdValue(body?.threshold),
      sort: normalizeSortValue(body?.sort),
      cursor: parseCursorValue(body?.cursor),
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
