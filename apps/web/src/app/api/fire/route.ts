import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';
import { createClient as createServerClient } from '@/lib/supabase/server';
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
const D7_READ_PROMPT_VERSION = 'd7_read_v16';
const PREVIEW_CAPTURE_START_DAY = (process.env.FIRE_PREVIEW_START_DAY || '2026-04-14').trim();
const FIRE_BOOTSTRAP_DAY_COUNT = 7;
const FIRE_DEFAULT_BOOTSTRAP_PAGE_SIZE = 20;
const FIRE_POST_LOOKBACK_DAYS = 35;
const FIRE_ACTIVE_STATE_TTL_MS = 2 * 60 * 1000;
const FIRE_META_TTL_MS = 10 * 60 * 1000;
const FIRE_PAGE_TTL_MS = 5 * 60 * 1000;
const FIRE_LIVE_PAGE_TTL_MS = 15 * 1000;
const FIRE_BOOTSTRAP_PREFETCH_DAY_COUNT = 3;
const FIRE_MAX_BOOTSTRAP_PREFETCH_DAY_COUNT = 3;
const FIRE_CACHE_VERSION = 'v10';
const FIRE_THUMBNAIL_ASSET_ROLES = [
  'thumbnail',
  'display',
  'carousel_0',
  'carousel_00',
  'carousel_01',
  'carousel_1',
];
const EMPTY_MEDIA_SOURCE_HASH = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';
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

function mediaRouteUrlForPostKey(postKey: string | null | undefined, role = 'thumbnail'): string | null {
  const key = typeof postKey === 'string' ? postKey.trim() : '';
  if (!key) return null;
  const params = new URLSearchParams({ postKey: key, role, v: FIRE_CACHE_VERSION });
  return `/api/media?${params.toString()}`;
}

function candidatePostKeys(postKey: string): string[] {
  const trimmed = (postKey || '').trim();
  const withoutHash = trimmed.split('#')[0] || '';
  const lower = trimmed.toLowerCase();
  const lowerWithoutHash = lower.split('#')[0] || '';
  return Array.from(new Set([trimmed, withoutHash, lower, lowerWithoutHash].filter(Boolean)));
}

function previewCaptureAllowedForBusinessDay(businessDay: string | null | undefined): boolean {
  const day = typeof businessDay === 'string' ? businessDay.trim() : '';
  if (!day || !PREVIEW_CAPTURE_START_DAY) return true;
  return day >= PREVIEW_CAPTURE_START_DAY;
}

function isViewsMetricSupported(mediaType: string | null | undefined): boolean {
  const normalized = String(mediaType || '').trim().toLowerCase();
  return normalized === 'reel' || normalized === 'video';
}

function metricValueFromPostMetric(row: FirePostMetricRow, metric: FireMetricKey, mediaType?: string | null): number | null {
  if (metric === 'views' && !isViewsMetricSupported(mediaType)) return null;
  if (metric === 'views') return nullableNumber(row.views);
  if (metric === 'likes') return nullableNumber(row.likes);
  return nullableNumber(row.comments);
}

function metricPercentileFromPostMetric(row: FirePostMetricRow, metric: FireMetricKey, mediaType?: string | null): number | null {
  if (metric === 'views' && !isViewsMetricSupported(mediaType)) return null;
  if (metric === 'views') return nullableNumber(row.views_percentile);
  if (metric === 'likes') return nullableNumber(row.likes_percentile);
  return nullableNumber(row.comments_percentile);
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

function readableText(value: unknown): string {
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    const record = value as Record<string, unknown>;
    return readableText(record.text)
      || readableText(record.summary)
      || readableText(record.label)
      || readableText(record.value)
      || readableText(record.note);
  }
  return '';
}

function readableList(value: unknown, max = 4): string[] {
  const source = Array.isArray(value) ? value : value ? [value] : [];
  return Array.from(
    new Set(
      source
        .map(readableText)
        .map((entry) => entry.replace(/\s+/g, ' ').trim())
        .filter(Boolean),
    ),
  ).slice(0, max);
}

function compactFingerprintLine(label: string, value: unknown): string | null {
  const textValue = readableText(value).replace(/\s+/g, ' ').trim();
  if (!textValue) return null;
  return `${label}: ${textValue}`;
}

function hasValidFingerprintMedia(row: FireIntelligenceRow | undefined): boolean {
  const mediaSourceHash = nullableString(row?.fingerprint_media_source_hash);
  return Boolean(mediaSourceHash && mediaSourceHash !== EMPTY_MEDIA_SOURCE_HASH);
}

function buildFingerprintIntelligencePayload(row: FireIntelligenceRow | undefined): Record<string, unknown> | null {
  if (!hasValidFingerprintMedia(row)) return null;
  const fingerprint = recordValue(row?.fingerprint);
  if (Object.keys(fingerprint).length === 0) return null;

  const observed = recordValue(fingerprint.observed);
  const synthesis = recordValue(fingerprint.synthesis);
  const visualRead = recordValue(fingerprint.visual_read);
  const captionRead = recordValue(fingerprint.caption_read);
  const clustering = recordValue(fingerprint.pool_clustering_fields);
  const structure = recordValue(clustering.structure);
  const receiptLines = Array.isArray(clustering.structural_receipts)
    ? clustering.structural_receipts.flatMap((item) => {
      const rec = recordValue(item);
      const receipt = readableText(rec.receipt);
      const evidence = readableList(rec.evidence, 3);
      return receipt ? [receipt, ...evidence] : evidence;
    })
    : [];
  const summary =
    readableText(clustering.dominant_tension)
    || readableText(synthesis.subject)
    || readableText(fingerprint.content_summary)
    || readableText(fingerprint.topic)
    || readableText(visualRead.subject_focus);
  const lines = [
    compactFingerprintLine('Entry', structure.entry_state),
    compactFingerprintLine('Progression', structure.progression),
    compactFingerprintLine('Shift', structure.shift),
    compactFingerprintLine('Ending', structure.ending_state),
    compactFingerprintLine('Caption role', clustering.caption_connection),
    compactFingerprintLine('Interaction surface', clustering.interaction_surface),
    compactFingerprintLine('Visual sequence', clustering.visual_sequence),
    compactFingerprintLine('Audio', clustering.audio_behavior),
    compactFingerprintLine('Craft', synthesis.craft),
    compactFingerprintLine('Voice', synthesis.voice),
    compactFingerprintLine('Proof', synthesis.proof),
    compactFingerprintLine('Audio', observed.audio_notes),
    compactFingerprintLine('Visual notes', observed.visual_notes),
    compactFingerprintLine('Hook', fingerprint.hook || fingerprint.opener),
    compactFingerprintLine('Payoff', fingerprint.payoff),
    compactFingerprintLine('Visual read', fingerprint.visual_sequence || visualRead.opening_frame || visualRead.subject_focus),
    compactFingerprintLine('Emotional pull', fingerprint.emotional_trigger || captionRead.emotional_register),
    compactFingerprintLine('Caption role', fingerprint.caption_role || captionRead.cta_style),
    ...readableList(fingerprint.craft_moves, 2).map((line) => `Craft: ${line}`),
    ...readableList(fingerprint.campaign_or_context_clues, 2).map((line) => `Context clue: ${line}`),
    ...receiptLines.slice(0, 5).map((line) => `Receipt: ${line}`),
  ].filter((line): line is string => Boolean(line));

  if (!summary && lines.length === 0) return null;

  return {
    source: 'post_fingerprint',
    source_label: 'Fingerprint',
    model_version: row?.fingerprint_model_version ?? null,
    matches: summary ? [summary] : lines.slice(0, 1),
    deviates: summary ? lines.slice(0, 5) : lines.slice(1, 6),
    unclear: [],
    notes: readableList(fingerprint.discussion_prompt, 1),
  };
}

function buildPostIntelligencePayload(row: FireIntelligenceRow | undefined): Record<string, unknown> | null {
  return buildFingerprintIntelligencePayload(row);
}

function d7ReadBody(value: unknown): Record<string, unknown> {
  const parsed = recordValue(value);
  const direct = recordValue(parsed.d7_read);
  if (Object.keys(direct).length > 0) return direct;
  return parsed;
}

function buildD7ReadPayload(row: FireD7ReadRow | undefined): Record<string, unknown> | null {
  if (!row?.parsed_output) return null;
  const body = d7ReadBody(row.parsed_output);
  const scene = readableText(body.scene);
  const recentRun = readableText(body.recent_run);
  // v11 renamed memory_match -> fit. v13 dropped the LLM metric field for a
  // worker-computed fun_fact box ({kind, text}). Fall back to legacy keys so
  // cards cached before these changes still render.
  const memoryMatch = readableText(body.fit) || readableText(body.memory_match);
  const funFact = recordValue(body.fun_fact);
  // numbers == the worker-computed fun_fact stat (its own section now).
  const numbers =
    readableText(funFact.text) || readableText(body.metric) || readableText(body.numbers);
  // headline == the LLM's 3-6 word teaser (v16+). Older reads have none — the
  // card falls back to the fun_fact stat for its preview line.
  const headline = readableText(body.headline);
  const metricContext = readableText(body.metric_context);
  const read = scene || readableText(body.read);
  const direction = [recentRun, memoryMatch].filter(Boolean).join('\n\n') || readableText(body.direction);
  if (!headline && !numbers && !metricContext && !read && !direction) return null;

  return {
    source: 'd7_read',
    source_label: 'D7 Post Mortem',
    model_version: row.model ? `openrouter:${row.model}:${row.prompt_version || D7_READ_PROMPT_VERSION}` : row.prompt_version || D7_READ_PROMPT_VERSION,
    scene,
    recent_run: recentRun,
    fit: memoryMatch,
    fun_fact: Object.keys(funFact).length > 0 ? funFact : null,
    memory_match: memoryMatch,
    numbers,
    headline,
    metric_context: metricContext,
    read,
    direction,
    matches: [headline, read].filter(Boolean),
    deviates: [recentRun, memoryMatch].filter(Boolean),
    unclear: [],
    notes: [],
  };
}

function isHotPercentile(value: number | null): boolean {
  return value != null && Number.isFinite(value) && value <= HOT_PERCENTILE_MAX;
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
  card_kind?: 'tracking';
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
  post_read?: Record<string, unknown> | null;
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
  fingerprint: Record<string, unknown> | null;
  fingerprint_model_version: string | null;
  fingerprint_media_source_hash: string | null;
  fingerprint_media_confidence: string | null;
};

type FireD7ReadRow = {
  post_key: string | null;
  model: string | null;
  prompt_version: string | null;
  parsed_output: Record<string, unknown> | null;
  updated_at: string | null;
};

type FirePostFingerprintRow = {
  post_key: string | null;
  fingerprint: Record<string, unknown> | null;
  model_version: string | null;
  media_source_hash: string | null;
  media_confidence: string | null;
};

type MediaAssetUrlRow = {
  post_key: string | null;
  asset_role: string | null;
  storage_provider: string | null;
  storage_path: string | null;
  mime_type: string | null;
  purge_after: string | null;
};

type ResolvedMediaUrls = {
  thumbnailUrls: Map<string, string>;
  previewUrls: Map<string, string>;
};

type FireMetricKey = 'views' | 'likes' | 'comments';

function metricPreferenceOrder(mediaType: string | null): FireMetricKey[] {
  if (isViewsMetricSupported(mediaType)) return ['views', 'likes', 'comments'];
  return ['likes', 'comments'];
}

function rowMetricValue(row: AlertSurfaceRow, metric: FireMetricKey): number | null {
  if (metric === 'views' && !isViewsMetricSupported(row.media_type)) return null;
  return metric === 'views' ? nullableNumber(row.views) : metric === 'likes' ? nullableNumber(row.likes) : nullableNumber(row.comments);
}

function rowMetricBaseline(row: AlertSurfaceRow, metric: FireMetricKey): number | null {
  if (metric === 'views' && !isViewsMetricSupported(row.media_type)) return null;
  return metric === 'views'
    ? nullableNumber(row.views_baseline)
    : metric === 'likes'
      ? nullableNumber(row.likes_baseline)
      : nullableNumber(row.comments_baseline);
}

function rowMetricMultiple(row: AlertSurfaceRow, metric: FireMetricKey): number | null {
  if (metric === 'views' && !isViewsMetricSupported(row.media_type)) return null;
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
  const storedMetricMatchesBest = nullableString(row.metric_key)?.toLowerCase() === bestMetric;
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
      post_read: row.post_read ?? null,
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
    metric_value: bestMetricValue ?? (storedMetricMatchesBest ? nullableNumber(row.metric_value) : null),
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

function hasStoredR2Media(row: MediaAssetUrlRow): boolean {
  return row.storage_provider === 'r2' && Boolean(nullableString(row.storage_path));
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

  const originalsByCandidate = new Map<string, string[]>();
  const queryPostKeys = Array.from(
    new Set(
      uniquePostKeys.flatMap((originalPostKey) => candidatePostKeys(originalPostKey).map((candidatePostKey) => {
        const bucket = originalsByCandidate.get(candidatePostKey) || [];
        bucket.push(originalPostKey);
        originalsByCandidate.set(candidatePostKey, bucket);
        return candidatePostKey;
      })),
    ),
  );

  const rolePriority = new Map([
    ['thumbnail', 0],
    ['display', 1],
    ['carousel_0', 2],
    ['carousel_00', 2],
    ['carousel_01', 2],
    ['carousel_1', 2],
  ]);
  const chosenThumbnailPriority = new Map<string, number>();

  for (let start = 0; start < queryPostKeys.length; start += POST_KEY_CHUNK_SIZE) {
    const chunk = queryPostKeys.slice(start, start + POST_KEY_CHUNK_SIZE);
    const { data, error } = await sb
      .from('post_media_assets')
      .select('post_key,asset_role,storage_provider,storage_path,mime_type,purge_after')
      .in('post_key', chunk)
      .in('asset_role', [...FIRE_THUMBNAIL_ASSET_ROLES, 'preview_5s'])
      .in('status', ['active', 'purge_pending', 'pending_capture']);

    if (error) throw error;

    for (const row of (data || []) as MediaAssetUrlRow[]) {
      const postKey = nullableString(row.post_key);
      const role = nullableString(row.asset_role);
      if (!postKey || !role) continue;

      const purgeAfter = row.purge_after ? Date.parse(row.purge_after) : Number.POSITIVE_INFINITY;
      if (Number.isFinite(purgeAfter) && purgeAfter <= Date.now()) continue;
      if (!hasStoredR2Media(row)) continue;

      const mimeType = nullableString(row.mime_type)?.toLowerCase() || '';
      const originalPostKeys = originalsByCandidate.get(postKey) || [postKey];

      if (role === 'preview_5s') {
        if (mimeType && !mimeType.startsWith('video/')) continue;
        for (const originalPostKey of originalPostKeys) {
          const url = mediaRouteUrlForPostKey(originalPostKey, 'preview_5s');
          if (url) previewUrls.set(originalPostKey, url);
        }
        continue;
      }

      if (mimeType && !mimeType.startsWith('image/')) continue;
      const priority = rolePriority.get(role) ?? Number.POSITIVE_INFINITY;
      for (const originalPostKey of originalPostKeys) {
        const currentPriority = chosenThumbnailPriority.get(originalPostKey) ?? Number.POSITIVE_INFINITY;
        if (priority < currentPriority) {
          const url = mediaRouteUrlForPostKey(originalPostKey);
          if (url) {
            thumbnailUrls.set(originalPostKey, url);
            chosenThumbnailPriority.set(originalPostKey, priority);
          }
        }
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

  const rowsByPostKey = new Map<string, FireIntelligenceRow>();
  for (let start = 0; start < postKeys.length; start += POST_KEY_CHUNK_SIZE) {
    const chunk = postKeys.slice(start, start + POST_KEY_CHUNK_SIZE);
    const { data, error } = await sb
      .from('post_fingerprints')
      .select('post_key,fingerprint,model_version,media_source_hash,media_confidence')
      .in('post_key', chunk);

    if (error) throw error;
    for (const row of (data || []) as FirePostFingerprintRow[]) {
      const postKey = nullableString(row.post_key);
      if (!postKey) continue;
      rowsByPostKey.set(postKey, {
        post_key: postKey,
        fingerprint: recordValue(row.fingerprint),
        fingerprint_model_version: nullableString(row.model_version),
        fingerprint_media_source_hash: nullableString(row.media_source_hash),
        fingerprint_media_confidence: nullableString(row.media_confidence),
      });
    }

  }

  return Array.from(rowsByPostKey.values());
}

async function fetchD7ReadRowsForPostKeys(
  sb: { from: ReturnType<typeof createClient>['from'] },
  postKeys: string[],
): Promise<FireD7ReadRow[]> {
  if (postKeys.length === 0) return [];

  const rowsByPostKey = new Map<string, FireD7ReadRow>();
  for (let start = 0; start < postKeys.length; start += POST_KEY_CHUNK_SIZE) {
    const chunk = postKeys.slice(start, start + POST_KEY_CHUNK_SIZE);
    const { data, error } = await sb
      .from('feeder_file_model_calls')
      .select('post_key,model,prompt_version,parsed_output,updated_at')
      .eq('call_type', 'd7_read')
      .eq('status', 'complete')
      .eq('prompt_version', D7_READ_PROMPT_VERSION)
      .in('post_key', chunk)
      .order('updated_at', { ascending: false });

    if (error) throw error;
    for (const row of (data || []) as FireD7ReadRow[]) {
      const postKey = nullableString(row.post_key);
      if (!postKey || rowsByPostKey.has(postKey)) continue;
      rowsByPostKey.set(postKey, {
        post_key: postKey,
        model: nullableString(row.model),
        prompt_version: nullableString(row.prompt_version),
        parsed_output: recordValue(row.parsed_output),
        updated_at: nullableString(row.updated_at),
      });
    }
  }

  return Array.from(rowsByPostKey.values());
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
  d7ReadRows: FireD7ReadRow[];
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

  const d7ReadByPostKey = new Map<string, FireD7ReadRow>();
  for (const row of options.d7ReadRows) {
    const postKey = typeof row.post_key === 'string' ? row.post_key.trim() : '';
    if (postKey) d7ReadByPostKey.set(postKey, row);
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
    const intelligenceRow = intelligenceByPostKey.get(postKey);
    const postIntelligence = buildPostIntelligencePayload(intelligenceRow);
    const d7Read = checkpoint === 'd7' ? buildD7ReadPayload(d7ReadByPostKey.get(postKey)) : null;
    if (checkpoint === 'd7' && !d7Read) {
      continue;
    }
    const surfacePercentile = nullableNumber(metricRow.percentile_performance);
    const surfacePercentileExact = nullableNumber(metricRow.percentile_performance_exact) ?? surfacePercentile;
    const displayPercentile = surfacePercentileExact ?? surfacePercentile;
    const hasIntelligence = Boolean(d7Read || postIntelligence);
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
      intelligence_skipped: intelligenceRow?.fingerprint_model_version === 'skipped',
      post_read: d7Read || postIntelligence,
      is_hot: isHot,
      has_intelligence: hasIntelligence,
      hide_signal_chrome: true,
    } as AlertSurfaceRow;

    const bestMetric = deriveBestMetric(rowSeed);
    const storedMetricMatchesBest = nullableString(metricRow.ranking_metric)?.toLowerCase() === bestMetric;
    const bestValue = metricValueFromPostMetric(metricRow, bestMetric, mediaType)
      ?? (storedMetricMatchesBest ? nullableNumber(metricRow.metric_value) : null);
    const bestPercentile = storedMetricMatchesBest
      ? displayPercentile
      : metricPercentileFromPostMetric(metricRow, bestMetric, mediaType);
    const hourMultiple = (storedMetricMatchesBest ? nullableNumber(metricRow.hour_multiple) : null)
      ?? computeMultiple(bestValue, hourBaselineValueFromRow(hourBaseline, bestMetric));
    rowSeed.metric_key = bestMetric;
    rowSeed.metric_value = bestValue;
    rowSeed.surface_percentile = bestPercentile;
    rowSeed.surface_percentile_exact = bestPercentile;
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
  const [allMetricRows, intelligenceRows, d7ReadRows, baselineRows, hourBaselineRows] = await Promise.all([
    fetchMetricRowsForPostKeys(sb, dayPostKeys, { checkpoints: [...TRACKING_CHECKPOINTS] }),
    fetchIntelligenceRowsForPostKeys(sb, dayPostKeys),
    options.trackingCheckpoints.includes('d7')
      ? fetchD7ReadRowsForPostKeys(sb, dayPostKeys)
      : Promise.resolve([] as FireD7ReadRow[]),
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
    d7ReadRows,
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

function apiErrorMessage(error: unknown, fallback: string): string {
  if (process.env.NODE_ENV === 'production') return fallback;
  if (error instanceof Error && error.message) return error.message;
  if (error && typeof error === 'object') {
    const record = error as Record<string, unknown>;
    const message = typeof record.message === 'string' ? record.message.trim() : '';
    const details = typeof record.details === 'string' ? record.details.trim() : '';
    const hint = typeof record.hint === 'string' ? record.hint.trim() : '';
    const code = typeof record.code === 'string' ? record.code.trim() : '';
    return [message, details, hint, code && `code ${code}`].filter(Boolean).join(' · ') || fallback;
  }
  return fallback;
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
  return `fire:meta:${FIRE_CACHE_VERSION}:${userId}:${FIRE_BOOTSTRAP_DAY_COUNT}`;
}

function firePageCacheKey(userId: string, requestState: FirePageRequestState) {
  return [
    `fire:page:${FIRE_CACHE_VERSION}:${userId}`,
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

function firePageCacheTtlMs(requestState: FirePageRequestState) {
  return requestState.day === todayIstDayKey() ? FIRE_LIVE_PAGE_TTL_MS : FIRE_PAGE_TTL_MS;
}

function fireResponseCacheOptions(day: string) {
  return day === todayIstDayKey()
    ? { maxAgeSeconds: 5, staleWhileRevalidateSeconds: 15 }
    : { maxAgeSeconds: 60, staleWhileRevalidateSeconds: 600 };
}

async function buildCachedTrackingFirePagePayload(
  sb: { from: ReturnType<typeof createClient>['from'] },
  userId: string,
  activeState: FireActiveState,
  requestState: FirePageRequestState,
) {
  return withServerRouteCache(
    firePageCacheKey(userId, requestState),
    firePageCacheTtlMs(requestState),
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
  const rows = await loadTrackingFireRows(sb, {
    day: requestState.day,
    feederRows: activeState.normalizedFeeders,
    effectiveFeedIds: scope.effectiveFeedIds,
    effectiveFeederIds: scope.effectiveFeederIds,
    trackingCheckpoints: selectedTrackingCheckpoints,
  });

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
        ? storedThumbnailUrls.get(row.post_key) || mediaRouteUrlForPostKey(row.post_key) || nullableString(row.thumbnail_url) || null
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
      const initialEntry = prefetchedPages.find((entry) => {
        const rows = Array.isArray(entry.payload?.rows) ? entry.payload.rows.length : 0;
        const total = typeof entry.payload?.total === 'number' ? entry.payload.total : 0;
        return rows > 0 || total > 0;
      });
      const initialDay = initialEntry?.day || recentKeys[0] || todayIstDayKey();
      const initialPage = initialEntry?.payload
        || prefetchedPages.find((entry) => entry.day === initialDay)?.payload
        || prefetchedPages[0]?.payload
        || emptyFirePagePayload(initialDay, 0);
      return privateJsonResponse(request, {
        ...meta,
        initialDay,
        initialPage,
        prefetchedPages,
      }, fireResponseCacheOptions(initialDay));
    } catch (error) {
      console.error('[/api/fire?mode=bootstrap] Error:', error);
      const message = error instanceof Error ? error.message : 'Failed to bootstrap fire';
      return NextResponse.json({ error: message }, { status: 500 });
    }
  }

  try {
    const requestState = {
      day: params.get('day') || todayIstDayKey(),
      threshold: normalizeThresholdValue(params.get('threshold')),
      mediaFilter: normalizeMediaFilterValue(params.get('mediaFilter')),
      sort: normalizeSortValue(params.get('sort')),
      cursor: parseCursorValue(params.get('cursor')),
      pageSize: parsePageSizeValue(params.get('pageSize')),
      requestedFeedIds: parseCsvNumbers(params.get('feed_ids')),
      requestedFeederIds: parseCsvNumbers(params.get('feeder_ids')),
      requestedCheckpoints: parseCsvStrings(params.get('checkpoints')),
    };
    const payload = await buildCachedTrackingFirePagePayload(supabase, userId, activeState, requestState);
    return privateJsonResponse(request, payload, fireResponseCacheOptions(requestState.day));
  } catch (error) {
    console.error('[/api/fire] Error:', error);
    const message = apiErrorMessage(error, 'Failed to load fire alerts');
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
    const message = apiErrorMessage(error, 'Failed to load fire alerts');
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
