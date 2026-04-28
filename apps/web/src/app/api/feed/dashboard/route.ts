import { NextRequest, NextResponse } from 'next/server';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient } from '@/lib/supabase/server';
import { getPatternCueLabel, getPatternMechanicLabel } from '@/lib/fireSignals';
import { privateJsonResponse } from '@/lib/privateJsonResponse';
import { withServerRouteCache } from '@/lib/serverRouteCache';

export const dynamic = 'force-dynamic';
const DASHBOARD_ROUTE_TTL_MS = 10 * 60 * 1000;
const DASHBOARD_ROUTE_CACHE_VERSION = 'v3';

function adminClient() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const serviceRole = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !serviceRole) {
    throw new Error('Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  }
  return createSupabaseClient(url, serviceRole, {
    auth: { autoRefreshToken: false, persistSession: false },
  });
}

function nullableString(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value : null;
}

function nullableNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function recordValue(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function arrayValue<T = unknown>(value: unknown): T[] {
  return Array.isArray(value) ? (value as T[]) : [];
}

function humanizeSignalLabel(value: string): string {
  return String(value || 'Signal')
    .replace(/_/g, ' ')
    .toLowerCase()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function parseIstDateKey(value: string | null): Date | null {
  if (!value) return null;
  const [year, month, day] = value.split('-').map(Number);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  return new Date(Date.UTC(year, month - 1, day));
}

function formatIstDateKey(date: Date): string {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function addUtcDays(date: Date, days: number): Date {
  const next = new Date(date);
  next.setUTCDate(next.getUTCDate() + days);
  return next;
}

function istDayStartUtcIso(dayKey: string | null): string | null {
  if (!dayKey) return null;
  const [year, month, day] = dayKey.split('-').map(Number);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  return new Date(Date.UTC(year, month - 1, day, -5, -30, 0, 0)).toISOString();
}

function toIstDateKey(value: string | null): string | null {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Kolkata',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
}

type ScopedFeeder = {
  id: number;
  handle: string | null;
  follower_count: number | null;
};

type PostingPatternStatus = 'accelerating' | 'steady' | 'slowing' | 'dormant' | 'insufficient_data';

type PostingPatternPostRow = {
  feeder_id: number | string | null;
  posted_at: string | null;
  media_type: string | null;
};

type PostingPatternPost = {
  feederId: number;
  handle: string;
  postedAtIst: string;
  dayKey: string;
  timestamp: number;
  mediaType: string;
};

const MS_PER_DAY = 24 * 60 * 60 * 1000;
const MS_PER_HOUR = 60 * 60 * 1000;
const IST_OFFSET_MINUTES = 330;

async function fetchScopedFeeders(
  sb: ReturnType<typeof adminClient>,
  feedId: number,
  handle: string | null,
): Promise<ScopedFeeder[]> {
  const { data, error } = await sb
    .from('feeders')
    .select('id,handle,follower_count')
    .eq('feed_id', feedId)
    .eq('status', 'active');
  if (error) throw error;

  return ((data || []) as Array<Record<string, unknown>>)
    .map((row) => ({
      id: Number(row.id),
      handle: nullableString(row.handle),
      follower_count: nullableNumber(row.follower_count),
    }))
    .filter((row) => Number.isFinite(row.id))
    .filter((row) => !handle || row.handle?.toLowerCase() === handle);
}

function clampNumber(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

function roundNumber(value: number | null, digits = 1): number | null {
  if (value == null || !Number.isFinite(value)) return null;
  const scale = 10 ** digits;
  return Math.round(value * scale) / scale;
}

function median(values: number[]): number | null {
  const sorted = values.filter(Number.isFinite).sort((a, b) => a - b);
  if (sorted.length === 0) return null;
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[middle];
  return (sorted[middle - 1] + sorted[middle]) / 2;
}

function normalizeMediaType(value: string | null): string {
  const normalized = (value || '').trim().toLowerCase();
  if (!normalized) return 'unknown';
  if (normalized.includes('sidecar') || normalized.includes('carousel')) return 'sidecar/carousel';
  if (normalized.includes('reel') || normalized.includes('video')) return 'reel';
  if (normalized.includes('image') || normalized.includes('photo')) return 'image';
  return 'unknown';
}

function toIstDateTimeValue(value: string | null): string | null {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;

  const shifted = new Date(date.getTime() + IST_OFFSET_MINUTES * 60 * 1000);
  const year = shifted.getUTCFullYear();
  const month = String(shifted.getUTCMonth() + 1).padStart(2, '0');
  const day = String(shifted.getUTCDate()).padStart(2, '0');
  const hour = String(shifted.getUTCHours()).padStart(2, '0');
  const minute = String(shifted.getUTCMinutes()).padStart(2, '0');
  const second = String(shifted.getUTCSeconds()).padStart(2, '0');
  return `${year}-${month}-${day}T${hour}:${minute}:${second}+05:30`;
}

function medianGapHours(posts: PostingPatternPost[]): number | null {
  const timestamps = posts
    .map((post) => post.timestamp)
    .filter(Number.isFinite)
    .sort((a, b) => a - b);
  if (timestamps.length < 2) return null;

  const gaps: number[] = [];
  for (let index = 1; index < timestamps.length; index += 1) {
    gaps.push(Math.max(0, (timestamps[index] - timestamps[index - 1]) / MS_PER_HOUR));
  }
  return roundNumber(median(gaps), 1);
}

function consistencyScore(posts: PostingPatternPost[]): number {
  const timestamps = posts
    .map((post) => post.timestamp)
    .filter(Number.isFinite)
    .sort((a, b) => a - b);
  if (timestamps.length < 2) return 0;

  const gaps: number[] = [];
  for (let index = 1; index < timestamps.length; index += 1) {
    gaps.push(Math.max(0, (timestamps[index] - timestamps[index - 1]) / MS_PER_HOUR));
  }
  if (gaps.length === 0) return 0;

  const mean = gaps.reduce((sum, gap) => sum + gap, 0) / gaps.length;
  if (mean <= 0) return 92;
  const variance = gaps.reduce((sum, gap) => sum + ((gap - mean) ** 2), 0) / gaps.length;
  const coefficientOfVariation = Math.sqrt(variance) / mean;
  return Math.round(clampNumber(100 - coefficientOfVariation * 70, 8, 100));
}

function cadenceStatus(
  currentCount: number,
  baselineCount: number,
  currentPpw: number,
  baselinePpw: number,
  deltaPercent: number | null,
  daysSinceLastPost: number | null,
  usualGapHours: number | null,
): PostingPatternStatus {
  if (currentCount + baselineCount < 2) return 'insufficient_data';
  if (currentCount === 0 && baselineCount > 0) return 'dormant';
  if (
    daysSinceLastPost != null
    && daysSinceLastPost >= 2
    && usualGapHours != null
    && daysSinceLastPost * 24 > usualGapHours * 2.35
  ) {
    return 'dormant';
  }
  if (baselinePpw <= 0 && currentPpw > 0) return 'accelerating';
  if (deltaPercent != null && deltaPercent >= 25) return 'accelerating';
  if (deltaPercent != null && deltaPercent <= -25) return 'slowing';
  return 'steady';
}

function dominantMedia(posts: PostingPatternPost[]): { type: string | null; share: number } {
  if (posts.length === 0) return { type: null, share: 0 };
  const counts = new Map<string, number>();
  for (const post of posts) counts.set(post.mediaType, (counts.get(post.mediaType) || 0) + 1);

  let bestType: string | null = null;
  let bestCount = 0;
  for (const [type, count] of counts.entries()) {
    if (count > bestCount) {
      bestType = type;
      bestCount = count;
    }
  }

  return { type: bestType, share: bestCount / posts.length };
}

function mediaShare(posts: PostingPatternPost[], type: string | null): number {
  if (!type || posts.length === 0) return 0;
  const count = posts.reduce((sum, post) => sum + (post.mediaType === type ? 1 : 0), 0);
  return count / posts.length;
}

function buildRhythmDays(currentRows: PostingPatternPost[], startDate: Date, endDate: Date) {
  const counts = new Map<string, number>();
  for (const post of currentRows) counts.set(post.dayKey, (counts.get(post.dayKey) || 0) + 1);

  const days: Array<{ day_ist: string; post_count: number }> = [];
  for (let cursor = new Date(startDate); cursor.getTime() <= endDate.getTime(); cursor = addUtcDays(cursor, 1)) {
    const dayKey = formatIstDateKey(cursor);
    days.push({ day_ist: dayKey, post_count: counts.get(dayKey) || 0 });
  }
  return days;
}

function buildMediaMix(posts: PostingPatternPost[]) {
  if (posts.length === 0) return [];
  const counts = new Map<string, number>();
  for (const post of posts) {
    const type = post.mediaType || 'unknown';
    counts.set(type, (counts.get(type) || 0) + 1);
  }
  const total = posts.length;
  return Array.from(counts.entries())
    .filter(([type]) => type !== 'unknown')
    .map(([type, count]) => ({ type, count, pct: Math.round((count / total) * 100) }))
    .sort((a, b) => b.count - a.count);
}

async function fetchPostingPattern(
  sb: ReturnType<typeof adminClient>,
  feeders: ScopedFeeder[],
  windowStartIst: string | null,
  windowEndIst: string | null,
) {
  const currentStartDate = parseIstDateKey(windowStartIst);
  const currentEndDate = parseIstDateKey(windowEndIst);
  if (!currentStartDate || !currentEndDate || currentStartDate.getTime() > currentEndDate.getTime()) return null;

  if (feeders.length === 0) return null;

  const currentWindowDays = Math.max(1, Math.round((currentEndDate.getTime() - currentStartDate.getTime()) / MS_PER_DAY) + 1);
  const baselineStartDate = addUtcDays(currentStartDate, -currentWindowDays);
  const maxLookbackStartDate = addUtcDays(addUtcDays(currentEndDate, 1), -180);
  const effectiveBaselineStartDate = baselineStartDate.getTime() < maxLookbackStartDate.getTime()
    ? maxLookbackStartDate
    : baselineStartDate;
  const baselineStartIst = formatIstDateKey(effectiveBaselineStartDate);
  const endExclusiveIst = formatIstDateKey(addUtcDays(currentEndDate, 1));
  const startIso = istDayStartUtcIso(baselineStartIst);
  const endExclusiveIso = istDayStartUtcIso(endExclusiveIst);
  if (!startIso || !endExclusiveIso) return null;

  const feederById = new Map<number, ScopedFeeder>();
  for (const feeder of feeders) feederById.set(feeder.id, feeder);

  const feederIds = feeders.map((feeder) => feeder.id);
  const rows: PostingPatternPostRow[] = [];
  for (let start = 0; ; start += 1000) {
    const { data, error } = await sb
      .from('posts')
      .select('feeder_id,posted_at,media_type')
      .in('feeder_id', feederIds)
      .gte('posted_at', startIso)
      .lt('posted_at', endExclusiveIso)
      .order('posted_at', { ascending: false })
      .range(start, start + 999);

    if (error) throw error;
    const batch = (data || []) as PostingPatternPostRow[];
    rows.push(...batch);
    if (batch.length < 1000) break;
  }

  const posts: PostingPatternPost[] = [];
  for (const row of rows) {
    const feederId = Number(row.feeder_id);
    const postedAt = nullableString(row.posted_at);
    const timestamp = postedAt ? Date.parse(postedAt) : NaN;
    if (!Number.isFinite(feederId) || !postedAt || !Number.isFinite(timestamp)) continue;

    const dayKey = toIstDateKey(postedAt);
    const feeder = feederById.get(feederId);
    if (!dayKey || !feeder) continue;

    posts.push({
      feederId,
      handle: feeder.handle || `feeder-${feederId}`,
      postedAtIst: toIstDateTimeValue(postedAt) || postedAt,
      dayKey,
      timestamp,
      mediaType: normalizeMediaType(row.media_type),
    });
  }

  const currentRows = posts.filter((post) => post.dayKey >= windowStartIst! && post.dayKey <= windowEndIst!);
  const baselineRows = posts.filter((post) => post.dayKey >= baselineStartIst && post.dayKey < windowStartIst!);
  const baselineWindowDays = Math.max(1, Math.round((currentStartDate.getTime() - effectiveBaselineStartDate.getTime()) / MS_PER_DAY));
  const currentWeeks = currentWindowDays / 7;
  const baselineWeeks = baselineWindowDays / 7;
  const currentPpw = currentRows.length / currentWeeks;
  const baselinePpw = baselineRows.length / baselineWeeks;
  const deltaPercent = baselinePpw > 0 ? ((currentPpw - baselinePpw) / baselinePpw) * 100 : null;
  const latestPost = posts.reduce<PostingPatternPost | null>((latest, post) => {
    if (!latest || post.timestamp > latest.timestamp) return post;
    return latest;
  }, null);
  const daysSinceLastPost = latestPost
    ? Math.max(0, Math.floor((Date.now() - latestPost.timestamp) / MS_PER_DAY))
    : null;
  const currentMedianGap = medianGapHours(currentRows);
  const usualGap = medianGapHours(baselineRows) ?? medianGapHours(posts);
  const gapVsUsual = usualGap != null && usualGap > 0 && currentMedianGap != null
    ? ((currentMedianGap - usualGap) / usualGap) * 100
    : null;
  const currentDominant = dominantMedia(currentRows);
  const baselineDominant = dominantMedia(baselineRows);
  const overallStatus = cadenceStatus(
    currentRows.length,
    baselineRows.length,
    currentPpw,
    baselinePpw,
    deltaPercent,
    daysSinceLastPost,
    usualGap,
  );

  const currentByFeeder = new Map<number, PostingPatternPost[]>();
  const baselineByFeeder = new Map<number, PostingPatternPost[]>();
  for (const post of currentRows) {
    const bucket = currentByFeeder.get(post.feederId) || [];
    bucket.push(post);
    currentByFeeder.set(post.feederId, bucket);
  }
  for (const post of baselineRows) {
    const bucket = baselineByFeeder.get(post.feederId) || [];
    bucket.push(post);
    baselineByFeeder.set(post.feederId, bucket);
  }

  const feederRows = feeders
    .map((feeder) => {
      const current = currentByFeeder.get(feeder.id) || [];
      const baseline = baselineByFeeder.get(feeder.id) || [];
      const feederCurrentPpw = current.length / currentWeeks;
      const feederBaselinePpw = baseline.length / baselineWeeks;
      const feederDelta = feederBaselinePpw > 0 ? ((feederCurrentPpw - feederBaselinePpw) / feederBaselinePpw) * 100 : null;
      const latest = [...current, ...baseline].reduce<PostingPatternPost | null>((candidate, post) => {
        if (!candidate || post.timestamp > candidate.timestamp) return post;
        return candidate;
      }, null);
      const latestAgeDays = latest
        ? Math.max(0, Math.floor((Date.now() - latest.timestamp) / MS_PER_DAY))
        : null;
      const feederUsualGap = medianGapHours(baseline) ?? medianGapHours([...current, ...baseline]);
      const feederMedia = dominantMedia(current).type ?? dominantMedia(baseline).type;
      const feederStatus = cadenceStatus(
        current.length,
        baseline.length,
        feederCurrentPpw,
        feederBaselinePpw,
        feederDelta,
        latestAgeDays,
        feederUsualGap,
      );

      return {
        feeder_id: feeder.id,
        handle: feeder.handle || `feeder-${feeder.id}`,
        posts_per_week_current: roundNumber(feederCurrentPpw, 1) || 0,
        delta_percent: roundNumber(feederDelta, 0),
        days_since_last_post: latestAgeDays,
        median_gap_hours: medianGapHours(current),
        dominant_media_type: feederMedia,
        status: feederStatus,
      };
    })
    .filter((row) => row.posts_per_week_current > 0 || row.delta_percent != null || row.days_since_last_post != null)
    .sort((a, b) => {
      if (overallStatus === 'dormant' || overallStatus === 'slowing') {
        return (b.days_since_last_post ?? -1) - (a.days_since_last_post ?? -1);
      }
      const aDelta = Math.abs(a.delta_percent ?? (a.posts_per_week_current > 0 ? 100 : 0));
      const bDelta = Math.abs(b.delta_percent ?? (b.posts_per_week_current > 0 ? 100 : 0));
      if (bDelta !== aDelta) return bDelta - aDelta;
      return b.posts_per_week_current - a.posts_per_week_current;
    })
    .slice(0, 8);

  return {
    status: overallStatus,
    posts_per_week_current: roundNumber(currentPpw, 1) || 0,
    posts_per_week_baseline: roundNumber(baselinePpw, 1) || 0,
    delta_percent: roundNumber(deltaPercent, 0),
    last_post_at_ist: latestPost?.postedAtIst ?? null,
    days_since_last_post: daysSinceLastPost,
    median_gap_hours: currentMedianGap,
    usual_gap_hours: usualGap,
    gap_vs_usual_percent: roundNumber(gapVsUsual, 0),
    consistency_score: consistencyScore(currentRows),
    media_shift: {
      current_dominant_type: currentDominant.type,
      baseline_dominant_type: baselineDominant.type,
      share_delta: currentDominant.type
        ? roundNumber((currentDominant.share - mediaShare(baselineRows, currentDominant.type)) * 100, 0)
        : null,
    },
    media_mix: buildMediaMix(currentRows),
    rhythm_days: buildRhythmDays(currentRows, currentStartDate, currentEndDate),
    feeder_rows: feederRows,
  };
}

function buildApexMixFromPostingPattern(pattern: Awaited<ReturnType<typeof fetchPostingPattern>>) {
  if (!pattern?.media_mix?.length) return null;
  const total = pattern.media_mix.reduce((sum, row) => sum + Math.max(0, Number(row.count) || 0), 0);
  if (total <= 0) return null;
  return pattern.media_mix.map((row) => ({
    media_type: row.type,
    post_count: row.count,
    share: Number((Math.max(0, Number(row.count) || 0) / total).toFixed(4)),
  }));
}

async function fetchRollingHeatmap(
  sb: ReturnType<typeof adminClient>,
  feedId: number,
  handle: string | null,
) {
  const { data, error } = await sb.rpc('fn_feed_dashboard', {
    p_feed_id: feedId,
    p_weeks: 90,
    p_handle: handle,
  });
  if (error) throw error;
  return arrayValue(recordValue(data).heatmap_daily);
}

type KillzonePostRow = {
  post_key: string | null;
  posted_at: string | null;
};

type KillzoneMetricRow = {
  post_key: string | null;
  checkpoint: string | null;
  computed_at: string | null;
  percentile_performance: number | string | null;
};

type DashboardMediaType = 'all' | 'reel' | 'image' | 'carousel' | 'unknown';

type EngagementPostRow = {
  post_key: string | null;
  media_type: string | null;
};

type EngagementMetricRow = {
  post_key: string | null;
  checkpoint: string | null;
  computed_at: string | null;
  views: number | string | null;
  likes: number | string | null;
  comments: number | string | null;
};

type EngagementLatestMetric = {
  rank: number;
  computedAt: number;
  views: number | null;
  likes: number | null;
  comments: number | null;
};

type EngagementBucket = {
  media_type: DashboardMediaType;
  post_count: number;
  metric_count: number;
  likes_total: number;
  likes_count: number;
  comments_total: number;
  comments_count: number;
  views_total: number;
  views_count: number;
};

const ENGAGEMENT_MEDIA_ORDER = ['all', 'reel', 'image', 'carousel'] as const;

function normalizeDashboardMediaType(value: string | null): Exclude<DashboardMediaType, 'all'> {
  const normalized = (value || '').trim().toLowerCase();
  if (!normalized) return 'unknown';
  if (normalized.includes('sidecar') || normalized.includes('carousel')) return 'carousel';
  if (normalized.includes('reel') || normalized.includes('video')) return 'reel';
  if (normalized.includes('image') || normalized.includes('photo')) return 'image';
  return 'unknown';
}

function createEngagementBucket(mediaType: DashboardMediaType): EngagementBucket {
  return {
    media_type: mediaType,
    post_count: 0,
    metric_count: 0,
    likes_total: 0,
    likes_count: 0,
    comments_total: 0,
    comments_count: 0,
    views_total: 0,
    views_count: 0,
  };
}

function pushEngagementMetric(bucket: EngagementBucket, metric: EngagementLatestMetric | null, includeViews = true) {
  if (!metric) return;
  const hasAnyMetric = metric.likes != null || metric.comments != null || (includeViews && metric.views != null);
  if (hasAnyMetric) bucket.metric_count += 1;
  if (metric.likes != null) {
    bucket.likes_total += metric.likes;
    bucket.likes_count += 1;
  }
  if (metric.comments != null) {
    bucket.comments_total += metric.comments;
    bucket.comments_count += 1;
  }
  if (includeViews && metric.views != null) {
    bucket.views_total += metric.views;
    bucket.views_count += 1;
  }
}

function roundedAverage(total: number, count: number): number | null {
  if (count <= 0) return null;
  return Math.round(total / count);
}

function serializeEngagementBucket(bucket: EngagementBucket) {
  return {
    media_type: bucket.media_type,
    post_count: bucket.post_count,
    metric_count: bucket.metric_count,
    avg_likes: roundedAverage(bucket.likes_total, bucket.likes_count),
    avg_comments: roundedAverage(bucket.comments_total, bucket.comments_count),
    avg_views: roundedAverage(bucket.views_total, bucket.views_count),
  };
}

async function fetchEngagementBreakdown(
  sb: ReturnType<typeof adminClient>,
  feederIds: number[],
  windowStartIst: string | null,
  windowEndIst: string | null,
) {
  const emptyBuckets = ENGAGEMENT_MEDIA_ORDER.map((type) => serializeEngagementBucket(createEngagementBucket(type)));
  const mediaTypeByPostKey = new Map<string, Exclude<DashboardMediaType, 'all'>>();

  if (!windowStartIst || !windowEndIst || feederIds.length === 0) {
    return { averages: emptyBuckets, mediaTypeByPostKey };
  }

  const startIso = istDayStartUtcIso(windowStartIst);
  const endDate = parseIstDateKey(windowEndIst);
  const endExclusiveIso = endDate ? istDayStartUtcIso(formatIstDateKey(addUtcDays(endDate, 1))) : null;
  if (!startIso || !endExclusiveIso) {
    return { averages: emptyBuckets, mediaTypeByPostKey };
  }

  const posts: EngagementPostRow[] = [];
  for (let start = 0; ; start += 1000) {
    const { data, error } = await sb
      .from('posts')
      .select('post_key,media_type')
      .in('feeder_id', feederIds)
      .gte('posted_at', startIso)
      .lt('posted_at', endExclusiveIso)
      .order('posted_at', { ascending: false })
      .range(start, start + 999);

    if (error) throw error;
    const batch = (data || []) as EngagementPostRow[];
    posts.push(...batch);
    if (batch.length < 1000) break;
  }

  const postKeys = Array.from(
    new Set(
      posts
        .map((row) => nullableString(row.post_key))
        .filter((value): value is string => Boolean(value)),
    ),
  );

  const checkpointRank: Record<string, number> = { d1: 1, d3: 2, d7: 3, d21: 4 };
  const latestMetricByPost = new Map<string, EngagementLatestMetric>();

  for (let start = 0; start < postKeys.length; start += 250) {
    const { data, error } = await sb
      .from('post_metrics')
      .select('post_key,checkpoint,computed_at,views,likes,comments')
      .in('post_key', postKeys.slice(start, start + 250))
      .in('checkpoint', ['d1', 'd3', 'd7', 'd21']);

    if (error) throw error;

    for (const row of (data || []) as EngagementMetricRow[]) {
      const postKey = nullableString(row.post_key);
      const checkpoint = nullableString(row.checkpoint)?.toLowerCase() || '';
      if (!postKey || !checkpointRank[checkpoint]) continue;

      const rank = checkpointRank[checkpoint];
      const computedAt = Date.parse(row.computed_at || '') || 0;
      const current = latestMetricByPost.get(postKey);
      if (!current || rank > current.rank || (rank === current.rank && computedAt > current.computedAt)) {
        latestMetricByPost.set(postKey, {
          rank,
          computedAt,
          views: nullableNumber(row.views),
          likes: nullableNumber(row.likes),
          comments: nullableNumber(row.comments),
        });
      }
    }
  }

  const buckets = new Map<DashboardMediaType, EngagementBucket>();
  for (const type of ENGAGEMENT_MEDIA_ORDER) buckets.set(type, createEngagementBucket(type));

  for (const post of posts) {
    const postKey = nullableString(post.post_key);
    const mediaType = normalizeDashboardMediaType(post.media_type);
    if (postKey) mediaTypeByPostKey.set(postKey, mediaType);

    const metric = postKey ? latestMetricByPost.get(postKey) ?? null : null;
    const includeViews = mediaType === 'reel';
    const allBucket = buckets.get('all');
    if (allBucket) {
      allBucket.post_count += 1;
      pushEngagementMetric(allBucket, metric, includeViews);
    }

    if (mediaType === 'unknown') continue;
    const bucket = buckets.get(mediaType);
    if (!bucket) continue;
    bucket.post_count += 1;
    pushEngagementMetric(bucket, metric, includeViews);
  }

  return {
    averages: ENGAGEMENT_MEDIA_ORDER.map((type) => serializeEngagementBucket(buckets.get(type) || createEngagementBucket(type))),
    mediaTypeByPostKey,
  };
}

async function fetchKillzoneDays(
  sb: ReturnType<typeof adminClient>,
  feederIds: number[],
  windowStartIst: string | null,
  windowEndIst: string | null,
) {
  if (!windowStartIst || !windowEndIst) return [];

  const startIso = istDayStartUtcIso(windowStartIst);
  const endDate = parseIstDateKey(windowEndIst);
  const endExclusiveIso = endDate ? istDayStartUtcIso(formatIstDateKey(addUtcDays(endDate, 1))) : null;
  if (!startIso || !endExclusiveIso) return [];

  if (feederIds.length === 0) return [];

  const posts: KillzonePostRow[] = [];
  for (let start = 0; ; start += 1000) {
    const { data, error } = await sb
      .from('posts')
      .select('post_key,posted_at')
      .in('feeder_id', feederIds)
      .gte('posted_at', startIso)
      .lt('posted_at', endExclusiveIso)
      .order('posted_at', { ascending: false })
      .range(start, start + 999);

    if (error) throw error;
    const batch = (data || []) as KillzonePostRow[];
    posts.push(...batch);
    if (batch.length < 1000) break;
  }

  const postKeys = Array.from(
    new Set(
      posts
        .map((row) => nullableString(row.post_key))
        .filter((value): value is string => Boolean(value)),
    ),
  );

  const checkpointRank: Record<string, number> = { d1: 1, d3: 2, d7: 3, d21: 4 };
  const latestMetricByPost = new Map<string, { rank: number; computedAt: number; percentile: number | null }>();

  for (let start = 0; start < postKeys.length; start += 250) {
    const { data, error } = await sb
      .from('post_metrics')
      .select('post_key,checkpoint,computed_at,percentile_performance')
      .in('post_key', postKeys.slice(start, start + 250))
      .in('checkpoint', ['d1', 'd3', 'd7', 'd21']);

    if (error) throw error;

    for (const row of (data || []) as KillzoneMetricRow[]) {
      const postKey = nullableString(row.post_key);
      const checkpoint = nullableString(row.checkpoint)?.toLowerCase() || '';
      if (!postKey || !checkpointRank[checkpoint]) continue;

      const rank = checkpointRank[checkpoint];
      const computedAt = Date.parse(row.computed_at || '') || 0;
      const percentile = nullableNumber(row.percentile_performance);
      const current = latestMetricByPost.get(postKey);
      if (!current || rank > current.rank || (rank === current.rank && computedAt > current.computedAt)) {
        latestMetricByPost.set(postKey, { rank, computedAt, percentile });
      }
    }
  }

  const dayLabels = ['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'];
  const buckets = dayLabels.map((label, day) => ({
    day_of_week: day,
    day_label: label,
    post_count: 0,
    metric_count: 0,
    total_percentile: 0,
  }));

  for (const post of posts) {
    const dayKey = toIstDateKey(post.posted_at);
    const parsedDay = parseIstDateKey(dayKey);
    const postKey = nullableString(post.post_key);
    if (!parsedDay || !postKey) continue;

    const day = parsedDay.getUTCDay();
    const bucket = buckets[day];
    bucket.post_count += 1;

    const metric = latestMetricByPost.get(postKey);
    if (metric?.percentile != null) {
      bucket.metric_count += 1;
      bucket.total_percentile += metric.percentile;
    }
  }

  return buckets.map(({ total_percentile: _total, ...bucket }) => ({
    ...bucket,
    avg_percentile_performance: bucket.metric_count > 0
      ? Number((_total / bucket.metric_count).toFixed(2))
      : null,
  }));
}

type PatternBoardCue = string;
type PatternBoardSupport = {
  post_key: string;
  handle: string | null;
  post_url: string | null;
  thumbnail_url: string | null;
  media_type: string | null;
};

const PATTERN_BOARD_LIMIT = 10;
const PATTERN_BOARD_CUE_MAX = 4;
const PATTERN_BOARD_SUPPORT_MAX = 6;

async function hydratePatternSupportPreviews(
  sb: ReturnType<typeof adminClient>,
  postKeys: string[],
): Promise<Map<string, PatternBoardSupport>> {
  const previews = new Map<string, PatternBoardSupport>();
  if (postKeys.length === 0) return previews;

  const unique = Array.from(new Set(postKeys.filter(Boolean)));
  if (unique.length === 0) return previews;

  const { data, error } = await sb
    .from('posts')
    .select('post_key,feeder_id,media_type,post_url,thumbnail_url')
    .in('post_key', unique);
  if (error) throw error;

  const rows = (data || []) as Array<{
    post_key: string | null;
    feeder_id: number | string | null;
    media_type: string | null;
    post_url: string | null;
    thumbnail_url: string | null;
  }>;

  const feederIds = new Set<number>();
  for (const row of rows) {
    const fid = Number(row.feeder_id);
    if (Number.isFinite(fid)) feederIds.add(fid);
  }

  const handleByFeederId = new Map<number, string | null>();
  if (feederIds.size > 0) {
    const { data: feederRows, error: feederErr } = await sb
      .from('feeders')
      .select('id,handle')
      .in('id', Array.from(feederIds));
    if (feederErr) throw feederErr;
    for (const row of (feederRows || []) as Array<{ id: number | string | null; handle: string | null }>) {
      const fid = Number(row.id);
      if (Number.isFinite(fid)) handleByFeederId.set(fid, nullableString(row.handle));
    }
  }

  for (const row of rows) {
    const postKey = nullableString(row.post_key);
    if (!postKey) continue;
    const fid = Number(row.feeder_id);
    previews.set(postKey, {
      post_key: postKey,
      handle: Number.isFinite(fid) ? handleByFeederId.get(fid) || null : null,
      post_url: nullableString(row.post_url),
      thumbnail_url: nullableString(row.thumbnail_url),
      media_type: nullableString(row.media_type),
    });
  }

  return previews;
}

async function fetchPatternBoard(
  sb: ReturnType<typeof adminClient>,
  feedId: number,
  feederIds: number[],
  windowStartIst: string | null,
  windowEndIst: string | null,
) {
  if (!windowStartIst || !windowEndIst) return [];
  if (feederIds.length === 0) return [];

  const { data, error } = await sb
    .from('signals')
    .select('id,signal_type,scope,business_date_ist,feeder_id,metric_snapshot')
    .eq('feed_id', feedId)
    .in('status', ['pending', 'fresh', 'stale'])
    .gte('business_date_ist', windowStartIst)
    .lte('business_date_ist', windowEndIst)
    .order('business_date_ist', { ascending: false })
    .limit(300);

  if (error) throw error;
  const signalRows = ((data || []) as Array<Record<string, unknown>>)
    .filter((row) => {
      const feederId = nullableNumber(row.feeder_id);
      return feederId == null || feederIds.includes(feederId);
    });
  const signalIds = signalRows
    .map((row) => nullableNumber(row.id))
    .filter((value): value is number => value != null);
  const supportBySignal = new Map<number, string[]>();
  if (signalIds.length > 0) {
    const { data: postRows, error: postError } = await sb
      .from('signal_posts')
      .select('signal_id,post_key,cohort,rank')
      .in('signal_id', signalIds)
      .order('rank', { ascending: true });
    if (postError) throw postError;
    for (const row of (postRows || []) as Array<Record<string, unknown>>) {
      const signalId = nullableNumber(row.signal_id);
      const postKey = nullableString(row.post_key);
      if (signalId == null || !postKey) continue;
      const bucket = supportBySignal.get(signalId) || [];
      bucket.push(postKey);
      supportBySignal.set(signalId, bucket);
    }
  }
  const cardBySignal = new Map<number, Record<string, unknown>>();
  if (signalIds.length > 0) {
    const { data: cardRows, error: cardError } = await sb
      .from('signal_intelligence')
      .select('signal_id,card')
      .in('signal_id', signalIds);
    if (cardError) throw cardError;
    for (const row of (cardRows || []) as Array<Record<string, unknown>>) {
      const signalId = nullableNumber(row.signal_id);
      const card = recordValue(row.card);
      if (signalId == null || Object.keys(card).length === 0) continue;
      cardBySignal.set(signalId, card);
    }
  }

  const aggregate = new Map<string, {
    firewatch_id: string;
    signal_code: string;
    context: 'own' | 'cross' | 'anchor';
    pattern_name: string | null;
    pattern_label: string;
    trigger_count: number;
    avg_total: number;
    avg_count: number;
    feeders_count: number;
    baseline_share: number | null;
    recent_lift: number | null;
    anchor_gap: number | null;
    match_count: number | null;
    latest_business_day: string | null;
    latest_payload: Record<string, unknown> | null;
  }>();

  for (const row of signalRows) {
    const signalId = nullableNumber(row.id) || 0;
    const snapshot = recordValue(row.metric_snapshot);
    const card = cardBySignal.get(signalId) || {};
    const commonPattern = arrayValue<unknown>(card.common_pattern)
      .map((value) => nullableString(value))
      .filter((value): value is string => Boolean(value))
      .slice(0, PATTERN_BOARD_CUE_MAX);
    const payload = {
      pattern_name: nullableString(row.signal_type),
      pattern_label: nullableString(card.title),
      avg_hot_percentile: nullableNumber(snapshot.avg_percentile) ?? nullableNumber(snapshot.best_percentile),
      feeders_count: nullableNumber(snapshot.contributing_feeders) ?? nullableNumber(snapshot.affected_feeders),
      baseline_share: null,
      recent_lift: null,
      anchor_gap: nullableNumber(snapshot.gap),
      match_count: nullableNumber(snapshot.post_count) ?? (supportBySignal.get(signalId) || []).length,
      support_post_keys: supportBySignal.get(signalId) || [],
      required_cues: [],
      cues: commonPattern.map((label) => ({ key: 'common_pattern', value: label, label })),
      card,
    };
    const signalCode = nullableString(row.signal_type) || 'SIGNAL';
    const context = (nullableString(row.scope) || 'own') as 'own' | 'cross' | 'anchor';
    const patternName = nullableString(payload.pattern_name);
    const key = `${signalCode}:${patternName || 'unknown'}`;
    const current = aggregate.get(key) || {
      firewatch_id: key,
      signal_code: signalCode,
      context,
      pattern_name: patternName,
      pattern_label: nullableString(payload.pattern_label) || getPatternMechanicLabel(patternName) || humanizeSignalLabel(patternName || signalCode),
      trigger_count: 0,
      avg_total: 0,
      avg_count: 0,
      feeders_count: 0,
      baseline_share: null,
      recent_lift: null,
      anchor_gap: null,
      match_count: null,
      latest_business_day: null,
      latest_payload: null,
    };

    current.trigger_count += 1;
    const avg = nullableNumber(payload.avg_hot_percentile);
    if (avg != null) {
      current.avg_total += avg;
      current.avg_count += 1;
    }
    const feedersCount = nullableNumber(payload.feeders_count);
    if (feedersCount != null) current.feeders_count = Math.max(current.feeders_count, Math.round(feedersCount));

    const baselineShare = nullableNumber(payload.baseline_share);
    if (baselineShare != null) {
      current.baseline_share = current.baseline_share == null ? baselineShare : Math.min(current.baseline_share, baselineShare);
    }

    const recentLift = nullableNumber(payload.recent_lift);
    if (recentLift != null) {
      current.recent_lift = current.recent_lift == null ? recentLift : Math.max(current.recent_lift, recentLift);
    }

    const anchorGap = nullableNumber(payload.anchor_gap);
    if (anchorGap != null) {
      current.anchor_gap = current.anchor_gap == null ? anchorGap : Math.max(current.anchor_gap, anchorGap);
    }

    const matchCount = nullableNumber(payload.match_count);
    if (matchCount != null) {
      current.match_count = current.match_count == null ? matchCount : Math.max(current.match_count, matchCount);
    }

    const businessDay = nullableString(row.business_date_ist);
    if (businessDay && (!current.latest_business_day || businessDay > current.latest_business_day)) {
      current.latest_business_day = businessDay;
      current.latest_payload = payload;
    }
    if (!current.latest_payload) current.latest_payload = payload;

    aggregate.set(key, current);
  }

  const ranked = Array.from(aggregate.values())
    .sort((a, b) => {
      const dayDiff = (b.latest_business_day || '').localeCompare(a.latest_business_day || '');
      if (dayDiff !== 0) return dayDiff;
      const triggerDiff = b.trigger_count - a.trigger_count;
      if (triggerDiff !== 0) return triggerDiff;
      return (a.avg_count > 0 ? a.avg_total / a.avg_count : 999) - (b.avg_count > 0 ? b.avg_total / b.avg_count : 999);
    })
    .slice(0, PATTERN_BOARD_LIMIT);

  // Collect support post keys across all top entries (use required_cues priority)
  const supportKeysByEntry = new Map<string, string[]>();
  const allKeys: string[] = [];
  for (const entry of ranked) {
    const payload = entry.latest_payload || {};
    const supportKeys = arrayValue<unknown>(payload.support_post_keys)
      .map((value) => nullableString(value))
      .filter((value): value is string => Boolean(value))
      .slice(0, PATTERN_BOARD_SUPPORT_MAX);
    supportKeysByEntry.set(entry.firewatch_id, supportKeys);
    for (const key of supportKeys) allKeys.push(key);
  }

  const supportPreviewByKey = await hydratePatternSupportPreviews(sb, allKeys);

  return ranked.map((entry) => {
    const payload = entry.latest_payload || {};
    const cuesRaw = arrayValue<Record<string, unknown>>(payload.required_cues).length > 0
      ? arrayValue<Record<string, unknown>>(payload.required_cues)
      : arrayValue<Record<string, unknown>>(payload.cues);
    const cues: PatternBoardCue[] = cuesRaw
      .map((cue) => {
        const cueRecord = recordValue(cue);
        const k = nullableString(cueRecord.key);
        const v = nullableString(cueRecord.value);
        const label = nullableString(cueRecord.label) || getPatternCueLabel(k, v);
        return label;
      })
      .filter((label): label is string => Boolean(label))
      .slice(0, PATTERN_BOARD_CUE_MAX);

    const supportKeys = supportKeysByEntry.get(entry.firewatch_id) || [];
    const support_posts: PatternBoardSupport[] = supportKeys
      .map((key) => supportPreviewByKey.get(key))
      .filter((preview): preview is PatternBoardSupport => Boolean(preview));

    return {
      firewatch_id: entry.firewatch_id,
      signal_code: entry.signal_code,
      context: entry.context,
      pattern_name: entry.pattern_name,
      pattern_label: entry.pattern_label,
      trigger_count: entry.trigger_count,
      avg_hot_percentile: entry.avg_count > 0 ? Number((entry.avg_total / entry.avg_count).toFixed(2)) : null,
      feeders_count: entry.feeders_count || null,
      baseline_share: entry.baseline_share,
      recent_lift: entry.recent_lift,
      anchor_gap: entry.anchor_gap,
      match_count: entry.match_count,
      latest_business_day: entry.latest_business_day,
      cues,
      support_posts,
    };
  });
}

export async function GET(request: NextRequest) {
  try {
    const supabase = await createClient();
    const {
      data: { user },
      error: authError,
    } = await supabase.auth.getUser();

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const feedId = Number(request.nextUrl.searchParams.get('feedId') || 0);
    const windowParam = Number(
      request.nextUrl.searchParams.get('days')
      || request.nextUrl.searchParams.get('weeks')
      || 30
    );
    const handleRaw = (request.nextUrl.searchParams.get('handle') || '').trim();
    const handle = handleRaw ? handleRaw.replace(/^@+/, '').toLowerCase() : null;

    if (!feedId) {
      return NextResponse.json({ error: 'feedId is required' }, { status: 400 });
    }
    if (![7, 30, 60, 90, 4, 12, 26, 52].includes(windowParam)) {
      return NextResponse.json({ error: 'days must be one of 7,30,60,90' }, { status: 400 });
    }

    const payload = await withServerRouteCache(
      `feed:dashboard:${DASHBOARD_ROUTE_CACHE_VERSION}:${user.id}:${feedId}:${windowParam}:${handle || 'all'}`,
      DASHBOARD_ROUTE_TTL_MS,
      async () => {
        const sb = adminClient();
        const { data: feedRow, error: feedErr } = await sb
          .from('feeds')
          .select('id,user_id')
          .eq('id', feedId)
          .eq('user_id', user.id)
          .single();
        if (feedErr || !feedRow) {
          throw new Error('Feed not found');
        }

        const { data, error } = await sb.rpc('fn_feed_dashboard', {
          p_feed_id: feedId,
          p_weeks: windowParam,
          p_handle: handle,
        });
        if (error) throw error;
        const dashboard = recordValue(data);
        const summary = recordValue(dashboard.summary);
        const windowStartIst = nullableString(summary.window_start_ist);
        const windowEndIst = nullableString(summary.window_end_ist);
        const scopedFeeders = await fetchScopedFeeders(sb, feedId, handle);
        const scopedFeederIds = scopedFeeders.map((row) => row.id);

        const [patternBoard, heatmapDaily, killzoneDays, postingPattern, engagementBreakdown] = await Promise.all([
          fetchPatternBoard(
            sb,
            feedId,
            scopedFeederIds,
            windowStartIst,
            windowEndIst,
          ),
          windowParam === 90
            ? Promise.resolve(arrayValue(dashboard.heatmap_daily))
            : fetchRollingHeatmap(sb, feedId, handle),
          fetchKillzoneDays(
            sb,
            scopedFeederIds,
            windowStartIst,
            windowEndIst,
          ),
          fetchPostingPattern(
            sb,
            scopedFeeders,
            windowStartIst,
            windowEndIst,
          ),
          fetchEngagementBreakdown(
            sb,
            scopedFeederIds,
            windowStartIst,
            windowEndIst,
          ),
        ]);

        const scatterPoints = arrayValue(dashboard.scatter_points).map((value) => {
          const row = recordValue(value);
          const postKey = nullableString(row.post_key);
          const mediaType = postKey
            ? engagementBreakdown.mediaTypeByPostKey.get(postKey) ?? normalizeDashboardMediaType(nullableString(row.media_type))
            : normalizeDashboardMediaType(nullableString(row.media_type));
          return {
            ...row,
            media_type: mediaType,
          };
        });
        const apexMix = buildApexMixFromPostingPattern(postingPattern) ?? arrayValue(dashboard.apex_mix);

        return {
          dashboard: {
            ...dashboard,
            apex_mix: apexMix,
            heatmap_daily: heatmapDaily,
            killzone_days: killzoneDays,
            pattern_board: patternBoard,
            posting_pattern: postingPattern,
            scatter_points: scatterPoints,
            engagement_averages: engagementBreakdown.averages,
          },
        };
      },
    );

    return privateJsonResponse(request, payload, {
      maxAgeSeconds: 60,
      staleWhileRevalidateSeconds: 600,
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to load dashboard';
    const status = message === 'Feed not found' ? 404 : 500;
    return NextResponse.json({ error: message }, { status });
  }
}
