import { NextRequest, NextResponse } from 'next/server';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient } from '@/lib/supabase/server';
import { getPatternMechanicLabel } from '@/lib/fireSignals';
import { privateJsonResponse } from '@/lib/privateJsonResponse';
import { withServerRouteCache } from '@/lib/serverRouteCache';

export const dynamic = 'force-dynamic';
const DASHBOARD_ROUTE_TTL_MS = 10 * 60 * 1000;

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

async function fetchAscentSeries(
  sb: ReturnType<typeof adminClient>,
  feedId: number,
  windowStartIst: string | null,
  windowEndIst: string | null,
  handle: string | null,
) {
  const startDate = parseIstDateKey(windowStartIst);
  const endDate = parseIstDateKey(windowEndIst);
  if (!startDate || !endDate || startDate.getTime() > endDate.getTime()) return [];

  const feeders = await fetchScopedFeeders(sb, feedId, handle);
  if (feeders.length === 0) return [];

  const feederIds = feeders.map((feeder) => feeder.id);
  const { data, error } = await sb
    .from('feeder_follower_snapshots')
    .select('feeder_id,snapshot_date_ist,follower_count')
    .in('feeder_id', feederIds)
    .lte('snapshot_date_ist', windowEndIst!);
  if (error) throw error;

  const snapshotsByFeeder = new Map<number, Array<{ snapshot_date_ist: string; follower_count: number }>>();
  for (const feeder of feeders) {
    snapshotsByFeeder.set(feeder.id, []);
  }

  for (const row of (data || []) as Array<Record<string, unknown>>) {
    const feederId = Number(row.feeder_id);
    const snapshotDate = nullableString(row.snapshot_date_ist);
    const followerCount = Math.max(0, Number(row.follower_count) || 0);
    if (!Number.isFinite(feederId) || !snapshotDate) continue;
    const bucket = snapshotsByFeeder.get(feederId);
    if (!bucket) continue;
    bucket.push({ snapshot_date_ist: snapshotDate, follower_count: followerCount });
  }

  for (const bucket of snapshotsByFeeder.values()) {
    bucket.sort((a, b) => a.snapshot_date_ist.localeCompare(b.snapshot_date_ist));
  }

  const currentTotal = feeders.reduce((sum, feeder) => sum + Math.max(0, feeder.follower_count || 0), 0);
  const baselineByFeeder = new Map<number, number>();
  for (const feeder of feeders) {
    const bucket = snapshotsByFeeder.get(feeder.id) || [];
    const firstKnownCount = bucket[0]?.follower_count;
    baselineByFeeder.set(
      feeder.id,
      Math.max(0, firstKnownCount ?? feeder.follower_count ?? 0),
    );
  }
  const series: Array<{ snapshot_date_ist: string; follower_count: number }> = [];

  for (let cursor = new Date(startDate); cursor.getTime() <= endDate.getTime(); cursor = addUtcDays(cursor, 1)) {
    const dayKey = formatIstDateKey(cursor);
    let total = 0;

    for (const feeder of feeders) {
      const bucket = snapshotsByFeeder.get(feeder.id) || [];
      let lastKnown = baselineByFeeder.get(feeder.id) ?? 0;
      for (const snapshot of bucket) {
        if (snapshot.snapshot_date_ist > dayKey) break;
        lastKnown = snapshot.follower_count;
      }
      total += Math.max(0, lastKnown);
    }

    series.push({
      snapshot_date_ist: dayKey,
      follower_count: total,
    });
  }

  if (series.length > 0 && currentTotal > 0) {
    series[series.length - 1] = {
      ...series[series.length - 1],
      follower_count: currentTotal,
    };
  }

  return series;
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
    .from('fire_alerts')
    .select('signal_code,context,business_date_ist,feeder_id,signal_payload')
    .eq('feed_id', feedId)
    .in('feeder_id', feederIds)
    .in('signal_code', ['OWN_PATTERN', 'CROSS_PATTERN', 'ANCHOR_PATTERN'])
    .gte('business_date_ist', windowStartIst)
    .lte('business_date_ist', windowEndIst)
    .order('business_date_ist', { ascending: false })
    .limit(300);

  if (error) throw error;

  const aggregate = new Map<string, {
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
    latest_business_day: string | null;
  }>();

  for (const row of (data || []) as Array<Record<string, unknown>>) {
    const payload = recordValue(row.signal_payload);
    const signalCode = nullableString(row.signal_code) || 'OWN_PATTERN';
    const context = (nullableString(row.context) || 'own') as 'own' | 'cross' | 'anchor';
    const patternName = nullableString(payload.pattern_name);
    const modifierKey = nullableString(payload.modifier_key);
    const modifierValue = nullableString(payload.modifier_value);
    const key = `${signalCode}:${patternName || 'unknown'}:${modifierKey || ''}:${modifierValue || ''}`;
    const current = aggregate.get(key) || {
      signal_code: signalCode,
      context,
      pattern_name: patternName,
      pattern_label: getPatternMechanicLabel(patternName) || 'Pattern',
      trigger_count: 0,
      avg_total: 0,
      avg_count: 0,
      feeders_count: 0,
      baseline_share: null,
      recent_lift: null,
      anchor_gap: null,
      latest_business_day: null,
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

    const businessDay = nullableString(row.business_date_ist);
    if (businessDay && (!current.latest_business_day || businessDay > current.latest_business_day)) {
      current.latest_business_day = businessDay;
    }

    aggregate.set(key, current);
  }

  return Array.from(aggregate.values())
    .map((entry) => ({
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
      latest_business_day: entry.latest_business_day,
    }))
    .sort((a, b) => {
      const dayDiff = (b.latest_business_day || '').localeCompare(a.latest_business_day || '');
      if (dayDiff !== 0) return dayDiff;
      const triggerDiff = b.trigger_count - a.trigger_count;
      if (triggerDiff !== 0) return triggerDiff;
      return (a.avg_hot_percentile ?? 999) - (b.avg_hot_percentile ?? 999);
    })
    .slice(0, 6);
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
      `feed:dashboard:${user.id}:${feedId}:${windowParam}:${handle || 'all'}`,
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

        const [patternBoard, heatmapDaily, killzoneDays, postingPattern] = await Promise.all([
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
        ]);

        return {
          dashboard: {
            ...dashboard,
            heatmap_daily: heatmapDaily,
            killzone_days: killzoneDays,
            pattern_board: patternBoard,
            posting_pattern: postingPattern,
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
