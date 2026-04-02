import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';
import { createClient as createServerClient } from '@/lib/supabase/server';

export const dynamic = 'force-dynamic';

const PAGE_SIZE = 30;
const CHECKPOINT_ORDER = ['D1', 'D3', 'D7', 'D21'];

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

function yesterdayIstDayKey(): string {
  return shiftIstDayKey(new Date(), -1);
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

function parseIsoTime(value: unknown): number {
  if (typeof value !== 'string' || !value) return 0;
  const ts = Date.parse(value);
  return Number.isFinite(ts) ? ts : 0;
}

function checkpointRank(value: unknown): number {
  const normalized = normalizeCheckpoint(value);
  const idx = CHECKPOINT_ORDER.indexOf(normalized);
  return idx === -1 ? -1 : idx;
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

function peakWindowLabel(hour: number | null): string | null {
  if (hour == null || !Number.isFinite(hour)) return null;
  if (hour >= 5 && hour < 12) return 'Morning';
  if (hour >= 12 && hour < 17) return 'Afternoon';
  if (hour >= 17 && hour < 22) return 'Evening';
  return 'Late Night';
}

function collapseRowsForAllCheckpoints(rows: Record<string, unknown>[]): Record<string, unknown>[] {
  const byPostKey = new Map<string, Record<string, unknown>>();

  for (const row of rows) {
    const postKey = typeof row.post_key === 'string' ? row.post_key.trim() : '';
    if (!postKey) continue;

    const current = byPostKey.get(postKey);
    if (!current) {
      byPostKey.set(postKey, row);
      continue;
    }

    const currentRank = checkpointRank(current.checkpoint ?? current.surface_checkpoint);
    const nextRank = checkpointRank(row.checkpoint ?? row.surface_checkpoint);
    if (nextRank !== currentRank) {
      if (nextRank > currentRank) byPostKey.set(postKey, row);
      continue;
    }

    const currentPercentile = percentileValue(current);
    const nextPercentile = percentileValue(row);
    if (nextPercentile !== currentPercentile) {
      if (nextPercentile < currentPercentile) byPostKey.set(postKey, row);
      continue;
    }

    if (parseIsoTime(row.created_at) > parseIsoTime(current.created_at)) {
      byPostKey.set(postKey, row);
    }
  }

  return Array.from(byPostKey.values());
}

type ActiveFeedRow = { id: number; name: string | null };
type ActiveFeederRow = { id: number; feed_id: number; handle: string | null };
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
  body: string | null;
  pattern_signal: string | null;
  pattern_payload: Record<string, unknown> | null;
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
  views_percentile: number | null;
  likes_percentile: number | null;
  comments_percentile: number | null;
  feed_percentile: number | null;
  views_baseline: number | null;
  likes_baseline: number | null;
  comments_baseline: number | null;
  views_multiple: number | null;
  likes_multiple: number | null;
  comments_multiple: number | null;
  hour_ist: number | null;
  hour_percentile: number | null;
  hour_multiple: number | null;
  feeder_rank: number | null;
  feed_rank: number | null;
  best_in_last_n: number | null;
  trajectory_d1: number | null;
  trajectory_d3: number | null;
  trajectory_d7: number | null;
  trajectory_d21: number | null;
  anchor_handle: string | null;
  anchor_best_pct: number | null;
  anchor_gap: number | null;
  intelligence_skipped: boolean | null;
};

function buildMetricPayload(row: AlertSurfaceRow, metric: 'views' | 'likes' | 'comments', bestMetric: string) {
  const isBestMetric = metric === bestMetric;
  const value = metric === 'views' ? row.views : metric === 'likes' ? row.likes : row.comments;
  const percentile =
    metric === 'views' ? row.views_percentile : metric === 'likes' ? row.likes_percentile : row.comments_percentile;
  const baseline =
    metric === 'views' ? row.views_baseline : metric === 'likes' ? row.likes_baseline : row.comments_baseline;
  const multiple =
    metric === 'views' ? row.views_multiple : metric === 'likes' ? row.likes_multiple : row.comments_multiple;

  return {
    value: nullableNumber(value),
    percentile: nullableNumber(percentile),
    baseline: nullableNumber(baseline),
    multiple: nullableNumber(multiple),
    rank_feed: isBestMetric ? nullableNumber(row.feed_rank) : null,
    feed_percentile: isBestMetric ? nullableNumber(row.feed_percentile) : null,
    best_in_last_n: isBestMetric ? nullableNumber(row.best_in_last_n) : null,
  };
}

function serializeAlertRow(row: AlertSurfaceRow): Record<string, unknown> {
  const bestMetric = (nullableString(row.metric_key) || 'views').toLowerCase();
  const payload = {
    best_metric: bestMetric,
    metrics: {
      views: buildMetricPayload(row, 'views', bestMetric),
      likes: buildMetricPayload(row, 'likes', bestMetric),
      comments: buildMetricPayload(row, 'comments', bestMetric),
    },
    position: {
      percentile: nullableNumber(row.surface_percentile),
      feed_rank: nullableNumber(row.feed_rank),
      feed_percentile: nullableNumber(row.feed_percentile),
      feeder_rank: nullableNumber(row.feeder_rank),
      rank_overall: nullableNumber(row.feeder_rank),
      shift: nullableNumber(row.surface_delta),
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
    anchor: {
      feeder_handle: nullableString(row.anchor_handle),
      best_pct: nullableNumber(row.anchor_best_pct),
      gap: nullableNumber(row.anchor_gap),
    },
    meta: {
      handle: nullableString(row.handle),
      media_type: nullableString(row.media_type),
      checkpoint: nullableString(row.checkpoint),
      post_url: nullableString(row.post_url),
      thumbnail_url: nullableString(row.thumbnail_url),
      business_date_ist: nullableString(row.business_date_ist),
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
    metric_value: nullableNumber(row.metric_value),
    body: row.body ?? '',
    pattern_signal: row.pattern_signal,
    pattern_payload: row.pattern_payload,
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

function hasUsableRawThumbnailUrl(value: unknown): boolean {
  const url = nullableString(value);
  if (!url) return false;
  return !/\.mp4(?:$|[?#])/i.test(url);
}

async function filterRowsForVisibleD21Thumbnails(
  sb: ReturnType<typeof createClient>,
  rows: AlertSurfaceRow[],
): Promise<AlertSurfaceRow[]> {
  const d21Rows = rows.filter((row) => normalizeCheckpoint(row.checkpoint) === 'D21');
  if (d21Rows.length === 0) return rows;

  const postKeysNeedingCachedImage = Array.from(
    new Set(
      d21Rows
        .filter((row) => !hasUsableRawThumbnailUrl(row.thumbnail_url))
        .map((row) => row.post_key?.trim())
        .filter(Boolean),
    ),
  ) as string[];

  if (postKeysNeedingCachedImage.length === 0) return rows;

  const { data: assetRows } = await sb
    .from('post_media_assets')
    .select('post_key,storage_path')
    .in('post_key', postKeysNeedingCachedImage)
    .in('asset_role', ['thumbnail', 'display', 'carousel_0'])
    .eq('status', 'active')
    .not('storage_path', 'is', null)
    .limit(Math.max(50, postKeysNeedingCachedImage.length * 3));

  const cachedImagePostKeys = new Set(
    (assetRows || []).map((row: { post_key?: string | null; storage_path?: string | null }) =>
      typeof row.post_key === 'string' && row.post_key.trim() && typeof row.storage_path === 'string' && row.storage_path.trim()
        ? row.post_key.trim()
        : '').filter(Boolean),
  );

  return rows.filter((row) => normalizeCheckpoint(row.checkpoint) !== 'D21' || hasUsableRawThumbnailUrl(row.thumbnail_url) || cachedImagePostKeys.has((row.post_key || '').trim()));
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

  const { data: activeFeeds, error: activeFeedsErr } = await supabase
    .from('feeds')
    .select('id,name')
    .eq('user_id', userId)
    .eq('status', 'active')
    .limit(2000);
  if (activeFeedsErr) {
    console.error('[/api/fire] Active feeds query error:', activeFeedsErr);
    return NextResponse.json({ error: activeFeedsErr.message }, { status: 500 });
  }

  const normalizedFeeds = (activeFeeds ?? []) as ActiveFeedRow[];
  const activeFeedIds = normalizedFeeds.map((row) => Number(row.id)).filter(Number.isFinite);
  if (activeFeedIds.length === 0) {
    if (mode === 'meta') {
      return NextResponse.json({ days: recentKeys, scopes: [], feeds: [], dayCounts: {} });
    }
    return NextResponse.json({
      rows: [],
      total: 0,
      hasMore: false,
      day: params.get('day') || todayIstDayKey(),
      cursor: 0,
      availableCheckpoints: [],
    });
  }

  const { data: activeFeeders, error: activeFeedersErr } = await supabase
    .from('feeders')
    .select('id,feed_id,handle')
    .eq('status', 'active')
    .in('feed_id', activeFeedIds)
    .limit(5000);
  if (activeFeedersErr) {
    console.error('[/api/fire] Active feeders query error:', activeFeedersErr);
    return NextResponse.json({ error: activeFeedersErr.message }, { status: 500 });
  }

  const normalizedFeeders = (activeFeeders ?? []) as ActiveFeederRow[];
  const activeFeederIds = normalizedFeeders.map((row) => Number(row.id)).filter(Number.isFinite);
  if (activeFeederIds.length === 0) {
    if (mode === 'meta') {
      return NextResponse.json({ days: recentKeys, scopes: [], feeds: [], dayCounts: {} });
    }
    return NextResponse.json({
      rows: [],
      total: 0,
      hasMore: false,
      day: params.get('day') || todayIstDayKey(),
      cursor: 0,
      availableCheckpoints: [],
    });
  }

  // ─── META MODE ─────────────────────────────────────────────
  // Returns available days plus nested feed / feeder options.
  if (mode === 'meta') {
    const windowDays = 14;
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - windowDays);
    const startIstDayKey = toIstDayKey(startDate);

    // Get distinct business days
    const { data: dayRows, error: dayErr } = await supabase
      .from('fire_alerts')
      .select('business_date_ist')
      .eq('signal_code', 'slot_v3')
      .eq('context', 'own')
      .in('feed_id', activeFeedIds)
      .in('feeder_id', activeFeederIds)
      .gte('business_date_ist', startIstDayKey)
      .not('status', 'in', '("dropped","error","archived")')
      .order('business_date_ist', { ascending: false })
      .limit(2000);

    if (dayErr) {
      console.error('[/api/fire?mode=meta] Supabase error:', dayErr);
      return NextResponse.json({ error: dayErr.message }, { status: 500 });
    }

    const daysSet = new Set<string>();
    const dayCounts: Record<string, number> = {};

    for (const row of dayRows ?? []) {
      const d = row.business_date_ist as string;
      if (d) {
        daysSet.add(d);
        dayCounts[d] = (dayCounts[d] ?? 0) + 1;
      }
    }

    // Ensure at least 7 days in the picker even if some have no data
    for (const k of recentKeys) daysSet.add(k);

    const days = Array.from(daysSet).sort((a, b) => b.localeCompare(a)).slice(0, 7);
    const feedersByFeed = new Map<number, { id: number; handle: string }[]>();

    for (const feeder of normalizedFeeders) {
      const feedId = Number(feeder.feed_id);
      if (!Number.isFinite(feedId)) continue;
      const bucket = feedersByFeed.get(feedId) || [];
      bucket.push({ id: Number(feeder.id), handle: String(feeder.handle || '') });
      feedersByFeed.set(feedId, bucket);
    }

    const feeds = normalizedFeeds
      .map((feed) => ({
        id: Number(feed.id),
        name: String(feed.name || 'UNTITLED FEED').toUpperCase(),
        feeders: (feedersByFeed.get(Number(feed.id)) || [])
          .filter((feeder) => Number.isFinite(feeder.id) && feeder.handle)
          .sort((a, b) => a.handle.localeCompare(b.handle)),
      }))
      .sort((a, b) => a.name.localeCompare(b.name));

    return NextResponse.json({ days, scopes: [], feeds, dayCounts });
  }

  // ─── PAGINATED ALERTS MODE ─────────────────────────────────
  const day = params.get('day') || todayIstDayKey();
  const threshold = params.get('threshold') || 'ALL';
  const requestedFeedIds = parseCsvNumbers(params.get('feed_ids'));
  const requestedFeederIds = parseCsvNumbers(params.get('feeder_ids'));
  const requestedCheckpoints = parseCsvStrings(params.get('checkpoints'));
  const sort = params.get('sort') || 'best';
  const cursor = Math.max(0, parseInt(params.get('cursor') || '0', 10) || 0);
  const feedIdSet = new Set(activeFeedIds);
  const effectiveFeedIds = requestedFeedIds.length > 0
    ? requestedFeedIds.filter((id) => feedIdSet.has(id))
    : activeFeedIds;

  if (requestedFeedIds.length > 0 && effectiveFeedIds.length === 0) {
    return NextResponse.json({ rows: [], total: 0, hasMore: false, day, cursor, availableCheckpoints: [] });
  }

  const feedersForFeeds = normalizedFeeders.filter((row) => effectiveFeedIds.includes(Number(row.feed_id)));
  const feederIdSet = new Set(feedersForFeeds.map((row) => Number(row.id)));
  const effectiveFeederIds = requestedFeederIds.length > 0
    ? requestedFeederIds.filter((id) => feederIdSet.has(id))
    : Array.from(feederIdSet);

  if (effectiveFeederIds.length === 0) {
    return NextResponse.json({ rows: [], total: 0, hasMore: false, day, cursor, availableCheckpoints: [] });
  }

  const checkpointFilterValues = Array.from(new Set(
    requestedCheckpoints.flatMap((checkpoint) => [checkpoint, checkpoint.toLowerCase()]),
  ));
  const collapseCheckpointDuplicates = checkpointFilterValues.length === 0;

  let availableCheckpointsQuery = supabase
    .from('fire_alerts')
    .select('checkpoint')
    .eq('signal_code', 'slot_v3')
    .eq('context', 'own')
    .in('feed_id', effectiveFeedIds)
    .in('feeder_id', effectiveFeederIds)
    .eq('business_date_ist', day)
    .not('status', 'in', '("dropped","error","archived")')
    .limit(2000);

  if (threshold !== 'ALL') {
    const limit = parseInt(threshold, 10);
    if (Number.isFinite(limit)) {
      availableCheckpointsQuery = availableCheckpointsQuery.lte('surface_percentile', limit);
    }
  }

  const { data: checkpointRows, error: checkpointErr } = await availableCheckpointsQuery;
  if (checkpointErr) {
    console.error('[/api/fire] Available checkpoints query error:', checkpointErr);
    return NextResponse.json({ error: checkpointErr.message }, { status: 500 });
  }

  const availableCheckpoints = sortCheckpoints(
    Array.from(
      new Set(
        (checkpointRows ?? [])
          .map((row) => normalizeCheckpoint(row.checkpoint))
          .filter(Boolean),
      ),
    ),
  );

  let query = supabase
    .from('v_fire_alert_surface')
    .select('*', { count: 'exact' })
    .eq('signal_code', 'slot_v3')
    .eq('context', 'own')
    .in('feed_id', effectiveFeedIds)
    .in('feeder_id', effectiveFeederIds)
    .eq('business_date_ist', day)
    .not('status', 'in', '("dropped","error","archived")');

  if (threshold !== 'ALL') {
    const limit = parseInt(threshold, 10);
    if (Number.isFinite(limit)) {
      query = query.lte('surface_percentile', limit);
    }
  }

  if (checkpointFilterValues.length > 0) {
    query = query.in('checkpoint', checkpointFilterValues);
  }

  if (sort === 'recent') {
    const { data, error } = await query.limit(5000);

    if (error) {
      console.error('[/api/fire] Supabase error:', error);
      return NextResponse.json({ error: error.message }, { status: 500 });
    }

    let rows = (data ?? []) as Record<string, unknown>[];
    rows = await filterRowsForVisibleD21Thumbnails(supabase, rows as AlertSurfaceRow[]) as Record<string, unknown>[];

    if (collapseCheckpointDuplicates) {
      rows = collapseRowsForAllCheckpoints(rows);
    }

    const sortedRows = [...rows].sort((a, b) => {
      const aPostedAt = parseIsoTime(a.posted_at);
      const bPostedAt = parseIsoTime(b.posted_at);
      if (bPostedAt !== aPostedAt) return bPostedAt - aPostedAt;

      const aPercentile = typeof a.surface_percentile === 'number' ? a.surface_percentile : Number.POSITIVE_INFINITY;
      const bPercentile = typeof b.surface_percentile === 'number' ? b.surface_percentile : Number.POSITIVE_INFINITY;
      if (aPercentile !== bPercentile) return aPercentile - bPercentile;

      return parseIsoTime(b.created_at) - parseIsoTime(a.created_at);
    });

    const total = sortedRows.length;
    const pagedRows = sortedRows.slice(cursor, cursor + PAGE_SIZE);
    const hasMore = cursor + pagedRows.length < total;
    return NextResponse.json({ rows: pagedRows.map((row) => serializeAlertRow(row as AlertSurfaceRow)), total, hasMore, day, cursor, availableCheckpoints });
  }

  const { data, error } = await query
    .order('surface_percentile', { ascending: true, nullsFirst: false })
    .order('created_at', { ascending: false })
    .limit(5000);

  if (error) {
    console.error('[/api/fire] Supabase error:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }

  let rows = (data ?? []) as Record<string, unknown>[];
  rows = await filterRowsForVisibleD21Thumbnails(supabase, rows as AlertSurfaceRow[]) as Record<string, unknown>[];
  if (collapseCheckpointDuplicates) {
    rows = collapseRowsForAllCheckpoints(rows);
  }
  rows = [...rows].sort((a, b) => {
    const aPercentile = percentileValue(a);
    const bPercentile = percentileValue(b);
    if (aPercentile !== bPercentile) return aPercentile - bPercentile;
    return parseIsoTime(b.created_at) - parseIsoTime(a.created_at);
  });

  const total = rows.length;
  const pagedRows = rows.slice(cursor, cursor + PAGE_SIZE);
  const hasMore = cursor + pagedRows.length < total;

  return NextResponse.json({ rows: pagedRows.map((row) => serializeAlertRow(row as AlertSurfaceRow)), total, hasMore, day, cursor, availableCheckpoints });
}
