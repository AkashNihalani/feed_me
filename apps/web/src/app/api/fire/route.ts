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

function buildRecentDayKeys(count: number): string[] {
  const keys: string[] = [];
  const now = new Date();
  for (let i = 1; i <= count; i++) {
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

type ActiveFeedRow = { id: number; name: string | null };
type ActiveFeederRow = { id: number; feed_id: number; handle: string | null };

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
      day: params.get('day') || yesterdayIstDayKey(),
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
      day: params.get('day') || yesterdayIstDayKey(),
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
      .select('business_date_ist, surface_handle')
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
  const day = params.get('day') || yesterdayIstDayKey();
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

  let availableCheckpointsQuery = supabase
    .from('fire_alerts')
    .select('checkpoint,surface_checkpoint')
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
          .map((row) => normalizeCheckpoint(row.checkpoint || row.surface_checkpoint))
          .filter(Boolean),
      ),
    ),
  );

  let query = supabase
    .from('fire_alerts')
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

  query = sort === 'recent'
    ? query
        .order('created_at', { ascending: false })
        .order('surface_percentile', { ascending: true, nullsFirst: false })
        .range(cursor, cursor + PAGE_SIZE - 1)
    : query
        .order('surface_percentile', { ascending: true, nullsFirst: false })
        .order('created_at', { ascending: false })
        .range(cursor, cursor + PAGE_SIZE - 1);

  const { data, error, count } = await query;

  if (error) {
    console.error('[/api/fire] Supabase error:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }

  const rows = data ?? [];
  const total = count ?? 0;
  const hasMore = cursor + rows.length < total;

  return NextResponse.json({ rows, total, hasMore, day, cursor, availableCheckpoints });
}
