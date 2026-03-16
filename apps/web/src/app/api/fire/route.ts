import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';
import { createClient as createServerClient } from '@/lib/supabase/server';

export const dynamic = 'force-dynamic';

const PAGE_SIZE = 30;

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
    .select('id')
    .eq('user_id', userId)
    .eq('status', 'active')
    .limit(2000);
  if (activeFeedsErr) {
    console.error('[/api/fire] Active feeds query error:', activeFeedsErr);
    return NextResponse.json({ error: activeFeedsErr.message }, { status: 500 });
  }

  const activeFeedIds = (activeFeeds ?? []).map((row) => Number((row as { id: number }).id)).filter(Number.isFinite);
  if (activeFeedIds.length === 0) {
    if (mode === 'meta') {
      return NextResponse.json({ days: recentKeys, scopes: [], dayCounts: {} });
    }
    return NextResponse.json({ rows: [], total: 0, hasMore: false, day: params.get('day') || yesterdayIstDayKey(), cursor: 0 });
  }

  const { data: activeFeeders, error: activeFeedersErr } = await supabase
    .from('feeders')
    .select('id')
    .eq('status', 'active')
    .in('feed_id', activeFeedIds)
    .limit(5000);
  if (activeFeedersErr) {
    console.error('[/api/fire] Active feeders query error:', activeFeedersErr);
    return NextResponse.json({ error: activeFeedersErr.message }, { status: 500 });
  }

  const activeFeederIds = (activeFeeders ?? []).map((row) => Number((row as { id: number }).id)).filter(Number.isFinite);
  if (activeFeederIds.length === 0) {
    if (mode === 'meta') {
      return NextResponse.json({ days: recentKeys, scopes: [], dayCounts: {} });
    }
    return NextResponse.json({ rows: [], total: 0, hasMore: false, day: params.get('day') || yesterdayIstDayKey(), cursor: 0 });
  }

  // ─── META MODE ─────────────────────────────────────────────
  // Returns available days and feeder scopes — very cheap query
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
    const scopesSet = new Set<string>();
    const dayCounts: Record<string, number> = {};

    for (const row of dayRows ?? []) {
      const d = row.business_date_ist as string;
      if (d) {
        daysSet.add(d);
        dayCounts[d] = (dayCounts[d] ?? 0) + 1;
      }
      const h = row.surface_handle as string;
      if (h) scopesSet.add(h);
    }

    // Ensure at least 7 days in the picker even if some have no data
    for (const k of recentKeys) daysSet.add(k);

    const days = Array.from(daysSet).sort((a, b) => b.localeCompare(a)).slice(0, 7);
    const scopes = Array.from(scopesSet).sort();

    return NextResponse.json({ days, scopes, dayCounts });
  }

  // ─── PAGINATED ALERTS MODE ─────────────────────────────────
  const day = params.get('day') || yesterdayIstDayKey();
  const scope = params.get('scope') || 'ALL';
  const threshold = params.get('threshold') || 'ALL';
  const cursor = Math.max(0, parseInt(params.get('cursor') || '0', 10) || 0);

  let query = supabase
    .from('fire_alerts')
    .select('*', { count: 'exact' })
    .eq('signal_code', 'slot_v3')
    .eq('context', 'own')
    .in('feed_id', activeFeedIds)
    .in('feeder_id', activeFeederIds)
    .eq('business_date_ist', day)
    .not('status', 'in', '("dropped","error","archived")');

  // Apply scope filter server-side
  if (scope !== 'ALL') {
    query = query.eq('surface_handle', scope);
  }

  // Apply threshold filter server-side (lower percentile = better)
  if (threshold !== 'ALL') {
    const limit = parseInt(threshold, 10);
    if (Number.isFinite(limit)) {
      query = query.lte('surface_percentile', limit);
    }
  }

  // Sort by percentile (best first), then recency
  query = query
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

  return NextResponse.json({ rows, total, hasMore, day, cursor });
}
