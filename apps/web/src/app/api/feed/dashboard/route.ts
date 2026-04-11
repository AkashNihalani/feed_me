import { NextRequest, NextResponse } from 'next/server';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient } from '@/lib/supabase/server';
import { getPatternMechanicLabel } from '@/lib/fireSignals';

export const dynamic = 'force-dynamic';

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
  feedId: number,
  windowStartIst: string | null,
  windowEndIst: string | null,
  handle: string | null,
) {
  if (!windowStartIst || !windowEndIst) return [];

  const startIso = istDayStartUtcIso(windowStartIst);
  const endDate = parseIstDateKey(windowEndIst);
  const endExclusiveIso = endDate ? istDayStartUtcIso(formatIstDateKey(addUtcDays(endDate, 1))) : null;
  if (!startIso || !endExclusiveIso) return [];

  const feederIds = (await fetchScopedFeeders(sb, feedId, handle)).map((row) => row.id);
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
  windowStartIst: string | null,
  windowEndIst: string | null,
  handle: string | null,
) {
  if (!windowStartIst || !windowEndIst) return [];

  const feederIds = (await fetchScopedFeeders(sb, feedId, handle)).map((row) => row.id);
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

    const sb = adminClient();
    const { data: feedRow, error: feedErr } = await sb
      .from('feeds')
      .select('id,user_id')
      .eq('id', feedId)
      .eq('user_id', user.id)
      .single();
    if (feedErr || !feedRow) {
      return NextResponse.json({ error: 'Feed not found' }, { status: 404 });
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

    const [patternBoard, ascentSeries, heatmapDaily, killzoneDays] = await Promise.all([
      fetchPatternBoard(
        sb,
        feedId,
        windowStartIst,
        windowEndIst,
        handle,
      ),
      fetchAscentSeries(
        sb,
        feedId,
        windowStartIst,
        windowEndIst,
        handle,
      ),
      windowParam === 90
        ? Promise.resolve(arrayValue(dashboard.heatmap_daily))
        : fetchRollingHeatmap(sb, feedId, handle),
      fetchKillzoneDays(
        sb,
        feedId,
        windowStartIst,
        windowEndIst,
        handle,
      ),
    ]);

    return NextResponse.json({
      dashboard: {
        ...dashboard,
        ascent_series: ascentSeries,
        heatmap_daily: heatmapDaily,
        killzone_days: killzoneDays,
        pattern_board: patternBoard,
      },
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to load dashboard';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
