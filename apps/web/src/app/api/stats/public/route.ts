import { NextResponse } from 'next/server';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { withServerRouteCache } from '@/lib/serverRouteCache';

export const dynamic = 'force-dynamic';

const CACHE_KEY = 'stats:public:v4';
const CACHE_TTL_MS = 5 * 60_000;

const METRIC_KEYS = ['views', 'likes', 'comments', 'posts', 'signals', 'accounts', 'feeds', 'creators'] as const;
type MetricKey = (typeof METRIC_KEYS)[number];
type Values = Record<MetricKey, number | null>;

const EMPTY_VALUES: Values = {
  views: null, likes: null, comments: null, posts: null,
  signals: null, accounts: null, feeds: null, creators: null,
};

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

type AdminClient = ReturnType<typeof adminClient>;

function num(value: unknown): number | null {
  if (value == null) return null;
  const parsed = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

// Fast path: the pg_cron-refreshed rollup is a single row holding the current AND
// previous snapshot, so a real growth rate falls straight out. Returns null until
// the migration is deployed.
async function loadFromRollup(
  sb: AdminClient,
): Promise<{ values: Values; rates: Record<MetricKey, number>; measuredAt: number } | null> {
  const { data, error } = await sb.from('platform_stats').select('*').eq('id', 1).maybeSingle();
  if (error || !data) return null;
  const row = data as Record<string, unknown>;

  const refreshedAt = typeof row.refreshed_at === 'string' ? Date.parse(row.refreshed_at) : NaN;
  const prevRefreshedAt = typeof row.prev_refreshed_at === 'string' ? Date.parse(row.prev_refreshed_at) : NaN;
  const dtSec = Number.isFinite(refreshedAt) && Number.isFinite(prevRefreshedAt) && refreshedAt > prevRefreshedAt
    ? (refreshedAt - prevRefreshedAt) / 1000
    : 0;

  const values = { ...EMPTY_VALUES };
  const rates = {} as Record<MetricKey, number>;
  for (const key of METRIC_KEYS) {
    const value = num(row[key]);
    const prev = num(row[`prev_${key}`]);
    values[key] = value;
    rates[key] = dtSec > 0 && value != null && prev != null && value > prev ? (value - prev) / dtSec : 0;
  }
  return { values, rates, measuredAt: Number.isFinite(refreshedAt) ? refreshedAt : Date.now() };
}

const CHECKPOINT_RANK: Record<string, number> = { d1: 1, d3: 2, d7: 3, d21: 4 };

// Direct compute (current scale is small): every real platform total in one pass.
async function computeDirect(sb: AdminClient): Promise<Values> {
  const [accountsRes, postsRes, feedsRes, signalsRes] = await Promise.all([
    sb.from('feeders').select('*', { count: 'exact', head: true }).eq('status', 'active'),
    sb.from('posts').select('*', { count: 'exact', head: true }),
    sb.from('feeds').select('user_id'),
    sb.from('signals').select('*', { count: 'exact', head: true }),
  ]);

  const feedRows = (feedsRes.data ?? []) as Array<{ user_id: string | null }>;

  // Engagement = the latest checkpoint per post (d21 > d7 > d3 > d1), summed.
  type Latest = { rank: number; computedAt: number; likes: number; comments: number; views: number };
  const latest = new Map<string, Latest>();
  let metricsOk = true;
  for (let start = 0; ; start += 1000) {
    const { data, error } = await sb
      .from('post_metrics')
      .select('post_key,checkpoint,computed_at,likes,comments,views')
      .range(start, start + 999);
    if (error) {
      metricsOk = false;
      break;
    }
    const rows = (data ?? []) as Array<Record<string, unknown>>;
    for (const row of rows) {
      const key = typeof row.post_key === 'string' ? row.post_key : null;
      if (!key) continue;
      const rank = CHECKPOINT_RANK[String(row.checkpoint || '').toLowerCase()] ?? 0;
      const computedAt = Date.parse(String(row.computed_at || '')) || 0;
      const current = latest.get(key);
      if (!current || rank > current.rank || (rank === current.rank && computedAt > current.computedAt)) {
        latest.set(key, {
          rank,
          computedAt,
          likes: num(row.likes) ?? 0,
          comments: num(row.comments) ?? 0,
          views: num(row.views) ?? 0,
        });
      }
    }
    if (rows.length < 1000) break;
  }

  let likes = 0;
  let comments = 0;
  let views = 0;
  for (const entry of latest.values()) {
    likes += entry.likes;
    comments += entry.comments;
    views += entry.views;
  }

  return {
    accounts: accountsRes.error ? null : accountsRes.count ?? null,
    posts: postsRes.error ? null : postsRes.count ?? null,
    likes: metricsOk ? likes : null,
    comments: metricsOk ? comments : null,
    views: metricsOk ? views : null,
    feeds: feedsRes.error ? null : feedRows.length,
    creators: feedsRes.error ? null : new Set(feedRows.map((row) => row.user_id).filter(Boolean)).size,
    signals: signalsRes.error ? null : signalsRes.count ?? null,
  };
}

// Module-memory previous snapshot — derives a real rate for the direct path across
// the 5-min recompute cadence (no DB history needed). Per-instance; resets on cold start.
let prevSnapshot: { values: Values; at: number } | null = null;

function ratesFromMemory(values: Values, nowMs: number): Record<MetricKey, number> {
  const dtSec = prevSnapshot ? (nowMs - prevSnapshot.at) / 1000 : 0;
  const rates = {} as Record<MetricKey, number>;
  for (const key of METRIC_KEYS) {
    const value = values[key];
    const prev = prevSnapshot?.values[key] ?? null;
    rates[key] = dtSec > 0 && value != null && prev != null && value > prev ? (value - prev) / dtSec : 0;
  }
  return rates;
}

// Dev-only: when there's no measured growth (frozen local DB), let the odometers
// breathe so the live mechanic is visible. Production uses only real measured rates.
function applyDevAmbient(values: Values, rates: Record<MetricKey, number>): Record<MetricKey, number> {
  if (process.env.NODE_ENV === 'production') return rates;
  const out = { ...rates };
  for (const key of METRIC_KEYS) {
    const value = values[key];
    if (out[key] === 0 && value != null && value > 0) out[key] = (value * 0.02) / 3600; // ~2%/hr
  }
  return out;
}

export async function GET() {
  try {
    const payload = await withServerRouteCache(CACHE_KEY, CACHE_TTL_MS, async () => {
      const sb = adminClient();
      const now = Date.now();

      const rollup = await loadFromRollup(sb);
      let values: Values;
      let rates: Record<MetricKey, number>;
      let measuredAt: number;
      let source: string;

      if (rollup) {
        ({ values, rates, measuredAt } = rollup);
        source = 'rollup';
      } else {
        values = await computeDirect(sb);
        rates = ratesFromMemory(values, now);
        prevSnapshot = { values, at: now };
        measuredAt = now;
        source = 'direct';
      }

      rates = applyDevAmbient(values, rates);

      const metrics = {} as Record<MetricKey, { value: number | null; ratePerSec: number }>;
      for (const key of METRIC_KEYS) {
        metrics[key] = { value: values[key], ratePerSec: Math.max(0, rates[key] ?? 0) };
      }

      return { metrics, measuredAt: new Date(measuredAt).toISOString(), source };
    });

    return NextResponse.json(payload, {
      headers: {
        'Cache-Control': 'public, max-age=30, s-maxage=300, stale-while-revalidate=600',
      },
    });
  } catch {
    const metrics = {} as Record<MetricKey, { value: number | null; ratePerSec: number }>;
    for (const key of METRIC_KEYS) metrics[key] = { value: null, ratePerSec: 0 };
    return NextResponse.json(
      { metrics, measuredAt: null, source: 'error' },
      { status: 200, headers: { 'Cache-Control': 'public, max-age=15' } },
    );
  }
}
