import { NextRequest, NextResponse } from 'next/server';
import { getSupabase } from '@/lib/supabase';
import { createClient } from '@/lib/supabase/server';
import { privateJsonResponse } from '@/lib/privateJsonResponse';
import {
  checkpointClaimableAt,
  currentOperationalStates,
  dateMs,
  type OperationalEvent,
  queueTiming,
} from './commandTruth';

export const dynamic = 'force-dynamic';

const COMMAND_ROUTE_TTL_SECONDS = 20;
const FEEDER_PRICE_INR = 1499;
const BRIGHT_DATA_USD_PER_1K_RECORDS = 1.5;
const STALE_HOURS = 24;
const FIRE_HOT_PERCENTILE_MAX = 35;

type DbRow = Record<string, unknown>;

type TableRead<T extends DbRow = DbRow> = {
  available: boolean;
  queried: boolean;
  rows: T[];
  count: number;
  error: string | null;
};

type QueryResult<T extends DbRow = DbRow> = {
  data: T[] | null;
  error: { message: string } | null;
  count?: number | null;
};

type AccessMode = 'platform_admin' | 'signed_in_account';

function adminEmails() {
  return (process.env.COMMAND_HUB_ADMIN_EMAILS || '')
    .split(',')
    .map((email) => email.trim().toLowerCase())
    .filter(Boolean);
}

async function requireUser() {
  const supabase = await createClient();
  const {
    data: { user },
    error,
  } = await supabase.auth.getUser();

  if (error || !user) {
    return {
      response: NextResponse.json({ error: 'Unauthorized' }, { status: 401 }),
      user: null,
    };
  }

  return { response: null, user };
}

async function readTable<T extends DbRow>(
  label: string,
  loader: () => PromiseLike<QueryResult<T>>,
): Promise<TableRead<T>> {
  try {
    const { data, error, count } = await loader();
    if (error) {
      return {
        available: false,
        queried: true,
        rows: [],
        count: 0,
        error: `${label}: ${error.message}`,
      };
    }

    const rows = data || [];
    return {
      available: true,
      queried: true,
      rows,
      count: count ?? rows.length,
      error: null,
    };
  } catch (error) {
    return {
      available: false,
      queried: true,
      rows: [],
      count: 0,
      error: `${label}: ${error instanceof Error ? error.message : 'read failed'}`,
    };
  }
}

function emptyRead<T extends DbRow>(): TableRead<T> {
  return { available: true, queried: false, rows: [], count: 0, error: null };
}

function asString(value: unknown): string | null;
function asString(value: unknown, fallback: string): string;
function asString(value: unknown, fallback: string | null = null): string | null {
  if (typeof value === 'string' && value.trim()) return value;
  return fallback;
}

function asNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function labelize(value: unknown, fallback = 'Unknown') {
  return (asString(value, fallback) || fallback)
    .replace(/[_-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function asDateMs(value: unknown): number {
  return dateMs(value);
}

function latestIso(rows: DbRow[], keys: string[]): string | null {
  let latest = 0;
  for (const row of rows) {
    for (const key of keys) {
      latest = Math.max(latest, asDateMs(row[key]));
    }
  }
  return latest > 0 ? new Date(latest).toISOString() : null;
}

function countWhere(rows: DbRow[], predicate: (row: DbRow) => boolean): number {
  return rows.reduce((total, row) => total + (predicate(row) ? 1 : 0), 0);
}

function countBy(rows: DbRow[], key: string): Record<string, number> {
  return rows.reduce<Record<string, number>>((acc, row) => {
    const value = asString(row[key]) || 'unknown';
    acc[value] = (acc[value] || 0) + 1;
    return acc;
  }, {});
}

function sumNumber(rows: DbRow[], key: string): number {
  return rows.reduce((total, row) => total + (asNumber(row[key]) || 0), 0);
}

function percent(numerator: number, denominator: number): number | null {
  if (!denominator) return null;
  return Math.round((numerator / denominator) * 1000) / 10;
}

function pickRows<T extends DbRow>(rows: T[], limit: number): T[] {
  return rows.slice(0, limit);
}

function recentErrors(rows: DbRow[], limit = 6) {
  return rows
    .filter((row) => asString(row.last_error) || asString(row.error))
    .sort((a, b) => asDateMs(b.updated_at) - asDateMs(a.updated_at))
    .slice(0, limit)
    .map((row) => ({
      id: String(row.id ?? row.post_key ?? row.call_key ?? 'unknown'),
      status: asString(row.status),
      source: asString(row.job_type) || asString(row.call_type) || asString(row.asset_role) || 'pipeline',
      lastError: asString(row.last_error) || asString(row.error),
      updatedAt: asString(row.updated_at),
    }));
}

function tableErrorGaps(reads: Record<string, TableRead>, optionalSources = new Set<string>()) {
  return Object.entries(reads)
    .filter(([, read]) => !read.available && read.error)
    .map(([label, read]) => ({
      id: `table:${label}`,
      label: `${label} table read`,
      status: optionalSources.has(label) ? 'partial' as const : 'unavailable' as const,
      detail: read.error || 'Read failed',
      path: optionalSources.has(label)
        ? 'Apply the optional observability migration or check its read privileges.'
        : 'Check Supabase table availability, privileges, or selected columns.',
    }));
}

function sourceReadState(read: TableRead, options: { limit?: number; orderBy?: string } = {}) {
  const loadedRows = read.rows.length;
  const truncated = read.available && read.queried && read.count > loadedRows;
  return {
    status: !read.queried
      ? 'not_applicable'
      : !read.available
        ? 'unavailable'
        : truncated
          ? 'sampled'
          : 'available',
    available: read.available,
    queried: read.queried,
    exactMatchingRows: read.available && read.queried ? read.count : null,
    loadedRows,
    truncated,
    limit: options.limit ?? null,
    orderBy: options.orderBy || null,
    error: read.error,
  };
}

function summaryPopulation(
  read: TableRead,
  options: { limit: number; orderBy: string; detailRows: number; definition: string },
) {
  return {
    definition: options.definition,
    summaryBasis: 'loaded_rows',
    exactMatchingRows: read.available && read.queried ? read.count : null,
    loadedRows: read.rows.length,
    detailRows: Math.min(read.rows.length, options.detailRows),
    truncated: read.available && read.queried && read.count > read.rows.length,
    limit: options.limit,
    orderBy: options.orderBy,
  };
}

function domainSourceHealth(reads: TableRead[]) {
  const queried = reads.filter((read) => read.queried);
  const unavailable = queried.filter((read) => !read.available).length;
  const sampled = queried.filter((read) => read.available && read.count > read.rows.length).length;
  return {
    status: unavailable === queried.length && queried.length > 0
      ? 'unavailable'
      : unavailable > 0
        ? 'degraded'
        : 'available',
    queriedSources: queried.length,
    unavailableSources: unavailable,
    sampledSources: sampled,
  };
}

function mediaRouteUrl(postKey: unknown): string | null {
  const key = asString(postKey);
  return key ? `/api/media?postKey=${encodeURIComponent(key)}&role=thumbnail` : null;
}

function decoratePostRows<T extends DbRow>(
  rows: T[],
  posts: DbRow[],
  feeders: DbRow[],
  mediaAssets: DbRow[],
): T[] {
  const postByKey = new Map(posts.map((post) => [asString(post.post_key), post]).filter((entry): entry is [string, DbRow] => Boolean(entry[0])));
  const feederById = new Map(feeders.map((feeder) => [asNumber(feeder.id), feeder]).filter((entry): entry is [number, DbRow] => entry[0] != null));
  const assetByPostKey = new Map(
    mediaAssets
      .filter((asset) => ['thumbnail', 'display', 'carousel_0', 'carousel_00', 'carousel_01', 'carousel_1'].includes((asString(asset.asset_role) || '').toLowerCase()))
      .map((asset) => [asString(asset.post_key), asset])
      .filter((entry): entry is [string, DbRow] => Boolean(entry[0])),
  );

  return rows.map((row) => {
    const postKey = asString(row.post_key);
    const post = postKey ? postByKey.get(postKey) : null;
    const feederId = post ? asNumber(post.feeder_id) : null;
    const feeder = feederId != null ? feederById.get(feederId) : null;
    return {
      ...row,
      post_key: postKey,
      feeder_id: feederId,
      feeder_handle: feeder ? asString(feeder.handle) : null,
      media_type: post ? asString(post.media_type) : null,
      thumbnail_url: postKey && (assetByPostKey.has(postKey) || post) ? mediaRouteUrl(postKey) : null,
      posted_at: post ? asString(post.posted_at) : null,
    };
  });
}

function fireMetricKey(row: DbRow): string | null {
  const stored = (asString(row.ranking_metric) || '').toLowerCase();
  if (['engagement_rate', 'likes', 'comments', 'views'].includes(stored)) return stored;

  const candidates = [
    ['engagement_rate', asNumber(row.engagement_rate_multiple)],
    ['likes', asNumber(row.likes_multiple)],
    ['comments', asNumber(row.comments_multiple)],
    ['views', asNumber(row.views_multiple)],
  ] as const;
  let bestKey: string | null = null;
  let bestMultiple = Number.NEGATIVE_INFINITY;
  for (const [key, multiple] of candidates) {
    if (multiple != null && multiple > bestMultiple) {
      bestKey = key;
      bestMultiple = multiple;
    }
  }
  return bestKey;
}

function fireMetricValue(row: DbRow, metricKey: string | null): number | null {
  if (!metricKey) return asNumber(row.metric_value);
  return asNumber(row[metricKey]) ?? asNumber(row.metric_value);
}

/** Canonical Fire is a projection of post_metrics, matching the live Fire surface. */
function buildCanonicalFireTracking(postMetrics: DbRow[], posts: DbRow[], feeders: DbRow[]) {
  const postByKey = new Map(
    posts
      .map((post) => [asString(post.post_key), post])
      .filter((entry): entry is [string, DbRow] => Boolean(entry[0])),
  );
  const feederById = new Map(
    feeders
      .map((feeder) => [asNumber(feeder.id), feeder])
      .filter((entry): entry is [number, DbRow] => entry[0] != null),
  );
  const latestByPost = new Map<string, DbRow>();

  for (const metric of postMetrics) {
    const postKey = asString(metric.post_key);
    if (!postKey) continue;
    const current = latestByPost.get(postKey);
    if (!current || asDateMs(metric.computed_at) > asDateMs(current.computed_at)) {
      latestByPost.set(postKey, metric);
    }
  }

  return Array.from(latestByPost.values())
    .map((metric) => {
      const postKey = asString(metric.post_key);
      const post = postKey ? postByKey.get(postKey) : null;
      const feederId = post ? asNumber(post.feeder_id) : null;
      const feeder = feederId != null ? feederById.get(feederId) : null;
      const feedId = feeder ? asNumber(feeder.feed_id) : null;
      const checkpoint = asString(metric.checkpoint, 'tracking');
      const metricKey = fireMetricKey(metric);
      const percentile = asNumber(metric.percentile_performance_exact)
        ?? asNumber(metric.percentile_performance);
      const multiple = metricKey ? asNumber(metric[`${metricKey}_multiple`]) : null;
      const metricValue = fireMetricValue(metric, metricKey);
      const bodyParts = [
        percentile != null ? `Top ${percentile}%` : null,
        multiple != null ? `${Math.round(multiple * 100) / 100}x ${labelize(metricKey, 'metric')}` : null,
      ].filter(Boolean);
      const computedAt = asString(metric.computed_at);

      return {
        ...metric,
        id: `tracking:${postKey || 'unknown'}:${checkpoint}`,
        dedupe_key: `tracking:${postKey || 'unknown'}:${checkpoint}`,
        feed_id: feedId,
        feeder_id: feederId,
        feeder_handle: feeder ? asString(feeder.handle) : null,
        post_key: postKey,
        card_kind: 'tracking',
        signal_code: 'TRACKING_BASE',
        context: 'own',
        alert_type: 'tracking',
        status: 'done',
        metric_key: metricKey,
        metric_value: metricValue,
        surface_percentile: percentile,
        surface_percentile_exact: percentile,
        surface_delta: asNumber(metric.delta_from_d1),
        body: bodyParts.join(' / ') || `${labelize(checkpoint)} metric tracking`,
        is_hot: percentile != null && percentile <= FIRE_HOT_PERCENTILE_MAX,
        source_contract: 'post_metrics_tracking',
        created_at: computedAt,
        updated_at: computedAt,
        thumbnail_url: postKey ? mediaRouteUrl(postKey) : null,
      };
    })
    .sort((a, b) => asDateMs(b.updated_at) - asDateMs(a.updated_at));
}

function eventHealth(status: unknown): 'smooth' | 'pending' | 'failed' {
  const key = (asString(status) || '').toLowerCase();
  if (['failed', 'error', 'capture_failed', 'purge_failed', 'unavailable'].includes(key)) return 'failed';
  if (['pending', 'retry', 'running', 'capturing', 'purging', 'pending_capture', 'purge_pending'].includes(key)) return 'pending';
  return 'smooth';
}

function buildFeedOps({
  feeds,
  feeders,
  posts,
  runJobs,
  checkpointJobs,
  postMetrics,
  signals,
  mediaAssets,
  pushJobs,
  modelCalls,
}: {
  feeds: DbRow[];
  feeders: DbRow[];
  posts: DbRow[];
  runJobs: DbRow[];
  checkpointJobs: DbRow[];
  postMetrics: DbRow[];
  signals: DbRow[];
  mediaAssets: DbRow[];
  pushJobs: DbRow[];
  modelCalls: DbRow[];
}) {
  const feedById = new Map(feeds.map((feed) => [asNumber(feed.id), feed]).filter((entry): entry is [number, DbRow] => entry[0] != null));
  const feederById = new Map(feeders.map((feeder) => [asNumber(feeder.id), feeder]).filter((entry): entry is [number, DbRow] => entry[0] != null));
  const feederByHandle = new Map(feeders.map((feeder) => [asString(feeder.handle), feeder]).filter((entry): entry is [string, DbRow] => Boolean(entry[0])));
  const postByKey = new Map(posts.map((post) => [asString(post.post_key), post]).filter((entry): entry is [string, DbRow] => Boolean(entry[0])));

  type FeedOpsEvent = OperationalEvent & {
    feedName: string | null;
    feederHandle: string | null;
    thumbnailUrl: string | null;
    title: string;
    detail: string;
  };

  function owner(input: { feedId?: unknown; feederId?: unknown; feederHandle?: unknown; postKey?: unknown }) {
    const post = asString(input.postKey) ? postByKey.get(asString(input.postKey) || '') : null;
    const feederId = asNumber(input.feederId) ?? (post ? asNumber(post.feeder_id) : null);
    const feeder = feederId != null
      ? feederById.get(feederId)
      : feederByHandle.get(asString(input.feederHandle) || '');
    const feedId = asNumber(input.feedId) ?? (feeder ? asNumber(feeder.feed_id) : null);
    const feed = feedId != null ? feedById.get(feedId) : null;
    return { feed, feedId, feeder, feederId, post };
  }

  function event(
    row: DbRow,
    source: string,
    kind: string,
    title: string,
    detail: string,
    dates: {
      happenedAt?: unknown;
      nextRunAt?: unknown;
      scheduledAt?: unknown;
      dueAt?: unknown;
      claimableAt?: unknown;
      isOperational?: boolean;
    },
    input: { feedId?: unknown; feederId?: unknown; feederHandle?: unknown; postKey?: unknown } = {},
  ): FeedOpsEvent {
    const found = owner(input);
    const postKey = asString(input.postKey);
    const status = asString(row.status, 'done');
    const scheduledAt = asString(dates.scheduledAt) || null;
    const dueAt = asString(dates.dueAt) || null;
    const claimableAt = asString(dates.claimableAt) || null;
    const timing = queueTiming(status, { scheduledAt, dueAt, claimableAt });
    return {
      id: `${source}:${String(row.id ?? row.post_key ?? row.call_key ?? `${kind}:${postKey || 'none'}`)}`,
      feedId: found.feedId ?? null,
      feedName: found.feed ? asString(found.feed.name) : null,
      feederId: found.feederId ?? null,
      feederHandle: found.feeder ? asString(found.feeder.handle) : asString(input.feederHandle),
      postKey: postKey || null,
      thumbnailUrl: postKey ? mediaRouteUrl(postKey) : null,
      source,
      kind,
      status,
      title,
      detail,
      happenedAt: asString(dates.happenedAt) || null,
      nextRunAt: asString(dates.nextRunAt) || timing.actionableAt,
      scheduledAt,
      dueAt,
      claimableAt,
      isOperational: Boolean(dates.isOperational),
      isOpen: timing.isOpen,
      overdue: timing.overdue,
      queueState: timing.queueState,
    };
  }

  const events = [
    ...runJobs.map((row) => event(row, 'run_jobs', asString(row.job_type, 'run'), 'Discovery run', asString(row.last_error, `${asString(row.business_date_ist, 'Run')} / ${asString(row.discovery_slot, 'legacy')} slot`), {
      happenedAt: row.updated_at,
      nextRunAt: row.next_run_at,
      scheduledAt: row.next_run_at,
      dueAt: row.next_run_at,
      claimableAt: row.next_run_at,
      isOperational: true,
    }, { feederId: row.feeder_id })),
    ...checkpointJobs.map((row) => {
      const claimableAt = checkpointClaimableAt(row.next_run_at, row.attempt);
      return event(row, 'checkpoint_jobs', asString(row.checkpoint, 'checkpoint'), `Checkpoint ${asString(row.checkpoint, '')}`, asString(row.last_error, asString(row.post_key, 'Checkpoint job')), {
        happenedAt: row.updated_at,
        nextRunAt: claimableAt || row.next_run_at,
        scheduledAt: row.next_run_at,
        dueAt: row.next_run_at,
        claimableAt,
        isOperational: true,
      }, { postKey: row.post_key });
    }),
    ...postMetrics.map((row) => event({ ...row, status: 'done' }, 'post_metrics', asString(row.checkpoint, 'metric'), `Fire tracking ${asString(row.checkpoint, '')}`, `${asNumber(row.views) || 0} views`, { happenedAt: row.computed_at }, { postKey: row.post_key })),
    ...signals.map((row) => event(row, 'signals', asString(row.signal_family, 'signal'), labelize(row.signal_type, 'Signal'), asString(row.body, asString(row.scope, 'Signal')), { happenedAt: row.last_fired_at || row.updated_at }, { feedId: row.feed_id, feederId: row.feeder_id })),
    ...mediaAssets.map((row) => event(row, 'post_media_assets', asString(row.asset_role, 'media'), labelize(row.asset_role, 'Media asset'), asString(row.last_error, `${asString(row.storage_bucket, 'storage')} ${asString(row.mime_type, '')}`), {
      happenedAt: row.updated_at || row.captured_at,
      nextRunAt: row.next_run_at,
      scheduledAt: row.next_run_at,
      dueAt: row.next_run_at,
      claimableAt: row.next_run_at,
      isOperational: true,
    }, { postKey: row.post_key })),
    ...pushJobs.map((row) => event(row, 'web_push_jobs', asString(row.kind, 'push'), labelize(row.kind, 'Push job'), asString(row.last_error, asString(row.dedupe_key, 'Push job')), {
      happenedAt: row.updated_at || row.sent_at,
      nextRunAt: row.next_run_at,
      scheduledAt: row.next_run_at,
      dueAt: row.next_run_at,
      claimableAt: row.next_run_at,
      isOperational: true,
    }, { feedId: row.feed_id })),
    ...modelCalls.map((row) => event(row, 'feeder_file_model_calls', asString(row.call_type, 'model'), labelize(row.call_type, 'Model call'), asString(row.error, asString(row.model, 'Model call')), { happenedAt: row.completed_at || row.updated_at }, { feederHandle: row.feeder_handle, postKey: row.post_key })),
  ].filter((item) => item.feedId != null || item.feederId != null || item.postKey || item.source === 'web_push_jobs');

  function healthFor(items: typeof events) {
    const currentStates = currentOperationalStates(items);
    if (currentStates.some((item) => eventHealth(item.status) === 'failed')) return 'failed';
    if (currentStates.some((item) => eventHealth(item.status) === 'pending')) return 'pending';
    const latest = items.map((item) => asDateMs(item.happenedAt)).sort((a, b) => b - a)[0] || 0;
    if (latest && Date.now() - latest > STALE_HOURS * 60 * 60 * 1000) return 'stale';
    return items.length ? 'smooth' : 'missing';
  }

  function latestDate(items: typeof events) {
    const dates = items.map((item) => asDateMs(item.happenedAt)).filter(Boolean).sort((a, b) => b - a);
    return dates[0] ? new Date(dates[0]).toISOString() : null;
  }

  function nextScheduledDate(items: typeof events) {
    const dates = currentOperationalStates(items)
      .filter((item) => item.queueState === 'scheduled')
      .map((item) => asDateMs(item.claimableAt || item.dueAt || item.scheduledAt))
      .filter((value) => value > Date.now())
      .sort((a, b) => a - b);
    return dates[0] ? new Date(dates[0]).toISOString() : null;
  }

  const historicalFailures = events
    .filter((item) => eventHealth(item.status) === 'failed')
    .sort((a, b) => asDateMs(b.happenedAt) - asDateMs(a.happenedAt));
  const currentStates = currentOperationalStates(events);
  const failures = currentStates
    .filter((item) => eventHealth(item.status) === 'failed')
    .sort((a, b) => asDateMs(b.happenedAt) - asDateMs(a.happenedAt));
  const overdueWork = currentStates
    .filter((item) => item.queueState === 'overdue')
    .sort((a, b) => asDateMs(a.claimableAt || a.dueAt || a.scheduledAt) - asDateMs(b.claimableAt || b.dueAt || b.scheduledAt));
  const inProgress = currentStates
    .filter((item) => item.queueState === 'in_progress')
    .sort((a, b) => asDateMs(b.happenedAt) - asDateMs(a.happenedAt));
  const queuedWork = currentStates
    .filter((item) => item.queueState === 'queued')
    .sort((a, b) => asDateMs(b.happenedAt) - asDateMs(a.happenedAt));
  const scheduledAhead = currentStates
    .filter((item) => item.queueState === 'scheduled')
    .sort((a, b) => asDateMs(a.claimableAt || a.dueAt || a.scheduledAt) - asDateMs(b.claimableAt || b.dueAt || b.scheduledAt));
  const pendingAhead = [...overdueWork, ...inProgress, ...queuedWork, ...scheduledAhead].slice(0, 80);
  const recentActivity = events
    .filter((item) => item.happenedAt)
    .sort((a, b) => asDateMs(b.happenedAt) - asDateMs(a.happenedAt))
    .slice(0, 80);

  const cards = feeds.map((feed) => {
    const feedId = asNumber(feed.id);
    const feedFeeders = feeders.filter((feeder) => asNumber(feeder.feed_id) === feedId);
    const feedEvents = events.filter((item) => item.feedId === feedId);
    const feedFailures = failures.filter((item) => item.feedId === feedId);
    const feedHistoricalFailures = historicalFailures.filter((item) => item.feedId === feedId);
    const feedStates = currentOperationalStates(feedEvents);
    return {
      feedId,
      feedName: asString(feed.name, 'Untitled feed'),
      status: asString(feed.status, 'unknown'),
      health: healthFor(feedEvents),
      feederCount: feedFeeders.length,
      activeFeederCount: countWhere(feedFeeders, (row) => asString(row.status) === 'active'),
      pausedFeederCount: countWhere(feedFeeders, (row) => asString(row.status) === 'paused'),
      lastActivityAt: latestDate(feedEvents),
      nextWorkAt: nextScheduledDate(feedEvents),
      latestFailure: feedFailures[0] || null,
      currentFailureCount: feedFailures.length,
      historicalFailureCount: feedHistoricalFailures.length,
      overdueCount: feedStates.filter((item) => item.overdue).length,
      feeders: feedFeeders.map((feeder) => {
        const feederId = asNumber(feeder.id);
        const feederEvents = events.filter((item) => item.feederId === feederId);
        const feederStates = currentOperationalStates(feederEvents);
        return {
          feederId,
          feedId,
          handle: asString(feeder.handle, 'unknown'),
          role: asString(feeder.role, 'standard'),
          status: asString(feeder.status, 'unknown'),
          health: healthFor(feederEvents),
          followerCount: asNumber(feeder.follower_count),
          lastActivityAt: latestDate(feederEvents),
          nextWorkAt: nextScheduledDate(feederEvents),
          pendingCount: feederStates.filter((item) => item.isOpen).length,
          overdueCount: feederStates.filter((item) => item.overdue).length,
          failureCount: feederStates.filter((item) => eventHealth(item.status) === 'failed').length,
          historicalFailureCount: feederEvents.filter((item) => eventHealth(item.status) === 'failed').length,
        };
      }),
    };
  });

  return {
    feeds: cards,
    currentState: currentStates.slice(0, 240),
    recentActivity,
    pendingAhead,
    failures: failures.slice(0, 80),
    scheduledAhead: scheduledAhead.slice(0, 80),
    overdueWork: overdueWork.slice(0, 80),
    inProgress: inProgress.slice(0, 80),
    historicalFailures: historicalFailures.slice(0, 80),
    semantics: {
      current: 'Latest state per operational source, kind, and feed/feeder lane.',
      upcoming: 'Open operational work with a future claimableAt; signals are excluded.',
      overdue: 'Open queued work whose claimableAt has passed; running work is in-progress, not overdue.',
      history: 'Recent activity and historical failures are retained separately from current health.',
    },
  };
}

export async function GET(request: NextRequest) {
  const auth = await requireUser();
  if (auth.response || !auth.user) return auth.response;

  const allowlist = adminEmails();
  const isAllowlistedAdmin = Boolean(
    auth.user.email && allowlist.includes(auth.user.email.toLowerCase()),
  );
  const requireAdmin = process.env.COMMAND_HUB_REQUIRE_ADMIN?.trim().toLowerCase() === 'true';
  if (requireAdmin && !isAllowlistedAdmin) {
    return NextResponse.json(
      {
        error: allowlist.length === 0
          ? 'Command Hub admin access is required, but COMMAND_HUB_ADMIN_EMAILS is empty.'
          : 'Forbidden: this account is not authorized for the Command Hub.',
      },
      { status: 403 },
    );
  }
  const accessMode: AccessMode = allowlist.length > 0 && isAllowlistedAdmin
    ? 'platform_admin'
    : 'signed_in_account';

  const sb = getSupabase(true);
  const generatedAt = new Date().toISOString();

  const users = await readTable('users', () => {
    const query = sb
      .from('users')
      .select('id,email,name,balance,pwa_push_enabled,created_at,updated_at', { count: 'exact' })
      .order('updated_at', { ascending: false })
      .limit(accessMode === 'platform_admin' ? 200 : 1);

    return accessMode === 'platform_admin'
      ? query
      : query.eq('id', auth.user.id);
  });

  const feeds = await readTable('feeds', () => {
    const query = sb
      .from('feeds')
      .select('id,user_id,name,status,context_brief,context_bible,created_at,updated_at', { count: 'exact' })
      .order('updated_at', { ascending: false })
      .limit(accessMode === 'platform_admin' ? 400 : 80);

    return accessMode === 'platform_admin'
      ? query
      : query.eq('user_id', auth.user.id);
  });

  const feedIds = feeds.rows
    .map((row) => asNumber(row.id))
    .filter((id): id is number => id != null);

  const feeders = feedIds.length === 0
    ? emptyRead()
    : await readTable('feeders', () => sb
      .from('feeders')
      .select('id,feed_id,handle,role,status,context_role,follower_count,bio,created_at,updated_at', { count: 'exact' })
      .in('feed_id', feedIds)
      .order('updated_at', { ascending: false })
      .limit(accessMode === 'platform_admin' ? 900 : 260));

  const feederIds = feeders.rows
    .map((row) => asNumber(row.id))
    .filter((id): id is number => id != null);

  const posts = feederIds.length === 0
    ? emptyRead()
    : await readTable('posts', () => sb
      .from('posts')
      .select('post_key,feeder_id,media_type,posted_at,created_at,updated_at', { count: 'exact' })
      .in('feeder_id', feederIds)
      .order('updated_at', { ascending: false })
      .limit(accessMode === 'platform_admin' ? 1200 : 420));

  const postKeys = posts.rows
    .map((row) => asString(row.post_key))
    .filter((key): key is string => Boolean(key))
    .slice(0, 900);

  const [
    runJobs,
    checkpointJobs,
    postMetrics,
    fireAlerts,
    signals,
    postFingerprints,
    postCondensations,
    postBreakdowns,
    feederFiles,
    mediaAssets,
    pushSubscriptions,
    pushJobs,
    transactions,
    workerSnapshot,
    scheduleSnapshot,
    opsEvents,
  ] = await Promise.all([
    feederIds.length === 0
      ? Promise.resolve(emptyRead())
      : readTable('run_jobs', () => sb
        .from('run_jobs')
        .select('id,feeder_id,job_type,business_date_ist,discovery_slot,status,attempt,next_run_at,last_error,created_at,updated_at', { count: 'exact' })
        .in('feeder_id', feederIds)
        .order('updated_at', { ascending: false })
        .limit(360)),
    postKeys.length === 0
      ? Promise.resolve(emptyRead())
      : readTable('checkpoint_jobs', () => sb
        .from('checkpoint_jobs')
        .select('id,post_key,checkpoint,status,attempt,next_run_at,last_error,created_at,updated_at', { count: 'exact' })
        .in('post_key', postKeys)
        .order('updated_at', { ascending: false })
        .limit(520)),
    postKeys.length === 0
      ? Promise.resolve(emptyRead())
      : readTable('post_metrics', () => sb
        .from('post_metrics')
        .select('post_key,checkpoint,business_date_ist,computed_at,views,likes,comments,engagement_rate,metric_value,percentile_performance,percentile_performance_exact,ranking_metric,ranking_multiple,views_percentile,likes_percentile,comments_percentile,engagement_rate_percentile,views_baseline,likes_baseline,comments_baseline,engagement_rate_baseline,views_multiple,likes_multiple,comments_multiple,engagement_rate_multiple,hour_multiple,feed_percentile,delta_from_d1', { count: 'exact' })
        .in('post_key', postKeys)
        .order('computed_at', { ascending: false })
        .limit(680)),
    feedIds.length === 0
      ? Promise.resolve(emptyRead())
      : readTable('fire_alerts', () => sb
        .from('fire_alerts')
        .select('id,feed_id,feeder_id,post_key,checkpoint,business_date_ist,signal_code,alert_type,status,metric_key,metric_value,surface_percentile,surface_delta,body,created_at,updated_at', { count: 'exact' })
        .in('feed_id', feedIds)
        .order('created_at', { ascending: false })
        .limit(260)),
    feedIds.length === 0
      ? Promise.resolve(emptyRead())
      : readTable('signals', () => sb
        .from('signals')
        .select('id,feed_id,feeder_id,scope,signal_type,signal_family,media_type,checkpoint,business_date_ist,status,body,last_fired_at,created_at,updated_at', { count: 'exact' })
        .in('feed_id', feedIds)
        .order('updated_at', { ascending: false })
        .limit(260)),
    postKeys.length === 0
      ? Promise.resolve(emptyRead())
      : readTable('post_fingerprints', () => sb
        .from('post_fingerprints')
        .select('post_key,media_confidence,model_version,generated_at,updated_at', { count: 'exact' })
        .in('post_key', postKeys)
        .order('updated_at', { ascending: false })
        .limit(420)),
    postKeys.length === 0
      ? Promise.resolve(emptyRead())
      : readTable('post_condensations', () => sb
        .from('post_condensations')
        .select('post_key,condensation_version,model_version,model_call_id,generated_at,updated_at', { count: 'exact' })
        .in('post_key', postKeys)
        .order('updated_at', { ascending: false })
        .limit(420)),
    postKeys.length === 0
      ? Promise.resolve(emptyRead())
      : readTable('post_breakdowns', () => sb
        .from('post_breakdowns')
        .select('post_key,breakdown_version,source_fingerprint_model_version,generated_at,updated_at', { count: 'exact' })
        .in('post_key', postKeys)
        .order('updated_at', { ascending: false })
        .limit(420)),
    feederIds.length === 0
      ? Promise.resolve(emptyRead())
      : readTable('feeder_files', () => sb
        .from('feeder_files')
        .select('id,feeder_id,feeder_handle,compile_version,active_window,status,source,generated_at,updated_at', { count: 'exact' })
        .in('feeder_id', feederIds)
        .order('updated_at', { ascending: false })
        .limit(180)),
    postKeys.length === 0
      ? Promise.resolve(emptyRead())
      : readTable('post_media_assets', () => sb
        .from('post_media_assets')
        .select('id,post_key,asset_role,storage_bucket,storage_path,mime_type,byte_size,status,attempt,next_run_at,captured_at,purge_after,deleted_at,last_error,created_at,updated_at', { count: 'exact' })
        .in('post_key', postKeys)
        .order('updated_at', { ascending: false })
        .limit(420)),
    readTable('web_push_subscriptions', () => {
      const query = sb
        .from('web_push_subscriptions')
        .select('id,user_id,enabled,last_error,last_seen_at,failed_at,created_at,updated_at', { count: 'exact' })
        .order('updated_at', { ascending: false })
        .limit(accessMode === 'platform_admin' ? 360 : 80);

      return accessMode === 'platform_admin'
        ? query
        : query.eq('user_id', auth.user.id);
    }),
    readTable('web_push_jobs', () => {
      const query = sb
        .from('web_push_jobs')
        .select('id,user_id,kind,fire_alert_id,feed_id,dedupe_key,status,attempt,next_run_at,claimed_at,sent_at,last_error,created_at,updated_at', { count: 'exact' })
        .order('updated_at', { ascending: false })
        .limit(accessMode === 'platform_admin' ? 360 : 100);

      return accessMode === 'platform_admin'
        ? query
        : query.eq('user_id', auth.user.id);
    }),
    readTable('transactions', () => {
      const query = sb
        .from('transactions')
        .select('id,user_id,amount,status,razorpay_payment_id,created_at,updated_at', { count: 'exact' })
        .order('updated_at', { ascending: false })
        .limit(accessMode === 'platform_admin' ? 180 : 50);

      return accessMode === 'platform_admin'
        ? query
        : query.eq('user_id', auth.user.id);
    }),
    accessMode !== 'platform_admin'
      ? Promise.resolve(emptyRead())
      : readTable('command_worker_snapshot', () => sb
        .from('command_worker_snapshot')
        .select('worker_id,worker_kind,host_name,process_id,release_sha,started_at,last_seen_at,phase,current_task,last_error,metadata,heartbeat_age_seconds,health', { count: 'exact' })
        .order('last_seen_at', { ascending: false })
        .limit(100)),
    accessMode !== 'platform_admin'
      ? Promise.resolve(emptyRead())
      : readTable('command_schedule_snapshot', () => sb
        .from('command_schedule_snapshot')
        .select('schedule_key,display_name,cron_job_name,cron_expression,timezone,expected_max_lag,enabled,description,previous_run_at,next_run_at,cron_registered,last_status,last_scheduled_for,last_started_at,last_completed_at,last_enqueued_count,last_error,health', { count: 'exact' })
        .order('next_run_at', { ascending: true })
        .limit(50)),
    accessMode !== 'platform_admin'
      ? Promise.resolve(emptyRead())
      : readTable('ops_events', () => sb
        .from('ops_events')
        .select('id,occurred_at,event_type,severity,source,actor_type,actor_id,entity_type,entity_id,feed_id,feeder_id,post_key,incident_key,transition_from,transition_to,correlation_id,message,release_sha,data', { count: 'exact' })
        .order('occurred_at', { ascending: false })
        .limit(160)),
  ]);

  const feederFileIds = feederFiles.rows
    .map((row) => asNumber(row.id))
    .filter((id): id is number => id != null);

  const [feederFilePatterns, modelCalls] = await Promise.all([
    accessMode === 'platform_admin'
      ? readTable('feeder_file_patterns', () => sb
        .from('feeder_file_patterns')
        .select('id,feeder_file_id,feeder_handle,pattern_id,status,core_post_count,support_post_count,pattern_model,proof_model,generated_at,updated_at', { count: 'exact' })
        .order('updated_at', { ascending: false })
        .limit(220))
      : feederFileIds.length === 0
        ? Promise.resolve(emptyRead())
        : readTable('feeder_file_patterns', () => sb
          .from('feeder_file_patterns')
          .select('id,feeder_file_id,feeder_handle,pattern_id,status,core_post_count,support_post_count,pattern_model,proof_model,generated_at,updated_at', { count: 'exact' })
          .in('feeder_file_id', feederFileIds)
          .order('updated_at', { ascending: false })
          .limit(220)),
    accessMode === 'platform_admin'
      ? readTable('feeder_file_model_calls', () => sb
        .from('feeder_file_model_calls')
        .select('id,feeder_file_id,call_key,call_type,feeder_handle,pattern_id,post_key,model,prompt_version,status,error,started_at,completed_at,updated_at', { count: 'exact' })
        .order('updated_at', { ascending: false })
        .limit(240))
      : feederFileIds.length === 0
        ? Promise.resolve(emptyRead())
        : readTable('feeder_file_model_calls', () => sb
          .from('feeder_file_model_calls')
          .select('id,feeder_file_id,call_key,call_type,feeder_handle,pattern_id,post_key,model,prompt_version,status,error,started_at,completed_at,updated_at', { count: 'exact' })
          .in('feeder_file_id', feederFileIds)
          .order('updated_at', { ascending: false })
          .limit(240)),
  ]);

  const allReads = {
    users,
    feeds,
    feeders,
    posts,
    runJobs,
    checkpointJobs,
    postMetrics,
    fireAlerts,
    signals,
    postFingerprints,
    postCondensations,
    postBreakdowns,
    feederFiles,
    feederFilePatterns,
    modelCalls,
    mediaAssets,
    pushSubscriptions,
    pushJobs,
    transactions,
    workerSnapshot,
    scheduleSnapshot,
    opsEvents,
  };

  const activeFeeders = countWhere(feeders.rows, (row) => asString(row.status) === 'active');
  const pausedFeeders = countWhere(feeders.rows, (row) => asString(row.status) === 'paused');
  const activeFeeds = countWhere(feeds.rows, (row) => asString(row.status) === 'active');
  const contextCovered = countWhere(feeds.rows, (row) => {
    const brief = row.context_brief;
    const bible = asString(row.context_bible);
    return Boolean(bible) || Boolean(brief && typeof brief === 'object' && Object.keys(brief).length > 0);
  });
  const modelCallFailures = countWhere(modelCalls.rows, (row) => ['failed', 'error'].includes(asString(row.status) || ''));
  const activePushSubscriptions = countWhere(pushSubscriptions.rows, (row) => row.enabled === true);
  const paidTransactionsPaise = transactions.rows
    .filter((row) => asString(row.status) === 'paid')
    .reduce((total, row) => total + (asNumber(row.amount) || 0), 0);
  const pendingTransactionsPaise = transactions.rows
    .filter((row) => asString(row.status) === 'pending')
    .reduce((total, row) => total + (asNumber(row.amount) || 0), 0);
  const fireTrackingRows = buildCanonicalFireTracking(postMetrics.rows, posts.rows, feeders.rows);
  const feedOps = buildFeedOps({
    feeds: feeds.rows,
    feeders: feeders.rows,
    posts: posts.rows,
    runJobs: runJobs.rows,
    checkpointJobs: checkpointJobs.rows,
    postMetrics: postMetrics.rows,
    signals: signals.rows,
    mediaAssets: mediaAssets.rows,
    pushJobs: pushJobs.rows,
    modelCalls: modelCalls.rows,
  });
  const currentRunStates = feedOps.currentState.filter((row) => row.source === 'run_jobs');
  const currentCheckpointStates = feedOps.currentState.filter((row) => row.source === 'checkpoint_jobs');
  const currentMediaStates = feedOps.currentState.filter((row) => row.source === 'post_media_assets');
  const currentPushStates = feedOps.currentState.filter((row) => row.source === 'web_push_jobs');
  const completedRunJobs = currentRunStates.filter((row) => row.status === 'done').length;
  const failedRunJobs = currentRunStates.filter((row) => eventHealth(row.status) === 'failed').length;
  const openRunJobs = currentRunStates.filter((row) => row.isOpen).length;
  const completedCheckpointJobs = currentCheckpointStates.filter((row) => row.status === 'done').length;
  const failedCheckpointJobs = currentCheckpointStates.filter((row) => eventHealth(row.status) === 'failed').length;
  const openCheckpointJobs = currentCheckpointStates.filter((row) => row.isOpen).length;
  const mediaFailures = currentMediaStates.filter((row) => eventHealth(row.status) === 'failed').length;
  const pushFailures = currentPushStates.filter((row) => eventHealth(row.status) === 'failed').length;
  const erroredSignals = countWhere(signals.rows, (row) => asString(row.status) === 'error');
  const runtimeOffline = countWhere(workerSnapshot.rows, (row) => asString(row.health) === 'offline');
  const runtimeStale = countWhere(workerSnapshot.rows, (row) => asString(row.health) === 'stale');
  const failedSchedules = countWhere(scheduleSnapshot.rows, (row) => asString(row.health) === 'failed');
  const missedSchedules = countWhere(scheduleSnapshot.rows, (row) => asString(row.health) === 'missed');
  const unregisteredSchedules = countWhere(scheduleSnapshot.rows, (row) => asString(row.health) === 'unregistered');
  const lateSchedules = countWhere(scheduleSnapshot.rows, (row) => asString(row.health) === 'late');
  const coreSources = [
    ['users', users],
    ['feeds', feeds],
    ['feeders', feeders],
    ['posts', posts],
    ['run_jobs', runJobs],
    ['checkpoint_jobs', checkpointJobs],
    ['post_metrics', postMetrics],
    ['signals', signals],
    ['post_media_assets', mediaAssets],
    ['web_push_subscriptions', pushSubscriptions],
    ['web_push_jobs', pushJobs],
  ] as const;
  const readinessBlockers = [
    ...coreSources
      .filter(([, read]) => read.queried && !read.available)
      .map(([source, read]) => ({
        id: `source:${source}`,
        source,
        detail: read.error || 'Required source is unavailable.',
      })),
    ...feedOps.failures.map((event) => ({
      id: `current:${event.id}`,
      source: event.source,
      detail: event.detail,
    })),
    ...(erroredSignals > 0
      ? [{ id: 'signals:error', source: 'signals', detail: `${erroredSignals} current signal rows are in error.` }]
      : []),
    ...(runtimeOffline > 0
      ? [{ id: 'workers:offline', source: 'command_worker_snapshot', detail: `${runtimeOffline} workers are offline.` }]
      : []),
    ...(failedSchedules > 0
      ? [{ id: 'schedules:failed', source: 'command_schedule_snapshot', detail: `${failedSchedules} schedules last failed.` }]
      : []),
    ...(missedSchedules > 0
      ? [{ id: 'schedules:missed', source: 'command_schedule_snapshot', detail: `${missedSchedules} schedules missed their latest nominal run.` }]
      : []),
    ...(unregisteredSchedules > 0
      ? [{ id: 'schedules:unregistered', source: 'command_schedule_snapshot', detail: `${unregisteredSchedules} schedules are missing, inactive, or drifted from their registered pg_cron contract.` }]
      : []),
  ];
  const readinessWarnings = [
    {
      id: 'notifications:producer-retired',
      source: 'notifications',
      detail: 'Subscriptions and historical jobs are readable, but automatic production delivery is not wired.',
    },
    ...(feedOps.overdueWork.length > 0
      ? [{ id: 'queue:overdue', source: 'feedOps', detail: `${feedOps.overdueWork.length} current jobs are overdue.` }]
      : []),
    ...(runtimeStale > 0
      ? [{ id: 'workers:stale', source: 'command_worker_snapshot', detail: `${runtimeStale} workers have stale heartbeats.` }]
      : []),
    ...(lateSchedules > 0
      ? [{ id: 'schedules:late', source: 'command_schedule_snapshot', detail: `${lateSchedules} schedules are late.` }]
      : []),
    ...(accessMode === 'platform_admin' && !workerSnapshot.available
      ? [{ id: 'workers:unavailable', source: 'command_worker_snapshot', detail: workerSnapshot.error || 'Worker telemetry is not installed.' }]
      : []),
    ...(accessMode === 'platform_admin' && !scheduleSnapshot.available
      ? [{ id: 'schedules:unavailable', source: 'command_schedule_snapshot', detail: scheduleSnapshot.error || 'Schedule telemetry is not installed.' }]
      : []),
  ];

  const payload = {
    generatedAt,
    access: {
      mode: accessMode,
      signedInEmail: auth.user.email || null,
      adminAllowlistConfigured: allowlist.length > 0,
      strictAdminRequired: requireAdmin,
      note: accessMode === 'platform_admin'
        ? 'Showing platform-wide read-only data because the signed-in email matches COMMAND_HUB_ADMIN_EMAILS.'
        : 'Showing the signed-in account scope because strict admin mode is disabled. Configure COMMAND_HUB_ADMIN_EMAILS and COMMAND_HUB_REQUIRE_ADMIN=true for a founder-only production hub.',
    },
    operationalReadiness: {
      policyVersion: 'command-v2',
      status: readinessBlockers.length > 0
        ? 'blocked'
        : readinessWarnings.length > 0
          ? 'watch'
          : 'ready',
      includedDomains: ['account', 'engine', 'checkpoints', 'fire', 'media', 'notifications', 'runtime'],
      excludedDomains: {
        intelligence: {
          status: 'not_connected',
          reason: 'Intelligence is intentionally excluded until its production contract is wired.',
        },
        finance: {
          status: 'not_connected',
          reason: 'Finance is intentionally excluded until a normalized production ledger is wired.',
        },
      },
      blockers: readinessBlockers.slice(0, 80),
      warnings: readinessWarnings,
    },
    sources: {
      users: sourceReadState(users, { limit: accessMode === 'platform_admin' ? 200 : 1, orderBy: 'updated_at desc' }),
      feeds: sourceReadState(feeds, { limit: accessMode === 'platform_admin' ? 400 : 80, orderBy: 'updated_at desc' }),
      feeders: sourceReadState(feeders, { limit: accessMode === 'platform_admin' ? 900 : 260, orderBy: 'updated_at desc' }),
      posts: sourceReadState(posts, { limit: accessMode === 'platform_admin' ? 1200 : 420, orderBy: 'updated_at desc' }),
      runJobs: sourceReadState(runJobs, { limit: 360, orderBy: 'updated_at desc' }),
      checkpointJobs: sourceReadState(checkpointJobs, { limit: 520, orderBy: 'updated_at desc' }),
      postMetrics: sourceReadState(postMetrics, { limit: 680, orderBy: 'computed_at desc' }),
      legacyFireAlerts: sourceReadState(fireAlerts, { limit: 260, orderBy: 'created_at desc' }),
      signals: sourceReadState(signals, { limit: 260, orderBy: 'updated_at desc' }),
      mediaAssets: sourceReadState(mediaAssets, { limit: 420, orderBy: 'updated_at desc' }),
      pushSubscriptions: sourceReadState(pushSubscriptions, { limit: accessMode === 'platform_admin' ? 360 : 80, orderBy: 'updated_at desc' }),
      pushJobs: sourceReadState(pushJobs, { limit: accessMode === 'platform_admin' ? 360 : 100, orderBy: 'updated_at desc' }),
      workerSnapshot: sourceReadState(workerSnapshot, { limit: 100, orderBy: 'last_seen_at desc' }),
      scheduleSnapshot: sourceReadState(scheduleSnapshot, { limit: 50, orderBy: 'next_run_at asc' }),
      opsEvents: sourceReadState(opsEvents, { limit: 160, orderBy: 'occurred_at desc' }),
    },
    runtime: {
      sourceHealth: domainSourceHealth([workerSnapshot, scheduleSnapshot]),
      workers: {
        totals: {
          workers: workerSnapshot.rows.length,
          healthy: countWhere(workerSnapshot.rows, (row) => asString(row.health) === 'healthy'),
          stale: runtimeStale,
          offline: runtimeOffline,
        },
        rows: workerSnapshot.rows,
        latestChangeAt: latestIso(workerSnapshot.rows, ['last_seen_at', 'started_at']),
      },
      schedules: {
        totals: {
          schedules: scheduleSnapshot.rows.length,
          healthy: countWhere(scheduleSnapshot.rows, (row) => asString(row.health) === 'healthy'),
          failed: failedSchedules,
          missed: missedSchedules,
          unregistered: unregisteredSchedules,
          late: lateSchedules,
          unobserved: countWhere(scheduleSnapshot.rows, (row) => asString(row.health) === 'unobserved'),
          disabled: countWhere(scheduleSnapshot.rows, (row) => asString(row.health) === 'disabled'),
        },
        rows: scheduleSnapshot.rows,
        nextRunAt: scheduleSnapshot.rows
          .map((row) => asString(row.next_run_at))
          .filter((value): value is string => Boolean(value) && asDateMs(value) > Date.now())
          .sort((a, b) => asDateMs(a) - asDateMs(b))[0] || null,
        latestChangeAt: latestIso(scheduleSnapshot.rows, ['last_completed_at', 'last_started_at', 'last_scheduled_for']),
      },
    },
    timeline: {
      sourceHealth: domainSourceHealth([opsEvents]),
      events: opsEvents.rows,
      latestEventAt: latestIso(opsEvents.rows, ['occurred_at']),
      population: summaryPopulation(opsEvents, {
        limit: 160,
        orderBy: 'occurred_at desc',
        detailRows: 160,
        definition: 'Latest append-only operational events in platform scope.',
      }),
    },
    pricing: {
      plannedPriceInrPerFeeder: FEEDER_PRICE_INR,
      razorpayLive: false,
      brightDataUsdPer1000Records: BRIGHT_DATA_USD_PER_1K_RECORDS,
    },
    topline: {
      users: users.count,
      feeds: feeds.count,
      activeFeeds,
      feeders: feeders.count,
      activeFeeders,
      pausedFeeders,
      posts: posts.count,
      followerReach: sumNumber(feeders.rows, 'follower_count'),
      contextCoveragePercent: percent(contextCovered, feeds.rows.length),
      plannedMonthlyRevenueInr: activeFeeders * FEEDER_PRICE_INR,
      knownPaidInr: Math.round(paidTransactionsPaise / 100),
      knownPendingInr: Math.round(pendingTransactionsPaise / 100),
      knownMediaBytes: sumNumber(mediaAssets.rows, 'byte_size'),
    },
    accountGraph: {
      sourceHealth: domainSourceHealth([users, feeds, feeders]),
      feedsByStatus: countBy(feeds.rows, 'status'),
      feedersByStatus: countBy(feeders.rows, 'status'),
      feedersByRole: countBy(feeders.rows, 'role'),
      contextCovered,
      latestChangeAt: latestIso([...feeds.rows, ...feeders.rows], ['updated_at', 'created_at']),
      feeds: pickRows(feeds.rows, 24),
      feeders: pickRows(feeders.rows, 48),
    },
    feedOps,
    engine: {
      sourceHealth: domainSourceHealth([runJobs]),
      population: {
        ...summaryPopulation(runJobs, {
          limit: 360,
          orderBy: 'updated_at desc',
          detailRows: 40,
          definition: 'Latest run-job rows for the scoped feeders.',
        }),
        summaryBasis: 'latest_state_per_source_kind_and_feeder',
        currentStateRows: currentRunStates.length,
      },
      totals: {
        jobs: currentRunStates.length,
        exactMatchingHistoryRows: runJobs.available ? runJobs.count : null,
        loadedHistoryRows: runJobs.rows.length,
        completed: completedRunJobs,
        open: openRunJobs,
        failed: failedRunJobs,
        successPercent: percent(completedRunJobs, completedRunJobs + failedRunJobs),
      },
      byStatus: countBy(currentRunStates.map((row) => ({ status: row.status })), 'status'),
      byType: countBy(currentRunStates.map((row) => ({ job_type: row.kind })), 'job_type'),
      recentJobs: pickRows(runJobs.rows, 40),
      recentErrors: recentErrors(runJobs.rows),
      latestChangeAt: latestIso(runJobs.rows, ['updated_at', 'created_at']),
    },
    checkpoints: {
      sourceHealth: domainSourceHealth([checkpointJobs, postMetrics]),
      population: {
        jobs: {
          ...summaryPopulation(checkpointJobs, {
            limit: 520,
            orderBy: 'updated_at desc',
            detailRows: 44,
            definition: 'Latest checkpoint-job rows for loaded scoped posts.',
          }),
          summaryBasis: 'latest_state_per_checkpoint_and_feeder',
          currentStateRows: currentCheckpointStates.length,
        },
        metrics: summaryPopulation(postMetrics, {
          limit: 680,
          orderBy: 'computed_at desc',
          detailRows: 32,
          definition: 'Latest metric rows for loaded scoped posts.',
        }),
      },
      totals: {
        jobs: currentCheckpointStates.length,
        exactMatchingHistoryRows: checkpointJobs.available ? checkpointJobs.count : null,
        loadedHistoryRows: checkpointJobs.rows.length,
        completed: completedCheckpointJobs,
        open: openCheckpointJobs,
        failed: failedCheckpointJobs,
        metrics: postMetrics.rows.length,
        exactMatchingMetrics: postMetrics.available ? postMetrics.count : null,
        successPercent: percent(completedCheckpointJobs, completedCheckpointJobs + failedCheckpointJobs),
      },
      jobsByStatus: countBy(currentCheckpointStates.map((row) => ({ status: row.status })), 'status'),
      jobsByCheckpoint: countBy(currentCheckpointStates.map((row) => ({ checkpoint: row.kind })), 'checkpoint'),
      metricsByCheckpoint: countBy(postMetrics.rows, 'checkpoint'),
      recentJobs: decoratePostRows(pickRows(checkpointJobs.rows, 44), posts.rows, feeders.rows, mediaAssets.rows),
      recentMetrics: decoratePostRows(pickRows(postMetrics.rows, 32), posts.rows, feeders.rows, mediaAssets.rows),
      recentErrors: recentErrors(checkpointJobs.rows),
      latestChangeAt: latestIso([...checkpointJobs.rows, ...postMetrics.rows], ['updated_at', 'computed_at', 'created_at']),
    },
    fireSignals: {
      sourceContract: 'post_metrics_tracking',
      sourceHealth: domainSourceHealth([postMetrics, signals]),
      population: {
        tracking: {
          definition: `Latest loaded post metric per post; hot means percentile <= ${FIRE_HOT_PERCENTILE_MAX}.`,
          summaryBasis: 'latest_metric_per_post',
          exactMatchingMetricRows: postMetrics.available ? postMetrics.count : null,
          loadedMetricRows: postMetrics.rows.length,
          currentTrackingRows: fireTrackingRows.length,
          truncated: postMetrics.available && postMetrics.count > postMetrics.rows.length,
        },
        signals: summaryPopulation(signals, {
          limit: 260,
          orderBy: 'updated_at desc',
          detailRows: 36,
          definition: 'Latest signal rows for scoped feeds.',
        }),
      },
      totals: {
        alerts: fireTrackingRows.length,
        trackingRows: fireTrackingRows.length,
        exactMatchingMetricRows: postMetrics.available ? postMetrics.count : null,
        signals: signals.rows.length,
        exactMatchingSignals: signals.available ? signals.count : null,
        hotPosts: countWhere(fireTrackingRows, (row) => row.is_hot === true),
        legacyAlertHistoryRows: fireAlerts.rows.length,
        staleOrSuppressedSignals: countWhere(signals.rows, (row) => ['stale', 'suppressed_cap', 'suppressed_confidence'].includes(asString(row.status) || '')),
        erroredSignals,
      },
      alertsByStatus: countBy(fireTrackingRows, 'status'),
      alertsByType: countBy(fireTrackingRows, 'alert_type'),
      trackingByCheckpoint: countBy(fireTrackingRows, 'checkpoint'),
      signalsByFamily: countBy(signals.rows, 'signal_family'),
      signalsByStatus: countBy(signals.rows, 'status'),
      recentAlerts: pickRows(fireTrackingRows, 36),
      recentSignals: pickRows(signals.rows, 36),
      legacyAlertHistory: {
        status: fireAlerts.available ? 'history_only' : 'unavailable',
        sourceContract: 'legacy_fire_alerts_history',
        count: fireAlerts.available ? fireAlerts.count : null,
        rows: pickRows(fireAlerts.rows, 24),
        latestChangeAt: latestIso(fireAlerts.rows, ['updated_at', 'created_at']),
        error: fireAlerts.error,
      },
      latestChangeAt: latestIso([...postMetrics.rows, ...signals.rows], ['computed_at', 'updated_at', 'last_fired_at', 'created_at']),
    },
    intelligence: {
      readiness: {
        included: false,
        status: 'not_connected',
      },
      totals: {
        fingerprints: postFingerprints.count,
        condensations: postCondensations.count,
        postBreakdowns: postBreakdowns.count,
        feederFiles: feederFiles.count,
        patterns: feederFilePatterns.count,
        modelCalls: modelCalls.count,
        failedModelCalls: modelCallFailures,
      },
      modelCallsByType: countBy(modelCalls.rows, 'call_type'),
      modelCallsByStatus: countBy(modelCalls.rows, 'status'),
      fingerprintConfidence: countBy(postFingerprints.rows, 'media_confidence'),
      feederFilesByStatus: countBy(feederFiles.rows, 'status'),
      artifacts: {
        fingerprints: pickRows(postFingerprints.rows, 18),
        condensations: pickRows(postCondensations.rows, 18),
        postBreakdowns: pickRows(postBreakdowns.rows, 18),
        feederFiles: pickRows(feederFiles.rows, 18),
        patterns: pickRows(feederFilePatterns.rows, 18),
      },
      recentModelCalls: pickRows(modelCalls.rows, 36),
      recentErrors: recentErrors(modelCalls.rows),
      latestChangeAt: latestIso([...postFingerprints.rows, ...postCondensations.rows, ...postBreakdowns.rows, ...feederFiles.rows, ...modelCalls.rows], ['updated_at', 'generated_at', 'completed_at', 'started_at']),
    },
    media: {
      sourceHealth: domainSourceHealth([mediaAssets]),
      population: summaryPopulation(mediaAssets, {
        limit: 420,
        orderBy: 'updated_at desc',
        detailRows: 40,
        definition: 'Latest media-asset rows for loaded scoped posts.',
      }),
      totals: {
        assets: mediaAssets.rows.length,
        exactMatchingAssets: mediaAssets.available ? mediaAssets.count : null,
        activeAssets: countWhere(mediaAssets.rows, (row) => asString(row.status) === 'active'),
        pendingCapture: countWhere(mediaAssets.rows, (row) => ['pending_capture', 'capturing'].includes(asString(row.status) || '')),
        purgeQueue: countWhere(mediaAssets.rows, (row) => ['purge_pending', 'purging'].includes(asString(row.status) || '')),
        failed: mediaFailures,
        knownBytes: sumNumber(mediaAssets.rows, 'byte_size'),
      },
      byStatus: countBy(mediaAssets.rows, 'status'),
      byRole: countBy(mediaAssets.rows, 'asset_role'),
      byBucket: countBy(mediaAssets.rows, 'storage_bucket'),
      recentAssets: pickRows(mediaAssets.rows, 40),
      recentErrors: recentErrors(mediaAssets.rows),
      latestChangeAt: latestIso(mediaAssets.rows, ['updated_at', 'captured_at', 'deleted_at', 'created_at']),
    },
    notifications: {
      productionDeliveryWired: false,
      deliveryContract: {
        status: 'producer_retired',
        subscriptionsReadable: pushSubscriptions.available,
        historicalJobsReadable: pushJobs.available,
        automaticProducerWired: false,
        detail: 'The legacy fire-alert-to-web-push trigger is retired; test jobs and historical delivery rows may still exist.',
      },
      sourceHealth: domainSourceHealth([pushSubscriptions, pushJobs]),
      population: {
        subscriptions: summaryPopulation(pushSubscriptions, {
          limit: accessMode === 'platform_admin' ? 360 : 80,
          orderBy: 'updated_at desc',
          detailRows: 24,
          definition: 'Latest push subscriptions in access scope.',
        }),
        jobs: {
          ...summaryPopulation(pushJobs, {
            limit: accessMode === 'platform_admin' ? 360 : 100,
            orderBy: 'updated_at desc',
            detailRows: 36,
            definition: 'Latest historical/test push jobs in access scope.',
          }),
          summaryBasis: 'latest_state_per_kind_and_feed',
          currentStateRows: currentPushStates.length,
        },
      },
      totals: {
        subscriptions: pushSubscriptions.rows.length,
        exactMatchingSubscriptions: pushSubscriptions.available ? pushSubscriptions.count : null,
        activeSubscriptions: activePushSubscriptions,
        jobs: currentPushStates.length,
        exactMatchingHistoryRows: pushJobs.available ? pushJobs.count : null,
        loadedHistoryRows: pushJobs.rows.length,
        sentJobs: currentPushStates.filter((row) => row.status === 'sent').length,
        openJobs: currentPushStates.filter((row) => row.isOpen).length,
        failedJobs: pushFailures,
      },
      subscriptionsByEnabled: {
        enabled: activePushSubscriptions,
        disabled: Math.max(0, pushSubscriptions.rows.length - activePushSubscriptions),
      },
      jobsByStatus: countBy(currentPushStates.map((row) => ({ status: row.status })), 'status'),
      jobsByKind: countBy(currentPushStates.map((row) => ({ kind: row.kind })), 'kind'),
      recentSubscriptions: pickRows(pushSubscriptions.rows, 24),
      recentJobs: pickRows(pushJobs.rows, 36),
      recentErrors: recentErrors([...pushJobs.rows, ...pushSubscriptions.rows]),
      latestChangeAt: latestIso([...pushJobs.rows, ...pushSubscriptions.rows], ['updated_at', 'last_seen_at', 'sent_at', 'created_at']),
    },
    finance: {
      readiness: {
        included: false,
        status: 'not_connected',
      },
      assumptions: {
        plannedPriceInrPerFeeder: FEEDER_PRICE_INR,
        razorpayLive: false,
        brightDataUsdPer1000Records: BRIGHT_DATA_USD_PER_1K_RECORDS,
      },
      realSurfaces: {
        userBalanceInr: sumNumber(users.rows, 'balance'),
        transactionReferencesAvailable: transactions.available,
        paidTransactionsInr: Math.round(paidTransactionsPaise / 100),
        pendingTransactionsInr: Math.round(pendingTransactionsPaise / 100),
        transactionCount: transactions.count,
      },
      plannedMonthlyRevenueInr: activeFeeders * FEEDER_PRICE_INR,
      costLedgerAvailable: false,
      providerSpendAvailable: false,
      recentTransactions: pickRows(transactions.rows, 24),
    },
    productSurfaces: [
      {
        id: 'feed',
        label: 'Feed',
        route: '/',
        lifecycle: 'live',
        dataMode: 'real',
        gatesOperationalReadiness: true,
        sourceProbe: domainSourceHealth([feeds, feeders, posts]),
      },
      {
        id: 'fire',
        label: 'Fire',
        route: '/fire',
        lifecycle: 'live',
        dataMode: 'real',
        gatesOperationalReadiness: true,
        sourceProbe: domainSourceHealth([postMetrics]),
      },
      {
        id: 'fund',
        label: 'Fund / Profile',
        route: '/profile',
        lifecycle: 'beta',
        dataMode: 'real',
        gatesOperationalReadiness: false,
        sourceProbe: domainSourceHealth([users, pushSubscriptions]),
      },
      {
        id: 'lead',
        label: 'Lead',
        route: '/lead',
        lifecycle: 'beta',
        dataMode: 'real',
        gatesOperationalReadiness: false,
        sourceProbe: domainSourceHealth([feeds, posts, postMetrics]),
      },
      {
        id: 'read',
        label: 'Read',
        route: '/read',
        lifecycle: 'beta',
        dataMode: 'fallback',
        gatesOperationalReadiness: false,
        sourceProbe: {
          status: 'unprobed',
          detail: 'The surface can fall back to local dossier data when its live read fails.',
        },
      },
      {
        id: 'drop',
        label: 'Drop',
        route: '/drop',
        lifecycle: 'preview',
        dataMode: 'fixture',
        gatesOperationalReadiness: false,
        sourceProbe: { status: 'not_applicable', detail: 'Explicit fixture preview.' },
      },
      {
        id: 'visit',
        label: 'Visit',
        route: '/visit',
        lifecycle: 'preview',
        dataMode: 'fixture',
        gatesOperationalReadiness: false,
        sourceProbe: { status: 'not_applicable', detail: 'Explicit fixture preview.' },
      },
    ],
    instrumentationGaps: [
      ...(accessMode !== 'platform_admin'
        ? [{
          id: 'admin-allowlist',
          label: 'Platform-wide admin access',
          status: 'configuration_needed' as const,
          detail: 'COMMAND_HUB_ADMIN_EMAILS is not configured for this signed-in user.',
          path: 'Set COMMAND_HUB_ADMIN_EMAILS to founder/admin emails to unlock platform scope.',
        }]
        : []),
      {
        id: 'ai-token-cost',
        label: 'AI token usage and model cost',
        status: 'missing_instrumentation' as const,
        detail: 'Model-call audit rows exist, but token counts and provider pricing snapshots are not stored.',
        path: 'Add input/output token columns and a provider price snapshot to feeder_file_model_calls.',
      },
      {
        id: 'bright-data-records',
        label: 'Bright Data records by job/provider',
        status: 'missing_instrumentation' as const,
        detail: `Pricing assumption is $${BRIGHT_DATA_USD_PER_1K_RECORDS.toFixed(2)} per 1000 records, but record counts are not persisted by job.`,
        path: 'Persist records_requested, records_returned, provider, and job id for scraper calls.',
      },
      {
        id: 'supabase-cost',
        label: 'Supabase cost and usage',
        status: 'missing_instrumentation' as const,
        detail: 'Database request, storage, egress, and realtime usage are not available in product tables.',
        path: 'Ingest Supabase usage exports or scheduled billing snapshots.',
      },
      {
        id: 'vercel-cost',
        label: 'Vercel traffic, build, and runtime cost',
        status: 'missing_instrumentation' as const,
        detail: 'No request, bandwidth, build-minute, or function-duration ledger exists.',
        path: 'Ingest Vercel analytics and billing snapshots into a stack_usage table.',
      },
      {
        id: 'r2-bandwidth',
        label: 'R2 media storage and bandwidth cost',
        status: 'partial' as const,
        detail: 'Known media byte sizes are available, but bandwidth and provider cost snapshots are not.',
        path: 'Join post_media_assets byte_size with Cloudflare R2 storage and egress exports.',
      },
      {
        id: 'server-health',
        label: 'Server uptime, memory, CPU, heartbeat',
        status: workerSnapshot.available ? 'partial' as const : 'missing_instrumentation' as const,
        detail: workerSnapshot.available
          ? 'Worker heartbeat, release, phase, and last-error snapshots are available; CPU and memory samples are not yet persisted.'
          : 'Worker heartbeat and host resource history are not readable in this environment.',
        path: 'Deploy the heartbeat reporter and add memory and CPU samples to telemetry metadata.',
      },
      {
        id: 'notification-producer',
        label: 'Automatic production push delivery',
        status: 'partial' as const,
        detail: 'Subscriptions and job history exist, but the legacy Fire-to-push producer is intentionally retired.',
        path: 'Wire the approved signal/event producer to web_push_jobs before treating Notifications as live.',
      },
      {
        id: 'traffic-sources',
        label: 'Website traffic sources',
        status: 'missing_instrumentation' as const,
        detail: 'Acquisition source, referrer, and campaign metrics are not stored in product tables.',
        path: 'Add privacy-safe web analytics ingestion for traffic-source rollups.',
      },
      {
        id: 'finance-ledger',
        label: 'Real finance ledger',
        status: transactions.available ? 'partial' as const : 'missing_instrumentation' as const,
        detail: transactions.available
          ? 'Transaction references exist, but there is no full revenue, refunds, fees, tax, and provider-cost ledger.'
          : 'The transactions table was not readable from this environment.',
        path: 'Create a normalized finance_ledger fed by Razorpay, provider bills, credits, refunds, and cost snapshots.',
      },
      ...tableErrorGaps(allReads, new Set([
        'fireAlerts',
        'postFingerprints',
        'postCondensations',
        'postBreakdowns',
        'feederFiles',
        'feederFilePatterns',
        'modelCalls',
        'transactions',
        'workerSnapshot',
        'scheduleSnapshot',
        'opsEvents',
      ])),
    ],
  };

  return privateJsonResponse(request, payload, {
    maxAgeSeconds: COMMAND_ROUTE_TTL_SECONDS,
    staleWhileRevalidateSeconds: 120,
  });
}
