import { NextRequest, NextResponse } from 'next/server';
import { getSupabase } from '@/lib/supabase';
import { createClient } from '@/lib/supabase/server';
import { privateJsonResponse } from '@/lib/privateJsonResponse';

export const dynamic = 'force-dynamic';

const COMMAND_ROUTE_TTL_SECONDS = 20;
const FEEDER_PRICE_INR = 1499;
const BRIGHT_DATA_USD_PER_1K_RECORDS = 1.5;
const STALE_HOURS = 24;

type DbRow = Record<string, unknown>;

type TableRead<T extends DbRow = DbRow> = {
  available: boolean;
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
        rows: [],
        count: 0,
        error: `${label}: ${error.message}`,
      };
    }

    const rows = data || [];
    return {
      available: true,
      rows,
      count: count ?? rows.length,
      error: null,
    };
  } catch (error) {
    return {
      available: false,
      rows: [],
      count: 0,
      error: `${label}: ${error instanceof Error ? error.message : 'read failed'}`,
    };
  }
}

function emptyRead<T extends DbRow>(): TableRead<T> {
  return { available: true, rows: [], count: 0, error: null };
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
  if (typeof value !== 'string' || !value) return 0;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : 0;
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

function uniqCount(rows: DbRow[], key: string): number {
  return new Set(rows.map((row) => asString(row[key]) || String(row[key] || '')).filter(Boolean)).size;
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

function tableErrorGaps(reads: Record<string, TableRead>) {
  return Object.entries(reads)
    .filter(([, read]) => !read.available && read.error)
    .map(([label, read]) => ({
      id: `table:${label}`,
      label: `${label} table read`,
      status: 'unavailable' as const,
      detail: read.error || 'Read failed',
      path: 'Check Supabase table availability, privileges, or selected columns.',
    }));
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
  fireAlerts,
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
  fireAlerts: DbRow[];
  signals: DbRow[];
  mediaAssets: DbRow[];
  pushJobs: DbRow[];
  modelCalls: DbRow[];
}) {
  const feedById = new Map(feeds.map((feed) => [asNumber(feed.id), feed]).filter((entry): entry is [number, DbRow] => entry[0] != null));
  const feederById = new Map(feeders.map((feeder) => [asNumber(feeder.id), feeder]).filter((entry): entry is [number, DbRow] => entry[0] != null));
  const feederByHandle = new Map(feeders.map((feeder) => [asString(feeder.handle), feeder]).filter((entry): entry is [string, DbRow] => Boolean(entry[0])));
  const postByKey = new Map(posts.map((post) => [asString(post.post_key), post]).filter((entry): entry is [string, DbRow] => Boolean(entry[0])));

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

  function event(row: DbRow, source: string, kind: string, title: string, detail: string, dates: { happenedAt?: unknown; nextRunAt?: unknown }, input: { feedId?: unknown; feederId?: unknown; feederHandle?: unknown; postKey?: unknown } = {}) {
    const found = owner(input);
    const postKey = asString(input.postKey);
    return {
      id: `${source}:${String(row.id ?? row.post_key ?? row.call_key ?? `${kind}:${postKey || 'none'}`)}`,
      feedId: found.feedId,
      feedName: found.feed ? asString(found.feed.name) : null,
      feederId: found.feederId,
      feederHandle: found.feeder ? asString(found.feeder.handle) : asString(input.feederHandle),
      postKey,
      thumbnailUrl: postKey ? mediaRouteUrl(postKey) : null,
      source,
      kind,
      status: asString(row.status, 'done'),
      title,
      detail,
      happenedAt: asString(dates.happenedAt) || null,
      nextRunAt: asString(dates.nextRunAt) || null,
    };
  }

  const events = [
    ...runJobs.map((row) => event(row, 'run_jobs', asString(row.job_type, 'run'), 'Discovery run', asString(row.last_error, asString(row.business_date_ist, 'Run job')), { happenedAt: row.updated_at, nextRunAt: row.next_run_at }, { feederId: row.feeder_id })),
    ...checkpointJobs.map((row) => event(row, 'checkpoint_jobs', asString(row.checkpoint, 'checkpoint'), `Checkpoint ${asString(row.checkpoint, '')}`, asString(row.last_error, asString(row.post_key, 'Checkpoint job')), { happenedAt: row.updated_at, nextRunAt: row.next_run_at }, { postKey: row.post_key })),
    ...postMetrics.map((row) => event({ ...row, status: 'done' }, 'post_metrics', asString(row.checkpoint, 'metric'), `Metric ${asString(row.checkpoint, '')}`, `${asNumber(row.views) || 0} views`, { happenedAt: row.computed_at }, { postKey: row.post_key })),
    ...fireAlerts.map((row) => event(row, 'fire_alerts', asString(row.alert_type, 'fire'), labelize(row.alert_type, 'Fire alert'), asString(row.body, asString(row.signal_code, 'Fire alert')), { happenedAt: row.updated_at || row.created_at }, { feedId: row.feed_id, feederId: row.feeder_id, postKey: row.post_key })),
    ...signals.map((row) => event(row, 'signals', asString(row.signal_family, 'signal'), labelize(row.signal_type, 'Signal'), asString(row.body, asString(row.scope, 'Signal')), { happenedAt: row.last_fired_at || row.updated_at }, { feedId: row.feed_id, feederId: row.feeder_id })),
    ...mediaAssets.map((row) => event(row, 'post_media_assets', asString(row.asset_role, 'media'), labelize(row.asset_role, 'Media asset'), asString(row.last_error, `${asString(row.storage_bucket, 'storage')} ${asString(row.mime_type, '')}`), { happenedAt: row.updated_at || row.captured_at, nextRunAt: row.next_run_at }, { postKey: row.post_key })),
    ...pushJobs.map((row) => event(row, 'web_push_jobs', asString(row.kind, 'push'), labelize(row.kind, 'Push job'), asString(row.last_error, asString(row.dedupe_key, 'Push job')), { happenedAt: row.updated_at || row.sent_at, nextRunAt: row.next_run_at }, { feedId: row.feed_id })),
    ...modelCalls.map((row) => event(row, 'feeder_file_model_calls', asString(row.call_type, 'model'), labelize(row.call_type, 'Model call'), asString(row.error, asString(row.model, 'Model call')), { happenedAt: row.completed_at || row.updated_at, nextRunAt: null }, { feederHandle: row.feeder_handle, postKey: row.post_key })),
  ].filter((item) => item.feedId != null || item.feederId != null || item.postKey || item.source === 'web_push_jobs');

  function healthFor(items: typeof events) {
    if (items.some((item) => eventHealth(item.status) === 'failed')) return 'failed';
    if (items.some((item) => eventHealth(item.status) === 'pending')) return 'pending';
    const latest = items.map((item) => asDateMs(item.happenedAt)).sort((a, b) => b - a)[0] || 0;
    if (latest && Date.now() - latest > STALE_HOURS * 60 * 60 * 1000) return 'stale';
    return items.length ? 'smooth' : 'missing';
  }

  function latestDate(items: typeof events, key: 'happenedAt' | 'nextRunAt') {
    const dates = items.map((item) => asDateMs(item[key])).filter(Boolean).sort((a, b) => key === 'nextRunAt' ? a - b : b - a);
    return dates[0] ? new Date(dates[0]).toISOString() : null;
  }

  const failures = events
    .filter((item) => eventHealth(item.status) === 'failed')
    .sort((a, b) => asDateMs(b.happenedAt) - asDateMs(a.happenedAt));
  const pendingAhead = events
    .filter((item) => eventHealth(item.status) === 'pending' || item.nextRunAt)
    .sort((a, b) => asDateMs(a.nextRunAt) - asDateMs(b.nextRunAt))
    .slice(0, 80);
  const recentActivity = events
    .filter((item) => item.happenedAt)
    .sort((a, b) => asDateMs(b.happenedAt) - asDateMs(a.happenedAt))
    .slice(0, 80);

  const cards = feeds.map((feed) => {
    const feedId = asNumber(feed.id);
    const feedFeeders = feeders.filter((feeder) => asNumber(feeder.feed_id) === feedId);
    const feedEvents = events.filter((item) => item.feedId === feedId);
    const feedFailures = failures.filter((item) => item.feedId === feedId);
    return {
      feedId,
      feedName: asString(feed.name, 'Untitled feed'),
      status: asString(feed.status, 'unknown'),
      health: healthFor(feedEvents),
      feederCount: feedFeeders.length,
      activeFeederCount: countWhere(feedFeeders, (row) => asString(row.status) === 'active'),
      pausedFeederCount: countWhere(feedFeeders, (row) => asString(row.status) === 'paused'),
      lastActivityAt: latestDate(feedEvents, 'happenedAt'),
      nextWorkAt: latestDate(feedEvents, 'nextRunAt'),
      latestFailure: feedFailures[0] || null,
      feeders: feedFeeders.map((feeder) => {
        const feederId = asNumber(feeder.id);
        const feederEvents = events.filter((item) => item.feederId === feederId);
        return {
          feederId,
          feedId,
          handle: asString(feeder.handle, 'unknown'),
          role: asString(feeder.role, 'standard'),
          status: asString(feeder.status, 'unknown'),
          health: healthFor(feederEvents),
          followerCount: asNumber(feeder.follower_count),
          lastActivityAt: latestDate(feederEvents, 'happenedAt'),
          nextWorkAt: latestDate(feederEvents, 'nextRunAt'),
          pendingCount: feederEvents.filter((item) => eventHealth(item.status) === 'pending').length,
          failureCount: feederEvents.filter((item) => eventHealth(item.status) === 'failed').length,
        };
      }),
    };
  });

  return {
    feeds: cards,
    recentActivity,
    pendingAhead,
    failures: failures.slice(0, 80),
  };
}

export async function GET(request: NextRequest) {
  const auth = await requireUser();
  if (auth.response || !auth.user) return auth.response;

  const allowlist = adminEmails();
  const isAllowlistedAdmin = Boolean(
    auth.user.email && allowlist.includes(auth.user.email.toLowerCase()),
  );
  const accessMode: AccessMode = allowlist.length > 0 && isAllowlistedAdmin
    ? 'platform_admin'
    : 'signed_in_account';

  const sb = getSupabase(true);
  const generatedAt = new Date().toISOString();

  const users = await readTable('users', () => {
    const query = sb
      .from('users')
      .select('id,email,name,balance,total_runs,data_points,success_rate,pwa_push_enabled,created_at,updated_at', { count: 'exact' })
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
  ] = await Promise.all([
    feederIds.length === 0
      ? Promise.resolve(emptyRead())
      : readTable('run_jobs', () => sb
        .from('run_jobs')
        .select('id,feeder_id,job_type,business_date_ist,status,attempt,next_run_at,last_error,created_at,updated_at', { count: 'exact' })
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
        .select('post_key,checkpoint,business_date_ist,computed_at,views,likes,comments,percentile_performance,feed_percentile,delta_from_d1', { count: 'exact' })
        .in('post_key', postKeys)
        .order('computed_at', { ascending: false })
        .limit(680)),
    feedIds.length === 0
      ? Promise.resolve(emptyRead())
      : readTable('fire_alerts', () => sb
        .from('fire_alerts')
        .select('id,feed_id,feeder_id,post_key,checkpoint,business_date_ist,signal_code,alert_type,status,metric_key,metric_value,surface_percentile,surface_delta,pattern_signal,body,created_at,updated_at', { count: 'exact' })
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
  ]);

  const feederFileIds = feederFiles.rows
    .map((row) => asNumber(row.id))
    .filter((id): id is number => id != null);
  const feederHandles = Array.from(
    new Set(feeders.rows.map((row) => asString(row.handle)).filter((handle): handle is string => Boolean(handle))),
  );

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
      : feederHandles.length === 0
        ? Promise.resolve(emptyRead())
        : readTable('feeder_file_model_calls', () => sb
          .from('feeder_file_model_calls')
          .select('id,feeder_file_id,call_key,call_type,feeder_handle,pattern_id,post_key,model,prompt_version,status,error,started_at,completed_at,updated_at', { count: 'exact' })
          .in('feeder_handle', feederHandles)
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
  };

  const activeFeeders = countWhere(feeders.rows, (row) => asString(row.status) === 'active');
  const pausedFeeders = countWhere(feeders.rows, (row) => asString(row.status) === 'paused');
  const activeFeeds = countWhere(feeds.rows, (row) => asString(row.status) === 'active');
  const contextCovered = countWhere(feeds.rows, (row) => {
    const brief = row.context_brief;
    const bible = asString(row.context_bible);
    return Boolean(bible) || Boolean(brief && typeof brief === 'object' && Object.keys(brief).length > 0);
  });
  const completedRunJobs = countWhere(runJobs.rows, (row) => asString(row.status) === 'done');
  const failedRunJobs = countWhere(runJobs.rows, (row) => asString(row.status) === 'failed');
  const openRunJobs = countWhere(runJobs.rows, (row) => ['pending', 'retry', 'running'].includes(asString(row.status) || ''));
  const completedCheckpointJobs = countWhere(checkpointJobs.rows, (row) => asString(row.status) === 'done');
  const failedCheckpointJobs = countWhere(checkpointJobs.rows, (row) => asString(row.status) === 'failed');
  const openCheckpointJobs = countWhere(checkpointJobs.rows, (row) => ['pending', 'retry', 'running'].includes(asString(row.status) || ''));
  const modelCallFailures = countWhere(modelCalls.rows, (row) => ['failed', 'error'].includes(asString(row.status) || ''));
  const mediaFailures = countWhere(mediaAssets.rows, (row) => ['capture_failed', 'purge_failed'].includes(asString(row.status) || ''));
  const activePushSubscriptions = countWhere(pushSubscriptions.rows, (row) => row.enabled === true);
  const pushFailures = countWhere(pushJobs.rows, (row) => asString(row.status) === 'failed');
  const paidTransactionsPaise = transactions.rows
    .filter((row) => asString(row.status) === 'paid')
    .reduce((total, row) => total + (asNumber(row.amount) || 0), 0);
  const pendingTransactionsPaise = transactions.rows
    .filter((row) => asString(row.status) === 'pending')
    .reduce((total, row) => total + (asNumber(row.amount) || 0), 0);
  const feedOps = buildFeedOps({
    feeds: feeds.rows,
    feeders: feeders.rows,
    posts: posts.rows,
    runJobs: runJobs.rows,
    checkpointJobs: checkpointJobs.rows,
    postMetrics: postMetrics.rows,
    fireAlerts: fireAlerts.rows,
    signals: signals.rows,
    mediaAssets: mediaAssets.rows,
    pushJobs: pushJobs.rows,
    modelCalls: modelCalls.rows,
  });

  const payload = {
    generatedAt,
    access: {
      mode: accessMode,
      signedInEmail: auth.user.email || null,
      adminAllowlistConfigured: allowlist.length > 0,
      note: accessMode === 'platform_admin'
        ? 'Showing platform-wide read-only data because the signed-in email matches COMMAND_HUB_ADMIN_EMAILS.'
        : 'Showing the signed-in account scope. Configure COMMAND_HUB_ADMIN_EMAILS for platform-wide admin reads.',
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
      totals: {
        jobs: runJobs.count,
        completed: completedRunJobs,
        open: openRunJobs,
        failed: failedRunJobs,
        successPercent: percent(completedRunJobs, completedRunJobs + failedRunJobs),
      },
      byStatus: countBy(runJobs.rows, 'status'),
      byType: countBy(runJobs.rows, 'job_type'),
      recentJobs: pickRows(runJobs.rows, 40),
      recentErrors: recentErrors(runJobs.rows),
      latestChangeAt: latestIso(runJobs.rows, ['updated_at', 'created_at']),
    },
    checkpoints: {
      totals: {
        jobs: checkpointJobs.count,
        completed: completedCheckpointJobs,
        open: openCheckpointJobs,
        failed: failedCheckpointJobs,
        metrics: postMetrics.count,
        successPercent: percent(completedCheckpointJobs, completedCheckpointJobs + failedCheckpointJobs),
      },
      jobsByStatus: countBy(checkpointJobs.rows, 'status'),
      jobsByCheckpoint: countBy(checkpointJobs.rows, 'checkpoint'),
      metricsByCheckpoint: countBy(postMetrics.rows, 'checkpoint'),
      recentJobs: decoratePostRows(pickRows(checkpointJobs.rows, 44), posts.rows, feeders.rows, mediaAssets.rows),
      recentMetrics: decoratePostRows(pickRows(postMetrics.rows, 32), posts.rows, feeders.rows, mediaAssets.rows),
      recentErrors: recentErrors(checkpointJobs.rows),
      latestChangeAt: latestIso([...checkpointJobs.rows, ...postMetrics.rows], ['updated_at', 'computed_at', 'created_at']),
    },
    fireSignals: {
      totals: {
        alerts: fireAlerts.count,
        signals: signals.count,
        hotPosts: uniqCount(fireAlerts.rows, 'post_key'),
        staleOrSuppressedSignals: countWhere(signals.rows, (row) => ['stale', 'suppressed_cap', 'suppressed_confidence'].includes(asString(row.status) || '')),
        erroredSignals: countWhere(signals.rows, (row) => asString(row.status) === 'error'),
      },
      alertsByStatus: countBy(fireAlerts.rows, 'status'),
      alertsByType: countBy(fireAlerts.rows, 'alert_type'),
      signalsByFamily: countBy(signals.rows, 'signal_family'),
      signalsByStatus: countBy(signals.rows, 'status'),
      recentAlerts: pickRows(fireAlerts.rows, 36),
      recentSignals: pickRows(signals.rows, 36),
      latestChangeAt: latestIso([...fireAlerts.rows, ...signals.rows], ['updated_at', 'last_fired_at', 'created_at']),
    },
    intelligence: {
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
      totals: {
        assets: mediaAssets.count,
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
      totals: {
        subscriptions: pushSubscriptions.count,
        activeSubscriptions: activePushSubscriptions,
        jobs: pushJobs.count,
        sentJobs: countWhere(pushJobs.rows, (row) => asString(row.status) === 'sent'),
        openJobs: countWhere(pushJobs.rows, (row) => ['pending', 'retry', 'running'].includes(asString(row.status) || '')),
        failedJobs: pushFailures,
      },
      subscriptionsByEnabled: {
        enabled: activePushSubscriptions,
        disabled: pushSubscriptions.count - activePushSubscriptions,
      },
      jobsByStatus: countBy(pushJobs.rows, 'status'),
      jobsByKind: countBy(pushJobs.rows, 'kind'),
      recentSubscriptions: pickRows(pushSubscriptions.rows, 24),
      recentJobs: pickRows(pushJobs.rows, 36),
      recentErrors: recentErrors([...pushJobs.rows, ...pushSubscriptions.rows]),
      latestChangeAt: latestIso([...pushJobs.rows, ...pushSubscriptions.rows], ['updated_at', 'last_seen_at', 'sent_at', 'created_at']),
    },
    finance: {
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
        status: 'missing_instrumentation' as const,
        detail: 'Worker and server health history is not persisted in the app schema.',
        path: 'Add heartbeat rows from worker/systemd plus memory and CPU samples.',
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
      ...tableErrorGaps(allReads),
    ],
  };

  return privateJsonResponse(request, payload, {
    maxAgeSeconds: COMMAND_ROUTE_TTL_SECONDS,
    staleWhileRevalidateSeconds: 120,
  });
}
