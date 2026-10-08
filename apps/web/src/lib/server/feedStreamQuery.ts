/* ─────────────────────────────────────────────────────────────
   FEED STREAM QUERIES — the server half of the Feed tab's stream:
   /api/feed/stream/index (the per-day index) and /api/feed/stream
   (the posts of a day range). Server only: it reads with the
   service role, so every read is scoped here to the signed-in
   user's active feeds and active feeders.

   The hot path. One round trip resolves the scope, then posts come
   with their checkpoint results (and, on pages, their stored
   preview clips) embedded, read in parallel slices and paginated
   past PostgREST's 1000-row cap.

   A post's result is its latest checkpoint that has a ranking (or
   its latest landed checkpoint when none is ranked yet): that row
   gives Top %, the checkpoint, likes, comments, engagement rate and
   the multiple, and its business_date_ist is the post's result day
   in 'results' order.

   The index also carries each day's Top % in display order (so the
   client packs all 90 days before loading a post) and the scope's
   profile, both from the same scan; the profile reads posts that
   went up in the window, so it is the same in either order.
   ───────────────────────────────────────────────────────────── */

import { NextResponse } from 'next/server';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient as createServerSupabaseClient } from '@/lib/supabase/server';
import { instagramLinkLive } from '@/lib/server/instagramLink';
import {
  CAPTION_MAX,
  STREAM_WINDOW_DAYS,
  WINNER_MAX,
  groupKeyOf,
  istDay,
  mediaProxyUrl,
  orderDayPosts,
  scopeKey,
  shiftDay,
  type FeedOrder,
  type StreamCheckpoint,
  type StreamDay,
  type StreamHighlight,
  type StreamIndex,
  type StreamMediaType,
  type StreamPage,
  type StreamPost,
  type StreamProfile,
  type StreamSlide,
} from '@/lib/feedStream/contract';

/* ── constants ─────────────────────────────────────────────── */

const DB_PAGE_SIZE = 1000; // PostgREST's default max rows per request
const KEY_CHUNK_SIZE = 200; // post keys per .in() (keeps the request URL short)
const KEY_CHUNK_CONCURRENCY = 4;
const INDEX_SLICES = 6; // the index reads its days as parallel slices
const MAX_PAGE_DAYS = 31;
const COVERS_PER_DAY = 4;
// 'results' order looks back this far before the window: a D21 lands 21 days after the post, plus a little slack
// for a late landing
const RESULT_REACH_DAYS = 24;
const FRESH_MS = 24 * 60 * 60 * 1000;
const IST_OFFSET_MS = 330 * 60 * 1000; // IST is UTC+5:30, no DST
const DAY_MS = 24 * 60 * 60 * 1000;
const CHECKPOINT_RANK: Record<string, number> = { d1: 1, d3: 2, d7: 3, d21: 4 };
const DB_CHECKPOINTS = ['d1', 'd3', 'd7', 'd21'];

// stored preview clips /api/media will redirect to. Anything it would skip makes it fall back to proxying the
// full Instagram video, so a preview is only offered when it clearly passes the same test, with a margin before
// its purge deadline (a page can sit in the client's memory for a while)
const PREVIEW_STATUSES = ['active', 'purge_pending'];
const PREVIEW_PURGE_MARGIN_MS = 6 * 60 * 60 * 1000;

const NO_STORE_HEADERS = { 'Cache-Control': 'private, no-store' };

// the index reads no thumbnail URLs (the proxy needs only the key); 'results' also needs when each result landed
const INDEX_POST_FIELDS = 'post_key,feeder_id,posted_at,media_type';
const INDEX_METRICS_POSTED = 'checkpoint,percentile_performance,business_date_ist,likes';
const INDEX_METRICS_RESULTS = 'checkpoint,percentile_performance,business_date_ist,computed_at,likes';
const PAGE_POST_FIELDS = 'post_key,feeder_id,media_type,posted_at,post_url,caption';
const PAGE_METRICS = 'checkpoint,percentile_performance,business_date_ist,computed_at,likes,comments,engagement_rate,ranking_multiple';
const PREVIEW_FIELDS = 'asset_role,status,storage_provider,storage_bucket,storage_path,mime_type,purge_after';

// the profile
const PROFILE_TYPICAL_DAYS = 30; // typical: the last 30 days; typicalPrev: the 30 before
const PROFILE_WEEKS = 13;
const PROFILE_MIN_POSTS = 4; // a posting window or a format needs this many measured posts to be named
const PROFILE_LEADER_MIN_POSTS = 5;
const PROFILE_RUN_DAYS = 7;
const PROFILE_HOUR_SPAN = 3;
const FORMATS: StreamMediaType[] = ['reel', 'carousel', 'image'];
// followers: the latest snapshot from the last week, against the one nearest 30 days before it (±3 days)
const FOLLOWER_RECENT_DAYS = 7;
const FOLLOWER_SPAN_DAYS = 30;
const FOLLOWER_SLACK_DAYS = 3;

/* ── types ─────────────────────────────────────────────────── */

type MetricRow = {
  post_key?: string | null;
  checkpoint: string | null;
  percentile_performance: number | string | null;
  business_date_ist?: string | null;
  computed_at?: string | null;
  likes?: number | string | null;
  comments?: number | string | null;
  engagement_rate?: number | string | null;
  ranking_multiple?: number | string | null;
};

type PreviewAssetRow = {
  post_key?: string | null;
  asset_role: string | null;
  status: string | null;
  storage_provider: string | null;
  storage_bucket: string | null;
  storage_path: string | null;
  mime_type: string | null;
  purge_after: string | null;
};

type PostRow = {
  post_key: string | null;
  feeder_id: number | string | null;
  posted_at: string | null;
  media_type?: string | null;
  post_url?: string | null;
  caption?: string | null;
  post_metrics?: MetricRow[] | null;
  post_media_assets?: PreviewAssetRow[] | null;
};

type FeederScopeRow = {
  id: number | string;
  feed_id: number | string;
  handle: string | null;
  follower_count?: number | string | null;
};

type SnapshotRow = {
  feeder_id: number | string | null;
  snapshot_date_ist: string | null;
  follower_count: number | string | null;
};

export type ScopeFeeder = { id: number; feedId: string; handle: string; followerCount: number | null };

export type StreamScopeContext = {
  key: string;
  order: FeedOrder;
  feeders: ScopeFeeder[];
  feederById: Map<number, ScopeFeeder>;
  // all feeds: an account can sit in two feeds (an anchor in each); its posts show once
  spansFeeds: boolean;
  // a single feeder picked (the profile is theirs: no leader, no run)
  oneFeeder: boolean;
};

export type StreamWindow = { today: string; start: string };

type Landed = { checkpoint: StreamCheckpoint; rank: number; topPercent: number | null; row: MetricRow };

type Candidate = {
  key: string;
  feeder: ScopeFeeder;
  row: PostRow;
  postedAtMs: number;
  postedAt: string;
  day: string;
  landed: Landed[];
  head: Landed | null;
  resultDay: string | null;
  resultAtMs: number;
  resultAt: string | null;
};

type QueryResult = { data: unknown; error: unknown };
type EmbedFilter = { column: string; op: 'eq' | 'in' | 'gte' | 'lte'; value: string | string[] };

/* ── responses, auth, client ───────────────────────────────── */

export function streamJson(body: unknown, status = 200) {
  return NextResponse.json(body, { status, headers: NO_STORE_HEADERS });
}

export function streamErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message) return error.message;
  if (error && typeof error === 'object' && typeof (error as { message?: unknown }).message === 'string') {
    return (error as { message: string }).message;
  }
  return fallback;
}

// the signed-in user's id, or null (the routes answer 401)
export async function streamUserId(): Promise<string | null> {
  const supabase = await createServerSupabaseClient();
  const {
    data: { user },
    error,
  } = await supabase.auth.getUser();
  return error || !user ? null : user.id;
}

function createAdminClient() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const serviceRole = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !serviceRole) {
    throw new Error('Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  }
  return createSupabaseClient(url, serviceRole, {
    auth: { autoRefreshToken: false, persistSession: false },
  });
}

type AdminClient = ReturnType<typeof createAdminClient>;
let adminClient: AdminClient | null = null;

export function streamAdminClient(): AdminClient {
  if (!adminClient) adminClient = createAdminClient();
  return adminClient;
}

/* ── small parsers ─────────────────────────────────────────── */

function nullableString(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value.trim() : null;
}

function nullableNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function parseIsoMs(value: unknown): number {
  if (typeof value !== 'string' || !value) return 0;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizeHandle(value: unknown): string {
  return String(value ?? '').trim().replace(/^@+/, '').toLowerCase();
}

function normalizeMediaType(value: unknown): StreamMediaType {
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
  if (normalized === 'sidecar' || normalized === 'sidcar' || normalized === 'carousel') return 'carousel';
  if (normalized === 'reel' || normalized === 'video') return 'reel';
  if (normalized === 'image' || normalized === 'photo') return 'image';
  return 'unknown';
}

// Top %: rounded, clamped to 1..100
function topPercentOf(value: unknown): number | null {
  const parsed = nullableNumber(value);
  return parsed == null ? null : Math.min(100, Math.max(1, Math.round(parsed)));
}

// post_metrics.engagement_rate is a fraction (engagements ÷ followers); the stream speaks percent
function percentOf(fraction: number | null): number | null {
  return fraction == null ? null : Math.round(fraction * 1_000_000) / 10_000;
}

function dateOnly(value: unknown): string | null {
  return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}/.test(value) ? value.slice(0, 10) : null;
}

// a YYYY-MM-DD that is a real calendar day, or null
export function parseStreamDay(value: string | null): string | null {
  const raw = (value || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return null;
  return shiftDay(raw, 0) === raw ? raw : null;
}

export function parseStreamOrder(params: URLSearchParams): FeedOrder | null {
  const raw = (params.get('order') || '').trim().toLowerCase();
  if (!raw || raw === 'posted') return 'posted';
  return raw === 'results' ? 'results' : null;
}

function dayNumber(day: string): number {
  const [year, month, date] = day.split('-').map(Number);
  return Date.UTC(year, month - 1, date) / DAY_MS;
}

function daySpan(from: string, to: string): number {
  return dayNumber(to) - dayNumber(from) + 1;
}

// the UTC moment an IST day starts
function istDayStartIso(day: string): string {
  const [year, month, date] = day.split('-').map(Number);
  return new Date(Date.UTC(year, month - 1, date) - IST_OFFSET_MS).toISOString();
}

function chunk<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let start = 0; start < items.length; start += size) chunks.push(items.slice(start, start + size));
  return chunks;
}

async function eachLimited<T>(items: T[], limit: number, run: (item: T) => Promise<void>): Promise<void> {
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const item = items[next];
      next += 1;
      await run(item);
    }
  });
  await Promise.all(workers);
}

/* ── the window ────────────────────────────────────────────── */

export function streamWindow(nowMs: number = Date.now()): StreamWindow {
  const today = istDay(nowMs);
  return { today, start: shiftDay(today, -(STREAM_WINDOW_DAYS - 1)) };
}

// [from..to] split into `parts` contiguous day ranges (fewer when the range is short)
function splitDays(from: string, to: string, parts: number): Array<{ from: string; to: string }> {
  const total = daySpan(from, to);
  if (total <= 0) return [];
  const size = Math.ceil(total / Math.max(1, parts));
  const slices: Array<{ from: string; to: string }> = [];
  for (let offset = 0; offset < total; offset += size) {
    slices.push({ from: shiftDay(from, offset), to: shiftDay(from, Math.min(total, offset + size) - 1) });
  }
  return slices;
}

/* ── the range of a page ───────────────────────────────────── */

export type StreamRange =
  | { ok: true; from: string; to: string; empty: boolean }
  | { ok: false; error: string };

// from/to inclusive, clamped to the window and to MAX_PAGE_DAYS (keeping the newest end)
export function parseStreamRange(params: URLSearchParams, window: StreamWindow): StreamRange {
  const from = parseStreamDay(params.get('from'));
  const to = parseStreamDay(params.get('to'));
  if (!from || !to || from > to) return { ok: false, error: 'from and to must be days (YYYY-MM-DD), from on or before to' };
  let clampedFrom = from < window.start ? window.start : from;
  const clampedTo = to > window.today ? window.today : to;
  if (clampedFrom > clampedTo) return { ok: true, from, to, empty: true };
  if (daySpan(clampedFrom, clampedTo) > MAX_PAGE_DAYS) clampedFrom = shiftDay(clampedTo, -(MAX_PAGE_DAYS - 1));
  return { ok: true, from: clampedFrom, to: clampedTo, empty: false };
}

/* ── the scope ─────────────────────────────────────────────── */

export type StreamScopeResult =
  | { ok: true; scope: StreamScopeContext }
  | { ok: false; status: number; error: string };

async function fetchActiveFeedIds(sb: AdminClient, userId: string, feedId: number | null): Promise<number[]> {
  let query = sb.from('feeds').select('id').eq('user_id', userId).eq('status', 'active');
  if (feedId) query = query.eq('id', feedId);
  const { data, error } = await query;
  if (error) throw error;
  return ((data || []) as Array<{ id: number | string }>).map((row) => Number(row.id)).filter((id) => Number.isFinite(id) && id > 0);
}

// active feeders of the user's active feeds (of one feed when feedId is set): one round trip through the embed,
// two plain reads if the embed is ever unavailable
async function fetchScopeFeederRows(sb: AdminClient, userId: string, feedId: number | null): Promise<FeederScopeRow[]> {
  let query = sb
    .from('feeders')
    .select('id,feed_id,handle,follower_count,feeds!inner(user_id,status)')
    .eq('status', 'active')
    .eq('feeds.user_id', userId)
    .eq('feeds.status', 'active');
  if (feedId) query = query.eq('feed_id', feedId);
  const embedded = await query;
  if (!embedded.error) return (embedded.data || []) as unknown as FeederScopeRow[];

  console.warn('[feed stream] scope embed failed, reading feeds then feeders:', streamErrorMessage(embedded.error, 'unknown'));
  const feedIds = await fetchActiveFeedIds(sb, userId, feedId);
  if (feedIds.length === 0) return [];
  const { data, error } = await sb
    .from('feeders')
    .select('id,feed_id,handle,follower_count')
    .in('feed_id', feedIds)
    .eq('status', 'active');
  if (error) throw error;
  return (data || []) as FeederScopeRow[];
}

export async function resolveStreamScope(
  sb: AdminClient,
  userId: string,
  params: URLSearchParams,
  order: FeedOrder,
): Promise<StreamScopeResult> {
  const rawFeedId = (params.get('feedId') || '').trim();
  let feedId: number | null = null;
  if (rawFeedId) {
    feedId = /^\d{1,15}$/.test(rawFeedId) ? Number(rawFeedId) : 0;
    if (!feedId) return { ok: false, status: 404, error: 'Feed not found' };
  }
  const rawHandle = normalizeHandle(params.get('handle'));
  const handle = rawHandle && rawHandle !== 'all' ? rawHandle : null;

  const rows = await fetchScopeFeederRows(sb, userId, feedId);
  if (feedId && rows.length === 0) {
    // a feed of yours with no active feeder yet is an empty feed, not a missing one
    const owned = await fetchActiveFeedIds(sb, userId, feedId);
    if (owned.length === 0) return { ok: false, status: 404, error: 'Feed not found' };
  }

  const feeders: ScopeFeeder[] = [];
  for (const row of rows) {
    const id = Number(row.id);
    const feederHandle = normalizeHandle(row.handle);
    if (!Number.isFinite(id) || !feederHandle) continue;
    if (handle && feederHandle !== handle) continue;
    feeders.push({ id, feedId: String(row.feed_id), handle: feederHandle, followerCount: nullableNumber(row.follower_count) });
  }
  if (handle && feeders.length === 0) return { ok: false, status: 404, error: 'Feeder not found' };

  return {
    ok: true,
    scope: {
      key: scopeKey({ feedId: feedId ? rawFeedId : null, handle, order }),
      order,
      feeders,
      feederById: new Map(feeders.map((feeder) => [feeder.id, feeder])),
      spansFeeds: !feedId,
      oneFeeder: Boolean(handle),
    },
  };
}

/* ── reads ─────────────────────────────────────────────────── */

async function fetchAllPages<Row>(page: (start: number, end: number) => PromiseLike<QueryResult>): Promise<Row[]> {
  const rows: Row[] = [];
  for (let start = 0; ; start += DB_PAGE_SIZE) {
    const { data, error } = await page(start, start + DB_PAGE_SIZE - 1);
    if (error) throw new Error(streamErrorMessage(error, 'Query failed'));
    const batch = Array.isArray(data) ? (data as Row[]) : [];
    for (const row of batch) rows.push(row);
    if (batch.length < DB_PAGE_SIZE) return rows;
  }
}

// posts of these feeders that went up on IST days fromDay..toDay, newest first, every page of them
function fetchPostRows(
  sb: AdminClient,
  feederIds: number[],
  fromDay: string,
  toDay: string,
  select: string,
  filters: EmbedFilter[] = [],
): Promise<PostRow[]> {
  if (feederIds.length === 0 || fromDay > toDay) return Promise.resolve([]);
  const fromIso = istDayStartIso(fromDay);
  const toIso = istDayStartIso(shiftDay(toDay, 1));
  return fetchAllPages<PostRow>((start, end) => {
    let query = sb
      .from('posts')
      .select(select)
      .in('feeder_id', feederIds)
      .gte('posted_at', fromIso)
      .lt('posted_at', toIso);
    for (const filter of filters) {
      if (filter.op === 'eq') query = query.eq(filter.column, filter.value as string);
      else if (filter.op === 'in') query = query.in(filter.column, filter.value as string[]);
      else if (filter.op === 'gte') query = query.gte(filter.column, filter.value as string);
      else query = query.lte(filter.column, filter.value as string);
    }
    // a unique tiebreak keeps the pages from skipping or repeating rows
    return query.order('posted_at', { ascending: false }).order('post_key', { ascending: true }).range(start, end);
  });
}

// rows of a per-post table for these keys, in key chunks (a few at a time), every page of each
async function fetchRowsByPostKeys<Row extends { post_key?: string | null }>(
  keys: string[],
  page: (chunkKeys: string[], start: number, end: number) => PromiseLike<QueryResult>,
): Promise<Map<string, Row[]>> {
  const byKey = new Map<string, Row[]>();
  await eachLimited(chunk(Array.from(new Set(keys)), KEY_CHUNK_SIZE), KEY_CHUNK_CONCURRENCY, async (chunkKeys) => {
    const rows = await fetchAllPages<Row>((start, end) => page(chunkKeys, start, end));
    for (const row of rows) {
      const key = nullableString(row.post_key);
      if (!key) continue;
      const bucket = byKey.get(key);
      if (bucket) bucket.push(row);
      else byKey.set(key, [row]);
    }
  });
  return byKey;
}

// the fallback when the embedded read fails: the same rows, joined here
async function attachSeparately(
  sb: AdminClient,
  rows: PostRow[],
  metricFields: string,
  previews: boolean,
): Promise<void> {
  const keys = rows.map((row) => nullableString(row.post_key)).filter((key): key is string => Boolean(key));
  const reelKeys = previews
    ? rows.filter((row) => normalizeMediaType(row.media_type) === 'reel').map((row) => nullableString(row.post_key)).filter((key): key is string => Boolean(key))
    : [];
  const [metricsByKey, assetsByKey] = await Promise.all([
    fetchRowsByPostKeys<MetricRow>(keys, (chunkKeys, start, end) => sb
      .from('post_metrics')
      .select(`post_key,${metricFields}`)
      .in('post_key', chunkKeys)
      .in('checkpoint', DB_CHECKPOINTS)
      .order('post_key', { ascending: true })
      .order('checkpoint', { ascending: true })
      .range(start, end)),
    reelKeys.length
      ? fetchRowsByPostKeys<PreviewAssetRow>(reelKeys, (chunkKeys, start, end) => sb
        .from('post_media_assets')
        .select(`post_key,${PREVIEW_FIELDS}`)
        .in('post_key', chunkKeys)
        .eq('asset_role', 'preview_5s')
        .in('status', PREVIEW_STATUSES)
        .order('post_key', { ascending: true })
        .order('asset_role', { ascending: true })
        .range(start, end))
      : Promise.resolve(new Map<string, PreviewAssetRow[]>()),
  ]);
  for (const row of rows) {
    const key = nullableString(row.post_key);
    row.post_metrics = (key && metricsByKey.get(key)) || [];
    if (previews) row.post_media_assets = (key && assetsByKey.get(key)) || [];
  }
}

/* ── results and candidates ────────────────────────────────── */

function landedResults(rows: MetricRow[] | null | undefined): Landed[] {
  const byRank: Array<Landed | undefined> = [];
  for (const row of rows || []) {
    const checkpoint = typeof row.checkpoint === 'string' ? row.checkpoint.trim().toLowerCase() : '';
    const rank = CHECKPOINT_RANK[checkpoint];
    if (!rank) continue;
    const existing = byRank[rank];
    if (existing && parseIsoMs(existing.row.computed_at) >= parseIsoMs(row.computed_at)) continue;
    byRank[rank] = {
      checkpoint: checkpoint.toUpperCase() as StreamCheckpoint,
      rank,
      topPercent: topPercentOf(row.percentile_performance),
      row,
    };
  }
  return byRank.filter((landed): landed is Landed => Boolean(landed));
}

// the post's result: its latest ranked checkpoint, or its latest landed one while none is ranked
function headOf(landed: Landed[]): Landed | null {
  for (let index = landed.length - 1; index >= 0; index -= 1) {
    if (landed[index].topPercent != null) return landed[index];
  }
  return landed.length ? landed[landed.length - 1] : null;
}

function toCandidate(row: PostRow, feederById: Map<number, ScopeFeeder>): Candidate | null {
  const key = nullableString(row.post_key);
  const feeder = row.feeder_id == null ? undefined : feederById.get(Number(row.feeder_id));
  const postedAtMs = parseIsoMs(row.posted_at);
  if (!key || !feeder || !postedAtMs) return null;
  const landed = landedResults(row.post_metrics);
  const head = headOf(landed);
  const resultAtMs = head ? parseIsoMs(head.row.computed_at) : 0;
  return {
    key,
    feeder,
    row,
    postedAtMs,
    postedAt: new Date(postedAtMs).toISOString(),
    day: istDay(postedAtMs),
    landed,
    head,
    resultDay: head ? dateOnly(head.row.business_date_ist) : null,
    resultAtMs,
    resultAt: resultAtMs ? new Date(resultAtMs).toISOString() : null,
  };
}

function toCandidates(rows: PostRow[], scope: StreamScopeContext): Candidate[] {
  const candidates: Candidate[] = [];
  for (const row of rows) {
    const candidate = toCandidate(row, scope.feederById);
    if (candidate) candidates.push(candidate);
  }
  return scope.spansFeeds ? dedupeCopies(candidates) : candidates;
}

/* An account tracked in two feeds has each of its posts twice (post_key is "<shortcode>#f<feeder id>"). Across
   feeds it shows once: the copy with the further result wins, then the older feeder. Index and pages pick alike.
   A collab (one shortcode on two different tracked accounts) stays twice: each account made it, each ranks it. */
function copyKey(candidate: Candidate): string | null {
  const hash = candidate.key.indexOf('#');
  const base = hash >= 0 ? candidate.key.slice(0, hash) : candidate.key;
  return base && base !== 'post' ? `${candidate.feeder.handle} ${base}` : null;
}

function copyScore(candidate: Candidate): number {
  return candidate.head ? candidate.head.rank * 2 + (candidate.head.topPercent != null ? 1 : 0) : 0;
}

function prefersCopy(next: Candidate, current: Candidate): boolean {
  const nextScore = copyScore(next);
  const currentScore = copyScore(current);
  if (nextScore !== currentScore) return nextScore > currentScore;
  if (next.feeder.id !== current.feeder.id) return next.feeder.id < current.feeder.id;
  return next.key < current.key;
}

function dedupeCopies(candidates: Candidate[]): Candidate[] {
  const chosen = new Map<string, Candidate>();
  const kept: Candidate[] = [];
  for (const candidate of candidates) {
    const base = copyKey(candidate);
    if (!base) {
      kept.push(candidate);
      continue;
    }
    const current = chosen.get(base);
    if (!current || prefersCopy(candidate, current)) chosen.set(base, candidate);
  }
  chosen.forEach((candidate) => kept.push(candidate));
  return kept;
}

function sectionDay(candidate: Candidate, order: FeedOrder): string | null {
  return order === 'results' ? candidate.resultDay : candidate.day;
}

// the moment that orders a day: the post going up ('posted') or its result landing ('results')
function momentOf(candidate: Candidate, order: FeedOrder): number {
  return order === 'results' && candidate.resultAtMs ? candidate.resultAtMs : candidate.postedAtMs;
}

// a page's order: newest moment first, then by key. The client groups a page by day in this order and hands each
// day to orderDayPosts, so the index's tops follow exactly this
function pageOrder(order: FeedOrder) {
  return (left: Candidate, right: Candidate) => momentOf(right, order) - momentOf(left, order)
    || (left.key < right.key ? -1 : left.key > right.key ? 1 : 0);
}

function topOf(candidate: Candidate): number | null {
  return candidate.head ? candidate.head.topPercent : null;
}

// a day's Top % in display order: light posts through the contract's own orderDayPosts, as the client will
function displayTops(posts: Candidate[], order: FeedOrder): Array<number | null> {
  const light: Array<Pick<StreamPost, 'key' | 'postedAt' | 'resultAt' | 'topPercent'>> = posts
    .slice()
    .sort(pageOrder(order))
    .map((candidate) => ({
      key: candidate.key,
      postedAt: candidate.postedAt,
      resultAt: candidate.resultAt,
      topPercent: topOf(candidate),
    }));
  return orderDayPosts(light as StreamPost[], order).map((post) => post.topPercent);
}

// postKey alone: /api/media resolves the stored copy (or the post's own source) from it and never reads `url`, so
// leaving the long Instagram link out keeps the index small (it was most of its bytes). A post known to have
// nothing to draw (drawable says so) gets no URL: the client draws its cover at once instead of asking
function thumbnailOf(candidate: Candidate, drawable?: ReadonlySet<string> | null): string | null {
  if (drawable && !drawable.has(candidate.key)) return null;
  return mediaProxyUrl(candidate.key, 'thumbnail');
}

/* ── posters ───────────────────────────────────────────────────
   Which posts /api/media can actually draw a picture for: a stored copy it can sign (R2), or the post's own
   Instagram link while it lasts (those expire within days). Read in two small queries per 200 keys; a failure
   answers null, and every post keeps its proxy URL as before. */

const POSTER_ROLES = ['thumbnail', 'display', 'carousel_0', 'carousel_00', 'carousel_01', 'carousel_1'];
const POSTER_STATUSES = ['active', 'purge_pending'];

type PosterAssetRow = { post_key?: string | null; storage_provider: string | null; storage_path: string | null; mime_type: string | null; purge_after: string | null };
type PosterSourceRow = { post_key?: string | null; thumbnail_url: string | null; carousel_urls: string[] | null };

// a carousel's stored slides (R2, not purged), in slide order. Only offered when /api/media can sign them
type SlideAssetRow = { post_key?: string | null; asset_role: string | null; mime_type: string | null; storage_provider: string | null; storage_path: string | null; purge_after: string | null };

async function storedSlides(sb: AdminClient, keys: string[], nowMs: number): Promise<Map<string, StreamSlide[]>> {
  const slides = new Map<string, StreamSlide[]>();
  const unique = Array.from(new Set(keys.filter(Boolean)));
  if (!unique.length || !previewsServable()) return slides;
  try {
    const rows = await fetchRowsByPostKeys<SlideAssetRow>(unique, (chunkKeys, start, end) => sb
      .from('post_media_assets')
      .select('post_key,asset_role,mime_type,storage_provider,storage_path,purge_after')
      .in('post_key', chunkKeys)
      .like('asset_role', 'carousel_%')
      .in('status', POSTER_STATUSES)
      .order('post_key', { ascending: true })
      .range(start, end));
    rows.forEach((assets, key) => {
      const list = assets
        .filter((row) => row.storage_provider === 'r2' && (row.storage_path || '').trim()
          && !(row.purge_after && Date.parse(row.purge_after) <= nowMs)
          && /^carousel_\d+$/.test((row.asset_role || '').trim().toLowerCase()))
        .map((row) => ({
          role: (row.asset_role as string).trim().toLowerCase(),
          video: (row.mime_type || '').trim().toLowerCase().startsWith('video/'),
        }))
        .sort((left, right) => Number(left.role.slice(9)) - Number(right.role.slice(9)));
      const seen = new Set<string>();
      const ordered = list.filter((slide) => (seen.has(slide.role) ? false : (seen.add(slide.role), true)));
      if (ordered.length > 1) slides.set(key, ordered);
    });
  } catch (error) {
    console.warn('[feed stream] slide read failed, carousels show their cover:', streamErrorMessage(error, 'unknown'));
  }
  return slides;
}

// the caption's opening, on one line's worth of whitespace
function captionOf(value: string | null | undefined): string | null {
  const text = (value || '').replace(/\s+/g, ' ').trim();
  if (!text) return null;
  return text.length > CAPTION_MAX ? `${text.slice(0, CAPTION_MAX - 1).trimEnd()}…` : text;
}

async function drawablePosters(sb: AdminClient, keys: string[], nowMs: number): Promise<Set<string> | null> {
  const unique = Array.from(new Set(keys.filter(Boolean)));
  const drawable = new Set<string>();
  if (!unique.length) return drawable;
  try {
    if (previewsServable()) {
      const assets = await fetchRowsByPostKeys<PosterAssetRow>(unique, (chunkKeys, start, end) => sb
        .from('post_media_assets')
        .select('post_key,storage_provider,storage_path,mime_type,purge_after')
        .in('post_key', chunkKeys)
        .in('asset_role', POSTER_ROLES)
        .in('status', POSTER_STATUSES)
        .order('post_key', { ascending: true })
        .range(start, end));
      assets.forEach((rows, key) => {
        const stored = rows.some((row) => {
          if (row.storage_provider !== 'r2' || !(row.storage_path || '').trim()) return false;
          if (row.purge_after && Date.parse(row.purge_after) <= nowMs) return false;
          const mime = (row.mime_type || '').trim().toLowerCase();
          return !mime || mime.startsWith('image/');
        });
        if (stored) drawable.add(key);
      });
    }
    const rest = unique.filter((key) => !drawable.has(key));
    if (rest.length) {
      const sources = await fetchRowsByPostKeys<PosterSourceRow>(rest, (chunkKeys, start, end) => sb
        .from('posts')
        .select('post_key,thumbnail_url,carousel_urls')
        .in('post_key', chunkKeys)
        .order('post_key', { ascending: true })
        .range(start, end));
      sources.forEach((rows, key) => {
        const row = rows[0];
        // the same source /api/media would fall back to: the thumbnail link, else the first slide
        const source = nullableString(row?.thumbnail_url)
          ?? (Array.isArray(row?.carousel_urls) ? row.carousel_urls.find((url) => typeof url === 'string' && url.trim()) ?? null : null);
        if (instagramLinkLive(source, nowMs)) drawable.add(key);
      });
    }
    return drawable;
  } catch (error) {
    console.warn('[feed stream] poster check failed, every post keeps its proxy:', streamErrorMessage(error, 'unknown'));
    return null;
  }
}

/* ── previews ──────────────────────────────────────────────── */

// /api/media can only sign a stored clip when R2 is configured; without it, a preview URL would fall through to
// the Instagram video
function previewsServable(): boolean {
  const endpoint = (process.env.R2_ENDPOINT_URL || '').trim();
  if (!endpoint || !(process.env.R2_ACCESS_KEY_ID || '').trim() || !(process.env.R2_SECRET_ACCESS_KEY || '').trim()) return false;
  try {
    const url = new URL(endpoint);
    return url.protocol === 'https:' || url.protocol === 'http:';
  } catch {
    return false;
  }
}

// the same test /api/media's fetchStoredAsset applies before redirecting to the stored clip, a little stricter
function isServablePreview(asset: PreviewAssetRow, nowMs: number): boolean {
  if ((asset.asset_role || '').trim().toLowerCase() !== 'preview_5s') return false;
  if (!PREVIEW_STATUSES.includes((asset.status || '').trim())) return false;
  if (asset.purge_after) {
    const purgeAt = Date.parse(asset.purge_after);
    if (!Number.isFinite(purgeAt) || purgeAt <= nowMs + PREVIEW_PURGE_MARGIN_MS) return false;
  }
  if (asset.storage_provider !== 'r2') return false;
  const path = typeof asset.storage_path === 'string' ? asset.storage_path.trim().replace(/^\/+/, '') : '';
  const bucket = (typeof asset.storage_bucket === 'string' ? asset.storage_bucket.trim() : '') || (process.env.R2_BUCKET || '').trim();
  if (!path || !bucket) return false;
  const mimeType = typeof asset.mime_type === 'string' ? asset.mime_type.trim().toLowerCase() : '';
  return !mimeType || mimeType.startsWith('video/');
}

/* ── the index ─────────────────────────────────────────────── */

async function fetchIndexCandidates(sb: AdminClient, scope: StreamScopeContext, window: StreamWindow): Promise<Candidate[]> {
  const feederIds = scope.feeders.map((feeder) => feeder.id);
  if (feederIds.length === 0) return [];
  // 'results' reaches back for posts whose late checkpoints land inside the window
  const fromDay = scope.order === 'results' ? shiftDay(window.start, -RESULT_REACH_DAYS) : window.start;
  const metricFields = scope.order === 'results' ? INDEX_METRICS_RESULTS : INDEX_METRICS_POSTED;
  const slices = splitDays(fromDay, window.today, INDEX_SLICES);
  const readSlices = (select: string) => Promise.all(slices.map((slice) => fetchPostRows(sb, feederIds, slice.from, slice.to, select)))
    .then((parts) => parts.flat());

  let rows: PostRow[];
  try {
    rows = await readSlices(`${INDEX_POST_FIELDS},post_metrics(${metricFields})`);
  } catch (error) {
    console.warn('[feed stream] index embed failed, joining metrics separately:', streamErrorMessage(error, 'unknown'));
    rows = await readSlices(INDEX_POST_FIELDS);
    await attachSeparately(sb, rows, metricFields, false);
  }
  return toCandidates(rows, scope);
}

function coverOrder(order: FeedOrder) {
  // the day's best measured posts first, then the newest
  return (left: Candidate, right: Candidate) => {
    const leftTop = topOf(left);
    const rightTop = topOf(right);
    if ((leftTop == null) !== (rightTop == null)) return leftTop == null ? 1 : -1;
    if (leftTop != null && rightTop != null && leftTop !== rightTop) return leftTop - rightTop;
    return momentOf(right, order) - momentOf(left, order) || (left.key < right.key ? -1 : left.key > right.key ? 1 : 0);
  };
}

/* ── followers (for the profile) ───────────────────────────── */

// the snapshot days worth reading: the last week (the latest count) and the days ±3 around 30 days before it
function followerSnapshotDays(today: string): string[] {
  const days: string[] = [];
  for (let back = 0; back < FOLLOWER_RECENT_DAYS; back += 1) days.push(shiftDay(today, -back));
  const nearest = FOLLOWER_SPAN_DAYS - FOLLOWER_SLACK_DAYS;
  const furthest = FOLLOWER_SPAN_DAYS + FOLLOWER_RECENT_DAYS - 1 + FOLLOWER_SLACK_DAYS;
  for (let back = nearest; back <= furthest; back += 1) days.push(shiftDay(today, -back));
  return days;
}

// one read, alongside the post scan; the profile goes without followers rather than fail
async function fetchFollowerSnapshots(sb: AdminClient, feeders: ScopeFeeder[], today: string): Promise<SnapshotRow[]> {
  if (feeders.length === 0) return [];
  const feederIds = feeders.map((feeder) => feeder.id);
  const days = followerSnapshotDays(today);
  try {
    return await fetchAllPages<SnapshotRow>((start, end) => sb
      .from('feeder_follower_snapshots')
      .select('feeder_id,snapshot_date_ist,follower_count')
      .in('feeder_id', feederIds)
      .in('snapshot_date_ist', days)
      .order('feeder_id', { ascending: true })
      .order('snapshot_date_ist', { ascending: false })
      .range(start, end));
  } catch (error) {
    console.warn('[feed stream] follower snapshots unavailable:', streamErrorMessage(error, 'unknown'));
    return [];
  }
}

type FollowerReading = { latestDay: string | null; latest: number | null; base: number | null };

function readFollowers(feeder: ScopeFeeder, counts: Map<string, number> | undefined, today: string): FollowerReading {
  const recentFrom = shiftDay(today, -(FOLLOWER_RECENT_DAYS - 1));
  let latestDay: string | null = null;
  for (const day of counts?.keys() ?? []) {
    if (day >= recentFrom && day <= today && (latestDay === null || day > latestDay)) latestDay = day;
  }
  if (!counts || latestDay === null) return { latestDay: null, latest: feeder.followerCount, base: null };
  const target = dayNumber(latestDay) - FOLLOWER_SPAN_DAYS;
  let base: number | null = null;
  let baseDistance = Infinity;
  for (const [day, count] of counts) {
    const distance = Math.abs(dayNumber(day) - target);
    if (distance <= FOLLOWER_SLACK_DAYS && distance < baseDistance) {
      base = count;
      baseDistance = distance;
    }
  }
  return { latestDay, latest: counts.get(latestDay) ?? null, base };
}

// followers: the latest counts summed over distinct accounts; the change: over the accounts with both counts
function followerSummary(feeders: ScopeFeeder[], rows: SnapshotRow[], today: string) {
  const byFeeder = new Map<number, Map<string, number>>();
  for (const row of rows) {
    const id = Number(row.feeder_id);
    const day = dateOnly(row.snapshot_date_ist);
    const count = nullableNumber(row.follower_count);
    if (!Number.isFinite(id) || !day || count == null) continue;
    const counts = byFeeder.get(id);
    if (counts) counts.set(day, count);
    else byFeeder.set(id, new Map([[day, count]]));
  }

  const byHandle = new Map<string, FollowerReading>();
  for (const feeder of feeders) {
    const reading = readFollowers(feeder, byFeeder.get(feeder.id), today);
    const current = byHandle.get(feeder.handle);
    const better = !current
      || (reading.latestDay !== null && (current.latestDay === null || reading.latestDay > current.latestDay))
      || (reading.latestDay === current.latestDay && reading.base != null && current.base == null)
      || (current.latest == null && reading.latest != null);
    if (better) byHandle.set(feeder.handle, reading);
  }

  let followers: number | null = null;
  let latestSum = 0;
  let baseSum = 0;
  for (const reading of byHandle.values()) {
    if (reading.latest != null) followers = (followers ?? 0) + reading.latest;
    if (reading.latestDay !== null && reading.latest != null && reading.base != null) {
      latestSum += reading.latest;
      baseSum += reading.base;
    }
  }
  return {
    followers,
    followerDelta30d: baseSum > 0 ? Math.round(((latestSum - baseSum) / baseSum) * 1000) / 10 : null,
  };
}

/* ── the profile ───────────────────────────────────────────── */

// the middle value, rounded to a whole Top %
function median(values: number[]): number | null {
  if (values.length === 0) return null;
  const sorted = values.slice().sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return Math.round(sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2);
}

function measuredTops(posts: Candidate[]): number[] {
  const tops: number[] = [];
  for (const post of posts) {
    const top = topOf(post);
    if (top != null) tops.push(top);
  }
  return tops;
}

function mediaOf(candidate: Candidate): StreamMediaType {
  return normalizeMediaType(candidate.row.media_type);
}

function likesOf(candidate: Candidate): number | null {
  return candidate.head ? nullableNumber(candidate.head.row.likes) : null;
}

// how far a post climbed from its first ranked checkpoint to its latest (lower Top % is better)
function jumpOf(candidate: Candidate): number | null {
  const head = candidate.head;
  if (!head || head.topPercent == null) return null;
  const first = candidate.landed.find((landed) => landed.topPercent != null);
  if (!first || first.rank >= head.rank) return null;
  const jump = (first.topPercent as number) - head.topPercent;
  return jump > 0 ? jump : null;
}

function istHourOf(candidate: Candidate): number {
  return new Date(candidate.postedAtMs + IST_OFFSET_MS).getUTCHours();
}

// many posts carry only their date (posted_at at exactly midnight, UTC or IST): no time of day to read from them
function hasTimeOfDay(candidate: Candidate): boolean {
  return candidate.postedAtMs % DAY_MS !== 0 && (candidate.postedAtMs + IST_OFFSET_MS) % DAY_MS !== 0;
}

function newestThenKey(left: Candidate, right: Candidate): number {
  return right.postedAtMs - left.postedAtMs || (left.key < right.key ? -1 : left.key > right.key ? 1 : 0);
}

function byTop(left: Candidate, right: Candidate): number {
  return (topOf(left) as number) - (topOf(right) as number) || newestThenKey(left, right);
}

// the group with the lowest typical Top % among those with enough measured posts (more posts, then the first
// listed, break a tie)
function bestGroup<Key>(groups: Map<Key, number[]>, minPosts: number): { key: Key; typical: number } | null {
  let best: { key: Key; typical: number; count: number } | null = null;
  for (const [key, tops] of groups) {
    if (tops.length < minPosts) continue;
    const typical = median(tops) as number;
    if (!best || typical < best.typical || (typical === best.typical && tops.length > best.count)) {
      best = { key, typical, count: tops.length };
    }
  }
  return best ? { key: best.key, typical: best.typical } : null;
}

function pushTo<Key, Value>(groups: Map<Key, Value[]>, key: Key, value: Value) {
  const bucket = groups.get(key);
  if (bucket) bucket.push(value);
  else groups.set(key, [value]);
}

function buildProfile(
  scope: StreamScopeContext,
  candidates: Candidate[],
  window: StreamWindow,
  followers: { followers: number | null; followerDelta30d: number | null },
): StreamProfile {
  const { today } = window;
  // the posts that went up in the window: the same set whichever order is being read
  const posts = candidates.filter((candidate) => candidate.day >= window.start && candidate.day <= today);
  const measured = posts.filter((candidate) => topOf(candidate) != null);

  const recentFrom = shiftDay(today, -(PROFILE_TYPICAL_DAYS - 1));
  const previousFrom = shiftDay(today, -(2 * PROFILE_TYPICAL_DAYS - 1));
  const typical = median(measuredTops(measured.filter((post) => post.day >= recentFrom)));
  const typicalPrev = median(measuredTops(measured.filter((post) => post.day >= previousFrom && post.day < recentFrom)));

  let lastPostMs = 0;
  for (const post of posts) if (post.postedAtMs > lastPostMs) lastPostMs = post.postedAtMs;

  // 13 Mon–Sun weeks, oldest first, ending with this one
  const thisWeek = groupKeyOf(today, 'week');
  const byWeek = new Map<string, Candidate[]>();
  for (const post of posts) pushTo(byWeek, groupKeyOf(post.day, 'week'), post);
  const weeks = Array.from({ length: PROFILE_WEEKS }, (_unused, index) => {
    const start = shiftDay(thisWeek, -7 * (PROFILE_WEEKS - 1 - index));
    const inWeek = byWeek.get(start) ?? [];
    return { start, typical: median(measuredTops(inWeek)), count: inWeek.length };
  });

  // up to five distinct posts, one per label, each the best of its kind not already shown
  const highlights: StreamHighlight[] = [];
  const shown = new Set<string>();
  const feature = (label: string, pool: Candidate[], compare: (left: Candidate, right: Candidate) => number) => {
    let pick: Candidate | null = null;
    for (const candidate of pool) {
      if (shown.has(candidate.key)) continue;
      if (!pick || compare(candidate, pick) < 0) pick = candidate;
    }
    if (!pick) return;
    shown.add(pick.key);
    highlights.push({
      key: pick.key,
      thumbnailUrl: thumbnailOf(pick),
      topPercent: topOf(pick),
      mediaType: mediaOf(pick),
      handle: pick.feeder.handle,
      label,
    });
  };
  feature('Best reel', measured.filter((post) => mediaOf(post) === 'reel'), byTop);
  feature('Best carousel', measured.filter((post) => mediaOf(post) === 'carousel'), byTop);
  feature(
    'Most liked',
    posts.filter((post) => (likesOf(post) ?? 0) > 0),
    (left, right) => (likesOf(right) as number) - (likesOf(left) as number) || newestThenKey(left, right),
  );
  feature(
    'Big jump',
    measured.filter((post) => jumpOf(post) != null),
    (left, right) => (jumpOf(right) as number) - (jumpOf(left) as number) || byTop(left, right),
  );
  feature('This week', measured.filter((post) => post.day >= thisWeek), byTop);

  // the best 3-hour IST window to post in, and the best format
  const byHours = new Map<number, number[]>();
  const byFormat = new Map<StreamMediaType, number[]>();
  for (const post of measured) {
    const top = topOf(post) as number;
    if (hasTimeOfDay(post)) pushTo(byHours, Math.floor(istHourOf(post) / PROFILE_HOUR_SPAN) * PROFILE_HOUR_SPAN, top);
    const format = mediaOf(post);
    if (FORMATS.includes(format)) pushTo(byFormat, format, top);
  }
  const hours = bestGroup(new Map(Array.from(byHours.entries()).sort((left, right) => left[0] - right[0])), PROFILE_MIN_POSTS);
  const format = bestGroup(new Map(FORMATS.filter((type) => byFormat.has(type)).map((type) => [type, byFormat.get(type) as number[]])), PROFILE_MIN_POSTS);

  // a feed: the feeder with the best typical post, and the one on a run of winners this week
  let leader: StreamProfile['leader'] = null;
  let onRun: StreamProfile['onRun'] = null;
  if (!scope.oneFeeder) {
    const byHandle = new Map<string, number[]>();
    for (const post of measured) pushTo(byHandle, post.feeder.handle, topOf(post) as number);
    const best = bestGroup(new Map(Array.from(byHandle.entries()).sort((left, right) => (left[0] < right[0] ? -1 : 1))), PROFILE_LEADER_MIN_POSTS);
    leader = best ? { handle: best.key, typical: best.typical } : null;

    const runFrom = shiftDay(today, -(PROFILE_RUN_DAYS - 1));
    const winners = new Map<string, number>();
    for (const post of measured) {
      if ((topOf(post) as number) > WINNER_MAX || !post.resultDay || post.resultDay < runFrom || post.resultDay > today) continue;
      winners.set(post.feeder.handle, (winners.get(post.feeder.handle) ?? 0) + 1);
    }
    for (const [handle, count] of winners) {
      if (!onRun || count > onRun.winners || (count === onRun.winners && handle < onRun.handle)) onRun = { handle, winners: count };
    }
  }

  return {
    posts: posts.length,
    typical,
    typicalPrev,
    followers: followers.followers,
    followerDelta30d: followers.followerDelta30d,
    lastPostAt: lastPostMs ? new Date(lastPostMs).toISOString() : null,
    weeks,
    highlights,
    bestHours: hours ? { start: hours.key, end: (hours.key + PROFILE_HOUR_SPAN) % 24, typical: hours.typical } : null,
    strongestFormat: format ? { type: format.key, typical: format.typical } : null,
    perWeek: Math.round((posts.length / (STREAM_WINDOW_DAYS / 7)) * 10) / 10,
    leader,
    onRun,
    today: {
      posts: posts.filter((post) => post.day === today).length,
      results: posts.filter((post) => post.resultDay === today).length,
    },
  };
}

/* ── the index ─────────────────────────────────────────────── */

export function buildStreamIndex(
  scope: StreamScopeContext,
  candidates: Candidate[],
  window: StreamWindow,
  nowMs: number,
  snapshots: SnapshotRow[] = [],
): StreamIndex {
  const groups = new Map<string, Candidate[]>();
  let total = 0;
  for (const candidate of candidates) {
    const day = sectionDay(candidate, scope.order);
    if (!day || day < window.start || day > window.today) continue;
    total += 1;
    pushTo(groups, day, candidate);
  }

  const byCover = coverOrder(scope.order);
  const days: StreamDay[] = Array.from(groups.keys())
    .sort((left, right) => (left < right ? 1 : left > right ? -1 : 0))
    .map((day) => {
      const posts = groups.get(day) || [];
      const covers = posts.slice().sort(byCover);
      return {
        day,
        count: posts.length,
        best: covers.length ? topOf(covers[0]) : null,
        covers: covers.slice(0, COVERS_PER_DAY).map((post) => ({
          key: post.key,
          thumbnailUrl: thumbnailOf(post),
          topPercent: topOf(post),
        })),
        tops: displayTops(posts, scope.order),
      };
    });

  // who posted in the last 24h, the most recent first (the same in either order)
  const freshSince = nowMs - FRESH_MS;
  const freshHandles: string[] = [];
  const seen = new Set<string>();
  candidates
    .filter((candidate) => candidate.postedAtMs >= freshSince)
    .sort((left, right) => right.postedAtMs - left.postedAtMs)
    .forEach((candidate) => {
      if (seen.has(candidate.feeder.handle)) return;
      seen.add(candidate.feeder.handle);
      freshHandles.push(candidate.feeder.handle);
    });

  return {
    scopeKey: scope.key,
    days,
    total,
    freshHandles,
    profile: buildProfile(scope, candidates, window, followerSummary(scope.feeders, snapshots, window.today)),
    generatedAt: new Date(nowMs).toISOString(),
  };
}

export async function loadStreamIndex(sb: AdminClient, scope: StreamScopeContext, nowMs: number = Date.now()): Promise<StreamIndex> {
  const window = streamWindow(nowMs);
  // the follower read runs alongside the post scan: no extra wait
  const [candidates, snapshots] = await Promise.all([
    fetchIndexCandidates(sb, scope, window),
    fetchFollowerSnapshots(sb, scope.feeders, window.today),
  ]);
  const index = buildStreamIndex(scope, candidates, window, nowMs, snapshots);
  // the profile's highlights draw at once: one without a picture to show gets its cover, not a doomed request
  const highlights = index.profile?.highlights ?? [];
  const drawable = highlights.length ? await drawablePosters(sb, highlights.map((item) => item.key), nowMs) : null;
  if (drawable) highlights.forEach((item) => {
    if (!drawable.has(item.key)) item.thumbnailUrl = null;
  });
  return index;
}

/* ── a page ────────────────────────────────────────────────── */

function toStreamPost(
  candidate: Candidate,
  previews: boolean,
  nowMs: number,
  drawable: ReadonlySet<string> | null,
  slides: ReadonlyMap<string, StreamSlide[]>,
): StreamPost {
  const { head, row } = candidate;
  const mediaType = normalizeMediaType(row.media_type);
  const hasPreview = previews
    && mediaType === 'reel'
    && (row.post_media_assets || []).some((asset) => isServablePreview(asset, nowMs));
  return {
    key: candidate.key,
    url: nullableString(row.post_url),
    handle: candidate.feeder.handle,
    feedId: candidate.feeder.feedId,
    mediaType,
    postedAt: candidate.postedAt,
    day: candidate.day,
    resultDay: candidate.resultDay,
    resultAt: candidate.resultAt,
    thumbnailUrl: thumbnailOf(candidate, drawable),
    previewUrl: hasPreview ? mediaProxyUrl(candidate.key, 'preview_5s') : null,
    slides: mediaType === 'carousel' ? slides.get(candidate.key) ?? [] : [],
    caption: captionOf(row.caption),
    results: candidate.landed.map((landed) => ({
      checkpoint: landed.checkpoint,
      topPercent: landed.topPercent,
      day: dateOnly(landed.row.business_date_ist),
    })),
    latestCheckpoint: head ? head.checkpoint : null,
    topPercent: head ? head.topPercent : null,
    multiple: head ? nullableNumber(head.row.ranking_multiple) : null,
    likes: head ? nullableNumber(head.row.likes) : null,
    comments: head ? nullableNumber(head.row.comments) : null,
    engagementRate: head ? percentOf(nullableNumber(head.row.engagement_rate)) : null,
  };
}

async function fetchPageCandidates(
  sb: AdminClient,
  scope: StreamScopeContext,
  from: string,
  to: string,
  previews: boolean,
): Promise<Candidate[]> {
  const feederIds = scope.feeders.map((feeder) => feeder.id);
  if (feederIds.length === 0) return [];
  const results = scope.order === 'results';
  // 'results': a result landing from..to belongs to a post that went up at most RESULT_REACH_DAYS before
  const postedFrom = results ? shiftDay(from, -RESULT_REACH_DAYS) : from;
  const base = PAGE_POST_FIELDS;

  let select = `${base},post_metrics(${PAGE_METRICS})`;
  const filters: EmbedFilter[] = [];
  if (previews) {
    select += `,post_media_assets(${PREVIEW_FIELDS})`;
    filters.push(
      { column: 'post_media_assets.asset_role', op: 'eq', value: 'preview_5s' },
      { column: 'post_media_assets.status', op: 'in', value: PREVIEW_STATUSES },
    );
  }
  if (results) {
    // only posts with a result landing in the range (their full results come in post_metrics above)
    select += ',landed:post_metrics!inner(business_date_ist)';
    filters.push(
      { column: 'landed.business_date_ist', op: 'gte', value: from },
      { column: 'landed.business_date_ist', op: 'lte', value: to },
    );
  }

  let rows: PostRow[];
  try {
    rows = await fetchPostRows(sb, feederIds, postedFrom, to, select, filters);
  } catch (error) {
    console.warn('[feed stream] page embed failed, joining results separately:', streamErrorMessage(error, 'unknown'));
    rows = await fetchPostRows(sb, feederIds, postedFrom, to, base);
    await attachSeparately(sb, rows, PAGE_METRICS, previews);
  }
  return toCandidates(rows, scope);
}

export async function loadStreamPage(
  sb: AdminClient,
  scope: StreamScopeContext,
  range: { from: string; to: string; empty: boolean },
  nowMs: number = Date.now(),
): Promise<StreamPage> {
  const page: StreamPage = { scopeKey: scope.key, from: range.from, to: range.to, posts: [] };
  if (range.empty) return page;

  const previews = previewsServable();
  const candidates = await fetchPageCandidates(sb, scope, range.from, range.to, previews);
  const inRange = candidates
    .filter((candidate) => {
      const day = sectionDay(candidate, scope.order);
      return Boolean(day) && (day as string) >= range.from && (day as string) <= range.to;
    })
    .sort(pageOrder(scope.order));
  const carouselKeys = inRange.filter((candidate) => normalizeMediaType(candidate.row.media_type) === 'carousel').map((candidate) => candidate.key);
  const [drawable, slides] = await Promise.all([
    drawablePosters(sb, inRange.map((candidate) => candidate.key), nowMs),
    storedSlides(sb, carouselKeys, nowMs),
  ]);
  page.posts = inRange.map((candidate) => toStreamPost(candidate, previews, nowMs, drawable, slides));
  return page;
}
