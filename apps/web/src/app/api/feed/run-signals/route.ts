import { promises as fs } from 'fs';
import path from 'path';
import { NextRequest, NextResponse } from 'next/server';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient } from '@/lib/supabase/server';
import { privateJsonResponse } from '@/lib/privateJsonResponse';
import type { RunSignal, RunSignalEvidence, RunSignalKind, RunSignalMetric } from '@/types/runSignals';
import anujRunBites from '@/data/runSignals/anuj_mp4_run_bites.json';
import lakmeRunBites from '@/data/runSignals/lakmeindia_run_bites.json';
import srishtiRunBites from '@/data/runSignals/srishtigargg_run_bites.json';

type FeederRow = {
  id: number;
  handle: string | null;
};

type PostRow = {
  post_key: string | null;
  feeder_id: number | null;
  caption: string | null;
  post_url: string | null;
  thumbnail_url: string | null;
  posted_at: string | null;
};

type MetricRow = {
  post_key: string | null;
  checkpoint: string | null;
  computed_at: string | null;
  views_multiple?: number | string | null;
  comments_multiple?: number | string | null;
  delta_from_d1?: number | string | null;
  percentile_performance?: number | string | null;
  percentile_performance_exact?: number | string | null;
};

type RunPostStat = {
  post?: string;
  placed?: string;
  views_vs_usual?: number;
  comments_vs_usual?: number;
  collab?: boolean;
  hour_ist?: number;
  legs?: boolean;
  carried_by?: string;
};

type RunBiteFixture = {
  run_stats?: {
    beat_usual_count?: number;
    of?: number;
    peak_placed?: string;
    typical_placed?: string;
    views_vs_usual_median?: number;
    comments_vs_usual_median?: number;
    followers_net?: number;
    posts_with_legs?: number;
    per_post?: RunPostStat[];
  };
  run_bites?: Array<{
    kind?: string;
    headline?: string;
    explainer?: string;
    evidence?: string[];
  }>;
};

type FeederFileReceipt = {
  post?: string;
  post_key?: string;
  weight?: string;
  how_it_shows_up?: string;
  rank_context?: {
    overall?: string;
    read?: string;
  };
};

type FeederFileBite = {
  name?: string;
  receipts?: FeederFileReceipt[];
};

type FeederFileMemory = {
  bites?: FeederFileBite[];
};

const RUN_SIGNAL_KINDS = new Set(['trend', 'watch', 'easy_win', 'what_changed', 'durability']);
const POST_KEY_CHUNK_SIZE = 250;
const STATIC_RUN_BITE_FIXTURES: Record<string, RunBiteFixture> = {
  anuj_mp4: anujRunBites as RunBiteFixture,
  lakmeindia: lakmeRunBites as RunBiteFixture,
  srishtigargg: srishtiRunBites as RunBiteFixture,
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

function normalizeHandle(value: string | null | undefined): string {
  return String(value || '').trim().replace(/^@+/, '').toLowerCase();
}

function accountForHandle(value: string | null | undefined): string {
  const handle = normalizeHandle(value);
  return handle ? `@${handle}` : '';
}

function fixtureSlug(handle: string): string {
  return normalizeHandle(handle).replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
}

function fixtureCandidates(): string[] {
  return [
    path.resolve(process.cwd(), '../worker/scripts/out'),
    path.resolve(process.cwd(), '../../apps/worker/scripts/out'),
    path.resolve(process.cwd(), 'apps/worker/scripts/out'),
  ];
}

function feederFileNames(handle: string): string[] {
  const slug = fixtureSlug(handle);
  const candidates: Record<string, string[]> = {
    anuj_mp4: [
      'anuj_mp4_merged_gpt54_20post_from_10chunk_v1.json',
      'anuj_mp4_feeder_file_cold_start_v8_1_gpt54.json',
    ],
    lakmeindia: [
      'lakmeindia_merged_gpt54_20post_from_10chunk_v1.json',
      'lakmeindia_feeder_file_cold_start_v8_1_gpt54.json',
    ],
    srishtigargg: [
      'srishtigargg_merged_srishti_gpt54_merge_next10_retry.json',
      'srishtigargg_merged_srishti_flash_merge_next10.json',
    ],
  };
  return candidates[slug] || [`${slug}_merged_gpt54_20post_from_10chunk_v1.json`];
}

async function readFeederMemory(handle: string): Promise<FeederFileMemory | null> {
  if (process.env.NODE_ENV === 'production') return null;

  for (const dir of fixtureCandidates()) {
    for (const fileName of feederFileNames(handle)) {
      try {
        const raw = await fs.readFile(path.join(dir, fileName), 'utf8');
        return JSON.parse(raw) as FeederFileMemory;
      } catch {}
    }
  }
  return null;
}

async function readFixture(handle: string): Promise<RunBiteFixture | null> {
  const staticFixture = STATIC_RUN_BITE_FIXTURES[fixtureSlug(handle)] || null;
  if (process.env.NODE_ENV === 'production') return staticFixture;

  const fileName = `${fixtureSlug(handle)}_run_bites.json`;
  for (const dir of fixtureCandidates()) {
    try {
      const raw = await fs.readFile(path.join(dir, fileName), 'utf8');
      return JSON.parse(raw) as RunBiteFixture;
    } catch {}
  }
  return staticFixture;
}

function mediaProxyUrl(postKey: string | null | undefined): string | null {
  const key = String(postKey || '').trim();
  return key ? `/api/media?postKey=${encodeURIComponent(key)}&role=thumbnail` : null;
}

function instagramPostUrl(postKey: string | null | undefined, postUrl?: string | null): string | null {
  const explicitUrl = nullableString(postUrl);
  if (explicitUrl) return explicitUrl;
  const cleanKey = String(postKey || '').trim().split('#')[0];
  const parts = cleanKey.split('/').filter(Boolean);
  const shortcode = parts.at(-1);
  const mediaType = parts[0] === 'reel' ? 'reel' : 'p';
  return shortcode ? `https://www.instagram.com/${mediaType}/${encodeURIComponent(shortcode)}/` : null;
}

function normText(value: string | null | undefined): string {
  return String(value || '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .replace(/[^\p{L}\p{N}\s@._-]/gu, '')
    .trim();
}

function evidenceKey(value: string | null | undefined): string {
  return normText(value).slice(0, 34);
}

function moveKey(value: string | null | undefined): string {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function displayMoveName(value: string | null | undefined): string | null {
  const key = moveKey(value);
  return key ? key.replace(/_/g, ' ') : null;
}

function compactNumber(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return '--';
  const rounded = Math.round(value);
  if (Math.abs(rounded) >= 1_000_000) return `${(rounded / 1_000_000).toFixed(1).replace(/\.0$/, '')}M`;
  if (Math.abs(rounded) >= 1_000) return `${(rounded / 1_000).toFixed(1).replace(/\.0$/, '')}K`;
  return String(rounded);
}

function signedCompact(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return '--';
  return `${value > 0 ? '+' : ''}${compactNumber(value)}`;
}

function formatMultiple(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return '--';
  return `${value.toFixed(value >= 10 ? 0 : 1).replace(/\.0$/, '')}x`;
}

function formatKind(value: RunSignalKind): string {
  if (value === 'easy_win') return 'Easy win';
  if (value === 'what_changed') return 'Changed';
  return value.replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase());
}

function metricList(fixture: RunBiteFixture): RunSignalMetric[] {
  const stats = fixture.run_stats || {};
  const metrics: RunSignalMetric[] = [];
  if (stats.beat_usual_count != null && stats.of != null) {
    metrics.push({ label: 'Hit rate', value: `${stats.beat_usual_count}/${stats.of}`, detail: 'beat usual', accent: true });
  }
  if (stats.peak_placed) metrics.push({ label: 'Peak', value: stats.peak_placed.replace(/^top\s+/i, 'Top '), detail: 'best post' });
  if (stats.views_vs_usual_median != null) metrics.push({ label: 'Views', value: formatMultiple(stats.views_vs_usual_median), detail: 'median' });
  if (stats.comments_vs_usual_median != null) metrics.push({ label: 'Talk', value: formatMultiple(stats.comments_vs_usual_median), detail: 'comments' });
  if (stats.posts_with_legs != null) metrics.push({ label: 'Legs', value: String(stats.posts_with_legs), detail: 'kept climbing' });
  if (stats.followers_net != null) metrics.push({ label: 'Followers', value: signedCompact(stats.followers_net), detail: 'net' });
  return metrics.slice(0, 5);
}

function latestMetricRows(rows: MetricRow[]): Map<string, MetricRow> {
  const checkpointRank: Record<string, number> = { d1: 1, d3: 2, d7: 3, d21: 4 };
  const latest = new Map<string, MetricRow & { rank: number; computedMs: number }>();
  for (const row of rows) {
    const postKey = nullableString(row.post_key);
    const checkpoint = normalizeHandle(row.checkpoint);
    const rank = checkpointRank[checkpoint] || 0;
    if (!postKey || !rank) continue;
    const computedMs = Date.parse(nullableString(row.computed_at) || '') || 0;
    const current = latest.get(postKey);
    if (!current || rank > current.rank || (rank === current.rank && computedMs > current.computedMs)) {
      latest.set(postKey, { ...row, rank, computedMs });
    }
  }
  return new Map(Array.from(latest.entries()).map(([key, value]) => [key, value]));
}

function parseRankFraction(value: string | null | undefined): number {
  const match = String(value || '').match(/(\d+)\s*\/\s*(\d+)/);
  if (!match) return Number.POSITIVE_INFINITY;
  const numerator = Number(match[1]);
  const denominator = Number(match[2]);
  return Number.isFinite(numerator) && Number.isFinite(denominator) && denominator > 0
    ? numerator / denominator
    : Number.POSITIVE_INFINITY;
}

function receiptWeightRank(value: string | null | undefined): number {
  const normalized = normalizeHandle(value);
  if (normalized === 'core') return 0;
  if (normalized === 'supporting') return 1;
  if (normalized === 'standby') return 2;
  return 3;
}

function memoryByMove(feederMemory: FeederFileMemory | null): Map<string, FeederFileBite> {
  const byMove = new Map<string, FeederFileBite>();
  for (const bite of feederMemory?.bites || []) {
    const key = moveKey(bite.name);
    if (key && !byMove.has(key)) byMove.set(key, bite);
  }
  return byMove;
}

async function fetchPostsAndMetrics(
  sb: ReturnType<typeof adminClient>,
  feeders: FeederRow[],
): Promise<{
  postsByHandle: Map<string, PostRow[]>;
  metricsByPost: Map<string, MetricRow>;
}> {
  if (feeders.length === 0) return { postsByHandle: new Map(), metricsByPost: new Map() };
  const feederIds = feeders.map((row) => row.id).filter((id) => Number.isFinite(id));
  const feederById = new Map(feeders.map((row) => [row.id, normalizeHandle(row.handle)]));

  const { data: postsData, error: postsError } = await sb
    .from('posts')
    .select('post_key,feeder_id,caption,post_url,thumbnail_url,posted_at')
    .in('feeder_id', feederIds)
    .order('posted_at', { ascending: false })
    .limit(500);
  if (postsError) throw postsError;

  const posts = (postsData || []) as PostRow[];
  const postsByHandle = new Map<string, PostRow[]>();
  for (const post of posts) {
    const handle = post.feeder_id != null ? feederById.get(post.feeder_id) : '';
    if (!handle) continue;
    const bucket = postsByHandle.get(handle) || [];
    bucket.push(post);
    postsByHandle.set(handle, bucket);
  }

  const postKeys = posts
    .map((post) => nullableString(post.post_key))
    .filter((value): value is string => Boolean(value));
  const metricRows: MetricRow[] = [];
  for (let start = 0; start < postKeys.length; start += POST_KEY_CHUNK_SIZE) {
    const { data, error } = await sb
      .from('post_metrics')
      .select('post_key,checkpoint,computed_at,views_multiple,comments_multiple,delta_from_d1,percentile_performance,percentile_performance_exact')
      .in('post_key', postKeys.slice(start, start + POST_KEY_CHUNK_SIZE))
      .in('checkpoint', ['d1', 'd3', 'd7', 'd21']);
    if (error) throw error;
    metricRows.push(...((data || []) as MetricRow[]));
  }

  return { postsByHandle, metricsByPost: latestMetricRows(metricRows) };
}

function buildEvidenceResolver(
  handle: string,
  fixture: RunBiteFixture,
  postsByHandle: Map<string, PostRow[]>,
  metricsByPost: Map<string, MetricRow>,
) {
  const postStats = fixture.run_stats?.per_post || [];
  const handlePosts = postsByHandle.get(normalizeHandle(handle)) || [];
  const postByKey = new Map<string, PostRow>();
  for (const post of handlePosts) {
    const key = nullableString(post.post_key);
    if (key && !postByKey.has(key)) postByKey.set(key, post);
  }

  const byCaptionKey = new Map<string, PostRow>();
  for (const post of handlePosts) {
    const key = evidenceKey(post.caption);
    if (key && !byCaptionKey.has(key)) byCaptionKey.set(key, post);
  }

  function matchPost(label: string): PostRow | null {
    const exact = byCaptionKey.get(evidenceKey(label));
    if (exact) return exact;
    const target = normText(label);
    if (!target) return null;
    return handlePosts.find((post) => {
      const caption = normText(post.caption);
      return caption.startsWith(target.slice(0, 28)) || target.startsWith(caption.slice(0, 28));
    }) || null;
  }

  function evidenceFromParts({
    label,
    post,
    postKey,
    stat,
    source = 'current',
    moveName = null,
    receiptRead = null,
    placedOverride = null,
  }: {
    label: string;
    post?: PostRow | null;
    postKey?: string | null;
    stat?: RunPostStat | null;
    source?: 'current' | 'memory';
    moveName?: string | null;
    receiptRead?: string | null;
    placedOverride?: string | null;
  }): RunSignalEvidence {
    const key = nullableString(postKey) || nullableString(post?.post_key) || '';
    const metric = key ? metricsByPost.get(key) || null : null;
    const viewsMultiple = nullableNumber(stat?.views_vs_usual) ?? nullableNumber(metric?.views_multiple);
    const commentsMultiple = nullableNumber(stat?.comments_vs_usual) ?? nullableNumber(metric?.comments_multiple);
    const percentile = nullableNumber(metric?.percentile_performance_exact) ?? nullableNumber(metric?.percentile_performance);
    const placed = placedOverride || stat?.placed || (percentile != null ? `top ${Math.round(percentile)}%` : null);

    return {
      post_key: key,
      post_url: instagramPostUrl(key, post?.post_url),
      thumbnail_url: mediaProxyUrl(key) || nullableString(post?.thumbnail_url),
      title: nullableString(label) || nullableString(post?.caption) || 'Evidence post',
      source,
      move_name: moveName,
      receipt_read: receiptRead,
      placed,
      views_vs_usual: viewsMultiple,
      comments_vs_usual: commentsMultiple,
      legs: Boolean(stat?.legs || (nullableNumber(metric?.delta_from_d1) ?? 0) >= 5),
      carried_by: nullableString(stat?.carried_by),
      hour_ist: nullableNumber(stat?.hour_ist),
    };
  }

  return {
    current(label: string): RunSignalEvidence {
      const stat = postStats.find((item) => evidenceKey(item.post) === evidenceKey(label))
        || postStats.find((item) => normText(item.post).startsWith(normText(label).slice(0, 28)));
      const post = matchPost(label);
      return evidenceFromParts({ label, post, postKey: post?.post_key, stat, source: 'current' });
    },
    memory(receipt: FeederFileReceipt, moveName: string): RunSignalEvidence {
      const postKey = nullableString(receipt.post_key) || '';
      const post = postKey ? postByKey.get(postKey) || null : null;
      return evidenceFromParts({
        label: nullableString(receipt.post) || nullableString(post?.caption) || moveName,
        post,
        postKey,
        source: 'memory',
        moveName,
        receiptRead: nullableString(receipt.how_it_shows_up),
        placedOverride: nullableString(receipt.rank_context?.overall),
      });
    },
  };
}

function memoryEvidenceForMoves({
  moveNames,
  moveMemory,
  resolveMemory,
  currentPostKeys,
}: {
  moveNames: string[];
  moveMemory: Map<string, FeederFileBite>;
  resolveMemory: (receipt: FeederFileReceipt, moveName: string) => RunSignalEvidence;
  currentPostKeys: Set<string>;
}): RunSignalEvidence[] {
  const selected: RunSignalEvidence[] = [];
  const seen = new Set<string>();

  for (const rawMove of moveNames) {
    const key = moveKey(rawMove);
    const bite = moveMemory.get(key);
    const moveName = displayMoveName(bite?.name || rawMove);
    if (!bite || !moveName) continue;

    const receipts = (bite.receipts || [])
      .filter((receipt) => nullableString(receipt.post_key) && !currentPostKeys.has(nullableString(receipt.post_key) || ''))
      .sort((a, b) => {
        const weightDelta = receiptWeightRank(a.weight) - receiptWeightRank(b.weight);
        if (weightDelta !== 0) return weightDelta;
        return parseRankFraction(a.rank_context?.overall) - parseRankFraction(b.rank_context?.overall);
      });

    for (const receipt of receipts.slice(0, 2)) {
      const evidence = resolveMemory(receipt, moveName);
      const evidenceKeyValue = evidence.post_key || `${moveName}:${evidence.title}`;
      if (!seen.has(evidenceKeyValue)) {
        selected.push(evidence);
        seen.add(evidenceKeyValue);
      }
      if (selected.length >= 3) return selected;
    }
  }

  return selected;
}

function buildSignalsForHandle(
  handle: string,
  fixture: RunBiteFixture,
  postsByHandle: Map<string, PostRow[]>,
  metricsByPost: Map<string, MetricRow>,
  feederMemory: FeederFileMemory | null,
): RunSignal[] {
  const evidenceResolver = buildEvidenceResolver(handle, fixture, postsByHandle, metricsByPost);
  const moveMemory = memoryByMove(feederMemory);
  const account = accountForHandle(handle);
  const sharedMetrics = metricList(fixture);
  const stats = fixture.run_stats || {};
  const runLabel = stats.of ? `Latest ${stats.of}-post run` : 'Latest run';

  return (fixture.run_bites || [])
    .map((bite, index) => {
      const normalizedKind = normalizeHandle(bite.kind) as RunSignalKind;
      const kind = RUN_SIGNAL_KINDS.has(normalizedKind) ? normalizedKind : 'trend';
      const evidence = (bite.evidence || []).slice(0, 3).map(evidenceResolver.current);
      const memoryMoves = Array.from(new Set(
        evidence
          .map((item) => displayMoveName(item.carried_by))
          .filter((item): item is string => Boolean(item)),
      ));
      const currentPostKeys = new Set(evidence.map((item) => item.post_key).filter(Boolean));
      const memoryEvidence = memoryEvidenceForMoves({
        moveNames: memoryMoves,
        moveMemory,
        resolveMemory: evidenceResolver.memory,
        currentPostKeys,
      });
      return {
        id: `${fixtureSlug(handle)}:${kind}:${index}`,
        account,
        accountLabel: account,
        kind,
        headline: nullableString(bite.headline) || formatKind(kind),
        explainer: nullableString(bite.explainer) || '',
        generatedAt: null,
        runLabel,
        metrics: sharedMetrics,
        evidence,
        memory_evidence: memoryEvidence,
        memory_moves: memoryMoves,
      };
    })
    .filter((signal) => signal.headline && signal.explainer);
}

export async function GET(req: NextRequest) {
  try {
    const sb = await createClient();
    const { data: { user }, error: authError } = await sb.auth.getUser();
    if (authError || !user) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });

    const feedId = Number(req.nextUrl.searchParams.get('feedId') || 0);
    const requestedHandle = normalizeHandle(req.nextUrl.searchParams.get('handle'));
    const limit = Math.max(1, Math.min(24, Number(req.nextUrl.searchParams.get('limit') || 12)));
    if (!feedId) return NextResponse.json({ error: 'feedId is required' }, { status: 400 });

    const admin = adminClient();
    const { data: feed, error: feedError } = await admin
      .from('feeds')
      .select('id,user_id')
      .eq('id', feedId)
      .eq('user_id', user.id)
      .maybeSingle();
    if (feedError) throw feedError;
    if (!feed) return NextResponse.json({ error: 'Feed not found' }, { status: 404 });

    const { data: feederData, error: feederError } = await admin
      .from('feeders')
      .select('id,handle')
      .eq('feed_id', feedId)
      .eq('status', 'active');
    if (feederError) throw feederError;

    const feeders = ((feederData || []) as FeederRow[])
      .map((row) => ({ id: Number(row.id), handle: row.handle }))
      .filter((row) => Number.isFinite(row.id) && normalizeHandle(row.handle))
      .filter((row) => !requestedHandle || requestedHandle === 'all' || normalizeHandle(row.handle) === requestedHandle);

    if (feeders.length === 0) return privateJsonResponse(req, { signals: [] });

    const fixtures = new Map<string, RunBiteFixture>();
    await Promise.all(feeders.map(async (feeder) => {
      const handle = normalizeHandle(feeder.handle);
      const fixture = await readFixture(handle);
      if (fixture?.run_bites?.length) fixtures.set(handle, fixture);
    }));

    if (fixtures.size === 0) return privateJsonResponse(req, { signals: [] });

    const feederMemories = new Map<string, FeederFileMemory | null>();
    await Promise.all(Array.from(fixtures.keys()).map(async (handle) => {
      feederMemories.set(handle, await readFeederMemory(handle));
    }));

    const { postsByHandle, metricsByPost } = await fetchPostsAndMetrics(admin, feeders);
    const signals = Array.from(fixtures.entries())
      .flatMap(([handle, fixture]) => buildSignalsForHandle(
        handle,
        fixture,
        postsByHandle,
        metricsByPost,
        feederMemories.get(handle) || null,
      ))
      .slice(0, limit);

    return privateJsonResponse(req, { signals });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Failed to load run signals';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
