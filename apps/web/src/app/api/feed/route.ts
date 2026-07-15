import { NextRequest, NextResponse } from 'next/server';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient } from '@/lib/supabase/server';
import { privateJsonResponse } from '@/lib/privateJsonResponse';
import {
  invalidateServerRouteCacheByPrefix,
  withServerRouteCache,
  writeServerRouteCache,
} from '@/lib/serverRouteCache';

type FeedRow = {
  id: number;
  user_id: string;
  name: string;
  status: string;
  created_at: string;
  context_brief?: Record<string, unknown> | null;
  context_bible?: string | null;
};

type FeederRow = {
  id: number;
  feed_id: number;
  handle: string;
  role: 'anchor' | 'standard';
  status: string;
  created_at: string;
  profile_pic_url: string | null;
  follower_count: number | null;
};

type PostRow = {
  post_key: string;
  feeder_id: number;
  posted_at: string | null;
  media_type: string | null;
};

type MetricRow = {
  post_key: string;
  checkpoint: string;
  likes: number | null;
  comments: number | null;
  views: number | null;
  computed_at: string | null;
};

type TickerRow = {
  id: string;
  handle: string;
  likesDelta: number;
  commentsDelta: number;
  viewsDelta: number;
  postsDelta: number;
};

type FeedBundlePayload = {
  feeds: Array<{
    id: string;
    title: string;
    feeders: Array<{
      handle: string;
      isAnchor: boolean;
      profilePicUrl?: string | null;
      followerCount?: number | null;
      metrics: {
        likes: string;
        comments: string;
        views: string;
        postsTracked: string;
      };
    }>;
    metrics: {
      likes: string;
      comments: string;
      views: string;
      postsTracked: string;
    };
    feedBrief?: Record<string, unknown>;
    feedBriefText?: string;
    contextBrief?: Record<string, unknown>;
    contextBible?: string;
  }>;
  ticker: TickerRow[];
};

type InstagramProfileProbe = {
  ok: boolean;
  profilePicUrl: string | null;
  followerCount: number | null;
  error?: string;
};

type SupabaseAdminClient = ReturnType<typeof adminClient>;

type MediaAssetCleanupRow = {
  storage_provider: string | null;
  storage_bucket: string | null;
  storage_path: string | null;
};

const STORAGE_DELETE_BATCH_SIZE = 100;
const DB_FETCH_BATCH_SIZE = 1000;
const FEED_BUNDLE_TTL_MS = 5 * 60 * 1000;

function feedBundleCacheKey(userId: string) {
  return `feed:bundle:${userId}`;
}

function invalidateUserDerivedCaches(userId: string) {
  invalidateServerRouteCacheByPrefix(`feed:bundle:${userId}`);
  invalidateServerRouteCacheByPrefix(`feed:dashboard:${userId}:`);
  invalidateServerRouteCacheByPrefix(`fire:active:${userId}`);
  invalidateServerRouteCacheByPrefix(`fire:meta:${userId}:`);
  invalidateServerRouteCacheByPrefix(`fire:page:${userId}:`);
}

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

function chunkArray<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

async function fetchPostKeysForFeederIds(sb: SupabaseAdminClient, feederIds: number[]) {
  if (feederIds.length === 0) return [];

  const postKeys: string[] = [];
  for (const feederChunk of chunkArray(feederIds, 100)) {
    for (let from = 0; ; from += DB_FETCH_BATCH_SIZE) {
      const { data, error } = await sb
        .from('posts')
        .select('post_key')
        .in('feeder_id', feederChunk)
        .range(from, from + DB_FETCH_BATCH_SIZE - 1);

      if (error) throw error;
      const rows = (data || []) as Array<{ post_key: string | null }>;
      for (const row of rows) {
        if (row.post_key) postKeys.push(row.post_key);
      }
      if (rows.length < DB_FETCH_BATCH_SIZE) break;
    }
  }

  return Array.from(new Set(postKeys));
}

async function purgeStoredMediaForPostKeys(sb: SupabaseAdminClient, postKeys: string[]) {
  if (postKeys.length === 0) return;

  const pathsByBucket = new Map<string, Set<string>>();
  for (const postKeyChunk of chunkArray(postKeys, 500)) {
    for (let from = 0; ; from += DB_FETCH_BATCH_SIZE) {
      const { data, error } = await sb
        .from('post_media_assets')
        .select('storage_provider,storage_bucket,storage_path')
        .in('post_key', postKeyChunk)
        .range(from, from + DB_FETCH_BATCH_SIZE - 1);

      if (error) throw error;
      const rows = (data || []) as MediaAssetCleanupRow[];
      for (const row of rows) {
        if ((row.storage_provider || 'supabase') !== 'supabase') continue;
        const bucket = (row.storage_bucket || '').trim();
        const path = (row.storage_path || '').trim();
        if (!bucket || !path) continue;
        const bucketPaths = pathsByBucket.get(bucket) || new Set<string>();
        bucketPaths.add(path);
        pathsByBucket.set(bucket, bucketPaths);
      }
      if (rows.length < DB_FETCH_BATCH_SIZE) break;
    }
  }

  for (const [bucket, paths] of pathsByBucket.entries()) {
    for (const pathBatch of chunkArray(Array.from(paths), STORAGE_DELETE_BATCH_SIZE)) {
      const { error } = await sb.storage.from(bucket).remove(pathBatch);
      if (error) throw error;
    }
  }
}

async function purgeStoredMediaForFeederIds(sb: SupabaseAdminClient, feederIds: number[]) {
  const postKeys = await fetchPostKeysForFeederIds(sb, feederIds);
  await purgeStoredMediaForPostKeys(sb, postKeys);
}

function normalizeHandle(raw: string) {
  return (raw || '').trim().replace(/^@+/, '').toLowerCase();
}

function normalizeUrl(url: string | null | undefined) {
  if (!url) return null;
  const trimmed = String(url).trim();
  return trimmed ? trimmed : null;
}

function buildProfileImageProxyUrl(url: string | null | undefined) {
  const normalized = normalizeUrl(url);
  if (!normalized) return null;
  const search = new URLSearchParams({
    role: 'thumbnail',
    url: normalized,
  });
  return `/api/media?${search.toString()}`;
}

function compactStringArray(value: unknown): string[] {
  return Array.isArray(value)
    ? value.map((item) => String(item || '').trim()).filter(Boolean).slice(0, 12)
    : [];
}

function formatBriefList(items: string[], fallback = 'metric-moving signals') {
  const clean = items.map((item) => item.trim()).filter(Boolean);
  if (clean.length === 0) return fallback;
  if (clean.length === 1) return clean[0];
  if (clean.length === 2) return `${clean[0]} and ${clean[1]}`;
  return `${clean.slice(0, -1).join(', ')}, and ${clean[clean.length - 1]}`;
}

function briefRelationshipPhrase(value: string) {
  switch (value) {
    case 'Watching competitors':
      return 'competitive awareness';
    case 'Tracking inspiration':
      return 'craft inspiration';
    case 'Monitoring our own brand':
      return 'own-brand monitoring';
    case 'Category research':
      return 'category research';
    default:
      return value ? value.toLowerCase() : 'social tracking';
  }
}

function briefFromBody(body: Record<string, unknown>) {
  const rawFeedBrief = body.feedBrief;
  const feedBrief = rawFeedBrief && typeof rawFeedBrief === 'object' && !Array.isArray(rawFeedBrief)
    ? (rawFeedBrief as Record<string, unknown>)
    : null;
  const contextBrief = body.contextBrief && typeof body.contextBrief === 'object' && !Array.isArray(body.contextBrief)
    ? (body.contextBrief as Record<string, unknown>)
    : null;
  return feedBrief || contextBrief || {};
}

function bibleFromBody(body: Record<string, unknown>) {
  return String(body.feedBriefText || body.contextBible || '').trim();
}

function buildFeedBriefTextFromBrief(brief: Record<string, unknown>, fallbackTitle: string) {
  const location = brief.location && typeof brief.location === 'object' && !Array.isArray(brief.location)
    ? brief.location as Record<string, unknown>
    : null;
  const locationText = typeof brief.location === 'string' ? brief.location : location?.value;
  const relationship = String(brief.relationship || brief.trackingMode || brief.trackingType || brief.tracking_type || brief.tracking_mode || '').trim();
  const accountTypes = compactStringArray(brief.accountTypes || brief.account_types);
  const legacyAccountType = String(brief.accountType || brief.accountTypeLabel || brief.account_type || '').trim();
  const category = String(brief.category || brief.market || locationText || '').trim();
  const geography = String(brief.geography || '').trim();
  const audience = String(brief.audience || '').trim();
  const priorities = compactStringArray(brief.priorities || brief.outcomes || brief.outcomesPriority || brief.outcomes_priority).slice(0, 3);
  const note = String(brief.note || brief.freeNote || brief.free_note || brief.customNote || brief.custom_note || '').trim();
  const scopePhrase = geography
    ? geography.toLowerCase() === 'global'
      ? 'globally'
      : `in ${geography}`
    : 'wherever the audience lives';
  const parts = [
    `This feed tracks ${category || fallbackTitle || 'social activity'} ${formatBriefList(accountTypes.length > 0 ? accountTypes : legacyAccountType ? [legacyAccountType] : [], 'accounts')} ${scopePhrase} for ${briefRelationshipPhrase(relationship)}.`,
    audience ? `The audience that matters: ${audience}.` : '',
    `Brief will surface ${formatBriefList(priorities)} first, while still flagging follower movement, fades, and gaps when they're real.`,
    note ? `Local context: ${note}` : '',
  ].filter(Boolean);
  return parts.join(' ').trim() || `This feed tracks ${fallbackTitle}. Brief should explain metric-moving account activity.`;
}

function isBadInstagramImageUrl(url: string | null | undefined) {
  if (!url) return true;
  const u = String(url).toLowerCase();
  if (u.includes('static.cdninstagram.com/rsrc.php')) return true;
  if (u.includes('/rsrc.php/')) return true;
  return false;
}

function toMetricString(value: number) {
  return String(Math.max(0, Math.floor(value)));
}

function toMetricNumber(value: unknown) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizeFeedBundlePayload(payload: unknown): FeedBundlePayload {
  const root = payload && typeof payload === 'object' && !Array.isArray(payload)
    ? (payload as Record<string, unknown>)
    : {};
  const feeds = Array.isArray(root.feeds) ? root.feeds : [];
  const ticker = Array.isArray(root.ticker) ? root.ticker : [];

  return {
    feeds: feeds.map((feed) => {
      const row = feed && typeof feed === 'object' && !Array.isArray(feed)
        ? (feed as Record<string, unknown>)
        : {};
      const feeders = Array.isArray(row.feeders) ? row.feeders : [];
      const metrics = row.metrics && typeof row.metrics === 'object' && !Array.isArray(row.metrics)
        ? (row.metrics as Record<string, unknown>)
        : {};

      return {
        id: String(row.id ?? ''),
        title: String(row.title ?? '').toUpperCase(),
        feedBrief: row.feedBrief && typeof row.feedBrief === 'object' && !Array.isArray(row.feedBrief)
          ? row.feedBrief as Record<string, unknown>
          : row.contextBrief && typeof row.contextBrief === 'object' && !Array.isArray(row.contextBrief)
            ? row.contextBrief as Record<string, unknown>
            : {},
        feedBriefText: String(row.feedBriefText || row.contextBible || ''),
        contextBrief: row.contextBrief && typeof row.contextBrief === 'object' && !Array.isArray(row.contextBrief)
          ? row.contextBrief as Record<string, unknown>
          : {},
        contextBible: String(row.contextBible || ''),
        feeders: feeders.map((feeder) => {
          const feederRow = feeder && typeof feeder === 'object' && !Array.isArray(feeder)
            ? (feeder as Record<string, unknown>)
            : {};
          const feederMetrics = feederRow.metrics && typeof feederRow.metrics === 'object' && !Array.isArray(feederRow.metrics)
            ? (feederRow.metrics as Record<string, unknown>)
            : {};
          const profilePicUrl = normalizeUrl(String(feederRow.profilePicUrl || ''));

          return {
            handle: String(feederRow.handle ?? ''),
            isAnchor: feederRow.isAnchor === true,
            profilePicUrl: profilePicUrl && !profilePicUrl.includes('unavatar.io/instagram')
              ? buildProfileImageProxyUrl(profilePicUrl)
              : null,
            followerCount: feederRow.followerCount == null ? null : toMetricNumber(feederRow.followerCount),
            metrics: {
              likes: toMetricString(toMetricNumber(feederMetrics.likes)),
              comments: toMetricString(toMetricNumber(feederMetrics.comments)),
              views: toMetricString(toMetricNumber(feederMetrics.views)),
              postsTracked: toMetricString(toMetricNumber(feederMetrics.postsTracked)),
            },
          };
        }),
        metrics: {
          likes: toMetricString(toMetricNumber(metrics.likes)),
          comments: toMetricString(toMetricNumber(metrics.comments)),
          views: toMetricString(toMetricNumber(metrics.views)),
          postsTracked: toMetricString(toMetricNumber(metrics.postsTracked)),
        },
      };
    }),
    ticker: ticker.map((item) => {
      const row = item && typeof item === 'object' && !Array.isArray(item)
        ? (item as Record<string, unknown>)
        : {};
      return {
        id: String(row.id ?? ''),
        handle: String(row.handle ?? ''),
        likesDelta: toMetricNumber(row.likesDelta),
        commentsDelta: toMetricNumber(row.commentsDelta),
        viewsDelta: toMetricNumber(row.viewsDelta),
        postsDelta: toMetricNumber(row.postsDelta),
      };
    }),
  };
}

async function attachFeedContexts(sb: SupabaseAdminClient, userId: string, bundle: FeedBundlePayload): Promise<FeedBundlePayload> {
  const feedIds = bundle.feeds.map((feed) => Number(feed.id)).filter((id) => Number.isFinite(id) && id > 0);
  if (feedIds.length === 0) return bundle;

  const { data, error } = await sb
    .from('feeds')
    .select('id,context_brief,context_bible')
    .eq('user_id', userId)
    .in('id', feedIds);
  if (error) return bundle;

  const contextById = new Map<number, { context_brief?: Record<string, unknown> | null; context_bible?: string | null }>();
  for (const row of data || []) {
    contextById.set(Number(row.id), row as { context_brief?: Record<string, unknown> | null; context_bible?: string | null });
  }

  return {
    ...bundle,
    feeds: bundle.feeds.map((feed) => {
      const context = contextById.get(Number(feed.id));
      if (!context) return feed;
      return {
        ...feed,
        feedBrief: context.context_brief && typeof context.context_brief === 'object' && !Array.isArray(context.context_brief)
          ? context.context_brief
          : {},
        feedBriefText: String(context.context_bible || ''),
        contextBrief: context.context_brief && typeof context.context_brief === 'object' && !Array.isArray(context.context_brief)
          ? context.context_brief
          : {},
        contextBible: String(context.context_bible || ''),
      };
    }),
  };
}

function decodeHtmlEntities(value: string) {
  return value
    .replace(/&#x([0-9a-f]+);/gi, (_, hex: string) => String.fromCodePoint(parseInt(hex, 16)))
    .replace(/&#(\d+);/g, (_, numeric: string) => String.fromCodePoint(parseInt(numeric, 10)))
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function extractMetaContent(html: string, property: string) {
  const escaped = property.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const propertyFirst = new RegExp(`<meta[^>]+property=["']${escaped}["'][^>]+content=(["'])(.*?)\\1`, 'i').exec(html);
  if (propertyFirst?.[2]) return decodeHtmlEntities(propertyFirst[2]);
  const contentFirst = new RegExp(`<meta[^>]+content=(["'])(.*?)\\1[^>]+property=["']${escaped}["']`, 'i').exec(html);
  return contentFirst?.[2] ? decodeHtmlEntities(contentFirst[2]) : null;
}

function extractCanonicalHandle(html: string) {
  const canonical = /<link[^>]+rel=["']canonical["'][^>]+href=["']([^"']+)["']/i.exec(html)?.[1];
  if (!canonical) return null;
  try {
    const parsed = new URL(decodeHtmlEntities(canonical));
    return parsed.pathname.split('/').filter(Boolean)[0]?.toLowerCase() || null;
  } catch {
    return null;
  }
}

function parseFollowerCountText(value: string | null) {
  if (!value) return null;
  const match = /([\d,.]+)\s*([kmb])?\s+followers/i.exec(value);
  if (!match) return null;
  const amount = Number(match[1].replace(/,/g, ''));
  if (!Number.isFinite(amount)) return null;
  const suffix = (match[2] || '').toLowerCase();
  const multiplier = suffix === 'k' ? 1_000 : suffix === 'm' ? 1_000_000 : suffix === 'b' ? 1_000_000_000 : 1;
  return Math.max(0, Math.round(amount * multiplier));
}

function parseIsoTime(value: string | null | undefined) {
  if (!value) return NaN;
  return Date.parse(value);
}

function finiteMetricNumber(value: number | null | undefined) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function isVideoMediaType(value: string | null | undefined) {
  const normalized = String(value || '').trim().toLowerCase();
  return normalized.includes('reel') || normalized.includes('video');
}

function isTrackedPost(post: PostRow, feeder: FeederRow, checkpointsByPost: Map<string, Set<string>>) {
  const checkpoints = checkpointsByPost.get(post.post_key);
  if (!checkpoints?.has('d1')) return false;

  const postedAt = parseIsoTime(post.posted_at);
  const trackingStartedAt = parseIsoTime(feeder.created_at);
  if (!Number.isFinite(postedAt) || !Number.isFinite(trackingStartedAt)) return false;
  return postedAt >= trackingStartedAt;
}

function buildTrackedPostState(posts: PostRow[], feeders: FeederRow[], metrics: MetricRow[]) {
  const feederById = new Map<number, FeederRow>(feeders.map((feeder) => [feeder.id, feeder]));
  const checkpointsByPost = new Map<string, Set<string>>();

  for (const metric of metrics) {
    const checkpoint = String(metric.checkpoint || '').toLowerCase();
    if (!checkpoint) continue;
    const checkpoints = checkpointsByPost.get(metric.post_key) || new Set<string>();
    checkpoints.add(checkpoint);
    checkpointsByPost.set(metric.post_key, checkpoints);
  }

  const trackedPostKeys = new Set<string>();
  for (const post of posts) {
    const feeder = feederById.get(post.feeder_id);
    if (!feeder) continue;
    if (isTrackedPost(post, feeder, checkpointsByPost)) {
      trackedPostKeys.add(post.post_key);
    }
  }

  return { feederById, checkpointsByPost, trackedPostKeys };
}

async function fetchInstagramWebProfile(handle: string): Promise<InstagramProfileProbe | null> {
  try {
    const res = await fetch(
      `https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(handle)}`,
      {
        method: 'GET',
        cache: 'no-store',
        headers: {
          'x-ig-app-id': '936619743392459',
          'user-agent': 'Mozilla/5.0',
          accept: 'application/json',
        },
      }
    );
    if (!res.ok) return null;

    const json = await res.json();
    const user = json?.data?.user;
    if (!user) return null;

    const uname = String(user.username || '').replace(/^@+/, '').toLowerCase();
    if (uname !== handle.toLowerCase()) {
      return { ok: false, profilePicUrl: null, followerCount: null, error: 'Handle verification failed' };
    }

    const followerRaw = user?.edge_followed_by?.count;
    const followerCount =
      followerRaw === null || followerRaw === undefined || followerRaw === ''
        ? null
        : Number.isFinite(Number(followerRaw))
          ? Number(followerRaw)
          : null;

    return {
      ok: true,
      profilePicUrl: (() => {
        const candidate = normalizeUrl(String(user.profile_pic_url_hd || user.profile_pic_url || ''));
        return isBadInstagramImageUrl(candidate) ? null : candidate;
      })(),
      followerCount,
    };
  } catch {
    return null;
  }
}

async function fetchInstagramPageProfile(handle: string): Promise<InstagramProfileProbe | null> {
  try {
    const res = await fetch(`https://www.instagram.com/${encodeURIComponent(handle)}/`, {
      method: 'GET',
      cache: 'no-store',
      headers: {
        'user-agent': 'Mozilla/5.0',
        accept: 'text/html,application/xhtml+xml',
      },
    });
    if (!res.ok) return null;

    const html = await res.text();
    const canonicalHandle = extractCanonicalHandle(html);
    const title = extractMetaContent(html, 'og:title');
    const description = extractMetaContent(html, 'og:description');
    if (
      canonicalHandle !== handle.toLowerCase() &&
      !String(title || '').toLowerCase().includes(`@${handle.toLowerCase()}`)
    ) {
      return { ok: false, profilePicUrl: null, followerCount: null, error: 'Handle verification failed' };
    }

    const image = normalizeUrl(extractMetaContent(html, 'og:image'));
    return {
      ok: true,
      profilePicUrl: isBadInstagramImageUrl(image) ? null : image,
      followerCount: parseFollowerCountText(description),
    };
  } catch {
    return null;
  }
}

async function probeInstagramHandleQuick(handle: string): Promise<InstagramProfileProbe> {
  const webProfile = await fetchInstagramWebProfile(handle);
  if (webProfile?.ok) return webProfile;
  const pageProfile = await fetchInstagramPageProfile(handle);
  if (pageProfile?.ok) return pageProfile;
  return pageProfile || webProfile || { ok: false, profilePicUrl: null, followerCount: null, error: 'Profile probe unavailable' };
}



function buildTickerItems(feeders: FeederRow[], posts: PostRow[], metrics: MetricRow[], trackedPostKeys: Set<string>): TickerRow[] {
  const postByKey = new Map<string, PostRow>(posts.map((post) => [post.post_key, post]));
  const postKeysByFeeder = new Map<number, string[]>();
  const latestD1MetricByPost = new Map<string, MetricRow>();

  for (const post of posts) {
    const bucket = postKeysByFeeder.get(post.feeder_id) || [];
    bucket.push(post.post_key);
    postKeysByFeeder.set(post.feeder_id, bucket);
  }

  for (const metric of metrics) {
    if (String(metric.checkpoint || '').toLowerCase() !== 'd1') continue;
    const current = latestD1MetricByPost.get(metric.post_key);
    if (!current || parseIsoTime(metric.computed_at) > parseIsoTime(current.computed_at)) {
      latestD1MetricByPost.set(metric.post_key, metric);
    }
  }

  const now = Date.now();
  const sevenDaysAgo = now - 7 * 24 * 60 * 60 * 1000;
  const windowSize = 5;
  const minSamples = windowSize * 2;

  type MetricSample = {
    value: number;
    postedAtMs: number;
    computedAtMs: number;
  };

  const computeDelta = (samples: MetricSample[]) => {
    if (samples.length < minSamples) return 0;
    const ordered = [...samples].sort((a, b) => (
      b.postedAtMs - a.postedAtMs
      || b.computedAtMs - a.computedAtMs
      || b.value - a.value
    ));
    const recentTotal = ordered.slice(0, windowSize)
      .reduce((sum, sample) => sum + sample.value, 0);
    const previousTotal = ordered.slice(windowSize, windowSize * 2)
      .reduce((sum, sample) => sum + sample.value, 0);
    return Math.round(recentTotal - previousTotal);
  };

  return feeders
    .map((feeder) => {
      const postKeys = postKeysByFeeder.get(feeder.id) || [];
      const likesSamples: MetricSample[] = [];
      const commentsSamples: MetricSample[] = [];
      const viewsSamples: MetricSample[] = [];
      let postsDelta = 0;

      for (const postKey of postKeys) {
        if (!trackedPostKeys.has(postKey)) continue;
        const post = postByKey.get(postKey);
        const metric = latestD1MetricByPost.get(postKey);
        const postedAtMs = parseIsoTime(post?.posted_at);
        if (!post || !metric || !Number.isFinite(postedAtMs)) continue;

        if (postedAtMs >= sevenDaysAgo) {
          postsDelta += 1;
        }

        const computedAtMs = Number.isFinite(parseIsoTime(metric.computed_at)) ? parseIsoTime(metric.computed_at) : 0;
        const likesValue = finiteMetricNumber(metric.likes);
        const commentsValue = finiteMetricNumber(metric.comments);
        const viewsValue = finiteMetricNumber(metric.views);

        if (likesValue != null) {
          likesSamples.push({ value: likesValue, postedAtMs, computedAtMs });
        }

        if (commentsValue != null) {
          commentsSamples.push({ value: commentsValue, postedAtMs, computedAtMs });
        }

        if (viewsValue != null && isVideoMediaType(post.media_type)) {
          viewsSamples.push({ value: viewsValue, postedAtMs, computedAtMs });
        }
      }

      const likesDelta = computeDelta(likesSamples);
      const commentsDelta = computeDelta(commentsSamples);
      const viewsDelta = computeDelta(viewsSamples);
      const liveMetricCount = [likesSamples, commentsSamples, viewsSamples]
        .filter((samples) => samples.length >= minSamples)
        .length;
      const score = liveMetricCount * 10000
        + Math.abs(likesDelta)
        + Math.abs(commentsDelta)
        + Math.abs(viewsDelta) * 0.01
        + postsDelta;

      return {
        row: {
          id: String(feeder.id),
          handle: `@${feeder.handle}`,
          likesDelta,
          commentsDelta,
          viewsDelta,
          postsDelta,
        },
        score,
      };
    })
    .sort((a, b) => b.score - a.score || a.row.handle.localeCompare(b.row.handle))
    .slice(0, 8)
    .map((entry) => entry.row);
}

async function getFeedBundle(userId: string) {
  const sb = adminClient();
  const { data, error } = await sb.rpc('fn_feed_bundle', {
    p_user_id: userId,
  });

  if (!error) {
    return attachFeedContexts(sb, userId, normalizeFeedBundlePayload(data));
  }

  const message = error?.message || '';
  const shouldFallbackToLegacy = message.toLowerCase().includes('fn_feed_bundle')
    || message.toLowerCase().includes('function public.fn_feed_bundle')
    || message.toLowerCase().includes('could not find')
    || message.toLowerCase().includes('does not exist');
  if (!shouldFallbackToLegacy) {
    throw error;
  }

  return getLegacyFeedBundle(userId);
}

async function getLegacyFeedBundle(userId: string): Promise<FeedBundlePayload> {
  const sb = adminClient();

  const { data: feedsData, error: feedsError } = await sb
    .from('feeds')
    .select('id,user_id,name,status,created_at,context_brief,context_bible')
    .eq('user_id', userId)
    .eq('status', 'active')
    .order('created_at', { ascending: false });
  if (feedsError) throw feedsError;

  const feeds = (feedsData || []) as FeedRow[];
  if (feeds.length === 0) return { feeds: [], ticker: [] };

  const feedIds = feeds.map((f) => f.id);

  const { data: feederData, error: feederError } = await sb
    .from('feeders')
    .select('id,feed_id,handle,role,status,created_at,profile_pic_url,follower_count')
    .in('feed_id', feedIds)
    .eq('status', 'active');
  if (feederError) throw feederError;

  const feeders = (feederData || []) as FeederRow[];
  const feederIds = feeders.map((f) => f.id);

  let posts: PostRow[] = [];
  let metrics: MetricRow[] = [];

  if (feederIds.length > 0) {
    const { data: postsData, error: postsError } = await sb
      .from('posts')
      .select('post_key,feeder_id,posted_at,media_type')
      .in('feeder_id', feederIds);
    if (postsError) throw postsError;
    posts = (postsData || []) as PostRow[];

    const postKeys = posts.map((p) => p.post_key);
    if (postKeys.length > 0) {
      const { data: metricsData, error: metricsError } = await sb
        .from('post_metrics')
        .select('post_key,checkpoint,likes,comments,views,computed_at')
        .in('post_key', postKeys)
        .order('computed_at', { ascending: false });
      if (metricsError) throw metricsError;
      metrics = (metricsData || []) as MetricRow[];
    }
  }

  const latestMetricByPost = new Map<string, MetricRow>();
  for (const metric of metrics) {
    if (!latestMetricByPost.has(metric.post_key)) {
      latestMetricByPost.set(metric.post_key, metric);
    }
  }

  const { checkpointsByPost, trackedPostKeys } = buildTrackedPostState(posts, feeders, metrics);
  const postByKey = new Map<string, PostRow>(posts.map((post) => [post.post_key, post]));

  const postKeysByFeeder = new Map<number, string[]>();
  for (const p of posts) {
    const arr = postKeysByFeeder.get(p.feeder_id) || [];
    arr.push(p.post_key);
    postKeysByFeeder.set(p.feeder_id, arr);
  }

  const feedersByFeed = new Map<number, FeederRow[]>();
  for (const f of feeders) {
    const arr = feedersByFeed.get(f.feed_id) || [];
    arr.push(f);
    feedersByFeed.set(f.feed_id, arr);
  }
  const feederMetrics = new Map<number, { likes: number; comments: number; views: number; postsTracked: number }>();
  for (const feeder of feeders) {
    const keys = postKeysByFeeder.get(feeder.id) || [];
    let likes = 0;
    let comments = 0;
    let views = 0;
    let postsTracked = 0;

    for (const key of keys) {
      const post = postByKey.get(key);
      if (!post || !isTrackedPost(post, feeder, checkpointsByPost)) continue;
      const m = latestMetricByPost.get(key);
      if (!m) continue;
      likes += m.likes || 0;
      comments += m.comments || 0;
      views += m.views || 0;
      postsTracked += 1;
    }

    feederMetrics.set(feeder.id, { likes, comments, views, postsTracked });
  }

  const mappedFeeds = feeds.map((feed) => {
    const feedFeeders = feedersByFeed.get(feed.id) || [];

    let totalLikes = 0;
    let totalComments = 0;
    let totalViews = 0;
    let totalPosts = 0;

    const mappedFeeders = feedFeeders.map((feeder) => {
      const stats = feederMetrics.get(feeder.id) || { likes: 0, comments: 0, views: 0, postsTracked: 0 };
      totalLikes += stats.likes;
      totalComments += stats.comments;
      totalViews += stats.views;
      totalPosts += stats.postsTracked;

      return {
        handle: feeder.handle,
        isAnchor: feeder.role === 'anchor',
        profilePicUrl: feeder.profile_pic_url && !feeder.profile_pic_url.includes('unavatar.io/instagram')
          ? buildProfileImageProxyUrl(feeder.profile_pic_url)
          : null,
        followerCount: feeder.follower_count,
        metrics: {
          likes: toMetricString(stats.likes),
          comments: toMetricString(stats.comments),
          views: toMetricString(stats.views),
          postsTracked: toMetricString(stats.postsTracked),
        },
      };
    });

    return {
      id: String(feed.id),
      title: feed.name.toUpperCase(),
      feedBrief: feed.context_brief && typeof feed.context_brief === 'object' && !Array.isArray(feed.context_brief)
        ? feed.context_brief
        : {},
      feedBriefText: String(feed.context_bible || ''),
      contextBrief: feed.context_brief && typeof feed.context_brief === 'object' && !Array.isArray(feed.context_brief)
        ? feed.context_brief
        : {},
      contextBible: String(feed.context_bible || ''),
      feeders: mappedFeeders,
      metrics: {
        likes: toMetricString(totalLikes),
        comments: toMetricString(totalComments),
        views: toMetricString(totalViews),
        postsTracked: toMetricString(totalPosts),
      },
    };
  });

  return {
    feeds: mappedFeeds,
    ticker: buildTickerItems(feeders, posts, metrics, trackedPostKeys),
  };
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

    const bundle = await withServerRouteCache(
      feedBundleCacheKey(user.id),
      FEED_BUNDLE_TTL_MS,
      () => getFeedBundle(user.id),
    );
    return privateJsonResponse(request, bundle, {
      maxAgeSeconds: 30,
      staleWhileRevalidateSeconds: 300,
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to load feeds';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function POST(request: NextRequest) {
  try {
    const supabase = await createClient();
    const {
      data: { user },
      error: authError,
    } = await supabase.auth.getUser();

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const sb = adminClient();
    const body = await request.json();
    const action = String(body?.action || '');

    if (action === 'create_feed') {
      const title = String(body?.title || '').trim();
      if (!title) {
        return NextResponse.json({ error: 'Feed name is required' }, { status: 400 });
      }
      const contextBrief = briefFromBody(body);
      const contextBible = bibleFromBody(body) || buildFeedBriefTextFromBrief(contextBrief, title);

      const { error } = await sb.from('feeds').insert({
        user_id: user.id,
        name: title,
        status: 'active',
        context_brief: contextBrief,
        context_bible: contextBible,
      });
      if (error) throw error;

      invalidateUserDerivedCaches(user.id);
      const bundle = await getFeedBundle(user.id);
      writeServerRouteCache(feedBundleCacheKey(user.id), FEED_BUNDLE_TTL_MS, bundle);
      return NextResponse.json(bundle);
    }

    if (action === 'update_feed_context') {
      const feedId = Number(body?.feedId);
      if (!feedId) {
        return NextResponse.json({ error: 'feedId is required' }, { status: 400 });
      }
      const contextBrief = briefFromBody(body);
      const contextBible = bibleFromBody(body) || buildFeedBriefTextFromBrief(contextBrief, String(body?.title || 'this feed'));
      const { error } = await sb
        .from('feeds')
        .update({ context_brief: contextBrief, context_bible: contextBible, updated_at: new Date().toISOString() })
        .eq('id', feedId)
        .eq('user_id', user.id);
      if (error) throw error;

      invalidateUserDerivedCaches(user.id);
      const bundle = await getFeedBundle(user.id);
      writeServerRouteCache(feedBundleCacheKey(user.id), FEED_BUNDLE_TTL_MS, bundle);
      return NextResponse.json(bundle);
    }

    if (action === 'add_feeder') {
      const feedId = Number(body?.feedId);
      const handle = normalizeHandle(String(body?.handle || ''));

      if (!feedId || !handle) {
        return NextResponse.json({ error: 'feedId and handle are required' }, { status: 400 });
      }

      const { data: feedRow, error: feedErr } = await sb
        .from('feeds')
        .select('id,user_id')
        .eq('id', feedId)
        .eq('user_id', user.id)
        .single();
      if (feedErr || !feedRow) {
        return NextResponse.json({ error: 'Feed not found' }, { status: 404 });
      }

      const { data: existing, error: existingErr } = await sb
        .from('feeders')
        .select('id')
        .eq('feed_id', feedId)
        .eq('status', 'active');
      if (existingErr) throw existingErr;
      if ((existing || []).length >= 15) {
        return NextResponse.json({ error: 'A feed can have max 15 feeders' }, { status: 400 });
      }

      const probe = await probeInstagramHandleQuick(handle).catch(() => ({ ok: false, profilePicUrl: null, followerCount: null, error: 'Profile probe failed' }));

      const feederRow: Record<string, unknown> = {
        feed_id: feedId,
        handle,
        role: 'standard',
        status: 'active',
        context_role: String(body?.contextRole || 'standard').trim() || 'standard',
        context_note: String(body?.contextNote || '').trim(),
      };

      if (probe.ok) {
        feederRow.profile_pic_url = probe.profilePicUrl;
        feederRow.follower_count = probe.followerCount;
      }

      const { data: upsertedFeeder, error: upsertErr } = await sb
        .from('feeders')
        .upsert(feederRow, { onConflict: 'feed_id,handle' })
        .select('id');
      if (upsertErr) throw upsertErr;

      const feederId = Number(upsertedFeeder?.[0]?.id || 0);
      if (feederId > 0) {
        const nowIso = new Date().toISOString();
        const { error: bootstrapErr } = await sb.rpc('bootstrap_feeder_jobs', {
          p_feeder_id: feederId,
          p_run_at: nowIso,
        });

        // The canonical RPC assigns the current AM/PM discovery slot.
        if (bootstrapErr) {
          const { error: enqueueErr } = await sb.rpc('enqueue_daily_job_for_feeder', {
            p_feeder_id: feederId,
            p_run_at: nowIso,
          });
          if (enqueueErr) throw enqueueErr;
        }
      }

      invalidateUserDerivedCaches(user.id);
      const bundle = await getFeedBundle(user.id);
      writeServerRouteCache(feedBundleCacheKey(user.id), FEED_BUNDLE_TTL_MS, bundle);
      return NextResponse.json(bundle);
    }

    if (action === 'make_anchor') {
      const feedId = Number(body?.feedId);
      const handle = normalizeHandle(String(body?.handle || ''));
      if (!feedId || !handle) {
        return NextResponse.json({ error: 'feedId and handle are required' }, { status: 400 });
      }

      const { data: feedRow, error: feedErr } = await sb
        .from('feeds')
        .select('id,user_id')
        .eq('id', feedId)
        .eq('user_id', user.id)
        .single();
      if (feedErr || !feedRow) return NextResponse.json({ error: 'Feed not found' }, { status: 404 });

      const { error: clearErr } = await sb
        .from('feeders')
        .update({ role: 'standard' })
        .eq('feed_id', feedId)
        .eq('status', 'active');
      if (clearErr) throw clearErr;

      const { error: anchorErr } = await sb
        .from('feeders')
        .update({ role: 'anchor' })
        .eq('feed_id', feedId)
        .eq('handle', handle)
        .eq('status', 'active');
      if (anchorErr) throw anchorErr;

      invalidateUserDerivedCaches(user.id);
      const bundle = await getFeedBundle(user.id);
      writeServerRouteCache(feedBundleCacheKey(user.id), FEED_BUNDLE_TTL_MS, bundle);
      return NextResponse.json(bundle);
    }

    if (action === 'remove_feeder') {
      const feedId = Number(body?.feedId);
      const handle = normalizeHandle(String(body?.handle || ''));
      if (!feedId || !handle) {
        return NextResponse.json({ error: 'feedId and handle are required' }, { status: 400 });
      }

      const { data: feedRow, error: feedErr } = await sb
        .from('feeds')
        .select('id,user_id')
        .eq('id', feedId)
        .eq('user_id', user.id)
        .single();
      if (feedErr || !feedRow) return NextResponse.json({ error: 'Feed not found' }, { status: 404 });

      const { data: feederRows, error: feederLookupErr } = await sb
        .from('feeders')
        .select('id')
        .eq('feed_id', feedId)
        .eq('handle', handle);
      if (feederLookupErr) throw feederLookupErr;

      const feederIds = ((feederRows || []) as Array<{ id: number | string | null }>)
        .map((row) => Number(row.id))
        .filter((id) => Number.isFinite(id) && id > 0);
      if (feederIds.length === 0) {
        return NextResponse.json({ error: 'Feeder not found' }, { status: 404 });
      }

      await purgeStoredMediaForFeederIds(sb, feederIds);

      const { error } = await sb
        .from('feeders')
        .delete()
        .in('id', feederIds);
      if (error) throw error;

      invalidateUserDerivedCaches(user.id);
      const bundle = await getFeedBundle(user.id);
      writeServerRouteCache(feedBundleCacheKey(user.id), FEED_BUNDLE_TTL_MS, bundle);
      return NextResponse.json(bundle);
    }

    if (action === 'delete_feed') {
      const feedId = Number(body?.feedId);
      if (!feedId) {
        return NextResponse.json({ error: 'feedId is required' }, { status: 400 });
      }

      const { data: feedRow, error: feedErr } = await sb
        .from('feeds')
        .select('id,user_id')
        .eq('id', feedId)
        .eq('user_id', user.id)
        .single();
      if (feedErr || !feedRow) {
        return NextResponse.json({ error: 'Feed not found' }, { status: 404 });
      }

      const { data: feederRows, error: feederErr } = await sb
        .from('feeders')
        .select('id')
        .eq('feed_id', feedId);
      if (feederErr) throw feederErr;

      const feederIds = ((feederRows || []) as Array<{ id: number | string | null }>)
        .map((row) => Number(row.id))
        .filter((id) => Number.isFinite(id) && id > 0);
      await purgeStoredMediaForFeederIds(sb, feederIds);

      const { error: deleteErr } = await sb
        .from('feeds')
        .delete()
        .eq('id', feedId)
        .eq('user_id', user.id);
      if (deleteErr) throw deleteErr;

      invalidateUserDerivedCaches(user.id);
      const bundle = await getFeedBundle(user.id);
      writeServerRouteCache(feedBundleCacheKey(user.id), FEED_BUNDLE_TTL_MS, bundle);
      return NextResponse.json(bundle);
    }

    return NextResponse.json({ error: 'Invalid action' }, { status: 400 });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to update feed';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
