import { NextRequest } from 'next/server';
import { createHash, createHmac } from 'crypto';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient as createServerSupabaseClient } from '@/lib/supabase/server';
import { instagramLinkLive } from '@/lib/server/instagramLink';

export const runtime = 'nodejs';

const ALLOWED_HOST_SUFFIXES = [
  'cdninstagram.com',
  'fbcdn.net',
  'instagram.com',
  'cdninstagram.net',
];
const REMOTE_FETCH_TIMEOUT_MS = 8_000;
const MAX_IMAGE_BYTES = 5 * 1024 * 1024;
const MAX_VIDEO_BYTES = 35 * 1024 * 1024;
const PRIVATE_MEDIA_CACHE_CONTROL = 'private, max-age=3600, stale-while-revalidate=86400';
const R2_SIGNED_URL_DEFAULT_EXPIRES_SECONDS = 600;
const R2_SIGNED_URL_MIN_EXPIRES_SECONDS = 60;
const R2_SIGNED_URL_MAX_EXPIRES_SECONDS = 3600;
// pictures sign from the start of the UTC day for two days (SigV4 allows 7), so the URL is one string all day
const R2_SIGNED_URL_DAY_SECONDS = 86_400;
const R2_SIGNED_URL_DAY_EXPIRES_SECONDS = 2 * R2_SIGNED_URL_DAY_SECONDS;
const R2_SIGNED_REDIRECT_DAY_MARGIN_SECONDS = 300;
const THUMBNAIL_ASSET_ROLES = [
  'thumbnail',
  'display',
  'carousel_0',
  'carousel_00',
  'carousel_01',
  'carousel_1',
];

type SupabaseAdminClient = ReturnType<typeof adminClient>;
type FeedIdRow = { id: number | string | null };
type FeederIdRow = { id: number | string | null };
type PostKeyRow = { post_key: string | null };
// freshSeconds: how long a day-bucketed URL stays the same string (null when it is signed per request)
type SignedR2Url = { url: string; expiresSeconds: number; freshSeconds: number | null };

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

function isAllowedHost(hostname: string): boolean {
  const h = (hostname || '').toLowerCase();
  return ALLOWED_HOST_SUFFIXES.some((suffix) => h === suffix || h.endsWith(`.${suffix}`));
}

function isVideoRole(assetRole: string): boolean {
  const role = (assetRole || '').trim().toLowerCase();
  return role === 'preview_5s' || role === 'video';
}

// a carousel slide (carousel_01 …): a picture or a video, whichever the slide is
function slideIndexOf(assetRole: string): number | null {
  const match = /^carousel_(\d+)$/.exec((assetRole || '').trim().toLowerCase());
  return match ? Number(match[1]) : null;
}

function expectedContentPrefix(assetRole: string): string {
  const role = (assetRole || 'thumbnail').trim().toLowerCase();
  if (slideIndexOf(role) != null) return '';
  if (isVideoRole(role)) return 'video/';
  return 'image/';
}

function maxBytesForRole(assetRole: string): number {
  return isVideoRole(assetRole) || slideIndexOf(assetRole) != null ? MAX_VIDEO_BYTES : MAX_IMAGE_BYTES;
}

function candidatePostKeys(postKey: string): string[] {
  const trimmed = (postKey || '').trim();
  const withoutHash = trimmed.split('#')[0] || '';
  const lower = trimmed.toLowerCase();
  const lowerWithoutHash = lower.split('#')[0] || '';
  return Array.from(new Set([trimmed, withoutHash, lower, lowerWithoutHash].filter(Boolean)));
}

function isCommandAdmin(email: string | null | undefined): boolean {
  if (!email) return false;
  const allowlist = (process.env.COMMAND_HUB_ADMIN_EMAILS || '')
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);
  return allowlist.includes(email.trim().toLowerCase());
}

function privateMediaHeaders(extra?: HeadersInit): Headers {
  const headers = new Headers(extra);
  if (!headers.has('cache-control')) {
    headers.set('cache-control', PRIVATE_MEDIA_CACHE_CONTROL);
  }
  headers.set('vary', 'Cookie');
  return headers;
}

// an Instagram link past its expiry: the CDN would only answer 403, so it isn't asked (the page draws its cover)
function expiredSourceResponse(): Response {
  return new Response('source expired', {
    status: 404,
    headers: privateMediaHeaders({ 'cache-control': 'private, max-age=300' }),
  });
}

// a day-bucketed URL is kept until 5 min before UTC midnight (its target stays valid a day past that)
function signedRedirectCacheControl(signed: SignedR2Url): string {
  const maxAge = signed.freshSeconds != null
    ? Math.max(0, signed.freshSeconds - R2_SIGNED_REDIRECT_DAY_MARGIN_SECONDS)
    : Math.max(0, Math.min(120, signed.expiresSeconds - 30));
  return `private, max-age=${maxAge}`;
}

function numericIds(rows: Array<{ id: number | string | null }>): number[] {
  return rows
    .map((row) => Number(row.id))
    .filter((id) => Number.isFinite(id) && id > 0);
}

async function fetchUserFeedIds(sb: SupabaseAdminClient, userId: string): Promise<number[]> {
  const { data, error } = await sb
    .from('feeds')
    .select('id')
    .eq('user_id', userId)
    .eq('status', 'active');

  if (error) throw error;
  return numericIds((data || []) as FeedIdRow[]);
}

async function fetchUserFeederIds(sb: SupabaseAdminClient, feedIds: number[]): Promise<number[]> {
  if (feedIds.length === 0) return [];
  const { data, error } = await sb
    .from('feeders')
    .select('id')
    .in('feed_id', feedIds)
    .eq('status', 'active');

  if (error) throw error;
  return numericIds((data || []) as FeederIdRow[]);
}

async function authorizedPostKeyCandidates(
  sb: SupabaseAdminClient,
  feedIds: number[],
  postKey: string,
): Promise<string[]> {
  const candidates = candidatePostKeys(postKey);
  if (candidates.length === 0 || feedIds.length === 0) return [];

  const feederIds = await fetchUserFeederIds(sb, feedIds);
  if (feederIds.length === 0) return [];

  const { data, error } = await sb
    .from('posts')
    .select('post_key')
    .in('post_key', candidates)
    .in('feeder_id', feederIds)
    .limit(candidates.length);

  if (error) throw error;
  const owned = new Set(((data || []) as PostKeyRow[]).map((row) => row.post_key).filter(Boolean));
  return candidates.filter((candidate) => owned.has(candidate));
}

async function profileImageUrlBelongsToUser(
  sb: SupabaseAdminClient,
  feedIds: number[],
  rawUrl: string,
): Promise<boolean> {
  if (!rawUrl || feedIds.length === 0) return false;
  const { data, error } = await sb
    .from('feeders')
    .select('id')
    .in('feed_id', feedIds)
    .eq('status', 'active')
    .eq('profile_pic_url', rawUrl)
    .limit(1);

  if (error) throw error;
  return Boolean(data?.length);
}

async function fetchStoredAsset(
  sb: SupabaseAdminClient,
  postKeyCandidates: string[],
  assetRole: string,
): Promise<Response | null> {
  const role = (assetRole || 'thumbnail').trim().toLowerCase();
  const candidateRoles = role === 'thumbnail' ? THUMBNAIL_ASSET_ROLES : [role];
  const anyMedia = slideIndexOf(role) != null;
  const needsVideoResponse = !anyMedia && isVideoRole(role);
  const needsImageResponse = !anyMedia && !needsVideoResponse;

  // every candidate key and role in one read, then taken in the same preference order (key first, then role,
  // newest first) the lookups used to walk one query at a time
  const { data, error } = await sb
    .from('post_media_assets')
    .select('post_key,asset_role,storage_provider,storage_bucket,storage_path,mime_type,status,purge_after,source_url,updated_at')
    .in('post_key', postKeyCandidates)
    .in('asset_role', candidateRoles)
    .in('status', ['active', 'purge_pending', 'pending_capture'])
    .order('updated_at', { ascending: false })
    .limit(64);
  if (error || !data?.length) return null;

  const rank = (row: { post_key?: string | null; asset_role?: string | null }) =>
    postKeyCandidates.indexOf(row.post_key || '') * candidateRoles.length + candidateRoles.indexOf(row.asset_role || '');
  const rows = data.slice().sort((left, right) => rank(left) - rank(right));
  for (const row of rows) {
    if (row.purge_after && new Date(row.purge_after).getTime() <= Date.now()) {
      continue;
    }
    if (row.storage_provider !== 'r2') continue;
    const contentType = typeof row.mime_type === 'string' ? row.mime_type.toLowerCase() : '';
    if (
      (!needsImageResponse || !contentType || contentType.startsWith('image/'))
      && (!needsVideoResponse || !contentType || contentType.startsWith('video/'))
    ) {
      // pictures get the day-bucketed URL the browser can keep; videos stay on the short per-request one
      const picture = needsImageResponse || contentType.startsWith('image/');
      const signedUrl = resolveSignedR2MediaUrl(row, picture);
      if (!signedUrl) continue;
      return new Response(null, {
        status: 302,
        headers: privateMediaHeaders({
          location: signedUrl.url,
          'cache-control': signedRedirectCacheControl(signedUrl),
        }),
      });
    }
  }

  return null;
}

type PostSourceRow = {
  thumbnail_url: string | null;
  video_url: string | null;
  carousel_urls: string[] | null;
};

async function fetchPostSourceUrl(
  sb: SupabaseAdminClient,
  postKeyCandidates: string[],
  assetRole: string,
): Promise<string | null> {
  const role = (assetRole || 'thumbnail').trim().toLowerCase();
  for (const candidatePostKey of postKeyCandidates) {
    const { data, error } = await sb
      .from('posts')
      .select('thumbnail_url,video_url,carousel_urls')
      .eq('post_key', candidatePostKey)
      .maybeSingle();

    if (error || !data) continue;

    const row = data as PostSourceRow;
    const slide = slideIndexOf(role);
    if (slide != null) {
      const source = Array.isArray(row.carousel_urls) ? row.carousel_urls[slide - 1] : null;
      if (typeof source === 'string' && source.trim()) return source.trim();
      continue;
    }
    if (isVideoRole(role)) {
      const videoUrl = typeof row.video_url === 'string' && row.video_url.trim() ? row.video_url.trim() : null;
      if (videoUrl) return videoUrl;
      continue;
    }

    if (typeof row.thumbnail_url === 'string' && row.thumbnail_url.trim()) {
      return row.thumbnail_url.trim();
    }

    if (Array.isArray(row.carousel_urls)) {
      const firstSlide = row.carousel_urls.find((value) => typeof value === 'string' && value.trim());
      if (firstSlide?.trim()) return firstSlide.trim();
    }
  }

  return null;
}

function boundedSignedUrlExpiresSeconds(): number {
  const configured = Number(process.env.R2_SIGNED_URL_EXPIRES_SECONDS || R2_SIGNED_URL_DEFAULT_EXPIRES_SECONDS);
  if (!Number.isFinite(configured)) return R2_SIGNED_URL_DEFAULT_EXPIRES_SECONDS;
  return Math.max(
    R2_SIGNED_URL_MIN_EXPIRES_SECONDS,
    Math.min(R2_SIGNED_URL_MAX_EXPIRES_SECONDS, Math.trunc(configured)),
  );
}

function hmac(key: string | Buffer, value: string): Buffer {
  return createHmac('sha256', key).update(value).digest();
}

function hmacHex(key: string | Buffer, value: string): string {
  return createHmac('sha256', key).update(value).digest('hex');
}

function sha256Hex(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

// the start of the current UTC day and the whole seconds left in it
function utcDayBucket(nowMs = Date.now()): { start: Date; secondsLeft: number } {
  const dayMs = R2_SIGNED_URL_DAY_SECONDS * 1000;
  const startMs = Math.floor(nowMs / dayMs) * dayMs;
  return { start: new Date(startMs), secondsLeft: Math.floor((startMs + dayMs - nowMs) / 1000) };
}

function amzDates(now = new Date()): { shortDate: string; longDate: string } {
  const iso = now.toISOString().replace(/[:-]|\.\d{3}/g, '');
  return {
    shortDate: iso.slice(0, 8),
    longDate: iso,
  };
}

function encodeRfc3986(value: string): string {
  return encodeURIComponent(value).replace(/[!'()*]/g, (char) =>
    `%${char.charCodeAt(0).toString(16).toUpperCase()}`,
  );
}

function encodePath(value: string): string {
  return value.split('/').map(encodeRfc3986).join('/');
}

function canonicalQuery(params: Array<[string, string]>): string {
  return params
    .map(([key, value]) => [encodeRfc3986(key), encodeRfc3986(value)] as const)
    .sort(([leftKey, leftValue], [rightKey, rightValue]) => {
      if (leftKey === rightKey) return leftValue.localeCompare(rightValue);
      return leftKey.localeCompare(rightKey);
    })
    .map(([key, value]) => `${key}=${value}`)
    .join('&');
}

// the derived key only changes with the date (pure HMAC work), so the last one is kept
let lastSigningKey: { id: string; key: Buffer } | null = null;

function r2SigningKey(secretAccessKey: string, shortDate: string, region: string): Buffer {
  const id = `${secretAccessKey}\n${shortDate}\n${region}`;
  if (lastSigningKey?.id === id) return lastSigningKey.key;
  const dateKey = hmac(`AWS4${secretAccessKey}`, shortDate);
  const regionKey = hmac(dateKey, region);
  const serviceKey = hmac(regionKey, 's3');
  const key = hmac(serviceKey, 'aws4_request');
  lastSigningKey = { id, key };
  return key;
}

function signedR2GetUrl(bucket: string, storagePath: string, dayBucket = false): SignedR2Url | null {
  const endpointRaw = (process.env.R2_ENDPOINT_URL || '').trim();
  const accessKeyId = (process.env.R2_ACCESS_KEY_ID || '').trim();
  const secretAccessKey = (process.env.R2_SECRET_ACCESS_KEY || '').trim();
  const region = (process.env.R2_REGION || 'auto').trim() || 'auto';
  const cleanBucket = bucket.trim();
  const cleanPath = storagePath.trim().replace(/^\/+/, '');

  if (!endpointRaw || !accessKeyId || !secretAccessKey || !cleanBucket || !cleanPath) {
    return null;
  }

  let endpoint: URL;
  try {
    endpoint = new URL(endpointRaw);
  } catch {
    return null;
  }

  if (!['http:', 'https:'].includes(endpoint.protocol)) {
    return null;
  }

  // a day-bucketed URL signs at 00:00Z, so every request that day gets the same URL (the browser's cache key)
  const day = dayBucket ? utcDayBucket() : null;
  const expiresSeconds = day ? R2_SIGNED_URL_DAY_EXPIRES_SECONDS : boundedSignedUrlExpiresSeconds();
  const { shortDate, longDate } = amzDates(day?.start);
  const credentialScope = `${shortDate}/${region}/s3/aws4_request`;
  const basePath = endpoint.pathname.replace(/\/+$/, '');
  const canonicalUri = `${basePath}/${encodePath(cleanBucket)}/${encodePath(cleanPath)}`;
  const params: Array<[string, string]> = [
    ['X-Amz-Algorithm', 'AWS4-HMAC-SHA256'],
    ['X-Amz-Credential', `${accessKeyId}/${credentialScope}`],
    ['X-Amz-Date', longDate],
    ['X-Amz-Expires', String(expiresSeconds)],
    ['X-Amz-SignedHeaders', 'host'],
  ];
  const query = canonicalQuery(params);
  const canonicalRequest = [
    'GET',
    canonicalUri,
    query,
    `host:${endpoint.host}`,
    '',
    'host',
    'UNSIGNED-PAYLOAD',
  ].join('\n');
  const stringToSign = [
    'AWS4-HMAC-SHA256',
    longDate,
    credentialScope,
    sha256Hex(canonicalRequest),
  ].join('\n');
  const signature = hmacHex(r2SigningKey(secretAccessKey, shortDate, region), stringToSign);

  return {
    url: `${endpoint.protocol}//${endpoint.host}${canonicalUri}?${query}&X-Amz-Signature=${signature}`,
    expiresSeconds,
    freshSeconds: day ? day.secondsLeft : null,
  };
}

function resolveSignedR2MediaUrl(data: {
  storage_provider?: string | null;
  storage_bucket?: string | null;
  storage_path?: string | null;
}, dayBucket = false): SignedR2Url | null {
  if (data.storage_provider !== 'r2') return null;

  const bucket = typeof data.storage_bucket === 'string' && data.storage_bucket.trim()
    ? data.storage_bucket.trim()
    : (process.env.R2_BUCKET || '').trim();
  const path = typeof data.storage_path === 'string' ? data.storage_path.trim().replace(/^\/+/, '') : '';
  if (!bucket || !path) return null;
  return signedR2GetUrl(bucket, path, dayBucket);
}

function limitedBodyStream(
  body: ReadableStream<Uint8Array>,
  maxBytes: number,
  onTooLarge: () => void,
  onDone: () => void,
): ReadableStream<Uint8Array> {
  let reader: ReadableStreamDefaultReader<Uint8Array> | null = null;
  let done = false;
  const finish = () => {
    if (done) return;
    done = true;
    onDone();
  };

  return new ReadableStream<Uint8Array>({
    async start(controller) {
      reader = body.getReader();
      let received = 0;
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          received += value.byteLength;
          if (received > maxBytes) {
            onTooLarge();
            await reader.cancel('remote asset too large');
            controller.error(new Error('remote asset too large'));
            finish();
            return;
          }
          controller.enqueue(value);
        }
        controller.close();
      } catch (error) {
        controller.error(error);
      } finally {
        finish();
      }
    },
    cancel(reason) {
      finish();
      return reader?.cancel(reason);
    },
  });
}

// the byte range a video's player asks for, when the role can be one (Safari asks for a range before it plays
// anything, and won't play a source that answers with the whole file)
function videoRange(assetRole: string, range: string | null): string | null {
  if (!isVideoRole(assetRole) && slideIndexOf(assetRole) == null) return null;
  const value = (range || '').trim();
  return /^bytes=(\d+-\d*|-\d+)$/.test(value) ? value : null;
}

async function fetchRemoteAsset(raw: string, assetRole: string, range: string | null = null): Promise<Response> {
  let target: URL;
  try {
    target = new URL(raw);
  } catch {
    return new Response('invalid url', { status: 400 });
  }

  if (!['http:', 'https:'].includes(target.protocol) || !isAllowedHost(target.hostname)) {
    return new Response('forbidden', { status: 403 });
  }

  const role = (assetRole || 'thumbnail').trim().toLowerCase();
  const expectedPrefix = expectedContentPrefix(role);
  const maxBytes = maxBytesForRole(role);
  const byteRange = videoRange(role, range);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REMOTE_FETCH_TIMEOUT_MS);
  let streamOwnsTimeout = false;

  try {
    const upstream = await fetch(target.toString(), {
      headers: {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari',
        'accept': '*/*',
        'referer': 'https://www.instagram.com/',
        ...(byteRange ? { range: byteRange } : {}),
      },
      cache: 'no-store',
      redirect: 'follow',
      signal: controller.signal,
    });

    if (!upstream.ok) {
      return new Response(`upstream ${upstream.status}`, { status: 502 });
    }

    const contentType = upstream.headers.get('content-type') || 'image/jpeg';
    if (expectedPrefix && !contentType.toLowerCase().startsWith(expectedPrefix)) {
      return new Response(`${role} source content type mismatch`, { status: 502 });
    }

    const contentLength = Number(upstream.headers.get('content-length') || 0);
    if (Number.isFinite(contentLength) && contentLength > maxBytes) {
      return new Response('remote asset too large', { status: 413 });
    }

    // a range answered in part goes back as the part, with where it sits in the whole file
    const partial = Boolean(byteRange) && upstream.status === 206 && Boolean(upstream.headers.get('content-range'));
    const passed: Record<string, string> = { 'content-type': contentType };
    if (partial) {
      passed['content-range'] = upstream.headers.get('content-range') as string;
      if (contentLength > 0 && !upstream.headers.get('content-encoding')) passed['content-length'] = String(contentLength);
    }
    if (byteRange && (upstream.headers.get('accept-ranges') || '').toLowerCase() === 'bytes') passed['accept-ranges'] = 'bytes';
    const status = partial ? 206 : 200;

    if (!upstream.body) {
      const body = await upstream.arrayBuffer();
      if (body.byteLength > maxBytes) {
        return new Response('remote asset too large', { status: 413 });
      }
      return new Response(body, {
        status,
        headers: privateMediaHeaders(passed),
      });
    }

    streamOwnsTimeout = true;
    const limitedStream = limitedBodyStream(
      upstream.body,
      maxBytes,
      () => controller.abort(),
      () => clearTimeout(timeout),
    );
    return new Response(limitedStream, {
      status,
      headers: privateMediaHeaders(passed),
    });
  } catch {
    return new Response('fetch failed', { status: 502 });
  } finally {
    if (!streamOwnsTimeout) {
      clearTimeout(timeout);
    }
  }
}

async function fetchRemoteAssetForRole(raw: string, assetRole: string, range: string | null = null): Promise<Response> {
  return fetchRemoteAsset(raw, assetRole, range);
}

export async function GET(req: NextRequest) {
  const authClient = await createServerSupabaseClient();
  const { data: authData, error: authError } = await authClient.auth.getUser();
  if (authError || !authData?.user) {
    return new Response('unauthorized', {
      status: 401,
      headers: { 'cache-control': 'no-store' },
    });
  }

  const sb = adminClient();
  const hasCommandAdminAccess = isCommandAdmin(authData.user.email);
  const feedIds = hasCommandAdminAccess ? [] : await fetchUserFeedIds(sb, authData.user.id);
  const postKey = (req.nextUrl.searchParams.get('postKey') || '').trim();
  const assetRole = (req.nextUrl.searchParams.get('role') || 'thumbnail').trim().toLowerCase();
  const raw = req.nextUrl.searchParams.get('url') || '';

  if (postKey) {
    try {
      const postKeyCandidates = hasCommandAdminAccess
        ? candidatePostKeys(postKey)
        : await authorizedPostKeyCandidates(sb, feedIds, postKey);
      if (postKeyCandidates.length === 0) {
        return new Response('media unavailable', { status: 404 });
      }

      const stored = await fetchStoredAsset(sb, postKeyCandidates, assetRole || 'thumbnail');
      if (stored) {
        return stored;
      }
      const sourceUrl = await fetchPostSourceUrl(sb, postKeyCandidates, assetRole || 'thumbnail');
      if (sourceUrl && !instagramLinkLive(sourceUrl)) {
        return expiredSourceResponse();
      }
      if (sourceUrl) {
        return fetchRemoteAssetForRole(sourceUrl, assetRole || 'thumbnail', req.headers.get('range'));
      }
    } catch {
      // fall through to unavailable response below
    }
    return new Response('media unavailable', { status: 404 });
  }

  if (assetRole === 'preview_5s') {
    return new Response('preview unavailable', { status: 404 });
  }

  if (!raw) {
    return new Response('missing url', { status: 400 });
  }

  if (!instagramLinkLive(raw)) {
    return expiredSourceResponse();
  }

  if (!await profileImageUrlBelongsToUser(sb, feedIds, raw)) {
    return new Response('media unavailable', { status: 404 });
  }

  return fetchRemoteAssetForRole(raw, assetRole || 'thumbnail');
}
