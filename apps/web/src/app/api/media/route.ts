import { NextRequest } from 'next/server';
import { createHash, createHmac } from 'crypto';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient as createServerSupabaseClient } from '@/lib/supabase/server';

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

function expectedContentPrefix(assetRole: string): string {
  const role = (assetRole || 'thumbnail').trim().toLowerCase();
  if (isVideoRole(role)) return 'video/';
  return 'image/';
}

function maxBytesForRole(assetRole: string): number {
  return isVideoRole(assetRole) ? MAX_VIDEO_BYTES : MAX_IMAGE_BYTES;
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

function signedRedirectCacheControl(expiresSeconds: number): string {
  const maxAge = Math.max(0, Math.min(120, expiresSeconds - 30));
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
  const needsVideoResponse = isVideoRole(role);
  const needsImageResponse = !needsVideoResponse;

  for (const candidatePostKey of postKeyCandidates) {
    for (const candidateRole of candidateRoles) {
      const { data, error } = await sb
        .from('post_media_assets')
        .select('storage_provider,storage_bucket,storage_path,mime_type,status,purge_after,source_url,updated_at')
        .eq('post_key', candidatePostKey)
        .eq('asset_role', candidateRole)
        .in('status', ['active', 'purge_pending', 'pending_capture'])
        .order('updated_at', { ascending: false })
        .limit(8);

      if (error || !data?.length) {
        continue;
      }
      for (const row of data) {
        if (row.purge_after && new Date(row.purge_after).getTime() <= Date.now()) {
          continue;
        }
        if (row.storage_provider === 'r2') {
          const contentType = typeof row.mime_type === 'string' ? row.mime_type.toLowerCase() : '';
          if (
            (!needsImageResponse || !contentType || contentType.startsWith('image/'))
            && (!needsVideoResponse || !contentType || contentType.startsWith('video/'))
          ) {
            const signedUrl = resolveSignedR2MediaUrl(row);
            if (!signedUrl) continue;
            return new Response(null, {
              status: 302,
              headers: privateMediaHeaders({
                location: signedUrl.url,
                'cache-control': signedRedirectCacheControl(signedUrl.expiresSeconds),
              }),
            });
          }
        }
      }
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

function r2SigningKey(secretAccessKey: string, shortDate: string, region: string): Buffer {
  const dateKey = hmac(`AWS4${secretAccessKey}`, shortDate);
  const regionKey = hmac(dateKey, region);
  const serviceKey = hmac(regionKey, 's3');
  return hmac(serviceKey, 'aws4_request');
}

function signedR2GetUrl(bucket: string, storagePath: string): { url: string; expiresSeconds: number } | null {
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

  const expiresSeconds = boundedSignedUrlExpiresSeconds();
  const { shortDate, longDate } = amzDates();
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
  };
}

function resolveSignedR2MediaUrl(data: {
  storage_provider?: string | null;
  storage_bucket?: string | null;
  storage_path?: string | null;
}): { url: string; expiresSeconds: number } | null {
  if (data.storage_provider !== 'r2') return null;

  const bucket = typeof data.storage_bucket === 'string' && data.storage_bucket.trim()
    ? data.storage_bucket.trim()
    : (process.env.R2_BUCKET || '').trim();
  const path = typeof data.storage_path === 'string' ? data.storage_path.trim().replace(/^\/+/, '') : '';
  if (!bucket || !path) return null;
  return signedR2GetUrl(bucket, path);
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

async function fetchRemoteAsset(raw: string, assetRole: string): Promise<Response> {
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
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REMOTE_FETCH_TIMEOUT_MS);
  let streamOwnsTimeout = false;

  try {
    const upstream = await fetch(target.toString(), {
      headers: {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari',
        'accept': '*/*',
        'referer': 'https://www.instagram.com/',
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

    if (!upstream.body) {
      const body = await upstream.arrayBuffer();
      if (body.byteLength > maxBytes) {
        return new Response('remote asset too large', { status: 413 });
      }
      return new Response(body, {
        status: 200,
        headers: privateMediaHeaders({ 'content-type': contentType }),
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
      status: 200,
      headers: privateMediaHeaders({ 'content-type': contentType }),
    });
  } catch {
    return new Response('fetch failed', { status: 502 });
  } finally {
    if (!streamOwnsTimeout) {
      clearTimeout(timeout);
    }
  }
}

async function fetchRemoteAssetForRole(raw: string, assetRole: string): Promise<Response> {
  return fetchRemoteAsset(raw, assetRole);
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
      if (sourceUrl) {
        return fetchRemoteAssetForRole(sourceUrl, assetRole || 'thumbnail');
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

  if (!await profileImageUrlBelongsToUser(sb, feedIds, raw)) {
    return new Response('media unavailable', { status: 404 });
  }

  return fetchRemoteAssetForRole(raw, assetRole || 'thumbnail');
}
