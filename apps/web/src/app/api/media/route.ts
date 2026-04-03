import { NextRequest } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const ALLOWED_HOST_SUFFIXES = [
  'cdninstagram.com',
  'fbcdn.net',
  'instagram.com',
  'cdninstagram.net',
];

function adminClient() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const serviceRole = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !serviceRole) {
    throw new Error('Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  }
  return createClient(url, serviceRole, {
    auth: { autoRefreshToken: false, persistSession: false },
  });
}

function isAllowedHost(hostname: string): boolean {
  const h = (hostname || '').toLowerCase();
  return ALLOWED_HOST_SUFFIXES.some((suffix) => h === suffix || h.endsWith(`.${suffix}`));
}

async function fetchStoredAsset(postKey: string, assetRole: string): Promise<Response | null> {
  const sb = adminClient();
  const role = (assetRole || 'thumbnail').trim().toLowerCase();
  const candidateRoles = role === 'thumbnail'
    ? ['thumbnail', 'display', 'carousel_0']
    : [role];
  const needsImageResponse = role === 'thumbnail';

  for (const candidateRole of candidateRoles) {
    const { data, error } = await sb
      .from('post_media_assets')
      .select('storage_bucket,storage_path,mime_type,status,purge_after,source_url')
      .eq('post_key', postKey)
      .eq('asset_role', candidateRole)
      .eq('status', 'active')
      .maybeSingle();

    if (error || !data) {
      continue;
    }
    if (data.purge_after && new Date(data.purge_after).getTime() <= Date.now()) {
      continue;
    }
    const sourceUrl = typeof data.source_url === 'string' ? data.source_url.trim() : '';

    if (data.storage_bucket && data.storage_path) {
      const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
      const serviceRole = process.env.SUPABASE_SERVICE_ROLE_KEY;
      if (!url || !serviceRole) return null;

      const upstream = await fetch(
        `${url.replace(/\/$/, '')}/storage/v1/object/authenticated/${encodeURIComponent(data.storage_bucket)}/${data.storage_path
          .split('/')
          .map((part: string) => encodeURIComponent(part))
          .join('/')}`,
        {
          headers: {
            apikey: serviceRole,
            Authorization: `Bearer ${serviceRole}`,
          },
          cache: 'no-store',
        },
      );

      if (upstream.ok) {
        const contentType = upstream.headers.get('content-type') || data.mime_type || 'application/octet-stream';
        if (!needsImageResponse || contentType.toLowerCase().startsWith('image/')) {
          const body = await upstream.arrayBuffer();
          return new Response(body, {
            status: 200,
            headers: {
              'content-type': contentType,
              'cache-control': 'public, max-age=3600, stale-while-revalidate=86400',
            },
          });
        }
      }
    }

    if (!sourceUrl) {
      continue;
    }

    const fallback = await fetchRemoteAsset(sourceUrl);
    if (!fallback.ok) {
      continue;
    }

    const contentType = fallback.headers.get('content-type') || '';
    if (needsImageResponse && !contentType.toLowerCase().startsWith('image/')) {
      continue;
    }

    return fallback;
  }

  return null;
}

async function fetchFallbackSourceAsset(postKey: string, assetRole: string): Promise<Response | null> {
  const sb = adminClient();
  const role = (assetRole || 'thumbnail').trim().toLowerCase();
  const candidateRoles = role === 'thumbnail'
    ? ['thumbnail', 'display', 'carousel_0']
    : [role];
  const needsImageResponse = role === 'thumbnail';

  const { data, error } = await sb
    .from('post_media_assets')
    .select('source_url,asset_role,status,updated_at')
    .eq('post_key', postKey)
    .in('asset_role', candidateRoles)
    .neq('status', 'deleted')
    .order('updated_at', { ascending: false })
    .limit(12);

  if (error || !data?.length) {
    return null;
  }

  for (const row of data) {
    const sourceUrl = typeof row.source_url === 'string' ? row.source_url.trim() : '';
    if (!sourceUrl) continue;
    const response = await fetchRemoteAsset(sourceUrl);
    if (!response.ok) continue;
    const contentType = response.headers.get('content-type') || '';
    if (needsImageResponse && !contentType.toLowerCase().startsWith('image/')) continue;
    return response;
  }

  return null;
}

async function fetchPostMediaFallback(postKey: string, assetRole: string): Promise<Response | null> {
  const sb = adminClient();
  const role = (assetRole || 'thumbnail').trim().toLowerCase();
  const { data, error } = await sb
    .from('posts')
    .select('thumbnail_url,carousel_urls,video_url')
    .eq('post_key', postKey)
    .maybeSingle();

  if (error || !data) {
    return null;
  }

  const candidateUrls: string[] = [];
  if (role === 'thumbnail') {
    const thumbnailUrl = typeof data.thumbnail_url === 'string' ? data.thumbnail_url.trim() : '';
    if (thumbnailUrl) candidateUrls.push(thumbnailUrl);

    if (Array.isArray(data.carousel_urls)) {
      for (const entry of data.carousel_urls) {
        const value = typeof entry === 'string' ? entry.trim() : '';
        if (value) candidateUrls.push(value);
      }
    }
  } else if (role === 'video') {
    const videoUrl = typeof data.video_url === 'string' ? data.video_url.trim() : '';
    if (videoUrl) candidateUrls.push(videoUrl);
  }

  for (const candidateUrl of candidateUrls) {
    const response = await fetchRemoteAsset(candidateUrl);
    if (!response.ok) continue;
    const contentType = response.headers.get('content-type') || '';
    if (role === 'thumbnail' && !contentType.toLowerCase().startsWith('image/')) continue;
    return response;
  }

  return null;
}

async function fetchRemoteAsset(raw: string): Promise<Response> {
  let target: URL;
  try {
    target = new URL(raw);
  } catch {
    return new Response('invalid url', { status: 400 });
  }

  if (!['http:', 'https:'].includes(target.protocol) || !isAllowedHost(target.hostname)) {
    return new Response('forbidden', { status: 403 });
  }

  try {
    const upstream = await fetch(target.toString(), {
      headers: {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari',
        'accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
        'referer': 'https://www.instagram.com/',
      },
      cache: 'no-store',
    });

    if (!upstream.ok) {
      return new Response(`upstream ${upstream.status}`, { status: 502 });
    }

    const contentType = upstream.headers.get('content-type') || 'image/jpeg';
    const body = await upstream.arrayBuffer();

    return new Response(body, {
      status: 200,
      headers: {
        'content-type': contentType,
        'cache-control': 'public, max-age=3600, stale-while-revalidate=86400',
      },
    });
  } catch {
    return new Response('fetch failed', { status: 502 });
  }
}

async function fetchRemoteAssetForRole(raw: string, assetRole: string): Promise<Response> {
  const response = await fetchRemoteAsset(raw);
  if (!response.ok) {
    return response;
  }

  if ((assetRole || 'thumbnail').trim().toLowerCase() === 'thumbnail') {
    const contentType = response.headers.get('content-type') || '';
    if (!contentType.toLowerCase().startsWith('image/')) {
      return new Response('thumbnail source not image', { status: 502 });
    }
  }

  return response;
}

export async function GET(req: NextRequest) {
  const postKey = (req.nextUrl.searchParams.get('postKey') || '').trim();
  const assetRole = (req.nextUrl.searchParams.get('role') || 'thumbnail').trim().toLowerCase();
  const raw = req.nextUrl.searchParams.get('url') || '';

  if (postKey) {
    try {
      const stored = await fetchStoredAsset(postKey, assetRole || 'thumbnail');
      if (stored) {
        return stored;
      }
      const staged = await fetchFallbackSourceAsset(postKey, assetRole || 'thumbnail');
      if (staged) {
        return staged;
      }
      const postFallback = await fetchPostMediaFallback(postKey, assetRole || 'thumbnail');
      if (postFallback) {
        return postFallback;
      }
    } catch {
      // fall through to legacy proxy behavior
    }
  }

  if (!raw) {
    return new Response('missing url', { status: 400 });
  }

  return fetchRemoteAssetForRole(raw, assetRole || 'thumbnail');
}
