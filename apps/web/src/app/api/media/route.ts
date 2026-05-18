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

function isVideoRole(assetRole: string): boolean {
  const role = (assetRole || '').trim().toLowerCase();
  return role === 'preview_5s' || role === 'video';
}

function candidatePostKeys(postKey: string): string[] {
  const trimmed = (postKey || '').trim();
  const withoutHash = trimmed.split('#')[0] || '';
  const lower = trimmed.toLowerCase();
  const lowerWithoutHash = lower.split('#')[0] || '';
  return Array.from(new Set([trimmed, withoutHash, lower, lowerWithoutHash].filter(Boolean)));
}

async function probePublicMediaUrl(
  publicUrl: string,
  assetRole: string,
  mimeType?: string | null,
): Promise<'ok' | 'missing' | 'unknown'> {
  const role = (assetRole || 'thumbnail').trim().toLowerCase();
  const expectedPrefix = role === 'thumbnail' ? 'image/' : role === 'preview_5s' || role === 'video' ? 'video/' : '';
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 4000);

  try {
    const response = await fetch(publicUrl, {
      method: 'HEAD',
      cache: 'no-store',
      redirect: 'follow',
      signal: controller.signal,
    });

    if (response.ok) {
      const contentType = (response.headers.get('content-type') || mimeType || '').toLowerCase();
      if (!expectedPrefix || !contentType || contentType.startsWith(expectedPrefix)) {
        return 'ok';
      }
      return 'missing';
    }

    if ([403, 404, 410].includes(response.status)) {
      return 'missing';
    }

    return 'unknown';
  } catch {
    return 'unknown';
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchStoredAsset(postKey: string, assetRole: string): Promise<Response | null> {
  const sb = adminClient();
  const role = (assetRole || 'thumbnail').trim().toLowerCase();
  const candidateRoles = role === 'thumbnail'
    ? ['thumbnail', 'display', 'carousel_0']
    : [role];
  const needsImageResponse = role === 'thumbnail';
  const needsVideoResponse = isVideoRole(role);
  const postKeyCandidates = candidatePostKeys(postKey);

  for (const candidatePostKey of postKeyCandidates) {
    for (const candidateRole of candidateRoles) {
      const { data, error } = await sb
        .from('post_media_assets')
        .select('storage_provider,storage_bucket,storage_path,public_url,mime_type,status,purge_after,source_url,updated_at')
        .eq('post_key', candidatePostKey)
        .eq('asset_role', candidateRole)
        .in('status', ['active', 'purge_pending'])
        .order('updated_at', { ascending: false })
        .limit(8);

      if (error || !data?.length) {
        continue;
      }
      for (const row of data) {
        if (row.purge_after && new Date(row.purge_after).getTime() <= Date.now()) {
          continue;
        }
        const publicUrl = resolvePublicMediaUrl(row);

        if (row.storage_provider === 'r2' && publicUrl) {
          const contentType = typeof row.mime_type === 'string' ? row.mime_type.toLowerCase() : '';
          if (
            (!needsImageResponse || !contentType || contentType.startsWith('image/'))
            && (!needsVideoResponse || !contentType || contentType.startsWith('video/'))
          ) {
            const probe = row.storage_provider === 'r2'
              ? await probePublicMediaUrl(publicUrl, candidateRole, row.mime_type)
              : 'ok';
            if (probe !== 'missing') {
              return new Response(null, {
                status: 302,
                headers: {
                  location: publicUrl,
                  'cache-control': 'public, max-age=3600, stale-while-revalidate=86400',
                },
              });
            }
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

async function fetchPostSourceUrl(postKey: string, assetRole: string): Promise<string | null> {
  const sb = adminClient();
  const role = (assetRole || 'thumbnail').trim().toLowerCase();
  for (const candidatePostKey of candidatePostKeys(postKey)) {
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

function resolvePublicMediaUrl(data: {
  storage_provider?: string | null;
  storage_path?: string | null;
  public_url?: string | null;
}): string | null {
  const publicUrl = typeof data.public_url === 'string' ? data.public_url.trim() : '';
  if (publicUrl) return publicUrl;
  if (data.storage_provider !== 'r2') return null;

  const base = (process.env.MEDIA_PUBLIC_BASE_URL || '').trim().replace(/\/$/, '');
  const path = typeof data.storage_path === 'string' ? data.storage_path.trim().replace(/^\/+/, '') : '';
  if (!base || !path) return null;
  return `${base}/${path.split('/').map(encodeURIComponent).join('/')}`;
}

function redirectRemoteAsset(raw: string): Response | null {
  let target: URL;
  try {
    target = new URL(raw);
  } catch {
    return null;
  }

  if (!['http:', 'https:'].includes(target.protocol) || !isAllowedHost(target.hostname)) {
    return null;
  }

  return new Response(null, {
    status: 302,
    headers: {
      location: target.toString(),
      'cache-control': 'public, max-age=1800, stale-while-revalidate=3600',
    },
  });
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
        'accept': '*/*',
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

  const role = (assetRole || 'thumbnail').trim().toLowerCase();
  if (role === 'thumbnail') {
    const contentType = response.headers.get('content-type') || '';
    if (!contentType.toLowerCase().startsWith('image/')) {
      return new Response('thumbnail source not image', { status: 502 });
    }
  }

  if (isVideoRole(role)) {
    const contentType = response.headers.get('content-type') || '';
    if (!contentType.toLowerCase().startsWith('video/')) {
      return new Response('preview source not video', { status: 502 });
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
      const sourceUrl = await fetchPostSourceUrl(postKey, assetRole || 'thumbnail');
      if (sourceUrl) {
        const redirect = redirectRemoteAsset(sourceUrl);
        if (redirect) return redirect;
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

  return fetchRemoteAssetForRole(raw, assetRole || 'thumbnail');
}
