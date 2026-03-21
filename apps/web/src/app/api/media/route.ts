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
  const { data, error } = await sb
    .from('post_media_assets')
    .select('storage_bucket,storage_path,mime_type,status,purge_after')
    .eq('post_key', postKey)
    .eq('asset_role', assetRole)
    .eq('status', 'active')
    .maybeSingle();

  if (error || !data?.storage_bucket || !data.storage_path) {
    return null;
  }
  if (data.purge_after && new Date(data.purge_after).getTime() <= Date.now()) {
    return null;
  }

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

  if (!upstream.ok) {
    return null;
  }

  const contentType = upstream.headers.get('content-type') || data.mime_type || 'application/octet-stream';
  const body = await upstream.arrayBuffer();
  return new Response(body, {
    status: 200,
    headers: {
      'content-type': contentType,
      'cache-control': 'public, max-age=3600, stale-while-revalidate=86400',
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
    } catch {
      // fall through to legacy proxy behavior
    }
  }

  if (!raw) {
    return new Response('missing url', { status: 400 });
  }

  return fetchRemoteAsset(raw);
}
