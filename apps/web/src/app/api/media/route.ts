import { NextRequest } from 'next/server';

const ALLOWED_HOST_SUFFIXES = [
  'cdninstagram.com',
  'fbcdn.net',
  'instagram.com',
  'cdninstagram.net',
];

function isAllowedHost(hostname: string): boolean {
  const h = (hostname || '').toLowerCase();
  return ALLOWED_HOST_SUFFIXES.some((suffix) => h === suffix || h.endsWith(`.${suffix}`));
}

export async function GET(req: NextRequest) {
  const raw = req.nextUrl.searchParams.get('url') || '';
  if (!raw) {
    return new Response('missing url', { status: 400 });
  }

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
