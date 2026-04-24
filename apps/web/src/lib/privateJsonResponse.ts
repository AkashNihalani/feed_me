import { createHash } from 'node:crypto';
import { NextResponse, type NextRequest } from 'next/server';

type PrivateJsonOptions = {
  maxAgeSeconds?: number;
  staleWhileRevalidateSeconds?: number;
  status?: number;
  headers?: HeadersInit;
};

function cacheControl(options: PrivateJsonOptions) {
  const maxAge = Math.max(0, Math.floor(options.maxAgeSeconds ?? 30));
  const stale = Math.max(0, Math.floor(options.staleWhileRevalidateSeconds ?? 300));
  return `private, max-age=${maxAge}, stale-while-revalidate=${stale}`;
}

function acceptsEtag(request: NextRequest, etag: string) {
  const incoming = request.headers.get('if-none-match');
  if (!incoming) return false;
  return incoming
    .split(',')
    .map((value) => value.trim())
    .includes(etag);
}

export function privateJsonResponse<T>(
  request: NextRequest,
  payload: T,
  options: PrivateJsonOptions = {},
) {
  const body = JSON.stringify(payload);
  const etag = `"${createHash('sha256').update(body).digest('base64url')}"`;
  const headers = new Headers(options.headers);
  headers.set('Cache-Control', cacheControl(options));
  headers.set('ETag', etag);
  headers.set('Vary', 'Cookie, Authorization');

  if (acceptsEtag(request, etag)) {
    return new NextResponse(null, { status: 304, headers });
  }

  headers.set('Content-Type', 'application/json; charset=utf-8');
  return new NextResponse(body, { status: options.status ?? 200, headers });
}
