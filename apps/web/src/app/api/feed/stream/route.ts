import { NextRequest } from 'next/server';
import {
  loadStreamPage,
  parseStreamOrder,
  parseStreamRange,
  resolveStreamScope,
  streamAdminClient,
  streamErrorMessage,
  streamJson,
  streamUserId,
  streamWindow,
} from '@/lib/server/feedStreamQuery';

export const dynamic = 'force-dynamic';

// GET /api/feed/stream?feedId=&handle=&order=&from=&to= → { page: StreamPage } (lib/feedStream/contract)
export async function GET(request: NextRequest) {
  try {
    const userId = await streamUserId();
    if (!userId) return streamJson({ error: 'Unauthorized' }, 401);

    const params = request.nextUrl.searchParams;
    const order = parseStreamOrder(params);
    if (!order) return streamJson({ error: 'order must be posted or results' }, 400);

    const nowMs = Date.now();
    const range = parseStreamRange(params, streamWindow(nowMs));
    if (!range.ok) return streamJson({ error: range.error }, 400);

    const sb = streamAdminClient();
    const resolved = await resolveStreamScope(sb, userId, params, order);
    if (!resolved.ok) return streamJson({ error: resolved.error }, resolved.status);

    const page = await loadStreamPage(sb, resolved.scope, range, nowMs);
    return streamJson({ page });
  } catch (error: unknown) {
    return streamJson({ error: streamErrorMessage(error, 'Failed to load posts') }, 500);
  }
}
