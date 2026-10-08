import { NextRequest } from 'next/server';
import {
  loadStreamIndex,
  parseStreamOrder,
  resolveStreamScope,
  streamAdminClient,
  streamErrorMessage,
  streamJson,
  streamUserId,
} from '@/lib/server/feedStreamQuery';

export const dynamic = 'force-dynamic';

// GET /api/feed/stream/index?feedId=&handle=&order= → { index: StreamIndex } (lib/feedStream/contract)
export async function GET(request: NextRequest) {
  try {
    const userId = await streamUserId();
    if (!userId) return streamJson({ error: 'Unauthorized' }, 401);

    const params = request.nextUrl.searchParams;
    const order = parseStreamOrder(params);
    if (!order) return streamJson({ error: 'order must be posted or results' }, 400);

    const sb = streamAdminClient();
    const resolved = await resolveStreamScope(sb, userId, params, order);
    if (!resolved.ok) return streamJson({ error: resolved.error }, resolved.status);

    const index = await loadStreamIndex(sb, resolved.scope);
    return streamJson({ index });
  } catch (error: unknown) {
    return streamJson({ error: streamErrorMessage(error, 'Failed to load the feed') }, 500);
  }
}
