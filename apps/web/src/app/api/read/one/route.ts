import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@/lib/supabase/server';
import { privateJsonResponse } from '@/lib/privateJsonResponse';

/* A feeder's read as one page (components/read/one): its week's drop and its post mortems. Only for someone signed in:
   the files are bundled with the server and never sit in public. (FM_PROBE=1 lets a build started on this machine read
   it signed out; never on Vercel.) */

export const dynamic = 'force-dynamic';

const READS: Record<string, () => Promise<{ drop: unknown; mortems: unknown }>> = {
  'anuj.mp4': async () => ({
    drop: (await import('@/data/readOne/anuj.mp4.drop.json')).default,
    mortems: (await import('@/data/readOne/anuj.mp4.mortems.json')).default,
  }),
};

export async function GET(request: NextRequest) {
  try {
    if (process.env.VERCEL || process.env.FM_PROBE !== '1') {
      const supabase = await createClient();
      const { data: { user }, error } = await supabase.auth.getUser();
      if (error || !user) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }
    const handle = request.nextUrl.searchParams.get('handle') ?? '';
    const read = Object.hasOwn(READS, handle) ? READS[handle] : null;
    if (!read) return NextResponse.json({ error: 'Not read yet' }, { status: 404 });
    return privateJsonResponse(request, await read(), { maxAgeSeconds: 300, staleWhileRevalidateSeconds: 3600 });
  } catch (error) {
    console.error('[read/one]', error);
    return NextResponse.json({ error: 'Failed to load the read' }, { status: 500 });
  }
}
