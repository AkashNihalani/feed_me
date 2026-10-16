import { NextRequest, NextResponse } from 'next/server';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient } from '@/lib/supabase/server';
import { privateJsonResponse } from '@/lib/privateJsonResponse';
import { withServerRouteCache } from '@/lib/serverRouteCache';
import { addDays, istDayKey, postMoment } from '@/lib/postMoment';

/* What Lead's habits and growth are drawn from, for the selected range and the equal range before it:
   every post that went out (ranked or not, to the minute: see lib/postMoment) and every feeder's daily
   follower reading. One call for all of the account's feeds; Lead scopes it on the client. */

export const dynamic = 'force-dynamic';
const ACTIVITY_TTL_MS = 5 * 60 * 1000;
const PAGE = 1000;

export type LeadActivity = {
  // the first day of the prior range and today, as IST day keys
  from: string;
  to: string;
  feeds: Array<{
    id: string;
    feeders: Array<{
      handle: string;
      // [day offset from `from`, followers]
      followers: Array<[number, number]>;
    }>;
  }>;
  // [feed id, handle, ms, media: r(eel) c(arousel) i(mage) u(nknown), post key]
  posts: Array<[string, string, number, string, string]>;
};

function adminClient() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const serviceRole = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !serviceRole) throw new Error('Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  return createSupabaseClient(url, serviceRole, { auth: { autoRefreshToken: false, persistSession: false } });
}

function mediaLetter(value: unknown) {
  const media = typeof value === 'string' ? value.trim().toLowerCase() : '';
  if (media === 'reel' || media === 'video') return 'r';
  if (media === 'sidecar' || media === 'sidcar' || media === 'carousel') return 'c';
  if (media === 'image' || media === 'photo') return 'i';
  return 'u';
}

async function pages<T>(query: (from: number, to: number) => PromiseLike<{ data: T[] | null; error: { message: string } | null }>) {
  const rows: T[] = [];
  for (let from = 0; ; from += PAGE) {
    const { data, error } = await query(from, from + PAGE - 1);
    if (error) throw new Error(error.message);
    rows.push(...(data || []));
    if (!data || data.length < PAGE) return rows;
  }
}

export async function GET(request: NextRequest) {
  try {
    const supabase = await createClient();
    const { data: { user }, error: authError } = await supabase.auth.getUser();
    if (authError || !user) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });

    const days = Number(request.nextUrl.searchParams.get('days') || 30);
    if (![7, 30, 60, 90].includes(days)) return NextResponse.json({ error: 'days must be one of 7,30,60,90' }, { status: 400 });

    const payload = await withServerRouteCache(`lead:activity:v1:${user.id}:${days}`, ACTIVITY_TTL_MS, async (): Promise<LeadActivity> => {
      const sb = adminClient();
      const to = istDayKey(Date.now());
      // the prior range, the selected one, and the reading the day before (a range's growth starts from it)
      const from = addDays(to, -(days * 2));
      const fromMs = Date.parse(`${from}T00:00:00+05:30`);

      const { data: feedRows, error: feedError } = await sb.from('feeds').select('id').eq('user_id', user.id).eq('status', 'active');
      if (feedError) throw new Error(feedError.message);
      const feedIds = (feedRows || []).map((row) => Number(row.id)).filter(Number.isFinite);
      if (!feedIds.length) return { from, to, feeds: [], posts: [] };

      const { data: feederRows, error: feederError } = await sb.from('feeders').select('id,feed_id,handle').in('feed_id', feedIds).eq('status', 'active');
      if (feederError) throw new Error(feederError.message);
      const feeders = (feederRows || []).map((row) => ({ id: Number(row.id), feedId: String(row.feed_id), handle: String(row.handle || '').replace(/^@+/, '').toLowerCase() }));
      const feederIds = feeders.map((feeder) => feeder.id);
      const byId = new Map(feeders.map((feeder) => [feeder.id, feeder]));

      // a post's stored date can sit a day early, so ask from a day before the range
      const [snapshots, posts] = await Promise.all([
        pages<{ feeder_id: number; snapshot_date_ist: string; follower_count: number | null }>((a, b) => sb
          .from('feeder_follower_snapshots')
          .select('feeder_id,snapshot_date_ist,follower_count')
          .in('feeder_id', feederIds)
          .gte('snapshot_date_ist', from)
          .order('snapshot_date_ist')
          .order('feeder_id')
          .range(a, b)),
        pages<{ post_key: string; feeder_id: number; posted_at: string | null; post_url: string | null; media_type: string | null }>((a, b) => sb
          .from('posts')
          .select('post_key,feeder_id,posted_at,post_url,media_type')
          .in('feeder_id', feederIds)
          .gte('posted_at', new Date(fromMs - 86_400_000).toISOString())
          .order('posted_at')
          .order('post_key')
          .range(a, b)),
      ]);

      const fromStart = Date.parse(`${from}T00:00:00Z`);
      const series = new Map<number, Array<[number, number]>>();
      for (const row of snapshots) {
        const count = Number(row.follower_count);
        if (!Number.isFinite(count) || count <= 0) continue;
        const offset = Math.round((Date.parse(`${row.snapshot_date_ist}T00:00:00Z`) - fromStart) / 86_400_000);
        const list = series.get(row.feeder_id) || [];
        list.push([offset, count]);
        series.set(row.feeder_id, list);
      }

      const feeds = feedIds.map((feedId) => ({
        id: String(feedId),
        feeders: feeders
          .filter((feeder) => feeder.feedId === String(feedId))
          .map((feeder) => ({ handle: feeder.handle, followers: series.get(feeder.id) || [] })),
      }));

      const seen = new Set<string>();
      const postRows: LeadActivity['posts'] = [];
      for (const row of posts) {
        const feeder = byId.get(Number(row.feeder_id));
        const moment = postMoment(row.posted_at, row.post_url);
        if (!feeder || moment == null || moment < fromMs) continue;
        // a collab lands on each partner's feeder: count it once per feed
        const key = `${feeder.feedId}:${row.post_key}`;
        if (seen.has(key)) continue;
        seen.add(key);
        postRows.push([feeder.feedId, feeder.handle, moment, mediaLetter(row.media_type), row.post_key]);
      }
      postRows.sort((a, b) => a[2] - b[2]);
      return { from, to, feeds, posts: postRows };
    });

    return privateJsonResponse(request, payload, { maxAgeSeconds: 60, staleWhileRevalidateSeconds: 300 });
  } catch (error: unknown) {
    return NextResponse.json({ error: error instanceof Error ? error.message : 'Activity could not be loaded' }, { status: 500 });
  }
}
