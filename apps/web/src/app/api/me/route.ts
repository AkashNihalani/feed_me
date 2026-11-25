import { NextRequest, NextResponse } from 'next/server';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient } from '@/lib/supabase/server';
import { privateJsonResponse } from '@/lib/privateJsonResponse';
import { withServerRouteCache } from '@/lib/serverRouteCache';
import { istDayKey, postMoment } from '@/lib/postMoment';

/* What Me draws: every post tracked since the account's first reading (its day and hour in IST, its kind, its likes and
   comments and how many times usual they were), and each feeder's followers. One call for all of the account's feeders;
   the page scopes it on the client, and takes the feeds and the faces from /api/feed. */

export const dynamic = 'force-dynamic';
const ME_TTL_MS = 10 * 60 * 1000;
const PAGE = 1000;
const FOLLOWER_DAYS = 40;
const DAY_MS = 86_400_000;

export type MeData = {
  // the first day tracked and today, as IST day keys
  began: string;
  today: string;
  // [handle, first day tracked, followers then, followers now]
  h: Array<[string, string, number, number]>;
  // [day from `began`, hour, feeder, 0 reel 1 carousel 2 picture, likes × usual ×10, likes, comments, comments × usual ×10]
  p: Array<[number, number, number, number, number, number, number, number]>;
  // a feeder's last readings: [day from `began`, followers]
  fo: Array<Array<[number, number]>>;
};

type PostRow = {
  post_key: string;
  feeder_id: number;
  posted_at: string | null;
  post_url: string | null;
  media_type: string | null;
  post_metrics: Array<{ likes: number | null; comments: number | null; likes_multiple: number | null; comments_multiple: number | null }> | null;
};

function adminClient() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const serviceRole = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !serviceRole) throw new Error('Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  return createSupabaseClient(url, serviceRole, { auth: { autoRefreshToken: false, persistSession: false } });
}

function mediaCode(value: unknown) {
  const media = typeof value === 'string' ? value.trim().toLowerCase() : '';
  if (media === 'reel' || media === 'video') return 0;
  if (media === 'sidecar' || media === 'sidcar' || media === 'carousel') return 1;
  return 2;
}

const count = (value: unknown) => {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : 0;
};

async function pages<T>(query: (from: number, to: number) => PromiseLike<{ data: T[] | null; error: { message: string } | null }>) {
  const rows: T[] = [];
  for (let from = 0; ; from += PAGE) {
    const { data, error } = await query(from, from + PAGE - 1);
    if (error) throw new Error(error.message);
    rows.push(...(data || []));
    if (!data || data.length < PAGE) return rows;
  }
}

async function readMe(userId: string): Promise<MeData> {
  const sb = adminClient();
  const today = istDayKey(Date.now());
  const empty: MeData = { began: today, today, h: [], p: [], fo: [] };

  const { data: feedRows, error: feedError } = await sb.from('feeds').select('id').eq('user_id', userId).eq('status', 'active');
  if (feedError) throw new Error(feedError.message);
  const feedIds = (feedRows || []).map((row) => Number(row.id)).filter(Number.isFinite);
  if (!feedIds.length) return empty;

  const { data: feederRows, error: feederError } = await sb.from('feeders').select('id,handle,follower_count').in('feed_id', feedIds).eq('status', 'active').order('id');
  if (feederError) throw new Error(feederError.message);
  // a handle in two feeds is one feeder here
  const handles: string[] = [];
  const now: number[] = [];
  const at = new Map<number, number>();
  for (const row of feederRows || []) {
    const handle = String(row.handle || '').trim().replace(/^@+/, '').toLowerCase();
    if (!handle) continue;
    let index = handles.indexOf(handle);
    if (index < 0) {
      index = handles.push(handle) - 1;
      now.push(count(row.follower_count));
    }
    at.set(Number(row.id), index);
  }
  const feederIds = [...at.keys()];
  if (!feederIds.length) return empty;

  const [readings, posts] = await Promise.all([
    pages<{ feeder_id: number; snapshot_date_ist: string; follower_count: number | null }>((a, b) => sb
      .from('feeder_follower_snapshots')
      .select('feeder_id,snapshot_date_ist,follower_count')
      .in('feeder_id', feederIds)
      .order('snapshot_date_ist')
      .order('feeder_id')
      .range(a, b)),
    pages<PostRow>((a, b) => sb
      .from('posts')
      .select('post_key,feeder_id,posted_at,post_url,media_type,post_metrics(likes,comments,likes_multiple,comments_multiple)')
      .in('feeder_id', feederIds)
      .order('posted_at')
      .order('post_key')
      .range(a, b) as unknown as PromiseLike<{ data: PostRow[] | null; error: { message: string } | null }>),
  ]);

  // each feeder's readings, a day each (the later of two feeds' on the same day)
  const series = handles.map(() => new Map<string, number>());
  for (const row of readings) {
    const index = at.get(Number(row.feeder_id));
    const followers = count(row.follower_count);
    if (index != null && followers) series[index].set(String(row.snapshot_date_ist).slice(0, 10), followers);
  }

  // each post once, at its moment, with the most it was counted at
  const seen = new Set<string>();
  const measured: string[] = handles.map(() => '');
  const rows: Array<{ day: string; ms: number; index: number; type: number; lm: number; likes: number; comments: number; cm: number; counted: boolean }> = [];
  for (const row of posts) {
    const index = at.get(Number(row.feeder_id));
    const ms = postMoment(row.posted_at, row.post_url);
    if (index == null || ms == null || seen.has(row.post_key)) continue;
    seen.add(row.post_key);
    let lm = 0;
    let likes = 0;
    let comments = 0;
    let cm = 0;
    for (const metric of row.post_metrics || []) {
      if (count(metric.likes) >= likes) {
        likes = count(metric.likes);
        lm = count(metric.likes_multiple);
      }
      comments = Math.max(comments, count(metric.comments));
      cm = Math.max(cm, count(metric.comments_multiple));
    }
    const day = istDayKey(ms);
    const counted = Boolean(row.post_metrics?.length);
    if (counted && (!measured[index] || day < measured[index])) measured[index] = day;
    rows.push({ day, ms, index, type: mediaCode(row.media_type), lm, likes, comments, cm, counted });
  }

  // tracked from its first reading, or the first post counted, whichever came first
  const since = handles.map((_, index) => {
    const first = series[index].keys().next().value as string | undefined;
    return [first, measured[index]].filter(Boolean).sort()[0] || today;
  });
  const began = [...since].sort()[0] || today;
  const start = Date.parse(`${began}T00:00:00Z`);
  const dayOf = (key: string) => Math.round((Date.parse(`${key}T00:00:00Z`) - start) / DAY_MS);
  const lastDay = dayOf(today);

  const h: MeData['h'] = handles.map((handle, index) => {
    const values = [...series[index].values()];
    return [handle, since[index], values[0] || now[index], values[values.length - 1] || now[index]];
  });
  const p: MeData['p'] = rows
    .filter((row) => row.day >= since[row.index] && row.day <= today)
    .sort((a, b) => a.ms - b.ms)
    .map((row) => {
      const hour = Math.floor((((row.ms + 19_800_000) % DAY_MS) + DAY_MS) % DAY_MS / 3_600_000);
      return [dayOf(row.day), hour, row.index, row.type, Math.round(row.lm * 10), row.likes, row.comments, Math.round(row.cm * 10)];
    });
  const fo: MeData['fo'] = series.map((one) => [...one]
    .map(([key, followers]) => [dayOf(key), followers] as [number, number])
    .filter(([day]) => day >= lastDay - FOLLOWER_DAYS));
  return { began, today, h, p, fo };
}

export async function GET(request: NextRequest) {
  try {
    const supabase = await createClient();
    const { data: { user }, error: authError } = await supabase.auth.getUser();
    if (authError || !user) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    const payload = await withServerRouteCache(`me:v1:${user.id}:${istDayKey(Date.now())}`, ME_TTL_MS, () => readMe(user.id));
    return privateJsonResponse(request, payload, { maxAgeSeconds: 60, staleWhileRevalidateSeconds: 600 });
  } catch (error: unknown) {
    return NextResponse.json({ error: error instanceof Error ? error.message : 'Me could not be loaded' }, { status: 500 });
  }
}
