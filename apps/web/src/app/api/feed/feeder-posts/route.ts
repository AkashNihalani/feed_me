import { NextRequest, NextResponse } from 'next/server';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient } from '@/lib/supabase/server';

export const dynamic = 'force-dynamic';

const TRACKING_CHECKPOINTS = ['d1', 'd3', 'd7', 'd21'] as const;
const CHECKPOINT_RANK: Record<(typeof TRACKING_CHECKPOINTS)[number], number> = {
  d1: 1,
  d3: 2,
  d7: 3,
  d21: 4,
};
const POST_KEY_CHUNK_SIZE = 250;

type TrackingCheckpoint = (typeof TRACKING_CHECKPOINTS)[number];

type FeedRow = {
  id: number;
  user_id: string;
  name: string;
};

type FeederRow = {
  id: number;
  feed_id: number;
  handle: string;
  profile_pic_url: string | null;
  follower_count: number | null;
  status: string;
};

type PostRow = {
  post_key: string | null;
  feeder_id: number | null;
  media_type: string | null;
  posted_at: string | null;
  post_url: string | null;
  thumbnail_url: string | null;
};

type PostMetricRow = {
  post_key: string | null;
  checkpoint: string | null;
  business_date_ist: string | null;
  computed_at: string | null;
  views: number | null;
  likes: number | null;
  comments: number | null;
  percentile_performance: number | null;
};

function adminClient() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const serviceRole = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !serviceRole) {
    throw new Error('Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  }
  return createSupabaseClient(url, serviceRole, {
    auth: { autoRefreshToken: false, persistSession: false },
  });
}

function nullableString(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value.trim() : null;
}

function buildProfileImageProxyUrl(url: string | null | undefined) {
  const normalized = nullableString(url);
  if (!normalized) return null;
  const search = new URLSearchParams({
    role: 'thumbnail',
    url: normalized,
  });
  return `/api/media?${search.toString()}`;
}

function nullableNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function parseIsoTime(value: unknown): number {
  if (typeof value !== 'string' || !value) return 0;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizeHandle(value: string | null): string {
  return (value || '').trim().replace(/^@+/, '').toLowerCase();
}

function normalizeTrackingCheckpoint(value: unknown): TrackingCheckpoint | '' {
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return normalized === 'd1' || normalized === 'd3' || normalized === 'd7' || normalized === 'd21'
    ? normalized
    : '';
}

function normalizeMediaType(value: unknown): 'image' | 'carousel' | 'reel' | 'unknown' {
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
  if (!normalized) return 'unknown';
  if (normalized === 'sidecar' || normalized === 'sidcar' || normalized === 'carousel') return 'carousel';
  if (normalized === 'reel' || normalized === 'video') return 'reel';
  if (normalized === 'image' || normalized === 'photo') return 'image';
  return 'unknown';
}

function buildMediaProxyUrl(postKey: string | null | undefined, url: string | null | undefined, role = 'thumbnail') {
  const safePostKey = typeof postKey === 'string' ? postKey.trim() : '';
  const safeUrl = typeof url === 'string' ? url.trim() : '';
  if (!safePostKey) return null;

  const search = new URLSearchParams({
    postKey: safePostKey,
    role,
  });

  if (safeUrl) {
    search.set('url', safeUrl);
  }

  return `/api/media?${search.toString()}`;
}

async function fetchMetricRowsForPostKeys(
  sb: ReturnType<typeof adminClient>,
  postKeys: string[],
): Promise<PostMetricRow[]> {
  if (postKeys.length === 0) return [];

  const rows: PostMetricRow[] = [];
  for (let start = 0; start < postKeys.length; start += POST_KEY_CHUNK_SIZE) {
    const chunk = postKeys.slice(start, start + POST_KEY_CHUNK_SIZE);
    const { data, error } = await sb
      .from('post_metrics')
      .select('post_key,checkpoint,business_date_ist,computed_at,views,likes,comments,percentile_performance')
      .in('post_key', chunk)
      .in('checkpoint', [...TRACKING_CHECKPOINTS]);

    if (error) throw error;
    rows.push(...((data || []) as PostMetricRow[]));
  }

  return rows;
}

export async function GET(request: NextRequest) {
  try {
    const supabase = await createClient();
    const {
      data: { user },
      error: authError,
    } = await supabase.auth.getUser();

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const feedId = Number(request.nextUrl.searchParams.get('feedId') || 0);
    const handle = normalizeHandle(request.nextUrl.searchParams.get('handle'));

    if (!feedId || !handle) {
      return NextResponse.json({ error: 'feedId and handle are required' }, { status: 400 });
    }

    const sb = adminClient();
    const { data: feedRow, error: feedError } = await sb
      .from('feeds')
      .select('id,user_id,name')
      .eq('id', feedId)
      .eq('user_id', user.id)
      .single<FeedRow>();

    if (feedError || !feedRow) {
      return NextResponse.json({ error: 'Feed not found' }, { status: 404 });
    }

    const { data: feederRow, error: feederError } = await sb
      .from('feeders')
      .select('id,feed_id,handle,profile_pic_url,follower_count,status')
      .eq('feed_id', feedId)
      .eq('status', 'active')
      .eq('handle', handle)
      .single<FeederRow>();

    if (feederError || !feederRow) {
      return NextResponse.json({ error: 'Feeder not found' }, { status: 404 });
    }

    const { data: postsData, error: postsError } = await sb
      .from('posts')
      .select('post_key,feeder_id,media_type,posted_at,post_url,thumbnail_url')
      .eq('feeder_id', feederRow.id)
      .order('posted_at', { ascending: false });

    if (postsError) throw postsError;

    const posts = (postsData || []) as PostRow[];
    const postKeys = posts
      .map((row) => nullableString(row.post_key))
      .filter((value): value is string => Boolean(value));

    const metricRows = await fetchMetricRowsForPostKeys(sb, postKeys);
    const latestMetricByPost = new Map<string, PostMetricRow & { checkpointRank: number }>();

    for (const row of metricRows) {
      const postKey = nullableString(row.post_key);
      const checkpoint = normalizeTrackingCheckpoint(row.checkpoint);
      if (!postKey || !checkpoint) continue;

      const checkpointRank = CHECKPOINT_RANK[checkpoint];
      const current = latestMetricByPost.get(postKey);
      if (
        !current
        || checkpointRank > current.checkpointRank
        || (checkpointRank === current.checkpointRank && parseIsoTime(row.computed_at) > parseIsoTime(current.computed_at))
      ) {
        latestMetricByPost.set(postKey, { ...row, checkpointRank });
      }
    }

    const trackedPosts = posts
      .map((post) => {
        const postKey = nullableString(post.post_key);
        if (!postKey) return null;

        const latestMetric = latestMetricByPost.get(postKey);
        if (!latestMetric) return null;

        return {
          postKey,
          postUrl: nullableString(post.post_url),
          thumbnailUrl: buildMediaProxyUrl(postKey, nullableString(post.thumbnail_url)),
          mediaType: normalizeMediaType(post.media_type),
          postedAt: nullableString(post.posted_at),
          latestCheckpoint: normalizeTrackingCheckpoint(latestMetric.checkpoint).toUpperCase(),
          latestBusinessDayIst: nullableString(latestMetric.business_date_ist),
          latestPercentile: nullableNumber(latestMetric.percentile_performance),
          views: nullableNumber(latestMetric.views),
          likes: nullableNumber(latestMetric.likes),
          comments: nullableNumber(latestMetric.comments),
        };
      })
      .filter((value): value is NonNullable<typeof value> => Boolean(value))
      .sort((a, b) => parseIsoTime(b.postedAt) - parseIsoTime(a.postedAt));

    const percentileValues = trackedPosts
      .map((post) => post.latestPercentile)
      .filter((value): value is number => value != null && Number.isFinite(value));

    return NextResponse.json({
      feeder: {
        feedId: feedRow.id,
        feedName: feedRow.name,
        handle: feederRow.handle,
        profilePicUrl: buildProfileImageProxyUrl(feederRow.profile_pic_url),
        followerCount: nullableNumber(feederRow.follower_count),
        trackedPosts: trackedPosts.length,
        topPercentile: percentileValues.length > 0 ? Math.min(...percentileValues) : null,
      },
      posts: trackedPosts,
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to load feeder posts';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
