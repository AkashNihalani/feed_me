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
  engagement_rate?: number | null;
  percentile_performance: number | null;
  percentile_performance_exact?: number | null;
  ranking_metric?: string | null;
  ranking_multiple?: number | null;
  likes_multiple?: number | null;
  comments_multiple?: number | null;
  engagement_rate_multiple?: number | null;
};

type TrackedPost = {
  postKey: string;
  postUrl: string | null;
  thumbnailUrl: string | null;
  mediaType: 'image' | 'carousel' | 'reel' | 'unknown';
  postedAt: string | null;
  handle: string | null;
  firstCheckpoint: string | null;
  firstPercentile: number | null;
  latestCheckpoint: string;
  latestBusinessDayIst: string | null;
  latestPercentile: number | null;
  views: number | null;
  likes: number | null;
  comments: number | null;
  engagementRate: number | null;
  rankingMetric: string | null;
  rankingMultiple: number | null;
  likesMultiple: number | null;
  commentsMultiple: number | null;
  engagementRateMultiple: number | null;
};

type MediaBreakdown = {
  reel: number;
  carousel: number;
  image: number;
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
    const canonical = await sb
      .from('post_metrics')
      .select('post_key,checkpoint,business_date_ist,computed_at,views,likes,comments,engagement_rate,percentile_performance,percentile_performance_exact,ranking_metric,ranking_multiple,likes_multiple,comments_multiple,engagement_rate_multiple')
      .in('post_key', chunk)
      .in('checkpoint', [...TRACKING_CHECKPOINTS]);

    if (!canonical.error) {
      rows.push(...((canonical.data || []) as PostMetricRow[]));
      continue;
    }

    const legacy = await sb
      .from('post_metrics')
      .select('post_key,checkpoint,business_date_ist,computed_at,views,likes,comments,percentile_performance')
      .in('post_key', chunk)
      .in('checkpoint', [...TRACKING_CHECKPOINTS]);
    if (legacy.error) throw legacy.error;
    rows.push(...((legacy.data || []) as PostMetricRow[]));
  }

  return rows;
}

function daysForTimeframe(value: string | null) {
  const parsed = Number.parseInt(String(value || '').replace(/\D/g, ''), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 30;
}

function compactNumber(value: number) {
  if (!Number.isFinite(value) || value <= 0) return '0';
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(value >= 10_000_000 ? 0 : 1)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(value >= 100_000 ? 0 : 1)}K`;
  return String(Math.round(value));
}

function metricValue(value: number | null | undefined) {
  return Number.isFinite(Number(value)) ? Number(value) : 0;
}

function postEngagement(post: TrackedPost) {
  return metricValue(post.comments) * 6 + metricValue(post.likes) + metricValue(post.views) * 0.04;
}

function mediaBreakdown(posts: TrackedPost[]): MediaBreakdown {
  return posts.reduce<MediaBreakdown>((counts, post) => {
    if (post.mediaType === 'reel') counts.reel += 1;
    if (post.mediaType === 'carousel') counts.carousel += 1;
    if (post.mediaType === 'image') counts.image += 1;
    return counts;
  }, { reel: 0, carousel: 0, image: 0 });
}

function stableVariantIndex(subject: string, count: number) {
  if (count <= 1) return 0;
  let hash = 0;
  for (let index = 0; index < subject.length; index += 1) {
    hash = (hash * 31 + subject.charCodeAt(index)) % 997;
  }
  return hash % count;
}

function pickLine(subject: string, variants: string[]) {
  return variants[stableVariantIndex(subject, variants.length)] || variants[0] || '';
}

function subjectName(value: string) {
  return value.trim() || 'This row';
}

function postHandle(post: TrackedPost | null) {
  return post?.handle ? `@${post.handle.replace(/^@+/, '')}` : 'one post';
}

function insightPeriodName(timeframeDays: number) {
  if (timeframeDays <= 7) return 'week';
  if (timeframeDays <= 31) return 'month';
  return `${timeframeDays}D window`;
}

function dominantMediaType(breakdown: MediaBreakdown) {
  const entries: Array<[string, number]> = [
    ['reels', breakdown.reel],
    ['carousels', breakdown.carousel],
    ['images', breakdown.image],
  ];
  return entries.sort((a, b) => b[1] - a[1])[0];
}

function buildMovementInsight(posts: TrackedPost[], timeframeDays: number, subject: string) {
  const subjectLabel = subjectName(subject);
  const periodName = insightPeriodName(timeframeDays);
  const now = Date.now();
  const windowMs = timeframeDays * 24 * 60 * 60 * 1000;
  const since = now - windowMs;
  const previousSince = now - windowMs * 2;
  const current = posts.filter((post) => {
    const time = parseIsoTime(post.postedAt);
    return time === 0 || time >= since;
  });
  const previous = posts.filter((post) => {
    const time = parseIsoTime(post.postedAt);
    return time > 0 && time >= previousSince && time < since;
  });
  const totalComments = current.reduce((sum, post) => sum + metricValue(post.comments), 0);
  const totalLikes = current.reduce((sum, post) => sum + metricValue(post.likes), 0);
  const previousEngagement = previous.reduce((sum, post) => sum + postEngagement(post), 0);
  const currentEngagement = current.reduce((sum, post) => sum + postEngagement(post), 0);
  const bestByComments = [...current].sort((a, b) => metricValue(b.comments) - metricValue(a.comments))[0] || null;
  const bestByLikes = [...current].sort((a, b) => metricValue(b.likes) - metricValue(a.likes))[0] || null;
  const bestByEngagement = [...current].sort((a, b) => postEngagement(b) - postEngagement(a))[0] || null;
  const bestPercentile = [...current]
    .filter((post) => post.latestPercentile != null)
    .sort((a, b) => metricValue(a.latestPercentile) - metricValue(b.latestPercentile))[0] || null;
  const topCommentShare = totalComments > 0 && bestByComments
    ? metricValue(bestByComments.comments) / totalComments
    : 0;
  const topEngagementShare = currentEngagement > 0 && bestByEngagement
    ? postEngagement(bestByEngagement) / currentEngagement
    : 0;
  const topLikeShare = totalLikes > 0 && bestByLikes
    ? metricValue(bestByLikes.likes) / totalLikes
    : 0;
  const bestPercentileValue = bestPercentile ? metricValue(bestPercentile.latestPercentile) : 0;
  const postsPerWeek = current.length / Math.max(1, timeframeDays / 7);
  const avgPercentile = current.length > 0
    ? current.reduce((sum, post) => sum + metricValue(post.latestPercentile), 0) / current.length
    : 0;
  const breakdown = mediaBreakdown(current);
  const dominantMedia = dominantMediaType(breakdown);
  const dominantMediaShare = current.length > 0 ? dominantMedia[1] / current.length : 0;
  type InsightCandidate = {
    score: number;
    headline: string;
    subline: string;
    tone: 'winner' | 'risk' | 'steady' | 'volatile';
  };

  const candidates: InsightCandidate[] = [];
  const addCandidate = (condition: boolean, candidate: InsightCandidate) => {
    if (condition && Number.isFinite(candidate.score)) candidates.push(candidate);
  };

  addCandidate(current.length === 0, {
    score: 100,
    headline: `${subjectLabel} has no post evidence in this window yet.`,
    subline: 'Once tracked posts land here, this row will show what changed.',
    tone: 'risk',
  });

  addCandidate(current.length > 0 && current.length <= 2, {
    score: 92 - current.length,
    headline: pickLine(subjectLabel, [
      `${subjectLabel} is working off a very small sample.`,
      `${subjectLabel} does not have much posted in this window.`,
      `${subjectLabel} is hard to read from volume alone.`,
    ]),
    subline: `${current.length} tracked ${current.length === 1 ? 'post is' : 'posts are'} carrying the current picture.`,
    tone: 'volatile',
  });

  addCandidate(topCommentShare >= 0.55 && Boolean(bestByComments), {
    score: 84 + topCommentShare * 12,
    headline: pickLine(subjectLabel, [
      `Most comments for ${subjectLabel} came from one post.`,
      `${subjectLabel}'s comments are bunched around one post.`,
      `One post is doing the comment work for ${subjectLabel}.`,
    ]),
    subline: `${postHandle(bestByComments)} has ${Math.round(topCommentShare * 100)}% of comments in this window.`,
    tone: 'volatile',
  });

  addCandidate(topLikeShare >= 0.6 && Boolean(bestByLikes) && bestByLikes?.postKey !== bestByComments?.postKey, {
    score: 81 + topLikeShare * 10,
    headline: pickLine(subjectLabel, [
      `Likes are clustering around one post for ${subjectLabel}.`,
      `${subjectLabel} has one post pulling the like count up.`,
      `One post is doing most of the like work for ${subjectLabel}.`,
    ]),
    subline: `${postHandle(bestByLikes)} has ${Math.round(topLikeShare * 100)}% of likes in this ${periodName}.`,
    tone: 'steady',
  });

  addCandidate(topEngagementShare >= 0.46 && Boolean(bestByEngagement), {
    score: 74 + topEngagementShare * 10,
    headline: pickLine(subjectLabel, [
      `${subjectLabel} is leaning on one standout post.`,
      `One post is carrying most of ${subjectLabel}'s response.`,
      `${subjectLabel} has one clear spike in this ${periodName}.`,
    ]),
    subline: `${postHandle(bestByEngagement)} is taking ${Math.round(topEngagementShare * 100)}% of the response here.`,
    tone: 'volatile',
  });

  addCandidate(Boolean(bestPercentile) && bestPercentileValue <= 5, {
    score: 72 + (5 - bestPercentileValue) * 3,
    headline: pickLine(subjectLabel, [
      `${subjectLabel} has one post clearly ahead of the rest.`,
      `${subjectLabel}'s best post is the cleanest signal here.`,
      `The strongest post for ${subjectLabel} is well above the pack.`,
    ]),
    subline: `${postHandle(bestPercentile)} reached top ${Math.round(bestPercentileValue)}% in this window.`,
    tone: 'winner',
  });

  addCandidate(previousEngagement > 0 && currentEngagement < previousEngagement * 0.72, {
    score: 68 + (1 - currentEngagement / previousEngagement) * 20,
    headline: pickLine(subjectLabel, [
      `${subjectLabel} has cooled off versus the prior window.`,
      `${subjectLabel} is quieter than the last window.`,
      `${subjectLabel}'s current posts are not landing as strongly as before.`,
    ]),
    subline: `This ${periodName} is at ${Math.round((currentEngagement / previousEngagement) * 100)}% of the prior response pace.`,
    tone: 'risk',
  });

  addCandidate(previousEngagement > 0 && currentEngagement > previousEngagement * 1.28, {
    score: 66 + ((currentEngagement / previousEngagement) - 1) * 12,
    headline: pickLine(subjectLabel, [
      `${subjectLabel} picked up compared with the prior window.`,
      `${subjectLabel} is getting more response than last window.`,
      `${subjectLabel}'s recent posts are landing better than before.`,
    ]),
    subline: `The response pace is up ${Math.round(((currentEngagement - previousEngagement) / previousEngagement) * 100)}%.`,
    tone: 'winner',
  });

  addCandidate(dominantMediaShare >= 0.65 && current.length >= 4, {
    score: 64 + dominantMediaShare * 10,
    headline: pickLine(subjectLabel, [
      `${subjectLabel} is mostly a ${dominantMedia[0]} read right now.`,
      `${subjectLabel}'s posts are leaning heavily toward ${dominantMedia[0]}.`,
      `${dominantMedia[0]} are shaping the read for ${subjectLabel}.`,
    ]),
    subline: `${dominantMedia[1]} of ${current.length} tracked posts in this ${periodName} are ${dominantMedia[0]}.`,
    tone: 'steady',
  });

  addCandidate(postsPerWeek >= 8 && avgPercentile > 35, {
    score: 62 + Math.min(16, postsPerWeek),
    headline: pickLine(subjectLabel, [
      `${subjectLabel} is posting often, but the depth is softer.`,
      `${subjectLabel} has volume, not enough bite yet.`,
      `${subjectLabel}'s output is high, but fewer posts are breaking through.`,
    ]),
    subline: `${postsPerWeek.toFixed(1)} posts per week are averaging top ${Math.round(avgPercentile)}%.`,
    tone: 'risk',
  });

  addCandidate(Boolean(bestPercentile) && bestPercentileValue <= 15 && avgPercentile <= 24, {
    score: 58 + (24 - avgPercentile),
    headline: pickLine(subjectLabel, [
      `${subjectLabel} looks consistently healthy this window.`,
      `${subjectLabel} has more than one post doing its job.`,
      `${subjectLabel}'s quality is fairly even right now.`,
    ]),
    subline: `${current.length} posts average top ${Math.round(avgPercentile)}%, with the best at top ${Math.round(bestPercentileValue)}%.`,
    tone: 'winner',
  });

  addCandidate(Boolean(bestByEngagement), {
    score: 20,
    headline: pickLine(subjectLabel, [
      `${subjectLabel} has a balanced post leading the window.`,
      `${subjectLabel}'s strongest post is doing a bit of everything.`,
      `${subjectLabel} is getting its clearest read from one balanced post.`,
    ]),
    subline: `${postHandle(bestByEngagement)} is the best all-round example in this ${periodName}.`,
    tone: 'steady',
  });

  const selected = candidates.sort((a, b) => b.score - a.score)[0] || {
    headline: `${subjectLabel} is steady, without a loud signal yet.`,
    subline: `${current.length} tracked posts are spread fairly evenly.`,
    tone: 'steady' as const,
  };

  return {
    headline: selected.headline,
    subline: selected.subline,
    tone: selected.tone,
    mediaBreakdown: breakdown,
    metrics: [
      { label: 'posts per week', value: postsPerWeek.toFixed(postsPerWeek >= 10 ? 0 : 1), detail: `R ${breakdown.reel} / C ${breakdown.carousel} / I ${breakdown.image}` },
      { label: 'avg', value: avgPercentile > 0 ? `${Math.round(avgPercentile)}%` : '--', detail: 'window depth' },
      { label: 'top posts', value: String(current.filter((post) => metricValue(post.latestPercentile) <= 20).length), detail: 'top 20% posts' },
      { label: 'tracked', value: compactNumber(current.length), detail: `${compactNumber(totalComments)} comments / ${compactNumber(totalLikes)} likes` },
    ],
  };
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
    const timeframeDays = daysForTimeframe(request.nextUrl.searchParams.get('timeframe'));
    const groupScope = handle === 'all';

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

    const feederQuery = sb
      .from('feeders')
      .select('id,feed_id,handle,profile_pic_url,follower_count,status')
      .eq('feed_id', feedId)
      .eq('status', 'active');

    const { data: feederRowsData, error: feederError } = groupScope
      ? await feederQuery.order('handle', { ascending: true })
      : await feederQuery.eq('handle', handle);

    if (feederError) throw feederError;

    const feederRows = (feederRowsData || []) as FeederRow[];
    const feederRow = feederRows[0] || null;
    if (!groupScope && !feederRow) {
      return NextResponse.json({ error: 'Feeder not found' }, { status: 404 });
    }
    if (groupScope && feederRows.length === 0) {
      return NextResponse.json({ error: 'No active feeders found' }, { status: 404 });
    }

    const feederIds = feederRows.map((row) => row.id);
    const feederById = new Map(feederRows.map((row) => [row.id, row]));

    const postsQuery = sb
      .from('posts')
      .select('post_key,feeder_id,media_type,posted_at,post_url,thumbnail_url')
      .order('posted_at', { ascending: false });
    const { data: postsData, error: postsError } = groupScope
      ? await postsQuery.in('feeder_id', feederIds)
      : await postsQuery.eq('feeder_id', feederRow.id);

    if (postsError) throw postsError;

    const posts = (postsData || []) as PostRow[];
    const postKeys = posts
      .map((row) => nullableString(row.post_key))
      .filter((value): value is string => Boolean(value));

    const metricRows = await fetchMetricRowsForPostKeys(sb, postKeys);
    const latestMetricByPost = new Map<string, PostMetricRow & { checkpointRank: number }>();
    const earliestMetricByPost = new Map<string, PostMetricRow & { checkpointRank: number }>();

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
      const earliest = earliestMetricByPost.get(postKey);
      if (
        !earliest
        || checkpointRank < earliest.checkpointRank
        || (checkpointRank === earliest.checkpointRank && parseIsoTime(row.computed_at) < parseIsoTime(earliest.computed_at))
      ) {
        earliestMetricByPost.set(postKey, { ...row, checkpointRank });
      }
    }

    const trackedPosts: TrackedPost[] = posts
      .map((post) => {
        const postKey = nullableString(post.post_key);
        if (!postKey) return null;

        const latestMetric = latestMetricByPost.get(postKey);
        if (!latestMetric) return null;
        const earliestMetric = earliestMetricByPost.get(postKey);
        const hasTrajectory = Boolean(earliestMetric) && earliestMetric!.checkpointRank < latestMetric.checkpointRank;

        return {
          postKey,
          postUrl: nullableString(post.post_url),
          thumbnailUrl: buildMediaProxyUrl(postKey, nullableString(post.thumbnail_url)),
          mediaType: normalizeMediaType(post.media_type),
          postedAt: nullableString(post.posted_at),
          handle: post.feeder_id != null ? feederById.get(post.feeder_id)?.handle ?? null : null,
          firstCheckpoint: hasTrajectory ? normalizeTrackingCheckpoint(earliestMetric!.checkpoint).toUpperCase() : null,
          firstPercentile: hasTrajectory
            ? nullableNumber(earliestMetric!.percentile_performance_exact) ?? nullableNumber(earliestMetric!.percentile_performance)
            : null,
          latestCheckpoint: normalizeTrackingCheckpoint(latestMetric.checkpoint).toUpperCase(),
          latestBusinessDayIst: nullableString(latestMetric.business_date_ist),
          latestPercentile: nullableNumber(latestMetric.percentile_performance_exact)
            ?? nullableNumber(latestMetric.percentile_performance),
          views: nullableNumber(latestMetric.views),
          likes: nullableNumber(latestMetric.likes),
          comments: nullableNumber(latestMetric.comments),
          engagementRate: nullableNumber(latestMetric.engagement_rate),
          rankingMetric: nullableString(latestMetric.ranking_metric),
          rankingMultiple: nullableNumber(latestMetric.ranking_multiple),
          likesMultiple: nullableNumber(latestMetric.likes_multiple),
          commentsMultiple: nullableNumber(latestMetric.comments_multiple),
          engagementRateMultiple: nullableNumber(latestMetric.engagement_rate_multiple),
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
        handle: groupScope ? 'all' : feederRow.handle,
        profilePicUrl: groupScope ? null : buildProfileImageProxyUrl(feederRow.profile_pic_url),
        followerCount: groupScope ? null : nullableNumber(feederRow.follower_count),
        trackedPosts: trackedPosts.length,
        topPercentile: percentileValues.length > 0 ? Math.min(...percentileValues) : null,
        groupScope,
        feederCount: feederRows.length,
      },
      movementInsight: buildMovementInsight(
        trackedPosts,
        timeframeDays,
        groupScope ? `${feedRow.name} feed` : `@${feederRow.handle}`,
      ),
      posts: trackedPosts,
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to load feeder posts';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
