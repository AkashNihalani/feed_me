import { NextRequest, NextResponse } from 'next/server';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient } from '@/lib/supabase/server';

type FeedRow = {
  id: number;
  user_id: string;
  name: string;
  status: string;
  created_at: string;
};

type FeederRow = {
  id: number;
  feed_id: number;
  handle: string;
  role: 'anchor' | 'standard';
  status: string;
  created_at: string;
  profile_pic_url: string | null;
  follower_count: number | null;
};

type PostRow = {
  post_key: string;
  feeder_id: number;
  posted_at: string | null;
};

type MetricRow = {
  post_key: string;
  checkpoint: string;
  likes: number | null;
  comments: number | null;
  views: number | null;
  computed_at: string;
};

type TickerRow = {
  id: string;
  handle: string;
  likesDelta: number;
  commentsDelta: number;
  viewsDelta: number;
  postsDelta: number;
};

type InstagramProfileProbe = {
  ok: boolean;
  profilePicUrl: string | null;
  followerCount: number | null;
  error?: string;
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

function normalizeHandle(raw: string) {
  return (raw || '').trim().replace(/^@+/, '').toLowerCase();
}

function normalizeUrl(url: string | null | undefined) {
  if (!url) return null;
  const trimmed = String(url).trim();
  return trimmed ? trimmed : null;
}

function isBadInstagramImageUrl(url: string | null | undefined) {
  if (!url) return true;
  const u = String(url).toLowerCase();
  if (u.includes('static.cdninstagram.com/rsrc.php')) return true;
  if (u.includes('/rsrc.php/')) return true;
  return false;
}

function toMetricString(value: number) {
  return String(Math.max(0, Math.floor(value)));
}

function parseIsoTime(value: string | null | undefined) {
  if (!value) return NaN;
  return Date.parse(value);
}

function isTrackedPost(post: PostRow, feeder: FeederRow, checkpointsByPost: Map<string, Set<string>>) {
  const checkpoints = checkpointsByPost.get(post.post_key);
  if (!checkpoints?.has('d1')) return false;

  const postedAt = parseIsoTime(post.posted_at);
  const trackingStartedAt = parseIsoTime(feeder.created_at);
  if (!Number.isFinite(postedAt) || !Number.isFinite(trackingStartedAt)) return false;
  return postedAt >= trackingStartedAt;
}

async function fetchInstagramWebProfile(handle: string): Promise<InstagramProfileProbe | null> {
  try {
    const res = await fetch(
      `https://www.instagram.com/api/v1/users/web_profile_info/?username=${encodeURIComponent(handle)}`,
      {
        method: 'GET',
        cache: 'no-store',
        headers: {
          'x-ig-app-id': '936619743392459',
          'user-agent': 'Mozilla/5.0',
          accept: 'application/json',
        },
      }
    );
    if (!res.ok) return null;

    const json = await res.json();
    const user = json?.data?.user;
    if (!user) return null;

    const uname = String(user.username || '').replace(/^@+/, '').toLowerCase();
    if (uname !== handle.toLowerCase()) {
      return { ok: false, profilePicUrl: null, followerCount: null, error: 'Handle verification failed' };
    }

    const followerRaw = user?.edge_followed_by?.count;
    const followerCount =
      followerRaw === null || followerRaw === undefined || followerRaw === ''
        ? null
        : Number.isFinite(Number(followerRaw))
          ? Number(followerRaw)
          : null;

    return {
      ok: true,
      profilePicUrl: (() => {
        const candidate = normalizeUrl(String(user.profile_pic_url_hd || user.profile_pic_url || ''));
        return isBadInstagramImageUrl(candidate) ? null : candidate;
      })(),
      followerCount,
    };
  } catch {
    return null;
  }
}

async function probeInstagramHandleQuick(handle: string): Promise<InstagramProfileProbe> {
  const webProfile = await fetchInstagramWebProfile(handle);
  if (webProfile?.ok) return webProfile;
  return webProfile || { ok: false, profilePicUrl: null, followerCount: null, error: 'Profile probe unavailable' };
}



function buildTickerItems(feeders: FeederRow[], posts: PostRow[], metrics: MetricRow[], trackedPostKeys: Set<string>): TickerRow[] {
  const feederById = new Map<number, FeederRow>(feeders.map((feeder) => [feeder.id, feeder]));
  const postByKey = new Map<string, PostRow>(posts.map((post) => [post.post_key, post]));
  const metricsByPost = new Map<string, MetricRow[]>();

  for (const metric of metrics) {
    const arr = metricsByPost.get(metric.post_key) || [];
    arr.push(metric);
    metricsByPost.set(metric.post_key, arr);
  }

  const now = Date.now();
  const sevenDaysAgo = now - 7 * 24 * 60 * 60 * 1000;
  const tickerByHandle = new Map<string, TickerRow>();

  for (const [postKey, postMetrics] of metricsByPost.entries()) {
    if (!trackedPostKeys.has(postKey)) continue;
    const post = postByKey.get(postKey);
    const feeder = post ? feederById.get(post.feeder_id) : null;
    if (!post || !feeder) continue;

    const sorted = [...postMetrics].sort((a, b) => Date.parse(b.computed_at) - Date.parse(a.computed_at));
    const latest = sorted[0];
    const previous = sorted[1] || null;

    const row = tickerByHandle.get(feeder.handle) || {
      id: String(feeder.id),
      handle: `@${feeder.handle}`,
      likesDelta: 0,
      commentsDelta: 0,
      viewsDelta: 0,
      postsDelta: 0,
    };

    row.likesDelta += (latest?.likes || 0) - (previous?.likes || 0);
    row.commentsDelta += (latest?.comments || 0) - (previous?.comments || 0);
    row.viewsDelta += (latest?.views || 0) - (previous?.views || 0);

    const postedAt = post.posted_at ? Date.parse(post.posted_at) : NaN;
    if (Number.isFinite(postedAt) && postedAt >= sevenDaysAgo) {
      row.postsDelta += 1;
    }

    tickerByHandle.set(feeder.handle, row);
  }

  return [...tickerByHandle.values()]
    .sort((a, b) => {
      const scoreA = Math.abs(a.likesDelta) + Math.abs(a.commentsDelta) + Math.abs(a.viewsDelta) * 0.01 + Math.abs(a.postsDelta) * 25;
      const scoreB = Math.abs(b.likesDelta) + Math.abs(b.commentsDelta) + Math.abs(b.viewsDelta) * 0.01 + Math.abs(b.postsDelta) * 25;
      return scoreB - scoreA;
    })
    .slice(0, 8);
}

async function getFeedBundle(userId: string) {
  const sb = adminClient();

  const { data: feedsData, error: feedsError } = await sb
    .from('feeds')
    .select('id,user_id,name,status,created_at')
    .eq('user_id', userId)
    .eq('status', 'active')
    .order('created_at', { ascending: false });
  if (feedsError) throw feedsError;

  const feeds = (feedsData || []) as FeedRow[];
  if (feeds.length === 0) return [];

  const feedIds = feeds.map((f) => f.id);

  const { data: feederData, error: feederError } = await sb
    .from('feeders')
    .select('id,feed_id,handle,role,status,created_at,profile_pic_url,follower_count')
    .in('feed_id', feedIds)
    .eq('status', 'active');
  if (feederError) throw feederError;

  const feeders = (feederData || []) as FeederRow[];
  const feederIds = feeders.map((f) => f.id);

  let posts: PostRow[] = [];
  let metrics: MetricRow[] = [];

  if (feederIds.length > 0) {
    const { data: postsData, error: postsError } = await sb
      .from('posts')
      .select('post_key,feeder_id,posted_at')
      .in('feeder_id', feederIds);
    if (postsError) throw postsError;
    posts = (postsData || []) as PostRow[];

    const postKeys = posts.map((p) => p.post_key);
    if (postKeys.length > 0) {
      const { data: metricsData, error: metricsError } = await sb
        .from('post_metrics')
        .select('post_key,checkpoint,likes,comments,views,computed_at')
        .in('post_key', postKeys)
        .order('computed_at', { ascending: false });
      if (metricsError) throw metricsError;
      metrics = (metricsData || []) as MetricRow[];
    }
  }

  const latestMetricByPost = new Map<string, MetricRow>();
  const checkpointsByPost = new Map<string, Set<string>>();
  for (const m of metrics) {
    const checkpoints = checkpointsByPost.get(m.post_key) || new Set<string>();
    checkpoints.add(String(m.checkpoint || '').toLowerCase());
    checkpointsByPost.set(m.post_key, checkpoints);
    if (!latestMetricByPost.has(m.post_key)) {
      latestMetricByPost.set(m.post_key, m);
    }
  }

  const feederById = new Map<number, FeederRow>(feeders.map((feeder) => [feeder.id, feeder]));
  const postByKey = new Map<string, PostRow>(posts.map((post) => [post.post_key, post]));

  const postKeysByFeeder = new Map<number, string[]>();
  for (const p of posts) {
    const arr = postKeysByFeeder.get(p.feeder_id) || [];
    arr.push(p.post_key);
    postKeysByFeeder.set(p.feeder_id, arr);
  }

  const feedersByFeed = new Map<number, FeederRow[]>();
  for (const f of feeders) {
    const arr = feedersByFeed.get(f.feed_id) || [];
    arr.push(f);
    feedersByFeed.set(f.feed_id, arr);
  }

  const trackedPostKeys = new Set<string>();
  for (const post of posts) {
    const feeder = feederById.get(post.feeder_id);
    if (!feeder) continue;
    if (isTrackedPost(post, feeder, checkpointsByPost)) {
      trackedPostKeys.add(post.post_key);
    }
  }

  const feederMetrics = new Map<number, { likes: number; comments: number; views: number; postsTracked: number }>();
  for (const feeder of feeders) {
    const keys = postKeysByFeeder.get(feeder.id) || [];
    let likes = 0;
    let comments = 0;
    let views = 0;
    let postsTracked = 0;

    for (const key of keys) {
      const post = postByKey.get(key);
      if (!post || !isTrackedPost(post, feeder, checkpointsByPost)) continue;
      const m = latestMetricByPost.get(key);
      if (!m) continue;
      likes += m.likes || 0;
      comments += m.comments || 0;
      views += m.views || 0;
      postsTracked += 1;
    }

    feederMetrics.set(feeder.id, { likes, comments, views, postsTracked });
  }

  const mappedFeeds = feeds.map((feed) => {
    const feedFeeders = feedersByFeed.get(feed.id) || [];

    let totalLikes = 0;
    let totalComments = 0;
    let totalViews = 0;
    let totalPosts = 0;

    const mappedFeeders = feedFeeders.map((feeder) => {
      const stats = feederMetrics.get(feeder.id) || { likes: 0, comments: 0, views: 0, postsTracked: 0 };
      totalLikes += stats.likes;
      totalComments += stats.comments;
      totalViews += stats.views;
      totalPosts += stats.postsTracked;

      return {
        handle: feeder.handle,
        isAnchor: feeder.role === 'anchor',
        profilePicUrl: feeder.profile_pic_url && !feeder.profile_pic_url.includes('unavatar.io/instagram') ? feeder.profile_pic_url : null,
        followerCount: feeder.follower_count,
        metrics: {
          likes: toMetricString(stats.likes),
          comments: toMetricString(stats.comments),
          views: toMetricString(stats.views),
          postsTracked: toMetricString(stats.postsTracked),
        },
      };
    });

    return {
      id: String(feed.id),
      title: feed.name.toUpperCase(),
      feeders: mappedFeeders,
      metrics: {
        likes: toMetricString(totalLikes),
        comments: toMetricString(totalComments),
        views: toMetricString(totalViews),
        postsTracked: toMetricString(totalPosts),
      },
    };
  });

  return {
    feeds: mappedFeeds,
    ticker: buildTickerItems(feeders, posts, metrics, trackedPostKeys),
  };
}

export async function GET() {
  try {
    const supabase = await createClient();
    const {
      data: { user },
      error: authError,
    } = await supabase.auth.getUser();

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const bundle = await getFeedBundle(user.id);
    return NextResponse.json(bundle);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to load feeds';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function POST(request: NextRequest) {
  try {
    const supabase = await createClient();
    const {
      data: { user },
      error: authError,
    } = await supabase.auth.getUser();

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const sb = adminClient();
    const body = await request.json();
    const action = String(body?.action || '');

    if (action === 'create_feed') {
      const title = String(body?.title || '').trim();
      if (!title) {
        return NextResponse.json({ error: 'Feed name is required' }, { status: 400 });
      }

      const { error } = await sb.from('feeds').insert({
        user_id: user.id,
        name: title,
        status: 'active',
      });
      if (error) throw error;

      const bundle = await getFeedBundle(user.id);
      return NextResponse.json(bundle);
    }

    if (action === 'add_feeder') {
      const feedId = Number(body?.feedId);
      const handle = normalizeHandle(String(body?.handle || ''));

      if (!feedId || !handle) {
        return NextResponse.json({ error: 'feedId and handle are required' }, { status: 400 });
      }

      const { data: feedRow, error: feedErr } = await sb
        .from('feeds')
        .select('id,user_id')
        .eq('id', feedId)
        .eq('user_id', user.id)
        .single();
      if (feedErr || !feedRow) {
        return NextResponse.json({ error: 'Feed not found' }, { status: 404 });
      }

      const { data: existing, error: existingErr } = await sb
        .from('feeders')
        .select('id')
        .eq('feed_id', feedId)
        .eq('status', 'active');
      if (existingErr) throw existingErr;
      if ((existing || []).length >= 15) {
        return NextResponse.json({ error: 'A feed can have max 15 feeders' }, { status: 400 });
      }

      const probe = await probeInstagramHandleQuick(handle).catch(() => ({ ok: false, profilePicUrl: null, followerCount: null, error: 'Profile probe failed' }));

      const feederRow: Record<string, unknown> = {
        feed_id: feedId,
        handle,
        role: 'standard',
        status: 'active',
      };

      if (probe.ok) {
        feederRow.profile_pic_url = probe.profilePicUrl;
        feederRow.follower_count = probe.followerCount;
      }

      const { data: upsertedFeeder, error: upsertErr } = await sb
        .from('feeders')
        .upsert(feederRow, { onConflict: 'feed_id,handle' })
        .select('id');
      if (upsertErr) throw upsertErr;

      const feederId = Number(upsertedFeeder?.[0]?.id || 0);
      if (feederId > 0) {
        const nowIso = new Date().toISOString();
        const { error: bootstrapErr } = await sb.rpc('bootstrap_feeder_jobs', {
          p_feeder_id: feederId,
          p_run_at: nowIso,
        });

        // Keep a safe fallback for partially migrated environments.
        if (bootstrapErr) {
          const { data: enqueueCount, error: enqueueErr } = await sb.rpc('enqueue_daily_job_for_feeder', {
            p_feeder_id: feederId,
            p_run_at: nowIso,
          });

          const { data: openJob, error: openJobErr } = await sb
            .from('run_jobs')
            .select('id')
            .eq('feeder_id', feederId)
            .eq('job_type', 'daily')
            .in('status', ['pending', 'running', 'retry'])
            .limit(1);
          if (openJobErr) throw openJobErr;

          if (!openJob || openJob.length === 0) {
            const { error: runJobErr } = await sb.from('run_jobs').insert({
              feeder_id: feederId,
              job_type: 'daily',
              status: 'pending',
              attempt: 0,
              next_run_at: nowIso,
              last_error: null,
            });
            if (runJobErr) throw runJobErr;
          }

          if (!enqueueErr && Number(enqueueCount || 0) > 0) {
            const followerRpcNames = [
              'enqueue_daily_follower_job_for_feeder',
              'enqueue_weekly_follower_job_for_feeder',
            ] as const;

            for (const rpcName of followerRpcNames) {
              const { error: followerErr } = await sb.rpc(rpcName, {
                p_feeder_id: feederId,
                p_run_at: nowIso,
              });
              if (!followerErr) break;
            }
          }
        }
      }

      const bundle = await getFeedBundle(user.id);
      return NextResponse.json(bundle);
    }

    if (action === 'make_anchor') {
      const feedId = Number(body?.feedId);
      const handle = normalizeHandle(String(body?.handle || ''));
      if (!feedId || !handle) {
        return NextResponse.json({ error: 'feedId and handle are required' }, { status: 400 });
      }

      const { data: feedRow, error: feedErr } = await sb
        .from('feeds')
        .select('id,user_id')
        .eq('id', feedId)
        .eq('user_id', user.id)
        .single();
      if (feedErr || !feedRow) return NextResponse.json({ error: 'Feed not found' }, { status: 404 });

      const { error: clearErr } = await sb
        .from('feeders')
        .update({ role: 'standard' })
        .eq('feed_id', feedId)
        .eq('status', 'active');
      if (clearErr) throw clearErr;

      const { error: anchorErr } = await sb
        .from('feeders')
        .update({ role: 'anchor' })
        .eq('feed_id', feedId)
        .eq('handle', handle)
        .eq('status', 'active');
      if (anchorErr) throw anchorErr;

      const bundle = await getFeedBundle(user.id);
      return NextResponse.json(bundle);
    }

    if (action === 'remove_feeder') {
      const feedId = Number(body?.feedId);
      const handle = normalizeHandle(String(body?.handle || ''));
      if (!feedId || !handle) {
        return NextResponse.json({ error: 'feedId and handle are required' }, { status: 400 });
      }

      const { data: feedRow, error: feedErr } = await sb
        .from('feeds')
        .select('id,user_id')
        .eq('id', feedId)
        .eq('user_id', user.id)
        .single();
      if (feedErr || !feedRow) return NextResponse.json({ error: 'Feed not found' }, { status: 404 });

      const { error } = await sb
        .from('feeders')
        .update({ status: 'removed', role: 'standard' })
        .eq('feed_id', feedId)
        .eq('handle', handle);
      if (error) throw error;

      const bundle = await getFeedBundle(user.id);
      return NextResponse.json(bundle);
    }

    if (action === 'delete_feed') {
      const feedId = Number(body?.feedId);
      if (!feedId) {
        return NextResponse.json({ error: 'feedId is required' }, { status: 400 });
      }

      const { data: feedRow, error: feedErr } = await sb
        .from('feeds')
        .select('id,user_id')
        .eq('id', feedId)
        .eq('user_id', user.id)
        .single();
      if (feedErr || !feedRow) {
        return NextResponse.json({ error: 'Feed not found' }, { status: 404 });
      }

      const { error: feederErr } = await sb
        .from('feeders')
        .update({ status: 'removed', role: 'standard' })
        .eq('feed_id', feedId)
        .eq('status', 'active');
      if (feederErr) throw feederErr;

      const { error: deleteErr } = await sb
        .from('feeds')
        .update({ status: 'archived' })
        .eq('id', feedId)
        .eq('user_id', user.id);
      if (deleteErr) throw deleteErr;

      const bundle = await getFeedBundle(user.id);
      return NextResponse.json(bundle);
    }

    return NextResponse.json({ error: 'Invalid action' }, { status: 400 });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to update feed';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
