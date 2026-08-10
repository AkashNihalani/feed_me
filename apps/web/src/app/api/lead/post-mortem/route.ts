import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@/lib/supabase/server';
import { buildPostMortemDeck } from '@/lib/feederboardFacts';

export const dynamic = 'force-dynamic';

type ClientPost = {
  postKey: string;
  postUrl: string | null;
  thumbnailUrl: string | null;
  mediaType: string;
  postedAt: string | null;
  handle: string | null;
  feedId?: string;
  feedName?: string;
  firstCheckpoint: string | null;
  firstPercentile: number | null;
  latestCheckpoint: string;
  latestPercentile: number | null;
  likes?: number | null;
  comments: number | null;
  rankingMultiple: number | null;
  likesMultiple?: number | null;
  commentsMultiple?: number | null;
};

export async function POST(request: NextRequest) {
  const supabase = await createClient();
  const { data: { user }, error } = await supabase.auth.getUser();
  if (error || !user) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });

  const body = await request.json().catch(() => null) as { posts?: ClientPost[]; days?: number } | null;
  const posts = Array.isArray(body?.posts) ? body.posts.slice(0, 500) : [];
  const days = Number.isFinite(body?.days) ? Math.max(1, Math.min(90, Number(body?.days))) : 30;
  const cutoff = Date.now() - days * 86_400_000;
  const windowPosts = posts.filter((post) => {
    const time = Date.parse(post.postedAt || '');
    return Number.isFinite(time) && time >= cutoff;
  });
  return NextResponse.json({ deck: buildPostMortemDeck(windowPosts, days) });
}
