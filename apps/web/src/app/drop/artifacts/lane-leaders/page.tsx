import type { Metadata } from 'next';
import LaneLeaders, { type LaneLeaderPost } from './LaneLeaders';
import { READER_FEEDERS } from '@/data/readerDrops';

export const metadata: Metadata = {
  title: 'Lane Leaders · Feed Me',
  description: 'A dual-state lane performance and weekly volume artifact.',
};

function laneLeaderPosts(): LaneLeaderPost[] {
  const seen = new Set<string>();
  const posts: LaneLeaderPost[] = [];

  for (const feeder of READER_FEEDERS) {
    for (const drop of feeder.drops) {
      const candidates = [
        ...(drop.stats.landing?.posts ?? []),
        ...drop.bites.flatMap((bite) => [...bite.evidence, ...bite.boundary]),
      ];
      for (const post of candidates) {
        if (!post.thumbnail || seen.has(post.title)) continue;
        seen.add(post.title);
        posts.push({ id: `${feeder.handle}:${post.title}`, title: post.title, thumbnail: post.thumbnail, url: post.url });
      }
    }
  }

  return posts.slice(0, 12);
}

export default function LaneLeadersPage() {
  return <LaneLeaders posts={laneLeaderPosts()} />;
}
