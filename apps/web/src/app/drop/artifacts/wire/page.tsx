import type { Metadata } from 'next';
import Wire, { type WirePost } from './Wire';
import { READER_FEEDERS } from '@/data/readerDrops';

export const metadata: Metadata = {
  title: 'The Wire · Feed Me',
  description: 'Trigger artifacts beyond the D7 verdict — early heat, late jumps, holds, fades, splits, waves and displacements.',
};

function wirePosts(): WirePost[] {
  const seen = new Set<string>();
  const posts: WirePost[] = [];

  for (const feeder of READER_FEEDERS) {
    for (const drop of feeder.drops) {
      const candidates = [
        ...(drop.stats.landing?.posts ?? []),
        ...drop.bites.flatMap((bite) => [...bite.evidence, ...bite.boundary]),
        ...drop.changed.flatMap((change) => change.posts),
      ];

      for (const post of candidates) {
        if (seen.has(post.title) || !post.thumbnail) continue;
        seen.add(post.title);
        posts.push({ id: `${feeder.handle}:${post.title}`, title: post.title, thumbnail: post.thumbnail, url: post.url });
      }
    }
  }

  return posts.slice(0, 14);
}

export default function WirePage() {
  return <Wire posts={wirePosts()} />;
}
