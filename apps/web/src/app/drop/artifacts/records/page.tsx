import type { Metadata } from 'next';
import Records, { type RecordPost } from './Records';
import { READER_FEEDERS } from '@/data/readerDrops';

export const metadata: Metadata = {
  title: 'Records Desk · Feed Me',
  description: 'Streaks, records and firsts — deterministic artifacts that print only when history breaks.',
};

function recordPosts(): RecordPost[] {
  const seen = new Set<string>();
  const posts: RecordPost[] = [];

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

export default function RecordsDeskPage() {
  return <Records posts={recordPosts()} />;
}
