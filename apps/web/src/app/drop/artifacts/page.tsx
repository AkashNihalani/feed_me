import type { Metadata } from 'next';
import ArtifactGallery, { type GalleryPost } from './ArtifactGallery';
import { READER_FEEDERS } from '@/data/readerDrops';

export const metadata: Metadata = {
  title: 'Metric Artifacts · Feed Me',
  description: 'Ten deterministic metric artifacts for the Feed Me Feeder Reader.',
};

function galleryPosts(): GalleryPost[] {
  const seen = new Set<string>();
  const posts: GalleryPost[] = [];

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

export default function ServerArtifactGalleryPage() {
  return <ArtifactGallery posts={galleryPosts()} />;
}
