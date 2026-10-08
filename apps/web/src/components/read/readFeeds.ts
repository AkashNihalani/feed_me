/* ─────────────────────────────────────────────────────────────
   READ FEEDS — what Read's story rail navigates: your feeds and
   their feeders, from the same /api/feed that Lead reads, each
   feeder marked with whether Read has a read for it yet.

   Reads exist for the sample feeders in data/readerPreview.json
   only. A sample feeder your feeds don't track still has to be
   reachable, so those are gathered into one extra "Samples" feed.
   ───────────────────────────────────────────────────────────── */

import readerData from '@/data/readerPreview.json';
import { normalizeHandle } from '@/lib/feedLabels';

// the app's names for feeds and feeders (the header uses the same ones)
export { feedInitials, normalizeHandle, titleCase } from '@/lib/feedLabels';

export type RailFeeder = {
  handle: string;
  profilePicUrl: string | null;
  // index into the reader data, or null when Read has nothing on this feeder yet
  read: number | null;
};

export type RailFeed = { id: string; title: string; feeders: RailFeeder[] };

type ApiFeed = {
  id?: string | null;
  title?: string | null;
  feeders?: { handle?: string | null; profilePicUrl?: string | null }[] | null;
};

const READ_FEEDERS = (readerData as { feeders: { handle: string; posts: unknown[] }[] }).feeders;
const READ_HANDLES = READ_FEEDERS.map((feeder) => normalizeHandle(feeder.handle));

export const READ_INDEX = new Map(READ_HANDLES.map((handle, index) => [handle, index] as const));
// how many posts each read covers (the engine reports the live number once it is showing one)
export const READ_POSTS = READ_FEEDERS.map((feeder) => feeder.posts.length);
export const FIRST_READ_HANDLE = READ_HANDLES[0] || null;
export const SAMPLE_FEED_ID = 'read:samples';

export function readOf(handle: string | null | undefined) {
  if (!handle) return null;
  return READ_INDEX.get(normalizeHandle(handle)) ?? null;
}

/* the feeders Read has a read for, as rail circles (before the feeds have loaded, and on the index pages) */
export function readFeeders(feeds: RailFeed[] | null): RailFeeder[] {
  const pics = new Map<string, string>();
  feeds?.forEach((feed) => feed.feeders.forEach((feeder) => {
    if (feeder.profilePicUrl && !pics.has(feeder.handle)) pics.set(feeder.handle, feeder.profilePicUrl);
  }));
  return READ_HANDLES.map((handle, index) => ({ handle, profilePicUrl: pics.get(handle) || null, read: index }));
}

export function buildRailFeeds(apiFeeds: ApiFeed[]): RailFeed[] {
  const feeds: RailFeed[] = apiFeeds
    .filter((feed) => Boolean(feed.id))
    .map((feed) => {
      const seen = new Set<string>();
      const feeders: RailFeeder[] = [];
      (feed.feeders || []).forEach((feeder) => {
        const handle = normalizeHandle(feeder.handle);
        if (!handle || seen.has(handle)) return;
        seen.add(handle);
        feeders.push({ handle, profilePicUrl: feeder.profilePicUrl || null, read: READ_INDEX.get(handle) ?? null });
      });
      return { id: String(feed.id), title: String(feed.title || 'Feed'), feeders };
    });
  const tracked = new Set(feeds.flatMap((feed) => feed.feeders.map((feeder) => feeder.handle)));
  const untracked = READ_HANDLES.filter((handle) => !tracked.has(handle));
  if (untracked.length) {
    feeds.push({
      id: SAMPLE_FEED_ID,
      title: 'Samples',
      feeders: untracked.map((handle) => ({ handle, profilePicUrl: null, read: READ_INDEX.get(handle) ?? null })),
    });
  }
  return feeds;
}

/* the feed a feeder sits in (the first, if it sits in several) */
export function feedOf(feeds: RailFeed[], handle: string | null) {
  if (!handle) return null;
  return feeds.find((feed) => feed.feeders.some((feeder) => feeder.handle === handle))?.id ?? null;
}

/* fetched once per visit to the app and kept for the tab's next mount; a failed or signed-out fetch still
   leaves the samples reachable */
let feedCache: RailFeed[] | null = null;

export function cachedFeeds() {
  return feedCache;
}

export async function loadFeeds(signal: AbortSignal): Promise<RailFeed[]> {
  try {
    const response = await fetch('/api/feed', { cache: 'no-store', credentials: 'include', signal });
    const payload = response.ok ? (await response.json()) as { feeds?: ApiFeed[] } : null;
    feedCache = buildRailFeeds(payload?.feeds || []);
  } catch (error) {
    if (signal.aborted) throw error;
    feedCache = feedCache || buildRailFeeds([]);
  }
  return feedCache;
}
