/* A post's views, wherever the app has them. The app's own reads (post_metrics.views, the `views` on a tracked post)
   come first; Bright Data stopped sending views in July 2026, so until the worker stores them again the plays Apify
   counted on 27 Oct 2026 (25 reels each of @anuj.mp4 and @lakmeindia, data/apifyPlays.json) fill in by shortcode.
   A post with neither has no views, and Lead says nothing about them. */

import apify from '@/data/apifyPlays.json';

const PLAYS: Record<string, number> = apify.plays;

function shortcodeOf(url: string | null) {
  return (url && /\/(?:p|reel|tv)\/([A-Za-z0-9_-]+)/i.exec(url)?.[1]) || null;
}

export function viewsOf(post: { views?: number | null; postUrl?: string | null }): number | null {
  const own = Number(post.views);
  if (Number.isFinite(own) && own > 0) return own;
  const code = shortcodeOf(post.postUrl ?? null);
  return (code && PLAYS[code]) || null;
}

/* where a post's views came from, so a single read can say when they were counted */
export function viewsNote(post: { views?: number | null; postUrl?: string | null }) {
  const own = Number(post.views);
  if (Number.isFinite(own) && own > 0) return 'latest read';
  return viewsOf(post) == null ? null : 'counted 27 Oct';
}

/* "1.4M views", or nothing */
export function viewsLabel(post: { views?: number | null; postUrl?: string | null }) {
  const views = viewsOf(post);
  if (views == null) return null;
  const label = views >= 1_000_000
    ? `${(views / 1_000_000).toFixed(views >= 10_000_000 ? 0 : 1).replace(/\.0$/, '')}M`
    : views >= 1_000
      ? `${(views / 1_000).toFixed(views >= 100_000 ? 0 : 1).replace(/\.0$/, '')}K`
      : String(Math.round(views));
  return `${label} views`;
}
