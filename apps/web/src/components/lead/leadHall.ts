/* ─────────────────────────────────────────────────────────────
   LEAD HALL — what Lead's Hall of Feed stands on, shared so the
   Feed's profile can show the same win Lead shows first, for the
   same scope and window, and never a different one.

   - leadScope: the pick every tab shares, as Lead reads it (a feed
     Lead doesn't have, or none yet, is all feeds; a feeder counts
     only inside a feed).
   - hallBasis: the scope's posts the facts read: every post in the
     window by when it went out (postMoment), the feeder's alone on
     a feeder, then the first 500 dated in the window by their
     stored date, as the facts' API routes read them.
   - the win labels, and which facts aren't wins (a run of misses
     is worth knowing, the Feederboard's verdicts show it, but it
     isn't a win).

   The facts themselves are lib/feederboardFacts.
   ───────────────────────────────────────────────────────────── */

import { buildFeederboardFacts } from '@/lib/feederboardFacts';
import { normalizeHandle, titleCase } from '@/lib/feedLabels';
import { postMoment } from '@/lib/postMoment';
import type { LeadLoad } from '@/components/lead/leadData';

// the window the Hall opens on (Lead's own default range)
export const HALL_DAYS = 30;

// asked by Feed's Hall of Feed row (sessionStorage): Lead opens at its Hall of Feed
export const LEAD_AT_KEY = 'feedme:lead-at';

// a win's handle: what kind of thing happened, in a word or two
export const WIN_LABEL: Record<string, string> = {
  'above-streak': 'Hot streak',
  'checkpoint-climb': 'Climber',
  'busiest-day': 'Busiest day',
  'publishing-run': 'On a roll',
  'format-run': 'Format win',
  'format-time': 'Sweet spot',
  'post-concentration': 'Comment magnet',
  'feeder-participation': 'Team effort',
  'feeder-concentration': 'Carried the chat',
  'response-imbalance': 'Talking point',
  'quiet-breakout': 'Comeback',
};
export const NOT_A_WIN = new Set(['below-streak']);

export type HallFact = {
  kind: string;
  headline: string;
  detail: string;
  thumbnailUrl: string | null;
  postUrl: string | null;
  evidencePostKeys: string[];
};

type ScopeFeed = { id: string; feeders: Array<{ handle: string }> };
type HallFeed<P> = { id: string; title: string; posts: P[] };
type HallPost = { postedAt: string | null; postUrl: string | null; handle: string | null };

export function leadScope(feeds: ScopeFeed[], scope: { feedId: string | null | undefined; handle: string | null }) {
  const feedId = scope.feedId ?? (scope.handle
    ? feeds.find((feed) => feed.feeders.some((feeder) => normalizeHandle(feeder.handle) === scope.handle))?.id ?? null
    : null);
  const activeFeedId = feedId && feeds.some((feed) => feed.id === feedId) ? feedId : 'all';
  return { activeFeedId, activeFeeder: activeFeedId !== 'all' && scope.handle ? scope.handle : '' };
}

const momentOf = (post: HallPost) => postMoment(post.postedAt, post.postUrl) ?? 0;

export function hallBasis<P extends HallPost>(feeds: Array<HallFeed<P>>, activeFeedId: string, feederHandle: string, days: number, timeOf: (post: P) => number = momentOf) {
  const cutoff = Date.now() - days * 86_400_000;
  const scoped = (activeFeedId === 'all' ? feeds : feeds.filter((feed) => feed.id === activeFeedId))
    .flatMap((feed) => feed.posts
      .filter((post) => {
        const time = timeOf(post);
        return time === 0 || time >= cutoff;
      })
      .map((post) => ({ ...post, feedName: titleCase(feed.title) })));
  const posts = feederHandle ? scoped.filter((post) => normalizeHandle(post.handle) === feederHandle) : scoped;
  return posts.slice(0, 500).filter((post) => {
    const time = Date.parse(post.postedAt || '');
    return Number.isFinite(time) && time >= cutoff;
  });
}

/* the Hall's wins for the pick every tab shares, from Lead's load, in the order Lead's Hall shows them */
const winsMade = new WeakMap<LeadLoad, Map<string, HallFact[]>>();
export function hallWins(load: LeadLoad, scope: { feedId: string | null | undefined; handle: string | null }, days = HALL_DAYS): HallFact[] {
  const feeds = load.feeds.map((feed) => ({
    id: feed.id,
    title: String(feed.title ?? ''),
    feeders: (Array.isArray(feed.feeders) ? feed.feeders : []) as Array<{ handle: string }>,
    posts: feed.posts as HallPost[],
  }));
  const { activeFeedId, activeFeeder } = leadScope(feeds, scope);
  const key = `${activeFeedId}|${activeFeeder}|${days}`;
  let made = winsMade.get(load);
  if (!made) winsMade.set(load, (made = new Map()));
  const known = made.get(key);
  if (known) return known;
  const facts = (buildFeederboardFacts(hallBasis(feeds, activeFeedId, activeFeeder, days), days) as HallFact[]).filter((fact) => !NOT_A_WIN.has(fact.kind));
  made.set(key, facts);
  return facts;
}
