import type {
  FeederVisitData,
  VisitCaseBeat,
  VisitReaderArtifact,
  VisitReaderHistory,
  VisitReaderPost,
} from '@/components/feed/FeederVisit';
import week1Output from './terraReaderRuns/anuj-w01-output.json';
import week1Request from './terraReaderRuns/anuj-w01-request.json';
import week2Output from './terraReaderRuns/anuj-w02-output.json';
import week2Request from './terraReaderRuns/anuj-w02-request.json';
import week3Output from './terraReaderRuns/anuj-w03-output.json';
import week3Request from './terraReaderRuns/anuj-w03-request.json';

type ReaderPost = {
  title: string;
  lane?: string;
  posted?: string;
  recent_rank?: string;
  overall_rank?: string;
};

type ReaderBite = {
  bite_id: string;
  display_rank: number;
  movement: string;
  title: string;
  current_read: string;
  changed_because: string;
  why_it_matters_now: string;
  evidence_refs: string[];
  counterevidence_refs: string[];
  reinterpretation: {
    old_read: string;
    new_read: string;
    evidence_refs: string[];
  } | null;
};

type ReaderOutput = {
  this_week: { header: string; tagline: string; summary: string };
  what_changed: { movement: string; detail: string; post_refs: string[] }[];
  bites: ReaderBite[];
  observations: { text: string; post_refs: string[] }[];
  feederverse_watch: { rank_pulse: string; signals: string[] };
  trigger_tags: { label: string; note: string; post_ref: string }[];
  next_watch: string;
};

type ReaderRequest = { current_posts: ReaderPost[] };

const FIXTURES = [
  { id: 'anuj-w01', output: week1Output as ReaderOutput, request: week1Request as ReaderRequest },
  { id: 'anuj-w02', output: week2Output as ReaderOutput, request: week2Request as ReaderRequest },
  { id: 'anuj-w03', output: week3Output as ReaderOutput, request: week3Request as ReaderRequest },
] as const;

function splitHeadline(headline: string): string[] {
  const words = headline.split(' ');
  const splitAt = Math.ceil(words.length / 2);
  return [words.slice(0, splitAt).join(' '), words.slice(splitAt).join(' ')].filter(Boolean);
}

function rankOf(value?: string): { rank: number; pool: number } {
  const match = value?.match(/^(\d+)\/(\d+)/);
  return match ? { rank: Number(match[1]), pool: Number(match[2]) } : { rank: 20, pool: 40 };
}

function rankPosition(value?: string) {
  const { rank, pool } = rankOf(value);
  return rank / pool;
}

function postedAgeDays(value?: string) {
  const match = value?.match(/^(\d+)\s+(day|week|month)s? ago$/);
  if (!match) return Number.POSITIVE_INFINITY;
  const count = Number(match[1]);
  return count * (match[2] === 'day' ? 1 : match[2] === 'week' ? 7 : 30);
}

function currentRunAverage(fixtureIndex: number) {
  const current = FIXTURES[fixtureIndex].request.current_posts;
  const previous = fixtureIndex > 0 ? FIXTURES[fixtureIndex - 1].request.current_posts : [];
  const previousTitles = new Set(previous.map((post) => post.title));
  let runPosts = current.filter((post) => !previousTitles.has(post.title));

  if (!runPosts.length || fixtureIndex === 0) {
    const newestAge = Math.min(...current.map((post) => postedAgeDays(post.posted)));
    runPosts = current.filter((post) => postedAgeDays(post.posted) === newestAge);
  }

  const positions = runPosts
    .map((post) => post.recent_rank || post.overall_rank)
    .filter((rank): rank is string => Boolean(rank))
    .map((rank) => rankPosition(rank) * 100);
  return positions.length ? Math.round(positions.reduce((sum, position) => sum + position, 0) / positions.length) : undefined;
}

function asArtifactPost(title: string, posts: Map<string, ReaderPost>): VisitReaderPost {
  const post = posts.get(title);
  return { title, recentRank: post?.recent_rank, overallRank: post?.overall_rank };
}

type ArtifactKind = VisitReaderArtifact['kind'];

function artifactFor(
  bite: ReaderBite,
  previous: ReaderBite | undefined,
  history: VisitReaderHistory[],
  posts: Map<string, ReaderPost>,
  used: Set<ArtifactKind>,
): VisitReaderArtifact {
  const previousRefs = new Set(previous?.evidence_refs || []);
  const currentRefs = new Set(bite.evidence_refs);
  const carried = bite.evidence_refs.filter((title) => previousRefs.has(title));
  const added = bite.evidence_refs.filter((title) => !previousRefs.has(title));
  const dropped = [...previousRefs].filter((title) => !currentRefs.has(title));
  const anomalyPool = (added.length >= 2 ? added : bite.evidence_refs)
    .map((title) => asArtifactPost(title, posts))
    .filter((post) => post.recentRank || post.overallRank)
    .sort((a, b) => rankPosition(a.recentRank || a.overallRank) - rankPosition(b.recentRank || b.overallRank));
  const anomalySpread = anomalyPool.length >= 2
    ? rankPosition(anomalyPool.at(-1)!.recentRank || anomalyPool.at(-1)!.overallRank)
      - rankPosition(anomalyPool[0].recentRank || anomalyPool[0].overallRank)
    : 0;
  const rankedEvidence = bite.evidence_refs
    .map((title) => asArtifactPost(title, posts))
    .filter((post) => post.recentRank || post.overallRank);
  const rankSpread = rankedEvidence.length >= 2
    ? Math.max(...rankedEvidence.map((post) => rankPosition(post.recentRank || post.overallRank)))
      - Math.min(...rankedEvidence.map((post) => rankPosition(post.recentRank || post.overallRank)))
    : 0;

  const candidates: { kind: ArtifactKind; build: () => VisitReaderArtifact }[] = [];
  if ((bite.movement === 'sharpened' || bite.movement === 'recast') && anomalySpread >= 0.45) {
    candidates.push({
      kind: 'anomaly_pair',
      build: () => ({ kind: 'anomaly_pair', posts: [anomalyPool[0], anomalyPool.at(-1)!] }),
    });
  }
  if (bite.movement === 'held') {
    candidates.push({ kind: 'tenure', build: () => ({ kind: 'tenure', history }) });
  }
  if (previous && (added.length > 0 || dropped.length > 0)) {
    candidates.push({
      kind: 'evidence_turnover',
      build: () => ({
        kind: 'evidence_turnover',
        carried: carried.map((title) => asArtifactPost(title, posts)),
        added: added.map((title) => asArtifactPost(title, posts)),
        dropped: dropped.map((title) => asArtifactPost(title, posts)),
      }),
    });
  }
  if (history.length >= 3) {
    candidates.push({ kind: 'tenure', build: () => ({ kind: 'tenure', history }) });
  }
  if (rankSpread >= 0.35) {
    candidates.push({ kind: 'rank_span', build: () => ({ kind: 'rank_span', posts: rankedEvidence }) });
  }
  candidates.push({
    kind: 'evidence_stack',
    build: () => ({ kind: 'evidence_stack', posts: bite.evidence_refs.map((title) => asArtifactPost(title, posts)) }),
  });

  const selected = candidates.find((candidate) => !used.has(candidate.kind)) || candidates[0];
  used.add(selected.kind);
  return selected.build();
}

function caseBeat(
  bite: ReaderBite,
  previous: ReaderBite | undefined,
  history: VisitReaderHistory[],
  posts: Map<string, ReaderPost>,
  used: Set<ArtifactKind>,
): VisitCaseBeat {
  const reinterpretationRefs = bite.reinterpretation?.evidence_refs || [];
  const refs = [...new Set([...bite.evidence_refs, ...bite.counterevidence_refs, ...reinterpretationRefs])];
  const receipt = posts.get(bite.evidence_refs[0]);
  const landing = rankOf(receipt?.recent_rank || receipt?.overall_rank);

  return {
    kicker: `${bite.movement} Bite`,
    hook: [bite.title],
    misses: 0,
    ...landing,
    rankNote: receipt?.recent_rank ? `${receipt.recent_rank} recent` : receipt?.overall_rank || 'current evidence',
    verdict: bite.changed_because,
    evidence: bite.why_it_matters_now,
    receipt: receipt?.title || bite.evidence_refs[0],
    fullRead: [bite.current_read, bite.changed_because, bite.why_it_matters_now],
    packet: refs.map((title, index) => {
      const post = posts.get(title);
      const rank = [post?.recent_rank && `${post.recent_rank} recent`, post?.overall_rank && `${post.overall_rank} overall`]
        .filter(Boolean)
        .join(' · ');
      return { tag: title, rank: rank || 'current evidence', breaker: index === 0 };
    }),
    reader: {
      biteId: bite.bite_id,
      movement: bite.movement,
      previousTitle: previous?.title,
      history,
      currentRead: bite.current_read,
      changedBecause: bite.changed_because,
      whyItMattersNow: bite.why_it_matters_now,
      evidenceRefs: bite.evidence_refs,
      counterevidenceRefs: bite.counterevidence_refs,
      reinterpretation: bite.reinterpretation
        ? {
            oldRead: bite.reinterpretation.old_read,
            newRead: bite.reinterpretation.new_read,
            evidenceRefs: bite.reinterpretation.evidence_refs,
          }
        : undefined,
      artifact: artifactFor(bite, previous, history, posts, used),
    },
  };
}

/* A held Bite is the same reading in the next portrait. The model may still
   polish its wording, but the Reader must not stage that copy-edit as an
   evolution event. Only a non-held movement earns a new display title. */
function resolvedBiteTitle(fixtureIndex: number, biteId: string): string | undefined {
  let title: string | undefined;
  for (let index = 0; index <= fixtureIndex; index += 1) {
    const bite = FIXTURES[index].output.bites.find((candidate) => candidate.bite_id === biteId);
    if (!bite) continue;
    if (bite.movement !== 'held' || !title) title = bite.title;
  }
  return title;
}

const usedArtifacts = new Map<string, Set<ArtifactKind>>();

export const ANUJ_TERRA_VISITS = FIXTURES.map((fixture, index) => {
  const { output, request } = fixture;
  const posts = new Map(request.current_posts.map((post) => [post.title, post]));
  const previousOutput = index > 0 ? FIXTURES[index - 1].output : undefined;
  const cases = output.bites.map((bite) => {
    const previousRaw = previousOutput?.bites.find((candidate) => candidate.bite_id === bite.bite_id);
    const currentTitle = resolvedBiteTitle(index, bite.bite_id) || bite.title;
    const previousTitle = index > 0 ? resolvedBiteTitle(index - 1, bite.bite_id) : undefined;
    const current = { ...bite, title: currentTitle };
    const previous = previousRaw ? { ...previousRaw, title: previousTitle || previousRaw.title } : undefined;
    const history = FIXTURES.slice(0, index + 1).flatMap((candidate, historyIndex) => {
      const historicalBite = candidate.output.bites.find((item) => item.bite_id === bite.bite_id);
      return historicalBite
        ? [{
            week: historyIndex + 1,
            movement: historicalBite.movement,
            title: resolvedBiteTitle(historyIndex, bite.bite_id) || historicalBite.title,
          }]
        : [];
    });
    const used = usedArtifacts.get(bite.bite_id) || new Set<ArtifactKind>();
    usedArtifacts.set(bite.bite_id, used);
    return caseBeat(current, previous, history, posts, used);
  });

  const visit: FeederVisitData = {
    handle: 'anuj.mp4',
    week: index + 1,
    tier: 2,
    cover: {
      headline: splitHeadline(output.this_week.header),
      sub: output.this_week.tagline,
      tagline: output.this_week.tagline,
      summary: output.this_week.summary,
    },
    runAverage: currentRunAverage(index),
    runLabel: 'current portrait',
    callback: {
      placed: index ? 'previous run' : 'cold read',
      hunch: output.this_week.summary,
      verdict: `${output.bites.length} Bites`,
      settle: output.this_week.tagline,
    },
    caseBeat: cases[0],
    caseBeats: cases,
    quiet: [
      ...output.what_changed.map((item) => ({
        line: `${item.movement.replace(/[.!?]+$/, '')}. ${item.detail}`,
        note: 'what changed',
        postRefs: item.post_refs,
      })),
      ...output.observations.map((item) => ({ line: item.text, note: 'observation', postRefs: item.post_refs })),
    ],
    gameplan: output.feederverse_watch.signals,
    worldWatch: {
      rankPulse: output.feederverse_watch.rank_pulse,
      signals: output.feederverse_watch.signals,
      triggers: output.trigger_tags.map((trigger) => ({
        label: trigger.label,
        note: trigger.note,
        postRef: trigger.post_ref,
      })),
    },
    newHunch: output.next_watch,
    laneLabel: output.trigger_tags.length ? `${output.trigger_tags.length} triggers on record` : 'Feederverse watch',
    closingState: 'That’s the picture for now.',
    plate: { filled: output.bites.length, total: 4 },
    shelf: FIXTURES.slice(0, index)
      .map((prior, priorIndex) => ({
        week: priorIndex + 1,
        line: prior.output.this_week.header,
        when: 'previous portrait',
      }))
      .reverse(),
  };

  return { id: fixture.id, label: `week ${index + 1}`, visit };
});
