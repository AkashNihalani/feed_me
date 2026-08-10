/* ─────────────────────────────────────────────────────────────
   READER DROP MODEL — turns a chain of Account Reader runs into
   the weekly drop's story model.

   Input is the reader contract itself (output JSON + the run's post
   ranks), never hand-shaped copy: any feeder, any number of runs, any
   bite count renders the same way. The builder's whole job is deriving
   what the prompt leaves implicit:

   - continuity: per-bite run history, resolved display titles (a held
     Bite keeps its title even if the model polished the wording)
   - the watch chain: last run's next_watch becomes this run's
     opening callback ("we left this on record — here's the answer")
   - proof staging: each bite's movement picks ONE proof grammar
     (flip / held / widened / entered), and evidence is resolved to
     actual posts with ranks — never reduced to a count
   ───────────────────────────────────────────────────────────── */

export type ReaderRunPost = {
  title: string;
  lane?: string;
  posted?: string;
  recent_rank?: string;
  overall_rank?: string;
  /* the structured Post Card text (POST NAME / HEADER / WORK / FLOW …) */
  post_card?: string;
  /* Server-owned post identity/media. The Reader never receives or reasons
     over these; the renderer joins them back onto cited evidence afterward. */
  post_key?: string;
  post_url?: string;
  thumbnail_url?: string;
  thumbnail?: string;
};

export type ReaderBiteOutput = {
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

export type ReaderRunOutput = {
  this_week: { header: string; tagline: string; summary: string };
  what_changed: { movement: string; detail: string; post_refs: string[] }[];
  bites: ReaderBiteOutput[];
  observations: { text: string; post_refs: string[] }[];
  feederverse_watch: { rank_pulse: string; signals: string[] };
  trigger_tags: { label: string; note: string; post_ref: string }[];
  next_watch: string;
};

export type ReaderRunRecord = {
  output: ReaderRunOutput;
  posts: ReaderRunPost[];
};

/* ── drop model ─────────────────────────────────────────────── */

export type DropPost = {
  title: string;
  recentRank?: string;
  overallRank?: string;
  /* one-sentence description of what the post is, lifted from the Post
     Card's WORK section — the context that makes a title mean something */
  summary?: string;
  /* the post's opening hook (TEXT & REFERENCES), when one exists */
  hook?: string;
  /* the mechanic fingerprint chain — how the post is built, beat by beat */
  mechanic?: string;
  /* filled from the 90-day per-post thumbnail store once wired */
  thumbnail?: string;
  /* source post, when the server can resolve one */
  url?: string;
};

export type DropMovement = 'new' | 'held' | 'strengthened' | 'sharpened' | 'narrowed' | 'recast';

/* Public language for each movement — the label a third party reads.
   The raw movement word stays available for styling decisions. */
export const MOVEMENT_PHRASE: Record<DropMovement, string> = {
  new: 'new this run',
  held: 'still true',
  strengthened: 'more proof',
  sharpened: 'came into focus',
  narrowed: 'drew its boundary',
  recast: 'changed meaning',
};

export type DropRunTick = { run: number; movement: DropMovement };

/* One proof grammar per bite. The movement decides which — so a calm
   week LOOKS calm and a recast week looks like something broke. */
export type DropProof =
  | { kind: 'flip'; oldRead: string; newRead: string; posts: DropPost[] }
  | { kind: 'held'; ticks: DropRunTick[] }
  | { kind: 'widened'; carried: DropPost[]; added: DropPost[] }
  | { kind: 'entered' };

export type DropBite = {
  id: string;
  order: number;
  movement: DropMovement;
  movementPhrase: string;
  title: string;
  previousTitle?: string;
  currentRead: string;
  changedBecause: string;
  whyItMattersNow: string;
  ticks: DropRunTick[];
  evidence: DropPost[];
  boundary: DropPost[];
  proof: DropProof;
};

/* Run texture computed from the supplied posts — powers the visual
   artifacts. Only overall rank is user-facing; recent rank stays an
   internal signal (the model's memory context), never rendered. */
export type DropLane = {
  label: string;
  count: number;
  /* mean overall percentile of the lane's posts — server-computed */
  avgTopPct?: number;
  best?: DropPost;
};

export type DropStats = {
  memory: number;
  newPosts?: number;
  /* every lane in the memory with its own performance read; the lanes
     beat renders only when there are two or more to compare */
  lanes: DropLane[];
  /* posts per week over the last six weeks, oldest → newest */
  cadence: { weeksAgo: number; count: number }[];
  /* THE LANDING — the server's own read of this run's posts, computed
     from overall ranks in code. Never part of the model payload; purely
     a performance layer for the drop. */
  landing?: {
    /* mean overall percentile of the run's posts — "Top N%" */
    avgTopPct: number;
    best: DropPost;
    /* how the run's posts distributed across the memory quarters */
    shape: { top: number; upper: number; lower: number; floor: number };
    /* the run's posts, strongest landing first — the filmstrip */
    posts: DropPost[];
  };
  /* average landing per run, first run → this run — the account's
     motion, drawn. Present once two runs have a measurable landing. */
  trend: { run: number; avgTopPct: number }[];
};

export type ReaderDropModel = {
  handle: string;
  run: number;
  cover: { headline: string[]; tagline: string; summary: string };
  stats: DropStats;
  /* last run's next_watch, settled by this run's field */
  callback?: { placedRun: number; watch: string; answer: string };
  portrait: { id: string; title: string; movement: DropMovement; movementPhrase: string; runsActive: number }[];
  bites: DropBite[];
  changed: { movement: string; detail: string; posts: DropPost[] }[];
  watch: {
    pulse: string;
    signals: string[];
    observations: { text: string; posts: DropPost[] }[];
    triggers: { label: string; note: string; post: DropPost }[];
  };
  nextWatch: string;
  shelf: { run: number; header: string }[];
};

/* ── helpers ────────────────────────────────────────────────── */

const asMovement = (value: string): DropMovement =>
  (['new', 'held', 'strengthened', 'sharpened', 'narrowed', 'recast'] as const).find((m) => m === value) ?? 'held';

/* Wrapped-style covers want the header broken into short lines. Split on
   word count, biased so the top line is the longer one. */
export function splitHeadline(header: string): string[] {
  const words = header.split(' ');
  if (words.length <= 3) return [header];
  const splitAt = Math.ceil(words.length / 2);
  return [words.slice(0, splitAt).join(' '), words.slice(splitAt).join(' ')].filter(Boolean);
}

/* Post Card sections (POST NAME / HEADER / WORK / FLOW / TEXT &
   REFERENCES / MECHANIC FINGERPRINT) are blank-line-separated blocks
   whose first line is the section name. */
function cardSection(postCard: string | undefined, name: string): string | undefined {
  if (!postCard) return undefined;
  const block = postCard
    .split(/\n\s*\n/)
    .find((candidate) => candidate.trimStart().toUpperCase().startsWith(name));
  if (!block) return undefined;
  const text = block.split('\n').slice(1).join('\n').trim();
  return text || undefined;
}

/* WORK is the one-sentence description of the post. */
function workSummary(postCard?: string): string | undefined {
  return cardSection(postCard, 'WORK')?.replace(/\s+/g, ' ').trim() || undefined;
}

/* TEXT & REFERENCES carries `Hook: …` — the post's opening line. */
function hookOf(postCard?: string): string | undefined {
  const section = cardSection(postCard, 'TEXT');
  const match = section?.match(/^Hook:\s*(.+)$/m);
  const hook = match?.[1].trim().replace(/^[“"]|[”"]$/g, '');
  if (!hook || /^none$/i.test(hook)) return undefined;
  return hook;
}

/* MECHANIC FINGERPRINT is an arrow chain of the post's construction. */
function mechanicOf(postCard?: string): string | undefined {
  const section = cardSection(postCard, 'MECHANIC');
  if (!section) return undefined;
  return section.replace(/\s*->\s*/g, ' → ').replace(/\s+/g, ' ').trim() || undefined;
}

function externalUrl(value?: string): string | undefined {
  if (!value) return undefined;
  try {
    const url = new URL(value);
    return url.protocol === 'https:' || url.protocol === 'http:' ? url.href : undefined;
  } catch {
    return undefined;
  }
}

function toDropPost(title: string, posts: Map<string, ReaderRunPost>): DropPost {
  const post = posts.get(title);
  return {
    title,
    recentRank: post?.recent_rank,
    overallRank: post?.overall_rank,
    summary: workSummary(post?.post_card),
    hook: hookOf(post?.post_card),
    mechanic: mechanicOf(post?.post_card),
    thumbnail: post?.thumbnail ?? post?.thumbnail_url,
    url: externalUrl(post?.post_url),
  };
}

function overallPctOf(post: ReaderRunPost): number | undefined {
  const match = post.overall_rank?.match(/^(\d+)\/(\d+)/);
  if (!match) return undefined;
  const pool = Number(match[2]);
  return pool > 1 ? (Number(match[1]) / pool) * 100 : undefined;
}

/* A held Bite is the same reading in the next portrait; its display title
   must not drift on a copy-edit. Walk the chain and only let a non-held
   movement earn a new title. */
function resolvedTitle(records: ReaderRunRecord[], upToIndex: number, biteId: string): string | undefined {
  let title: string | undefined;
  for (let index = 0; index <= upToIndex; index += 1) {
    const bite = records[index].output.bites.find((candidate) => candidate.bite_id === biteId);
    if (!bite) continue;
    if (bite.movement !== 'held' || !title) title = bite.title;
  }
  return title;
}

/* "5 days ago" / "3 weeks ago" / "1 month ago" → age in days. Coarse by
   design — the reader gets no timestamps, so neither do we. */
function postedAgeDays(posted?: string): number | undefined {
  if (!posted) return undefined;
  const clean = posted.trim().toLowerCase();
  if (clean === 'today') return 0;
  if (clean === 'yesterday') return 1;
  const match = clean.match(/^(\d+)\s+(day|week|month)s?\s+ago$/);
  if (!match) return undefined;
  return Number(match[1]) * (match[2] === 'day' ? 1 : match[2] === 'week' ? 7 : 30);
}

function statsFor(posts: ReaderRunPost[], previousPosts?: ReaderRunPost[]): DropStats {
  const postIndex = new Map(posts.map((post) => [post.title, post]));
  const laneGroups = new Map<string, ReaderRunPost[]>();
  posts.forEach((post) => {
    const label = (post.lane || 'post').toLowerCase();
    laneGroups.set(label, [...(laneGroups.get(label) ?? []), post]);
  });
  const lanes: DropLane[] = [...laneGroups.entries()]
    .map(([label, lanePosts]) => {
      const pcts = lanePosts
        .map((post) => ({ post, pct: overallPctOf(post) }))
        .filter((item): item is { post: ReaderRunPost; pct: number } => item.pct != null);
      const best = pcts.length ? pcts.reduce((a, b) => (a.pct <= b.pct ? a : b)).post : undefined;
      return {
        label,
        count: lanePosts.length,
        avgTopPct: pcts.length ? Math.max(1, Math.min(100, Math.round(pcts.reduce((sum, item) => sum + item.pct, 0) / pcts.length))) : undefined,
        best: best ? toDropPost(best.title, postIndex) : undefined,
      };
    })
    .sort((a, b) => b.count - a.count)
    .slice(0, 4);

  /* Cadence is anchored to the newest post, not the capture date — ages in
     the payload are relative to whenever the run was captured, and the
     chart's job is the account's rhythm, not the archive's age. */
  const ages = posts
    .map((post) => postedAgeDays(post.posted))
    .filter((age): age is number => age != null);
  const newest = ages.length ? Math.min(...ages) : 0;
  const cadence = Array.from({ length: 6 }, (_, i) => ({ weeksAgo: 5 - i, count: 0 }));
  ages.forEach((age) => {
    const bucket = Math.floor((age - newest) / 7);
    if (bucket <= 5) cadence[5 - bucket].count += 1;
  });

  const previousTitles = previousPosts ? new Set(previousPosts.map((post) => post.title)) : undefined;
  /* "this run's posts": everything the previous run hadn't seen; on a
     first run, the newest week of the memory stands in. */
  const runPosts = previousTitles
    ? posts.filter((post) => !previousTitles.has(post.title))
    : posts.filter((post) => {
        const age = postedAgeDays(post.posted);
        return age != null && age <= newest + 6;
      });
  const newPosts = previousTitles ? runPosts.length : undefined;

  const rankedRun = runPosts
    .map((post) => ({ post, pct: overallPctOf(post) }))
    .filter((item): item is { post: ReaderRunPost; pct: number } => item.pct != null)
    .sort((a, b) => a.pct - b.pct);

  let landing: DropStats['landing'];
  if (rankedRun.length >= 2) {
    const shape = { top: 0, upper: 0, lower: 0, floor: 0 };
    rankedRun.forEach(({ pct }) => {
      if (pct <= 25) shape.top += 1;
      else if (pct <= 50) shape.upper += 1;
      else if (pct <= 75) shape.lower += 1;
      else shape.floor += 1;
    });
    landing = {
      avgTopPct: Math.max(1, Math.min(100, Math.round(rankedRun.reduce((sum, item) => sum + item.pct, 0) / rankedRun.length))),
      best: toDropPost(rankedRun[0].post.title, postIndex),
      shape,
      posts: rankedRun.map(({ post }) => toDropPost(post.title, postIndex)),
    };
  }

  return { memory: posts.length, newPosts, lanes, cadence, landing, trend: [] };
}

function ticksFor(records: ReaderRunRecord[], upToIndex: number, biteId: string): DropRunTick[] {
  return records.slice(0, upToIndex + 1).flatMap((record, index) => {
    const bite = record.output.bites.find((candidate) => candidate.bite_id === biteId);
    return bite ? [{ run: index + 1, movement: asMovement(bite.movement) }] : [];
  });
}

function proofFor(
  bite: ReaderBiteOutput,
  previous: ReaderBiteOutput | undefined,
  ticks: DropRunTick[],
  posts: Map<string, ReaderRunPost>,
): DropProof {
  const movement = asMovement(bite.movement);
  /* Reinterpretation is the strongest object the reader produces — whenever
     one exists, the flip IS the proof, whatever the movement label. */
  if (bite.reinterpretation) {
    return {
      kind: 'flip',
      oldRead: bite.reinterpretation.old_read,
      newRead: bite.reinterpretation.new_read,
      posts: bite.reinterpretation.evidence_refs.map((title) => toDropPost(title, posts)),
    };
  }
  if (movement === 'held') return { kind: 'held', ticks };
  if (movement === 'new') return { kind: 'entered' };
  const previousRefs = new Set(previous?.evidence_refs ?? []);
  return {
    kind: 'widened',
    carried: bite.evidence_refs.filter((title) => previousRefs.has(title)).map((title) => toDropPost(title, posts)),
    added: bite.evidence_refs.filter((title) => !previousRefs.has(title)).map((title) => toDropPost(title, posts)),
  };
}

/* ── builder ────────────────────────────────────────────────── */

export function buildReaderDrops(handle: string, records: ReaderRunRecord[]): ReaderDropModel[] {
  const allStats = records.map((record, index) => statsFor(record.posts, index > 0 ? records[index - 1].posts : undefined));
  return records.map((record, index) => {
    const { output } = record;
    const posts = new Map(record.posts.map((post) => [post.title, post]));
    const previousOutput = index > 0 ? records[index - 1].output : undefined;
    const trend = allStats
      .slice(0, index + 1)
      .flatMap((stats, statsIndex) => (stats.landing ? [{ run: statsIndex + 1, avgTopPct: stats.landing.avgTopPct }] : []));

    const bites: DropBite[] = [...output.bites]
      .sort((a, b) => a.display_rank - b.display_rank)
      .map((bite, order) => {
        const previous = previousOutput?.bites.find((candidate) => candidate.bite_id === bite.bite_id);
        const movement = asMovement(bite.movement);
        const ticks = ticksFor(records, index, bite.bite_id);
        const title = resolvedTitle(records, index, bite.bite_id) ?? bite.title;
        const previousTitle = index > 0 && previous ? resolvedTitle(records, index - 1, bite.bite_id) : undefined;
        return {
          id: bite.bite_id,
          order: order + 1,
          movement,
          movementPhrase: MOVEMENT_PHRASE[movement],
          title,
          previousTitle: previousTitle !== title ? previousTitle : undefined,
          currentRead: bite.current_read,
          changedBecause: bite.changed_because,
          whyItMattersNow: bite.why_it_matters_now,
          ticks,
          evidence: bite.evidence_refs.map((ref) => toDropPost(ref, posts)),
          boundary: bite.counterevidence_refs.map((ref) => toDropPost(ref, posts)),
          proof: proofFor(bite, previous, ticks, posts),
        };
      });

    return {
      handle,
      run: index + 1,
      cover: {
        headline: splitHeadline(output.this_week.header),
        tagline: output.this_week.tagline,
        summary: output.this_week.summary,
      },
      stats: { ...allStats[index], trend: trend.length >= 2 ? trend : [] },
      callback: previousOutput?.next_watch
        ? { placedRun: index, watch: previousOutput.next_watch, answer: output.this_week.summary }
        : undefined,
      portrait: bites.map((bite) => ({
        id: bite.id,
        title: bite.title,
        movement: bite.movement,
        movementPhrase: bite.movementPhrase,
        runsActive: bite.ticks.length,
      })),
      bites,
      changed: output.what_changed.map((item) => ({
        movement: item.movement.replace(/[.!?]+$/, ''),
        detail: item.detail,
        posts: item.post_refs.map((ref) => toDropPost(ref, posts)),
      })),
      watch: {
        pulse: output.feederverse_watch.rank_pulse,
        signals: output.feederverse_watch.signals,
        observations: output.observations.map((item) => ({
          text: item.text,
          posts: item.post_refs.map((ref) => toDropPost(ref, posts)),
        })),
        triggers: output.trigger_tags.map((trigger) => ({
          label: trigger.label,
          note: trigger.note,
          post: toDropPost(trigger.post_ref, posts),
        })),
      },
      nextWatch: output.next_watch,
      shelf: records
        .slice(0, index)
        .map((prior, priorIndex) => ({ run: priorIndex + 1, header: prior.output.this_week.header }))
        .reverse(),
    };
  });
}
