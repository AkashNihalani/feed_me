const IST = 'Asia/Kolkata';

function number(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function handle(post) {
  return post?.handle ? `@${String(post.handle).replace(/^@+/, '')}` : post?.feedName || 'This feed';
}

function format(value) {
  return value ? `${value[0].toUpperCase()}${value.slice(1)}` : 'Posts';
}

// "reels", "carousels": a media type counted in a sentence
function plural(value) {
  return value && value !== 'unknown' ? `${value}s` : 'posts';
}

function multiple(value) {
  return `${value.toFixed(value >= 10 ? 0 : 1).replace(/\.0$/, '')}×`;
}

// one formatter for every read (making one is the slow part), and each date read once
let istFormat = null;
const partsRead = new Map();

function parts(postedAt) {
  const key = postedAt || '';
  if (partsRead.has(key)) return partsRead.get(key);
  const date = new Date(key);
  let read = null;
  if (!Number.isNaN(date.getTime())) {
    istFormat = istFormat || new Intl.DateTimeFormat('en-GB', { timeZone: IST, day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', hourCycle: 'h23' });
    read = istFormat.formatToParts(date);
  }
  if (partsRead.size >= 5000) partsRead.clear();
  partsRead.set(key, read);
  return read;
}

function value(partsList, type) {
  return partsList?.find((part) => part.type === type)?.value || '';
}

function dayKey(post) {
  const dateParts = parts(post.postedAt);
  const year = value(dateParts, 'year');
  const month = value(dateParts, 'month');
  const day = value(dateParts, 'day');
  return year && month && day ? `${year}-${month}-${day}` : null;
}

function dayLabel(post) {
  const dateParts = parts(post.postedAt);
  const month = value(dateParts, 'month');
  const day = value(dateParts, 'day');
  return month && day ? `${day} ${month}` : 'this window';
}

function dayOrdinal(post) {
  const dateParts = parts(post.postedAt);
  const year = Number(value(dateParts, 'year'));
  const day = Number(value(dateParts, 'day'));
  const month = value(dateParts, 'month');
  const timestamp = Date.parse(`${day} ${month} ${year} 00:00:00 UTC`);
  return Number.isFinite(timestamp) ? Math.round(timestamp / 86_400_000) : null;
}

function dayPart(post) {
  const hour = Number(value(parts(post.postedAt), 'hour')) % 24;
  if (!Number.isFinite(hour)) return null;
  if (hour >= 6 && hour < 11) return 'morning';
  if (hour >= 11 && hour < 16) return 'midday';
  if (hour >= 16 && hour < 21) return 'evening';
  return 'night';
}

function postTime(post) {
  const time = Date.parse(post.postedAt || '');
  return Number.isFinite(time) ? time : 0;
}

function score(post) {
  return post.latestPercentile == null ? 101 : number(post.latestPercentile);
}

function representative(posts) {
  return [...posts].sort((a, b) => score(a) - score(b) || String(a.postKey).localeCompare(String(b.postKey)))[0] || null;
}

function group(posts, keyFor) {
  const groups = new Map();
  for (const post of posts) {
    const key = keyFor(post);
    if (!key) continue;
    const items = groups.get(key);
    if (items) items.push(post);
    else groups.set(key, [post]);
  }
  return [...groups.entries()].map(([key, items]) => ({ key, posts: items }));
}

function fact(kind, headline, detail, posts, scoreValue) {
  const representativePost = representative(posts);
  return {
    kind,
    headline,
    detail,
    thumbnailUrl: representativePost?.thumbnailUrl || null,
    postUrl: representativePost?.postUrl || null,
    evidencePostKeys: posts.map((post) => post.postKey).filter(Boolean),
    _score: scoreValue,
    _evidence: new Set(posts.map((post) => post.postKey).filter(Boolean)),
  };
}

function consecutiveRun(posts, predicate) {
  let best = [];
  let run = [];
  for (const post of [...posts].sort((a, b) => postTime(a) - postTime(b) || String(a.postKey).localeCompare(String(b.postKey)))) {
    if (predicate(post)) run.push(post);
    else run = [];
    if (run.length > best.length) best = [...run];
  }
  return best;
}

function hasEvidenceOverlap(selected, candidate) {
  return selected.some((fact) => {
    const overlap = [...candidate._evidence].filter((key) => fact._evidence.has(key)).length;
    const smallerSet = Math.min(candidate._evidence.size, fact._evidence.size);
    return smallerSet > 0 && overlap / smallerSet >= 0.75;
  });
}

/** Deterministic Feederboard facts. Every clause is composed from the supplied post data. */
function buildFeederboardFacts(posts, windowDays) {
  const scoped = posts.filter((post) => post && post.thumbnailUrl);
  const candidates = [];
  const above = (post) => number(post.rankingMultiple) >= 1.1;
  const below = (post) => number(post.rankingMultiple) > 0 && number(post.rankingMultiple) <= 0.9;

  for (const [direction, predicate] of [['above', above], ['below', below]]) {
    const run = consecutiveRun(scoped, predicate);
    if (run.length >= 3) {
      const formats = group(run, (post) => post.mediaType).sort((a, b) => b.posts.length - a.posts.length || a.key.localeCompare(b.key));
      const dominant = formats[0];
      const subject = handle(run[run.length - 1]);
      const verb = direction === 'above' ? 'is' : 'has missed';
      const detail = dominant && dominant.posts.length >= 2
        ? `${dominant.posts.length} are ${format(dominant.key).toLowerCase()}.`
        : `The run spans ${windowDays} days.`;
      candidates.push(fact(
        `${direction}-streak`,
        direction === 'above' ? `${subject} is ${run.length} posts into an above-usual run.` : `${subject} has missed usual on ${run.length} straight posts.`,
        detail,
        run,
        90 + run.length,
      ));
      void verb;
    }
  }

  const climber = [...scoped].sort((a, b) => (number(b.firstPercentile) - number(b.latestPercentile)) - (number(a.firstPercentile) - number(a.latestPercentile)) || String(a.postKey).localeCompare(String(b.postKey)))[0];
  const climb = climber ? number(climber.firstPercentile) - number(climber.latestPercentile) : 0;
  if (climber && climber.firstPercentile != null && climber.latestPercentile != null && climb >= 5) {
    candidates.push(fact('checkpoint-climb', `${handle(climber)} moved ${Math.round(climb)} places between ${climber.firstCheckpoint || 'D1'} and ${climber.latestCheckpoint || 'the latest read'}.`, `${format(climber.mediaType)} · ${dayLabel(climber)}.`, [climber], 86 + climb));
  }

  const busiest = group(scoped, dayKey).filter((entry) => entry.posts.length >= 2).sort((a, b) => b.posts.length - a.posts.length || a.key.localeCompare(b.key))[0];
  if (busiest) {
    const contributors = group(busiest.posts, (post) => handle(post)).sort((a, b) => b.posts.length - a.posts.length || a.key.localeCompare(b.key));
    const lead = contributors[0];
    candidates.push(fact('busiest-day', `${scoped[0]?.feedName || 'This feed'} posted ${busiest.posts.length} times on ${dayLabel(busiest.posts[0])}.`, lead && lead.posts.length > 1 ? `${lead.key} supplied ${lead.posts.length}.` : `${contributors.length} feeders contributed.`, busiest.posts, 78 + busiest.posts.length));
  }

  const publishingDays = group(scoped, dayKey)
    .map((entry) => ({ ...entry, ordinal: dayOrdinal(entry.posts[0]) }))
    .filter((entry) => entry.ordinal != null)
    .sort((a, b) => a.ordinal - b.ordinal);
  let dayRun = [];
  let longestDayRun = [];
  for (const day of publishingDays) {
    dayRun = dayRun.length && day.ordinal === dayRun[dayRun.length - 1].ordinal + 1 ? [...dayRun, day] : [day];
    if (dayRun.length > longestDayRun.length) longestDayRun = [...dayRun];
  }
  if (longestDayRun.length >= 3) {
    const runPosts = longestDayRun.flatMap((day) => day.posts);
    const lead = group(runPosts, (post) => handle(post)).sort((a, b) => b.posts.length - a.posts.length || a.key.localeCompare(b.key))[0];
    candidates.push(fact('publishing-run', `${lead?.key || scoped[0]?.feedName || 'This feed'} posted on ${longestDayRun.length} straight days.`, `${runPosts.length} posts landed in that run.`, runPosts, 69 + longestDayRun.length));
  }

  const formatStreak = group(scoped, (post) => post.mediaType).filter((entry) => entry.posts.length >= 3).sort((a, b) => b.posts.length - a.posts.length || a.key.localeCompare(b.key))[0];
  if (formatStreak) {
    const aboveCount = formatStreak.posts.filter(above).length;
    if (aboveCount >= 2) candidates.push(fact('format-run', `${aboveCount} of ${formatStreak.posts.length} ${plural(formatStreak.key)} beat their usual this window.`, `${handle(representative(formatStreak.posts))} supplied the strongest one.`, formatStreak.posts, 72 + aboveCount));
  }

  const formatTime = group(scoped.filter(above), (post) => `${post.mediaType}:${dayPart(post)}`).filter((entry) => entry.posts.length >= 2).sort((a, b) => b.posts.length - a.posts.length || a.key.localeCompare(b.key))[0];
  if (formatTime) {
    const [media, time] = formatTime.key.split(':');
    candidates.push(fact('format-time', `${format(plural(media))} landed above usual ${formatTime.posts.length} times in the ${time}.`, `${handle(representative(formatTime.posts))} appears in that run.`, formatTime.posts, 68 + formatTime.posts.length));
  }

  const totalComments = scoped.reduce((sum, post) => sum + number(post.comments), 0);
  const commentLeader = [...scoped].sort((a, b) => number(b.comments) - number(a.comments) || String(a.postKey).localeCompare(String(b.postKey)))[0];
  if (commentLeader && totalComments > 0 && number(commentLeader.comments) / totalComments >= 0.4) {
    const share = Math.round((number(commentLeader.comments) / totalComments) * 100);
    candidates.push(fact('post-concentration', `One post supplied ${share}% of ${scoped[0]?.feedName || 'this feed'}’s comments this window.`, `${handle(commentLeader)} · ${format(commentLeader.mediaType)}.`, [commentLeader], 80 + share / 10));
  }

  const feeders = group(scoped, (post) => handle(post));
  const participating = feeders.filter((entry) => entry.posts.some(above));
  if (feeders.length >= 2 && participating.length >= 2) {
    candidates.push(fact('feeder-participation', `${participating.length} of ${feeders.length} feeders landed an above-usual post this window.`, `${handle(representative(participating.flatMap((entry) => entry.posts.filter(above))))} led the group.`, participating.flatMap((entry) => entry.posts.filter(above)), 66 + participating.length));
  }

  const feederComments = feeders.map((entry) => ({ ...entry, comments: entry.posts.reduce((sum, post) => sum + number(post.comments), 0) })).sort((a, b) => b.comments - a.comments || a.key.localeCompare(b.key))[0];
  if (feederComments && totalComments > 0 && feederComments.comments / totalComments >= 0.45) {
    const share = Math.round((feederComments.comments / totalComments) * 100);
    candidates.push(fact('feeder-concentration', `${feederComments.key} supplied ${share}% of ${scoped[0]?.feedName || 'this feed'}’s comments this window.`, `${feederComments.posts.length} posts carried that share.`, feederComments.posts, 70 + share / 10));
  }

  const imbalance = [...scoped]
    .filter((post) => post.commentsMultiple != null && post.likesMultiple != null)
    .sort((a, b) => Math.abs(number(b.commentsMultiple) - number(b.likesMultiple)) - Math.abs(number(a.commentsMultiple) - number(a.likesMultiple)) || String(a.postKey).localeCompare(String(b.postKey)))[0];
  if (imbalance && Math.abs(number(imbalance.commentsMultiple) - number(imbalance.likesMultiple)) >= 0.8) {
    candidates.push(fact('response-imbalance', `${handle(imbalance)}: ${multiple(number(imbalance.commentsMultiple))} usual comments, ${multiple(number(imbalance.likesMultiple))} usual likes.`, `${format(imbalance.mediaType)} · ${dayLabel(imbalance)}.`, [imbalance], 67 + Math.abs(number(imbalance.commentsMultiple) - number(imbalance.likesMultiple)) * 8));
  }

  const quietBreakout = [...scoped].sort((a, b) => postTime(a) - postTime(b));
  for (let index = 1; index < quietBreakout.length; index += 1) {
    const gapDays = (postTime(quietBreakout[index]) - postTime(quietBreakout[index - 1])) / 86_400_000;
    if (gapDays >= 3 && number(quietBreakout[index].rankingMultiple) >= 1.5) {
      const post = quietBreakout[index];
      candidates.push(fact('quiet-breakout', `${handle(post)} returned after ${Math.floor(gapDays)} quiet days with a ${multiple(number(post.rankingMultiple))} post.`, `${format(post.mediaType)} · ${dayLabel(post)}.`, [post], 74 + Math.min(10, gapDays)));
      break;
    }
  }

  return candidates
    .sort((a, b) => b._score - a._score || a.kind.localeCompare(b.kind))
    .reduce((selected, candidate) => selected.length >= 5 || hasEvidenceOverlap(selected, candidate) ? selected : [...selected, candidate], [])
    .map((candidate) => ({
      kind: candidate.kind,
      headline: candidate.headline,
      detail: candidate.detail,
      thumbnailUrl: candidate.thumbnailUrl,
      postUrl: candidate.postUrl,
      evidencePostKeys: candidate.evidencePostKeys,
    }));
}

function postMortemTag(kind) {
  return String(kind || 'trigger').replace(/-/g, ' ').toUpperCase();
}

/** A compact, server-composed deck for the existing Post Mortem stack. */
function buildPostMortemDeck(posts, windowDays) {
  const byKey = new Map(posts.map((post) => [post.postKey, post]));
  return buildFeederboardFacts(posts, windowDays).map((fact, index) => {
    const hero = byKey.get(fact.evidencePostKeys[0]) || null;
    return {
      id: `${fact.kind}:${hero?.postKey || index}`,
      postKey: hero?.postKey || '',
      postUrl: fact.postUrl,
      thumbnailUrl: fact.thumbnailUrl,
      feedId: hero?.feedId || '',
      feedName: hero?.feedName || 'Feed',
      handle: hero?.handle || '',
      trigger: fact.kind,
      tag: postMortemTag(fact.kind),
      statement: fact.headline,
      proof: fact.detail || null,
      supportingPostKeys: fact.evidencePostKeys,
      priority: 100 - index,
    };
  });
}

/* ─────────────────────────────────────────────────────────────
   HERO FACTS — what Lead's hero says, one fact at a time.

   Counted from the runtime posts, never written: every claim is a
   count or a median of the supplied posts' "× usual" (rankingMultiple;
   over 1 beat the usual, under 1 sat under it), and carries the posts
   it stands on. A fact is beating, sinking, or plain (cadence and
   spread facts that say nothing about how a post did).

   A fact reads as a line in three slots (subject · verb · object)
   plus one figure, said one way: "4.4× usual", "9 of 10", "1 in 9".
   No percentiles: Top % is a different yardstick.

   Posts may carry two extras the caller worked out:
     at       when the post went out, in ms (a stored bare date gives
              way to the shortcode: lib/postMoment)
     clock    whether `at` is a real time of day (not a bare date)
     seconds  a reel's length, where it is known (the fingerprints)
   ───────────────────────────────────────────────────────────── */

const HERO_MAX = 12;
const HERO_PROOF = 6;
const HERO_KIND_CAP = 2;

const HERO_LABEL = {
  'format-streak': 'Format',
  'format-slump': 'Format',
  'format-run': 'Format',
  'format-miss': 'Format',
  'reel-length': 'Length',
  'last-ten': 'Form',
  'post-vs-previous': 'Latest',
  'shift': 'Shift',
  'format-return': 'Format',
  'above-streak': 'Run',
  'below-streak': 'Run',
  'breakouts': 'Breakouts',
  'lone-breakout': 'Breakout',
  'time-of-day': 'Timing',
  'format-time': 'Timing',
  'quiet-breakout': 'Comeback',
  'checkpoint-climb': 'Climb',
  'feeder-spread': 'Spread',
  'busiest-day': 'Cadence',
  'publishing-run': 'Cadence',
  'post-concentration': 'Chat',
  'feeder-concentration': 'Chat',
  'response-imbalance': 'Chat',
};

// the base each kind sorts on (a counted fact over a cadence fact), the strength of the claim adds to it
const HERO_BASE = {
  'format-streak': 96,
  'reel-length': 94,
  'post-vs-previous': 95,
  'last-ten': 92,
  'shift': 91,
  'above-streak': 90,
  'lone-breakout': 89,
  'format-slump': 88,
  'below-streak': 87,
  'format-run': 86,
  'format-miss': 85,
  'breakouts': 84,
  'format-return': 82,
  'time-of-day': 80,
  'format-time': 78,
  'quiet-breakout': 76,
  'checkpoint-climb': 75,
  'feeder-spread': 70,
  'response-imbalance': 58,
  'post-concentration': 56,
  'feeder-concentration': 55,
  'busiest-day': 52,
  'publishing-run': 50,
};

const HERO_FORMAT = { reel: 'reels', carousel: 'carousels', image: 'images' };
const HERO_PART = { morning: 'Morning', midday: 'Midday', evening: 'Evening', night: 'Night' };

function heroIso(post) {
  const at = Number(post.at);
  return Number.isFinite(at) && at > 0 ? new Date(at).toISOString() : post.postedAt;
}

function heroTime(post) {
  const at = Number(post.at);
  return Number.isFinite(at) && at > 0 ? at : postTime(post);
}

function heroBareDate(iso) {
  return !iso || /T00:00:00(?:\.0+)?(?:Z|\+00:00)$/.test(iso) || /^\d{4}-\d{2}-\d{2}$/.test(iso);
}

// whether the post's time of day can be trusted (a stored bare date is midnight UTC, "night" every time)
function heroHasClock(post) {
  if (post.clock === true) return true;
  if (post.clock === false) return false;
  return !heroBareDate(post.postedAt);
}

function heroPart(post) {
  if (!heroHasClock(post)) return null;
  const hour = Number(value(parts(heroIso(post)), 'hour')) % 24;
  if (!Number.isFinite(hour)) return null;
  if (hour >= 6 && hour < 11) return 'morning';
  if (hour >= 11 && hour < 16) return 'midday';
  if (hour >= 16 && hour < 21) return 'evening';
  return 'night';
}

function heroDay(post) {
  const dateParts = parts(heroIso(post));
  const year = value(dateParts, 'year');
  const month = value(dateParts, 'month');
  const day = value(dateParts, 'day');
  return year && month && day ? { key: `${year}-${month}-${day}`, label: `${Number(day)} ${month.replace(/^Sept$/, 'Sep')}`, ordinal: Math.round(Date.parse(`${day} ${month} ${year} 00:00:00 UTC`) / 86_400_000) } : null;
}

// the post's × usual, or null where it has none yet
function usual(post) {
  if (post.rankingMultiple == null || post.rankingMultiple === '') return null;
  const parsed = Number(post.rankingMultiple);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

const heroBeat = (post) => usual(post) > 1;
const heroMiss = (post) => usual(post) < 1;

function median(values) {
  const sorted = values.filter((one) => Number.isFinite(one)).sort((a, b) => a - b);
  if (!sorted.length) return null;
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

function usualOf(posts) {
  return median(posts.map(usual).filter((one) => one != null));
}

// "4.4×", and "<0.1×" for what is nearly nothing
function times(value) {
  if (value == null) return '—';
  if (value < 0.1) return '<0.1×';
  return multiple(value);
}

// an edge said beside the post's own figure: a second decimal where one would make them read the same
function apart(edge, mine) {
  const said = times(edge);
  return said !== times(mine) ? said : `${edge.toFixed(2)}×`;
}

function heroCount(value) {
  const absolute = Math.abs(value);
  if (absolute >= 1_000_000) return `${(absolute / 1_000_000).toFixed(1).replace(/\.0$/, '')}M`;
  if (absolute >= 1_000) return `${(absolute / 1_000).toFixed(1).replace(/\.0$/, '')}K`;
  return String(Math.round(absolute));
}

function bareHandle(post) {
  return post?.handle ? String(post.handle).replace(/^@+/, '').toLowerCase() : '';
}

function byTimeAsc(a, b) {
  return heroTime(a) - heroTime(b) || String(a.postKey).localeCompare(String(b.postKey));
}

// a fact's posts in the order the hero pictures them: the strongest first for a win, the weakest first for a miss
function heroProof(posts, tone) {
  const sign = tone === 'sinking' ? 1 : -1;
  return [...posts]
    .sort((a, b) => sign * ((usual(a) ?? 0) - (usual(b) ?? 0)) || byTimeAsc(b, a))
    .slice(0, HERO_PROOF)
    .map((post) => ({
      postKey: post.postKey,
      postUrl: post.postUrl || null,
      thumbnailUrl: post.thumbnailUrl || null,
      handle: bareHandle(post) || null,
      mediaType: post.mediaType || 'unknown',
      multiple: usual(post),
      at: heroTime(post) || null,
    }));
}

// when the posts went out, as short as it can be: "12 Oct", "3–9 Oct", "28 Sep – 3 Oct"
function heroSpan(posts) {
  const times = posts.map(heroTime).filter((time) => time > 0).sort((a, b) => a - b);
  if (!times.length) return '';
  const first = heroDay({ at: times[0] })?.label;
  const last = heroDay({ at: times[times.length - 1] })?.label;
  if (!first || !last) return '';
  if (first === last) return first;
  const [firstDay, firstMonth] = first.split(' ');
  const [lastDay, lastMonth] = last.split(' ');
  return firstMonth === lastMonth ? `${firstDay}–${lastDay} ${lastMonth}` : `${first} – ${last}`;
}

// the short, specific name the index rail gives a fact ("Long reels", "Latest carousel", "7 in a row")
function heroShort(kind, line) {
  const head = line[0];
  const all = line.join(' ');
  switch (kind) {
    case 'format-streak':
    case 'format-slump':
      return head.replace(/^The /, '').replace(/^l/, 'L');
    case 'format-run':
    case 'format-miss':
      return head;
    case 'reel-length':
      return /under/.test(head) ? 'Short reels' : 'Long reels';
    case 'last-ten':
      return `Last ${/last (\d+)/.exec(all)?.[1] || 10} posts`;
    case 'post-vs-previous': {
      const latest = /latest (\w+)/.exec(head);
      if (latest) return `Latest ${latest[1]}`;
      const dated = /(\d+\u00a0\w+)/.exec(head);
      const thing = /(carousel|reel|image|post)\b/.exec(head);
      return dated && thing ? `${dated[1].replace('\u00a0', ' ')} ${thing[1]}` : 'Latest post';
    }
    case 'shift':
      return 'Last 5 posts';
    case 'format-return':
      return `First ${/first (\w+)/.exec(head)?.[1] || 'post'}`;
    case 'above-streak':
    case 'below-streak':
      return `${/(\d+) posts/.exec(head + ' ' + line[2])?.[1] || ''} in a row`.trim();
    case 'breakouts': return 'Past 2×';
    case 'lone-breakout': return 'One breakout';
    case 'time-of-day':
    case 'format-time':
      return head;
    case 'quiet-breakout': return 'Comeback';
    case 'checkpoint-climb': return 'Climber';
    case 'feeder-spread': return 'Spread';
    case 'busiest-day': return 'Busiest day';
    case 'publishing-run': return 'Daily streak';
    case 'post-concentration': return 'Top talker';
    case 'feeder-concentration': return 'Comment share';
    case 'response-imbalance': return 'Likes vs comments';
    default: return HERO_LABEL[kind] || kind;
  }
}

function heroFact(kind, tone, line, figure, meta, evidence, score, view = null) {
  const proof = heroProof(evidence, tone);
  const handles = [...new Set(evidence.map(bareHandle).filter(Boolean))];
  const keys = evidence.map((post) => post.postKey).filter(Boolean);
  return {
    id: `${kind}:${[...keys].sort().slice(0, 4).join('+')}:${keys.length}`,
    kind,
    label: HERO_LABEL[kind] || kind.replace(/-/g, ' '),
    short: heroShort(kind, line),
    tone,
    subject: line[0],
    verb: line[1],
    object: line[2],
    figure,
    meta,
    span: heroSpan(evidence),
    // the shape the Hall's cards read, so one fact can be drawn by either
    headline: line.join(' ').replace(/\s+/g, ' ').trim(),
    detail: meta,
    thumbnailUrl: proof[0]?.thumbnailUrl || null,
    postUrl: proof[0]?.postUrl || null,
    evidencePostKeys: keys,
    proof,
    proofCount: keys.length,
    handle: handles.length === 1 ? handles[0] : null,
    _score: score,
    _evidence: new Set(keys),
    _view: view,
  };
}

// "the rest of the feed" in the words of the scope the hero is on
function heroRest(scope) {
  if (scope.kind === 'feeder') return 'the rest';
  if (scope.kind === 'feed') return `the rest of ${scope.name}`;
  return 'the rest of your feeds';
}

// one feeder in scope: the eyebrow already says whose, so a line never repeats the handle
const heroSolo = (scope) => scope.kind === 'feeder';

function heroUnder(scope) {
  return scope.kind === 'all' ? 'Your feeds' : scope.name;
}

// a streak's second sentence: still going since its first day, or the days it took
function streakSpan(top) {
  if (top.ongoing) return `Every post since ${(heroDay(top.run[0])?.label || 'then').replace(' ', '\u00a0')}.`;
  const span = heroSpan(top.run);
  return span ? `They went out ${span}.` : '';
}

// how many of the rest beat the usual: "9 of the other 25 did."
function restBeat(others, verb, plural = '') {
  if (!others.length) return '';
  return `${others.filter(heroBeat).length} of the other ${others.length}${plural ? ` ${plural}` : ''} ${verb}.`;
}

// a usual posting day, to set the busiest one against
function usualDay(rated, solo) {
  const perDay = median(group(rated, (post) => heroDay(post)?.key || null).map((entry) => entry.posts.length));
  if (perDay == null) return '';
  const count = Math.round(perDay);
  if (solo) return count === 1 ? 'Most days it posts once.' : count === 2 ? 'Most days it posts twice.' : `Most days it posts ${count} times.`;
  return count === 1 ? 'Most days see one post.' : `Most days see ${count} posts.`;
}

/* The leading run of a post list (newest first) that holds `keep`, as long as it goes. */
function leadingRun(newestFirst, keep) {
  const run = [];
  for (const post of newestFirst) {
    if (!keep(post)) break;
    run.push(post);
  }
  return run;
}

function heroCandidates(rated, scope) {
  const candidates = [];
  const feeders = group(rated, (post) => bareHandle(post) || null);

  // ── the last N of a format beat (or sank under) the rest ──
  const formats = group(rated, (post) => (HERO_FORMAT[post.mediaType] ? post.mediaType : null));
  for (const entry of formats) {
    const sorted = [...entry.posts].sort(byTimeAsc).reverse();
    const word = HERO_FORMAT[entry.key];
    const others = rated.filter((post) => post.mediaType !== entry.key);
    const otherUsual = usualOf(others);
    const win = leadingRun(sorted, heroBeat);
    if (win.length >= 3) {
      const strength = usualOf(win);
      const stronger = group(others, (post) => (HERO_FORMAT[post.mediaType] ? post.mediaType : null)).filter((one) => one.posts.length >= 2);
      const everyOther = stronger.length >= 2 && stronger.every((one) => usualOf(one.posts) < strength);
      if (strength >= 1.2 && (otherUsual == null || strength > otherUsual * 1.15)) {
        candidates.push(heroFact(
          'format-streak', 'beating',
          [`The last ${win.length} ${word}`, 'beat', otherUsual == null ? 'the usual' : everyOther ? 'every other format' : 'the other formats'],
          { value: times(strength), unit: 'usual' },
          otherUsual == null ? '' : `The other formats ran ${times(otherUsual)}.`,
          win, HERO_BASE['format-streak'] + win.length + Math.min(strength, 6),
        ));
      }
    }
    const slump = leadingRun(sorted, heroMiss);
    if (slump.length >= 3) {
      const strength = usualOf(slump);
      if (strength <= 0.85) {
        candidates.push(heroFact(
          'format-slump', 'sinking',
          [`The last ${slump.length} ${word}`, 'sat under', 'the usual'],
          { value: times(strength), unit: 'usual' },
          otherUsual == null ? '' : `The other formats ran ${times(otherUsual)}.`,
          slump, HERO_BASE['format-slump'] + slump.length + (1 - strength) * 4,
        ));
      }
    }
    // how many of the format beat the usual across the whole window
    if (entry.posts.length >= 4) {
      const beat = entry.posts.filter(heroBeat);
      const missed = entry.posts.filter(heroMiss);
      const rate = beat.length / entry.posts.length;
      if (rate >= 0.7 && beat.length >= 3) {
        candidates.push(heroFact(
          'format-run', 'beating',
          [`${format(word)}`, 'beat', 'the usual more often than not'],
          { value: `${beat.length} of ${entry.posts.length}`, unit: 'beat' },
          `The typical one ran ${times(usualOf(entry.posts))}.`,
          beat, HERO_BASE['format-run'] + rate * 4,
        ));
      } else if (rate <= 0.3 && missed.length >= 3) {
        candidates.push(heroFact(
          'format-miss', 'sinking',
          [`${format(word)}`, 'mostly sat under', 'the usual'],
          { value: `${beat.length} of ${entry.posts.length}`, unit: 'beat' },
          `The typical one ran ${times(usualOf(entry.posts))}.`,
          missed, HERO_BASE['format-miss'] + (1 - rate) * 4,
        ));
      }
    }
  }

  // ── a reel's length (only where the length is known) ──
  const timed = rated.filter((post) => post.mediaType === 'reel' && Number(post.seconds) > 0);
  if (timed.length >= 6) {
    let best = null;
    for (const cut of [15, 20, 30, 45, 60]) {
      const shorter = timed.filter((post) => Number(post.seconds) < cut);
      const longer = timed.filter((post) => Number(post.seconds) >= cut);
      if (shorter.length < 3 || longer.length < 3) continue;
      const shortUsual = usualOf(shorter);
      const longUsual = usualOf(longer);
      const gap = Math.abs(shortUsual - longUsual);
      if (!best || gap > best.gap + 1e-9) best = { cut, shorter, longer, shortUsual, longUsual, gap };
    }
    if (best && best.gap >= 0.4) {
      const shortWins = best.shortUsual > best.longUsual;
      const win = shortWins ? best.shorter : best.longer;
      const winUsual = shortWins ? best.shortUsual : best.longUsual;
      const lose = shortWins ? best.longer : best.shorter;
      const loseUsual = shortWins ? best.longUsual : best.shortUsual;
      if (winUsual > 1) {
        candidates.push(heroFact(
          'reel-length', 'beating',
          [shortWins ? `Reels under ${best.cut} seconds` : `Reels of ${best.cut} seconds and up`, 'beat', shortWins ? 'the longer ones' : 'the shorter ones'],
          { value: times(winUsual), unit: 'usual' },
          `The ${lose.length} ${shortWins ? 'longer' : 'shorter'} ones ran ${times(loseUsual)}.`,
          win, HERO_BASE['reel-length'] + best.gap * 3,
        ));
      }
      if (loseUsual < 0.9) {
        candidates.push(heroFact(
          'reel-length', 'sinking',
          [shortWins ? `Reels of ${best.cut} seconds and up` : `Reels under ${best.cut} seconds`, 'sat under', 'the usual'],
          { value: times(loseUsual), unit: 'usual' },
          `The ${win.length} ${shortWins ? 'shorter' : 'longer'} ones ran ${times(winUsual)}.`,
          lose, HERO_BASE['reel-length'] - 6 + best.gap * 2,
        ));
      }
    }
  }

  // ── form: how many of each feeder's last posts beat the usual ──
  const forms = [];
  for (const entry of feeders) {
    const lastN = [...entry.posts].sort(byTimeAsc).reverse().slice(0, 10);
    if (lastN.length < 6) continue;
    const beat = lastN.filter(heroBeat);
    forms.push({ entry, lastN, beat, rate: beat.length / lastN.length, who: `@${entry.key}` });
  }
  const bestForm = [...forms].sort((a, b) => b.rate - a.rate || b.lastN.length - a.lastN.length || a.who.localeCompare(b.who))[0];
  const worstForm = [...forms].sort((a, b) => a.rate - b.rate || b.lastN.length - a.lastN.length || a.who.localeCompare(b.who))[0];
  if (bestForm && bestForm.rate >= 0.7) {
    candidates.push(heroFact(
      'last-ten', 'beating',
      heroSolo(scope)
        ? [`${bestForm.beat.length} of the last ${bestForm.lastN.length} posts`, 'beat', 'the usual']
        : [bestForm.who, 'beat the usual on', `${bestForm.beat.length} of the last ${bestForm.lastN.length} posts`],
      { value: `${bestForm.beat.length} of ${bestForm.lastN.length}`, unit: 'beat' },
      `The typical one ran ${times(usualOf(bestForm.lastN))}.`,
      bestForm.beat, HERO_BASE['last-ten'] + bestForm.rate * 6,
    ));
  }
  if (worstForm && worstForm.rate <= 0.3 && worstForm !== bestForm) {
    const missed = worstForm.lastN.filter((post) => !heroBeat(post));
    candidates.push(heroFact(
      'last-ten', 'sinking',
      heroSolo(scope)
        ? [worstForm.beat.length ? `Only ${worstForm.beat.length} of the last ${worstForm.lastN.length} posts` : `None of the last ${worstForm.lastN.length} posts`, 'beat', 'the usual']
        : [worstForm.who, worstForm.beat.length ? 'beat the usual on only' : 'has not beaten the usual in', worstForm.beat.length ? `${worstForm.beat.length} of the last ${worstForm.lastN.length} posts` : `the last ${worstForm.lastN.length} posts`],
      { value: `${worstForm.beat.length} of ${worstForm.lastN.length}`, unit: 'beat' },
      `The typical one ran ${times(usualOf(worstForm.lastN))}.`,
      missed, HERO_BASE['last-ten'] - 4 + (1 - worstForm.rate) * 6,
    ));
  }

  // ── one of the newest posts against the ones before it, per feeder: it beat (or sat under) all of the last 10 ──
  for (const entry of feeders) {
    const sequence = [...entry.posts].sort(byTimeAsc);
    if (sequence.length < 7) continue;
    let found = null;
    for (let index = sequence.length - 1; index >= Math.max(1, sequence.length - 3); index -= 1) {
      const post = sequence[index];
      const earlier = sequence.slice(0, index);
      const thing = HERO_FORMAT[post.mediaType] ? post.mediaType : 'post';
      const sameFormat = thing === 'post' ? [] : earlier.filter((one) => one.mediaType === post.mediaType);
      const tries = [
        ...[20, 10].map((size) => ({ size, pool: earlier, plural: 'posts', matched: false })),
        ...[10, 6].map((size) => ({ size, pool: sameFormat, plural: HERO_FORMAT[post.mediaType], matched: true })),
      ].filter((one) => one.plural && one.pool.length >= one.size);
      for (const [tone, ok] of [['beating', usual(post) >= 1.3], ['sinking', usual(post) <= 0.8]]) {
        if (!ok) continue;
        const wins = (one) => (tone === 'beating' ? usual(one) < usual(post) : usual(one) > usual(post));
        const best = tries
          .filter((one) => one.pool.slice(-one.size).every(wins))
          .sort((a, b) => b.size - a.size || Number(b.matched) - Number(a.matched))[0];
        if (!best) continue;
        const against = best.pool.slice(-best.size);
        const edge = tone === 'beating' ? Math.max(...against.map(usual)) : Math.min(...against.map(usual));
        const rank = (tone === 'beating' ? Math.min(usual(post), 6) : (1 - usual(post)) * 4) + best.size / 2 - (sequence.length - 1 - index) * 2;
        if (!found || rank > found.rank) found = { post, index, tone, best, against, edge, thing, rank };
      }
    }
    if (!found) continue;
    const { post, index, tone, best, against, edge, thing } = found;
    const newest = index === sequence.length - 1;
    candidates.push(heroFact(
      'post-vs-previous', tone,
      [
        heroSolo(scope)
          ? (newest ? `The latest ${thing}` : `The ${(heroDay(post)?.label || 'last week').replace(' ', '\u00a0')} ${thing}`)
          : `@${entry.key}’s ${newest ? `latest ${thing}` : `${(heroDay(post)?.label || 'last week').replace(' ', '\u00a0')} ${thing}`}`,
        tone === 'beating' ? 'beat' : 'sat under',
        `the ${best.size} before it`,
      ],
      { value: times(usual(post)), unit: 'usual' },
      `The ${tone === 'beating' ? 'best' : 'weakest'} of the ${best.size} before did ${apart(edge, usual(post))}.`,
      [post], HERO_BASE['post-vs-previous'] + found.rank,
      { pool: sequence, base: new Set(against.map((one) => one.postKey)), baseLabel: `Previous ${best.size} ${best.plural}`, proofLabel: `This ${thing}` },
    ));
  }

  // ── the last 5 posts against the 10 before them, per feeder ──
  for (const entry of feeders) {
    const sequence = [...entry.posts].sort(byTimeAsc);
    if (sequence.length < 15) continue;
    const recent = sequence.slice(-5);
    const before = sequence.slice(-15, -5);
    const now = usualOf(recent);
    const was = usualOf(before);
    if (now == null || !was) continue;
    const ratio = now / was;
    const tone = ratio >= 1.6 && now >= 1.2 ? 'beating' : ratio <= 0.6 && now <= 0.85 ? 'sinking' : null;
    if (!tone) continue;
    candidates.push(heroFact(
      'shift', tone,
      heroSolo(scope)
        ? ['The last 5 posts', tone === 'beating' ? 'beat' : 'ran behind', 'the 10 before']
        : [`@${entry.key}’s last 5 posts`, tone === 'beating' ? 'ran ahead of' : 'ran behind', 'the 10 before'],
      { value: times(now), unit: 'usual' },
      `The 10 before typically ran ${times(was)}.`,
      recent, HERO_BASE.shift + (tone === 'beating' ? Math.min(ratio, 4) * 2 : (1 - ratio) * 6),
      { pool: sequence, base: new Set(before.map((one) => one.postKey)), baseLabel: 'The 10 before', proofLabel: 'Last 5' },
    ));
  }

  // ── a format back after a long gap, per feeder: the first carousel in 9 posts ──
  for (const entry of feeders) {
    const sequence = [...entry.posts].sort(byTimeAsc);
    if (sequence.length < 8) continue;
    for (const media of Object.keys(HERO_FORMAT)) {
      const spots = sequence.map((post, at) => (post.mediaType === media ? at : -1)).filter((at) => at >= 0);
      if (spots.length < 2) continue;
      const last = spots[spots.length - 1];
      const gap = last - spots[spots.length - 2] - 1;
      if (gap < 6 || last < sequence.length - 3) continue;
      const post = sequence[last];
      const tone = usual(post) >= 1.3 ? 'beating' : usual(post) <= 0.75 ? 'sinking' : null;
      if (!tone) continue;
      candidates.push(heroFact(
        'format-return', tone,
        [`${heroSolo(scope) ? 'Its' : `@${entry.key}’s`} first ${media} since ${(heroDay(sequence[spots[spots.length - 2]])?.label || 'then').replace(' ', '\u00a0')}`, tone === 'beating' ? 'beat' : 'sat under', 'the usual'],
        { value: times(usual(post)), unit: 'usual' },
        `${gap} posts went out in between.`,
        [post], HERO_BASE['format-return'] + Math.min(gap, 12) / 2 + (tone === 'beating' ? Math.min(usual(post), 6) : (1 - usual(post)) * 4),
        { pool: sequence, base: null, baseLabel: null, proofLabel: `This ${media}` },
      ));
    }
  }

  // ── runs: in a row, per feeder ──
  for (const [direction, keep] of [['above', (post) => usual(post) >= 1.1], ['below', (post) => usual(post) <= 0.9]]) {
    let top = null;
    for (const entry of feeders) {
      const sequence = [...entry.posts].sort(byTimeAsc);
      let run = [];
      let best = [];
      for (const post of sequence) {
        run = keep(post) ? [...run, post] : [];
        if (run.length > best.length) best = run;
      }
      if (best.length < 3) continue;
      const ongoing = best[best.length - 1] === sequence[sequence.length - 1];
      if (!top || best.length > top.run.length || (best.length === top.run.length && ongoing && !top.ongoing)) top = { run: best, ongoing, who: `@${entry.key}` };
    }
    if (!top) continue;
    const strength = usualOf(top.run);
    if (direction === 'above') {
      candidates.push(heroFact(
        'above-streak', 'beating',
        heroSolo(scope)
          ? [top.ongoing ? `The last ${top.run.length} posts` : `${top.run.length} posts in a row`, 'beat', 'the usual']
          : [top.who, top.ongoing ? 'is beating the usual' : 'beat the usual', top.ongoing ? `on ${top.run.length} posts running` : `on ${top.run.length} posts in a row`],
        { value: times(strength), unit: 'usual' },
        streakSpan(top),
        top.run, HERO_BASE['above-streak'] + top.run.length + (top.ongoing ? 2 : 0),
      ));
    } else {
      candidates.push(heroFact(
        'below-streak', 'sinking',
        heroSolo(scope)
          ? [top.ongoing ? `The last ${top.run.length} posts` : `${top.run.length} posts in a row`, 'sat under', 'the usual']
          : [top.who, top.ongoing ? 'is under the usual' : 'sat under the usual', top.ongoing ? `on ${top.run.length} posts running` : `on ${top.run.length} posts in a row`],
        { value: times(strength), unit: 'usual' },
        streakSpan(top),
        top.run, HERO_BASE['below-streak'] + top.run.length + (top.ongoing ? 2 : 0),
      ));
    }
  }

  // ── breakouts ──
  if (rated.length >= 6) {
    const past = rated.filter((post) => usual(post) >= 2);
    const ranked = [...rated].sort((a, b) => usual(b) - usual(a));
    if (past.length >= 2) {
      candidates.push(heroFact(
        'breakouts', 'beating',
        [`1 post in ${Math.max(2, Math.round(rated.length / past.length))}`, 'went past', '2× usual'],
        { value: `1 in ${Math.max(2, Math.round(rated.length / past.length))}`, unit: 'past 2×' },
        `${past.length} of ${rated.length} posts did it.`,
        past, HERO_BASE.breakouts + Math.min(past.length, 6),
      ));
    } else if (past.length === 1 && usual(ranked[0]) >= 3 && usual(ranked[1]) <= usual(ranked[0]) / 2) {
      const lone = ranked[0];
      candidates.push(heroFact(
        'lone-breakout', 'beating',
        [`One ${HERO_FORMAT[lone.mediaType] ? lone.mediaType : 'post'}`, 'ran away from', heroRest(scope)],
        { value: times(usual(lone)), unit: 'usual' },
        `The next best did ${times(usual(ranked[1]))}.`,
        [lone], HERO_BASE['lone-breakout'] + Math.min(usual(lone), 8),
      ));
    }
  }

  // ── when it went out ──
  const clocked = rated.filter((post) => heroPart(post));
  const byPart = group(clocked, heroPart).filter((entry) => entry.posts.length >= 4);
  const bestPart = [...byPart].sort((a, b) => b.posts.filter(heroBeat).length / b.posts.length - a.posts.filter(heroBeat).length / a.posts.length || b.posts.length - a.posts.length)[0];
  const worstPart = [...byPart].sort((a, b) => a.posts.filter(heroBeat).length / a.posts.length - b.posts.filter(heroBeat).length / b.posts.length || b.posts.length - a.posts.length)[0];
  if (bestPart) {
    const beat = bestPart.posts.filter(heroBeat);
    const rate = beat.length / bestPart.posts.length;
    if (rate >= 0.75 && beat.length >= 3) {
      candidates.push(heroFact(
        'time-of-day', 'beating',
        [`${HERO_PART[bestPart.key]} posts`, 'beat', 'the usual'],
        { value: `${beat.length} of ${bestPart.posts.length}`, unit: 'beat' },
        restBeat(clocked.filter((post) => heroPart(post) !== bestPart.key), 'did'),
        beat, HERO_BASE['time-of-day'] + rate * 5,
      ));
    }
  }
  if (worstPart && worstPart !== bestPart) {
    const missed = worstPart.posts.filter((post) => !heroBeat(post));
    const rate = worstPart.posts.filter(heroBeat).length / worstPart.posts.length;
    if (rate <= 0.25 && missed.length >= 3) {
      candidates.push(heroFact(
        'time-of-day', 'sinking',
        [`${HERO_PART[worstPart.key]} posts`, 'sat under', 'the usual'],
        { value: `${worstPart.posts.length - missed.length} of ${worstPart.posts.length}`, unit: 'beat' },
        restBeat(clocked.filter((post) => heroPart(post) !== worstPart.key), 'beat it'),
        missed, HERO_BASE['time-of-day'] - 4 + (1 - rate) * 5,
      ));
    }
  }
  const formatTimes = group(clocked.filter((post) => HERO_FORMAT[post.mediaType]), (post) => `${post.mediaType}:${heroPart(post)}`).filter((entry) => entry.posts.length >= 3);
  const bestFormatTime = [...formatTimes].sort((a, b) => b.posts.filter(heroBeat).length / b.posts.length - a.posts.filter(heroBeat).length / a.posts.length || b.posts.length - a.posts.length)[0];
  if (bestFormatTime) {
    const beat = bestFormatTime.posts.filter(heroBeat);
    const rate = beat.length / bestFormatTime.posts.length;
    if (rate >= 0.75 && beat.length >= 3) {
      const [media, part] = bestFormatTime.key.split(':');
      candidates.push(heroFact(
        'format-time', 'beating',
        [`${HERO_PART[part]} ${HERO_FORMAT[media]}`, 'beat', 'the usual'],
        { value: `${beat.length} of ${bestFormatTime.posts.length}`, unit: 'beat' },
        restBeat(clocked.filter((post) => post.mediaType === media && heroPart(post) !== part), 'did', HERO_FORMAT[media]),
        beat, HERO_BASE['format-time'] + rate * 5,
      ));
    }
  }

  // ── a comeback: back after a quiet stretch with a post well over the usual, per feeder ──
  let comeback = null;
  for (const entry of feeders) {
    const sequence = [...entry.posts].sort(byTimeAsc);
    for (let index = 1; index < sequence.length; index += 1) {
      const gapDays = (heroTime(sequence[index]) - heroTime(sequence[index - 1])) / 86_400_000;
      if (gapDays >= 3 && usual(sequence[index]) >= 1.5 && (!comeback || heroTime(sequence[index]) > heroTime(comeback.post))) comeback = { post: sequence[index], gapDays: Math.floor(gapDays), who: `@${entry.key}` };
    }
  }
  if (comeback) {
    candidates.push(heroFact(
      'quiet-breakout', 'beating',
      heroSolo(scope)
        ? [`A ${HERO_FORMAT[comeback.post.mediaType] ? comeback.post.mediaType : 'post'}`, 'landed after', `${comeback.gapDays} quiet days`]
        : [comeback.who, `came back after ${comeback.gapDays} quiet days`, `with a ${HERO_FORMAT[comeback.post.mediaType] ? comeback.post.mediaType : 'post'}`],
      { value: times(usual(comeback.post)), unit: 'usual' },
      '',
      [comeback.post], HERO_BASE['quiet-breakout'] + Math.min(comeback.gapDays, 10),
    ));
  }

  // ── a post that climbed between its reads ──
  const climber = [...rated]
    .filter((post) => post.firstPercentile != null && post.latestPercentile != null)
    .sort((a, b) => (number(b.firstPercentile) - number(b.latestPercentile)) - (number(a.firstPercentile) - number(a.latestPercentile)) || String(a.postKey).localeCompare(String(b.postKey)))[0];
  if (climber && number(climber.firstPercentile) - number(climber.latestPercentile) >= 5 && usual(climber) > 1) {
    candidates.push(heroFact(
      'checkpoint-climb', 'beating',
      [heroSolo(scope) ? `One ${HERO_FORMAT[climber.mediaType] ? climber.mediaType : 'post'}` : `@${bareHandle(climber) || 'a feeder'}’s ${HERO_FORMAT[climber.mediaType] ? climber.mediaType : 'post'}`, 'climbed', `between ${climber.firstCheckpoint || 'D1'} and ${climber.latestCheckpoint || 'its latest read'}`],
      { value: times(usual(climber)), unit: 'usual' },
      '',
      [climber], HERO_BASE['checkpoint-climb'] + Math.min(number(climber.firstPercentile) - number(climber.latestPercentile), 25) / 5,
    ));
  }

  // ── spread: how many of the feeders landed a post over the usual ──
  if (feeders.length >= 3) {
    const landed = feeders.filter((entry) => entry.posts.some(heroBeat));
    const rate = landed.length / feeders.length;
    const posts = landed.flatMap((entry) => entry.posts.filter(heroBeat));
    if (rate >= 0.6 && landed.length >= 3) {
      candidates.push(heroFact(
        'feeder-spread', 'beating',
        [heroUnder(scope), 'had a post over the usual from', rate === 1 ? 'every feeder' : 'most feeders'],
        { value: `${landed.length} of ${feeders.length}`, unit: 'feeders' },
        '', posts, HERO_BASE['feeder-spread'] + rate * 4,
      ));
    } else if (rate <= 0.34 && posts.length) {
      candidates.push(heroFact(
        'feeder-spread', 'sinking',
        [heroUnder(scope), 'had a post over the usual from', landed.length === 1 ? 'one feeder' : 'few feeders'],
        { value: `${landed.length} of ${feeders.length}`, unit: 'feeders' },
        '', posts, HERO_BASE['feeder-spread'] - 2 + (1 - rate) * 4,
      ));
    }
  }

  // ── cadence and chat: said plainly, neither a win nor a miss ──
  const busiest = group(rated, (post) => heroDay(post)?.key || null).filter((entry) => entry.posts.length >= 2).sort((a, b) => b.posts.length - a.posts.length || a.key.localeCompare(b.key))[0];
  if (busiest) {
    candidates.push(heroFact(
      'busiest-day', 'plain',
      ['The busiest day', 'was', heroDay(busiest.posts[0]).label.replace(' ', '\u00a0')],
      { value: String(busiest.posts.length), unit: 'posts' },
      usualDay(rated, heroSolo(scope)), busiest.posts, HERO_BASE['busiest-day'] + busiest.posts.length,
    ));
  }
  const days0 = group(rated, (post) => heroDay(post)?.key || null).map((entry) => ({ ...entry, ordinal: heroDay(entry.posts[0]).ordinal })).sort((a, b) => a.ordinal - b.ordinal);
  let dayRun = [];
  let longestDayRun = [];
  for (const day of days0) {
    dayRun = dayRun.length && day.ordinal === dayRun[dayRun.length - 1].ordinal + 1 ? [...dayRun, day] : [day];
    if (dayRun.length > longestDayRun.length) longestDayRun = [...dayRun];
  }
  if (longestDayRun.length >= 3) {
    const runPosts = longestDayRun.flatMap((day) => day.posts);
    candidates.push(heroFact(
      'publishing-run', 'plain',
      heroSolo(scope) ? ['Posts', 'went out on', `${longestDayRun.length} straight days`] : [heroUnder(scope), 'posted on', `${longestDayRun.length} straight days`],
      { value: String(longestDayRun.length), unit: 'days' },
      `${runPosts.length} posts in all.`,
      runPosts, HERO_BASE['publishing-run'] + longestDayRun.length,
    ));
  }
  const totalComments = rated.reduce((sum, post) => sum + number(post.comments), 0);
  const talked = [...rated].sort((a, b) => number(b.comments) - number(a.comments) || String(a.postKey).localeCompare(String(b.postKey)))[0];
  if (talked && totalComments > 0 && number(talked.comments) / totalComments >= 0.4 && rated.length >= 4) {
    candidates.push(heroFact(
      'post-concentration', 'plain',
      ['One post', 'drew', 'most of the comments'],
      { value: `${heroCount(number(talked.comments))} of ${heroCount(totalComments)}`, unit: 'comments' },
      '',
      [talked], HERO_BASE['post-concentration'] + (number(talked.comments) / totalComments) * 6,
    ));
  }
  const heaviest = feeders.map((entry) => ({ ...entry, comments: entry.posts.reduce((sum, post) => sum + number(post.comments), 0) })).sort((a, b) => b.comments - a.comments || a.key.localeCompare(b.key))[0];
  if (feeders.length >= 2 && heaviest && totalComments > 0 && heaviest.comments / totalComments >= 0.45) {
    candidates.push(heroFact(
      'feeder-concentration', 'plain',
      [`@${heaviest.key}`, 'drew', 'most of the comments'],
      { value: `${heroCount(heaviest.comments)} of ${heroCount(totalComments)}`, unit: 'comments' },
      `Across ${heaviest.posts.length} posts.`,
      heaviest.posts, HERO_BASE['feeder-concentration'] + (heaviest.comments / totalComments) * 6,
    ));
  }
  const imbalance = [...rated]
    .filter((post) => post.commentsMultiple != null && post.likesMultiple != null)
    .sort((a, b) => Math.abs(number(b.commentsMultiple) - number(b.likesMultiple)) - Math.abs(number(a.commentsMultiple) - number(a.likesMultiple)) || String(a.postKey).localeCompare(String(b.postKey)))[0];
  if (imbalance && Math.abs(number(imbalance.commentsMultiple) - number(imbalance.likesMultiple)) >= 0.8) {
    const louder = number(imbalance.commentsMultiple) > number(imbalance.likesMultiple);
    candidates.push(heroFact(
      'response-imbalance', 'plain',
      heroSolo(scope)
        ? [`One ${HERO_FORMAT[imbalance.mediaType] ? imbalance.mediaType : 'post'}’s ${louder ? 'comments' : 'likes'}`, 'outran', `its ${louder ? 'likes' : 'comments'}`]
        : [`One @${bareHandle(imbalance) || 'a feeder'} ${HERO_FORMAT[imbalance.mediaType] ? imbalance.mediaType : 'post'}’s ${louder ? 'comments' : 'likes'}`, 'outran', `its ${louder ? 'likes' : 'comments'}`],
      { value: times(louder ? number(imbalance.commentsMultiple) : number(imbalance.likesMultiple)), unit: louder ? 'comments' : 'likes' },
      `Its ${louder ? 'likes' : 'comments'} did ${times(louder ? number(imbalance.likesMultiple) : number(imbalance.commentsMultiple))}.`,
      [imbalance], HERO_BASE['response-imbalance'] + Math.abs(number(imbalance.commentsMultiple) - number(imbalance.likesMultiple)),
    ));
  }

  return candidates;
}

// the bars a fact draws: the posts it was counted over, oldest to newest, the ones it stands on marked (r 1), the ones
// it was set against (r 2), the rest (r 0). A plain fact (cadence, chat) has none: its bars would say a win or a miss.
const HERO_STRIP = 28;
const HERO_STRIP_MAX = 44;

function heroStrip(candidate, rated) {
  if (candidate.tone === 'plain') return null;
  const view = candidate._view || {};
  const pool = [...(view.pool || (candidate.handle ? rated.filter((post) => bareHandle(post) === candidate.handle) : rated))].sort(byTimeAsc);
  if (pool.length < 4) return null;
  const base = view.base || null;
  let start = Math.max(0, pool.length - HERO_STRIP);
  for (const holds of [(post) => candidate._evidence.has(post.postKey), (post) => Boolean(base && base.has(post.postKey))]) {
    const first = pool.findIndex(holds);
    if (first >= 0 && first < start) start = Math.max(first, pool.length - HERO_STRIP_MAX);
  }
  const bars = pool.slice(start).map((post) => ({
    k: post.postKey,
    m: Math.round(usual(post) * 100) / 100,
    r: candidate._evidence.has(post.postKey) ? 1 : base && base.has(post.postKey) ? 2 : 0,
  }));
  if (!bars.some((bar) => bar.r === 1)) return null;
  return { bars, proofLabel: view.proofLabel || null, baseLabel: view.baseLabel || null };
}

/** Deterministic hero facts for one scope and window. `scope` is { kind: 'all' | 'feed' | 'feeder', name } in the words the line uses. */
function buildHeroFacts(posts, windowDays, scope = { kind: 'all', name: 'All feeds' }) {
  const rated = posts.filter((post) => post && post.postKey && usual(post) != null);
  const candidates = heroCandidates(rated, scope);
  const perKind = new Map();
  const picked = candidates
    .sort((a, b) => b._score - a._score || a.id.localeCompare(b.id))
    .reduce((selected, candidate) => {
      if (selected.length >= HERO_MAX) return selected;
      const seen = perKind.get(`${candidate.kind}:${candidate.tone}`) || 0;
      if (seen >= HERO_KIND_CAP || hasEvidenceOverlap(selected, candidate)) return selected;
      perKind.set(`${candidate.kind}:${candidate.tone}`, seen + 1);
      return [...selected, candidate];
    }, []);

  // in the order of their weight, but two of a kind never read back to back, no tone runs three long, and a plain
  // fact never opens
  const ordered = [];
  const rest = picked.filter((one) => one.tone !== 'plain');
  const plainOnes = picked.filter((one) => one.tone === 'plain');
  while (rest.length) {
    const last = ordered[ordered.length - 1];
    const before = ordered[ordered.length - 2];
    const fits = (one) => (!last || one.kind !== last.kind) && !(last && before && last.tone === one.tone && before.tone === one.tone) && (ordered.length || one.tone !== 'plain');
    let index = rest.findIndex(fits);
    if (index < 0) index = rest.findIndex((one) => !last || one.kind !== last.kind);
    if (index < 0) index = 0;
    ordered.push(rest.splice(index, 1)[0]);
  }
  // plain cadence facts are context, not findings: they close the list
  ordered.push(...plainOnes);
  // in a feed or across feeds, a fact whose posts all belong to one feeder says whose they are
  const seenShort = new Map();
  return ordered.map((candidate) => {
    if (scope.kind !== 'feeder' && candidate.handle && !candidate.subject.includes('@')) {
      const who = `@${candidate.handle}`;
      const underName = scope.kind === 'all' ? 'Your feeds' : scope.name;
      if (candidate.kind === 'breakouts') candidate.subject = `${who}: ${candidate.subject}`;
      else if (candidate.kind === 'lone-breakout') {
        candidate.subject = `${who}’s ${candidate.subject.replace(/^One /, '')}`;
        candidate.object = 'the rest of its posts';
      } else if (candidate.subject === underName) candidate.subject = who;
      else {
        const rest = candidate.subject.replace(/^One /, '').replace(/^The /, '');
        candidate.subject = `${who}’s ${rest.charAt(0).toLowerCase()}${rest.slice(1)}`;
      }
      if (candidate.object === 'the usual') candidate.object = 'its usual';
      candidate.headline = `${candidate.subject} ${candidate.verb} ${candidate.object}`;
    }
    const taken = seenShort.get(candidate.short) || 0;
    seenShort.set(candidate.short, taken + 1);
    if (taken) candidate.short = `${candidate.short} ${taken + 1}`;
    const fact = { ...candidate, strip: heroStrip(candidate, rated) };
    delete fact._score;
    delete fact._evidence;
    delete fact._view;
    return fact;
  });
}

module.exports = { buildFeederboardFacts, buildPostMortemDeck, buildHeroFacts, dayKey, dayPart };
