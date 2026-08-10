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

function multiple(value) {
  return `${value.toFixed(value >= 10 ? 0 : 1).replace(/\.0$/, '')}×`;
}

function parts(postedAt) {
  const date = new Date(postedAt || '');
  if (Number.isNaN(date.getTime())) return null;
  return new Intl.DateTimeFormat('en-GB', { timeZone: IST, day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', hourCycle: 'h23' }).formatToParts(date);
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
    groups.set(key, [...(groups.get(key) || []), post]);
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
    candidates.push(fact('busiest-day', `${scoped[0]?.feedName || 'This feed'} dropped ${busiest.posts.length} posts on ${dayLabel(busiest.posts[0])}.`, lead && lead.posts.length > 1 ? `${lead.key} supplied ${lead.posts.length}.` : `${contributors.length} feeders contributed.`, busiest.posts, 78 + busiest.posts.length));
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
    if (aboveCount >= 2) candidates.push(fact('format-run', `${format(formatStreak.key)} went ${aboveCount} for ${formatStreak.posts.length} above usual this window.`, `${handle(representative(formatStreak.posts))} supplied the strongest one.`, formatStreak.posts, 72 + aboveCount));
  }

  const formatTime = group(scoped.filter(above), (post) => `${post.mediaType}:${dayPart(post)}`).filter((entry) => entry.posts.length >= 2).sort((a, b) => b.posts.length - a.posts.length || a.key.localeCompare(b.key))[0];
  if (formatTime) {
    const [media, time] = formatTime.key.split(':');
    candidates.push(fact('format-time', `${format(media)} landed above usual ${formatTime.posts.length} times in the ${time}.`, `${handle(representative(formatTime.posts))} appears in that run.`, formatTime.posts, 68 + formatTime.posts.length));
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
    candidates.push(fact('response-imbalance', `${handle(imbalance)} drew ${multiple(number(imbalance.commentsMultiple))} usual comments while likes held at ${multiple(number(imbalance.likesMultiple))}.`, `${format(imbalance.mediaType)} · ${dayLabel(imbalance)}.`, [imbalance], 67 + Math.abs(number(imbalance.commentsMultiple) - number(imbalance.likesMultiple)) * 8));
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

module.exports = { buildFeederboardFacts, buildPostMortemDeck, dayKey, dayPart };
