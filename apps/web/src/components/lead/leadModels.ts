/* The numbers behind Lead's Growth and Habits bands. Pure functions over what the page already holds (its ranked
   posts) and what /api/lead/activity adds (every post to the minute, every follower reading). Days are IST days;
   "the window" is the selected range ending today, "prior" the equal range before it. No prose here beyond labels:
   every word a band shows is threshold math on these. Each model makes one pass over the posts, and what it makes
   for one pick is remembered (see remember): a switch back to a range or a scope already seen is a lookup. */

import type { LeadActivity } from '@/app/api/lead/activity/route';
import { normalizeHandle, titleCase } from '@/lib/feedLabels';
import { addDays, istDayKey, istDayStart, istWeekday, postMoment } from '@/lib/postMoment';

export type LeadPost = {
  postKey: string;
  postUrl: string | null;
  thumbnailUrl: string | null;
  mediaType: 'image' | 'carousel' | 'reel' | 'unknown';
  postedAt: string | null;
  handle: string | null;
  latestCheckpoint: string;
  latestPercentile: number | null;
  likes: number | null;
  comments: number | null;
  engagementRate: number | null;
  rankingMultiple: number | null;
};

export type LeadFeed = {
  id: string;
  title: string;
  feeders: Array<{ handle: string; profilePicUrl?: string | null }>;
  posts: LeadPost[];
};

export type Scope = { feeds: LeadFeed[]; activity: LeadActivity | null; feedId: string; feeder: string; days: number };

export type Format = 'reel' | 'carousel' | 'image';
export const FORMATS: Format[] = ['reel', 'carousel', 'image'];
export const FORMAT_LABEL: Record<Format, string> = { reel: 'Reels', carousel: 'Carousels', image: 'Images' };
const LETTER: Record<string, Format | null> = { r: 'reel', c: 'carousel', i: 'image', u: null };

/* one post as the bands read it: the ranked copy when there is one */
export type Shot = {
  key: string;
  feedId: string;
  handle: string;
  ms: number;
  format: Format | null;
  ranked: LeadPost | null;
  // the post's picture: the ranked copy's, or the media proxy's stored copy for posts not ranked yet
  thumb: string;
  // where it falls on the IST clock: its day, weekday (0 = Monday) and hour
  day: string;
  weekday: number;
  hour: number;
};

const proxyThumb = (postKey: string) => `/api/media?${new URLSearchParams({ postKey, role: 'thumbnail' }).toString()}`;

export type Lane = { id: string; name: string; avatar: string | null; feedId: string; handle: string; format: Format | null };

const DAY = 86_400_000;
const IST_OFFSET_MS = 330 * 60_000;

/* ── Remembered ────────────────────────────────────────────────────────── */

/* What a builder made for one set of loaded data and one pick, kept while that data is on screen: a range or a
   scope seen before is a lookup, not a rebuild. The data is the posts the page loaded (and the activity, when a
   builder reads it too); the key names the builder and the pick. The hour is part of it: the windows end at the
   clock, so a page left open reads them again once an hour. */
const NO_ACTIVITY = {};
const remembered = new WeakMap<object, WeakMap<object, { hour: number; values: Map<string, unknown> }>>();

export function remember<T>(data: object, also: object | null, key: string, build: () => T): T {
  const hour = Math.floor(Date.now() / 3_600_000);
  let byAlso = remembered.get(data);
  if (!byAlso) {
    byAlso = new WeakMap();
    remembered.set(data, byAlso);
  }
  let slot = byAlso.get(also ?? NO_ACTIVITY);
  if (!slot || slot.hour !== hour) {
    slot = { hour, values: new Map() };
    byAlso.set(also ?? NO_ACTIVITY, slot);
  }
  if (slot.values.has(key)) return slot.values.get(key) as T;
  const value = build();
  slot.values.set(key, value);
  return value;
}

const pickKey = (scope: Scope) => `${scope.feedId}|${scope.feeder}|${scope.days}`;

export function median(values: number[]) {
  const sorted = values.filter(Number.isFinite).sort((a, b) => a - b);
  if (!sorted.length) return null;
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

export function bestFirst(a: LeadPost, b: LeadPost) {
  return (a.latestPercentile ?? 101) - (b.latestPercentile ?? 101) || (b.rankingMultiple ?? 0) - (a.rankingMultiple ?? 0);
}

const percentileOf = (shot: Shot) => shot.ranked?.latestPercentile ?? null;
const EMPTY = { latestPercentile: null, rankingMultiple: null } as LeadPost;
// ranked posts first (best first), then the rest newest first
export const shotOrder = (a: Shot, b: Shot) => {
  const ranked = Number(percentileOf(b) != null) - Number(percentileOf(a) != null);
  return ranked || bestFirst(a.ranked || EMPTY, b.ranked || EMPTY) || b.ms - a.ms;
};

/* the window's first day, today, and the prior window's first day */
export function windowDays(days: number, today = istDayKey(Date.now())) {
  const start = addDays(today, -(days - 1));
  return { start, end: today, priorStart: addDays(start, -days) };
}

function scopedFeeds(scope: Scope) {
  return scope.feedId === 'all' ? scope.feeds : scope.feeds.filter((feed) => feed.id === scope.feedId);
}

/* every post in scope across both windows, ranked copy attached: the bands each ask for it, so it is built once per
   set of loaded data and scope (whatever the range) and shared */
export function shotsFor(scope: Scope): Shot[] {
  return remember(scope.feeds, scope.activity, `shots|${scope.feedId}|${scope.feeder}`, () => buildShots(scope));
}

function shotOf(key: string, feedId: string, handle: string, ms: number, format: Format | null, ranked: LeadPost | null): Shot {
  const local = ms + IST_OFFSET_MS;
  const dayNumber = Math.floor(local / DAY);
  return {
    key,
    feedId,
    handle,
    ms,
    format,
    ranked,
    thumb: ranked?.thumbnailUrl || proxyThumb(key),
    day: new Date(dayNumber * DAY).toISOString().slice(0, 10),
    // 1 Jan 1970 was a Thursday (3, Monday first)
    weekday: (((dayNumber + 3) % 7) + 7) % 7,
    hour: Math.floor((local - dayNumber * DAY) / 3_600_000),
  };
}

function buildShots(scope: Scope): Shot[] {
  const feeds = scopedFeeds(scope);
  const ranked = new Map<string, LeadPost>();
  for (const feed of feeds) for (const post of feed.posts) ranked.set(`${feed.id}:${post.postKey}`, post);
  const ids = new Set(feeds.map((feed) => feed.id));
  const shots: Shot[] = [];
  const seen = new Set<string>();
  for (const [feedId, handle, ms, letter, key] of scope.activity?.posts || []) {
    if (!ids.has(feedId) || (scope.feeder && handle !== scope.feeder)) continue;
    seen.add(`${feedId}:${key}`);
    shots.push(shotOf(key, feedId, handle, ms, LETTER[letter] ?? null, ranked.get(`${feedId}:${key}`) || null));
  }
  // ranked posts the activity call didn't carry (it hasn't arrived yet, or an edge of the range)
  for (const feed of feeds) {
    for (const post of feed.posts) {
      const handle = normalizeHandle(post.handle);
      if (seen.has(`${feed.id}:${post.postKey}`) || (scope.feeder && handle !== scope.feeder)) continue;
      const ms = postMoment(post.postedAt, post.postUrl);
      if (ms == null) continue;
      shots.push(shotOf(post.postKey, feed.id, handle, ms, post.mediaType === 'unknown' ? null : post.mediaType, post));
    }
  }
  return shots.sort((a, b) => a.ms - b.ms);
}

/* who the bands compare: feeds on All, feeders in a feed, formats for one feeder */
export function lanesFor(scope: Scope): Lane[] {
  if (scope.feeder) {
    return FORMATS.map((format) => ({ id: `format:${format}`, name: FORMAT_LABEL[format], avatar: null, feedId: scope.feedId, handle: scope.feeder, format }));
  }
  if (scope.feedId === 'all') {
    return scope.feeds.map((feed) => ({ id: `feed:${feed.id}`, name: titleCase(feed.title), avatar: feed.feeders[0]?.profilePicUrl || null, feedId: feed.id, handle: '', format: null }));
  }
  const feed = scope.feeds.find((candidate) => candidate.id === scope.feedId);
  return (feed?.feeders || []).map((feeder) => {
    const handle = normalizeHandle(feeder.handle);
    return { id: `feeder:${handle}`, name: `@${handle}`, avatar: feeder.profilePicUrl || null, feedId: feed!.id, handle, format: null };
  });
}

function laneOf(shot: Shot, lanes: Lane[]) {
  return lanes.findIndex((lane) => (lane.format ? shot.format === lane.format : lane.handle ? shot.feedId === lane.feedId && shot.handle === lane.handle : shot.feedId === lane.feedId));
}

/* ── What the bands read ───────────────────────────────────────────────── */

/* each band's model for one pick, built once (see remember) */
export const growthFor = (scope: Scope) => remember(scope.feeds, scope.activity, `growth|${pickKey(scope)}`, () => growthModel(scope, shotsFor(scope), lanesFor(scope)));
export const rhythmFor = (scope: Scope) => remember(scope.feeds, scope.activity, `rhythm|${pickKey(scope)}`, () => rhythmModel(scope, shotsFor(scope), lanesFor(scope)));
export const timingFor = (scope: Scope) => remember(scope.feeds, scope.activity, `timing|${pickKey(scope)}`, () => timingModel(scope, shotsFor(scope)));
export const formatsFor = (scope: Scope) => remember(scope.feeds, scope.activity, `formats|${pickKey(scope)}`, () => formatsModel(scope, shotsFor(scope)));

/* ── Growth ────────────────────────────────────────────────────────────── */

export type GrowthDay = {
  key: string;
  followers: number | null;
  delta: number | null;
  // no reading that day (or one that didn't refresh) for a fifth or more of the scope's followers: the curve is
  // drawn straight across the gap, so this day's move is an estimate
  estimated: boolean;
  shots: Shot[];
  best: number | null;
};

export type Moment = {
  key: string;
  index: number;
  delta: number;
  rise: boolean;
  // the strongest post that went out that day or the two before it
  after: Shot | null;
  daysBefore: number;
};

export type GrowthModel = {
  days: GrowthDay[];
  net: number | null;
  priorNet: number | null;
  first: number | null;
  last: number | null;
  // the last day with a real reading (the curve stops there)
  lastRead: number;
  moments: Moment[];
  drivers: Array<{ id: string; name: string; net: number }>;
};

/* Each feeder's readings over the whole fetched span. A big account never reads exactly the same two days running,
   so a repeat is a reading that didn't refresh and counts as missing. Missing days between two readings are drawn
   straight across (so a gap doesn't land as one cliff on the day readings resume); before the first reading and
   after the last, the feeder holds level, so it doesn't add a cliff to a sum. */
function feederSeries(followers: Array<[number, number]>, span: number) {
  const read: Array<number | null> = Array(span).fill(null);
  for (const [offset, count] of followers) if (offset >= 0 && offset < span) read[offset] = count;
  for (let index = span - 1; index > 0; index -= 1) {
    if (read[index] != null && read[index]! > 5000 && read[index] === read[index - 1]) read[index] = null;
  }
  const values: Array<number | null> = Array(span).fill(null);
  const stale = read.map((value) => value == null);
  let before = -1;
  for (let index = 0; index < span; index += 1) {
    if (read[index] == null) continue;
    if (before < 0) for (let fill = 0; fill < index; fill += 1) values[fill] = read[index];
    else for (let fill = before + 1; fill < index; fill += 1) values[fill] = read[before]! + ((read[index]! - read[before]!) * (fill - before)) / (index - before);
    values[index] = read[index];
    before = index;
  }
  if (before >= 0) for (let fill = before + 1; fill < span; fill += 1) values[fill] = read[before];
  return { values, stale };
}

function sumSeries(series: Array<{ values: Array<number | null>; stale: boolean[] }>, span: number) {
  const values: Array<number | null> = Array(span).fill(null);
  const stale: boolean[] = Array(span).fill(false);
  for (let index = 0; index < span; index += 1) {
    let total = 0;
    let missing = 0;
    let any = false;
    for (const one of series) {
      const value = one.values[index];
      if (value == null) continue;
      total += value;
      any = true;
      if (one.stale[index]) missing += value;
    }
    values[index] = any ? Math.round(total) : null;
    stale[index] = any && missing / Math.max(1, total) >= 0.2;
  }
  return { values, stale };
}

function bestOf(list: Shot[]) {
  let best: number | null = null;
  for (const shot of list) {
    const value = percentileOf(shot);
    if (value != null && (best == null || value < best)) best = value;
  }
  return best;
}

export function growthModel(scope: Scope, shots: Shot[], lanes: Lane[]): GrowthModel {
  const activity = scope.activity;
  const { start, end, priorStart } = windowDays(scope.days, activity?.to);
  const keys: string[] = [];
  for (let key = start; key <= end; key = addDays(key, 1)) keys.push(key);
  const empty: GrowthModel = { days: keys.map((key) => ({ key, followers: null, delta: null, estimated: false, shots: [], best: null })), net: null, priorNet: null, first: null, last: null, lastRead: keys.length - 1, moments: [], drivers: [] };
  if (!activity) return empty;

  const span = Math.round((istDayStart(activity.to) - istDayStart(activity.from)) / DAY) + 1;
  const offsetOf = (key: string) => Math.round((istDayStart(key) - istDayStart(activity.from)) / DAY);
  const feeders = activity.feeds
    .filter((feed) => scope.feedId === 'all' || feed.id === scope.feedId)
    .flatMap((feed) => feed.feeders
      .filter((feeder) => !scope.feeder || feeder.handle === scope.feeder)
      .map((feeder) => ({ feedId: feed.id, handle: feeder.handle, ...feederSeries(feeder.followers, span) })));
  if (!feeders.some((feeder) => feeder.values.some((value) => value != null))) return empty;
  const total = sumSeries(feeders, span);

  const startOffset = offsetOf(start);
  // past the last day with readings the curve stops (today's reading may not be in yet)
  let lastRead = span - 1;
  while (lastRead > 0 && total.stale[lastRead]) lastRead -= 1;
  const at = (offset: number) => (offset >= 0 && offset <= lastRead ? total.values[offset] : null);
  const byDay = new Map<string, Shot[]>();
  for (const shot of shots) {
    const list = byDay.get(shot.day);
    if (list) list.push(shot);
    else byDay.set(shot.day, [shot]);
  }
  // each day's posts best first, the days before the window too (a moment looks back up to two days)
  for (const list of byDay.values()) list.sort(shotOrder);

  const days: GrowthDay[] = keys.map((key, index) => {
    const offset = startOffset + index;
    const today = at(offset);
    const before = at(offset - 1);
    const own = byDay.get(key) || [];
    return {
      key,
      followers: today,
      delta: today != null && before != null ? today - before : null,
      estimated: Boolean(total.stale[offset] || total.stale[offset - 1]),
      shots: own,
      best: bestOf(own),
    };
  });

  let lastIndex = days.length - 1;
  while (lastIndex > 0 && days[lastIndex].followers == null) lastIndex -= 1;
  const last = days[lastIndex]?.followers ?? null;
  const first = at(startOffset - 1) ?? days.find((day) => day.followers != null)?.followers ?? null;
  // the window before only counts if something was read before this one began (before the first reading the curve
  // is held flat at it, which would read as 'up from 0')
  const firstRead = Math.min(...feeders.map((feeder) => feeder.stale.indexOf(false)).filter((offset) => offset >= 0));
  const priorRead = firstRead < startOffset - 1;
  const priorFirst = priorRead ? at(offsetOf(priorStart) - 1) : null;
  const priorLast = priorRead ? at(startOffset - 1) : null;

  // an estimated day's move is a share of a gap, not a day: it can't be a moment
  const measured = days.map((day, index) => ({ day, index })).filter(({ day }) => !day.estimated && day.delta != null);
  const rises = measured.filter(({ day }) => day.delta! > 0).sort((a, b) => b.day.delta! - a.day.delta!).slice(0, 3);
  const fall = measured.filter(({ day }) => day.delta! < 0).sort((a, b) => a.day.delta! - b.day.delta!)[0];
  const momentOf = ({ day, index }: { day: GrowthDay; index: number }): Moment => {
    let after: Shot | null = null;
    let daysBefore = 0;
    for (let back = 0; back <= 2; back += 1) {
      const best = (byDay.get(addDays(day.key, -back)) || [])[0];
      if (best && (!after || shotOrder(best, after) < 0)) {
        after = best;
        daysBefore = back;
      }
    }
    return { key: day.key, index, delta: day.delta ?? 0, rise: (day.delta ?? 0) > 0, after, daysBefore };
  };
  const moments = [...rises.sort((a, b) => a.index - b.index), ...(fall ? [fall] : [])].map(momentOf);

  const drivers = lanes.filter((lane) => !lane.format).map((lane) => {
    const own = feeders.filter((feeder) => feeder.feedId === lane.feedId && (!lane.handle || feeder.handle === lane.handle));
    const series = sumSeries(own, span);
    const a = series.values[startOffset - 1] ?? series.values.slice(startOffset).find((value) => value != null) ?? null;
    const b = series.values[lastRead] ?? null;
    return { id: lane.id, name: lane.name, net: a != null && b != null ? b - a : 0 };
  }).sort((a, b) => b.net - a.net);

  return {
    days,
    net: first != null && last != null ? last - first : null,
    priorNet: priorFirst != null && priorLast != null ? priorLast - priorFirst : null,
    first,
    last,
    lastRead: lastIndex,
    moments,
    drivers,
  };
}

/* ── Rhythm ────────────────────────────────────────────────────────────── */

export type CalendarDay = { key: string; count: number; today: boolean; inWindow: boolean; shots: Shot[] };
export const CALENDAR_WEEKS = 13;

export type LaneRhythm = Lane & {
  count: number;
  prior: number;
  perDay: number;
  // this lane's part of everything that went out
  share: number;
  change: number | null;
};

export type RhythmModel = {
  // the last thirteen Monday-first weeks, oldest first (a cell is null after today); the window is lit in them
  weeks: Array<{ key: string; total: number; days: Array<CalendarDay | null> }>;
  most: number;
  count: number;
  prior: number;
  perDay: number;
  priorPerDay: number;
  activeDays: number;
  days: number;
  // the usual gap between two posts and the longest quiet stretch, in hours
  gapHours: number | null;
  silenceHours: number | null;
  silenceFrom: number | null;
  lanes: LaneRhythm[];
};

export function rhythmModel(scope: Scope, shots: Shot[], lanes: Lane[]): RhythmModel {
  const { start, end } = windowDays(scope.days, scope.activity?.to);
  const startMs = istDayStart(start);
  const priorMs = startMs - scope.days * DAY;
  const endMs = istDayStart(end) + DAY;
  const now = Math.min(Date.now(), endMs);

  const byDay = new Map<string, Shot[]>();
  const laneCount = lanes.map(() => 0);
  const lanePrior = lanes.map(() => 0);
  const times: number[] = [];
  // the days with a post, from the window's posts (the calendar's thirteen weeks can start inside a 90D window)
  const active = new Set<string>();
  let prior = 0;
  // the calendar reaches back thirteen weeks whatever the window, so its size and its scale never change
  const firstMonday = addDays(end, -(istWeekday(istDayStart(end) + 3_600_000) + (CALENDAR_WEEKS - 1) * 7));
  const calendarMs = istDayStart(firstMonday);
  for (const shot of shots) {
    if (shot.ms >= endMs) continue;
    if (shot.ms >= calendarMs) {
      const list = byDay.get(shot.day);
      if (list) list.push(shot);
      else byDay.set(shot.day, [shot]);
    }
    if (shot.ms < priorMs) continue;
    const lane = laneOf(shot, lanes);
    if (shot.ms < startMs) {
      prior += 1;
      if (lane >= 0) lanePrior[lane] += 1;
      continue;
    }
    times.push(shot.ms);
    active.add(shot.day);
    if (lane >= 0) laneCount[lane] += 1;
  }

  const weeks: RhythmModel['weeks'] = [];
  let most = 0;
  for (let week = 0; week < CALENDAR_WEEKS; week += 1) {
    const monday = addDays(firstMonday, week * 7);
    let total = 0;
    const cells = Array.from({ length: 7 }, (_, weekday) => {
      const key = addDays(monday, weekday);
      if (key > end) return null;
      const own = (byDay.get(key) || []).sort(shotOrder);
      const inWindow = key >= start;
      most = Math.max(most, own.length);
      if (inWindow) total += own.length;
      return { key, count: own.length, today: key === end, inWindow, shots: own };
    });
    weeks.push({ key: monday, total, days: cells });
  }

  times.sort((a, b) => a - b);
  const gaps = times.slice(1).map((time, index) => (time - times[index]) / 3_600_000);
  // the quiet stretch runs to now if nothing has gone out since the last post
  let silenceHours: number | null = null;
  let silenceFrom: number | null = null;
  const edges = times.map((time, index) => [time, index + 1 < times.length ? times[index + 1] : now] as const);
  for (const [from, to] of edges) {
    const hours = (to - from) / 3_600_000;
    if (silenceHours == null || hours > silenceHours) {
      silenceHours = hours;
      silenceFrom = from;
    }
  }

  const count = times.length;
  return {
    weeks,
    most,
    count,
    prior,
    perDay: count / scope.days,
    priorPerDay: prior / scope.days,
    activeDays: active.size,
    days: scope.days,
    gapHours: median(gaps),
    silenceHours,
    silenceFrom,
    lanes: lanes.map((lane, index) => ({
      ...lane,
      count: laneCount[index],
      prior: lanePrior[index],
      perDay: laneCount[index] / scope.days,
      share: count ? laneCount[index] / count : 0,
      change: lanePrior[index] ? (laneCount[index] - lanePrior[index]) / lanePrior[index] : null,
    })).sort((a, b) => b.count - a.count || a.name.localeCompare(b.name)),
  };
}

/* ── Timing ────────────────────────────────────────────────────────────── */

export const WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
export const WEEKDAYS_LONG = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
// three-hour blocks of the IST day
export const BLOCKS = [0, 3, 6, 9, 12, 15, 18, 21];

export type Slot = {
  id: string;
  day: number;
  block: number;
  count: number;
  ranked: number;
  top10: number;
  typical: number | null;
  shots: Shot[];
};

export type TimingModel = {
  slots: Slot[];
  ranked: number;
  count: number;
  typical: number | null;
  // fewest ranked posts a slot needs before it can be called
  floor: number;
  best: Slot | null;
  busiest: Slot | null;
  dayTypical: Array<number | null>;
  blockTypical: Array<number | null>;
};

export function timingModel(scope: Scope, shots: Shot[]): TimingModel {
  const { start } = windowDays(scope.days, scope.activity?.to);
  const startMs = istDayStart(start);
  const lists: Shot[][] = Array.from({ length: 7 * BLOCKS.length }, () => []);
  const dayRanks: number[][] = Array.from({ length: 7 }, () => []);
  const blockRanks: number[][] = BLOCKS.map(() => []);
  const all: number[] = [];
  let count = 0;
  for (const shot of shots) {
    if (shot.ms < startMs) continue;
    count += 1;
    const day = shot.weekday;
    const block = Math.floor(shot.hour / 3);
    lists[day * BLOCKS.length + block].push(shot);
    const value = percentileOf(shot);
    if (value == null) continue;
    all.push(value);
    dayRanks[day].push(value);
    blockRanks[block].push(value);
  }
  // one lucky post can't call a time: the more ranked posts, the more a slot needs
  const floor = all.length >= 40 ? Math.max(3, Math.round(all.length / 80)) : 2;
  const slots: Slot[] = lists.map((list, index) => {
    const values = list.map(percentileOf).filter((value): value is number => value != null);
    return {
      id: `${Math.floor(index / BLOCKS.length)}:${index % BLOCKS.length}`,
      day: Math.floor(index / BLOCKS.length),
      block: index % BLOCKS.length,
      count: list.length,
      ranked: values.length,
      top10: values.filter((value) => value <= 10).length,
      typical: median(values),
      shots: list.sort(shotOrder),
    };
  });
  const callable = slots.filter((slot) => slot.ranked >= floor && slot.typical != null);
  const best = callable.slice().sort((a, b) => (a.typical ?? 101) - (b.typical ?? 101) || b.ranked - a.ranked)[0] || null;
  const busiest = slots.slice().sort((a, b) => b.count - a.count || (a.typical ?? 101) - (b.typical ?? 101))[0] || null;
  return {
    slots,
    ranked: all.length,
    count,
    typical: median(all),
    floor,
    best,
    busiest: busiest && busiest.count > 0 ? busiest : null,
    dayTypical: dayRanks.map((values) => (values.length >= floor ? median(values) : null)),
    blockTypical: blockRanks.map((values) => (values.length >= floor ? median(values) : null)),
  };
}

/* ── Formats ───────────────────────────────────────────────────────────── */

export type FormatRead = {
  format: Format;
  count: number;
  prior: number;
  share: number;
  priorShare: number;
  ranked: number;
  typical: number | null;
  priorTypical: number | null;
  // its posts that cleared the bar, and its part of everything that did
  hits: number;
  hitShare: number;
  best: Shot | null;
  er: number | null;
};

export type FormatsModel = {
  reads: FormatRead[];
  count: number;
  prior: number;
  // the bar a post has to clear to count as a hit: top 10% when there are enough of those, else wider
  bar: number;
  hits: number;
  lead: Format | null;
};

export function formatsModel(scope: Scope, shots: Shot[]): FormatsModel {
  const { start } = windowDays(scope.days, scope.activity?.to);
  const startMs = istDayStart(start);
  const priorMs = startMs - scope.days * DAY;
  const now: Record<Format, Shot[]> = { reel: [], carousel: [], image: [] };
  const before: Record<Format, Shot[]> = { reel: [], carousel: [], image: [] };
  for (const shot of shots) {
    if (!shot.format || shot.ms < priorMs) continue;
    (shot.ms >= startMs ? now : before)[shot.format].push(shot);
  }
  const count = FORMATS.reduce((sum, format) => sum + now[format].length, 0);
  const prior = FORMATS.reduce((sum, format) => sum + before[format].length, 0);
  const ranks = (list: Shot[]) => list.map(percentileOf).filter((value): value is number => value != null);
  const allRanks = FORMATS.flatMap((format) => ranks(now[format]));
  const bar = [10, 25, 50].find((cut) => allRanks.filter((value) => value <= cut).length >= 8) ?? 50;
  const hits = allRanks.filter((value) => value <= bar).length;

  const reads = FORMATS.map((format) => {
    const own = now[format];
    const values = ranks(own);
    const formatHits = values.filter((value) => value <= bar).length;
    return {
      format,
      count: own.length,
      prior: before[format].length,
      share: count ? own.length / count : 0,
      priorShare: prior ? before[format].length / prior : 0,
      ranked: values.length,
      typical: median(values),
      priorTypical: median(ranks(before[format])),
      hits: formatHits,
      hitShare: hits ? formatHits / hits : 0,
      best: own.slice().sort(shotOrder)[0] || null,
      er: median(own.map((shot) => shot.ranked?.engagementRate ?? NaN)),
    };
  });
  const lead = reads.filter((read) => read.ranked >= 3 && read.typical != null).sort((a, b) => (a.typical ?? 101) - (b.typical ?? 101))[0]?.format ?? null;
  return { reads, count, prior, bar, hits, lead };
}

/* a feed's follower curve (the board's "Follower move" and the leaders' payoff read it): its feeders' readings
   summed day by day, each carried across the days it has none */
export function ascentFromActivity(activity: LeadActivity | null, feedId: string) {
  const feed = activity?.feeds.find((candidate) => candidate.id === feedId);
  if (!activity || !feed) return [];
  const span = Math.round((istDayStart(activity.to) - istDayStart(activity.from)) / DAY) + 1;
  const total = sumSeries(feed.feeders.map((feeder) => feederSeries(feeder.followers, span)), span);
  return total.values.flatMap((value, offset) => (value == null ? [] : [{ snapshot_date_ist: addDays(activity.from, offset), follower_count: value }]));
}
