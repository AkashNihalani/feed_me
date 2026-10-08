/* ─────────────────────────────────────────────────────────────
   FEED STREAM — the contract every piece of the new Feed tab
   builds against: the server (/api/feed/stream*), the client
   stores (lib/feedStream/store, lib/feedsStore), the layout and
   scroller (lib/feedStream/layout, components/feed/stream/
   FeedStream) and the drawn pieces (components/feed/stream/*).

   Feed is the feed: every post your feeders made in the rolling
   90 days, newest first, grouped by the IST day it went up. The
   story rail picks who (all feeds / a feed / a feeder, from
   lib/tabScope); a pinch picks how close (reels: one post at a
   time; grid: many; days: one line per day).

   Pure and dependency-free: safe to import from server routes
   and client components alike.
   ───────────────────────────────────────────────────────────── */

/* ── constants ─────────────────────────────────────────────── */

// the rolling memory: today (IST) and the 89 days before it
export const STREAM_WINDOW_DAYS = 90;
export const STREAM_CHECKPOINTS = ['D1', 'D3', 'D7', 'D21'] as const;
// a post at or under this top % wears the vibrant punch (filled rose number)
export const WINNER_MAX = 10;
// a day's best post opens its day (and is drawn big in the grid) when it is at or under this top %
export const HIGHLIGHT_MAX = 25;
// previews are only kept for recent reels; anything older shows its cover
export const PREVIEW_MAX_AGE_DAYS = 30;
// how much of a caption travels with a post (its opening; the rest is on Instagram)
export const CAPTION_MAX = 320;

/* ── data ──────────────────────────────────────────────────── */

export type StreamCheckpoint = (typeof STREAM_CHECKPOINTS)[number];
export type StreamMediaType = 'reel' | 'carousel' | 'image' | 'unknown';

// one landed checkpoint result. topPercent: where the post ranks among the feeder's own posts at that checkpoint,
// 1 = best (top 1%), 100 = bottom. Lower is stronger.
export type StreamResult = {
  checkpoint: StreamCheckpoint;
  topPercent: number | null;
  // the IST day the result landed (YYYY-MM-DD)
  day: string | null;
};

// one stored carousel slide: the media proxy's role for it (carousel_01 …) and whether it is a video
export type StreamSlide = { role: string; video: boolean };

export type StreamPost = {
  key: string; // post_key, unique across the app
  url: string | null; // the Instagram permalink
  handle: string; // lowercase, no @
  feedId: string;
  mediaType: StreamMediaType;
  postedAt: string; // ISO timestamp
  day: string; // IST day it went up (YYYY-MM-DD) — its section in 'posted' order
  resultDay: string | null; // IST day its latest checkpoint result landed — its section in 'results' order
  resultAt: string | null; // ISO moment that latest result was computed (orders a results day)
  // always the stable /api/media proxy (it redirects to the stored copy; MediaFallback covers failures)
  thumbnailUrl: string | null;
  // only set when a stored preview clip exists for this reel (never a proxied Instagram video)
  previewUrl: string | null;
  // a carousel's stored slides, in order (empty when none are stored: the poster alone is shown)
  slides: StreamSlide[];
  // the caption's opening (at most CAPTION_MAX characters), null when the post has none
  caption: string | null;
  results: StreamResult[]; // landed checkpoints, oldest first (D1 → D21)
  latestCheckpoint: StreamCheckpoint | null; // null until D1 lands
  topPercent: number | null; // at the latest checkpoint
  multiple: number | null; // the ranking metric against the feeder's usual (×usual)
  likes: number | null;
  comments: number | null;
  engagementRate: number | null; // percent, e.g. 1.37 means 1.37%
};

export type StreamDay = {
  day: string; // IST day (YYYY-MM-DD)
  // in scope: posts that went up that day ('posted') / posts whose latest result landed that day ('results')
  count: number;
  best: number | null; // the best (lowest) topPercent among them, null when none is measured yet
  // up to 4 posts for the days overview strip, best first: enough to draw it without loading the day
  covers: Array<{ key: string; thumbnailUrl: string | null; topPercent: number | null }>;
  // every post's topPercent in the day's display order (orderDayPosts): the layout sizes posts (spanOf) and packs
  // the whole 90 days exactly before a single post is loaded
  tops: Array<number | null>;
};

export type StreamHighlight = {
  key: string;
  thumbnailUrl: string | null;
  topPercent: number | null;
  mediaType: StreamMediaType;
  handle: string;
  label: string; // Best reel · Best carousel · Most liked · Big jump · This week
};

// the profile at the top of the feed: a feeder's, a feed's, or all feeds'. Every number is computed from the same
// posts and checkpoints the feed shows (no generated copy)
export type StreamProfile = {
  posts: number; // posts in the 90-day window
  typical: number | null; // median topPercent of measured posts, last 30 days
  typicalPrev: number | null; // the same, the 30 days before (for "▲ from 55%")
  followers: number | null; // latest follower count (summed across feeders for a feed / all feeds)
  followerDelta30d: number | null; // percent change over the last 30 days, e.g. 3.0
  lastPostAt: string | null; // ISO
  // the last 13 weeks (Mon–Sun, IST), oldest first: the typical post and how many went up
  weeks: Array<{ start: string; typical: number | null; count: number }>;
  highlights: StreamHighlight[]; // up to 5, distinct posts
  bestHours: { start: number; end: number; typical: number | null } | null; // best 3-hour IST window (0–23)
  strongestFormat: { type: StreamMediaType; typical: number | null } | null;
  perWeek: number; // posts a week over the window
  // feed / all feeds only (null on a feeder)
  leader: { handle: string; typical: number | null } | null; // the feeder with the best typical post (≥5 posts)
  onRun: { handle: string; winners: number } | null; // the most top-10% posts in the last 7 days
  today: { posts: number; results: number }; // posts that went up today / results that landed today
};

export type StreamIndex = {
  scopeKey: string;
  days: StreamDay[]; // newest first; only days with at least one post; within the window
  total: number; // posts in the window, in scope
  freshHandles: string[]; // feeders in scope with a post in the last 24h (they wear the rose story ring)
  profile: StreamProfile;
  generatedAt: string; // ISO
};

export type StreamPage = {
  scopeKey: string;
  from: string; // inclusive IST day range this page covers
  to: string;
  posts: StreamPost[]; // every post in scope posted from..to, newest first
};

/* ── the HTTP contract ─────────────────────────────────────────
   GET /api/feed/stream/index?feedId=<id>&handle=<handle>&order=<order>        → { index: StreamIndex }
   GET /api/feed/stream?feedId=<id>&handle=<handle>&order=<order>&from=<day>&to=<day> → { page: StreamPage }
   feedId omitted = every feed of the signed-in user; handle omitted = every feeder in scope; order omitted =
   'posted'. In 'results' order days (and from/to) are the day a post's LATEST result landed, and posts with no
   result yet are left out. 401 when signed out, 400 on a bad range, ranges are clamped to the window and to at
   most 31 days per request. */

/* ── order ─────────────────────────────────────────────────── */

// posted: the day a post went up (newest first) — what's new
// results: the day a post's latest checkpoint landed — a D7 that locked in today comes first. Each post appears
// once, at its latest result; posts not measured yet aren't in this order
export type FeedOrder = 'posted' | 'results';

/* ── scope ─────────────────────────────────────────────────── */

// the pick from lib/tabScope (feedId null = all feeds; handle null = every feeder in scope) plus the feed's order
export type StreamScope = { feedId: string | null; handle: string | null; order?: FeedOrder };

export function scopeKey(scope: StreamScope): string {
  return `${scope.feedId ?? 'all'}:${scope.handle ?? 'all'}:${scope.order ?? 'posted'}`;
}

export function scopeQuery(scope: StreamScope): URLSearchParams {
  const query = new URLSearchParams();
  if (scope.feedId) query.set('feedId', scope.feedId);
  if (scope.handle) query.set('handle', scope.handle);
  if (scope.order === 'results') query.set('order', 'results');
  return query;
}

// the day a post sits under in an order
export function sectionDayOf(post: StreamPost, order: FeedOrder = 'posted'): string | null {
  return order === 'results' ? post.resultDay : post.day;
}

/* ── zoom ──────────────────────────────────────────────────────
   One timeline, three zooms: DAY · WEEK · MONTH (the owner's call, 2026-10-07; the 90D zoom was dropped). Every
   zoom scrolls the whole 90 days; the zoom sets how much time fits on a screen and how posts are grouped:
   today (DAY): full-body posts, one per swipe (the reel card; the phone default). On a wide screen the post sits
                beside its reading. No headers: the card (or the date beside it) says its day.
   week:        3 across on a phone, every post, grouped by week (Mon–Sun, IST).
   month:       4 across, grouped by month: each month's highlights only, then "+N" for the rest.
   A post's size follows its top % (spanOf), so what mattered stands out at every zoom. */
export type FeedMode = 'today' | 'week' | 'month';
// pinching together moves right along this list (more posts per screen), spreading moves left
export const ZOOM_ORDER: readonly FeedMode[] = ['today', 'week', 'month'];
export const ZOOM_LABEL: Record<FeedMode, string> = { today: 'DAY', week: 'WEEK', month: 'MONTH' };
// columns at each zoom: phone (< 768px) / tablet (< 1100px) / desktop
export const ZOOM_COLUMNS: Record<Exclude<FeedMode, 'today'>, readonly [number, number, number]> = {
  week: [3, 4, 5],
  month: [4, 6, 7],
};
export type GroupUnit = 'day' | 'week' | 'month';
export const ZOOM_GROUP: Record<FeedMode, GroupUnit> = { today: 'day', week: 'week', month: 'month' };

// how many cells a post takes at a zoom: the top 10% are 2×2, the top 25% stand tall (1×2), everything else (and
// anything not measured yet) is one cell
export function spanOf(topPercent: number | null | undefined, mode: FeedMode): { cols: number; rows: number } {
  if (topPercent == null || mode === 'today') return { cols: 1, rows: 1 };
  if (topPercent <= WINNER_MAX) return { cols: 2, rows: 2 };
  if (topPercent <= HIGHLIGHT_MAX) return { cols: 1, rows: 2 };
  return { cols: 1, rows: 1 };
}

// the group a day belongs to at a unit: the day itself, its week's Monday, or its month (YYYY-MM)
export function groupKeyOf(day: string, unit: GroupUnit): string {
  if (unit === 'day') return day;
  if (unit === 'month') return day.slice(0, 7);
  const [year, month, date] = day.split('-').map(Number);
  const weekday = new Date(Date.UTC(year, month - 1, date)).getUTCDay();
  return shiftDay(day, -((weekday + 6) % 7));
}

const MONTH_NAMES = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

// a group's header: "Today" / "Thu 2 Oct" (day), "This week" / "Last week" / "21–27 Sep" (week), "October" (month)
export function groupLabel(key: string, unit: GroupUnit, today: string = istDay()): string {
  if (unit === 'day') return dayLabel(key, today).long;
  if (unit === 'month') return MONTH_NAMES[Number(key.slice(5, 7)) - 1];
  const thisWeek = groupKeyOf(today, 'week');
  if (key === thisWeek) return 'This week';
  if (key === shiftDay(thisWeek, -7)) return 'Last week';
  const start = dayLabel(key, today);
  const end = dayLabel(shiftDay(key, 6), today);
  return start.month === end.month ? `${start.date}–${end.date} ${end.month}` : `${start.date} ${start.month}–${end.date} ${end.month}`;
}

// a group's header in two parts: a small rose kicker over a big bold title
//   day:   TODAY / YESTERDAY / FRIDAY  over  "7 October"
//   week:  THIS WEEK / LAST WEEK / WEEK  over  "29 Sep – 5 Oct"
//   month: THIS MONTH / LAST MONTH / the year  over  "October"
const WEEKDAY_NAMES = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

export function groupParts(key: string, unit: GroupUnit, today: string = istDay()): { kicker: string; title: string } {
  if (unit === 'month') {
    const thisMonth = today.slice(0, 7);
    const [year, month] = thisMonth.split('-').map(Number);
    const lastMonth = month === 1 ? `${year - 1}-12` : `${year}-${String(month - 1).padStart(2, '0')}`;
    const kicker = key === thisMonth ? 'This month' : key === lastMonth ? 'Last month' : key.slice(0, 4);
    return { kicker, title: MONTH_NAMES[Number(key.slice(5, 7)) - 1] };
  }
  if (unit === 'day') {
    const [year, month, date] = key.split('-').map(Number);
    const weekday = WEEKDAY_NAMES[new Date(Date.UTC(year, month - 1, date)).getUTCDay()];
    const kicker = key === today ? 'Today' : key === shiftDay(today, -1) ? 'Yesterday' : weekday;
    return { kicker, title: `${date} ${MONTH_NAMES[month - 1]}` };
  }
  const thisWeek = groupKeyOf(today, 'week');
  const kicker = key === thisWeek ? 'This week' : key === shiftDay(thisWeek, -7) ? 'Last week' : 'Week';
  const start = dayLabel(key, today);
  const end = dayLabel(shiftDay(key, 6), today);
  const title = start.month === end.month ? `${start.date}–${end.date} ${end.month}` : `${start.date} ${start.month} – ${end.date} ${end.month}`;
  return { kicker, title };
}

/* ── order inside a day ────────────────────────────────────────
   The same order in every mode (so a post keeps its place when you zoom): the day's best post first when it is a
   highlight (top HIGHLIGHT_MAX% or better), then everything else newest first — newest post ('posted') or newest
   result ('results'). */

export function isWinner(topPercent: number | null | undefined): boolean {
  return topPercent != null && topPercent <= WINNER_MAX;
}

export function isHighlight(topPercent: number | null | undefined): boolean {
  return topPercent != null && topPercent <= HIGHLIGHT_MAX;
}

export function highlightOf(posts: StreamPost[]): StreamPost | null {
  let best: StreamPost | null = null;
  for (const post of posts) {
    if (post.topPercent == null) continue;
    if (!best || post.topPercent < (best.topPercent as number)
      || (post.topPercent === best.topPercent && post.postedAt > best.postedAt)) best = post;
  }
  return best && isHighlight(best.topPercent) ? best : null;
}

export function orderDayPosts(posts: StreamPost[], order: FeedOrder = 'posted'): StreamPost[] {
  const moment = (post: StreamPost) => (order === 'results' ? post.resultAt ?? post.postedAt : post.postedAt);
  const newestFirst = [...posts].sort((a, b) => {
    const left = moment(a);
    const right = moment(b);
    return left < right ? 1 : left > right ? -1 : 0;
  });
  const highlight = highlightOf(newestFirst);
  return highlight ? [highlight, ...newestFirst.filter((post) => post !== highlight)] : newestFirst;
}

/* ── days and labels ───────────────────────────────────────── */

const IST_DAY = new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Kolkata', year: 'numeric', month: '2-digit', day: '2-digit' });

// the IST calendar day of a moment, YYYY-MM-DD
export function istDay(value: Date | string | number = new Date()): string {
  return IST_DAY.format(new Date(value));
}

export function shiftDay(day: string, delta: number): string {
  const [year, month, date] = day.split('-').map(Number);
  const moment = new Date(Date.UTC(year, month - 1, date + delta));
  return moment.toISOString().slice(0, 10);
}

const WEEKDAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

export type DayLabel = {
  short: string; // the header chip: TODAY, YESTERDAY, THU 2
  long: string; // a day header: Today, Yesterday, Thu 2 Oct
  weekday: string; // Thu
  date: number; // 2
  month: string; // Oct
};

export function dayLabel(day: string, today: string = istDay()): DayLabel {
  const [year, month, date] = day.split('-').map(Number);
  const weekday = WEEKDAYS[new Date(Date.UTC(year, month - 1, date)).getUTCDay()];
  const monthName = MONTHS[month - 1];
  if (day === today) return { short: 'TODAY', long: 'Today', weekday, date, month: monthName };
  if (day === shiftDay(today, -1)) return { short: 'YESTERDAY', long: 'Yesterday', weekday, date, month: monthName };
  return { short: `${weekday.toUpperCase()} ${date}`, long: `${weekday} ${date} ${monthName}`, weekday, date, month: monthName };
}

// "now", "12m", "5h", "3d", "Thu 2 Oct"
export function relativeTime(iso: string, now: number = Date.now()): string {
  const minutes = Math.max(0, Math.round((now - new Date(iso).getTime()) / 60000));
  if (minutes < 1) return 'now';
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h`;
  const days = Math.round(hours / 24);
  if (days < 7) return `${days}d`;
  return dayLabel(istDay(iso)).long;
}

export function formatCount(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '—';
  const abs = Math.abs(value);
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(abs >= 10_000_000 ? 0 : 1).replace(/\.0$/, '')}M`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(abs >= 10_000 ? 0 : 1).replace(/\.0$/, '')}K`;
  return String(Math.round(value));
}

export function formatMultiple(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '—';
  return `${value >= 10 ? Math.round(value) : value.toFixed(1).replace(/\.0$/, '')}×`;
}

export function formatTopPercent(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '—';
  return `${Math.max(1, Math.round(value))}%`;
}

/* ── media ─────────────────────────────────────────────────── */

// the stable media proxy (the same URL shape every surface uses); role: thumbnail, preview_5s or a carousel slide's
export function mediaProxyUrl(postKey: string, role: string = 'thumbnail', sourceUrl?: string | null): string {
  const query = new URLSearchParams({ postKey, role });
  if (sourceUrl) query.set('url', sourceUrl);
  return `/api/media?${query.toString()}`;
}

/* ── the drawn pieces (components/feed/stream/*) ────────────────
   Shared rules: the scroller (FeedStream) positions every piece; a piece fills the box it is given and never
   sets a transform on its own root (the zoom transition moves the box). Every post root carries
   data-post-key={post.key}; its media box carries data-post-media. Colours come from the stream tokens
   (stream.css, on .fm-stream): --st-bg, --st-surface, --st-surface-2, --st-line, --st-text, --st-text-2,
   --st-text-3, --st-scrim, plus the app's --fm-accent / --fm-accent-bright. Only opacity and transform ever
   animate; no backdrop-filter, no filter, nothing runs forever. */

export type StreamFeeder = {
  handle: string;
  profilePicUrl: string | null;
  feedId: string;
  feedTitle: string;
  isAnchor: boolean;
  followerCount: number | null;
};

export type PostMediaProps = {
  post: StreamPost;
  variant: 'reel' | 'tile' | 'hero' | 'sheet' | 'mini';
  // reel and sheet: the card in view / the open sheet — its preview plays (one at a time, app-wide), its slides swipe
  active?: boolean;
  // the first screen: fetch at high priority
  eager?: boolean;
  // where a carousel's dots sit: over the top (the phone's Day card, whose foot carries its reading) or the foot
  controls?: 'top' | 'bottom';
  // cover: the media fills its box (the grids). frame: the box is a 4:5 frame (the wide Day view, the sheet). portrait:
  // the 4:5 frame runs across the top of a taller box (a phone's Day card). In a frame a picture taller than 4:5 fills
  // it; one 4:5 or wider shows whole over a blurred wash of itself
  fit?: 'cover' | 'frame' | 'portrait';
  className?: string;
};

export type TopBadgeProps = {
  topPercent: number | null;
  checkpoint: StreamCheckpoint | null;
  size: 'sm' | 'md' | 'lg' | 'xl';
  className?: string;
};

export type ReelCardProps = {
  post: StreamPost;
  feeder: StreamFeeder | null;
  active: boolean; // the card in view: its preview plays, its number settles in
  standout: boolean; // the very first card when it is the newest day's highlight ("Today's standout")
  // the day's first post (a phone's Day view): the date it wears, where one day breaks from the next
  dayStart: { kicker: string; title: string } | null;
  width: number;
  height: number;
  // a wide screen: the post's media is this wide, at the card's left, and its reading fills the rest of the width;
  // null: the media fills the card and the reading sits over it (a phone)
  mediaWidth: number | null;
  onOpen: (post: StreamPost) => void; // tap (a phone): the details sheet. A wide card's reading is already beside it
};

// a post in the week / month grids. Its box is cols×rows cells (spanOf). Cover text is minimal but bold: the
// number alone (rose for the top 10%), "TOP · D3" above it on a 2×2, "TOP" on a tall one. A reel shows a small ▶.
// Nothing else on the cover.
export type GridTileProps = {
  post: StreamPost | null; // null: a placeholder while its day loads
  mode: Exclude<FeedMode, 'today'>;
  cols: number;
  rows: number;
  width: number;
  height: number;
  eager: boolean;
  feeder: StreamFeeder | null; // on a 2×2 in a mixed scope (all feeds / a feed): a tiny face + @handle
  onOpen: (post: StreamPost, element: HTMLElement) => void; // tap: zoom into today at this post
};

// a group's header in the grids: a rose kicker over the group in big bold type ("FRIDAY" / "2 October"), how many
// it holds, and its best number in rose on the right
export type SectionHeaderProps = {
  label: string; // groupLabel(): the header's accessible name
  kicker: string; // groupParts()
  title: string;
  // the first group on the page has no rule over it
  first: boolean;
  count: number;
  best: number | null;
  order: FeedOrder; // words the count: "posts" / "results"
  width: number;
  height: number;
};

// the profile at the top of the feed (above the grids) and the first card at the today zoom
export type ProfileHeaderProps = {
  variant: 'page' | 'card';
  scope: StreamScope;
  title: string; // @northside.coffee · Eat · All feeds
  subtitle: string; // Cafés · anchor · last post 3h ago
  feeder: StreamFeeder | null; // a feeder: their big face
  feeds: Array<{ id: string; title: string; feederCount: number }>; // all feeds: the stacked feed badges; a feed: itself
  faces: Array<{ handle: string; profilePicUrl: string | null }>; // a feed: its feeders' faces, stacked
  profile: StreamProfile | null; // null while the index loads (draw the frame, no numbers)
  fresh: boolean; // posted in the last 24h: the big face wears the rose story ring
  // the order, as Instagram's tab row under a bio: two tabs, "Newest posts" · "Latest results", the current one
  // underlined rose (page); a compact two-way pill on the card
  order: FeedOrder;
  onOrderChange: (order: FeedOrder) => void;
  width: number;
  onLead: () => void; // "Numbers in Lead": the Lead tab, same pick
  onRead: () => void; // "Story in Read": the Read tab, same pick
  onManage: () => void; // the ⋯ button
  onOpenPost: (key: string) => void; // a highlight: today zoom at that post
};

export type PostSheetProps = {
  post: StreamPost | null; // null: closed
  feeder: StreamFeeder | null;
  onClose: () => void;
};

// the header's control (TabHeader's 44px slot): the zoom, and nothing else — the SAME control as Lead's 30D
// (TabHeader's TimeframeControl, generalised to labels): on a phone a rolling dropdown that lists only the OTHER
// zooms; from sm up a segmented track DAY · WEEK · MONTH. Never repeat a choice in two places: the order
// lives on the profile's tab row (ProfileHeaderProps.order), not here
export type FeedControlProps = {
  mode: FeedMode;
  onModeChange: (mode: FeedMode) => void;
  // a zoom about to be taken (the pointer on it, a press): the feed gets it ready
  onModeIntent?: (mode: FeedMode) => void;
};
