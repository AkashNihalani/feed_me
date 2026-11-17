/* ─────────────────────────────────────────────────────────────
   READ — the model. The shape of a feeder's drops
   (public/readdrops/<handle>.json), what a card or an open thing
   stands for at a drop, and where every tile of the Feed goes in a
   box of a given width (no measuring: the width and the count
   decide it, so a tap and React's render land on the same pixels).
   ───────────────────────────────────────────────────────────── */

export type Bands = Record<string, [number, number, number]>;
export type State = 'hot' | 'cold' | 'mixed' | 'flipped' | 'too_few';

export type Post = {
  code: string;
  cover: string;
  url: string;
  date: string;
  time: string;
  format: string;
  seconds: number | null;
  slides: number | null;
  likes: number;
  comments: number;
  // a reel's plays, where we have them
  plays?: number | null;
  x_likes: number | null;
  x_comments: number | null;
  landed: boolean;
  caption: string;
  told: string;
  // its post mortem, for the posts that were new in a drop
  line?: string | null;
  // the post replayed: a reel's moments or a carousel's slides, each a still with what it shows and what is said
  frames?: Frame[];
};
export type Frame = { t: string | null; src: string; shows: string | null; said: string | null };
export type DropPost = Post & { id: string };

export type Drop = {
  n: number;
  as_of: string;
  newest_post: string;
  label: string;
  top_line: string;
  // the hundred posts in memory, in posting order (oldest first)
  window: string[];
  new_posts: string[];
  left: string[];
  landed: number;
  // each post's rank in his last 90 days
  standing: Record<string, number>;
  posts_in_90_days?: number;
  // each reel's plays per like against his usual (the usual moves with the hundred)
  x_plays_per_like?: Record<string, number | null>;
  // phrases in top_line that lead somewhere: a file (file:F3), a row of Everything (every:places:2) or a post (post:P118)
  marks?: { phrase: string; to: string }[];
  // the questions the reader raises from this drop, for the box to roll: the line, why it is raised, and what taking it
  // does (search:<text> runs that search; every:<group>:<i> or file:<id> opens free)
  leads?: { line: string; sub: string; to: string; kind?: string }[];
  // the week's bulletin: up to five pointers, each a label, one line and its tags (one lens, up to two subjects)
  bulletin?: { label: string; line: string; lens: string; subjects: string[]; posts: string[] }[];
};
export type Evidence = { label: string; chip: string; text: string; post_ids: string[]; covers?: { id: string; cover: string; date: string }[] };
export type FeedBack = { owner: string; competitor: string; brand: string };

export type FileAt = {
  title: string;
  state: State;
  flipped_from: State | null;
  fading: boolean;
  line: string;
  reading: string;
  // the reader's explanation of where the file stands (coming to the data; the reading stands in until it does)
  summary?: string | null;
  reading_is_new: boolean;
  note: string | null;
  posts: string[];
  new: string[];
  left: string[];
  last_post: string | null;
  count: { posts: number; landed: number; comments_over_usual?: number };
  first_reading: boolean;
  scan_line: string;
  // the file shape from the files simulation: the story entry's state, evidence blocks, what the feed tells each
  // reader, what the next post settles, and the honest limit
  story_state?: string | null;
  evidence?: Evidence[];
  feed_back?: FeedBack;
  next_test?: string;
  cant_tell?: string | null;
};
export type DropFile = {
  // what the file is, fixed when it opens (the search's label, or the reader's name for it). Its title is the finding,
  // which may change from drop to drop
  name?: string | null;
  id: string;
  origin: 'reader' | 'user';
  typed: string | null;
  looking_for: string;
  question: string;
  // what a post's result is judged on in this file
  metric?: 'likes' | 'comments';
  opened_drop: number;
  followed_drop: number | null;
  by_drop: Record<string, FileAt>;
  // a file born of a question the reader raises (taking it from the box costs a scan): never handed over free
  ask?: boolean;
};
export type EveryRow = { kind: 'piece' | 'person' | 'product' | 'place'; name: string; by_drop: Record<string, { posts: string[]; count: { posts: number; landed: number }; last_post: string | null }> };
export type Search = { typed: string; ok: boolean; looking_for: string; question: string | null; reason: string | null; file: string | null; ask?: boolean };

export type Drops = {
  handle: string;
  name: string;
  bites: { scan: number; follow: number };
  drops: Drop[];
  posts: Record<string, Post>;
  files: DropFile[];
  everything: Record<'pieces' | 'people' | 'products' | 'places', EveryRow[]>;
  searches: Search[];
  bands: Bands;
};

/* ── words ── */
export const multiple = (x: number | null | undefined) => (x == null ? '' : x >= 10 ? `${Math.round(x).toLocaleString('en-US')}x` : `${x.toFixed(1)}x`);
export const thousands = (n: number) => n.toLocaleString('en-US');
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
export const shortOf = (iso: string) => {
  const [, m, d] = iso.split('-').map(Number);
  return `${d} ${MONTHS[(m || 1) - 1]}`;
};
export const STATE_WORD: Record<State, string> = { hot: 'Hot', cold: 'Cold', mixed: 'Mixed', flipped: 'Flipped', too_few: 'Too few to call' };

/* the count line a scan shows, built from a count: "6 posts. None landed. Last one 14 Oct." */
export function countLine(n: number, landed: number, last: string | null) {
  if (!n) return 'No posts in memory.';
  const k = landed === 0 ? 'None landed.' : landed === n && n > 1 ? 'All landed.' : `${landed} landed.`;
  return `${n} ${n === 1 ? 'post' : 'posts'}. ${k}${last ? ` Last one ${last}.` : ''}`;
}

/* ── where every tile goes ── */
export const ASPECT = 1.25; // portrait tiles, 4:5
export type Rect = { x: number; y: number; w: number; h: number };
export type Layout = { tiles: Map<string, Rect>; height: number; cols: number };

/* one tile size on the whole page, at rest and open, so a tile only ever moves: the wall at about 124px wide on a laptop,
   four across a phone */
export const GAP_DESK = 12;
export const GAP_PHONE = 6;
export function tileOf(W: number, phone: boolean) {
  if (phone) return Math.floor((W - 3 * GAP_PHONE) / 4);
  const cols = Math.max(4, Math.round((W + GAP_DESK) / 136));
  return Math.floor((W - (cols - 1) * GAP_DESK) / cols);
}
/* the ids laid in rows across a box W wide, tile t: left-aligned, whole pixels */
export function gridOf(ids: string[], W: number, t: number, phone: boolean): Layout {
  const gap = phone ? GAP_PHONE : GAP_DESK;
  const cols = Math.max(1, Math.floor((W + gap) / (t + gap)));
  const h = Math.round(t * ASPECT);
  const tiles = new Map<string, Rect>();
  ids.forEach((id, i) => tiles.set(id, { x: (i % cols) * (t + gap), y: Math.floor(i / cols) * (h + gap), w: t, h }));
  const rows = Math.ceil(ids.length / cols);
  return { tiles, height: rows ? rows * h + (rows - 1) * gap : 0, cols };
}

/* ── the Feed's groups: one run (Recent or posted order), or bands by where each ranks in his last 90 days ── */
export type FeedGroup = { key: string; label: string | null; ids: string[] };
export type WallView = 'recent' | 'ranks';
const BANDS = [
  { k: 'r10', l: 'Top 10', f: (v: number) => v <= 10 },
  { k: 'r25', l: '11 to 25', f: (v: number) => v > 10 && v <= 25 },
  { k: 'r50', l: '26 to 50', f: (v: number) => v > 25 && v <= 50 },
  { k: 'r99', l: 'Below 50', f: (v: number) => v > 50 },
];
export function groupsOf(ids: string[], view: WallView, standing: Record<string, number>): FeedGroup[] {
  if (view === 'recent') return [{ key: 'all', label: null, ids }];
  const ranked = ids.filter((id) => standing[id] != null).sort((a, b) => standing[a] - standing[b]);
  const out: FeedGroup[] = BANDS.map((b) => {
    const list = ranked.filter((id) => b.f(standing[id]));
    return { key: b.k, label: `${b.l} · ${list.length}`, ids: list };
  }).filter((g) => g.ids.length);
  const none = ids.filter((id) => standing[id] == null);
  if (none.length) out.push({ key: 'rnone', label: `Not ranked · ${none.length}`, ids: none });
  return out;
}

/* how many across a set is laid. A small file gets bigger pictures, so its wall stands about as tall as its reading:
   two across for up to four posts, three across for up to twelve; anything larger keeps the wall's own size. Three
   sizes in all, so a post only ever changes between sizes it has had before */
export const acrossFor = (n: number) => (n <= 4 ? 2 : n <= 12 ? 3 : 0);

/* the groups laid in rows, each under its label: tiles and labels in inner coords */
export const LABEL_H = 36;
export type GroupLayout = Layout & { labels: { key: string; text: string; x: number; y: number }[] };
export function groupedGrid(groups: FeedGroup[], W: number, base: number, phone: boolean, across?: number, size?: number, tall?: number): GroupLayout {
  const gap = phone ? GAP_PHONE : GAP_DESK;
  const natural = Math.max(1, Math.floor((W + gap) / (base + gap)));
  // (a set open on its own is laid as many across as fits it to the room: fewer, its pictures larger, or more, smaller)
  const cols = across && across > 0 ? across : natural;
  // (a set fitted to the room may be given its picture size outright: the largest that lets it stand within the screen)
  const t = size && size > 0 ? size : cols === natural ? base : Math.floor((W - (cols - 1) * gap) / cols);
  // (a set fitted to one screen is given its height too: taller than 4:5, up to the reel's own 9:16, so it fills it)
  const h = size && size > 0 && tall && tall > 0 ? tall : Math.round(t * ASPECT);
  // (a set fitted to one screen stands in the middle of the wall: what the screen's height leaves of its width is shared
  // on both sides, never left as a strip on one)
  const ox = size && size > 0 ? Math.max(0, Math.floor((W - (cols * t + (cols - 1) * gap)) / 2)) : 0;
  const tiles = new Map<string, Rect>();
  const labels: GroupLayout['labels'] = [];
  let y = 0;
  groups.forEach((g, gi) => {
    if (gi > 0) y += gap * 2;
    if (g.label) {
      labels.push({ key: g.key, text: g.label, x: ox, y });
      y += LABEL_H;
    }
    g.ids.forEach((id, i) => tiles.set(id, { x: ox + (i % cols) * (t + gap), y: y + Math.floor(i / cols) * (h + gap), w: t, h }));
    const rows = Math.ceil(g.ids.length / cols);
    y += rows ? rows * h + (rows - 1) * gap : 0;
  });
  return { tiles, labels, height: y, cols };
}

/* ── the trail: what a post beat, on the one yardstick its tile shows (its multiple of his usual likes; a tie goes to
   more likes), back through the posts before it in the set, while it keeps beating them; the first one it does not
   beat stops it ── */
export type Trail = { up: boolean; chain: string[]; end: string | null; text: string };
const better = (x: Post, y: Post) => (x.x_likes ?? 0) > (y.x_likes ?? 0) || (x.x_likes === y.x_likes && x.likes > y.likes);
export function trailOf(id: string, pool: DropPost[]): Trail {
  const ch = pool.filter((p) => p.x_likes != null);
  const i = ch.findIndex((p) => p.id === id);
  const me = ch[i];
  if (!me) return { up: false, chain: [], end: null, text: 'Too new to judge' };
  if (i <= 0) return { up: false, chain: [], end: null, text: 'The oldest one here' };
  const up = better(me, ch[i - 1]);
  const chain: string[] = [];
  let end: string | null = null;
  for (let k = i - 1; k >= 0; k -= 1) {
    if (up ? better(me, ch[k]) : better(ch[k], me)) chain.push(ch[k].id);
    else {
      end = ch[k].id;
      break;
    }
  }
  const n = chain.length;
  const text = up ? (n === 1 ? 'Beat the one before it' : n === i ? `Beat all ${n} before it` : `Beat the ${n} before it`) : n === 1 ? 'The one before it did better' : `The ${n} before it did better`;
  return { up, chain, end, text };
}

/* ── a file's SHIFT: its numbers as figures, built from the drop (no prose). The big figure is landed of posts (or, for a
   file judged on comments, posts over his usual on comments); then what this drop added, the biggest post as a
   multiple, and the last post's date ── */
export type Figure = { value: string; label: string; sub?: string | null; tone?: 'hot' | 'cold' | null; post?: string | null };
/* a file's SHIFT. One figure (how many landed), then the numbers we track, each as what a typical post in the file
   does against his usual, and every post of the file with its own numbers, so the page can show one post's in the
   same place. */
export type MetricKey = 'likes' | 'comments' | 'plays';
export type ShiftMetric = { key: MetricKey; label: string; value: string; over: boolean; lately: 'up' | 'down' | null };
export type ShiftPost = { id: string; day: string; cover: string; fresh: boolean; x: Record<MetricKey, number | null>; n: Record<MetricKey, number | null> };
export type Shift = { big: Figure; rest: Figure[]; metrics: ShiftMetric[]; posts: ShiftPost[]; lead: MetricKey };

/* the post picked on the wall, for anything that wants to follow it without the page rendering again */
let pickedId: string | null = null;
let pickedTrail = '';
// a pick is a glance first (the trail on the wall, one line about it); opened is the post itself, replayed
let pickedOpen = false;
// the trail's run: which way it went, how long it is, and how far the wall has lit it so far (the glance counts with it)
let pickedRun = { up: true, n: 0, beat: 110 };
let pickedStep = 0;
const pickedSubs = new Set<() => void>();
export const pickedPost = {
  get: () => pickedId,
  // what the wall's trail found for it ("Beat all 7 before it")
  trail: () => pickedTrail,
  run: () => pickedRun,
  step: () => pickedStep,
  isOpen: () => pickedOpen,
  set(id: string | null, trail = '', run?: { up: boolean; n: number; beat: number }) {
    if (id === pickedId) return;
    pickedId = id;
    pickedTrail = trail;
    pickedRun = run ?? { up: true, n: 0, beat: 110 };
    pickedStep = 0;
    pickedOpen = false;
    pickedSubs.forEach((f) => f());
  },
  setStep(k: number) {
    if (!pickedId || k === pickedStep) return;
    pickedStep = k;
    pickedSubs.forEach((f) => f());
  },
  setOpen(open: boolean) {
    if (!pickedId || open === pickedOpen) return;
    pickedOpen = open;
    pickedSubs.forEach((f) => f());
  },
  sub(f: () => void) {
    pickedSubs.add(f);
    return () => {
      pickedSubs.delete(f);
    };
  },
};
export const compact = (n: number) => (n >= 1e6 ? `${(n / 1e6).toFixed(n >= 1e7 ? 0 : 1)}M` : n >= 1e4 ? `${Math.round(n / 1e3)}k` : n >= 1e3 ? `${(n / 1e3).toFixed(1)}k` : String(n));
const stamp = (p: { date: string; time: string }) => Date.parse(`${p.date}T${p.time || '12:00'}:00Z`);
const median = (v: number[]) => {
  const a = [...v].sort((x, y) => x - y);
  const m = a.length >> 1;
  return a.length ? (a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2) : 0;
};
export function shiftOf(
  ids: string[],
  o: { metric?: 'likes' | 'comments'; fresh?: string[]; last?: string | null; overComments?: number; feed?: { landed: number; posts: number }; window?: string[]; plays?: Record<string, number | null>; standing?: Record<string, number>; pool?: number },
  byId: Map<string, DropPost>,
): Shift {
  const ps = (ids.map((id) => byId.get(id)).filter(Boolean) as DropPost[]).sort((a, b) => stamp(a) - stamp(b));
  const onComments = o.metric === 'comments';
  const xOf = (p: DropPost) => (onComments ? p.x_comments : p.x_likes) ?? 0;
  const otherOf = (p: DropPost) => (onComments ? p.x_likes : p.x_comments) ?? 0;
  const won = (p: DropPost) => (onComments ? (p.x_comments ?? 0) > 1 : p.landed);
  const k = onComments && o.overComments != null ? o.overComments : ps.filter(won).length;
  const big: Figure = {
    value: `${k}/${ps.length}`,
    label: onComments ? 'over his usual on comments' : 'landed',
    sub: !onComments && o.feed ? `his feed lands ${o.feed.landed} in ${o.feed.posts}` : null,
    tone: ps.length ? (k * 2 >= ps.length ? 'hot' : 'cold') : null,
  };
  const rest: Figure[] = [];
  const freshSet = new Set(o.fresh ?? []);
  if (o.fresh) {
    const fresh = o.fresh.map((id) => byId.get(id)).filter(Boolean) as DropPost[];
    const fk = fresh.filter(won).length;
    rest.push(fresh.length ? { value: `+${fresh.length}`, label: 'This drop', sub: `${fk} ${onComments ? 'over on comments' : 'landed'}`, tone: fk ? 'hot' : null } : { value: '+0', label: 'This drop', sub: 'nothing new' });
  }
  const win = (o.window ?? []).map((id) => byId.get(id)).filter(Boolean) as DropPost[];
  // its rhythm: how long since the last one, against how often it comes when it runs
  if (ps.length && win.length) {
    const DAY = 86400000;
    const lastOne = ps[ps.length - 1];
    const quiet = Math.max(0, Math.round((stamp(win[win.length - 1]) - stamp(lastOne)) / DAY));
    const gaps = ps.slice(1).map((p, i) => (stamp(p) - stamp(ps[i])) / DAY);
    const g = gaps.length >= 2 ? median(gaps) : null;
    const often = g == null ? null : g < 1.5 ? 'about daily' : `every ${Math.round(g)} days`;
    rest.push(quiet <= 1 ? { value: shortOf(lastOne.date), label: 'Last one', sub: often, post: lastOne.id } : { value: `${quiet} days`, label: 'Quiet for', sub: `last one ${shortOf(lastOne.date)}`, post: lastOne.id });
  } else if (o.last) rest.push({ value: o.last, label: 'Last one' });
  // what the file is read against: how his whole feed lands
  if (o.feed && !onComments) rest.push({ value: `${o.feed.landed} of ${o.feed.posts}`, label: 'His feed lands' });
  // the numbers we track: a typical post of the file on each, and whether the last few run above or below the rest
  const posts: ShiftPost[] = ps.map((p) => ({
    id: p.id,
    day: shortOf(p.date),
    cover: p.cover,
    fresh: freshSet.has(p.id),
    x: { likes: p.x_likes ?? null, comments: p.x_comments ?? null, plays: o.plays?.[p.id] ?? null },
    n: { likes: p.likes ?? null, comments: p.comments ?? null, plays: p.plays ?? null },
  }));
  const metric = (key: MetricKey, label: string): ShiftMetric | null => {
    const xs = posts.map((p) => p.x[key]).filter((v): v is number => v != null);
    if (!xs.length || (key === 'plays' && xs.length < 3)) return null;
    const mid = median(xs);
    let lately: ShiftMetric['lately'] = null;
    if (xs.length >= 6) {
      const now = median(xs.slice(-3));
      const was = median(xs.slice(0, -3));
      if (was > 0) lately = now / was >= 1.25 ? 'up' : now / was <= 0.8 ? 'down' : null;
    }
    return { key, label, value: multiple(mid), over: mid > 1, lately };
  };
  const metrics = [metric('likes', 'Likes'), metric('comments', 'Comments'), o.plays ? metric('plays', 'Plays per like') : null].filter(Boolean) as ShiftMetric[];
  return { big, rest, metrics, posts, lead: onComments ? 'comments' : 'likes' };
}
