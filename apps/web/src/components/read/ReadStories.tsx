'use client';

/* ─────────────────────────────────────────────────────────────
   READ STORIES — the top of a read, played the way Instagram plays
   a story. What makes it one is its top: the bars, the one in view
   filling while you read, then the next comes on by itself, and
   which story it is under them. A tap goes on (the left of its
   picture goes back), with a finger or a mouse alike; a press holds
   it while it's down. Nothing animates apart from its card.

   The stories, the same every time: the read's start (the verdict
   over every post), then Feeding, Fading, Odd, Watch (what comes
   next), what the posts are made of, each rule the crowd rewards,
   and the map. Each is a post and its reading: its number, the
   line, why, and what to do for whoever is reading (the read's
   start says who that is; tap it to read as someone else).

   - A phone: the read's start (whose, how many posts, when, for
     whom), then a stack of cards as tall as the screen once the
     app's header folds (a tap on a card not yet fully in view
     brings it up; it plays only in full view). On a card, its
     number and line over its picture's foot; a tap on them opens
     the rest (why, what to do, the post mortem and the map) as a
     Feed card's reading opens. Going on, the card slides away to
     the side and the next rises from under it; a finger drags it.
   - A laptop up (where the rail takes the chrome): the top runs
     across the whole stage, from the line the wall starts on; under
     it the stories are cards in a row: the one in view (its post at
     4:5 carrying its number, the whole reading beside it) and the
     next waiting at its side, dimmed. A tap slides the row along.

   A reel fills its picture; a post of another shape shows whole,
   over a soft wash of itself. Whose read it is is the app's
   header's to say: nothing here repeats the account's picture.
   ───────────────────────────────────────────────────────────── */

import { useCallback, useEffect, useMemo, useLayoutEffect, useRef, useState, useSyncExternalStore, type CSSProperties, type PointerEvent as RPointerEvent, type MouseEvent as RMouseEvent } from 'react';
import { linkText } from '@/components/read/readProofs';
import { TEMPO } from '@/lib/tempo';
import '@/components/read/readStories.css';

type Row = { name: string; icon?: string; n: number; landed: number };
type Proof = { id: string; label: string; x: number; date: string; likes: number };
type Story = {
  kind: string; label: string; title: string; line?: string; why?: string; cat?: number; v?: string; u?: string; so?: Record<string, string>; cover?: string | null;
  proofs?: { id: string; label: string; line: string }[];
  open?: { post?: string; law?: string; node?: string }; rows?: Row[]; sides?: { lands?: string; sinks?: string }; rec?: string;
  counts?: { posts: number; things: number; rules: number }; mosaic?: boolean; proof?: Proof; span?: string; posts?: number;
};
type Post = { id: string; thumb?: string; label?: string; lane?: string; lane_rank?: string };
type Feeder = { handle: string; posts: Post[]; reader?: { stories?: Story[]; brief?: { name: string; kind: string; whose?: string } | null } };
// from: where it was asked from (the story's card or picture), for the layer to open out of and close back into
export type ReadOpen = { file?: { post?: string; law?: string; node?: string } | true; posts?: true; from?: () => DOMRect | null };

const NONE: Story[] = [];
// v12: a story per section of the Feeder File. Its why is the reading; its action opens the map at that section
const SECTION_KINDS = new Set(['profile', 'section']);
const norm = (s: Story): Story => (SECTION_KINDS.has(s.kind) ? { ...s, line: s.line ?? s.why, open: s.open ?? { node: `cat:${s.cat ?? -1}` } } : s);
const tm = (ms: number) => ms * TEMPO;
// what settles in
const SOFT = 'cubic-bezier(.16, .9, .2, 1)';
const reduced = () => typeof window !== 'undefined' && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
// the read's chapters: the rules are one chapter however many there are. What comes next is Watch: a tap's "next" is
// the read's own way on
const CHAPTER: Record<string, string> = { verdict: 'The read', feeding: 'Feeding', fading: 'Fading', odd: 'Odd', next: 'The next test', made: 'What it posts', rule: 'Rules', end: 'The map' };
const chapterOf = (s: Story) => (s.kind === 'rule' ? s.label : CHAPTER[s.kind] || s.label);
const WHO: [string, (n: string, kind: string) => string][] = [
  ['own', (n) => `${n}’s team`], ['rival', () => 'a rival'], ['partner', (_, k) => (k === 'creator' ? 'a brand' : 'a partner')], ['learn', () => 'your own posts'],
];
const num = (v: string) => String(v || '').replace(/^-(?=\d)/, '−');

/* how long a story stays before the next comes on: long enough to read it */
const words = (t?: string) => (t ? t.replace(/\[([^\]]+)\]\([^)]+\)/g, '$1').split(/\s+/).filter(Boolean).length : 0);
const readTime = (s: Story, who: string) => {
  const w = words(s.title) + words(s.line) + words(s.u) + words(s.so?.[who]) + words(s.sides?.lands) + words(s.sides?.sinks) + words(s.rec)
    + (s.rows || []).reduce((a, r) => a + words(r.name) + 3, 0);
  return Math.min(16000, Math.max(6000, 2500 + w * 280));
};

/* who's reading, remembered on this device (and in memory where storage is off); shared with the rest of the read */
const TAKES_KEY = 'fm.read.takes';
const takesSubs = new Set<() => void>();
let takesMem: string | null = null;
const readTakes = () => {
  if (takesMem) return takesMem;
  try {
    const v = window.localStorage.getItem(TAKES_KEY);
    return v && WHO.some(([k]) => k === v) ? v : 'own';
  } catch { return 'own'; }
};
const writeTakes = (v: string) => {
  takesMem = v;
  try { window.localStorage.setItem(TAKES_KEY, v); } catch { /* storage can be off */ }
  takesSubs.forEach((f) => f());
};
const subTakes = (f: () => void) => { takesSubs.add(f); return () => { takesSubs.delete(f); }; };
// a laptop up (the card with the reading beside it, where the rail takes over the chrome) or a phone or tablet
const WIDE_Q = '(min-width: 1024px)';
const subWide = (f: () => void) => { const m = window.matchMedia(WIDE_Q); m.addEventListener('change', f); return () => m.removeEventListener('change', f); };
const readWide = () => window.matchMedia(WIDE_Q).matches;
// something open over the page (a post mortem, the map's layer, any dialog): the story holds, and the arrow keys are its.
// (The read keeps its sheet in the page, hidden, between post mortems: hidden doesn't count)
const covered = () => !!document.querySelector('#rd-ov.on')
  || Array.from(document.querySelectorAll('[aria-modal="true"]')).some((d) => {
    if (!d.getClientRects().length) return false;
    const st = getComputedStyle(d);
    return st.visibility !== 'hidden' && st.opacity !== '0';
  });

/* a post landed when it ranks in the top half of its kind's last 90 days */
const landed = (p: Post) => {
  const [r, of] = String(p.lane_rank || '').split('/').map(Number);
  return r > 0 && of > 0 && r <= of / 2;
};
/* the map, drawn: the feed round the edge in order (a mark a post, red where it landed), the things on their orbits */
function MapArt({ posts }: { posts: Post[] }) {
  const marks = posts.map((p, k) => {
    const a = -Math.PI / 2 + (k / posts.length) * Math.PI * 2;
    return { x: 200 + Math.cos(a) * 150, y: 356 + Math.sin(a) * 150, up: landed(p) };
  });
  return (
    <svg className="rs-map" viewBox="0 0 400 711" aria-hidden="true" preserveAspectRatio="xMidYMid slice">
      <circle cx="200" cy="356" r="150" />
      <circle cx="200" cy="356" r="104" />
      <circle cx="200" cy="356" r="62" />
      {marks.map((m, k) => <rect key={k} className={m.up ? 'up' : ''} x={m.x - 4} y={m.y - 5} width="8" height="10" rx="2" />)}
      <polygon className="hx up" points="140,324 153,316 166,324 166,340 153,348 140,340" />
      <polygon className="hx" points="246,282 258,275 270,282 270,296 258,303 246,296" />
      <polygon className="hx up" points="234,392 247,384 260,392 260,408 247,416 234,408" />
      <circle className="core" cx="200" cy="356" r="20" />
    </svg>
  );
}

/* a story's post on a card: a reel fills it; a post of another shape shows whole, over a soft wash of itself; the
   read's start is every post; the end is the map */
function Cover({ s, posts, post }: { s: Story; posts: Post[]; post?: Post }) {
  if (s.mosaic) return <span className="rs-mo">{posts.filter((p) => p.thumb).map((p) => <i key={p.id} style={{ backgroundImage: `url(${p.thumb})` }} />)}</span>;
  if (s.kind === 'end') return <MapArt posts={posts} />;
  if (!post?.thumb) return <span className="rs-cv rs-glow" />;
  const fit = !!post.lane && post.lane !== 'reel';
  return (
    <>
      {fit ? <span className="rs-wash" style={{ backgroundImage: `url(${post.thumb})` }} /> : null}
      <img className={`rs-cv${fit ? ' fit' : ''}`} src={post.thumb} alt="" draggable={false} />
    </>
  );
}

/* what a story holds back until its foot is opened: the reasons, what to do, where it leads */
// (the take is its own story: its line is the take for whoever is reading, so it has nothing more to open)
const hasMore = (x: Story, who: string) => x.kind !== 'end' && x.kind !== 'take' && Boolean(x.line || x.sides || x.rec || x.so?.[who] || x.proof?.id || x.open);

export default function ReadStories({ feeder, avatar = null, onOpen }: { feeder: Feeder; avatar?: string | null; onOpen: (o: ReadOpen) => void }) {
  const raw = feeder.reader?.stories || NONE;
  const stories = useMemo(() => (raw === NONE ? NONE : raw.map(norm)), [raw]);
  const n = stories.length;
  const brief = feeder.reader?.brief;
  const name = brief?.name || feeder.handle;
  const kind = brief?.kind || 'creator';
  const byId = Object.fromEntries(feeder.posts.map((p) => [p.id, p]));
  const wide = useSyncExternalStore(subWide, readWide, () => false);
  const who = useSyncExternalStore(subTakes, readTakes, () => 'own');
  const stage = useRef<HTMLDivElement | null>(null);
  const card = useRef<HTMLDivElement | null>(null);
  const bars = useRef<HTMLDivElement | null>(null);
  const head = useRef<HTMLDivElement | null>(null);
  const probe = useRef<HTMLSpanElement | null>(null);
  const [at, setAt] = useState(0);
  // a move under way to another story (a phone): the top says where it's going as the card starts to move
  const [ahead, setAhead] = useState<number | null>(null);
  // the story in view has its foot open (a phone): the reasons and what to do, over its picture
  const [open, setOpen] = useState(false);
  const atRef = useRef(0);
  const openRef = useRef(false);
  const fill = useRef<Animation | null>(null);
  // what holds the story: a press on it, its foot open, the card not fully in view
  const hold = useRef({ press: false, away: false, user: false });
  // held by its pause button (until it's played again)
  const [paused, setPaused] = useState(false);
  const press = useRef<{ x: number; y: number; t: number; id: number; dir: 0 | 1 | -1; p: number; v: [number, number][] } | null>(null);
  // a click that follows a press is that press, already acted on
  const pressed = useRef(0);
  // a move between two stories in flight (a phone): its animations, and where it lands
  const stack = useRef<{ anims: Animation[]; to: number } | null>(null);

  const go = useCallback((k: number) => {
    if (k < 0 || k >= n || k === atRef.current) return;
    atRef.current = k;
    openRef.current = false;
    setOpen(false);
    setAhead(null);
    setAt(k);
  }, [n]);

  const sync = useCallback(() => {
    const a = fill.current;
    if (!a || a.playState === 'finished') return;
    const h = hold.current;
    const stop = h.press || h.away || h.user || openRef.current || Boolean(stack.current) || document.hidden || covered();
    if (stop && a.playState === 'running') a.pause();
    else if (!stop && a.playState === 'paused') a.play();
  }, []);

  /* ── a phone: the frame stays where it is (its corners, the bars, whose stories they are) and the stories move inside
     it: going on, this one slides out to the left as the next slides in from the right, the two edge to edge; back, the
     other way. Each moves whole, its words on it; its picture moves at half its pace, so it sits a little deeper than
     its words. A finger drags it (and lets it go on or fall back); a tap or the bar running out plays the same move ── */
  const faceOf = (k: number) => card.current?.querySelector<HTMLElement>(`.rs-face[data-k="${k}"]`) || null;
  const layer = (f: HTMLElement | null, sel: string) => f?.querySelector<HTMLElement>(sel) || null;
  const tx = (x: number) => `translate3d(${x.toFixed(1)}px, 0, 0)`;
  // p: how far the move has gone (0 at rest, 1 landed)
  const pose = (dir: 1 | -1, p: number, w: number) => ({
    here: tx(-dir * p * w), herePic: tx(dir * p * w * 0.5), hereShade: 0.32 * p,
    there: tx(dir * (1 - p) * w), therePic: tx(-dir * (1 - p) * w * 0.5), thereShade: 0.32 * (1 - p),
  });
  const clearStack = useCallback(() => {
    const c = card.current;
    stack.current?.anims.forEach((a) => a.cancel());
    stack.current = null;
    c?.querySelectorAll<HTMLElement>('.rs-face').forEach((f) => {
      f.classList.remove('turn');
      f.style.transform = '';
      const m = f.querySelector<HTMLElement>('.rs-media');
      const sh = f.querySelector<HTMLElement>('.rs-shade');
      if (m) m.style.transform = '';
      if (sh) sh.style.opacity = '';
    });
  }, []);
  // the two stories a move involves: the one in view, and the one it moves to
  const pair = (dir: 1 | -1) => {
    const here = faceOf(atRef.current);
    const there = faceOf(atRef.current + dir);
    return here && there ? { here, there } : null;
  };
  const place = (dir: 1 | -1, p: number) => {
    const pr = pair(dir);
    const w = card.current?.offsetWidth || 360;
    if (!pr) return;
    const q = pose(dir, p, w);
    pr.here.classList.add('turn');
    pr.there.classList.add('turn');
    pr.here.style.transform = q.here;
    pr.there.style.transform = q.there;
    const hm = layer(pr.here, '.rs-media');
    const tm2 = layer(pr.there, '.rs-media');
    if (hm) hm.style.transform = q.herePic;
    if (tm2) tm2.style.transform = q.therePic;
    const a = layer(pr.here, '.rs-shade');
    const b = layer(pr.there, '.rs-shade');
    if (a) a.style.opacity = String(q.hereShade);
    if (b) b.style.opacity = String(q.thereShade);
  };
  const glide = useCallback((dir: 1 | -1, p0: number, done: boolean) => {
    const to = atRef.current + dir;
    if (to < 0 || to >= n) return;
    if (stack.current) { const s = stack.current; clearStack(); if (s.to !== atRef.current) go(s.to); }
    const pr = pair(dir);
    if (!pr || reduced()) { if (done) go(to); return; }
    const w = card.current?.offsetWidth || 360;
    const a = pose(dir, p0, w);
    const b = pose(dir, done ? 1 : 0, w);
    pr.here.classList.add('turn');
    pr.there.classList.add('turn');
    // a tap's move eases in and out; one a finger let go of carries on at its speed; one that falls back is quick
    const o: KeyframeAnimationOptions = done
      ? { duration: tm(p0 > 0 ? 150 + 230 * (1 - p0) : 380), easing: p0 > 0 ? 'cubic-bezier(.2, .7, .25, 1)' : 'cubic-bezier(.62, .02, .3, 1)', fill: 'forwards' }
      : { duration: tm(230), easing: 'cubic-bezier(.3, .7, .3, 1)', fill: 'forwards' };
    const hm = layer(pr.here, '.rs-media');
    const tmd = layer(pr.there, '.rs-media');
    const sa = layer(pr.here, '.rs-shade');
    const sb = layer(pr.there, '.rs-shade');
    const anims = [
      pr.here.animate([{ transform: a.here }, { transform: b.here }], o),
      pr.there.animate([{ transform: a.there }, { transform: b.there }], o),
    ];
    if (hm) anims.push(hm.animate([{ transform: a.herePic }, { transform: b.herePic }], o));
    if (tmd) anims.push(tmd.animate([{ transform: a.therePic }, { transform: b.therePic }], o));
    if (sa) anims.push(sa.animate([{ opacity: a.hereShade }, { opacity: b.hereShade }], o));
    if (sb) anims.push(sb.animate([{ opacity: a.thereShade }, { opacity: b.thereShade }], o));
    [pr.here, pr.there, hm, tmd].forEach((el) => { if (el) el.style.transform = ''; });
    if (sa) sa.style.opacity = '';
    if (sb) sb.style.opacity = '';
    const mine = { anims, to: done ? to : atRef.current };
    stack.current = mine;
    // going on: the story it leaves reads as done, and the bars and the head move on as the move starts
    if (done) { fill.current?.cancel(); fill.current = null; setAhead(to); }
    sync();
    anims[0].finished.then(() => {
      if (stack.current !== mine) return;
      if (done) go(to);
      else { clearStack(); sync(); }
    }).catch(() => undefined);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [n, go, clearStack, sync]);
  // a move landed: the story it brought rests on top, the others back in the deck
  useLayoutEffect(() => {
    clearStack();
    sync();
  }, [at, clearStack, sync]);

  // on to the next story: a phone's stack plays its move, a laptop's row slides (readStories.css)
  const next = useCallback(() => {
    if (atRef.current >= n - 1) return;
    if (readWide()) go(atRef.current + 1);
    else glide(1, 0, true);
  }, [n, go, glide]);

  // the story in view: its bar fills over the time it takes to read it, and when it's full the next comes on (the
  // last stays). A story that comes back starts its bar over
  useLayoutEffect(() => {
    const i = bars.current?.children[at]?.firstElementChild as HTMLElement | undefined;
    if (!i || !n || reduced()) return undefined;
    const a = i.animate([{ transform: 'scaleX(0)' }, { transform: 'scaleX(1)' }], { duration: readTime(stories[at], who), easing: 'linear', fill: 'forwards' });
    fill.current = a;
    sync();
    a.finished.then(() => { if (atRef.current === at && at < n - 1) next(); }).catch(() => undefined);
    return () => { a.cancel(); if (fill.current === a) fill.current = null; };
  }, [at, n, who, next, sync, stories]);

  // the bars move on as a turn starts (the story it's going to), and settle on the story in view
  const tAt = ahead ?? at;

  // a phone's foot opening (or closing) rises (or sinks) as one block from where it stood, the rest coming up a line
  // at a time as it opens, the way a Feed card's reading does
  const footFrom = useRef<number | null>(null);
  useLayoutEffect(() => {
    openRef.current = open;
    sync();
    const from = footFrom.current;
    footFrom.current = null;
    const f = faceOf(atRef.current);
    const block = layer(f, '.rs-foot');
    if (from == null || !block || reduced()) return;
    const dy = from - block.getBoundingClientRect().top;
    const o: KeyframeAnimationOptions = { duration: tm(open ? 460 : 420), easing: 'cubic-bezier(.32, .72, 0, 1)', fill: 'backwards' };
    if (Math.abs(dy) > 0.5) block.animate([{ transform: `translate3d(0, ${dy.toFixed(1)}px, 0)` }, { transform: 'none' }], o);
    if (open) {
      Array.from(layer(f, '.rs-more')?.children || []).forEach((line, k) => (line as HTMLElement).animate(
        [{ opacity: 0, transform: 'translate3d(0, 12px, 0)' }, { opacity: 1, transform: 'none' }],
        { duration: tm(380), delay: tm(110 + k * 60), easing: SOFT, fill: 'backwards' },
      ));
    }
  }, [open, sync]);
  const toggleFoot = (v?: boolean) => {
    const want = v ?? !openRef.current;
    if (want === openRef.current) return;
    footFrom.current = layer(faceOf(atRef.current), '.rs-foot')?.getBoundingClientRect().top ?? null;
    openRef.current = want;
    setOpen(want);
  };

  // a phone's card plays only in full view (under the folded header, over the nav); a laptop's while half of it shows
  useEffect(() => {
    const c = wide ? stage.current : card.current;
    if (!c) return undefined;
    const io = new IntersectionObserver(([e]) => { hold.current.away = e.intersectionRatio < (wide ? 0.5 : 0.94); sync(); },
      { threshold: [0, 0.5, 0.94, 1], rootMargin: wide ? '0px' : '-120px 0px -96px 0px' });
    io.observe(c);
    const iv = window.setInterval(sync, 400);
    document.addEventListener('visibilitychange', sync);
    return () => { io.disconnect(); window.clearInterval(iv); document.removeEventListener('visibilitychange', sync); };
  }, [sync, wide]);

  // a tap acted on at the touch's end: the click the browser sends after it (late, to whatever is under the finger by
  // then, such as a button its own tap just brought there) is swallowed before it reaches anything on the card
  useEffect(() => {
    const st = stage.current;
    if (!st) return undefined;
    const swallow = (e: MouseEvent) => {
      if (performance.now() < pressed.current && e.target instanceof Node && st.contains(e.target)) { e.preventDefault(); e.stopPropagation(); }
    };
    document.addEventListener('click', swallow, true);
    return () => document.removeEventListener('click', swallow, true);
  }, [wide]);

  // a laptop: the arrow keys move a story while it's in view and nothing is open over the read
  useEffect(() => {
    if (!wide) return undefined;
    const onKey = (e: KeyboardEvent) => {
      if (hold.current.away || e.metaKey || e.ctrlKey || e.altKey || covered()) return;
      const t = e.target as HTMLElement | null;
      if (t && (t.isContentEditable || /^(input|textarea|select)$/i.test(t.tagName))) return;
      if (e.key === 'ArrowRight') { e.preventDefault(); go(atRef.current + 1); }
      if (e.key === 'ArrowLeft') { e.preventDefault(); go(atRef.current - 1); }
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [wide, go]);

  if (!n) return null;
  const s = stories[at];
  const verdict = stories.find((x) => x.kind === 'verdict');
  const whoK = WHO.findIndex(([k]) => k === who);
  const whoLabel = WHO[Math.max(0, whoK)][1](name, kind);
  const nextWho = () => writeTakes(WHO[(whoK + 1) % WHO.length][0]);

  // what the map opens out of: the story's card (a phone), the picture of the story in view (a laptop)
  const fromCard = () => (readWide() ? stage.current?.querySelector('.rs-slide.on .rs-pic') : card.current)?.getBoundingClientRect() ?? null;
  // a phone's card not yet fully in view (the page at its top): a tap brings it up under the folded header first
  const settle = () => {
    const c = card.current;
    const room = probe.current;
    if (!c || !room) return false;
    const r = c.getBoundingClientRect();
    const top = room.offsetHeight + 12;
    if (r.top >= top - 4 && r.bottom <= window.innerHeight - 90) return false;
    // the header folds as the card comes up (the read's engine holds it folded while the page glides)
    window.dispatchEvent(new CustomEvent('rd:fold'));
    window.scrollTo({ top: Math.max(0, window.scrollY + r.top - top), behavior: reduced() ? 'auto' : 'smooth' });
    return true;
  };
  // a tap: on to the next (the last opens the map), back from the left of the picture. A phone: a tap on a story's
  // foot opens it (and anywhere, shuts it); a card not yet in full view comes up first. A laptop: a waiting card is
  // taken
  const step = (t: Element, x: number) => {
    if (t.closest('.rs-top, .rs-intro')) return;
    if (!wide) {
      if (settle()) return;
      const r = card.current?.getBoundingClientRect();
      const u = r ? (x - r.left) / r.width : 0.5;
      const last = atRef.current >= n - 1;
      const st = stories[atRef.current];
      // a third each: back, open or shut (the map's door opens the map), on
      if (u < 1 / 3) { glide(-1, 0, true); return; }
      if (u <= 2 / 3 && hasMore(st, who)) { toggleFoot(); return; }
      if (last) { onOpen({ file: true, from: fromCard }); return; }
      next();
      return;
    }
    const sl = t.closest<HTMLElement>('.rs-slide');
    if (sl && !sl.classList.contains('on')) { go(Number(sl.dataset.k)); return; }
    const r = sl?.querySelector('.rs-pic')?.getBoundingClientRect();
    if (r && x >= r.left && x <= r.right && x - r.left < r.width * 0.3) { go(atRef.current - 1); return; }
    if (atRef.current >= n - 1) { onOpen({ file: true, from: fromCard }); return; }
    next();
  };
  // a press holds the story while it's down; a short one is a tap, a long one only holds, a sideways one (a phone)
  // drags the stack and lets it go on or back
  const down = (e: RPointerEvent) => {
    if (e.button || (e.target as Element).closest('a, button')) return;
    press.current = { x: e.clientX, y: e.clientY, t: performance.now(), id: e.pointerId, dir: 0, p: 0, v: [[e.clientX, performance.now()]] };
    hold.current.press = true;
    sync();
  };
  const moveTo = (e: RPointerEvent) => {
    const p = press.current;
    if (!p || wide || p.id !== e.pointerId || openRef.current) return;
    const dx = e.clientX - p.x;
    const dy = e.clientY - p.y;
    if (!p.dir) {
      if (Math.abs(dx) < 12 || Math.abs(dx) < Math.abs(dy) * 1.2) return;
      const dir = dx < 0 ? 1 : -1;
      if (atRef.current + dir < 0 || atRef.current + dir >= n) return;
      if (stack.current) { const s = stack.current; clearStack(); if (s.to !== atRef.current) { go(s.to); press.current = null; return; } }
      p.dir = dir;
      try { (e.currentTarget as Element).setPointerCapture(e.pointerId); } catch { /* the pointer may be gone */ }
    }
    const w = card.current?.offsetWidth || 360;
    p.p = Math.min(1, Math.max(0, (p.dir > 0 ? -dx : dx) / w));
    p.v.push([e.clientX, performance.now()]);
    if (p.v.length > 6) p.v.shift();
    place(p.dir, p.p);
  };
  const up = (e: RPointerEvent) => {
    const p = press.current;
    press.current = null;
    if (!p) return;
    hold.current.press = false;
    pressed.current = performance.now() + 500;
    if (p.dir) {
      const [x0, t0] = p.v[0];
      const v = (e.clientX - x0) / Math.max(16, performance.now() - t0);
      // (a fling counts once the card has really moved)
      const fling = Math.abs(e.clientX - p.x) > 32 && (p.dir > 0 ? v < -0.45 : v > 0.45);
      glide(p.dir, p.p, p.p > 0.3 || fling);
      return;
    }
    sync();
    const dx = e.clientX - p.x;
    const dy = e.clientY - p.y;
    if (performance.now() - p.t > 360 || Math.hypot(dx, dy) > 12 || String(window.getSelection() || '')) return;
    step(e.target as Element, e.clientX);
  };
  const cancel = () => {
    const p = press.current;
    if (!p) return;
    press.current = null;
    hold.current.press = false;
    if (p.dir) glide(p.dir, p.p, false);
    else sync();
  };
  // a click with no press before it (a keyboard, an assistive tool): a tap too
  const click = (e: RMouseEvent) => {
    if (performance.now() < pressed.current) return;
    if ((e.target as Element).closest('a, button')) return;
    step(e.target as Element, e.clientX);
  };

  // which story it is: a quiet kicker on the card (the head is the feeder's)
  const kickOf = (x: Story) => <span className="rs-kk">{chapterOf(x)}</span>;
  // a story's number, big, with what it counts
  const statOf = (x: Story) => (x.kind === 'verdict'
    ? <div className="rs-stat"><b className="rs-num">{x.posts ?? feeder.posts.length}</b><span className="rs-unit">posts read{x.span ? `, ${x.span}` : ''}</span></div>
    : x.v || x.u ? <div className="rs-stat">{x.v ? <b className="rs-num">{num(x.v)}</b> : null}{x.u ? <span className="rs-unit">{x.u}</span> : null}</div> : null);
  // what to do is written once per reader (the team, a rival, a partner, someone learning): the take story shows the one
  // for whoever is reading, and the start row's switch changes it
  const titleOf = (x: Story) => (x.kind === 'verdict' ? <h2 className="rs-verdict">{x.title}</h2>
    : <h3 className="rs-claim">{x.kind === 'take' ? x.so?.[who] || x.title : x.title}</h3>);
  const rowsOf = (x: Story) => (x.rows ? (
    <ul className="rs-rows">
      {x.rows.map((r) => <li key={r.name}><b>{r.name}</b><em>{r.n} posts · {r.landed} beat</em><span className="rs-rb"><i style={{ transform: `scaleX(${(r.landed / Math.max(1, r.n)).toFixed(3)})` }} /></span></li>)}
    </ul>
  ) : null);
  // where a story leads: its post mortem, its place on the map
  const actsOf = (x: Story) => {
    const post = x.kind === 'verdict' ? null : x.proof?.id || null;
    const map = x.kind === 'end' ? true : x.open && (x.open.post || x.open.law || x.open.node) ? x.open : null;
    if (!post && !map) return null;
    return (
      <div className="rs-acts">
        {post ? (
          <a href="#" className="rs-act ql" data-post={post} onClick={(e) => e.preventDefault()}>
            Post mortem<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M7 17 17 7M8 7h9v9" /></svg>
          </a>
        ) : null}
        {map ? (
          <a href="#" className={`rs-act${post ? ' ghost' : ''}`} onClick={(e) => { e.preventDefault(); const el = e.currentTarget; onOpen({ file: map, from: () => (el.isConnected ? el.getBoundingClientRect() : fromCard()) }); }}>
            <svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="8.5" /><circle cx="12" cy="12" r="2.6" /><circle cx="18.2" cy="6.2" r="1.6" /></svg>{x.kind === 'end' ? 'Open the map' : SECTION_KINDS.has(x.kind) ? 'Open on the map' : 'On the map'}
          </a>
        ) : null}
      </div>
    );
  };
  // the rest of its reading: why, what to do (for whoever is reading), where it leads
  const moreOf = (x: Story) => (
    <>
      {x.kind !== 'made' ? rowsOf(x) : null}
      {x.sides && (x.sides.lands || x.sides.sinks) ? (
        <p className="rs-sides">{x.sides.lands ? <span className="lands"><i>{'▲︎'}</i>{x.sides.lands}</span> : null}{x.sides.sinks ? <span className="sinks"><i>{'▼︎'}</i>{x.sides.sinks}</span> : null}</p>
      ) : null}
      {x.line && x.kind !== 'end' ? <p className="rs-why" dangerouslySetInnerHTML={{ __html: linkText(x.line) }} /> : null}
      {x.proofs?.length ? <ul className="rs-pfl">{x.proofs.map((pr) => <li key={pr.id} dangerouslySetInnerHTML={{ __html: linkText(`[${pr.label}](${pr.id}): ${pr.line}`) }} />)}</ul> : null}
      {x.rec ? <span className="rs-rec">{x.rec}</span> : null}
      {x.so?.[who] && x.kind !== 'take' ? <p className="rs-so"><em>For {whoLabel}</em><span dangerouslySetInnerHTML={{ __html: linkText(x.so[who]) }} /></p> : null}
      {actsOf(x)}
    </>
  );
  // a laptop's card: the whole reading beside its post
  const bodyOf = (x: Story) => (
    <div className="rs-body">
      {kickOf(x)}
      {titleOf(x)}
      {x.kind === 'made' ? rowsOf(x) : null}
      {x.kind === 'end' && x.line ? <p className="rs-why">{x.line}</p> : null}
      {x.kind === 'end' && x.counts ? <span className="rs-rec">{x.counts.posts} posts · {x.counts.things} things · {x.counts.rules} rules</span> : null}
      {moreOf(x)}
    </div>
  );
  // a phone's foot: its number and line (what the posts are made of, the map's door, as they are), and, a tap on, the rest
  const footOf = (x: Story, j: number) => {
    const more = hasMore(x, who);
    return (
      <div className="rs-foot">
        <div className={`rs-sum${more ? ' more' : ''}`}>
          {kickOf(x)}
          {statOf(x)}
          <div className="rs-line1">
            {titleOf(x)}
            {more ? (
              <button type="button" className="rs-tog" tabIndex={j === at ? 0 : -1} onClick={() => toggleFoot()} aria-expanded={j === at && open} aria-label={j === at && open ? 'Hide the details' : 'Show the details'}>
                <svg viewBox="0 0 24 24" aria-hidden="true"><path d="m6 15 6-6 6 6" /></svg>
              </button>
            ) : null}
          </div>
          {x.kind === 'made' ? rowsOf(x) : null}
          {x.kind === 'end' ? <>{x.line ? <p className="rs-why">{x.line}</p> : null}{x.counts ? <span className="rs-rec">{x.counts.posts} posts · {x.counts.things} things · {x.counts.rules} rules</span> : null}{actsOf(x)}</> : null}
        </div>
        {more ? <div className="rs-more" inert={!(j === at && open)}>{moreOf(x)}</div> : null}
      </div>
    );
  };

  const whoChip = (
    <button type="button" className="rs-who" onClick={nextWho} aria-label={`Reading for ${whoLabel}; tap for someone else`}>
      <span>For</span>{whoLabel}<i aria-hidden="true">⌄</i>
    </button>
  );
  // the top, as Instagram's: the bars, whose stories they are (the feeder's picture and handle, and when this story's
  // post went up) and the pause. They stay put while the stories move under them, as an account's own stories do
  const whenOf = (x: Story) => (x.kind === 'verdict' || x.kind === 'end' ? '' : x.proof?.date || '');
  const face = (
    <span className="rs-av" aria-hidden="true">{avatar ? <img src={avatar} alt="" draggable={false} /> : <i>{feeder.handle.slice(0, 1).toUpperCase()}</i>}</span>
  );
  const togglePause = () => { const v = !hold.current.user; hold.current.user = v; setPaused(v); sync(); };
  const pauseBtn = (
    <button type="button" className="rs-pp" onClick={togglePause} aria-label={paused ? 'Play the stories' : 'Pause the stories'} aria-pressed={paused}>
      <svg viewBox="0 0 24 24" aria-hidden="true">{paused ? <path d="M8 5.5v13l10.5-6.5z" /> : <><rect x="6.5" y="5" width="3.6" height="14" rx="1.2" /><rect x="13.9" y="5" width="3.6" height="14" rx="1.2" /></>}</svg>
    </button>
  );
  const tw = whenOf(stories[tAt]);
  const top = (
    <div className="rs-top">
      <div ref={bars} className="rs-bars" aria-hidden="true">
        {stories.map((x, j) => <span key={j} className={`rs-bar${j < tAt ? ' past' : j === tAt ? ' on' : ''}`}><i /></span>)}
      </div>
      {wide ? (
        <div ref={head} className="rs-head">
          {face}
          <b>{feeder.handle}</b>
          <em>{tAt + 1} / {n}</em>
          {verdict ? <span className="rs-whose">{verdict.posts ?? feeder.posts.length} posts{verdict.span ? ` · ${verdict.span}` : ''}</span> : null}
          {whoChip}
          {pauseBtn}
        </div>
      ) : (
        <div className="rs-head rs-fh">
          {face}
          <b>{feeder.handle}</b>
          {tw ? <em key={tAt} className="rs-when">{tw}</em> : null}
          {pauseBtn}
        </div>
      )}
    </div>
  );
  const chevrons = (
    <>
      <span className="rs-tap prev" aria-hidden="true"><i><svg viewBox="0 0 24 24"><path d="M15 4.5 7.5 12l7.5 7.5" /></svg></i></span>
      <span className="rs-tap next" aria-hidden="true"><i><svg viewBox="0 0 24 24"><path d="M9 4.5 16.5 12 9 19.5" /></svg></i></span>
    </>
  );

  return (
    <section className="rs rd" aria-label={`The read on ${name}`} aria-roledescription="carousel">
      {/* (the folded header's foot, measured: where a phone's card comes to rest) */}
      <span ref={probe} className="rs-probe" aria-hidden="true" />
      {wide ? null : (
        /* a phone: the read's start over its card (whose read, how many posts, when, and who it's read for); scrolled
           past, the app's header folds and the card has the screen */
        <div className="rs-intro">
          <div className="rs-intro-t"><b>The read</b>{verdict ? <span>{verdict.posts ?? feeder.posts.length} posts{verdict.span ? ` · ${verdict.span}` : ''}</span> : null}</div>
          {whoChip}
        </div>
      )}
      <div ref={stage} className={`rs-stage k-${s.kind}${at === 0 ? ' first' : ''}${open ? ' open' : ''}`} onPointerDown={down} onPointerMove={moveTo} onPointerUp={up} onPointerCancel={cancel} onClick={click}>
        {/* the top, as Instagram's: the bars (those read full, the one in view filling) and which story it is */}
        {top}
        {wide ? (
          /* a laptop: the stories as cards in a row, the one in view first, the next waiting dimmed at its side */
          <div className="rs-view">
            <div className="rs-track" style={{ '--at': at } as CSSProperties}>
              {stories.map((x, j) => (
                <div key={j} className={`rs-slide k-${x.kind}${j === at ? ' on' : ''}`} data-k={j} role="group" aria-roledescription="slide" aria-label={`${j + 1} of ${n}: ${chapterOf(x)}`} aria-hidden={j !== at}>
                  <div className="rs-pic">
                    <div className="rs-media"><Cover s={x} posts={feeder.posts} post={x.cover ? byId[x.cover] : undefined} /></div>
                    <span className="rs-scrim" />
                    <div className="rs-foot">{statOf(x)}</div>
                    {j === at ? chevrons : null}
                  </div>
                  <div className="rs-sr">{bodyOf(x)}</div>
                  <span className="rs-dim" />
                </div>
              ))}
            </div>
          </div>
        ) : (
          /* a phone: the stack, every story's card in it, the one in view on top */
          <div ref={card} className="rs-card" role="group" aria-roledescription="slide" aria-label={`${at + 1} of ${n}: ${chapterOf(s)}`}>
            {stories.map((x, j) => (
              <div key={j} className={`rs-face k-${x.kind}${j === at ? ' on' : ''}${j === at && open ? ' open' : ''}`} data-k={j} aria-hidden={j !== at}>
                <div className="rs-media"><Cover s={x} posts={feeder.posts} post={x.cover ? byId[x.cover] : undefined} /></div>
                <span className="rs-scrim" />
                <span className="rs-fshade" />
                {footOf(x, j)}
                <span className="rs-shade" />
              </div>
            ))}
          </div>
        )}
      </div>
    </section>
  );
}
