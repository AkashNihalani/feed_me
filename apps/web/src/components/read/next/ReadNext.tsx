'use client';

/* ─────────────────────────────────────────────────────────────
   READ — a drop, its files, and the Feed.

   Every 10 new posts the reader writes a drop: one top line about
   where the account is, a dated line on every file that moved, a
   post mortem per new post. The page opens with that top line over
   the Ask box. Under them two shelves (Following,
   Everything) and the Feed: the hundred posts in memory.

   One mechanism: a tap changes what the page holds (React renders
   it at once, in the tap's own task: the words are small), then the
   Feed re-forms to the new set and the page is carried to where it
   now stands, both started in that same task on the same glide.
   - a card opens its file: its words beside the Feed (a laptop) or
     above it (a phone), the Feed re-formed to the file's posts in
     the order he posted them;
   - a search scans: the shelves step aside, a lens goes over every
     post, and the Feed re-forms to what it caught, with Follow;
   - Back, Esc, Show all or the same card again close it.

   It sits inside the app's own chrome: the side rail from a laptop
   up, the dock on a phone. The page scrolls on the root.
   ───────────────────────────────────────────────────────────── */

import { memo, startTransition, useCallback, useDeferredValue, useEffect, useLayoutEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react';
import { createPortal, flushSync } from 'react-dom';
import { useRouter, useSearchParams } from 'next/navigation';
import Dock from '@/components/shell/Dock';
import { SideRail } from '@/components/shell/SideRail';
import { useTabHeader } from '@/components/shell/TabHeader';
import { useMobileImmersiveViewport } from '@/lib/useMobileImmersiveViewport';
import { acquireDocumentClass, acquireRootPageScroll } from '@/lib/rootScrollMode';
import { cachedFeeds, feedOf, loadFeeds, readFeeders, readOf, type RailFeed } from '@/components/read/readFeeds';
import { setTabScope } from '@/lib/tabScope';
import { ms } from '@/lib/tempo';
import { cn } from '@/lib/utils';
import { setBands } from '../files/Pic';
import Ask, { type AskLead, type AskMatch, type AskPhase, type AskSearch } from './Ask';
import Feed, { TRAVEL_EASE, WALL_MS, type FeedApi } from './Feed';
import PostTray from './PostTray';
import { carryPage, holdHeight } from './fly';
import { GAP_DESK, GAP_PHONE, acrossFor, groupsOf, pickedPost, shiftOf, tileOf, type FeedGroup, type WallView, type DropFile, type DropPost, type Drops, type EveryRow, type FileAt } from './model';
import { Rail, type ShelfData } from './Shelf';
import { Opened, type CardData, type OpenedData } from './Words';
import './readNext.css';

const HANDLE = 'anuj.mp4';
const STORE = 'feedme:read-next:searches';
// the names a person gave their files (this browser only, in the preview)
const NAMES = 'feedme:read-next:names';
// what a row of Everything is, in a word (for the box's leads and matches)
const KIND: Record<'pieces' | 'people' | 'products' | 'places', string> = { pieces: 'Kind of post', people: 'Person', products: 'Product', places: 'Place' };
const plural = (k: number, w: string) => `${k} ${k === 1 ? w : `${w}s`}`;
const GROUPS = [
  ['pieces', 'Piece'],
  ['people', 'Person'],
  ['products', 'Product'],
  ['places', 'Place'],
] as const;

type Stage = 'rest' | 'open' | 'scan' | 'answer';
type Mode = { stage: Stage; key: string | null; question: string; notice: { title: string; text: string } | null };
const REST: Mode = { stage: 'rest', key: null, question: '', notice: null };

const wideQuery = '(min-width: 1024px)';
const subscribeWide = (cb: () => void) => {
  const m = window.matchMedia(wideQuery);
  m.addEventListener('change', cb);
  return () => m.removeEventListener('change', cb);
};
const useDesktop = () => useSyncExternalStore(subscribeWide, () => window.matchMedia(wideQuery).matches, () => true);

const loadList = (): string[] => {
  try {
    const raw = window.localStorage.getItem(STORE);
    const v = raw ? (JSON.parse(raw) as unknown) : [];
    return Array.isArray(v) ? v.filter((x): x is string => typeof x === 'string').slice(0, 5) : [];
  } catch {
    return [];
  }
};
const saveList = (list: string[]) => {
  try {
    window.localStorage.setItem(STORE, JSON.stringify(list));
  } catch {
    // storage blocked: this visit only
  }
};

// (a laptop: the reading and the wall stand pinned this far under the top of the screen)
const DESK_PIN = 16;

export default function ReadNext({ seededFeeds = null }: { seededFeeds?: RailFeed[] | null; preview?: boolean }) {
  const [data, setData] = useState<Drops | null>(null);
  useEffect(() => {
    let live = true;
    // ?src=sim reads the files simulation (readdrops/anuj-sim.json) instead of the current drops
    const sim = new URLSearchParams(window.location.search).get('src') === 'sim';
    fetch(sim ? '/readdrops/anuj-sim.json' : '/readdrops/anuj.json')
      .then((r) => r.json() as Promise<Drops>)
      .then((d) => {
        if (!live) return;
        setBands(d.bands);
        setData(d);
      })
      .catch(() => {});
    return () => {
      live = false;
    };
  }, []);
  return (
    <>
      <Chrome />
      {data ? <Page data={data} seededFeeds={seededFeeds} /> : <div className="rn rn--wait" />}
    </>
  );
}

/* the app's chrome for this page, mounted where the app's shell mounts it: on the body, outside the layer a page is
   drawn in. That layer is a transform layer, and a fixed box inside one is fixed to the layer, not the screen: drawn
   in the page, the dock sat at the page's foot and the side rail scrolled away */
function Chrome() {
  const [on, setOn] = useState(false);
  useEffect(() => setOn(true), []);
  if (!on) return null;
  return createPortal(
    <>
      <SideRail tab="read" />
      <Dock tab="read" />
    </>,
    document.body,
  );
}

/* the app's header for this tab (drawn by the side rail and the dock): its own component, so the page's changes never
   redraw the chrome. There is no timeframe on the Read */
const Header = memo(function Header({ eyebrow, feeds, seededFeeds }: { eyebrow: string; feeds: RailFeed[] | null; seededFeeds: RailFeed[] | null }) {
  const router = useRouter();
  const feedId = feeds ? feedOf(feeds, HANDLE) : undefined;
  useLayoutEffect(() => {
    setTabScope({ feedId, handle: HANDLE });
  }, [feedId]);
  const pending = useMemo(() => readFeeders(feeds), [feeds]);
  const leave = useCallback(
    (scope: { feedId: string | null | undefined; handle: string | null }) => {
      setTabScope(scope);
      if (!seededFeeds) router.push('/read');
    },
    [router, seededFeeds],
  );
  useTabHeader('read', {
    title: 'READ',
    eyebrow,
    label: `@${HANDLE}`,
    railControl: null,
    compressed: false,
    feeds,
    pendingFeeders: pending,
    feedId,
    handle: HANDLE,
    feederLabel: (h) => (readOf(h) != null ? `Read @${h}` : `@${h}, not read yet`),
    onAll: () => leave({ feedId: null, handle: null }),
    onFeed: (id) => leave({ feedId: id, handle: null }),
    onFeeder: (h) => {
      if (h !== HANDLE) leave({ feedId: feeds ? feedOf(feeds, h) : undefined, handle: h });
    },
  });
  return null;
});

/* the review control: which drop the page shows (for review only, not product: it sits at the page's foot) */
const DropSwitch = memo(function DropSwitch({ labels, n, onPick }: { labels: string[]; n: number; onPick: (n: number) => void }) {
  return (
    <div className="rn-review" role="group" aria-label="Review: which drop">
      <span className="rn-review__k">Review drop</span>
      {labels.map((l, i) => (
        <button key={l} type="button" aria-pressed={n === i + 1} onClick={() => onPick(i + 1)}>
          {l}
        </button>
      ))}
    </div>
  );
});

/* the drop's note: the reader's line for this drop, the page's lead sentence, under the search. A phone holds it to
   three lines with a way to read the rest; a laptop shows it whole. Its marked phrases are leads: a tap puts one in the
   box (it does not act; the person presses send) */
type Mark = { phrase: string; to: string };
function marked(line: string, marks: Mark[]): (string | Mark)[] {
  const at = marks
    .map((m) => ({ m, i: line.indexOf(m.phrase) }))
    .filter((x) => x.i >= 0)
    .sort((a, b) => a.i - b.i);
  const out: (string | Mark)[] = [];
  let from = 0;
  at.forEach(({ m, i }) => {
    if (i < from) return;
    if (i > from) out.push(line.slice(from, i));
    out.push(m);
    from = i + m.phrase.length;
  });
  if (from < line.length) out.push(line.slice(from));
  return out;
}
type Pointer = NonNullable<Drops['drops'][number]['bulletin']>[number];
const DropNote = memo(function DropNote({ n, label, line, marks, phone, onMark, bulletin, onTag }: { n: number; label: string; line: string; marks: Mark[]; phone: boolean; onMark: (m: Mark) => void; bulletin?: Pointer[]; onTag?: (tag: string) => void }) {
  const [all, setAll] = useState(false);
  const [long, setLong] = useState(false);
  const ref = useRef<HTMLParagraphElement | null>(null);
  useEffect(() => setAll(false), [n]);
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el || !phone) {
      setLong(false);
      return undefined;
    }
    if (all) return undefined;
    const check = () => setLong(el.scrollHeight > el.clientHeight + 1);
    check();
    const ro = new ResizeObserver(check);
    ro.observe(el);
    return () => ro.disconnect();
  }, [line, phone, all]);
  return (
    <section className="rn-drop" aria-label="This drop">
      <p className="rn-drop__k">
        <b>This drop</b>
        <span>{label}</span>
      </p>
      <p ref={ref} id="rn-drop-line" className={cn('rn-drop__t', phone && !all && 'is-held')} key={n}>
        {marked(line, marks).map((part, i) =>
          typeof part === 'string' ? (
            part
          ) : (
            <span
              key={`${part.to}:${i}`}
              role="button"
              tabIndex={0}
              className="rn-cue"
              onClick={() => onMark(part)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault();
                  onMark(part);
                }
              }}
            >
              {part.phrase}
            </span>
          ),
        )}
      </p>
      {phone && (long || all) ? (
        <button type="button" className="rn-drop__more" aria-expanded={all} aria-controls="rn-drop-line" onClick={() => setAll((v) => !v)}>
          {all ? 'Less' : 'More'}
        </button>
      ) : null}
      {bulletin?.length ? (
        <ul className="rn-bul" key={`b${n}`} aria-label="Bulletin">
          {bulletin.map((p, i) => (
            <li key={`${p.label}:${i}`}>
              <span className="rn-bul__k">{p.label}</span>
              <p className="rn-bul__l">{p.line}</p>
              <span className="rn-bul__tags">
                {[p.lens, ...p.subjects].map((t, j) => (
                  <button key={t} type="button" className={cn('rn-tag', j === 0 && 'is-lens')} onClick={() => onTag?.(t)}>
                    {t}
                  </button>
                ))}
              </span>
            </li>
          ))}
        </ul>
      ) : null}
    </section>
  );
});

/* the person's own searches under the box, one row that swipes: their recent searches first, then the ready ones the
   reader can answer. Each a small raised token with its file's newest post and how many posts it holds. A tap sends it */
/* (words too common to find posts by) */
const QUIET_WORDS = new Set(['videos', 'video', 'posts', 'post', 'where', 'when', 'with', 'than', 'more', 'plays', 'that', 'this', 'stuff', 'someone', 'something', 'takes', 'about', 'from', 'what', 'does', 'they', 'them', 'their', 'his', 'him', 'her', 'anuj']);
type Ready = { text: string; cover: string | null; covers: string[]; posts: number | null };
const ReadyChips = memo(function ReadyChips({ list, busy, onSend }: { list: Ready[]; busy: boolean; onSend: (t: string) => void }) {
  if (!list.length) return null;
  return (
    <div className="rn-ready" role="group" aria-label="Your searches">
      {list.map((r) => (
        <button key={r.text} type="button" className="rn-ready__c" aria-disabled={busy} onClick={() => !busy && onSend(r.text)}>
          {r.cover ? (
            <span className="rn-ready__pic" aria-hidden="true">
              <img src={r.cover} alt="" loading="lazy" decoding="async" draggable={false} />
            </span>
          ) : null}
          <span className="rn-ready__t">
            <span className="rn-ready__p">{r.text}</span>
            {r.posts != null ? (
              <small>
                {r.posts} {r.posts === 1 ? 'post' : 'posts'}
              </small>
            ) : null}
          </span>
        </button>
      ))}
    </div>
  );
});

function Page({ data, seededFeeds }: { data: Drops; seededFeeds: RailFeed[] | null }) {
  const params = useSearchParams();
  const desktop = useDesktop();
  const phone = !desktop;
  const reduce = useRef(false);
  useEffect(() => {
    reduce.current = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  }, []);
  /* a phone runs edge to edge as the app's tabs do: the page scrolls on the root */
  const { appShellStyle, useBrowserPageScroll } = useMobileImmersiveViewport();
  useEffect(() => {
    if (!useBrowserPageScroll) return undefined;
    const body = document.body;
    const prev = body.style.overflow;
    body.style.overflow = 'visible';
    return () => {
      body.style.overflow = prev;
    };
  }, [useBrowserPageScroll]);
  useEffect(() => acquireDocumentClass('rn-route'), []);
  useEffect(() => acquireRootPageScroll(), []);
  useEffect(() => {
    const was = window.history.scrollRestoration;
    window.history.scrollRestoration = 'manual';
    return () => {
      window.history.scrollRestoration = was;
    };
  }, []);

  const [feeds, setFeeds] = useState<RailFeed[] | null>(() => seededFeeds ?? cachedFeeds());
  useEffect(() => {
    if (seededFeeds) return undefined;
    const c = new AbortController();
    loadFeeds(c.signal).then(setFeeds).catch(() => {});
    return () => c.abort();
  }, [seededFeeds]);

  /* ── the posts: all of them, mounted once ── */
  const posts = useMemo<DropPost[]>(() => Object.entries(data.posts).map(([id, p]) => ({ ...p, id })), [data]);
  const byId = useMemo(() => new Map(posts.map((p) => [p.id, p])), [posts]);
  const posted = useCallback((ids: string[]) => ids.filter((id) => byId.has(id)).sort((a, b) => (byId.get(b)!.date + byId.get(b)!.time).localeCompare(byId.get(a)!.date + byId.get(a)!.time)), [byId]);

  /* ── the drop on show (review control; the latest by default) ── */
  const [n, setN] = useState(() => {
    const d = Number(params.get('drop'));
    return d >= 1 && d <= data.drops.length ? d : data.drops.length;
  });
  const drop = data.drops[n - 1];
  const windowIds = useMemo(() => [...drop.window].reverse().filter((id) => byId.has(id)), [drop, byId]);
  const fresh = useMemo(() => new Set(drop.new_posts), [drop]);
  const year = drop.as_of.slice(0, 4);
  const dayOf = useCallback((label: string | null) => (label ? Date.parse(`${label} ${year}`) || 0 : 0), [year]);

  /* ── what is followed: the data's, and what was followed on this visit ── */
  const [mine, setMine] = useState<string[]>([]);
  const fileAt = useCallback((f: DropFile, d = n): FileAt | null => f.by_drop[String(d)] ?? null, [n]);
  const isFollowed = useCallback((key: string) => {
    if (mine.includes(key)) return true;
    if (key.startsWith('file:')) {
      const f = data.files.find((x) => `file:${x.id}` === key);
      return Boolean(f && f.followed_drop != null && f.followed_drop <= n);
    }
    return false;
  }, [mine, data, n]);

  /* ── the shelves ── */
  const everyOf = useCallback((key: string): { row: EveryRow; group: string } | null => {
    const [, g, i] = key.split(':');
    const list = data.everything[g as keyof Drops['everything']];
    const row = list?.[Number(i)];
    const group = GROUPS.find((x) => x[0] === g)?.[1] ?? '';
    return row ? { row, group } : null;
  }, [data]);
  /* ── a file's name: the reader titles it; a person can rename it, and the reader's title stays under it ── */
  const [names, setNames] = useState<Record<string, string>>({});
  useEffect(() => {
    try {
      const v = JSON.parse(window.localStorage.getItem(NAMES) ?? '{}') as unknown;
      if (v && typeof v === 'object' && !Array.isArray(v)) setNames(v as Record<string, string>);
    } catch {
      // storage blocked: no saved names
    }
  }, []);
  const nameKey = useCallback((key: string) => `${data.handle}:${key}`, [data.handle]);
  const rename = useCallback((key: string, name: string | null) => {
    setNames((m) => {
      const next = { ...m };
      const t = (name ?? '').trim().replace(/\s+/g, ' ').slice(0, 80);
      if (t) next[nameKey(key)] = t;
      else delete next[nameKey(key)];
      try {
        window.localStorage.setItem(NAMES, JSON.stringify(next));
      } catch {
        // storage blocked: this visit only
      }
      return next;
    });
  }, [nameKey]);
  const fileCard = useCallback((f: DropFile, at: FileAt): CardData => ({
    kind: 'file',
    key: `file:${f.id}`,
    title: names[nameKey(`file:${f.id}`)] ?? f.name ?? at.title,
    // the finding, under the name (a file with no name of its own is titled by its finding, and shows this drop's line)
    finding: f.name || names[nameKey(`file:${f.id}`)] ? at.title : null,
    state: at.state,
    from: at.flipped_from,
    fading: at.fading,
    line: at.line,
    // the big figure: landed of posts (posts over his usual on comments, for a file judged on comments)
    fig: `${f.metric === 'comments' ? at.count.comments_over_usual ?? 0 : at.count.landed}/${at.count.posts}`,
    hot: at.state === 'hot',
    search: f.origin === 'user' ? f.typed : null,
    fresh: at.new.length,
  }), [names, nameKey]);
  const everyCard = (key: string, group: string, name: string, c: { count: { posts: number; landed: number } } | undefined): CardData => ({ kind: 'every', key, group, name, fig: `${c?.count.landed ?? 0}/${c?.count.posts ?? 0}`, hot: Boolean(c && c.count.posts && c.count.landed * 2 > c.count.posts) });
  const shelves = useMemo<ShelfData[]>(() => {
    const live = data.files.map((f) => ({ f, at: fileAt(f) })).filter((x): x is { f: DropFile; at: FileAt } => Boolean(x.at));
    const moved = (a: { at: FileAt }, b: { at: FileAt }) => (b.at.new.length ? 1 : 0) - (a.at.new.length ? 1 : 0) || dayOf(b.at.last_post) - dayOf(a.at.last_post);
    const following: CardData[] = live.filter(({ f }) => isFollowed(`file:${f.id}`)).sort(moved).slice(0, 10).map(({ f, at }) => fileCard(f, at));
    mine.filter((k) => k.startsWith('every:')).forEach((k) => {
      const e = everyOf(k);
      const c = e?.row.by_drop[String(n)];
      if (e && following.length < 10) following.push(everyCard(k, e.group, e.row.name, c));
    });
    const every: CardData[] = GROUPS.flatMap(([g, group]) =>
      data.everything[g].map((row, i) => {
        return everyCard(`every:${g}:${i}`, group, row.name, row.by_drop[String(n)]);
      }),
    ).filter((c) => !mine.includes(c.key));
    return [
      { key: 'following', name: 'Following', cards: following },
      { key: 'everything', name: 'Everything', cards: every },
    ];
  }, [data, n, fileAt, isFollowed, fileCard, dayOf, mine, everyOf]);

  /* ── what a key holds: its posts on the Feed (posted order) and its words ── */
  const searchFile = useCallback((i: number) => {
    const s = data.searches[i];
    return s?.file ? data.files.find((f) => f.id === s.file) ?? null : null;
  }, [data]);
  const scanIds = useCallback((i: number): { ids: string[]; at: FileAt | null } => {
    const f = searchFile(i);
    if (!f) return { ids: [], at: null };
    const at = fileAt(f);
    if (at) return { ids: posted(at.posts), at };
    // the file did not exist yet at this drop: its latest membership, held to the posts in memory now
    const latest = f.by_drop[String(Math.max(...Object.keys(f.by_drop).map(Number)))];
    const inWin = new Set(drop.window);
    return { ids: posted(latest.posts.filter((id) => inWin.has(id))), at: null };
  }, [searchFile, fileAt, posted, drop]);
  const idsOf = useCallback((m: Mode): string[] => {
    if (m.stage === 'scan' || m.stage === 'rest' || !m.key) return windowIds;
    const [kind, a, b] = m.key.split(':');
    if (kind === 'file') {
      const f = data.files.find((x) => x.id === a);
      const at = f ? fileAt(f) : null;
      return at ? posted(at.posts) : windowIds;
    }
    if (kind === 'every') return posted(everyOf(m.key)?.row.by_drop[String(n)]?.posts ?? []);
    if (kind === 'scan') return scanIds(Number(a)).ids;
    void b;
    return windowIds;
  }, [windowIds, data, fileAt, posted, everyOf, n, scanIds]);

  const storyOf = useCallback((f: DropFile) => {
    const out = [];
    for (let d = n; d >= 1; d -= 1) {
      const at = f.by_drop[String(d)];
      // (a quiet week writes no story entry)
      if (!at || (at.story_state !== undefined && !at.line)) continue;
      out.push({ label: data.drops[d - 1].label, line: at.line, rewritten: at.reading_is_new && !at.first_reading, first: at.first_reading, now: d === n, state: at.story_state ?? null });
    }
    return out;
  }, [data, n]);
  const newIn = useCallback((ids: string[]) => ids.filter((id) => fresh.has(id)), [fresh]);
  const openedOf = useCallback((m: Mode): OpenedData | null => {
    if (!m.key || (m.stage !== 'open' && m.stage !== 'answer')) return null;
    const [kind, a] = m.key.split(':');
    if (kind === 'file') {
      const f = data.files.find((x) => x.id === a);
      const at = f ? fileAt(f) : null;
      if (!f || !at) return null;
      const shift = shiftOf(at.posts, { metric: f.metric, fresh: at.new, last: at.last_post, overComments: at.count.comments_over_usual, feed: { landed: drop.landed, posts: drop.window.length }, window: drop.window, plays: drop.x_plays_per_like, standing: drop.standing, pool: drop.posts_in_90_days }, byId);
      return { kind: 'file', key: m.key, title: names[nameKey(m.key)] ?? f.name ?? at.title, readerTitle: names[nameKey(m.key)] ? (f.name ?? at.title) : null, finding: f.name || names[nameKey(m.key)] ? at.title : null, state: at.state, from: at.flipped_from, fading: at.fading, question: f.question, shift, summary: at.summary ?? at.reading, story: storyOf(f), note: at.note, followed: isFollowed(m.key), search: f.origin === 'user' ? f.typed : null, evidence: at.evidence?.map((e) => ({ ...e, covers: e.post_ids.flatMap((id) => { const q = byId.get(id); return q ? [{ id, cover: q.cover, date: q.date }] : []; }) })) ?? null, feedBack: at.feed_back ?? null, nextTest: at.next_test ?? null, cantTell: at.cant_tell ?? null };
    }
    if (kind === 'every') {
      const e = everyOf(m.key);
      const c = e?.row.by_drop[String(n)];
      if (!e) return null;
      const ids = posted(c?.posts ?? []);
      return { kind: 'scan', key: m.key, k: e.group, title: e.row.name, question: null, shift: shiftOf(ids, { fresh: newIn(ids), last: c?.last_post ?? null, feed: { landed: drop.landed, posts: drop.window.length }, window: drop.window, plays: drop.x_plays_per_like, standing: drop.standing, pool: drop.posts_in_90_days }, byId), followed: isFollowed(m.key), file: null, summary: null };
    }
    if (kind === 'scan') {
      const i = Number(a);
      const s = data.searches[i];
      const f = searchFile(i);
      const { ids, at } = scanIds(i);
      const fk = f ? `file:${f.id}` : m.key;
      let summary: { title: string; text: string; from: string | null } | null = null;
      if (f) {
        if (at) summary = { title: at.title, text: at.summary ?? at.reading, from: null };
        else {
          const first = f.by_drop[String(f.opened_drop)];
          summary = { title: first.title, text: first.summary ?? first.reading, from: `This file opens at the ${data.drops[f.opened_drop - 1].label} drop. Its first summary, from then.` };
        }
      }
      return { kind: 'scan', key: fk, k: 'Scan', title: s.looking_for.replace(/^Looking for:\s*/, ''), question: s.question, shift: shiftOf(ids, { metric: f?.metric, fresh: newIn(ids), feed: { landed: drop.landed, posts: drop.window.length }, window: drop.window, plays: drop.x_plays_per_like, standing: drop.standing, pool: drop.posts_in_90_days }, byId), followed: isFollowed(fk), file: f?.id ?? null, summary };
    }
    return null;
  }, [data, drop, fileAt, storyOf, isFollowed, everyOf, n, searchFile, scanIds, byId, posted, newIn, names, nameKey]);

  /* ── the page's state ── */
  const [mode, setMode] = useState<Mode>(REST);
  const modeRef = useRef(mode);
  modeRef.current = mode;
  const [recent, setRecent] = useState<string[]>([]);
  useEffect(() => setRecent(loadList()), []);
  const [focusKey, setFocusKey] = useState(0);
  const rootRef = useRef<HTMLDivElement | null>(null);
  // (the wall takes its new height just after the page takes its new state: go() measures again then)
  const pinRef = useRef<(() => void) | null>(null);
  /* a laptop, with something open: whichever of the reading and the wall is shorter stays on screen while the other
     scrolls, so posts never run out beside text still being read. Each is pinned under the docked row of cards; one
     taller than the room under that row is pinned by its foot instead, so all of it can still be reached */
  useEffect(() => {
    const root = rootRef.current;
    if (!root || phone) return undefined;
    const sel = '.rn-stage > .rn-pane, .rn-stage > .rn-feed';
    const put = () => {
      const rail = root.querySelector<HTMLElement>('.rn-rail');
      const docked = rail && !root.classList.contains('is-off') && getComputedStyle(rail).position === 'sticky' ? (parseFloat(getComputedStyle(rail).top) || 0) + rail.offsetHeight : 0;
      root.querySelectorAll<HTMLElement>(sel).forEach((el) => {
        el.style.setProperty('--rn-pin', `${Math.round(Math.min(docked + DESK_PIN, window.innerHeight - el.offsetHeight - 24))}px`);
      });
    };
    put();
    pinRef.current = put;
    const ro = new ResizeObserver(put);
    root.querySelectorAll(`${sel}, .rn-rail`).forEach((el) => ro.observe(el));
    window.addEventListener('resize', put);
    return () => {
      pinRef.current = null;
      ro.disconnect();
      window.removeEventListener('resize', put);
    };
  }, [phone, mode.stage, mode.key]);
  const feedApi = useRef<FeedApi | null>(null);
  const [view, setView] = useState<WallView>(() => (params.get('view') === 'ranks' ? 'ranks' : 'recent'));
  const viewRef = useRef(view);

  /* the tile (one size on the whole page) and the laptop's open table, from the stage's width */
  const [stageW, setStageW] = useState(0);
  useLayoutEffect(() => {
    const el = rootRef.current?.querySelector<HTMLElement>('.rn-stage');
    if (!el) return undefined;
    const put = () => setStageW(Math.floor(el.clientWidth));
    put();
    const ro = new ResizeObserver(put);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  // (the screen's height: an open set is fitted to it, so a change in height lays the wall out again too)
  const [screenH, setScreenH] = useState(0);
  useEffect(() => {
    const put = () => setScreenH(window.innerHeight);
    put();
    window.addEventListener('resize', put);
    return () => window.removeEventListener('resize', put);
  }, []);
  const tile = stageW ? tileOf(stageW, phone) : 0;
  const gap = phone ? GAP_PHONE : GAP_DESK;
  const tableW = useMemo(() => {
    if (!stageW || phone) return stageW;
    const cols = Math.max(3, Math.round(((stageW - 56) * 0.56 + gap) / (tile + gap)));
    return cols * tile + (cols - 1) * gap;
  }, [stageW, phone, tile, gap]);

  /* the Feed at its first width: laid out at once, no motion */
  const laid = useRef(false);
  useLayoutEffect(() => {
    const api = feedApi.current;
    if (!api || !tile) return;
    const s = api.snap();
    if (!s) return;
    const W = Math.floor(rootRef.current?.querySelector<HTMLElement>('.rn-feed__in')?.clientWidth ?? stageW);
    const groups = groupsFor(modeRef.current, undefined, viewRef.current);
    api.reform({ ...s, drawn: laid.current ? s.drawn : new Map() }, groups, { W, t: tile, ...fitOf(modeRef.current, groups, W), dur: 0, fresh });
    pinRef.current?.();
    laid.current = true;
    // (a resize lays the Feed out again, still)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tile, stageW, phone, screenH]);

  /* ── Back: one history entry at most, owned by whatever is open (the typing list, a scan, an open file) ── */
  const hist = useRef({ pushed: false, ignore: 0 });
  const holdEntry = useCallback(() => {
    try {
      if (hist.current.pushed) window.history.replaceState({ ...(window.history.state ?? {}), rn: 1 }, '');
      else {
        window.history.pushState({ ...(window.history.state ?? {}), rn: 1 }, '');
        hist.current.pushed = true;
      }
    } catch {
      // no history: Esc and the close controls still work
    }
  }, []);
  const dropEntry = useCallback(() => {
    if (!hist.current.pushed) return;
    hist.current.pushed = false;
    hist.current.ignore += 1;
    window.history.back();
  }, []);

  /* ── the one mechanism: the page takes its new state, the Feed re-forms, the page is carried ── */
  const go = useCallback(
    (next: Mode, o: { nextN?: number; nextView?: WallView; scroll?: 'keep' | 'dock' | 'top' } = {}) => {
      const root = rootRef.current;
      const api = feedApi.current;
      const was = modeRef.current;
      if (!root || !api || !tile) {
        flushSync(() => {
          setMode(next);
          if (o.nextN) setN(o.nextN);
          if (o.nextView) setView(o.nextView);
        });
        return;
      }
      // what is drawn now (a carry still running is finished first: its end is where the page stands)
      root.getAnimations().forEach((a) => a.finish());
      // (a laptop's columns scroll on their own: each new thing starts at the top of both)
      root.querySelectorAll<HTMLElement>('.rn-stage > .rn-pane, .rn-stage > .rn-feed').forEach((el) => { el.scrollTop = 0; });
      const s = api.snap();
      // (a search's answer comes out of the search box: each caught post sets off from its thumbnail there)
      const fromTray = Boolean(s) && was.stage === 'scan' && next.stage === 'answer' && !reduce.current;
      if (s && fromTray) {
        document.querySelectorAll<HTMLElement>('[data-caught][data-in]').forEach((slot) => {
          const r = slot.getBoundingClientRect();
          if (slot.dataset.caught && r.width) s.drawn.set(slot.dataset.caught, { x: r.left, y: r.top, w: r.width, h: r.height });
        });
      }
      const y0 = window.scrollY;
      // the rail steps aside where it stands while a scan holds the page
      const shelvesEl = root.querySelector<HTMLElement>('.rn-rail');
      const offNow = next.stage === 'scan' || next.stage === 'answer';
      const offWas = was.stage === 'scan' || was.stage === 'answer';
      if (offNow && !offWas && shelvesEl) {
        root.style.setProperty('--rn-sh-top', `${shelvesEl.offsetTop}px`);
        root.style.setProperty('--rn-sh-left', `${shelvesEl.offsetLeft}px`);
        root.style.setProperty('--rn-sh-w', `${shelvesEl.offsetWidth}px`);
      }
      if (was.stage === 'scan' && next.stage !== 'scan') {
        if (next.stage === 'rest') api.stopScan();
        // (to the answer the window lets go after the re-form, on the glide's own clock)
        if (next.stage !== 'answer') api.window(null);
      }
      const v = o.nextView ?? viewRef.current;
      viewRef.current = v;
      // (a phone: where what sits under the rail stands now, so a fold of the rail's cards does not jump it)
      const stageEl = phone ? root.querySelector<HTMLElement>('.rn-stage') : null;
      const railEl = phone ? root.querySelector<HTMLElement>('.rn-rail') : null;
      const stage0 = stageEl?.getBoundingClientRect().top ?? 0;
      const rail0 = railEl?.getBoundingClientRect().height ?? 0;
      flushSync(() => {
        setMode(next);
        if (o.nextN) setN(o.nextN);
        if (o.nextView) setView(o.nextView);
      });
      if (!s) {
        if (was.stage === 'scan' && next.stage === 'answer') api.window(null);
        return;
      }
      // where the page comes to stand
      const H = window.innerHeight;
      let target = y0;
      if (o.scroll === 'top') target = 0;
      else if (o.scroll === 'dock') {
        // the page travels at most the hero's height, once: the rail docks under the top of the screen
        const mark = root.querySelector<HTMLElement>('.rn-dockmark');
        const rail = root.querySelector<HTMLElement>('.rn-rail');
        // (only when the start of the new set would be off the top of the screen: then the page comes back to it. With
        // the files or the start of the wall on screen, a tap on a file moves nothing)
        const d = mark && rail ? mark.getBoundingClientRect().top - (parseFloat(getComputedStyle(rail).top) || 0) : 0;
        const wallTop = root.querySelector<HTMLElement>('.rn-feed')?.getBoundingClientRect().top ?? 0;
        if (!phone) {
          // (a laptop: the set is fitted to the screen the wall is pinned in beside the reading, so the page glides to
          // there: the wall and the reading at the top of the screen, the row of files just above)
          const feedTop = root.querySelector<HTMLElement>('.rn-feed')?.getBoundingClientRect().top ?? DESK_PIN;
          target = Math.max(0, y0 + feedTop - DESK_PIN);
        } else if (d < 0 && wallTop < 0) target = Math.max(0, y0 + d);
        // (a phone under its hero: the set is fitted to the screen under the docked rail, so the page docks the rail and
        // the posts come up under it)
        else if (d > 0) target = y0 + d;
      }
      // (a file opening, switching or closing is the wall's one clock; a scan keeps its own beat)
      const scanBeat = next.stage === 'scan' || (was.stage === 'scan' && next.stage === 'answer');
      const dur = reduce.current ? 0 : ms(scanBeat ? 560 : WALL_MS);
      holdHeight(root, Math.max(target, y0) + H);
      if (Math.abs(target - y0) >= 1) window.scrollTo(0, target);
      const dy = window.scrollY - y0;
      /* a phone: the rail's cards fold (or open) as the file opens (or shuts), and the layout takes the new height at
         once. What sits under the rail is carried from where it stood on the wall's clock, and the rail's own floor
         glides with the cards' faces, so the row, the wall's head and the reading move as one instead of jumping */
      const shift = stageEl && dur && !offNow && !offWas ? Math.round(stage0 - stageEl.getBoundingClientRect().top - dy) : 0;
      // the new set, in a box as wide as the Feed now stands
      const inner = root.querySelector<HTMLElement>('.rn-feed__in');
      const W = Math.floor(inner?.clientWidth ?? stageW);
      const groups = groupsFor(next, o.nextN, v);
      api.reform(s, groups, { W, t: tile, ...fitOf(next, groups, W), dur, dy: dy + shift, fresh: freshFor(o.nextN, next), fromTray });
      if (was.stage === 'scan' && next.stage === 'answer') api.release(dur);
      pinRef.current?.();
      carryPage(root, dy, dur);
      if (shift && Math.abs(shift) <= 240 && stageEl) {
        stageEl.animate([{ transform: `translate3d(0, ${shift}px, 0)` }, { transform: 'translate3d(0, 0, 0)' }], { duration: dur, easing: TRAVEL_EASE, fill: 'backwards' });
        const fold = railEl ? Math.round(rail0 - railEl.getBoundingClientRect().height) : 0;
        if (railEl && fold > 0) {
          const cs = getComputedStyle(railEl);
          const hair = cs.boxShadow.match(/rgba?\([^)]*\)/)?.[0] ?? 'transparent';
          const floor = (d: number) => `0 ${d}px 0 0 ${cs.backgroundColor}, 0 ${d + 1}px 0 0 ${hair}`;
          railEl.animate([{ boxShadow: floor(fold) }, { boxShadow: floor(0) }], { duration: dur, easing: TRAVEL_EASE });
        }
      }
      if (offWas && !offNow && shelvesEl && dur) shelvesEl.animate([{ opacity: 0, transform: 'translate3d(0, 14px, 0)' }, { opacity: 1, transform: 'translate3d(0, 0, 0)' }], { duration: ms(420), delay: ms(120), easing: 'cubic-bezier(0.16, 1, 0.3, 1)', fill: 'backwards' });
      // the page gives back the height it no longer needs once it is still
      window.setTimeout(() => holdHeight(root, 0), dur + 200);
    },
    // (the helpers below are stable per drop; see windowIdsFor / idsOfFor)
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [tile, phone, stageW],
  );
  /* (a drop switch lays out the next drop's set: these read the drop asked for, not React's current one) */
  const idsRef = useRef({ idsOf, windowIds, fresh, data, n });
  idsRef.current = { idsOf, windowIds, fresh, data, n };
  const windowIdsFor = (nn?: number) => (nn ? [...idsRef.current.data.drops[nn - 1].window].reverse().filter((id) => byId.has(id)) : idsRef.current.windowIds);
  const idsOfFor = (m: Mode, nn?: number) => (nn && m.stage === 'rest' ? windowIdsFor(nn) : idsRef.current.idsOf(m));
  // what the Feed holds for a state: a scan reads the whole hundred as they stand; otherwise the set, in the view asked
  /* a set open on its own fills ONE SCREEN, edge to edge. The screen is the one the wall stands in while it is read: a
     phone, from where the wall starts under the docked row of files down to above its bottom bar; a laptop, the column
     pinned beside the reading, from under its head line to the foot of the screen. For each count across, the tiles
     take the wall's full width and the screen's full height, as tall as a reel (9:16) and never squatter than a little
     over square; the count whose pictures cover the most wins, a layout that leaves a strip at the sides only if none
     fills, and empty places on a last row count against it. Below a floor (64px, a phone 48px) the page scrolls. The
     hundred keep the wall's own size */
  function fitOf(m: Mode, groups: FeedGroup[], W?: number): { across: number; size?: number; tall?: number } {
    if (m.stage !== 'open' && m.stage !== 'answer') return { across: 0 };
    const sets = groups.filter((g) => g.ids.length);
    const count = sets.reduce((a, g) => a + g.ids.length, 0);
    const root = rootRef.current;
    if (!W || !count || !root) return { across: acrossFor(count) };
    const gap = phone ? GAP_PHONE : GAP_DESK;
    const floor = phone ? 48 : 64;
    const inner = root.querySelector<HTMLElement>('.rn-feed__in');
    const feed = root.querySelector<HTMLElement>('.rn-feed');
    let top: number;
    let foot: number;
    if (phone) {
      const mark = root.querySelector<HTMLElement>('.rn-dockmark');
      const rail = root.querySelector<HTMLElement>('.rn-rail');
      const docked = rail ? parseFloat(getComputedStyle(rail).top) || 0 : 0;
      top = mark && inner ? inner.getBoundingClientRect().top - mark.getBoundingClientRect().top + docked : 160;
      foot = (document.querySelector<HTMLElement>('.fm-dock-bar')?.getBoundingClientRect().top ?? window.innerHeight) - 12;
    } else {
      top = DESK_PIN + (inner && feed ? inner.getBoundingClientRect().top - feed.getBoundingClientRect().top : 56);
      foot = window.innerHeight - 24;
    }
    const room = foot - top;
    const labels = sets.filter((g) => g.label).length * 36 + (sets.length - 1) * gap * 2;
    const TALLEST = 16 / 9;
    const SQUAT = 1.1;
    type Fit = { across: number; size: number; tall: number; fills: boolean; score: number };
    let best: Fit | null = null;
    for (let c = 1; c <= Math.min(count, phone ? 8 : 12); c += 1) {
      const rows = sets.reduce((a, g) => a + Math.ceil(g.ids.length / c), 0);
      const empty = sets.reduce((a, g) => a + Math.ceil(g.ids.length / c) * c - g.ids.length, 0);
      const full = Math.floor((W - (c - 1) * gap) / c);
      const fitH = Math.floor((room - labels - (rows - sets.length) * gap) / rows);
      let w = full;
      let h = Math.min(fitH, Math.round(full * TALLEST));
      if (fitH < full * SQUAT) {
        w = Math.floor(fitH / SQUAT);
        h = fitH;
      }
      if (w < floor) continue;
      const fills = W - (c * w + (c - 1) * gap) <= Math.max(8, W * 0.04);
      const score = w * h * (count - 0.6 * empty);
      const f = { across: c, size: w, tall: h, fills, score };
      if (!best || (f.fills && !best.fills) || (f.fills === best.fills && f.score > best.score)) best = f;
    }
    if (!best) return { across: Math.max(2, Math.floor((W + gap) / (floor + gap))), size: floor };
    return { across: best.across, size: best.size, tall: best.tall };
  }
  function groupsFor(m: Mode, nn: number | undefined, v: WallView): FeedGroup[] {
    if (m.stage === 'scan') return [{ key: 'all', label: null, ids: windowIdsFor(nn) }];
    return groupsOf(idsOfFor(m, nn), v, idsRef.current.data.drops[(nn ?? idsRef.current.n) - 1].standing);
  }
  const freshFor = (nn: number | undefined, m: Mode) => {
    const set = nn ? new Set(idsRef.current.data.drops[nn - 1].new_posts) : idsRef.current.fresh;
    return m.stage === 'scan' ? new Set<string>() : set;
  };

  /* ── opening, closing, switching ── */
  const close = useCallback((viaBack = false) => {
    const was = modeRef.current;
    if (was.stage === 'rest' && !was.notice) return;
    if (!viaBack) dropEntry();
    if (was.stage === 'rest') {
      setMode(REST);
      return;
    }
    // (nothing scrolls: the wall re-forms where it stands, the rail stays docked)
    go(REST, { scroll: 'keep' });
  }, [go, dropEntry]);
  const openKey = useCallback((key: string) => {
    const was = modeRef.current;
    if (was.stage === 'open' && was.key === key) return close();
    holdEntry();
    go({ stage: 'open', key, question: '', notice: null }, { scroll: 'dock' });
  }, [go, close, holdEntry]);

  /* ── a search: a known sample scans; a turned-away one says why and spends nothing; anything else is not known here ── */
  const remember = useCallback((q: string) => {
    setRecent((list) => {
      const next = [q, ...list.filter((x) => x.toLowerCase() !== q.toLowerCase())].slice(0, 5);
      saveList(next);
      return next;
    });
  }, []);
  const onHop = useCallback((read: number, total: number, caught: number) => {
    const root = rootRef.current;
    const live = root?.querySelector<HTMLElement>('.rn-ask__live');
    if (live) live.textContent = `Reading ${read} of ${total} · ${caught} found`;
  }, []);
  // the posts a scan has caught, in order: the search box's tray holds a slot for each
  const [caught, setCaught] = useState<{ id: string; cover: string }[]>([]);
  const onCatch = useCallback((id: string) => {
    const cover = byId.get(id)?.cover;
    if (cover) setCaught((l) => (l.some((c) => c.id === id) ? l : [...l, { id, cover }]));
  }, [byId]);
  const ask = useCallback((text: string) => {
    const q = text.trim();
    if (!q) return;
    remember(q);
    const i = data.searches.findIndex((s) => s.typed.toLowerCase() === q.toLowerCase());
    const s = data.searches[i];
    const was = modeRef.current;
    if (!s || !s.ok) {
      // nothing scans: the page stays as it is, the box says why
      const notice = s ? { title: s.looking_for.replace(/^Looking for:\s*/, ''), text: s.reason ?? '' } : { title: q, text: 'This preview only knows the sample searches. Pick one from the box.' };
      holdEntry();
      if (was.stage !== 'rest') go({ ...REST, question: q, notice }, { scroll: 'top' });
      else setMode({ ...REST, question: q, notice });
      return;
    }
    holdEntry();
    setCaught([]);
    go({ stage: 'scan', key: `scan:${i}`, question: q, notice: null }, { scroll: 'top' });
    const api = feedApi.current;
    const root = rootRef.current;
    if (!api || !root) return;
    // the Feed is a window onto itself while it is read: to the screen's foot
    const box = root.querySelector<HTMLElement>('.rn-feed__box');
    let top = box?.getBoundingClientRect().top ?? 0;
    // (a phone: the box and the searches under it are still folding to their scan size; the window is sized for where
    // the wall will stand once they have, so it never grows in a jump)
    if (phone) {
      const wrap = root.querySelector<HTMLElement>('.rn-askwrap');
      const under = root.querySelector<HTMLElement>('.rn-recent, .rn-ready');
      top -= Math.max(0, (wrap?.getBoundingClientRect().height ?? 200) - 200) + (under?.getBoundingClientRect().height ?? 0);
    }
    // (a phone: the window ends above the bottom bar, so the last rows the lens reads are never under it)
    const foot = () => (phone ? (document.querySelector<HTMLElement>('.fm-dock-bar')?.getBoundingClientRect().top ?? window.innerHeight) - 12 : window.innerHeight - 24);
    api.window(Math.max(240, foot() - top));
    // (a phone: the recent searches tuck under the box for the search; the window takes the room they gave back)
    if (phone) {
      window.setTimeout(() => {
        if (modeRef.current.stage !== 'scan') return;
        const t2 = root.querySelector<HTMLElement>('.rn-feed__box')?.getBoundingClientRect().top ?? top;
        api.window(Math.max(240, foot() - t2));
      }, ms(520));
    }
    const hits = new Set(scanIds(i).ids);
    window.setTimeout(() => {
      if (modeRef.current.stage !== 'scan' || modeRef.current.key !== `scan:${i}`) return;
      api.scan({
        order: idsRef.current.windowIds,
        hits,
        onHop,
        onCatch,
        onDone: () => {
          if (modeRef.current.stage !== 'scan') return;
          go({ stage: 'answer', key: `scan:${i}`, question: q, notice: null }, { scroll: 'keep' });
        },
      });
    }, reduce.current ? 0 : ms(560));
  }, [data, remember, go, holdEntry, phone, scanIds, onHop, onCatch]);
  const stop = useCallback(() => {
    feedApi.current?.stopScan();
    feedApi.current?.window(null);
    setCaught([]);
    close();
  }, [close]);

  /* Back closes what is open */
  useEffect(() => {
    const onPop = () => {
      if (hist.current.ignore) {
        hist.current.ignore -= 1;
        return;
      }
      if (!hist.current.pushed) return;
      hist.current.pushed = false;
      window.dispatchEvent(new Event('rn:ask-close'));
      const m = modeRef.current;
      if (m.stage === 'scan') {
        feedApi.current?.stopScan();
        feedApi.current?.window(null);
      }
      close(true);
    };
    window.addEventListener('popstate', onPop);
    return () => window.removeEventListener('popstate', onPop);
  }, [close]);
  const onTyping = useCallback((on: boolean) => {
    if (on) holdEntry();
    else if (modeRef.current.stage === 'rest' && !modeRef.current.notice) dropEntry();
  }, [holdEntry, dropEntry]);

  // a dot in a file's Shift picks that post on the wall (the wall itself brings the picked post to the top of the
  // screen, so nothing here scrolls)
  const pickPost = useCallback((id: string) => {
    feedApi.current?.pick(id);
  }, []);
  const clearPost = useCallback(() => feedApi.current?.pick(null), []);
  const follow = useCallback((key: string) => {
    setMine((m) => (m.includes(key) ? m : [...m, key]));
  }, []);

  /* the drop switch (review): whatever is open closes at once, the Feed re-forms to that drop's hundred */
  const pickDrop = useCallback((k: number) => {
    if (k === n) return;
    dropEntry();
    go(REST, { nextN: k, scroll: 'keep' });
  }, [n, go, dropEntry]);

  /* keys: / types, Esc closes, F follows */
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const el = document.activeElement as HTMLElement | null;
      if (el && (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA')) return;
      if (e.metaKey || e.ctrlKey || e.altKey) return;
      if (e.key === '/') {
        e.preventDefault();
        setFocusKey((x) => x + 1);
      } else if (e.key === 'Escape') {
        const m = modeRef.current;
        if (m.stage === 'scan') stop();
        else close();
      } else if (e.key === 'f' || e.key === 'F') {
        const d = openedOf(modeRef.current);
        if (d && (d.kind === 'file' || d.kind === 'scan') && !d.followed) follow(d.key);
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [close, stop, follow, openedOf]);

  /* review links: ?open=file:F3, ?answer=<search>, ?ask=<search>, ?post=<id>, ?typing=1 */
  const once = useRef(false);
  useEffect(() => {
    if (once.current || !tile) return;
    once.current = true;
    const open = params.get('open');
    const answer = params.get('answer');
    const q = params.get('ask');
    const post = params.get('post');
    window.setTimeout(() => {
      if (open) openKey(open);
      if (answer) {
        const i = data.searches.findIndex((s) => s.typed === answer);
        if (i >= 0 && data.searches[i].ok) go({ stage: 'answer', key: `scan:${i}`, question: answer, notice: null }, { scroll: 'top' });
        else ask(answer);
      }
      if (q) ask(q);
      if (params.get('typing')) setFocusKey((x) => x + 1);
      if (post) window.setTimeout(() => feedApi.current?.pick(post), reduce.current ? 50 : ms(700));
    }, 120);
  }, [tile, params, openKey, ask, go, data]);

  /* ── React's copy of the page ── */
  const opened = useMemo(() => openedOf(mode), [openedOf, mode]);
  // (a phone's reading sits under the wall, below the screen as a file opens: it is drawn just after the wall sets off,
  // so the tap's own frame carries only the wall and the rail)
  const openedBody = useDeferredValue(opened);
  const askPhase: AskPhase = mode.stage === 'scan' ? 'scan' : mode.stage === 'answer' || mode.notice ? 'answer' : 'rest';
  const ids = mode.stage === 'open' || mode.stage === 'answer' ? idsOf(mode) : windowIds;
  const lead = mode.stage === 'scan' ? 'Scanning' : mode.stage === 'rest' ? drop.label : `${ids.length} ${ids.length === 1 ? 'post' : 'posts'}`;
  const line = mode.stage === 'scan' || mode.stage === 'rest' ? `${windowIds.length} posts in memory` : 'newest first';
  const onShowAll = useCallback(() => close(), [close]);
  // Recent or Ranks: the wall re-forms in place, nothing scrolls
  const onView = useCallback((v: WallView) => {
    if (v === viewRef.current || modeRef.current.stage === 'scan') return;
    go(modeRef.current, { nextView: v, scroll: 'keep' });
  }, [go]);
  const onSend = useCallback((t: string) => startTransition(() => ask(t)), [ask]);
  /* ── the search section's three places ── */
  const fileName = useCallback((f: DropFile, at: FileAt) => names[nameKey(`file:${f.id}`)] ?? f.name ?? at.title, [names, nameKey]);
  // 2. typing matches what the reader already knows: every file, and the rows of Everything with posts in memory
  const matches = useMemo<AskMatch[]>(() => {
    const coverOf = (ids: string[]) => {
      const id = posted(ids)[0];
      return id ? byId.get(id)!.cover : null;
    };
    // (not the files born of the reader's own questions: their answer costs the scan the box offers)
    const files: AskMatch[] = data.files.flatMap((f) => {
      const at = f.ask ? null : fileAt(f);
      return at ? [{ key: `file:${f.id}`, name: fileName(f, at), alt: f.typed, kind: 'File', cover: coverOf(at.posts), posts: at.count.posts, landed: at.count.landed }] : [];
    });
    const rows: AskMatch[] = GROUPS.flatMap(([g]) =>
      data.everything[g].flatMap((row, i) => {
        const c = row.by_drop[String(n)];
        return c && c.posts.length ? [{ key: `every:${g}:${i}`, name: row.name, alt: null, kind: KIND[g], cover: coverOf(c.posts), posts: c.count.posts, landed: c.count.landed }] : [];
      }),
    );
    return [...files, ...rows];
  }, [data, fileAt, fileName, n, posted, byId]);
  const searches = useMemo<AskSearch[]>(() => data.searches.map((s) => ({ text: s.typed, ok: s.ok, reason: s.reason })), [data]);
  // 3. the pills are the person's own: recent searches first, then the ready ones the reader can answer
  const ready = useMemo<Ready[]>(() => {
    const seen = new Set<string>();
    // (the reader's own questions are the box's, not pills, unless the person has run one: then it is theirs)
    const list = [...recent, ...data.searches.filter((s) => s.ok && !s.ask).map((s) => s.typed)].filter((t) => {
      const k = t.toLowerCase();
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });
    return list.slice(0, 6).map((t) => {
      const s = data.searches.find((x) => x.typed.toLowerCase() === t.toLowerCase());
      const f = s?.file ? data.files.find((x) => x.id === s.file) : null;
      const at = f ? fileAt(f) : null;
      // the posts it found: its file's; else (a search this preview never ran) the posts whose telling names its words
      let ids = at ? posted(at.posts) : [];
      if (!at) {
        const words = t.toLowerCase().split(/[^a-z0-9]+/).filter((w) => w.length >= 4 && !QUIET_WORDS.has(w));
        if (words.length) ids = posted(windowIds.filter((id) => { const p = byId.get(id); const hay = `${p?.told ?? ''} ${p?.caption ?? ''}`.toLowerCase(); return words.some((w) => hay.includes(w)); }));
      }
      const covers = ids.slice(0, 4).map((id) => byId.get(id)?.cover).filter((c): c is string => Boolean(c));
      return { text: t, cover: covers[0] ?? null, covers, posts: at ? at.count.posts : ids.length || null };
    });
  }, [recent, data, fileAt, posted, byId, windowIds]);
  // 1. the box rolls the questions the reader raises from this drop (they come with the drop; none, and the box shows
  // one still invitation). Never file names, tagged rows or ready searches
  const leads = useMemo<AskLead[]>(
    () =>
      (drop.leads ?? []).map((l, i) => {
        const search = l.to.startsWith('search:');
        return { key: `${n}:${i}:${l.to}`, text: l.line, sub: l.sub, to: search ? l.to.slice('search:'.length) : l.to, search, kind: l.kind };
      }),
    [drop, n],
  );
  // (a phone's hero box rolls only the questions; the drop's tags sit under it as short words to try)
  const askLeads = useMemo(() => (phone ? leads.filter((l) => l.kind !== 'tag') : leads), [phone, leads]);
  // a lead taken from the box: a search runs as if typed (bites), a row or a file opens (free)
  // a bulletin tag: a file the person follows opens; anything else goes to the box as a search
  const onTag = useCallback((t: string) => {
    const f = data.files.find((x) => [x.name, x.typed].some((s) => s && s.toLowerCase() === t.toLowerCase()));
    if (f && f.by_drop[String(n)]) return openKey(`file:${f.id}`);
    // (or a tagged row of Everything by its name: a person, product, place or piece)
    for (const [g, rows] of Object.entries(data.everything)) {
      const i = rows.findIndex((r) => r.name.toLowerCase() === t.toLowerCase());
      if (i >= 0 && rows[i].by_drop[String(n)]?.posts.length) return openKey(`every:${g}:${i}`);
    }
    onSend(t);
  }, [data, n, openKey, onSend]);
  // ?open=<tag or file name> (a tag tapped in the feeder's story on Feed) opens that file, or searches for it, once
  const openedFromStory = useRef(false);
  useEffect(() => {
    const want = params.get('open');
    if (openedFromStory.current || !want) return;
    // (after the wall has laid out: opening runs the page's carry, which measures it and flushes)
    const t = window.setTimeout(() => {
      openedFromStory.current = true;
      onTag(want);
    }, 700);
    return () => window.clearTimeout(t);
  }, [params, onTag]);
  const takeLead = useCallback((l: AskLead) => {
    if (l.search) onSend(l.to);
    else if (modeRef.current.stage === 'open' && modeRef.current.key === l.to) return;
    else openKey(l.to);
  }, [onSend, openKey]);
  // a phrase in the reader's sentence acts at once: a file or a row opens, a post is picked on the wall
  const onMark = useCallback((m: Mark) => {
    const [kind, id] = m.to.split(':');
    if (kind === 'post') {
      pickPost(id);
      return;
    }
    if (modeRef.current.stage === 'scan') stop();
    openKey(m.to);
  }, [pickPost, stop, openKey]);
  const marks = useMemo(
    () =>
      (drop.marks ?? []).filter((m) => {
        const [kind, a] = m.to.split(':');
        if (kind === 'post') return byId.has(a);
        if (kind === 'file') {
          const f = data.files.find((x) => x.id === a);
          return Boolean(f && fileAt(f));
        }
        if (kind === 'every') return Boolean(everyOf(m.to)?.row.by_drop[String(n)]?.posts.length);
        return false;
      }),
    [drop, byId, data, fileAt, everyOf, n],
  );
  // the box is forward (its list open): the rest of the page recedes behind one veil
  const [lifted, setLifted] = useState(false);
  const dropLabels = useMemo(() => data.drops.map((d) => d.label), [data]);
  const answerNote = mode.notice ? 'Nothing spent' : `Scanned ${windowIds.length} posts · ${data.bites.scan} bites`;

  return (
    <div
      ref={rootRef}
      className={cn('rn', phone && 'is-phone', `is-${mode.stage}`, (mode.stage === 'scan' || mode.stage === 'answer') && 'is-off')}
      style={{ ...(phone ? { minHeight: appShellStyle.minHeight } : null), ['--rn-tile' as string]: `${tile}px`, ['--rn-tile-h' as string]: `${Math.round(tile * 1.25)}px`, ['--rn-tablew' as string]: `${tableW}px` }}
      data-theme-surface=""
    >
      <Header eyebrow={`Drop · ${drop.label}`} feeds={feeds} seededFeeds={seededFeeds} />
      <header className="rn-top">
        <Ask leads={askLeads} matches={matches} searches={searches} phase={askPhase} question={mode.question} answerNote={answerNote} phone={phone} handle={data.handle} cost={data.bites.scan} scope={windowIds.length} caught={caught} focusKey={focusKey} onTake={takeLead} onOpen={openKey} onSend={onSend} onClear={close} onStop={stop} onTyping={onTyping} onLift={setLifted} />
        {mode.notice ? (
          <div className="rn-notice" key={mode.notice.title}>
            <p className="rn-notice__k">Turned away</p>
            <p className="rn-notice__t">{mode.notice.title}</p>
            <p className="rn-notice__x">{mode.notice.text}</p>
          </div>
        ) : null}
        <ReadyChips list={ready} busy={mode.stage === 'scan'} onSend={onSend} />
        {/* (the drop's note and bulletin now live in the feeder's story on Feed: components/feed/stream/DropStory) */}
      </header>
      {/* the veil the page recedes behind while the box is forward (a press on it closes the search: Ask listens) */}
      <div className={cn('rn-hush', lifted && 'is-on')} aria-hidden="true" />
      <span className="rn-dockmark" aria-hidden="true" />
      <Rail shelves={shelves} sel={mode.stage === 'open' ? mode.key : null} onOpen={openKey} phone={phone} open={mode.stage === 'open'} />
      <div className="rn-stage">
        {/* a laptop: the reading beside the Feed. A phone: the posts first (the wall re-forming is what a tap shows), the
            whole reading under them; only a turned-away search, which has no posts, speaks above */}
        {opened && (!phone || opened.kind === 'refused' || opened.kind === 'unknown') ? (
          <div className={cn('rn-pane', phone && 'is-head')}>
            <Opened d={opened} follow={data.bites.follow} onFollow={follow} onPost={pickPost} onClear={clearPost} onRename={rename} part="all" />
          </div>
        ) : null}
        <Feed posts={posts} phone={phone} apiRef={feedApi} lead={lead} line={line} onShowAll={onShowAll} showAll={mode.stage === 'open' || mode.stage === 'answer'} view={view} onView={onView} orderLabel={mode.stage === 'rest' ? 'Recent' : 'Posted'} scanning={mode.stage === 'scan'} />
        {opened && openedBody && phone && (openedBody.kind === 'file' || openedBody.kind === 'scan') ? (
          <div className="rn-pane is-body">
            <Opened d={openedBody} follow={data.bites.follow} onFollow={follow} onPost={pickPost} onClear={clearPost} onRename={rename} part="all" />
          </div>
        ) : null}
      </div>
      <footer className="rn-foot">
        <DropSwitch labels={dropLabels} n={n} onPick={pickDrop} />
      </footer>
      <PostTray byId={byId} plays={drop.x_plays_per_like} phone={phone} onClear={clearPost} />
    </div>
  );
}
