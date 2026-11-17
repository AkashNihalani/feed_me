'use client';

/* ─────────────────────────────────────────────────────────────
   THE WORDS — everything on the page that is not a picture.

   - Mark: a file's state as one disc (hot, cold, flipped, mixed,
     too few). Fading is not drawn: a fading file's card goes quiet.
   - Card: a file on a shelf (its mark and state, its title, this
     drop's line; the count small), or an Everything row (its kind,
     its name, its count).
   - Opened: what opens beside (laptop) or above (phone) the Feed:
     a file (its question, its reading, a dated line per drop, the
     reader's note, Follow), or a scan's answer (its question, the
     count line, Follow), or a search turned away (the reason).
   ───────────────────────────────────────────────────────────── */

import { memo, useEffect, useId, useLayoutEffect, useMemo, useRef, useState, useSyncExternalStore, type CSSProperties, type ReactNode } from 'react';
import { cn } from '@/lib/utils';
import { ms } from '@/lib/tempo';
import './readLit.css';
import { STATE_WORD, compact, multiple, pickedPost, thousands, type Evidence, type FeedBack, type Figure, type MetricKey, type Shift, type State } from './model';

export type Turn = 'up' | 'down';
const ACC = 'var(--fm-accent)';
const INK = 'var(--fm-fg)';
const PAGE = 'var(--fm-page)';
const ARROWS = {
  hot: 'M15 33L33 15M19 15h14v14',
  cold: 'M15 15l18 18M33 19v14H19',
  flat: 'M11 24h26M27 14l10 10-10 10',
  down: 'M10 15h12a8 8 0 0 1 8 8v12M21 26.5l9 9 9-9',
  up: 'M10 33h12a8 8 0 0 0 8-8V13M21 21.5l9-9 9 9',
} as const;

/* the disc. A flipped file is two halves in time order across a diagonal (upper left what it was, lower right what it
   is now) under a corner arrow; mixed is a ring with a level arrow; too few to call is a ring with one dot */
export const Mark = memo(function Mark({ state, from, size = 'm' }: { state: State; from?: State | null; size?: 's' | 'm' | 'l' }) {
  const id = useId().replace(/:/g, '');
  let body: ReactNode;
  if (state === 'flipped') {
    // turned down: it was landing and now misses
    const turn: Turn = from === 'cold' ? 'up' : 'down';
    const was = turn === 'down' ? ACC : INK;
    const now = turn === 'down' ? INK : ACC;
    const on = (c: string) => (c === ACC ? '#fff' : PAGE);
    body = (
      <>
        <defs>
          <clipPath id={`a${id}`}>
            <polygon points="0,0 48,0 0,48" />
          </clipPath>
          <clipPath id={`b${id}`}>
            <polygon points="48,0 48,48 0,48" />
          </clipPath>
          <clipPath id={`c${id}`}>
            <circle cx="24" cy="24" r="24" />
          </clipPath>
        </defs>
        <g clipPath={`url(#c${id})`}>
          <rect width="48" height="48" fill={was} clipPath={`url(#a${id})`} />
          <rect width="48" height="48" fill={now} clipPath={`url(#b${id})`} />
          <line x1="48" y1="0" x2="0" y2="48" stroke={PAGE} strokeWidth="1.5" />
        </g>
        <g fill="none" strokeWidth="5" strokeLinecap="round" strokeLinejoin="round">
          <g clipPath={`url(#a${id})`}>
            <path d={ARROWS[turn]} stroke={on(was)} />
          </g>
          <g clipPath={`url(#b${id})`}>
            <path d={ARROWS[turn]} stroke={on(now)} />
          </g>
        </g>
      </>
    );
  } else if (state === 'mixed') {
    body = (
      <>
        <circle cx="24" cy="24" r="22.25" fill="none" stroke={INK} strokeWidth="3.5" />
        <path d={ARROWS.flat} fill="none" stroke={INK} strokeWidth="5" strokeLinecap="round" strokeLinejoin="round" />
      </>
    );
  } else if (state === 'too_few') {
    body = (
      <>
        <circle cx="24" cy="24" r="22.25" fill="none" stroke={INK} strokeOpacity="0.42" strokeWidth="3.5" />
        <circle cx="24" cy="24" r="5.5" fill={INK} fillOpacity="0.55" />
      </>
    );
  } else {
    body = (
      <>
        <circle cx="24" cy="24" r="24" fill={state === 'hot' ? ACC : INK} />
        <path d={ARROWS[state]} fill="none" stroke={state === 'hot' ? '#fff' : PAGE} strokeWidth="5.5" strokeLinecap="round" strokeLinejoin="round" />
      </>
    );
  }
  return (
    <span className={cn('rn-mark', `is-${state}`, `is-${size}`)} aria-hidden="true">
      <svg viewBox="0 0 48 48">{body}</svg>
    </span>
  );
});

/* ── a card on the rail: the big figure (landed of posts) beside the state arrow at the same weight, the title as the
   context under it, then this drop's line. New posts this drop: "+3 new" ── */
export type FileCard = { kind: 'file'; key: string; title: string; finding: string | null; state: State; from: State | null; fading: boolean; line: string; fig: string; hot: boolean; search: string | null; fresh: number };
export type EveryCard = { kind: 'every'; key: string; group: string; name: string; fig: string; hot: boolean };
export type CardData = FileCard | EveryCard;

export const Card = memo(function Card({ card, sel, onOpen }: { card: CardData; sel: boolean; onOpen: (key: string) => void }) {
  if (card.kind === 'every') {
    return (
      <button type="button" className={cn('rn-card is-every', sel && 'is-sel')} data-key={card.key} onClick={() => onOpen(card.key)} aria-pressed={sel}>
        <span className="rn-card__fig">
          <b className={cn(card.hot && 'is-hot')}>{card.fig}</b>
        </span>
        <span className="rn-card__k">{card.group}</span>
        <b className="rn-card__t">{card.name}</b>
      </button>
    );
  }
  return (
    <button type="button" className={cn('rn-card', `is-${card.state}`, card.fading && 'is-fading', sel && 'is-sel')} data-key={card.key} onClick={() => onOpen(card.key)} aria-pressed={sel} aria-label={`${card.title}. ${card.finding ? `${card.finding}. ` : ''}${STATE_WORD[card.state]}. ${card.fig} landed.`}>
      <span className="rn-card__fig">
        <Mark state={card.state} from={card.from} size="l" />
        <b className={cn(card.hot && 'is-hot')}>{card.fig}</b>
        {card.fresh ? <span className="rn-card__new">+{card.fresh} new</span> : null}
      </span>
      <b className="rn-card__t">{card.title}</b>
      <span className="rn-card__l">{card.finding ?? card.line}</span>
      {card.search || card.fading ? (
        <span className="rn-card__foot">
          {card.fading ? <span className="rn-card__fade">Fading</span> : null}
          {card.search ? (
            <span className="rn-card__q">
              <svg viewBox="0 0 24 24" aria-hidden="true">
                <circle cx="10.5" cy="10.5" r="6.5" />
                <path d="M15.5 15.5L20 20" />
              </svg>
              {card.search}
            </span>
          ) : null}
        </span>
      ) : null}
    </button>
  );
});

/* ── what opens: a file in three parts. SHIFT (its numbers, as figures), SUMMARY (the reader's explanation, as words),
   STORY (one dated note per drop, newest first). A scan or an Everything row has its SHIFT and Follow ── */
export type StoryNote = { label: string; line: string; rewritten: boolean; first: boolean; now: boolean; state?: string | null };
export type OpenedData =
  | { kind: 'file'; key: string; title: string; readerTitle: string | null; finding: string | null; state: State; from: State | null; fading: boolean; question: string; shift: Shift; summary: string; story: StoryNote[]; note: string | null; followed: boolean; search: string | null; evidence?: Evidence[] | null; feedBack?: FeedBack | null; nextTest?: string | null; cantTell?: string | null }
  | { kind: 'scan'; key: string; k: string; title: string; question: string | null; shift: Shift; followed: boolean; file: string | null; summary: { title: string; text: string; from: string | null } | null }
  | { kind: 'refused'; key: string; title: string; reason: string }
  | { kind: 'unknown'; key: string; title: string };

const ORD = (n: number) => `${n}${n % 100 >= 11 && n % 100 <= 13 ? 'th' : ['th', 'st', 'nd', 'rd'][n % 10] ?? 'th'}`;
// what a dot shows for a number: none, under, over, or a big one
const sigOf = (x: number | null) => (x == null ? 'n' : x >= 3 ? 'U' : x > 1 ? 'u' : 'd');
const NOUN: Record<MetricKey, string> = { likes: 'likes', comments: 'comments', plays: 'plays per like' };

/* a file's SHIFT. Three things, each one plain to read:
   - how many landed, large, with the state beside it;
   - the numbers we track, as big figures: what a typical post of the file does on each against his usual. None is
     chosen to begin with (the file overall). A tap chooses one, a tap on it again lets it go;
   - every post of the file as one dot, oldest first: crimson when it landed, or, with a number chosen, when it beat
     his usual on that number.
   It follows the wall both ways. A tap on a dot picks that post on the wall; a post picked on the wall lights its dot.
   Either way the big figures turn to that post's own numbers, until it is let go. */
function ShiftRow({ shift, state, from, fading, onPost, onClear }: { shift: Shift; state?: State; from?: State | null; fading?: boolean; onPost?: (id: string) => void; onClear?: () => void }) {
  // no number chosen: the file overall (the dots show what landed). A tap chooses one; a tap on it again lets it go
  const [chosen, setChosen] = useState<MetricKey | null>(null);
  // (a tap or a click chooses a number; nothing changes under a resting pointer)
  const choose = (k: MetricKey) => setChosen((c) => (c === k ? null : k));
  const pickedId = useSyncExternalStore(pickedPost.sub, pickedPost.get, () => null);
  const sel = pickedId ? (shift.posts.find((p) => p.id === pickedId) ?? null) : null;
  const pinned = chosen && shift.metrics.some((m) => m.key === chosen) ? chosen : null;
  // what the dots and the big figure show: the number chosen
  const one = pinned;
  // overall is read on the number the file is judged by
  const key = one ?? shift.lead;
  // the file's posts that have this number, best first
  const ranked = useMemo(() => shift.posts.filter((p) => p.x[key] != null).sort((a, b) => (b.x[key] as number) - (a.x[key] as number)), [shift.posts, key]);
  /* a change of number, played only on the dots it changes. A dot whose answer is the same stays still; one that
     turns (over to under, or into or out of a big one) rolls out of the top of its slot and its new self rolls in
     from below, one after another along the row: the motion the figures have, with no bounce. With the count beside
     the big figure rolling to its new number, what changed is the only thing that moves */
  const rowRef = useRef<HTMLDivElement | null>(null);
  const shown = useRef<{ key: MetricKey; sig: Map<string, string> } | null>(null);
  useLayoutEffect(() => {
    const sig = new Map(shift.posts.map((p) => [p.id, sigOf(p.x[key])]));
    const was = shown.current;
    shown.current = { key, sig };
    const row = rowRef.current;
    if (!row || !was || was.key === key || window.matchMedia('(prefers-reduced-motion: reduce)').matches) return undefined;
    const dur = ms(460);
    const turning = [...row.querySelectorAll<HTMLElement>('.rn-dot')].filter((el) => was.sig.get(el.dataset.id ?? '') !== sig.get(el.dataset.id ?? ''));
    if (!turning.length) return undefined;
    // (always the same move, so it never reads as two different things: every dot that turns rolls. A few go one
    // after another; many go as one quick sweep, the whole of it over in about a quarter of a second, never a
    // long cascade down the row)
    const gap = Math.min(45, 260 / turning.length);
    const turned: HTMLElement[] = [];
    turning.forEach((el, n) => {
      const delay = Math.round(n * gap);
      const dot = el.firstElementChild as HTMLElement | null;
      if (!dot) return;
      // (it turns colour and size while it is out of sight, between leaving and coming back)
      dot.style.transitionDuration = '0s';
      dot.style.transitionDelay = `${delay + Math.round(dur * 0.42)}ms`;
      turned.push(dot);
      dot.animate(
        [
          { translate: '0 0', easing: 'cubic-bezier(0.5, 0, 0.9, 0.4)' },
          { translate: '0 -34px', offset: 0.42, easing: 'steps(1, end)' },
          { translate: '0 34px', offset: 0.44, easing: 'cubic-bezier(0.16, 1, 0.3, 1)' },
          { translate: '0 0' },
        ],
        { duration: dur, delay },
      );
    });
    const t = window.setTimeout(() => turned.forEach((d) => {
      d.style.transitionDuration = '';
      d.style.transitionDelay = '';
    }), dur + 360);
    return () => {
      window.clearTimeout(t);
      turned.forEach((d) => {
        d.style.transitionDuration = '';
        d.style.transitionDelay = '';
      });
    };
  }, [key, shift.posts]);
  const top = ranked[0] ?? null;
  // the dots run as the wall does: the newest post first
  const run = useMemo(() => [...shift.posts].reverse(), [shift.posts]);
  const place = sel ? ranked.findIndex((p) => p.id === sel.id) + 1 : 0;
  // the figure: how many landed, or, with a number chosen, how many beat his usual on it
  const over = ranked.filter((p) => (p.x[key] as number) > 1).length;
  const [num, den] = one ? [String(over), String(ranked.length)] : shift.big.value.split('/');
  const figHot = one ? over * 2 >= ranked.length && ranked.length > 0 : shift.big.tone === 'hot';
  const figLabel = one ? `over his usual on ${NOUN[one]}` : shift.big.label;
  const facts: Figure[] = [...(top && ranked.length > 1 ? [{ label: 'Biggest', value: multiple(top.x[key]), sub: top.day, tone: (top.x[key] as number) > 1 ? ('hot' as const) : null, post: top.id }] : []), ...shift.rest];
  return (
    <section className={cn('rn-shift', sel && 'is-post')} aria-label="Shift">
      <p className="rn-sec__k">Shift</p>
      <div className="rn-shift__top">
        <p className="rn-shift__n" key={`${one ?? 'all'}:${num}`}>
          <b className={cn(figHot && 'is-hot')}>{num}</b>
          {den ? <span>/{den}</span> : null}
          <em>{figLabel}</em>
        </p>
        {state ? (
          <p className="rn-shift__state">
            <Mark state={state} from={from} size="s" />
            {STATE_WORD[state]}
            {fading ? ', fading' : ''}
          </p>
        ) : null}
      </div>
      {shift.metrics.length ? (
        <>
          <div className="rn-dots">
            <p className="rn-dots__k">
              Newest first. <i aria-hidden="true" /> {one || shift.lead === 'comments' ? 'over his usual' : 'landed'}
            </p>
            <div ref={rowRef} className="rn-dots__row" style={{ '--n': shift.posts.length } as CSSProperties}>
              {run.map((p, i) => {
                const x = p.x[key];
                return (
                  <button
                    key={p.id}
                    type="button"
                    data-id={p.id}
                    className={cn('rn-dot', x == null ? 'is-none' : x > 1 && 'is-up', x != null && x >= 3 && 'is-big', p.fresh && 'is-new', sel?.id === p.id && 'is-sel')}
                    style={{ animationDelay: `${Math.min(i * 14, 420)}ms`, '--i': Math.min(i, 40) } as CSSProperties}
                    aria-pressed={sel?.id === p.id}
                    aria-label={`${p.day}, ${x == null ? `no ${NOUN[key]}` : `${multiple(x)} on ${NOUN[key]}`}${p.fresh ? ', new this drop' : ''}`}
                    onClick={() => onPost?.(p.id)}
                  >
                    <i />
                  </button>
                );
              })}
            </div>
            {shift.posts.length > 1 ? (
              <p className="rn-dots__ax">
                <span>{run[0].day}</span>
                {shift.posts.some((p) => p.fresh) ? <span className="rn-dots__new">underlined: new this drop</span> : null}
                <span>{run[run.length - 1].day}</span>
              </p>
            ) : null}
          </div>
          <div className="rn-shift__who" aria-live="polite">
            {sel ? (
              <>
                <img className="rn-shift__thumb" src={sel.cover} alt="" />
                <p key={sel.id}>
                  <b>{sel.day}</b>
                  {place ? ` · ${ORD(place)} of ${ranked.length} on ${NOUN[key]}` : ''}
                  {sel.fresh ? ' · new' : ''}
                </p>
                <button type="button" onClick={() => onClear?.()}>
                  Back to the file
                </button>
              </>
            ) : (
              <p>A typical post here, against his usual</p>
            )}
          </div>
          <div className="rn-nums" role="group" aria-label="Numbers">
            {shift.metrics.map((m) => {
              const x = sel ? sel.x[m.key] : null;
              const n = sel ? sel.n[m.key] : null;
              const value = sel ? (x == null ? '·' : multiple(x)) : m.value;
              const hot = sel ? (x ?? 0) > 1 : m.over;
              const sub = sel ? (n == null ? (m.key === 'plays' ? 'not a reel' : '') : `${n >= 10000 ? compact(n) : thousands(n)} ${m.key === 'plays' ? 'plays' : m.key}`) : m.lately === 'up' ? 'rising lately' : m.lately === 'down' ? 'falling lately' : '';
              return (
                <button key={m.key} type="button" className="rn-num" aria-pressed={pinned === m.key} onClick={() => choose(m.key)}>
                  <span className="rn-num__k">{m.label}</span>
                  <b key={value} className={cn('rn-num__v', hot && 'is-hot')}>
                    {value}
                  </b>
                  <span key={sub} className="rn-num__s">
                    {sub || '\u00a0'}
                  </span>
                </button>
              );
            })}
          </div>
        </>
      ) : null}
      {facts.length ? (
        <div className="rn-shift__facts">
          {facts.map((f) => {
            const inner = (
              <>
                <span className="rn-fact__k">{f.label}</span>
                <b key={f.value} className={cn('rn-fact__v', f.tone === 'hot' && 'is-hot')}>
                  {f.value}
                </b>
                {f.sub ? <small>{f.sub}</small> : null}
              </>
            );
            return f.post ? (
              <button key={f.label} type="button" className={cn('rn-fact is-tap', sel?.id === f.post && 'is-sel')} onClick={() => onPost?.(f.post!)} aria-label={`${f.label}: ${f.value}. Show this post.`}>
                {inner}
              </button>
            ) : (
              <div key={f.label} className="rn-fact">
                {inner}
              </div>
            );
          })}
        </div>
      ) : null}
    </section>
  );
}

/* evidence lights its posts on the wall: a ring drawn in on each (readLit.css), nothing else dimmed. Only a click or a
   tap does it: it selects the card (its posts stay lit, a second tap clears, another card switches) and brings the posts
   into view. Nothing lights under a resting pointer */
let selKey: string | null = null;
const keyOf = (ids: string[]) => ids.join(',');
function paintLit() {
  const on = new Set(selKey ? selKey.split(',') : []);
  document.querySelectorAll<HTMLElement>('.rn-t').forEach((el) => el.classList.toggle('is-lit', on.has(el.dataset.id ?? '')));
  document.querySelectorAll<HTMLElement>('.rn-ev__b[data-k]').forEach((el) => {
    if (el.dataset.k && el.dataset.k === selKey) el.dataset.lit = 'sel';
    else delete el.dataset.lit;
  });
}
function selectPosts(ids: string[]) {
  const key = keyOf(ids);
  selKey = selKey === key ? null : key;
  paintLit();
  if (!selKey) return;
  const lit = Array.from(document.querySelectorAll<HTMLElement>('.rn-t.is-lit'));
  if (!lit.length) return;
  const reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const behavior: ScrollBehavior = reduce ? 'auto' : 'smooth';
  // the wall scrolls on its own (a laptop): bring the lit posts into it, the whole run if it fits, else from its first
  const box = lit[0].closest<HTMLElement>('.rn-feed');
  if (box && box.scrollHeight > box.clientHeight + 1) {
    const at = box.getBoundingClientRect().top - box.scrollTop;
    const tops = lit.map((el) => el.getBoundingClientRect().top - at);
    const bottoms = lit.map((el) => el.getBoundingClientRect().bottom - at);
    const first = Math.min(...tops);
    const span = Math.max(...bottoms) - first;
    const top = span <= box.clientHeight - 32 ? first - (box.clientHeight - span) / 2 : first - 16;
    box.scrollTo({ top: Math.max(0, top), behavior });
  }
  // the page (a phone: the wall sits above or below the reading): only when none of them is on screen
  const vh = window.innerHeight;
  const seen = lit.some((el) => { const r = el.getBoundingClientRect(); return r.bottom > 0 && r.top < vh; });
  if (!seen) lit[0].scrollIntoView({ block: 'center', behavior });
}

/* the reading's own text, with the posts it names as links: the writer marks them [[P092|the words]]. A link opens its
   post (a click or a tap; nothing happens under a resting pointer) */
const MARK = /\[\[(P\d{3})\|(.+?)\]\]/g;
export const plainOf = (text: string) => text.replace(MARK, '$2');
function Linked({ text, onPost }: { text: string; onPost?: (id: string) => void }) {
  const out: ReactNode[] = [];
  let from = 0;
  for (const m of text.matchAll(MARK)) {
    const at = m.index ?? 0;
    if (at > from) out.push(text.slice(from, at));
    const id = m[1];
    out.push(
      <span
        key={`${id}:${at}`}
        role="button"
        tabIndex={0}
        className="rn-ln"
        onClick={(ev) => { ev.stopPropagation(); onPost?.(id); }}
        onKeyDown={(ev) => { if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); ev.stopPropagation(); onPost?.(id); } }}
      >
        {m[2]}
      </span>,
    );
    from = at + m[0].length;
  }
  if (from < text.length) out.push(text.slice(from));
  return <>{out}</>;
}

/* what is open, whole (a laptop: beside the Feed), or in two parts round the Feed (a phone: its head and shift above
   the posts, its summary and story under them) */
export type Part = 'all' | 'head' | 'body';
export const Opened = memo(function Opened({ d, follow, onFollow, onPost, onClear, onRename, part = 'all' }: { d: OpenedData; follow: number; onFollow: (key: string) => void; onPost?: (id: string) => void; onClear?: () => void; onRename?: (key: string, name: string | null) => void; part?: Part }) {
  // (what this file lit on the wall goes when the file closes or another opens: a lit post must never wait on the wall
  // for the next search to find it)
  useEffect(() => () => {
    selKey = null;
    paintLit();
  }, [d.key]);
  const head = part !== 'body';
  const body = part !== 'head';
  if (d.kind === 'refused' || d.kind === 'unknown') {
    if (!head) return null;
    return (
      <div className="rn-open is-refused" key={d.key}>
        <p className="rn-open__k">{d.kind === 'refused' ? 'Turned away' : 'Not in this preview'}</p>
        <h2 className="rn-open__t">{d.title}</h2>
        <p className="rn-open__sum">{d.kind === 'refused' ? d.reason : 'This preview only knows the sample searches. Pick one from the box.'}</p>
        <p className="rn-open__from">Nothing spent.</p>
      </div>
    );
  }
  if (d.kind === 'scan') {
    return (
      <div className={cn('rn-open is-scan', `is-${part}`)} key={d.key}>
        {head ? (
          <>
            <p className="rn-open__k">{d.k}</p>
            <h2 className="rn-open__t">{d.title}</h2>
            {d.question ? <p className="rn-open__q">Looking for: {d.question}</p> : null}
            <ShiftRow shift={d.shift} onPost={onPost} onClear={onClear} />
          </>
        ) : null}
        {body && d.followed && d.summary ? (
          <section className="rn-sum" aria-label="Summary">
            <p className="rn-sec__k">Summary</p>
            <h3 className="rn-sum__t">{d.summary.title}</h3>
            <p className="rn-open__sum">{d.summary.text}</p>
            {d.summary.from ? <p className="rn-open__from">{d.summary.from}</p> : null}
          </section>
        ) : body && d.followed ? (
          <p className="rn-open__from">The reader writes its first summary at the next drop.</p>
        ) : null}
        {body ? <FollowButton followed={d.followed} cost={follow} onClick={() => onFollow(d.key)} /> : null}
      </div>
    );
  }
  return (
    <div className={cn('rn-open is-file', `is-${part}`, d.fading && 'is-fading')} key={d.key}>
      {head ? (
        <>
          <FileName title={d.title} readerTitle={d.readerTitle} onRename={onRename ? (name) => onRename(d.key, name) : undefined} />
          {d.finding ? <p className="rn-open__find">{d.finding}</p> : null}
          <p className="rn-open__q">Looking for: {d.question}</p>
          {d.search ? <p className="rn-open__from">From your search “{d.search}”</p> : null}
          <ShiftRow shift={d.shift} state={d.state} from={d.from} fading={d.fading} onPost={onPost} onClear={onClear} />
        </>
      ) : null}
      {body ? (
        <>
          <section className="rn-sum" aria-label="Summary">
            <p className="rn-sec__k">Summary</p>
            <p className="rn-open__sum"><Linked text={d.summary} onPost={onPost} /></p>
          </section>
          {d.evidence?.length ? (
            <section className="rn-ev" aria-label="Evidence">
              <p className="rn-sec__k">Evidence</p>
              {d.evidence.map((e) => (
                <div
                  key={e.label}
                  className="rn-ev__b"
                  data-k={keyOf(e.post_ids)}
                  onClick={(ev) => { if (!(ev.target as HTMLElement).closest('button')) selectPosts(e.post_ids); }}
                >
                  <p className="rn-ev__t">{e.label}</p>
                  <button type="button" className="rn-ev__chip" onClick={() => e.post_ids[0] && onPost?.(e.post_ids[0])}>
                    {e.chip}
                  </button>
                  <p className="rn-ev__x"><Linked text={e.text} onPost={onPost} /></p>
                </div>
              ))}
            </section>
          ) : null}
          <section className="rn-story" aria-label="Story">
            <p className="rn-sec__k">Story</p>
            <ol>
              {d.story.map((h) => (
                <li key={h.label} className={cn(h.now && 'is-now')}>
                  <i aria-hidden="true" />
                  <span className="rn-story__d">
                    {h.label}
                    {h.state ? <em>{h.state}</em> : h.first ? <em>Opened</em> : h.rewritten ? <em>Summary rewritten</em> : null}
                  </span>
                  <span className="rn-story__l"><Linked text={h.line} onPost={onPost} /></span>
                </li>
              ))}
            </ol>
          </section>
          {d.note ? (
            <div className="rn-note">
              <p className="rn-note__k">The reader&rsquo;s note</p>
              <p>{d.note}</p>
            </div>
          ) : null}
          {d.feedBack ? (
            <section className="rn-fb" aria-label="Feed back">
              <p className="rn-sec__k">Feed · back</p>
              <dl>
                <dt>Owner</dt>
                <dd><Linked text={d.feedBack.owner} onPost={onPost} /></dd>
                <dt>Competitor</dt>
                <dd><Linked text={d.feedBack.competitor} onPost={onPost} /></dd>
                <dt>Brand</dt>
                <dd><Linked text={d.feedBack.brand} onPost={onPost} /></dd>
              </dl>
            </section>
          ) : null}
          {d.nextTest ? (
            <div className="rn-note">
              <p className="rn-note__k">Next test</p>
              <p><Linked text={d.nextTest} onPost={onPost} /></p>
            </div>
          ) : null}
          {d.cantTell ? <p className="rn-open__from">Can&rsquo;t tell: <Linked text={d.cantTell} onPost={onPost} /></p> : null}
          <FollowButton followed={d.followed} cost={follow} onClick={() => onFollow(d.key)} />
        </>
      ) : null}
    </div>
  );
});

/* a file's name: what it collects, fixed when it opens. Rename turns it into a field (Enter keeps it, Esc leaves it as
   it was, empty gives its first name back). The finding, which may change each drop, sits under it */
function FileName({ title, readerTitle, onRename }: { title: string; readerTitle: string | null; onRename?: (name: string | null) => void }) {
  const [editing, setEditing] = useState(false);
  const ref = useRef<HTMLTextAreaElement | null>(null);
  useEffect(() => {
    const el = ref.current;
    if (!editing || !el) return;
    el.style.height = 'auto';
    el.style.height = `${el.scrollHeight}px`;
    el.focus({ preventScroll: true });
    el.select();
  }, [editing]);
  const done = (keep: boolean) => {
    const v = ref.current?.value ?? '';
    setEditing(false);
    if (!keep || !onRename) return;
    const t = v.trim();
    if (!t || t === readerTitle) onRename(null);
    else if (t !== title) onRename(t);
  };
  return (
    <div className="rn-name">
      {editing ? (
        <textarea
          ref={ref}
          className="rn-open__t rn-name__in"
          defaultValue={title}
          rows={1}
          maxLength={80}
          aria-label="File name"
          onInput={(e) => {
            const el = e.currentTarget;
            el.style.height = 'auto';
            el.style.height = `${el.scrollHeight}px`;
          }}
          onKeyDown={(e) => {
            e.stopPropagation();
            if (e.key === 'Enter') {
              e.preventDefault();
              done(true);
            } else if (e.key === 'Escape') {
              e.preventDefault();
              done(false);
            }
          }}
          onBlur={() => done(true)}
        />
      ) : (
        <h2 className="rn-open__t">{title}</h2>
      )}
      {onRename ? (
        <p className="rn-name__row">
          {editing ? (
            <span>Enter to keep · Esc to cancel</span>
          ) : (
            <>
              <button type="button" onClick={() => setEditing(true)}>
                Rename
              </button>
              {readerTitle ? (
                <>
                  <button type="button" onClick={() => onRename(null)}>
                    Use its first name
                  </button>
                  <span>It opened as: {readerTitle}</span>
                </>
              ) : null}
            </>
          )}
        </p>
      ) : null}
    </div>
  );
}

function FollowButton({ followed, cost, onClick }: { followed: boolean; cost: number; onClick: () => void }) {
  if (followed) return <p className="rn-follow is-on">Following</p>;
  return (
    <button type="button" className="rn-follow" onClick={onClick} aria-keyshortcuts="F">
      Follow
      <span>{cost} bites</span>
    </button>
  );
}
