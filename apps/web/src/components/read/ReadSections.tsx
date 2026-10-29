'use client';

/* ─────────────────────────────────────────────────────────────
   READ SECTIONS — a feeder's read in the agreed order (Oct 29):
   This week · Phases · Lanes · Drivers, then the wall and the
   Feeder File. Each answers one question and opens on one counted
   line in Lead's hero grammar: a figure, a line in ink, an aside in
   grey (data/readSections.json, built offline from the tracked
   posts; nothing here is written).

   One yardstick throughout, the app's own: Top % (a post's rank
   among the account's own posts on engagement; 1 = its top 1%).
   Drivers also read likes and comments on their own ranks, and
   followers week against week.

   - This week: the last full week's posts, against a usual week.
   - Phases: the typical rank week by week since April, cut into the
     stretches where what was posted and how it ranked held steady.
   - Lanes: reels, carousels, photos: the share then and now, what
     goes in each, where each typically ranks.
   - Drivers: the facts that move each number, each tested against
     600 shuffles; the rest named as no real effect.
   - The wall (for feeders the app's own wall doesn't read yet).
   A post opens in a sheet on a phone, a panel at the side wider up.
   ───────────────────────────────────────────────────────────── */

import { useEffect, useMemo, useState, type CSSProperties } from 'react';
import { createPortal } from 'react-dom';
import { appFont } from '@/lib/fonts';
import sectionsData from '@/data/readSections.json';
import { normalizeHandle } from '@/lib/feedLabels';
import './readSections.css';

type Line = { figure: string; unit: string; tone: string; line: string; aside: string };
type Post = { d: string; top: number | null; pat: string | null; fmt: string | null; c: string | null; t: string | null; beat: number; stop: string | null; under: number; ustop: string | null; cap?: string | null; reads?: [string, number, number | null][] | null; trail?: string[]; utrail?: string[] };
type Phase = { name: string; span: string; a: number; b: number; rank: number | null; likes: number | null; gain: number | null; perWeek: number; top: [string, number][]; half: number; read: number; tag: string; best: string[] };
type Lane = { name: string; n: number; es: number; ls: number; rank: number | null; half: number; read: number; inside: { name: string; n: number; rank: number | null }[]; best: string | null };
type Driver = { id: string; label: string; n: number; no: number; w: number; wo: number; sure: string; ids?: string[]; against?: string[] };
type Week = { start: string; m: string; rank: number | null; gain: number | null; posts: number; gap: boolean };
type Metric = 'overall' | 'likes' | 'comments' | 'followers';
export type ReadSection = {
  name: string; feed: string; poss: string; since: number;
  week: Line & { span: string; posts: number; gain: number | null; ids: string[] };
  phases: { head: Line; list: Phase[]; weeks: Week[] };
  lanes: { head: Line; list: Lane[] };
  drivers: { head: Line; rows: Record<Metric, Driver[]> };
  wall: string[];
  posts: Record<string, Post>;
  pats: { name: string; means: string; n: number; rank: number | null; half: number; read: number }[];
};

const ALL = sectionsData as unknown as Record<string, ReadSection>;
export function sectionsFor(handle: string | null | undefined): ReadSection | null {
  if (!handle) return null;
  const h = normalizeHandle(handle);
  const key = Object.keys(ALL).find((k) => normalizeHandle(k) === h);
  return key ? ALL[key] : null;
}

const K = (n: number | null | undefined) => {
  if (n == null) return '–';
  const a = Math.abs(n);
  const s = a >= 1e6 ? `${(a / 1e6).toFixed(2).replace(/\.?0+$/, '')}M` : a >= 1e4 ? `${Math.round(a / 1e3)}K` : a >= 1e3 ? `${(a / 1e3).toFixed(1).replace(/\.0$/, '')}K` : String(Math.round(a));
  return (n < 0 ? '−' : '') + s;
};
const sg = (n: number | null | undefined) => (n == null ? '–' : (n > 0 ? '+' : '') + K(n));
const pct = (v: number) => `${Math.round(v * 100)}%`;
// a rank as people say it; the top half is good news, the top tenth great
const top = (v: number | null | undefined) => (v == null ? '–' : `Top ${Math.max(1, Math.round(v))}%`);
const good = (v: number | null | undefined) => v != null && v <= 25;
const CPN: Record<string, string> = { d1: 'Day 1', d3: 'Day 3', d7: 'Day 7', d21: 'Day 21' };

/* ── the opening line, in Lead's hero grammar ── */
function Lede({ label, head }: { label: string; head: Line }) {
  return (
    <header className="rv-lede" data-tone={head.tone}>
      <span className="rv-label">{label}</span>
      <div className="rv-head"><span className="rv-num">{head.figure}</span><span className="rv-unit">{head.unit}</span></div>
      <p className="rv-line">{head.line} <span className="rv-aside">{head.aside}</span></p>
    </header>
  );
}

function Cover({ id, post, onOpen, ring }: { id: string; post: Post | undefined; onOpen: (id: string) => void; ring?: boolean }) {
  if (!post) return null;
  return (
    <button type="button" className={`rv-cv${ring ? ' ring' : ''}`} onClick={() => onOpen(id)} aria-label={`${post.pat ?? 'Post'}, ${post.d}, ${top(post.top)}`}>
      {post.c ? <img src={post.c} alt="" loading="lazy" decoding="async" /> : null}
      <span className="rv-cd">{post.d}</span>
      {post.top != null ? <span className={`rv-cx${good(post.top) ? ' hot' : ''}`}>{top(post.top)}</span> : null}
    </button>
  );
}

/* ── This week ── */
function ThisWeek({ s, open }: { s: ReadSection; open: (id: string) => void }) {
  const best = s.week.ids[0];
  return (
    <section className="rv-sec" id="rv-week">
      <Lede label="This week" head={s.week} />
      <div className="rv-grid">{s.week.ids.map((id) => <Cover key={id} id={id} post={s.posts[id]} onOpen={open} ring={id === best && good(s.posts[id]?.top)} />)}</div>
    </section>
  );
}

/* ── Phases: the typical rank, week by week; taller is better ── */
function Phases({ s, open }: { s: ReadSection; open: (id: string) => void }) {
  const { list, weeks } = s.phases;
  const phaseOf = (i: number) => list.findIndex((p) => i >= p.a && i < p.b);
  return (
    <section className="rv-sec" id="rv-phases">
      <Lede label="Phases" head={s.phases.head} />
      <div className="rv-traj" role="img" aria-label="The typical post's rank, week by week since April (taller is better)">
        <div className="rv-bars">
          {weeks.map((w, i) => {
            const p = list[phaseOf(i)];
            const h = w.rank != null ? Math.max(4, 101 - w.rank) : 0;
            return (
              <span key={i} className={`rv-bar${w.gap ? ' gap' : ''}${p?.tag === 'best' ? ' best' : ''}${phaseOf(i) % 2 ? ' alt' : ''}`} title={`${w.start}: ${w.gap ? 'not tracked' : `typical post ${top(w.rank)}`}`}>
                {w.gap || w.rank == null ? null : <i style={{ height: `${h}%`, '--rd': `${i * 16}ms` } as CSSProperties} />}
              </span>
            );
          })}
          <span className="rv-mid" aria-hidden="true"><em>top half</em></span>
        </div>
        <div className="rv-phs">{list.map((p, i) => <span key={i} className={`rv-ph${p.tag === 'best' ? ' best' : ''}`} style={{ flex: p.b - p.a }}><b>{p.name}</b></span>)}</div>
        <div className="rv-months">{weeks.map((w, i) => (i === 0 || w.m !== weeks[i - 1].m ? <span key={i} style={{ left: `${(i / weeks.length) * 100}%` }}>{w.m}</span> : null))}</div>
      </div>
      <ol className="rv-plist">
        {list.slice().reverse().map((p) => (
          <li key={p.span} className={`rv-prow${p.tag ? ` ${p.tag}` : ''}`}>
            <div className="rv-pt">
              <span className="rv-pk">{p.span}{p.tag ? <em>{p.tag === 'best' ? 'best stretch' : 'lowest'}</em> : null}</span>
              <b className="rv-pn">{p.name}</b>
              <span className="rv-pm">mostly {p.top.slice(0, 2).map(([n, v]) => `${n.toLowerCase()} ${pct(v)}`).join(', ')}</span>
            </div>
            <div className="rv-ps">
              <span><b className={good(p.rank) || p.tag === 'best' ? 'up' : ''}>{top(p.rank)}</b>typical post</span>
              <span><b>{sg(p.gain)}</b>followers</span>
              <span><b>{p.perWeek}</b>posts a week</span>
            </div>
            <div className="rv-pc">{p.best.map((id) => <Cover key={id} id={id} post={s.posts[id]} onOpen={open} />)}</div>
          </li>
        ))}
      </ol>
    </section>
  );
}

/* ── Lanes ── */
function Lanes({ s, open }: { s: ReadSection; open: (id: string) => void }) {
  const mx = Math.max(...s.lanes.list.flatMap((l) => l.inside.map((r) => r.n)), 1);
  return (
    <section className="rv-sec" id="rv-lanes">
      <Lede label="Lanes" head={s.lanes.head} />
      <div className="rv-lanes">
        {s.lanes.list.map((l) => (
          <article key={l.name} className="rv-lane">
            <div className="rv-lh">
              <b className="rv-ln">{l.name}</b>
              <span className="rv-lshare"><b>{pct(l.ls)}</b>of posts now<em>{pct(l.es)} in spring</em></span>
              <span className="rv-lx"><b className={good(l.rank) ? 'up' : ''}>{top(l.rank)}</b>typical post</span>
            </div>
            <div className="rv-in">
              <span className="rv-sub">What goes in it</span>
              {l.inside.slice(0, 4).map((r) => (
                <div key={r.name} className="rv-ir">
                  <span className="rv-irn">{r.name}</span>
                  <i><em style={{ width: `${(r.n / mx) * 100}%` }} /></i>
                  <span className="rv-irv">{r.n} · <b className={good(r.rank) ? 'up' : ''}>{top(r.rank)}</b></span>
                </div>
              ))}
            </div>
            {l.best ? <div className="rv-lb"><span className="rv-sub">Its best</span><Cover id={l.best} post={s.posts[l.best]} onOpen={open} /></div> : null}
          </article>
        ))}
      </div>
    </section>
  );
}

/* ── Drivers ── */
const METRICS: [Metric, string][] = [['overall', 'Overall'], ['likes', 'Likes'], ['comments', 'Comments'], ['followers', 'Followers']];
function Drivers({ s, open }: { s: ReadSection; open: (id: string) => void }) {
  const [metric, setMetric] = useState<Metric>('overall');
  const [shown, setShown] = useState<string | null>(null);
  const rows = s.drivers.rows[metric] || [];
  const real = rows.filter((r) => r.sure !== 'chance');
  const none = rows.filter((r) => r.sure === 'chance');
  const weeks = metric === 'followers';
  const val = (v: number) => (weeks ? `${sg(v)} a week` : top(v));
  const better = (r: Driver) => (weeks ? r.w > r.wo : r.w < r.wo);
  return (
    <section className="rv-sec" id="rv-drivers">
      <Lede label="Drivers" head={s.drivers.head} />
      <div className="rv-seg" role="group" aria-label="What to read">
        {METRICS.map(([m, l]) => <button key={m} type="button" aria-pressed={m === metric} onClick={() => { setMetric(m); setShown(null); }}>{l}</button>)}
      </div>
      <p className="rv-note">{weeks ? 'Weeks against weeks: what the weeks that gained more followers had.' : `Each fact splits the posts in two. The number is where the typical post ranks with it${metric === 'overall' ? '' : ` on ${metric}`}, against without.`}</p>
      <div className="rv-drv">
        {real.length ? real.map((r) => {
          const up = better(r);
          const isOpen = shown === r.id && !!r.ids;
          return (
            <div key={r.id} className={`rv-dr${isOpen ? ' open' : ''}`}>
              <button type="button" className="rv-drh" onClick={() => setShown(isOpen ? null : r.id)} aria-expanded={isOpen}>
                <span className={`rv-dv${up ? ' up' : ''}`}>{val(r.w)}</span>
                <span className="rv-dt"><b>{r.label}</b><span>{up ? 'Lifts it' : 'Sinks it'} · {val(r.wo)} for the rest · {r.n} {weeks ? 'weeks' : 'posts'}</span></span>
                <span className={`rv-sure${r.sure === 'sure' ? ' s' : ''}`}>{r.sure}</span>
              </button>
              {isOpen && r.ids ? (
                <div className="rv-ev">
                  <div><span className="rv-sub">With it · best first</span><div className="rv-ev4">{r.ids.map((id) => <Cover key={id} id={id} post={s.posts[id]} onOpen={open} />)}</div></div>
                  <div><span className="rv-sub">Without it · best first</span><div className="rv-ev4">{(r.against || []).map((id) => <Cover key={id} id={id} post={s.posts[id]} onOpen={open} />)}</div></div>
                </div>
              ) : null}
            </div>
          );
        }) : <p className="rv-note">Nothing we can test moves this beyond chance yet.</p>}
      </div>
      {none.length ? <p className="rv-none"><b>No real effect:</b> {none.map((r) => r.label.toLowerCase()).join(' · ')}.</p> : null}
    </section>
  );
}

/* ── the wall, for feeders the app's own wall doesn't read yet ── */
function Wall({ s, open }: { s: ReadSection; open: (id: string) => void }) {
  const [rank, setRank] = useState(false);
  const order = useMemo(() => (rank ? s.wall.slice().sort((a, b) => (s.posts[a]?.top ?? 101) - (s.posts[b]?.top ?? 101)) : s.wall), [rank, s]);
  return (
    <section className="rv-sec" id="rv-wall">
      <header className="rv-lede"><span className="rv-label">The wall</span><p className="rv-line">The last 100 posts. <span className="rv-aside">Tap one to see the posts it beat, back to the one it couldn&apos;t.</span></p></header>
      <div className="rv-seg" role="group" aria-label="Order">
        <button type="button" aria-pressed={!rank} onClick={() => setRank(false)}>Recent</button>
        <button type="button" aria-pressed={rank} onClick={() => setRank(true)}>Rank</button>
      </div>
      <div className="rv-wall">{order.map((id, i) => <span key={id} className="rv-wt">{rank && i < 10 ? <i className={i < 3 ? 'first' : ''}>{i + 1}</i> : null}<Cover id={id} post={s.posts[id]} onOpen={open} /></span>)}</div>
    </section>
  );
}

/* ── a post: its rank, its reads and its trail ── */
function PostCard({ s, id, onClose, onOpen }: { s: ReadSection; id: string; onClose: () => void; onOpen: (id: string) => void }) {
  const p = s.posts[id];
  useEffect(() => {
    const key = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', key);
    return () => window.removeEventListener('keydown', key);
  }, [onClose]);
  const down = !!p && !p.beat && p.under > 0;
  const trail = useMemo(() => (p ? (down ? p.utrail : p.trail) || [] : []).filter((k) => s.posts[k]), [p, down, s.posts]);
  if (!p) return null;
  const stopId = down ? p.ustop : p.stop;
  const n = down ? p.under : p.beat;
  const stop = stopId ? s.posts[stopId] : null;
  const pat = s.pats.find((q) => q.name === p.pat);
  const kind = p.top == null ? 'Too new to rank' : p.top <= 10 ? 'Broke out' : p.top >= 90 ? 'Fell flat' : p.top <= 50 ? 'Top half' : 'Bottom half';
  const rec = p.top == null || !pat || pat.rank == null ? '' : `${pat.name} typically lands ${top(pat.rank).toLowerCase()}; ${pat.half} of ${pat.read} made the top half.`;
  const run = p.top == null ? 'Its first reads come in over the next days.' : n ? `${down ? `${n} posts before it in a row did better` : `It beat the last ${n} posts`}${stop ? `, back to ${stop.d} (${top(stop.top).toLowerCase()})` : ''}.` : stop ? `The post before it did better (${top(stop.top).toLowerCase()}).` : '';
  return (
    <div className="rv-card-wrap" onClick={onClose}>
      <aside className="rv-card" role="dialog" aria-label="Post" onClick={(e) => e.stopPropagation()}>
        <div className="rv-card-top"><span className="rv-label">{kind}</span><span className="rv-cardk">{[p.pat, p.fmt, p.d].filter(Boolean).join(' · ')}</span><button type="button" className="rv-x" onClick={onClose}>Close</button></div>
        <div className="rv-card-main">
          <div className="rv-card-cv">{p.c ? <img src={p.c} alt="" /> : null}</div>
          <div>
            <b className={`rv-big${good(p.top) ? ' up' : ''}`}>{top(p.top)}</b>
            <p className="rv-say"><b>{run}</b> {rec}</p>
          </div>
        </div>
        {p.reads?.length ? <div className="rv-reads">{p.reads.map(([k, l, c]) => <div key={k}><span>{CPN[k]}</span><b>{K(l)}</b><i>{K(c)} comments</i></div>)}</div> : null}
        {trail.length || stop ? (
          <div className="rv-trail">
            <span className="rv-sub">{down ? `The ${n} before it that did better` : `The ${n} it beat`}{n > trail.length ? ` · first ${trail.length}` : ''}</span>
            <div className="rv-tstrip">
              {trail.map((k, i) => <span key={k} className="rv-ts"><Cover id={k} post={s.posts[k]} onOpen={onOpen} /><i>{i + 1}</i></span>)}
              {stop && stopId ? <span className="rv-ts stop"><Cover id={stopId} post={stop} onOpen={onOpen} /><i>{down ? '↓' : '↑'}</i></span> : null}
            </div>
            <p className="rv-note">{stop ? (down ? `↓ ${stop.d} is the first one it beat.` : `↑ ${stop.d} did better: that's where its run stops.`) : ''}</p>
          </div>
        ) : null}
        {p.cap ? <p className="rv-cap">“{p.cap}”</p> : null}
      </aside>
    </div>
  );
}

/* the Feeder File's door: after the wall, wherever the wall is */
export function FileDoor({ name, onFile }: { name: string; onFile: () => void }) {
  return (
    <section className="rv-sec rv-filesec">
      <button type="button" className="rv-door" onClick={onFile}>
        <span className="rv-label">Feeder File</span>
        <b>The map of what {name} is made of</b>
        <span>Its patterns, the people in it, the places, what it promotes.</span>
        <i aria-hidden="true">→</i>
      </button>
    </section>
  );
}

type Props = { data: ReadSection; ownWall: boolean; onFile: (() => void) | null };

export default function ReadSections({ data, ownWall, onFile }: Props) {
  const [post, setPost] = useState<string | null>(null);
  return (
    <div className="rv wrap">
      <ThisWeek s={data} open={setPost} />
      <Phases s={data} open={setPost} />
      <Lanes s={data} open={setPost} />
      <Drivers s={data} open={setPost} />
      {ownWall ? <Wall s={data} open={setPost} /> : null}
      {ownWall && onFile ? <FileDoor name={data.name} onFile={onFile} /> : null}
      {/* the post lives at body level, as Read's other overlays do, so no transformed tab container traps it */}
      {post && typeof document !== 'undefined'
        ? createPortal(<div className={`rd ${appFont.variable}`}><PostCard s={data} id={post} onClose={() => setPost(null)} onOpen={setPost} /></div>, document.body)
        : null}
    </div>
  );
}
