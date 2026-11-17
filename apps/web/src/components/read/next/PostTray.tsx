'use client';

/* ─────────────────────────────────────────────────────────────
   THE PICKED POST — off the wall, in two steps.

   1. THE GLANCE. A post tapped on the wall lights its trail, and
      one pill says what it found: the post's picture, how it did
      ("Landed at 21x"), the trail's count rolling up with the wall
      as it lights, and Open. On a phone the pill IS the dock for as
      long as the post is picked: the dock's place, size and glass,
      the post in its circle, Open in its disc; the dock steps out
      of the way underneath (an html class this page's stylesheet
      reads) and comes back when the post is let go. On a laptop
      the same pill stands at the foot of the window.

   2. THE POST. The pill (or the same post tapped again) opens it:
      - a phone: one surface rising out of the pill's own place
        (its caps and middle drawn apart: transforms alone) to at
        most 70% of the screen, never its edges, and folding back
        into it;
      - a laptop: a panel in the middle of the window over a veil
        of the page's colour (the page behind it inert): the still
        large on one side, how it did on the other.
      Both: the verdict, its three figures in type (a carousel has
      no plays: two), one sentence about it, and one still that
      steps (bars over it; its right half on, left half back; a
      swipe; the arrow keys) with its words beside it in a block of
      fixed height.

   It is mounted on the body, as the app's dock is: inside the
   page nothing can be pinned to the screen.
   ───────────────────────────────────────────────────────────── */

import { memo, useCallback, useEffect, useLayoutEffect, useRef, useState, useSyncExternalStore, type CSSProperties, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { ArrowUpRight, ChevronUp, X } from 'lucide-react';
import { cn } from '@/lib/utils';
import { acquireDocumentClass } from '@/lib/rootScrollMode';
import { compact, multiple, pickedPost, shortOf, thousands, type DropPost, type Frame } from './model';

const kindOf = (p: DropPost) => (p.format === 'reel' ? `Reel${p.seconds ? `, ${p.seconds}s` : ''}` : p.slides && p.slides > 1 ? `${p.slides} slides` : p.slides === 1 ? 'Photo' : p.format.charAt(0).toUpperCase() + p.format.slice(1));
const countOf = (n: number) => (n >= 10000 ? compact(n) : thousands(n));
// a still's width over its height, kept within what the stage can hold
const clampRatio = (r: number) => Math.min(1.5, Math.max(0.5, r));
// how long the dock takes to come back once the pill has gone (the class that carries its return is held this long)
const DOCK_BACK_MS = 900;

/* how it did: Landed (crimson) or Missed, at its multiple of his usual likes */
function Verdict({ p }: { p: DropPost }) {
  return (
    <>
      <b className={cn(p.landed && 'is-l')}>{p.landed ? 'Landed' : 'Missed'}</b>
      {p.x_likes != null ? ` at ${multiple(p.x_likes)}` : ''}
    </>
  );
}

/* what the trail found, counted up in step with the wall: the number rolls as each post lights (the loudest type in
   the pill while it does), and the whole line settles once the end point has landed (the wall sets the step past the
   run's length). A number in it is crimson when the post beat them, ink when they did better */
function Count({ text, short, className }: { text: string; short?: boolean; className?: string }) {
  const run = useSyncExternalStore(pickedPost.sub, pickedPost.run, pickedPost.run);
  const step = useSyncExternalStore(pickedPost.sub, pickedPost.step, () => 0);
  const done = run.n === 0 || step > run.n;
  // (the pill's line is narrow: when they did better it drops its first word)
  const said = short ? text.replace(/^The one before it did better$/, 'One before it did better').replace(/^The (\d+) before it did better$/, '$1 before it did better') : text;
  const num = (s: string) => s.split(/(\d+)/).map((x, i) => (/^\d+$/.test(x) ? <b key={i}>{x}</b> : x));
  return (
    <span className={cn('rn-count', !run.up && 'is-ink', !done && step > 0 && 'is-live', className)} style={{ '--tick': `${Math.round(Math.min(220, run.beat * 0.9))}ms` } as CSSProperties}>
      {done ? (
        <span key={`land:${said}`} className="rn-count__l is-land">
          {num(said)}
        </span>
      ) : step === 0 ? (
        <span className="rn-count__l">{' '}</span>
      ) : (
        <span key="run" className="rn-count__l is-run">
          {run.up ? 'Beat ' : short ? '' : 'The '}
          <span className="rn-count__slot">
            <b key={step}>{Math.min(step, run.n)}</b>
          </span>
          {run.up ? ' before it' : ' before it did better'}
        </span>
      )}
    </span>
  );
}

/* a still's words: its time (or which slide), what it shows, what is said */
function Words({ f, i, n, slides }: { f: Frame | null; i: number; n: number; slides: boolean }) {
  const label = !f ? null : slides ? (n > 1 ? `Slide ${i + 1} of ${n}` : null) : f.t;
  return (
    <>
      {label ? <p className="rn-say__t">{label}</p> : null}
      {f?.shows ? <p className="rn-say__shows">{f.shows}</p> : null}
      {f?.said ? <p className="rn-say__said">“{f.said}”</p> : null}
      {!f ? <p className="rn-say__none">No replay stills for this post yet.</p> : null}
    </>
  );
}

type Fig = { key: string; label: string; x: number | null; sub: string };
const figsOf = (p: DropPost, ppl: number | null | undefined): Fig[] => [
  { key: 'likes', label: 'likes', x: p.x_likes, sub: countOf(p.likes) },
  { key: 'comments', label: 'comments', x: p.x_comments, sub: countOf(p.comments) },
  ...(p.format === 'reel' ? [{ key: 'plays', label: 'plays per like', x: ppl ?? null, sub: p.plays ? `${countOf(p.plays)} plays` : 'No plays yet' }] : []),
];

function Figs({ p, ppl }: { p: DropPost; ppl: number | null | undefined }) {
  const figs = figsOf(p, ppl);
  return (
    <dl className="rn-figs" style={{ '--n': figs.length } as CSSProperties}>
      {figs.map((g) => (
        <div key={g.key} className="rn-fig">
          <dt className="rn-fig__k">{g.label}</dt>
          <dd className="rn-fig__v">
            <b className={cn((g.x ?? 0) > 1 && 'is-hot')}>{g.x == null ? '·' : multiple(g.x)}</b>
            <span>{g.sub}</span>
          </dd>
        </div>
      ))}
    </dl>
  );
}

/* the still: bars over it, one picture that stays in place and changes; a tap on its right half steps on, the left half
   back, a swipe sideways too (a swipe that ends on a half is not also a tap) */
function Still({ p, at, dir, ratio, live, onStep }: { p: DropPost; at: number; dir: 1 | -1; ratio: number; live: boolean; onStep: (d: 1 | -1) => void }) {
  const frames = p.frames ?? [];
  const n = frames.length;
  const f = frames[Math.min(at, Math.max(0, n - 1))] ?? null;
  const tab = live ? 0 : -1;
  const swipe = useRef<{ x: number; y: number; id: number } | null>(null);
  const swiped = useRef(false);
  const tap = (d: 1 | -1) => {
    if (swiped.current) {
      swiped.current = false;
      return;
    }
    onStep(d);
  };
  return (
    <div className="rn-still" style={{ '--wr': ratio } as CSSProperties}>
      <div className="rn-still__bars" aria-hidden="true">
        {n > 1 ? frames.map((x, i) => <i key={x.src} className={cn(i < at && 'is-past', i === at && 'is-at')} />) : null}
      </div>
      <figure
        className={cn('rn-still__frame', p.format !== 'reel' && 'is-slide')}
        onPointerDown={(e) => {
          swiped.current = false;
          swipe.current = { x: e.clientX, y: e.clientY, id: e.pointerId };
        }}
        onPointerUp={(e) => {
          const s = swipe.current;
          swipe.current = null;
          if (!s || s.id !== e.pointerId || n < 2) return;
          const dx = e.clientX - s.x;
          if (Math.abs(dx) < 36 || Math.abs(dx) < Math.abs(e.clientY - s.y)) return;
          swiped.current = true;
          onStep(dx < 0 ? 1 : -1);
        }}
        onPointerCancel={() => {
          swipe.current = null;
        }}
      >
        {/* eslint-disable-next-line @next/next/no-img-element */}
        <img key={f?.src ?? p.cover} className={cn(n > 1 && (dir > 0 ? 'is-on' : 'is-back'))} src={f?.src ?? p.cover} alt="" decoding="async" draggable={false} />
        {n > 1 ? (
          <>
            <button type="button" className="rn-still__half is-l" onClick={() => tap(-1)} disabled={at === 0} aria-label="Previous still" tabIndex={tab} />
            <button type="button" className="rn-still__half is-r" onClick={() => tap(1)} disabled={at >= n - 1} aria-label="Next still" tabIndex={tab} />
          </>
        ) : null}
      </figure>
    </div>
  );
}

/* the still's words, whole: every still's words are laid in the same place and only the one shown is seen, so the block
   is as tall as the longest of them and a step never changes its height (a post with unusually long words scrolls in
   this block alone) */
function Say({ p, at }: { p: DropPost; at: number }) {
  const frames = p.frames ?? [];
  const n = frames.length;
  const slides = p.format !== 'reel';
  const box = useRef<HTMLDivElement | null>(null);
  // (a still whose words run past the block fades at its foot until it is scrolled to the end)
  const more = (el: HTMLElement) => {
    if (el.scrollHeight - el.scrollTop - el.clientHeight > 2) el.dataset.more = '';
    else delete el.dataset.more;
  };
  useLayoutEffect(() => {
    box.current?.querySelectorAll<HTMLElement>('.rn-say__f').forEach((el) => {
      el.scrollTop = 0;
      more(el);
    });
  }, [at, p.id]);
  return (
    <div className="rn-say" ref={box} aria-live="polite">
      {n ? (
        frames.map((f, i) => (
          <div key={f.src} className={cn('rn-say__f', i === at && 'is-at')} aria-hidden={i !== at} onScroll={(e) => more(e.currentTarget)}>
            <Words f={f} i={i} n={n} slides={slides} />
          </div>
        ))
      ) : (
        <div className="rn-say__f is-at">
          <Words f={null} i={0} n={0} slides={slides} />
        </div>
      )}
    </div>
  );
}

const metaOf = (p: DropPost) => `${shortOf(p.date)} · ${kindOf(p)}`;

/* the opened post on a phone: the verdict and the way out, one sentence, its figures in one row of type, then the
   still (the largest thing) with that still's words whole beside it */
function PhonePost({ p, ppl, trail, at, dir, ratio, live, onStep, onClose }: { p: DropPost; ppl: number | null | undefined; trail: string; at: number; dir: 1 | -1; ratio: number; live: boolean; onStep: (d: 1 | -1) => void; onClose: () => void }) {
  return (
    <div className="rn-pp">
      <div className="rn-pp__head">
        <div className="rn-pp__id">
          <p className="rn-pp__v">
            <Verdict p={p} />
          </p>
          <p className="rn-pp__m">
            {metaOf(p)}
            {trail ? ` · ${trail}` : ''}
          </p>
        </div>
        <button type="button" className="rn-x" onClick={onClose} aria-label="Fold the post back" tabIndex={live ? 0 : -1}>
          <X aria-hidden="true" strokeWidth={2.4} />
        </button>
      </div>
      <p className="rn-pp__line">{p.line || p.told}</p>
      <Figs p={p} ppl={ppl} />
      <div className="rn-pp__stage" style={{ '--wr': ratio } as CSSProperties}>
        <Still p={p} at={at} dir={dir} ratio={ratio} live={live} onStep={onStep} />
        <div className="rn-pp__side">
          <Say p={p} at={at} />
        </div>
      </div>
    </div>
  );
}

/* the opened post on a laptop: the still large on the left; on the right how it did, its figures, the sentence about
   it, the still's words and the way to the post itself */
function DeskPost({ p, ppl, trail, at, dir, ratio, live, onStep, onClose, closeRef }: { p: DropPost; ppl: number | null | undefined; trail: string; at: number; dir: 1 | -1; ratio: number; live: boolean; onStep: (d: 1 | -1) => void; onClose: () => void; closeRef: (el: HTMLButtonElement | null) => void }) {
  const n = p.frames?.length ?? 0;
  return (
    <div className="rn-dp" style={{ '--wr': ratio } as CSSProperties}>
      <Still p={p} at={at} dir={dir} ratio={ratio} live={live} onStep={onStep} />
      <div className="rn-dp__side">
        <div className="rn-dp__head">
          <p className="rn-dp__v">
            <Verdict p={p} />
          </p>
          <button type="button" ref={closeRef} className="rn-x" onClick={onClose} aria-label="Fold the post back" tabIndex={live ? 0 : -1}>
            <X aria-hidden="true" strokeWidth={2.4} />
          </button>
        </div>
        <p className="rn-dp__m">{metaOf(p)}</p>
        <Count text={trail} className="rn-dp__c" />
        <Figs p={p} ppl={ppl} />
        <p className="rn-dp__line">{p.line || p.told}</p>
        <Say p={p} at={at} />
        <div className="rn-dp__foot">
          <a className="rn-dp__ig" href={p.url} target="_blank" rel="noreferrer" tabIndex={live ? 0 : -1}>
            Open on Instagram
            <ArrowUpRight aria-hidden="true" strokeWidth={2.4} />
          </a>
          {n > 1 ? <span className="rn-dp__keys">{p.format === 'reel' ? `Still ${at + 1} of ${n}` : `Slide ${at + 1} of ${n}`}</span> : null}
        </div>
      </div>
    </div>
  );
}

function PostTray({ byId, plays, phone, onClear }: { byId: Map<string, DropPost>; plays?: Record<string, number | null>; phone: boolean; onClear: () => void }) {
  const id = useSyncExternalStore(pickedPost.sub, pickedPost.get, () => null);
  const opened = useSyncExternalStore(pickedPost.sub, pickedPost.isOpen, () => false);
  // the last post stays drawn while its pill or its sheet leaves
  const [last, setLast] = useState<{ id: string; trail: string } | null>(null);
  if (id && id !== last?.id) setLast({ id, trail: pickedPost.trail() });
  const [at, setAt] = useState(0);
  const [dir, setDir] = useState<1 | -1>(1);
  const [atOf, setAtOf] = useState<string | null>(null);
  if (last && atOf !== last.id) {
    setAtOf(last.id);
    setAt(0);
    setDir(1);
  }
  const pillRef = useRef<HTMLDivElement | null>(null);
  const viewRef = useRef<HTMLDivElement | null>(null);
  const closeBtn = useRef<HTMLButtonElement | null>(null);
  const picked = Boolean(id);
  const p = last ? byId.get(last.id) : null;
  const n = p?.frames?.length ?? 0;

  /* the stage takes the shape of the post's first still (read while the glance is up, so it is known before it opens);
     until then a reel is 9:16 and a carousel 3:4 */
  const [shape, setShape] = useState<{ id: string; r: number } | null>(null);
  const first = p ? (p.frames?.[0]?.src ?? p.cover) : null;
  useEffect(() => {
    if (!p || !first) return undefined;
    const img = new Image();
    img.decoding = 'async';
    img.onload = () => {
      if (img.naturalWidth && img.naturalHeight) setShape({ id: p.id, r: clampRatio(img.naturalWidth / img.naturalHeight) });
    };
    img.src = first;
    return () => {
      img.onload = null;
    };
  }, [p, first]);
  // every still, fetched once it opens, so a step never waits
  useEffect(() => {
    if (!opened || !p?.frames) return;
    p.frames.forEach((x) => {
      const img = new Image();
      img.decoding = 'async';
      img.src = x.src;
    });
  }, [opened, p]);
  const ratio = p && shape?.id === p.id ? shape.r : p?.format === 'reel' ? 9 / 16 : 3 / 4;

  const step = useCallback(
    (d: 1 | -1) => {
      setDir(d);
      setAt((i) => Math.min(Math.max(0, n - 1), Math.max(0, i + d)));
    },
    [n],
  );

  /* a phone: the pill takes the dock's place. The dock steps out underneath for as long as a post is picked and comes
     back after. When the dock is open the two glasses are the same glass, so the pill's is simply there and only what
     they carry changes; when it is folded to its circle, the pill's glass comes in over it */
  useLayoutEffect(() => {
    const pill = pillRef.current;
    if (!phone || !picked || !pill) return undefined;
    const open = () => !document.querySelector('.fm-dock[data-folded]');
    const cut = open();
    if (cut) pill.dataset.cut = '';
    else delete pill.dataset.cut;
    const off = [acquireDocumentClass('rn-glance'), ...(cut ? [acquireDocumentClass('rn-glance-cut')] : [])];
    return () => {
      off.forEach((f) => f());
      const back = open();
      if (back) pill.dataset.cut = '';
      else delete pill.dataset.cut;
      const hold = [acquireDocumentClass('rn-dock-back'), ...(back ? [acquireDocumentClass('rn-dock-back-cut')] : [])];
      window.setTimeout(() => hold.forEach((f) => f()), DOCK_BACK_MS);
    };
  }, [phone, picked]);

  /* the page keeps room under the wall for the pill (a laptop: a phone already keeps the dock's). Where the picked post
     sits is the wall's to decide: it glides it to the top of the screen as it picks it (Feed) */
  useEffect(() => {
    const root = document.querySelector<HTMLElement>('.rn');
    if (!root) return;
    if (picked) root.dataset.tray = 'peek';
    else delete root.dataset.tray;
  }, [picked]);

  /* a phone: the sheet grows out of the pill's own place. Its height and the pill's width are read while the glance is
     up (the sheet is laid out while hidden), so when it opens its caps and middle start exactly on the pill */
  useLayoutEffect(() => {
    const view = viewRef.current;
    const pill = pillRef.current;
    if (!phone || !picked || opened || !view || !pill) return;
    const v = view.getBoundingClientRect();
    const w = pill.getBoundingClientRect().width;
    if (!v.height || !w) return;
    view.style.setProperty('--rise', `${Math.max(0, v.height - 64)}px`);
    view.style.setProperty('--sx', String(Math.min(1, w / v.width)));
    // (styled now, so an opening that follows starts from here)
    view.querySelector('.rn-view__face')?.getBoundingClientRect();
  }, [phone, picked, opened, p?.id, ratio]);

  // a laptop: the page behind the opened post is inert, and the post takes the focus while it is up
  useEffect(() => {
    if (phone || !opened) return undefined;
    const page = document.querySelector<HTMLElement>('.rn');
    page?.setAttribute('inert', '');
    const back = document.activeElement as HTMLElement | null;
    const t = window.setTimeout(() => closeBtn.current?.focus({ preventScroll: true }), 40);
    return () => {
      window.clearTimeout(t);
      page?.removeAttribute('inert');
      if (back && document.contains(back)) back.focus({ preventScroll: true });
    };
  }, [phone, opened]);

  // Esc folds the post back to the glance, and the glance lets the post go; the arrow keys step through its stills
  useEffect(() => {
    if (!picked) return undefined;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.stopPropagation();
        if (pickedPost.isOpen()) pickedPost.setOpen(false);
        else onClear();
      } else if (opened && (e.key === 'ArrowRight' || e.key === 'ArrowLeft') && !e.metaKey && !e.altKey && !e.ctrlKey) {
        const el = document.activeElement;
        if (el && (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA')) return;
        e.preventDefault();
        e.stopPropagation();
        step(e.key === 'ArrowRight' ? 1 : -1);
      }
    };
    window.addEventListener('keydown', onKey, true);
    return () => window.removeEventListener('keydown', onKey, true);
  }, [picked, opened, step, onClear]);

  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  if (!mounted) return null;

  const glance = picked && !opened;
  const live = picked && opened;
  const fold = () => pickedPost.setOpen(false);
  const setClose = (el: HTMLButtonElement | null) => {
    closeBtn.current = el;
  };
  let body: ReactNode = null;
  if (p) {
    body = phone ? (
      <PhonePost key={p.id} p={p} ppl={plays?.[p.id]} trail={last?.trail ?? ''} at={at} dir={dir} ratio={ratio} live={live} onStep={step} onClose={fold} />
    ) : (
      <DeskPost key={p.id} p={p} ppl={plays?.[p.id]} trail={last?.trail ?? ''} at={at} dir={dir} ratio={ratio} live={live} onStep={step} onClose={fold} closeRef={setClose} />
    );
  }

  return createPortal(
    <>
      {/* 1. the glance: the dock's own shape (a phone: in the dock's place) */}
      <div ref={pillRef} className={cn('rn-pill', phone ? 'is-phone' : 'is-desk', picked && 'is-on', opened && 'is-under')} aria-hidden={!glance}>
        <span className="rn-pill__glass" aria-hidden="true" />
        {p ? (
          <button type="button" className="rn-pill__go" onClick={() => pickedPost.setOpen(true)} tabIndex={glance ? 0 : -1} aria-label={`Open the post. ${p.landed ? 'Landed' : 'Missed'} at ${multiple(p.x_likes)} his usual likes. ${last?.trail ?? ''}`}>
            <span className="rn-pill__pic" key={`pic:${p.id}`}>
              {/* eslint-disable-next-line @next/next/no-img-element */}
              <img src={p.cover} alt="" decoding="async" />
            </span>
            <span className="rn-pill__w" key={`w:${p.id}`}>
              <span className="rn-pill__v">
                <Verdict p={p} />
              </span>
              <Count text={last?.trail || metaOf(p)} short className="rn-pill__c" />
            </span>
            <span className="rn-pill__open" aria-hidden="true">
              <ChevronUp strokeWidth={2.6} />
            </span>
          </button>
        ) : null}
      </div>
      {/* 2. the post. A phone: one surface out of the pill. A laptop: a panel over a veil of the page's colour */}
      {phone ? (
        <div ref={viewRef} className={cn('rn-view', live && 'is-on')} role="dialog" aria-label="The post" aria-hidden={!live}>
          <span className="rn-view__sh" aria-hidden="true" />
          <span className="rn-view__face" aria-hidden="true">
            <i className="is-t" />
            <i className="is-m" />
            <i className="is-b" />
          </span>
          <div className="rn-view__in">{body}</div>
        </div>
      ) : (
        <>
          <div className={cn('rn-veil2', live && 'is-on')} aria-hidden="true" onClick={fold} />
          <div className={cn('rn-panel', live && 'is-on')} role="dialog" aria-modal={live} aria-label="The post" aria-hidden={!live}>
            {body}
          </div>
        </>
      )}
    </>,
    document.body,
  );
}

export default memo(PostTray);
