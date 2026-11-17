'use client';

/* ─────────────────────────────────────────────────────────────
   THE FEED — the only place a post's cover is on the page.

   Every post is one tile, mounted once and never re-rendered: a
   button holding its cover, its multiple and (when it is new this
   drop) a New chip, both drawn by the tile's own pseudo-elements,
   and one ring child that every outline on it uses. All tiles are
   the same size everywhere on the page, so a tile only ever MOVES:
   a change of set is a translate per tile, nothing else.

   Motion rules this engine keeps:
   - transform and opacity only, on the compositor (WAAPI); no
     width, height, top/left, clip or shadow is ever animated;
   - one read pass, then one write pass, per change; no script
     runs per frame;
   - every tile's resting transform is written before its
     animation starts, and the animation only fills backwards, so
     the end state IS the last animated frame (nothing settles or
     snaps when a flight ends);
   - outlines are children of their tile (they ride its transform
     and never lag it) and show or hide by an opacity transition.

   The scan is here too: a lens hops post to post over the real
   tiles, each hop one animation, the wall travelling under it.
   ───────────────────────────────────────────────────────────── */

import { memo, useCallback, useEffect, useLayoutEffect, useMemo, useRef, type MutableRefObject } from 'react';
import { fitOf, trueFitOf } from '../files/Pic';
import { playHaptic } from '@/lib/haptics';
import { ASPECT, GAP_DESK, GAP_PHONE, groupedGrid, multiple, shortOf, trailOf, type DropPost, type FeedGroup, type Layout, type Rect, type WallView, pickedPost } from './model';

export const GLIDE = 'cubic-bezier(0.32, 0.72, 0, 1)';
export const ARRIVE = 'cubic-bezier(0.16, 1, 0.3, 1)';
export const EXIT = 'cubic-bezier(0.4, 0, 1, 1)';
// a tile's travel across the wall: it gathers, crosses and settles (a soft start, a long landing)
export const TRAVEL_EASE = 'cubic-bezier(0.42, 0, 0.1, 1)';
/* the wall's one clock: a file opening, switching or closing is one gesture of this length on TRAVEL_EASE, and everything
   that moves with it (the page, the reading under the wall, the row of files) runs on the same length and curve */
export const WALL_MS = 660;

export type Box = { left: number; top: number };
// what the wall looked like just before a change (read before any write of it)
export type Snap = { inner: Box; drawn: Map<string, Rect>; band: { top: number; bottom: number } };
export type ReformOpts = {
  W: number;
  // the tile's width on the wall (the page's own size)
  t: number;
  // a small set laid fewer across than that size gives: its pictures larger (see acrossFor)
  across?: number;
  // a set fitted to the room: its picture width outright (with `across`), and its height (taller than 4:5, to fill it)
  size?: number;
  tall?: number;
  // the page itself is carried from `dy` px lower back to rest over the same glide (the page's scroll moved by dy)
  dy?: number;
  dur: number;
  // the new set's tiles that are new this drop (a small chip)
  fresh?: Set<string>;
  // a search's answer: the caught posts come out of the search box (their snapped rects are the box's thumbnails); the
  // rest of the wall fades first, then they grow out over everything to their places
  fromTray?: boolean;
};
export type ScanOpts = {
  order: string[];
  hits: Set<string>;
  onHop: (read: number, total: number, caught: number) => void;
  // a post was caught (in the wall's order): the search box adds a slot for it, which its picture flies to
  onCatch?: (id: string) => void;
  onDone: () => void;
};
export type FeedApi = {
  snap: () => Snap | null;
  where: () => Box | null;
  reform: (snap: Snap, groups: FeedGroup[], o: ReformOpts) => void;
  // the whole wall moves as one (the shelves left or came back above it): one animation on its inner box
  carry: (fromTop: number, dur: number) => void;
  scan: (o: ScanOpts) => void;
  stopScan: () => void;
  // the wall's height while a scan reads it: a window onto the wall, or null to give it back
  window: (h: number | null) => void;
  // give the window's height back over `dur` ms on the glide's clock (after the re-form: posts travel out of it, none clipped)
  release: (dur: number) => void;
  pick: (id: string | null) => void;
};

/* the trail: the even curve, how far the rest of the wall steps back, the beat between posts and the most the run may
   take, and how long the wall takes to come back (a base, times the app's tempo) */
const EVEN = 'cubic-bezier(0.3, 0.3, 0.2, 1)';
const TRAIL_DIM = 0.14;
const TRAIL_BEAT = 110;
const TRAIL_BEAT_MIN = 45;
const TRAIL_RUN = 1500;
const TRAIL_BACK = 340;
const tempo = () => parseFloat(getComputedStyle(document.documentElement).getPropertyValue('--fm-tempo')) || 1;
const T = (r: { x: number; y: number }) => `translate3d(${r.x}px, ${r.y}px, 0)`;

/* TRAVEL_EASE as a function of time (0..1), for a glide written out frame by frame */
const travelAt = (() => {
  const [x1, y1, x2, y2] = [0.42, 0, 0.1, 1];
  const bx = (t: number) => 3 * x1 * t * (1 - t) ** 2 + 3 * x2 * t * t * (1 - t) + t ** 3;
  const by = (t: number) => 3 * y1 * t * (1 - t) ** 2 + 3 * y2 * t * t * (1 - t) + t ** 3;
  return (x: number) => {
    let lo = 0;
    let hi = 1;
    for (let i = 0; i < 24; i += 1) {
      const mid = (lo + hi) / 2;
      if (bx(mid) < x) lo = mid;
      else hi = mid;
    }
    return by((lo + hi) / 2);
  };
})();

/* a tile's picture cropped for a box of its shape (the wall's own crop, for any shape), laid out in the picture's own
   shape; returns the aspect of what the crop covers */
function cropTo(el: HTMLElement, w: number, h: number): number {
  const img = el.querySelector('img');
  if (!img) return 9 / 16;
  // (a picture not loaded yet is laid as a reel, and laid again in its own shape once it has loaded)
  if (!img.naturalWidth && !img.dataset.wait) {
    img.dataset.wait = '1';
    img.addEventListener('load', () => cropTo(el, el.offsetWidth, el.offsetHeight), { once: true });
  }
  const natural = img.naturalWidth && img.naturalHeight ? img.naturalWidth / img.naturalHeight : 9 / 16;
  const fit = trueFitOf(img.getAttribute('src'), w / h, natural);
  const key = `${(w / h).toFixed(3)}:${natural.toFixed(3)}`;
  if (el.dataset.box !== key) {
    el.dataset.box = key;
    const st = fit.style as Record<string, string | undefined>;
    (['width', 'height', 'left', 'top', 'objectFit'] as const).forEach((k) => (img.style[k] = st[k] ?? ''));
  }
  return fit.region;
}

/* one tile: built once, never redrawn by React (its looks change by classes the engine sets) */
const Tile = memo(function Tile({ post }: { post: DropPost }) {
  return (
    <button
      type="button"
      className="rn-t"
      data-id={post.id}
      data-bd={post.x_likes == null ? '' : multiple(post.x_likes)}
      data-l={post.landed ? '' : undefined}
      tabIndex={-1}
      aria-label={`${shortOf(post.date)}, ${post.landed ? 'landed' : 'missed'} at ${multiple(post.x_likes)} his usual likes. ${post.told}`}
    >
      {/* (the picture in its own face and the chips in their own layer: while a tile changes shape, each is held to its
          own true shape inside the frame, so nothing in it ever stretches) */}
      <span className="rn-t__f">
        {/* eslint-disable-next-line @next/next/no-img-element */}
        <img src={post.cover} alt="" width={320} height={568} decoding="async" draggable={false} style={fitOf(post.cover, 0.8)} />
      </span>
      <span className="rn-t__c" data-bd={post.x_likes == null ? '' : multiple(post.x_likes)} />
      <i className="rn-t__ring" aria-hidden="true" />
    </button>
  );
});

function Feed({ posts, phone, apiRef, line, lead, onShowAll, showAll, view, onView, orderLabel, scanning }: { posts: DropPost[]; phone: boolean; apiRef: MutableRefObject<FeedApi | null>; line: string; lead: string; onShowAll: () => void; showAll: boolean; view: WallView; onView: (v: WallView) => void; orderLabel: string; scanning: boolean }) {
  const boxRef = useRef<HTMLDivElement | null>(null);
  const innerRef = useRef<HTMLDivElement | null>(null);
  const lensRef = useRef<HTMLDivElement | null>(null);
  const byId = useMemo(() => new Map(posts.map((p) => [p.id, p])), [posts]);

  /* the engine's own state: where each tile rests (inner coords), the animation on it, the set shown */
  const els = useRef(new Map<string, HTMLElement>());
  const at = useRef(new Map<string, Rect>());
  const anim = useRef(new Map<string, Animation>());
  const shown = useRef(new Set<string>());
  const extra = useRef<Animation[]>([]);
  const picked = useRef<string | null>(null);
  const layout = useRef<Layout | null>(null);
  const scanT = useRef(0);
  const groupsNow = useRef<FeedGroup[]>([]);
  const veilRef = useRef<HTMLDivElement | null>(null);
  // how far down the wall the scan's veil reaches (0: no veil)
  const veilAt = useRef(0);
  const clearVeil = useCallback((dur: number) => {
    const veil = veilRef.current;
    if (!veil || !veilAt.current) return;
    veilAt.current = 0;
    veil.getAnimations().forEach((a) => a.cancel());
    const off = () => {
      veil.style.visibility = 'hidden';
      veil.style.opacity = '';
    };
    if (!dur) return off();
    const a = veil.animate([{ opacity: 1 }, { opacity: 0 }], { duration: dur, easing: 'cubic-bezier(0.4, 0, 1, 1)', fill: 'forwards' });
    a.onfinish = () => {
      off();
      a.cancel();
    };
  }, []);

  useLayoutEffect(() => {
    innerRef.current?.querySelectorAll<HTMLElement>('.rn-t').forEach((el) => els.current.set(el.dataset.id!, el));
  }, []);

  /* every cover decoded ahead, at idle: a tile never swaps its picture mid-flight */
  useEffect(() => {
    const imgs = [...(innerRef.current?.querySelectorAll('img') ?? [])];
    let i = 0;
    let t = 0;
    const ric = (window as Window & { requestIdleCallback?: (f: () => void, o?: { timeout: number }) => number }).requestIdleCallback;
    const next = () => {
      imgs.slice(i, i + 8).forEach((im) => im.decode().catch(() => {}));
      i += 8;
      if (i < imgs.length) t = ric ? ric(next, { timeout: 800 }) : window.setTimeout(next, 60);
    };
    t = window.setTimeout(next, 200);
    return () => window.clearTimeout(t);
  }, []);

  const where = useCallback((): Box | null => {
    const r = innerRef.current?.getBoundingClientRect();
    return r ? { left: r.left, top: r.top } : null;
  }, []);

  /* READ: where every visible tile is drawn now. A tile mid-flight is read from its running transform (only those) */
  const snap = useCallback((): Snap | null => {
    const inner = innerRef.current;
    const box = boxRef.current;
    if (!inner || !box) return null;
    const ir = inner.getBoundingClientRect();
    const br = box.getBoundingClientRect();
    const clipped = box.classList.contains('is-window');
    const band = { top: Math.max(0, clipped ? br.top : 0), bottom: Math.min(window.innerHeight, clipped ? br.bottom : window.innerHeight) };
    const drawn = new Map<string, Rect>();
    shown.current.forEach((id) => {
      const r = at.current.get(id);
      if (!r) return;
      const a = anim.current.get(id);
      if (a && a.playState === 'running') {
        // (mid-flight it may be between sizes: its scale is about its middle)
        const m = new DOMMatrixReadOnly(getComputedStyle(els.current.get(id)!).transform);
        const k = m.a || 1;
        drawn.set(id, { x: ir.left + m.m41 + ((1 - k) * r.w) / 2, y: ir.top + m.m42 + ((1 - k) * r.h) / 2, w: r.w * k, h: r.h * k });
      } else drawn.set(id, { x: ir.left + r.x, y: ir.top + r.y, w: r.w, h: r.h });
    });
    return { inner: { left: ir.left, top: ir.top }, drawn, band };
  }, []);

  const clearExtra = () => {
    extra.current.forEach((a) => a.cancel());
    extra.current = [];
  };

  const putHeight = (h: number) => {
    const inner = innerRef.current;
    if (!inner) return;
    const v = `${Math.max(0, Math.round(h))}px`;
    if (inner.style.height !== v) inner.style.height = v;
  };

  /* the post card goes with any change of set */
  const trailTimers = useRef<number[]>([]);
  // the posts a trail stepped back (one compositor fade each, held; let go with a fade back)
  const dimmed = useRef(new Map<string, Animation>());
  const dimLeft = useRef(new Set<string>());
  // the shadows under the posts a trail brings forward: one plate each in the wall, under its tile (the tile clips what
  // is inside it), shown and hidden by opacity alone
  const plates = useRef(new Map<string, HTMLElement>());
  const unpick = useCallback(() => {
    const ms = Math.round(TRAIL_BACK * tempo());
    dimLeft.current = new Set(dimmed.current.keys());
    dimmed.current.forEach((a, id) => {
      const el = els.current.get(id);
      const from = el ? Number(getComputedStyle(el).opacity) : TRAIL_DIM;
      a.cancel();
      el?.animate([{ opacity: from }, { opacity: 1 }], { duration: ms, easing: EVEN });
    });
    dimmed.current.clear();
    const id = picked.current;
    picked.current = null;
    pickedPost.set(null);
    if (id) els.current.get(id)?.classList.remove('is-sel');
    trailTimers.current.forEach((t) => window.clearTimeout(t));
    trailTimers.current = [];
    innerRef.current?.querySelectorAll('.rn-wash').forEach((w) => w.remove());
    plates.current.forEach((pl) => {
      pl.classList.remove('is-on');
      window.setTimeout(() => pl.remove(), ms + 60);
    });
    /* the posts it brought forward settle back about their own middle; their origin is given back once they have. When
       the wall re-forms straight after (its flights start in this same task), they settle at once instead, so a flight
       never scales about a borrowed origin */
    const back = [...plates.current.keys()].map((q) => els.current.get(q)).filter((el): el is HTMLElement => Boolean(el));
    plates.current.clear();
    if (back.length) {
      queueMicrotask(() => {
        back.forEach((el) => {
          if (picked.current && els.current.get(picked.current) === el) return;
          const flying = el.getAnimations().some((a) => !(a instanceof CSSTransition) && a.effect instanceof KeyframeEffect && a.effect.getKeyframes().some((f) => 'transform' in f));
          if (!flying) return;
          el.style.transition = 'none';
          el.style.transformOrigin = '';
          el.getBoundingClientRect();
          el.style.transition = '';
        });
      });
      window.setTimeout(() => back.forEach((el) => {
        if (!el.matches('.is-sel, .is-beat, .is-above, .is-stop')) el.style.transformOrigin = '';
      }), ms + 60);
    }
    const inner = innerRef.current;
    if (inner?.classList.contains('is-trail')) {
      inner.classList.remove('is-trail');
      inner.querySelectorAll<HTMLElement>('.rn-t.is-beat, .rn-t.is-above, .rn-t.is-stop, .rn-t.is-end').forEach((t) => t.classList.remove('is-beat', 'is-above', 'is-stop', 'is-end'));
    }
  }, []);

  /* WRITE: the new set, as one staggered gesture on the real tiles. The tiles that leave fall away first (a short stagger
     in reading order); the tiles that stay glide to their new places (a stagger in reading order of where they land);
     tiles that join grow in where they land; the group labels arrive last. Every tile's resting transform is written
     first and its animation only fills backwards, so where a flight ends is where the tile rests */
  const labels = useRef(new Map<string, { el: HTMLElement; x: number; y: number }>());
  const heldRef = useRef<HTMLElement[]>([]);
  const insideRef = useRef<Animation[]>([]);
  const heldTimer = useRef(0);
  const reform = useCallback(
    (s: Snap, groups: FeedGroup[], o: ReformOpts) => {
      const inner = innerRef.current;
      if (!inner) return;
      unpick();
      clearExtra();
      const L = groupedGrid(groups, o.W, o.t, phone, o.across, o.size, o.tall);
      layout.current = L;
      groupsNow.current = groups;
      // the new box's place on screen at the first frame (the page may be carried from dy lower)
      const dy = o.dy ?? 0;
      const ir = inner.getBoundingClientRect();
      const base = { left: ir.left, top: ir.top + dy };
      const hWas = inner.offsetHeight;
      putHeight(L.height);
      const H = window.innerHeight;
      const vis = (r: Rect, b = s.band) => r.y + r.h > b.top - 4 && r.y < b.bottom + 4;
      const next = new Set(L.tiles.keys());
      const dur = o.dur;
      type Move = { el: HTMLElement; id: string; from: Rect | null; to: Rect; order: number };
      const moves: Move[] = [];
      const leaving: { el: HTMLElement; id: string; r: Rect; o: number; order: number }[] = [];
      // 1. stop what runs (its current place was read in the snap)
      anim.current.forEach((a) => a.cancel());
      anim.current.clear();
      window.clearTimeout(heldTimer.current);
      heldRef.current.forEach((el) => (el.style.zIndex = ''));
      insideRef.current.forEach((x) => x.cancel());
      insideRef.current = [];
      heldRef.current = [];
      // 2. resting places, written once
      els.current.forEach((el, id) => {
        const was = s.drawn.get(id) ?? null;
        const to = L.tiles.get(id);
        const fresh = Boolean(o.fresh?.has(id));
        if (el.classList.contains('is-new') !== fresh) el.classList.toggle('is-new', fresh);
        // (a post the scan dimmed leaves from where it is drawn: dim)
        const old = at.current.get(id);
        const dim = veilAt.current > 0 && Boolean(old && old.y + old.h <= veilAt.current);
        // (a catch lets go of its size and its own origin at once: the flight below owns its scale from here)
        if (el.classList.contains('is-hit')) {
          el.style.transition = 'none';
          el.classList.remove('is-hit');
          el.style.transformOrigin = '';
          void el.offsetWidth;
          el.style.transition = '';
        }
        if (to) {
          at.current.set(id, to);
          el.style.transform = T(to);
          // (its size, when the set is laid larger than the wall's own: the tile is drawn at that size, so it rests
          // sharp, and the glide below takes it there from the size it had)
          const own = Math.abs(to.w - o.t) < 0.5 && Math.abs(to.h - Math.round(o.t * ASPECT)) < 0.5;
          const wpx = own ? '' : `${to.w}px`;
          if (el.style.width !== wpx) el.style.width = wpx;
          const hpx = own ? '' : `${to.h}px`;
          if (el.style.height !== hpx) el.style.height = hpx;
          cropTo(el, to.w, to.h);
          if (el.style.visibility !== 'visible') el.style.visibility = 'visible';
          const toScreen = { x: base.left + to.x, y: base.top + to.y, w: to.w, h: to.h };
          const onTo = toScreen.y + to.h > -4 && toScreen.y < H + 4;
          if (was) {
            if (vis(was) || onTo) moves.push({ el, id, from: was, to, order: toScreen.y * 4 + toScreen.x / 400 });
          } else if (onTo) moves.push({ el, id, from: null, to, order: toScreen.y * 4 + toScreen.x / 400 });
        } else {
          at.current.delete(id);
          if (was && vis(was)) leaving.push({ el, id, r: was, o: dim ? 0.34 : dimLeft.current.has(id) ? 0.32 : 1, order: was.y * 4 + was.x / 400 });
          else if (el.style.visibility !== 'hidden') el.style.visibility = 'hidden';
        }
      });
      shown.current = next;
      // the labels: the ones that stay travel, the new ones are made now (shown after the tiles), the old ones go
      const keep = new Map<string, { el: HTMLElement; x: number; y: number }>();
      const labelPlans: { el: HTMLElement; from: { x: number; y: number } | null; to: { x: number; y: number } }[] = [];
      L.labels.forEach((l) => {
        const was = labels.current.get(l.key);
        const el = was?.el ?? document.createElement('p');
        if (!was) {
          el.className = 'rn-gl';
          inner.appendChild(el);
        }
        el.getAnimations().forEach((a) => a.cancel());
        const words = el.firstElementChild instanceof HTMLSpanElement ? el.firstElementChild : el.appendChild(document.createElement('span'));
        words.getAnimations().forEach((a) => a.cancel());
        if (words.textContent !== l.text) words.textContent = l.text;
        el.style.transform = T(l);
        keep.set(l.key, { el, x: l.x, y: l.y });
        labelPlans.push({ el, from: was ? { x: was.x + (s.inner.left - base.left), y: was.y + (s.inner.top - base.top) } : null, to: l });
      });
      const goneLabels: HTMLElement[] = [];
      labels.current.forEach((v, k) => {
        if (keep.has(k)) return;
        v.el.getAnimations().forEach((a) => a.cancel());
        (v.el.firstElementChild as HTMLElement | null)?.getAnimations().forEach((a) => a.cancel());
        goneLabels.push(v.el);
      });
      labels.current = keep;
      if (!dur) {
        leaving.forEach(({ el }) => (el.style.visibility = 'hidden'));
        goneLabels.forEach((el) => el.remove());
        clearVeil(0);
        return;
      }
      // the scan's veil goes as the dimmed posts leave (they leave from the same shade)
      clearVeil(240);
      // the same set moved as one (the rail stepped aside or came back): one animation on the box, not one per tile
      if (!leaving.length && !goneLabels.length && moves.length > 1 && moves.every((m) => m.from)) {
        const dx = moves[0].from!.x - base.left - moves[0].to.x;
        const dy0 = moves[0].from!.y - base.top - moves[0].to.y;
        if (moves.every((m) => Math.abs(m.from!.w - m.to.w) < 0.5 && Math.abs(m.from!.x - base.left - m.to.x - dx) < 0.5 && Math.abs(m.from!.y - base.top - m.to.y - dy0) < 0.5)) {
          if (Math.abs(dx) >= 0.5 || Math.abs(dy0) >= 0.5) extra.current.push(inner.animate([{ transform: `translate3d(${dx}px, ${dy0}px, 0)` }, { transform: 'translate3d(0, 0, 0)' }], { duration: dur, easing: GLIDE, fill: 'backwards' }));
          return;
        }
      }
      const tray = Boolean(o.fromTray);
      // (a search's answer comes out of the search box: its own gesture, below; every other change is the reshape)
      let underDur = dur;
      if (tray) {
        /* 3. OUT OF THE SEARCH BOX (as approved): the wall clears in place, the caught posts set off from their
           thumbnails in the box one after another, left to right, and the rest touch down row by row */
        const u = dur / 560;
        const rowH = (L.tiles.values().next().value?.h ?? leaving[0]?.r.h ?? 160) + (phone ? GAP_PHONE : GAP_DESK);
        const ripple = (pts: { x: number; y: number }[], perRow: number, cap: number, lean: number) => {
          let y0 = Infinity;
          let y1 = -Infinity;
          pts.forEach((q) => {
            y0 = Math.min(y0, q.y);
            y1 = Math.max(y1, q.y);
          });
          const span = y1 > y0 ? Math.min(cap, ((y1 - y0) / rowH) * perRow) : 0;
          return (q: { x: number; y: number }) => Math.round((y1 > y0 ? ((q.y - y0) / (y1 - y0)) * span : 0) + Math.max(0, Math.min(1, q.x / Math.max(1, o.W))) * lean);
        };
        const clampY = (y: number) => Math.max(s.band.top - rowH, Math.min(s.band.bottom, y));
        // where each one is drawn now and where it will rest, on screen (what the eye sees), x inside the wall
        const hereOf = (r: Rect) => ({ x: r.x - base.left, y: clampY(r.y) });
        const thereOf = (r: Rect) => ({ x: r.x, y: Math.max(-rowH, Math.min(H, base.top + r.y)) });
        const depart = ripple([...leaving.map((l) => hereOf(l.r)), ...moves.filter((m) => m.from).map((m) => hereOf(m.from!))], 26 * u, 150 * u, 36 * u);
        const land = ripple(moves.map((m) => thereOf(m.to)), 40 * u, 260 * u, 50 * u);
        const TRAVEL = Math.round(620 * u);
        // (the side a post leaves by or comes in from: the nearer edge of the screen; a wall standing at the screen's right,
        // beside the reading, always uses its right edge, so nothing crosses the reading)
        const VW = window.innerWidth;
        const wallR = inner.getBoundingClientRect();
        const rightOnly = wallR.left > VW * 0.25;
        const outX = (screenX: number, w: number) => (rightOnly || screenX + w / 2 > VW / 2 ? VW + 12 - screenX : -(screenX + w + 12));
        // (from the search box: the wall clears first, a quick fade in place, and the caught posts set off as it goes)
        const OUT = Math.round(220 * u);
        const after = tray ? Math.round(OUT * 0.6) : 0;
        {
          const feedEl = inner.closest<HTMLElement>('.rn-feed');
          boxRef.current?.classList.add('is-tray');
          feedEl?.classList.add('is-tray');
          window.setTimeout(() => {
            boxRef.current?.classList.remove('is-tray');
            feedEl?.classList.remove('is-tray');
          }, after + Math.round(940 * u) + 12 * Math.round(110 * u) + 40);
        }
        // the staying set off a beat after the leaving start to go
        const lead = leaving.length && !tray ? Math.round(140 * u) : 0;
        const LEAVE = Math.round(520 * u);
        leaving.forEach(({ el, id, r, o: o0 }) => {
          const x = r.x - base.left;
          const y = r.y - base.top;
          el.style.transform = T({ x, y });
          const a = tray
            ? el.animate([{ opacity: o0, transform: T({ x, y }) }, { opacity: 0, transform: T({ x, y }) }], { duration: OUT, easing: 'linear', fill: 'both' })
            : el.animate([{ opacity: o0, transform: T({ x, y }) }, { opacity: o0, transform: T({ x: x + outX(r.x, r.w), y }) }], { duration: LEAVE, delay: depart(hereOf(r)), easing: TRAVEL_EASE, fill: 'both' });
          anim.current.set(id, a);
          a.onfinish = () => {
            if (anim.current.get(id) !== a) return;
            el.style.visibility = 'hidden';
            a.cancel();
            anim.current.delete(id);
          };
        });
        // (out of the search box: slower and one after another, left to right as they sat in the box, each on the
        // gather-cross-settle curve)
        const TRAY = Math.round(940 * u);
        const STEP = Math.round(110 * u);
        const trayOrder = tray ? moves.filter((m) => m.from).sort((a, b) => a.from!.x - b.from!.x).map((m) => m.id) : [];
        const ENTER = Math.round(560 * u);
        moves.forEach((m) => {
          const ti = trayOrder.indexOf(m.id);
          const touch = lead + after + TRAVEL + land(thereOf(m.to));
          let a: Animation;
          if (m.from) {
            // its flight in inner coords: from where it was drawn on screen (a far one starts at most 60% of a screen off)
            let fy = m.from.y - base.top;
            const lim = H * 0.6 + m.to.h;
            if (m.from.y < -lim) fy = -lim - base.top;
            if (m.from.y > H + lim) fy = H + lim - base.top;
            const fx = m.from.x - base.left;
            // (and from the size it was drawn at: it grows or shrinks along the same glide, about its own middle, so
            // where it is seen to set off is where it was)
            const k = m.from.w / m.to.w;
            const sized = Math.abs(k - 1) > 0.005;
            if (!sized && Math.abs(fx - m.to.x) < 0.5 && Math.abs(fy - m.to.y) < 0.5) return;
            const first = sized ? `${T({ x: fx - ((1 - k) * m.to.w) / 2, y: fy - ((1 - k) * m.to.h) / 2 })} scale(${k.toFixed(4)})` : T({ x: fx, y: fy });
            const start = ti >= 0 ? after + ti * STEP : lead + after + depart(hereOf(m.from));
            a = m.el.animate(
              [{ transform: first }, { transform: sized ? `${T(m.to)} scale(1)` : T(m.to) }],
              ti >= 0 ? { duration: TRAY, delay: start, easing: 'cubic-bezier(0.33, 0.04, 0.16, 1)', fill: 'backwards' } : { duration: Math.max(Math.round(380 * u), touch - start), delay: start, easing: TRAVEL_EASE, fill: 'backwards' },
            );
          } else {
            // a post joining comes in from the nearer edge, at its own size, and touches down with its row
            const sx = base.left + m.to.x;
            const fromX = m.to.x - outX(sx, m.to.w);
            a = m.el.animate([{ transform: T({ x: fromX, y: m.to.y }) }, { transform: T(m.to) }], { duration: ENTER, delay: Math.max(0, touch - ENTER), easing: TRAVEL_EASE, fill: 'backwards' });
          }
          anim.current.set(m.id, a);
          a.onfinish = () => anim.current.get(m.id) === a && anim.current.delete(m.id);
        });
        underDur = lead + TRAVEL;
      } else {
        /* 3. THE RESHAPE, on one clock: the gesture starts on one frame and ends on one frame, WALL_MS later.
             - a post in both sets (a file opening out of the wall, or closing back into it) travels from where it
               stands to its new place, growing or shrinking to its new size, for the whole gesture;
             - a post that goes sinks a little as it fades, and is gone a little before halfway;
             - a post that comes rises a little as it fades in, from a third of the way, and lands on the gesture's
               last frame, with the posts that travelled.
           A file to another file shares no posts: the one sinks away as the other rises in. */
        const D = dur;
        const OUT = Math.round(D * 0.45);
        const IN_AT = Math.round(D * 0.33);
        const SINK = Math.round(Math.max(14, Math.min(28, (L.tiles.values().next().value?.h ?? 120) * 0.16)));
        const lim = H * 0.6;
        const local = (r: Rect): Rect => {
          let y = r.y - base.top;
          if (r.y < -(lim + r.h)) y = -(lim + r.h) - base.top;
          if (r.y > H + lim + r.h) y = H + lim + r.h - base.top;
          return { x: r.x - base.left, y, w: r.w, h: r.h };
        };
        const held: HTMLElement[] = [];
        const inside = insideRef.current;
        const lift = (el: HTMLElement, z: number) => {
          el.style.zIndex = String(z);
          held.push(el);
        };
        const own = (id: string, a: Animation | null, end?: () => void) => {
          if (!a) return end?.();
          anim.current.set(id, a);
          a.onfinish = () => {
            if (anim.current.get(id) !== a) return;
            anim.current.delete(id);
            end?.();
          };
        };
        moves.forEach((m) => {
          if (m.from) {
            const from = local(m.from);
            const to = m.to;
            const kx0 = from.w / to.w;
            const ky0 = from.h / to.h;
            const sized = Math.abs(kx0 - 1) > 0.005 || Math.abs(ky0 - 1) > 0.005;
            if (!sized && Math.abs(from.x - to.x) < 0.5 && Math.abs(from.y - to.y) < 0.5) return;
            lift(m.el, 2);
            if (Math.abs(kx0 - ky0) < 0.01) {
              const first = sized ? `${T({ x: from.x - ((1 - kx0) * to.w) / 2, y: from.y - ((1 - kx0) * to.h) / 2 })} scale(${kx0.toFixed(4)})` : T(from);
              own(m.id, m.el.animate([{ transform: first }, { transform: sized ? `${T(to)} scale(1)` : T(to) }], { duration: D, easing: TRAVEL_EASE, fill: 'backwards' }));
              return;
            }
            /* it changes shape (the wall's 4:5 to a fitted set's taller tiles, or back). The frame stretches as it travels;
               inside it the picture is scaled back to its true shape, covering the frame the way the crop covers any box,
               and the chips are held at their true shape in their corners. Written out frame by frame on the one curve */
            const rho = cropTo(m.el, to.w, to.h);
            const cover = (kx: number, ky: number) => Math.max((kx * to.w) / rho, ky * to.h) / Math.max(to.w / rho, to.h);
            const frame: Keyframe[] = [];
            const face: Keyframe[] = [];
            const chip: Keyframe[] = [];
            const N = 20;
            for (let i = 0; i <= N; i += 1) {
              const e = travelAt(i / N);
              const kx = kx0 + (1 - kx0) * e;
              const ky = ky0 + (1 - ky0) * e;
              const x = from.x + (to.x - from.x) * e;
              const y = from.y + (to.y - from.y) * e;
              const c = cover(kx, ky);
              const u = (kx + ky) / 2;
              frame.push({ offset: i / N, transform: `translate3d(${(x - ((1 - kx) * to.w) / 2).toFixed(2)}px, ${(y - ((1 - ky) * to.h) / 2).toFixed(2)}px, 0) scale(${kx.toFixed(4)}, ${ky.toFixed(4)})` });
              face.push({ offset: i / N, transform: `scale(${(c / kx).toFixed(4)}, ${(c / ky).toFixed(4)})` });
              chip.push({ offset: i / N, transform: `scale(${(u / kx).toFixed(4)}, ${(u / ky).toFixed(4)})` });
            }
            const run = { duration: D, easing: 'linear', fill: 'backwards' as FillMode };
            own(m.id, m.el.animate(frame, run));
            const f = m.el.querySelector<HTMLElement>('.rn-t__f');
            const cl = m.el.querySelector<HTMLElement>('.rn-t__c');
            if (f) inside.push(f.animate(face, run));
            if (cl) {
              inside.push(cl.animate(chip, { ...run, pseudoElement: '::after' }));
              if (m.el.classList.contains('is-new')) inside.push(cl.animate(chip, { ...run, pseudoElement: '::before' }));
            }
          } else {
            own(m.id, m.el.animate([{ opacity: 0, transform: T({ x: m.to.x, y: m.to.y + SINK }) }, { opacity: 1, transform: T(m.to) }], { duration: D - IN_AT, delay: IN_AT, easing: ARRIVE, fill: 'backwards' }));
          }
        });
        leaving.forEach(({ el, id, r, o: o0 }) => {
          const at = local(r);
          el.style.transform = T(at);
          own(id, el.animate([{ opacity: o0, transform: T(at) }, { opacity: 0, transform: T({ x: at.x, y: at.y + SINK }) }], { duration: OUT, easing: EXIT, fill: 'forwards' }), () => {
            el.style.visibility = 'hidden';
            el.getAnimations().forEach((x) => x.cancel());
          });
        });
        // (the travelling posts ride over the ones going, for the gesture only)
        heldRef.current = held;
        window.clearTimeout(heldTimer.current);
        heldTimer.current = window.setTimeout(() => {
          held.forEach((el) => (el.style.zIndex = ''));
          if (heldRef.current === held) heldRef.current = [];
        }, D + 40);
      }
      // the labels: one that stays travels with its row; a new one rises in with the posts that come, an old one sinks
      // away with the posts that go
      labelPlans.forEach(({ el, from, to }) => {
        if (from && (Math.abs(from.x - to.x) > 0.5 || Math.abs(from.y - to.y) > 0.5)) el.animate([{ transform: T(from) }, { transform: T(to) }], { duration: dur, easing: TRAVEL_EASE, fill: 'backwards' });
        else if (!from) el.animate([{ opacity: 0, transform: T({ x: to.x, y: to.y + 12 }) }, { opacity: 1, transform: T(to) }], { duration: dur - Math.round(dur * 0.33), delay: Math.round(dur * 0.33), easing: ARRIVE, fill: 'backwards' });
      });
      goneLabels.forEach((el) => {
        const cur = el.style.transform;
        const a = el.animate([{ opacity: 1, transform: `${cur} translate3d(0, 0, 0)` }, { opacity: 0, transform: `${cur} translate3d(0, 12px, 0)` }], { duration: Math.round(dur * 0.45), easing: EXIT, fill: 'forwards' });
        a.onfinish = () => el.remove();
      });
      // what sits under the wall (a phone: the reading) rides the wall's change of height on the same clock
      const under = inner.closest('.rn-feed')?.nextElementSibling as HTMLElement | null;
      let dh = hWas - Math.round(L.height);
      /* (the wall shrank by more than a screen, as from the hundred to a file's few: the reading would appear at once
         under posts still in flight. It rises from the foot of the screen instead, as far as it has to) */
      if (under && dh > H) {
        const firstTop = under.getBoundingClientRect().top + dy;
        dh = Math.max(0, Math.round(H + 8 - firstTop));
      }
      if (!boxRef.current?.classList.contains('is-window') && under?.classList.contains('rn-pane') && Math.abs(dh) >= 1 && Math.abs(dh) <= H) extra.current.push(under.animate([{ transform: `translate3d(0, ${dh}px, 0)` }, { transform: 'translate3d(0, 0, 0)' }], { duration: underDur, easing: TRAVEL_EASE, fill: 'backwards' }));
    },
    [phone, unpick, clearVeil],
  );

  const carry = useCallback((fromTop: number, dur: number) => {
    const inner = innerRef.current;
    if (!inner || !dur) return;
    const ir = inner.getBoundingClientRect();
    const d = fromTop - ir.top;
    if (Math.abs(d) < 0.5) return;
    extra.current.push(inner.animate([{ transform: `translate3d(0, ${d}px, 0)` }, { transform: `translate3d(0, ${-scanT.current}px, 0)` }], { duration: dur, easing: GLIDE, fill: 'backwards' }));
  }, []);

  const releaseTimer = useRef(0);
  const setWindow = useCallback((h: number | null) => {
    const box = boxRef.current;
    if (!box) return;
    window.clearTimeout(releaseTimer.current);
    if (h == null) {
      box.classList.remove('is-window', 'is-release');
      box.style.height = '';
      return;
    }
    box.classList.add('is-window');
    box.style.height = `${Math.round(h)}px`;
  }, []);

  /* the window lets go on the same clock as the wall's glide: the box's height runs from the window's to the wall's own
     (the page below rides it), and the clip keeps only its top edge, so a post is never cut off mid-flight */
  const release = useCallback(
    (dur: number) => {
      const box = boxRef.current;
      if (!box) return;
      window.clearTimeout(releaseTimer.current);
      const from = box.getBoundingClientRect().height;
      if (!dur || !box.classList.contains('is-window')) return setWindow(null);
      box.style.height = '';
      const to = box.getBoundingClientRect().height;
      box.classList.add('is-release');
      if (Math.abs(from - to) >= 1) extra.current.push(box.animate([{ height: `${from}px` }, { height: `${to}px` }], { duration: Math.round(dur * (760 / 560)), easing: GLIDE }));
      releaseTimer.current = window.setTimeout(() => {
        box.classList.remove('is-window', 'is-release');
        box.style.height = '';
      }, Math.round(dur * 2) + 120);
    },
    [setWindow],
  );

  /* ── the scan: the lens hops over every post in the wall's order. A miss: a short hop (it speeds up over a run of
     misses), the post it leaves dims. A catch: the lens brakes, rings crimson, the post comes forward and keeps its
     ring. The wall travels under the lens so the lens stays in the middle of the window ── */
  const scanRun = useRef<{ timer: number; stop: boolean } | null>(null);
  // undo functions for what the scan has in the air (a pluck, a clone in flight, its landing timer)
  const scanFx = useRef(new Set<() => void>());
  const stopScan = useCallback(() => {
    const s = scanRun.current;
    if (s) {
      s.stop = true;
      window.clearTimeout(s.timer);
    }
    scanRun.current = null;
    innerRef.current?.classList.remove('is-scanning');
    const lens = lensRef.current;
    if (lens) {
      lens.getAnimations().forEach((a) => a.cancel());
      lens.style.visibility = 'hidden';
      lens.classList.remove('is-hit', 'is-pluck');
    }
    clearVeil(200);
    scanFx.current.forEach((fn) => fn());
    scanFx.current.clear();
  }, [clearVeil]);
  const scan = useCallback(
    (o: ScanOpts) => {
      const inner = innerRef.current;
      const box = boxRef.current;
      const lens = lensRef.current;
      if (!inner || !box || !lens) return;
      stopScan();
      unpick();
      const run = { timer: 0, stop: false };
      scanRun.current = run;
      inner.classList.add('is-scanning');
      const winH = box.clientHeight;
      const Lh = layout.current?.height ?? 0;
      let k = 0;
      let caught = 0;
      let per = 90;
      let prev: { x: number; y: number } | null = null;
      lens.style.visibility = 'visible';
      veilAt.current = 0;
      const still = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      const kt = tempo();
      // caught posts the lens has not yet gone past: they fly to the search box when it leaves them
      const pending: string[] = [];
      let landBy = 0;
      const slotOf = (id: string) => document.querySelector<HTMLElement>(`[data-caught="${id}"]`);
      const settle = (el: HTMLElement, from: number) => {
        if (still) return;
        el.animate([{ scale: String(from) }, { scale: '1' }], { duration: Math.round(320 * kt), easing: ARRIVE });
      };
      // the picture leaves its tile on a gentle arc and shrinks into its slot; the slot shows it when it lands
      // a caught picture, lifted off its post: an outer box (it carries x on the flight) and the picture (y and size)
      /* a catch comes forward larger about its own middle: a tile's place is its transform, so its scale is taken about
         where it rests in the wall, never about the wall's corner (which would carry it into a neighbour's cell) */
      const markHit = (id: string) => {
        const el = els.current.get(id);
        const r = at.current.get(id);
        if (!el) return;
        if (r) el.style.transformOrigin = `${r.x + r.w / 2}px ${r.y + r.h / 2}px`;
        el.classList.add('is-hit');
      };
      const lifted = new Map<string, { clone: HTMLDivElement; pic: HTMLImageElement; k: number }>();
      // (clearly larger: the catch comes forward while the lens holds on it, and only then goes to the box)
      const LIFT = 1.2;
      const lift = (id: string) => {
        const el = els.current.get(id);
        const src = el?.querySelector('img')?.currentSrc || byId.get(id)?.cover;
        if (!el || !src) return;
        const a = el.getBoundingClientRect();
        const clone = document.createElement('div');
        clone.setAttribute('aria-hidden', 'true');
        Object.assign(clone.style, { position: 'fixed', left: `${a.left}px`, top: `${a.top}px`, width: `${a.width}px`, height: `${a.height}px`, zIndex: '9999', pointerEvents: 'none', willChange: 'translate' });
        const pic = document.createElement('img');
        pic.src = src;
        pic.alt = '';
        pic.draggable = false;
        Object.assign(pic.style, { display: 'block', width: '100%', height: '100%', objectFit: 'cover', borderRadius: getComputedStyle(el).borderRadius, margin: '0', maxWidth: 'none', willChange: 'translate, scale' });
        clone.appendChild(pic);
        document.body.appendChild(clone);
        // it comes up toward the eye: clearly larger about its own middle, the crimson ring of a catch on it, a shadow all
        // round (not dropped below it, which reads as falling)
        pic.animate(
          [
            { scale: '1', boxShadow: '0 0 0 0 rgb(var(--fm-accent-rgb) / 0), 0 0 0 rgb(0 0 0 / 0)' },
            { scale: String(LIFT), boxShadow: '0 0 0 2.5px rgb(var(--fm-accent-rgb) / 1), 0 6px 26px 0 rgb(0 0 0 / 0.22)' },
          ],
          { duration: Math.round(320 * kt), easing: 'cubic-bezier(0.25, 0.8, 0.3, 1)', fill: 'forwards' },
        );
        // (it stays over its post while the wall travels under the lens: the copy follows the post, frame by frame)
        const follow = () => {
          if (!lifted.has(id) || run.stop) return;
          const r = el.getBoundingClientRect();
          clone.style.left = `${r.left}px`;
          clone.style.top = `${r.top}px`;
          requestAnimationFrame(follow);
        };
        requestAnimationFrame(follow);
        lifted.set(id, { clone, pic, k: LIFT });
        scanFx.current.add(() => clone.remove());
      };
      const fly = (id: string, tries = 0) => {
        const el = els.current.get(id);
        if (!el) return;
        const slot = slotOf(id);
        if (!slot) {
          // (the box draws the slot a frame after the catch: look once more, then leave the post where it is)
          if (tries < 3) {
            const t = window.setTimeout(() => !run.stop && fly(id, tries + 1), 60);
            const undo = () => window.clearTimeout(t);
            scanFx.current.add(undo);
          } else {
            lifted.get(id)?.clone.remove();
            lifted.delete(id);
          }
          return;
        }
        const show = () => {
          const s2 = slotOf(id);
          if (s2) s2.dataset.in = '1';
        };
        if (still) {
          show();
          return;
        }
        const a = el.getBoundingClientRect();
        const b = slot.getBoundingClientRect();
        const src = el.querySelector('img')?.currentSrc || byId.get(id)?.cover;
        if (!src || !a.width || !b.width) {
          lifted.get(id)?.clone.remove();
          lifted.delete(id);
          show();
          return;
        }
        // an outer box carries x (on GLIDE), the picture inside carries y and its shrink (on TRAVEL_EASE) as separate
        // translate and scale properties: translate is applied before scale, so the drop is never shrunk with the picture
        // and the picture lands centred on its slot. x leading y bows the path.
        // (the lifted copy goes on from where it hangs; a catch with no lift, a fresh copy at the post)
        const held = lifted.get(id);
        lifted.delete(id);
        const k0 = held?.k ?? 1;
        let clone: HTMLDivElement;
        let pic: HTMLImageElement;
        if (held) {
          ({ clone, pic } = held);
          pic.getAnimations().forEach((an) => an.cancel());
          Object.assign(pic.style, { scale: String(k0), boxShadow: '0 0 0 2.5px rgb(var(--fm-accent-rgb) / 1), 0 6px 26px 0 rgb(0 0 0 / 0.22)' });
        } else {
          clone = document.createElement('div');
          clone.setAttribute('aria-hidden', 'true');
          Object.assign(clone.style, { position: 'fixed', left: `${a.left}px`, top: `${a.top}px`, width: `${a.width}px`, height: `${a.height}px`, zIndex: '9999', pointerEvents: 'none', willChange: 'translate' });
          pic = document.createElement('img');
          pic.src = src;
          pic.alt = '';
          pic.draggable = false;
          Object.assign(pic.style, { display: 'block', width: '100%', height: '100%', objectFit: 'cover', borderRadius: getComputedStyle(el).borderRadius, boxShadow: '0 10px 22px -12px rgb(0 0 0 / 0.28)', willChange: 'translate, scale', margin: '0', maxWidth: 'none' });
          clone.appendChild(pic);
          document.body.appendChild(clone);
        }
        const sc = b.height / a.height;
        // (the tile is in the picture's place until the clone has it: it settles back as the clone leaves)
        const dx = b.left + b.width / 2 - (a.left + a.width / 2);
        const dy = b.top + b.height / 2 - (a.top + a.height / 2);
        const dur = Math.round(640 * kt);
        const o = { duration: dur, fill: 'forwards' as const };
        clone.animate([{ translate: '0px 0px' }, { translate: `${dx}px 0px` }], { ...o, easing: GLIDE });
        pic.animate([{ translate: '0px 0px', scale: String(k0) }, { translate: `0px ${dy}px`, scale: String(sc) }], { ...o, easing: 'cubic-bezier(0.33, 0.04, 0.16, 1)' });
        let done = false;
        const land = () => {
          if (done) return;
          done = true;
          window.clearTimeout(t);
          clone.remove();
          show();
          scanFx.current.delete(undo);
        };
        const t = window.setTimeout(land, dur);
        const undo = () => {
          done = true;
          window.clearTimeout(t);
          clone.remove();
        };
        scanFx.current.add(undo);
        landBy = Math.max(landBy, performance.now() + dur);
      };
      // the lens has gone ahead of what it caught: the tile settles in place, its picture goes to the box
      const letGo = () => {
        // (the lens keeps its crimson ring, and never its dark frame, for as long as it holds a catch)
        lens.classList.remove('is-hit', 'is-pluck');
        pending.forEach((pid) => markHit(pid));
        lens.getAnimations().forEach((an) => {
          if (an.id === 'pluck') an.cancel();
        });
        while (pending.length) {
          const id = pending.shift()!;
          const el = els.current.get(id);
          if (!el) continue;
          el.getAnimations().forEach((an) => {
            if (an.id === 'pluck') an.cancel();
          });
          fly(id);
        }
      };
      const hop = () => {
        if (run.stop) return;
        letGo();
        if (k >= o.order.length) {
          // (the lens goes, then the scan is done. The end is on a timer, not on the fade's own finish: a browser that
          // is not drawing, a tab in the background, never reports an animation finished, and the scan would hang at
          // its last post)
          lens.animate([{ opacity: 1 }, { opacity: 0 }], { duration: 200, fill: 'forwards' });
          // (a picture still in the air lands first)
          const late = Math.max(0, landBy - performance.now() + 40);
          run.timer = window.setTimeout(() => {
            if (run.stop) return;
            lens.style.visibility = 'hidden';
            lens.getAnimations().forEach((a) => a.cancel());
            scanRun.current = null;
            inner.classList.remove('is-scanning');
            o.onDone();
          }, 220 + late);
          return;
        }
        const id = o.order[k];
        k += 1;
        const r = at.current.get(id);
        const el = els.current.get(id);
        if (!r || !el) {
          run.timer = window.setTimeout(hop, 0);
          return;
        }
        const hit = o.hits.has(id);
        const ahead = o.hits.has(o.order[k] ?? '') || o.hits.has(o.order[k + 1] ?? '');
        per = hit ? 90 : ahead ? Math.min(110, per + 30) : Math.max(42, per * 0.86);
        const pad = Math.round(r.w * 0.08);
        const L = { x: r.x - pad, y: r.y - pad };
        lens.style.width = `${r.w + pad * 2}px`;
        lens.style.height = `${r.h + pad * 2}px`;
        lens.style.transform = T(L);
        if (prev) lens.animate([{ transform: T(prev) }, { transform: T(L) }], { duration: Math.round(per * 0.7), easing: 'cubic-bezier(0.3, 0.6, 0.3, 1)' });
        else lens.animate([{ opacity: 0, transform: `${T(L)} scale(1.2)` }, { opacity: 1, transform: `${T(L)} scale(1)` }], { duration: 260, easing: ARRIVE });
        prev = L;
        // the camera: the wall rises so the lens's row sits in the window's upper middle
        const want = Math.max(0, Math.min(Math.max(0, Lh - winH), r.y + r.h / 2 - winH * 0.42));
        if (Math.abs(want - scanT.current) > 1) {
          const from = scanT.current;
          scanT.current = want;
          inner.style.transform = `translate3d(0, ${-want}px, 0)`;
          extra.current.push(inner.animate([{ transform: `translate3d(0, ${-from}px, 0)` }, { transform: `translate3d(0, ${-want}px, 0)` }], { duration: 420, easing: 'cubic-bezier(0.25, 0.1, 0.25, 1)' }));
        }
        let wait = per;
        if (hit) {
          caught += 1;
          // (one red border while it plucks: the lifted picture's own. The lens steps out of sight over it, and the post
          // takes its ring once the lens has gone past. With reduced motion there is no lift: the post and lens ring at once)
          if (still) {
            markHit(id);
            lens.classList.add('is-hit');
          } else {
            lens.classList.add('is-pluck');
            lift(id);
          }
          pending.push(id);
          o.onCatch?.(id);
          // (the lens holds on a catch for a second: it is seen, then the scan goes on)
          wait = Math.round(1000 * kt);
        }
        o.onHop(k, o.order.length, caught);
        run.timer = window.setTimeout(hop, wait);
      };
      run.timer = window.setTimeout(hop, 120);
    },
    [stopScan, unpick, phone],
  );
  // after a scan the camera's offset is folded into the next re-form (the snap reads it with the inner box)
  const reformAfterScan = useCallback(
    (s: Snap, groups: FeedGroup[], o: ReformOpts) => {
      const inner = innerRef.current;
      if (inner && scanT.current) {
        scanT.current = 0;
        inner.style.transform = '';
      }
      clearExtra();
      reform(s, groups, o);
    },
    [reform],
  );

  /* ── a tap on a post: its trail. The post comes forward first, in ink, over everything; then the posts it beat come
     forward one at a time going back in time, each on its own shadow, ringed crimson and numbered (when it did worse,
     the ones before it that did better, in ink); last, the post that stopped the run comes forward in ink with a tag
     saying so (or, when nothing stopped it, the oldest one says that). An even beat, the whole run inside about 1.5s
     however long it is. The rest of the wall steps right back and stops answering; the glance (PostTray) counts the run
     as it plays. The same post tapped again opens it ── */
  /* the top of what can be seen over the wall once the page has moved: under the file row while it is still pinned there
     (a laptop: it is sticky to the page's own box, so far enough down it scrolls away), else under the status bar. On a
     phone the row scrolls with the page (the page scrolls on the root, where a sticky row does not hold), and its own
     sticky top is the safe area */
  const recentre = useCallback((id: string, still: boolean) => {
    const el = els.current.get(id);
    if (!el) return false;
    // (a set standing whole on the screen, as a file fitted to it does: the trail is all in view already, so the page
    // stays put; a move would only carry the wall off its place beside the reading, or under the docked row)
    const whole = innerRef.current?.getBoundingClientRect();
    if (whole && whole.height && whole.top >= -1 && whole.bottom <= window.innerHeight + 1) return false;
    const rail = document.querySelector<HTMLElement>('.rn-rail');
    const rs = rail ? getComputedStyle(rail) : null;
    const sticky = rs?.position === 'sticky';
    const pins = sticky && !phone;
    const stickTop = sticky ? parseFloat(rs!.top) || 0 : 0;
    const railH = pins ? rail!.getBoundingClientRect().height : 0;
    const box = pins ? rail!.parentElement!.getBoundingClientRect() : null;
    const top = el.getBoundingClientRect().top;
    const room = document.documentElement.scrollHeight - window.innerHeight - window.scrollY;
    const clamp = (v: number) => Math.max(-window.scrollY, Math.min(room, v));
    // (where the row would sit after a move of `by`: pinned at its top, or carried off with the end of its box)
    const bandAfter = (by: number) => {
      if (!box) return stickTop + 12;
      const railTop = Math.min(stickTop, box.bottom - by - railH);
      return Math.max(stickTop, railTop + railH) + 12;
    };
    let band = bandAfter(clamp(top - (stickTop + railH + 12)));
    band = bandAfter(clamp(top - band));
    // (already in the top band: stay)
    if (top >= band - 8 && top <= band + 48) return false;
    const by = clamp(top - band);
    if (Math.abs(by) < 8) return false;
    window.scrollBy({ top: by, behavior: still ? 'auto' : 'smooth' });
    return true;
  }, [phone]);
  const pick = useCallback(
    (id: string | null) => {
      const inner = innerRef.current;
      if (!inner) return;
      const was = picked.current;
      // (the same post again: the glance opens into the post itself, and a tap after that folds it back)
      if (id && id === was && !scanRun.current) {
        pickedPost.setOpen(!pickedPost.isOpen());
        return;
      }
      if (!id || scanRun.current) {
        unpick();
        return;
      }
      unpick();
      const r = at.current.get(id);
      const p = byId.get(id);
      if (!r || !p) return;
      picked.current = id;
      const pool = [...shown.current].map((x) => byId.get(x)!).filter(Boolean).sort((a, b) => (a.date + a.time).localeCompare(b.date + b.time));
      const tr = trailOf(id, pool);

      inner.classList.add('is-trail');
      const still = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      const k = tempo();
      /* 1. the page glides so the picked post sits at the top of what can be seen (under the pinned file row, or the
         status bar), so as much of the trail below it as possible is on screen. Only when that helps: a post already in
         that band stays where it is */
      const moved = recentre(id, still);
      /* one post forward, as one move: its lift, its ring, its number and (a beaten post) a wash of colour over its
         picture all start together. Its scale is about its own middle (its place is its transform, so the origin is
         moved there), and a plate under it carries its shadow */
      const forward = (q: string, cls: string[], plate: string, wash: 'hit' | 'ink' | null) => {
        const el = els.current.get(q);
        const rr = at.current.get(q);
        if (!el || !rr) return null;
        el.style.transformOrigin = `${rr.x + rr.w / 2}px ${rr.y + rr.h / 2}px`;
        const pl = document.createElement('i');
        pl.className = `rn-lift ${plate}`;
        pl.setAttribute('aria-hidden', 'true');
        pl.style.cssText = `left:${rr.x}px;top:${rr.y}px;width:${rr.w}px;height:${rr.h}px`;
        inner.appendChild(pl);
        plates.current.get(q)?.remove();
        plates.current.set(q, pl);
        pl.getBoundingClientRect();
        pl.classList.add('is-on');
        el.classList.add(...cls);
        if (wash && !still) {
          // (the hit: on at once, settling away slowly as the ring lands; a child of the tile, so it is clipped to it)
          const w = document.createElement('i');
          w.className = `rn-wash${wash === 'ink' ? ' is-ink' : ''}`;
          w.setAttribute('aria-hidden', 'true');
          el.appendChild(w);
          w.animate([{ opacity: 1 }, { opacity: 0 }], { duration: Math.round(720 * k), easing: 'cubic-bezier(0.2, 0, 0.2, 1)', fill: 'forwards' }).onfinish = () => w.remove();
        }
        return el;
      };
      forward(id, ['is-sel'], 'is-sel', null);
      // 2. the rest of the wall recedes; the posts on the trail and its end point never dim
      const lit = new Set([id, ...tr.chain, ...(tr.end ? [tr.end] : [])]);
      shown.current.forEach((x) => {
        if (lit.has(x)) return;
        const el = els.current.get(x);
        if (el) dimmed.current.set(x, el.animate([{ opacity: 1 }, { opacity: TRAIL_DIM }], { duration: still ? 0 : Math.round(400 * k), easing: EVEN, fill: 'forwards' }));
      });
      /* 3. the run: one wave going back in time, set off on an even beat once the glide is nearly over, every post's
         own move long enough to overlap the next. A long run tightens the beat to fit the cap, never below its floor */
      const n = tr.chain.length;
      const steps = n + (tr.end ? 1 : 0);
      const beat = steps ? Math.max(TRAIL_BEAT_MIN, Math.min(TRAIL_BEAT, TRAIL_RUN / steps)) : 0;
      // (a run too long for the cap even at the floor: the posts past it, far down the page, come forward together)
      const at0 = moved ? 460 : 260;
      const last = beat ? Math.ceil(TRAIL_RUN / beat) : 0;
      const when = (i: number) => at0 + Math.min(i, last) * beat;
      pickedPost.set(id, tr.text, { up: tr.up, n, beat });
      const later = (ms: number, f: () => void) => {
        if (still) f();
        else trailTimers.current.push(window.setTimeout(f, ms));
      };
      tr.chain.forEach((q, i) => {
        later(when(i), () => {
          const el = els.current.get(q);
          const ring = el?.querySelector<HTMLElement>('.rn-t__ring');
          if (ring) {
            ring.dataset.n = String(i + 1);
            // (nothing stopped the run: the oldest post here says so)
            if (i === n - 1 && !tr.end) ring.dataset.tag = 'The oldest';
          }
          forward(q, [tr.up ? 'is-beat' : 'is-above', ...(i === n - 1 && !tr.end ? ['is-end'] : [])], 'is-run', tr.up ? 'hit' : 'ink');
          pickedPost.setStep(i + 1);
        });
      });
      // 4. the end point lands last, then the glance's line settles (and a light tap where the phone can give one)
      const endAt = when(n);
      if (tr.end) {
        const end = tr.end;
        later(endAt, () => {
          const ring = els.current.get(end)?.querySelector<HTMLElement>('.rn-t__ring');
          if (ring) ring.dataset.tag = tr.up ? 'Stopped it' : 'Did worse';
          forward(end, ['is-stop', 'is-end'], 'is-run', null);
        });
      }
      if (n) {
        later((tr.end ? endAt : when(n - 1)) + 220, () => {
          pickedPost.setStep(n + 1);
          if (!still) playHaptic('pick');
        });
      }
    },
    [byId, unpick, recentre],
  );

  useLayoutEffect(() => {
    apiRef.current = { snap, where, reform: reformAfterScan, carry, scan, stopScan, window: setWindow, release, pick };
  }, [apiRef, snap, where, reformAfterScan, carry, scan, stopScan, setWindow, release, pick]);
  useEffect(() => () => stopScan(), [stopScan]);

  return (
    <section className="rn-feed" aria-label="His posts">
      <div className="rn-feed__line">
        <p className="rn-feed__what">
          <b>{lead}</b>
          <span> · {line}</span>
        </p>
        {showAll ? (
          <button type="button" className="rn-feed__all" onClick={onShowAll}>
            Show all
          </button>
        ) : null}
        {scanning ? null : (
          <div className="rn-sw" role="group" aria-label="Lay the posts out">
            <button type="button" aria-pressed={view === 'recent'} onClick={() => onView('recent')}>
              {orderLabel}
            </button>
            <button type="button" aria-pressed={view === 'ranks'} onClick={() => onView('ranks')}>
              Ranks
            </button>
          </div>
        )}
      </div>
      <div className="rn-feed__box" ref={boxRef}>
        <div
          className="rn-feed__in"
          ref={innerRef}
          onClick={(e) => {
            const t = (e.target as HTMLElement).closest<HTMLElement>('.rn-t');
            // (while a post is picked, the wall answers one way: the post opens, any other tap lets it go)
            if (picked.current) pick(t?.dataset.id === picked.current ? picked.current : null);
            else pick(t ? t.dataset.id! : null);
          }}
        >
          {posts.map((p) => (
            <Tile key={p.id} post={p} />
          ))}
          <div className="rn-lens" ref={lensRef} aria-hidden="true" />
        </div>
      </div>
    </section>
  );
}

export default memo(Feed);
