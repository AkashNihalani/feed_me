/* ─────────────────────────────────────────────────────────────
   DAY WIDE — the Day view on a wide window, as one picture and
   its words (the desktop try in .claude/read_scratch/day-wide,
   approved over six rounds, brought into the app).

   The post's picture stands off the window's top, right and foot
   (4:5 at the window's height; narrower windows crop its sides
   rather than squeeze the words). Its words stand in one block
   in the middle of what is left, the same place for every post.
   The page goes with the trackpad 1:1 and lets go to a post (one
   a swipe; a mouse's notch or a key is one post); the next post
   comes up out of its own colours (its head's, over the join),
   while the words go with the scroll: the date's figures and the
   rank roll like an odometer, the rest leaves and comes. A
   carousel's slides go sideways the same way. A reel plays its
   preview once the page rests on it.

   Plain DOM, written every frame from one position (pos), so the
   motion never waits on React: DayWide (the component) gives it
   the posts and flies it in and out. Transforms and opacity only.
   ───────────────────────────────────────────────────────────── */

import { formatCount, formatMultiple, formatTopPercent, isWinner } from '@/lib/feedStream/contract';

export type WideRead = { cp: number; top: number | null; likes: number | null; com: number | null; er: number | null; mul: number | null };
export type WidePost = {
  key: string;
  handle: string;
  // the feeder's picture (null: their initial)
  face: string | null;
  // what it is ("Reel", "Carousel", "Post")
  kind: string;
  // the IST day it went up (YYYY-MM-DD)
  day: string;
  // its ranked reads, oldest first (D1 → D21); none yet: still measuring
  reads: WideRead[];
  // measuring: when its first read lands ("First read in 4h")
  wait: string | null;
  caption: string;
  url: string | null;
  // its slides' pictures (the cover first); one for a post that isn't a carousel
  slides: string[];
  // a reel's preview clip, when stored
  preview: string | null;
};

export type WideOptions = {
  // its face or name tapped (null: not offered, a feeder's own page)
  onWho: ((handle: string) => void) | null;
  // the post the page rests on now
  onRest?: (key: string) => void;
  formatRate: (value: number | null) => string;
};

const CP = ['D1', 'D3', 'D7', 'D21'];
const clamp = (v: number, a = 0, b = 1) => Math.min(b, Math.max(a, v));
const ss = (a: number, b: number, v: number) => {
  const t = clamp((v - a) / (b - a));
  return t * t * (3 - 2 * t);
};
const mix = (a: number, b: number, t: number) => a + (b - a) * t;
const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
const reduced = () => typeof window !== 'undefined' && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
const WK = new Intl.DateTimeFormat('en-GB', { weekday: 'long', timeZone: 'UTC' });
const MON = new Intl.DateTimeFormat('en-GB', { month: 'long', timeZone: 'UTC' });
const dayOf = (d: string) => new Date(`${d}T12:00:00Z`);
const svg = (d: string, s = 20, w = 2.4) => `<svg width="${s}" height="${s}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="${w}" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${d}</svg>`;

// the words' natural size (px at a scale of 1): what they need of the window's height, and of its width
const BLOCK_W = 520;
const BLOCK_H = 640;
// the words keep at least this much of the window beside the picture (px); less, and the picture is cropped at its sides
const WORDS_MIN = 400;
// a spring that lets go of a post (stiffness's root): the try's
const W = 9;
// the mist over the join: of the picture's height, over its head and under it
const MIST_UP = 0.27;
const MIST_DOWN = 0.25;

type Pic = {
  key: string;
  el: HTMLElement;
  imgs: HTMLImageElement[];
  up: HTMLCanvasElement;
  down: HTMLCanvasElement;
  sink: HTMLElement;
  track: HTMLElement | null;
  dots: HTMLElement[];
  count: HTMLElement | null;
  video: HTMLVideoElement | null;
  tint: string | null;
  n: number;
  k: number;
  sx: number;
  shown: number;
};

type Slot = {
  set: (i: number, j: number, p: number, at?: number, reach?: number) => void;
  swap: (ha: string, hb: string, p: number, at?: number, reach?: number, back?: boolean) => void;
  reset: () => void;
};

type Wheel = { on: boolean; tail: boolean; base: number; n: number; peak: number; prev: number; fall: number; at: number; v: number; done: number; low: number };
const freshWheel = (): Wheel => ({ on: false, tail: false, base: 0, n: 0, peak: 0, prev: 0, fall: 0, at: 0, v: 0, done: 0, low: 0 });

// a cubic-bezier as a function (the carry curve: picks)
function bez(x1: number, y1: number, x2: number, y2: number) {
  return (x: number) => {
    let s = x;
    for (let k = 0; k < 8; k += 1) {
      const cx = 3 * (1 - s) * (1 - s) * s * x1 + 3 * (1 - s) * s * s * x2 + s * s * s - x;
      const d = 3 * (1 - s) * (1 - s) * x1 + 6 * (1 - s) * s * (x2 - x1) + 3 * s * s * (1 - x2);
      if (Math.abs(cx) < 1e-5 || !d) break;
      s = clamp(s - cx / d);
    }
    return 3 * (1 - s) * (1 - s) * s * y1 + 3 * (1 - s) * s * s * y2 + s * s * s;
  };
}
const carry = bez(0.3, 0.3, 0.2, 1);

const TEMPLATE = `
<div class="dw-grounds"><div class="dw-ground"></div><div class="dw-ground"></div></div>
<div class="dw-pics"></div>
<div class="dw-read"><div class="dw-block">
  <div class="dw-date"><span class="dw-dnum"><span class="dw-dcol"></span><span class="dw-dcol"></span></span><span class="dw-dmon"></span><span class="dw-dwk">&#8203;<span></span><span></span></span></div>
  <div class="dw-slot dw-who"></div>
  <div class="dw-lead">
    <div class="dw-num"><span class="dw-top">Top</span><span class="dw-cols"><span class="dw-col"></span><span class="dw-col"></span><span class="dw-col"></span></span><span class="dw-pct">%</span></div>
    <div class="dw-slot dw-wait"></div>
  </div>
  <div class="dw-strip" role="radiogroup" aria-label="Reads"><em></em>${CP.map((c, k) => `<button type="button" data-cp="${k}" tabindex="-1">${c}<u>${c}</u></button>`).join('')}</div>
  <div class="dw-figs"><div><div class="dw-slot"></div><small>likes</small></div><div><div class="dw-slot"></div><small>comments</small></div><div><div class="dw-slot"></div><small>engagement</small></div><div><div class="dw-slot"></div><small>vs usual</small></div></div>
  <div class="dw-slot dw-cap"></div>
  <div class="dw-igw"><a class="dw-ig" target="_blank" rel="noopener noreferrer">Open on Instagram${svg('<path d="M7 7h10v10"/><path d="M7 17 17 7"/>', 16)}</a></div>
</div></div>`;

export class WideDay {
  readonly root: HTMLElement;
  private opts: WideOptions;
  private P: WidePost[] = [];
  private N = 0;
  private pics = new Map<string, Pic>();
  private slideAt = new Map<string, number>();
  private pickAt = new Map<string, number>();
  // geometry
  private H = 800;
  private Wd = 1280;
  private L = 124;
  private picW = 640;
  private s = 1;
  private gw: Record<string, number> = {};
  private dw: Record<string, number> = {};
  // where the page is (px: a post a window's height)
  private pos = 0;
  private last = -1;
  private lastOver = 0;
  private gi = -1;
  private spring: { v: number; to: number; at: number } | null = null;
  private raf = 0;
  private g: Wheel = freshWheel();
  private quiet = 0;
  private sg: Wheel & { key: string } = { ...freshWheel(), key: '' };
  private squiet = 0;
  private sspring: { x: Pic; to: number; v: number; at: number } | null = null;
  private sraf = 0;
  private axis: 'x' | 'y' = 'y';
  private axisAt = 0;
  private tw: { i: number; from: Read; to: Read; back: boolean; t0: number; dur: number } | null = null;
  private twRaf = 0;
  private restKey: string | null = null;
  private restTimer = 0;
  private igAt = -1;
  // a finger on it (a tablet as wide as a laptop): where it landed, and how fast it goes
  private t0: { y: number; p: number; base: number; v: number; at: number; ly: number } | null = null;
  // a move in or out: the page holds still under it, and takes no input
  busy = false;
  private disposed = false;
  // parts
  private $ = <T extends HTMLElement = HTMLElement>(s: string) => this.root.querySelector<T>(s) as T;
  private grounds: HTMLElement[];
  private picsEl: HTMLElement;
  private numEl: HTMLElement;
  private cols: { el: HTMLElement; a: HTMLElement; b: HTMLElement }[];
  private dcols: { el: HTMLElement; a: HTMLElement; b: HTMLElement }[];
  private wa: HTMLElement;
  private wb: HTMLElement;
  private stripEl: HTMLElement;
  private glide: HTMLElement;
  private segs: HTMLElement[];
  private figsEl: HTMLElement;
  private ig: HTMLAnchorElement;
  private slots: { who: Slot; wait: Slot; cap: Slot; figs: Slot[] };

  constructor(root: HTMLElement, opts: WideOptions) {
    this.root = root;
    this.opts = opts;
    root.classList.add('dw');
    root.innerHTML = TEMPLATE;
    this.grounds = [...root.querySelectorAll<HTMLElement>('.dw-ground')];
    this.picsEl = this.$('.dw-pics');
    this.numEl = this.$('.dw-num');
    const pair = (el: HTMLElement) => {
      const a = document.createElement('span');
      const b = document.createElement('span');
      el.append(a, b);
      return { el, a, b };
    };
    this.cols = [...root.querySelectorAll<HTMLElement>('.dw-col')].map(pair);
    this.dcols = [...root.querySelectorAll<HTMLElement>('.dw-dcol')].map(pair);
    [this.wa, this.wb] = [...root.querySelectorAll<HTMLElement>('.dw-dwk span')];
    this.stripEl = this.$('.dw-strip');
    this.glide = this.$('.dw-strip em');
    this.segs = [...root.querySelectorAll<HTMLElement>('.dw-strip button')];
    this.figsEl = this.$('.dw-figs');
    this.ig = this.$<HTMLAnchorElement>('.dw-ig');
    this.slots = {
      who: this.slot(this.$('.dw-who'), (i) => this.whoHtml(i)),
      wait: this.slot(this.$('.dw-wait'), (i) => this.waitHtml(i)),
      cap: this.slot(this.$('.dw-cap'), (i) => (this.P[i].caption ? `<p>${esc(this.P[i].caption)}</p>` : '')),
      figs: [...root.querySelectorAll<HTMLElement>('.dw-figs .dw-slot')].map((el, k) => this.slot(el, (i) => this.figHtml(this.has(i) ? this.R(i) : null, k))),
    };
    root.addEventListener('wheel', this.onWheel, { passive: false });
    root.addEventListener('click', this.onClick);
    root.addEventListener('touchstart', this.onTouchStart, { passive: true });
    root.addEventListener('touchmove', this.onTouchMove, { passive: false });
    root.addEventListener('touchend', this.onTouchEnd);
    root.addEventListener('touchcancel', this.onTouchEnd);
    window.addEventListener('keydown', this.onKey, true);
  }

  /* ── the posts ── */

  // the posts, in the Day's order; the page stays on the post it was on (or goes to `focus`)
  setPosts(posts: WidePost[], focus?: string | null) {
    const keep = focus ?? this.keyAt(this.here());
    this.P = posts;
    this.N = posts.length;
    const live = new Set(posts.map((post) => post.key));
    this.pics.forEach((x, key) => {
      if (!live.has(key)) {
        x.el.remove();
        this.pics.delete(key);
      }
    });
    const at = keep ? posts.findIndex((post) => post.key === keep) : -1;
    if (at >= 0 && (focus || this.here() !== at)) {
      this.spring = null;
      this.pos = at * this.H;
    }
    this.pos = clamp(this.pos, 0, Math.max(0, this.N - 1) * this.H);
    this.slots.who.reset();
    this.slots.wait.reset();
    this.slots.cap.reset();
    this.slots.figs.forEach((f) => f.reset());
    this.gi = -1;
    this.draw(true);
  }

  keyAt(i: number): string | null {
    return this.P[i]?.key ?? null;
  }
  get index() {
    return this.here();
  }
  get count() {
    return this.N;
  }

  /* ── geometry: the window it stands in, the column's left (past the side rail) ── */

  resize(width: number, height: number, left: number) {
    const k = this.here();
    this.Wd = width;
    this.H = height;
    this.L = left;
    // the picture: 4:5 at the window's height, its sides cropped before the words get less than they need
    this.picW = Math.round(clamp(width - left - WORDS_MIN, height * 0.62, height * 0.8));
    const words = width - this.picW - left;
    // the words grow with the room beside the picture, never past the window's height
    this.s = Math.min(clamp(words / 600, 0.7, 1.32), (height - 72) / BLOCK_H);
    const r = this.root.style;
    r.setProperty('--dw-h', `${height}px`);
    r.setProperty('--dw-pic', `${this.picW}px`);
    r.setProperty('--dw-left', `${left}px`);
    r.setProperty('--dw-s', this.s.toFixed(3));
    r.setProperty('--dw-block', `${Math.round(Math.min(BLOCK_W * this.s, words - 48))}px`);
    this.measure();
    this.spring = null;
    this.pos = k * height;
    this.pics.forEach((x) => {
      if (x.track) {
        x.sx = x.k * this.picW;
        this.slideDraw(x);
      }
    });
    this.gi = -1;
    this.draw(true);
  }

  /* The post in view's picture (its frame, the slide in view in it), for a move in or out to carry (DayMove), and the
     box the pictures stand in, let draw past its edges while one flies (a tile can be anywhere on the window) */
  pictureEl(): HTMLElement | null {
    const post = this.P[this.here()];
    return (post && this.pics.get(post.key)?.el) || null;
  }
  flying(on: boolean) {
    this.picsEl.style.overflow = on ? 'visible' : '';
  }

  private measure() {
    const m = document.createElement('span');
    m.className = 'dw-measure';
    this.root.append(m);
    const num = getComputedStyle(this.$('.dw-col'));
    m.style.font = `700 ${num.fontSize} ${num.fontFamily}`;
    m.style.letterSpacing = num.letterSpacing;
    m.style.fontVariantNumeric = 'tabular-nums';
    for (const g of '0123456789') {
      m.textContent = g;
      this.gw[g] = m.getBoundingClientRect().width;
    }
    const date = getComputedStyle(this.$('.dw-date'));
    m.style.font = `700 ${date.fontSize} ${date.fontFamily}`;
    m.style.letterSpacing = date.letterSpacing;
    m.style.fontVariantNumeric = 'normal';
    for (const g of '0123456789') {
      m.textContent = g;
      this.dw[g] = m.getBoundingClientRect().width;
    }
    m.remove();
  }

  /* ── a post's words ── */

  private has(i: number) {
    return i >= 0 && i < this.N && this.P[i].reads.length > 0 && this.P[i].reads[this.P[i].reads.length - 1].top != null;
  }
  // the read a post shows: its latest, or the one picked on its strip
  private R(i: number): Read {
    const post = this.P[i];
    const last = post.reads.length - 1;
    const k = this.pickAt.get(post.key);
    const r = post.reads[k == null ? last : k];
    return r ? { ...r } : { cp: -1, top: null, likes: null, com: null, er: null, mul: null };
  }
  private whoHtml(i: number) {
    const post = this.P[i];
    const face = post.face
      ? `<img src="${esc(post.face)}" alt="" onerror="this.style.visibility='hidden'">`
      : `<i>${esc(post.handle.slice(0, 1).toUpperCase())}</i>`;
    const inner = `<span class="dw-face">${face}</span><span class="dw-name"><b>@${esc(post.handle)}</b><span>${esc(post.kind)}</span></span>`;
    return this.opts.onWho ? `<button type="button" class="dw-whob" data-who="${esc(post.handle)}" aria-label="Open @${esc(post.handle)}'s page">${inner}</button>` : `<span class="dw-whob">${inner}</span>`;
  }
  private waitHtml(i: number) {
    if (this.has(i)) return '';
    const post = this.P[i];
    return `<span class="dw-waiting"><i aria-hidden="true"></i><span><b>Measuring</b><small>${esc(post.wait ?? 'First read due soon')}</small></span></span>`;
  }
  private figHtml(r: Read | null, k: number) {
    if (!r) return '';
    const values = [formatCount(r.likes), formatCount(r.com), this.opts.formatRate(r.er), formatMultiple(r.mul)];
    return `<b>${values[k]}</b>`;
  }

  /* a slot per line, two items in each (the post's and the next one's): the one goes up and out as the page goes on,
     the next comes up into its place, unless they say the same */
  private slot(el: HTMLElement, html: (i: number) => string): Slot {
    const a = document.createElement('div');
    const b = document.createElement('div');
    a.className = b.className = 'dw-it';
    el.append(a, b);
    const st = { a, b, ai: -1, bi: -1, same: false, ha: null as string | null, hb: null as string | null };
    const put = (n: HTMLElement, i: number) => {
      n.innerHTML = i < this.N ? html(i) : '';
      st.ha = st.hb = null;
      return i;
    };
    const pose = (n: HTMLElement, o: number, y: number) => {
      n.style.opacity = String(o);
      n.style.visibility = o > 0 ? 'visible' : 'hidden';
      n.style.transform = `translate3d(0,${y}px,0)`;
    };
    return {
      set: (i, j, p, at = 0, reach = 26) => {
        if (st.ai !== i || st.bi !== j) {
          if (st.ai !== i) {
            if (st.bi === i) {
              [st.a, st.b] = [st.b, st.a];
              [st.ai, st.bi] = [st.bi, st.ai];
            } else st.ai = put(st.a, i);
          }
          if (st.bi !== j) st.bi = put(st.b, j);
          st.same = j >= this.N || html(i) === html(j);
        }
        const out = st.same ? 0 : ss(0.06 + at, 0.36 + at, p);
        const come = st.same ? 0 : ss(0.46 + at, 0.82 + at, p);
        pose(st.a, 1 - out, -reach * this.s * out);
        pose(st.b, come, reach * this.s * (1 - come));
      },
      swap: (ha, hb, p, at = 0, reach = 26, back = false) => {
        if (st.ha !== ha || st.hb !== hb) {
          st.a.innerHTML = ha;
          st.b.innerHTML = hb;
          st.ha = ha;
          st.hb = hb;
          st.ai = st.bi = -1;
        }
        const same = ha === hb;
        const w = back ? -1 : 1;
        const out = same ? 0 : ss(0.06 + at, 0.36 + at, p);
        const come = same ? 0 : ss(0.46 + at, 0.82 + at, p);
        pose(st.a, same ? 1 : 1 - out, -reach * this.s * out * w);
        pose(st.b, come, reach * this.s * (1 - come) * w);
      },
      reset: () => {
        st.ai = st.bi = -1;
        st.ha = st.hb = null;
      },
    };
  }

  /* ── pictures, made as they come near: each with its mist (its head's own colours, over the join) ── */

  private pic(i: number): Pic {
    const post = this.P[i];
    const had = this.pics.get(post.key);
    if (had) {
      had.el.style.zIndex = String(i + 1);
      return had;
    }
    const el = document.createElement('div');
    el.className = 'dw-pic';
    el.style.zIndex = String(i + 1);
    const many = post.slides.length > 1;
    el.innerHTML =
      (many
        ? `<div class="dw-track">${post.slides.map((src) => `<img alt="" src="${esc(src)}" decoding="async" draggable="false">`).join('')}</div>
           <span class="dw-cnt"></span><span class="dw-dots">${post.slides.map(() => '<i></i>').join('')}</span>
           <button type="button" class="dw-arw dw-prev" aria-label="Previous slide">${svg('<path d="m15 18-6-6 6-6"/>')}</button>
           <button type="button" class="dw-arw dw-next" aria-label="Next slide">${svg('<path d="m9 18 6-6-6-6"/>')}</button>`
        : `<img alt="" src="${esc(post.slides[0] ?? '')}" decoding="async" draggable="false">`) +
      '<span class="dw-sink"></span><canvas class="dw-mist"></canvas><canvas class="dw-mist"></canvas>';
    this.picsEl.append(el);
    const [up, down] = [...el.querySelectorAll('canvas')];
    const x: Pic = {
      key: post.key,
      el,
      imgs: [...el.querySelectorAll('img')],
      up,
      down,
      sink: el.querySelector('.dw-sink') as HTMLElement,
      track: el.querySelector('.dw-track'),
      dots: [...el.querySelectorAll<HTMLElement>('.dw-dots i')],
      count: el.querySelector('.dw-cnt'),
      video: null,
      tint: null,
      n: post.slides.length || 1,
      k: 0,
      sx: 0,
      shown: -1,
    };
    up.style.transform = down.style.transform = 'scaleY(0)';
    this.pics.set(post.key, x);
    if (x.track) {
      x.k = this.slideAt.get(post.key) ?? 0;
      x.sx = x.k * this.picW;
      this.slideDraw(x);
    }
    const face = x.imgs[x.k] ?? x.imgs[0];
    if (face) {
      face.decode().then(() => {
        if (this.disposed || this.pics.get(post.key) !== x) return;
        this.colours(x, face);
        const i2 = this.P.findIndex((p) => p.key === post.key);
        if (this.gi === i2 || this.gi + 1 === i2) {
          this.gi = -1;
          this.draw(true);
        }
      }, () => {});
    }
    return x;
  }

  /* its colours: the mist over its head (its top band, stretched and faded) and its tint for the page. The pictures come
     from the media store, another origin, so they are drawn, never read, wherever that can be helped; the tint alone
     needs a pixel, and goes without when the picture can't give one */
  private colours(x: Pic, im: HTMLImageElement) {
    const w = im.naturalWidth;
    const h = im.naturalHeight;
    if (!w || !h) return;
    // what the 4:5 frame shows of it
    const sw = Math.min(w, h * 0.8);
    const sh = Math.min(h, w / 0.8);
    const sx = (w - sw) / 2;
    const sy = (h - sh) / 2;
    // the head's colours, left to right: its top band, averaged down to a few across
    const band = document.createElement('canvas');
    band.width = 6;
    band.height = 1;
    const bc = band.getContext('2d');
    if (!bc) return;
    bc.imageSmoothingQuality = 'high';
    bc.drawImage(im, sx, sy, sw, sh * 0.1, 0, 0, 6, 1);
    const sm = (t: number) => t * t * t * (t * (6 * t - 15) + 10);
    for (const [c, reach, whole] of [[x.up, MIST_UP, true], [x.down, MIST_DOWN, false]] as const) {
      c.width = 64;
      c.height = 96;
      const cx = c.getContext('2d');
      if (!cx) continue;
      cx.imageSmoothingEnabled = true;
      cx.imageSmoothingQuality = 'high';
      cx.drawImage(band, 0, 0, 6, 1, 0, 0, 64, 96);
      cx.globalCompositeOperation = 'destination-in';
      const gy = cx.createLinearGradient(0, 0, 0, 96);
      for (let k = 0; k <= 20; k += 1) gy.addColorStop(k / 20, `rgba(0,0,0,${(whole ? sm(k / 20) : 1 - sm(k / 20)).toFixed(3)})`);
      cx.fillStyle = gy;
      cx.fillRect(0, 0, 64, 96);
      c.style.cssText = (whole ? `bottom:calc(100% - 1px);height:${reach * 100}%;transform-origin:50% 100%` : `top:0;height:${reach * 100}%;transform-origin:50% 0`) + ';transform:scaleY(0)';
    }
    // the page's tint: its left half, as one colour
    try {
      const one = document.createElement('canvas');
      one.width = one.height = 1;
      const oc = one.getContext('2d', { willReadFrequently: true });
      if (oc) {
        oc.drawImage(im, sx, sy, sw * 0.5, sh, 0, 0, 1, 1);
        const d = oc.getImageData(0, 0, 1, 1).data;
        x.tint = `rgb(${d[0]} ${d[1]} ${d[2]})`;
      }
    } catch {
      x.tint = null;
    }
  }

  private groundOf(i: number) {
    const post = this.P[i];
    const t = post ? this.pics.get(post.key)?.tint : null;
    const dark = document.documentElement.classList.contains('dark');
    if (!t) return dark ? 'var(--fm-page)' : 'var(--fm-page)';
    return dark
      ? `linear-gradient(to right, var(--fm-page) ${this.L}px, oklch(from ${t} .2 min(c, .045) h) 60%)`
      : `linear-gradient(to right, var(--fm-page) ${this.L}px, oklch(from ${t} .95 min(calc(c * .5), .03) h) 60%)`;
  }

  /* ── one frame ── */

  private here() {
    return Math.round((this.spring ? this.spring.to : this.pos) / this.H);
  }

  private kick() {
    if (this.raf || this.disposed) return;
    this.raf = requestAnimationFrame(this.frame);
  }

  private frame = (now: number) => {
    this.raf = 0;
    const sp = this.spring;
    if (sp) {
      const dt = clamp((now - sp.at) / 1000, 0, 0.032);
      sp.at = now;
      for (let k = 0; k < 4; k += 1) {
        const d = dt / 4;
        const a = -W * W * (this.pos - sp.to) - 2 * W * sp.v;
        sp.v += a * d;
        this.pos += sp.v * d;
      }
      if (Math.abs(this.pos - sp.to) < 0.3 && Math.abs(sp.v) < 6) {
        this.pos = sp.to;
        this.spring = null;
      }
    }
    this.draw();
    if (this.spring) this.kick();
    else this.rested();
  };

  draw(force = false) {
    if (!this.N) return;
    const { H, N } = this;
    const s = clamp(this.pos / H, 0, N - 1);
    const over = this.pos / H - s;
    if (!force && Math.abs(s - this.last) < 1e-5 && over === this.lastOver) return;
    if (over !== this.lastOver || force) {
      this.lastOver = over;
      this.picsEl.style.transform = over ? `translate3d(0,${(-over * H).toFixed(2)}px,0)` : '';
    }
    this.last = s;
    const i = Math.min(N - 1, Math.floor(s + 1e-6));
    const p = clamp(s - i);
    const j = i + 1;
    if (this.pickAt.size) {
      const keep = new Set([this.keyAt(i), this.keyAt(j)]);
      this.pickAt.forEach((_, key) => {
        if (!keep.has(key)) this.pickAt.delete(key);
      });
    }
    for (let k = Math.max(0, i - 1); k <= Math.min(N - 1, i + 2); k += 1) this.pic(k);
    const near = new Map<string, number>();
    for (let k = Math.max(0, i - 3); k <= Math.min(N - 1, i + 4); k += 1) near.set(this.P[k].key, k);
    this.pics.forEach((x, key) => {
      const k = near.get(key);
      if (k == null) {
        this.dropVideo(x);
        x.el.remove();
        this.pics.delete(key);
        return;
      }
      const on = k === i || (k === j && p > 0);
      x.el.classList.toggle('is-on', on);
      if (!on) return;
      if (k === i) {
        const g = clamp((p - 0.1) / 0.9);
        x.el.style.transform = `translate3d(0,${(-0.34 * g * g * H).toFixed(2)}px,0)`;
        x.sink.style.opacity = (0.16 * ss(0.1, 0.9, p)).toFixed(3);
        x.up.style.transform = x.down.style.transform = 'scaleY(0)';
      } else {
        x.el.style.transform = `translate3d(0,${((1 - p) * H).toFixed(2)}px,0)`;
        x.sink.style.opacity = '0';
        x.up.style.transform = `scaleY(${clamp(p * 4).toFixed(3)})`;
        x.down.style.transform = `scaleY(${clamp((1 - p) * 4.2).toFixed(3)})`;
      }
    });
    if (this.gi !== i) {
      this.gi = i;
      this.grounds[0].style.background = this.groundOf(i);
      this.grounds[1].style.background = this.groundOf(Math.min(N - 1, j));
    }
    this.grounds[1].style.opacity = ss(0.05, 0.95, p).toFixed(3);
    // (the words stand in one place for every post: a post still measuring, or with a long caption, moves nothing)
    this.date(i, j, p);
    this.slots.who.set(i, j, p, 0);
    this.slots.wait.set(i, j, p, 0.03);
    this.number(i, j, p);
    this.reads(i, j, p);
    this.slots.figs.forEach((f, k) => f.set(i, j, p, 0.05 + k * 0.012, 9));
    const shown = this.show1(i, j, p);
    this.figsEl.style.opacity = shown.toFixed(3);
    this.stripEl.style.opacity = shown.toFixed(3);
    this.stripEl.style.visibility = shown > 0 ? '' : 'hidden';
    this.slots.cap.set(i, j, p, 0.12);
    // the way out goes to the post nearest
    const nearest = p < 0.5 ? i : Math.min(j, N - 1);
    if (this.igAt !== nearest) {
      this.igAt = nearest;
      const url = this.P[nearest].url;
      if (url) this.ig.href = url;
      this.ig.style.visibility = url ? '' : 'hidden';
    }
  }

  private show1(i: number, j: number, p: number) {
    const a = this.has(i);
    const b = j < this.N && this.has(j);
    return a && (j >= this.N || b) ? 1 : a ? 1 - ss(0.2, 0.6, p) : b ? ss(0.4, 0.8, p) : 0;
  }

  private date(i: number, j: number, p: number) {
    const da = dayOf(this.P[i].day);
    const db = j < this.N ? dayOf(this.P[j].day) : da;
    const an = String(da.getUTCDate());
    const bn = String(db.getUTCDate());
    this.dcols.forEach((c, k) => {
      const x = an[k] ?? '';
      const y = bn[k] ?? '';
      const t = x === y ? 0 : ss(0.22 + (1 - k) * 0.05, 0.72 + (1 - k) * 0.05, p);
      if (c.a.textContent !== x) c.a.textContent = x;
      if (c.b.textContent !== y) c.b.textContent = y;
      c.a.style.transform = `translate3d(0,${-t * 100}%,0)`;
      c.b.style.transform = `translate3d(0,${(1 - t) * 100}%,0)`;
      c.b.style.visibility = t > 0 ? 'visible' : 'hidden';
      c.el.style.width = `${mix(this.dw[x] ?? 0, this.dw[y] ?? 0, t).toFixed(2)}px`;
    });
    const am = MON.format(da);
    const bm = MON.format(db);
    const moved = am !== bm;
    const mon = this.$('.dw-dmon');
    const m = moved && p >= 0.5 ? bm : am;
    if (mon.textContent !== m) mon.textContent = m;
    mon.style.opacity = String(moved ? Math.abs(2 * p - 1) : 1);
    const aw = WK.format(da);
    const bw = WK.format(db);
    const same = aw === bw;
    const out = same ? 0 : ss(0.1, 0.42, p);
    const come = same ? 0 : ss(0.48, 0.82, p);
    if (this.wa.textContent !== aw) this.wa.textContent = aw;
    if (this.wb.textContent !== bw) this.wb.textContent = bw;
    this.wa.style.opacity = String(1 - out);
    this.wa.style.transform = `translate3d(0,${-12 * this.s * out}px,0)`;
    this.wb.style.opacity = String(come);
    this.wb.style.transform = `translate3d(0,${12 * this.s * (1 - come)}px,0)`;
  }

  private number(i: number, j: number, p: number) {
    const ra = this.has(i) ? this.R(i) : null;
    this.numberAt(ra, j < this.N ? (this.has(j) ? this.R(j) : null) : ra, p);
  }

  private numberAt(ra: Read | null, rb: Read | null, p: number, back = false) {
    let a = ra?.top != null ? formatTopPercent(ra.top).slice(0, -1) : '';
    let b = rb?.top != null ? formatTopPercent(rb.top).slice(0, -1) : '';
    if (!a) a = b;
    else if (!b) b = a;
    const w = back ? -1 : 1;
    const wOf = (g: string) => (g ? this.gw[g] ?? 90 * this.s : 0);
    this.cols.forEach((c, k) => {
      const t = ss(0.2 + k * 0.05, 0.74 + k * 0.05, p);
      const x = a[k] ?? '';
      const y = b[k] ?? '';
      if (c.a.textContent !== x) c.a.textContent = x;
      if (c.b.textContent !== y) c.b.textContent = y;
      const off = x === y ? 0 : t;
      c.a.style.transform = `translate3d(0,${-off * 100 * w}%,0)`;
      c.b.style.transform = `translate3d(0,${(1 - off) * 100 * w}%,0)`;
      c.b.style.visibility = off > 0 ? 'visible' : 'hidden';
      c.el.style.width = `${mix(wOf(x), wOf(y), off).toFixed(2)}px`;
    });
    const show = ra && rb ? 1 : ra ? 1 - ss(0.06, 0.4, p) : rb ? ss(0.5, 0.86, p) : 0;
    this.numEl.style.opacity = String(show);
    this.numEl.style.visibility = show > 0 ? '' : 'hidden';
    this.numEl.style.transform = `translate3d(0,${((1 - show) * (ra ? -22 : 22) * this.s).toFixed(2)}px,0)`;
    const win = mix(isWinner(ra?.top) ? 1 : 0, isWinner(rb?.top) ? 1 : 0, ss(0.35, 0.65, p));
    this.numEl.style.color = win > 0 ? `color-mix(in oklab, var(--fm-accent) ${(win * 100).toFixed(1)}%, var(--fm-fg))` : '';
  }

  /* the strip (D1 · D3 · D7 · D21): its glide on the read shown, the reads it has lit */
  private reads(i: number, j: number, p: number) {
    const k = Math.min(j, this.N - 1);
    this.stripAt(i, this.R(i), this.R(k), ss(0.25, 0.75, p), p < 0.5 ? i : k);
  }

  private stripAt(i: number, ra: Read, rb: Read, t: number, near: number) {
    const la = ra.cp;
    const lb = rb.cp;
    const x = mix(Math.max(0, la), Math.max(0, lb), t);
    const o = mix(la < 0 ? 0 : 1, lb < 0 ? 0 : 1, t);
    this.glide.style.transform = `translate3d(${(x * 54 * this.s).toFixed(2)}px,0,0)`;
    this.glide.style.opacity = o.toFixed(3);
    const nr = near === i ? (t < 0.5 ? ra : rb) : this.R(near);
    this.glide.classList.toggle('is-win', isWinner(nr.top));
    const had = new Set(this.P[near].reads.map((r) => r.cp));
    this.segs.forEach((seg, n) => {
      seg.classList.toggle('is-had', had.has(n));
      seg.setAttribute('aria-checked', String(Math.round(x) === n));
      (seg.lastChild as HTMLElement).style.opacity = (o * clamp(1 - Math.abs(n - x))).toFixed(3);
    });
  }

  /* picking a read on the strip: the glide goes to it, the rank rolls to what it was then and the figures to what the
     post had (a later read rolls up, as the page does going on; an earlier one down), on the carry curve. A pick holds
     while the post is in view; scrolling, a key or a move ends one in motion at its end */
  private pick(n: number) {
    const i = this.here();
    if (this.busy || this.spring || Math.abs(this.pos - i * this.H) > 2 || !this.has(i)) return;
    const post = this.P[i];
    const idx = post.reads.findIndex((r) => r.cp === n);
    if (idx < 0) return;
    this.endPick();
    const from = this.R(i);
    if (from.cp === n) return;
    if (idx === post.reads.length - 1) this.pickAt.delete(post.key);
    else this.pickAt.set(post.key, idx);
    const to = this.R(i);
    this.tw = { i, from, to, back: to.cp < from.cp, t0: performance.now(), dur: reduced() ? 1 : 440 * 1.35 };
    this.twRaf = requestAnimationFrame(this.pickStep);
  }

  private pickStep = (now: number) => {
    const tw = this.tw;
    if (!tw) return;
    const t = clamp((now - tw.t0) / tw.dur);
    const p = carry(t);
    this.numberAt(tw.from, tw.to, p, tw.back);
    this.stripAt(tw.i, tw.from, tw.to, p, tw.i);
    this.slots.figs.forEach((f, k) => f.swap(this.figHtml(tw.from, k), this.figHtml(tw.to, k), p, 0.05 + k * 0.012, 9, tw.back));
    if (t >= 1) {
      this.tw = null;
      this.slots.figs.forEach((f) => f.reset());
      this.draw(true);
      return;
    }
    this.twRaf = requestAnimationFrame(this.pickStep);
  };

  private endPick() {
    if (!this.tw) return;
    cancelAnimationFrame(this.twRaf);
    this.tw = null;
    this.slots.figs.forEach((f) => f.reset());
    this.draw(true);
  }

  /* ── the page: with the fingers, let go to a post (one a swipe); a notch or a key, a post ── */

  to(k: number, v = 0) {
    if (!this.N) return;
    const at = clamp(k, 0, this.N - 1);
    const d = at * this.H - this.pos;
    this.spring = { v: d ? Math.sign(d) * clamp(v * Math.sign(d), 0, W * Math.abs(d)) : 0, to: at * this.H, at: performance.now() };
    if (reduced()) {
      this.pos = this.spring.to;
      this.spring = null;
      this.draw();
      this.rested();
      return;
    }
    this.kick();
  }

  step(way: 1 | -1) {
    if (this.busy) return;
    this.endPick();
    this.to(this.here() + way, this.spring?.v ?? 0);
  }

  // straight to a post (no glide)
  goTo(key: string) {
    const at = this.P.findIndex((post) => post.key === key);
    if (at < 0) return;
    this.spring = null;
    this.pos = at * this.H;
    this.draw(true);
    this.rested();
  }

  private letGo(flick: boolean) {
    const g = this.g;
    if (!g.on) return;
    g.on = false;
    g.tail = true;
    g.done = g.at;
    g.low = g.prev;
    const moved = this.pos - g.base * this.H;
    const way = Math.sign(moved) || 1;
    const go = g.n <= 2 ? 1 : flick && g.peak >= 5 && Math.abs(moved) > 8 ? 1 : Math.abs(moved) > 0.2 * this.H ? 1 : 0;
    const back = Math.abs(moved) > 8 && Math.sign(g.v) !== way && Math.abs(g.v) > 250;
    this.to(g.base + (go && !back ? way : 0), g.v);
  }

  private dayWheel(e: WheelEvent) {
    const dy = e.deltaY * (e.deltaMode === 1 ? 24 : 1);
    const a = Math.abs(dy);
    const gap = e.timeStamp - this.g.at;
    this.g.at = e.timeStamp;
    if (!a) return;
    window.clearTimeout(this.quiet);
    const fresh = gap > 140;
    // a mouse's notch (or a big first step): one post
    if (fresh && (e.deltaMode === 1 || a >= 90)) {
      this.g = { ...this.g, on: false, tail: true, prev: a, low: a, done: e.timeStamp };
      this.to(this.here() + Math.sign(dy), this.spring?.v ?? 0);
      return;
    }
    // the tail of a swipe already let go: nothing, unless a new swipe starts within it
    if (this.g.tail && !fresh) {
      const again = a > this.g.prev && a > this.g.low * 1.8 + 4 && e.timeStamp - this.g.done > 140;
      this.g.low = Math.min(this.g.low, a);
      this.g.prev = a;
      if (!again) return;
    }
    if (fresh || this.g.tail || !this.g.on) this.g = { ...freshWheel(), on: true, base: this.here(), at: e.timeStamp };
    const g = this.g;
    this.spring = null;
    g.n += 1;
    g.peak = Math.max(g.peak, a);
    g.fall = a < g.prev ? g.fall + 1 : a > g.prev ? 0 : g.fall;
    g.prev = a;
    const dt = clamp(gap, 6, 40);
    g.v = g.n === 1 ? 0 : g.v * 0.6 + (dy / dt) * 1000 * 0.4;
    const { H, N } = this;
    const edge = this.pos < 0 || this.pos > (N - 1) * H;
    this.pos = clamp(this.pos + dy * (edge ? 0.3 : 1), Math.max((g.base - 1) * H, -0.12 * H), Math.min((g.base + 1) * H, (N - 1 + 0.12) * H));
    this.draw();
    if (g.n > 5 && g.fall >= 4 && a < g.peak * 0.8) this.letGo(true);
    else this.quiet = window.setTimeout(() => this.letGo(false), 100);
  }

  /* ── a carousel's slides: sideways as the page goes up and down; its arrows a slide each ── */

  private slideDraw(x: Pic) {
    if (!x.track) return;
    x.track.style.transform = `translate3d(${(-x.sx).toFixed(2)}px,0,0)`;
    const k = clamp(Math.round(x.sx / this.picW), 0, x.n - 1);
    if (k === x.shown) return;
    x.shown = k;
    x.dots.forEach((d, n) => d.classList.toggle('is-on', n === k));
    if (x.count) x.count.textContent = `${k + 1}/${x.n}`;
    x.el.classList.toggle('is-first', k === 0);
    x.el.classList.toggle('is-last', k === x.n - 1);
  }

  private slideTo(key: string, k: number, v = 0) {
    const x = this.pics.get(key);
    if (!x || x.n < 2 || this.busy) return;
    const at = clamp(k, 0, x.n - 1);
    x.k = at;
    this.slideAt.set(key, at);
    const to = at * this.picW;
    const d = to - x.sx;
    this.sspring = { x, to, v: d ? Math.sign(d) * clamp(v * Math.sign(d), 0, W * Math.abs(d)) : 0, at: performance.now() };
    if (!this.sraf) this.sraf = requestAnimationFrame(this.slideStep);
  }

  private slideStep = (now: number) => {
    this.sraf = 0;
    const s = this.sspring;
    if (!s) return;
    const dt = clamp((now - s.at) / 1000, 0, 0.032);
    s.at = now;
    for (let k = 0; k < 4; k += 1) {
      const d = dt / 4;
      const a = -W * W * (s.x.sx - s.to) - 2 * W * s.v;
      s.v += a * d;
      s.x.sx += s.v * d;
    }
    if (Math.abs(s.x.sx - s.to) < 0.3 && Math.abs(s.v) < 6) {
      s.x.sx = s.to;
      this.sspring = null;
      this.slideDraw(s.x);
      this.slideSettled(s.x);
      return;
    }
    this.slideDraw(s.x);
    this.sraf = requestAnimationFrame(this.slideStep);
  };

  // a slide come to rest: the colours that join it to its neighbours follow it
  private slideSettled(x: Pic) {
    const im = x.imgs[x.k];
    if (!im) return;
    im.decode().then(() => {
      if (this.pics.get(x.key) === x && x.imgs[x.k] === im) {
        this.colours(x, im);
        this.gi = -1;
        this.draw(true);
      }
    }, () => {});
  }

  private slideLetGo(flick: boolean) {
    const sg = this.sg;
    if (!sg.on) return;
    sg.on = false;
    sg.tail = true;
    sg.done = sg.at;
    sg.low = sg.prev;
    const x = this.pics.get(sg.key);
    if (!x) return;
    const w = this.picW;
    const moved = x.sx - sg.base * w;
    const way = Math.sign(moved) || 1;
    const go = sg.n <= 2 ? 1 : flick && sg.peak >= 5 && Math.abs(moved) > 8 ? 1 : Math.abs(moved) > 0.2 * w ? 1 : 0;
    const back = Math.abs(moved) > 8 && Math.sign(sg.v) !== way && Math.abs(sg.v) > 250;
    this.slideTo(sg.key, sg.base + (go && !back ? way : 0), sg.v);
  }

  private slideWheel(e: WheelEvent, x: Pic) {
    const dx = e.deltaX * (e.deltaMode === 1 ? 24 : 1);
    const a = Math.abs(dx);
    const gap = e.timeStamp - this.sg.at;
    this.sg.at = e.timeStamp;
    if (!a) return;
    window.clearTimeout(this.squiet);
    const fresh = gap > 140 || this.sg.key !== x.key;
    if (fresh && (e.deltaMode === 1 || a >= 90)) {
      this.sg = { ...this.sg, key: x.key, on: false, tail: true, prev: a, low: a, done: e.timeStamp };
      this.slideTo(x.key, x.k + Math.sign(dx), this.sspring?.v ?? 0);
      return;
    }
    if (this.sg.tail && !fresh) {
      const again = a > this.sg.prev && a > this.sg.low * 1.8 + 4 && e.timeStamp - this.sg.done > 140;
      this.sg.low = Math.min(this.sg.low, a);
      this.sg.prev = a;
      if (!again) return;
    }
    if (fresh || this.sg.tail || !this.sg.on) this.sg = { ...freshWheel(), key: x.key, on: true, base: x.k, at: e.timeStamp };
    const sg = this.sg;
    if (this.sspring?.x === x) this.sspring = null;
    sg.n += 1;
    sg.peak = Math.max(sg.peak, a);
    sg.fall = a < sg.prev ? sg.fall + 1 : a > sg.prev ? 0 : sg.fall;
    sg.prev = a;
    const dt = clamp(gap, 6, 40);
    sg.v = sg.n === 1 ? 0 : sg.v * 0.6 + (dx / dt) * 1000 * 0.4;
    const w = this.picW;
    const edge = x.sx < 0 || x.sx > (x.n - 1) * w;
    x.sx = clamp(x.sx + dx * (edge ? 0.3 : 1), Math.max((sg.base - 1) * w, -0.12 * w), Math.min((sg.base + 1) * w, (x.n - 1 + 0.12) * w));
    this.slideDraw(x);
    if (sg.n > 5 && sg.fall >= 4 && a < sg.peak * 0.8) this.slideLetGo(true);
    else this.squiet = window.setTimeout(() => this.slideLetGo(false), 100);
  }

  /* ── a reel at rest plays its preview (muted, round and round); the page going on stops it ── */

  private rested() {
    window.clearTimeout(this.restTimer);
    this.restTimer = window.setTimeout(() => {
      if (this.disposed || this.spring || this.busy) return;
      const i = this.here();
      const post = this.P[i];
      if (!post || Math.abs(this.pos - i * this.H) > 2) return;
      if (this.restKey !== post.key) {
        this.restKey = post.key;
        this.opts.onRest?.(post.key);
      }
      this.pics.forEach((x, key) => {
        if (key !== post.key) this.dropVideo(x);
      });
      const x = this.pics.get(post.key);
      if (x && post.preview && post.slides.length <= 1 && !x.video && !reduced()) this.playVideo(x, post.preview);
    }, 140);
  }

  private playVideo(x: Pic, src: string) {
    const v = document.createElement('video');
    v.className = 'dw-video';
    v.muted = true;
    v.loop = true;
    v.playsInline = true;
    v.preload = 'auto';
    v.setAttribute('muted', '');
    v.setAttribute('playsinline', '');
    v.src = src;
    v.addEventListener('playing', () => v.classList.add('is-on'), { once: true });
    v.addEventListener('error', () => this.dropVideo(x), { once: true });
    x.el.insertBefore(v, x.sink);
    x.video = v;
    void v.play().catch(() => this.dropVideo(x));
  }

  private dropVideo(x: Pic) {
    const v = x.video;
    if (!v) return;
    x.video = null;
    v.pause();
    v.removeAttribute('src');
    v.load();
    v.remove();
  }

  /* ── input ── */

  private onWheel = (e: WheelEvent) => {
    if (e.ctrlKey) return;
    e.preventDefault();
    if (this.busy || !this.N) return;
    this.endPick();
    if (e.timeStamp - this.axisAt > 140) this.axis = Math.abs(e.deltaX) > Math.abs(e.deltaY) * 1.2 ? 'x' : 'y';
    this.axisAt = e.timeStamp;
    if (this.axis === 'x') {
      const i = this.here();
      const x = this.pics.get(this.keyAt(i) ?? '');
      if (x && x.n > 1 && !this.spring && Math.abs(this.pos - i * this.H) < 2) this.slideWheel(e, x);
      return;
    }
    this.pics.forEach((x) => this.dropVideo(x));
    this.dayWheel(e);
  };

  /* a finger (a wide tablet): the page goes with it, and lets go to a post as a swipe does */
  private onTouchStart = (e: TouchEvent) => {
    if (this.busy || e.touches.length !== 1) {
      this.t0 = null;
      return;
    }
    const y = e.touches[0].clientY;
    this.endPick();
    this.t0 = { y, p: this.pos, base: this.here(), v: 0, at: e.timeStamp, ly: y };
    this.spring = null;
  };

  private onTouchMove = (e: TouchEvent) => {
    const t = this.t0;
    if (!t || e.touches.length !== 1) return;
    e.preventDefault();
    const y = e.touches[0].clientY;
    t.v = ((t.ly - y) / Math.max(6, e.timeStamp - t.at)) * 1000;
    t.ly = y;
    t.at = e.timeStamp;
    this.pos = clamp(t.p + (t.y - y), -0.1 * this.H, (this.N - 0.9) * this.H);
    this.pics.forEach((x) => this.dropVideo(x));
    this.draw();
  };

  private onTouchEnd = () => {
    const t = this.t0;
    if (!t) return;
    this.t0 = null;
    const m = this.pos - t.base * this.H;
    this.to(t.base + (Math.abs(t.v) > 300 ? Math.sign(t.v) : Math.abs(m) > 0.3 * this.H ? Math.sign(m) : 0), t.v);
  };

  private onKey = (e: KeyboardEvent) => {
    if (e.defaultPrevented || e.metaKey || e.ctrlKey || e.altKey) return;
    const target = e.target as HTMLElement | null;
    if (target && (target.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName))) return;
    const down = ['ArrowDown', 'j', 'J', 'PageDown', ' '].includes(e.key);
    const up = ['ArrowUp', 'k', 'K', 'PageUp'].includes(e.key);
    if (!down && !up) return;
    e.preventDefault();
    if (e.repeat && this.spring) return;
    this.step(down ? 1 : -1);
  };

  private onClick = (e: MouseEvent) => {
    const t = e.target as HTMLElement;
    const who = t.closest<HTMLElement>('[data-who]');
    if (who?.dataset.who && this.opts.onWho) {
      e.preventDefault();
      this.opts.onWho(who.dataset.who);
      return;
    }
    const seg = t.closest<HTMLElement>('[data-cp]');
    if (seg?.dataset.cp) {
      this.pick(Number(seg.dataset.cp));
      return;
    }
    const arrow = t.closest<HTMLElement>('.dw-arw');
    if (arrow) {
      const key = this.keyAt(this.here());
      const x = key ? this.pics.get(key) : null;
      if (key && x) this.slideTo(key, x.k + (arrow.classList.contains('dw-next') ? 1 : -1));
    }
  };

  dispose() {
    this.disposed = true;
    cancelAnimationFrame(this.raf);
    cancelAnimationFrame(this.sraf);
    cancelAnimationFrame(this.twRaf);
    window.clearTimeout(this.quiet);
    window.clearTimeout(this.squiet);
    window.clearTimeout(this.restTimer);
    this.pics.forEach((x) => this.dropVideo(x));
    this.root.removeEventListener('wheel', this.onWheel);
    this.root.removeEventListener('click', this.onClick);
    this.root.removeEventListener('touchstart', this.onTouchStart);
    this.root.removeEventListener('touchmove', this.onTouchMove);
    this.root.removeEventListener('touchend', this.onTouchEnd);
    this.root.removeEventListener('touchcancel', this.onTouchEnd);
    window.removeEventListener('keydown', this.onKey, true);
  }
}

type Read = WideRead;
