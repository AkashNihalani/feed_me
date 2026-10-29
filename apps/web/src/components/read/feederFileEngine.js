/* The Feeder File: the Read tab's top component. One map of a feeder's world, seen two ways, and one split that runs
   through it and the read below: Feeding (a post that landed above their usual post, the top half of their window) and
   Fading (under it). The map lays out the things the reader found in their posts (who's in them, the parts they play,
   brand deals, places, moments, lines, sounds, looks): rings by how often (the centre holds what's in the most posts),
   a sector per kind, and their posts round the rim as their own covers. Split tilts it: what feeds them rises off the
   frosted floor and what fades sinks under it. Map looks straight down and says the split in colour (red their top 10,
   black feeding, grey fading). Dragging only turns it.
   The panel shows the split; it never explains why a post landed. The rules below do, and a row a rule explains carries
   a chip that brings that rule up (the 'ff:rule' event, heard by readerEngine); a post's card opens its post mortem the
   way the wall does (data-post). Every other control here is data-ff*, so the read's own click router never mistakes
   one for its own.
   Ported from the approved prototype and kept as plain JS like readerEngine. FeederFile.tsx mounts it on its section
   and tears it down when the tab hides; it builds its own markup inside that section, styled by feederFile.css under
   .ff, and paints the canvas from the same tokens. */
import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { TEMPO } from '@/lib/tempo';

// the panel's own motion plays at the app's pace (lib/tempo); the map's camera and idle keep their own clock
const tm = (o) => ({ ...o, duration: (o.duration ?? 0) * TEMPO, delay: (o.delay ?? 0) * TEMPO });

const TEMPLATE = `<div class="ff-wrap" data-ff="wrap">
    <div class="ff-head"><div class="ff-title"><span class="ff-eyebrow"><i></i>Feeder File</span></div></div>
    <div class="ff-stage" data-ff="stage">
      <canvas data-ff="cv" aria-label="The account’s world as one map: the things in its posts by kind, its posts round the rim. In Split, what feeds it rises off the floor and what fades sinks under it."></canvas>
      <div class="ff-labels" data-ff="labels"></div>
      <span class="ff-axis" data-ff="axisHi">↑ Feeding</span><span class="ff-axis mid" data-ff="axisMid">— His usual post</span><span class="ff-axis" data-ff="axisLo">↓ Fading</span>
      <div class="ff-hud" data-ff="hud">
        <div class="ff-lenses" data-ff="lenses" role="tablist" aria-label="View"><button type="button" role="tab" data-l="split"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3.5l7.5 4.3v8.4L12 20.5l-7.5-4.3V7.8z"/><path d="M12 12l7.5-4.2M12 12v8.5M12 12L4.5 7.8"/></svg><b>Split</b></button><button type="button" role="tab" data-l="map"><svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="2.2"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="9.8"/></svg><b>Map</b></button></div>
        <p class="ff-key" data-ff="key"></p>
      </div>
      <div class="ff-card hide" data-ff="card" aria-live="polite"></div>
    </div>
    <div class="ff-tools">
      <button type="button" class="ff-tool" data-ff="full" aria-label="Full screen"><svg class="ic" viewBox="0 0 24 24" aria-hidden="true"><path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5"/></svg><span>Full screen</span></button>
      <button type="button" class="ff-tool" data-ff="zfit" aria-label="Show everything"><svg class="ic" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="4.5"/><path d="M12 3v3.5M12 17.5V21M3 12h3.5M17.5 12H21"/></svg><span>Reset</span></button>
      <button type="button" class="ff-tool sq" data-ff="zout" aria-label="Zoom out">−</button>
      <button type="button" class="ff-tool sq" data-ff="zin" aria-label="Zoom in">+</button>
    </div>
    <div class="ff-body" data-ff="detail"><section class="ff-panel" data-ff="reading" aria-live="polite"></section></div>
  </div>`;

export function mountFeederFile(host, { data: D, thumbs: THUMBS = {} }) {
host.innerHTML = TEMPLATE;
const q = (id) => host.querySelector(`[data-ff="${id}"]`);
let dead = false, raf = 0;

// one map, two ways to look at it: Split tilts it (what feeds him rises, what fades sinks); Map looks straight down
const ord = (k) => { k = Math.round(k); const s = ['th', 'st', 'nd', 'rd'], v = k % 100; return k + (s[(v - 20) % 10] || s[v] || s[0]); };
const med = (a) => { const s = [...a].sort((x, y) => x - y), m = s.length >> 1; return s.length ? (s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2) : null; };
const list = (a) => (a.length <= 1 ? a.join('') : a.slice(0, -1).join(', ') + ' and ' + a[a.length - 1]);
const isDark = () => document.documentElement.classList.contains('dark');
const reduce = matchMedia('(prefers-reduced-motion: reduce)').matches;
// the icon library; each feed's kinds (from the data) pick from it
const ICON = {
  person: 'M12 12a4 4 0 1 0 0-8 4 4 0 0 0 0 8zM4.5 20.5c1.4-3.6 4.2-5.5 7.5-5.5s6.1 1.9 7.5 5.5',
  speech: 'M4 5h16v11h-9l-5 4v-4H4zM8.5 10.5h.01M12 10.5h.01M15.5 10.5h.01',
  note: 'M9 17.5V6l10-2v11.5M9 17.5a2.75 2.75 0 1 1-5.5 0 2.75 2.75 0 0 1 5.5 0zM19 15.5a2.75 2.75 0 1 1-5.5 0 2.75 2.75 0 0 1 5.5 0z',
  pin: 'M12 21s-6.5-6-6.5-11.5a6.5 6.5 0 0 1 13 0C18.5 15 12 21 12 21zM12 11.8a2.3 2.3 0 1 0 0-4.6 2.3 2.3 0 0 0 0 4.6z',
  shirt: 'M8.5 3.5L3.5 6.5l2 4.5 2.5-1v10.5h8V10l2.5 1 2-4.5-5-3c-.6 1.4-2 2.3-3.5 2.3S9.1 4.9 8.5 3.5z',
  bolt: 'M13.5 2.5L5 13.5h6.5l-1 8 8.5-11h-6.5z',
  people: 'M9 11a3.25 3.25 0 1 0 0-6.5A3.25 3.25 0 0 0 9 11zM2.5 20c1-3.4 3.5-5.3 6.5-5.3s5.5 1.9 6.5 5.3M15.5 4.6a3.25 3.25 0 0 1 0 6.3M17.5 14.9c1.9.6 3.3 2.3 4 5.1',
  tag: 'M3.5 11.5V3.5h8l9 9-8 8zM8 9.2a1.2 1.2 0 1 0 0-2.4 1.2 1.2 0 0 0 0 2.4z',
  box: 'M3.5 7.5L12 3l8.5 4.5v9L12 21l-8.5-4.5zM3.5 7.5L12 12l8.5-4.5M12 12v9',
  film: 'M4 5h16v14H4zM4 9h16M4 15h16M8 5v4M12 5v4M16 5v4M8 15v4M12 15v4M16 15v4',
};
const KIND = Object.fromEntries(D.kinds.map((k) => [k.id, k]));
const icon = (type) => ICON[KIND[type].icon];
const svg = (type) => `<svg class="ic" viewBox="0 0 24 24" aria-hidden="true"><path d="${icon(type)}"/></svg>`;

// ---------- model: his posts, the things the reader found in them, and which posts each thing is in
const posts = D.posts.map((p, i) => ({ ...p, kind: 'post', i, of: i, frags: [] })), np = posts.length;
const tags = D.tags.map((t, k) => ({ ...t, kind: 'tag', i: np + k }));
const N = [...posts, ...tags], L = tags.flatMap((t) => t.ps.map((j) => ({ s: t.i, t: j })));
D.frags.forEach((f) => posts[f.p].frags.push(f));
const nb = N.map(() => []); L.forEach((l, k) => { nb[l.s].push({ o: l.t, k }); nb[l.t].push({ o: l.s, k }); });
const tagsOf = (i) => nb[i].filter((e) => N[e.o].kind === 'tag').map((e) => e.o);
const postsOf = (i) => nb[i].filter((e) => N[e.o].kind === 'post').map((e) => e.o);
// each post is ranked among its own format (reels against reels, carousels against carousels); a feeder in one format
// reads as before. Its top quarter is the red punch (an account of 40 reels: its top 10)
const poolOf = (p) => p.pool || np, LANEW = { reel: 'reels', carousel: 'carousels', image: 'images' }, LANE1 = { reel: 'reel', carousel: 'carousel', image: 'image' };
const multi = new Set(posts.map((p) => p.lane || 'post')).size > 1;
const isTop = (p) => p.rank <= Math.max(1, Math.round(poolOf(p) / 4));
const place = (p) => `${ord(p.rank)} of ${poolOf(p)}${multi && LANEW[p.lane] ? ` ${LANEW[p.lane]}` : ''}`;
const power = (p) => { const n = poolOf(p); return (n / 2 + .5 - p.rank) / (n / 2 - .5); };
// who the account is, for the copy: his, her, their, its
const WHO = D.who || 'his', Who = WHO.charAt(0).toUpperCase() + WHO.slice(1), TOPW = multi ? 'Top quarter' : `${Who} top ${Math.round(np / 4)}`;
const allSecs = med(posts.filter((p) => p.secs > 0).map((p) => p.secs));
// the split the whole read runs on: feeding = landed above his usual post (the top half of his window), fading = under it
const feeds = (p) => p.rank <= poolOf(p) / 2;
// a set of posts: how many fed, how many faded, and which side it sits on (2+ posts: two thirds one way, else both sides)
function sideOf(ps) { const n = ps.length, f = ps.filter(feeds).length; return { n, feed: f, fade: n - f, side: n < 2 ? (f ? 'feed' : 'fade') : f * 3 >= n * 2 ? 'feed' : f * 3 <= n ? 'fade' : 'both' }; }
// dates arrive as '2 May'; a set's dates as '2 May – 15 Jun' or '12–14 Jun'
const dspan = (ps) => { const a = ps[0].date, b = ps[ps.length - 1].date; if (a === b) return a; const [da, ma] = a.split(' '), [db, mb] = b.split(' '); return ma === mb ? `${da}–${db} ${ma}` : `${a} – ${b}`; };

// ---------- each thing: its posts in posting order, its split, when it shows up, what it often comes with
const TS = new Map();
tags.forEach((tg) => {
  const ps = postsOf(tg.i).map((j) => N[j]).sort((a, b) => a.of - b.of), co = new Map();
  ps.forEach((p) => tagsOf(p.i).forEach((o) => { if (o !== tg.i) co.set(o, (co.get(o) || 0) + 1); }));
  TS.set(tg.i, { ps, ...sideOf(ps), w: med(ps.map((p) => p.rank)), wp: med(ps.map((p) => (p.rank - .5) / poolOf(p))), first: ps[0].of, last: ps[ps.length - 1].of, with: [...co].filter(([, c]) => c >= 2).sort((a, b) => b[1] - a[1]).map(([o]) => o) });
});
const ts = (x) => TS.get(x.i);
const RULE = Object.fromEntries((D.rules || []).map((r) => [r.id, r]));
// height, for Split: a post by where it landed (top of the window highest), a thing by how much of it fed him: always
// feeding high, always fading low, both sides on the floor; a thing in one or two posts lifts less than a pattern does
posts.forEach((p) => { p.y = power(p) * .78; });
tags.forEach((t) => { const s = ts(t); t.y = (s.feed / s.n - .5) * 1.25 * Math.min(1, (s.n + 1) / 4); });

// ---------- the map: rings by how often (the centre holds what's in the most posts), a sector per kind sized by how many
// things it holds, his posts round the rim, each turned toward the things it's made of
const RR = [.24, .48, .7, .92], POST_R = 1.16, ringOf = (n) => (n >= 5 ? 0 : n >= 3 ? 1 : n === 2 ? 2 : 3), SECTORS = [];
{ const TY = D.kinds.map((k) => k.id), wt = TY.map((ty) => Math.max(2.4, tags.filter((t) => t.type === ty).length)), sum = wt.reduce((a, b) => a + b, 0), GAP = .05; let a0 = -Math.PI / 2;
  TY.forEach((ty, k) => { const span = wt[k] / sum * Math.PI * 2; SECTORS.push({ type: ty, a0: a0 + GAP / 2, a1: a0 + span - GAP / 2 }); a0 += span; }); }
SECTORS.forEach((sc) => { for (let r = 0; r < 4; r++) { const its = tags.filter((t) => t.type === sc.type && ringOf(ts(t).n) === r).sort((a, b) => ts(a).wp - ts(b).wp), m = its.length; its.forEach((t, j) => { const a = m === 1 ? (sc.a0 + sc.a1) / 2 : sc.a0 + (j + .5) / m * (sc.a1 - sc.a0); t.ang = a; t.core = [Math.cos(a) * RR[r], Math.sin(a) * RR[r]]; }); } });
{ const want = posts.map((p) => { const ts_ = tagsOf(p.i).map((j) => N[j]); if (!ts_.length) return -Math.PI / 2 + p.i * 2.399; let x = 0, y = 0; ts_.forEach((t) => { x += Math.cos(t.ang) / t.ps.length; y += Math.sin(t.ang) / t.ps.length; }); return Math.atan2(y, x); });
  const ordr = posts.map((p) => p.i).sort((a, b) => want[a] - want[b]), ang = ordr.map((i) => want[i]), g = (Math.PI * 2 / np) * .86;
  for (let it = 0; it < 400; it++) for (let k = 0; k < np; k++) { const k2 = (k + 1) % np; let d = ang[k2] - ang[k]; if (k2 === 0) d += Math.PI * 2; if (d < g) { const push = (g - d) / 2; ang[k] -= push; ang[k2] += push; } }
  ordr.forEach((i, k) => { posts[i].core = [Math.cos(ang[k]) * POST_R, Math.sin(ang[k]) * POST_R]; }); }

// ---------- two ways to look at the one map: Split (tilted) and Map (straight down). Same places, only the tilt and
// the heights change
const TOP = .002;
const LENS = {
  split: { name: 'Split', key: 'Above the floor: feeding · Under it: fading', polar: 1.28, flat: false },
  map: { name: 'Map', key: 'Centre: in the most posts', polar: TOP, flat: true },
};
let lens = 'split';
N.forEach((x) => { x.at = { split: x.core, map: x.core }; });
const lensR = {}; Object.keys(LENS).forEach((k) => { lensR[k] = Math.max(...N.map((x) => Math.hypot(x.at[k][0], x.at[k][1]))); });
N.forEach((x) => { x.ph = (x.i * 2.399) % 6.283; x.vis = 1; x.app = 0; x.pulse = 0; x.cx = x.at[lens][0]; x.cz = x.at[lens][1]; x.pos = [x.cx, x.y, x.cz]; });

// ---------- theme toggle + page furniture
// ---------- three
const stage = q('stage'), cv = q('cv'), labelsEl = q('labels'), wrap = q('wrap'), detail = q('detail'), hud = q('hud');
const FONT = getComputedStyle(host).fontFamily || '"Space Grotesk", "Helvetica Neue", Arial, sans-serif';
let dark = isDark();
const renderer = new THREE.WebGLRenderer({ canvas: cv, antialias: true, powerPreference: 'high-performance' });
renderer.setPixelRatio(Math.min(2, devicePixelRatio || 1)); renderer.outputColorSpace = THREE.SRGBColorSpace;
const scene = new THREE.Scene(), camera = new THREE.PerspectiveCamera(40, 1, .01, 60);
const controls = new OrbitControls(camera, cv); cv.style.touchAction = 'pan-y';
// on the page the card zooms with the wheel, but a page scroll that drifts onto it keeps scrolling the page until it
// pauses, so the card never grabs a scroll halfway. A vertical swipe scrolls the page; a sideways drag, two fingers or
// a pinch work the map. Full screen hands every gesture to the map.
let pageWheelAt = 0, full = false;
const onPageWheel = (e) => { if (!stage.contains(e.target)) pageWheelAt = performance.now(); };
window.addEventListener('wheel', onPageWheel, { passive: true, capture: true });
stage.addEventListener('wheel', (e) => { if (full) return; const now = performance.now(); if (now - pageWheelAt < 450) { pageWheelAt = now; e.stopPropagation(); } }, { capture: true });
Object.assign(controls, { enableDamping: true, dampingFactor: .075, rotateSpeed: .55, zoomSpeed: .9, panSpeed: .8, enablePan: true, screenSpacePanning: true, minDistance: .6, maxDistance: 6, minPolarAngle: .22, maxPolarAngle: 1.5, autoRotateSpeed: .5 });
let BG = new THREE.Color(), INK = new THREE.Color(), RED = new THREE.Color(), LOW = new THREE.Color();
function glowTex() { const c = document.createElement('canvas'); c.width = c.height = 128; const g = c.getContext('2d'), gr = g.createRadialGradient(64, 64, 0, 64, 64, 64); gr.addColorStop(0, 'rgba(255,255,255,1)'); gr.addColorStop(.2, 'rgba(255,255,255,.5)'); gr.addColorStop(.5, 'rgba(255,255,255,.12)'); gr.addColorStop(1, 'rgba(255,255,255,0)'); g.fillStyle = gr; g.fillRect(0, 0, 128, 128); const t = new THREE.CanvasTexture(c); t.colorSpace = THREE.SRGBColorSpace; return t; }
const GLOW = glowTex();
const DOT = (() => { const c = document.createElement('canvas'); c.width = c.height = 64; const g = c.getContext('2d'); g.fillStyle = '#fff'; g.beginPath(); g.arc(32, 32, 26, 0, 6.283); g.fill(); return new THREE.CanvasTexture(c); })();
// a colour for this theme, faded toward the ground by how much it should recede
const paint = (C, base, it) => C.copy(base).lerp(BG, 1 - Math.max(0, Math.min(1, it)));
const recede = (C, base, v, floor = .14) => paint(C, C.copy(base).lerp(LOW, (1 - v) * .85), floor + (1 - floor) * v);

// the floor: his usual post. a frosted plane; anything under it reads veiled
// the floor: his usual post, the line between feeding and fading. A faint tinted disc with a crisp edge: enough to read as
// a surface, never the main thing, and clear enough that every stem and thread shows through it; it reads from above and below
const FR = 1.48;
const veilMap = (() => { const c = document.createElement('canvas'); c.width = c.height = 256; const g = c.getContext('2d'), gr = g.createRadialGradient(128, 128, 0, 128, 128, 128); gr.addColorStop(0, '#fff'); gr.addColorStop(.965, '#fff'); gr.addColorStop(1, '#000'); g.fillStyle = gr; g.fillRect(0, 0, 256, 256); return new THREE.CanvasTexture(c); })();
const veil = new THREE.Mesh(new THREE.CircleGeometry(FR, 96), new THREE.MeshBasicMaterial({ transparent: true, depthWrite: false, alphaMap: veilMap, fog: false, side: THREE.DoubleSide }));
veil.rotation.x = -Math.PI / 2; veil.renderOrder = 1; scene.add(veil);
const FLOOR = new THREE.Color();
// its edge: a crisp line, and a second one just under it so the floor has a lip from any angle
const rimOf = (y) => { const g = new THREE.BufferGeometry().setFromPoints(Array.from({ length: 192 }, (_, k) => new THREE.Vector3(Math.cos(k / 192 * Math.PI * 2) * FR * .962, y, Math.sin(k / 192 * Math.PI * 2) * FR * .962))); const o = new THREE.LineLoop(g, new THREE.LineBasicMaterial({ transparent: true, depthWrite: false, opacity: 0 })); o.renderOrder = 2; scene.add(o); return o; };
const rims = [rimOf(.002), rimOf(-.012)];
const grid = (() => { const s = .085, h = s * Math.sqrt(3), P = [], C = []; for (let row = -20; row <= 20; row++) for (let col = -20; col <= 20; col++) { const cx = col * s * 1.5, cz = row * h + (col % 2 ? h / 2 : 0); if (Math.hypot(cx, cz) > FR) continue; for (let q = 0; q < 3; q++) { const a0 = q * Math.PI / 3, a1 = (q + 1) * Math.PI / 3, x0 = cx + Math.cos(a0) * s, z0 = cz + Math.sin(a0) * s, x1 = cx + Math.cos(a1) * s, z1 = cz + Math.sin(a1) * s; P.push(x0, .001, z0, x1, .001, z1); [[x0, z0], [x1, z1]].forEach(([x, z]) => { const f = Math.max(0, 1 - Math.pow(Math.hypot(x, z) / FR, 2)); C.push(1, 1, 1, f); }); } } const g = new THREE.BufferGeometry(); g.setAttribute('position', new THREE.Float32BufferAttribute(P, 3)); g.setAttribute('color', new THREE.Float32BufferAttribute(C, 4)); const m = new THREE.LineSegments(g, new THREE.LineBasicMaterial({ vertexColors: true, transparent: true, depthWrite: false, fog: false })); m.renderOrder = 2; return m; })();
scene.add(grid);

// the map's floor marks: the rings, the sector edges and the rim his posts sit on
const guides = {}, fade = { core: 0, split: 0 };
const lineMat = (base, red) => { const m = new THREE.LineBasicMaterial({ transparent: true, depthWrite: false, opacity: 0 }); m.userData = { base, red }; return m; };
const lineOf = (pts, base, red, loop) => { const g = new THREE.BufferGeometry().setFromPoints(pts.map((p) => new THREE.Vector3(...p))); const o = loop ? new THREE.LineLoop(g, lineMat(base, red)) : new THREE.Line(g, lineMat(base, red)); o.renderOrder = 2; return o; };
const circ = (cx, cz, r, n = 96, y = .003) => Array.from({ length: n }, (_, k) => [cx + Math.cos(k / n * Math.PI * 2) * r, y, cz + Math.sin(k / n * Math.PI * 2) * r]);
{ const core = new THREE.Group(); RR.forEach((r, k) => core.add(lineOf(circ(0, 0, r), k === 0 ? .9 : .55, k === 0, true))); core.add(lineOf(circ(0, 0, POST_R), .3, false, true));
  SECTORS.forEach((s) => { const a = s.a0 - .025; core.add(lineOf([[Math.cos(a) * .13, .003, Math.sin(a) * .13], [Math.cos(a) * 1.04, .003, Math.sin(a) * 1.04]], .32, false)); });
  guides.core = core; scene.add(core); }

// posts: each one his own cover, round, on the rim. A red ring for his top 10, a black one when it fed; a post that faded
// goes grey with no ring. Drawn to a canvas once the cover loads, and again when the theme changes
const CV = 128, tmpC = document.createElement('canvas'); tmpC.width = tmpC.height = CV;
// covers shrink to fit the rim when a feeder has many posts (Lakmé's 89), so neighbours never sit on each other
const CVS = Math.min(1, (Math.PI * 2 * POST_R / np) * .92 / .104);
const coverImg = posts.map((p) => { if (!THUMBS[p.id]) return null; const im = new Image(); im.decoding = 'async'; im.onload = () => { if (!dead) drawCover(p); }; im.src = THUMBS[p.id]; return im; });
posts.forEach((p) => { const c = document.createElement('canvas'); c.width = c.height = CV; const t = new THREE.CanvasTexture(c); t.colorSpace = THREE.SRGBColorSpace; t.anisotropy = 4; const s = new THREE.Sprite(new THREE.SpriteMaterial({ map: t, transparent: true, depthWrite: false })); s.renderOrder = p.y >= 0 ? 3 : 0; scene.add(s); Object.assign(p, { cvs: c, ctex: t, spr: s, cs: .088 }); });
function drawCover(p) {
  const g = p.cvs.getContext('2d'), im = coverImg[p.i], top = isTop(p), fed = feeds(p), R = CV / 2, hx = (c) => `#${c.getHexString()}`, ring = top ? 8 : fed ? 4.5 : 0, gap = ring ? 4 : 0, r = R - 1 - ring - gap;
  g.clearRect(0, 0, CV, CV);
  if (ring) { g.beginPath(); g.arc(R, R, R - 1, 0, 6.283); g.fillStyle = top ? hx(RED) : hx(INK); g.fill(); g.beginPath(); g.arc(R, R, R - 1 - ring, 0, 6.283); g.fillStyle = hx(BG); g.fill(); }
  const tg = tmpC.getContext('2d'), D2 = Math.ceil(r * 2); tg.clearRect(0, 0, CV, CV);
  if (im && im.complete && im.naturalWidth) { tg.drawImage(im, 0, 0, D2, D2); if (!fed) { const d = tg.getImageData(0, 0, D2, D2), a = d.data; for (let k = 0; k < a.length; k += 4) { const l = a[k] * .299 + a[k + 1] * .587 + a[k + 2] * .114; a[k] = a[k + 1] = a[k + 2] = l; } tg.putImageData(d, 0, 0); } }
  else { const gr = tg.createLinearGradient(0, 0, D2 * .6, D2); gr.addColorStop(0, (p.tint || [])[0] || hx(LOW)); gr.addColorStop(1, (p.tint || [])[1] || hx(LOW)); tg.fillStyle = gr; tg.fillRect(0, 0, D2, D2); tg.fillStyle = p.tint ? 'rgba(255,255,255,.92)' : hx(INK); tg.font = `700 ${Math.round(D2 * .38)}px ${FONT}`; tg.textAlign = 'center'; tg.textBaseline = 'middle'; tg.fillText(p.name.charAt(0), D2 / 2, D2 / 2 + 2); }
  g.save(); g.beginPath(); g.arc(R, R, r, 0, 6.283); g.clip(); g.drawImage(tmpC, 0, 0, D2, D2, R - r, R - r, D2, D2);
  if (!fed) { g.fillStyle = hx(BG); g.globalAlpha = dark ? .45 : .35; g.fillRect(0, 0, CV, CV); g.globalAlpha = 1; }
  g.restore(); if (!fed) { g.beginPath(); g.arc(R, R, r, 0, 6.283); g.lineWidth = 1.5; g.strokeStyle = hx(LOW); g.stroke(); }
  p.ctex.needsUpdate = true;
}
const postHalo = posts.filter((p) => isTop(p)).map((p) => { const s = new THREE.Sprite(new THREE.SpriteMaterial({ map: GLOW, transparent: true, depthWrite: false })); s.userData.p = p; s.renderOrder = 2; scene.add(s); return s; });
// stems: in 3D every post stands on the floor or hangs under it, so above and below read from any angle
const mkSeg = (n, order) => { const g = new THREE.BufferGeometry(); g.setAttribute('position', new THREE.BufferAttribute(new Float32Array(n * 6), 3)); g.setAttribute('color', new THREE.BufferAttribute(new Float32Array(n * 6), 3)); const m = new THREE.LineSegments(g, new THREE.LineBasicMaterial({ vertexColors: true, transparent: true, depthWrite: false })); m.renderOrder = order; scene.add(m); return m; };
const stemUp = mkSeg(posts.filter((p) => p.y >= 0).length, 2), stemDown = mkSeg(posts.filter((p) => p.y < 0).length, 0);
const mkPts = (n, size, order) => { const g = new THREE.BufferGeometry(); g.setAttribute('position', new THREE.BufferAttribute(new Float32Array(n * 3), 3)); g.setAttribute('color', new THREE.BufferAttribute(new Float32Array(n * 3), 3)); const m = new THREE.PointsMaterial({ size, map: DOT, alphaTest: .5, vertexColors: true, transparent: true, depthWrite: false }); const o = new THREE.Points(g, m); o.renderOrder = order; scene.add(o); return o; };
const marks = mkPts(np, .026, 3);
const tagUp = mkSeg(tags.filter((t) => t.y >= 0).length, 2), tagDown = mkSeg(tags.filter((t) => t.y < 0).length, 0);
// tags: hexagon badges with an icon for the kind of thing; bigger = in more posts; red = usually his top 10
const BADGE = {};
function drawBadge(c, type, hot) {
  const g = c.getContext('2d'); g.clearRect(0, 0, 128, 128);
  const hexP = (r) => { g.beginPath(); for (let q = 0; q < 6; q++) { const a = q * Math.PI / 3 - Math.PI / 2, x = 64 + Math.cos(a) * r, y = 64 + Math.sin(a) * r; q ? g.lineTo(x, y) : g.moveTo(x, y); } g.closePath(); };
  const hx = (c) => `#${c.getHexString()}`, fill = hot ? hx(RED) : dark ? hx(BG.clone().lerp(INK, .09)) : '#ffffff', edge = hot ? hx(RED.clone().lerp(new THREE.Color(dark ? '#ffffff' : '#000000'), .2)) : hx(INK.clone().lerp(BG, dark ? .12 : 0)), ink = hot ? '#ffffff' : hx(INK);
  g.save(); g.shadowColor = dark ? 'rgba(0,0,0,.45)' : 'rgba(18,19,23,.16)'; g.shadowBlur = 6; g.shadowOffsetY = 2; hexP(53); g.fillStyle = fill; g.fill(); g.restore();
  hexP(52); g.lineWidth = hot ? 3 : 4; g.strokeStyle = edge; g.stroke();
  g.save(); g.translate(39, 39); g.scale(50 / 24, 50 / 24); g.lineWidth = 2; g.lineCap = 'round'; g.lineJoin = 'round'; g.strokeStyle = ink; g.stroke(new Path2D(icon(type))); g.restore();
}
const badgeTex = (type, hot) => { const k = type + hot; if (!BADGE[k]) { const c = document.createElement('canvas'); c.width = c.height = 128; const t = new THREE.CanvasTexture(c); t.colorSpace = THREE.SRGBColorSpace; t.anisotropy = 4; BADGE[k] = { c, t, type, hot }; } return BADGE[k].t; };
tags.forEach((x) => { const s = ts(x), hot = s.wp <= .25;
  const spr = new THREE.Sprite(new THREE.SpriteMaterial({ map: badgeTex(x.type, hot), transparent: true, depthWrite: false })); spr.renderOrder = x.y >= 0 ? 3 : 0; scene.add(spr);
  const halo = new THREE.Sprite(new THREE.SpriteMaterial({ map: GLOW, transparent: true, depthWrite: false })); halo.renderOrder = x.y >= 0 ? 2 : 0; scene.add(halo);
  Object.assign(x, { spr, halo, hot, size: .07 + Math.sqrt(s.n) * .03 }); });
// links, sparks, the reader's fragments orbiting their posts
const linkGeo = new THREE.BufferGeometry(), lp = new Float32Array(L.length * 6), lc = new Float32Array(L.length * 6);
linkGeo.setAttribute('position', new THREE.BufferAttribute(lp, 3)); linkGeo.setAttribute('color', new THREE.BufferAttribute(lc, 3));
const linkMesh = new THREE.LineSegments(linkGeo, new THREE.LineBasicMaterial({ vertexColors: true, transparent: true, depthWrite: false })); linkMesh.renderOrder = 2; scene.add(linkMesh);
let seed = 7; const rnd = () => ((seed = (seed * 16807) % 2147483647) / 2147483647);
const SPK = 160, spkS = Array.from({ length: SPK }, (_, k) => ({ l: k % L.length, u: rnd(), v: .06 + rnd() * .1 })), sparks = mkPts(SPK, .02, 3);
const FR_UP = [], FR_DN = []; posts.forEach((p) => p.frags.forEach(() => { const o = { p, r: .03 + rnd() * .05, w: (.12 + rnd() * .3) * (rnd() < .5 ? -1 : 1), a: rnd() * 6.283, tilt: rnd() * .9, yaw: rnd() * 6.283, hot: isTop(p) && rnd() > .5, s: rnd() }; (p.y >= 0 ? FR_UP : FR_DN).push(o); }));
const fragUp = mkPts(FR_UP.length, .009, 2), fragDn = mkPts(FR_DN.length, .009, 0);
// the idle scan: a ring sweeping out across the floor, lighting each post as it passes
const scan = lineOf(circ(0, 0, 1, 128, .016), 0, true, true); scan.renderOrder = 4; scene.add(scan);

function theme() {
  dark = isDark(); const cs = getComputedStyle(stage), tok = (k, f) => cs.getPropertyValue(k).trim() || f;
  BG.set(tok('--ff-bg', dark ? '#0c0c0f' : '#ffffff')); INK.set(tok('--ff-ink', dark ? '#fafafa' : '#0b0b0f')); RED.set(tok('--ff-red', '#DA2E46')); LOW.copy(BG).lerp(INK, .2);
  renderer.setClearColor(BG); scene.fog = new THREE.FogExp2(BG.getHex(), .1); FLOOR.copy(BG).lerp(INK, dark ? .045 : .03); veil.material.color.copy(FLOOR); grid.material.color.copy(INK); rims.forEach((r) => r.material.color.copy(INK));
  postHalo.forEach((h) => h.material.color.copy(RED)); tags.forEach((x) => x.halo.material.color.copy(RED));
  Object.values(BADGE).forEach((b) => { drawBadge(b.c, b.type, b.hot); b.t.needsUpdate = true; });
  posts.forEach(drawCover);
}
theme(); const themeWatch = new MutationObserver(theme); themeWatch.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });

// ---------- camera: view framing, the card's room, idle drift
const camGoal = { target: new THREE.Vector3(0, .04, 0), dist: 3, phi: null, theta: null, on: false };
const off = new THREE.Vector3(), sph = new THREE.Spherical();
let W = 1, H = 1; const view = { x: 0, y: 0 }, viewGoal = { x: 0, y: 0 };
const phone = () => W < 700;
let hudB = 10, moved = false, wide = false;
// each view has its own panel (its read, as a short scouting report) beside the map on a wide card, under it on a narrow
// one; open a thing and its card takes the panel on a wide card, or rises as a peek sheet on the map on a narrow one
const sheet = () => !wide && W < 700;
const restY = () => -Math.round(hudB * .5);
const fitDist = (k) => { const vf = Math.tan(THREE.MathUtils.degToRad(camera.fov / 2)), hf = vf * camera.aspect, R = lensR[k] * (LENS[k].flat ? 1.02 : .98) + .1, ph = LENS[k].polar, vHalf = LENS[k].flat ? R : R * Math.cos(ph) + .82 * Math.sin(ph) + .06, room = Math.max(.55, 1 - (hudB + 16) / H); return Math.max(2.2, Math.min(6, Math.max(R / hf, vHalf / (vf * room)))); };
function home(snap) { camGoal.target.set(0, LENS[lens].flat ? 0 : .04, 0); camGoal.dist = fitDist(lens); camGoal.phi = LENS[lens].polar; camGoal.theta = LENS[lens].flat ? 0 : null; camGoal.on = true; if (snap) { sph.set(camGoal.dist, camGoal.phi, .32); camera.position.setFromSpherical(sph).add(camGoal.target); controls.target.copy(camGoal.target); controls.update(); } }
// Split turns any way you drag it: round, down to eye level, under the floor to look up through it. Map turns flat on
// its centre. The wheel or a pinch zooms; nothing pans, so the map never drifts off
function controlsFor() { controls.enablePan = false; controls.touches = { ONE: THREE.TOUCH.ROTATE, TWO: THREE.TOUCH.DOLLY_ROTATE }; controls.mouseButtons = { LEFT: THREE.MOUSE.ROTATE, MIDDLE: THREE.MOUSE.DOLLY, RIGHT: THREE.MOUSE.ROTATE }; }
function limits() { if (camGoal.on) { controls.minPolarAngle = 0; controls.maxPolarAngle = Math.PI; } else if (LENS[lens].flat) { controls.minPolarAngle = controls.maxPolarAngle = LENS[lens].polar; } else { controls.minPolarAngle = .12; controls.maxPolarAngle = Math.PI - .12; } }
function applyView(force) { if (!force) { const dx = viewGoal.x - view.x, dy = viewGoal.y - view.y; if (Math.abs(dx) < .3 && Math.abs(dy) < .3) { if (view.x === viewGoal.x && view.y === viewGoal.y) return; view.x = viewGoal.x; view.y = viewGoal.y; } else { view.x += dx * .12; view.y += dy * .12; } }
  if (Math.abs(view.x) < .3 && Math.abs(view.y) < .3) camera.clearViewOffset(); else camera.setViewOffset(W, H, view.x, view.y, W, H); camera.updateProjectionMatrix(); }
function size() { W = stage.clientWidth; H = stage.clientHeight; wide = wrap.clientWidth >= 860; hudB = hud.offsetTop + hud.offsetHeight + 6; placeCard(); renderer.setSize(W, H, false); camera.aspect = W / H; camera.fov = W < 500 ? 46 : 38; if (!sel) { viewGoal.y = restY(); view.y = viewGoal.y; } applyView(true); }

// ---------- selection
let hov = -1, sel = null, filter = null, hoverI = -1, ambient = null, lastTouch = performance.now(), flatK = 0; const hood = new Set(), ambientHood = new Set();
const lit = (i) => { if (sel) return hood.has(i); if (filter) { const x = N[i]; return x.kind === 'tag' ? x.type === filter : tagsOf(i).some((j) => N[j].type === filter); } return true; };
const targetOf = (x) => [x.at[lens][0], LENS[lens].flat ? 0 : x.y, x.at[lens][1]];
let baseDist = 3;
function frameOn(idxs, center) { camGoal.theta = null; let far = .2; idxs.forEach((j) => { const p = targetOf(N[j]); far = Math.max(far, Math.hypot(p[0] - center[0], (p[1] - center[1]) * .7, p[2] - center[2])); }); camGoal.target.set(...center); baseDist = Math.max(LENS[lens].flat ? 2.1 : 1.8, Math.min(3.8, far * 3 + .6)); camGoal.dist = baseDist * roomK(); camGoal.phi = null; camGoal.on = true; }
const roomK = () => (sel && sheet() && cardH ? Math.min(1.7, Math.max(1, Math.sqrt(H / Math.max(120, H - cardH - 16)))) : 1);
function select(x) {
  sel = x || null; hood.clear(); ambient = null; ambientHood.clear(); lastTouch = performance.now();
  if (!sel) { card(); return; }
  if (sel.kind === 'post') { hood.add(sel.i); nb[sel.i].forEach((e) => hood.add(e.o)); }
  else { hood.add(sel.i); postsOf(sel.i).forEach((j) => hood.add(j)); }
  const vis = [...hood], p = targetOf(sel), hs = vis.map((j) => targetOf(N[j])), c = [0, 1, 2].map((k) => hs.reduce((s, q) => s + q[k], 0) / hs.length);
  frameOn(vis, p.map((v, k) => v * .55 + c[k] * .45));
  card();
}

// ---------- the card: a slim peek that leaves the map in view; pull it up for the full read. It says what a thing is,
// where it shows up and which side of the split its posts fell on; why they fell there lives in the rules, one tap away
const cardEl = q('card'); let cardOpen = false, cardH = 0;
const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const pl = (n, w, many) => `${n} ${n === 1 ? w : many || w + 's'}`;
const pad2 = (n) => String(n).padStart(2, '0');
// how a post shows its side everywhere: red his top 10, ink feeding, grey fading
const pcls = (p) => (isTop(p) ? 'hot' : feeds(p) ? 'up' : 'dn');
const cover = (p) => (THUMBS[p.id] ? `<img src="${THUMBS[p.id]}" alt="" loading="lazy" decoding="async" draggable="false">` : `<i class="ff-cvf" style="background:linear-gradient(155deg, ${(p.tint || [])[0] || '#8d9098'}, ${(p.tint || [])[1] || '#3a3c42'})">${esc(p.name.charAt(0))}</i>`);
const placeCell = (p) => `<span class="ff-rr${isTop(p) ? ' hot' : ''}"><em>${ord(p.rank)}</em><i>of ${poolOf(p)}</i></span>`;
const postRow = (p, line) => `<button type="button" class="ff-row post" data-ffgo="${p.i}" data-ffh="${p.i}"><span class="ff-cv ${pcls(p)}">${cover(p)}</span><span class="ff-rt"><b>${esc(p.name)}</b><i>${esc(line ?? p.label)}</i></span>${placeCell(p)}</button>`;
// the split, shown: a dot a post, best first; red his top 10, ink feeding, hollow fading
const dots = (ps) => { const f = ps.filter(feeds).length; return `<span class="ff-sd" role="img" aria-label="${f} feeding, ${ps.length - f} fading">${ps.slice().sort((a, b) => a.rank - b.rank).map((p) => `<i class="${pcls(p)}"></i>`).join('')}</span>`; };
const splitWords = (s) => `${s.feed} feeding · ${s.fade} fading`;
const ruleChip = (id, ask) => (RULE[id] ? `<button type="button" class="ff-chip" data-ffrule="${id}">${ask ? '<em>What decides it</em>' : ''}<span>Rule ${pad2(RULE[id].n)} · ${esc(RULE[id].short)}</span><i aria-hidden="true">↓</i></button>` : '');
const nowTag = (first, last) => (first >= np - 10 ? `<span class="ff-st new">New in ${WHO} last 10</span>` : last < np - 10 ? `<span class="ff-st">Not in ${WHO} last 10</span>` : '<span class="ff-st">Still going</span>');
const bySide = (ps) => { const f = ps.filter(feeds).sort((a, b) => a.rank - b.rank), d = ps.filter((p) => !feeds(p)).sort((a, b) => a.rank - b.rank);
  return (f.length ? `<div class="cl">Feeding · ${f.length}</div>${f.map((p) => postRow(p)).join('')}` : '') + (d.length ? `<div class="cl">Fading · ${d.length}</div>${d.map((p) => postRow(p)).join('')}` : ''); };
const pill = (i) => { const x = N[i]; return x.kind === 'post' ? `<button type="button" class="pill" data-ffgo="${i}">${esc(x.name)}<i class="${isTop(x) ? 'r' : ''}">${ord(x.rank)}</i></button>` : `<button type="button" class="pill" data-ffgo="${i}">${svg(x.type)}${esc(x.name)}</button>`; };
function cardParts(x) {
  const close = `<button type="button" class="x" data-ffclose aria-label="Close">✕</button>`;
  if (x.kind === 'tag') {
    const s = ts(x), one = s.ps[0];
    const peek = `<div class="ck"><span class="t">${svg(x.type)}${KIND[x.type].one}</span><span>${pl(s.n, 'post')} · ${dspan(s.ps)}</span>${close}</div><h2 class="ch">${esc(x.name)}</h2>`
      + `<div class="kv">${s.n > 1 ? `${dots(s.ps)}<span>${splitWords(s)}</span>${nowTag(s.first, s.last)}` : `<span class="${pcls(one)}">${feeds(one) ? 'Feeding' : 'Fading'} · ${place(one)}</span>`}</div>`;
    return { peek, more: `<p class="cm">${esc(x.mean)}</p>${bySide(s.ps)}${s.with.length ? `<div class="cl">Often with</div><div class="pills">${s.with.slice(0, 5).map(pill).join('')}</div>` : ''}${x.rule ? `<div class="ff-cr">${ruleChip(x.rule, s.side === 'both')}</div>` : ''}` };
  }
  const tg = tagsOf(x.i), who = tg.map((o) => N[o]).find((t) => t.type === 'people');
  const peek = `<div class="ck"><span class="t${isTop(x) ? ' r' : ''}">${feeds(x) ? 'Feeding' : 'Fading'} · ${place(x)}</span><span>${x.date}${x.paid ? ' · paid' : ''}</span>${close}</div><h2 class="ch">${esc(x.name)}</h2>`
    + `<div class="kv">${who ? `<span>${esc(who.name)}</span>` : ''}${x.secs ? `<span>${x.secs} sec</span>` : ''}${multi && LANE1[x.lane] ? `<span>${LANE1[x.lane]}</span>` : ''}</div>`;
  const said = x.frags.filter((f) => f.k === 'line').slice(0, 2).map((f) => `“${esc(f.t)}”`).join(' '), onscreen = x.frags.find((f) => f.k === 'text'), refs = [...new Set(x.frags.filter((f) => f.k === 'ref').map((f) => f.t))].slice(0, 3);
  return { peek, more: `<p class="cm">${esc(x.label)}</p>${said ? `<div class="said"><span>What’s said</span>${said}</div>` : ''}${onscreen && (!said || !said.includes(esc(onscreen.t))) ? `<div class="said"><span>On screen</span>“${esc(onscreen.t)}”</div>` : ''}${refs.length ? `<div class="said"><span>It references</span>${esc(refs.join(' · '))}</div>` : ''}${tg.length ? `<div class="cl">Made of</div><div class="pills">${tg.map(pill).join('')}</div>` : ''}<button type="button" class="ff-pm" data-post="${esc(x.id)}"><span class="ff-pmc">${cover(x)}</span><span>Open its post mortem</span><i aria-hidden="true">↓</i></button>` };
}
function card() {
  if (!sel) { cardEl.classList.add('hide'); detail.classList.remove('carded'); wrap.classList.remove('carded'); cardH = 0; viewGoal.x = 0; viewGoal.y = restY(); return; }
  const { peek, more } = cardParts(sel);
  cardEl.innerHTML = `<button type="button" class="grab" aria-label="Show more"></button><div class="peek">${peek}<button type="button" class="exp" data-ffexp>${cardOpen ? 'Less' : 'More'}</button></div><div class="more">${more}</div>`;
  cardEl.classList.toggle('open', cardOpen || !sheet()); cardEl.classList.remove('hide'); detail.classList.toggle('carded', wide); wrap.classList.toggle('carded', wide); cardEl.querySelector('.more').scrollTop = 0; measureCard();
}
function placeCard() { const side = wide, homeEl = side ? detail : stage; if (cardEl.parentNode !== homeEl) homeEl.appendChild(cardEl); detail.classList.toggle('carded', side && !!sel); wrap.classList.toggle('carded', side && !!sel); cardEl.classList.toggle('open', cardOpen || !sheet()); }
function measureCard() { requestAnimationFrame(() => { if (!sel) return; if (wide) { cardH = 0; viewGoal.x = 0; viewGoal.y = restY(); return; } const r = cardEl.getBoundingClientRect(); cardH = r.height; if (sheet()) { viewGoal.x = 0; viewGoal.y = Math.min(H * .3, cardH / 2 + 4); camGoal.dist = baseDist * roomK(); camGoal.on = true; } else { viewGoal.y = 0; viewGoal.x = (r.width + 24) / 2; } }); }
function setOpen(v) { cardOpen = v; cardEl.classList.toggle('open', v || !sheet()); const b = cardEl.querySelector('[data-ffexp]'); if (b) b.textContent = v ? 'Less' : 'More'; measureCard(); }
// the rules live in the read below: ask it to bring one up and open it (readerEngine hears 'ff:rule')
function toRule(id) { if (full) setFull(false); window.dispatchEvent(new CustomEvent('ff:rule', { detail: id })); }
{ let dy0 = null; cardEl.addEventListener('pointerdown', (e) => { if (e.target.closest('.peek, .grab') && !e.target.closest('button:not(.grab)')) dy0 = e.clientY; });
  cardEl.addEventListener('pointerup', (e) => { if (dy0 == null) return; const dy = e.clientY - dy0; dy0 = null; if (dy < -24) setOpen(true); else if (dy > 24) { if (cardOpen) setOpen(false); else select(null); } else if (sheet() && !e.target.closest('button')) setOpen(!cardOpen); });
  cardEl.addEventListener('click', (e) => { const g = e.target.closest('[data-ffgo]'); if (g) { select(N[+g.dataset.ffgo]); return; } if (e.target.closest('[data-ffclose]')) { select(null); return; } const rc = e.target.closest('[data-ffrule]'); if (rc) { toRule(rc.dataset.ffrule); return; } if (e.target.closest('[data-post]')) { if (full) setFull(false); return; } if (e.target.closest('[data-ffexp], .grab') && sheet()) setOpen(!cardOpen); }); }

// ---------- the mode switch
const lensesEl = q('lenses'), keyEl = q('key'), readEl = q('reading');
function setLens(k) { lens = k; [...lensesEl.children].forEach((b) => { b.classList.toggle('on', b.dataset.l === k); b.setAttribute('aria-selected', b.dataset.l === k); }); keyEl.textContent = LENS[k].key; controlsFor(); if (sel) select(sel); else home(); lastTouch = performance.now(); }
lensesEl.addEventListener('click', (e) => { const b = e.target.closest('button'); if (b && b.dataset.l !== lens) setLens(b.dataset.l); });
// ---------- the panel: one read for both modes. The account in a line, the reader's headline, what feeding and fading
// mean, then the things that sit on both sides (and the rule that decides them), then his world kind by kind, each thing
// in plain words with a post it turns up in. Rows tie to the map: pointing at one rings it, tapping opens it. Nothing
// here says why a post landed; where a rule explains a row, a chip points down to it
const HEAD = (D.heads || {}).map || '';
const sec = (title, sub, body) => (body ? `<section class="ff-sec"><div class="ff-sh"><b>${title}</b>${sub ? `<span>${sub}</span>` : ''}</div>${body}</section>` : '');
const sideCell = (ps) => (ps.length > 1 ? `<span class="ff-rd">${dots(ps)}</span>` : placeCell(ps[0]));
const nowMark = (s) => (s.first >= np - 10 ? '<span class="ff-nw new">New</span>' : s.n > 1 && s.last < np - 10 ? '<span class="ff-nw">Not lately</span>' : '');
const thingRow = (x) => { const s = ts(x), ex = x.ex && posts[x.ex.p]; return `<button type="button" class="ff-row code" data-ffgo="${x.i}" data-ffh="${x.i}"><span class="ff-ri">${svg(x.type)}</span><span class="ff-rt"><b>${esc(x.name)}${nowMark(s)}</b><i>${esc(x.mean)}</i>${ex ? `<em class="ff-ex">${esc(x.ex.line)}${s.n > 1 ? ` <u class="${pcls(ex)}">${ord(ex.rank)}</u>` : ''}</em>` : ''}</span>${sideCell(s.ps)}</button>`; };
const kindPosts = (k) => posts.filter((p) => tags.some((t) => t.type === k && t.ps.includes(p.i)));
const standing = (t) => ({ feed: 0, both: 1, fade: 2 })[ts(t).side];
const kindTags = (k) => tags.filter((t) => t.type === k).sort((a, b) => standing(a) - standing(b) || ts(b).n - ts(a).n || ts(a).wp - ts(b).wp);
// on both sides: a thing that fed and faded, or a kind whose things are one post each (his brand deals); its best
// feeding post against its worst fading one, then the rule that decides it
const both = [...tags.filter((t) => ts(t).n >= 2 && ts(t).side === 'both').map((t) => ({ u: t, ps: ts(t).ps })),
  ...D.kinds.filter((k) => { const ks = tags.filter((t) => t.type === k.id); return ks.length >= 2 && ks.every((t) => ts(t).n === 1); }).map((k) => ({ u: k, ps: kindPosts(k.id) })).filter((x) => sideOf(x.ps).side === 'both')]
  .sort((a, b) => !!b.u.rule - !!a.u.rule || b.ps.length - a.ps.length)
  // two rows that come down to the same pair of posts say the same thing: keep the first
  .filter(function (x) { const f = x.ps.filter(feeds).sort((a, b) => a.rank - b.rank)[0], d = x.ps.filter((p) => !feeds(p)).sort((a, b) => b.rank - a.rank)[0], k = `${f.i}:${d.i}`; if (this.has(k)) return false; this.add(k); return true; }, new Set());
const bothRow = ({ u, ps }) => { const f = ps.filter(feeds).sort((a, b) => a.rank - b.rank)[0], d = ps.filter((p) => !feeds(p)).sort((a, b) => b.rank - a.rank)[0], isKind = u.kind !== 'tag';
  const side = (p, w) => `<button type="button" class="ff-vs" data-ffgo="${p.i}" data-ffh="${p.i}"><span class="ff-cv ${pcls(p)}">${cover(p)}</span><span class="ff-vt"><em class="${pcls(p)}">${w} · ${ord(p.rank)}</em><b>${esc(p.name)}</b></span></button>`;
  return `<div class="ff-both"><button type="button" class="ff-bh" ${isKind ? `data-ffkind="${u.id}"` : `data-ffgo="${u.i}"`}><span class="ff-ri">${svg(isKind ? u.id : u.type)}</span><b>${esc(isKind ? u.title : u.name)}</b><span class="ff-rd">${dots(ps)}</span></button><div class="ff-vss">${side(f, 'Fed')}${side(d, 'Faded')}</div>${u.rule ? ruleChip(u.rule, true) : '<span class="ff-open">What decides it: still open</span>'}</div>`; };
// the account in one line, from code: how many, when, how often, how long, how many paid
const dayOf = (s) => { const [d, m] = s.split(' '); return Date.UTC(2026, 'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split(' ').indexOf(m), +d) / 864e5; };
const days = Math.max(7, dayOf(posts[np - 1].date) - dayOf(posts[0].date) + 1), unit = D.unit || 'posts';
const lanes = Object.entries(posts.reduce((m, p) => { const k = p.lane || 'post'; m[k] = (m[k] || 0) + 1; return m; }, {})).sort((a, b) => b[1] - a[1]);
const kicker = [multi ? lanes.map(([k, n]) => pl(n, LANE1[k] || k, LANEW[k])).join(', ') : pl(np, unit.replace(/s$/, ''), unit), dspan(posts), `${Math.round(np / days * 7)} a week`, ...(allSecs ? [`${Math.round(allSecs)} sec typical`] : []), `${posts.filter((p) => p.paid).length} ${D.paidWord || 'paid'}`].join(' · ');
const SHOW = 3;
let openKind = null;
function reading(fresh) {
  let eyebrow, head, sub, body, pre = '', legend = '';
  if (openKind) {
    const k = KIND[openKind], ks = kindTags(openKind), ps = kindPosts(openKind);
    pre = `<button type="button" class="ff-back" data-ffback>← ${Who} world</button>`;
    eyebrow = `<b>${esc(k.title)}</b>${pl(ks.length, 'thing')} · ${pl(ps.length, 'post')}`;
    head = `${splitWords(sideOf(ps))}.`; sub = 'Feeding first. Each one in plain words, then a post it turns up in.';
    body = ks.map(thingRow).join('');
  } else {
    eyebrow = `<b>${esc(D.handle)}</b>${esc(kicker)}`; head = HEAD;
    sub = `<b>Feeding</b> means a post landed above ${WHO} usual one, in the top half of ${multi ? `${WHO} posts in the same format` : `${WHO} ${np}`}. <b>Fading</b> means it landed under it.`;
    legend = `<div class="ff-lg" aria-hidden="true"><span><i class="hot"></i>${TOPW}</span><span><i class="up"></i>Feeding</span><span><i class="dn"></i>Fading</span></div>`;
    body = sec('On both sides', 'Same thing, opposite results', both.slice(0, 4).map(bothRow).join(''))
      + D.kinds.map((k) => { const ks = kindTags(k.id); if (!ks.length) return ''; return sec(esc(k.title), splitWords(sideOf(kindPosts(k.id))), ks.slice(0, SHOW).map(thingRow).join('') + (ks.length > SHOW ? `<button type="button" class="ff-more" data-ffkind="${k.id}">All ${ks.length} ${esc(k.many.toLowerCase())}<i aria-hidden="true">→</i></button>` : '')); }).join('');
  }
  readEl.innerHTML = `${pre}<div class="ff-ph"><span class="ff-pk">${eyebrow}</span>${head ? `<h3 class="ff-head1">${esc(head)}</h3>` : ''}${sub ? `<p class="ff-psub">${sub}</p>` : ''}${legend}</div>${body}`;
  readEl.scrollTop = 0; detail.scrollTop = 0;
  // a fresh panel arrives row by row, slow and in order
  if (fresh && !reduce) [...readEl.querySelectorAll('.ff-back, .ff-ph, .ff-sh, .ff-row, .ff-both, .ff-more')].slice(0, 24).forEach((el, j) => el.animate([{ opacity: 0, transform: 'translateY(10px)' }, { opacity: 1, transform: 'none' }], tm({ duration: 620, delay: 30 + j * 34, easing: 'cubic-bezier(.22, 1, .36, 1)', fill: 'backwards' })));
}
readEl.addEventListener('click', (e) => {
  const rc = e.target.closest('[data-ffrule]'); if (rc) { toRule(rc.dataset.ffrule); return; }
  const kb = e.target.closest('[data-ffkind]'); if (kb) { openKind = kb.dataset.ffkind; filter = openKind; select(null); reading(true); return; }
  if (e.target.closest('[data-ffback]')) { openKind = null; filter = null; select(null); reading(true); return; }
  const g = e.target.closest('[data-ffgo]'); if (!g) return;
  select(N[+g.dataset.ffgo]); if (!wide) stage.scrollIntoView({ block: 'nearest', behavior: reduce ? 'auto' : 'smooth' });
});
// pointing at a row rings its thing on the map
const onRowOver = (e) => { const r = e.target.closest('[data-ffh]'), v = r ? +r.dataset.ffh : -1; if (v !== hoverCand) { hoverCand = v; hoverT = 0; } };
[readEl, cardEl].forEach((el) => { el.addEventListener('mouseover', onRowOver); el.addEventListener('mouseleave', () => { hoverCand = -1; }); });
q('zin').onclick = () => { moved = true; camGoal.dist = Math.max(.7, camera.position.distanceTo(controls.target) / 1.4); camGoal.target.copy(controls.target); camGoal.phi = null; camGoal.on = true; lastTouch = performance.now(); };
q('zout').onclick = () => { moved = true; camGoal.dist = Math.min(5.6, camera.position.distanceTo(controls.target) * 1.4); camGoal.target.copy(controls.target); camGoal.phi = null; camGoal.on = true; lastTouch = performance.now(); };
const fullBtn = q('full'), FULL_IN = fullBtn.innerHTML, FULL_OUT = '<svg class="ic" viewBox="0 0 24 24" aria-hidden="true"><path d="M6 6l12 12M18 6L6 18"/></svg><span>Close</span>';
let portal = null, spacer = null, rootOverflow = '';
function setFull(v) {
  if (v === full || dead) return; full = v; lastTouch = performance.now();
  if (v) {
    spacer = document.createElement('div'); spacer.style.height = `${wrap.offsetHeight}px`; wrap.parentNode.insertBefore(spacer, wrap);
    portal = document.createElement('div'); portal.className = 'ff ff-portal'; document.body.appendChild(portal); portal.appendChild(wrap);
    rootOverflow = document.documentElement.style.overflow; document.documentElement.style.overflow = 'hidden'; cv.style.touchAction = 'none';
  } else {
    if (spacer) spacer.replaceWith(wrap); spacer = null; if (portal) portal.remove(); portal = null;
    document.documentElement.style.overflow = rootOverflow; cv.style.touchAction = 'pan-y';
  }
  fullBtn.innerHTML = v ? FULL_OUT : FULL_IN; fullBtn.setAttribute('aria-label', v ? 'Close full screen' : 'Full screen'); moved = false;
}
fullBtn.onclick = () => setFull(!full);
const onKey = (e) => { if (e.key === 'Escape' && full) setFull(false); };
window.addEventListener('keydown', onKey);
q('zfit').onclick = () => { moved = false; filter = null; openKind = null; reading(); select(null); home(); };
controls.addEventListener('start', () => { camGoal.on = false; moved = true; limits(); });

// ---------- picking: tap the nearest thing; double-tap empty space to dive in
const v3 = new THREE.Vector3(), ray = new THREE.Raycaster(), plane = new THREE.Plane(new THREE.Vector3(0, 1, 0), 0);
function screenOf(x, y, z) { v3.set(x, y, z).project(camera); return [(v3.x + 1) / 2 * W, (1 - v3.y) / 2 * H, v3.z]; }
let down = null, lastTap = { t: 0, x: 0, y: 0 };
['pointerdown', 'wheel', 'keydown', 'touchstart'].forEach((ev) => stage.addEventListener(ev, () => { lastTouch = performance.now(); }, { passive: true }));
cv.addEventListener('pointerdown', (e) => { down = { x: e.clientX, y: e.clientY, t: performance.now() }; });
cv.addEventListener('pointerup', (e) => { if (!down) return; const travel = Math.hypot(e.clientX - down.x, e.clientY - down.y), quick = performance.now() - down.t < 400; down = null; if (travel > 8 || !quick) return; const r = cv.getBoundingClientRect(), mx = e.clientX - r.left, my = e.clientY - r.top, now = performance.now(), hit = pick(mx, my);
  if (hit < 0 && now - lastTap.t < 320 && Math.hypot(mx - lastTap.x, my - lastTap.y) < 30) { ray.setFromCamera(new THREE.Vector2(mx / W * 2 - 1, -(my / H) * 2 + 1), camera); plane.constant = -controls.target.y; const p = ray.ray.intersectPlane(plane, new THREE.Vector3()); if (p) { moved = true; camGoal.target.copy(p.clampLength(0, 1.3)); camGoal.dist = Math.max(.8, camera.position.distanceTo(controls.target) / 1.8); camGoal.phi = null; camGoal.on = true; } lastTap.t = 0; return; }
  lastTap = { t: now, x: mx, y: my }; if (hit >= 0 || sel) select(hit >= 0 ? N[hit] : null); });
let hoverCand = -1, hoverT = 0;
cv.addEventListener('pointermove', (e) => { if (e.pointerType !== 'mouse' || e.buttons) { hoverCand = -1; return; } const r = cv.getBoundingClientRect(), h = pick(e.clientX - r.left, e.clientY - r.top); if (h !== hoverCand) { hoverCand = h; hoverT = performance.now(); } cv.style.cursor = h >= 0 ? 'pointer' : LENS[lens].flat ? 'move' : 'grab'; });
cv.addEventListener('pointerleave', () => { hoverCand = -1; hoverI = -1; });
function pick(mx, my) { let best = -1, bd = 26; N.forEach((x) => { if (!x.shown || x.scr == null || x.scr[2] > 1) return; if (sel && !hood.has(x.i)) return; const d = Math.hypot(mx - x.scr[0], my - x.scr[1]) - (x.scrR || 0) * .8; if (d < bd) { bd = d; best = x.i; } }); return best; }

// ---------- labels (HTML, crisp): things, plus each view's own captions
const lbs = N.map((x) => { const e = document.createElement('div'); e.className = 'ff-lb ' + x.kind + ((x.kind === 'post' ? isTop(x) : x.hot) ? ' red' : ''); if (x.kind === 'post') e.innerHTML = `<b>${esc(x.name)}</b><i>${feeds(x) ? 'Feeding' : 'Fading'} · ${place(x)}</i>`; else { const s = ts(x); e.innerHTML = `<b>${svg(x.type)}${esc(x.name)}</b><i>${s.n > 1 ? splitWords(s) : `${feeds(s.ps[0]) ? 'Feeding' : 'Fading'} · ${ord(s.ps[0].rank)}`}${s.first >= np - 10 ? ' · new' : ''}</i>`; } e.style.opacity = 0; labelsEl.appendChild(e); return e; });
const GL = [];
const gl = (lensK, html, pos, cls = '', act) => { const e = document.createElement(act ? 'button' : 'div'); e.className = 'ff-gl ' + cls; e.innerHTML = html; if (act) { e.type = 'button'; e.addEventListener('click', act); } e.style.opacity = 0; labelsEl.appendChild(e); GL.push({ lens: lensK, e, pos, act: !!act }); };
// floor print: captions lying on the floor, always turned to read from where you stand
const floorTexts = [];
function floorText(lensK, text, a, r, opt = {}) {
  const fs = 44, c = document.createElement('canvas'), g = c.getContext('2d'), label = text.toUpperCase(), font = `700 ${fs}px ${FONT}`; g.font = font;
  const sp = fs * .24; let w = 0; for (const ch of label) w += g.measureText(ch).width + sp; const iw = opt.icon ? fs * 1.35 : 0;
  c.width = Math.ceil(w + iw + 20); c.height = 64; g.font = font; g.fillStyle = '#fff'; g.textBaseline = 'middle'; let x = 10;
  if (opt.icon) { g.save(); g.translate(x, 32 - fs * .46); g.scale(fs * .92 / 24, fs * .92 / 24); g.strokeStyle = '#fff'; g.lineWidth = 2.3; g.lineCap = 'round'; g.lineJoin = 'round'; g.stroke(new Path2D(icon(opt.icon))); g.restore(); x += iw; }
  for (const ch of label) { g.fillText(ch, x, 34); x += g.measureText(ch).width + sp; }
  const tex = new THREE.CanvasTexture(c); tex.colorSpace = THREE.SRGBColorSpace; tex.anisotropy = 8; const h = opt.h || .045;
  const m = new THREE.Mesh(new THREE.PlaneGeometry(h * c.width / c.height, h), new THREE.MeshBasicMaterial({ map: tex, transparent: true, depthWrite: false, opacity: 0, fog: false }));
  m.rotation.order = 'YXZ'; m.renderOrder = 2; m.userData = { lens: lensK, a, base: opt.base || .7, red: !!opt.red }; m.position.set(Math.cos(a) * r, .006, Math.sin(a) * r); scene.add(m); floorTexts.push(m);
}
function buildFloorPrint() {
  ['In 5+ posts', 'In 3–4', 'In 2', 'Once'].forEach((t, k) => floorText('core', t, -Math.PI / 2, RR[k] + .045, { red: k === 0, base: k === 0 ? 1 : .75 }));
  floorText('core', `${Who} posts`, -Math.PI / 2, POST_R + .1, { base: .6 });
  // the floor names itself between two kinds, front and left, clear of the captions at the top
  [Math.PI / 2, Math.PI].map((want) => SECTORS.map((sc) => sc.a0 - .025).reduce((m, a) => (Math.abs(Math.atan2(Math.sin(a - want), Math.cos(a - want))) < Math.abs(Math.atan2(Math.sin(m - want), Math.cos(m - want))) ? a : m))).forEach((a) => floorText('split', `${Who} usual post`, a, FR * .885, { base: .5, h: .036 }));
  SECTORS.forEach((s) => floorText('core', KIND[s.type].many, (s.a0 + s.a1) / 2, 1.4, { icon: s.type, h: .06, base: .9 }));
}
const ring = document.createElement('div'); ring.className = 'ff-hring'; ring.innerHTML = '<svg viewBox="-1.1 -1.1 2.2 2.2" aria-hidden="true"><circle r="1"/></svg>'; labelsEl.appendChild(ring);
const axHi = q('axisHi'), axMid = q('axisMid'), axLo = q('axisLo'); axMid.textContent = `— ${Who} usual post`;

// ---------- frame
const M4 = new THREE.Matrix4(), Q = new THREE.Quaternion(), S3 = new THREE.Vector3(), P3 = new THREE.Vector3(), C = new THREE.Color(), C2 = new THREE.Color();
const IDLE_MS = 12000;
let lookUp = false;
let t = 0, t0 = performance.now(), lastT = t0, visible = true, idle = false, idleT0 = 0, amp = 1, scanT = -1, ambientAt = 0, ambientK = 0, scanBase = 0;
const order = N.map((x) => x.i).sort((a, b) => (N[a].kind === 'post' ? N[a].of : TS.get(a).first + .5) - (N[b].kind === 'post' ? N[b].of : TS.get(b).first + .5));
const appearAt = new Map(order.map((i, k) => [i, k * (reduce ? 0 : 46)]));
// standouts the idle hover drifts across, per view
const standouts = () => [...tags.filter((t) => ts(t).n >= 3), ...posts.filter((p) => p.rank <= 4)];
// performance as colour for the flat views: red his top 10, bold above his usual, faint under it
function frame(now) {
  if (dead) return;
  const dt = Math.min(50, now - lastT) / 1000; lastT = now; if (!reduce) t += dt; const age = now - t0, flat = LENS[lens].flat;
  if (hoverCand !== hoverI && (hoverCand < 0 || now - hoverT > 160)) hoverI = hoverCand;
  // idle: after a few untouched seconds it starts drifting, sweeping and hovering on its own
  // idle never moves your view: no fly-home, no new angle, no zoom. Brain drifts round very slowly; the flat maps hold still
  const wantIdle = !reduce && !sel && hoverI < 0 && now - lastTouch > IDLE_MS && !camGoal.on;
  if (wantIdle && !idle) { idle = true; idleT0 = now; controls.autoRotate = !flat; controls.autoRotateSpeed = .16; scanT = now + 1500; ambientAt = now + 2500; ambientK = 0; }
  if (!wantIdle && idle) { idle = false; controls.autoRotate = false; ambient = null; ambientHood.clear(); }
  amp += ((idle && !flat ? 1.6 : 1) - amp) * .015;
  if (idle) {
    if (now > ambientAt) { const lst = standouts(); ambient = lst[ambientK % lst.length]; ambientK++; ambientAt = now + 5200; ambientHood.clear(); if (ambient) { ambientHood.add(ambient.i); nb[ambient.i].forEach((e) => { if (ambient.kind === 'post' || !L[e.k].sim) ambientHood.add(e.o); }); } }
    if (!flat && now > scanT + 2800) scanT = now + 9000;
  }
  if (camGoal.on) { controls.target.lerp(camGoal.target, .085); off.copy(camera.position).sub(controls.target); sph.setFromVector3(off); const d0 = sph.radius; sph.radius += (camGoal.dist - sph.radius) * .085; if (camGoal.phi != null) sph.phi += (camGoal.phi - sph.phi) * .085; let dth = 0; if (camGoal.theta != null) { dth = Math.atan2(Math.sin(camGoal.theta - sph.theta), Math.cos(camGoal.theta - sph.theta)); sph.theta += dth * .085; } off.setFromSpherical(sph); camera.position.copy(controls.target).add(off); if (Math.abs(d0 - camGoal.dist) < .004 && controls.target.distanceTo(camGoal.target) < .004 && (camGoal.phi == null || Math.abs(sph.phi - camGoal.phi) < .002) && Math.abs(dth) < .003) { camGoal.on = false; camGoal.theta = null; } }
  limits(); controls.update();
  { const under = camera.position.y < 0; if (under !== lookUp) { lookUp = under; const o = (y) => ((y >= 0) !== under ? 3 : 0); posts.forEach((p) => { p.spr.renderOrder = o(p.y); }); tags.forEach((x) => { x.spr.renderOrder = o(x.y); x.halo.renderOrder = o(x.y) ? 2 : 0; }); postHalo.forEach((h) => { h.renderOrder = o(h.userData.p.y) ? 2 : 0; }); stemUp.renderOrder = tagUp.renderOrder = under ? 0 : 2; stemDown.renderOrder = tagDown.renderOrder = under ? 2 : 0; } } scene.fog.density = (dark ? .16 : .11) * (1 - .8 * flatK) / Math.max(1.2, camera.position.distanceTo(controls.target));
  { const tg = controls.target, r = Math.hypot(tg.x, tg.z), lim = 1.35; if (r > lim || tg.y < -.7 || tg.y > .75) { P3.copy(tg); const k = r > lim ? lim / r : 1; tg.x *= k; tg.z *= k; tg.y = THREE.MathUtils.clamp(tg.y, -.7, .75); camera.position.add(P3.set(tg.x - P3.x, tg.y - P3.y, tg.z - P3.z)); } }
  applyView(false);
  flatK += ((flat ? 1 : 0) - flatK) * .07; const tall = 1 - flatK;
  hov = sel ? (hoverI >= 0 && hood.has(hoverI) && hoverI !== sel.i ? hoverI : -1) : hoverI >= 0 ? hoverI : ambient && ambient.kind !== 'group' ? ambient.i : -1;
  // morph toward this view; nodes arrive in the order he posted
  N.forEach((x) => { x.cx += (x.at[lens][0] - x.cx) * .065; x.cz += (x.at[lens][1] - x.cz) * .065; const a = Math.max(0, Math.min(1, (age - appearAt.get(x.i)) / 700)); x.app = reduce ? 1 : 1 - Math.pow(1 - a, 3); x.shown = x.app > .5;
    if (x.tv == null) { x.tv = 1; x.fx = 0; }
    const on = lit(x.i); x.vis += ((on ? 1 : sel ? .1 : .3) - x.vis) * .08; x.pulse *= .955;
    if (x.kind === 'post') x.fx += ((x === sel || x.i === hov ? 1 : 0) - x.fx) * .08; });
  fade.core += (1 - fade.core) * .08; fade.split = tall;
  const yD = (x) => x.y * tall + .016 * flatK, wob = amp * tall + .3 * flatK;
  const pos = (x) => [x.cx + Math.sin(t * .3 + x.ph) * .005 * wob, yD(x) + Math.sin(t * .45 + x.ph * 1.7) * .01 * amp * tall, x.cz + Math.cos(t * .27 + x.ph) * .005 * wob];
  veil.material.opacity = (dark ? .27 : .22) * tall; veil.visible = tall > .01; grid.material.opacity = dark ? .06 : .065; rims[0].material.opacity = .3 * tall; rims[1].material.opacity = .1 * tall; rims.forEach((r) => { r.visible = tall > .01; });
  // scan ring
  let scanR = -1; if (idle && !flat && now > scanT) { const u = (now - scanT) / 2800; if (u <= 1) { scanR = .05 + u * 1.45; scan.scale.set(scanR, 1, scanR); scanBase = Math.sin(u * Math.PI) * .7; } } if (scanR < 0) scanBase *= .9;
  scan.material.color.copy(RED); scan.material.opacity = scanBase; scan.visible = scanBase > .01;
  if (scanR > 0) N.forEach((x) => { if (Math.abs(Math.hypot(x.cx, x.cz) - scanR) < .05) x.pulse = 1; });
  Object.entries(guides).forEach(([k, g]) => { g.visible = fade[k] > .01; g.children.forEach((c) => { const m = c.material; m.color.copy(m.userData.red ? RED : INK); m.opacity = m.userData.base * fade[k] * (dark ? .42 : .38) * (sel ? .55 : 1); }); });
  // the traced storyline: the one opened, else the one pointed at, only on the spiral
  // posts: their covers, on stems with floor marks when tilted
  const sp = stemUp.geometry.attributes, sd = stemDown.geometry.attributes, mk = marks.geometry.attributes; let ku = 0, kd = 0;
  posts.forEach((p, k) => { const [x, y, z] = pos(p), top = isTop(p), v = p.vis, pu = p.pulse;
    const cs = (.086 + .018 * flatK) * CVS * p.app * (p === sel ? 1.45 : p.i === hov ? 1.2 : 1) * (1 + .3 * pu) * (top ? 1.06 : 1); p.cs = cs; p.spr.position.set(x, y, z); p.spr.scale.set(cs || 1e-5, cs || 1e-5, 1); p.spr.material.opacity = Math.min(1, .16 + .84 * v) * Math.min(1, p.app * 1.6);
    p.pos = [x, y, z];
    const sb = top ? RED : INK, hi = (top ? .8 : .42) * (.15 + .85 * v) * p.app * tall * (1 + 1.2 * pu), lo = .12 * (.2 + .8 * v) * p.app * tall;
    const at = p.y >= 0 ? sp : sd, j = p.y >= 0 ? ku++ : kd++; at.position.array.set([x, .003, z, x, y, z], j * 6); paint(C2, sb, lo).toArray(at.color.array, j * 6); paint(C2, sb, hi).toArray(at.color.array, j * 6 + 3);
    mk.position.array.set([x, .004, z], k * 3); paint(C2, sb, (top ? .7 : .35) * (.25 + .75 * v) * p.app * tall * (1 + pu)).toArray(mk.color.array, k * 3); });
  stemUp.material.opacity = stemDown.material.opacity = marks.material.opacity = tall; stemUp.visible = stemDown.visible = marks.visible = tall > .01;
  [sp, sd, mk].forEach((a) => { a.position.needsUpdate = true; a.color.needsUpdate = true; });
  postHalo.forEach((h) => { const p = h.userData.p, s = (.09 + Math.max(0, power(p)) * .05) * p.app * (1 + .1 * Math.sin(t * 2 + p.i) + .4 * p.pulse); h.position.set(...p.pos); h.scale.set(s, s, s); h.material.opacity = (dark ? .15 : .1) * p.vis; });
  // tag badges
  tags.forEach((x) => { const [px, py, pz] = pos(x); x.pos = [px, py, pz]; const s = x.size * x.app * (x === sel ? 1.25 : 1) * (1 + .03 * Math.sin(t * 1.6 + x.ph) + .22 * x.pulse); x.spr.position.set(px, py, pz); x.spr.scale.set(s * x.tv || 1e-5, s * x.tv || 1e-5, 1); x.spr.material.opacity = Math.max(.08, x.vis) * x.tv;
    const hs = s * 3.2; x.halo.position.set(px, py, pz); x.halo.scale.set(hs || 1e-5, hs || 1e-5, 1); x.halo.material.opacity = x.hot ? (dark ? .13 : .09) * x.vis * x.tv : 0; });
  { const au = tagUp.geometry.attributes, ad = tagDown.geometry.attributes; let u = 0, d = 0;
    tags.forEach((x) => { const [px, py, pz] = x.pos, up = x.y >= 0, at = up ? au : ad, j = up ? u++ : d++, base = x.hot ? RED : INK, it = (x.hot ? .55 : .34) * (.15 + .85 * x.vis) * x.app * tall; at.position.array.set([px, .003, pz, px, py, pz], j * 6); paint(C2, base, it * .3).toArray(at.color.array, j * 6); paint(C2, base, it).toArray(at.color.array, j * 6 + 3); });
    [au, ad].forEach((a) => { a.position.needsUpdate = true; a.color.needsUpdate = true; }); tagUp.visible = tagDown.visible = tall > .01; }
  // links draw in as both ends arrive; the focus's links burn
  const focus = sel && sel.kind !== 'group' ? sel.i : -1, ideaSet = sel && sel.kind === 'group' ? new Set(sel.ps) : null;
  L.forEach((l, k) => { const a = N[l.s], b = N[l.t], g = Math.min(a.app, b.app), A = a.pos, B = b.pos; lp.set([A[0], A[1], A[2], A[0] + (B[0] - A[0]) * g, A[1] + (B[1] - A[1]) * g, A[2] + (B[2] - A[2]) * g], k * 6);
    const mine = focus >= 0 ? (l.s === focus || l.t === focus) : ideaSet ? !l.sim && (ideaSet.has(l.t) || ideaSet.has(l.s)) : false, hotEnd = Math.min(a.rank || 99, b.rank || 99) <= 10 || (a.kind === 'tag' && a.hot) || (b.kind === 'tag' && b.hot);
    const rest = flat ? 0 : l.sim ? .06 : .12, mineH = hov >= 0 && (l.s === hov || l.t === hov);
    let it = rest; if (focus >= 0 || ideaSet) it = mine ? (l.sim ? .5 : .95) : mineH ? (l.sim ? .22 : .42) : .02; else if (mineH) it = l.sim ? .22 : .42; else if (filter && !(lit(l.s) && lit(l.t))) it = flat ? 0 : .03;
    it *= Math.min(a.tv ?? 1, b.tv ?? 1); const base = hotEnd && !l.sim ? RED : INK, dimA = a.y < 0 ? 1 - .45 * tall : 1, dimB = b.y < 0 ? 1 - .45 * tall : 1; paint(C, base, it * dimA).toArray(lc, k * 6); paint(C, base, it * dimB).toArray(lc, k * 6 + 3); });
  linkGeo.attributes.position.needsUpdate = true; linkGeo.attributes.color.needsUpdate = true; linkMesh.material.opacity = 1; linkMesh.visible = true;
  // sparks run along the lit links
  const fl = focus >= 0 ? nb[focus].filter((e) => (N[e.o].tv ?? 1) > .5).map((e) => e.k) : ideaSet ? L.map((l, k) => (!l.sim && (ideaSet.has(l.t) || ideaSet.has(l.s)) ? k : -1)).filter((k) => k >= 0) : null, sk = sparks.geometry.attributes;
  spkS.forEach((s, k) => { if (fl && fl.length && !fl.includes(s.l)) s.l = fl[k % fl.length]; s.u = (s.u + s.v * dt * (fl ? 2.2 : 1)) % 1; const l = L[s.l], A = N[l.s].pos, B = N[l.t].pos, g = Math.min(N[l.s].app, N[l.t].app), u = s.u * g; sk.position.array.set([A[0] + (B[0] - A[0]) * u, A[1] + (B[1] - A[1]) * u, A[2] + (B[2] - A[2]) * u], k * 3);
    paint(C, RED, Math.sin(s.u * Math.PI) * (fl ? 1 : 0)).toArray(sk.color.array, k * 3); });
  sk.position.needsUpdate = true; sk.color.needsUpdate = true; sparks.visible = !!fl;
  // fragments: what the reader logged, orbiting their post
  [[FR_UP, fragUp], [FR_DN, fragDn]].forEach(([arr, obj]) => { const a = obj.geometry.attributes; arr.forEach((f, k) => { const p = f.p, P = p.pos, ang = f.a + t * f.w, cr = Math.cos(ang) * f.r * p.app, sr = Math.sin(ang) * f.r * p.app, lx = cr, ly = sr * Math.sin(f.tilt) * tall, lz = sr * Math.cos(f.tilt), cy = Math.cos(f.yaw), sy = Math.sin(f.yaw);
    a.position.array.set([P[0] + lx * cy - lz * sy, P[1] + ly, P[2] + lx * sy + lz * cy], k * 3); paint(C, f.hot ? RED : INK, (.3 + .4 * f.s) * p.app * p.fx).toArray(a.color.array, k * 3); }); a.position.needsUpdate = true; a.color.needsUpdate = true; obj.material.opacity = 1; obj.visible = true; });
  // floor print turns to read from the camera's side
  { const cx = camera.position.x - controls.target.x, cz = camera.position.z - controls.target.z;
    floorTexts.forEach((m) => { const u = m.userData, f = fade[u.lens]; m.visible = f > .01; if (!m.visible) return; const nearSide = Math.cos(u.a) * cx + Math.sin(u.a) * cz > 0; m.rotation.set(-Math.PI / 2, -Math.PI / 2 - u.a + (nearSide ? Math.PI : 0), 0); m.material.color.copy(u.red ? RED : INK); m.material.opacity = u.base * f * (dark ? .6 : .66) * (sel ? .45 : 1); }); }
  renderer.render(scene, camera);
  // labels: the focus and its neighbours in full, this view's captions, then a few standouts in brief; skip collisions
  const cand = [], foc = sel && sel.kind !== 'group' ? sel.i : -1, near = sel ? hood : null;
  const bottom = H - (sel ? cardH + 12 : 46), topB = hudB;
  N.forEach((x) => { const [sx, sy, sz] = screenOf(...x.pos); x.scr = [sx, sy, sz]; x.scrR = x.kind === 'tag' ? Math.abs(screenOf(x.pos[0] + x.size * .5, x.pos[1], x.pos[2])[0] - sx) : Math.abs(screenOf(x.pos[0] + x.cs * .5, x.pos[1], x.pos[2])[0] - sx); const e = lbs[x.i]; let pri = 99;
    if (x.shown) { if (x.i === hov) pri = .5; else if (near) { if (x.i === foc) pri = 0; else if (near.has(x.i)) pri = x.kind === 'tag' ? 1 : 2; }
      else if (x.kind === 'tag' && lit(x.i) && (ts(x).n >= 3 || x.hot)) pri = 3.2 + (6 - Math.min(6, ts(x).n)) * .2; else if (x.kind === 'post' && x.rank <= 3 && !filter) pri = 3; }
    if (sz > 1 || pri === 99 || sx < -40 || sx > W + 40 || sy < topB || sy > bottom) { e.style.opacity = 0; return; } cand.push({ x, e, sx, sy, pri }); });
  GL.forEach((g) => { const f = fade[g.lens]; if (f < .2) { g.e.style.opacity = 0; g.e.style.pointerEvents = 'none'; return; } const [sx, sy, sz] = screenOf(...g.pos); if (sz > 1 || sy < topB || sy > bottom || sx < 8 || sx > W - 8) { g.e.style.opacity = 0; g.e.style.pointerEvents = 'none'; return; } cand.push({ g, e: g.e, sx, sy, pri: near ? 5 : g.act ? 2.4 : 2.8, f }); });
  cand.forEach((c) => { if (c.e._on) c.pri -= .3; }); cand.sort((a, b) => a.pri - b.pri); const boxes = []; [...lbs, ...GL.map((g) => g.e)].forEach((e) => (e._was = e._on, e._on = false));
  if (!sel && tall > .5) [axHi, axMid, axLo].forEach((e) => { const m = /translate3d\(([-\d.]+)px, ([-\d.]+)px/.exec(e.style.transform || ''); if (m) boxes.push([+m[1] - 2, +m[2] - 2, (e._w || (e._w = e.offsetWidth)) + 4, 14]); });
  cand.forEach((c) => { const { e, sx, sy, pri } = c, isSel = !c.g && pri <= 0, isHov = !c.g && c.x.i === hov, brief = pri >= 3 && !c.g; if (!e._w || (c.g && !e._was) || e._sel !== isSel || e._brief !== brief || e._hov !== isHov) { e.classList.toggle('sel', isSel); e.classList.toggle('hov', isHov); e.classList.toggle('brief', brief); e._sel = isSel; e._hov = isHov; e._brief = brief; e._w = e.offsetWidth; e._h = e.offsetHeight; }
    let bx, by; if (c.g) { bx = Math.max(6, Math.min(W - 6 - e._w, sx - e._w / 2)); by = sy - e._h / 2; } else { const r = Math.max(7, (c.x.scrR || 0) * (c.x.kind === 'post' ? 1 : .6) + 4); bx = sx + r; by = sy - e._h / 2; if (bx + e._w > W - 6) bx = sx - r - e._w; bx = Math.max(6, Math.min(W - 6 - e._w, bx)); }
    const box = [bx - 2, by - 2, e._w + 4, e._h + 4]; const hidden = boxes.some((b) => box[0] < b[0] + b[2] && box[0] + box[2] > b[0] && box[1] < b[1] + b[3] && box[1] + box[3] > b[1]);
    if (hidden) { e.style.opacity = 0; if (c.g) e.style.pointerEvents = 'none'; return; } boxes.push(box); e._on = true; e.style.opacity = c.g ? (c.f * (near ? .5 : 1)).toFixed(2) : 1; if (c.g) e.style.pointerEvents = c.g.act ? 'auto' : 'none'; e.style.transform = `translate3d(${bx.toFixed(1)}px, ${by.toFixed(1)}px, 0)`; });
  { const x = hov >= 0 ? N[hov] : null, show = x && x.shown && x.scr && x.scr[2] < 1;
    if (show) { const rr = x.scrR + 6; ring.style.transform = `translate3d(${(x.scr[0] - 20).toFixed(1)}px, ${(x.scr[1] - 20).toFixed(1)}px, 0) scale(${(rr / 20).toFixed(3)})`; ring.classList.toggle('red', x.kind === 'post' ? isTop(x) : x.hot); }
    ring.classList.toggle('on', !!show); }
  // the altitude key, pinned to the floor's left edge; only the 3D view has altitude
  { const ex = -Math.min(1.25, lensR[lens] + .08), [hx, hy] = screenOf(ex, .66, 0), [mx, my] = screenOf(ex, 0, 0), [lx, ly] = screenOf(ex, -.6, 0), o = sel || tall < .5 ? 0 : .95 * tall;
    axHi.style.transform = `translate3d(${Math.max(10, hx - 14).toFixed(0)}px, ${hy.toFixed(0)}px, 0)`; axMid.style.transform = `translate3d(${Math.max(10, mx - 14).toFixed(0)}px, ${(my - 6).toFixed(0)}px, 0)`; axLo.style.transform = `translate3d(${Math.max(10, lx - 14).toFixed(0)}px, ${ly.toFixed(0)}px, 0)`; axHi.style.opacity = hy < hudB ? 0 : o; axMid.style.opacity = my < hudB ? 0 : o; axLo.style.opacity = ly < hudB ? 0 : o; }
  if (visible && !dead) raf = requestAnimationFrame(frame);
}
const io = new IntersectionObserver(([en]) => { const was = visible; visible = en.isIntersecting; if (visible && !was && !dead) { lastT = performance.now(); cancelAnimationFrame(raf); raf = requestAnimationFrame(frame); } }); io.observe(stage);
const ro = new ResizeObserver(() => { if (dead) return; size(); if (sel) measureCard(); else if (!moved) home(); }); ro.observe(stage);
(document.fonts && document.fonts.ready ? document.fonts.ready : Promise.resolve()).then(() => { if (dead) return; buildFloorPrint(); size(); setLens(lens); reading(true); home(true); raf = requestAnimationFrame(frame); });
return {
  unmount() {
    if (full) setFull(false); window.removeEventListener('wheel', onPageWheel, { capture: true }); window.removeEventListener('keydown', onKey);
    dead = true; cancelAnimationFrame(raf); io.disconnect(); ro.disconnect(); themeWatch.disconnect(); controls.dispose();
    scene.traverse((o) => { if (o.geometry) o.geometry.dispose(); const m = o.material; (Array.isArray(m) ? m : m ? [m] : []).forEach((mm) => { Object.values(mm).forEach((v) => { if (v && v.isTexture) v.dispose(); }); if (mm.uniforms && mm.uniforms.map && mm.uniforms.map.value) mm.uniforms.map.value.dispose(); mm.dispose(); }); });
    renderer.dispose(); renderer.forceContextLoss(); host.innerHTML = '';
  },
};
}
