/* The Read tab engine: the approved prototype's plain JS, mounted and torn down by ReadTab. Kept as JS (not TS)
   so the prototype code stays byte-for-byte reviewable against docs/feeder-reader/feeder-reader.html. */
/*
 * The Read tab's engine: the Read prototype's script, run imperatively against the static markup that
 * readMarkup.ts renders into the app header capsule (#rd-hdr), the page (#rd-main) and the fixed layer
 * (#rd-layer). What changed in the port, and nothing else:
 *   - every id carries an rd- prefix (other tabs share the document);
 *   - the prototype's own tab bar is the app's bottom nav ([data-fm-bottom-nav]);
 *   - the header geometry is the app capsule's (see ht/hb), and the phone/desktop breakpoint is 1023/1024px;
 *   - setCompact also drives the app header's compression (setCompressed);
 *   - stand-ins and flying copies go into the tab's fixed layer instead of <body>;
 *   - everything a mount starts (listeners, observers, timers, frames, motion, stand-ins, the sheet's scroll
 *     lock) is undone by the cleanup it returns, and mounting again renders from scratch.
 */

const SCENES = {
  bedroom: ['#80573a', '#1f140d'], night: ['#22357a', '#3b2810'], car: ['#1f5753', '#081716'], garage: ['#323b4e', '#06070a'],
  office: ['#5c7590', '#18202a'], turf: ['#45a856', '#0d3519'], restaurant: ['#b05e30', '#2a150a'], icecream: ['#f2aac2', '#7a3550'],
  studio: ['#cfcfca', '#6a6a65'], gray: ['#a0a0a0', '#141414'], home: ['#9c805f', '#30271c'], party: ['#6538b0', '#140926'],
  ui: ['#323c52', '#0b0e14'], piano: ['#7c1a1a', '#120303'], store: ['#cc6a22', '#361806'], balcony: ['#82b6dc', '#284760'],
  plain: ['#716a63', '#211e1b'], luxe: ['#47361d', '#0b0804'], counter: ['#634029', '#180f09'], street: ['#a8622c', '#221309'],
  lips: ['#d0206a', '#470920'], gold: ['#dcad4a', '#46310e'], nude: ['#dcab96', '#663f33'], mint: ['#86d0bd', '#214c44'],
  shelf: ['#f2d9dc', '#986870'], phone: ['#433366', '#0c0915'],
};

export function mountReader({ data, layer, setCompressed }) {
/* Everything below runs per mount. The prototype's code keeps its own (top-level) indentation, so its
   multi-line template strings stay byte-for-byte what they were. */
let alive = true;
const offs = [];
const listen = (t, type, fn, o) => { t.addEventListener(type, fn, o); offs.push(() => t.removeEventListener(type, fn, o)); };
/* timers and frames go through these (they shadow the globals in here), so nothing fires after cleanup */
const timers = new Set(), frames = new Set();
const setTimeout = (fn, ms) => { const id = window.setTimeout(() => { timers.delete(id); if (alive) fn(); }, ms); timers.add(id); return id; };
const clearTimeout = (id) => { timers.delete(id); window.clearTimeout(id); };
const requestAnimationFrame = (fn) => { const id = window.requestAnimationFrame((t) => { frames.delete(id); if (alive) fn(t); }); frames.add(id); return id; };
let ro = null;
let DATA = data;

/* layout test at the memory cap: ?demo=100 appends 60 placeholder reels to the first feeder */
(function () {
  if (new URLSearchParams(location.search).get('demo') !== '100') return;
  DATA = JSON.parse(JSON.stringify(data));
  const f = DATA.feeders[0], sc = Object.keys(SCENES), sr = f.series.map((x) => x.id);
  let seed = 7, d = f.posts[f.posts.length - 1].date;
  const rnd = () => (seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0) / 4294967296;
  for (let k = 0; k < 60; k++) {
    const t = new Date(d + 'T12:00:00Z'); t.setUTCDate(t.getUTCDate() + (k % 3 ? 1 : 2)); d = t.toISOString().slice(0, 10);
    f.posts.push({ id: 't' + k, date: d, rank_final: 0.5 + rnd() * 40, scene: sc[(k * 7) % sc.length], art: ['p1', 'p2', 'p3'][k % 3], hook: 'Layout test ' + (k + 1), flow: [], series: sr[k % sr.length], label: 'Layout test reel ' + (k + 1), driver: 'idea', take: 'Placeholder reel for the 100-reel layout test.' });
  }
})();

/* ---------- helpers ---------- */
const $ = (id) => document.getElementById(id);
const qa = (sel, root = document) => [...root.querySelectorAll(sel)];
const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const mqReduce = matchMedia('(prefers-reduced-motion: reduce)');
let RM = mqReduce.matches;
if (mqReduce.addEventListener) listen(mqReduce, 'change', (e) => { RM = e.matches; });
const mqPhone = matchMedia('(max-width: 1023px)');
const phone = () => mqPhone.matches;
/* the app's chrome. The header is the app's glass capsule (#rd-hdr is its content box): on a phone its top
   moves with the safe area, it is 152px tall and clipped to 68px when compressed; on desktop it is one 80px
   row whose bottom (+8) is where the sticky lens sits. The tab bar is the app's own bottom nav. */
const ht = () => $('rd-hdr').getBoundingClientRect().top;
const hb = () => { const h = $('rd-hdr'), b = h ? h.getBoundingClientRect().bottom : 0; return b ? b + 8 : 108; };
const navTop = () => { const n = document.querySelector('[data-fm-bottom-nav]'), r = n && n.getBoundingClientRect(); return r && r.height ? r.top : innerHeight - 88; };
const fine = matchMedia('(hover: hover) and (pointer: fine)').matches;
const SOFT = 'cubic-bezier(.16,.9,.2,1)';
const GLIDE = 'cubic-bezier(.33,.0,.15,1)';
const EASE_IN = 'cubic-bezier(.4, 0, 1, 1)';
const anim = (el, kf, o = {}) => (RM || !el || !el.animate ? null : el.animate(kf, { duration: o.dur ?? 460, easing: o.ease ?? SOFT, delay: o.delay ?? 0, fill: o.fill ?? 'backwards' }));
const rise = (el, o = {}) => anim(el, [{ opacity: 0, transform: `translateY(${o.y ?? 12}px)` }, { opacity: 1, transform: 'none' }], o);
const buzz = () => { try { if (navigator.vibrate && (!navigator.userActivation || navigator.userActivation.hasBeenActive)) navigator.vibrate(6); } catch (e) {} };
const MON = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const D = (iso) => new Date(iso + 'T12:00:00');
const fd = (iso) => { const d = D(iso); return `${MON[d.getMonth()]} ${d.getDate()}`; };
const span = (a, b) => { const x = D(a), y = D(b); return x.getMonth() === y.getMonth() ? `${MON[x.getMonth()]} ${x.getDate()}–${y.getDate()}` : `${fd(a)} – ${fd(b)}`; };
const med = (a) => { const s = [...a].sort((x, y) => x - y); if (!s.length) return null; const m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
const pc = (p) => Math.max(1, Math.round(p));
const band = (p) => (p <= 10 ? 3 : p <= 25 ? 2 : p <= 50 ? 1 : 0);
const plural = (n, a, b) => `${n} ${n === 1 ? a : b || a + 's'}`;
const ord = (n) => n + (['th', 'st', 'nd', 'rd'][(n % 100 - 20) % 10] || ['th', 'st', 'nd', 'rd'][n % 100] || 'th');
const IC = {
  x: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" aria-hidden="true"><path d="m4 4 8 8M12 4l-8 8"/></svg>',
  l: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M10 3.5 5.5 8 10 12.5"/></svg>',
  r: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M6 3.5 10.5 8 6 12.5"/></svg>',
  chev: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 6l4 4 4-4"/></svg>',
};

/* ---------- state ---------- */
const S = { f: 0, layer: 'read', sort: 'recent', sel: null, pick: null, drop: null, take: '', stack: [], compact: false, tr: null };
let ovOpen = false;
let bodyLock = false, bodyPrev = '';

/* ---------- model: rank in memory, beat streak, bits, runs ---------- */
const CACHE = new Map();
function model(fi) {
  if (CACHE.has(fi)) return CACHE.get(fi);
  const f = DATA.feeders[fi], n = f.posts.length;
  const runCount = n > 10 ? Math.ceil(n / 10) : 1;
  const ps = f.posts.map((p, i) => ({ ...p, i, run: runCount > 1 ? Math.floor(i / 10) + 1 : 1 }));
  const score = (p) => (p.pct_fixed != null ? p.pct_fixed : p.rank_final);
  [...ps].sort((a, b) => score(a) - score(b)).forEach((p, k) => { p.rank = k + 1; });
  ps.forEach((p) => { p.pct = p.pct_fixed != null ? p.pct_fixed : (p.rank / n) * 100; p.band = band(p.pct); });
  ps.forEach((p, i) => {
    let ripple = 0, stopper = null, shortOf = 0;
    for (let j = i - 1; j >= 0; j--) { if (ps[j].pct > p.pct) ripple++; else { stopper = ps[j]; break; } }
    for (let j = i - 1; j >= 0; j--) { if (ps[j].pct < p.pct) shortOf++; else break; }
    Object.assign(p, { ripple, stopper, shortOf });
  });
  const series = f.series.map((s) => {
    const items = ps.filter((p) => p.series === s.id);
    return items.length ? { ...s, items, med: med(items.map((p) => p.pct)), first: items[0].date, last: items[items.length - 1].date } : null;
  }).filter(Boolean).sort((a, b) => (a.id === 'oneoff') - (b.id === 'oneoff') || a.med - b.med);
  const calm = Math.max(2, Math.round(n / 8));
  const runs = Array.from({ length: runCount }, (_, k) => {
    const r = k + 1, items = ps.filter((p) => p.run === r), md = med(items.map((p) => p.rank));
    return { r, items, md, typical: Math.floor(md + 0.5), top: items.filter((p) => p.pct <= 25).length, best: [...items].sort((a, b) => a.rank - b.rank)[0], d: (f.dispatch || {})[String(r)] || { head: 'Run ' + r, body: 'Not read yet.' } };
  });
  runs.forEach((R, k) => { const P = runs[k - 1]; R.prev = P || null; const dm = P ? P.md - R.md : 0; R.dir = !P ? 'first' : dm >= calm ? 'up' : dm <= -calm ? 'down' : 'level'; });
  runs.forEach((R) => { R.tp = Math.max(1, Math.round(med(R.items.map((p) => p.pct)))); R.low = R.items.filter((p) => p.pct > 50).length; });
  const byId = Object.fromEntries(ps.map((p) => [p.id, p]));
  const reps = (f.repeats || []).map((x) => { const items = x.posts.map((id) => byId[id]).filter(Boolean).sort((a, b) => a.i - b.i); return { ...x, items, ids: new Set(x.posts), med: med(items.map((p) => p.pct)) }; });
  const Mo = { f, n, posts: ps, byId, series, sById: Object.fromEntries(series.map((s) => [s.id, s])), runs, reps, rById: Object.fromEntries(reps.map((x) => [x.id, x])) };
  CACHE.set(fi, Mo);
  return Mo;
}
const M = () => model(S.f);

/* ---------- words: plain feed language ---------- */
const RANKW = ['Below 50%', 'Top 50%', 'Top 25%', 'Top 10%'];
const RANKV = [
  (w, n, they) => `Below ${w} own middle of the last ${n}. ${they} saw it and kept scrolling.`,
  (w) => `Above ${w} middle, outside the top quarter. Did the job, didn’t travel.`,
  (w, n) => `${w[0].toUpperCase() + w.slice(1)} top quarter of the last ${n}, just under the best tenth. The solid hits.`,
  (w, n) => `The best tenth of ${w} last ${n}. The bar every new reel gets measured against.`,
];
const DRV = { idea: ['The idea', 'Idea'], moment: ['The moment', 'Moment'], faces: ['The faces', 'Faces'], craft: ['The craft', 'Craft'] };
const DRVQ = { idea: 'whether the joke itself was any good', moment: 'something already in the air', faces: 'who’s in it', craft: 'how it’s made: length, pace, format' };
const SHORT = { biz: 'Own industry', football: 'Football', crew: 'The crew', bmw: 'The BMW', food: 'Eating out', place: 'Place puns', street: 'Street', city: 'Delhi v Mumbai', brand: 'Brand deal', both: 'Both sides', solo: 'Solo', family: 'Told off', skin: 'Her day', people: 'People', eye: 'Eye hero', oneoff: 'One-off' };
const LAYERN = { read: 'Read', repeat: 'Repeat', rank: 'Rank' };
/* the lens's three tabs: Read and Repeat keep the feed newest first, Rank puts it best first */
const TABS = [['read', 'Read'], ['repeat', 'Repeat'], ['rank', 'Rank']];
const TAB_ATTR = { read: 'data-layer="read" data-sort="recent"', repeat: 'data-layer="repeat"', rank: 'data-sort="rank"' };
const lensKey = () => (S.sort === 'rank' ? 'rank' : S.layer);
const tabIx = (k) => TABS.findIndex(([v]) => v === k);
/* Top % is the app's unit: where a post sits in this account's own memory, lower is stronger */
const pcx = (p) => Math.max(1, Math.round(p.pct));
/* the shade of a run, from its typical post against his own middle (top 50%): above the middle it
   reddens continuously toward the Feed card's crimson (top 1%), below it it darkens. So top 5% and
   top 20% never share a shade, and the brightest red is earned. */
const SH_HI = [247, 24, 82], SH_MID = [84, 84, 92], SH_LO = [30, 30, 35];
const mix = (a, b, t) => a.map((x, i) => Math.round(x + (b[i] - x) * t));
function shade(v) {
  const t = Math.min(100, Math.max(1, v));
  if (t <= 50) {
    const k = Math.pow((50 - t) / 49, 0.9), g = Math.max(0, (k - 0.35) / 0.65);
    return { c: `rgb(${mix(SH_MID, SH_HI, k)})`, tx: '#fff', gc: g ? `rgba(247,24,82,${(g * 0.6).toFixed(2)})` : 'transparent', gi: (0.1 + g * 0.24).toFixed(2) };
  }
  const w = Math.min(1, (t - 50) / 35);
  return { c: `rgb(${mix(SH_MID, SH_LO, w)})`, tx: `rgba(255,255,255,${(0.9 - w * 0.36).toFixed(2)})`, gc: 'transparent', gi: '0.08' };
}
const shadeVars = (v) => { const x = shade(v); return `--c:${x.c};--tx:${x.tx};--gc:${x.gc};--gi:${x.gi}`; };
const RAMP = `linear-gradient(90deg, ${[1, 8, 15, 22, 29, 36, 43, 50, 60, 70, 85, 100].map((v) => `${shade(v).c} ${(((v - 1) / 99) * 100).toFixed(1)}%`).join(', ')})`;
const AV = ['linear-gradient(135deg, #fb7185, #7c3aed)', 'linear-gradient(135deg, #f9a8d4, #be123c)'];
const who = () => M().f.who;
const they = () => (who() === 'his' ? 'His crowd' : 'Their crowd');
const word = (p) => RANKW[p.band];
const beatShort = (p) => (p.i === 0 ? 'First in memory' : p.ripple ? `▲ beat the last ${p.ripple}` : `▼ last ${p.shortOf} did better`);
function beatLine(p) {
  const Mo = M(), q = p.stopper, many = Mo.runs.length > 1;
  if (p.i === 0) return 'First reel in memory, so there’s nothing before it to beat.';
  if (p.ripple === p.i) return `Beat all <b>${p.i}</b> reels before it. A new high for the feed.`;
  if (p.ripple > 0) return `Beat the <b>${plural(p.ripple, 'post')}</b> before it, going back through ${many && q.run !== p.run ? 'earlier runs' : 'the feed'} until <b>${esc(q.label)}</b>${many ? ` (run ${q.run}, top ${pcx(q)}%)` : ` (top ${pcx(q)}%)`}, which did better.`;
  if (p.shortOf === 1) return `<b>${esc(q.label)}</b>, right before it, did better (top ${pcx(q)}%).`;
  return `The <b>${p.shortOf}</b> reels in a row before it all did better.`;
}
const note = () => { const Mo = M(); return `Sample data. Covers are mock thumbnails built from each postcard’s on-screen hook and scene. ${Mo.f.dateNote || ''} Rank = where a reel landed at day 7 against ${Mo.f.who} own last ${Mo.n}. Takes, bits and run reads are hand-written stand-ins for the run-of-10 reader.`; };

/* ---------- covers ---------- */
function art(kind) {
  const P = (cx, s) => `<circle cx="${cx}" cy="${40 - 18 * s}" r="${4.6 * s}"/><path d="M${cx - 9 * s} 41 Q${cx - 9 * s} ${40 - 11 * s} ${cx} ${40 - 11.5 * s} Q${cx + 9 * s} ${40 - 11 * s} ${cx + 9 * s} 41Z"/>`;
  let g = '';
  if (kind === 'p1') g = P(15, 1.2);
  else if (kind === 'p2') g = P(10, 1) + P(21, 1.05);
  else if (kind === 'p3') g = P(6.5, 0.85) + P(23.5, 0.85) + P(15, 1.05);
  else if (kind === 'car') g = '<path d="M2 36 L4.5 30.5 Q7 27 12 27 L19 27 Q23 27 26 30.5 L28.5 36 Z"/><circle cx="8" cy="36.5" r="2.6" fill="#000"/><circle cx="22.5" cy="36.5" r="2.6" fill="#000"/><rect x="5" y="31" width="20" height=".8" fill="rgba(140,180,255,.6)"/>';
  else if (kind === 'product') g = '<rect x="12.5" y="17" width="5" height="18" rx="1.2"/><rect x="12" y="13" width="6" height="5" rx="1" fill="rgba(255,255,255,.35)"/>';
  else if (kind === 'arm') g = '<path d="M-2 30 Q10 24 32 26 L32 34 Q10 33 -2 38Z"/>';
  else if (kind === 'ui') g = '<rect x="4" y="16" width="14" height="4" rx="2" fill="rgba(255,255,255,.2)"/><rect x="11" y="22" width="15" height="4" rx="2" fill="rgba(251,113,133,.4)"/><rect x="4" y="28" width="10" height="4" rx="2" fill="rgba(255,255,255,.2)"/>';
  return `<svg class="art" viewBox="0 0 30 40" preserveAspectRatio="xMidYMax slice" aria-hidden="true"><g fill="rgba(0,0,0,.52)">${g}</g></svg>`;
}
function tile(p, o = {}) {
  const g = SCENES[p.scene] || SCENES.plain;
  const hook = p.hook || '', cap = hook.length > 72 ? hook.slice(0, 70) + '…' : hook;
  const still = !!o.still, tag = still ? 'div' : 'button';
  const attrs = still ? 'aria-hidden="true"' : `type="button" data-post="${p.id}" aria-label="${esc(p.label)}, top ${pcx(p)}%, ${esc(beatShort(p).replace(/[▲▼] /, ''))}"`;
  return `<${tag} ${attrs} class="tile b${p.band} ${o.cls || ''}" data-pid="${p.id}" style="--g1:${g[0]};--g2:${g[1]};${o.style || ''}"><span class="face ${p.scene === 'gray' ? 'gray' : ''}">${art(p.art)}${o.nocap ? '' : `<span class="cap"><span>${esc(cap)}</span></span>`}${o.res ? '<svg class="rl" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2.2" stroke-linejoin="round" aria-hidden="true"><rect x="3" y="3" width="18" height="18" rx="5"/><path d="M3 8.5h18M8.5 3l3 5.5M14.5 3l3 5.5" stroke-linecap="round"/><path d="M10 12.2v5l4.2-2.5z" fill="#fff" stroke="none"/></svg>' : ''}${o.badge ? `<span class="br b${p.band}">${pcx(p)}%</span>` : ''}${o.pc ? `<span class="pcv">${pcx(p)}<span>%</span></span>` : ''}${o.res ? '<span class="mk"></span>' : ''}</span><span class="ring"></span>${o.extra || ''}</${tag}>`;
}
function stack(items, o = {}) {
  const sorted = [...items].sort((a, b) => a.pct - b.pct), lead = sorted[0], rest = sorted.slice(1, 3);
  const edge = (q, c) => { const g = SCENES[q.scene] || SCENES.plain; return `<i class="e ${c}" style="--g1:${g[0]};--g2:${g[1]}"></i>`; };
  return `<span class="stack"${o.w ? ` style="--sw:${o.w}px"` : ''}>${rest[1] ? edge(rest[1], 'e2') : ''}${rest[0] ? edge(rest[0], 'e1') : ''}${tile(lead, { still: true, nocap: !o.cap, cls: 'lit' })}${o.nocount ? '' : `<b class="cnt">×${items.length}</b>`}</span>`;
}
function podium(items, o = {}) {
  const n = items.length;
  const lines = [[10, 'TOP 10%', 't10'], [25, 'TOP 25%', 't25'], [50, 'TOP 50%', '']].map(([y, l, c]) => `<span class="gl ${c}" style="--gy:${y}"><span>${l}</span></span>`).join('');
  return `<div class="pod" style="--n:${n};${o.style || ''}">${lines}${items.map((p, k) => `<div class="col" style="--y:${Math.min(100, p.pct).toFixed(1)};--k:${k}">${tile(p, { nocap: n > 6 || o.nocap, badge: n <= 6, cls: (p.id === o.cur ? 'cur ' : '') + 'lit', style: `--k:${k}` })}</div>`).join('')}</div>
    <div class="podd" style="--n:${n}">${items.map((p) => `<span class="${p.band >= 2 ? 'hi' : ''}">${n > 6 ? pcx(p) + '%' : fd(p.date)}</span>`).join('')}</div>`;
}
function fixPods(root = document) { qa('.pod', root).forEach((pod) => { const t = pod.querySelector('.tile'); if (t && t.offsetHeight) pod.style.setProperty('--th', t.offsetHeight + 'px'); }); }

/* ---------- the lens's track: three equal tabs, one indicator that only ever translates ---------- */
function tabsHTML(id) {
  const k = lensKey();
  return `<div class="tabs" id="${id}" role="group" aria-label="What the wall shows" style="--i:${tabIx(k)};--td:0ms"><span class="ind" aria-hidden="true"><span class="ind-l">${TABS.map(([, l]) => `<span>${l}</span>`).join('')}</span></span>${TABS.map(([v, l]) => `<button type="button" data-lens="${v}" ${TAB_ATTR[v]} aria-pressed="${v === k}"><span class="tl">${l}</span></button>`).join('')}</div>`;
}
/* the indicator's curve, and when along it a trip reaches a given share of its length */
const IND = [0.45, 0, 0.15, 1];
function bez(t, c) {
  // progress of cubic-bezier c at time fraction t (and, with inv, the time fraction at which it reaches progress t)
  const X = (s) => 3 * (1 - s) * (1 - s) * s * c[0] + 3 * (1 - s) * s * s * c[2] + s * s * s;
  const Y = (s) => 3 * (1 - s) * (1 - s) * s * c[1] + 3 * (1 - s) * s * s * c[3] + s * s * s;
  let lo = 0, hi = 1;
  for (let k = 0; k < 28; k++) { const m = (lo + hi) / 2; if (X(m) < t) lo = m; else hi = m; }
  return Y((lo + hi) / 2);
}
function bezInv(p, c) {
  if (p <= 0) return 0; if (p >= 1) return 1;
  let lo = 0, hi = 1;
  for (let k = 0; k < 28; k++) { const m = (lo + hi) / 2; if (bez(m, c) < p) lo = m; else hi = m; }
  return (lo + hi) / 2;
}
/* The indicator glides to its slot (a two-slot trip takes a little longer, never faster). The lit labels
   ride inside it (see .ind-l), so nothing here times a label: setting --i moves the pill and its window
   together, and a trip that is interrupted simply retargets from wherever the pill is. */
function syncTabs(el, animate) {
  if (!el) return;
  const k = lensKey(), i = tabIx(k), ind = el.querySelector('.ind');
  qa('[data-lens]', el).forEach((b) => b.setAttribute('aria-pressed', String(b.dataset.lens === k)));
  const W = ind.offsetWidth;
  if (!animate || RM || !W) { el.style.setProperty('--td', '0ms'); el.style.setProperty('--i', i); return; }
  const from = new DOMMatrixReadOnly(getComputedStyle(ind).transform).m41 / W;
  el.style.setProperty('--td', Math.round(Math.abs(i - from) * W < 1 ? 120 : 520 + 140 * Math.min(2, Math.abs(i - from))) + 'ms');
  el.style.setProperty('--i', i);
}
/* The pill row hands off in turn, never on top of itself: the row that is showing leaves first, as one
   frozen copy, quickly and the way the pill moved away from it; once it has fully gone the new pills
   come in from the side the pill moved toward, one after another, and the row starts again at its first
   pill (the scroll resets under the leaving copy, so it never jumps). */
const PILL = { OUT: 150, GAP: 20, STEP: 55, IN: 560 };
const pillKey = () => `${S.f}|${lensKey()}`;
// the scroll that shows the picked pill (or the row's start): set under the leaving copy, so nothing scrolls later
function pillScroll(row) {
  const c = row.querySelector('[aria-pressed="true"]'); if (!c) return 0;
  const pr = parseFloat(getComputedStyle(row).paddingRight) || 0, right = c.offsetLeft - row.offsetLeft + c.offsetWidth;
  return Math.max(0, right - (row.clientWidth - pr));
}
function swapPills(row, dir) {
  if (!row) return;
  const html = chipsHTML(), key = pillKey(), host = row.parentNode, older = qa(':scope > .chips.ghost', host);
  clearTimeout(row._pe); row.style.pointerEvents = '';
  if (!dir || RM || !row.getClientRects().length) { older.forEach((g) => g.remove()); row.innerHTML = html; row._k = key; row.scrollLeft = pillScroll(row); return; }
  // changed your mind before the new pills showed: the row that is leaving comes back from where it is, as it was
  const back = older.length === 1 && older[0]._k === key && qa(':scope > .chip', row).every((c) => !c.getAnimations().length || +getComputedStyle(c).opacity < 0.05) ? older[0] : null;
  if (back) {
    const cs = getComputedStyle(back), op = +cs.opacity, tf = cs.transform, sl = back.scrollLeft;
    back.getAnimations().forEach((x) => x.cancel()); back.remove();
    row.innerHTML = html; row._k = key; row.scrollLeft = sl;
    row.animate([{ opacity: op, transform: tf === 'none' ? 'none' : tf }, { opacity: 1, transform: 'none' }], { duration: Math.round(160 + 140 * (1 - op)), easing: SOFT });
    return;
  }
  // a copy that is already leaving hurries out, so it is gone before anything new arrives
  older.forEach((g) => g.getAnimations().forEach((a) => a.updatePlaybackRate(3)));
  const g = row.cloneNode(true), live = qa(':scope > .chip', row), copy = qa(':scope > .chip', g);
  g.removeAttribute('id'); g.removeAttribute('role'); g.classList.add('ghost'); g.setAttribute('aria-hidden', 'true'); g.inert = true; g._k = row._k;
  // a pill still arriving leaves from where it is, at the opacity it has reached
  live.forEach((c, j) => { if (!c.getAnimations().length) return; const cs = getComputedStyle(c); copy[j].style.opacity = cs.opacity; if (cs.transform !== 'none') copy[j].style.transform = cs.transform; });
  host.appendChild(g); g.scrollLeft = row.scrollLeft;
  row.innerHTML = html; row._k = key; row.scrollLeft = pillScroll(row);
  // the new pills take no taps until they start to show (the copy on top is only a picture)
  row.style.pointerEvents = 'none'; row._pe = setTimeout(() => { row.style.pointerEvents = ''; }, PILL.OUT + PILL.GAP);
  const a = g.animate([{ opacity: 1, transform: 'none' }, { opacity: 0, transform: `translateX(${-dir * 14}px)` }], { duration: PILL.OUT, easing: EASE_IN, fill: 'forwards' });
  a.onfinish = a.oncancel = () => g.remove();
  const w = row.clientWidth, x0 = row.scrollLeft;
  let j = 0;
  qa(':scope > .chip', row).forEach((c) => { const x = c.offsetLeft - row.offsetLeft - x0; if (x + c.offsetWidth < -20 || x > w + 40) return; anim(c, [{ opacity: 0, transform: `translateX(${dir * 26}px)` }, { opacity: 1, transform: 'none' }], { dur: PILL.IN, delay: PILL.OUT + PILL.GAP + j++ * PILL.STEP, ease: SOFT }); });
}
/* the view, in plain words, in the kicker: one line, in a window that clips it. The old line leaves
   first (up, if you moved along the track to the right), then the new one comes in from the other side. */
// on a very narrow phone the kicker keeps its point (the order) and drops the lead-in
const mqNarrow = matchMedia('(max-width: 369px)');
const DESC = () => {
  const k = lensKey(), n = mqNarrow.matches;
  if (k === 'rank') return 'By where it landed · <b>best first</b>';
  if (k === 'repeat') return n ? 'Repeats · <b>newest first</b>' : 'What keeps coming back · <b>newest first</b>';
  return n ? `${who() === 'his' ? 'His' : 'Their'} series · <b>newest first</b>` : `What ${who() === 'his' ? 'he keeps' : 'they keep'} making · <b>newest first</b>`;
};
/* A line of words that rolls over in its window: the old words leave first (up, if you moved along the track
   to the right), then the new ones come in from the other side. Changing your mind before the new words have
   shown brings the old ones back from where they are, rather than playing the whole hand-off again. */
function rollText(el, html, dir) {
  if (!el) return;
  const spans = qa(':scope > span', el), cur = spans[spans.length - 1];
  if (cur && cur._h === html && !cur._out) return;
  const mk = () => { const n = document.createElement('span'); n.innerHTML = html; n._h = html; return n; };
  if (!cur || !dir || RM) { spans.forEach((x) => x.remove()); el.appendChild(mk()); return; }
  const up = dir > 0 ? -1 : 1, now = spans.map((x) => { const cs = getComputedStyle(x); return { x, op: +cs.opacity, tf: cs.transform }; });
  const back = now.find((o) => o.x._out && o.x._h === html && o.op > 0.05);
  now.forEach(({ x, op, tf }) => {
    x.getAnimations().forEach((a) => a.cancel());
    if (back && x === back.x) return;
    if (op < 0.05) { x.remove(); return; }
    x._out = true;
    const a = x.animate([{ opacity: op, transform: tf }, { opacity: 0, transform: `translateY(${up * 5}px)` }], { duration: Math.round(140 * op) || 1, easing: EASE_IN, fill: 'forwards' });
    a.onfinish = () => x.remove();
  });
  if (back) {
    back.x._out = false; el.appendChild(back.x);
    back.x.animate([{ opacity: back.op, transform: back.tf }, { opacity: 1, transform: 'none' }], { duration: Math.round(180 + 200 * (1 - back.op)), easing: SOFT });
    return;
  }
  const n = mk(); el.appendChild(n);
  anim(n, [{ opacity: 0, transform: `translateY(${-up * 5}px)` }, { opacity: 1, transform: 'none' }], { dur: 480, delay: 160 });
}
const rollDesc = (el, dir) => rollText(el, DESC(), dir);
const hlensHTML = () => `<span class="hl-w"><i aria-hidden="true">${LAYERN.repeat}</i><span>${LAYERN[lensKey()]}</span></span>${IC.chev}`;
/* a pill's pressed state changes in place: its fill and label cross-fade, the row keeps its scroll,
   and a pill that sits half off the edge slides fully into view */
function syncPressed(reveal) {
  qa('#rd-chips, #rd-chips2').forEach((row) => {
    let on = null;
    qa('[data-pick]', row).forEach((b) => { const p = String(S.pick) === b.dataset.pick; b.setAttribute('aria-pressed', String(p)); if (p) on = b; });
    if (reveal && on) showPill(row, on);
  });
}
function showPill(row, b) {
  const r = row.getBoundingClientRect(), c = b.getBoundingClientRect(), cs = getComputedStyle(row);
  const l = r.left + parseFloat(cs.paddingLeft), rr = r.right - parseFloat(cs.paddingRight);
  const dx = c.left < l ? c.left - l : c.right > rr ? c.right - rr : 0;
  if (Math.abs(dx) > 1) row.scrollTo({ left: row.scrollLeft + dx, behavior: RM ? 'auto' : 'smooth' });
}

/* ---------- header ---------- */
function renderHeader() {
  const Mo = M();
  $('rd-hscope').textContent = '@' + Mo.f.handle;
  $('rd-hmeta').textContent = `${Mo.n} of 100 posts · ranked at day 7`;
  $('rd-hcircles').innerHTML = DATA.feeders.map((x, k) => `<button type="button" class="fc ${k === S.f ? 'on' : ''}" data-f="${k}" aria-pressed="${k === S.f}" aria-label="@${esc(x.handle)}"><span class="av" style="background:${AV[k % AV.length]}">${esc(x.handle.slice(0, 2).toUpperCase())}</span><span class="fl">${esc(x.handle)}</span></button>`).join('');
  $('rd-hlens').innerHTML = hlensHTML();
}
function setCompact(c) {
  if (!phone()) c = false;
  else if (S.sel || S.pick != null || S.drop) c = true;
  if (c === S.compact) return;
  S.compact = c;
  $('rd-hdr').classList.toggle('compact', c);
  setCompressed(c);
}
function openDrop(kind) {
  S.drop = kind; setCompact(true);
  const d = $('rd-hdrop');
  if (kind === 'take') d.innerHTML = `<p class="take">${esc(S.take)}</p>`;
  else {
    d.innerHTML = `<div class="dl"><div class="ld-k"><span class="k">Every post</span><span class="kd" id="rd-kd2"></span></div>${tabsHTML('rd-lenssw2')}<div class="pillrow"><div class="chips" id="rd-chips2" role="group"></div></div></div>`;
    rollDesc($('rd-kd2'), 0); swapPills($('rd-chips2'), 0); spyBand(true);
  }
  d.classList.add('on');
  qa(`[data-drop="${kind}"]`).forEach((t) => t.setAttribute('aria-expanded', 'true'));
}
function closeDrop() {
  if (!S.drop) return;
  qa(`[data-drop="${S.drop}"]`).forEach((t) => t.setAttribute('aria-expanded', 'false'));
  S.drop = null; $('rd-hdrop').classList.remove('on');
  lensOn(); spyBand();
}
/* the header's lens button shows once the real lens has scrolled away, and stays while its panel is open */
function lensOn() { const l = $('rd-lens'); $('rd-hdr').classList.toggle('lens-on', phone() && !!l && (S.drop === 'lens' || l.getBoundingClientRect().bottom < ht() + 68)); }
/* Rank: the band you are reading through, marked on the jump pills (in the lens and in the header's panel).
   The dividers' places are layout positions (never a transition's transform), cached until the page's
   height changes, so the check on each animation frame of a scroll only compares numbers. */
const SPY = { cur: null, tops: null, goal: null, goalUntil: 0 };
function spyBand(force) {
  let cur = null;
  if (S.sort === 'rank' && $('rd-feed')) {
    if (!SPY.tops) { const w = $('rd-wall'); SPY.tops = qa('#rd-feed > .bdiv').map((d) => [d.dataset.bdiv, w.offsetTop + d.offsetTop]); }
    const line = scrollY + 40 + (phone() ? viewTop() : hb() + $('rd-lens').offsetHeight);
    SPY.tops.forEach(([b, t]) => { if (t <= line) cur = b; });
    // scrolled to the very bottom, the last band can never reach the line: the last divider on screen is the one you are in
    if (scrollY + innerHeight >= document.documentElement.scrollHeight - 2) SPY.tops.forEach(([b, t]) => { if (t < scrollY + innerHeight - (phone() ? 140 : 80)) cur = b; });
    // a jump marks the band it is heading for at once, and keeps it marked until you scroll yourself (so a band
    // that sits too near the end to reach the line is still the one you asked for)
    if (SPY.goal != null) cur = SPY.goal;
  }
  if (cur === SPY.cur && !force) return;
  const moved = cur !== SPY.cur; SPY.cur = cur;
  qa('#rd-chips .chip.jump, #rd-chips2 .chip.jump').forEach((c) => c.classList.toggle('here', c.dataset.band === cur));
  // the marked pill slides into view if it sits off the edge of its row (not while a row is still handing off)
  if (moved && cur != null) qa('#rd-chips, #rd-chips2').forEach((row) => { const c = row.querySelector('.chip.here'); if (c && !row.parentNode.querySelector('.chips.ghost') && row.getClientRects().length) showPill(row, c); });
}

/* ---------- the trajectory: ten runs, each drawn as a bloom of its ten posts ---------- */
/* Each petal is one post, in posting order, clockwise from twelve. The longer the petal, the better it
   landed (Top %). The dashed ring is the account's own middle (top 50%); the inner one, its top quarter. */
const BLEN = (pct) => 0.2 + 0.8 * (1 - (Math.min(100, Math.max(1, pct)) - 1) / 99);
function bloom(items, o = {}) {
  const S = o.size || 60, c = S / 2, R = c - (o.pad ?? 1), r0 = o.hole ?? Math.max(4, Math.round(S * 0.13));
  const n = 10, step = (2 * Math.PI) / n, half = step * (o.fill ?? 0.37);
  const P = (a, r) => `${(c + r * Math.sin(a)).toFixed(2)} ${(c - r * Math.cos(a)).toFixed(2)}`;
  const wedge = (k, len) => { const a = k * step, a0 = a - half, a1 = a + half, rr = r0 + (R - r0) * len; return `M${P(a0, r0)}L${P(a0, rr)}A${rr.toFixed(2)} ${rr.toFixed(2)} 0 0 1 ${P(a1, rr)}L${P(a1, r0)}A${r0} ${r0} 0 0 0 ${P(a0, r0)}Z`; };
  const ring = (pct, cls) => `<circle class="${cls}" cx="${c}" cy="${c}" r="${(r0 + (R - r0) * BLEN(pct)).toFixed(2)}"/>`;
  let s = ring(50, 'b50') + (o.rings ? ring(25, 'b25') : '');
  for (let k = 0; k < n; k++) {
    const p = items[k];
    s += p ? `<path class="pt m${p.band}${o.on === p.id ? ' on' : ''}" d="${wedge(k, BLEN(p.pct))}" style="--k:${k}"${o.tap ? ` data-petal="${p.id}"` : ''}><title>${esc(p.label)}, top ${pcx(p)}%</title></path>` : `<path class="pt e" d="${wedge(k, 0.2)}"/>`;
  }
  s += `<circle class="bv" cx="${c}" cy="${c}" r="${R}"/>`;
  if (o.labels) items.forEach((p, k) => {
    if (!p) return;
    const a = k * step, rr = r0 + (R - r0) * BLEN(p.pct) + 12;
    s += `<text class="bl m${p.band}" data-pl="${p.id}" x="${(c + rr * Math.sin(a)).toFixed(1)}" y="${(c - rr * Math.cos(a) + 4).toFixed(1)}">${pcx(p)}</text>`;
  });
  return `<svg class="bloom ${o.cls || ''}" viewBox="0 0 ${S} ${S}" aria-hidden="true">${s}${o.center || ''}</svg>`;
}
function trajRun() { const Mo = M(); return Mo.runs[(S.tr || Mo.runs.length) - 1] || Mo.runs[Mo.runs.length - 1]; }
function trajHTML() {
  const Mo = M(), n = Mo.runs.length, R = trajRun();
  const slots = Array.from({ length: 10 }, (_, k) => {
    const r = k + 1, X = Mo.runs[k];
    if (X) {
      return `<button type="button" class="tj-b ${r === R.r ? 'sel' : ''} ${r === n ? 'cur' : ''}" data-tr="${r}" aria-pressed="${r === R.r}" style="${shadeVars(X.tp)}" aria-label="Run ${r}, ${span(X.items[0].date, X.items[X.items.length - 1].date)}: ${esc(X.d.head)}. Typical post top ${X.tp}%"><span class="tj-c"><i class="tj-f"></i><span class="tj-v">${X.tp}<span>%</span></span><span class="tj-u">typ</span></span><small>Run ${r}</small></button>`;
    }
    if (r === n + 1) return `<span class="tj-b next" role="img" aria-label="Run ${r} starts with the next post"><span class="tj-c"><span class="tj-v">0<span>/10</span></span></span><small>Next</small></span>`;
    return `<span class="tj-b empty" aria-hidden="true"><span class="tj-c"></span><small>Run ${r}</small></span>`;
  }).join('');
  return `<section class="traj" id="rd-traj" aria-labelledby="rd-tj-k">
    <div class="tj-hd"><span class="tj-k" id="rd-tj-k"><i></i>The trajectory</span><span class="tj-m">${n} of 10 runs · ${Mo.n} posts</span></div>
    <div class="tj-runs" role="group" aria-label="Runs, oldest first. The colour shows how strong each run was.">${slots}</div>
    <div class="tj-key" aria-hidden="true"><span class="tj-kl">Top 1%</span><span class="tj-ramp" style="--ramp:${RAMP}"><i class="mid"></i>${Mo.runs.map((X) => `<i class="tk ${X.r === R.r ? 'on' : ''}" data-r="${X.r}" style="left:${(((X.tp - 1) / 99) * 100).toFixed(1)}%"></i>`).join('')}</span><span class="tj-kl">Bottom</span></div>
    <div class="tj-body" id="rd-tjbody">${trajBody(R)}</div>
  </section>`;
}
const dur = (x) => { if (!x) return ''; if (/^\d+:\d\d$/.test(x)) return x; const n = parseFloat(x); if (!isFinite(n) || n <= 0) return ''; const v = Math.round(n); return `${Math.floor(v / 60)}:${String(v % 60).padStart(2, '0')}`; };
/* the three numbers, for one post: where it landed, its streak against the posts before it, when it went up */
function postStats(p) {
  const Mo = M(), R = Mo.runs[p.run - 1], nth = R.items.indexOf(p) + 1, q = p.stopper, prev = Mo.posts[p.i - 1] || p, L = dur(p.length);
  const st = p.i === 0 ? ['—', 'first in memory', 'nothing before it']
    : p.ripple ? [`<i class="up">▲</i>${p.ripple}`, `beat the last ${p.ripple}`, q ? `until a top ${pcx(q)}%` : 'a new high']
    : [`<i class="dn">▼</i>${p.shortOf}`, `last ${p.shortOf} did better`, p.shortOf === 1 ? `it was top ${pcx(prev)}%` : 'in a row'];
  return [
    `<b class="${p.band >= 2 ? 'r' : ''}">${pcx(p)}<small>%</small></b><span>top % at day 7</span><em>post ${nth} of ${R.items.length}</em>`,
    `<b>${st[0]}</b><span>${st[1]}</span><em>${st[2]}</em>`,
    `<b class="dt">${fd(p.date)}</b><span>posted</span><em>${L ? L + ' · ' : ''}reel</em>`,
  ];
}
/* the post card: what the post is, in the reader's words, with a way to open it or follow it on the wall */
function cardHTML(p, mode) {
  const Mo = M(), R = Mo.runs[p.run - 1], nth = R.items.indexOf(p) + 1, sr = Mo.sById[p.series], dv = DRV[p.driver];
  const head = mode === 'run'
    ? `<span class="pk r">Best of run ${R.r}</span><span class="pk-h">Tap any petal</span>`
    : `<span class="pk">Post ${nth} of ${R.items.length}</span><button type="button" class="pk-x" data-unpick aria-label="Back to run ${R.r}">Run ${R.r}${IC.x}</button>`;
  return `<div class="pci"><div class="pcover">${tile(p, { still: true, pc: true })}</div><div class="pcb"><div class="pch">${head}</div><b class="pct">${esc(p.label)}</b><p class="pctk">${esc(p.take || '')}</p></div></div>
    <div class="pcs">${sr ? `<span>${esc(sr.pill || sr.name)}</span>` : ''}${dv ? `<span><i class="sw d-${p.driver}"></i>Decided by ${dv[0].toLowerCase()}</span>` : ''}</div>
    <div class="pca"><button type="button" class="pc-open" data-open="${p.id}">Open post</button><button type="button" class="pc-trace" data-trace="${p.id}">Trace on the wall${IC.r}</button></div>`;
}
function trajBody(R) {
  const Mo = M(), h = who(), P = R.prev, d = P ? P.tp - R.tp : 0, S0 = 240;
  const move = !P ? 'first run' : R.dir === 'level' ? `level with run ${P.r}` : `<i class="${d > 0 ? 'up' : 'dn'}">${d > 0 ? '▲' : '▼'} ${Math.abs(d)}</i> vs run ${P.r}`;
  const bits = [...new Set(R.items.map((p) => p.series))].map((id) => ({ s: Mo.sById[id], a: R.items.filter((p) => p.series === id).sort((x, y) => x.pct - y.pct) })).filter((x) => x.s).sort((x, y) => x.a[0].pct - y.a[0].pct);
  const center = `<g class="bcg run"><text class="bc" x="${S0 / 2}" y="${S0 / 2 + 6}">${R.tp}<tspan dx="1" class="bcp">%</tspan></text><text class="bcs" x="${S0 / 2}" y="${S0 / 2 + 22}">TYPICAL</text></g><g class="bcg post"><text class="bc" id="rd-bcpv" x="${S0 / 2}" y="${S0 / 2 + 6}"></text><text class="bcs" x="${S0 / 2}" y="${S0 / 2 + 22}">THIS POST</text></g>`;
  const runL = [
    `<b class="${R.top >= 3 ? 'r' : ''}">${R.top}<small>/${R.items.length}</small></b><span>in ${h} top 25%</span><em>${P ? `run ${P.r}: ${P.top}` : 'first run'}</em>`,
    `<b>${R.low}<small>/${R.items.length}</small></b><span>in ${h} bottom half</span><em>${P ? `run ${P.r}: ${P.low}` : 'first run'}</em>`,
    `<b class="${R.best.band >= 2 ? 'r' : ''}">${pcx(R.best)}<small>%</small></b><span>best post</span><em>${esc(R.best.label)}</em>`,
  ];
  return `<div class="tr-hd"><p class="tj-head">${esc(R.d.head)}</p><span class="tr-meta">Run ${R.r} · ${span(R.items[0].date, R.items[R.items.length - 1].date)} · ${move}</span></div>
    <div class="tr-main">
      <div class="tr-bloom" id="rd-trbloom">${bloom(R.items, { size: S0, pad: 20, hole: 44, rings: 1, labels: true, tap: true, center, cls: 'big' })}</div>
      <div class="tr-stats" id="rd-trstats">${runL.map((x, k) => `<div class="tsc" style="--i:${k}"><div class="tsl run"${k === 2 ? ` data-petal="${R.best.id}"` : ''}>${x}</div><div class="tsl post" aria-hidden="true"></div></div>`).join('')}</div>
    </div>
    <div class="tr-card" id="rd-trcard" aria-live="polite">${cardHTML(R.best, 'run')}</div>
    <p class="tj-sup ${R.d.body.length > 260 ? 'clamp' : ''}" id="rd-trsup">${esc(R.d.body)}</p>${R.d.body.length > 260 ? '<button type="button" class="tj-more" data-more aria-expanded="false">More</button>' : ''}
    <div class="tr-mix"><span class="k">What it was made of</span><div class="rs-chips">${bits.map(({ s, a }) => `<button type="button" class="mx" data-pickbit="${s.id}" aria-pressed="false" aria-label="${esc(s.pill || s.name)}: ${a.map((p) => 'top ' + pcx(p) + '%').join(', ')}. Light it up on the wall."><span class="cl"><span>${esc(s.pill || s.name)}</span><span aria-hidden="true">${esc(s.pill || s.name)}</span></span>${a.map((p) => `<em class="m${p.band}"><span>${pcx(p)}%</span><span aria-hidden="true">${pcx(p)}%</span></em>`).join('')}</button>`).join('')}</div></div>
    <div class="tr-foot"><button type="button" class="tj-go" data-wallrun="${R.r}">Its ${R.items.length} posts on the wall${IC.chev}</button></div>`;
}
/* tap a petal: the post steps forward in the bloom, the centre and the three numbers become that post's,
   and its card fills in below, so you can read what it is without leaving the top. Tap it again (or the
   bloom, or Esc) to go back to the run. */
function pickPetal(id) {
  const box = $('rd-trbloom'); if (!box) return;
  if ((box.dataset.on || '') === id) { unpickPetal(); return; }
  const p = M().byId[id]; if (!p) return;
  buzz();
  const was = box.classList.contains('picking');
  box.dataset.on = id; box.classList.add('picking');
  qa('.pt[data-petal]', box).forEach((pt) => pt.classList.toggle('on', pt.dataset.petal === id));
  qa('.bl[data-pl]', box).forEach((t) => t.classList.toggle('on', t.dataset.pl === id));
  const cv = $('rd-bcpv'); cv.innerHTML = `${pcx(p)}<tspan dx="1" class="bcp">%</tspan>`;
  if (was && !RM) anim(cv, [{ opacity: 0 }, { opacity: 1 }], { dur: 320 });
  const st = $('rd-trstats'), L = postStats(p);
  qa('.tsl.post', st).forEach((el, k) => { el.innerHTML = L[k]; if (was && !RM) rise(el, { y: 6, dur: 380, delay: k * 45 }); });
  st.classList.add('post');
  setCard(p, 'post', box.querySelector(`.pt[data-petal="${id}"]`));
  if (phone()) {
    const c = $('rd-trcard').getBoundingClientRect(), bot = navTop() - 14;
    const pt = box.querySelector(`.pt[data-petal="${id}"]`).getBoundingClientRect(), room = pt.top - ($('rd-hdr').getBoundingClientRect().bottom + 10);
    const need = Math.min(c.bottom - bot, room);
    if (need > 8) quietTo(scrollY + need);
  }
}
function unpickPetal() {
  const box = $('rd-trbloom'); if (!box || !box.classList.contains('picking')) return;
  buzz(); box.dataset.on = ''; box.classList.remove('picking');
  qa('.pt.on, .bl.on', box).forEach((x) => x.classList.remove('on'));
  $('rd-trstats').classList.remove('post');
  setCard(trajRun().best, 'run');
}
function setCard(p, mode, from) {
  const el = $('rd-trcard'); if (!el) return;
  const html = cardHTML(p, mode);
  if (el._h === html) return;
  el._h = html; el.innerHTML = html;
  if (RM) return;
  const cv = el.querySelector('.pcover');
  if (from) flyPetal(from, cv);
  anim(cv, [{ opacity: 0, transform: 'scale(.92) translateY(6px)' }, { opacity: 1, transform: 'none' }], { dur: 560, delay: from ? 140 : 0 });
  qa('.pch, .pct, .pctk, .pcs', el).forEach((x, k) => rise(x, { y: 8, dur: 460, delay: 60 + k * 45 }));
}
/* the picked petal's colour lifts off and settles into the card's cover; page coordinates, so a scroll can't strand it */
function flyPetal(pt, cv) {
  if (RM || !pt || !cv) return;
  const a = pt.getBoundingClientRect(), b = cv.getBoundingClientRect();
  if (!a.width || !b.width) return;
  const g = document.createElement('div'); g.className = 'pfly';
  g.style.cssText = `left:${b.left + scrollX}px;top:${b.top + scrollY}px;width:${b.width}px;height:${b.height}px;background:${getComputedStyle(pt).fill}`;
  layer.appendChild(g);
  const an = g.animate([{ transform: `translate(${a.left - b.left}px, ${a.top - b.top}px) scale(${a.width / b.width}, ${a.height / b.height})`, opacity: 0.95 }, { opacity: 0.85, offset: 0.6 }, { transform: 'none', opacity: 0 }], { duration: 600, easing: SOFT });
  an.onfinish = an.oncancel = () => g.remove();
}
/* pick another run: its bloom and read settle in where the last ones were */
function setTr(r, scrollUp) {
  const Mo = M(), cur = trajRun();
  if (!Mo.runs[r - 1]) return;
  if (scrollUp) { const t = $('rd-traj'), top = phone() ? $('rd-hdr').getBoundingClientRect().top + 76 : hb() + 8; quietTo(scrollY + t.getBoundingClientRect().top - top); }
  if (cur.r === r) { if (!scrollUp) wallRun(r); return; }
  buzz(); S.tr = r;
  qa('#rd-traj .tj-b[data-tr]').forEach((b) => { const on = +b.dataset.tr === r; b.classList.toggle('sel', on); b.setAttribute('aria-pressed', String(on)); });
  qa('#rd-traj .tk').forEach((k) => k.classList.toggle('on', +k.dataset.r === r));
  const body = $('rd-tjbody');
  const swap = () => flipWall(() => { body.innerHTML = trajBody(Mo.runs[r - 1]); }, () => { if (RM) return; qa(':scope > *', body).forEach((el, k) => rise(el, { y: 8, dur: 480, delay: k * 45 })); growBloom(body); });
  if (RM) { swap(); return; }
  const a = body.animate([{ opacity: 1 }, { opacity: 0 }], { duration: 150, easing: 'cubic-bezier(.4, 0, 1, 1)', fill: 'forwards' });
  a.onfinish = () => { if (!alive) return; swap(); a.cancel(); };
}
/* petals grow out from the centre, one after another in posting order */
function growBloom(root, d0 = 0) {
  if (RM) return;
  qa('.bloom .pt:not(.e)', root).forEach((pt) => anim(pt, [{ transform: 'scale(.15)', opacity: 0 }, { transform: 'none', opacity: 1 }], { dur: 700, delay: d0 + (+pt.style.getPropertyValue('--k') || 0) * 55, ease: 'cubic-bezier(.2, .8, .2, 1)' }));
}
/* from the read at the top to the run's posts on the wall */
function wallRun(r) {
  if (S.sort !== 'recent') setSort('recent');
  const b = document.querySelector(`.rbk[data-rbk="${r}"]`); if (!b) return;
  const top = phone() ? $('rd-hdr').getBoundingClientRect().top + 76 : hb() + $('rd-lens').offsetHeight + 8;
  quietTo(scrollY + b.getBoundingClientRect().top - top);
  if (RM) return;
  setTimeout(() => qa('#rd-feed .tile[data-post]').filter((t) => M().byId[t.dataset.post].run === r).forEach((t, k) => anim(t, [{ opacity: 0, transform: 'translateY(10px)' }, { opacity: rest(t), transform: 'none' }], { dur: 520, delay: k * 35 })), 380);
}
/* anything that changes height above the wall: snapshot, change, then let the wall glide to its new place.
   The glide is added on top of whatever else is moving a piece (composite: add), so it can never fight an
   order switch that is still settling over the same posts. */
function flipWall(change, after, ease = GLIDE, dur = 0) {
  const T = (dy) => (typeof dur === 'function' ? dur() : dur) || Math.min(700, 420 + Math.abs(dy) * 0.5);
  if (RM) { change(); if (after) after(); return; }
  const vh = innerHeight, near = (r) => r.height > 0 && r.bottom > -80 && r.top < vh + 80;
  const snap = new Map(qa('#rd-lens, #rd-ins, .lens-head, #rd-feed > *').map((el) => [el, el.getBoundingClientRect()]));
  change();
  // everything that is on screen before or after moves together, so nothing arriving from off screen lands early
  snap.forEach((a, el) => {
    const b = el.getBoundingClientRect(), dy = a.top - b.top;
    if (!a.height || !b.height || Math.abs(dy) <= 1 || (!near(a) && !near(b))) return;
    el.animate([{ transform: `translateY(${dy}px)` }, { transform: 'translateY(0px)' }], { duration: T(dy), easing: ease, composite: 'add', id: 'fw' });
  });
  if (after) after();
}
/* ---------- the lens over the wall: Read · Repeat · Rank, and the pills for whichever is on ---------- */
function lensHTML() {
  return `<div class="lens-head"><span class="tj-k"><i></i>Every post</span><span class="kd" id="rd-kd"></span></div><div class="lens" id="rd-lens">${tabsHTML('rd-lenssw')}<div class="pillrow"><div class="chips" id="rd-chips" role="group"></div></div></div><div class="ins" id="rd-ins" hidden></div>`;
}
function chipsHTML() {
  const Mo = M(), pr = (v) => String(S.pick) === String(v);
  const bars = (items) => `<span class="mp" aria-hidden="true">${items.map((p) => `<i class="m${p.band}"></i>`).join('')}</span>`;
  const lab = (t) => `<span class="cl"><span>${t}</span><span aria-hidden="true">${t}</span></span>`;
  if (S.sort === 'rank') return [3, 2, 1, 0].map((b) => { const k = Mo.posts.filter((p) => p.band === b).length; return k ? `<button type="button" class="chip jump${SPY.cur === String(b) ? ' here' : ''}" data-band="${b}" aria-label="Jump to ${RANKW[b]}, ${plural(k, 'post')}"><i class="sw m${b}"></i>${lab(RANKW[b])}<em>${k}</em>${IC.chev.replace('<svg ', '<svg class="jv" ')}</button>` : ''; }).join('');
  if (S.layer === 'repeat') return Mo.reps.map((x) => `<button type="button" class="chip" data-pick="${x.id}" aria-pressed="${pr(x.id)}">${bars(x.items)}${lab(esc(x.name))}</button>`).join('');
  return Mo.series.map((x) => `<button type="button" class="chip" data-pick="${x.id}" aria-pressed="${pr(x.id)}">${bars(x.items)}${lab(esc(x.pill || x.name))}</button>`).join('');
}
function pickedSet() {
  if (S.pick == null || S.sort !== 'recent') return null;
  const Mo = M();
  return S.layer === 'repeat' ? Mo.rById[S.pick] || null : Mo.sById[S.pick] || null;
}
function insHTML() {
  const x = pickedSet(); if (!x) return '';
  return `<div class="ins-hd"><b>${esc(x.pill || x.name)}</b><span>${plural(x.items.length, 'post')}</span><button type="button" class="rd-x" data-clear aria-label="Clear">${IC.x}</button></div><p>${esc(x.say || x.take || '')}</p><div class="ins-film">${x.items.map((p) => `<button type="button" class="ifm" data-sel="${p.id}" aria-label="${esc(p.label)}, top ${pcx(p)}%. Trace it.">${tile(p, { still: true, nocap: true, pc: true })}<span class="ifm-t">${esc(p.label)}</span></button>`).join('')}</div>`;
}
/* The insight card under the pills. Opening, it comes down with the gap: the wall glides down and the card
   slides out from under the pill row on the same curve, its bottom edge riding just above the wall, so the
   opening is never seen empty and the card never covers the pills. Closing is the same trip back up: the
   card slides under the pills while the wall closes up behind it. Picking another pill while it is open
   keeps the shell: the words cross-fade, and the shell stretches with the wall to the new height. While
   the wall is still turning over (an order switch), the card waits for it to settle. */
const INS = { wait: 0, settle: 0 };
const OPEN = [0.25, 1, 0.3, 1], CLOSE_EASE = 'cubic-bezier(.22, .75, .25, 1)';
const fillIns = (el, html) => { el.innerHTML = html ? `<div class="ins-in"><span class="ins-bg" aria-hidden="true"></span>${html}</div>` : ''; };
function settleIns(el) {
  [el, ...el.querySelectorAll('*')].forEach((x) => own(x).forEach((a) => a.cancel()));
  qa(':scope > .ins-old', el).forEach((n) => n.remove()); el.classList.remove('drw');
}
function renderIns(mode) {
  const el = $('rd-ins'); if (!el) return;
  const html = insHTML();
  if (html === (el._h ?? '')) return;
  clearTimeout(INS.wait);
  // 'place': something else animates the change (the order switch), or there is no motion
  if (mode === 'place' || RM) { settleIns(el); el._h = html; fillIns(el, html); el.hidden = !html; INS.settle = 0; return; }
  const left = MORPH.until - performance.now();
  if (left > 0) { INS.wait = setTimeout(() => renderIns(), left + 30); return; }
  el._h = html;
  const inn = !el.hidden && el.querySelector(':scope > .ins-in');
  if (!inn) { settleIns(el); if (html) openIns(el, html); return; }
  if (!html) closeIns(el, inn); else swapIns(el, inn, html);
}
function openIns(el, html) {
  // a card still sliding away hurries, so the two never share the spot for long
  qa('.ins-g').forEach((g) => g.getAnimations({ subtree: true }).forEach((a) => a.updatePlaybackRate(3)));
  let grow = 0, dur = 0;
  flipWall(() => { fillIns(el, html); el.hidden = false; grow = el.offsetHeight + 16; dur = Math.round(Math.min(560, 360 + grow * 0.35)); }, null, `cubic-bezier(${OPEN})`, () => dur);
  INS.settle = performance.now() + dur;
  const inn = el.firstElementChild; el.classList.add('drw');
  const a = inn.animate([{ opacity: 0, transform: `translateY(${-grow}px)` }, { offset: 0.3, opacity: 1 }, { opacity: 1, transform: 'none' }], { duration: dur, easing: `cubic-bezier(${OPEN})`, fill: 'backwards' });
  a.onfinish = () => el.classList.remove('drw');
}
function closeIns(el, inn) {
  const r = el.getBoundingClientRect(), h = el.offsetHeight, grow = h + 16, dur = Math.round(Math.min(560, 380 + grow * 0.3));
  // the card leaves the layout at once (so whatever measures next sees the closed page), and slides away in a
  // stand-in box laid exactly where it was; its own running motion goes with it
  const g = document.createElement('div');
  g.className = 'ins ins-g drw'; g.inert = true; g.setAttribute('aria-hidden', 'true');
  Object.assign(g.style, { left: r.left + scrollX + 'px', top: r.top + scrollY + 'px', width: r.width + 'px', height: h + 'px' });
  holdAnchor(dur + 200);
  flipWall(() => {
    layer.appendChild(g); g.appendChild(inn); qa(':scope > .ins-old', el).forEach((n) => g.appendChild(n));
    settleIns(el); el.hidden = true; el.innerHTML = '';
  }, null, CLOSE_EASE, () => dur);
  INS.settle = performance.now() + dur;
  inn.animate([{ transform: 'translateY(0px)' }, { transform: `translateY(${-grow}px)` }], { duration: dur, easing: CLOSE_EASE, composite: 'add', fill: 'forwards' });
  const f = g.animate([{ opacity: 1 }, { offset: 0.3, opacity: 1 }, { opacity: 0 }], { duration: dur, easing: 'linear', fill: 'forwards' });
  f.onfinish = f.oncancel = () => g.remove();
}
function swapIns(el, inn, html) {
  const h0 = el.offsetHeight, tf = getComputedStyle(inn).transform;
  // what was showing leaves as one frozen copy laid over the shell (a copy still leaving hurries out)
  qa(':scope > .ins-old', el).forEach((n) => n.getAnimations().forEach((a) => a.updatePlaybackRate(3)));
  const kids = qa(':scope > :not(.ins-bg)', inn), old = inn.cloneNode(true);
  old.classList.add('ins-old'); old.inert = true; old.setAttribute('aria-hidden', 'true');
  old.querySelector('.ins-bg').remove();
  qa(':scope > *', old).forEach((c, j) => { const op = +getComputedStyle(kids[j]).opacity; if (op < 0.999) c.style.opacity = op; });
  if (tf !== 'none') old.style.transform = tf;
  const sl = (inn.querySelector('.ins-film') || {}).scrollLeft || 0;
  let dh = 0, dur = 0;
  flipWall(() => { kids.forEach((c) => c.remove()); inn.insertAdjacentHTML('beforeend', html); dh = el.offsetHeight - h0; dur = Math.round(Math.min(520, 340 + Math.abs(dh) * 0.4)); }, null, `cubic-bezier(${OPEN})`, () => dur);
  INS.settle = performance.now() + dur;
  el.appendChild(old);
  const of = old.querySelector('.ins-film'); if (of) of.scrollLeft = sl;
  const a = old.animate([{ opacity: 1 }, { opacity: 0, transform: `${tf === 'none' ? '' : tf + ' '}translateY(-4px)` }], { duration: 120, easing: LEAVE, fill: 'forwards' });
  a.onfinish = a.oncancel = () => old.remove();
  qa(':scope > :not(.ins-bg)', inn).forEach((c, k) => c.animate([{ opacity: 0, transform: 'translateY(6px)' }, { opacity: 1, transform: 'none' }], { duration: 460, delay: 130 + k * 40, easing: SOFT, fill: 'backwards' }));
  // the shell stays and stretches (or shrinks) with the wall, its bottom edge riding just above it
  if (Math.abs(dh) > 1) inn.querySelector('.ins-bg').animate([{ transform: `scaleY(${h0 / (h0 + dh)})` }, { transform: 'none' }], { duration: dur, easing: `cubic-bezier(${OPEN})` });
}

function renderLens(animate, dir = 0) {
  const go = !!animate && !RM;
  syncTabs($('rd-lenssw'), go); syncTabs($('rd-lenssw2'), go);
  swapPills($('rd-chips'), go ? dir : 0); swapPills($('rd-chips2'), go ? dir : 0);
  rollDesc($('rd-kd'), go ? dir || 1 : 0); rollDesc($('rd-kd2'), go ? dir || 1 : 0);
  spyBand(true);
  const lab = S.sort === 'rank' ? 'Jump to a band' : S.layer === 'repeat' ? 'Light up a repeat' : 'Light up a series';
  qa('#rd-chips, #rd-chips2').forEach((r) => r.setAttribute('aria-label', lab));
  const hw = $('rd-hlens').querySelector('.hl-w'); if (hw) rollText(hw, LAYERN[lensKey()], go ? dir || 1 : 0); else $('rd-hlens').innerHTML = hlensHTML();
}
function goBand(b) {
  buzz();
  const d = document.querySelector(`#rd-feed > .bdiv[data-bdiv="${b}"]`); if (!d) return;
  const top = phone() ? $('rd-hdr').getBoundingClientRect().top + 76 : hb() + $('rd-lens').offsetHeight + 8;
  const dy = d.getBoundingClientRect().top - top;
  SPY.goal = String(b); SPY.goalUntil = performance.now() + 250;
  quietTo(scrollY + dy);
  spyBand(true);
  if (S.drop) closeDrop();
}

/* A band's read on a phone (what's in it, why it lands here) opens under its headline: the posts below
   glide down, and the read comes in line by line right behind them, each line as soon as the posts have
   cleared it, so the opening is never seen empty. Closing, the read lets go first, then the posts close up. */
function toggleBand(b) {
  const d = document.querySelector(`#rd-feed > .bdiv[data-bdiv="${b}"]`), x = d && d.querySelector('.bd-x'), btn = d && d.querySelector('.bd-more');
  if (!x || !btn) return;
  const open = !d._open; d._open = open; buzz();
  const set = (v) => { d.toggleAttribute('data-open', v); btn.setAttribute('aria-expanded', String(v)); };
  const parts = [...x.children], laid = d.hasAttribute('data-open');
  if (RM) { set(open); return; }
  if (open && laid) {
    // reopened while its read was still letting go: the read comes back from where it is
    parts.forEach((c) => { const op = +getComputedStyle(c).opacity; own(c).forEach((a) => a.cancel()); c.animate([{ opacity: op }, { opacity: 1 }], { duration: 260, easing: SOFT }); });
    return;
  }
  parts.forEach((c) => own(c).forEach((a) => a.cancel())); qa('.mw-lines', x).forEach(unlines);
  if (!open) {
    const a = parts.map((c) => c.animate([{ opacity: 1, transform: 'none' }, { opacity: 0, transform: 'translateY(-4px)' }], { duration: 160, easing: EASE_IN, fill: 'forwards' }));
    a[0].onfinish = () => { if (!alive || d._open) return; flipWall(() => { set(false); a.forEach((z) => z.cancel()); }, null, CLOSE_EASE); };
    return;
  }
  const b0 = d.getBoundingClientRect().bottom;
  let G = 0, dur = 0;
  flipWall(() => { set(true); G = d.getBoundingClientRect().bottom - b0; dur = Math.round(Math.min(640, 380 + G * 0.4)); }, null, `cubic-bezier(${OPEN})`, () => dur);
  if (G <= 0) return;
  const units = [];
  parts.forEach((c) => { const ws = c.tagName === 'P' ? lineIn(c) : null; if (ws) ws.forEach((w) => units.push({ el: w.el, r: w.r, win: c })); else units.push({ el: c, r: c.getBoundingClientRect() }); });
  units.forEach((u) => {
    const at = Math.round(dur * bezInv(clamp01((u.r.bottom + 4 - b0) / G), OPEN));
    u.el.animate([{ opacity: 0, transform: 'translateY(6px)' }, { opacity: 1, transform: 'none' }], { duration: 520, delay: at, easing: SOFT, fill: 'backwards' });
  });
  new Set(units.filter((u) => u.win).map((u) => u.win)).forEach((p) => { const k = p._lk; Promise.allSettled(qa(':scope > .mw-w', p).flatMap((w) => w.getAnimations()).map((a) => a.finished)).then(() => { if (alive && p._lk === k) unlines(p); }); });
}

/* ---------- the wall ---------- */
function wtile(p) {
  const tags = `<span class="tg tb b${p.band}">${pcx(p)}%</span>${p.i > 0 ? `<span class="tg tr ${p.ripple ? 'up' : 'dn'}">${p.ripple ? '▲' + p.ripple : '▼' + p.shortOf}</span>` : ''}`;
  return tile(p, { res: true, extra: tags });
}
function breakerHTML(R) {
  const Mo = M(), latest = R.r === Mo.runs.length && Mo.runs.length > 1, d = R.prev ? R.prev.tp - R.tp : 0;
  const delta = R.dir === 'first' ? 'first run' : R.dir === 'level' ? `level with run ${R.prev.r}` : `<i class="${d > 0 ? 'up' : 'dn'}">${d > 0 ? '▲' : '▼'} ${Math.abs(d)}</i> vs run ${R.prev.r}`;
  return `<div class="rbk" data-rbk="${R.r}">
    <button type="button" class="rbk-hd" data-tropen="${R.r}" aria-label="Run ${R.r}, ${esc(R.d.head)}, typical post top ${R.tp}%. Open its read at the top.">
      <span class="rbk-bk" style="${shadeVars(R.tp)}"><b>${R.tp}<span>%</span></b></span>
      <span class="rbk-l"><span class="k">${latest ? '<i class="live" aria-hidden="true"></i>' : ''}Run ${R.r} · ${span(R.items[0].date, R.items[R.items.length - 1].date)}</span><b>${esc(R.d.head)}</b></span>
      <span class="rbk-r">${delta}</span>
    </button>
  </div>`;
}
function feedHTML() {
  const Mo = M(), W = Mo.f.bands || {};
  const bdivs = [3, 2, 1, 0].map((b) => { const k = Mo.posts.filter((p) => p.band === b).length; return k ? `<div class="bdiv" data-bdiv="${b}"><div class="bd-hd"><span class="bd-n"><small>${b ? 'Top' : 'Below'}</small>${[50, 50, 25, 10][b]}<i>%</i></span><span class="sub">${plural(k, 'post')}</span>${W[b] ? `<button type="button" class="bd-more" data-bdmore="${b}" aria-expanded="false" aria-label="Why ${RANKW[b]}: what’s in it and why it lands here">Why here${IC.chev}</button>` : ''}</div>${W[b] ? `<p class="bd-h">${esc(W[b].head)}</p><div class="bd-x"><span class="k">What’s in it</span><p>${esc(W[b].what)}</p><span class="k">Why it lands here</span><p>${esc(W[b].why)}</p></div>` : ''}</div>` : ''; });
  return Mo.runs.map(breakerHTML).join('') + bdivs.join('') + Mo.posts.map(wtile).join('');
}
function applySort() {
  const feed = $('rd-feed'); if (!feed) return;
  const Mo = M(), posted = S.sort === 'recent';
  const wall = $('rd-wall'); wall.classList.toggle('S-recent', posted); wall.classList.toggle('S-rank', !posted);
  const T = new Map(qa('.tile[data-post]', feed).map((t) => [t.dataset.post, t])), order = [];
  if (posted) for (let r = Mo.runs.length; r >= 1; r--) { order.push(feed.querySelector(`[data-rbk="${r}"]`)); Mo.posts.filter((p) => p.run === r).reverse().forEach((p) => order.push(T.get(p.id))); }
  else for (let b = 3; b >= 0; b--) { const a = Mo.posts.filter((p) => p.band === b).sort((x, y) => x.pct - y.pct); if (!a.length) continue; order.push(feed.querySelector(`[data-bdiv="${b}"]`)); a.forEach((p) => order.push(T.get(p.id))); }
  order.forEach((x) => x && feed.appendChild(x));
  feed.classList.toggle('ranked', !posted);
  SPY.tops = null;
}
/* ---------- changing the order: the wall turns over under a lens that stays put ----------
   Snapshot what is on screen as it looks right now (mid-switch included), change the order, then: a post
   that is on screen in both orders and only a short way off glides there (same surface, so it moves).
   The new order arrives in one calm sweep down the screen, left to right within a row and never earlier
   than anything before it in reading order. The old order leaves just ahead of it: each old piece holds
   its place until something new needs that spot, then lets go quickly, so the wall is never empty and old
   and new never share a pixel. Breakers, dividers and the insight card that drop out of the layout leave
   as stand-in copies, a divider part by part. If you were down inside the wall, the new order opens at
   its top, under cover. Tapping another tab mid-sweep starts the next sweep from exactly what is showing. */
const MW = { L: 140, SW: 440, D0: 150, ROW: 70, IN: 640, UP: 6, RISE: 12 };
const LEAVE = 'cubic-bezier(.3, 0, .6, 1)';
const MORPH = { until: 0, anchor: 0, anchorEnd: 0 };
const clamp01 = (v) => Math.min(1, Math.max(0, v));
/* a band divider moves in its parts: the number, the headline, and each label and paragraph of its read */
/* a paragraph of a band's read moves line by line; the headline always moves as one piece */
const byLine = (c) => c.tagName === 'P' && !c.classList.contains('bd-h');
const bparts = (el) => [...el.children].flatMap((c) => (c.classList.contains('bd-x') ? (getComputedStyle(c).display === 'none' ? [] : [...c.children]) : [c]));
/* while the order changes, the browser must not drag the page along with whichever post it was anchoring to */
function holdAnchor(ms) {
  const set = (v) => { document.documentElement.style.overflowAnchor = v; const w = $('rd-wall'); if (w) w.style.overflowAnchor = v; };
  // a shorter hold never cuts a longer one short
  const end = performance.now() + ms; if (MORPH.anchorEnd > end && MORPH.anchor) return;
  MORPH.anchorEnd = end;
  set('none'); clearTimeout(MORPH.anchor); MORPH.anchor = setTimeout(() => { MORPH.anchor = 0; MORPH.anchorEnd = 0; set(''); }, ms);
}
/* where the wall can be seen from: under the header, or under the header's panel while it is open */
function viewTop() {
  if (!phone()) return hb() - 8;
  if (S.drop === 'lens') return $('rd-hdrop').getBoundingClientRect().bottom;
  return $('rd-hdr').getBoundingClientRect().top + (S.compact ? 68 : 152);
}
// script-made motion on a piece of the wall (the CSS lit wave and transitions are left alone)
const own = (el) => el.getAnimations().filter((a) => !(window.CSSTransition && a instanceof CSSTransition) && !(window.CSSAnimation && a instanceof CSSAnimation));
function morphWall(change) {
  const lens = $('rd-lens'), lh = document.querySelector('.lens-head'), wall = $('rd-wall');
  const panel = phone() && S.drop === 'lens' ? $('rd-hdrop') : null;
  const inside = !!lens && (phone() ? lens.getBoundingClientRect().bottom < ht() + 68 : lh.getBoundingClientRect().bottom < hb());
  const below = () => { const i = $('rd-ins'); return (i && !i.hidden ? i : wall).getBoundingClientRect().top; };
  // down inside the wall, the new order opens at its top: right under the header's panel if you switched from it
  // (the panel stays open over the real lens), right under the stuck lens on desktop (it stays stuck at hb()), and
  // otherwise with the lens's kicker just under the header
  const jump = () => {
    if (!inside) return false;
    hdrLock = performance.now() + 900; acc = 0;
    if (phone()) setCompact(true);
    const dy = panel ? below() - (panel.getBoundingClientRect().bottom + 8) : phone() ? lh.getBoundingClientRect().top - (ht() + 80) : below() - (hb() + lens.offsetHeight);
    scrollTo({ top: Math.max(0, scrollY + dy), behavior: 'instant' });
    return true;
  };
  holdAnchor(RM ? 80 : 2200);
  if (RM) { change(); const j = jump(); MORPH.until = 0; SPY.tops = null; spyBand(true); return { jumped: j, t: 0 }; }
  const vh = innerHeight, v0 = viewTop(), vis = (b, top) => b.height > 0 && b.bottom > top + 2 && b.top < vh;
  const pool = () => qa('#rd-traj, .lens-head, #rd-lens, #rd-ins, #rd-feed > *');
  const parts = '#rd-traj, .lens-head, #rd-lens, #rd-ins, #rd-feed > *, #rd-feed > .bdiv > *, #rd-feed > .bdiv > .bd-x > *';
  // 1. what is showing, as it looks right now
  const A = new Map();
  pool().forEach((el) => {
    const b = el.getBoundingClientRect(); if (!vis(b, v0)) return;
    const cs = getComputedStyle(el), s = { b, op: +cs.opacity };
    if (el.matches('.rbk, .bdiv, #rd-ins')) {
      Object.assign(s, { node: el.cloneNode(true), pt: cs.paddingTop, bt: cs.borderTop });
      if (el.matches('.bdiv')) s.kids = bparts(el).map((c) => {
        const k = getComputedStyle(c), ops = c.classList.contains('mw-lines') ? qa(':scope > .mw-w', c).map((w) => +getComputedStyle(w).opacity) : null;
        if (ops) unlines(c);
        return { r: c.getBoundingClientRect(), op: ops ? Math.max(...ops, 0) : +k.opacity, tf: k.transform, lines: byLine(c) ? lineRects(c) : null, ops };
      });
    }
    if (s.op < 0.04 || (s.kids && !s.kids.some((k) => k.op >= 0.04))) return; // had not arrived yet: nothing to take away
    A.set(el, s);
  });
  // stand-ins still leaving from a switch this one interrupts carry on leaving from where they are, quickly
  const gone = [];
  qa('.mw-g').forEach((g) => {
    const fades = [];
    [g, ...g.querySelectorAll('*')].forEach((x) => {
      const an = x.getAnimations(); if (!an.length) return;
      const op = +getComputedStyle(x).opacity, r = x.getBoundingClientRect();
      an.forEach((a) => a.cancel()); x.style.opacity = op;
      if (op > 0.02) { fades.push(x.animate([{ opacity: op }, { opacity: 0 }], { duration: 120, easing: LEAVE, fill: 'forwards' })); gone.push({ r, end: 120 }); }
    });
    g.dataset.done = 1; dropWhen(g, fades);
  });
  qa(parts).forEach((el) => own(el).forEach((a) => a.cancel()));
  qa('.mw-lines').forEach(unlines);
  // 2. the change
  change();
  SPY.tops = null;
  const jumped = jump();
  const W = wall.getBoundingClientRect(), v1 = viewTop(), LIM = Math.min(320, vh * 0.4);
  const out = [], enter = [], hop = [], fade = [];
  pool().forEach((el) => {
    const s = A.get(el), b = el.getBoundingClientRect(), now = vis(b, v1);
    if (s && !b.height) { if (s.node) out.push({ s, ghost: true }); return; }
    if (!s && !now) return;
    if (now && wallIO) wallIO.unobserve(el);
    const dx = s ? s.b.left - b.left : 0, dy = s ? s.b.top - b.top : 0, d = Math.hypot(dx, dy);
    if (s && now) {
      if (d < 1) { const to = rest(el); if (Math.abs(s.op - to) > 0.03) fade.push({ el, from: s.op, to }); return; }
      const o = { el, s, b, dx, dy, d, both: true };
      if (d <= LIM) hop.push(o); else { out.push(o); enter.push(o); }
      return;
    }
    if (s) { out.push({ el, s, dx, dy }); return; }
    enter.push({ el, b });
  });
  // a piece glides to its new place only when its path is clear of everything arriving; otherwise it joins the sweep
  const hit = (r, q, m = 4) => r.left < q.right - m && r.right > q.left + m && r.top < q.bottom - m && r.bottom > q.top + m;
  const glides = [];
  hop.forEach((o) => {
    const a = o.s.b, path = { left: Math.min(a.left, o.b.left), right: Math.max(a.right, o.b.right), top: Math.min(a.top, o.b.top), bottom: Math.max(a.bottom, o.b.bottom) };
    if (enter.some((e) => e.el !== o.el && hit(path, e.b))) { out.push(o); enter.push(o); } else glides.push(o);
  });
  // 3. when: the new order's sweep first, then the old order lets go just ahead of whatever needs its spot
  const units = [];
  enter.forEach((o) => {
    if (o.el.matches('.bdiv') && !o.both) bparts(o.el).forEach((c) => { const ws = byLine(c) ? lineIn(c) : null; if (ws) ws.forEach((w) => units.push({ el: w.el, b: w.r, to: 1, win: c })); else units.push({ el: c, b: c.getBoundingClientRect(), to: 1 }); });
    else units.push({ ...o, src: o, to: rest(o.el) });
  });
  const olds = [], made = { ghosts: [] };
  out.forEach((o) => {
    if (o.ghost) { const g = ghost(o.s, W); made.ghosts.push(g.g); g.parts.forEach((p) => olds.push(p)); return; }
    olds.push({ o, r: o.s.b, op: o.s.op });
  });
  const tops = [...units.map((u) => u.b.top), ...olds.map((x) => x.r.top)];
  const y0 = Math.max(v1, Math.min(vh, ...tops)), span = Math.max(1, vh - y0);
  const sweep = (r) => MW.D0 + Math.round(MW.SW * clamp01((r.top - y0) / span) + MW.ROW * clamp01((r.left - W.left) / Math.max(1, W.width)));
  units.sort((x, y) => x.b.top - y.b.top || x.b.left - y.b.left);
  let last = 0;
  units.forEach((u) => {
    last = Math.max(last, sweep(u.b), ...gone.filter((g) => hit(g.r, u.b, 2)).map((g) => g.end));
    u.at = last;
    u.zone = { left: u.b.left, right: u.b.right, top: u.b.top, bottom: u.b.bottom + MW.RISE };
  });
  olds.forEach((x) => {
    x.L = MW.L;
    // the insight card is opaque: it holds until the first new row starts rising under it, then lets go a little slower
    if (x.card) { x.t = units.length ? Math.max(0, units[0].at - 30) : 0; x.L = 220; return; }
    const zone = { left: x.r.left, right: x.r.right, top: x.r.top - MW.UP, bottom: x.r.bottom };
    // a piece that reappears elsewhere also lets go in time to arrive with its new row
    const need = units.filter((u) => hit(zone, u.zone, 2) || (x.o && x.o.both && u.src === x.o)).map((u) => u.at - MW.L);
    x.t = Math.max(0, Math.min(sweep(x.r) - MW.D0, ...need));
  });
  // the lines of one paragraph leave close together (never more than 40ms apart, counting down from its first
  // to go), so no word is ever left stranded on its own
  const grp = new Map();
  olds.forEach((x) => { if (x.grp) { if (!grp.has(x.grp)) grp.set(x.grp, []); grp.get(x.grp).push(x); } });
  grp.forEach((ls) => { const t0 = Math.min(...ls.map((x) => x.t)); ls.sort((a, b) => a.li - b.li).forEach((x, i) => { x.t = Math.min(x.t, t0 + 40 * i); }); });
  // a piece that leaves one spot and arrives at another never arrives before it has left
  units.forEach((u) => { if (u.both) { const x = olds.find((y) => y.o === u.src); if (x) u.at = Math.max(u.at, x.t + MW.L); } });
  // 4. motion (everything started from here on is this switch's own, so it can be turned back as one)
  const before = new Set(document.getAnimations());
  let end = 0;
  olds.forEach((x) => {
    end = Math.max(end, x.t + x.L);
    if (x.part) { x.part.animate([{ opacity: x.op, transform: x.tf }, { opacity: 0, transform: `${x.tf} translateY(-${MW.UP}px)` }], { duration: x.L, delay: x.t, easing: LEAVE, fill: 'both' }); return; }
    const o = x.o; if (o.both) return;
    o.el.animate([{ opacity: x.op, transform: `translate(${o.dx}px, ${o.dy}px)` }, { opacity: 0, transform: `translate(${o.dx}px, ${o.dy - MW.UP}px)` }], { duration: MW.L, delay: x.t, easing: LEAVE, fill: 'backwards', id: 'mw' });
  });
  units.forEach((u) => {
    if (u.both) {
      const o = u, x = olds.find((y) => y.o === u.src), t = x ? x.t : 0, D = u.at, T = D + MW.IN, from = `translate(${o.dx}px, ${o.dy}px)`;
      o.el.animate([
        { offset: 0, opacity: o.s.op, transform: from },
        { offset: t / T, opacity: o.s.op, transform: from, easing: LEAVE },
        { offset: (t + MW.L) / T, opacity: 0, transform: `translate(${o.dx}px, ${o.dy - MW.UP}px)`, easing: 'steps(1, end)' },
        { offset: D / T, opacity: 0, transform: `translateY(${MW.RISE}px)`, easing: SOFT },
        { offset: Math.min(1, (D + MW.IN * 0.3) / T), opacity: u.to * 0.92 },
        { offset: 1, opacity: u.to, transform: 'none' }], { duration: T, fill: 'backwards', id: 'mw' });
      end = Math.max(end, T); return;
    }
    riseIn(u.el, u.at, u.to); end = Math.max(end, u.at + MW.IN);
  });
  // a paragraph that arrived line by line is itself again once its last line has settled
  new Set(units.filter((u) => u.win).map((u) => u.win)).forEach((p) => { const k = p._lk; Promise.allSettled(qa(':scope > .mw-w', p).flatMap((w) => w.getAnimations()).map((a) => a.finished)).then(() => { if (alive && p._lk === k) unlines(p); }); });
  glides.forEach((o) => { end = Math.max(end, glide(o)); });
  fade.forEach((f) => { f.el.animate([{ opacity: f.from }, { opacity: f.to }], { duration: 420, easing: SOFT, fill: 'backwards', id: 'mw' }); end = Math.max(end, 420); });
  made.anims = document.getAnimations().filter((a) => !before.has(a) && !(window.CSSTransition && a instanceof CSSTransition) && !(window.CSSAnimation && a instanceof CSSAnimation));
  // the stand-ins stay (at nothing) until the whole switch is done, so a switch turned back can bring them back
  qa('.mw-g:not([data-done])').forEach((g) => { g.dataset.done = 1; dropWhen(g, made.anims); });
  MORPH.until = performance.now() + end;
  MORPH.last = jumped ? null : { anims: made.anims, ghosts: made.ghosts, change, total: end, dir: 1, pos: 0, at: performance.now(), tok: 0 };
  spyBand(true);
  return { jumped, t: end };
}
/* Turning a switch back: tapping the tab you just left while the wall is still turning over plays the same
   motion backwards, from exactly where it is (a little quicker), so what left comes back and what was arriving
   goes back, with nothing rebuilt; at the start of its timeline the old order is laid out again under it. And
   turning it forward again carries on where it is. */
const TURN = 1.6;
function turnMorph(L, revert) {
  const now = performance.now(), run = L.anims.find((a) => a.playState === 'running' && a.currentTime != null);
  // where the switch is along its own timeline (every piece of it shares one clock)
  L.pos = Math.max(0, run ? run.currentTime : L.pos + (now - L.at) * (L.dir > 0 ? 1 : -TURN)); L.at = now; L.dir = -L.dir;
  const rate = L.dir > 0 ? 1 : -TURN, tok = ++L.tok, t0 = document.timeline.currentTime;
  // every piece is put on the same clock again, running the other way; a piece that had already finished holds
  // where it ended until the clock comes back to it (no rewinding, which would bring it back early)
  L.anims.forEach((a) => { if (a.playState === 'idle') return; a.playbackRate = rate; a.startTime = t0 - L.pos / rate; });
  const left = L.dir > 0 ? Math.max(0, L.total - L.pos) : L.pos / TURN;
  MORPH.until = now + left;
  if (L.dir > 0) return { jumped: false, t: left };
  Promise.all(L.anims.filter((a) => a.playState !== 'idle').map((a) => a.finished)).then(() => {
    if (!alive || L.tok !== tok || L.dir > 0 || MORPH.last !== L) return;
    MORPH.last = null; MORPH.until = 0;
    L.anims.forEach((a) => a.cancel()); L.ghosts.forEach((g) => g.remove()); qa('.mw-lines').forEach(unlines);
    revert(); SPY.tops = null; spyBand(true);
  }, () => {});
  return { jumped: false, t: left };
}
const wallSig = () => `${S.f}|${S.sort}|${S.sel}|${S.pick}|${S.pick != null ? S.layer : ''}`;
// where an element is headed once this change settles: its resting opacity (a dimmed tile stays dimmed)
function rest(el) { el.style.transition = 'none'; const v = +getComputedStyle(el).opacity; el.style.transition = ''; return v; }
/* arriving: the opacity comes up early and the lift settles slowly, so a piece reads as soon as it starts */
function riseIn(el, delay, to = 1) {
  el.animate([{ opacity: 0, transform: `translateY(${MW.RISE}px)`, easing: SOFT }, { offset: 0.3, opacity: to * 0.92 }, { opacity: to, transform: 'none' }], { duration: MW.IN, delay, fill: 'backwards', id: 'mw' });
}
function glide(o) {
  const { el, dx, dy, d, s } = o, to = rest(el), tile = el.classList.contains('tile');
  if (tile) el.style.zIndex = 4;
  const kf = [{ transform: `translate(${dx}px, ${dy}px)` }, { transform: 'none' }];
  if (Math.abs(s.op - to) > 0.02) { kf[0].opacity = s.op; kf[1].opacity = to; }
  const T = Math.round(Math.min(780, 560 + d * 0.6));
  const a = el.animate(kf, { duration: T, delay: 40, easing: GLIDE, fill: 'backwards', id: 'mw' });
  if (tile) a.onfinish = a.oncancel = () => { el.style.zIndex = ''; };
  return T + 40;
}
/* a stand-in goes once its own fades are done (by the animations, not a clock, so it can never pop early);
   a newer call for the same stand-in supersedes an older one */
function dropWhen(g, anims) {
  const k = (g._k || 0) + 1; g._k = k;
  Promise.allSettled(anims.map((a) => a.finished)).then(() => { if (g._k === k) g.remove(); });
}
/* the lines of a paragraph, as boxes: a paragraph leaves line by line, so the sweep never waits on a whole block */
function lineRects(p) {
  const rg = document.createRange(); rg.selectNodeContents(p);
  const lines = [];
  [...rg.getClientRects()].filter((r) => r.width > 1 && r.height > 1).sort((a, b) => a.top - b.top).forEach((r) => {
    const l = lines[lines.length - 1];
    if (l && Math.abs(r.top - l.top) < 3) { l.left = Math.min(l.left, r.left); l.right = Math.max(l.right, r.right); l.bottom = Math.max(l.bottom, r.bottom); }
    else lines.push({ left: r.left, right: r.right, top: r.top, bottom: r.bottom });
  });
  // a short last line (a word or two) travels with the line above it
  const n = lines.length, W = p.getBoundingClientRect().width;
  if (n >= 2 && lines[n - 1].right - lines[n - 1].left < W * 0.25) { const z = lines.pop(), l = lines[n - 2]; l.left = Math.min(l.left, z.left); l.right = Math.max(l.right, z.right); l.bottom = z.bottom; }
  return lines;
}
/* a paragraph arriving with the sweep, as one window per line (see .mw-lines); returns the windows and their boxes */
function lineIn(p) {
  const L = lineRects(p); if (L.length < 2) return null;
  const pr = p.getBoundingClientRect(), copy = p.cloneNode(true);
  p._lk = (p._lk || 0) + 1; p.classList.add('mw-lines');
  return L.map((ln, i) => {
    const top = i ? (L[i - 1].bottom + ln.top) / 2 : pr.top, bot = i < L.length - 1 ? (ln.bottom + L[i + 1].top) / 2 : pr.bottom;
    const w = document.createElement('div'), inner = copy.cloneNode(true);
    w.className = 'mw-w'; w.setAttribute('aria-hidden', 'true');
    w.style.cssText = `top:${top - pr.top}px;width:${pr.width}px;height:${bot - top}px`;
    inner.style.cssText = `top:${pr.top - top}px;width:${pr.width}px`;
    w.appendChild(inner); p.appendChild(w);
    return { el: w, r: { left: ln.left, right: ln.right, top, bottom: bot } };
  });
}
function unlines(p) { p._lk = (p._lk || 0) + 1; qa(':scope > .mw-w', p).forEach((w) => w.remove()); p.classList.remove('mw-lines'); }
/* a stand-in for something that has left the layout, laid where it was and as it looked (each part of a
   divider at the opacity it had reached); its parts fade on the sweep's schedule, a paragraph line by line
   (each line a window onto a copy of the paragraph, so the text inside never reflows) */
function ghost(s, W) {
  const g = s.node, a = s.b;
  g.removeAttribute('id'); qa('[id]', g).forEach((n) => n.removeAttribute('id'));
  qa('.mw-w', g).forEach((w) => w.remove()); qa('.mw-lines', g).forEach((x) => x.classList.remove('mw-lines'));
  g.hidden = false; g.classList.add('mw-g'); g.setAttribute('aria-hidden', 'true'); g.inert = true;
  Object.assign(g.style, { position: 'absolute', zIndex: 2, left: a.left - W.left + 'px', top: a.top - W.top + 'px', width: a.width + 'px', height: a.height + 'px', margin: '0', paddingTop: s.pt, borderTop: s.bt, pointerEvents: 'none', transform: 'none', opacity: s.kids ? 1 : s.op });
  $('rd-wall').appendChild(g);
  const parts = [];
  if (s.kids) bparts(g).forEach((c, j) => {
    const k = s.kids[j]; if (!k || k.op < 0.03) { c.style.opacity = 0; return; }
    const tf = k.tf === 'none' ? 'translateY(0px)' : k.tf;
    if (!k.lines || k.lines.length < 2) { parts.push({ part: c, r: k.r, op: k.op, tf }); return; }
    const pr = k.r, L = k.lines, copy = c.cloneNode(true);
    Object.assign(c.style, { visibility: 'hidden', position: 'relative', transform: tf });
    L.forEach((ln, i) => {
      const top = i ? (L[i - 1].bottom + ln.top) / 2 : pr.top, bot = i < L.length - 1 ? (ln.bottom + L[i + 1].top) / 2 : pr.bottom;
      const w = document.createElement('div'), inner = copy.cloneNode(true);
      const op = k.ops && k.ops.length === L.length ? k.ops[i] : k.op; if (op < 0.03) return;
      w.style.cssText = `position:absolute;left:0;top:${top - pr.top}px;width:${pr.width}px;height:${bot - top}px;overflow:hidden;visibility:visible;opacity:${op}`;
      inner.style.cssText = `position:absolute;left:0;top:${pr.top - top}px;width:${pr.width}px;margin:0;visibility:visible`;
      w.appendChild(inner); c.appendChild(w);
      parts.push({ part: w, r: { left: ln.left, right: ln.right, top, bottom: bot }, op, tf: 'translateY(0px)', grp: c, li: i });
    });
  });
  else parts.push({ part: g, r: a, op: s.op, tf: 'translateY(0px)', card: g.classList.contains('ins') });
  return { g, parts };
}
let wallIO;
function revealWall() {
  if (wallIO) wallIO.disconnect();
  if (RM || !('IntersectionObserver' in window)) return;
  let batch = [], raf = 0;
  wallIO = new IntersectionObserver((es) => {
    es.forEach((e) => { if (e.isIntersecting) { wallIO.unobserve(e.target); batch.push(e.target); } });
    if (!raf) raf = requestAnimationFrame(() => {
      raf = 0;
      batch.sort((a, b) => { const x = a.getBoundingClientRect(), y = b.getBoundingClientRect(); return x.top - y.top || x.left - y.left; });
      // each piece rises to where it rests (a post dimmed by a pick stays dim, never flashing bright first)
      batch.forEach((t, k) => anim(t, [{ opacity: 0, transform: 'translateY(10px)' }, { opacity: rest(t), transform: 'none' }], { dur: 520, delay: Math.min(k * 22, 320) }));
      batch = [];
    });
  }, { rootMargin: '0px 0px -24px 0px', threshold: 0.05 });
  qa('#rd-feed > .tile, #rd-feed > .rbk').forEach((t) => wallIO.observe(t));
}
function hits(q) {
  const x = pickedSet(); if (!x) return false;
  return S.layer === 'repeat' ? x.ids.has(q.id) : q.series === x.id;
}
function decorate(wave) {
  const wall = $('rd-wall'); if (!wall) return;
  const Mo = M(), p = S.sel && Mo.byId[S.sel], trace = p && S.sort === 'recent';
  wall.classList.toggle('focus', !!(p || pickedSet()));
  let h = 0;
  qa('.tile[data-post]', wall).forEach((t) => {
    const q = Mo.byId[t.dataset.post], c = t.classList, isHit = !trace && hits(q);
    c.toggle('sel', q.id === S.sel); c.toggle('hit', isHit); c.remove('go');
    if (isHit) t.style.setProperty('--wd', `${Math.min(h++ * 24, 360)}ms`);
  });
  if (wave && !RM) { void wall.offsetWidth; qa('.tile.hit', wall).forEach((t) => t.classList.add('go')); }
  qa('#rd-trbloom .pt[data-petal]').forEach((pt) => pt.classList.toggle('sel', pt.dataset.petal === S.sel));
}
/* ---------- the trail: count back through the feed, across runs ---------- */
const TR = { timers: [], follow: null, rel: 0 };
const FADE = 320;
function trailOn() { const w = $('rd-wall'); return !!w && (!!w.querySelector('.tile.beat, .tile.above, .tile.stop, .tile.was') || $('rd-thead').classList.contains('on')); }
function wipeTrail() {
  clearTimeout(TR.rel); TR.rel = 0;
  const wall = $('rd-wall'); if (!wall) return;
  qa('.tile.beat, .tile.above, .tile.stop, .tile.was', wall).forEach((t) => { t.classList.remove('beat', 'above', 'stop', 'go', 'was'); const mk = t.querySelector('.mk'); if (mk) mk.removeAttribute('data-n'); t.querySelector('.ring').removeAttribute('data-flag'); });
  $('rd-tlines').innerHTML = ''; $('rd-thead').classList.remove('on', 'dn', 'end');
  wall.classList.remove('out');
}
/* fade: let the trail go as one piece (lines, counter, numbers, flag, rings), then wipe it */
function clearTrail(fade) {
  TR.timers.forEach(clearTimeout); TR.timers = []; TR.follow = null;
  if (!$('rd-wall')) return 0;
  if (!fade || RM || !trailOn()) { wipeTrail(); return 0; }
  $('rd-wall').classList.add('out');
  clearTimeout(TR.rel); TR.rel = setTimeout(wipeTrail, FADE);
  return FADE;
}
/* letting go of the picked reel: its ring fades with the trail instead of snapping off */
function letGo() {
  const wall = $('rd-wall'); if (!wall) return 0;
  qa('.tile.sel', wall).forEach((t) => t.classList.add('was'));
  return clearTrail(true);
}
/* where a post sits in the layout, on screen: a glide still settling on the wall (the card closing, an order
   switch) is left out, so the trail and its scroll are planned against where everything is headed */
function laid(t) {
  const w = $('rd-wall').getBoundingClientRect(), top = w.top + t.offsetTop, left = w.left + t.offsetLeft;
  return { top, left, bottom: top + t.offsetHeight, right: left + t.offsetWidth, width: t.offsetWidth, height: t.offsetHeight };
}
function followTile(t) {
  if (!TR.follow || !t) return;
  const r = laid(t), lim = TR.follow.bot - 30;
  if (r.bottom > lim) scrollTo({ top: scrollY + (r.bottom - lim) + 40, behavior: RM ? 'auto' : 'smooth' });
}
function playTrail(p, opts = {}) {
  const after = opts.after || 0;
  if (!after) clearTrail();
  if (!p || S.sort !== 'recent' || p.i === 0) return 0;
  const Mo = M(), wall = $('rd-wall'), feed = $('rd-feed');
  const up = p.ripple > 0, n = up ? p.ripple : p.shortOf;
  const chain = Array.from({ length: n }, (_, k) => Mo.posts[p.i - 1 - k]);
  const end = up ? p.stopper : Mo.posts[p.i - n - 1] || null;
  const T = (q) => feed.querySelector(`.tile[data-post="${q.id}"]`);
  if (opts.scroll) {
    // fit the whole trail between the compact header (or the sticky lens) and the tab bar; if it is taller, follow the count down
    const top = phone() ? $('rd-hdr').getBoundingClientRect().top + 70 : hb() + $('rd-lens').offsetHeight + 8;
    const bot = navTop() - 6;
    const rs = [p, ...chain, end].filter(Boolean).map((q) => laid(T(q)));
    const t0 = Math.min(...rs.map((r) => r.top)) - 4, b0 = Math.max(...rs.map((r) => r.bottom)) + 22, room = bot - top;
    let delta;
    if (b0 - t0 <= room) delta = t0 - top - (room - (b0 - t0)) / 2;
    else { delta = rs[0].top - 4 - top; TR.follow = { bot }; }
    if (Math.abs(delta) > 6) scrollTo({ top: Math.max(0, scrollY + delta), behavior: RM ? 'auto' : 'smooth' });
  }
  if (after) { const fl = TR.follow; TR.timers.push(setTimeout(() => { playTrail(p, {}); TR.follow = fl; }, after + 20)); return after + 20; }
  const gy = Math.min(7, (parseFloat(getComputedStyle(feed).rowGap) || 14) / 2);
  const pos = (el) => ({ x: el.offsetLeft + el.offsetWidth / 2, y: el.offsetTop + el.offsetHeight + gy });
  const svg = $('rd-tlines'); svg.setAttribute('viewBox', `0 0 ${wall.clientWidth} ${wall.clientHeight}`);
  const head = $('rd-thead');
  const place = (c) => { const w = head.offsetWidth || 90, x = Math.max(w / 2 + 2, Math.min(wall.clientWidth - w / 2 - 2, c.x)); head.style.setProperty('--x', x + 'px'); head.style.setProperty('--y', c.y + 'px'); };
  const dt = RM ? 0 : Math.max(60, Math.min(150, 1800 / Math.max(1, n)));
  let prev = pos(T(p));
  head.classList.toggle('dn', !up); head.style.setProperty('--dt', '0ms');
  head.textContent = up ? 'beat 0' : '0 did better'; place(prev);
  void head.offsetWidth; head.style.setProperty('--dt', dt * 1.5 + 'ms'); head.classList.add('on');
  const line = (a, b, cls) => {
    if (Math.abs(a.y - b.y) > 4) return;
    const ln = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    ln.setAttribute('x1', a.x); ln.setAttribute('x2', b.x); ln.setAttribute('y1', a.y); ln.setAttribute('y2', b.y); ln.setAttribute('class', cls);
    svg.appendChild(ln);
    if (dt) { ln.style.transformBox = 'fill-box'; ln.style.transformOrigin = `${b.x < a.x ? '100%' : '0%'} 50%`; ln.animate([{ transform: 'scaleX(0)' }, { transform: 'none' }], { duration: dt * 1.4, easing: 'ease-out' }); }
  };
  const at = (ms, fn) => (dt ? TR.timers.push(setTimeout(fn, ms)) : fn());
  chain.forEach((q, k) => at(180 + k * dt, () => {
    const t = T(q), c = pos(t);
    t.classList.add(up ? 'beat' : 'above'); t.querySelector('.mk').setAttribute('data-n', k + 1);
    line(prev, c, up ? 'up' : 'dn');
    head.textContent = up ? `beat ${k + 1}` : `${k + 1} did better`; place(c);
    prev = c; followTile(t);
  }));
  const total = 180 + n * dt;
  at(total + 60, () => {
    if (end) {
      const t = T(end), c = pos(t);
      line(prev, c, up ? 'up' : 'dn');
      t.classList.add('stop'); t.querySelector('.ring').setAttribute('data-flag', up ? `#${end.rank} did better` : `It beat #${end.rank}`);
      prev = c; followTile(t);
    }
    head.textContent = up ? (end ? `beat the last ${n}` : `beat all ${n}`) : `last ${n} did better`;
    head.classList.add('end'); place(prev);
  });
  return total;
}

/* ---------- what you picked: the header readout on a phone, a card on desktop ---------- */
function picked() {
  const Mo = M(), f = Mo.f, his = f.who;
  if (S.sel) {
    const p = Mo.byId[S.sel];
    const sub = `Top ${pcx(p)}% · ` + (p.i === 0 ? 'first in memory' : p.ripple ? `<i class="up">▲</i> ${p.ripple === p.i ? `beat all ${p.ripple}` : `beat the last ${p.ripple}`}` : `<i class="dn">▼</i> last ${p.shortOf} did better`);
    return { lead: tile(p, { still: true, nocap: true, cls: 'lit' }), title: p.label, sub, act: `<button type="button" class="rd-open" data-open="${p.id}">Open</button>`, take: p.take };
  }
  const x = pickedSet(); if (!x) return null;
  const hard = x.items.filter((q) => q.band >= 2).length;
  return { lead: stack(x.items, { w: 34, nocount: true }), title: x.pill || x.name, sub: `${plural(x.items.length, 'post')} · ${hard} in ${his} top 25%`, act: S.layer === 'read' ? `<button type="button" class="rd-open" data-oseries="${x.id}">Every go</button>` : '', take: x.say || x.take || '' };
}
function renderReadout() {
  const r = picked(), hdr = $('rd-hdr'), was = hdr.classList.contains('reading');
  hdr.classList.toggle('reading', !!r);
  if (!r) { if (S.drop === 'take') closeDrop(); setCompact(scrollY < 40 ? false : S.compact); return; }
  if (phone()) setCompact(true);
  const el = $('rd-hread'), html = `<span class="rd-v">${r.lead}</span><button type="button" class="rd-t" data-drop="take" aria-expanded="${S.drop === 'take'}" aria-label="${esc(r.title)}. Show the reader’s take"><b>${esc(r.title)}</b><span>${r.sub}</span></button>${r.act}<button type="button" class="rd-x" data-clear aria-label="Clear">${IC.x}</button>`;
  if (el._h !== html) {
    // moving from one pick to the next: the old cover and title let go first, as frozen copies laid where they
    // were, and the new ones settle in just behind them, so the row is never seen cut over cold
    const go = was && !RM && !!el.getClientRects().length, R = el.getBoundingClientRect();
    qa(':scope > .rd-g', el).forEach((g) => g.getAnimations().forEach((a) => a.updatePlaybackRate(3)));
    const gs = go ? qa(':scope > .rd-v, :scope > .rd-t, :scope > .rd-open', el).map((x) => {
      const r = x.getBoundingClientRect(), op = +getComputedStyle(x).opacity, g = x.cloneNode(true);
      g.classList.add('rd-g'); g.inert = true; g.setAttribute('aria-hidden', 'true');
      Object.assign(g.style, { position: 'absolute', left: r.left - R.left + 'px', top: r.top - R.top + 'px', width: r.width + 'px', height: r.height + 'px', margin: '0', pointerEvents: 'none', opacity: op });
      return g;
    }).filter((g) => +g.style.opacity > 0.03) : [];
    el.innerHTML = html; el._h = html;
    gs.forEach((g) => { el.appendChild(g); const a = g.animate([{ opacity: +g.style.opacity, transform: 'none' }, { opacity: 0, transform: 'translateY(-4px)' }], { duration: 120, easing: EASE_IN, fill: 'forwards' }); a.onfinish = a.oncancel = () => g.remove(); });
    if (go) qa(':scope > .rd-v, :scope > .rd-t, :scope > .rd-open', el).forEach((x, k) => rise(x, { y: 5, dur: 400, delay: 110 + k * 45 }));
  }
  S.take = r.take || '';
  if (S.drop === 'take') $('rd-hdrop').innerHTML = `<p class="take">${esc(S.take)}</p>`;
}
function renderFocusUI(mode) {
  renderReadout();
  qa('.mx[data-pickbit]').forEach((m) => m.setAttribute('aria-pressed', String(S.sort === 'recent' && S.layer === 'read' && S.pick === m.dataset.pickbit)));
  renderIns(mode);
}

/* ---------- focus: a reel, a lit set, a run ---------- */
function selectPost(id, opts = {}) {
  const p = M().byId[id]; if (!p) return;
  hideTip(); buzz();
  const after = letGo();
  S.sel = id; S.pick = null;
  if (S.drop) closeDrop();
  decorate(true); syncPressed(); renderFocusUI();
  // if the card just closed, the trail starts drawing once the wall has closed up under it (its scroll starts now)
  const settle = Math.max(0, INS.settle - performance.now() - 160), turning = MORPH.last && MORPH.last.dir < 0 ? MORPH.until - performance.now() + 30 : 0;
  playTrail(p, { ...opts, after: Math.max(after, Math.round(settle), Math.round(turning)) });
}
function pick(v) {
  buzz(); letGo(); S.sel = null;
  S.pick = String(S.pick) === String(v) ? null : v;
  decorate(true); syncPressed(true); renderFocusUI();
}
function clearFocus() { letGo(); S.sel = null; S.pick = null; decorate(false); syncPressed(); renderFocusUI(); }
/* one way in from the track: which tab, and which way along it. A series picked from elsewhere (the
   trajectory's "What it was made of") rides along, so the switch and the pick are one change. */
function setLens(k, pk) {
  const cur = lensKey(); if (tabIx(k) < 0) return;
  if (k === cur) { if (pk != null) pick(pk); return; }
  const dir = Math.sign(tabIx(k) - tabIx(cur));
  if (k === 'rank') setSort('rank', dir);
  else if (S.sort === 'rank') { S.layer = k; setSort('recent', dir, pk); }
  else setLayer(k, dir, pk);
}
function setLayer(l, dir, pk) {
  if (l === S.layer) return;
  const d = dir || Math.sign(tabIx(l) - tabIx(S.layer)) || 1;
  if (pk != null) { letGo(); S.sel = null; }
  S.layer = l; buzz(); S.pick = pk ?? null;
  renderLens(true, d); decorate(true); renderFocusUI();
}
/* Read/Repeat and Rank change the wall's order: the wall turns over in one sweep (morphWall). With a pick
   riding along, the insight card and the lit posts are part of the new layout and arrive with the sweep,
   so the card's own glide never pulls on posts the sweep is moving. Switched from the header's panel while
   down in the wall, the panel stays open and the new order starts right under it. */
function setSort(v, dir, pk) {
  if (v === S.sort) return;
  const L = MORPH.last, pre = wallSig(); SPY.goal = null;
  if (pk != null) { letGo(); S.sel = null; }
  S.sort = v; buzz(); clearTrail(true); S.pick = pk ?? null;
  renderLens(true, dir || (v === 'rank' ? 1 : -1));
  const change = () => { applySort(); decorate(false); renderFocusUI('place'); };
  const live = L && !RM && performance.now() < MORPH.until && L.anims.every((a) => a.playState !== 'idle') && L.ghosts.every((g) => g.isConnected);
  const sig = wallSig(), turn = live && (L.dir > 0 ? sig === L.pre : sig === L.post);
  // changed your mind while the wall is still turning over: it turns back (or on again) from where it is
  let r;
  if (turn) r = turnMorph(L, change);
  else { r = morphWall(change); if (MORPH.last) { MORPH.last.pre = pre; MORPH.last.post = sig; } }
  if (S.sel && v === 'recent') TR.timers.push(setTimeout(() => playTrail(M().byId[S.sel]), RM ? 0 : r.t + 60));
}
function pickBit(id) {
  if (lensKey() !== 'read') { setLens('read', id); return; }
  pick(id);
}
/* A tap anywhere off the highlight gets you out. On the wall that tap is used up; a control
   elsewhere also does its own job. */
function outsideTap(e) {
  if (ovOpen) return false;
  const t = e.target;
  if (S.drop && !t.closest('#rd-hdrop, #rd-hdr')) { closeDrop(); return true; }
  const lit = !!S.sel || S.pick != null;
  if (!lit) return false;
  if (t.closest('#rd-hdr, #rd-hdrop, #rd-lens, [data-fm-bottom-nav], #rd-ins, #rd-traj, .bd-more')) return false;
  const tl = t.closest('#rd-feed .tile[data-post]'), inWall = !!t.closest('#rd-wall');
  if (lit) {
    if (tl && tl.matches('.sel, .beat, .above, .stop, .hit')) return false;
    buzz(); clearFocus();
    return inWall || !t.closest('button, a');
  }
  return false;
}

/* ---------- the detail sheet ---------- */
function resChart(p, compact) {
  const Mo = M();
  const spanN = Math.min(compact ? 14 : 25, Math.max(p.ripple + 1, p.shortOf, compact ? 6 : 8));
  const bars = Mo.posts.slice(Math.max(0, p.i - spanN), p.i + 1);
  const W = 340, H = compact ? 92 : 120, bw = (W - 8) / bars.length;
  const hy = (q) => 16 + (q.pct / 100) * (H - 26), yP = hy(p);
  let s = '', lx = null;
  bars.forEach((q, k) => {
    const x = 4 + k * bw + bw * 0.16, w = bw * 0.68, y = hy(q), h = H - 6 - y;
    const isBeat = q.i < p.i && q.i >= p.i - p.ripple, isAbove = q.i < p.i && q.i >= p.i - p.shortOf, isStop = p.stopper && q.id === p.stopper.id;
    const fill = q.id === p.id ? 'var(--ink)' : isBeat ? 'var(--rose)' : isStop || isAbove ? 'rgba(228,228,231,.82)' : 'var(--s3)';
    s += `<rect class="b" style="--d:${(p.i - q.i) * 38 + 150}ms" x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${w.toFixed(1)}" height="${Math.max(2, h).toFixed(1)}" rx="${Math.min(3, w / 2).toFixed(1)}" fill="${fill}"/>`;
    if (isStop) { s += `<text class="lb" style="--ld:${(p.i - q.i) * 38 + 650}ms" x="${(x + w / 2).toFixed(1)}" y="${(y - 7).toFixed(1)}" text-anchor="middle" font-size="9" font-weight="700" fill="var(--down)" font-family="JetBrains Mono, monospace" letter-spacing="1">DID BETTER</text>`; lx = x + w + 2; }
    if (q.id === p.id) s += `<text class="lb" style="--ld:200ms" x="${(x + w / 2).toFixed(1)}" y="${H + 8}" text-anchor="middle" font-size="9" font-weight="700" fill="var(--ink)" font-family="JetBrains Mono, monospace" letter-spacing="1">THIS</text>`;
  });
  const xThis = 4 + (bars.length - 1) * bw + bw * 0.16;
  const xEnd = p.ripple > 0 ? (lx ?? 4) : 4 + (bars.length - 1 - p.shortOf) * bw + bw * 0.1;
  if (p.i > 0 && xThis - xEnd > 2) {
    const len = Math.round(xThis - xEnd), col = p.ripple > 0 ? 'var(--rose)' : 'rgba(228,228,231,.75)', ld = Math.min(p.ripple || p.shortOf, 25) * 38 + 500;
    s += `<line class="ln2" style="--lo:${xEnd < xThis - 2 ? '100%' : '0%'};--ld:${ld}ms" x1="${(xThis - 2).toFixed(1)}" y1="${yP.toFixed(1)}" x2="${xEnd.toFixed(1)}" y2="${yP.toFixed(1)}" stroke="${col}" stroke-width="1.6" stroke-linecap="round"/>`;
    if (p.ripple > 0 || p.shortOf >= 2) s += `<text class="lb" style="--ld:${ld + 300}ms" x="${((xThis + xEnd) / 2).toFixed(1)}" y="${(p.ripple > 0 ? yP - 6 : 7).toFixed(1)}" text-anchor="middle" font-size="10" font-weight="600" fill="${col}" font-family="JetBrains Mono, monospace">${p.ripple > 0 ? `beat ${p.ripple}` : `${p.shortOf} did better`}</text>`;
  }
  s += `<line x1="0" x2="${W}" y1="${H - 6}" y2="${H - 6}" stroke="rgba(255,255,255,.12)"/>`;
  const svg = `<svg class="ch" viewBox="0 -4 ${W} ${H + 14}" role="img" aria-label="${esc(beatShort(p).replace(/[▲▼] /, ''))}">${s}</svg>`;
  if (compact) return svg;
  return `<div class="box res">${svg}<p>${beatLine(p)}</p><div class="key"><span><i style="background:var(--rose)"></i>It beat these</span><span><i style="background:var(--down)"></i>These did better</span><span><i style="background:var(--ink)"></i>This reel</span></div></div>`;
}
function postView(p) {
  const Mo = M(), f = Mo.f, s = Mo.sById[p.series], sib = s.items, nth = sib.indexOf(p) + 1, his = f.who;
  const beats = (p.flow || []).slice(0, 6);
  return `<div class="pv">
    <div class="ph">${tile(p, { still: true, cls: 'hero lit' })}
      <div class="info"><button type="button" class="chip vpc" data-series="${s.id}">${esc(s.pill || s.name)} <em>${s.id === 'oneoff' ? 'one-off' : `${nth} of ${sib.length}`}</em></button>
        <h2>${esc(p.label)}</h2><span class="sub">${fd(p.date)} · ${Mo.runs.length > 1 ? `Run ${p.run} · ` : ''}${p.length ? esc(p.length) + ' · ' : ''}reel</span>
        <div class="rkb"><div class="big b${p.band}"><small>top </small>${pcx(p)}<small>%</small></div><div class="track"><i style="--to:${Math.min(100, p.pct)}%"></i></div><div class="tl2"><span>${his.toUpperCase()} BEST</span><span>${word(p).toUpperCase()}</span><span>WORST</span></div></div></div></div>
    <section class="sec rv"><span class="k">The reader’s take</span><p class="why">${esc(p.take)}</p></section>
    <section class="sec rv"><span class="k"><span>Against the reels before it</span><span class="bw ${p.ripple ? 'b2' : 'b0'}">${beatShort(p)}</span></span>${resChart(p)}</section>
    <section class="sec rv"><span class="k">What decided it</span><div class="box"><span class="bw d-${p.driver}" style="justify-self:start">${DRV[p.driver] ? DRV[p.driver][0] : ''}: ${DRVQ[p.driver] || ''}</span><p>${esc((f.drivers || {})[p.driver] || '')}</p></div></section>
    ${beats.length ? `<section class="sec rv"><span class="k">What happens in it</span><ol class="beats">${beats.map((b) => `<li><span>${esc(b.t || '•')}</span><span>${esc(b.beat)}</span></li>`).join('')}</ol></section>` : `<section class="sec rv"><span class="k">Caption</span><p class="why" style="color:var(--ink-2)">“${esc(p.hook)}”</p></section>`}
    ${sib.length > 1 ? `<section class="sec rv"><span class="k"><span>${s.id === 'oneoff' ? 'Other one-offs' : `Every go at “${esc(s.name)}”`}</span><span>higher = ranked higher</span></span>${podium(sib, { cur: p.id, style: `--ph:${phone() ? 160 : 180}px;--tw:86px` })}</section>` : ''}
  </div>`;
}
function seriesView(s) {
  const hard = s.items.filter((p) => p.band >= 2).length;
  return `<div class="pv">
    <div class="shero rv">${stack(s.items, { cap: true })}<div style="display:grid;gap:12px"><span class="k">${s.id === 'oneoff' ? 'One-offs' : `A bit ${who() === 'his' ? 'he keeps' : 'they keep'} doing`}</span><h2 style="font-size:clamp(30px,3.6vw,44px);line-height:.98;letter-spacing:-.045em">${esc(s.pill || s.name)}</h2><span class="bw b${band(s.med)}" style="justify-self:start">Typical: top ${pc(s.med)}%</span></div></div>
    <div class="stats4 rv"><div><span class="k">Goes</span><strong>×${s.items.length}</strong></div><div><span class="k">First</span><strong>${fd(s.first)}</strong></div><div><span class="k">Latest</span><strong>${fd(s.last)}</strong></div><div><span class="k">In top 25%</span><strong class="${hard ? 'r' : ''}">${hard} of ${s.items.length}</strong></div></div>
    <section class="sec rv"><span class="k">The read</span><p class="verdict">${esc(s.say || s.take)}</p></section>
    <section class="sec rv"><span class="k"><span>Every go, in order</span><span>higher = ranked higher · tap one</span></span>${podium(s.items, { style: `--ph:${phone() ? 180 : 210}px;--tw:96px` })}</section>
  </div>`;
}
const ovTitle = (e) => (e.type === 'post' ? M().byId[e.id].label : M().sById[e.id].pill || M().sById[e.id].name);
function renderOv() {
  const top = S.stack[S.stack.length - 1], Mo = M();
  $('rd-obody').innerHTML = top.type === 'post' ? postView(Mo.byId[top.id]) : seriesView(Mo.sById[top.id]);
  $('rd-obody').scrollTop = 0; $('rd-shd').classList.remove('stuck');
  $('rd-miniTitle').textContent = ovTitle(top);
  const prev = S.stack[S.stack.length - 2];
  $('rd-backSlot').innerHTML = prev ? `<button type="button" class="back" data-back>${IC.l}<span>${esc(ovTitle(prev))}</span></button>` : '';
  let acts = '';
  if (top.type === 'post') { const p = Mo.byId[top.id], o = Mo.posts[p.i - 1], n = Mo.posts[p.i + 1]; acts += `<button type="button" class="ib" ${o ? `data-step="${o.id}"` : 'disabled'} aria-label="Older reel">${IC.l}</button><button type="button" class="ib" ${n ? `data-step="${n.id}"` : 'disabled'} aria-label="Newer reel">${IC.r}</button>`; }
  $('rd-acts').innerHTML = acts + `<button type="button" class="ib" id="rd-closeOv" aria-label="Close">${IC.x}</button>`;
  fixPods($('rd-obody'));
  qa('#rd-obody .rv').forEach((el, k) => rise(el, { y: 14, dur: 520, delay: 160 + k * 70 }));
}
function fly(src, a, b, dur, done) {
  const c = src.cloneNode(true);
  c.removeAttribute('data-post'); c.classList.add('fly', 'lit'); c.classList.remove('sel', 'go', 'beat', 'above', 'stop');
  Object.assign(c.style, { left: b.left + 'px', top: b.top + 'px', width: b.width + 'px', height: b.height + 'px', aspectRatio: 'auto', transformOrigin: '0 0', opacity: '1' });
  layer.appendChild(c);
  const an = c.animate([{ transform: `translate(${a.left - b.left}px, ${a.top - b.top}px) scale(${a.width / b.width}, ${a.height / b.height})` }, { transform: 'none' }], { duration: dur, easing: SOFT, fill: 'forwards' });
  an.onfinish = () => { if (done) done(); c.remove(); };
}
function openEntry(entry, src) {
  hideTip(); buzz(); closeDrop();
  if (ovOpen) { S.stack.push(entry); swapOv(1); return; }
  S.stack = [entry];
  renderOv();
  const ov = $('rd-ov'), sheet = $('rd-sheet'), scrim = $('rd-scrim');
  [sheet, scrim].forEach((el) => el.getAnimations().forEach((a) => a.cancel()));
  sheet.style.transform = '';
  ov.classList.add('on'); ov.setAttribute('aria-hidden', 'false'); ovOpen = true;
  // in the app the page scrolls on the root element, so that is what the sheet locks
  if (!bodyLock) { bodyLock = true; bodyPrev = document.documentElement.style.overflow; }
  document.documentElement.style.overflow = 'hidden';
  if (RM) { sheet.style.opacity = 1; scrim.style.opacity = 1; return; }
  sheet.style.opacity = '';
  const srcTile = src && src.classList && src.classList.contains('tile') ? src : null;
  const target = $('rd-obody').querySelector('.tile.hero');
  const a = srcTile ? srcTile.getBoundingClientRect() : null, b = target ? target.getBoundingClientRect() : null;
  scrim.animate([{ opacity: 0 }, { opacity: 1 }], { duration: 320, easing: 'ease-out', fill: 'forwards' });
  sheet.animate(phone() ? [{ opacity: 1, transform: 'translateY(100%)' }, { opacity: 1, transform: 'none' }] : [{ opacity: 0, transform: 'translateY(18px) scale(.98)' }, { opacity: 1, transform: 'none' }], { duration: 520, easing: SOFT, fill: 'forwards' });
  if (a && b && a.width && b.width) { target.style.opacity = 0; fly(srcTile, a, b, 520, () => { target.style.opacity = ''; }); }
}
function swapOv(dir) {
  const body = $('rd-obody');
  if (RM) { renderOv(); return; }
  body.animate([{ opacity: 1, transform: 'none' }, { opacity: 0, transform: `translateX(${-dir * 20}px)` }], { duration: 160, easing: 'ease-in', fill: 'forwards' }).onfinish = () => {
    if (!alive) return;
    renderOv(); body.getAnimations().forEach((x) => x.cancel());
    body.animate([{ opacity: 0, transform: `translateX(${dir * 20}px)` }, { opacity: 1, transform: 'none' }], { duration: 420, easing: SOFT });
  };
}
function stepTo(id, dir) { S.stack[S.stack.length - 1] = { type: 'post', id }; buzz(); swapOv(dir); }
function closeOv() {
  if (!ovOpen) return;
  ovOpen = false; buzz();
  const ov = $('rd-ov'), sheet = $('rd-sheet'), scrim = $('rd-scrim');
  const done = () => { ov.classList.remove('on'); ov.setAttribute('aria-hidden', 'true'); if (bodyLock) document.documentElement.style.overflow = bodyPrev; bodyLock = false; sheet.style.transform = ''; [sheet, scrim].forEach((el) => el.getAnimations().forEach((x) => x.cancel())); sheet.style.opacity = ''; scrim.style.opacity = ''; };
  if (RM) { done(); return; }
  const top = S.stack[S.stack.length - 1], hero = $('rd-obody').querySelector('.tile.hero');
  const dest = top && top.type === 'post' ? qa(`#rd-feed .tile[data-post="${top.id}"]`).find((t) => { const r = t.getBoundingClientRect(); return r.width && r.bottom > 0 && r.top < innerHeight; }) : null;
  if (hero && dest) { const a = hero.getBoundingClientRect(), b = dest.getBoundingClientRect(); hero.style.opacity = 0; dest.style.opacity = 0; fly(hero, a, b, 360, () => { dest.style.opacity = ''; }); }
  scrim.animate([{ opacity: 1 }, { opacity: 0 }], { duration: 280, easing: 'ease-in', fill: 'forwards' });
  const cur = getComputedStyle(sheet).transform;
  sheet.animate(phone() ? [{ opacity: 1, transform: cur === 'none' ? 'none' : cur }, { opacity: 1, transform: 'translateY(100%)' }] : [{ opacity: 1, transform: 'none' }, { opacity: 0, transform: 'translateY(14px) scale(.98)' }], { duration: 300, easing: 'cubic-bezier(.4,0,.7,.2)', fill: 'forwards' }).onfinish = () => { if (alive) done(); };
}
(function dragSheet() {
  const sh = $('rd-sheet'); let y0 = null, dy = 0, t0 = 0;
  const start = (e) => { if (!phone() || e.target.closest('button')) return; y0 = e.clientY; dy = 0; t0 = performance.now(); e.currentTarget.setPointerCapture(e.pointerId); sh.getAnimations().forEach((a) => { try { a.commitStyles(); } catch (_) {} a.cancel(); }); sh.style.opacity = 1; };
  const move = (e) => { if (y0 == null) return; dy = Math.max(0, e.clientY - y0); sh.style.transform = `translateY(${dy}px)`; $('rd-scrim').style.opacity = Math.max(0.2, 1 - dy / 600); };
  const end = () => { if (y0 == null) return; const vel = dy / Math.max(1, performance.now() - t0); y0 = null; if (dy > 120 || vel > 0.7) closeOv(); else { anim(sh, [{ transform: `translateY(${dy}px)` }, { transform: 'none' }], { dur: 420, fill: 'none' }); sh.style.transform = 'none'; $('rd-scrim').style.opacity = 1; } };
  ['rd-grab', 'rd-shd'].forEach((id) => { const el = $(id); listen(el, 'pointerdown', start); listen(el, 'pointermove', move); listen(el, 'pointerup', end); listen(el, 'pointercancel', end); });
  listen($('rd-obody'), 'scroll', () => $('rd-shd').classList.toggle('stuck', $('rd-obody').scrollTop > 140), { passive: true });
})();

/* ---------- desktop hover tooltip ---------- */
function showTip(el) {
  if (!fine || ovOpen) return;
  const p = M().byId[el.dataset.post]; if (!p) return;
  const t = $('rd-tip');
  t.innerHTML = `${tile(p, { still: true, nocap: true, cls: 'lit' })}<div><b>${esc(p.label)}</b><span>${fd(p.date)} · top ${pcx(p)}% · ${beatShort(p)}</span></div>`;
  const r = el.getBoundingClientRect();
  t.style.left = Math.min(innerWidth - 300, Math.max(10, r.left + r.width / 2 - 145)) + 'px';
  t.style.top = (r.top - 84 < 120 ? r.bottom + 10 : r.top - 84) + 'px';
  t.classList.add('on');
}
function hideTip() { $('rd-tip').classList.remove('on'); }
listen(document, 'pointerover', (e) => { const el = e.target.closest('#rd-feed .tile[data-post]'); if (el) showTip(el); });
listen(document, 'pointerout', (e) => { if (e.target.closest('#rd-feed .tile')) hideTip(); });

/* ---------- page ---------- */
function render(enter) {
  renderHeader();
  $('rd-main').innerHTML = trajHTML() + lensHTML() + `<section class="wall S-${S.sort}" id="rd-wall" aria-label="Every post"><div class="feed" id="rd-feed">${feedHTML()}</div><svg class="tlines" id="rd-tlines" aria-hidden="true"></svg><span class="thead" id="rd-thead" aria-hidden="true"></span></section><p class="foot">${esc(note())}</p>`;
  renderLens(false); applySort(); decorate(false); renderFocusUI(); revealWall();
  if (enter !== false && !RM) {
    qa('#rd-traj .tj-b').forEach((b, k) => rise(b, { y: 8, dur: 560, delay: 60 + k * 36 }));
    qa('#rd-traj .tj-f').forEach((f, k) => anim(f, [{ transform: 'scaleX(0)' }, { transform: 'none' }], { dur: 820, delay: 240 + k * 110, ease: 'cubic-bezier(.18, .86, .22, 1)' }));
    qa('#rd-tjbody > *').forEach((el, k) => rise(el, { y: 12, dur: 620, delay: 360 + k * 70 }));
    growBloom($('rd-trbloom'), 520);
  }
}
function switchFeeder(k) {
  if (k === S.f) return;
  closeOv(); closeDrop(); clearTrail();
  MORPH.until = 0; MORPH.last = null; INS.settle = 0; clearTimeout(INS.wait);
  clearTimeout(MORPH.anchor); MORPH.anchor = 0; MORPH.anchorEnd = 0; document.documentElement.style.overflowAnchor = '';
  qa('.ins-g, .mw-g').forEach((g) => g.remove());
  Object.assign(S, { f: k, sel: null, pick: null, sort: 'recent', layer: 'read', tr: null }); SPY.cur = null; SPY.goal = null;
  buzz();
  const main = $('rd-main'), go = () => { scrollTo({ top: 0 }); render(); };
  if (RM) { go(); return; }
  const a = main.animate([{ opacity: 1 }, { opacity: 0 }], { duration: 180, easing: 'ease-in', fill: 'forwards' });
  a.onfinish = () => { if (!alive) return; go(); a.cancel(); };
}

/* ---------- events ---------- */
listen(document, 'click', (e) => {
  if (outsideTap(e)) return;
  const q = (s) => e.target.closest(s); let b;
  if ((b = q('[data-f]'))) { switchFeeder(+b.dataset.f); return; }
  if ((b = q('[data-drop]'))) { const k = b.dataset.drop; if (S.drop === k) closeDrop(); else openDrop(k); return; }
  if ((b = q('[data-clear]'))) { buzz(); clearFocus(); return; }
  if ((b = q('[data-more]'))) { const p = $('rd-trsup'), open = p.classList.toggle('clamp'); b.textContent = open ? 'More' : 'Less'; b.setAttribute('aria-expanded', String(!open)); return; }
  if ((b = q('[data-tr]'))) { setTr(+b.dataset.tr); return; }
  if ((b = q('[data-bdmore]'))) { toggleBand(+b.dataset.bdmore); return; }
  if ((b = q('[data-band]'))) { goBand(+b.dataset.band); return; }
  if ((b = q('[data-tropen]'))) { setTr(+b.dataset.tropen, true); return; }
  if ((b = q('[data-wallrun]'))) { wallRun(+b.dataset.wallrun); return; }
  if ((b = q('[data-unpick]'))) { unpickPetal(); return; }
  if ((b = q('[data-petal]'))) { pickPetal(b.dataset.petal); return; }
  if ((b = q('#rd-trbloom'))) { unpickPetal(); return; }
  if ((b = q('[data-trace]'))) { selectPost(b.dataset.trace, { scroll: true }); return; }
  if ((b = q('[data-pickbit]'))) { pickBit(b.dataset.pickbit); return; }
  if ((b = q('[data-fm-bottom-nav]'))) return; // the app's nav does its own job (and its own haptics)
  if ((b = q('[data-lens]'))) { setLens(b.dataset.lens); return; }
  if ((b = q('[data-pick]'))) { pick(b.dataset.pick); if (S.drop === 'lens') closeDrop(); return; }
  if ((b = q('#rd-closeOv')) || (b = q('#rd-scrim'))) { closeOv(); return; }
  if ((b = q('[data-back]'))) { S.stack.pop(); swapOv(-1); return; }
  if ((b = q('[data-step]'))) { const cur = M().byId[S.stack[S.stack.length - 1].id], nx = M().byId[b.dataset.step]; stepTo(nx.id, nx.i > cur.i ? 1 : -1); return; }
  if ((b = q('[data-open]'))) { const id = b.dataset.open, own = b.closest('#rd-trcard'); openEntry({ type: 'post', id }, own ? own.querySelector('.pcover .tile') : document.querySelector(`#rd-feed .tile[data-post="${id}"]`)); return; }
  if ((b = q('[data-oseries]'))) { openEntry({ type: 'series', id: b.dataset.oseries }, null); return; }
  if ((b = q('[data-sel]'))) { const id = b.dataset.sel; if (S.sel === id) openEntry({ type: 'post', id }, document.querySelector(`#rd-feed .tile[data-post="${id}"]`)); else selectPost(id, { scroll: true }); return; }
  if ((b = q('#rd-feed .tile[data-post]'))) { const id = b.dataset.post; if (S.sel === id) openEntry({ type: 'post', id }, b); else selectPost(id, { scroll: true }); return; }
  if ((b = q('[data-post]'))) { openEntry({ type: 'post', id: b.dataset.post }, b); return; }
  if ((b = q('[data-series]'))) { openEntry({ type: 'series', id: b.dataset.series }, b); return; }
});
listen(document, 'keydown', (e) => {
  if (e.key === 'Escape') {
    if (!ovOpen && !S.drop && $('rd-trbloom') && $('rd-trbloom').classList.contains('picking')) { unpickPetal(); return; }
    if (ovOpen) { if (S.stack.length > 1) { S.stack.pop(); swapOv(-1); } else closeOv(); }
    else if (S.drop) closeDrop();
    else if (S.sel || S.pick != null) clearFocus();
    return;
  }
  if (ovOpen && (e.key === 'ArrowLeft' || e.key === 'ArrowRight')) {
    const top = S.stack[S.stack.length - 1]; if (top.type !== 'post') return;
    const p = M().byId[top.id], n = M().posts[p.i + (e.key === 'ArrowRight' ? 1 : -1)];
    if (n) stepTo(n.id, e.key === 'ArrowRight' ? 1 : -1);
  }
});
let lastY = 0, acc = 0, ticking = false, hdrLock = 0;
/* a scroll the page makes for you keeps the header compact, whichever way it goes */
function quietTo(top) {
  hdrLock = performance.now() + 1000; acc = 0;
  if (phone()) setCompact(true);
  scrollTo({ top: Math.max(0, top), behavior: RM ? 'auto' : 'smooth' });
}
let lastScrollAt = 0;
function onScroll() {
  const y = scrollY, dy = y - lastY; lastY = y;
  // a scroll that starts after a jump's own scroll has come to rest is yours: the jump lets go of its mark
  const t = performance.now(); if (SPY.goal != null && t > SPY.goalUntil && t - lastScrollAt > 180) SPY.goal = null; lastScrollAt = t;
  if (phone()) {
    if (performance.now() < hdrLock) acc = 0;
    else if (y < 40) { acc = 0; setCompact(false); }
    else { if ((dy > 0 && acc < 0) || (dy < 0 && acc > 0)) acc = 0; acc += dy; if (acc > 80) { setCompact(true); acc = 0; } else if (acc < -56) { setCompact(false); acc = 0; } }
    lensOn();
  }
  spyBand();
}
listen(window, 'scroll', () => { if (ticking) return; ticking = true; requestAnimationFrame(() => { ticking = false; onScroll(); }); }, { passive: true });
let rz;
listen(window, 'resize', () => {
  clearTimeout(rz);
  rz = setTimeout(() => { fixPods(); setCompact(S.compact); renderFocusUI(); SPY.tops = null; spyBand(true); if (S.sel && S.sort === 'recent' && trailOn()) playTrail(M().byId[S.sel]); rollDesc($('rd-kd'), 0); rollDesc($('rd-kd2'), 0); }, 160);
});
if (document.fonts) document.fonts.ready.then(() => { if (alive) fixPods(); });
freshChrome();
render();
// anything that changes the page's height (the card, a run's read) moves the dividers: the spy re-reads them
if ('ResizeObserver' in window) { ro = new ResizeObserver(() => { if (!alive) return; SPY.tops = null; spyBand(); }); ro.observe($('rd-main')); }

/* ---------- the tab's chrome back to its markup state: on a fresh mount, and on cleanup ---------- */
function quietLayer() {
  ovOpen = false; S.stack = [];
  const ov = $('rd-ov'), sheet = $('rd-sheet'), scrim = $('rd-scrim'), shd = $('rd-shd'), d = $('rd-hdrop'), tip = $('rd-tip');
  if (ov) { ov.classList.remove('on'); ov.setAttribute('aria-hidden', 'true'); }
  if (sheet) { sheet.style.transform = ''; sheet.style.opacity = ''; }
  if (scrim) scrim.style.opacity = '';
  if (shd) shd.classList.remove('stuck');
  ['rd-obody', 'rd-backSlot', 'rd-miniTitle', 'rd-acts'].forEach((id) => { const el = $(id); if (el) el.innerHTML = ''; });
  S.drop = null; qa('[data-drop]').forEach((t) => t.setAttribute('aria-expanded', 'false'));
  if (d) { d.classList.remove('on'); d.innerHTML = ''; }
  if (tip) { tip.classList.remove('on'); tip.innerHTML = ''; }
  qa(':scope > .pfly, :scope > .fly, :scope > .ins-g', layer).forEach((n) => n.remove());
  if (bodyLock) { document.documentElement.style.overflow = bodyPrev; bodyLock = false; }
  if (MORPH.anchor) { clearTimeout(MORPH.anchor); MORPH.anchor = 0; MORPH.anchorEnd = 0; document.documentElement.style.overflowAnchor = ''; }
}
function freshChrome() {
  quietLayer();
  const hdr = $('rd-hdr'), hr = $('rd-hread');
  hdr.classList.remove('compact', 'reading', 'lens-on');
  if (hr) { hr.innerHTML = ''; hr._h = undefined; }
  setCompressed(false);
}

return function cleanup() {
  if (!alive) return;
  alive = false;
  offs.splice(0).forEach((off) => off());
  timers.forEach((id) => window.clearTimeout(id)); timers.clear();
  frames.forEach((id) => window.cancelAnimationFrame(id)); frames.clear();
  if (wallIO) { wallIO.disconnect(); wallIO = null; }
  if (ro) { ro.disconnect(); ro = null; }
  // the script's own motion on the tab's pieces (CSS transitions and keyframes belong to the stylesheet)
  const roots = [$('rd-main'), layer, $('rd-hdr')].filter(Boolean);
  document.getAnimations().forEach((a) => {
    const t = a.effect && a.effect.target;
    if (!t || (window.CSSTransition && a instanceof CSSTransition) || (window.CSSAnimation && a instanceof CSSAnimation)) return;
    if (roots.some((r) => r.contains(t))) a.cancel();
  });
  // the sheet, the header's panel, the tip, every stand-in and flying copy, the page's scroll lock
  quietLayer();
  const main = $('rd-main'); if (main) qa('.mw-g, .chips.ghost, .ins-old', main).forEach((n) => n.remove());
};
}
