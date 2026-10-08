/* The Read tab engine: the approved prototype's plain JS, mounted and torn down by ReadTab. Kept as JS (not TS)
   so the prototype code stays byte-for-byte reviewable against docs/feeder-reader/feeder-reader.html. */
/*
 * The Read tab's engine: the Read prototype's script, run imperatively against the page (#rd-main), the
 * fixed layer (#rd-layer, markup from readMarkup.ts) and the readout row of the header. What changed in the
 * port, and nothing else:
 *   - every id carries an rd- prefix (other tabs share the document);
 *   - the prototype's own tab bar is the app's bottom nav ([data-fm-bottom-nav]);
 *   - the header is the app's, drawn by React (ReadHeader: the title row #rd-hdr and Lead's story rail of
 *     feeds and feeders under it). React owns which feeder is read: it mounts the engine on one (initial)
 *     and switches it with the show(k) the mount returns. The engine tells the header what it is showing
 *     (onHeader), fills the readout (#rd-hread) and toggles `reading` on #rd-hdr; the view switch
 *     (data-drop) goes through the engine's own click router. The header folds as Lead's does, at every
 *     width (setCompact, which drives the app's compression through setCompressed), and the phone/desktop
 *     breakpoint is 1023/1024px;
 *   - stand-ins and flying copies go into the tab's fixed layer instead of <body>;
 *   - everything a mount starts (listeners, observers, timers, frames, motion, stand-ins, the sheet's scroll
 *     lock, the readout) is undone by the unmount it returns, and mounting again renders from scratch.
 */


// the colours of each drawn cover's scene (also the board's stand-in for a sample post's cover)
export const SCENES = {
  bedroom: ['#80573a', '#1f140d'], night: ['#22357a', '#3b2810'], car: ['#1f5753', '#081716'], garage: ['#323b4e', '#06070a'],
  office: ['#5c7590', '#18202a'], turf: ['#45a856', '#0d3519'], restaurant: ['#b05e30', '#2a150a'], icecream: ['#f2aac2', '#7a3550'],
  studio: ['#cfcfca', '#6a6a65'], gray: ['#a0a0a0', '#141414'], home: ['#9c805f', '#30271c'], party: ['#6538b0', '#140926'],
  ui: ['#323c52', '#0b0e14'], piano: ['#7c1a1a', '#120303'], store: ['#cc6a22', '#361806'], balcony: ['#82b6dc', '#284760'],
  plain: ['#716a63', '#211e1b'], luxe: ['#47361d', '#0b0804'], counter: ['#634029', '#180f09'], street: ['#a8622c', '#221309'],
  lips: ['#d0206a', '#470920'], gold: ['#dcad4a', '#46310e'], nude: ['#dcab96', '#663f33'], mint: ['#86d0bd', '#214c44'],
  shelf: ['#f2d9dc', '#986870'], phone: ['#433366', '#0c0915'],
};

export function mountReader({ data, layer, setCompressed, onHeader, initial = 0 }) {
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
/* the app's chrome. The header is the app's glass capsule: its top moves with the safe area; it is 152px tall
   on a phone and 168px on desktop, and folds to its top 68px (the title row) as you scroll down. Its height
   comes from the engine's own state, so a measure taken right after a fold already sees where it is going.
   hb() is 8px under it: where the desktop lens sticks. hbc() is the same line with the header folded: where
   the header's panel drops, and where every scroll the page makes for you lands things (it folds the
   header). The tab bar is the app's own bottom nav. */
const cap = () => { const h = $('rd-hdr'); return h ? h.closest('.fm-depth-chrome--header') : null; };
const ht = () => { const c = cap(); return c ? c.getBoundingClientRect().top : 10; };
const hh = () => (S.compact ? 68 : phone() ? 152 : 168);
const hb = () => ht() + hh() + 8;
const hbc = () => ht() + 76;
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
const S = { f: DATA.feeders[initial] ? initial : 0, layer: 'read', sort: 'recent', sel: null, pick: null, drop: null, take: '', stack: [], compact: false, tr: null, rule: null };
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
  // the reader: its rules (the account), its run reads, and the concepts the wall groups by. A feeder without one
  // falls back to its series and run dispatches
  const rd = f.reader || null;
  const runs = Array.from({ length: runCount }, (_, k) => {
    const r = k + 1, items = ps.filter((p) => p.run === r), md = med(items.map((p) => p.rank)), rr = rd && rd.runs ? rd.runs[String(r)] || null : null;
    const d = (f.dispatch || {})[String(r)] || { head: 'Run ' + r, body: 'Not read yet.' };
    return { r, items, md, typical: Math.floor(md + 0.5), top: items.filter((p) => p.pct <= 25).length, best: [...items].sort((a, b) => a.rank - b.rank)[0], rr, d: rr ? { head: rr.head, body: d.body } : d };
  });
  runs.forEach((R, k) => { const P = runs[k - 1]; R.prev = P || null; const dm = P ? P.md - R.md : 0; R.dir = !P ? 'first' : dm >= calm ? 'up' : dm <= -calm ? 'down' : 'level'; });
  runs.forEach((R) => { R.tp = Math.max(1, Math.round(med(R.items.map((p) => p.pct)))); R.low = R.items.filter((p) => p.pct > 50).length; });
  const byId = Object.fromEntries(ps.map((p) => [p.id, p]));
  const conSrc = rd && rd.concepts ? rd.concepts : series.map((s) => ({ id: s.id, name: s.name, short: s.pill || s.name, read: s.say || s.take || '', posts: s.items.map((p) => p.id) }));
  // Repeats: most-posted concept first; a tie goes to the one that lands better
  const cons = conSrc.map((c) => {
    const items = c.posts.map((id) => byId[id]).filter(Boolean).sort((a, b) => a.i - b.i);
    return items.length ? { ...c, short: c.short || c.name, items, ids: new Set(items.map((p) => p.id)), med: med(items.map((p) => p.pct)), first: items[0].date, last: items[items.length - 1].date } : null;
  }).filter(Boolean).sort((a, b) => b.items.length - a.items.length || a.med - b.med);
  cons.forEach((c) => c.items.forEach((p) => { p.concept = c.id; }));
  /* a rule's weight, from the posts it names: lands (it says these should land), sinks (these should sink). Reach =
     how many posts it speaks for; held = how often the post did what the rule said; carry = how many of the
     account's top-quarter posts it explains; breaks = the posts that did the opposite. The board ranks rules by
     carry, then by how often they hold, then by reach, and a rule's number is its place on the board */
  const half = n / 2, quarter = Math.max(1, Math.round(n / 4));
  const weigh = (r) => {
    const L = (r.lands || r.proof || []).filter((id) => byId[id]), Sk = (r.sinks || r.against || []).filter((id) => byId[id]), rk = (id) => byId[id].rank;
    const held = L.filter((id) => rk(id) <= half).length + Sk.filter((id) => rk(id) > half).length, calls = L.length + Sk.length;
    const brokenBy = [...L.filter((id) => rk(id) > half), ...Sk.filter((id) => rk(id) <= half)];
    const items = [...new Set([...L, ...Sk])].map((id) => byId[id]).sort((a, b) => a.i - b.i);
    return { ...r, L, Sk, calls, held, reach: items.length, carry: L.filter((id) => rk(id) <= quarter).length, quarter, brokenBy, items, ids: new Set(items.map((p) => p.id)), rate: calls ? held / calls : 0 };
  };
  const rules = (rd && rd.rules ? rd.rules : []).map(weigh).sort((a, b) => b.carry - a.carry || b.rate - a.rate || b.reach - a.reach).map((r, k) => ({ ...r, n: k + 1 }));
  const retired = (rd && rd.retired ? rd.retired : []).map(weigh);
  const ruleById = Object.fromEntries([...rules, ...retired.map((r) => ({ ...r, gone: true }))].map((r) => [r.id, r]));
  const Mo = { f, n, rd, posts: ps, byId, series, sById: Object.fromEntries(series.map((s) => [s.id, s])), runs, cons, cById: Object.fromEntries(cons.map((c) => [c.id, c])), rules, retired, ruleById };
  CACHE.set(fi, Mo);
  return Mo;
}
const M = () => model(S.f);

/* ---------- words: plain feed language ---------- */
const RANKW = ['Bottom half', 'Top half', 'Top 25%', 'Top 10%'];
const RANKV = [
  (w, n, they) => `Below ${w} own middle of the last ${n}. ${they} saw it and kept scrolling.`,
  (w) => `Above ${w} middle, outside the top quarter. Did the job, didn’t travel.`,
  (w, n) => `${w[0].toUpperCase() + w.slice(1)} top quarter of the last ${n}, just under the best tenth. The solid hits.`,
  (w, n) => `The best tenth of ${w} last ${n}. The bar every new reel gets measured against.`,
];
const DRV = { idea: ['The idea', 'Idea'], moment: ['The moment', 'Moment'], faces: ['The faces', 'Faces'], craft: ['The craft', 'Craft'] };
const DRVQ = { idea: 'whether the joke itself was any good', moment: 'something already in the air', faces: 'who’s in it', craft: 'how it’s made: length, pace, format' };
const SHORT = { biz: 'Own industry', football: 'Football', crew: 'The crew', bmw: 'The BMW', food: 'Eating out', place: 'Place puns', street: 'Street', city: 'Delhi v Mumbai', brand: 'Brand deal', both: 'Both sides', solo: 'Solo', family: 'Told off', skin: 'Her day', people: 'People', eye: 'Eye hero', oneoff: 'One-off' };
/* the lens's three tabs, each its own order of the same wall: Runs (newest first, a breaker per run), Repeats
   (grouped by the idea at each post's core, most-posted first), Ranks (best first, a divider per band) */
const TABS = [['runs', 'Runs'], ['repeats', 'Repeats'], ['ranks', 'Ranks']];
const SORT_OF = { runs: 'recent', repeats: 'concept', ranks: 'rank' };
const LENS_OF = { recent: 'runs', concept: 'repeats', rank: 'ranks' };
const lensKey = () => LENS_OF[S.sort] || 'runs';
/* a rule's move, in plain words, and whether it reads for (up) or against (dn) the rule */
const MOVE = { born: ['New', 'up'], sharpened: ['Stronger', 'up'], held: ['Held', ''], tweaked: ['Narrowed', ''], strains: ['Strained', 'dn'], breaks: ['Broke', 'dn'], dropped: ['Dropped', 'dn'], backs: ['Backed', 'up'], none: ['No change', ''] };
const moveW = (m) => (MOVE[m] || [m, ''])[0];
const moveC = (m) => (MOVE[m] || ['', ''])[1];
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
const who = () => M().f.who;
const they = () => (who() === 'his' ? 'His crowd' : 'Their crowd');
const word = (p) => RANKW[p.band];
const beatShort = (p) => (p.i === 0 ? 'First in memory' : p.ripple ? (p.ripple === p.i ? `Beat all ${p.ripple} before it` : `Beat the ${p.ripple} before it`) : p.shortOf === 1 ? 'The one before it did better' : `The ${p.shortOf} before it did better`);
function beatLine(p) {
  const Mo = M(), q = p.stopper, many = Mo.runs.length > 1;
  if (p.i === 0) return 'First reel in memory, so there’s nothing before it to beat.';
  if (p.ripple === p.i) return `Beat all <b>${p.i}</b> reels before it. A new high for the feed.`;
  if (p.ripple > 0) return `Beat the <b>${plural(p.ripple, 'post')}</b> before it, going back through ${many && q.run !== p.run ? 'earlier runs' : 'the feed'} until <b>${esc(q.label)}</b>${many ? ` (run ${q.run}, ${rkw(q).toLowerCase()})` : ` (${rkw(q).toLowerCase()})`}, which did better.`;
  if (p.shortOf === 1) return `<b>${esc(q.label)}</b>, right before it, did better (${rkw(q).toLowerCase()}).`;
  return `The <b>${p.shortOf}</b> reels in a row before it all did better.`;
}
const note = () => { const Mo = M(); return `Sample data. ${Mo.posts.some((p) => p.thumb) ? 'Covers are the reels’ own thumbnails.' : 'Covers are mock thumbnails built from each postcard’s on-screen hook and scene.'} ${Mo.f.dateNote || ''} Rank = where a reel landed at day 7 against ${Mo.f.who} own last ${Mo.n}. Rules, run reads and post mortems are hand-written placeholders for the feeder reader.`; };

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
  const attrs = still ? 'aria-hidden="true"' : `type="button" data-post="${p.id}" aria-label="${esc(p.label)}, ${rkw(p)}, ${esc(beatShort(p))}"`;
  return `<${tag} ${attrs} class="tile b${p.band} ${o.cls || ''}" data-pid="${p.id}" style="--g1:${g[0]};--g2:${g[1]};${o.style || ''}"><span class="face ${p.scene === 'gray' ? 'gray' : ''}${p.thumb ? ' img' : ''}">${p.thumb ? `<img class="cov" src="${p.thumb}" alt="" decoding="async" draggable="false">` : art(p.art)}${o.nocap || p.thumb ? '' : `<span class="cap"><span>${esc(cap)}</span></span>`}${o.badge ? `<span class="br b${p.band}">${rkw(p)}</span>` : ''}${o.pc ? `<span class="pcv">${rkw(p)}</span>` : ''}${o.res ? '<span class="mk"></span>' : ''}</span><span class="ring"></span>${o.extra || ''}</${tag}>`;
}
function stack(items, o = {}) {
  const sorted = [...items].sort((a, b) => a.pct - b.pct), lead = sorted[0], rest = sorted.slice(1, 3);
  const edge = (q, c) => { const g = SCENES[q.scene] || SCENES.plain; return `<i class="e ${c}" style="--g1:${g[0]};--g2:${g[1]}${q.thumb ? `;background:center/cover url(${q.thumb})` : ''}"></i>`; };
  return `<span class="stack"${o.w ? ` style="--sw:${o.w}px"` : ''}>${rest[1] ? edge(rest[1], 'e2') : ''}${rest[0] ? edge(rest[0], 'e1') : ''}${tile(lead, { still: true, nocap: !o.cap, cls: 'lit' })}${o.nocount ? '' : `<b class="cnt">×${items.length}</b>`}</span>`;
}
function podium(items, o = {}) {
  const n = items.length;
  const lines = [[10, 'TOP 10%', 't10'], [25, 'TOP 25%', 't25'], [50, 'TOP HALF', '']].map(([y, l, c]) => `<span class="gl ${c}" style="--gy:${y}"><span>${l}</span></span>`).join('');
  return `<div class="pod" style="--n:${n};${o.style || ''}">${lines}${items.map((p, k) => `<div class="col" style="--y:${Math.min(100, p.pct).toFixed(1)};--k:${k}">${tile(p, { nocap: n > 6 || o.nocap, badge: n <= 6, cls: (p.id === o.cur ? 'cur ' : '') + 'lit', style: `--k:${k}` })}</div>`).join('')}</div>
    <div class="podd" style="--n:${n}">${items.map((p) => `<span class="${p.band >= 2 ? 'hi' : ''}">${n > 6 ? ord(p.rank) : fd(p.date)}</span>`).join('')}</div>`;
}
function fixPods(root = document) { qa('.pod', root).forEach((pod) => { const t = pod.querySelector('.tile'); if (t && t.offsetHeight) pod.style.setProperty('--th', t.offsetHeight + 'px'); }); }

/* ---------- the lens's track: three equal tabs, one indicator that only ever translates ---------- */
function tabsHTML(id) {
  const k = lensKey();
  return `<div class="tabs" id="${id}" role="group" aria-label="What the wall shows" style="--i:${tabIx(k)};--td:0ms"><span class="ind" aria-hidden="true"><span class="ind-l">${TABS.map(([, l]) => `<span>${l}</span>`).join('')}</span></span>${TABS.map(([v, l]) => `<button type="button" data-lens="${v}" aria-pressed="${v === k}"><span class="tl">${l}</span></button>`).join('')}</div>`;
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
  return;
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
  if (k === 'ranks') return 'By where it landed · <b>best first</b>';
  if (k === 'repeats') return n ? 'By idea · <b>most posted first</b>' : `What ${who() === 'his' ? 'he keeps' : 'they keep'} making · <b>most posted first</b>`;
  return n ? 'Runs of 10 · <b>newest first</b>' : 'Run by run · <b>newest first</b>';
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
function renderHeader() { tellHeader(); }
/* what the header shows (which feeder, which view of the wall, how many posts) is React's to draw */
function tellHeader() { if (onHeader) onHeader({ f: S.f, lens: lensKey(), n: M().n }); }
function setCompact(c) {
  // a panel open under the header, or a pick being read on a phone, holds the header folded
  if (S.drop || (phone() && (S.sel || S.pick != null))) c = true;
  if (c === S.compact) return;
  S.compact = c;
  setCompressed(c);
  placeLens();
}
/* The desktop lens sticks 8px under the header, wherever the header's bottom is (--rd-hb on the page). The
   fold moves that line by 100px in one step, so a lens that is stuck there is played from where it was to
   where it is now, on the header's own curve, and glides with the header's edge instead of jumping. (The
   header's panel needs no such line: an open panel holds the header folded, so it always drops from there.) */
let lensFlip = null;
function placeLens() {
  const main = $('rd-main'), v = phone() ? '' : Math.round(hb()) + 'px';
  if (!main || main.style.getPropertyValue('--rd-hb') === v) return;
  const lens = $('rd-lens'), go = !!v && !!lens && !RM, from = go ? lens.getBoundingClientRect().top : 0;
  if (lensFlip) { lensFlip.cancel(); lensFlip = null; }
  if (v) main.style.setProperty('--rd-hb', v); else main.style.removeProperty('--rd-hb');
  if (!go) return;
  const dy = from - lens.getBoundingClientRect().top;
  if (Math.abs(dy) < 1) return;
  const a = lens.animate([{ transform: `translateY(${dy}px)` }, { transform: 'none' }], { duration: 340, easing: 'cubic-bezier(.32,.72,0,1)' });
  lensFlip = a; a.onfinish = a.oncancel = () => { if (lensFlip === a) lensFlip = null; };
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
  if (scrollY <= 30) setCompact(false);
  spyBand();
}
/* Rank: the band you are reading through, marked on the jump pills (in the lens and in the header's panel).
   The dividers' places are layout positions (never a transition's transform), cached until the page's
   height changes, so the check on each animation frame of a scroll only compares numbers. */
const SPY = { cur: null, tops: null, goal: null, goalUntil: 0 };
/* each order's dividers on the wall, and the data key that names them: a run's breaker, a concept's, a band's */
const DIV = { recent: ['.rbk', 'rbk'], concept: ['.cdiv', 'cdiv'], rank: ['.bdiv', 'bdiv'] };
function spyBand(force) {
  return;
  let cur = null;
  if ($('rd-feed')) {
    if (!SPY.tops) { const w = $('rd-wall'), [sel, key] = DIV[S.sort]; SPY.tops = qa(`#rd-feed > ${sel}`).filter((d) => d.offsetParent).map((d) => [d.dataset[key], w.offsetTop + d.offsetTop]); }
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
  qa('#rd-chips .chip.jump, #rd-chips2 .chip.jump').forEach((c) => c.classList.toggle('here', c.dataset.jump === cur));
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
    s += p ? `<path class="pt m${p.band}${o.on === p.id ? ' on' : ''}" d="${wedge(k, BLEN(p.pct))}" style="--k:${k}"${o.tap ? ` data-petal="${p.id}"` : ''}><title>${esc(p.label)}, ${rkw(p)}</title></path>` : `<path class="pt e" d="${wedge(k, 0.2)}"/>`;
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
/* ---------- the rules: the account, as the reader understands it right now (the page's hero) ----------
   Lead's grammar, for rules: a board. The reader's line on the account heads it; under it every rule is a row,
   ranked by how much of the account's best work it explains. A row reads at a glance (the law, how far it
   carries, how often it held, how far it reaches, how often it broke) and opens in place into its proof: the
   wins it explains, the flops it calls, the posts that break it, and its life run by run. */
const pad2 = (k) => String(k).padStart(2, '0');
// a number that rolls up into place (Lead's SlotText): the digits sit in a window and rise into it
const slot = (v) => `<span class="slt"><span>${v}</span></span>`;
function ruleRow(r) {
  const Mo = M(), last = (r.life || [])[(r.life || []).length - 1], born = (r.life || [])[0];
  const throne = !r.gone && r.n === 1, strength = r.gone ? 0 : Math.max(0.06, r.carry / r.quarter);
  const mv = r.gone ? `<span class="mv dn">Dropped · run ${last ? last.run : Mo.runs.length}</span>` : `<span class="mv ${moveC(r.status)}">${esc(moveW(r.status))} · run ${Mo.runs.length}</span>`;
  const st = (cls, v, u, l, bar) => `<span class="rbd-st ${cls}"><b>${slot(v)}${u ? `<small>${u}</small>` : ''}</b>${bar != null ? `<i class="rbd-bar"><i style="--v:${bar.toFixed(3)}"></i></i>` : ''}<em>${l}</em></span>`;
  return `<div class="rbd-row ${throne ? 'throne' : ''} ${r.gone ? 'gone' : ''}" data-ru="${r.id}">
    <button type="button" class="rbd-hd" data-rsel="${r.id}" aria-expanded="false" aria-label="${esc(r.law)} ${r.gone ? 'Dropped.' : `Carries ${r.carry} of his top ${r.quarter}. Held ${r.held} of ${r.calls}.`} Show the proof">
      <span class="rbd-fill" style="--s:${strength.toFixed(3)}" aria-hidden="true"></span>
      ${throne ? '<span class="rbd-crown" aria-hidden="true"></span>' : ''}
      <span class="rbd-n" aria-hidden="true">${r.gone ? '×' : pad2(r.n)}</span>
      <span class="rbd-main"><b class="rbd-law">${esc(r.law)}</b><span class="rbd-cap">${mv}<span class="sep">·</span><span>born run ${born ? born.run : 1}</span><span class="rbd-mini">${r.gone ? '' : `<span class="sep">·</span>held ${r.held}/${r.calls}<span class="sep">·</span>${r.brokenBy.length ? `${plural(r.brokenBy.length, 'break')}` : 'unbroken'}`}</span></span></span>
      ${r.gone ? st('c', '—', '', 'retired') : st('c', r.carry, `/${r.quarter}`, `of his top ${r.quarter}`)}
      ${st('h', r.held, `/${r.calls}`, 'held', r.calls ? r.held / r.calls : 0)}
      ${st('r', r.reach, `/${Mo.n}`, 'posts it speaks for', r.reach / Mo.n)}
      ${st('x', r.brokenBy.length, '', r.brokenBy.length === 1 ? 'post breaks it' : 'posts break it')}
      <span class="rbd-cv" aria-hidden="true">${IC.chev}</span>
    </button>
    <div class="rbd-x" hidden></div>
  </div>`;
}
/* a row, opened: why it holds, its proof in two lanes, the posts that break it, how the reader got here */
function ruleOpen(r) {
  const Mo = M(), cur = Mo.runs.length, brk = new Set(r.brokenBy);
  const lane = (ids) => ids.filter((id) => !brk.has(id)).map((id) => Mo.byId[id]).sort((a, b) => a.rank - b.rank);
  const pf = (p, cls) => `<button type="button" class="rbx-pf ${cls}" data-post="${p.id}" aria-label="${esc(p.label)}, ${ord(p.rank)} of ${Mo.n}. Post mortem">${tile(p, { still: true, nocap: true, cls: 'lit' })}<b>${ord(p.rank)}</b></button>`;
  const up = lane(r.L), dn = lane(r.Sk), br = r.brokenBy.map((id) => Mo.byId[id]).sort((a, b) => a.rank - b.rank);
  const life = (r.life || []).map((x) => `<li class="${moveC(x.move)} ${x.run === cur ? 'now' : ''}"><i></i><b>Run ${x.run}</b><span>${esc(moveW(x.move))}</span><p>${esc(x.line)}</p></li>`).join('');
  return `<div class="rbx">
    <p class="rbx-bc">${esc(r.gone ? r.line : r.because)}</p>
    ${up.length || dn.length ? `<div class="rbx-lanes">
      ${up.length ? `<div class="rbx-lane up"><span class="k">The wins it explains · ${up.length}</span><div class="rbx-strip">${up.map((p) => pf(p, 'up')).join('')}</div></div>` : ''}
      ${dn.length ? `<div class="rbx-lane dn"><span class="k">The flops it calls · ${dn.length}</span><div class="rbx-strip">${dn.map((p) => pf(p, 'dn')).join('')}</div></div>` : ''}
    </div>` : ''}
    <div class="rbx-brk"><span class="k">${br.length ? `Where it breaks · ${br.length}` : 'Where it breaks'}</span>${br.length ? br.map((p) => `<button type="button" class="rbx-b" data-post="${p.id}">${tile(p, { still: true, nocap: true, cls: 'lit' })}<span><b>${ord(p.rank)} · ${esc(p.label)}</b>${esc((r.breaks || {})[p.id] || '')}</span></button>`).join('') : `<p class="rbx-none">Nothing has broken it yet: ${r.calls} calls, ${r.held} held.</p>`}</div>
    <div class="rbx-life"><span class="k">How the reader got here</span><ol>${life}</ol></div>
    ${r.gone ? '' : `<div class="rbx-ft"><button type="button" class="tj-go" data-rule="${r.id}" aria-pressed="${S.pick === r.id}">Light it up on the wall${IC.chev}</button></div>`}
  </div>`;
}
function rulesHTML() { return boardHTML(); }
/* open a rule in place: the rows under it (and the wall) glide down to make room while its proof rises in behind
   them; opening another closes the last in the same glide; tapping it again closes it */
function glideBelow(change, ms = 620) {
  const els = qa('#rd-rules .rbd-row, #rd-rules .rbd-foot, .lens-head, #rd-lens, #rd-ins, #rd-feed > *'), vh = innerHeight;
  const near = (r) => r.height > 0 && r.bottom > -80 && r.top < vh + 80;
  const before = new Map(els.map((el) => [el, el.getBoundingClientRect()]));
  change();
  if (RM) return;
  before.forEach((a, el) => { const b = el.getBoundingClientRect(), dy = a.top - b.top; if (Math.abs(dy) > 1 && (near(a) || near(b))) el.animate([{ transform: `translateY(${dy}px)` }, { transform: 'none' }], { duration: Math.min(760, ms + Math.abs(dy) * 0.25), easing: `cubic-bezier(${OPEN})`, composite: 'add' }); });
}
function setRule(id) {
  const r = M().ruleById[id]; if (!r) return;
  buzz();
  const was = S.rule, open = was !== id;
  const row = (k) => document.querySelector(`#rd-rules .rbd-row[data-ru="${k}"]`);
  const set = (k, on, rule) => { const el = row(k); if (!el) return; const x = el.querySelector('.rbd-x'); x.innerHTML = on ? ruleOpen(rule) : ''; x.hidden = !on; el.toggleAttribute('data-open', on); el.querySelector('.rbd-hd').setAttribute('aria-expanded', String(on)); };
  S.rule = open ? id : null;
  glideBelow(() => { if (was) set(was, false); if (open) set(id, true, r); });
  if (!open || RM) return;
  const x = row(id).querySelector('.rbd-x');
  qa('.rbx-bc, .rbx-lane, .rbx-brk, .rbx-life, .rbx-ft', x).forEach((el, k) => rise(el, { y: 12, dur: 640, delay: 140 + k * 80 }));
  qa('.rbx-pf', x).forEach((el, k) => anim(el, [{ opacity: 0, transform: 'translateY(10px) scale(.92)' }, { opacity: 1, transform: 'none' }], { dur: 560, delay: 260 + k * 45 }));
  const top = row(id).getBoundingClientRect().top, want = phone() ? hbc() + 8 : hbc() + 16;
  if (top < want || top > innerHeight * 0.55) quietTo(scrollY + top - want);
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
/* what a post did to the rules, as small chips: R1 · Backs */
function fxChips(p) {
  const Mo = M(), fx = ((p.mortem || {}).fx || []).filter((x) => x.r && Mo.ruleById[x.r]);
  return fx.map((x) => `<span class="fxc ${moveC(x.m)}"><b>R${Mo.ruleById[x.r].n || x.r.slice(1)}</b>${moveW(x.m)}</span>`).join('');
}
function cardHTML(p, mode) {
  const Mo = M(), R = Mo.runs[p.run - 1], nth = R.items.indexOf(p) + 1, c = Mo.cById[p.concept];
  const head = mode === 'run'
    ? `<span class="pk r">Best of run ${R.r}</span><span class="pk-h">Tap any petal</span>`
    : `<span class="pk">Post ${nth} of ${R.items.length}</span><button type="button" class="pk-x" data-unpick aria-label="Back to run ${R.r}">Run ${R.r}${IC.x}</button>`;
  return `<div class="pci"><div class="pcover">${tile(p, { still: true, pc: true })}</div><div class="pcb"><div class="pch">${head}</div><b class="pct">${esc(p.label)}</b><p class="pctk">${esc((p.mortem || {}).why || p.take || '')}</p></div></div>
    <div class="pcs">${c ? `<span>${esc(c.short)}</span>` : ''}${fxChips(p)}${p.paid ? '<span>Paid</span>' : ''}</div>
    <div class="pca"><button type="button" class="pc-open" data-open="${p.id}">Post mortem</button><button type="button" class="pc-trace" data-trace="${p.id}">Trace on the wall${IC.r}</button></div>`;
}
/* a run's read, opened under its breaker on the wall: the bloom of its ten posts, then the reader on the run */
function runBody(R) { return ''; }
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
    const pt = box.querySelector(`.pt[data-petal="${id}"]`).getBoundingClientRect(), room = pt.top - (ht() + hh() + 10);
    const need = Math.min(c.bottom - bot, room);
    if (need > 8) quietTo(scrollY + need);
  }
}
function unpickPetal(quiet) {
  const box = $('rd-trbloom'); if (!box || !box.classList.contains('picking')) return;
  if (!quiet) buzz();
  box.dataset.on = ''; box.classList.remove('picking');
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
/* Tap a run's breaker: its read opens right there, under the breaker, and the posts below glide down to make
   room; the read comes in piece by piece behind them. One run is open at a time: opening another closes the last
   (in the same glide). Tap the breaker again to close it. */
function toggleRun(r, o = {}) {
  const Mo = M(); if (!Mo.runs[r - 1]) return;
  const was = S.tr, open = was !== r;
  buzz(); unpickPetal(true);
  const box = (k) => document.querySelector(`.rbk[data-rbk="${k}"] .rbk-x`);
  const set = (k, on) => { const b = box(k); if (!b) return; b.innerHTML = on ? runBody(Mo.runs[k - 1]) : ''; b.hidden = !on; if (on) mountBoard(b); const rb = b.closest('.rbk'); rb.toggleAttribute('data-open', on); rb.querySelector('.rbk-hd').setAttribute('aria-expanded', String(on)); };
  S.tr = open ? r : null;
  flipWall(() => { if (was) set(was, false); if (open) set(r, true); }, () => {
    if (!open) return;
    const b = box(r);
    if (o.scroll !== false) { const rb = b.closest('.rbk'), top = phone() ? hbc() : hbc() + $('rd-lens').offsetHeight + 8; quietTo(scrollY + rb.getBoundingClientRect().top - top); }
    if (RM) return;
    qa('.mur > .lk-p, .mur > .call, .mur > .lk-sec:last-child', b).forEach((el, k) => rise(el, { y: 12, dur: 620, delay: 260 + k * 70 }));
  }, `cubic-bezier(${OPEN})`);
}
/* a run's box on the rules: the wall goes to Runs and that run opens */
function runJump(r) {
  if (S.sort !== 'recent') setSort('recent');
  const go = () => { if (S.tr !== r) toggleRun(r); else { const rb = document.querySelector(`.rbk[data-rbk="${r}"]`); if (rb) quietTo(scrollY + rb.getBoundingClientRect().top - (phone() ? hbc() : hbc() + $('rd-lens').offsetHeight + 8)); } };
  const left = MORPH.until - performance.now();
  if (left > 0) setTimeout(go, left + 40); else go();
}
/* petals grow out from the centre, one after another in posting order */
function growBloom(root, d0 = 0) {
  if (RM) return;
  qa('.bloom .pt:not(.e)', root).forEach((pt) => anim(pt, [{ transform: 'scale(.15)', opacity: 0 }, { transform: 'none', opacity: 1 }], { dur: 700, delay: d0 + (+pt.style.getPropertyValue('--k') || 0) * 55, ease: 'cubic-bezier(.2, .8, .2, 1)' }));
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
/* every tab's pills jump to its dividers: a run, a concept, a band; the one you are reading through is marked */
function chipsHTML() {
  const Mo = M();
  const bars = (items) => `<span class="mp" aria-hidden="true">${items.slice(0, 10).map((p) => `<i class="m${p.band}"></i>`).join('')}</span>`;
  const lab = (t) => `<span class="cl"><span>${t}</span><span aria-hidden="true">${t}</span></span>`;
  const jv = IC.chev.replace('<svg ', '<svg class="jv" ');
  const chip = (key, label, inner, aria) => `<button type="button" class="chip jump${SPY.cur === key ? ' here' : ''}" data-jump="${key}" aria-label="${aria}">${inner}${lab(label)}${jv}</button>`;
  if (S.sort === 'rank') return [3, 2, 1, 0].map((b) => { const k = Mo.posts.filter((p) => p.band === b).length; return k ? chip(String(b), RANKW[b], `<i class="sw m${b}"></i>`, `Jump to ${RANKW[b]}, ${plural(k, 'post')}`).replace(`${jv}</button>`, `<em>${k}</em>${jv}</button>`) : ''; }).join('');
  if (S.sort === 'concept') return Mo.cons.map((c) => chip(c.id, esc(c.short), bars(c.items), `Jump to ${esc(c.name)}, ${plural(c.items.length, 'post')}`).replace(`${jv}</button>`, `<em>${c.items.length}</em>${jv}</button>`)).join('');
  return [...Mo.runs].reverse().map((R) => chip(String(R.r), `Run ${R.r}`, `<i class="sw rs" style="${shadeVars(R.tp)}"></i>`, `Jump to run ${R.r}: ${esc(R.d.head)}`)).join('');
}
/* what is lit on the wall: a rule's posts (from the rules), any order */
function pickedSet() {
  if (S.pick == null) return null;
  const r = M().ruleById[S.pick];
  return r && r.items ? r : null;
}
function insHTML() {
  const x = pickedSet(); if (!x) return '';
  return `<div class="ins-hd"><span class="ins-n">Rule ${pad2(x.n)}</span><b>${esc(x.law)}</b><button type="button" class="rd-x" data-clear aria-label="Clear">${IC.x}</button></div>`;
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
  const lab = S.sort === 'rank' ? 'Jump to a band' : S.sort === 'concept' ? 'Jump to an idea' : 'Jump to a run';
  qa('#rd-chips, #rd-chips2').forEach((r) => r.setAttribute('aria-label', lab));
  tellHeader();
}
function goBand(b) {
  buzz();
  const [sel, key] = DIV[S.sort];
  const d = qa(`#rd-feed > ${sel}`).find((x) => x.dataset[key] === String(b)); if (!d) return;
  const top = phone() ? hbc() : hbc() + $('rd-lens').offsetHeight + 8;
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
/* a post on the wall: where it landed, its streak (Runs), and in Ranks the rule that put it there */
function ruleTag(p) {
  const Mo = M(), x = ((p.mortem || {}).fx || []).find((f) => f.r && Mo.ruleById[f.r] && !Mo.ruleById[f.r].gone);
  if (!x) return '';
  // rose when the rule is why it landed, dark when the rule is why it sank
  const r = Mo.ruleById[x.r];
  return `<span class="tg trl ${p.pct > 50 ? 'dn' : 'up'}">R${r.n}</span>`;
}
function wtile(p) {
  return tile(p, { res: true, extra: `<span class="rkp${p.band >= 2 ? ' hi' : ''}" aria-hidden="true">${rkw(p)}</span>` });
}
function runInk(tp) {
  const t = Math.min(100, Math.max(1, tp));
  if (t <= 50) return `rgb(${mix([245, 245, 241], [255, 23, 79], Math.pow((50 - t) / 49, 0.8))})`;
  return `rgba(245,245,241,${(0.62 - Math.min(1, (t - 50) / 40) * 0.3).toFixed(2)})`;
}
function breakerHTML(R) {
  const Mo = M(), latest = R.r === Mo.runs.length && Mo.runs.length > 1;
  return `<div class="rbk" data-rbk="${R.r}">
    <button type="button" class="rbk-hd" data-tropen="${R.r}" aria-haspopup="dialog" aria-label="Run ${R.r}: ${esc(R.d.head)}. Typical post ${rkv(R.tp)}. Open my read of this run.">
      <span class="rbk-n" style="--c:${runInk(R.tp)}" aria-hidden="true">${pad2(R.r)}</span>
      <span class="rbk-l"><span class="k">Run ${R.r} · ${span(R.items[0].date, R.items[R.items.length - 1].date)}${latest ? '<em>Latest</em>' : ''}</span><b>${esc(R.d.head)}</b></span>
      <span class="rbk-p${R.tp <= 25 ? ' r' : ''}"><em>Typical</em>${rkv(R.tp)}</span>
      <span class="rbk-go" aria-hidden="true">${IC.r}</span>
    </button>
    <div class="rbk-x" hidden></div>
  </div>`;
}
/* a concept's divider in Repeats: the idea, how often, how it usually lands, and the reader's line on it */
function cdivHTML(c) { return ideaDivHTML(c); }
function feedHTML() {
  const Mo = M(), W = Mo.f.bands || {};
  const bdivs = [3, 2, 1, 0].map((b) => { const k = Mo.posts.filter((p) => p.band === b).length; return k ? `<div class="bdiv" data-bdiv="${b}"><div class="bd-hd"><span class="bd-n">${RANKW[b]}</span><span class="sub">${plural(k, 'post')}</span>${W[b] ? `<button type="button" class="bd-more" data-bdmore="${b}" aria-expanded="false" aria-label="Why ${RANKW[b]}: what’s in it and why it lands here">Why here${IC.chev}</button>` : ''}</div>${W[b] ? `<p class="bd-h">${esc(W[b].head)}</p><div class="bd-x"><span class="k">What’s in it</span><p>${esc(W[b].what)}</p><span class="k">Why it lands here</span><p>${esc(W[b].why)}</p></div>` : ''}</div>` : ''; });
  return Mo.runs.map(breakerHTML).join('') + Mo.cons.map(cdivHTML).join('') + bdivs.join('') + Mo.posts.map(wtile).join('');
}
function applySort() {
  const feed = $('rd-feed'); if (!feed) return;
  const Mo = M(), posted = S.sort === 'recent', grouped = S.sort === 'concept';
  const wall = $('rd-wall'); wall.classList.toggle('S-recent', posted); wall.classList.toggle('S-concept', grouped); wall.classList.toggle('S-rank', S.sort === 'rank');
  const T = new Map(qa('.tile[data-post]', feed).map((t) => [t.dataset.post, t])), order = [];
  if (posted) for (let r = Mo.runs.length; r >= 1; r--) { order.push(feed.querySelector(`[data-rbk="${r}"]`)); Mo.posts.filter((p) => p.run === r).reverse().forEach((p) => order.push(T.get(p.id))); }
  else if (grouped) Mo.cons.forEach((c) => { order.push(feed.querySelector(`[data-cdiv="${c.id}"]`)); [...c.items].reverse().forEach((p) => order.push(T.get(p.id))); });
  else for (let b = 3; b >= 0; b--) { const a = Mo.posts.filter((p) => p.band === b).sort((x, y) => x.pct - y.pct); if (!a.length) continue; order.push(feed.querySelector(`[data-bdiv="${b}"]`)); a.forEach((p) => order.push(T.get(p.id))); }
  order.forEach((x) => x && feed.appendChild(x));
  feed.classList.toggle('ranked', S.sort === 'rank'); feed.classList.toggle('grouped', grouped);
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
  const set = (v) => { const w = $('rd-wall'); if (w) w.style.overflowAnchor = v; };
  // a shorter hold never cuts a longer one short
  const end = performance.now() + ms; if (MORPH.anchorEnd > end && MORPH.anchor) return;
  MORPH.anchorEnd = end;
  set('none'); clearTimeout(MORPH.anchor); MORPH.anchor = setTimeout(() => { MORPH.anchor = 0; MORPH.anchorEnd = 0; set(''); }, ms);
}
/* where the wall can be seen from: under the header, or under the header's panel while it is open */
function viewTop() {
  if (phone() && S.drop === 'lens') return $('rd-hdrop').getBoundingClientRect().bottom;
  return ht() + hh();
}
// script-made motion on a piece of the wall (the CSS lit wave and transitions are left alone)
const own = (el) => el.getAnimations().filter((a) => !(window.CSSTransition && a instanceof CSSTransition) && !(window.CSSAnimation && a instanceof CSSAnimation));
function morphWall(change) {
  const lens = $('rd-lens'), lh = document.querySelector('.lens-head'), wall = $('rd-wall');
  const panel = phone() && S.drop === 'lens' ? $('rd-hdrop') : null;
  const inside = !!lens && (phone() ? lens.getBoundingClientRect().bottom < ht() + hh() : lh.getBoundingClientRect().bottom < hb());
  const below = () => { const i = $('rd-ins'); return (i && !i.hidden ? i : wall).getBoundingClientRect().top; };
  // down inside the wall, the new order opens at its top: right under the header's panel if you switched from it
  // (the panel stays open over the real lens), right under the stuck lens on desktop (it stays stuck at hb()), and
  // otherwise with the lens's kicker just under the header
  const jump = () => {
    if (!inside) return false;
    hdrLock = performance.now() + 900; acc = 0;
    setCompact(true);
    const dy = panel ? below() - (panel.getBoundingClientRect().bottom + 8) : phone() ? lh.getBoundingClientRect().top - (ht() + 80) : below() - (hb() + lens.offsetHeight);
    scrollTo({ top: Math.max(0, scrollY + dy), behavior: 'instant' });
    return true;
  };
  holdAnchor(RM ? 80 : 2200);
  if (RM) { change(); const j = jump(); MORPH.until = 0; SPY.tops = null; spyBand(true); return { jumped: j, t: 0 }; }
  const vh = innerHeight, v0 = viewTop(), vis = (b, top) => b.height > 0 && b.bottom > top + 2 && b.top < vh;
  const pool = () => qa('#rd-rules, .lens-head, #rd-lens, #rd-ins, #rd-feed > *');
  const parts = '#rd-rules, .lens-head, #rd-lens, #rd-ins, #rd-feed > *, #rd-feed > .bdiv > *, #rd-feed > .bdiv > .bd-x > *';
  // 1. what is showing, as it looks right now
  const A = new Map();
  pool().forEach((el) => {
    const b = el.getBoundingClientRect(); if (!vis(b, v0)) return;
    const cs = getComputedStyle(el), s = { b, op: +cs.opacity };
    if (el.matches('.rbk, .bdiv, .cdiv, #rd-ins')) {
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
const wallSig = () => `${S.f}|${S.sort}|${S.sel}|${S.pick}`;
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
  // what is on screen when the read arrives comes in at once (so no frame of it shows first), after the
  // trajectory and the lens have started (enterPage), in reading order; the rest rises as you scroll to it
  const tiles = qa('#rd-feed > .tile, #rd-feed > .rbk'), vh = innerHeight;
  const now = tiles.map((t) => [t, t.getBoundingClientRect()]).filter(([, r]) => r.height && r.bottom > 0 && r.top < vh - 24)
    .sort(([, a], [, b]) => a.top - b.top || a.left - b.left).map(([t]) => t);
  now.forEach((t, k) => anim(t, [{ opacity: 0, transform: 'translateY(12px)' }, { opacity: rest(t), transform: 'none' }], { dur: 640, delay: 600 + Math.min(k * 28, 380) }));
  const shown = new Set(now);
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
  tiles.forEach((t) => { if (!shown.has(t)) wallIO.observe(t); });
}
function hits(q) {
  const x = pickedSet(); if (!x) return false;
  return x.ids.has(q.id);
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
    // fit the whole trail between the folded header (or the lens stuck under it) and the tab bar; if it is taller, follow
    // the count down. The header is held folded for the fit, so the room it measures is the room there will be
    hold();
    const top = phone() ? ht() + 70 : hbc() + $('rd-lens').offsetHeight + 8;
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
    const sub = `${rkw(p)} · ${esc(beatShort(p).toLowerCase())}`;
    return { lead: tile(p, { still: true, nocap: true, cls: 'lit' }), title: p.label, sub, act: `<button type="button" class="rd-open" data-open="${p.id}">Open</button>`, take: p.take };
  }
  const x = pickedSet(); if (!x) return null;
  const lands = (x.proof || []).length, sinks = (x.against || []).length;
  return { lead: stack(x.items, { w: 34, nocount: true }), title: `Rule ${pad2(x.n)} · ${x.law}`, sub: `${x.L.length} should land · ${x.Sk.length} should sink`, act: '', take: x.because || '' };
}
function renderReadout() {
  const r = picked(), hdr = $('rd-hdr'), was = hdr.classList.contains('reading');
  hdr.classList.toggle('reading', !!r);
  if (!r) { if (S.drop === 'take') closeDrop(); setCompact(scrollY <= 30 ? false : S.compact); return; }
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
  qa('#rd-rules [data-rule]').forEach((m) => { const on = S.pick === m.dataset.rule; m.setAttribute('aria-pressed', String(on)); m.firstChild.textContent = on ? 'Lit on the wall' : 'Light it up on the wall'; });
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
/* one way in from the track: which tab, and which way along it. Each tab is its own order of the wall */
function setLens(k) {
  const cur = lensKey(); if (tabIx(k) < 0 || k === cur) return;
  setSort(SORT_OF[k], Math.sign(tabIx(k) - tabIx(cur)));
}
/* a rule, from the rules: its posts light up on the wall (in whichever order the wall is in), and the wall comes
   into view under the lens */
function lightRule(id) {
  const on = String(S.pick) !== String(id);
  pick(id);
  if (!on) return;
  const lh = document.querySelector('#rd-main .lens-head'); if (!lh) return;
  quietTo(scrollY + lh.getBoundingClientRect().top - (phone() ? hbc() : hbc() + 8));
}
/* a run's read closes at once (leaving Runs: the breaker is going anyway, and the switch's own sweep covers it) */
function closeRunNow() {
  if (!S.tr) return;
  const b = document.querySelector(`.rbk[data-rbk="${S.tr}"] .rbk-x`);
  if (b) { b.innerHTML = ''; b.hidden = true; const rb = b.closest('.rbk'); rb.removeAttribute('data-open'); rb.querySelector('.rbk-hd').setAttribute('aria-expanded', 'false'); }
  S.tr = null;
}
/* Runs, Repeats and Ranks change the wall's order: the wall turns over in one sweep (morphWall). With a pick
   riding along, the insight card and the lit posts are part of the new layout and arrive with the sweep,
   so the card's own glide never pulls on posts the sweep is moving. Switched from the header's panel while
   down in the wall, the panel stays open and the new order starts right under it. */
function setSort(v, dir, pk) {
  if (v === S.sort) return;
  const L = MORPH.last, pre = wallSig(); SPY.goal = null;
  if (pk != null) { letGo(); S.sel = null; }
  S.sort = v; buzz(); clearTrail(true); if (pk != null) S.pick = pk;
  renderLens(true, dir || (v === 'rank' ? 1 : -1));
  const change = () => { if (v !== 'recent') closeRunNow(); applySort(); decorate(false); renderFocusUI('place'); };
  const live = L && !RM && performance.now() < MORPH.until && L.anims.every((a) => a.playState !== 'idle') && L.ghosts.every((g) => g.isConnected);
  const sig = wallSig(), turn = live && (L.dir > 0 ? sig === L.pre : sig === L.post);
  // changed your mind while the wall is still turning over: it turns back (or on again) from where it is
  void turn; const r = RM ? (change(), { t: 0 }) : flipSort(change);
  if (S.sel && v === 'recent') TR.timers.push(setTimeout(() => playTrail(M().byId[S.sel]), RM ? 0 : r.t + 60));
}
/* A tap anywhere off the highlight gets you out. On the wall that tap is used up; a control
   elsewhere also does its own job. */
function outsideTap(e) {
  if (ovOpen) return false;
  const t = e.target;
  if (S.drop && !t.closest('#rd-hdrop, #rd-hdr, #rd-rail')) { closeDrop(); return true; }
  const lit = !!S.sel || S.pick != null;
  if (!lit) return false;
  if (t.closest('#rd-hdr, #rd-rail, #rd-hdrop, #rd-lens, [data-fm-bottom-nav], #rd-ins, #rd-rules, .rbk-x, .bd-more')) return false;
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
  const svg = `<svg class="ch" preserveAspectRatio="xMinYMax meet" viewBox="0 -4 ${W} ${H + 14}" role="img" aria-label="${esc(beatShort(p).replace(/[▲▼] /, ''))}">${s}</svg>`;
  if (compact) return svg;
  return `<div class="box res">${svg}<p>${beatLine(p)}</p><div class="key"><span><i style="background:var(--rose)"></i>It beat these</span><span><i style="background:var(--down)"></i>These did better</span><span><i style="background:var(--ink)"></i>This reel</span></div></div>`;
}
/* the post mortem: one post, dissected. What it was, why it landed where it did, and what it did to the rules,
   then the evidence: the streak against the reels before it, what happens in it, every go at its idea */
function postView(p) {
  const Mo = M(), c = Mo.cById[p.concept], sib = c ? c.items : [p], m = p.mortem || null, R = Mo.runs[p.run - 1], nth = R.items.indexOf(p) + 1;
  const beats = (p.flow || []).slice(0, 6);
  const fx = m ? (m.fx || []).filter((x) => x.m !== 'none').map((x) => { const r = x.r && Mo.ruleById[x.r]; return `<li><span class="${moveC(x.m)}">${esc(moveW(x.m))}</span>${r ? `<b>${r.gone ? 'Dropped: ' : ''}${esc(r.law)}</b>` : ''}${esc(x.line)}</li>`; }).join('') : '';
  const streak = p.i === 0 ? ['—', 'first in memory'] : p.ripple ? [`Beat ${p.ripple}`, p.ripple === p.i ? 'every post before it' : 'posts before it', 'r'] : [`${p.shortOf}<small> did better</small>`, p.shortOf === 1 ? 'right before it' : 'in a row before it'];
  return `<div class="pv lkp pv2">
    <div class="pv2-l rv">
      <div class="pv2-cv">${tile(p, { still: true, nocap: true, cls: 'hero lit' })}</div>
      ${bmx([[rkw(p), 'where it landed', p.band >= 2 ? 'r' : ''], [`${ord(p.rank)}<small> of ${Mo.n}</small>`, 'in his memory'], streak])}
    </div>
    <div class="pv2-r">
      <div class="pv2-hd rv">${ktop(`${Mo.runs.length > 1 ? `Run ${p.run} · ` : ''}post ${nth} of ${R.items.length}`, `${fd(p.date)}${p.length ? ' · ' + esc(dur(p.length) || p.length) : ''}${p.paid ? ' · paid' : ''}`, p.band >= 2 ? 'r' : '')}<h2>${esc(p.label)}</h2></div>
      <section class="sec rv">${m ? `<span class="k">Why it landed here</span><p class="lk-p">${esc(m.why)}</p><span class="k">What it was</span><p class="lk-fp">${esc(m.what)}</p>` : `<p class="lk-p">${esc(p.take)}</p>`}</section>
      ${fx ? `<section class="sec rv"><span class="k">What it did to my rules</span><ul class="lk-ch">${fx}</ul></section>` : ''}
      <section class="sec rv"><span class="k">Against the posts before it</span><div class="lk-res res">${resChart(p, true)}</div><p class="lk-fp">${beatLine(p)}</p></section>
      ${beats.length ? `<section class="sec rv"><span class="k">What happens in it</span><ol class="beats">${beats.map((b) => `<li><span>${esc(b.t || '•')}</span><span>${esc(b.beat)}</span></li>`).join('')}</ol></section>` : p.hook ? `<section class="sec rv"><span class="k">On screen</span><p class="lk-fp">“${esc(p.hook)}”</p></section>` : ''}
      ${c && sib.length > 1 ? `<section class="sec rv"><button type="button" class="pr" data-concept="${c.id}"><span class="pr-x">×${sib.length}</span><span class="pr-t"><span class="pr-k"><em>An idea ${who() === 'his' ? 'he keeps' : 'they keep'} making · go ${sib.indexOf(p) + 1} of ${sib.length}</em></span><strong>${esc(c.name)}</strong></span>${IC.r}</button></section>` : ''}
    </div>
  </div>`;
}
/* an idea the account keeps making: how often, how it usually lands, the reader's line, every go at it */
function conceptView(c) { return ideaView(c); }
const ovTitle = (e) => (e.type === 'post' ? M().byId[e.id].label : e.type === 'run' ? `Run ${e.id}` : M().cById[e.id].short);
function renderOv() {
  const top = S.stack[S.stack.length - 1], Mo = M();
  $('rd-obody').innerHTML = top.type === 'post' ? postView(Mo.byId[top.id]) : top.type === 'run' ? runView(Mo.runs[top.id - 1]) : conceptView(Mo.cById[top.id]);
  $('rd-obody').scrollTop = 0; $('rd-shd').classList.remove('stuck');
  $('rd-miniTitle').textContent = ovTitle(top);
  const prev = S.stack[S.stack.length - 2];
  $('rd-backSlot').innerHTML = prev ? `<button type="button" class="back" data-back>${IC.l}<span>${esc(ovTitle(prev))}</span></button>` : '';
  let acts = '';
  if (top.type === 'post') { const p = Mo.byId[top.id], o = Mo.posts[p.i - 1], n = Mo.posts[p.i + 1]; acts += `<button type="button" class="ib" ${o ? `data-step="${o.id}"` : 'disabled'} aria-label="Older reel">${IC.l}</button><button type="button" class="ib" ${n ? `data-step="${n.id}"` : 'disabled'} aria-label="Newer reel">${IC.r}</button>`; }
  $('rd-acts').innerHTML = acts + `<button type="button" class="ib" id="rd-closeOv" aria-label="Close">${IC.x}</button>`;
  fixPods($('rd-obody')); mountBoard($('rd-obody'));
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
function openEntry(entry, src, home) {
  hideTip(); buzz(); closeDrop();
  if (ovOpen) { const from = entry.type === 'post' && !RM ? flyFrom(entry.id, src) : null; S.stack.push(entry); swapOv(1, from && { ...from, p: M().byId[entry.id] }); return; }
  const from = entry.type === 'post' && !RM ? flyFrom(entry.id, src) : null;
  const org = !from && src && src.getBoundingClientRect ? src.getBoundingClientRect() : null;
  S.stack = [entry];
  renderOv();
  const ov = $('rd-ov'), sheet = $('rd-sheet'), scrim = $('rd-scrim');
  [sheet, scrim].forEach((el) => el.getAnimations().forEach((a) => a.cancel()));
  qa('.cfly').forEach((g) => g.getAnimations().forEach((a) => a.cancel()));
  sheet.style.transform = ''; sheet.style.transformOrigin = '';
  ov.classList.add('on'); ov.setAttribute('aria-hidden', 'false'); ovOpen = true;
  // in the app the page scrolls on the root element, so that is what the sheet locks
  if (!bodyLock) { bodyLock = true; bodyPrev = document.documentElement.style.overflow; }
  document.documentElement.style.overflow = 'hidden';
  FLY.id = entry.type === 'post' ? entry.id : null; FLY.el = null;
  if (RM) { sheet.style.opacity = 1; scrim.style.opacity = 1; return; }
  sheet.style.opacity = '';
  // measured with the sheet at rest, before anything moves it
  const to = from ? heroRect() : null, sr = sheet.getBoundingClientRect();
  scrim.animate([{ opacity: 0 }, { opacity: 1 }], { duration: 420, easing: 'cubic-bezier(.2,.7,.3,1)', fill: 'forwards' });
  if (phone()) sheet.animate([{ opacity: 1, transform: 'translateY(100%)' }, { opacity: 1, transform: 'none' }], { duration: 700, easing: EZ.sheet, fill: 'forwards' });
  else {
    // the sheet grows out of what you tapped
    const o = from ? from.r : org;
    if (o) sheet.style.transformOrigin = `${(o.left + o.width / 2 - sr.left).toFixed(0)}px ${(o.top + o.height / 2 - sr.top).toFixed(0)}px`;
    sheet.animate([{ opacity: 0, transform: 'scale(.9)' }, { opacity: 1, offset: 0.4 }, { opacity: 1, transform: 'none' }], { duration: 640, easing: EZ.sheet, fill: 'forwards' });
  }
  if (from && to) {
    const p = M().byId[entry.id], g = cardAt(p, from.r, from.rad);
    FLY.el = home || from.el; setAway(from.el.closest('.pk-v') ? FLY.el : from.el);
    to.hero.style.visibility = 'hidden';
    flyCard(g, from.r, from.rad, to.r, to.rad, { dur: 640, ease: EZ.fly, land: () => { to.hero.style.visibility = ''; to.hero.classList.add('landed'); } });
  }
}
function swapOv(dir, from) {
  const body = $('rd-obody');
  if (RM) { renderOv(); return; }
  body.getAnimations().forEach((x) => x.cancel());
  if (from && dir > 0) {
    // a cover tapped inside the sheet lifts out while the read under it gives way, then lands as the new hero
    const g = cardAt(from.p, from.r, from.rad);
    g.animate([{ transform: 'none' }, { transform: 'scale(1.06)' }], { duration: 150, easing: 'cubic-bezier(.3,0,.2,1)', fill: 'forwards' });
    body.animate([{ opacity: 1, transform: 'none' }, { opacity: 0, transform: 'translateY(-10px)' }], { duration: 150, easing: 'cubic-bezier(.4,0,1,1)', fill: 'forwards' }).onfinish = () => {
      if (!alive) { g.remove(); return; }
      renderOv(); body.getAnimations().forEach((x) => x.cancel());
      const to = heroRect();
      body.animate([{ opacity: 0 }, { opacity: 1 }], { duration: 360, easing: 'ease-out' });
      g.getAnimations().forEach((x) => x.cancel());
      const a = from.r, s = 1.06, la = { left: a.left - (a.width * (s - 1)) / 2, top: a.top - (a.height * (s - 1)) / 2, width: a.width * s, height: a.height * s };
      if (to) { to.hero.style.visibility = 'hidden'; flyCard(g, la, from.rad * s, to.r, to.rad, { dur: 620, ease: EZ.fly, land: () => { to.hero.style.visibility = ''; to.hero.classList.add('landed'); } }); }
      else g.remove();
    };
    return;
  }
  body.animate([{ opacity: 1, transform: 'none' }, { opacity: 0, transform: `translateX(${-dir * 24}px)` }], { duration: 170, easing: 'cubic-bezier(.4,0,1,1)', fill: 'forwards' }).onfinish = () => {
    if (!alive) return;
    renderOv(); body.getAnimations().forEach((x) => x.cancel());
    body.animate([{ opacity: 0, transform: `translateX(${dir * 24}px)` }, { opacity: 1, transform: 'none' }], { duration: 520, easing: EZ.sheet });
  };
}
function stepTo(id, dir) { S.stack[S.stack.length - 1] = { type: 'post', id }; buzz(); swapOv(dir); }
function closeOv() {
  if (!ovOpen) return;
  ovOpen = false; buzz();
  const ov = $('rd-ov'), sheet = $('rd-sheet'), scrim = $('rd-scrim');
  const done = () => { ov.classList.remove('on'); ov.setAttribute('aria-hidden', 'true'); if (bodyLock) document.documentElement.style.overflow = bodyPrev; bodyLock = false; sheet.style.transform = ''; sheet.style.transformOrigin = ''; [sheet, scrim].forEach((el) => el.getAnimations().forEach((x) => x.cancel())); sheet.style.opacity = ''; scrim.style.opacity = ''; };
  if (RM) { setAway(null); done(); return; }
  const top = S.stack[S.stack.length - 1], body = $('rd-obody');
  const hero = top && top.type === 'post' ? body.querySelector('.tile.hero') : null;
  const home = hero ? homeFor(top.id) : null;
  if (!home) qa('.v15-away').forEach((x) => { x.classList.remove('v15-away'); x.animate([{ opacity: 0 }, { opacity: 1 }], { duration: 360, easing: 'ease-out' }); });
  if (hero && home) {
    let a = hero.getBoundingClientRect(); const ra = pxR(hero, a), br = body.getBoundingClientRect();
    // the hero scrolled away inside the sheet: the cover leaves from the top of what you can see instead
    const gone = a.bottom < br.top + 40;
    if (gone) a = { left: a.left, top: br.top + 12, width: a.width, height: a.height };
    const p = M().byId[top.id], g = cardAt(p, a, ra);
    hero.style.visibility = 'hidden';
    const keep = home.el.classList.contains('v15-away') ? null : home.el;
    if (keep) keep.style.visibility = 'hidden';
    flyCard(g, a, ra, home.r, pxR(home.el, home.r), { dur: 560, ease: EZ.home, fadeIn: gone, land: () => { if (keep) keep.style.visibility = ''; setAway(null); landPulse(home.el.closest('.tile, .dk-c, .lc, .pr, .evr') || home.el); } });
  }
  scrim.animate([{ opacity: 1 }, { opacity: 0 }], { duration: 440, easing: 'cubic-bezier(.4,0,.2,1)', fill: 'forwards' });
  const cur = getComputedStyle(sheet).transform;
  let an;
  if (phone()) an = sheet.animate([{ opacity: 1, transform: cur === 'none' ? 'none' : cur }, { opacity: 1, transform: 'translateY(100%)' }], { duration: 440, easing: EZ.away, fill: 'forwards' });
  else {
    if (home) { const sr = sheet.getBoundingClientRect(); sheet.style.transformOrigin = `${(home.r.left + home.r.width / 2 - sr.left).toFixed(0)}px ${(home.r.top + home.r.height / 2 - sr.top).toFixed(0)}px`; }
    an = sheet.animate([{ opacity: 1, transform: 'none' }, { opacity: 0, transform: 'scale(.94)' }], { duration: 340, easing: EZ.away, fill: 'forwards' });
  }
  an.onfinish = () => { if (alive) done(); };
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
  t.innerHTML = `${tile(p, { still: true, nocap: true, cls: 'lit' })}<div><b>${esc(p.label)}</b><span>${fd(p.date)} · ${rkw(p)} · ${esc(beatShort(p).toLowerCase())}</span></div>`;
  const r = el.getBoundingClientRect();
  t.style.left = Math.min(innerWidth - 300, Math.max(10, r.left + r.width / 2 - 145)) + 'px';
  t.style.top = (r.top - 84 < 120 ? r.bottom + 10 : r.top - 84) + 'px';
  t.classList.add('on');
}
function hideTip() { $('rd-tip').classList.remove('on'); }
listen(document, 'pointerover', (e) => { const el = e.target.closest('#rd-feed .tile[data-post]'); if (el) showTip(el); });
listen(document, 'pointerout', (e) => { if (e.target.closest('#rd-feed .tile')) hideTip(); });

/* ---------- the read, v13: every component answers its own question, in bold, at full width ----------
   The formula, tested: posts that hit two rules against posts that missed one.
   A rule: what is it worth (two ledges, the second falls by what missing it costs), what happens in the posts
   that made it, and how I got there run by run.
   What the details say: patterns read off the posts themselves, as a stack of cards you tap through.
   A run, or an idea he keeps making: one pop-up, the same frame for both. Its posts rise one after another to
   the height each landed (the trajectory), then what it taught me.
   Ranks already shows where things landed; none of this repeats it. */
let FSEQ = 0;
const PDATA = new Map();
let boardIO = null;

/* the words for a rank: strong posts by how near the top, weak ones by how near the bottom */
const bot = (p) => Math.max(1, Math.round(100 - p.pct + 100 / M().n));
const rkw = (p) => (p.pct <= 50 ? `Top ${pcx(p)}%` : `Bottom ${bot(p)}%`);
const rkv = (v) => (v <= 50 ? `Top ${Math.max(1, Math.round(v))}%` : `Bottom ${Math.max(1, Math.round(100 - v + 100 / M().n))}%`);
const STATUSW = { born: 'New in run', sharpened: 'Stronger after run', held: 'Held through run', tweaked: 'Narrowed in run', strains: 'Strained in run', breaks: 'Broke in run', dropped: 'Dropped in run', backs: 'Backed in run' };
const VERDICT = { called: 'It held', missed: 'It didn’t hold', open: 'Still open' };
const byIds = (ids) => (ids || []).map((id) => M().byId[id]).filter(Boolean);
const byRank = (a) => [...a].sort((x, y) => x.rank - y.rank);
const mx = (cols) => `<div class="lk-mx">${cols.map(([b, l, c]) => `<span><b class="${c || ''}">${b}</b>${l}</span>`).join('')}</div>`;
const bmx = (cols) => `<div class="bmx">${cols.map(([b, l, c]) => `<span><b class="${c || ''}">${b}</b><em>${l}</em></span>`).join('')}</div>`;
const ktop = (l, r, cl = '', cr = '') => `<div class="lk-top"><span class="${cl}">${l}</span><span class="${cr}">${r}</span></div>`;
const medPct = (a) => med(a.map((p) => p.pct));
const places = (pts) => Math.round((pts * M().n) / 100);
const tAt = (pct) => { const lo = 100 / M().n; return clamp01((pct - lo) / (100 - lo)); };
const cover = (p) => (p.thumb ? `<img src="${p.thumb}" alt="" decoding="async" draggable="false">` : `<span class="cv-f" style="--g1:${(SCENES[p.scene] || SCENES.plain)[0]};--g2:${(SCENES[p.scene] || SCENES.plain)[1]}"></span>`);
const words = (t) => String(t).split(/\s+/).filter(Boolean).map((w, j) => `<span class="w"><span style="--j:${j}">${w}</span></span>`).join(' ');
function syncRuleBtns(root) { qa('[data-rule]', root).forEach((m) => { const on = S.pick === m.dataset.rule; m.setAttribute('aria-pressed', String(on)); m.firstChild.textContent = on ? 'Lit on the wall' : 'Light it up on the wall'; }); }
// which of my live rules a post carries, and which it misses
function sig(p) { let has = 0, miss = 0; M().rules.forEach((r) => { if (r.L.includes(p.id)) has++; else if (r.Sk.includes(p.id)) miss++; }); return { has, miss }; }

/* ---------- a deck: posts fanned on a ledge; the front card is the strongest case ---------- */
function deck(ps, cls) {
  return `<span class="dk ${cls}" style="--n:${ps.length}">${ps.map((p, k) => `<button type="button" class="dk-c" data-post="${p.id}" style="--i:${k};z-index:${ps.length - k}" aria-label="${esc(p.label)}, ${rkw(p)}">${cover(p)}<span class="dk-r">${rkw(p)}</span></button>`).join('')}</span>`;
}

/* ---------- the formula, tested ---------- */
function formulaHTML() {
  const Mo = M(), half = Mo.n / 2;
  const two = byRank(Mo.posts.filter((p) => { const s = sig(p); return s.has >= 2 && !s.miss; }));
  const miss = byRank(Mo.posts.filter((p) => sig(p).miss > 0)).reverse();
  if (two.length < 3 || miss.length < 3) return '';
  const up = two.filter((p) => p.rank <= half).length, dn = miss.filter((p) => p.rank > half).length;
  // the posts that bear it out first, the exceptions last (dimmed)
  const order = (all, ok) => [...all.filter(ok), ...all.filter((p) => !ok(p))];
  const row = (cls, n, of, text, all, ok) => `<div class="fm-row ${cls}"><span class="fm-big"><b data-to="${n}">${n}</b><small>of ${of}</small></span><p>${text}</p><div class="fm-dk">${deck(order(all, ok), cls === 'up' ? 'r' : 'w').replace(/class="dk-c" data-post="(\w+)"/g, (m, id) => (ok(Mo.byId[id]) ? m : `class="dk-c off" data-post="${id}"`))}</div></div>`;
  return `<div class="fm" data-fm>
    ${row('up', up, two.length, 'posts that hit two of my rules landed in his <b>top half</b>.', two, (p) => p.rank <= half)}
    ${row('dn', dn, miss.length, 'posts that missed one sank into his <b>bottom half</b>.', miss, (p) => p.rank > half)}
  </div>`;
}

/* ---------- a rule: what it is worth, what happens in its posts, how I got there ---------- */
const whyOf = (p) => (p.mortem && p.mortem.why) || p.line || p.take || '';
function evidenceHTML(r, gone) {
  const Mo = M(), brk = new Set(r.brokenBy);
  const land = byRank(byIds(r.L).filter((p) => !brk.has(p.id))).slice(0, 2);
  const sink = byRank(byIds(r.Sk).filter((p) => !brk.has(p.id))).reverse().slice(0, 1);
  const br = byRank(byIds(r.brokenBy)).slice(0, 1);
  const row = (p, tag, cls, text) => `<button type="button" class="evr ${cls}" data-post="${p.id}"><span class="evr-i">${cover(p)}</span><span class="evr-t"><span class="evr-k"><b>${rkw(p)}</b><em>${tag}</em></span><strong>${esc(p.label)}</strong><span class="evr-x">${esc(text)}</span></span></button>`;
  const rows = [...land.map((p) => row(p, gone ? 'It named it' : 'Has it, landed', 'up', whyOf(p))), ...sink.map((p) => row(p, 'Lacks it, sank', 'dn', whyOf(p))), ...br.map((p) => row(p, 'Breaks it', 'x', (r.breaks || {})[p.id] || whyOf(p)))];
  return rows.length ? `<div class="ev"><span class="k">What happens in them</span>${rows.join('')}</div>` : '';
}
function lifeHTML(r) {
  const life = r.life || []; if (!life.length) return '';
  return `<div class="lf"><span class="k">How I got here</span><ol>${life.map((x, k) => `<li class="${moveC(x.move)}${k === life.length - 1 ? ' last' : ''}"><span class="lf-h"><b>Run ${x.run}</b><em>${esc(moveW(x.move))}</em></span><p>${esc(x.line)}</p></li>`).join('')}</ol></div>`;
}
function plateHTML(r, gone) {
  const Mo = M(), N = Mo.runs.length, life = r.life || [];
  const has = byRank(byIds(r.L)), lacks = byRank(byIds(r.Sk)).reverse();
  const mh = has.length ? medPct(has) : null, ml = lacks.length ? medPct(lacks) : null;
  const w = mh != null && ml != null ? places(ml - mh) : null;
  const id = 'dp' + (++FSEQ); PDATA.set(id, { r, has, lacks, mh, ml, w });
  const body = gone ? String(r.line || '').replace(/^Dropped in run \d+\.\s*/, '') : r.because;
  const alt = (v) => (v / 100).toFixed(4);
  const ledge = (ps, v, cls, lab) => `<div class="dp-l ${cls}" style="--a:${alt(v)}">${deck(ps, cls === 'has' ? 'r' : 'w')}<i class="dp-rule"></i><span class="dp-lab"><b>${lab}</b> · ${ps.length} · ${rkv(v)}</span><span class="dp-cap" aria-hidden="true"></span></div>`;
  const ev = evidenceHTML(r, gone), lf = lifeHTML(r), evN = (ev.match(/class="evr /g) || []).length;
  const fold = (key, label, count, html) => (html ? `<button type="button" class="rk-fb" data-fold="${key}" aria-expanded="false"><span>${label}</span><b>${count}</b><i aria-hidden="true"></i></button>` : '');
  const kick = gone ? `<em class="dn">Dropped</em>${life.length ? ` · Runs ${life[0].run}–${life[life.length - 1].run}` : ''}` : `<em>Rule ${pad2(r.n)}</em> · ${STATUSW[r.status] || 'Held through run'} ${N}`;
  const big = w != null ? [String(w), Math.abs(w) === 1 ? 'place' : 'places'] : ['—', 'no misses yet'];
  const meta = gone ? `<span>It never moved him</span>${has.length ? ` · <span>named <b>${has.length}</b></span>` : ''}` : `<span>With it <b class="r">${rkv(mh)}</b></span>${ml != null ? ` · <span>without <b>${rkv(ml)}</b></span>` : ''}`;
  const bid = 'rlb' + FSEQ;
  return `<article class="rp rl-i${gone ? ' gone' : ''}" data-rs="${r.id}">
    <button type="button" class="rl-hd" aria-expanded="false" aria-controls="${bid}">
      <span class="rl-n${gone ? ' dim' : ''}"><b>${gone && w == null ? '0' : big[0]}</b><small>${gone && w == null ? 'places' : big[1]}</small></span>
      <span class="rl-t"><span class="rl-k">${kick}</span><span class="rs-law">${esc(r.law)}</span><span class="rl-m">${meta}</span></span>
      <i class="rl-x" aria-hidden="true"></i>
    </button>
    <div class="rl-bd" id="${bid}" hidden><div class="rl-in">
      <p class="rs-bc">${esc(body)}</p>
      <div class="dp${w == null ? ' solo' : ''}" data-dp="${id}" style="--wf:${w == null ? 0 : clamp01(w / Mo.n).toFixed(3)}" role="group" aria-label="${w != null ? `Posts with it typically land ${rkv(mh)}; without it, ${rkv(ml)}. Missing it costs ${w} places.` : `Its posts typically land ${rkv(mh)}.`}">
        <i class="dp-g t"><span>His best</span></i><i class="dp-g b"><span>His worst</span></i>
        ${has.length ? ledge(has, mh, 'has', gone ? 'What it named' : 'With it') : ''}
        ${lacks.length ? ledge(lacks, ml, 'lacks', 'Without it') : ''}
        ${w != null ? `<div class="dp-w" style="--a:${alt(mh)};--b:${alt(ml)}"><i class="dp-drop"></i><span class="dp-v"><b class="dp-num">${w}</b><small>${Math.abs(w) === 1 ? 'place' : 'places'}</small></span><em>${w >= 0 ? 'what missing it costs' : 'it works the other way'}</em></div>` : `<div class="dp-w none" style="--a:${alt(mh)};--b:${alt(mh)}"><span class="dp-v"><b class="dp-num">0</b><small>places</small></span><em>${gone ? 'it never moved him' : 'nothing without it yet'}</em></div>`}
      </div>
      <div class="rl-ctl">
        <div class="rk-fbs">${fold('ev', gone ? 'What it named' : 'What happens in them', evN, ev)}${fold('lf', 'How I got here', `${life.length} ${life.length === 1 ? 'run' : 'runs'}`, lf)}</div>
        <div class="rs-ft"><span>Speaks for <b>${r.reach}</b> of ${Mo.n}</span><span><b>${r.carry}</b> of his top ${r.quarter}</span><span>Held <b>${r.held}</b> of ${r.calls}</span>${gone ? '' : `<button type="button" class="rs-go" data-rule="${r.id}" aria-pressed="${S.pick === r.id}">Light it up on the wall${IC.r}</button>`}</div>
      </div>
      ${ev ? `<div class="rk-fold" data-fbody="ev" hidden>${ev}</div>` : ''}
      ${lf ? `<div class="rk-fold" data-fbody="lf" hidden>${lf}</div>` : ''}
    </div></div>
  </article>`;
}
function openHTML() {
  const op = M().rd.open || []; if (!op.length) return '';
  const bid = 'rlb' + (++FSEQ);
  return `<article class="rp rl-i open" data-rs="open">
    <button type="button" class="rl-hd" aria-expanded="false" aria-controls="${bid}">
      <span class="rl-n dim"><b>${op.length}</b><small>${op.length === 1 ? 'question' : 'questions'}</small></span>
      <span class="rl-t"><span class="rl-k"><em class="dn">Still open</em></span><span class="rs-law">What I can’t call yet</span><span class="rl-m">${esc(String(op[0].q).split(/[.?]/)[0])}${op.length > 1 ? ` · and ${op.length - 1} more` : ''}</span></span>
      <i class="rl-x" aria-hidden="true"></i>
    </button>
    <div class="rl-bd" id="${bid}" hidden><div class="rl-in">
      ${op.map((o) => `<div class="oq"><p>${esc(o.q)}</p><div class="oq-dk">${deck(byRank(byIds(o.posts)), 'q')}</div></div>`).join('')}
    </div></div>
  </article>`;
}

/* ---------- what the details say: patterns read off the posts themselves ---------- */
function secsOf(p) {
  const x = String(p.length || '');
  let m = x.match(/^(\d+):(\d\d)$/); if (m) return +m[1] * 60 + +m[2];
  m = x.match(/^(\d+(?:\.\d+)?)s?$/); if (m) return +m[1];
  const f = p.flow || [], t = f.length ? String(f[f.length - 1].t).match(/(\d+):(\d\d)\s*$/) : null;
  return t ? +t[1] * 60 + +t[2] : null;
}
function insightsData() {
  const Mo = M(), ps = Mo.posts, half = Mo.n / 2, out = [];
  const typ = (a) => Math.round(med(a.map((p) => p.rank)));
  const low = (a) => a.filter((p) => p.rank > half).length;
  const names = (a, k = 3) => a.slice(0, k).map((p) => p.label.charAt(0).toLowerCase() + p.label.slice(1)).join('; ');
  // 1. what carries a post
  const DRVH = { moment: 'Riding a moment', faces: 'Riding who’s in it', idea: 'On the idea alone', craft: 'Leaning on craft' };
  const DRVL = { moment: 'a moment', faces: 'who’s in it', idea: 'the idea alone', craft: 'craft' };
  const g = Object.keys(DRVH).map((k) => ({ k, ps: byRank(ps.filter((p) => p.driver === k)) })).filter((x) => x.ps.length >= 3).map((x) => ({ ...x, t: typ(x.ps) })).sort((a, b) => a.t - b.t);
  if (g.length >= 2 && g[g.length - 1].t - g[0].t >= Mo.n * 0.25) {
    const a = g[0], b = g[g.length - 1];
    out.push({ tag: 'What carries a post', head: `${DRVH[a.k]}, he lands ${ord(a.t)}. ${DRVH[b.k]}, ${ord(b.t)}.`, proof: `His typical landing by what carries the post: ${g.map((x) => `${DRVL[x.k]} ${ord(x.t)} (${x.ps.length})`).join(', ')}.`, mx: [[ord(a.t), DRVL[a.k], 'r'], [ord(b.t), DRVL[b.k]]], posts: a.ps, cover: a.ps[0] });
  }
  // 2. who it is for
  const lost = byRank(ps.filter((p) => /^(unclear|nobody)/i.test(String(p.speaks || '').trim())));
  if (lost.length >= 3 && low(lost) / lost.length >= 0.8) out.push({ tag: 'Who it’s for', head: 'When I can’t tell who it’s for, it sinks.', proof: `${low(lost)} of ${lost.length} landed in his bottom half: ${names(lost.slice().reverse())}.`, mx: [[`${low(lost)}/${lost.length}`, 'sank', 'r'], [ord(typ(lost)), 'typical landing']], posts: lost.slice().reverse(), cover: lost[lost.length - 1] });
  // 3. hooks that talk straight at you
  const AT = /\b(guys|pov|bhai|bhaiya|y'?all)\b/i, at = byRank(ps.filter((p) => AT.test(p.hook || '')));
  if (at.length >= 4 && low(at) / at.length >= 0.7) {
    const toks = [...new Set(at.map((p) => (String(p.hook).match(AT) || [''])[0].toLowerCase().replace("y'all", 'y’all').replace('yall', 'y’all')))].slice(0, 4).map((t) => `“${t === 'pov' ? 'POV' : t}”`);
    out.push({ tag: 'The hook', head: 'Hooks that talk at the camera mostly sink.', proof: `${toks.join(', ')}: ${low(at)} of ${at.length} of those hooks landed in his bottom half.`, mx: [[`${low(at)}/${at.length}`, 'sank', 'r'], [ord(typ(at)), 'typical landing']], posts: at.slice().reverse(), cover: at[at.length - 1] });
  }
  // 4. one bit of slang, both ends of his memory
  const SL = ['rn', 'bro', 'fr', 'lowkey', 'ngl', 'yaar', 'literally'];
  const sl = SL.map((t) => ({ t, ps: byRank(ps.filter((p) => new RegExp(`\\b${t}\\b`, 'i').test(p.hook || ''))) })).filter((x) => x.ps.length >= 3).map((x) => ({ ...x, span: x.ps[x.ps.length - 1].rank - x.ps[0].rank })).sort((a, b) => b.span - a.span)[0];
  if (sl && sl.span >= half) out.push({ tag: 'The slang', head: `“${sl.t}” isn’t the joke. What comes before it is.`, proof: sl.ps.map((p) => `“${String(p.hook).trim()}” came ${ord(p.rank)}`).join('. ') + '.', mx: [[ord(sl.ps[0].rank), 'best', 'r'], [ord(sl.ps[sl.ps.length - 1].rank), 'worst']], posts: sl.ps, cover: sl.ps[0] });
  // 5. length
  const withS = ps.map((p) => ({ p, s: secsOf(p) })).filter((x) => x.s != null);
  const sh = byRank(withS.filter((x) => x.s <= 20).map((x) => x.p)), lg = byRank(withS.filter((x) => x.s >= 60).map((x) => x.p));
  if (sh.length >= 3 && lg.length >= 3) {
    const ts = typ(sh), tl = typ(lg), close = Math.abs(ts - tl) <= Mo.n * 0.1;
    out.push({ tag: 'Length', head: close ? 'Length doesn’t decide it.' : ts < tl ? 'Short wins.' : 'Long wins.', proof: `Under 20 seconds (${sh.length} posts) he typically lands ${ord(ts)}. Over a minute (${lg.length}), ${ord(tl)}.`, mx: [[ord(ts), 'under 20s', ts <= tl ? 'r' : ''], [ord(tl), 'over a minute', tl < ts ? 'r' : '']], posts: [...sh.slice(0, 3), ...lg.slice(0, 3)], cover: sh[0] });
  }
  return out;
}
const slots = (t) => String(t).split(/\s+/).filter(Boolean).map((w, j) => `<span class="ws"><span style="--j:${j}">${esc(w)}</span></span>`).join(' ');
function insightsHTML() {
  const L = insightsData(); if (L.length < 2) return '';
  const n = L.length;
  return `<section class="dt" data-dt aria-roledescription="carousel" aria-label="What the details say">
    <div class="dt-hd"><span class="tj-k"><i></i>What the details say</span><span class="dt-ct"><b>01</b> / ${pad2(n)}</span></div>
    <span class="dt-bar" aria-hidden="true">${L.map((_, k) => `<i class="${k ? '' : 'on'}"><b></b></i>`).join('')}</span>
    <div class="dt-track" tabindex="0">
      ${L.map((x, k) => `<article class="dt-s${k ? '' : ' on'}" data-k="${k}" aria-roledescription="slide" aria-label="${k + 1} of ${n}: ${esc(x.tag)}">
        <div class="dt-tx">
          <span class="dt-tag">${esc(x.tag)}</span>
          <div class="dt-big">${x.mx.map(([v, l, c], j) => `<span class="dt-n ${j ? 'w' : 'r'}"><b><span>${esc(v)}</span></b><em>${esc(l)}</em></span>`).join('<i class="dt-vs">vs</i>')}</div>
          <h3 class="dt-h">${slots(x.head)}</h3>
          <p class="dt-p">${esc(x.proof)}</p>
        </div>
        <div class="dt-dk"><span class="dt-l">The posts behind it · ${x.posts.length}</span>${deck(x.posts, 'r')}</div>
      </article>`).join('')}
    </div>
    <div class="dt-ft"><span>${fine ? 'Click' : 'Tap or swipe'} for the next</span><span class="dt-nav"><button type="button" class="dt-b" data-dtgo="-1" aria-label="Previous">${IC.l}</button><button type="button" class="dt-b" data-dtgo="1" aria-label="Next">${IC.r}</button></span></div>
  </section>`;
}
function rulesDeckHTML() {
  const Mo = M();
  return `<section class="rl" data-rl aria-label="My rules">
    <div class="rl-key"><span>What missing it costs</span><span>Tap a rule to open it</span></div>
    ${Mo.rules.map((r) => plateHTML(r)).join('')}${Mo.retired.map((r) => plateHTML(r, true)).join('')}${openHTML()}
  </section>`;
}
function boardHTML() {
  const Mo = M(), rd = Mo.rd; if (!rd || !Mo.rules.length) return '';
  const n = Mo.runs.length, sub = String(rd.whoSub || '').replace(/\s*[^.!?]*funniest when[^.!?]*[.!?]\s*$/i, '');
  return `<section class="rules bd" id="rd-rules" aria-labelledby="rd-ru-who">
    <div class="bd-top">
      <span class="tj-k"><i></i>The read on @${esc(Mo.f.handle)} · ${n} ${n === 1 ? 'run' : 'runs'} · ${Mo.n} posts</span>
      <h2 class="lk-who" id="rd-ru-who">${esc(rd.who)}</h2>
      ${sub ? `<p class="lk-p">${esc(sub)}</p>` : ''}
    </div>
    <div class="bdh"><span class="tj-k"><i></i>The rules · ${Mo.rules.length} live${Mo.retired.length ? `, ${Mo.retired.length} dropped` : ''}</span>${rd.formula ? `<h3>${esc(rd.formula)}</h3>` : ''}</div>
    ${formulaHTML()}
    ${rulesDeckHTML()}
    ${insightsHTML()}
  </section>`;
}

/* ---------- a run, or an idea he keeps making: one pop-up frame ----------
   Its posts rise off the floor one after another, in posting order, each to the height it landed in his memory:
   the skyline of covers is the trajectory. Then what it taught me. */
const SKY = new Map();
function skyHTML(items, o = {}) {
  const id = 'sk' + (++FSEQ), n = items.length, best = byRank(items)[0], worst = byRank(items)[n - 1];
  SKY.set(id, { items, typ: o.typ, prev: o.prev });
  return `<div class="sky" data-sky="${id}" role="group" aria-label="${esc(o.label || '')} Post by post. Typical ${rkv(o.typ)}${o.prev != null ? `; ${o.prevLabel} was ${rkv(o.prev)}` : ''}.">
    <div class="sky-hd"><span class="k">${o.kick || 'Post by post'}</span><span class="sky-key"><i class="ty"></i>Typical${o.prev != null ? `<i class="g"></i>${esc(o.prevLabel)}` : ''}<i class="h"></i>Half</span></div>
    <div class="sky-st">
      <span class="sky-ax t">Top</span><span class="sky-ax b">Bottom</span>
      <i class="sky-ln h"><span>Half</span></i>
      ${o.prev != null ? `<i class="sky-ln g"></i>` : ''}<i class="sky-ln ty"></i>
      <span class="dk sky-dk" style="--n:${n}">${items.map((p, k) => `<button type="button" class="dk-c b${p.band}${p === best ? ' best' : ''}${p === worst && n > 2 ? ' worst' : ''}" data-post="${p.id}" style="--i:${k};z-index:${k + 1}" aria-label="${k + 1}: ${esc(p.label)}, ${rkw(p)}">${cover(p)}<span class="dk-r">${rkw(p)}</span><span class="sky-n">${k + 1}</span></button>`).join('')}</span>
    </div>
    <div class="sky-ft"><span>${o.first || 'First post'}</span><span>${fine ? 'Hover' : 'Slide across'} for each</span><span>${o.last || 'Latest'}</span></div>
  </div>`;
}
function laySky(el) {
  const st = el.querySelector('.sky-st'), W = st.clientWidth, D = SKY.get(el.dataset.sky); if (!W || !D) return;
  const ph = W < 600, n = D.items.length, cw = ph ? clampN(Math.round(W / (n * 0.7 + 0.3)), 48, 70) : clampN(Math.round(W / 11), 88, 128), ch = Math.round(cw * 1.25), span = ph ? 236 : clampN(Math.round(W * 0.2), 200, 260);
  const stride = n > 1 ? Math.min(cw + (ph ? 10 : 22), (W - cw) / (n - 1)) : 0, x0 = (W - (stride * (n - 1) + cw)) / 2;
  const yOf = (v) => (1 - tAt(v)) * span;
  st.style.height = span + ch + 8 + 'px';
  qa('.sky-dk .dk-c', st).forEach((c, k) => { Object.assign(c.style, { left: (x0 + k * stride).toFixed(1) + 'px', bottom: yOf(D.items[k].pct).toFixed(1) + 'px', width: cw + 'px', height: ch + 'px' }); });
  const put = (cls, v) => { const l = st.querySelector('.sky-ln.' + cls); if (l && v != null) { l.style.bottom = yOf(v).toFixed(1) + 'px'; l.dataset.y = yOf(v); } };
  put('h', 50); put('ty', D.typ); put('g', D.prev);
  el.classList.toggle('tight', stride < cw - 4);
}
// a rule's memory: every post it has named, run by run, as its cover; solid ring = it did what the rule said,
// dashed and faded = it broke it; this run's are the big red ones
function memRow(r, R, x) {
  const Mo = M(), half = Mo.n / 2, named = (p) => r.L.includes(p.id) || r.Sk.includes(p.id), ok = (p) => (r.L.includes(p.id) ? p.rank <= half : p.rank > half);
  const groups = Mo.runs.slice(0, R.r).map((Rk) => {
    const g = Rk.items.filter(named), now = Rk.r === R.r;
    return `<span class="lg-g${now ? ' now' : ''}"><span class="lg-cs">${g.length ? g.map((p) => `<button type="button" class="lc ${ok(p) ? 'ok' : 'no'}" data-post="${p.id}" aria-label="${esc(p.label)}, ${rkw(p)}, ${ok(p) ? 'held' : 'broke it'}">${cover(p)}</button>`).join('') : '<i class="none"></i>'}</span><em>Run ${Rk.r}</em></span>`;
  }).join('');
  return `<li class="mu"><span class="mu-m ${moveC(x.m)}">${esc(moveW(x.m))}</span><div class="mu-b"><b>${esc(r.law)}</b><span class="lg">${groups}</span><p>${esc(x.line)}</p></div></li>`;
}
function runView(R) {
  const Mo = M(), rr = R.rr, P = R.prev, latest = R.r === Mo.runs.length && Mo.runs.length > 1;
  const row = (o, k, cls) => { const p = o && Mo.byId[o.id]; return p ? `<button type="button" class="pr ${cls}" data-post="${p.id}"><span class="pr-i">${cover(p)}</span><span class="pr-t"><span class="pr-k"><b>${rkw(p)}</b><em>${k} · post ${R.items.indexOf(p) + 1}</em></span><strong>${esc(o.line)}</strong></span>${IC.r}</button>` : ''; };
  const rows = rr ? (rr.changed || []).map((x) => { const r = Mo.ruleById[x.r]; return r ? memRow(r, R, x) : ''; }).join('') : '';
  const st = rr && rr.settled;
  return `<div class="pv lkp lks run mur">
    <div class="lks-hero rv">
      <div class="lks-hd">${ktop(`Run ${R.r} · ${span(R.items[0].date, R.items[R.items.length - 1].date)}`, latest ? 'Latest' : `${R.items.length} posts`, 'r')}<h2>${esc(R.d.head)}</h2></div>
      ${bmx([[rkv(R.tp), 'typical post', R.tp <= 25 ? 'r' : ''], [`${R.top}<small>/${R.items.length}</small>`, 'in his top quarter', R.top >= 3 ? 'r' : ''], P ? [rkv(P.tp), `run ${P.r}’s typical`] : ['—', 'first run']])}
    </div>
    <section class="sec rv lks-sky">${skyHTML(R.items, { typ: R.tp, prev: P ? P.tp : null, prevLabel: P ? `Run ${P.r}` : '', label: `Run ${R.r}.` })}</section>
    ${rr ? `<section class="sec rv lks-a"><span class="k">Why it went this way</span><p class="lk-p">${esc(rr.why)}</p><div class="prs">${row(rr.bit, 'Bit hardest', 'up')}${row(rr.left, 'Left', 'dn')}</div></section>
    ${rows ? `<section class="sec rv lks-b"><span class="k">What run ${R.r} did to my rules</span><span class="mu-key"><i class="ok"></i>Did what the rule said<i class="no"></i>Broke it</span><ol class="mu-l">${rows}</ol></section>` : ''}
    ${st ? `<section class="sec rv call lks-c ${st.verdict}"><span class="k">After run ${st.run} I said</span><p class="lk-said">“${esc(st.call)}”</p><p class="call-v"><b class="stamp">${VERDICT[st.verdict] || ''}</b>${esc(st.line)}</p></section>` : ''}
    ${rr.watch ? `<section class="sec rv lks-d"><span class="k">Watching next</span><p class="lk-said">${esc(rr.watch)}</p></section>` : ''}` : `<section class="sec rv lks-a"><p class="lk-p">${esc(R.d.body)}</p></section>`}
  </div>`;
}
function ideaStats(c) {
  const Mo = M(), n = c.items.length, ranked = byRank(c.items);
  const mp = med(c.items.map((p) => p.pct)), hard = c.items.filter((p) => p.band >= 2).length;
  let trend = null;
  if (n >= 4) {
    const h = Math.floor(n / 2), a = Math.round(med(c.items.slice(0, h).map((p) => p.rank))), b = Math.round(med(c.items.slice(n - h).map((p) => p.rank))), d = a - b, flat = Math.abs(d) < Mo.n * 0.12;
    trend = { a, b, word: flat ? 'Holding steady' : d > 0 ? 'Getting better' : 'Wearing out', cls: flat ? '' : d > 0 ? 'r' : 'w' };
  }
  const rs = Mo.rules.map((r) => ({ r, has: c.items.filter((p) => r.L.includes(p.id)).length, lacks: c.items.filter((p) => r.Sk.includes(p.id)).length }));
  return { n, best: ranked[0], worst: ranked[n - 1], mp, hard, trend, carries: rs.filter((x) => x.has >= 2).sort((a, b) => b.has - a.has).slice(0, 2), misses: rs.filter((x) => x.lacks >= 2).sort((a, b) => b.lacks - a.lacks).slice(0, 1) };
}
function ideaDivHTML(c) {
  const I = ideaStats(c), paid = c.items.filter((p) => p.paid).length, law = (r) => esc(String(r.law).replace(/\.$/, ''));
  const go = (p, k, cls) => `<button type="button" class="cd-e ${cls}" data-post="${p.id}"><span class="cd-ei">${cover(p)}</span><span class="cd-et"><em>${k}</em><b>${rkw(p)}</b><span>${esc(p.label)}</span></span></button>`;
  return `<div class="cdiv" data-cdiv="${c.id}">
    <div class="cd-l">
      <div class="cd-hd"><b class="cd-x">×${I.n}</b><span class="cd-t"><span class="sub">An idea ${who() === 'his' ? 'he keeps' : 'they keep'} making · ${span(c.first, c.last)}${paid ? ` · ${paid} paid` : ''}</span><b>${esc(c.name)}</b></span></div>
      <p class="cd-r">${esc(c.read)}</p>
      ${I.carries.length || I.misses.length ? `<div class="cd-ru">${I.carries.map((x) => `<span class="up"><b>${x.has}/${I.n}</b>have “${law(x.r)}”</span>`).join('')}${I.misses.map((x) => `<span class="dn"><b>${x.lacks}/${I.n}</b>miss “${law(x.r)}”</span>`).join('')}</div>` : ''}
    </div>
    <div class="cd-rt">
      <div class="cd-mx">
        <span><b class="${I.mp <= 25 ? 'r' : ''}">${rkv(I.mp)}</b><em>typical go</em></span>
        <span><b class="${I.hard * 2 >= I.n ? 'r' : ''}">${I.hard}<small>/${I.n}</small></b><em>in his top quarter</em></span>
        ${I.trend ? `<span><b class="${I.trend.cls}">${I.trend.word}</b><em>first goes ${ord(I.trend.a)} → latest ${ord(I.trend.b)}</em></span>` : ''}
      </div>
      <div class="cd-ex">${go(I.best, 'Best go', 'up')}${I.n > 1 ? go(I.worst, 'Worst go', 'dn') : ''}</div>
    </div>
  </div>`;
}
// from a post's pop-up: the wall turns to Repeats and stops at that idea
function goIdea(id) {
  const was = ovOpen; if (was) closeOv();
  setTimeout(() => {
    const turn = S.sort !== 'concept'; if (turn) setSort('concept', -1);
    setTimeout(() => { const d = document.querySelector(`#rd-feed > .cdiv[data-cdiv="${id}"]`); if (d) quietTo(scrollY + d.getBoundingClientRect().top - (phone() ? hbc() : hbc() + $('rd-lens').offsetHeight + 8)); }, turn && !RM ? 760 : 0);
  }, was && !RM ? 340 : 0);
}

/* ---------- living it: each piece plays once, the first time it is in view ---------- */
function mountBoard(root = document) {
  qa('.sky', root).forEach(laySky);
  if (!RM && 'IntersectionObserver' in window) {
    if (!boardIO) boardIO = new IntersectionObserver((es) => es.forEach((e) => { if (e.isIntersecting) { boardIO.unobserve(e.target); play(e.target); } }), { threshold: 0.35 });
    qa('.dp:not([data-seen]), .fm:not([data-seen]), .sky:not([data-seen]), .mu-l:not([data-seen]), .dt:not([data-seen])', root).forEach((el) => { el.dataset.seen = '1'; el.classList.add('pre'); boardIO.observe(el); });
  }
  qa('.dt', root).forEach(mountDt);
  if (root === document || root.id === 'rd-main') sizeRows();
}
function boardOff() { if (boardIO) { boardIO.disconnect(); boardIO = null; } PDATA.clear(); hidePeek(true); }
function play(el) {
  el.classList.remove('pre');
  if (el.classList.contains('dp')) playDrop(el);
  else if (el.classList.contains('fm')) playFormula(el);
  else if (el.classList.contains('sky')) playSky(el);
  else if (el.classList.contains('dt')) { const s0 = el.querySelector('.dt-s.on'); if (s0) { s0.classList.remove('on'); void s0.offsetWidth; s0.classList.add('on'); const dk = s0.querySelector('.dk'); if (dk) deal(dk, 300); } }
  else playMem(el);
}
const roll = (b, to, dur, delay) => { const t0 = performance.now() + delay; b.textContent = '0'; const step = (t) => { const k = clamp01((t - t0) / dur), e = bez(k, [0.65, 0, 0.35, 1]); b.textContent = String(Math.round(to * e)); if (k < 1) requestAnimationFrame(step); }; requestAnimationFrame(step); };
const deal = (dk, d0) => qa('.dk-c', dk).forEach((c, k) => anim(c, [{ opacity: 0, transform: `translateX(${-k * 100}%)` }, { opacity: c.classList.contains('off') ? 0.4 : 1, offset: 0.3 }, { opacity: c.classList.contains('off') ? 0.4 : 1, transform: 'none' }], { dur: 640, delay: d0 + k * 32, ease: 'cubic-bezier(.2,.8,.2,1)' }));
/* the drop: the decks are dealt, then the ledge without the ingredient falls to where those posts actually land; the
   arrow falls with it and the count runs to what missing the rule costs */
function playDrop(el) {
  const D = PDATA.get(el.dataset.dp); if (!D) return;
  qa('.dk', el).forEach((dk, j) => deal(dk, 80 + j * 140));
  const has = el.querySelector('.dp-l.has'), lacks = el.querySelector('.dp-l.lacks'), w = el.querySelector('.dp-w');
  const T = 640, DROP = 1050, E = 'cubic-bezier(.7,0,.25,1)';
  qa('.dp-lab', el).forEach((l) => anim(l, [{ opacity: 0 }, { opacity: 1 }], { dur: 420, delay: T + DROP - 200 }));
  if (has && lacks && D.w != null) {
    // on a phone the ledges stack; the fall is the part that is the rule's weight
    const dy = phone() ? (+el.style.getPropertyValue('--wf') || 0) * 150 : lacks.offsetTop - has.offsetTop;
    anim(lacks, [{ transform: `translateY(${-dy}px)` }, { transform: 'none' }], { dur: DROP, delay: T, ease: E });
    anim(w.querySelector('.dp-drop'), [{ transform: 'scaleY(0)' }, { transform: 'scaleY(1)' }], { dur: DROP, delay: T, ease: E });
    anim(w.querySelector('.dp-v'), [{ opacity: 0, transform: 'translateY(-8px)' }, { opacity: 1, transform: 'none' }], { dur: 500, delay: T + 120 });
    anim(w.querySelector('em'), [{ opacity: 0 }, { opacity: 1 }], { dur: 420, delay: T + DROP - 100 });
    roll(w.querySelector('.dp-num'), D.w, DROP, T);
  } else if (w) anim(w, [{ opacity: 0 }, { opacity: 1 }], { dur: 420, delay: T });
}
function playFormula(el) {
  qa('.fm-row', el).forEach((row, j) => {
    const d0 = j * 260;
    rise(row.querySelector('p'), { y: 8, dur: 560, delay: d0 + 80 });
    deal(row.querySelector('.dk'), d0 + 200);
    const b = row.querySelector('.fm-big b'); roll(b, +b.dataset.to, 900, d0 + 200);
  });
}
/* post by post: the covers rise off the floor one after another in posting order, each to the height it landed,
   so the run plays out as a skyline; then the typical line moves from where the last run's sat to this one's */
function playSky(el) {
  const cs = qa('.sky-dk .dk-c', el);
  cs.forEach((c, k) => { const y = parseFloat(c.style.bottom) || 0; anim(c, [{ opacity: 0, transform: `translateY(${y + 24}px)` }, { opacity: 1, offset: 0.3 }, { opacity: 1, transform: 'none' }], { dur: 760, delay: 120 + k * 110, ease: 'cubic-bezier(.2,.8,.2,1)' }); });
  const end = 120 + cs.length * 110 + 500, g = el.querySelector('.sky-ln.g'), t = el.querySelector('.sky-ln.ty');
  anim(el.querySelector('.sky-ln.h'), [{ opacity: 0 }, { opacity: 1 }], { dur: 400, delay: 80 });
  if (g) anim(g, [{ opacity: 0 }, { opacity: 1 }], { dur: 360, delay: end });
  if (t) { const dy = g ? +t.dataset.y - +g.dataset.y : 0; anim(t, [{ opacity: 0, transform: `translateY(${dy}px)` }, { opacity: 1, transform: `translateY(${dy}px)`, offset: 0.2 }, { opacity: 1, transform: 'none' }], { dur: 1250, delay: end + 160, ease: 'cubic-bezier(.65,0,.2,1)' }); }
}
/* my memory updating: each rule's evidence from this run drops in, then the move stamps; my old call is checked */
function playMem(el) {
  qa('.mu', el).forEach((row, j) => {
    const d0 = 80 + j * 240;
    rise(row, { y: 10, dur: 520, delay: d0 });
    const cs = qa('.lg-g.now .lc', row);
    cs.forEach((c, k) => anim(c, [{ opacity: 0, transform: 'translateY(-14px) scale(.8)' }, { opacity: 1, transform: 'none' }], { dur: 480, delay: d0 + 260 + k * 90, ease: 'cubic-bezier(.2,.8,.2,1)' }));
    anim(row.querySelector('.mu-m'), [{ opacity: 0, transform: 'scale(1.3)' }, { opacity: 1, transform: 'none' }], { dur: 380, delay: d0 + 320 + cs.length * 90, ease: 'cubic-bezier(.2,.8,.2,1)' });
  });
  const s = el.closest('.mur') && el.closest('.mur').querySelector('.stamp');
  if (s) anim(s, [{ opacity: 0, transform: 'rotate(-8deg) scale(1.35)' }, { opacity: 1, transform: 'rotate(-3deg) scale(1)' }], { dur: 420, delay: 80 + qa('.mu', el).length * 240 + 700, ease: 'cubic-bezier(.2,.8,.2,1)' });
}

/* ---------- the details: the track scrolls natively (a swipe, a wheel, a key), a tap anywhere off the posts moves
   to the next, and the panel that lands plays its own entrance (numbers rise, words rise, the deck deals) ---------- */
function mountDt(sec) {
  if (sec._dt) return; sec._dt = { k: 0 };
  const tr = sec.querySelector('.dt-track');
  let t = 0;
  listen(tr, 'scroll', () => { if (t) return; t = requestAnimationFrame(() => { t = 0; dtSync(sec); }); }, { passive: true });
  listen(tr, 'keydown', (e) => { if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') { e.preventDefault(); dtGo(sec, e.key === 'ArrowRight' ? 1 : -1); } });
}
function dtSync(sec) {
  const tr = sec.querySelector('.dt-track'), n = qa('.dt-s', tr).length, k = clampN(Math.round(tr.scrollLeft / Math.max(1, tr.clientWidth)), 0, n - 1);
  if (k === sec._dt.k) return;
  sec._dt.k = k; buzz();
  qa('.dt-s', tr).forEach((s0, j) => s0.classList.toggle('on', j === k));
  qa('.dt-bar i', sec).forEach((b, j) => { b.classList.toggle('on', j === k); b.classList.toggle('done', j < k); });
  const ct = sec.querySelector('.dt-ct b'); if (ct) ct.textContent = pad2(k + 1);
  const dk = qa('.dt-s', tr)[k].querySelector('.dk'); if (dk && !RM) deal(dk, 260);
}
function dtGo(sec, d) {
  const tr = sec.querySelector('.dt-track'), n = qa('.dt-s', tr).length, k = (sec._dt.k + d + n) % n;
  tr.scrollTo({ left: k * tr.clientWidth, behavior: RM ? 'auto' : 'smooth' });
}
const clampN = (v, a, b) => Math.min(b, Math.max(a, v));
let rzT = 0;
listen(window, 'resize', () => { clearTimeout(rzT); rzT = setTimeout(() => { qa('.sky').forEach(laySky); }, 120); });
listen(window, 'scroll', () => { if (PK && PK.classList.contains('on') && !RF) { hidePeek(true); qa('.dk').forEach(unriffle); } }, { passive: true });

/* ---------- a deck under a finger: the card under it comes forward, and on a phone a big peek of it rises above;
   let go and the peek stays a moment, a tap on it opens the post ---------- */
let RF = null, PK = null, pkHide = 0;
function peek() {
  if (PK && PK.isConnected) return PK;
  PK = document.createElement('button'); PK.type = 'button'; PK.className = 'pk-v'; PK.setAttribute('aria-hidden', 'true'); PK.tabIndex = -1;
  PK.innerHTML = '<span class="pk-i"></span><span class="pk-t"><b></b><span></span><em>Open</em></span>';
  layer.appendChild(PK); return PK;
}
function showPeek(card) {
  const p = M().byId[card.dataset.post]; if (!p) return;
  const v = peek(); clearTimeout(pkHide);
  v._card = card;
  if (v._id !== p.id) { v._id = p.id; v.querySelector('.pk-i').innerHTML = cover(p); const b = v.querySelector('b'); b.textContent = rkw(p); b.className = p.band >= 2 ? 'r' : ''; v.querySelector('.pk-t > span').textContent = p.label; }
  const r = card.getBoundingClientRect(), w = v.offsetWidth || 300, h = v.offsetHeight || 150;
  const x = clampN(r.left + r.width / 2 - w / 2, 10, innerWidth - w - 10), y = r.top - h - 18 > hbc() + 6 ? r.top - h - 18 : r.bottom + 14;
  const to = `translate(${x.toFixed(0)}px, ${y.toFixed(0)}px)`;
  if (!v.classList.contains('on')) { v.style.transition = 'none'; v.style.transform = to; void v.offsetWidth; v.style.transition = ''; v.classList.add('on'); } else v.style.transform = to;
}
function hidePeek(now) { clearTimeout(pkHide); const go = () => { if (PK) { PK.classList.remove('on'); PK._id = null; } }; if (now) go(); else pkHide = setTimeout(go, 2600); }
function riffle(dk, x) {
  const cs = qa('.dk-c', dk); if (!cs.length) return null;
  const r = dk.getBoundingClientRect(); let best = cs[0], bd = 1e9;
  cs.forEach((c) => { const b = c.getBoundingClientRect(), d = Math.abs(x - (b.left + Math.min(b.width, 22) / 2)); if (x >= b.left - 4 && d < bd) { bd = d; best = c; } });
  if (x < r.left) best = cs[0];
  cs.forEach((c) => c.classList.toggle('up', c === best)); dk.classList.add('riff');
  const cap = dk.parentNode.querySelector('.dp-cap'), p = M().byId[best.dataset.post];
  if (cap && p) { cap.textContent = `${rkw(p)} · ${p.label}`; dk.parentNode.classList.add('capped'); }
  return best;
}
function unriffle(dk) { dk.classList.remove('riff'); qa('.dk-c.up', dk).forEach((c) => c.classList.remove('up')); if (dk.parentNode) dk.parentNode.classList.remove('capped'); }
listen(document, 'pointerdown', (e) => {
  const dk = e.target.closest && e.target.closest('.dk');
  if (!(e.target.closest && e.target.closest('.pk-v'))) hidePeek(true);
  RF = dk && e.pointerType !== 'mouse' ? { dk, x: e.clientX, y: e.clientY, id: e.pointerId, on: false } : null;
}, { passive: true });
listen(document, 'pointermove', (e) => {
  if (RF && e.pointerId === RF.id) {
    const dx = e.clientX - RF.x, dy = e.clientY - RF.y;
    if (!RF.on && Math.abs(dy) > 12 && Math.abs(dy) > Math.abs(dx)) { RF = null; return; }
    if (!RF.on && Math.abs(dx) > 6) RF.on = true;
    if (RF.on) { const c = riffle(RF.dk, e.clientX); if (c && c !== RF.c) { RF.c = c; showPeek(c); buzz(); } }
    return;
  }
  if (e.pointerType !== 'mouse' || !fine) return;
  const dk = e.target.closest && e.target.closest('.dk');
  qa('.dk').forEach((d) => { if (d !== dk && d.querySelector('.dk-c.up')) unriffle(d); });
  if (dk) riffle(dk, e.clientX);
}, { passive: true });
listen(document, 'pointerup', () => { if (RF && RF.on) { RF.dk._riffled = performance.now(); const dk = RF.dk; setTimeout(() => unriffle(dk), 2600); hidePeek(); } RF = null; }, { passive: true });
listen(document, 'pointercancel', () => { if (RF) { unriffle(RF.dk); hidePeek(true); } RF = null; }, { passive: true });
// a tap right after a riffle is the riffle ending; a tap on the peek opens what it shows; a post tapped anywhere in
// the read or a run's read opens itself (an open run's box carries data-open, which the router would otherwise
// take for a card's Open button)
function boardClick(e) {
  const lt = e.target.closest('[data-drop="lens"]');
  if (lt) { lnmOpen(lt, e.detail === 0); return true; }
  const rh = e.target.closest('.rl-hd');
  if (rh) { rlToggle(rh); return true; }
  const fb = e.target.closest('[data-fold]');
  if (fb) { toggleFold(fb); return true; }
  const nb = e.target.closest('[data-dtgo]');
  if (nb) { dtGo(nb.closest('.dt'), +nb.dataset.dtgo); return true; }
  const sl = e.target.closest('.dt-s');
  if (sl && !e.target.closest('.dk')) { dtGo(sl.closest('.dt'), 1); return true; }
  const pv = e.target.closest('.pk-v');
  if (pv) { const id = pv._id; if (id) openEntry({ type: 'post', id }, pv.querySelector('.pk-i'), pv._card); hidePeek(true); return true; }
  const dk = e.target.closest('.dk');
  if (dk && performance.now() - (dk._riffled || 0) < 400) return true;
  const b = e.target.closest('#rd-rules [data-post], #rd-obody .lks [data-post]');
  if (b && M().byId[b.dataset.post]) { hideTip(); hidePeek(true); openEntry({ type: 'post', id: b.dataset.post }, b); return true; }
  return false;
}

/* ---------- the wall changes order without letting go of a single cover ----------
   v15: slower and settled. Every cover on screen before or after lifts off its slot (a touch smaller while it
   travels, so neighbours never pile up), glides on one long ease-out curve and lands. The wave runs from the
   top of the screen down, so the eye follows it; covers arriving from off screen rise in behind the wave.
   Dividers that are gone fade where they were; new ones come up into their place once the covers pass. */
function flipSort(change) {
  const vh = innerHeight, vis = (r) => r && r.height > 0 && r.bottom > -20 && r.top < vh + 20;
  qa('.cfly').forEach((g) => g.getAnimations().forEach((a) => a.finish()));
  const els = qa('#rd-feed > .tile, #rd-feed > .rbk, #rd-feed > .cdiv, #rd-feed > .bdiv');
  const before = new Map(els.map((el) => [el, el.getBoundingClientRect()]));
  els.forEach((el) => el.getAnimations().forEach((a) => { if (a.id === 'fs') a.cancel(); }));
  if (wallIO) { wallIO.disconnect(); wallIO = null; }
  const wall0 = $('rd-wall').getBoundingClientRect().top;
  holdAnchor(1800);
  change();
  const lensH = phone() ? 0 : ($('rd-lens') ? $('rd-lens').offsetHeight + 8 : 0), line = hbc() + lensH + 8;
  if (wall0 < line - 40) { const w = $('rd-wall').getBoundingClientRect(); scrollTo({ top: scrollY + w.top - line, behavior: 'auto' }); hold(); }
  const after = new Map(els.map((el) => [el, el.getBoundingClientRect()]));
  els.forEach((el) => {
    if (el.classList.contains('tile')) return;
    const a = before.get(el), b = after.get(el);
    if (!vis(a) || b.height) return;
    const g = el.cloneNode(true); g.classList.add('fs-g'); g.inert = true; g.setAttribute('aria-hidden', 'true');
    Object.assign(g.style, { position: 'fixed', left: a.left + 'px', top: a.top + 'px', width: a.width + 'px', height: a.height + 'px', margin: '0', zIndex: 4, pointerEvents: 'none', display: 'block' });
    layer.appendChild(g);
    const an = g.animate([{ opacity: 1, transform: 'none' }, { opacity: 0, transform: 'translateY(-10px)' }], { duration: 260, easing: 'cubic-bezier(.4,0,1,1)', fill: 'forwards' });
    an.onfinish = an.oncancel = () => g.remove();
  });
  const top0 = Math.max(0, line - 20), DUR = phone() ? 900 : 960, WAVE = phone() ? 200 : 240;
  const waveAt = (r) => Math.round(clamp01((r.top - top0) / Math.max(1, vh - top0)) * WAVE + clamp01(r.left / innerWidth) * 36);
  let T = 0;
  els.forEach((el) => {
    const a = before.get(el), b = after.get(el);
    if (!b.height || (!vis(a) && !vis(b))) return;
    const was = vis(a) && a.height;
    const delay = waveAt(was ? (a.top < b.top ? a : b) : b);
    let end = 0;
    if (el.classList.contains('tile')) {
      if (was) {
        const dx = a.left - b.left, dy = a.top - b.top, d = Math.hypot(dx, dy);
        if (d < 1) return;
        el.animate([{ transform: `translate3d(${dx}px, ${dy}px, 0)` }, { transform: 'translate3d(0,0,0)' }], { duration: DUR, delay, easing: EZ.wall, fill: 'backwards', id: 'fs' });
        if (d > 40) {
          const s = (1 - Math.min(0.08, d / 4000)).toFixed(3);
          el.animate([{ scale: '1', easing: 'cubic-bezier(.45,0,.55,1)' }, { scale: s, offset: 0.36, easing: 'cubic-bezier(.3,0,.15,1)' }, { scale: '1' }], { duration: DUR, delay, easing: 'linear', fill: 'backwards', id: 'fs' });
        }
        end = delay + DUR;
      } else {
        const d2 = delay + 160;
        el.animate([{ opacity: 0, transform: 'translate3d(0, 30px, 0) scale(.94)' }, { opacity: 1, transform: 'translate3d(0,0,0) scale(1)' }], { duration: DUR - 80, delay: d2, easing: EZ.wall, fill: 'backwards', id: 'fs' });
        end = d2 + DUR - 80;
      }
    } else if (!was) {
      // a new divider comes up as the old ones go, so the covers never travel through an empty wall
      const d2 = Math.round(delay * 0.5) + 90;
      el.animate([{ opacity: 0, transform: 'translate3d(0, 14px, 0)' }, { opacity: 1, transform: 'translate3d(0,0,0)' }], { duration: 620, delay: d2, easing: 'cubic-bezier(.2,.7,.2,1)', fill: 'backwards', id: 'fs' });
      end = d2 + 620;
    } else {
      const dy = a.top - b.top;
      if (Math.abs(dy) >= 1) { el.animate([{ transform: `translate3d(0, ${dy}px, 0)` }, { transform: 'translate3d(0,0,0)' }], { duration: DUR, delay, easing: EZ.wall, fill: 'backwards', id: 'fs' }); end = delay + DUR; }
    }
    T = Math.max(T, end);
  });
  MORPH.last = null; MORPH.until = performance.now() + T;
  return { t: T };
}

/* ---------- v15: motion curves ----------
   A damped spring, written out as a linear() curve so the compositor runs it: z is the damping (1 = no bounce),
   k how far it has settled by the end. Browsers without linear() get a close cubic-bezier. */
const LIN = !!(window.CSS && CSS.supports && CSS.supports('animation-timing-function', 'linear(0, 1)'));
function spring(z, k = 6.5, n = 60) {
  const wd = Math.sqrt(Math.max(0, 1 - z * z)), pts = [];
  for (let i = 0; i <= n; i++) {
    const a = (k * i) / n, w = a / z;
    const x = z < 1 ? 1 - Math.exp(-a) * (Math.cos(w * wd) + (z / wd) * Math.sin(w * wd)) : 1 - (1 + a) * Math.exp(-a);
    pts.push(i === n ? 1 : +x.toFixed(4));
  }
  return `linear(${pts.join(', ')})`;
}
const EZ = {
  fly: LIN ? spring(0.82) : 'cubic-bezier(.2,.9,.1,1)',      // a cover leaving its slot for the sheet
  home: LIN ? spring(0.9, 7) : 'cubic-bezier(.22,.88,.14,1)', // the cover going back into its slot
  sheet: LIN ? spring(1, 7.5) : 'cubic-bezier(.16,.9,.2,1)',
  clip: 'cubic-bezier(.3,.75,.2,1)',
  wall: 'cubic-bezier(.42,0,.12,1)',                           // the wall: eases off its slot, lands soft
  away: 'cubic-bezier(.4,0,.6,1)',
};

/* ---------- v15: a post opens out of the cover you tapped, and closes back into it ----------
   The cover lifts off its slot and grows into the sheet's hero while the sheet comes up behind it; the slot
   stays empty while the post is open. Closing sends the cover back into that same slot (the page first brings
   the slot on screen if it slid under the header), then the slot takes it back. Everything is measured at the
   sheet's resting layout, so the cover never lands out of frame. A post opened inside the sheet (a run's covers,
   its rows) flies the same way from where it was tapped. */
const FLY = { el: null, id: null };
const pxR = (el, r) => { const v = getComputedStyle(el).borderTopLeftRadius || '0'; return v.endsWith('%') ? (parseFloat(v) / 100) * Math.min(r.width, r.height) : parseFloat(v) || 0; };
function coverEl(el) {
  if (!el || !el.isConnected || !el.matches) return null;
  if (el.matches('.tile, .dk-c, .lc, .pk-i, .pr-i, .evr-i')) return el;
  return el.querySelector('.pr-i, .evr-i, .pk-i, .face, img, .cv-f');
}
function safeBand() {
  const h = document.getElementById('rd-hdr'), hb = h ? h.getBoundingClientRect().bottom + 10 : 0;
  let t = Math.max(hbc(), hb) + 6;
  // on a laptop the lens bar sticks under the header once the wall is up
  const L = $('rd-lens'), lr = L && L.getBoundingClientRect();
  if (lr && lr.height && lr.top < t + 24 && lr.bottom > 0) t = Math.max(t, lr.bottom + 8);
  return [t, navTop() - 8];
}
function onScreen(el) {
  if (!el || !el.isConnected) return null;
  const r = el.getBoundingClientRect(); if (!r.width || !r.height) return null;
  if (el.closest('#rd-hread, #rd-obody, .pk-v')) return r.bottom > 0 && r.top < innerHeight ? r : null;
  const [t, b] = safeBand();
  return r.top >= t - r.height * 0.2 && r.bottom <= b + r.height * 0.2 && r.right > 0 && r.left < innerWidth ? r : null;
}
function flyFrom(id, src) {
  let el = coverEl(src), r = onScreen(el);
  if (!r) { el = document.querySelector(`#rd-hread .tile[data-pid="${id}"]`); r = onScreen(el); }
  return r ? { el, r, rad: pxR(el, r) } : null;
}
// the post's cover as a loose card, laid over the page at rect a
function cardAt(p, a, rad) {
  const g = document.createElement('div'); g.className = 'cfly'; g.setAttribute('aria-hidden', 'true');
  g.innerHTML = cover(p);
  const im = g.querySelector('img'); if (im) { im.decoding = 'sync'; im.loading = 'eager'; }
  Object.assign(g.style, { left: a.left + 'px', top: a.top + 'px', width: a.width + 'px', height: a.height + 'px', borderRadius: rad + 'px' });
  layer.appendChild(g);
  return g;
}
// move a loose card from rect a (radius ra) to rect b (radius rb): one uniform scale, clipped to each end's shape
function flyCard(g, a, ra, b, rb, o = {}) {
  Object.assign(g.style, { left: b.left + 'px', top: b.top + 'px', width: b.width + 'px', height: b.height + 'px', borderRadius: '0' });
  const s = Math.max(a.width / b.width, a.height / b.height);
  const dx = a.left + a.width / 2 - (b.left + b.width / 2), dy = a.top + a.height / 2 - (b.top + b.height / 2);
  const ix = Math.max(0, (b.width - a.width / s) / 2), iy = Math.max(0, (b.height - a.height / s) / 2);
  const dur = o.dur || 620;
  const mv = g.animate([{ transform: `translate(${dx}px, ${dy}px) scale(${s})` }, { transform: 'none' }], { duration: dur, easing: o.ease || EZ.fly, fill: 'both' });
  g.animate([{ clipPath: `inset(${iy}px ${ix}px round ${ra / s}px)` }, { clipPath: `inset(0px 0px round ${rb}px)` }], { duration: dur * 0.8, easing: EZ.clip, fill: 'both' });
  if (o.fadeIn) g.animate([{ opacity: 0 }, { opacity: 1 }], { duration: dur * 0.3, easing: 'ease-out', fill: 'backwards' });
  mv.onfinish = () => {
    if (o.land) o.land();
    // the card hands over to the real thing: it fades off what is now sitting exactly under it
    const f = g.animate([{ opacity: 1 }, { opacity: 0 }], { duration: 160, easing: 'ease-out', fill: 'forwards' });
    f.onfinish = f.oncancel = () => g.remove();
  };
  mv.oncancel = () => { if (o.land) o.land(); g.remove(); };
  return mv;
}
function heroRect() {
  const hero = $('rd-obody').querySelector('.tile.hero'); if (!hero) return null;
  const rv = hero.closest('.rv');
  if (rv) { rv.getAnimations().forEach((x) => x.cancel()); const bx = rv.querySelector('.bmx'); if (bx && !RM) rise(bx, { y: 14, dur: 560, delay: 300 }); }
  const r = hero.getBoundingClientRect();
  return r.width ? { hero, r, rad: pxR(hero, r) } : null;
}
function setAway(el) { qa('.v15-away').forEach((x) => { if (x !== el) x.classList.remove('v15-away'); }); if (el) el.classList.add('v15-away'); }
function landPulse(el) {
  if (!el || RM) return;
  el.animate([{ filter: 'brightness(1.35)' }, { filter: 'brightness(1)' }], { duration: 520, easing: 'ease-out' });
}
// where a closing post goes home: the slot it came out of, else its cover on the wall if that is on screen
function homeFor(id) {
  let el = FLY.id === id && FLY.el && FLY.el.isConnected && !FLY.el.closest('#rd-obody') ? FLY.el : null;
  if (el && el.closest('.pk-v')) el = null;
  if (el && !el.closest('#rd-hread')) {
    let r = el.getBoundingClientRect();
    const [t, b] = safeBand();
    if (r.width && (r.top < t || r.bottom > b)) {
      // it slid under the header (or the nav): bring it back on screen, under the scrim, before it flies home
      const want = t + Math.max(0, (b - t - r.height) / 2);
      hold(); scrollTo(0, Math.max(0, scrollY + r.top - want));
      r = el.getBoundingClientRect();
    }
    if (r.width) return { el: coverEl(el) || el, r: (coverEl(el) || el).getBoundingClientRect() };
  }
  const w = document.querySelector(`#rd-feed .tile[data-post="${id}"]`), wr = onScreen(w);
  return wr ? { el: w, r: wr } : null;
}
// rows of the read get their real size remembered once, so a jump past them lands where it means to
function sizeRows() {
  const rs = $('rd-rules'); if (!rs || rs._sized) return; rs._sized = true;
  rs.classList.add('cv-on');
  requestAnimationFrame(() => requestAnimationFrame(() => rs.classList.remove('cv-on')));
}

/* ---------- v17: the rules, one under another ----------
   Each rule is one row, like a run's breaker on the wall: what missing it costs, big, then the rule. Tap it and it
   opens right there; the one that was open closes, and the row you tapped glides up to the top while it opens, so
   you never hunt for it. What happens in its posts and how I got to it stay folded until asked. */
function rlSet(it, open) {
  const hd = it.querySelector('.rl-hd'), bd = it.querySelector('.rl-bd');
  hd.setAttribute('aria-expanded', String(open)); it.classList.toggle('on', open);
  bd.hidden = !open; bd.style.height = ''; bd.style.overflow = '';
}
function rlToggle(hd) {
  const it = hd.closest('.rl-i'), list = it.closest('.rl'), open = hd.getAttribute('aria-expanded') !== 'true';
  const bd = it.querySelector('.rl-bd'), others = qa('.rl-i.on', list).filter((o) => o !== it);
  buzz(); hold();
  if (RM) { others.forEach((o) => rlSet(o, false)); rlSet(it, open); if (open) { const r = hd.getBoundingClientRect(), line = hbc() + 8; if (r.top < line || r.top > innerHeight * 0.45) scrollTo(0, scrollY + r.top - line); } return; }
  if (it._rlA) cancelAnimationFrame(it._rlA);
  const y0 = hd.getBoundingClientRect().top;
  // the ones closing: from their height to nothing
  const shut = others.map((o) => { const b = o.querySelector('.rl-bd'); const h = b.offsetHeight; b.style.overflow = 'hidden'; o.querySelector('.rl-hd').setAttribute('aria-expanded', 'false'); o.classList.remove('on'); return { o, b, h }; });
  let h1 = 0;
  if (open) { bd.hidden = false; bd.style.height = 'auto'; h1 = bd.offsetHeight; bd.style.overflow = 'hidden'; bd.style.height = '0px'; hd.setAttribute('aria-expanded', 'true'); it.classList.add('on'); qa('.rl-in > *', bd).forEach((el, j) => rise(el, { y: 18, dur: 560, delay: 120 + j * 70 })); }
  else { h1 = bd.offsetHeight; bd.style.overflow = 'hidden'; hd.setAttribute('aria-expanded', 'false'); it.classList.remove('on'); }
  // opening: the tapped row travels to just under the header while everything resizes around it
  const line = hbc() + 8, yT = open && (y0 > innerHeight * 0.4 || shut.length) ? Math.max(line, Math.min(y0, line)) : y0;
  const dur = open ? 620 : 420, t0 = performance.now();
  const step = (t) => {
    const k = clamp01((t - t0) / dur), e = bez(k, [0.22, 0.86, 0.26, 1]);
    shut.forEach((x) => { x.b.style.height = (x.h * (1 - e)).toFixed(1) + 'px'; });
    bd.style.height = (open ? h1 * e : h1 * (1 - e)).toFixed(1) + 'px';
    if (open) { const want = y0 + (yT - y0) * e, now = hd.getBoundingClientRect().top; if (Math.abs(now - want) > 0.5) scrollTo(0, scrollY + now - want); }
    if (k < 1) { it._rlA = requestAnimationFrame(step); return; }
    it._rlA = 0;
    shut.forEach((x) => rlSet(x.o, false));
    rlSet(it, open);
  };
  it._rlA = requestAnimationFrame(step);
}
function toggleFold(btn) {
  const s0 = btn.closest('.rl-i'), body = s0 && s0.querySelector(`[data-fbody="${btn.dataset.fold}"]`); if (!body) return;
  const open = body.hidden; body.hidden = !open; btn.setAttribute('aria-expanded', String(open)); buzz();
  if (open && !RM) qa('.evr, .lf li', body).forEach((el, j) => rise(el, { y: 14, dur: 520, delay: 40 + j * 55 }));
}

/* ---------- v18: the view menu grows out of its own button ----------
   The header's view button does not drop a copy of the page's lens. The button itself opens: its face stays
   exactly where it was (the current view, the chevron turning over), and the box it sits in widens to the left
   and grows down to show the other two views, each with a line on what it does. Pick one and its name rolls
   into the face while the box closes back into the button; then the wall turns (and comes into view if it was
   off screen). A tap outside, Escape or a scroll closes it the same way. */
const LNM = { el: null, tr: null, y: 0 };
const lnmDesc = (k) => (k === 'ranks' ? 'By where it landed · best first' : k === 'repeats' ? `What ${who() === 'his' ? 'he keeps' : 'they keep'} making · most posted first` : 'Run by run · newest first');
function lnmOpen(tr, keys) {
  if (LNM.el) { lnmClose(); return; }
  const r = tr.getBoundingClientRect(); if (!r.width) return;
  closeDrop(); hideTip(); hidePeek(true); buzz();
  const cur = lensKey(), others = TABS.filter(([k]) => k !== cur);
  // as wide as the button: the other views drop straight down out of it, in the same column as its label
  const W = Math.round(r.width), H = Math.round(r.height) + 6 + others.length * 40 + 6;
  const m = document.createElement('div');
  m.className = 'lnm'; m.setAttribute('role', 'menu'); m.setAttribute('aria-label', 'Show the wall by');
  Object.assign(m.style, { top: r.top + 'px', right: (innerWidth - r.right) + 'px', width: W + 'px', height: H + 'px', fontFamily: getComputedStyle(tr).fontFamily });
  // the face sits on the right, where the button is; the list rides the box's left edge as it widens, so its words
  // are always read from their start
  m.innerHTML = `<i class="lnm-bg" aria-hidden="true"></i><i class="lnm-rule" aria-hidden="true"></i><div class="lnm-list" style="width:${W}px;top:${Math.round(r.height)}px">${others.map(([k, l]) => `<button type="button" role="menuitem" class="lnm-o" data-lnm="${k}"><span>${l}</span></button>`).join('')}</div><div class="lnm-in" style="width:${r.width}px;height:${r.height}px"></div>`;
  // the face: the button's own insides, in the same place, so nothing about it moves when it opens
  const face = tr.cloneNode(true);
  ['data-drop', 'aria-haspopup', 'aria-expanded', 'aria-label', 'style', 'tabindex'].forEach((a) => face.removeAttribute(a));
  face.classList.add('lnm-face'); face.setAttribute('data-lnm-x', ''); face.setAttribute('aria-label', 'Close'); face.tabIndex = -1;
  const cs = getComputedStyle(tr);
  Object.assign(face.style, { width: r.width + 'px', height: r.height + 'px', font: cs.font, letterSpacing: cs.letterSpacing, textTransform: cs.textTransform, color: cs.color });
  qa('.lnm-o', m).forEach((o) => Object.assign(o.style, { font: cs.font, letterSpacing: cs.letterSpacing, textTransform: cs.textTransform }));
  m.querySelector('.lnm-in').appendChild(face);
  layer.appendChild(m);
  LNM.el = m; LNM.tr = tr; LNM.y = scrollY;
  tr.style.visibility = 'hidden'; tr.setAttribute('aria-expanded', 'true');
  listen(m, 'click', (e) => {
    e.stopPropagation();
    const o = e.target.closest('[data-lnm]'); if (o) { lnmPick(o.dataset.lnm); return; }
    if (e.target.closest('[data-lnm-x]')) lnmClose();
  });
  listen(m, 'keydown', (e) => {
    const os = qa('.lnm-o', m), i = os.indexOf(document.activeElement);
    if (e.key === 'Escape') { e.preventDefault(); lnmClose(); tr.focus(); }
    else if (e.key === 'ArrowDown' || e.key === 'ArrowUp') { e.preventDefault(); os[(i + (e.key === 'ArrowDown' ? 1 : -1) + os.length) % os.length].focus(); }
  });
  const chev = face.querySelector('svg');
  if (RM) { if (chev) chev.style.transform = 'rotate(180deg)'; }
  else {
    m.animate([{ height: r.height + 'px', borderRadius: '14px', boxShadow: '0 0 0 rgba(0,0,0,0)' }, { height: H + 'px', borderRadius: '16px', boxShadow: '0 22px 44px -18px rgba(0,0,0,.95), 0 0 34px -22px rgba(255,23,79,.6)' }], { duration: 560, easing: EZ.sheet });
    m.querySelector('.lnm-bg').animate([{ opacity: 0 }, { opacity: 1 }], { duration: 220, easing: 'ease-out', fill: 'backwards' });
    qa('.lnm-o', m).forEach((o, i) => anim(o, [{ opacity: 0, transform: 'translateY(-10px)' }, { opacity: 1, transform: 'none' }], { dur: 420, delay: 90 + i * 55 }));
    if (chev) chev.animate([{ transform: 'rotate(0deg)' }, { transform: 'rotate(180deg)' }], { duration: 480, easing: EZ.sheet, fill: 'forwards' });
  }
  if (keys) requestAnimationFrame(() => { const o = m.querySelector('.lnm-o'); if (o) o.focus({ preventScroll: true }); });
}
function lnmClose(to) {
  const m = LNM.el, tr = LNM.tr; if (!m) return;
  LNM.el = null; tr.setAttribute('aria-expanded', 'false');
  const done = () => { m.remove(); tr.style.visibility = ''; };
  if (RM) { done(); return; }
  const face = m.querySelector('.lnm-face'), chev = face && face.querySelector('svg');
  if (to) {
    // the new view's name rolls into the face, the way the button's own label rolls
    const slot = face.querySelector('span > span');
    if (slot) {
      const dir = tabIx(to) >= tabIx(lensKey()) ? 1 : -1, nx = slot.cloneNode(true);
      nx.textContent = (TABS.find(([k]) => k === to) || [])[1] || ''; slot.parentNode.appendChild(nx);
      slot.animate([{ transform: 'translateY(0)', opacity: 1 }, { transform: `translateY(${-dir * 110}%)`, opacity: 0 }], { duration: 340, easing: 'cubic-bezier(.4,0,.2,1)', fill: 'forwards' });
      nx.animate([{ transform: `translateY(${dir * 110}%)`, opacity: 0 }, { transform: 'translateY(0)', opacity: 1 }], { duration: 480, easing: SOFT });
    }
  }
  qa('.lnm-o', m).forEach((o) => o.animate([{ opacity: 1 }, { opacity: 0 }], { duration: 140, easing: 'ease-in', fill: 'forwards' }));
  if (chev) chev.animate([{ transform: 'rotate(180deg)' }, { transform: 'rotate(0deg)' }], { duration: 380, easing: 'cubic-bezier(.32,.72,0,1)', fill: 'forwards' });
  const r = tr.getBoundingClientRect(), a = m.animate([{ height: m.offsetHeight + 'px', borderRadius: '16px' }, { height: r.height + 'px', borderRadius: '14px', boxShadow: '0 0 0 rgba(0,0,0,0)' }], { duration: 380, easing: 'cubic-bezier(.32,.72,0,1)', fill: 'forwards' });
  m.querySelector('.lnm-bg').animate([{ opacity: 1 }, { opacity: 1, offset: 0.6 }, { opacity: 0 }], { duration: 400, fill: 'forwards' });
  a.onfinish = () => setTimeout(done, to ? 120 : 0);
}
function lnmPick(k) {
  buzz();
  const cur = lensKey();
  lnmClose(k === cur ? null : k);
  if (k === cur) return;
  // the wall turns once the box has mostly closed, so the two never fight for the same frames
  setTimeout(() => {
    if (!alive) return;
    setLens(k);
    const lens = $('rd-lens'), w = $('rd-wall').getBoundingClientRect();
    if (w.top > innerHeight * 0.62 || w.bottom < hbc()) { const t = (lens || $('rd-wall')).getBoundingClientRect().top; quietTo(scrollY + t - hbc() - 6); }
  }, RM ? 0 : 300);
}
listen(document, 'pointerdown', (e) => { if (LNM.el && !e.target.closest('.lnm, [data-drop="lens"]')) lnmClose(); }, { capture: true, passive: true });
listen(window, 'scroll', () => { if (LNM.el && Math.abs(scrollY - LNM.y) > 8) lnmClose(); }, { passive: true });
listen(window, 'resize', () => { if (LNM.el) lnmClose(); });
listen(document, 'keydown', (e) => { if (e.key === 'Escape' && LNM.el) { lnmClose(); if (LNM.tr) LNM.tr.focus(); } });

/* ---------- page ---------- */
function render(enter) {
  renderHeader(); boardOff();
  $('rd-main').innerHTML = rulesHTML() + lensHTML() + `<section class="wall S-${S.sort}" id="rd-wall" aria-label="Every post"><div class="feed" id="rd-feed">${feedHTML()}</div><svg class="tlines" id="rd-tlines" aria-hidden="true"></svg><span class="thead" id="rd-thead" aria-hidden="true"></span></section><p class="foot">${esc(note())}</p>`;
  mountBoard($('rd-main')); renderLens(false); applySort(); decorate(false); renderFocusUI(); revealWall();
  if (enter !== false && !RM) {
    // the board arrives the way Lead's does: the reader's line word by word, then each rule's row rising in rank
    // order, its strength filling in behind it and its numbers rolling up into place
    const ru = $('rd-rules');
    if (ru) {
      const who = ru.querySelector('.rbd-who');
      if (who) { who.innerHTML = who.textContent.split(' ').map((w) => `<span class="w">${esc(w)}</span>`).join(' '); qa('.w', who).forEach((w, k) => rise(w, { y: 18, dur: 820, delay: 80 + k * 45 })); }
      qa('.rbd-top .tj-k, .rbd-sub', ru).forEach((el, k) => rise(el, { y: 10, dur: 700, delay: k ? 560 : 0 }));
      qa('.rbd-bh, .rbd-cols', ru).forEach((el, k) => rise(el, { y: 10, dur: 640, delay: 700 + k * 60 }));
      qa('.rbd-row', ru).forEach((row, k) => {
        const d = 820 + k * 90;
        anim(row, [{ opacity: 0, transform: 'translateY(22px)' }, { opacity: 1, transform: 'none' }], { dur: 760, delay: d });
        const f = row.querySelector('.rbd-fill'); if (f) anim(f, [{ transform: 'scaleX(0)', opacity: 0 }, { opacity: 1, offset: 0.5 }, { transform: `scaleX(${+f.style.getPropertyValue('--s') || 0})`, opacity: 1 }], { dur: 1100, delay: d + 160 });
        const c = row.querySelector('.rbd-crown'); if (c) anim(c, [{ opacity: 0 }, { opacity: 1 }], { dur: 900, delay: d + 300 });
        qa('.slt > span', row).forEach((x, j) => anim(x, [{ transform: 'translateY(105%)' }, { transform: 'none' }], { dur: 720, delay: d + 220 + j * 70 }));
        qa('.rbd-bar > i', row).forEach((x, j) => anim(x, [{ transform: 'scaleX(0)' }, { transform: `scaleX(${+x.style.getPropertyValue('--v') || 0})` }], { dur: 1000, delay: d + 300 + j * 80 }));
      });
      qa('.rbd-foot > *, .rbd-note', ru).forEach((el, k) => rise(el, { y: 10, dur: 680, delay: 1300 + k * 80 }));
    }
  }
}
function switchFeeder(k) {
  if (k === S.f) return;
  closeOv(); closeDrop(); clearTrail();
  MORPH.until = 0; MORPH.last = null; INS.settle = 0; clearTimeout(INS.wait);
  clearTimeout(MORPH.anchor); MORPH.anchor = 0; MORPH.anchorEnd = 0; document.documentElement.style.overflowAnchor = '';
  qa('.ins-g, .mw-g').forEach((g) => g.remove());
  Object.assign(S, { f: k, sel: null, pick: null, sort: 'recent', layer: 'read', tr: null, rule: null }); SPY.cur = null; SPY.goal = null;
  tellHeader();
  buzz();
  // the tab has already played the old read's leave (ReadTab, readMotion): the next one builds in at once
  scrollTo({ top: 0 });
  render();
  enterPage();
}
/* how a read arrives, on a fresh mount and after a switch: never as one block. The trajectory's card rises
   while its own entrance plays inside it (render: boxes, fills, the run's read, the bloom); the lens's kicker
   and the lens follow a beat apart; the wall comes in after them, tile by tile (revealWall) */
function enterPage() {
  if (RM) return;
  const t = $('rd-rules'), lh = document.querySelector('#rd-main .lens-head'), ln = $('rd-lens');
  if (t) anim(t, [{ opacity: 0, transform: 'translateY(18px)' }, { opacity: 1, transform: 'none' }], { dur: 720 });
  if (lh) rise(lh, { y: 14, dur: 680, delay: t ? 900 : 440 });
  if (ln) rise(ln, { y: 14, dur: 680, delay: t ? 970 : 510 });
}

/* ---------- events ---------- */
listen(document, 'click', (e) => {
  if (outsideTap(e)) return;
  if (boardClick(e)) return;
  const q = (s) => e.target.closest(s); let b;
  if ((b = q('[data-drop]'))) { const k = b.dataset.drop; if (S.drop === k) closeDrop(); else openDrop(k); return; }
  if ((b = q('[data-clear]'))) { buzz(); clearFocus(); return; }
  if ((b = q('[data-more]'))) { const p = $('rd-trsup'), open = p.classList.toggle('clamp'); b.textContent = open ? 'More' : 'Less'; b.setAttribute('aria-expanded', String(!open)); return; }
  if ((b = q('[data-runjump]'))) { runJump(+b.dataset.runjump); return; }
  if ((b = q('[data-rule]'))) { lightRule(b.dataset.rule); return; }
  if ((b = q('[data-rsel]'))) { setRule(b.dataset.rsel); return; }
  if ((b = q('[data-bdmore]'))) { toggleBand(+b.dataset.bdmore); return; }
  if ((b = q('[data-jump]'))) { goBand(b.dataset.jump); return; }
  if ((b = q('[data-tropen]'))) { openEntry({ type: 'run', id: +b.dataset.tropen }, b); return; }
  if ((b = q('[data-unpick]'))) { unpickPetal(); return; }
  if ((b = q('[data-petal]'))) { pickPetal(b.dataset.petal); return; }
  if ((b = q('#rd-trbloom'))) { unpickPetal(); return; }
  if ((b = q('[data-trace]'))) { selectPost(b.dataset.trace, { scroll: true }); return; }
  if ((b = q('[data-fm-bottom-nav]'))) return; // the app's nav does its own job (and its own haptics)
  if ((b = q('[data-lens]'))) { setLens(b.dataset.lens); return; }
  if ((b = q('[data-pick]'))) { pick(b.dataset.pick); if (S.drop === 'lens') closeDrop(); return; }
  if ((b = q('#rd-closeOv')) || (b = q('#rd-scrim'))) { closeOv(); return; }
  if ((b = q('[data-back]'))) { S.stack.pop(); swapOv(-1); return; }
  if ((b = q('[data-step]'))) { const cur = M().byId[S.stack[S.stack.length - 1].id], nx = M().byId[b.dataset.step]; stepTo(nx.id, nx.i > cur.i ? 1 : -1); return; }
  if ((b = q('[data-open]'))) { const id = b.dataset.open, own = b.closest('#rd-trcard'); openEntry({ type: 'post', id }, own ? own.querySelector('.pcover .tile') : document.querySelector(`#rd-feed .tile[data-post="${id}"]`)); return; }
  if ((b = q('[data-sel]'))) { const id = b.dataset.sel; if (S.sel === id) openEntry({ type: 'post', id }, document.querySelector(`#rd-feed .tile[data-post="${id}"]`)); else selectPost(id, { scroll: true }); return; }
  if ((b = q('#rd-feed .tile[data-post]'))) { const id = b.dataset.post; if (S.sel === id) openEntry({ type: 'post', id }, b); else selectPost(id, { scroll: true }); return; }
  if ((b = q('[data-post]'))) { openEntry({ type: 'post', id: b.dataset.post }, b); return; }
  if ((b = q('[data-concept]'))) { goIdea(b.dataset.concept); return; }
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
/* a scroll the page makes for you keeps the header folded, whichever way it goes */
function hold() { hdrLock = performance.now() + 1000; acc = 0; setCompact(true); }
function quietTo(top) {
  hold();
  scrollTo({ top: Math.max(0, top), behavior: RM ? 'auto' : 'smooth' });
}
let lastScrollAt = 0;
function onScroll() {
  const y = scrollY, dy = y - lastY; lastY = y;
  // a scroll that starts after a jump's own scroll has come to rest is yours: the jump lets go of its mark
  const t = performance.now(); if (SPY.goal != null && t > SPY.goalUntil && t - lastScrollAt > 180) SPY.goal = null; lastScrollAt = t;
  // the header folds as Lead's does: 150px down folds it, 64px back up opens it, and the top 30px keep it open
  if (performance.now() < hdrLock) acc = 0;
  else if (y <= 30) { acc = 0; setCompact(false); }
  else { if ((dy > 0 && acc < 0) || (dy < 0 && acc > 0)) acc = 0; acc += dy; if (acc > 150) { setCompact(true); acc = 0; } else if (acc < -64) { setCompact(false); acc = 0; } }
  spyBand();
}
listen(window, 'scroll', () => { if (ticking) return; ticking = true; requestAnimationFrame(() => { ticking = false; onScroll(); }); }, { passive: true });
let rz;
listen(window, 'resize', () => {
  clearTimeout(rz);
  rz = setTimeout(() => { fixPods(); setCompact(S.compact); placeLens(); renderFocusUI(); SPY.tops = null; spyBand(true); if (S.sel && S.sort === 'recent' && trailOn()) playTrail(M().byId[S.sel]); rollDesc($('rd-kd'), 0); rollDesc($('rd-kd2'), 0); }, 160);
});
if (document.fonts) document.fonts.ready.then(() => { if (alive) fixPods(); });
freshChrome();
render();
enterPage();
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
  qa(':scope > .pfly, :scope > .fly, :scope > .ins-g, :scope > .cfly, :scope > .lnm, :scope > .fs-g', layer).forEach((n) => n.remove());
  try { if (LNM.tr) LNM.tr.style.visibility = ''; LNM.el = null; } catch (_) { /* not mounted that far */ }
  qa('.v15-away').forEach((x) => x.classList.remove('v15-away'));
  if (bodyLock) { document.documentElement.style.overflow = bodyPrev; bodyLock = false; }
  if (MORPH.anchor) { clearTimeout(MORPH.anchor); MORPH.anchor = 0; MORPH.anchorEnd = 0; document.documentElement.style.overflowAnchor = ''; }
}
function freshChrome() {
  quietLayer();
  const hdr = $('rd-hdr'), hr = $('rd-hread');
  hdr.classList.remove('reading');
  if (hr) { hr.innerHTML = ''; hr._h = undefined; }
  S.compact = false; setCompressed(false);
  placeLens();
}

function cleanup() {
  if (!alive) return;
  alive = false;
  offs.splice(0).forEach((off) => off());
  timers.forEach((id) => window.clearTimeout(id)); timers.clear();
  frames.forEach((id) => window.cancelAnimationFrame(id)); frames.clear();
  if (wallIO) { wallIO.disconnect(); wallIO = null; }
  if (ro) { ro.disconnect(); ro = null; }
  boardOff();
  // the script's own motion on the tab's pieces (CSS transitions and keyframes belong to the stylesheet)
  const roots = [$('rd-main'), layer, $('rd-hdr')].filter(Boolean);
  document.getAnimations().forEach((a) => {
    const t = a.effect && a.effect.target;
    if (!t || (window.CSSTransition && a instanceof CSSTransition) || (window.CSSAnimation && a instanceof CSSAnimation)) return;
    if (roots.some((r) => r.contains(t))) a.cancel();
  });
  // the sheet, the header's panel, the tip, every stand-in and flying copy, the page's scroll lock
  quietLayer();
  const main = $('rd-main'); if (main) { qa('.mw-g, .chips.ghost, .ins-old', main).forEach((n) => n.remove()); main.style.removeProperty('--rd-hb'); main.style.pointerEvents = ''; }
  // the readout (React may move on to a feeder Read has nothing on while the tab stays on screen)
  const hdr = $('rd-hdr'), hr = $('rd-hread');
  if (hdr) hdr.classList.remove('reading');
  if (hr) { hr.innerHTML = ''; hr._h = undefined; }
}
/* React's handle on the mount: switch the feeder being read (once the tab has played the old one out), and
   undo everything */
return { show: (k) => { if (alive && DATA.feeders[k]) switchFeeder(k); }, unmount: cleanup };
}
