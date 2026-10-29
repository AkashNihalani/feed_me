/* The Feeder File as a solar system woven by its feed: the Read tab's top component, built from the same parts as the
   map before it (feederFileEngine): a frosted floor that is the account's usual post, things as hex badges standing on
   hairline stems, tracked-caps names, glows kept faint, and what recedes goes grey, never pale pink.
   The centre is the account itself: its profile picture in a crimson ring, standing over a faint crimson pool with a
   dial etched round its foot, captioned with what the reader says it is being right now. Round it, everything in its
   posts, in five wedges that each answer one question (Cast: who's on screen; Formats: what it keeps making; Products
   and deals: what it sells; Signatures: how you'd know it; References: what it rides), and on three orbits by how much
   it is its own (own, regulars, borrowed). The things that keep turning up are planets: hex badges sized by how many
   posts they're in, standing above the floor and red when most of their posts landed, under it and grey when most
   faded. Only what earns it gets a node: the reader asks of each thing whether the post would be a different post
   without it, keeps what a post turns on (it carries the post) and what says who the account is being (it signals,
   with the stance), and leaves out whatever is only named or seen in passing. What carries a post, or turns up in two
   or more, is a planet; something that signals in one post is a small hex on its wedge and orbit. Each says what it
   does for its posts (or what it says about the account). Every thing has one home.
   Round the outside stands the feed: every post as its own cover, 4:5 like the feed, standing above its ring as far as
   it landed above the account's usual post and hanging under it as far as it fell below; a red frame (and stem) for its
   format's top quarter, ink when it landed, grey when it fell. Each post stands beside what it's made of, and a thread
   runs from every post to every thing in it, so the system is woven by the feed: what turns up together shares posts,
   and you see it share them. Threads rest grey; red only at full strength (an open top-quarter thread, or the light
   running along one), because what recedes goes grey, never pale pink.
   It arrives the way the feed went up: the rings sweep in and the account rises, then post by post in posting order
   each card rises and sends its threads in, and each thing appears as its first thread reaches it. After that the feed
   replays itself, one post at a time in the order it went up, light running in along its threads to its things; left
   alone a while, it drifts round very slowly. Point at or open a post and its threads light and its things are named,
   and softly the few posts most alike with it; open a thing and its posts stand out on the ring, light runs in to it
   along its threads, and softly, in ink and unnamed, the other things its posts hold, with faint threads to them (its
   cross ties, findable at a glance), the rest gone grey. Opening keeps the whole system in view, leaning a
   little toward what's open; ‹ › (or the arrow keys) step to the next; closing glides back to the view you had before.
   The reading is the account, its numbers, what it feeds and fades on, then one row per wedge that opens in place to its
   things with their numbers (so it never runs long). Opening one turns the reading to its page: its read (what it is,
   what the account is going for with it, what it got, with the posts it quotes as links), the law behind it, and its
   posts as a list, best first, the account's usual drawn between the ones above it and the ones under. A law's page
   leads with where its posts landed (each side on a ledge, as the rules always showed it), then why it works, what to
   do, its proof folded, how the reading got there. A post opens its post mortem the way the wall does (data-post); a law
   or a thing a reading names opens here (data-law, data-node, and 'rd:law' / 'rd:node' from the rest of the page).
   Every other control here is data-ff*. Results are said one way everywhere, as in Feed: Top % of the format's last
   90 days.
   On a wide card the reading sits beside the system; on a narrow one right under it, in the page itself, so a swipe
   anywhere on the card scrolls the page (a sideways drag turns the system, two fingers zoom it). In the read's layer, or
   full screen, it is the whole of what you're in: a mouse drag turns it round and tips it up and down, a scroll zooms
   toward the pointer (one way of moving, said beside its buttons); on a phone one finger moves it, a pinch zooms toward
   the fingers and two fingers turn it.
   Plain JS like readerEngine; FeederFile.tsx mounts it and tears it down. Styled by feederFile.css under .ff. */
import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { LineSegments2 } from 'three/addons/lines/LineSegments2.js';
import { LineSegmentsGeometry } from 'three/addons/lines/LineSegmentsGeometry.js';
import { LineMaterial } from 'three/addons/lines/LineMaterial.js';
import { TEMPO } from '@/lib/tempo';
import { proofHTML, layProof, playProof, linkText, dealOf, dealWith } from '@/components/read/readProofs';

// the panel and the system's motion play at the app's pace (lib/tempo)
const tm = (o) => ({ ...o, duration: (o.duration ?? 0) * TEMPO, delay: (o.delay ?? 0) * TEMPO });
// the app's carry curve, cubic-bezier(.3, .3, .2, 1), for every glide the camera and the moons make
const bezier = (x1, y1, x2, y2) => { const A = (a, b) => 1 - 3 * b + 3 * a, B = (a, b) => 3 * b - 6 * a, Cc = (a) => 3 * a, f = (t, a, b) => ((A(a, b) * t + B(a, b)) * t + Cc(a)) * t, d = (t, a, b) => 3 * A(a, b) * t * t + 2 * B(a, b) * t + Cc(a);
  return (x) => { if (x <= 0) return 0; if (x >= 1) return 1; let t = x; for (let k = 0; k < 8; k++) { const dx = f(t, x1, x2) - x, dd = d(t, x1, x2); if (Math.abs(dx) < 1e-5 || !dd) break; t -= dx / dd; } return f(Math.max(0, Math.min(1, t)), y1, y2); }; };
const carry = bezier(.3, .3, .2, 1);
const CARRY = 'cubic-bezier(.3, .3, .2, 1)';

const TEMPLATE = `<div class="ff-wrap sys" data-ff="wrap">
    <div class="ff-head"><div class="ff-title"><span class="ff-eyebrow"><i></i>Feeder File</span></div></div>
    <div class="ff-stage" data-ff="stage">
      <div class="ff-map" data-ff="map"><canvas data-ff="cv" aria-label="The account as a solar system: the account in the middle, everything in its posts round it in five wedges, closer when it is more its own, bigger when in more posts, higher and red when more of its posts landed."></canvas>
      <div class="ff-labels" data-ff="labels"></div>
      <span class="ff-axis" data-ff="axisHi">↑ Feeding</span><span class="ff-axis mid" data-ff="axisMid"></span><span class="ff-axis" data-ff="axisLo">↓ Fading</span></div>
      <div class="ff-hud" data-ff="hud"><p class="ff-key ff-lg sys" data-ff="key" aria-hidden="true"></p></div>
    <div class="ff-tools ov">
      <span class="ff-hint" aria-hidden="true"><span><svg class="ic" viewBox="0 0 24 24"><path d="M19.5 12a7.5 7.5 0 1 1-2.2-5.3M19.5 4.5v4h-4"/></svg>Drag to turn</span><span><svg class="ic" viewBox="0 0 24 24"><path d="M12 3.5a5 5 0 0 0-5 5v7a5 5 0 0 0 10 0v-7a5 5 0 0 0-5-5zM12 7v3"/></svg>Scroll to zoom</span></span>
      <button type="button" class="ff-tool" data-ff="full" aria-label="Full screen"><svg class="ic" viewBox="0 0 24 24" aria-hidden="true"><path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5"/></svg><span>Full screen</span></button>
      <button type="button" class="ff-tool" data-ff="zfit" aria-label="Show everything"><svg class="ic" viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="4.5"/><path d="M12 3v3.5M12 17.5V21M3 12h3.5M17.5 12H21"/></svg><span>Reset</span></button>
      <button type="button" class="ff-tool sq" data-ff="zout" aria-label="Zoom out">−</button>
      <button type="button" class="ff-tool sq" data-ff="zin" aria-label="Zoom in">+</button>
    </div>
    </div>
    <div class="ff-body" data-ff="detail"><section class="ff-panel" data-ff="reading" aria-live="polite"></section></div>
  </div>`;

/** @param {HTMLElement} host @param {{ data: any, thumbs?: Record<string, string>, avatar?: string | null }} opts */
// layer: an element the full screen and the phone's sheet are put in instead of the page's body (the read's own layer,
// which carries them as it opens and closes); then the map's "Close" in full screen on a phone hands back to the read
// (ff:leave) instead of playing its own way out. intro: false draws the map whole from its first frame (no build-up);
// onReady: told once the map is ready (and in full screen, if it was asked for with no motion)
export function mountFeederSystem(host, { data: D, thumbs: THUMBS = {}, avatar: AVATAR = null, layer: LAYER = null, intro: INTRO = true, onReady: ON_READY = null }) {
host.innerHTML = TEMPLATE;
const q = (id) => host.querySelector(`[data-ff="${id}"]`);
let dead = false, raf = 0;

const med = (a) => { const s = [...a].sort((x, y) => x - y), m = s.length >> 1; return s.length ? (s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2) : null; };
const clamp = (v, a = 0, b = 1) => Math.max(a, Math.min(b, v));
const isDark = () => document.documentElement.classList.contains('dark');
const motionQ = matchMedia('(prefers-reduced-motion: reduce)'); let reduce = motionQ.matches; const onMotion = (e) => { reduce = e.matches; }; motionQ.addEventListener('change', onMotion);
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
  sun: 'M12 16.5a4.5 4.5 0 1 0 0-9 4.5 4.5 0 0 0 0 9zM12 2v2.5M12 19.5V22M2 12h2.5M19.5 12H22M4.9 4.9l1.8 1.8M17.3 17.3l1.8 1.8M4.9 19.1l1.8-1.8M17.3 6.7l1.8-1.8',
};
const svg = (ic) => `<svg class="ic" viewBox="0 0 24 24" aria-hidden="true"><path d="${ICON[ic] || ICON.tag}"/></svg>`;

// ---------- model: the posts, then the account at the centre, its planets and its small bodies, each with its posts
const posts = D.posts.map((p) => ({ ...p, kind: 'post', frags: [] })), np = posts.length;
(D.frags || []).forEach((f) => { if (posts[f.p]) posts[f.p].frags.push(f); });
posts.forEach((p, k) => { p.of = k; });
const poolOf = (p) => p.pool || np, LANEW = { reel: 'reels', carousel: 'carousels', image: 'images' }, LANE1 = { reel: 'reel', carousel: 'carousel', image: 'image' };
const multi = new Set(posts.map((p) => p.lane || 'post')).size > 1;
const isTop = (p) => p.rank <= Math.max(1, Math.round(poolOf(p) / 4));
const topP = (p) => p.top ?? Math.max(1, Math.round((p.rank - .5) / poolOf(p) * 100));
const place = (p) => `Top ${topP(p)}%${multi && LANEW[p.lane] ? ` of ${LANEW[p.lane]}` : ''}`;
// the split the whole read runs on: feeding = landed in the top half of its format's last 90 days, fading = under it
const feeds = (p) => p.rank <= poolOf(p) / 2;
// where a post landed against its format's pool: 0 at the top, 1 at the bottom; and as a height, 1 top to -1 bottom
const pctOf = (p) => clamp((p.rank - .5) / poolOf(p));
const power = (p) => 1 - 2 * pctOf(p);
// the ring stands at most 30 covers (v10): the last two runs of 10, then the best of the rest. Every other post is a small
// mark on the ring at its height, coloured by where it landed, and stands its cover only while it's lit or open. At 30
// posts or fewer every post is a card, as before
const RUN = 10, CARDS = 30;
{ const older = posts.slice(0, Math.max(0, np - 2 * RUN)).sort((a, b) => pctOf(a) - pctOf(b)), keep = new Set([...posts.slice(-2 * RUN), ...older.slice(0, CARDS - 2 * RUN)]); posts.forEach((p) => { p.card = keep.has(p); }); }
const nCard = posts.filter((p) => p.card).length, DENSE = np > CARDS;
function sideOf(ps) { const n = ps.length, f = ps.filter(feeds).length; return { n, feed: f, fade: n - f, share: n ? f / n : 0 }; }
const dspan = (ps) => { if (!ps.length) return ''; const a = ps[0].date, b = ps[ps.length - 1].date; if (a === b) return a; const [da, ma] = a.split(' '), [db, mb] = b.split(' '); return ma === mb ? `${da}–${db} ${ma}` : `${a} – ${b}`; };
// who the account is, for the copy: his/he/him, her/she/her, their/they/them, its/it/it
const WHO = D.who || 'its', Who = WHO.charAt(0).toUpperCase() + WHO.slice(1), SUBJ = { his: 'he', her: 'she', their: 'they', its: 'it' }[WHO] || 'it', OBJ = { his: 'him', her: 'her', their: 'them', its: 'it' }[WHO] || 'it', s3 = SUBJ === 'they' ? '' : 's';
const ORBIT = ['Own', 'Regular', 'Borrowed'], ORBIT_LONG = [`${Who} own`, `${Who} regulars`, `What ${SUBJ} borrow${s3}`];
// v12: when the file carries its fixed sections (Patterns, People, Promoted, Places) they are the wedges, named by the file;
// a section the file leaves out gets no wedge. Without them the five older wedges stand as before
const SECT = Array.isArray(D.sections) && D.sections.length ? D.sections : null;
const SECT_DEF = [{ k: 'patterns', name: 'Patterns', q: 'What keeps coming back' }, { k: 'people', name: 'People', q: 'Who shows up' }, { k: 'promoted', name: 'Promoted', q: `What ${SUBJ} sell${s3}` }, { k: 'places', name: 'Places', q: 'Where it happens' }];
const CATS = SECT ? SECT_DEF.map((d, k) => { const s = SECT.find((o) => o.cat === k); return s ? { k: s.k || d.k, name: s.name || d.name, q: s.q || d.q, line: s.line || '', hits: s.hits || null, n: s.n ?? null } : d; }) : [
  { k: 'cast', name: 'People', q: 'Who’s on screen' },
  { k: 'formats', name: 'Kinds of post', q: `What ${SUBJ} keep${s3} making` },
  { k: 'products', name: 'Products & deals', q: `What ${SUBJ} sell${s3}` },
  { k: 'signatures', name: 'Trademarks', q: `How you’d know ${OBJ}` },
  { k: 'references', name: 'Talked about', q: `What ${SUBJ} talk${s3} about` },
];
const CAT_OR = (c, d) => (c != null && c >= 0 && c < CATS.length ? c : SECT ? 0 : d);
const KIND_CAT = { person: 0, part: 0, device: 1, product: 2, brand: 2, look: 3, sound: 3, line: 3, speech: 3, note: 3, shirt: 3, people: 0, box: 2, film: 1, pin: 4, bolt: 4 };
const byPs = (ps) => ps.map((i) => posts[i]).filter(Boolean).sort((a, b) => a.of - b.of);
const best = (ps) => [...ps].sort((a, b) => a.rank / poolOf(a) - b.rank / poolOf(b))[0];
const sun = { kind: 'sun', name: D.sun.name, line: D.sun.line, ps: byPs(D.sun.ps), icon: 'sun', orbit: -1, cat: -1, doss: D.sun.doss || null };
sun.cover = posts[D.sun.cover] || best(sun.ps);
const allPl = D.planets.map((p) => ({ kind: 'planet', name: p.name, what: p.what || p.line || '', how: p.how || '', does: p.does || '', role: p.role || 'carries', orbit: clamp(p.orbit, 0, 2), cat: CAT_OR(p.cat ?? KIND_CAT[p.kind] ?? KIND_CAT[p.icon], 4), shelf: p.shelf || null, icon: p.icon || 'tag', rule: p.rule || null, ps: byPs(p.ps), doss: p.doss || null, typx: p.typx ?? null, hits: p.hits || null }));
// a long feed finds more things than a wedge has room for (v10): a planet needs a post in every 40 of the feed (one up to
// 79 posts, two at 80+), and a wedge stands its 8 in the most posts. The rest lie on the floor as small hexes beside what
// they turn up with, still in the reading and still openable. At today's 30 posts nothing moves
const PL_MIN = Math.max(1, Math.floor(np / 40)), PL_CAP = 8;
const keepPl = new Set(); CATS.forEach((_, c) => allPl.filter((p) => p.cat === c && (p.ps.length >= PL_MIN || p.rule)).sort((a, b) => b.ps.length - a.ps.length).slice(0, PL_CAP).forEach((p) => keepPl.add(p)));
const planets = allPl.filter((p) => keepPl.has(p)); planets.forEach((p, k) => { p.k = k; });
const bits = [...((D.belt || {}).items || []).map((b) => ({ kind: 'bit', name: b.name, icon: b.icon || 'tag', what: b.what || '', how: '', does: b.does || '', role: b.role || 'signals', orbit: clamp(b.orbit ?? 2, 0, 2), cat: CAT_OR(b.cat ?? KIND_CAT[b.icon], 4), ps: byPs(b.ps) })),
  ...allPl.filter((p) => !keepPl.has(p)).map((p) => ({ ...p, kind: 'bit' }))].filter((b) => b.ps.length);
const solids = [sun, ...planets], bodies = [...solids, ...bits];
const N = [...bodies, ...posts]; N.forEach((x, i) => { x.i = i; x.ph = (i * 2.399) % 6.283; });
bodies.forEach((b) => { Object.assign(b, sideOf(b.ps)); b.hot = b.n > 0 && b.share >= 2 / 3; b.mix = !b.hot && b.share > 1 / 3; b.typ = med(b.ps.map(pctOf)); b.isNew = b !== sun && b.ps[0].of >= np - 10; });
const RULE = Object.fromEntries((D.rules || []).map((r) => [r.id, r]));
// the laws the reader holds (Read v5): what the crowd rewards, each with its record, its proof and the runs that made it.
// Opened from the reading, a law lights its posts on the ring and the parts it covers
const byName = new Map([...planets, ...bits].map((b) => [b.name, b]));
const LAWS = (D.laws || []).map((L, k) => ({ ...L, kind: 'law', k, i: -1 - k, ps_l: L.ps.lands, ps_s: L.ps.sinks, ps: [...new Set([...L.ps.lands, ...L.ps.sinks])].sort((a, b) => a - b).map((j) => posts[j]).filter(Boolean),
  brokeSet: new Set(L.ps.broke.map((j) => posts[j])), partsB: (L.parts || []).map((nm) => byName.get(nm)).filter(Boolean) }));
const LAW = Object.fromEntries(LAWS.map((L) => [L.id, L]));
// the order the reading lists things in, and ‹ › steps through: the account, then each wedge's things, those in the most
// posts first, then those that landed best
// (Patterns lists its kinds before its repeats)
const shelfRank = (b) => (SECT && b.cat === 0 && b.shelf === 'repeat' ? 1 : 0);
const inCat = (k, list) => list.filter((b) => b.cat === k).sort((a, b) => shelfRank(a) - shelfRank(b) || b.n - a.n || a.typ - b.typ || a.orbit - b.orbit);
const STEPS = [sun, ...CATS.flatMap((_, k) => inCat(k, planets))];
// which bodies a post turns up in (the account holds every post, so it's left out), and what a body is seen with
const homesOf = (p) => bodies.filter((b) => b !== sun && b.ps.includes(p));
// ---------- relations: what turns up together and what did alike. Two things are linked when they share posts (the
// more they share, the stronger; red when those posts landed); two planets did alike when their posts typically land
// within seven places of each other's, on the same side of the account's usual, without sharing a post (that is the link)
const shared = (a, b) => a.ps.filter((p) => b.ps.includes(p));
const LINKS = [];
planets.forEach((a, i) => { [...planets.slice(i + 1), ...bits].forEach((b) => { const s2 = shared(a, b); if (s2.length) LINKS.push({ a, b, k: s2.length, feed: s2.filter(feeds).length }); }); });
bodies.forEach((b) => { b.nbr = new Set(); });
LINKS.forEach((l) => { l.a.nbr.add(l.b); l.b.nbr.add(l.a); l.hot = l.feed * 3 >= l.k * 2; l.thick = l.k >= 2; });
planets.forEach((p) => { p.kin = p.n < 2 ? [] : planets.filter((o) => o !== p && !p.nbr.has(o) && o.n >= 2 && Math.abs(o.typ - p.typ) <= .07 && (o.typ <= .5) === (p.typ <= .5)).sort((x, y) => Math.abs(x.typ - p.typ) - Math.abs(y.typ - p.typ)).slice(0, 3); });
// a small body's anchor: the planet it shares the most posts with
bits.forEach((b) => { b.anchor = planets.map((p) => ({ p, k: shared(p, b).length })).filter((o) => o.k).sort((x, y) => y.k - x.k || y.p.n - x.p.n)[0]?.p || null; });

// ---------- the layout: the account in the middle; five wedges round it, three orbits across them; the feed round the
// outside
const sizeOf = (n) => Math.min(.46, .19 + .075 * Math.sqrt(n));
let seed = 11; const rnd = () => ((seed = (seed * 16807) % 2147483647) / 2147483647);
const SUN_Y = .46; sun.size = .5; sun.home = [0, 0, 0]; sun.y = SUN_Y;
planets.forEach((p) => { p.size = sizeOf(p.n); });
// each wedge gets room by what it holds; a gap at the back keeps the orbits' names clear
// (v12 sections: four sectors from 12 o'clock, clockwise, sharing the whole circle with no gap between them)
const BACK = SECT ? 0 : .56, SEP = SECT ? 0 : .09, weight = CATS.map((_, k) => { const a = planets.filter((p) => p.cat === k).length, b = bits.filter((x) => x.cat === k).length; return a || b ? (SECT ? 3 + a + .4 * b : Math.max(1.3, a + .3 * b)) : 0; });
const usedCats = CATS.map((_, k) => k).filter((k) => weight[k] > 0), WSUM = weight.reduce((s, w) => s + w, 0), AVAIL = Math.PI * 2 - BACK - SEP * Math.max(0, usedCats.length - 1);
const WEDGE = []; { let a = -Math.PI / 2 + BACK / 2; usedCats.forEach((k) => { const span = weight[k] / WSUM * AVAIL; WEDGE[k] = { a0: a, a1: a + span, mid: a + span / 2, span }; a += span + SEP; }); }
// the orbits sit at .72, 1.14 and 1.56 unless a wedge needs more room on one: then that orbit and those past it move out
const ORB = [], has = (o) => planets.some((p) => p.orbit === o), hasBits = (o) => bits.some((b) => b.orbit === o);
// v12: a section's things stand in tidy rows, evenly spaced across its sector: one row, or for Patterns two (its kinds on
// the inner row, its repeats on the outer). The row is the node's orbit now, and the radii grow only as far as the
// things need to stand clear of one another
const ROWS = [], SPAD = .05;
if (SECT) {
  const rowOf = (b) => (b.cat === 0 && b.shelf === 'repeat' ? 1 : 0), wOfN = (b) => (b.kind === 'bit' ? .2 : (b.size || .3) + .2);
  usedCats.forEach((k) => { const its = [...inCat(k, planets), ...inCat(k, bits)]; its.forEach((b) => { b.orbit = rowOf(b); }); ROWS[k] = [its.filter((b) => b.orbit === 0), its.filter((b) => b.orbit === 1)]; });
  const need = (r) => Math.max(0, ...usedCats.map((k) => { const row = ROWS[k][r]; return row.length ? row.length * Math.max(...row.map(wOfN)) / Math.max(.3, WEDGE[k].span - 2 * SPAD) : 0; }));
  ORB[0] = Math.max(.95, need(0)); if (usedCats.some((k) => ROWS[k][1].length)) ORB[1] = Math.max(ORB[0] + .55, need(1));
}
if (!SECT) [0, 1, 2].forEach((o) => { const need = Math.max(0, ...usedCats.map((k) => planets.filter((p) => p.cat === k && p.orbit === o).reduce((s, p) => s + p.size + .2, 0) / (WEDGE[k].span * .9)));
  ORB[o] = Math.max(o ? ORB[o - 1] + (has(o) ? .42 : hasBits(o) ? .34 : .26) : .72, has(o) ? [.72, 1.14, 1.56][o] : 0, need); });
const LAST = Math.max(0, ...[0, 1, 2].filter((o) => has(o) || hasBits(o))), RIM = ORB[LAST] + .36;
// the feed round the outside: every post standing above its ring as far as it landed above the account's usual and
// hanging under it as far as it fell below. Cards are 4:5, as wide as the ring leaves room for
const FEED = ORB[LAST] + .8, A0 = -Math.PI / 2, SPAN = Math.PI * 2;
// a mark takes about a quarter of a card's room on the ring, so the covers keep their size as the feed grows
const MARKW = .26, wOf = (p) => (p.card ? 1 : MARKW), RU = SPAN / Math.max(1, posts.reduce((s, p) => s + wOf(p), 0));
const CW = Math.min(.27, RU * FEED * .62), FR = FEED + CW * .9 + .12, LBL_R = FR + .3; // (v12: the section names stand outside the feed's ring)
if (SECT) usedCats.forEach((k) => ROWS[k].forEach((row, r) => { const W = WEDGE[k], m = row.length; row.forEach((b, j) => { const a = W.a0 + SPAD + (W.span - 2 * SPAD) * (j + .5) / m, R = ORB[r]; b.ang = a; b.home = [Math.cos(a) * R, 0, Math.sin(a) * R]; if (b.kind === 'bit') b.size = .085; }); }));
// planets spread across their wedge on their orbit, the biggest at the wedge's middle
if (!SECT) usedCats.forEach((k) => [0, 1, 2].forEach((o) => { const W = WEDGE[k], ps = planets.filter((p) => p.cat === k && p.orbit === o).sort((a, b) => b.n - a.n), m = ps.length; if (!m) return;
  const slots = Array.from({ length: m }, (_, j) => W.a0 + W.span * (j + .5) / m).sort((a, b) => Math.abs(a - W.mid) - Math.abs(b - W.mid));
  ps.forEach((p, j) => { const a = slots[j]; p.ang = a; p.home = [Math.cos(a) * ORB[o], 0, Math.sin(a) * ORB[o]]; }); }));
// small bodies sit beside what they appear with: each on its own wedge and orbit, drawn round toward the planet it
// shares most posts with (right beside it when that planet is in the same wedge, at the wedge's nearest edge when not),
// spread so none overlap, clear of the planets
if (!SECT) { const angDist = (a, b) => Math.abs(Math.atan2(Math.sin(a - b), Math.cos(a - b))), sdiff = (a, b) => Math.atan2(Math.sin(a - b), Math.cos(a - b));
  usedCats.forEach((k) => [0, 1, 2].forEach((o) => { const W = WEDGE[k], its = bits.filter((b) => b.cat === k && b.orbit === o), m = its.length, r = ORB[o]; if (!m) return;
    its.forEach((b, j) => { const an = b.anchor; b.want = an ? W.mid + clamp(sdiff(an.ang, W.mid) + (rnd() - .5) * .24, -W.span / 2 + .03, W.span / 2 - .03) : W.a0 + W.span * (j + .5) / m; });
    its.sort((x, y) => x.want - y.want); const g = Math.min(W.span / (m + 1), .12 / r);
    for (let it = 0; it < 80; it++) for (let j = 1; j < m; j++) { const d = its[j].want - its[j - 1].want; if (d < g) { const push = (g - d) / 2; its[j - 1].want = Math.max(W.a0 + .02, its[j - 1].want - push); its[j].want = Math.min(W.a1 - .02, its[j].want + push); } }
    its.forEach((b, j) => { const a = b.want; let rr = r + (rnd() - .5) * .2; const hit = planets.find((p) => p.orbit === o && angDist(a, p.ang) < (p.size * .5 + .07) / r); if (hit) rr = r + (j % 2 ? .19 : -.19);
      if (b.anchor && b.anchor.cat === k && b.anchor.orbit !== o) rr += (ORB[b.anchor.orbit] - r) * .22;
      b.ang = a; b.home = [Math.cos(a) * rr, 0, Math.sin(a) * rr]; b.size = .085; }); })); }
// each post stands on the ring beside what it's made of: at the weighted middle of its things' angles (a planet counts
// for more than a passing thing), then the posts spread evenly enough not to touch, keeping that order. A thread to a
// post's main thing runs short; one that crosses the system is a real tie between two parts of it
posts.forEach((p, k) => { let ax = 0, az = 0; homesOf(p).forEach((b) => { const w = b.kind === 'planet' ? 2 + .25 * b.n : 1; ax += Math.cos(b.ang) * w; az += Math.sin(b.ang) * w; }); p.want = ax || az ? Math.atan2(az, ax) : A0 + SPAN * k / Math.max(1, np); p.ang = p.want; p.u = np > 1 ? k / (np - 1) : .5; });
{ const ring = [...posts].sort((a, b) => a.want - b.want);
  for (let it = 0; it < 240; it++) for (let j = 0; j < np; j++) { const a = ring[j], b = ring[(j + 1) % np], g = RU * .9 * (wOf(a) + wOf(b)) / 2; const d = b.ang - a.ang + (j === np - 1 ? SPAN : 0); if (d < g) { const push = (g - d) / 2; a.ang -= push; b.ang += push; } } }
posts.forEach((p) => { p.ring = [Math.cos(p.ang) * FEED, 0, Math.sin(p.ang) * FEED]; p.h = power(p) * .42; });
// heights: a planet rises the more of its posts landed and sinks the more faded, even on the floor; something in one or
// two posts lifts less than a pattern does. Small bodies lie on the floor. The account stands over them all
planets.forEach((b) => { b.y = (b.share - .5) * 1.1 * Math.min(1, (b.n + 1) / 4); });
bits.forEach((b) => { b.y = 0; });
const YMAX = Math.max(SUN_Y + sun.size, ...planets.map((b) => Math.abs(b.y) + b.size), .42 + CW * 1.6);
bodies.forEach((b) => { b.app = 0; b.vis = 1; b.pos = [...b.home]; b.foot = [...b.home]; b.top = [...b.home]; });
posts.forEach((p) => { p.app = 0; p.vis = 1; p.sc = 0; p.cur = 0; p.pulse = -1; p.pos = [...p.ring]; p.cats = new Set(homesOf(p).map((b) => b.cat)); });
// the loom: a thread from every post to every thing in it
const LOOM = []; [...planets, ...bits].forEach((b) => b.ps.forEach((p) => LOOM.push({ p, b, top: isTop(p), fed: feeds(p), vis: 0 })));
LOOM.forEach((l, j) => { l.j = j; });
// how strong the resting threads are: as today up to about 80 of them, fainter past that
const LDENS = Math.min(1, 80 / Math.max(1, LOOM.filter((l) => l.p.card).length));
posts.forEach((p) => { p.links = LOOM.filter((l) => l.p === p); });
bodies.forEach((b) => { b.links = b === sun ? sun.ps.flatMap((p) => p.links) : LOOM.filter((l) => l.b === b); });
// what's lit when something is open or pointed at: a post, its threads and the things in it, then softly the few posts
// most alike with it and their threads into what they share; a thing (or a rule), its threads and its posts, then softly
// the other threads of those posts and the things they reach (what it's seen with: its cross ties, findable at a glance)
// (Oct 22 night: the planets that did alike are no longer lit with it, since nothing on the map draws that tie). The
// strongest ties (t2s: in the most of its posts) and the threads into them (lit2) carry a light of their own, so you see
// where its posts lead without the whole map lighting up; a post's alike posts all do
const FOCUS = new Map();
// the few things most tied to a set of posts besides their own: in two or more of them (any, when there are only a few
// posts), the most shared first, then the rarer
function strongest(ps, own) {
  const co = new Map(); ps.forEach((p) => homesOf(p).forEach((b) => { if (!own.has(b)) co.set(b, (co.get(b) || 0) + 1); }));
  const need = ps.length >= 4 ? 2 : 1; return new Set([...co].filter(([, c]) => c >= need).sort((a, b) => b[1] - a[1] || a[0].ps.length - b[0].ps.length).slice(0, 5).map(([b]) => b));
}
function focusOf(x) {
  if (!x) return null; if (FOCUS.has(x)) return FOCUS.get(x);
  let f; const none = new Set();
  if (x.kind === 'post') { const t1 = new Set(homesOf(x)), score = new Map();
    // alike: sharing its rarer things (a thing in most of the feed ties nothing), the rarest counting most
    t1.forEach((b) => { if (b.ps.length > Math.max(3, np * .25)) return; b.ps.forEach((p) => { if (p !== x) score.set(p, (score.get(p) || 0) + 1 / b.ps.length); }); });
    const p2 = new Set([...score].sort((a, b) => b[1] - a[1]).slice(0, 6).map(([p]) => p)), l2 = new Set(); p2.forEach((p) => p.links.forEach((l) => { if (t1.has(l.b)) l2.add(l); }));
    f = { x, ps: new Set([x]), p2, t1, t2: none, t2s: none, l1: new Set(x.links), l2, lit2: l2 }; }
  else if (x.kind === 'wedge') f = { x, ps: new Set(x.ps), p2: none, t1: new Set(x.things), t2: none, t2s: none, l1: new Set(x.things.flatMap((b) => b.links)), l2: none, lit2: none };
  else { const law = x.kind === 'law' || x.kind === 'evid', own = law ? new Set(x.partsB) : new Set([x]), ps = x.ps, l1 = new Set(), l2 = new Set(), t2 = new Set();
    ps.forEach((p) => p.links.forEach((l) => { if (own.has(l.b)) { if (law) l1.add(l); } else { if (x !== sun) l2.add(l); t2.add(l.b); } }));
    const t2s = x === sun ? none : strongest(ps, own);
    f = { x, ps: new Set(ps), p2: none, t1: own, t2, t2s, l1: law ? l1 : new Set(x.links), l2, lit2: new Set([...l2].filter((l) => t2s.has(l.b))) }; }
  FOCUS.set(x, f); return f;
}

// ---------- three
if (SECT) host.querySelector('[data-ff="wrap"]').classList.add('sect');
const stage = q('stage'), cv = q('cv'), labelsEl = q('labels'), wrap = q('wrap'), hud = q('hud'), mapEl = q('map');
const FONT = getComputedStyle(host).fontFamily || '"Space Grotesk", "Helvetica Neue", Arial, sans-serif';
let dark = isDark();
const renderer = new THREE.WebGLRenderer({ canvas: cv, antialias: true, powerPreference: 'high-performance' });
renderer.setPixelRatio(Math.min(2, devicePixelRatio || 1)); renderer.outputColorSpace = THREE.SRGBColorSpace;
const scene = new THREE.Scene(), camera = new THREE.PerspectiveCamera(40, 1, .01, 80);
const controls = new OrbitControls(camera, cv);
let full = false;
// In the read's layer, or full screen, the map is the whole of what you're in (roaming, below): a mouse drag turns it round
// and tips it up and down, directly, and a scroll zooms in toward where you point (Oct 22 night: a drag that moved the map,
// with ⇧ + drag to turn, was "very unintuitive"; one way of moving only); a finger moves it, a pinch zooms toward the
// fingers, two fingers turn it. As a card in a page, a scroll is the page's (⌘ or ctrl + scroll zooms) and a drag turns it
const roam = () => full || !!LAYER;
// (the controls only turn and tip it: the zooms and the moves are the map's own, below)
Object.assign(controls, { enableDamping: true, dampingFactor: .13, rotateSpeed: .6, enableZoom: false, enablePan: false, minDistance: .6, maxDistance: 9, autoRotateSpeed: .16 });
controls.mouseButtons = { LEFT: THREE.MOUSE.ROTATE, MIDDLE: THREE.MOUSE.DOLLY, RIGHT: THREE.MOUSE.ROTATE };
// how the map takes a finger: in a page a sideways drag turns it, a pinch zooms, a tap opens what's under it, and an
// up-or-down swipe is the page's. Roaming, and while the reading sheet is up on a narrow card (the page holds still then),
// every gesture is the map's: roaming, one finger moves it and two pinch and turn it
function applyGestures() { const r = roam(); wrap.classList.toggle('roam', r); cv.style.touchAction = r || (!wide && sheetOpen) ? 'none' : 'pan-y'; controls.touches = { ONE: r ? -1 : THREE.TOUCH.ROTATE, TWO: THREE.TOUCH.DOLLY_ROTATE }; }
const BG = new THREE.Color(), INK = new THREE.Color(), RED = new THREE.Color(), LOW = new THREE.Color(), FLOOR = new THREE.Color();
// a colour for this theme, faded toward the ground by how much it should recede
const paint = (C, base, it) => C.copy(base).lerp(BG, 1 - clamp(it));
const hx = (c) => `#${c.getHexString()}`;
const radial = (stops) => { const c = document.createElement('canvas'); c.width = c.height = 128; const g = c.getContext('2d'), gr = g.createRadialGradient(64, 64, 0, 64, 64, 64); stops.forEach(([o, a]) => gr.addColorStop(o, `rgba(255,255,255,${a})`)); g.fillStyle = gr; g.fillRect(0, 0, 128, 128); const t = new THREE.CanvasTexture(c); t.colorSpace = THREE.SRGBColorSpace; return t; };
const GLOW = radial([[0, 1], [.2, .5], [.5, .12], [1, 0]]), POOL = radial([[0, 1], [.35, .55], [.7, .14], [1, 0]]), SHADE = radial([[0, 1], [.45, .5], [1, 0]]);
const DOT = (() => { const c = document.createElement('canvas'); c.width = c.height = 64; const g = c.getContext('2d'); const gr = g.createRadialGradient(32, 32, 0, 32, 32, 30); gr.addColorStop(0, 'rgba(255,255,255,1)'); gr.addColorStop(.35, 'rgba(255,255,255,.9)'); gr.addColorStop(1, 'rgba(255,255,255,0)'); g.fillStyle = gr; g.fillRect(0, 0, 64, 64); return new THREE.CanvasTexture(c); })();
// a small body is a small solid hex, the planets' shape at the size of a grain
const HEXDOT = (() => { const c = document.createElement('canvas'); c.width = c.height = 64; const g = c.getContext('2d'); g.fillStyle = '#fff'; g.beginPath(); for (let k = 0; k < 6; k++) { const a = k * Math.PI / 3 - Math.PI / 2, x = 32 + Math.cos(a) * 28, y = 32 + Math.sin(a) * 28; k ? g.lineTo(x, y) : g.moveTo(x, y); } g.closePath(); g.fill(); return new THREE.CanvasTexture(c); })();
const flat = (tex, r, order) => { const m = new THREE.Mesh(new THREE.PlaneGeometry(r * 2, r * 2), new THREE.MeshBasicMaterial({ map: tex, transparent: true, depthWrite: false, opacity: 0, fog: false })); m.rotation.x = -Math.PI / 2; m.renderOrder = order; scene.add(m); return m; };

// the floor: the account's usual post, the line between feeding and fading. A faint tinted disc with a crisp lip: enough
// to read as a surface, never the main thing, clear enough that every stem and thread shows through it
const veilMap = (() => { const c = document.createElement('canvas'); c.width = c.height = 256; const g = c.getContext('2d'), gr = g.createRadialGradient(128, 128, 0, 128, 128, 128); gr.addColorStop(0, '#fff'); gr.addColorStop(.975, '#fff'); gr.addColorStop(1, '#000'); g.fillStyle = gr; g.fillRect(0, 0, 256, 256); return new THREE.CanvasTexture(c); })();
const veil = new THREE.Mesh(new THREE.CircleGeometry(FR, 160), new THREE.MeshBasicMaterial({ transparent: true, depthWrite: false, alphaMap: veilMap, fog: false, side: THREE.DoubleSide }));
veil.rotation.x = -Math.PI / 2; veil.renderOrder = 1; scene.add(veil);
// rings are open lines that sweep in on arrival: their draw range grows round the circle from the back, a bright head
// riding the front of each
const RN = 192, ringPt = (r, k, y) => { const a = -Math.PI / 2 + k / RN * Math.PI * 2; return new THREE.Vector3(Math.cos(a) * r, y, Math.sin(a) * r); };
const ringGeo = (r, y) => new THREE.BufferGeometry().setFromPoints(Array.from({ length: RN + 1 }, (_, k) => ringPt(r, k, y)));
const arcPt = (r, u, y) => { const a = A0 + SPAN * u; return new THREE.Vector3(Math.cos(a) * r, y, Math.sin(a) * r); };
const lineOf = (geo, order = 2) => { const o = new THREE.Line(geo, new THREE.LineBasicMaterial({ transparent: true, depthWrite: false, opacity: 0 })); o.renderOrder = order; scene.add(o); return o; };
const rims = [lineOf(ringGeo(FR * .985, .002))]; rims[0].visible = false;
const orbits = ORB.map((r) => lineOf(ringGeo(r, .003)));
// v12: each sector visibly bounded: a thin divider from near the account to the rim between neighbours, a faint arc along
// the rim, and a very faint tint (alternate sectors a touch deeper); the one picked comes up, the rest sit back
const SECT_LN = [], SECT_TN = [];
if (SECT) {
  const R1 = RIM + .06, V3 = THREE.Vector3;
  usedCats.forEach((k, j) => { const W = WEDGE[k];
    const dv = lineOf(new THREE.BufferGeometry().setFromPoints([new V3(Math.cos(W.a0) * .52, .003, Math.sin(W.a0) * .52), new V3(Math.cos(W.a0) * R1, .003, Math.sin(W.a0) * R1)])); dv.userData.k = -1; SECT_LN.push(dv);
    const ar = lineOf(new THREE.BufferGeometry().setFromPoints(Array.from({ length: 33 }, (_, i) => { const a = W.a0 + .025 + (W.span - .05) * i / 32; return new V3(Math.cos(a) * R1, .003, Math.sin(a) * R1); }))); ar.userData.k = k; SECT_LN.push(ar);
    const tn = new THREE.Mesh(new THREE.RingGeometry(.46, R1, 40, 1, W.a0, W.span), new THREE.MeshBasicMaterial({ transparent: true, depthWrite: false, side: THREE.DoubleSide, opacity: 0 }));
    tn.rotation.x = Math.PI / 2; tn.position.y = .0015; tn.renderOrder = 1; tn.userData.k = k; tn.userData.alt = j % 2; scene.add(tn); SECT_TN.push(tn); });
}
// the feed's ring, drawn round as the posts arrive
const feedRing = lineOf(ringGeo(FEED, .003));
// the account's foot: a faint crimson pool, a red ring and a dial of fine ticks etched round it
const pool = flat(POOL, 1.05, 1.5);
const coreMark = lineOf(ringGeo(.27, .004));
const dial = (() => { const P = []; for (let k = 0; k < 72; k++) { const a = k / 72 * Math.PI * 2, r0 = .31, r1 = k % 6 ? .34 : .375; P.push(Math.cos(a) * r0, .004, Math.sin(a) * r0, Math.cos(a) * r1, .004, Math.sin(a) * r1); } const g = new THREE.BufferGeometry(); g.setAttribute('position', new THREE.Float32BufferAttribute(P, 3)); const m = new THREE.LineSegments(g, new THREE.LineBasicMaterial({ transparent: true, depthWrite: false, opacity: 0 })); m.renderOrder = 2; scene.add(m); return m; })();

// bodies. A planet is a hex badge with an icon for the kind of thing it is: solid red when most of its posts landed,
// ink-edged when they split, grey when most faded. The account is its profile picture in a crimson ring. A post is its
// own cover as a 4:5 card: a bold red frame for its format's top quarter, a fine ink one when it landed above the usual,
// grey and bare when it fell under
const hexPath = (g, cx, cy, r) => { g.beginPath(); for (let k = 0; k < 6; k++) { const a = k * Math.PI / 3 - Math.PI / 2, x = cx + Math.cos(a) * r, y = cy + Math.sin(a) * r; k ? g.lineTo(x, y) : g.moveTo(x, y); } g.closePath(); };
const rrect = (g, x, y, w, h, r) => { g.beginPath(); g.moveTo(x + r, y); g.arcTo(x + w, y, x + w, y + h, r); g.arcTo(x + w, y + h, x, y + h, r); g.arcTo(x, y + h, x, y, r); g.arcTo(x, y, x + w, y, r); g.closePath(); };
const imgs = new Map();
const load = (key, src, then) => { if (!src) return null; if (!imgs.has(key)) { const im = new Image(); im.decoding = 'async'; im.onload = () => { if (!dead) then(); }; im.src = src; imgs.set(key, im); } return imgs.get(key); };
const imgOf = (p) => (p ? load(p.id, THUMBS[p.id], () => redrawFor(p)) : null);
const stateOf = (b) => (b.hot ? 'hot' : b.mix ? 'mix' : 'dn');
const BADGE = {};
function drawBadge(c, icon, state) {
  const g = c.getContext('2d'); g.clearRect(0, 0, 128, 128);
  const hot = state === 'hot', dn = state === 'dn', surf = dark ? hx(BG.clone().lerp(INK, .09)) : '#ffffff';
  const fill = hot ? hx(RED) : surf, edge = hot ? hx(RED.clone().lerp(new THREE.Color(dark ? '#ffffff' : '#000000'), .2)) : dn ? hx(INK.clone().lerp(BG, .45)) : hx(INK.clone().lerp(BG, dark ? .12 : 0)), ink = hot ? '#ffffff' : dn ? hx(INK.clone().lerp(BG, .38)) : hx(INK);
  g.save(); g.shadowColor = dark ? 'rgba(0,0,0,.45)' : 'rgba(18,19,23,.18)'; g.shadowBlur = 7; g.shadowOffsetY = 2; hexPath(g, 64, 64, 53); g.fillStyle = fill; g.fill(); g.restore();
  hexPath(g, 64, 64, 52); g.lineWidth = hot ? 3 : dn ? 3 : 4.5; g.strokeStyle = edge; g.stroke();
  g.save(); g.translate(39, 39); g.scale(50 / 24, 50 / 24); g.lineWidth = 2.1; g.lineCap = 'round'; g.lineJoin = 'round'; g.strokeStyle = ink; g.stroke(new Path2D(ICON[icon] || ICON.tag)); g.restore();
}
const badgeTex = (icon, state) => { const k = `${icon}:${state}`; if (!BADGE[k]) { const c = document.createElement('canvas'); c.width = c.height = 128; const t = new THREE.CanvasTexture(c); t.colorSpace = THREE.SRGBColorSpace; t.anisotropy = 4; BADGE[k] = { c, t, icon, state }; } return BADGE[k].t; };
const tmpC = document.createElement('canvas'); tmpC.width = tmpC.height = 256;
const greyOut = (g, w, h) => { const d = g.getImageData(0, 0, w, h), a = d.data; for (let k = 0; k < a.length; k += 4) { const l = a[k] * .299 + a[k + 1] * .587 + a[k + 2] * .114; a[k] = a[k + 1] = a[k + 2] = l; } g.putImageData(d, 0, 0); };
function drawImgTo(g, im, fallback, cx, cy, r, grey) {
  const tg = tmpC.getContext('2d'), D2 = Math.ceil(r * 2); tg.clearRect(0, 0, 256, 256);
  if (im && im.complete && im.naturalWidth) { const s = Math.min(im.naturalWidth, im.naturalHeight); tg.imageSmoothingQuality = 'high'; tg.drawImage(im, (im.naturalWidth - s) / 2, (im.naturalHeight - s) / 2, s, s, 0, 0, D2, D2); if (grey) greyOut(tg, D2, D2); }
  else { tg.fillStyle = hx(LOW); tg.fillRect(0, 0, D2, D2); tg.fillStyle = hx(INK); tg.font = `700 ${Math.round(D2 * .38)}px ${FONT}`; tg.textAlign = 'center'; tg.textBaseline = 'middle'; tg.fillText((fallback || '·').charAt(0).toUpperCase(), D2 / 2, D2 / 2 + 2); }
  g.save(); g.beginPath(); g.arc(cx, cy, r, 0, 6.283); g.clip(); g.drawImage(tmpC, 0, 0, D2, D2, cx - r, cy - r, D2, D2);
  if (grey) { g.fillStyle = hx(BG); g.globalAlpha = dark ? .45 : .35; g.fillRect(0, 0, 256, 256); g.globalAlpha = 1; } g.restore();
}
// the account: its profile picture (else the cover of the post that shows it best) in a bold crimson ring; and its grey
// twin for while something else is open, because what recedes goes grey, never pale pink
const sunImg = () => (AVATAR ? load('avatar', AVATAR, drawSun) : imgOf(sun.cover));
function drawSun() {
  [[sun.cvs, sun.tex, false], [sun.cvsDim, sun.texDim, true]].forEach(([c, tex, dim]) => { const g = c.getContext('2d'), R = 128; g.clearRect(0, 0, 256, 256);
    g.save(); g.shadowColor = dark ? 'rgba(0,0,0,.55)' : 'rgba(18,19,23,.22)'; g.shadowBlur = dim ? 0 : 14; g.shadowOffsetY = 4; g.beginPath(); g.arc(R, R, 118, 0, 6.283); g.fillStyle = dim ? hx(INK.clone().lerp(BG, .55)) : hx(RED); g.fill(); g.restore();
    g.beginPath(); g.arc(R, R, 104, 0, 6.283); g.fillStyle = hx(BG); g.fill();
    drawImgTo(g, sunImg(), D.handle, R, R, 97, dim); tex.needsUpdate = true; });
}
const KW = 128, KH = 160, cardTmp = document.createElement('canvas'); cardTmp.width = KW; cardTmp.height = KH;
function drawCard(p) {
  const im = imgOf(p), top = isTop(p), fed = feeds(p);
  [[p.cvs, p.tex, false], [p.cvsDim, p.texDim, true]].forEach(([c, tex, dim]) => {
    const g = c.getContext('2d'); g.clearRect(0, 0, KW, KH);
    const grey = dim || !fed, bw = dim ? 3 : top ? 9 : fed ? 5 : 3, x0 = 8, y0 = 5, w = KW - 16, h = KH - 15, r = 16;
    const frame = dim ? hx(INK.clone().lerp(BG, .72)) : top ? hx(RED) : fed ? hx(INK) : hx(INK.clone().lerp(BG, .6));
    g.save(); if (!dim) { g.shadowColor = dark ? 'rgba(0,0,0,.6)' : 'rgba(18,19,23,.26)'; g.shadowBlur = 9; g.shadowOffsetY = 3; } rrect(g, x0, y0, w, h, r); g.fillStyle = frame; g.fill(); g.restore();
    const ix = x0 + bw, iy = y0 + bw, iw = Math.round(w - bw * 2), ih = Math.round(h - bw * 2), tg = cardTmp.getContext('2d'); tg.clearRect(0, 0, KW, KH);
    if (im && im.complete && im.naturalWidth) { const s = Math.min(im.naturalWidth / iw, im.naturalHeight / ih), sw = iw * s, sh = ih * s; tg.imageSmoothingQuality = 'high'; tg.drawImage(im, (im.naturalWidth - sw) / 2, (im.naturalHeight - sh) / 2, sw, sh, 0, 0, iw, ih); if (grey) greyOut(tg, iw, ih); }
    else { tg.fillStyle = hx(LOW); tg.fillRect(0, 0, iw, ih); tg.fillStyle = hx(INK); tg.font = `700 ${Math.round(iw * .4)}px ${FONT}`; tg.textAlign = 'center'; tg.textBaseline = 'middle'; tg.fillText((p.name || '·').charAt(0).toUpperCase(), iw / 2, ih / 2 + 2); }
    g.save(); rrect(g, ix, iy, iw, ih, Math.max(5, r - bw)); g.clip(); g.drawImage(cardTmp, 0, 0, iw, ih, ix, iy, iw, ih);
    if (grey) { g.fillStyle = hx(BG); g.globalAlpha = dim ? .55 : dark ? .42 : .34; g.fillRect(ix, iy, iw, ih); g.globalAlpha = 1; } g.restore();
    tex.needsUpdate = true; });
}
function redrawFor(p) { if (!AVATAR && sun.cover === p) drawSun(); drawCard(p); }
const spriteOf = (map) => { const s = new THREE.Sprite(new THREE.SpriteMaterial({ map, transparent: true, depthWrite: false })); scene.add(s); return s; };
const canvasTex = (S, S2 = S) => { const c = document.createElement('canvas'); c.width = S; c.height = S2; const t = new THREE.CanvasTexture(c); t.colorSpace = THREE.SRGBColorSpace; t.anisotropy = 4; return { c, t }; };
{ const a = canvasTex(256), d = canvasTex(256); Object.assign(sun, { cvs: a.c, tex: a.t, cvsDim: d.c, texDim: d.t, tx: a.t, txDim: d.t }); sun.spr = spriteOf(a.t); }
// (a thing tied to what's open wears ink, whatever it did: tied reads as lit, the rest as grey, and red stays with what's open)
planets.forEach((b) => { b.tx = badgeTex(b.icon, stateOf(b)); b.txDim = badgeTex(b.icon, 'dn'); b.txTie = badgeTex(b.icon, 'mix'); b.spr = spriteOf(b.tx); });
solids.forEach((b) => { b.halo = spriteOf(GLOW); b.shade = flat(SHADE, 1, 1.5); });
posts.forEach((p) => { const a = canvasTex(KW, KH), d = canvasTex(KW, KH); Object.assign(p, { cvs: a.c, tex: a.t, cvsDim: d.c, texDim: d.t }); p.spr = spriteOf(a.t); p.spr.renderOrder = 5; });
// stems, floor marks, the feed's ticks, the small bodies, and the bright heads that ride the rings in on arrival
const mkSeg = (n, order) => { const g = new THREE.BufferGeometry(); g.setAttribute('position', new THREE.BufferAttribute(new Float32Array(Math.max(1, n) * 6), 3)); g.setAttribute('color', new THREE.BufferAttribute(new Float32Array(Math.max(1, n) * 6), 3)); const m = new THREE.LineSegments(g, new THREE.LineBasicMaterial({ vertexColors: true, transparent: true, depthWrite: false })); m.renderOrder = order; scene.add(m); return m; };
const mkPts = (n, size, order, map = DOT) => { const g = new THREE.BufferGeometry(); g.setAttribute('position', new THREE.BufferAttribute(new Float32Array(Math.max(1, n) * 3), 3)); g.setAttribute('color', new THREE.BufferAttribute(new Float32Array(Math.max(1, n) * 3), 3)); const o = new THREE.Points(g, new THREE.PointsMaterial({ size, map, alphaTest: map === DOT ? .02 : .5, vertexColors: true, transparent: true, depthWrite: false })); o.renderOrder = order; scene.add(o); return o; };
const stemUp = mkSeg(solids.length, 2), stemDn = mkSeg(solids.length, 0), postStem = mkSeg(np, 2), ticks = mkSeg(np, 2);
posts.forEach((p, k) => { const r0 = FEED - .05, r1 = FEED + .05, c = Math.cos(p.ang), s = Math.sin(p.ang); ticks.geometry.attributes.position.array.set([c * r0, .003, s * r0, c * r1, .003, s * r1], k * 6); });
const marks = mkPts(solids.length, .024, 3), heads = mkPts(ORB.length + 2, .09, 4);
const bitPts = mkPts(bits.length, .085, 3, HEXDOT);
// a post without a standing cover is a mark: a small dot at the head of its stem, red for its format's top quarter, ink
// when it landed, grey when it fell
const postMarks = mkPts(np, .05, 5); posts.forEach((p, k) => { if (p.card) postMarks.geometry.attributes.position.array.set([0, -500, 0], k * 3); });
// the replay plays the covers, in the order they went up
const CARDP = posts.filter((p) => p.card);
// a thread bows in toward the account and lifts a little off the floor, so the feed's threads sweep round the centre
const threadTo = (out, o, A, B, u) => { const mx = (A[0] + B[0]) * .37, mz = (A[2] + B[2]) * .37, ch = Math.hypot(A[0] - B[0], A[2] - B[2]), my = .015 + .05 * ch, a = (1 - u) * (1 - u), b = 2 * (1 - u) * u, c = u * u; out[o] = A[0] * a + mx * b + B[0] * c; out[o + 1] = A[1] * a + my * b + B[1] * c; out[o + 2] = A[2] * a + mz * b + B[2] * c; };
// the loom (v10): every thread is laid once, cut into short segments that each know how far along their thread they sit
// (aU) and which thread they're on (aT). Threads never move, so nothing is rewritten per frame: each frame writes a few
// numbers per thread into a small table (how strong it is, red or not, where its light is, how far it has drawn in, which
// layer it's in) and the shader lights every pixel from it, so the light glides along at the screen's own rate. One
// geometry for the fine threads; the open thing's own threads are copied into a small one of their own when what's open
// changes (never per frame) and drawn bold over them
const LSEG = 16, NT = Math.max(1, LOOM.length), TSW = Math.min(NT, 1024), TSH = Math.ceil(NT / TSW) * 2;
const TS = new Float32Array(TSW * TSH * 4), tsTex = new THREE.DataTexture(TS, TSW, TSH, THREE.RGBAFormat, THREE.FloatType); tsTex.needsUpdate = true;
LOOM.forEach((l) => { const b = l.b; l.A = [l.p.ring[0], .004, l.p.ring[2]]; l.B = [b.home[0], b.kind === 'bit' ? .004 : .003, b.home[2]]; l.ts = ((l.j % TSW) + Math.floor(l.j / TSW) * 2 * TSW) * 4; });
const segGeo = () => { const g = new LineSegmentsGeometry(), P = new Float32Array(NT * LSEG * 6), U = new Float32Array(NT * LSEG * 2), T = new Float32Array(NT * LSEG); g.setPositions(P); g.setAttribute('aU', new THREE.InstancedBufferAttribute(U, 2)); g.setAttribute('aT', new THREE.InstancedBufferAttribute(T, 1)); return { g, P: g.attributes.instanceStart.data, U: g.attributes.aU, T: g.attributes.aT }; };
const LG = segGeo(), HG = segGeo();
LOOM.forEach((l) => { for (let k = 0; k < LSEG; k++) { const i = l.j * LSEG + k; threadTo(LG.P.array, i * 6, l.A, l.B, k / LSEG); threadTo(LG.P.array, i * 6 + 3, l.A, l.B, (k + 1) / LSEG); LG.U.array[i * 2] = k / LSEG; LG.U.array[i * 2 + 1] = (k + 1) / LSEG; LG.T.array[i] = l.j; } });
HG.g.instanceCount = 0;
// the bold layer takes the open thing's threads, copied from the fine layer's
let hiF = null;
function fillHi(F) { hiF = F; let n = 0; if (F) LOOM.forEach((l) => { if (!F.l1.has(l)) return; const a = l.j * LSEG; HG.P.array.set(LG.P.array.subarray(a * 6, (a + LSEG) * 6), n * LSEG * 6); HG.U.array.set(LG.U.array.subarray(a * 2, (a + LSEG) * 2), n * LSEG * 2); HG.T.array.fill(l.j, n * LSEG, (n + 1) * LSEG); n++; });
  HG.g.instanceCount = n * LSEG; if (n) { HG.P.needsUpdate = true; HG.U.needsUpdate = true; HG.T.needsUpdate = true; } }
const LOOM_U = { uTS: { value: tsTex }, uTW: { value: TSW }, uInk: { value: new THREE.Color() }, uRed: { value: new THREE.Color() }, uBg: { value: new THREE.Color() } };
// the light: full at its head, a soft tail behind it, a soft edge ahead, red where it runs strong along a top-quarter thread
const LOOM_V = `attribute vec2 aU; attribute float aT; uniform sampler2D uTS; uniform float uTW, uHi; varying float vU; varying vec4 vS; varying vec2 vG;
void main() { int ti = int(aT + .5), tw = int(uTW); ivec2 c0 = ivec2(ti - (ti / tw) * tw, (ti / tw) * 2); vS = texelFetch(uTS, c0, 0); vec4 s1 = texelFetch(uTS, c0 + ivec2(0, 1), 0); vG = s1.xy;
  vU = position.y < .5 ? aU.x : aU.y; bool keep = abs(s1.z - uHi) < .5 && aU.x < s1.x && (vS.x > .002 || vS.w > .002);`;
const LOOM_F = `uniform vec3 uInk, uRed, uBg; varying float vU; varying vec4 vS; varying vec2 vG;
void main() {`;
// segments meet without their round caps (overlapping caps would bead a see-through line), and a thread's strength is
// given as the cover it had when it was mixed toward the ground, so threads read as they always have
const LOOM_C = `if (vU > vG.x || abs(vUv.y) > 1.) discard; float dh = vS.z - vU, lit = vS.w * (dh >= 0. ? exp(-dh / .16) : 1. - smoothstep(0., .06, -dh));
  float hot = max(vS.y, vG.y * smoothstep(.3, .65, lit)), it = clamp(vS.x + lit * .8, 0., 1.);
  float g0 = pow(uBg.g, .4545), g1 = pow(uInk.g, .4545), a = clamp((pow(mix(uBg.g, uInk.g, it), .4545) - g0) / (g1 - g0 + (g1 >= g0 ? 1e-4 : -1e-4)), 0., 1.);
  gl_FragColor = vec4(mix(uInk, uRed, hot), alpha * max(a, hot));`;
const fatLoom = (geo, width, hi, order) => { const m = new LineMaterial({ linewidth: width, transparent: true, depthWrite: false, worldUnits: false }); Object.assign(m.uniforms, LOOM_U, { uHi: { value: hi } });
  m.onBeforeCompile = (sh) => { sh.vertexShader = sh.vertexShader.replace('void main() {', LOOM_V).replace('gl_Position = clip;', 'gl_Position = keep ? clip : vec4(0., 0., 2., 1.);'); sh.fragmentShader = sh.fragmentShader.replace('void main() {', LOOM_F).replace('gl_FragColor = vec4( diffuseColor.rgb, alpha );', LOOM_C); };
  const o = new LineSegments2(geo, m); o.frustumCulled = false; o.renderOrder = order; scene.add(o); return { o, m }; };
const loom = fatLoom(LG.g, 1.1, 0, 2), loomHi = fatLoom(HG.g, 2.3, 1, 4);
const loomHeads = mkPts(NT, .065, 4), KDOT = 18, kinDots = mkPts(3 * KDOT, .03, 4);
const arcAt = (A, B, u, lift) => { const mx = (A[0] + B[0]) / 2, my = (A[1] + B[1]) / 2 + lift, mz = (A[2] + B[2]) / 2, a = (1 - u) * (1 - u), b = 2 * (1 - u) * u, c = u * u; return [A[0] * a + mx * b + B[0] * c, A[1] * a + my * b + B[1] * c, A[2] * a + mz * b + B[2] * c]; };

function theme() {
  dark = isDark(); const cs = getComputedStyle(stage), tok = (k, f) => cs.getPropertyValue(k).trim() || f;
  BG.set(tok('--ff-bg', dark ? '#0c0c0f' : '#ffffff')); INK.set(tok('--ff-ink', dark ? '#fafafa' : '#0b0b0f')); RED.set(tok('--ff-red', '#DA2E46')); LOW.copy(BG).lerp(INK, .2);
  renderer.setClearColor(BG); scene.fog = new THREE.FogExp2(BG.getHex(), .1); FLOOR.copy(BG).lerp(INK, dark ? .045 : .028); veil.material.color.copy(FLOOR);
  [...rims, ...orbits, ...SECT_LN, ...SECT_TN, feedRing].forEach((o) => o.material.color.copy(INK)); LOOM_U.uInk.value.copy(INK); LOOM_U.uRed.value.copy(RED); LOOM_U.uBg.value.copy(BG); [coreMark, dial].forEach((o) => o.material.color.copy(RED)); pool.material.color.copy(RED);
  solids.forEach((b) => { b.halo.material.color.copy(RED); b.shade.material.color.set(dark ? '#000000' : '#121317'); });
  Object.values(BADGE).forEach((b) => { drawBadge(b.c, b.icon, b.state); b.t.needsUpdate = true; });
  drawSun(); posts.forEach(drawCard);
}
theme(); const themeWatch = new MutationObserver(theme); themeWatch.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });

// ---------- the camera: one view you can tip from side-on down to straight overhead, and the glide between views
// a narrow card is tall, so it looks down a little more steeply and the system fills its width and more of its height
const POLAR = .98, POLAR_N = .5;
const polar = () => (wide ? POLAR : POLAR_N);
const KEY = `<span><i class="o"></i>Round the edge: ${WHO} posts, each beside what it’s made of</span><span><i class="b"></i>Threads: what each post is made of</span><span><i class="g"></i>Higher and red: landed above ${WHO} usual</span>`;
const off = new THREE.Vector3(), sph = new THREE.Spherical();
const cam = { on: false, t0: 0, dur: 1, from: { tg: new THREE.Vector3(), r: 3, phi: 1, th: 0 }, to: { tg: new THREE.Vector3(), r: 3, phi: null, th: null } };
// the map's own moves (roaming): a drag's hold on the floor, the glide after it, a scroll's zoom still to come; and how far
// back the whole map sits (the further out, the nearer the middle the view is kept)
const grab = { id: -1, on: false, x: 0, y: 0, t: 0, vx: 0, vz: 0, at: new THREE.Vector3() }, drift = { on: false, vx: 0, vz: 0 }, zoom = { by: 0, at: new THREE.Vector3() };
let restD = 4;
// glide the camera to a target, distance and (optionally) tilt and turn, on the carry curve; a drag cancels it
function flyTo({ target = null, dist = null, phi = null, theta = null, dur = 1000 }) {
  drift.on = false; zoom.by = 0;
  off.copy(camera.position).sub(controls.target); sph.setFromVector3(off);
  cam.from.tg.copy(controls.target); cam.from.r = sph.radius; cam.from.phi = sph.phi; cam.from.th = sph.theta;
  cam.to.tg.copy(target || controls.target); cam.to.r = dist ?? sph.radius; cam.to.phi = phi; cam.to.th = theta;
  cam.t0 = performance.now(); cam.dur = reduce ? 1 : dur * TEMPO; cam.on = true;
}
let holdT = false, holdS = false;
function drawNow() { if (dead) return; cancelAnimationFrame(raf); const a = holdT, b = holdS; holdT = holdS = false; frame(performance.now()); holdT = a; holdS = b; }
// a change that lays out a lot goes in one frame; the motion starts on the next painted one, so none of its steps are lost
const afterPaint = (fn) => requestAnimationFrame(() => requestAnimationFrame(() => { if (!dead) fn(); }));
let sizedOnce = false, sizedOnce2 = false, W = 1, H = 1, hudB = 10, wide = false, moved = false, toolsBox = [0, 0, 0, 0], glW = 0, glH = 0;
// the part of the map's canvas you can see (all of it but on a phone's card), in the canvas's own pixels
const vis = { x0: 0, y0: 0, x1: 1, y1: 1 };
// the tallest the screen gets (a phone browser's bars away)
let lvhPx = 0;
const lvh = () => { if (!lvhPx) { const pr = document.createElement('div'); pr.style.cssText = 'position:fixed;left:0;top:0;width:0;height:100lvh;visibility:hidden;pointer-events:none'; document.body.appendChild(pr); lvhPx = pr.offsetHeight || innerHeight; pr.remove(); } return Math.max(lvhPx, innerHeight); }; const view = { x: 0, y: 0 }, viewGoal = { x: 0, y: 0 };
// cards and badges draw a little larger on a phone, where the whole system has to fit its width
const cardK = () => (W < 500 ? 1.3 : 1);
// at rest the view clears the key at the top; on a narrow card it centres on the room above the sheet
// on a phone's full screen the map centres in the room above the reading's sheet, at whatever height it sits (docked, half,
// a glance); set with the sheet's height (setDetent)
let sheetLift = 0;
const restY = () => -Math.round(hudB * .5) - Math.round(fitShift * H / 2) + sheetLift;
// the frame holds the feed's ring and the cards standing on it. Tilted, the near side draws larger than the far one and
// the cards stand up off the floor, so the height gets room for both; the room is the map above the sheet on a phone
const fitCam = new THREE.PerspectiveCamera(), fitTg = new THREE.Vector3(), fitV = new THREE.Vector3();
// the points that have to fit: each post's card at its real height, the ring at its foot, and the account's head
const fitPts = () => { const P = fitPts0(); if (SECT) usedCats.forEach((k) => { const a = WEDGE[k].mid, r = LBL_R + .12; P.push(new THREE.Vector3(Math.cos(a) * r, 0, Math.sin(a) * r)); }); return P; };
const fitPts0 = () => { const P = [], w0 = CW * cardK(); posts.forEach((p) => { const w = p.card ? w0 : .05, h = w * 1.25, r = FEED + w * .5, c = Math.cos(p.ang), s2 = Math.sin(p.ang); P.push(new THREE.Vector3(c * r, p.h + (p.h >= 0 ? h + .03 : -h - .03), s2 * r), new THREE.Vector3(c * r, 0, s2 * r)); }); P.push(new THREE.Vector3(0, SUN_Y + sun.size, 0)); return P; };
// how far back the camera sits for the whole system (the feed's ring and its cards standing up and hanging down) to fit
// the room: found by trying distances against the real projection, so the near side drawing larger is allowed for. The
// room is the map above the sheet on a phone, under the key on a wide card
function fitView(roomIn, theta) {
  const room = roomIn ?? Math.max(.3, (vis.y1 - vis.y0 - hudB - 16) / H), th = theta ?? (off.copy(camera.position).sub(controls.target), sph.setFromVector3(off), sph.theta), P = polar(), mx = wide ? .95 * (vis.x1 - vis.x0) / W : .9, my = room * .96;
  fitCam.fov = camera.fov; fitCam.aspect = camera.aspect; fitCam.updateProjectionMatrix(); fitTg.set(0, MIDY, 0); const PTS = fitPts();
  // the extent the points take on screen at distance d; the view is then shifted so that extent sits in the room's middle
  const span = (d) => { fitV.setFromSphericalCoords(d, P, th).add(fitTg); fitCam.position.copy(fitV); fitCam.lookAt(fitTg); fitCam.updateMatrixWorld(); let x0 = 9, x1 = -9, y0 = 9, y1 = -9, bad = false; PTS.forEach((q0) => { fitV.copy(q0).project(fitCam); if (fitV.z >= 1) bad = true; x0 = Math.min(x0, fitV.x); x1 = Math.max(x1, fitV.x); y0 = Math.min(y0, fitV.y); y1 = Math.max(y1, fitV.y); }); return { ok: !bad && Math.max(-x0, x1) <= mx && (y1 - y0) / 2 <= my, cy: (y0 + y1) / 2 }; };
  let lo = 1.5, hi = wide ? 14 : 44; if (!span(hi).ok) return { d: hi, cy: 0 }; for (let k = 0; k < 22; k++) { const mid = (lo + hi) / 2; if (span(mid).ok) hi = mid; else lo = mid; }
  return { d: hi, cy: span(hi).cy };
}
const fitDist = (roomIn, theta) => fitView(roomIn, theta).d;
// the shift that centres the system's extent in the room, set whenever the view is framed
let fitShift = 0;
// the view aims at the system's middle height, so an account whose things mostly stand high (or sink) sits centred
const MIDY = clamp(.08 + .4 * (planets.reduce((s, p) => s + p.y, 0) / Math.max(1, planets.length)), 0, .3);
function home(snap) { const tg = new THREE.Vector3(0, MIDY, 0), fv = fitView(undefined, snap ? .28 : undefined), d = fv.d; fitShift = fv.cy; restD = d; viewGoal.y = restY(); if (snap) { view.y = viewGoal.y; applyView(true); } if (snap) { sph.set(d, polar(), .28); camera.position.setFromSpherical(sph).add(tg); controls.target.copy(tg); controls.update(); cam.on = false; return; } flyTo({ target: tg, dist: d, phi: polar(), theta: null, dur: 1100 }); }
// it turns round its centre and tips from straight overhead down to eye level
function limits() { if (cam.on) { controls.minPolarAngle = 0; controls.maxPolarAngle = Math.PI; } else { controls.minPolarAngle = .04; controls.maxPolarAngle = 1.5; } }
function applyView(force) { if (!force) { const dx = viewGoal.x - view.x, dy = viewGoal.y - view.y; if (Math.abs(dx) < .3 && Math.abs(dy) < .3) { if (view.x === viewGoal.x && view.y === viewGoal.y) return; view.x = viewGoal.x; view.y = viewGoal.y; } else { view.x += dx * .12; view.y += dy * .12; } }
  if (Math.abs(view.x) < .3 && Math.abs(view.y) < .3) camera.clearViewOffset(); else camera.setViewOffset(W, H, view.x, view.y, W, H); camera.updateProjectionMatrix(); }
function size() { const wasWide = wide; wide = wrap.clientWidth >= 860; const sw = stage.clientWidth, sh = stage.clientHeight;
  if (wide) { W = sw; H = sh; mapEl.style.width = mapEl.style.height = mapEl.style.left = mapEl.style.top = ''; }
  else { W = Math.max(sw, innerWidth); H = Math.max(sh, lvh()); mapEl.style.width = `${W}px`; mapEl.style.height = `${H}px`; mapEl.style.left = `${Math.round((sw - W) / 2)}px`; mapEl.style.top = `${Math.round((sh - H) / 2)}px`; }
  vis.x0 = Math.max(0, Math.round((W - sw) / 2)); vis.y0 = Math.max(0, Math.round((H - sh) / 2)); vis.x1 = vis.x0 + Math.min(sw, W); vis.y1 = vis.y0 + Math.min(sh, H);
  if (wasWide !== wide || !sizedOnce) { sizedOnce = true; applyGestures(); } hudB = hud.offsetTop + hud.offsetHeight + 6; if (W !== glW || H !== glH) { glW = W; glH = H; renderer.setSize(W, H, false); } camera.aspect = W / H; camera.fov = W < 500 ? 46 : 38; [loom, loomHi].forEach((f) => f.m.resolution.set(W, H)); const cut = vis.x0 > .5 || vis.y0 > .5; renderer.setScissorTest(cut); if (cut) renderer.setScissor(vis.x0, H - vis.y1, vis.x1 - vis.x0, vis.y1 - vis.y0); { const fd = fitDist(); restD = fd; controls.maxDistance = Math.max(6, fd * 1.45); } if (!sel) { viewGoal.y = restY(); view.y = viewGoal.y; } applyView(true); toolsBox = [vis.x0 + toolsEl.offsetLeft - 6, vis.y0 + toolsEl.offsetTop - 6, toolsEl.offsetWidth + 12, toolsEl.offsetHeight + 12]; if (wasWide !== wide && sizedOnce2) { trail = []; show(); } sizedOnce2 = true; }

// ---------- selection: opening a post lights its threads and names its things; opening a thing stands its posts out on
// the ring, lights its threads and, fainter, what those posts also hold. What's open is flown to: the system turns till
// it faces you and the camera closes in on it and what it's tied to; closing glides back to the view you had before you
// opened anything
let sel = null, ctx = null, hov = -1, hoverCand = -1, hoverT = 0, lastTouch = performance.now(), openAt = 0, before = null;
const restCentre = (b) => (b.kind === 'post' ? b.ring : [b.home[0], b.y + (b.kind === 'bit' ? 0 : (b.y >= 0 ? 1 : -1) * b.size * .5), b.home[2]]);
// where an open thing sits and what it's tied to: a post and the things in it; a thing and its posts on the ring; a rule
// (or its proof) and the posts and parts it speaks for
const tiesOf = (x) => (x.kind === 'post' ? { own: [x.ring], tied: homesOf(x).map(restCentre) }
  : x.kind === 'law' || x.kind === 'evid' ? { own: [], tied: [...(x.partsB || []).map(restCentre), ...x.ps.map((p) => p.ring)] }
  : { own: [restCentre(x)], tied: x.ps.map((p) => p.ring) });
// the camera turns till what's open faces you (its own place counting three times what it's tied to) and closes in till
// it and its ties fill the room, never further out than the whole system; spread right round, it just turns to face the
// open one. A wedge turns to face you; the account goes home
function frameFocus(x) {
  if (x === sun) { const fv = fitView(); fitShift = fv.cy; viewGoal.y = restY(); flyTo({ target: new THREE.Vector3(0, MIDY, 0), dist: fv.d, phi: polar(), dur: 1000 }); return; }
  if (x.kind === 'wedge') { fitShift = 0; viewGoal.y = restY(); frameWedge(x.k); return; }
  const { own, tied } = tiesOf(x), pts = [...own.map((q0) => [q0, 3]), ...tied.map((q0) => [q0, 1])].filter(([q0]) => Math.hypot(q0[0], q0[2]) > .05);
  if (!pts.length) return;
  let ax = 0, az = 0, cx = 0, cz = 0, wt = 0;
  pts.forEach(([q0, w]) => { const a = Math.atan2(q0[2], q0[0]); ax += Math.cos(a) * w; az += Math.sin(a) * w; cx += q0[0] * w; cz += q0[2] * w; wt += w; });
  const spread = Math.hypot(ax, az) / wt < .3, a = spread && own.length ? Math.atan2(own[0][2], own[0][0]) : Math.atan2(az, ax);
  // the aim sits between the open one and its ties' middle, pulled toward the open one
  const o0 = own[0], tx = o0 && !spread ? o0[0] * .45 + (cx / wt) * .55 : spread ? (o0 ? o0[0] * .5 : 0) : cx / wt, tz = o0 && !spread ? o0[2] * .45 + (cz / wt) * .55 : spread ? (o0 ? o0[2] * .5 : 0) : cz / wt;
  const tg = new THREE.Vector3(tx, MIDY * .6, tz), P = polar(), vf = Math.tan(THREE.MathUtils.degToRad(camera.fov / 2));
  const rad = Math.max(.55, ...pts.map(([q0]) => Math.hypot(q0[0] - tx, q0[2] - tz))) + CW * cardK() * .8 + .12;
  const need = Math.max(rad / (vf * camera.aspect), (rad * Math.cos(P) + YMAX * Math.sin(P) * .6 + .1) / vf), whole = fitDist();
  fitShift = 0; viewGoal.y = restY();
  flyTo({ target: tg, dist: clamp(need, Math.min(whole, 1.6), whole), phi: P, theta: Math.atan2(Math.cos(a), Math.sin(a)), dur: 1100 });
}
function select(x) {
  lastTouch = performance.now();
  if (!x) { const had = !!sel; sel = null; ctx = null; if (had && before) fitShift = before.shift; show(); if (had && before) { off.copy(before.pos).sub(before.tg); sph.setFromVector3(off); flyTo({ target: before.tg, dist: sph.radius, phi: sph.phi, theta: sph.theta, dur: 1000 }); } before = null; return; }
  if (!sel) before = { tg: controls.target.clone(), pos: camera.position.clone(), shift: fitShift };
  // a post opened from a thing's page keeps that thing as the order ‹ › step through
  ctx = x.kind === 'post' ? (sel && sel.kind !== 'post' ? sel : ctx) : null;
  if (x !== sel) openAt = performance.now();
  sel = x; show(); frameFocus(x);
}
// ‹ › and the arrow keys: the next thing in the panel's order, or the next post (of the thing it was opened from)
function step(dir) { sdir = dir < 0 ? -1 : 1; if (!sel) { go(STEPS[dir > 0 ? 0 : STEPS.length - 1]); return; } if (sel.kind === 'law') { select(LAWS[(sel.k + dir + LAWS.length) % LAWS.length]); return; } if (sel.kind === 'evid') { select(EVID[(sel.k + dir + EVID.length) % EVID.length]); return; } if (sel.kind === 'wedge') { select(WK[(sel.j + dir + WK.length) % WK.length]); return; } if (sel.kind === 'post') { const m = ctx ? ctx.ps : posts, k = m.indexOf(sel); select(m[(k + dir + m.length) % m.length]); return; } const k = STEPS.indexOf(sel); select(STEPS[((k < 0 ? 0 : k) + dir + STEPS.length) % STEPS.length]); }

// ---------- words: the reading and each thing's page. A result is said one way everywhere, as in Feed: Top % of its
// format's last 90 days
const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const pl = (n, w, many) => `${n} ${n === 1 ? w : many || w + 's'}`;
const pad2 = (n) => String(n).padStart(2, '0');
const pcls = (p) => (isTop(p) ? 'hot' : feeds(p) ? 'up' : 'dn');
const cover = (p) => (p && THUMBS[p.id] ? `<img src="${THUMBS[p.id]}" alt="" loading="lazy" decoding="async" draggable="false">` : `<i class="ff-cvf">${esc(((p && p.name) || '·').charAt(0))}</i>`);
const ruleChip = (id) => (LAW[id] ? `<button type="button" class="ff-chip" data-fflaw="${id}"><em>The rule behind it</em><span>Rule ${pad2(LAW[id].n)} · ${esc(LAW[id].short)}</span><i aria-hidden="true">→</i></button>` : RULE[id] ? `<button type="button" class="ff-chip" data-ffrule="${id}"><em>The rule behind it</em><span>Rule ${pad2(RULE[id].n)} · ${esc(RULE[id].short)}</span><i aria-hidden="true">↓</i></button>` : '');
const hexBadge = (b, big) => `<span class="ff-hx ${stateOf(b)}${big ? ' big' : ''}">${svg(b.icon)}</span>`;
const avatarHTML = () => (AVATAR ? `<img src="${AVATAR}" alt="" decoding="async" draggable="false">` : cover(sun.cover));
const pill = (b) => `<button type="button" class="pill${b.hot ? ' hot' : ''}" data-ffgo="${b.i}" data-ffh="${b.i}">${svg(b.icon)}${esc(b.name)}</button>`;
const score = (b) => `<span class="ffs-score${b.hot ? ' r' : ''}"><b>${b.feed}<i>/${b.n}</i></b><em>landed</em></span>`;
const typPlace = (b) => Math.max(1, Math.round(b.typ * 100));
// what a thing's posts typically got, in a few words: '3 posts · typically Top 21%'
const numsOf = (b) => `${pl(b.n, 'post')} · ${b.n > 1 ? 'typically ' : ''}Top ${typPlace(b)}%`;
const block = (h, body) => (body ? `<section class="ffs-blk"><h4>${h}</h4>${body}</section>` : '');
// a thing's posts as a strip of covers, best first, each with where it landed
const strip = (ps) => `<div class="ffs-strip">${[...ps].sort((a, b) => pctOf(a) - pctOf(b)).map((p) => `<button type="button" class="ffs-sc ${pcls(p)}" data-ffgo="${p.i}" data-ffh="${p.i}" aria-label="${esc(p.name)}, ${esc(place(p))}"><span class="ffs-sci">${cover(p)}</span><em>Top ${topP(p)}%</em></button>`).join('')}</div>`;
// a thing's posts as a list, best first: what each one is and where it landed, with the account's usual drawn between
// the ones above it and the ones under, the way the rules' split shows it. A post opens its post mortem (the map lights
// it while pointed at)
const postRows = (ps) => {
  const s2 = [...ps].sort((a, b) => pctOf(a) - pctOf(b)), k = s2.findIndex((p) => !feeds(p)), at = k < 0 ? s2.length : k;
  const li = (p) => `<li><button type="button" class="ffs-pr ${pcls(p)}" data-post="${esc(p.id)}" data-ffh="${p.i}" aria-label="${esc(p.name)}, ${esc(place(p))}"><span class="ffs-sci">${cover(p)}</span><span class="ffs-prt"><b>${esc(p.name)}</b><em>${p.date}${dealOf(p) ? `<i class="ffs-dl ${dealOf(p).k}">${dealOf(p).w}</i>` : ''}</em></span><span class="ffs-prp">Top ${topP(p)}%</span></button></li>`;
  return `<ol class="ffs-prs">${s2.slice(0, at).map(li).join('')}<li class="ffs-us" aria-hidden="true"><span>${Who} usual</span></li>${s2.slice(at).map(li).join('')}</ol>`;
};
const navHTML = `<span class="ffs-nv"><button type="button" data-ffstep="-1" aria-label="Previous">‹</button><button type="button" data-ffstep="1" aria-label="Next">›</button></span>`;
// back names where it goes: the page you came from, or Everything (the list)
const titleOf = (x) => (!x ? (wide ? 'Everything' : 'The reading') :x.kind === 'law' ? `Rule ${pad2(x.n)}` : x === sun ? `@${D.handle}` : x.name);
const backTo = (p) => (p && p.ext ? p.ext.title : titleOf(p ? p.x : null));
const backHTML = () => `<div class="ffs-bar"><button type="button" class="ffs-back" data-ffback><span aria-hidden="true">←</span><span>${esc(backTo(trail[trail.length - 1]))}</span></button>${navHTML}</div>`;
// a thing's page: its read (what it is, what the account is going for with it, what it got), the law behind it, and its
// posts, each with what it is and where it landed. A post's: where it landed, what happens in it, what it's part of, and
// the way to its post mortem
// the proofs read the same posts the map does
const postById = Object.fromEntries(posts.map((p) => [p.id, p]));
const PCTX = { cover: (id) => cover(postById[id]), label: (id) => (postById[id] || {}).name || id, top: (id) => (postById[id] ? topP(postById[id]) : 50), who: WHO, usual: `${Who} usual`, best: `${Who} best`, worst: `${Who} worst` };
// ---------- the reading as things you can see (v7, Oct 22): each rule a card (its number, its short name, its headline
// number, a small picture of where its posts landed, its record); each rule's proof its own card (followers day by day,
// what each paid post got, the same idea twice, the order posts went up, the teasers and their episodes) with its chart.
// A card lights its posts on the map when pointed at (or swiped to, on a phone), and opens its page
const PROOF_T = { followers: 'Followers, day by day', money: 'What each paid post got', twin: 'The same idea, two ways', arc: 'In the order they went up', pairs: 'Each teaser and its episode', share: 'Where the likes went' };
const proofIds = (pr) => (pr.kind === 'followers' ? pr.pins.map((x) => x.id) : pr.kind === 'money' || pr.kind === 'twin' ? pr.rows.map((r) => r.id) : pr.kind === 'arc' ? pr.steps.map((x) => x.id) : pr.kind === 'pairs' ? pr.pairs.flatMap((x) => [x.a, x.b]) : pr.kind === 'share' ? pr.pins : []);
const EVID = LAWS.filter((L) => L.proof && PROOF_T[L.proof.kind]).map((L, k) => ({ kind: 'evid', k, i: -100 - k, id: `E${L.id}`, L, proof: L.proof, name: L.proofHead || PROOF_T[L.proof.kind], ps: [...new Set(proofIds(L.proof))].map((id) => postById[id]).filter(Boolean), partsB: [] }));
const EVB = Object.fromEntries(EVID.map((E) => [E.id, E]));
// a wedge (Cast, Formats, ...) as something you can open: all its things on a page of their own. Its card leads the
// reading (v8, Oct 22: "the formats, characters etc are way too buried")
const WK = usedCats.map((k, j) => { const things = [...inCat(k, planets), ...inCat(k, bits)]; return { kind: 'wedge', k, j, i: -200 - k, id: `W${k}`, name: CATS[k].name, things, ps: [...new Set(things.flatMap((b) => b.ps))] }; });
const WKB = Object.fromEntries(WK.map((w) => [w.id, w]));
const itemOf = (id) => LAW[id] || EVB[id] || WKB[id] || null;
const med2 = (a) => { const v = [...a].sort((x, y) => x - y), m = v.length >> 1; return v.length ? (v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2) : null; };
const at99 = (t) => ((Math.min(100, Math.max(1, t)) - 1) / 99 * 100).toFixed(1);
// where a rule's posts usually land, on one track from best to worst: its posts in red, the others in ink, the usual a tick
function miniSplit(L) {
  const S = L.split, hi = S.lands.length ? med2(S.lands.map((x) => x.top)) : null, lo = S.sinks.length ? med2(S.sinks.map((x) => x.top)) : null;
  return `<span class="mk-sp" aria-hidden="true"><i class="tr"></i><i class="us"></i>${hi != null && lo != null ? `<i class="gp" style="left:${at99(Math.min(hi, lo))}%;width:${Math.abs(at99(lo) - at99(hi)).toFixed(1)}%"></i>` : ''}${hi != null ? `<i class="dt hi" style="left:${at99(hi)}%"><b>Top ${Math.round(hi)}%</b></i>` : ''}${lo != null ? `<i class="dt lo" style="left:${at99(lo)}%"><b>Top ${Math.round(lo)}%</b></i>` : ''}<em class="e0">best</em><em class="e1">worst</em></span>`;
}
// followers day by day: each day's gain up, each day's loss down, the biggest day in red
function miniFollowers(pr) {
  const days = pr.days.filter((d) => !d.gap && d.v != null), mx = Math.max(1, ...days.map((d) => Math.abs(d.v))), top = days.reduce((a, d) => (!a || d.v > a.v ? d : a), null);
  return `<span class="mk-fo" aria-hidden="true">${pr.days.map((d) => (d.gap || d.v == null ? '<i class="gap"></i>' : `<i class="${d === top ? 'top' : d.v < 0 ? 'neg' : ''}" style="--h:${(Math.abs(d.v) / mx).toFixed(3)}"></i>`)).join('')}</span>`;
}
// likes, short: 867,907 → 868K
const kn = (v) => (v >= 1e6 ? `${(v / 1e6).toFixed(v >= 1e7 ? 0 : 1)}M` : v >= 1e4 ? `${Math.round(v / 1e3)}K` : v >= 1e3 ? `${(v / 1e3).toFixed(1)}K` : `${v}`);
// each paid post as a share of the usual, the usual a line across (the proof card prints each share)
function miniMoney(pr) { const mx = Math.max(100, ...pr.rows.map((r) => r.pct)); return `<span class="mk-mo" aria-hidden="true"><i class="us" style="--y:${(100 / mx).toFixed(3)}"><em>${WHO} usual</em></i>${pr.rows.map((r) => `<i class="${r.pct >= 100 ? 'ok' : ''}" style="--h:${Math.max(.03, r.pct / mx).toFixed(3)}"><b>${r.pct}%</b></i>`).join('')}</span>`; }
// the same idea twice: two bars of likes
function miniTwin(pr) { const mx = Math.max(...pr.rows.map((r) => r.likes)); return `<span class="mk-tw" aria-hidden="true">${pr.rows.map((r, k) => `<span><em>${esc(r.side)}</em><span><i class="${k === 0 ? 'hi' : ''}" style="--w:${Math.max(.02, r.likes / mx).toFixed(3)}"></i><b>${kn(r.likes)}</b></span></span>`).join('')}</span>`; }
// in the order they went up: each post at the height it landed
function miniArc(pr) { return `<span class="mk-ar" aria-hidden="true">${pr.steps.map((x) => `<i class="${x.top <= 25 ? 'hot' : ''}" style="--y:${((x.top - 1) / 99).toFixed(3)}"></i>`).join('')}</span>`; }
// each teaser beside its episode
function miniPairs(pr) { const mx = Math.max(...pr.pairs.flatMap((x) => [x.aLikes, x.bLikes])); return `<span class="mk-pa" aria-hidden="true">${pr.pairs.map((x) => `<span class="${x.off ? 'off' : ''}"><i style="--h:${(x.aLikes / mx).toFixed(3)}"></i><i class="ep" style="--h:${(x.bLikes / mx).toFixed(3)}"></i></span>`).join('')}</span>`; }
const miniProof = (pr) => ({ followers: miniFollowers, money: miniMoney, twin: miniTwin, arc: miniArc, pairs: miniPairs }[pr.kind] || (() => ''))(pr);
const miniOf = (L) => (L.split ? miniSplit(L) : L.proof ? miniProof(L.proof) : '');
// a rule's record: since Read v7 it counts only the posts after the rule was written ('fit' is what it was written from),
// so a rule no post has tested yet says so; older data counts every post it names
const recW = (L) => (!L.record.fit ? `Held ${L.record.held} of ${L.record.calls}` : L.record.calls ? `Held ${L.record.held} of ${L.record.calls} since`
  : (L.record.since || 0) > (L.life.find((e) => e.move === 'born') || { run: 0 }).run ? `Not tested since run ${L.record.since}` : 'New, not tested yet');
// a rule card, the same parts in the same order every time: its number and record, the rule as a sentence, the two
// sides it splits the posts into, its number, its picture
const sidesHTML = (L) => (L.sides && (L.sides.lands || L.sides.sinks) ? `<span class="rk-sd">${L.sides.lands ? `<span class="up"><i aria-hidden="true">▲</i>${esc(L.sides.lands)}</span>` : ''}${L.sides.sinks ? `<span class="dn"><i aria-hidden="true">▼</i>${esc(L.sides.sinks)}</span>` : ''}</span>` : '');
// the record in full: "Held 3 of 5 since run 2"
const recFull = (L) => { const w = recW(L); return / since$/.test(w) ? `${w} run ${L.record.since || (L.life.find((e) => e.move === 'born') || { run: 1 }).run}` : w; };
const ruleCard = (L) => `<button type="button" class="rk" data-fflaw="${L.id}" data-ffhx="${L.id}"><span class="rk-k"><b>Rule ${pad2(L.n)}</b> · ${esc(recFull(L))}</span><b class="rk-t">${esc(L.short || L.law)}</b>${sidesHTML(L)}<span class="rk-n"><b>${esc(L.stat.v)}</b><em>${esc(L.stat.u)}</em></span><span class="rk-v">${miniOf(L)}</span></button>`;
function evidPage(E) {
  const L = E.L;
  return { head: `<div class="ffs-lw"><em class="r">Rule ${pad2(L.n)} · the proof</em><h2>${esc(E.name)}</h2>${chips([[`${L.stat.v} ${L.stat.u}`, 'r'], recW(L)])}</div>`,
    body: `${proofHTML(E.proof, PCTX)}<div class="ff-cr">${ruleChip(L.id)}</div>${E.ps.length ? block(E.ps.length > 1 ? `Its ${E.ps.length} posts` : 'Its post', postRows(E.ps)) : ''}` };
}
const panim = (el, kf, o = {}) => (reduce || !el ? null : el.animate(kf, tm({ duration: o.dur ?? 460, delay: o.delay ?? 0, easing: o.ease ?? CARRY, fill: 'backwards' })));
// a page's proofs play as it arrives; a folded one waits until it's opened
function playProofs(root) { requestAnimationFrame(() => { if (dead) return; root.querySelectorAll('.pf').forEach((el) => { if (el.closest('[hidden]')) return; layProof(el); playProof(el, reduce ? null : panim); }); }); }
const MOVEW = { born: 'New', sharpened: 'Sharpened', narrowed: 'Narrowed', held: 'Held', strains: 'Strained', breaks: 'Broke', dropped: 'Dropped', backs: 'Backed', tweaked: 'Narrowed' };
const lawState = (L) => { const e = L.life[L.life.length - 1]; return e ? `${MOVEW[e.move] || e.move} in run ${e.run}` : ''; };
// how the law came to be: what each run did to it, each opening that run's read below
const lifeHTML = (L) => `<ol class="ffs-life">${L.life.map((e) => `<li><button type="button" data-ffrun="${e.run}"><b>Run ${e.run}</b><em class="${e.move === 'dropped' || e.move === 'breaks' || e.move === 'strains' ? 'dn' : e.move === 'held' ? '' : 'up'}">${esc(MOVEW[e.move] || e.move)}</em><span>${esc(e.head || e.line)}</span><i aria-hidden="true">→</i></button></li>`).join('')}</ol>`;
// what to do with a rule, for whoever is reading (Read v7): the account itself, a rival, someone who'd work with it, and
// someone learning from it for their own posts
const takesHTML = (t) => `<div class="ffs-do ffs-takes">${[['own', `If you’re ${OBJ === 'it' ? 'the brand' : OBJ === 'them' ? 'them' : OBJ}`], ['rival', `Up against ${OBJ}`], ['partner', `Working with ${OBJ}`], ['learn', 'For your own posts']]
  .filter(([k]) => t[k]).map(([k, w]) => `<p><em>${w}</em><span>${linkText(t[k])}</span></p>`).join('')}</div>`;
function lawPage(L) {
  // where its posts landed, the way the rules always showed it (each side on a ledge at its height, feeding above the
  // usual, fading under it); then why, what to do, the proof folded until asked for, how I got here, where it shows up
  const born = L.life.find((e) => e.move === 'born');
  // a law about followers or teasers has no split: its proof is the picture, open; under a split it waits, folded
  const lead = L.split ? proofHTML(L.split, PCTX) : L.proof ? proofHTML(L.proof, PCTX) : '';
  const proof = L.split && L.proof ? `<section class="ffs-blk ffs-pf"><button type="button" class="ffs-pfb" data-ffproof aria-expanded="false"><span><em>The proof</em>${esc(L.proofHead || 'The numbers behind it')}</span><i aria-hidden="true">+</i></button><div class="ffs-pfx" hidden>${proofHTML(L.proof, PCTX)}</div></section>` : '';
  return { head: `<div class="ffs-lw"><em class="r">Rule ${pad2(L.n)}${lawState(L) ? ` · ${esc(lawState(L))}` : ''}</em><h2>${esc(L.short || L.law)}</h2>${chips([L.record.fit ? `${recW(L)}${L.record.calls ? ` run ${L.record.since || (born ? born.run : 1)}` : ''}` : `Held ${L.record.held} of ${L.record.calls}${born ? ` · since run ${born.run}` : ''}`, L.stat ? [`${L.stat.v} ${L.stat.u}`, 'r'] : null])}</div>`,
    body: `${lead}${block('Why it works', `<p>${linkText(L.why)}</p>`)}${L.takes ? takesHTML(L.takes) : L.take ? `<p class="ffs-do"><em>Do this</em><span>${linkText(L.take)}</span></p>` : ''}${L.limits ? `<p class="ffs-lim">${linkText(L.limits)}</p>` : ''}`
      + `${proof}${block('How I got here', lifeHTML(L))}${L.partsB.length ? block('Where it shows up', `<div class="pills">${L.partsB.map(pill).join('')}</div>`) : ''}<button type="button" class="ff-pm lt" data-fflight="${L.id}"><span>Light its posts on the wall</span><i aria-hidden="true">↓</i></button>` };
}
// a page's head is its glance: what it is, its title and a few numbers you take in at once (all a peeking sheet shows)
const n0 = (v) => Math.round(v).toLocaleString('en-US');
const chips = (list) => `<span class="ffs-chips">${list.filter(Boolean).map((c) => (Array.isArray(c) ? `<span class="${c[1] || ''}">${esc(c[0])}</span>` : `<span>${esc(c)}</span>`)).join('')}</span>`;
// v10: a thing explained. Its numbers said plainly up front (how many posts, how many beat the usual, how far they
// typically went); then why the account uses it, how it lands and why, what to take from it, and each post it's in with
// one line on how it was used there and how that post did
const xPlain = (x) => (x >= 10 ? `${Math.round(x)}×` : x >= 1.2 ? `${x.toFixed(1)}×` : x >= 0.8 ? 'about' : `${Math.round(x * 100)}% of`);
const eachRows = (ps, each) => `<ol class="ffs-prs ffs-each">${[...ps].sort((a, b) => a.i - b.i).map((p) => `<li><button type="button" class="ffs-pr ${pcls(p)}" data-post="${esc(p.id)}" data-ffh="${p.i}" aria-label="${esc(p.name)}"><span class="ffs-sci">${cover(p)}</span><span class="ffs-prt"><b>${esc(p.name)}</b><em>${p.date}${dealOf(p) ? `<i class="ffs-dl ${dealOf(p).k}">${dealOf(p).w}</i>` : ''}</em>${each[String(p.i)] ? `<span class="ffs-ln">${linkText(each[String(p.i)])}</span>` : ''}</span></button></li>`).join('')}</ol>`;
// a thing's kicker: its section and then, for Patterns, the shelf it sits on (Kind or Repeat); the older files say its orbit
const tagOf = (x) => (SECT ? `${CATS[x.cat].name}${x.cat === 0 && x.shelf ? ` · ${x.shelf === 'repeat' ? 'Repeat' : 'Kind'}` : ''}` : `${CATS[x.cat].name}${x.cat === 1 ? '' : ` · ${ORBIT[x.orbit]}`}`);
function dossPage(x, so) {
  const d = x.doss, h = x.hits || [x.feed, x.n];
  const usual = x.typx == null ? '' : x.typx >= 0.8 && x.typx < 1.2 ? `typically about ${WHO} usual` : `typically ${xPlain(x.typx)} ${WHO} usual`;
  return { head: `<div class="ffs-dh">${hexBadge(x, true)}<div><em class="${x.hot ? 'r' : ''}">${esc(tagOf(x))}${x.isNew ? ' · new' : ''}</em><h2>${esc(x.name)}</h2>${chips([`${pl(x.n, 'post')} · ${dspan(x.ps)}`, [`${h[0]} of ${h[1]} beat ${WHO} usual`, h[0] * 2 > h[1] ? 'r' : ''], usual])}</div></div>`,
    body: `${x.what ? `<p class="ffs-what">${linkText(x.what)}</p>` : ''}${block(`Why ${SUBJ} use${s3} it`, d.going ? `<p>${linkText(d.going)}</p>` : '')}${block('How it lands, and why', d.landing ? `<p>${linkText(d.landing)}</p>` : '')}${block('What to take from it', d.take ? `<p class="ffs-tk">${linkText(d.take)}</p>` : '')}${so}${block(x.n > 1 ? `Each time, ${x.n} posts` : 'Its post', eachRows(x.ps, d.each || {}))}` };
}
// the Profile's list of sections: each its line, its posts and how many beat the usual; each opens its wedge
function secList() {
  const rows = usedCats.map((k) => { const c = CATS[k], w = WKB[`W${k}`], sd = sideOf(w.ps), h = c.hits || [sd.feed, sd.n], n = c.n ?? sd.n;
    return `<button type="button" class="ffs-sec" data-ffitem="W${k}" data-ffh="${w.i}"><b>${esc(c.name)}</b>${c.line ? `<span class="ffs-sl">${linkText(c.line)}</span>` : ''}<em>${pl(n, 'post')} · ${h[0]} of ${h[1]} beat ${WHO} usual</em></button>`; });
  return rows.length ? `<div class="ffs-secs">${rows.join('')}</div>` : '';
}
function pageOf(x) {
  if (x.kind === 'law') return lawPage(x);
  if (x.kind === 'evid') return evidPage(x);
  if (x.kind === 'wedge') return wedgePage(x);
  if (x.kind === 'post') {
    const homes = homesOf(x);
    return { head: `<div class="ffs-dh post"><span class="ff-cv ${pcls(x)} big">${cover(x)}</span><div><em class="${isTop(x) ? 'r' : ''}">${feeds(x) ? 'Feeding' : 'Fading'} · ${place(x)}</em><h2>${esc(x.name)}</h2>${chips([`${x.date}${x.secs ? ` · ${x.secs} sec` : ''}`, x.likes != null ? [`${n0(x.likes)} likes`, isTop(x) ? 'r' : ''] : null, dealOf(x) ? [dealOf(x).w, `dl ${dealOf(x).k}`] : null])}</div></div>`,
      body: `${x.what ? `<p class="ffs-what">${esc(x.what)}</p>` : x.label && x.label !== x.name ? `<p class="ffs-what">${esc(x.label)}</p>` : ''}${dealOf(x) ? `<div class="ffs-deal"><span class="dl ${dealOf(x).k}">${dealOf(x).w}</span><b>${esc(dealWith(x))}</b>${x.coNote ? `<p>${esc(x.coNote)}</p>` : ''}</div>` : ''}${block(`In ${WHO} system`, homes.length ? `<div class="pills">${homes.map(pill).join('')}</div>` : '')}<button type="button" class="ff-pm" data-post="${esc(x.id)}"><span class="ff-pmc">${cover(x)}</span><span>Open its post mortem</span><i aria-hidden="true">↓</i></button>` };
  }
  if (x === sun) {
    const all = sideOf(posts);
    const mix = CATS.map((c, k) => ({ c, n: planets.filter((p) => p.cat === k).length + bits.filter((b) => b.cat === k).length })).filter((o) => o.n);
    return { head: `<div class="ffs-dh"><span class="ffs-av big">${avatarHTML()}</span><div>${SECT ? '' : `<em class="r">@${esc(D.handle)} · right now</em>`}<h2>${esc(sun.name)}</h2>${chips([`${np} posts in ${days} days`, [`${all.feed} above ${WHO} usual`, all.share >= .5 ? 'r' : ''], `${Math.round(np / Math.max(7, days) * 7)} a week`])}</div></div>`,
      // on a phone the account's numbers, the map's key and the dropped rules live here, off the list (v10)
      body: `<p class="ffs-what">${linkText(sun.line)}</p>${sun.doss ? `${block(`What ${SUBJ} ${SUBJ === 'they' ? 'are' : 'is'} doing, and why`, `<p>${linkText(sun.doss.going)}</p>`)}${block('What lands, and why', `<p>${linkText(sun.doss.landing)}</p>`)}${block('What to take from it', `<p class="ffs-tk">${linkText(sun.doss.take)}</p>`)}` : ''}${wide ? '' : `${statsHTML()}<p class="ff-lg sys ff-plg" aria-hidden="true">${KEY}</p>`}${block('Its posts', `<div class="ffs-did"><div><b>${np}</b><span>posts · ${dspan(posts)}</span></div><div><b class="${all.share >= .5 ? 'r' : ''}">${all.feed}</b><span>landed above ${WHO} usual post</span></div></div>`)}${SECT ? block('The sections', secList()) : block('What it’s made of', `<div class="ffs-mix">${mix.map((o) => `<span><b>${o.n}</b>${esc(o.c.name)}</span>`).join('')}</div>`)}${block('The posts that show it best', strip(sun.ps))}${!wide && (D.dropped || []).length ? block('Rules let go', goneHTML()) : ''}` };
  }
  // where a law speaks for it, the law is its takeaway: the page points there instead of saying it again
  const so = x.rule ? `<div class="ff-cr">${ruleChip(x.rule)}</div>` : x.how ? block(D.howLabel || 'How it shows up', `<p>${linkText(x.how)}</p>`) : '';
  if (x.doss) return dossPage(x, so);
  return { head: `<div class="ffs-dh">${hexBadge(x, true)}<div><em class="${x.hot ? 'r' : ''}">${esc(tagOf(x))}${x.isNew ? ' · new' : ''}</em><h2>${esc(x.name)}</h2>${chips([`${pl(x.n, 'post')} · ${dspan(x.ps)}`, [`${x.feed} of ${x.n} landed`, x.hot ? 'r' : ''], `${x.n > 1 ? 'typically ' : ''}Top ${typPlace(x)}%`])}</div></div>`,
    body: `${x.what ? `<p class="ffs-what">${linkText(x.what)}</p>` : ''}${so}${block(x.n > 1 ? `Its ${x.n} posts` : 'Its post', postRows(x.ps))}` };
}

// ---------- the reading: the account and its numbers, what it feeds and fades on, then one row per wedge that opens in
// place to its things with their numbers; opening a thing turns the reading to its page. Beside the system on a wide card
// (it never has to scroll far: the wedges stay shut till asked); right under the system on a narrow one, in the page
// itself, so a swipe anywhere on the card scrolls the page and the map and the reading are never fighting for it
const readEl = q('reading'), keyEl = q('key');
const HEAD = (D.heads || {}).map || '';
const dayOf = (s) => { const [d, m] = s.split(' '); return Date.UTC(2026, 'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split(' ').indexOf(m), +d) / 864e5; };
const days = Math.max(1, dayOf(posts[np - 1].date) - dayOf(posts[0].date) + 1);
const allSide = sideOf(posts), paidN = posts.filter((p) => dealOf(p)).length, coN = posts.filter((p) => p.co && p.co.length).length;
const splitHead = (() => { const m = /^\s*feeds on (.+?);\s*fades on (.+?)\.?\s*$/i.exec(HEAD); return m ? [m[1], m[2]] : null; })();
const statsHTML = () => `<div class="ffs-stats"><div><b>${np}</b><span>posts in ${days} days</span></div><div><b class="r">${allSide.feed}</b><span>landed above ${WHO} usual</span></div><div><b>${Math.round(np / Math.max(7, days) * 7)}</b><span>a week</span></div><div><b>${paidN}</b><span>${esc(D.paidWord || 'paid')}${coN ? `, ${coN} as collabs` : ''}</span></div></div>`;
function topHTML() {
  // on a phone the sheet's dock line already says who this is; the head goes straight to what it's doing
  const me = wide ? `<div class="ffs-me"><button type="button" class="ffs-av" data-ffgo="${sun.i}" data-ffh="${sun.i}" aria-label="The account’s page">${avatarHTML()}</button><span class="ffs-id"><em>@${esc(D.handle)} · right now</em><b>${esc(sun.name)}</b><span>${linkText(sun.line)}</span></span></div>` : `<p class="ffs-what ffs-ln">${linkText(sun.line)}</p>`;
  return `<header class="ffs-top">${me}`
    + statsHTML()
    // with laws, the laws say what it feeds and fades on (and why); without them the reader's one-line split stays
    + (LAWS.length ? '' : splitHead ? `<div class="ffs-split"><div class="up"><em>Feeds on</em><p>${esc(splitHead[0])}</p></div><div class="dn"><em>Fades on</em><p>${esc(splitHead[1])}</p></div></div>` : HEAD ? `<p class="ffs-head">${esc(HEAD)}</p>` : '') + `<p class="ff-lg sys ff-plg" aria-hidden="true">${KEY}</p></header>`;
}
// a wedge as a card: its name and question, its top things (badge, name, how many of their posts landed) and the way to
// the rest. In the reading beside the map it opens in place; in a phone's sheet it opens its page
const wkRow = (b) => `<button type="button" class="wk-r" data-ffgo="${b.i}" data-ffh="${b.i}">${hexBadge(b)}<b>${esc(b.name)}</b>${score(b)}</button>`;
const wkTop = (k, n) => { const ps = inCat(k, planets); return (ps.length ? ps : inCat(k, bits)).slice(0, n); };
const wkAll = (k) => inCat(k, planets).length + inCat(k, bits).length;
const WK_N = 3;
function wedgeCard(k, paged) {
  const c = CATS[k], top = wkTop(k, WK_N), n = wkAll(k), more = n - top.length, act = paged ? `data-ffitem="W${k}"` : `data-ffwedge="${k}"`, open = !paged && wedgeOpen === k;
  return `<section class="rk wk${open ? ' on' : ''}" data-ffcat="${k}" data-ffhx="W${k}"><button type="button" class="wk-h" ${act}><span class="rk-k"><b>${esc(c.name)}</b> · ${n}</span><b class="rk-t">${esc(c.q)}</b></button>`
    + `<div class="wk-rows">${open ? wedgeRows(k) : top.map(wkRow).join('')}</div>${more > 0 ? `<button type="button" class="wk-more" ${act}>${open ? 'Fewer' : pl(more, 'more thing', 'more things')}</button>` : '<span class="wk-more none"></span>'}</section>`;
}
const madeOfHTML = () => `<section class="ffs-laws ffs-made"><div class="ffs-lh"><b>What ${WHO} posts are made of</b><span>Lit on the map</span></div><div class="rks wks">${usedCats.map((k) => wedgeCard(k, !wide)).join('')}</div></section>`;
function wedgePage(w) {
  const c = CATS[w.k], ps = inCat(w.k, planets), its = inCat(w.k, bits), sd = sideOf(w.ps);
  return { head: `<div class="ffs-lw"><em class="r">${esc(c.name)}</em><h2>${esc(c.q)}</h2>${c.line ? `<p class="ffs-what">${linkText(c.line)}</p>` : ''}${chips([pl(w.things.length, 'thing'), pl(w.ps.length, 'post'), [`${sd.feed} landed above ${WHO} usual`, sd.share >= .5 ? 'r' : '']])}</div>`,
    body: `${ps.length ? `<div class="wk-all">${ps.map(row).join('')}</div>` : ''}${its.length ? `<div class="ffs-once"><span>${onceW(ps, its)} · ${its.length}</span><div class="pills">${its.map(pill).join('')}</div></div>` : ''}` };
}
// a thing in a wedge's list: its badge, its name, its numbers, how many of its posts landed
const row = (b) => `<button type="button" class="ffs-row" data-ffgo="${b.i}" data-ffh="${b.i}">${hexBadge(b)}<span class="ffs-rt"><b>${esc(b.name)}${b.isNew ? '<i class="nw">New</i>' : ''}</b><em>${numsOf(b)}</em></span>${score(b)}</button>`;
const SHOW = 6;
// the small things under a wedge's planets: in one post each, or (on a long feed) smaller than its planets
const onceW = (ps, its) => (its.every((b) => b.n === 1) ? (ps.length ? 'Also, in one post' : 'In one post') : ps.length ? 'Also, smaller' : 'Smaller');
let wedgeOpen = null, wedgeAll = false, wedgeBefore = null;
function wedgeRows(k) {
  const ps = inCat(k, planets), its = inCat(k, bits), cap = wedgeAll ? ps.length : SHOW;
  const once = its.length ? `<div class="ffs-once"><span>${onceW(ps, its)} · ${its.length}</span><div class="pills">${its.slice(0, 8).map(pill).join('')}${its.length > 8 ? `<button type="button" class="pill more" data-ffmore="${k}">+${its.length - 8} more</button>` : ''}</div></div>` : '';
  return ps.slice(0, cap).map(row).join('') + (ps.length > cap ? `<button type="button" class="ffs-more" data-ffall="${k}">${ps.length - cap} more</button>` : '') + once;
}
// the laws the reader let go, each with the run that dropped it
const goneHTML = () => (D.dropped || []).map((g) => { const e = g.life[g.life.length - 1] || {}; return `<p class="ffs-ld"><em>${e.run ? `Dropped in run ${e.run}` : 'Dropped'}</em><s>${esc(g.law)}</s><span>${linkText(g.line || e.line)}</span></p>`; }).join('');
// the laws, first thing under the account: each its number, the law, its record and its receipt
function lawsHTML() {
  if (!LAWS.length) return '';
  return `<section class="ffs-laws"><div class="ffs-lh"><b>What ${WHO} crowd rewards</b><span>Tested every 10 posts</span></div><div class="rks">${LAWS.map(ruleCard).join('')}</div>${goneHTML()}</section>`;
}
const countsOf = () => [LAWS.length ? pl(LAWS.length, 'rule') : '', pl(planets.length + bits.length, 'thing'), pl(np, 'post')].filter(Boolean).join(' · ');
// a phone's sheet, docked (v9, Oct 22): one line of who this is over the foot of the map. Its picture opens the account's
// page; the rest raises the reading to half and back
const dockHTML = () => `<div class="ffs-dock"><button type="button" class="ffs-av" data-ffgo="${sun.i}" aria-label="The account’s page">${avatarHTML()}</button><button type="button" class="ffs-dkt" data-ffdock aria-label="The reading of @${esc(D.handle)}"><em>@${esc(D.handle)}</em><b>${esc(sun.name)}</b><i>${countsOf()}</i><span class="ffs-dku" aria-hidden="true">↑</span></button></div>`;
// a phone's list is two shelves under the dock line (v10): what the posts are made of, then what the crowd rewards, each a
// row of cards that snaps card by card with the next one peeking. It fits the sheet at half height, so nothing scrolls
// up and down; the account's paragraph, numbers and key live on its own page (the dock's picture), and each rule's page
// leads with its proof
const shelfHTML = (key, head, sub, cards) => `<section class="ffs-shf" data-ffshelf="${key}"><div class="ffs-lh"><b>${head}</b><span>${sub}</span></div><div class="ffs-shr" data-ffrow="${key}">${cards}</div></section>`;
const shelvesHTML = () => `<div class="ffs ffs-sh">${dockHTML()}${shelfHTML('made', `What ${WHO} posts are made of`, pl(usedCats.length, 'part'), usedCats.map((k) => wedgeCard(k, true)).join(''))}${LAWS.length ? shelfHTML('laws', `What ${WHO} crowd rewards`, 'Tested every 10 posts', LAWS.map(ruleCard).join('')) : ''}</div>`;
// beside the map, only what the map can show: the account, what its posts are made of (the nodes) and the rules (each
// lights its posts and things). A rule's proof is its page's lead, not a chart of its own down the list (Oct 22: "those
// artifacts below don't have a dedicated node… it doesn't belong there")
// ---------- v12 sections: the panel shows one section at a time under a switcher in the map's order (Profile, then each
// section). A tab picks its sector on the map and a sector picks its tab; the Profile tab is the account's page. A row
// opens that thing's page, and back lands on the tab again
let secTab = -1;
const tabsHTML = () => `<div class="ffs-tabs" role="tablist" aria-label="Sections">${[-1, ...usedCats].map((k) => `<button type="button" role="tab" class="ffs-tab${k === secTab ? ' on' : ''}" aria-selected="${k === secTab}" data-fftab="${k}"${k >= 0 ? ` data-ffhx="W${k}" data-ffcat="${k}"` : ''}>${k < 0 ? 'Profile' : esc(CATS[k].name)}</button>`).join('')}</div>`;
const sxRow = (b) => { const h = b.hits || [b.feed, b.n]; return `<button type="button" class="ffs-row" data-ffgo="${b.i}" data-ffh="${b.i}">${hexBadge(b)}<span class="ffs-rt"><b>${esc(b.name)}${b.isNew ? '<i class="nw">New</i>' : ''}</b><em>${pl(b.n, 'post')} · ${h[0]} beat ${WHO} usual</em></span></button>`; };
// every post sits in one kind, so the kinds' shares fill the bar
const kindBar = (kinds) => `<div class="ffs-kb" role="img" aria-label="Share of posts by kind">${kinds.map((b, j) => { const h = b.hits || [b.feed, b.n], hot = h[0] * 2 > h[1]; return `<i class="${hot ? 'hot' : ''}" style="flex:${b.n} 1 0;${hot ? '' : `opacity:${(.8 - .5 * j / Math.max(1, kinds.length - 1)).toFixed(2)}`}" title="${esc(b.name)} · ${b.n}"></i>`; }).join('')}</div>`;
function sxBody(k) {
  if (k < 0) { const { head, body } = pageOf(sun); return `<article class="ffs-page">${head}${body}</article>`; }
  const c = CATS[k], w = WKB[`W${k}`]; if (!w) return '';
  const sd = sideOf(w.ps), hits = c.hits || [sd.feed, sd.n], its = w.things;
  const grp = (h, list, extra = '') => (list.length ? `<section class="ffs-gp"><h4 class="ffs-gh">${h}<span>${list.length}</span></h4>${extra}<div class="ffs-gl">${list.map(sxRow).join('')}</div></section>` : '');
  const head = `<div class="ffs-sxh">${c.line ? `<p class="ffs-what">${linkText(c.line)}</p>` : ''}${chips([pl(c.n ?? sd.n, 'post'), [`${hits[0]} of ${hits[1]} beat ${WHO} usual`, hits[0] * 2 > hits[1] ? 'r' : '']])}</div>`;
  if (k === 0) { const kinds = its.filter((b) => b.shelf !== 'repeat'); return head + grp('Kinds', kinds, kindBar(kinds)) + grp('Repeats', its.filter((b) => b.shelf === 'repeat')); }
  return `${head}<div class="ffs-gl">${its.map(sxRow).join('')}</div>`;
}
const sectHome = () => `<div class="ffs ffs-sx">${wide ? '' : dockHTML()}${tabsHTML()}<div class="ffs-sxb">${sxBody(secTab)}</div></div>`;
function paintSect() {
  const host = listHost(), root = host && host.querySelector('.ffs-sx'); if (!root) return;
  const bar = root.querySelector('.ffs-tabs');
  bar.querySelectorAll('.ffs-tab').forEach((t) => { const on = +t.dataset.fftab === secTab; t.classList.toggle('on', on); t.setAttribute('aria-selected', String(on)); if (on) bar.scrollTo({ left: Math.max(0, t.offsetLeft - (bar.clientWidth - t.offsetWidth) / 2), behavior: reduce ? 'auto' : 'smooth' }); });
  const bd = root.querySelector('.ffs-sxb'); bd.getAnimations().forEach((a) => a.cancel()); bd.innerHTML = sxBody(secTab); playProofs(bd);
  if (!wide && sheetOpen) host.scrollTop = 0;
  stagger(bd, '.ffs-sxh > *, .ffs-gp, .ffs-sxb > .ffs-gl > *, .ffs-page > *', 20, 10);
}
function pickSection(k) {
  if (k >= 0 && !WKB[`W${k}`]) return;
  secTab = k; chipCat = k < 0 ? null : k; wedgeOpen = null;
  const inHome = !sel && (wide ? readEl.dataset.view === 'list' : sheetOpen && isHome());
  if (inHome) { paintSect(); if (!wide && detent === 'dock') setDetent('half'); }
  else { trail = []; homeOpen = true; if (sel) select(null); else show(); }
  frameWedge(k < 0 ? null : k); lastTouch = performance.now();
}
const listHTML = () => (SECT ? sectHome() : wide ? `<div class="ffs">${topHTML()}${madeOfHTML()}${lawsHTML()}</div>` : shelvesHTML());
// as a phone's list scrolls, the card at the top of what you can see lights on the map (a wedge softly), worked out from
// the list's scroll against where its cards sit, read once
let listFocus = null, listMarks = null, listRaf = 0;
// on a phone the card a shelf has snapped to lights on the map, from the shelf touched last: worked out from scrollLeft
// against where its cards sit, read once when the list is filled
let shelfLast = null, shelfPos = null, restoreX = null;
function shelfAt(row) {
  if (!shelfPos) shelfPos = {}; let m = shelfPos[row.dataset.ffrow];
  if (!m) { const pad = parseFloat(getComputedStyle(row).paddingLeft) || 0; m = shelfPos[row.dataset.ffrow] = [...row.children].map((c) => [c.offsetLeft - pad, c.dataset.ffhx]); }
  const x = row.scrollLeft; let best = m[0], bd = 1e9; m.forEach((c) => { const d = Math.abs(c[0] - x); if (d < bd) { bd = d; best = c; } });
  return best ? best[1] : null;
}
function onShelf(e) { const row = e.target; if (!row.dataset || !row.dataset.ffrow) return; shelfLast = row.dataset.ffrow; listAt(); }
const shelvesNow = () => { const o = { last: shelfLast }; if (sheetEl) sheetEl.querySelectorAll('[data-ffrow]').forEach((r) => { o[r.dataset.ffrow] = r.scrollLeft; }); return o; };
function listAt() {
  cancelAnimationFrame(listRaf);
  if (!wide && sheetEl && isHome()) { if (detent === 'dock' || detent === 'peek' || !shelfLast) { listFocus = null; return; }
    const row = sheetEl.querySelector(`[data-ffrow="${shelfLast}"]`); listFocus = row ? itemOf(shelfAt(row)) : null; lastTouch = performance.now(); return; }
  if (!sheetEl || !isHome() || detent === 'dock' || detent === 'peek') { listFocus = null; return; }
  listRaf = requestAnimationFrame(() => { if (dead || !sheetEl) return; const body = sheetEl.querySelector('.ffx-body');
    if (!listMarks) { const b0 = body.getBoundingClientRect(), s0 = body.scrollTop; listMarks = [...body.querySelectorAll('[data-ffhx]')].map((c) => { const r = c.getBoundingClientRect(); return [r.top - b0.top + s0, r.bottom - b0.top + s0, c.dataset.ffhx]; }); }
    const st = body.scrollTop, m = listMarks.find((x) => x[1] > st + 64);
    listFocus = m && m[0] < st + (halfH() - 20) * .45 ? itemOf(m[2]) : null; lastTouch = performance.now(); });
}
function stagger(root, sel2, base = 30, max = 22) { if (reduce) return; [...root.querySelectorAll(sel2)].slice(0, max).forEach((el, j) => el.animate([{ opacity: 0, transform: 'translateY(8px)' }, { opacity: 1, transform: 'none' }], tm({ duration: 480, delay: base + j * 40, easing: CARRY, fill: 'backwards' }))); }
function reading(fresh) {
  // a phone's full screen reads in its sheet (v9): nothing sits over the map but the dock
  if (!wide && full) { readEl.innerHTML = ''; readEl.dataset.view = 'none'; readEl.dataset.ffsel = ''; return; }
  if (!wide) { readEl.innerHTML = stripHTML(); readEl.dataset.view = 'strip'; readEl.dataset.ffsel = ''; if (fresh) stagger(readEl, '.ffs-fold', 260, 1); return; }
  readEl.innerHTML = listHTML(); readEl.dataset.view = 'list'; if (fresh) stagger(readEl, '.ffs-top > *, .ffs-lh, .rk, .ck, .ffs-ld');
  playProofs(readEl);
}
// under the map on a narrow card, the whole reading folded into one bold strip: the account's line, what's in the reading,
// and the way in (it opens in the sheet)
const stripHTML = () => `<button type="button" class="ffs-fold" data-ffhome aria-label="The reading of @${esc(D.handle)}"><span class="ffs-av">${avatarHTML()}</span><span class="ffs-bt"><em>@${esc(D.handle)} · the reading</em><b>${esc(sun.name)}</b><i>${countsOf()}</i></span><span class="ffs-bu" aria-hidden="true"><svg class="ic go-full" viewBox="0 0 24 24"><path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5"/></svg><i class="go-up">↑</i></span></button>`;
// a wedge's row opens in place (the one that was open shuts): its list glides to its height on the carry curve and its
// things rise in turn, and the system turns that wedge to face you with its things lit. Shutting the last one glides the
// system back to the view you had before
const listHost = () => (!wide && sheetOpen ? sheetEl.querySelector('.ffx-body') : readEl);
function toggleWedge(k) {
  const was = wedgeOpen; wedgeOpen = was === k ? null : k; wedgeAll = true; chipCat = wedgeOpen;
  [was, wedgeOpen].forEach((j) => {
    const sec = j == null ? null : listHost().querySelector(`.wk[data-ffcat="${j}"]`); if (!sec) return;
    const open = j === wedgeOpen, list = sec.querySelector('.wk-rows'), more = sec.querySelector('.wk-more:not(.none)');
    sec.classList.toggle('on', open); list.getAnimations().forEach((a) => a.cancel());
    const h0 = list.offsetHeight; list.innerHTML = open ? wedgeRows(j) : wkTop(j, WK_N).map(wkRow).join('');
    if (more) more.textContent = open ? 'Fewer' : pl(wkAll(j) - wkTop(j, WK_N).length, 'more thing', 'more things');
    if (reduce) return;
    const h1 = list.offsetHeight; list.style.overflow = 'hidden';
    const a = list.animate([{ height: `${h0}px` }, { height: `${h1}px` }], tm({ duration: open ? 560 : 380, easing: CARRY }));
    a.onfinish = () => { list.style.overflow = ''; };
    if (open) stagger(list, `.ffs-row:nth-child(n+${WK_N + 1}), .ffs-more, .ffs-once`, 90, 12);
    // the opened wedge's head glides into view: in the sheet (risen to full height) on a narrow card, in the panel's own
    // scroll in full screen on a wide one, else in the page
    if (open && !wide && sheetOpen) { if (detent === 'peek') setDetent('full'); panelTo(sec.offsetTop - 8, listHost()); }
    else if (open && panelScrolls()) panelTo(sec.offsetTop - 6);
    else if (open) { const t = sec.getBoundingClientRect().top; if (t < 24 || t > innerHeight * .55) glideScroll(Math.max(0, scrollY + t - 24)); }
  });
  if (wedgeOpen != null) { if (was == null) wedgeBefore = { tg: controls.target.clone(), pos: camera.position.clone(), shift: fitShift }; frameWedge(wedgeOpen); }
  else if (wedgeBefore) { const b = wedgeBefore; wedgeBefore = null; fitShift = b.shift; viewGoal.y = restY(); off.copy(b.pos).sub(b.tg); sph.setFromVector3(off); flyTo({ target: b.tg, dist: sph.radius, phi: sph.phi, theta: sph.theta, dur: 1000 }); }
  lastTouch = performance.now();
}
// the reading scrolls with the page (the map stays pinned beside it on a wide card), except in full screen on a wide card,
// where it scrolls in its own column
const panelScrolls = () => wide && full;
let panelRaf = 0;
function panelTo(y1, el = readEl) {
  const y0 = el.scrollTop, t0 = performance.now(), dur = reduce ? 1 : 560 * TEMPO; cancelAnimationFrame(panelRaf);
  const go = (t) => { const u = carry(clamp((t - t0) / dur)), max = el.scrollHeight - el.clientHeight; el.scrollTop = Math.min(max, y0 + (y1 - y0) * u); if (u < 1 && !dead) panelRaf = requestAnimationFrame(go); };
  panelRaf = requestAnimationFrame(go);
}
// on a narrow card the reading is part of the page, so a page change there keeps your place: a page opened from the map
// while the reading is off the bottom of the screen glides up just far enough to show its head; one opened from a row
// further down the list glides back to its head; going back to the list returns you to where you were in it
const navTop = () => { const n = document.querySelector('[data-fm-bottom-nav]'), r = n && n.getBoundingClientRect(); return r && r.height ? r.top : innerHeight; };
const TOP_SAFE = 96;
let glideRaf = 0;
function glideScroll(y1) {
  const y0 = scrollY, t0 = performance.now(), dur = reduce ? 1 : 620 * TEMPO; cancelAnimationFrame(glideRaf);
  if (Math.abs(y1 - y0) < 2) return;
  const stop = () => { cancelAnimationFrame(glideRaf); window.removeEventListener('touchstart', stop); window.removeEventListener('wheel', stop); };
  window.addEventListener('touchstart', stop, { passive: true, once: true }); window.addEventListener('wheel', stop, { passive: true, once: true });
  const go = (t) => { const u = carry(clamp((t - t0) / dur)); window.scrollTo(0, y0 + (y1 - y0) * u); if (u < 1 && !dead) glideRaf = requestAnimationFrame(go); else stop(); };
  glideRaf = requestAnimationFrame(go);
}
function revealReading() {
  if (full) return; const r = readEl.getBoundingClientRect(), foot = navTop();
  // beside the pinned map, a page opened from further down the list starts at its head
  if (wide) { if (r.top < 0) glideScroll(Math.max(0, scrollY + r.top - 24)); return; }
  if (r.top < TOP_SAFE) glideScroll(Math.max(0, scrollY + r.top - TOP_SAFE));
  else if (r.top > foot - 150) glideScroll(scrollY + r.top - (foot - 280));
}
// ---------- on a phone the reading lives in a sheet (v6.1, Oct 21; one sheet since v9, Oct 22). In the page the reading
// is one bold strip under the map: the account's line, and the way into full screen. In full screen the reading is the
// sheet, at one of three heights: docked (one line of who this is; the map has the screen), half (the reading in the
// bottom half, the map centred above it, the card at the top of the list lit on it) and full. A card or a row of the list
// opens its page at full height, and back (or a pull down) returns to the list where you were. A thing tapped on the map
// comes up as a glance (its head and a few numbers) with the map above it, what's open lit; pull it up to read it, pull it
// down and it's gone, back to the dock. While it's up the map is all yours: a drag turns it, a pinch zooms, a tap opens
// something else, a tap on nothing docks it all. The bar steps ‹ › and closes (×, Escape). A post mortem opened from it
// comes up over it
const SHEET_EASE = 'cubic-bezier(.16, .9, .2, 1)', CLOSE_X = '<svg class="ic" viewBox="0 0 24 24" aria-hidden="true"><path d="M6 6l12 12M18 6L6 18"/></svg>';
let sheetEl = null, sheetOpen = false, sheetAt = 0, sheetPrev = '', sheetLocked = false, sdir = 1, detent = 'peek', homeOpen = false, draggedAt = 0, openDirect = false, nextDet = null;
const DETS = ['full', 'half', 'peek', 'dock'];
// the bar names what's open once its head has scrolled away, as the read's sheet does
const sheetTitle = (x) => (x.kind === 'law' ? `Rule ${pad2(x.n)} · ${x.short || x.law}` : x === sun ? `@${D.handle}` : x.name);
const shEls = () => ({ el: sheetEl, sh: sheetEl.querySelector('.ffx-sheet'), scrim: sheetEl.querySelector('.ffx-scrim'), body: sheetEl.querySelector('.ffx-body') });
// peeking, it shows the glance: the bar and the head of what's open (its title and a few numbers), nothing more; the rest
// of the screen is the map
let safeBottom = null;
const safeB = () => { if (safeBottom == null) { const pr = document.createElement('div'); pr.style.cssText = 'position:fixed;left:0;bottom:0;width:0;height:env(safe-area-inset-bottom,0px);visibility:hidden;pointer-events:none'; document.body.appendChild(pr); safeBottom = pr.offsetHeight; pr.remove(); } return safeBottom; };
const peekH = () => {
  const { sh, body } = shEls(), g = body.querySelector('.ffs-page > :first-child');
  if (!g) return Math.round(innerHeight * .3);
  return Math.round(Math.max(140, Math.min(innerHeight * .42, g.getBoundingClientRect().bottom - sh.getBoundingClientRect().top + body.scrollTop + 16 + safeB())));
};
// docked, the bar's grab (18) and the dock line (2 + 56) with room under it (14): feederFile.css keeps these heights.
// Half, the reading takes the bottom 56% of the screen
const DOCK = 90, dockH = () => DOCK + safeB(), halfH = () => Math.round(innerHeight * .56);
const isHome = () => !!sheetEl && sheetEl.dataset.ffview === 'home';
// a page opened from the list has no glance: pulled down, it goes back to the list
const noPeek = () => !!sheetEl && sheetEl.dataset.ffnopeek === '1';
const yOf = (d) => { const h = sheetEl.querySelector('.ffx-sheet').offsetHeight; return d === 'full' ? 0 : Math.max(0, h - (d === 'dock' ? dockH() : d === 'half' ? halfH() : peekH())); };
const curY = (sh) => { const m = getComputedStyle(sh).transform; return m && m !== 'none' ? new DOMMatrixReadOnly(m).m42 : 0; };
const stopAll = (...ns) => ns.forEach((n) => n.getAnimations().forEach((a) => { try { a.commitStyles(); } catch (_) { /* gone */ } a.cancel(); }));
function sheetDom() {
  if (sheetEl) return sheetEl;
  sheetEl = document.createElement('div'); sheetEl.className = 'ff ffx'; sheetEl.setAttribute('aria-hidden', 'true');
  sheetEl.innerHTML = `<div class="ffx-scrim" data-ffpeek></div><div class="ffx-sheet" role="dialog" aria-label="The Feeder File"><div class="ffx-hd" data-ffgrab><i class="ffx-grab" aria-hidden="true"></i><div class="ffx-bar"><button type="button" class="ffx-back" data-ffback hidden><span aria-hidden="true">←</span><span>The reading</span></button><span class="ffx-t"></span>${navHTML}<button type="button" class="ffx-x" data-ffclose aria-label="Close">${CLOSE_X}</button></div></div><div class="ffx-body"></div></div>`;
  (LAYER || document.body).appendChild(sheetEl);
  sheetEl.addEventListener('click', onPanelClick);
  sheetEl.addEventListener('mouseover', onRowOver);
  const { sh, scrim, body } = shEls(), hd = sheetEl.querySelector('.ffx-hd');
  body.addEventListener('scroll', () => { hd.classList.toggle('stuck', body.scrollTop > 140); listAt(); }, { passive: true });
  body.addEventListener('scroll', onShelf, { passive: true, capture: true });
  // a pull: the sheet follows the finger and settles at the nearest height, or the one it was flicked toward. It takes the
  // pull while it's docked or peeking, from its bar or the dock line, or pulling down from the top of the reading;
  // otherwise the reading scrolls
  let g = null;
  const begin = (y, x, inBody) => ({ y0: y, x0: x, inBody, top: body.scrollTop <= 0, on: false, base: 0, last: y, lt: performance.now(), v: 0 });
  const move = (y, x) => {
    const dy = y - g.y0, dx = x - g.x0;
    if (!g.on) {
      if (!(detent === 'peek' || detent === 'dock' || !g.inBody || (g.top && dy > 0))) { if (Math.abs(dy) > 2) g = null; return false; }
      if (Math.abs(dy) < 6 && Math.abs(dx) < 6) return true;
      if (Math.abs(dx) > Math.abs(dy)) { g = null; return false; }
      stopAll(sh, scrim); g.on = true; g.y0 = y; g.base = curY(sh); g.P = Math.max(1, yOf(isHome() || noPeek() ? 'half' : 'peek')); holdS = false;
    }
    const ty = Math.max(0, g.base + (y - g.y0));
    sh.style.transform = `translateY(${ty}px)`; scrim.style.opacity = String(Math.max(0, 1 - ty / g.P));
    const now = performance.now(); g.v = (y - g.last) / Math.max(1, now - g.lt); g.last = y; g.lt = now;
    return true;
  };
  const end = () => { const d = g; g = null; if (!d || !d.on) return; draggedAt = performance.now(); settle(curY(sh), d.v); };
  sh.addEventListener('touchstart', (e) => { g = e.touches.length === 1 ? begin(e.touches[0].clientY, e.touches[0].clientX, !!e.target.closest('.ffx-body') && !e.target.closest('.ffs-dock')) : null; }, { passive: true });
  sh.addEventListener('touchmove', (e) => { if (g && move(e.touches[0].clientY, e.touches[0].clientX)) e.preventDefault(); }, { passive: false });
  sh.addEventListener('touchend', end); sh.addEventListener('touchcancel', end);
  // with a mouse, the bar drags it
  hd.addEventListener('pointerdown', (e) => { if (e.pointerType !== 'mouse' || e.target.closest('button')) return; g = begin(e.clientY, e.clientX, false); hd.setPointerCapture(e.pointerId); });
  hd.addEventListener('pointermove', (e) => { if (g && e.pointerType === 'mouse') move(e.clientY, e.clientX); });
  hd.addEventListener('pointerup', (e) => { if (e.pointerType === 'mouse') end(); });
  return sheetEl;
}
// where a pull lands: a flick goes to the next height the way it was flicked, a slow pull to the nearest. The list's
// heights are docked, half and full (the dock is as low as it goes); a page from the list has full, and under it the way
// back to the list; a glance has peek and full, and under them the dock
function settle(y, v) {
  const H = sheetEl.querySelector('.ffx-sheet').offsetHeight, home = isHome(), ds = home ? ['full', 'half', 'dock'] : noPeek() ? ['full'] : ['full', 'peek'];
  const stops = [...ds.map(yOf), ...(home ? [] : [noPeek() ? H * .5 : H])];
  let k = v > .55 ? stops.findIndex((s) => s > y + 4) : v < -.55 ? stops.map((s, j) => (s < y - 4 ? j : -1)).filter((j) => j >= 0).pop() ?? 0 : -1;
  if (k < 0) { let bd = 1e9; stops.forEach((s, j) => { if (Math.abs(s - y) < bd) { bd = Math.abs(s - y); k = j; } }); if (v > .55) k = stops.length - 1; }
  if (k < ds.length) { setDetent(ds[k], y); return; }
  if (noPeek()) toList(); else closeAll();
}
function setDetent(d, from) {
  if (d === 'peek' && noPeek()) { toList(); return; }
  const { el, sh, scrim, body } = shEls(), y0 = from ?? curY(sh);
  if (d === 'peek' || d === 'dock') body.scrollTop = 0;
  const y1 = yOf(d), o0 = +getComputedStyle(scrim).opacity;
  detent = d; DETS.forEach((c) => el.classList.toggle(c, c === d)); if (d === 'half') sh.style.setProperty('--ffx-off', `${y1}px`);
  // at full height the map is covered, so it holds still; lower, it centres in the room above the sheet
  if (d === 'full') { if (!wide) holdS = true; } else { holdS = false; revealMap(); if (!wide) { sheetLift = Math.round((sh.offsetHeight - y1) / 2); restGoal(); } }
  listAt();
  stopAll(sh, scrim); sh.style.transform = `translateY(${y1}px)`; scrim.style.opacity = d === 'full' ? '1' : '0';
  if (reduce) return;
  sh.animate([{ transform: `translateY(${y0}px)` }, { transform: `translateY(${y1}px)` }], tm({ duration: 560, easing: SHEET_EASE }));
  scrim.animate([{ opacity: o0 }, { opacity: d === 'full' ? 1 : 0 }], tm({ duration: 420, easing: CARRY }));
}
// one step down from where it is: the list to half, a page from the list back to it, a glance to its peek
const stepDown = () => { if (isHome()) setDetent(detent === 'half' ? 'dock' : 'half'); else if (noPeek()) toList(); else setDetent(detent === 'full' ? 'peek' : 'full'); };
// it rises from under the screen; in full screen it waits for the frame to be mostly open (delay)
function openSheet(d, delay = 0) {
  sheetDom(); const { el, sh, scrim } = shEls();
  sheetOpen = true; sheetAt = performance.now(); detent = d;
  el.classList.add('on'); el.classList.toggle('over', full); DETS.forEach((c) => el.classList.toggle(c, c === d)); el.setAttribute('aria-hidden', 'false');
  stopAll(sh, scrim);
  // over the page the page under it holds still (in the app the page scrolls on the root element); a phone's full screen
  // holds itself (touch-action), so Safari's bars and the viewport never move
  if (!full) { sheetPrev = document.documentElement.style.overflow; document.documentElement.style.overflow = 'hidden'; sheetLocked = true; }
  applyGestures(); window.dispatchEvent(new CustomEvent('ff:hold', { detail: true }));
  const y1 = yOf(d); if (d === 'half') sh.style.setProperty('--ffx-off', `${y1}px`);
  if (!wide) { sheetLift = Math.round((sh.offsetHeight - y1) / 2); restGoal(); }
  scrim.style.opacity = d === 'full' ? '1' : '0';
  if (reduce) { sh.style.transform = `translateY(${y1}px)`; return; }
  const h = sh.offsetHeight; sh.style.transform = `translateY(${h}px)`; if (d === 'full') scrim.style.opacity = '0';
  afterPaint(() => { if (!sheetOpen || detent !== d) return; sh.style.transform = `translateY(${y1}px)`; sh.animate([{ transform: `translateY(${h}px)` }, { transform: `translateY(${y1}px)` }], tm({ duration: delay ? 640 : 700, delay, easing: delay ? CARRY : SHEET_EASE, fill: 'backwards' }));
    if (d === 'full') { scrim.style.opacity = '1'; scrim.animate([{ opacity: 0 }, { opacity: 1 }], tm({ duration: 420, delay, easing: CARRY, fill: 'backwards' })); } });
  if (d === 'full' && !wide) holdS = true;
}
function closeSheet(now) {
  if (!sheetOpen) return; sheetOpen = false; holdS = false; listFocus = null;
  const { el, sh, scrim } = shEls();
  const done = () => { if (sheetOpen) return; el.classList.remove('on', 'over', ...DETS); el.setAttribute('aria-hidden', 'true'); stopAll(sh, scrim); sh.style.transform = ''; scrim.style.opacity = ''; el.querySelector('.ffx-body').innerHTML = ''; el.dataset.ffsel = ''; el.dataset.ffview = ''; listMarks = null; };
  if (sheetLocked) { document.documentElement.style.overflow = sheetPrev; sheetLocked = false; }
  applyGestures(); window.dispatchEvent(new CustomEvent('ff:hold', { detail: false }));
  if (sheetLift) { sheetLift = 0; restGoal(); }
  if (reduce || now) { done(); return; }
  const y0 = curY(sh), op = +getComputedStyle(scrim).opacity;
  stopAll(sh, scrim);
  scrim.animate([{ opacity: op }, { opacity: 0 }], tm({ duration: 400, easing: CARRY, fill: 'forwards' }));
  sh.animate([{ transform: `translateY(${y0}px)` }, { transform: `translateY(${sh.offsetHeight}px)` }], tm({ duration: 440, easing: CARRY, fill: 'forwards' })).onfinish = done;
}
// ---------- the trail: where you've been in the reading. Every page opened from the reading (a row, a chip, a link) or
// from the map goes on top of it; back takes you to the one before, exactly where you were in it (its scroll, the wedge
// you had open, the sheet's height). ‹ › move to the next of the same kind in place, and closing (×, Escape, a tap on
// nothing, the sheet pulled away) empties it. A wide card's reading always has the list under it; a phone's sheet shows
// back only when there's somewhere to go back to (a thing tapped on the map over the docked sheet has nowhere: closing it
// docks)
let trail = [], restoreY = null;
const scrollNow = () => (!wide && sheetOpen ? sheetEl.querySelector('.ffx-body').scrollTop : panelScrolls() ? readEl.scrollTop : scrollY);
function go(x, from) {
  if (!x) return;
  // with sections, the account and each section are tabs of the reading, not pages
  if (SECT && !from && (x === sun || x.kind === 'wedge')) { pickSection(x === sun ? -1 : x.k); return; } if (x === sel && !from) { select(x); return; }
  const open = wide || (sheetOpen && !(isHome() && detent === 'dock'));
  if (!open && !from) trail = [];
  if (open || from) trail.push({ x: sel || null, y: scrollNow(), sx: !wide && isHome() ? shelvesNow() : null, wedge: wedgeOpen, sheet: sheetOpen, home: homeOpen, det: detent, ext: from || null });
  select(x);
}
function back() {
  const e = trail.pop(); if (!e) { if (noPeek()) toList(); else closeAll(); return; }
  // a hand-over from the read's sheets goes back to it on the page where it was open (the wall, say)
  if (e.ext && !wide && !e.sheet) { trail = []; closeAll(); cancelAnimationFrame(glideRaf); scrollTo(0, e.y); }
  else { restoreY = e.y; restoreX = e.sx; wedgeOpen = e.wedge; sdir = -1; if (e.x) select(e.x); else { homeOpen = wide || e.home || full; if (!wide && full) nextDet = e.det === 'peek' ? 'dock' : e.det; select(null); } }
  if (e.ext) window.dispatchEvent(new CustomEvent('ff:reopen', { detail: e.ext }));
}
// a page opened from a phone's list, pulled down: back to the list where you left it (a list left at full comes back at
// half, so the map shows again)
function toList() {
  const root = trail[0], home = root && !root.x && !root.ext; trail = []; homeOpen = true; sdir = -1;
  restoreY = home ? root.y : 0; restoreX = home ? root.sx : null; nextDet = home && root.det !== 'peek' ? (root.det === 'full' ? 'half' : root.det) : 'dock';
  if (sel) select(null); else show();
}
// everything away: what was open closes and the page scrolls again; a phone's full screen docks its sheet. A wide card's
// list comes back where you left it
function closeAll() {
  const root = trail[0]; trail = []; homeOpen = false;
  if (!wide && full) { homeOpen = true; nextDet = 'dock'; if (sel) select(null); else show(); return; }
  if (wide && sel && root && !root.x) { restoreY = root.y; wedgeOpen = root.wedge; }
  if (sel) select(null); else show();
}
// with the sheet peeking, the map is in view above it: a page opened from elsewhere on the page glides the map under the
// header first
function revealMap() {
  if (full) return;
  const hdr = document.getElementById('rd-hdr'), cap = hdr && hdr.closest('.fm-depth-chrome--header'), top = (cap ? cap.getBoundingClientRect().top : 10) + 76;
  const r = stage.getBoundingClientRect(); if (r.top >= top - 4 && r.bottom <= innerHeight - peekH() + 4) return;
  glideScroll(Math.max(0, scrollY + r.top - top));
}
// what the sheet holds: a page (what's open) or the whole reading (the list). Opening fills it and brings it up (in full
// screen, after a delay); changing what it holds lets the old content give way to the side it's going and brings the new
// in from the other. The height goes with it: the list keeps its height (or the one back asks for); a page from the list
// opens at full height, a thing from the map as a glance; a page from a page keeps the height it had
function sheetShow(kind, delay = 0) {
  const el = sheetDom(), { body } = shEls(), key = kind === 'page' ? String(sel.i) : 'home', fromHome = !sheetOpen || isHome(), direct = openDirect;
  let det = nextDet; nextDet = null; openDirect = false;
  if (!det) det = kind === 'home' ? (sheetOpen && isHome() ? detent : 'dock') : fromHome ? (direct ? 'full' : 'peek') : detent;
  if (sheetOpen && el.dataset.ffsel === key) { if (det !== detent) setDetent(det); return; }
  if (kind === 'page' && fromHome) el.dataset.ffnopeek = direct ? '1' : '';
  const stepping = sheetOpen, dir = sdir, y = restoreY; sdir = 1; restoreY = null;
  const fill = () => {
    const was = el.dataset.ffview; el.dataset.ffsel = key; el.dataset.ffview = kind; listMarks = null; shelfPos = null; const sx = restoreX; restoreX = null; shelfLast = kind === 'home' && sx ? sx.last : null;
    el.querySelector('.ffx-t').textContent = kind === 'page' ? sheetTitle(sel) : `@${D.handle}`;
    const prev = trail[trail.length - 1], bk = el.querySelector('.ffx-back');
    bk.hidden = !prev; if (prev) bk.querySelector('span:last-child').textContent = backTo(prev);
    if (kind === 'page') { const { head, body: b } = pageOf(sel); body.innerHTML = `<article class="ffs-page">${head}${b}</article>`; } else body.innerHTML = listHTML();
    // the shelves fit the half height on most phones: then the list holds still (no empty scroll under it); on a short
    // screen it still scrolls
    const shf = kind === 'home' && !wide ? body.querySelector('.ffs-sh') : null; el.classList.toggle('fit', !!shf && shf.getBoundingClientRect().bottom - el.querySelector('.ffx-sheet').getBoundingClientRect().top + 12 <= halfH());
    body.scrollTop = y ?? 0; if (sx) body.querySelectorAll('[data-ffrow]').forEach((r) => { if (sx[r.dataset.ffrow]) r.scrollLeft = sx[r.dataset.ffrow]; }); el.querySelector('.ffx-hd').classList.toggle('stuck', body.scrollTop > 140); playProofs(body);
    // the bar (back, ‹ ›, ×) comes in with a page and goes with the list
    if (stepping && was !== kind && kind === 'page' && !reduce) el.querySelector('.ffx-bar').animate([{ opacity: 0, transform: `translateX(${dir * 24}px)` }, { opacity: 1, transform: 'none' }], tm({ duration: 520, easing: SHEET_EASE }));
    if (stepping && det !== detent) setDetent(det); else listAt();
  };
  if (!stepping) { fill(); openSheet(det, delay); if (det === 'peek') revealMap(); if (!delay) stagger(body, kind === 'page' ? '.ffs-page > *' : '.ffs-top > *, .ffs-lh, .rk, .ffs-ld', 240, 9); return; }
  // a height that doesn't hang on the new content moves at once; a glance's is measured from its head, so it waits for it
  if (det !== 'peek' && det !== detent) setDetent(det);
  body.getAnimations().forEach((a) => a.cancel());
  if (reduce) { fill(); return; }
  const out = body.animate([{ opacity: 1, transform: 'none' }, { opacity: 0, transform: `translateX(${-dir * 24}px)` }], tm({ duration: 170, easing: 'cubic-bezier(.4, 0, 1, 1)', fill: 'forwards' }));
  // the new page goes in while the sheet's body is clear, and slides in from the next painted frame
  out.onfinish = () => { if (dead || !sheetOpen) return; fill(); body.getAnimations().forEach((a) => a.cancel()); body.style.opacity = '0';
    afterPaint(() => { body.style.opacity = ''; body.animate([{ opacity: 0, transform: `translateX(${dir * 24}px)` }, { opacity: 1, transform: 'none' }], tm({ duration: 520, easing: SHEET_EASE })); }); };
}
// show what's open: the reading turns to its page (sliding in from the side) and back again. A narrow card keeps its strip
// in the page and shows what's open in the sheet: a page, else the whole reading if it was asked for, else nothing
function show() {
  viewGoal.x = 0; viewGoal.y = restY(); wrap.classList.toggle('has-sel', !!sel);
  if (!wide) {
    if (readEl.dataset.view !== (full ? 'none' : 'strip')) reading(false);
    // on a narrow card the reading happens only in full screen: what opens takes the map full screen first
    if ((sel || homeOpen) && !full) { setFull(true, () => { if (sel || homeOpen) show(); }); return; }
    // in full screen the sheet is always there: what's open, else the list
    if (sel) sheetShow('page'); else if (homeOpen || full) sheetShow('home'); else if (sheetOpen) closeSheet();
    return;
  }
  homeOpen = false;
  if (sheetOpen) closeSheet(true);
  const y = restoreY; restoreY = null;
  if (!sel) {
    if (readEl.dataset.view !== 'list') { reading(false); readEl.dataset.ffsel = ''; if (!reduce) readEl.animate([{ opacity: 0, transform: 'translateX(-12px)' }, { opacity: 1, transform: 'none' }], tm({ duration: 300, easing: CARRY })); }
    if (y != null) putScroll(y);
    return;
  }
  // the first page slides in from the side; stepping from one page to the next only crossfades, so nothing jumps
  if (readEl.dataset.ffsel === String(sel.i) && readEl.dataset.view === 'page') return;
  const { head, body } = pageOf(sel), was = readEl.dataset.view;
  readEl.innerHTML = `<article class="ffs-page">${backHTML()}${head}${body}</article>`; readEl.dataset.view = 'page'; readEl.dataset.ffsel = String(sel.i); if (panelScrolls() && y == null) readEl.scrollTop = 0;
  if (!reduce) { if (was === 'list') { readEl.animate([{ opacity: 0, transform: 'translateX(16px)' }, { opacity: 1, transform: 'none' }], tm({ duration: 420, easing: CARRY })); stagger(readEl, '.ffs-page > *', 40, 7); }
    else readEl.animate([{ opacity: .2 }, { opacity: 1 }], tm({ duration: 240, easing: 'ease-out' })); }
  playProofs(readEl);
  if (y != null) putScroll(y); else revealReading();
}
// back lands exactly where you were (no glide: it's where you left it)
function putScroll(y) { if (panelScrolls()) readEl.scrollTop = y; else { cancelAnimationFrame(glideRaf); scrollTo(0, y); } }
function toRule(id) { const go = () => window.dispatchEvent(new CustomEvent('ff:rule', { detail: id })); if (sheetOpen && !(full && !wide)) closeAll(); if (full && !(LAYER && !wide)) setFull(false, go); else go(); }
// a run's read and the wall belong to the read below (readerEngine hears these)
function toRun(r) { window.dispatchEvent(new CustomEvent('ff:run', { detail: r })); }
// a law opened from elsewhere on the page (a post mortem's chip, a run's read): the reading turns to it, in view
function openLaw(id, from) {
  const L = LAW[id]; if (!L) return;
  go(L, from);
  if (!wide) return;
  const t = wrap.getBoundingClientRect().top;
  if (t < 0 || t > innerHeight * .4) glideScroll(Math.max(0, scrollY + t - 24));
}
// the read hands over { id, from }: from is the sheet it came from, which back brings back
const onRdLaw = (e) => { const d = e && e.detail; openLaw(d && typeof d === 'object' ? d.id : d, d && d.from); };
window.addEventListener('rd:law', onRdLaw);
// a thing named in a reading elsewhere on the page (a post mortem's move, a run's read): its page, in view
function openThing(name, from) {
  const b = byName.get(name); if (!b) return;
  go(b, from);
  if (!wide) return;
  const t = wrap.getBoundingClientRect().top;
  if (t < 0 || t > innerHeight * .4) glideScroll(Math.max(0, scrollY + t - 24));
}
const onRdNode = (e) => { const d = e && e.detail; openThing(d && typeof d === 'object' ? d.id : d, d && d.from); };
window.addEventListener('rd:node', onRdNode);
// the proof under a law, folded until asked for: it plays the first time it opens
function foldProof(btn) {
  const open = btn.getAttribute('aria-expanded') !== 'true', box = btn.nextElementSibling; if (!box) return;
  btn.setAttribute('aria-expanded', String(open)); box.hidden = !open;
  if (!open) return;
  panim(box, [{ opacity: 0, transform: 'translateY(-6px)' }, { opacity: 1, transform: 'none' }], { dur: 380 });
  const f = box.querySelector('.pf'); if (f && !f.dataset.played) { f.dataset.played = '1'; requestAnimationFrame(() => { if (dead) return; layProof(f); playProof(f, reduce ? null : panim); }); }
}
// a card or a row of a phone's list opens its page at full height, straight away (no glance)
function onPanelClick(e) { openDirect = !wide && e.currentTarget === sheetEl && isHome(); panelClick(e); openDirect = false; }
function panelClick(e) {
  if (e.target.closest('a.ql')) e.preventDefault();
  const tb = e.target.closest('[data-fftab]'); if (tb) { pickSection(+tb.dataset.fftab); return; }
  const it = e.target.closest('[data-ffitem]'); if (it) { const x = itemOf(it.dataset.ffitem); if (x) go(x); return; }
  const st = e.target.closest('[data-ffstep]'); if (st) { step(+st.dataset.ffstep); return; }
  if (e.target.closest('[data-ffclose]')) { closeAll(); return; }
  if (e.target.closest('[data-ffhome]')) { if (!wide && !full) { setFull(true); return; } trail = []; homeOpen = true; if (sel) select(null); else show(); return; }
  if (e.target.closest('[data-ffback]')) { back(); return; }
  if (e.target.closest('[data-ffpeek]')) { if (performance.now() - sheetAt > 450 && detent === 'full') stepDown(); return; }
  // the bar, or the dock line: docked ↔ half for the list, peek ↔ full for a glance, back to the list for a page from it
  const tg = e.target.closest('[data-ffdock]') || (e.target.closest('[data-ffgrab]') && !e.target.closest('button'));
  if (tg) { if (performance.now() - draggedAt > 350) { if (isHome()) setDetent(detent === 'dock' ? 'half' : 'dock'); else stepDown(); } return; }
  const lw = e.target.closest('[data-fflaw]'); if (lw) { const L = LAW[lw.dataset.fflaw]; if (L) go(L); return; }
  const ev = e.target.closest('[data-ffev]'); if (ev) { if (EVB[ev.dataset.ffev]) go(EVB[ev.dataset.ffev]); return; }
  const pf = e.target.closest('[data-ffproof]'); if (pf) { foldProof(pf); return; }
  // a reading's links: a law or a thing opens here (the read's router stays out of it); a run opens its read below
  const ql = e.target.closest('[data-law]'); if (ql) { e.stopPropagation(); if (LAW[ql.dataset.law]) go(LAW[ql.dataset.law]); return; }
  const qn = e.target.closest('[data-node]'); if (qn) { e.stopPropagation(); if (byName.has(qn.dataset.node)) go(byName.get(qn.dataset.node)); return; }
  const qr = e.target.closest('[data-tropen]'); if (qr) { e.stopPropagation(); toRun(+qr.dataset.tropen); return; }
  const lt = e.target.closest('[data-fflight]'); if (lt) { toRule(lt.dataset.fflight); return; }
  const rn = e.target.closest('[data-ffrun]'); if (rn) { toRun(+rn.dataset.ffrun); return; }
  const rc = e.target.closest('[data-ffrule]'); if (rc) { toRule(rc.dataset.ffrule); return; }
  const wd = e.target.closest('[data-ffwedge]'); if (wd) { toggleWedge(+wd.dataset.ffwedge); return; }
  const al = e.target.closest('[data-ffall]'); if (al) { wedgeAll = true; const list = al.closest('.wk-rows, .wk-all'); list.innerHTML = wedgeRows(+al.dataset.ffall); stagger(list, `.ffs-row:nth-child(n+${SHOW + 1})`, 20, 12); return; }
  const mo = e.target.closest('[data-ffmore]'); if (mo) { mo.closest('.pills').innerHTML = inCat(+mo.dataset.ffmore, bits).map(pill).join(''); return; }
  if (e.target.closest('[data-post]')) return;
  const g = e.target.closest('[data-ffgo]'); if (!g) return; go(N[+g.dataset.ffgo]);
}
readEl.addEventListener('click', onPanelClick);
let chipCat = null, hoverCat = null;
// a wedge, framed: the system turns till that wedge faces you and fits its things; none takes the view home
function frameWedge(k) {
  if (k == null) { home(); return; }
  // sections: the whole circle stays in view, at most a touch closer, with the picked sector turned toward you
  if (SECT) { const a = WEDGE[k].mid; flyTo({ target: new THREE.Vector3(0, MIDY, 0), dist: fitDist() / 1.15, phi: polar(), theta: Math.atan2(Math.cos(a), Math.sin(a)), dur: 1100 }); return; }
  const its = [...planets, ...bits].filter((b) => b.cat === k); if (!its.length) return;
  const pts = its.map(restCentre), xs = pts.map((p) => p[0]), zs = pts.map((p) => p[2]), a = WEDGE[k].mid;
  const tg = new THREE.Vector3((Math.min(...xs) + Math.max(...xs)) / 2, MIDY * .6, (Math.min(...zs) + Math.max(...zs)) / 2);
  const rad = Math.max(.45, ...pts.map((p) => Math.hypot(p[0] - tg.x, p[2] - tg.z))) + .2, vf = Math.tan(THREE.MathUtils.degToRad(camera.fov / 2)), P = polar();
  const need = Math.max(rad / (vf * camera.aspect), (rad * Math.cos(P) + YMAX * Math.sin(P) * .6 + .1) / vf);
  flyTo({ target: tg, dist: Math.min(need, fitDist()), phi: P, theta: Math.atan2(Math.cos(a), Math.sin(a)), dur: 1100 });
}
// pointing at a wedge's row lights that wedge on the system
readEl.addEventListener('mouseover', (e) => { const c = e.target.closest('[data-ffcat]'); hoverCat = c ? +c.dataset.ffcat : null; });
readEl.addEventListener('mouseleave', () => { hoverCat = null; });
let hoverX = null;
function onRowOver(e) { const r = e.target.closest('[data-ffh]'), v = r ? +r.dataset.ffh : -1; if (v !== hoverCand) { hoverCand = v; hoverT = 0; } const hx = e.target.closest('[data-ffhx]'); hoverX = hx ? itemOf(hx.dataset.ffhx) : null; }
readEl.addEventListener('mouseover', onRowOver); readEl.addEventListener('mouseleave', () => { hoverCand = -1; hoverX = null; });
q('zin').onclick = () => { moved = true; flyTo({ dist: Math.max(.7, camera.position.distanceTo(controls.target) / 1.4), dur: 450 }); lastTouch = performance.now(); };
q('zout').onclick = () => { moved = true; flyTo({ dist: Math.min(controls.maxDistance, camera.position.distanceTo(controls.target) * 1.4), dur: 450 }); lastTouch = performance.now(); };
const fullBtn = q('full'), FULL_IN = fullBtn.innerHTML, FULL_OUT = '<svg class="ic" viewBox="0 0 24 24" aria-hidden="true"><path d="M6 6l12 12M18 6L6 18"/></svg><span>Close</span>';
let portal = null, spacer = null, rootOverflow = '';
// ---------- full screen, as one move: the card's frame grows from where it sits to fill the screen while the map keeps
// its size and its place, then settles into the bigger room; leaving, the frame shrinks back to the card's place and the
// map back into it. The card moves into a layer over everything at once; what you see is that layer clipped to the card,
// the clip opening on the carry curve while the camera's distance and offset carry the map from where it was to where it
// rests. With reduced motion it just switches
const FULL_MS = 680;
let slot = null, fullAnim = null, viewTween = null;
const camDist = () => off.copy(camera.position).sub(controls.target).length();
function setCamDist(d) { off.copy(camera.position).sub(controls.target).setLength(d); camera.position.copy(controls.target).add(off); cam.on = false; }
// how far the camera has to sit, on a canvas this tall, to draw the map as big as it was on the card's
const sameSize = (d0, h0, fov0) => d0 * (H / h0) * (Math.tan(THREE.MathUtils.degToRad(fov0 / 2)) / Math.tan(THREE.MathUtils.degToRad(camera.fov / 2)));
const insetOf = (r, P, rad) => `inset(${(r.top - P.top).toFixed(1)}px ${(P.right - r.right).toFixed(1)}px ${(P.bottom - r.bottom).toFixed(1)}px ${(r.left - P.left).toFixed(1)}px round ${rad}px)`;
// the card's own pieces (its head, the map's key and buttons, the reading or its strip) glide from where they sat to
// where they land, on the same curve, so nothing jumps when the layout changes under them
const flipEls = () => [wrap.querySelector('.ff-head'), hud, toolsEl, wrap.querySelector('.ff-body')].filter(Boolean);
const rectsOf = (els) => els.map((e) => e.getBoundingClientRect());
// the view's resting place changed (the sheet came up or went): a move in flight is retargeted, else the view eases there
function restGoal() { const y = restY(); if (viewTween) viewTween.ty = y; else viewGoal.y = y; }
function tweenView(tx, ty) { viewTween = { t0: performance.now(), dur: reduce ? 1 : FULL_MS * TEMPO, fx: view.x, fy: view.y, tx, ty }; }
function chrome(v) {
  applyGestures(); document.body.classList.toggle('ff-full', v); document.body.classList.toggle('ff-full-n', v && !wide);
  window.dispatchEvent(new CustomEvent('ff:full', { detail: v }));
  if (sheetEl) sheetEl.classList.toggle('over', v && sheetOpen);
  fullBtn.innerHTML = v ? FULL_OUT : FULL_IN; fullBtn.setAttribute('aria-label', v ? 'Close full screen' : 'Full screen'); moved = false;
}
// a way in or out of full screen with no motion of its own (the read's layer is carrying it)
let inst = false;
function setFull(v, then) {
  if (dead) return;
  if (v === full || fullAnim) { if (then && v === full && !fullAnim) then(); return; }
  lastTouch = performance.now();
  if (v) (wide ? enterFull : enterFullN)(then); else (portal && portal.classList.contains('ff-pn') ? exitFullN : exitFull)(then);
}
function enterFull(then) {
  const r0 = wrap.getBoundingClientRect(), s0 = stage.getBoundingClientRect(), els = flipEls(), e0 = rectsOf(els);
  slot = { s0off: [s0.left - r0.left, s0.top - r0.top, s0.width, s0.height], h0: H, fov0: camera.fov, d0: camDist(), vx: view.x, vy: view.y, shift: fitShift, eoff: e0.map((r) => [r.left - r0.left, r.top - r0.top]) };
  spacer = document.createElement('div'); spacer.style.height = `${wrap.offsetHeight}px`; wrap.parentNode.insertBefore(spacer, wrap);
  portal = document.createElement('div'); portal.className = 'ff ff-portal'; (LAYER || document.body).appendChild(portal); portal.appendChild(wrap);
  rootOverflow = document.documentElement.style.overflow; document.documentElement.style.overflow = 'hidden';
  full = true; chrome(true); if (!wide) reading(false); size();
  const P = portal.getBoundingClientRect(), s1 = stage.getBoundingClientRect();
  const fv = fitView(); fitShift = fv.cy;
  // something open comes back, when it closes, to the whole map at full-screen size (not the card's view)
  if (sel || before) { const tg = new THREE.Vector3(0, MIDY, 0); off.copy(camera.position).sub(controls.target); sph.setFromVector3(off); sph.radius = fv.d; before = { tg, pos: new THREE.Vector3().setFromSpherical(sph).add(tg), shift: fv.cy }; }
  if ((reduce || inst)) { setCamDist(fv.d); view.x = viewGoal.x = 0; view.y = viewGoal.y = restY(); applyView(true); if (then) then(); return; }
  // the map where it was: as big as it was, where it was
  setCamDist(sameSize(slot.d0, slot.h0, slot.fov0));
  view.x = viewGoal.x = (s1.left + s1.width / 2) - (s0.left + s0.width / 2) + slot.vx; view.y = viewGoal.y = (s1.top + s1.height / 2) - (s0.top + s0.height / 2) + slot.vy; applyView(true);
  // then one move: the frame opens, the map settles into the room
  flyTo({ dist: fv.d, dur: FULL_MS }); tweenView(0, restY());
  const a = portal.animate([{ clipPath: insetOf(r0, P, 26) }, { clipPath: insetOf(P, P, 0) }], tm({ duration: FULL_MS, easing: CARRY }));
  rectsOf(els).forEach((r, k) => { const dx = e0[k].left - r.left, dy = e0[k].top - r.top; if (Math.abs(dx) + Math.abs(dy) > .5) els[k].animate([{ transform: `translate(${dx}px, ${dy}px)` }, { transform: 'none' }], tm({ duration: FULL_MS, easing: CARRY })); });
  fullAnim = a; a.onfinish = () => { fullAnim = null; if (!dead && then) then(); };
}
function exitFull(then) {
  // what's open in the reading sheet goes first; the frame closes as it does
  if (sheetOpen) { trail = []; homeOpen = false; if (sel) select(null); else closeSheet(); }
  const done = () => {
    fullAnim = null; if (dead) return;
    if (spacer) spacer.replaceWith(wrap); spacer = null; if (portal) portal.remove(); portal = null; document.documentElement.style.overflow = rootOverflow;
    full = false; chrome(false); sheetLift = 0; if (!wide) reading(false); size();
    if (slot) { fitShift = slot.shift; setCamDist(slot.d0); view.x = viewGoal.x = slot.vx; view.y = viewGoal.y = slot.vy; applyView(true); }
    viewTween = null; slot = null; before = null; if (then) then();
  };
  if ((reduce || inst) || !slot || !spacer || !portal) { done(); return; }
  const P = portal.getBoundingClientRect(), sp = spacer.getBoundingClientRect(), s1 = stage.getBoundingClientRect();
  const r0 = { left: sp.left, top: sp.top, right: sp.right, bottom: sp.top + sp.height }, s0 = { left: sp.left + slot.s0off[0], top: sp.top + slot.s0off[1], width: slot.s0off[2], height: slot.s0off[3] };
  // the map back to its size and place on the card, the frame back to the card's
  flyTo({ dist: sameSize(slot.d0, slot.h0, slot.fov0), dur: FULL_MS * .9 });
  tweenView((s1.left + s1.width / 2) - (s0.left + s0.width / 2) + slot.vx, (s1.top + s1.height / 2) - (s0.top + s0.height / 2) + slot.vy);
  viewTween.dur = FULL_MS * .9 * TEMPO;
  const a = portal.animate([{ clipPath: insetOf(P, P, 0) }, { clipPath: insetOf(r0, P, 26) }], tm({ duration: FULL_MS * .9, easing: CARRY, fill: 'forwards' }));
  const els = flipEls(), e1 = rectsOf(els), moves = [];
  if (slot.eoff && slot.eoff.length === els.length) e1.forEach((r, k) => { const dx = sp.left + slot.eoff[k][0] - r.left, dy = sp.top + slot.eoff[k][1] - r.top; if (Math.abs(dx) + Math.abs(dy) > .5) moves.push(els[k].animate([{ transform: 'none' }, { transform: `translate(${dx}px, ${dy}px)` }], tm({ duration: FULL_MS * .9, easing: CARRY, fill: 'forwards' }))); });
  fullAnim = a; a.onfinish = () => { moves.forEach((m) => m.cancel()); done(); };
}
// ---------- full screen on a phone (v8; v9, Oct 22: "the exit animation… is not optimised on safari ios"). The map already
// draws at the screen's size (the card crops it), so nothing is redrawn at a new size mid-move, and the map isn't drawn at
// all while it runs. The move is transforms only, which Safari runs off the main thread (a clip-path it doesn't): the
// layer over the page is a frame, its corners rounded once, that scales from the card's rect to the screen, and the layer
// inside it scales back so nothing in it stretches; the map layer and the card's pieces glide inside it from where they
// sat to where they land. Every step is sampled on the carry curve and played linear, so the frame and what's in it stay
// locked together. When it lands the view takes the layer's offset in the frame the layer comes home. The strip rides the
// frame's foot down and goes; the reading's sheet rises, docked, as the frame finishes opening. Closing is the same,
// backwards: the sheet sinks first, the map keeps the picture it has, and the view goes back to the card's only in the
// frame the card is put back
const flipN = () => [wrap.querySelector('.ff-head'), hud, toolsEl].filter(Boolean);
let ghost = null, pin = null;
const dropGhost = () => { if (ghost) ghost.remove(); ghost = null; };
const NK = 24, f3 = (v) => v.toFixed(3);
// the move's keyframes: the frame from rect A to rect B (P is its own rect, the screen), the layer inside scaled back, and
// each piece [el, from, to, its place at rest, opacity by progress] at its own place on the screen all the way
function moveKF(A, B, P, parts) {
  const K = { fr: [], pin: [], parts: parts.map(() => []), els: parts.map((x) => x[0]) };
  for (let k = 0; k <= NK; k++) {
    const u = k / NK, c = carry(u), x = A.left + (B.left - A.left) * c, y = A.top + (B.top - A.top) * c, sx = (A.width + (B.width - A.width) * c) / P.width, sy = (A.height + (B.height - A.height) * c) / P.height;
    K.fr.push({ offset: u, transform: `translate(${f3(x - P.left)}px, ${f3(y - P.top)}px) scale(${sx.toFixed(5)}, ${sy.toFixed(5)})` });
    K.pin.push({ offset: u, transform: `scale(${(1 / sx).toFixed(5)}, ${(1 / sy).toFixed(5)})` });
    parts.forEach(([, a, b, lay, op], j) => { const f = { offset: u, transform: `translate(${f3(a.left + (b.left - a.left) * c - x - (lay.left - P.left))}px, ${f3(a.top + (b.top - a.top) * c - y - (lay.top - P.top))}px)` }; if (op) f.opacity = +op(c).toFixed(3); K.parts[j].push(f); });
  }
  return K;
}
// put the move at its start or its end (inline), or play it
function atKF(K, i) { portal.style.transform = K.fr[i].transform; pin.style.transform = K.pin[i].transform; K.els.forEach((el, j) => { el.style.transform = K.parts[j][i].transform; if (K.parts[j][i].opacity != null) el.style.opacity = String(K.parts[j][i].opacity); }); }
function playKF(K, o) { const a = portal.animate(K.fr, o); pin.animate(K.pin, o); K.els.forEach((el, j) => el.animate(K.parts[j], o)); atKF(K, NK); return a; }
// the frame's corners, rounded once in its own space so they read 24px on the card (iPhone screens are round at full)
const cornersFor = (R, P) => `${(24 * P.width / R.width).toFixed(2)}px / ${(24 * P.height / R.height).toFixed(2)}px`;
const glide = (on) => { [portal, pin].forEach((n) => { if (n) n.style.willChange = on ? 'transform' : ''; }); };
const fadeAt = (c0, c1, up) => (c) => { const v = clamp((c - c0) / (c1 - c0)); return up ? v : 1 - v; };
function enterFullN(then) {
  const r0 = wrap.getBoundingClientRect(), m0 = mapEl.getBoundingClientRect(), els = flipN(), e0 = rectsOf(els), sEl = readEl.querySelector('.ffs-fold'), sr = sEl && sEl.getBoundingClientRect();
  slot = { n: true, vx: view.x, vy: view.y, shift: fitShift, m: [m0.left - r0.left, m0.top - r0.top], e: e0.map((r) => [r.left - r0.left, r.top - r0.top]), s: sr && [sr.left - r0.left, sr.top - r0.top, sr.width, sr.height] };
  spacer = document.createElement('div'); spacer.style.height = `${wrap.offsetHeight}px`; wrap.parentNode.insertBefore(spacer, wrap);
  const hp = document.querySelector('.fm-depth-chrome--header'), hpr = hp && hp.getBoundingClientRect(), fold = hp ? parseFloat(getComputedStyle(hp).getPropertyValue('--fm-mobile-header-chrome-compressed-height')) || 78 : 0;
  portal = document.createElement('div'); portal.className = 'ff ff-portal ff-pn'; if (hpr && hpr.height) portal.style.setProperty('--ff-top', `${Math.round(hpr.top + fold)}px`); pin = document.createElement('div'); pin.className = 'ff-pin'; portal.appendChild(pin); (LAYER || document.body).appendChild(portal); pin.appendChild(wrap);
  dropGhost(); if (sEl && sr && !(reduce || inst)) { ghost = sEl.cloneNode(true); ghost.removeAttribute('data-ffhome'); ghost.classList.add('ffs-ghost'); Object.assign(ghost.style, { position: 'absolute', left: '0', top: '0', width: `${sr.width}px`, height: `${sr.height}px`, margin: '0', zIndex: '7', pointerEvents: 'none' }); pin.appendChild(ghost); }
  full = true; chrome(true); reading(false); size(); view.x = viewGoal.x = slot.vx; view.y = viewGoal.y = slot.vy; applyView(true);
  const P = portal.getBoundingClientRect(), m1 = mapEl.getBoundingClientRect(), e1 = rectsOf(els);
  // the reading's sheet comes with it, docked (or what was tapped, as a glance), rising as the frame finishes opening;
  // where the map will rest is the room above it. The map is drawn once, whole, as it was; then held for the move
  homeOpen = true; sheetShow(sel ? 'page' : 'home', (reduce || inst) ? 0 : 34 + FULL_MS * .5);
  const vT = restY(), lift = vT - slot.vy;
  drawNow();
  if ((reduce || inst)) { view.y = viewGoal.y = vT; applyView(true); dropGhost(); drawNow(); if (then) then(); return; }
  holdT = true;
  const R = { left: r0.left, top: r0.top, width: r0.width, height: r0.height }, foot = P.bottom - r0.bottom;
  const parts = [[mapEl, m0, { left: m1.left, top: m1.top - lift }, m1], ...els.map((el, k) => [el, e0[k], e1[k], e1[k]])];
  if (ghost) parts.push([ghost, sr, { left: sr.left, top: sr.top + foot }, P, fadeAt(.3, .62, false)]);
  const K = moveKF(R, P, P, parts);
  portal.style.borderRadius = cornersFor(R, P); glide(true); atKF(K, 0);
  fullAnim = { cancel() {} };
  afterPaint(() => {
    const a = playKF(K, tm({ duration: FULL_MS, easing: 'linear' }));
    portal.animate([{ borderRadius: cornersFor(R, P) }, { borderRadius: '0px / 0px' }], tm({ duration: FULL_MS, easing: CARRY })); portal.style.borderRadius = '0px';
    fullAnim = a; a.onfinish = () => {
      fullAnim = null; if (dead || !portal) return;
      // the swap: the view takes the layer's offset, drawn in the frame the layer comes home
      [portal, pin, ...K.els].forEach((n) => { n.getAnimations().forEach((x) => x.cancel()); n.style.transform = ''; }); portal.style.borderRadius = ''; glide(false); dropGhost();
      view.y = viewGoal.y = vT; applyView(true); holdT = false; drawNow();
      if (then) then();
    };
  });
}
function exitFullN(then) {
  window.dispatchEvent(new CustomEvent('ff:hold', { detail: false }));
  // what's open goes with the frame: its sheet sinks first, and the map comes back to the card as the card had it
  if (sheetOpen || sel || homeOpen) { trail = []; homeOpen = false; listFocus = null; const b = before; if (sel) { sel = null; ctx = null; wrap.classList.remove('has-sel'); } before = null; if (b) { fitShift = b.shift; controls.target.copy(b.tg); camera.position.copy(b.pos); cam.on = false; controls.update(); } }
  const done = () => {
    fullAnim = null; if (dead) return;
    closeSheet(true);
    if (spacer) spacer.replaceWith(wrap); spacer = null; if (portal) portal.remove(); portal = null; pin = null;
    full = false; chrome(false); sheetLift = 0; reading(false); size();
    if (slot) { fitShift = slot.shift; view.x = viewGoal.x = slot.vx; view.y = viewGoal.y = slot.vy; applyView(true); }
    [mapEl, ...flipN()].forEach((n) => { n.getAnimations().forEach((x) => x.cancel()); n.style.transform = ''; }); dropGhost(); holdT = false; drawNow();
    viewTween = null; slot = null; if (then) then();
  };
  if ((reduce || inst) || !slot || !spacer || !portal) { done(); return; }
  // nothing heavy this frame: the map keeps the picture it has (held), and the view goes back to the card's only in the
  // frame the card is put back, so the layer carries the difference
  holdT = true;
  const P = portal.getBoundingClientRect(), sp = spacer.getBoundingClientRect(), m1 = mapEl.getBoundingClientRect(), R = { left: sp.left, top: sp.top, width: sp.width, height: sp.height }, foot = P.bottom - sp.bottom;
  const lx = view.x - slot.vx, ly = view.y - slot.vy, els = flipN(), e1 = rectsOf(els);
  const parts = [[mapEl, m1, { left: sp.left + slot.m[0] + lx, top: sp.top + slot.m[1] + ly }, m1], ...els.map((el, k) => [el, e1[k], { left: sp.left + slot.e[k][0], top: sp.top + slot.e[k][1] }, e1[k]])];
  // the strip comes back up with the frame's foot
  dropGhost(); if (slot.s) { const t = document.createElement('div'); t.innerHTML = stripHTML(); ghost = t.firstElementChild; ghost.removeAttribute('data-ffhome'); ghost.classList.add('ffs-ghost'); Object.assign(ghost.style, { position: 'absolute', left: '0', top: '0', width: `${slot.s[2]}px`, height: `${slot.s[3]}px`, margin: '0', zIndex: '7', pointerEvents: 'none', opacity: '0' }); pin.appendChild(ghost);
    const at = { left: sp.left + slot.s[0], top: sp.top + slot.s[1] }; parts.push([ghost, { left: at.left, top: at.top + foot }, at, P, fadeAt(.38, .72, true)]); }
  const K = moveKF(P, R, P, parts), SINK = 300;
  fullAnim = { cancel() {} };
  afterPaint(() => {
    // the sheet sinks under the foot first; the frame follows it in
    if (sheetOpen) { const { sh, scrim } = shEls(), y0 = curY(sh), h = sh.offsetHeight; stopAll(sh, scrim); sh.style.transform = `translateY(${h}px)`; scrim.style.opacity = '0';
      sh.animate([{ transform: `translateY(${y0}px)` }, { transform: `translateY(${h}px)` }], tm({ duration: SINK, easing: 'cubic-bezier(.4, 0, 1, 1)' })); }
    glide(true);
    const a = playKF(K, tm({ duration: FULL_MS * .9, delay: SINK * .4, easing: 'linear', fill: 'backwards' }));
    portal.animate([{ borderRadius: '0px / 0px' }, { borderRadius: cornersFor(R, P) }], tm({ duration: FULL_MS * .9, delay: SINK * .4, easing: CARRY, fill: 'both' }));
    fullAnim = a; a.onfinish = done;
  });
}
const leave = () => window.dispatchEvent(new CustomEvent('ff:leave'));
fullBtn.onclick = () => { if (full && !wide && LAYER) leave(); else setFull(!full); };
// Escape closes what's open first, then full screen; the arrows step while something is open
const onKey = (e) => { if (e.key === 'Escape') { if (full && !wide) { if (sel || detent !== 'dock') closeAll(); else if (LAYER) leave(); else setFull(false); } else if (sel || sheetOpen) closeAll(); else if (full) setFull(false); } else if (sel && (e.key === 'ArrowRight' || e.key === 'ArrowLeft') && !e.target.closest?.('input, textarea')) { e.preventDefault(); step(e.key === 'ArrowRight' ? 1 : -1); } };
window.addEventListener('keydown', onKey);
q('zfit').onclick = () => { moved = false; before = null; closeAll(); home(); };
controls.addEventListener('start', () => { cam.on = false; moved = true; limits(); });

// ---------- roaming: a scroll zooms in toward the point under the pointer (out from it) and eases there, so a mouse's
// notches glide and a trackpad's stream follows; a finger moves the map round the floor, the point it took hold of
// staying under it, gliding on a little after a flick; a pinch zooms toward the fingers (always: the controls' own pinch
// aims by the page's coordinates, off by however far the page under the read's layer is scrolled). A mouse drag is the
// controls' turn. What's looked at never leaves the floor, and the further out you are the nearer its middle it's kept,
// so the map can't be lost
const ray = new THREE.Raycaster(), ndc = new THREE.Vector2(), floorAt = new THREE.Plane(new THREE.Vector3(0, 1, 0), 0), hitV = new THREE.Vector3();
// where the pointer meets the floor, at the height of what's looked at (nothing when it looks out over the horizon)
function floorHit(cx, cy, out) {
  const r = cv.getBoundingClientRect(); ndc.set((cx - r.left) / r.width * 2 - 1, -(cy - r.top) / r.height * 2 + 1);
  camera.updateMatrixWorld(); ray.setFromCamera(ndc, camera); floorAt.constant = -controls.target.y;
  return !!ray.ray.intersectPlane(floorAt, out) && out.distanceTo(camera.position) < camDist() * 8;
}
const shift = (dx, dz) => { controls.target.x += dx; controls.target.z += dz; camera.position.x += dx; camera.position.z += dz; };
const tgR = () => Math.hypot(controls.target.x, controls.target.z);
// a move can't take what's looked at off the floor, and once you're further out than the whole map's view, the further
// out the nearer the middle it's kept (r0: how far out it was before the move, so being out there already never snaps
// it in); a zoom out draws it in by pull
function keepIn(r0, pull = 0) {
  const lim = FR * 1.02 * clamp(1 - (camDist() - restD) / (restD * .8), .3, 1), r = tgR(), cap = r0 > lim ? Math.max(lim, r0 - (r0 - lim) * clamp(pull)) : lim;
  if (r > cap) shift(controls.target.x * (cap / r - 1), controls.target.z * (cap / r - 1));
}
// zoom by s about a point on the floor: that point keeps its place on screen
function zoomAbout(A, s, pull) { const r0 = tgR(); camera.position.sub(A).multiplyScalar(s).add(A); controls.target.sub(A).multiplyScalar(s).add(A); keepIn(r0, pull); }
// the fingers down on the map, where each is (on the screen), and how far apart a pinch's two were last
const fingers = new Map(); let pinchD = 0;
function pinchTo() {
  const [a, b] = [...fingers.values()], d = Math.max(24, Math.hypot(a.x - b.x, a.y - b.y)), d0 = camDist(), s = clamp(d0 * pinchD / d, controls.minDistance, controls.maxDistance) / d0; pinchD = d;
  if (Math.abs(s - 1) < 1e-4) return;
  if (!floorHit((a.x + b.x) / 2, (a.y + b.y) / 2, hitV)) hitV.copy(controls.target);
  zoomAbout(hitV, s, s > 1 ? (s - 1) * 3 : 0); moved = true; lastTouch = performance.now();
}
stage.addEventListener('wheel', (e) => {
  // the reading's card and the buttons over the map keep their own scroll
  if (!mapEl.contains(e.target)) return;
  if (!roam() && !e.ctrlKey && !e.metaKey) { e.stopPropagation(); return; }
  e.preventDefault(); e.stopPropagation();
  // a mouse's notch is about 100; a trackpad pinch comes as ctrl and small steps
  const px = e.deltaMode === 1 ? 16 : e.deltaMode === 2 ? H : 1;
  zoom.by = clamp(zoom.by + e.deltaY * px * (e.ctrlKey ? .012 : .0024), -1.6, 1.6);
  if (!floorHit(e.clientX, e.clientY, zoom.at)) zoom.at.copy(controls.target);
  cam.on = false; drift.on = false; moved = true; lastTouch = performance.now();
}, { capture: true, passive: false });
// a press: a mouse's turns the map (the controls'), its hand closing; roaming, a finger's is the map's own move, and a
// second finger starts a pinch (the controls turn the map with the two; the zoom is ours)
stage.addEventListener('pointerdown', (e) => {
  if (e.target !== cv) return;
  if (e.pointerType !== 'touch') { cv.style.cursor = 'grabbing'; return; }
  fingers.set(e.pointerId, { x: e.clientX, y: e.clientY });
  drift.on = false; grab.id = -1; grab.on = false;
  if (fingers.size === 2) { const [a, b] = [...fingers.values()]; pinchD = Math.hypot(a.x - b.x, a.y - b.y); cam.on = false; zoom.by = 0; return; }
  if (!roam() || fingers.size > 1 || !floorHit(e.clientX, e.clientY, grab.at)) return;
  Object.assign(grab, { id: e.pointerId, x: e.clientX, y: e.clientY, t: performance.now(), vx: 0, vz: 0 });
}, { capture: true });
cv.addEventListener('pointermove', (e) => {
  const f = fingers.get(e.pointerId);
  if (f) { f.x = e.clientX; f.y = e.clientY; if (pinchD && fingers.size === 2) { pinchTo(); return; } }
  if (e.pointerId !== grab.id) return;
  if (!grab.on) { if (Math.hypot(e.clientX - grab.x, e.clientY - grab.y) < 4) return; grab.on = true; cam.on = false; moved = true; }
  if (!floorHit(e.clientX, e.clientY, hitV)) return;
  const dx = grab.at.x - hitV.x, dz = grab.at.z - hitV.z, now = performance.now(), span = Math.max(4, now - grab.t), k = Math.min(1, span / 50);
  const r0 = tgR(); shift(dx, dz); keepIn(r0);
  grab.vx += (dx / span * 1000 - grab.vx) * k; grab.vz += (dz / span * 1000 - grab.vz) * k; grab.t = now; lastTouch = now;
});
// a flick carries on, never more than about a third of the view; a drag that had stopped before it let go stays where it is
const letGo = (e) => { if (e.pointerType !== 'touch') cv.style.cursor = 'grab'; fingers.delete(e.pointerId); if (fingers.size < 2) pinchD = 0; if (e.pointerId !== grab.id) return; grab.id = -1; if (!grab.on) return; grab.on = false;
  const fresh = performance.now() - grab.t < 80, v = Math.hypot(grab.vx, grab.vz), k = fresh ? Math.min(1, camDist() * 1.5 / Math.max(1e-6, v)) : 0;
  drift.vx = grab.vx * k; drift.vz = grab.vz * k; drift.on = !reduce && v * k > .05; };
cv.addEventListener('pointerup', letGo); cv.addEventListener('pointercancel', letGo);

// ---------- picking: tap the nearest card or body; what's faded back is still there to tap, a little harder to hit
const v3 = new THREE.Vector3();
function screenOf(x, y, z) { v3.set(x, y, z).project(camera); return [(v3.x + 1) / 2 * W, (1 - v3.y) / 2 * H, v3.z]; }
let down = null;
['pointerdown', 'wheel', 'keydown', 'touchstart'].forEach((ev) => stage.addEventListener(ev, () => { lastTouch = performance.now(); }, { passive: true }));
cv.addEventListener('pointerdown', (e) => { down = { x: e.clientX, y: e.clientY, t: performance.now() }; });
// a tap acts on its click, not on the finger lifting: whatever it opens (the reading sheet, full screen) is up before the
// click the browser sends after a touch, so that click can't land on the sheet's scrim and shut it again
let tap = null;
cv.addEventListener('pointerup', (e) => { if (!down) return; const travel = Math.hypot(e.clientX - down.x, e.clientY - down.y), quick = performance.now() - down.t < 400; down = null; tap = travel > 8 || !quick ? null : { touch: e.pointerType !== 'mouse', at: performance.now() }; });
cv.addEventListener('click', (e) => { const t = tap; tap = null; if (!t || performance.now() - t.at > 700) return;
  const r = cv.getBoundingClientRect(), lb = pickLabel(e.clientX, e.clientY), hit = lb >= 0 ? lb : pick(e.clientX - r.left, e.clientY - r.top, t.touch ? 34 : 22);
  // a tap on nothing puts everything away
  if (hit < 0) { if (sel || homeOpen) closeAll(); return; }
  go(N[hit]); });
// a shown label under the finger (with a little room round it) picks its thing
function pickLabel(cx, cy) { let hit = -1; lbs.forEach((e, i) => { if (hit >= 0 || +e.style.opacity < .5) return; const b = e.getBoundingClientRect(); if (cx >= b.left - 6 && cx <= b.right + 6 && cy >= b.top - 6 && cy <= b.bottom + 6) hit = i; }); return hit; }
cv.addEventListener('pointermove', (e) => { if (e.pointerType !== 'mouse' || e.buttons) { hoverCand = -1; return; } const r = cv.getBoundingClientRect(), h = pick(e.clientX - r.left, e.clientY - r.top); if (h !== hoverCand) { hoverCand = h; hoverT = performance.now(); } cv.style.cursor = h >= 0 ? 'pointer' : 'grab'; });
cv.addEventListener('pointerleave', () => { hoverCand = -1; });
function pick(mx, my, reach = 22) { let bi = -1, bd = reach; N.forEach((x) => { if (!x.scr || x.scr[2] > 1 || x.app < .5) return; const card = x.kind === 'post', dx = mx - x.scr[0], dy = (my - x.scr[1]) / (card ? 1.25 : 1); const d = Math.hypot(dx, dy) - (x.scrR || 0) * (card ? 1.05 : .85) + (x.kind === 'bit' ? 4 : 0) + (1 - clamp(x.vis)) * 10; if (d < bd) { bd = d; bi = x.i; } }); return bi; }

// ---------- labels (HTML, crisp): the account and every planet as room allows; a post, a passing thing and what a post
// holds when it's pointed at or open. A planet shows its name; pointed at or open, its result too
const lbs = N.map((x) => { const e = document.createElement('div'); e.className = `ff-lb ${x.kind}`; e._red = x.kind === 'post' ? isTop(x) : x === sun || !!x.hot; e.classList.toggle('red', e._red);
  if (x.kind === 'post') e.innerHTML = `<b>${esc(x.name)}</b><i>${x.date} · ${feeds(x) ? 'Feeding' : 'Fading'} · ${place(x)}</i>`;
  else if (x === sun) e.innerHTML = `<b>${esc(x.name)}</b><i>@${esc(D.handle)}</i>`;
  else e.innerHTML = `<b>${svg(x.icon)}${esc(x.name)}</b><i>${x.feed} of ${pl(x.n, 'post')} landed</i>`;
  e.style.opacity = 0; labelsEl.appendChild(e); return e; });
// each wedge names itself at its outer edge; the orbits name themselves in the gap at the back
const capOf = (text, r, a, cls) => { const e = document.createElement('div'); e.className = `ff-gl ${cls}`; e.textContent = text; e.style.opacity = 0; labelsEl.appendChild(e); return { e, r, a }; };
// v12: each section's name stands large at the rim of its sector with its posts, always shown (never given up to the
// labels' crowding); a tap on it picks the section
function sectCap(k) {
  const w = WKB[`W${k}`], e = document.createElement('div'); e.className = 'ff-gl cat sx'; e.innerHTML = `<b>${esc(CATS[k].name)}</b><i>${pl(w.ps.length, 'post')}</i>`; e.style.opacity = 0; e.setAttribute('role', 'button'); e.tabIndex = 0;
  e.addEventListener('click', (ev) => { ev.stopPropagation(); pickSection(k); }); e.addEventListener('keydown', (ev) => { if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); pickSection(k); } });
  labelsEl.appendChild(e); return { e, r: LBL_R, a: WEDGE[k].mid, sx: k };
}
const caps = SECT ? usedCats.map(sectCap) : [...usedCats.map((k) => capOf(CATS[k].name, RIM, WEDGE[k].mid, 'cat')), ...ORB.map((r, o) => (has(o) || hasBits(o) ? capOf(ORBIT_LONG[o], r, -Math.PI / 2, 'orb') : null)).filter(Boolean)];
const toolsEl = host.querySelector('.ff-tools.ov');
const axHi = q('axisHi'), axMid = q('axisMid'), axLo = q('axisLo'); axMid.textContent = `— ${Who} usual post`;
keyEl.innerHTML = KEY;

// ---------- frame
const C = new THREE.Color(), C2 = new THREE.Color();
const IDLE_MS = 12000;
let t = 0, t0 = performance.now(), lastT = t0, visible = true, idle = false, lookUp = null, fCat = null;
// arrival, the way the feed went up: the rings sweep in and the account rises; then post by post round the ring each
// card rises and sends its threads in, and each thing appears as its first thread reaches it
const RISE = 760 * TEMPO, SWEEP = 1150 * TEMPO, POST_T0 = 620 * TEMPO, POST_STEP = 58 * TEMPO, GROW = 640 * TEMPO;
posts.forEach((p, k) => { p.at = POST_T0 + k * POST_STEP; });
bodies.forEach((b) => { b.at = b === sun ? 140 * TEMPO : Math.min(POST_T0 + np * POST_STEP, ...b.ps.map((p) => p.at + RISE * .35 + GROW * .8)); });
const DONE = POST_T0 + np * POST_STEP + RISE + GROW;
// the replay: once it has all arrived, the feed plays back post by post in the order it went up, each lifting a touch as
// light runs in along its threads to the things it's made of, so the threads read as the feed flowing into them
const BEAT = 1700 * TEMPO;
const ease = (u) => 1 - Math.pow(1 - u, 3);
// labels and captions: frames clear before showing, frames crowded before hiding
const HOLD_IN = 6, HOLD_OUT = 18;
// how many things are named at rest: fewer the longer the feed (14 beside the reading and 8 on a phone at 30 posts; 8 and 4 at 100)
let REST_L = 8; const restL = () => Math.round((wide ? 14 : 8) * Math.min(1, Math.sqrt(30 / np)));
const OFF6 = [0, -500, 0, 0, -500, 0], OFF3 = [0, -500, 0];
const so = (e, v) => { if (e._so !== v) { e._so = v; e.style.opacity = v; } }, st = (e, v) => { if (e._st !== v) { e._st = v; e.style.transform = v; } };
function frame(now) {
  if (dead) return;
  if (holdT || holdS) { const d = now - lastT; if (cam.on) cam.t0 += d; if (viewTween) viewTween.t0 += d; lastT = now; raf = requestAnimationFrame(frame); return; }
  const dt = Math.min(50, now - lastT) / 1000; lastT = now; if (!reduce) t += dt; const age = reduce ? 1e9 : now - t0;
  if (hoverCand !== hov && (hoverCand < 0 || now - hoverT > 50)) hov = hoverCand;
  // what's lit and what's pointed at ease in on the clock, not the frame: about a sixth of a second whatever the screen's
  // rate (a 120Hz phone the same as a 60Hz one); the scale of what's pointed at glides too, so nothing pops
  const kv = reduce ? 1 : 1 - Math.exp(-dt * 15), ks = reduce ? 1 : 1 - Math.exp(-dt * 19);
  // left alone a while, it drifts round very slowly; it never changes the tilt, the zoom or what it looks at
  const wantIdle = !reduce && !sel && hov < 0 && now - lastTouch > IDLE_MS && !cam.on;
  if (wantIdle !== idle) { idle = wantIdle; controls.autoRotate = idle; }
  if (cam.on) { const u = carry(clamp((now - cam.t0) / cam.dur)), F0 = cam.from, T = cam.to; controls.target.lerpVectors(F0.tg, T.tg, u); sph.radius = F0.r + (T.r - F0.r) * u; sph.phi = T.phi == null ? F0.phi : F0.phi + (T.phi - F0.phi) * u; sph.theta = T.th == null ? F0.th : F0.th + Math.atan2(Math.sin(T.th - F0.th), Math.cos(T.th - F0.th)) * u; sph.makeSafe(); off.setFromSpherical(sph); camera.position.copy(controls.target).add(off); if (u >= 1) cam.on = false; }
  // a scroll's zoom eases in toward where it was aimed; a flicked drag glides to a stop; a pinch is kept over the floor
  if (zoom.by && !cam.on) { const step = reduce ? zoom.by : zoom.by * (1 - Math.exp(-dt * 14)), d0 = camDist(), s = clamp(d0 * Math.exp(step), controls.minDistance, controls.maxDistance) / d0, A = zoom.at;
    zoom.by = Math.abs(zoom.by - step) < 1e-3 || Math.abs(s - 1) < 1e-5 ? 0 : zoom.by - step;
    zoomAbout(A, s, step > 0 ? step * 3 : 0); }
  if (drift.on) { const r0 = tgR(); shift(drift.vx * dt, drift.vz * dt); keepIn(r0); const k = Math.exp(-dt * 4.5); drift.vx *= k; drift.vz *= k; if (Math.hypot(drift.vx, drift.vz) < .03) drift.on = false; }
  { const r0 = tgR(); limits(); controls.update(); if (fingers.size > 1 && !cam.on) keepIn(r0); }
  if (viewTween) { const u = carry(clamp((now - viewTween.t0) / viewTween.dur)); view.x = viewGoal.x = viewTween.fx + (viewTween.tx - viewTween.fx) * u; view.y = viewGoal.y = viewTween.fy + (viewTween.ty - viewTween.fy) * u; if (u >= 1) viewTween = null; applyView(true); }
  else applyView(false);
  // as it tips toward overhead the heights fold down, so every name reads from above
  off.copy(camera.position).sub(controls.target); const tilt = Math.acos(clamp(off.y / Math.max(1e-6, off.length()), -1, 1)), tall = clamp((tilt - .12) / .45), breath = reduce ? 0 : tall;
  // what sits under the floor draws before it, so the frosted floor veils it; turned round when you look up from below
  { const under = camera.position.y < 0; if (under !== lookUp) { lookUp = under; const o = (y) => ((y >= 0) !== under ? 3 : 0); solids.forEach((x) => { x.spr.renderOrder = o(x.y); x.halo.renderOrder = o(x.y) ? 2 : 0; }); posts.forEach((p) => { p.spr.renderOrder = o(p.h) ? 5 : 0; }); stemUp.renderOrder = under ? 0 : 2; stemDn.renderOrder = under ? 2 : 0; } }
  scene.fog.density = (dark ? .14 : .1) * (.2 + .8 * tall) / Math.max(1.2, camera.position.distanceTo(controls.target));
  const big = W < 500 ? 1.42 : 1, ck = cardK();
  // what's lit: what's open, else what's pointed at; with neither, the wedge being browsed (its chip or its section of
  // the panel) lights its things and the threads into them
  // a wedge lights the way a wedge's row does: its things and the threads into them, softly
  const pickX = sel || (hov >= 0 ? N[hov] : null) || hoverX || (full && !wide ? listFocus : null), F = pickX && pickX.kind === 'wedge' ? null : focusOf(pickX);
  fCat = F ? null : pickX && pickX.kind === 'wedge' ? pickX.k : hoverCat ?? chipCat;
  if (SECT) { const pk = !!sel || chipCat != null; if (pk !== wrap._pk) { wrap._pk = pk; wrap.classList.toggle('pk', pk); } }
  // what's open and what belongs to it come forward and everything else goes well back, near gone on a phone (Oct 22:
  // the softer split let the background crowd what you'd opened). Between them, softly, what it's tied to across the
  // map: the things its posts also hold, in their own colours but quieter and unnamed, with faint threads to them (a
  // post's: the posts most alike with it, grey, at full size). The account is seen with everything, so with it open the
  // rest just stays back
  const nar = !wide; REST_L = restL(); const ties = !!F && F.x !== sun;
  const wantB = (b) => (F ? (F.t1.has(b) ? 1 : F.t2.has(b) ? (!ties ? (nar ? .2 : .32) : F.t2s.has(b) ? (nar ? .86 : .92) : nar ? .58 : .62) : b === sun ? (nar ? .4 : .45) : nar ? .07 : .08) : fCat != null ? (b === sun || b.cat === fCat ? 1 : nar ? .2 : .3) : 1);
  const wantP = (p) => (F ? (F.ps.has(p) ? 1 : F.p2.has(p) ? .5 : nar ? .08 : .1) : fCat != null ? (p.cats.has(fCat) ? 1 : nar ? .2 : .3) : 1);
  // at rest a long feed keeps its threads calm: only the covers' threads show, fainter the more there are; a mark's
  // threads come in when it's lit
  const wantL = (l) => (F ? (F.l1.has(l) ? .95 : F.lit2.has(l) ? (nar ? .26 : .3) : F.l2.has(l) ? (nar ? .08 : .12) : nar ? .004 : .008) : fCat != null ? (SECT ? 0 : l.b.cat === fCat ? (l.p.card ? .5 : .22) : nar ? .008 : .02) : SECT ? 0 : l.p.card ? (l.top ? .4 : l.fed ? .2 : .1) * (l.b.kind === 'bit' ? .7 : 1) * (dark ? .75 : 1) * LDENS : 0);
  // bodies rise in turn and breathe very faintly
  bodies.forEach((b) => { const u = clamp((age - b.at) / RISE); b.app = ease(u); b.vis += (wantB(b) - b.vis) * kv;
    const fx = b.home[0] + Math.sin(t * .3 + b.ph) * .004 * breath, fz = b.home[2] + Math.cos(t * .27 + b.ph) * .004 * breath;
    const y = b.y * tall * b.app + Math.sin(t * .45 + b.ph * 1.7) * .006 * breath, lift = b.kind === 'bit' ? .016 : (b.y >= 0 ? 1 : -1) * b.size * .5 * tall * b.app + .016 * (1 - tall);
    b.foot = [fx, .003, fz]; b.top = [fx, y, fz]; b.pos = [fx, y + lift, fz]; });
  // the floor, its lip, the orbits sweeping in, the feed's ring drawn round as its posts arrive, the account's pool,
  // ring and dial
  const sweep = clamp(age / SWEEP), inK = clamp(age / (900 * TEMPO + 1)), quiet = F ? .55 : 1, feedU = clamp((age - POST_T0) / (np * POST_STEP + RISE * .5));
  veil.material.opacity = (dark ? .27 : .2) * tall; veil.visible = tall > .01;
  const hd = heads.geometry.attributes;
  if (SECT) { const q = carry(clamp(sweep * 1.2)); SECT_LN.forEach((o) => { const on = fCat == null || o.userData.k < 0 || o.userData.k === fCat; o.material.opacity = (o.userData.k < 0 ? (dark ? .3 : .26) : (dark ? .5 : .42) * (on ? 1 : .35)) * q; });
    SECT_TN.forEach((o) => { const on = fCat == null ? 1 : o.userData.k === fCat ? 1.9 : .35; o.material.opacity = (o.userData.alt ? .05 : .026) * (dark ? 1.2 : 1) * on * q; }); }
  orbits.forEach((o, k) => { const u = carry(clamp(sweep * 1.18 - k * .09)); o.geometry.setDrawRange(0, Math.ceil((RN + 1) * u)); o.material.opacity = SECT ? 0 : (k === 0 ? 1 : k === 1 ? .72 : .55) * (has(k) || hasBits(k) ? 1 : .45) * (dark ? .26 : .22) * quiet;
    // the bright head riding the front of each orbit as it draws, gone once the ring is closed
    const p = ringPt(ORB[k], u * RN, .004); hd.position.array.set([p.x, p.y, p.z], k * 3); paint(C, k === 0 ? RED : INK, SECT ? 0 : u > 0 && u < 1 ? 1 : 0).toArray(hd.color.array, k * 3); });
  { const u = carry(sweep), p = ringPt(.27, u * RN, .005); hd.position.array.set([p.x, p.y, p.z], ORB.length * 3); paint(C, RED, u > 0 && u < 1 ? 1 : 0).toArray(hd.color.array, ORB.length * 3); }
  { const p = arcPt(FEED, feedU, .005); hd.position.array.set([p.x, p.y, p.z], (ORB.length + 1) * 3); paint(C, RED, feedU > 0 && feedU < 1 ? 1 : 0).toArray(hd.color.array, (ORB.length + 1) * 3); }
  hd.position.needsUpdate = true; hd.color.needsUpdate = true; heads.visible = sweep < 1 || feedU < 1;
  feedRing.geometry.setDrawRange(0, Math.ceil((RN + 1) * feedU)); feedRing.material.opacity = (dark ? .5 : .42) * (F ? .7 : 1);
  coreMark.geometry.setDrawRange(0, Math.ceil((RN + 1) * carry(sweep))); coreMark.material.opacity = .85 * sun.vis; dial.material.opacity = .55 * inK * sun.vis * (.4 + .6 * tall);
  pool.material.opacity = (dark ? .16 : .1) * sun.app * (.35 + .65 * sun.vis); pool.position.y = .0015;
  // the account and the planets: on stems, a faint red halo on what landed, a soft shadow under each on the floor
  const su = stemUp.geometry.attributes, sd = stemDn.geometry.attributes, mk = marks.geometry.attributes; let ku = 0, kd = 0;
  const stem = (x, base, it) => { const up = x.y >= 0, at = up ? su : sd, j = up ? ku++ : kd++; at.position.array.set([...x.foot, ...x.top], j * 6); paint(C2, base, it * .25).toArray(at.color.array, j * 6); paint(C2, base, it).toArray(at.color.array, j * 6 + 3); };
  solids.forEach((b, k) => { const isSel = b === sel, isHov = b.i === hov, on = b.vis >= .55, tie = ties && F.t2.has(b) && !isHov, red = (b === sun || b.hot) && on && !tie, inT = !!F && F.t1.has(b) && !isSel;
    // what recedes also shrinks, by how far it has gone back (more on a phone); what belongs to the open one stands a touch bigger
    b.km = (b.km ?? 1) + ((isSel ? 1.42 : isHov ? 1.12 : inT ? (nar ? 1.12 : 1.06) : 1) - (b.km ?? 1)) * ks;
    const s = b.size * big * Math.max(.001, b.app) * b.km * (1 - (1 - clamp(b.vis)) * (nar ? .38 : .12)) * (1 + .022 * Math.sin(t * 1.6 + b.ph) * breath);
    const map = !on ? b.txDim : tie && b.txTie ? b.txTie : b.tx; if (b.spr.material.map !== map) { b.spr.material.map = map; b.spr.material.needsUpdate = true; }
    b.cur = s; b.spr.position.set(...b.pos); b.spr.scale.set(s, s, 1); b.spr.material.opacity = (on ? b.vis : Math.max(nar && F ? .2 : F ? .26 : .42, b.vis)) * Math.min(1, b.app * 1.6);
    // the open one wears a halo, wider and warmer on a phone, so it's found at a glance
    const glow = tie ? 0 : b === sun ? (dark ? .2 : .14) * (F && !isSel && nar ? .4 : 1) : b.hot ? (dark ? .16 : .12) * (isSel ? (nar ? 2.2 : 1.6) : 1) : isSel ? (dark ? .12 : .08) * (nar ? 1.8 : 1) : 0, hs = s * (b === sun ? 2.6 : isSel && nar ? 4 : 3.2); b.halo.position.set(...b.pos); b.halo.scale.set(hs, hs, 1); b.halo.material.opacity = glow * b.vis * b.app * (on ? 1 : 0); b.halo.material.color.copy(b.hot || b === sun ? RED : INK); b.halo.visible = glow > 0 && on;
    const sr = s * (.62 + .25 * Math.abs(b.top[1])); b.shade.position.set(b.foot[0], .002, b.foot[2]); b.shade.scale.set(sr, sr, 1); b.shade.material.opacity = (dark ? .5 : .16) * (1 - clamp(Math.abs(b.top[1]) * .7, 0, .7)) * b.app * tall * (.4 + .6 * b.vis);
    stem(b, red ? RED : INK, (red ? (b === sun ? .85 : .65) : .38) * (.15 + .85 * b.vis) * b.app * tall);
    mk.position.array.set(b.foot, k * 3); paint(C2, red ? RED : INK, (red ? .75 : .38) * (.25 + .75 * b.vis) * b.app * tall).toArray(mk.color.array, k * 3); });
  for (let k = ku; k < su.position.count / 2; k++) su.position.array.fill(0, k * 6, k * 6 + 6);
  for (let k = kd; k < sd.position.count / 2; k++) sd.position.array.fill(0, k * 6, k * 6 + 6);
  [su, sd, mk].forEach((a) => { a.position.needsUpdate = true; a.color.needsUpdate = true; }); stemUp.visible = stemDn.visible = marks.visible = tall > .01;
  // the passing things: small hexes on the floor, red for a post in its format's top quarter, grey for one that faded
  { const ba = bitPts.geometry.attributes;
    bits.forEach((b, j) => { const p0 = b.ps[0], base = isTop(p0) && b.vis > .6 && !(ties && F.t2.has(b) && b.i !== hov) ? RED : INK, f = (isTop(p0) ? 1 : feeds(p0) ? .72 : .4) * (.12 + .88 * b.vis) * b.app * (b === sel || b.i === hov ? 1.5 : 1);
      ba.position.array.set(b.pos, j * 3); paint(C, base, f).toArray(ba.color.array, j * 3); });
    ba.position.needsUpdate = true; ba.color.needsUpdate = true; bitPts.material.size = .085 * (W < 500 ? 1.25 : 1); }
  // the feed: every post a card standing on its stem above the ring (or hanging under it), a tick on the ring at its foot.
  // The open post stands tallest; the open thing's posts stand out; the replay lifts each a touch in turn
  const beat = !reduce && !F && age > DONE ? (age - DONE) / BEAT : -1, playK = beat >= 0 ? Math.floor(beat) % CARDP.length : -1, playU = beat >= 0 ? beat % 1 : 0;
  { const ps = postStem.geometry.attributes, tk = ticks.geometry.attributes, pm = postMarks.geometry.attributes;
    posts.forEach((p, k) => { const u = clamp((age - p.at) / RISE); p.app = ease(u); p.vis += (wantP(p) - p.vis) * kv;
      p.pulse = playK >= 0 && CARDP[playK] === p ? playU : -1;
      const isSel = p === sel, isHov = p.i === hov, inF = !!F && F.ps.has(p) && F.x !== p, in2 = !!F && F.p2.has(p), bump = p.pulse >= 0 ? Math.sin(Math.PI * p.pulse) : 0;
      // a mark stands its cover only while it's lit or open; on a phone what isn't lit draws back to a small card (a post
      // alike with the open one keeps its size, grey)
      const show = p.card || isSel || isHov || inF || in2 || (!!F && F.ps.has(p)), back = !!F && !F.ps.has(p) && !in2;
      // the open post: a cover big enough to find whatever the feed's length (a 100-post ring makes small cards), lifted
      // clear of its neighbours; what isn't lit draws back further
      const cw0 = CW * ck, goal = show ? (isSel ? Math.max(cw0 * 2.3, .5 * ck) : isHov ? Math.max(cw0 * 1.5, .3 * ck) : cw0 * (inF ? (p.card ? 1.45 : 1.15) : 1)) * (1 + .1 * bump) * (back ? (nar ? .62 : .72) : 1) : 0;
      p.sc += (goal - p.sc) * ks; const s = Math.max(1e-4, p.sc * p.app), up = p.h >= 0;
      p.lf = (p.lf ?? 0) + ((isSel ? .16 : inF ? .08 : 0) - (p.lf ?? 0)) * ks;
      const y = (p.h + (up ? p.lf : -p.lf)) * tall * p.app, edge = y + (up ? .012 : -.012), [x, , z] = p.ring;
      p.spr.center.set(.5, up ? 0 : 1); p.spr.position.set(x, edge, z); p.spr.scale.set(s, s * 1.25, 1);
      const on = p.vis >= .55, map = on ? p.tex : p.texDim; if (p.spr.material.map !== map) { p.spr.material.map = map; p.spr.material.needsUpdate = true; }
      p.spr.material.opacity = Math.min(1, p.app * 1.6) * (on ? 1 : in2 ? .92 : nar && F ? .3 + .5 * p.vis : .6 + .4 * p.vis); p.spr.visible = p.sc > .004;
      p.cur = s; p.pos = [x, edge + (up ? 1 : -1) * s * .625, z];
      if (!p.card) { const m0 = 1 - clamp(p.sc / (CW * ck * .5)); pm.position.array[k * 3] = x; pm.position.array[k * 3 + 1] = edge; pm.position.array[k * 3 + 2] = z; paint(C2, isTop(p) && p.vis > .6 ? RED : INK, (isTop(p) ? .85 : feeds(p) ? .62 : .32) * (.15 + .85 * p.vis) * p.app * m0).toArray(pm.color.array, k * 3); if (p.pulse >= 0) p.pos[1] = edge; }
      const base = isTop(p) ? RED : INK, it = (isTop(p) ? .75 : feeds(p) ? .5 : .3) * (.2 + .8 * p.vis) * p.app;
      ps.position.array.set([x, .003, z, x, edge, z], k * 6);
      if (isTop(p) && p.vis > .6 && tall > .5) { RED.toArray(ps.color.array, k * 6); RED.toArray(ps.color.array, k * 6 + 3); } else { paint(C2, INK, it * .35 * tall).toArray(ps.color.array, k * 6); paint(C2, INK, it * tall).toArray(ps.color.array, k * 6 + 3); }
      (isTop(p) && p.vis > .6 ? C2.copy(RED) : paint(C2, INK, it * (1 + bump))).toArray(tk.color.array, k * 6); C2.toArray(tk.color.array, k * 6 + 3); });
    [ps, tk].forEach((a) => { a.position.needsUpdate = true; a.color.needsUpdate = true; }); postStem.visible = tall > .01; ticks.visible = feedU > 0;
    if (DENSE) { pm.position.needsUpdate = true; pm.color.needsUpdate = true; postMarks.material.size = .05 * (W < 500 ? 1.3 : 1); } postMarks.visible = DENSE; }
  // the loom: a thread from every post to every thing in it, bowing in round the account. Each draws in from its post as
  // the post arrives, a light at its head. What's open has its own threads drawn bold with light running in along them
  // again and again; the replay sends light in along each post's threads in turn
  { const hp = loomHeads.geometry.attributes, flow = (now - openAt) / (1700 * TEMPO), W4 = TSW * 4; let hiN = 0; if (F !== hiF) fillHi(F);
    LOOM.forEach((l) => { const p = l.p, j = l.j, o = l.ts; l.vis += (wantL(l) - l.vis) * kv;
      const grow = reduce ? 1 : clamp((age - p.at - RISE * .35) / GROW), prim = !!F && F.l1.has(l), sec = !prim && !!F && F.lit2.has(l); if (prim) hiN++;
      let h = -1, amp = 0;
      if (grow < 1) { h = grow; amp = grow > 0 ? 1 : 0; }
      else if (!reduce && prim) { h = ((flow + j * .137) % 1) * 1.3 - .15; amp = .9; }
      // the threads to its strongest ties carry a softer, slower light, out of sync with the open one's, never red
      else if (!reduce && sec) { h = ((flow * .72 + j * .31) % 1) * 1.3 - .15; amp = .5; }
      else if (p.pulse >= 0) { h = p.pulse * 1.3 - .1; amp = .7; }
      // the open thing's own threads breathe a little under the light running in along them
      TS[o] = l.vis * (l.top && !prim ? 1.25 : prim && !reduce ? .9 + .1 * Math.sin(t * 2.2 + j * .7) : 1); TS[o + 1] = l.top && prim ? 1 : 0; TS[o + 2] = h; TS[o + 3] = amp;
      TS[o + W4] = grow; TS[o + W4 + 1] = l.top && !sec ? 1 : 0; TS[o + W4 + 2] = prim ? 1 : 0;
      // red only at full strength (an open top-quarter thread, or the light running along one); at rest a thread is grey,
      // because what recedes goes grey, never pale pink. A small bright head rides the light
      const headOn = h > 0 && h < 1 && amp > 0 && (l.vis > .02 || grow < 1); if (headOn) threadTo(hp.position.array, j * 3, l.A, l.B, h); else hp.position.array.set(OFF3, j * 3);
      (headOn && l.top && (prim || grow < 1) ? C.copy(RED) : paint(C, INK, headOn ? amp * Math.sin(Math.PI * h) * .95 * Math.min(1, l.vis * 2 + (grow < 1 ? 1 : 0)) : 0)).toArray(hp.color.array, j * 3); });
    tsTex.needsUpdate = true; loomHi.o.visible = hiN > 0; hp.position.needsUpdate = true; hp.color.needsUpdate = true; }
  // dotted arcs from the open planet to the planets that did alike, drawn out from it, a slow shimmer along them
  { const kd2 = kinDots.geometry.attributes, kin = [], grow = clamp((now - openAt) / (1000 * TEMPO));
    for (let q2 = 0; q2 < 3; q2++) { const o = kin[q2]; for (let d = 0; d < KDOT; d++) { const idx = (q2 * KDOT + d) * 3, u = (d + .5) / KDOT; if (!o || u > grow) { kd2.position.array.set(OFF3, idx); continue; }
      const A = sel.pos, B = o.pos, lift = .18 + .3 * Math.hypot(A[0] - B[0], A[2] - B[2]); kd2.position.array.set(arcAt(A, B, u, lift), idx); (o.hot && sel.hot ? C.copy(RED) : paint(C, INK, .55 + .3 * Math.sin(t * 3 - u * 9))).toArray(kd2.color.array, idx); } }
    kd2.position.needsUpdate = true; kd2.color.needsUpdate = true; }
  renderer.render(scene, camera);
  // labels: what's open first, then what's pointed at, the account, then planets by size (or, with something open, the
  // things it reaches); a passing thing when its wedge is browsed or something open reaches it. No label covers a card or
  // a body but its own: a name sits beside it on the side away from the core, else the other side, over or under it;
  // each keeps the place it had while that stays clear
  const cand = [], discs = [], bottom = vis.y1 - (wide ? 40 : 10), topB = vis.y0 + hudB, L0 = vis.x0, L1 = vis.x1;
  N.forEach((x) => { const [sx, sy, sz] = screenOf(...x.pos); x.scr = [sx, sy, sz]; x.scrR = Math.abs(screenOf(x.pos[0] + (x.cur || x.size || .02) * .5, x.pos[1], x.pos[2])[0] - sx); const e = lbs[x.i]; let pri = 99;
    if (sz < 1 && x.app > .3 && x.kind !== 'bit' && x.vis > .5) discs.push([sx, sy, x.scrR * (x.kind === 'post' ? 1.2 : .86) + 2, x.i]);
    if (x.app > .6) {
      if (x === sel) pri = 0; else if (x.i === hov) pri = .4;
      else if (x.kind === 'post') pri = 99;
      else if (x === sun) pri = SECT || W < 500 || (F && !F.t1.has(x)) ? 99 : 1;
      // with something open only what belongs to it is named (Oct 22: naming what it's seen with crowded the open one)
      else if (F) pri = F.t1.has(x) ? .3 : 99;
      // what a post turns on is named before what only signals; bigger first within each
      else if (x.kind === 'planet') pri = fCat == null || x.cat === fCat ? (x.role === 'signals' ? 1.9 : 1.5) + (1 - Math.min(1, x.n / 8)) : 99;
      else if (x.kind === 'bit' && fCat === x.cat) pri = 4.6;
    }
    if (sz > 1 || pri === 99 || sx < L0 - 40 || sx > L1 + 40 || sy < topB || sy > bottom) { so(e, 0); e._shown = false; e._ok = 0; return; } cand.push({ x, e, sx, sy, pri }); });
  cand.forEach((c) => { if (c.e._on) c.pri -= .3; }); cand.sort((a, b) => a.pri - b.pri); const boxes = []; lbs.forEach((e) => { e._on = false; });
  const hitsBox = (box) => [toolsBox, ...boxes].some((b) => box[0] < b[0] + b[2] && box[0] + box[2] > b[0] && box[1] < b[1] + b[3] && box[1] + box[3] > b[1]);
  const hitsDisc = (box, own) => discs.some(([cx, cy, r, id]) => id !== own && Math.hypot(cx - Math.max(box[0], Math.min(cx, box[0] + box[2])), cy - Math.max(box[1], Math.min(cy, box[1] + box[3]))) < r);
  if (!sel && tall > .5 && W >= 500) [axHi, axMid, axLo].forEach((e) => { const m = /translate3d\(([-\d.]+)px, ([-\d.]+)px/.exec(e.style.transform || ''); if (m && +e.style.opacity > .1) boxes.push([+m[1] - 2, +m[2] - 2, (e._w || (e._w = e.offsetWidth)) + 4, 16]); });
  caps.forEach((c) => { const [sx, sy, sz] = screenOf(Math.cos(c.a) * c.r, 0, Math.sin(c.a) * c.r), w = c.e._w || (c.e._w = c.e.offsetWidth), h = c.e._h || (c.e._h = c.e.offsetHeight), bx = sx - w / 2, by = sy - h / 2, box = [bx - 2, by - 2, w + 4, h + 4];
    if (c.sx != null) { // a section's name: kept in view at the rim, dimmed while another is picked, never dropped for a clash
      const cx = clamp(bx, L0 + 4, Math.max(L0 + 4, L1 - w - 4)), cy = clamp(by, topB + 2, Math.max(topB + 2, bottom - h - 2)), back = sz > 1;
      c.e.classList.toggle('on', fCat === c.sx || (fCat == null && secTab === c.sx)); so(c.e, back ? 0 : +((F ? .45 : fCat == null || fCat === c.sx ? 1 : .4) * clamp(sweep * 1.4 - .3)).toFixed(3)); st(c.e, `translate3d(${cx.toFixed(1)}px, ${cy.toFixed(1)}px, 0)`); return; }
    // on a phone the reading's rows name the wedges, so a caption shows only for the wedge being browsed
    const cat = c.e.classList.contains('cat'), want = !F && (W >= 500 || (cat && fCat != null && CATS[fCat].name === c.e.textContent));
    const off2 = !want || sz > 1 || by < topB || by + h > bottom || bx < L0 + 4 || bx + w > L1 - 4, clash = off2 || hitsBox(box) || hitsDisc(box, -1);
    // a caption comes in only once it's been clear a moment and goes only once it's been crowded a while
    c.ok = clash ? 0 : (c.ok || 0) + 1; c.no = clash ? (c.no || 0) + 1 : 0; if (!c.on && c.ok >= HOLD_IN) c.on = true; if (c.on && (off2 || c.no >= HOLD_OUT)) c.on = false;
    const lit = !F && (fCat == null || !cat || CATS[fCat].name === c.e.textContent);
    so(c.e, c.on ? +((cat ? .95 : .8) * (lit ? 1 : .4) * clamp(sweep * 1.4 - .3)).toFixed(3) : 0); if (c.on) { boxes.push(box); st(c.e, `translate3d(${bx.toFixed(1)}px, ${by.toFixed(1)}px, 0)`); } });
  const cx0 = sun.scr ? sun.scr[0] : W / 2, cy0 = sun.scr ? sun.scr[1] : H / 2;
  let restN = 0;
  cand.forEach((c) => { const { e, x, sx, sy, pri } = c, isSel = pri <= .2 && x === sel, isHov = x.i === hov, brief = (x.kind === 'planet' || x.kind === 'bit') && !isSel && !isHov;
    // at rest a longer feed names fewer things: the biggest, as many as REST_L
    if (!F && fCat == null && x.kind === 'planet' && !isHov && restN >= REST_L) { so(e, 0); e._shown = false; e._ok = 0; e._at = null; return; }
    if (!e._w || e._sel !== isSel || e._hov !== isHov || e._brief !== brief) { e.classList.toggle('sel', isSel); e.classList.toggle('hov', isHov); e.classList.toggle('brief', brief); e._sel = isSel; e._hov = isHov; e._brief = brief; e._w = e.offsetWidth; e._h = e.offsetHeight; }
    const card = x.kind === 'post', rx = (x.scrR || 0) * (card ? 1.05 : x.kind === 'bit' ? 1.4 : .86) + (x.kind === 'bit' ? 3 : 0), ry = card ? rx * 1.25 : rx, w = e._w, h = e._h;
    const under = [sx - w / 2, sy + ry + 4], over = [sx - w / 2, sy - ry - 4 - h], right = [sx + rx + 6, sy - h / 2], left = [sx - rx - 6 - w, sy - h / 2];
    // and, failing those, tucked in at a corner
    const ur = [sx + rx * .7 + 2, sy - ry * .7 - h - 2], dr = [sx + rx * .7 + 2, sy + ry * .7 + 2], ul = [sx - rx * .7 - 2 - w, sy - ry * .7 - h - 2], dl = [sx - rx * .7 - 2 - w, sy + ry * .7 + 2];
    // a planet's name sits away from the core; a post's goes outward from the ring, above it or below it
    const away = sx >= cx0, outward = sy < cy0;
    const tries = x === sun ? [over, under, right, left] : card ? (outward ? [over, under, away ? right : left, away ? left : right] : [under, over, away ? right : left, away ? left : right]) : away ? [right, left, over, under, dr, ur, dl, ul] : [left, right, over, under, dl, ul, dr, ur];
    const order = e._at != null && e._at < tries.length ? [e._at, ...tries.map((_, k) => k).filter((k) => k !== e._at)] : tries.map((_, k) => k);
    let at = -1, bx = 0, by = 0;
    for (const k of order) { const tx = Math.max(L0 + 6, Math.min(L1 - 6 - w, tries[k][0])), ty = tries[k][1], box = [tx - 2, ty - 2, w + 4, h + 4];
      // a name pushed back in at the edge must not slide over its own body
      if (Math.abs(tx - tries[k][0]) > 2 && tries[k] !== over && tries[k] !== under) continue;
      if (ty >= topB && ty + h <= bottom && !hitsBox(box) && !hitsDisc(box, x.i)) { at = k; bx = tx; by = ty; break; } }
    // the account, what's open and what's pointed at always say their name
    const must = isSel || isHov || (x === sun && W >= 500 && !F);
    // with no clear place, it takes the first place off the map's buttons and inside the room, else its usual side
    if (at < 0 && must) { const tb = toolsBox, fits = (k) => { const tx = Math.max(L0 + 6, Math.min(L1 - 6 - w, tries[k][0])), ty = tries[k][1]; return ty >= topB && ty + h <= bottom && !(tx < tb[0] + tb[2] && tx + w > tb[0] && ty < tb[1] + tb[3] && ty + h > tb[1]); };
      at = order.find(fits) ?? order[0]; bx = Math.max(L0 + 6, Math.min(L1 - 6 - w, tries[at][0])); by = tries[at][1]; }
    // a name comes in only once its place has been clear a moment, and while it's showing it holds its place beside its
    // body through a short crowding rather than blinking out; it goes only once crowded a while
    if (at >= 0) { e._no = 0; e._ok = (e._ok || 0) + 1; if (!e._shown && !must && e._ok < HOLD_IN) { so(e, 0); return; } }
    else if (e._shown && e._dx != null && (e._no = (e._no || 0) + 1) < HOLD_OUT) { at = e._at; bx = sx + e._dx; by = sy + e._dy; }
    else { so(e, 0); e._at = null; e._shown = false; e._ok = 0; e._no = 0; return; }
    e._shown = true; e._dx = bx - sx; e._dy = by - sy;
    const back = !!F && !F.t1.has(x) && x !== sel && !isHov && x !== sun; if (e._back !== back) { e._back = back; e.classList.toggle('red', !back && e._red); }
    if (!F && fCat == null && x.kind === 'planet') restN++;
    e._at = at; boxes.push([bx - 2, by - 2, w + 4, h + 4]); e._on = true; so(e, back ? .62 : 1); st(e, `translate3d(${bx.toFixed(1)}px, ${by.toFixed(1)}px, 0)`); });
  // the altitude key stays hidden: the key over the map names the cards' heights
  so(axHi, 0); so(axMid, 0); so(axLo, 0);
  if (visible && !dead) raf = requestAnimationFrame(frame);
}
const io = new IntersectionObserver(([en]) => { const was = visible; visible = en.isIntersecting; if (visible && !was && !dead) { lastT = performance.now(); cancelAnimationFrame(raf); raf = requestAnimationFrame(frame); } }); io.observe(stage);
const ro = new ResizeObserver(() => { if (dead) return; const w0 = W, h0 = H; size(); readEl.querySelectorAll('.pf').forEach(layProof); if (!sel && !moved && !fullAnim && (wide || W !== w0 || H !== h0)) home(); }); ro.observe(stage);
let ready = false; const pending = [], now = [];
(document.fonts && document.fonts.ready ? document.fonts.ready : Promise.resolve()).then(() => { if (dead) return; size(); reading(true); home(true); t0 = INTRO ? performance.now() : performance.now() - 1e7; raf = requestAnimationFrame(frame);
  ready = true; now.splice(0).forEach((fn) => fn()); if (ON_READY) ON_READY(); pending.splice(0).forEach((fn, k) => setTimeout(() => { if (!dead) fn(); }, (450 + k * 700) * TEMPO)); });
return {
  // open a post, a thing or a law by its id or name, as a tap would (the test pages use it)
  open(id) { const cm = /^cat:(-?\d+)$/.exec(String(id)); const x = cm ? (+cm[1] < 0 ? sun : WKB[`W${cm[1]}`]) : postById[id] || byName.get(id) || LAW[id] || (id === 'sun' ? sun : null); if (!x || dead) return false; if (!ready) { pending.push(() => go(x)); return true; } go(x); return true; },
  // full screen on or off, as the map's own button does (a tucked-away Feeder File opens straight into it on a phone)
  // (instant: with no motion of its own, and as soon as the map is ready)
  full(v, then, instant) {
    if (dead) return;
    const run = () => { if (dead) return; inst = !!instant; try { setFull(!!v, then); } finally { inst = false; } };
    if (ready) run(); else if (instant) now.push(run); else pending.push(run);
  },
  unmount() {
    if (full) setFull(false); window.removeEventListener('keydown', onKey); window.removeEventListener('rd:law', onRdLaw); window.removeEventListener('rd:node', onRdNode); motionQ.removeEventListener('change', onMotion);
    if (sheetOpen) { if (sheetLocked) document.documentElement.style.overflow = sheetPrev; window.dispatchEvent(new CustomEvent('ff:hold', { detail: false })); }
    if (sheetEl) { sheetEl.remove(); sheetEl = null; sheetOpen = false; }
    dead = true; cancelAnimationFrame(raf); io.disconnect(); ro.disconnect(); themeWatch.disconnect(); controls.dispose();
    scene.traverse((o) => { if (o.geometry) o.geometry.dispose(); const m = o.material; (Array.isArray(m) ? m : m ? [m] : []).forEach((mm) => { Object.values(mm).forEach((v) => { if (v && v.isTexture) v.dispose(); }); mm.dispose(); }); });
    renderer.dispose(); renderer.forceContextLoss(); host.innerHTML = '';
  },
};
}
