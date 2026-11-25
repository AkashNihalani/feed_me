/* eslint-disable */
/* ─────────────────────────────────────────────────────────────
   READ ONE — Read as one page: the search, the files and the wall,
   and a file's artifact with its way back. Ported from the approved
   prototype (public/_lab/read-one.html) by a script, then kept here.

   Words on one side, posts on the other, always. At rest the wall;
   a file, or a search, is the same view about something else: its
   posts stand as the figure, and every other post is put away (on a
   phone into the dock, on a laptop off the bottom of the screen).

   One element a post, moved by transform only. Every move is one
   animation a tile that starts and lands on the same frame as all
   the others; a tile that waits its turn waits inside its own clock.

   It draws inside `root` (the page) and `layer` (at body level: what
   the dock turns into, which nothing inside the app's page could pin
   to the screen). The app's own dock is the bar: it is told which
   file is open (onChrome), and is the way back out of it. `keep` is
   where the tab is left, and put back from, when it is hidden.
   ───────────────────────────────────────────────────────────── */
const R1_PAGE = `<div class="page is-rest" id="page">
  <i class="stage off" id="stage"></i>
  <header class="head" id="head">
    <div class="panel" id="pRest"></div>
    <div class="panel" id="pFile" data-hid></div>
  </header>
  <section class="field" id="field" aria-label="Posts">
    <div class="bits is-hid" id="bits"></div>
    <div class="more num is-hid" id="more"></div>
  </section>
  <section class="tail" id="tail" aria-label="The reading"></section>
</div>`;
const R1_LAYER = `<div class="veil" id="veil"></div>
<div class="nav" id="nav" data-at="dock"><i class="nav__sh" data-k="glance"></i><i class="nav__sh" data-k="sheet"></i><div class="nav__face"><i class="m"></i><i class="t"></i><i class="b"></i><i class="tl"></i><i class="tr"></i><i class="bl"></i><i class="br"></i></div>
  <i class="nav__grab"></i>
  <button class="nc nc-glance" aria-label="Open the post mortem"><img alt=""><span class="nc__l"><strong class="gl1"></strong><em class="gl2 num"></em></span><u class="nc__stop"><img alt=""></u><i><svg viewBox="0 0 24 24"><path d="M6 15l6-6 6 6"/></svg></i></button>
  <div class="nc nc-sheet" role="dialog" aria-label="Post mortem"><div class="pm" id="pm"></div></div></div>`;

export function mountReadOne({ root, layer, data, mortems, handle, keep, quiet = false, onChrome }) {
root.innerHTML = R1_PAGE; layer.innerHTML = R1_LAYER;
// what is set going here stops with it
let dead = false; const gone = new AbortController(), timers = new Set(), frames = new Set();
const on = (t, k, f, o) => t.addEventListener(k, f, { ...(typeof o === 'object' ? o : { capture: o === true }), signal: gone.signal });
const setTimeout = (f, ms) => { const t = window.setTimeout(() => { timers.delete(t); if (!dead) f(); }, ms); timers.add(t); return t; }, clearTimeout = (t) => { timers.delete(t); window.clearTimeout(t); };
const requestAnimationFrame = (f) => { const t = window.requestAnimationFrame((n) => { frames.delete(t); if (!dead) f(n); }); frames.add(t); return t; }, cancelAnimationFrame = (t) => { frames.delete(t); window.cancelAnimationFrame(t); };
const $ = (s, el = root) => el.querySelector(s), $$ = (s, el = root) => [...el.querySelectorAll(s)];
const REDUCED = matchMedia('(prefers-reduced-motion: reduce)').matches; let still = REDUCED; // (and while the tab is put back as it was left)
const phoneMQ = matchMedia('(max-width: 860px)');
const DAY = 864e5, MON = 'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split(' '), WD = 'Su Mo Tu We Th Fr Sa'.split(' ');
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const short = (iso) => { const [, m, d] = iso.split('-').map(Number); return `${d} ${MON[m - 1]}`; };
const mult = (x) => (x >= 10 ? `${Math.round(x).toLocaleString('en-US')}x` : `${x.toFixed(1)}x`);
const signed = (n, tight) => { const a = Math.abs(n); return `${n < 0 ? '−' : '+'}${tight && a >= 1000 ? `${(a / 1000).toFixed(1)}k` : a.toLocaleString('en-US')}`; };
const ARROW = '<svg viewBox="0 0 24 24"><path d="M5 12h14M13 6l6 6-6 6"/></svg>';

/* ── the data: the real run ── */
const D = data;
const drop = D.drops[D.drops.length - 1], asOf = Date.parse(drop.as_of), P = D.posts;
let ALL = (drop.window || Object.keys(P)).filter((id) => P[id]);
const stamp = (id) => Date.parse(`${P[id].date}T${P[id].time || '12:00'}:00Z`), age = (id) => (asOf - Date.parse(P[id].date)) / DAY;
ALL.forEach((id) => { P[id].ts = stamp(id); });
ALL.sort((a, b) => P[b].ts - P[a].ts);
const newest = (ids) => [...ids].sort((a, b) => P[b].ts - P[a].ts);
const judged = (id) => P[id].x_likes != null && (P[id].at?.[String(drop.n)]?.stage || 'SETTLED') !== 'EARLY';
const FOLLOW = new Map(D.followers || []);
const folAt = (t) => { for (let k = 0; k < 4; k++) { const v = FOLLOW.get(new Date(t - k * DAY).toISOString().slice(0, 10)); if (v != null) return v; } return null; };
const MARK = /\[\[(P\d{3})\|(.+?)\]\]/g, plain = (t) => t.replace(MARK, '$2'), NAMES = {};
const FR = [7, 30, 90];
const fileOf = (f) => { const at = f.by_drop[String(drop.n)], sh = at.shape; [sh.read, sh.riding, sh.recommend, ...sh.stages.flatMap((s) => s.findings)].forEach((t) => { for (const m of (t || '').matchAll(MARK)) NAMES[m[1]] = m[2]; });
  return { id: f.id, name: f.name || at.title, title: at.title, kind: sh.kind, posts: newest(at.posts.filter((id) => P[id])), read: sh.read, riding: sh.riding, recommend: sh.recommend, paras: FR.map((d) => (sh.stages.find((s) => parseInt(s.span, 10) === d) || { findings: [] }).findings), followed: true }; };
const FILES = D.files.map(fileOf);
const FOUND = ['people', 'pieces', 'products', 'places', 'trending'].flatMap((g) => (D.everything[g] || []).map((e) => { const at = e.by_drop[String(drop.n)]; if (!at || !at.posts) return null; const ids = newest(at.posts.filter((id) => P[id]));
  return { id: `e-${e.id}`, name: e.name, title: e.name, kind: 'A kind of post', group: { people: 'Person', pieces: 'Kind of post', products: 'Product', places: 'Place', trending: 'In the news' }[g], posts: ids, read: '', riding: '', recommend: '', about: (at.about || []).filter((t) => !/outside it/.test(t)), followed: false }; })).filter((e) => e && !FILES.some((f) => f.id === e.id.slice(2)));
FOUND.forEach((e) => { e.paras = FR.map((d) => { const ids = e.posts.filter((id) => age(id) <= d), j = ids.filter(judged), l = j.filter((id) => P[id].landed); const line = !ids.length ? `None in these ${d} days.` : `${ids.length} ${ids.length === 1 ? 'post' : 'posts'} in ${d} days.${j.length ? ` ${l.length} of ${j.length} landed.` : ' None old enough to call.'}`; return d === 90 ? [line, ...e.about] : [line]; }); });
const SLOTS = [[6, 12, 'Before noon', 'am'], [12, 14, '12–2pm', '12'], [14, 16, '2–4pm', '2'], [16, 18, '4–6pm', '4'], [18, 20, '6–8pm', '6'], [20, 22, '8–10pm', '8'], [22, 30, 'After 10pm', '10']];
const hourOf = (id) => { const t = P[id].time; if (!t) return null; const h = Number(t.slice(0, 2)) + Number(t.slice(3, 5)) / 60; return h < 6 ? h + 24 : h; };
const midOf = (v) => { const a = [...v].sort((x, y) => x - y), m = a.length >> 1; return a.length ? (a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2) : null; };
// (each slot of the day: its posts, and the middle of what they got; a slot is only called with `least` judged posts)
const slotsOf = (d, least) => SLOTS.map(([a, b, name, tag]) => { const ids = ALL.filter((id) => age(id) >= 1 && age(id) <= d && hourOf(id) >= a && hourOf(id) < b), xs = ids.filter(judged).map((id) => P[id].x_likes); return { name, tag, a, ids, n: xs.length, x: xs.length >= least ? midOf(xs) : null }; });
const bestSlot = (d, least) => slotsOf(d, least).filter((q) => q.x != null).sort((p, q) => q.x - p.x)[0] || null;
const CLOCK = { id: 'clock', name: 'When @anuj.mp4 posts', kind: 'Clock', group: 'Time of day', posts: [...ALL], read: '', riding: '', recommend: '', followed: true };
{ const b = bestSlot(90, 8), busy = slotsOf(90, 1).sort((p, q) => q.ids.length - p.ids.length)[0]; CLOCK.title = b ? `${b.name} lands best, at ${mult(b.x)} usual` : 'No time of day stands out';
  CLOCK.paras = FR.map((d) => { const least = d === 90 ? 8 : 3, q = bestSlot(d, least), bz = slotsOf(d, 1).sort((m, n) => n.ids.length - m.ids.length)[0]; return [q ? `${q.name} did best. The middle one of its ${q.n} posts got ${mult(q.x)} usual likes.${bz && bz.name !== q.name && bz.x != null ? ` Most posts go up ${bz.name.toLowerCase().replace(/^(\d)/, 'at $1')}, and the middle one there got ${mult(bz.x)}.` : ''}` : `Too few to call. Not one time of day has ${least} judged posts in these ${d} days.`, ...(d === 90 ? ['Counted from the minute each post was uploaded, India time.'] : [])]; }); }
const nameOf = (id) => NAMES[id] || `${P[id].format === 'reel' ? 'Reel' : P[id].format === 'carousel' ? 'Carousel' : 'Post'} of ${short(P[id].date)}`;
// today's answer: the first event of the week, with the posts its run is made of
const EVENT = drop.bulletin.find((b) => b.head && b.file) || drop.bulletin[0];
const TODAY = { head: EVENT.head || EVENT.line, sub: EVENT.facts?.[0]?.v || '', file: FILES.find((f) => EVENT.file && `file:${f.id}` === EVENT.file.to) || FILES[0], posts: newest(EVENT.art?.ids ? EVENT.art.ids.slice(-(EVENT.art.run || 8)) : EVENT.posts || []).filter((id) => P[id]) };

/* ── words ── */
function sentences(text) { const out = []; let depth = 0, from = 0; for (let i = 0; i < text.length; i++) { const c = text[i]; if (c === '[' && text[i + 1] === '[') depth++; else if (c === ']' && text[i + 1] === ']') depth = Math.max(0, depth - 1); else if (!depth && '.!?'.includes(c) && text[i + 1] === ' ' && /[A-Z0-9"'\[(@]/.test(text[i + 2] || '')) { out.push(text.slice(from, i + 1).trim()); from = i + 2; } } if (from < text.length) out.push(text.slice(from).trim()); return out.filter(Boolean); }
const refs = (t) => { let out = '', last = 0; for (const m of t.matchAll(MARK)) { out += esc(t.slice(last, m.index)) + `<span class="ref" role="button" tabindex="0" data-id="${m[1]}"><svg viewBox="0 0 24 24"><path d="M6 4l14 8-14 8z"/></svg>${esc(m[2])}${P[m[1]] && judged(m[1]) ? `<i class="num">${mult(x7(m[1]))}</i>` : ''}</span>`; last = m.index + m[0].length; } return out + esc(t.slice(last)); };
const prose = (t) => sentences(t).map((s) => { const ids = [...s.matchAll(MARK)].map((m) => m[1]); return `<span class="s"${ids.length ? ` data-ids="${ids.join(' ')}"` : ''}>${refs(s)}</span>`; }).join(' ');
const cap = (s) => s.charAt(0).toUpperCase() + s.slice(1);
function headOf(text) { const ss = sentences(text); let h = ss[0] || text, rest = ss.slice(1).join(' '); if (MARK.test(h) || h.length > 84) { MARK.lastIndex = 0; const i = h.indexOf(': '); if (i > 12 && i < 84) { rest = `${cap(h.slice(i + 2))}${rest ? ` ${rest}` : ''}`; h = h.slice(0, i); } } MARK.lastIndex = 0; return [plain(h).replace(/\.$/, ''), rest]; }
const PARTS = [[/^what works/i, 'Works'], [/^what does not/i, 'Misses'], [/^what the counts?/i, 'The counts show'], [/^what (it|they) cannot/i, "The counts can't say"], [/^(still|what is) unknown/i, 'Still open']];
function moves(text) { const re = /(What works(?: here)?|What does not(?: work)?|What the counts? shows?|What (?:it|they) cannot (?:show|say|tell)|Still unknown|What is unknown):\s*/gi, hits = [...text.matchAll(re)]; return hits.map((h, i) => ({ label: (PARTS.find(([r]) => r.test(h[1])) || [0, h[1]])[1], text: cap(text.slice(h.index + h[0].length, i + 1 < hits.length ? hits[i + 1].index : text.length).trim()) })).filter((m) => m.text); }
const byDay = (f) => f.kind === 'Timing' || f.kind === 'Followers' || f.kind === 'Clock';
const inWin = (f, d) => f.posts.filter((id) => age(id) >= 0 && age(id) <= d);
function countOf(f, d) {
  if (f.kind === 'Followers') { const a = folAt(asOf - DAY), b = folAt(asOf - (d + 1) * DAY); return a != null && b != null ? `${signed(a - b)}` : ''; }
  if (f.kind === 'Clock') { const q = bestSlot(d, d === 90 ? 8 : 3); return q ? q.name : 'Too few'; }
  if (f.kind === 'Timing') return `${ALL.filter((id) => age(id) <= d && age(id) >= 1).length} posts`;
  const ids = inWin(f, d), j = ids.filter(judged); if (!ids.length) return 'None';
  return j.length ? `${j.filter((id) => P[id].landed).length} of ${j.length} landed` : `${ids.length} new`;
}

/* ── the weeks. Seven days each, newest first. A line for a past week is counted from its posts; this week's is the read's own ── */
const WEEKS = 13;
const weekOf = (id) => Math.max(0, Math.floor((Math.round(age(id)) - 1) / 7));
const dm = (t) => { const d = new Date(t); return [d.getUTCDate(), MON[d.getUTCMonth()]]; };
const weekSpan = (k) => { const a = dm(asOf - (7 * k + 7) * DAY), b = dm(asOf - (7 * k + 1) * DAY); return a[1] === b[1] ? `${a[0]}–${b[0]} ${b[1]}` : `${a[0]} ${a[1]} – ${b[0]} ${b[1]}`; };
const weekName = (k) => (k === 0 ? 'This week' : k === 1 ? 'Last week' : `${k} weeks ago`);
const nOf = (n, one, many) => `${n === 1 ? 'One' : n} ${n === 1 ? one : many}`;
const bestOf = (ids) => [...ids].sort((a, b) => (xOfL(b) - xOfL(a)) || P[b].ts - P[a].ts); const xOfL = (id) => x7(id) ?? -1;
function weekLine(f, k) {
  if (f.kind === 'Followers') { const a = folAt(asOf - (7 * k + 1) * DAY), b = folAt(asOf - (7 * k + 8) * DAY); if (a == null || b == null) return ['Not counted that week', '']; let top = null; for (let i = 7 * k + 1; i <= 7 * k + 7; i++) { const x = folAt(asOf - i * DAY), y = folAt(asOf - (i + 1) * DAY); if (x == null || y == null) continue; const v = x - y; if (!top || Math.abs(v) > Math.abs(top[0])) top = [v, asOf - i * DAY]; }
    return [`${a - b < 0 ? 'Down' : 'Up'} ${Math.abs(a - b).toLocaleString('en-US')}`, top && top[0] ? `${signed(top[0])} of it on ${dm(top[1]).join(' ')}.` : '']; }
  const src = f.kind === 'Timing' ? ALL : f.posts, ids = src.filter((id) => weekOf(id) === k && age(id) >= 1), j = ids.filter(judged), l = j.filter((id) => P[id].landed).length;
  if (f.kind === 'Timing') return ids.length ? [`${nOf(ids.length, 'post', 'posts')} on ${new Set(ids.map((id) => P[id].date)).size} days`, j.length ? `${l || 'None'} landed. Best: ${nameOf(bestOf(ids)[0])}, ${mult(x7(bestOf(ids)[0]))} usual likes.` : ''] : ['Nothing posted', ''];
  if (!ids.length) { let q = 0; while (k + q < WEEKS && !src.some((id) => weekOf(id) === k + q)) q++; const later = src.some((id) => weekOf(id) > k); return [!later ? 'Not started yet' : q > 1 ? `None, ${q} weeks in a row` : 'None', '']; }
  const b = bestOf(ids)[0], bx = x7(b); if (!j.length) return [`${nOf(ids.length, 'post', 'posts')}, too new to call`, ''];
  if (ids.length === 1) return [nameOf(b), `${P[b].landed ? 'Landed' : 'Missed'}, ${mult(bx)} usual likes.`];
  return [`${ids.length} posts, ${l === ids.length ? 'all landed' : l ? `${l} landed` : 'none landed'}`, `Best: ${nameOf(b)}, ${mult(bx)} usual likes.`];
}
const weekNum = (f, k) => { if (f.kind === 'Timing') return [String(ALL.filter((id) => weekOf(id) === k && age(id) >= 1).length), false]; const a = folAt(asOf - (7 * k + 1) * DAY), b = folAt(asOf - (7 * k + 8) * DAY); return a == null || b == null ? ['', false] : [signed(a - b, true), a < b]; };
const headNow = (f) => { const t = f.paras[0] && f.paras[0][0]; if (!t) return weekLine(f, 0); let [h, rest] = headOf(t); const i = h.length > 40 ? h.indexOf(': ') : -1; if (i > 8) { rest = `${cap(h.slice(i + 2))}. ${rest}`; h = h.slice(0, i); } return [h, plain(sentences(rest)[0] || '')]; };

/* ── the page's state ── */
const S = { file: null, win: 90, sort: 'layer', met: 'likes', lit: [], pin: null };
const page = $('#page'), head = $('#head'), field = $('#field'), tail = $('#tail'), pRest = $('#pRest'), pFile = $('#pFile'), bits = $('#bits'), more = $('#more'); [pRest, pFile].forEach((el) => Object.defineProperty(el, 'hidden', { configurable: true, get: () => el.hasAttribute('data-hid'), set: (v) => { el.toggleAttribute('data-hid', Boolean(v)); } }));
pRest.innerHTML = `<div class="askveil" id="askveil"></div><div class="ask find" id="ask"><div class="ask__cast"><i></i></div><i class="ask__b"><i></i></i><i class="ask__m"></i><i class="ask__sh"></i><div class="ask__glow g"><i></i><i></i></div><form role="search"><div><p class="ask__k">Ask @${esc(handle)}</p><div class="ask__line"><span class="ask__roll" id="roll" aria-hidden="true"><span></span><i></i><span></span></span><input id="q" type="text" autocomplete="off" autocorrect="off" autocapitalize="none" spellcheck="false" enterkeyhint="search" aria-label="Search the feed"><span class="ask__ph" aria-hidden="true"><b>Person or brand</b></span></div></div><p class="eye ask__foot num" id="askSub"></p><button class="disc" type="submit" aria-label="Open it"><i>${ARROW}</i><i><svg viewBox="0 0 24 24"><path d="M12 19V5M6 11l6-6 6 6"/></svg></i></button></form><canvas aria-hidden="true"></canvas><div class="hits" id="hits"><div class="hits__in"><p class="hits__k"></p><div class="hits__r"><i class="hl"></i></div></div></div></div>
  <div class="tabs" role="group" aria-label="Files"><button data-tab="0" aria-pressed="true">Following<sup class="num"></sup></button><button data-tab="1" aria-pressed="false">Everything<sup class="num"></sup></button><span class="num"><i>Week of </i>${weekSpan(0)}</span></div>
  <div class="strips"><div class="strip" data-strip="0"></div><div class="strip off" data-strip="1"></div></div>`;
const IC = { kind: '<path d="M4 5h7v14H4zM15 5h5v6h-5zM15 15h5v4h-5z"/>', cal: '<path d="M4 6h16v14H4zM4 10h16M9 3v4M15 3v4"/>', clock: '<circle cx="12" cy="12" r="8.5"/><path d="M12 7.5V12l3 2"/>', people: '<circle cx="9" cy="8.5" r="3.5"/><path d="M2.5 20c0-4 3-6 6.5-6s6.5 2 6.5 6M16 5.5a3.2 3.2 0 0 1 0 6M18 14.5c2.2.7 3.5 2.6 3.5 5.5"/>', scale: '<path d="M8 4v16M8 4L4.5 7.5M8 4l3.5 3.5M16 20V4M16 20l-3.500-3.500M16 20l3.500-3.500"/>', person: '<circle cx="12" cy="8" r="4"/><path d="M4.500 20.500c0-4.500 3.500-7 7.500-7s7.500 2.500 7.500 7"/>', place: '<path d="M12 21s7-6.300 7-11.500a7 7 0 0 0-14 0C5 14.700 12 21 12 21z"/><circle cx="12" cy="9.500" r="2.400"/>', box: '<path d="M4 8l8-4 8 4v8l-8 4-8-4zM4 8l8 4 8-4M12 12v8"/>', news: '<path d="M4 5h13v14H6a2 2 0 0 1-2-2zM17 9h3v8a2 2 0 0 1-2 2M8 9h5M8 13h5"/>' };
const iconOf = (f) => `<svg viewBox="0 0 24 24" aria-hidden="true">${IC[f.kind === 'Timing' ? 'cal' : f.kind === 'Clock' ? 'clock' : f.kind === 'Followers' ? 'people' : f.kind === 'A measure' ? 'scale' : { Person: 'person', Place: 'place', Product: 'box', 'In the news': 'news' }[f.group] || 'kind']}</svg>`;
const WA = 3 / 4; // (the wall's and the shelf's shape)
let IH = innerHeight; const deep = () => (phoneMQ.matches ? Math.max(0, Math.min(170, screen.height - innerHeight)) : 0);
const PLAYMID = midOf(ALL.map((id) => P[id].plays).filter((v) => v != null)) || 1, xPlays = (id) => (P[id].plays == null ? null : P[id].x && P[id].x.plays != null ? P[id].x.plays : P[id].plays / PLAYMID);
const x7 = (id) => P[id].at?.[String(drop.n)]?.x_by?.d7 ?? P[id].x_likes; // (a landing is called at day 7, so a cover draws day 7)
const lg = (x, lo, hi) => Math.max(lo, Math.min(hi, Math.log2(Math.max(0.05, x))));
const defSort = () => 'layer';
/* one number per post per metric, all of them called at day 7 (plays are counted once, after the first week). A rank is the post's place on the whole feed's ninety days */
const METS = { likes: (id) => (judged(id) ? x7(id) : null), comments: (id) => (judged(id) ? P[id].x_comments ?? null : null), plays: (id) => (judged(id) ? P[id].plays ?? null : null) }, RANK = {};
const rankOf = (m) => { if (RANK[m]) return RANK[m]; const l = ALL.filter((id) => METS[m](id) != null).sort((a, b) => METS[m](b) - METS[m](a) || (P[b].likes || 0) - (P[a].likes || 0)), map = new Map(l.map((id, i) => [id, i + 1])); if (m === 'likes' && drop.standing && l.every((id) => drop.standing[id] != null)) l.forEach((id) => map.set(id, drop.standing[id])); return (RANK[m] = { map, n: l.length }); };
const met = () => (S.file && S.sort !== 'layer' ? S.met : 'likes');
const over = (id, m) => (m === 'comments' ? judged(id) && P[id].x_comments >= 1 : m === 'plays' ? judged(id) && xPlays(id) >= 1 : judged(id) && P[id].landed);
const ord = (n) => `${n}${n % 100 > 10 && n % 100 < 14 ? 'th' : ['th', 'st', 'nd', 'rd'][n % 10] || 'th'}`;
const said = (id, m) => (m === 'comments' ? (METS.comments(id) == null ? '<b>Too new to call</b>' : `<b class="v${over(id, m) ? ' l' : ''}">${mult(METS.comments(id))}</b> usual comments`) : m === 'plays' ? (METS.plays(id) == null ? '<b>No plays counted</b>' : `<b class="v${over(id, m) ? ' l' : ''}">${kilo(METS.plays(id))}</b> plays`) : verdictOf(id));
/* RECENT. One run, newest first, left to right and down, as the wall is: what is before a post is always the next one on */
function plotRun(f, d, o, W, H, g, least) {
  const ids = [...figIds(f, d).ids].sort((a, b) => P[b].ts - P[a].ts), n = ids.length || 1; o.w = W; o.h = H; let b = null;
  for (let c = 1; c <= 20; c++) { const rows = Math.ceil(n / c), w = Math.min((W - g * (c - 1)) / c, ((H - g * (rows - 1)) / rows) * WA); if (w > 4 && (!b || w > b.w + 0.01)) b = { c, w, rows }; }
  if (b.w < least) { const c = Math.max(1, Math.floor((W + g) / (least + g))), w = (W - g * (c - 1)) / c; b = { c, w, rows: Math.max(1, Math.floor((H + g) / (w / WA + g))) }; }
  ids.slice(0, b.c * b.rows).forEach((id, i) => o.slots.push({ id, x: (i % b.c) * (b.w + g), y: Math.floor(i / b.c) * (b.w / WA + g), w: b.w, h: b.w / WA })); return o;
}
/* RANKED. The wall's own bands: where each post ranks on the whole feed, best first, each band under its name. Small bands share a row */
function plotRank(f, d, m, o, W, H, g, least, phone) {
  const R = rankOf(m), ids = figIds(f, d).ids, LH = phone ? 24 : 28, top = ids.filter((id) => R.map.has(id)).sort((a, b) => R.map.get(a) - R.map.get(b)), rest = ids.filter((id) => !R.map.has(id)).sort((a, b) => P[b].ts - P[a].ts);
  const groups = [['Top 10', 1, 10], ['11 to 25', 11, 25], ['26 to 50', 26, 50], ['Below 50', 51, 1e9]].map(([t, a, z]) => ({ t, ids: top.filter((id) => R.map.get(id) >= a && R.map.get(id) <= z) })).filter((q) => q.ids.length); if (rest.length) groups.push({ t: m === 'plays' ? 'Not ranked' : 'Too new', ids: rest });
  const lay = (c, w, cut) => { const h = w / WA, need = Math.max(1, Math.ceil((92 + g) / (w + g))), out = []; let y = 0, row = null; groups.forEach((q, i) => { const n = q.ids.length, span = Math.max(n, need); if (row && n <= c && row.col + span <= c) { out.push({ q, x: row.col, y: row.y, n }); row.col += span; return; } if (i) y += g * 2; let rows = Math.ceil(n / c); if (cut) { const fitN = Math.floor((H - y - LH + g) / (h + g)); rows = Math.min(rows, fitN); if (rows < 1) return; } out.push({ q, x: 0, y, n: Math.min(n, rows * c) }); row = rows === 1 && span < c ? { y, col: span } : null; y += LH + rows * h + (rows - 1) * g; }); return { out, y }; };
  o.w = W; o.h = H; let b = null; for (let c = 1; c <= 20; c++) { const w = Math.min((W - g * (c - 1)) / c, 132), q = lay(c, w); if (q.y <= H + 0.5) { b = { c, w, ...q }; break; } }
  if (!b || b.w < least) { const c = Math.max(1, Math.floor((W + g) / (least + g))), w = (W - g * (c - 1)) / c; b = { c, w, ...lay(c, w, true) }; }
  b.out.forEach(({ q, x, y, n }) => { o.heads.push({ x: x * (b.w + g), y, t: q.t, n: q.ids.length }); q.ids.slice(0, n).forEach((id, i) => o.slots.push({ id, x: (x + (i % b.c)) * (b.w + g), y: y + LH + Math.floor(i / b.c) * (b.w / WA + g), w: b.w, h: b.w / WA })); }); return o;
}
const XM = { likes: x7, comments: (id) => P[id].x_comments, plays: xPlays };
const kilo = (v) => (v >= 1e3 && v < 9950 ? `${(v / 1e3).toFixed(1)}k` : v >= 1e6 ? `${(v / 1e6).toFixed(v >= 9.95e6 ? 0 : 1)}M` : v >= 1e3 ? `${Math.round(v / 1e3)}k` : String(v));
const gapOf = (id) => Math.log2(Math.max(0.02, xPlays(id)) / Math.max(0.02, x7(id)));
/* the figure's room. Nothing but the screen decides it, so a cover can draw a file's figure before the file is open */
const BITH = () => (phoneMQ.matches ? 44 : 48);
const padX = () => { const c = getComputedStyle(page); return parseFloat(c.paddingLeft) + parseFloat(c.paddingRight); }, topY = () => parseFloat(getComputedStyle(page).paddingTop) || 0;
const SIDEG = 48, sideW = () => Math.round(Math.max(380, Math.min(600, (page.clientWidth - padX() - SIDEG) * 0.47)));
function figBox() { const phone = phoneMQ.matches, W = field.clientWidth - (!phone && !S.file ? sideW() + SIDEG : 0); return { phone, W, H: phone ? Math.round(Math.max(300, Math.min(440, IH * 0.475))) : Math.max(360, IH - topY() - 28 - BITH()) }; }
/* PLOT. A file is a few big shapes. The cover is those shapes and nothing else; the file is the same shapes, large, with posts in
   them. A post is never drawn smaller than it can be made out: what does not fit stays on the shelf, and the shape says how many there are */
const LEAST = () => (phoneMQ.matches ? 37 : 50);
function plot(f, d, sort, box) {
  const { phone, W, H } = box, o = { slots: [], shapes: [], sides: [], feet: [], tips: [], zones: [], cov: [], heads: [], pad: { l: 0, t: 0, b: 0 }, says: '' }, g = phone ? 3 : 5, least = LEAST(); if (sort === 'recent') return plotRun(f, d, o, W, H, g, least); if (sort === 'rank') return plotRank(f, d, S.met, o, W, H, g, least, phone);
  const pads = (l, t, b) => { o.pad = { l, t, b }; o.w = W - l; o.h = H - t - b; };
  const line = (c, x1, y1, x2, y2) => o.shapes.push({ k: 'line', c, x1, y1, x2, y2 }), rect = (c, x, y, w, h, rr) => o.shapes.push({ k: 'rect', c, x, y, w, h, r: rr }), poly = (c, pts) => o.shapes.push({ k: 'poly', c, pts });
  const R = phone ? 16 : 22, by = { likes: (a, b) => x7(b) - x7(a), comments: (a, b) => P[b].x_comments - P[a].x_comments, plays: (a, b) => (P[b].plays ?? -1) - (P[a].plays ?? -1), recent: (a, b) => P[b].ts - P[a].ts };
  if (f.kind === 'Timing' && d === 7) {
    const days = Array.from({ length: 7 }, (_, k) => ALL.filter((id) => Math.round(age(id)) === k + 1).sort((a, b) => xOfL(b) - xOfL(a))); pads(0, 28, 22); const m = Math.max(1, ...days.map((a) => a.length)), cw = o.w / 7, bw = cw - (phone ? 5 : 14), pad = 3, a = Math.max(1, Math.floor((bw - pad * 2 + 2) / (least + 14))), tw = (bw - pad * 2 - 2 * (a - 1)) / a, lines = Math.max(1, Math.min(Math.ceil(m / a), Math.floor((o.h - pad * 2 + 2) / (tw / WA + 2)))), th = tw / WA, full = Math.ceil(m / a);
    days.forEach((ids, c) => { const x = c * cw + (cw - bw) / 2, rows = Math.min(lines, full > lines ? Math.max(ids.length ? 1 : 0, Math.round((Math.ceil(ids.length / a) * lines) / full)) : Math.ceil(ids.length / a)), h = ids.length ? rows * (th + 2) - 2 + pad * 2 : 6, won = ids.filter((id) => judged(id) && P[id].landed).length; rect('z miss', x, o.h - h, bw, h, Math.min(R, bw / 2.6)); if (won) rect('z hit', x, o.h - Math.max(bw / 2, (h * won) / ids.length), bw, Math.max(bw / 2, (h * won) / ids.length), Math.min(R, bw / 2.6));
      ids.slice(0, rows * a).forEach((id, i) => o.slots.push({ id, x: x + pad + (i % a) * (tw + 2), y: o.h - pad - th - Math.floor(i / a) * (th + 2), w: tw, h: th })); const t = new Date(asOf - (c + 1) * DAY); o.feet.push({ x: c * cw + cw / 2, t: `${WD[t.getUTCDay()]} ${t.getUTCDate()}` }); o.tips.push({ x: c * cw + cw / 2, y: o.h - h, t: String(ids.length), dim: !ids.length }); });
    o.says = 'Posts each day'; return o;
  }
  if (f.kind === 'Timing') { // (how often: a band a stretch, as long as its posts are many, its landed ones first and in red)
    const n = d === 90 ? 3 : d === 30 ? 5 : 7, step = d === 90 ? 30 : d === 30 ? 7 : 1; let rows = Array.from({ length: n }, (_, k) => ALL.filter((id) => age(id) >= 1 && Math.floor((Math.round(age(id)) - 1) / step) === k && age(id) <= d)); if (d === 30 && !rows[4].length) rows = rows.slice(0, 4);
    pads(phone ? 84 : 120, 0, 0); const m = Math.max(1, ...rows.map((a) => a.length)), rh = o.h / rows.length, gp = phone ? 8 : 12, bh = rh - gp, pad = 4; let best = null;
    for (let l = 1; l <= 5; l++) { const th = (bh - pad * 2 - 2 * (l - 1)) / l, want = (o.w - pad * 2 + 2) / Math.ceil(m / l) - 2, tw = Math.min(th * WA, Math.max(least, want)); if (tw < least && best) break; const c = Math.floor((o.w - pad * 2 + 2.01) / (tw + 2)), q = { l, tw, th: tw / WA, c, cap: c * l }; if (!best || (best.cap < m && q.cap > best.cap) || (best.cap >= m && q.cap >= m && q.tw * q.th > best.tw * best.th)) best = q; }
    rows.forEach((ids, k) => { const y = k * rh + gp / 2, won = ids.filter((id) => judged(id) && P[id].landed).sort(by.likes), lost = ids.filter((id) => !won.includes(id)).sort((a, b) => xOfL(b) - xOfL(a)), c = best.cap >= m ? Math.ceil(ids.length / best.l) : Math.max(ids.length ? 1 : 0, Math.round((best.c * ids.length) / m)), bw = ids.length ? (best.cap >= m ? c * (best.tw + 2) - 2 + pad * 2 : Math.max((o.w * ids.length) / m, best.tw + pad * 2)) : 6;
      const show = Math.min(ids.length, c * best.l), sw_ = Math.min(won.length, Math.round((show * won.length) / Math.max(1, ids.length))), list = [...won.slice(0, sw_), ...lost.slice(0, show - sw_)]; rect('z miss', 0, y, bw, bh, R); if (won.length) rect('z hit', 0, y, Math.max(R, (bw * won.length) / ids.length), bh, R);
      list.forEach((id, i) => o.slots.push({ id, x: pad + Math.floor(i / best.l) * (best.tw + 2), y: y + (bh - best.l * (best.th + 2) + 2) / 2 + (i % best.l) * (best.th + 2), w: best.tw, h: best.th }));
      const t = new Date(asOf - (k * step + 1) * DAY), any = ids.some(judged), t0 = d === 90 ? `${short(new Date(asOf - (k * step + step) * DAY).toISOString().slice(0, 10))} on` : d === 30 ? `to ${t.getUTCDate()} ${MON[t.getUTCMonth()]}` : `${WD[t.getUTCDay()]} ${t.getUTCDate()}`;
      o.sides.push({ y: y + bh / 2, t: t0, sub: [ids.length ? `${ids.length} ${ids.length === 1 ? 'post' : 'posts'}` : 'none', ...(d > 7 && ids.length ? [any ? `${won.length} landed` : 'too new'] : [])] }); o.cov.push({ x: 0, y: y + bh / 2, dx: 12, t: String(ids.length), red: ids.length ? (bw * won.length) / ids.length : 0, c: 'bk' }); });
    o.says = 'Each band: its posts. Red: landed'; return o;
  }
  if (f.kind === 'Clock') { // (time of day: a column a slot, filled as high as its posts did)
    const sl = slotsOf(d, d === 90 ? 8 : 3), top = Math.max(1.1, ...sl.map((q) => q.x || 0)), hot = sl.findIndex((q) => q.x != null && q.x === Math.max(...sl.map((z) => z.x ?? -1))); pads(phone ? 42 : 60, 30, 22);
    const cw = o.w / 7, bw = cw - (phone ? 5 : 14), pad = 3, a = Math.max(1, Math.floor((bw - pad * 2 + 2) / (least + 14))), tw = (bw - pad * 2 - 2 * (a - 1)) / a, th = tw / 0.8;
    sl.forEach((z, c) => { const x = c * cw + (cw - bw) / 2, h = z.x == null ? 12 : Math.max(12, (o.h * z.x) / top), y = o.h - h; rect('trk', x, 0, bw, o.h, Math.min(R, bw / 2.6)); rect(z.x == null ? 'cb few' : c === hot ? 'cb hot' : 'cb', x, y, bw, h, Math.min(R, bw / 2.6)); const per = z.x == null ? 0 : Math.max(0, Math.floor((h - pad * 2 + 2) / (th + 2)));
      [...z.ids].filter((id) => x7(id) != null).sort(by.likes).slice(0, per * a).forEach((id, i) => o.slots.push({ id, x: x + pad + (i % a) * (tw + 2), y: o.h - pad - th - Math.floor(i / a) * (th + 2), w: tw, h: th }));
      o.feet.push({ x: c * cw + cw / 2, t: phone ? z.tag : z.name, hot: c === hot }); o.tips.push({ x: c * cw + cw / 2, y, t: z.x != null ? mult(z.x) : '·', hot: c === hot, dim: z.x == null }); if (c === hot) { o.cov.push({ x: c * cw + cw / 2, y, dy: -7, t: mult(z.x), a: 'middle', c: 'c-hot' }); o.cov.push({ x: c * cw + cw / 2, y: o.h, dy: 15, t: z.name, a: 'middle', c: 'sm' }); } });
    line('usual', 0, o.h - o.h / top, o.w, o.h - o.h / top); o.sides.push({ y: o.h - o.h / top, t: 'usual', on: true }); o.says = 'Likes by time of day, its best posts inside'; return o;
  }
  if (f.kind === 'Followers') { // (a bar a stretch, as tall as what it brought or lost)
    const n = d === 7 ? 7 : 6, step = d / n, bins = Array.from({ length: n }, () => []); ALL.forEach((id) => { if (age(id) < 1 || age(id) > d) return; bins[Math.min(n - 1, Math.max(0, Math.ceil(Math.round(age(id)) / step) - 1))].push(id); });
    const own = new Set(f.posts), val = bins.map((b, c) => { const a = folAt(asOf - (Math.round(c * step) + 1) * DAY), z = folAt(asOf - (Math.round((c + 1) * step) + 1) * DAY); return a == null || z == null ? null : a - z; }); bins.forEach((b) => b.sort((x, y) => own.has(y) - own.has(x) || xOfL(y) - xOfL(x)));
    const up = Math.max(0, ...val.map((v) => v || 0)), down = Math.max(0, ...val.map((v) => -(v || 0))); pads(0, up ? 28 : 4, 22 + (down ? 26 : 4)); const k = (o.h - 2) / (up + down || 1), y0 = up * k + 1, cw = o.w / n, bw = cw - (phone ? 6 : 16), hot = val.indexOf(Math.max(...val.map((v) => v || 0))), pad = 3, a = Math.max(1, Math.floor((bw - pad * 2 + 2) / (least + 8))), tw = (bw - pad * 2 - 2 * (a - 1)) / a, th = tw / WA;
    val.forEach((v, c) => { const x = c * cw + (cw - bw) / 2, h = v == null ? 4 : Math.max(4, Math.abs(v) * k), neg = v < 0, y = neg ? y0 + 1 : y0 - h - 1; rect(v == null ? 'cb few' : neg ? 'cb neg' : c === hot && v > 0 ? 'cb hot' : 'cb', x, y, bw, h, Math.min(R, bw / 3));
      const per = Math.max(0, Math.floor((h - pad * 2 + 2) / (th + 2))); bins[c].slice(0, per * a).forEach((id, i) => { const rr = Math.floor(i / a); o.slots.push({ id, x: x + pad + (i % a) * (tw + 2), y: neg ? y + pad + rr * (th + 2) : y + h - pad - th - rr * (th + 2), w: tw, h: th }); });
      const t0 = new Date(asOf - (Math.round(c * step) + 1) * DAY), t1 = new Date(asOf - Math.round((c + 1) * step) * DAY); o.feet.push({ x: c * cw + cw / 2, t: n === 7 ? `${WD[t0.getUTCDay()]} ${t0.getUTCDate()}` : `${t1.getUTCDate()} ${MON[t1.getUTCMonth()]}` }); o.tips.push({ x: c * cw + cw / 2, y: neg ? y + h : y, under: neg, t: v == null ? '·' : signed(v, phone || n === 7), hot: c === hot && v > 0, neg }); if (v != null && (c === 0 || c === hot)) o.cov.push({ x: c === 0 ? x : c * cw + cw / 2, y: c === 0 && neg ? y + h : y, dy: c === 0 && neg ? 16 : -7, t: signed(v, true), a: c === 0 ? 'start' : 'middle', c: c === hot ? 't-hit' : 't-miss' }); });
    line('base', 0, y0, o.w, y0); o.says = d === 7 ? 'Followers, each day' : `Followers, every ${step} days`; return o;
  }
  if (f.kind === 'A measure') {
    // (played against liked: the ones played further than they were liked, beside the ones liked further than they were played. Both numbers stand under every post)
    const all = figIds(f, d).ids.filter((id) => xPlays(id) != null && x7(id) != null), P_ = all.filter((id) => gapOf(id) >= 0).sort((a, b) => P[b].plays - P[a].plays), K_ = all.filter((id) => gapOf(id) < 0).sort((a, b) => x7(b) - x7(a)), Z = [['miss', 'Played more', P_], ['hit', 'Liked more', K_]].filter(([, , a]) => a.length); pads(0, 0, 0);
    const pad = phone ? 8 : 14, gp = phone ? 6 : 10, HEAD = phone ? 38 : 46, CAP = phone ? 36 : 40, rg = phone ? 6 : 10; let b = null;
    for (let rows = 1; rows <= 4; rows++) { const cs = Z.map(([, , a]) => Math.ceil(a.length / rows)), C = cs.reduce((x, y) => x + y, 0), w0 = (o.w - gp * (Z.length - 1) - Z.length * pad * 2 - (C - Z.length) * g) / C, h = (o.h - HEAD - pad - rg * (rows - 1)) / rows - CAP, w = Math.min(w0, h * WA); if (h < 30) continue; const q = { rows, cs, w, w0, h: w / WA, rh: h }; if (!b || w * q.h > b.w * b.h) b = q; }
    if (!b) return o; if (b.w < least) { const C = Math.max(Z.length, Math.floor((o.w - gp * (Z.length - 1) - Z.length * pad * 2 + g) / (least + g))), w0 = (o.w - gp * (Z.length - 1) - Z.length * pad * 2 - (C - Z.length) * g) / C; const w1 = Math.min(w0, b.rh * WA); b = { ...b, w: w1, w0, cs: Z.length < 2 ? [C] : [Math.max(1, Math.min(C - 1, Math.round((C * P_.length) / all.length))), 0], h: w1 / WA }; if (Z.length > 1) b.cs[1] = C - b.cs[0]; }
    let x = 0; Z.forEach(([c, t, ids], z) => { const zw = b.cs[z] * (b.w0 + g) - g + pad * 2, like = c === 'hit'; rect(`z ${c}`, x, 0, zw, o.h, R); o.zones.push({ x: x + pad + 4, y: HEAD / 2 + 2, n: ids.length, t: t.toLowerCase(), c, slim: true, top: true }); o.cov.push({ x, y: 0, dx: 14, dy: 34, t: String(ids.length), c: like ? 'wh xl' : 'bk xl' }, { x, y: 0, dx: 14, dy: 54, t: t.toLowerCase(), c: like ? 'wh sm' : 'bk sm' });
      ids.slice(0, b.cs[z] * b.rows).forEach((id, i) => { const cx = x + pad + (i % b.cs[z]) * (b.w0 + g) + (b.w0 - b.w) / 2, y = HEAD + Math.floor(i / b.cs[z]) * (b.rh + CAP + rg) + (b.rh - b.h) / 2; o.slots.push({ id, x: cx, y, w: b.w, h: b.h }); const lk = `${mult(x7(id))} likes`, pl_ = `${kilo(P[id].plays)} plays`; o.tips.push({ cap: true, x: cx + b.w / 2, y: y + b.h + 4, t: like ? mult(x7(id)) : kilo(P[id].plays), sub: like ? pl_ : lk, hot: like, sm: b.w0 < 62 }); }); x += zw + gp; });
    o.says = 'Likes are against usual, after a week'; return o;
  }
  // (a kind of post: what landed over what missed, each as large as it is many, best first)
  const ids = figIds(f, d).ids, Z = [['new', 'new', ids.filter((id) => !judged(id))], ['hit', 'landed', ids.filter((id) => judged(id) && P[id].landed)], ['miss', 'missed', ids.filter((id) => judged(id) && !P[id].landed)]].filter(([c, , a]) => a.length || (c !== 'new' && ids.some(judged))).map(([c, t, a]) => ({ c, t, ids: [...a].sort(by[sort] || by.likes).filter(Boolean), n: a.length }));
  pads(0, 0, 0); const GUT = phone ? 78 : 118, pad = phone ? 8 : 12, gp = phone ? 6 : 10, SL = phone ? 42 : 52, full = Z.filter((q) => q.n), aw = o.w - GUT - pad, room_ = o.h - gp * (Z.length - 1) - (Z.length - full.length) * SL - full.length * pad * 2; let b = null;
  for (let c = 1; c <= 20; c++) { const rows = full.reduce((a, q) => a + Math.ceil(q.n / c), 0) || 1, th = (room_ - (rows - full.length) * g) / rows, w = Math.min((aw - g * (c - 1)) / c, th * WA), h = w / WA; if (h > 4 && (!b || w * h > b.w * b.h * 1.0001)) b = { c, w, h, rows: full.map((q) => Math.ceil(q.n / c)) }; }
  if (!b || b.w < least) { const c = Math.max(1, Math.floor((aw + g) / (least + g))), w = (aw - g * (c - 1)) / c, h0 = w / WA, R_ = Math.max(full.length, Math.floor((room_ + g) / (h0 + g))), want = full.map((q) => Math.ceil(q.n / c)), rows = want.map(() => 1); let left = R_ - rows.length; while (left > 0) { let k = -1, v = 0; want.forEach((x, i) => { const u = x / rows[i]; if (rows[i] < x && u > v) { v = u; k = i; } }); if (k < 0) break; rows[k]++; left--; } const tot = rows.reduce((x, y) => x + y, 0); b = { c, w, h: h0, rows }; }
  const tot = b.rows.reduce((x, y) => x + y, 0), spare = Math.max(0, room_ - tot * b.h - (tot - full.length) * g); let y = 0, fi = 0;
  Z.forEach((q) => { const k = q.n ? b.rows[fi++] : 0, h = q.n ? k * b.h + (k - 1) * g + pad * 2 + (spare * k) / tot : SL; rect(`z ${q.c}${q.n ? '' : ' nil'}`, 0, y, o.w, h, R); const ih = k * b.h + (k - 1) * g, y1 = y + (h - ih) / 2; q.ids.slice(0, k * b.c).forEach((id, i) => o.slots.push({ id, x: GUT + (i % b.c) * (b.w + g), y: y1 + Math.floor(i / b.c) * (b.h + g), w: b.w, h: b.h })); o.zones.push({ y: y + h / 2, n: q.n, t: q.t, c: q.c, slim: h < 64 }); o.cov.push({ x: 0, y: y + h / 2, dx: 14, zh: h, t: `${q.n} ${q.t}`, c: q.c === 'hit' && q.n ? 'wh' : 'bk' }); y += h + gp; });
  o.says = ''; return o;
}
const room = (f, d, sort) => { const B = figBox(); return { ...B, pl: plot(f, d, sort, B) }; };
const n1 = (v) => v.toFixed(1);
/* the picture's shapes, drawn at any size from the same plot */
function ink(pl, sx, sy, ox, oy, cover) {
  const X = (x) => n1(ox + x * sx), Y = (y) => n1(oy + y * sy);
  return pl.shapes.map((q) => (cover && q.file ? '' : q.k === 'line' ? `<path class="${q.c}" d="M${X(q.x1)} ${Y(q.y1)}L${X(q.x2)} ${Y(q.y2)}"/>` : q.k === 'poly' ? `<path class="${q.c}" d="M${q.pts.map(([x, y]) => `${X(x)} ${Y(y)}`).join('L')}z"/>` : `<rect class="${q.c}" x="${X(q.x)}" y="${Y(q.y)}" width="${n1(Math.max(1, q.w * sx))}" height="${n1(Math.max(1.6, q.h * sy))}" rx="${n1(Math.min(q.r, q.w / 2, q.h / 2) * sx)}" ry="${n1(Math.min(q.r, q.w / 2, q.h / 2) * sy)}"/>`)).join('');
}
const CW = 206, CH = 118, miniPad = (pl) => ({ l: 0, t: pl.pad.t > 10 ? 22 : 0, b: pl.cov.some((q) => q.dy > 0 && q.dy < 30) ? 20 : 0 });
/* a cover: the file's shapes over ninety days, solid, and the one or two words they need */
function figOf(f) {
  const pl = room(f, 90, defSort(f)).pl, m = miniPad(pl), sx = (CW - m.l) / pl.w, sy = (CH - m.t - m.b) / pl.h; let words = '';
  pl.cov.forEach((q) => { if (q.red != null) { if (q.red * sx > 46) q.c = 'wh'; else q.dx = q.red * sx + 9; } const fs = /xl/.test(q.c) ? 34 : q.zh != null ? Math.max(13, Math.min(22, q.zh * sy * 0.62)) : /sm/.test(q.c) ? 13 : 17; words += `<text class="${q.c}" style="font-size:${n1(fs)}px" text-anchor="${q.a || 'start'}" x="${n1(Math.max(q.a === 'middle' ? 24 : 0, Math.min(q.a === 'middle' ? CW - 24 : CW, m.l + q.x * sx + (q.dx || 0))))}" y="${n1(m.t + q.y * sy + (q.dy || fs * 0.35))}">${esc(q.t)}</text>`; });
  return `<svg class="fg solid" viewBox="0 0 ${CW} ${CH}" aria-hidden="true">${ink(pl, sx, sy, m.l, m.t, true)}${words}</svg>`;
}
/* the week's line, the same words on the cover and over the verdict */
function weekWords(f) { if (f === CLOCK) return countOf(f, 7) === 'Too few' ? 'Too few to call' : countOf(f, 7); let t = FILES.includes(f) ? headNow(f)[0] : weekLine(f, 0)[0]; return t.split(/, | this week/)[0]; }
/* a card: its name, what the week brought, the verdict it opens on, and its shape */
function cardOf(f) {
  const own = FILES.includes(f) || f === CLOCK || f.followed, now = (f.kind === 'Timing' ? ALL : f.posts).filter((id) => age(id) >= 1 && weekOf(id) === 0).length, chip = f.kind === 'Followers' ? countOf(f, 7) : f.kind === 'Clock' ? '' : now ? `${now} new` : 'None new';
  return `<button class="card" data-file="${f.id}"><small><span>${esc(own ? f.name : f.group)}</span><u class="num">${chip}</u></small><strong>${esc(own ? f.title.replace(/, at .*/, '') : f.name)}</strong><div class="cv"><div>${figOf(f)}</div></div></button>`;
}
/* a cover draws itself, from its latest end on: a window slid over a still picture, so the browser moves two layers and paints nothing */
function drawCovers(strip, again) {
  if (still) return; const b = strip.getBoundingClientRect(); let n = 0;
  $$('.card', strip).forEach((c) => { const q = c.getBoundingClientRect(); if (q.right < b.left || q.left > b.right || (c.dataset.drawn && !again)) return; c.dataset.drawn = '1'; const o = { duration: 1100, delay: 90 * n++, easing: 'cubic-bezier(.3,.5,.1,1)', fill: 'backwards' }, cv = $('.cv', c); cv.animate([{ transform: 'translateX(-101%)' }, { transform: 'none' }], o); cv.firstElementChild.animate([{ transform: 'translateX(101%)' }, { transform: 'none' }], o); });
}
let tab = 0, strongW = 0;
function drawStrips() {
  const all = everyFile(), fol = all.filter((f) => f.followed), first = [TODAY.file, FILES.find((f) => f.kind === 'Followers'), CLOCK], follow = [...first.filter((f) => f && f.followed), ...fol.filter((f) => !first.includes(f))], rest = all.filter((f) => !f.followed).sort((a, b) => b.posts.length - a.posts.length);
  [follow, rest].forEach((list, i) => { $(`[data-strip="${i}"]`).innerHTML = list.map(cardOf).join(''); $(`[data-tab="${i}"] sup`).textContent = list.length; });
  const st = $$('.card strong'); { const w0 = st[0] && st[0].getBoundingClientRect().width; if (w0) strongW = w0; } const fitOne = () => st.filter((e) => e.scrollHeight > e.clientHeight + 2 && e.textContent.includes(', ')); for (let n = 0; n < 4; n++) { const over = fitOne(); if (!over.length) break; over.forEach((e) => { e.textContent = e.textContent.replace(/, [^,]*$/, ''); }); }
}
function setTab(i) {
  if (i === tab) return; const a = $(`[data-strip="${tab}"]`), b = $(`[data-strip="${i}"]`), d = i > tab ? 1 : -1; tab = i;
  $$('[data-tab]').forEach((e) => e.setAttribute('aria-pressed', String(Number(e.dataset.tab) === i))); b.classList.remove('off'); b.scrollLeft = 0; drawCovers(b, true);
  if (still) return void a.classList.add('off'); const w = a.offsetWidth + 24; [a, b].forEach((e) => e.getAnimations().forEach((x) => x.cancel()));
  const h = 620 / 920, kf = (x, y) => [{ transform: x, easing: EASE }, { transform: y, offset: h }, { transform: y }], p = { duration: 920, fill: 'both' }, A1 = b.animate(kf(`translateX(${d * w}px)`, 'translateX(0)'), p), A2 = a.animate(kf('translateX(0)', `translateX(${-d * w}px)`), p); A2.onfinish = () => { if (tab === i) a.classList.add('off'); A1.cancel(); A2.cancel(); };
}
const everyFile = () => [...FILES, CLOCK, ...FOUND];
const FKEY = `fm:read-one:following:${handle}`; const keptFollows = (() => { try { return JSON.parse(localStorage.getItem(FKEY) || '{}') || {}; } catch { return {}; } })();
const keepFollow = (f) => { keptFollows[f.id] = f.followed; try { localStorage.setItem(FKEY, JSON.stringify(keptFollows)); } catch {} };
everyFile().forEach((f) => { if (f.id in keptFollows) f.followed = Boolean(keptFollows[f.id]); });
const TIPIC = { Works: '<path d="M5 12.5l4.500 4.500L19 7.500"/>', Misses: '<path d="M6 6l12 12M18 6L6 18"/>', 'Rides on': '<path d="M9.500 14.500l5-5M8 12l-2 2a3.500 3.500 0 0 0 5 5l2-2M16 12l2-2a3.500 3.500 0 0 0-5-5l-2 2"/>', 'Still open': '<path d="M9 9a3 3 0 1 1 4 2.800c-.700.300-1 .900-1 1.700v.500M12 17.500v.100"/>', Why: '<path d="M12 4v10M12 18.500v.100M7 9l5-5 5 5"/>' };
function drawFile(f) {
  pFile.innerHTML = `<div class="bar"><button class="ib back" aria-label="Back to Read"><svg viewBox="0 0 24 24"><path d="M15 5l-7 7 7 7"/></svg></button><span class="nm">${iconOf(f)}<span>${esc(f.name)}</span></span><button class="fol follow${f.followed ? ' on yet born' : ''}" aria-pressed="${Boolean(f.followed)}" aria-label="Follow ${esc(f.name)}"><span class="fol__e"><i></i></span><span class="fol__o"><i></i><i></i><b></b><svg viewBox="0 0 18 18"><path d="M5 9.400l2.700 2.700L13 6.500"/></svg></span><span class="fol__w"><b>Follow</b><b>Following</b></span></button></div>
    <div class="hc"><p class="now"><b>This week</b>${esc(weekWords(f))}</p><h1 class="verdict">${esc(f.title)}</h1>
    <div class="wins" role="group" aria-label="Time frame">${FR.map((d) => `<button data-win="${d}" aria-pressed="false"><b class="num">${d}D</b><span class="num">${esc(countOf(f, d))}</span></button>`).join('')}<i></i></div></div>`;
  const rows = moves(f.recommend || ''); if (f.riding) rows.splice(Math.max(0, rows.length - 1), 0, { label: 'Rides on', text: f.riding }); if (f.read) rows.unshift({ label: 'Why', text: f.read }); const note = (m) => ['Why', 'Rides on', 'Still open'].includes(m.label), does = rows.filter((m) => !note(m)), notes = rows.filter(note);
  const list = [...FILES, CLOCK], nx = list[(list.indexOf(f) + 1) % list.length];
  tail.innerHTML = `<div class="tail-in"><section class="sheet read"><div class="track-clip"><div class="track" id="track">${FR.map((d, i) => { const fs = f.paras[i] || []; const [h, rest] = fs[0] ? headOf(fs[0]) : ['Nothing to say in this time frame yet', '']; return `<article class="para" data-win="${d}"><h2>${esc(h)}</h2>${[rest, ...fs.slice(1)].filter(Boolean).map((t) => `<p>${prose(t)}</p>`).join('')}</article>`; }).join('')}</div></div></section>
    <div class="after" id="after">${does.length ? `<section class="sheet does">${does.map((m, i) => `<div class="${m.label === 'Misses' || i ? 'no' : 'yes'}"><i><svg viewBox="0 0 24 24">${TIPIC[m.label] || (i ? TIPIC['Still open'] : TIPIC.Works)}</svg></i><span><b>${esc(m.label)}</b><p>${prose(m.text)}</p></span></div>`).join('')}</section>` : ''}${notes.length ? `<div class="tips">${notes.map((m) => `<section class="sheet tip"><h3><svg viewBox="0 0 24 24">${TIPIC[m.label] || TIPIC.Why}</svg>${esc(m.label)}</h3><p>${prose(m.text)}</p></section>`).join('')}</div>` : ''}${weeksOf(f)}<button class="sheet next" data-file="${nx.id}">${iconOf(nx)}<span><small>Next</small>${esc(nx.name)}</span>${ARROW}</button></div></div>`;
  const wk = $('#wkp'); if (wk) wkRead(f, 0);
}
function weekChart(f, N) {
  const most0 = Math.max(1, ...Array.from({ length: N }, (_, k) => (f.kind === 'Timing' ? ALL : f.posts).filter((id) => age(id) >= 1 && weekOf(id) === k).length)), W = 320, H = f.kind === 'Followers' ? 96 : Math.max(44, Math.min(96, most0 * 22)), pitch = W / N, w = pitch - Math.max(4, pitch * 0.24), out = { l: '', m: '', d: '', n: '', z: '' }, rect = (k, x, y, ww, h, rr) => { const q = Math.min(rr, h / 2, ww / 2); out[k] += `M${(x + q).toFixed(1)} ${y.toFixed(1)}h${(ww - 2 * q).toFixed(1)}a${q} ${q} 0 0 1 ${q} ${q}v${(h - 2 * q).toFixed(1)}a${q} ${q} 0 0 1 ${-q} ${q}h${(-(ww - 2 * q)).toFixed(1)}a${q} ${q} 0 0 1 ${-q} ${-q}v${(-(h - 2 * q)).toFixed(1)}a${q} ${q} 0 0 1 ${q} ${-q}z`; };
  if (f.kind === 'Followers') { const v = Array.from({ length: N }, (_, k) => { const a = folAt(asOf - (7 * k + 1) * DAY), b = folAt(asOf - (7 * k + 8) * DAY); return a == null || b == null ? null : a - b; }), up = Math.max(1, ...v.map((x) => x || 0)), dn = Math.max(0, ...v.map((x) => -(x || 0))), k0 = (H - 4) / (up + dn), y0 = up * k0 + 2; v.forEach((x, k) => { if (x == null) return rect('z', k * pitch, y0 - 1, w, 2, 1); const h = Math.max(3, Math.abs(x) * k0); rect(x < 0 ? 'd' : 'm', k * pitch, x < 0 ? y0 + 1 : y0 - h - 1, w, h, 4); }); }
  else { const src = f.kind === 'Timing' ? ALL : f.posts, wk = Array.from({ length: N }, (_, k) => src.filter((id) => age(id) >= 1 && weekOf(id) === k)), most = Math.max(1, ...wk.map((a) => a.length)), g = most > 8 ? 2 : 3, h = Math.min(19, (H - (most - 1) * g) / most); wk.forEach((ids, k) => { if (!ids.length) return rect('z', k * pitch, H - 3, w, 3, 1.5); const st = (id) => (!judged(id) ? 2 : P[id].landed ? 0 : 1); [...ids].sort((a, b) => st(a) - st(b)).forEach((id, n) => { const t = st(id), y = H - (n + 1) * h - n * g; if (t === 2) rect('n', k * pitch + 0.8, y + 0.8, w - 1.6, Math.max(1.5, h - 1.6), 4); else rect(t ? 'm' : 'l', k * pitch, y, w, h, 4); }); }); }
  return `<svg class="wk" style="height:${H}px" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" aria-hidden="true">${['z', 'm', 'd', 'l', 'n'].filter((k) => out[k]).map((k) => `<path class="${k}" d="${out[k]}"/>`).join('')}</svg>`;
}
function weeksOf(f) { if (f.kind === 'Clock') return ''; const N = WEEKS, kind = f.kind !== 'Followers'; return `<section class="sheet wkp" id="wkp"><header><h3>Week by week</h3><span class="num" id="wkD"></span></header><strong id="wkH"></strong><p class="num" id="wkP"></p><div class="wkc" id="wkc" data-n="${N}">${weekChart(f, N)}<i></i></div><div class="wkx"><span>This week</span><span>${N - 1} weeks ago</span></div>${kind ? '<div class="wkk"><span class="l">Landed</span><span>Missed</span><span class="n">Too new</span></div>' : ''}</section>`; }
function wkRead(f, k) { const N = WEEKS, c = $('#wkc'), [h, u] = weekLine(f, k); if (c.dataset.k === String(k)) return; c.dataset.k = k; $('#wkD').textContent = weekSpan(k); $('#wkH').textContent = h; $('#wkP').textContent = u; const m = $('i', c); m.style.width = `${(100 / N) * 0.76}%`; m.style.transform = `translateX(${(k * 100) / 0.76}%)`; }
// what the figure stands: the file's posts in the time frame, or, when there are none, the posts that ran instead
function figIds(f, d, sort) {
  let ids = byDay(f) ? ALL.filter((id) => age(id) >= 1 && age(id) <= d) : inWin(f, d); const instead = !ids.length;
  return { ids, instead };
}
const LAYER = { Timing: 'Pace', Clock: 'Hours', Followers: 'Followers', 'A measure': 'Gap' }, MIC = { likes: '<path d="M12 20s-7.500-4.600-7.500-10A4.300 4.300 0 0 1 12 7a4.300 4.300 0 0 1 7.500 3c0 5.400-7.500 10-7.500 10z"/>', comments: '<path d="M20 11.500a7.800 7.800 0 0 1-11.600 6.800L4 19.500l1.300-4A7.800 7.800 0 1 1 20 11.500z"/>', plays: '<path d="M7 5l12 7-12 7z"/>' };
function drawBits() {
  const f = S.file; if (!f) return;
  const one = S.pin || (S.lit.length === 1 ? S.lit[0] : null);
  if (one) { const p = P[one]; bits.dataset.k = ''; bits.innerHTML = `<p class="pk"><b>${esc(nameOf(one))}</b><span class="num">${short(p.date)}${METS.likes(one) != null ? ` · ${mult(METS.likes(one))} usual` : ''}</span></p>${S.pin ? `<a href="${esc(p.url)}" target="_blank" rel="noopener">Open</a>` : ''}`; return; }
  const { ids, instead } = figIds(f, S.win); if (instead) { bits.dataset.k = ''; bits.innerHTML = ''; return; } const pl = ids.filter((id) => METS.plays(id) != null).length >= 3, key = `${f.id}:${pl}`;
  if (bits.dataset.k !== key) { bits.dataset.k = key; bits.innerHTML = `<div class="views">${[['layer', LAYER[f.kind] || 'Landed'], ['recent', 'Recent'], ['rank', 'Ranked']].map(([k, t]) => `<button data-sort="${k}">${t}</button>`).join('')}</div><p class="says"></p><div class="mets" role="group" aria-label="Judged on">${MK.filter((k) => k !== 'plays' || pl).map((k) => `<button data-met="${k}" aria-label="${cap(k)}"><svg viewBox="0 0 24 24">${MIC[k]}</svg></button>`).join('')}</div>`; void bits.offsetWidth; }
  if (S.met === 'plays' && !pl) S.met = 'likes'; $$('[data-sort]', bits).forEach((b) => b.setAttribute('aria-pressed', String(b.dataset.sort === S.sort))); $$('[data-met]', bits).forEach((b) => b.setAttribute('aria-pressed', String(b.dataset.met === S.met))); $('.mets', bits).classList.toggle('on', S.sort !== 'layer'); $('.says', bits).innerHTML = S.sort === 'layer' ? (L && L.fig && L.fig.says) || '' : '';
}

/* ── where every post stands: on the wall, in a file's figure, or put away ── */
function fit(n, W, H, g) {
  let best = null;
  for (let c = 1; c <= n; c++) {
    const r = Math.ceil(n / c); let w = (W - g * (c - 1)) / c, h = Math.min((H - g * (r - 1)) / r, w / 0.45);
    if (w / h > (n === 1 ? 0.8 : 1.1)) w = h * (n === 1 ? 0.8 : 1.1);
    const used = n * w * h - (c * r - n) * w * h * 0.35; if (!best || used > best.used) best = { c, r, w, h, used };
  }
  return best;
}
// (a wall tile's size: the same whether a file is open or not, so no post is ever laid out again)
const BLEED = 14;
function baseWall(W, phone) { const full = phone ? W : page.clientWidth - padX(), c = phone ? 5 : 15, g = phone ? 2 : 3; return (full + (phone ? 2 * BLEED : 0) - g * (c - 1)) / c; }
function layout(guess) {
  const phone = phoneMQ.matches, W = field.clientWidth, g = phone ? 2 : 3, at = new Map(), o = { at, W, phone };
  if (!S.file) {
    const c = phone ? 5 : 15, w = baseWall(W, phone), h = w / WA, LH = 0, x0 = phone ? -BLEED : 0; o.base = { w, h }; o.labels = []; let y = 0;
    const rest = ALL;
    rest.forEach((id, i) => { const col = i % c, row = Math.floor(i / c), x = x0 + col * (w + g), ty = y + row * (h + LH + g) + LH; at.set(id, { x, y: ty, w, h, k: 'wall', i }); });
    o.H = y + Math.ceil(rest.length / c) * (h + LH + g);
    // (a date that would run into the one beside it is left out)
    { let px = -99, py = -1; o.labels = o.labels.filter(([x, ly]) => { if (ly === py && x - px < 48) return false; px = x; py = ly; return true; }); }
    return o;
  }
  const f = S.file, instead = figIds(f, S.win, S.sort).instead, B = instead ? figBox() : room(f, S.win, S.sort), { H } = B, baseW = baseWall(W, phone), BIT = BITH(); o.base = { w: baseW, h: baseW / WA };
  o.instead = instead;
  if (instead) { const l = f.posts[0]; o.empty = { y: 0, h: H, n: l ? Math.max(1, Math.round(age(l))) : 0, name: l ? nameOf(l) : '', date: l ? short(P[l].date) : '' }; }
  else { o.fig = B.pl; o.fig.slots.forEach((q, i) => at.set(q.id, { x: o.fig.pad.l + q.x, y: o.fig.pad.t + q.y, w: q.w, h: q.h, z: q.z, k: 'fig', i })); }
  o.figY = BIT; o.figH = H; at.forEach((q) => { q.y += BIT; }); if (o.empty) o.empty.y += BIT;
  // (every other post is put away)
  ALL.forEach((id, i) => { if (!at.has(id)) at.set(id, { x: 0, y: -6000, w: baseW, h: baseW / WA, k: 'box', i }); });
  o.box = { x: 0, y: 0, w: 0, h: 0, n: 0 }; o.bitsY = 0; o.H = BIT + H;
  return o;
}
/* the figure's own layer: its lines, its words, and a block under every post for the moment the cover becomes the file */
const gclip = document.createElement('div'); gclip.className = 'gclip'; field.prepend(gclip); let gnow = null;
function buildFig(pl, W, H, y) {
  const o = pl.pad, e = document.createElement('div'); e.className = 'glay'; e.style.cssText = `top:${y}px;height:${H}px;width:${W}px`;
  const D_ = (c, x, y, t, sub) => `<div class="${c}" style="transform:translate(${n1(x)}px,${n1(y)}px)"><span>${esc(t)}${(sub || []).map((q) => `<small>${esc(q)}</small>`).join('')}</span></div>`; let h = `<svg class="tint" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" aria-hidden="true">${ink(pl, 1, 1, o.l, o.t)}</svg><svg class="solid" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" aria-hidden="true">${ink(pl, 1, 1, o.l, o.t, true)}</svg><div class="gl">`;
  pl.heads.forEach((q) => { h += `<div class="gh num" style="transform:translate(${n1(o.l + q.x)}px,${n1(o.t + q.y)}px)"><span><b>${esc(q.t)}</b>${q.n}</span></div>`; }); pl.zones.forEach((q) => { h += `<div class="gz num ${q.c}${q.slim ? ' slim' : ''}" style="transform:translate(${n1((q.x || 14) - 14)}px,${n1(o.t + q.y)}px)"><span><b>${q.n}</b>${q.t}</span></div>`; });
  pl.sides.forEach((q) => { h += D_(`gs num${q.dim ? ' dim' : ''}${q.on ? ' on' : ''}${q.sub ? ' big' : ''}`, 0, o.t + q.y, q.t, q.sub); });
  pl.feet.forEach((q) => { h += D_(`gf num a-${q.a || 'middle'}${q.hot ? ' hot' : ''}`, o.l + q.x, H - 18, q.t); });
  pl.tips.forEach((q) => { if (q.cap) { h += `<div class="gc num${q.hot ? ' hot' : ''}${q.sm ? ' sm' : ''}" style="transform:translate(${n1(o.l + q.x)}px,${n1(o.t + q.y)}px)"><span><b>${esc(q.t)}</b>${esc(q.sub)}</span></div>`; return; } h += D_(`gn num${q.hot ? ' hot' : ''}${q.dim ? ' dim' : ''}${q.mid ? ' mid' : ''}${q.sm ? ' sm' : ''}`, o.l + q.x, o.t + q.y + (q.mid ? 0 : q.under ? 2 : -28), q.t); });
  e._pl = pl; e.innerHTML = h + '</div>'; return e;
}

/* ── tiles ── */
const tiles = new Map(), AR = new Map();
function tileOf(id) {
  let t = tiles.get(id); if (t) return t;
  const el = document.createElement('div'); el.className = 'tile off'; el.dataset.id = id; 
  const inn = document.createElement('div'); inn.className = 'in'; const img = document.createElement('img'); img.alt = ''; img.decoding = 'sync'; img.draggable = false; img.src = P[id].cover; inn.appendChild(img); const ring = document.createElement('i'); ring.className = 'rg'; inn.appendChild(ring); el.appendChild(inn); field.appendChild(el);
  t = { id, el, inn, ring, img, bw: 0, bh: 0, r: null, on: false }; tiles.set(id, t);
  if (!AR.has(P[id].cover)) img.addEventListener('load', () => { if (img.naturalWidth) { AR.set(P[id].cover, img.naturalWidth / img.naturalHeight); t.bw && size(t, t.bw, t.bh, true); } }, { once: true });
  return t;
}
const coverK = (ar, w, h) => Math.max(w / ar, h); // (the picture's drawn height when it covers w by h)
function size(t, w, h, force) {
  if (!force && Math.abs(t.bw - w) < 0.1 && Math.abs(t.bh - h) < 0.1) return;
  t.bw = w; t.bh = h; const ar = AR.get(P[t.id].cover) || 0.5625, k = coverK(ar, w, h);
  t.el.style.width = `${w}px`; t.el.style.height = `${h}px`; t.img.style.width = `${(k * ar).toFixed(2)}px`; t.img.style.height = `${k.toFixed(2)}px`; t.img.style.left = `${((w - k * ar) / 2).toFixed(2)}px`; t.img.style.top = `${((h - k) / 2).toFixed(2)}px`;
}
const tf = (x, y, sx, sy) => `translate(${x.toFixed(2)}px,${y.toFixed(2)}px) scale(${sx.toFixed(5)},${sy.toFixed(5)})`;
// a tile's own size: the figure's when it stands there, else the wall's (a shelf tile is the wall's tile drawn smaller)
// (a tile leaving the figure keeps its own size for the flight, so its corners never swell, and is set to its new one as it lands)
function stand(t, r, base) {
  const fig = r.k === 'fig', w = Math.max(t.bw, base.w, fig ? Math.ceil(r.w) : 0); if (w !== t.bw) size(t, w, w / WA);
  t.r = r; t.el.style.transform = tf(r.x, r.y, r.w / t.bw, r.h / t.bh); if (!moving) zed(t);
  t.el.classList.toggle('is-fig', fig); if (fig) t.el.classList.toggle('landed', over(t.id, met()));
}
const zed = (t, z) => { const v = z != null ? z : (t.r && t.r.z) || ''; if (t.z !== v) { t.z = v; t.el.style.zIndex = v; } };
const show = (t, on) => { if (t.on === on) return; t.on = on; t.el.classList.toggle('off', !on); };
let L = null, origin = 0;
const near = (r, pad) => { const y = r.y + origin - scrollY; return y + r.h > -pad * IH && y < IH * (1 + pad) + deep(); };
/* only what is on screen, or about to be, has an element in play */
function settleWindow(pad = 1) {
  for (const [id, r] of L.at) { const keep = r.k !== 'box' && (r.k !== 'wall' || near(r, pad)); if (keep) { const t = tileOf(id); if (t.r !== r) stand(t, r, L.base); show(t, true); } else { const t = tiles.get(id); if (t && t.on) show(t, false); } }
}
function furniture() { // (what stands with the posts: the rows' words, the sorts, the box, the bars)
  const f = S.file; $$('.empty', field).forEach((e) => e.remove()); gnow = null; $$('.glay:not(.old)', gclip).forEach((e) => e.remove());
  if (f && L.fig) { gnow = buildFig(L.fig, L.W, L.figH, L.figY); gclip.append(gnow); }
  bits.classList.toggle('is-hid', !f); more.classList.toggle('is-hid', !f || !L.box.n); 
  if (!f) return;
  bits.style.transform = `translate(0px,${L.bitsY}px)`;
  more.style.width = `${L.box.w}px`; more.style.height = `${L.box.h}px`; more.style.transform = `translate(${L.box.x}px,${L.box.y}px)`;
  placeStage();
  if (L.empty) { const e = document.createElement('div'); e.className = 'empty'; e.style.cssText = `top:${L.empty.y}px;height:${L.empty.h}px`; e.innerHTML = `<b class="num">${L.empty.n}</b><span>${L.empty.n === 1 ? 'day' : 'days'} since the last one, ${esc(L.empty.name)}${/ of \d/.test(L.empty.name) ? '' : ` on ${L.empty.date}`}</span>`; field.append(e); L.emptyEl = e; }
}
/* the stage: the raised panel the posts stand on. On a phone it holds the headline and the time frames too */
const stage = $('#stage'); const dress = () => { const home = phoneMQ.matches ? page : field; if (stage.parentNode !== home) home.prepend(stage); }; dress();
function placeStage() { const pr = page.getBoundingClientRect(), fr = field.getBoundingClientRect(), phone = phoneMQ.matches, hc = $('.hc', pFile); if (!hc) return; const y0 = phone ? hc.getBoundingClientRect().top - 2 : fr.top - 14, x0 = phone ? pr.left + 6 : fr.left - 14, x1 = phone ? pr.right - 6 : fr.right + 14, y1 = fr.top + L.H + (phone ? 6 : 10); L.stage = { x: x0, y: y0, w: x1 - x0, h: y1 - y0 }; stage.style.cssText = phone ? `left:${x0 - pr.left}px;top:${y0 - pr.top}px;width:${x1 - x0}px;height:${y1 - y0}px` : `left:${x0 - fr.left}px;top:${y0 - fr.top}px;width:${x1 - x0}px;height:${y1 - y0}px`; stage.classList.remove('off'); }
// (from one place on the screen to its own, or away from its own)
function stageFrom(A, B, own) { if (FILL) { const t0 = (dx) => `translate(${(A.x - own.x + dx).toFixed(1)}px,${(A.y - own.y).toFixed(1)}px) scale(${(A.w / own.w).toFixed(4)},${(A.h / own.h).toFixed(4)})`; return void anim(stage, [{ transform: t0(0), easing: EASE }, { transform: t0(vw()), offset: OUT }, { transform: t0(vw()) }], 'linear'); } const t = (q) => `translate(${(q.x - own.x).toFixed(1)}px,${(q.y - own.y).toFixed(1)}px) scale(${(q.w / own.w).toFixed(4)},${(q.h / own.h).toFixed(4)})`; anim(stage, [{ transform: t(A) }, { transform: t(B) }]); }
const rectOf = (e) => { const b = e.getBoundingClientRect(); return { x: b.left, y: b.top, w: b.width, h: b.height }; };
/* ── the one move. Read where everything is drawn, put the page in its new state, and fly every tile that is on screen
   before or after from where it was drawn to where it now stands. One length, one first frame, one last frame ── */
const sweep = () => { $$('.gone,.ghost,.glay.old,.glay.home').forEach((e) => e.remove()); gclip.classList.remove('free'); $$('.cvw,.fly').forEach((e) => e.remove()); $$('.flying').forEach((e) => e.classList.remove('flying')); $$('.panel.is-leaving').forEach((e) => { e.hidden = true; e.classList.remove('is-leaving'); e.style.cssText = ''; }); };
const T = 920, RIPPLE = 130, EASE = 'cubic-bezier(.32,.62,.16,1)';
const cub = (() => { const [x1, y1, x2, y2] = [0.32, 0.62, 0.16, 1], f = (a, b, t) => { const u = 1 - t; return 3 * u * u * t * a + 3 * u * t * t * b + t * t * t; }; return (p) => { let lo = 0, hi = 1, t = p; for (let i = 0; i < 18; i++) { t = (lo + hi) / 2; if (f(x1, x2, t) < p) lo = t; else hi = t; } return f(y1, y2, t); }; })();
let landNow = () => {}, moveAt = 0, stripX = [0, 0], restRect = { left: 0, top: 0, width: 0 }, lastJS = 0, anims = [], moveNo = 0, moving = false, savedY = 0;
/* Safari shows a move's first frame some time after its clock has started (it takes it a while to set a hundred posts off), and ends it on
   the clock: the beginning is skipped and the end is cut. So a move stands still for LEAD first, and stands at its end for TAIL after */
const TAIL = 240, LEAD = phoneMQ.matches ? 100 : 40, ALLT = LEAD + T + TAIL;
// (Safari misplaces a layer when 'none' stands beside a list of functions: it is always given the same functions, at rest)
const atRest = (t) => t.replace(/(translate[XY]?)\([^)]*\)/g, (m, f) => (f === 'translate' ? 'translate(0px,0px)' : `${f}(0px)`)).replace(/scale\([^)]*\)/g, 'scale(1,1)');
let Tm = T, FILL = null, POP = false; const OUT = 0.4, POP_T = 1060, SETTLE = 'cubic-bezier(.5,.02,.16,1)', FILL_T = 1360, FILL_M = 0.54, dur = () => LEAD + Tm + TAIL;
const held = (kf0, easing = EASE) => { const some = kf0.find((k) => k.transform && k.transform !== 'none'), kf = some ? kf0.map((k) => (k.transform === 'none' ? { ...k, transform: atRest(some.transform) } : k)) : kf0, n = kf.length, at = (o) => (LEAD + o * Tm) / dur(), out = kf.map((k, i) => ({ ...k, offset: at(k.offset != null ? k.offset : i / (n - 1)), easing: k.easing || (easing === 'linear' ? 'linear' : i === 0 && n === 2 ? easing : 'linear') })), bare = (k) => { const c = { ...k }; delete c.offset; delete c.easing; return c; }; return [{ ...bare(kf[0]), offset: 0, easing: 'linear' }, ...out, { ...bare(kf[n - 1]), offset: 1 }]; };
const anim = (el, kf, easing = EASE) => { const a = el.animate(held(kf, easing), { duration: dur(), easing: 'linear', fill: 'both' }); anims.push(a); return a; };
/* put away: on a phone a post goes into the navbar's picture, and comes out of it. On a laptop the wall drops off the bottom of the screen as one sheet, and rises from there */
const dockSpot = () => { const f = dockEl() && dockEl().querySelector('.fm-dock-face'), b = f && f.getBoundingClientRect(); if (b && b.width) return { x: b.left + b.width / 2, y: b.top + b.height / 2 }; const n = nav.getBoundingClientRect(); return { x: n.left, y: n.top - 32 }; };
function move(change, opt = {}) {
  const tJS = performance.now(); 
  const oldL = L, oldScroll = scrollY, fr0 = field.getBoundingClientRect(), wasMoving = moving, from = new Map(), st0 = stage.classList.contains('off') ? null : rectOf(stage), emp0 = oldL && oldL.emptyEl && oldL.emptyEl.isConnected ? lift(oldL.emptyEl) : null;
  // 1 · where every tile on the screen is drawn now
  for (const [id, t] of tiles) { if (!t.on) continue; if (wasMoving) { const b = t.el.getBoundingClientRect(); from.set(id, { x: b.left, y: b.top, w: b.width, h: b.height }); } else from.set(id, { x: fr0.left + t.r.x, y: fr0.top + t.r.y, w: t.r.w, h: t.r.h }); }
  if (wasMoving && S.file && $('#track')) live = { tr: getComputedStyle($('#track')).transform, mk: getComputedStyle($('.wins i', pFile)).transform }; else live = {};
  const gx0 = gnow && wasMoving ? gnow.getBoundingClientRect().left - fr0.left : 0; anims.forEach((a) => a.cancel()); anims = []; const my = ++moveNo; moving = true; moveAt = performance.now();
  Tm = T; FILL = null; POP = Boolean(opt.pop) && !still; if (POP) Tm = POP_T; sweep(); glim.hold(); 
  const f0 = S.file, as0 = f0 && (S.win !== 90 || S.sort !== defSort(f0)), g0 = gnow, gr0 = g0 ? rectOf(g0) : null; if (g0) g0.classList.add('old');
  // 2 · the new state, whole, in this one task
  unpick(); change(); if (emp0) page.append(emp0); clearLit(true);
  if (opt.scroll != null) { L = null; }
  const l0 = (L = layout()); field.style.height = `${L.H}px`;
  if (opt.scroll != null) scrollTo(0, typeof opt.scroll === 'function' ? opt.scroll() : opt.scroll); pins();
  const fr1 = field.getBoundingClientRect(); origin = fr1.top + scrollY; const dy = scrollY - oldScroll;
  furniture(); drawBits(); if (!S.file) look();
  // 3 · every post that is on the screen at either end
  const vh = innerHeight + deep(), onS = (r) => r.y + r.h > 0 && r.y < vh, jobs = []; if (opt.fill && !still && fr1.top > 60) { FILL = { rise: fr1.top - 4 }; Tm = FILL_T; } const up = (e) => (FILL ? { ...e, y: e.y - FILL.rise } : e);
  for (const [id, r] of L.at) {
    const q = oldL && oldL.at.get(id), was = from.get(id) || (q && q.k !== 'box' ? { x: fr0.left + q.x, y: fr0.top + q.y, w: q.w, h: q.h } : null), to = r.k === 'box' ? null : { x: fr1.left + r.x, y: fr1.top + r.y, w: r.w, h: r.h }, a = was && onS(was), b = to && onS(r.k === 'wall' ? up(to) : to), swap = phoneMQ.matches && oldL && q && (q.k === 'wall') !== (r.k === 'wall');
    if (a || b) jobs.push({ id, r, fm: swap && b && !a ? null : was || (oldL ? null : to), to: swap && a && !b ? null : to, pour: !to || (swap && a && !b) ? -1 : (!was || (swap && b && !a)) && oldL ? 1 : 0 }); else { const t = tiles.get(id); if (t) { if (!to || !near(r, 1)) { show(t, false); t.r = null; t.el.style.transform = 'translate(0px,-6000px)'; } else { stand(t, r, L.base); show(t, true); } } }
  }
  // (a post whose other end is far off the screen keeps its order but is brought near, so it does not shoot across)
  const squeeze = (pick) => { let far = 0; jobs.forEach((j) => { if (!j[pick]) return; const y = j[pick].y; if (y > vh) far = Math.max(far, y - vh); if (y < 0) far = Math.max(far, -y - j[pick].h); }); const c = far > 0.9 * vh ? (0.9 * vh) / far : 1; if (c < 1) jobs.forEach((j) => { const e = j[pick]; if (!e) return; if (e.y > vh) j[pick] = { ...e, y: vh + (e.y - vh) * c }; else if (e.y + e.h < 0) j[pick] = { ...e, y: -e.h + (e.y + e.h) * c }; }); };
  squeeze('fm'); if (!FILL) squeeze('to');
  { const phone = phoneMQ.matches, dk = phone ? dockSpot() : null, low = (k, e) => vh - Math.min(vh, ...jobs.filter((j) => j.pour === k).map((j) => Math.max(0, j[e].y))) + 40, D = { '-1': phone ? 0 : low(-1, 'fm'), 1: phone ? 0 : low(1, 'to') }, away = (e, k) => (phone ? { x: dk.x - 7, y: dk.y - 9, w: 14, h: 18.67 } : { ...e, y: e.y + D[k] });
    jobs.forEach((j) => { if (j.pour < 0) j.to = away(j.fm, -1); else if (j.pour > 0) j.fm = away(j.to, 1); });
    const far = (e) => (phone ? Math.hypot(e.x + e.w / 2 - dk.x, e.y + e.h / 2 - dk.y) : 0), P_ = jobs.filter((j) => j.pour), in_ = P_.filter((j) => j.pour < 0), out_ = P_.filter((j) => j.pour > 0);
    if (phone) { in_.sort((p, q) => far(p.fm) - far(q.fm)); out_.sort((p, q) => far(up(q.to)) - far(up(p.to))); const late = in_.length && S.file ? 0.12 : FILL ? 0.05 : 0.02; in_.forEach((j, n) => { j.hold = (n / Math.max(1, in_.length - 1)) * 0.18; }); out_.forEach((j, n) => { j.hold = late + (n / Math.max(1, out_.length - 1)) * 0.28; }); }
    else { P_.sort((p, q) => (p.pour < 0 ? q.fm.y - p.fm.y || q.fm.x - p.fm.x : p.to.y - q.to.y || p.to.x - q.to.x)).forEach((j, n) => { j.hold = ((n / Math.max(1, P_.length - 1)) * 190) / T; }); }
    if (phone && P_.some((j) => j.pour < 0) && !still) { const my_ = my; setTimeout(() => { if (my_ === moveNo && S.file) dockEl()?.querySelector('.fm-dock-hero')?.animate([{ transform: 'scale(1,1)' }, { transform: 'scale(1.08,1.08)', offset: 0.35, easing: 'cubic-bezier(.3,1.6,.5,1)' }, { transform: 'scale(1,1)' }], { duration: 620, easing: EASE }); }, LEAD + T * 0.4); } }
  // (who goes first: the figure's own posts in their order, then the rest as they lie)
  // (opening, one wave down the screen. Closing, the file's posts lead and the shelf unrolls behind them. A file's posts always pass in front)
  jobs.forEach((j) => { const t = tiles.get(j.id); j.fig = j.r.k === 'fig' || Boolean(t && t.r && oldL && oldL.at.get(j.id)?.k === 'fig'); });
  const closing = Boolean(oldL && oldL.box && !L.box), order = jobs.filter((j) => !j.pour).sort((p, q) => (closing ? (oldL.at.get(p.id).k === 'fig' ? 0 : 1) - (oldL.at.get(q.id).k === 'fig' ? 0 : 1) || q.fm.y - p.fm.y : p.fm.y - q.fm.y) || p.fm.x - q.fm.x), step = order.length > 1 ? RIPPLE / (order.length - 1) : 0;
  [...order, ...jobs.filter((j) => j.pour)].forEach((j, n) => {
    const t = tileOf(j.id), { fm, to, r } = j; stand(t, r, L.base); show(t, true);
    // (a tile that goes off the screen flies to the near place, and stands at its own)
    const real = { x: fr1.left, y: fr1.top }, A = { x: fm.x - real.x, y: fm.y - real.y, w: fm.w, h: fm.h }, B = { x: to.x - real.x, y: to.y - real.y, w: to.w, h: to.h };
    if (still) return;
    if (!(FILL && r.k === 'wall') && Math.abs(A.x - B.x) < 0.5 && Math.abs(A.y - B.y) < 0.5 && Math.abs(A.w - B.w) < 0.5 && Math.abs(A.h - B.h) < 0.5) { if (j.pour < 0) { show(t, false); t.r = null; } return; }
    const hold = j.pour ? j.hold : (n * step) / T, a0 = tf(A.x, A.y, A.w / t.bw, A.h / t.bh), b0 = tf(B.x, B.y, B.w / t.bw, B.h / t.bh);
    if (j.pour < 0) anim(t.el, [{ transform: a0, offset: 0, easing: 'linear' }, { transform: a0, offset: hold, easing: 'cubic-bezier(.5,.04,.3,1)' }, { transform: b0, offset: FILL ? Math.min(0.52, hold + 0.4) : Math.min(0.96, hold + 0.66), easing: 'linear' }, { transform: b0, offset: 1 }], 'linear');
    else if (FILL && r.k === 'wall') { const m0 = tf(B.x, B.y - FILL.rise, B.w / t.bw, B.h / t.bh); anim(t.el, [{ transform: a0, offset: 0, easing: 'linear' }, { transform: a0, offset: hold, easing: EASE }, { transform: m0, offset: FILL_M, easing: SETTLE }, { transform: b0, offset: 1 }], 'linear'); }
    else anim(t.el, hold > 0 ? [{ transform: a0, easing: 'linear' }, { transform: a0, offset: hold, easing: EASE }, { transform: b0 }] : [{ transform: a0, easing: EASE }, { transform: b0 }], 'linear');
    zed(t, j.pour && phoneMQ.matches ? (j.pour > 0 ? 7 : 6) : j.fig ? 3 : '');
  });
  
  // 4 · the words, on the same clock
  if (!S.file) { if (!still && f0) figHome(f0, as0, g0, oldL, fr0, opt, vh); else if (g0) g0.remove(); } // (read before the words start moving: where the card will stand)
  if (!still && opt.words) opt.words(dy, vh, fr0, fr1);
  if (!still && oldL && oldL.box && L.box && opt.dir && Math.abs(oldL.bitsY - L.bitsY) > 0.5) anim(bits, [{ transform: `translate(0px,${oldL.bitsY}px)` }, { transform: `translate(0px,${L.bitsY}px)` }]);
  if (!still && L.emptyEl) anim(L.emptyEl, [{ transform: 'scale(.5)' }, { transform: 'none' }]);
  if (!still && emp0) anim(emp0, [{ transform: 'none' }, { transform: 'scale(.001)' }]);
  if (!still && S.file && L.stage) { const A = opt.stageFrom || st0; if (A && (opt.stageFrom || oldL.box) && (Math.abs(A.x - L.stage.x) + Math.abs(A.y - L.stage.y) + Math.abs(A.w - L.stage.w) + Math.abs(A.h - L.stage.h) > 1)) stageFrom(A, L.stage, L.stage); }
  if (S.file) { if (!still) figTo(g0, gr0, gx0, opt, vh); else if (g0) g0.remove(); }
  const n1 = L.box ? L.box.n : 0; if (!opt.keepN) more.textContent = plus(n1);
  const land = () => { if (my !== moveNo) return; const done_ = anims; moving = false;  anims = []; sweep(); bits.classList.remove('bare'); if (gnow) gnow.classList.add('done'); done_.forEach((a) => a.cancel()); jobs.forEach((j) => { if (!j.pour) return; const t = tiles.get(j.id); if (t) zed(t, j.r.k === 'fig' ? 3 : ''); }); if (n1) more.textContent = plus(n1); if (!S.file) { stage.classList.add('off'); $$('.card.lifted').forEach((c) => { c.classList.remove('lifted'); }); } look(); glim.go(); settleWindow(0.02); setTimeout(() => { if (my === moveNo && !moving) settleWindow(0.5); }, 320); setTimeout(() => { if (my === moveNo && !moving) settleWindow(); }, 640); if (opt.done) opt.done(); };
  landNow = land; lastJS = performance.now() - tJS; if (still || !anims.length) return land(); if (!S.file) setTimeout(() => { if (my === moveNo) glim.go(true); }, LEAD + Tm * 0.6);
  anims[0].onfinish = () => setTimeout(land, 0);
}
/* the figure's layer, on the same clock. From a cover it grows out of the cover's own drawing, block for block; back into it the
   same way. From one time frame, or file, to the next the old one slides off the side the new one does not come from */
function fromCover(card, own, pl) { const c = card && $('.fg', card); if (!c) return null; card.getAnimations({ subtree: true }).forEach((a) => a.finish()); const b = c.getBoundingClientRect(), u = Math.min(b.width / CW, b.height / CH), ox = b.left + (b.width - CW * u) / 2, oy = b.top + (b.height - CH * u) / 2, m = miniPad(pl), sx = (((CW - m.l) / pl.w) * u), sy = (((CH - m.t - m.b) / pl.h) * u); return `translate(${(ox + m.l * u - pl.pad.l * sx - own.x).toFixed(1)}px,${(oy + m.t * u - pl.pad.t * sy - own.y).toFixed(1)}px) scale(${sx.toFixed(4)},${sy.toFixed(4)})`; }
// (going home: what stands there shrinks into the cover. Closed on a time frame the cover does not draw, the one it does slides in on the way)
function figHome(f, other, g0, l0, fr0, opt, vh) {
  if (g0) g0.remove(); if (opt.bare) return; const B = room(f, 90, defSort(f)), b = document.createElement('div'), y = fr0.top + l0.figY; b.className = 'glay home'; const pr = page.getBoundingClientRect(); b.style.cssText = `left:${fr0.left - pr.left}px;top:${y - pr.top}px;width:${B.W}px;height:${l0.figH}px;${other ? 'overflow:hidden' : ''}`;
  const put = (e) => { e.className = 'glay'; e.style.top = '0'; e.style.left = '0'; b.append(e); return e; }; if (g0) put(g0); let home = g0; if (other) { home = put(buildFig(B.pl, B.W, B.H, 0)); const W = B.W + 40; anim(home, [{ transform: `translateX(${W}px)` }, { transform: 'none' }]); if (g0) { g0.classList.add('done'); anim(g0, [{ transform: 'none' }, { transform: `translateX(${-W}px)` }]); } }
  if (!home) return; page.append(b); if (other) home.querySelector('.gl').remove(); if (g0 && !other) anim(g0.querySelector('.solid'), [{ opacity: 0 }, { opacity: 1, offset: 0.6 }, { opacity: 1 }], 'linear'); if (g0 && !other) anim(g0.querySelector('.gl'), [{ opacity: 1 }, { opacity: 0, offset: 0.2 }, { opacity: 0 }], 'linear'); const c = opt.card && opt.card(), q = c && c.isConnected && fromCover(c, { x: fr0.left, y }, home._pl); if (FILL) return void anim(b, [{ transform: 'translate(0px,0px) scale(1,1)', easing: EASE }, { transform: `translate(${vw()}px,0px) scale(1,1)`, offset: OUT }, { transform: `translate(${vw()}px,0px) scale(1,1)` }], 'linear'); anim(b, [{ transform: 'translate(0px,0px) scale(1,1)' }, { transform: q || `translate(0px,${vh}px) scale(1,1)` }]);
}
/* a text that both ends have: one copy of it flies from the one to the other, and both stand hidden until it lands */
function flyText(from, to, big, way) { // (big: the file's element, whose size the copy is laid out at)
  if (!from || !to || still) return; const pr = page.getBoundingClientRect(), e = big.cloneNode(true), B = way > 0 ? to.r : from.r, A = way > 0 ? from : to, k = A.fs / (way > 0 ? to.fs : from.fs); e.classList.add('fly'); e.removeAttribute('id'); const c = getComputedStyle(big); e.style.cssText = `left:${B.x - pr.left}px;top:${B.y - pr.top}px;width:${Math.ceil(B.w) + 1}px;font:${c.font};letter-spacing:${c.letterSpacing};color:${c.color};line-height:${c.lineHeight}`;
  page.append(e); [from.el, to.el].forEach((x) => { x.classList.add('flying'); }); const at = `translate(${(A.r.x - B.x).toFixed(1)}px,${(A.r.y - B.y + (A.lh - (way > 0 ? to : from).lh * k) / 2).toFixed(1)}px) scale(${k.toFixed(4)},${k.toFixed(4)})`, rest = 'translate(0px,0px) scale(1,1)'; anim(e, way > 0 ? [{ transform: at }, { transform: rest }] : [{ transform: rest }, { transform: at }]);
}
const textAt = (el) => { if (!el) return null; const c = getComputedStyle(el), r_ = rectOf(el); return { el, r: r_, fs: parseFloat(c.fontSize), lh: parseFloat(c.lineHeight) || parseFloat(c.fontSize) * 1.2 }; };
const cardText = (card) => (card ? { head: textAt($('strong', card)), name: textAt($('small span', card)) } : null), fileText = () => ({ head: textAt($('.verdict', pFile)), name: textAt($('.bar .nm span', pFile)) });
function coverWords(card, b, way) { const e = $('.fg', card).cloneNode(true), pr = page.getBoundingClientRect(); [...e.children].forEach((k) => { if (k.tagName !== 'text') k.remove(); }); e.setAttribute('class', 'fg cvw'); e.style.cssText = `left:${b.x - pr.left}px;top:${b.y - pr.top}px;width:${b.w}px;height:${b.h}px`; page.append(e);
  anim(e, way > 0 ? [{ opacity: 1 }, { opacity: 0, offset: 0.26 }, { opacity: 0 }] : [{ opacity: 0 }, { opacity: 0, offset: 0.62 }, { opacity: 1, offset: 0.9 }, { opacity: 1 }], 'linear'); }
function figTo(g0, gr0, gx0, opt, vh) {
  const W = field.clientWidth + 40, d = opt.dir || 1;
  if (g0 && S.file) { if (!g0.isConnected) gclip.append(g0); anim(g0, [{ transform: `translateX(${gx0}px)` }, { transform: `translateX(${-d * W}px)` }]); }
  if (!gnow) return; gnow._pl = L.fig;
  if (g0 || opt.dir) { gnow.classList.add('done'); anim(gnow, [{ transform: `translateX(${d * W}px)` }, { transform: 'none' }]); return; }
  gclip.classList.add('free'); const own = rectOf(gnow), q = opt.from && fromCover(opt.from, own, L.fig); if (!q) gnow.classList.add('done'); else { anim($('.gl', gnow), [{ opacity: 0 }, { opacity: 0, offset: 0.4 }, { opacity: 1, offset: 0.85 }, { opacity: 1 }], 'linear'); anim($('.solid', gnow), [{ opacity: 1 }, { opacity: 1, offset: 0.3 }, { opacity: 0 }], 'linear'); } anim(gnow, [{ transform: q || `translate(0px,${vh}px) scale(1,1)` }, { transform: 'translate(0px,0px) scale(1,1)' }]);
}
const vw = () => innerWidth;
const plus = (n) => (!n ? '' : n > 999 ? `+${(n / 1000).toFixed(1)}k` : `+${n}`);
// (what leaves with the words is held where it is drawn, out of the page, and carried off from there)
function lift(el, col) { const b = el.getBoundingClientRect(), inner = el.cloneNode(true); let g = inner; const cut = col && phoneMQ.matches, t0 = cut ? Math.max(-8, b.top) : b.top, t1 = cut ? Math.max(t0 + 1, Math.min(b.bottom, innerHeight + deep())) : b.bottom; if (col) { g = document.createElement('div'); g.style.overflow = 'hidden'; g.appendChild(inner); if (cut) { g.className = 'gtail'; inner.style.cssText = `width:${b.width}px;margin-top:${b.top - t0}px;will-change:auto`; g._x = $$('.tips', el).map((e) => e.scrollLeft); } } g._m = cut ? g : inner; g.removeAttribute('id'); $$('[id]', g).forEach((e) => e.removeAttribute('id')); g.classList.add('gone', 'is-leaving'); g._at = [b.left, t0]; Object.assign(g.style, { left: `${b.left}px`, top: `${t0}px`, width: `${b.width}px`, height: `${t1 - t0}px`, transform: 'none' }); return g; }
/* (placed once the page has its new scroll, so each stands on the screen exactly where it stood) */
const pins = () => { const g = $$('.gone', page).filter((e) => e._at); if (!g.length) return; const pr = page.getBoundingClientRect(); g.forEach((e) => { e.style.left = `${e._at[0] - pr.left}px`; e.style.top = `${e._at[1] - pr.top}px`; }); };
const lay = (...gs) => { page.append(...gs); gs.forEach((g) => { if (g._x) $$('.tips', g).forEach((e, i) => { e.scrollLeft = g._x[i] || 0; }); }); };
const late = (a, b) => [{ transform: a, offset: 0, easing: 'linear' }, { transform: a, offset: 0.34, easing: 'cubic-bezier(.22,.6,.2,1)' }, { transform: b, offset: 1 }];
const early = (a, b) => [{ transform: a, offset: 0, easing: 'cubic-bezier(.5,0,.5,1)' }, { transform: b, offset: 0.42, easing: 'linear' }, { transform: b, offset: 1 }];
function wordsTo(dir, oldPanel, newPanel, between) { // (the old words leave on one side as the new ones come in on the other)
  return (dy, vh, fr0, fr1) => {
    const w = phoneMQ.matches ? vw() : head.clientWidth + 56;
    const big = !phoneMQ.matches, up = (e) => -(e.offsetHeight + 120);
    if (oldPanel === pRest && big) { pRest.hidden = false; pRest.classList.add('is-leaving'); { const hr = head.getBoundingClientRect(); Object.assign(pRest.style, { position: 'absolute', left: `${restRect.left - hr.left}px`, right: 'auto', top: `${restRect.top - hr.top}px`, width: `${restRect.width}px` }); } anim(pRest, [{ transform: 'none' }, { transform: `translateY(${up(pRest)}px)` }]); }
    else if (oldPanel && (FILL || POP)) { oldPanel.hidden = false; oldPanel.classList.add('is-leaving'); oldPanel.style.top = `${dy}px`; anim(oldPanel, [{ transform: 'translateX(0px)', easing: EASE }, { transform: `translateX(${w}px)`, offset: OUT }, { transform: `translateX(${w}px)` }], 'linear'); }
    else if (oldPanel) { oldPanel.hidden = false; oldPanel.classList.add('is-leaving'); oldPanel.style.top = `${dy}px`; anim(oldPanel, [{ transform: 'translateX(0)' }, { transform: `translateX(${-dir * w}px)` }]); }
    if (newPanel === pRest && FILL) anim(pRest, [{ transform: `translateY(${-FILL.rise}px)` }, { transform: `translateY(${-FILL.rise}px)`, offset: FILL_M, easing: SETTLE }, { transform: 'translateY(0px)' }], 'linear');
    else if (newPanel === pRest && POP) { const h = Math.max(0, fr1.top + 8); anim(pRest, [{ transform: `translateY(${-h}px)` }, { transform: `translateY(${-h}px)`, offset: 0.18, easing: EASE }, { transform: 'translateY(0px)' }], 'linear'); }
    else if (newPanel === pRest && big) anim(pRest, [{ transform: `translateY(${up(pRest)}px)` }, { transform: 'none' }]);
    else if (big && !between) anim(newPanel, late(`translateX(${dir * w}px)`, 'translateX(0)'), 'linear');
    else anim(newPanel, [{ transform: `translateX(${dir * w}px)` }, { transform: 'none' }]);

    if (phoneMQ.matches) {
      if (dir > 0) anim($('.tail-in', tail), [{ transform: between ? `translateX(${w}px)` : `translateY(${vh * 0.6}px)` }, { transform: 'none' }]);
    } else if (dir > 0) anim($('.tail-in', tail), between ? [{ transform: `translateX(${w}px)` }, { transform: 'none' }] : late(`translateX(${w}px)`, 'translateX(0)'), between ? EASE : 'linear');
    if (between) { /* the sorts and the box stay where they are */ } else if (dir > 0) { bits.classList.add('bare'); anim(bits, [{ transform: `translate(${w + 40}px,${L.bitsY}px)`, offset: 0, easing: 'linear' }, { transform: `translate(${w + 40}px,${L.bitsY}px)`, offset: 0.3, easing: 'cubic-bezier(.22,.6,.2,1)' }, { transform: `translate(0px,${L.bitsY}px)`, offset: 1 }], 'linear'); anim(more, [{ transform: `translate(${L.box.x + 40 + vw() - field.getBoundingClientRect().left - L.box.x}px,${L.box.y}px)` }, { transform: `translate(${L.box.x}px,${L.box.y}px)` }]); }

  };
}
let live = {}; // (where the reading was drawn, when a move is cut short by the next)
function setWin(d, quiet) {
  const i = FR.indexOf(d); S.win = d; chrome();
  $$('.wins button', pFile).forEach((b) => b.setAttribute('aria-pressed', String(Number(b.dataset.win) === d)));
  const tr = $('#track'), mk = $('.wins i', pFile), clip = tr.parentNode, after = $('#after'), paras = $$('.para', tr);
  const x0 = live.tr || tr.style.transform, m0 = live.mk || mk.style.transform; live = {};
  tr.style.transform = `translateX(${-i * 100}%)`; mk.style.transform = `translateX(${i * 100}%)`;
  // (what follows the paragraph rides up or down to meet it)
  const h0 = clip.offsetHeight, h1 = paras[i].offsetHeight; paras.forEach((p, k) => p.classList.toggle('on', k === i)); clip.style.height = `${h1}px`;
  if (!quiet && !still) { tr.classList.add('is-moving'); clearTimeout(tr._t); tr._t = setTimeout(() => tr.classList.remove('is-moving'), ALLT + 40); anim(tr, [{ transform: x0 }, { transform: tr.style.transform }]); anim(mk, [{ transform: m0 }, { transform: mk.style.transform }]); if (h0 !== h1) anim(after, [{ transform: `translateY(${h0 - h1}px)` }, { transform: 'none' }]); }
}
let folT = 0; const showFol = (ms) => { clearTimeout(folT); folT = setTimeout(() => { const e = $('.fol', pFile); if (e) e.classList.remove('yet'); }, still ? 0 : ms); };
function open(f, card) {
  if (card && !card.classList.contains('card')) card = null;
  const was = S.file; if (was === f) return; if (!was) { savedY = scrollY; stripX = $$('.strip').map((e) => e.scrollLeft); restRect = pRest.getBoundingClientRect(); setTimeout(look, 560); }
  const ct = card && !was ? cardText(card) : null; if (card && !was) card.classList.add('lifted');
  const oldTail = was ? lift($('.tail-in', tail), true) : null;
  move(() => {
    closeHits(); page.className = 'page is-file'; S.file = f; S.win = 90; S.sort = defSort(f); S.met = 'likes'; chrome();
    if (was) { lay(oldTail); // (from one file to the next: the old words are copied so both can be on the move)
      const ghost = pFile.cloneNode(true); ghost.removeAttribute('id'); ghost.removeAttribute('data-hid'); ghost.classList.add('is-leaving', 'ghost'); head.appendChild(ghost); pFile._ghost = ghost;
    }
    pRest.hidden = true; pFile.hidden = false; drawFile(f); showFol(LEAD + T * 0.72); { const v = $('.verdict', pFile), k = parseFloat(getComputedStyle(v).fontSize) / 18, w = ct ? ct.head.r.w : strongW; if (w) v.style.maxWidth = `${Math.ceil(w * k) + 1}px`; } setWin(90, true);
  }, { scroll: 0, dir: was ? 1 : 0, from: card, stageFrom: was ? null : card ? rectOf(card) : { x: 16, y: innerHeight, w: innerWidth - 32, h: 300 }, words: (dy, vh, fr0, fr1) => { if (ct) { coverWords(card, rectOf($('.fg', card)), 1); const ft = fileText(); flyText(ct.head, ft.head, ft.head.el, 1); anim(ft.name.el.parentNode, [{ opacity: 0 }, { opacity: 0, offset: 0.45 }, { opacity: 1, offset: 0.8 }, { opacity: 1 }], 'linear'); } const g = pFile._ghost; pFile._ghost = null; wordsTo(1, g || pRest, pFile, Boolean(g))(dy, vh, fr0, fr1); if (oldTail) anim(oldTail._m, [{ transform: 'none' }, { transform: `translateX(${-(phoneMQ.matches ? vw() : head.clientWidth + 56)}px)` }]); } , done: () => $$('.ghost', head).forEach((e) => e.remove()) });
}
const exitY = () => Math.max(0, Math.round($('.tabs', pRest).getBoundingClientRect().top + scrollY - 12));
function close() {
  if (!S.file) return;
  const side = !phoneMQ.matches, fr_ = field.getBoundingClientRect(), top_ = fr_.top + L.figY, seenH = Math.min(innerHeight, top_ + L.figH) - Math.max(0, top_), home_ = $(`.strip:not(.off) [data-file="${S.file.id}"]`) || $(`.strip [data-file="${S.file.id}"]`), pop = !side && Boolean(home_) && seenH > Math.min(180, L.figH * 0.45), bare = !side && !pop && fr_.bottom < 20, shut = S.file, ft0 = fileText(), stOld = rectOf(stage), g = [lift($('.tail-in', tail), true), ...(side ? [lift(pFile, true)] : []), ...[bits, more].filter((e) => !bare && !e.classList.contains('is-hid')).map((e) => { const q = lift(e); if (e === bits) (q._m || q).classList.add('bare'); return q; })];
  move(() => { lay(...g); page.className = 'page is-rest'; S.file = null; chrome(); pFile.hidden = true; pRest.hidden = false; $$('.card.lifted').forEach((c) => c.classList.remove('lifted')); $$('.strip').forEach((e, i) => { e.scrollLeft = stripX[i] || 0; }); if (pop) { const e = home_.closest('.strip'), k_ = Number(e.dataset.strip); if (k_ !== tab) { tab = k_; $$('[data-tab]').forEach((b) => b.setAttribute('aria-pressed', String(Number(b.dataset.tab) === k_))); $$('.strip').forEach((v, i) => v.classList.toggle('off', i !== k_)); } const x = home_.offsetLeft - e.offsetLeft - e.scrollLeft; if (x < 8 || x + home_.offsetWidth > e.clientWidth - 8) e.scrollLeft = home_.offsetLeft - e.offsetLeft - 16; } tail.innerHTML = ''; }, { scroll: side ? savedY : exitY, bare, pop, fill: !side && !pop, card: () => { const c = $(`.strip:not(.off) [data-file="${shut.id}"]`), b = c && !bare && !FILL && rectOf(c); return b && b.x < vw() && b.x + b.w > 0 && b.y + b.h > 0 ? c : null; }, words: (dy, vh, fr0, fr1) => {
    const c = $(`.strip:not(.off) [data-file="${shut.id}"]`), cr = c && rectOf(c), own = rectOf(stage), seen = !bare && !FILL && cr && cr.x < vw() && cr.x + cr.w > 0 && cr.y + cr.h > 0; if (c && seen) { c.classList.add('lifted'); coverWords(c, rectOf($('.fg', c)), -1); const ct = cardText(c); flyText(ft0.head, ct.head, ft0.head.el, -1); anim(ft0.name.el.parentNode, [{ opacity: 1 }, { opacity: 0, offset: 0.18 }, { opacity: 0 }], 'linear'); }
    if (bare) stage.classList.add('off'); else stageFrom(stOld, seen ? cr : { ...stOld, y: vh + 40 }, own);
    wordsTo(-1, side ? null : pFile, pRest)(dy, vh, fr0, fr1);
    g.forEach((e, i) => i < 2 && side ? anim(e._m, early('translateX(0)', `translateX(${e.offsetWidth + 60}px)`), 'linear') : anim(e._m, FILL || POP ? [{ transform: 'translateX(0px)', easing: EASE }, { transform: `translateX(${vw()}px)`, offset: OUT }, { transform: `translateX(${vw()}px)` }] : [{ transform: 'none' }, { transform: i === 0 && side ? `translateX(${head.clientWidth + 56}px)` : e._m.classList.contains('more') ? `translateX(${vw() - parseFloat(e._m.style.left) + 40}px)` : `translateY(${vh}px)` }])); } });
}
const frame = (d) => { if (!S.file || d === S.win) return; const dir = d > S.win ? 1 : -1; move(() => setWin(d), { dir }); };
const VIEWS = ['layer', 'recent', 'rank'], MK = ['likes', 'comments', 'plays'], sortBy = (k) => { if (k === S.sort || !VIEWS.includes(k)) return; const dir = VIEWS.indexOf(k) > VIEWS.indexOf(S.sort) ? 1 : -1; move(() => { S.sort = k; }, { dir }); };
/* on another number: Ranked is ranked again; Recent keeps its order, and what beat usual on that number is marked */
const setMet = (m) => { if (m === S.met || S.sort === 'layer') return; const dir = MK.indexOf(m) > MK.indexOf(S.met) ? 1 : -1; if (S.sort === 'rank') return move(() => { S.met = m; }, { dir }); unpick(); S.met = m; drawBits(); strips(); };
const strips = () => { const m = met(); tiles.forEach((t) => { if (t.r && t.r.k === 'fig') t.el.classList.toggle('landed', over(t.id, m)); }); };

/* ── a post picked out by the words ── */
function setLit(ids, pin) {
  S.lit = ids; S.pin = pin || null; const set = new Set(ids);
  const any = ids.some((id) => tiles.get(id)?.on && L.at.get(id).k === 'fig'); field.classList.toggle('has-lit', any && Boolean(S.file));
  tiles.forEach((t) => t.el.classList.toggle('lit', set.has(t.id) && t.on));
  $$('.ref', tail).forEach((e) => e.classList.toggle('on', set.has(e.dataset.id))); drawBits();
}
function clearLit(quiet) { if (!S.lit.length && !S.pin) return; S.lit = []; S.pin = null; field.classList.remove('has-lit'); tiles.forEach((t) => t.el.classList.remove('lit')); $$('.ref.on').forEach((e) => e.classList.remove('on')); if (!quiet) drawBits(); }

/* ── a tap on a post: its trail, as the app's wall plays it. Going back in time from the post, the run of posts it beat (or that
   did better), then the one that stopped it. Judged on one yardstick: day 7, or an early post at its own checkpoint ── */
function trailOf(id, pool, m = 'likes') {
  const at = (x) => P[x].at?.[String(drop.n)] || {}, early = (x) => at(x).stage === 'EARLY', cp = m === 'likes' && early(id) ? (['d3', 'd1'].find((k) => at(id).x_by && at(id).x_by[k] != null) || null) : null, yard = (x) => (cp ? at(x).x_by?.[cp] ?? null : METS[m](x)), on = m === 'likes' ? '' : ` on ${m}`;
  if (early(id) && !cp) return { up: false, chain: [], end: null, text: 'Too early to judge' };
  const ch = pool.filter((x) => yard(x) != null), i = ch.indexOf(id); if (i < 0) return { up: false, chain: [], end: null, text: m === 'plays' && P[id].format !== 'reel' ? 'Not a reel, so no plays' : 'Too new to judge' }; if (i === 0) return { up: false, chain: [], end: null, text: 'The oldest one here' };
  const better = (x, y) => yard(x) > yard(y) || (!cp && yard(x) === yard(y) && P[x].likes > P[y].likes), good = (x) => (cp ? yard(x) >= 1 : over(x, m)), up = good(id), chain = []; let end = null;
  for (let k = i - 1; k >= 0; k--) { if (up ? better(id, ch[k]) : better(ch[k], id)) chain.push(ch[k]); else { end = ch[k]; break; } }
  const n = chain.length, fmt = (x) => (m === 'plays' ? kilo(yard(x)) : mult(yard(x))), me = fmt(id), show = (x) => (x !== id && m !== 'plays' && yard(x) < 10 && fmt(x) === me ? `${yard(x).toFixed(2)}x` : fmt(x)), w = on || (cp ? ` at day ${cp.slice(1)}` : ''), all = n === i, say = up ? (all && n > 1 ? `Beat all ${n} before it` : n === 1 ? 'Beat the last one' : `Beat the last ${n}`) : all && n > 1 ? `Under all ${n} before it` : n === 1 ? 'Under the last one' : `Under the last ${n}`;
  return { up, chain, end, show: (x) => (x === id && [...chain, end].some((y) => y && fmt(y) === me) && m !== 'plays' && yard(id) < 10 ? `${yard(id).toFixed(2)}x` : show(x)), good, text: n ? say + w : up ? `Right after a ${show(end)}` : `Over the ${show(end)} before it`, short: n ? say : '', live: (k) => `${up ? 'Beat' : 'Under'} the last <b>${k}</b>`, tail: cp ? `Counted at day ${cp.slice(1)}` : end ? (up ? `Stopped by a ${show(end)}` : `Ahead of a ${show(end)}`) : said(id, m).replace(/<[^>]+>/g, '') };
}
/* in Ranked: the ones that rank above it here, nearest first, nearest first. The top one's trail runs down instead */
function rankTrail(id, list, m) {
  const R = rankOf(m), l = list.filter((x) => R.map.has(x)), i = l.indexOf(id), k = R.map.get(id); if (i < 0) return { up: false, chain: [], end: null, text: m === 'plays' && P[id].format !== 'reel' ? 'Not a reel, so no plays' : 'Too new to rank' };
  const first = i === 0, chain = first ? l.slice(1) : l.slice(0, i).reverse(), here = `${ord(i + 1)} of ${l.length} here`;
  return { rank: true, up: first, chain, end: null, good: (x) => over(x, m), show: (x) => `#${R.map.get(x)}`, text: `${ord(k)} of ${R.n} on the feed${m === 'likes' ? '' : `, by ${m}`}`, short: `${ord(k)} of ${R.n} on the feed`, live: (q_) => `<b>${q_}</b> ${first ? 'under' : 'over'} it here`, tail: first ? `First of ${l.length} here` : here };
}
/* the navbar's three sizes */
/* what the app's chrome is told */
let told = ''; const chrome = () => { if (dead) return; const c = { file: S.file ? { id: S.file.id, name: S.file.name } : null, win: S.win, away: navAt !== 'dock', art }, k = JSON.stringify([c.file, c.win, c.away, Boolean(art)]); if (k === told) return; told = k; onChrome(c); };
const nav = $('#nav', layer), FACE = Object.fromEntries(['m', 't', 'b', 'tl', 'tr', 'bl', 'br'].map((k) => [k, $(`.nav__face .${k}`, layer)])); FACE.grab = $('.nav__grab', layer); let navAt = 'dock', sheetH = 480, MORT = {}, rp = null;
MORT = mortems || {};
/* the bar as it stands: whole, or folded to its circle. (A laptop has none: the line comes up from under the screen) */
const mine = (t, s) => { const e = t && t.closest ? t.closest(s) : null; return e && e !== root && e !== layer && (root.contains(e) || layer.contains(e)) ? e : null; }, dockEl = () => document.querySelector('.fm-dock'), dockBox = () => { const d = dockEl(), b = d && d.querySelector('.fm-dock-bar'); if (!b) return { w: 184, h: 64, b: -140 }; return { w: d.hasAttribute('data-folded') ? 64 : Math.round(b.getBoundingClientRect().width) || 312, h: 64, b: 0 }; };
function navRect(st) { const phone = phoneMQ.matches, vw_ = innerWidth, gw = phone ? Math.min(330, vw_ - 44) : 400, sw = phone ? vw_ - 16 : Math.min(980, vw_ - 160), sh = phone ? sheetH : Math.min(660, innerHeight - 72); const gx = phone ? 0 : (() => { const f = field.getBoundingClientRect(); return Math.round(f.left + f.width / 2 - vw_ / 2); })(); return st === 'glance' ? { w: gw, h: 64, b: 0, x: gx } : st === 'sheet' ? { w: sw, h: sh, b: phone ? 0 : (innerHeight - sh) / 2 - 24 } : dockBox(); }
function navPose({ w, h, b, x = 0 }) { const l = x - w / 2, t = -b - h, q = (x, y, a = 1, c = 1) => `translate(${x.toFixed(1)}px,${y.toFixed(1)}px) scale(${a.toFixed(4)},${c.toFixed(4)})`; return { tl: q(l, t), tr: q(l + w - 32, t), bl: q(l, -b - 32), br: q(l + w - 32, -b - 32), t: q(l + 31, t, (w - 62) / 100), b: q(l + 31, -b - 32, (w - 62) / 100), m: q(l, t + 31, w / 100, Math.max(0.0001, (h - 62) / 100)) }; }
function navVars() { const g = navRect('glance'), k = navRect('sheet'); nav.style.setProperty('--gw', `${g.w}px`); nav.style.setProperty('--gx', `${g.x || 0}px`); nav.style.setProperty('--sw', `${k.w}px`); nav.style.setProperty('--sh', `${k.h}px`); nav.style.setProperty('--sb', `${k.b}px`); }
function faceTo(pose, ms, ease = EASE) { Object.keys(FACE).forEach((k) => { const e = FACE[k], m = new DOMMatrix(getComputedStyle(e).transform), from = `translate(${m.e}px,${m.f}px) scale(${m.a},${m.d})`; e.getAnimations().forEach((a) => { if (!(a instanceof CSSTransition)) a.cancel(); }); e.style.transform = pose[k]; if (!still && ms) e.animate([{ transform: from, easing: ease }, { transform: pose[k], offset: 0.8 }, { transform: pose[k] }], { duration: ms }); }); }
let awayT = 0; function setNav(st, at) {
  if (st === navAt && !at) return; const was = navAt; navAt = st; clearTimeout(awayT); if (st === 'dock' && was !== 'dock' && !at) awayT = setTimeout(chrome, 400); else chrome(); navVars(); nav.dataset.at = st; $('#veil', layer).classList.toggle('on', st === 'sheet' && !phoneMQ.matches); if (was === 'sheet' && st !== 'sheet') stopReplay(); const pose = navPose(navRect(st));
  const to_ = navRect(st); pose.grab = `translate(${(to_.x || 0).toFixed(1)}px,${-to_.b - to_.h + (st === 'sheet' ? 9 : 5)}px) scale(1,1)`; faceTo(pose, at ? 0 : st === 'sheet' || was === 'sheet' ? 800 : 650);
}
const verdictOf = (id) => { const p = P[id]; return judged(id) ? `<b class="v${p.landed ? ' l' : ''}">${p.landed ? 'Landed' : 'Missed'}</b> at ${mult(x7(id))}` : p.x_likes != null ? `<b>Early</b>, at ${mult(p.x_likes)} so far` : '<b>Too new to call</b>'; };
const kindOfPost = (p) => (p.format === 'reel' ? `Reel${p.seconds ? `, ${p.seconds}s` : ''}` : p.slides > 1 ? `${p.slides} slides` : 'Photo');
/* the glance: the post, how it did, and its trail counted as it lights */
function drawGlance(id, tr) {
  const e = $('.nc-glance', nav), [l1, l2] = $$('.gl1, .gl2', e), stop = $('.nc__stop', e), n = tr.chain.length, bold = (t) => esc(t).replace(/(\d+(?:st|nd|rd|th)?)/, '<b>$1</b>'); $('img', e).src = P[id].cover; e.classList.remove('has-stop'); e.classList.toggle('is-ink', !(tr.good && tr.good(id)));
  const put = (el, html) => { const a = el.firstElementChild, b = document.createElement('span'); b.innerHTML = html; el.append(b); if (still) return a && a.remove(); const o = { duration: 420, easing: EASE, fill: 'both' }; b.animate([{ transform: 'translateY(100%)' }, { transform: 'translateY(0)' }], o); if (a) a.animate([{ transform: 'translateY(0)' }, { transform: 'translateY(-100%)' }], o).onfinish = () => a.remove(); };
  l1.innerHTML = `<span>${said(id, met())}</span>`; l2.innerHTML = `<span>${n ? '&nbsp;' : esc(tr.text)}</span>`;
  return { tick: (k) => { l2.firstElementChild.innerHTML = tr.live(k); }, end: () => { if (tr.short) { put(l1, bold(tr.short)); put(l2, esc(tr.tail)); } if (tr.end && !tr.rank) { $('img', stop).src = P[tr.end].cover; e.classList.add('has-stop'); } } };
}
/* the post mortem: the navbar opened out. The replay runs its stills by itself, the read is under it */
function openMortem(id, again) {
  const p = P[id], m = MORT[p.code], fr = p.frames && p.frames.length ? p.frames : [{ src: p.cover }], reel = p.format === 'reel', L_ = (i, f) => (reel ? f.t || '' : fr.length > 1 ? `Slide ${i + 1} of ${fr.length}` : ''), say = (f, i) => `${L_(i, f) ? `<small class="num">${L_(i, f)}</small>` : ''}${f.shows ? `<p>${esc(f.shows)}</p>` : ''}${f.said ? `<p>“${esc(f.said)}”</p>` : ''}`, tr = trailNow(id), num = (x, k, c) => `<div><b class="${x > 1 ? 'l' : ''}">${x == null ? '·' : mult(x)}</b><span>${k}</span><span class="num">${c == null ? 'hidden' : kilo(c)}</span></div>`;
  const own = (t) => refs(t.replace(MARK, (x, i, w) => (i === id ? w : x))), pm = $('#pm', layer); pm.scrollTop = 0; pm.style.setProperty('--wr', reel ? 0.5625 : 0.8);
  pm.innerHTML = `<div class="pm__h"><div><strong>${verdictOf(id)}</strong><span class="num">${short(p.date)} · ${kindOfPost(p)}${tr.chain.length || tr.rank ? ` · ${esc(tr.text)}` : ''}</span></div><button class="pm__x" aria-label="Close the post mortem"><svg viewBox="0 0 24 24"><path d="M6 6l12 12M18 6L6 18"/></svg></button></div>
    <div class="rp"><div class="rp__l"><div class="rp__bars">${fr.map(() => '<i><b></b></i>').join('')}</div><div class="rp__f">${fr.map((f, i) => `<img src="${f.src}" alt="" decoding="sync" draggable="false"${i ? '' : ' class="at"'}>`).join('')}<button aria-label="Back"></button><button aria-label="On"></button></div></div><div class="rp__s">${fr.map((f, i) => `<div${i ? '' : ' class="at"'}>${say(f, i)}</div>`).join('')}</div></div>
    <div class="pm__r">${m ? `<h3>${own(m.h)}</h3><p>${own(m.r)}</p>${m.f ? `<p><b>Day 21</b> ${refs(m.f)}</p>` : ''}` : `<h3>Too new for a post mortem</h3><p>${esc(p.told || '')}</p>`}</div><div class="pm__s2">${say(fr[0], 0)}</div>
    <div><div class="pm__n">${num(p.x_likes, 'likes', p.likes)}${num(p.x_comments, 'comments', p.comments)}${reel ? (p.x && p.x.plays != null ? num(p.x.plays, 'plays', p.plays) : `<div><b>${p.plays == null ? '·' : kilo(p.plays)}</b><span>plays</span><span>${p.plays == null ? 'not counted yet' : 'in all'}</span></div>`) : ''}</div><a class="pm__o" href="${esc(p.url)}" target="_blank" rel="noopener">Open on Instagram<svg viewBox="0 0 24 24"><path d="M7 17L17 7M9 7h8v8"/></svg></a></div>`;
  rp = { id, n: fr.length, i: 0, fr, say, an: null }; if (phoneMQ.matches) { pm.style.height = 'auto'; nav.style.setProperty('--sw', `${innerWidth - 16}px`); sheetH = Math.min(Math.round(innerHeight * 0.76), pm.scrollHeight); pm.style.height = ''; navVars(); }
  if (navAt === 'sheet') setNav('sheet', true); else setNav('sheet'); if (!again) setTimeout(() => runReplay(), 620); else runReplay();
}
function stopReplay() { if (rp && rp.an) { rp.an.cancel(); rp.an = null; } }
function runReplay() { if (!rp || navAt !== 'sheet') return; const bars = $$('.rp__bars i', nav); bars.forEach((b, i) => b.classList.toggle('past', i < rp.i)); stopReplay(); const b = $('b', bars[rp.i]); if (!b) return; if (rp.n < 2 || still) { bars[rp.i].classList.add('past'); return; } rp.an = b.animate([{ transform: 'scaleX(0)' }, { transform: 'scaleX(1)' }], { duration: 3000, fill: 'forwards' }); rp.an.onfinish = () => { if (rp && rp.i < rp.n - 1) stepReplay(1); else if (bars[rp.i]) bars[rp.i].classList.add('past'); }; }
function stepReplay(d) {
  if (!rp) return; const to = d > 0 && rp.i === rp.n - 1 ? 0 : Math.max(0, Math.min(rp.n - 1, rp.i + d)); if (to === rp.i) return void runReplay(); const im = $$('.rp__f img', nav), a = im[rp.i], b = im[to]; im.forEach((x) => { x.getAnimations().forEach((k) => k.cancel()); x.classList.toggle('at', x === a || x === b); x.style.zIndex = x === b ? 1 : 0; });
  const hide = () => im.forEach((x, i) => x.classList.toggle('at', i === rp.i)); rp.i = to; if (still) hide(); else { const o = { duration: 520, easing: 'linear', fill: 'both' }, k = (x, y) => [{ transform: x, easing: EASE }, { transform: y, offset: 0.8 }, { transform: y }]; const A = a.animate(k('translateX(0%)', `translateX(${-d * 30}%)`), o), B = b.animate(k(`translateX(${d * 100}%)`, 'translateX(0%)'), o); B.onfinish = () => { if (rp && im[rp.i] === b) hide(); A.cancel(); B.cancel(); }; }
  $$('.rp__s > div', nav).forEach((x, i) => x.classList.toggle('at', i === to)); const s2 = $('.pm__s2', nav); if (s2) s2.innerHTML = rp.say(rp.fr[to], to); runReplay();
}
nav.addEventListener('click', (e) => {
  if (Date.now() - swiped < 400) return;
  if (mine(e.target, '.nc-glance')) return void openMortem(picked); if (mine(e.target, '.pm__x')) return void setNav('glance');
  const f = mine(e.target, '.rp__f button'); if (f) return void stepReplay(f === f.parentNode.querySelector('button') ? -1 : 1);
  const rf = mine(e.target, '.ref'); if (rf) { e.stopPropagation(); const id = rf.dataset.id; if (P[id]) { const q = L.at.get(id); if (q && q.k !== 'box' && tiles.get(id)?.on) { pickPost(id); openMortem(id, true); } else openMortem(id, true); } }
});
$('#veil', layer).addEventListener('click', () => setNav('glance'));
let swiped = 0, picked = null, trailT = []; const TCLS = ['is-sel', 'is-hop', 'is-stop', 't-hit', 't-miss', 'on'];
function unpick() { trailT.forEach(clearTimeout); trailT = []; if (!picked) return; picked = null; field.classList.remove('is-trail'); tiles.forEach((t) => { t.el.classList.remove(...TCLS); }); $$('.wash', field).forEach((e) => e.remove()); $$('.ref.on').forEach((e) => e.classList.remove('on')); setNav('dock'); }
const figNow = () => (S.file ? [...L.at].filter(([, q]) => q.k === 'fig').sort((a, b) => a[1].i - b[1].i).map(([x]) => x) : ALL), trailNow = (id) => (S.file && S.sort === 'rank' ? rankTrail(id, figNow(), S.met) : trailOf(id, figNow().slice().sort((a, b) => P[a].ts - P[b].ts), met()));
function pickPost(id) {
  if (moving) { if (performance.now() - moveAt < LEAD + Tm + 110) return; landNow(); } const was = picked; unpick(); if (!id) return; const t0 = tiles.get(id), r0 = L.at.get(id); if (!t0 || !t0.on || !r0 || r0.k === 'box') return; clearLit(true);
  picked = id; const tr = trailNow(id), n = tr.chain.length;
  // (the wall glides just far enough for the post and its trail to stand clear of the navbar)
  let moved = false; if (!S.file) { const ys = [id, ...tr.chain, ...(tr.end ? [tr.end] : [])].map((x) => L.at.get(x)).filter(Boolean).map((q) => q.y + q.h + origin - scrollY), y0 = r0.y + origin - scrollY, low = Math.max(...ys), over = low - (innerHeight - 112), go = Math.min(over, y0 - 84); if (go > 12) { scrollBy({ top: go, behavior: still ? 'auto' : 'smooth' }); moved = true; } }
  field.classList.add('is-trail'); t0.el.classList.add('on'); const col = (x) => (tr.good && tr.good(x) ? 't-hit' : 't-miss'); t0.el.classList.add('is-sel', col(id)); t0.ring.dataset.n = tr.show && (n || tr.end) ? tr.show(id) : ''; $$('.ref', tail).forEach((e) => e.classList.toggle('on', e.dataset.id === id));
  const say = drawGlance(id, tr); setNav('glance');
  const steps = n + (tr.end ? 1 : 0), beat = steps ? Math.max(45, Math.min(110, 1500 / steps)) : 0, at0 = moved ? 460 : 260, lastI = beat ? Math.ceil(1500 / beat) : 0, when = (i) => at0 + Math.min(i, lastI) * beat, later = (ms, f) => { if (still) f(); else trailT.push(setTimeout(f, ms)); };
  const hit = (x, cls, ink) => { const t = tiles.get(x); if (!t || !t.on) return; t.el.classList.add(...cls); if (still) return; const w = document.createElement('i'); w.className = `wash${ink ? ' ink' : ''}`; t.inn.appendChild(w); w.animate([{ opacity: 1 }, { opacity: 0 }], { duration: 970, easing: 'cubic-bezier(.2,0,.2,1)', fill: 'forwards' }).onfinish = () => w.remove(); };
  tr.chain.forEach((x, i) => later(when(i), () => { const t = tiles.get(x); if (t) t.ring.dataset.n = tr.show(x); hit(x, ['on', 'is-hop', col(x)], col(x) === 't-miss'); say.tick(i + 1); }));
  // (what ended it, a beat after the run: it steps forward, framed like the one tapped)
  if (tr.end) later(when(n) + 160, () => { const t = tiles.get(tr.end); if (!t || !t.on) return; t.ring.dataset.n = tr.show(tr.end); t.el.classList.add('on', 'is-stop', col(tr.end)); });
  if (n || tr.end) later(when(n) + 420, () => say.end());
}

/* ── hands ── */
field.addEventListener('click', (e) => {
  if (Date.now() - swiped < 400) return;
  const so = mine(e.target, '[data-sort]'); if (so) return void sortBy(so.dataset.sort); const mt = mine(e.target, '[data-met]'); if (mt) return void setMet(mt.dataset.met);
  if (mine(e.target, 'a')) return;
  const t = mine(e.target, '.tile'); if (!t) { clearLit(); return; }
  if (picked && t.dataset.id === picked) openMortem(picked); else pickPost(t.dataset.id);
});
on(document, 'click', (e) => {
  if (!picked || layer.contains(e.target)) return; if (!root.contains(e.target)) { if (navAt !== 'sheet') unpick(); return; } const t = mine(e.target, '.tile'); if (navAt !== 'sheet' && t && t.dataset.id === picked) return;
  e.stopPropagation(); e.preventDefault(); if (Date.now() - swiped < 400) return; if (navAt === 'sheet') setNav('glance'); else unpick();
}, true);
on(document, 'click', (e) => {
  if (!root.contains(e.target)) { if (!layer.contains(e.target)) closeHits(); return; }
  const b = mine(e.target, '[data-file]'); if (b) return void open(everyFile().find((f) => f.id === b.dataset.file), b.classList.contains('card') ? b : null);
  if (mine(e.target, '#today')) return void open(TODAY.file);
  if (mine(e.target, '.back')) return void close();
  const w = mine(e.target, '[data-win]'); if (w && w.tagName === 'BUTTON') return void frame(Number(w.dataset.win));
  const fo = mine(e.target, '.follow'); if (fo && S.file) { S.file.followed = !S.file.followed; fo.classList.remove('yet', 'born'); fo.classList.toggle('on', S.file.followed); fo.setAttribute('aria-pressed', String(S.file.followed)); keepFollow(S.file); drawStrips(); return; }
  const tb = mine(e.target, '[data-tab]'); if (tb) return void setTab(Number(tb.dataset.tab));
  const r = mine(e.target, '.ref'); if (r) { const q = L.at.get(r.dataset.id); if (q && q.k === 'fig') pickPost(r.dataset.id); else { unpick(); S.pin === r.dataset.id ? clearLit() : setLit([r.dataset.id], r.dataset.id); } return; }
  if (!mine(e.target, '.find')) closeHits();
});
// (a mouse on a sentence lifts the posts it is about; a picked post stays picked)
tail.addEventListener('pointerover', (e) => { if (e.pointerType !== 'mouse' || S.pin || picked || moving) return; const s = mine(e.target, '.s[data-ids]'); if (s) setLit(s.dataset.ids.split(' ')); else if (S.lit.length) clearLit(); });
tail.addEventListener('pointerleave', () => { if (!S.pin) clearLit(); });
on(window, 'keydown', (e) => {
  if (e.defaultPrevented || e.metaKey || e.ctrlKey || e.altKey || (e.target.matches && e.target.matches('input, textarea, select, [contenteditable="true"]'))) return;
  if (navAt === 'sheet' && (e.key === 'ArrowLeft' || e.key === 'ArrowRight')) { e.preventDefault(); return void stepReplay(e.key === 'ArrowRight' ? 1 : -1); }
  if (e.key === 'Escape') { if (navAt === 'sheet') setNav('glance'); else if (picked) unpick(); else if (S.pin) clearLit(); else close(); return; }
  if (!S.file || (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight')) return; e.preventDefault();
  const i = FR.indexOf(S.win) + (e.key === 'ArrowRight' ? 1 : -1); if (FR[i]) frame(FR[i]);
});
// (a finger across the posts, the row or the paragraph changes the time frame: left is longer. It never takes the scroll)
{ let st = null;
  const down = (e) => { const t = e.touches[0]; st = S.file && e.touches.length === 1 && t.clientX > 24 && !mine(e.target, '.tips, .wkc') ? { x: t.clientX, y: t.clientY, go: null } : null; };
  const go = (e) => { if (!st) return; const t = e.touches[0], dx = t.clientX - st.x, dy = t.clientY - st.y; if (st.go === null && Math.hypot(dx, dy) > 8) st.go = Math.abs(dx) > Math.abs(dy) * 1.3; if (st.go === false) { st = null; return; } if (st.go && Math.abs(dx) > 34) { const i = FR.indexOf(S.win) + (dx < 0 ? 1 : -1); st = null; swiped = Date.now(); if (FR[i]) frame(FR[i]); } };
  [field, head, tail].forEach((el) => { el.addEventListener('touchstart', down, { passive: true }); el.addEventListener('touchmove', go, { passive: true }); el.addEventListener('touchend', () => { st = null; }, { passive: true }); });
  pFile.style.touchAction = 'pan-y'; }
/* ── the way back: the bar, or the circle it folds to, with the wall in small on it ── */
let giveF = 0; const give = (f, k = 0.1) => { giveF = f * k; nav.style.transform = f ? `scale(${(1 + giveF * 0.3).toFixed(4)},${(1 - giveF).toFixed(4)})` : ''; }, unGive = () => { const a = giveF; give(0); if (!still && a > 0) nav.animate([{ transform: `scale(${1 + a * 0.3},${1 - a})` }, { transform: 'scale(1,1)' }], { duration: 560, easing: 'cubic-bezier(.3,1.6,.5,1)' }); };
/* the wall in small, with the way back on it: nine covers, the dark over them and the arrow, drawn as one picture, for the dock's face while a file is open */
let art = null; setTimeout(async () => { const load = (id) => new Promise((ok) => { const im = new Image(); im.onload = () => ok(im); im.onerror = () => ok(null); im.src = P[id].cover; }), ims = await Promise.all(ALL.slice(0, 9).map(load)); if (dead) return; const c = document.createElement('canvas'); c.width = c.height = 192; const g = c.getContext('2d'); g.fillStyle = '#15161a'; g.fillRect(0, 0, 192, 192); ims.forEach((im, n) => { if (!im) return; const w = im.naturalWidth, h = im.naturalHeight, s = Math.min(w, h); g.drawImage(im, (w - s) / 2, (h - s) * 0.4, s, s, (n % 3) * 65, Math.floor(n / 3) * 65, 62, 62); }); g.fillStyle = 'rgb(11 11 15 / .52)'; g.fillRect(0, 0, 192, 192); g.strokeStyle = '#fff'; g.lineWidth = 13; g.lineCap = g.lineJoin = 'round'; g.beginPath(); g.moveTo(107, 68); g.lineTo(79, 96); g.lineTo(107, 124); g.stroke(); try { art = c.toDataURL('image/jpeg', 0.86); } catch {} chrome(); }, 400);
{ const pm = $('#pm', layer);
  // (the post mortem: from its top, pulled down, it folds back to its line)
  let st = null; pm.addEventListener('touchstart', (e) => { st = e.touches.length === 1 && pm.scrollTop <= 0 && navAt === 'sheet' ? { y: e.touches[0].clientY, x: e.touches[0].clientX, go: null, dy: 0, t: e.timeStamp } : null; }, { passive: true });
  pm.addEventListener('touchmove', (e) => { if (!st) return; const t = e.touches[0], dy = t.clientY - st.y, dx = t.clientX - st.x; if (st.go === null && Math.hypot(dx, dy) > 7) { st.go = dy > 0 && dy > Math.abs(dx) * 1.2; if (!st.go) { st = null; return; } } if (!st.go) return; e.preventDefault(); st.dy = dy; give(Math.max(0, Math.min(1, dy / 140)), 0.04); }, { passive: false });
  const pmEnd = (e) => { if (!st) return; const q_ = st; st = null; if (!q_.go) return; swiped = Date.now(); unGive(); if (e.type === 'touchend' && (q_.dy > 70 || (q_.dy > 24 && q_.dy / (e.timeStamp - q_.t) > 0.5))) setNav('glance'); }; pm.addEventListener('touchend', pmEnd); pm.addEventListener('touchcancel', pmEnd);
}
/* the weeks inside a file: a mouse over them, or a finger along them, reads that week */
{ const readAt = (c, x) => { const b = c.getBoundingClientRect(), n = Number(c.dataset.n); wkRead(S.file, Math.max(0, Math.min(n - 1, Math.floor(((x - b.left) / b.width) * n)))); }; let on = null;
  tail.addEventListener('pointermove', (e) => { const c = e.target.closest && mine(e.target, '.wkc'); if (c && e.pointerType === 'mouse') readAt(c, e.clientX); });
  tail.addEventListener('touchstart', (e) => { on = mine(e.target, '.wkc'); if (on) readAt(on, e.touches[0].clientX); }, { passive: true }); tail.addEventListener('touchmove', (e) => { if (on) readAt(on, e.touches[0].clientX); }, { passive: true }); tail.addEventListener('touchend', () => { on = null; }, { passive: true }); }
/* search: what it finds opens the same view a file does */
const q = $('#q'), hits = $('#hits'), hitR = $('.hits__r', hits), hl = $('.hl', hits), ask = $('#ask'); const roll0 = $('#roll'); let rowsN = 5, pick = 0, found = [], grown = 0, roller = { erase() {}, again() {} }, zT = 0, litT = 0;
/* what opens under the line slides out once, and its foot uncovers the list on the way: a window over a still list, the same clock as the foot */
const GROW = 560, LEAD_G = () => (phoneMQ.matches ? 60 : 0), growKf = (a, b, t = (v) => `translateY(${v}px)`) => { const L0 = LEAD_G() / (GROW + LEAD_G()); return [{ transform: t(a) }, { transform: t(a), offset: L0, easing: EASE }, { transform: t(b), offset: L0 + (1 - L0) * 0.84 }, { transform: t(b) }]; };
let growA = [];
function grow(g, from = grown) {
  const face = $('.ask__b', ask), mid = $('.ask__m', ask), lt = phoneMQ.matches ? 92 : 126, full = Math.max(g, from), vis = (v) => (v ? ask.offsetHeight + v - lt - 10 : 0), tot = vis(full), o = { duration: GROW + LEAD_G(), fill: 'both' }; growA.forEach((a) => a.cancel()); growA = []; grown = g;
  if (full) ask.style.setProperty('--g', `${full}px`); face.style.transform = `translateY(${g}px)`; mid.style.transform = `scaleY(${g ? (g + 20) / 100 : 0})`; if (still) return;
  growA = [face.animate(growKf(from, g), o), mid.animate(growKf(from, g, (v) => `scaleY(${v ? (v + 20) / 100 : 0.0001})`), o), hits.animate(growKf(vis(from) - tot, vis(g) - tot), o), $('.hits__in', hits).animate(growKf(tot - vis(from), tot - vis(g)), o)]; if (g && g < full) growA[0].onfinish = () => { if (grown === g) { growA.forEach((a) => a.cancel()); growA = []; ask.style.setProperty('--g', `${g}px`); } };
}
function closeHits() { if (!ask.classList.contains('is-typing')) return; grow(0); ask.classList.remove('is-typing', 'is-clear', 'is-lit', 'no-q', 'is-rolling'); [roll0, $('.ask__ph b', ask)].forEach((e) => e.getAnimations().forEach((a) => a.cancel())); head.classList.remove('is-typing'); clearTimeout(litT); clearTimeout(zT); zT = setTimeout(() => { head.classList.remove('is-finding'); if (!ask.classList.contains('is-typing')) { ask.classList.remove('has-q'); slots.forEach((q_) => { q_.el.remove(); }); slots.length = 0; } }, GROW + LEAD_G() + 40); q.value = ''; if (q.matches(':focus')) q.blur(); roller.again(); }
let asking = false;
function startAsk() {
  if (asking || ask.classList.contains('is-typing') || moving) return; if (still || !phoneMQ.matches || !('ontouchstart' in window)) return beginAsk();
  asking = true; const t0 = performance.now(); let last = t0, n = 0, held_ = false; const tick = () => { const t = performance.now(), gap = t - last; last = t; n++; if (n > 1 && gap > 40) held_ = true; else if ((held_ && gap < 40) || (!held_ && t - t0 > 66) || t - t0 > 420) { asking = false; if (document.activeElement === q || t - t0 > 420) beginAsk(); return; } requestAnimationFrame(tick); }; requestAnimationFrame(tick);
}
function beginAsk() { if (ask.classList.contains('is-typing') || moving) return; clearTimeout(zT); head.classList.add('is-finding', 'is-typing'); ask.classList.add('is-typing'); ask.classList.add('has-q'); roller.erase(() => ask.classList.add('is-clear')); litT = setTimeout(() => ask.classList.add('is-lit'), 620); if (scrollY > 8) scrollTo({ top: 0, behavior: still ? 'auto' : 'smooth' }); sizeHits(); drawHits(true); }
/* as many rows as stand above the keyboard. On a phone it has not come up yet when the box opens, so its room is taken as read; if it turns out taller, the last row sinks out and the foot rises to meet it */
let kbSeen = 0; try { kbSeen = Number(localStorage.getItem('one-kb')) || 0; } catch {} const roomNow = () => (phoneMQ.matches && 'ontouchstart' in window ? Math.min(seenTo(), kbSeen && kbSeen < innerHeight ? kbSeen : innerHeight * 0.5) : seenTo());
const rowsFor = (bottom) => { const lt = phoneMQ.matches ? 92 : 126, top = Math.max(0, phoneMQ.matches ? ask.offsetTop + 6 : ask.getBoundingClientRect().top); return Math.max(2, Math.min(5, Math.floor((bottom - top - lt - 48) / 60))); }, heightFor = (n) => Math.max(1, (phoneMQ.matches ? 92 : 126) + 30 + 60 * n + 12 - ask.offsetHeight), seenTo = () => { const vv = window.visualViewport; return vv ? vv.offsetTop + vv.height : innerHeight; };
function sizeHits() { rowsN = rowsFor(roomNow()); grow(heightFor(rowsN), 0); }
if (window.visualViewport) { let vt = 0; on(visualViewport, 'resize', () => { clearTimeout(vt); vt = setTimeout(() => { if (!ask.classList.contains('is-typing')) return; const h = seenTo(), n = rowsFor(h); if (h < innerHeight - 120) { kbSeen = h; try { localStorage.setItem('one-kb', String(Math.round(h))); } catch {} } if (n < rowsN) { rowsN = n; grow(heightFor(n)); drawHits(); } }, 180); }); }
const markOf = (name, s) => { const k = name.toLowerCase().indexOf(s); return k < 0 ? esc(name) : `${esc(name.slice(0, k))}<mark>${esc(name.slice(k, k + s.length))}</mark>${esc(name.slice(k + s.length))}`; };
/* each row is a slot with a fixed place. What is in it changes by rolling: the old one out of the top as the new one comes in from under, one slot after another. A row that stays the same does not move */
const slots = [];
function fillSlot(q_, x, i, first) {
  const was = q_.in, e = document.createElement('button'), c = (byDay(x.f) ? ALL : x.f.posts)[0]; e.type = 'button'; e.className = `hit${x.ask ? ' is-q' : ''}`; e.dataset.file = x.f.id; e.dataset.i = i; e.innerHTML = `<img src="${c ? P[c].cover : ''}" alt="" decoding="sync"><span><b>${x.t}</b><small class="num">${x.sub}</small></span>${!x.ask && x.f.followed ? '<i>Following</i>' : ''}`; q_.el.append(e); q_.in = e; q_.k = x.k;
  if (still) { if (was) was.remove(); return; } const d = i * 34, o = { duration: 520, delay: d, easing: 'linear', fill: 'both' }, kf = (a, b) => [{ transform: `translateY(${a}px)`, easing: EASE }, { transform: `translateY(${b}px)`, offset: 0.84 }, { transform: `translateY(${b}px)` }];
  if (first) { e.animate(kf(40, 0), { ...o, delay: LEAD_G() + 50 + i * 46, duration: 680 }).onfinish = (ev) => ev.target.cancel(); return; }
  const A = e.animate(kf(60, 0), o); A.onfinish = () => A.cancel(); if (was) { was.style.pointerEvents = 'none'; was.animate(kf(new DOMMatrix(getComputedStyle(was).transform).f, -60), o).onfinish = () => was.remove(); }
}
function drawHits(first) {
  if (!ask.classList.contains('is-typing')) return startAsk(); const s = q.value.trim().toLowerCase(); if (s) ask.classList.add('is-clear');
  const rank = (f) => { const n = f.name.toLowerCase(); return n.startsWith(s) ? 0 : n.includes(` ${s}`) ? 1 : n.includes(s) ? 2 : f.title.toLowerCase().includes(s) ? 3 : 9; }, asked = asks.map((a) => ({ k: `q:${a.q}`, f: a.f, t: esc(a.q), sub: '', ask: 1 }));
  const rows = !s ? asked : everyFile().map((f) => [rank(f), f]).filter(([k]) => k < 9).sort((a, b) => a[0] - b[0] || b[1].posts.length - a[1].posts.length).map(([, f]) => ({ k: `f:${f.id}`, f, t: markOf(f.name, s), sub: `${esc(f.group || f.kind)} · ${byDay(f) ? ALL.length : f.posts.length} posts` })), hitN = Math.min(rows.length, rowsN), list = rows.slice(0, rowsN); if (s) asked.forEach((a) => { if (list.length < rowsN && !list.some((x) => x.f === a.f)) list.push(a); }); found = list.map((x) => x.f); pick = 0;
  ask.classList.toggle('no-q', !s); const say = !s ? 'Raised this week' : hitN ? 'Already read, free to open' : `Nothing tagged matches “${q.value.trim()}”`, lab = $('.hits__k', hits); if (lab.textContent !== say) lab.textContent = say; hl.classList.toggle('no', !hitN || !s); hl.style.transform = '';
  list.forEach((x, i) => {
    let q_ = slots[i]; if (!q_) { q_ = slots[i] = { el: document.createElement('div'), k: null, in: null }; q_.el.className = 'slot'; q_.el.style.top = `${i * 60}px`; hitR.append(q_.el); }
    if (q_.k === x.k) { const t0 = $('b', q_.in); if (t0.innerHTML !== x.t) t0.innerHTML = x.t; return; } fillSlot(q_, x, i, first === true);
  });
  slots.slice(list.length).forEach((q_, n) => { const e = q_.in; q_.in = null; q_.k = null; if (!e) return; if (still) return e.remove(); e.style.pointerEvents = 'none'; e.animate([{ transform: 'translateY(0)' }, { transform: 'translateY(-60px)' }], { duration: 320, delay: n * 34, easing: EASE, fill: 'both' }).onfinish = () => e.remove(); });
}
const at_ = (i) => { pick = i; hl.style.transform = `translateY(${i * 60}px)`; };
q.addEventListener('focus', startAsk); q.addEventListener('input', drawHits);
ask.addEventListener('click', (e) => { if (mine(e.target, 'button') || e.target === q) return; startAsk(); q.focus({ preventScroll: true }); });
hits.addEventListener('pointerdown', (e) => e.preventDefault()); $('.disc', ask).addEventListener('pointerdown', (e) => { if (ask.classList.contains('is-typing')) e.preventDefault(); });
hitR.addEventListener('pointerover', (e) => { if (e.pointerType !== 'mouse') return; const b = mine(e.target, 'button'); if (b) at_(Number(b.dataset.i)); });
$('.find form').addEventListener('submit', (e) => { e.preventDefault(); if (!ask.classList.contains('is-typing')) return void (asks[askI] && open(asks[askI].f)); if (found[pick] && q.value.trim() && !hl.classList.contains('no')) open(found[pick]); });
q.addEventListener('blur', () => setTimeout(() => { if (!q.matches(':focus')) closeHits(); }, 120));
q.addEventListener('keydown', (e) => { if (e.key === 'Escape') { e.stopPropagation(); if (q.value) { q.value = ''; drawHits(); } else closeHits(); return; } if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp') return; e.preventDefault(); if (found.length) at_((pick + (e.key === 'ArrowDown' ? 1 : found.length - 1)) % found.length); });

/* the questions the read raises this week type themselves, one at a time; the disc opens the one on show */
const asks = (drop.leads || []).map((l) => { const [a, b, c] = l.to.split(':'); const f = a === 'file' ? FILES.find((x) => x.id === b) : (() => { const e = (D.everything[b] || [])[Number(c)]; return e && everyFile().find((x) => x.id === `e-${e.id}` || x.id === e.id); })(); return f && { q: l.line, sub: l.sub, f }; }).filter(Boolean);
const onSeen = [], glim = { hold() {}, go() {} }; let seenNow = true; const look = () => { const v = !S.file && scrollY < (phoneMQ.matches ? 290 : 214); if (v !== seenNow) { seenNow = v; onSeen.forEach((f) => f(v)); } };
on(window, 'scroll', look, { passive: true });
let askI = 0; { const roll = $('#roll'), sub = $('#askSub'), [ty, , re] = roll.children; let n = 0, way = 1, seen = true, hold = 0; const put = (t, k) => { ty.textContent = t.slice(0, k); re.textContent = t.slice(k); roll.classList.toggle('whole', k === t.length); roll.classList.toggle('long', t.length > 34); }; const free = () => seen && !S.file && !moving && !ask.classList.contains('is-typing') && !document.hidden;
  onSeen.push((v) => { seen = v; });
  roller.erase = (then) => { n = 0; way = 1; then(); if (still) return; const L0 = LEAD_G(), o = { duration: 420 + L0 + 60, fill: 'both' }, f0 = L0 / o.duration, f1 = (L0 + 420) / o.duration, kf = (a, b) => [{ transform: a }, { transform: a, offset: f0, easing: EASE }, { transform: b, offset: f1 }, { transform: b }]; roll.classList.remove('whole'); const A = roll.animate(kf('translateY(0)', 'translateY(-110%)'), o), B = $('.ask__ph b', ask).animate(kf('translateY(110%)', 'translateY(0)'), o); ask.classList.add('is-rolling'); A.onfinish = () => { ask.classList.remove('is-rolling'); A.cancel(); B.cancel(); }; };
  roller.again = () => { n = 0; way = 1; put(asks[askI] ? asks[askI].q : '', 0); };
  const step = () => { if (!free()) return void setTimeout(step, 400); const a = asks[askI]; if (!a) return; sub.textContent = a.sub; if (still) { put(a.q, a.q.length); return; } n += way * (way < 0 ? 2 : 1); n = Math.max(0, Math.min(a.q.length, n)); put(a.q, n); if (way > 0 && n === a.q.length) { way = -1; return void setTimeout(step, 3400); } if (way < 0 && n === 0) { way = 1; askI = (askI + 1) % asks.length; return void setTimeout(step, 260); } setTimeout(step, way > 0 ? 38 + (/[\s,.?!:]/.test(a.q[n - 1] || '') ? 46 : 0) : 18); };
  if (asks.length) { put(asks[0].q, asks[0].q.length); sub.textContent = asks[0].sub; n = asks[0].q.length; way = -1; setTimeout(step, 4200); } }


/* ── the streak, as it is in the app ── */
{
const LAP = 8.8; // seconds round the box at the mean pace
const SHORT = 0.055; // the breath's two ends, as shares of the outline
const LONG = 0.34;
const CREEP = 0.2; // the tail never moves slower than this share of the mean pace (so it never runs backwards)
const CATCH = 0.42; // seconds: how quickly the tail closes on the length the breath wants
const BLEED = 14; // px the canvas reaches past the box on every side: room for the bloom
const SAMPLES = 24; // gradient stops along the streak (the browser blends between them)
const SPREAD_IN = 0.52; // seconds for the streak to run round the whole edge
const GATHER = 1.05; // seconds for the ring to gather back to a streak
const HAND_OVER = 560; // ms the canvas keeps its last frame while it hands over to the glow (it fades in CSS)
const SETTLE = 160; // ms the box must hold its size before the light comes back to it
const FLAT = 0.34; // the ring's strength once spread (close to the glow it hands over to)
const PEAK = 0.7; // where along the streak it is brightest (0 tail, 1 front)

function noise(seed) {
  const N = 512;
  const knots = new Float32Array(N);
  let s = seed >>> 0 || 0x9e3779b9;
  for (let i = 0; i < N; i += 1) {
    s = (s ^ (s << 13)) >>> 0;
    s = (s ^ (s >>> 17)) >>> 0;
    s = (s ^ (s << 5)) >>> 0;
    knots[i] = (s / 4294967296) * 2 - 1;
  }
  const at = (x) => {
    const i = Math.floor(x);
    const f = x - i;
    const u = f * f * f * (f * (f * 6 - 15) + 10);
    const a = knots[((i % N) + N) % N];
    const b = knots[(((i + 1) % N) + N) % N];
    return a + (b - a) * u;
  };
  return (x) => at(x) * 0.7 + at(x * 2.37 + 101.3) * 0.3;
}

const clamp01 = (x) => (x < 0 ? 0 : x > 1 ? 1 : x);
const soft = (x) => x * x * (3 - 2 * x);
const mod = (x, m) => ((x % m) + m) % m;

/* the streak along its length, u from its tail (0) to its front (1) */

// how bright: a long soft rise to the lit part near the front, a short soft fall in front of it
const alphaAt = (u) => (u <= 0 || u >= 1 ? 0 : u < PEAK ? Math.pow(soft(u / PEAK), 1.35) : Math.pow(soft((1 - u) / (1 - PEAK)), 0.8));
// which colour: oxblood, deep crimson, the crimson, the pale lit tip, the crimson again (indices into the palette)
const TINT = [
  [0, 0],
  [0.32, 1],
  [0.52, 2],
  [0.7, 3],
  [0.82, 2],
  [1, 2],
];
function colourAt(u, pal) {
  const v = clamp01(u);
  for (let i = 1; i < TINT.length; i += 1) {
    if (v <= TINT[i][0]) {
      const k = (v - TINT[i - 1][0]) / (TINT[i][0] - TINT[i - 1][0]);
      const a = pal[TINT[i - 1][1]];
      const b = pal[TINT[i][1]];
      return [a[0] + (b[0] - a[0]) * k, a[1] + (b[1] - a[1]) * k, a[2] + (b[2] - a[2]) * k];
    }
  }
  return pal[2];
}
const parse = (s, fallback) => {
  const t = s.trim();
  const hex = /^#([0-9a-f]{6})$/i.exec(t);
  if (hex) return [parseInt(hex[1].slice(0, 2), 16), parseInt(hex[1].slice(2, 4), 16), parseInt(hex[1].slice(4, 6), 16)];
  const n = t.match(/[\d.]+/g);
  return n && n.length >= 3 ? [Number(n[0]), Number(n[1]), Number(n[2])] : fallback;
};

/* a rounded rectangle's outline as pieces, clockwise from the top left: a side is a line, a corner a quarter turn.
   a is where a piece starts along the outline */

function outline(x, y, w, h, radius) {
  const r = Math.max(0, Math.min(radius, w / 2, h / 2));
  const q = (Math.PI * r) / 2;
  const L = x + r;
  const R = x + w - r;
  const T = y + r;
  const B = y + h - r;
  const pieces = [];
  let a = 0;
  const line = (x0, y0, x1, y1) => {
    const len = Math.hypot(x1 - x0, y1 - y0);
    if (len > 0.01) pieces.push({ a, len, x0, y0, x1, y1, arc: null });
    a += len;
  };
  const turn = (cx, cy, from) => {
    if (q > 0.01) pieces.push({ a, len: q, x0: cx + r * Math.cos(from), y0: cy + r * Math.sin(from), x1: cx + r * Math.cos(from + Math.PI / 2), y1: cy + r * Math.sin(from + Math.PI / 2), arc: { cx, cy, r, from } });
    a += q;
  };
  line(L, y, R, y);
  turn(R, T, -Math.PI / 2);
  line(x + w, T, x + w, B);
  turn(R, B, 0);
  line(R, y + h, L, y + h);
  turn(L, B, Math.PI / 2);
  line(x, B, x, T);
  turn(L, T, Math.PI);
  return { pieces, P: a };
}


  const host = ask, canvas = $('canvas', ask), ctx = canvas.getContext('2d'), light = $('.ask__cast i'), inset = 0;
  const pace = noise(Math.random() * 4294967296), breath = noise(Math.random() * 4294967296);
  let radius = 30, core = 2.25, pieces = [], P = 0, W = 0, H = 0, bufW = 0, bufH = 0, dpr = 0, head = 0, tail = 0, t = Math.random() * 400, glow = 1, last = 0, raf = 0, seen = true, spread = 0, dirty = false;
  const pal = [[121, 27, 38], [171, 3, 44], [218, 46, 70], [251, 161, 162]];
    const fit = () => {
      dirty = false;
      const w = host.clientWidth ? host.clientWidth + 2 * BLEED : 0;
      const h = host.clientHeight + 2 * BLEED;
      if (!w || !h) return;
      const d = Math.min(3, window.devicePixelRatio || 1);
      if (d !== dpr || w > bufW || h > bufH || w * h < bufW * bufH * 0.45) {
        const grow = bufH > 0 && h > bufH;
        dpr = d;
        bufW = Math.ceil(w);
        bufH = Math.ceil(grow ? h * 1.25 : h);
        canvas.width = Math.round(bufW * dpr);
        canvas.height = Math.round(bufH * dpr);
        canvas.style.width = `${bufW}px`;
        canvas.style.height = `${bufH}px`;
      }
      W = w;
      H = h;
      const was = P;
      const at = BLEED + inset;
      const o = outline(at, at, w - 2 * at, h - 2 * at, radius - inset);
      pieces = o.pieces;
      P = o.P;
      if (was) {
        head *= P / was;
        tail *= P / was;
      } else {
        head = Math.random() * P;
        tail = head - P * 0.12;
      }
    };

    const move = (dt) => {
      const mean = P / LAP;
      t += dt;
      head += mean * Math.min(1.6, Math.max(0.45, 1 + 0.62 * pace(t / 2.6))) * dt;
      const b = soft(clamp01(0.5 + 0.95 * breath(t / 3.4 + 50)));
      const want = head - P * (SHORT + (LONG - SHORT) * b);
      let next = tail + (want - tail) * (1 - Math.exp(-dt / CATCH));
      next = Math.max(next, tail + mean * CREEP * dt);
      next = Math.min(next, head - P * 0.03);
      tail = Math.max(next, head - P * 0.5);
      const f = (head - tail) / P;
      glow = 0.84 + 0.16 * (1 - clamp01((f - SHORT) / (LONG - SHORT)));
      if (tail > P * 2) {
        head -= P;
        tail -= P;
      }
    };

    const draw = () => {
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, bufW, bufH);
      if (!P) return;
      const e = soft(spread);
      const L0 = Math.max(P * 0.03, head - tail);
      const extra = (P - L0) * e;
      const len = Math.min(P, L0 + extra);
      const from = mod(tail - extra / 2, P);
      const strength = glow + (1 - glow) * e;
      const accent = pal[2];
      // (the ends sharpen as the ring closes)
      const taper = (1 - e) * 0.16;

      /* the light the streak casts under the box follows its bright part: where it is along the outline, and how much
         it lies along the foot (strongest there, none along the top). Written straight onto the light: transform and
         opacity only */
      if (light) {
        const at = mod(tail + (head - tail) * PEAK, P);
        let cx = W / 2;
        let cy = 0;
        for (let k = 0; k < pieces.length; k += 1) {
          const pc = pieces[k];
          if (at < pc.a || at > pc.a + pc.len) continue;
          const f = (at - pc.a) / pc.len;
          if (pc.arc) {
            const ang = pc.arc.from + (f * Math.PI) / 2;
            cx = pc.arc.cx + pc.arc.r * Math.cos(ang);
            cy = pc.arc.cy + pc.arc.r * Math.sin(ang);
          } else {
            cx = pc.x0 + (pc.x1 - pc.x0) * f;
            cy = pc.y0 + (pc.y1 - pc.y0) * f;
          }
          break;
        }
        const bw = W - 2 * BLEED;
        const bh = H - 2 * BLEED;
        const foot = bh ? (cy - BLEED) / bh : 0;
        const w = Math.round(Math.min(bw * 0.4, 120 + L0 * 0.45));
        const x = Math.max(w / 2 + 28, Math.min(bw - w / 2 - 28, cx - BLEED));
        const near = Math.max(0, (foot - 0.6) / 0.4);
        light.style.transform = `translate3d(${(x - w / 2).toFixed(1)}px, 0, 0) scaleX(${(w / 240).toFixed(3)})`;
        light.style.opacity = (0.45 * near * near * (1 - e)).toFixed(3);
      }

      for (let k = 0; k < pieces.length; k += 1) {
        const pc = pieces[k];
        if (len < P - 0.5) {
          let hit = false;
          for (let lap = -1; lap <= 1 && !hit; lap += 1) {
            const s0 = from + lap * P;
            if (s0 < pc.a + pc.len && s0 + len > pc.a) hit = true;
          }
          if (!hit) continue;
        }
        // the stops: the piece's two ends, and evenly along the streak wherever it lies on this piece (any lap)
        const pts = [pc.a, pc.a + pc.len];
        for (let j = 0; j <= SAMPLES; j += 1) {
          const p = from + (j / SAMPLES) * len;
          for (let lap = -1; lap <= 1; lap += 1) {
            const q = p + lap * P;
            if (q > pc.a && q < pc.a + pc.len) pts.push(q);
          }
        }
        pts.sort((x, y) => x - y);
        const g = ctx.createLinearGradient(pc.x0, pc.y0, pc.x1, pc.y1);
        for (let j = 0; j < pts.length; j += 1) {
          const q = pts[j];
          const s = (q - pc.a) / pc.len;
          const d = mod(q - from, P);
          const u = d / len;
          let a = 0;
          let c = accent;
          if (u <= 1) {
            const ends = taper > 0 ? Math.min(1, u / taper, (1 - u) / taper) : 1;
            a = alphaAt(u) * (1 - e) + FLAT * ends * e;
            const tint = colourAt(u, pal);
            c = [tint[0] + (accent[0] - tint[0]) * e, tint[1] + (accent[1] - tint[1]) * e, tint[2] + (accent[2] - tint[2]) * e];
          }
          // (a corner's paint runs along its chord: where a point of the turn falls on it)
          const off = pc.arc ? (1 - Math.cos((s * Math.PI) / 2) + Math.sin((s * Math.PI) / 2)) / 2 : s;
          g.addColorStop(clamp01(off), `rgba(${Math.round(c[0])},${Math.round(c[1])},${Math.round(c[2])},${a.toFixed(3)})`);
        }
        ctx.beginPath();
        if (pc.arc) ctx.arc(pc.arc.cx, pc.arc.cy, pc.arc.r, pc.arc.from, pc.arc.from + Math.PI / 2);
        else {
          ctx.moveTo(pc.x0, pc.y0);
          ctx.lineTo(pc.x1, pc.y1);
        }
        ctx.strokeStyle = g;
        // a wide faint bloom, a nearer one, then the bright core on the edge itself
        ctx.globalAlpha = 0.09 * strength;
        ctx.lineWidth = 14;
        ctx.stroke();
        ctx.globalAlpha = 0.24 * strength;
        ctx.lineWidth = 6;
        ctx.stroke();
        ctx.globalAlpha = strength;
        ctx.lineWidth = core;
        ctx.stroke();
      }
      ctx.globalAlpha = 1;
    };


  const frame = (now) => {
    raf = 0; const real = Math.min(0.25, Math.max(0, (now - last) / 1000)), dt = Math.min(0.064, real); last = now; if (!seen) return;
    const focus = ask.classList.contains('is-typing');
    if (focus) spread = Math.min(1, spread + real / SPREAD_IN); else if (spread > 0) spread = Math.max(0, spread - real / GATHER);
    move(dt); draw(); raf = requestAnimationFrame(frame);
  };
  glim.hold = () => { cancelAnimationFrame(raf); raf = 0; }; glim.go = (now) => { if (!still) run(now); };
  const run = (now) => { if (raf || !seen || (moving && now !== true)) return; last = performance.now(); raf = requestAnimationFrame(frame); };
  const size0 = () => { radius = parseFloat(getComputedStyle(ask).getPropertyValue('--r')) || 30; core = radius < 30 ? 2 : 2.25; fit(); };
  size0(); if (still) draw(); else run();
  new ResizeObserver(() => { if (ask.offsetWidth) { size0(); if (!raf) draw(); } }).observe(ask);
  onSeen.push((v) => { seen = v; if (!seen) { cancelAnimationFrame(raf); raf = 0; } else if (!still) run(); });
}

let sr = 0; on(window, 'scroll', () => { if (moving || S.file || sr) return; sr = requestAnimationFrame(() => { sr = 0; if (!moving && !S.file) settleWindow(); }); }, { passive: true });
let lw = innerWidth, lh = innerHeight; on(window, 'resize', () => { setNav(navAt, true); if (phoneMQ.matches && innerWidth === lw) return; lw = innerWidth; lh = innerHeight; IH = innerHeight; relay(); });
const sides = () => page.style.setProperty('--side', `${sideW()}px`); sides();
function relay() { dress(); sides(); anims.forEach((a) => a.cancel()); anims = []; moving = false; sweep(); drawStrips(); if (S.file) setWin(S.win, true); L = layout(); field.style.height = `${L.H}px`; origin = field.getBoundingClientRect().top + scrollY; tiles.forEach((t) => { t.r = null; t.bw = 0; }); furniture(); if (gnow) { gnow._pl = L.fig; gnow.classList.add('done'); } drawBits(); if (L.box) more.textContent = plus(L.box.n); settleWindow(); }
relay(); setNav('dock', true); rowsN = rowsFor(roomNow()); ask.style.setProperty('--g', `${heightFor(rowsN)}px`);
// (every cover is asked for while the page is idle, so a post that has not been seen yet is ready when it is called)
(window.requestIdleCallback || ((f) => setTimeout(f, 400)))(() => [...new Set(ALL.map((id) => P[id].cover))].forEach((src) => { if (AR.has(src)) return; const im = new Image(); im.onload = () => { if (im.naturalWidth) AR.set(src, im.naturalWidth / im.naturalHeight); }; im.src = src; }));
// (the hundred the shelf shows are made ahead, a few at a time while nothing moves, so a tap has nothing to build)
{ let i = 0; const make = () => { if (moving) return void setTimeout(make, 400); const end = Math.min(i + 6, ALL.length, 130); for (; i < end; i++) { const id = ALL[i], t = tileOf(id); if (!t.r) { const r = L.at.get(id); if (r) { stand(t, r, L.base); } } if (t.img.decode) t.img.decode().catch(() => {}); } if (i < Math.min(ALL.length, 130)) setTimeout(make, 60); }; setTimeout(make, 500); }
{ const wait = { n: 0 }; $$('.strip').forEach((st) => st.addEventListener('scroll', () => { if (wait.n) return; wait.n = requestAnimationFrame(() => { wait.n = 0; drawCovers(st); }); }, { passive: true })); }
function arrive() {
  drawCovers($('.strip:not(.off)')); if (still) return; const on = [...tiles.values()].filter((t) => t.on && near(t.r, 0)); if (!on.length) return; moving = true; const my = ++moveNo;
  on.sort((a, b) => a.r.y - b.r.y || a.r.x - b.r.x).forEach((t, i) => { const hold = Math.min(0.3, (i / on.length) * 0.3), to = t.el.style.transform, fr = tf(t.r.x, t.r.y + IH * 0.7 + t.r.y * 0.25, t.r.w / t.bw, t.r.h / t.bh); anim(t.el, [{ transform: fr, easing: 'linear' }, { transform: fr, offset: hold, easing: 'cubic-bezier(.2,.7,.15,1)' }, { transform: to }], 'linear'); });
  const mine = anims; anims[0].onfinish = () => { if (my === moveNo) { moving = false; mine.forEach((a) => a.cancel()); anims = []; } };
}
if (document.fonts && document.fonts.status !== 'loaded') document.fonts.ready.then(() => { if (!dead && !moving) relay(); });
// the tab put back as it was left: the file, its time frame, its order and number, and where the page stood
const kept = keep.was;
if (kept) {
  still = true;
  if (kept.tab && kept.tab !== tab) setTab(kept.tab);
  $$('.strip').forEach((e, n) => { e.scrollLeft = kept.strips[n] || 0; });
  savedY = kept.savedY;
  const f = kept.file && everyFile().find((x) => x.id === kept.file);
  if (f) { open(f); savedY = kept.savedY; if (kept.win !== S.win) frame(kept.win); if (kept.sort !== S.sort) sortBy(kept.sort); if (kept.met !== S.met) setMet(kept.met); const e = $('.fol', pFile); if (e) e.classList.remove('yet', 'born'); }
  still = REDUCED; scrollTo(0, kept.y); settleWindow(); look();
} else if (!quiet) arrive();
chrome();
if (new URLSearchParams(location.search).get('one') || window.__one) window.__one = { setMet, rankOf, trailNow, P, openMortem, stepReplay, setNav, get picked() { return picked; }, get navAt() { return navAt; }, get folded() { return Boolean(dockEl()?.hasAttribute('data-folded')); }, CLOCK, setTab, pickPost, unpick, weekLine, S, FILES, FOUND, open, close, frame, sortBy, tiles, get L() { return L; }, get anims() { return anims; }, get moving() { return moving; } };
// (where the page stands, as it is scrolled: by the time the tab is put away the page under it has already changed)
let stands = scrollY; on(window, 'scroll', () => { stands = scrollY; }, { passive: true });
const keepIt = () => { keep.was = { file: S.file && S.file.id, win: S.win, sort: S.sort, met: S.met, tab, savedY: S.file ? savedY : 0, y: stands, strips: $$('.strip').map((e) => e.scrollLeft) }; };
return {
  frame: (d) => { if (!moving) frame(d); },
  back: () => { if (S.file && !moving) close(); },
  unmount() {
    keepIt(); dead = true; gone.abort(); timers.forEach((t) => window.clearTimeout(t)); frames.forEach((t) => window.cancelAnimationFrame(t)); stopReplay(); anims.forEach((a) => a.cancel());
    root.replaceChildren(); layer.replaceChildren();
  },
};
}
