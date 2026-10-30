/* ─────────────────────────────────────────────────────────────
   READ 17 — a feeder's read in three parts, mounted by Read17 into the Read tab:
   - Story: the week as cards, shown the way Instagram shows stories (a laptop up: the card on show in the middle, the
     ones either side smaller), opening on what changed in views, likes, comments and follows since the last read; a
     tap reads a card, a swipe moves on, and its words settle a beat after it lands.
   - Strategy: the current 100 in three acts. Slices, each with its four numbers; Views, Likes, Comments and Follows,
     each the things that move it; Streaks, Shifts and Frequency, the 100 over time. Every part heads with its own
     course, week by week, and every card is marked New, Shifted or Held against the last read.
   - Split: the wall, newest first or in rank groups, under a live lane of the posts under a week old and their day-3
     calls; a pick lights the run of posts it beat.
   Depth opens in one panel (a sheet on a phone, a panel at the side on a laptop): a fact's posts, or a post's mortem
   (its beats, what it was going for, how it landed and why, against its closest posts). On scroll, reels glide in and
   heads rise as they arrive, on the browser's own scroll timelines (readParts.css), never hiding anything.

   Plain JS, ported from the v17 prototype as readerEngine was from its own: it draws the page into `root` and its panel
   and readout into `layer` (at body level, so position: fixed is never trapped by the tab's container), finds its
   elements inside those two only (the tabs kept alive beside it have DOM of their own), and on unmount takes back every
   listener, observer and timer it set. Every number on the page is counted from `data` (public/read17/<acct>.json);
   the breakdowns' words come from a model, checked against those counts when they were written.
   ───────────────────────────────────────────────────────────── */

const MON = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const K = (n) => { const a = Math.abs(n); const s = a >= 1e6 ? (a / 1e6).toFixed(2).replace(/\.?0+$/, '') + 'M' : a >= 1e4 ? (a / 1e3).toFixed(1).replace(/\.0$/, '') + 'K' : Math.round(a).toLocaleString('en-US'); return (n < 0 ? '−' : '') + s; };
const sg = (n) => (n > 0 ? '+' : '') + K(n);
const r1 = (v) => Math.round(v * 2) / 2;
const top = (v) => v == null ? '–' : `Top ${r1(+v)}%`;
const shade = (v) => v == null ? 0 : v <= 3 ? 6 : v <= 10 ? 5 : v <= 25 ? 4 : v <= 50 ? 3 : v <= 70 ? 2 : v <= 85 ? 1 : 0;
const sv = (v) => `var(--s${shade(v)})`;
const wt = (v) => shade(v) >= 5 ? ' w' : '';
const day = (iso) => { const d = new Date(iso + 'T12:00:00'); return `${d.getDate()} ${MON[d.getMonth()]}`; };
const ARROW = '<svg viewBox="0 0 24 24"><path d="M12 5v14M6 13l6 6 6-6"/></svg>';
const SV = (d) => `<svg viewBox="0 0 24 24"><path d="${d}"/></svg>`;
// the covers (small, inline), fetched once a feeder and kept across mounts
const coverCache = {};
// the parts the header names: the story, Strategy's eight parts, the wall
const PARTS = ['story', 'slices', 'views', 'likes', 'comments', 'follows', 'streaks', 'shifts', 'frequency', 'split'];

export function mountRead17({ root, layer, data, acct, base, onChapter, fold }) {
const BASE = base;
let A = data, covers = coverCache[acct] || {}, dead = false, raf = 0, rt = 0;
const offs = [];
const on = (el_, type, fn, opt) => { el_.addEventListener(type, fn, opt); offs.push(() => el_.removeEventListener(type, fn, opt)); };
const $ = (id) => root.querySelector(`#${id}`) || layer.querySelector(`#${id}`);
const qa = (sel) => root.querySelectorAll(sel);
const q1 = (sel) => root.querySelector(sel);
function localGet(k) { try { return localStorage.getItem('read17.' + k); } catch { return null; } }
function localSet(k, v) { try { localStorage.setItem('read17.' + k, v); } catch {} }
const cv = (id) => covers[id] ? `background-image:url(${covers[id]})` : '';
function paintCovers() { [root, layer].forEach((r) => r.querySelectorAll('[data-cv]').forEach((el_) => { const u = covers[el_.dataset.cv]; if (u && !el_.style.backgroundImage) el_.style.backgroundImage = `url(${u})`; })); }
function loadCovers() {
  if (coverCache[acct]) { covers = coverCache[acct]; paintCovers(); return; }
  fetch(`${BASE}c-${acct}.json`).then((r) => r.json()).then((j) => { coverCache[acct] = j; if (dead) return; covers = j; paintCovers(); }).catch(() => {});
}

const SAY = { anuj: 'A holiday week. Eleven posts, six from Japan, and his worst follower week on file.', lakme: 'Faces won. Its best week on likes since September, after the Fashion Week flood.' };

/* =====================================================================
   STORY: ten cards, a glimpse each
   ===================================================================== */
let at = 0;
const kickOf = (s) => `<span class="kick ${s.ch.toLowerCase()}${s._t ? ` t-${s._t}` : ''}"><span class="dot"></span>${esc((typeof KICK !== 'undefined' && KICK[s.ch]) || s.ch)}</span>`;
// a picture's label: the post's likes against the usual post (its rank only where it has no count yet)
function xBadge(id, v) {
  const m = id ? xPost(id, 'lk') : null;
  if (m != null) { const h = xshade(m); return `<b class="${h >= 5 ? 'w' : ''}" style="background:var(--s${h})">${fx(m)}</b>`; }
  return v != null ? `<b class="${wt(v).trim()}" style="background:${sv(v)}">${top(v)}</b>` : '';
}
function labelHTML(it) { return `<div class="plab">${xBadge(it.id, it.v)}<span>${esc(it.label)}</span></div>`; }
function mediaHTML(s, k) {
  const m = s.media;
  if (m.kind === 'grid') return quadHTML(m.ids.slice(0, 4).map((id) => ({ id, lab: labelHTML({ id, v: A.posts[id] && A.posts[id].top, label: (m.labels || {})[id] || '' }) })));
  // (one post by its cover, for posts with no story still)
  if (m.kind === 'post') { const it = m.items[0]; return `<div class="md bleed"><span class="qc" data-cv="${it.id}" style="${coverStyle(it.id)}"></span>${labelHTML(it)}</div>`; }
  if (m.kind === 'pair') {
    const fr = m.items.map((it) => (A.posts[it.id] && A.posts[it.id].rp) || []);
    if (m.items.length === 2 && fr.every((f) => f.length >= 2)) {
      const two = (f) => [f[0], f[Math.max(1, Math.round((f.length - 1) / 2))]], [a, b] = fr.map(two);
      return quadHTML([{ img: BASE + a[0].f, lab: labelHTML(m.items[0]) }, { img: BASE + b[0].f, lab: labelHTML(m.items[1]) }, { img: BASE + a[1].f }, { img: BASE + b[1].f }]);
    }
    const it = [...m.items].sort((x, y) => ((A.posts[y.id] || {}).d || '').localeCompare((A.posts[x.id] || {}).d || ''))[0];
    return `<div class="md bleed"><img src="${BASE}st/${it.id}.jpg" alt="" draggable="false" loading="lazy">${labelHTML(it)}</div>`;
  }
  if (m.kind === 'single') { const it = m.items[0]; return `<div class="md bleed"><img src="${BASE}st/${it.id}.jpg" alt="" draggable="false" loading="lazy">${labelHTML(it)}</div>`; }
  if (m.kind === 'weeks') return `<div class="md pad"><div class="wk16" data-wk16="${k}"></div></div>`;
  if (m.kind === 'formats') { const ids = byRank(s.rows.flatMap((r) => r.ids || [])).slice(0, 4); return ids.length === 4 ? quadHTML(ids.map((id) => ({ id, lab: labelHTML({ id, v: A.posts[id].top, label: `${cap1(FMT1[A.posts[id].fmt] || 'post')} · ${day(A.posts[id].d)}` }) }))) : `<div class="md pad">${formatThumbs(s)}</div>`; }
  if (m.kind === 'days') return `<div class="md pad">${daysViz(s)}</div>`;
  if (m.kind === 'fol') return `<div class="md pad">${folViz(s)}</div>`;
  if (m.kind === 'reviews' && s.reviews.length >= 4) return quadHTML(s.reviews.slice(0, 4).map((r) => ({ id: r.id, lab: labelHTML({ id: r.id, v: A.posts[r.id] && A.posts[r.id].top, label: r.t.split(' · ')[0] }) })));
  if (m.kind === 'reviews') return `<div class="md pad"><div class="rv4">${s.reviews.map((r) => `<div><div class="c" data-cv="${r.id}" style="${cv(r.id)}">${xBadge(r.id, A.posts[r.id]?.top) || '<b>new</b>'}</div><em>${esc(r.t)}</em></div>`).join('')}</div></div>`;
  if (m.kind === 'four') return `<div class="md pad"><div class="four">${m.rows.map((r) => `<div class="fr${r.k === m.lead ? ' on' : ''}${r.b != null && r.a != null ? (r.b > r.a ? ' up' : ' dn') : ''}"><span class="fk">${r.nm}</span><span class="fa">${fourSay(r, r.a)}</span><i>${ARW.up}</i><b class="fb">${fourSay(r, r.b)}</b></div>`).join('')}<p class="fl"><span>Last read</span><span>This read</span></p></div></div>`;
  if (m.kind === 'list') return `<div class="md"><div class="erows">${s.rows.map((r, j) => `<div class="erow"><span class="n">${j + 1}</span><span class="ek">${esc(r.k)}</span><span class="ev">${esc(r.v)}</span><span class="ew">${esc(r.why)}</span></div>`).join('')}</div></div>`;
  return '';
}
function formatThumbs(s) {
  return `<div class="thumbs">${s.rows.filter((r) => r.n).map((r) => `<div class="hd"><span>${esc(r.k)} · ${r.n}</span><span>${fx(xSet(r.ids, 'lk'))}</span></div><div class="tg">${r.ids.map((i) => `<span class="c" data-cv="${i}" style="${cv(i)}">${xBadge(i, A.posts[i].top) || '<b>new</b>'}</span>`).join('')}</div>`).join('')}</div>`;
}
function daysViz(s) {
  const mx = Math.max(6, ...s.days.map((d) => d.n)), per = s.usual / 7, H = 150;
  return `<div class="dweek">${s.days.map((d, j) => `<div class="dc"><span class="dn">${d.n || ''}</span><span class="dots">${Array.from({ length: d.n }, (_, k) => `<i style="--j:${j};--k:${k}"></i>`).join('')}</span><span class="dl">${d.d}</span></div>`).join('')}
    <span class="us" style="bottom:${32 + per * 19}px"><span>usual ${per.toFixed(1)} a day</span></span></div>`;
}
function folViz(s) {
  const v = s.wdaily.map((x) => x[1]), mx = Math.max(...v.map(Math.abs)), up = v.every((x) => x >= 0), dn = v.every((x) => x <= 0);
  const H = 120, zero = dn ? 0 : up ? H : H * Math.max(...v) / (Math.max(...v) - Math.min(...v));
  const bars = s.wdaily.map(([d, g], j) => { const h = Math.max(3, Math.abs(g) / mx * (up || dn ? H : H)); const d0 = new Date(d + 'T12:00:00'); return `<div class="b ${g >= 0 ? 'up' : 'dn'}" style="--j:${j}"><i style="height:${h}px;${g >= 0 ? `margin-bottom:${H - zero}px` : `align-self:start;margin-top:${zero}px`}"></i></div>`; }).join('');
  const lbls = s.wdaily.map(([d, g]) => `<span class="lb" style="text-align:center">${['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][new Date(d + 'T12:00:00').getDay()]}<br>${sg(g)}</span>`).join('');
  return `<div class="fol7" style="height:${H}px"><span class="z" style="top:${zero}px"></span>${bars}</div><div class="fol7" style="height:auto;margin-top:6px">${lbls}</div>
    <div class="rk" style="--n:${s.rank[1]}">${Array.from({ length: s.rank[1] }, (_, k) => `<i class="${k === s.rank[0] - 1 ? 'me' : ''}"></i>`).join('')}</div>
    <div class="rkl"><span>Best week</span><span>This week: ${s.rank[0]} of ${s.rank[1]}</span><span>Worst</span></div>`;
}
function moreHTML(s) {
  let x = '';
  if (s.why) x += `<p class="why">${esc(s.why)}</p>`;
  if (s.media.kind === 'four') x += `<p class="keyl">${esc(`Views, likes and comments are the week’s typical post against ${A.his} usual post; follows are the week’s gain.`)}</p>`;
  // (each format's week against the usual post, beside what that format does lately)
  if (s.rows && s.media.kind === 'formats') x += `<div class="frows">${s.rows.map((r) => { const ids = r.ids || [], xl = xSet(ids, 'lk'), xc = xSet(ids, 'cm'), u = xSet(A.order.slice(-100).filter((i) => A.posts[i].fmt === r.k), 'lk'); return `<div class="frow"><b>${esc(r.k)} · ${r.n}</b><span class="u">${u != null ? `lately ${fx(u)}` : ''}</span><span class="v">${r.n && xl != null ? fx(xl) : '–'}</span><span class="nt">${esc(r.note)}${r.n && xc != null ? ` · comments ${fx(xc)}` : ''}</span></div>`; }).join('')}</div>`;
  if (s.reviews) x += `<div class="rev">${s.reviews.map((r) => `<p><b>${esc(r.t)}.</b> ${esc(r.s)}</p>`).join('')}</div>`;
  if (s.so) x += `<p class="so">${esc(s.so)}</p>`;
  if (s.figs) x += `<div class="figs">${s.figs.map(([b, e]) => `<span><b>${esc(b)}</b><em>${esc(e)}</em></span>`).join('')}</div>`;
  if (s.mix) { const tot = A.n; x += `<div class="mix">${s.mix.map(([, n]) => `<i style="flex:${n}"></i>`).join('')}</div><div class="mixk">${s.mix.slice(0, 5).map(([nm, n]) => `<span><b>${Math.round(n / tot * 100)}%</b> ${esc(nm.toLowerCase())}</span>`).join('')}</div>`; }
  // (the ramp the badges are shaded on: each step a likes multiple, from well under the usual post to three times it)
  if (s.key) x += `<p class="keyl">${esc(s.key)}</p><div class="ramp"><b>×</b>${[['3+', 6], ['2', 5], ['1.5', 4], ['1', 3], ['0.8', 2], ['0.5', 1], ['0.3', 0]].map(([l, v]) => `<span style="background:var(--s${v});${v >= 5 ? 'color:#fff' : ''}">${l}</span>`).join('')}</div>`;
  return x;
}
// a line set word by word, each word in a mask of its own, so it can rise into place
const wordsHTML = (t) => String(t).split(/\s+/).filter(Boolean).map((w) => `<span class="w"><span>${esc(w)}</span></span>`).join(' ');
function footHTML(s) {
  const wide = /[a-z]/i.test(s.num) && !/^Top/.test(s.num) && !/K$|M$|weeks/.test(s.num);
  const list = s.media.kind === 'list';
  return `<div class="foot">${kickOf(s)}<div class="fig"><b class="num${wide ? ' wd' : ''}"><span>${esc(s.num)}</span></b><span class="unit">${esc(s.unit)}</span></div>
    <div class="lrow"><h3 class="line" aria-label="${esc(s.line)}">${wordsHTML(s.line)}</h3>${list ? '' : '<span class="tog" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M6 15l6-6 6 6"/></svg></span>'}</div>
    ${list ? '' : `<div class="more">${moreHTML(s)}</div>`}</div>`;
}
// the story's kicks: the bookends keep their places, and the pointers between them are named by their kind (a post that
// broke out or sank, a run, a group gone quiet, a trend, a pattern, a post whose likes arrived late with no talk)
const KICK = { Since: 'Since the last read', Profile: 'Who this is', Feeding: 'What landed', Folding: 'What sank', Format: 'Formats this week', Frequency: 'Posting this week', Followers: 'Followers this week', Feedback: "Feeder Reader's take", 'Follow-up': 'Next week',
  Breakout: 'Breakout', Sank: 'Sank', Streak: 'Streak', Quiet: 'Gone quiet', Trend: 'Trend', Pattern: 'Pattern', Boosted: 'Looks boosted' };
function storyHTML() {
  const best = A.grid.top.reduce((a, b) => (b[1] < a[1] ? b : a))[0], n = A.stories.length;
  return `<div class="player" id="player">
    <div class="vp" id="vp"><div class="track" id="track">${A.stories.map((s, k) => `<article class="slide${k === 0 ? ' on' : ''}" data-k="${k}"><i class="sdim"></i>
        <div class="card k-${s.media.kind}${s._t ? ` t-${s._t}` : ''}" data-k="${k}">${mediaHTML(s, k)}<span class="shade"></span>${footHTML(s)}</div></article>`).join('')}</div>
      <div class="igtop"><div class="igbars" id="igbars">${A.stories.map((s, k) => `<button data-k="${k}" class="${s._t ? `t-${s._t}` : ''}" aria-label="${esc(spineOf(s))}"><i><b></b></i></button>`).join('')}</div>
        <div class="ighead"><span class="av" data-cv="${best}" style="${cv(best)}"></span><b>${esc(A.handle.replace('@', ''))}</b><em id="pch" class="vd${A.stories[0]._t ? ` t-${A.stories[0]._t}` : ''}">${verdictHTML(A.stories[0])}</em>
          <button class="pp" id="pp" aria-label="Pause"><svg viewBox="0 0 24 24" id="ppi"><rect x="6" y="5" width="4" height="14" rx="1"/><rect x="14" y="5" width="4" height="14" rx="1"/></svg></button></div></div>
      <button class="arr p" id="arrp" aria-label="Back"><svg viewBox="0 0 24 24"><path d="M15 5l-7 7 7 7"/></svg></button><button class="arr n" id="arrn" aria-label="Next"><svg viewBox="0 0 24 24"><path d="M9 5l7 7-7 7"/></svg></button></div>
    <div class="hint" id="hint">${isDesk() ? '<span>Click the card to read it · ← → for the next · hold to pause</span>' : '<span>Tap a side to move · tap the words to read</span>'}</div></div>`;
}
const chLabel = (s) => (s.ch === 'Since' ? KICK.Since : s.ch) + (s.idx ? ` · ${s.idx} of 2` : '');
// a slide's verdict, said on the outside: its short phrase (what's inside, good or bad) behind an arrow that leans the
// way it went, or a plain dot for a slide that only states something
const ARW = { up: '<svg viewBox="0 0 24 24"><path d="M12 19V5M5.5 11.5L12 5l6.5 6.5"/></svg>', dn: '<svg viewBox="0 0 24 24"><path d="M12 5v14M5.5 12.5L12 19l6.5-6.5"/></svg>' };
const mark = (s) => `<i class="mk${s._t ? ` t-${s._t}` : ''}">${ARW[s._t] || ''}</i>`;
const spineOf = (s) => s.spine || KICK[s.ch] || s.ch;
const verdictHTML = (s) => `${mark(s)}<span>${esc(spineOf(s))}</span>`;
function sizePlayer() {
  if (DS) { deskSize(); return; }
  const p = $('player'); if (!p) return;
  if (isDesk()) { const h = Math.round(Math.max(520, Math.min(660, innerHeight * .74))); p.style.setProperty('--ch', h + 'px'); p.style.setProperty('--cw', Math.round(h / 1.42) + 'px'); }
  else { const w = p.clientWidth; p.style.setProperty('--ch', Math.round(Math.min(w * 1.42, Math.max(420, innerHeight - 170))) + 'px'); p.style.setProperty('--cw', w + 'px'); }
}
function measureMore() {
  if (DS) return;
  qa('.card').forEach((c) => { const m = c.querySelector('.more'), f = c.querySelector('.foot'); if (!m) return; c.style.setProperty('--mh', `${m.offsetHeight + 6}px`); c.style.setProperty('--fh', `${f.offsetHeight - m.offsetHeight - 6 + 8}px`); });
}
function slideW() { const v = $('vp'); return v ? v.clientWidth : 0; }
let el = 0, held = false, paused = false, inView = true, last = 0, durs = [];
const words = (s) => [s.unit, s.line, s.why, s.so].filter(Boolean).join(' ').split(/\s+/).length;
// the progress bars: those before the card on show full, those after empty, and the one under way filling on its own (a
// compositor animation from where it stands to full, played while the story runs and paused while it's held, paused, out of
// view or behind another tab); nothing is written to the bars frame by frame, which kept the page re-sorting its layers
// sixty times a second
let barA = null, barRun = null;
function paintBars() {
  const bs = qa('#igbars b'), d = durs[at] || 9000;
  if (barA) barA.cancel(); barA = null; barRun = null;
  bs.forEach((b, j) => { b.style.transform = j < at ? 'scaleX(1)' : 'scaleX(0)'; });
  const b = bs[at]; if (!b || RM) return;
  barA = b.animate([{ transform: `scaleX(${Math.min(1, el / d)})` }, { transform: 'scaleX(1)' }], { duration: Math.max(1, d - el), easing: 'linear', fill: 'forwards' });
  barA.pause();
}
function go(k, p0 = 0) {
  if (DS) { deskGo(k); return; }
  const n = A.stories.length; k = Math.max(0, Math.min(n - 1, k));
  const from = at;
  if (k !== at) qa('.card.open').forEach((c) => c.classList.remove('open'));
  at = k; el = 0;
  DRAGGED = p0 > .04;
  if (k !== from) turnTo(from, k, p0); else { qa('.slide').forEach((s_, j) => s_.classList.toggle('on', j === k)); if (p0) settleBack(p0); }
  { const ch = $('pch'), s = A.stories[k]; ch.className = `vd${s._t ? ` t-${s._t}` : ''}`; ch.innerHTML = verdictHTML(s); }
  $('arrp').disabled = k === 0; $('arrn').disabled = k === n - 1;
  paintBars();
  const c = q1(`.card[data-k="${k}"]`); if (c) layoutCard(c, k);
  storyArrive(k);
}
function frame(t) {
  if (dead) return;
  const dt = last ? Math.min(100, t - last) : 0; last = t;
  const run = !!(A && durs.length && !held && !paused && !q1('.card.open') && inView && !document.hidden);
  if (run) { el += dt; const d = durs[at] || 9000; if (el >= d) { if (at < A.stories.length - 1) go(at + 1); else el = d; } }
  if (barA && run !== barRun) { barRun = run; if (run) barA.play(); else barA.pause(); }
  raf = requestAnimationFrame(frame);
}
function setPaused(p) {
  paused = p;
  $('ppi').innerHTML = p ? '<path d="M7 5v14l12-7z"/>' : '<rect x="6" y="5" width="4" height="14" rx="1"/><rect x="14" y="5" width="4" height="14" rx="1"/>';
  $('pp').setAttribute('aria-label', p ? 'Play' : 'Pause');
}
// a tap on the story when it isn't square on the screen: the page glides it to the middle of what the header and the
// bottom bar leave once both are folded (and folds them), and that is all the tap does; once it's there a tap reads it.
// True when the tap was spent on that
function centreStory() {
  const vp = $('vp'); if (!vp || DS) return false;
  const hd = document.querySelector('header[aria-label="Read"]'), row = hd && hd.querySelector(':scope > div > div');
  const nav = document.querySelector('[data-fm-bottom-nav]'), H = innerHeight;
  // the header folds to its title row; the bar to its 56px circle
  const top = row ? row.getBoundingClientRect().bottom + 18 : 150, bottom = nav ? nav.getBoundingClientRect().bottom - 56 - 12 : H - 90;
  const b = vp.getBoundingClientRect(), off = (b.top + b.bottom) / 2 - (top + bottom) / 2;
  const y = Math.max(0, Math.min(document.documentElement.scrollHeight - H, Math.round(scrollY + off)));
  const folded = !nav || nav.hasAttribute('data-mini');
  if (Math.abs(y - scrollY) < 12) { if (folded) return false; if (fold) fold(scrollY); return true; }
  if (fold) fold(y);
  scrollTo({ top: y, behavior: RM ? 'auto' : 'smooth' });
  return true;
}
function hookPlayer() {
  const vp = $('vp');
  durs = A.stories.map((s) => Math.max(8000, Math.min(16000, 5000 + words(s) * 170))); el = 0; paused = false;
  let drag = null, holdT = null;
  vp.addEventListener('pointerdown', (e) => { if (e.button > 0 || e.target.closest('.godeep, .arr, .pp, .igbars')) return; drag = { x: e.clientX, y: e.clientY, t: performance.now(), dx: 0, mode: null, id: e.pointerId, target: e.target }; clearTimeout(holdT); holdT = setTimeout(() => { held = true; }, 220); });
  vp.addEventListener('pointermove', (e) => {
    if (!drag || e.pointerId !== drag.id) return;
    const dx = e.clientX - drag.x, dy = e.clientY - drag.y;
    if (!drag.mode) { if (Math.abs(dx) > 8 || Math.abs(dy) > 10) { clearTimeout(holdT); held = false; } if (Math.abs(dx) > 8 && Math.abs(dx) > Math.abs(dy) * 1.1) { drag.mode = 'x'; try { vp.setPointerCapture(e.pointerId); } catch {} } else if (Math.abs(dy) > 10) drag.mode = 'y'; }
    if (drag.mode === 'x') { const now = performance.now(); drag.v = (dx - drag.dx) / Math.max(1, now - (drag.lt || drag.t)); drag.lt = now; drag.dx = dx; const nb = at + (dx < 0 ? 1 : -1), cur = slideOf(at); if (RM || !cur) return;
      // (past the first or the last card it only gives a little, as if held at the edge)
      if (nb < 0 || nb >= A.stories.length) { if (DRN != null) { rest(slideOf(DRN), false); DRN = null; } cur.style.transform = `translate3d(${dx * .22}px,0,0)`; return; }
      cur.style.transform = ''; turnAt(nb, dx < 0 ? 1 : -1, Math.min(1, Math.abs(dx) / slideW())); }
  });
  const end = (e) => {
    clearTimeout(holdT); const wasHeld = held; held = false;
    if (!drag || e.pointerId !== drag.id) return; const d = drag; drag = null;
    if (wasHeld && !d.mode) return;
    if (d.mode === 'x') {
      const v = d.v ?? d.dx / Math.max(1, performance.now() - d.t), W = slideW(), k = d.dx < -W * .16 || v < -.45 ? at + 1 : d.dx > W * .16 || v > .45 ? at - 1 : at;
      // the glide leaves at the finger's speed (the glide curve starts 2.25 times its average), between 260 and 520ms
      const p0 = Math.min(1, Math.abs(d.dx) / W), cur = slideOf(at);
      if (k === at || k < 0 || k >= A.stories.length) { if (cur && cur.style.transform && DRN == null) cur.animate([{ transform: cur.style.transform }, { transform: 'none' }], { duration: 380, easing: GROW }).onfinish = () => { cur.style.transform = ''; }; if (cur && DRN == null) cur.style.transform = ''; if (DRN != null) settleBack(p0); return; }
      // (the spring sets off at the finger's speed, in whole remaining distances a second)
      SWV = Math.min(7, (Math.abs(v) * 1000) / Math.max(40, (1 - p0) * W));
      go(k, p0); return;
    }
    if (d.mode) return;
    if (centreStory()) return;
    // a tap: on (or just above) the words, they open or close; elsewhere the left third goes back and the rest goes on,
    // as on Instagram. An open card closes on any tap
    const card = q1(`.card[data-k="${at}"]`); if (!card || !d.target.closest('.slide')) return;
    if (card.classList.contains('open')) { setOpen(card, false); return; }
    const foot = card.querySelector('.foot'), r = card.getBoundingClientRect();
    if (!card.classList.contains('k-list') && foot && d.y >= foot.getBoundingClientRect().top - 28) { setOpen(card, true); return; }
    if (d.x - r.left < r.width / 3) { if (at > 0) go(at - 1); else { el = 0; paintBars(); } } else if (at < A.stories.length - 1) go(at + 1);
  };
  vp.addEventListener('pointerup', end); vp.addEventListener('pointercancel', (e) => { if (drag && drag.mode === 'x') { const p0 = Math.min(1, Math.abs(drag.dx) / slideW()); drag = null; if (DRN != null) settleBack(p0); } else drag = null; });
  let acc = 0, lock = false;
  vp.addEventListener('wheel', (e) => { if (Math.abs(e.deltaX) <= Math.abs(e.deltaY)) return; e.preventDefault(); if (lock) return; acc += e.deltaX; if (Math.abs(acc) > 46) { go(at + (acc > 0 ? 1 : -1)); acc = 0; lock = true; setTimeout(() => { lock = false; }, 520); } }, { passive: false });
  $('arrp').addEventListener('click', () => go(at - 1));
  $('arrn').addEventListener('click', () => go(at + 1));
  qa('#igbars button').forEach((b) => b.addEventListener('click', (e) => { e.stopPropagation(); go(+b.dataset.k); }));
  $('pp').addEventListener('click', (e) => { e.stopPropagation(); setPaused(!paused); });
  const io = new IntersectionObserver((es) => es.forEach((x) => { inView = x.intersectionRatio > .5; }), { threshold: [0, .5, 1] }); io.observe(vp); ios.push(io);
}
on(window, 'keydown', (e) => {
  if (!A || e.target.closest('input,textarea,select')) return;
  const p = $('player') || $('ds'); if (!p) return; const r = p.getBoundingClientRect();
  if (r.bottom < 80 || r.top > innerHeight - 80) return;
  if (e.key === 'ArrowRight') { go(at + 1); e.preventDefault(); } if (e.key === 'ArrowLeft') { go(at - 1); e.preventDefault(); }
});
function layoutCard(card, k) {
  const s = A.stories[k];
  if (s.media.kind === 'weeks') { const h = card.querySelector('[data-wk16]'); if (h && !h.dataset.drawn) { drawWk16(h, s); h.dataset.drawn = 1; } }
}
// the weeks as bars on the usual post: each week's typical post, over it or under it, as far as its likes multiple
// (on a log scale, a quarter to four times)
function drawWk16(host, s) {
  const card = host.closest('.card'), ws = A.weeks.filter((w) => !w.part).slice(-s.media.n), W = host.clientWidth || 300, H = Math.max(170, (card ? card.clientHeight : 480) - 230), N = ws.length, cw = W / N, bw = cw * .62, mid = 20 + (H - 40) / 2, amp = (H - 40) / 2;
  let x = `<svg viewBox="0 0 ${W} ${H}" height="${H}"><line x1="0" x2="${W}" y1="${mid}" y2="${mid}" stroke="rgb(255 255 255 / .35)" stroke-dasharray="3 3"/>`;
  ws.forEach((w, i) => {
    const xx = i * cw + (cw - bw) / 2, m = w.ids.length ? xSet(w.ids, 'lk') : null;
    if (m == null) { x += `<rect x="${xx}" y="${mid - 1}" width="${bw}" height="2" fill="rgb(255 255 255 / .3)"/>`; }
    else { const L = Math.max(-2, Math.min(2, Math.log2(Math.max(m, .01)))), h = Math.max(3, Math.abs(L) / 2 * amp), up = L >= 0, me = i === N - 1; x += `<rect x="${xx}" y="${up ? mid - h : mid}" width="${bw}" height="${h}" rx="3" fill="${up ? '#fff' : me ? 'var(--accent)' : 'rgb(255 255 255 / .28)'}"/>`; }
    const lb = w.s.split(' '); if ((+lb[0] <= 7 && i < N - 2) || i === N - 1) x += `<text x="${i === N - 1 ? W : i * cw + cw / 2}" y="${H - 2}" text-anchor="${i === N - 1 ? 'end' : 'middle'}" font-size="10" fill="rgb(255 255 255 / ${i === N - 1 ? '.9' : '.5'})">${i === N - 1 ? 'This week' : lb[1]}</text>`;
  });
  x += `<text x="0" y="${mid - 7}" font-size="10" font-weight="600" fill="rgb(255 255 255 / .7)" stroke="var(--card)" stroke-width="3" paint-order="stroke">${esc(A.his)} usual post</text><text x="0" y="16" font-size="10" fill="rgb(255 255 255 / .5)">↑ over it</text><text x="0" y="${H - 18}" font-size="10" fill="rgb(255 255 255 / .5)">↓ under it</text>`;
  host.innerHTML = x + '</svg>';
}

let ios = [];
const RM = matchMedia('(prefers-reduced-motion: reduce)').matches;
const isDesk = () => matchMedia('(min-width: 1024px)').matches;
const CARRY = 'cubic-bezier(.3, .3, .2, 1)', ROLL = 'cubic-bezier(.22, 1, .36, 1)', OUTE = 'cubic-bezier(.2, .8, .2, 1)';
const WD = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const FMT1 = { Reels: 'reel', Carousels: 'carousel', Photos: 'photo' };
const low = (s) => (/^(Aneet|Ved|Rohit|Simran|Kareena)\b/.test(s) ? s : s[0].toLowerCase() + s.slice(1));
const cap1 = (s) => (s ? s[0].toUpperCase() + s.slice(1) : s);
const plain = (s) => String(s ?? '').replace(/\s*\{c\d?\}/g, '');
const medOf = (v) => { v = v.filter((x) => x != null).sort((a, b) => a - b); if (!v.length) return null; const m = v.length >> 1; return v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2; };
const byRank = (ids, key = 'top') => [...ids].sort((a, b) => (A.posts[a][key] ?? 999) - (A.posts[b][key] ?? 999) || (A.posts[b].lk ?? 0) - (A.posts[a].lk ?? 0));
const byF48 = (ids) => [...ids].sort((a, b) => (A.posts[b].f48 ?? -1e9) - (A.posts[a].f48 ?? -1e9));
const dr = (a, b) => (a === b ? day(a) : `${day(a)} – ${day(b)}`);
const isoAdd = (iso, n) => { const d = new Date(iso + 'T12:00:00'); d.setDate(d.getDate() + n); return d.toISOString().slice(0, 10); };
const dow = (iso) => new Date(iso + 'T12:00:00').getDay();
const dayNo = (iso) => Math.round(new Date(iso + 'T12:00:00').getTime() / 864e5);
const mname = (m) => (m === 'ct' ? 'comments' : m === 'lt' || A.yard === 'likes' ? 'likes' : 'overall');
const ord = (n) => (n === 1 ? 'best' : n === 2 ? 'second best' : n === 3 ? 'third best' : `${n}th`);
const tone = (v) => (v == null ? 'rgb(var(--ink-rgb) / .16)' : v < 50 ? `rgb(218 46 70 / ${Math.min(1, .3 + (50 - v) / 40 * .7).toFixed(2)})` : `rgb(var(--ink-rgb) / ${(.1 + Math.min(1, (v - 50) / 50) * .28).toFixed(2)})`);
const ICX = '<svg viewBox="0 0 24 24"><path d="M6 6l12 12M18 6L6 18"/></svg>';
const ICL = '<svg viewBox="0 0 24 24"><path d="M15 5l-7 7 7 7"/></svg>';
const ICR = '<svg viewBox="0 0 24 24"><path d="M9 5l7 7-7 7"/></svg>';
let IGS = new Set();
function coverStyle(id) {
  const p = A.posts[id], lo = covers[id], h = IGS.has(id) ? `url(${BASE}ig/${id}.jpg)` : p && p.rp && p.rp[0] ? `url(${BASE}${p.rp[0].f})` : '';
  const layers = [h, lo ? `url(${lo})` : ''].filter(Boolean);
  return layers.length ? `background-image:${layers.join(',')}` : '';
}
// a post named the way a fan would: the name the read file gives it (voice.names), else the name the story gave it, else
// its caption where that's a short line of its own (handles and tags off), else a longer caption by its opening words,
// else who's in it, else its slice; the last two with its format and its day
function lab(id) {
  const p = A.posts[id]; if (!p) return '';
  const nm = ((A.voice || {}).names || {})[id]; if (nm) return nm;
  const rv = A.stories.flatMap((s) => s.reviews || []).find((r) => r.id === id);
  if (rv) return `‘${rv.t.split(' · ')[0]}’`;
  const c = (p.cap || '').split('\n')[0].replace(/[@#][\w.]+/g, '').replace(/[^\p{L}\p{N}'’?!,. ]/gu, ' ').replace(/\s+/g, ' ').replace(/[\s,.]+$/, '').trim(), cw = c.split(' '), nw = cw.length;
  if (nw >= 2 && nw <= 7 && c.length <= 44) return `‘${c}’`;
  const op = cw.slice(0, 5).join(' ').replace(/[\s,.!?;:]+$/, '');
  if (nw > 7 && op.length <= 36) return `‘${op}…’`;
  const who = regsAll().find((g) => g.kind === 'person' && g.re.test(txtOf(id)));
  return `the ${who ? who.name : low(p.pat)} ${FMT1[p.fmt] || 'post'} from ${day(p.d)}`;
}
/* ---- multiples: a count against the account's usual post (the median of the posts in the current 100 that have
   reached day 7). A post's likes ÷ the usual likes; a set's typical likes ÷ the usual likes. Top % stays as a quiet
   second ---- */
const B = {};
const KEYN = { lk: 'likes', cm: 'comments', pl: 'plays', f48: 'follows', fd: 'follows' };
function baseBuild() {
  const P = A.posts, W = A.order.slice(-100), R = W.filter((i) => P[i].top != null);
  ['lk', 'cm', 'pl', 'f48'].forEach((k) => { const v = R.map((i) => P[i][k]).filter((x) => x != null); B[k] = v.length >= 8 ? medOf(v) : null; });
  const d0 = P[W[0]].d, days = A.daily.filter((x) => x[0] >= d0).map((x) => x[1]);
  B.day = days.length ? medOf(days) : null;
  // a post's follows against the pace at the time (fd). Its "48h after" is the follower change over its day and the two
  // after, so every post that day shares it: fd takes off three days at the pace of the ten days either side (the
  // post's own three left out), so a slump or a surge the whole feed rode doesn't land on whichever post was up then
  const DD = Object.fromEntries(A.daily.map((x) => [x[0], x[1]]));
  A.order.forEach((i) => { const p = P[i]; if ('fd' in p) return; const around = p.f48 == null ? [] : [...Array.from({ length: 10 }, (_, j) => isoAdd(p.d, -10 + j)), ...Array.from({ length: 10 }, (_, j) => isoAdd(p.d, 3 + j))].map((x) => DD[x]).filter((x) => x != null); p.fd = around.length >= 7 ? p.f48 - 3 * medOf(around) : null; });
}
const xPost = (id, k) => (B[k] > 0 && A.posts[id] && A.posts[id][k] != null ? A.posts[id][k] / B[k] : null);
const xSet = (ids, k) => {
  if (!(B[k] > 0)) return null;
  const P = A.posts, r = ids.filter((i) => P[i].top != null), v = (r.length ? r : ids).map((i) => P[i][k]).filter((x) => x != null);
  return v.length >= (k === 'pl' ? 2 : 1) ? medOf(v) / B[k] : null;
};
const fx = (m) => (m == null ? '–' : m === 0 ? '0×' : m >= 10 ? `${Math.round(m)}×` : m >= .1 ? `${String(Math.round(m * 10) / 10)}×` : `${m.toFixed(2)}×`);
const tx = (m) => (m != null && m >= 1 ? 'beating' : 'plain');
const xshade = (m) => (m == null ? 0 : m >= 3 ? 6 : m >= 2 ? 5 : m >= 1.4 ? 4 : m >= 1 ? 3 : m >= .7 ? 2 : m >= .45 ? 1 : 0);
const listOf = (a) => (a.length <= 1 ? a.join('') : `${a.slice(0, -1).join(', ')} and ${a[a.length - 1]}`);
// a story card that leads on a rank leads on a multiple instead: its post, or the week's set it speaks for
// each slide's lean: up (it ran hot), down (it ran cold) or level, from its own numbers. The followers slide is set
// against a usual week, the median of the ten full weeks before it, and says so
function storyTone() {
  const W = A.weeks.filter((w) => !w.part), i = W.findIndex((w) => w.s === '19 Oct');
  const prev = W.slice(Math.max(0, i - 10), Math.max(0, i)).map((w) => w.g).filter((g) => g != null), uw = prev.length ? medOf(prev) : null;
  A.stories.forEach((s) => {
    let t = '';
    // (a pointer says which way it went: crimson for good, grey for bad, none for neither)
    if (s.tone) { s._t = s.tone === 'none' ? '' : s.tone; return; }
    if (s.ch === 'Since') { const r = s.media.rows.find((x) => x.k === s.media.lead); t = r && r.a != null && r.b != null ? (r.b >= r.a ? 'up' : 'dn') : ''; }
    else if (s.ch === 'Feeding') t = 'up';
    else if (s.ch === 'Folding') t = 'dn';
    else if (/×$/.test(s.num)) t = parseFloat(s.num) >= 1 ? 'up' : 'dn';
    else if (s.ch === 'Followers' && i >= 0 && W[i].g != null) {
      const g = W[i].g, m = g > 0 && uw > 0 ? g / uw : null;
      if (!s._u && uw != null && !/usual week/.test(s.unit)) { s._u = true; s.unit = `followers this week · ${m != null ? `${fx(m)} ` : ''}a usual week: ${sg(Math.round(uw))}${s.unit.replace(/^followers this week/, '')}`; }
      t = g < 0 || (uw > 0 && g < uw * .5) ? 'dn' : uw != null && g >= uw ? 'up' : '';
    } else if (s.ch === 'Feedback' && /^\d+ of \d+$/.test(s.num)) { const [a, b] = s.num.split(' of ').map(Number); t = a / b >= .55 ? 'up' : a / b <= .4 ? 'dn' : ''; }
    s._t = t;
  });
}
function storyX() {
  const P = A.posts, lw = A.weeks.find((w) => w.s === '19 Oct'), wk = lw ? lw.ids : [];
  A.stories.forEach((s) => {
    if (s._x || !/^Top /.test(s.num)) return;
    const v = parseFloat(s.num.slice(4)), it = (s.media.items || []).find((x) => x.v != null && Math.abs(x.v - v) < .6);
    let ids = it ? [it.id] : null;
    if (!ids && s.media.kind === 'weeks') ids = wk;
    if (!ids && s.media.kind === 'formats') { const f = /reels/.test(s.unit) ? 'Reels' : /carousels/.test(s.unit) ? 'Carousels' : /photos/.test(s.unit) ? 'Photos' : ''; ids = wk.filter((i) => P[i].fmt === f); }
    if (!ids && /famous face/.test(s.unit)) ids = wk.filter((i) => P[i].fx && P[i].fx.known);
    const m = ids && ids.length ? (ids.length === 1 ? xPost(ids[0], 'lk') : xSet(ids, 'lk')) : null;
    if (m == null) return;
    s._x = true;
    const was = s.num;
    s.unit = `of ${A.his} usual ${K(Math.round(B.lk))} likes · ${s.unit.replace(/\s*·\s*(usual for \w+|the other \d+): Top [\d.]+%/, '').replace(/^(his|its) /, '')} · ${was}`;
    s.num = fx(m);
    // (a figure that repeated the old headline would repeat the new one: it goes)
    if (s.figs) s.figs = s.figs.filter(([v]) => v !== was);
  });
}
function badgeOf(p, metric) {
  if (metric && metric[0] === 'x') { const m = B[metric.slice(1)] > 0 && p[metric.slice(1)] != null ? p[metric.slice(1)] / B[metric.slice(1)] : null, h = xshade(m); return `<span class="bdg${h >= 5 ? ' w' : ''}" style="background:var(--s${h})">${m == null ? (p.top == null ? 'New' : '–') : fx(m)}</span>`; }
  if (metric === 'f48' || metric === 'fd') return p[metric] == null ? '' : `<span class="bdg fl${p[metric] < 0 ? ' dn' : ''}">${sg(Math.round(p[metric]))}</span>`;
  const v = p[metric || 'top'];
  return `<span class="bdg${wt(v)}" style="background:${v == null ? 'var(--s0)' : sv(v)}">${v == null ? 'New' : top(v)}</span>`;
}
function tileHTML(id, g = {}) {
  const p = A.posts[id]; if (!p) return '';
  const tag = g.tag && g.tag[id];
  return `<button class="pt${g.mark && g.mark.includes(id) ? ' mk' : ''}" data-post="${id}"><span class="cv" data-cv="${id}" style="${coverStyle(id)}">${badgeOf(p, g.metric)}${tag ? `<span class="ptg">${esc(tag)}</span>` : ''}</span><span class="m1"><b>${day(p.d)}</b><span>${cap1(FMT1[p.fmt] || 'post')}</span></span><span class="m2">${esc(p.cap || p.desc || '')}</span></button>`;
}
const rk = (v) => `<span class="rkp${wt(v)}" style="background:${v == null ? 'var(--s0)' : sv(v)}">${v == null ? 'New' : top(v)}</span>`;
function segHTML(name, opts, on) { return `<div class="seg" data-seg="${name}"><span class="pill"></span>${opts.map(([v, l]) => `<button data-sv="${esc(v)}" aria-pressed="${v === on}">${l}</button>`).join('')}</div>`; }
function placeSeg(seg) { if (!seg) return; const b = seg.querySelector('button[aria-pressed="true"]'), p = seg.querySelector('.pill'); if (!b || !p || !b.offsetWidth) return; p.style.width = b.offsetWidth + 'px'; p.style.transform = `translateX(${b.offsetLeft - 4}px)`; }

const headHTML = (f) => `<div class="hd1"><b class="nm">${esc(f.num)}</b><span class="un">${esc(f.unit)}</span></div>`;
const bodyHTML = (f) => `<p class="ln">${esc(plain(f.line))}${f.aside ? ` <span class="as">${esc(plain(f.aside))}</span>` : ''}</p>`;

/* =====================================================================
   STRATEGY: the current 100 in three acts. Act 1, Slices: each kind of post and its four numbers (views, likes,
   comments, follows), led by the one it stands out on. Act 2, a part for each count, each made of what moves it: the
   sides that hold up, the formats and the regulars, every one the posts with it against the posts without. Act 3, the
   100 over time: its runs (Streaks), what changed between its older half and its newer one (Shifts), and how often it
   posts (Frequency). Every card is marked against the last read (New, Shifted or Held), every part heads with its own
   course, week by week, and last week's posts are red in every one. A row opens its posts in the panel, and the panel
   steps through the part's rows.
   ===================================================================== */
const LENS = [
  ['slices', 'Slices', 'What each kind of post earns', 1], ['views', 'Views', 'What moves the plays', 2], ['likes', 'Likes', 'What moves the likes', 2],
  ['comments', 'Comments', 'What moves the comments', 2], ['follows', 'Follows', 'What brings followers', 2],
  ['streaks', 'Streaks', 'The 100 in order', 3], ['shifts', 'Shifts', 'What changed inside the 100', 3], ['frequency', 'Frequency', 'How often, and what it does', 3],
];
const ACTS = { 1: 'Slices', 2: 'Views · Likes · Comments · Follows', 3: 'Streaks · Shifts · Frequency' };
// the count each part of Act 2 reads
const LK = { views: 'pl', likes: 'lk', comments: 'cm', follows: 'fd' };
const S = { win: [], inW: new Set(), wk: new Set(), wkDays: new Set(), span: '', L: {}, last: '', F: null, F1: null };
const rankOf = (ids, key = 'top') => medOf(ids.map((i) => A.posts[i][key]));
const ofW = (ids) => ids.filter((i) => S.inW.has(i));
const capHTML = (f) => `<div class="cap"><div class="w1"><div class="c" data-tone="${f.tone}">${headHTML(f)}</div></div><div class="w2"><div class="c">${bodyHTML(f)}</div></div></div>`;
const keyed = (rows, k = (r) => r.key) => Object.fromEntries(rows.map((r) => [k(r), r]));

/* ---- a read's footing: its 100 posts, those of them that had reached day 7, and its usual post on each count. This
   read stands on today. The last read is rebuilt as it stood the day before this week began: the posts up to then,
   counted only once they were a week old then ---- */
function footing(asOf) {
  const P = A.posts, W = (asOf ? A.order.filter((i) => P[i].d <= asOf) : A.order).slice(-100);
  const R = new Set(W.filter((i) => P[i].top != null && (!asOf || P[i].d <= isoAdd(asOf, -7))));
  const b = {}; ['lk', 'cm', 'pl', 'f48'].forEach((k) => { const v = W.filter((i) => R.has(i)).map((i) => P[i][k]).filter((x) => x != null); b[k] = v.length >= 8 ? medOf(v) : null; });
  return { W, R, B: b, asOf };
}
// a set's typical count against a footing's usual post, as xSet does it for the page
function xF(F, ids, k) { if (!(F.B[k] > 0)) return null; const r = ids.filter((i) => F.R.has(i)), v = (r.length ? r : ids).map((i) => A.posts[i][k]).filter((x) => x != null); return v.length >= (k === 'pl' ? 2 : 1) ? medOf(v) / F.B[k] : null; }
// a set's typical follows against the pace at the time: a count, since it runs either side of zero
function folF(F, ids) { const r = ids.filter((i) => F.R.has(i)), v = (r.length ? r : ids).map((i) => A.posts[i].fd).filter((x) => x != null); return v.length >= 3 ? medOf(v) : null; }
// a set's four numbers: views, likes and comments against the usual post, follows against the pace at the time
function fourOf(F, ids) { return { pl: xF(F, ids, 'pl'), lk: xF(F, ids, 'lk'), cm: xF(F, ids, 'cm'), fd: folF(F, ids), npl: ids.filter((i) => A.posts[i].pl != null).length }; }
// how far a multiple sits from the usual post, on one scale for every count
const lean = (m) => (m == null ? 0 : Math.abs(Math.log(Math.max(m, .01))));
// moved since the last read: it went up or down by half or more, or crossed the usual post (zero, for a count) by
// more than a hair. `sc` is a count's scale (the usual post's follows), so a count near zero isn't moved by a handful
function moved(now, was, count, sc = 1) {
  if (now == null || was == null) return false;
  if (count) return Math.abs(now - was) >= .25 * Math.abs(sc) && (Math.sign(now) !== Math.sign(was) || Math.abs(now - was) >= .5 * Math.abs(was));
  return Math.abs(now / was - 1) >= .5 || ((now >= 1) !== (was >= 1) && Math.abs(Math.log(now / was)) >= Math.log(1.15));
}
const MK = { new: 'New', shifted: 'Shifted', held: 'Held' };
// a number's mark: New where the last read had no such card, Shifted where it moved, else Held
const markOf = (was, now, prev, count) => (!was ? { t: 'new' } : moved(now, prev, count, B.f48) ? { t: 'shifted', was: count ? sg(Math.round(prev)) : fx(prev) } : { t: 'held' });

/* ---- Act 1: the slices, each with its four numbers, led by the multiple it stands out on (furthest from the usual
   post; plays only once three of its posts have them). Follows stay in the four: a count can't be set against a
   multiple ---- */
function slicesOf(F) {
  const P = A.posts, g = {}; F.W.forEach((i) => (g[P[i].pat] = g[P[i].pat] || []).push(i));
  return Object.entries(g).map(([name, ids]) => { const f = fourOf(F, ids); return { key: name, title: name, ids: byRank(ids), n: ids.length, f, lead: leadOf(f, F) }; });
}
function leadOf(f, F) {
  const c = [['lk', lean(f.lk)], ['cm', lean(f.cm)]];
  if (f.npl >= 3 && f.pl != null) c.push(['pl', lean(f.pl)]);
  return c.sort((a, b) => b[1] - a[1])[0][0];
}

/* ---- Act 2: what a post can have. The sides that hold up (inside the slice where they only hold there; the ones that
   only look real are left out), its format, and the regulars. Each is the posts with it against the posts without ---- */
const REGL = (g) => (g.kind === 'person' ? `With ${g.name}` : g.kind === 'place' ? `In ${g.name}` : g.kind === 'event' ? `At ${g.name}` : g.name);
const txtOf = (i) => { const p = A.posts[i]; return `${p.cap || ''} ${p.sum || ''} ${p.prem || ''}`; };
function candidatesOf(F) {
  const P = A.posts, W = F.W, inW = new Set(W), out = [];
  A.sides.forEach((g) => { if (/real/.test(g.name)) return; g.rows.forEach((r) => out.push({ key: `${r.label}|${r.slice || ''}|${r.key || 'top'}`, title: r.name, slice: r.slice, kind: 'side', sm: r.key || 'top', wiAll: r.wi.filter((i) => inW.has(i)), woAll: r.wo_ids.filter((i) => inW.has(i)), lw: ['With it', 'Without it'] })); });
  ['Reels', 'Carousels', 'Photos'].forEach((f) => out.push({ key: `fmt:${f}`, title: f, kind: 'format', wiAll: W.filter((i) => P[i].fmt === f), woAll: W.filter((i) => P[i].fmt !== f), lw: [f, 'The rest'] }));
  regsAll().forEach((g) => { const wi = W.filter((i) => g.re.test(txtOf(i))), has = new Set(wi); out.push({ key: `reg:${g.name}`, title: REGL(g), kind: 'regular', wiAll: wi, woAll: W.filter((i) => !has.has(i)), lw: [REGL(g), 'The rest'] }); });
  return out;
}
/* one thing on one count: its typical post against the rest's. It moves the count where the gap is 1.4× or more (for
   follows against the pace, half the usual post's follows) and most of its posts sit on that side of the rest's
   typical post, so one big post can't make it. Plays need three posts on each side, the other counts four */
function moverOf(F, c, k) {
  const P = A.posts, min = k === 'pl' ? 3 : 4, cnt = (ids) => { const v = ids.filter((i) => P[i][k] != null), r = v.filter((i) => F.R.has(i)); return r.length >= min ? r : v; };
  const wi = cnt(c.wiAll), wo = cnt(c.woAll), days = (ids) => new Set(ids.map((i) => P[i].d)).size;
  // (posts on the same day share their follows, so follows want four posting days on each side, not four posts)
  if (wi.length < min || wo.length < min || (k === 'fd' && (days(wi) < 4 || days(wo) < 4))) return null;
  const sc = k === 'fd' ? F.B.f48 : F.B[k], a = medOf(wi.map((i) => P[i][k])), o = medOf(wo.map((i) => P[i][k])), lin = k === 'fd' || !(a > 0 && o > 0), x = lin ? null : a / o, up = lin ? a > o : x > 1;
  if (lin ? Math.abs(a - o) < .5 * Math.abs(sc || 1) : Math.abs(Math.log(x)) < Math.log(1.4)) return null;
  const sh = wi.filter((i) => P[i][k] > o).length / wi.length; if (up ? sh < .6 : sh > .4) return null;
  // (and it holds inside the slices: where two or more slices have posts both with it and without, it goes the same way
  // in two of every three, and the other counts take that test where it can be run. Follows ride the calendar, so
  // theirs is run inside the weeks instead, and a follows mover has to pass it)
  if (!c.slice || k === 'fd') {
    const wkOf = (i) => (A.weeks.find((w) => w.ids.includes(i)) || {}).s, by_ = k === 'fd' ? wkOf : (i) => P[i].pat;
    const g = {}; [...wo.map((i) => [i, 0]), ...wi.map((i) => [i, 1])].forEach(([i, h]) => { (g[by_(i)] = g[by_(i)] || [[], []])[h].push(P[i][k]); });
    const st = Object.values(g).filter(([x, y]) => x.length >= 2 && y.length >= 2), agree = st.filter(([x, y]) => medOf(y) > medOf(x) === up).length;
    if (st.length >= 2 ? agree < (2 / 3) * st.length : k === 'fd') return null;
  }
  const by = (ids) => [...ids].sort((p, q) => P[q][k] - P[p][k]);
  return { ...c, k, ids: by(wi), wo: by(wo), a, o, x, lin, up, n: wi.length, v: lin ? a - o : x, w: lin ? Math.abs(a - o) / Math.abs(sc || 1) : Math.abs(Math.log(x)) };
}
function moversOf(F, k) {
  const rows = candidatesOf(F).map((c) => moverOf(F, c, k)).filter(Boolean).sort((p, q) => q.w - p.w), keep = [];
  // (two that hold nearly the same posts, the same way, are one: the stronger stands)
  rows.forEach((r) => { if (keep.some((q) => q.up === r.up && r.ids.filter((i) => q.ids.includes(i)).length >= .8 * Math.min(r.ids.length, q.ids.length))) return; keep.push(r); });
  return keep.slice(0, 6).sort((p, q) => (q.up ? q.w : -q.w) - (p.up ? p.w : -p.w));
}
// the follows part's own rows: the 48h after the best posts against the rest, and the best and worst days in the 100
function folRowsOf(F) {
  const P = A.posts, W = F.W, d0 = P[W[0]].d, end = F.asOf || '9999', days = A.daily.filter((x) => x[0] >= d0 && x[0] <= end);
  const ok = W.filter((i) => F.R.has(i) && P[i].f48 != null), t10 = byF48(ok.filter((i) => P[i].top <= 10)), rest = ok.filter((i) => P[i].top > 10);
  return { days, t10, a: medOf(t10.map((i) => P[i].f48)), b: medOf(rest.map((i) => P[i].f48)), hi: [...days].sort((x, z) => z[1] - x[1])[0], lo: [...days].sort((x, z) => x[1] - z[1])[0] };
}

/* ---- Act 3. Streaks: six or more posts in a row over (or under) the usual post, counted on likes and on comments
   against the same usual post the multiples use. A comments run that mostly covers a likes run is the same run ---- */
function runsOf(F) {
  const P = A.posts, W = F.W, runs = [];
  ['lk', 'cm'].forEach((k) => {
    if (!(F.B[k] > 0)) return;
    const R = W.filter((i) => F.R.has(i) && P[i][k] != null), side = (i) => (P[i][k] >= F.B[k] ? 1 : -1);
    let s0 = 0;
    for (let j = 1; j <= R.length; j++) if (j === R.length || side(R[j]) !== side(R[s0])) { if (j - s0 >= 6) runs.push({ k, run: R.slice(s0, j), up: side(R[s0]) > 0, brk: R[j] || null }); s0 = j; }
  });
  const merged = [];
  runs.forEach((r) => { const m = merged.find((x) => x.up === r.up && r.run.filter((i) => x.run.includes(i)).length >= .8 * Math.min(x.run.length, r.run.length)); if (m) m.ks.push(r.k); else merged.push({ ...r, ks: [r.k] }); });
  return merged.map((r) => ({ key: r.brk || `run-${r.run[0]}`, mk: `${r.up ? 'u' : 'd'}:${r.run[0]}`, brk: r.brk, k: r.k, ks: r.ks, up: r.up, n: r.run.length, a: P[r.run[0]].d, b: P[r.run[r.run.length - 1]].d, run: r.run, ids: r.brk ? [...r.run, r.brk] : r.run,
    mark: r.brk ? [r.brk] : null, tag: r.brk ? { [r.brk]: 'Ended it' } : null, title: `${r.run.length} in a row ${r.up ? 'over' : 'under'} the usual` })).sort((x, z) => z.b.localeCompare(x.b));
}
/* Shifts: the 100's newer half against its older half. The typical post on each count, a typical follower day, and
   each slice's count of posts and its likes. Kept where a number crossed the usual post or moved by half, or a slice
   at least doubled (or halved) by five posts or more */
function shiftsOf(F) {
  const P = A.posts, W = F.W, h = W.length >> 1, old = W.slice(0, h), nu = W.slice(h), d0 = P[old[0]].d, d1 = P[nu[0]].d, rows = [];
  const enough = (ids, k) => ids.filter((i) => P[i][k] != null).length >= 3;
  ['pl', 'lk', 'cm'].forEach((k) => {
    if (!enough(old, k) || !enough(nu, k)) return;
    const a = xF(F, old, k), b = xF(F, nu, k);
    if (moved(b, a) && Math.abs(Math.log(b / a)) >= Math.log(1.25)) rows.push({ key: `all:${k}`, kind: 'all', k, title: `${cap1(A.his)} typical post`, ids: nu, wo: old, a, b, dir: Math.sign(b - a), w: Math.abs(Math.log(b / a)) });
  });
  const end = F.asOf || A.daily[A.daily.length - 1][0], dd = (x, y) => A.daily.filter((q) => q[0] >= x && q[0] < y).map((q) => q[1]);
  const fa = medOf(dd(d0, d1)), fb = medOf(dd(d1, isoAdd(end, 1)));
  if (moved(fb, fa, true, F.B.f48 / 3)) rows.push({ key: 'days', kind: 'days', k: 'f48', title: 'A typical day', ids: nu, wo: old, a: fa, b: fb, dir: Math.sign(fb - fa), w: Math.sign(fa) !== Math.sign(fb) ? 2 : Math.abs(Math.log(Math.max(1, Math.abs(fb)) / Math.max(1, Math.abs(fa)))) });
  const g = {}, isNew = new Set(nu); W.forEach((i) => { (g[P[i].pat] = g[P[i].pat] || [[], []])[isNew.has(i) ? 1 : 0].push(i); });
  Object.entries(g).forEach(([name, [o, n]]) => {
    const mix = Math.abs(n.length - o.length) >= 5 && (Math.min(n.length, o.length) === 0 || Math.max(n.length, o.length) / Math.min(n.length, o.length) >= 2);
    const a = o.length >= 3 ? xF(F, o, 'lk') : null, b = n.length >= 3 ? xF(F, n, 'lk') : null, pay = moved(b, a) && Math.abs(Math.log(b / a)) >= Math.log(1.25);
    if (!mix && !pay) return;
    rows.push({ key: `slice:${name}`, kind: 'slice', k: 'lk', title: name, ids: byRank(n), wo: byRank(o), a, b, nn: n.length, no: o.length, mix, pay, dir: mix ? Math.sign(n.length - o.length) : Math.sign(b - a),
      w: Math.max(mix ? Math.abs(Math.log((n.length + 1) / (o.length + 1))) : 0, pay ? Math.abs(Math.log(b / a)) : 0) });
  });
  rows.forEach((r) => { r.d0 = d0; r.d1 = d1; });
  return rows.sort((p, q) => q.w - p.w).slice(0, 6);
}
/* Frequency: how often it posts, and what that does. A usual week is the median count of the full weeks on file (weeks
   we collected nothing for are left out); the rows are the busiest week the 100 spans, the week after it where the
   busiest ran well past the usual, the best and the worst of those weeks, and the busiest day where it ran at three
   usual days or more. A week's number is its typical post against the usual post */
function freqOf(F) {
  const P = A.posts, inW = new Set(F.W), WS = A.weeks.filter((w) => !F.asOf || w.ids.every((i) => P[i].d <= F.asOf));
  const FW = WS.filter((w) => !w.part && !w.gap && w.n), uw = medOf(FW.map((w) => w.n));
  const spanW = WS.filter((w) => !w.part && !w.gap && w.ids.some((i) => inW.has(i)));
  const wkRow = (w, key) => ({ key, title: `Week of ${w.s}`, week: w.s, ids: byRank(w.ids), n: w.n, g: w.g, x: xF(F, w.ids, 'lk') });
  const fq = [], peak = [...spanW].sort((a, b) => b.n - a.n)[0];
  if (peak) {
    fq.push(wkRow(peak, 'peak'));
    const nx = WS[WS.indexOf(peak) + 1];
    if (nx && !nx.part && !nx.gap && nx.n && peak.n >= uw * 1.6) fq.push(wkRow(nx, 'after'));
  }
  const byX = spanW.filter((w) => xF(F, w.ids, 'lk') != null).sort((a, b) => xF(F, b.ids, 'lk') - xF(F, a.ids, 'lk'));
  [['best', byX[0]], ['worst', byX[byX.length - 1]]].forEach(([k, w]) => { if (w && !fq.some((r) => r.week === w.s)) fq.push(wkRow(w, k)); });
  const dd = Object.entries(F.W.reduce((m, i) => ((m[P[i].d] = m[P[i].d] || []).push(i), m), {})).sort((a, b) => b[1].length - a[1].length)[0];
  if (dd && dd[1].length >= Math.max(4, (uw / 7) * 3)) fq.push({ key: 'day', title: `${WD[dow(dd[0])]} ${day(dd[0])}`, iso: dd[0], week: (A.weeks.find((w) => w.ids.includes(dd[1][0])) || {}).s, ids: byRank(dd[1]), n: dd[1].length, x: xF(F, dd[1], 'lk') });
  return { usual: uw, rows: fq };
}

function strategyBuild() {
  const P = A.posts, W = A.order.slice(-100);
  S.win = W; S.inW = new Set(W); S.L = {};
  const lw = A.weeks.find((w) => w.s === '19 Oct');
  S.wk = new Set(ofW(lw ? lw.ids : []));
  const y = P[W[W.length - 1]].d.slice(0, 4), [wd, wm] = (lw ? lw.s : '1 Jan').split(' '), w0 = `${y}-${String(MON.indexOf(wm) + 1).padStart(2, '0')}-${String(+wd).padStart(2, '0')}`;
  S.wkDays = new Set(Array.from({ length: 7 }, (_, j) => isoAdd(w0, j)));
  S.span = `${day(P[W[0]].d)} – ${day(P[W[W.length - 1]].d)}`;
  // this read's footing, and the last read's as it stood the day before this week began
  const F = footing(null), F1 = footing(isoAdd(w0, -1));
  S.F = F; S.F1 = F1; S.last = F1.asOf;
  S.L.slices = { rows: slicesOf(F), was: keyed(slicesOf(F1)) };
  Object.entries(LK).forEach(([lens, k]) => { S.L[lens] = { k, rows: moversOf(F, k), was: keyed(moversOf(F1, k)) }; });
  S.L.follows.fol = folRowsOf(F); S.L.follows.fol1 = folRowsOf(F1);
  S.L.streaks = { rows: runsOf(F), was: keyed(runsOf(F1), (r) => r.mk) };
  S.L.shifts = { rows: shiftsOf(F), was: keyed(shiftsOf(F1)) };
  const fq = freqOf(F); S.L.frequency = { usual: fq.usual, rows: fq.rows, was: keyed(freqOf(F1).rows) };
}
// a day's follows, and the posts behind them (that day's and the day before's)
function dayRow(d, kind) {
  const ids = byF48(S.win.filter((i) => A.posts[i].d === d[0] || A.posts[i].d === isoAdd(d[0], -1)));
  const nm = kind === 'best' ? 'best' : kind === 'worst' ? 'worst' : '';
  return { key: d[0], kind, g: d[1], title: `${sg(d[1])} on ${WD[dow(d[0])]} ${day(d[0])}`, sub: nm ? `The ${nm} day in the 100` : '', ids, metric: 'f48', k: 'f48', iso: d[0] };
}
/* ---- read cards: every number leads to the posts behind it. A card's top is its evidence (a cut of the post that says
   it best, its beats playing like a gif; or its stills; for a side, the posts with it over the posts without; for a
   run, the run and the post that ended it), its bottom the number and its line ---- */
const em_ = (s) => `<b>${esc(s)}</b>`;
// a picture shows once in Strategy where it can: each card takes the posts not yet shown first (used), then the rest
const fresh = (ids, used, n) => { const out = [...ids.filter((i) => !used.has(i)), ...ids.filter((i) => used.has(i))].slice(0, n); out.forEach((i) => used.add(i)); return out; };
const cutOf = (ids, used) => { const c = ids.filter((i) => (A.posts[i].rp || []).length >= 2), pick = (used && c.find((i) => !used.has(i))) || c[0] || null; if (pick && used) used.add(pick); return pick; };
function cutHTML(id, metric = 'xlk') {
  const p = A.posts[id], fr = p.rp, ats = fr.map((f) => f.at || '');
  return `<span class="cut" data-ats="${esc(ats.join('|'))}">${fr.map((f, k) => `<img class="${k ? '' : 'on'}" src="${BASE}${f.f}" alt="" loading="lazy" draggable="false">`).join('')}<span class="cbar">${fr.map(() => '<i><b></b></i>').join('')}</span>${ats[0] ? `<em class="crole">${esc(ats[0])}</em>` : ''}${badgeOf(p, metric)}</span>`;
}
const stillHTML = (id, metric) => `<span class="stl" data-cv="${id}" style="${coverStyle(id)}">${badgeOf(A.posts[id], metric)}</span>`;
function stillsHTML(ids, metric, used) { const n = ids.length >= 4 ? 4 : ids.length >= 2 ? 2 : 1, pick = used ? fresh(ids, used, n) : ids.slice(0, n); return `<span class="sts n${n}">${pick.map((id) => stillHTML(id, metric)).join('')}</span>`; }
/* every post a set holds, as one cover: the best of them large in the corner (playing, where it has frames to play), the
   rest packed around it best first, as many as there are (a few large; dozens small, a wall of the set). `aspect` is the
   box's width over its height, so the grid fills it; `k` is the count they're ranked and badged on */
// (the grid is fitted to the count, so it fills the box with no empty cell: the lead takes whatever block of cells is left
// over, and the cells stay close to the covers' own 4:5)
function mosaicFit(n, aspect) {
  if (n <= 1) return { c: 1, r: 1, sx: 1, sy: 1 };
  let best = null;
  for (let c = 1; c <= 14; c++) for (let r = 1; r <= 14; r++) {
    const A = c * r - (n - 1); if (A < 1 || A > 9 || A > c * r * .5 || (n >= 3 && r < 2)) continue;
    for (let sx = 1; sx <= Math.min(c, 4); sx++) {
      if (A % sx) continue; const sy = A / sx; if (sy > Math.min(r, 4)) continue;
      const cell = (aspect * r) / c, leadA = (cell * sx) / sy;
      // (a full card wants its best post large; a half-height strip is fine with even tiles)
      const score = Math.abs(Math.log(cell / .8)) + .6 * Math.abs(Math.log(leadA / .8)) + (n >= 5 && A < 4 && aspect < 1.5 ? .8 : 0);
      if (!best || score < best.score) best = { c, r, sx, sy, score };
    }
  }
  return best || { c: Math.ceil(Math.sqrt(n * aspect)), r: Math.ceil(n / Math.ceil(Math.sqrt(n * aspect))), sx: 1, sy: 1 };
}
function mosaicHTML(ids, k = 'lk', aspect = 1, play = true) {
  const P = A.posts, by = (i) => P[i][k];
  const L = [...ids].sort((a, b) => (by(b) ?? -1e9) - (by(a) ?? -1e9)), n = L.length; if (!n) return '';
  const badge = k === 'f48' || k === 'fd' ? k : 'x' + k, { c, r, sx, sy } = mosaicFit(n, aspect);
  const big = c <= 4, first = L[0], cut = play && (P[first].rp || []).length >= 2;
  const leadHTML = `<span class="mt ml" style="--sx:${sx};--sy:${sy};${cut ? '' : coverStyle(first)}"${cut ? '' : ` data-cv="${first}"`}>${cut ? cutHTML(first, badge) : badgeOf(P[first], badge)}</span>`;
  return `<span class="mos${big ? ' big' : ''}" style="--c:${c};--r:${r}">${leadHTML}${L.slice(1).map((id) => `<span class="mt" data-cv="${id}" style="${coverStyle(id)}">${big ? badgeOf(P[id], badge) : ''}</span>`).join('')}</span>`;
}
/* ---- the course: every week on file a column, every post in it a square, shaded by its count against the usual post
   (likes unless it says), so how often and how well read together, week by week. A set shows only its own posts; a week
   we collected nothing for stands as a gap, and the week a row is about is marked. `only` keeps to the weeks the set
   spans ---- */
// (under a long course its months; under a short one, eight weeks or fewer, every week)
const monthLabels = (ws, mark) => `<span class="crl" style="--n:${ws.length}">${ws.map((w) => { const [d, mo] = w.s.split(' '); return mark === w.s ? `<span class="on">${esc(w.s)}</span>` : `<span>${ws.length <= 8 ? esc(w.s) : +d <= 7 ? esc(mo) : ''}</span>`; }).join('')}</span>`;
function courseHTML(sel, mark, k = 'lk', only = false) {
  const has = new Set(sel), all = A.weeks.map((w) => ({ w, c: w.ids.filter((i) => has.has(i)) })), ws = only ? all.filter((x) => x.c.length) : all;
  const cols = ws.map((x) => x.c.sort((a, b) => (xPost(b, k) ?? -1) - (xPost(a, k) ?? -1)));
  const mx = Math.max(1, ...cols.map((c) => c.length));
  return `<span class="crs" style="--n:${ws.length};--g:${mx > 14 ? 0 : 1}px">${ws.map(({ w }, j) => { const c = cols[j]; return `<span class="cc${w.gap ? ' gap' : ''}${mark === w.s ? ' on' : ''}"><span class="cb" style="height:${c.length ? Math.max(5, Math.round((c.length / mx) * 100)) : 0}%">${c.map((i) => { const m = xPost(i, k); return `<i class="q${m == null ? 'n' : xshade(m)}"></i>`; }).join('')}</span></span>`; }).join('')}</span>
    ${monthLabels(ws.map((x) => x.w), mark)}
    <span class="crk"><span><i class="q6"></i>3× or more</span><span><i class="q3"></i>over the usual</span><span><i class="q1"></i>under it</span><span>${mx} in the busiest week</span></span>`;
}
// the slices' course: every post a square in its week, coloured by its slice (the four biggest in the 100 each a colour,
// the rest grey), so what it posts reads week by week
function mixCourseHTML(sel) {
  const P = A.posts, n = {}; S.win.forEach((i) => { n[P[i].pat] = (n[P[i].pat] || 0) + 1; });
  const big = Object.entries(n).sort((a, b) => b[1] - a[1]).slice(0, 4).map((x) => x[0]), cls = (i) => `m${big.indexOf(P[i].pat) + 1}`;
  const has = new Set(sel), ws = A.weeks, cols = ws.map((w) => w.ids.filter((i) => has.has(i)).sort((a, b) => big.indexOf(P[a].pat) - big.indexOf(P[b].pat))), mx = Math.max(1, ...cols.map((c) => c.length));
  return `<span class="crs mix" style="--n:${ws.length};--g:${mx > 14 ? 0 : 1}px">${ws.map((w, j) => `<span class="cc${w.gap ? ' gap' : ''}"><span class="cb" style="height:${cols[j].length ? Math.max(5, Math.round((cols[j].length / mx) * 100)) : 0}%">${cols[j].map((i) => `<i class="${cls(i)}"></i>`).join('')}</span></span>`).join('')}</span>
    ${monthLabels(ws)}
    <span class="crk">${big.map((s, j) => `<span><i class="m${j + 1}"></i>${esc(low(s))}</span>`).join('')}<span><i class="m0"></i>the rest</span></span>`;
}
// a count's course: each week's typical post as a bar over or under the usual post (on a log scale, a quarter to four
// times), or, for follows, each week's gain over or under zero. It starts with the first week that has the count (plays
// reach back only so far); a week after that with nothing to count stands empty
function barsHTML(vals0, count, k) {
  const f0 = Math.max(0, vals0.findIndex((v) => v != null)), ws = A.weeks.slice(f0), vals = vals0.slice(f0), mx = count ? Math.max(1, ...vals.map((v) => Math.abs(v ?? 0))) : 2;
  const h = (v) => (count ? Math.abs(v) / mx : Math.min(1, Math.abs(Math.log2(Math.max(v, .01))) / 2));
  const lw = (A.weeks.filter((w) => !w.part).slice(-1)[0] || {}).s, n = vals.filter((v) => v != null).length;
  return `<span class="wkb" style="--n:${ws.length}">${ws.map((w, j) => { const v = vals[j]; if (v == null || w.gap) return `<span class="wc nil${w.gap ? ' gap' : ''}"></span>`; const up = count ? v >= 0 : v >= 1; return `<span class="wc ${up ? 'up' : 'dn'}${w.part ? ' pt' : ''}${lw === w.s ? ' on' : ''}" title="${esc(`Week of ${w.s}: ${count ? sg(v) : fx(v)}`)}"><i style="--h:${Math.max(.03, h(v)).toFixed(3)}"></i></span>`; }).join('')}</span>
    ${monthLabels(ws, lw)}
    <span class="crk"><span><i class="q5"></i>${count ? 'followers gained' : `over ${A.his} usual`}</span><span><i class="q1"></i>${count ? 'lost' : 'under it'}</span>${f0 ? `<span>since ${esc(ws[0].s)}, the first week with ${KEYN[k]}</span>` : n < ws.length / 2 ? `<span>${n} of ${ws.length} weeks counted</span>` : ''}</span>`;
}
// a run of follower days as bars over and under zero, the newer days from `d1` on in crimson
function daysHTML(d0, d1) {
  const ds = A.daily.filter((x) => x[0] >= d0), mx = Math.max(1, ...ds.map((x) => Math.abs(x[1])));
  return `<span class="dys"><span class="dyb" style="--n:${ds.length}">${ds.map(([d, g]) => `<span class="${g >= 0 ? 'up' : 'dn'}${d >= d1 ? ' nw' : ''}"><i style="--h:${Math.max(.02, Math.abs(g) / mx).toFixed(3)}"></i></span>`).join('')}</span><em>${esc(day(d0))}</em><em class="r">${esc(day(ds[ds.length - 1][0]))}</em></span>`;
}
/* ---- a run as a trace, the way a heart monitor draws one: every post a beat at the hour it went up, so how often it
   posted is the spacing between beats; a beat rises for a post over the usual and dips for one under it, as far as its
   multiple on the run's own count (on a log scale, a sixth to six times). The run's beats are crimson, the post that
   ended it ringed. On a card it is the run and a few posts either side; in the panel every post of the 100, the weeks
   marked under it. Posts are the ones the runs are counted on: ranked, with the count ---- */
function ecgHTML(r, tight) {
  const P = A.posts, k = r.k || 'lk', all = S.win.filter((i) => (P[i].top != null || r.run.includes(i)) && P[i][k] != null && B[k] > 0);
  const a = all.indexOf(r.run[0]), b = all.indexOf(r.brk || r.run[r.run.length - 1]);
  const ids = tight ? all.slice(Math.max(0, a - 4), b + 5) : all; if (ids.length < 2) return '';
  const t = (i) => new Date(P[i].at).getTime(), t0 = t(ids[0]), sp = Math.max(1, t(ids[ids.length - 1]) - t0);
  const VW = 600, VH = 100, y0 = 50, amp = 46, hw = Math.max(1.2, Math.min(5, (VW / ids.length) * .32));
  const pts = ids.map((i) => { const L = Math.max(-2.5, Math.min(2.5, Math.log2(Math.max(P[i][k] / B[k], .01)))); return { i, x: 8 + ((t(i) - t0) / sp) * (VW - 16), y: y0 - (L / 2.5) * amp }; });
  const beats = (list) => list.map((p) => `L${(p.x - hw).toFixed(1)},${y0} L${p.x.toFixed(1)},${p.y.toFixed(1)} L${(p.x + hw).toFixed(1)},${y0}`).join(' ');
  const inRun = new Set(r.run), rp = pts.filter((p) => inRun.has(p.i)), bk = r.brk ? pts.find((p) => p.i === r.brk) : null;
  const pc = (x) => `${((x / VW) * 100).toFixed(2)}%`, yr = P[ids[0]].d.slice(0, 4);
  // (the weeks under it: every Monday the trace spans, every other one once there are more than nine)
  const mons = tight ? [] : A.weeks.map((w) => { const [d, mo] = w.s.split(' '); return { s: w.s, t: new Date(`${yr}-${String(MON.indexOf(mo) + 1).padStart(2, '0')}-${String(+d).padStart(2, '0')}T00:00:00`).getTime() }; }).filter((w) => w.t >= t0 && w.t <= t0 + sp);
  const step = mons.length > 9 ? 2 : 1, ticks = mons.filter((_, j) => j % step === 0).map((w) => `<span class="et" style="left:${pc(8 + ((w.t - t0) / sp) * (VW - 16))}">${esc(w.s)}</span>`).join('');
  return `<span class="ecg${tight ? ' tight' : ''}"><span class="ei"><svg viewBox="0 0 ${VW} ${VH}" preserveAspectRatio="none" aria-hidden="true"><line class="e0" x1="0" x2="${VW}" y1="${y0}" y2="${y0}"/><path class="e1" d="M0,${y0} ${beats(pts)} L${VW},${y0}"/>${rp.length ? `<path class="e2" d="M${(rp[0].x - hw).toFixed(1)},${y0} ${beats(rp)} L${(rp[rp.length - 1].x + hw).toFixed(1)},${y0}"/>` : ''}</svg>${bk ? `<i class="ek" style="left:${pc(bk.x)};top:${((bk.y / VH) * 100).toFixed(1)}%"></i>` : ''}</span>${ticks ? `<span class="ets">${ticks}</span>` : ''}</span>`;
}
const evidenceOf = (ids, metric, used) => { const c = metric ? null : cutOf(ids, used); return c ? cutHTML(c) : ids.length ? stillsHTML(ids, metric || 'xlk', used) : ''; };
const typical = (ids, key, used, n) => { const m = rankOf(ids, key); return fresh([...ids].filter((i) => A.posts[i][key] != null).sort((a, b) => Math.abs(A.posts[a][key] - m) - Math.abs(A.posts[b][key] - m)), used, n); };
function vsHTML(r) { const f4 = r.k === 'f48' || r.k === 'fd', mk = r.k === 'fd' ? 'fd' : 'f48', k = r.k && !f4 ? r.k : r.metric === 'ct' ? 'cm' : 'lk', [a, b] = r.lw || ['With it', 'Without'], xa = f4 ? null : xSet(r.ids, k), xo = f4 ? null : xSet(r.wo, k), u = (m) => (m == null ? '' : ` · ${fx(m)} usual`); return `<span class="vs"><span class="vsr">${mosaicHTML(r.ids, f4 ? mk : k, 2, false)}<em>${esc(a)} · ${r.ids.length}${u(xa)}</em></span><span class="vsr wo">${mosaicHTML(r.wo, f4 ? mk : k, 2, false)}<em>${esc(b)} · ${r.wo.length}${u(xo)}</em></span></span>`; }
function runHTML(r, used) {
  const run = r.run, big = r.brk || r.run[r.run.length - 1]; used.add(big);
  // (the run as its trace on top, a few posts either side of it; the post that ended it, or its latest, under it)
  return `<span class="run"><span class="rst">${ecgHTML(r, true) || run.map((id) => `<i data-cv="${id}" style="${coverStyle(id)}"></i>`).join('')}</span><span class="rbk">${(A.posts[big].rp || []).length >= 2 ? cutHTML(big, 'x' + r.k) : stillHTML(big, 'x' + r.k)}<em>${r.brk ? 'Ended it' : 'Latest'}</em></span></span>`;
}
// the old read files voiced the parts this one dissolved: a card falls back on their words where it is the same thing
// read the same way (a side on the count it was found on, a followers row in Follows)
function legacyKeys(lens, r) {
  if (lens === 'follows' && !['side', 'format', 'regular'].includes(r.kind)) return [`followers:${r.key}`];
  if (r.kind === 'side' && ((lens === 'comments' && r.sm === 'ct') || (lens === 'likes' && r.sm !== 'ct'))) return [`sides:${r.key}`];
  return [];
}
const voiceOf = (map, lens, r) => { if (!map) return null; for (const k of [`${lens}:${r.key}`, ...legacyKeys(lens, r)]) if (map[k]) return map[k]; return null; };
function readCards() {
  const P = A.posts, his = A.his, F = S.F, used = new Set();
  // the panel's headline for a row is its card's: the multiple, what it's of, and the line
  const head = (c) => ({ num: c.num, unit: `${c.unit} · ${c.kick.toLowerCase()}${c.count ? `, ${c.count}` : ''}`, line: c.line, aside: c.aside, tone: c.tone });
  const lastWk = (ids) => { const n = ids.filter((i) => S.wk.has(i)).length; return n ? `${n} from last week.` : 'None from last week.'; };
  const said = (id, k) => (k === 'fd' ? `${sg(Math.round(P[id].fd))} followers against ${his} pace` : k === 'f48' ? `${sg(P[id].f48)} followers in 48h` : `${fx(xPost(id, k))} ${his} usual ${KEYN[k]}`);
  const FOL = `followers against ${his} pace at the time`;

  // Act 1, slices: best typical likes first, each led by the count it stands out on, its four numbers under it
  const SL = S.L.slices;
  // (a slice of one post says nothing a post mortem doesn't)
  SL.rows = SL.rows.filter((r) => r.n >= 2);
  SL.rows.sort((a, b) => (b.f.lk ?? -1) - (a.f.lk ?? -1));
  SL.rows.forEach((r) => {
    const k = r.lead, f = r.f, b = [...r.ids].filter((i) => P[i][k] != null).sort((p, q) => P[q][k] - P[p][k])[0];
    r.card = { kick: r.title, count: `${r.n} posts`, num: fx(f[k]), unit: `${his} usual ${KEYN[k]}`, four: f, lead: k, line: b ? `Best: ${lab(b)}, ${said(b, k)}.` : '', aside: lastWk(r.ids), tone: tx(f[k]), media: mosaicHTML(r.ids, k) };
    const w = SL.was[r.key]; r.mark = markOf(w, f[k], w && w.f[k]);
    r.head = head(r.card);
  });
  const bs = SL.rows.find((g) => g.n >= 4 && g.f.lk != null) || SL.rows[0], bg = [...SL.rows].sort((x, z) => z.n - x.n)[0];
  SL.hl = bg !== bs && bg.f.lk ? `${esc(cap1(low(bs.title)))} gets ${em_(fx(bs.f.lk / bg.f.lk))} the likes of ${esc(low(bg.title))}, ${his} biggest slice.` : `${esc(cap1(low(bs.title)))} leads ${his} slices on likes.`;
  SL.sub = '';
  SL.course = mixCourseHTML(A.order);

  // Act 2: each count's movers, the posts with it over the posts without; the other counts said alongside
  const vsWhat = (r, k) => (r.kind === 'side' ? (r.slice ? `${low(r.slice)} without it` : 'posts without it') : `${his} other posts`);
  const ratioOn = (r, q) => { const min = q === 'pl' ? 3 : 4, n = (ids) => ids.filter((i) => P[i][q] != null).length; if (n(r.wiAll) < min || n(r.woAll) < min) return null; const a = xF(F, r.wiAll, q), o = xF(F, r.woAll, q); return a != null && o ? a / o : null; };
  Object.entries(LK).forEach(([lens, k]) => {
    const L = S.L[lens];
    L.rows.forEach((r) => {
      // (the other counts, two at most: likes and comments first, plays where one of those is the card's own)
      const other = ['lk', 'cm', 'pl'].filter((q) => q !== k).map((q) => { const m = ratioOn(r, q); return m == null ? null : `${KEYN[q]} ${fx(m)}`; }).filter(Boolean).slice(0, 2);
      const b = r.up ? r.ids[0] : r.ids[r.ids.length - 1];
      r.card = { kick: r.title, count: r.slice ? `in ${low(r.slice)}` : `${r.n} posts`, num: r.lin ? sg(Math.round(r.a)) : fx(r.x), unit: r.lin ? `${FOL}, typical · ${sg(Math.round(r.o))} without it` : `the ${KEYN[k]} of ${vsWhat(r, k)}`,
        line: b ? `${r.up ? 'Highest' : 'Lowest'}: ${lab(b)}, ${said(b, k)}.` : '', aside: other.length ? `${cap1(other.join(' · '))} with it.` : '', tone: r.up ? 'beating' : 'plain', media: vsHTML(r) };
      const w = L.was[r.key]; r.mark = markOf(w, r.v, w && w.v, r.lin || (w && w.lin));
      r.head = head(r.card);
    });
    // (plays reach back only a few weeks, so each reel with plays is a square of its own there; the other counts are
    // each week's typical post as a bar)
    L.course = k === 'pl' ? courseHTML(A.order.filter((i) => P[i].pl != null), null, 'pl', true) : barsHTML(A.weeks.map((wk) => (k === 'fd' ? wk.g : xSet(wk.ids, k))), k === 'fd', k);
    L.none = k === 'pl' ? `Plays are on file for ${A.order.filter((i) => P[i].pl != null).length} reels, too few to split by what they have.` : `Nothing in the 100 moves ${his} ${KEYN[k]} by 1.4× or more.`;
  });
  const hlOf = (lens) => {
    const L = S.L[lens], k = L.k, up = L.rows.filter((r) => r.up), dn = L.rows.filter((r) => !r.up), name = (r) => esc(low(r.title));
    if (!L.rows.length) return k === 'pl' ? `Only ${em_(`${A.order.filter((i) => P[i].pl != null).length} reels`)} have plays on file.` : `Nothing moves ${his} ${KEYN[k]} much.`;
    const say = (r) => em_(r.lin ? sg(Math.round(r.a)) : fx(r.x)), d = dn[dn.length - 1], pl = (r, v) => (r.kind === 'format' || /&/.test(r.title) ? v : `${v}s`);
    return [up.length ? `${esc(cap1(low(up[0].title)))} ${pl(up[0], 'lift')} ${his} ${KEYN[k]} most: ${say(up[0])}.` : '', d ? `${esc(cap1(low(d.title)))} ${pl(d, 'drag')} ${up.length ? 'them' : `${his} ${KEYN[k]}`} most: ${say(d)}.` : ''].filter(Boolean).join(' ');
  };
  ['views', 'likes', 'comments', 'follows'].forEach((lens) => { S.L[lens].hl = hlOf(lens); S.L[lens].sub = ''; });
  { const n = A.order.filter((i) => P[i].pl != null).length, a = P[A.order.find((i) => P[i].pl != null)]; S.L.views.sub = `Plays are on file for ${n} reels, all since ${a ? day(a.d) : '–'}. A usual reel’s plays come to ${Math.round((B.pl / A.fol.now) * 100)}% of ${his} follower count.`; }

  // follows: the movers, then the 48h after the best posts against the rest, then the best and the worst day
  const FL = S.L.follows, fo = FL.fol, fo1 = FL.fol1;
  if (fo.t10.length >= 3) {
    const tr = fo.b > 0 && fo.a > 0 ? fo.a / fo.b : null, tr1 = fo1.b > 0 && fo1.a > 0 ? fo1.a / fo1.b : null, r = { key: 't10', ids: fo.t10, metric: 'f48', k: 'f48', title: 'After a Top 10% post' };
    r.card = { kick: `After ${his} best posts`, count: `${fo.t10.length} posts`, num: tr != null ? fx(tr) : sg(Math.round(fo.a)), unit: tr != null ? 'the follows after the rest' : 'follows in the 48h after', line: `${sg(Math.round(fo.a))} in the 48 hours after, against ${sg(Math.round(fo.b))}.`, aside: '', tone: tr == null || tr >= 1 ? 'beating' : 'plain', media: mosaicHTML(fo.t10, 'f48') };
    r.mark = markOf(fo1.t10.length >= 3, tr ?? fo.a, tr1 ?? fo1.a, tr == null || tr1 == null); r.head = head(r.card); FL.rows.push(r);
  }
  [[fo.hi, 'best', fo1.hi], [fo.lo, 'worst', fo1.lo]].forEach(([d, kind, d1]) => {
    if (!d) return;
    const r = dayRow(d, kind), b = r.ids[0], xd = B.day > 0 && r.g > 0 ? r.g / B.day : null, xb = b ? xPost(b, 'lk') : null;
    if (b) used.add(b);
    r.card = { kick: `${WD[dow(r.iso)]} ${day(r.iso)}`, count: `${kind} day`, num: xd != null ? fx(xd) : sg(r.g), unit: xd != null ? 'a usual day’s follows' : 'followers that day', line: xd != null ? `${sg(r.g)} that day; a usual day, ${sg(Math.round(B.day))}.` : `A usual day: ${sg(Math.round(B.day))}.`, aside: b ? `Behind it: ${lab(b)}${xb != null ? `, ${fx(xb)} likes` : ''}.` : 'No post that day or the day before.', tone: r.g >= 0 ? 'beating' : 'plain', media: r.ids.length ? mosaicHTML(r.ids, 'f48') : `<span class="rce">${esc(day(r.iso))}</span>` };
    r.mark = d1 && d1[0] === d[0] ? { t: 'held' } : { t: 'new' }; r.head = head(r.card); FL.rows.push(r);
  });

  // Act 3, streaks: what the run was on, how far over or under it ran on each count, and what ended it
  const ST = S.L.streaks, kn = (ks) => listOf(ks.map((k) => KEYN[k]));
  ST.rows.forEach((r) => {
    const own = xSet(r.run, r.k), others = ['lk', 'cm', 'pl'].filter((k) => !r.ks.includes(k)).map((k) => { const m = xSet(r.run, k); return m == null ? null : `${KEYN[k]} ${fx(m)}`; }).filter(Boolean);
    r.card = { kick: dr(r.a, r.b), count: '', num: `${r.n}`, unit: `in a row ${r.up ? 'over' : 'under'} ${his} usual ${kn(r.ks)}`, line: `${fx(own)} over the run.${others.length ? ` ${cap1(others.join(', '))} alongside.` : ''}`, aside: r.brk ? `Ended by ${lab(r.brk)}.` : '', tone: r.up ? 'beating' : 'plain', media: runHTML(r, used) };
    const w = ST.was[r.mk]; r.mark = !w ? { t: 'new' } : !w.brk && r.brk ? { t: 'shifted', was: 'running' } : { t: 'held' };
    r.head = head(r.card);
    if (!r.brk) r.head.unit += ', still running';
  });
  const live = ST.rows.find((r) => !r.brk), nup = ST.rows.filter((r) => r.up).length;
  ST.hl = ST.rows.length ? `${em_(`${ST.rows.length} run${ST.rows.length === 1 ? '' : 's'}`)} of six or more in the 100: ${em_(`${nup} over`)} ${his} usual, ${em_(`${ST.rows.length - nup} under`)}.${live ? ` The one on ${esc(kn(live.ks))} is still running.` : ''}` : 'Nothing in the 100 ran six in a row over or under the usual.';
  ST.sub = `Counted against ${his} usual post, on likes and on comments.`;
  ST.course = ecgHTML(live || ST.rows[0] || { k: 'lk', run: [], brk: null }, false);

  // shifts: the newer half's number, the older half's beside it
  const SH = S.L.shifts;
  SH.rows.forEach((r) => {
    const since = day(r.d1), c = r.kind === 'all'
      ? { kick: r.title, count: KEYN[r.k], num: fx(r.b), unit: `${his} usual ${KEYN[r.k]}, since ${since}`, line: `The 50 posts before did ${fx(r.a)}.`, aside: '' }
      : r.kind === 'days'
        ? { kick: r.title, count: 'followers', num: sg(Math.round(r.b)), unit: `followers a day, since ${since}`, line: `${sg(Math.round(r.a))} a day before it.`, aside: '' }
        : r.mix
          ? { kick: r.title, count: 'posts', num: `${r.nn}`, unit: `of ${his} last 50 posts, since ${since}`, line: r.nn ? `${r.no} in the 50 before.` : `None since, after ${r.no} in the 50 before.`, aside: r.a != null && r.b != null ? `Likes ${fx(r.a)} before, ${fx(r.b)} since.` : '' }
          : { kick: r.title, count: 'likes', num: fx(r.b), unit: `${his} usual likes, since ${since}`, line: `${fx(r.a)} in the 50 posts before.`, aside: `${r.nn} posts since, ${r.no} before.` };
    r.lw = ['Since', 'Before']; r.card = { ...c, tone: r.dir > 0 ? 'beating' : 'plain', media: r.kind === 'days' ? daysHTML(r.d0, r.d1) : vsHTML(r) };
    const w = SH.was[r.key]; r.mark = !w ? { t: 'new' } : w.dir !== r.dir ? { t: 'shifted' } : { t: 'held' };
    r.head = head(r.card);
  });
  SH.hl = SH.rows.length ? `${em_(`${SH.rows.length} thing${SH.rows.length === 1 ? '' : 's'}`)} moved between the older half of the 100 and the newer.` : 'The newer half of the 100 reads like the older.';
  SH.sub = SH.rows.length ? `The newer half starts ${day(SH.rows[0].d1)}.` : '';
  SH.course = courseHTML(S.win, SH.rows.length ? (A.weeks.find((w) => w.ids.includes(S.win[S.win.length >> 1])) || {}).s : null, 'lk', true);

  // frequency: a week (or the day) with every post in it, led by how much it posted against a usual week (or a usual
  // day); how those posts did stays in the words and the panel, the rest of the page already counts posts
  const FQ = S.L.frequency, uday = FQ.usual / 7;
  FQ.rows.forEach((r) => {
    const day_ = r.key === 'day', m = r.n / (day_ ? uday : FQ.usual); r.fx = m;
    r.card = { kick: r.title, count: `${r.n} posts`, num: fx(m), unit: day_ ? `a usual day’s posts` : `${his} usual week’s posts`, line: day_ ? `${r.n} posts in a day; a usual day has ${uday.toFixed(1)}.` : `${r.n} posts; ${his} usual week has ${FQ.usual}.`, aside: r.g != null ? `${sg(r.g)} followers that week.` : '', tone: 'plain', media: mosaicHTML(r.ids, 'lk') };
    const w = FQ.was[r.key]; r.mark = w && (w.week === r.week && w.iso === r.iso) ? { t: 'held' } : { t: 'new' };
    r.head = head(r.card);
  });
  FQ.hl = `${esc(cap1(his))} usual week is ${em_(`${FQ.usual} posts`)}.`; FQ.sub = '';
  FQ.course = courseHTML(A.order);

  // Feeder Reader's own words, where the read file has them: a card's kick, count, line and aside, a part's headline
  // (its numbers marked **so**) and the line under it. The numbers on the cards stay counted
  const V = A.voice || {}, rich = (s) => esc(s).replace(/\*\*(.+?)\*\*/g, '<b>$1</b>');
  Object.entries(S.L).forEach(([lens, L]) => {
    L.rows.forEach((r) => {
      const v = voiceOf(V.cards, lens, r); if (!v || !r.card) return;
      ['kick', 'count', 'line', 'aside'].forEach((f) => { if (v[f] != null) r.card[f] = v[f]; });
      if (r.slice && v.kick && v.count == null && v.kick.toLowerCase().includes(r.slice.toLowerCase())) r.card.count = '';
      r.head = head(r.card); if (lens === 'streaks' && !r.brk) r.head.unit += ', still running';
    });
    const pv = (V.parts || {})[lens]; if (pv) { if (pv.hl) L.hl = rich(pv.hl); if (pv.sub != null) L.sub = pv.sub; }
  });
}
// a slice's four numbers under its lead: views, likes and comments against the usual post, follows as the card says them
function fourHTML(f, lead) {
  const cell = (k, nm) => {
    const v = f[k] == null ? '–' : k === 'fd' ? sg(Math.round(f[k])) : fx(f[k]), up = f[k] != null && (k === 'fd' ? f[k] > 0 : f[k] >= 1);
    return `<span class="${k === lead ? 'on' : ''}${up ? ' up' : ''}"><b>${v}</b><em>${k === 'pl' && f.pl == null ? 'no plays' : nm}</em></span>`;
  };
  return `<span class="rc4">${cell('pl', 'views')}${cell('lk', 'likes')}${cell('cm', 'comments')}${cell('fd', 'follows')}</span>`;
}
// (each card sits in a wrapper the reel snaps to: the card glides on scroll, its wrapper stays put)
function readCard(key, i, r) {
  const c = r.card, m = r.mark;
  const mk = m ? `<i class="mkc ${m.t}"><b>${MK[m.t]}</b>${m.was ? `<span>${m.was === 'running' ? 'ended since' : `was ${esc(m.was)}`}</span>` : ''}</i>` : '';
  return `<span class="rcw" style="--i:${Math.min(i, 4)}"><button class="rc" data-lens="${key}" data-row="${i}"><span class="rcm">${c.media}</span><span class="rct"><span class="rck"><b>${esc(c.kick)}</b>${c.count ? `<em>${esc(c.count)}</em>` : ''}</span><span class="rcnr"><span class="rcn${c.tone === 'beating' ? ' up' : ''}">${esc(c.num)}</span>${mk}</span><span class="rcu">${esc(c.unit)}</span>${c.four ? fourHTML(c.four, c.lead) : ''}<span class="rcl">${esc(c.line)}</span>${c.aside ? `<span class="rca">${esc(c.aside)}</span>` : ''}</span></button></span>`;
}
function lensSection(key) {
  const [, name, sub] = LENS.find((l) => l[0] === key), L = S.L[key];
  return `<article class="ls ls-${key}" id="${key}"><header class="lh"><span class="lk"><b>${name}</b><span>${sub}</span></span><p class="lhl">${L.hl}</p>${L.sub ? `<p class="lhs">${esc(L.sub)}</p>` : ''}${L.course ? `<div class="lcs">${L.course}</div>` : ''}
      ${L.rows.length > 1 ? `<span class="lnv"><button class="nb" data-rl="-1" aria-label="Back">${SV('M15 5l-7 7 7 7')}</button><button class="nb" data-rl="1" aria-label="More">${SV('M9 5l7 7-7 7')}</button></span>` : ''}</header>
    <div class="rl">${L.rows.length ? L.rows.map((r, i) => readCard(key, i, r)).join('') : `<p class="rl0">${esc(L.none || '')}</p>`}</div></article>`;
}
const actHTML = (a) => `<div class="act"><b>Act ${a}</b><span>${esc(ACTS[a])}</span></div>`;
/* ---- the cuts: one plays at a time, the one you're on (under the pointer, else the card nearest the middle of the
   screen once the page comes to rest), a frame every 1.7s with a slow drift, its bar filling as it goes; every other cut
   holds its first frame. Nothing plays while the panel is open ---- */
const HOLD = 1700;
let cutOn = null, cutT = 0, cutPin = null, cutIO = null, cutPick = 0;
const cutVis = new Set();
function cutFrame(c, k, run) {
  const im = c.querySelectorAll('img'), n = im.length; if (!n) return;
  k = ((k % n) + n) % n; c.dataset.k = k;
  im.forEach((x, m) => x.classList.toggle('on', m === k));
  c.querySelectorAll('.cbar i').forEach((x, m) => { x.classList.toggle('done', m < k); x.classList.toggle('run', run && m === k); });
  const lb = c.querySelector('.crole'), ats = (c.dataset.ats || '').split('|'); if (lb && ats[k]) lb.textContent = ats[k];
}
function cutStep() {
  clearTimeout(cutT);
  if (dead || !cutOn || cutOn.querySelectorAll('img').length < 2) return;
  cutT = setTimeout(() => { if (dead || !cutOn) return; cutFrame(cutOn, (+cutOn.dataset.k || 0) + 1, true); cutStep(); }, HOLD);
}
function playCut(c) {
  if (c === cutOn) return;
  clearTimeout(cutT);
  if (cutOn) { cutOn.classList.remove('play'); cutFrame(cutOn, 0, false); }
  cutOn = RM ? null : c || null;
  if (cutOn) { cutOn.classList.add('play'); cutFrame(cutOn, 0, true); cutStep(); }
}
function pickCut() {
  if (dead) return;
  if (PN.open || document.hidden) { playCut(null); return; }
  if (cutPin && cutPin.isConnected) { playCut(cutPin); return; }
  const mid = innerHeight / 2; let best = null, bs = Infinity;
  cutVis.forEach((c) => {
    const b = c.getBoundingClientRect(), rl = c.closest('.rl'); if (b.bottom < 80 || b.top > innerHeight - 80) return;
    const sc = Math.abs((b.top + b.bottom) / 2 - mid) + Math.max(0, b.left - (rl ? rl.getBoundingClientRect().left : 0)) * .6;
    if (sc < bs) { bs = sc; best = c; }
  });
  playCut(best);
}
// once scrolling stops, never during it
const pickSoon = () => { clearTimeout(cutPick); cutPick = setTimeout(pickCut, 240); };
function hookCuts() {
  cutVis.clear(); cutOn = null; cutPin = null; clearTimeout(cutT);
  cutIO = new IntersectionObserver((es) => { es.forEach((x) => (x.intersectionRatio >= .6 ? cutVis.add(x.target) : cutVis.delete(x.target))); pickSoon(); }, { threshold: [0, .6, 1] });
  root.querySelectorAll('.rl .cut').forEach((c) => cutIO.observe(c)); ios.push(cutIO);
}

/* ---- the story opens on what changed since the last read: this week's posts against the week before's, on each of
   the four (views, likes and comments the week's typical post against the usual post; follows the week's gain). It
   leads on the one that moved most ---- */
const fourSay = (r, v) => (v == null ? (r.k === 'pl' ? 'no plays' : '–') : r.k === 'f48' ? sg(v) : fx(v));
function sinceStory() {
  if (A.stories.some((s) => s.ch === 'Since')) return;
  const ws = A.weeks.filter((w) => !w.part), i = ws.findIndex((w) => w.s === '19 Oct'); if (i < 1) return;
  const now = ws[i], was = ws[i - 1], V = (A.voice || {}).since || {};
  const rows = [['pl', 'Views'], ['lk', 'Likes'], ['cm', 'Comments']].map(([k, nm]) => { const a = xSet(was.ids, k), b = xSet(now.ids, k); return { k, nm, a, b, s: a != null && b != null ? Math.abs(Math.log(b / a)) : -1 }; });
  const ga = was.g, gb = now.g;
  rows.push({ k: 'f48', nm: 'Follows', a: ga, b: gb, s: ga == null || gb == null ? -1 : Math.sign(ga) !== Math.sign(gb) ? 3 : Math.abs(Math.log(Math.max(1, Math.abs(gb)) / Math.max(1, Math.abs(ga)))) });
  const lead = [...rows].sort((p, q) => q.s - p.s)[0];
  A.stories.unshift({ ch: 'Since', spine: 'Since the last read', num: fourSay(lead, lead.b), unit: lead.k === 'f48' ? `followers this week · ${sg(lead.a)} the week before` : `${A.his} usual ${KEYN[lead.k]} this week · ${fx(lead.a)} the week before`,
    line: V.line || `${lead.nm} moved most since the last read.`, why: V.why || '', media: { kind: 'four', rows, lead: lead.k } });
}
/* ---- the live lane: the posts under a week old, newest first, each with its day-3 call. The call is its likes at day 3
   against the usual post's likes at day 3 (the usual post's day-7 likes times the share a post here has by day 3), so
   it reads as where it is headed by day 7. Before day 3 it says when the call comes ---- */
const today = () => A.daily[A.daily.length - 1][0];
const liveIds = () => [...A.order].reverse().filter((i) => A.posts[i].d > isoAdd(today(), -7));
function callOf(id) {
  const p = A.posts[id], c = (A.curve || {}).d3 || {};
  if (p.cp !== 'd3' || p.lk == null || !(B.lk > 0) || !c.lk) return null;
  return { x: p.lk / (B.lk * c.lk), xc: p.cm != null && B.cm > 0 && c.cm ? p.cm / (B.cm * c.cm) : null };
}
function liveTile(id) {
  const p = A.posts[id], c = callOf(id), h = c ? xshade(c.x) : 0;
  return `<button class="lvt" data-post="${id}" data-live="1"><span class="cv" data-cv="${id}" style="${coverStyle(id)}"><span class="dt">${day(p.d)}</span>${c ? `<span class="bdg${h >= 5 ? ' w' : ''}" style="background:var(--s${h})">${fx(c.x)}</span>` : ''}</span>
    <span class="lvc">${c ? `<b>Day-3 call</b><em>${c.x >= 1 ? 'over' : 'under'} ${A.his} usual${c.xc != null ? ` · comments ${fx(c.xc)}` : ''}</em>` : `<b>Call on ${day(isoAdd(p.d, 3))}</b><em>${p.cp === 'd1' ? 'day 1 counted' : 'not counted yet'}</em>`}</span></button>`;
}
function liveHTML() {
  const ids = liveIds(); if (!ids.length) return '';
  const n = ids.filter((i) => callOf(i)).length, up = ids.filter((i) => callOf(i) && callOf(i).x >= 1).length;
  return `<div class="live"><div class="lvh"><span class="lvk"><i></i>Live</span><b>${ids.length} posts under a week old</b><span>${n ? `${up} of the ${n} called so far are headed over ${A.his} usual likes.` : 'None has reached day 3 yet.'}</span></div><div class="lvr">${ids.map(liveTile).join('')}</div></div>`;
}

/* =====================================================================
   SPLIT: the wall, newest first or in rank groups; a pick lights its trail
   ===================================================================== */
let WV = localGet('wall') === 'ranks' ? 'ranks' : 'recent', WPICK = null, TRT = [];
const RANKG = [
  { l: 'Top 10%', f: (v) => v != null && v <= 10, c: 'hot' }, { l: 'Top 25%', f: (v) => v != null && v > 10 && v <= 25, c: 'hot' }, { l: 'Top half', f: (v) => v != null && v > 25 && v <= 50, c: '' },
  { l: 'Below the middle', f: (v) => v != null && v > 50 && v <= 75, c: '' }, { l: 'Bottom 25%', f: (v) => v != null && v > 75, c: '' }, { l: 'Too new to rank', f: (v) => v == null, c: 'new' },
];
function splitHTML() {
  const L = A.lead.split, V = (A.voice && A.voice.split) || {}, rk = A.wall.filter((i) => A.posts[i].top != null), nrank = rk.length, up = rk.filter((i) => xPost(i, 'lk') >= 1).length;
  const f = { num: `${up} of ${nrank}`, unit: `beat ${A.his} usual likes by day 7`, line: V.line || `The best was ${lab(L.best)}, at ${fx(xPost(L.best, 'lk'))} ${A.his} usual likes; the lowest, ${lab(L.worst)}, at ${fx(xPost(L.worst, 'lk'))}.`, aside: V.aside != null ? V.aside : A.wall.length > nrank ? `${A.wall.length - nrank} are too new to count.` : '', tone: up / Math.max(1, nrank) >= .5 ? 'beating' : 'plain' };
  return `<header class="op"><span class="ok"><b>03</b>Split · the last ${A.wall.length} posts</span></header><div class="cap capx"><div class="w1"><div class="c">${headHTML(f)}</div></div><div class="w2"><div class="c">${bodyHTML(f)}</div></div></div>
    ${liveHTML()}<div class="wbar">${segHTML('wv', [['recent', 'Recent'], ['ranks', 'Ranks']], WV)}<span class="wh" id="wh">${WV === 'recent' ? 'Newest first. Pick a post to see what it beat.' : 'Grouped by where each landed.'}</span></div>
    <div class="wall" id="wall"><svg class="tlx" id="tlx"></svg><span class="thd" id="thd"></span><div id="wg"></div></div>`;
}
function wallTile(id) {
  const p = A.posts[id], v = p.top, pl = v == null ? 'var(--s0)' : v <= 10 ? 'var(--s5)' : v <= 25 ? 'var(--s4)' : v <= 50 ? 'var(--s3)' : 'var(--s1)';
  return `<button class="wt" data-post="${id}" data-wall="1"><span class="plt" style="background:${pl}"></span><span class="cv" data-cv="${id}" style="${coverStyle(id)}"></span><span class="dt">${day(p.d)}</span>${badgeOf(p)}<span class="mk"></span><span class="rgn"></span></button>`;
}
function likesRange(ids) { const v = ids.map((i) => A.posts[i].lk).filter((x) => x != null); return v.length ? ` · ${K(Math.min(...v))}${v.length > 1 ? `–${K(Math.max(...v))}` : ''} likes` : ''; }
function writeUp(ids, g, said) {
  if (g.c === 'new') return `${ids.length === 1 ? 'Up' : 'All up'} in the last few days. <span>They get a rank at day 7.</span>`;
  const cnt = {}; ids.forEach((i) => { cnt[A.posts[i].pat] = (cnt[A.posts[i].pat] || 0) + 1; });
  const tops = Object.entries(cnt).sort((a, b) => b[1] - a[1]);
  let s = ids.length === 1 ? `One post: ${lab(ids[0])}.` : tops[0][1] === 1 ? `${ids.length} posts, ${ids.length} different slices.` : `Mostly ${tops.slice(0, 2).filter((x, i) => i === 0 || x[1] > 1).map(([k, v]) => `${low(k)} (${v})`).join(' and ')}.`;
  const wallCnt = {}; A.wall.forEach((i) => { if (A.posts[i].top != null) wallCnt[A.posts[i].pat] = (wallCnt[A.posts[i].pat] || 0) + 1; });
  const miss = Object.entries(wallCnt).filter(([k, v]) => v >= 3 && !cnt[k] && !said.has(k)).sort((a, b) => b[1] - a[1])[0];
  if (miss) { said.add(miss[0]); s += ` <span>None of the ${miss[1]} ${low(miss[0])} posts.</span>`; }
  return s;
}
function drawWall() {
  const wg = $('wg'); if (!wg) return; clearTrail();
  if (WV === 'recent') { wg.className = 'wgrid'; wg.innerHTML = A.wall.map(wallTile).join(''); }
  else { const said = new Set(); wg.className = ''; wg.innerHTML = RANKG.map((g) => { const ids = byRank(A.wall.filter((i) => g.f(A.posts[i].top))); if (!ids.length) return ''; return `<section class="rgp ${g.c}"><i class="rgl"></i><div class="rh"><b class="rl"><span>${g.l}</span></b><span class="rm">${ids.length} post${ids.length > 1 ? 's' : ''}${likesRange(ids)}</span></div><p class="rw">${writeUp(ids, g, said)}</p><div class="wgrid">${ids.map(wallTile).join('')}</div></section>`; }).join(''); }
  if (WPICK) { wg.querySelector(`.wt[data-post="${WPICK}"]`)?.classList.add('sel'); if (WV === 'recent') playTrail(WPICK, true); }
}
/* ---- Recent ⇄ Ranks: the wall morphs rather than redraws. Every post's tile flies from where it stood to where it
   stands now (moving and resizing as one, the top of each band leading), and the bands' words arrive as the tiles land:
   its divider draws across, its name rises out of a mask, its count and its line lift in after. Going back, the words
   step out of the way first. Tiles far off screen both before and after just take their place; one that would cross more
   than a screen comes in from close by instead of flying the whole way ---- */
function morphWall() {
  const wg = $('wg'), wall = $('wall'); if (!wg || !wall || RM) { drawWall(); return; }
  const H = innerHeight, on = (r) => r.bottom > -60 && r.top < H + 60, wr = wall.getBoundingClientRect();
  const was = new Map(); wg.querySelectorAll('.wt').forEach((t) => was.set(t.dataset.post, t.getBoundingClientRect()));
  // the words of the bands now on screen leave as ghosts, a step up and out, while the tiles fly beneath them
  const ghosts = [...wg.querySelectorAll('.rgp')].flatMap((sec) => [...sec.querySelectorAll(':scope > .rh, :scope > .rw')].map((e_) => ({ e_, c: sec.className, r: e_.getBoundingClientRect() }))).filter((g) => on(g.r));
  drawWall();
  ghosts.forEach(({ e_, c, r }, j) => {
    const box = document.createElement('section'); box.className = `${c} rgh`; box.setAttribute('aria-hidden', 'true');
    Object.assign(box.style, { left: `${r.left - wr.left}px`, top: `${r.top - wr.top}px`, width: `${r.width}px` }); box.appendChild(e_.cloneNode(true)); wall.appendChild(box);
    box.animate([{ opacity: 1, transform: 'none' }, { opacity: 0, transform: 'translate3d(0,-16px,0)' }], { duration: 420, delay: j * 40, easing: 'cubic-bezier(.45, 0, .2, 1)', fill: 'forwards' }).onfinish = () => box.remove();
  });
  const D = 1250;
  [...wg.querySelectorAll('.wt')].forEach((t, i) => {
    const a = was.get(t.dataset.post), b = t.getBoundingClientRect(); if (!a) return;
    if (!on(a) && !on(b)) return;
    const delay = 60 + Math.min(i * 14, 460);
    if (Math.abs(a.top - b.top) > H * .9 || !on(a)) {
      t.animate([{ opacity: 0, transform: `translate3d(0,${a.top > b.top ? 70 : -70}px,0) scale(.9)` }, { opacity: 1, transform: 'none' }], { duration: D - 150, delay: delay + 160, easing: BEASE, fill: 'backwards' });
      return;
    }
    const dx = a.left - b.left, dy = a.top - b.top, sx = a.width / b.width, sy = a.height / b.height;
    if (Math.abs(dx) < .5 && Math.abs(dy) < .5 && Math.abs(sx - 1) < .01) return;
    t.style.zIndex = 3;
    t.animate([{ transformOrigin: '0 0', transform: `translate3d(${dx}px,${dy}px,0) scale(${sx},${sy})` }, { transformOrigin: '0 0', transform: 'none' }], { duration: D, delay, easing: GROW, fill: 'backwards' }).onfinish = () => { t.style.zIndex = ''; };
  });
  // the bands' words come in as their tiles land, a band at a time
  [...wg.querySelectorAll('.rgp')].forEach((sec, g) => {
    if (!on(sec.getBoundingClientRect())) return;
    const t0 = 520 + g * 150, o = (dl, dur = 900) => ({ duration: dur, delay: t0 + dl, easing: BEASE, fill: 'backwards' });
    const ln = sec.querySelector('.rgl'), nm = sec.querySelector('.rl > span'), rm = sec.querySelector('.rm'), rw = sec.querySelector('.rw');
    if (ln) ln.animate([{ transform: 'scaleX(0)' }, { transform: 'none' }], o(0, 1200));
    if (nm) nm.animate([{ transform: 'translate3d(0,120%,0)' }, { transform: 'none' }], o(90, 1000));
    if (rm) rm.animate([{ opacity: 0, transform: 'translate3d(-12px,0,0)' }, { opacity: 1, transform: 'none' }], o(280));
    if (rw) rw.animate([{ opacity: 0, transform: 'translate3d(0,16px,0)' }, { opacity: 1, transform: 'none' }], o(380));
  });
}
function beatOf(id, pool) {
  const ch = pool.filter((i) => A.posts[i].top != null), i = ch.indexOf(id), v = A.posts[id].top;
  if (v == null) return { t: 'Too new to rank yet', chain: [] };
  if (i <= 0) return { t: 'The oldest one here', chain: [] };
  const better = (a, b) => { const x = A.posts[a], y = A.posts[b]; return x.top < y.top || (x.top === y.top && (x.lk ?? 0) > (y.lk ?? 0)); };
  const up = better(id, ch[i - 1]), chain = []; let end = null;
  for (let k = i - 1; k >= 0; k--) { if (up ? better(id, ch[k]) : better(ch[k], id)) chain.push(ch[k]); else { end = ch[k]; break; } }
  const n = chain.length;
  return { up, n, chain, end, all: up && n === i, t: up ? (n === i ? `Beat all ${n} before it` : `Beat the ${n} before it`) : n === 1 ? 'The one before it did better' : `The ${n} before it did better` };
}
function clearTrail() {
  TRT.forEach(clearTimeout); TRT = [];
  const wall = $('wall'); if (!wall) return;
  wall.classList.remove('trail'); wall.querySelectorAll('.wt').forEach((t) => { t.classList.remove('beat', 'above', 'stop'); t.querySelector('.fl')?.remove(); });
  const svg = $('tlx'); if (svg) svg.innerHTML = ''; $('thd')?.classList.remove('on');
}
function playTrail(id, instant) {
  clearTrail();
  const wall = $('wall'), r = beatOf(id, [...A.wall].reverse()); if (!r.chain.length && !r.end) return r;
  const T = (q) => wall.querySelector(`.wt[data-post="${q}"]`), svg = $('tlx'), head = $('thd'), box = wall.getBoundingClientRect();
  wall.classList.add('trail');
  const pos = (el) => { const b = el.getBoundingClientRect(); return { x: b.left - box.left + b.width / 2, y: b.bottom - box.top + 6 }; };
  svg.setAttribute('viewBox', `0 0 ${wall.clientWidth} ${wall.clientHeight}`);
  const dt = instant || RM ? 0 : Math.max(60, Math.min(140, 1500 / Math.max(1, r.chain.length)));
  let prev = pos(T(id));
  head.classList.toggle('dn', !r.up); head.style.setProperty('--dt', '0ms');
  const place = (c) => { head.style.left = Math.max(head.offsetWidth / 2 + 2, Math.min(wall.clientWidth - head.offsetWidth / 2 - 2, c.x)) + 'px'; head.style.top = c.y + 'px'; };
  head.textContent = r.up ? 'beat 0' : '0 did better'; place(prev); void head.offsetWidth; head.style.setProperty('--dt', dt * 1.4 + 'ms'); head.classList.add('on');
  const line = (a, b) => { if (Math.abs(a.y - b.y) > 4) return; const ln = document.createElementNS('http://www.w3.org/2000/svg', 'line'); ln.setAttribute('x1', a.x); ln.setAttribute('x2', b.x); ln.setAttribute('y1', a.y); ln.setAttribute('y2', b.y); if (!r.up) ln.setAttribute('class', 'dn'); svg.appendChild(ln); if (dt) { ln.style.transformBox = 'fill-box'; ln.style.transformOrigin = `${b.x < a.x ? '100%' : '0%'} 50%`; ln.animate([{ transform: 'scaleX(0)' }, { transform: 'none' }], { duration: dt * 1.3, easing: 'ease-out' }); } };
  const at_ = (ms, fn) => (dt ? TRT.push(setTimeout(fn, ms)) : fn());
  r.chain.forEach((q, k) => at_(150 + k * dt, () => { const t = T(q); if (!t) return; const c = pos(t); t.classList.add(r.up ? 'beat' : 'above'); t.querySelector('.mk').textContent = k + 1; line(prev, c); head.textContent = r.up ? `beat ${k + 1}` : `${k + 1} did better`; place(c); prev = c; }));
  at_(150 + r.chain.length * dt + 60, () => { if (r.end) { const t = T(r.end); if (t) { const c = pos(t); line(prev, c); t.classList.add('stop'); t.insertAdjacentHTML('beforeend', `<span class="fl">${r.up ? `${top(A.posts[r.end].top)} did better` : 'It beat this'}</span>`); prev = c; } } head.textContent = r.up ? (r.all ? `beat all ${r.n}` : `beat the last ${r.n}`) : `last ${r.n} did better`; place(prev); });
  return r;
}
function wallPick(id) {
  const wall = $('wall');
  if (WPICK === id) { openPost(id, A.wall); return; }
  WPICK = id; wall.querySelectorAll('.wt.sel').forEach((t) => t.classList.remove('sel')); wall.querySelector(`.wt[data-post="${id}"]`)?.classList.add('sel');
  const r = WV === 'recent' ? playTrail(id) : (clearTrail(), beatOf(id, [...A.wall].reverse()));
  const p = A.posts[id], ro = $('ro');
  ro.innerHTML = `<span class="c" style="${coverStyle(id)}"></span><span class="t"><b>${esc(cap1(lab(id)))}</b><span><em>${xPost(id, 'lk') != null ? `${fx(xPost(id, 'lk'))} likes` : 'New'}</em>${p.top != null ? ` · ${top(p.top)}` : ''} · ${esc(r ? r.t.toLowerCase() : '')}${r && r.chain && r.chain.length ? ' on the wall' : ''}</span></span><button class="o" data-open="${id}">Open</button><button class="x" data-unpick aria-label="Clear">${ICX}</button>`;
  ro.getAnimations().forEach((a) => a.cancel());
  const pop = isDesk(); ro.classList.toggle('pop', pop); ro.classList.add('on');
  if (!pop) return;
  // (a laptop: the card stands beside the post, and grows out of the side facing it)
  placeRo();
  if (!RM) ro.animate([{ opacity: 0, transform: `translate3d(${ro.dataset.side === 'r' ? -12 : 12}px,0,0) scale(.94)` }, { opacity: 1, transform: 'none' }], { duration: 380, easing: BEASE });
}
// the laptop card's place: beside the picked post (right of it, or left where the right has no room), level with its top
// and kept on screen; it follows the post as the page scrolls
function placeRo() {
  const ro = $('ro'); if (!WPICK || !ro.classList.contains('pop')) return;
  const t = q1(`#wall .wt[data-post="${WPICK}"]`); if (!t) return;
  const b = t.getBoundingClientRect(), W = ro.offsetWidth, H = ro.offsetHeight, gap = 14, right = b.right + gap + W <= innerWidth - 16;
  ro.dataset.side = right ? 'r' : 'l';
  ro.style.left = `${right ? b.right + gap : Math.max(16, b.left - gap - W)}px`;
  ro.style.top = `${Math.max(16, Math.min(innerHeight - H - 16, b.top))}px`;
  ro.style.transformOrigin = right ? '0% 24px' : '100% 24px';
  ro.classList.toggle('off', b.bottom < 40 || b.top > innerHeight - 40);
}
let roRaf = 0;
const roFollow = () => { if (!WPICK || roRaf) return; roRaf = requestAnimationFrame(() => { roRaf = 0; placeRo(); }); };
function unpick() {
  WPICK = null; clearTrail(); qa('#wall .wt.sel').forEach((t) => t.classList.remove('sel'));
  const ro = $('ro');
  if (!ro.classList.contains('on')) return;
  if (ro.classList.contains('pop') && !RM) {
    ro.getAnimations().forEach((a) => a.cancel());
    ro.animate([{ opacity: 1, transform: 'none' }, { opacity: 0, transform: 'scale(.96)' }], { duration: 180, easing: 'ease-in', fill: 'forwards' }).onfinish = (e) => { if (WPICK) return; ro.classList.remove('on'); e.target.cancel(); };
  } else ro.classList.remove('on');
}
/* =====================================================================
   THE PANEL: a slide's posts, or a post's mortem; it steps through its list and keeps what's behind it in step
   ===================================================================== */
const PN = { open: false, view: null, back: null };
function openRow(lens, i) { PN.back = null; PN.view = { kind: 'row', lens, i }; showPanel(); }
function openPost(pid, list) { const back = PN.open && PN.view && PN.view.kind !== 'post' ? PN.view : null; PN.back = back; PN.view = { kind: 'post', id: pid, list: list && list.includes(pid) ? list : [pid] }; showPanel(); }
function showPanel() {
  const was = PN.open; renderPanel(0); PN.open = true; $('pn').classList.add('on'); $('pns').classList.add('on'); markRow(); pickCut();
  // (opening, its contents rise in behind the panel, a piece at a time)
  if (was || RM) return;
  const v = $('pnb').querySelector('.pnv'); if (!v) return;
  [...v.children].flatMap((e_) => (e_.matches('.pv-t, .pm-hd') ? [...e_.children] : [e_])).slice(0, 9).forEach((e_, j) => e_.animate([{ opacity: 0, transform: 'translate3d(0,22px,0)' }, { opacity: 1, transform: 'none' }], { duration: 620, delay: 140 + j * 60, easing: BEASE, fill: 'backwards' }));
}
function closePanel() { PN.open = false; PN.back = null; const pn = $('pn'); pn.classList.remove('on'); pn.style.transform = ''; $('pns').classList.remove('on'); markRow(); pickSoon(); }
// the row the panel is on (or came from) stays marked on the page
const LNAME = Object.fromEntries(LENS.map((l) => [l[0], l[1]]));
function markRow() {
  qa('[data-lens].on').forEach((b) => b.classList.remove('on'));
  const v = !PN.open ? null : PN.view && PN.view.kind === 'row' ? PN.view : PN.back;
  if (v) qa(`[data-lens="${v.lens}"][data-row="${v.i}"]`).forEach((b) => b.classList.add('on'));
}
function stepPanel(d) {
  const v = PN.view;
  if (v.kind === 'post') { const i = v.list.indexOf(v.id), j = i + d; if (j < 0 || j >= v.list.length) return; v.id = v.list[j]; renderPanel(d); return; }
  const j = v.i + d; if (j < 0 || j >= S.L[v.lens].rows.length) return; v.i = j; renderPanel(d); markRow();
}
function renderPanel(dir) {
  const v = PN.view, bk = $('pnbk');
  bk.classList.toggle('on', !!PN.back && v.kind === 'post');
  if (PN.back) bk.innerHTML = `${ICL}<span>${esc(LNAME[PN.back.lens])}</span>`;
  const rows = v.kind === 'row' ? S.L[v.lens].rows : null, n = rows ? rows.length : v.list.length, i = rows ? v.i : v.list.indexOf(v.id);
  $('pnp').disabled = i <= 0; $('pnn').disabled = i >= n - 1;
  $('pnk').textContent = rows ? `${LNAME[v.lens]} · ${i + 1} of ${n}` : postKicker(v.id);
  const html = rows ? rowView(v.lens, v.i) : postView(v.id);
  const body = $('pnb'), nv = document.createElement('div'); nv.className = 'pnv'; nv.innerHTML = html;
  const ov = body.querySelector('.pnv');
  if (!ov || RM || !dir) { body.replaceChildren(nv); body.scrollTop = 0; return; }
  // a page sliding across: the last view goes off the side it came from, the next arrives from the other, as one move
  const W = body.clientWidth, top0 = parseFloat(getComputedStyle(body).paddingTop) - body.scrollTop;
  Object.assign(ov.style, { position: 'absolute', left: getComputedStyle(body).paddingLeft, right: getComputedStyle(body).paddingRight, top: top0 + 'px' });
  body.style.position = 'relative'; body.appendChild(nv); body.scrollTop = 0;
  ov.animate([{ transform: 'none' }, { transform: `translateX(${-dir * W}px)` }], { duration: 420, easing: CARRY, fill: 'forwards' }).onfinish = () => ov.remove();
  nv.animate([{ transform: `translateX(${dir * W}px)` }, { transform: 'none' }], { duration: 420, easing: CARRY });
}
const gridHTML = (ids, g = {}) => `<div class="pgrid">${ids.map((id) => tileHTML(id, g)).join('')}</div>`;
// what a post is, in a line: its summary, else its own caption (first line, tags off), else its type
function saidOf(p) {
  if (p.sum) return p.sum;
  const c = (p.cap || '').split('\n')[0].replace(/#[\w.]+/g, '').replace(/(\s*@[\w.]+)+\s*$/, '').replace(/\s{2,}/g, ' ').trim();
  if (c) return `“${c.length > 150 ? `${c.slice(0, c.lastIndexOf(' ', 146))}…` : c}”`;
  return p.desc || '';
}
// a post as a line of the panel: its cover, what it got, when, and what happens in it
function postRowHTML(id, xk, tag) {
  const p = A.posts[id], fol = xk === 'f48' || xk === 'fd', v = fol ? null : xPost(id, xk.slice(1));
  const m = xk === 'fd' ? (p.fd != null ? `${sg(Math.round(p.fd))} followers against ${A.his} pace` : 'Followers not counted') : xk === 'f48' ? (p.f48 != null ? `${sg(p.f48)} followers in 48h` : 'Followers not counted') : v == null ? (p.top == null && p.d >= isoAdd(A.posts[A.order[A.order.length - 1]].d, -7) ? 'Too new to count' : xk === 'xpl' ? 'No plays on file' : 'Not counted') : `${fx(v)} ${KEYN[xk.slice(1)]}`;
  const up = fol ? p[xk] > 0 : v != null && v >= 1;
  return `<button class="ppr${tag ? ' mk' : ''}" data-post="${id}"><span class="ppc" data-cv="${id}" style="${coverStyle(id)}"></span><span class="ppt"><span class="pph"><b${up ? ' class="up"' : ''}>${esc(m)}</b><em>${day(p.d)} · ${cap1(FMT1[p.fmt] || 'post')}${tag ? ` · ${esc(tag)}` : ''}</em></span><span class="ppd">${esc(saidOf(p))}</span></span></button>`;
}
// a set of posts, each said in a line: all of them up to eight; past that the four best and the three worst in words,
// and the rest as covers (no post shows twice)
function postListHTML(label, sub, ids, xk, tags = {}) {
  if (!ids.length) return '';
  const counted = xk === 'f48' || xk === 'fd' ? ids.filter((x) => A.posts[x][xk] != null) : ids.filter((x) => xPost(x, xk.slice(1)) != null);
  const many = counted.length > 8, best = many ? counted.slice(0, 4) : counted, worst = many ? counted.slice(-3) : [], words = [...best, ...worst], rest = ids.filter((x) => !words.includes(x));
  const lines = (list) => list.map((id) => postRowHTML(id, xk, tags[id])).join('');
  return `<section class="pps"><h5>${esc(label)}<em>${esc(sub)}</em></h5>
    <div class="ppl">${lines(best)}${many ? `<p class="ppg">${counted.length - 7} more between them</p>${lines(worst)}` : ''}</div>
    ${rest.length ? `<p class="pl2">The other ${rest.length}</p>${gridHTML(rest, { metric: xk })}` : ''}</section>`;
}
function rowView(lens, i) {
  const r = S.L[lens].rows[i], P = A.posts, f48 = r.k === 'f48' || r.k === 'fd' || r.metric === 'f48', k = r.k && !f48 ? r.k : r.metric === 'ct' ? 'cm' : 'lk', xk = f48 ? (r.k === 'fd' ? 'fd' : 'f48') : 'x' + k;
  const most = (ids, kk = k) => [...ids].sort((a, b) => (P[b][kk] ?? -1e9) - (P[a][kk] ?? -1e9)), byF = (ids) => (r.k === 'fd' ? most(ids, 'fd') : byF48(ids));
  const ids = r.run ? r.ids : f48 ? byF(r.ids) : most(r.ids);
  const lw = ids.filter((x) => S.wk.has(x)).length, fol = medOf(ids.map((x) => P[x].f48)), xl = xSet(ids, 'lk'), xc = xSet(ids, 'cm'), xp = ids.filter((x) => P[x].pl != null).length >= 3 ? xSet(ids, 'pl') : null, h = { ...r.head };
  // the posts below name the best one, the one that ended a run and the one behind a day, so the head doesn't
  if (/^(Best:|Highest:|Lowest:|Ended by|Behind it:)/.test(h.aside || '')) h.aside = '';
  const said = [h.num, h.unit, h.line, h.aside].join(' ').toLowerCase(), has = (w) => said.includes(w);
  const stats = [
    xp != null && !has('plays') ? [fx(xp), 'usual plays'] : null,
    xl != null && !has('likes') ? [fx(xl), `${A.his} usual likes`] : null,
    xc != null && !has('comments') ? [fx(xc), 'usual comments'] : null,
    fol != null && lens !== 'follows' ? [sg(Math.round(fol)), 'followers in 48h, typical'] : null,
    lw && !has('last week') ? [lw === ids.length && ids.length > 1 ? 'All' : `${lw}`, 'from last week'] : null,
  ].filter(Boolean);
  // what happened across these posts, and why they land where they do: written from every post's own description
  const rd = voiceOf(A.reads, lens, r);
  const kn = f48 ? 'followers' : KEYN[k], [la, lb] = r.lw || ['With it', 'Without it'], lists = r.wo
    ? postListHTML(la, `${r.ids.length}, most ${kn} first`, f48 ? byF(r.ids) : most(r.ids), xk) + postListHTML(lb, `${r.wo.length}, most ${kn} first`, f48 ? byF(r.wo) : most(r.wo), xk)
    : r.run ? postListHTML('The run', r.brk ? 'in order, and the post that ended it' : 'in order, still going', ids, xk, r.tag || {})
    : f48 ? postListHTML(r.key === 't10' ? 'The posts' : 'That day and the day before', 'most followers first', ids, xk)
    : postListHTML('The posts', `${ids.length}, most ${kn} first`, ids, xk);
  // the course of it: a slice week by week since the first week on file, a mover's posts and a shift's halves week by
  // week across the 100, every post week by week with the row's week marked, or a run as its trace across the 100
  const pcs = (t, e, x) => `<section class="pcs"><h5>${t}<em>${esc(e)}</em></h5>${x}</section>`, wkOf = (iso) => (A.weeks.find((w) => w.ids.some((x) => P[x].d >= iso)) || {}).s;
  const course = lens === 'slices' ? pcs('Week by week', `every ${low(r.title)} post since ${A.weeks[0].s}`, courseHTML(A.order.filter((x) => P[x].pat === r.key)))
    : lens === 'frequency' ? pcs('Week by week', `every post since ${A.weeks[0].s}`, courseHTML(A.order, r.week))
    : lens === 'streaks' ? pcs('How it ran', `every post, ${S.span}, on ${A.his} usual ${KEYN[r.k || 'lk']}`, ecgHTML(r, false))
    : lens === 'shifts' ? (r.kind === 'days' ? pcs('Day by day', `every day of the 100, since ${day(r.d1)} in red`, daysHTML(r.d0, r.d1)) : pcs('Week by week', `${r.kind === 'slice' ? `every ${low(r.title)} post` : 'every post'} in the 100, the newer half from ${day(r.d1)}`, courseHTML([...r.ids, ...r.wo], wkOf(r.d1), k, true)))
    : r.wiAll ? pcs('Week by week', `every post ${r.kind === 'side' ? 'with it' : r.kind === 'format' ? `of the ${r.title.toLowerCase()}` : `${low(r.title)}`} in the 100, on ${A.his} usual ${KEYN[r.k] || 'follows'}`, courseHTML(r.wiAll, null, f48 ? 'f48' : r.k, true)) : '';
  return `<div class="pv-t">${capHTML(h)}
    ${stats.length ? `<div class="pst">${stats.map(([b, e]) => `<span><b>${b}</b><em>${e}</em></span>`).join('')}</div>` : ''}
    ${rd ? `<section class="pwh"><h5>What happened</h5><p class="pw1">${esc(rd.what)}</p><p class="pw2"><b>Why</b>${esc(rd.why)}</p></section>` : ''}</div>
    ${course}${lists}`;
}
function postKicker(id) { const p = A.posts[id]; return `${WD[new Date(p.at).getDay()]} ${day(p.d)}`; }

/* ---- the post mortem: the breakdown (each frame: where it sits, what happens there, what was said), what it was going
   for, how it landed and why, and against its closest posts ---- */
const PEOPLE = { 0: 'nobody on screen', 1: 'one person on screen', 2: 'two people on screen', 3: 'three or more on screen' };
const STYLE = { talking: 'talking to camera', skit: 'an acted skit', vlog: 'a vlog', photo: 'a posed photo', graphic: 'a designed graphic', meme: 'a meme', product: 'a product shot', event: 'an event' };
// the regulars: the people, brands, products, places and bits that keep coming back on this feed, named in the read file
// (voice.regulars) and found in a post's caption and description. A post's regulars give it a record: every other post
// with the same one, how those typically did, and the last one before it
let REGS = null;
function regsAll() { if (!REGS) REGS = ((A.voice || {}).regulars || []).map((g) => ({ ...g, re: new RegExp(g.rx, 'i') })); return REGS; }
function regsOf(id) {
  return regsAll().filter((g) => g.re.test(txtOf(id))).map((g) => {
    const others = A.order.filter((i) => i !== id && g.re.test(txtOf(i)));
    const before = others.filter((i) => A.posts[i].d < A.posts[id].d && xPost(i, 'lk') != null);
    return { ...g, label: REGL(g), others, n: others.length, x: xSet(others, 'lk'), last: before[before.length - 1] || null };
  });
}
// what a post has, each with its record, the reason most likely first: its regulars, its slice, the sides it has that
// hold up, and what it did for followers
function whyOf(id, xl) {
  const p = A.posts[id], his = A.his, out = [], seen = new Set();
  regsOf(id).forEach((g) => {
    // (two regulars that only ever share the same posts are one record: the first named stands for both)
    const key = g.others.join(); if (g.n && seen.has(key)) return; seen.add(key);
    if (!g.n) { out.push({ t: g.label, s: 'The first time on file.', g: 'First', d: 'no', w: 3 }); return; }
    const last = g.last ? `; the last one before this, ${day(A.posts[g.last].d)}, did ${fx(xPost(g.last, 'lk'))}` : '';
    out.push({ t: g.label, s: `${g.n === 1 ? '1 other post' : `${g.n} other posts`}${g.x != null ? `, ${g.n === 1 ? 'at' : 'typically'} ${fx(g.x)} ${his} usual likes` : ''}${last}.`, g: g.x != null ? fx(g.x) : `${g.n}`, d: g.x == null ? 'no' : g.x >= 1 ? 'up' : 'dn', w: 2 + (g.x != null ? Math.abs(Math.log(g.x)) : 0) });
  });
  const sl = S.win.filter((i) => A.posts[i].pat === p.pat), sx = xSet(sl, 'lk');
  if (sx != null) out.push({ t: cap1(low(p.pat)), s: `${cap1(his)} ${sl.length} lately typically did ${fx(sx)}${xl != null ? `; this one ${fx(xl)}` : ''}.`, g: fx(sx), d: sx >= 1 ? 'up' : 'dn', w: 1 + Math.abs(Math.log(sx)) });
  A.sides.forEach((g) => g.rows.forEach((x) => {
    if (/real/.test(g.name) || !x.wi.includes(id) || (x.slice && x.slice !== p.pat)) return;
    const k = x.key === 'ct' ? 'cm' : 'lk', ma = xSet(x.wi, k), mo = xSet(x.wo_ids, k), m = ma != null && mo ? ma / mo : null; if (m == null) return;
    out.push({ t: `${x.name}${x.slice ? `, in ${low(x.slice)}` : ''}`, s: `Posts with it get ${fx(m)} the ${KEYN[k]} of posts without${x.slice ? ` in ${low(x.slice)}` : ''}.`, g: fx(m), d: m >= 1 ? 'up' : 'dn', w: Math.abs(Math.log(m)) });
  }));
  const best = out.sort((a, b) => b.w - a.w).slice(0, 4);
  if (p.f48 != null && B.f48 != null) best.push({ t: 'Followers', s: `${sg(p.f48)} in the two days after; a usual post brings ${sg(Math.round(B.f48))}.`, g: sg(p.f48), d: p.f48 >= B.f48 ? 'up' : 'dn' });
  return best;
}
// plays it takes to earn a like on a usual reel lately
const usualPPL = () => medOf(S.win.filter((i) => A.posts[i].fmt === 'Reels' && A.posts[i].pl && A.posts[i].lk).map((i) => A.posts[i].pl / A.posts[i].lk));
/* ---- the post mortem, in the page's own grammar: the count it moved most against the usual post, Feeder Reader's
   take and read where the read file has them, the breakdown, why it went the way it did (what it has, and how that has
   gone before), every other time the same regular turned up, where it sits in the run of posts, and its closest posts ---- */
function postView(id) {
  const p = A.posts[id], v = p.top, his = A.his, r = beatOf(id, A.order), bk = p.bk || null, PV = ((A.voice || {}).posts || {})[id] || null;
  // (its place in its week on the count the page reads: likes, where the overall rank leans on a wrong follower count)
  const rkey = A.yard === 'likes' ? 'lt' : 'top', wk = A.weeks.find((w) => w.ids.includes(id)), wr = wk && p[rkey] != null ? byRank(wk.ids, rkey).indexOf(id) + 1 : 0;
  const xl = xPost(id, 'lk'), xc = xPost(id, 'cm');
  // (it leads on comments only where they ran much further from the usual than its likes did)
  const byC = xc != null && xl != null && Math.abs(Math.log(Math.max(xc, .01))) > Math.abs(Math.log(Math.max(xl, .01))) + .5, hx = byC ? xc : xl;
  const head = { num: hx != null ? fx(hx) : 'New', unit: hx != null ? `${his} usual ${byC ? 'comments' : 'likes'}` : 'counted at day 7', line: PV && PV.take ? PV.take : `${r.t}.`,
    aside: callOf(id) && liveIds().includes(id) ? `Its day-3 call: ${fx(callOf(id).x)} ${his} usual likes by day 7.` : v == null ? 'Still counting: it’s under a week old.' : wk && wr ? (wr === 1 ? `The best of its week’s ${wk.ids.length}.` : `The ${ord(wr)} of ${wk.ids.length} that week.`) : '', tone: tx(hx) };
  const ppl = p.pl && p.lk ? Math.round(p.pl / p.lk) : null, upl = usualPPL();
  const stats = [
    p.lk != null ? [K(p.lk), `likes${!byC || xl == null ? '' : ` · ${fx(xl)} usual`}`] : null,
    p.cm != null ? [K(p.cm), `comments${byC || xc == null ? '' : ` · ${fx(xc)} usual`}`] : null,
    p.pl ? [K(p.pl), `plays · a like every ${ppl}${upl ? `, usual ${Math.round(upl)}` : ''}`] : null,
    p.f48 != null ? [sg(p.f48), `followers in two days · usual ${sg(Math.round(B.f48))}`] : null,
  ].filter(Boolean);
  const fr = p.rp || [], sq = p.fmt !== 'Reels';
  const beats = fr.length ? `<div class="rpl">${fr.map((f, k) => { const b = bk && bk.frames && bk.frames[k]; return `<div class="rf${sq ? ' sq' : ''}"><span class="im" style="background-image:url(${BASE}${f.f})">${f.at ? `<span>${esc(f.at)}</span>` : ''}</span><span class="rft">${b ? `<b class="bl">${esc(b.line)}</b>` : ''}<span class="q${f.kind === 'shows' ? ' sh' : ''}">${esc(f.line)}</span></span></div>`; }).join('')}</div>`
    : `<div class="rpl"><div class="rf sq"><span class="im" style="${coverStyle(id)}"></span><span class="q sh">Only the cover is on file: frames start with the posts fingerprinted from ${A.acct === 'anuj' ? '19' : '21'} Sep.</span></div></div>`;
  const f = p.fx || {}, bits = [PEOPLE[f.people], f.shot === 'self' ? 'self-shot' : f.shot === 'other' ? 'filmed by someone else' : null, STYLE[f.style], f.hook ? 'words on the cover' : null, f.known ? 'a famous face' : null].filter(Boolean);
  const why = whyOf(id, xl);
  // every other time: the regular with the longest record, else the last posts of its slice
  const rg = regsOf(id).filter((g) => g.n).sort((a, b) => b.n - a.n)[0];
  const again = rg ? [...rg.others].reverse().slice(0, 9) : S.win.filter((i) => i !== id && A.posts[i].pat === p.pat).reverse().slice(0, 9);
  const againHd = rg ? `${rg.label.replace(/^With /, 'with ').replace(/^In /, 'in ').replace(/^At /, 'at ')} · ${rg.n === 1 ? '1 other post' : `${rg.n} other posts`}${rg.x != null ? `, ${rg.n === 1 ? 'at' : 'typically'} ${fx(rg.x)}` : ''}` : `the last ${again.length} ${low(p.pat)} posts`;
  const near = (bk && bk.near) || [];
  const going = bk ? bk.going_for : p.prem || '';
  const what = p.sum ? esc(p.sum) : p.cap ? `“${esc(p.cap)}”` : esc(cap1(bits.join(', ')));
  return `<div class="pm-hd"><p class="pm-k">${esc(`${p.at.slice(11, 16)} · ${cap1(FMT1[p.fmt] || 'post')} · ${p.pat}`)}</p><div class="cap capx"><div class="w1"><div class="c" data-tone="${head.tone}">${headHTML(head)}</div></div><div class="w2"><div class="c">${bodyHTML(head)}</div></div></div><p class="pm-w">${what}</p>
      ${stats.length ? `<div class="pst">${stats.map(([b, e]) => `<span><b>${b}</b><em>${esc(e)}</em></span>`).join('')}</div>` : ''}</div>
    ${PV && PV.read ? `<section class="pwh"><h5>What happened</h5><p class="pw1">${esc(PV.read)}</p></section>` : ''}
    <section class="pms2"><h5>The breakdown<em>${fr.length ? `${fr.length} frame${fr.length === 1 ? '' : 's'}` : 'the cover'}</em></h5>${beats}</section>
    ${going ? `<section class="pms2"><h5>What it was going for</h5><p class="pm-p">${esc(going)}</p>${bits.length ? `<p class="pm-q">On screen: ${esc(bits.join(', '))}.</p>` : ''}</section>` : ''}
    ${why.length ? `<section class="pms2"><h5>Its record<em>what it has, and how that has gone before</em></h5><div class="lvs why">${why.map((x, j) => `<div class="lv"><span class="ar2 ${x.d}">${j + 1}</span><span class="t2">${esc(x.t)}<span>${esc(x.s)}</span></span><span class="g2${x.d === 'up' ? ' up' : ''}">${esc(x.g)}</span></div>`).join('')}</div></section>` : ''}
    ${again.length ? `<section class="pms2"><h5>Every other time<em>${esc(againHd)}, newest first</em></h5><div class="pgrid">${again.map((i) => tileHTML(i, { metric: 'xlk' })).join('')}</div></section>` : ''}
    ${S.win.includes(id) ? `<section class="pms2"><h5>Where it sits<em>every post, ${esc(S.span)}, on ${his} usual likes</em></h5>${ecgHTML({ k: 'lk', run: [id], brk: null }, false)}</section>` : ''}
    ${near.length ? `<section class="pms2"><h5>Against its closest posts<em>same slice, same format</em></h5>${PV && PV.twin ? `<p class="pm-p">${esc(PV.twin)}</p>` : ''}<div class="pgrid">${near.filter((i) => A.posts[i]).map((i) => tileHTML(i, { metric: 'xlk' })).join('')}</div></section>` : ''}
    <a class="pmig" href="https://www.instagram.com/p/${id}/" target="_blank" rel="noopener">Open on Instagram ↗</a>`;
}

/* ---- the sheet follows a finger down on a phone ---- */
function hookPanel() {
  const pn = $('pn'); let d = null;
  pn.addEventListener('pointerdown', (e) => { if (isDesk() || !e.target.closest('.grab, .pnh') || e.target.closest('button')) return; d = { y: e.clientY, t: performance.now(), id: e.pointerId }; pn.classList.add('drag'); try { pn.setPointerCapture(e.pointerId); } catch {} });
  pn.addEventListener('pointermove', (e) => { if (!d || e.pointerId !== d.id) return; pn.style.transform = `translateY(${Math.max(0, e.clientY - d.y)}px)`; });
  const end = (e) => { if (!d || e.pointerId !== d.id) return; const dy = Math.max(0, e.clientY - d.y), vv = dy / Math.max(1, performance.now() - d.t); d = null; pn.classList.remove('drag'); if (dy > 120 || vv > .6) closePanel(); else pn.style.transform = ''; };
  pn.addEventListener('pointerup', end); pn.addEventListener('pointercancel', end);
  pn.addEventListener('click', (e) => {
    const b = e.target.closest('button, a'); if (!b) return;
    if (b.id === 'pnx') { closePanel(); return; } if (b.id === 'pnp') { stepPanel(-1); return; } if (b.id === 'pnn') { stepPanel(1); return; }
    if (b.id === 'pnbk') { PN.view = PN.back; PN.back = null; renderPanel(-1); markRow(); return; }
    if (b.dataset.post) { const g = b.closest('.pgrid, .ppl'); openPost(b.dataset.post, g ? [...g.querySelectorAll('[data-post]')].map((x) => x.dataset.post) : [b.dataset.post]); }
  });
  $('pns').addEventListener('click', closePanel);
}

/* =====================================================================
   STORY: the cards as Instagram shows stories; on a laptop the card on show sits in the middle with the ones either
   side of it smaller. A card's words ride with it and settle a beat after it lands
   ===================================================================== */
let SK = -1;
function baseX() { if (!isDesk()) return 0; const vp = $('vp'), s = q1('#track .slide'); return vp && s ? (vp.clientWidth - s.offsetWidth) / 2 : 0; }
function storyPlace() { const p = $('player'); if (p) p.style.setProperty('--bx', baseX() + 'px'); }
/* ---- a phone's story slides from card to card inside its frame: the card on show slides out whole, settling back a touch
   and dimming as it goes, while the next slides in beside it. Under a finger it follows the drag; let go, it carries on
   on a spring that sets off at the finger's speed (SWV) or falls back. Taps, the bars and the clock move it too. As a card lands its pictures drift
   into place inside their tiles, then its words come in one after another: the kick from the side, the number up out of
   its mask, what it counts, and the headline a word at a time ---- */
const BEAT = 54, BEASE = 'cubic-bezier(.16, .9, .2, 1)', SHADE = .5;
const WORDS = '.foot > .kick, .foot .num > span, .foot .unit, .foot .line .w > span, .foot .tog';
let SWV = 0, SWT = 600, DRN = null, DRAGGED = false;
const slideOf = (k) => q1(`.slide[data-k="${k}"]`);
function faces(dir, p) {
  return {
    out: { transform: `translate3d(${-dir * p * 100}%,0,0) scale(${1 - .06 * p})`, transformOrigin: '50% 50%' },
    in: { transform: `translate3d(${dir * (1 - p) * 100}%,0,0)`, transformOrigin: '50% 50%' },
  };
}
function rest(s_, on) { if (!s_) return; s_.getAnimations().forEach((a) => a.cancel()); s_.style.transform = ''; s_.style.transformOrigin = ''; s_.classList.remove('mv'); s_.classList.toggle('on', on); const d = s_.querySelector('.sdim'); if (d) { d.getAnimations().forEach((a) => a.cancel()); d.style.opacity = ''; } }
// a drag: the card on show and the one it turns toward, at p of the way
function turnAt(nb, dir, p) {
  const a = slideOf(at), b = slideOf(nb), f = faces(dir, p);
  if (DRN != null && DRN !== nb) rest(slideOf(DRN), false);
  DRN = nb;
  [a, b].forEach((s_) => { s_.getAnimations().forEach((x) => x.cancel()); s_.classList.add('mv'); });
  Object.assign(a.style, f.out); Object.assign(b.style, f.in);
  a.querySelector('.sdim').style.opacity = p * SHADE; b.querySelector('.sdim').style.opacity = 0;
}
function turnTo(from, k, p0) {
  const SP = spring(SWV, .86, 9.5), D = SP.duration; SWV = 0; SWT = D;
  const dir = k > from ? 1 : -1, a = slideOf(from), b = slideOf(k);
  qa('.slide').forEach((s_) => { if (s_ !== a && s_ !== b) rest(s_, false); });
  if (DRN != null && DRN !== k && DRN !== from) rest(slideOf(DRN), false);
  DRN = null;
  if (RM || !a || !b) { rest(a, false); rest(b, true); return; }
  const f0 = faces(dir, p0), f1 = faces(dir, 1), o = { duration: D, easing: SP.easing, fill: 'forwards' };
  [a, b].forEach((s_) => { s_.getAnimations().forEach((x) => x.cancel()); s_.classList.add('mv'); });
  a.classList.remove('on'); b.classList.add('on');
  a.style.transformOrigin = f0.out.transformOrigin; b.style.transformOrigin = f0.in.transformOrigin;
  const run = a.animate([{ transform: f0.out.transform }, { transform: f1.out.transform }], o);
  b.animate([{ transform: f0.in.transform }, { transform: f1.in.transform }], o);
  a.querySelector('.sdim').animate([{ opacity: p0 * SHADE }, { opacity: SHADE }], o);
  run.onfinish = () => { if (at !== k) return; rest(a, false); rest(b, true); };
}
// a drag let go short of turning: the two fall back
function settleBack(p0) {
  const a = slideOf(at), b = DRN != null ? slideOf(DRN) : null; if (!b) return;
  const SP = spring(0, .9, 12), dir = DRN > at ? 1 : -1, f0 = faces(dir, p0), f1 = faces(dir, 0), o = { duration: SP.duration, easing: SP.easing, fill: 'forwards' }, nb = DRN;
  const run = a.animate([{ transform: f0.out.transform }, { transform: f1.out.transform }], o);
  b.animate([{ transform: f0.in.transform }, { transform: f1.in.transform }], o);
  a.querySelector('.sdim').animate([{ opacity: p0 * SHADE }, { opacity: 0 }], o);
  run.onfinish = () => { if (DRN !== nb && DRN != null) return; rest(a, true); rest(b, false); DRN = null; };
}
function storyArrive(k) {
  const prev = SK, dir = SK < 0 ? 0 : k > SK ? 1 : k < SK ? -1 : 0; SK = k; storyPlace();
  const card = q1(`.card[data-k="${k}"]`), ch = $('pch');
  if (!card || !dir || RM) return;
  const EASE = 'cubic-bezier(.22, 1, .36, 1)';
  if (ch) ch.animate([{ opacity: 0, transform: `translate3d(${dir * 10}px,0,0)` }, { opacity: 1, transform: 'none' }], { duration: 560, delay: 120, easing: EASE, fill: 'backwards' });
  // (a card dragged in has been seen the whole way: it lands as it is)
  if (DRAGGED) { DRAGGED = false; return; }
  const D = SWT, at0 = D * .3;
  const pics = [...card.querySelectorAll('.md img, .md .qc')], labs = [...card.querySelectorAll('.md .plab, .md.pad > *')];
  const kick = card.querySelector('.foot > .kick'), num = card.querySelector('.foot .num > span'), unit = card.querySelector('.foot .unit'), ws = [...card.querySelectorAll('.foot .line .w > span')], tog = card.querySelector('.foot .tog');
  [...pics, ...labs, kick, num, unit, ...ws, tog].forEach((e_) => e_ && e_.getAnimations().forEach((a) => a.cancel()));
  const o = (delay, dur = 820) => ({ duration: dur, delay, easing: EASE, fill: 'backwards' });
  // the pictures ease back from a breath of zoom; then the words in four beats: the kick, the number out of its mask, what
  // it counts, and the headline rising whole
  pics.forEach((e_) => e_.animate([{ transform: 'scale(1.05)' }, { transform: 'none' }], { duration: D + 600, easing: EASE }));
  labs.forEach((e_) => e_.animate([{ opacity: 0 }, { opacity: 1 }], o(at0 + 120, 600)));
  if (kick) kick.animate([{ opacity: 0, transform: 'translate3d(0,8px,0)' }, { opacity: 1, transform: 'none' }], o(at0));
  if (num) num.animate([{ transform: 'translate3d(0,110%,0)' }, { transform: 'none' }], o(at0 + 80, 900));
  if (unit) unit.animate([{ opacity: 0, transform: 'translate3d(0,8px,0)' }, { opacity: 1, transform: 'none' }], o(at0 + 170));
  ws.forEach((e_) => e_.animate([{ transform: 'translate3d(0,120%,0)' }, { transform: 'none' }], o(at0 + 240, 900)));
  if (tog) tog.animate([{ opacity: 0 }, { opacity: 1 }], o(at0 + 320, 600));
}
// a card's words open (a tap on them): the card darkens and the rest of its words rise in, one piece after another
function setOpen(card, on) {
  card.classList.toggle('open', on);
  if (!on || RM) return;
  const parts = [...card.querySelectorAll('.more > *')].flatMap((e_) => (e_.matches('.figs, .frows, .rev, .mixk') ? [...e_.children] : [e_]));
  parts.forEach((e_, j) => { e_.getAnimations().forEach((a) => a.cancel()); e_.animate([{ opacity: 0, transform: 'translate3d(0,18px,0)' }, { opacity: 1, transform: 'none' }], { duration: 560, delay: 120 + Math.min(j, 10) * 58, easing: BEASE, fill: 'backwards' }); });
}
/* ---- the story a laptop up: the ten cards stand side by side as spines, and the one on show opens wide, its words on
   the left and its picture on the right. Another picked opens as this one folds, in one move; its words slide in a
   beat behind it, one line after another ---- */
let DS = false;
function deskStoryHTML() {
  const best = A.grid.top.reduce((a, b) => (b[1] < a[1] ? b : a))[0], n = A.stories.length, no = (k) => String(k + 1).padStart(2, '0');
  return `<div class="ds" id="ds"><div class="dsr" id="dsr">${A.stories.map((s, k) => `<article class="dsc k-${s.media.kind}${s._t ? ` t-${s._t}` : ''}${k === 0 ? ' on' : ''}" data-k="${k}" style="z-index:${k + 1}">${k ? '<i class="dgut"></i>' : ''}<div class="dsi">
      <div class="dz">${mediaHTML(s, k)}</div>
      <div class="dtx"><div class="dhd"><span class="av" data-cv="${best}" style="${cv(best)}"></span><b>${esc(A.handle.replace('@', ''))}</b><em>${esc(chLabel(s))}</em><span class="dno">${no(k)} / ${n}</span></div>
        <div class="dft">${kickOf(s)}<div class="fig"><b class="num">${esc(s.num)}</b><span class="unit">${esc(s.unit)}</span></div><h3 class="line">${esc(s.line)}</h3>${s.media.kind === 'list' ? '' : `<div class="more">${moreHTML(s)}</div>`}</div></div>
      <button class="dsp" data-k="${k}" aria-label="${esc(spineOf(s))}"><b>${no(k)}</b><span>${esc(spineOf(s))}</span>${mark(s)}</button></div></article>`).join('')}</div>
    <div class="dsb"><div class="dpr" id="igbars">${A.stories.map((s, k) => `<button data-k="${k}" class="${s._t ? `t-${s._t}` : ''}" aria-label="${esc(spineOf(s))}"><i><b></b></i></button>`).join('')}</div>
      <div class="dct"><button class="nb" id="arrp" aria-label="Back">${SV('M15 5l-7 7 7 7')}</button><button class="nb" id="pp" aria-label="Pause"><svg viewBox="0 0 24 24" id="ppi"><rect x="6" y="5" width="4" height="14" rx="1"/><rect x="14" y="5" width="4" height="14" rx="1"/></svg></button><button class="nb" id="arrn" aria-label="Next">${SV('M9 5l7 7-7 7')}</button></div></div></div>`;
}
// the open card takes what the spines leave; below 620px of it the words lie over the picture instead
function deskSize() {
  const ds = $('ds'); if (!ds) return;
  const W = ds.clientWidth, n = A.stories.length, gap = 8, sw = Math.max(40, Math.min(58, Math.round((W - 680) / (n - 1) - gap)));
  const dw = W - (n - 1) * (sw + gap); DSZ = { sw, dw, gap };
  ds.style.setProperty('--sw', sw + 'px'); ds.style.setProperty('--dw', dw + 'px');
  ds.style.setProperty('--dh', Math.round(Math.max(540, Math.min(680, innerHeight - 190))) + 'px');
  ds.style.setProperty('--tw', Math.round(Math.max(340, Math.min(420, dw * .52))) + 'px');
  ds.classList.toggle('ov', dw < 620);
}
// the row moves as one, and the card opening grows out of its own spine. Each card lies over the one before it (a gutter
// in the page's colour between them), so a closed card shows only its sliver; a closed card whose panel is a picture
// shows a sliver of that picture (from the middle of a single one, from the middle of the left column of four). Picking a
// card slides the cards after it out of the way while its picture glides from the sliver to its place, its label melts
// and its words slide in once the picture is under way; the card closing darkens to its spine as it folds. The gutter
// before a card is part of that card, so the two can never drift apart mid-move (Safari runs each layer's animation on
// its own). Transforms and opacity only, every moving piece on a layer of its own from the start, so the browser moves
// it without repainting a thing; a pick mid-move carries on from wherever everything is
let DSZ = { sw: 48, dw: 680, gap: 8 };
const GROW = 'cubic-bezier(.32, .72, 0, 1)';
/* a spring (a mass on a spring, damped just short of still: it settles with the faintest give, under 1%), sampled into
   an easing curve, with the time it takes to come to rest. v0 is the speed it sets off at, in whole distances a
   second: a flick hands its speed on, a tap starts it from rest. Where linear() easings aren't supported, the glide
   curve stands in */
const LIN = typeof CSS !== 'undefined' && CSS.supports && CSS.supports('animation-timing-function', 'linear(0, 1)');
function spring(v0 = 0, zeta = .86, w = 9) {
  // (zeta 1: critically damped, the fastest glide that never passes its mark)
  const crit = zeta >= 1, wd = crit ? 0 : w * Math.sqrt(1 - zeta * zeta), B = crit ? 0 : (zeta * w - v0) / wd;
  const x = crit ? (t) => 1 - Math.exp(-w * t) * (1 + (w - v0) * t) : (t) => 1 - Math.exp(-zeta * w * t) * (Math.cos(wd * t) + B * Math.sin(wd * t));
  let T = .25; while (T < 2.5 && (crit ? Math.exp(-w * T) * (1 + Math.abs(w - v0) * T) : Math.exp(-zeta * w * T) * (1 + Math.abs(B))) > .002) T += .02;
  // (at(p): when, in ms, it first gets p of the way)
  const at = (p) => { for (let t = 0; t < T; t += .004) if (x(t) >= p) return Math.round(t * 1000); return Math.round(T * 1000); };
  if (!LIN) return { duration: Math.round(T * 1000), easing: GROW, at };
  const n = 60, pts = Array.from({ length: n + 1 }, (_, i) => (i === n ? 1 : +x((T * i) / n).toFixed(4)));
  return { duration: Math.round(T * 1000), easing: `linear(${pts.join(', ')})`, at };
}
function deskPlace(k, dir) {
  const move = !!dir;
  const { sw, dw, gap } = DSZ, ds = $('ds'); if (!ds) return;
  // (the cards ride one spring, iOS-quick and critically damped: most of the way in a third of a second, then easing into
  // place without ever passing it. Nothing
  // inside a card moves: its picture stands at its left, so the spine is the picture's own edge, and opening it is the
  // window sweeping right across a still picture and then the words beside it)
  const SP = spring(0, 1, 11.5), D = SP.duration, ov = ds.classList.contains('ov'), tw = ov ? 0 : parseFloat(ds.style.getPropertyValue('--tw')) || 360;
  const xOf = (j) => j * (sw + gap) + (j > k ? dw - sw : 0);
  const stateOf = (c, j) => {
    const open = j === k;
    return {
      card: { transform: `translate3d(${xOf(j)}px,0,0)` },
      tx: { opacity: open ? 1 : 0 },
      sp: { opacity: open ? 0 : 1 },
    };
  };
  const cards = [...qa('.dsc')];
  const plan = cards.map((c, j) => {
    const el = { card: c, tx: c.querySelector('.dtx'), sp: c.querySelector('.dsp') }, to = stateOf(c, j), from = {}, dz = c.querySelector('.dz');
    Object.keys(el).forEach((n) => { const cs = getComputedStyle(el[n]); from[n] = Object.fromEntries(Object.keys(to[n]).map((p) => [p, cs[p]])); });
    return { el, to, from, dz, open: j === k, was: c.classList.contains('on') };
  });
  plan.forEach(({ el, to, from, dz, open, was }) => {
    Object.keys(el).forEach((n) => { el[n].getAnimations().forEach((a) => a.cancel()); Object.assign(el[n].style, to[n]); });
    if (!move || RM) return;
    const go = (n, o) => el[n].animate([from[n], to[n]], { duration: D, easing: SP.easing, fill: 'backwards', ...o });
    go('card');
    const labs = dz.querySelectorAll('.plab'), words = [el.tx.querySelector('.dhd'), el.tx.querySelector('.dft')].filter(Boolean);
    [el.tx, ...words, ...labs].forEach((e_) => e_.getAnimations().forEach((a) => a.cancel()));
    if (open && !was) {
      // its dark lifts at once as it starts to open; the picture's captions come up once it's mostly open; the words
      // when the edge has swept half across their panel (beside the whole picture, laid over it on a narrow card)
      go('sp', { duration: 200, easing: 'ease-out' });
      const half = ov ? D * .3 : SP.at(Math.min(.95, (dw - tw * .75 - sw) / (dw - sw)));
      el.tx.animate([{ opacity: 0 }, { opacity: 1 }], { duration: 160, delay: Math.max(0, half - 80), easing: 'ease-out', fill: 'backwards' });
      words.forEach((e_) => e_.animate([{ opacity: 0 }, { opacity: 1 }], { duration: 300, delay: half, easing: 'ease-out', fill: 'backwards' }));
      labs.forEach((l) => l.animate([{ opacity: 0 }, { opacity: 1 }], { duration: 300, delay: SP.at(.55), easing: 'ease-out', fill: 'backwards' }));
    } else if (!open && was) {
      // its words and captions go at once (the edge is about to cover them); its picture stays clear while it folds, and
      // its spine's wash only comes on as it narrows to a spine (on the whole picture it would flood it)
      go('tx', { duration: 120, easing: 'ease-out' }); go('sp', { duration: 240, delay: SP.at(.72), easing: 'ease-out', fill: 'backwards' });
      labs.forEach((l) => l.animate([{ opacity: 1 }, { opacity: 0 }], { duration: 120, easing: 'ease-out', fill: 'forwards' }));
    }
  });
}
function deskGo(k) {
  const n = A.stories.length; k = Math.max(0, Math.min(n - 1, k));
  const dir = k > at ? 1 : k < at ? -1 : 0; at = k; el = 0;
  if (!$('dsr')) return;
  deskPlace(k, dir);
  qa('.dsc').forEach((c, j) => c.classList.toggle('on', j === k));
  $('arrp').disabled = k === 0; $('arrn').disabled = k === n - 1;
  paintBars();
  const c = q1(`.dsc[data-k="${k}"]`); if (!c) return;
  layoutCard(c, k);
}
function hookDesk() {
  const ds = $('ds');
  // a card whose panel is a picture folds to a sliver of it; a chart or a list folds to a plain spine
  qa('.dsc').forEach((c) => c.classList.toggle('pic', !!c.querySelector('.dz img, .dz .qc')));
  qa('.dsc img').forEach((im) => { im.loading = 'eager'; if (im.decode) im.decode().catch(() => {}); });
  durs = A.stories.map((s) => Math.max(8000, Math.min(16000, 5000 + words(s) * 170))); el = 0; paused = false;
  ds.addEventListener('click', (e) => { const b = e.target.closest('.dsp, #igbars button'); if (b) { e.stopPropagation(); go(+b.dataset.k); } });
  $('arrp').addEventListener('click', () => go(at - 1));
  $('arrn').addEventListener('click', () => go(at + 1));
  $('pp').addEventListener('click', () => setPaused(!paused));
  // a mouse resting on the story holds it, as a finger does on a phone
  ds.addEventListener('pointerenter', (e) => { if (e.pointerType === 'mouse') held = true; });
  ds.addEventListener('pointerleave', (e) => { if (e.pointerType === 'mouse') held = false; });
  let acc = 0, lock = false;
  ds.addEventListener('wheel', (e) => { if (Math.abs(e.deltaX) <= Math.abs(e.deltaY)) return; e.preventDefault(); if (lock) return; acc += e.deltaX; if (Math.abs(acc) > 46) { go(at + (acc > 0 ? 1 : -1)); acc = 0; lock = true; setTimeout(() => { lock = false; }, 620); } }, { passive: false });
  const io = new IntersectionObserver((es) => es.forEach((x) => { inView = x.intersectionRatio > .5; }), { threshold: [0, .5, 1] }); io.observe(ds); ios.push(io);
}
function quadHTML(items) { return `<div class="md quad">${items.map((x) => `<div class="qd">${x.img ? `<img src="${x.img}" alt="" draggable="false" loading="lazy">` : `<span class="qc" style="${coverStyle(x.id)}"></span>`}${x.lab || ''}</div>`).join('')}</div>`; }

/* =====================================================================
   PAGE
   ===================================================================== */
function strategyHTML() {
  strategyBuild(); readCards();
  const V = A.voice || {}, usual = [[B.pl, K(Math.round(B.pl || 0)), 'plays'], [B.lk, K(Math.round(B.lk || 0)), 'likes'], [B.cm, K(Math.round(B.cm || 0)), 'comments'], [B.f48, sg(Math.round(B.f48 || 0)), 'followers in 48h']].filter((x) => x[0] != null);
  let act = 0;
  return `<header class="op"><span class="ok"><b>02</b>Strategy · the current 100</span><p class="ol">${esc(V.strategy || A.lead.strategy)}</p><p class="osb">${esc(cap1(A.his))} last 100 posts, since ${day(A.posts[S.win[0]].d)}, in three acts. Every × is against ${A.his} usual post, and every card is marked against the last read, on ${day(S.last)}.</p>
      <div class="usl"><span class="ut">${esc(cap1(A.his))} usual post</span>${usual.map(([, b, e]) => `<span><b>${b}</b><em>${e}</em></span>`).join('')}</div></header>
    <div class="lz">${LENS.map(([k, , , a]) => (a !== act ? ((act = a), actHTML(a)) : '') + lensSection(k)).join('')}</div>`;
}
function drawAll() { qa('.seg').forEach(placeSeg); if (!DS) storyPlace(); }

/* =====================================================================
   PAGE
   ===================================================================== */
function jumpTo(target) {
  if (target === 'story') { scrollTo({ top: 0, behavior: RM ? 'auto' : 'smooth' }); return; }
  const e_ = $(target); if (e_) e_.scrollIntoView({ behavior: RM ? 'auto' : 'smooth', block: 'start' });
}
function render() {
  IGS = new Set(A.ig || []); SK = -1; DS = isDesk();
  baseBuild(); sinceStory(); storyX(); storyTone();
  ios.forEach((o) => o.disconnect()); ios = [];
  root.innerHTML = `
    <section class="sec wrap" id="story"><header class="op"><span class="ok"><b>01</b>Story · week of 19–25 Oct</span><p class="ol">${esc((A.voice && A.voice.say) || SAY[acct])}</p></header>${DS ? deskStoryHTML() : storyHTML()}</section>
    <section class="sec wrap" id="strategy">${strategyHTML()}</section>
    <section class="sec wrap" id="split">${splitHTML()}</section>
    <div class="wrap foot2"><p>Feeder Reader reads every post on ${esc(A.own)} feed: its likes, comments and plays, the followers each day, and what happens on screen, frame by frame. Every × compares a post (or the typical post of a set) with the usual post among the latest 100 that have reached day 7. The wall’s Top % is where a post ranks among ${esc(A.own)} own posts by day 7${A.yard === 'likes' ? ', on likes: Lakmē’s overall rank leans on a follower count recorded wrong for 57 of its posts' : ''}. ${A.dropped} posts with no cover on file are left out of every count. A side holds up when it survives a shuffle test inside each slice.</p>
      <p>Plays are Instagram’s own counts from a 27 Oct check. Followers are daily snapshots; “in the 48h after” is the change over a post’s day and the two after it. On a card, follows are that change against the pace of the ten days either side. A live post’s call sets its likes at day 3 against what the usual post has by then. Breakdown frames are cut from each post at the beats its fingerprint timed; the lines on them are written by a model from the post’s own beats and checked against these counts.</p></div>`;
  at = 0;
  sizePlayer(); if (DS) hookDesk(); else hookPlayer(); measureMore(); go(0);
  WPICK = null; $('ro').classList.remove('on'); closePanel();
  drawWall(); drawAll(); hookCuts();
  // the part you're in, for the header's switch
  const ch = new IntersectionObserver((es) => es.forEach((e) => { if (e.isIntersecting && onChapter) onChapter(e.target.id === 'strategy' ? 'slices' : e.target.id); }), { rootMargin: '-35% 0px -60% 0px' });
  [...PARTS, 'strategy'].forEach((id) => { const e_ = $(id); if (e_) ch.observe(e_); }); ios.push(ch);
  paintCovers(); loadCovers();
  requestAnimationFrame(() => { if (dead) return; sizePlayer(); measureMore(); go(at); drawAll(); });
}
layer.innerHTML = `<div class="ro" id="ro"></div><div class="pns" id="pns"></div>
  <aside class="pn" id="pn" aria-label="Detail"><div class="grab"></div><div class="pnh"><button class="bk" id="pnbk"></button><span class="hk" id="pnk"></span><button class="nb" id="pnp" aria-label="Previous">${SV('M15 5l-7 7 7 7')}</button><button class="nb" id="pnn" aria-label="Next">${SV('M9 5l7 7-7 7')}</button><button class="nb" id="pnx" aria-label="Close">${SV('M6 6l12 12M18 6L6 18')}</button></div><div class="pnb" id="pnb"></div></aside>`;
on(root, 'click', (e) => {
  const rl = e.target.closest('[data-rl]'); if (rl) { const reel = rl.closest('.ls').querySelector('.rl'); reel.scrollBy({ left: +rl.dataset.rl * Math.max(280, reel.clientWidth * .8), behavior: RM ? 'auto' : 'smooth' }); return; }
  const lr = e.target.closest('[data-lens][data-row]'); if (lr) { openRow(lr.dataset.lens, +lr.dataset.row); return; }
  const sv_ = e.target.closest('.seg [data-sv]');
  if (sv_) { const seg = sv_.closest('.seg'), v = sv_.dataset.sv; seg.querySelectorAll('button').forEach((b) => b.setAttribute('aria-pressed', String(b === sv_))); placeSeg(seg); if (seg.dataset.seg === 'wv') { WV = v; localSet('wall', v); const wh = $('wh'); wh.textContent = v === 'recent' ? 'Newest first. Pick a post to see what it beat.' : 'Grouped by where each landed.'; if (!RM) wh.animate([{ opacity: 0, transform: 'translate3d(0,8px,0)' }, { opacity: 1, transform: 'none' }], { duration: 480, easing: BEASE }); morphWall(); } return; }
  const t = e.target.closest('[data-post]'); if (!t) return;
  if (t.dataset.wall) { wallPick(t.dataset.post); return; }
  if (t.dataset.live) { openPost(t.dataset.post, liveIds()); return; }
  if (t.closest('.wf')) { openPost(t.dataset.post, S.win); return; }
  const box = t.closest('.pgrid'); openPost(t.dataset.post, box ? [...box.querySelectorAll('[data-post]')].map((x) => x.dataset.post) : [t.dataset.post]);
});
on(document, 'click', (e) => {
  if (!WPICK || e.target.closest('.r17 .ro')) return;
  const tile = e.target.closest('#wall .wt');
  if (tile && tile.dataset.post === WPICK) return;
  unpick();
  if (root.contains(e.target)) { e.stopPropagation(); e.preventDefault(); }
}, true);
on(window, 'scroll', roFollow, { passive: true });
on($('ro'), 'click', (e) => { if (e.target.closest('[data-open]')) openPost(e.target.closest('[data-open]').dataset.open, A.wall); if (e.target.closest('[data-unpick]')) unpick(); });
on(window, 'keydown', (e) => {
  if (!PN.open) { if (e.key === 'Escape' && WPICK) unpick(); return; }
  if (e.key === 'Escape') closePanel(); else if (e.key === 'ArrowRight') stepPanel(1); else if (e.key === 'ArrowLeft') stepPanel(-1); else return;
  e.preventDefault(); e.stopImmediatePropagation();
}, true);
on(window, 'resize', () => { clearTimeout(rt); rt = setTimeout(() => { if (dead) return; if (isDesk() !== DS) { render(); return; } sizePlayer(); measureMore(); go(at); qa('[data-wk16]').forEach((h) => { h.dataset.drawn = ''; }); const c = q1(`.card[data-k="${at}"], .dsc[data-k="${at}"]`); if (c) layoutCard(c, at); drawAll(); if (WPICK && WV === 'recent') playTrail(WPICK, true); placeRo(); }, 140); });
hookPanel();
render();
raf = requestAnimationFrame(frame);
on(window, 'scroll', pickSoon, { passive: true }); on(root, 'scroll', pickSoon, { capture: true, passive: true });
on(document, 'visibilitychange', pickSoon);
on(root, 'pointerover', (e) => {
  if (e.pointerType !== 'mouse') return;
  const c = e.target.closest('.rc'), cut = c ? c.querySelector('.cut') : null;
  if (cut === cutPin) return;
  cutPin = cut; if (cut) { clearTimeout(cutPick); playCut(cut); } else pickSoon();
});
return {
  jump: jumpTo,
  unmount() {
    dead = true; cancelAnimationFrame(raf); clearTimeout(rt); clearTimeout(cutT); clearTimeout(cutPick); TRT.forEach(clearTimeout);
    ios.forEach((o) => o.disconnect()); offs.forEach((off) => off());
    root.innerHTML = ''; layer.innerHTML = '';
  },
};
}
