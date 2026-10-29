/* ─────────────────────────────────────────────────────────────
   READ 17 — a feeder's read in three parts, mounted by Read17 into the Read tab:
   - Story: the week as ten cards, shown the way Instagram shows stories (a laptop up: the card on show in the middle,
     the ones either side smaller); a tap reads a card, a swipe moves on, and its words settle a beat after it lands.
   - Strategy: the current 100 posts as one mosaic, each slice a block of its own covers, last week's marked with a dot;
     the facts about the 100 sit beside it, each lighting the posts it is about.
   - Split: the wall, newest first or in rank groups; a pick lights the run of posts it beat.
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
// the parts the header names: the story, Strategy's five pieces, the wall
const PARTS = ['story', 'slices', 'sides', 'streaks', 'followers', 'formats', 'split'];

export function mountRead17({ root, layer, data, acct, base, onChapter }) {
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
const kickOf = (s) => `<span class="kick ${s.ch.toLowerCase()}"><span class="dot"></span>${esc((typeof KICK !== 'undefined' && KICK[s.ch]) || s.ch)}</span>`;
function labelHTML(it) { const v = it.v; return `<div class="plab">${v != null ? `<b class="${wt(v).trim()}" style="background:${sv(v)}">${top(v)}</b>` : ''}<span>${esc(it.label)}</span></div>`; }
function mediaHTML(s, k) {
  const m = s.media;
  if (m.kind === 'grid') return quadHTML(m.ids.slice(0, 4).map((id) => ({ id, lab: labelHTML({ v: A.posts[id] && A.posts[id].top, label: '' }) })));
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
  if (m.kind === 'formats') { const ids = byRank(s.rows.flatMap((r) => r.ids || [])).slice(0, 4); return ids.length === 4 ? quadHTML(ids.map((id) => ({ id, lab: labelHTML({ v: A.posts[id].top, label: `${cap1(FMT1[A.posts[id].fmt] || 'post')} · ${day(A.posts[id].d)}` }) }))) : `<div class="md pad">${formatThumbs(s)}</div>`; }
  if (m.kind === 'days') return `<div class="md pad">${daysViz(s)}</div>`;
  if (m.kind === 'fol') return `<div class="md pad">${folViz(s)}</div>`;
  if (m.kind === 'reviews' && s.reviews.length >= 4) return quadHTML(s.reviews.slice(0, 4).map((r) => ({ id: r.id, lab: labelHTML({ v: A.posts[r.id] && A.posts[r.id].top, label: r.t.split(' · ')[0] }) })));
  if (m.kind === 'reviews') return `<div class="md pad"><div class="rv4">${s.reviews.map((r) => { const v = A.posts[r.id]?.top; return `<div><div class="c" data-cv="${r.id}" style="${cv(r.id)}"><b class="${wt(v).trim()}" style="background:${sv(v)}">${v == null ? 'new' : r1(v)}</b></div><em>${esc(r.t)}</em></div>`; }).join('')}</div></div>`;
  if (m.kind === 'list') return `<div class="md"><div class="erows">${s.rows.map((r, j) => `<div class="erow"><span class="n">${j + 1}</span><span class="ek">${esc(r.k)}</span><span class="ev">${esc(r.v)}</span><span class="ew">${esc(r.why)}</span></div>`).join('')}</div></div>`;
  return '';
}
function formatThumbs(s) {
  return `<div class="thumbs">${s.rows.filter((r) => r.n).map((r) => `<div class="hd"><span>${esc(r.k)} · ${r.n}</span><span>${top(r.top)}</span></div><div class="tg">${r.ids.map((i) => { const v = A.posts[i].top; return `<span class="c" data-cv="${i}" style="${cv(i)}"><b class="${wt(v).trim()}" style="background:${sv(v)}">${v == null ? 'new' : r1(v)}</b></span>`; }).join('')}</div>`).join('')}</div>`;
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
  if (s.rows && s.media.kind === 'formats') x += `<div class="frows">${s.rows.map((r) => `<div class="frow"><b>${esc(r.k)} · ${r.n}</b><span class="u">${r.usual != null ? `usual ${top(r.usual)}` : ''}</span><span class="v">${r.n ? top(r.top) : '–'}</span>${r.n ? `<span class="nt">${esc(r.note)} · likes ${top(r.lt)} · comments ${top(r.ct)}</span>` : `<span class="nt">${esc(r.note)}</span>`}</div>`).join('')}</div>`;
  if (s.reviews) x += `<div class="rev">${s.reviews.map((r) => `<p><b>${esc(r.t)}.</b> ${esc(r.s)}</p>`).join('')}</div>`;
  if (s.so) x += `<p class="so">${esc(s.so)}</p>`;
  if (s.figs) x += `<div class="figs">${s.figs.map(([b, e]) => `<span><b>${esc(b)}</b><em>${esc(e)}</em></span>`).join('')}</div>`;
  if (s.mix) { const tot = A.n; x += `<div class="mix">${s.mix.map(([, n]) => `<i style="flex:${n}"></i>`).join('')}</div><div class="mixk">${s.mix.slice(0, 5).map(([nm, n]) => `<span><b>${Math.round(n / tot * 100)}%</b> ${esc(nm.toLowerCase())}</span>`).join('')}</div>`; }
  if (s.key) x += `<p class="keyl">${esc(s.key)}</p><div class="ramp"><b>Top</b>${[['1–3', 6], ['4–10', 5], ['11–25', 4], ['26–50', 3], ['51–70', 2], ['71–85', 1], ['86–100', 0]].map(([l, v]) => `<span style="background:var(--s${v});${v >= 5 ? 'color:#fff' : ''}">${l}</span>`).join('')}</div>`;
  return x;
}
function footHTML(s) {
  const wide = /[a-z]/i.test(s.num) && !/^Top/.test(s.num) && !/K$|M$|weeks/.test(s.num);
  const list = s.media.kind === 'list';
  return `<div class="foot">${kickOf(s)}<div class="fig"><b class="num${wide ? ' wd' : ''}">${esc(s.num)}</b><span class="unit">${esc(s.unit)}</span></div>
    <div class="lrow"><h3 class="line">${esc(s.line)}</h3>${list ? '' : '<span class="tog" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M6 15l6-6 6 6"/></svg></span>'}</div>
    ${list ? '' : `<div class="more">${moreHTML(s)}</div>`}</div>`;
}
const KICK = { Profile: 'Who this is', Feeding: 'What landed', Folding: 'What sank', Format: 'Formats this week', Frequency: 'Posting this week', Followers: 'Followers this week', Feedback: "The reader's review", 'Follow-up': 'Next week' };
function storyHTML() {
  const best = A.grid.top.reduce((a, b) => (b[1] < a[1] ? b : a))[0], n = A.stories.length;
  return `<div class="player" id="player">
    <div class="vp" id="vp"><div class="track" id="track">${A.stories.map((s, k) => `<article class="slide${k === 0 ? ' on' : ''}" data-k="${k}">
        <div class="card k-${s.media.kind}" data-k="${k}">${mediaHTML(s, k)}<span class="shade"></span>${footHTML(s)}</div></article>`).join('')}</div>
      <div class="igtop"><div class="igbars" id="igbars">${A.stories.map((s, k) => `<button data-k="${k}" aria-label="${esc(s.ch)}"><i><b></b></i></button>`).join('')}</div>
        <div class="ighead"><span class="av" data-cv="${best}" style="${cv(best)}"></span><b>${esc(A.handle.replace('@', ''))}</b><em id="pch">${esc(chLabel(A.stories[0]))}</em>
          <button class="pp" id="pp" aria-label="Pause"><svg viewBox="0 0 24 24" id="ppi"><rect x="6" y="5" width="4" height="14" rx="1"/><rect x="14" y="5" width="4" height="14" rx="1"/></svg></button></div></div>
      <button class="arr p" id="arrp" aria-label="Back"><svg viewBox="0 0 24 24"><path d="M15 5l-7 7 7 7"/></svg></button><button class="arr n" id="arrn" aria-label="Next"><svg viewBox="0 0 24 24"><path d="M9 5l7 7-7 7"/></svg></button></div>
    <div class="hint" id="hint">${isDesk() ? '<span>Click the card to read it · ← → for the next · hold to pause</span>' : '<span>Tap to read · swipe for the next · hold to pause</span>'}</div></div>`;
}
const chLabel = (s) => s.ch + (s.idx ? ` · ${s.idx} of 2` : '');
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
function slideW() { const s = q1('.slide'); return s ? s.getBoundingClientRect().width + parseFloat(getComputedStyle($('track')).columnGap || 12) : 0; }
let el = 0, held = false, paused = false, inView = true, last = 0, durs = [];
const words = (s) => [s.unit, s.line, s.why, s.so].filter(Boolean).join(' ').split(/\s+/).length;
function paintBars() {
  qa('#igbars b').forEach((b, j) => { b.style.transform = j < at ? 'scaleX(1)' : j > at ? 'scaleX(0)' : `scaleX(${Math.min(1, el / (durs[at] || 9000))})`; });
}
function go(k) {
  if (DS) { deskGo(k); return; }
  const n = A.stories.length; k = Math.max(0, Math.min(n - 1, k));
  if (k !== at) qa('.card.open').forEach((c) => c.classList.remove('open'));
  at = k; el = 0;
  const t = $('track'); t.style.transition = ''; t.style.transform = `translate3d(${baseX() - k * slideW()}px,0,0)`;
  qa('.slide').forEach((s, j) => s.classList.toggle('on', j === k));
  $('pch').textContent = chLabel(A.stories[k]);
  $('arrp').disabled = k === 0; $('arrn').disabled = k === n - 1;
  paintBars();
  const c = q1(`.card[data-k="${k}"]`); if (c) layoutCard(c, k);
  storyArrive(k);
}
function frame(t) {
  if (dead) return;
  const dt = last ? Math.min(100, t - last) : 0; last = t;
  const open = q1('.card.open');
  if (A && durs.length && !held && !paused && !open && inView && !document.hidden) {
    el += dt; const d = durs[at] || 9000;
    if (el >= d) { if (at < A.stories.length - 1) go(at + 1); else el = d; }
    paintBars();
  }
  raf = requestAnimationFrame(frame);
}
function setPaused(p) {
  paused = p;
  $('ppi').innerHTML = p ? '<path d="M7 5v14l12-7z"/>' : '<rect x="6" y="5" width="4" height="14" rx="1"/><rect x="14" y="5" width="4" height="14" rx="1"/>';
  $('pp').setAttribute('aria-label', p ? 'Play' : 'Pause');
}
function hookPlayer() {
  const vp = $('vp'), track = $('track');
  durs = A.stories.map((s) => Math.max(8000, Math.min(16000, 5000 + words(s) * 170))); el = 0; paused = false;
  let drag = null, holdT = null;
  vp.addEventListener('pointerdown', (e) => { if (e.button > 0 || e.target.closest('.godeep, .arr, .pp, .igbars')) return; drag = { x: e.clientX, y: e.clientY, t: performance.now(), dx: 0, mode: null, id: e.pointerId, target: e.target }; clearTimeout(holdT); holdT = setTimeout(() => { held = true; }, 220); });
  vp.addEventListener('pointermove', (e) => {
    if (!drag || e.pointerId !== drag.id) return;
    const dx = e.clientX - drag.x, dy = e.clientY - drag.y;
    if (!drag.mode) { if (Math.abs(dx) > 8 || Math.abs(dy) > 10) { clearTimeout(holdT); held = false; } if (Math.abs(dx) > 8 && Math.abs(dx) > Math.abs(dy) * 1.1) { drag.mode = 'x'; track.style.transition = 'none'; try { vp.setPointerCapture(e.pointerId); } catch {} } else if (Math.abs(dy) > 10) drag.mode = 'y'; }
    if (drag.mode === 'x') { drag.dx = dx; const edge = (at === 0 && dx > 0) || (at === A.stories.length - 1 && dx < 0); track.style.transform = `translate3d(${baseX() - at * slideW() + (edge ? dx * .3 : dx)}px,0,0)`; }
  });
  const end = (e) => {
    clearTimeout(holdT); const wasHeld = held; held = false;
    if (!drag || e.pointerId !== drag.id) return; const d = drag; drag = null;
    if (wasHeld && !d.mode) return;
    if (d.mode === 'x') { const v = d.dx / Math.max(1, performance.now() - d.t), W = slideW(); if (d.dx < -W * .16 || v < -.45) go(at + 1); else if (d.dx > W * .16 || v > .45) go(at - 1); else go(at); return; }
    if (d.mode) return;
    const card = d.target.closest('.card'), slide = d.target.closest('.slide');
    if (!slide) return;
    const k = +slide.dataset.k;
    if (k !== at) { go(k); return; }
    if (card && !card.classList.contains('k-list')) { card.classList.toggle('open'); }
  };
  vp.addEventListener('pointerup', end); vp.addEventListener('pointercancel', (e) => { if (drag && drag.mode === 'x') { drag = null; go(at); } else drag = null; });
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
function drawWk16(host, s) {
  const card = host.closest('.card'), ws = A.weeks.filter((w) => !w.part).slice(-s.media.n), W = host.clientWidth || 300, H = Math.max(170, (card ? card.clientHeight : 480) - 230), N = ws.length, cw = W / N, bw = cw * .62, mid = 20 + (H - 40) * .38;
  let x = `<svg viewBox="0 0 ${W} ${H}" height="${H}"><line x1="0" x2="${W}" y1="${mid}" y2="${mid}" stroke="rgb(255 255 255 / .35)" stroke-dasharray="3 3"/>`;
  ws.forEach((w, i) => {
    const xx = i * cw + (cw - bw) / 2;
    if (w.r == null) { x += `<rect x="${xx}" y="${mid - 1}" width="${bw}" height="2" fill="rgb(255 255 255 / .3)"/>`; }
    else { const h = Math.max(3, Math.abs(50 - w.r) / 50 * ((H - 40) / 2)), up = w.r < 50, me = i === N - 1; x += `<rect x="${xx}" y="${up ? mid - h : mid}" width="${bw}" height="${h}" rx="3" fill="${up ? '#fff' : me ? 'var(--accent)' : 'rgb(255 255 255 / .28)'}"/>`; }
    const lb = w.s.split(' '); if ((+lb[0] <= 7 && i < N - 2) || i === N - 1) x += `<text x="${i === N - 1 ? W : i * cw + cw / 2}" y="${H - 2}" text-anchor="${i === N - 1 ? 'end' : 'middle'}" font-size="10" fill="rgb(255 255 255 / ${i === N - 1 ? '.9' : '.5'})">${i === N - 1 ? 'This week' : lb[1]}</text>`;
  });
  x += `<text x="0" y="${mid - 7}" font-size="10" font-weight="600" fill="rgb(255 255 255 / .7)" stroke="var(--card)" stroke-width="3" paint-order="stroke">Top 50%, his middle</text><text x="0" y="16" font-size="10" fill="rgb(255 255 255 / .5)">↑ beat the middle</text><text x="0" y="${H - 18}" font-size="10" fill="rgb(255 255 255 / .5)">↓ below it</text>`;
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
function lab(id) {
  const p = A.posts[id]; if (!p) return '';
  const rv = A.stories.flatMap((s) => s.reviews || []).find((r) => r.id === id);
  if (rv) return `‘${rv.t.split(' · ')[0]}’`;
  return `the ${low(p.pat)} ${FMT1[p.fmt] || 'post'} from ${day(p.d)}`;
}
/* ---- multiples: a count against the account's usual post (the median of the posts in the current 100 that have
   reached day 7). A post's likes ÷ the usual likes; a set's typical likes ÷ the usual likes. Top % stays as a quiet
   second ---- */
const B = {};
const KEYN = { lk: 'likes', cm: 'comments', pl: 'plays', f48: 'follows' };
function baseBuild() {
  const P = A.posts, W = A.order.slice(-100), R = W.filter((i) => P[i].top != null);
  ['lk', 'cm', 'pl', 'f48'].forEach((k) => { const v = R.map((i) => P[i][k]).filter((x) => x != null); B[k] = v.length >= 8 ? medOf(v) : null; });
  const d0 = P[W[0]].d, days = A.daily.filter((x) => x[0] >= d0).map((x) => x[1]);
  B.day = days.length ? medOf(days) : null;
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
// the other counts of a set, as context: "Comments 2.4× · plays 3.1×"
function ctxLine(ids, keys) { const p = keys.map((k) => { const m = xSet(ids, k); return m == null ? null : `${KEYN[k]} ${fx(m)}`; }).filter(Boolean); return p.length ? `${cap1(p.join(' · '))}.` : ''; }
const listOf = (a) => (a.length <= 1 ? a.join('') : `${a.slice(0, -1).join(', ')} and ${a[a.length - 1]}`);
// a story card that leads on a rank leads on a multiple instead: its post, or the week's set it speaks for
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
    s.unit = `${A.his} usual likes · ${s.unit.replace(/\s*·\s*(usual for \w+|the other \d+): Top [\d.]+%/, '').replace(/^(his|its) /, '')} · ${was}`;
    s.num = fx(m);
    // (a figure that repeated the old headline would repeat the new one: it goes)
    if (s.figs) s.figs = s.figs.filter(([v]) => v !== was);
  });
}
function badgeOf(p, metric) {
  if (metric && metric[0] === 'x') { const m = B[metric.slice(1)] > 0 && p[metric.slice(1)] != null ? p[metric.slice(1)] / B[metric.slice(1)] : null, h = xshade(m); return `<span class="bdg${h >= 5 ? ' w' : ''}" style="background:var(--s${h})">${m == null ? 'New' : fx(m)}</span>`; }
  if (metric === 'f48') return p.f48 == null ? '' : `<span class="bdg fl${p.f48 < 0 ? ' dn' : ''}">${sg(p.f48)}</span>`;
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
   STRATEGY: the current 100, cut five ways, each a piece of its own with a picture of its own. Slices: where each
   slice's posts land. Sides: what lifts a post and what drags it. Streaks: the 100 in order, as a pulse, and its runs
   on one side of the middle. Followers: the days, and what brings them. Formats: the 100, post by post. Last week's
   posts are red in every one. A row opens its posts in the panel, and the panel steps through the piece's rows.
   ===================================================================== */
const LENS = [['slices', 'Slices', 'Where each slice lands'], ['sides', 'Sides', 'What lifts a post, what drags it'], ['streaks', 'Streaks', 'The 100 in order'], ['followers', 'Followers', 'The days, and what brings them'], ['formats', 'Formats', 'The 100, post by post']];
const S = { win: [], inW: new Set(), wk: new Set(), wkDays: new Set(), span: '', L: {} };
const rankOf = (ids, key = 'top') => medOf(ids.map((i) => A.posts[i][key]));
const ofW = (ids) => ids.filter((i) => S.inW.has(i));
const capHTML = (f) => `<div class="cap"><div class="w1"><div class="c" data-tone="${f.tone}">${headHTML(f)}</div></div><div class="w2"><div class="c">${bodyHTML(f)}</div></div></div>`;
function strategyBuild() {
  const P = A.posts, W = A.order.slice(-100), his = A.his;
  S.win = W; S.inW = new Set(W); S.L = {};
  const lw = A.weeks.find((w) => w.s === '19 Oct');
  S.wk = new Set(ofW(lw ? lw.ids : []));
  const y = P[W[W.length - 1]].d.slice(0, 4), [wd, wm] = (lw ? lw.s : '1 Jan').split(' '), w0 = `${y}-${String(MON.indexOf(wm) + 1).padStart(2, '0')}-${String(+wd).padStart(2, '0')}`;
  S.wkDays = new Set(Array.from({ length: 7 }, (_, j) => isoAdd(w0, j)));
  S.span = `${day(P[W[0]].d)} – ${day(P[W[W.length - 1]].d)}`;
  const nwk = S.wk.size, inWk = (ids) => ids.filter((i) => S.wk.has(i)).length;

  // slices: where each one's posts land, best typical post first
  const groups = Object.entries(W.reduce((m, i) => ((m[P[i].pat] = m[P[i].pat] || []).push(i), m), {})).map(([name, ids]) => ({ name, ids: byRank(ids), n: ids.length, rank: rankOf(ids), wk: inWk(ids) }));
  const sl = groups.filter((g) => g.rank != null).sort((a, b) => a.rank - b.rank);
  const big = [...groups].sort((a, b) => b.n - a.n)[0], best = sl.find((g) => g.n >= 4) || sl[0];
  const heavy = groups.filter((g) => g.wk).sort((a, b) => b.wk - a.wk).slice(0, 3);
  const ranked = (ids) => ids.filter((i) => P[i].top != null);
  S.L.slices = {
    head: { num: top(best.rank), unit: `${low(best.name)} · ${best.n} of the 100`, line: big === best ? `${cap1(his)} best slice is also ${his} biggest.` : `${cap1(his)} best slice. The biggest, ${low(big.name)} at ${big.n}, lands ${top(big.rank)}.`, aside: heavy.length ? `Last week’s ${nwk}: ${heavy.map((g) => `${low(g.name)} (${g.wk})`).join(', ')}.` : '', tone: best.rank < 50 ? 'beating' : 'plain' },
    rows: sl.map((g) => ({ key: g.name, title: g.name, ids: g.ids, n: g.n, rank: g.rank, head: { num: top(g.rank), unit: `${low(g.name)} · ${g.n} of the 100`, line: `${ranked(g.ids).filter((i) => P[i].top <= 50).length} of its ${ranked(g.ids).length} ranked posts landed in ${his} top half.`, aside: g.wk ? `${g.wk} from last week.` : 'None from last week.', tone: g.rank < 50 ? 'beating' : 'plain' } })),
  };

  // sides: what a post has, against the rest of the 100 (inside its slice, where a side only holds there)
  const sides = [], fakes = [];
  A.sides.forEach((g) => g.rows.forEach((r) => {
    const key = r.key || 'top', wi = ofW(r.wi), wo = ofW(r.wo_ids);
    if (wi.length < 4 || wo.length < 4) return;
    const w = rankOf(wi, key), o = rankOf(wo, key); if (w == null || o == null) return;
    const row = { key: `${r.label}|${r.slice || ''}|${key}`, title: r.name, slice: r.slice, metric: key, ids: byRank(wi, key), wo: byRank(wo, key), w, o, gap: Math.round(o - w), n: wi.length, wk: inWk(wi) };
    if (/real/.test(g.name)) { if (Math.abs(row.gap) >= 10) fakes.push(row); } else if (Math.abs(row.gap) >= 8) sides.push(row);
  }));
  const sd = sides.sort((a, b) => Math.abs(b.gap) - Math.abs(a.gap)).slice(0, 6).sort((a, b) => b.gap - a.gap), fk = fakes.sort((a, b) => Math.abs(b.gap) - Math.abs(a.gap))[0];
  const sideHead = (r) => ({ num: top(r.w), unit: `${low(r.title)}${r.slice ? `, in ${low(r.slice)}` : ''}${r.metric === 'ct' ? ', on comments' : ''} · ${r.n} of the 100`, line: `Without it, ${r.slice ? `the rest of ${low(r.slice)} lands` : 'posts land'} ${top(r.o)}: ${Math.abs(r.gap)} points ${r.gap > 0 ? 'worse' : 'better'}.`, aside: r.wk ? `${r.wk} of last week’s ${nwk} had it.` : `None of last week’s ${nwk} had it.`, tone: r.gap > 0 ? 'beating' : 'plain' });
  const lift = sd.find((r) => r.gap > 0) || sd[0];
  S.L.sides = {
    head: lift ? sideHead(lift) : { num: '–', unit: 'no side moves the 100 by eight points', line: '', aside: '', tone: 'plain' },
    rows: sd.map((r) => ({ ...r, head: sideHead(r) })),
    fake: fk ? (() => { const k = fk.metric === 'ct' ? 'cm' : 'lk', a = xSet(fk.ids, k), o = xSet(fk.wo, k), m = a != null && o ? a / o : null; return `${cap1(low(fk.title))} looks like a ${m != null ? (m >= 1 ? 'lift' : 'drag') : fk.gap > 0 ? 'lift' : 'drag'}${m != null ? `: ${fx(m)} the ${KEYN[k]} of posts without it` : ''}. Inside each slice the gap is gone.`; })() : '',
  };

  // streaks: six or more posts in a row over (or under) the usual post, counted inside the 100 on likes and on comments,
  // against the same usual post the multiples use. A comments run that mostly covers a likes run is the same run
  const runs = [];
  ['lk', 'cm'].forEach((k) => {
    const R = W.filter((i) => P[i].top != null && P[i][k] != null), side = (i) => (P[i][k] >= B[k] ? 1 : -1);
    let s0 = 0;
    for (let j = 1; j <= R.length; j++) if (j === R.length || side(R[j]) !== side(R[s0])) { if (j - s0 >= 6) runs.push({ k, run: R.slice(s0, j), up: side(R[s0]) > 0, brk: R[j] || null }); s0 = j; }
  });
  const merged = [];
  runs.forEach((r) => { const m = merged.find((x) => x.up === r.up && r.run.filter((i) => x.run.includes(i)).length >= .8 * Math.min(x.run.length, r.run.length)); if (m) m.ks.push(r.k); else merged.push({ ...r, ks: [r.k] }); });
  const stRows = merged.map((r) => ({ key: r.brk || `run-${r.run[0]}`, brk: r.brk, k: r.k, ks: r.ks, up: r.up, n: r.run.length, a: P[r.run[0]].d, b: P[r.run[r.run.length - 1]].d, run: r.run, ids: r.brk ? [...r.run, r.brk] : r.run,
    mark: r.brk ? [r.brk] : null, tag: r.brk ? { [r.brk]: 'Ended it' } : null, title: `${r.run.length} in a row ${r.up ? 'over' : 'under'} the usual` })).sort((x, z) => z.b.localeCompare(x.b));
  S.L.streaks = { head: { num: `${stRows.length}`, unit: 'runs of six or more', line: '', aside: `${stRows.length ? `${stRows.length} run${stRows.length === 1 ? '' : 's'} of six or more in the 100` : 'No run of six in the 100'}, counted against ${his} usual post.`, tone: 'plain' }, rows: stRows };

  // followers: the 100's days, and the 48h after its posts
  const d0 = P[W[0]].d, days = A.daily.filter((x) => x[0] >= d0);
  const ok = W.filter((i) => P[i].top != null && P[i].f48 != null), t10 = byF48(ok.filter((i) => P[i].top <= 10)), rest = ok.filter((i) => P[i].top > 10);
  const a = medOf(t10.map((i) => P[i].f48)), b = medOf(rest.map((i) => P[i].f48)), tot = days.reduce((s, x) => s + x[1], 0);
  const hi = [...days].sort((x, z) => z[1] - x[1])[0], lo = [...days].sort((x, z) => x[1] - z[1])[0];
  const lwg = lw && lw.g != null ? `Last week ${sg(lw.g)}.` : '';
  S.L.followers = {
    days,
    head: t10.length >= 3 ? { num: sg(Math.round(a)), unit: 'followers in the 48h after a Top 10% post', line: `${sg(Math.round(b))} after the rest of the 100.`, aside: `${sg(tot)} over the ${days.length} days. ${lwg}`.trim(), tone: a > b * 1.5 ? 'beating' : 'plain' }
      : { num: sg(tot), unit: `followers, ${day(d0)} – ${day(days[days.length - 1][0])}`, line: lwg, aside: '', tone: tot > 0 ? 'beating' : 'plain' },
    rows: [
      t10.length ? { key: 't10', fa: a, fb: b, rest: sg(Math.round(b)), title: 'After a Top 10% post', sub: `${t10.length} posts, ${sg(Math.round(a))} typical in the 48h after`, ids: t10, metric: 'f48', head: { num: sg(Math.round(a)), unit: `followers in the 48h after a Top 10% post · ${t10.length} of the 100`, line: `${sg(Math.round(b))} after the rest.`, aside: '', tone: 'beating' } } : null,
      dayRow(hi, 'best'), dayRow(lo, 'worst'),
    ].filter(Boolean),
  };

  // formats: the 100 post by post, and what each format lands
  const fm = ['Reels', 'Carousels', 'Photos'].map((f) => { const ids = W.filter((i) => P[i].fmt === f); return { key: f, title: f, ids: byRank(ids), n: ids.length, rank: rankOf(ids), wk: inWk(ids) }; }).filter((r) => r.n).sort((x, z) => z.n - x.n);
  const main = fm[0], second = fm[1];
  const inside = Object.entries(main.ids.reduce((m, i) => ((m[P[i].pat] = m[P[i].pat] || []).push(i), m), {})).map(([name, ids]) => ({ name, n: ids.length, rank: rankOf(ids) })).filter((x) => x.n >= 4 && x.rank != null).sort((x, z) => x.rank - z.rank);
  S.L.formats = {
    head: { num: `${main.n} of 100`, unit: `are ${main.title.toLowerCase()}`, line: `${main.title} land ${top(main.rank)}${second ? `; the ${second.n} ${second.title.toLowerCase()}, ${top(second.rank)}` : ''}.`, aside: insideOf(main.ids) ? `What goes in decides it. ${insideOf(main.ids)}` : '', tone: 'plain' },
    rows: fm.map((r) => ({ ...r, head: { num: `${r.n} of 100`, unit: r.title.toLowerCase(), line: `Typical post ${top(r.rank)}.`, aside: r.wk ? `${r.wk} from last week.` : 'None from last week.', tone: r.rank < 50 ? 'beating' : 'plain' } })),
  };
}
// a day's follows, and the posts behind them (that day's and the day before's)
function dayRow(d, kind) {
  const ids = byF48(S.win.filter((i) => A.posts[i].d === d[0] || A.posts[i].d === isoAdd(d[0], -1)));
  const nm = kind === 'best' ? 'best' : kind === 'worst' ? 'worst' : '';
  return { key: d[0], g: d[1], title: `${sg(d[1])} on ${WD[dow(d[0])]} ${day(d[0])}`, sub: nm ? `The ${nm} day in the 100` : '', ids, metric: 'f48', iso: d[0],
    head: { num: sg(d[1]), unit: `followers on ${WD[dow(d[0])]} ${day(d[0])}${nm ? `, the ${nm} day in the 100` : ''}`, line: ids.length ? `${ids.length} post${ids.length === 1 ? '' : 's'} that day and the day before.` : 'No posts that day or the day before.', aside: '', tone: d[1] >= 0 ? 'beating' : 'plain' } };
}
/* ---- read cards: every number leads to the posts behind it. A card's top is its evidence (a cut of the post that says
   it best, its beats playing like a gif; or its stills; for a side, the posts with it over the posts without; for a
   run, the run and the post that ended it), its bottom the number and its line ---- */
const OBJ = A.his === 'his' ? 'him' : A.his === 'her' ? 'her' : 'it';
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
const evidenceOf = (ids, metric, used) => { const c = metric ? null : cutOf(ids, used); return c ? cutHTML(c) : ids.length ? stillsHTML(ids, metric || 'xlk', used) : ''; };
const typical = (ids, key, used, n) => { const m = rankOf(ids, key); return fresh([...ids].filter((i) => A.posts[i][key] != null).sort((a, b) => Math.abs(A.posts[a][key] - m) - Math.abs(A.posts[b][key] - m)), used, n); };
function vsHTML(r, used) { const k = r.metric === 'ct' ? 'xcm' : 'xlk'; return `<span class="vs"><span class="vsr">${typical(r.ids, r.metric, used, 2).map((id) => stillHTML(id, k)).join('')}<em>With it · ${fx(r.xa)} usual</em></span><span class="vsr wo">${typical(r.wo, r.metric, used, 2).map((id) => stillHTML(id, k)).join('')}<em>Without · ${fx(r.xo)} usual</em></span></span>`; }
function runHTML(r, used) {
  const run = r.run.slice(-12), big = r.brk || r.run[r.run.length - 1]; used.add(big);
  return `<span class="run"><span class="rst">${run.map((id) => `<i data-cv="${id}" style="${coverStyle(id)}"></i>`).join('')}</span><span class="rbk">${(A.posts[big].rp || []).length >= 2 ? cutHTML(big, 'x' + r.k) : stillHTML(big, 'x' + r.k)}<em>${r.brk ? 'Ended it' : 'Latest'}</em></span></span>`;
}
// the inside of a set: its best and worst slice, three posts or more each
function insideOf(ids) {
  const g = Object.entries(ids.reduce((m, i) => ((m[A.posts[i].pat] = m[A.posts[i].pat] || []).push(i), m), {})).map(([name, x]) => ({ name, n: x.length, m: xSet(x, 'lk') })).filter((x) => x.n >= 3 && x.m != null).sort((a, b) => b.m - a.m);
  return g.length >= 2 ? `Inside: ${low(g[0].name)} ${fx(g[0].m)}, ${low(g[g.length - 1].name)} ${fx(g[g.length - 1].m)}.` : '';
}
function readCards() {
  const P = A.posts, his = A.his, nwk = S.wk.size, used = new Set();
  const bestX = (ids, k = 'lk') => [...ids].filter((i) => P[i][k] != null).sort((a, b) => P[b][k] - P[a][k])[0];
  // the panel's headline for a row is its card's: the multiple, what it's of, and the line
  const head = (c) => ({ num: c.num, unit: `${c.unit} · ${c.kick.toLowerCase()}${c.count ? `, ${c.count}` : ''}`, line: c.line, aside: c.aside, tone: c.tone });

  // slices: each one's typical likes against the usual post, best first; comments and plays as context; its rank, quietly
  const SL = S.L.slices;
  SL.rows.forEach((r) => { r.x = xSet(r.ids, 'lk'); });
  SL.rows.sort((a, b) => (b.x ?? -1) - (a.x ?? -1));
  SL.rows.forEach((r) => { const b = bestX(r.ids); r.card = { kick: r.title, count: `${r.n} posts · ${top(r.rank)}`, num: fx(r.x), unit: `${his} usual likes`, line: ctxLine(r.ids, ['cm', 'pl']), aside: b ? `Best: ${lab(b)}, ${fx(xPost(b, 'lk'))} likes.` : '', tone: tx(r.x), media: evidenceOf(r.ids, null, used) }; r.head = head(r.card); });
  const bs = SL.rows.find((g) => g.n >= 4 && g.x != null) || SL.rows[0], bg = [...SL.rows].sort((x, z) => z.n - x.n)[0];
  SL.hl = bg !== bs && bg.x ? `${esc(cap1(low(bs.title)))} gets ${em_(fx(bs.x / bg.x))} the likes of ${esc(low(bg.title))}, ${his} biggest slice.` : `${esc(cap1(low(bs.title)))} leads ${his} slices on likes.`;
  SL.sub = SL.head.aside;

  // sides: the posts with it against the posts without, on likes (on comments where that's where it shows)
  const SD = S.L.sides, kOf = (r) => (r.metric === 'ct' ? 'cm' : 'lk');
  SD.rows.forEach((r) => { const k = kOf(r), a = xSet(r.ids, k), o = xSet(r.wo, k); r.xa = a; r.xo = o; r.x = a != null && o ? a / o : null; });
  SD.rows.sort((a, b) => (b.x ?? 0) - (a.x ?? 0));
  const vsWhat = (r) => (r.slice ? `${low(r.slice)} without it` : 'posts without it');
  SD.rows.forEach((r) => {
    const k = kOf(r), other = (k === 'lk' ? ['cm', 'pl'] : ['lk']).map((q) => { const a = xSet(r.ids, q), o = xSet(r.wo, q); return a != null && o ? `${KEYN[q]} ${fx(a / o)}` : null; }).filter(Boolean);
    r.card = { kick: r.title, count: r.slice ? `in ${low(r.slice)}` : `${r.n} posts`, num: fx(r.x), unit: `the ${KEYN[k]} of ${vsWhat(r)}`, line: other.length ? `${cap1(other.join(' · '))} too.` : '', aside: `${top(r.w)} with it, ${top(r.o)} without${k === 'cm' ? ', on comments' : ''}. ${r.wk ? `${r.wk} of last week’s ${nwk} had it.` : `None of last week’s ${nwk} had it.`}`, tone: tx(r.x), media: vsHTML(r, used) };
    r.head = head(r.card);
  });
  const lift = SD.rows.find((r) => r.x != null && r.x > 1), drag = [...SD.rows].reverse().find((r) => r.x != null && r.x < 1), nm = (r) => `${esc(cap1(low(r.title)))}${r.slice ? ` in ${esc(low(r.slice))}` : ''}`;
  const nl = SD.rows.filter((r) => r.x != null && r.x > 1).length, nd = SD.rows.filter((r) => r.x != null && r.x < 1).length;
  SD.hl = SD.rows.length ? `${em_(`${nl} lift${nl === 1 ? 's' : ''}`)} ${his === 'its' ? 'it' : OBJ}, ${em_(`${nd} drag${nd === 1 ? 's' : ''}`)}.${lift ? ` The biggest lift: ${esc(low(lift.title))}${lift.slice ? ` in ${esc(low(lift.slice))}` : ''}.` : ''}` : 'No side moves the current 100 by much.';
  SD.sub = SD.fake;

  // streaks: what the run was on, how far over or under it ran on each count, and what ended it
  const ST = S.L.streaks, kn = (ks) => listOf(ks.map((k) => KEYN[k]));
  ST.rows.forEach((r) => {
    const own = xSet(r.run, r.k), others = ['lk', 'cm', 'pl'].filter((k) => !r.ks.includes(k)).map((k) => { const m = xSet(r.run, k); return m == null ? null : `${KEYN[k]} ${fx(m)}`; }).filter(Boolean);
    r.card = { kick: dr(r.a, r.b), count: '', num: `${r.n}`, unit: `in a row ${r.up ? 'over' : 'under'} ${his} usual ${kn(r.ks)}`, line: `${fx(own)} over the run.${others.length ? ` ${cap1(others.join(', '))} alongside.` : ''}`, aside: r.brk ? `Ended by ${lab(r.brk)}.` : '', tone: r.up ? 'beating' : 'plain', media: runHTML(r, used) };
    r.head = head(r.card);
    if (!r.brk) r.head.unit += ', still running';
  });
  const live = ST.rows.find((r) => !r.brk), nup = ST.rows.filter((r) => r.up).length;
  ST.hl = ST.rows.length ? `${em_(`${ST.rows.length} run${ST.rows.length === 1 ? '' : 's'}`)} of six or more in the 100: ${em_(`${nup} over`)} ${his} usual, ${em_(`${ST.rows.length - nup} under`)}.${live ? ` The one on ${esc(kn(live.ks))} is still running.` : ''}` : 'Nothing in the 100 ran six in a row over or under the usual.';
  ST.sub = `Counted against ${his} usual post, on likes and on comments.`;

  // followers: the follows after the best posts against the rest; a day against a usual day
  const FL = S.L.followers;
  FL.rows.filter((r) => r.key !== 't10').forEach((r) => {
    const b = r.ids[0], xd = B.day > 0 && r.g > 0 ? r.g / B.day : null, xb = b ? xPost(b, 'lk') : null;
    if (b) used.add(b);
    r.card = { kick: `${WD[dow(r.iso)]} ${day(r.iso)}`, count: r.sub.replace(/^The /, '').replace(' in the 100', ''), num: xd != null ? fx(xd) : sg(r.g), unit: xd != null ? 'a usual day’s follows' : 'followers that day', line: xd != null ? `${sg(r.g)} that day; a usual day, ${sg(Math.round(B.day))}.` : `A usual day: ${sg(Math.round(B.day))}.`, aside: b ? `Behind it: ${lab(b)}${xb != null ? `, ${fx(xb)} likes` : ''}.` : 'No post that day or the day before.', tone: r.g >= 0 ? 'beating' : 'plain', media: b ? ((P[b].rp || []).length >= 2 ? cutHTML(b) : stillsHTML(r.ids, 'xlk')) : `<span class="rce">${esc(day(r.iso))}</span>` };
    r.head = head(r.card);
  });
  const t = FL.rows.find((r) => r.key === 't10'), tr = t && t.fb > 0 ? t.fa / t.fb : null;
  if (t) { t.card = { kick: `After ${his} best posts`, count: `${t.ids.length} posts · Top 10%`, num: tr != null ? fx(tr) : sg(Math.round(t.fa)), unit: tr != null ? 'the follows after the rest' : 'follows in the 48h after', line: `${sg(Math.round(t.fa))} in the 48 hours after, against ${sg(Math.round(t.fb))}.`, aside: '', tone: 'beating', media: stillsHTML(t.ids, 'f48', used) }; t.head = head(t.card); }
  const tot = FL.days.reduce((q, x) => q + x[1], 0), lwk = A.weeks.find((w) => w.s === '19 Oct');
  FL.hl = `${em_(sg(tot))} followers over the ${FL.days.length} days of the 100${lwk && lwk.g != null ? `; last week, ${em_(sg(lwk.g))}` : ''}.`;
  FL.sub = '';

  // formats: each format's typical likes against the usual post
  const FM = S.L.formats;
  FM.rows.forEach((r) => { r.x = xSet(r.ids, 'lk'); r.card = { kick: r.title, count: `${r.n} of 100 · ${top(r.rank)}`, num: fx(r.x), unit: `${his} usual likes`, line: ctxLine(r.ids, ['cm', 'pl']), aside: insideOf(r.ids), tone: tx(r.x), media: evidenceOf(r.ids, null, used) }; r.head = head(r.card); });
  const mn = FM.rows[0], sec = FM.rows[1];
  FM.hl = `${em_(`${mn.n} of the 100`)} are ${esc(mn.title.toLowerCase())}${sec ? `, ${em_(`${sec.n}`)} ${esc(sec.title.toLowerCase())}` : ''}.`;
  FM.sub = FM.head.aside;
}
// (each card sits in a wrapper the reel snaps to: the card glides on scroll, its wrapper stays put)
function readCard(key, i, r) {
  const c = r.card;
  return `<span class="rcw" style="--i:${Math.min(i, 4)}"><button class="rc" data-lens="${key}" data-row="${i}"><span class="rcm">${c.media}</span><span class="rct"><span class="rck"><b>${esc(c.kick)}</b>${c.count ? `<em>${esc(c.count)}</em>` : ''}</span><span class="rcn${c.tone === 'beating' ? ' up' : ''}">${esc(c.num)}</span><span class="rcu">${esc(c.unit)}</span><span class="rcl">${esc(c.line)}</span>${c.aside ? `<span class="rca">${esc(c.aside)}</span>` : ''}</span></button></span>`;
}
function lensSection(key) {
  const [, name, sub] = LENS.find((l) => l[0] === key), L = S.L[key];
  return `<article class="ls ls-${key}" id="${key}"><header class="lh"><span class="lk"><b>${name}</b><span>${sub}</span></span><p class="lhl">${L.hl}</p>${L.sub ? `<p class="lhs">${esc(L.sub)}</p>` : ''}
      ${L.rows.length > 1 ? `<span class="lnv"><button class="nb" data-rl="-1" aria-label="Back">${SV('M15 5l-7 7 7 7')}</button><button class="nb" data-rl="1" aria-label="More">${SV('M9 5l7 7-7 7')}</button></span>` : ''}</header>
    <div class="rl">${L.rows.map((r, i) => readCard(key, i, r)).join('')}</div></article>`;
}
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

/* =====================================================================
   SPLIT: the wall, newest first or in rank groups; a pick lights its trail
   ===================================================================== */
let WV = localGet('wall') === 'ranks' ? 'ranks' : 'recent', WPICK = null, TRT = [];
const RANKG = [
  { l: 'Top 10%', f: (v) => v != null && v <= 10, c: 'hot' }, { l: 'Top 25%', f: (v) => v != null && v > 10 && v <= 25, c: 'hot' }, { l: 'Top half', f: (v) => v != null && v > 25 && v <= 50, c: '' },
  { l: 'Below the middle', f: (v) => v != null && v > 50 && v <= 75, c: '' }, { l: 'Bottom 25%', f: (v) => v != null && v > 75, c: '' }, { l: 'Too new to rank', f: (v) => v == null, c: 'new' },
];
function splitHTML() {
  const L = A.lead.split, b = A.posts[L.best], w = A.posts[L.worst], nrank = A.wall.filter((i) => A.posts[i].top != null).length;
  const f = { num: L.num, unit: L.unit, line: `The best was ${lab(L.best)}, at ${fx(xPost(L.best, 'lk'))} ${A.his} usual likes; the lowest, ${lab(L.worst)}, at ${fx(xPost(L.worst, 'lk'))}.`, aside: A.wall.length > nrank ? `${A.wall.length - nrank} are too new to rank.` : '', tone: 'plain' };
  return `<header class="op"><span class="ok"><b>03</b>Split · the last ${A.wall.length} posts</span></header><div class="cap capx"><div class="w1"><div class="c">${headHTML(f)}</div></div><div class="w2"><div class="c">${bodyHTML(f)}</div></div></div>
    <div class="wbar">${segHTML('wv', [['recent', 'Recent'], ['ranks', 'Ranks']], WV)}<span class="wh" id="wh">${WV === 'recent' ? 'Newest first. Pick a post to see what it beat.' : 'Grouped by where each landed.'}</span></div>
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
  else { const said = new Set(); wg.className = ''; wg.innerHTML = RANKG.map((g) => { const ids = byRank(A.wall.filter((i) => g.f(A.posts[i].top))); if (!ids.length) return ''; return `<section class="rgp ${g.c}"><div class="rh"><b class="rl">${g.l}</b><span class="rm">${ids.length} post${ids.length > 1 ? 's' : ''}${likesRange(ids)}</span></div><p class="rw">${writeUp(ids, g, said)}</p><div class="wgrid">${ids.map(wallTile).join('')}</div></section>`; }).join(''); }
  if (WPICK) { wg.querySelector(`.wt[data-post="${WPICK}"]`)?.classList.add('sel'); if (WV === 'recent') playTrail(WPICK, true); }
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
  ro.classList.add('on');
}
function unpick() { WPICK = null; clearTrail(); qa('#wall .wt.sel').forEach((t) => t.classList.remove('sel')); $('ro').classList.remove('on'); }

/* =====================================================================
   THE PANEL: a slide's posts, or a post's mortem; it steps through its list and keeps what's behind it in step
   ===================================================================== */
const PN = { open: false, view: null, back: null };
function openRow(lens, i) { PN.back = null; PN.view = { kind: 'row', lens, i }; showPanel(); }
function openPost(pid, list) { const back = PN.open && PN.view && PN.view.kind !== 'post' ? PN.view : null; PN.back = back; PN.view = { kind: 'post', id: pid, list: list && list.includes(pid) ? list : [pid] }; showPanel(); }
function showPanel() { renderPanel(0); PN.open = true; $('pn').classList.add('on'); $('pns').classList.add('on'); markRow(); pickCut(); }
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
  const p = A.posts[id], v = xk === 'f48' ? null : xPost(id, xk.slice(1));
  const m = xk === 'f48' ? (p.f48 != null ? `${sg(p.f48)} followers in 48h` : 'Followers not counted') : v == null ? (p.top == null && p.d >= isoAdd(A.posts[A.order[A.order.length - 1]].d, -7) ? 'Too new to count' : 'Not counted') : `${fx(v)} ${KEYN[xk.slice(1)]}`;
  const up = xk === 'f48' ? p.f48 > 0 : v != null && v >= 1;
  return `<button class="ppr${tag ? ' mk' : ''}" data-post="${id}"><span class="ppc" data-cv="${id}" style="${coverStyle(id)}"></span><span class="ppt"><span class="pph"><b${up ? ' class="up"' : ''}>${esc(m)}</b><em>${day(p.d)} · ${cap1(FMT1[p.fmt] || 'post')}${tag ? ` · ${esc(tag)}` : ''}</em></span><span class="ppd">${esc(saidOf(p))}</span></span></button>`;
}
// a set of posts, each said in a line: all of them up to eight; past that the four best and the three worst in words,
// and the rest as covers (no post shows twice)
function postListHTML(label, sub, ids, xk, tags = {}) {
  if (!ids.length) return '';
  const counted = xk === 'f48' ? ids.filter((x) => A.posts[x].f48 != null) : ids.filter((x) => xPost(x, xk.slice(1)) != null);
  const many = counted.length > 8, best = many ? counted.slice(0, 4) : counted, worst = many ? counted.slice(-3) : [], words = [...best, ...worst], rest = ids.filter((x) => !words.includes(x));
  const lines = (list) => list.map((id) => postRowHTML(id, xk, tags[id])).join('');
  return `<section class="pps"><h5>${esc(label)}<em>${esc(sub)}</em></h5>
    <div class="ppl">${lines(best)}${many ? `<p class="ppg">${counted.length - 7} more between them</p>${lines(worst)}` : ''}</div>
    ${rest.length ? `<p class="pl2">The other ${rest.length}</p>${gridHTML(rest, { metric: xk })}` : ''}</section>`;
}
function rowView(lens, i) {
  const r = S.L[lens].rows[i], P = A.posts, k = r.metric === 'ct' ? 'cm' : 'lk', xk = r.metric === 'f48' ? 'f48' : 'x' + (r.k || k);
  const most = (ids, kk = k) => [...ids].sort((a, b) => (P[b][kk] ?? -1) - (P[a][kk] ?? -1));
  const ids = r.run ? r.ids : r.metric === 'f48' ? byF48(r.ids) : most(r.ids);
  const lw = ids.filter((x) => S.wk.has(x)).length, fol = medOf(ids.map((x) => P[x].f48)), xl = xSet(ids, 'lk'), xc = xSet(ids, 'cm'), h = { ...r.head };
  // the posts below name the best one, the one that ended a run and the one behind a day, so the head doesn't
  if (/^(Best:|Ended by|Behind it:)/.test(h.aside || '')) h.aside = '';
  const said = [h.num, h.unit, h.line, h.aside].join(' ').toLowerCase(), has = (w) => said.includes(w);
  const stats = [
    xl != null && !has('likes') ? [fx(xl), `${A.his} usual likes`] : null,
    xc != null && !has('comments') ? [fx(xc), 'usual comments'] : null,
    fol != null && lens !== 'followers' ? [sg(Math.round(fol)), 'followers in 48h, typical'] : null,
    lw && !has('last week') ? [lw === ids.length && ids.length > 1 ? 'All' : `${lw}`, 'from last week'] : null,
  ].filter(Boolean);
  // what happened across these posts, and why they land where they do: written from every post's own description
  const rd = (A.reads || {})[`${lens}:${r.key}`];
  const kn = KEYN[k], lists = r.wo
    ? postListHTML('With it', `${r.ids.length}, most ${kn} first`, most(r.ids), xk) + postListHTML('Without it', `${r.wo.length}, most ${kn} first`, most(r.wo), xk)
    : r.run ? postListHTML('The run', r.brk ? 'in order, and the post that ended it' : 'in order, still going', ids, xk, r.tag || {})
    : r.metric === 'f48' ? postListHTML(r.key === 't10' ? 'The posts' : 'That day and the day before', 'most followers first', ids, 'f48')
    : postListHTML('The posts', `${ids.length}, most ${kn} first`, ids, xk);
  return `<div class="pv-t">${capHTML(h)}
    ${stats.length ? `<div class="pst">${stats.map(([b, e]) => `<span><b>${b}</b><em>${e}</em></span>`).join('')}</div>` : ''}
    ${rd ? `<section class="pwh"><h5>What happened</h5><p class="pw1">${esc(rd.what)}</p><p class="pw2"><b>Why</b>${esc(rd.why)}</p></section>` : ''}</div>
    ${lists}`;
}
function postKicker(id) { const p = A.posts[id]; return `${WD[new Date(p.at).getDay()]} ${day(p.d)}`; }

/* ---- the post mortem: the breakdown (each frame: where it sits, what happens there, what was said), what it was going
   for, how it landed and why, and against its closest posts ---- */
const PEOPLE = { 0: 'nobody on screen', 1: 'one person on screen', 2: 'two people on screen', 3: 'three or more on screen' };
const STYLE = { talking: 'talking to camera', skit: 'an acted skit', vlog: 'a vlog', photo: 'a posed photo', graphic: 'a designed graphic', meme: 'a meme', product: 'a product shot', event: 'an event' };
function postView(id) {
  const p = A.posts[id], v = p.top, his = A.his, r = beatOf(id, A.order), bk = p.bk || null;
  const s = A.slices.find((x) => x.name === p.pat), wk = A.weeks.find((w) => w.ids.includes(id)), wr = wk ? byRank(wk.ids).indexOf(id) + 1 : 0;
  const xl = xPost(id, 'lk');
  const head = { num: xl != null ? fx(xl) : v != null ? top(v) : 'New', unit: xl != null ? `${his} usual likes` : v != null ? `among ${his} posts by day 7` : 'ranked at day 7', line: `${r.t}.`, aside: `${v != null ? `${top(v)} by day 7` : 'Too new to rank'}${wk && wr ? (wr === 1 ? ', the best of its week' : `, the ${ord(wr)} of ${wk.ids.length} that week`) : ''}.`, tone: tx(xl) };
  const fr = p.rp || [], sq = p.fmt !== 'Reels';
  const beats = fr.length ? `<div class="rpl">${fr.map((f, k) => { const b = bk && bk.frames && bk.frames[k]; return `<div class="rf${sq ? ' sq' : ''}"><span class="im" style="background-image:url(${BASE}${f.f})">${f.at ? `<span>${esc(f.at)}</span>` : ''}</span><span class="rft">${b ? `<b class="bl">${esc(b.line)}</b>` : ''}<span class="q${f.kind === 'shows' ? ' sh' : ''}">${esc(f.line)}</span></span></div>`; }).join('')}</div>`
    : `<div class="rpl"><div class="rf sq"><span class="im" style="${coverStyle(id)}"></span><span class="q sh">Only the cover is on file: frames start with the posts fingerprinted from ${A.acct === 'anuj' ? '19' : '21'} Sep.</span></div></div>`;
  const f = p.fx || {}, bits = [PEOPLE[f.people], f.shot === 'self' ? 'self-shot' : f.shot === 'other' ? 'filmed by someone else' : null, STYLE[f.style], f.hook ? 'words on the cover' : null, f.known ? 'a famous face' : null].filter(Boolean);
  const xs = (k) => (xPost(id, k) != null ? `${fx(xPost(id, k))} usual` : '');
  const nums = [[p.lk != null ? K(p.lk) : '–', 'likes', xl != null ? (p.lt != null ? `${top(p.lt)} on likes` : '') : xs('lk')], [p.cm != null ? K(p.cm) : '–', 'comments', [xs('cm'), p.ct != null ? top(p.ct) : ''].filter(Boolean).join(' · ')],
    p.pl ? [K(p.pl), 'plays', [xs('pl'), p.lk ? `a like every ${Math.round(p.pl / p.lk)}` : ''].filter(Boolean).join(' · ')] : null, [p.f48 != null ? sg(p.f48) : '–', 'followers', `in the 48h after${B.f48 ? ` · usual ${sg(Math.round(B.f48))}` : ''}`]].filter(Boolean);
  const lv = [];
  const sx = xSet(A.order.slice(-100).filter((i) => A.posts[i].pat === p.pat), 'lk');
  if (sx != null) lv.push({ d: sx >= 1 ? 'up' : 'dn', t: `${p.pat}`, s: `${cap1(his)} ${low(p.pat)} posts typically get ${fx(sx)} ${his} usual likes; this one ${fx(xl)}.`, g: `${fx(sx)} usual` });
  A.sides.forEach((g) => g.rows.forEach((x) => {
    if (!x.wi.includes(id) || (x.slice && x.slice !== p.pat)) return;
    const fake = /real/.test(g.name), better = x.w < x.wo;
    const k = x.key === 'ct' ? 'cm' : 'lk', ma = xSet(x.wi, k), mo = xSet(x.wo_ids, k), m = ma != null && mo ? ma / mo : null;
    lv.push({ d: fake ? 'no' : better ? 'up' : 'dn', t: `${x.name}${x.slice ? ` in ${low(x.slice)}` : ''}${x.key === 'ct' ? ', on comments' : ''}`, s: fake ? `It looks like it matters${m != null ? ` (${fx(m)} the ${KEYN[k]} of posts without it)` : ''}, but inside each slice the gap is gone.` : m != null ? `Posts with it get ${fx(m)} the ${KEYN[k]} of posts without it.` : `Posts with it land ${top(x.w)}, without ${top(x.wo)}.`, g: fake ? 'not real' : m != null ? fx(m) : `${Math.abs(x.wo - x.w)} pts ${better ? 'lift' : 'drag'}` });
  }));
  const near = (bk && bk.near) || [];
  const going = bk ? bk.going_for : p.prem || '';
  const what = p.sum ? esc(p.sum) : p.cap ? `“${esc(p.cap)}”` : esc(cap1(bits.join(', ')));
  return `<div class="pm-hd"><p class="pm-k">${esc(`${p.at.slice(11, 16)} · ${cap1(FMT1[p.fmt] || 'post')} · ${p.pat}`)}</p><div class="cap capx"><div class="w1"><div class="c" data-tone="${head.tone}">${headHTML(head)}</div></div><div class="w2"><div class="c">${bodyHTML(head)}</div></div></div><p class="pm-w">${what}</p></div>
    <section class="pms2"><h5>The breakdown<em>${fr.length ? `${fr.length} frame${fr.length === 1 ? '' : 's'}` : 'the cover'}</em></h5>${beats}</section>
    ${going ? `<section class="pms2"><h5>What it was going for</h5><p class="pm-p">${esc(going)}</p>${bits.length ? `<p class="pm-q">On screen: ${esc(bits.join(', '))}.</p>` : ''}</section>` : ''}
    <section class="pms2"><h5>How it landed<em>and why</em></h5><div class="pmn">${nums.map(([b, e, x]) => `<div><b>${b}</b><em>${e}</em>${x ? `<span>${x}</span>` : ''}</div>`).join('')}</div>
      ${bk && bk.landed && bk.landed.length ? `<div class="pm-l">${bk.landed.map((x) => `<p>${esc(x)}</p>`).join('')}</div>` : ''}
      <div class="lvs">${lv.map((x) => `<div class="lv"><span class="ar2 ${x.d}">${x.d === 'up' ? '▲' : x.d === 'dn' ? '▼' : '·'}</span><span class="t2">${esc(x.t)}<span>${esc(x.s)}</span></span><span class="g2${x.d === 'up' ? ' up' : ''}">${esc(x.g)}</span></div>`).join('')}</div></section>
    ${near.length ? `<section class="pms2"><h5>Against its closest posts<em>same slice, same format</em></h5>${bk && bk.vs ? `<p class="pm-p">${esc(bk.vs)}</p>` : ''}<div class="pgrid">${near.map((i) => tileHTML(i)).join('')}</div></section>` : ''}
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
function storyArrive(k) {
  const dir = SK < 0 ? 0 : k > SK ? 1 : k < SK ? -1 : 0; SK = k; storyPlace();
  if (!dir || RM) return;
  const card = q1(`.card[data-k="${k}"]`); if (!card) return;
  [...card.querySelectorAll('.foot > .kick, .foot > .fig, .foot > .lrow, .md .plab')].forEach((el_, j) => el_.animate([{ transform: `translateX(${dir * 46}px)` }, { transform: 'none' }], { duration: 640 + j * 60, easing: 'cubic-bezier(.32, .72, 0, 1)' }));
}
/* ---- the story a laptop up: the ten cards stand side by side as spines, and the one on show opens wide, its words on
   the left and its picture on the right. Another picked opens as this one folds, in one move; its words slide in a
   beat behind it, one line after another ---- */
let DS = false;
function spineBg(s) {
  const m = s.media, it = m.items && m.items[0];
  if (it) return `background-image:url(${BASE}st/${it.id}.jpg)`;
  const id = (m.ids && m.ids[0]) || (s.reviews && s.reviews[0] && s.reviews[0].id) || (s.rows || []).flatMap((r) => r.ids || [])[0];
  return id ? coverStyle(id) : '';
}
function deskStoryHTML() {
  const best = A.grid.top.reduce((a, b) => (b[1] < a[1] ? b : a))[0], n = A.stories.length, no = (k) => String(k + 1).padStart(2, '0');
  return `<div class="ds" id="ds"><div class="dsr" id="dsr">${A.stories.map((s, k) => `<article class="dsc k-${s.media.kind}${k === 0 ? ' on' : ''}" data-k="${k}">
      <div class="dz">${mediaHTML(s, k)}</div>
      <div class="dtx"><div class="dhd"><span class="av" data-cv="${best}" style="${cv(best)}"></span><b>${esc(A.handle.replace('@', ''))}</b><em>${esc(chLabel(s))}</em><span class="dno">${no(k)} / ${n}</span></div>
        <div class="dft">${kickOf(s)}<div class="fig"><b class="num">${esc(s.num)}</b><span class="unit">${esc(s.unit)}</span></div><h3 class="line">${esc(s.line)}</h3>${s.media.kind === 'list' ? '' : `<div class="more">${moreHTML(s)}</div>`}</div></div>
      <button class="dsp" data-k="${k}" style="${spineBg(s)}" aria-label="${esc(KICK[s.ch] || s.ch)}"><b>${no(k)}</b><span>${esc(KICK[s.ch] || s.ch)}</span><i class="${s.ch.toLowerCase()}"></i></button></article>`).join('')}</div>
    <div class="dsb"><div class="dpr" id="igbars">${A.stories.map((s, k) => `<button data-k="${k}" aria-label="${esc(KICK[s.ch] || s.ch)}"><i><b></b></i></button>`).join('')}</div>
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
// the row moves as one: every card glides to its place, the one opening widens its window as the one folding narrows
// its own, and each spine rides its card's moving edge (tucked behind it once the card is open, out from under it as
// the card folds). Transforms and a clip only, so nothing reflows or fades while it moves; a pick mid-move carries on
// from wherever things are
let DSZ = { sw: 48, dw: 680, gap: 8 };
function deskPlace(k, move) {
  const { sw, dw, gap } = DSZ;
  const plan = [...qa('.dsc')].map((c, j) => {
    const sp = c.querySelector('.dsp'), cs = getComputedStyle(c), ss = getComputedStyle(sp);
    return { c, sp, from: { transform: cs.transform, clipPath: cs.clipPath }, sfrom: { transform: ss.transform },
      to: { transform: `translate3d(${j * (sw + gap) + (j > k ? dw - sw : 0)}px,0,0)`, clipPath: `inset(0px ${j === k ? 0 : dw - sw}px 0px 0px round 26px)` },
      st: { transform: `translate3d(${j === k ? dw : 0}px,0,0)` } };
  });
  plan.forEach(({ c, sp, from, sfrom, to, st }) => {
    c.getAnimations().forEach((a) => a.cancel()); sp.getAnimations().forEach((a) => a.cancel());
    Object.assign(c.style, to); Object.assign(sp.style, st);
    if (!move || RM || from.clipPath === 'none') return;
    c.animate([from, to], { duration: 780, easing: CARRY });
    sp.animate([sfrom, st], { duration: 780, easing: CARRY });
  });
}
function deskGo(k) {
  const n = A.stories.length; k = Math.max(0, Math.min(n - 1, k));
  const dir = k > at ? 1 : k < at ? -1 : 0; at = k; el = 0;
  if (!$('dsr')) return;
  deskPlace(k, dir !== 0);
  qa('.dsc').forEach((c, j) => c.classList.toggle('on', j === k));
  $('arrp').disabled = k === 0; $('arrn').disabled = k === n - 1;
  paintBars();
  const c = q1(`.dsc[data-k="${k}"]`); if (!c) return;
  layoutCard(c, k);
  if (dir && !RM) [c.querySelector('.dhd'), ...c.querySelectorAll('.dft > *')].forEach((e_, j) => e_ && e_.animate([{ transform: `translateX(${dir * 36}px)` }, { transform: 'none' }], { duration: 780 + j * 70, easing: 'cubic-bezier(.32, .72, 0, 1)' }));
}
function hookDesk() {
  const ds = $('ds');
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
  return `<header class="op"><span class="ok"><b>02</b>Strategy · the current 100</span><p class="ol">${esc(A.lead.strategy)}</p><p class="osb">The latest 100 posts, ${S.span}, read five ways. Every multiple is against ${A.his} usual post: ${K(Math.round(B.lk))} likes, ${K(Math.round(B.cm))} comments${B.pl ? `, ${K(Math.round(B.pl))} plays` : ''}.</p></header>
    <div class="lz">${LENS.map(([k]) => lensSection(k)).join('')}</div>`;
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
  baseBuild(); storyX();
  ios.forEach((o) => o.disconnect()); ios = [];
  root.innerHTML = `
    <section class="sec wrap" id="story"><header class="op"><span class="ok"><b>01</b>Story · week of 19–25 Oct</span><p class="ol">${esc(SAY[acct])}</p></header>${DS ? deskStoryHTML() : storyHTML()}</section>
    <section class="sec wrap" id="strategy">${strategyHTML()}</section>
    <section class="sec wrap" id="split">${splitHTML()}</section>
    <div class="wrap foot2"><p>${A.yard === 'likes' ? 'Top % here ranks likes: Lakmē’s overall rank leans on a follower count recorded wrong for 57 of its posts.' : `Top % is where a post lands among ${esc(A.own)} own posts, by day 7.`} A multiple compares a post (or the typical post of a set) with the usual post among the latest 100 that have reached day 7. Top 1% is the best, Top 50% the middle. ${A.dropped} posts with no cover on file are left out of every count. A side holds up when it survives a shuffle test inside each slice.</p>
      <p>Plays are Instagram’s own counts from a 27 Oct check. Followers are daily snapshots; “in the 48h after” runs from the day before a post to two days after. Breakdown frames are cut from each post at the beats its fingerprint timed; the lines on them are written by a model from the post’s own beats and checked against these counts.</p></div>`;
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
  if (sv_) { const seg = sv_.closest('.seg'), v = sv_.dataset.sv; seg.querySelectorAll('button').forEach((b) => b.setAttribute('aria-pressed', String(b === sv_))); placeSeg(seg); if (seg.dataset.seg === 'wv') { WV = v; localSet('wall', v); $('wh').textContent = v === 'recent' ? 'Newest first. Pick a post to see what it beat.' : 'Grouped by where each landed.'; drawWall(); } return; }
  const t = e.target.closest('[data-post]'); if (!t) return;
  if (t.dataset.wall) { wallPick(t.dataset.post); return; }
  if (t.closest('.wf')) { openPost(t.dataset.post, S.win); return; }
  const box = t.closest('.pgrid'); openPost(t.dataset.post, box ? [...box.querySelectorAll('[data-post]')].map((x) => x.dataset.post) : [t.dataset.post]);
});
on($('ro'), 'click', (e) => { if (e.target.closest('[data-open]')) openPost(e.target.closest('[data-open]').dataset.open, A.wall); if (e.target.closest('[data-unpick]')) unpick(); });
on(window, 'keydown', (e) => {
  if (!PN.open) { if (e.key === 'Escape' && WPICK) unpick(); return; }
  if (e.key === 'Escape') closePanel(); else if (e.key === 'ArrowRight') stepPanel(1); else if (e.key === 'ArrowLeft') stepPanel(-1); else return;
  e.preventDefault(); e.stopImmediatePropagation();
}, true);
on(window, 'resize', () => { clearTimeout(rt); rt = setTimeout(() => { if (dead) return; if (isDesk() !== DS) { render(); return; } sizePlayer(); measureMore(); go(at); qa('[data-wk16]').forEach((h) => { h.dataset.drawn = ''; }); const c = q1(`.card[data-k="${at}"], .dsc[data-k="${at}"]`); if (c) layoutCard(c, at); drawAll(); if (WPICK && WV === 'recent') playTrail(WPICK, true); }, 140); });
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
