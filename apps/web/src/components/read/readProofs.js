/* The proof a finding carries, drawn the way its kind needs (Read v5, Oct 21 2026): a twin puts both posts on one
   clock, a follower move shows the daily follower line with the posts pinned on their days, money shows each paid post
   as a share of the usual, a lane shows where its posts landed against the usual, an arc shows its posts in order hours
   apart, teasers join each teaser to its episode, a share splits every like in the window. The Feeder File's law pages
   and the run views both draw them (feederSystemEngine, readerEngine), so this knows nothing of either: it takes the
   proof's data (built by the worker's read_v5_to_page.py) and a context for the posts (ctx.cover, ctx.label, ctx.top),
   and every cover is a button with data-post, which opens that post's post mortem wherever it sits. Styled under .pf
   (readTab.css), on the read's own tokens, so it looks the same inside the map's card and the read's sheets.
   Each plays once, the first time it is in view (playProof), on the read's carry curve at the app's tempo; laid out
   again when its width changes (layProof). */

const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const n0 = (v) => Math.round(v).toLocaleString('en-US');
const sgn = (v) => (v > 0 ? `+${n0(v)}` : v < 0 ? `−${n0(-v)}` : '0');
const clock = (s) => `${Math.floor(s / 60)}:${String(Math.round(s % 60)).padStart(2, '0')}`;
const cls = (t) => (t <= 25 ? 'hot' : t <= 50 ? 'up' : 'dn');
const CARRY = 'cubic-bezier(.3, .3, .2, 1)';

/* ---------- links in a reading: what a line quotes opens where it lives ----------
   The words mark them as [label](P27) a post (its post mortem), [label](L2) a law (its page in the Feeder File),
   [label](run3) a run (its read), [label](node:Name) a thing in the Feeder File. A dropped law has no page: its label
   stays plain. Everything else is escaped. Each is an inline link, so a long one wraps with the line like any words (a
   button would hold its label on one line); the engines route the clicks and keep the '#' off the address (readerEngine:
   data-post, data-law, data-tropen, data-node; the Feeder File handles a law or a thing in place) */
const LINK = /\[([^\]]+)\]\((P\d\d|L\d+|D\d+|run\d+|node:[^)]+)\)/g;
export function linkText(s) {
  return esc(s).replace(LINK, (m, label, ref) => {
    if (/^P\d\d$/.test(ref)) return `<a href="#" class="ql" data-post="${ref}">${label}</a>`;
    if (/^L\d+$/.test(ref)) return `<a href="#" class="ql lw" data-law="${ref}">${label}</a>`;
    if (/^run\d+$/.test(ref)) return `<a href="#" class="ql rn" data-tropen="${ref.slice(3)}">${label}</a>`;
    if (ref.startsWith('node:')) return `<a href="#" class="ql nd" data-node="${ref.slice(5)}">${label}</a>`;
    return label;
  });
}
// the words without their links (an aria-label, a title)
export const plainText = (s) => String(s ?? '').replace(LINK, '$1');
// who a post was made with or for: a collab (it sits on the partner's feed too and shares one like count), Instagram's
// paid label, or a brand deal the read spotted with no label. dealWith says it after the tag: "with @x · paid"
export const dealOf = (p) => (p.co && p.co.length ? { k: 'co', w: 'Collab' } : p.paid ? { k: 'pd', w: 'Paid' } : p.deal ? { k: 'ad', w: 'Ad' } : null);
const handlesOf = (co) => (co.length === 1 ? co[0] : `${co.slice(0, -1).join(', ')} and ${co[co.length - 1]}`);
export const dealWith = (p) => (p.co && p.co.length ? `with ${handlesOf(p.co)}${p.paid ? ' · paid' : ''}` : p.paid ? 'partnership' : p.deal ? `for ${p.deal} · no paid label` : '');

/* ---------- each kind's picture ---------- */
const KIND = {
  twin(p, c) {
    const max = Math.max(...p.rows.map((r) => r.secs)), hi = Math.max(...p.rows.map((r) => r.likes));
    // the side that won is red (every row but the last unless the data says otherwise), the other ink
    const lo = (r, k) => (r.lo != null ? r.lo : k === p.rows.length - 1);
    const rows = p.rows.map((r, k) => `<button type="button" class="pf-tr ${lo(r, k) ? 'lo' : 'hi'}" data-post="${r.id}" aria-label="${esc(r.side)}: ${n0(r.likes)} likes, ${esc(r.mark)} at ${clock(r.at)} of ${clock(r.secs)}">
      <span class="pf-cv ${cls(r.top)}">${c.cover(r.id)}</span>
      <span class="pf-tb">
        <span class="pf-th"><em>${esc(r.side)}</em><b data-to="${r.likes}">${n0(r.likes)}</b><i>likes · Top ${r.top}%</i></span>
        <span class="pf-lk"><i style="--w:${Math.max(.012, r.likes / hi).toFixed(4)}"></i></span>
        <span class="pf-ck" style="--l:${(r.secs / max).toFixed(4)};--x:${(r.at / max).toFixed(4)};--t:${r.at}">
          <i class="pf-ck-t"></i><i class="pf-ck-m"></i>
          <span class="pf-ck-l"><b>${clock(r.at)}</b>${esc(r.mark)}</span><span class="pf-ck-e">${clock(r.secs)}</span>
        </span>
      </span>
    </button>`).join('');
    const won = p.rows.filter((r, k) => !lo(r, k)), lost = p.rows.filter((r, k) => lo(r, k)), a = won[0], b = lost[0] || p.rows[p.rows.length - 1];
    const span = (rs) => { const t = rs.map((r) => r.at), m0 = Math.min(...t), m1 = Math.max(...t); return m0 === m1 ? clock(m0) : `${clock(m0)}–${clock(m1)}`; };
    const foot = p.rows.length === 2 ? `One clock for both: ${esc(a.mark)} at ${clock(a.at)} against ${clock(b.at)}.` : `One clock for all: the ${won.length} that travelled get there at ${span(won)}; the other at ${clock(b.at)}, ${esc(b.mark)}.`;
    return { label: `${n0(a.likes)} likes against ${n0(b.likes)}: ${p.ratio} times`, cap: p.rows.length === 2 ? 'The same idea, twice' : `The same idea, ${p.rows.length} times`, side: `<b>${p.ratio}×</b> the likes`,
      body: `<div class="pf-rows">${rows}</div>`, foot };
  },
  followers(p, c) {
    const vals = p.days.map((d) => (d.gap ? 0 : d.v ?? d.sum ?? 0)), up = Math.max(1, ...vals), dn = Math.max(1, ...vals.map((v) => -v));
    const z = up / (up + dn), pin = Object.fromEntries(p.pins.map((x) => [x.d, x]));
    const cols = p.days.map((d) => {
      const v = d.gap ? null : d.v ?? d.sum, h = v == null ? 0 : Math.abs(v) / (up + dn);
      return `<span class="pf-fd${d.gap ? ' gap' : ''}${d.since ? ' since' : ''}${pin[d.d] ? ' pin' : ''}">${v == null ? '<i class="pf-fbar gap"></i>' : `<i class="pf-fbar ${v >= 0 ? 'up' : 'dn'}" style="--h:${h.toFixed(4)}"></i>`}</span>`;
    }).join('');
    // the posts ride above the chart, each on a stem down to its day (spread apart when their days are close: layProof)
    const pins = p.days.map((d, k) => [d, k]).filter(([d]) => pin[d.d]).map(([d, k]) => { const x = pin[d.d], v = d.v ?? d.sum;
      return `<button type="button" class="pf-fpin" data-post="${x.id}" style="--c:${k}" aria-label="${esc(c.label(x.id))}, ${esc(d.d)}: ${sgn(v)} followers"><span class="pf-cv ${cls(c.top(x.id))}">${c.cover(x.id)}</span><b class="${v >= 0 ? 'up' : 'dn'}">${sgn(v)}</b></button><i class="pf-fst" style="--c:${k}"></i>`; }).join('');
    const ax = [0, p.days.length - 1].map((k) => `<span style="--i:${k}">${esc(p.days[k].d)}</span>`).join('');
    const gaps = p.days.filter((d) => d.gap), since = p.days.find((d) => d.since);
    const note = gaps.length ? ` ${esc(gaps[0].d)}: no count${since ? `; ${esc(since.d)} shows ${sgn(since.sum)} since the last one` : ''}.` : '';
    return { label: `Followers gained or lost each day, ${p.days[0].d} to ${p.days[p.days.length - 1].d}`, cap: 'Followers gained or lost, day by day', side: '',
      body: `<div class="pf-fc" style="--n:${p.days.length};--z:${z.toFixed(4)}"><i class="pf-fz"></i><span class="pf-fy up">${sgn(up)}</span>${dn > 1 ? `<span class="pf-fy dn">${sgn(-dn)}</span>` : ''}<div class="pf-fcs">${cols}</div>${pins}</div><div class="pf-fx" style="--n:${p.days.length}">${ax}</div>`,
      foot: `Each bar is one day’s net change for the whole account; the covers sit on the days they went up.${note}` };
  },
  money(p, c) {
    const max = Math.max(120, ...p.rows.map((r) => r.pct)) * 1.04, x = (v) => (v / max).toFixed(4);
    const rows = p.rows.map((r, k) => `<button type="button" class="pf-mr${r.pct >= 95 ? ' held' : ''}" data-post="${r.id}" style="--i:${k}" aria-label="${esc(r.name)}: ${n0(r.likes)} likes, ${r.pct}% of a usual post">
      <span class="pf-cv sm ${r.pct >= 95 ? 'up' : 'dn'}">${c.cover(r.id)}</span><span class="pf-mn">${esc(r.name)}</span>
      <span class="pf-mt"><i style="--w:${x(Math.max(1.5, r.pct))}"></i></span><b data-to="${r.pct}">${r.pct}%</b>
    </button>`).join('');
    // a few posts get a count of the ones that held; enough of them, their typical share and its line
    const many = p.rows.length >= 6, held = p.rows.filter((r) => r.pct >= 95).length;
    return { label: many ? `Each paid post as a share of a usual post; typically ${p.median}%` : `${held} of ${p.rows.length} paid posts at the usual post`, cap: 'Each paid post, as a share of a usual post', side: many ? `<b>${p.median}%</b> typically` : `<b>${held}/${p.rows.length}</b> at ${c.who} usual`,
      body: `<div class="pf-m" style="--u:${x(100)};--md:${x(p.median)}"><i class="pf-mu"><span>${c.usual}</span></i>${many ? `<i class="pf-mm"><span>Typical ${p.median}%</span></i>` : ''}${rows}</div>`,
      foot: 'Likes against the usual post of the same format at the time.' };
  },
  lane(p, c) {
    const items = p.posts.map((q) => `<button type="button" class="pf-lc ${cls(q.top)}" data-post="${q.id}" style="--x:${((100 - q.top) / 99).toFixed(4)}" aria-label="${esc(c.label(q.id))}, Top ${q.top}%"><span class="pf-cv ${cls(q.top)}">${c.cover(q.id)}</span><b>Top ${q.top}%</b></button>`).join('');
    const over = p.posts.filter((q) => q.top <= 50).length;
    return { label: `${over} of ${p.posts.length} above the usual post`, cap: 'Where each landed', side: `<b>${over}/${p.posts.length}</b> above ${c.who} usual`,
      body: `<div class="pf-ln"><i class="pf-lt"></i><i class="pf-lu"><span>${c.usual}</span></i>${items}</div><div class="pf-la"><span>Bottom</span><span>Top</span></div>`,
      foot: 'Each post against its own format’s last 90 days.' };
  },
  arc(p, c) {
    const steps = p.steps.map((s, k) => `<div class="pf-as" style="--i:${k};--y:${((100 - s.top) / 99).toFixed(4)}">
      ${s.gap ? `<span class="pf-ag">${esc(s.gap)}</span>` : ''}
      <button type="button" class="pf-ac ${cls(s.top)}" data-post="${s.id}" aria-label="${esc(c.label(s.id))}, Top ${s.top}%"><span class="pf-cv ${cls(s.top)}">${c.cover(s.id)}</span><b>Top ${s.top}%</b></button>
      <span class="pf-aw"><b>${esc(s.tag)}</b>${esc(s.when)}</span>
    </div>`).join('');
    return { label: p.steps.map((s) => `${s.tag}, Top ${s.top}%`).join('; '), cap: 'In the order it went up', side: '',
      body: `<div class="pf-ar" style="--n:${p.steps.length}"><svg class="pf-al" aria-hidden="true"><polyline points=""/></svg><i class="pf-au"><span>${c.usual}</span></i>${steps}</div>`,
      foot: 'Higher is better: each post against its own format’s last 90 days.' };
  },
  split(p, c) {
    // each side's posts on a ledge at the height they typically landed (best at the top, worst at the bottom, the usual
    // across the middle), as the rules always showed them; the fall between the two is what the law is worth
    const med = (a) => { const v = [...a].sort((x, y) => x - y), m = v.length >> 1; return v.length ? (v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2) : null; };
    const y = (t) => ((Math.min(100, Math.max(1, t)) - 1) / 99).toFixed(4);
    const sides = [['hi', p.lands, p.sides.lands], ['lo', p.sinks, p.sides.sinks]].filter(([, xs]) => xs.length);
    const typ = Object.fromEntries(sides.map(([k, xs]) => [k, Math.round(med(xs.map((x) => x.top)))]));
    const broke = new Set(p.broke || []);
    const ledge = ([k, xs, lab]) => `<div class="pf-xl ${k}" style="--y:${y(typ[k])}">
      <span class="pf-dk" style="--n:${xs.length}">${xs.map((x, j) => `<button type="button" class="pf-dc ${cls(x.top)}${broke.has(x.id) ? ' broke' : ''}" data-post="${x.id}" style="--i:${j};z-index:${xs.length - j}" aria-label="${esc(c.label(x.id))}, Top ${x.top}%"><span class="pf-cv ${cls(x.top)}">${c.cover(x.id)}</span><b>Top ${x.top}%</b></button>`).join('')}</span>
      <i class="pf-xr"></i><span class="pf-xb"><b>${esc(lab)}</b> · ${xs.length} ${xs.length === 1 ? 'post' : 'posts'}, usually Top ${typ[k]}%</span>
    </div>`;
    const both = sides.length === 2, gap = both ? Math.round(((typ.lo - typ.hi) / 100) * (p.n || 30)) : 0;
    const above = [...p.lands, ...p.sinks].filter((x) => x.top <= 50).length, all = p.lands.length + p.sinks.length;
    return { label: both ? `${p.sides.lands}: usually Top ${typ.hi}%. ${p.sides.sinks}: usually Top ${typ.lo}%.` : `${all} posts, usually Top ${typ[sides[0][0]]}%`, cap: 'Where its posts landed',
      side: both ? `<b>${Math.abs(gap)}</b> ${Math.abs(gap) === 1 ? 'place' : 'places'} apart` : `<b>${above}/${all}</b> above ${c.who} usual`,
      body: `<div class="pf-xp${both ? '' : ' one'}" style="${both ? `--a:${y(typ.hi)};--b:${y(typ.lo)}` : ''}">
        <i class="pf-xg t"><span>${esc(c.best || 'Best')}</span></i><i class="pf-xg u"><span>${esc(c.usual)}</span></i><i class="pf-xg b"><span>${esc(c.worst || 'Worst')}</span></i>
        ${sides.map(ledge).join('')}
        ${both ? `<div class="pf-xw"><i class="pf-xd"></i></div>` : ''}
      </div>`,
      foot: both ? `Each post against its own format’s last 90 days, among ${c.who} last ${p.n || 30}.` : 'Each post against its own format’s last 90 days.' };
  },
  pairs(p, c) {
    const L = [...p.pairs].sort((a, b) => b.aLikes - a.aLikes), R = [...p.pairs].sort((a, b) => b.bLikes - a.bLikes);
    const side = (list, key, likes) => list.map((x, k) => `<button type="button" class="pf-pi ${x.off ? 'off' : ''}" data-post="${x[key]}" data-pair="${x.a}" style="--r:${k}" aria-label="${esc(c.label(x[key]))}, ${n0(x[likes])} likes"><span class="pf-cv sm ${cls(c.top(x[key]))}">${c.cover(x[key])}</span><b>${n0(x[likes])}</b></button>`).join('');
    const called = p.pairs.filter((x) => !x.off).length;
    return { label: `${called} of ${p.pairs.length} teasers called their episode`, cap: 'Each teaser, joined to its episode', side: `<b>${called}/${p.pairs.length}</b> called`,
      body: `<div class="pf-pr" style="--n:${p.pairs.length}"><div class="pf-pc l">${side(L, 'a', 'aLikes')}</div><svg class="pf-pl" aria-hidden="true"></svg><div class="pf-pc r">${side(R, 'b', 'bLikes')}</div></div><div class="pf-pa"><span>Teasers, most likes first</span><span>Episodes, most likes first</span></div>`,
      foot: 'A straight line: the teaser’s pull called its episode. A crossing: it didn’t.' };
  },
  share(p, c) {
    const total = p.parts.reduce((s, x) => s + x.likes, 0), on = p.parts.filter((x) => x.on), onL = on.reduce((s, x) => s + x.likes, 0);
    const segs = p.parts.map((x, k) => `<button type="button" class="pf-sg${x.on ? ' on' : ''}" data-post="${x.id}" style="--w:${(x.likes / total * 100).toFixed(3)}%;--i:${k}" aria-label="${esc(c.label(x.id))}, ${n0(x.likes)} likes"></button>`).join('');
    const pins = p.pins.map((id) => { const k = p.parts.findIndex((x) => x.id === id), at = p.parts.slice(0, k).reduce((s, x) => s + x.likes, 0) + p.parts[k].likes / 2; return `<button type="button" class="pf-sp" data-post="${id}" style="--x:${(at / total).toFixed(4)}"><span class="pf-cv sm hot">${c.cover(id)}</span><b>${n0(p.parts[k].likes)}</b></button>`; }).join('');
    return { label: `${p.label.on}: ${n0(onL)} of ${n0(total)} likes`, cap: 'Every like in the window', side: `<b>${Math.round(onL / total * 100)}%</b> ${esc(p.label.onShort || '')}`,
      body: `<div class="pf-sl"><span class="on"><b data-to="${onL}">${n0(onL)}</b>${esc(p.label.on)}</span><span><b data-to="${total - onL}">${n0(total - onL)}</b>${esc(p.label.off)}</span></div><div class="pf-sb">${segs}</div><div class="pf-sps">${pins}</div>`,
      foot: 'One bar, every post’s likes side by side.' };
  },
};

/** the proof as markup. p: the proof's data (p.kind picks the picture); c: { cover(id), label(id), top(id), who, usual } */
export function proofHTML(p, c) {
  const k = p && KIND[p.kind]; if (!k) return '';
  const ctx = { who: 'its', usual: 'The usual post', ...c };
  const v = k(p, ctx);
  return `<figure class="pf pf-${p.kind} pre" data-pf="${p.kind}" role="group" aria-label="${esc(v.label)}">
    <figcaption class="pf-cap"><span class="pf-k">${esc(v.cap)}</span>${v.side ? `<span class="pf-sd">${v.side}</span>` : ''}</figcaption>
    ${v.body}
    ${v.foot ? `<p class="pf-ft">${v.foot}</p>` : ''}
  </figure>`;
}

/* ---------- laying out what CSS alone can't: lanes that would overlap stack up, the arc's line, the pairs' lines ---------- */
export function layProof(el) {
  const kind = el.dataset.pf;
  if (kind === 'lane') {
    const box = el.querySelector('.pf-ln'), W = box.clientWidth, cs = [...box.querySelectorAll('.pf-lc')]; if (!W || !cs.length) return;
    const cw = cs[0].offsetWidth || 44, rows = [];
    const items = cs.map((b) => ({ b, x: Math.min(W - cw, Math.max(0, +b.style.getPropertyValue('--x') * W - cw / 2)) })).sort((a, b) => a.x - b.x);
    items.forEach((it) => { let r = 0; while (rows[r] != null && it.x < rows[r] + cw + 6) r++; rows[r] = it.x; it.b.style.left = `${it.x.toFixed(1)}px`; it.b.style.setProperty('--row', r); });
    box.style.setProperty('--rows', Math.max(1, rows.length));
  } else if (kind === 'followers') {
    const box = el.querySelector('.pf-fc'), W = box.clientWidth, n = +box.style.getPropertyValue('--n'), pins = [...box.querySelectorAll('.pf-fpin')], stems = [...box.querySelectorAll('.pf-fst')]; if (!W || !pins.length) return;
    const col = W / n, pw = pins[0].offsetWidth || 30, want = pins.map((b) => (+b.style.getPropertyValue('--c') + .5) * col), at = [...want];
    // spread the pins so none overlaps, keeping their order; each keeps a stem to its own day
    for (let it = 0; it < 60; it++) for (let j = 1; j < at.length; j++) { const d = at[j] - at[j - 1]; if (d < pw + 4) { const push = (pw + 4 - d) / 2; at[j - 1] -= push; at[j] += push; } }
    at.forEach((x, j) => { const cx = Math.min(W - pw / 2, Math.max(pw / 2, x)); pins[j].style.left = `${cx.toFixed(1)}px`; const s = stems[j]; if (s) { const a2 = want[j], dx = cx - a2; s.style.left = `${a2.toFixed(1)}px`; s.style.setProperty('--dx', `${dx.toFixed(1)}px`); } });
  } else if (kind === 'arc') {
    const box = el.querySelector('.pf-ar'), svg = box.querySelector('.pf-al'), r0 = box.getBoundingClientRect();
    const pts = [...box.querySelectorAll('.pf-ac .pf-cv')].map((cv) => { const r = cv.getBoundingClientRect(); return `${(r.left + r.width / 2 - r0.left).toFixed(1)},${(r.top + r.height / 2 - r0.top).toFixed(1)}`; });
    svg.setAttribute('viewBox', `0 0 ${r0.width.toFixed(0)} ${r0.height.toFixed(0)}`); svg.querySelector('polyline').setAttribute('points', pts.join(' '));
  } else if (kind === 'split') {
    // the usual's name moves to the other side when a ledge's caption sits on it (a narrow card puts them close)
    const u = el.querySelector('.pf-xg.u span'); if (!u) return; u.classList.remove('left');
    const hit = (a, b) => a.left < b.right && a.right > b.left && a.top < b.bottom && a.bottom > b.top;
    const caps = [...el.querySelectorAll('.pf-xb')].map((c) => c.getBoundingClientRect());
    if (caps.some((c) => hit(u.getBoundingClientRect(), c))) { u.classList.add('left'); if (caps.some((c) => hit(u.getBoundingClientRect(), c))) u.classList.add('gone'); else u.classList.remove('gone'); }
  } else if (kind === 'pairs') {
    const box = el.querySelector('.pf-pr'), svg = box.querySelector('.pf-pl'), r0 = svg.getBoundingClientRect(); if (!r0.width) return;
    const L = Object.fromEntries([...box.querySelectorAll('.pf-pc.l .pf-pi')].map((b) => [b.dataset.pair, b])), R = [...box.querySelectorAll('.pf-pc.r .pf-pi')];
    svg.setAttribute('viewBox', `0 0 ${r0.width.toFixed(0)} ${r0.height.toFixed(0)}`);
    svg.innerHTML = R.map((b) => { const a = L[b.dataset.pair]; if (!a) return ''; const ra = a.getBoundingClientRect(), rb = b.getBoundingClientRect(), y1 = ra.top + ra.height / 2 - r0.top, y2 = rb.top + rb.height / 2 - r0.top;
      return `<line class="${b.classList.contains('off') ? 'off' : ''}" x1="0" y1="${y1.toFixed(1)}" x2="${r0.width.toFixed(1)}" y2="${y2.toFixed(1)}"/>`; }).join('');
  }
}

/** plays the proof once: what it shows arrives the way its kind reads (bars grow, the clock runs, covers find their
 * places). anim(el, keyframes, { dur, delay, ease }) is the engine's (tempo and reduced motion are its business);
 * without it, or with reduced motion, the proof just shows */
export function playProof(el, anim) {
  el.classList.remove('pre');
  if (!anim) return;
  const qa = (s) => [...el.querySelectorAll(s)], kind = el.dataset.pf;
  const roll = (b, d0, dur = 900) => { const to = +b.dataset.to; if (!isFinite(to)) return; const t0 = performance.now() + d0 * (+getComputedStyle(el).getPropertyValue('--fm-tempo') || 1), D = dur * (+getComputedStyle(el).getPropertyValue('--fm-tempo') || 1), fmt = b.textContent.trim().endsWith('%') ? (v) => `${Math.round(v)}%` : (v) => n0(v);
    const step = (t) => { const u = Math.min(1, Math.max(0, (t - t0) / D)), e = 1 - Math.pow(1 - u, 3); b.textContent = fmt(to * e); if (u < 1) requestAnimationFrame(step); }; requestAnimationFrame(step); };
  anim(el.querySelector('.pf-cap'), [{ opacity: 0, transform: 'translateY(6px)' }, { opacity: 1, transform: 'none' }], { dur: 420 });
  if (kind === 'twin') {
    qa('.pf-tr').forEach((r, j) => {
      const d = 120 + j * 160;
      anim(r, [{ opacity: 0, transform: 'translateY(10px)' }, { opacity: 1, transform: 'none' }], { dur: 560, delay: d });
      const w = r.querySelector('.pf-lk i'); anim(w, [{ transform: 'scaleX(0)' }, { transform: `scaleX(${w.style.getPropertyValue('--w')})` }], { dur: 1000, delay: d + 200, ease: CARRY });
      roll(r.querySelector('.pf-th b'), d + 200, 1000);
      // both clocks start together and run at one speed, so the faster moment lands first, as it did on screen
      const ck = r.querySelector('.pf-ck'), t = +ck.style.getPropertyValue('--t'), x = ck.style.getPropertyValue('--x');
      anim(ck.querySelector('.pf-ck-t'), [{ transform: 'scaleX(0)' }, { transform: 'scaleX(1)' }], { dur: 480, delay: 760, ease: CARRY });
      const travel = -(+x) * ck.clientWidth;
      anim(ck.querySelector('.pf-ck-m'), [{ transform: `translateX(${travel.toFixed(1)}px)` }, { transform: 'none' }], { dur: Math.max(120, t * 60), delay: 1100, ease: 'linear' });
      anim(ck.querySelector('.pf-ck-l'), [{ opacity: 0, transform: 'translate(-50%, 4px)' }, { opacity: 1, transform: 'translate(-50%, 0)' }], { dur: 380, delay: 1100 + t * 60 });
    });
  } else if (kind === 'followers') {
    qa('.pf-fd').forEach((d, k) => {
      const b = d.querySelector('.pf-fbar'); if (b && !b.classList.contains('gap')) anim(b, [{ transform: 'scaleY(0)' }, { transform: 'scaleY(1)' }], { dur: 640, delay: 140 + k * 34, ease: CARRY });
    });
    qa('.pf-fpin').forEach((p, j) => { const k = +p.style.getPropertyValue('--c'); anim(p, [{ opacity: 0, transform: 'translate(-50%, -10px)' }, { opacity: 1, transform: 'translate(-50%, 0)' }], { dur: 520, delay: 520 + k * 34 }); const s = el.querySelectorAll('.pf-fst')[j]; if (s) anim(s, [{ opacity: 0 }, { opacity: 1 }], { dur: 300, delay: 560 + k * 34 }); });
  } else if (kind === 'money') {
    anim(el.querySelector('.pf-mu'), [{ opacity: 0 }, { opacity: 1 }], { dur: 420, delay: 80 });
    anim(el.querySelector('.pf-mm'), [{ opacity: 0 }, { opacity: 1 }], { dur: 420, delay: 160 });
    qa('.pf-mr').forEach((r, k) => {
      anim(r, [{ opacity: 0, transform: 'translateY(6px)' }, { opacity: 1, transform: 'none' }], { dur: 460, delay: 120 + k * 70 });
      const i = r.querySelector('.pf-mt i'); anim(i, [{ transform: 'scaleX(0)' }, { transform: `scaleX(${i.style.getPropertyValue('--w')})` }], { dur: 900, delay: 220 + k * 70, ease: CARRY });
      roll(r.querySelector(':scope > b'), 220 + k * 70, 900);
    });
  } else if (kind === 'lane') {
    anim(el.querySelector('.pf-lu'), [{ transform: 'scaleY(0)' }, { transform: 'scaleY(1)' }], { dur: 520, delay: 60, ease: CARRY });
    qa('.pf-lc').forEach((b, k) => anim(b, [{ opacity: 0, transform: `translateX(${-(parseFloat(b.style.left) || 0)}px)` }, { opacity: 1, offset: .25 }, { opacity: 1, transform: 'none' }], { dur: 900, delay: 160 + k * 90, ease: CARRY }));
  } else if (kind === 'arc') {
    qa('.pf-as').forEach((s, k) => anim(s.querySelector('.pf-ac'), [{ opacity: 0, transform: 'translateY(24px)' }, { opacity: 1, offset: .3 }, { opacity: 1, transform: 'none' }], { dur: 760, delay: 140 + k * 220, ease: 'cubic-bezier(.22, 1, .36, 1)' }));
    const pl = el.querySelector('.pf-al polyline'); const len = pl.getTotalLength ? pl.getTotalLength() : 0;
    if (len) { pl.style.strokeDasharray = `${len}`; anim(pl, [{ strokeDashoffset: len }, { strokeDashoffset: 0 }], { dur: 300 + qa('.pf-as').length * 220, delay: 360, ease: CARRY }); }
  } else if (kind === 'split') {
    qa('.pf-xl').forEach((l, j) => qa('.pf-dc', l).forEach((d, k) => anim(d, [{ opacity: 0, transform: `translateX(${-k * 60}%)` }, { opacity: d.classList.contains('broke') ? .55 : 1, offset: .3 }, { opacity: d.classList.contains('broke') ? .55 : 1, transform: 'none' }], { dur: 640, delay: 80 + j * 140 + k * 32, ease: 'cubic-bezier(.22, 1, .36, 1)' })));
    const hi = el.querySelector('.pf-xl.hi'), lo = el.querySelector('.pf-xl.lo'), w = el.querySelector('.pf-xw');
    if (hi && lo) {
      const dy = lo.offsetTop - hi.offsetTop;
      anim(lo, [{ transform: `translateY(${-dy}px)` }, { transform: 'none' }], { dur: 1050, delay: 640, ease: CARRY });
      if (w) anim(w.querySelector('.pf-xd'), [{ transform: 'scaleY(0)' }, { transform: 'scaleY(1)' }], { dur: 1050, delay: 640, ease: CARRY });
    }
    qa('.pf-xb').forEach((l) => anim(l, [{ opacity: 0 }, { opacity: 1 }], { dur: 420, delay: 1300 }));
  } else if (kind === 'pairs') {
    qa('.pf-pc.l .pf-pi').forEach((b, k) => anim(b, [{ opacity: 0, transform: 'translateX(-10px)' }, { opacity: 1, transform: 'none' }], { dur: 480, delay: 100 + k * 80 }));
    qa('.pf-pl line').forEach((l, k) => { const len = Math.hypot(+l.getAttribute('x2') - +l.getAttribute('x1'), +l.getAttribute('y2') - +l.getAttribute('y1')); l.style.strokeDasharray = l.classList.contains('off') ? '' : `${len}`; anim(l, l.classList.contains('off') ? [{ opacity: 0 }, { opacity: 1 }] : [{ strokeDashoffset: len }, { strokeDashoffset: 0 }], { dur: 640, delay: 460 + k * 140, ease: CARRY }); });
    qa('.pf-pc.r .pf-pi').forEach((b, k) => anim(b, [{ opacity: 0, transform: 'translateX(10px)' }, { opacity: 1, transform: 'none' }], { dur: 480, delay: 900 + k * 80 }));
  } else if (kind === 'share') {
    anim(el.querySelector('.pf-sb'), [{ clipPath: 'inset(0 100% 0 0 round 6px)' }, { clipPath: 'inset(0 0 0 0 round 6px)' }], { dur: 1100, delay: 120, ease: CARRY });
    qa('.pf-sl b').forEach((b) => roll(b, 120, 1100));
    qa('.pf-sp').forEach((b, k) => anim(b, [{ opacity: 0, transform: 'translate(-50%, -8px)' }, { opacity: 1, transform: 'translate(-50%, 0)' }], { dur: 480, delay: 900 + k * 90 }));
  }
}
