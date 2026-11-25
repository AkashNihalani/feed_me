/* ─────────────────────────────────────────────────────────────
   ME's engine — plain JS, mounted by Me.tsx on the page (root) and
   on a layer at body level (its sheet, its one line of news).

   Two layers: the card and buying behind, a sheet about your feeds
   in front. On a phone the document scrolls and the sheet rides up
   over the card; from a laptop up they stand side by side and the
   sheet scrolls in its own panel.

   Nothing travels on a pick: a line rolls inside its own line,
   digits inside their own places, dots and bars grow where they
   stand; only the circles of the rail move. The card's mark is the
   one thing that moves on its own, and only when Bitecoins do.

   It is told what it shows (feeds, the Bitecoin ledger, the posts:
   set), and tells the app what was asked (host).
   ───────────────────────────────────────────────────────────── */

import { Board } from './board.js';
import { Tones } from './sound.js';
import { EMOJI, GROUPS, ICONS } from './icons.js';

const IC = {"l": "<path d=\"m15 18-6-6 6-6\"/>", "r": "<path d=\"m9 18 6-6-6-6\"/>", "x": "<path d=\"M18 6 6 18M6 6l12 12\"/>", "plus": "<path d=\"M5 12h14M12 5v14\"/>", "sun": "<circle cx=\"12\" cy=\"12\" r=\"4\"/><path d=\"M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M6.3 17.7l-1.4 1.4M19.1 4.9l-1.4 1.4\"/>", "con": "<circle cx=\"12\" cy=\"12\" r=\"10\"/><path d=\"M12 18a6 6 0 0 0 0-12v12z\"/>", "moon": "<path d=\"M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z\"/>", "out": "<path d=\"M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4M16 17l5-5-5-5M21 12H9\"/>"};
const ic = (n) => `<svg class="i" viewBox="0 0 24 24" aria-hidden="true">${IC[n]}</svg>`;
const T = 1.35, CARRY = 'cubic-bezier(.3,.3,.2,1)';
const fmt = (n) => n.toLocaleString('en-US');
const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
const ALERTS_KEY = 'fm:me-alerts:v1';
const alerts0 = () => ({ base: { likes: { on: true, v: 5 }, comm: { on: true, v: 5 }, fol: { on: false, v: 1 }, gap: { on: true, v: 5 }, post: { on: false, v: 0 } }, own: {}, one: {}, when: 0, hush: true });
const readAlerts = () => { try { const a = JSON.parse(localStorage.getItem(ALERTS_KEY) || 'null'); return a && a.base && a.own && a.one ? a : alerts0(); } catch { return alerts0(); } };

const PAGE = () => `<section class="mst" id="mst">
        <header class="top"><h1><button class="meb" id="go-acc" aria-label="Me. Open account"><span class="av" id="av"></span><span class="tx"><b>Me</b><span id="me-s"></span></span></button></h1><div class="sg th" id="look" role="radiogroup" aria-label="Look" style="--n:3;--at:1"><button role="radio" data-th="light" aria-label="Light">${ic("sun")}</button><button role="radio" data-th="auto" aria-label="Same as this device">${ic("con")}</button><button role="radio" data-th="dark" aria-label="Dark">${ic("moon")}</button></div></header>
        <div class="lead"><div class="stg3"><div class="tilt" id="tilt"><span class="shd" id="shd"></span>
        <div class="con" id="con" tabindex="0" role="slider" aria-label="Bitecoins this week">
          <span class="mat"></span>
          <svg class="deb" viewBox="150 140 220 220" aria-hidden="true"><defs><linearGradient id="me-dg" x1="0" y1="0" x2=".7" y2="1"><stop offset="0" stop-color="#000" stop-opacity=".34"/><stop offset="1" stop-color="#000" stop-opacity=".1"/></linearGradient></defs><path d="M256.427 176C209.902 176 172.19 213.712 172.19 260.237C172.19 306.762 209.902 344.474 256.427 344.474C302.952 344.474 340.664 306.762 340.664 260.237V227.827L280.449 244.736L295.564 176H256.427Z" fill="url(#me-dg)"/><path d="M256.427 176C209.902 176 172.19 213.712 172.19 260.237C172.19 306.762 209.902 344.474 256.427 344.474C302.952 344.474 340.664 306.762 340.664 260.237V227.827L280.449 244.736L295.564 176H256.427Z" fill="none" stroke="#fff" stroke-opacity=".11" stroke-width=".8" transform="translate(.5 .8)"/><path d="M256.427 176C209.902 176 172.19 213.712 172.19 260.237C172.19 306.762 209.902 344.474 256.427 344.474C302.952 344.474 340.664 306.762 340.664 260.237V227.827L280.449 244.736L295.564 176H256.427Z" fill="none" stroke="#000" stroke-opacity=".5" stroke-width=".8" transform="translate(-.3 -.5)"/></svg>
          <svg class="trkv" id="bv" aria-hidden="true"></svg><canvas id="bc"></canvas><span class="gl2" id="gl2"></span>
          <div class="fc">
            <div class="hd"><svg viewBox="168 136 218 212" aria-hidden="true"><path d="M256.427 176C209.902 176 172.19 213.712 172.19 260.237C172.19 306.762 209.902 344.474 256.427 344.474C302.952 344.474 340.664 306.762 340.664 260.237V227.827L280.449 244.736L295.564 176H256.427Z" fill="#fff"/><circle cx="355.5" cy="167.5" r="27.5" fill="#fff"/></svg><div class="cap now" id="cap"><div class="tx"><b id="cap-t"></b><span id="cap-s"></span></div></div><span id="of"></span></div>
            <div class="trk" id="trk"></div>
            <div class="ft"><div class="num" id="num"></div><span class="nm2" id="nm2"></span></div>
          </div>
        </div>
        </div></div></div>
        <section class="ctl" id="ctl" aria-label="Buy Bitecoins">
          <div class="buy"><div class="sg" id="pk" role="radiogroup" aria-label="How many" style="--n:3;--at:1"></div><button class="bb" id="b-go"><span>Buy</span> <span class="ln" id="bq-p"></span></button></div>
        </section>
        <button class="plr" id="go-plan"><span class="tx"><b>Plan</b><span id="pl-s"></span></span><span class="tx r"><b class="ln" id="pl-n"></b><span id="pl-d"></span></span>${ic("r")}</button>
        <span id="sr" aria-live="polite" style="position:absolute;width:1px;height:1px;overflow:hidden;clip-path:inset(50%)"></span>
      </section>
      <section class="col" id="col">
        <div class="rl2" id="rail2" role="group" aria-label="Whose"></div>
        <p class="none lone" id="lone">Your feeders show up here.</p>
        <div class="zs">
        <header class="hdw"><h2 class="ln" id="wh-t"></h2><p class="ln" id="wh-s"></p><button class="tx1" id="wh-e">Edit</button></header>

        <section class="z" id="s-spent">
          <div class="zl"><h3 class="ln" id="sp-t"></h3><span class="wk"><button class="rb" id="wk-b" aria-label="The week before"></button><button class="rb" id="wk-n" aria-label="The week after"></button></span></div>
          <div class="one"><b class="fig" id="sp-n"></b><span class="un">spent</span><button class="tx1" id="sp-a">Statement</button></div>
          <div class="cut" id="cut" aria-hidden="true"></div>
          <div class="tri leg" id="leg"></div>
          <p class="none" id="sp-z"></p>
        </section>

        <section class="z" id="s-al">
          <div class="zl"><h3>Alerts</h3><button class="tx1 ln" id="al-x"></button></div>
          <div class="rus" id="rus"></div>
          <div class="pan" id="pan"><div class="strip" id="strip"><svg id="sv" aria-hidden="true"><g id="sv-g"><path id="sv-a"/><path id="sv-b"/></g></svg><div class="stl" id="st-l" role="slider" tabindex="0" aria-label="Where the line is"><b id="st-v"></b></div><div class="stk2" id="st-k" aria-hidden="true"></div></div><div class="say2"><p class="ln" id="ru-c"></p><button class="tx1" id="ru-n">See all</button></div><div class="fr" id="stk"></div></div>
          <div class="ro two"><span>Delivery</span><div class="sg sm" id="al-w" role="radiogroup" aria-label="Delivery" style="--n:2;--at:0"><button role="radio" data-w="0">Instant</button><button role="radio" data-w="1">Daily, 9am</button></div></div>
          <div class="ro two"><span>Quiet hours, 11pm to 8am</span><button class="tg" id="al-q" role="switch" aria-label="Quiet hours, 11pm to 8am"></button></div>
          <p class="none rule">Alerts are not sent yet. What you set is saved on this device.</p>
        </section>

        <section class="z" id="s-nu">
          <div class="zl"><h3>Tracked</h3><p class="ln zr" id="nu-s"></p></div>
          <div class="tri figs"><div><b class="fig" id="n-a"></b><span>posts</span></div><div><b class="fig" id="n-b"></b><span>likes</span></div><div class="hot"><b class="fig" id="n-c"></b><span>over 5× usual</span></div></div>
          <div class="cal" id="cal"><svg id="cs" aria-hidden="true"></svg><div class="mon" id="mon" aria-hidden="true"></div></div>
          <p class="ln cap2" id="cal-s"></p>
          <div class="tri recs" id="recs"></div>
        </section>
        </div>
      </section>`;
const LAYER = `<div class="scr" id="scr"></div><section class="sht" id="sht" role="dialog" aria-modal="true" aria-labelledby="sht-t" inert><header id="sht-h"><h2 id="sht-t"></h2><button class="rb" id="sht-x" aria-label="Close"></button></header><div class="shc" id="shc"></div></section><div class="say" id="say" role="status"></div>`;

export function mountMe({ root, layer, host }) {
const RM = matchMedia('(prefers-reduced-motion: reduce)').matches, WIDE_Q = matchMedia('(min-width: 1024px)'), wide = () => WIDE_Q.matches;
root.innerHTML = PAGE(); layer.innerHTML = LAYER;
const $ = (s, r) => (r ? r.querySelector(s) : root.querySelector(s) || layer.querySelector(s)), $$ = (s, r) => (r ? [...r.querySelectorAll(s)] : [...root.querySelectorAll(s), ...layer.querySelectorAll(s)]);
const ms = (v) => RM ? 1 : Math.round(v * T), wait = (v) => new Promise((r) => setTimeout(r, RM ? 1 : v));
let awake = false;
const LONG = ['Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday', 'Monday'];
const COST = { search: 25, follow: 15, update: 15 };
/* what it shows: the feeds and their faces, the ledger, the alerts */
const S = { feeders: 0, feeds: [], faces: new Map(), pick: { feed: null, h: null }, al: readAlerts(), weekly: 0, bought: 0, full: 0, topped: 0, today: 0, wk: 0, weeks: [{ t: 'This week', d: [[], [], [], [], [], [], []] }] };
const saveAl = () => { try { localStorage.setItem(ALERTS_KEY, JSON.stringify(S.al)); } catch {} };
const cap = () => S.feeders * 100, bal = () => S.weekly + S.bought;
const con = $('#con'), stg = $('.stg3'), numEl = $('#num'), capEl = $('#cap'), tiltEl = $('#tilt'), B = Board($('#bv'), $('#bc'));

let G = null, shown = 0, busy = false;
const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
const E = (b) => (1 - clamp(b / Math.max(1, S.full), 0, 1)) * B.N;
const stale = () => !G || G.wide !== wide() || Math.abs(G.W - con.clientWidth) > 1;
function measure() {
  const w0 = wide(), W = con.clientWidth, H = con.clientHeight, fs = Math.round(H * .3); con.style.setProperty('--fs', fs + 'px'); G = { W, H, wide: w0, fs, step: w0 ? 64 : 44 };
  const a = con.getBoundingClientRect(), t = $('#trk').getBoundingClientRect(), k = a.width / W || 1; B.size(W, H, Math.min(w0 ? 2 : 3, devicePixelRatio || 1), { x: (t.left - a.left) / k, y: (t.top - a.top + t.height * (w0 ? .5 : .52)) / k, w: t.width / k }); light();
}
const tween = (t, fn, ease = (k) => k) => RM ? (fn(1), Promise.resolve()) : new Promise((res) => { const s0 = performance.now(); const st = (n) => { const k = clamp((n - s0) / t, 0, 1); fn(ease(k), k); k < 1 ? requestAnimationFrame(st) : res(); }; requestAnimationFrame(st); });
const io = (k) => k < .5 ? 4 * k * k * k : 1 - Math.pow(-2 * k + 2, 3) / 2, sine = (k) => .5 - .5 * Math.cos(Math.PI * k), out = (k) => 1 - Math.pow(1 - k, 3);
const buzz = (n) => { try { navigator.vibrate && navigator.vibrate(n); } catch {} };

/* ── sound (sound.js): a beat for every five Bitecoins, a soft mouth opening and closing, set on the audio clock so it keeps time; a swallow when eating ends; one ending when they are added ── */
let AC = null, BUS = null, LATER = []; const BUF = {}, ENDS = Tones.ENDS;
function bufs(kind) { const id = kind; return BUF[id] || (BUF[id] = Array.from({ length: kind === 'wa' || kind === 'ka' ? 5 : 1 }, () => { const T0 = Tones, d = kind === 'wa' || kind === 'ka' ? T0.dot(kind, AC.sampleRate, Math.random) : kind === 'end' ? T0.end(ENDS[0], AC.sampleRate, Math.random) : T0[kind](AC.sampleRate, Math.random), b = AC.createBuffer(1, d.length, AC.sampleRate); b.getChannelData(0).set(d); return b; })); }
const warm = () => ['wa', 'ka', 'gulp', 'end'].forEach((k, i) => setTimeout(() => AC && bufs(k), 20 + i * 40));
function ac() { try { if (!AC) { AC = new (window.AudioContext || window.webkitAudioContext)({ latencyHint: 'interactive' }); BUS = AC.createGain(); BUS.gain.value = .92; BUS.connect(AC.destination); } return AC; } catch { return null; } }
function unlock() { const A = ac(); if (!A) return; if (A.state !== 'running') A.resume().catch(() => {}); if (!A.woke) { A.woke = true; const s = A.createBufferSource(); s.buffer = A.createBuffer(1, 1, 22050); s.connect(A.destination); s.start(0); warm(); } }
const heard = () => !RM && AC && AC.state === 'running';
/* how late the speaker is (headphones over the air most of all): the picture waits for it, so a hit is heard when it is seen */
const late = () => heard() ? Math.min(220, (AC.outputLatency || 0) * 1000) : 0;
function play(kind, v, e = 60, after = 0) {
  if (!heard()) return; const list = bufs(kind), s = AC.createBufferSource(), g = AC.createGain(), when = AC.currentTime + Math.max(0, after); s.buffer = list[(Math.random() * list.length) | 0]; const small = kind === 'wa' || kind === 'ka'; if (small) s.playbackRate.value = 1 + (Math.random() - .5) * .016; g.gain.value = v * (small ? .9 + .1 * Math.random() : 1);
  if (small && AC.createStereoPanner) { const p = AC.createStereoPanner(); p.pan.value = clamp(e / B.N, 0, 1) * .7 - .35; s.connect(g).connect(p).connect(BUS); } else s.connect(g).connect(kind === 'end' ? AC.destination : BUS); s.start(when); if (after > .03) { LATER = LATER.filter((x) => x.when > AC.currentTime); LATER.push({ s, when }); }
}
const unplay = () => { if (AC) LATER.forEach((x) => { if (x.when > AC.currentTime + .005) try { x.s.stop(); } catch {} }); LATER = []; };
const beatNo = (j, v, e, after) => play(j % 2 ? 'ka' : 'wa', v, e, after);

/* ── what the card says ── */
function setNum(b) { const n = Math.round(b), z = n <= 0, l = fmt(n).length; numEl.textContent = z ? 'Feed me' : fmt(n); numEl.style.fontSize = z ? Math.round(G.fs * .74) + 'px' : l > 5 ? Math.round(G.fs * .8) + 'px' : ''; }
/* the week as places on the card: 0 is now, k is just before the kth newest spend */
let ST = [], P = 0, lineAt = 1e9, slid = false;
function stops() { const ev = S.weeks[0].d.flatMap((d, i) => d.map((e) => ({ ...e, day: i }))); const o = [{ b: bal() }]; let b = bal(); for (let k = ev.length - 1; k >= 0; k--) { const e = ev[k]; if (!COST[e.t]) break; b += e.n; o.push({ b, e }); } ST = o; return o; }
const ahead = () => Math.floor((bal() + .001) / COST.search);
const dayOf = (i) => i === S.today ? 'Today' : LONG[i];
function line(j, head, dir = 0) {
  if (j === lineAt && head === undefined) return; lineAt = j; const s = ST[j], e = s && s.e, now = !e && !(j < 0); capEl.classList.toggle('now', now); const tx = (t, u) => { $('#cap-t').textContent = t; $('#cap-s').innerHTML = u; };
  if (j < 0) { const k = -j, left = Math.max(0, Math.round(bal() - COST.search * k)); tx(k === 1 ? 'One more search' : `${fmt(k)} more searches`, `${left ? `Leaves ${fmt(left)}` : 'Leaves none'}, not spent yet`); }
  else if (e) { tx(esc(e.title || ''), head || `${dayOf(e.day)}, ${e.t === 'search' ? 'search' : e.t === 'follow' ? 'followed' : 'file update'}, −${e.n}`); }
  else { const b = Math.round(bal()); $('#of').textContent = !S.feeders ? 'No feeders yet' : b <= 0 ? `Back to ${fmt(cap() + S.bought)} on Tuesday` : `of ${fmt(S.full)}`; }
  if (dir && !RM && !now) $('.tx', capEl).animate([{ transform: `translateX(${dir * 12}px)`, opacity: .15 }, { transform: 'none', opacity: 1 }], { duration: 190, easing: 'cubic-bezier(.2,.8,.2,1)' });
}
/* everything the card does is this one move: stand at a place in the week */
function stand(p) { const N = ST.length - 1; P = p; const q = clamp(p, -ahead(), N), i = Math.min(Math.max(0, N - 1), Math.floor(q)), b = q < 0 ? Math.max(0, ST[0].b + COST.search * q) : N ? ST[i].b + (ST[i + 1].b - ST[i].b) * (q - i) : ST[0].b; const was = shown; shown = b; B.set({ eaten: E(b) }); setNum(b); return Math.abs(b - was); }
function doors(b) { con.setAttribute('aria-valuetext', `${fmt(Math.round(b))} of ${fmt(S.full)}`); }
function rest(at) { stops(); if (!slid) $('#nm2').textContent = ST.length > 1 ? 'Slide to rewind' : 'Slide to look ahead'; if (at != null) { const k = ST.findIndex((s) => s.b === at); stand(Math.max(0, k)); } else stand(0); B.set({ mouth: 33, grow: null, warm: false, th: 0, pull: 0 }); B.snap(); lineAt = 1e9; line(0); doors(bal()); }
/* eating, and the mouth in time with it */
const trap = (k, a) => { const v = 1 / (1 - a); return k < a ? v * k * k / (2 * a) : k < 1 - a ? v * (k - a / 2) : 1 - v * (1 - k) * (1 - k) / (2 * a); };
const chew = (amt = 1) => 33 - 6.5 * clamp(amt, 0, 1) * (.5 - .5 * Math.cos(performance.now() / 420 * 2 * Math.PI));
function sweep() { if (RM) return; $('#gl2').animate([{ transform: `translateX(${-G.W * .5}px) skewX(-18deg)` }, { transform: `translateX(${G.W * 1.15}px) skewX(-18deg)` }], { duration: ms(620), easing: 'cubic-bezier(.4,0,.2,1)' }); }
/* a quiet glide from one place to another; the line follows */
async function glide(to, t, ease = io) { const from = P, me = RW.tn; await tween(t, (k) => { if (RW.on || me !== RW.tn) return; stand(from + (to - from) * k); const j = clamp(Math.round(P), -ahead(), ST.length - 1); if (j !== lineAt) line(j, undefined, Math.sign(to - from)); }, ease); if (!RW.on && me === RW.tn) stand(to); }
/* a layer of its own only while it moves: at rest it is drawn with the page, as sharp as the type */
let hotT = 0; const hot = (on) => { clearTimeout(hotT); if (on) stg.style.willChange = numEl.style.willChange = 'transform'; else hotT = setTimeout(() => { if (!busy) stg.style.willChange = numEl.style.willChange = ''; }, 900); };
/* it lifts while they go in, and is set down */
function raise(v) { stg.style.transform = v ? `translateY(${(-9 * v).toFixed(2)}px) scale(${(1 + .012 * v).toFixed(4)})` : ''; $('#shd').style.translate = v ? `0 ${(2.5 * v).toFixed(2)}px` : ''; $('#shd').style.scale = v ? (1 - .028 * v).toFixed(4) : ''; }
/* how far it has grown, 0 to 1: from the mark it is to one the whole number fits in */
const full = () => Math.max(G.H * 1.02, G.W * .67), grown = (w) => { const m = B.mark().r; return m * Math.pow(full() / m, w); };
/* bought, or Tuesday: the mark has opened as the last of them went in. It opens wide, shuts on the old number and is home, small, in one move; the new number is there, and that is the one hit, heard on the frame it is seen */
async function swallow(to, w0 = 0) {
  hot(true); const near = [$('.cap'), $('#ctl')]; near.forEach((el) => { el.style.willChange = 'transform'; });
  const OPEN = 154, HANG = 46, SHUT = 84, SNAP = 104, S1 = OPEN + HANG, S2 = S1 + SHUT, ALL = S2 + SNAP, a0 = B.st.grow != null ? B.st.gMouth : B.st.mouth, WIDE = 56; let swapped = false;
  play('end', 1, 60, (ALL + 20) / 1000 - Tones.PRE); const l = late(); if (l) await wait(l);
  await tween(ALL, (k) => {
    const t = k * ALL; let w, m;
    if (t < S1) { const q = out(Math.min(1, t / OPEN)); w = w0 + (1 - w0) * q; m = a0 + (WIDE - a0) * q; raise(1 + .9 * q); }
    else if (t < S2) { const q = (t - S1) / SHUT; w = 1; m = 1 + (WIDE - 1) * (1 - q * q); }
    else { const q = (t - S2) / SNAP, z = q * q * (1.6 * q - .6 < 0 ? q : 1); w = 1 - z; m = 1; raise(1.9 * (1 - q * q * q)); numEl.style.transform = `scale(${(1.34 - .34 * q * q * q).toFixed(4)})`; if (!swapped) { swapped = true; setNum(to); B.set({ eaten: 0 }); B.snap(); $('#of').textContent = `of ${fmt(S.full)}`; } }
    B.set({ grow: grown(clamp(w, 0, 1)), gMouth: m });
  });
  raise(0); numEl.style.transform = ''; B.set({ grow: null, warm: false, eaten: 0, mouth: 2 }); shown = to; setNum(to); buzz([20, 26, 10]);
  if (!RM) {
    stg.animate([{ transform: 'translateY(0) scale(1)' }, { transform: 'translateY(8px) scale(.966)', offset: .11, easing: 'cubic-bezier(.25,.7,.2,1)' }, { transform: 'none' }], { duration: 620, easing: 'cubic-bezier(.1,.7,.3,1)' });
    numEl.animate([{ transform: 'scale(1)' }, { transform: 'scale(.955)', offset: .12 }, { transform: 'scale(1)' }], { duration: 520, easing: 'cubic-bezier(.2,.7,.2,1)' });
    near.forEach((el, i) => el.animate([{ transform: 'none' }, { transform: `translateY(${8 - i * 1.6}px)`, offset: .18, easing: 'cubic-bezier(.2,.8,.2,1)' }, { transform: 'none' }], { duration: 380, delay: 12 + i * 14, easing: 'cubic-bezier(.1,.7,.3,1)' }));
  }
  sweep(); await wait(70); await tween(330, (k) => B.set({ mouth: 2 + 31 * (1 - Math.pow(1 - k, 3)) })); rest(); hot(false); await wait(260); near.forEach((el) => { el.style.willChange = ''; });
}
/* Tuesday: the dots come back, a beat at a time, the same way they went */
async function refill(n) {
  hot(true); B.set({ warm: true }); const e0 = B.st.eaten, K = clamp(Math.round(n / 20), 4, 18), step = 78, t0 = 40, dur = K * step + 16; for (let j = 0; j < K; j++) beatNo(j, .24 + .1 * j / K, e0 * (1 - j / K), (t0 + j * step) / 1000);
  const l = late(); if (l) await wait(l); await tween(dur, (k) => { const q = trap(k, .12); const g = clamp((k - .78) / .22, 0, 1), w = .6 * g * g; B.set({ eaten: e0 * (1 - q), mouth: chew(.8), grow: w > .015 ? grown(w) : null, gMouth: 33 + 16 * g }); raise(q); });
}

/* ── taking hold of it: slide anywhere on the card and the mark goes with your thumb, facing the way it goes, its mouth and its sound drawn by how far it has gone. Back is the week you had; on is what is left. Let go and it goes home. Touch the card and it turns and snaps at your finger ── */
const RW = { id: null, on: false, x: 0, y: 0, p0: 0, lx: 0, lt: 0, v: 0, back: 0, dir: 1, tn: 0, beat: null, bt: 0, turn: 0 };
const pxOf = (n) => n * 24 * B.unit() / Math.max(1, S.full), gear = (p) => p < 0 ? Math.max(1.4, pxOf(COST.search)) : clamp(2 * pxOf(COST.search), 13, G.step);
/* it turns round by way of the top, so the light on it stays where it is */
function turn(dir) { if (RW.dir === dir) return; RW.dir = dir; const from = B.st.th, to = dir > 0 ? 0 : -Math.PI, me = ++RW.turn; tween(190, (k) => { if (me === RW.turn) B.set({ th: from + (to - from) * k }); }, io); }
/* five Bitecoins to a beat, whichever way it is going; quieter when they are coming back */
function tread(now, loud = 1) { const e = (S.full - shown) / 5, j = Math.floor(e + .001); if (RW.beat == null) RW.beat = j; if (j === RW.beat) return; const on = j > RW.beat; RW.beat = j; if (now - RW.bt < 56) return; RW.bt = now; beatNo(Math.abs(j) % 2, (on ? .27 : .15) * loud, B.st.eaten); buzz(3); }
async function settle(to) {
  clearTimeout(RW.back); const me = ++RW.tn, live2 = () => me === RW.tn && !RW.on; await glide(to, 220, out); if (!live2()) return; B.set({ mouth: 33, pull: 0 }); line(clamp(Math.round(to), -ahead(), ST.length - 1)); if (to !== 0) RW.back = setTimeout(() => home(me), to < 0 ? 900 : 3200);
}
async function home(me = ++RW.tn) {
  if (RW.on || busy || !P) return; const was = P, n = Math.abs(shown - bal()) / 5, ms0 = clamp(220 + n * 26, 360, 1250), from = P; RW.beat = null; turn(was < 0 ? -1 : 1);
  if (was < 0) { $('#cap-t').textContent = 'Nothing spent'; $('#cap-s').textContent = `Still ${fmt(Math.round(bal()))}`; }
  await tween(ms0, (k) => { if (RW.on || me !== RW.tn) return; stand(from * (1 - k)); tread(performance.now(), was < 0 ? 1.1 : .6); B.set({ pull: was > 0 ? Math.min(1, k * 6, (1 - k) * 5) : 0 }); if (was > 0) { const j = clamp(Math.round(P), 0, ST.length - 1); if (j !== lineAt) line(j, undefined, 1); } }, sine);
  if (RW.on || me !== RW.tn) return; stand(0); B.set({ pull: 0, mouth: 33 }); turn(1); lineAt = 1e9; await wait(was < 0 ? 520 : 60); if (!RW.on && me === RW.tn && P === 0) { lineAt = 1e9; line(0, undefined, was < 0 ? 0 : 1); }
}
/* touch it and it turns and snaps at your finger; touch the mark itself and it goes right round */
async function snapAt(x, y) {
  const m = B.mark(), dx = x - m.x, dy = y - m.y, on = Math.hypot(dx, dy) < m.r, th = on ? -2 * Math.PI : Math.atan2(dy, dx), me = ++RW.turn; RW.dir = 1; const ok = () => me === RW.turn && !RW.on && !busy;
  if (on) { play('wa', .3, B.st.eaten); play('ka', .34, B.st.eaten, .2); await tween(430, (k) => { if (ok()) B.set({ th: th * k, mouth: 33 - 26 * Math.sin(Math.PI * Math.min(1, k * 1.1)) }); }, io); }
  else { await tween(120, (k) => { if (ok()) B.set({ th: th * k, mouth: 33 + 9 * k }); }, out); if (ok()) { play('ka', .38, B.st.eaten); buzz(8); } await tween(64, (k) => { if (ok()) B.set({ mouth: 42 - 40 * k }); }, out); await tween(300, (k) => { if (ok()) B.set({ mouth: 2 + 31 * Math.min(1, k * 1.5), th: th * (1 - k) }); }, io); }
  if (ok()) B.set({ th: 0, mouth: 33 });
}
con.addEventListener('pointerdown', (e) => { if (busy || e.button) return; RW.id = e.pointerId; RW.x = RW.lx = e.clientX; RW.y = e.clientY; RW.lt = e.timeStamp; RW.p0 = P; RW.on = false; RW.v = 0; });
con.addEventListener('pointermove', (e) => {
  if (RW.id !== e.pointerId) return; const dx = e.clientX - RW.x, dy = e.clientY - RW.y;
  if (!RW.on) { if (Math.abs(dx) < 6) return; if (Math.abs(dy) > Math.abs(dx)) { RW.id = null; return; } RW.on = true; slid = true; $('#nm2').textContent = 'Bitecoins'; RW.tn++; clearTimeout(RW.back); hot(true); con.classList.add('held'); try { con.setPointerCapture(e.pointerId); } catch {} if (P === 0) stops(); RW.lx = RW.x = e.clientX; RW.beat = null; B.set({ pull: 0 }); turn(dx > 0 ? 1 : -1); return; }
  const d = e.clientX - RW.lx, dt = Math.max(1, e.timeStamp - RW.lt); RW.v = RW.v * .6 + (d / dt) * .4; RW.lx = e.clientX; RW.lt = e.timeStamp;
  // the gear changes at now, so one thumb's travel reaches either end; past an end it gives
  let p = P - d / gear(d > 0 && P <= 0 || d < 0 && P < 0 ? -1 : 1); const N = ST.length - 1, F = ahead(); if (p < -F || p > N) p = P - d / gear(p < 0 ? -1 : 1) * .2; stand(p); tread(e.timeStamp); if (Math.abs(RW.v) > .05) turn(RW.v > 0 ? 1 : -1); B.set({ pull: RW.v > 0 ? clamp(RW.v * 2.2, 0, 1) : 0 });
  const j = clamp(Math.round(P), -F, N); if (j !== lineAt) line(j, undefined, RW.v > 0 ? 1 : -1);
});
const lift = (e) => { if (RW.id !== e.pointerId) return; RW.id = null; con.classList.remove('held'); if (!RW.on) { if (e.type === 'pointerup' && !busy) { if (Math.abs(P) > .01) home(); else { const q = con.getBoundingClientRect(), z = q.width / con.clientWidth || 1; snapAt((e.clientX - q.left) / z, (e.clientY - q.top) / z); } } return; } RW.on = false; hot(false); settle(clamp(Math.round(P - clamp(RW.v * 110 / gear(P < 0 ? -1 : 1), -3, 3)), -ahead(), ST.length - 1)); };
con.addEventListener('pointerup', lift); con.addEventListener('pointercancel', lift);
const step1 = (d) => { if (busy) return; if (P === 0) stops(); turn(d < 0 ? 1 : -1); RW.beat = null; const to = clamp(Math.round(P) + d, -ahead(), ST.length - 1); settle(to).then(() => turn(1)); beatNo(d < 0 ? 0 : 1, .26, B.st.eaten); };

/* ── the light: one streak, moving only as the page does ── */
let scr = 0; function light() { if (G) $('#gl2').style.transform = `translateX(${(G.W * .62 + scr * 1.15).toFixed(1)}px) skewX(-18deg)`; }
let scrRaf = 0; const onScroll = () => { tuckRail(); if (scrRaf) return; scrRaf = requestAnimationFrame(() => { scrRaf = 0; scr = Math.min(420, scrollY); light(); }); };
/* once, as it arrives */
function arrive() { if (RM || !G) return; tiltEl.animate([{ transform: 'translateY(22px)' }, { transform: 'none' }], { duration: ms(460), easing: 'cubic-bezier(.16,.9,.2,1)' }); setTimeout(sweep, 160); }

/* ── buying: how many, and what they come to ── */
const PACKS = [[500, '500'], [1100, '1,000'], [3000, '2,500']]; let pk = 1;
function packs() { const p = PACKS[pk][1], sg = $('#pk'); if (!sg.children.length) sg.innerHTML = PACKS.map(([n]) => `<button role="radio">${fmt(n)}</button>`).join(''); sg.style.setProperty('--at', pk); [...sg.children].forEach((b, i) => b.setAttribute('aria-checked', i === pk)); rolling = true; ln($('#bq-p'), `₹${p}`); rolling = false; $('#b-go').setAttribute('aria-label', `Buy ${fmt(PACKS[pk][0])} for ₹${p}`); }
$('#pk').onclick = (e) => { const b = e.target.closest('button'); if (!b) return; pk = [...$('#pk').children].indexOf(b); buzz(4); packs(); }; $('#b-go').onclick = () => say('Buying is not open yet');

/* ── who the page is about: every feed, one feed, or one feeder ── */
const feedBy = (id) => S.feeds.find((x) => x.id === id), feedOf = (w) => S.feeds.find((x) => x.f.includes(w)) || S.feeds[0], ini = (w) => w.replace(/[^a-z0-9]/gi, '').slice(0, 2).toUpperCase(), pl = (n, t) => `${fmt(n)} ${t}${n === 1 ? '' : 's'}`;
const PERSON = (z) => `<svg class="fi" viewBox="0 0 24 24" style="width:${z}px;height:${z}px"><circle cx="12" cy="8" r="4.600"/><path d="M4.500 20.500a7.500 7.500 0 0 1 15 0"/></svg>`, ico = (d) => `<svg class="fi" viewBox="0 0 24 24">${d}</svg>`, fi = (x) => { const d = host.icon(x.id, x.t); return d ? ico(d) : esc(x.t.slice(0, 2).toUpperCase()); };
const art = (w) => { const u = S.faces.get(w); return u ? `<img src="${esc(u)}" alt="" data-ini="${ini(w)}" decoding="async">` : ini(w); };
const inPick = (w) => S.pick.h ? w === S.pick.h : S.pick.feed ? feedBy(S.pick.feed).f.includes(w) : true;
const level = () => S.pick.h ? 'feeder' : S.pick.feed ? 'feed' : 'all';
const events = () => S.weeks[S.wk].d.flatMap((d, i) => d.filter((e) => !e.fresh).map((e) => ({ ...e, day: i })));
const mine = (e) => COST[e.t] ? inPick(e.h) : level() === 'all';
const sum = (l) => l.reduce((a, e) => a + (COST[e.t] ? e.n : 0), 0);
const whoPick = () => S.pick.h ? [S.pick.h] : S.pick.feed ? feedBy(S.pick.feed).f : [...new Set(S.feeds.flatMap((x) => x.f))];
const pid = () => S.pick.h ? 'h:' + S.pick.h : S.pick.feed ? 'f:' + S.pick.feed : 'all';


/* ── motion. Nothing travels. A line rolls inside its own line, digits roll inside their own places, bars and keys fill; only the circles move, out of the feed you tap and back into it. One curve, one tempo ── */
let rolling = false;
const drawn = (fn) => requestAnimationFrame(() => requestAnimationFrame(fn)), ROLL = () => ({ duration: ms(380), easing: CARRY });
function ln(el, html) {
  html = String(html); if (el._s === html) return; const was = el._s; el._s = html; if (RM || was == null || !rolling) { el.innerHTML = `<span>${html}</span>`; return; }
  const me = el._r = (el._r || 0) + 1; el.innerHTML = `<span>${html}</span><span aria-hidden="true">${was}</span>`; const [n, o] = el.children; n.animate([{ transform: 'translate3d(0, 101%, 0)' }, { transform: 'none' }], { ...ROLL(), fill: 'backwards' }); o.animate([{ transform: 'none' }, { transform: 'translate3d(0, -101%, 0)' }], { ...ROLL(), fill: 'forwards' }).finished.then(() => { if (el._r === me) o.remove(); }).catch(() => {});
}
function slot(el, text) {
  text = String(text); const was = el._s; if (was === text) return; el._s = text; el.setAttribute('aria-label', text); if (RM || was == null || !rolling) { el.textContent = text; return; }
  const n = Math.max(was.length, text.length), A = was.padStart(n, '\u0000'), Z = text.padStart(n, '\u0000'), me = el._r = (el._r || 0) + 1; let o = '';
  for (let i = 0; i < n; i++) { const a = A[i], z = Z[i]; if (z === '\u0000') continue; o += a === z ? `<span class="sl">${z}</span>` : `<span class="sl"><span class="sz">${z}</span><u class="o">${a === '\u0000' ? '' : a}</u><u class="n">${z}</u></span>`; }
  el.innerHTML = `<span class="slr" aria-hidden="true">${o}</span>`; setTimeout(() => { if (el._r === me) el.textContent = text; }, ms(420) + 60);
}
/* a picture changes inside its own circle */
function face(el, html) { if (el._s === html) return; const was = el._s; el._s = html; if (RM || was == null || !rolling) { el.innerHTML = `<span>${html}</span>`; return; } const me = el._r = (el._r || 0) + 1; el.innerHTML = `<span>${was}</span><span>${html}</span>`; const [o, n] = el.children; n.animate([{ transform: 'translate3d(0, 101%, 0)' }, { transform: 'none' }], { ...ROLL(), fill: 'backwards' }); o.animate([{ transform: 'none' }, { transform: 'translate3d(0, -101%, 0)' }], { ...ROLL(), fill: 'forwards' }).finished.then(() => { if (el._r === me) o.remove(); }).catch(() => {}); }

/* ── the rail ── */
const WEDGE = (z) => `<svg viewBox="0 0 42 42" style="width:${z}px;height:${z}px;display:block" aria-hidden="true"><path d="M 21 21 L 35.87 12.76 A 17 17 0 1 0 35.87 29.24 Z" fill="none" stroke="currentColor" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
const railItems = () => { const F = S.pick.feed && feedBy(S.pick.feed), it = (id, t, inner) => [id, `<button class="wc" data-p="${id}"><span class="d">${inner}<svg class="rg" viewBox="0 0 60 60" aria-hidden="true"><circle cx="30" cy="30" r="28.6" pathLength="100"/></svg></span><span class="nm">${t}</span></button>`], fb = (x) => it('f:' + x.id, esc(x.t), `<i class="p">${fi(x)}</i>`); return [it('all', 'All', `<i class="p wd">${WEDGE(24)}</i>`), ...(F ? [fb(F), ...F.f.map((w) => it('h:' + w, w, `<i class="p">${art(w)}</i>`))] : S.feeds.map(fb))]; };
function put(list, items) {
  const old = new Map([...list.children].filter((c) => c.dataset.id).map((c) => [c.dataset.id, c])), next = items.map(([id, html]) => { let el = old.get(id); if (!el) { const t = document.createElement('template'); t.innerHTML = html; el = t.content.firstElementChild; el.dataset.id = id; } old.delete(id); return el; });
  const gone = [...old.values()]; list.replaceChildren(...next); return gone;
}
const places = (l) => { const b = l.getBoundingClientRect(), z = l.offsetWidth ? b.width / l.offsetWidth : 1, m = new Map(); [...l.children].forEach((c) => { if (!c.dataset.id) return; const q = c.getBoundingClientRect(); m.set(c.dataset.id, (q.left - b.left) / z + l.scrollLeft); }); return m; };
function railOn() { const id = pid(); $$('#rail2 .wc').forEach((b) => b.setAttribute('aria-pressed', b.dataset.p === id)); }
/* one glide, out of the tapped feed's place or back into it */
function railUi(move) {
  const r = $('#rail2'), was = places(r), gone = put(r, railItems()); railOn(); if (!move || RM) return; if (r.scrollLeft) r.scrollLeft = 0;
  const now = places(r), hub = r.dataset.hub, hx = now.get(hub) ?? was.get(hub), o = { duration: ms(560), easing: CARRY }; if (hx == null) return;
  gone.forEach((g) => { const x = was.get(g.dataset.id); if (x == null) return; delete g.dataset.id; Object.assign(g.style, { position: 'absolute', left: x + 'px', top: r.firstElementChild.offsetTop + 'px', zIndex: 0, pointerEvents: 'none' }); g.setAttribute('aria-hidden', 'true'); r.append(g); $('.nm', g).style.visibility = 'hidden'; g.animate([{ transform: 'none' }, { transform: `translate3d(${(hx - x).toFixed(1)}px, 0, 0) scale(.94)` }], { ...o, fill: 'forwards' }).finished.then(() => g.remove()).catch(() => g.remove()); });
  [...r.children].forEach((c) => { const id = c.dataset.id; if (!id) return; c.style.zIndex = id === hub || id === 'all' ? 3 : 1; const a = was.get(id), b = now.get(id); if (a != null) { if (Math.abs(a - b) > .6) c.animate([{ transform: `translate3d(${(a - b).toFixed(1)}px, 0, 0)` }, { transform: 'none' }], o); } else if (b < r.clientWidth + 160) { c.animate([{ transform: `translate3d(${(hx - b).toFixed(1)}px, 0, 0) scale(.94)` }, { transform: 'none' }], { ...o, fill: 'backwards' }); const nm = $('.nm', c); nm.animate([{ visibility: 'hidden' }, { visibility: 'hidden', offset: .99 }, { visibility: 'visible' }], { duration: ms(380) }); } });
}
function pick(feed, w = null, told = false) {
  if (S.pick.feed === feed && S.pick.h === w) return; if (!told) host.pick(feed, w); const r = $('#rail2'), same = S.pick.feed === feed; r.dataset.hub = 'f:' + (feed || S.pick.feed); S.pick = { feed, h: w };
  drawn(() => { rolling = true; if (same) railOn(); else railUi(true); fill(); rolling = false; const c = same && $('#rail2 [aria-pressed="true"]'); if (c) { const a = c.offsetLeft - 60, b = c.offsetLeft + c.offsetWidth + 60; if (a < r.scrollLeft || b > r.scrollLeft + r.clientWidth) r.scrollTo({ left: Math.max(0, a), behavior: 'smooth' }); } });
}
$('#rail2').onclick = (e) => { const b = e.target.closest('.wc'); if (!b) return; const id = b.dataset.p; if (id === 'all') pick(null); else if (id.startsWith('f:')) { pick(id.slice(2)); } else { const w = id.slice(2); S.pick.h === w ? pick(S.pick.feed) : pick(S.pick.feed || feedOf(w).id, w); } };
let FED = { began: '2026-01-01', today: '2026-01-01', h: [], p: [], fo: [] }, FI = new Map(), D0 = new Date(), LASTD = 0, FROM = 0, DAYN = 1, OFF = 0, COLS = 26, loaded = false;
const IT = new Map(), PIN = new Map(), HOT = 50, MON = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], dayAt = (n) => new Date(D0.getTime() + n * 864e5);
const ds = (d) => `${d.getUTCDate()} ${MON[d.getUTCMonth()].slice(0, 3)}`, dm = (d) => `${d.getUTCDate()} ${MON[d.getUTCMonth()]}`, big = (n) => n >= 1e9 ? (n / 1e9).toFixed(1) + 'B' : n >= 1e8 ? (n / 1e6).toFixed(0) + 'M' : n >= 1e6 ? (n / 1e6).toFixed(1) + 'M' : n >= 1e5 ? Math.round(n / 1e3) + 'K' : fmt(n);
const fedPick = () => new Set(whoPick().map((w) => FI.get(w)).filter((i) => i != null));
/* ── whose, in words ── */
function whoUi() { const F = S.pick.feed && feedBy(S.pick.feed), w = S.pick.h; ln($('#wh-t'), w ? '@' + w : F ? esc(F.t) : 'All feeds'); ln($('#wh-s'), w ? `In ${F ? esc(F.t) : 'your feeds'}` : F ? pl(F.f.length, 'feeder') : `${pl(S.feeders, 'feeder')} in ${pl(S.feeds.length, 'feed')}`); $('#wh-e').hidden = !F || Boolean(w); }
$('#wh-e').onclick = () => sheet('icon');

/* ── spent: one number, where it went ── */
function weekUi() {
  const l = events().filter((e) => COST[e.t] && mine(e)), w = S.weeks[S.wk], tot = sum(l), lv = level(), F = S.pick.feed && feedBy(S.pick.feed);
  let g = lv === 'all' ? S.feeds.map((x) => [esc(x.t), sum(l.filter((e) => x.f.includes(e.h)))]) : lv === 'feed' ? F.f.map((q) => ['@' + q, sum(l.filter((e) => e.h === q))]) : [['Searches', sum(l.filter((e) => e.t === 'search'))], ['Files', sum(l.filter((e) => e.t !== 'search'))]];
  g = g.filter((x) => x[1] > 0).sort((a, b) => b[1] - a[1]); if (g.length > 3) { const o = g.slice(2).reduce((a, x) => a + x[1], 0); g = [...g.slice(0, 2), [`${g.length - 2} more`, o]]; }
  ln($('#sp-t'), S.wk === 0 ? 'This week' : S.wk === 1 ? 'Last week' : w.t); slot($('#sp-n'), fmt(tot)); $('#wk-b').disabled = S.wk >= S.weeks.length - 1; $('#wk-n').disabled = S.wk <= 0;
  const c = $('#cut'), le = $('#leg'); if (!c.children.length) { c.innerHTML = '<i></i><i></i><i></i>'; le.innerHTML = '<div><span class="ln"></span><b></b></div>'.repeat(3); } let x = 0; [0, 1, 2].forEach((i) => { const v = g[i] ? g[i][1] / tot : 0, e = c.children[i], d = le.children[i]; e.style.setProperty('--x', x.toFixed(4)); e.style.setProperty('--w', v.toFixed(4)); x += v; d.classList.toggle('no', !g[i]); if (g[i]) { ln($('.ln', d), g[i][0]); slot($('b', d), fmt(g[i][1])); } });
  root.toggleAttribute('data-spent', tot > 0); ln($('#sp-z'), S.wk === 0 ? `A search uses 25, a file 15. Back to ${fmt(cap())} on Tuesday.` : 'Nothing spent that week.');
}
$('#wk-b').innerHTML = ic('l'); $('#wk-n').innerHTML = ic('r');
const week = (d) => { const to = S.wk + d; if (to < 0 || to >= S.weeks.length) return; S.wk = to; rolling = true; weekUi(); rolling = false; };
$('#wk-b').onclick = () => week(1); $('#wk-n').onclick = () => week(-1); $('#sp-a').onclick = () => sheet('spent');

/* ── alerts. Five exact sentences; four of them carry a number you set, on a line drawn through what really happened in the last 30 days ── */
const TYPES = ['Reel', 'Carousel', 'Picture'], DN = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const n1 = (x) => (x >= 10 ? fmt(Math.round(x)) : (Math.round(x * 10) / 10).toString()), k3 = (n) => n >= 1e4 && n < 1e6 ? Math.round(n / 1e3) + 'K' : big(n), h12 = (h) => `${h % 12 || 12}${h < 12 ? 'am' : 'pm'}`;
const RU = [
  { id: 'likes', say: (v) => `Likes over <i>${n1(v)}×</i> usual`, val: (v) => `${n1(v)}×`, log: true, lo: .94, hi: 120, steps: [1.5, 2, 3, 4, 5, 7, 10, 15, 20, 30, 50], ticks: [[1, 'Usual'], [2, '2×'], [5, '5×'], [10, '10×'], [50, '50×']], unit: 'post' },
  { id: 'comm', say: (v) => `Comments over <i>${n1(v)}×</i> usual`, val: (v) => `${n1(v)}×`, log: true, lo: .94, hi: 120, steps: [1.5, 2, 3, 4, 5, 7, 10, 15, 20, 30, 50], ticks: [[1, 'Usual'], [2, '2×'], [5, '5×'], [10, '10×'], [50, '50×']], unit: 'post' },
  { id: 'fol', say: (v) => `Followers move <i>${n1(v)}%</i> in a day`, val: (v) => `${n1(v)}%`, log: true, lo: .09, hi: 12, steps: [.2, .5, 1, 2, 3, 5], ticks: [[.1, '0.1%'], [.5, '0.5%'], [1, '1%'], [5, '5%']], unit: 'day' },
  { id: 'gap', say: (v) => `No post for <i>${v}</i> days`, val: (v) => `${v} days`, lo: .4, hi: 15, steps: [2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14], ticks: [[1, '1 day'], [4, '4'], [7, '7'], [10, '10'], [14, '14']], unit: 'gap' },
  { id: 'post', say: () => 'Every new post' },
], RB = new Map(RU.map((r) => [r.id, r]));
let ruAt = 'likes';
const copyCfg = (c) => Object.fromEntries(Object.entries(c).map(([k, v]) => [k, { ...v }]));
const cfgOf = (w) => S.al.one[w] || S.al.own[(feedOf(w) || {}).id] || S.al.base, scopeCfg = () => S.pick.h ? cfgOf(S.pick.h) : S.pick.feed ? S.al.own[S.pick.feed] || S.al.base : S.al.base, upName = () => S.pick.h && S.al.own[S.pick.feed] ? esc(feedBy(S.pick.feed).t) : 'all feeds';
const isOwn = () => Boolean(S.pick.h ? S.al.one[S.pick.h] : S.pick.feed && S.al.own[S.pick.feed]);
function ownCfg() { if (S.pick.h) return S.al.one[S.pick.h] ||= copyCfg(cfgOf(S.pick.h)); if (S.pick.feed) return S.al.own[S.pick.feed] ||= copyCfg(S.al.base); return S.al.base; }
/* what happened, for one rule, to whoever the page is about: each with how far it went */
function itemsOf(w, id) {
  const key = w + '|' + id; if (IT.has(key)) return IT.get(key); const f9 = FI.get(w), o = []; IT.set(key, o); if (f9 == null) return o;
  if (id === 'likes' || id === 'comm' || id === 'post') FED.p.forEach((p, i) => { if (p[2] !== f9 || p[0] < FROM) return; const jt = (((i * 2654435761) >>> 0) / 4294967296 - .5) * .1, v0 = id === 'post' ? 1 : id === 'likes' ? p[4] / 10 : p[7] / 10, v = v0; if (v > 0) o.push({ v, at: Math.max(.01, v + jt), d: p[0], i, w }); });
  else if (id === 'fol') { const s = FED.fo[f9] || []; for (let n = 1; n < s.length; n++) { const df = s[n][1] - s[n - 1][1], pc = df / s[n - 1][1] * 100; if (s[n][0] >= FROM && s[n][0] - s[n - 1][0] <= 2 && pc) o.push({ v: Math.abs(pc), pc, df, d: s[n][0], w }); } }
  else { const t = [...new Set(FED.p.filter((p) => p[2] === f9).map((p) => p[0]))]; t.forEach((x, n) => { const nx = n + 1 < t.length ? t[n + 1] : LASTD, g = nx - x; if (nx >= FROM && g >= 1) o.push({ v: g, at: g + ((((x * 7919 + f9 * 104729) * 2654435761) >>> 0) / 4294967296 - .5) * .72, d: nx, w, from: x }); }); }
  return o;
}
const items = (id) => whoPick().flatMap((w) => itemsOf(w, id)), past = (id, v) => items(id).filter((x) => id === 'post' || x.v >= v).sort((a, b) => b.d - a.d || b.v - a.v);
const usualGap = (w) => { const t = [...new Set(FED.p.filter((p) => p[2] === FI.get(w)).map((p) => p[0]))]; return t.length > 2 ? (t[t.length - 1] - t[0]) / (t.length - 1) : 0; };
function noteTx(id, x) {
  const p = x.i != null ? FED.p[x.i] : null;
  if (id === 'likes') return [`@${x.w}`, `${TYPES[p[3]]}, ${n1(x.v)}× usual likes, ${k3(p[5])}`]; if (id === 'comm') return [`@${x.w}`, `${TYPES[p[3]]}, ${n1(x.v)}× usual comments, ${k3(p[6])}`];
  if (id === 'fol') return [`@${x.w}`, `${x.df > 0 ? '+' : '−'}${fmt(Math.abs(x.df))} followers, ${x.pc > 0 ? '+' : '−'}${n1(x.v)}%`]; if (id === 'gap') { const u = usualGap(x.w); return [`@${x.w}`, `No post for ${x.v} days${u ? `, usually ${u < 1.5 ? '1' : Math.round(u)}` : ''}`]; }
  return [`@${x.w}`, `New ${TYPES[p[3]].toLowerCase()}, ${h12(p[1])}`];
}
const dayTx = (x) => `${dayAt(x.d).getUTCDate()} ${MON[dayAt(x.d).getUTCMonth()].slice(0, 3)}`;
const noteHtml = (id, x) => { const [a, b] = noteTx(id, x); return `<div class="nt"><span class="fa">${art(x.w)}</span><span class="tx"><b>${a}</b><span>${b}</span></span><time>${dayTx(x)}</time></div>`; };
/* the next message comes up from under, as the one before it goes */
function notes(html) {
  const box = $('#stk'); if (box._s === html) return; const was = box._s; box._s = html; if (RM || was == null || !rolling) { box.innerHTML = html; return; }
  const o = box.lastElementChild, t = document.createElement('template'); t.innerHTML = html; const n = t.content.firstElementChild; [...box.children].forEach((x) => { if (x !== o) x.remove(); }); box.classList.add('mv'); box.append(n); const d = { duration: ms(380), easing: CARRY };
  n.animate([{ transform: 'translate3d(0, 116%, 0)' }, { transform: 'none' }], { ...d, fill: 'backwards' }).finished.then(() => { if (box.lastElementChild === n) { [...box.children].forEach((x) => { if (x !== n) x.remove(); }); box.classList.remove('mv'); } }).catch(() => {}); if (o) o.animate([{ transform: 'none' }, { transform: 'translate3d(0, -116%, 0)' }], { ...d, fill: 'forwards' });
}
/* the strip: a dot for each, as far along as it went; past the line is red */
const SV = { W: 0, H: 58, pts: [], x: null };
const posOf = (r, v) => clamp(r.log ? Math.log(v / r.lo) / Math.log(r.hi / r.lo) : (v - r.lo) / (r.hi - r.lo), 0, 1);
function stripLay() { const el = $('#sv'), r = RB.get(ruAt), W = el.clientWidth || SV.W, l = items(ruAt).filter((x) => x.v >= r.lo); SV.W = W; if (!W || !r.steps) return; let pt = 7; for (; pt > 2.4; pt -= .1) { const n = new Map(); let top = 0; for (const x of l) { const c = Math.min(Math.floor(posOf(r, x.at ?? x.v) * W / pt), Math.floor(W / pt) - 1), k = (n.get(c) || 0) + 1; n.set(c, k); if (k > top) top = k; } if (top * pt <= SV.H) break; } const n = new Map(); SV.r = pt * .4; SV.pts = l.slice().sort((a, b) => a.v - b.v).map((x) => { const c = Math.min(Math.floor(posOf(r, x.at ?? x.v) * W / pt), Math.floor(W / pt) - 1), k = n.get(c) || 0; n.set(c, k + 1); return [(c + .5) * pt, SV.H - (k + .5) * pt, x.v]; }); }
const stripDraw = (v) => { const a = [], b = []; SV.pts.forEach(([x, y, q]) => (q >= v ? b : a).push(`M${x.toFixed(1)} ${y.toFixed(1)}h.01`)); $('#sv-a').setAttribute('d', a.join('')); $('#sv-b').setAttribute('d', b.join('')); $$('#sv path').forEach((p) => p.setAttribute('stroke-width', (SV.r * 2).toFixed(2))); };
function stripUi(grow) {
  const r = RB.get(ruAt), c = scopeCfg()[ruAt], on = c.on, v = c.v; if (!r.steps) return; stripLay(); const s = $('#sv'); s.setAttribute('viewBox', `0 0 ${SV.W || 353} ${SV.H}`); stripDraw(on ? v : Infinity); $('#st-l').classList.toggle('off', !on); lineAt2(posOf(r, v)); $('#st-v').textContent = r.val(v); $('#st-l').setAttribute('aria-valuetext', r.val(v)); $('#st-k').innerHTML = r.ticks.map(([q, t], i) => `<span style="left:${(posOf(r, q) * 100).toFixed(2)}%"${i ? '' : ' class="f"'}>${t}</span>`).join('');
  if (grow && !RM) $('#sv-g').animate([{ transform: 'scaleY(0)' }, { transform: 'none' }], { duration: ms(360), easing: CARRY });
}
const lineAt2 = (k) => { $('#st-l').style.setProperty('--x', (k * 100).toFixed(3) + '%'); };
const countTx = (r, n) => r.unit === 'post' ? `${pl(n, 'post')} in 30 days` : r.unit === 'day' ? `${n === 1 ? 'Once' : `${fmt(n)} times`} in 30 days` : `${n === 1 ? 'Once' : `${fmt(n)} times`} in 30 days`;
function sayStrip() { const r = RB.get(ruAt), c = scopeCfg()[ruAt], l = c.on ? past(ruAt, c.v) : [], b = $('#ru-n'); ln($('#ru-c'), !c.on ? 'Off' : l.length ? countTx(r, l.length) : 'None in 30 days'); b.hidden = !l.length; const pickd = ruAt === 'likes' || ruAt === 'comm' || ruAt === 'fol' ? l.slice().sort((a, b9) => b9.v - a.v)[0] : l[0]; notes(pickd ? noteHtml(ruAt, pickd) : `<div class="nt no">${c.on ? 'Nothing has crossed this yet' : 'You won’t get this one'}</div>`); }
function rowSay(id) { const r = RB.get(id), c = scopeCfg()[id], el = $(`.ru[data-r="${id}"]`); $('.sen', el).innerHTML = r.say(c.v); $('.tg', el).setAttribute('aria-checked', c.on); $('.tg', el).setAttribute('aria-label', $('.sen', el).textContent); el.classList.toggle('at', id === ruAt); $('.sen', el).setAttribute('aria-expanded', id === ruAt); }
function alertsUi() {
  const lv = level(), apart = Object.keys(S.al.own).length + Object.keys(S.al.one).length, box = $('#rus');
  if (!box.children.length) { box.innerHTML = RU.map((r) => `<div class="ru" data-r="${r.id}"><button class="sen"></button><button class="tg" role="switch"></button></div>`).join(''); box.querySelector('[data-r="likes"]').after($('#pan')); }
  const x = $('#al-x'), go = lv === 'all' ? (apart ? 'apart' : '') : isOwn() ? 'same' : ''; x.dataset.go = go; x.disabled = !go; ln(x, lv === 'all' ? (apart ? `${apart} custom` : '') : isOwn() ? `Reset to ${upName()}` : `Same as ${upName()}`);
  RU.forEach((r) => rowSay(r.id)); stripUi(rolling); sayStrip(); const w = $('#al-w'); w.style.setProperty('--at', S.al.when); $$('button', w).forEach((b) => b.setAttribute('aria-checked', +b.dataset.w === S.al.when)); $('#al-q').setAttribute('aria-checked', S.al.hush);
}
/* opening another rule: its panel goes to it, and the rows between make way */
function openRule(id) {
  if (id === ruAt || !RB.get(id).steps) return; const rows = $$('#rus > *'), was = rows.map((e) => e.getBoundingClientRect().top); ruAt = id; $(`.ru[data-r="${id}"]`).after($('#pan')); RU.forEach((r) => rowSay(r.id)); rolling = true; stripUi(true); sayStrip(); rolling = false; if (RM) return;
  rows.forEach((e, i) => { if (e.id === 'pan') return; const d = was[i] - e.getBoundingClientRect().top; if (Math.abs(d) > 1) e.animate([{ transform: `translate3d(0, ${d.toFixed(1)}px, 0)` }, { transform: 'none' }], { duration: ms(380), easing: CARRY }); }); $('#pan').animate([{ clipPath: 'inset(0 0 100% 0)' }, { clipPath: 'inset(0 0 -20px 0)' }], { duration: ms(380), easing: CARRY });
}
function setV(id, v, soft) { const c = ownCfg()[id]; if (c.v === v && c.on) return false; c.v = v; c.on = true; saveAl(); rowSay(id); $('#st-v').textContent = RB.get(id).val(v); $('#st-l').setAttribute('aria-valuetext', RB.get(id).val(v)); $('#st-l').classList.remove('off'); if (!soft) lineAt2(posOf(RB.get(id), v)); stripDraw(v); const l = past(id, v); $('#ru-c')._s = null; ln($('#ru-c'), l.length ? countTx(RB.get(id), l.length) : 'None in 30 days'); $('#ru-n').hidden = !l.length; return true; }
$('#s-al').addEventListener('click', (e) => {
  const t = (q) => e.target.closest(q); let b; const go = () => { rolling = true; alertsUi(); rolling = false; };
  if ((b = t('.sen'))) openRule(b.parentElement.dataset.r);
  else if ((b = t('.ru .tg'))) { const id = b.parentElement.dataset.r, c = ownCfg()[id]; c.on = !c.on; saveAl(); buzz(4); if (RB.get(id).steps && id !== ruAt && c.on) openRule(id); go(); }
  else if ((b = t('#al-x'))) { if (b.dataset.go === 'apart') sheet('apart'); else if (b.dataset.go === 'same') { if (S.pick.h) delete S.al.one[S.pick.h]; else delete S.al.own[S.pick.feed]; saveAl(); go(); } }
  else if (t('#ru-n')) sheet('heard');
  else if ((b = t('[data-w]'))) { S.al.when = +b.dataset.w; saveAl(); go(); }
  else if (t('#al-q')) { S.al.hush = !S.al.hush; saveAl(); buzz(4); go(); }
});
{ const el = $('#strip'), L = $('#st-l'); let on = null; const at = (e) => { const q = $('#sv').getBoundingClientRect(); return clamp((e.clientX - q.left) / q.width, 0, 1); }, step = (k) => { const r = RB.get(ruAt); let best = r.steps[0], bd = 9; r.steps.forEach((s) => { const d = Math.abs(posOf(r, s) - k); if (d < bd) { bd = d; best = s; } }); return best; }, move = (e) => { const r = RB.get(ruAt), k = clamp(at(e), posOf(r, r.steps[0]), posOf(r, r.steps[r.steps.length - 1])); lineAt2(k); if (setV(ruAt, step(k), true)) buzz(3); };
  el.addEventListener('pointerdown', (e) => { if (e.button) return; on = { id: e.pointerId, x: e.clientX, y: e.clientY, go: e.pointerType === 'mouse' }; if (on.go) { el.classList.add('drag'); move(e); try { el.setPointerCapture(e.pointerId); } catch {} } });
  el.addEventListener('pointermove', (e) => { if (!on || e.pointerId !== on.id) return; if (!on.go) { const dx = Math.abs(e.clientX - on.x), dy = Math.abs(e.clientY - on.y); if (dx < 5 && dy < 5) return; if (dy > dx) { on = null; return; } on.go = true; el.classList.add('drag'); try { el.setPointerCapture(e.pointerId); } catch {} } move(e); });
  const up = (e) => { if (!on || e.pointerId !== on.id) return; if (!on.go && e.type === 'pointerup') move(e); on = null; el.classList.remove('drag'); const c = scopeCfg()[ruAt]; lineAt2(posOf(RB.get(ruAt), c.v)); rolling = true; alertsHead(); sayStrip(); rolling = false; }; el.addEventListener('pointerup', up); el.addEventListener('pointercancel', up);
  L.addEventListener('keydown', (e) => { const d = { ArrowRight: 1, ArrowUp: 1, ArrowLeft: -1, ArrowDown: -1 }[e.key]; if (!d) return; e.preventDefault(); e.stopPropagation(); const r = RB.get(ruAt), i = r.steps.indexOf(scopeCfg()[ruAt].v); setV(ruAt, r.steps[clamp(i + d, 0, r.steps.length - 1)]); rolling = true; alertsHead(); sayStrip(); rolling = false; }); }
function alertsHead() { const lv = level(), apart = Object.keys(S.al.own).length + Object.keys(S.al.one).length, x = $('#al-x'), go = lv === 'all' ? (apart ? 'apart' : '') : isOwn() ? 'same' : ''; x.dataset.go = go; x.disabled = !go; ln(x, lv === 'all' ? (apart ? `${apart} custom` : '') : isOwn() ? `Reset to ${upName()}` : `Same as ${upName()}`); }

/* every day since you joined, a dot: the more posts, the bigger */
let CL = { n: [], h: [], at: -1 }; const PX = 8.6;
function numsUi() {
  const set = fedPick(), n = Array(DAYN).fill(0), h = Array(DAYN).fill(0), b1 = Array(DAYN).fill(0); let likes = 0, hits = 0, all = 0; const l = []; FED.p.forEach((p, i) => { if (!set.has(p[2])) return; l.push(i); all++; likes += p[5]; n[p[0]]++; if (p[4] >= HOT) { hits++; h[p[0]]++; } if (p[4] >= 200) b1[p[0]] = 1; });
  const H = S.pick.h && FED.h[FI.get(S.pick.h)]; ln($('#nu-s'), `Since ${H ? dm(new Date(H[1] + 'T00:00:00Z')) : dm(D0)}`); slot($('#n-a'), fmt(all)); slot($('#n-b'), big(likes)); slot($('#n-c'), fmt(hits));
  const s = $('#cs'); if (!s.children.length) { s.setAttribute('viewBox', `0 0 ${(COLS * PX).toFixed(1)} ${(7 * PX).toFixed(1)}`); s.innerHTML = Array.from({ length: DAYN - OFF }, (_, d) => `<circle cx="${((Math.floor(d / 7) + .5) * PX).toFixed(2)}" cy="${((d % 7 + .5) * PX).toFixed(2)}" r=".8"/>`).join(''); const out = []; let last = -1; for (let w = 0; w < COLS; w++) { const m = dayAt(OFF + w * 7 + 3).getUTCMonth(); if (OFF + w * 7 > LASTD) break; if (m !== last) { out.push([w, MON[m].slice(0, 3)]); last = m; } } $('#mon').innerHTML = out.filter((x, i) => i === 0 || x[0] - out[i - 1][0] > 2).map(([w, t]) => `<span style="left:${(100 * w / COLS + .4).toFixed(2)}%">${t}</span>`).join(''); void s.getBoundingClientRect(); }
  n.splice(0, OFF); h.splice(0, OFF); b1.splice(0, OFF); const sr = n.filter(Boolean).sort((a, b) => a - b), most = sr[Math.floor(sr.length * .97)] || 1; [...s.children].forEach((c, d) => { c.style.r = (n[d] ? 1.1 + 2.7 * Math.pow(Math.min(1, n[d] / most), 1.15) : .7).toFixed(2) + 'px'; c.setAttribute('class', n[d] ? (b1[d] ? 'hot' : '') : 'z0'); }); CL = { n, h, b1, at: -1 }; calSay(-1); records(set, l);
}
function calSay(d) { CL.at = d; const el = $('#cal'); el.classList.toggle('on', d >= 0); $$('#cs .at').forEach((c) => c.classList.remove('at')); const r9 = rolling; if (d >= 0) { $('#cs').children[d].classList.add('at'); rolling = false; } ln($('#cal-s'), d < 0 ? 'A dot a day, sized by posts. Red: one over 20× usual' : `<b>${DN[dayAt(OFF + d).getUTCDay()]} ${ds(dayAt(OFF + d))}</b>, ${CL.n[d] ? pl(CL.n[d], 'post') : 'no posts'}${CL.h[d] ? `, <em>${CL.h[d]} over 5× usual</em>` : ''}`); rolling = r9; }
{ const el = $('#cal'), sv = $('#cs'); let on = null, bk = 0; const at = (e) => { const q = sv.getBoundingClientRect(), x = clamp(Math.floor((e.clientX - q.left) / q.width * COLS), 0, COLS - 1), y = clamp(Math.floor((e.clientY - q.top) / q.height * 7), 0, 6); return clamp(x * 7 + y, 0, DAYN - OFF - 1); }; el.addEventListener('pointerdown', (e) => { if (e.button) return; on = { id: e.pointerId, x: e.clientX, y: e.clientY, go: false }; clearTimeout(bk); }); el.addEventListener('pointermove', (e) => { if (e.pointerType === 'mouse' && !on) { clearTimeout(bk); const d = at(e); if (d !== CL.at) calSay(d); return; } if (!on || e.pointerId !== on.id) return; if (!on.go) { const dx = Math.abs(e.clientX - on.x), dy = Math.abs(e.clientY - on.y); if (dx < 5 && dy < 5) return; if (dy > dx) { on = null; return; } on.go = true; try { el.setPointerCapture(e.pointerId); } catch {} } const d = at(e); if (d !== CL.at) { calSay(d); buzz(2); } }); const home = () => { rolling = true; calSay(-1); rolling = false; }, up = (e) => { if (!on || e.pointerId !== on.id) return; if (!on.go && e.type === 'pointerup') calSay(at(e)); on = null; if (e.pointerType !== 'mouse') bk = setTimeout(home, 2600); }; el.addEventListener('pointerup', up); el.addEventListener('pointercancel', up); el.addEventListener('pointerleave', (e) => { if (e.pointerType === 'mouse') home(); }); }
/* the records, always the same six, on the same three columns */
function records(set, l) {
  const one = set.size === 1, P = (i) => FED.p[i], H = (i) => FED.h[i][0], best = (k) => l.reduce((m, i) => P(i)[k] > (m < 0 ? -1 : P(m)[k]) ? i : m, -1), most = (f9) => { const c = new Map(); l.forEach((i) => { if (f9(i)) c.set(P(i)[2], (c.get(P(i)[2]) || 0) + 1); }); return [...c].sort((x, y) => y[1] - x[1])[0]; };
  const lk = best(5), lm = best(4), cm = best(6), tall = most(() => 1), hi = most((i) => P(i)[4] >= HOT), fol = [...set].map((i) => [i, FED.h[i][3] - FED.h[i][2]]).sort((x, y) => y[1] - x[1])[0], L = [['Most likes', lk < 0 ? null : H(P(lk)[2]), lk < 0 ? '0' : k3(P(lk)[5])], ['Top multiple', lm < 0 ? null : H(P(lm)[2]), lm < 0 ? '0' : `${fmt(Math.round(P(lm)[4] / 10))}×`], ['Most comments', cm < 0 ? null : H(P(cm)[2]), cm < 0 ? '0' : k3(P(cm)[6])], [one ? 'Posts' : 'Most posts', tall ? H(tall[0]) : null, tall ? fmt(tall[1]) : '0'], [one ? 'Over 5×' : 'Most over 5×', hi ? H(hi[0]) : tall ? H(tall[0]) : null, hi ? fmt(hi[1]) : '0'], ['New followers', fol ? H(fol[0]) : null, fol ? `${fol[1] < 0 ? '−' : '+'}${k3(Math.abs(fol[1]))}` : '0']];
  const box = $('#recs'); if (!box.children.length) box.innerHTML = L.map(() => `<button class="md"><span class="fc3"></span><b></b><span class="ln"></span></button>`).join(''); [...box.children].forEach((m, i) => { const [t, w, n] = L[i]; m.dataset.w = one ? '' : w || ''; m.setAttribute('aria-label', `${t}, ${n}${w ? ', @' + w : ''}`); face($('.fc3', m), w ? art(w) : ''); slot($('b', m), n); ln($('.ln', m), t); });
}
$('#recs').onclick = (e) => { const b = e.target.closest('.md'); if (b && b.dataset.w && feedOf(b.dataset.w)) pick(S.pick.feed || feedOf(b.dataset.w).id, b.dataset.w); };

/* ── lists, when you ask for one ── */
let shAt = '', shutT = 0;
function sheet(k) { shAt = k; clearTimeout(shutT); $('#sht-t').textContent = { spent: S.wk === 0 ? 'Spent this week' : `Spent, ${S.weeks[S.wk].t}`, acc: 'Account', icon: '', heard: '', apart: 'Custom alerts', plan: 'Plan' }[k]; $('#sht').inert = false; layer.setAttribute('data-leaving', ''); $('#shc').scrollTop = 0; sheetUi(); void $('#sht').offsetHeight; requestAnimationFrame(() => { layer.setAttribute('data-sheet', k); shutT = setTimeout(() => layer.removeAttribute('data-leaving'), ms(400)); }); }
const shut = () => { if (!layer.hasAttribute('data-sheet')) return; clearTimeout(shutT); layer.setAttribute('data-leaving', ''); layer.removeAttribute('data-sheet'); shutT = setTimeout(() => layer.removeAttribute('data-leaving'), ms(400)); const s = $('#sht'); if (s.contains(document.activeElement)) document.activeElement.blur(); s.inert = true; s.style.transform = ''; };
function sheetUi() {
  const b = $('#shc'), rowOf = (e) => { if (e.t === 'top') return `<div class="rw"><span class="tx"><b>Tuesday top up</b><span>Back to ${fmt(e.to || 0)}</span></span><span class="amt up">+${fmt(e.n)}</span></div>`; if (e.t === 'buy') return `<div class="rw"><span class="tx"><b>Bitecoins bought</b><span>No expiry</span></span><span class="amt up">+${fmt(e.n)}</span></div>`; return `<div class="rw"><span class="fa">${art(e.h || '')}</span><span class="tx"><b>${esc(e.title || '')}</b><span>${e.t === 'search' ? 'Search' : e.t === 'follow' ? 'File followed' : 'File renewed'}${e.h ? `, @${esc(e.h)}` : ''}</span></span><span class="amt">−${e.n}</span></div>`; };
  if (shAt === 'spent') { const l = events().filter(mine).reverse(), by = new Map(); l.forEach((e) => { if (!by.has(e.day)) by.set(e.day, []); by.get(e.day).push(e); }); b.innerHTML = [...by].map(([d, x]) => `<h4>${S.wk === 0 && d === S.today ? 'Today' : LONG[d]}</h4>${x.map(rowOf).join('')}`).join('') || '<p>Nothing spent here this week.</p>'; }
  if (shAt === 'acc') b.innerHTML = `<div class="rw"><span class="tx"><b>Signed in as</b><span>${esc(host.email() || 'Your account')}</span></span></div><button class="rw" data-do="manage"><span class="tx"><b>Your feeds</b><span>${pl(S.feeders, 'feeder')} in ${pl(S.feeds.length, 'feed')}</span></span>${ic('r')}</button><button class="rw" data-do="help"><span class="tx"><b>Help</b><span>Write to us</span></span>${ic('r')}</button><button class="rw" data-do="out"><span class="tx"><b>Sign out</b></span></button>`;
  if (shAt === 'plan') b.innerHTML = planSheet();
  if (shAt === 'heard') { const l = past(ruAt, scopeCfg()[ruAt].v), F = S.pick.feed && feedBy(S.pick.feed); $('#sht-t').textContent = $(`.ru[data-r="${ruAt}"] .sen`).textContent; b.innerHTML = `<p>${S.pick.h ? '@' + esc(S.pick.h) : F ? esc(F.t) : 'All feeds'}, last 30 days.</p>` + l.map((x) => { const [a, y] = noteTx(ruAt, x); return `<div class="rw"><span class="fa">${art(x.w)}</span><span class="tx"><b>${a}</b><span>${y}</span></span><em>${dayTx(x)}</em></div>`; }).join(''); }
  if (shAt === 'apart') { const tx = (c) => RU.filter((r) => c[r.id].on).map((r) => ({ likes: `Likes ${n1(c.likes.v)}×`, comm: `Comments ${n1(c.comm.v)}×`, fol: `Followers ${n1(c.fol.v)}%`, gap: `${c.gap.v} days quiet`, post: 'Every post' })[r.id]).join(', ') || 'No alerts'; b.innerHTML = '<p>Feeds and feeders with alerts of their own.</p>' + [...Object.keys(S.al.own).map((k) => feedBy(k)).filter(Boolean).map((x) => [`<span class="fa">${fi(x)}</span>`, esc(x.t), tx(S.al.own[x.id]), `f:${x.id}`]), ...Object.keys(S.al.one).filter((w) => known(w)).map((w) => [`<span class="fa">${art(w)}</span>`, '@' + w, tx(S.al.one[w]), `h:${w}`])].map(([a, t2, s2, g]) => `<button class="rw" data-go3="${esc(g)}">${a}<span class="tx"><b>${t2}</b><span>${s2}</span></span>${ic('r')}</button>`).join(''); }
  if (shAt === 'icon') { const F = feedBy(S.pick.feed); if (!F) return; $('#sht-t').textContent = F.t; b.innerHTML = `<button class="rw" data-do="feed"><span class="tx"><b>Feeders, brief and export</b><span>${pl(F.f.length, 'feeder')}</span></span>${ic('r')}</button><div class="icx"><i id="wi">${fi(F)}</i><input id="ic-q" type="text" autocomplete="off" autocapitalize="off" spellcheck="false" placeholder="Search, or type an emoji" aria-label="Search, or type an emoji"></div><div id="ic-b"></div>`; iconUi(); }
}
const known = (w) => S.feeds.some((x) => x.f.includes(w));
function iconUi() {
  const F = feedBy(S.pick.feed), raw = $('#ic-q').value.trim(), q = raw.toLowerCase(), at = host.iconName(F.id, F.t), cell = (n) => `<button class="ec" data-e="${n}" aria-label="${n.replace(/-/g, ' ')}" ${at === n ? 'aria-current="true"' : ''}>${ico(ICONS[n])}</button>`, flat = GROUPS.flatMap(([, l]) => l), words = F.t.toLowerCase().split(/\W+/), typed = /\p{Extended_Pictographic}/u.test(raw) ? (EMOJI[raw.replace(/[\uFE0E\uFE0F]/g, '')] || '') : null; let o = '';
  const find = (ws) => flat.map(([n, k]) => [n, ws.reduce((a, w) => a + (w.length > 1 && k.split(' ').some((x) => x === w) ? 2 : w.length > 2 && k.split(' ').some((x) => x.startsWith(w.replace(/s$/, ''))) ? 1 : 0), 0)]).filter((x) => x[1]).sort((a, b) => b[1] - a[1]).map((x) => x[0]);
  if (q) { const l = find(typed == null ? q.split(/\s+/) : typed.split(/\s+/)).slice(0, 24); o = l.length ? `<div class="eg">${l.map(cell).join('')}</div>` : `<p class="nil">No icon for that. Try another word.</p>`; }
  else { const g = host.guess(F.t), near = [...new Set([g, ...find(words)].filter((n) => n && ICONS[n]))].slice(0, 7); o = `<h4>For ${esc(F.t)}</h4><div class="eg"><button class="ec" data-e="" ${at ? '' : 'aria-current="true"'}>${esc(F.t.slice(0, 2).toUpperCase())}</button>${near.map(cell).join('')}</div>` + GROUPS.map(([n, l]) => `<h4>${n}</h4><div class="eg">${l.map(([e]) => cell(e)).join('')}</div>`).join(''); }
  $('#ic-b').innerHTML = o;
}
$('#shc').addEventListener('input', (e) => { if (e.target.id === 'ic-q') iconUi(); });
$('#shc').addEventListener('click', (e) => {
  const t = (q) => e.target.closest(q); let b;
  if ((b = t('[data-do]'))) { const k = b.dataset.do; if (k === 'out') { if (b.dataset.sure) return host.signOut(); b.dataset.sure = '1'; $('.tx b', b).textContent = 'Tap again to sign out'; return; } if (k === 'help') return host.help(); const F = k === 'feed' ? S.pick.feed : null; shut(); setTimeout(() => host.manage(F), ms(220)); }
  else if ((b = t('[data-fo]'))) { const g = b.parentElement, op = g.classList.toggle('op'); b.setAttribute('aria-expanded', op); }
  else if ((b = t('[data-go3]'))) { const id = b.dataset.go3; shut(); setTimeout(() => { if (id.startsWith('f:')) pick(id.slice(2)); else { const w = id.slice(2); pick(feedOf(w).id, w); } }, ms(260)); }
  else if ((b = t('.ec'))) { const F = feedBy(S.pick.feed); host.setIcon(F.id, b.dataset.e, ICONS[b.dataset.e] || ''); $$('#ic-b .ec').forEach((x) => x.removeAttribute('aria-current')); b.setAttribute('aria-current', 'true'); $('#wi').innerHTML = fi(F); const p2 = $(`#rail2 [data-p="f:${F.id}"] .p`); if (p2) p2.innerHTML = fi(F); if (!RM) [$('#wi'), p2].forEach((x) => x && x.animate([{ transform: 'scale(.84)' }, { transform: 'none' }], { duration: ms(300), easing: 'cubic-bezier(.34,1.56,.64,1)' })); }
});
$('#sht-x').innerHTML = ic('x'); $('#sht-x').onclick = shut; $('#scr').onclick = shut;
{ let shd = null; const h = $('#sht-h'), el = $('#sht'); h.addEventListener('pointerdown', (e) => { if (e.target.closest('button') || wide() || e.button) return; shd = { y: e.clientY, id: e.pointerId, t: e.timeStamp }; try { h.setPointerCapture(e.pointerId); } catch {} }); h.addEventListener('pointermove', (e) => { if (!shd || e.pointerId !== shd.id) return; el.style.transition = 'none'; el.style.transform = `translate3d(0, ${Math.max(0, e.clientY - shd.y)}px, 0)`; }); const up = (e) => { if (!shd || e.pointerId !== shd.id) return; const d = e.clientY - shd.y, v = d / Math.max(1, e.timeStamp - shd.t); shd = null; el.style.transition = ''; void el.offsetWidth; el.style.transform = ''; if (d > 80 || (d > 24 && v > .5)) shut(); }; h.addEventListener('pointerup', up); h.addEventListener('pointercancel', up); }

/* ── the top: me, and the look ── */
const lookUi = () => { const o = ['light', 'auto', 'dark'], sg = $('#look'), cur = host.theme(); sg.style.setProperty('--at', Math.max(0, o.indexOf(cur))); $$('button', sg).forEach((b) => b.setAttribute('aria-checked', b.dataset.th === cur)); };
$('#go-acc').onclick = () => sheet('acc');
$('#look').onclick = (e) => { const b = e.target.closest('[data-th]'); if (!b) return; const q = b.getBoundingClientRect(); host.setTheme(b.dataset.th, { x: q.left + q.width / 2, y: q.top + q.height / 2 }); lookUi(); };

/* ── plan. Rupees pay for feeders, once a month; Bitecoins pay for asking. The only thread between them: a feeder brings 100 Bitecoins a week. Nothing is charged in the beta: what the month would come to is counted from what is tracked ── */
const RATE = 999, OVER = 20, FREE = 30, inr = (n) => '₹' + n.toLocaleString('en-IN');
const monthFrom = () => { const t = new Date(FED.today + 'T00:00:00Z'); return Math.round((Date.UTC(t.getUTCFullYear(), t.getUTCMonth(), 1) - D0.getTime()) / 864e5); }, monthName = () => MON[new Date(FED.today + 'T00:00:00Z').getUTCMonth()];
const postsOf = (w) => { if (!PIN.has(w)) { const f9 = FI.get(w), m = monthFrom(); PIN.set(w, f9 == null ? 0 : FED.p.reduce((a, p) => a + (p[2] === f9 && p[0] >= m ? 1 : 0), 0)); } return PIN.get(w); }, overOf = (w) => Math.max(0, postsOf(w) - FREE);
const bill = () => { const all = [...new Set(S.feeds.flatMap((x) => x.f))], over = all.reduce((a, w) => a + overOf(w), 0); return { n: all.length, over, base: all.length * RATE, extra: over * OVER, total: all.length * RATE + over * OVER }; };
function planUi() { ln($('#pl-n'), 'Free'); $('#pl-s').textContent = `${pl(S.feeders, 'feeder')}, monthly`; $('#pl-d').textContent = 'in the beta'; }
$('#go-plan').onclick = () => sheet('plan');
const planRow = (w) => { const n = postsOf(w), o = overOf(w); return `<div class="rw"><span class="fa">${art(w)}</span><span class="tx"><b>@${esc(w)}</b><span>${o ? `${n} posts, ${o} over` : pl(n, 'post')}</span></span></div>`; };
const planSheet = () => { const q = bill(); return `<div class="bill"><b class="fig">Free</b><span>In the beta. Nothing is charged.</span></div>
  <h4>What ${monthName()} would come to so far</h4><div class="bl"><span>${pl(q.n, 'feeder')} × ${inr(RATE)}</span><b>${inr(q.base)}</b></div><div class="bl"><span>${pl(q.over, 'post')} over ${FREE} a feeder × ${inr(OVER)}</span><b>${inr(q.extra)}</b></div><div class="bl tt"><span>In all</span><b>${inr(q.total)}</b></div>
  <h4>Feeders</h4><div>${S.feeds.map((x) => `<div class="fdg"><button class="rw" data-fo="${esc(x.id)}" aria-expanded="false"><span class="fa">${fi(x)}</span><span class="tx"><b>${esc(x.t)}</b><span>${pl(x.f.length, 'feeder')}</span></span>${ic('r')}</button><div class="in2"><div>${x.f.map(planRow).join('')}</div></div></div>`).join('')}<button class="rw" data-do="manage" style="border-top:1px solid var(--line)"><span class="tx"><b>Add a feeder</b><span>100 Bitecoins a week</span></span>${ic('plus')}</button></div>
  <p class="nt2"><b>${fmt(cap())} Bitecoins</b> come with your feeders every Tuesday, for searches and files. Tracking never uses them: every post is checked on time, whatever your balance.</p>
  <h4>Billing</h4><div class="rw"><span class="tx"><b>Payment method</b><span>Not needed in the beta</span></span></div><div class="rw"><span class="tx"><b>Invoices</b><span>None yet</span></span></div>`; };

function lsUi() { lookUi(); planUi(); $('#av').innerHTML = PERSON(21); $('#me-s').textContent = 'Account'; }
function fill() { whoUi(); weekUi(); if (!loaded) return; alertsUi(); numsUi(); }
const tuckRail = () => { const c = $('#col'); c.classList.toggle('stuck', wide() ? c.scrollTop > 4 : c.getBoundingClientRect().top < $('#rail2').getBoundingClientRect().top - .5); };
let sayT = 0; const say = (t) => { const el = $('#say'); el.textContent = t; el.classList.add('on'); clearTimeout(sayT); sayT = setTimeout(() => el.classList.remove('on'), 2200); };
const lock = async (fn) => { if (busy) return; busy = true; try { await fn(); } finally { busy = false; } };
/* a face that can't be shown gives way to its letters */
[root, layer].forEach((r) => r.addEventListener('error', (e) => { const im = e.target; if (im instanceof HTMLImageElement && im.dataset.ini) im.replaceWith(im.dataset.ini); }, true));
const gauge = () => { measure(); rest(); };
/* the rail dealt from nothing, and everything under it, at once */
function rebuild() { if (!awake) return; lsUi(); packs(); measure(); rest(); $('#rail2').replaceChildren(); railUi(); fill(); tuckRail(); }
let sizeT = 0; const RO = new ResizeObserver(() => { clearTimeout(sizeT); sizeT = setTimeout(() => { if (!awake || busy) return; if (stale()) gauge(); if (loaded) stripUi(false); tuckRail(); }, 120); });
const onKey = (e) => {
  if (e.key === 'Escape' && layer.hasAttribute('data-sheet')) return shut();
  if (e.metaKey || e.ctrlKey || e.altKey || e.defaultPrevented || layer.hasAttribute('data-sheet') || (e.target.matches && e.target.matches('input, textarea, [contenteditable]'))) return;
  if (e.key === 'ArrowLeft') step1(1); else if (e.key === 'ArrowRight') step1(-1);
};
const UNLOCK = ['pointerup', 'touchend', 'click', 'keydown'];
function listen(on) {
  const f = on ? 'addEventListener' : 'removeEventListener'; window[f]('scroll', onScroll, { passive: true }); window[f]('keydown', onKey); UNLOCK.forEach((t) => window[f](t, unlock, { capture: true, passive: true })); WIDE_Q[f]('change', rebuild);
  if (on) RO.observe(root); else RO.disconnect();
}
$('#col').addEventListener('scroll', tuckRail, { passive: true });

/* what came in since: a week that turned over while you were away is topped up as you look at it */
async function tuesday(n) { await lock(async () => { const was = Math.max(0, bal() - n); B.set({ eaten: E(was) }); B.snap(); setNum(was); await wait(760); if (!awake) return; await refill(n); await swallow(bal(), .6); $('#sr').textContent = `${fmt(bal())} Bitecoins`; }); if (awake) rest(); }
const api = {
  // the feeds and their faces
  setFeeds(feeds) {
    if (S.src === feeds) return; S.src = feeds;
    S.feeds = feeds.map((x) => ({ id: x.id, t: x.title, f: x.feeders.map((y) => y.handle) })); S.faces = new Map(feeds.flatMap((x) => x.feeders.filter((y) => y.profilePicUrl).map((y) => [y.handle, y.profilePicUrl]))); S.feeders = new Set(S.feeds.flatMap((x) => x.f)).size;
    if ((S.pick.feed && !feedBy(S.pick.feed)) || (S.pick.h && !known(S.pick.h))) S.pick = { feed: null, h: null };
    root.toggleAttribute('data-empty', !S.feeders); IT.clear(); PIN.clear(); rebuild();
  },
  // how many are left, and the weeks they were spent in
  setLedger(l) {
    root.setAttribute('data-known', ''); S.weekly = l.weekly; S.bought = l.bought; S.full = Math.max(1, l.full);
    S.weeks = l.weeks.map((w) => ({ t: w.t, d: w.d })); S.today = l.today; S.wk = Math.min(S.wk, S.weeks.length - 1);
    S.topped = l.topped || 0; if (awake && !busy) { if (G) rest(); fill(); }
  },
  // the posts, as they came to the page
  setData(d) {
    if (FED === d) return; FED = d; FI = new Map(d.h.map((x, i) => [x[0], i])); D0 = new Date(d.began + 'T00:00:00Z'); LASTD = Math.max(0, Math.round((Date.parse(d.today + 'T00:00:00Z') - D0.getTime()) / 864e5)); FROM = LASTD - 29; DAYN = LASTD + 1; OFF = Math.max(0, Math.ceil((DAYN - 53 * 7) / 7) * 7); COLS = Math.max(26, Math.ceil((DAYN - OFF) / 7));
    IT.clear(); PIN.clear(); $('#cs').replaceChildren(); loaded = true; root.setAttribute('data-loaded', ''); if (awake) { fill(); tuckRail(); }
  },
  // the pick, made somewhere else (the dock, the side rail)
  setPick(feed, h) { if (feed && !feedBy(feed)) return; const go = () => pick(feed, h, true); if (awake) go(); else S.pick = { feed, h }; },
  setLook: () => lookUi(),
  say,
  wake(quiet) {
    if (awake) return; awake = true; listen(true); rebuild();
    if (!quiet && !RM) { if (!wide()) { arrive(); $('#col').animate([{ transform: 'translate3d(0, 18px, 0)' }, { transform: 'none' }], { duration: ms(380), easing: 'cubic-bezier(.16,.9,.2,1)' }); } else setTimeout(sweep, 200); }
    document.fonts?.ready?.then(() => { if (awake && !busy && P === 0) gauge(); });
    if (S.topped > 0 && !RM) { const n = S.topped; S.topped = 0; host.toppedSeen(); tuesday(n); } else if (S.topped) { S.topped = 0; host.toppedSeen(); }
  },
  sleep() { if (!awake) return; awake = false; listen(false); clearTimeout(RW.back); unplay(); layer.removeAttribute('data-sheet'); layer.removeAttribute('data-leaving'); $('#sht').inert = true; $('#say').classList.remove('on'); },
  unmount() { api.sleep(); clearTimeout(sizeT); clearTimeout(hotT); root.replaceChildren(); layer.replaceChildren(); },
};
return api;
}
