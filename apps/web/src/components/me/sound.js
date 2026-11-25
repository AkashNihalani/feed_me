/* The sounds, made here, nothing sampled. The dots sit on the fifth (E and A), so the swallow and the landing can come home to D.
   A phone's speaker plays little under 300 Hz, so a hit has to be made of what lies over it; the weight under that is for headphones and laptops. */
export const Tones = (() => {
  const TAU = Math.PI * 2, noiseOf = (rnd) => rnd() * 2 - 1;
  const G = { wa: [376, 508], ka: [486, 356], glide: .042, at: .0036, hold: .018, dec: .0135, h2: [.12, .34], h3: .08, tick: .6, felt: .1, lp: 5200, gulp: [430, 214] };
  const close = [[.0067, .16], [.0121, .1], [.0203, .06]];
  function finish(d, sr, peak, lp, room, drive) {
    const n = d.length; let s = 0, pk = 0; const k = 1 - Math.exp(-TAU * lp / sr);
    for (const [dl, a] of room || close) { const o = Math.round(dl * sr); for (let i = n - 1; i >= o; i--) d[i] += d[i - o] * a; }
    for (let i = 0; i < n; i++) { s += (d[i] - s) * k; d[i] = s * Math.min(1, (n - i) / (sr * .02)); pk = Math.max(pk, Math.abs(d[i])); }
    if (drive) { const z = Math.tanh(drive); for (let i = 0; i < n; i++) d[i] = Math.tanh(d[i] / pk * drive) / z; pk = 1; }
    for (let i = 0; i < n; i++) d[i] = d[i] / pk * peak; return d;
  }
  /* one dot: the body says wa or ka, the tick is the bead touching */
  function dot(kind, sr, rnd) {
    const g = G, n = Math.round(sr * .15), d = new Float32Array(n), [f0, f1] = kind === 'wa' ? g.wa : g.ka, tune = 1 + (rnd() - .5) * .024, up = kind === 'wa'; let ph = 0, lp = 0;
    const ticks = [[1240, .58, .0085], [2120, .3, .005], [3420, .12, .0028]].map(([f, a, t]) => [f * (1 + (rnd() - .5) * .16), a * (.8 + .4 * rnd()), t, rnd() * .6]);
    for (let i = 0; i < n; i++) {
      const t = i / sr, q = Math.min(1, t / g.glide), gl = 1 - (1 - q) * (1 - q), f = (f0 + (f1 - f0) * gl) * tune, open = up ? gl : 1 - gl; ph += TAU * f / sr;
      const env = (t < g.at ? .5 - .5 * Math.cos(Math.PI * t / g.at) : 1) * (t < g.at + g.hold ? 1 : Math.exp(-(t - g.at - g.hold) / g.dec));
      let v = env * (Math.sin(ph) + (g.h2[0] + (g.h2[1] - g.h2[0]) * open) * Math.sin(2 * ph) + g.h3 * open * Math.sin(3 * ph));
      for (const [tf, a, tt, p0] of ticks) v += g.tick * a * Math.exp(-t / tt) * Math.sin(TAU * tf * t + p0) * Math.min(1, t / .0009);
      lp += (noiseOf(rnd) - lp) * .22; v += g.felt * lp * Math.exp(-t / .0035) * 2; d[i] = v;
    }
    return finish(d, sr, .82, g.lp);
  }
  /* the end of eating: one swallow, down, and it comes to rest on D */
  function gulp(sr, rnd) {
    const g = G, n = Math.round(sr * .5), d = new Float32Array(n), [f0, f1] = g.gulp; let ph = 0, lp = 0, p2 = 0;
    for (let i = 0; i < n; i++) {
      const t = i / sr, q = Math.min(1, t / .07), f = f0 * Math.pow(f1 / f0, q * q * (3 - 2 * q)); ph += TAU * f / sr;
      let v = Math.min(1, t / .006) * Math.exp(-Math.max(0, t - .034) / .02) * (Math.sin(ph) + .34 * Math.sin(2 * ph) + .1 * Math.sin(3 * ph)) * .86;
      const u = t - .072; if (u > 0) { const a = Math.min(1, u / .004); p2 += TAU * 293.7 * (1 + .05 * Math.exp(-u / .012)) / sr; v += a * (Math.exp(-u / .085) * (Math.sin(p2) + .22 * Math.sin(2 * p2)) * .95 + Math.exp(-u / .11) * .55 * Math.sin(p2 / 2) + Math.exp(-u / .06) * .42 * Math.sin(p2 * 1.5) + Math.exp(-u / .05) * .2 * Math.sin(p2 * 2)); lp += (noiseOf(rnd) - lp) * .12; v += lp * 1.6 * Math.exp(-u / .005); }
      d[i] = v;
    }
    return finish(d, sr, .86, 3600);
  }
  /* the end of a feed: a weight set down. No ring, no crack, no note: a low drop, dull, over in a fifth of a second. A phone can't play its bottom, so the drop is driven until its overtones (two, three and four times it) carry it, and a dull knock sits where a phone is strong */
  const END = {
    /* a fist on a heavy table, with the mouth's last ka running into it */
    KaThump: { ka: 1, lo: 92, d1: 230, t1: .011, d2: 74, t2: .034, decay: .05, at: .0016, drive: 1.5, even: .5, knock: [[186, .9, .042], [268, .95, .036], [372, .66, .028], [508, .34, .018], [690, .16, .011]], dull: [340, 2.1, .022], touch: [1100, .8, .0032], lp: 1900, room: [[.009, .16], [.017, .11], [.029, .07]] },
  }, PRE = .1;
  function end(which, sr, rnd) {
    const g = END[which], n = Math.round(sr * (PRE + .6)), d = new Float32Array(n), kn = g.knock.map((m) => [...m, 0]); let ph = 0, n1 = 0, n2 = 0, t1 = 0, kp = 0, s1 = 0, s2 = 0, pk = 0; const kd = 1 - Math.exp(-TAU * g.dull[0] / sr), kt = 1 - Math.exp(-TAU * g.touch[0] / sr), z0 = Math.round(sr * PRE);
    for (let i = z0; i < n; i++) {
      const u = (i - z0) / sr, f = g.lo + g.d1 * Math.exp(-u / g.t1) + g.d2 * Math.exp(-u / g.t2), on = u < g.at ? .5 - .5 * Math.cos(Math.PI * u / g.at) : 1; ph += TAU * f / sr;
      let c = on * Math.exp(-u / g.decay) * Math.sin(ph); c = Math.tanh(g.drive * (c + g.even * c * c)) / Math.tanh(g.drive);
      let v = c; for (const m of kn) { m[3] += TAU * m[0] * (1 + .24 * Math.exp(-u / .01)) / sr; v += .55 * on * m[1] * Math.exp(-u / m[2]) * Math.sin(m[3]); }
      const w = noiseOf(rnd); n1 += (w - n1) * kd; n2 += (n1 - n2) * kd; v += g.dull[1] * 3 * n2 * on * Math.exp(-u / g.dull[2]); t1 += (w - t1) * kt; v += g.touch[1] * t1 * Math.exp(-u / g.touch[2]);
      d[i] = v;
    }
    for (const [dl, a] of g.room) { const o = Math.round(dl * sr); for (let i = n - 1; i >= o; i--) d[i] += d[i - o] * a; }
    const kl = 1 - Math.exp(-TAU * g.lp / sr); for (let i = 0; i < n; i++) { s1 += (d[i] - s1) * kl; s2 += (s1 - s2) * kl; d[i] = s2 * Math.min(1, (n - i) / (sr * .03)); pk = Math.max(pk, Math.abs(d[i])); } const dz = Math.tanh(1.7); for (let i = 0; i < n; i++) d[i] = Math.tanh(d[i] / pk * 1.7) / dz * .98;
    if (g.ka) { const k = dot('ka', sr, rnd), o = Math.round(sr * (PRE - .082)); for (let i = 0; i < k.length && o + i < n; i++) d[o + i] += k[i] * .4; for (let i = 0; i < n; i++) d[i] = Math.max(-.99, Math.min(.99, d[i])); }
    return d;
  }
  return { ENDS: Object.keys(END), PRE, dot, gulp, end };
})();
