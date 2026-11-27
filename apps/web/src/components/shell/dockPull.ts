/* ─────────────────────────────────────────────────────────────
   DOCK PULL — the dock's third gesture: hold it for the tab's
   choice (Lead's range, Feed's zoom, Read's part), then slide.
   Its pill, its springs and its ring serve the tab swipe too
   (Dock). (It began as a pull down; a pull toward the screen's
   foot ran into iOS's own gestures there, Reachability and
   Safari's toolbar, so a hold opens it now: `hold`.)

   Made of the dock's own parts. As the thumb rests, the bar
   presses in, the hero's ring is drawn round in the accent as far
   as the window you're on reaches (the ring a sideways swipe
   draws in ink), and one small pill, the tabs' own a size up, comes up
   over the bar, clear of the thumb, with the tab's choices. It opens on where you are. Slide and the mark goes
   with the thumb, a stop at a time, right for longer and left for
   shorter (the arrow keys' order), the ring sweeping with it; on
   a page that can keep up (Lead), the page follows the thumb, so
   nothing is left to do on letting go: back and forth without
   lifting, to compare.

   One rAF loop, transforms alone. The page's own change waits
   until the mark is nearly home, so its work never holds the mark
   up. Nothing here goes below the bar or touches the screen's
   edges (iOS would paint its bars solid).
   ───────────────────────────────────────────────────────────── */

import { CARRY_EASE_CSS } from '@/lib/feedStream/motion';
import { playHaptic } from '@/lib/haptics';
import { ms } from '@/lib/tempo';

// pulled this far (pt) it is open; back up past the second, shut again
export const PULL_ARM = 40;
const PULL_DISARM = 24;
// a flick: down and off again this soon (ms), at least this far, and straight
const FLICK_PT = 14;
const FLICK_MS = 260;
const FLICK_SIDE = 22;
// the bar presses in this much, all of it by this far
export const PRESS = 0.035;
const PRESS_AT = 52;
// what the finger draws follows it a breath behind (s), as the swipe's ring does
const LAG = 0.05;
// the pill's way up, and how much of it the pull itself covers (opening does the rest)
const RISE = 14;
const RISE_BEFORE = 0.7;
// one choice to the next along the slide (pt): less where the thumb has less room
const STEP_MAX = 54;
const STEP_MIN = 34;
// how far the mark leans toward the next stop before it goes (of a stop)
const LEAN = 0.16;
// the page follows once the mark is nearly there
const FOLLOW_MS = 120;
// a flick's answer is seen this long
const FLICK_HOLD_MS = 420;
// a page that moves as a whole (Feed's zoom) goes once the pill is away
const AFTER_MS = ms(200);

const clamp = (value: number, low: number, high: number) => Math.min(high, Math.max(low, value));
const still = () => typeof window !== 'undefined' && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

export type PullDom = {
  bar: HTMLElement;
  face: HTMLElement;
  ring: { right: HTMLElement | null; left: HTMLElement | null; lead: HTMLElement | null; start: HTMLElement | null; end: HTMLElement | null };
  // the pill, its mark (a window) and the bright words the mark shows through it
  pill: HTMLElement;
  mark: HTMLElement;
  bright: HTMLElement;
};

export type PullChoice = {
  count: number;
  at: number;
  last: number;
  // the page follows the thumb (else it goes on letting go)
  live: boolean;
  onPick: (index: number, way: 1 | -1) => void;
  // over: where it was left
  onDone: (index: number) => void;
};

/* ── the pill, for this pull and for the tab swipe (Dock): up by `r` (0 → 1), its mark `x` px along. The bright words
   ride back by as much as the mark goes, so a word turns under its edge as it passes ── */
export const pillAt = (r: number) => `translate3d(0, ${(RISE * (1 - r)).toFixed(2)}px, 0) scale(${(0.9 + 0.1 * clamp(r, 0, 1)).toFixed(4)})`;
export const pillShown = (r: number) => clamp(r * 2.2, 0, 1);
export function posePill(pill: HTMLElement, mark: HTMLElement, bright: HTMLElement, x: number, r: number) {
  pill.style.visibility = r > 0.004 ? 'visible' : 'hidden';
  pill.style.transform = pillAt(r);
  pill.style.opacity = pillShown(r).toFixed(3);
  mark.style.transform = `translate3d(${x.toFixed(2)}px, 0, 0)`;
  bright.style.transform = `translate3d(${(-x).toFixed(2)}px, 0, 0)`;
}

// the bar, pressed in, springs home
export function springBar(bar: HTMLElement, press: number) {
  bar.style.transform = '';
  if (press > 0.001 && !still()) {
    bar.animate([{ transform: `scale(${(1 - press).toFixed(4)})` }, { transform: 'scale(1)' }], { duration: ms(340), easing: 'cubic-bezier(.3, 1.3, .5, 1)' });
  }
}

// the ring, closed in the accent, gives itself back to the hero's own (drawn whole under it)
export function coolRing(bar: HTMLElement, done: () => void) {
  const ring = bar.querySelector<HTMLElement>('.fm-dock-ring');
  if (!ring || still()) {
    done();
    return;
  }
  const cool = ring.animate([{ opacity: 1 }, { opacity: 0 }], { duration: ms(260), easing: 'linear', fill: 'forwards' });
  cool.finished.then(() => {
    done();
    cool.cancel();
  }, () => {});
}

// critically damped: it arrives, never past
export class Glide {
  v = 0;

  constructor(public x: number, public to = x) {}

  step(dt: number, k: number) {
    const c = 2 * Math.sqrt(k);
    for (let left = dt; left > 0; left -= 0.008) {
      const h = Math.min(0.008, left);
      this.v += (k * (this.to - this.x) - c * this.v) * h;
      this.x += this.v * h;
    }
  }

  set(x: number) {
    this.x = this.to = x;
    this.v = 0;
  }

  get moving() {
    return Math.abs(this.to - this.x) > 0.002 || Math.abs(this.v) > 0.01;
  }
}

export class DockPull {
  private dom: PullDom | null = null;
  private choice: PullChoice | null = null;
  private raf = 0;
  private lastT = 0;
  private down = 0;
  private p = 0;
  private pTo = 0;
  private press = 0;
  private open = false;
  private at = 0;
  // where the page is (a live one): it follows `at` a beat behind
  private shown = 0;
  private openX = 0;
  private thumbX = 0;
  private stepL = STEP_MAX;
  private stepR = STEP_MAX;
  private lean = 0;
  private t0 = 0;
  private x0 = 0;
  private seg = 62;
  private rise = new Glide(0);
  private mark = new Glide(0);
  private ring = new Glide(0);
  private taken = false;
  // opened by a hold (the slide alone moves it from there), not by a pull
  private byHold = false;
  private follow = 0;
  private timer = 0;
  // the finger is on it
  active = false;

  // (`t0`: when the finger landed)
  begin(dom: PullDom, choice: PullChoice, x: number, t0: number) {
    this.stop();
    this.dom = dom;
    this.choice = choice;
    this.active = true;
    this.open = false;
    this.taken = false;
    this.byHold = false;
    this.at = this.shown = choice.at;
    this.lean = 0;
    this.down = this.p = this.pTo = this.press = 0;
    this.thumbX = x;
    this.t0 = t0;
    this.x0 = x;
    this.seg = dom.bright.offsetWidth / Math.max(1, choice.count) || 62;
    this.rise.set(0);
    this.ring.set(0);
    this.mark.set(choice.at);
    dom.bar.dataset.pulling = '';
    delete dom.bar.dataset.pulled;
    this.run();
  }

  /* the thumb rested on the dock: it opens at once where the thumb is, the bar pressed in as far as a pull would have
     taken it, and from here only the slide moves it (nothing heads for the screen's foot) */
  hold(x: number) {
    if (!this.active || !this.choice) return;
    this.byHold = true;
    this.thumbX = x;
    this.down = PULL_ARM;
    this.pTo = 1;
    this.openAt(x);
    this.run();
  }

  move(down: number, x: number) {
    if (!this.active || !this.choice) return;
    if (this.byHold) {
      this.thumbX = x;
      if (this.open) this.slide(x);
      this.run();
      return;
    }
    this.down = Math.max(0, down);
    this.thumbX = x;
    this.pTo = clamp((this.down - 6) / (PULL_ARM - 6), 0, 1);
    if (!this.open && this.down >= PULL_ARM) this.openAt(x);
    else if (this.open && this.down < PULL_DISARM) this.shut();
    if (this.open) this.slide(x);
    this.run();
  }

  private openAt(x: number) {
    const { choice } = this;
    if (!choice) return;
    this.open = true;
    this.openX = x;
    // (a stop is as far as the thumb has room for, either way)
    const room = window.innerWidth - 22;
    const right = choice.count - 1 - choice.at;
    this.stepR = right > 0 ? clamp((room - x) / right, STEP_MIN, STEP_MAX) : STEP_MAX;
    this.stepL = choice.at > 0 ? clamp((x - 22) / choice.at, STEP_MIN, STEP_MAX) : STEP_MAX;
    playHaptic('snapLock');
  }

  // pushed back up: as it was
  private shut() {
    this.open = false;
    this.lean = 0;
    this.pick(this.choice!.at);
  }

  // a stop holds until the thumb is past halfway to the next, the mark leaning that way as it nears
  private slide(x: number) {
    const { choice } = this;
    if (!choice) return;
    const dx = x - this.openX;
    const raw = clamp(choice.at + dx / (dx < 0 ? this.stepL : this.stepR), -0.45, choice.count - 0.55);
    const next = clamp(Math.round(raw), 0, choice.count - 1);
    const off = raw - next;
    const end = (next === 0 && off < 0) || (next === choice.count - 1 && off > 0);
    this.lean = end ? 0 : Math.sign(off) * LEAN * Math.min(1, Math.abs(off) * 2) ** 2;
    if (next !== this.at) this.pick(next);
  }

  private pick(index: number) {
    if (index === this.at) return;
    this.at = index;
    playHaptic('snapLock');
    if (!this.choice?.live) return;
    window.clearTimeout(this.follow);
    this.follow = window.setTimeout(() => this.toPage(), FOLLOW_MS);
  }

  private toPage() {
    window.clearTimeout(this.follow);
    if (!this.choice || this.shown === this.at) return;
    const way = this.at > this.shown ? 1 : -1;
    this.shown = this.at;
    this.choice.onPick(this.at, way);
  }

  end(t: number, cancelled = false) {
    const { choice } = this;
    if (!this.active || !choice) return;
    if (!cancelled && !this.byHold && t - this.t0 < FLICK_MS && this.down > FLICK_PT && this.at === choice.at && Math.abs(this.thumbX - this.x0) < FLICK_SIDE) {
      // a flick: the pill opens all the way, its mark goes back to the one before, and it is let go once that is seen
      this.lean = 0;
      if (!this.open) this.openAt(this.thumbX);
      this.active = false;
      this.pTo = 1;
      this.pick(choice.last);
      this.run();
      this.timer = window.setTimeout(() => this.letGo(true), still() ? 0 : FLICK_HOLD_MS);
      return;
    }
    if (cancelled) this.shut();
    this.letGo(!cancelled && this.open);
  }

  private letGo(kept: boolean) {
    const { dom, choice } = this;
    if (!dom || !choice) return;
    this.active = false;
    this.open = false;
    this.lean = 0;
    this.down = 0;
    this.pTo = 0;
    if (!kept) this.at = choice.at;
    this.taken = kept && this.at !== choice.at;
    const { at, taken } = this;
    this.springBack();
    if (taken) {
      playHaptic('pick');
      this.pressHero();
    }
    if (choice.live) this.toPage();
    else if (taken) window.setTimeout(() => choice.onPick(at, at > choice.at ? 1 : -1), still() ? 0 : AFTER_MS);
    choice.onDone(at);
    this.run();
  }

  private springBack() {
    const { press } = this;
    this.press = 0;
    if (this.dom) springBar(this.dom.bar, press);
  }

  // the hero takes it with the press a closed swipe gives it
  private pressHero() {
    if (still()) return;
    this.dom?.face.animate(
      [{ transform: 'scale(1)' }, { transform: 'scale(0.92)', offset: 0.32 }, { transform: 'scale(1.02)', offset: 0.72 }, { transform: 'scale(1)' }],
      { duration: ms(600), easing: CARRY_EASE_CSS },
    );
  }

  // whatever was under way gives way to a new pull
  private stop() {
    cancelAnimationFrame(this.raf);
    this.raf = 0;
    window.clearTimeout(this.timer);
    if (this.choice?.live) this.toPage();
    this.active = false;
    this.open = false;
    const { dom } = this;
    if (!dom) return;
    dom.bar.querySelector<HTMLElement>('.fm-dock-ring')?.getAnimations().forEach((animation) => animation.cancel());
    delete dom.bar.dataset.pulled;
    delete dom.bar.dataset.pulling;
  }

  dispose() {
    this.stop();
    window.clearTimeout(this.follow);
  }

  private run() {
    if (this.raf) return;
    this.lastT = performance.now();
    this.raf = requestAnimationFrame(this.frame);
  }

  private frame = (now: number) => {
    const { choice } = this;
    if (!choice) return;
    const dt = clamp((now - this.lastT) / 1000, 0.004, 0.04);
    this.lastT = now;
    const ease = 1 - Math.exp(-dt / LAG);
    this.p += (this.pTo - this.p) * ease;
    if (Math.abs(this.pTo - this.p) < 0.002) this.p = this.pTo;
    const held = this.active || this.open;
    if (held) this.press += (clamp(this.down / PRESS_AT, 0, 1) * PRESS - this.press) * ease;
    this.rise.to = this.open ? 1 : held ? RISE_BEFORE * this.p : 0;
    this.rise.step(dt, held ? 460 : 260);
    this.mark.to = this.at + this.lean;
    this.mark.step(dt, 520);
    // the ring: as far round as the window reaches, drawn by the pull, swept by the slide; taken, it closes
    const reach = clamp((this.at + this.lean + 1) / choice.count, 0.06, 1);
    this.ring.to = held ? (this.open ? reach : this.p * reach) : this.taken ? 1 : 0;
    this.ring.step(dt, held ? 620 : 300);
    this.write();
    if (held || this.rise.moving || this.ring.moving || this.mark.moving || this.p > 0.002) {
      this.raf = requestAnimationFrame(this.frame);
      return;
    }
    this.raf = 0;
    this.rest();
  };

  private write() {
    const { dom } = this;
    if (!dom) return;
    const r = this.rise.x;
    if (this.active || this.open) dom.bar.style.transform = this.press > 0.0004 ? `scale(${(1 - this.press).toFixed(4)})` : '';
    posePill(dom.pill, dom.mark, dom.bright, this.mark.x * this.seg, r);
    this.paintRing(this.ring.x);
  }

  // the hero's ring drawn round (Dock's own halves: rotations alone)
  private paintRing(k: number) {
    const { dom } = this;
    if (!dom) return;
    const angle = 360 * clamp(k, 0, 1);
    const { right, left, lead, start, end } = dom.ring;
    if (!right || !left || !lead || !start || !end) return;
    right.style.transform = `rotate(${(-180 + Math.min(angle, 180)).toFixed(2)}deg)`;
    right.style.opacity = angle > 0.5 ? '1' : '0';
    left.style.transform = `rotate(${(-180 + Math.max(angle - 180, 0)).toFixed(2)}deg)`;
    left.style.opacity = angle > 180 ? '1' : '0';
    lead.style.transform = `rotate(${angle.toFixed(2)}deg)`;
    start.style.opacity = end.style.opacity = angle > 0.5 ? '1' : '0';
  }

  // home: the ring, closed in the accent, is the hero's own again
  private rest() {
    const { dom } = this;
    if (!dom) return;
    dom.pill.style.visibility = 'hidden';
    const done = () => {
      if (this.active || this.raf) return;
      this.ring.set(0);
      this.paintRing(0);
      delete dom.bar.dataset.pulling;
      delete dom.bar.dataset.pulled;
    };
    if (!this.taken) {
      done();
      return;
    }
    dom.bar.dataset.pulled = '';
    coolRing(dom.bar, done);
  }
}
