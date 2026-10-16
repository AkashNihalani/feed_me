/* The probe's run (components/PerfProbe loads this only when the URL carries `?probe`): a scripted smoothness pass,
   the same in every browser, so Chrome, Safari on the Mac, the iOS Simulator and a real iPhone can be compared on one
   page.

   It waits for the page to settle, measures the idle frame rate, then plays: a scroll down the page and back (twice:
   cold, while bands render for the first time, and warm), the same warm scroll with the header blur off and with the
   chrome/panel shadows off (the ?noblur / ?noshadow switches in globals.css), three time-range switches and a rail
   fan-out + fold. Every frame's gap is recorded by requestAnimationFrame. A frame counts as dropped when its gap
   holds more than one of the browser's own idle frames, so a 60 Hz Safari and a 120 Hz Chrome are judged on their
   own clocks.

   `?probe=deep` adds experiments that each switch one cost off for a pass and then restore the page exactly: the tape
   paused, pictures hidden, the chrome flat (no blur, no shadows); switches without the number rolls, without the
   :has() rules, without any motion (CSS and WAAPI at 0ms), without WebKit's holds. A switch also counts the DOM
   changes it caused, and splits its main-thread time into script and frame work. */

type Split = { script: number; frame: number; longestScript: number; longestFrame: number };
export type Pass = { name: string; ms: number; frames: number; fps: number; dropped: number; smooth: number; hitches: number; worst: number; p95: number; changes?: number; split?: Split };
type Costs = { restyle: number; relayout: number; nodes: number; rootVars: number; inlineVars: number };
export type Report = { at: string; url: string; ua: string; dpr: number; size: string; hz: number; nominal: number; costs: Costs; passes: Pass[]; note: string[]; css?: Record<string, number>; same?: unknown };

const SCROLL_PX_S = 1600;
const SCROLL_CAP = 7000;
const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/* record frame gaps while `run` plays (it gets each frame's time and returns false when done) or for `ms` */
function record(run: ((now: number) => boolean) | null, ms = 0): Promise<number[]> {
  return new Promise((resolve) => {
    const gaps: number[] = [];
    let last = 0;
    let start = 0;
    const tick = (now: number) => {
      if (!start) start = now;
      if (last) gaps.push(now - last);
      last = now;
      const going = run ? run(now) : now - start < ms;
      if (going) requestAnimationFrame(tick);
      else resolve(gaps);
    };
    requestAnimationFrame(tick);
  });
}

function summarize(name: string, gaps: number[], nominal: number): Pass {
  const ms = gaps.reduce((sum, gap) => sum + gap, 0);
  const sorted = [...gaps].sort((a, b) => a - b);
  // frames the display had room for but the page did not draw
  const dropped = gaps.reduce((sum, gap) => sum + Math.max(0, Math.round(gap / nominal) - 1), 0);
  const expected = Math.max(1, Math.round(ms / nominal));
  return {
    name,
    ms: Math.round(ms),
    frames: gaps.length,
    fps: Math.round((gaps.length * 1000) / Math.max(1, ms)),
    dropped,
    smooth: Math.round(((expected - dropped) / expected) * 100),
    // a hitch: a gap of three or more frames, the kind an eye catches as a stutter
    hitches: gaps.filter((gap) => gap >= nominal * 2.5).length,
    worst: Math.round(sorted[sorted.length - 1] || 0),
    p95: Math.round(sorted[Math.floor(sorted.length * 0.95)] || 0),
  };
}

/* DOM changes while `ms` passes: nodes added or removed, attributes and text changed */
function countChanges(ms: number): Promise<number> {
  return new Promise((resolve) => {
    let changes = 0;
    const observer = new MutationObserver((records) => {
      for (const record of records) changes += record.type === 'childList' ? record.addedNodes.length + record.removedNodes.length : 1;
    });
    observer.observe(document.body, { subtree: true, childList: true, attributes: true, characterData: true });
    setTimeout(() => { observer.disconnect(); resolve(changes); }, ms);
  });
}

/* Where a switch's main-thread time goes, from a stream of zero-delay tasks (MessageChannel pings) and a frame clock.
   A stretch with no ping that holds no frame start is script in a task (React rendering and committing, effects,
   timers). A stretch that does hold one is the frame's work, from our frame callback (registered first, so it runs
   first) to the next ping: the other frame callbacks (the Ledger's play, the rolls) and the browser's style, layout,
   paint and layer commit. Only stretches over 4ms count. */
function busySplit(ms: number): Promise<Split> {
  return new Promise((resolve) => {
    const channel = new MessageChannel();
    const pings: number[] = [];
    const frames: number[] = [];
    let live = true;
    channel.port1.onmessage = () => {
      pings.push(performance.now());
      if (live) channel.port2.postMessage(0);
    };
    channel.port2.postMessage(0);
    const tick = () => {
      frames.push(performance.now());
      if (live) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
    setTimeout(() => {
      live = false;
      channel.port1.close();
      const split: Split = { script: 0, frame: 0, longestScript: 0, longestFrame: 0 };
      let next = 0;
      for (let index = 1; index < pings.length; index += 1) {
        const from = pings[index - 1];
        const to = pings[index];
        while (next < frames.length && frames[next] <= from) next += 1;
        const frame = next < frames.length && frames[next] < to ? frames[next] : null;
        const script = (frame ?? to) - from;
        const work = frame == null ? 0 : to - frame;
        if (script > 4) { split.script += script; split.longestScript = Math.max(split.longestScript, script); }
        if (work > 4) { split.frame += work; split.longestFrame = Math.max(split.longestFrame, work); }
      }
      resolve({ script: Math.round(split.script), frame: Math.round(split.frame), longestScript: Math.round(split.longestScript), longestFrame: Math.round(split.longestFrame) });
    }, ms);
  });
}

/* What it costs this browser to restyle every element (a custom property set on the root reaches them all) and to lay
   the whole page out again (main 1px narrower), each forced and timed five times (the median). Also the page's size
   in elements, the custom properties the root defines and the elements that set their own inline. */
function measureCosts(): Costs {
  const root = document.documentElement;
  const main = document.querySelector('main') as HTMLElement | null;
  const median = (values: number[]) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];
  const restyle: number[] = [];
  const relayout: number[] = [];
  void document.body.offsetHeight;
  for (let round = 0; round < 5; round += 1) {
    let start = performance.now();
    root.style.setProperty('--probe-tick', String(round));
    void document.body.offsetHeight;
    restyle.push(performance.now() - start);
    if (main) {
      start = performance.now();
      main.style.width = round % 2 ? '' : 'calc(100% - 1px)';
      void document.body.offsetHeight;
      relayout.push(performance.now() - start);
    }
  }
  root.style.removeProperty('--probe-tick');
  if (main) main.style.width = '';
  void document.body.offsetHeight;
  const rootStyle = getComputedStyle(root);
  let rootVars = 0;
  for (let index = 0; index < rootStyle.length; index += 1) if (rootStyle[index].startsWith('--')) rootVars += 1;
  const inlineVars = [...document.querySelectorAll<HTMLElement>('[style*="--"]')].length;
  return {
    restyle: Math.round(median(restyle) * 10) / 10,
    relayout: relayout.length ? Math.round(median(relayout) * 10) / 10 : 0,
    nodes: document.getElementsByTagName('*').length,
    rootVars,
    inlineVars,
  };
}

/* the median time to restyle every element once (a custom property set on the root reaches them all) */
function restyleAll(rounds = 7) {
  const root = document.documentElement;
  const times: number[] = [];
  void document.body.offsetHeight;
  for (let round = 0; round < rounds; round += 1) {
    const start = performance.now();
    root.style.setProperty('--probe-tick', String(round));
    void document.body.offsetHeight;
    times.push(performance.now() - start);
  }
  root.style.removeProperty('--probe-tick');
  void document.body.offsetHeight;
  return Math.round([...times].sort((a, b) => a - b)[Math.floor(times.length / 2)] * 10) / 10;
}

/* style rules matching `test` taken out of every sheet; returns the put-back (lowest index first, so each lands where it was) */
function takeRules(test: (rule: CSSRule) => boolean) {
  type Holder = { cssRules: CSSRuleList; deleteRule: (index: number) => void; insertRule: (rule: string, index?: number) => number };
  const taken: Array<{ holder: Holder; index: number; text: string }> = [];
  const walk = (holder: Holder) => {
    for (let index = holder.cssRules.length - 1; index >= 0; index -= 1) {
      const rule = holder.cssRules[index];
      if (test(rule)) {
        taken.push({ holder, index, text: rule.cssText });
        holder.deleteRule(index);
      } else if ('cssRules' in rule && 'deleteRule' in rule) {
        walk(rule as unknown as Holder);
      }
    }
  };
  for (const sheet of [...document.styleSheets]) {
    try { walk(sheet as unknown as Holder); } catch { /* another origin's sheet */ }
  }
  return () => { for (const { holder, index, text } of [...taken].reverse()) holder.insertRule(text, index); };
}

/* What makes a restyle cost what it does here: the same full restyle timed with one kind of CSS taken out at a time
   (and put back): the color-mix() rules (Tailwind's opacity colours), every rule that reads a var(), the :root custom
   properties, the :where() variants (light:/dark:), the :has() rules, the elements' own inline custom properties, and
   with no style sheets at all (the floor). */
function cssCosts(): Record<string, number> {
  const out: Record<string, number> = { baseline: restyleAll() };
  const style = (rule: CSSRule): rule is CSSStyleRule => rule instanceof CSSStyleRule;
  const trials: Array<[string, (rule: CSSRule) => boolean]> = [
    ['no color-mix', (rule) => style(rule) && rule.cssText.includes('color-mix(')],
    ['no var()', (rule) => style(rule) && rule.style.cssText.includes('var(')],
    ['no root vars', (rule) => style(rule) && /(^|,)\s*(:root|html)\b/.test(rule.selectorText) && rule.style.cssText.includes('--')],
    ['no :where', (rule) => style(rule) && rule.selectorText.includes(':where(')],
    ['no :has', (rule) => style(rule) && rule.selectorText.includes(':has(')],
  ];
  for (const [name, test] of trials) {
    const putBack = takeRules(test);
    out[name] = restyleAll();
    putBack();
  }
  const inline = [...document.querySelectorAll<HTMLElement>('[style*="--"]')].map((el) => [el, el.getAttribute('style') || ''] as const);
  for (const [el] of inline) for (const name of [...el.style].filter((property) => property.startsWith('--'))) el.style.removeProperty(name);
  out['no inline vars'] = restyleAll();
  for (const [el, saved] of inline) el.setAttribute('style', saved);
  const sheets = [...document.styleSheets].filter((sheet) => !sheet.disabled);
  sheets.forEach((sheet) => { sheet.disabled = true; });
  out['no css'] = restyleAll();
  sheets.forEach((sheet) => { sheet.disabled = false; });
  void document.body.offsetHeight;
  return out;
}

/* A/B against an earlier stylesheet: the page's main sheet (the one carrying the app's tokens) swapped for the CSS at
   `url` and back, in place (same spot in the cascade), so two versions of the CSS are timed in the same page under
   the same conditions. */
async function cssSwap(url: string) {
  // its url()s were written relative to /_next/static/chunks/ (the fonts live in ../media)
  const text = (await fetch(url).then((response) => response.text())).replace(/url\((['"]?)\.\.\//g, 'url($1/_next/static/');
  const main = [...document.styleSheets].find((sheet) => {
    try { return [...sheet.cssRules].some((rule) => rule.cssText.includes('--fm-accent:')); } catch { return false; }
  });
  let swapped: HTMLStyleElement | null = null;
  return {
    found: Boolean(main && text.length > 1000),
    old: () => {
      if (swapped || !main?.ownerNode) return;
      swapped = document.createElement('style');
      swapped.textContent = text;
      (main.ownerNode as Element).after(swapped);
      main.disabled = true;
      void document.body.offsetHeight;
    },
    now: () => {
      if (!swapped || !main) return;
      main.disabled = false;
      swapped.remove();
      swapped = null;
      void document.body.offsetHeight;
    },
  };
}

/* Does the earlier stylesheet draw the page any differently? Every element's (and its ::before/::after's) computed
   style under the current CSS against the one at `url`, swapped in place, and every element's box once the fonts are
   in. Values that only follow from the boxes are compared as boxes; one colour written two ways (oklab vs rgba) is
   compared as a pixel. Run in the current theme, then in the other one (the html class flipped for the check only). */
async function sameDrawing(url: string) {
  const swap = await cssSwap(url);
  const layout = new Set(['width', 'height', 'inline-size', 'block-size', 'perspective-origin', 'transform-origin', 'top', 'right', 'bottom', 'left', 'inset-block-start', 'inset-block-end', 'inset-inline-start', 'inset-inline-end', 'margin-top', 'margin-right', 'margin-bottom', 'margin-left', 'margin-block-start', 'margin-block-end', 'margin-inline-start', 'margin-inline-end', 'grid-template-columns', 'grid-template-rows', 'cx', 'cy', 'r', 'rx', 'ry', 'x', 'y']);
  const props = [...getComputedStyle(document.documentElement)].filter((property) => !property.startsWith('--') && !layout.has(property));
  const SEP = '\u0001';
  const canvas = document.createElement('canvas');
  canvas.width = 1;
  canvas.height = 1;
  const context = canvas.getContext('2d', { willReadFrequently: true });
  const pixel = (colour: string) => {
    if (!context) return colour;
    context.clearRect(0, 0, 1, 1);
    context.fillStyle = '#000';
    context.fillStyle = colour;
    context.fillRect(0, 0, 1, 1);
    return [...context.getImageData(0, 0, 1, 1).data].map((value) => Math.round(value / 2)).join(',');
  };
  const colours = /(oklab|oklch|lab|lch|color|rgba?|hsla?)\([^()]*\)|#[0-9a-f]{3,8}\b/gi;
  const norm = (value: string) => value.replace(colours, (match) => `px(${pixel(match)})`).replace(/\s+/g, ' ');
  const fontsIn = async () => {
    await Promise.all([400, 500, 600, 700].map((weight) => document.fonts.load(`${weight} 16px "Space Grotesk"`, 'Aa0').catch(() => [])));
    await document.fonts.ready;
    await wait(300);
  };
  const check = async () => {
    // not the meters drawn over the page, nor what is animating right now (its values move between the two reads)
    const els = [...document.querySelectorAll<Element>('html, html *')].filter((el) => !el.closest('[style*="2147483647"]') && !el.getAnimations().length);
    const one = (el: Element, pseudo: string | null) => {
      const style = getComputedStyle(el, pseudo);
      if (pseudo && (style.content === 'none' || style.content === 'normal')) return '';
      let out = '';
      for (const property of props) out += style.getPropertyValue(property) + SEP;
      return out;
    };
    const styles = () => els.map((el) => [one(el, null), one(el, '::before'), one(el, '::after')]);
    const boxes = () => els.map((el) => { const box = el.getBoundingClientRect(); return [box.x, box.y, box.width, box.height].map((value) => Math.round(value * 2) / 2).join(','); });
    const nowStyles = styles();
    const nowBoxes = boxes();
    swap.old();
    await fontsIn();
    const oldStyles = styles();
    const oldBoxes = boxes();
    swap.now();
    await fontsIn();
    const nowBoxes2 = boxes();
    const kinds: Record<string, number> = {};
    let differ = 0;
    let notation = 0;
    for (let index = 0; index < els.length; index += 1) for (let part = 0; part < 3; part += 1) {
      const a = nowStyles[index][part];
      const b = oldStyles[index][part];
      if (a === b) continue;
      const after = a.split(SEP);
      const before = b.split(SEP);
      props.forEach((property, at) => {
        if (after[at] === before[at]) return;
        if (norm(after[at]) === norm(before[at])) { notation += 1; return; }
        differ += 1;
        const key = `${property}: ${before[at].slice(0, 60)} → ${after[at].slice(0, 60)} (${els[index].tagName.toLowerCase()}.${String(els[index].getAttribute('class') || '').slice(0, 40)})`;
        kinds[key] = (kinds[key] || 0) + 1;
      });
    }
    let moved = 0;
    let unstable = 0;
    const movedExamples: string[] = [];
    for (let index = 0; index < els.length; index += 1) {
      if (nowBoxes[index] !== nowBoxes2[index]) { unstable += 1; continue; }
      if (nowBoxes2[index] !== oldBoxes[index]) {
        moved += 1;
        if (movedExamples.length < 6) movedExamples.push(`${els[index].tagName.toLowerCase()}.${String(els[index].getAttribute('class') || '').slice(0, 40)} ${oldBoxes[index]} → ${nowBoxes2[index]}`);
      }
    }
    return { elements: els.length, differ, notation, moved, unstable, kinds: Object.entries(kinds).sort((a, b) => b[1] - a[1]).slice(0, 12), movedExamples };
  };
  // no transitions while comparing: a theme flip or the swap itself would start colour fades read halfway
  const still = document.createElement('style');
  still.textContent = '*,*::before,*::after{transition:none!important}';
  document.head.appendChild(still);
  const root = document.documentElement;
  const theme = root.classList.contains('dark') ? 'dark' : 'light';
  const other = theme === 'dark' ? 'light' : 'dark';
  const first = await check();
  root.classList.replace(theme, other);
  await wait(400);
  const second = await check();
  root.classList.replace(other, theme);
  still.remove();
  return { found: swap.found, [theme]: first, [other]: second };
}

/* Which custom properties make Safari's restyle dear: the full restyle timed with (1) the root's custom properties that
   nothing reads taken off, (2) the @property registrations taken out (Tailwind registers its --tw-* ones), (3) both,
   (4) every --tw-* declaration taken off the utility rules; each put back after. */
function varCosts(): Record<string, number> {
  const out: Record<string, number> = { baseline: restyleAll() };
  type Taken = Array<[CSSStyleDeclaration, string, string, string]>;
  const styleRules: CSSStyleRule[] = [];
  const holders: Array<{ holder: CSSStyleSheet | CSSGroupingRule; rule: CSSRule; index: number }> = [];
  const walk = (holder: CSSStyleSheet | CSSGroupingRule) => {
    [...holder.cssRules].forEach((rule, index) => {
      if (rule instanceof CSSStyleRule) styleRules.push(rule);
      if (typeof CSSPropertyRule !== 'undefined' && rule instanceof CSSPropertyRule) holders.push({ holder, rule, index });
      if ('cssRules' in rule && rule instanceof CSSGroupingRule) walk(rule);
    });
  };
  for (const sheet of [...document.styleSheets]) {
    try { walk(sheet); } catch { /* another origin's sheet */ }
  }
  let text = styleRules.map((rule) => rule.cssText).join('\n');
  for (const el of document.querySelectorAll('[style]')) text += el.getAttribute('style');
  const read = new Set((text.match(/var\(\s*--[\w-]+/g) || []).map((match) => match.replace(/var\(\s*/, '')));
  const takeDeclarations = (rules: CSSStyleRule[], test: (name: string) => boolean): Taken => {
    const taken: Taken = [];
    for (const rule of rules) {
      for (const name of [...rule.style].filter((property) => property.startsWith('--') && test(property))) {
        taken.push([rule.style, name, rule.style.getPropertyValue(name), rule.style.getPropertyPriority(name)]);
        rule.style.removeProperty(name);
      }
    }
    return taken;
  };
  const putBack = (taken: Taken) => { for (const [style, name, value, priority] of taken) style.setProperty(name, value, priority); };
  const takeRegistrations = () => {
    const sorted = [...holders].sort((a, b) => b.index - a.index);
    for (const { holder, index } of sorted) holder.deleteRule(index);
    return () => { for (const { holder, rule, index } of [...sorted].reverse()) holder.insertRule(rule.cssText, index); };
  };
  const rootRules = styleRules.filter((rule) => /(^|,)\s*(:root|html)\b/.test(rule.selectorText));
  const unused = (name: string) => !read.has(name);
  let taken = takeDeclarations(rootRules, unused);
  out[`no unused root vars (${taken.length})`] = restyleAll();
  putBack(taken);
  let restore = takeRegistrations();
  out[`no @property (${holders.length})`] = restyleAll();
  taken = takeDeclarations(rootRules, unused);
  out['no unused root vars, no @property'] = restyleAll();
  putBack(taken);
  restore();
  taken = takeDeclarations(styleRules, (name) => name.startsWith('--tw-'));
  out[`no --tw-* declarations (${taken.length})`] = restyleAll();
  putBack(taken);
  restore = () => {};
  restore();
  void document.body.offsetHeight;
  return out;
}

/* an experiment switches one cost off for a few passes, then puts the page back exactly as it was */
type Experiment = { name: string; on: () => void; off: () => void };

function styleExperiment(name: string, css: string): Experiment {
  let sheet: HTMLStyleElement | null = null;
  return {
    name,
    on: () => { sheet = document.createElement('style'); sheet.textContent = css; document.head.appendChild(sheet); },
    off: () => { sheet?.remove(); sheet = null; },
  };
}

function rootExperiment(name: string, attributes: string[]): Experiment {
  const root = document.documentElement;
  return { name, on: () => attributes.forEach((attribute) => root.setAttribute(attribute, '')), off: () => attributes.forEach((attribute) => root.removeAttribute(attribute)) };
}

const NO_ROLLS = () => styleExperiment('no rolls', '.fm-slot-roll__face{animation:none!important;will-change:auto!important}.fm-slot-roll__face--old{display:none!important}');
const TAPE_PAUSED = () => styleExperiment('tape paused', '[data-testid="lead-v3-tape"] *{animation-play-state:paused!important}');
const NO_IMAGES = () => styleExperiment('no images', 'main img{visibility:hidden!important}');
const FLAT = () => rootExperiment('flat chrome', ['data-perf-noblur', 'data-perf-noshadow']);

/* every motion at 0ms: CSS animations and transitions, and WAAPI (the Ledger, the rings, the rail) */
function noMotion(): Experiment {
  const sheet = styleExperiment('', '*,*::before,*::after{animation-duration:0s!important;animation-delay:0s!important;transition-duration:0s!important;transition-delay:0s!important}');
  let animate: typeof Element.prototype.animate | null = null;
  return {
    name: 'no motion',
    on: () => {
      sheet.on();
      animate = Element.prototype.animate;
      const original = animate;
      Element.prototype.animate = function (this: Element, keyframes: Keyframe[] | PropertyIndexedKeyframes | null, options?: number | KeyframeAnimationOptions) {
        const timing = typeof options === 'number' || options == null ? { duration: 0 } : { ...options, duration: 0, delay: 0, endDelay: 0 };
        return original.call(this, keyframes, timing);
      };
    },
    off: () => { sheet.off(); if (animate) Element.prototype.animate = animate; },
  };
}

/* WebKit's holds off (lib/webkitMotion): moves start in the frame that draws them, as in Chrome */
function noHolds(): Experiment {
  const win = window as unknown as { __fmNoHold?: boolean };
  return { name: 'no holds', on: () => { win.__fmNoHold = true; }, off: () => { delete win.__fmNoHold; } };
}

/* the :has() rules taken out of the style sheets (WebKit re-checks them on DOM changes), put back where they were */
function noHas(): Experiment {
  type Holder = { cssRules: CSSRuleList; deleteRule: (index: number) => void; insertRule: (rule: string, index?: number) => number };
  let taken: Array<{ holder: Holder; index: number; text: string }> = [];
  const walk = (holder: Holder) => {
    for (let index = holder.cssRules.length - 1; index >= 0; index -= 1) {
      const rule = holder.cssRules[index];
      if (rule instanceof CSSStyleRule && rule.selectorText.includes(':has(')) {
        taken.push({ holder, index, text: rule.cssText });
        holder.deleteRule(index);
      } else if ('cssRules' in rule && 'deleteRule' in rule) {
        walk(rule as unknown as Holder);
      }
    }
  };
  return {
    name: 'no :has',
    on: () => {
      taken = [];
      for (const sheet of [...document.styleSheets]) {
        try { walk(sheet as unknown as Holder); } catch { /* another origin's sheet */ }
      }
    },
    // put back lowest index first, so each lands where it was
    off: () => { for (const { holder, index, text } of [...taken].reverse()) holder.insertRule(text, index); taken = []; },
  };
}

/* the page's scroller: the window on a phone, the tab's own panel from desktop up (Lead scrolls inside .fm-lead) */
function scroller(): { max: number; to: (y: number) => void } {
  const page = document.documentElement.scrollHeight - window.innerHeight;
  if (page > 200) return { max: page, to: (y) => window.scrollTo(0, y) };
  const own = [...document.querySelectorAll<HTMLElement>('main *')]
    .filter((el) => /(auto|scroll)/.test(getComputedStyle(el).overflowY) && el.scrollHeight > el.clientHeight + 200)
    .sort((a, b) => b.clientHeight - a.clientHeight)[0];
  if (!own) return { max: 0, to: () => {} };
  return { max: own.scrollHeight - own.clientHeight, to: (y) => { own.scrollTop = y; } };
}

async function scrollPass(): Promise<number[]> {
  const target = scroller();
  const max = Math.min(SCROLL_CAP, target.max);
  target.to(0);
  await wait(300);
  const down = (max / SCROLL_PX_S) * 1000;
  let t0 = 0;
  const gaps = await record((now) => {
    if (!t0) t0 = now;
    const t = now - t0;
    // down, then straight back up
    const y = t <= down ? (t / down) * max : Math.max(0, max - ((t - down) / down) * max);
    target.to(y);
    return t < down * 2;
  });
  target.to(0);
  return gaps;
}

const visible = (el: Element) => {
  const rect = el.getBoundingClientRect();
  return rect.width > 0 && rect.height > 0 && getComputedStyle(el).visibility !== 'hidden';
};

/* pick a time range the way a person would: the track's button from sm up, the menu on a phone */
async function pickRange(label: string) {
  const buttons = [...document.querySelectorAll('button')].filter((button) => button.textContent?.trim() === label && visible(button));
  const track = buttons.find((button) => button.hasAttribute('aria-pressed'));
  if (track) { track.click(); return true; }
  // the header's menu chip: Lead's time range, or Feed's zoom
  const trigger = [...document.querySelectorAll('button[aria-haspopup="menu"]')].find((button) => /^(Time range|Zoom)\b|D\b/.test(button.getAttribute('aria-label') || '') && visible(button)) as HTMLButtonElement | undefined;
  if (!trigger) return false;
  trigger.click();
  await wait(450);
  const item = [...document.querySelectorAll('[role="menuitem"]')].find((el) => el.textContent?.trim() === label) as HTMLElement | undefined;
  item?.click();
  return !!item;
}

/* Feed's zoom switches (DAY · WEEK · MONTH), each held 2s so the move, its arrivals and the posters landing after it
   are all inside the record; the page is left where the previous switch put it (no scroll reset: where a switch
   lands is part of what is measured). */
async function zoomRound(passes: Pass[], nominal: number, note: string[], suffix = '') {
  for (const label of ['WEEK', 'MONTH', 'DAY', 'MONTH', 'WEEK', 'DAY']) {
    const started = record(null, 2000);
    const changes = countChanges(2000);
    const split = busySplit(2000);
    const ok = await pickRange(label);
    if (!ok) note.push(`zoom ${label}: control not found`);
    passes.push({ ...summarize(`zoom → ${label}${suffix}`, await started, nominal), changes: await changes, split: await split });
    await wait(800);
  }
}

/* the first load as a person sees it: frames from the probe's start (the page has just mounted) until the first real
   post on screen has its picture, and when that was (ms since navigation) */
async function loadPass(passes: Pass[], nominal: number, note: string[]) {
  let firstPost = 0;
  const ready = () => [...document.querySelectorAll<HTMLElement>('[data-post-key]')].some((cell) => {
    const box = cell.getBoundingClientRect();
    if (box.bottom < 0 || box.top > window.innerHeight || !box.width) return false;
    const img = cell.querySelector('img');
    return Boolean(img && img.complete && img.naturalWidth > 0);
  });
  const gaps = await record(() => {
    if (!firstPost && ready()) firstPost = Math.round(performance.now());
    return (!firstPost && performance.now() < 8000) || (firstPost > 0 && performance.now() < firstPost + 1200);
  });
  passes.push(summarize('load', gaps, nominal));
  note.push(firstPost ? `first post with its picture at ${firstPost}ms` : 'no post with a picture within 8s');
}

async function rangeRound(passes: Pass[], nominal: number, note: string[], suffix = '') {
  scroller().to(0);
  for (const label of ['7D', '90D', '30D']) {
    const started = record(null, 1600);
    const changes = countChanges(1600);
    const split = busySplit(1600);
    const ok = await pickRange(label);
    if (!ok) note.push(`range ${label}: control not found`);
    passes.push({ ...summarize(`range → ${label}${suffix}`, await started, nominal), changes: await changes, split: await split });
    await wait(700);
  }
}

async function railRound(passes: Pass[], nominal: number, note: string[], suffix = '') {
  scroller().to(0);
  await wait(600);
  const feeds = [...document.querySelectorAll('[data-ledger^="rail:feed:"]')].filter(visible) as HTMLElement[];
  if (feeds.length < 2) { note.push('rail: fewer than two feed circles'); return; }
  const fan = record(null, 1600);
  const fanChanges = countChanges(1600);
  const fanSplit = busySplit(1600);
  feeds[1].click();
  passes.push({ ...summarize(`rail fan${suffix}`, await fan, nominal), changes: await fanChanges, split: await fanSplit });
  await wait(700);
  const back = [...document.querySelectorAll('[data-ledger^="rail:feed:"]')].filter(visible)[0] as HTMLElement | undefined;
  const fold = record(null, 1600);
  const foldChanges = countChanges(1600);
  const foldSplit = busySplit(1600);
  back?.click();
  passes.push({ ...summarize(`rail fold${suffix}`, await fold, nominal), changes: await foldChanges, split: await foldSplit });
  await wait(700);
}

/* deep runs every experiment, or the ones `&x=` names: tape, images, flat (scroll); rolls, has, motion, holds (switches) */
/* old CSS against new, alternating in one page: restyle, scrolling and switches */
async function runAb(url: string, note: string[]): Promise<Report> {
  await wait(4000);
  for (let tries = 0; tries < 16 && scroller().max <= 0; tries += 1) await wait(500);
  const swap = await cssSwap(url);
  if (!swap.found) note.push('ab: old CSS or main sheet not found');
  const idle = await record(null, 1500);
  const nominal = [...idle].sort((a, b) => a - b)[Math.floor(idle.length / 2)] || 16.7;
  const passes: Pass[] = [summarize('idle', idle, nominal)];
  // every band drawn once, so both versions restyle the same page
  const target = scroller();
  for (let y = 0; y <= target.max; y += 600) { target.to(y); await wait(120); }
  target.to(0);
  await wait(1500);
  const css: Record<string, number> = {};
  const oldTimes: number[] = [];
  const newTimes: number[] = [];
  for (let round = 0; round < 5; round += 1) {
    swap.old(); oldTimes.push(restyleAll(5));
    swap.now(); newTimes.push(restyleAll(5));
  }
  const median = (values: number[]) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];
  css['restyle old'] = median(oldTimes);
  css['restyle new'] = median(newTimes);
  // `&only=tabs` skips straight to the tab switches
  const tabsOnly = new URLSearchParams(window.location.search).get('only') === 'tabs';
  for (const [name, apply] of tabsOnly ? [] : [['old', swap.old], ['new', swap.now], ['old', swap.old], ['new', swap.now]] as const) {
    apply();
    await wait(500);
    passes.push(summarize(`scroll · ${name}`, await scrollPass(), nominal));
    await wait(500);
  }
  for (const [name, apply] of tabsOnly ? [] : [['old', swap.old], ['new', swap.now], ['old', swap.old], ['new', swap.now]] as const) {
    apply();
    await wait(500);
    await rangeRound(passes, nominal, note, ` · ${name}`);
    await railRound(passes, nominal, note, ` · ${name}`);
  }
  // tab switches: a whole tab shown and another hidden at once (the bottom nav's links)
  const tab = (label: string) => document.querySelector<HTMLElement>(`a[aria-label="${label}"]`);
  for (const [name, apply] of [['old', swap.old], ['new', swap.now], ['old', swap.old], ['new', swap.now]] as const) {
    apply();
    await wait(500);
    for (const label of ['Feed', 'Lead', 'Read', 'Lead']) {
      const link = tab(label);
      if (!link) { note.push(`tab ${label}: link not found`); continue; }
      const started = record(null, 1400);
      const split = busySplit(1400);
      link.click();
      passes.push({ ...summarize(`tab → ${label} · ${name}`, await started, nominal), split: await split });
      await wait(900);
    }
  }
  swap.now();
  return {
    at: new Date().toISOString(), url: location.pathname, ua: navigator.userAgent, dpr: window.devicePixelRatio,
    size: `${window.innerWidth}×${window.innerHeight}`, hz: Math.round(1000 / nominal), nominal: Math.round(nominal * 10) / 10,
    costs: measureCosts(), passes, note, css,
  };
}

/* Feed, frame by frame: where the reader's post is on screen through the first load and through zoom switches (the
   real cell and, while WebKit's still copy covers the page, its copy, with the copy's opacity), with the scroll, the
   page root's offset (a held jump) and how many cells are drawn. A jump shows as a step in the visible position. */
type TraceRow = [number, number, number | null, number | null, number | null, number, string, number];
function traceRows(key: () => string | null, ms: number): Promise<TraceRow[]> {
  return new Promise((resolve) => {
    const rows: TraceRow[] = [];
    const start = performance.now();
    const root = document.querySelector<HTMLElement>('.fm-stream[data-tab-scroll="own"]');
    const tick = () => {
      const now = performance.now() - start;
      const k = key();
      const still = document.querySelector<HTMLElement>('[data-still-frame]');
      const stillOpacity = still ? Number(getComputedStyle(still).opacity) : -1;
      const all = k ? Array.from(document.querySelectorAll<HTMLElement>(`[data-post-key="${CSS.escape(k)}"]`)) : [];
      const real = all.find((el) => !el.closest('[data-still-frame]'));
      const copy = all.find((el) => Boolean(el.closest('[data-still-frame]')));
      const box = real?.getBoundingClientRect();
      const copyBox = copy?.getBoundingClientRect();
      rows.push([
        Math.round(now),
        Math.round(window.scrollY),
        box ? Math.round(box.top) : null,
        box ? Math.round(box.left) : null,
        copyBox ? Math.round(copyBox.top) : null,
        Math.round(stillOpacity * 100) / 100,
        root?.style.top || '',
        document.querySelectorAll('[data-post-key]').length,
      ]);
      if (now < ms) requestAnimationFrame(tick);
      else resolve(rows);
    };
    requestAnimationFrame(tick);
  });
}

function middleKey() {
  const cells = Array.from(document.querySelectorAll<HTMLElement>('[data-post-key]'))
    .filter((el) => !el.closest('[data-still-frame]'))
    .map((el) => ({ el, box: el.getBoundingClientRect() }))
    .filter(({ box }) => box.width > 0 && box.bottom > 0 && box.top < window.innerHeight);
  const cx = window.innerWidth / 2;
  const cy = window.innerHeight / 2;
  cells.sort((a, b) => Math.hypot(a.box.left + a.box.width / 2 - cx, a.box.top + a.box.height / 2 - cy) - Math.hypot(b.box.left + b.box.width / 2 - cx, b.box.top + b.box.height / 2 - cy));
  return cells[0]?.el.getAttribute('data-post-key') ?? null;
}

async function runTrace(note: string[]): Promise<Report> {
  const trace: Record<string, TraceRow[]> = {};
  // the first load: whichever post is nearest the middle once there is one
  let loadKey: string | null = null;
  trace.load = await traceRows(() => (loadKey ??= middleKey()), 4500);
  await wait(1500);
  window.scrollTo(0, 2400);
  await wait(1500);
  const current = document.querySelector('button[aria-haspopup="menu"]')?.getAttribute('aria-label')?.split(': ')[1] ?? 'DAY';
  const order = current === 'DAY' ? ['WEEK', 'MONTH', 'DAY'] : current === 'WEEK' ? ['DAY', 'MONTH', 'WEEK'] : ['WEEK', 'DAY', 'MONTH'];
  let from = current;
  for (const label of order) {
    const key = middleKey();
    const rows = traceRows(() => key, 2800);
    const ok = await pickRange(label);
    if (!ok) note.push(`zoom ${label}: control not found`);
    trace[`${from}→${label}`] = await rows;
    from = label;
    await wait(1200);
  }
  return {
    at: new Date().toISOString(), url: location.pathname, ua: navigator.userAgent, dpr: window.devicePixelRatio,
    size: `${window.innerWidth}×${window.innerHeight}`, hz: 0, nominal: 0, costs: measureCosts(), passes: [], note, same: { columns: 't, scrollY, top, left, copyTop, stillOpacity, rootTop, cells', trace },
  };
}

export async function runProbe(note: string[], deep: boolean, only: string[], cssOnly: boolean): Promise<Report> {
  const pick = (key: string) => deep && (!only.length || only.includes(key));
  const params = new URLSearchParams(window.location.search);
  if (params.get('probe') === 'trace') return runTrace(note);
  // an old stylesheet only from this app's own origin (never another site's, a data: or a blob: URL)
  const asked = params.get('css');
  const ab = asked && (() => {
    try {
      return new URL(asked, location.href).origin === location.origin;
    } catch {
      return false;
    }
  })() ? asked : null;
  if (asked && !ab) note.push('css: only a stylesheet from this origin');
  if (ab && params.get('probe') === 'same') {
    // the page settled: its element count unchanged for a couple of seconds (data in, pictures placed)
    await wait(4000);
    let last = -1;
    for (let still = 0, tries = 0; still < 4 && tries < 40; tries += 1) {
      await wait(500);
      const count = document.getElementsByTagName('*').length;
      still = count === last ? still + 1 : 0;
      last = count;
    }
    return {
      at: new Date().toISOString(), url: location.pathname, ua: navigator.userAgent, dpr: window.devicePixelRatio,
      size: `${window.innerWidth}×${window.innerHeight}`, hz: 0, nominal: 0, costs: measureCosts(), passes: [], note, same: await sameDrawing(ab),
    };
  }
  if (ab) return runAb(ab, note);
  if (params.get('probe') === 'vars') {
    await wait(4000);
    for (let tries = 0; tries < 16 && scroller().max <= 0; tries += 1) await wait(500);
    const target = scroller();
    for (let y = 0; y <= target.max; y += 600) { target.to(y); await wait(120); }
    target.to(0);
    await wait(1500);
    return {
      at: new Date().toISOString(), url: location.pathname, ua: navigator.userAgent, dpr: window.devicePixelRatio,
      size: `${window.innerWidth}×${window.innerHeight}`, hz: 0, nominal: 0, costs: measureCosts(), passes: [], note, css: varCosts(),
    };
  }
  if (cssOnly) {
    await wait(6000);
    const css = cssCosts();
    // and once every band has been drawn (content-visibility skips the ones never scrolled to)
    const target = scroller();
    for (let y = 0; y <= target.max; y += 600) { target.to(y); await wait(120); }
    target.to(0);
    await wait(1500);
    css['after scroll'] = restyleAll();
    return {
      at: new Date().toISOString(), url: location.pathname, ua: navigator.userAgent, dpr: window.devicePixelRatio,
      size: `${window.innerWidth}×${window.innerHeight}`, hz: 0, nominal: 0, costs: measureCosts(), passes: [], note, css,
    };
  }
  // Feed: the first load is part of the run (frames on the default 16.7ms clock until the idle rate is known)
  const feed = location.pathname === '/';
  const loadPasses: Pass[] = [];
  if (feed) await loadPass(loadPasses, 1000 / 60, note);
  // let the page load, decode and warm up (Lead warms its builders at idle), and wait until it can scroll
  await wait(4000);
  for (let tries = 0; tries < 16 && scroller().max <= 0; tries += 1) await wait(500);
  const idle = await record(null, 1500);
  const sorted = [...idle].sort((a, b) => a - b);
  const nominal = sorted[Math.floor(sorted.length / 2)] || 16.7;
  const passes: Pass[] = [...loadPasses, summarize('idle', idle, nominal)];
  const costs = measureCosts();

  passes.push(summarize('scroll · cold', await scrollPass(), nominal));
  await wait(800);
  passes.push(summarize('scroll · warm', await scrollPass(), nominal));
  await wait(800);

  const root = document.documentElement;
  root.setAttribute('data-perf-noblur', '');
  await wait(400);
  passes.push(summarize('scroll · no blur', await scrollPass(), nominal));
  root.removeAttribute('data-perf-noblur');
  root.setAttribute('data-perf-noshadow', '');
  await wait(400);
  passes.push(summarize('scroll · no shadows', await scrollPass(), nominal));
  root.removeAttribute('data-perf-noshadow');
  await wait(800);

  if (feed) await zoomRound(passes, nominal, note);
  else await rangeRound(passes, nominal, note);
  await railRound(passes, nominal, note);

  const scrolls = [pick('tape') && TAPE_PAUSED(), pick('images') && NO_IMAGES(), pick('flat') && FLAT()].filter(Boolean) as Experiment[];
  for (const experiment of scrolls) {
    experiment.on();
    await wait(400);
    passes.push(summarize(`scroll · ${experiment.name}`, await scrollPass(), nominal));
    experiment.off();
    await wait(600);
  }
  const switches = [pick('holds') && noHolds(), pick('rolls') && NO_ROLLS(), pick('has') && noHas(), pick('motion') && noMotion()].filter(Boolean) as Experiment[];
  for (const experiment of switches) {
    experiment.on();
    await rangeRound(passes, nominal, note, ` · ${experiment.name}`);
    await railRound(passes, nominal, note, ` · ${experiment.name}`);
    experiment.off();
  }

  return {
    at: new Date().toISOString(),
    url: location.pathname,
    ua: navigator.userAgent,
    dpr: window.devicePixelRatio,
    size: `${window.innerWidth}×${window.innerHeight}`,
    hz: Math.round(1000 / nominal),
    nominal: Math.round(nominal * 10) / 10,
    costs,
    passes,
    note,
  };
}
