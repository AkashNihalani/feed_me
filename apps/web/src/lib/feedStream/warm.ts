/* ─────────────────────────────────────────────────────────────
   WARM PICTURES — posters fetched and decoded before they are
   needed: a pick about to be tapped (the rail's hover or press),
   a zoom about to be taken. A picture in the document's list of
   available images draws on the frame its <img> mounts (complete
   at once, even a lazy one), so a card moving into view never
   arrives as a cover that turns into a picture on the way.

   The images are held (the most recent few dozen) so the
   browser keeps them; decode() is asked once per picture.
   ───────────────────────────────────────────────────────────── */

const HELD = 64;
const held = new Map<string, { image: HTMLImageElement; ready: Promise<void>; done: boolean }>();

function warmOne(src: string): Promise<void> {
  const known = held.get(src);
  if (known) {
    // most recently wanted last: the oldest go first
    held.delete(src);
    held.set(src, known);
    return known.ready;
  }
  const image = new Image();
  image.decoding = 'async';
  image.src = src;
  const entry = { image, ready: Promise.resolve(), done: false };
  entry.ready = (typeof image.decode === 'function' ? image.decode() : Promise.resolve()).catch(() => undefined).then(() => {
    entry.done = true;
  });
  held.set(src, entry);
  const ready = entry.ready;
  while (held.size > HELD) {
    const oldest = held.keys().next().value as string;
    held.delete(oldest);
  }
  return ready;
}

/* Fetches and decodes the pictures; resolves once they all have (or failed), or after `wait` ms, whichever is first */
export function warmPictures(srcs: readonly string[], wait = 4000): Promise<void> {
  if (typeof window === 'undefined' || !srcs.length) return Promise.resolve();
  const all = Promise.allSettled(srcs.map(warmOne)).then(() => undefined);
  return Promise.race([all, new Promise<void>((resolve) => window.setTimeout(resolve, wait))]);
}

// a picture warmed here and decoded (or given up on): drawing it costs nothing more
export function isWarm(src: string): boolean {
  return held.get(src)?.done === true;
}

/* LOOKAHEAD — the pictures a scroll is heading for, fetched before their posts are put up (FeedStream asks, nearest
   the landing first). A few at a time, so the ones wanted soonest go first and the posts already up keep the network;
   a scroll that moves on drops what it no longer wants (a request already sent finishes). Fetched only: each picture
   is decoded by its own <img>, which goes up a screen before it shows. A request that hangs gives its turn up after a
   while (it may still land). */
const AHEAD_AT_ONCE = 6;
const AHEAD_TURN_MS = 4000;
const AHEAD_KEEP = 160;
const ahead = new Map<string, HTMLImageElement>();
let aheadQueue: string[] = [];
let aheadRunning = 0;

function pumpAhead() {
  while (aheadRunning < AHEAD_AT_ONCE && aheadQueue.length) {
    const src = aheadQueue.shift() as string;
    if (ahead.has(src) || held.has(src)) continue;
    const image = new Image();
    image.decoding = 'async';
    let turn = true;
    const done = () => {
      if (!turn) return;
      turn = false;
      window.clearTimeout(timer);
      aheadRunning -= 1;
      pumpAhead();
    };
    const timer = window.setTimeout(done, AHEAD_TURN_MS);
    image.onload = done;
    image.onerror = done;
    aheadRunning += 1;
    image.src = src;
    ahead.set(src, image);
    while (ahead.size > AHEAD_KEEP) ahead.delete(ahead.keys().next().value as string);
  }
}

// the pictures wanted next, soonest first: replaces what was waiting
export function warmAhead(srcs: readonly string[]): void {
  if (typeof window === 'undefined') return;
  const waiting: string[] = [];
  srcs.forEach((src) => {
    const image = ahead.get(src);
    if (image) {
      // still wanted: kept longest
      ahead.delete(src);
      ahead.set(src, image);
    } else if (!held.has(src)) {
      waiting.push(src);
    }
  });
  aheadQueue = waiting;
  pumpAhead();
}
