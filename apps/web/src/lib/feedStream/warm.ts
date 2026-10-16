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

// how many decoded pictures are held (a warm asking for more pushes its own first ones out)
export const HELD = 64;
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

/* SPARE — the pictures of a view that may come next (the Feed's other zooms, where it rests), fetched only once the
   look-ahead has nothing waiting, two at a time, at low priority: what the screen wants always goes first. Kept apart
   from the look-ahead's (they never push each other out); what hasn't been asked for yet is let go the moment
   something real is wanted. Fetched only, like the look-ahead's */
const SPARE_AT_ONCE = 2;
const SPARE_KEEP = 128;
const spare = new Map<string, HTMLImageElement>();
let spareQueue: string[] = [];
let spareRunning = 0;

// fetched (or on its way) already, by any of the three
const kept = (src: string) => ahead.has(src) || spare.has(src) || held.has(src);

// one picture fetched and kept in `into`; `done` once it has loaded or failed, or hung past its turn
function fetchOne(src: string, into: Map<string, HTMLImageElement>, keep: number, low: boolean, done: () => void) {
  const image = new Image();
  image.decoding = 'async';
  if (low) image.fetchPriority = 'low';
  let turn = true;
  const finish = () => {
    if (!turn) return;
    turn = false;
    window.clearTimeout(timer);
    done();
  };
  const timer = window.setTimeout(finish, AHEAD_TURN_MS);
  image.onload = finish;
  image.onerror = finish;
  image.src = src;
  into.set(src, image);
  while (into.size > keep) into.delete(into.keys().next().value as string);
}

function pumpAhead() {
  while (aheadRunning < AHEAD_AT_ONCE && aheadQueue.length) {
    const src = aheadQueue.shift() as string;
    if (kept(src)) continue;
    aheadRunning += 1;
    fetchOne(src, ahead, AHEAD_KEEP, false, () => {
      aheadRunning -= 1;
      pumpAhead();
    });
  }
  // nothing of the look-ahead's waiting: the spare's turn
  pumpSpare();
}

function pumpSpare() {
  while (spareRunning < SPARE_AT_ONCE && !aheadQueue.length && spareQueue.length) {
    const src = spareQueue.shift() as string;
    if (kept(src)) continue;
    spareRunning += 1;
    fetchOne(src, spare, SPARE_KEEP, true, () => {
      spareRunning -= 1;
      pumpSpare();
    });
  }
}

// the spare pictures, soonest first: replaces what was waiting. Returns what was waiting (let go unasked)
export function warmSpare(srcs: readonly string[]): string[] {
  if (typeof window === 'undefined') return [];
  const waiting = spareQueue;
  spareQueue = srcs.filter((src) => !kept(src));
  pumpSpare();
  return waiting;
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
    } else if (!kept(src)) {
      waiting.push(src);
    }
  });
  aheadQueue = waiting;
  pumpAhead();
}
