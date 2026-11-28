/* ─────────────────────────────────────────────────────────────
   DAY MOVE — the one move between a grid's tile and the Day
   view's post, a phone's (DayLayer) and a wide window's
   (DayWide) alike, so the two feel the same.

   In: the Day's own picture is laid over the tile's picture just
   as the tile draws it (cut to the tile's box, wearing what the
   tile wears) and carried to its place, its cut opening as it
   grows; the grid zooms on the picture's curve, so the tile stays
   under it; the Day's ground comes over the grid by half way;
   the post's number flies from the tile's corner to the Day's
   (rankFlight); what the Day's picture wears comes in late, its
   words later still. Out: all of it back, onto the post's tile.

   A move is laid out at its first frame and held there (its
   flight's hold) till it is drawn, PRIME_FRAMES on: the first
   paint of a new layer would eat its start.
   ───────────────────────────────────────────────────────────── */

import { frameRect, rankFlight, rankIn, type RankEnd, type RankRide, type RankSweep } from '@/components/feed/stream/flightParts';
import { CARRY_EASE_CSS, carry, carryCurve, rectOf, run, type Flight, type Rect } from '@/lib/feedStream/motion';
import { moveMs } from '@/lib/tempo';

// one move in or out
export const DAY_MOVE_MS = moveMs(560);
// a move held at its first frame (released PRIME_FRAMES on, once its layers are drawn)
export const DAY_HOLD_MS = 800;
// how many frames a move is held at its first pose before its clock runs (?prime=N to try others)
export const PRIME_FRAMES = typeof window !== 'undefined' && /[?&]prime=(\d)/.test(window.location.search) ? Number(/[?&]prime=(\d)/.exec(window.location.search)?.[1]) : 2;

/* The Day's ground over the grid: in, whole by GROUND_IN (so nothing of the page, its gaps three times their size, is
   left showing round the picture); out, lifting from GROUND_OUT as the grid zooms back */
const GROUND_IN = 0.5;
const GROUND_OUT = 0.4;
/* What the Day's picture wears that its tile doesn't: in late on the way in, gone early on the way out, so the picture
   flies as the tile's own */
const WEAR_IN_FROM = 0.4;
const WEAR_OUT_BY = 0.3;
// ...and what its tile wears (tileWear): on from here on the way out, whole by here
const WEAR_TILE_FROM = 0.45;
const WEAR_TILE_BY = 0.9;
// the Day's words: in from here (risen a little), out by here
const WORDS_IN_FROM = 0.45;
const WORDS_OUT_BY = 0.22;
const WORDS_RISE = 'translate3d(0px, 10px, 0px)';
const WORDS_STILL = 'translate3d(0px, 0px, 0px)';
/* A wide window's Day (DayMove.wide) moves as its first try did, the one the grid was seen moving in: its ground (the
   light page) on the move's own curve, whole by WIDE_GROUND_IN going in and gone by WIDE_GROUND_OUT coming out, so the
   grid zooms on whole most of the way (a phone's even wash over it read as a fade there); its words slide in from the
   left a beat on and out to the left at once; its number's colour swept over by the picture's edge (RankSweep) */
const WIDE_GROUND_IN = 0.45;
const WIDE_GROUND_OUT = 0.5;
const WIDE_WORDS_FROM = 'translate3d(-32px, 0px, 0px)';
const WIDE_WORDS_TO = 'translate3d(-24px, 0px, 0px)';
const WIDE_WORDS_AFTER = 0.3;
const WIDE_WORDS_GO = 0.29;
const WIDE_WORDS_GO_EASE = 'cubic-bezier(0.4, 0, 1, 1)';
// the picture's box sampled this often over a move, for the number's sweep
const SWEEP_SAMPLES = 24;

export const frames = (count: number) =>
  new Promise<void>((resolve) => {
    let left = count;
    const step = () => {
      left -= 1;
      if (left <= 0) resolve();
      else requestAnimationFrame(step);
    };
    requestAnimationFrame(step);
  });

/* The grid's zoom that puts a tile (P, untransformed: where it stands with the page's own shift `shift` taken off) onto
   the Day's picture (to): a translate and scale of the page about its top-left (L) */
function camera(page: Rect, shift: number, tile: Rect, to: Rect): string {
  const k = to.width / Math.max(tile.width, 0.01);
  const left = page.left;
  const top = page.top - shift;
  const tx = to.left - left - (tile.left - left) * k;
  const ty = to.top - top - (tile.top - shift - top) * k;
  return `translate(${tx.toFixed(2)}px, ${ty.toFixed(2)}px) scale(${k.toFixed(4)})`;
}
export const shifted = (by: number) => `translate(0px, ${by.toFixed(2)}px) scale(1)`;

/* Where a frame's picture is drawn: its content as object-fit and object-position lay it in the box it is drawn in
   (`box`: its own on screen; a tile's frame, as the tile's picture gives under a finger). For a carousel, the slide in
   view. Null while it has no size */
const POSITION: Record<string, string> = { left: '0%', top: '0%', center: '50%', right: '100%', bottom: '100%' };
function pictureOf(frame: HTMLElement, boxOf: (img: HTMLImageElement) => Rect = rectOf, pictures = 'img.st-img'): Rect | null {
  const frameBox = rectOf(frame);
  for (const img of Array.from(frame.querySelectorAll<HTMLImageElement>(pictures))) {
    if (!img.naturalWidth || !img.naturalHeight) continue;
    const at = boxOf(img);
    const mid = at.left + at.width / 2;
    if (at.width < 1 || at.height < 1 || mid < frameBox.left || mid > frameBox.left + frameBox.width) continue;
    const style = getComputedStyle(img);
    if (style.objectFit !== 'cover' && style.objectFit !== 'contain') return at;
    const sx = at.width / img.naturalWidth;
    const sy = at.height / img.naturalHeight;
    const scale = style.objectFit === 'cover' ? Math.max(sx, sy) : Math.min(sx, sy);
    const width = img.naturalWidth * scale;
    const height = img.naturalHeight * scale;
    const [x = '50%', y = '50%'] = style.objectPosition.trim().split(/\s+/).map((part) => POSITION[part] ?? part);
    const place = (value: string, free: number) => (value.endsWith('%') ? (free * parseFloat(value)) / 100 : parseFloat(value) || 0);
    return { left: at.left + place(x, at.width - width), top: at.top + place(y, at.height - height), width, height };
  }
  return null;
}

/* The Day's frame (at rest at `at`) laid over its tile so the picture stands just where the tile draws it: the tile is
   3:4 and crops its picture about its middle, the Day shows its own window of it (a phone's 4:5 under a top grown up
   under the clock, a wide window's picture off three edges), so the two pictures are matched, not the two boxes.
   `onto`: the Day's frame there; `clip`: its frame cut to the tile's box (frame px); `inCard`: the tile's box in the
   Day's terms, for the grid's zoom */
type Cuts = { top: number; right: number; bottom: number; left: number };
type Landing = { k: number; onto: Rect; clip: string; cuts: Cuts; radius: number; inCard: Rect };
function landing(frame: HTMLElement, at: Rect, tileFrame: HTMLElement, pictures?: string): Landing {
  const tileBox = frameRect(tileFrame);
  const card = pictureOf(frame, rectOf, pictures);
  const tile = pictureOf(tileFrame, () => tileBox);
  let k: number;
  let onto: Rect;
  if (card && tile && card.width > 0) {
    k = tile.width / card.width;
    onto = { left: tile.left - (card.left - at.left) * k, top: tile.top - (card.top - at.top) * k, width: at.width * k, height: at.height * k };
  } else {
    // (no picture to go by: the Day's window onto the tile's box)
    const window = frameRect(frame);
    k = tileBox.width / Math.max(window.width, 0.01);
    onto = { left: tileBox.left - (window.left - at.left) * k, top: tileBox.top - (window.top - at.top) * k, width: at.width * k, height: at.height * k };
  }
  const cutOf = (value: number) => Math.max(0, value / Math.max(k, 0.01));
  const cuts = {
    top: cutOf(tileBox.top - onto.top),
    right: cutOf(onto.left + onto.width - tileBox.left - tileBox.width),
    bottom: cutOf(onto.top + onto.height - tileBox.top - tileBox.height),
    left: cutOf(tileBox.left - onto.left),
  };
  const radius = (parseFloat(getComputedStyle(tileFrame).borderTopLeftRadius) || 0) / Math.max(k, 0.01);
  const px = (value: number) => `${value.toFixed(1)}px`;
  const clip = `inset(${px(cuts.top)} ${px(cuts.right)} ${px(cuts.bottom)} ${px(cuts.left)} round ${radius.toFixed(1)}px)`;
  const inCard = {
    left: at.left + (tileBox.left - onto.left) / k,
    top: at.top + (tileBox.top - onto.top) / k,
    width: tileBox.width / k,
    height: tileBox.height / k,
  };
  return { k, onto, clip, cuts, radius, inCard };
}

/* The picture's box on screen through a move (the frame as carried, cut as its clip is), as clip-path keyframes in the
   layer's terms, linear in time: what the number's white copy is cut to (RankSweep). Near the tile, where the grid shows
   round the frame, the tile's whole box under the zoom counts too: a Day's 4:5 window is shorter than the 3:4 tile, and
   the foot of the number settling into the tile's corner stood under the window's edge, in ink */
const TILE_COUNTS_TO = 0.5;
const TILE_COUNTS_FADE = 0.15;
function pictureBoxes(layer: Rect, at: Rect, tileBox: Rect, fit: Landing, way: 'in' | 'out'): Keyframe[] {
  const frames: Keyframe[] = [];
  const lerp = (from: number, to: number, t: number) => from + (to - from) * t;
  for (let step = 0; step <= SWEEP_SAMPLES; step += 1) {
    const offset = step / SWEEP_SAMPLES;
    const e = carryCurve(offset);
    // (how far it is from the tile to the Day: the frame there, and the share of its cut left)
    const p = way === 'in' ? e : 1 - e;
    const left = lerp(fit.onto.left, at.left, p);
    const top = lerp(fit.onto.top, at.top, p);
    const width = lerp(fit.onto.width, at.width, p);
    const height = lerp(fit.onto.height, at.height, p);
    const scale = (width / Math.max(at.width, 0.01)) * (1 - p);
    let box = { left: left + fit.cuts.left * scale, top: top + fit.cuts.top * scale, right: left + width - fit.cuts.right * scale, bottom: top + height - fit.cuts.bottom * scale };
    const counts = Math.min(1, Math.max(0, (TILE_COUNTS_TO - p) / TILE_COUNTS_FADE));
    if (counts > 0) {
      const tile = {
        left: lerp(tileBox.left, fit.inCard.left, p),
        top: lerp(tileBox.top, fit.inCard.top, p),
        right: lerp(tileBox.left + tileBox.width, fit.inCard.left + fit.inCard.width, p),
        bottom: lerp(tileBox.top + tileBox.height, fit.inCard.top + fit.inCard.height, p),
      };
      box = {
        left: lerp(box.left, Math.min(box.left, tile.left), counts),
        top: lerp(box.top, Math.min(box.top, tile.top), counts),
        right: lerp(box.right, Math.max(box.right, tile.right), counts),
        bottom: lerp(box.bottom, Math.max(box.bottom, tile.bottom), counts),
      };
    }
    const inset = (value: number) => `${Math.max(0, value).toFixed(1)}px`;
    frames.push({
      offset,
      clipPath: `inset(${inset(box.top - layer.top)} ${inset(layer.left + layer.width - box.right)} ${inset(layer.top + layer.height - box.bottom)} ${inset(box.left - layer.left)} round ${(fit.radius * scale).toFixed(1)}px)`,
    });
  }
  return frames;
}

/* What a tile wears over its picture and the Day's picture doesn't (its shade, its ▶, a 2×2's face, a winner's ring;
   never its number, which flies on its own): copied into the Day's frame where the tile lies in it, so it rides the
   frame's carry and is the tile's own size as it lands. Without it they were gone the moment a move in set off, and
   came in only as the tile took over, a beat after a move out had landed */
function tileWear(frame: HTMLElement, at: Rect, tileFrame: HTMLElement, fit: Landing): HTMLElement | null {
  const parts = tileFrame.querySelectorAll<HTMLElement>(':scope > [data-layer="chips"], :scope > [data-ring]');
  if (!parts.length) return null;
  const box = frameRect(tileFrame);
  const wear = document.createElement('span');
  wear.setAttribute('aria-hidden', 'true');
  wear.style.cssText = [
    'position:absolute',
    'pointer-events:none',
    'z-index:7',
    'transform-origin:0 0',
    `left:${(fit.inCard.left - at.left).toFixed(2)}px`,
    `top:${(fit.inCard.top - at.top).toFixed(2)}px`,
    `width:${box.width.toFixed(2)}px`,
    `height:${box.height.toFixed(2)}px`,
    `transform:scale(${(1 / Math.max(fit.k, 0.01)).toFixed(5)})`,
    'opacity:0',
  ].join(';');
  parts.forEach((part) => {
    const copy = part.cloneNode(true) as HTMLElement;
    copy.querySelectorAll<HTMLElement>('[data-rank]').forEach((rank) => (rank.style.visibility = 'hidden'));
    wear.appendChild(copy);
  });
  frame.appendChild(wear);
  return wear;
}

export type DayMove = {
  way: 'in' | 'out';
  // the Day's layer (the number's copies fly in it)
  layer: HTMLElement;
  // the Day's picture, carried (and the pictures in it, when not a PostCell's: img.st-img)
  frame: HTMLElement;
  pictures?: string;
  // the post's tile in the grid, and its frame
  tile: HTMLElement;
  tileFrame: HTMLElement;
  // the grid, and the shift it stands at (a phone's page held down under the clock as Day opened at its top)
  page: HTMLElement;
  shift?: number;
  // the Day's number (null: the post has none, or it isn't shown)
  rank: RankEnd | null;
  // the Day's ground, and how it looks gone and whole
  ground: { el: HTMLElement; clear: Keyframe; whole: Keyframe };
  // the Day's words: in late, out early
  words: HTMLElement[];
  // what the Day's picture wears and its tile doesn't
  wears: HTMLElement[];
  // in once the ground is mostly over (a phone card's own ground under its reading)
  late?: HTMLElement[];
  // what of the Day isn't the post (a phone's other cards): gone at once on the way out
  others?: HTMLElement[];
  // a wide window's Day (its own timing: see WIDE_GROUND_IN), and its pictures' box (the number's ink copy flies under it)
  wide?: { pictures: HTMLElement };
  duration?: number;
};

/* Lays a move out on `flight` (held: it runs once released). Everything is measured before anything is moved. Returns
   the tile's wear riding the frame, for the caller to take away once the move is done */
export function layDayMove(flight: Flight, move: DayMove): { wear: HTMLElement | null } {
  const { way, layer, frame, tile, tileFrame, page, ground } = move;
  const duration = move.duration ?? DAY_MOVE_MS;
  const shift = move.shift ?? 0;
  const tileBox = frameRect(tileFrame);
  const at = rectOf(frame);
  const fit = landing(frame, at, tileFrame, move.pictures);
  const wear = tileWear(frame, at, tileFrame, fit);
  // (the number measured where both stand, before anything moves them)
  const tileRank = rankIn(tile, 'frame');
  const wide = move.wide ?? null;
  const sweep: RankSweep | undefined = wide ? { day: way === 'in' ? 'to' : 'from', under: wide.pictures, clip: pictureBoxes(rectOf(layer), at, tileBox, fit, way) } : undefined;
  const ride: RankRide | undefined = wide ? { tile: way === 'in' ? 'from' : 'to', box: tileBox, zoomed: fit.inCard } : undefined;
  const rank = tileRank && move.rank ? (way === 'in' ? rankFlight(layer, tileRank, move.rank, { sweep, ride }) : rankFlight(layer, move.rank, tileRank, { sweep, ride })) : null;
  const zoomed = camera(rectOf(page), shift, tileBox, fit.inCard);
  const still = shifted(shift);
  const linear = (fill: FillMode): KeyframeAnimationOptions => ({ duration, easing: 'linear', fill });
  /* (what fades goes to and from its own look at rest, not to whole: a wide window's picture wears a shade and arrows
     unseen at rest, which a fade to 1 would have shown) */
  const restOf = (el: HTMLElement) => {
    const opacity = Number.parseFloat(getComputedStyle(el).opacity);
    return Number.isFinite(opacity) ? opacity : 1;
  };
  const wears = move.wears.map((el) => [el, restOf(el)] as const);
  const words = move.words.map((el) => [el, restOf(el)] as const);
  const late = (move.late ?? []).map((el) => [el, restOf(el)] as const);
  const curved = (fill: FillMode): KeyframeAnimationOptions => ({ duration, easing: CARRY_EASE_CSS, fill });
  page.style.transformOrigin = '0 0';
  if (way === 'in') {
    carry(flight, frame, at, fit.onto, at, { duration });
    run(flight, frame, [{ clipPath: fit.clip }, { clipPath: 'inset(0px 0px 0px 0px round 0px)' }], curved('backwards'));
    // (the tile's shade and ▶ go with its picture as it grows, gone by the time it is the Day's)
    if (wear) run(flight, wear, [{ opacity: 1 }, { opacity: 0, offset: WEAR_OUT_BY }, { opacity: 0 }], linear('both'));
    if (wide) run(flight, ground.el, [ground.clear, { ...ground.whole, offset: WIDE_GROUND_IN }, ground.whole], curved('both'));
    else run(flight, ground.el, [ground.clear, { ...ground.whole, offset: GROUND_IN }, ground.whole], linear('both'));
    late.forEach(([el, rest]) => run(flight, el, [{ opacity: 0 }, { opacity: 0, offset: 0.45 }, { opacity: rest }], linear('backwards')));
    wears.forEach(([el, rest]) => run(flight, el, [{ opacity: 0 }, { opacity: 0, offset: WEAR_IN_FROM }, { opacity: rest }], linear('backwards')));
    words.forEach(([el, rest]) => {
      if (wide) {
        run(flight, el, [{ opacity: 0, transform: WIDE_WORDS_FROM }, { opacity: rest, transform: WORDS_STILL }], {
          duration: duration * (1 - WIDE_WORDS_AFTER),
          delay: duration * WIDE_WORDS_AFTER,
          easing: CARRY_EASE_CSS,
          fill: 'backwards',
        });
        return;
      }
      run(flight, el, [
        { opacity: 0, transform: WORDS_RISE },
        { opacity: 0, transform: WORDS_RISE, offset: WORDS_IN_FROM },
        { opacity: rest, transform: WORDS_STILL },
      ], curved('backwards'));
    });
    // the grid zooms on the picture's curve, and goes as the Day's ground comes over it
    run(flight, page, [{ transform: still }, { transform: zoomed }], curved('both'));
  } else {
    // (it holds where it lands till the Day is taken down: never a frame back where it was)
    carry(flight, frame, at, at, fit.onto, { duration, fill: 'both' });
    run(flight, frame, [{ clipPath: 'inset(0px 0px 0px 0px round 0px)' }, { clipPath: fit.clip }], curved('both'));
    // (the tile's shade and ▶ come on with the picture as it shrinks into its place, whole as it lands)
    if (wear) run(flight, wear, [{ opacity: 0 }, { opacity: 0, offset: WEAR_TILE_FROM }, { opacity: 1, offset: WEAR_TILE_BY }, { opacity: 1 }], linear('both'));
    if (wide) run(flight, ground.el, [ground.whole, { ...ground.clear, offset: WIDE_GROUND_OUT }, ground.clear], curved('both'));
    else run(flight, ground.el, [ground.whole, { ...ground.whole, offset: GROUND_OUT }, ground.clear], linear('both'));
    late.forEach(([el, rest]) => run(flight, el, [{ opacity: rest }, { opacity: rest, offset: 0.15 }, { opacity: 0, offset: GROUND_OUT }, { opacity: 0 }], linear('both')));
    wears.forEach(([el, rest]) => run(flight, el, [{ opacity: rest }, { opacity: 0, offset: WEAR_OUT_BY }, { opacity: 0 }], linear('both')));
    words.forEach(([el, rest]) => {
      if (wide) run(flight, el, [{ opacity: rest, transform: WORDS_STILL }, { opacity: 0, transform: WIDE_WORDS_TO }], { duration: duration * WIDE_WORDS_GO, easing: WIDE_WORDS_GO_EASE, fill: 'both' });
      else run(flight, el, [{ opacity: rest }, { opacity: 0, offset: WORDS_OUT_BY }, { opacity: 0 }], linear('both'));
    });
    move.others?.forEach((el) => run(flight, el, [{ opacity: 0 }, { opacity: 0 }], linear('both')));
    run(flight, page, [{ transform: zoomed }, { transform: still }], curved('both'));
    // (the grid whole under the ground from the start: it shows as the ground lifts)
    run(flight, page, [{ opacity: 1 }, { opacity: 1 }], linear('both'));
  }
  rank?.(flight, duration);
  return { wear };
}
