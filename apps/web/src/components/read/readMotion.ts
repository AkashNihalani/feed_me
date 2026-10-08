/* ─────────────────────────────────────────────────────────────
   READ MOTION — how a Read page leaves, piece by piece.

   Never the page as one block: every piece on screen (a header
   line, a board row, a run box, a post, the lens, a wall tile)
   lifts away on its own, in reading order, across a fixed spread
   so a full page takes as long as a sparse one. A trajectory's
   boxes leave one after another, left to right, and its card
   lets go of its surface once most of them have gone.

   When the leave starts from a row you picked, the rest of the
   page dims at once and parts around it, nearest first (above
   drifts up, below drifts down), while the row's own runs sweep
   once and it lifts away last.

   Used by the tab (ReadTab: any page change) and by the engine
   (one read to another). Only transform and opacity; only what
   is on screen; the leaving page takes no taps until it has gone.
   ───────────────────────────────────────────────────────────── */

const EASE_OUT = 'cubic-bezier(.16, .9, .2, 1)';
const EASE_IN_OUT = 'cubic-bezier(.45, 0, .2, 1)';
const EASE_IN = 'cubic-bezier(.4, 0, 1, 1)';

// the spread every piece's start is laid across, and how long each one takes to go
const SPREAD = 220;
const LEAVE = 360;
const DRIFT = 14;

const PIECES = [
  // the index pages: the header's lines, the board, a feeder's posts and picks
  '.ix-hd > *', '.ix-gh', '.ix-row', '.ix-sh', '.ix-post', '.ix-sec > .k', '.ix-best', '.ix-pick', '.ix > .ix-p',
  // any trajectory card's own pieces (a read's, or a feeder's)
  '.traj .tj-hd', '.traj .tj-b', '.traj .tj-key', '#rd-tjbody > *',
  // a read below its trajectory
  '.lens-head', '#rd-lens', '#rd-ins:not([hidden])', '#rd-feed > *',
].join(', ');
const SURFACES = '.traj';

function onScreen(rect: DOMRect, height: number) {
  return rect.width > 0 && rect.height > 0 && rect.bottom > 0 && rect.top < height;
}

export function leavePage(root: HTMLElement | null, chosen?: HTMLElement | null): Promise<void> {
  if (!root || typeof window === 'undefined' || window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    return Promise.resolve();
  }
  const height = window.innerHeight;
  // a page that is leaving takes no more taps (what they would land on is no longer what the page shows)
  root.style.pointerEvents = 'none';
  // every read first, every write after
  const pieces = Array.from(root.querySelectorAll<HTMLElement>(PIECES))
    .filter((el) => el !== chosen && !(chosen && chosen.contains(el)))
    .map((el) => ({ el, rect: el.getBoundingClientRect() }))
    .filter(({ rect }) => onScreen(rect, height))
    .map((piece) => ({ ...piece, from: Number(getComputedStyle(piece.el).opacity) }))
    .sort((a, b) => a.rect.top - b.rect.top || a.rect.left - b.rect.left);
  const surfaces = Array.from(root.querySelectorAll<HTMLElement>(SURFACES))
    .filter((el) => onScreen(el.getBoundingClientRect(), height));
  const pivot = chosen ? chosen.getBoundingClientRect() : null;
  const runs = chosen ? Array.from(chosen.querySelectorAll<HTMLElement>('.ix-run')) : [];

  const anims: Animation[] = [];
  const order = pivot
    ? [...pieces].sort((a, b) => Math.abs(a.rect.top - pivot.top) - Math.abs(b.rect.top - pivot.top))
    : pieces;
  const at = (k: number) => (order.length > 1 ? (k / (order.length - 1)) * SPREAD : 0);

  order.forEach(({ el, rect, from }, k) => {
    const start = Number.isFinite(from) ? from : 1;
    // a headline leaves the way it came: its words slide up out of their masks, one after another
    const words = el.matches('.ix-h') ? Array.from(el.querySelectorAll<HTMLElement>('.ix-w > span')) : [];
    if (words.length) {
      const begin = (pivot ? 80 : 20) + at(k);
      words.forEach((word, j) => {
        anims.push(word.animate([{ transform: 'none' }, { transform: 'translateY(-108%)' }], { duration: 380, delay: begin + j * 16, easing: EASE_IN, fill: 'forwards' }));
      });
      return;
    }
    if (pivot) {
      // dim at once, so the row you picked is the only thing lit; then part around it
      const dim = start * 0.3;
      anims.push(el.animate([{ opacity: start }, { opacity: dim }], { duration: 200, easing: EASE_OUT, fill: 'forwards' }));
      const away = rect.top > pivot.top ? DRIFT : -DRIFT;
      anims.push(el.animate(
        [{ opacity: dim, transform: 'none' }, { opacity: 0, transform: `translateY(${away}px)` }],
        { duration: LEAVE, delay: 120 + at(k), easing: EASE_IN_OUT, fill: 'forwards' },
      ));
    } else {
      anims.push(el.animate(
        [{ opacity: start, transform: 'none' }, { opacity: 0, transform: `translateY(${-DRIFT}px)` }],
        { duration: LEAVE, delay: 40 + at(k), easing: EASE_IN_OUT, fill: 'forwards' },
      ));
    }
  });

  // a card lets go of its surface once most of its pieces are on their way
  surfaces.forEach((el) => {
    anims.push(el.animate([{ opacity: 1 }, { opacity: 0 }], { duration: 320, delay: (pivot ? 120 : 40) + SPREAD * 0.6, easing: EASE_IN_OUT, fill: 'forwards' }));
  });

  if (chosen) {
    // the row you picked: its runs sweep once, left to right, while it lifts; then it goes last
    runs.forEach((box, k) => {
      anims.push(box.animate(
        [{ transform: 'none' }, { transform: 'translateY(-3px) scaleY(1.1)', offset: 0.45 }, { transform: 'none' }],
        { duration: 440, delay: k * 32, easing: EASE_IN_OUT },
      ));
    });
    anims.push(chosen.animate([{ transform: 'none' }, { transform: 'translateY(-3px)' }], { duration: 260, easing: EASE_OUT, fill: 'forwards' }));
    anims.push(chosen.animate(
      [{ opacity: 1, transform: 'translateY(-3px)' }, { opacity: 0, transform: 'translateY(-26px)' }],
      { duration: 400, delay: 340, easing: EASE_IN, fill: 'forwards' },
    ));
  }

  return Promise.all(anims.map((anim) => anim.finished.catch(() => undefined))).then(() => {
    root.style.pointerEvents = '';
  });
}
