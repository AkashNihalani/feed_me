/* ─────────────────────────────────────────────────────────────
   STILL FRAME — a copy of what an element shows on screen, laid
   over it while the page underneath changes in a way WebKit would
   show as blank: a scroll the page makes itself far from where it
   was. iOS Safari draws the stretch of page it lands on only once
   its UI process has scrolled there (2–7 frames of nothing, the
   page's black). The copy holds the picture still over that, then
   goes, and nothing on screen has moved.

   Its pictures are not copied <img>s (a new image to WebKit, drawn
   a beat late or fetched again) but canvases painted from the ones
   on screen, so the copy is whole the moment it is drawn. It is
   drawn nearly invisible first and then shown, by opacity alone,
   so showing it changes nothing either. Canvases are copied pixel
   for pixel, a playing preview as its current frame; what is off
   screen is left out.

   It is fixed over the screen (an element of the page that scrolls
   with it does not hide WebKit's blank: measured, it showed every
   time), but leaves the screen's top edge alone: Safari 26 colours
   the status bar from what the fixed layers paint in a strip a few
   pixels from the top, and once it has a colour it keeps it for as
   long as the same header sits there (WebKit: LocalFrameView::
   fixedContainerEdges, PageColorSampler::predominantColor). So the
   copy starts below that strip, and is taller than the screen, which
   Safari never takes for a header.
   ───────────────────────────────────────────────────────────── */

export type StillFrame = {
  // resolves once the copy is on screen, whole
  shown: Promise<void>;
  // back over the screen after the page scrolled under it (a fixed copy: nothing to do)
  follow: () => void;
  remove: () => void;
  // goes by dissolving (drifting `dy` as it does) instead of at once; with `hold`, it drifts from the start but stays
  // whole that long before it dissolves (the page under it still being drawn)
  fadeOut: (ms: number, dy?: number, easing?: string, hold?: number) => void;
};

const nextFrame = () => new Promise<void>((resolve) => requestAnimationFrame(() => resolve()));

/* An element of the page (absolute) laid over a box of the screen, as a fixed one would be without being one (see
   above): placed, measured, and moved by however far it is off. Run again after the page scrolls */
export function pinToScreen(el: HTMLElement, box: { left: number; top: number }) {
  const now = el.getBoundingClientRect();
  const dx = box.left - now.left;
  const dy = box.top - now.top;
  if (Math.abs(dx) >= 0.5) el.style.left = `${(parseFloat(el.style.left) || 0) + dx}px`;
  if (Math.abs(dy) >= 0.5) el.style.top = `${(parseFloat(el.style.top) || 0) + dy}px`;
}
const sleep = (ms: number) => new Promise<void>((resolve) => window.setTimeout(resolve, ms));

/* A copy shows things as they stand: nothing in it plays again from its start (an arrival, a develop). And it is never
   somewhere the page snaps to: its posts would be snap points of the page's own snap (the phone's Day view), and Safari,
   finding the list of them changed, snaps the page again by its place in that list, to another card or anywhere */
const STYLE_ID = 'fm-still-frame';
const STYLE_RULES = '[data-still-frame], [data-still-frame] * { animation: none !important; transition: none !important; scroll-snap-align: none !important; scroll-snap-stop: normal !important; }';
function ensureStyle() {
  let style = document.getElementById(STYLE_ID);
  if (!style) {
    style = document.createElement('style');
    style.id = STYLE_ID;
    document.head.appendChild(style);
  }
  if (style.textContent !== STYLE_RULES) style.textContent = STYLE_RULES;
}

// a copy of part of the page that must not be a snap point of it (see above), for a copy outside a still frame
export function unsnap(el: HTMLElement) {
  el.style.scrollSnapAlign = 'none';
  el.style.scrollSnapStop = 'normal';
  el.querySelectorAll<HTMLElement>('*').forEach((node) => {
    if (node.style.scrollSnapAlign) node.style.scrollSnapAlign = 'none';
  });
}

function offScreen(el: Element, height: number): boolean {
  const box = el.getBoundingClientRect();
  return box.bottom < 0 || box.top > height || box.width < 1;
}

// a picture's canvas: never more pixels than it shows (cropped by object-fit: cover, at the screen's density)
const MAX_DENSITY = 2;
// the copy's height against the screen's: past Safari's 105% (a fixed element that large is never a header to it)
const TALLER = 1.2;
// the top of the screen the copy leaves alone: Safari samples the status bar's colour 4 to 6px from the top
const EDGE_CLEAR = 12;

/* A picture as a canvas painted from the <img> showing it: same class, style and data (object-fit and the rest apply
   to a canvas as to an image), the image's own aspect, as many pixels as the box shows. A picture from another origin
   still draws (the canvas is only barred from being read) */
function pictureCanvas(original: HTMLImageElement, copied: HTMLImageElement): HTMLCanvasElement | null {
  const box = original.getBoundingClientRect();
  const naturalWidth = original.naturalWidth;
  const naturalHeight = original.naturalHeight;
  if (!naturalWidth || !naturalHeight || box.width < 1 || box.height < 1) return null;
  const shown = Math.max(box.width / naturalWidth, box.height / naturalHeight) * Math.min(MAX_DENSITY, window.devicePixelRatio || 1);
  const scale = Math.min(1, shown);
  const canvas = document.createElement('canvas');
  canvas.width = Math.max(1, Math.round(naturalWidth * scale));
  canvas.height = Math.max(1, Math.round(naturalHeight * scale));
  const context = canvas.getContext('2d');
  if (!context) return null;
  try {
    context.imageSmoothingQuality = 'high';
    context.drawImage(original, 0, 0, canvas.width, canvas.height);
  } catch {
    return null;
  }
  for (const attribute of Array.from(copied.attributes)) {
    if (attribute.name === 'class' || attribute.name === 'style' || attribute.name.startsWith('data-')) canvas.setAttribute(attribute.name, attribute.value);
  }
  return canvas;
}

export function stillFrame(source: HTMLElement, { zIndex }: { zIndex: number }): StillFrame {
  const height = window.innerHeight;
  const rect = source.getBoundingClientRect();
  const copy = source.cloneNode(true) as HTMLElement;
  copy.removeAttribute('data-tab-scroll');

  // the pieces paired with their copies before anything is left out (both lists in document order)
  const pairs = <T extends Element>(selector: string) => {
    const from = source.querySelectorAll<T>(selector);
    const to = copy.querySelectorAll<T>(selector);
    return Array.from(from, (original, i) => [original, to[i]] as const).filter(([, copied]) => Boolean(copied));
  };
  const cells = pairs<HTMLElement>('[data-cell]');
  const images = pairs<HTMLImageElement>('img');
  const canvases = pairs<HTMLCanvasElement>('canvas');
  const videos = pairs<HTMLVideoElement>('video');

  // what is not part of the picture: an earlier copy still going, what the page marks as never drawn, and what a move
  // has let go of (unseen: copying it is only work)
  copy.querySelectorAll('[data-still-frame], [data-still-skip], [data-gone], [data-layer$="-out"]').forEach((node) => node.remove());

  cells.forEach(([original, copied]) => {
    if (offScreen(original, height)) copied.remove();
  });

  images.forEach(([original, copied]) => {
    if (!copy.contains(copied)) return;
    // a picture still loading shows nothing on the page either
    const canvas = !offScreen(original, height) && original.complete ? pictureCanvas(original, copied) : null;
    if (canvas) copied.replaceWith(canvas);
    else copied.remove();
  });
  canvases.forEach(([original, copied]) => {
    if (!copy.contains(copied) || original.width < 1 || original.height < 1) return;
    copied.width = original.width;
    copied.height = original.height;
    copied.getContext('2d')?.drawImage(original, 0, 0);
  });
  videos.forEach(([original, copied]) => {
    if (!copy.contains(copied)) return;
    // the frame it is showing, or the poster under it shows instead
    if (original.readyState >= 2 && original.videoWidth > 0 && !offScreen(original, height)) {
      try {
        const frame = document.createElement('canvas');
        frame.width = original.videoWidth;
        frame.height = original.videoHeight;
        frame.getContext('2d')?.drawImage(original, 0, 0);
        frame.className = copied.className;
        frame.setAttribute('style', copied.getAttribute('style') ?? '');
        copied.replaceWith(frame);
        return;
      } catch {
        // a frame it may not read
      }
    }
    copied.remove();
  });

  // a picture, not a page: nothing in it is focusable, labelled or a layer of its own
  copy.querySelectorAll<HTMLElement>('[style*="will-change"]').forEach((node) => {
    node.style.willChange = '';
  });
  copy.querySelectorAll('[id]').forEach((node) => node.removeAttribute('id'));
  Object.assign(copy.style, {
    position: 'absolute',
    left: `${rect.left}px`,
    top: `${rect.top - EDGE_CLEAR}px`,
    width: `${rect.width}px`,
    height: `${rect.height}px`,
    minHeight: '0px',
    margin: '0px',
    willChange: '',
  });

  ensureStyle();
  const cover = document.createElement('div');
  cover.setAttribute('data-still-frame', '');
  cover.setAttribute('aria-hidden', 'true');
  cover.inert = true;
  Object.assign(cover.style, {
    position: 'fixed',
    left: '0px',
    // below the strip Safari colours the status bar from, and taller than the screen (above)
    top: `${EDGE_CLEAR}px`,
    width: '100%',
    height: `${Math.round(TALLER * 100)}%`,
    overflow: 'hidden',
    pointerEvents: 'none',
    zIndex: String(zIndex),
    opacity: '0.01',
    // a layer of its own, drawn once
    willChange: 'opacity',
  });
  cover.appendChild(copy);
  document.body.appendChild(cover);
  const follow = () => undefined;

  let removed = false;
  const shown = (async () => {
    // painted while all but invisible; shown by opacity alone (nothing to paint again), trusted a frame on
    await nextFrame();
    await nextFrame();
    if (removed) return;
    cover.style.opacity = '1';
    await nextFrame();
  })();

  return {
    shown,
    follow,
    remove: () => {
      removed = true;
      cover.remove();
    },
    fadeOut: (ms, dy = 0, easing = 'cubic-bezier(0.2, 0.7, 0.2, 1)', hold = 0) => {
      removed = true;
      const total = ms + hold;
      // with a hold, the drift is one even ease from the start, and only the opacity waits
      const frames: Keyframe[] = hold > 0
        ? [
            { offset: 0, opacity: 1, transform: 'none', easing: 'cubic-bezier(0.25, 0.5, 0.35, 1)' },
            { offset: hold / total, opacity: 1, easing },
            { offset: 1, opacity: 0, transform: `translate3d(0px, ${dy}px, 0px)` },
          ]
        : [{ opacity: 1, transform: 'none' }, { opacity: 0, transform: `translate3d(0px, ${dy}px, 0px)` }];
      const fade = cover.animate(frames, hold > 0 ? { duration: total, fill: 'forwards' } : { duration: ms, easing, fill: 'forwards' });
      void fade.finished.catch(() => undefined).then(() => cover.remove());
    },
  };
}

/* The page under a copy, drawn where it now is: the pictures it shows decoded (WebKit paints a picture that has moved
   into the page's own tiles from a fresh decode, blurred or blank until then) and the frames coming at their pace again
   (the landing's work done). At most `budgetMs` */
export async function whenDrawn(page: HTMLElement, budgetMs: number): Promise<void> {
  const start = performance.now();
  const height = window.innerHeight;
  const shown = Array.from(page.querySelectorAll<HTMLImageElement>('img')).filter((img) => img.complete && img.naturalWidth > 0 && !offScreen(img, height));
  await Promise.race([Promise.all(shown.map((img) => img.decode().catch(() => undefined))), sleep(budgetMs)]);
  let smooth = 0;
  let last = performance.now();
  while (smooth < 3 && performance.now() - start < budgetMs) {
    await nextFrame();
    const now = performance.now();
    smooth = now - last < 25 ? smooth + 1 : 0;
    last = now;
  }
}
