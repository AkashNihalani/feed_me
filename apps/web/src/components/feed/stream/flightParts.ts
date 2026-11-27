/* ─────────────────────────────────────────────────────────────
   FLIGHT PARTS — what a move between a grid and a phone's Day
   cards measures and flies, shared by the stream (FeedStream)
   and the Day layer over it (DayLayer): a post's frame as its
   4:5, and its number flown between its tile and its card.
   ───────────────────────────────────────────────────────────── */

import { CARRY_EASE_CSS, rectOf, run, type Flight, type Rect } from '@/lib/feedStream/motion';
import { THEME_COLORS } from '@/lib/themeBootstrap';

/* The page's own colours while a phone's Day card is up: the cards' dark (globals.css, [data-feed-card]), so the strip
   Safari tints at its status bar and round its toolbar reads as the card's; the theme's again once it is down */
const CARD_PAGE = '#141416';
export function cardPage(on: boolean) {
  const html = document.documentElement;
  const metas = document.querySelectorAll<HTMLMetaElement>('meta[name="theme-color"]');
  if (on) html.dataset.feedCard = '';
  else delete html.dataset.feedCard;
  const color = on ? CARD_PAGE : THEME_COLORS[html.classList.contains('dark') ? 'dark' : 'light'];
  metas.forEach((meta) => meta.setAttribute('content', color));
}

/* A post's frame on screen: a phone card's grown up under the status bar and on down to its 5:8 (PostCell, data-grown,
   data-below) counts as its 4:5, the picture a move carries */
export function frameRect(el: HTMLElement): Rect {
  const rect = rectOf(el);
  const grown = Number(el.dataset.grown) || 0;
  const below = Number(el.dataset.below) || 0;
  return grown > 0 || below > 0 ? { ...rect, top: rect.top + grown, height: rect.height - grown - below } : rect;
}

/* The post's number, one thing in both views (a card's "Top 27%", its tile's "27%"): two exact copies of it fly as one,
   each laid where its own stands (the old one where it stood as the move began), their first figures' baselines matched
   and scaled by their sizes, the first handing over to the second on the way (RANK_HAND). The two real ones are hidden
   meanwhile and shown again as it lands, where the second copy stands. Before, the one went and the other came in
   where it was, the number gone for a moment between */
export const RANK_HAND: [number, number] = [0.35, 0.65];
export type RankEnd = { el: HTMLElement; value: HTMLElement; shift: { x: number; y: number } };
// where a line of type stands: its first letter's left and its baseline (an empty inline probe), and its size
function markOf(el: HTMLElement, shift: { x: number; y: number }): { x: number; y: number; size: number } {
  const probe = document.createElement('span');
  probe.style.cssText = 'display:inline-block;width:0;height:0;vertical-align:baseline';
  el.prepend(probe);
  const box = probe.getBoundingClientRect();
  probe.remove();
  return { x: box.left + shift.x, y: box.bottom + shift.y, size: parseFloat(getComputedStyle(el).fontSize) || 1 };
}
// a still copy of a number where its own stands (shifted to where it stood), in the layer, its type as it is drawn
function rankTwin(end: RankEnd, layer: HTMLElement, layerBox: Rect): HTMLElement {
  const box = rectOf(end.el);
  const twin = end.el.cloneNode(true) as HTMLElement;
  twin.removeAttribute('data-rank');
  twin.removeAttribute('aria-live');
  twin.setAttribute('aria-hidden', 'true');
  twin.querySelectorAll('[id]').forEach((node) => node.removeAttribute('id'));
  // (no roll of its figures again, nor the old figures still rolling out; a card's "Top" stays with its words)
  twin.querySelectorAll('.st-roll-in').forEach((node) => node.classList.remove('st-roll-in'));
  twin.querySelectorAll('.st-roll > .absolute').forEach((node) => node.remove());
  if (end.value !== end.el) {
    const top = twin.firstElementChild as HTMLElement | null;
    if (top && !top.classList.contains('st-roll')) top.style.visibility = 'hidden';
  }
  const style = getComputedStyle(end.el);
  Object.assign(twin.style, {
    position: 'absolute',
    left: `${(box.left + end.shift.x - layerBox.left).toFixed(2)}px`,
    top: `${(box.top + end.shift.y - layerBox.top).toFixed(2)}px`,
    width: `${box.width.toFixed(2)}px`,
    height: `${box.height.toFixed(2)}px`,
    margin: '0px',
    zIndex: '8',
    pointerEvents: 'none',
    visibility: 'visible',
    opacity: '0',
    color: style.color,
    fontFamily: style.fontFamily,
    fontWeight: style.fontWeight,
    letterSpacing: style.letterSpacing,
    lineHeight: style.lineHeight,
    textShadow: style.textShadow,
    whiteSpace: 'nowrap',
  });
  layer.appendChild(twin);
  return twin;
}
// (measured and copied as the move is laid out, before any of its carries moves what they stand in; flown on its clock)
export function rankFlight(layer: HTMLElement, from: RankEnd, to: RankEnd): (flight: Flight, duration: number) => void {
  const layerBox = rectOf(layer);
  const a = markOf(from.value, from.shift);
  const b = markOf(to.value, to.shift);
  const fromBox = rectOf(from.el);
  const toBox = rectOf(to.el);
  const k = b.size / Math.max(a.size, 0.01);
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const one = rankTwin(from, layer, layerBox);
  const two = rankTwin(to, layer, layerBox);
  one.style.transformOrigin = `${(a.x - fromBox.left - from.shift.x).toFixed(2)}px ${(a.y - fromBox.top - from.shift.y).toFixed(2)}px`;
  two.style.transformOrigin = `${(b.x - toBox.left).toFixed(2)}px ${(b.y - toBox.top).toFixed(2)}px`;
  // (what each real one hides meanwhile: its figures and its %, never a card's "Top", which stays with its words)
  const parts = (end: RankEnd) => (end.value === end.el ? [end.el] : Array.from(end.el.children).filter((child): child is HTMLElement => child instanceof HTMLElement && (child === end.value || child.previousElementSibling !== null)));
  const hidden = [...parts(from), ...parts(to)];
  return (flight, duration) => {
    hidden.forEach((el) => (el.style.visibility = 'hidden'));
    const still = 'translate3d(0px, 0px, 0px) scale(1)';
    const timing: KeyframeAnimationOptions = { duration, easing: CARRY_EASE_CSS, fill: 'both' };
    run(flight, one, [
      { offset: 0, transform: still, opacity: 1 },
      { offset: RANK_HAND[0], opacity: 1 },
      { offset: RANK_HAND[1], opacity: 0 },
      { offset: 1, transform: `translate3d(${dx.toFixed(2)}px, ${dy.toFixed(2)}px, 0px) scale(${k.toFixed(4)})`, opacity: 0 },
    ], timing);
    run(flight, two, [
      { offset: 0, transform: `translate3d(${(-dx).toFixed(2)}px, ${(-dy).toFixed(2)}px, 0px) scale(${(1 / k).toFixed(4)})`, opacity: 0 },
      { offset: RANK_HAND[0], opacity: 0 },
      { offset: RANK_HAND[1], opacity: 1 },
      { offset: 1, transform: still, opacity: 1 },
    ], timing);
    // (the real ones back the moment it lands, not when the flight is let go: that can be a while on)
    let back = false;
    const land = () => {
      if (back) return;
      back = true;
      one.remove();
      two.remove();
      hidden.forEach((el) => (el.style.visibility = ''));
    };
    const flying = flight.animations[flight.animations.length - 1];
    void flying.finished.then(land, land);
    flight.cleanups.push(land);
  };
}
// a post's number in one of its layers, and the figures in it (a card's after its "Top")
export function rankIn(cell: HTMLElement, name: string, shift = { x: 0, y: 0 }): RankEnd | null {
  const el = cell.querySelector<HTMLElement>(`:scope > [data-layer="${name}"] [data-rank]`);
  if (!el) return null;
  return { el, value: el.querySelector<HTMLElement>('.st-roll') ?? el, shift };
}

