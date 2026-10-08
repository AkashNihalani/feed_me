'use client';

/* ─────────────────────────────────────────────────────────────
   MEDIA FALLBACK — what a post image that can't load looks like,
   decided in one place for every surface.

   Post thumbnails come through /api/media: the stored copy in R2
   when there is one, else the Instagram link, which expires after
   a few days. When neither answers, the browser would draw its
   broken-image glyph. Instead the image swaps to a quiet cover,
   toned from the post's key so it is the same every time it is
   drawn, and keeps its own frame and object-fit.

   An image that handles its own failure opts out with
   data-media-fallback="off" (FeederStoryAvatar falls back to the
   feeder's initials).
   ───────────────────────────────────────────────────────────── */

import { useEffect } from 'react';

// dark, low-chroma pairs that sit on the app's ground the way Read's drawn covers do (SVG stops can't take var())
const TONES = [
  ['#3a1520', '#0d0507'],
  ['#2a1a22', '#0a0709'],
  ['#1f2128', '#08090b'],
  ['#33201a', '#0c0806'],
  ['#1c2622', '#070a09'],
  ['#2b1630', '#0a060c'],
] as const;

const MEDIA_HOST_SUFFIXES = ['cdninstagram.com', 'fbcdn.net'];

function parse(src: string) {
  try {
    return new URL(src, window.location.href);
  } catch {
    return null;
  }
}

function isPostMedia(url: URL) {
  if (url.pathname === '/api/media') return true;
  const host = url.hostname.toLowerCase();
  return MEDIA_HOST_SUFFIXES.some((suffix) => host === suffix || host.endsWith(`.${suffix}`));
}

// FNV-1a: a stable tone per post
function hash(value: string) {
  let h = 0x811c9dc5;
  for (let i = 0; i < value.length; i += 1) {
    h ^= value.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

export function mediaFallbackSrc(seed: string) {
  const [from, to] = TONES[hash(seed) % TONES.length];
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 400 500" preserveAspectRatio="xMidYMid slice">`
    + `<defs><linearGradient id="g" x1="0" y1="0" x2="0.6" y2="1"><stop offset="0" stop-color="${from}"/><stop offset="1" stop-color="${to}"/></linearGradient>`
    + `<radialGradient id="v" cx="0.5" cy="0.4" r="0.78"><stop offset="0.5" stop-color="#000" stop-opacity="0"/><stop offset="1" stop-color="#000" stop-opacity="0.45"/></radialGradient></defs>`
    + `<rect width="400" height="500" fill="url(#g)"/><rect width="400" height="500" fill="url(#v)"/>`
    + `<g transform="translate(164 214)" fill="none" stroke="#fff" stroke-opacity="0.2" stroke-width="5" stroke-linecap="round" stroke-linejoin="round">`
    + `<rect width="72" height="72" rx="18"/><circle cx="50" cy="22" r="7"/><path d="M8 60 28 38 44 52 52 46 66 60"/></g></svg>`;
  return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`;
}

function cover(img: HTMLImageElement) {
  if (img.dataset.mediaFallback === 'off') return;
  const src = img.currentSrc || img.src;
  if (!src || src.startsWith('data:')) return;
  const url = parse(src);
  if (!url || !isPostMedia(url)) return;
  img.removeAttribute('srcset');
  img.src = mediaFallbackSrc(url.searchParams.get('postKey') || src);
}

export default function MediaFallback() {
  useEffect(() => {
    // error doesn't bubble, so it is caught on the way down
    const onError = (event: Event) => {
      if (event.target instanceof HTMLImageElement) cover(event.target);
    };
    document.addEventListener('error', onError, true);
    // the ones that failed before this listener existed
    Array.from(document.images).forEach((img) => {
      if (img.complete && img.naturalWidth === 0) cover(img);
    });
    return () => document.removeEventListener('error', onError, true);
  }, []);
  return null;
}
