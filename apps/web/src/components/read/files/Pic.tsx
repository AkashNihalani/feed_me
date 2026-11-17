'use client';

/* ─────────────────────────────────────────────────────────────
   PIC — one picture treatment for the whole page. Many covers and
   stills are a split-screen or a 16:9 shot letterboxed onto a 9:16
   card: flat bands of colour above and below. Those bands were
   measured offline (the read file's `bands`: src → [top, bottom,
   aspect]); a picture here is cropped to what is actually in it
   and fitted to its box like `object-fit: cover`, in plain CSS.
   No state, no effects, no measuring at run time.
   ───────────────────────────────────────────────────────────── */

import { memo, type CSSProperties } from 'react';
import { cn } from '@/lib/utils';

type Bands = [number, number, number];
let BANDS: Record<string, Bands> = {};
// the read file's bands, set once when it loads
export function setBands(bands: Record<string, Bands> | undefined) {
  BANDS = bands ?? {};
}

const COVER: CSSProperties = { width: '100%', height: '100%', left: 0, top: 0, objectFit: 'cover' };

/* where the picture sits in a box of aspect `box` (width / height) so that what is in it covers the box; `trimTop` keeps
   that much more of its top out of sight (captions burned into the top band, under a mark or a name) */
export function fitOf(src: string | null, box: number, trimTop = 0): CSSProperties {
  const b = src ? BANDS[src] : undefined;
  if (!b) return trimTop ? { ...COVER, objectPosition: '50% 72%' } : COVER;
  const top = Math.max(b[0], trimTop);
  const bottom = b[1];
  const aspect = b[2] || 9 / 16;
  if (!top && !bottom) return COVER;
  const keep = Math.max(0.3, 1 - top - bottom);
  const region = aspect / keep;
  if (region >= box) {
    const height = 100 / keep;
    const width = (height * aspect) / box;
    return { height: `${height}%`, width: `${width}%`, top: `${-(top / keep) * 100}%`, left: `${(100 - width) / 2}%` };
  }
  const height = (box / aspect) * 100;
  const regionH = height * keep;
  return { width: '100%', height: `${height}%`, left: '0%', top: `${(100 - regionH) / 2 - top * height}%` };
}

/* the same crop laid out in the picture's own shape (never as object-fit), so the picture box keeps that shape whatever
   box it covers: what a box that changes shape needs, scaling the picture back to its true shape inside it. `natural` is
   the picture's own aspect when it has no measured bands. Returns the layout and the aspect of what it covers */
export function trueFitOf(src: string | null, box: number, natural = 9 / 16): { style: CSSProperties; region: number } {
  const b = src ? BANDS[src] : undefined;
  const top = b?.[0] ?? 0;
  const bottom = b?.[1] ?? 0;
  const aspect = b?.[2] || natural;
  const keep = Math.max(0.3, 1 - top - bottom);
  const region = aspect / keep;
  if (region >= box) {
    const height = 100 / keep;
    const width = (height * aspect) / box;
    return { region, style: { height: `${height}%`, width: `${width}%`, top: `${-(top / keep) * 100}%`, left: `${(100 - width) / 2}%`, objectFit: 'fill' } };
  }
  const height = (box / aspect) * 100;
  const regionH = height * keep;
  return { region, style: { width: '100%', height: `${height}%`, left: '0%', top: `${(100 - regionH) / 2 - top * height}%`, objectFit: 'fill' } };
}

export const Pic = memo(function Pic({ src, box, className, eager, alt = '', trimTop = 0 }: { src: string | null; box: number; className?: string; eager?: boolean; alt?: string; trimTop?: number }) {
  return (
    <span className={cn('rf-pic', className)}>
      {src ? (
        // eslint-disable-next-line @next/next/no-img-element
        <img src={src} alt={alt} width={420} height={746} decoding="async" loading={eager ? 'eager' : 'lazy'} draggable={false} style={fitOf(src, box, trimTop)} />
      ) : null}
    </span>
  );
});
