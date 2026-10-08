'use client';

/* ─────────────────────────────────────────────────────────────
   POST COVER — what a post wears when there is no picture to
   show (nothing stored and its Instagram link expired, or a
   picture that failed): a cover drawn rather than a broken
   image.

   The tone is the feeder's, so their posts read as theirs across
   a grid, lit softly from the top; over it the feeder's initials
   are cut large and faint, sized to the box (stream.css,
   .st-cover: a big box takes the faint monogram, a small one the
   tone alone). A chip (a highlight circle) shows the initials
   plainly instead, the way an avatar without a photo does.

   Still and painted once: two gradients and a line of type.
   The root is [data-post-cover]; a zoom flies its tone.
   ───────────────────────────────────────────────────────────── */

import { memo, type CSSProperties } from 'react';
import { feederInitials } from '@/components/feed/FeederStoryAvatar';
import { cn } from '@/lib/utils';
import '@/components/feed/stream/stream.css';

// dark tones with a little colour in their lit end, every one of them under white type (SVG-free, so no hex limits)
const TONES = [
  ['#4a1828', '#120609'],
  ['#3d2142', '#0e0710'],
  ['#1e2c42', '#070a11'],
  ['#45281b', '#0f0805'],
  ['#1d3529', '#060c09'],
  ['#35244c', '#0b0713'],
  ['#3c3220', '#0e0b06'],
  ['#2b2f36', '#0a0b0d'],
] as const;

// FNV-1a: the same tone for a handle every time it is drawn
function hash(value: string) {
  let h = 0x811c9dc5;
  for (let i = 0; i < value.length; i += 1) {
    h ^= value.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

export function coverTone(handle: string) {
  return TONES[hash(handle.replace(/^@+/, '').toLowerCase()) % TONES.length];
}

// bare: the tone alone, under a picture while it loads (so a tile is never a black hole, and the picture develops
// over its own feeder's colour)
function PostCover({ handle, chip = false, bare = false, className }: { handle: string; chip?: boolean; bare?: boolean; className?: string }) {
  const [from, to] = coverTone(handle);
  const style = { '--st-cover-from': from, '--st-cover-to': to } as CSSProperties;
  if (bare) return <span aria-hidden="true" className={cn('st-cover st-cover--bare', className)} style={style} />;
  return (
    <span aria-hidden="true" data-post-cover="" className={cn('st-cover', chip && 'st-cover--chip', className)} style={style}>
      <span className="st-cover-mono">{feederInitials(handle)}</span>
    </span>
  );
}

export default memo(PostCover);
