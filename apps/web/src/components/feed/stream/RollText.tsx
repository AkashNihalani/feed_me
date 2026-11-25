'use client';

/* ─────────────────────────────────────────────────────────────
   ROLL TEXT — a post's number (and its figures) rolling to what
   it was at another of its reads.

   What changed turns in its place: the old glyphs leave one way
   as the new come in from the other, a beat apart, up for a later
   read and down for an earlier one (`at` says which is later).
   The new text is laid out at once, so nothing is ever held open
   for a glyph that has gone: what stands after the text (`after`:
   the % sign) glides from where it was to where it now is.
   Transform and opacity only; under reduced motion it just says
   the new value.
   ───────────────────────────────────────────────────────────── */

import { useEffect, useLayoutEffect, useRef, useState, type CSSProperties, type ReactNode } from 'react';
import { CARRY_EASE_CSS, reducedMotion } from '@/lib/feedStream/motion';
import { ms } from '@/lib/tempo';
import { cn } from '@/lib/utils';
import '@/components/feed/stream/stream.css';

const useIsoLayoutEffect = typeof window !== 'undefined' ? useLayoutEffect : useEffect;

// one glyph turns this long after the one before it; what stands after the text is out of their way as they land
const STEP_MS = ms(46);
const GLIDE_MS = ms(300);

function firstDifference(a: string, b: string) {
  let index = 0;
  while (index < a.length && index < b.length && a[index] === b[index]) index += 1;
  return index;
}

export default function RollText({ value, at = 0, after, className, style }: {
  value: string;
  // where the value stands in time (a later one rolls up, an earlier one down)
  at?: number;
  after?: ReactNode;
  className?: string;
  style?: CSSProperties;
}) {
  const [shown, setShown] = useState<{ value: string; at: number; old: string | null; way: 1 | -1; turn: number }>({ value, at, old: null, way: 1, turn: 0 });
  if (shown.value !== value) setShown({ value, at, old: shown.value, way: at >= shown.at ? 1 : -1, turn: shown.turn + 1 });
  else if (shown.at !== at) setShown({ ...shown, at });

  const afterRef = useRef<HTMLSpanElement | null>(null);
  const textRef = useRef<HTMLSpanElement | null>(null);
  const widthRef = useRef(0);
  // what stands after the text: from where the old text's end had it, to the new one's
  useIsoLayoutEffect(() => {
    const was = widthRef.current;
    const width = textRef.current?.offsetWidth ?? 0;
    widthRef.current = width;
    const el = afterRef.current;
    if (!el || !shown.turn || !was || Math.abs(was - width) < 0.5 || reducedMotion()) return;
    el.getAnimations().forEach((animation) => animation.cancel());
    el.animate([{ transform: `translate3d(${was - width}px, 0px, 0px)` }, { transform: 'translate3d(0px, 0px, 0px)' }], { duration: GLIDE_MS, easing: CARRY_EASE_CSS });
  }, [shown.turn]);

  const from = shown.old === null ? Number.POSITIVE_INFINITY : firstDifference(shown.old, shown.value);
  const steps = Math.max(0, (shown.old?.length ?? 0) - from - 1);
  return (
    <>
      <span ref={textRef} aria-label={shown.value} role="text" className={cn('st-roll relative inline-block', className)} style={style} data-way={shown.way > 0 ? 'up' : 'down'}>
        <span aria-hidden="true" className="inline-flex whitespace-pre">
          {Array.from(shown.value).map((glyph, index) => (
            <span key={`${index < from ? 'still' : shown.turn}:${index}`} className={cn('inline-block', index >= from && 'st-roll-in')} style={index >= from ? ({ '--st-roll-at': `${(index - from) * STEP_MS}ms` } as CSSProperties) : undefined}>{glyph}</span>
          ))}
        </span>
        {shown.old !== null && from < shown.old.length ? (
          <span key={`old:${shown.turn}`} aria-hidden="true" className="pointer-events-none absolute left-0 top-0 inline-flex whitespace-pre">
            {Array.from(shown.old).map((glyph, index) => (
              <span
                key={index}
                className={cn('inline-block', index < from ? 'invisible' : 'st-roll-out')}
                style={index >= from ? ({ '--st-roll-at': `${(index - from) * STEP_MS}ms` } as CSSProperties) : undefined}
                onAnimationEnd={index - from === steps ? () => setShown((now) => (now.turn === shown.turn ? { ...now, old: null } : now)) : undefined}
              >
                {glyph}
              </span>
            ))}
          </span>
        ) : null}
      </span>
      {after ? <span ref={afterRef} className="inline-block">{after}</span> : null}
    </>
  );
}
