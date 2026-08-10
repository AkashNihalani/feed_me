'use client';

/* ─────────────────────────────────────────────
   SLOT TEXT

   The login odometer grammar (components/login/Odometer.tsx) generalized to
   arbitrary strings: when the value changes, only the glyphs that differ roll
   with the weighted vertical slot animation, then settle to static text.
   `align="right"` diffs from the units glyph (numbers), `align="left"` from
   the first glyph (words). Reduced motion -> plain text swap.
   ───────────────────────────────────────────── */

import { CSSProperties, useEffect, useState } from 'react';
import { useReducedMotion } from 'framer-motion';
import { cn } from '@/lib/utils';

const SLOT_STAGGER_MS = 24;
const SLOT_MAX_STAGGER_STEPS = 10;
const SLOT_SETTLE_MS = 420 + 34 + SLOT_MAX_STAGGER_STEPS * SLOT_STAGGER_MS + 60;

type SlotAlign = 'left' | 'right';

type Glyph = {
  key: string;
  previousChar: string;
  currentChar: string;
  changed: boolean;
  delaySteps: number;
};

function glyphsFor(previous: string, current: string, align: SlotAlign): Glyph[] {
  const previousChars = Array.from(previous);
  const currentChars = Array.from(current);
  const length = Math.max(previousChars.length, currentChars.length);

  return Array.from({ length }, (_, index) => {
    const previousIndex = align === 'right' ? previousChars.length - length + index : index;
    const currentIndex = align === 'right' ? currentChars.length - length + index : index;
    const previousChar = previousIndex >= 0 && previousIndex < previousChars.length ? previousChars[previousIndex] : '';
    const currentChar = currentIndex >= 0 && currentIndex < currentChars.length ? currentChars[currentIndex] : '';
    const anchorDistance = align === 'right' ? length - index - 1 : index;

    return {
      key: `${align}:${anchorDistance}`,
      previousChar,
      currentChar,
      changed: previousChar !== currentChar,
      delaySteps: Math.min(anchorDistance, SLOT_MAX_STAGGER_STEPS),
    };
  });
}

export default function SlotText({
  value,
  align = 'right',
  className,
}: {
  value: string;
  align?: SlotAlign;
  className?: string;
}) {
  const reduce = Boolean(useReducedMotion());
  const [slotText, setSlotText] = useState({ previous: value, current: value, revision: 0 });

  useEffect(() => {
    if (slotText.current === value) return;
    let frame = 0;
    frame = requestAnimationFrame(() => {
      setSlotText((current) => (
        current.current === value
          ? current
          : { previous: current.current, current: value, revision: current.revision + 1 }
      ));
    });
    return () => cancelAnimationFrame(frame);
  }, [value, slotText]);

  useEffect(() => {
    if (slotText.previous === slotText.current) return;
    const timeout = window.setTimeout(() => {
      setSlotText((current) => (
        current.revision === slotText.revision ? { ...current, previous: current.current } : current
      ));
    }, SLOT_SETTLE_MS);
    return () => window.clearTimeout(timeout);
  }, [slotText]);

  if (reduce) return <span className={className}>{value}</span>;

  const glyphs = glyphsFor(slotText.previous, slotText.current, align);

  return (
    <span className={cn('fm-slot-roll', className)} aria-label={slotText.current}>
      {glyphs.map((glyph) => (
        <span
          key={glyph.key}
          aria-hidden="true"
          className={cn('fm-slot-roll__slot', glyph.changed && 'fm-slot-roll__slot--changed')}
          style={{ '--fm-slot-delay': `${glyph.delaySteps * SLOT_STAGGER_MS}ms` } as CSSProperties}
        >
          <span className="fm-slot-roll__sizer">{glyph.currentChar || glyph.previousChar || ' '}</span>
          <span key={`old:${slotText.revision}:${glyph.previousChar}`} className="fm-slot-roll__face fm-slot-roll__face--old">
            {glyph.previousChar || ' '}
          </span>
          <span key={`new:${slotText.revision}:${glyph.currentChar}`} className="fm-slot-roll__face fm-slot-roll__face--new">
            {glyph.currentChar || ' '}
          </span>
        </span>
      ))}
    </span>
  );
}
