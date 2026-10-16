'use client';

/* ─────────────────────────────────────────────
   SLOT TEXT

   The login odometer grammar (components/login/Odometer.tsx) generalized to
   arbitrary strings: when the value changes, only the glyphs that differ roll
   with the weighted vertical slot animation, then settle to static text.
   `align="right"` diffs from the units glyph (numbers), `align="left"` from
   the first glyph (words). Reduced motion -> plain text swap.

   A slot text rolls in the render that changes it (the value it is leaving
   is kept as state set while rendering) and every slot text that rolled
   settles in the same task, on one shared timer: a page of numbers changing
   together costs one extra render, not two per number. On WebKit the roll
   waits at its start for the frame that draws it (lib/webkitMotion), so it
   plays with everything else the switch set moving.

   `delay` starts the roll later (a row's beat in a list re-dealt top first,
   components/lead/ledgerMotion): the shared settle waits for the latest roll
   to finish, so nothing settles in the middle of a cascade.
   ───────────────────────────────────────────── */

import { CSSProperties, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { useReducedMotion } from 'framer-motion';
import { cn } from '@/lib/utils';
import { afterDrawn, holdsFlights } from '@/lib/webkitMotion';
import { ms } from '@/lib/tempo';

// the stagger is played out by TEMPO here (it reaches the CSS as --fm-slot-delay, not stretched again there); the
// faces' 420ms roll and their 34ms offset are stretched in globals.css (.fm-slot-roll), and the settle waits for that
const SLOT_STAGGER_MS = ms(24);
const SLOT_MAX_STAGGER_STEPS = 10;
const SLOT_SETTLE_MS = ms(420) + ms(34) + SLOT_MAX_STAGGER_STEPS * SLOT_STAGGER_MS + 60;

// the rolls that started together settle together, once the last of them has finished (a roll joining later moves
// the settle on, so none is cut short)
const settling = new Set<() => void>();
let settleTimer = 0;
let settleAt = 0;
function settleLater(settle: () => void, delay: number) {
  settling.add(settle);
  const at = performance.now() + SLOT_SETTLE_MS + delay;
  if (!settleTimer || at > settleAt) {
    window.clearTimeout(settleTimer);
    settleAt = at;
    settleTimer = window.setTimeout(() => {
      settleTimer = 0;
      const all = [...settling];
      settling.clear();
      all.forEach((one) => one());
    }, at - performance.now());
  }
  return () => {
    settling.delete(settle);
  };
}

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
  delay = 0,
}: {
  value: string;
  align?: SlotAlign;
  className?: string;
  // ms before the roll starts
  delay?: number;
}) {
  const reduce = Boolean(useReducedMotion());
  // the delay a roll started with stays its own (a later render with another delay doesn't re-time a roll under way)
  const [slotText, setSlotText] = useState({ previous: value, current: value, revision: 0, delay });

  if (slotText.current !== value) setSlotText({ previous: slotText.current, current: value, revision: slotText.revision + 1, delay });

  // on WebKit a roll waits at its start until the frame that draws it is on screen
  const rootRef = useRef<HTMLSpanElement | null>(null);
  useLayoutEffect(() => {
    const el = rootRef.current;
    if (!el || !slotText.revision || !holdsFlights()) return undefined;
    el.style.setProperty('--fm-slot-play', 'paused');
    return afterDrawn(() => el.style.removeProperty('--fm-slot-play'));
  }, [slotText.revision]);

  useEffect(() => {
    if (slotText.previous === slotText.current) return undefined;
    const settle = () => setSlotText((current) => (
      current.revision === slotText.revision ? { ...current, previous: current.current } : current
    ));
    if (!holdsFlights()) return settleLater(settle, slotText.delay);
    // on WebKit the roll starts once its frame is on screen: so does the clock that settles it
    let stop = () => {};
    const cancel = afterDrawn(() => {
      stop = settleLater(settle, slotText.delay);
    });
    return () => {
      cancel();
      stop();
    };
  }, [slotText]);

  if (reduce) return <span className={className}>{value}</span>;

  const glyphs = glyphsFor(slotText.previous, slotText.current, align);

  return (
    <span ref={rootRef} className={cn('fm-slot-roll', className)} aria-label={slotText.current} style={slotText.delay ? ({ '--fm-slot-base': `${slotText.delay}ms` } as CSSProperties) : undefined}>
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
