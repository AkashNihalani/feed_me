'use client';

/* ─────────────────────────────────────────────
   LOGIN SLOT NUMBER

   Discrete textmotion-style number shifts for the pre-login dashboard.
   No continuous counter: when the parent gives a fresh value, changed glyphs
   roll once with a weighted vertical slot animation, then settle to static text.
   Reduced motion -> plain grouped number.
   ───────────────────────────────────────────── */

import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { cn } from '@/lib/utils';
import { usePrefersReducedMotion } from '@/lib/useLiveStats';

const SLOT_STAGGER_MS = 26;
const SLOT_SETTLE_MS = 840;
const NBSP = '\u00a0';
const numberFormat = new Intl.NumberFormat('en-US');

function formatNumber(value: number) {
  return numberFormat.format(Math.max(0, Math.round(value)));
}

function charsFromRight(previous: string, current: string) {
  const previousChars = Array.from(previous);
  const currentChars = Array.from(current);
  const length = Math.max(previousChars.length, currentChars.length);

  return Array.from({ length }, (_, index) => {
    const previousIndex = previousChars.length - length + index;
    const currentIndex = currentChars.length - length + index;
    const previousChar = previousIndex >= 0 ? previousChars[previousIndex] : '';
    const currentChar = currentIndex >= 0 ? currentChars[currentIndex] : '';
    const positionFromRight = length - index - 1;

    return {
      previousChar,
      currentChar,
      positionFromRight,
      changed: previousChar !== currentChar,
    };
  });
}

function glyphTone(char: string) {
  if (!char) return 'empty';
  return /\d/.test(char) ? 'digit' : 'mark';
}

function SlotGlyph({
  previousChar,
  currentChar,
  changed,
  positionFromRight,
  transitionDelayMs,
  revision,
}: {
  previousChar: string;
  currentChar: string;
  changed: boolean;
  positionFromRight: number;
  transitionDelayMs: number;
  revision: number;
}) {
  const displayChar = currentChar || previousChar || NBSP;
  const tone = glyphTone(currentChar || previousChar);
  const delayMs = transitionDelayMs + Math.min(positionFromRight, 10) * SLOT_STAGGER_MS;
  const style = { '--slot-delay': `${delayMs}ms` } as React.CSSProperties;

  return (
    <span
      className={cn(
        'fm-login-slot-number__slot',
        tone === 'digit' && 'fm-login-slot-number__slot--digit',
        tone === 'mark' && 'fm-login-slot-number__slot--mark',
        tone === 'empty' && 'fm-login-slot-number__slot--empty',
        changed && 'fm-login-slot-number__slot--changed',
      )}
      style={style}
      aria-hidden="true"
    >
      <span className="fm-login-slot-number__sizer">{displayChar}</span>
      <span key={`old-${revision}-${previousChar}`} className="fm-login-slot-number__face fm-login-slot-number__face--old">
        {previousChar || NBSP}
      </span>
      <span key={`new-${revision}-${currentChar}`} className="fm-login-slot-number__face fm-login-slot-number__face--new">
        {currentChar || NBSP}
      </span>
    </span>
  );
}

function SlotNumber({
  text,
  animateOnMount,
  revealDelayMs,
}: {
  text: string;
  animateOnMount: boolean;
  revealDelayMs: number;
}) {
  const [slotText, setSlotText] = useState(() => (
    animateOnMount
      ? { previous: '', current: text, revision: 1, transitionDelayMs: revealDelayMs }
      : { previous: text, current: text, revision: 0, transitionDelayMs: 0 }
  ));

  useEffect(() => {
    if (slotText.current === text) return;
    let frame = 0;
    frame = requestAnimationFrame(() => {
      setSlotText((current) => (
        current.current === text
          ? current
          : { previous: current.current, current: text, revision: current.revision + 1, transitionDelayMs: 0 }
      ));
    });

    return () => cancelAnimationFrame(frame);
  }, [text, slotText]);

  useEffect(() => {
    if (slotText.previous === slotText.current) return;
    const timeout = window.setTimeout(() => {
      setSlotText((current) => (
        current.revision === slotText.revision
          ? { ...current, previous: current.current, transitionDelayMs: 0 }
          : current
      ));
    }, SLOT_SETTLE_MS + slotText.transitionDelayMs);

    return () => window.clearTimeout(timeout);
  }, [slotText]);

  const slots = charsFromRight(slotText.previous, slotText.current);

  return (
    <span className="fm-login-slot-number" aria-label={text}>
      {slots.map((slot) => (
        <SlotGlyph
          key={`slot-${slot.positionFromRight}`}
          previousChar={slot.previousChar}
          currentChar={slot.currentChar}
          changed={slot.changed}
          positionFromRight={slot.positionFromRight}
          transitionDelayMs={slotText.transitionDelayMs}
          revision={slotText.revision}
        />
      ))}
    </span>
  );
}

export default function Odometer({
  value,
  color,
  className,
  fallback = '—',
  animateOnMount = false,
  revealDelayMs = 0,
}: {
  value: number | null;
  color?: string;
  className?: string;
  fallback?: string;
  animateOnMount?: boolean;
  revealDelayMs?: number;
  durationMs?: number;
}) {
  const reduced = usePrefersReducedMotion();
  const outerRef = useRef<HTMLSpanElement>(null);
  const innerRef = useRef<HTMLSpanElement>(null);
  const [scale, setScale] = useState(1);

  const hasValue = value != null && Number.isFinite(value);
  const formattedValue = hasValue ? formatNumber(value as number) : fallback;

  useLayoutEffect(() => {
    const measure = () => {
      const outer = outerRef.current;
      const inner = innerRef.current;
      if (!outer || !inner) return;
      const avail = outer.clientWidth;
      const natural = inner.scrollWidth;
      if (natural > 0 && avail > 0) setScale(Math.min(1, avail / natural));
    };

    measure();
    const ro = new ResizeObserver(measure);
    if (outerRef.current) ro.observe(outerRef.current);
    return () => ro.disconnect();
  }, [formattedValue]);

  const innerStyle: React.CSSProperties = {
    transform: scale < 1 ? `scale(${scale})` : undefined,
    transformOrigin: 'left center',
  };

  if (!hasValue || reduced) {
    return (
      <span ref={outerRef} className={cn('block overflow-hidden', className)} style={{ color }}>
        <span ref={innerRef} className="inline-flex tabular-nums leading-none whitespace-nowrap" style={innerStyle}>
          {formattedValue}
        </span>
      </span>
    );
  }

  return (
    <span ref={outerRef} className={cn('block overflow-hidden', className)} style={{ color }}>
      <span ref={innerRef} className="inline-flex tabular-nums leading-none whitespace-nowrap" style={innerStyle}>
        <SlotNumber text={formattedValue} animateOnMount={animateOnMount} revealDelayMs={revealDelayMs} />
      </span>
    </span>
  );
}
