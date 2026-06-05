'use client';

/* ─────────────────────────────────────────────
   ODOMETER — anchored live number

   Values increase in place with no vertical reel/slot-machine motion.
   We still ease between updates, but the glyphs stay on the same baseline,
   which makes the live dashboard feel calmer and more app-like.
   Reduced motion → plain grouped number.
   ───────────────────────────────────────────── */

import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { cn } from '@/lib/utils';
import { usePrefersReducedMotion } from '@/lib/useLiveStats';

const MIN_UPDATE_MS = 6200;
const MAX_UPDATE_MS = 10_000;
const easeInOut = (t: number) => t * t * t * (t * (t * 6 - 15) + 10);
const numberFormat = new Intl.NumberFormat('en-US');

function formatNumber(value: number) {
  return numberFormat.format(Math.max(0, Math.round(value)));
}

function durationForShift(delta: number) {
  if (delta <= 0) return MIN_UPDATE_MS;
  const scaled = MIN_UPDATE_MS + Math.log10(delta + 1) * 720;
  return Math.min(MAX_UPDATE_MS, Math.max(MIN_UPDATE_MS, scaled));
}

export default function Odometer({
  value,
  color,
  className,
  fallback = '—',
  durationMs,
}: {
  value: number | null;
  color?: string;
  className?: string;
  fallback?: string;
  durationMs?: number;
}) {
  const reduced = usePrefersReducedMotion();
  const outerRef = useRef<HTMLSpanElement>(null);
  const innerRef = useRef<HTMLSpanElement>(null);
  const currentRef = useRef<number | null>(null); // live animated value
  const rafRef = useRef(0);
  const commitRef = useRef(0);
  const [scale, setScale] = useState(1);
  const [displayValue, setDisplayValue] = useState<number | null>(null);

  const hasValue = value != null && Number.isFinite(value);
  const target = hasValue ? Math.max(0, Math.round(value as number)) : null;

  useEffect(() => {
    const commitDisplay = (next: number | null) => {
      cancelAnimationFrame(commitRef.current);
      commitRef.current = requestAnimationFrame(() => setDisplayValue(next));
    };

    if (target == null) {
      currentRef.current = null;
      commitDisplay(null);
      return () => cancelAnimationFrame(commitRef.current);
    }

    const from = currentRef.current;

    if (reduced || from == null || Math.round(from) === target) {
      currentRef.current = target;
      commitDisplay(target);
      return () => cancelAnimationFrame(commitRef.current);
    }

    cancelAnimationFrame(rafRef.current);
    const startT = performance.now();
    const duration = durationMs ?? durationForShift(Math.abs(target - from));

    const frame = (now: number) => {
      const t = Math.min(1, (now - startT) / duration);
      const e = easeInOut(t);
      currentRef.current = from + (target - from) * e;
      setDisplayValue(currentRef.current);

      if (t < 1) {
        rafRef.current = requestAnimationFrame(frame);
      } else {
        currentRef.current = target;
        setDisplayValue(target);
      }
    };

    rafRef.current = requestAnimationFrame(frame);
    return () => {
      cancelAnimationFrame(rafRef.current);
      cancelAnimationFrame(commitRef.current);
    };
  }, [fallback, target, reduced, durationMs]);

  // Auto-fit to the tile as the digit count grows (transforms don't change layout width).
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
  }, [target]);

  const innerStyle: React.CSSProperties = {
    transform: scale < 1 ? `scale(${scale})` : undefined,
    transformOrigin: 'left center',
  };
  const renderedValue = displayValue ?? target ?? 0;

  if (!hasValue) {
    return (
      <span ref={outerRef} className={cn('block', className)} style={{ color }}>
        {fallback}
      </span>
    );
  }

  if (reduced) {
    return (
      <span ref={outerRef} className={cn('block overflow-hidden', className)} style={{ color }}>
        <span ref={innerRef} className="inline-flex tabular-nums leading-none whitespace-nowrap" style={innerStyle}>
          {formatNumber(renderedValue)}
        </span>
      </span>
    );
  }

  return (
    <span ref={outerRef} className={cn('block overflow-hidden', className)} style={{ color }}>
      <span ref={innerRef} className="inline-block tabular-nums leading-none whitespace-nowrap" style={innerStyle}>
        {formatNumber(renderedValue)}
      </span>
    </span>
  );
}
