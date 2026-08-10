'use client';

import { AnimatePresence, motion, useReducedMotion } from 'framer-motion';
import { cn } from '@/lib/utils';

const ROLL_SPRING = { type: 'spring', stiffness: 380, damping: 34, mass: 0.84 } as const;

export default function RollingNumber({
  value,
  revision,
  className,
}: {
  value: string;
  revision: number;
  className?: string;
}) {
  const reduce = Boolean(useReducedMotion());

  if (reduce) return <span className={className}>{value}</span>;

  return (
    <span className={cn('relative inline-grid overflow-hidden align-baseline', className)} aria-label={value}>
      <span className="invisible col-start-1 row-start-1" aria-hidden="true">{value}</span>
      <AnimatePresence initial={false} mode="popLayout">
        <motion.span
          key={`${revision}:${value}`}
          initial={{ y: '100%', opacity: 0 }}
          animate={{ y: 0, opacity: 1 }}
          exit={{ y: '-100%', opacity: 0 }}
          transition={ROLL_SPRING}
          className="col-start-1 row-start-1 whitespace-nowrap"
        >
          {value}
        </motion.span>
      </AnimatePresence>
    </span>
  );
}
