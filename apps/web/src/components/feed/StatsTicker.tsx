'use client';

import { motion } from 'framer-motion';
import { cn } from '@/lib/utils';

export type TickerEntry = {
  id: string;
  text: string;
  tone: 'up' | 'down' | 'flat';
};

interface StatsTickerProps {
  entries: TickerEntry[];
}

export default function StatsTicker({ entries }: StatsTickerProps) {
  const fallback: TickerEntry[] = [
    { id: 'f1', text: 'SYSTEM ACTIVE', tone: 'flat' },
    { id: 'f2', text: 'WAITING FOR SIGNALS', tone: 'flat' },
  ];

  const tape = (entries.length ? entries : fallback).slice(0, 24);
  const loop = [...tape, ...tape, ...tape];

  return (
    <div className="w-full overflow-hidden border-y-4 border-black dark:border-white bg-[#050505] py-2.5 mb-8">
      <motion.div
        className="flex whitespace-nowrap"
        animate={{ x: ['0%', '-50%'] }}
        transition={{ repeat: Infinity, ease: 'linear', duration: 28 }}
      >
        <div className="flex items-center gap-3 pr-3">
          {loop.map((entry, idx) => (
            <span
              key={`${entry.id}-${idx}`}
              className={cn(
                'inline-flex items-center gap-2 px-3 py-1 border-2 border-black font-black uppercase tracking-wide text-[11px] md:text-xs',
                entry.tone === 'up' && 'bg-[#CCFF00] text-black',
                entry.tone === 'down' && 'bg-[#FF6B00] text-black',
                entry.tone === 'flat' && 'bg-[#E5E7EB] text-black'
              )}
            >
              <span className="font-mono text-[10px]">{entry.tone === 'up' ? '▲' : entry.tone === 'down' ? '▼' : '•'}</span>
              {entry.text}
            </span>
          ))}
        </div>
      </motion.div>
    </div>
  );
}
