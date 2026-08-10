'use client';

/* Reader Drop preview backed only by records that already ship on main. */

import { useState } from 'react';
import ReaderDrop from '@/components/feed/ReaderDrop';
import { FOLLOWER_RUN_WINDOWS } from '@/app/drop/artifacts/followers/fixture';
import { READER_FEEDERS } from '@/data/readerDrops';
import { cn } from '@/lib/utils';

export default function ReaderDropPreviewPage() {
  const [feederIndex, setFeederIndex] = useState(0);
  const feeder = READER_FEEDERS[feederIndex];
  const [runIndex, setRunIndex] = useState(feeder.drops.length - 1);
  const drop = feeder.drops[Math.min(runIndex, feeder.drops.length - 1)];
  const artifacts = drop.run === 3
    ? { laneLeaders: true, followers: FOLLOWER_RUN_WINDOWS }
    : undefined;

  const pill = 'rounded-full px-3 py-2 text-[8px] font-black uppercase tracking-[0.14em] transition-colors sm:px-4 sm:text-[9px]';

  return (
    <main className="relative h-[100dvh] overflow-hidden bg-background text-foreground" style={{ width: '100vw', maxWidth: '100vw' }}>
      <nav
        className="fixed left-1/2 z-30 flex -translate-x-1/2 items-center gap-2 sm:bottom-6"
        style={{ bottom: 'calc(14px + env(safe-area-inset-bottom, 0px))' }}
      >
        <div className="flex gap-1 rounded-full border border-foreground/10 bg-background/80 p-1 shadow-sm backdrop-blur-xl">
          {READER_FEEDERS.map((item, index) => (
            <button
              key={item.handle}
              type="button"
              onClick={() => {
                setFeederIndex(index);
                setRunIndex(READER_FEEDERS[index].drops.length - 1);
              }}
              className={cn(pill, index === feederIndex ? 'bg-foreground text-background' : 'text-foreground/38 hover:text-foreground/70')}
            >
              @{item.handle}
            </button>
          ))}
        </div>
        <div className="flex gap-1 rounded-full border border-foreground/10 bg-background/80 p-1 shadow-sm backdrop-blur-xl">
          {feeder.drops.map((item, index) => (
            <button
              key={item.run}
              type="button"
              onClick={() => setRunIndex(index)}
              className={cn(pill, index === runIndex ? 'bg-[#E11D48] text-white' : 'text-foreground/38 hover:text-foreground/70')}
            >
              run {index + 1}
            </button>
          ))}
        </div>
      </nav>
      <ReaderDrop artifacts={artifacts} key={`${feeder.handle}:${drop.run}`} drop={drop} />
    </main>
  );
}
