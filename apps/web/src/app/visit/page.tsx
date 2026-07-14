'use client';

import { useState } from 'react';
import FeederVisit from '@/components/feed/FeederVisit';
import { ANUJ_TERRA_VISITS } from '@/data/anujTerraVisits';
import { cn } from '@/lib/utils';

export default function VisitPreviewPage() {
  const [selected, setSelected] = useState<(typeof ANUJ_TERRA_VISITS)[number]['id']>('anuj-w03');
  const run = ANUJ_TERRA_VISITS.find((item) => item.id === selected) ?? ANUJ_TERRA_VISITS[0];

  return (
    <main className="relative h-[100dvh] overflow-hidden bg-background text-foreground" style={{ width: '100vw', maxWidth: '100vw' }}>
      <nav
        className="fixed left-1/2 z-30 -translate-x-1/2 sm:bottom-6"
        style={{ bottom: 'calc(14px + env(safe-area-inset-bottom, 0px))' }}
      >
        <div className="flex gap-1 rounded-full border border-foreground/10 bg-background/80 p-1 shadow-sm backdrop-blur-xl">
          {ANUJ_TERRA_VISITS.map((item) => (
            <button
              key={item.id}
              type="button"
              onClick={() => setSelected(item.id)}
              className={cn(
                'rounded-full px-3 py-2 text-[8px] font-black uppercase tracking-[0.14em] transition-colors sm:px-4 sm:text-[9px]',
                selected === item.id ? 'bg-[#E11D48] text-white' : 'text-foreground/38 hover:text-foreground/70',
              )}
            >
              {item.label}
            </button>
          ))}
        </div>
      </nav>
      <FeederVisit key={run.id} visit={run.visit} />
    </main>
  );
}
