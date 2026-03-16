'use client';

interface BloombergTickerProps {
  entries: string[];
  className?: string;
}

function TickerChips({ entries }: { entries: string[] }) {
  return (
    <div className="flex min-w-max items-center pr-5">
      {entries.map((entry, idx) => {
        const isPositive = entry.includes('↓');
        const isNegative = entry.includes('↑');

        return (
          <div key={`${entry}-${idx}`} className="mx-2 shrink-0">
            <div
              className={[
                'relative overflow-hidden rounded-full px-3 py-1.5 font-mono text-[10px] font-black uppercase tracking-[0.11em] shadow-[0_4px_12px_rgba(0,0,0,0.1),inset_0_1px_1px_rgba(255,255,255,0.4)] dark:shadow-[0_4px_12px_rgba(0,0,0,0.4),inset_0_1px_1px_rgba(255,255,255,0.1)]',
                'bg-black/5 dark:bg-black/40',
                isPositive
                  ? 'text-lime dark:text-lime drop-shadow-none dark:drop-shadow-[0_0_8px_rgba(204,255,0,0.3)]'
                  : isNegative
                    ? 'text-red-600 dark:text-red-400'
                    : 'text-black/70 dark:text-white/70',
              ].join(' ')}
            >
              {entry}
            </div>
          </div>
        );
      })}
    </div>
  );
}

export default function BloombergTicker({ entries, className = '' }: BloombergTickerProps) {
  if (!entries || entries.length === 0) return null;

  return (
    <div className={['relative overflow-hidden select-none', className].join(' ')}>
      <div className="relative overflow-hidden py-1.5">
        <div className="ticker-track-a absolute left-0 top-1/2 -translate-y-1/2 will-change-transform">
          <TickerChips entries={entries} />
        </div>
        <div className="ticker-track-b absolute left-0 top-1/2 -translate-y-1/2 will-change-transform">
          <TickerChips entries={entries} />
        </div>
        <div className="h-8" />
      </div>
    </div>
  );
}
