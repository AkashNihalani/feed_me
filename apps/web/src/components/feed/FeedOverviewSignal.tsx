'use client';

import { motion } from 'framer-motion';

interface FeedOverviewSignalProps {
  title: string;
  feederCount: number;
}

const hoverUp = { whileHover: { y: -2, scale: 1.02 }, whileTap: { scale: 0.98 } };

export default function FeedOverviewSignal({ title, feederCount }: FeedOverviewSignalProps) {
  const scopes = [
    { label: 'All Targets', active: true },
    { label: 'Anchor', active: false },
    { label: 'Rising', active: false },
    { label: 'Stable', active: false },
    { label: 'Drop Risk', active: false },
  ];

  return (
    <div className="fm-glass-panel relative w-full overflow-hidden p-5 md:p-6">
      <div className="absolute inset-0 bg-[radial-gradient(115%_85%_at_0%_0%,rgba(255,255,255,0.28),transparent_45%),radial-gradient(120%_80%_at_100%_100%,rgba(204,255,0,0.08),transparent_52%)] opacity-85" />

      <div className="relative z-10 flex flex-col gap-4">
        <div className="flex flex-col justify-between gap-4 md:flex-row md:items-end">
          <div>
            <h2 className="mb-1 text-[10px] font-black uppercase tracking-[0.22em] text-foreground/55">Signal Scope</h2>
            <div className="flex flex-wrap items-center gap-3">
              <h3 className="text-4xl font-black uppercase tracking-[-0.04em] leading-[0.9] text-foreground/95 md:text-5xl lg:text-6xl">
                {title}
              </h3>
              <motion.div {...hoverUp} className="fm-glass-chip rounded-full px-3 py-1.5">
                <span className="text-[10px] font-black tracking-[0.16em] text-foreground/80">{feederCount} Live</span>
              </motion.div>
            </div>
          </div>

          <motion.div className="fm-glass-chip flex items-center gap-1 rounded-[14px] p-1" whileHover={{ y: -1 }}>
            {['24H', '7D', '30D'].map((t, i) => (
              <motion.button
                key={t}
                whileTap={{ scale: 0.95 }}
                className={`rounded-[10px] px-3 py-1.5 text-[10px] font-black tracking-[0.15em] transition-all ${
                  i === 0
                    ? 'bg-white/88 text-black shadow-[0_4px_10px_rgba(0,0,0,0.2)] dark:bg-white/18 dark:text-white'
                    : 'text-foreground/55 hover:bg-white/35 hover:text-foreground/88 dark:hover:bg-white/12'
                }`}
              >
                {t}
              </motion.button>
            ))}
          </motion.div>
        </div>

        <div className="flex items-center gap-2 overflow-x-auto hide-scrollbar">
          {scopes.map((item) => (
            <motion.button
              key={item.label}
              whileHover={{ y: -2 }}
              whileTap={{ scale: 0.97 }}
              className={`whitespace-nowrap rounded-full px-4 py-2 text-[11px] font-black uppercase tracking-[0.08em] transition-all ${
                item.active
                  ? 'fm-glass-chip text-foreground'
                  : 'border border-white/22 bg-white/14 text-foreground/60 hover:bg-white/24 hover:text-foreground dark:border-white/12 dark:bg-white/5 dark:hover:bg-white/11'
              }`}
            >
              {item.label}
            </motion.button>
          ))}
        </div>
      </div>
    </div>
  );
}
