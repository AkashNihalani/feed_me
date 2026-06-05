'use client';

import { motion } from 'framer-motion';
import { Download } from 'lucide-react';

type FeedExportTileProps = {
  scopeLabel: string;
  from: string;
  to: string;
  onFromChange: (value: string) => void;
  onToChange: (value: string) => void;
  onExport: () => void;
};

export default function FeedExportTile({
  scopeLabel,
  from,
  to,
  onFromChange,
  onToChange,
  onExport,
}: FeedExportTileProps) {
  return (
    <div className="fm-depth-glass relative flex h-full w-full flex-col overflow-hidden rounded-[22px] p-3 sm:p-3.5 lg:p-4">
      <div className="relative z-10 flex h-full flex-col">
        <div className="flex items-center justify-between gap-3">
          <div className="min-w-0">
            <div className="fm-label fm-depth-title">Export Window</div>
            <div className="mt-1.5 text-[18px] font-black leading-[0.92] tracking-[-0.04em] text-foreground dark:text-white sm:text-[22px]">
              {scopeLabel}
            </div>
          </div>
          <div className="rounded-full border border-[#FB7185] bg-[#E11D48] px-2 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-white shadow-[0_4px_10px_rgba(225,29,72,0.22)]">
            XLS
          </div>
        </div>

        <div className="mt-2.5 grid grid-cols-2 gap-2">
          <label className="flex flex-col gap-0.5 rounded-[12px] border border-white/82 bg-white/74 px-2.5 py-2 shadow-[inset_0_1px_0_rgba(255,255,255,0.98),0_3px_8px_rgba(15,23,42,0.05)] dark:border-white/10 dark:bg-white/[0.05] dark:shadow-[0_3px_10px_rgba(0,0,0,0.35),0_1px_0_rgba(255,255,255,0.06)_inset]">
            <span className="text-[8px] font-black uppercase tracking-[0.12em] text-black/42 dark:text-white/36">From</span>
            <input
              type="date"
              value={from}
              max={to || undefined}
              onChange={(event) => onFromChange(event.target.value)}
              className="bg-transparent text-[16px] font-black tracking-[0.02em] text-black outline-none dark:text-white"
            />
          </label>
          <label className="flex flex-col gap-0.5 rounded-[12px] border border-white/82 bg-white/74 px-2.5 py-2 shadow-[inset_0_1px_0_rgba(255,255,255,0.98),0_3px_8px_rgba(15,23,42,0.05)] dark:border-white/10 dark:bg-white/[0.05] dark:shadow-[0_3px_10px_rgba(0,0,0,0.35),0_1px_0_rgba(255,255,255,0.06)_inset]">
            <span className="text-[8px] font-black uppercase tracking-[0.12em] text-black/42 dark:text-white/36">To</span>
            <input
              type="date"
              value={to}
              min={from || undefined}
              onChange={(event) => onToChange(event.target.value)}
              className="bg-transparent text-[16px] font-black tracking-[0.02em] text-black outline-none dark:text-white"
            />
          </label>
        </div>

        <div className="mt-2.5 flex flex-1 flex-col justify-end gap-1.5">
          <motion.button
            type="button"
            whileTap={{ scale: 0.98 }}
            onClick={onExport}
            className="flex w-full items-center justify-center gap-2 rounded-[14px] bg-[#E11D48] px-3.5 py-2.5 text-[10px] font-black uppercase tracking-[0.14em] text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.72),0_8px_18px_-8px_rgba(225,29,72,0.32)] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.2),0_10px_24px_-10px_rgba(225,29,72,0.28)]"
          >
            <Download size={14} strokeWidth={2.8} />
            Export XLS
          </motion.button>
          <div className="text-center text-[8px] font-bold uppercase tracking-[0.1em] text-foreground/32 dark:text-white/26">
            Last 90 days · selected handle scope
          </div>
        </div>
      </div>
    </div>
  );
}
